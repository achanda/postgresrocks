#!/bin/bash

set -euo pipefail

DB_NAME="${DB_NAME:-postgres}"
ROWS="${ROWS:-50000}"
RUNS="${RUNS:-3}"
PSQL="${PSQL:-psql}"
READ_POINT_QUERIES="${READ_POINT_QUERIES:-1000}"
BENCHMARK_WRITE_MODE="${BENCHMARK_WRITE_MODE:-0}"
SCHEMA_PROFILE="${SCHEMA_PROFILE:-narrow}"
BENCH_SUFFIX="${BENCH_SUFFIX:-${SCHEMA_PROFILE}_${ROWS}_$$}"
LAST_AVG_MS=""
LAST_WARM_AVG_MS=""
LAST_CREATE_AVG_MS=""
LAST_INSERT_AVG_MS=""
LAST_CREATE_WARM_AVG_MS=""
LAST_INSERT_WARM_AVG_MS=""
HEAP_CREATE_AVG_MS=""
HEAP_INSERT_AVG_MS=""
HEAP_CREATE_WARM_AVG_MS=""
HEAP_INSERT_WARM_AVG_MS=""
PGROCKS_CREATE_AVG_MS=""
PGROCKS_INSERT_AVG_MS=""
PGROCKS_CREATE_WARM_AVG_MS=""
PGROCKS_INSERT_WARM_AVG_MS=""
HEAP_SEQ_AVG_MS=""
HEAP_POINT_AVG_MS=""
HEAP_SEQ_WARM_AVG_MS=""
HEAP_POINT_WARM_AVG_MS=""
PGROCKS_SEQ_AVG_MS=""
PGROCKS_SCAN_POINT_AVG_MS=""
PGROCKS_DIRECT_LOOKUP_AVG_MS=""
PGROCKS_METADATA_COUNT_MS=""
PGROCKS_SEQ_WARM_AVG_MS=""
PGROCKS_SCAN_POINT_WARM_AVG_MS=""
PGROCKS_DIRECT_LOOKUP_WARM_AVG_MS=""

if ! command -v "$PSQL" >/dev/null 2>&1; then
    echo "psql not found: $PSQL" >&2
    exit 1
fi

PGROCKS_SETUP_SQL="LOAD 'postgresrocks';"
if [ "$BENCHMARK_WRITE_MODE" = "1" ]; then
    PGROCKS_SETUP_SQL="$PGROCKS_SETUP_SQL SET postgresrocks.benchmark_write_mode = on;"
fi
if [ "$SCHEMA_PROFILE" != "narrow" ]; then
    PGROCKS_SETUP_SQL="$PGROCKS_SETUP_SQL SET postgresrocks.enable_shared_writer = off;"
fi

SCHEMA_SQL=""
INSERT_SELECT_SQL=""

case "$SCHEMA_PROFILE" in
    narrow)
        SCHEMA_SQL="(id integer, name text)"
        INSERT_SELECT_SQL="SELECT g, 'value-' || g
FROM generate_series(1, \$ROWS) AS g"
        ;;
    medium)
        SCHEMA_SQL="(id integer, name text, score integer, amount bigint, tag text, note text, bucket integer, city text)"
        INSERT_SELECT_SQL="SELECT g,
       'value-' || g,
       (g % 1000),
       (g::bigint * 17),
       'tag-' || (g % 50),
       'note-' || (g % 100),
       (g % 32),
       'city-' || (g % 20)
FROM generate_series(1, \$ROWS) AS g"
        ;;
    wide)
        SCHEMA_SQL="(id integer, name text, score integer, amount bigint, tag text, note text, bucket integer, city text, region text, status integer, comment text, payload text, counter bigint, category integer, zip text, code text)"
        INSERT_SELECT_SQL="SELECT g,
       'value-' || g,
       (g % 1000),
       (g::bigint * 17),
       'tag-' || (g % 50),
       'note-' || (g % 100),
       (g % 32),
       'city-' || (g % 20),
       'region-' || (g % 10),
       (g % 7),
       'comment-' || (g % 200),
       repeat(md5(g::text), 2),
       (g::bigint * 100),
       (g % 16),
       lpad(((g % 100000)::text), 5, '0'),
       'code-' || (g % 500)
FROM generate_series(1, \$ROWS) AS g"
        ;;
    *)
        echo "Unsupported SCHEMA_PROFILE: $SCHEMA_PROFILE" >&2
        exit 1
        ;;
esac

INSERT_SELECT_EXPANDED="${INSERT_SELECT_SQL//\$ROWS/$ROWS}"
HEAP_WRITE_TABLE="bench_heap_${BENCH_SUFFIX}"
PGROCKS_WRITE_TABLE="bench_postgresrocks_${BENCH_SUFFIX}"
HEAP_READ_TABLE="bench_heap_read_${BENCH_SUFFIX}"
PGROCKS_READ_TABLE="bench_postgresrocks_read_${BENCH_SUFFIX}"
HEAP_READ_INDEX="${HEAP_READ_TABLE}_id_idx"
PGROCKS_READ_INDEX="${PGROCKS_READ_TABLE}_id_idx"

run_sql() {
    "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -q -c "$1" >/dev/null
}

run_sql_best_effort() {
    "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -q -c "$1" >/dev/null 2>&1 || true
}

extract_duration_ms() {
    perl -e '
        my @lines = <STDIN>;
        chomp @lines;
        my ($start, $end);
        for (my $i = 0; $i < @lines; $i++) {
            $start = $lines[$i + 1] if $lines[$i] eq "BENCH_START";
            $end = $lines[$i + 1] if $lines[$i] eq "BENCH_END";
        }
        printf "%.3f", ($end - $start) * 1000;
    '
}

format_count() {
    perl -e 'my $n = shift; $n =~ s/(\d)(?=(\d{3})+(\D|$))/$1,/g; print $n;' "$1"
}

format_ms_value() {
    perl -e 'printf "%.3f", ($ARGV[0] || 0) / 1000.0' "$1"
}

compute_avg_ms() {
    perl -e 'printf "%.3f", $ARGV[0] / $ARGV[1]' "$1" "$2"
}

extract_section_times() {
    local file="$1"
    local marker="$2"
    local limit="$3"

    perl -e '
        my ($marker, $limit) = @ARGV;
        my $in = 0;
        my @vals;
        while (<STDIN>) {
            chomp;
            if ($_ eq $marker) {
                $in = 1;
                next;
            }
            next unless $in;
            if (/^bench_/ && $_ ne $marker) {
                last;
            }
            if (/^Time: ([0-9.]+) ms$/) {
                push @vals, $1;
                last if @vals >= $limit;
            }
        }
        print join("\n", @vals);
    ' "$marker" "$limit" < "$file"
}

average_from_stream() {
    perl -ne 'chomp; next if $_ eq q{}; $sum += $_; $n++; END { printf "%.3f", $n ? $sum / $n : 0 }'
}

warm_average_from_stream() {
    perl -ne 'chomp; next if $_ eq q{}; $i++; next if $i == 1; $sum += $_; $n++; END { printf "%.3f", $n ? $sum / $n : 0 }'
}

print_summary_table() {
    echo
    echo "summary table"
    printf "%-34s %14s %18s %15s %19s\n" "workload" "heap avg" "postgresrocks avg" "heap warm" "postgresrocks warm"
    printf "%-34s %14s %18s %15s %19s\n" "----------------------------------" "--------------" "------------------" "---------------" "-------------------"
    printf "%-34s %14s %18s %15s %19s\n" "create table" "${HEAP_CREATE_AVG_MS:-n/a}" "${PGROCKS_CREATE_AVG_MS:-n/a}" "${HEAP_CREATE_WARM_AVG_MS:-n/a}" "${PGROCKS_CREATE_WARM_AVG_MS:-n/a}"
    printf "%-34s %14s %18s %15s %19s\n" "insert" "${HEAP_INSERT_AVG_MS:-n/a}" "${PGROCKS_INSERT_AVG_MS:-n/a}" "${HEAP_INSERT_WARM_AVG_MS:-n/a}" "${PGROCKS_INSERT_WARM_AVG_MS:-n/a}"
    printf "%-34s %14s %18s %15s %19s\n" "seq read" "${HEAP_SEQ_AVG_MS:-n/a}" "${PGROCKS_SEQ_AVG_MS:-n/a}" "${HEAP_SEQ_WARM_AVG_MS:-n/a}" "${PGROCKS_SEQ_WARM_AVG_MS:-n/a}"
    printf "%-34s %14s %18s %15s %19s\n" "point read" "${HEAP_POINT_AVG_MS:-n/a}" "${PGROCKS_SCAN_POINT_AVG_MS:-n/a}" "${HEAP_POINT_WARM_AVG_MS:-n/a}" "${PGROCKS_SCAN_POINT_WARM_AVG_MS:-n/a}"
    printf "%-34s %14s %18s %15s %19s\n" "direct rowid lookup" "n/a" "${PGROCKS_DIRECT_LOOKUP_AVG_MS:-n/a}" "n/a" "${PGROCKS_DIRECT_LOOKUP_WARM_AVG_MS:-n/a}"
    printf "%-34s %14s %18s %15s %19s\n" "metadata row count" "n/a" "${PGROCKS_METADATA_COUNT_MS:-n/a}" "n/a" "${PGROCKS_METADATA_COUNT_MS:-n/a}"
}

measure_sql_runs() {
    local label="$1"
    local body_sql="$2"
    local total_ms="0"
    local warm_total_ms="0"
    local run

    echo
    echo "$label"

    for run in $(seq 1 "$RUNS"); do
        local sql_file result_file duration
        sql_file="$(mktemp)"
        result_file="$(mktemp)"

        cat > "$sql_file" <<SQL
\echo BENCH_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
$body_sql
\echo BENCH_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
SQL

        "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -At -F '|' -f "$sql_file" > "$result_file"
        duration="$(extract_duration_ms < "$result_file")"
        total_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$total_ms" "$duration")"
        if [ "$run" -gt 1 ]; then
            warm_total_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$warm_total_ms" "$duration")"
        fi

        rm -f "$sql_file" "$result_file"
        echo "  run $run: ${duration} ms"
    done

    LAST_AVG_MS="$(compute_avg_ms "$total_ms" "$RUNS")"
    if [ "$RUNS" -gt 1 ]; then
        LAST_WARM_AVG_MS="$(compute_avg_ms "$warm_total_ms" "$((RUNS - 1))")"
    else
        LAST_WARM_AVG_MS="$LAST_AVG_MS"
    fi
    printf "  avg: %s ms\n" "$LAST_AVG_MS"
    printf "  warm avg: %s ms\n" "$LAST_WARM_AVG_MS"
}

measure_create_insert_runs() {
    local label="$1"
    local table_name="$2"
    local create_sql="$3"
    local insert_sql="$4"
    local setup_sql="${5:-}"
    local stats_sql="${6:-}"
    local total_create_ms="0"
    local total_insert_ms="0"
    local warm_create_ms="0"
    local warm_insert_ms="0"
    local run

    echo
    echo "$label"

    for run in $(seq 1 "$RUNS"); do
        local sql_file result_file create_ms insert_ms
        sql_file="$(mktemp)"
        result_file="$(mktemp)"

        cat > "$sql_file" <<SQL
$setup_sql
DROP TABLE IF EXISTS $table_name;
\echo CREATE_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
$create_sql
\echo CREATE_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
\echo INSERT_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
$insert_sql
\echo INSERT_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
${stats_sql:+\echo INSERT_STATS}
${stats_sql:+$stats_sql}
DROP TABLE $table_name;
SQL

        "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -At -F '|' -f "$sql_file" > "$result_file"

        create_ms="$(
            perl -e '
                my @lines = <STDIN>;
                chomp @lines;
                my ($start, $end);
                for (my $i = 0; $i < @lines; $i++) {
                    $start = $lines[$i + 1] if $lines[$i] eq "CREATE_START";
                    $end = $lines[$i + 1] if $lines[$i] eq "CREATE_END";
                }
                printf "%.3f", ($end - $start) * 1000;
            ' < "$result_file"
        )"

        insert_ms="$(
            perl -e '
                my @lines = <STDIN>;
                chomp @lines;
                my ($start, $end);
                for (my $i = 0; $i < @lines; $i++) {
                    $start = $lines[$i + 1] if $lines[$i] eq "INSERT_START";
                    $end = $lines[$i + 1] if $lines[$i] eq "INSERT_END";
                }
                printf "%.3f", ($end - $start) * 1000;
            ' < "$result_file"
        )"

        total_create_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$total_create_ms" "$create_ms")"
        total_insert_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$total_insert_ms" "$insert_ms")"
        if [ "$run" -gt 1 ]; then
            warm_create_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$warm_create_ms" "$create_ms")"
            warm_insert_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$warm_insert_ms" "$insert_ms")"
        fi

        echo "  run $run create: ${create_ms} ms"
        echo "  run $run insert: ${insert_ms} ms"

        if [ -n "$stats_sql" ]; then
            perl -ne '
                chomp;
                if ($_ eq "INSERT_STATS") {
                    my $stats = <STDIN>;
                    chomp($stats);
                    my @f = split(/\|/, $stats);
                    my ($serialize_calls, $serialize_us,
                        $batch_flush_calls, $batch_flush_rows, $batch_flush_us,
                        $metadata_flush_calls, $metadata_flush_us,
                        $rowid_calls, $rowid_us,
                        $batch_put_calls, $batch_put_us,
                        $tuple_insert_us,
                        $init_rocksdb_calls, $init_rocksdb_us,
                        $precommit_flush_us) = @f;
                    for my $count_ref (\$serialize_calls, \$batch_flush_calls, \$batch_flush_rows,
                                       \$metadata_flush_calls, \$rowid_calls,
                                       \$batch_put_calls, \$init_rocksdb_calls) {
                        $$count_ref =~ s/(\d)(?=(\d{3})+(\D|$))/$1,/g;
                    }
                    printf "    direct stats: serialize=%s calls / %.3f ms, batch flush=%s calls / %s rows / %.3f ms, metadata flush=%s calls / %.3f ms, rowid=%s calls / %.3f ms, batch_put=%s calls / %.3f ms, tuple_insert=%.3f ms, init_rocksdb=%s calls / %.3f ms, precommit_flush=%.3f ms\n",
                        $serialize_calls, $serialize_us / 1000.0,
                        $batch_flush_calls, $batch_flush_rows, $batch_flush_us / 1000.0,
                        $metadata_flush_calls, $metadata_flush_us / 1000.0,
                        $rowid_calls, $rowid_us / 1000.0,
                        $batch_put_calls, $batch_put_us / 1000.0,
                        $tuple_insert_us / 1000.0,
                        $init_rocksdb_calls, $init_rocksdb_us / 1000.0,
                        $precommit_flush_us / 1000.0;
                }
            ' < "$result_file"
        fi

        rm -f "$sql_file" "$result_file"
    done

    LAST_CREATE_AVG_MS="$(compute_avg_ms "$total_create_ms" "$RUNS")"
    LAST_INSERT_AVG_MS="$(compute_avg_ms "$total_insert_ms" "$RUNS")"
    if [ "$RUNS" -gt 1 ]; then
        LAST_CREATE_WARM_AVG_MS="$(compute_avg_ms "$warm_create_ms" "$((RUNS - 1))")"
        LAST_INSERT_WARM_AVG_MS="$(compute_avg_ms "$warm_insert_ms" "$((RUNS - 1))")"
    else
        LAST_CREATE_WARM_AVG_MS="$LAST_CREATE_AVG_MS"
        LAST_INSERT_WARM_AVG_MS="$LAST_INSERT_AVG_MS"
    fi
    printf "  create avg: %s ms\n" "$LAST_CREATE_AVG_MS"
    printf "  insert avg: %s ms\n" "$LAST_INSERT_AVG_MS"
    printf "  create warm avg: %s ms\n" "$LAST_CREATE_WARM_AVG_MS"
    printf "  insert warm avg: %s ms\n" "$LAST_INSERT_WARM_AVG_MS"
}

echo "Database: $DB_NAME"
echo "Rows per run: $(format_count "$ROWS")"
echo "Runs: $(format_count "$RUNS")"
echo "Point lookups per read run: $(format_count "$READ_POINT_QUERIES")"
echo "Benchmark write mode: $BENCHMARK_WRITE_MODE"
echo "Schema profile: $SCHEMA_PROFILE"

run_sql "CREATE EXTENSION IF NOT EXISTS postgresrocks"

measure_create_insert_runs "bench_heap write path" \
    "$HEAP_WRITE_TABLE" \
    "CREATE TABLE $HEAP_WRITE_TABLE $SCHEMA_SQL;" \
    "INSERT INTO $HEAP_WRITE_TABLE
$INSERT_SELECT_EXPANDED;"
HEAP_CREATE_AVG_MS="$LAST_CREATE_AVG_MS"
HEAP_INSERT_AVG_MS="$LAST_INSERT_AVG_MS"
HEAP_CREATE_WARM_AVG_MS="$LAST_CREATE_WARM_AVG_MS"
HEAP_INSERT_WARM_AVG_MS="$LAST_INSERT_WARM_AVG_MS"

measure_create_insert_runs "bench_postgresrocks write path" \
    "$PGROCKS_WRITE_TABLE" \
    "CREATE TABLE $PGROCKS_WRITE_TABLE $SCHEMA_SQL USING postgresrocks;" \
    "INSERT INTO $PGROCKS_WRITE_TABLE
$INSERT_SELECT_EXPANDED;" \
    "$PGROCKS_SETUP_SQL
SELECT postgresrocks_reset_insert_stats();" \
    "SELECT * FROM postgresrocks_direct_write_stats();"
PGROCKS_CREATE_AVG_MS="$LAST_CREATE_AVG_MS"
PGROCKS_INSERT_AVG_MS="$LAST_INSERT_AVG_MS"
PGROCKS_CREATE_WARM_AVG_MS="$LAST_CREATE_WARM_AVG_MS"
PGROCKS_INSERT_WARM_AVG_MS="$LAST_INSERT_WARM_AVG_MS"

run_sql "DROP TABLE IF EXISTS $HEAP_READ_TABLE"
run_sql "CREATE TABLE $HEAP_READ_TABLE $SCHEMA_SQL"
run_sql "INSERT INTO $HEAP_READ_TABLE
         $INSERT_SELECT_EXPANDED"
run_sql "CREATE INDEX $HEAP_READ_INDEX ON $HEAP_READ_TABLE(id)"
run_sql "ANALYZE $HEAP_READ_TABLE"

measure_sql_runs "bench_heap seq reads" "
SELECT count(*) FROM $HEAP_READ_TABLE WHERE id > 0;
"
HEAP_SEQ_AVG_MS="$LAST_AVG_MS"
HEAP_SEQ_WARM_AVG_MS="$LAST_WARM_AVG_MS"

measure_sql_runs "bench_heap indexed point reads" "
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM $HEAP_READ_TABLE
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
"
HEAP_POINT_AVG_MS="$LAST_AVG_MS"
HEAP_POINT_WARM_AVG_MS="$LAST_WARM_AVG_MS"

echo
echo "bench_postgresrocks combined bench"
combined_output_file="$(mktemp)"
"$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" > "$combined_output_file" <<SQL
$PGROCKS_SETUP_SQL
DROP TABLE IF EXISTS $PGROCKS_READ_TABLE;
CREATE TABLE $PGROCKS_READ_TABLE $SCHEMA_SQL USING postgresrocks;
INSERT INTO $PGROCKS_READ_TABLE
$INSERT_SELECT_EXPANDED;
CREATE INDEX $PGROCKS_READ_INDEX ON $PGROCKS_READ_TABLE(id);
ANALYZE $PGROCKS_READ_TABLE;
SELECT postgresrocks_force_flush();
SELECT postgresrocks_reset_read_cache();
\echo
\echo bench_postgresrocks seq reads
\timing on
SELECT count(*) FROM $PGROCKS_READ_TABLE WHERE id > 0;
SELECT count(*) FROM $PGROCKS_READ_TABLE WHERE id > 0;
SELECT count(*) FROM $PGROCKS_READ_TABLE WHERE id > 0;
\echo
\echo bench_postgresrocks indexed point reads
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM $PGROCKS_READ_TABLE
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM $PGROCKS_READ_TABLE
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM $PGROCKS_READ_TABLE
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
\echo
\echo bench_postgresrocks direct rowid lookups
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
WHERE postgresrocks_lookup_text('$PGROCKS_READ_TABLE', ((g * 37) % $ROWS) + 1, 2) IS NOT NULL;
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
WHERE postgresrocks_lookup_text('$PGROCKS_READ_TABLE', ((g * 37) % $ROWS) + 1, 2) IS NOT NULL;
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
WHERE postgresrocks_lookup_text('$PGROCKS_READ_TABLE', ((g * 37) % $ROWS) + 1, 2) IS NOT NULL;
\echo
\echo bench_postgresrocks metadata row count
SELECT postgresrocks_count_rows('$PGROCKS_READ_TABLE');
\echo
\echo bench_heap seq reads plan
EXPLAIN (ANALYZE, COSTS ON) SELECT count(*) FROM $HEAP_READ_TABLE WHERE id > 0;
\echo
\echo bench_postgresrocks seq reads plan
EXPLAIN (ANALYZE, COSTS ON) SELECT count(*) FROM $PGROCKS_READ_TABLE WHERE id > 0;
\echo
\echo bench_heap indexed point reads plan
EXPLAIN (ANALYZE, COSTS ON)
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM $HEAP_READ_TABLE
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
\echo
\echo bench_postgresrocks indexed point reads plan
EXPLAIN (ANALYZE, COSTS ON)
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM $PGROCKS_READ_TABLE
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
\echo
\echo bench_postgresrocks direct rowid lookups plan
EXPLAIN (ANALYZE, COSTS ON)
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
WHERE postgresrocks_lookup_text('$PGROCKS_READ_TABLE', ((g * 37) % $ROWS) + 1, 2) IS NOT NULL;
SQL
cat "$combined_output_file"

PGROCKS_SEQ_AVG_MS="$(extract_section_times "$combined_output_file" "bench_postgresrocks seq reads" "$RUNS" | average_from_stream)"
PGROCKS_SEQ_WARM_AVG_MS="$(extract_section_times "$combined_output_file" "bench_postgresrocks seq reads" "$RUNS" | warm_average_from_stream)"
PGROCKS_SCAN_POINT_AVG_MS="$(extract_section_times "$combined_output_file" "bench_postgresrocks indexed point reads" "$RUNS" | average_from_stream)"
PGROCKS_SCAN_POINT_WARM_AVG_MS="$(extract_section_times "$combined_output_file" "bench_postgresrocks indexed point reads" "$RUNS" | warm_average_from_stream)"
PGROCKS_DIRECT_LOOKUP_AVG_MS="$(extract_section_times "$combined_output_file" "bench_postgresrocks direct rowid lookups" "$RUNS" | average_from_stream)"
PGROCKS_DIRECT_LOOKUP_WARM_AVG_MS="$(extract_section_times "$combined_output_file" "bench_postgresrocks direct rowid lookups" "$RUNS" | warm_average_from_stream)"
PGROCKS_METADATA_COUNT_MS="$(extract_section_times "$combined_output_file" "bench_postgresrocks metadata row count" "1" | head -n 1)"
rm -f "$combined_output_file"

print_summary_table

run_sql_best_effort "DROP TABLE IF EXISTS $HEAP_WRITE_TABLE"
run_sql_best_effort "DROP TABLE IF EXISTS $HEAP_READ_TABLE"
