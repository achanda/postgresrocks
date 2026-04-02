#!/bin/bash

set -euo pipefail

DB_NAME="${DB_NAME:-postgres}"
ROWS="${ROWS:-50000}"
RUNS="${RUNS:-3}"
PSQL="${PSQL:-psql}"
READ_POINT_QUERIES="${READ_POINT_QUERIES:-1000}"
BENCHMARK_WRITE_MODE="${BENCHMARK_WRITE_MODE:-0}"

if ! command -v "$PSQL" >/dev/null 2>&1; then
    echo "psql not found: $PSQL" >&2
    exit 1
fi

PGROCKS_SETUP_SQL="LOAD 'postgresrocks';"
if [ "$BENCHMARK_WRITE_MODE" = "1" ]; then
    PGROCKS_SETUP_SQL="$PGROCKS_SETUP_SQL SET postgresrocks.benchmark_write_mode = on;"
fi

run_sql() {
    "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -q -c "$1" >/dev/null
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

measure_sql_runs() {
    local label="$1"
    local body_sql="$2"
    local total_ms="0"
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

        rm -f "$sql_file" "$result_file"
        echo "  run $run: ${duration} ms"
    done

    perl -e 'printf "  avg: %.3f ms\n", $ARGV[0] / $ARGV[1]' "$total_ms" "$RUNS"
}

echo "Database: $DB_NAME"
echo "Rows per run: $ROWS"
echo "Runs: $RUNS"
echo "Point lookups per read run: $READ_POINT_QUERIES"
echo "Benchmark write mode: $BENCHMARK_WRITE_MODE"

run_sql "DROP EXTENSION IF EXISTS postgresrocks CASCADE"
run_sql "CREATE EXTENSION postgresrocks"

measure_sql_runs "bench_heap inserts" "
DROP TABLE IF EXISTS bench_heap;
CREATE TABLE bench_heap (id integer, name text);
INSERT INTO bench_heap
SELECT g, 'value-' || g
FROM generate_series(1, $ROWS) AS g;
"

echo
echo "bench_postgresrocks inserts"
"$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -At -F '|' <<SQL > /tmp/postgresrocks-insert-bench.out
$PGROCKS_SETUP_SQL
DROP TABLE IF EXISTS bench_postgresrocks;
\echo PGR_INSERT_1_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
CREATE TABLE bench_postgresrocks (id integer, name text) USING postgresrocks;
INSERT INTO bench_postgresrocks
SELECT g, 'value-' || g
FROM generate_series(1, $ROWS) AS g;
\echo PGR_INSERT_1_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
DROP TABLE bench_postgresrocks;
\echo PGR_INSERT_2_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
CREATE TABLE bench_postgresrocks (id integer, name text) USING postgresrocks;
INSERT INTO bench_postgresrocks
SELECT g, 'value-' || g
FROM generate_series(1, $ROWS) AS g;
\echo PGR_INSERT_2_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
DROP TABLE bench_postgresrocks;
\echo PGR_INSERT_3_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
CREATE TABLE bench_postgresrocks (id integer, name text) USING postgresrocks;
INSERT INTO bench_postgresrocks
SELECT g, 'value-' || g
FROM generate_series(1, $ROWS) AS g;
\echo PGR_INSERT_3_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
DROP TABLE bench_postgresrocks;
SQL
perl -ne '
BEGIN { $sum = 0; }
chomp;
$start{$1} = <> if /^PGR_INSERT_(\d+)_START$/;
$end{$1} = <> if /^PGR_INSERT_(\d+)_END$/;
END {
  for my $i (1..3) {
    chomp($start{$i});
    chomp($end{$i});
    my $ms = ($end{$i} - $start{$i}) * 1000;
    $sum += $ms;
    printf "  run %d: %.3f ms\n", $i, $ms;
  }
  printf "  avg: %.3f ms\n", $sum / 3;
}' /tmp/postgresrocks-insert-bench.out
rm -f /tmp/postgresrocks-insert-bench.out

run_sql "DROP TABLE IF EXISTS bench_heap_read"
run_sql "CREATE TABLE bench_heap_read (id integer, name text)"
run_sql "INSERT INTO bench_heap_read
         SELECT g, 'value-' || g
         FROM generate_series(1, $ROWS) AS g"

measure_sql_runs "bench_heap seq reads" "
SELECT count(*) FROM bench_heap_read WHERE id > 0;
"

measure_sql_runs "bench_heap point reads" "
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM bench_heap_read
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
"

echo
echo "bench_postgresrocks combined bench"
"$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" <<SQL
$PGROCKS_SETUP_SQL
DROP TABLE IF EXISTS bench_postgresrocks_read;
CREATE TABLE bench_postgresrocks_read (id integer, name text) USING postgresrocks;
INSERT INTO bench_postgresrocks_read
SELECT g, 'value-' || g
FROM generate_series(1, $ROWS) AS g;
SELECT postgresrocks_force_flush();
\echo
\echo bench_postgresrocks seq reads
\timing on
SELECT count(*) FROM bench_postgresrocks_read WHERE id > 0;
SELECT count(*) FROM bench_postgresrocks_read WHERE id > 0;
SELECT count(*) FROM bench_postgresrocks_read WHERE id > 0;
\echo
\echo bench_postgresrocks point reads
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM bench_postgresrocks_read
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM bench_postgresrocks_read
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM bench_postgresrocks_read
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
\echo
\echo bench_postgresrocks helper row count
SELECT count(*) FROM postgresrocks_read_data('bench_postgresrocks_read');
\echo
\echo bench_heap seq reads plan
EXPLAIN (ANALYZE, COSTS ON) SELECT count(*) FROM bench_heap_read WHERE id > 0;
\echo
\echo bench_postgresrocks seq reads plan
EXPLAIN (ANALYZE, COSTS ON) SELECT count(*) FROM bench_postgresrocks_read WHERE id > 0;
\echo
\echo bench_heap point reads plan
EXPLAIN (ANALYZE, COSTS ON)
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM bench_heap_read
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
\echo
\echo bench_postgresrocks point reads plan
EXPLAIN (ANALYZE, COSTS ON)
SELECT count(*)
FROM generate_series(1, $READ_POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT name FROM bench_postgresrocks_read
    WHERE id = ((g * 37) % $ROWS) + 1
) s;
SQL

run_sql "DROP TABLE IF EXISTS bench_heap"
run_sql "DROP TABLE IF EXISTS bench_heap_read"
