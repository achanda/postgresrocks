#!/bin/bash

set -euo pipefail

DB_NAME="${DB_NAME:-postgres}"
ROWS="${ROWS:-50000}"
RUNS="${RUNS:-3}"
PSQL="${PSQL:-psql}"
READ_POINT_QUERIES="${READ_POINT_QUERIES:-1000}"

if ! command -v "$PSQL" >/dev/null 2>&1; then
    echo "psql not found: $PSQL" >&2
    exit 1
fi

now_seconds() {
    perl -MTime::HiRes=time -e 'printf "%.6f\n", time'
}

elapsed_ms() {
    perl -e 'printf "%.3f", ($ARGV[1] - $ARGV[0]) * 1000' "$1" "$2"
}

run_sql() {
    "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -q -c "$1" >/dev/null
}

run_sql_pgrocks() {
    "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -q -c "LOAD 'postgresrocks'; SET postgresrocks.benchmark_write_mode = on; $1" >/dev/null
}

measure_insert() {
    local table_name="$1"
    local using_clause="$2"
    local setup_sql="$3"
    local total_ms="0"
    local run

    echo
    echo "$table_name inserts"

    for run in $(seq 1 "$RUNS"); do
        local result duration stats

        result="$("$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -At -F '|' <<SQL
$setup_sql
DROP TABLE IF EXISTS $table_name;
CREATE TABLE $table_name (id integer, name text) $using_clause;
\echo BENCH_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
INSERT INTO $table_name
SELECT g, 'value-' || g
FROM generate_series(1, $ROWS) AS g;
\echo BENCH_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
SQL
)"

        duration="$(printf '%s\n' "$result" | perl -e '
            my @lines = <STDIN>;
            chomp @lines;
            my ($start, $end);
            for (my $i = 0; $i < @lines; $i++) {
                $start = $lines[$i + 1] if $lines[$i] eq "BENCH_START";
                $end = $lines[$i + 1] if $lines[$i] eq "BENCH_END";
            }
            printf "%.3f", ($end - $start) * 1000;
        ')"
        total_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$total_ms" "$duration")"

        echo "  run $run: ${duration} ms"
    done

    perl -e 'printf "  avg: %.3f ms\n", $ARGV[0] / $ARGV[1]' "$total_ms" "$RUNS"
}

echo "Database: $DB_NAME"
echo "Rows per run: $ROWS"
echo "Runs: $RUNS"
echo "Point lookups per read run: $READ_POINT_QUERIES"

run_sql "DROP EXTENSION IF EXISTS postgresrocks CASCADE"
run_sql "CREATE EXTENSION postgresrocks"

measure_insert "bench_heap" "" ""
measure_insert "bench_postgresrocks" "USING postgresrocks" "LOAD 'postgresrocks'; SET postgresrocks.benchmark_write_mode = on; SELECT postgresrocks_reset_insert_stats();"

prepare_read_tables() {
    run_sql "DROP TABLE IF EXISTS bench_heap_read"
    run_sql "DROP TABLE IF EXISTS bench_postgresrocks_read"
    run_sql "CREATE TABLE bench_heap_read (id integer, name text)"
    run_sql_pgrocks "CREATE TABLE bench_postgresrocks_read (id integer, name text) USING postgresrocks"

    run_sql "INSERT INTO bench_heap_read
             SELECT g, 'value-' || g
             FROM generate_series(1, $ROWS) AS g"
    run_sql_pgrocks "INSERT INTO bench_postgresrocks_read
                     SELECT g, 'value-' || g
                     FROM generate_series(1, $ROWS) AS g"
}

measure_read() {
    local label="$1"
    local table_name="$2"
    local setup_sql="$3"
    local query_sql="$4"
    local total_ms="0"
    local run

    echo
    echo "$label"

    for run in $(seq 1 "$RUNS"); do
        local result duration

        result="$("$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" -At -F '|' <<SQL
$setup_sql
\echo BENCH_START
SELECT EXTRACT(EPOCH FROM clock_timestamp());
$query_sql
\echo BENCH_END
SELECT EXTRACT(EPOCH FROM clock_timestamp());
SQL
)"

        duration="$(printf '%s\n' "$result" | perl -e '
            my @lines = <STDIN>;
            chomp @lines;
            my ($start, $end);
            for (my $i = 0; $i < @lines; $i++) {
                $start = $lines[$i + 1] if $lines[$i] eq "BENCH_START";
                $end = $lines[$i + 1] if $lines[$i] eq "BENCH_END";
            }
            printf "%.3f", ($end - $start) * 1000;
        ')"

        total_ms="$(perl -e 'printf "%.3f", $ARGV[0] + $ARGV[1]' "$total_ms" "$duration")"
        echo "  run $run: ${duration} ms"
    done

    perl -e 'printf "  avg: %.3f ms\n", $ARGV[0] / $ARGV[1]' "$total_ms" "$RUNS"
}

print_explain() {
    local label="$1"
    local setup_sql="$2"
    local query_sql="$3"

    echo
    echo "$label"
    "$PSQL" -v ON_ERROR_STOP=1 -d "$DB_NAME" <<SQL
$setup_sql
EXPLAIN (ANALYZE, COSTS ON) $query_sql
SQL
}

prepare_read_tables

measure_read "bench_heap seq reads" "bench_heap_read" "" "SELECT count(*) FROM bench_heap_read WHERE id > 0;"
measure_read "bench_postgresrocks seq reads" "bench_postgresrocks_read" "LOAD 'postgresrocks';" "SELECT count(*) FROM bench_postgresrocks_read WHERE id > 0;"

measure_read "bench_heap point reads" "bench_heap_read" "" "SELECT count(*) FROM generate_series(1, $READ_POINT_QUERIES) AS g CROSS JOIN LATERAL (SELECT name FROM bench_heap_read WHERE id = ((g * 37) % $ROWS) + 1) s;"
measure_read "bench_postgresrocks point reads" "bench_postgresrocks_read" "LOAD 'postgresrocks';" "SELECT count(*) FROM generate_series(1, $READ_POINT_QUERIES) AS g CROSS JOIN LATERAL (SELECT name FROM bench_postgresrocks_read WHERE id = ((g * 37) % $ROWS) + 1) s;"

print_explain "bench_heap seq reads plan" "" "SELECT count(*) FROM bench_heap_read WHERE id > 0;"
print_explain "bench_postgresrocks seq reads plan" "LOAD 'postgresrocks';" "SELECT count(*) FROM bench_postgresrocks_read WHERE id > 0;"
print_explain "bench_heap point reads plan" "" "SELECT count(*) FROM generate_series(1, $READ_POINT_QUERIES) AS g CROSS JOIN LATERAL (SELECT name FROM bench_heap_read WHERE id = ((g * 37) % $ROWS) + 1) s;"
print_explain "bench_postgresrocks point reads plan" "LOAD 'postgresrocks';" "SELECT count(*) FROM generate_series(1, $READ_POINT_QUERIES) AS g CROSS JOIN LATERAL (SELECT name FROM bench_postgresrocks_read WHERE id = ((g * 37) % $ROWS) + 1) s;"

run_sql "DROP TABLE IF EXISTS bench_heap"
run_sql "DROP TABLE IF EXISTS bench_postgresrocks"
run_sql "DROP TABLE IF EXISTS bench_heap_read"
run_sql "DROP TABLE IF EXISTS bench_postgresrocks_read"
