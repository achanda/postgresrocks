#!/bin/bash

set -euo pipefail

ROWS_LIST="${ROWS_LIST:-50000 1000000}"
SCHEMA_PROFILES="${SCHEMA_PROFILES:-narrow medium wide}"
RUNS="${RUNS:-3}"
READ_POINT_QUERIES="${READ_POINT_QUERIES:-1000}"
OUTPUT_DIR="${OUTPUT_DIR:-benchmark_results}"

mkdir -p "$OUTPUT_DIR"

echo "benchmark matrix"
echo "rows list: $ROWS_LIST"
echo "schema profiles: $SCHEMA_PROFILES"
echo "runs: $RUNS"
echo "point lookups: $READ_POINT_QUERIES"
echo "output dir: $OUTPUT_DIR"

for profile in $SCHEMA_PROFILES; do
    for rows in $ROWS_LIST; do
        output_file="$OUTPUT_DIR/benchmark_${profile}_${rows}.txt"
        echo
        echo "=== profile=$profile rows=$rows ==="
        echo "saving output to: $output_file"
        SCHEMA_PROFILE="$profile" ROWS="$rows" RUNS="$RUNS" READ_POINT_QUERIES="$READ_POINT_QUERIES" ./benchmark_insert.sh | tee "$output_file"
    done
done
