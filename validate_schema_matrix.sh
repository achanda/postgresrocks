#!/bin/bash

set -euo pipefail

SCHEMA_PROFILES="${SCHEMA_PROFILES:-narrow medium wide}"
ROWS_LIST="${ROWS_LIST:-1000 5000}"
POINT_QUERIES="${POINT_QUERIES:-100}"
DB_NAME="${DB_NAME:-postgres}"
PSQL="${PSQL:-psql}"

echo "validation matrix"
echo "database: $DB_NAME"
echo "schema profiles: $SCHEMA_PROFILES"
echo "rows list: $ROWS_LIST"
echo "point queries: $POINT_QUERIES"

for profile in $SCHEMA_PROFILES; do
    for rows in $ROWS_LIST; do
        echo
        echo "=== validate profile=$profile rows=$rows ==="
        DB_NAME="$DB_NAME" \
        PSQL="$PSQL" \
        SCHEMA_PROFILE="$profile" \
        ROWS="$rows" \
        POINT_QUERIES="$POINT_QUERIES" \
        ./validate_multibackend.sh
    done
done

