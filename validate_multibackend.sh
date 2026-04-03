#!/bin/bash

set -euo pipefail

DB_NAME="${DB_NAME:-postgres}"
PSQL="${PSQL:-psql}"
ROWS="${ROWS:-1000}"
SCHEMA_PROFILE="${SCHEMA_PROFILE:-narrow}"
TABLE_NAME="${TABLE_NAME:-mbv_${SCHEMA_PROFILE}_$$}"
POINT_QUERIES="${POINT_QUERIES:-100}"

SCHEMA_SQL=""
INSERT_SELECT_SQL=""
POINT_ATTR_NAME="name"
POINT_ATTNUM="2"
ROLLBACK_INSERT_SQL=""
ROLLBACK_CHECK_ENABLED="1"

case "$SCHEMA_PROFILE" in
    narrow)
        SCHEMA_SQL="(id integer, name text)"
        INSERT_SELECT_SQL="SELECT g, 'value-' || g
FROM generate_series(1, $ROWS) AS g"
        POINT_ATTR_NAME="name"
        POINT_ATTNUM="2"
        ROLLBACK_INSERT_SQL="INSERT INTO $TABLE_NAME VALUES ($((ROWS + 1)), 'rolled-back');"
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
FROM generate_series(1, $ROWS) AS g"
        POINT_ATTR_NAME="name"
        POINT_ATTNUM="2"
        ROLLBACK_INSERT_SQL="INSERT INTO $TABLE_NAME VALUES (
            $((ROWS + 1)), 'rolled-back', 1, 17, 'tag', 'note', 1, 'city'
        );"
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
FROM generate_series(1, $ROWS) AS g"
        POINT_ATTR_NAME="name"
        POINT_ATTNUM="2"
        ROLLBACK_INSERT_SQL="INSERT INTO $TABLE_NAME VALUES (
            $((ROWS + 1)), 'rolled-back', 1, 17, 'tag', 'note', 1, 'city',
            'region', 2, 'comment', md5('rollback-wide') || md5('payload-wide'), 99, 3, '12345', 'code'
        );"
        ;;
    *)
        echo "Unsupported SCHEMA_PROFILE: $SCHEMA_PROFILE" >&2
        exit 1
        ;;
esac

echo "multi-backend validation"
echo "database: $DB_NAME"
echo "table: $TABLE_NAME"
echo "rows: $ROWS"
echo "schema profile: $SCHEMA_PROFILE"
echo "point queries: $POINT_QUERIES"

setup_sql_file="$(mktemp)"
cat > "$setup_sql_file" <<SQL
LOAD 'postgresrocks';
SET postgresrocks.enable_shared_writer = off;
CREATE TABLE $TABLE_NAME $SCHEMA_SQL USING postgresrocks;
INSERT INTO $TABLE_NAME
$INSERT_SELECT_SQL;
$(if [ "$ROLLBACK_CHECK_ENABLED" = "1" ]; then cat <<EOS
BEGIN;
$ROLLBACK_INSERT_SQL
ROLLBACK;
EOS
fi)
CREATE INDEX ${TABLE_NAME}_id_idx ON $TABLE_NAME(id);
ANALYZE $TABLE_NAME;
SELECT postgresrocks_force_flush();
SQL
"$PSQL" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$setup_sql_file" >/dev/null
rm -f "$setup_sql_file"

seq_count="$("$PSQL" -d "$DB_NAME" -At -v ON_ERROR_STOP=1 -c "
SET enable_indexscan=off;
SET enable_indexonlyscan=off;
SELECT count(*) FROM $TABLE_NAME WHERE id > 0;
" | tail -n 1)"

point_count="$("$PSQL" -d "$DB_NAME" -At -v ON_ERROR_STOP=1 -c "
SELECT count(*)
FROM generate_series(1, $POINT_QUERIES) AS g
CROSS JOIN LATERAL (
    SELECT $POINT_ATTR_NAME FROM $TABLE_NAME WHERE id = g
) s;
")"

helper_count="$("$PSQL" -d "$DB_NAME" -At -v ON_ERROR_STOP=1 -c "
SELECT postgresrocks_count_rows('$TABLE_NAME');
")"

rollback_count="$("$PSQL" -d "$DB_NAME" -At -v ON_ERROR_STOP=1 -c "
SELECT count(*) FROM $TABLE_NAME WHERE id = $((ROWS + 1));
")" 

helper_lookup_count="$("$PSQL" -d "$DB_NAME" -At -v ON_ERROR_STOP=1 -c "
SELECT count(*)
FROM generate_series(1, $POINT_QUERIES) AS g
WHERE postgresrocks_lookup_text('$TABLE_NAME', g, $POINT_ATTNUM) IS NOT NULL;
")"

echo "seq read count: $seq_count"
echo "indexed point count: $point_count"
echo "metadata row count: $helper_count"
echo "direct helper lookup count: $helper_lookup_count"
echo "rolled-back row visible count: $rollback_count"

if [ "$seq_count" != "$ROWS" ]; then
    echo "FAIL: expected seq read count $ROWS, got $seq_count" >&2
    exit 1
fi

if [ "$point_count" != "$POINT_QUERIES" ]; then
    echo "FAIL: expected indexed point count $POINT_QUERIES, got $point_count" >&2
    exit 1
fi

if [ "$helper_lookup_count" != "$POINT_QUERIES" ]; then
    echo "FAIL: expected helper lookup count $POINT_QUERIES, got $helper_lookup_count" >&2
    exit 1
fi

if [ "$helper_count" != "$ROWS" ]; then
    echo "FAIL: expected metadata row count $ROWS, got $helper_count" >&2
    exit 1
fi

if [ "$rollback_count" != "0" ]; then
    echo "FAIL: expected rollback visibility count 0, got $rollback_count" >&2
    exit 1
fi

echo "PASS"
