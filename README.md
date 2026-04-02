# PostgresRocks

A PostgreSQL extension that implements a custom table access method using RocksDB as the storage engine.

## Storage Format

The extension currently uses a RocksDB-backed row store where each PostgreSQL row is serialized as one RocksDB value.

### RocksDB Key Layout

- **Row data**: `[tag=1][table_oid_be][rowid_be]` -> complete serialized row
- **Metadata**: `meta_<oid>_info` -> table metadata

The row key is fixed-width binary:

- `1 byte` row tag
- `4 bytes` table OID in big-endian order
- `8 bytes` row ID in big-endian order

This gives:

- stable key size
- ordered row keys per table
- efficient prefix iteration for table scans

### Table Metadata

Each table stores one metadata record:

- `row_count`
- `column_count`
- `layout` hint (`row`, `hybrid`, or `column`)
- column type OIDs

### Row Storage

Complete rows are stored together:

```
Row-Based Storage:
┌─────────────────────────┐
│ Row 1: [A, B, metadata] │
│ Row 2: [C, D, metadata] │
│ Row 3: [E, F, metadata] │
└─────────────────────────┘
```

### Serialized Row Format

Each value is a binary blob with:

- **Row header**: number of attributes and total row length
- **Null bitmap**: one boolean per attribute
- **Attribute payloads**: encoded in column order

Supported payload encodings today:

- `INT4`: 4-byte integer
- `INT8`: 8-byte integer
- `TEXT` / `VARCHAR`: 4-byte length followed by raw bytes

### Data Organization

1. **Complete row storage**: each row is stored as one self-contained RocksDB value
2. **Binary serialization**: rows are encoded in a compact typed binary format
3. **Ordered keys**: row keys are sortable and grouped by table
4. **Prefix scans**: table scans can iterate over one table’s row-key prefix
5. **Simple metadata record**: table-level information is stored separately from row data

### Row Format

Each serialized row contains:
```
┌─────────────┬─────────────┬─────────────────┐
│ Row Header  │ Null Bitmap │ Attribute Data  │
│ (metadata)  │ (per attr)  │ (typed values)  │
└─────────────┴─────────────┴─────────────────┘
```

## Prerequisites

### macOS
- Homebrew
- RocksDB: `brew install rocksdb`
- PostgreSQL 17: `brew install postgresql@17`

### Linux
- RocksDB development libraries
- PostgreSQL 17 development headers

## Build and Install

### Automated Build (Recommended)

```bash
chmod +x build.sh
./build.sh
```

The script will:
1. Check and install dependencies
2. Build the extension
3. Install it to PostgreSQL
4. Provide testing instructions

### Manual Build

```bash
# Set PostgreSQL paths
export PG_CONFIG=/opt/homebrew/bin/pg_config
export MACOSX_DEPLOYMENT_TARGET=11.0

# Build and install
make clean
make
make install

# Create extension in PostgreSQL
psql -d postgres -c "CREATE EXTENSION postgresrocks;"
```

## Usage

### Create Tables

```sql
-- Optional: choose the default layout hint for new tables in this session
SET postgresrocks.default_layout = 'hybrid';

-- Create a table using the postgresrocks storage engine
CREATE TABLE test_table (
    id SERIAL,
    name TEXT,
    value INTEGER
) USING postgresrocks;

-- Inspect or override the persisted layout hint later
SELECT postgresrocks_get_table_layout('test_table');
SELECT postgresrocks_set_table_layout('test_table', 'row');

-- Insert data
INSERT INTO test_table (name, value) VALUES 
    ('Alice', 100),
    ('Bob', 200),
    ('Charlie', 300);

-- Query data
SELECT * FROM test_table WHERE value > 150;
```

### Supported Data Types

- **Integers**: `INTEGER` (INT4), `BIGINT` (INT8)
- **Text**: `TEXT`, `VARCHAR`
- **Constraints**: `UNIQUE` constraints with speculative insertion support

### Layout Hints

The extension now accepts a user-provided layout hint for each table:

- `row`: best fit for point reads and whole-row OLTP access
- `hybrid`: intended for mixed workloads
- `column`: intended for scan-heavy analytics

Today, PostgresRocks persists and exposes the hint, but still executes through the row-store code path while hybrid/column storage is being built out.

### Current Limitations

- Reads and scans still use a row-oriented execution model
- There is no true columnar or hybrid execution path yet
- Multi-backend coordination around RocksDB access is still limited
- Read/query performance is not yet as integrated with PostgreSQL as the native heap AM

## Testing

After installation, run the comprehensive test suite:

```bash
# Run all tests
psql postgres -f test.sql
```

Or test manually:

```sql
-- Test basic functionality
CREATE TABLE test (id INT, name TEXT) USING postgresrocks;
INSERT INTO test VALUES (1, 'Hello'), (2, 'World'), (3, 'RocksDB');
SELECT * FROM test;
SELECT * FROM test WHERE id > 1;
SELECT name FROM test;

-- Test data persistence
SELECT postgresrocks_read_data('test');

-- Test speculative insertion (used by ON CONFLICT)
CREATE TABLE test_unique (id INT UNIQUE, value TEXT) USING postgresrocks;
INSERT INTO test_unique VALUES (1, 'first');
INSERT INTO test_unique VALUES (2, 'second');
SELECT * FROM test_unique;
```
