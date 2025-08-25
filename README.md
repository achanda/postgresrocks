# PostgresRocks

A PostgreSQL extension that implements a custom table access method using RocksDB as the storage engine.

## Storage Format

The extension uses a row-based storage format where complete rows are stored:

```
Row-Based Storage:
┌─────────────────────────┐
│ Row 1: [A, B, metadata] │
│ Row 2: [C, D, metadata] │
│ Row 3: [E, F, metadata] │
└─────────────────────────┘
```

### Key Structure

- **Row Data**: `row_<table_oid>_<row_id>` → [complete_serialized_row]
- **Metadata**: `meta_<table_oid>_info` → [row_count, column_count, column_types]

### Data Organization

1. **Complete Row Storage**: Each row is stored as a single, self-contained unit
2. **Binary Serialization**: Efficient binary format with row header and typed data
3. **Null Bitmap**: Compact null value representation
4. **Type-Aware Format**: Optimized serialization for INT4, INT8, TEXT, and VARCHAR
5. **Simple Key Schema**: Direct row lookup without complex mapping

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
-- Create a table using the postgresrocks storage engine
CREATE TABLE test_table (
    id SERIAL,
    name TEXT,
    value INTEGER
) USING postgresrocks;

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