# PostgresRocks

A PostgreSQL extension that implements a custom table access method using RocksDB as the storage engine with **columnar storage format**.

## Storage Format

### Columnar Architecture

The extension uses a **columnar storage format** where data is organized by columns rather than rows:

```
Columnar Storage:
┌─────────┬─────────┐
│ Col 1   │ Col 2   │
├─────────┼─────────┤
│ A, C    │ B, D    │
└─────────┴─────────┘
```

### Key Structure

- **Column Data**: `col_<table_oid>_<column_index>_<chunk_id>` → [column_values]
- **Metadata**: `meta_<table_oid>_info` → [row_count, column_count, chunk_size]
- **Row Mapping**: `row_<table_oid>_<row_id>` → [chunk_id, chunk_offset]

### Data Organization

1. **Chunked Storage**: Data is divided into chunks of 1000 rows for efficient access
2. **Column Compression**: Similar data types are stored together for better compression
3. **Null Bitmap**: Efficient null value representation using bitmaps
4. **Type-Aware Serialization**: Optimized binary format for each data type


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
make
sudo make install

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

- **Integers**: `INTEGER`, `BIGINT`
- **Text**: `TEXT`, `VARCHAR`

## Testing

After installation, test the extension:

```sql
-- Test basic functionality
CREATE TABLE test (id INT, name TEXT) USING postgresrocks;
INSERT INTO test VALUES (1, 'Hello'), (2, 'World');
SELECT * FROM test;

-- Test speculative insertion
CREATE TABLE test_unique (id INT UNIQUE, name TEXT) USING postgresrocks;
INSERT INTO test_unique VALUES (1, 'First') ON CONFLICT (id) DO NOTHING;
```
