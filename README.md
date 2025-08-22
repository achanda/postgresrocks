# PostgresRocks - RocksDB-Backed PostgreSQL Table Access Method

A complete PostgreSQL table access method extension that uses RocksDB as the storage engine. This allows PostgreSQL tables to be stored in RocksDB's key-value format while maintaining full SQL compatibility.

## Storage Format

- **Row Keys**: `table_<oid>_<rowid>` - stores serialized tuple data
- **Metadata Keys**: `meta_<oid>_rowcount` - tracks row count for ID generation
- **Binary Serialization**: Tuples are serialized with type information and null flags

## Prerequisites

### macOS (Recommended)

1. **Install PostgreSQL and RocksDB**:
   ```bash
   # Install PostgreSQL (includes development headers)
   brew install postgresql
   
   # Install RocksDB
   brew install rocksdb
   
   # Start PostgreSQL service
   brew services start postgresql
   ```

### Linux

1. **PostgreSQL Development Headers**: 
   ```bash
   # Ubuntu/Debian
   sudo apt-get install postgresql-server-dev-all
   ```

2. **RocksDB Library**:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install librocksdb-dev
   
   # From source
   git clone https://github.com/facebook/rocksdb.git
   cd rocksdb
   make shared_lib
   sudo make install-shared
   ```

## Building and Installing

### Quick Start (Automated)

Use the provided build script for automatic dependency checking and installation:

```bash
# Build and install (checks dependencies automatically)
./build.sh install

# Or just build
./build.sh
```

### Manual Build

1. **Build the extension**:
   ```bash
   make clean
   make
   ```

2. **Install the extension**:
   ```bash
   # macOS (Homebrew PostgreSQL usually doesn't need sudo)
   make install
   
   # Linux or system PostgreSQL
   sudo make install
   ```

## Usage

1. **Create the extension** in your database:
   ```sql
   CREATE EXTENSION postgresrocks;
   ```

2. **Create tables** using the RocksDB storage engine:
   ```sql
   CREATE TABLE my_table(id INT, name TEXT, value BIGINT) USING postgresrocks;
   ```

3. **Use standard SQL operations**:
   ```sql
   -- Insert data
   INSERT INTO my_table VALUES (1, 'Hello', 1000);
   INSERT INTO my_table VALUES (2, 'World', 2000);
   
   -- Query data
   SELECT * FROM my_table;
   SELECT * FROM my_table WHERE id > 1;
   SELECT name FROM my_table ORDER BY id;
   
   -- Delete data
   DELETE FROM my_table WHERE id = 1;
   
   -- Truncate table
   TRUNCATE TABLE my_table;
   
   -- Check table size
   SELECT pg_relation_size('my_table') as size_bytes;
   
   -- See query planning with size estimates
   EXPLAIN (COSTS ON) SELECT * FROM my_table;
   
   -- Test speculative insertion support
   CREATE TABLE unique_table(id INT UNIQUE, name TEXT) USING postgresrocks;
   INSERT INTO unique_table VALUES (1, 'test');
   ```

## Supported Data Types

Currently supported PostgreSQL data types:
- `INT` (4-byte integer)
- `BIGINT` (8-byte integer)  
- `TEXT` (variable-length text)
- `VARCHAR(n)` (variable-length character string)

## Testing

Run the comprehensive test suite:
```bash
psql -d your_database -f test.sql
```

## Architecture

The table access method implements all required PostgreSQL storage callbacks:

1. **Scan Operations**: `rocks_beginscan`, `rocks_getnextslot`, `rocks_endscan`
2. **Modification Operations**: `rocks_tuple_insert`, `rocks_tuple_delete`
3. **Speculative Operations**: `rocks_tuple_insert_speculative`, `rocks_tuple_complete_speculative`
4. **Relation Management**: `rocks_relation_set_new_filelocator`, `rocks_relation_nontransactional_truncate`
5. **Utility Operations**: Serialization, key generation, RocksDB management

## References

- [Phil Eaton's PostgreSQL Table Access Methods Tutorial](https://notes.eatonphil.com/2023-11-01-postgres-table-access-methods.html)
- [PostgreSQL Table Access Method Documentation](https://www.postgresql.org/docs/current/tableam.html)
- [RocksDB C API Documentation](https://github.com/facebook/rocksdb/wiki/RocksDB-C-API)
