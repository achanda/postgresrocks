-- Test script for PostgresRocks RocksDB Table Access Method
DROP EXTENSION IF EXISTS postgresrocks CASCADE;
CREATE EXTENSION postgresrocks;

-- Create a test table using our RocksDB storage engine
CREATE TABLE test_table(id INT, name TEXT) USING postgresrocks;

-- Test basic insertion
INSERT INTO test_table VALUES (1, 'Hello');
INSERT INTO test_table VALUES (2, 'World');
INSERT INTO test_table VALUES (3, 'RocksDB');

-- Test basic selection
SELECT * FROM test_table;

-- Test more complex queries
SELECT * FROM test_table WHERE id > 1;
SELECT name FROM test_table ORDER BY id;

-- Test with different data types
CREATE TABLE test_numbers(small_num INT, big_num BIGINT, description TEXT) USING postgresrocks;
INSERT INTO test_numbers VALUES (42, 9223372036854775807, 'Max bigint');
INSERT INTO test_numbers VALUES (-1, -9223372036854775808, 'Min bigint');
INSERT INTO test_numbers VALUES (0, 0, 'Zero');

SELECT * FROM test_numbers ORDER BY small_num;

-- Test that data persists (this would need a restart to fully verify)
\echo 'Testing data persistence...'
SELECT COUNT(*) as total_rows FROM test_table;
SELECT COUNT(*) as number_rows FROM test_numbers;

-- Test size estimation functions (these will now work with our implementation)
\echo 'Testing size estimation...'
SELECT pg_relation_size('test_table') as table_size_bytes;
SELECT pg_relation_size('test_numbers') as numbers_size_bytes;

-- Test with EXPLAIN to see if planner uses our size estimates
\echo 'Testing query planning with size estimates...'
EXPLAIN (COSTS ON) SELECT * FROM test_table;
EXPLAIN (COSTS ON) SELECT * FROM test_numbers WHERE small_num > 0;

-- Test speculative insertion (this would be used by INSERT ... ON CONFLICT)
\echo 'Testing speculative insertion capabilities...'
CREATE TABLE test_unique(id INT UNIQUE, value TEXT) USING postgresrocks;
INSERT INTO test_unique VALUES (1, 'first');
INSERT INTO test_unique VALUES (2, 'second');

-- This should work normally
SELECT * FROM test_unique ORDER BY id;
