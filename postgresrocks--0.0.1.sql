CREATE OR REPLACE FUNCTION postgresrocks_tableam_handler(internal)
RETURNS table_am_handler AS 'postgresrocks', 'postgresrocks_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD postgresrocks TYPE TABLE HANDLER postgresrocks_tableam_handler;

-- Create a custom function to demonstrate SELECT functionality
CREATE OR REPLACE FUNCTION postgresrocks_read_data(table_name text)
RETURNS TABLE(id integer, name text) AS 'postgresrocks', 'postgresrocks_read_data'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION postgresrocks_count_rows(table_name text)
RETURNS bigint AS 'postgresrocks', 'postgresrocks_count_rows'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION postgresrocks_lookup_text(table_name text, rowid bigint, attnum integer)
RETURNS text AS 'postgresrocks', 'postgresrocks_lookup_text'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION postgresrocks_reset_read_cache()
RETURNS void AS 'postgresrocks', 'postgresrocks_reset_read_cache'
LANGUAGE C;

CREATE OR REPLACE FUNCTION postgresrocks_handle_sql_drop()
RETURNS event_trigger AS 'postgresrocks', 'postgresrocks_handle_sql_drop'
LANGUAGE C;

CREATE EVENT TRIGGER postgresrocks_sql_drop_cleanup
ON sql_drop
EXECUTE FUNCTION postgresrocks_handle_sql_drop();

CREATE OR REPLACE FUNCTION postgresrocks_set_table_layout(table_name regclass, layout text)
RETURNS text AS 'postgresrocks', 'postgresrocks_set_table_layout'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION postgresrocks_get_table_layout(table_name regclass)
RETURNS text AS 'postgresrocks', 'postgresrocks_get_table_layout'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION postgresrocks_reset_insert_stats()
RETURNS void AS 'postgresrocks', 'postgresrocks_reset_insert_stats'
LANGUAGE C;

CREATE OR REPLACE FUNCTION postgresrocks_insert_stats(
    OUT tuple_insert_calls bigint,
    OUT multi_insert_calls bigint,
    OUT multi_inserted_tuples bigint
) AS 'postgresrocks', 'postgresrocks_insert_stats'
LANGUAGE C;

CREATE OR REPLACE FUNCTION postgresrocks_force_flush()
RETURNS void AS 'postgresrocks', 'postgresrocks_force_flush'
LANGUAGE C;

CREATE OR REPLACE FUNCTION postgresrocks_direct_write_stats(
    OUT serialize_calls bigint,
    OUT serialize_us bigint,
    OUT batch_flush_calls bigint,
    OUT batch_flush_rows bigint,
    OUT batch_flush_us bigint,
    OUT metadata_flush_calls bigint,
    OUT metadata_flush_us bigint,
    OUT rowid_calls bigint,
    OUT rowid_us bigint,
    OUT batch_put_calls bigint,
    OUT batch_put_us bigint,
    OUT tuple_insert_us bigint,
    OUT init_rocksdb_calls bigint,
    OUT init_rocksdb_us bigint,
    OUT precommit_flush_us bigint,
    OUT init_lockfile_open_us bigint,
    OUT init_flock_us bigint,
    OUT init_db_open_us bigint,
    OUT init_upgrade_reopen_calls bigint,
    OUT init_upgrade_reopen_us bigint
) AS 'postgresrocks', 'postgresrocks_direct_write_stats'
LANGUAGE C;
