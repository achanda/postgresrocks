CREATE OR REPLACE FUNCTION postgresrocks_tableam_handler(internal)
RETURNS table_am_handler AS 'postgresrocks', 'postgresrocks_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD postgresrocks TYPE TABLE HANDLER postgresrocks_tableam_handler;

-- Create a custom function to demonstrate SELECT functionality
CREATE OR REPLACE FUNCTION postgresrocks_read_data(table_name text)
RETURNS TABLE(id integer, name text) AS 'postgresrocks', 'postgresrocks_read_data'
LANGUAGE C STRICT;

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
