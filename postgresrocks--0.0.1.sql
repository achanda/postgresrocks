CREATE OR REPLACE FUNCTION postgresrocks_tableam_handler(internal)
RETURNS table_am_handler AS 'postgresrocks', 'postgresrocks_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD postgresrocks TYPE TABLE HANDLER postgresrocks_tableam_handler;

-- Create a custom function to demonstrate SELECT functionality
CREATE OR REPLACE FUNCTION postgresrocks_read_data(table_name text)
RETURNS TABLE(id integer, name text) AS 'postgresrocks', 'postgresrocks_read_data'
LANGUAGE C STRICT;
