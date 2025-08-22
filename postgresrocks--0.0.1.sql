CREATE OR REPLACE FUNCTION postgresrocks_tableam_handler(internal)
RETURNS table_am_handler AS 'postgresrocks', 'postgresrocks_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD postgresrocks TYPE TABLE HANDLER postgresrocks_tableam_handler;
