-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION qdrant_fdw" to load this file. \quit

CREATE FUNCTION qdrant_fdw_handler()
    RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER qdrant_fdw
  HANDLER qdrant_fdw_handler;

CREATE FUNCTION sim(varchar, varchar)
    returns float as 'MODULE_PATHNAME' LANGUAGE C;