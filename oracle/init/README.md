Oracle initialization notes

- Place DDL files in oracle/sql/init.
- Files are executed automatically by the Oracle XE entrypoint on first startup.
- Use numeric prefixes for deterministic ordering, for example 01_create_source_tables.sql.
