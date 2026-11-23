# [pg_lake] REST catalogs without a `prefix` cannot be used: pg_lake always inserts one

**Component:** pg_lake `iceberg_catalog` foreign data wrapper, server type `rest`
**Version:** pg_lake 3.7.0 (Postgres 19)

## Summary

pg_lake builds every REST path as `<rest_endpoint>/v1/<prefix>/namespaces/...`. The `prefix` is
optional in the Iceberg REST spec: a catalog whose `/v1/config` returns no `prefix` (in
`overrides` or `defaults`) serves `/v1/namespaces/...`. pg_lake then inserts the catalog name
(by default the database name) as prefix, and every request goes to a path the catalog does not
have:

```
could not look up namespace "postgres.ns" in the REST catalog:
No route for request: GET v1/postgres/namespaces/ns
```

There is no option to leave the prefix out: `catalog_name` must not be empty
(`invalid value for "catalog_name": must not be empty`), and there is no `prefix` option
(valid options: rest_endpoint, rest_auth_type, oauth_endpoint, scope, enable_vended_credentials,
location_prefix, catalog_name, warehouse).

Found with the Apache Iceberg REST fixture (`apache/iceberg-rest-fixture`), whose `/v1/config`
answers `{"defaults": {}, "overrides": {"namespace-separator": "%2E"}, ...}` - no prefix.
PyIceberg, DuckDB, Polars, Daft, Spark, Trino and Flink work with it.

## Steps to reproduce

```sql
CREATE SERVER ref TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
  OPTIONS (rest_endpoint 'http://reference:8181', warehouse 'reference',
           rest_auth_type 'none', location_prefix 's3://reference/wh/pg_lake');
-- any table on this server, e.g. CREATE TABLE ... USING iceberg WITH (catalog = 'ref', ...)
```

## Expected

Without a `prefix` in `/v1/config`, pg_lake uses `/v1/namespaces/...` (as the spec and the other
clients do).

## Notes

- Lab: every pg_lake test with `CATALOG=reference` (central rule `pglake-requires-rest-prefix`
  in `docs/gaps.toml`), e.g. `tests/engines/test_interop.py::test_write_then_read[pyiceberg-pg_lake]`.
- Related to 06: pg_lake also carries on with a wrong prefix when `/v1/config` answers 404.
