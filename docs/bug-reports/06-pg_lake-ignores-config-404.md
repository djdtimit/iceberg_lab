# REST catalog: 404 from /v1/config is ignored, the next request goes to a wrong prefix

**Component:** pg_lake_iceberg, REST catalog with OAuth2
**Versions:** pg_lake 3.6.0 and 3.7 (extension 3.7, release v3.7.0, Postgres 19); catalog
Lakekeeper with OpenFGA authorization - found with v0.14.3, reproduced with v0.15.0 and
(2026-11-23) v0.15.1 on a freshly created authorization stack

## Summary

When the catalog answers `GET /v1/config?warehouse=<name>` with 404 (here: the user has no
permission, so Lakekeeper hides the warehouse with `NoSuchWarehouseException`), pg_lake does not
report that answer. It carries on and sends its next request with a prefix that is not the
warehouse id. Lakekeeper refuses that path with 400, and the user sees
`could not look up namespace "..." in the REST catalog: Provided WarehouseId is not a valid UUID.
Got: {s}` (`WarehouseIdIsNotUUID`) instead of the actual reason.

The literal `{s}` is Lakekeeper's own message: it answers every non-UUID prefix with exactly this
text (checked: `GET /catalog/v1/demo-sts/namespaces` and `GET /catalog/v1/hello/namespaces` both
return `Got: {s}`) - a separate, minor Lakekeeper issue. An earlier version of this report
blamed pg_lake for sending `{s}`; that was wrong.

## Steps to reproduce

1. REST server (`CREATE SERVER ... TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog OPTIONS (..., warehouse '<name>', oauth_endpoint '...', scope 'openid')`) with credentials of a user that has no permission on the warehouse.
2. `CREATE TABLE x () USING iceberg WITH (catalog = '<server>', read_only = true, catalog_namespace = 'ns', catalog_table_name = 't')` and select from it.

## Expected

An error that reports the catalog's answer to `/v1/config` (404 / warehouse not found or not
permitted).

## Actual

`Provided WarehouseId is not a valid UUID. Got: {s}` (`WarehouseIdIsNotUUID`, HTTP 400). Nothing
is read - the refusal holds - only the reason is lost.

## Note on an earlier "no longer reproduces"

From 2026-11-20 the lab showed the refusal correctly. That came from leftover state: the
long-running authorization stack still gave the test user access to the warehouse, so
`/v1/config` answered 200. On a freshly created stack (2026-11-23) the behaviour is back.
