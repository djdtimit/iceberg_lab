# REST catalog: 404 from /v1/config is ignored, literal "{s}" sent as warehouse id

**Component:** pg_lake_iceberg, REST catalog with OAuth2
**Versions:** pg_lake 3.6.0; catalog Lakekeeper with OpenFGA authorization - found with v0.14.3,
reproduced again (2026-11-16) with v0.15.0

## Summary

When the catalog answers `GET /v1/config?warehouse=<name>` with 404 (here: the user has no
permission, so Lakekeeper hides the warehouse with `NoSuchWarehouseException`), pg_lake
continues and builds a request with the literal placeholder `{s}` as warehouse/prefix. The
user sees `HTTP request failed (HTTP 400) DETAIL: Provided WarehouseId is not a valid UUID.
Got: {s}` instead of the actual reason.

## Steps to reproduce

1. REST server (`CREATE SERVER ... TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog OPTIONS (..., warehouse '<name>', oauth_endpoint '...', scope 'openid')`) with credentials of a user that may not see the warehouse.
2. `CREATE TABLE x () USING iceberg WITH (catalog = '<server>', read_only = true, catalog_namespace = 'ns', catalog_table_name = 't')` and select from it.

## Expected

An error that reports the catalog's answer (404 / warehouse not found or not permitted).

## Actual

`Provided WarehouseId is not a valid UUID. Got: {s}` (`WarehouseIdIsNotUUID`).
