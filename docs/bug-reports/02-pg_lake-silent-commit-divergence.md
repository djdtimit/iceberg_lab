# COMMIT succeeds although the REST catalog refused the commit (Postgres and catalog diverge)

**Component:** pg_lake_iceberg, REST catalog tables
**Versions:** pg_lake 3.6.0 on Postgres 18.7; found with catalog Lakekeeper v0.14.3, reproduced again
(2026-11-16) with Lakekeeper v0.15.0 and Apache Polaris 1.9.0; second part with Unity Catalog OSS 0.7.0

## Summary

When the REST catalog answers the table commit with an error (here HTTP 503 on
`POST /v1/{prefix}/transactions/commit`), pg_lake still reports `COMMIT` as successful. The
new rows are visible in Postgres, but not in the catalog - so no other engine sees them.
pg_lake does not retry later and logs nothing.

## Steps to reproduce

1. Create two pg_lake tables in a REST catalog (`CREATE TABLE ... USING iceberg WITH (catalog = '<rest server>')`).
2. Put a proxy in front of the catalog that answers every `/transactions/commit` request
   mentioning table B with HTTP 503 (body `{"error": {"message": "...", "type": "ServiceUnavailable", "code": 503}}`).
3. `BEGIN; INSERT INTO a VALUES (1, 'a', 1.0); INSERT INTO b VALUES (2, 'b', 2.0); COMMIT;`

## Expected

`COMMIT` fails with an error, and neither table changes (in Postgres and in the catalog).

## Actual

- `COMMIT` returns without error.
- `SELECT count(*)` in Postgres: 1 row in A, 1 row in B.
- The catalog (read with PyIceberg or any other engine): 0 rows in A and B.
- pg_lake sent the commit 3 times (all refused), then gave up silently; no WARNING or ERROR
  in the Postgres log; after the proxy is transparent again nothing is re-sent.

## Same root cause without any fault injection: catalogs without `/transactions/commit`

pg_lake commits **every** write (a single-table INSERT too) through
`POST /v1/{prefix}/transactions/commit`. Unity Catalog OSS 0.7.0 does not implement that
endpoint and does not advertise it in `/v1/config`; it answers 404. pg_lake ignores the 404:

- `INSERT INTO t VALUES (1), (2)` returns without error, `SELECT count(*)` in Postgres says 2.
- The catalog keeps the table's first metadata file with no snapshot - no other engine ever
  sees the rows.
- Afterwards `DROP SCHEMA ... CASCADE` fails with
  `duplicate key value violates unique constraint "deletion_queue_pkey"` (the same metadata
  path is queued for deletion twice).

Captured request sequence for the INSERT (proxy between pg_lake and Unity):

```
GET  /api/2.1/unity-catalog/iceberg/v1/catalogs/lab/namespaces/<ns>/tables/t      200
POST /api/2.1/unity-catalog/iceberg/v1/catalogs/lab/transactions/commit           404
```

Expected: pg_lake either uses the single-table commit endpoint
(`POST /v1/{prefix}/namespaces/{ns}/tables/{table}`) when `/transactions/commit` is not
advertised, or at least fails the statement.

Lab test: `tests/specific/test_pglake.py::test_insert_commits_to_catalog` (CATALOG=unity).
