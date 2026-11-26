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

## The everyday case: a concurrent commit (409)

The same happens without any fault and on catalogs that implement `/transactions/commit`
(reproduced 2026-11-23, pg_lake 3.7.0 on Postgres 19, Lakekeeper 0.15.1 and Apache Polaris
1.9.0): another client commits to the table between pg_lake's table load and its commit.

1. A pg_lake table `t` with three rows.
2. Just before pg_lake's commit reaches the catalog, another client (PyIceberg) appends one row
   to `t` - a normal concurrent write.
3. `INSERT INTO t VALUES (99, 'fault', 9.0);`

The catalog answers pg_lake's commit correctly with 409:

```
POST /catalog/v1/<warehouse>/transactions/commit   409
{"error":{"message":"Cannot add snapshot with sequence number 2 older than last sequence number 2","type":"CommitFailedException","code":409}}
```

Polaris: the same 409 ("Validation failed, please retry: ...").

pg_lake neither reloads the table and retries (what a 409 `CommitFailedException` asks for) nor
fails the statement: the INSERT returns without error, Postgres shows row 99, the catalog has the
other client's row but not row 99 - also 5 s later. So any concurrent writer can make pg_lake
lose writes silently.

Expected: reload and retry the commit (the append does not conflict with the other append), or
fail the statement.

Lab test: `tests/storage/test_robustness.py::test_catalog_fault[pg_lake-concurrent_commit]`.

## CREATE TABLE refused by the catalog (reproduced 2026-11-25, pg_lake 3.7.1 on Postgres 19)

The same happens when the catalog refuses the creation itself:

- **Unity Catalog OSS 0.7.0** refuses a `time` column with 400.
  `CREATE TABLE ns.t (id bigint, v time) USING iceberg WITH (catalog = '<unity server>')`
  returns without error, and the table exists in Postgres. In the catalog there is no table
  (`loadTable` 404), so every later INSERT goes nowhere.
- **Gravitino 1.3.1 in front of Lakekeeper or RustFS** (`lakehouse-iceberg` catalog with
  `catalog-backend = rest`) refuses the commit that completes a staged creation with HTTP 500
  ("Cannot assign a new UUID" from Lakekeeper, "cannot reassign table uuid" from RustFS). That is
  a Gravitino issue, see report 19 and the lab's `gravitino-rest-backend-staged-create`. pg_lake
  still reports `CREATE TABLE` as successful. Every pg_lake table there exists only in
  Postgres: 28 lab tests per profile fail later with "table not found" in the catalog.

Expected: `CREATE TABLE` fails with the catalog's error, and no table is left in Postgres.

Lab test: `tests/specific/test_pglake.py::test_create_refused_by_catalog_is_an_error`
(`CATALOG=unity`, `gravitino_lakekeeper`, `gravitino_rustfs`).
