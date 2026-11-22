# [iceberg] Lost commit response: DuckDB deletes the files of a commit the catalog applied (table corrupted)

**Component:** DuckDB iceberg extension (REST catalog, write path)
**Versions:** DuckDB 2.0.0, iceberg extension 4c0c0874c4; storage S3-compatible (RustFS 1.0.5). Found with catalog
Lakekeeper v0.14.3; reproduced again (2026-11-16) with Lakekeeper v0.15.0, Apache Polaris 1.9.0 and
Unity Catalog OSS 0.7.0 - the behaviour does not depend on the catalog.

## Summary

If the HTTP response to a table commit is lost (the catalog applied the commit, but the
client never receives the answer), DuckDB treats the commit as failed and deletes the files
it wrote - including the manifest list of the new snapshot. The catalog's current snapshot
then points to a manifest list that no longer exists: the table can no longer be read by
any engine.

## Steps to reproduce

1. Put a forwarding HTTP proxy between DuckDB and the REST catalog that forwards the commit
   request (`POST /v1/{prefix}/namespaces/{ns}/tables/{table}` or
   `/transactions/commit`) to the catalog, then closes the client connection without
   sending the response.
2. `ATTACH '<warehouse>' AS lk (TYPE iceberg, ENDPOINT 'http://<proxy>/catalog', ...)`
3. `CREATE TABLE lk.ns.t (id INTEGER, name VARCHAR, amount DOUBLE)` and insert some rows
   (proxy transparent).
4. Arm the proxy, then `INSERT INTO lk.ns.t VALUES (99, 'x', 9.0)`.
5. Load the table with any client (e.g. PyIceberg) and read it.

## Expected

The outcome of the commit is unknown to the client. It must not delete any file that the
commit referenced (Iceberg Java raises `CommitStateUnknownException` and keeps the files).
The error may surface, but the table must stay readable - with or without the new row.

## Actual

- DuckDB reports `TransactionContext Error: Failed to commit Iceberg transaction: HTTP GET
  error reading 's3://.../metadata/snap-....avro'`.
- The catalog's current snapshot is the new one (the commit was applied).
- Its manifest list `snap-<id>-....avro` does not exist in storage any more (3 of 3 runs).
- Every reader fails with "file not found" for that manifest list.

## Notes

Lakekeeper advertises `idempotency-key-lifetime` in `/v1/config`; sending an
`Idempotency-Key` and retrying would make the commit safe to repeat.
