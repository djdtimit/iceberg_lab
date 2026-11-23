# [DuckDB iceberg] Two concurrent MERGE INTO of the same new key both succeed - the key is in the table twice

> **Fixed in DuckDB 2.0.1** (2026-11-22: 5 of 5 runs without duplicate; the lab's known gap reported `[gap fixed]`). Do not file.

**Component:** DuckDB iceberg extension (REST catalog write path, MERGE INTO)
**Versions:** DuckDB 2.0.0; catalogs Lakekeeper v0.15.1, Polaris 1.9.0, RustFS 1.0.6, Unity 0.7.0 (same on all)

## Summary

Two connections run `MERGE INTO t ... WHEN NOT MATCHED THEN INSERT` for the same key that does not
exist yet, at the same moment. Both statements report success, and the table then contains the key
twice. Iceberg's default isolation level for MERGE is `serializable`
(`write.merge.isolation-level`): a concurrent commit that adds rows matching the MERGE condition must
make the later commit fail (Spark and Trino do exactly that in the same test - one MERGE is refused
with a commit conflict).

## Steps to reproduce

1. Table `t (id int, name varchar, amount double)` with rows 1, 2, 3 (format v2 or v3).
2. Two DuckDB connections, each attached to the same REST catalog, run at the same time:
   ```sql
   MERGE INTO cat.ns.t t USING (SELECT * FROM (VALUES (4, 'x', 4.0)) AS s(id, name, amount)) s
   ON t.id = s.id
   WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount
   WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.amount);
   ```
   (the second with `(4, 'y', 5.0)`).
3. `SELECT * FROM cat.ns.t WHERE id = 4`

## Expected

One row with id 4: either both MERGEs applied one after the other (the second one updates the row
the first inserted), or one of them fails with a commit conflict.

## Actual

Both succeed; two rows with id 4: `(4, 'x', 4.0)` and `(4, 'y', 5.0)`. Reproduced on every run and
on all four catalogs.

## Notes

- Lab test: `tests/engines/test_concurrency.py::test_concurrent_upsert_same_key[duckdb-v2|duckdb-v3]`.
- The same class of bug exists in PyIceberg 0.12 (apache/iceberg-python#4194, already reported).
- Related: DuckDB's commit does carry a check - it retries an append after a 409 (4 times) - but
  it does not validate concurrent inserts for MERGE.
