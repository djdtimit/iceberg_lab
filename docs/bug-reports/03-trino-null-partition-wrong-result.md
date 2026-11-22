# Wrong result: NULL rows returned for `ts < <day boundary>` on day(ts)-partitioned tables without null counts

**Component:** Trino Iceberg connector
**Versions:** found with Trino 483 (catalog Lakekeeper v0.14.3); reproduced again (2026-11-16) with
Trino 484 on Apache Polaris 1.9.0; table written by pg_lake 3.6.0. (With Lakekeeper v0.15.0 pg_lake
cannot create partitioned tables at all - see 08 - so the case cannot be set up there.)

## Summary

On a table partitioned by `day(ts)` whose data files carry no `null_value_counts` (pg_lake
does not write them - they are optional in the spec), the predicate `ts < TIMESTAMP
'<midnight>'` returns the row with `ts IS NULL`. The plan shows the predicate as enforced
by the partition (`constraint on [ts]`), so it is not re-evaluated on the rows, and the
NULL partition is kept.

## Steps to reproduce

1. Table `(id bigint, name varchar, ts timestamp(6))` partitioned by `day(ts)`, rows:
   `(1, '2025-12-31 23:59:59')`, `(2, '2026-01-01 00:00:00')`, `(3, '2026-01-15 13:30:00')`,
   `(6, NULL)` - written by a writer that omits `null_value_counts` (e.g. pg_lake).
2. `SELECT id FROM t WHERE ts < TIMESTAMP '2026-01-15 00:00:00'`

## Expected

`1, 2`

## Actual

`1, 2, 6`

Correct (checked): the same predicate one second later (`< '2026-01-15 00:00:01'`, not
aligned with a day boundary), `month(ts)` partitioning, `>=` predicates, and tables written
by Spark/PyIceberg/DuckDB (which write null counts). PyIceberg, DuckDB, Spark, Polars, Daft
and Flink return `1, 2` on the same table.
