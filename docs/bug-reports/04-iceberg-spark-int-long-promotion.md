# Spark vectorized reader: ClassCastException after int -> long promotion on DuckDB-written Parquet files

> **Fixed - do not file.** No longer reproduces with Iceberg 1.12.0 + Spark 4.1.4 (lab run
> 2026-11-16, `test_schema_evolution_read[duckdb-v2-spark]` passes on Lakekeeper, Polaris and Unity; on RustFS
> DuckDB cannot write, so it is not testable there). Kept for reference.

**Component:** iceberg-spark-runtime-4.0_2.13 (vectorized Parquet/Arrow reader)
**Versions:** Iceberg 1.11.0, Spark 4.0.5; files written by DuckDB 2.0.0 (iceberg extension 4c0c0874c4)

## Summary

After promoting a column from `int` to `long`, Spark fails to read data files written
before the promotion if they were written by DuckDB:
`ClassCastException: class ...arrow.vector.BigIntVector cannot be cast to class ...arrow.vector.IntVector`.
Files of the same table written by Spark, Trino, Flink or PyIceberg are read fine; PyIceberg,
DuckDB, Trino, Polars, Daft and Flink read the DuckDB files fine.

## Steps to reproduce

1. Table `(id int, name string, amount double)`; DuckDB inserts two rows.
2. Schema update: `id` int -> long (here with PyIceberg `update_column`), plus rename/add/drop.
3. Spark: `SELECT id, label, extra FROM t`.

The DuckDB file's Parquet schema: `optional int32 field_id=1 id (Int(bitWidth=32, isSigned=true))`
- i.e. INT32 with an explicit INTEGER(32, signed) logical type annotation.

## Expected

Rows with `id` as bigint.

## Actual

`ClassCastException BigIntVector -> IntVector` in the vectorized reader.
