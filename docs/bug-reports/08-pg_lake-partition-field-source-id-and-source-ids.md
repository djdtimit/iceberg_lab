# [pg_lake] Partition fields carry both `source-id` and `source-ids` - Lakekeeper 0.15 rejects every partitioned table

**Component:** pg_lake (pg_lake_iceberg, REST catalog write path)
**Versions:** pg_lake 3.6.0; catalog Lakekeeper v0.15.0 (worked with v0.14.3); storage S3-compatible (RustFS 1.0.5)

## Summary

pg_lake serialises every partition field with both `source-id` and `source-ids`:

```json
{"action":"add-spec","spec":{"spec-id":1,"fields":[
  {"source-id":2,"source-ids":[2],"field-id":1000,"name":"name","transform":"identity"}]}}
```

Lakekeeper 0.15 (newer iceberg-rust) treats the two keys as mutually exclusive and rejects the
request with `422 Unprocessable Entity`:

```
Failed to deserialize the JSON body into the target type: updates[9]: DataInvalid =>
source-id and source-ids are mutually exclusive at line 1 column 1009
```

Because pg_lake logs the 422 only as a WARNING, the statement continues and fails later
with misleading errors:

- `CREATE TABLE ... USING iceberg WITH (partition_by = ...)`: `HTTP request failed (HTTP 404)` /
  `Error getting tabular from catalog` / `NoSuchTableException` (the table was never created
  in the catalog).
- Partition evolution (`ALTER TABLE ... SET (partition_by = ...)` and the next INSERT):
  `partition spec 1 not found for manifest` (the `add-spec` update in `/transactions/commit` was
  rejected).

Unpartitioned tables are not affected.

## Steps to reproduce

1. Lakekeeper v0.15.0 as REST catalog for pg_lake.
2. `CREATE TABLE t (id bigint, name text, ts timestamp) USING iceberg WITH (partition_by = 'name');`
3. `INSERT INTO t VALUES (1, 'a', now());`

## Expected

The Iceberg spec (format/spec.md, Appendix C, partition specs) is explicit: *"For partition
fields with a transform with a single argument, only `source-id` is written. In case of a
multi-argument transform, only `source-ids` is written."* Appendix E (v3) adds that `source-id`
must be written for single-argument transforms. So pg_lake must send only `source-id` here;
the partitioned table is then created and the insert is committed.

The double field is also visible in pg_lake issue #112 (a request body quoted there, about TIME
partitions), but has not been reported as a problem of its own.

## Actual

422 from the catalog, ignored by pg_lake; the statement then fails with a 404 or
`partition spec 1 not found for manifest`.

## Notes

- Lakekeeper is strict here, Iceberg Java is lenient (it prefers `source-ids` when both are
  present). Fix in pg_lake: send only `source-id` for single-argument transforms. Lakekeeper
  could also accept both keys when they agree.
- Second issue: pg_lake treats a 4xx from the catalog on create/commit as a warning instead
  of failing the statement (see also 02-pg_lake-silent-commit-divergence.md).
- Lab proof: `tests/engines/test_evolution.py::test_partitioned_write[pg_lake-*-v2]`,
  `test_partition_evolution_write[pg_lake-v2]`,
  `tests/engines/test_operations.py::test_operation[pg_lake-partition_evolution-v2]` with `CATALOG=lakekeeper`.
