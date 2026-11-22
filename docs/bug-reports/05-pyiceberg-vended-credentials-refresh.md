# Vended credentials of a loaded table are never refreshed (ACCESS_DENIED after expiry)

**Component:** PyIceberg REST catalog / FileIO
**Versions:** PyIceberg 0.12.0; storage RustFS 1.0.5. Found with catalog Lakekeeper v0.14.3 (STS, 900 s
validity); reproduced again (2026-11-16) with Lakekeeper v0.15.0 (900 s), Apache Polaris 1.9.0 and
Unity Catalog OSS 0.7.0 (1 h each)

## Summary

`loadTable` returns short-lived vended credentials plus
`s3.session-token-expires-at-ms` and `client.refresh-credentials-endpoint`. A `Table`
object that is kept longer than the credentials' lifetime fails every later read with
`ACCESS_DENIED`; PyIceberg does not call the refresh endpoint. Polars (`scan_iceberg(table)`)
and Daft (`read_iceberg(table)`) inherit the problem.

## Steps to reproduce

1. Warehouse with vended STS credentials valid for 900 s.
2. `t = catalog.load_table("ns.t"); t.scan().to_arrow()`  -> works
3. Wait until `s3.session-token-expires-at-ms` has passed.
4. `t.scan().to_arrow()`

## Expected

PyIceberg obtains fresh credentials (via `client.refresh-credentials-endpoint`, or by
reloading the table) before they expire, and the read succeeds.

## Actual

`OSError: When reading information for key '.../snap-....avro' ...: AWS Error ACCESS_DENIED during HeadObject operation`.
DuckDB, Spark, Trino and Flink keep working in the same session.
