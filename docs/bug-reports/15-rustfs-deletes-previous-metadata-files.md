# [RustFS] Iceberg catalog deletes every previous metadata file on commit (metadata-log points to missing files)

**Component:** RustFS Iceberg REST catalog (table buckets)
**Version:** RustFS 1.0.6

## Summary

After each commit, RustFS's catalog deletes the table's previous `*.metadata.json`, although the
table does not set `write.metadata.delete-after-commit.enabled` (the spec default is `false`).
The `metadata-log` in the current metadata still lists the old files, but they are gone: a HEAD
with the root keys answers 403 (RustFS's answer for a missing key in the table bucket).

Clients that hold an older metadata file break. In the lab, DuckDB's
`iceberg_rewrite_data_files` running while another client appends fails with
`HTTP GET error reading '.../metadata/00004-....metadata.json' (HTTP 403 Forbidden)`. Time travel
or rollback by metadata file and registering an older metadata file cannot work either.

Lakekeeper 0.15.1, Polaris 1.9.0 and Unity Catalog 0.7.0 keep the files (same lab test).

## Steps to reproduce

```python
t = catalog.create_table("ns.t", pa.schema([("id", pa.int64())]))  # RustFS REST catalog, SigV4
for i in range(3):
    t.append(pa.table({"id": [i]}))
t = catalog.load_table("ns.t")
for e in t.metadata.metadata_log:  # 3 entries
    s3.head_object(Bucket=..., Key=...)  # 403 for each; the current file: 200
```

## Expected

The files in `metadata-log` stay until `write.metadata.delete-after-commit.enabled=true` (then
only beyond `write.metadata.previous-versions-max`), or until snapshot expiry / orphan cleanup
removes them - as in the Iceberg spec and the other catalogs.

## Notes

- Lab tests: `tests/catalog/test_catalog_consistency.py::test_metadata_log_files_are_kept`
  (all catalogs; only RustFS fails), `tests/engines/test_concurrency.py::test_append_during_compaction[duckdb-v2]`
  in the run behind the gaps (DuckDB reaches RustFS only there).
- Listing the internal `.rustfs-table/` prefix returns nothing, so the files cannot be found
  by listing either.
