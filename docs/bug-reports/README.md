# Bug report drafts (NOT submitted)

Drafts of upstream bug reports for findings of the lab test suite. Nothing here has been
filed. Before filing: re-check against the newest release, search the project's tracker for
duplicates, and remove anything specific to this lab.

| # | Project | Finding | Severity | Lab test |
|---|---|---|---|---|
| 01 | DuckDB iceberg extension | Lost commit response -> table points to deleted manifest list | data corruption | `test_robustness.py::test_catalog_fault[duckdb-lost_response]` |
| 02 | pg_lake | COMMIT reports success although the REST catalog refused it; on catalogs without `/transactions/commit` (Unity) every write is lost silently; a refused CREATE TABLE is reported as success too (2026-11-25) | silent data loss | `test_transactions.py::test_multi_table_atomicity[pg_lake]` |
| 03 | Trino | `ts < <day boundary>` returns NULL rows on day(ts) tables without null counts | wrong result | `test_evolution.py::test_partitioned_read[pg_lake-day-v2-trino]` (CATALOG=polaris) |
| 04 | Apache Iceberg (Spark) | ClassCastException reading int->long promoted column from DuckDB Parquet | read failure - **fixed in Iceberg 1.12** (no longer reproduces, do not file) | `test_evolution.py::test_schema_evolution_read[duckdb-v2-spark]` |
| 05 | PyIceberg | Vended credentials of a loaded table are never refreshed | read failure after expiry | `test_sts_expiry.py::test_read_after_credentials_expired[pyiceberg]` |
| 06 | pg_lake | 404 from `/v1/config` ignored, the next request goes to a wrong prefix (the `{s}` in the message is Lakekeeper's) | misleading error - reproduces again with pg_lake 3.7 on a fresh authz stack (2026-11-23); the earlier "no longer reproduces" came from leftover grants | `test_authz.py::test_engine_reads[pg_lake-bob]` |
| 07 | Apache Polaris (image) | Federation: brotli answer of the remote -> UnsatisfiedLinkError -> HTTP 500 | feature unusable | `test_catalog_features.py::test_federation_direct` (CATALOG=polaris) |
| 08 | pg_lake | Partition fields carry both `source-id` and `source-ids`; Lakekeeper 0.15 rejects it with 422, pg_lake ignores that -> every partitioned table / partition evolution fails | feature unusable | `test_evolution.py::test_partitioned_write[pg_lake-identity-v2]` (CATALOG=lakekeeper) |
| 09 | PyIceberg | Spec form `geometry(OGC:CRS84)` (unquoted CRS, written by Iceberg 1.12) not parsed -> tables with geo columns cannot be loaded | feature unusable | `test_types.py::test_write[pyiceberg-v3-geometry]` (CATALOG=polaris) |
| 10 | PyIceberg | `list_namespaces(parent)` double-encodes the `parent` parameter -> children of namespaces with non-ASCII/`%`/`\` names cannot be listed | tooling breaks | `test_security.py::test_name_is_refused_or_safe` (probe in `scripts/probes/probe_parent_encoding.py`) |
| 11 | RustFS | ~~Catalog access needs `admin:*`~~ **withdrawn: lab misconfiguration** - RustFS has granular table actions; with them no self-escalation (403) | – | – |
| 12 | DuckDB iceberg extension | Two concurrent MERGE INTO of the same new key both succeed - the key is there twice (no serializable validation) | duplicate data - **fixed in DuckDB 2.0.1** (5/5 runs clean, do not file) | `test_concurrency.py::test_concurrent_upsert_same_key[duckdb-v2]` (all catalogs) |
| 13 | Lakekeeper | Empty bodies for 404/405 (no Iceberg error document); any `snapshots` value accepted | client errors unreadable | `test_rest_conformance.py::test_error_body`, `::test_snapshots_value` |
| 14 | RustFS | STS raises DurationSeconds=60 to 900 silently; wildcard actions in session policies refused | keys live longer than asked / AWS incompatibility | `test_storage_s3.py::test_sts_session_policy` |
| 15 | RustFS | Iceberg catalog deletes every previous metadata file on commit (`write.metadata.delete-after-commit.enabled` off) - `metadata-log` points to missing files | inconsistent table history / concurrent clients fail | `test_catalog_consistency.py::test_metadata_log_files_are_kept` (CATALOG=rustfs) |
| 16 | Apache Iceberg (REST fixture) | Concurrent `POST /transactions/commit` all answered 204, only one change kept (server-side retry finds "Nothing to commit") | silent data loss | `test_catalog_consistency.py::test_parallel_transaction_commits_are_not_lost` (CATALOG=reference) |
| 17 | pg_lake | Always inserts a prefix into REST paths; catalogs without `prefix` in `/v1/config` (optional in the spec) cannot be used, no option to leave it out | feature unusable | `test_interop.py::test_write_then_read[pyiceberg-pg_lake]` (CATALOG=reference) |
| 18 | Apache Gravitino | REST-backend catalog with authorization on: a user with only SELECT gets the backend's vended keys, which write the table (Lakekeeper, Unity); a credential provider on the catalog changes nothing | privilege escalation (storage) | `test_authz.py::test_select_grant_vends_read_only_credentials` (CATALOG=gravitino_lakekeeper, gravitino_unity) |
| 19 | Apache Gravitino | REST-backend catalog: the backend's client errors (400/406/414) come back as HTTP 500, message kept | misleading error / retries | `test_catalog_consistency.py::test_invalid_commit_is_refused` (CATALOG=gravitino_rustfs), `test_types.py::test_v3_types_rejected_on_v2_tables` (CATALOG=gravitino_lakekeeper) |

Versions 01-07 were found with (all except 04 reproduced again on 2026-11-16 with the versions
listed for 08-09; 03 only on Polaris, see the report): Lakekeeper v0.14.3, RustFS 1.0.5, DuckDB 2.0.0 (iceberg extension 4c0c0874c4),
pg_lake 3.6.0 (Postgres 18.7), Trino 483, Spark 4.0.5 + iceberg-spark-runtime-4.0_2.13 1.11.0,
PyIceberg 0.12.0, Polars 2.1.0, Daft 0.7.26.
Versions for 08-11: Lakekeeper v0.15.0, pg_lake 3.6.0, Polaris 1.9.0, Trino 484, Spark 4.1.4 + Iceberg 1.12.0, Flink 2.3.0.
Versions for 12-14 (2026-11-18): Lakekeeper v0.15.1, RustFS 1.0.6, DuckDB 2.0.0, Polaris 1.9.0, Unity 0.7.0.
Version 15 (2026-11-20): RustFS 1.0.6.
Versions 16-17 (2026-11-23): Apache Iceberg REST fixture (Iceberg `main` c75374c, image digest 009f9f6897cc), pg_lake 3.7.0, DuckDB 2.0.1.

Full run 2026-11-20 (Lakekeeper v0.15.1, Polaris 1.9.0, RustFS 1.0.6, Unity 0.7.0; engines as for 12-14):
all findings 01-03, 05, 07-10 and 12-15 reproduced again (04 fixed, 06 seemed fixed but reproduces again on a fresh authz stack, 11 withdrawn).
Versions 18-19 (2026-11-25): Apache Gravitino 1.3.1 in front of Lakekeeper v0.15.1, Apache Polaris 1.9.0, Unity Catalog OSS 0.7.0 and RustFS 1.0.6. The facts of a cause (where it shows, the same tests elsewhere, the raw errors, the lab command) come from `uv run python scripts/gap_register.py draft <cause id>`.
