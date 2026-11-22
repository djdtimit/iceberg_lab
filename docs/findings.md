# Gaps and bugs found

Back to the [README](../README.md). Findings by run date: [CHANGELOG.md](../CHANGELOG.md).


**Scope of the catalogs**: by capability (feature inventory, 53 in total) described in the API:
Lakekeeper 38, Polaris 22, Unity 20, RustFS 16 – of which also implemented by the server 35, 21,
20, 16 (Lakekeeper describes scan planning, functions and unregister but answers 404; Polaris remote
signing with 501). With 178 endpoints, Lakekeeper's API is more than twice as large as Polaris' (76)
– mostly because of finer-grained administration; on the Iceberg REST standard both are close
(35 to 29). Endpoint counts are therefore no measure for comparison (details:
`docs/test-architecture.md`, "API size").

**Catalogs compared** (per catalog only what the others do not have; evidence in the `KNOWN_GAPS`
of the test files and the central rules in `tests/conftest.py`)
- **All four**: no server-side scan planning (`/plan` not offered), no row/column security, no
  `read-restrictions` in `loadTable`.
- **Lakekeeper 0.15**: hides objects without a grant (answers "does not exist" instead of 403);
  rejects `geometry`/`geography` (422 - every spelling: plain, with CRS, with SRID, with edge
  algorithm; checked by raw REST on 2026-11-22: it cannot parse such a schema at all) and – new in 0.15 – partition fields with both `source-id`
  and `source-ids` (affects pg_lake, see below); describes `functions` and `unregister` in the API
  but answers 404; the namespace properties response lacks the field `missing` (PyIceberg fails);
  unknown paths and disallowed methods with an empty body instead of an Iceberg error document
  (404/405); accepts any value for `snapshots` (spec: `all`/`refs`).
- **Polaris 1.9**: remote signing is in the API but answers 501; no moving tables into another
  namespace (1.7 could); direct federation fails on Brotli (native module missing in the image,
  `docs/bug-reports/07`; without compression through a proxy it works); `/transactions/commit`
  requires `TABLE_FULL_METADATA`. More below under "Polaris 1.9".
- **RustFS catalog 1.0.5/1.0.6**: no format v3 (406); no DROP with purge (406), and clients may not
  delete table files themselves (403) – Spark/Trino DROP fails; catalog rights only per bucket, not
  per table – metadata of every table visible to every catalog user; requires
  `x-amz-content-sha256` on every signed POST (PyIceberg, Polars, Daft fail; the lab fixes this only
  for its own client); DuckDB cannot attach SigV4 catalogs outside AWS; a rename leaves an invisible
  entry behind (the namespace can no longer be dropped); `register`, sync and rollback only within
  its own table layout; table buckets can be neither disabled nor deleted after use; several
  endpoints answer 501; rejects views from Spark and Flink (field ID 0); Flink's first upsert into
  an empty table fails.
- **Unity Catalog OSS 0.7**: no `/transactions/commit` (no multi-table transaction; pg_lake silently
  loses every write); views readable only, not creatable; no types `time`, `timestamp_ns`,
  `timestamptz_ns`, `unknown`, `geometry`/`geography` (400 on create; `variant` works); fixed
  hierarchy without nested namespaces; rename only within the same namespace; ignores `pageSize`;
  answers `ns.t.snapshots` with NoSuchNamespace instead of NoSuchTable – Spark and Flink therefore
  find no metadata tables, branches and tags; deletes the files of dropped tables only after 7 days;
  SCIM DELETE only deactivates users.

**Access and remote signing**
- Only **PyIceberg**, **Spark** and **Flink** can do remote signing. DuckDB, Daft, the Polars reader
  and Trino cannot.
- **Remote signing with PyIceberg is intermittently unstable (cause found):** about 1–10 % of the
  commits on `demo-signing` fail with 403. The reason is a **race condition in PyIceberg**
  (`pyiceberg/io/fsspec.py`, `_s3`): fsspec returns a shared, cached S3FileSystem instance, and
  every new FileIO unregisters and re-registers the signer on it – from any thread. Requests signed
  exactly in that window (under 2 ms) go out **unsigned**; RustFS logs them as
  `account: <anonymous>`. Lakekeeper and RustFS are not to blame (0 errors in 9000 locally signed
  requests and 3000 signed by Lakekeeper). Spark is not affected (0/30). Workaround until a fix:
  `demo-sts` instead of remote signing.
- The remote signer checks permissions, not the table ID in the signer URL: without authz it also
  signs paths of other tables (paths outside every table are rejected).
- Lakekeeper v0.15 rejects partition fields with `source-id` **and** `source-ids` (422, "mutually
  exclusive"; v0.14 accepted them). pg_lake sends both and ignores the rejection: **every
  partitioned pg_lake table and every partition evolution fails** – with misleading follow-up errors
  (404 `NoSuchTableException`, `partition spec 1 not found for manifest`).

**PyIceberg 0.12** (also affects Polars/Daft when writing)
- supports v3 **only partly**: **creating and reading** v3 tables (incl. deletion vectors,
  `initial-default`, ns timestamps) and **changing metadata** (schema, branches/tags, rename) works.
  **Writing data into v3 tables** (append/overwrite/delete/upsert) does not: in 0.12.0 – the current
  version – `write_manifest` and `write_manifest_list` implement only v1 and v2
  (`Cannot write manifest for table version: 3`). Polars and Daft write through PyIceberg and
  inherit this.
- does not read **position deletes from Trino** (`DeltaLengthByteArrayDecoder`).
- `upsert` after `rename_column` fails (compares against the old column names).
- `NULL` in `list<struct>` becomes `[]` on write and read.
- does not know `variant` (tables with a variant cannot be loaded).

**Others**
- DuckDB writes `fixed[n]` as variable `binary` → PyIceberg/Spark reject it.
- DuckDB v3 manifests have no partition summaries → the `manifests` metadata table crashes in Trino
  (and Spark for tables created by PyIceberg).
- After every data-changing operation DuckDB and Spark read back (`cross_check` in
  `test_operations.py`): if an engine writes files only it can read, this stands out.
- Daft does not read DuckDB's nested Parquet lists and maps (two different errors) – this also
  affects pg_lake, which writes its files through `pgduck_server` (DuckDB).
- Daft does not apply v3 deletion vectors (reads Puffin as Parquet) and ignores `initial-default`.
- Polars: no `uuid`/`fixed`, `time` is ns-precise (Iceberg: µs).
- Spark 4.1: no `time`, no ns timestamps. Trino 484: reads `unknown` (since 484), does not write it;
  no branch creation, no atomic overwrite (Iceberg writes only in autocommit).
- DuckDB 2.0: `iceberg_rewrite_data_files` (compaction) not yet for v3.

**pg_lake 3.6** (extension version; built from release tag v3.7.0)
- no `MERGE`, no rename of Iceberg tables in the REST catalog.
- does not read v3 deletion vectors or the types `unknown`, `variant`, `timestamptz_ns`;
  `timestamp_ns` is truncated to microseconds (Postgres precision).
- refuses tables with `initial-default`.
- compaction only with `VACUUM (FULL)` – a plain `VACUUM` deliberately leaves small files.
- `VACUUM` with `max_snapshot_age = 0` does not expire reliably (intermittent, two symptoms): the
  old snapshot stays in the metadata, or it disappears but its data file stays in storage – even
  after another `VACUUM` and `lake_engine.flush_deletion_queue()`.

**Flink 2.3 / Iceberg 1.12**
- no `DELETE`/`UPDATE` (the sink does not implement `SupportsRowLevelDelete/Update`) and no `MERGE`
  statement; Flink's substitute is the upsert (primary key + `write.upsert.enabled`), which writes
  **equality deletes** – which **PyIceberg, Polars and Daft cannot read** (DuckDB, Spark, Trino,
  pg_lake can).
- no procedures (`getProcedure is not implemented for class FlinkCatalog`): no rollback, no
  compaction, no expire through SQL (only the Java API `TableMaintenance`); tags and partition
  evolution have no SQL syntax. Branches work through `OPTIONS('branch'=...)`.
- `TIME` is truncated to seconds on write and to milliseconds on read (Iceberg: µs); `unknown` and
  `variant` cannot be written, `variant` cannot be read through the gateway, `uuid` shows as
  `BINARY(16)`.
- ignores v3 `initial-default` (returns `NULL`).

**Maintenance**
- PyIceberg 0.12: `expire_snapshots` removes snapshots only from the metadata – data, manifest and
  manifest list files stay in storage.
- Lakekeeper deletes files on DROP only with `purgeRequested=true`: PyIceberg `purge_table`, Spark,
  Trino, pg_lake and Flink do that; **DuckDB's `DROP TABLE` does not** (files stay).
- DuckDB 2.0.1: `iceberg_rewrite_data_files` now removes the applied position delete files (fixed;
  2.0.0 left them behind), still cannot do v3 and does not retry after a commit conflict (409) –
  with concurrent appends it fails. The new `iceberg_expire_snapshots` removes snapshots only from
  the metadata (data files and manifest lists stay, like PyIceberg). On v3 DuckDB cannot take a
  data file out of a table at all: a full `DELETE` or `TRUNCATE` only adds deletion vectors.
- pg_lake: `VACUUM` does not remove foreign orphan files (only those it tracks itself).
- Spark's SQL procedure `remove_orphan_files` refuses intervals under 24 h; the Java action
  (`deleteOrphanFiles().usePrefixListing(true)`) works.
- **pg_lake** rejects v3 tables whose manifests still list a removed deletion vector (status
  DELETED) – i.e. every v3 table after a DV compaction by Spark/Trino.

**Concurrency and robustness**
- **DuckDB: data corruption** – if the response to a commit is lost (the catalog accepted it),
  DuckDB deletes the commit's files; the current snapshot then points to a missing manifest list
  (reproduced on Lakekeeper, Polaris and Unity; on RustFS DuckDB cannot connect).
- DuckDB does not retry commits after a conflict (409) (intermittent, depending on timing).
- DuckDB 2.0.1 fixed concurrent `MERGE INTO` of the same new key (2.0.0: both succeeded, the key
  was there twice); one of them is now refused, as in Spark and Trino.
- All other engines: parallel appends are retried, DELETE against UPDATE is serializable, lost
  responses do not lead to duplicate rows.

**Evolution, views, namespaces**
- **Trino returns wrong results**: with `day(ts)` partitioning and the filter `ts < <day boundary>`,
  the row with `ts = NULL` is included if the files have no `null_value_counts` (pg_lake writes
  none). One second later, with `month(ts)` or `>=`, the result is right.
- Fixed with Iceberg 1.12: Spark's vectorized reader crashed after `int → long` on Parquet files
  written by DuckDB (`ClassCastException BigIntVector → IntVector`).
- Spark caches tables (`cache-enabled=true`, 30 s): it sees changes of other engines (renamed
  column, new partition spec) only afterwards – until then it writes with the old spec.
- Views: Trino and DuckDB only run views in their own dialect, Spark also foreign ones; Flink 2.3
  creates and reads views (Flink 2.1 could do neither). Views store the creator's catalog name
  (`default-catalog`) – portable only if all engines give the catalog the same name (here `sts` or
  `psts`/`rsts`/`usts`).
- DuckDB: `CREATE SCHEMA lk."a"."b"` silently creates a top-level namespace `b` (correct:
  `lk."a.b"`). Lakekeeper's namespace properties response lacks the field `missing` that PyIceberg
  0.12 expects (Polaris sends it, there it works). Flink knows no nested namespaces.
- Statistics: Spark (`compute_table_stats`) and Trino (`ANALYZE`) write identical NDV sketches, and
  Trino also uses Spark's; pg_lake's `ANALYZE` writes nothing into the Iceberg metadata.

**Transactions, WAP, changelog, layout**
- **pg_lake reports COMMIT success although the REST catalog rejected the commit**: Postgres shows
  the rows, the catalog and all other engines do not – silent divergence, nothing in the log.
- Flink `STATEMENT SET` is not atomic (every table committed separately); Trino does not write in
  explicit transactions. DuckDB and pg_lake use `/transactions/commit` (DuckDB atomically).
- WAP: Trino cannot create branches, Flink cannot publish a branch.
- Trino `table_changes` reports unchanged rows as delete + insert with copy-on-write.
- PyIceberg, Polars, Daft, pg_lake and Flink ignore the table's sort order; pg_lake ignores
  `write.target-file-size-bytes`.
- pg_lake ignores a 404 response of the catalog (warehouse invisible to the user) and sends the
  literal placeholder `{s}` as warehouse ID. Trino reports a refused write only as "Error committing
  write to Parquet file".

**Expiry of vended credentials** (`test_sts_expiry.py`: Lakekeeper 900 s, Polaris and Unity 1 h –
the same result on all three; RustFS vends no credentials)
- A held PyIceberg table object (also when read by Polars/Daft) gets `ACCESS_DENIED` after expiry –
  no refresh through `client.refresh-credentials-endpoint`. DuckDB, Spark, Trino and Flink keep
  reading in the same session.

**Polaris 1.9 (compared to Lakekeeper)**
- rejects a v3 type in a v2 table with HTTP 500 instead of 400 – the Apache Iceberg REST fixture
  (Iceberg Java's own handlers) answers the same, so this is Iceberg Java behaviour that Polaris
  passes through, not a Polaris bug.
- writes (Iceberg 1.12) the CRS of geo columns explicitly and, per spec, unquoted
  (`geometry(OGC:CRS84)`); PyIceberg 0.12 expects it quoted and **cannot load such tables at all**
  – neither can Polars/Daft (`docs/bug-reports/09`).
- allows `geometry`/`geography` (Lakekeeper: 422) – this exposes engine gaps. Read from the
  neutral source (Iceberg Java core): Spark 4.1 and Trino read both types (Trino reports
  `geography` as `SphericalGeography`), DuckDB 2.0 reads `geometry` but not `geography`;
  PyIceberg (and thus Polars/Daft) cannot load such tables, Flink and pg_lake 3.6 do not know the
  types. Writing: only Trino writes `geometry` (no `geography`); Spark, DuckDB and Flink have no
  SQL type for them, pg_lake creates only v2 tables.
- deletes on DROP only with `purgeRequested=true`: pg_lake's `DROP TABLE` does not request it (under
  Lakekeeper's hard delete this goes unnoticed); Spark needs `DROP TABLE ... PURGE`.
- returns `client.refresh-credentials-endpoint` relative to the catalog URI (Lakekeeper: absolute);
  STS keys are restricted to the table as with Lakekeeper.

**Spark with Lakekeeper**
- `DROP TABLE … PURGE` collides with Lakekeeper's own deletion (`delete-profile: hard`): on
  `demo-signing` the signer refuses Spark's delete requests for the already removed table (always),
  on `demo-sts` there is an intermittent `NoSuchKeyException`. With Lakekeeper use `DROP TABLE`
  **without** `PURGE` – Lakekeeper deletes the data itself.

**Type fidelity** (the value is right, the type is weaker)
- Spark: `uuid` → `string`, `fixed(n)` → `binary`. Polars: `uuid`/`fixed(n)` → `Binary`.
  Trino: `fixed(n)` → `varbinary`. pg_lake: `fixed(n)` → `bytea`, `timestamp_ns` → µs timestamp.

**Observed, but not reproducible**
- Once under load: DuckDB `Cannot reconstruct table … at the transaction start` when reading a table
  just written by pg_lake (10 repetitions without error). If it happens again, it shows up as a new
  failure.
- Once: the Spark notebook failed, then stable green; Spark is demonstrably not affected by the
  signing 403 (0/30).

The complete list is in the `KNOWN_GAPS` tables of the test files.
