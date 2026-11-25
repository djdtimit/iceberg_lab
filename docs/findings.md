# Gaps and bugs found

Back to the [README](../README.md). Findings by run date: [CHANGELOG.md](../CHANGELOG.md).


**Scope of the catalogs**: by capability (feature inventory, 59 in total, 8 of them out of scope
because they have no bearing on Iceberg tables) described in the API: Lakekeeper 39, Gravitino 30,
Polaris 23, Unity 21, RustFS 18, Iceberg REST fixture 10 – of which also implemented by the server
37, 30, 22, 21, 18, 10 (Lakekeeper describes scan planning and unregister but answers 404; Polaris
remote signing with 501). With 178 endpoints, Lakekeeper's API is more than twice as large as Polaris' (76)
– mostly because of finer-grained administration; on the Iceberg REST standard both are close
(35 to 29). Endpoint counts are therefore no measure for comparison (details:
`docs/test-architecture.md`, "API size").

**Catalogs compared** (per catalog only what the others do not have; evidence in the `KNOWN_GAPS`
of the test files, the central rules in `src/iceberg_lab/lab/gaps.py` and the list of all causes
at the end)
- **All six**: no row/column security, no `read-restrictions` in `loadTable`. Server-side scan
  planning (`/plan`): only Gravitino and the Iceberg REST fixture; the other four do not offer it.
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
- **Gravitino 1.3** (its own catalogs: lakehouse-iceberg with a JDBC backend, STS vending on
  RustFS): SECURITY: accepts every table location and vends STS keys that read another table's
  files. Several refusals take effect anyway: a namespace with a dot (400, created; afterwards no
  top-level listing of the catalog works), a table named `a\b` (400, created), a `variant` column
  (406, created), writes to a switched-off catalog (500, done). No `/transactions/commit`; no
  privilege for describing or dropping a table, and listing a schema's tables needs a table
  privilege; error answers carry Java stack traces; a v3 type in a v2 table gets 500;
  `testConnection` does not check the storage.
- **Iceberg REST fixture (Apache Iceberg's test fixture, build c75374c)**: an open catalog (no
  authentication, no authorization); the only one that implements server-side scan planning.
  SECURITY: accepts every table location (inside, equal to or above another table's directory,
  set later, registered from foreign metadata, `write.data.path`). DATA LOSS: answers concurrent
  `POST /transactions/commit` with 204 although it kept only one of them (DuckDB commits through
  this endpoint). Error answers carry the Java exception class and stack trace; malformed
  requests get 500 with an HTML page, an unsupported method 501 with an empty body, a v3 type in
  a v2 table 500 instead of 400; accepts names it cannot address afterwards (encoded `/`, `\`,
  newline, `%`).

**Gravitino in front of another catalog** (profiles `gravitino_<backend>`, a catalog with
`catalog-backend = rest`; [setup](setup.md))
- **What works**: what is written through Gravitino is the backend's table (same metadata file),
  the backend's own commits and drops are seen at once, and Gravitino's own API lists, loads and
  tags tables it never created. Storage credentials the backend vends reach the clients unchanged.
- **The gaps of both layers add up**: the backend's own gaps stay, Gravitino's are added (for
  example `gravitino-dotted-names`: a namespace with a dot reaches the backend, then Gravitino
  lists nothing of the catalog).
- **Only through the REST backend**: a staged creation (CTAS, create and write in one commit) is
  refused by Lakekeeper and RustFS (`gravitino-rest-backend-staged-create`; pg_lake then loses
  the table); some of the backend's client errors come back as 500
  (`gravitino-rest-backend-client-error-500`); a name too long for Gravitino's store is refused
  after the backend created it (`gravitino-rest-backend-long-name-left`).
- **SECURITY with the default configuration**: Gravitino does not check its own privileges for
  such a catalog (`gravitino.iceberg-rest.disable-rest-authz`, default `true`). A user without
  any grant got an empty namespace listing, but loaded, read and created tables; only the
  backend's check of the forwarded token was left (Lakekeeper and Unity accept it). The lab sets
  the switch to `false`, so Gravitino decides before it forwards; this turned 45 failing cases in
  front of Lakekeeper and 43 in front of Unity into passes and broke none. Since the lab no longer
  runs the default, this finding has no gap entry.
- **SECURITY: the backend's storage credentials pass unchanged.** Gravitino checks its own
  privileges, but hands on the keys the backend vends for the forwarded token: a user with only
  SELECT in Gravitino gets keys that write the table's files in front of Lakekeeper and Unity
  (`gravitino-rest-backend-credentials-not-narrowed`). On its own catalogs Gravitino vends
  read-only keys for SELECT.
- **Gravitino advertises endpoints the backend lacks**: its `/v1/config` lists its own
  endpoints, so clients try registering a table or creating a view in front of Unity and
  renaming a view in front of RustFS, and fail late (`gravitino-rest-backend-advertises-unsupported`).
- **Server errors come late**: Gravitino retries a backend's 500 for about 30 s before passing
  it on (`gravitino-rest-backend-retries-server-errors`).
- **Double authorization in front of Polaris**: Gravitino forwards the caller's token, and Polaris
  decides again by its own grants, which Gravitino does not set. Every right has to be granted in
  both; a grant through Gravitino's API fails with 500 because Gravitino loads the object from
  Polaris with the caller's token (`gravitino-rest-backend-grants-not-forwarded`).
- **A fixed service identity is no way out** (tested 2026-11-25, not kept): Gravitino always
  installs its token-forwarding auth manager for a REST backend, but adds the caller's token only
  if the request has no `Authorization` header yet - a static `header.Authorization` on the
  catalog wins. With Polaris' root token there, 24 of the 30 double-authorization cases passed,
  but Polaris vended write credentials for the service identity to a user with SELECT only, every
  user who may see the catalog could read the token in its properties (the S3 secret is masked,
  this header is not), and the token is never refreshed.

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
- DuckDB 2.0.1 writes only Parquet data files and now refuses a table with
  `write.format.default` ORC/Avro (2.0.0 wrote Parquet instead).
- Flink 2.3: no FLOAT NaN/Infinity in SQL (`CAST('NaN' AS FLOAT)`, also via DOUBLE:
  `NumberFormatException`); DOUBLE NaN works.
- Flink 2.3, **wrong result:** the literal `CAST('-0.0' AS DOUBLE)` becomes +0.0 (`-CAST(0.0 AS
  DOUBLE)` keeps the sign).

**pg_lake 3.7** (extension version 3.7, release v3.7.0, Postgres 19)
- no `MERGE`, no rename of Iceberg tables in the REST catalog.
- does not read v3 deletion vectors or the types `unknown`, `variant`, `timestamptz_ns`;
  `timestamp_ns` is truncated to microseconds (Postgres precision).
- refuses tables with `initial-default`.
- compaction only with `VACUUM (FULL)` – a plain `VACUUM` deliberately leaves small files.
- `VACUUM` with `max_snapshot_age = 0` does not expire reliably (intermittent, two symptoms): the
  old snapshot stays in the metadata, or it disappears but its data file stays in storage – even
  after another `VACUUM` and `lake_engine.flush_deletion_queue()`.
- after `VACUUM` expired snapshots, pg_lake's own next `INSERT` into the table fails
  (`duplicate key ... deletion_queue_pkey`: the same manifest is queued for deletion twice).
- **wrong result:** resolves struct fields by name, not by field id - a renamed struct field
  reads as NULL; with no field name in common the read fails (`STRUCT to STRUCT cast`).
  It cannot evolve a struct column itself (`ALTER TYPE` on a type used by an Iceberg table is
  refused).
- new in 3.7: `lake_iceberg.catalog_stats()` - request counters and times of its REST catalog
  calls per operation and result (tested in `test_pglake.py`).
- a refused `/v1/config` is ignored; the refusal shows as HTTP 400 for a wrong prefix
  (docs/bug-reports/06).

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
  2.0.0 left them behind), still cannot do v3 and fails on the conflict when DuckDB appends to the table
  at the same time (Spark and Trino finish both). The new `iceberg_expire_snapshots` removes snapshots only from
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
- DuckDB retries a conflicting commit 4 times (5 attempts, as many as Iceberg Java's and
  PyIceberg's default; forced by the fault proxy: 3 conflicts in a row pass, 5 fail). With 4 or
  more writers committing at once all attempts can conflict (409, intermittent).
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
- DuckDB up to 2.0.0: `CREATE SCHEMA lk."a"."b"` silently created a top-level namespace `b`
  (correct: `lk."a.b"`); fixed in 2.0.1. Lakekeeper's namespace properties response lacks the field `missing` that PyIceberg
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
  PyIceberg (and thus Polars/Daft) cannot load such tables, Flink and pg_lake 3.7 do not know the
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

## All known gaps, by cause

Generated from the gap register [`docs/gaps.toml`](gaps.toml): one line per cause, severe ones
first, with the versions it was last reproduced with (`seen_at`), bug report, upstream issue and
how many test cases of each reference in `results/` hit it. The test files say where each cause
applies (their `KNOWN_GAPS` tables name the cause id at the end of each line).

<!-- gap-register:start (generated by scripts/gap_register.py findings) -->

**lakekeeper** (9 causes)

- Lakekeeper accepts a table named '..'; HTTP clients must remove '..' path segments (RFC 3986), so the table can only be reached percent-encoded (%2E%2E) - Polaris, RustFS and Unity refuse the name — lakekeeper 0.15.1 · hit in lakekeeper (1) `lakekeeper-dotdot-table-name`
- Lakekeeper answers a path it does not have (404) or a method a path does not support (405) with an empty body - no Iceberg error document (Iceberg clients cannot parse the error; same as unitycatalog#1835) — lakekeeper 0.15.1 · [bug report](bug-reports/13-lakekeeper-error-bodies-and-snapshots-param.md) · hit in lakekeeper (2) `lakekeeper-error-body-empty`
- Lakekeeper 0.15 describes GET .../namespaces/{ns}/functions and POST .../tables/{t}/unregister in its API, but the server answers 404 (not advertised in /v1/config) — lakekeeper 0.15.1 · hit in lakekeeper (1) `lakekeeper-functions-unregister-404`
- Spark PURGE races with Lakekeeper's hard delete (intermittent NoSuchKeyException) — lakekeeper 0.15.1, spark 4.1.4+iceberg1.12.0 · intermittent · hit in lakekeeper (1) `lakekeeper-hard-delete-race`
- Lakekeeper's update-namespace-properties response lacks the field 'missing' that PyIceberg 0.12 requires (Polaris sends it) — lakekeeper 0.15.1, pyiceberg 0.12.0 · hit in lakekeeper (1) `lakekeeper-ns-properties-missing-field`
- Lakekeeper 0.15 with OpenFGA ignores roles from the token: LAKEKEEPER__OPENID_ROLES_CLAIM is used only by the Cedar authorizer (docs: 'The default OpenFGA implementation does not use token roles') - membership must be granted in Lakekeeper — lakekeeper 0.15.1, openfga 1.23.0 · hit in lakekeeper (1) `lakekeeper-openfga-ignores-token-roles`
- Lakekeeper v0.15 rejects geometry/geography in table schemas (422) — lakekeeper 0.15.1 · hit in gravitino_lakekeeper (23), lakekeeper (23) `lakekeeper-rejects-geo`
- Spark PURGE after drop: Lakekeeper's signer refuses deletes for the dropped table — lakekeeper 0.15.1, spark 4.1.4+iceberg1.12.0 · hit in lakekeeper (1) `lakekeeper-signer-refuses-purge`
- Lakekeeper accepts any value of loadTable's 'snapshots' parameter (spec: all|refs; Unity, Polaris and RustFS answer 400) — lakekeeper 0.15.1 · [bug report](bug-reports/13-lakekeeper-error-bodies-and-snapshots-param.md) · hit in lakekeeper (1) `lakekeeper-snapshots-param`

**polaris** (6 causes)

- Polaris 1.9.0 image: federation asks the remote catalog for brotli, but brotli4j has no native library for x86_64 (UnsatisfiedLinkError) - every answer from Lakekeeper (Content-Encoding: br) fails with HTTP 500; without compression federation works (test_federation) — polaris 1.9.0 · [bug report](bug-reports/07-polaris-federation-brotli.md) · hit in polaris (1) `polaris-federation-brotli`
- Polaris activates a role from the token (principal-roles-mapper) only if the principal is ALSO assigned that role in Polaris: the identity provider can narrow the roles, not grant them — polaris 1.9.0, keycloak 26.8.2 · upstream apache/polaris#2373 · hit in polaris (1) `polaris-idp-roles-narrow-only`
- Polaris 1.9 refuses moving a table into another namespace: the table keeps its location under the old namespace, which is outside the new namespace's allowed location (1.7 allowed it) — polaris 1.9.0 · hit in gravitino_polaris (1), polaris (1) `polaris-rename-across-namespaces`
- Polaris 1.9 has the Iceberg signing endpoint in its API but answers 501 Not Implemented (not advertised in /v1/config; no switch to enable S3 remote signing) — polaris 1.9.0 · hit in polaris (1) `polaris-signing-501`
- Polaris requires TABLE_FULL_METADATA for /transactions/commit (TABLE_WRITE_DATA suffices for a normal commit) - DuckDB always commits that way, so a write grant does not let it write — polaris 1.9.0, duckdb 2.0.1 · hit in polaris (1) `polaris-tx-commit-full-metadata`
- Polaris rejects a v3 type in a v2 table with HTTP 500 instead of 400 (Bad Request) — polaris 1.9.0 · hit in gravitino_polaris (3), polaris (3) `polaris-v3-type-in-v2-500`

**unity** (17 causes, 2 severe)

- **DATA LOSS** Unity loses concurrent namespace property updates (confirmed, then gone; about 1 race of 8 writers in 3) — unity 0.7.0 · hit in gravitino_unity (1), unity (1) `unity-loses-namespace-property`
- **SECURITY** Unity issues its own tokens for 24 h regardless of the Keycloak token's lifetime (1 h): a user disabled in Keycloak keeps access to Unity for up to a day — unity 0.7.0, keycloak 26.8.2 · hit in unity (1) `unity-token-lifetime`
- Unity's authz stack once answered pg_lake's namespace lookup for a user without grants with HTTP 500 instead of 403 (1 of 7 runs; no trace in the server logs) — unity 0.7.0, pg_lake 3.7.1-pg19 · intermittent · hit in unity (1) `unity-authz-lookup-500`
- Unity Catalog OSS 0.7 deletes the files of a dropped managed table only later (server.storage-cleanup.initial-delay, default P7D = 7 days), purgeRequested=true does not delete them at once — unity 0.7.0 · hit in gravitino_unity (5), unity (5) `unity-delayed-file-cleanup`
- Unity's error message for a malformed request names internal Java classes (com.fasterxml.jackson..., org.apache.iceberg.rest.requests...) — unity 0.7.0 · hit in unity (1) `unity-error-internal-names`
- Unity Catalog OSS 0.7 ignores pageSize: all tables in one page, next-page-token null — unity 0.7.0 · hit in unity (1) `unity-ignores-page-size`
- Unity refuses this invalid commit, but with 500 (ValidationException) instead of 400 — unity 0.7.0 · hit in unity (2) `unity-invalid-commit-500`
- Unity Catalog has a fixed catalog.schema.table hierarchy: no nested namespaces (400) — unity 0.7.0 · hit in gravitino_unity (4), unity (4) `unity-no-nested-namespaces`
- Unity Catalog has no /transactions/commit - no multi-table transaction (DuckDB refuses the COMMIT) — unity 0.7.0, duckdb 2.0.1 · hit in unity (2) `unity-no-transactions-commit`
- Unity Catalog OSS 0.7 does not support the Iceberg types time, timestamp_ns, timestamptz_ns, unknown, geometry and geography (400 on CREATE TABLE) — unity 0.7.0 · hit in gravitino_unity (93), unity (93) `unity-no-v3-types`
- Unity answers a lookup of table 'ns.t.<x>' with NoSuchNamespace instead of NoSuchTable - Spark and Flink then do not fall back to metadata tables, branches or tags (t.snapshots, t.branch_x, t.changes) — unity 0.7.0, spark 4.1.4+iceberg1.12.0, flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino_unity (3), unity (8) `unity-nosuchnamespace-for-metadata-tables`
- Unity Catalog OSS 0.7 renames only within a namespace (501 for a move into another one) — unity 0.7.0 · hit in gravitino_unity (1), unity (1) `unity-rename-same-namespace-only`
- Unity's SCIM DELETE only deactivates a user (active=false); it stays listed and retrievable — unity 0.7.0 · hit in unity (1) `unity-scim-delete-deactivates`
- Unity refuses a token with alg 'none' or an HS256-signed token (algorithm confusion), but with 409 Conflict ('Unsupported RSA algorithm') instead of 401 — unity 0.7.0 · hit in unity (2) `unity-token-alg-409`
- Unity answers an unknown table update action with 500 (ServiceFailureException) instead of 400 — unity 0.7.0 · hit in unity (1) `unity-unknown-update-500`
- Unity Catalog rejects a v3 type in a v2 table with HTTP 500 (ServiceFailureException) instead of 400 (Bad Request) — unity 0.7.0 · hit in gravitino_unity (3), unity (3) `unity-v3-type-in-v2-500`
- Unity Catalog OSS 0.7 offers only GET for views (no create/replace/drop endpoints) - no engine can create an Iceberg view — unity 0.7.0 · hit in gravitino_unity (6), unity (6) `unity-views-read-only`

**rustfs** (18 causes, 5 severe)

- **INCONSISTENT** RustFS deletes every previous metadata file on commit although write.metadata.delete-after-commit.enabled is off - the table's metadata-log points to missing files (a client holding older metadata fails, e.g. DuckDB's compaction during appends: 403, RustFS's answer for a missing key) — rustfs 1.0.6 · [bug report](bug-reports/15-rustfs-deletes-previous-metadata-files.md) · hit in gravitino_rustfs (1), rustfs (1) `rustfs-deletes-metadata-log`
- **SECURITY** RustFS STS silently raises DurationSeconds=60 to 900 s instead of refusing it (AWS: 400 below the minimum) - a client asking for 1-minute keys gets 15-minute keys — rustfs 1.0.6 · [bug report](bug-reports/14-rustfs-sts-duration-and-policy-wildcards.md) · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `rustfs-sts-duration`
- **SECURITY** RustFS' STS (AssumeRoleWithWebIdentity) issues temporary keys for up to 12 h on a Keycloak token that lives 1 h - the key lifetime is not bounded by the identity token: a user disabled in Keycloak keeps access for up to 12 h — rustfs 1.0.6, keycloak 26.8.2 · hit in rustfs (1) `rustfs-sts-outlives-identity-token`
- **SECURITY** RustFS catalog rights can be scoped per bucket only, not per table: a user who may read one table also loads the metadata (schema, storage location) of every other table in the warehouse - only reading the data is refused by the S3 policy — rustfs 1.0.6 · hit in rustfs (1) `rustfs-table-level-rights`
- **SECURITY** RustFS accepts write.data.path pointing into another table's directory (no overlap check); it issues no keys, so only the writer's own S3 policy keeps the tables apart — rustfs 1.0.6 · hit in gravitino_rustfs (1), rustfs (1) `rustfs-write-data-path-overlap`
- RustFS keeps hidden catalog metadata of dropped tables: a table bucket that ever held a table can neither be disabled (409 'reserved metadata') nor deleted (BucketNotEmpty), even after all visible objects are removed (checked) — rustfs 1.0.6 · hit in rustfs (1) `rustfs-bucket-not-removable`
- RustFS catalog rejects DROP with purge (purgeRequested=true -> 406), and clients may not delete table files themselves (403) - Spark, Trino and Flink DROP fails — rustfs 1.0.6 · hit in gravitino_rustfs (9), rustfs (9) `rustfs-drop-purge-refused`
- RustFS refuses DuckDB's SigV4-signed request for a nested namespace (separator %1F in the path) with 403 - PyIceberg's signed requests for the same path work — rustfs 1.0.6, duckdb 2.0.1 · hit in rustfs (1) `rustfs-duckdb-nested-namespace-403`
- RustFS cannot list table buckets (GET /iceberg/v1/buckets answers 501 Not Implemented) — rustfs 1.0.6 · hit in rustfs (1) `rustfs-list-buckets-501`
- RustFS catalog does not support format v3 (406 unsupported format-version: 3; Trino only reports 'Failed to create transaction') — rustfs 1.0.6 · hit in gravitino_rustfs (925), rustfs-nohelp (239), rustfs (871) `rustfs-no-v3`
- RustFS registers or syncs only metadata inside its own table layout (.rustfs-table/...) - a table another catalog or system wrote cannot be adopted — rustfs 1.0.6 · hit in gravitino_rustfs (1), rustfs (2) `rustfs-own-layout-only`
- RustFS keeps a hidden entry for the old name of a renamed table: after dropping the table the namespace stays 'not empty' (nothing listed) until the old name is re-created and dropped — rustfs 1.0.6 · hit in gravitino_rustfs (1), rustfs (1) `rustfs-rename-hidden-entry`
- RustFS refuses a rollback to an earlier metadata file ('last-sequence-number must not decrease') - the rollback endpoint cannot restore an earlier table state — rustfs 1.0.6 · hit in rustfs (1) `rustfs-rollback-refused`
- RustFS answers routes it has with 501 Not Implemented: GET .../maintenance/metadata, .../catalog/recovery, .../catalog/backup — rustfs 1.0.6 · hit in rustfs (2) `rustfs-routes-501`
- RustFS catalog rejects a row-level commit (equality deletes) on a table without snapshot - Flink's first upsert into a new table — rustfs 1.0.6, flink 2.3.0-iceberg1.12.0-hadoop3.5.0 `rustfs-row-level-needs-parent`
- RustFS refuses wildcards in the actions of an STS session policy ('s3:Get*': invalid action) - AWS allows them; Unity's session policies use them (the lab's STS shim expands them) — rustfs 1.0.6 · [bug report](bug-reports/14-rustfs-sts-duration-and-policy-wildcards.md) · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `rustfs-sts-policy-wildcards`
- RustFS reports a bucket that does not exist as a disabled table bucket (GET 200, enabled=false); DELETE on it answers 500 instead of 404 — rustfs 1.0.6 · hit in rustfs (1) `rustfs-unknown-bucket-200`
- RustFS catalog rejects the view schema of Spark and Flink (field id 0) - Lakekeeper and Polaris accept it — rustfs 1.0.6, spark 4.1.4+iceberg1.12.0, flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino_rustfs (2), rustfs (2) `rustfs-view-field-id-0`

**reference** (7 causes, 2 severe)

- **DATA LOSS** The Iceberg REST fixture answers concurrent POST /transactions/commit with 204 although it kept only one of them: it retries a conflicting commit on the server, finds 'Nothing to commit' and reports success (8 writers: 8 x 204, 1 change kept; the table endpoint answers 409 correctly) - DuckDB, which commits through this endpoint, loses appends — reference sha256:009f9f6897cc, duckdb 2.0.1 · [bug report](bug-reports/16-iceberg-rest-fixture-transaction-commit-drops-changes.md) · hit in reference (3) `reference-tx-commit-loses-updates`
- **SECURITY** The Iceberg REST fixture accepts every table location: inside, equal to or above another table's directory, set later, registered from foreign metadata or as write.data.path — reference sha256:009f9f6897cc · hit in reference (7) `reference-no-location-checks`
- The Iceberg REST fixture answers a malformed request (broken JSON, wrong types, an unknown update) with 500 and an HTML error page instead of 400 and an Iceberg error document — reference sha256:009f9f6897cc · hit in reference (3) `reference-client-error-500`
- The Iceberg REST fixture answers an unsupported method (PATCH) with 501 and an empty body instead of an Iceberg error document — reference sha256:009f9f6897cc · hit in reference (1) `reference-error-body-empty`
- The Iceberg REST fixture puts the Java exception class and its stack trace ('stack') into every error answer — reference sha256:009f9f6897cc · hit in reference (2) `reference-error-stack-traces`
- The Iceberg REST fixture accepts names it cannot serve afterwards: Jetty refuses an encoded '/', '\', newline or '%' in the path (400 'Ambiguous URI path separator'), and the JDBC store splits namespace names at '.' ('../../x' is listed as ''): such objects can be neither loaded nor dropped — reference sha256:009f9f6897cc · hit in reference (8) `reference-names-unaddressable`
- The Iceberg REST fixture refuses a v3 type in a v2 table with HTTP 500 (IllegalStateException) instead of 400 (Bad Request) — reference sha256:009f9f6897cc · hit in reference (3) `reference-v3-type-in-v2-500`

**gravitino** (18 causes, 8 severe)

- **INCONSISTENT** Gravitino answers creating a table named 'a\b' with 400 ('Illegal character in path' in the location it builds from the name), but the table is created anyway and is listed — gravitino 1.3.1 · hit in gravitino (1) `gravitino-backslash-table-refused-created`
- **INCONSISTENT** A switched-off Gravitino catalog (inUse=false) still serves Iceberg REST reads, and a write it answers with 500 'not in use' takes effect anyway — gravitino 1.3.1 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1) `gravitino-disabled-catalog-served`
- **INCONSISTENT** Gravitino answers a namespace name with a dot ('..', '../../escape') with 400 ('.' is reserved as the qualified-name separator) but creates the namespace anyway; it can then not be loaded or dropped (500 'Authorization failed due to system internal error'), and every top-level namespace listing of the catalog fails with 400 - Iceberg REST and Gravitino's own API - until the row is removed from the backend database; through a REST backend the namespace (and a table with such a name) is created in the fronted catalog, and Gravitino lists nothing of the catalog (or of that namespace) until it is dropped there — gravitino 1.3.1 · hit in gravitino (3), gravitino_lakekeeper (3), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (1) `gravitino-dotted-names`
- **INCONSISTENT** Through a REST backend, Gravitino answers the backend's client errors with HTTP 500: the backend's message is kept, the status is not (Lakekeeper's 400 'Namespace parts cannot contain .', RustFS's 406 for format version 3 as 'Unable to process (code: 406 ...)'); clients then treat a refusal as a server fault and may retry it — gravitino 1.3.1 · hit in gravitino_lakekeeper (3), gravitino_polaris (1), gravitino_rustfs (8), gravitino_unity (2) `gravitino-rest-backend-client-error-500`
- **INCONSISTENT** Through a REST backend, a namespace or table with a 300-character name is refused with 500 ('Failed to import schema entity to the store': too long for Gravitino's own store) - after the backend has already created it, so it exists there anyway — gravitino 1.3.1 · hit in gravitino_lakekeeper (2), gravitino_polaris (1) `gravitino-rest-backend-long-name-left`
- **INCONSISTENT** Gravitino answers creating a table with a variant column with 406 'Unsupported type: variant', but the Iceberg table is created anyway and loads over Iceberg REST; Gravitino's own API cannot load it, and no role can name it (role creation fails with the same error) — gravitino 1.3.1 · hit in gravitino (11), gravitino_lakekeeper (11), gravitino_polaris (11), gravitino_unity (11) `gravitino-variant-refused-created`
- **SECURITY** Gravitino accepts every table location - inside, equal to or above another table's directory, set later, registered from foreign metadata or as write.data.path - and vends STS keys for it that read the other table's files — gravitino 1.3.1, rustfs 1.0.6 · hit in gravitino (7) `gravitino-no-location-checks`
- **SECURITY** Through a REST backend, Gravitino checks its own privileges (disable-rest-authz = false) but passes on the storage credentials the backend vends for the forwarded token unchanged: a user with only SELECT in Gravitino gets keys that write the table's files when the backend grants more (Lakekeeper without authorization, Unity) - on its own catalogs Gravitino vends read-only keys for SELECT — gravitino 1.3.1 · hit in gravitino_lakekeeper (1), gravitino_unity (1) `gravitino-rest-backend-credentials-not-narrowed`
- Gravitino's Iceberg REST service puts the Java exception class and its stack trace ('stack') into error answers — gravitino 1.3.1 · hit in gravitino (5), gravitino_lakekeeper (5), gravitino_polaris (4), gravitino_rustfs (4), gravitino_unity (4) `gravitino-error-stack-traces`
- Gravitino has no privilege to list a schema's tables: a listing shows only the tables the user may read or modify (USE_SCHEMA alone lists none) — gravitino 1.3.1 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_rustfs (1), gravitino_unity (1) `gravitino-listing-needs-table-privilege`
- Gravitino has no /transactions/commit - no multi-table transaction (DuckDB refuses the COMMIT) — gravitino 1.3.1, duckdb 2.0.1 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (2), gravitino_unity (2) `gravitino-no-transactions-commit`
- Through a REST backend, Gravitino advertises its own Iceberg REST endpoints in /v1/config, whatever the backend offers: calls the backend does not have fail late - registering a table in front of Unity (404 'Not Found', a ServerError for PyIceberg), creating a view in front of Unity (406, 'Server does not support endpoint'), renaming a view in front of RustFS - where the backend alone is not offered them — gravitino 1.3.1 · hit in gravitino_rustfs (1), gravitino_unity (5) `gravitino-rest-backend-advertises-unsupported`
- Through a REST backend that authorizes the forwarded token itself (Polaris), a Gravitino grant is not enough: Gravitino passes the caller's token on, the backend decides again by its own grants ('Principal alice ... is not authorized'), and Gravitino does not push its grants to the backend - every right has to be kept in both — gravitino 1.3.1 · hit in gravitino_polaris (34) `gravitino-rest-backend-grants-not-forwarded`
- Through a REST backend, Gravitino retries a backend's server error for about 30 s before passing it on: a table whose metadata file is missing on Unity (Unity answers 500 at once) takes 33 s through Gravitino, clients with a 30 s timeout get no answer at all — gravitino 1.3.1 · hit in gravitino_unity (1) `gravitino-rest-backend-retries-server-errors`
- Through a REST backend, a staged table creation fails at its commit (stage-create, then the commit with assert-create - CTAS, or creating and writing a table in one transaction): Lakekeeper refuses it ('Cannot assign a new UUID', with data 'tabular locations have to be empty'), RustFS too ('cannot reassign table uuid'); the same calls work on the backend directly, on Gravitino's own catalogs and through Gravitino in front of Polaris — gravitino 1.3.1 · hit in gravitino_lakekeeper (64), gravitino_rustfs (33) `gravitino-rest-backend-staged-create`
- Gravitino accepts any value of loadTable's 'snapshots' parameter (spec: all|refs) — gravitino 1.3.1 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1) `gravitino-snapshots-param`
- Gravitino's testConnection for a lakehouse-iceberg catalog checks only the catalog backend (JDBC): a warehouse in a bucket that does not exist (or wrong S3 keys) passes as working (200) - Lakekeeper's validate-profile reports such a profile as not valid — gravitino 1.3.1 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1) `gravitino-test-connection-no-storage-check`
- Gravitino refuses a v3-only type in a format-v2 table with HTTP 500 instead of 400 — gravitino 1.3.1 · hit in gravitino (3) `gravitino-v3-type-in-v2-500`

**catalogs** (2 causes)

- The catalog does not implement server-side scan planning — lakekeeper 0.15.1, polaris 1.9.0, unity 0.7.0, rustfs 1.0.6 · hit in lakekeeper (2), polaris (2), rustfs (2), unity (2) `catalogs-no-scan-planning`
- The catalog accepts a schema change incompatible with the existing data files (no server-side check - the REST spec leaves it to the client); every read fails afterwards: 'Cannot promote long to string' — lakekeeper 0.15.1, polaris 1.9.0, unity 0.7.0, gravitino 1.3.1, reference sha256:009f9f6897cc · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), unity (2) `catalogs-no-schema-compat-check`

**pyiceberg** (19 causes, 3 severe)

- **DATA LOSS** PyIceberg writes a NULL list<struct> as [] (Polars writes through it) — pyiceberg 0.12.0 · hit in gravitino (16), gravitino_lakekeeper (16), gravitino_polaris (16), gravitino_rustfs (16), gravitino_unity (16), lakekeeper (16), polaris (16), reference (14), rustfs (14), unity (16) `pyiceberg-null-list-struct-write`
- **WRONG RESULT** PyIceberg 0.12: two concurrent upserts of the same new key both succeed - the loser's retry re-appends its row without checking for concurrent inserts (0.11.1 raised CommitFailedException) — pyiceberg 0.12.0 · upstream apache/iceberg-python#4194 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `pyiceberg-concurrent-upsert-duplicates`
- **WRONG RESULT** PyIceberg reads a NULL list<struct> as [] — pyiceberg 0.12.0 · hit in gravitino (11), gravitino_lakekeeper (11), gravitino_polaris (11), gravitino_rustfs (6), gravitino_unity (11), lakekeeper (12), polaris (12), reference (11), rustfs-nohelp (4), rustfs (6), unity (11) `pyiceberg-null-list-struct-read`
- PyIceberg gives up after its 4 default commit retries while 7 other engines commit (Unity: 409 'branch main has changed' or 'Metadata location changed concurrently') - Polars and Daft commit through it — pyiceberg 0.12.0, unity 0.7.0 · intermittent · hit in gravitino_unity (3), unity (3) `pyiceberg-commit-retries-exhausted`
- PyIceberg 0.12 deletes only copy-on-write (rewrites the data file) and ignores write.delete.mode=merge-on-read — pyiceberg 0.12.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `pyiceberg-delete-copy-on-write-only`
- PyIceberg 0.12 expire_snapshots only removes snapshots from the metadata; data files, manifests and manifest lists stay in storage — pyiceberg 0.12.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `pyiceberg-expire-keeps-files`
- PyIceberg 0.12 cannot parse the spec form geometry(OGC:CRS84) / geography(OGC:CRS84, spherical) that Iceberg 1.12 writes (it expects the CRS in quotes) - the table cannot be loaded (also by Polars and Daft) — pyiceberg 0.12.0 · [bug report](bug-reports/09-pyiceberg-unquoted-geometry-crs.md) · hit in gravitino (6), gravitino_polaris (9), polaris (19), reference (19) `pyiceberg-geo-crs-parse`
- PyIceberg 0.12 turns geometry/geography columns into binary when writing: the write stops at the schema check ('Mismatch in fields') - Polars writes through it — pyiceberg 0.12.0, gravitino 1.3.1 · hit in gravitino (4), gravitino_polaris (2) `pyiceberg-geo-write-binary`
- PyIceberg ignores the table's sort order when writing (Polars and Daft write through it) — pyiceberg 0.12.0 · hit in gravitino (3), gravitino_lakekeeper (3), gravitino_polaris (3), gravitino_rustfs (3), gravitino_unity (3), lakekeeper (3), polaris (3), reference (3), rustfs (3), unity (3) `pyiceberg-ignores-sort-order`
- PyIceberg 0.12 cannot read Avro data files (ORC works) - Polars plans through it — pyiceberg 0.12.0 · hit in gravitino (8), gravitino_lakekeeper (8), gravitino_polaris (8), gravitino_rustfs (8), gravitino_unity (8), lakekeeper (8), polaris (8), reference (8), rustfs-nohelp (8), rustfs (8), unity (8) `pyiceberg-no-avro-read`
- PyIceberg 0.12: a loaded table keeps its vended keys - no refresh via client.refresh-credentials-endpoint after expiry (Polars and Daft read held PyIceberg table objects) — pyiceberg 0.12.0 · [bug report](bug-reports/05-pyiceberg-vended-credentials-refresh.md) · hit in gravitino (3), gravitino_lakekeeper (3), gravitino_polaris (3), gravitino_unity (3), lakekeeper (3), polaris (3), unity (3) `pyiceberg-no-credential-refresh`
- PyIceberg cannot apply equality deletes - tables with Flink upserts or other equality-delete writers are unreadable (also for Polars and Daft) — pyiceberg 0.12.0 · upstream apache/iceberg#6568 · hit in gravitino (12), gravitino_lakekeeper (12), gravitino_polaris (12), gravitino_rustfs (3), gravitino_unity (12), lakekeeper (12), polaris (12), reference (12), rustfs-nohelp (3), rustfs (3), unity (12) `pyiceberg-no-equality-deletes`
- PyIceberg 0.12 cannot read geometry/geography columns — pyiceberg 0.12.0 · hit in gravitino (3), gravitino_polaris (3) `pyiceberg-no-geo-read`
- PyIceberg 0.12 cannot parse a schema with variant (Polars and Daft load tables through it) — pyiceberg 0.12.0 · hit in lakekeeper (12), polaris (12), reference (12), unity (12) `pyiceberg-no-variant`
- PyIceberg 0.12 writes only Parquet data files (refuses ORC/Avro) - Polars writes through it — pyiceberg 0.12.0 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), gravitino_rustfs (4), gravitino_unity (4), lakekeeper (4), polaris (4), reference (4), rustfs (4), unity (4) `pyiceberg-parquet-only-write`
- PyIceberg race: re-registering the remote signer on a shared s3fs client leaves a window in which requests go out unsigned (RustFS: anonymous -> 403) — pyiceberg 0.12.0 · intermittent · hit in lakekeeper (1) `pyiceberg-signer-race`
- PyIceberg's SigV4 adapter sends x-amz-content-sha256 only for empty bodies; the RustFS catalog rejects every signed POST without it (AWS S3 Tables does not) — pyiceberg 0.12.0, rustfs 1.0.6 · hit in rustfs-nohelp (411), rustfs (6) `pyiceberg-sigv4-content-sha256`
- PyIceberg 0.12: upsert compares against pre-rename physical column names — pyiceberg 0.12.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (2), polaris (1), reference (1), rustfs (1), unity (1) `pyiceberg-upsert-renamed-columns`
- PyIceberg 0.12 cannot write DATA to v3 tables (manifest writers exist only for v1/v2: 'Cannot write manifest for table version: 3'); create/read/metadata changes on v3 work - Polars and Daft write through it — pyiceberg 0.12.0 · hit in gravitino (223), gravitino_lakekeeper (221), gravitino_polaris (222), gravitino_unity (210), lakekeeper (221), polaris (221), reference (221), unity (210) `pyiceberg-v3-data-write`

**pyarrow** (1 cause)

- PyArrow cannot decode Trino's position-delete files (DELTA_LENGTH_BYTE_ARRAY) — pyarrow 25.0.1, trino 484 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), gravitino_rustfs (4), gravitino_unity (4), lakekeeper (4), polaris (4), reference (4), rustfs (4), unity (4) `pyarrow-trino-delta-length-byte-array`

**polars** (6 causes)

- Polars has no geometry type: reads geometry as Binary — polars 2.2.0 · hit in gravitino (1), gravitino_polaris (1) `polars-geo-as-binary`
- Polars has no fixed-size binary: writes it as binary (schema mismatch) and reads fixed(4) as Binary — polars 2.2.0 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), gravitino_rustfs (2), gravitino_unity (4), lakekeeper (4), polaris (4), reference (4), rustfs-nohelp (1), rustfs (2), unity (4) `polars-no-fixed`
- Polars' Parquet reader does not know the GEOMETRY/GEOGRAPHY logical type — polars 2.2.0 · hit in gravitino (2), gravitino_polaris (2) `polars-no-geo-parquet`
- Polars has no UUID type: writes it as binary (schema mismatch) and reads uuid as Binary — polars 2.2.0 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), gravitino_rustfs (2), gravitino_unity (4), lakekeeper (4), polaris (4), reference (4), rustfs-nohelp (1), rustfs (2), unity (4) `polars-no-uuid`
- Polars 2.1 sink_iceberg(engine='streaming') writes all 500 rows in one commit at the end - a batch write through the streaming engine, no incremental commits — polars 2.2.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `polars-streaming-one-commit`
- Polars Time is time64[ns]; Iceberg time is microseconds — polars 2.2.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), lakekeeper (2), polaris (2), reference (2), rustfs (1) `polars-time-ns`

**daft** (7 causes, 1 severe)

- **WRONG RESULT** Daft ignores initial-default and returns NULL for the added column — daft 0.7.26 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), unity (1) `daft-ignores-initial-default`
- Daft cannot read DuckDB's Parquet maps ('Map field must have exactly one key_value child') - also pg_lake's, which writes through DuckDB — daft 0.7.26, duckdb 2.0.1 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (3), polaris (3), reference (2), rustfs (1), unity (2) `daft-duckdb-maps`
- Daft cannot read DuckDB's nested Parquet lists ('list type must have a single child') - also pg_lake's, which writes through DuckDB — daft 0.7.26, duckdb 2.0.1 · hit in gravitino (6), gravitino_lakekeeper (6), gravitino_polaris (6), gravitino_rustfs (3), gravitino_unity (6), lakekeeper (8), polaris (8), reference (6), rustfs (3), unity (6) `daft-duckdb-nested-lists`
- Daft cannot apply v3 deletion vectors (reads the Puffin file as Parquet) — daft 0.7.26 · hit in gravitino (18), gravitino_lakekeeper (16), gravitino_polaris (18), gravitino_unity (18), lakekeeper (18), polaris (18), reference (18), unity (18) `daft-dv-as-parquet`
- Daft has no geometry type: reads geometry as Binary — daft 0.7.26 · hit in gravitino (1), gravitino_polaris (1) `daft-geo-as-binary`
- Daft ignores write.format.default and writes Parquet instead — daft 0.7.26 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (2), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (2), unity (2) `daft-ignores-write-format`
- Daft reads only Parquet data files — daft 0.7.26 · hit in gravitino (8), gravitino_lakekeeper (8), gravitino_polaris (8), gravitino_rustfs (8), gravitino_unity (8), lakekeeper (8), polaris (8), reference (8), rustfs-nohelp (8), rustfs (8), unity (8) `daft-parquet-only-read`

**duckdb** (14 causes, 1 severe)

- **DATA CORRUPTION** After a lost commit response DuckDB deletes the files of the commit Lakekeeper already applied (current snapshot -> missing manifest list) — duckdb 2.0.1, lakekeeper 0.15.1 · [bug report](bug-reports/01-duckdb-lost-commit-response-corruption.md) · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), unity (1) `duckdb-lost-response-corruption`
- DuckDB gives up a conflicting commit after 5 attempts (4 retries, as many as Iceberg Java's and PyIceberg's default) - with 4 or more writers committing at once all 5 can conflict (409) — duckdb 2.0.1 · intermittent · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), gravitino_rustfs (2), gravitino_unity (4), lakekeeper (4), polaris (4), rustfs (2), unity (4) `duckdb-commit-retries-exhausted`
- DuckDB's compaction (iceberg_rewrite_data_files) while DuckDB appends to the same table fails on the conflict (409, or 'table snapshot changed between planning and execution') - Spark and Trino finish both — duckdb 2.0.1 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `duckdb-compaction-conflict`
- DuckDB DROP TABLE does not request a purge (purgeRequested=false), so Lakekeeper keeps the data files — duckdb 2.0.1 · hit in gravitino (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `duckdb-drop-no-purge`
- DuckDB 2.0.1 iceberg_expire_snapshots only removes snapshots from the metadata; data files and manifest lists stay in storage — duckdb 2.0.1 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `duckdb-expire-keeps-files`
- DuckDB writes fixed[4] as variable-length binary — duckdb 2.0.1 · hit in gravitino (5), gravitino_lakekeeper (5), gravitino_polaris (5), gravitino_rustfs (2), gravitino_unity (5), lakekeeper (5), polaris (5), reference (5), rustfs (2), unity (5) `duckdb-fixed-as-binary`
- DuckDB 2.0 reads Iceberg geometry but not geography — duckdb 2.0.1 · hit in gravitino (1), gravitino_polaris (1), polaris (1), reference (1) `duckdb-no-geography-read`
- DuckDB: no PRIMARY KEY / identifier fields on Iceberg tables (only NOT NULL) — duckdb 2.0.1 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (2), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (2), unity (2) `duckdb-no-primary-key`
- DuckDB 2.0: iceberg_rewrite_data_files does not support v3 tables - no compaction, and a data file cannot be taken out of a v3 table (a full DELETE only adds deletion vectors) — duckdb 2.0.1 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), gravitino_unity (4), lakekeeper (4), polaris (4), reference (4), unity (4) `duckdb-no-v3-rewrite`
- DuckDB reads only Parquet data files — duckdb 2.0.1 · hit in gravitino (8), gravitino_lakekeeper (8), gravitino_polaris (8), gravitino_rustfs (8), gravitino_unity (8), lakekeeper (8), polaris (8), reference (8), rustfs (8), unity (8) `duckdb-parquet-only-read`
- DuckDB 2.0.1 writes only Parquet data files (refuses ORC/Avro; 2.0.0 wrote Parquet instead of the requested format) — duckdb 2.0.1 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (2), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (2), unity (2) `duckdb-parquet-only-write`
- DuckDB derives the SigV4 service from an AWS host name - SigV4 catalogs on other hosts (RustFS) cannot be attached — duckdb 2.0.1, rustfs 1.0.6 · hit in rustfs-nohelp (344), rustfs (12) `duckdb-sigv4-aws-host`
- The 'manifests' metadata table of Spark and Trino crashes on DuckDB-written v3 manifests (null partition summaries) — duckdb 2.0.1, trino 484, spark 4.1.4+iceberg1.12.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), unity (1) `duckdb-v3-null-partition-summaries`
- DuckDB only runs views that have a 'duckdb' SQL representation — duckdb 2.0.1 · hit in gravitino (3), gravitino_lakekeeper (3), gravitino_polaris (3), gravitino_rustfs (1), lakekeeper (3), polaris (3), reference (3), rustfs (1) `duckdb-view-dialect`

**spark** (7 causes, 1 severe)

- **WRONG RESULT** Spark's catalog cache (cache-enabled, 30 s) still holds the dropped table after a re-create under the same name: Spark reads its old rows, or fails on its deleted files once they are purged — spark 4.1.4+iceberg1.12.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `spark-cache-recreated-table`
- Spark's Iceberg catalog caches loaded tables (cache-enabled defaults to true, 30 s expiry): a commit, a renamed column or a new partition spec by another client stays invisible in the same session until the cache expires or REFRESH TABLE — spark 4.1.4+iceberg1.12.0 · hit in gravitino (6), gravitino_lakekeeper (6), gravitino_polaris (6), gravitino_rustfs (3), gravitino_unity (6), lakekeeper (6), polaris (6), reference (6), rustfs (3), unity (6) `spark-catalog-cache`
- Spark represents fixed(4) as variable-length binary — spark 4.1.4+iceberg1.12.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `spark-fixed-as-binary`
- Spark has no TIME type — spark 4.1.4+iceberg1.12.0 · hit in gravitino (12), gravitino_lakekeeper (12), gravitino_polaris (12), gravitino_rustfs (7), lakekeeper (13), polaris (13), reference (12), rustfs (7) `spark-no-time`
- Spark has no nanosecond timestamps — spark 4.1.4+iceberg1.12.0 · hit in gravitino (9), gravitino_lakekeeper (9), gravitino_polaris (9), lakekeeper (9), polaris (9), reference (9) `spark-no-timestamp-ns`
- Spark represents Iceberg uuid as string — spark 4.1.4+iceberg1.12.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `spark-uuid-as-string`
- Iceberg views store the creator's catalog name (default-catalog); Spark cannot resolve a view created in Trino catalog 'lk_sts'/'pol_sts' — spark 4.1.4+iceberg1.12.0, trino 484 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), lakekeeper (1), polaris (1), reference (1), rustfs (1) `spark-view-default-catalog`

**trino** (9 causes, 1 severe)

- **WRONG RESULT** Trino returns the ts=NULL row for 'ts < <day boundary>' on a day(ts)-partitioned table without null_value_counts (pg_lake writes none) — trino 484, pg_lake 3.7.1-pg19 · [bug report](bug-reports/03-trino-null-partition-wrong-result.md) · hit in polaris (1) `trino-null-partition-pruning`
- Trino Iceberg writes only in autocommit: no multi-statement transactions, so no atomic overwrite and no multi-table transaction — trino 484 · hit in gravitino (8), gravitino_lakekeeper (8), gravitino_polaris (8), gravitino_rustfs (5), gravitino_unity (8), lakekeeper (8), polaris (8), reference (8), rustfs (5), unity (8) `trino-autocommit-only`
- Trino table_changes reports copy-on-write carry-over rows as delete + insert (no net changes) — trino 484 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `trino-changelog-carry-over`
- Trino represents fixed(4) as varbinary — trino 484 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `trino-fixed-as-varbinary`
- Trino reports a refused table load (403) only as 'Failed to load table' / 'Failed to load view' / 'Cannot obtain metadata' - the refusal is not shown — trino 484 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), polaris (1), rustfs (1), unity (1) `trino-hides-load-refusal`
- Trino reports a refused write (read-only keys) only as 'Error committing write to Parquet file' - the S3 AccessDenied is not shown — trino 484 · intermittent · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), rustfs (1), unity (1) `trino-hides-write-refusal`
- Trino 484 cannot create branches ('This connector does not support creating branches'; no SQL for it) - so no write-audit-publish — trino 484 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), gravitino_rustfs (2), gravitino_unity (4), lakekeeper (4), polaris (4), reference (4), rustfs (2), unity (4) `trino-no-create-branch`
- Trino 484 reads Iceberg 'unknown' columns but cannot write them — trino 484 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), lakekeeper (1), polaris (1), reference (1) `trino-no-unknown-write`
- Trino only runs views that have a 'trino' SQL representation — trino 484 · hit in gravitino (3), gravitino_lakekeeper (3), gravitino_polaris (3), gravitino_rustfs (1), lakekeeper (3), polaris (3), reference (3), rustfs (1) `trino-view-dialect`

**flink** (19 causes, 5 severe)

- **DATA LOSS** Flink truncates TIME to seconds when writing — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (14), gravitino_lakekeeper (14), gravitino_polaris (14), gravitino_rustfs (7), lakekeeper (14), polaris (14), reference (12), rustfs-nohelp (3), rustfs (6) `flink-time-seconds-write`
- **INCONSISTENT** Flink STATEMENT SET is not atomic across tables: A stays committed when B's commit fails — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · intermittent · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `flink-statement-set-not-atomic`
- **WRONG RESULT** Flink (Iceberg 1.12) ignores initial-default and returns NULL — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), unity (1) `flink-ignores-initial-default`
- **WRONG RESULT** Flink 2.3 parses the literal CAST('-0.0' AS DOUBLE) as +0.0 and stores that (-CAST(0.0 AS DOUBLE) keeps the sign) — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (16), gravitino_lakekeeper (16), gravitino_polaris (16), gravitino_rustfs (8), gravitino_unity (16), lakekeeper (16), polaris (16), reference (14), rustfs-nohelp (3), rustfs (7), unity (16) `flink-neg-zero-literal`
- **WRONG RESULT** Flink 2.3 reads TIME with millisecond precision only (Iceberg time has microseconds) — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (8), gravitino_lakekeeper (8), gravitino_polaris (8), gravitino_rustfs (5), lakekeeper (9), polaris (9), reference (8), rustfs (5) `flink-time-millis-read`
- Flink 2.3 SQL cannot produce a FLOAT NaN/Infinity: CAST('NaN' AS FLOAT) and even CAST(CAST('NaN' AS DOUBLE) AS FLOAT) fail (the cast goes through BigDecimal); DOUBLE NaN works — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `flink-float-nan`
- Flink ignores the table's sort order when writing — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `flink-ignores-sort-order`
- Flink SQL writes/reads branches (OPTIONS('branch')) but cannot create tags — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `flink-no-create-tag`
- Iceberg's Flink sink does not support DELETE (no SupportsRowLevelDelete) - so Flink cannot create or compact delete files — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (6), gravitino_lakekeeper (6), gravitino_polaris (6), gravitino_rustfs (3), gravitino_unity (6), lakekeeper (6), polaris (6), reference (6), rustfs (3), unity (6) `flink-no-delete`
- Flink has no geometry/geography type — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (5), gravitino_polaris (5), polaris (4), reference (4) `flink-no-geo`
- Flink catalogs have single-level databases: no nested Iceberg namespaces — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `flink-no-nested-namespaces`
- Flink SQL has no procedures (FlinkCatalog: getProcedure is not implemented) - no compaction, maintenance or rollback in SQL, only the Java TableMaintenance API — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (12), gravitino_lakekeeper (12), gravitino_polaris (12), gravitino_rustfs (6), gravitino_unity (12), lakekeeper (12), polaris (12), reference (12), rustfs (6), unity (12) `flink-no-procedures`
- Flink writes and reads branches (OPTIONS('branch')) but has no SQL to publish one to main — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `flink-no-publish-branch`
- Iceberg's Flink sink does not support UPDATE (no SupportsRowLevelUpdate) — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `flink-no-update`
- Flink cannot INSERT a VARIANT value into an Iceberg variant column — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in lakekeeper (1), polaris (1), reference (1), unity (1) `flink-no-variant-insert`
- Flink STATEMENT SET commits every table separately (no /transactions/commit) — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), rustfs (1), unity (1) `flink-statement-set-per-table`
- Flink maps Iceberg unknown to its NULL type: such a column can neither be written nor read — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), lakekeeper (4), polaris (4), reference (4) `flink-unknown-type`
- Flink has no UUID type: reads uuid as BINARY(16) — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), reference (2), rustfs (1), unity (2) `flink-uuid-as-binary`
- Flink SQL Gateway cannot serialize VARIANT (and VARIANT cannot be cast to STRING) — flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in lakekeeper (3), polaris (3), reference (3), unity (3) `flink-variant-gateway`

**pg_lake** (29 causes, 4 severe)

- **DATA LOSS** pg_lake reports COMMIT success although the REST catalog refused the commit (Unity and Gravitino have no /transactions/commit: 404; a concurrent commit: 409, which it does not retry either): Postgres shows the rows, the catalog and all other engines do not - writes are silently lost, nothing logged — pg_lake 3.7.1-pg19 · [bug report](bug-reports/02-pg_lake-silent-commit-divergence.md) · hit in gravitino (94), gravitino_lakekeeper (68), gravitino_polaris (94), gravitino_rustfs (68), gravitino_unity (93), lakekeeper (3), polaris (3), unity (93) `pglake-ignores-refused-commit`
- **DATA LOSS** pg_lake truncates timestamp_ns to microseconds and maps the type to a microsecond timestamp (Postgres timestamps have microsecond precision) — pg_lake 3.7.1-pg19 · hit in gravitino (5), gravitino_lakekeeper (5), gravitino_polaris (5), lakekeeper (5), polaris (5) `pglake-timestamp-ns-truncated`
- **WRONG RESULT** pg_lake does not see commits by other clients to a table it created (keeps its own metadata pointer; attached tables do follow - test_pglake.py) — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-no-external-commits`
- **WRONG RESULT** pg_lake resolves struct fields by name, not by field id - a renamed struct field reads as NULL; with no field name in common the read fails ('STRUCT to STRUCT cast must have at least one matching member') — pg_lake 3.7.1-pg19 · hit in gravitino (11), gravitino_lakekeeper (11), gravitino_polaris (11), gravitino_rustfs (7), gravitino_unity (11), lakekeeper (11), polaris (11), unity (11) `pglake-struct-by-name`
- pg_lake ANALYZE only collects Postgres planner statistics; nothing is written to the Iceberg metadata — pg_lake 3.7.1-pg19 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), unity (1) `pglake-analyze-no-iceberg-stats`
- pg_lake ignores write.delete.mode=merge-on-read: deleting 1 of 3 rows rewrites the data file (with 1 of 97 rows it writes a position delete file - test_maintenance) — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-delete-copy-on-write`
- pg_lake 3.7: after VACUUM expired snapshots, its own next INSERT into the table fails - the same manifest path is put into lake_engine.deletion_queue twice — pg_lake 3.7.1-pg19 · intermittent · hit in lakekeeper (1), polaris (1) `pglake-deletion-queue-duplicate`
- pg_lake DROP TABLE does not request a purge (no purgeRequested) - files stay on catalogs that only delete on purge (Polaris; Lakekeeper's hard delete hides it) — pg_lake 3.7.1-pg19, polaris 1.9.0 · hit in polaris (1) `pglake-drop-no-purge`
- pg_lake VACUUM expiry is unreliable: the old snapshot stays, or it is removed but its data file stays in storage — pg_lake 3.7.1-pg19 · intermittent · hit in lakekeeper (1), polaris (1) `pglake-expiry-unreliable`
- pg_lake represents fixed(4) as bytea — pg_lake 3.7.1-pg19 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_rustfs (1), gravitino_unity (2), lakekeeper (2), polaris (2), unity (2) `pglake-fixed-as-bytea`
- pg_lake ignores a refused /v1/config (Lakekeeper hides the warehouse with 404) and sends its next request with a wrong prefix - the refusal shows as HTTP 400 'WarehouseIdIsNotUUID' instead of a permission error — pg_lake 3.7.1-pg19, lakekeeper 0.15.1 · [bug report](bug-reports/06-pg_lake-ignores-config-404.md) · hit in lakekeeper (1) `pglake-ignores-config-404`
- pg_lake ignores the catalog's refusal of CREATE TABLE (Unity refuses TIME; Gravitino in front of Lakekeeper or RustFS refuses its staged create): it reports success, the table exists only in Postgres — pg_lake 3.7.1-pg19, unity 0.7.0 · hit in gravitino_lakekeeper (28), gravitino_rustfs (28), gravitino_unity (2), unity (2) `pglake-ignores-refused-create`
- pg_lake ignores the table's sort order when writing — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-ignores-sort-order`
- pg_lake ignores write.target-file-size-bytes (uses its own pg_lake_table.target_file_size_mb, default 512 MB) — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-ignores-target-file-size`
- pg_lake 3.7 cannot read v3 deletion vectors — pg_lake 3.7.1-pg19 · hit in gravitino (18), gravitino_lakekeeper (16), gravitino_polaris (18), gravitino_unity (18), lakekeeper (18), polaris (18), unity (18) `pglake-no-dv-read`
- pg_lake VACUUM only deletes files it tracked itself (deletion queue), not foreign orphan files — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-no-foreign-orphans`
- pg_lake 3.7 does not support geometry/geography — pg_lake 3.7.1-pg19 · hit in gravitino (5), gravitino_polaris (5), polaris (4) `pglake-no-geo`
- pg_lake 3.7 refuses tables whose columns declare an initial-default — pg_lake 3.7.1-pg19 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_unity (1), lakekeeper (1), polaris (1), unity (1) `pglake-no-initial-default`
- pg_lake 3.7 does not support MERGE on Iceberg tables — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-no-merge`
- pg_lake: no PRIMARY KEY on Iceberg tables (foreign tables) — pg_lake 3.7.1-pg19 · hit in gravitino (1), gravitino_lakekeeper (1), gravitino_polaris (1), gravitino_rustfs (1), gravitino_unity (1), lakekeeper (1), polaris (1), reference (1), unity (1) `pglake-no-primary-key`
- pg_lake 3.7 cannot rename Iceberg tables in a REST catalog — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-no-rename`
- pg_lake cannot evolve a struct column: ALTER TYPE (add/rename/drop attribute) on a composite type used by an Iceberg table is refused — pg_lake 3.7.1-pg19 · hit in lakekeeper (1), polaris (1) `pglake-no-struct-evolution`
- pg_lake/pgduck cannot read timestamptz_ns — pg_lake 3.7.1-pg19 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), lakekeeper (4), polaris (4) `pglake-no-timestamptz-ns`
- pg_lake does not support Iceberg type 'unknown' — pg_lake 3.7.1-pg19 · hit in gravitino (4), gravitino_lakekeeper (4), gravitino_polaris (4), lakekeeper (4), polaris (4) `pglake-no-unknown`
- pg_lake does not support Iceberg type 'variant' — pg_lake 3.7.1-pg19 · hit in lakekeeper (4), polaris (4), unity (4) `pglake-no-variant`
- pg_lake sends partition fields with both 'source-id' and 'source-ids'; Lakekeeper 0.15 rejects that (HTTP 422) and pg_lake ignores the error - partitioned tables and partition evolution fail — pg_lake 3.7.1-pg19, lakekeeper 0.15.1 · [bug report](bug-reports/08-pg_lake-partition-field-source-id-and-source-ids.md) · hit in lakekeeper (9) `pglake-partition-source-ids`
- pg_lake reads every data file as Parquet: ORC/Avro fail with a misleading 'No magic bytes found' — pg_lake 3.7.1-pg19 · hit in gravitino (8), gravitino_lakekeeper (8), gravitino_polaris (8), gravitino_rustfs (8), gravitino_unity (8), lakekeeper (8), polaris (8), unity (8) `pglake-reads-all-as-parquet`
- pg_lake rejects v3 tables whose manifests still list a removed (status DELETED) deletion vector, although no live DV exists - also when the reference implementation wrote the table — pg_lake 3.7.1-pg19 · hit in gravitino (3), gravitino_lakekeeper (3), gravitino_polaris (3), gravitino_unity (3), lakekeeper (3), polaris (3), unity (3) `pglake-rejects-deleted-dv`
- pg_lake always puts a prefix segment into REST paths (/v1/<catalog_name>/...); a catalog whose /v1/config sets no prefix (optional in the REST spec - the Iceberg REST fixture) gets requests for paths it does not have, pg_lake cannot use it at all — pg_lake 3.7.1-pg19, reference sha256:009f9f6897cc · [bug report](bug-reports/17-pg_lake-requires-rest-prefix.md) · hit in reference (780) `pglake-requires-rest-prefix`

**engines** (4 causes)

- Spark and Flink refuse a table re-created under the same name while their catalog cache holds the dropped one ('Table UUID does not match') - safe, but the new table is not usable until the cache expires — spark 4.1.4+iceberg1.12.0, flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (3), gravitino_lakekeeper (3), gravitino_polaris (3), gravitino_rustfs (3), gravitino_unity (3), lakekeeper (3), polaris (3), reference (3), rustfs (3), unity (3) `engines-cache-recreated-uuid-mismatch`
- Iceberg Java (Spark, Flink) refreshes its OAuth2 session token by token exchange authenticated only with the old token; Keycloak refuses it (invalid_client), so a session that outlives the token (1 h) fails with 'Authentication token is expired' — spark 4.1.4+iceberg1.12.0, flink 2.3.0-iceberg1.12.0-hadoop3.5.0, keycloak 26.8.2 · hit in gravitino (2), gravitino_lakekeeper (2), gravitino_polaris (2), gravitino_unity (2) `engines-java-oauth-refresh-keycloak`
- Manifests are written without the 'schema-id' key-value metadata the spec requires from v2 on: PyIceberg (also for Polars and Daft) and Iceberg Java, the reference implementation (Spark, Trino, Flink) — pyiceberg 0.12.0, spark 4.1.4+iceberg1.12.0, trino 484, flink 2.3.0-iceberg1.12.0-hadoop3.5.0 · hit in gravitino (9), gravitino_lakekeeper (9), gravitino_polaris (9), gravitino_rustfs (6), gravitino_unity (9), lakekeeper (9), polaris (9), reference (9), rustfs (6), unity (9) `engines-manifest-without-schema-id`
- DuckDB, Daft and Polars' native reader do not support Iceberg REST remote signing — duckdb 2.0.1, daft 0.7.26, polars 2.2.0 · hit in lakekeeper (5) `engines-no-remote-signing`

<!-- gap-register:end -->
