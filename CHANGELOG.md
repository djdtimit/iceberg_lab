# Changelog

What changed in the lab and what each run found, by date. Current findings by topic: [docs/findings.md](docs/findings.md); the test design: [docs/test-architecture.md](docs/test-architecture.md).

## Lab changes 2026-11-22 (3): one registry, adapters, new versions show up

- **Framework is a package** (`src/iceberg_lab/lab/`): gaps, dependent results, inventory,
  capabilities, reporting and manifest moved out of `tests/conftest.py` and `tests/lab/`;
  conftest keeps the fixtures and re-exports the helpers.
- **Capability registry** (`capabilities.OFFERED`, `PROVEN_BY`, `require`): every "not offered"
  skip of an engine goes through it - checked against the live API or proven by a named test.
  Removed the remaining hard-coded engine skips (pg_lake as writer, upsert, session time zone,
  access-delegation combinations). `test_capability_registry.py` fails on a new
  `if engine == ...: pytest.skip(...)`.
- **Engine steps in the adapters**: `append_arrow` (PyIceberg/Polars/Daft), `set_session_zone`,
  `PgLakeEngine.create_owned_table` replace the copies in test_evolution, test_operations,
  test_timezones and test_types. The Trino, Flink and pg_lake parts were moved verbatim and have
  not run in this setup yet.
- **New features cannot pass unseen**: Renovate config (one PR per engine/catalog, incl. Iceberg
  Java runtime, Hadoop, pg_lake ref); every engine API name is used by a test or classified
  (`docs/api/engine-classified.toml`: 27 ignored, 79 untested - the open list, e.g.
  `pl.delete_iceberg`, pg_lake `vacuum`, Spark `rewrite_table_path`); the Iceberg spec
  (REST endpoints, types) is recorded per release and every entry needs a test. Its first run
  found five 1.12 endpoints without a test - now in `test_rest_conformance.py`: Lakekeeper signs
  (`/sign` works), functions, register-view and unregister are not offered on Lakekeeper.
- **DuckDB 2.0.1 checked** (Lakekeeper; `VERIFIED` updated): the known gaps "concurrent MERGE
  duplicates the key" (bug report 12, do not file) and "compaction keeps the position delete
  files" (v2) report `[gap fixed]` and are removed. New in the inventory: `iceberg_expire_snapshots`
  (tested: only removes snapshots from the metadata, files stay - a gap like PyIceberg's; on v3 no
  data file can be taken out of a table at all), `iceberg_partitions`, `iceberg_schemas` (untested).
- Polars `sink_iceberg(engine='streaming')` writes, but in one commit at the end - documented gap.
- The three claims the API check refuted are tested now: Polars adds a column while writing
  (`sink_iceberg(schema_mode="merge")`, v2; v3 has PyIceberg's write gap), DuckDB sets namespace
  properties (`set_iceberg_schema_properties`), PyIceberg creates views (`Catalog.create_view`,
  one representation per dialect - DuckDB and Spark run them).
- Fixed: the "incremental reads" API check matched DuckDB's `iceberg_rollback_to_snapshot`;
  Polars' `sink_iceberg` got no storage options (failed on the EC2 metadata endpoint).

## Lab changes 2026-11-22 (2): honest counts, checked skips, new tests

- **"Not testable" counts at its cause** (`reporting.blocked_by`, report and dashboard): a read
  whose table the writer could not produce no longer lowers the reader's coverage; it shows as
  "blocks N cases of other engines" at the writer (or the catalog). With the data of 2026-11-20
  the coverage rises from 63 % to 80 % (Lakekeeper), 64 → 80 % (Polaris), 57 → 76 % (Unity),
  32 → 45 % (RustFS); reliability is unchanged. The dashboard shows planned vs. ran cases.
- **"Not offered" is checked** (`iceberg_lab.lab.capabilities`, `CAPABILITY_API`, `NOT_OFFERED_EXEMPT`):
  every skip of that kind first checks the engine's live API; tries where cheap (Flink MERGE,
  Lakekeeper generic tables, Polars `sink_iceberg`). Found four outdated claims: Polars
  incremental reads (now tested), PyIceberg `create_view`, DuckDB namespace properties, Polars
  adding columns while writing - the last three fail until tested.
- **New tests**: special floats (NaN, ±Inf, -0.0) in the type matrix and `test_nan_metrics`
  (no NaN in column bounds); `test_timezones.py` (session zones Kathmandu, Berlin at the DST
  switch, Los Angeles 1969); nested schema evolution in `test_evolution.py`;
  `test_maintenance_cross_write` (every engine appends after another engine's maintenance).
  pg_lake takes part in the nested evolution with its own DDL instead of being skipped.
- **Checked live** (Lakekeeper v0.15.1): every spelling of `geometry`/`geography` is refused with
  422 ("untagged enum SchemaEnum") - Lakekeeper cannot parse a schema with these types.

## Versions 2026-11-22

- DuckDB 2.0.0 → 2.0.1 (bug-fix release of 2026-11-20: newer `iceberg` and `httpfs` extensions,
  Parquet min/max kept for all-NULL row groups; no change named for commits, conflicts or MERGE).
- OpenFGA v1.22.1 → v1.23.0 (authorization backend of `lakekeeper-authz`).
- The manifest now records the installed DuckDB extension versions: `INSTALL iceberg` fetches the
  build that belongs to the DuckDB version, so the package version alone does not say which
  Iceberg code ran.

## Lab changes 2026-11-22: comparability and overview

- **Run manifest** (`src/iceberg_lab/lab/manifest.py`): every result row names the run that produced it
  (`run`); each report directory gets `manifest.json` and `runs.json` with git commit, package
  versions, declared images and the digests of the running lab containers. `run_all.sh` gives all
  pytest processes of a run one id (`LAB_RUN_ID`).
- **Themes** (`reporting.THEMES`): the compared areas are grouped into eight themes; the report adds
  reliability per theme and the mean over the themes (each theme counts equally - the type matrix
  alone is about a third of all cases). Self-test: every compared area has exactly one theme.
- **Versioned reference** (`results/`, `scripts/export_reference.py`): the latest full run per
  catalog, one test per line, so `git diff results/` shows what changed. Seeded from the runs up to
  2026-11-20 (converted to the current RustFS model: `rustfs` with help, `rustfs-nohelp` without).
- **Offline self-tests**: `uv run pytest -m offline` runs the lab self-tests without catalog and
  engines (the session bootstrap is skipped when only `offline` tests run) - the documented
  "runs without catalog or engines" was not true before.
- **Dashboard** rebuilt around the overview: catalogs at a glance, engines × catalogs, themes ×
  engines, severe gaps and new failures first, biggest causes, then details; reference tables
  folded away. The data builder adds themes, severity and run ids and drops rows the page never
  shows.
- **Docs**: the README is split into `docs/setup.md`, `docs/tests.md`, `docs/findings.md` and this
  changelog; dead references fixed (`behind_gaps_doc.py`, `docs/iceberg-maintenance.md`); Envoy
  pinned to `v1.40.2` (was `v1.40-latest`, same digest); pg_lake's version naming clarified
  (release v3.7.0, extension 3.6).

## Findings by run

**New base cases and authorization on all catalogs (2026-11-15)**

- Spark and pg_lake do not see commits of other clients: Spark keeps loaded tables in its cache
  (`cache-enabled`, 30 s), pg_lake does not follow for tables it created (`sees_external_commit`).
- PyIceberg and pg_lake ignore `write.delete.mode=merge-on-read` and write DELETE as copy-on-write
  (`delete_merge_on_read`).
- DuckDB and pg_lake: no primary key / identifier field on Iceberg tables (`identifier_fields`).
- Trino: no write transactions, so neither several changes in one commit nor ROLLBACK.
- RustFS catalog: after a rename an invisible entry remains under the old name, and the namespace
  cannot be dropped (`test_drop_namespace_after_rename`). Catalog rights exist per action
  (`admin:GetTable`, `admin:CreateTable`, `admin:CommitTable`, ... – the docs name only some), but
  only per bucket (= warehouse), not per namespace or table; data access per table through S3 path
  rights.
- Polaris requires `TABLE_FULL_METADATA` for `/transactions/commit`; with a write grant
  (`TABLE_WRITE_DATA`) DuckDB therefore cannot write.
- Trino hides the reason of a refusal ("Failed to load table", "Cannot obtain metadata", "Error
  committing write to Parquet file"); so does pg_lake (Lakekeeper: `WarehouseIdIsNotUUID`, Unity:
  HTTP 500 instead of 403).
- Unity: deletes the files of dropped managed tables only after 7 days
  (`server.storage-cleanup.initial-delay`); vended credentials live a fixed 1 h; pg_lake reports a
  refused CREATE TABLE as success (`test_create_refused_by_catalog_is_an_error`).

**Update to the latest versions (2026-11-16)**

- Fixed: Spark `int → long` (Iceberg 1.12), Trino reads `unknown` (484), Flink creates and reads
  views (2.3).
- New: pg_lake + Lakekeeper 0.15 – no more partitioned tables (see above, `docs/bug-reports/08`);
  PyIceberg 0.12 cannot load Polaris tables with geo columns (`docs/bug-reports/09`); the RustFS
  catalog rejects Flink views like Spark views (`view schema field id must be positive`).
- Lab operation: tens of thousands of orphaned test objects make RustFS so slow that large read and
  write tests time out (bucket listing ~18 s with 20,000 objects) – `run_all.sh` therefore cleans up
  first (`scripts/cleanup_storage.py`). A Flink TaskManager that loses its heartbeat (overloaded or
  suspended machine) exits and is not restarted; jobs then wait forever for a slot. The Flink adapter
  now checks this and aborts at once.

**Security, storage faults, lab adapters (2026-11-17)**

- Login: all four catalogs reject forged (`alg: none`, HS256 confusion), tampered, expired and
  foreign tokens or wrong SigV4 signatures. Unity answers `alg: none` and HS256 with 409 instead of
  401; Lakekeeper still accepts expired tokens for ~60 s (clock leeway). Unity issues its own tokens
  for 24 h, regardless of the 1 h Keycloak token: a user blocked in Keycloak keeps access for up to a
  day.
- Storage location: no catalog lets a table reach foreign files through its location, a moved
  location, foreign metadata or `write.data.path`; RustFS accepts `write.data.path` into another
  table without a check (only the S3 policy separates them).
- No secrets in responses; no stack traces. Unity names internal Java classes and answers an
  unknown table change with 500.
- Names: RustFS and Unity very strict, Polaris medium, Lakekeeper lenient (a table named `..` is
  then unreachable for HTTP clients). PyIceberg double-encodes `parent` (`docs/bug-reports/10`).
- Privilege escalation: none of the catalogs allows it. An earlier RustFS finding was a lab
  misconfiguration (`admin:*`); RustFS has fine-grained table actions, but only per bucket.
- Storage faults: Lakekeeper and Polaris report errors cleanly (Polaris: "commit state unknown"),
  no table breaks, both recover. Lakekeeper loads tables without storage (metadata in the
  database); Polaris needs storage for that.
- Lab adapters: the STS adapter for Unity did not check callers and dropped deny rules – fixed
  (signature check, session policy required), checked with a real Unity request.

**File formats and catalog consistency (2026-11-17)**

- Only Spark, Trino and Flink write ORC and Avro. **DuckDB and Daft ignore `write.format.default`
  and silently write Parquet**; PyIceberg/Polars refuse. Reading: Spark, Trino, Flink everything;
  PyIceberg/Polars ORC, no Avro; DuckDB, Daft, pg_lake only Parquet – pg_lake reads every file as
  Parquet and reports a misleading "No magic bytes found".
- **Lakekeeper, Polaris and Unity accept commits that make a table unreadable** (incompatible type
  change, reused field ID): the REST spec leaves the check to the client, so a faulty client can
  destroy the table for everyone. The RustFS catalog rejects both. Unity rejects two further
  invalid commits with 500 instead of 400.
- Lakekeeper 0.15.0 lost concurrent namespace property updates (7 of 8); 0.15.1 fixes this – the
  test reported it as "gap fixed" after the update. Unity loses the same kind of update occasionally
  (about every third run, one of eight) – kept as an intermittent gap.

**Roles, single privileges, web UI, neutral read source (2026-11-19)**

- **Role from the Keycloak token**: works only on RustFS (the token names the IAM policy).
  Lakekeeper 0.15 takes token roles only with the Cedar authorizer, not with OpenFGA; Polaris
  activates a token role only if it is also assigned to the principal in Polaris – Keycloak can only
  narrow roles there, not grant them (cf. apache/polaris#2373). Unity OSS has no roles.
- **Single privileges**: Lakekeeper and Polaris cleanly separate see, list, create, alter, drop and
  grant on (each privilege opens exactly its action); with `TABLE_READ_PROPERTIES` Polaris gives
  metadata but no storage credentials. RustFS likewise, only granting on is IAM administration
  without restriction to one table. **Unity OSS 0.7 knows none of these privileges except
  `CREATE TABLE`** – only the owner may alter, drop and grant on.
- **Web UI**: Lakekeeper (`/ui/`) and RustFS (console) serve it from the server, Polaris (Polaris
  Console) and Unity as separate containers. On all four, the data API behind it refuses anonymous
  requests.
- **Neutral read source** (Iceberg Java core writes every state): all engines read partitions,
  evolution and maintenance results correctly; the reader gaps of the cross-read matrix lie with the
  reader (PyIceberg/Polars/Daft without equality deletes, Daft and pg_lake without deletion vectors).

**Full run with the latest versions (2026-11-20)**

- **RustFS deletes every previous metadata file on commit** (`docs/bug-reports/15`), although
  `write.metadata.delete-after-commit.enabled` is off: the `metadata-log` points to missing files.
  Lakekeeper, Polaris and Unity keep them. With the lab's help (DuckDB reaches RustFS) this breaks DuckDB's compaction while
  another client appends (it re-reads its base metadata: 403, RustFS's answer for a missing key).
- RustFS answers a bucket listing on the table bucket slowly even with `max-keys=1` (~9 s after
  cleanup) – the SigV4 refusal test now uses `GetBucketLocation`, which answers at once.
- With the lab's help, RustFS refuses DuckDB's signed request for a nested namespace (separator `%1F`
  in the path) with 403; PyIceberg's signed requests for the same path work.
- Since Lakekeeper 0.15.1, pg_lake shows a hidden warehouse's refusal correctly
  (`docs/bug-reports/06` no longer reproduces; on Unity too). Spark reports Lakekeeper's hiding
  refusal as `TABLE_OR_VIEW_NOT_FOUND`; Trino on Unity also as "Failed to load view".
- Unity ran the engine tests 2–4 times slower than Polaris (every S3 access goes through the
  AWS-emulating gateway); one DuckDB read hit its HTTP timeout once (not reproducible).
- All bug-report findings except 04 (fixed), 06 and 11 (withdrawn) reproduced again.

**Keycloak, REST conformance, engine semantics, storage (2026-11-18)**

- **Keycloak on all four catalogs**: Unity now checks the audience (previously
  `server.audiences=*`), RustFS issues temporary credentials to Keycloak identities through STS
  (exactly the rights of the policy from the token), Polaris accepts Keycloak tokens next to its own
  (`mixed`). All four reject expired and foreign tokens and tokens without their audience. RustFS
  also rejects tokens with additional audiences (strict OIDC check).
- **Outage of the token endpoint**: running sessions (PyIceberg, Spark) keep working on all three
  token catalogs, new ones fail at once and recognisably, then recovery.
- **REST conformance**: Polaris, RustFS and Unity without deviation on the rules checked (Unity 0.7
  fixed the reported deviations #1800/#1835/#1840; it does not offer views and
  `/transactions/commit`); Lakekeeper see above. None offers scan planning.
- **Concurrent upserts of the same new key**: Spark and Trino reject one (MERGE is `serializable`
  by default); **PyIceberg 0.12 and DuckDB write the key twice** (PyIceberg:
  apache/iceberg-python#4194; DuckDB: no conflict detected) – on all catalogs.
- **Re-creating with an open session**: PyIceberg, Polars, Daft, DuckDB and Trino read and write the
  new table, pg_lake reads it (it does not write foreign tables). **Spark reads the dropped table from
  its 30 s catalog cache** (old rows or errors on its deleted files); Spark writes and Flink refuse
  with "Table UUID does not match" (safe, but unusable until the cache expires).
- **Branch isolation**: all eight engines read `main` and write to `main` without touching the
  branch.
- **Files per specification**: field IDs in all Parquet files. Manifests: all required fields
  except `schema-id` – it is missing with PyIceberg/Polars/Daft and with Spark/Trino/Flink (Iceberg
  Java, the reference implementation, does not write it although the spec requires it from v2 on);
  DuckDB and pg_lake write it. pg_lake 3.6 writes all required fields (pg_lake#659 is fixed).
- **Re-creating at the location of a dropped table**: Lakekeeper and Unity assign their own
  locations and reject the old location (Lakekeeper 409 until the purge), Polaris creates the new
  table at the same location – it survives the purge of the old one (60 s observed).
- **Listing with hidden tables** (paged): complete and without duplicates on all four.
- **Security regressions**: registering metadata outside the allowed locations is rejected by all
  that offer `register` (Polaris CVE); no storage keys in view responses; no secrets in Trino's query
  details.
- **RustFS as S3 storage**: checksums, conditional writes (`If-None-Match`/`If-Match`) and multipart
  error codes as on AWS. STS: **`DurationSeconds=60` is silently raised to 900 s** (AWS rejects) and
  **wildcards in actions of session policies (`s3:Get*`) are rejected**.
- **Lab self-tests**: too general gap patterns are rejected (all 131–144 per catalog are specific);
  a targeted partial run no longer overwrites results with "not testable" (had falsified 40 Spark
  read rows on Polaris); the report took a docstring excerpt instead of the reason for 33 gaps –
  fixed.

## Lab changes: rework for comparability (2026-11-15 to 2026-11-20)


The tables show the starting point before the rework and what became of it; the state is under
"Order".

### Catalog level

| Before | Problem | Done |
|---|---|---|
| `test_authz.py` (12 tests) | Lakekeeper only | **Base**: the same scenarios on all catalogs, own setup per catalog (Lakekeeper OpenFGA, Polaris principals/roles, Unity with authorization enabled, RustFS IAM policies) |
| `test_sts_expiry.py` | Lakekeeper only | **Base**: on all catalogs that vend credentials (short lifetime configured per catalog) |
| `test_access_delegation.py` | parameters named after warehouse names | neutral parameter names |
| `test_catalog_features.py` | REST specification and special features mixed | split: REST part → base, Lakekeeper/Polaris features → "Catalog-specific" |
| `test_notebooks.py`, Lakekeeper parts in `test_pyiceberg.py` | Lakekeeper only | notebooks catalog-neutral, run on all four catalogs (area "Demo notebooks", not in the comparison); PyIceberg parts checked |

### Engine level

The engines' own test files (`test_spark`, `test_trino`, `test_duckdb`, `test_flink`,
`test_pyiceberg`, `test_dataframes`, `test_pglake`) were split up (`test_trino.py` is gone
entirely):

**Already in the base matrices (engine test dropped or reduced to a smoke test):**
MERGE/upsert, CTAS, UPDATE/DELETE, overwrite, time travel, schema evolution, branches, metadata
tables, DROP with purge, visibility to other engines.

**New base cases for all eight engines** (previously checked for one engine only):

| Capability | previously only in |
|---|---|
| reading after row-level deletes of other engines (position deletes, equality deletes, deletion vectors) | `test_trino` (PyIceberg reads Trino deletes), `test_flink` (equality deletes) |
| DELETE as merge-on-read (writes delete files instead of rewriting) | `test_spark` |
| partition pruning (files read with a filter on the partition column) | `test_pyiceberg` |
| several changes to one table in one transaction (atomic) | `test_pyiceberg` |
| an aborted transaction leaves no snapshot | `test_pglake` |
| sees foreign commits without reconnecting (metadata cache) | `test_pglake` |
| query across two catalogs/warehouses (join) | `test_flink` |
| setting identifier fields (primary key) | `test_flink` |
| a CREATE TABLE refused by the catalog is an error, not a silent success | `test_pglake` |
| filter/aggregation returns correct values | `test_duckdb`, `test_dataframes` |

Implementation (2026-11-15): as new operations in `tests/engines/test_operations.py`. They thereby
run automatically for all 8 engines × v2/v3 × all 4 catalogs, and the content-changing ones are read
back by all other engines: `delete_merge_on_read`, `transaction_multi_change`,
`transaction_rollback`, `sees_external_commit`, `identifier_fields`, `create_existing_fails`,
`filter_aggregate`, `list_tables`. Reading foreign deletes was already covered by the cross-reads
(after DELETE, UPDATE and MERGE, Flink's upsert with equality deletes included).

Still open because infrastructure is missing (until then engine-specific or not checked):
- **Partition pruning**: needs an engine-neutral measurement of the files read, for example a
  logging S3 proxy in front of RustFS for all engines. The engines' own counting (EXPLAIN formats)
  would not be comparable.
- **Query across two warehouses**: every catalog needs a second warehouse or a second catalog in
  every engine (today only `test_flink.py::test_join_across_warehouses`).

**Stay "Engine-specific":** pg_lake extension installed, attached table read-only, join with a
Postgres heap table; Polars lazy API; DuckDB three-part schema name. That Trino uses table
statistics is now checked by `test_statistics.py` (area "Statistics").

### Order

1. ✅ Neutral parameter names, marker `specific`, split of `test_catalog_features.py`, explicit
   area assignment (2026-11-15).
2. ✅ New base cases on the engine level (8 operations in `test_operations.py`, MERGE as one
   commit), engine files slimmed down (`test_trino.py` gone entirely), checked one by one on all four
   catalogs.
3. ✅ `test_authz` on all catalogs (`tests/lab/authz_backends.py`; Unity with a second instance
   `unitycatalog-authz` and `unity-token-shim`), checked one by one on all four catalogs;
   `test_sts_expiry` on all catalogs that vend credentials (Polaris/Unity: 1 h wait).
4. Report ✅ (areas, coverage incl. "not testable"). Dashboard with the tables "Catalog level" and
   "Special features" (each with the test ID as evidence): still open, built with the data of the
   running full run of all four catalogs.
5. ✅ Reliable full run (2026-11-16):
   - `scripts/run_all.sh` first removes the catalog's test leftovers (`scripts/cleanup_storage.py`).
     Orphaned objects slowed RustFS down until the large tests timed out. Only what belongs to no
     table and is older than 3 h is deleted.
   - The Flink adapter aborts at once when the cluster has no TaskManager left, instead of waiting
     for the global timeout.
   - The per-file summary reads the pytest result line even if more output follows.

6. ✅ Blind spots and security (2026-11-17):
   - **Neutral read source** (`test_types.py::test_read_neutral`, area "Read types (neutral
     source)"): every reader reads a table written by the lab client. The lab client writes v3 as v2
     and upgrades the table to v3 through REST; types PyIceberg does not write faithfully (v3-only,
     `list_of_struct`) are "not testable".
   - **Precise refusal** (`test_authz.py`): "not found" counts as a refusal only on Lakekeeper
     (hides objects, measured), otherwise only 401/403; before every refusal the admin checks that
     the table exists.
   - **Lab self-tests** (`test_lab_selftest.py`): the gap mechanism, the central rules and the
     notebook rule let nothing through – checked with deliberately sabotaged copies.
   - **Security** (`test_security.py`, area "Security", base): login, token lifetime, escaping
     through the storage location, secrets, error messages, names, privilege escalation. Audit is not
     testable (no catalog has an API for audit logs, no container access).
   - **Storage faults** (`test_storage_faults.py`, area "Storage faults (catalog)"): a proxy in
     front of RustFS for the catalog itself.
   - **Lab adapters** (`test_lab_adapters.py`): token and STS adapters for Unity.
   - **File formats** (`test_file_formats.py`, areas "Write file formats" / "Read file formats"):
     ORC and Avro.

7. ✅ Meaningfulness of the existing tests (2026-11-17):
   - **"Not offered" is checked** (`test_na_claims.py`): version pin per engine + probes for the
     weightiest claims (found: Trino 484 can do `VARIANT`).
   - **New features stand out**: `/v1/config` endpoints per catalog against
     `docs/api/advertised-*.txt`.
   - **Neutral read source** writes with the lab client (also on RustFS).
   - **Spark's table cache** is dropped in the read matrices (`Engine.refresh`) so it cannot hide
     data loss.
   - **Catalog consistency** (`test_catalog_consistency.py`): invalid commits, lost updates.
   - **Storage faults** (`test_storage_faults.py`), **file formats** (`test_file_formats.py`).

### Done on 2026-11-18 (previously "Open")

Meaningfulness:
- Lab configuration without disabled checks: Unity checks the audience (`server.audiences=unity`
  instead of `*`); Polaris accepts Keycloak tokens next to its own (`mixed`), RustFS through STS
  (`AssumeRoleWithWebIdentity`) – the Keycloak cases now run on all four catalogs.
- Too general gap patterns: `expect_gap` rejects patterns that also match generic errors (outage,
  timeout, wrong result, 500; `conftest.GENERIC_ERRORS`); all gaps of all catalogs checked
  (`scripts/probes/check_gap_patterns.py`).
- Catalog checks with raw REST: rename tests in `test_catalog_features.py`; new catalog tests
  (`test_rest_conformance.py`, `test_catalog_consistency.py`) only with raw REST
  (`tests/lab/rest_client.py`).
- Neutral read source for the cross-reads of operations, partitions, evolution and maintenance
  (`test_neutral_source.py`, four areas "Read … (neutral source)"): the writer is Iceberg Java core,
  the reference implementation, directly through PySpark's JVM (`tests/lab/reference_writer.py`:
  its own REST client, `GenericFileWriterFactory`, core commits) – no engine under test, no Spark SQL,
  no Spark cache. This also gives states PyIceberg cannot write: native v3, position deletes (v2),
  deletion vectors (v3), equality deletes. Every state is checked at the source (Java reads it back,
  the expected kinds of delete files are there) before a reader reads it. Result: the reader gaps of
  the cross-read matrix (PyIceberg/Polars/Daft without equality deletes, Daft and pg_lake without
  deletion vectors, pg_lake after compacting DVs) lie with the reader, not the writer.
  `remove_orphan_files` has no neutral state (it changes nothing a reader sees).
  The earlier evaluation stays valid: there is no table state that no reader could check, except
  where it cannot come about (RustFS without v3, pg_lake without SigV4). The apparent gap "Spark
  reads nothing on Polaris" was a report bug (fixed through `conftest.skip_if_not_produced`).

New areas: REST conformance (`docs/rest-conformance.md`), engine inventory
(`docs/engine-inventory.md`), outage of the token endpoint, branch isolation, re-creating with an
open session, files per specification, concurrent upsert, RustFS as S3 storage, security regressions
(register outside allowed locations, keys in views, Trino query details), listing with hidden
tables, re-creating at the location of a dropped table.

### Done on 2026-11-19: what gaps hide

- **Passing on a result despite a gap** (`conftest.keep_if_content_ok`): if a producer (operation,
  maintenance, partitions, evolution) hits a gap AFTER the content check – no delete file written,
  files left in storage, Spark cache during partition evolution – the writing engine's own read
  decides: if the content is right, all other engines still read the table back. Maintenance now
  checks content and storage integrity BEFORE the task-specific checks. Lab self-tests check the
  mechanism (also: wrong content and a skip pass nothing on).
- **Gap severity** (`conftest.SEVERITIES`): a gap reason starts with `DATA LOSS:`,
  `DATA CORRUPTION:`, `INCONSISTENT:`, `WRONG RESULT:` or `SECURITY:` if the gap changes data or
  results or opens access wider than intended. `expect_gap` rejects a reason that sounds like a
  defect (silent, lost, duplicate, truncated, stale, ...) but carries no severity. The report lists
  "Severe gaps" separately and counts them per engine.
- **Behind the gaps** (`LAB_BEHIND_GAPS=1`, RustFS only): on RustFS almost all tests of PyIceberg,
  Polars, Daft (missing `x-amz-content-sha256` header) and DuckDB (no AWS host name) ended at the
  first step. A second run gives exactly these engines the workaround (header; RustFS alias
  `rustfs-lab.s3.us-east-1.amazonaws.com`) and writes to `reports/rustfs-behind/` – never into the
  comparison. (Replaced on 2026-11-20 by the run without help, see below; the script
  `behind_gaps_doc.py` and `docs/behind-the-gaps.md` are gone, `docs/lab-helps.md` took their
  place.) First finding already in the targeted run: PyIceberg `DROP ... PURGE` also fails on
  RustFS on the purge refusal (406; the central rule now covers all DROP tests). pg_lake stays "not
  offered" (no SigV4).
- Targeted runs write into their own directory with `REPORT_DIR=<dir>`.

Also on 2026-11-19:
- **Column "RustFS + help"** instead of a separate report: the run `LAB_BEHIND_GAPS=1` appears in the
  dashboard as a fifth, grey column next to the four catalogs (PyIceberg, Polars, Daft, DuckDB
  only) and counts in no number. Evidence for the gap itself: `test_stock_connection.py` (always
  without help).
- **Roles and inherited grants** (`test_rbac.py`, area "Roles and inherited grants"), including a
  role that only the identity provider grants (`test_idp_role_is_a_catalog_role`): RustFS yes
  (policy claim), Lakekeeper no (token roles only with Cedar, not with OpenFGA), Polaris only if the
  role is also assigned to the principal in Polaris; Unity has no roles.
- **Web UI**: Polaris Console (self-built, `scripts/build_polaris_console.sh`) and Unity UI as
  separate containers (`polaris-console`, `unity-ui`), tested in `test_ui.py` (area "Web UI").
- **Single privileges** (`test_privileges.py`, area "Single privileges (inventory and effect)"):
  privilege inventory per catalog (`docs/api/privileges-*.txt`, `scripts/privilege_inventory.py`)
  and a matrix of the common privileges. Unity OSS has none of them except CREATE TABLE (owner
  only), RustFS cannot restrict granting to one table.
- **Docstrings**: every function, method and class (except `scripts/probes`) describes its arguments
  and result, every test additionally describes under `Checks:` exactly what is checked – enforced
  by two lab self-tests.
- **Neutral type source for every type**: `test_read_neutral` (area "Read types (neutral source)")
  had no source for the types the lab client cannot write faithfully – `timestamp_ns`,
  `timestamptz_ns`, `unknown`, `geometry`, `geography`, `variant` and NULLs inside `list<struct>`
  (64 reader cells "not testable"). These are now written by Iceberg Java core
  (`ReferenceWriter.write_values`), natively in v2/v3. Type fidelity (`test_reader_type`) falls
  back to the same source when no engine writes the type. First findings: on Polaris (the only
  catalog accepting geo types) Spark 4.1 reads geometry/geography correctly (the documented
  "Spark has no geo type" was never backed by a read and was removed), DuckDB reads geometry but
  not geography, PyIceberg-based readers cannot load spec-form geo tables.
- **Test folders**: `tests/` is split by topic (`lab`, `engines`, `catalog`, `security`, `storage`,
  `specific`, `selftest`, `demos`), each folder with its own README.

On 2026-11-20:
- **Lab helps count; without help shown next to them, for every catalog** (user decision: tests
  that only work with a sensible workaround count as working): all lab helps for engines are listed
  in one place (`iceberg_lab.config.HELPS`) and are on in every run. The gap itself is shown once
  per engine by `test_stock_connection.py`, always without help. The run without help
  (`LAB_NO_HELP=1`, `reports/<catalog>-nohelp/`) selects only the tests of the helped engines
  (`conftest.pytest_collection_modifyitems`) and is shown, not counted. This replaces the run
  "behind the gaps" (the old RustFS reports were swapped: the full run with help is now
  `reports/rustfs/`, the old normal run `reports/rustfs-nohelp/`). pg_lake on Lakekeeper and Unity
  needs no help: its option `rest_auth_type 'none'` connects without a token (the lab's token
  stub is no longer used; server name `<prefix>_noauth`, servers persist in Postgres). The
  configured S3 endpoint for Unity's vended keys is configuration, not a help.
  Dashboard: a grey column "without help" next to RustFS (and any other catalog that gets a help), and the
  difference table also lists capabilities whose result changes without help;
  `docs/lab-helps.md` shows works/all per area without and with help.

