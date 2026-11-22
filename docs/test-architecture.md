# Test architecture: base and special features

Goal: an overview of what works with which engine and which catalog and what does not, and which
special features exist. Every claim in it is backed by a test: every cell has its test ID (pytest
node ID) with status and cause, verifiable by running exactly this test again. Without a test there
is no claim.

There are two layers for this.

## 1. Base tests (feed the comparison)

The base tests are identical on both levels, and **all four catalogs and all eight engines take
part**: every base case runs for every combination. Exceptions exist only as a result, never as a
missing row.

- **Every engine** runs the same test cases. If an engine cannot do something, it shows "not
  offered" with a verified reason. The test case is not left out.
- **Every catalog** runs the same test cases (same test names, same parameters). What a catalog
  does not offer is also "not offered" with a verified reason.
- Parameter names are neutral (`sts`, `signing`), not catalog-specific warehouse names, so that the
  same cases stand side by side in the comparison.
- Only the base tests count in coverage, reliability and the catalog comparison.

## 2. Special features (not in the comparison)

Separate tests for what only one catalog or only one engine has. They show failures the same way
(known gaps only with the exact message), but do not count in the comparison:

- Area **"Catalog-specific"**, for example Lakekeeper soft delete/undrop, tags, statistics,
  namespace move and format policy; Polaris policies and federation; RustFS table buckets; Unity
  volumes and models. (Polaris and Unity both offer generic tables – that is base.)
- Area **"Engine-specific"**, for example pg_lake (extension, attached tables read-only, join with
  heap tables), the Polars lazy API and DuckDB's three-part schema name.

Rule (2026-11-15): classification is by **capability**, not by endpoint.

- **Base**: a capability that at least two catalogs (or engines) offer – whether through the
  Iceberg REST specification or through their own APIs. The test is the same for all; the
  implementation per catalog lives in an adapter (pattern: `tests/lab/authz_backends.py`). Catalogs
  without the capability show "not offered".
- **Special feature**: a capability that exactly one catalog or exactly one engine has.

Which catalog offers which capability through which endpoint is in the feature inventory
(`docs/feature-inventory.md`, generated from the catalogs' API descriptions).

## 3. Rework for comparability (2026-11-15/16, done except for "Open" below)

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

### Assignment in the report (`tests/lab/reporting.py`)

Every test belongs explicitly to one area. Only "Engine-specific" (the engine files in
`ENGINE_FILES`), "Catalog-specific" (marker `@pytest.mark.specific`), "Demo notebooks" and "Lab
self-test" are not compared. A test without an area shows up as "Unassigned" and is counted. So it
does not silently drop out of the comparison, as happened before to the base tests for scan
planning, object storage and credential reach.

### Result overview (goal of the rework)

- **Engines × catalogs**: base areas per engine and catalog (works / gap / not offered / not
  testable) with cause.
- **Catalog level**: tests without an engine (REST functions, authorization, scan planning, object
  storage) as a separate table catalog × capability. They were missing from the engine tables
  because they belong to no engine.
- **Special features**: one list per catalog and per engine, separate from the comparison.
- Every cell links to its test IDs as evidence.

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
  comparison. `scripts/behind_gaps_doc.py` writes `docs/behind-the-gaps.md`: what comes after the
  first failure. First finding already in the targeted run: PyIceberg `DROP ... PURGE` also fails on
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

### Open

- Missing infrastructure: partition pruning (an engine-neutral measurement of the files read, e.g.
  a logging S3 endpoint all eight engines use – pg_lake, Trino and DuckDB have their own, statically
  configured S3 access), query across two warehouses (every engine needs a second catalog).
- Keycloak outage on the catalog side (the catalog itself can no longer reach Keycloak): a proxy in
  front changes the address and thus the tokens' issuer; checked is the outage of the token endpoint
  from the clients' point of view.

## Feature inventory (as of the pinned versions)

All 353 endpoints of the four catalogs (Lakekeeper 178, Polaris 76, RustFS 37, Unity 62) are
assigned in `tests/lab/feature_inventory.py` to exactly one of 53 capabilities; the overview is in
`docs/feature-inventory.md`. Every capability has a test as evidence, every base capability a test
that runs on all catalogs. The inventory distinguishes ✓ (described and implemented), ◐ (described,
not implemented by the server – example: Lakekeeper describes scan planning but answers 404;
Polaris describes remote signing but answers 501) and – (not offered).

### API size is no measure of functionality

The endpoint counts cannot be compared directly:

- **The sources differ** (header of the files in `docs/api/`): Lakekeeper from the running OpenAPI
  description, Polaris from the JARs' annotations, RustFS from strings in the binary (without HTTP
  methods, hence too low), Unity only its own API – its Iceberg REST endpoints are not listed there.
- **The cut differs**: Lakekeeper (178) and Polaris (76) are close on the Iceberg REST standard
  (35 to 29). The difference is in administration (143 to 47): for tables, namespaces, views and
  generic tables Lakekeeper has separate endpoints each for protection, tags, permissions and allowed
  actions (about 43 together), plus projects, tag definitions, background tasks, trash bin, search
  and statistics. Polaris handles permissions compactly through roles and grants (21 endpoints).

The number of capabilities is more meaningful (of 53, header of `docs/feature-inventory.md`):

| | Lakekeeper | Polaris | RustFS | Unity |
|---|---|---|---|---|
| described in the API | 38 | 22 | 16 | 20 |
| of which implemented (without ◐) | 35 | 21 | 16 | 20 |

◐ means: described, but not implemented by the server at all – Lakekeeper scan planning, functions
and unregister (404 each), Polaris remote signing (501). Every ◐ is backed by a test's known gap
(`test_feature_inventory.py::test_not_implemented_is_proven`). Partial failures (e.g. RustFS: backup
501, directory and index work) stay ✓ with a note; how well a capability works in detail is shown by
the test results in the report. `tests/selftest/test_feature_inventory.py` keeps it that way: a new
endpoint after an update, a missing test or a stale overview stands out there.
