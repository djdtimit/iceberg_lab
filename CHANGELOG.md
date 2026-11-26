# Changelog

What changed in the lab and what each run found, by date. Current findings by topic: [docs/findings.md](docs/findings.md); the test design: [docs/test-architecture.md](docs/test-architecture.md).

## Lab changes 2026-11-25 (3): lab self-checks, bug report facts, coverage

- **`PROFILE.name` is no longer compared with a catalog that can sit behind Gravitino.** A new
  self-test finds such comparisons in `src/`, `scripts/` and `tests/`. Each of the 31 places now
  says what it means:
  - 26 ask the server's own API through `PROFILE.server` (admin, signing, identities,
    bootstrap). This is unchanged for every profile.
  - 5 ask a catalog's behaviour through `PROFILE.layers`. Reruns on seven profiles gave the
    same results.
- **Storage cleanup in front of Unity fixed.** Before each full run, `scripts/cleanup_storage.py`
  keeps the managed storage of Unity's other catalogs. In front of Unity through Gravitino it did
  not, because it asked `PROFILE.name == "unity"` and then the wrong server. The cleanup before
  the `gravitino_unity` run on 2026-11-25 deleted 9743 objects. A check of both Unity instances
  found every live table intact; the deleted objects were leftovers.
- **Teardown errors are no longer NEW.** A teardown error after the test itself ran (for
  example a namespace RustFS cannot drop) is now listed under "cleanup errors" in the overview,
  `run_all.sh`, `run_parallel.sh` and `rerun.sh` (`CLEANUP`). Before, the overview counted it as
  NEW, while `results.json` kept the test's own outcome.
- **A merged rerun drops parameters that are gone.** When a whole file or directory is rerun, old
  records of test ids it no longer collects leave the report. Before, they stayed until the next
  full run and broke `test_references_hold_the_same_tests`.
- **A quoted `[known gap]` is no longer a gap.** `reporting._marked` takes a reason only from the
  raised `KnownGapError` or from the front of the report (central rules). A self-test whose
  assertion diff contained the marker had been recorded as a gap without a cause. A check of all
  run logs found no other case.
- **The raw error is recorded.** Every failed test (gap, fixed, new) keeps the error as the
  engine or catalog raised it, before the lab turned it into a gap text (`error` in
  `results.json`, `comparison_rows.json` and `results/<name>.rows.json.gz`). The line
  references `results/<name>.jsonl` stay without it, because the messages hold random names
  and `git diff results/` would show every gap as changed.
- **The run scripts read a JSON overview.** Every pytest process writes its overview to
  `LAB_SUMMARY` (`gaps.write_summary`). `scripts/run_summary.py` replaces the awk lines of
  `run_all.sh`, `run_parallel.sh` and `rerun.sh`. This fixes two silent problems:
  - the text overview lists 50 entries per section, and the "... and N more" line was dropped;
  - a pytest process that died before its overview looked like a clean file. It now counts as
    NEW, with its log.

  `run_all.sh` exits 1 on NEW, and `run_parallel.sh` takes that exit code.
- **Every test has an area again.** The lab self-tests were recognised by a list of file
  names: eleven newer self-test files and `tests/catalog/test_gravitino_fronting.py` were
  "Unassigned" and compared like base tests. A self-test parametrized by engine gave pg_lake on
  RustFS (everything else not offered there) "1 of 1 works", shown as 100 % in the dashboard.
  Self-tests are now everything under `tests/selftest/`, the fronting tests are
  catalog-specific, and a self-test fails on any reference row without an area.
- **Guards against distorted figures.** That error changed the numbers without failing anything.
  Three checks now catch that kind of error, whatever its cause:
  - a test without an area or theme is a NEW failure of the run (`reporting.unclassified`);
  - the dashboard shows `works/offered`, hatched, instead of a percentage when fewer than 5 % of
    the planned cases were offered;
  - `scripts/reference_shifts.py` compares every exported reference with its committed state and
    prints a `SHIFT` line per engine whose reliability moved by more than 5 points or whose
    offered cases changed by more than 10 % (`run_all.sh`, `rerun.sh`, `run_parallel.sh`).

  Its first run showed how far the self-tests had reached: the catalog's own tests had about 68
  offered cases too many on every catalog, and reliability of the reference catalog's own tests
  drops from 84 % to 76 % without them.
- **More ways into the figures closed.** A search for further spots found three:
  - A plain `pytest` wrote into Lakekeeper's report directory, and the next export took its rows
    into the reference (322 self-test rows of another run, harmless this time). Only the run
    scripts (`LAB_RUN_ID`) write the references' directories now; a plain pytest goes to
    `reports/adhoc/<catalog>/`. The export names rows of another run (`SHIFT`).
  - Creating a view in `test_rest_conformance.py` booked every failure as "not offered", a
    500 too - a crash left the denominator. Only "no such route" counts now, and a self-test
    refuses `if not r.ok: pytest.skip("... has no API ...")`.
  - Flink's MERGE try counted any error naming "merge", "parse" or "validation" as "not
    offered" (Flink repeats the statement in its errors, and a missing table is a validation
    error too). Only the planner's `NullPointerException: rowType`, the parse error at `MERGE`
    and an explicit "not supported" count now.
- **No more `comparison.md`.** The Markdown report each run wrote is gone (about 400 lines
  of `reporting.py`, barely tested). The dashboard shows the same tables over all catalogs, and
  `docs/findings.md` keeps what stands out. `results.json` and `comparison_rows.json` stay:
  the references and the dashboard are built from them.
- **Facts for bug reports.** `scripts/gap_register.py draft <cause id>` lists, for one cause, the
  register entry, where the lab sees it, how the same tests end on the other references, the raw
  errors from the run logs (before the lab's text replaced them) and the lab command.
  New drafts:
  - [18](docs/bug-reports/18-gravitino-rest-backend-passes-backend-credentials.md): Gravitino
    gives a SELECT-only user the backend's write keys. Tested 2026-11-25 with a credential
    provider on the catalog (also a new catalog): Gravitino still returns Lakekeeper's own key.
  - [19](docs/bug-reports/19-gravitino-rest-backend-client-errors-as-500.md): the backend's
    client errors come back as HTTP 500.
  - [02](docs/bug-reports/02-pg_lake-silent-commit-divergence.md) gains the refused CREATE TABLE.

  `draft` takes the recorded errors first and the run logs only for older runs. A self-test keeps
  the overview `docs/bug-reports/README.md`, the drafts and the register's `bug_report` links in
  step. The overview is checked, not generated, because it also keeps withdrawn and fixed
  reports.
- **New self-tests:** `test_reporting.py`, `test_export_reference.py`, `test_datatypes.py`,
  `test_cleanup_storage.py`, `test_bug_drafts.py`, `test_run_summary.py`,
  `test_cloud_profiles.py` and `test_reference_shifts.py`. Mutations of the code under test (a
  reversed log pairing, the old Unity check, `-0.0` treated as `0.0`) make them fail.
  `datatypes.normalize` loses an unreachable geometry branch.
- **Coverage:** `pytest-cov` is a dev dependency. Branch coverage of the self-tests: 32 % -> 38 %
  of the lab code; `datatypes` 14 % -> 80 %, `export_reference` 0 % -> 39 %, `cleanup_storage`
  0 % -> 26 % (the rest is S3 and catalog I/O). See
  [test-architecture](docs/test-architecture.md) for running it.

## Lab changes 2026-11-25 (2): Gravitino in front of the other catalogs, web UI login

- **Four new catalog profiles `gravitino_lakekeeper`, `gravitino_polaris`, `gravitino_unity`,
  `gravitino_rustfs`**: a Gravitino catalog with `catalog-backend = rest` forwards to the other
  catalog, and the whole suite runs through it. Gravitino forwards the caller's token (Polaris:
  tokens with `scope=gravitino polaris`, Polaris principal `lab-admin`); towards RustFS it signs
  with SigV4. Trino catalogs `grvlk_*`, `grvpol_*`, `grvuc_*`, `grvrfs_*`; the profiles are in
  `rerun.sh`, `run_parallel.sh`, the dashboard and the generated docs.
- **New `tests/catalog/test_gravitino_fronting.py`** uses both sides (the backend directly through
  `tests/lab/fronting.py`): writes through Gravitino are the backend's table, the backend's
  commits and drops are seen at once, Gravitino's own API lists, loads and tags tables it never
  created, and a staged creation lands in the backend.
- **The gaps of both layers apply** to a fronted profile (`gap_register.find_gap`,
  `gaps.CATALOG_GAPS`, and `layer_rows` for gap blocks of one catalog): booked when the error
  matches, silent when the case passes. Notebook expectations of either layer count too
  ([test-architecture](docs/test-architecture.md)). The first full run had lost 130 cases in
  front of Unity because blocks like `if PROFILE.name == "unity"` did not apply there. Two
  more such checks now use `PROFILE.layers`: the namespace cleanup tolerates RustFS's hidden
  entry of a renamed table in front of RustFS too (before: teardown errors), and
  `gaps.central_gap` takes the layers only for the catalog under test. Notebook expectations
  that start with global flags (`(?i)...`) keep them when combined. The name test drops tables
  with dots in their name in the backend as well.
- **New gaps**:
  - `gravitino-rest-backend-grants-not-forwarded`: in front of Polaris, a Gravitino grant is not
    enough. Gravitino forwards the caller's token and Polaris decides again by its own grants,
    so every right has to be kept in both. 30 authorization cases are booked to it.
  - `gravitino-rest-backend-staged-create`: in front of Lakekeeper and RustFS, the commit of a
    staged creation (CTAS, create and write in one commit) is refused. It works on Polaris. As a
    consequence pg_lake loses its tables (`pglake-ignores-refused-create`).
  - `gravitino-rest-backend-client-error-500`: many client errors of the backend come back as
    HTTP 500 (refused types, commits and purges, huge names, Unity's managed-storage location).
  - `gravitino-rest-backend-credentials-not-narrowed` (SECURITY): in front of Lakekeeper and
    Unity, a user with only SELECT in Gravitino gets storage keys that write the table.
  - `gravitino-rest-backend-advertises-unsupported`: Gravitino advertises its own endpoints, the
    backend may not have them (register and views on Unity, view rename on RustFS).
  - `gravitino-rest-backend-retries-server-errors`: a backend's 500 arrives after about 30 s.
  - `gravitino-rest-backend-long-name-left`: a name too long for Gravitino's store is refused,
    but it exists in the backend anyway.
  - `gravitino-dotted-names` now also covers federation. A namespace with a dot reaches the
    backend, and Gravitino then lists nothing of the catalog. The first Polaris run lost 314
    cases to it. The name test now removes such names in the backend.
- **Gravitino checks its own privileges in front of other catalogs.** By default
  (`gravitino.iceberg-rest.disable-rest-authz = true`) it does not: a user without any grant
  read and created tables through it. This was a SECURITY gap in the first runs. The lab now
  sets the switch to `false`. In front of Lakekeeper 45 and in front of Unity 43 failing cases
  passed with it, none broke. Since the default can no longer be reproduced with the lab's
  configuration, it is a finding in [docs/findings.md](docs/findings.md) rather than a gap.
- **A fixed service identity towards Polaris was tested and rejected.** A static
  `header.Authorization` on the catalog replaces the forwarded token (Gravitino only adds the
  caller's token if no such header is set). 24 of the 30 cases passed then, but Polaris vended
  write credentials for the service identity to a user with SELECT only. The token is also
  readable in the catalog's properties by every user who may see the catalog, and it is never
  refreshed.
- **Fronted RustFS**: clients without vended credentials (OAuth2 profiles too) use the lab's S3
  keys (`catalog_auth_props`, DuckDB).
- **Gravitino web UI login from the host**: public Keycloak client `gravitino-ui` (PKCE), user
  `lab-ui` with role `lab_ui`. Service `keycloak-browser` on `127.0.0.2:8080` plus the hosts
  line `127.0.0.2 keycloak`, so the browser reaches the one issuer Gravitino accepts. Trino's
  port is now bound to `127.0.0.1` only. `gravitino-single-issuer` is gone.
- **Fix: Gravitino's token checks were never tested.** `test_bad_token_is_refused` and
  `test_catalog_token_does_not_outlive_identity_token` reported Gravitino as "open catalog"
  (not offered), because it was missing from their catalog lists. Now they cover Gravitino and
  the fronted profiles: forged, tampered, expired, foreign and audience-less tokens are refused.
  An expired token may also get 419, the REST spec's Authentication Timeout (Gravitino answers
  that).
- Docs: diagrams for login and S3 access per catalog, the run pipeline, the gap register, the
  life of an intermittent gap, the version mechanisms and the federation's gap handling. Rows
  for four undocumented test files. Stale facts fixed: OpenFGA persistence, pg_lake 3.7.1,
  pinned versions, the "four catalogs" with authentication.
- Dashboard: the reference catalog is always the rightmost column.

## Lab changes 2026-11-25: Gravitino in the feature inventory, scope = Iceberg REST

- **The lab tests Iceberg REST catalogs.** Capabilities without a bearing on Iceberg tables are
  now recorded as **Out of scope** in `tests/lab/feature_inventory.py` (`scope: "out"` with an
  `out_reason`). Their endpoints stay classified, so a new route still has to be sorted in, but
  they need no test. Eight capabilities: ML models, volumes/filesets, functions, generic/Delta
  tables, topics, jobs, the own schema/table APIs next to Iceberg REST, and credentials for
  non-Iceberg objects. Their tests were removed: `test_generic_tables`,
  `test_catalog_functions`, `test_unity_volumes`, `test_unity_functions`, `test_unity_models`,
  `test_unity_schema_api`.
- **Gravitino's 156 routes are classified** (`docs/api/gravitino-1.3.1.txt`). Gravitino
  implements every Iceberg REST capability it advertises, server-side scan planning included.
  Four capabilities of a single catalog became base capabilities. Their tests now run on every
  catalog, with one adapter per catalog:
  - `test_tags`: Lakekeeper and Gravitino.
  - `test_policies`: Polaris and Gravitino. A policy attached to the namespace is inherited by
    the table.
  - `test_projects`: Lakekeeper projects and Gravitino metalakes. It was `test_lakekeeper_projects`.
  - `test_storage_validation`: Lakekeeper `validate-profile` and Gravitino `testConnection`. It
    was `test_lakekeeper_storage_validation`.

  New for Gravitino only: `test_gravitino_owners` (hand over ownership) and
  `test_gravitino_statistics` (custom statistics). `test_whoami` now also asks Gravitino
  (`/api/authn/me`).
- **New gap `gravitino-test-connection-no-storage-check`**: `testConnection` checks only the JDBC
  backend. A warehouse in a bucket that does not exist, or wrong S3 keys, pass as working.
- **Fix**: on Gravitino, `test_catalog_update` deleted the catalog under its old name after the
  rename. Gravitino answers that with 200 `dropped: false`, so four catalogs had been left
  behind. The handle now moves with the name, as on Unity, and the delete checks `dropped`.
- Dashboard: the capability tables show Gravitino. The counts separate capabilities in scope
  from those out of scope.

## Lab changes 2026-11-24 (3): pg_lake 3.7.1, Polars 2.2.0, web UIs, persistent OpenFGA, Gravitino

- **Version updates**: pg_lake 3.7.1 (one fix: a CVE in brotli), Polars 2.2.0 and
  polars-iceberg 0.1.1. The pg_lake and Polars tests ran again on all catalogs: no new failures
  and no fixed gaps. `seen_at` moved forward for 36 causes. Two Polars geo causes cannot be
  reached on any catalog, because earlier gaps hide them (catalogs reject geo types; PyIceberg
  cannot parse the spec's geo form). They were reproduced directly with Polars 2.2.0 and
  confirmed by hand:
  - `polars-no-geo-parquet`: "LogicalType union has no variant set";
  - `polars-geo-as-binary`: Polars still has no geo dtype.

  Polars' "not offered" claims are verified at 2.2.0.
- **Web UIs work from the host's browser**:
  - Polaris Console: CORS on in Polaris.
  - Lakekeeper UI: `LAKEKEEPER__UI__LAKEKEEPER_URL`; the UI otherwise calls a name only the
    compose network knows.
  - A second Unity UI for the main instance on `localhost:3001`, with `WDS_SOCKET_PORT` so its
    dev server stops reloading the page every second.
  - Login to the Lakekeeper UI of `lakekeeper-authz` (`localhost:8282/ui`): public Keycloak
    client `lakekeeper-ui` with PKCE, user `lab-ui` (password `lab-ui`) as project admin.
    Lakekeeper also accepts the issuer of Keycloak as the browser reaches it
    (`LAKEKEEPER__OPENID_ADDITIONAL_ISSUERS`, a plain string, not a JSON list).

  `tests/security/test_ui.py` checks each UI's published port, the hosts its runtime config
  names, CORS, the dev server's socket port, and that the UI login reaches the data.
- **OpenFGA keeps its tuples in Postgres** (database `openfga` in `db-authz`). The in-memory
  store lost them on every restart while `db-authz` still said "bootstrapped". After that, nobody
  owned the server or the default project, and every project or warehouse grant failed with
  403 `can_grant_*`. The authz stack was reset once.
- **Apache Gravitino 1.3.1 as sixth catalog** (`CATALOG=gravitino`, compose profile
  `gravitino`, `gravitino/gravitino.conf`):
  - Iceberg REST runs as an auxiliary service of the Gravitino server, and the warehouse names a
    `lakehouse-iceberg` catalog in metalake `lab`.
  - The Iceberg JDBC backend is in Postgres. Data is on RustFS, with vended STS session
    credentials (`s3-token`).
  - Keycloak tokens (client credentials of `lab-admin`, `scope=gravitino` for the audience) and
    authorization are on.
  - Token endpoints can now be on another host than the catalog (`Settings.token_endpoint`).
  - pg_lake gets the OAuth scope as server option `scope`. Its default is Polaris'
    `PRINCIPAL_ROLE:ALL`, which Keycloak refuses with 400.
  - Admin adapter (`test_catalog_admin.py`): create, rename, switch off/on and delete
    catalogs, change storage properties, version, users with a role.
  - Each catalog passes its own name to Iceberg's JdbcCatalog (`catalog-backend-name`). By
    default (documented), every JDBC-backed Gravitino catalog uses the name "jdbc", so all of
    them share one set of namespaces and tables.

  - Permission backend (`tests/lab/authz_backends.py`, `GravitinoAuthz`):
    - Users are the token's `service-account-<client>` in metalake `lab`. Privileges go to
      roles, roles go to users (alice: role `az_alice`).
    - The token-only role works through groups: `groupsFields = lab_roles` in
      `gravitino.conf`. The role is granted to group `lab-readers`, and nobody is a member in
      Gravitino. Keycloak's `rbac-idp` client now also carries audience `gravitino`.
    - Per-user Trino catalogs: `grv_alice` and `grv_bob`.
    - Privilege inventory `docs/api/privileges-gravitino.txt`, read from the image's
      `Privilege.Name` enum (`scripts/privilege_inventory.py gravitino`).
    - Web UI in `test_ui.py` (`gravitino:8090`; login settings from `/configs`).
  - Gravitino's routes are recorded in `docs/api/gravitino-1.3.1.txt` (155, read from the
    jars). They are classified in the feature inventory since 2026-11-25 (see above).

  Findings of the first full run (11,435 tests, 37 new failures, all classified):
  - **`gravitino-disabled-catalog-served`** (INCONSISTENT): a switched-off catalog
    (`inUse=false`) still serves Iceberg REST reads. A namespace create is answered with
    500 "not in use", but the namespace exists afterwards.
  - **`gravitino-dotted-names`** (INCONSISTENT): a namespace name with a dot (`..`,
    `../../escape`) is answered with 400, but the namespace is created anyway. It can then not
    be loaded or dropped (500). Every top-level namespace listing of the catalog fails with 400,
    on Iceberg REST and on Gravitino's own API, until the row is deleted from the database.
  - **`gravitino-variant-refused-created`** (INCONSISTENT): a table with a variant column is
    answered with 406 "Unsupported type: variant", but it exists and loads over Iceberg REST.
    Gravitino's own API cannot load it, and no role can name it.
  - **`gravitino-no-location-checks`** (SECURITY): every overlapping table location is accepted
    (same, inside, above, set later, registered, `write.data.path`). The vended STS keys of the
    new table read the other table's files.
  - **`gravitino-backslash-table-refused-created`** (INCONSISTENT): a table named `a\b` is
    answered with 400 ("Illegal character in path" in the location built from the name), but
    it exists and is listed.
  - `gravitino-single-issuer`: tokens are checked against exactly one issuer, and the web UI
    logs in at that address. A login from the host's browser (Keycloak as `localhost:30080`)
    is refused, and the UI's login settings name `keycloak:8080`.
  - `gravitino-error-stack-traces`, `gravitino-snapshots-param`, `gravitino-v3-type-in-v2-500`,
    `gravitino-listing-needs-table-privilege`; Gravitino added to
    `catalogs-no-schema-compat-check` and `trino-hides-load-refusal`.
  - Permissions otherwise as expected: table grants, revokes, roles, namespace grants, the
    token-only role through groups, and alice cannot grant herself rights.
  - **`gravitino-no-transactions-commit`**: no `/transactions/commit`. DuckDB refuses the
    multi-table COMMIT, and pg_lake loses its writes (`pglake-ignores-refused-commit`, now also
    on Gravitino).
  - Server-side scan planning (`/plan`) works; so far only the reference had it.
  - `pyiceberg-geo-write-binary`: Gravitino serves a default-CRS geometry as plain `geometry`,
    which PyIceberg can load. Its write then stops at the schema check, because PyIceberg turns
    geo columns into binary.
  - **`engines-java-oauth-refresh-keycloak`**: Iceberg Java (Spark, Flink) refreshes its OAuth2
    token by a token exchange authenticated only with the old token. Keycloak refuses it
    (`invalid_client`), so a session that outlives the token (1 h) fails with "Authentication
    token is expired". It shows on Gravitino because its STS keys live 1 h, so
    `test_sts_expiry` waits past the token's lifetime. On Lakekeeper the wait is 16 min.
- **`test_name_is_refused_or_safe`**: a refused name must not exist anyway, and the listing of
  its parent must still work. If one landed anyway, it is removed from the backend database.
- **`test_namespace_grant_covers_its_tables`** drops the namespace grant right away (where it is
  a role). Before, bob kept it until the end of the module. On Gravitino, the first catalog
  where the token-only role works, the later test then found A.t1 open without the role.
- **`rerun.sh`** runs on all six catalogs by default.
- Docs and dashboard: REST conformance, engine inventory and the dashboard include Gravitino
  (the capability tables do not yet).
- **`test_catalog_deactivate` also checks writes**: a write while switched off must be refused,
  and must not have happened after reactivation.
- **`rerun.sh`** no longer reports failed register self-tests for the run without help, where
  they are deselected (pytest exit code 5).

## Lab changes 2026-11-24 (2): cloud catalogs, parallel runs, spec responses, trend

- **Cloud catalogs** (`docs/cloud-catalogs.md`): Snowflake Open Catalog, Snowflake Horizon
  Catalog and Databricks Unity Catalog are profiles once their `LAB_<NAME>_*` variables are set
  (environment or `.env.cloud`, which git ignores). Horizon follows Snowflake's documentation:
  - the programmatic access token is the client secret, sent without a client id;
  - the scope is `session:role:<ROLE>`;
  - `warehouse` is the database name.

  A cloud catalog runs only REST conformance, namespaces, operations, types, evolution and
  the read/write matrix, with PyIceberg, Polars, Daft, DuckDB and Spark. It runs no
  attack-like security tests and nothing that disturbs the catalog; the other tests are
  deselected. **Not yet run against a real account.**
- **Several catalogs at once**: every in-process proxy (fault, federation, storage, IdP) listens
  on a port per catalog (`config.lab_port`). The Trino `*_proxy` catalogs name these ports.
  pg_lake's proxy server has the port in its name, because pg_lake refuses to change the
  endpoint of a server with user mappings. `scripts/run_parallel.sh` runs the full
  runs in lanes (default 2; `PARALLEL=n`).
- **`scripts/run_all.sh` runs `test_sts_expiry.py` beside the other files**: it mostly waits
  for vended keys to expire (about 1 h on Polaris and Unity). Reports are written under a lock
  (`reporting.report_lock`), so both processes merge their results.
- **Smoke run** `scripts/smoke.sh`: a cross-section of about 10 minutes on one catalog, with its
  own report in `reports/smoke/<catalog>/`.
- **Responses the spec defines**: `scripts/iceberg_spec.py` also records each operation's
  status codes (`docs/api/iceberg-rest-responses.txt`, 300 for Iceberg 1.12.0). Every run counts
  the codes the catalog gives the lab's own requests (`iceberg_lab.lab.rest_observed`,
  `results/<catalog>.rest.json`). `docs/rest-conformance.md` shows for each operation which
  defined codes were provoked, and lists any answer the spec does not define.
- **Trend**: `results/history/runs.jsonl` gets one line per catalog and run, with status counts and
  gap causes (`export_reference.py`; `--history` rebuilds it from git). The dashboard shows it in
  "Over the runs", with the causes added and gone since the previous run.
- **Weekly check of the intermittent gaps**: `scripts/flaky_weekly.sh` runs
  `scripts/rerun.sh --flaky 5`, scheduled by a systemd user timer (`scripts/systemd/install.sh`,
  Sundays 03:00). It skips when the lab is down, another run is active, or `results/` has
  uncommitted changes.
- **Flink's non-atomic STATEMENT SET is an intermittent gap** (`flink-statement-set-not-atomic`,
  `flaky = true`). The tables commit in parallel: when B's refusal comes before A's commit, A is
  not committed either. Run alone, the gap reproduces every time (3 of 3). In the parallel full
  run on Polaris it did not, once.
- **Trino's hidden write refusal is an intermittent gap** (`trino-hides-write-refusal`,
  `flaky = true`). On Unity the error once named the S3 AccessDenied (parallel full run). Run
  alone, it hides it every time (3 of 3).
- **Bare-secret OAuth credentials** (`config.oauth_client`): the token request, DuckDB's secret
  and pg_lake's user mapping work without a client id.
- **Full runs of 2026-11-24** with `scripts/run_parallel.sh` (two lanes): Lakekeeper and Polaris
  62 min each, RustFS 51 min, Unity 2 h 17 min, about 4 h in all instead of about 12 h in a row.
  No new failures on any catalog. The first reference run broke off when the disk ran full (the
  JVMs died with SIGBUS) and was run again in full after space was freed.
- **Two intermittent gaps were expected where they cannot occur**, so they always ended up as
  PROBABLY FIXED:
  - DuckDB's exhausted commit retries (`duckdb-commit-retries-exhausted`) on the reference. The
    fixture answers a losing `/transactions/commit` with 204 instead of 409
    (`reference-tx-commit-loses-updates`), so DuckDB never retries there: not seen in 10 runs,
    while the data loss reproduced in 5 of 5. No longer expected on the reference; the cause
    stays for the other catalogs.
  - Spark PURGE racing Lakekeeper's hard delete (`lakekeeper-hard-delete-race`) was expected on
    every catalog. Now on Lakekeeper only.
- **Counters of intermittent gaps follow the tests**: when a test passes without declaring an
  intermittent gap it has a counter for, `flaky_history.json` drops that counter
  (`gaps._update_flaky_history`). Before, the counter stayed: never reported again, but
  `rerun.sh --flaky` kept repeating the test. A failed test keeps its counters, since it may
  have stopped before its check. A run with no intermittent gap and nothing to drop leaves the
  file untouched.
- The weekly timer is not installed by default; `scripts/systemd/install.sh` does that.

## Lab changes 2026-11-24: cause ids, the reference's separator, proxy diagnostics

- **Result rows keep the gap register's cause id** (`cause`, next to the reason text): rewording
  a cause in `docs/gaps.toml` no longer needs test reruns - `scripts/export_reference.py
  --refresh` rewrites every `results/<catalog>.jsonl` and `.rows.json.gz` with the current
  reasons. The register self-test refuses a gap row without a known cause or with a stale reason.
  Used right away: the pg_lake cause now also names the 409 of a concurrent commit.
- **Dangerous namespace names on the reference fixture** are created under the test's parent
  namespace (as on Lakekeeper and Polaris), so DuckDB's catalog listing no longer meets an
  "empty namespace name" left at the top level. The name tests follow the catalog's
  `namespace-separator` from `/v1/config` (the reference fixture: `%2E`; the lab used `%1F`,
  which it refuses with 400).
- **`scripts/rerun.sh`** runs the register self-tests only after the export, not also in the
  per-catalog loop (where they read the old references and reported a false NEW).
- **The fault proxy reports what it saw**: "fault was not triggered" now lists the requests since
  arming and the engine's error; the conflict assertions list the engine's POSTs.
- **The dashboard builder** reads the comparison rows from `results/<catalog>.rows.json.gz` first
  (the reruns' merged state), the report directory only as fallback.
- **Retry intervals after a conflict, 5 runs each on Lakekeeper (4 conflicts in a row)**:
  PyIceberg (about 440 / 600 / 780 ms) and Trino (about 230 / 290 / 510 ms) wait longer every
  time; DuckDB scatters (318-503 ms for the first wait, 259-1284 ms after that) without a clear
  growth - not recorded as a gap, the evidence does not decide it.

## Lab changes 2026-11-23 (3): no lucky passes, forced races, neutral sources everywhere

- **An intermittent gap that did not occur stays a gap**: the report used to show such a run as
  "pass". Now the case keeps the gap; `flaky_history.json` / `results/<catalog>.flaky.json`
  count the runs without it, and after 5 the summary says PROBABLY FIXED (`scripts/rerun.sh`
  prints `FIXED?`). `scripts/rerun.sh --flaky [n]` reruns every intermittent gap n times.
  History entries whose reason the register no longer has (a reworded cause) are dropped.
- **`scripts/rerun.sh` survives being edited while it runs** (everything inside `main()`; bash
  reads a script as it goes, an edit during a long rerun broke its last steps once).
- **Races forced instead of hoped for** (`test_robustness.py`, two new scenarios for every engine):
  the fault proxy lets another client commit right before the engine's commit, once
  (`concurrent_commit`) or before each of 3 attempts (`repeated_conflicts`). Findings:
  - **pg_lake ignores the 409** of Lakekeeper and Polaris: the INSERT succeeds, Postgres shows the
    row, the catalog does not - the known DATA LOSS cause `pglake-ignores-refused-commit`, now
    without any fault injection (bug report draft 02 extended).
  - **DuckDB does retry**: 3 conflicts in a row pass, 5 fail - 4 retries, as many as Iceberg
    Java's and PyIceberg's default. The old cause "DuckDB does not retry a commit after a
    conflict" was wrong; split into `duckdb-commit-retries-exhausted` (intermittent, 4+ writers)
    and `duckdb-compaction-conflict` (compaction while appending, reproducible).
  - All other engines retry correctly.
- **Unity's lost namespace property update is a stable gap**: the race is repeated up to 15
  rounds and stops at the first loss (was: one round, lost in about 1 of 3 runs).
- **Neutral sources for the last blocked read/write matrices** (`test_neutral_source.py`,
  Iceberg Java writes, every engine reads or writes): `test_write_maintained` (append after
  maintenance - counterpart of the 1740 blocked cells of `test_maintenance_cross_write`),
  `test_read_format` (ORC/Avro files), `test_read_tz` (the time zone values). New facts from
  cells that were blocked: Spark, Trino and Flink read ORC and Avro, PyIceberg and Polars read
  ORC; every engine reads the time zone values unshifted; every engine but the PyIceberg family
  on v3 appends after maintenance. The reference writer writes ORC/Avro and microsecond
  timestamps.
- **Views of engines without view DDL** (Polars, Daft, pg_lake) are "not offered" in the read
  matrix instead of "not testable" - there is no such case.
- **Skips that could hide a regression**: `test_listing_with_hidden_tables` fails (instead of
  skipping) when alice may not list after `allow_listing`; `test_location_cannot_reach_another_table`
  skips "no API" only when the catalog's API description lacks the endpoint too.
- **Dashboard**: blocked cases are labelled "not run (a step before failed)" instead of "not
  testable".
- **Docs and docstrings**: CI checks that docstrings match the signatures (Args, Returns, Yields)
  and that library code names what it raises; stale statements fixed (four -> five catalogs,
  capability counts, the reference fixture in the findings summary, a selftest path).

## Lab changes 2026-11-23 (2): tests that skipped too much, CI, reruns

- **Hard-coded catalog skips removed where the catalog does offer the mechanism**:
  - `test_idp_outage` skipped RustFS ("no bearer tokens"), although RustFS logs in through
    Keycloak (token -> STS `AssumeRoleWithWebIdentity` -> temporary SigV4 keys). The proxy now
    sits in front of Keycloak for RustFS as well; all six cases pass (keys obtained before the
    outage keep working, a new login fails fast and recognisably, recovery works).
  - `test_catalog_token_does_not_outlive_identity_token` skipped Polaris and RustFS. Polaris
    (`mixed`) takes the Keycloak token itself: passes. RustFS' STS issues keys for up to 12 h
    on a 1 h Keycloak token: **new SECURITY gap** `rustfs-sts-outlives-identity-token` (as
    Unity's 24 h tokens).
  - `test_storage_faults` gave the reference catalog Unity's reason; own reason now.
  - Checked and kept: Unity has no roles (its API describes only SCIM `/Users` and `/Me`;
    the reason now says so).
  - `test_error_response_is_clean[traversal]` skipped RustFS because the lab could not sign
    the path. Cause: the lab's SigV4 signer (`config.catalog_auth`) used botocore's S3 path
    rule (path signed as sent), RustFS' catalog expects the standard SigV4 rule (each `%`
    encoded again, as PyIceberg and Iceberg Java sign) - every raw REST request with an
    encoded character (`%1F`, `%2F`, non-ASCII) got 403. Fixed; the case runs (400, clean).
    DuckDB signs like the old lab signer (gap `duckdb` nested namespace on RustFS stays).
- **Self-test against hard-coded catalog skips** (`tests/selftest/test_catalog_claims.py`, the
  counterpart of the engine registry check): every skip that decides by catalog name alone
  decides by a list checked against the feature inventory (`offered_by`, the admin adapters,
  `USERS`/`BACKENDS`/`SERVICES`/`UIS`, name chains) or has a written reason; every catalog
  whose API describes a capability runs one of its evidence tests.
- **`scripts/rerun.sh --changed [ref]`** reruns the test files changed since `ref` (default
  HEAD, incl. uncommitted and new ones) and names changed shared code it cannot map.
- **Intermittent gaps**: the "runs in a row without the gap" counter is exported to
  `results/<name>.flaky.json` and a run continues from it when `reports/` is gone.
- **Generated docs from `results/`**: `rest_conformance_doc.py` and `engine_inventory_doc.py`
  read `results/<name>.rows.json.gz` instead of `reports/`; the CI regenerates every generated
  doc (findings, REST conformance, engine and feature inventory) and fails on a diff.
- **Dashboard data encoded per column** (dictionary per column, the page's loader decodes it):
  the results file shrinks from 19 MB to a few MB.
- **Reference catalog in the feature inventory**: `docs/api/reference-c75374c.txt` (31 routes
  of the fixture's `Route` enum, the same as its `/v1/config` endpoints); fifth column in
  `docs/feature-inventory.md` and the dashboard. Unregister is described by two catalogs now,
  so `test_lakekeeper_unregister` became the base test `test_unregister` (reference: works,
  Lakekeeper: 404 gap).
- **`scripts/rerun.sh`**: reruns tests on all (or `CATALOGS=...`) catalogs merged into each
  catalog's full run, exports the references, runs the register self-tests on them, exports
  again and regenerates the docs - the order that had to be done by hand.
- **`export_reference`**: leaves out rows of tests that no longer exist (a merged rerun after a
  rename kept the old name), writes the full comparison rows to `results/<name>.rows.json.gz`
  (~110 kB; `reports/` is not kept), `--run-id` prints the id of a report's full run. The
  dashboard builder falls back to these files.
- **CI** (`.github/workflows/offline.yml`): ruff, format and the offline self-tests on push to
  main and on pull requests. Renovate's checklist points to the gap register.
- **Notebooks lint-clean**: unused imports of the setup cells removed, the interop matrix's
  catch-all marked as intended.
- **Run without help on RustFS** repeated (6574 tests, nothing new).

## Full runs 2026-11-23 (Polaris, Unity, RustFS, reference) and the gap register

- **Five catalogs**: the Apache Iceberg REST fixture (`apache/iceberg-rest-fixture`, Iceberg
  `main` c75374c, JDBC/SQLite in the container) is the fifth catalog (`CATALOG=reference`,
  compose service `reference`, no auth, static S3 keys). All five references in `results/`
  hold the same 11252 tests (self-test). Per catalog pass / known gap / not offered / not
  testable: Lakekeeper 6918/635/988/2471, Polaris 7022/624/1031/2335, Unity 6283/713/1058/2958,
  RustFS 3743/1036/2811/3422, reference 5776/1319/1155/2762. No NEW failure, no fixed gap.
- **Gap register** (`docs/gaps.toml`, `iceberg_lab.lab.gap_register`): every known gap names a
  cause (170: id, summary, severity, `seen_at` versions, flaky, bug report); tests keep their
  selectors and refer to the cause through the shared `Gap` type (`gap`, `gap_table`,
  `expect`, `expect_cause`). Self-tests: canonical file, every cause used, no free-text gaps,
  unknown cause / bad regex refused, causes still seen with the current versions (after an
  upgrade: the list to check again, `scripts/gap_register.py confirm`), every reason in
  `results/` is a cause, all references hold the same tests. The gap list of
  `docs/findings.md` is generated (`scripts/gap_register.py findings`).
- **pydantic** (explicit dependency) for the register, `ResultRow`, `RunManifest` and
  `engine-classified.toml` - not for test parameters, adapters or config.
- **Alternatives**: several gap rows for the same case are alternatives; the error picks the
  row (`find_gap` returns all, `matching()`). Before, the first row won by selectors, so the
  second cause never matched (views on Unity, concurrency on RustFS).
- **REST prefix in one place** (`config.rest_prefix_path`, `rest_base`): a catalog without a
  `prefix` in `/v1/config` got `/v1/<warehouse>/...` in ~80 tests (lab bug, found with the
  fixture). `test_security` treated every unknown catalog as Unity; fixed.
- **Reference findings**: `/transactions/commit` answers concurrent commits with 204 and keeps
  only one (DATA LOSS, bug report 16; new test
  `test_parallel_transaction_commits_are_not_lost` on every catalog that advertises the
  endpoint - Lakekeeper refuses the losers correctly); no location checks (SECURITY); stack
  traces and 500 for client errors, empty error bodies; names it accepts but cannot address
  afterwards (`%2E` separator, Jetty refuses encoded `/`, `\`, `%`); v3 type in a v2 table is a
  500. It is the only catalog with server-side scan planning.
- **pg_lake requires a REST prefix** (bug report 17): with a catalog without prefix every
  request goes to `/v1/<catalog_name>/...`; pg_lake cannot use the fixture at all (central rule
  in `gaps.CATALOG_GAPS`).
- **Unity**: authz lookup intermittently answers 500 (flaky cause); `create_view` on the
  read-only view API answers `NotFoundException`. **PyIceberg** commit retries: the message
  "Metadata location ... changed concurrently" belongs to the same cause.
- **Lab fixes**: DuckDB's function inventory is read without an attached catalog (a leftover
  namespace with an empty name broke the listing on the fixture); `drop_namespace_tree` drops
  only existing tables; `export_reference` takes the manifest of the run with most rows;
  cross-reads of an engine's own result are "self", not "na"; `cleanup_storage` accepts Unity's
  "Catalog not found"; `scripts/engine_inventory_doc.py` imported `reporting` from its old place.
  `rest-conformance.md`, `engine-inventory.md` and the dashboard show the fifth catalog; the
  dashboard data leaves out empty optional fields (data file 19 MB).
- **Open**: the feature inventory (`tests/lab/feature_inventory.py`, `docs/api/`) has no column
  for the reference catalog yet. `reports/rustfs-nohelp` was lost while freeing disk space; its
  reference in `results/` is kept, the dashboard takes those rows from its previous data.
- **Disk**: the runs filled the disk (btrfs); dangling images removed.

## Full run 2026-11-23 (Lakekeeper, all eight engines)

- **pg_lake image fixed** (`pg_lake/Dockerfile`, pg_lake 3.7.0): the build passed, but
  pgduck_server stopped at start ("could not start query engine" in ~320 cases). Three causes:
  libavro/libduckdb are installed into the system libdir and were not copied to the runtime
  stage; the shallow submodule clone has no git tags, so DuckDB called itself v0.0.1 and fetched
  its extensions from a URL that does not exist (now `OVERRIDE_GIT_DESCRIBE` from pg_lake's
  `DUCKDB_VERSION`, checked in the build); no CA certificates in the runtime image. The build
  tree (~30 GB) is removed in the same layer.
- **DuckDB 2.0.1**: `CREATE SCHEMA lk."a"."b"` no longer creates a top-level namespace `b`
  (gap removed, test kept as regression check); ORC/Avro tables are now refused ("only parquet
  is supported") instead of silently getting Parquet files (gap reason updated).
- **Flink 2.3**: no FLOAT NaN/Infinity in SQL - `CAST('NaN' AS FLOAT)` and
  `CAST(CAST('NaN' AS DOUBLE) AS FLOAT)` fail with `NumberFormatException` (new gap; the
  `float_nan` case ran on Flink for the first time). `MERGE INTO` now parses but the planner
  fails validating it (NPE `rowType`) - still refused, the concurrency probe accepts this form.
- **pg_lake 3.7** (first full run; `VERIFIED` and the inventory updated): resolves struct
  fields by name, not by field id (wrong result: a renamed struct field reads as NULL; with no
  name in common the read fails); cannot evolve a struct column (`ALTER TYPE` refused); after
  `VACUUM` expired snapshots its own next INSERT fails (`deletion_queue_pkey`); new
  `lake_iceberg.catalog_stats()` (tested). The partition bug (bug report 08) reproduces with new
  wording. Bug report 06 reproduces again and was wrong in one point: the `{s}` comes from
  Lakekeeper's message (it answers every non-UUID prefix that way), not from pg_lake; the earlier
  "no longer reproduces" came from leftover grants on the long-running authz stack.
- **Adapter code that had not run before**: the pg_lake paths of the nested evolution
  (`map_type.create` names the type `float4`, not `real`) and of the time-zone matrix
  (`insert_case` refused every table) fixed. `PgLakeEngine.insert_case` no longer decides up
  front: on a table pg_lake did not create it attaches and tries, so pg_lake answers itself.
- **pg_lake image**: `libduckdb.so` stripped of debug info (image 2.69 GB -> 1.13 GB).
- **Run manifest**: a compose service key with a trailing comment made the next service's
  image overwrite the previous one (pg-lake was recorded as `eclipse-temurin`, unity-gateway as
  `python`); fixed, and a self-test compares the parser with PyYAML (now a dev dependency).
- **Lab fixes found on a fresh authz stack**: `authz_backends.rest()` used the warehouse name
  as REST prefix when the user may not read `/v1/config` (Lakekeeper hides the warehouse), so
  every refusal was a 400 for a wrong path - now the warehouse id (`rest_prefix`).
  `test_rbac` creates its Keycloak token-role clients itself instead of relying on
  `test_security` having run first. Checked: OpenFGA 1.22.1 shows the same, the bump to 1.23.0
  is not involved.

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

