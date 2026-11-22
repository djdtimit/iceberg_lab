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

## 3. Themes

The 50+ compared areas are grouped into eight themes (`tests/lab/reporting.py`, `THEMES`; a lab self-test keeps every compared area in exactly one theme). The report gives reliability per theme and the mean over the themes, each theme counting equally - without that, the large matrices (the type matrix alone is about a third of all cases) decide every total. The dashboard starts from the themes.

## 4. Assignment in the report (`tests/lab/reporting.py`)

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

## 5. Open

- Missing infrastructure: partition pruning (an engine-neutral measurement of the files read, e.g.
  a logging S3 endpoint all eight engines use – pg_lake, Trino and DuckDB have their own, statically
  configured S3 access), query across two warehouses (every engine needs a second catalog).
- Keycloak outage on the catalog side (the catalog itself can no longer reach Keycloak): a proxy in
  front changes the address and thus the tokens' issuer; checked is the outage of the token endpoint
  from the clients' point of view.

The history of the rework (2026-11-15 to 2026-11-20) is in [CHANGELOG.md](../CHANGELOG.md).

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
