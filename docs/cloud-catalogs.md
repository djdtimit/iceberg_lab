# Cloud catalogs: Snowflake and Databricks

The lab can run part of its suite against Iceberg REST catalogs in the cloud. Such a catalog is a
profile like the local ones (`CATALOG=<name>`), but only when its variables are set, and it runs
a narrower session.

**Status: not yet run against a real account.** The profiles follow the providers' documented
endpoints and authentication. The first run against each provider shows what still needs
adjusting (`tests/catalog/test_rest_conformance.py` alone is a good first step).

## Profiles

| Profile | Catalog | `LAB_<NAME>_URI` (example) | Authentication |
|---|---|---|---|
| `snowflake_open` | Snowflake Open Catalog (managed Apache Polaris) | `https://<account>.snowflakecomputing.com/polaris/api/catalog` | `CREDENTIAL` = client id and secret of a service connection; scope `PRINCIPAL_ROLE:ALL` |
| `snowflake_horizon` | Snowflake Horizon Catalog, Iceberg REST endpoint (Snowflake-managed Iceberg tables, read and write) | `https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog` | `CREDENTIAL` = the programmatic access token (PAT) alone, exchanged at `<URI>/v1/oauth/tokens`; `ROLE` = a role the PAT may use (scope `session:role:<ROLE>`) |
| `databricks` | Databricks Unity Catalog | `https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest` | `TOKEN` (personal access token) or `CREDENTIAL` of a service principal (OAuth M2M, `/oidc/v1/token`, scope `all-apis`) |

Check each URI against the provider's current documentation before the first run.

Notes from the providers' documentation (November 2026):

- **Snowflake Horizon** and **Open Catalog** share the path `/polaris/api/catalog`. Horizon is
  Snowflake's own account.
  - `<org>-<account>` is the account identifier, not the account locator:
    `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`
  - `WAREHOUSE` is the database name. It must be in capitals if the database was created
    without quotes.
  - The PAT goes into `CREDENTIAL` without a client id. The lab sends it as `client_secret`,
    like Iceberg clients do with a `credential` that has no colon.
  - Access is through vended credentials.
  - The role needs USAGE on the database and schema. Writing additionally needs SELECT,
    INSERT, UPDATE, DELETE and TRUNCATE on the tables, CREATE ICEBERG TABLE on the schema,
    USAGE on the external volume, and CREATE SCHEMA on the database.
  - Snowflake documents what is not supported: CTAS from an engine, equality deletes,
    branches and tags, registering tables, and rollback. The lab's tests then show these as
    gaps or "not offered".
- **Databricks**: the metastore needs external data access enabled, and the principal needs
  `EXTERNAL USE SCHEMA` on the schema. There is no credential vending on workspaces with
  default storage. Databricks recommends Iceberg clients 1.9.2 or later.

## Documented endpoint support

Neither Snowflake nor Databricks publishes a list of the Iceberg REST operations it implements.
The table below records what their documentation shows (November 2026):

- **yes**: documented, or used in a documented example;
- **no**: documented as unsupported;
- **–**: not mentioned.

The first run of `test_rest_conformance.py` (`spec_endpoint`, `/v1/config`'s `endpoints`, the
spec responses) gives the actual answer. It is the result that counts, not this table.

| Operation | Snowflake Horizon | Databricks Unity Catalog |
|---|---|---|
| `GET /v1/config` | yes (`warehouse=<DATABASE>`; prefix = the database name) | yes (`warehouse=<catalog>`; paths `/v1/catalogs/<catalog>/...`) |
| Token endpoint | yes, `<URI>/v1/oauth/tokens` (PAT as `client_secret`) | Databricks' own `/oidc/v1/token`, or a PAT as bearer token |
| List namespaces | yes (Spark `SHOW NAMESPACES`) | – |
| Create / drop namespace | – for Snowflake-managed tables (yes for catalog-linked databases) | – |
| Load namespace, HEAD, properties | – | – |
| List tables | yes | – |
| Create table | yes (no base location) | yes (Spark `CREATE TABLE ... USING iceberg`) |
| Load table | yes (`loadTable`, vended credentials in `config`) | yes (vended credentials in `config`, valid 1 h) |
| Commit (append, update, delete, add column) | yes (via Spark DML) | yes for managed Iceberg tables |
| Drop table | yes (purge not mentioned) | – |
| Rename table | yes (needs OWNERSHIP) | – |
| Register table | **no** | – |
| `GET .../credentials`, credential refresh | – (vending through `X-Iceberg-Access-Delegation`) | refresh: yes, except Iceberg 1.9.0 |
| Scan planning | – | – |
| `POST /transactions/commit` | **no** (for catalog-linked databases; – otherwise) | – |
| Views | – | – |
| Remote signing | – (vended credentials only) | – (vended credentials only) |

Further documented limits:

- **Snowflake Horizon:**
  - only Snowflake-managed Iceberg tables, read and write, v2 and v3;
  - no equality deletes, branches or tags, CTAS from an engine, rollback, or upgrade from v2
    to v3;
  - unquoted names are upper case, and a name in the wrong case gives 404;
  - no S3-compatible storage outside AWS;
  - Snowflake announced billing for the feature for the second half of 2026.
- **Databricks:**
  - external clients write only managed Iceberg tables; foreign Iceberg and Delta with Iceberg
    reads are read-only;
  - no tables with `UUID`, `Fixed`, `TIME` or nested required struct fields;
  - no table maintenance by external clients (expiring snapshots, removing orphan files);
  - equality deletes are refused (v3: copy-on-write or deletion vectors);
  - duplicate data files are refused;
  - new tables are v2 unless created with `format-version` 3.

  The lab's type and operation matrices will show these as gaps or "not offered".

Sources: [Snowflake Horizon](https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon), [Snowflake externally managed tables via Horizon](https://docs.snowflake.com/en/user-guide/externally-managed-iceberg-tables-access-horizon-irc), [Databricks Iceberg clients](https://docs.databricks.com/aws/en/external-access/iceberg), [Databricks credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending), [Databricks Iceberg v3](https://docs.databricks.com/aws/en/iceberg/iceberg-v3).

## Setup

Put the variables in `.env.cloud` in the repository root. Git ignores the file, so it is never
committed. The environment wins over the file.

```sh
# .env.cloud - never commit
LAB_DATABRICKS_URI=https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest
LAB_DATABRICKS_WAREHOUSE=<catalog name>
LAB_DATABRICKS_TOKEN=<personal access token>

LAB_SNOWFLAKE_OPEN_URI=https://<account>.snowflakecomputing.com/polaris/api/catalog
LAB_SNOWFLAKE_OPEN_WAREHOUSE=<catalog name>
LAB_SNOWFLAKE_OPEN_CREDENTIAL=<client id>:<client secret>

LAB_SNOWFLAKE_HORIZON_URI=https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog
LAB_SNOWFLAKE_HORIZON_WAREHOUSE=<DATABASE NAME>
LAB_SNOWFLAKE_HORIZON_CREDENTIAL=<programmatic access token>
LAB_SNOWFLAKE_HORIZON_ROLE=<role the token may use>
```

Variables per profile (`LAB_<NAME>_...`):

| Variable | Meaning |
|---|---|
| `URI` | Iceberg REST URI, without `/v1` |
| `WAREHOUSE` | catalog or warehouse name sent to `/v1/config` |
| `TOKEN` | bearer token |
| `CREDENTIAL` | `client_id:client_secret`, or a bare secret (Horizon: the PAT), for OAuth client credentials (instead of `TOKEN`) |
| `SCOPE` | OAuth scope (default per profile) |
| `ROLE` | Horizon: builds the scope `session:role:<ROLE>` |
| `TOKEN_URL` | token endpoint on the same host (default per profile, else `<URI>/v1/oauth/tokens`) |

Use a test account or a catalog of its own, without production data. Each test creates its own
`test_<hex>` namespace and drops it afterwards. `scripts/cleanup_storage.py` drops leftovers;
on a cloud catalog it does not touch storage.

## What runs

`tests/conftest.py` keeps two lists:

- `REMOTE_SAFE`: the test files that run. These are REST conformance, namespaces, operations,
  types, schema evolution and the engines' read/write matrix.
- `REMOTE_ENGINES`: the engines that take part. These are PyIceberg, Polars, Daft, DuckDB and
  Spark. Trino, Flink and pg_lake are set up in their containers for the lab's catalogs.

All other tests are deselected. They are not run and not reported as "not offered".

They are left out for these reasons:

- **Security tests** (name injection, other tables' locations, path tricks) look like attacks.
  A provider's terms for security testing decide whether they are allowed.
- **Fault proxies, IdP outage, the lab's users and roles** rebuild or disturb the catalog.
- **RustFS-specific storage tests** have no counterpart in a provider's storage.

## Running

```sh
# in the dev container; the results go to reports/<profile>/
CATALOG=databricks uv run pytest tests/catalog/test_rest_conformance.py
CATALOG=databricks scripts/run_all.sh
```

`/v1/config` and `test_rest_conformance.py::test_spec_endpoint` show which endpoints the catalog
offers and advertises.

The export writes `results/<profile>.jsonl`. It holds the account's catalog names and error
texts, so decide before committing it.

## Sources

- Snowflake: [Access Apache Iceberg tables with an external engine through Snowflake Horizon Catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon)
- Databricks: [Access Databricks tables from Apache Iceberg clients](https://docs.databricks.com/aws/en/external-access/iceberg), [credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)

## Costs

REST calls to the Iceberg REST endpoints need no Snowflake virtual warehouse. Data files are written to the provider's storage
through vended credentials, which costs storage and requests. The tests write a few rows each,
and their namespaces are dropped with purge.
