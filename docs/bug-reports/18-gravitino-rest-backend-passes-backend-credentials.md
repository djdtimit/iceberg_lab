# [Gravitino] Iceberg REST catalog with a REST backend hands out the backend's write credentials to a user with only SELECT

**Component:** Gravitino Iceberg REST service, `lakehouse-iceberg` catalog with `catalog-backend = rest`
**Version:** Gravitino 1.3.1; backends Lakekeeper v0.15.1 and Unity Catalog OSS 0.7.0
**Severity:** security - a read grant becomes write access to the table's files

## Summary

With authorization on in the Iceberg REST service
(`gravitino.iceberg-rest.disable-rest-authz = false`), Gravitino checks its own privileges
for a catalog that fronts another Iceberg REST catalog. It then passes on the storage
credentials the backend vends, unchanged. The backend does not know what Gravitino allowed. If
the backend vends write-capable keys (Lakekeeper without its own authorization, Unity), a user
with only `SELECT_TABLE` in Gravitino gets keys that write and delete the table's files.

On Gravitino's own `lakehouse-iceberg` catalogs (JDBC backend, `credential-providers = s3-token`)
the same grant gets keys that are refused on a write. Setting a credential provider on the
REST-backend catalog does not change anything: the response still carries the backend's keys.

## Steps to reproduce

1. Gravitino 1.3.1 with authorization (`gravitino.authorization.enable = true`), OAuth login,
   and `gravitino.iceberg-rest.disable-rest-authz = false`.
2. A Lakekeeper warehouse with an S3 storage profile that vends STS credentials, without
   Lakekeeper authorization.
3. In Gravitino, a `lakehouse-iceberg` catalog `fed_lakekeeper`:
   `catalog-backend = rest`, `uri = http://lakekeeper:8181/catalog`, `warehouse = <warehouse>`.
4. A table `ns.t` in it. User `alice` gets a role with `USE_CATALOG`, `USE_SCHEMA` and only
   `SELECT_TABLE` on `ns.t`.
5. As alice:
   ```
   GET /iceberg/v1/fed_lakekeeper/namespaces/ns/tables/t
   X-Iceberg-Access-Delegation: vended-credentials
   ```
   200, with `s3.access-key-id`, `s3.secret-access-key` and `s3.session-token` in `config`
   and in `storage-credentials`.
6. With these keys: `PUT s3://<bucket>/<table location>/data/evil.bin`. **The PUT succeeds.**

Variation: add `credential-providers = s3-token` (plus `s3-role-arn` and
`s3-token-service-endpoint`) to `fed_lakekeeper`, or create a new catalog with them from the
start. Step 5 returns the same access key id as a `loadTable` directly at Lakekeeper.

Same result with Unity Catalog OSS 0.7.0 as backend. With Apache Polaris as backend it does not
happen: there the forwarded token is alice's, and Polaris scopes the keys by its own grants.

## Expected

Gravitino has made the authorization decision, so the credentials it returns should match
it. Possible ways:

- vend its own credentials scoped to the granted privileges, as it does for its own catalogs,
  when a credential provider is configured on the REST-backend catalog;
- otherwise, do not pass on backend credentials to a user who may only read (no
  `vended-credentials`, or remote signing).

At the least, the documentation of `catalog-backend = rest` should say that storage
credentials are passed through from the backend unchanged.

## Notes

- Lab test: `tests/security/test_authz.py::test_select_grant_vends_read_only_credentials`
  (`CATALOG=gravitino_lakekeeper` and `gravitino_unity`).
- The same test passes against Lakekeeper and Unity directly, and against Gravitino's own
  catalog (profile `gravitino`).
- Facts: `uv run python scripts/gap_register.py draft gravitino-rest-backend-credentials-not-narrowed`.
- Re-check against the newest Gravitino release before filing.
