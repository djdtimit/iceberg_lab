# [Gravitino] Iceberg REST catalog with a REST backend turns the backend's client errors into HTTP 500

**Component:** Gravitino Iceberg REST service, `lakehouse-iceberg` catalog with `catalog-backend = rest`
**Version:** Gravitino 1.3.1; backends Lakekeeper v0.15.1, Apache Polaris 1.9.0, Unity Catalog OSS 0.7.0, RustFS 1.0.6 (built-in Iceberg catalog)
**Severity:** inconsistent - a refusal looks like a server fault

## Summary

When the backend refuses a request with a client error (400, 406, 414 ...), Gravitino answers
the client with **500**. The body keeps the backend's message and exception type. Clients
decide by status: they report a server fault instead of the refusal, and some retry the
request (DuckDB and pg_lake show only "HTTP 500").

Examples seen in the lab (direct answer of the backend -> answer through Gravitino):

| Request | Backend directly | Through Gravitino |
|---|---|---|
| create namespace `a.b` (Lakekeeper) | 400 `Namespace parts cannot contain .` | 500, same message |
| create table, format version 3 (RustFS) | 406 `unsupported Iceberg table format-version: 3` | 500 `Unable to process (code: 406, type: UnsupportedOperationException): ...` |
| commit with an incompatible type change (RustFS) | 400 | 500 `{"message":"Malformed request: schema field 1 has an incompatible type evolution","type":"BadRequestException","code":500}` |
| drop table with `purgeRequested=true` (RustFS) | 406 `purgeRequested=true is not supported` | 500 `Unable to process (code: 406, ...)` |
| create table with a 300-character name (Polaris) | 414 | 500 `Unable to process (code: 414, type: null)` |
| create table on v2 with a v3-only type (Lakekeeper; Polaris and Unity answer 500 themselves) | 400 | 500 `BadRequestException: Malformed request: Invalid schema for v2 ...` |

The stack in the body shows where it comes from: Iceberg's `RESTSessionCatalog` /
`HTTPClient` raises the matching exception (`BadRequestException`, `RESTException` with
`code: 406`), and Gravitino's `FederatedCatalogWrapper.callRemoteCatalog` passes it on. The
answer then gets 500, also for `BadRequestException`.

## Steps to reproduce

1. Gravitino 1.3.1 with a `lakehouse-iceberg` catalog `fed`: `catalog-backend = rest`,
   `uri` and `warehouse` of any Iceberg REST catalog (e.g. Lakekeeper).
2. `POST /iceberg/v1/fed/namespaces` with `{"namespace": ["a.b"]}`.
3. The same request directly at the backend.

## Expected

Gravitino passes the backend's client error on with its status (400, 406, 414, ...), as it
already passes on the message - or maps it to the matching Iceberg REST error
(`BadRequestException` -> 400, `UnsupportedOperationException` -> 406).

## Notes

- Lab tests (booked to cause `gravitino-rest-backend-client-error-500`):
  `tests/engines/test_types.py::test_v3_types_rejected_on_v2_tables`,
  `tests/catalog/test_catalog_consistency.py::test_invalid_commit_is_refused`,
  `::test_recreate_at_dropped_location`,
  `tests/security/test_security.py::test_error_response_is_clean[huge_name]`
  (`CATALOG=gravitino_lakekeeper|polaris|unity|rustfs`).
- Related: Gravitino's own catalogs answer a v3 type in a v2 table with 500 too
  (`gravitino-v3-type-in-v2-500`), and every error answer carries the Java stack trace
  (`gravitino-error-stack-traces`).
- Facts: `uv run python scripts/gap_register.py draft gravitino-rest-backend-client-error-500`.
- Re-check against the newest Gravitino release before filing.
