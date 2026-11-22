# [Lakekeeper] Empty error bodies for unknown paths/methods; any value accepted for loadTable's `snapshots`

**Component:** Lakekeeper Iceberg REST catalog
**Version:** v0.15.1

## Summary

1. A request to a path the server does not have answers `404` with an empty body; a method a path
   does not support answers `405` with an empty body. The Iceberg REST spec defines every error
   answer as an error document `{"error": {"message", "type", "code"}}`; Iceberg clients
   (`ErrorResponseParser`) cannot parse an empty body and report a parse error instead of the cause.
   Polaris, Unity (0.7, after unitycatalog#1835) and the RustFS catalog answer both with an error
   document.
2. `GET .../tables/{table}?snapshots=<value>` accepts any value. The spec defines `all` and `refs`;
   the other three catalogs answer `400` for an undefined value.

## Steps to reproduce

```
GET  /catalog/v1/{prefix}/namespaces/{ns}/no-such-route   -> 404, body empty
PATCH /catalog/v1/{prefix}/namespaces/{ns}                -> 405, body empty
GET  /catalog/v1/{prefix}/namespaces/{ns}/tables/{t}?snapshots=some -> 200
```

## Expected

1. `404`/`405` with `{"error": {"message": "...", "type": "...", "code": 404|405}}`.
2. `400` (BadRequestException) for a value other than `all`/`refs`.

## Notes

- Lab tests: `tests/catalog/test_rest_conformance.py::test_error_body[unknown_route|unsupported_method]`,
  `::test_snapshots_value` (CATALOG=lakekeeper). Overview: `docs/rest-conformance.md`.
