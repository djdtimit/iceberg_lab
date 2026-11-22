# [pyiceberg] `list_namespaces(parent)` double-encodes the `parent` query parameter

**Component:** PyIceberg REST catalog (`RestCatalog.list_namespaces`)
**Versions:** PyIceberg 0.12.0; catalogs Lakekeeper v0.15.0 and Apache Polaris 1.9.0 (both behave correctly)

## Summary

`list_namespaces(parent)` builds the `parent` query parameter with `_encode_namespace_path`,
the function meant for the URL **path** (it percent-encodes each part and joins with `%1F`).
`requests` then encodes the query parameter again. Every namespace part with a character that
needs encoding (non-ASCII letters, `%`, `\`, ...) therefore reaches the catalog still encoded:

```
list_namespaces(("ns", "Grüße"))
-> NoSuchNamespaceException: Namespace with name 'ns.Gr%C3%BC%C3%9Fe' does not exist
```

The children of such a namespace cannot be listed - and tools that walk a namespace tree
(drop recursively, sync, list everything) fail on it.

## Steps to reproduce

1. `catalog.create_namespace(("ns", "Grüße"))` (Lakekeeper or Polaris; both allow nested
   namespaces and such names).
2. `catalog.list_namespaces(("ns", "Grüße"))`

## Expected

The catalog receives `parent=ns%1FGr%C3%BC%C3%9Fe` (encoded once) and returns the children
(an empty list here). A request with `requests.get(url, params={"parent": "ns\x1fGrüße"})`
does exactly that and gets 200 from both catalogs.

## Actual

`NoSuchNamespaceException` - the catalog sees the literal name `Gr%C3%BC%C3%9Fe`.

## Notes

- Fix: for the query parameter join the raw parts with `\x1f` and let the HTTP library encode
  once (only the path needs `_encode_namespace_path`).
- Found by the lab's name-validation test (`tests/security/test_security.py::test_name_is_refused_or_safe`),
  which therefore checks namespaces with raw, correctly encoded requests.
