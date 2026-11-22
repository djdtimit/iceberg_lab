# [pyiceberg] `geometry(OGC:CRS84)` (spec form, written by Iceberg Java 1.12) cannot be parsed - table cannot be loaded

**Component:** PyIceberg (`pyiceberg/types.py`, `GEOMETRY_REGEX` / `GEOGRAPHY_REGEX`)
**Versions:** PyIceberg 0.12.0; catalog Apache Polaris 1.9.0 (Iceberg 1.12.0); storage S3-compatible (RustFS 1.0.5)

## Summary

The Iceberg spec writes the geo types as `geometry(C)` and `geography(C, A)` with the CRS
**unquoted**, e.g. `geometry(OGC:CRS84)`, `geography(OGC:CRS84, spherical)`. Iceberg Java 1.12
(Polaris 1.9) serialises the default CRS explicitly in that form. PyIceberg 0.12 only accepts
a quoted CRS:

```python
GEOMETRY_REGEX = re.compile(r"geometry(?:\(\s*['\"]([^'\"]+)['\"]\s*\))?$")
```

so every `load_table` of such a table fails:

```
pyiceberg.exceptions.ValidationError: Could not parse geometry(OGC:CRS84) into a GeometryType
pyiceberg.exceptions.ValidationError: Could not parse geography(OGC:CRS84, spherical) into a GeographyType
```

The table is not only unreadable for its geo column - PyIceberg (and Polars/Daft, which load
through it) cannot open the table at all.

## Steps to reproduce

1. Polaris 1.9 (or any catalog on Iceberg Java 1.12 that accepts geo types).
2. Create a v3 table with a `geometry` column (Spark, DuckDB or Trino).
3. `catalog.load_table("ns.t")` with PyIceberg 0.12.0.

## Expected

The table loads; `geometry(OGC:CRS84)` is parsed as `GeometryType(crs="OGC:CRS84")`. Quotes
should be optional in both regexes (the spec examples are unquoted).

## Actual

`ValidationError` while parsing the schema in the LoadTable response.

## Notes

- Lab proof: `tests/engines/test_types.py::test_write[pyiceberg-v3-geometry]` (and geography, polars,
  daft, trino) with `CATALOG=polaris`.
- Before Polaris 1.9 the CRS was omitted (plain `geometry`) and loading worked; behind this
  error PyIceberg 0.12 still writes geo columns as binary (schema mismatch on append).
