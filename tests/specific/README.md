# tests/specific – Engine-specific tests

What concerns only one engine (area "Engine-specific", not counted in the comparison): pg_lake
(extension, attached tables, join with Postgres tables), DuckDB's three-part schema names, Polars'
lazy API, Spark `DROP ... PURGE`, Flink with two warehouses, PyIceberg partition pruning. Anything
several engines can do is in the matrices under `engines/`.

| File | Content | Test functions |
|---|---|---|
| [`test_dataframes.py`](test_dataframes.py) | Engine-specific: Polars' lazy API on an Iceberg table (not part of the comparison). | 1 |
| [`test_duckdb.py`](test_duckdb.py) | Engine-specific: DuckDB's three-part schema names (not part of the comparison). | 1 |
| [`test_flink.py`](test_flink.py) | Engine-specific: Flink with both warehouses in one session (not part of the comparison). | 1 |
| [`test_pglake.py`](test_pglake.py) | Engine-specific: Postgres + pg_lake (not part of the comparison). | 6 |
| [`test_pyiceberg.py`](test_pyiceberg.py) | Engine-specific: PyIceberg (not part of the comparison), on both warehouses. | 2 |
| [`test_spark.py`](test_spark.py) | Engine-specific: Spark (not part of the comparison), on both warehouses (``sts``, ``signing``). | 1 |

## Running

```bash
CATALOG=lakekeeper uv run pytest tests/specific            # whole folder (CATALOG: lakekeeper|polaris|rustfs|unity)
CATALOG=polaris REPORT_DIR=/tmp/x uv run pytest tests/specific/<file>.py -k <filter>   # targeted, own report
```

Always run targeted runs with `REPORT_DIR`, otherwise they overwrite the report of the last full run.

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
