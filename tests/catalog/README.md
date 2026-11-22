# tests/catalog – Catalog level

What the catalog itself can do and how it behaves – without an engine, with raw REST where the
catalog is judged: Iceberg REST conformance, consistency (invalid commits, lost updates),
administration, namespaces, transactions across several tables, scan planning and the catalog
features (base features and – with `@pytest.mark.specific` – special features of single catalogs).

| File | Content | Test functions |
|---|---|---|
| [`test_catalog_admin.py`](test_catalog_admin.py) | Catalog administration - base capabilities, the same tests on every catalog. | 7 |
| [`test_catalog_consistency.py`](test_catalog_consistency.py) | Catalog consistency (base tests on catalog level, raw Iceberg REST - no engine involved). | 4 |
| [`test_catalog_features.py`](test_catalog_features.py) | Catalog features: what does the catalog under test offer beyond the engine matrices? | 44 |
| [`test_namespaces.py`](test_namespaces.py) | Namespaces per engine: create, nest, properties, refuse dropping a non-empty one, drop. | 1 |
| [`test_rest_conformance.py`](test_rest_conformance.py) | REST conformance (base tests on catalog level, raw Iceberg REST - no engine or client library). | 7 |
| [`test_scan_planning.py`](test_scan_planning.py) | Capability tests: server-side scan planning and fine-grained (row/column) access control. | 4 |
| [`test_transactions.py`](test_transactions.py) | Multi-table transactions: one logical change to two tables - all or nothing? | 2 |

## Running

```bash
CATALOG=lakekeeper uv run pytest tests/catalog            # whole folder (CATALOG: lakekeeper|polaris|rustfs|unity)
CATALOG=polaris REPORT_DIR=/tmp/x uv run pytest tests/catalog/<file>.py -k <filter>   # targeted, own report
```

Always run targeted runs with `REPORT_DIR`, otherwise they overwrite the report of the last full run.

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
