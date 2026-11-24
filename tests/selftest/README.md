# tests/selftest – Lab self-tests

Check the lab itself (area "Lab self-test", not counted in the comparison): the gap mechanism lets
nothing through, central rules are narrow, docstrings are complete, the "not offered" claims still
hold (version pin + active probes), the inventories of the catalogs (endpoints) and engines
(functions) are up to date, and the lab adapters for Unity (token and STS shim) open or close nothing
by themselves.

| File | Content | Test functions |
|---|---|---|
| [`test_engine_inventory.py`](test_engine_inventory.py) | Engine inventory: what each engine offers for Iceberg, read from the engine itself. | 1 |
| [`test_feature_inventory.py`](test_feature_inventory.py) | The feature inventory (tests/lab/feature_inventory.py) is complete and current. | 8 |
| [`test_lab_adapters.py`](test_lab_adapters.py) | Lab self-test: the lab's own adapters between the catalogs and Keycloak / RustFS. | 9 |
| [`test_lab_infrastructure.py`](test_lab_infrastructure.py) | Lab self-test: the run infrastructure fits together (ports, cloud profiles, smoke run). | 10 |
| [`test_lab_selftest.py`](test_lab_selftest.py) | Self-tests of the lab itself: its mechanisms must never hide a failure they do not document. | 34 |
| [`test_na_claims.py`](test_na_claims.py) | Lab self-test: every "not offered" claim is still true. | 6 |

## Running

```bash
CATALOG=lakekeeper uv run pytest tests/selftest            # whole folder (CATALOG: lakekeeper|polaris|rustfs|unity)
CATALOG=polaris REPORT_DIR=/tmp/x uv run pytest tests/selftest/<file>.py -k <filter>   # targeted, own report
```

Always run targeted runs with `REPORT_DIR`, otherwise they overwrite the report of the last full run.

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
