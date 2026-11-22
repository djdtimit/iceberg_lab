# tests/lab – Lab helper modules

No tests, but the shared building blocks the tests rely on: authorization backends per catalog,
report, raw REST client, neutral writer (Iceberg Java core), fault proxy, data generator and the
catalogs' feature inventory. `tests/conftest.py` puts this folder on the import path; the modules
are imported by name (`from authz_backends import backend`).

| File | Content |
|---|---|
| [`authz_backends.py`](authz_backends.py) | Permission tests on every catalog: one backend per catalog behind the same interface. |
| [`datagen.py`](datagen.py) | Generate ``n`` rows ``(id, md5(id) as name, id * 0.5 as amount)`` with each engine's OWN write path. |
| [`feature_inventory.py`](feature_inventory.py) | Feature inventory: every endpoint of the four catalogs -> a capability -> tests (evidence). |
| [`reference_writer.py`](reference_writer.py) | Neutral source: tables written with Iceberg's Java core API (the reference implementation). |
| [`reporting.py`](reporting.py) | Engine comparison report: collects every test result and writes a side-by-side overview. |
| [`rest_client.py`](rest_client.py) | Raw Iceberg REST client for catalog-level tests: no engine or client library in between, so a library bug can never be booked as a catalog gap. |
| [`storage_proxy.py`](storage_proxy.py) | Fault-injecting HTTP proxy (tests/storage/test_storage_faults.py: in front of RustFS; tests/security/test_idp_outage.py: in front of a token endpoint). |

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
