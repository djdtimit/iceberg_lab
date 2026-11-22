# tests/storage – Storage and fault behaviour

RustFS as storage under all catalogs (S3 behaviour, large files, AWS semantics of checksums,
conditional writes, multipart and STS) and what happens when something fails: faults in front of
the catalog (lost responses, 503, catalog gone) and under the catalog (storage does not answer).
Faults are produced with proxies – no test stops a container.

| File | Content | Test functions |
|---|---|---|
| [`test_robustness.py`](test_robustness.py) | Robustness against catalog faults: lost commit responses, transient 503, catalog down. | 2 |
| [`test_rustfs.py`](test_rustfs.py) | RustFS as Iceberg storage: the S3 features engines rely on, and large files per engine. | 7 |
| [`test_storage_faults.py`](test_storage_faults.py) | Storage faults seen by the catalog: what does it do when RustFS fails under it? | 1 |
| [`test_storage_s3.py`](test_storage_s3.py) | The storage under every catalog: RustFS's S3 API where Iceberg clients rely on AWS semantics. | 4 |

## Running

```bash
CATALOG=lakekeeper uv run pytest tests/storage            # whole folder (CATALOG: lakekeeper|polaris|rustfs|unity)
CATALOG=polaris REPORT_DIR=/tmp/x uv run pytest tests/storage/<file>.py -k <filter>   # targeted, own report
```

Always run targeted runs with `REPORT_DIR`, otherwise they overwrite the report of the last full run.

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
