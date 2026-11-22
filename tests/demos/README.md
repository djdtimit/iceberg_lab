# tests/demos – Demo notebooks

Runs every example notebook on the chosen catalog and checks the expected outputs per catalog
(area "Demo notebooks", not counted in the comparison).

| File | Content | Test functions |
|---|---|---|
| [`test_notebooks.py`](test_notebooks.py) | Execute every example notebook top to bottom on the catalog under test (all four catalogs). | 1 |

## Running

```bash
CATALOG=lakekeeper uv run pytest tests/demos            # whole folder (CATALOG: lakekeeper|polaris|rustfs|unity)
CATALOG=polaris REPORT_DIR=/tmp/x uv run pytest tests/demos/<file>.py -k <filter>   # targeted, own report
```

Always run targeted runs with `REPORT_DIR`, otherwise they overwrite the report of the last full run.

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
