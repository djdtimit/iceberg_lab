# tests/engines – Engine matrices

What the eight engines do with Iceberg tables – the same on every catalog (base tests, counted in
the comparison): operations, types, partitions and evolution, maintenance, file formats and files
per specification, concurrent commits, views, WAP, incremental reads, statistics, layout, branches,
re-creating with an open session, performance (only correctness counts). Read matrices depend on
their write tests (`iceberg_lab.lab.results.skip_if_not_produced`); the neutral read source writes every state with
the reference implementation.

| File | Content | Test functions |
|---|---|---|
| [`test_branch_isolation.py`](test_branch_isolation.py) | Branch isolation (base test, every engine): commits on a branch stay on that branch. | 2 |
| [`test_concurrency.py`](test_concurrency.py) | Concurrent commits: Iceberg's optimistic concurrency, per engine and across engines. | 5 |
| [`test_evolution.py`](test_evolution.py) | Partitioning and evolution across engines: what one engine writes, every engine must read. | 6 |
| [`test_file_formats.py`](test_file_formats.py) | Data file formats: ORC and Avro besides Parquet - who writes them, who reads them. | 2 |
| [`test_file_spec.py`](test_file_spec.py) | Files as the Iceberg spec requires them (base test, every engine that writes). | 4 |
| [`test_incremental.py`](test_incremental.py) | Incremental reads, changelogs and streaming per engine. | 4 |
| [`test_interop.py`](test_interop.py) | Cross-engine interoperability: every engine writes, every engine reads. | 1 |
| [`test_layout.py`](test_layout.py) | File layout per writer: does the engine honour the table's sort order and target file size? | 2 |
| [`test_maintenance.py`](test_maintenance.py) | Table maintenance matrix: the same maintenance tasks for every engine, format v2 and v3. | 3 |
| [`test_neutral_source.py`](test_neutral_source.py) | Neutral read source for operations, partitions, evolution and maintenance (base, every reader). | 6 |
| [`test_operations.py`](test_operations.py) | Operation matrix: every data and table operation an engine offers, on format v2 and v3. | 3 |
| [`test_performance.py`](test_performance.py) | Performance comparison: same workloads, every engine, wall-clock seconds. | 2 |
| [`test_recreate_session.py`](test_recreate_session.py) | Drop and re-create under the same name while an engine has the table open (base, every engine). | 2 |
| [`test_statistics.py`](test_statistics.py) | Iceberg statistics: who can compute them, and who uses statistics another engine wrote. | 3 |
| [`test_types.py`](test_types.py) | Data type matrix: format version x data type x writer engine x reader engine. | 5 |
| [`test_v3_features.py`](test_v3_features.py) | Iceberg format v3 features beyond data types. | 6 |
| [`test_views.py`](test_views.py) | Iceberg views across engines: who can create one, and who can query whose view? | 3 |
| [`test_wap.py`](test_wap.py) | Write-Audit-Publish (WAP) per engine: write to a branch, audit it, then publish it to main. | 1 |

## Running

```bash
CATALOG=lakekeeper uv run pytest tests/engines            # whole folder (CATALOG: lakekeeper|polaris|rustfs|unity)
CATALOG=polaris REPORT_DIR=/tmp/x uv run pytest tests/engines/<file>.py -k <filter>   # targeted, own report
```

Always run targeted runs with `REPORT_DIR`, otherwise they overwrite the report of the last full run.

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
