# Exploration scripts (evidence, not tests)

One-off scripts used to check findings before a test was written. Only those referenced by bug
reports and docs are kept, plus a cleanup script:

| Script | Purpose |
|---|---|
| `check_gap_patterns.py` | checks the gap patterns against real error messages (`docs/test-architecture.md`) |
| `probe_parent_encoding.py` | evidence for `docs/bug-reports/10` (double-encoded `parent`) |
| `probe_rustfs_granular.py`, `probe_rustfs_scope.py` | evidence for `docs/bug-reports/11` (catalog rights and IAM on RustFS) |
| `cleanup_polaris_probe_roles.py` | removes leftover probe roles in Polaris |

They run in the dev container with the catalog from `CATALOG`:

    docker cp scripts/probes/<script>.py iceberg-lab-dev-1:/tmp/ && \
      docker exec -w /workspaces/iceberg_lab iceberg-lab-dev-1 bash -lc 'CATALOG=polaris uv run python /tmp/<script>.py'

Some create test users or namespaces and clean up afterwards; read them before running.
