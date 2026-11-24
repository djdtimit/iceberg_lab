#!/usr/bin/env bash
# Smoke run: a quick cross-section of the suite on one catalog (~10 min), for checking a change
# or a new version before a full run. Its report goes to reports/smoke/<catalog>/ - it never
# touches the full run's report or results/.
# Usage (in the dev container): [CATALOG=polaris] scripts/smoke.sh [extra pytest args]
# The files are SMOKE_FILES below; tests/selftest/test_lab_infrastructure.py checks that they exist.
set -uo pipefail
cd "$(dirname "$0")/.."
SMOKE_FILES=(
  tests/catalog/test_rest_conformance.py     # every spec endpoint, defined behaviour
  tests/catalog/test_namespaces.py           # namespace tasks of every engine
  tests/catalog/test_transactions.py         # multi-table commits
  tests/engines/test_interop.py              # every engine writes, every engine reads
  tests/engines/test_views.py
  tests/storage/test_robustness.py           # lost responses, 503, catalog down, conflicts
  tests/security/test_access_delegation.py   # vended credentials / remote signing
  tests/selftest                             # the lab's own mechanisms
)
catalog="${CATALOG:-lakekeeper}"
export REPORT_DIR="reports/smoke/$catalog" REPORT_MERGE=0 COLUMNS=230
export LAB_RUN_ID="${LAB_RUN_ID:-smoke-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
echo "smoke run on $catalog -> $REPORT_DIR"
exec uv run pytest "${SMOKE_FILES[@]}" -p no:cacheprovider --tb=line -q "$@"
