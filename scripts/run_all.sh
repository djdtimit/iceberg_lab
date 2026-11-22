#!/usr/bin/env bash
# Full run, one pytest process per test file (fresh JVMs, a hang or crash stays local).
# Starts with an empty report; each file's results are merged in; per-file logs in <out>/logs/.
# Usage (in the dev container): [CATALOG=polaris] scripts/run_all.sh [extra pytest args]
# Without help (only the tests of the engines with a lab help, iceberg_lab.config.HELPS -
# RustFS, Lakekeeper, Unity): CATALOG=rustfs LAB_NO_HELP=1 scripts/run_all.sh
# First removes test leftovers of the catalog (scripts/cleanup_storage.py; CLEANUP=0 skips it):
# thousands of orphaned objects slow RustFS down until large reads/writes time out.
# Reports: reports/ for Lakekeeper (default), reports/<catalog>/ for other catalogs.
set -uo pipefail
cd "$(dirname "$0")/.."
catalog="${CATALOG:-lakekeeper}"
out=reports; [ "$catalog" != lakekeeper ] && out="reports/$catalog"
# LAB_NO_HELP=1: the run without lab help (iceberg_lab.config.WITH_HELP false), own report.
if [ "${LAB_NO_HELP:-0}" = 1 ]; then
  out="reports/$catalog-nohelp"
  helped=$(CATALOG="$catalog" uv run python -c "from iceberg_lab.config import helped_engines; print(' '.join(sorted(helped_engines())))")
  if [ -z "$helped" ]; then echo "no lab help on $catalog (iceberg_lab.config.HELPS) - nothing to run"; exit 0; fi
  echo "run without help on $catalog: tests of $helped"
fi
mkdir -p "$out/logs"
rm -f "$out/results.json" "$out"/logs/*.log   # logs of earlier runs (and of renamed tests) would mix into the summary
export REQUIRE_ENGINES="${REQUIRE_ENGINES:-all}" REPORT_MERGE=1 COLUMNS=230
# One run id for all pytest processes of this run: every result row names it, and
# <out>/runs.json records what the run was produced with (tests/lab/manifest.py).
export LAB_RUN_ID="${LAB_RUN_ID:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
echo "run $LAB_RUN_ID on $catalog -> $out"
if [ "${CLEANUP:-1}" = 1 ]; then
  uv run python scripts/cleanup_storage.py --apply || echo "cleanup failed - continuing with the leftovers"
  echo
fi
for f in $(find tests -name 'test_*.py' | sort); do
  name=$(basename "$f" .py)
  printf '%-32s ' "$name"
  uv run pytest "$f" -p no:cacheprovider --tb=line -q "$@" > "$out/logs/$name.log" 2>&1
  # pytest's summary line (a native library may print after it, e.g. nanobind leak warnings)
  grep -E "[0-9]+ (passed|failed|skipped|errors?)( |,|$)" "$out/logs/$name.log" | tail -1 | grep . || tail -1 "$out/logs/$name.log"
done
echo
echo "NEW / unexpected failures:"
for log in "$out"/logs/*.log; do
  awk '/NEW \/ unexpected failures/{on=1; next} /known gaps reproduced|FIXED gaps|intermittent gaps/{on=0} on && /^  tests\//' "$log"
done
echo "FIXED gaps (remove the entries):"
for log in "$out"/logs/*.log; do
  awk '/FIXED gaps/{on=1; next} /intermittent gaps|short test summary/{on=0} on && /^  tests\//' "$log"
done
echo "PROBABLY FIXED intermittent gaps (not seen in 5+ runs in a row - check, remove):"
for log in "$out"/logs/*.log; do
  awk '/PROBABLY FIXED intermittent/{on=1; next} /short test summary/{on=0} on && /^  tests\//' "$log"
done
echo
# Versioned reference (results/): one line per test, so `git diff results/` shows what changed.
uv run python scripts/export_reference.py "$out" || echo "reference export failed"
