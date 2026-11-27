#!/usr/bin/env bash
# Full run, one pytest process per test file (fresh JVMs, a hang or crash stays local).
# Starts with an empty report; each file's results are merged in; per-file logs in <out>/logs/.
# Files that mostly wait (LONG_FILES: vended keys expiring, ~1 h) run beside the others; the
# report is written under a lock (reporting.report_lock), so both merge their results.
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
rm -f "$out/results.json" "$out/rest_observed.json" "$out"/logs/*.log "$out"/logs/*.summary.json   # earlier runs (and renamed tests) would mix into the summary
export REQUIRE_ENGINES="${REQUIRE_ENGINES:-all}" REPORT_MERGE=1 COLUMNS=230
# One run id for all pytest processes of this run: every result row names it, and
# <out>/runs.json records what the run was produced with (src/iceberg_lab/lab/manifest.py).
export LAB_RUN_ID="${LAB_RUN_ID:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
echo "run $LAB_RUN_ID on $catalog -> $out"
if [ "${CLEANUP:-1}" = 1 ]; then
  uv run python scripts/cleanup_storage.py --apply || echo "cleanup failed - continuing with the leftovers"
  echo
fi
# This run is in progress (removed on exit): another run's cleanup on the same catalog - e.g.
# gravitino_<catalog>, which sees this catalog's namespaces - leaves them alone.
marker="$out/.running.$$"
echo "$catalog" > "$marker"
trap 'rm -f "$marker"' EXIT
LONG_FILES="tests/security/test_sts_expiry.py"
summary() {  # pytest's summary line (a native library may print after it, e.g. nanobind leak warnings)
  grep -E "[0-9]+ (passed|failed|skipped|errors?)( |,|$)" "$1" | tail -1 | grep . || tail -1 "$1"
}
long_pids=()
for f in $LONG_FILES; do
  name=$(basename "$f" .py)
  echo "$name: started beside the others"
  LAB_SUMMARY="$out/logs/$name.summary.json" uv run pytest "$f" -p no:cacheprovider --tb=line -q "$@" > "$out/logs/$name.log" 2>&1 &
  long_pids+=($!)
done
for f in $(find tests -name 'test_*.py' | sort); do
  case " $LONG_FILES " in *" $f "*) continue ;; esac
  name=$(basename "$f" .py)
  printf '%-32s ' "$name"
  LAB_SUMMARY="$out/logs/$name.summary.json" uv run pytest "$f" -p no:cacheprovider --tb=line -q "$@" > "$out/logs/$name.log" 2>&1
  summary "$out/logs/$name.log"
done
for i in "${!long_pids[@]}"; do
  wait "${long_pids[$i]}"
done
for f in $LONG_FILES; do
  name=$(basename "$f" .py)
  printf '%-32s ' "$name"
  summary "$out/logs/$name.log"
done
echo
# Every pytest process wrote its overview as JSON (gaps.write_summary, LAB_SUMMARY): complete
# lists, and a process that died before its overview counts as NEW.
uv run python scripts/run_summary.py "$out/logs"
rc=$?
echo
# Versioned reference (results/): one line per test, so `git diff results/` shows what changed.
uv run python scripts/export_reference.py "$out" || echo "reference export failed"
# Jumps in the figures against the committed reference: a lab error shows here, whatever it is.
uv run python scripts/reference_shifts.py "$(basename "$(realpath "$out")" | sed 's/^reports$/lakekeeper/')"
exit $rc  # 1: a NEW failure (run_parallel.sh marks the lane)
