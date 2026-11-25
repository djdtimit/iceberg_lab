#!/usr/bin/env bash
# Re-run some tests on several catalogs and merge them into each catalog's last full run, then
# bring everything that reads the results up to date - in the order that works:
#   1. the tests, per catalog, under the id of its full run (the rows replace the old ones);
#   2. the references in results/ (scripts/export_reference.py: tests that no longer exist
#      are left out);
#   3. the findings gap list, then the register self-tests, which read results/ (so they ran on
#      the new references) and check that list, and the export once more to record their result;
#   4. the generated docs (findings gap list, REST conformance, engine inventory).
# Usage (in the dev container):
#   scripts/rerun.sh tests/security/test_idp_outage.py tests/x.py::test_y
#   CATALOGS="polaris unity" scripts/rerun.sh tests/engines/test_views.py
#   scripts/rerun.sh --changed [<git ref>]   test files changed since <ref> (default HEAD, incl.
#                                             uncommitted and new ones)
#   scripts/rerun.sh --flaky [<n>]           each catalog's intermittent gaps (results/<c>.flaky.json)
#                                             <n> times (default 5): every run counts in the
#                                             history, so "occurred k of n times" is visible and
#                                             a gap gone for good is listed as FIXED?
# Without help: LAB_NO_HELP=1 CATALOGS=rustfs scripts/rerun.sh ... (report reports/rustfs-nohelp).
# Ends with the NEW / FIXED / FIXED? lines per catalog; exit code 1 if any catalog has a NEW
# failure. FIXED?: an intermittent gap not seen in gaps.FLAKY_FIXED_AFTER runs in a row.
set -uo pipefail
# Everything runs inside main(): bash reads a script while it runs it, so editing this file
# during a long rerun would otherwise break the rest of it. A function is read in full first.
main() {
cd "$(dirname "$0")/.."
[ $# -gt 0 ] || { sed -n '2,21p' "$0"; exit 2; }
if [ "$1" = --changed ]; then
  ref="${2:-HEAD}"
  mapfile -t files < <( { git diff --name-only "$ref" -- tests; git ls-files --others --exclude-standard -- tests; } \
    | grep -E '(^|/)test_[^/]*\.py$' | sort -u | while read -r f; do [ -f "$f" ] && echo "$f"; done)
  other=$(git diff --name-only "$ref" -- src tests/lab tests/conftest.py | head -5)
  [ -n "$other" ] && echo "note: shared code changed too (tests using it are not picked): $other"
  [ ${#files[@]} -gt 0 ] || { echo "no test file changed since $ref"; exit 0; }
  echo "changed test files: ${files[*]}"
  set -- "${files[@]}"
fi
repeat=1
if [ "$1" = --flaky ]; then
  repeat="${2:-5}"
  set -- FLAKY
fi
catalogs="${CATALOGS:-lakekeeper polaris unity rustfs reference gravitino gravitino_lakekeeper gravitino_polaris gravitino_unity gravitino_rustfs}"
export REQUIRE_ENGINES="${REQUIRE_ENGINES:-all}" REPORT_MERGE=1 COLUMNS=230
suffix=""; [ "${LAB_NO_HELP:-0}" = 1 ] && suffix="-nohelp"
dir() { if [ "$1$suffix" = lakekeeper ]; then echo reports; else echo "reports/$1$suffix"; fi; }

declare -A run
for c in $catalogs; do
  run[$c]=$(uv run python scripts/export_reference.py --run-id "$(dir "$c")") || exit 2
done

new=0
for c in $catalogs; do
  out=$(dir "$c")
  mkdir -p "$out/logs"
  echo "=== $c (run ${run[$c]})"
  tests=("$@")
  if [ "$1" = FLAKY ]; then  # node ids of this catalog's intermittent gaps whose file still exists
    mapfile -t tests < <(python3 -c 'import json, os, sys
ids = {k.split(" :: ")[0] for k in json.load(open(sys.argv[1]))}
print("\n".join(sorted(i for i in ids if os.path.exists(i.split("::")[0]))))' "results/$c$suffix.flaky.json")
    [ ${#tests[@]} -gt 0 ] || { echo "no intermittent gaps"; continue; }
  fi
  for i in $(seq 1 "$repeat"); do
    [ "$repeat" -gt 1 ] && echo "--- run $i of $repeat"
    # The register self-tests read results/, which the export below has not updated yet - they
    # run after it (step 3); here they would only report the old references.
    CATALOG=$c LAB_RUN_ID=${run[$c]} uv run pytest "${tests[@]}" -p no:cacheprovider --tb=line -q \
      --deselect tests/selftest/test_gap_register.py > "$out/logs/rerun.log" 2>&1
    grep -E "[0-9]+ (passed|failed|skipped|errors?)( |,|$)" "$out/logs/rerun.log" | tail -1
    if awk '/NEW \/ unexpected failures/{on=1; next} /known gaps reproduced|FIXED gaps|intermittent gaps|PROBABLY FIXED/{on=0} on && /^  tests\//' "$out/logs/rerun.log" | sed 's/^/NEW /' | grep .; then
      new=1
    fi
    awk '/^FIXED gaps/{on=1; next} /intermittent gaps|PROBABLY FIXED|short test summary/{on=0} on && /^  tests\//' "$out/logs/rerun.log" | sed 's/^/FIXED /'
  done
  awk '/PROBABLY FIXED/{on=1; next} /short test summary/{on=0} on && /^  tests\//' "$out/logs/rerun.log" | sed 's/^/FIXED? /'
done

export_all() { for c in $catalogs; do uv run python scripts/export_reference.py "$(dir "$c")" | tail -1; done; }
export_all
uv run python scripts/gap_register.py findings  # the register self-tests check it
for c in $catalogs; do
  # Exit code 5 = nothing collected: the run without help deselects the register self-tests.
  CATALOG=$c LAB_RUN_ID=${run[$c]} uv run pytest tests/selftest/test_gap_register.py \
    -p no:cacheprovider -q --tb=line > "$(dir "$c")/logs/rerun-selftest.log" 2>&1
  rc=$?
  if [ $rc -ne 0 ] && [ $rc -ne 5 ]; then
    echo "register self-tests failed on $c: $(dir "$c")/logs/rerun-selftest.log"; new=1
  fi
done
export_all
uv run python scripts/gap_register.py findings
uv run python scripts/rest_conformance_doc.py
uv run python scripts/engine_inventory_doc.py
exit $new
}
main "$@"
exit $?
