#!/usr/bin/env bash
# Full runs of several catalogs at the same time (scripts/run_all.sh per catalog, in lanes).
# Each catalog's in-process proxies listen on ports of their own (config.lab_port), and every
# catalog writes its own report, so the runs do not meet. The run without help of a catalog
# shares that catalog's ports: it runs in the same lane, after it.
# Usage (in the dev container):
#   scripts/run_parallel.sh                         all local catalogs, 2 lanes
#   PARALLEL=3 scripts/run_parallel.sh lakekeeper polaris unity
#   scripts/run_parallel.sh rustfs rustfs:nohelp     "<catalog>:nohelp" = LAB_NO_HELP=1
# The reference fixture keeps names it cannot drop: empty it before its run, on the host:
#   docker compose -f .devcontainer/docker-compose.yml --profile reference up -d --force-recreate reference
# Per-lane output: reports/parallel/lane<n>.log; exit code 1 if any catalog had NEW failures.
set -uo pipefail
cd "$(dirname "$0")/.."
main() {
  local specs=("$@")
  [ ${#specs[@]} -eq 0 ] && specs=(lakekeeper polaris unity rustfs rustfs:nohelp reference gravitino gravitino_lakekeeper gravitino_polaris gravitino_unity gravitino_rustfs)
  local lanes=${PARALLEL:-2}
  # One lane per group; a catalog and its run without help stay together.
  local -a groups=()
  local s
  for s in "${specs[@]}"; do
    if [[ $s == *:nohelp ]] && [ ${#groups[@]} -gt 0 ]; then
      local i found=0
      for i in "${!groups[@]}"; do
        if [[ " ${groups[$i]} " == *" ${s%%:*} "* ]]; then groups[$i]+=" $s"; found=1; fi
      done
      [ $found = 1 ] && continue
    fi
    groups+=("$s")
  done
  mkdir -p reports/parallel
  local -a lane_specs=()
  local n=0 g
  for g in "${groups[@]}"; do
    lane_specs[$((n % lanes))]+=" $g"
    n=$((n + 1))
  done
  echo "lanes: $lanes"
  local -a pids=()
  local l
  for l in "${!lane_specs[@]}"; do
    echo "  lane $l:${lane_specs[$l]}"
    run_lane "$l" ${lane_specs[$l]} > "reports/parallel/lane$l.log" 2>&1 &
    pids+=($!)
  done
  local rc=0 p
  for p in "${pids[@]}"; do wait "$p" || rc=1; done
  for l in "${!lane_specs[@]}"; do
    echo "=== lane $l"
    grep -E "^##### |^NEW / unexpected|^FIXED gaps|^PROBABLY FIXED|^  tests/" "reports/parallel/lane$l.log"
  done
  return $rc
}

run_lane() {  # run_lane <lane> <spec>...: the lane's catalogs one after the other
  local lane=$1 spec rc=0
  shift
  for spec in "$@"; do
    local catalog=${spec%%:*} nohelp=0
    [[ $spec == *:nohelp ]] && nohelp=1
    echo "##### $spec start $(date -u +%FT%TZ)"
    CATALOG=$catalog LAB_NO_HELP=$nohelp scripts/run_all.sh | tee "reports/parallel/$catalog$([ $nohelp = 1 ] && echo -nohelp).log"
    # run_all.sh prints the NEW lines under "NEW / unexpected failures:"
    if awk '/^NEW \/ unexpected failures:/{on=1; next} /^FIXED gaps/{on=0} on && /^  tests\//{f=1} END{exit !f}' \
        "reports/parallel/$catalog$([ $nohelp = 1 ] && echo -nohelp).log"; then
      rc=1
    fi
    echo "##### $spec done $(date -u +%FT%TZ)"
  done
  return $rc
}

main "$@"
exit $?
