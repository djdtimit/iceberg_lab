#!/usr/bin/env bash
# Weekly check of the intermittent gaps (run on the host by the systemd user timer that
# scripts/systemd/install.sh sets up): scripts/rerun.sh --flaky 5 in the dev container, so a gap
# that is gone for good shows up as FIXED? without anyone starting it.
# It does nothing (and says why in the log) when
#   - the lab's dev container is not running,
#   - another lab run is active (run_all.sh, rerun.sh, run_parallel.sh, smoke.sh),
#   - results/ has uncommitted changes (the rerun would mix into someone's work).
# Log: reports/flaky-weekly/<date>.log; the results land in results/ as after any rerun.
set -uo pipefail
cd "$(dirname "$0")/.."
log="reports/flaky-weekly/$(date -u +%Y-%m-%d).log"
mkdir -p "$(dirname "$log")"
DC=(docker compose -f .devcontainer/docker-compose.yml)
{
  echo "flaky check $(date -u +%FT%TZ)"
  if [ -z "$("${DC[@]}" ps -q dev 2>/dev/null)" ]; then
    echo "skipped: the dev container is not running"; exit 0
  fi
  if "${DC[@]}" exec -T dev pgrep -f 'run_all.sh|rerun.sh|run_parallel.sh|smoke.sh' >/dev/null; then
    echo "skipped: another lab run is active"; exit 0
  fi
  if [ -n "$(git status --porcelain -- results/)" ]; then
    echo "skipped: results/ has uncommitted changes"; exit 0
  fi
  "${DC[@]}" exec -T -u vscode -w /workspaces/iceberg_lab dev scripts/rerun.sh --flaky 5
  echo "exit $? at $(date -u +%FT%TZ)"
} >> "$log" 2>&1
