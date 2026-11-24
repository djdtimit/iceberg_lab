#!/usr/bin/env bash
# Install the weekly flaky check (scripts/flaky_weekly.sh) as a systemd user timer:
# Sundays 03:00 local time, caught up after a missed run (Persistent=true).
#   scripts/systemd/install.sh            install and enable
#   scripts/systemd/install.sh --remove   disable and remove
# Status: systemctl --user list-timers iceberg-lab-flaky.timer
set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
if [ "${1:-}" = --remove ]; then
  systemctl --user disable --now iceberg-lab-flaky.timer 2>/dev/null || true
  rm -f "$dir/iceberg-lab-flaky.service" "$dir/iceberg-lab-flaky.timer"
  systemctl --user daemon-reload
  echo "removed"; exit 0
fi
mkdir -p "$dir"
cat > "$dir/iceberg-lab-flaky.service" <<UNIT
[Unit]
Description=Iceberg lab: rerun the intermittent gaps (scripts/flaky_weekly.sh)

[Service]
Type=oneshot
WorkingDirectory=$repo
ExecStart=$repo/scripts/flaky_weekly.sh
UNIT
cat > "$dir/iceberg-lab-flaky.timer" <<UNIT
[Unit]
Description=Iceberg lab: weekly check of the intermittent gaps

[Timer]
OnCalendar=Sun 03:00
Persistent=true

[Install]
WantedBy=timers.target
UNIT
systemctl --user daemon-reload
systemctl --user enable --now iceberg-lab-flaky.timer
systemctl --user list-timers iceberg-lab-flaky.timer --no-pager
