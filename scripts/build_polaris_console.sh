#!/usr/bin/env bash
# Build the Polaris Console image (apache/polaris-tools, console/) - the project publishes no image.
# Pinned commit, so the lab's UI test always runs against the same console.
# Usage (on the host, needs git and docker): scripts/build_polaris_console.sh
set -euo pipefail
COMMIT="${POLARIS_TOOLS_COMMIT:-823dddd5fa81385f52d03b6466acd8d4b252cfff}"
TAG="iceberg-lab/polaris-console:${COMMIT:0:12}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git clone --quiet https://github.com/apache/polaris-tools.git "$work/polaris-tools"
git -C "$work/polaris-tools" checkout --quiet "$COMMIT"
docker build -q -f "$work/polaris-tools/console/docker/Dockerfile" -t "$TAG" "$work/polaris-tools/console"
echo "$TAG"
