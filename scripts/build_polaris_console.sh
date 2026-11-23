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
dockerfile="$work/polaris-tools/console/docker/Dockerfile"
# COPY --chmod needs BuildKit; without buildx the legacy builder refuses it - copy, then chmod.
if ! docker buildx version >/dev/null 2>&1; then
  sed -i -E 's|^COPY (--chown=\S+ )?--chmod=([0-7]+) (\S+) (\S+)$|COPY \1\3 \4\nRUN chmod \2 \4|' "$dockerfile"
fi
docker build -q -f "$dockerfile" -t "$TAG" "$work/polaris-tools/console"
echo "$TAG"
