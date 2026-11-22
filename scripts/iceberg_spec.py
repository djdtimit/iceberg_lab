"""Record what the Iceberg specification defines: REST endpoints and data types.

    uv run python scripts/iceberg_spec.py            # the release of engines.ICEBERG_VERSION
    uv run python scripts/iceberg_spec.py 1.13.0     # another release

Reads ``open-api/rest-catalog-open-api.yaml`` and ``format/spec.md`` of the release tag from
github.com/apache/iceberg and writes docs/api/iceberg-rest-spec.txt and docs/api/iceberg-types.txt.
tests/selftest/test_iceberg_spec.py then checks that every endpoint has a conformance test, every
type a type case, and that the recorded release is the lab's Iceberg version - so a new Iceberg
release (a Renovate PR for ICEBERG_VERSION) shows what it adds.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "api" / "iceberg-rest-spec.txt"
TYPES_OUT = ROOT / "docs" / "api" / "iceberg-types.txt"
BASE = "https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-{v}/"


def endpoints(yaml_text: str) -> list[str]:
    """``METHOD /path`` of every operation in the OpenAPI document.

    A line scan of the ``paths`` block (two-space path keys, four-space method keys), so no YAML
    library is needed.

    Args:
        yaml_text: The OpenAPI document.

    Returns:
        The endpoints, sorted.
    """
    out, path = [], None
    for line in yaml_text.splitlines():
        if m := re.match(r"^  (/\S+):\s*$", line):
            path = m.group(1)
        elif re.match(r"^\S", line):
            path = None
        elif path and (m := re.match(r"^    (get|post|put|delete|head|patch):", line)):
            out.append(f"{m.group(1).upper()} {path}")
    return sorted(out)


def types(spec_md: str) -> list[str]:
    """Type names of the format spec: the bold names of its "Nested Types", "Semi-structured
    Types" and "Primitive Types" sections, without parameters (``decimal(P,S)`` -> ``decimal``).

    Args:
        spec_md: ``format/spec.md``.

    Returns:
        The type names, sorted.
    """
    out, on = set(), False
    for line in spec_md.splitlines():
        if line.startswith("#### "):
            on = line[5:].strip() in ("Nested Types", "Semi-structured Types", "Primitive Types")
        elif on:
            out.update(re.findall(r"\*\*`([a-z_]+)", line))
    return sorted(out)


def main(version: str | None = None) -> None:
    """Download the spec of ``version`` and write :data:`OUT` and :data:`TYPES_OUT`.

    Args:
        version: Iceberg release (default: ``ICEBERG_VERSION`` of src/iceberg_lab/engines.py).
    """
    if version is None:
        src = (ROOT / "src" / "iceberg_lab" / "engines.py").read_text()
        version = re.search(r'^ICEBERG_VERSION = "([\d.]+)"', src, re.MULTILINE).group(1)
    for out, file, parse in (
        (OUT, "open-api/rest-catalog-open-api.yaml", endpoints),
        (TYPES_OUT, "format/spec.md", types),
    ):
        r = requests.get(BASE.format(v=version) + file, timeout=30)
        r.raise_for_status()
        names = parse(r.text)
        out.write_text(f"# apache-iceberg-{version}\n" + "\n".join(names) + "\n")
        print(f"{out.relative_to(ROOT)}: {len(names)} entries of apache-iceberg-{version}")


if __name__ == "__main__":
    main(*sys.argv[1:2])
