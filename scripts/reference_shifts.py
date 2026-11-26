"""Warn when a reference's figures jump against its committed state.

A lab error that distorts the results (a test compared that should not be, a status booked
wrongly, a skip that hides a whole engine) shows as a jump in the figures, whatever its cause.
This compares each ``results/<name>.rows.json.gz`` with the same file in git ``HEAD``, per
engine and the catalog's own tests, the way the dashboard counts (theme "Not compared" left
out, a case blocked by another engine counted at that engine):

* reliability (works / offered) moved by more than ``POINTS`` percentage points;
* the number of offered cases changed by more than ``SHARE`` (and at least ``MIN_CASES``);
* an engine's cases appeared or disappeared.

A jump can be right (a fixed gap, a new engine version): it is a warning to look at, before
the commit. Exit code 0 either way.

    uv run python scripts/reference_shifts.py                 # every reference
    uv run python scripts/reference_shifts.py polaris unity   # these
"""

from __future__ import annotations

import gzip
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
POINTS = 5
SHARE = 0.10
MIN_CASES = 10


def tally(rows: list[dict]) -> dict[str, dict[str, int]]:
    """Works and offered cases per engine (``""`` = tests without an engine).

    Args:
        rows: Rows of a ``rows.json.gz``.

    Returns:
        ``{engine: {"works": n, "offered": n, "planned": n}}``.
    """
    out: dict[str, Counter] = {}
    for r in rows:
        if r.get("theme") == "Not compared" or r["status"] == "self":
            continue
        engine = r.get("engine") or ""
        by = r.get("blocked_by") or ""
        if r["status"] == "blocked" and by and by != (engine or "catalog"):
            continue  # counted at the step that failed, not here
        t = out.setdefault(engine, Counter())
        t["planned"] += 1
        if r["status"] in ("pass", "fixed"):
            t["works"] += 1
        if r["status"] in ("pass", "fixed", "gap", "new"):
            t["offered"] += 1
    return {e: dict(t) for e, t in out.items()}


def shifts(old: list[dict], new: list[dict]) -> list[str]:
    """The jumps between two states of a reference.

    Args:
        old: Rows of the committed state.
        new: Rows of the current state.

    Returns:
        One line per engine with a jump, empty if none.
    """
    a, b = tally(old), tally(new)
    out = []
    for engine in sorted(set(a) | set(b)):
        label = engine or "catalog tests"
        x, y = a.get(engine, {}), b.get(engine, {})
        if not x or not y:
            out.append(f"{label}: cases {'appeared' if y else 'disappeared'}")
            continue
        ox, oy = x.get("offered", 0), y.get("offered", 0)
        notes = []
        if ox and oy:
            rx, ry = x.get("works", 0) / ox, y.get("works", 0) / oy
            if abs(ry - rx) * 100 > POINTS:
                notes.append(f"reliability {rx:.0%} -> {ry:.0%}")
        if abs(oy - ox) >= MIN_CASES and abs(oy - ox) > SHARE * max(ox, oy):
            notes.append(f"offered {ox} -> {oy} of {y.get('planned', 0)} planned")
        if notes:
            out.append(f"{label}: " + ", ".join(notes))
    return out


def committed(path: Path) -> list[dict] | None:
    """The rows of a reference as in git ``HEAD``.

    Args:
        path: The reference file.

    Returns:
        The rows, or None when git has no such file (new reference, or no git).
    """
    try:
        data = subprocess.run(
            ["git", "show", f"HEAD:{path.relative_to(ROOT)}"],
            cwd=ROOT,
            capture_output=True,
            check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    return json.loads(gzip.decompress(data))


def main(argv: list[str] | None = None) -> int:
    """Print the jumps of the given (default: all) references.

    Args:
        argv: Reference names (default: sys.argv[1:]).

    Returns:
        0.
    """
    names = argv if argv is not None else sys.argv[1:]
    paths = (
        [RESULTS / f"{n}.rows.json.gz" for n in names]
        if names
        else sorted(RESULTS.glob("*.rows.json.gz"))
    )
    for path in paths:
        name = path.name.removesuffix(".rows.json.gz")
        if not path.exists():
            continue
        old = committed(path)
        if old is None:
            print(f"SHIFT? {name}: no committed state to compare (new reference, or no git)")
            continue
        for line in shifts(old, json.loads(gzip.decompress(path.read_bytes()))):
            print(f"SHIFT {name} {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
