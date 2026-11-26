"""Collect the overviews of a run's pytest processes (``LAB_SUMMARY`` files of gaps.write_summary).

run_all.sh gives every pytest process ``LAB_SUMMARY=<logs>/<name>.summary.json`` next to its
``<name>.log``; rerun.sh one file per rerun. This prints the sections and exits 1 when there is
a NEW failure - or when a log has no summary: that pytest process died before its overview
(a crash would otherwise look like a clean file).

    uv run python scripts/run_summary.py reports/polaris/logs          # run_all.sh, run_parallel.sh
    uv run python scripts/run_summary.py --prefixed --kinds new,fixed,cleanup <file>   # rerun.sh

Sections: ``new``, ``fixed``, ``probably_fixed``, ``cleanup`` (default: all, in that order).
``--prefixed`` prints one ``NEW <test>`` / ``FIXED <test>`` / ``FIXED? <test>`` /
``CLEANUP <test>`` line per entry instead of headed sections.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

#: Section -> (heading in the full run's output, prefix for --prefixed).
SECTIONS = {
    "new": ("NEW / unexpected failures:", "NEW"),
    "fixed": ("FIXED gaps (remove the entries):", "FIXED"),
    "probably_fixed": (
        "PROBABLY FIXED intermittent gaps (not seen in 5+ runs in a row - check, remove):",
        "FIXED?",
    ),
    "cleanup": ("Cleanup errors (teardown; the results keep the tests' own outcome):", "CLEANUP"),
}


def collect(paths: list[Path]) -> dict[str, list[str]]:
    """The entries of all summaries; a log without its summary counts as NEW.

    Args:
        paths: Summary files, or log directories (each ``<name>.log`` needs
            ``<name>.summary.json``).

    Returns:
        ``{section: [entry, ...]}`` in file order.
    """
    out: dict[str, list[str]] = {k: [] for k in SECTIONS}
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            for log in sorted(p.glob("*.log")):
                if log.name.startswith("rerun"):
                    continue  # rerun.sh's own logs: not part of the full run
                summary = log.with_suffix(".summary.json")
                if summary.exists():
                    files.append(summary)
                else:
                    out["new"].append(f"{log}: no summary - pytest did not finish (see the log)")
        else:
            files.append(p)
    for f in files:
        if not f.exists():
            out["new"].append(f"{f}: no summary - pytest did not finish")
            continue
        data = json.loads(f.read_text())
        for k in SECTIONS:
            out[k].extend(data.get(k, []))
    return out


def main(argv: list[str] | None = None) -> int:
    """Print the selected sections.

    Args:
        argv: Command line (default: sys.argv[1:]).

    Returns:
        1 if a NEW entry was selected and found, else 0.
    """
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("paths", nargs="+", type=Path)
    ap.add_argument("--kinds", default=",".join(SECTIONS))
    ap.add_argument("--prefixed", action="store_true")
    args = ap.parse_args(argv)
    kinds = [k for k in args.kinds.split(",") if k]
    unknown = set(kinds) - set(SECTIONS)
    if unknown:
        ap.error(f"unknown kinds {sorted(unknown)}")
    found = collect(args.paths)
    for k in kinds:
        heading, prefix = SECTIONS[k]
        if not args.prefixed:
            print(heading)
        for entry in found[k]:
            print(f"{prefix} {entry}" if args.prefixed else f"  {entry}")
    return 1 if "new" in kinds and found["new"] else 0


if __name__ == "__main__":
    sys.exit(main())
