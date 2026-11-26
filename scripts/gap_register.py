"""Maintain the gap register (docs/gaps.toml, ``iceberg_lab.lab.gap_register``).

    uv run python scripts/gap_register.py format     rewrite the register in canonical form
    uv run python scripts/gap_register.py findings   regenerate the gap list in docs/findings.md
    uv run python scripts/gap_register.py confirm    after full runs: move ``seen_at`` forward
                                                     for every cause a reference reproduced
    uv run python scripts/gap_register.py outdated   causes last seen with another version
    uv run python scripts/gap_register.py draft <id> facts for an upstream bug report of a cause
                                                     (iceberg_lab.lab.bug_drafts), to stdout

``confirm`` reads ``results/<name>.jsonl`` and the versions of ``results/<name>.manifest.json``
(the run that produced it): a cause whose reason a reference reports as a known gap was seen
with those versions. Causes a reference reports as fixed are listed - remove their entries
(and the test's gap) by hand.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from iceberg_lab.lab import gap_register as gr

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
FINDINGS = ROOT / "docs" / "findings.md"


def write(causes: dict[str, gr.Cause]) -> None:
    """Write the register (canonical form).

    Args:
        causes: The register.
    """
    gr.REGISTER_FILE.write_text(gr.dump(causes))


def findings() -> None:
    """Regenerate the gap list of docs/findings.md (between the markers)."""
    block = gr.findings_markdown(gr.load(), gr.hits(RESULTS))
    FINDINGS.write_text(gr.replace_block(FINDINGS.read_text(), block))
    print(f"{FINDINGS.relative_to(ROOT)}: gap list regenerated")


def confirm() -> None:
    """Move ``seen_at`` forward for the causes the references reproduced."""
    causes = gr.load()
    hit = gr.hits(RESULTS)
    moved, fixed = 0, set()
    for path in sorted(RESULTS.glob("*.manifest.json")):
        ref = path.name.removesuffix(".manifest.json")
        versions = gr.versions_of(json.loads(path.read_text()))
        for cid, per_ref in hit.items():
            status = per_ref.get(ref, {})
            if cid not in causes:
                continue
            if status.get("fixed"):
                fixed.add(cid)
            if not status.get("gap"):
                continue
            c = causes[cid]
            seen = {k: versions.get(k, v) for k, v in c.seen_at.items()}
            if seen != c.seen_at:
                causes[cid] = c.model_copy(update={"seen_at": seen})
                moved += 1
    write(causes)
    print(f"seen_at moved forward: {moved} updates")
    for cid in sorted(fixed):
        print(f"reported FIXED somewhere - check and remove: {cid}")


def outdated() -> None:
    """List the causes last seen with another version than the lab runs now."""
    for c, comp, seen, now in gr.outdated(gr.load().values(), gr.current_versions()):
        print(f"{c.id}: {comp} {seen} -> {now}")


if __name__ == "__main__":
    commands = {
        "format": lambda: write(gr.load()),
        "findings": findings,
        "confirm": confirm,
        "outdated": outdated,
    }
    if len(sys.argv) == 3 and sys.argv[1] == "draft":
        from iceberg_lab.lab.bug_drafts import draft

        print(draft(sys.argv[2]))
        sys.exit(0)
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        sys.exit(__doc__)
    commands[sys.argv[1]]()
