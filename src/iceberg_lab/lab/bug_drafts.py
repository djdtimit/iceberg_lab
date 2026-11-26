"""Facts for an upstream bug report of one gap cause (``scripts/gap_register.py draft <id>``).

The drafts in ``docs/bug-reports/`` are written by hand; this collects what they are built
from, so nothing has to be looked up in five places:

* the cause from the register (summary, severity, component and version it was seen with);
* where it shows: per reference in ``results/``, the tests booked to it;
* where the same tests pass - the comparison an upstream report needs ("works on X");
* the raw error of each affected test, as the engine or catalog reported it: the ``error``
  field of the full rows in ``results/<name>.rows.json.gz`` (recorded since 2026-11-25), else
  the run logs in ``reports/`` (``--tb=line``: the first ``E`` line of a failure is the original
  exception, before the lab turned it into the cause's text);
* the command that reproduces it in the lab.

The result is a starting point: the reproduction without the lab, the expected behaviour and
the title stay to be written.
"""

from __future__ import annotations

import gzip
import json
import re
from collections import Counter
from pathlib import Path

from iceberg_lab.lab import gap_register as gr

ROOT = Path(__file__).resolve().parents[3]
RESULTS = ROOT / "results"
REPORTS = ROOT / "reports"
#: Most tests listed per reference (and raw errors shown in all).
SHOWN = 5


def report_dir(reference: str) -> Path:
    """Report directory of a reference (``reports/`` is Lakekeeper's, see export_reference).

    Args:
        reference: Reference name, e.g. ``gravitino_unity``.

    Returns:
        The directory.
    """
    return REPORTS if reference == "lakekeeper" else REPORTS / reference


def references(results_dir: Path = RESULTS) -> dict[str, dict[str, dict]]:
    """Every reference's rows by test id.

    Args:
        results_dir: ``results/``.

    Returns:
        ``{reference: {test: row}}``.
    """
    out = {}
    for path in sorted(results_dir.glob("*.jsonl")):
        rows = (json.loads(line) for line in path.read_text().splitlines())
        out[path.stem] = {r["test"]: r for r in rows}
    return out


def affected(refs: dict[str, dict[str, dict]], cid: str) -> dict[str, list[str]]:
    """The tests booked to a cause, per reference.

    Args:
        refs: :func:`references`.
        cid: Cause id.

    Returns:
        ``{reference: [test, ...]}`` (references without any left out).
    """
    out = {}
    for ref, rows in refs.items():
        tests = sorted(t for t, r in rows.items() if gr.row_cause(r) == cid)
        if tests:
            out[ref] = tests
    return out


def _outcome(row: dict) -> str:
    """A row's status, with the cause for a gap.

    Args:
        row: Reference row.

    Returns:
        E.g. ``pass`` or ``gap (pglake-ignores-refused-commit)``.
    """
    cid = gr.row_cause(row)
    return f"{row['status']} ({cid})" if cid else row["status"]


def elsewhere(refs: dict[str, dict[str, dict]], hit: dict[str, list[str]]) -> dict[str, Counter]:
    """How the affected tests end on the references without the cause.

    Args:
        refs: :func:`references`.
        hit: :func:`affected`.

    Returns:
        ``{reference: Counter(outcome)}`` over the affected test ids it ran (a gap with its
        cause: another cause there is worth a line in the report too).
    """
    tests = {t for ts in hit.values() for t in ts}
    out = {}
    for ref, rows in refs.items():
        if ref in hit:
            continue
        counts = Counter(_outcome(rows[t]) for t in tests if t in rows)
        if counts:
            out[ref] = counts
    return out


_LOCATION = re.compile(r"^/\S+\.py:\d+: ")


def _blocks(section: list[str]) -> list[list[str]]:
    """Split a ``--tb=line`` FAILURES/ERRORS section into one block per failure.

    Args:
        section: The section's lines.

    Returns:
        The blocks (each ends with its ``path:line: message`` line).
    """
    blocks, cur = [], []
    for line in section:
        cur.append(line)
        if _LOCATION.match(line):
            blocks.append(cur)
            cur = []
    return blocks


def _first_error(block: list[str]) -> str:
    """The original exception of a failure block (its first ``E`` line and continuation).

    Args:
        block: One failure block.

    Returns:
        The error text (at most 600 characters), or "".
    """
    out = []
    for line in block:
        if line.startswith("E   "):
            if out:
                break
            out.append(line[4:].strip())
        elif out and line.startswith("      ") and line.strip():
            out.append(line.strip())
        elif out:
            break
    return " ".join(out)[:600]


def log_errors(directory: Path) -> dict[str, str]:
    """The raw error of every failed test in the ``--tb=line`` logs of a report directory.

    FAILURES blocks pair with the FAILED lines of the short summary, ERRORS blocks with the
    ERROR lines, in order; a log where the counts differ is skipped. Later logs (a rerun)
    overwrite earlier ones.

    Args:
        directory: Report directory.

    Returns:
        ``{test: error}``.
    """
    out: dict[str, str] = {}
    logs = sorted((directory / "logs").glob("*.log"), key=lambda p: p.stat().st_mtime)
    for log in logs:
        lines = log.read_text(errors="replace").splitlines()
        sections: dict[str, list[str]] = {}
        current = None
        for line in lines:
            head = re.match(r"^=+ (.+?) =+$", line)
            if head:
                current = {"FAILURES": "FAILED", "ERRORS": "ERROR"}.get(head.group(1))
                if current:
                    sections[current] = []
                continue
            if current:
                sections[current].append(line)
        for kind, section in sections.items():
            ids = [m.group(1) for m in (re.match(rf"^{kind} (\S+)", x) for x in lines) if m]
            blocks = _blocks(section)
            if len(ids) != len(blocks):
                continue
            for test, block in zip(ids, blocks, strict=True):
                error = _first_error(block)
                if error:
                    out[test] = error
    return out


def row_errors(results_dir: Path, reference: str) -> dict[str, str]:
    """The raw errors recorded in a reference's full rows (``<name>.rows.json.gz``).

    Args:
        results_dir: ``results/``.
        reference: Reference name.

    Returns:
        ``{test: error}`` for the rows that have one.
    """
    path = results_dir / f"{reference}.rows.json.gz"
    if not path.exists():
        return {}
    rows = json.loads(gzip.decompress(path.read_bytes()))
    return {r["test"]: r["error"] for r in rows if r.get("error")}


def draft(cid: str, results_dir: Path = RESULTS) -> str:
    """Markdown facts for a bug report of a cause.

    Args:
        cid: Cause id.
        results_dir: ``results/``.

    Returns:
        The draft.
    """
    c = gr.cause(cid)
    refs = references(results_dir)
    hit = affected(refs, cid)
    versions = ", ".join(f"{k} {v}" for k, v in c.seen_at.items())
    out = [
        f"# [{', '.join(c.seen_at)}] TODO title",
        "",
        f"**Component:** {', '.join(c.seen_at)}  ",
        f"**Version:** {versions}  ",
        f"**Lab cause:** `{c.id}`" + (f" ({c.severity})" if c.severity else ""),
        "",
        f"> Facts collected by `scripts/gap_register.py draft {c.id}` from the references in",
        "> `results/` and the logs in `reports/`. Before filing: write the title, a reproduction",
        "> without the lab and the expected behaviour; re-check with the newest release; search",
        "> the tracker for duplicates.",
        "",
        "## Summary",
        "",
        c.summary,
        "",
        "## Where the lab sees it",
        "",
    ]
    if not hit:
        out += ["No reference books a test to this cause (yet).", ""]
    for ref, tests in hit.items():
        out.append(f"- **{ref}**: {len(tests)} test(s)")
        out += [f"  - `{t}`" for t in tests[:SHOWN]]
        if len(tests) > SHOWN:
            out.append(f"  - ... and {len(tests) - SHOWN} more")
    other = elsewhere(refs, hit)
    out += ["", "## The same tests elsewhere", ""]
    if other:
        for ref, counts in other.items():
            out.append(f"- {ref}: " + ", ".join(f"{n} {s}" for s, n in counts.most_common()))
    else:
        out.append("No other reference ran these tests.")
    out += ["", "## Errors as reported", ""]
    shown = 0
    seen: set[str] = set()
    for ref, tests in hit.items():
        errors = log_errors(report_dir(ref)) | row_errors(results_dir, ref)
        for t in tests:
            e = errors.get(t)
            if not e or e in seen or e.startswith("iceberg_lab.lab.gaps."):
                continue
            seen.add(e)
            out += [f"`{t}` ({ref}):", "", "```", e, "```", ""]
            shown += 1
            if shown >= SHOWN:
                break
        if shown >= SHOWN:
            break
    if not shown:
        out += [
            "Neither the references nor the logs in `reports/` hold an error of these tests.",
            "",
        ]
    out += ["## Reproduce in the lab", "", "```"]
    for ref, tests in list(hit.items())[:2]:
        catalog = ref.removesuffix("-nohelp")
        ids = " ".join(f'"{t}"' for t in tests[:2])
        out.append(f"CATALOG={catalog} uv run pytest {ids}")
    out += ["```", "", "## Expected", "", "TODO", ""]
    return "\n".join(out)
