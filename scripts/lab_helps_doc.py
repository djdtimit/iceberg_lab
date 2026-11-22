"""Write docs/lab-helps.md: the same tests without and with lab help, side by side.

For every catalog with a lab help (iceberg_lab.config.HELPS) it compares the run without help
(reports/<catalog>-nohelp/, LAB_NO_HELP=1, only the tests of the helped engines) with the
normal run, which uses the helps and is the comparison (reports/<catalog>/, Lakekeeper:
reports/): per area and engine "works / all" without and with help, what became of the tests
the connection gap hides, and the known gaps and NEW failures behind it.

Usage: uv run python scripts/lab_helps_doc.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from iceberg_lab.config import HELPS, helped_engines

CATALOGS = ("lakekeeper", "polaris", "rustfs", "unity")
TITLE = {"lakekeeper": "Lakekeeper", "polaris": "Polaris", "rustfs": "RustFS", "unity": "Unity"}
ENGINE_ORDER = ["pyiceberg", "duckdb", "polars", "daft", "spark", "trino", "pg_lake", "flink"]
LABEL = {
    "pass": "works",
    "gap": "known gap",
    "new": "NEW FAILURE",
    "fixed": "gap fixed",
    "na": "not offered",
    "blocked": "not testable",
}
#: Areas that are reported but not compared (src/iceberg_lab/lab/reporting.py).
NOT_COMPARED = {"Catalog-specific", "Engine-specific", "Lab self-test", "Demo notebooks"}


def report_dir(catalog: str, helped: bool) -> Path:
    """Report directory of a run.

    Args:
        catalog: Catalog profile name.
        helped: The normal run (with help) instead of the run without help.

    Returns:
        The directory.
    """
    if not helped:
        return ROOT / "reports" / f"{catalog}-nohelp"
    return ROOT / "reports" / ("" if catalog == "lakekeeper" else catalog)


def load(catalog: str, helped: bool) -> tuple[dict, dict]:
    """Results and comparison rows of a run, keyed by node id.

    Args:
        catalog: Catalog profile name.
        helped: The run with help.

    Returns:
        (results by node id, comparison rows by node id)
    """
    d = report_dir(catalog, helped)
    results = {r["nodeid"]: r for r in json.loads((d / "results.json").read_text())}
    rows = {r["test"]: r for r in json.loads((d / "comparison_rows.json").read_text())}
    return results, rows


def gap_of_help(detail: str, catalog: str) -> bool:
    """Whether a result is the connection gap a help steps over (or a test it left untestable).

    Args:
        detail: Result detail (gap reason).
        catalog: Catalog profile name.

    Returns:
        True for a help's own gap.
    """
    keys = {
        "sigv4-content-hash": "x-amz-content-sha256",
        "duckdb-aws-host": "derives the SigV4 service",
    }
    return any(keys[h.name] in detail for h in HELPS if catalog in h.catalogs)


def cell(rows: list[dict]) -> str:
    """``works/all`` of comparison rows (not offered and not testable count as cases).

    Args:
        rows: Comparison rows.

    Returns:
        The cell text.
    """
    if not rows:
        return "–"
    works = sum(r["status"] in ("pass", "fixed") for r in rows)
    return f"{works}/{len(rows)}"


def section(catalog: str) -> list[str]:
    """The markdown section of one catalog.

    Args:
        catalog: Catalog profile name.

    Returns:
        The lines.
    """
    helped = helped_engines(catalog)
    normal_res, normal_rows = load(catalog, False)
    help_res, help_rows = load(catalog, True)
    # Only the tests of the run without help (it selects the helped engines' tests).
    help_res = {n: r for n, r in help_res.items() if n in normal_res}
    help_rows = {n: r for n, r in help_rows.items() if n in normal_rows}
    engines = [e for e in ENGINE_ORDER if e in helped]
    both = [n for n in help_rows if n in normal_rows]
    lines = [f"## {TITLE[catalog]}", ""]
    for h in HELPS:
        if catalog in h.catalogs:
            lines.append(f"- **{', '.join(h.engines)}**: {h.gap} - help: {h.workaround}.")
    lines += [
        "",
        (
            f"{len(both)} tests in both runs. Works / all cases per area (an engine counts "
            "where it writes or reads; not offered and not testable cases count):"
        ),
        "",
        "| Area | " + " | ".join(f"{e} without help | {e} with help" for e in engines) + " |",
        "|---|" + "---|---|" * len(engines),
    ]
    areas: dict[str, dict[str, tuple[list, list]]] = defaultdict(
        lambda: defaultdict(lambda: ([], []))
    )
    for n in both:
        a, b = normal_rows[n], help_rows[n]
        if a["area"] in NOT_COMPARED or a["status"] == "self":
            continue
        for e in engines:
            if e in (a["engine"], a.get("writer")):
                areas[a["area"]][e][0].append(a)
                areas[a["area"]][e][1].append(b)
    for area in sorted(areas):
        cells = [x for e in engines for x in (cell(areas[area][e][0]), cell(areas[area][e][1]))]
        lines.append(f"| {area} | " + " | ".join(cells) + " |")
    # What became of the tests the gap hid (gap of a help, or not testable because of it).
    hidden = [
        n
        for n in help_res
        if n in normal_res
        and (
            normal_res[n]["status"] == "blocked"
            or (normal_res[n]["status"] == "gap" and gap_of_help(normal_res[n]["detail"], catalog))
        )
    ]
    totals = Counter(help_res[n]["status"] for n in hidden)
    lines += [
        "",
        (
            f"Hidden without help (connection gap, or not testable): **{len(hidden)}** tests. "
            "With help:"
        ),
        "",
        "| Result with help | Tests |",
        "|---|---|",
        *[f"| {LABEL.get(k, k)} | {v} |" for k, v in totals.most_common()],
        "",
    ]
    by_reason = defaultdict(list)
    for n in hidden:
        b = help_res[n]
        if b["status"] in ("new", "gap") and not gap_of_help(b["detail"], catalog):
            by_reason[(b["status"], b["detail"][:300])].append(n)
    for status, title in (
        ("new", "New failures behind the gap (hidden so far)"),
        ("gap", "Further known gaps behind it"),
    ):
        items = [(k[1], v) for k, v in by_reason.items() if k[0] == status]
        if not items:
            continue
        lines += [f"### {title}", ""]
        for detail, nodes in sorted(items, key=lambda x: -len(x[1])):
            lines.append(f"- {detail} ({len(nodes)}×, e.g. `{nodes[0]}`)")
        lines.append("")
    return lines


def main() -> None:
    """Write docs/lab-helps.md for every catalog with a help and both reports."""
    lines = [
        "# Lab helps: without and with",
        "",
        (
            "Generated by `scripts/lab_helps_doc.py`. Some engines cannot connect to a catalog "
            "with their stock setup, but a sensible workaround gets them there (list: "
            "`iceberg_lab.config.HELPS`). The lab applies it in every run, and a test that "
            "works with it counts as working - that is the comparison. The gap itself is "
            "shown once per engine by `tests/security/test_stock_connection.py` (area "
            '"Connection without help"). The run without help (`LAB_NO_HELP=1`) runs the '
            "tests of these engines once more without the workaround; it is shown here and "
            "in the dashboard next to the comparison, never counted."
        ),
        "",
    ]
    for catalog in CATALOGS:
        if not helped_engines(catalog):
            continue
        if not (report_dir(catalog, False) / "results.json").exists():
            lines += [f"## {TITLE[catalog]}", "", "No run without help yet.", ""]
            continue
        lines += section(catalog)
    out = ROOT / "docs" / "lab-helps.md"
    out.write_text("\n".join(lines))
    print(f"{out} written")


if __name__ == "__main__":
    main()
