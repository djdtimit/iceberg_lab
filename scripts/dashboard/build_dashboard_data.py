"""Build the dashboard's data files from the latest full runs (run in the dev container).

    testergebnisse.json  every comparison row of all four catalogs, with a "catalog" field
                         (reports/comparison_rows.json = Lakekeeper, reports/<cat>/... = others),
                         plus the runs without lab help as catalog ``<cat>_nohelp`` (rows of
                         the helped engines only; shown next to the catalog, never counted).
                         Every row has its theme (src/iceberg_lab/lab/reporting.py THEMES), the id of
                         the run that produced it and, for a "not testable" case, the engine
                         (or "catalog") whose step failed before it (reporting.blocked_by). Left out, because the page shows them
                         nowhere: lab self-tests and cross-reads of an engine's own result.
    runs.json            one row per recorded run (reports/.../runs.json): catalog, run id,
                         start, end, git commit, versions of engines and images
    inventar.json        one row per capability of tests/lab/feature_inventory.py: title, kind and
                         ✓ / ◐ / – per catalog, plus the note

Usage: uv run python scripts/dashboard/build_dashboard_data.py <out dir>
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOGS = ("lakekeeper", "polaris", "rustfs", "unity")
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT / "tests" / "lab")]


def current_tests() -> set[str]:
    """Node ids the suite has today (per catalog: parametrisation may differ). Reports merge
    results over runs, so a renamed or removed test would otherwise linger in the data.

    Returns:
        The node ids.
    """
    ids: set[str] = set()
    for cat in CATALOGS:
        out = subprocess.run(
            ["uv", "run", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider"],
            check=True,
            cwd=ROOT,
            env={**os.environ, "CATALOG": cat},
            capture_output=True,
            text=True,
        ).stdout
        ids |= {line.strip() for line in out.splitlines() if "::" in line}
    return ids


def report_dir(name: str) -> Path:
    """Report directory of a catalog or of a run without help (``<cat>-nohelp``).

    Args:
        name: Catalog name, or ``<catalog>-nohelp``.

    Returns:
        The directory (``reports/`` for Lakekeeper).
    """
    return ROOT / "reports" / ("" if name == "lakekeeper" else name)


def complete(row: dict, catalog: str) -> dict:
    """A dashboard row: catalog, theme, severity and the cause of a blocked case filled in also
    for reports written before those fields existed.

    Args:
        row: A row of comparison_rows.json.
        catalog: The column it belongs to (``<cat>`` or ``<cat>_nohelp``).

    Returns:
        The row.
    """
    from iceberg_lab.lab.gaps import gap_severity
    from iceberg_lab.lab.reporting import blocked_by, theme_of

    sev = row.get("severity") or (gap_severity(row["detail"]) if row["status"] == "gap" else "")
    return {
        "catalog": catalog,
        **row,
        "theme": row.get("theme") or theme_of(row["area"]),
        "severity": sev or "",
        "run": row.get("run", ""),
        "blocked_by": row.get("blocked_by", blocked_by(row)),
    }


def shown(row: dict) -> bool:
    """Whether the page shows a row anywhere (self-tests and own-result reads it does not).

    Args:
        row: A row of comparison_rows.json.

    Returns:
        True if the row goes into the data file.
    """
    return row["status"] != "self" and row["area"] != "Lab self-test"


def results() -> list[dict]:
    """Comparison rows of all catalogs (plus the columns without help), without tests that no
    longer exist.

    Returns:
        The rows with a ``catalog`` field.
    """
    from iceberg_lab.config import helped_engines

    known = current_tests()
    rows, dropped = [], 0
    for cat in CATALOGS:
        for r in json.loads((report_dir(cat) / "comparison_rows.json").read_text()):
            if r["test"] not in known:
                dropped += 1
            elif shown(r):
                rows.append(complete(r, cat))
    # Columns "without help": the runs without lab help (LAB_NO_HELP=1), only rows of the
    # helped engines (as engine or as writer). Shown, never counted (page: EV.compared).
    for cat in CATALOGS:
        path = report_dir(f"{cat}-nohelp") / "comparison_rows.json"
        helped = helped_engines(cat)
        if not path.exists() or not helped:
            continue
        for r in json.loads(path.read_text()):
            if (
                r["test"] in known
                and shown(r)
                and (r["engine"] in helped or r.get("writer") in helped)
            ):
                rows.append(complete(r, f"{cat}_nohelp"))
    print(f"dropped {dropped} rows of tests that no longer exist")
    return rows


def runs() -> list[dict]:
    """One row per recorded run of every report directory (src/iceberg_lab/lab/manifest.py).

    Returns:
        The rows, newest first per catalog.
    """
    out = []
    for cat in CATALOGS:
        for name, column in ((cat, cat), (f"{cat}-nohelp", f"{cat}_nohelp")):
            path = report_dir(name) / "runs.json"
            if not path.exists():
                continue
            for m in json.loads(path.read_text()).values():
                out.append(
                    {
                        "catalog": column,
                        "run": m["run"],
                        "started": m.get("started", ""),
                        "finished": m.get("finished", ""),
                        "commit": (m.get("git", {}).get("commit") or "")[:12],
                        "dirty": m.get("git", {}).get("dirty", False),
                        "packages": ", ".join(f"{k} {v}" for k, v in m.get("packages", {}).items()),
                        "images": ", ".join(sorted(set(m.get("images_declared", {}).values()))),
                    }
                )
    return sorted(out, key=lambda r: (r["catalog"], r["run"]), reverse=True)


def inventory() -> list[dict]:
    """One row per capability of the feature inventory.

    Returns:
        The rows.
    """
    import feature_inventory as inv

    out = []
    for name, cap in inv.CAPABILITIES.items():
        offered = set(inv.offered_by(cap))
        missing = cap.get("not_implemented", {})
        row = {"capability": name, "title": cap["title"], "kind": inv.kind(name, cap)}
        for c in CATALOGS:
            row[c] = "◐" if c in missing else "✓" if c in offered else "–"
        gaps = [f"{c}: described, not implemented ({m['status']})" for c, m in missing.items()]
        row["note"] = " ".join(x for x in (*gaps, cap.get("note", "")) if x)
        row["tests"] = ", ".join(cap["tests"])
        out.append(row)
    return out


if __name__ == "__main__":
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    res = results()
    (out / "testergebnisse.json").write_text(json.dumps(res, ensure_ascii=False))
    run_rows = runs()
    (out / "runs.json").write_text(json.dumps(run_rows, ensure_ascii=False))
    inv_rows = inventory()
    (out / "inventar.json").write_text(json.dumps(inv_rows, ensure_ascii=False))
    print(
        f"testergebnisse.json: {len(res)} rows; runs.json: {len(run_rows)} runs; "
        f"inventar.json: {len(inv_rows)} capabilities"
    )
