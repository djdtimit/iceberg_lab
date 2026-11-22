"""Build the dashboard's two data files from the latest full runs (run in the dev container).

    testergebnisse.json  every comparison row of all four catalogs, with a "catalog" field
                         (reports/comparison_rows.json = Lakekeeper, reports/<cat>/... = others),
                         plus the runs without lab help as catalog ``<cat>_nohelp`` (rows of
                         the helped engines only; shown next to the catalog, never counted)
    inventar.json        one row per capability of tests/lab/feature_inventory.py: title, kind and
                         ✓ / ◐ / – per catalog, plus the note

Usage: cd /workspaces/iceberg_lab && uv run python <this file> <out dir>
"""

import json
import sys
from pathlib import Path

ROOT = Path("/workspaces/iceberg_lab")
CATALOGS = ("lakekeeper", "polaris", "rustfs", "unity")


def current_tests() -> set[str]:
    """Node ids the suite has today (per catalog: parametrisation may differ). Reports merge
    results over runs, so a renamed or removed test would otherwise linger in the data.

    Returns:
        The node ids.
    """
    import os
    import subprocess

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


def results() -> list[dict]:
    """Comparison rows of all catalogs (plus the help column), without tests that no longer exist.

    Returns:
        The rows with a ``catalog`` field.
    """
    known = current_tests()
    rows, dropped = [], 0
    for cat in CATALOGS:
        path = ROOT / "reports" / ("" if cat == "lakekeeper" else cat) / "comparison_rows.json"
        for r in json.loads(path.read_text()):
            if r["test"] not in known:
                dropped += 1
                continue
            rows.append({"catalog": cat, **r})
    # Columns "without help": the runs without lab help (LAB_NO_HELP=1), only rows of the
    # helped engines (as engine or as writer). Shown, never counted (page: EV.compared).
    sys.path.insert(0, str(ROOT / "src"))
    from iceberg_lab.config import helped_engines

    for cat in CATALOGS:
        behind = ROOT / "reports" / f"{cat}-nohelp" / "comparison_rows.json"
        helped = helped_engines(cat)
        if not behind.exists() or not helped:
            continue
        for r in json.loads(behind.read_text()):
            if r["test"] in known and (r["engine"] in helped or r.get("writer") in helped):
                rows.append({"catalog": f"{cat}_nohelp", **r})
    print(f"dropped {dropped} rows of tests that no longer exist")
    return rows


def inventory() -> list[dict]:
    """One row per capability of the feature inventory.

    Returns:
        The rows.
    """
    sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "tests" / "lab")]
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
    inv_rows = inventory()
    (out / "inventar.json").write_text(json.dumps(inv_rows, ensure_ascii=False))
    print(f"testergebnisse.json: {len(res)} rows; inventar.json: {len(inv_rows)} capabilities")
