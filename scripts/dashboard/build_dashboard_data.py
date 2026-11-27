"""Build the dashboard's data files from the latest full runs (run in the dev container).

    testergebnisse.json  every comparison row of all catalogs, with a "catalog" field
                         (results/<cat>.rows.json.gz, else the report directory),
                         plus the runs without lab help as catalog ``<cat>_nohelp`` (rows of
                         the helped engines only; shown next to the catalog, never counted).
                         Every row has its theme (src/iceberg_lab/lab/reporting.py THEMES), the id of
                         the run that produced it and, for a case that did not run, the engine
                         (or "catalog") whose step failed before it (reporting.blocked_by). Left out, because the page shows them
                         nowhere: lab self-tests and cross-reads of an engine's own result.
                         Encoded per column (:func:`compact`); the page's loader decodes it.
    runs.json            one row per recorded run (reports/.../runs.json): catalog, run id,
                         start, end, git commit, versions of engines and images, and the
                         versions its reruns used where they differ
    inventar.json        one row per capability of tests/lab/feature_inventory.py: title, kind and
                         ✓ / ◐ / – per catalog, plus the note
    verlauf.json         one row per catalog and run (results/history/runs.jsonl): status counts, the
                         number of gap causes, and the causes added and gone since the catalog's
                         previous run

Usage: uv run python scripts/dashboard/build_dashboard_data.py <out dir>
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOGS = (
    "lakekeeper",
    "polaris",
    "rustfs",
    "unity",
    "reference",
    "gravitino",
    "gravitino_lakekeeper",
    "gravitino_polaris",
    "gravitino_unity",
    "gravitino_rustfs",
)
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


def comparison_rows(name: str) -> list[dict] | None:
    """Comparison rows of a catalog or run without help: the exported copy in
    ``results/<name>.rows.json.gz`` (scripts/export_reference.py: gap texts current, rows of
    removed tests left out), else its report directory.

    Args:
        name: Catalog name, or ``<catalog>-nohelp``.

    Returns:
        The rows, or None if neither exists.
    """
    import gzip

    saved = ROOT / "results" / f"{name}.rows.json.gz"
    if saved.exists():
        return json.loads(gzip.decompress(saved.read_bytes()))
    path = report_dir(name) / "comparison_rows.json"
    return json.loads(path.read_text()) if path.exists() else None


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
        for r in comparison_rows(cat) or []:
            if r["test"] not in known:
                dropped += 1
            elif shown(r):
                rows.append(complete(r, cat))
    # Columns "without help": the runs without lab help (LAB_NO_HELP=1), only rows of the
    # helped engines (as engine or as writer). Shown, never counted (page: EV.compared).
    for cat in CATALOGS:
        nohelp = comparison_rows(f"{cat}-nohelp")
        helped = helped_engines(cat)
        if not nohelp or not helped:
            continue
        for r in nohelp:
            if (
                r["test"] in known
                and shown(r)
                and (r["engine"] in helped or r.get("writer") in helped)
            ):
                rows.append(complete(r, f"{cat}_nohelp"))
    print(f"dropped {dropped} rows of tests that no longer exist")
    return rows


def rerun_note(m: dict) -> str:
    """What the reruns merged into a run changed: how many, and the versions that differ from
    the full run's (src/iceberg_lab/lab/manifest.py ``reruns``).

    Args:
        m: A run manifest.

    Returns:
        E.g. ``"3 reruns; polars 2.2.0, pg-lake ...:3.7.1-pg19"``, or "".
    """
    reruns = m.get("reruns") or []
    if not reruns:
        return ""
    other: dict[str, str] = {}
    for r in reruns:
        for field in ("packages", "images_declared"):
            for k, v in r.get(field, {}).items():
                # changed versions only (a service added later ran in no compared row)
                if m.get(field, {}).get(k) not in (None, v):
                    other[k] = v  # the latest rerun's version wins
    count = f"{len(reruns)} rerun{'s' if len(reruns) > 1 else ''}"
    if not other:
        return f"{count}, same versions"
    return f"{count}; some rows with " + ", ".join(f"{k} {v}" for k, v in sorted(other.items()))


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
                        "reruns": rerun_note(m),
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
        for c in inv.CATALOGS:  # catalogs whose API is classified in the inventory
            row[c] = "◐" if c in missing else "✓" if c in offered else "–"
        gaps = [f"{c}: described, not implemented ({m['status']})" for c, m in missing.items()]
        row["note"] = " ".join(x for x in (*gaps, cap.get("note", "")) if x)
        row["tests"] = ", ".join(cap["tests"])
        out.append(row)
    return out


#: Columns of a result row, in the order of the encoded file.
COLUMNS = (
    "catalog", "engine", "theme", "area", "capability", "status", "detail", "severity",
    "writer", "test", "run", "blocked_by",
)  # fmt: skip


def compact(rows: list[dict]) -> str:
    """The rows dictionary-encoded per column, so the file stays far below the artifact's
    upload limit: ``{"columns": [...], "values": {column: [distinct values]}, "rows": [[index
    per column], ...]}``. The page's loader for ``results`` turns it back into row objects.

    Args:
        rows: Dashboard rows (a missing field counts as "").

    Returns:
        The JSON text.
    """
    values: dict[str, list[str]] = {c: [] for c in COLUMNS}
    index: dict[str, dict[str, int]] = {c: {} for c in COLUMNS}
    encoded = []
    for r in rows:
        line = []
        for c in COLUMNS:
            v = r.get(c, "") or ""
            if v not in index[c]:
                index[c][v] = len(values[c])
                values[c].append(v)
            line.append(index[c][v])
        encoded.append(line)
    doc = {"columns": list(COLUMNS), "values": values, "rows": encoded}
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":"))


def history() -> list[dict]:
    """The trend rows: per catalog and run, the counts and the causes that changed.

    Returns:
        Rows ``name, run, finished, works, gaps, new, na, blocked, causes, added, gone``
        (``added``/``gone``: cause ids separated by ", "; empty for a catalog's first run).
    """
    path = ROOT / "results" / "history" / "runs.jsonl"
    if not path.exists():
        return []
    lines = [json.loads(x) for x in path.read_text().splitlines() if x.strip()]
    out, previous = [], {}
    for x in sorted(lines, key=lambda x: (x["name"], x["finished"])):
        c, causes = x["counts"], set(x["causes"])
        before = previous.get(x["name"])
        out.append(
            {
                "name": x["name"].replace("-nohelp", "_nohelp"),
                "run": x["run"],
                "finished": x["finished"],
                "works": c.get("pass", 0) + c.get("fixed", 0),
                "gaps": c.get("gap", 0),
                "new": c.get("new", 0),
                "na": c.get("na", 0),
                "blocked": c.get("blocked", 0),
                "causes": len(causes),
                "added": ", ".join(sorted(causes - before)) if before is not None else "",
                "gone": ", ".join(sorted(before - causes)) if before is not None else "",
            }
        )
        previous[x["name"]] = causes
    return out


if __name__ == "__main__":
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    res = results()
    (out / "testergebnisse.json").write_text(compact(res))
    run_rows = runs()
    (out / "runs.json").write_text(json.dumps(run_rows, ensure_ascii=False))
    inv_rows = inventory()
    (out / "inventar.json").write_text(json.dumps(inv_rows, ensure_ascii=False))
    hist = history()
    (out / "verlauf.json").write_text(json.dumps(hist, ensure_ascii=False))
    print(
        f"testergebnisse.json: {len(res)} rows; runs.json: {len(run_rows)} runs; "
        f"inventar.json: {len(inv_rows)} capabilities; verlauf.json: {len(hist)} runs"
    )
