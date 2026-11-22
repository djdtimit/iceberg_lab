"""Engine comparison report: collects every test result and writes a side-by-side overview.

Registered by ``conftest.py``. After each run it writes (``reports/`` for Lakekeeper,
``reports/<catalog>/`` for the others, ``reports/<catalog>-nohelp/`` for the run without lab
help; ``REPORT_DIR=<dir>`` sends a targeted run elsewhere):

* ``results.json``         - one record per test: file, test, parameters, status, detail
* ``comparison.md``        - per-engine summary, one matrix per test area, severe gaps
* ``comparison_rows.json`` - one row per test for the dashboard (area, capability, severity)

Status per test (combined over setup/call):

    pass     the capability works
    gap      documented gap reproduced ("[known gap] <reason>", or XFAIL in xfail mode)
    new      failure that is not documented - a new error
    fixed    documented gap no longer reproduces ("[gap fixed]") - remove the entry
    na       not offered: the engine/catalog has no API/type for it (skip with that reason)
    blocked  could not be tested: a prerequisite failed (e.g. the producing write)
    self     cross-read of an engine's own result (verified in the operation test itself)

A skip because the producing test did not run in this session is no record at all, so a
targeted run cannot overwrite a full run's result. Gaps whose reason starts with a severity
(``conftest.SEVERITIES``) are listed separately as severe.

Two figures per engine and area, so missing capabilities are visible instead of silently
shrinking the denominator:

* coverage    = works / (works + gap + new + na + blocked) - what works of all cases
* reliability = works / (works + gap + new)                - how well what is offered works
(works = pass + fixed; ``self`` counts nowhere). Areas of one catalog's or one engine's special
features, demo notebooks and lab self-tests are reported but not compared (``NOT_COMPARED``).
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import pytest

_CATALOG = os.getenv("CATALOG", "lakekeeper")
#: reports/ for the reference catalog (Lakekeeper), reports/<catalog>/ for the others.
#: REPORT_DIR=<dir> sends a targeted run's report elsewhere, so it cannot touch the full run's.
REPORT_DIR = (
    Path(os.environ["REPORT_DIR"])
    if os.getenv("REPORT_DIR")
    else Path(__file__).resolve().parents[2]
    / "reports"
    / ("" if _CATALOG == "lakekeeper" else _CATALOG)
)
if os.getenv("LAB_NO_HELP") == "1" and not os.getenv("REPORT_DIR"):
    # Run without help (iceberg_lab.config.WITH_HELP false): its own report, never the
    # comparison - reports/<catalog>-nohelp/ for every catalog (Lakekeeper's own is reports/).
    REPORT_DIR = Path(__file__).resolve().parents[2] / "reports" / f"{_CATALOG}-nohelp"
ENGINE_ORDER = ["pyiceberg", "duckdb", "polars", "daft", "spark", "trino", "pg_lake", "flink"]
SYMBOL = {"pass": "✅", "gap": "❌", "new": "⚠️", "fixed": "🔧", "na": "—", "blocked": "·"}

#: skip reasons meaning "the engine does not offer this" (vs. "could not be tested")
_NA_PATTERNS = (
    "has no api",
    "has no sql type",
    "cannot build",
    "has no column type",
    "creates only",
    "not testable as writer",
    "always uses",
    "has no remote signing",
)

_records: dict[str, dict] = {}


# =========================================================================== collection


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Attach the test parameters to every report (needed to place it in a matrix).

    Args:
        item: The test item.
        call: The call phase info.

    Yields:
        Control to the other hooks (hookwrapper).
    """
    outcome = yield
    report = outcome.get_result()
    callspec = getattr(item, "callspec", None)
    params = {}
    if callspec is not None:
        for k, v in callspec.params.items():
            params[k] = getattr(v, "name", v) if not isinstance(v, (str, int, float)) else v
    report.lab_params = params
    report.lab_specific = item.get_closest_marker("specific") is not None


def pytest_runtest_logreport(report):
    """Fold setup/call/teardown reports into one record per test.

    Args:
        report: The phase report.
    """
    rec = _records.setdefault(
        report.nodeid,
        {
            "nodeid": report.nodeid,
            "file": report.nodeid.split("::")[0].split("/")[-1].removesuffix(".py"),
            "test": report.nodeid.split("::")[-1].split("[")[0],
            "params": getattr(report, "lab_params", {}),
            "specific": getattr(report, "lab_specific", False),
            "status": None,
            "detail": "",
        },
    )
    if report.when == "teardown" and rec["status"] is not None:
        return
    status, detail = _classify(report)
    if status is not None:
        rec["status"], rec["detail"] = status, detail


def _classify(report) -> tuple[str | None, str]:
    """Status and detail of a report (pass, gap, fixed, new, na, blocked, ...).

    Args:
        report: The phase report.

    Returns:
        ``(status, detail)``; status None: no record (producer not run).
    """
    text = report.longreprtext if report.longrepr else ""
    if hasattr(report, "wasxfail"):
        return "gap", report.wasxfail
    if report.passed:
        return ("pass", "") if report.when == "call" else (None, "")
    if report.skipped:
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else text
        reason = reason.removeprefix("Skipped: ")
        low = reason.lower()
        if "[producer not run in this session]" in low:
            return None, ""  # no result: keeps the last real one (see conftest.NOT_RUN)
        if "own result is verified" in low:
            return "self", reason  # not a test case of this engine at all
        return ("na" if any(p in low for p in _NA_PATTERNS) else "blocked"), reason
    gap = _marked(text, "KnownGapError", r"\[known gap\] ")
    if gap:
        return "gap", gap
    fixed = _marked(text, "GapFixedError", r"\[gap fixed\][^:]*: ")
    if fixed:
        return "fixed", fixed
    first = next((ln.strip() for ln in text.splitlines() if ln.strip()), "failed")
    return "new", first[:240]


def _marked(text: str, error: str, marker: str) -> str | None:
    """The reason after ``marker``: from the raised ``error`` line if there is one, else (central
    rules put it in front of the report) the first real one. Tracebacks also show expect_gap's
    source, whose docstring and raise lines contain the marker with placeholders.

    Args:
        text: Long report text.
        error: Exception class name that carries the reason.
        marker: Regex in front of the reason.

    Returns:
        The reason, or None.
    """
    raised = re.findall(rf"{error}: {marker}([^\n]+)", text)
    if raised:
        return raised[-1].strip()
    found = [
        m.strip()
        for m in re.findall(rf"{marker}([^\n]+)", text)
        if not m.startswith("<reason>") and "{reason}" not in m
    ]
    return found[0] if found else None


# =========================================================================== output


def pytest_sessionfinish(session):
    """Write results.json and comparison.md (only if anything ran).

    Args:
        session: The pytest session.
    """
    records = [r for r in _records.values() if r["status"] is not None]
    if not records:
        return
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    # Merge this run's records into the existing results by test id (default), so a targeted
    # re-run updates the report instead of replacing it. REPORT_MERGE=0 starts from scratch;
    # scripts/run_all.sh deletes results.json before a full run.
    if os.getenv("REPORT_MERGE", "1") != "0" and (REPORT_DIR / "results.json").exists():
        old = json.loads((REPORT_DIR / "results.json").read_text())
        mine = {r["nodeid"] for r in records}
        records = [r for r in old if r["nodeid"] not in mine] + records
    (REPORT_DIR / "results.json").write_text(
        json.dumps(records, indent=1, ensure_ascii=False, default=str)
    )
    md = render_markdown(records)
    perf = REPORT_DIR / "performance.md"  # written by test_performance.py, if it ran
    if perf.exists() and any(r["file"] == "test_performance" for r in records):
        md += "\n\n" + perf.read_text().replace("# Leistung", "## Leistung", 1)
    (REPORT_DIR / "comparison.md").write_text(md)
    (REPORT_DIR / "comparison_rows.json").write_text(
        json.dumps(flat_rows(records), indent=0, ensure_ascii=False, default=str)
    )


def _warehouse_role(warehouse) -> str:
    """Neutral name of a warehouse (``signing``/``sts``) - the same on every catalog.

    Args:
        warehouse: Warehouse name.

    Returns:
        ``signing``, ``sts`` or the name.
    """
    from iceberg_lab.config import WAREHOUSE_SIGNING, WAREHOUSE_STS

    return {WAREHOUSE_SIGNING: "signing", WAREHOUSE_STS: "sts"}.get(warehouse, str(warehouse))


def _capability(rec) -> str:
    """Short, human-readable name of what a record tests (row label in the matrices).

    Args:
        rec: Result record.

    Returns:
        The label.
    """
    p = rec["params"]
    case = getattr(p.get("case"), "name", p.get("case"))
    fv = f" v{p['fv']}" if "fv" in p else ""
    t = rec["test"]
    if t == "test_operation" or t == "test_operation_cross_read":
        return f"{p.get('op')}{fv}"
    if rec["file"] == "test_views" and t == "test_read_view":
        return f"reads view of {p.get('creator')}"
    if rec["file"] == "test_robustness" and t == "test_catalog_fault":
        return str(p.get("scenario"))
    if rec["file"] == "test_namespaces":
        return str(p.get("task"))
    if rec["file"] == "test_neutral_source":
        return (
            f"{p.get('op') or p.get('case') or p.get('task') or t.removeprefix('test_read_')}{fv}"
        )
    if rec["file"] == "test_evolution":
        return f"{p.get('case', t.removeprefix('test_'))}{fv}"
    if t in ("test_maintenance", "test_maintenance_cross_read"):
        return f"{p.get('task')}{fv}"
    if rec["file"] == "test_types":
        return f"{case}{fv}"
    if rec["file"] == "test_access_delegation" and t in ("test_read", "test_write"):
        return f"{t.removeprefix('test_')} {_warehouse_role(p.get('warehouse'))}"
    if rec["file"] == "test_interop":
        return f"reads from {p.get('writer')}"
    return t.removeprefix("test_")


def severity(rec) -> str | None:
    """Severity of a reproduced gap (conftest.SEVERITIES): "Wrong result", "Data loss",
    ... - ``None`` for a limitation ("not supported") and for every other status.

    Args:
        rec: Result record.

    Returns:
        The label, or None.
    """
    from conftest import gap_severity

    return gap_severity(rec["detail"]) if rec["status"] == "gap" else None


def flat_rows(records: list[dict]) -> list[dict]:
    """One row per test for the dashboard: engine, area, capability, status, detail.

    Args:
        records: All result records.

    Returns:
        The rows.
    """
    rows = []
    for r in records:
        # Unassigned tests stay visible (and compared) instead of silently dropping out.
        area = next((name for name, pred in DIMENSIONS if pred(r)), "Unassigned")
        p = r["params"]
        rows.append(
            {
                "engine": _engine(r) or "",
                "area": area,
                "capability": _capability(r),
                "status": r["status"],
                "detail": r["detail"],
                "severity": severity(r) or "",
                "writer": p.get("writer")
                or (p.get("engine") if r["test"] in CROSS_READ_TESTS else ""),
                "test": r["nodeid"],
            }
        )
    return rows


CROSS_READ_TESTS = ("test_operation_cross_read", "test_maintenance_cross_read")


def _engine(rec) -> str | None:
    """The engine a record is about (reader for read tests, writer/engine otherwise).

    Args:
        rec: Result record.

    Returns:
        The engine, or None.
    """
    p = rec["params"]
    for key in ("reader", "engine", "writer", "creator"):
        if key in p and rec["test"] not in ("test_write",):
            if rec["test"] in CROSS_READ_TESTS and key == "reader":
                return p["reader"]
            return p[key]
    if "writer" in p:
        return p["writer"]
    f = rec["file"].removeprefix("test_")
    if f in ("spark", "trino", "duckdb", "pyiceberg", "pglake", "flink"):
        return {"pglake": "pg_lake"}.get(f, f)
    if f == "dataframes":
        return "polars" if "polars" in rec["test"] else "daft"
    return None


#: Test files about one engine's own features (docs/test-architecture.md, "Engine-specific").
ENGINE_FILES = (
    "test_spark",
    "test_trino",
    "test_duckdb",
    "test_flink",
    "test_pyiceberg",
    "test_dataframes",
    "test_pglake",
)

DIMENSIONS = [
    (
        "Access modes",
        lambda r: (
            r["file"] == "test_access_delegation" and r["test"] in ("test_read", "test_write")
        ),
    ),
    ("Interop (one writes, all read)", lambda r: r["file"] == "test_interop"),
    ("Write types", lambda r: r["file"] == "test_types" and r["test"] == "test_write"),
    ("Read types", lambda r: r["file"] == "test_types" and r["test"] == "test_read"),
    (
        "Read types (neutral source)",
        lambda r: r["file"] == "test_types" and r["test"] == "test_read_neutral",
    ),
    # Neutral source (tests/engines/test_neutral_source.py): the reference implementation writes, every
    # engine reads - the counterpart of each read matrix.
    (
        "Read results (neutral source)",
        lambda r: r["file"] == "test_neutral_source" and r["test"] == "test_read_operation",
    ),
    (
        "Read partitions (neutral source)",
        lambda r: r["file"] == "test_neutral_source" and r["test"] == "test_read_partitioned",
    ),
    (
        "Read evolution (neutral source)",
        lambda r: (
            r["file"] == "test_neutral_source"
            and r["test"] in ("test_read_schema_evolved", "test_read_partition_evolved")
        ),
    ),
    (
        "Read maintenance results (neutral source)",
        lambda r: r["file"] == "test_neutral_source" and r["test"] == "test_read_maintained",
    ),
    (
        "Write file formats",
        lambda r: r["file"] == "test_file_formats" and r["test"] == "test_write_format",
    ),
    (
        "Read file formats",
        lambda r: r["file"] == "test_file_formats" and r["test"] == "test_read_format",
    ),
    ("Type fidelity", lambda r: r["file"] == "test_types" and r["test"] == "test_reader_type"),
    ("Operations", lambda r: r["file"] == "test_operations" and r["test"] == "test_operation"),
    ("Read others' results", lambda r: r["test"] == "test_operation_cross_read"),
    ("v3 features", lambda r: r["file"] == "test_v3_features"),
    (
        "Write large files",
        lambda r: r["file"] == "test_rustfs" and r["test"] == "test_big_write",
    ),
    ("Read large files", lambda r: r["file"] == "test_rustfs" and r["test"] == "test_big_read"),
    ("Write partitions", lambda r: r["test"] == "test_partitioned_write"),
    ("Read partitions", lambda r: r["test"] == "test_partitioned_read"),
    ("Schema evolution", lambda r: r["test"].startswith("test_schema_evolution")),
    ("Partition evolution", lambda r: r["test"].startswith("test_partition_evolution")),
    ("Concurrent commits", lambda r: r["file"] == "test_concurrency"),
    ("Views", lambda r: r["file"] == "test_views"),
    ("Namespaces", lambda r: r["file"] == "test_namespaces"),
    ("Statistics", lambda r: r["file"] == "test_statistics"),
    ("Robustness (catalog faults)", lambda r: r["file"] == "test_robustness"),
    ("Authorization", lambda r: r["file"] == "test_authz"),
    ("Roles and inherited grants", lambda r: r["file"] == "test_rbac"),
    ("Single privileges (inventory and effect)", lambda r: r["file"] == "test_privileges"),
    ("Web UI", lambda r: r["file"] == "test_ui"),
    ("Transactions (several tables)", lambda r: r["file"] == "test_transactions"),
    ("Write-Audit-Publish", lambda r: r["file"] == "test_wap"),
    ("Incremental & streaming", lambda r: r["file"] == "test_incremental"),
    ("STS expiry", lambda r: r["file"] == "test_sts_expiry"),
    ("File layout", lambda r: r["file"] == "test_layout"),
    ("Performance (correctness only)", lambda r: r["file"] == "test_performance"),
    ("STS reach", lambda r: r["file"] == "test_sts_scope"),
    # Features of ONE catalog (marker "specific"; Lakekeeper demo notebooks): not compared.
    (
        "Catalog-specific",
        lambda r: r.get("specific") and r["file"] == "test_catalog_features",
    ),
    # The example notebooks run on every catalog, but they are demos, not capability tests.
    ("Demo notebooks", lambda r: r["file"] == "test_notebooks"),
    ("Catalog functions", lambda r: r["file"] == "test_catalog_features"),
    ("Security", lambda r: r["file"] == "test_security"),
    ("Catalog consistency", lambda r: r["file"] == "test_catalog_consistency"),
    ("REST conformance", lambda r: r["file"] == "test_rest_conformance"),
    ("Branch isolation", lambda r: r["file"] == "test_branch_isolation"),
    ("Re-create with an open session", lambda r: r["file"] == "test_recreate_session"),
    ("Files per specification", lambda r: r["file"] == "test_file_spec"),
    ("Storage (RustFS S3 API)", lambda r: r["file"] == "test_storage_s3"),
    ("Token endpoint outage", lambda r: r["file"] == "test_idp_outage"),
    ("Connection without help", lambda r: r["file"] == "test_stock_connection"),
    ("Storage faults (catalog)", lambda r: r["file"] == "test_storage_faults"),
    ("Catalog administration", lambda r: r["file"] == "test_catalog_admin"),
    ("Credentials: reach and signing", lambda r: r["file"] == "test_access_delegation"),
    ("Scan planning (server)", lambda r: r["file"] == "test_scan_planning"),
    ("Object storage (S3 behaviour)", lambda r: r["file"] == "test_rustfs"),
    ("Type check by the catalog", lambda r: r["file"] == "test_types"),
    # ---- reported, NOT compared (NOT_COMPARED below)
    ("Engine-specific", lambda r: r["file"] in ENGINE_FILES),
    (
        "Lab self-test",
        lambda r: (
            r["file"]
            in (
                "test_feature_inventory",
                "test_engine_inventory",
                "test_lab_selftest",
                "test_lab_adapters",
                "test_na_claims",
            )
            or r["test"].endswith(
                (
                    "_define_only_known_operations",
                    "_define_only_known_tasks",
                    "_cover_the_matrices",
                )
            )
        ),
    ),
    ("Maintenance", lambda r: r["test"] == "test_maintenance"),
    ("Read others' maintenance results", lambda r: r["test"] == "test_maintenance_cross_read"),
]


#: Areas that are reported but not part of the comparison (features of one catalog/engine).
NOT_COMPARED = ("Catalog-specific", "Engine-specific", "Lab self-test", "Demo notebooks")


def _counts(statuses: list[str]) -> dict:
    """Counts per status and the derived totals (works, offered, all).

    Args:
        statuses: Statuses of the cases.

    Returns:
        The counts.
    """
    c = Counter(statuses)
    works = c["pass"] + c["fixed"]
    offered = works + c["gap"] + c["new"]
    return {
        "works": works,
        "gap": c["gap"],
        "new": c["new"],
        "na": c["na"],
        "blocked": c["blocked"],
        "offered": offered,
        # Coverage counts "not testable" as not working: a case that never ran is no success.
        "all": offered + c["na"] + c["blocked"],
    }


def _coverage(statuses: list[str]) -> str:
    """Coverage cell: works / all cases (new errors marked).

    Args:
        statuses: Statuses of the cases.

    Returns:
        The cell text.
    """
    k = _counts(statuses)
    if not k["all"]:
        return "·"
    s = f"{k['works']}/{k['all']}"
    return s + (f" ⚠️{k['new']}" if k["new"] else "")


def _reliability(statuses: list[str]) -> str:
    """Reliability cell: works / offered cases (percent).

    Args:
        statuses: Statuses of the cases.

    Returns:
        The cell text.
    """
    k = _counts(statuses)
    return f"{k['works']}/{k['offered']} ({k['works'] / k['offered']:.0%})" if k["offered"] else "—"


def _cell(statuses: list[str]) -> str:
    """One symbol for several results (e.g. one reader over all writers).

    Args:
        statuses: Statuses of the cases.

    Returns:
        The symbol.
    """
    c = Counter(statuses)
    applicable = c["pass"] + c["gap"] + c["new"] + c["fixed"]
    if not applicable:
        return SYMBOL["na"] if c["na"] else SYMBOL["blocked"]
    if c["new"]:
        return SYMBOL["new"]
    if c["gap"] and (c["pass"] or c["fixed"]):
        return f"◐ {c['pass'] + c['fixed']}/{applicable}"
    if c["gap"]:
        return SYMBOL["gap"]
    return SYMBOL["fixed"] if c["fixed"] and not c["pass"] else SYMBOL["pass"]


def _table(rows: list[str], cols: list[str], cells: dict[tuple[str, str], list[str]]) -> str:
    """Markdown table of the cells.

    Args:
        rows: Row labels.
        cols: Column labels.
        cells: (row, col) -> statuses.

    Returns:
        The table.
    """
    out = ["| | " + " | ".join(cols) + " |", "|---|" + "---|" * len(cols)]
    for r in rows:
        out.append(f"| {r} | " + " | ".join(_cell(cells.get((r, c), [])) for c in cols) + " |")
    return "\n".join(out)


def render_markdown(records: list[dict]) -> str:
    """The comparison report (overview, matrices, severe gaps, gap lists, news).

    Args:
        records: All result records.

    Returns:
        The Markdown text.
    """
    engines = [e for e in ENGINE_ORDER if any(_engine(r) == e for r in records)]
    lines = [
        "# Engine comparison",
        "",
        "Generated by `tests/lab/reporting.py` during the last test run.",
        "",
        (
            "Legend: ✅ works · ❌ known gap · ◐ partly (works/applicable) · "
            "⚠️ new failure · 🔧 gap fixed · — not applicable (no API/no type) · "
            "· not tested (engine not running / prerequisite failed)"
        ),
        "",
        "## Overview",
        "",
        (
            "Every engine runs the same test cases in every area. **Coverage** = works / "
            "all cases (capabilities that are not offered count as cases too). **Reliability** = "
            "works / offered cases. Cases that are not testable (prerequisite failed) count as not "
            "working for coverage. Reading back an engine's own results counts nowhere, nor do "
            "tests of special features of one engine or catalog (engine-/catalog-specific) - only "
            "the base tests, which are the same for all (docs/test-architecture.md)."
        ),
        "",
    ]
    hdr = "| Area | " + " | ".join(engines) + " |"
    sep = "|---|" + "---|" * len(engines)
    per_area = {}
    totals = defaultdict(list)
    for name, pred in DIMENSIONS:
        if name in NOT_COMPARED:
            continue
        by_engine = defaultdict(list)
        for r in records:
            if pred(r) and _engine(r) in engines and r["status"] != "self":
                by_engine[_engine(r)].append(r["status"])
                totals[_engine(r)].append(r["status"])
        per_area[name] = by_engine
    lines += ["### Coverage (works / all cases)", "", hdr, sep]
    for name, by_engine in per_area.items():
        lines.append(f"| {name} | " + " | ".join(_coverage(by_engine[e]) for e in engines) + " |")
    lines.append(
        "| **total** | " + " | ".join(f"**{_coverage(totals[e])}**" for e in engines) + " |"
    )
    lines += ["", "### Reliability (works / offered cases)", "", hdr, sep]
    for name, by_engine in per_area.items():
        lines.append(
            f"| {name} | " + " | ".join(_reliability(by_engine[e]) for e in engines) + " |"
        )
    lines.append(
        "| **total** | " + " | ".join(f"**{_reliability(totals[e])}**" for e in engines) + " |"
    )
    lines += ["", "### Breakdown, total", "", "| | " + " | ".join(engines) + " |", sep]
    for key, label in (
        ("works", "works"),
        ("gap", "documented gap"),
        ("severe", "of which severe (result, data, security)"),
        ("new", "new failure"),
        ("na", "not offered"),
        ("blocked", "not testable"),
    ):
        if key == "severe":
            n = {e: sum(1 for r in records if _engine(r) == e and severity(r)) for e in engines}
            lines.append(f"| {label} | " + " | ".join(str(n[e]) for e in engines) + " |")
            continue
        lines.append(
            f"| {label} | " + " | ".join(str(_counts(totals[e])[key]) for e in engines) + " |"
        )
    lines += [
        "",
        (
            "Every engine writes the starting data of its tests itself; if it cannot (e.g. v3 "
            "with PyIceberg/Polars/Daft), the test counts as failed - no substitute engine."
        ),
        "",
    ]

    # ---- detail matrices
    def section(title, pred, row_of, col_of=_engine, rows_sort=None):
        """Append a matrix section (rows x engine columns) for the matching records.

        Args:
            title: Section title.
            pred: Record filter.
            row_of: Record -> row label.
            col_of: Record -> column label.
            rows_sort: Sort key of the rows (None: alphabetical).
        """
        cells, rows = defaultdict(list), []
        for r in records:
            if not pred(r):
                continue
            row, col = row_of(r), col_of(r)
            if row is None or col is None:
                continue
            cells[(row, col)].append(r["status"])
            if row not in rows:
                rows.append(row)
        if not rows:
            return
        if rows_sort:
            rows.sort(key=rows_sort)
        cols = [e for e in engines if any(k[1] == e for k in cells)]
        lines.extend([f"## {title}", "", _table(rows, cols, cells), ""])

    p = lambda r, k: r["params"].get(k)
    section(
        "Operations (engine executes)",
        lambda r: r["test"] == "test_operation",
        lambda r: f"{p(r, 'op')} v{p(r, 'fv')}",
    )
    section(
        "Reading other engines' results (column = reader, over all writing engines)",
        lambda r: r["test"] == "test_operation_cross_read",
        lambda r: f"{p(r, 'op')} v{p(r, 'fv')}",
        col_of=lambda r: p(r, "reader"),
    )
    section(
        "Files an engine writes are readable by others (column = writer)",
        lambda r: r["test"] == "test_operation_cross_read",
        lambda r: f"{p(r, 'op')} v{p(r, 'fv')}",
        col_of=lambda r: p(r, "engine"),
    )
    section(
        "Write types",
        lambda r: r["file"] == "test_types" and r["test"] == "test_write",
        lambda r: f"{getattr(p(r, 'case'), 'name', p(r, 'case'))} v{p(r, 'fv')}",
        col_of=lambda r: p(r, "writer"),
    )
    section(
        "Read types (over all writing engines)",
        lambda r: r["file"] == "test_types" and r["test"] == "test_read",
        lambda r: f"{getattr(p(r, 'case'), 'name', p(r, 'case'))} v{p(r, 'fv')}",
        col_of=lambda r: p(r, "reader"),
    )
    section(
        "Type fidelity (reported type matches the Iceberg type)",
        lambda r: r["test"] == "test_reader_type",
        lambda r: f"{getattr(p(r, 'case'), 'name', p(r, 'case'))} v{p(r, 'fv')}",
        col_of=lambda r: p(r, "reader"),
    )
    section(
        "Access modes (catalog credentials only)",
        lambda r: (
            r["file"] == "test_access_delegation" and r["test"] in ("test_read", "test_write")
        ),
        lambda r: f"{r['test'].removeprefix('test_')} {p(r, 'warehouse')}",
    )
    section(
        "Interop (row = writer, column = reader)",
        lambda r: r["file"] == "test_interop",
        lambda r: p(r, "writer"),
        col_of=lambda r: p(r, "reader"),
        rows_sort=lambda x: ENGINE_ORDER.index(x) if x in ENGINE_ORDER else 99,
    )
    section(
        "v3 features",
        lambda r: r["file"] == "test_v3_features",
        lambda r: (
            r["test"].removeprefix("test_")
            + (f" (written by {p(r, 'writer')})" if p(r, "writer") and p(r, "reader") else "")
        ),
        col_of=lambda r: p(r, "reader") or p(r, "writer") or None,
    )

    # ---- severe gaps first: defects, not missing features
    severe = defaultdict(lambda: defaultdict(list))
    for r in records:
        if severity(r):
            severe[severity(r)][r["detail"]].append(_engine(r) or "-")
    if severe:
        lines += [
            "## Severe gaps (wrong result, data loss, security)",
            "",
            (
                "Documented gaps that are no missing feature but change data or results or "
                "open access wider than intended (classified in the gap reason, "
                "`conftest.SEVERITIES`)."
            ),
            "",
        ]
        for label in sorted(severe):
            lines.append(f"**{label}**")
            for reason, engines_hit in sorted(severe[label].items()):
                hit = ", ".join(sorted(set(engines_hit)))
                lines.append(f"- {reason} ({len(engines_hit)}×; {hit})")
            lines.append("")

    # ---- gap list per engine
    lines += ["## Known gaps per engine", ""]
    by_engine = defaultdict(Counter)
    for r in records:
        if r["status"] == "gap" and _engine(r):
            by_engine[_engine(r)][r["detail"]] += 1
    for e in engines:
        if by_engine[e]:
            lines.append(f"**{e}**")
            lines += [f"- {reason} ({n}×)" for reason, n in by_engine[e].most_common()]
            lines.append("")
    news = [r for r in records if r["status"] in ("new", "fixed")]
    if news:
        lines += ["## New failures and fixed gaps", ""]
        lines += [f"- {SYMBOL[r['status']]} `{r['nodeid']}`: {r['detail']}" for r in news]
        lines.append("")
    return "\n".join(lines)
