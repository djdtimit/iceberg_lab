"""Test results: collects every test result for the references and the dashboard.

Registered by ``tests/conftest.py``. After each run it writes to ``REPORT_DIR`` (below: the
run scripts write the references' directories, a plain pytest ``reports/adhoc/<catalog>/``):

* ``results.json``         - one record per test: file, test, parameters, status, detail, run
* ``comparison_rows.json`` - one row per test for the dashboard (theme, area, capability,
  severity, run)
* ``manifest.json``, ``runs.json`` - what each run was produced with (``src/iceberg_lab/lab/manifest.py``):
  results are merged over runs, every record names its run

Areas are grouped into a few themes (``THEMES``). Totals over all cases are dominated by the
large matrices (the type matrix alone is about a third of all cases), so the dashboard also gives
reliability per theme and the mean over the themes, each theme counting equally.

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

import contextlib
import fcntl
import json
import os
import re
from pathlib import Path

import pytest

from iceberg_lab.lab import manifest

_CATALOG = os.getenv("CATALOG", "lakekeeper")
_REPORTS = Path(__file__).resolve().parents[3] / "reports"
#: reports/ for Lakekeeper, reports/<catalog>/ for the others, reports/<catalog>-nohelp/ for a
#: run without help (iceberg_lab.config.WITH_HELP false; never the comparison). These are the
#: directories scripts/export_reference.py turns into the references, so only the run scripts
#: write there: they set LAB_RUN_ID (run_all.sh, rerun.sh with the full run's id). A plain
#: pytest goes to reports/adhoc/<catalog>/ - before, it merged into the reference unnoticed.
#: REPORT_DIR=<dir> sends a run anywhere.
_NAME = f"{_CATALOG}-nohelp" if os.getenv("LAB_NO_HELP") == "1" else _CATALOG
if os.getenv("REPORT_DIR"):
    REPORT_DIR = Path(os.environ["REPORT_DIR"])
elif not os.getenv("LAB_RUN_ID"):
    REPORT_DIR = _REPORTS / "adhoc" / _NAME
else:
    REPORT_DIR = _REPORTS / ("" if _NAME == "lakekeeper" else _NAME)
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
#: What this session collected (deselected items too) and the paths it collected in full, so a
#: merged rerun drops the old records of parameters that no longer exist (see _write_reports).
_collected: set[str] = set()
_whole_scopes: list[str] = []
_collect_failed = False


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
    excinfo = getattr(call, "excinfo", None)
    skipped = excinfo is not None and excinfo.errisinstance(pytest.skip.Exception)
    report.lab_error = original_error(excinfo.value) if excinfo and not skipped else ""


#: Exceptions the lab raises around the real error (a known gap, a fixed gap).
_LAB_ERRORS = ("KnownGapError", "GapFixedError")


def original_error(exc: BaseException | None) -> str:
    """The error as the engine or catalog raised it, before the lab turned it into a gap.

    A known (or fixed) gap is raised *from* the real error; a central rule leaves the error
    itself in place. The name is module-qualified like pytest prints it
    (``requests.exceptions.HTTPError: 404 ...``), whitespace collapsed, at most 500 characters.

    Args:
        exc: The raised exception.

    Returns:
        ``<type>: <message>``, or "" when the lab raised the gap without an underlying error.
    """
    seen: set[int] = set()
    while exc is not None and type(exc).__name__ in _LAB_ERRORS and id(exc) not in seen:
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    if exc is None or type(exc).__name__ in _LAB_ERRORS:
        return ""
    t = type(exc)
    name = t.__qualname__ if t.__module__ == "builtins" else f"{t.__module__}.{t.__qualname__}"
    message = " ".join(str(exc).split())
    return f"{name}: {message}"[:500] if message else name


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
            "run": manifest.RUN_ID,
        },
    )
    if report.when == "teardown" and rec["status"] is not None:
        return
    status, detail = _classify(report)
    p = rec["params"]
    if status == "na" and rec["test"] in CROSS_READ_TESTS and p.get("reader") == p.get("engine"):
        # An engine reading its own result: verified in the producing test. Without this, an
        # engine the catalog cannot serve (skip mark, before the body's own "self" skip) would
        # add these cases on that catalog only.
        status, detail = "self", "own result is verified in the producing test"
    if status is not None:
        rec["status"], rec["detail"] = status, detail
        # The register id of the gap (the reported text is the register's current reason):
        # lets the export reword the text without a rerun.
        from iceberg_lab.lab.gap_register import cause_of_reason

        rec["cause"] = (cause_of_reason(detail) or "") if status in ("gap", "fixed") else ""
        # The raw error (bug reports, scripts/gap_register.py draft): only where a test failed.
        failed = status in ("gap", "fixed", "new")
        rec["error"] = getattr(report, "lab_error", "") if failed else ""


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
        if report.when != "call":
            return None, ""
        # An intermittent gap that did not occur this time is still a gap, not a pass: one
        # lucky run says nothing. Whether it occurred is kept in flaky_history.json; after
        # gaps.FLAKY_FIXED_AFTER runs without it the summary lists it as "probably fixed".
        from iceberg_lab.lab import gaps

        flaky = gaps.flaky_not_seen(report.nodeid)
        return ("gap", flaky) if flaky else ("pass", "")
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
    """The reason after ``marker``: from the raised ``error`` line if there is one, else from the
    report's first line (a central rule puts ``[known gap] <reason>`` in front of the report -
    conftest). Elsewhere the marker is only quoted (expect_gap's source with placeholders, an
    assertion diff of a self-test comparing report texts) and books nothing.

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
    front = re.match(rf"{marker}([^\n]+)", text.lstrip())
    return front.group(1).strip() if front else None


# =========================================================================== output


@contextlib.contextmanager
def report_lock():
    """Hold the report directory's lock while reading and rewriting its files.

    Two pytest processes write into one report at the same time when ``scripts/run_all.sh``
    runs a long waiting file (``test_sts_expiry.py``) beside the others; each merges its records
    into ``results.json`` (read, merge, write), and without the lock one would drop the other's.

    Yields:
        Nothing; the caller's reads and writes run in between.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_DIR / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def pytest_collectreport(report):
    """Note a collection error: then nothing is known to be gone.

    Args:
        report: The collect report.
    """
    global _collect_failed
    if report.failed:
        _collect_failed = True


def pytest_deselected(items):
    """Deselected items (``-k``, ``-m``, ``--deselect``) still exist.

    Args:
        items: The deselected items.
    """
    _collected.update(i.nodeid for i in items)


def pytest_collection_finish(session):
    """Record the collected test ids and the paths given in full (files, directories).

    Args:
        session: The pytest session.
    """
    _collected.update(i.nodeid for i in session.items)
    root = Path(session.config.rootpath)
    here = Path(session.config.invocation_params.dir)
    for arg in session.config.args:
        if "::" in arg:
            continue  # single tests: their siblings were not collected
        path = (here / arg).resolve()
        try:
            _whole_scopes.append(path.relative_to(root).as_posix())
        except ValueError:
            continue


def _gone(nodeid: str) -> bool:
    """Whether an old record's test lies in a path collected in full but was not collected.

    A rerun of a whole file whose parameters changed would otherwise keep the old ids in
    the report (and in the reference) until the next full run.

    Args:
        nodeid: Test id of the old record.

    Returns:
        True if the test no longer exists.
    """
    if _collect_failed or nodeid in _collected:
        return False
    return any(nodeid.startswith((f"{s}::", f"{s}/")) or s == "." for s in _whole_scopes)


def pytest_sessionfinish(session):
    """Write results.json and comparison_rows.json (only if anything ran).

    Args:
        session: The pytest session.
    """
    from iceberg_lab.lab import rest_observed

    records = [r for r in _records.values() if r["status"] is not None]
    if not records and not rest_observed.OBSERVED:
        return
    with report_lock():
        rest_observed.write(REPORT_DIR)
        if records:
            _write_reports(records)


def _write_reports(records: list[dict]) -> None:
    """Merge ``records`` into the report directory's results and rewrite the derived files.

    Args:
        records: This session's records.
    """
    # Merge this run's records into the existing results by test id (default), so a targeted
    # re-run updates the report instead of replacing it. REPORT_MERGE=0 starts from scratch;
    # scripts/run_all.sh deletes results.json before a full run.
    if os.getenv("REPORT_MERGE", "1") != "0" and (REPORT_DIR / "results.json").exists():
        old = json.loads((REPORT_DIR / "results.json").read_text())
        mine = {r["nodeid"] for r in records}
        records = [r for r in old if r["nodeid"] not in mine and not _gone(r["nodeid"])] + records
    (REPORT_DIR / "results.json").write_text(
        json.dumps(records, indent=1, ensure_ascii=False, default=str)
    )
    (REPORT_DIR / "comparison_rows.json").write_text(
        json.dumps(flat_rows(records), indent=0, ensure_ascii=False, default=str)
    )
    manifest.write(REPORT_DIR, _CATALOG, with_help=os.getenv("LAB_NO_HELP") != "1")


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
        if t == "test_read_tz":
            return f"{p.get('kind')} {p.get('zone')}"
        state = p.get("op") or p.get("case") or p.get("task") or p.get("fmt")
        return f"{state or t.removeprefix('test_read_')}{fv}"
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
    from iceberg_lab.lab.gaps import gap_severity

    return gap_severity(rec["detail"]) if rec["status"] == "gap" else None


#: "Not testable" reasons that name no engine: the catalog (or the lab's storage setup) makes
#: the state impossible, e.g. a type the catalog rejects.
_CATALOG_BLOCKED = ("neither an engine nor the neutral source", "not testable on ")


def blocked_by(row: dict) -> str:
    """Who a "not testable" case is blocked by: the engine whose step failed before it, or
    ``"catalog"``. Empty for every other status.

    A blocked read is no statement about the reader, so it must not count against it: the
    report charges it to the cause instead. The skip reasons name the cause first ("not
    written by X", "X v3 did not succeed in ...", "X did not create a view", ...); without such
    a name the writer of the row decides, else the engine itself.

    Args:
        row: A row of :func:`flat_rows` (``status``, ``detail``, ``engine``, ``writer``).

    Returns:
        The engine name, ``"catalog"`` or ``""``.
    """
    if row["status"] != "blocked":
        return ""
    detail = row.get("detail") or ""
    if m := re.match(r"not written by ([\w-]+)", detail):
        return m.group(1)
    first = detail.split(" ", 1)[0]
    if first in ENGINE_ORDER:
        return first
    if detail.startswith(_CATALOG_BLOCKED):
        return "catalog"
    return row.get("writer") or row.get("engine") or "catalog"


def unclassified() -> list[str]:
    """Tests of this session the dashboard could not place: no area, or an area without theme.

    ``flat_rows`` keeps such a test visible as "Unassigned" / "Other" - and compared like a base
    test, which can turn "nothing offered" into "1 of 1 works". The run overview lists them as
    NEW (gaps.write_summary), so the run fails instead of the numbers.

    Returns:
        One ``"<test id>: <what is missing>"`` per test.
    """
    out = []
    for r in _records.values():
        if r["status"] is None:
            continue
        area = next((name for name, pred in DIMENSIONS if pred(r)), None)
        if area is None:
            out.append(f"{r['nodeid']}: no area (reporting.DIMENSIONS)")
        elif theme_of(area) == "Other":
            out.append(f"{r['nodeid']}: area {area!r} has no theme (reporting.THEMES)")
    return out


def flat_rows(records: list[dict]) -> list[dict]:
    """One row per test for the dashboard: engine, area, capability, status, detail.

    Args:
        records: All result records.

    Returns:
        The rows.
    """
    rows = []
    for r in records:
        # Unassigned tests stay visible instead of silently dropping out; the run lists them
        # as NEW (unclassified).
        area = next((name for name, pred in DIMENSIONS if pred(r)), "Unassigned")
        p = r["params"]
        row = {
            "engine": _engine(r) or "",
            "theme": theme_of(area),
            "area": area,
            "capability": _capability(r),
            "status": r["status"],
            "detail": r["detail"],
            "cause": r.get("cause", ""),
            "error": r.get("error", ""),
            "severity": severity(r) or "",
            "writer": (
                p.get("engine")
                if r["test"] in (*CROSS_READ_TESTS, CROSS_WRITE_TEST)
                else p.get("writer") or ""
            ),
            "test": r["nodeid"],
            # Results of runs before manifests existed have no run id.
            "run": r.get("run", ""),
        }
        row["blocked_by"] = blocked_by(row)
        rows.append(row)
    return rows


CROSS_READ_TESTS = ("test_operation_cross_read", "test_maintenance_cross_read")
#: Writes after another engine's maintenance: the row is about the writer, produced by "engine".
CROSS_WRITE_TEST = "test_maintenance_cross_write"


def _engine(rec) -> str | None:
    """The engine a record is about (reader for read tests, writer/engine otherwise).

    Args:
        rec: Result record.

    Returns:
        The engine, or None.
    """
    p = rec["params"]
    if rec["test"] == CROSS_WRITE_TEST:
        return p["writer"]  # about the engine that writes after the other one's maintenance
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
        "Write after maintenance (neutral source)",
        lambda r: r["file"] == "test_neutral_source" and r["test"] == "test_write_maintained",
    ),
    (
        "Read file formats (neutral source)",
        lambda r: r["file"] == "test_neutral_source" and r["test"] == "test_read_format",
    ),
    (
        "Time zones (neutral source)",
        lambda r: r["file"] == "test_neutral_source" and r["test"] == "test_read_tz",
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
    ("Nested schema evolution", lambda r: r["test"].startswith("test_nested_evolution")),
    ("Time zones", lambda r: r["file"] == "test_timezones"),
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
        lambda r: (
            r.get("specific") and r["file"] in ("test_catalog_features", "test_gravitino_fronting")
        ),
    ),
    # The example notebooks run on every catalog, but they are demos, not capability tests.
    ("Demo notebooks", lambda r: r["file"] == "test_notebooks"),
    ("Catalog functions", lambda r: r["file"] == "test_catalog_features"),
    ("Security", lambda r: r["file"] == "test_security"),
    ("Catalog consistency", lambda r: r["file"] == "test_catalog_consistency"),
    ("REST conformance", lambda r: r["file"] == "test_rest_conformance"),
    ("Branch isolation", lambda r: r["file"] == "test_branch_isolation"),
    ("Re-create with an open session", lambda r: r["file"] == "test_recreate_session"),
    (
        "Files per specification",
        lambda r: r["file"] == "test_file_spec" or r["test"] == "test_nan_metrics",
    ),
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
            r["nodeid"].startswith("tests/selftest/")
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
    ("Write after others' maintenance", lambda r: r["test"] == CROSS_WRITE_TEST),
]


#: Areas that are reported but not part of the comparison (features of one catalog/engine).
NOT_COMPARED = ("Catalog-specific", "Engine-specific", "Lab self-test", "Demo notebooks")

#: Every compared area belongs to exactly one theme (checked by
#: tests/selftest/test_lab_selftest.py::test_every_compared_area_has_a_theme). The dashboard and
#: the theme table start from these few groups instead of 50+ areas.
THEMES = {
    "Read & write data": (
        "Access modes",
        "Interop (one writes, all read)",
        "Operations",
        "Read others' results",
        "Read results (neutral source)",
        "Write file formats",
        "Read file formats",
        "Read file formats (neutral source)",
        "Write large files",
        "Read large files",
        "Performance (correctness only)",
    ),
    "Types": (
        "Write types",
        "Read types",
        "Read types (neutral source)",
        "Type fidelity",
        "Type check by the catalog",
        "Time zones",
        "Time zones (neutral source)",
    ),
    "Partitions & evolution": (
        "Write partitions",
        "Read partitions",
        "Read partitions (neutral source)",
        "Schema evolution",
        "Nested schema evolution",
        "Partition evolution",
        "Read evolution (neutral source)",
    ),
    "Table features": (
        "v3 features",
        "Views",
        "Namespaces",
        "Statistics",
        "File layout",
        "Files per specification",
        "Branch isolation",
        "Write-Audit-Publish",
        "Incremental & streaming",
    ),
    "Maintenance": (
        "Maintenance",
        "Read others' maintenance results",
        "Write after others' maintenance",
        "Read maintenance results (neutral source)",
        "Write after maintenance (neutral source)",
    ),
    "Concurrency & robustness": (
        "Concurrent commits",
        "Transactions (several tables)",
        "Re-create with an open session",
        "Robustness (catalog faults)",
        "Storage faults (catalog)",
        "Catalog consistency",
        "Token endpoint outage",
    ),
    "Security & access": (
        "Authorization",
        "Roles and inherited grants",
        "Single privileges (inventory and effect)",
        "Security",
        "STS reach",
        "STS expiry",
        "Credentials: reach and signing",
        "Connection without help",
        "Web UI",
    ),
    "Catalog functions": (
        "Catalog functions",
        "Catalog administration",
        "REST conformance",
        "Scan planning (server)",
        "Object storage (S3 behaviour)",
        "Storage (RustFS S3 API)",
    ),
}
_THEME_OF = {area: theme for theme, areas in THEMES.items() for area in areas}


def theme_of(area: str) -> str:
    """The theme of an area (``"Not compared"`` for special features, demos and self-tests,
    ``"Other"`` for an area no theme lists - the lab self-test reports that).

    Args:
        area: Area name (``DIMENSIONS``).

    Returns:
        The theme.
    """
    if area in NOT_COMPARED:
        return "Not compared"
    return _THEME_OF.get(area, "Other")
