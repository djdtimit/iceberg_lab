"""Self-tests of the lab itself: its mechanisms must never hide a failure they do not document.

Runs without catalog or engines. Covered:

* ``expect_gap``: only the documented error counts as a known gap; any other error comes
  through unchanged; a gap that no longer reproduces fails as "gap fixed"; a gap without
  pattern or with one that also matches unrelated errors (outage, timeout, wrong result) is
  refused; a reason that sounds like a defect needs a severity prefix.
* central catalog rules (``CATALOG_GAPS``): they apply only to the engines/files named in the
  rule, only on their catalog, and only with the required traceback frames; they are specific
  and classified.
* dependent reads: a read whose producer did not run is no record, one after a failed producer
  is blocked; a producer that failed after correct content still feeds the reads, wrong or
  unreadable content and skips do not (``keep_if_content_ok``).
* the report: it takes the raised reason (not expect_gap's docstring), central and fixed
  reasons, and marks severe gaps; every compared area has a theme; every row names its run.
* notebooks: a ``catalog_limit`` cell may print "not possible" only where an
  ``expect_<catalog>`` documents that limit.
* documentation: every test says what it checks (``Checks:``) and describes its arguments;
  every function, method and class documents arguments and result (``scripts/probes`` aside).
"""

from __future__ import annotations

import types

import pytest
import test_notebooks

from iceberg_lab.lab import gaps, reporting
from iceberg_lab.lab.gaps import GapFixedError, KnownGapError, expect_gap

import conftest

#: Needs no catalog or engine: runs without the lab (``uv run pytest -m offline``).
pytestmark = pytest.mark.offline


@pytest.fixture(autouse=True)
def _isolated_summary(monkeypatch):
    """The checks below record gap hits; keep them out of the run's real summary.

    Args:
        monkeypatch: pytest monkeypatch fixture.
    """
    for name in ("_KNOWN_GAP_HITS", "_FIXED_GAPS", "_FLAKY_NOT_SEEN", "_FLAKY_SEEN"):
        monkeypatch.setattr(gaps, name, [])
    monkeypatch.setattr(gaps, "KNOWN_GAPS_MODE", "fail")


# ---- expect_gap -------------------------------------------------------------------------------


def test_expect_gap_documented_error_is_a_known_gap():
    """An error matching the documented pattern becomes a known gap.

    Checks:
        * ``KnownGapError`` "[known gap] <reason>" is raised and the hit is recorded.
    """
    with pytest.raises(KnownGapError, match=r"\[known gap\] doc"), expect_gap("doc", r"boom \d+"):
        raise RuntimeError("boom 42")
    assert [r for _, r in gaps._KNOWN_GAP_HITS] == ["doc"]


def test_expect_gap_other_error_comes_through_unchanged():
    """An error NOT matching the pattern comes through unchanged (a new failure).

    Checks:
        * the original ValueError propagates and no gap hit is recorded.
    """
    with pytest.raises(ValueError, match="something else"), expect_gap("doc", r"boom \d+"):
        raise ValueError("something else")
    assert gaps._KNOWN_GAP_HITS == []


def test_expect_gap_matches_type_name_and_message():
    # The regex sees "<ExceptionType>: <message>" - a gap on one type does not catch another.
    """The pattern is matched against ``"<ExceptionType>: <message>"``.

    Checks:
        * a pattern naming another exception type does not catch a KeyError.
    """
    with pytest.raises(KeyError), expect_gap("doc", r"^RuntimeError: x"):
        raise KeyError("x")


def test_expect_gap_no_error_means_fixed():
    """A documented gap that does not occur is reported as fixed.

    Checks:
        * ``GapFixedError`` "[gap fixed]" is raised and recorded.
    """
    with pytest.raises(GapFixedError, match=r"\[gap fixed\]"), expect_gap("doc", "boom"):
        pass
    assert [r for _, r in gaps._FIXED_GAPS] == ["doc"]


def test_expect_gap_flaky_without_error_passes_and_is_recorded():
    """An intermittent gap that does not occur passes and is recorded as "not seen".

    Checks:
        * no error; the gap is in the not-seen list, not in the fixed list.
    """
    with expect_gap("doc", "boom", flaky=True):
        pass
    assert [r for _, r in gaps._FLAKY_NOT_SEEN] == ["doc"]
    assert gaps._FIXED_GAPS == []
    # The report classifies this test's own row before the fixture's teardown restores the
    # list: left in, "doc" would make this self-test a gap (reporting._classify).
    gaps._FLAKY_NOT_SEEN.clear()


def test_expect_gap_without_reason_changes_nothing():
    """Without a documented gap the block runs normally.

    Checks:
        * errors propagate unchanged; a passing block passes.
    """
    with pytest.raises(RuntimeError, match="boom"), expect_gap(None, None):
        raise RuntimeError("boom")
    with expect_gap(None, None):
        pass


# ---- central catalog rules --------------------------------------------------------------------


def _central(monkeypatch, catalog: str, nodeid: str, exc: BaseException):
    """Run the report hook on a fake failed test; return the report (longrepr is labelled).

    Args:
        monkeypatch: pytest monkeypatch fixture.
        catalog: Catalog to pretend.
        nodeid: Fake test id.
        exc: The failure.

    Returns:
        The report.
    """
    monkeypatch.setattr(conftest, "PROFILE", types.SimpleNamespace(name=catalog))
    try:
        raise exc
    except BaseException:  # noqa: BLE001 - captures whatever the fake test raised
        excinfo = pytest.ExceptionInfo.from_current()
    report = types.SimpleNamespace(failed=True, longrepr="original", longreprtext="original")
    outcome = types.SimpleNamespace(get_result=lambda: report)
    call = types.SimpleNamespace(when="call", excinfo=excinfo)
    hook = conftest.pytest_runtest_makereport(types.SimpleNamespace(nodeid=nodeid), call)
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(outcome)
    return report


DUCKDB_HOST = RuntimeError(
    "Invalid Input Error: Could not parse AWS service from host: rustfs:9000"
)


def test_central_rule_applies_to_its_engine(monkeypatch):
    """A central catalog rule books a failure of its engine's test as known gap.

    Checks:
        * the report starts with "[known gap]".

    Args:
        monkeypatch: pytest fixture to swap the catalog profile / rules for the check.
    """
    report = _central(monkeypatch, "rustfs", "tests/test_x.py::test_read[duckdb]", DUCKDB_HOST)
    assert str(report.longrepr).startswith("[known gap]")


def test_central_rule_does_not_cover_another_engine(monkeypatch):
    # Same message in a Spark test: must stay a new failure (a shared fixture hid 258 checks once).
    """A central rule does not book another engine's failure.

    Checks:
        * the report stays the original and no hit is recorded.

    Args:
        monkeypatch: pytest fixture to swap the catalog profile / rules for the check.
    """
    report = _central(monkeypatch, "rustfs", "tests/test_x.py::test_read[spark]", DUCKDB_HOST)
    assert report.longrepr == "original"
    assert gaps._KNOWN_GAP_HITS == []


def test_central_rule_does_not_cover_another_catalog(monkeypatch):
    """A central rule of one catalog does not apply on another catalog.

    Checks:
        * the report stays the original.

    Args:
        monkeypatch: pytest fixture to swap the catalog profile / rules for the check.
    """
    report = _central(monkeypatch, "polaris", "tests/test_x.py::test_read[duckdb]", DUCKDB_HOST)
    assert report.longrepr == "original"


def test_central_rule_requires_its_traceback_frames(monkeypatch):
    # The RustFS SigV4 rule needs a frame in pyiceberg/catalog/rest - raised here, it is absent.
    """A central rule with required frames applies only if they are in the traceback.

    Checks:
        * without the frames the report stays the original.

    Args:
        monkeypatch: pytest fixture to swap the catalog profile / rules for the check.
    """
    exc = RuntimeError("missing header: x-amz-content-sha256")
    report = _central(monkeypatch, "rustfs", "tests/test_x.py::test_write[pyiceberg]", exc)
    assert report.longrepr == "original"


def test_every_central_rule_names_engines_or_files():
    # An empty node part would match every test of the catalog.
    """Every central rule names the engines/files it belongs to and a real pattern.

    Checks:
        * no empty alternative in the node part; the regex is not ``.*``/``.+``.
    """
    for cat, node, regex, _frames, reason in gaps.CATALOG_GAPS:
        assert all(alt.strip() for alt in node.split("|")), (cat, reason)
        assert regex and regex not in (".*", ".+"), (cat, reason)


def test_gap_without_pattern_is_refused():
    """A gap without an error pattern (would accept any failure) is refused.

    Checks:
        * ValueError "without error pattern".
    """
    with pytest.raises(ValueError, match="without error pattern"), expect_gap("doc", None):
        pass


@pytest.mark.parametrize("pattern", [r"error", r"timed? out", r"500", r"Connection", r"assert"])
def test_too_generic_gap_pattern_is_refused(pattern):
    # Such a pattern would book an outage, a timeout or a wrong result as "known gap".
    """A gap pattern that also matches generic errors (outage, timeout, 500, ...) is refused.

    Checks:
        * ValueError "too generic".

    Args:
        pattern: A too generic gap pattern.
    """
    with pytest.raises(ValueError, match="too generic"), expect_gap("doc", pattern):
        raise RuntimeError("never reached")


def test_central_rules_are_specific():
    # Rules that also require traceback frames may be broader (the frames narrow them).
    """Every central rule's pattern is specific.

    Checks:
        * no central rule matches a generic error (``GENERIC_ERRORS``).
    """
    rules = [(r, reason) for _c, _n, r, frames, reason in gaps.CATALOG_GAPS if not frames]
    rules += [(r, reason) for _n, r, frames, reason in gaps.FLAKY_GAPS if not frames]
    for regex, reason in rules:
        gaps._require_specific(regex, reason)


# ---- notebooks --------------------------------------------------------------------------------


def _cell(source: str, output: str, lab: dict | None = None):
    """A fake executed notebook cell.

    Args:
        source: Cell source.
        output: Printed output.
        lab: ``lab`` metadata (None: none).

    Returns:
        The cell.
    """
    meta = {"lab": lab} if lab is not None else {}
    outputs = [{"output_type": "stream", "name": "stdout", "text": output}]
    return {"cell_type": "code", "source": source, "metadata": meta, "outputs": outputs}


LIMIT = 'with catalog_limit("Branch anlegen"):\n    t.manage_snapshots()'


def test_notebook_limit_without_expectation_fails():
    """A notebook cell showing a limitation without a written expectation fails.

    Checks:
        * AssertionError "forbidden".
    """
    cell = _cell(LIMIT, "[rustfs] Create branch not possible: 406")
    with pytest.raises(AssertionError, match="forbidden"):
        test_notebooks.check_cell(0, cell, "rustfs")


def test_notebook_limit_with_expectation_passes():
    """A notebook cell whose limitation is expected for this catalog passes.

    Checks:
        * the cell counts as checked.
    """
    cell = _cell(LIMIT, "[rustfs] Create branch not possible: 406", {"expect_rustfs": "406"})
    assert test_notebooks.check_cell(0, cell, "rustfs")


def test_notebook_expectation_of_another_catalog_does_not_count():
    """An expectation written for another catalog does not excuse this one.

    Checks:
        * AssertionError "forbidden".
    """
    cell = _cell(LIMIT, "[rustfs] Create branch not possible: 406", {"expect_unity": "not"})
    with pytest.raises(AssertionError, match="forbidden"):
        test_notebooks.check_cell(0, cell, "rustfs")


def test_notebook_expect_must_match():
    """An expected-output check must match the cell's output.

    Checks:
        * AssertionError "does not match".
    """
    cell = _cell("print(t.scan().to_arrow().num_rows)", "3", {"expect": r"^5$"})
    with pytest.raises(AssertionError, match="does not match"):
        test_notebooks.check_cell(0, cell, "lakekeeper")


def test_notebook_cell_without_checks_is_not_counted():
    """A cell without expected-output checks does not count as checked.

    Checks:
        * ``check_cell`` returns false.
    """
    assert not test_notebooks.check_cell(0, _cell("x = 1", ""), "lakekeeper")


# ---- dependent read tests ---------------------------------------------------------------------


def test_read_without_producer_in_session_is_not_a_result():
    # A targeted run of only the readers must not overwrite real results with "blocked".
    """A dependent read whose producer did not run in this session records no result.

    Checks:
        * the skip reason carries the NOT_RUN marker;
        * the report classifies it as "no record" - a targeted re-run cannot overwrite the full
          run's result.
    """
    with pytest.raises(pytest.skip.Exception) as exc:
        conftest.skip_if_not_produced({}, ("spark", "insert", 2), "test_operation")
    assert conftest.NOT_RUN in str(exc.value)
    report = types.SimpleNamespace(
        longrepr=("f", 1, f"Skipped: {exc.value}"), longreprtext="", passed=False, skipped=True
    )
    assert reporting._classify(report) == (None, "")


def test_read_after_failed_producer_is_blocked():
    """A dependent read after a failed producer is "blocked" (not testable).

    Checks:
        * the skip names the producer that did not succeed, without the NOT_RUN marker.
    """
    with pytest.raises(pytest.skip.Exception) as exc:
        conftest.skip_if_not_produced({("spark", "insert", 2): None}, ("spark", "insert", 2), "x")
    assert conftest.NOT_RUN not in str(exc.value)
    assert "spark insert v2 did not succeed in x" in str(exc.value)


# ---- reporting --------------------------------------------------------------------------------


def _failed(text: str):
    """A fake failed report with this long text.

    Args:
        text: Report text.

    Returns:
        The report.
    """
    return types.SimpleNamespace(longrepr=text, longreprtext=text, passed=False, skipped=False)


def test_severe_sounding_gap_without_severity_is_refused():
    """A gap reason that sounds like a defect must carry a severity prefix.

    Checks:
        * ValueError mentioning the severity.
    """
    with (
        pytest.raises(ValueError, match="severity"),
        expect_gap("engine silently drops the row", "row missing"),
    ):
        pass


def test_gap_with_severity_is_accepted_and_classified():
    """A gap with a severity prefix is accepted and classified.

    Checks:
        * the gap reproduces as known gap;
        * ``gap_severity`` maps the prefix to its label, a plain reason to None.
    """
    with (
        pytest.raises(KnownGapError),
        expect_gap("WRONG RESULT: engine silently drops the row", "row missing"),
    ):
        raise AssertionError("row missing")
    assert gaps.gap_severity("WRONG RESULT: x") == "Wrong result"
    assert gaps.gap_severity("Engine has no API for x") is None


def test_central_rules_are_classified():
    """Every central rule's reason that sounds severe carries a severity.

    Checks:
        * ``_require_severity`` accepts all FLAKY_GAPS and CATALOG_GAPS reasons.
    """
    for *_, reason in list(gaps.FLAKY_GAPS) + list(gaps.CATALOG_GAPS):
        gaps._require_severity(reason)


def test_report_marks_severe_gaps():
    """The report marks severe gaps.

    Checks:
        * a gap with ``DATA LOSS:`` has severity Datenverlust; a pass has none.
    """
    rec = {"status": "gap", "detail": "DATA LOSS: writes are lost"}
    assert reporting.severity(rec) == "Data loss"
    assert reporting.severity({"status": "pass", "detail": "DATA LOSS: x"}) is None


def test_every_compared_area_has_a_theme():
    """The dashboard and the theme table group areas into themes; none may fall outside.

    Checks:
        * every compared area of ``reporting.DIMENSIONS`` is in exactly one theme;
        * every area a theme lists exists (no stale name after a rename);
        * areas that are not compared get the theme "Not compared".
    """
    compared = [n for n, _ in reporting.DIMENSIONS if n not in reporting.NOT_COMPARED]
    listed = [a for areas in reporting.THEMES.values() for a in areas]
    assert sorted(set(compared) - set(listed)) == [], "areas without a theme"
    assert sorted(set(listed) - set(compared)) == [], "themes list unknown areas"
    assert len(listed) == len(set(listed)), "an area is in two themes"
    assert {reporting.theme_of(a) for a in reporting.NOT_COMPARED} == {"Not compared"}


def test_report_records_the_run():
    """Results are merged over runs, so every row must say which run produced it.

    Checks:
        * a row of ``flat_rows`` carries the record's run id and its area's theme;
        * a record of a run before manifests existed gets an empty run id, not a wrong one.
    """
    rec = {
        "nodeid": "tests/engines/test_views.py::test_x",
        "file": "test_views",
        "test": "test_x",
        "params": {},
        "status": "pass",
        "detail": "",
        "run": "R1",
    }
    row = reporting.flat_rows([rec])[0]
    assert (row["run"], row["area"], row["theme"]) == ("R1", "Views", "Table features")
    old = {k: v for k, v in rec.items() if k != "run"}
    assert reporting.flat_rows([old])[0]["run"] == ""


@pytest.mark.parametrize(
    ("detail", "writer", "engine", "expected"),
    [
        ("not written by polars: ValueError", "polars", "duckdb", "polars"),
        ("pg_lake identity v2 did not succeed in test_partitioned_write", "", "duckdb", "pg_lake"),
        ("spark did not create a view (see test_create_view)", "", "trino", "spark"),
        ("duckdb could not delete on v3: HTTP Error", "", "duckdb", "duckdb"),
        ("neither an engine nor the neutral source wrote geometry on v3", "", "spark", "catalog"),
        ("not testable on rustfs: the catalog is the storage itself", "", "", "catalog"),
        ("some other prerequisite failed", "flink", "spark", "flink"),
        ("some other prerequisite failed", "", "spark", "spark"),
    ],
)
def test_blocked_case_is_charged_to_its_cause(detail, writer, engine, expected):
    """A blocked read says nothing about the reader, so it counts at the step that failed.

    Checks:
        * ``blocked_by`` names the engine the reason starts with, the catalog for reasons
          without an engine, else the row's writer, else the engine itself;
        * ``counted_status`` makes a case blocked by someone else "foreign" (not counted for the
          engine) and keeps one blocked by the engine's own step "blocked" - for a test without
          an engine the catalog is its own subject.

    Args:
        detail: Skip reason.
        writer: Writer of the row ("" if none).
        engine: Engine of the row.
        expected: Expected cause.
    """
    row = {"status": "blocked", "detail": detail, "writer": writer, "engine": engine}
    assert reporting.blocked_by(row) == expected
    own = engine or "catalog"  # a test without an engine is about the catalog
    assert reporting.counted_status(row) == ("blocked" if expected == own else "foreign")
    assert reporting.blocked_by({**row, "status": "gap"}) == ""


def test_foreign_blocked_cases_leave_the_engines_coverage():
    """Coverage of an engine must not drop because another engine could not write.

    Checks:
        * a case blocked by another engine is in no denominator of the engine;
        * a case blocked by the engine's own step still counts against its coverage.
    """
    k = reporting._counts(["pass", "pass", "foreign", "foreign", "blocked"])
    assert (k["works"], k["all"], k["foreign"]) == (2, 3, 2)


def _producer(results, content_ok, error):
    """A producer block with keep_if_content_ok + expect_gap that fails with ``error``.

    Args:
        results: Result dict.
        content_ok: Content check.
        error: The failure to raise.
    """
    with (
        pytest.raises(KnownGapError),
        conftest.keep_if_content_ok(results, ("e", "op"), ("ns", "t"), content_ok),
        expect_gap("doc", "property check"),
    ):
        raise error


def test_gap_after_correct_content_still_feeds_the_reads():
    """A producer failing AFTER correct content still feeds the dependent reads.

    Checks:
        * the result is booked.
    """
    results = {("e", "op"): None}
    _producer(results, lambda: True, AssertionError("property check failed"))
    assert results[("e", "op")] == ("ns", "t")


def test_gap_with_wrong_content_blocks_the_reads():
    """A producer with wrong content feeds no reads.

    Checks:
        * the result stays None (blocked).
    """
    results = {("e", "op"): None}
    _producer(results, lambda: False, AssertionError("property check failed"))
    assert results[("e", "op")] is None


def test_unreadable_table_blocks_the_reads():
    """A producer whose table cannot even be read feeds no reads.

    Checks:
        * the result stays None.
    """

    def unreadable():
        """A content check whose table does not exist.

        Raises:
            RuntimeError: always.
        """
        raise RuntimeError("table does not exist")

    results = {("e", "op"): None}
    _producer(results, unreadable, AssertionError("property check failed"))
    assert results[("e", "op")] is None


def test_skipped_producer_feeds_no_reads():
    """A skipped producer (no API) feeds no reads.

    Checks:
        * the result stays None.
    """
    results = {("e", "op"): None}
    with (
        pytest.raises(pytest.skip.Exception),
        conftest.keep_if_content_ok(results, ("e", "op"), ("ns", "t"), lambda: True),
    ):
        pytest.skip("engine has no API")
    assert results[("e", "op")] is None


def test_report_takes_the_raised_gap_reason_not_the_docstring():
    """The report takes the gap reason from the raised error, not from expect_gap's docstring.

    Checks:
        * classified as gap with the real reason.
    """
    text = (
        '    ``fail`` mode: :class:`KnownGapError` "[known gap] <reason>" chained to the original\n'
        '        raise KnownGapError(f"[known gap] {reason}") from exc\n'
        "E   conftest.KnownGapError: [known gap] the real reason\n"
    )
    assert reporting._classify(_failed(text)) == ("gap", "the real reason")


def test_report_takes_a_central_rule_reason():
    """The report takes a central rule's reason.

    Checks:
        * classified as gap with the central reason.
    """
    text = '[known gap] central reason\n\n    "[known gap] <reason>" chained to the original\n'
    assert reporting._classify(_failed(text)) == ("gap", "central reason")


def test_report_takes_the_fixed_reason():
    """The report takes the reason of a fixed gap.

    Checks:
        * classified as fixed with that reason.
    """
    text = (
        '    :class:`GapFixedError` "[gap fixed] <reason>" in both modes\n'
        "E   conftest.GapFixedError: [gap fixed] no longer reproduces - remove: it works now\n"
    )
    assert reporting._classify(_failed(text)) == ("fixed", "it works now")


def test_every_test_documents_what_it_checks_and_its_arguments():
    """Every test says what exactly it checks and what each of its arguments is.

    Checks:
        * every ``test_*`` function in ``tests/**/test_*.py`` has a docstring with a ``Checks:``
          section (``Checks (<scope>):`` too);
        * every parameter of it is described under ``Args:`` (``name: ...``).
    """
    import ast
    import pathlib
    import re

    problems = []
    for path in sorted(pathlib.Path(__file__).resolve().parents[1].rglob("test_*.py")):
        for node in ast.parse(path.read_text()).body:
            if not (isinstance(node, ast.FunctionDef) and node.name.startswith("test_")):
                continue
            doc = ast.get_docstring(node) or ""
            where = f"{path.name}::{node.name}"
            if not re.search(r"^Checks\b[^\n]*:$", doc, re.MULTILINE):
                problems.append(f"{where}: no 'Checks:' section")
            args = doc.split("Args:", 1)[1] if "Args:" in doc else ""
            for a in node.args.args:
                if not re.search(rf"^\s+{a.arg}:", args, re.MULTILINE):
                    problems.append(f"{where}: argument '{a.arg}' not described under 'Args:'")
    assert not problems, "\n".join(problems)


def test_every_function_documents_its_arguments_and_result():
    """Every function, method and class of the lab is documented (not only the tests).

    Scope: ``src/iceberg_lab``, ``tests``, ``unity`` and ``scripts`` - except
    ``scripts/probes`` (one-off exploration scripts, not part of the lab's logic).

    Checks:
        * every class and every function (also nested ones and methods) has a docstring;
        * every parameter (except ``self``/``cls``) is described under ``Args:``;
        * a function that returns a value has ``Returns:``, a generator has ``Yields:``.
        Test functions are covered by the test above (``Checks:`` instead of ``Returns:``).
    """
    import ast
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[2]
    files = [
        *sorted((root / "src" / "iceberg_lab").glob("*.py")),
        *sorted((root / "tests").rglob("*.py")),
        *sorted((root / "unity").glob("*.py")),
        *sorted((root / "scripts").glob("*.py")),
        *sorted((root / "scripts" / "dashboard").glob("*.py")),
    ]

    def own(fn):
        """Nodes of a function body, without descending into nested functions/classes.

        Args:
            fn: Function node.

        Yields:
            The nodes.
        """
        stack = list(fn.body)
        while stack:
            n = stack.pop()
            yield n
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            stack.extend(ast.iter_child_nodes(n))

    problems = []

    def check(node, where):
        """Record the documentation problems of one class or function.

        Args:
            node: Class or function node.
            where: ``<file>::<qualified name>`` for the message.
        """
        doc = ast.get_docstring(node)
        if not doc:
            problems.append(f"{where}: no docstring")
            return
        if isinstance(node, ast.ClassDef):
            return
        a = node.args
        params = [
            x.arg for x in a.posonlyargs + a.args + a.kwonlyargs if x.arg not in ("self", "cls")
        ]
        params += [x.arg for x in (a.vararg, a.kwarg) if x]
        args = doc.split("Args:", 1)[1] if "Args:" in doc else ""
        for p in params:
            if not re.search(rf"^\s+\*{{0,2}}{p}( \(.*\))?:", args, re.MULTILINE):
                problems.append(f"{where}: argument '{p}' not under 'Args:'")
        body = list(own(node))
        yields = any(isinstance(n, (ast.Yield, ast.YieldFrom)) for n in body)
        returns = any(
            isinstance(n, ast.Return)
            and n.value is not None
            and not (isinstance(n.value, ast.Constant) and n.value.value is None)
            for n in body
        )
        if yields and "Yields:" not in doc:
            problems.append(f"{where}: generator without 'Yields:'")
        elif returns and not yields and "Returns:" not in doc:
            problems.append(f"{where}: returns a value, no 'Returns:'")

    def visit(node, prefix, path, top):
        """Check every class/function below a node (test functions excluded).

        Args:
            node: AST node.
            prefix: Qualified name so far.
            path: Source file.
            top: Module level (test functions are skipped there).
        """
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                q = f"{prefix}.{child.name}" if prefix else child.name
                is_test = top and child.name.startswith("test_") and path.name.startswith("test_")
                if not is_test:
                    check(child, f"{path.relative_to(root)}::{q}")
                visit(child, q, path, False)

    for path in files:
        visit(ast.parse(path.read_text()), "", path, True)
    assert not problems, f"{len(problems)} undocumented:\n" + "\n".join(problems[:60])


def test_manifest_reads_every_compose_image():
    """The run manifest's compose parser (no YAML library at run time) finds the image of every
    service exactly as a YAML parser does.

    Checks:
        * ``manifest.declared_images()`` equals ``{service: image}`` from PyYAML - a service key
          with a trailing comment once made the next service's image overwrite pg-lake's.
    """
    import yaml

    from iceberg_lab.lab.manifest import COMPOSE, declared_images

    services = yaml.safe_load(COMPOSE.read_text())["services"]
    expected = {name: s["image"] for name, s in services.items() if "image" in s}
    assert declared_images() == expected
