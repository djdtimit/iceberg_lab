"""Self-tests of the reference export (``scripts/export_reference.py``).

It writes ``results/``: what git diffs, the findings and the dashboard read. Everything here
runs on temporary directories.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
from pathlib import Path

import pytest

from iceberg_lab.lab import gap_register as gr

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "export_reference.py"


@pytest.fixture(scope="module")
def ex():
    """The script as a module (scripts/ is not a package).

    Returns:
        The loaded module.
    """
    spec = importlib.util.spec_from_file_location("export_reference", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def cause():
    """Any cause of the register.

    Returns:
        The cause.
    """
    return next(iter(gr.register().values()))


def test_name_of_report_directories(ex, tmp_path):
    """``reports/`` is Lakekeeper's report; every other directory is named after itself.

    Checks:
        * ``reports`` -> ``lakekeeper``; ``reports/gravitino_unity`` -> ``gravitino_unity``.

    Args:
        ex: The export script.
        tmp_path: pytest's temporary directory.
    """
    assert ex.name_of(tmp_path / "reports") == "lakekeeper"
    assert ex.name_of(tmp_path / "reports" / "gravitino_unity") == "gravitino_unity"


def test_still_exists_follows_the_function(ex):
    """A row is exported only while its test function exists (a rename leaves no old row).

    Checks:
        * an existing test with parameters exists; a missing function or file does not.

    Args:
        ex: The export script.
    """
    assert ex.still_exists(
        "tests/selftest/test_export_reference.py::test_name_of_report_directories"
    )
    assert ex.still_exists("tests/engines/test_types.py::test_write[duckdb-v2-int]")
    assert not ex.still_exists("tests/engines/test_types.py::test_no_such_function[x]")
    assert not ex.still_exists("tests/no_such_file.py::test_x")


def test_with_causes_sets_the_current_reason(ex, cause):
    """A gap's detail becomes its cause's current reason; a reworded one keeps its cause.

    Checks:
        * a row with a cause gets that cause's reason as detail;
        * a gap row whose old reason no longer names a cause gets it from the reference
          (``known``);
        * a pass and a gap without any known cause are left as they are.

    Args:
        ex: The export script.
        cause: A cause of the register.
    """
    rows = [
        {"test": "t::a", "status": "gap", "detail": "old wording", "cause": cause.id},
        {"test": "t::b", "status": "gap", "detail": "reworded long ago"},
        {"test": "t::c", "status": "pass", "detail": ""},
        {"test": "t::d", "status": "gap", "detail": "nobody knows"},
    ]
    out = ex.with_causes(rows, {"t::b": cause.id})
    assert out[0] == {**rows[0], "detail": cause.reason}
    assert out[1] == {**rows[1], "cause": cause.id, "detail": cause.reason}
    assert out[2:] == rows[2:]


def test_export_writes_sorted_lines_without_self(ex, tmp_path, cause):
    """The reference holds one sorted line per test, no ``self`` rows, a cause only on gaps.

    Checks:
        * lines sorted by test, the ``self`` row left out;
        * ``cause`` only in the gap line;
        * the full rows (``self`` too) are in ``<name>.rows.json.gz``;
        * a temporary target writes no run history.

    Args:
        ex: The export script.
        tmp_path: pytest's temporary directory (the target).
        cause: A cause of the register.
    """
    rows = [
        {"test": "tests/t.py::test_b", "status": "pass", "detail": ""},
        {"test": "tests/t.py::test_a", "status": "gap", "detail": "x", "cause": cause.id},
        {"test": "tests/t.py::test_c", "status": "self", "detail": "own result"},
    ]
    path = ex.export(rows, "probe", None, out=tmp_path)
    lines = [json.loads(x) for x in path.read_text().splitlines()]
    assert [x["test"] for x in lines] == ["tests/t.py::test_a", "tests/t.py::test_b"]
    assert lines[0]["cause"] == cause.id and "cause" not in lines[1]
    full = json.loads(gzip.decompress((tmp_path / "probe.rows.json.gz").read_bytes()))
    assert [r["test"] for r in full] == [
        "tests/t.py::test_a",
        "tests/t.py::test_b",
        "tests/t.py::test_c",
    ]
    assert not (tmp_path / "history").exists()


def test_history_counts_and_replaces_its_run(ex, tmp_path):
    """The trend line counts statuses without ``self``; a rerun replaces its run's line.

    Checks:
        * counts leave ``self`` out, causes are listed once;
        * no line without a recorded run;
        * a second line for the same reference and run replaces the first, other runs stay.

    Args:
        ex: The export script.
        tmp_path: pytest's temporary directory (the history file).
    """
    rows = [
        {"status": "pass"},
        {"status": "gap", "cause": "c1"},
        {"status": "gap", "cause": "c1"},
        {"status": "self"},
    ]
    line = ex.history_line("unity", rows, {"run": "r1", "finished": "f1"})
    assert line["counts"] == {"gap": 2, "pass": 1} and line["causes"] == ["c1"]
    assert ex.history_line("unity", rows, None) is None
    hist = tmp_path / "runs.jsonl"
    ex.update_history(line, hist)
    ex.update_history({**line, "run": "r0"}, hist)
    ex.update_history({**line, "counts": {"pass": 3}}, hist)
    got = [json.loads(x) for x in hist.read_text().splitlines()]
    assert [(x["run"], x["counts"]) for x in got] == [
        ("r0", {"gap": 2, "pass": 1}),
        ("r1", {"pass": 3}),
    ]


def test_rows_of_another_run_are_reported(ex):
    """Results of a run that was not meant as the reference are named at the export.

    A plain pytest wrote into Lakekeeper's report directory, and the next export took its 322
    rows into the reference.

    Checks:
        * rows of the full run and rows without run id (older results) -> no warning;
        * rows of another run -> one SHIFT line with their count;
        * no full run known -> nothing to compare.

    Args:
        ex: The script as a module.
    """
    rows = [{"run": "full"}] * 3 + [{"run": ""}] + [{"run": "adhoc"}] * 2
    assert ex.foreign_runs(rows[:4], "full") == []
    (line,) = ex.foreign_runs(rows, "full")
    assert line.startswith("SHIFT 2 rows from run adhoc, not from the full run full")
    assert ex.foreign_runs(rows, None) == []
