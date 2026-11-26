"""Self-tests of the run overview the shell scripts read (``scripts/run_summary.py``).

run_all.sh, run_parallel.sh and rerun.sh decide by it whether a run had NEW failures. Before,
each cut the text overview apart with its own awk line: more than 50 entries and a crashed
pytest process went unnoticed.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "run_summary.py"


@pytest.fixture(scope="module")
def rs():
    """The script as a module (scripts/ is not a package).

    Returns:
        The loaded module.
    """
    spec = importlib.util.spec_from_file_location("run_summary", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _logs(tmp_path, **files):
    """A log directory: ``name=summary dict`` writes ``name.log`` and its summary, ``None`` only
    the log (a pytest process that died).

    Args:
        tmp_path: pytest's temporary directory.
        **files: Summary per log name.

    Returns:
        The directory.
    """
    d = tmp_path / "logs"
    d.mkdir()
    for name, summary in files.items():
        (d / f"{name}.log").write_text("...")
        if summary is not None:
            (d / f"{name}.summary.json").write_text(json.dumps(summary))
    return d


def test_all_entries_and_crashes_count(rs, tmp_path, capsys):
    """Every entry is listed, a log without summary is NEW, exit code 1.

    Checks:
        * 60 NEW entries of one file all appear (the text overview stops at 50);
        * a log without its summary appears as NEW with its path;
        * the other sections keep their entries; the exit code is 1.

    Args:
        rs: The run summary script.
        tmp_path: pytest's temporary directory.
        capsys: pytest fixture capturing the output.
    """
    many = [f"tests/a.py::test_{i}" for i in range(60)]
    d = _logs(
        tmp_path,
        test_a={"new": many, "fixed": ["tests/a.py::test_f"], "cleanup": []},
        test_b={
            "new": [],
            "cleanup": ["tests/b.py::test_c"],
            "probably_fixed": ["tests/b.py::test_p"],
        },
        test_c=None,
    )
    assert rs.main([str(d)]) == 1
    out = capsys.readouterr().out
    assert all(f"  {t}\n" in out for t in many)
    assert f"  {d / 'test_c.log'}: no summary" in out
    assert "  tests/a.py::test_f\n" in out and "  tests/b.py::test_c\n" in out
    assert "  tests/b.py::test_p\n" in out


def test_clean_run_and_prefixed_kinds(rs, tmp_path, capsys):
    """Without NEW the exit code is 0; rerun.sh's prefixed form shows only the kinds asked.

    Checks:
        * a run with only fixed and cleanup entries exits 0;
        * ``--prefixed --kinds fixed,cleanup`` prints ``FIXED`` and ``CLEANUP`` lines, no headings;
        * rerun.sh's own logs in the directory are not part of a full run.

    Args:
        rs: The run summary script.
        tmp_path: pytest's temporary directory.
        capsys: pytest fixture capturing the output.
    """
    d = _logs(
        tmp_path, test_a={"new": [], "fixed": ["tests/a.py::t"], "cleanup": ["tests/a.py::c"]}
    )
    (d / "rerun.log").write_text("no summary next to it")
    assert rs.main([str(d)]) == 0
    capsys.readouterr()
    assert rs.main(["--prefixed", "--kinds", "fixed,cleanup", str(d)]) == 0
    assert capsys.readouterr().out == "FIXED tests/a.py::t\nCLEANUP tests/a.py::c\n"


def test_missing_summary_file_is_new(rs, tmp_path, capsys):
    """rerun.sh names its summary file; if pytest died before writing it, that is NEW.

    Checks:
        * a missing file given directly -> ``NEW <file>: no summary ...``, exit code 1.

    Args:
        rs: The run summary script.
        tmp_path: pytest's temporary directory.
        capsys: pytest fixture capturing the output.
    """
    f = tmp_path / "rerun.summary.json"
    assert rs.main(["--prefixed", "--kinds", "new", str(f)]) == 1
    assert capsys.readouterr().out.startswith(f"NEW {f}: no summary")
