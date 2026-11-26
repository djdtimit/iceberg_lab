"""Self-tests of the bug report facts (``iceberg_lab.lab.bug_drafts``, ``gap_register.py draft``).

The log parser pairs failure blocks with the short summary's FAILED/ERROR lines by their order:
a wrong pairing would put another test's error into a report without anyone noticing.
"""

from __future__ import annotations

import json

from iceberg_lab.lab import bug_drafts

#: A ``--tb=line`` log: two failures (the second a known gap with the original error first),
#: one setup error, and the short summary in pytest's order.
LOG = """\
..F.E.F                                                                    [100%]
=================================== ERRORS ===================================
E   RuntimeError: fixture broke
/workspaces/x/tests/t.py:5: RuntimeError: fixture broke
================================== FAILURES ==================================
E   AssertionError: Regex pattern did not match.
      Actual message: 'HTTP 500'

The above exception was the direct cause of the following exception:
E   iceberg_lab.lab.gaps.KnownGapError: [known gap] something
/workspaces/x/src/iceberg_lab/lab/gaps.py:206: iceberg_lab.lab.gaps.KnownGapError: [known gap]
E   requests.exceptions.HTTPError: 404 Client Error
/workspaces/x/tests/t.py:9: requests.exceptions.HTTPError: 404 Client Error
========================== iceberg lab: failure overview ==========================
NEW / unexpected failures: 0
=========================== short test summary info ===========================
FAILED tests/t.py::test_a[duckdb] - KnownGapError: [known gap] something
FAILED tests/t.py::test_b - requests.exceptions.HTTPError: 404 Client Error
ERROR tests/t.py::test_c - RuntimeError: fixture broke
2 failed, 1 error in 1.00s
"""


def _results(tmp_path, refs: dict[str, list[dict]]):
    """Write reference files ``<name>.jsonl`` into a temporary ``results/``.

    Args:
        tmp_path: pytest's temporary directory.
        refs: ``{reference: [row, ...]}``.

    Returns:
        The directory.
    """
    d = tmp_path / "results"
    d.mkdir()
    for name, rows in refs.items():
        (d / f"{name}.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    return d


def test_log_errors_pair_each_failure_with_its_test(tmp_path):
    """Each test gets the original error of its own failure block.

    Checks:
        * a known gap's test gets the first error of its block (with the continuation line),
          not the KnownGapError the lab raised from it;
        * the second failure and the setup error (ERRORS section) get their own errors.

    Args:
        tmp_path: pytest's temporary directory (a report directory with ``logs/``).
    """
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "t.log").write_text(LOG)
    errors = bug_drafts.log_errors(tmp_path)
    assert errors == {
        "tests/t.py::test_a[duckdb]": (
            "AssertionError: Regex pattern did not match. Actual message: 'HTTP 500'"
        ),
        "tests/t.py::test_b": "requests.exceptions.HTTPError: 404 Client Error",
        "tests/t.py::test_c": "RuntimeError: fixture broke",
    }


def test_log_errors_skip_a_log_that_does_not_pair(tmp_path):
    """A log whose blocks and summary lines differ in number gives no errors at all.

    Checks:
        * with a FAILED line missing, no test of that section gets an error (a shifted pairing
          would name the wrong test).

    Args:
        tmp_path: pytest's temporary directory (a report directory with ``logs/``).
    """
    (tmp_path / "logs").mkdir()
    broken = LOG.replace(
        "FAILED tests/t.py::test_b - requests.exceptions.HTTPError: 404 Client Error\n", ""
    )
    (tmp_path / "logs" / "t.log").write_text(broken)
    errors = bug_drafts.log_errors(tmp_path)
    assert "tests/t.py::test_a[duckdb]" not in errors
    assert errors == {"tests/t.py::test_c": "RuntimeError: fixture broke"}


def test_affected_and_elsewhere_split_the_references(tmp_path):
    """The references with the cause list its tests; the others say how the same tests end.

    Checks:
        * only references that book a test to the cause are affected;
        * elsewhere counts the affected tests per outcome, a gap with its own cause;
        * a reference that ran none of them is left out.

    Args:
        tmp_path: pytest's temporary directory (for a ``results/``).
    """
    cid = "pglake-ignores-refused-create"
    other = "pglake-ignores-refused-commit"
    d = _results(
        tmp_path,
        {
            "unity": [
                {"test": "t::a", "status": "gap", "detail": "", "cause": cid},
                {"test": "t::b", "status": "pass", "detail": ""},
            ],
            "polaris": [
                {"test": "t::a", "status": "pass", "detail": ""},
                {"test": "t::b", "status": "pass", "detail": ""},
            ],
            "gravitino": [{"test": "t::a", "status": "gap", "detail": "", "cause": other}],
            "rustfs": [{"test": "t::z", "status": "pass", "detail": ""}],
        },
    )
    refs = bug_drafts.references(d)
    hit = bug_drafts.affected(refs, cid)
    assert hit == {"unity": ["t::a"]}
    assert bug_drafts.elsewhere(refs, hit) == {
        "gravitino": {f"gap ({other})": 1},
        "polaris": {"pass": 1},
    }


def test_draft_names_cause_tests_and_reproduction(tmp_path, monkeypatch):
    """The draft holds the cause, where it shows, where it passes and how to reproduce it.

    Checks:
        * component and version from the register, the cause id;
        * the affected test under its reference, the other reference's outcome;
        * the raw error from the reference's log, not the lab's gap text;
        * a ``CATALOG=<reference> uv run pytest <test>`` line.

    Args:
        tmp_path: pytest's temporary directory (``results/`` and a report directory).
        monkeypatch: pytest fixture to point the report directories at it.
    """
    cid = "pglake-ignores-refused-create"
    d = _results(
        tmp_path,
        {
            "unity": [{"test": "tests/t.py::test_b", "status": "gap", "detail": "", "cause": cid}],
            "polaris": [{"test": "tests/t.py::test_b", "status": "pass", "detail": ""}],
        },
    )
    (tmp_path / "unity" / "logs").mkdir(parents=True)
    (tmp_path / "unity" / "logs" / "t.log").write_text(LOG)
    monkeypatch.setattr(bug_drafts, "report_dir", lambda ref: tmp_path / ref)
    text = bug_drafts.draft(cid, d)
    assert "**Version:** pg_lake " in text and f"`{cid}`" in text
    assert "- **unity**: 1 test(s)\n  - `tests/t.py::test_b`" in text
    assert "- polaris: 1 pass" in text
    assert "requests.exceptions.HTTPError: 404 Client Error" in text
    assert 'CATALOG=unity uv run pytest "tests/t.py::test_b"' in text


def test_recorded_errors_win_over_the_logs(tmp_path, monkeypatch):
    """The error stored in the reference's full rows is used before the logs.

    Checks:
        * with an ``error`` in ``<name>.rows.json.gz`` and another in the log, the draft shows
          the stored one;
        * without a log at all the stored error is still shown.

    Args:
        tmp_path: pytest's temporary directory (``results/`` and a report directory).
        monkeypatch: pytest fixture to point the report directories at it.
    """
    import gzip

    cid = "pglake-ignores-refused-create"
    test = "tests/t.py::test_b"
    d = _results(tmp_path, {"unity": [{"test": test, "status": "gap", "detail": "", "cause": cid}]})
    rows = [{"test": test, "status": "gap", "error": "psycopg.errors.X: stored error"}]
    (d / "unity.rows.json.gz").write_bytes(gzip.compress(json.dumps(rows).encode()))
    monkeypatch.setattr(bug_drafts, "report_dir", lambda ref: tmp_path / "none")
    assert "psycopg.errors.X: stored error" in bug_drafts.draft(cid, d)
    (tmp_path / "unity" / "logs").mkdir(parents=True)
    (tmp_path / "unity" / "logs" / "t.log").write_text(LOG)
    monkeypatch.setattr(bug_drafts, "report_dir", lambda ref: tmp_path / ref)
    text = bug_drafts.draft(cid, d)
    assert "stored error" in text and "404 Client Error" not in text


def test_bug_report_overview_matches_the_files_and_the_register():
    """docs/bug-reports/README.md, the drafts and the register stay in step.

    The overview also keeps withdrawn and fixed reports (their causes left the register), so it
    is checked, not generated.

    Checks:
        * every draft file ``NN-*.md`` has its row ``| NN |`` in the overview, and every row a file;
        * every register cause's ``bug_report`` file has a row there.
    """
    import re

    from iceberg_lab.lab import gap_register as gr

    readme = (gr.BUG_REPORTS / "README.md").read_text()
    rows = set(re.findall(r"^\| (\d\d) \|", readme, re.MULTILINE))
    files = {p.name[:2]: p.name for p in gr.BUG_REPORTS.glob("[0-9][0-9]-*.md")}
    assert set(files) - rows == set(), f"drafts without a row in the overview: {set(files) - rows}"
    assert rows - set(files) == set(), f"rows without a draft: {rows - set(files)}"
    linked = {c.bug_report[:2] for c in gr.register().values() if c.bug_report}
    assert linked <= rows, f"register links to reports missing in the overview: {linked - rows}"
