"""Self-tests of the status a test gets in the report (``iceberg_lab.lab.reporting``).

The status decides what the references, the findings and the dashboard say: a wrong one is
silent. Fake reports stand in for pytest's.
"""

from __future__ import annotations

import types

import pytest

from iceberg_lab.lab import gaps, reporting


def _report(when="call", outcome="failed", text="", **extra):
    """A fake pytest test report.

    Args:
        when: Phase (setup, call, teardown).
        outcome: passed, failed or skipped.
        text: Long report text (a skip's reason for skipped).
        **extra: More attributes (e.g. ``wasxfail``).

    Returns:
        The report.
    """
    longrepr = ("f.py", 1, f"Skipped: {text}") if outcome == "skipped" else (text or None)
    return types.SimpleNamespace(
        nodeid="tests/t.py::test_x",
        when=when,
        passed=outcome == "passed",
        skipped=outcome == "skipped",
        failed=outcome == "failed",
        longrepr=longrepr,
        longreprtext=text,
        **extra,
    )


@pytest.fixture(autouse=True)
def _no_flaky(monkeypatch):
    """No intermittent gap is pending (a pass stays a pass).

    Args:
        monkeypatch: pytest fixture to empty the bookkeeping.
    """
    monkeypatch.setattr(gaps, "_FLAKY_NOT_SEEN", [])


def test_pass_and_phases():
    """A passed call is a pass; a passed setup or teardown gives no record.

    Checks:
        * call passed -> ``("pass", "")``;
        * setup passed -> no status.
    """
    assert reporting._classify(_report(outcome="passed")) == ("pass", "")
    assert reporting._classify(_report(when="setup", outcome="passed"))[0] is None


def test_intermittent_gap_that_did_not_occur_stays_a_gap(monkeypatch):
    """One lucky run of an intermittent gap is not a pass.

    Checks:
        * a passed call with a pending intermittent gap -> ``gap`` with its reason.

    Args:
        monkeypatch: pytest fixture to register the intermittent gap.
    """
    monkeypatch.setattr(gaps, "_FLAKY_NOT_SEEN", [("tests/t.py::test_x", "races sometimes")])
    assert reporting._classify(_report(outcome="passed")) == ("gap", "races sometimes")


@pytest.mark.parametrize(
    ("reason", "status"),
    [
        ("catalog unity has no API for remote signing", "na"),
        ("[producer not run in this session]", None),
        ("own result is verified in the producing test", "self"),
        ("Spark failed before: cannot reach the table", "blocked"),
    ],
)
def test_skip_reasons(reason, status):
    """A skip's reason decides between not offered, own result, no record and blocked.

    Checks:
        * a "has no API" reason -> ``na``; the producer marker -> no record;
          "own result is verified" -> ``self``; anything else -> ``blocked``.

    Args:
        reason: The skip reason.
        status: The expected status.
    """
    assert reporting._classify(_report(outcome="skipped", text=reason))[0] == status


def test_raised_known_gap_takes_the_last_raised_reason():
    """A KnownGapError's reason comes from the raised line, not from expect_gap's source.

    Checks:
        * with the template line of expect_gap's source and the raised error in the text, the
          raised reason is the detail.
    """
    text = (
        'raise KnownGapError(f"[known gap] {reason}")\n'
        "E   iceberg_lab.lab.gaps.KnownGapError: [known gap] DuckDB loses the commit\n"
    )
    assert reporting._classify(_report(text=text)) == ("gap", "DuckDB loses the commit")


def test_central_rule_label_in_front_is_a_gap():
    """A central rule puts the reason in front of the report (conftest): that is a gap.

    Checks:
        * ``[known gap] <reason>`` as the first line -> ``gap`` with the reason.
    """
    text = "[known gap] RustFS refuses purge\n\nE   RESTError: 406"
    assert reporting._classify(_report(text=text)) == ("gap", "RustFS refuses purge")


def test_known_gap_text_inside_an_assertion_is_new():
    """The marker quoted inside a failing assertion is not a booked gap.

    A self-test that compares report texts failed with an assertion diff that contained
    "[known gap] ..." - it was recorded as a gap without a cause instead of a new failure.

    Checks:
        * an AssertionError whose diff contains ``[known gap]`` -> ``new``.
    """
    text = (
        "E   AssertionError: assert '[known gap] ...d\\n\\noriginal' == 'original'\n"
        "      + [known gap] DuckDB derives the SigV4 service from an AWS host name\n"
    )
    assert reporting._classify(_report(text=text))[0] == "new"


def test_fixed_gap_and_new_failure():
    """A GapFixedError is ``fixed``; any other failure is ``new`` with its first line.

    Checks:
        * GapFixedError's reason after the marker -> ``fixed``;
        * a plain failure -> ``new``, its first non-empty line cut to 240 characters.
    """
    fixed = "E   iceberg_lab.lab.gaps.GapFixedError: [gap fixed] remove: Trino reads it now\n"
    assert reporting._classify(_report(text=fixed)) == ("fixed", "Trino reads it now")
    long = "\n  E   ValueError: " + "x" * 300
    status, detail = reporting._classify(_report(text=long))
    assert status == "new" and detail.startswith("E   ValueError: x") and len(detail) == 240


def test_xfail_mode_is_a_gap():
    """KNOWN_GAPS=xfail turns a known gap into an xfail: still a gap with its reason.

    Checks:
        * a report with ``wasxfail`` -> ``gap`` with that reason.
    """
    assert reporting._classify(_report(outcome="skipped", wasxfail="reason")) == (
        "gap",
        "reason",
    )


def test_original_error_is_the_one_below_the_lab():
    """The recorded error is what the engine raised, not the lab's gap text.

    Checks:
        * a KnownGapError raised from an HTTP error records the HTTP error, module-qualified;
        * a GapFixedError over a KnownGapError over the error still finds the error;
        * an error left in place (central rule) records itself, a builtin without module;
        * a gap without an underlying error records nothing; long messages are cut to 500.
    """
    import requests

    def raised(make):
        """The exception a function raises.

        Args:
            make: Function that raises.

        Returns:
            The exception.
        """
        try:
            make()
        except Exception as e:  # noqa: BLE001 - whatever it raised
            return e
        raise AssertionError("did not raise")

    def gap_from_http():
        """Raise a known gap from an HTTP error."""
        try:
            raise requests.HTTPError("404 Client Error:\n  Not Found")
        except requests.HTTPError as e:
            raise gaps.KnownGapError("[known gap] x") from e

    def fixed_over_gap():
        """Raise a fixed gap over a known gap over the error."""
        try:
            gap_from_http()
        except gaps.KnownGapError as e:
            raise gaps.GapFixedError("[gap fixed] y") from e

    http = "requests.exceptions.HTTPError: 404 Client Error: Not Found"
    assert reporting.original_error(raised(gap_from_http)) == http
    assert reporting.original_error(raised(fixed_over_gap)) == http
    assert reporting.original_error(ValueError("bad")) == "ValueError: bad"
    assert reporting.original_error(gaps.KnownGapError("[known gap] z")) == ""
    assert len(reporting.original_error(ValueError("x" * 900))) == 500


def test_unplaced_test_is_listed_as_new(monkeypatch):
    """A test without an area would be compared like a base test: the run lists it as NEW.

    A self-test parametrized by engine fell through the area rules and gave pg_lake on RustFS
    "1 of 1 works" (100 %) in the dashboard.

    Checks:
        * a record of an unknown test file -> listed with "no area";
        * a lab self-test and a record without status (no record) -> not listed.

    Args:
        monkeypatch: pytest fixture to set this session's records.
    """

    def rec(nodeid, status="pass"):
        """A result record.

        Args:
            nodeid: Test id.
            status: Status (None = no record).

        Returns:
            The record.
        """
        path, test = nodeid.split("::")
        return {
            "nodeid": nodeid,
            "file": path.rsplit("/", 1)[-1].removesuffix(".py"),
            "test": test.split("[")[0],
            "params": {"engine": "pg_lake"},
            "specific": False,
            "status": status,
            "detail": "",
        }

    records = [
        rec("tests/engines/test_brand_new.py::test_x[pg_lake]"),
        rec("tests/selftest/test_whatever.py::test_y[pg_lake]"),
        rec("tests/engines/test_brand_new.py::test_z[pg_lake]", status=None),
    ]
    monkeypatch.setattr(reporting, "_records", {r["nodeid"]: r for r in records})
    assert reporting.unclassified() == [
        "tests/engines/test_brand_new.py::test_x[pg_lake]: no area (reporting.DIMENSIONS)"
    ]
