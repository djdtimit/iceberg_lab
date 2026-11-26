"""Self-tests of the jump warning against the committed reference (``scripts/reference_shifts.py``).

The warning catches lab errors that distort the figures whatever their cause; it counts the way
the dashboard counts, or it would warn about the wrong things.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "reference_shifts.py"


@pytest.fixture(scope="module")
def rs():
    """The script as a module (scripts/ is not a package).

    Returns:
        The loaded module.
    """
    spec = importlib.util.spec_from_file_location("reference_shifts", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _rows(engine, works=0, gaps=0, na=0, foreign=0, theme="Types"):
    """Rows of one engine.

    Args:
        engine: Engine ("" = catalog test).
        works: Passed cases.
        gaps: Gap cases.
        na: Not offered.
        foreign: Cases blocked by another engine.
        theme: Theme of the rows.

    Returns:
        The rows.
    """
    row = {"engine": engine, "theme": theme, "blocked_by": ""}
    return (
        [row | {"status": "pass"}] * works
        + [row | {"status": "gap"}] * gaps
        + [row | {"status": "na"}] * na
        + [row | {"status": "blocked", "blocked_by": "spark"}] * foreign
    )


def test_counts_like_the_dashboard(rs):
    """Not compared rows, own results and cases blocked by another engine are left out.

    Checks:
        * works and offered count pass/gap; "na" is planned, not offered;
        * a case blocked by spark is not counted for duckdb; "Not compared" and "self" rows
          are not counted at all.

    Args:
        rs: The script as a module.
    """
    rows = (
        _rows("duckdb", works=3, gaps=1, na=2, foreign=5)
        + _rows("duckdb", works=7, theme="Not compared")
        + [{"engine": "duckdb", "theme": "Types", "status": "self", "blocked_by": ""}]
    )
    assert rs.tally(rows) == {"duckdb": {"works": 3, "offered": 4, "planned": 6}}


def test_the_pg_lake_case_is_reported(rs):
    """The error that gave pg_lake on RustFS 100 %: a whole engine's offered cases changed.

    Checks:
        * 1 offered case -> 300: reported with the planned count;
        * an unchanged engine and a small change (2 points, 3 cases) -> nothing.

    Args:
        rs: The script as a module.
    """
    old = _rows("pg_lake", works=1, na=1342) + _rows("spark", works=100, gaps=10)
    new = _rows("pg_lake", works=200, gaps=100, na=1043) + _rows("spark", works=98, gaps=12)
    assert rs.shifts(old, new) == [
        "pg_lake: reliability 100% -> 67%, offered 1 -> 300 of 1343 planned"
    ]


def test_appeared_and_disappeared(rs):
    """An engine that is suddenly missing (or new) is reported.

    Checks:
        * cases only in the old state -> "disappeared"; only in the new -> "appeared".

    Args:
        rs: The script as a module.
    """
    assert rs.shifts(_rows("flink", works=5), _rows("trino", works=5)) == [
        "flink: cases disappeared",
        "trino: cases appeared",
    ]
