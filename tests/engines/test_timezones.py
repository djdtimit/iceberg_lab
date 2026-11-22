"""Session time zones (base test, every SQL engine): a non-UTC session must not shift values.

Engines interpret timestamp literals in their session time zone. Two rules follow from the
Iceberg types, and both are classic sources of values silently shifted by hours:

``timestamptz`` (instant)
    a literal without offset is local time of the session; the stored instant is that local
    time converted to UTC. Every reader (UTC session) must return that instant.
``timestamp`` (no zone)
    the value has no zone at all; written in any session zone it must come back unchanged.

Each SQL engine writes one value of each kind while its session zone is set to one of
:data:`ZONES` - an offset that is no whole hour (Kathmandu, +05:45), a local time right after
the daylight-saving switch (Berlin, 2026-03-29), and a date before 1970 (Los Angeles, July 1969:
negative epoch micros). The session zone is checked after setting it and always set back to
UTC. Then every engine reads the table.

``test_tz_write[<writer>-<kind>-<zone>]``            the write in the non-UTC session
``test_tz_read[<writer>-<reader>-<kind>-<zone>]``    every reader returns the right value

PyIceberg, Polars and Daft have no session time zone (they take Arrow values with an explicit
zone), so as writers they are not offered; they read like every other engine. pg_lake writes
only tables it created itself, so it creates its table through Postgres.
"""

from __future__ import annotations

import datetime as dt
import uuid

import pytest
import test_operations as base

from iceberg_lab.datatypes import TypeCase, canonical, normalize
from iceberg_lab.engines import pyiceberg_catalog

from conftest import (
    ENGINES,
    engine_param,
    expect_gap,
    fresh,
    offers,
    require,
    skip_if_not_produced,
)

WAREHOUSE = base.WAREHOUSE
UTC = dt.UTC

#: (zone, local time written in that zone, the same instant in UTC).
ZONES = {
    "kathmandu": (
        "Asia/Kathmandu",
        "2026-03-29 01:30:00.123456",
        dt.datetime(2026, 3, 28, 19, 45, 0, 123456),
    ),
    "berlin_dst": (
        "Europe/Berlin",
        "2026-03-29 03:30:00.123456",
        dt.datetime(2026, 3, 29, 1, 30, 0, 123456),
    ),
    "la_1969": (
        "America/Los_Angeles",
        "1969-07-20 13:17:40.123456",
        dt.datetime(1969, 7, 20, 20, 17, 40, 123456),
    ),
}
KINDS = ("tz", "ntz")

#: Literal per dialect: ``tz`` = instant from local session time, ``ntz`` = plain timestamp.
LITERALS = {
    "tz": {
        "duckdb": "TIMESTAMPTZ '{L}'",
        "spark": "TIMESTAMP_LTZ '{L}'",
        "trino": "CAST(TIMESTAMP '{L}' AS TIMESTAMP(6) WITH TIME ZONE)",
        "flink": "CAST(TIMESTAMP '{L}' AS TIMESTAMP_LTZ(6))",
        "pg_lake": "TIMESTAMPTZ '{L}'",
    },
    "ntz": {
        "duckdb": "TIMESTAMP '{L}'",
        "spark": "TIMESTAMP_NTZ '{L}'",
        "trino": "TIMESTAMP '{L}'",
        "flink": "TIMESTAMP '{L}'",
        "pg_lake": "TIMESTAMP '{L}'",
    },
}

#: Known gaps: (writer, reader, kind, zone) globs -> (regex, reason). Empty until a run shows one.
KNOWN_GAPS: list[tuple[str, str, str, str, str, str]] = []


@pytest.fixture(scope="module")
def tz_results():
    """Module cache ``(writer, kind, zone) -> (namespace, table) | None``.

    Returns:
        An empty dict.
    """
    return {}


def _case(kind: str, zone: str, dialect: str | None) -> TypeCase:
    """The type case of one cell: Iceberg type, expected value and the writer's literal.

    Args:
        kind: ``"tz"`` or ``"ntz"``.
        zone: Key of :data:`ZONES`.
        dialect: SQL dialect of the writer (None for a pure reader).

    Returns:
        The type case.
    """
    from pyiceberg.types import TimestampType, TimestamptzType

    _, local, utc = ZONES[zone]
    if kind == "tz":
        iceberg, value = TimestamptzType(), utc.replace(tzinfo=UTC)
    else:
        iceberg, value = TimestampType(), dt.datetime.fromisoformat(local)
    sql = {dialect: LITERALS[kind][dialect].format(L=local)} if dialect in LITERALS[kind] else {}
    return TypeCase(f"{kind}_{zone}", iceberg, value, sql)


def _gap(writer: str, reader: str, kind: str, zone: str):
    """``expect_gap`` context for one cell (first matching entry of :data:`KNOWN_GAPS`).

    Args:
        writer: Engine that wrote.
        reader: Engine that reads ("-" for the write itself).
        kind: ``"tz"`` or ``"ntz"``.
        zone: Key of :data:`ZONES`.

    Returns:
        The context manager.
    """
    import fnmatch

    for w, r, k, z, match, reason in KNOWN_GAPS:
        if all(fnmatch.fnmatch(a, b) for a, b in ((writer, w), (reader, r), (kind, k), (zone, z))):
            return expect_gap(reason, match)
    return expect_gap(None, None)


def _write(writer: str, kind: str, zone: str, namespace: str, engine_pool) -> tuple[str, str]:
    """Create the table and write ``(1, value)``, ``(2, NULL)`` in the non-UTC session.

    Args:
        writer: Engine that writes.
        kind: ``"tz"`` or ``"ntz"``.
        zone: Key of :data:`ZONES`.
        namespace: Namespace of the table.
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        ``(namespace, table)``.
    """
    engine = engine_pool(writer)
    case = _case(kind, zone, engine.dialect)
    table = f"t_tz_{writer}_{case.name}_{uuid.uuid4().hex[:6]}"
    if not offers(writer, "writing tables it did not create"):  # it creates its own (pg_lake)
        pg_type = "timestamptz" if kind == "tz" else "timestamp"
        engine.create_owned_table(namespace, table, f"id bigint, v {pg_type}")
    else:
        pyiceberg_catalog(WAREHOUSE).create_table(
            f"{namespace}.{table}", schema=case.schema(), properties={"format-version": "2"}
        )
    engine.set_session_zone(ZONES[zone][0])
    try:
        engine.insert_case(namespace, table, case)
    finally:
        engine.set_session_zone("UTC")
    return namespace, table


@pytest.mark.parametrize("zone", list(ZONES))
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_tz_write(writer, kind, zone, namespace, engine_pool, tz_results):
    """The writer stores a timestamp written while its session zone is not UTC.

    Checks:
        * the session zone can be set (and is set back to UTC afterwards);
        * the insert works. The value itself is checked by every reader (test_tz_read).

    Args:
        writer: Engine that writes.
        kind: ``tz`` (timestamptz) or ``ntz`` (timestamp).
        zone: Key of :data:`ZONES`.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters (conftest fixture).
        tz_results: Module cache of the written tables.
    """
    key = (writer, kind, zone)
    tz_results[key] = None
    require(writer, "a session time zone", engine_pool)
    with _gap(writer, "-", kind, zone):
        tz_results[key] = _write(writer, kind, zone, namespace, engine_pool)


@pytest.mark.parametrize("zone", list(ZONES))
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_tz_read(writer, reader, kind, zone, engine_pool, tz_results):
    """Every reader returns the value as the session zone of the writer defined it.

    Checks:
        * ``tz``: the instant is the writer's local session time converted to UTC;
        * ``ntz``: the plain timestamp is exactly the literal, not shifted by any zone;
        * row 2 stays NULL.
        Skipped (blocked) if the writer could not write it.

    Args:
        writer: Engine that wrote.
        reader: Engine that reads (in a UTC session).
        kind: ``tz`` (timestamptz) or ``ntz`` (timestamp).
        zone: Key of :data:`ZONES`.
        engine_pool: Session pool of engine adapters (conftest fixture).
        tz_results: Module cache of the written tables.
    """
    key = (writer, kind, zone)
    require(writer, "a session time zone", engine_pool)  # not offered: no cell to read
    ns, table = skip_if_not_produced(tz_results, key, "test_tz_write")
    case = _case(kind, zone, None)
    with _gap(writer, reader, kind, zone):
        got = [
            (i, normalize(case, v))
            for i, v in fresh(engine_pool(reader), ns, table).select(ns, table, case)
        ]
        assert got == [(1, canonical(case)), (2, None)]
