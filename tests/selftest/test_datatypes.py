"""Self-tests of the value and type comparison across engines (``iceberg_lab.datatypes``).

``normalize`` removes only differences of representation (how a client hands over a value);
a different value must still differ. Too lenient and an interop test passes that should fail,
too strict and an engine is blamed for its client.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

import pandas as pd
import pytest

from iceberg_lab import datatypes as d

C = d.CASES_BY_NAME


@pytest.mark.parametrize("case", d.ALL_CASES, ids=lambda c: c.name)
def test_canonical_value_normalizes_to_itself(case):
    """The value the lab writes is what every reader must produce.

    Checks:
        * ``normalize(case, case.value) == canonical(case)``;
        * the case has a type family (``expected_family`` does not raise).

    Args:
        case: Each type case of the matrix (v2 and v3).
    """
    assert d.normalize(case, case.value) == d.canonical(case)
    assert d.expected_family(case)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        # Trino: "... UTC" suffix; pandas would drop the nanoseconds without the rewrite.
        ("timestamptz_ns", "2026-11-13 13:45:30.123456789 UTC"),
        ("timestamp_ns", pd.Timestamp("2026-11-13 13:45:30.123456789")),
        ("timestamp_ns", "2026-11-13 13:45:30.123456789"),
        ("uuid", str(d.UUID_VALUE)),
        ("uuid", d.UUID_VALUE.bytes),
        ("binary", bytearray(C["binary"].value)),
        ("decimal_10_2", str(C["decimal_10_2"].value)),
        ("list_int", tuple(C["list_int"].value)),
        ("map_string_int", list(C["map_string_int"].value.items())),
        ("map_string_int", [{"key": k, "value": v} for k, v in C["map_string_int"].value.items()]),
        ("map_string_int", [{"key": k, "val": v} for k, v in C["map_string_int"].value.items()]),
        ("struct", tuple(C["struct"].value.values())),
        ("double_nan", "NaN"),
        ("double_neg_zero", -0.0),
    ],
)
def test_client_representations_match(name, value):
    """What the engines' clients hand over normalizes to the canonical value.

    Checks:
        * each client form (text, bytes, tuples, Polars/pg_lake map entries, positional
          structs, NaN as text) equals :func:`canonical`.

    Args:
        name: Type case.
        value: The value as one client returns it.
    """
    case = C[name]
    assert d.normalize(case, value) == d.canonical(case)


def test_timestamptz_in_any_zone_is_the_same_instant():
    """A zoned timestamp compares by instant, whatever zone the client attached.

    Checks:
        * the value in UTC+2 and as pandas Timestamp equal the canonical naive UTC value;
        * one microsecond later does not.
    """
    case = C["timestamptz"]
    v = case.value.astimezone(dt.timezone(dt.timedelta(hours=2)))
    assert d.normalize(case, v) == d.canonical(case)
    assert d.normalize(case, pd.Timestamp(v)) == d.canonical(case)
    assert d.normalize(case, v + dt.timedelta(microseconds=1)) != d.canonical(case)


@pytest.mark.parametrize(
    ("name", "wrong"),
    [
        ("timestamp_ns", pd.Timestamp("2026-11-13 13:45:30.123456")),  # nanoseconds lost
        ("double_neg_zero", 0.0),  # sign of zero lost
        ("double_nan", 0.0),
        ("list_int", []),  # a NULL list must not pass as empty
        ("decimal_10_2", Decimal(0)),
        ("uuid", uuid.UUID(int=0)),
        ("struct", (None,) * len(C["struct"].value)),
    ],
)
def test_real_differences_stay(name, wrong):
    """Normalizing never makes a different value pass.

    Checks:
        * truncated nanoseconds, a lost sign of zero, NaN vs 0, an empty list, another
          decimal, uuid or struct do not equal the canonical value.

    Args:
        name: Type case.
        wrong: A value that differs in content.
    """
    case = C[name]
    assert d.normalize(case, wrong) != d.canonical(case)
    assert d.normalize(case, None) is None


def test_geometry_wkb_and_wkt_compare_equal():
    """POINT in WKB of either byte order and WKT with any spacing are the same text.

    Checks:
        * little- and big-endian WKB of POINT(1 2) and ``POINT(1 2)`` give ``POINT (1 2)``;
        * another shape stays hex (fails visibly instead of being guessed).
    """
    import struct

    big = b"\x00" + struct.pack(">I", 1) + struct.pack(">dd", 1.0, 2.0)
    case = C["geometry"]
    for v in (d.POINT_WKB, big, "POINT(1 2)", "POINT  (1   2)"):
        assert d.normalize(case, v) == "POINT (1 2)"
    line = bytes.fromhex("010200000000000000")
    assert d.normalize(case, line) == line.hex()


@pytest.mark.parametrize(
    ("engine", "name", "family"),
    [
        ("spark", "timestamp", "timestamptz"),  # Spark's TIMESTAMP is the zoned one
        ("spark", "timestamp_ntz", "timestamp"),
        ("trino", "timestamp(6) with time zone", "timestamptz"),
        ("trino", "timestamp(9)", "timestamp_ns"),
        ("polars", "Datetime(time_unit='us', time_zone='UTC')", "timestamptz"),
        ("polars", "Datetime(time_unit='ns', time_zone=None)", "timestamp_ns"),
        ("flink", "TIMESTAMP_LTZ(9)", "timestamptz_ns"),
        ("flink", "BINARY(16) NOT NULL", "fixed(16)"),
        ("pyiceberg", "decimal128(10, 2)", "decimal(10,2)"),
        ("daft", "Decimal", "decimal(?)"),
        ("trino", "unknown", "unknown"),
        ("duckdb", "GEOMETRY('OGC:CRS84')", "geometry"),
        ("trino", "SphericalGeography", "geography"),
        ("pg_lake", "integer[]", "list"),
        ("trino", "row(a integer, b varchar)", "struct"),
        ("duckdb", "HUGEINT", "other:hugeint"),
    ],
)
def test_type_family(engine, name, family):
    """Engine type names map to the Iceberg family; an unknown name fails visibly.

    Checks:
        * each engine-specific spelling gives its family, an unknown one ``other:<name>``.

    Args:
        engine: Engine name.
        name: Type name as the engine reports it.
        family: Expected family.
    """
    assert d.type_family(engine, name) == family


def test_same_family_accepts_hidden_decimal_parameters_only():
    """``decimal(?)`` matches any decimal; nothing else is loosened.

    Checks:
        * ``decimal(?)`` vs ``decimal(10,2)`` matches; ``decimal(38,10)`` vs ``decimal(10,2)``
          and ``int64`` vs ``int32`` do not.
    """
    assert d.same_family("decimal(10,2)", "decimal(?)")
    assert not d.same_family("decimal(10,2)", "decimal(38,10)")
    assert not d.same_family("int32", "int64")
