"""Catalog of all Iceberg data types (format v2 and v3) with one test value each.

Used by ``tests/engines/test_types.py`` to build the matrix *type x writer engine x reader engine*.

Every :class:`TypeCase` knows

* the Iceberg type. Test tables are always created by PyIceberg with exactly this type (or
  by Spark for ``variant``, which PyIceberg cannot model), so the *table* type is never
  influenced by an engine's DDL type mapping - only the engine's write path is tested;
* one canonical Python value that every reader must return after :func:`normalize`;
* how each SQL engine writes the value (``sql``: a literal per dialect, ``None`` when the
  dialect has no such type) - DataFrame engines write :meth:`TypeCase.arrow` instead.

Test values are chosen to hit edge cases: type limits (max int/long), values that are exact in
binary floating point (so float equality is meaningful), maximum decimal precision, sub-second
time precision, non-ASCII and quote characters, non-printable bytes, and NULLs inside nested
types. Every table additionally gets a row with a top-level NULL.

Why :func:`normalize` exists: each client library returns the "same" value differently -
Trino returns ``NamedRowTuple`` for structs, Spark ``Row``; Arrow returns maps as lists of
pairs; UUIDs arrive as ``UUID``, ``str`` or 16 raw bytes; timestamps with or without tzinfo.
Normalisation removes *representation* differences only. A real difference in the *value*
(lost nanoseconds, wrong NULL handling, truncated decimals) must still fail.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pandas as pd
import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    GeographyType,
    GeometryType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampNanoType,
    TimestampType,
    TimestamptzNanoType,
    TimestamptzType,
    TimeType,
    UnknownType,
    UUIDType,
)

UTC = dt.UTC
UUID_VALUE = uuid.UUID("6f1c2a7e-3b4d-4e5f-9a8b-1c2d3e4f5a6b")
# Microsecond precision on purpose: Iceberg time/timestamp are microsecond types, so the
# full sub-second part must survive every engine.
TS = dt.datetime(2026, 11, 13, 13, 45, 30, 123456)


@dataclass(frozen=True)
class TypeCase:
    """One Iceberg data type plus everything needed to write and verify one value of it.

    Attributes:
        name: Short id used in test ids (``test_read[duckdb-trino-v2-<name>]``).
        iceberg: The exact Iceberg column type. ``None`` means PyIceberg cannot model the type
            (``variant``); the table is then created by Spark using :attr:`spark_ddl`.
        value: The canonical value; :func:`canonical` derives what readers must return.
        sql: SQL literal per dialect (``"duckdb"``, ``"spark"``, ``"trino"``). ``None`` or a
            missing key means the dialect cannot express the type -> the write test *skips*.
        min_format_version: First Iceberg format version that has this type (3 for v3 types).
        spark_ddl: Spark column type for ``CREATE TABLE`` when :attr:`iceberg` is ``None``.
        arrow_writable: ``False`` when DataFrame engines cannot build the value from Arrow.
    """

    name: str
    iceberg: IcebergType | None
    value: Any
    sql: dict[str, str | None] = field(default_factory=dict)
    min_format_version: int = 1
    spark_ddl: str | None = None
    arrow_writable: bool = True

    def read_expr(self, dialect: str | None) -> str:
        """SELECT expression for column ``v`` in the given SQL dialect.

        Nanosecond timestamps are cast to text: the Python clients of DuckDB, Trino and Spark
        convert timestamps to ``datetime``, which only has microseconds, so the nanoseconds
        would be lost on the *client* side and the test would blame the engine wrongly.
        DataFrame engines (``dialect=None``) go through Arrow, which keeps nanoseconds.

        Args:
            dialect: SQL dialect of the reading engine (None: plain ``v``).

        Returns:
            The SELECT expression.
        """
        if isinstance(self.iceberg, (TimestampNanoType, TimestamptzNanoType)) and dialect:
            return {"spark": "CAST(v AS STRING)", "flink": "CAST(v AS STRING)"}.get(
                dialect, "CAST(v AS VARCHAR)"
            )
        if dialect == "flink" and isinstance(self.iceberg, DecimalType):
            # Flink's JSON result format sends decimals as JSON numbers (float precision).
            return "CAST(v AS STRING)"
        if dialect == "pg_lake" and _has_struct_or_map(self.iceberg):
            # psycopg returns Postgres composite/map values as text like '(1,x)'; JSON keeps
            # the structure (maps become [{"key": .., "val": ..}]).
            return "to_jsonb(v)"
        return "v"

    def schema(self) -> Schema:
        """Iceberg schema of the test table: ``id BIGINT, v <type>``, both optional.

        Returns:
            The Iceberg schema.
        """
        return Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "v", self.iceberg, required=False),
        )

    def arrow(self) -> pa.Table:
        """Test data for DataFrame engines: ``id=1`` holds the value, ``id=2`` holds NULL.

        Returns:
            Two-row Arrow table (value, NULL).
        """
        # Build the Arrow schema WITHOUT field-id metadata: the catalog re-assigns the ids of
        # nested fields on create_table (e.g. list element 20 -> 3), and PyIceberg would match
        # the stale ids from the metadata instead of the names ("Could not find field id").
        arrow_schema = schema_to_pyarrow(self.schema(), include_field_ids=False)
        return pa.Table.from_pylist(
            [{"id": 1, "v": self._arrow_value()}, {"id": 2, "v": None}], schema=arrow_schema
        )

    def _arrow_value(self):
        """Convert :attr:`value` into what ``pa.Table.from_pylist`` expects for the type.

        Returns:
            The value in Arrow's Python form.
        """
        if isinstance(self.iceberg, (GeometryType, GeographyType)):
            return self.value  # already WKB bytes (Arrow binary)
        if isinstance(self.iceberg, UUIDType):
            return self.value.bytes  # Iceberg uuid = Arrow fixed_size_binary(16)
        if isinstance(self.iceberg, MapType):
            return list(self.value.items())  # Arrow maps are lists of (key, value) pairs
        return self.value


def _has_struct_or_map(t) -> bool:
    """True for struct/map types and lists of them (types psycopg cannot decode natively).

    Args:
        t: Iceberg type.

    Returns:
        Whether the type is (a list of) struct or map.
    """
    if isinstance(t, (StructType, MapType)):
        return True
    return isinstance(t, ListType) and isinstance(t.element_type, (StructType, MapType))


def _struct(**fields):
    """Shorthand for an optional-fields StructType: ``_struct(a=IntegerType(), b=...)``.

    Field ids start at 10 to stay clear of the top-level ids 1/2; the catalog re-numbers
    them anyway when the table is created.

    Args:
        fields: Field name -> Iceberg type, in order.

    Returns:
        The StructType.
    """
    return StructType(
        *(NestedField(i + 10, k, t, required=False) for i, (k, t) in enumerate(fields.items()))
    )


# =========================================================================== format v1/v2 types
# Comments on individual cases explain why a value was chosen or why a dialect has no literal.


CASES: list[TypeCase] = [
    TypeCase(
        "boolean",
        BooleanType(),
        True,
        {"duckdb": "true", "spark": "true", "trino": "true"},
    ),
    TypeCase(
        "int",
        IntegerType(),
        2_147_483_647,  # INT32 max: catches engines that silently widen or overflow
        {"duckdb": "2147483647::INTEGER", "spark": "2147483647", "trino": "2147483647"},
    ),
    TypeCase(
        "long",
        LongType(),
        9_223_372_036_854_775_807,  # INT64 max: would be corrupted by any float conversion
        {
            "duckdb": "9223372036854775807::BIGINT",
            "spark": "9223372036854775807L",
            "trino": "BIGINT '9223372036854775807'",
        },
    ),
    TypeCase(
        "float",
        FloatType(),
        1.5,  # exactly representable in float32, so equality after widening is meaningful
        {"duckdb": "1.5::FLOAT", "spark": "CAST(1.5 AS FLOAT)", "trino": "REAL '1.5'"},
    ),
    TypeCase(
        "double",
        DoubleType(),
        3.141592653589793,
        {
            "duckdb": "3.141592653589793::DOUBLE",
            "spark": "CAST(3.141592653589793 AS DOUBLE)",
            "trino": "DOUBLE '3.141592653589793'",
        },
    ),
    # Special floating-point values: Iceberg's metrics treat NaN apart (nan_value_counts, never in
    # lower/upper bounds), and -0.0 must keep its sign. Compared via _float_key: NaN != NaN.
    TypeCase(
        "double_nan",
        DoubleType(),
        float("nan"),
        {"duckdb": "'NaN'::DOUBLE", "spark": "CAST('NaN' AS DOUBLE)", "trino": "nan()"},
    ),
    TypeCase(
        "double_inf",
        DoubleType(),
        float("inf"),
        {
            "duckdb": "'Infinity'::DOUBLE",
            "spark": "CAST('Infinity' AS DOUBLE)",
            "trino": "infinity()",
        },
    ),
    TypeCase(
        "double_neg_zero",
        DoubleType(),
        -0.0,
        {"duckdb": "'-0.0'::DOUBLE", "spark": "CAST('-0.0' AS DOUBLE)", "trino": "DOUBLE '-0.0'"},
    ),
    TypeCase(
        "float_nan",
        FloatType(),
        float("nan"),
        {"duckdb": "'NaN'::FLOAT", "spark": "CAST('NaN' AS FLOAT)", "trino": "CAST(nan() AS REAL)"},
    ),
    TypeCase(
        "decimal_10_2",
        DecimalType(10, 2),
        Decimal("12345678.90"),
        {
            "duckdb": "12345678.90::DECIMAL(10,2)",
            "spark": "CAST('12345678.90' AS DECIMAL(10,2))",
            "trino": "DECIMAL '12345678.90'",
        },
    ),
    TypeCase(
        "decimal_38_10",
        DecimalType(38, 10),
        # All 38 digits used: needs a 16-byte decimal, breaks engines that go through double.
        Decimal("1234567890123456789012345678.0123456789"),
        {
            "duckdb": "'1234567890123456789012345678.0123456789'::DECIMAL(38,10)",
            "spark": "CAST('1234567890123456789012345678.0123456789' AS DECIMAL(38,10))",
            "trino": "DECIMAL '1234567890123456789012345678.0123456789'",
        },
    ),
    TypeCase(
        "date",
        DateType(),
        dt.date(2026, 11, 13),
        {"duckdb": "DATE '2026-11-13'", "spark": "DATE '2026-11-13'", "trino": "DATE '2026-11-13'"},
    ),
    TypeCase(
        "time",
        TimeType(),
        dt.time(13, 45, 30, 123456),
        {
            "duckdb": "TIME '13:45:30.123456'",
            "spark": None,  # Spark 4.1 has no TIME type
            "trino": "TIME '13:45:30.123456'",
        },
    ),
    TypeCase(
        "timestamp",
        TimestampType(),
        TS,
        {
            "duckdb": "TIMESTAMP '2026-11-13 13:45:30.123456'",
            "spark": "TIMESTAMP_NTZ '2026-11-13 13:45:30.123456'",
            "trino": "TIMESTAMP '2026-11-13 13:45:30.123456'",
        },
    ),
    TypeCase(
        "timestamptz",
        TimestamptzType(),
        TS.replace(tzinfo=UTC),
        {
            "duckdb": "TIMESTAMPTZ '2026-11-13 13:45:30.123456+00'",
            "spark": "TIMESTAMP '2026-11-13 13:45:30.123456 UTC'",
            "trino": "TIMESTAMP '2026-11-13 13:45:30.123456 UTC'",
        },
    ),
    TypeCase(
        "string",
        StringType(),
        'Grüße 🧊 "quoted"',  # 2-byte and 4-byte UTF-8 plus double quotes
        {
            "duckdb": "'Grüße 🧊 \"quoted\"'",
            "spark": "'Grüße 🧊 \"quoted\"'",
            "trino": "'Grüße 🧊 \"quoted\"'",
        },
    ),
    TypeCase(
        "uuid",
        UUIDType(),
        UUID_VALUE,
        {
            "duckdb": f"'{UUID_VALUE}'::UUID",
            "spark": f"'{UUID_VALUE}'",  # Spark maps Iceberg uuid to string
            "trino": f"UUID '{UUID_VALUE}'",
        },
    ),
    TypeCase(
        "fixed_4",
        FixedType(4),
        b"\x00\x01\x02\x03",  # starts with a NUL byte: catches C-string handling
        {
            # DuckDB has no fixed-size binary type; BLOB is the closest it can write.
            "duckdb": "'\\x00\\x01\\x02\\x03'::BLOB",
            "spark": "X'00010203'",
            "trino": "X'00010203'",
        },
    ),
    TypeCase(
        "binary",
        BinaryType(),
        b"\x00\xffbin",
        {
            "duckdb": "'\\x00\\xFFbin'::BLOB",
            "spark": "X'00FF62696E'",
            "trino": "X'00FF62696E'",
        },
    ),
    TypeCase(
        "list_int",
        ListType(element_id=20, element_type=IntegerType(), element_required=False),
        [1, None, 3],  # NULL *inside* the list, distinct from the NULL list in row 2
        {
            "duckdb": "[1, NULL, 3]::INTEGER[]",
            "spark": "array(1, NULL, 3)",
            "trino": "ARRAY[1, NULL, 3]",
        },
    ),
    TypeCase(
        "map_string_int",
        MapType(
            key_id=21,
            key_type=StringType(),
            value_id=22,
            value_type=IntegerType(),
            value_required=False,
        ),
        {"a": 1, "b": 2},
        {
            "duckdb": "MAP {'a': 1, 'b': 2}",
            "spark": "map('a', 1, 'b', 2)",
            "trino": "MAP(ARRAY['a', 'b'], ARRAY[1, 2])",
        },
    ),
    TypeCase(
        "struct",
        _struct(a=IntegerType(), b=StringType()),
        {"a": 1, "b": "x"},
        {
            "duckdb": "{'a': 1, 'b': 'x'}",
            "spark": "named_struct('a', 1, 'b', 'x')",
            "trino": "CAST(ROW(1, 'x') AS ROW(a INTEGER, b VARCHAR))",
        },
    ),
    TypeCase(
        "list_of_struct",
        ListType(
            element_id=23,
            element_type=_struct(a=IntegerType(), b=StringType()),
            element_required=False,
        ),
        # NULL inside a struct inside a list; row 2 holds a NULL list. PyIceberg 0.12 turns
        # that NULL list into [] (see KNOWN_GAPS in tests/engines/test_types.py).
        [{"a": 1, "b": "x"}, {"a": 2, "b": None}],
        {
            "duckdb": "[{'a': 1, 'b': 'x'}, {'a': 2, 'b': NULL}]",
            "spark": "array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', CAST(NULL AS STRING)))",
            "trino": "ARRAY[CAST(ROW(1, 'x') AS ROW(a INTEGER, b VARCHAR)), CAST(ROW(2, NULL) AS ROW(a INTEGER, b VARCHAR))]",
        },
    ),
]

# =========================================================================== format v3 only
# These types only exist in Iceberg format v3. Tables using them must be created with
# format-version 3; on v2 tables they are rejected (tests/engines/test_types.py checks that).

# pandas.Timestamp instead of datetime: Python's datetime cannot hold nanoseconds.
TS_NS = pd.Timestamp("2026-11-13 13:45:30.123456789")
# Well-known binary (WKB) of POINT(1 2): byte order, geometry type 1, then x and y as doubles.
POINT_WKB = bytes.fromhex("0101000000000000000000f03f0000000000000040")

V3_CASES: list[TypeCase] = [
    TypeCase(
        "timestamp_ns",
        TimestampNanoType(),
        TS_NS,
        {
            "duckdb": "TIMESTAMP_NS '2026-11-13 13:45:30.123456789'",
            "spark": None,  # Spark has no nanosecond timestamps
            "trino": "TIMESTAMP '2026-11-13 13:45:30.123456789'",
        },
        min_format_version=3,
    ),
    TypeCase(
        "timestamptz_ns",
        TimestamptzNanoType(),
        TS_NS.tz_localize("UTC"),
        {
            "duckdb": None,  # DuckDB has no nanosecond TIMESTAMPTZ
            "spark": None,
            "trino": "TIMESTAMP '2026-11-13 13:45:30.123456789 UTC'",
        },
        min_format_version=3,
    ),
    TypeCase(
        # "unknown" can only ever hold NULL; it is the type of a column whose type is not
        # known yet (e.g. a JSON field that has only been null so far).
        "unknown",
        UnknownType(),
        None,
        {"duckdb": "NULL", "spark": "NULL", "trino": "NULL"},
        min_format_version=3,
    ),
    TypeCase(
        "geometry",
        GeometryType(),
        POINT_WKB,
        {
            "duckdb": None,  # needs the spatial extension's GEOMETRY type
            "spark": None,  # Spark 4.1 has no geometry type
            "trino": "ST_Point(1, 2)",
        },
        min_format_version=3,
    ),
    TypeCase(
        "geography",
        GeographyType(),
        POINT_WKB,
        {"duckdb": None, "spark": None, "trino": None},
        min_format_version=3,
    ),
    TypeCase(
        # Semi-structured value (like JSON, but binary-encoded with typed leaves).
        # PyIceberg 0.12 has no VariantType, so iceberg=None and Spark creates the table.
        "variant",
        None,
        {"a": 1, "b": [True, None, "x"]},
        {
            "duckdb": None,
            "spark": 'parse_json(\'{"a":1,"b":[true,null,"x"]}\')',
            # Trino 484 has a VARIANT type (found by tests/selftest/test_na_claims.py).
            "trino": 'CAST(JSON \'{"a":1,"b":[true,null,"x"]}\' AS VARIANT)',
        },
        min_format_version=3,
        spark_ddl="VARIANT",
        arrow_writable=False,
    ),
]

ALL_CASES = CASES + V3_CASES
CASES_BY_NAME = {c.name: c for c in ALL_CASES}

# Flink SQL literals, kept as one table (Flink was added later). Notes:
# * Iceberg uuid and fixed(n) are BINARY in Flink; ARRAY/ROW elements need explicit NULL types.
# * TIMESTAMP_LTZ literals are interpreted in the session time zone, which the adapter pins
#   to UTC.  * None: no Flink type for it (yet).
FLINK_SQL: dict[str, str | None] = {
    "boolean": "TRUE",
    "int": "2147483647",
    "long": "CAST(9223372036854775807 AS BIGINT)",
    "float": "CAST(1.5 AS FLOAT)",
    "double": "CAST(3.141592653589793 AS DOUBLE)",
    "double_nan": "CAST('NaN' AS DOUBLE)",
    "double_inf": "CAST('Infinity' AS DOUBLE)",
    "double_neg_zero": "CAST('-0.0' AS DOUBLE)",
    "float_nan": "CAST('NaN' AS FLOAT)",
    "decimal_10_2": "CAST('12345678.90' AS DECIMAL(10, 2))",
    "decimal_38_10": "CAST('1234567890123456789012345678.0123456789' AS DECIMAL(38, 10))",
    "date": "DATE '2026-11-13'",
    "time": "TIME '13:45:30.123456'",
    "timestamp": "TIMESTAMP '2026-11-13 13:45:30.123456'",
    "timestamptz": "CAST(TIMESTAMP '2026-11-13 13:45:30.123456' AS TIMESTAMP_LTZ(6))",
    "string": "'Grüße 🧊 \"quoted\"'",
    "uuid": f"X'{UUID_VALUE.hex}'",
    "fixed_4": "X'00010203'",
    "binary": "X'00FF62696E'",
    "list_int": "ARRAY[1, CAST(NULL AS INT), 3]",
    "map_string_int": "MAP['a', 1, 'b', 2]",
    "struct": "CAST(ROW(1, 'x') AS ROW<a INT, b STRING>)",
    "list_of_struct": (
        "ARRAY[CAST(ROW(1, 'x') AS ROW<a INT, b STRING>), "
        "CAST(ROW(2, CAST(NULL AS STRING)) AS ROW<a INT, b STRING>)]"
    ),
    "timestamp_ns": "TIMESTAMP '2026-11-13 13:45:30.123456789'",
    "timestamptz_ns": "CAST(TIMESTAMP '2026-11-13 13:45:30.123456789' AS TIMESTAMP_LTZ(9))",
    "unknown": "NULL",
    "geometry": None,
    "geography": None,
    "variant": 'PARSE_JSON(\'{"a":1,"b":[true,null,"x"]}\')',
}
for _case in ALL_CASES:
    _case.sql.setdefault("flink", FLINK_SQL.get(_case.name))


def canonical(case: TypeCase):
    """The value every reader must produce for row ``id=1`` after :func:`normalize`.

    Usually ``case.value`` itself; two exceptions mirror the normalisation rules:

    * nanosecond timestamps -> ISO string (``datetime`` cannot hold nanoseconds);
    * timestamptz -> naive UTC ``datetime`` (engines differ in which zone they attach).

    Args:
        case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).

    Returns:
        The expected normalised value.
    """
    if isinstance(case.value, pd.Timestamp):
        v = case.value
        return (v.tz_convert("UTC").tz_localize(None) if v.tzinfo else v).isoformat()
    if isinstance(case.value, dt.datetime) and case.value.tzinfo:
        return case.value.astimezone(UTC).replace(tzinfo=None)
    if isinstance(case.iceberg, (GeometryType, GeographyType)):
        return _geo_text(case.value)
    if isinstance(case.iceberg, (FloatType, DoubleType)):
        return _float_key(case.value)
    return case.value


def _float_key(x) -> float | str:
    """A float in a form ``==`` can compare: NaN is never equal to itself and ``-0.0 == 0.0``,
    so both become text; every other value stays a float.

    Args:
        x: The value (float, numpy float, or text such as "NaN" from a JSON-based client).

    Returns:
        ``"NaN"``, ``"-0.0"`` or the float.
    """
    import math

    f = float(x)
    if math.isnan(f):
        return "NaN"
    if f == 0.0 and math.copysign(1.0, f) < 0:
        return "-0.0"
    return f


def _geo_text(v):
    """Geometry as WKT text: WKB bytes (Iceberg's storage form) and engine WKT compare equal.

    Only POINT is decoded (the lab's test value); any other WKB stays as hex, so a different
    shape still fails visibly instead of being guessed.

    Args:
        v: Geometry as WKB bytes, WKT text, or a PySpark ``Geometry``/``Geography`` object
            (Spark 4.1 returns these; ``getBytes()`` is their WKB).

    Returns:
        WKT text (POINT decoded), else hex.
    """
    import struct

    if hasattr(v, "getBytes"):  # pyspark.sql.types.Geometry / Geography
        v = v.getBytes()
    if isinstance(v, str):
        return " ".join(v.replace("(", " (").split()).replace("( ", "(")
    b = bytes(v)
    if len(b) == 21 and b[0] in (0, 1):
        order = "<" if b[0] == 1 else ">"
        (geom_type,) = struct.unpack(order + "I", b[1:5])
        if geom_type == 1:
            x, y = struct.unpack(order + "dd", b[5:21])
            return f"POINT ({x:g} {y:g})"
    return b.hex()


def normalize(case: TypeCase, v):
    """Map one engine-specific Python value to the canonical representation for its type.

    Only *representation* differences are removed; the actual value is preserved, so e.g. a
    truncated nanosecond or a NULL turned into ``[]`` still fails the comparison.

    Args:
        case: The type case, used to decide how to interpret ``v``.
        v: The raw value as returned by the engine's Python client.

    Returns:
        A value comparable with :func:`canonical` via ``==``.
    """
    if v is None:
        return None
    t = case.iceberg
    if isinstance(t, (GeometryType, GeographyType)):  # WKB bytes or WKT text (e.g. Trino)
        return _geo_text(v)
    if t is None:  # variant
        if hasattr(v, "toPython"):  # pyspark VariantVal -> plain dict/list/scalars
            return v.toPython()
        return json.loads(v) if isinstance(v, str) else v  # engines that return JSON text
    if isinstance(t, (TimestampNanoType, TimestamptzNanoType)):
        # Compare as ISO string so nanoseconds survive (python datetime only has micros).
        # Inputs: pandas.Timestamp (Arrow), or text from the CAST in TypeCase.read_expr.
        if isinstance(v, str) and v.endswith(" UTC"):
            # Trino formats "... 13:45:30.123456789 UTC"; pandas parses that suffix with a
            # different parser that silently drops the nanoseconds, so rewrite it as offset.
            v = v[: -len(" UTC")] + "+00:00"
        ts = pd.Timestamp(v)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts.isoformat()
    if isinstance(t, (GeometryType, GeographyType)):
        if hasattr(v, "getBytes"):  # pyspark.sql.types.Geometry / Geography
            return bytes(v.getBytes())
        return bytes(v) if isinstance(v, (bytes, bytearray, memoryview)) else v
    if isinstance(t, UUIDType):
        # Arrow: 16 raw bytes; DuckDB/Trino: uuid.UUID; Spark: str.
        if isinstance(v, (bytes, bytearray)):
            return uuid.UUID(bytes=bytes(v))
        return uuid.UUID(str(v))
    if isinstance(t, (FixedType, BinaryType)):
        return bytes(v)  # Spark returns bytearray
    if isinstance(t, TimestamptzType):
        # Some clients attach the session zone, some UTC, some return pandas.Timestamp.
        if hasattr(v, "to_pydatetime"):
            v = v.to_pydatetime()
        if v.tzinfo is not None:
            v = v.astimezone(UTC)
        return v.replace(tzinfo=None)
    if isinstance(t, TimestampType):
        return v.to_pydatetime() if hasattr(v, "to_pydatetime") else v
    if isinstance(t, MapType):
        if isinstance(v, list) and v and isinstance(v[0], dict):
            # Polars: [{key, value}]; pg_lake (to_jsonb): [{key, val}]
            return {e["key"]: e["value"] if "value" in e else e["val"] for e in v}
        return dict(v)  # dict, or list of (key, value) tuples from Arrow
    if isinstance(t, StructType):
        return _norm_struct(v, t)
    if isinstance(t, ListType):
        el = t.element_type
        if isinstance(el, StructType):
            return [None if x is None else _norm_struct(x, el) for x in v]
        return list(v)  # tuples / numpy arrays -> list
    if isinstance(t, (FloatType, DoubleType)):
        return _float_key(v)  # numpy.float32 -> float; NaN and -0.0 comparable
    if isinstance(t, DecimalType):
        return Decimal(str(v))
    return v


def _norm_struct(v, t: StructType) -> dict:
    """Convert a struct value from any client into ``{field_name: value}``.

    Handles dicts (Arrow, DuckDB), pyspark ``Row`` and positional tuples (Trino's
    ``NamedRowTuple``). The Row check uses the class name on purpose: ``NamedRowTuple``
    answers *every* attribute lookup (``hasattr(v, "asDict")`` is true and returns ``None``).

    Args:
        v: Struct value as a client delivers it.
        t: The struct's Iceberg type.

    Returns:
        ``{field_name: value}``.
    """
    names = [f.name for f in t.fields]
    if isinstance(v, dict):
        return {k: v[k] for k in names}
    if type(v).__name__ == "Row":
        return v.asDict()
    return dict(zip(names, v))


# =========================================================================== type fidelity


def expected_family(case: TypeCase) -> str:
    """Canonical type family a reader should report for ``case`` (see :func:`type_family`).

    Args:
        case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).

    Returns:
        The canonical family name.

    Raises:
        ValueError: the type has no family.

    Raises:
        ValueError: the type has no family.
    """
    t = case.iceberg
    if t is None:
        return "variant"
    simple = [
        (BooleanType, "boolean"),
        (IntegerType, "int32"),
        (LongType, "int64"),
        (FloatType, "float32"),
        (DoubleType, "float64"),
        (DateType, "date"),
        (TimeType, "time"),
        (TimestampNanoType, "timestamp_ns"),
        (TimestamptzNanoType, "timestamptz_ns"),
        (TimestamptzType, "timestamptz"),
        (TimestampType, "timestamp"),
        (StringType, "string"),
        (UUIDType, "uuid"),
        (BinaryType, "binary"),
        (ListType, "list"),
        (MapType, "map"),
        (StructType, "struct"),
        (UnknownType, "unknown"),
        (GeometryType, "geometry"),
        (GeographyType, "geography"),
    ]
    if isinstance(t, DecimalType):
        return f"decimal({t.precision},{t.scale})"
    if isinstance(t, FixedType):
        return f"fixed({len(t)})"
    for cls, fam in simple:
        if isinstance(t, cls):
            return fam
    raise ValueError(f"no family for {t}")


def type_family(engine: str, type_name: str) -> str:
    """Map an engine's own type name to a canonical Iceberg type family.

    The patterns were collected from what each engine actually reports (``typeof()`` in DuckDB,
    Spark and Trino, ``pg_typeof()`` in pg_lake, the Arrow/Polars/Daft schema otherwise), e.g.
    ``timestamp(6) with time zone`` (Trino), ``Datetime(time_unit='us', time_zone='UTC')``
    (Polars), ``timestamp`` (Spark: TIMESTAMP is the *zoned* type, TIMESTAMP_NTZ the naive one).

    Returns a family like ``int64``, ``decimal(10,2)``, ``fixed(4)``, ``timestamptz_ns`` -
    or ``decimal(?)``/``fixed(?)`` when the engine does not expose the parameters, and
    ``other:<name>`` when nothing matches (which then fails the comparison visibly).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        type_name: Type name as the engine reports it.

    Returns:
        The canonical family name (``?`` parts where the engine hides parameters).
    """
    import re

    # Flink: "BINARY(4) NOT NULL", "ROW<`a` INT, ...>", "TIMESTAMP_LTZ(6)".
    s = type_name.strip().lower().replace("`", "").removesuffix(" not null")
    if engine == "flink":
        m = re.fullmatch(r"binary\((\d+)\)", s)
        if m:  # Flink has no UUID type: Iceberg uuid surfaces as BINARY(16) too
            return f"fixed({m.group(1)})"
        if s == "bytes":
            return "binary"
        if s.startswith("timestamp_ltz"):
            return "timestamptz_ns" if s.endswith("(9)") else "timestamptz"
    if "arrow.uuid" in s or s == "uuid":
        return "uuid"
    m = re.search(r"fixed_size_binary\[(\d+)\]|binary\[(\d+)\]|iceberg_fixed\((\d+)\)", s)
    if m:
        return f"fixed({next(g for g in m.groups() if g)})"
    if s in ("null", '"null"', "void") or (engine == "trino" and s == "unknown"):
        return "unknown"  # Trino's null type is called "unknown", like Iceberg's
    if "variant" in s:
        return "variant"
    if s.startswith("geometry"):  # e.g. DuckDB "GEOMETRY('OGC:CRS84')"
        return "geometry"
    if s.startswith("geography") or s == "sphericalgeography":  # Trino: SphericalGeography
        return "geography"
    if s.endswith("[]") or s.startswith(("list", "large_list", "array")):
        return "list"
    if s.startswith(("map", "map_type.")):
        return "map"
    if s.startswith(("struct", "row(", "row<", "lake_struct.")):
        return "struct"
    if s.startswith(("decimal", "numeric")):
        nums = re.findall(r"\d+", s.replace("decimal128", "decimal"))
        return f"decimal({nums[-2]},{nums[-1]})" if len(nums) >= 2 else "decimal(?)"
    if "timestamp" in s or "datetime" in s:
        ns = bool(re.search(r"\bns\b|'ns'|\(9\)|_ns", s))
        tz = (
            "tz=" in s
            or "with time zone" in s
            or bool(re.search(r"time_zone='(?!none)", s))
            or s.endswith("; utc]")
            or s.startswith("timestamptz")
            or (engine == "spark" and s == "timestamp")
        )
        return ("timestamptz" if tz else "timestamp") + ("_ns" if ns else "")
    if s.startswith("time"):
        return "time"
    if s.startswith("date"):
        return "date"
    if s.startswith("bool"):
        return "boolean"
    if s in ("string", "large_string", "utf8", "text") or s.startswith("varchar"):
        return "string"
    if s in ("binary", "large_binary", "blob", "varbinary", "bytea"):
        return "binary"
    if s in ("double", "float64", "float8", "double precision"):
        return "float64"
    if s in ("float", "float32", "real", "float4"):
        return "float32"
    if s in ("int64", "bigint", "long", "int8"):
        return "int64"
    if s in ("int32", "integer", "int", "int4"):
        return "int32"
    return f"other:{s}"


def same_family(expected: str, actual: str) -> bool:
    """Equality, except that ``decimal(?)`` (parameters not exposed) matches any decimal.

    Args:
        expected: Expected family.
        actual: Reported family.

    Returns:
        Whether they match.
    """
    return expected == actual or (actual == "decimal(?)" and expected.startswith("decimal("))
