"""Data type matrix: format version x data type x writer engine x reader engine.

How it works
    For every (format version, type, writer) one table ``(id BIGINT, v <type>)`` is created -
    by PyIceberg, or by Spark for ``variant`` which PyIceberg cannot model. Creating the table
    outside the writer guarantees the exact Iceberg type, so only the writer's *write path*
    is tested, not its DDL type mapping. The writer inserts ``(1, value)`` and ``(2, NULL)``
    (SQL literal or Arrow data, see ``iceberg_lab.datatypes``). Every reader then reads the
    table back; values are normalised and compared with the canonical value.

    ``test_write[<writer>-v<fv>-<type>]``           can the engine write the type?
    ``test_read[<writer>-<reader>-v<fv>-<type>]``   can the reader read it back losslessly?

    ``test_read_neutral[<reader>-v<fv>-<type>]``   reads a table a neutral source wrote (lab
                                                   client, or Iceberg Java core for types the
                                                   lab client cannot write: can the reader read
                                                   the type at all?)
    ``test_reader_type[<reader>-v<fv>-<type>]``    does the reader report the matching type?
    ``test_nan_metrics[<writer>-v<fv>-<type>]``    no NaN in the column bounds, NaN/NULL counts
                                                   right (filters must not prune wrongly)
    ``test_v3_types_rejected_on_v2_tables``         v3-only types are refused on v2 tables

    Writes are cached per session (fixture ``written``), so each table is written once and
    read by all readers.

    Exception pg_lake: it can only write tables it created itself, so as a writer it creates
    the table with a Postgres type (:data:`PG_TYPES`; composite type for structs,
    ``map_type`` for maps) and the test first checks that this yields the expected Iceberg
    type. No Postgres type exists for fixed(n), and pg_lake creates only format v2.

Result semantics
    pass   - works, value is identical after normalisation
    skip   - the engine cannot express the type at all (no SQL type / no Arrow builder), or
             the table could not be written, so there is nothing to read
    FAILED - does not work. Documented cases (:data:`KNOWN_GAPS`) are labelled
             ``[known gap] <reason>``, everything else is a new error. With
             ``KNOWN_GAPS=xfail`` documented cases are reported as XFAIL instead.

Runtime: ~10-15 min with all engines.
"""

from __future__ import annotations

import fnmatch
import json
import re
import uuid
from typing import NamedTuple

import pytest

from iceberg_lab.config import PROFILE, WAREHOUSE_STS
from iceberg_lab.datatypes import (
    ALL_CASES,
    CASES,
    TypeCase,
    canonical,
    expected_family,
    normalize,
    same_family,
    type_family,
)
from iceberg_lab.engines import SPARK_CATALOGS, pyiceberg_catalog

from conftest import ENGINES, engine_param, expect_gap, fresh

#: All type tables live in demo-sts: every engine can use catalog-vended access there.
WAREHOUSE = WAREHOUSE_STS


class Gap(NamedTuple):
    """A known gap: where it applies (shell-style globs) and how exactly it fails.

    Attributes:
        op: ``"write"`` or ``"read"``.
        engine: For writes the writer (``"duckdb"``); for reads ``"<writer>><reader>"``
            (``"duckdb>daft"``, ``"*>spark"``), because some read failures depend on who wrote.
        fv: Format version as string (``"2"``, ``"3"``, ``"*"``).
        case: Type case name (``"list_*"``).
        match: Regex the error (``"<Type>: <message>"``) must contain. A failure with any
            other error is reported as a real failure, so a gap cannot hide new bugs.
        reason: Shown after "[known gap]" in the failure message.
    """

    op: str
    engine: str
    fv: str
    case: str
    match: str
    reason: str


# PyIceberg 0.12 supports v3 only partly: creating, reading and metadata-only changes work,
# but write_manifest()/write_manifest_list() implement only format v1 and v2.
PYICEBERG_NO_V3 = (
    "PyIceberg 0.12 cannot write DATA to v3 tables (manifest writers exist only for v1/v2: "
    "'Cannot write manifest for table version: 3'); create/read/metadata changes on v3 work"
)
NO_V3_MSG = r"Cannot write manifest for table version: 3"
DUCKDB_NESTED = (
    "Daft cannot read DuckDB's nested Parquet lists ('list type must have a single child')"
)
NULL_LIST = r"\(2, \[\]\)"  # assertion shows row 2 as (2, []) instead of (2, None)

# Every known gap of the matrix, one per line so it reads like a table. FIRST MATCH WINS, so
# specific entries (one type) must come before broad ones (whole engine) - otherwise a broad
# entry claims failures that have a different cause (the error regex then reports them).
# fmt: off
KNOWN_GAPS: list[Gap] = [
    # ---- writes: catalog (table creation already fails, for every writer) -------------------------
    # Unity Catalog OSS 0.7 refuses these types when the table is created (400).
    *([Gap("write", "pg_lake", "2", "time", r"NoSuchTableException: Table not found: lab\.\w+\.t_pg_lake_v2_time_\w+", "Unity refuses TIME, but pg_lake ignores the 400 on CREATE TABLE and reports success - the table exists only in Postgres (tests/specific/test_pglake.py::test_create_refused_by_catalog_is_an_error)")]
      + [Gap("write", "*", "*", c, rf"BadRequestException: (Malformed request: )?Iceberg type {c.upper()}(\([^)]*\))? is not supported by Unity Catalog", f"Unity Catalog OSS 0.7 does not support Iceberg type {c} (400 on CREATE TABLE)")
         for c in ("time", "timestamp_ns", "timestamptz_ns", "unknown", "geometry", "geography")]
      if PROFILE.name == "unity" else []),
    # Catalog-specific: Lakekeeper refuses geo types already when the table is created.
    *([Gap("write", "*", "3", "geo*", r"(RESTError 422: |\(code: 422, ).*untagged enum SchemaEnum", "Lakekeeper v0.15 rejects geometry/geography in table schemas (422)")]
      if PROFILE.name == "lakekeeper" else []),
    # Visible where the catalog accepts geo types (Polaris). Iceberg 1.12 writes the CRS without
    # quotes ("geometry(OGC:CRS84)", as in the spec); PyIceberg 0.12's parser only accepts
    # quoted CRS, so the lab client cannot even load the table (docs/bug-reports/09).
    # Behind it (seen with Polaris 1.7): PyIceberg writes geo columns as binary -> schema mismatch.
    *([Gap("write", "*", "3", "geo*", r"Could not parse geo(metry|graphy)\(OGC:CRS84(, spherical)?\) into a Geo(metry|graphy)Type", "PyIceberg 0.12 cannot parse the spec form geometry(OGC:CRS84) / geography(OGC:CRS84, spherical) that Iceberg 1.12 writes (it expects the CRS in quotes) - the table cannot be loaded")]
      if PROFILE.name == "polaris" else []),
    # ---- writes: single types ---------------------------------------------------------------------
    Gap("write", "polars", "*", "time", r"time64\[ns\]", "Polars Time is time64[ns]; Iceberg time is microseconds"),
    Gap("write", "polars", "*", "uuid", r"Mismatch in fields", "Polars has no UUID type (becomes binary -> schema mismatch)"),
    Gap("write", "polars", "*", "fixed_4", r"Mismatch in fields", "Polars has no fixed-size binary (binary -> schema mismatch)"),
    Gap("write", "trino", "3", "unknown", r"Writing to Iceberg tables with unknown type columns is not supported", "Trino 484 reads Iceberg 'unknown' columns but cannot write them"),
    Gap("write", "flink", "3", "variant", r"No assign rules for VARIANT defined", "Flink cannot INSERT a VARIANT value into an Iceberg variant column"),
    Gap("write", "flink", "3", "unknown", r"Illegal use of 'NULL'", "Flink maps Iceberg unknown to its NULL type, which cannot be a column value"),
    # ---- writes: whole engines on v3 (AFTER the specific entries above: first match wins) -----
    Gap("write", "pyiceberg", "3", "*", NO_V3_MSG, PYICEBERG_NO_V3),
    Gap("write", "polars", "3", "*", NO_V3_MSG, "Polars writes through PyIceberg: " + PYICEBERG_NO_V3),
    Gap("write", "daft", "3", "*", NO_V3_MSG, "Daft writes through PyIceberg: " + PYICEBERG_NO_V3),
    # ---- reads: reader cannot represent the type --------------------------------------------------
    Gap("read", "*>spark", "*", "time", r"does not support time", "Spark has no TIME type"),
    Gap("read", "*>spark", "3", "timestamp*_ns", r"unsupported type to Spark: timestamp(tz)?_ns", "Spark has no nanosecond timestamps"),
    Gap("read", "*>pyiceberg", "3", "variant", r"ValidationError.*TableResponse", "PyIceberg 0.12 cannot parse a schema with variant"),
    Gap("read", "*>polars", "3", "variant", r"ValidationError.*TableResponse", "Polars loads tables via PyIceberg (no variant)"),
    Gap("read", "*>daft", "3", "variant", r"ValidationError.*TableResponse", "Daft loads tables via PyIceberg (no variant)"),
    Gap("read", "*>pg_lake", "3", "unknown", r"iceberg type unknown is not supported", "pg_lake does not support Iceberg type 'unknown'"),
    Gap("read", "*>pg_lake", "3", "variant", r"iceberg type variant is not supported", "pg_lake does not support Iceberg type 'variant'"),
    Gap("read", "*>pg_lake", "3", "timestamptz_ns", r"Type with name timestamptz_ns does not exist", "pg_lake/pgduck cannot read timestamptz_ns"),
    Gap("read", "*>pg_lake", "3", "timestamp_ns", r"30\.123456'\) != \(1, '2026-11-13T13:45:30\.123456789'", "DATA LOSS: pg_lake truncates timestamp_ns to microseconds (Postgres timestamps have microsecond precision)"),
    Gap("read", "*>flink", "3", "unknown", r"null type is reserved for representing untyped NULL", "Flink cannot read a column of Iceberg type unknown"),
    Gap("read", "*>flink", "3", "variant", r"Could not map response to JSON", "Flink SQL Gateway cannot serialize VARIANT (and VARIANT cannot be cast to STRING)"),
    # Flink's TIME has second precision at runtime (even CAST(TIME '..123' AS STRING) drops it).
    Gap("read", "flink>*", "*", "time", r"time\(13, 45, 30\)\) != \(1, datetime\.time\(13, 45, 30, 123456\)", "DATA LOSS: Flink truncates TIME to seconds when writing"),
    Gap("read", "*>flink", "*", "time", r"time\(13, 45, 30, 123000\)\) != \(1, datetime\.time\(13, 45, 30, 123456\)", "WRONG RESULT: Flink 2.3 reads TIME with millisecond precision only (Iceberg time has microseconds)"),
    # ---- reads: geometry/geography (only where the catalog accepts them, e.g. Polaris) ----------
    # Spec form geometry(OGC:CRS84) (written by Iceberg 1.12, e.g. the neutral source): PyIceberg 0.12
    # cannot parse it, so it and the engines loading tables through it cannot even load the table
    # (docs/bug-reports/09). Visible where the catalog accepts geo types (Polaris).
    Gap("read", "*>pyiceberg", "3", "geo*", r"Could not parse geo(metry|graphy)\(OGC:CRS84(, spherical)?\) into a Geo(metry|graphy)Type", "PyIceberg 0.12 cannot parse the spec form geometry(OGC:CRS84) / geography(OGC:CRS84, spherical) - the table cannot be loaded"),
    Gap("read", "*>polars", "3", "geo*", r"Could not parse geo(metry|graphy)\(OGC:CRS84(, spherical)?\) into a Geo(metry|graphy)Type", "Polars loads tables via PyIceberg, which cannot parse the spec form geometry(OGC:CRS84)"),
    Gap("read", "*>daft", "3", "geo*", r"Could not parse geo(metry|graphy)\(OGC:CRS84(, spherical)?\) into a Geo(metry|graphy)Type", "Daft loads tables via PyIceberg, which cannot parse the spec form geometry(OGC:CRS84)"),
    Gap("read", "*>duckdb", "3", "geography", r"Not implemented Error: Geography support", "DuckDB 2.0 reads Iceberg geometry but not geography"),
    Gap("read", "*>pyiceberg", "3", "geo*", r"Cannot promote an binary to geo", "PyIceberg 0.12 cannot read geometry/geography columns"),
    Gap("read", "*>polars", "3", "geo*", r"LogicalType union has no variant set", "Polars' Parquet reader does not know the GEOMETRY/GEOGRAPHY logical type"),
    Gap("read", "*>pg_lake", "3", "geo*", r"iceberg type geo\w+(\([^)]*\))? is not supported", "pg_lake 3.6 does not support geometry/geography"),
    Gap("read", "*>flink", "3", "geo*", r"Cannot convert unknown type to Flink: geo", "Flink has no geometry/geography type"),
    # ---- reads: writer produced files other engines reject ----------------------------------------
    # DuckDB has no fixed-size binary and writes BLOB into the fixed[4] column.
    Gap("read", "duckdb>pyiceberg", "*", "fixed_4", r"Cannot promote an binary to fixed", "DuckDB writes fixed[4] as variable-length binary"),
    Gap("read", "duckdb>spark", "*", "fixed_4", r"FixedSizeBinaryVector cannot be cast", "DuckDB writes fixed[4] as variable-length binary"),
    Gap("read", "duckdb>daft", "*", "list_*", r"list type must have a single child", DUCKDB_NESTED),
    Gap("read", "duckdb>daft", "*", "map_*", r"Map field must have exactly one key_value child", "Daft cannot read DuckDB's Parquet maps ('Map field must have exactly one key_value child')"),
    # pg_lake writes its Parquet files through pgduck_server (= DuckDB): same Daft problem.
    Gap("read", "pg_lake>daft", "*", "list_*", r"list type must have a single child", "pg_lake writes via DuckDB (pgduck_server): " + DUCKDB_NESTED),
    Gap("read", "pg_lake>daft", "*", "map_*", r"Map field must have exactly one key_value child", "pg_lake writes via DuckDB (pgduck_server): Daft cannot read its Parquet maps"),
    # ---- reads: NULL list<struct> becomes [] (PyIceberg write path AND read path) -----------------
    Gap("read", "pyiceberg>*", "*", "list_of_struct", NULL_LIST, "DATA LOSS: PyIceberg writes a NULL list<struct> as []"),
    Gap("read", "polars>*", "*", "list_of_struct", NULL_LIST, "DATA LOSS: Polars writes via PyIceberg: NULL list<struct> -> []"),
    Gap("read", "*>pyiceberg", "*", "list_of_struct", NULL_LIST, "WRONG RESULT: PyIceberg reads a NULL list<struct> as []"),
]
# fmt: on


def known_gap(op: str, engine: str, fv: int, case: str) -> Gap | None:
    """Return the first :data:`KNOWN_GAPS` entry that applies, else ``None``.

    Args:
        op: ``write`` or ``read``.
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).
        case: Type case.

    Returns:
        The gap, or None.
    """
    for g in KNOWN_GAPS:
        if (
            fnmatch.fnmatch(op, g.op)
            and fnmatch.fnmatch(engine, g.engine)
            and fnmatch.fnmatch(str(fv), g.fv)
            and fnmatch.fnmatch(case, g.case)
        ):
            return g
    return None


def type_params():
    """``(fv, case)`` parameters: all v1/v2 types on v2 tables, all types on v3 tables.

    Yields:
        The parameters.
    """
    for fv in (2, 3):
        for case in CASES if fv == 2 else ALL_CASES:
            yield pytest.param(fv, case, id=f"v{fv}-{case.name}")


# =========================================================================== write cache


@pytest.fixture(scope="session")
def written():
    """Session cache ``(writer, fv, case_name) -> (namespace, table) | Exception``.

    ``test_write`` fills it; ``test_read`` reuses it (or writes on demand, e.g. when run with
    ``-k`` filters that deselect the write test).

    Returns:
        An empty dict.
    """
    return {}


def _create_table(namespace: str, table: str, case: TypeCase, fv: int, engine_pool) -> None:
    """Create the ``(id, v)`` table with the exact Iceberg type, independent of the writer.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
        table: Table name.
        case: Type case.
        fv: Iceberg format version (2 or 3).
        engine_pool: Session pool of engine adapters (conftest fixture).
    """
    props = {"format-version": str(fv)}
    if case.iceberg is not None:
        pyiceberg_catalog(WAREHOUSE).create_table(
            f"{namespace}.{table}", schema=case.schema(), properties=props
        )
        return
    # Types PyIceberg cannot model (variant): let Spark create the table.
    spark = engine_pool("spark").spark
    spark.sql(
        f"CREATE TABLE {SPARK_CATALOGS[WAREHOUSE]}.{namespace}.{table} "
        f"(id BIGINT, v {case.spark_ddl}) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{fv}')"
    )


class NotApplicable(Exception):
    """The writer has no way to express this type (no literal / column type / format).

    Deliberately NOT ``NotImplementedError``: a library raising that while writing is a real
    failure and must not turn into a skip.
    """


def _write(writer: str, fv: int, case: TypeCase, namespace: str, engine_pool, written):
    """Create and fill the table for ``(writer, fv, case)`` once; return the cached result.

    Returns:
        ``(namespace, table)`` on success; :class:`NotApplicable` if the writer cannot
        express the type - decided BEFORE writing (-> skip); any exception raised while
        writing, a library's ``NotImplementedError`` included, is a failure (-> FAILED).

    Args:
        writer: Engine that writes.
        fv: Iceberg format version (2 or 3).
        case: Type case.
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).
        written: The session's write cache.
    """
    key = (writer, fv, case.name)
    if key in written:
        return written[key]
    try:
        engine = engine_pool(writer)
    except Exception as e:  # noqa: BLE001 - e.g. DuckDB cannot attach a SigV4 catalog: recorded
        written[key] = e  # like a failed write, so test_reader_type falls back to the next source
        return written[key]
    if writer == "pg_lake":
        # pg_lake can only write tables it created itself, so it creates the table with a
        # Postgres type (see PG_TYPES) instead of using the PyIceberg-created table.
        if fv != 2:
            written[key] = NotApplicable("pg_lake creates only format v2 tables")
            return written[key]
        if PG_TYPES.get(case.name) is None:
            written[key] = NotApplicable(f"Postgres has no column type for {case.name}")
            return written[key]
        table = f"t_pg_lake_v{fv}_{case.name}_{uuid.uuid4().hex[:6]}"
        try:
            _write_pg_lake(engine, namespace, table, case)
            written[key] = (namespace, table)
        except Exception as e:  # noqa: BLE001 - recorded and re-raised by test_write
            written[key] = e
        return written[key]
    # DataFrame engines (no dialect) need Arrow data; SQL engines need a literal.
    if engine.dialect is None and (case.iceberg is None or not case.arrow_writable):
        written[key] = NotApplicable(f"{writer} cannot build {case.name} values")
        return written[key]
    if engine.dialect is not None and case.sql.get(engine.dialect) is None:
        written[key] = NotApplicable(f"{writer} has no SQL type for {case.name}")
        return written[key]
    table = f"t_{writer}_v{fv}_{case.name}_{uuid.uuid4().hex[:6]}"
    try:
        _create_table(namespace, table, case, fv, engine_pool)
        engine.insert_case(namespace, table, case)
        written[key] = (namespace, table)
    except Exception as e:  # noqa: BLE001 - recorded and re-raised by test_write
        written[key] = e
    return written[key]


#: Postgres column type and SQL literal per type case for pg_lake as WRITER (format v2).
#: ``{ns}`` is replaced by the quoted namespace (composite types live in that schema).
#: ``None``: Postgres has no matching type (fixed-size binary) - the write test skips.
PG_TYPES: dict[str, tuple[str, str] | None] = {
    "boolean": ("boolean", "true"),
    "int": ("integer", "2147483647"),
    "long": ("bigint", "9223372036854775807"),
    "float": ("real", "1.5::real"),
    "double": ("double precision", "3.141592653589793::double precision"),
    "double_nan": ("double precision", "'NaN'::double precision"),
    "double_inf": ("double precision", "'Infinity'::double precision"),
    "double_neg_zero": ("double precision", "'-0'::double precision"),
    "float_nan": ("real", "'NaN'::real"),
    "decimal_10_2": ("numeric(10,2)", "12345678.90"),
    "decimal_38_10": ("numeric(38,10)", "1234567890123456789012345678.0123456789"),
    "date": ("date", "DATE '2026-11-13'"),
    "time": ("time", "TIME '13:45:30.123456'"),
    "timestamp": ("timestamp", "TIMESTAMP '2026-11-13 13:45:30.123456'"),
    "timestamptz": ("timestamptz", "TIMESTAMPTZ '2026-11-13 13:45:30.123456+00'"),
    "string": ("text", "'Grüße 🧊 \"quoted\"'"),
    "uuid": ("uuid", "'6f1c2a7e-3b4d-4e5f-9a8b-1c2d3e4f5a6b'::uuid"),
    "fixed_4": None,
    "binary": ("bytea", "'\\x00ff62696e'::bytea"),
    "list_int": ("integer[]", "ARRAY[1, NULL, 3]::integer[]"),
    "map_string_int": (
        "map_type.key_text_val_int",
        "ARRAY[('a', 1), ('b', 2)]::map_type.key_text_val_int",
    ),
    "struct": ("{ns}.st_ab", "ROW(1, 'x')::{ns}.st_ab"),
    "list_of_struct": (
        "{ns}.st_ab[]",
        "ARRAY[ROW(1, 'x')::{ns}.st_ab, ROW(2, NULL)::{ns}.st_ab]",
    ),
}


def _write_pg_lake(engine, namespace: str, table: str, case: TypeCase) -> None:
    """Create ``(id, v)`` through pg_lake with a Postgres type, check the resulting Iceberg
    type, then insert ``(1, value)`` and ``(2, NULL)``.

    Raises AssertionError if pg_lake registers a different Iceberg type family than the case
    expects - writing "the same value" into a weaker type is a fidelity problem, not a pass.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        namespace: Namespace of the table (the session namespace for fixtures).
        table: Table name.
        case: Type case.
    """
    from iceberg_lab.datatypes import expected_family as fam

    ns_q = f'"{namespace}"'
    pg_type, literal = (x.format(ns=ns_q) for x in PG_TYPES[case.name])
    engine.execute(f"CREATE SCHEMA IF NOT EXISTS {ns_q}")
    # Supporting types (idempotent): a composite for struct cases, the map type for maps.
    engine.execute(f"""DO $$ BEGIN
        CREATE TYPE {ns_q}.st_ab AS (a integer, b text);
        EXCEPTION WHEN duplicate_object THEN NULL; END $$""")
    map_exists = engine.execute(
        "SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace "
        "WHERE n.nspname = 'map_type' AND t.typname = 'key_text_val_int'"
    )
    if not map_exists:
        engine.execute("SELECT map_type.create('text', 'int')")
    engine.create_owned_table(namespace, table, f"id bigint, v {pg_type}")
    actual = (
        pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}").schema().find_field("v")
    )
    created = fam(TypeCase(case.name, actual.field_type, None))
    assert created == fam(case), (
        f"pg_lake created column '{pg_type}' as Iceberg {actual.field_type} ({created}), "
        f"expected {fam(case)}"
    )
    engine.execute(f"INSERT INTO {engine.fq(namespace, table)} VALUES (1, {literal}), (2, NULL)")


def _gap(op, engine, fv, case):
    """``expect_gap`` context for this cell: no-op without gap, strict match with gap.

    Args:
        op: ``write`` or ``read``.
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).
        case: Type case.

    Returns:
        The context manager.
    """
    g = known_gap(op, engine, fv, case.name)
    return expect_gap(g.reason if g else None, g.match if g else None)


# =========================================================================== tests


@pytest.mark.parametrize(("fv", "case"), list(type_params()))
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_write(writer, fv, case, namespace, engine_pool, written):
    """The writer inserts a value and a NULL of the type into a format-v2/v3 table.

    Checks:
        * the insert works and the catalog accepts the column type (a type the writer has no
          SQL for: not offered).

    Args:
        writer: Engine that writes the table.
        fv: Iceberg format version of the table (2 or 3).
        case: The Iceberg type case (see ``CASES``).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        written: ``(writer, fv, case) -> (namespace, table)`` or the writer's exception.
    """
    result = _write(writer, fv, case, namespace, engine_pool, written)
    if isinstance(result, NotApplicable):
        pytest.skip(str(result))
    with _gap("write", writer, fv, case):
        if isinstance(result, Exception):
            raise result


@pytest.mark.parametrize(("fv", "case"), list(type_params()))
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_read(writer, reader, fv, case, namespace, engine_pool, written):
    """The reader returns exactly the value and the NULL the writer stored.

    Checks:
        * rows ``(1, <canonical value>)`` and ``(2, NULL)`` - compared after the documented
          normalisation per type.
        Skipped (blocked) if the writer could not write it.

    Args:
        writer: Engine that writes the table.
        reader: Engine that reads it.
        fv: Iceberg format version of the table (2 or 3).
        case: The Iceberg type case (see ``CASES``).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        written: ``(writer, fv, case) -> (namespace, table)`` or the writer's exception.
    """
    result = _write(writer, fv, case, namespace, engine_pool, written)
    if isinstance(result, Exception):
        # Write problems are reported by test_write; do not report them N times again here.
        # NotApplicable (the writer cannot express the type) keeps its message, so the report
        # counts the cell as "not offered" instead of "not testable".
        detail = f": {result}" if isinstance(result, NotApplicable) else ""
        pytest.skip(f"not written by {writer}: {type(result).__name__}{detail}")
    ns, table = result
    with _gap("read", f"{writer}>{reader}", fv, case):
        got = [
            (i, normalize(case, v))
            for i, v in fresh(engine_pool(reader), ns, table).select(ns, table, case)
        ]
        assert got == [(1, canonical(case)), (2, None)]


# =========================================================================== neutral source

#: Writer key of the neutral source in the ``written`` cache - the lab client, not an engine.
NEUTRAL = "lab"
_V2_CASES = {c.name for c in CASES}
#: Types the lab client does not write faithfully - a neutral source must store the canonical
#: value, or the readers would be blamed for the writer's bug (test_read shows it as writer gap).
#: These are written by the reference implementation (Iceberg Java core) instead.
_NEUTRAL_UNFAITHFUL = {"list_of_struct": "PyIceberg writes a NULL list<struct> as []"}
_reference_writer = None


def _reference(engine_pool):
    """The session's reference writer (Iceberg Java core in Spark's JVM, created once).

    Args:
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        The ReferenceWriter.
    """
    global _reference_writer
    if _reference_writer is None:
        from reference_writer import ReferenceWriter

        _reference_writer = ReferenceWriter(engine_pool("spark").spark, WAREHOUSE)
    return _reference_writer


def _neutral_by_java(case: TypeCase) -> bool:
    """Whether the lab client cannot write ``case`` faithfully, so Iceberg Java writes it.

    Args:
        case: Type case.

    Returns:
        True for v3-only types, ``variant`` and the cases in ``_NEUTRAL_UNFAITHFUL``.
    """
    return (
        case.name in _NEUTRAL_UNFAITHFUL
        or case.name not in _V2_CASES
        or case.iceberg is None
        or not case.arrow_writable
    )


def _write_neutral(fv: int, case: TypeCase, namespace: str, engine_pool, written):
    """Table written by a neutral source - never by an engine under test.

    * Types the lab client writes faithfully: the lab client (PyIceberg WITH the lab fixes, as
      in all setup code). Format v3: PyIceberg 0.12 writes no v3 data and refuses to upgrade a
      table itself, so the table is written as v2 and upgraded with the REST update
      ``upgrade-format-version``.
    * All others (v3-only types, ``variant``, NULLs in ``list<struct>``): the reference
      implementation, Iceberg Java core (``tests/lab/reference_writer.py``), natively in ``fv``.

    Args:
        fv: Iceberg format version (2 or 3).
        case: Type case.
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).
        written: The session's write cache.

    Returns:
        ``(namespace, table)`` or the exception.
    """
    key = (NEUTRAL, fv, case.name)
    if key in written:
        return written[key]
    from pyiceberg.table.update import AssertTableUUID, UpgradeFormatVersionUpdate

    from iceberg_lab import engines

    table = f"t_lab_v{fv}_{case.name}_{uuid.uuid4().hex[:6]}"
    if _neutral_by_java(case):
        try:
            type_json = (
                "variant" if case.iceberg is None else json.loads(case.iceberg.model_dump_json())
            )
            _reference(engine_pool).write_values(
                namespace, table, type_json, fv, [(1, case.value), (2, None)]
            )
            written[key] = (namespace, table)
        except Exception as e:  # noqa: BLE001 - recorded and re-raised by test_read_neutral
            written[key] = e
        return written[key]
    fixes, engines.LAB_FIXES_ENABLED = engines.LAB_FIXES_ENABLED, True
    try:
        _create_table(namespace, table, case, 2, engine_pool)
        # the lab's own client (with its fixes), not the PyIceberg engine adapter under test
        cat = pyiceberg_catalog(WAREHOUSE)
        cat.load_table(f"{namespace}.{table}").append(case.arrow())
        if fv == 3:
            t = cat.load_table(f"{namespace}.{table}")
            cat.commit_table(
                t,
                (AssertTableUUID(uuid=t.metadata.table_uuid),),
                (UpgradeFormatVersionUpdate(format_version=3),),
            )
        written[key] = (namespace, table)
    except Exception as e:  # noqa: BLE001 - recorded and re-raised by test_read_neutral
        written[key] = e
    finally:
        engines.LAB_FIXES_ENABLED = fixes
    return written[key]


@pytest.mark.parametrize(("fv", "case"), list(type_params()))
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
def test_read_neutral(reader, fv, case, namespace, engine_pool, written):
    """The reader reads a table a neutral source wrote (lab client or Iceberg Java core).

    Checks:
        * the neutral table was written (else the catalog's own write gap, e.g. a type the
          catalog refuses);
        * the reader returns ``(1, <canonical value>)`` and ``(2, NULL)``.

    Args:
        reader: Engine that reads it.
        fv: Iceberg format version of the table (2 or 3).
        case: The Iceberg type case (see ``CASES``).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        written: ``(writer, fv, case) -> (namespace, table)`` or the writer's exception.
    """
    result = _write_neutral(fv, case, namespace, engine_pool, written)
    if isinstance(result, NotApplicable):
        pytest.skip(str(result))
    if isinstance(result, Exception):
        # The source must work. What the catalog itself refuses (Unity: no TIME) is the
        # catalog's write gap for every writer ("*") - writer-specific gaps do not apply here.
        with _gap("write", NEUTRAL, fv, case):
            raise result
    ns, table = result
    with _gap("read", f"{NEUTRAL}>{reader}", fv, case):
        got = [
            (i, normalize(case, v))
            for i, v in fresh(engine_pool(reader), ns, table).select(ns, table, case)
        ]
        assert got == [(1, canonical(case)), (2, None)]


# =========================================================================== type fidelity

#: Preferred writer per format version for the type-fidelity check (first one that works).
TYPE_SOURCE_WRITERS = {
    2: ("pyiceberg", "duckdb", "spark", "trino"),
    3: ("duckdb", "trino", "spark"),
}

# Type-fidelity gaps: the reader reads the value but reports a weaker/other type.
# Matched against "<reader> reports '<engine type>' (<family>) for Iceberg <expected>".
# fmt: off
TYPE_GAPS: list[Gap] = [
    Gap("type", "spark", "*", "uuid", r"spark reports 'string' \(string\) for Iceberg uuid", "Spark represents Iceberg uuid as string"),
    Gap("type", "spark", "*", "fixed_4", r"spark reports 'binary' \(binary\) for Iceberg fixed\(4\)", "Spark represents fixed(4) as variable-length binary"),
    Gap("type", "polars", "*", "uuid", r"polars reports 'Binary' \(binary\) for Iceberg uuid", "Polars has no UUID type: reads uuid as Binary"),
    Gap("type", "polars", "*", "fixed_4", r"polars reports 'Binary' \(binary\) for Iceberg fixed\(4\)", "Polars has no fixed-size binary: reads fixed(4) as Binary"),
    Gap("type", "trino", "*", "fixed_4", r"trino reports 'varbinary' \(binary\) for Iceberg fixed\(4\)", "Trino represents fixed(4) as varbinary"),
    Gap("type", "pg_lake", "*", "fixed_4", r"pg_lake reports 'bytea' \(binary\) for Iceberg fixed\(4\)", "pg_lake represents fixed(4) as bytea"),
    Gap("type", "pg_lake", "3", "timestamp_ns", r"pg_lake reports 'timestamp without time zone' \(timestamp\) for Iceberg timestamp_ns", "pg_lake maps timestamp_ns to microsecond timestamp"),
    Gap("type", "flink", "*", "uuid", r"flink reports 'BINARY\(16\)' \(fixed\(16\)\) for Iceberg uuid", "Flink has no UUID type: reads uuid as BINARY(16)"),
    # geometry/geography (only where the catalog accepts them, e.g. Polaris)
    Gap("type", "pyiceberg", "3", "geo*", r"Cannot promote an binary to geo", "PyIceberg 0.12 cannot read geometry/geography columns"),
    Gap("type", "polars", "3", "geo*", r"polars reports 'Binary' \(binary\) for Iceberg geo", "Polars has no geometry type: reads geometry as Binary"),
    Gap("type", "daft", "3", "geo*", r"daft reports 'Binary' \(binary\) for Iceberg geo", "Daft has no geometry type: reads geometry as Binary"),
    Gap("type", "pg_lake", "3", "geo*", r"iceberg type geo\w+ is not supported", "pg_lake 3.6 does not support geometry/geography"),
    Gap("type", "flink", "3", "geo*", r"Cannot convert unknown type to Flink: geo", "Flink has no geometry/geography type"),
]
# fmt: on


@pytest.mark.parametrize(("fv", "case"), list(type_params()))
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
def test_reader_type(reader, fv, case, namespace, engine_pool, written):
    """The reader reports a type of the same family as the Iceberg column type.

    Checks:
        * the reported type belongs to the expected family (e.g. uuid, not string; fixed binary,
          not variable); source table: the first engine of ``TYPE_SOURCE_WRITERS`` that wrote
          it, else the neutral source.

    Args:
        reader: Engine that reads it.
        fv: Iceberg format version of the table (2 or 3).
        case: The Iceberg type case (see ``CASES``).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        written: ``(writer, fv, case) -> (namespace, table)`` or the writer's exception.
    """
    source = None
    for writer in ("spark",) if case.iceberg is None else TYPE_SOURCE_WRITERS[fv]:
        result = _write(writer, fv, case, namespace, engine_pool, written)
        if not isinstance(result, Exception):
            source = (writer, result)
            break
    if source is None:
        # No engine writes the type (e.g. geo, unknown): the neutral source (Iceberg Java core)
        # still gives every reader a table to inspect.
        result = _write_neutral(fv, case, namespace, engine_pool, written)
        if not isinstance(result, Exception):
            source = (NEUTRAL, result)
    if source is None:
        pytest.skip(f"neither an engine nor the neutral source wrote {case.name} on v{fv}")
    writer, (ns, table) = source
    expected = expected_family(case)
    engine = engine_pool(reader)

    # 1) Inspecting the type requires reading the table. If the reader cannot read this type
    #    at all, that is the documented READ gap - but only when it actually occurs: read gaps
    #    about wrong *values* (e.g. NULL list -> []) do not affect the type and must not be
    #    reported as "fixed" here.
    read_gap = known_gap("read", f"{writer}>{reader}", fv, case.name)
    try:
        name = fresh(engine, ns, table).type_of(ns, table)  # refresh may already fail to load
    except Exception as exc:
        text = f"{type(exc).__name__}: {exc}"
        if read_gap and re.search(read_gap.match, text, re.IGNORECASE | re.DOTALL):
            with expect_gap(read_gap.reason, read_gap.match):
                raise
        raise

    # 2) The reported type must belong to the same family as the Iceberg type.
    type_gap = next(
        (
            g
            for g in TYPE_GAPS
            if fnmatch.fnmatch(reader, g.engine)
            and fnmatch.fnmatch(str(fv), g.fv)
            and fnmatch.fnmatch(case.name, g.case)
        ),
        None,
    )
    family = type_family(reader, name)
    with expect_gap(type_gap.reason if type_gap else None, type_gap.match if type_gap else None):
        assert same_family(expected, family), (
            f"{reader} reports '{name}' ({family}) for Iceberg {expected}"
        )


NAN_CASES = [c for c in ALL_CASES if c.name in ("double_nan", "float_nan")]


def _float_bound(raw: bytes | None, case: TypeCase) -> float | None:
    """Decode a lower/upper bound of a float/double column (Iceberg single-value binary form:
    little-endian IEEE 754, 4 bytes for float, 8 for double).

    Args:
        raw: The serialized bound, or None if the writer wrote none.
        case: The type case (float or double).

    Returns:
        The bound, or None.
    """
    import struct

    if raw is None:
        return None
    return struct.unpack("<f" if case.name.startswith("float") else "<d", bytes(raw))[0]


@pytest.mark.parametrize("case", NAN_CASES, ids=lambda c: c.name)
@pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_nan_metrics(writer, fv, case, namespace, engine_pool, written):
    """Column metrics of a NaN value follow the spec, so filters cannot prune wrongly.

    The table holds one NaN and one NULL in column ``v``. Readers that prune files by
    lower/upper bounds return wrong results if a writer puts NaN into the bounds.

    Checks:
        * no data file has NaN as lower or upper bound of ``v`` (spec: bounds exclude NaN);
        * where the writer records ``nan_value_counts`` for ``v``, it counts exactly 1 (a
          missing count is allowed - the field is optional);
        * ``null_value_counts`` for ``v``, where recorded, counts exactly 1 (the NULL row).
        Skipped (blocked) if the writer could not write the table.

    Args:
        writer: Engine that wrote the table.
        fv: Iceberg format version (2 or 3).
        case: ``double_nan`` or ``float_nan``.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        written: ``(writer, fv, case) -> (namespace, table)`` or the writer's exception.
    """
    import math

    result = _write(writer, fv, case, namespace, engine_pool, written)
    if isinstance(result, Exception):
        detail = f": {result}" if isinstance(result, NotApplicable) else ""
        pytest.skip(f"not written by {writer}: {type(result).__name__}{detail}")
    ns, table = result
    with _gap("metrics", writer, fv, case):
        tbl = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
        fid = tbl.schema().find_field("v").field_id
        files = tbl.inspect.files().to_pylist()
        assert files, f"{writer} left no data file"
        problems = []
        counts = {"nan_value_counts": [], "null_value_counts": []}
        for f in files:
            name = f["file_path"].rsplit("/", 1)[-1]
            for kind in ("lower_bounds", "upper_bounds"):
                bound = _float_bound(dict(f.get(kind) or {}).get(fid), case)
                if bound is not None and math.isnan(bound):
                    problems.append(f"{name}: NaN in {kind}")
            for kind, got in counts.items():
                got.append(dict(f.get(kind) or {}).get(fid))
        # A writer may put the two rows into separate files: the counts add up over all files.
        # Only checked where every file records the count (the field is optional).
        for kind, got in counts.items():
            if got and None not in got and sum(got) != 1:
                problems.append(f"{kind} over {len(got)} file(s) = {sum(got)}, expected 1")
        assert not problems, f"{writer}: " + "; ".join(problems)


@pytest.mark.parametrize(
    "case",
    # geometry/geography are rejected by Lakekeeper v0.14 in every format version (see
    # KNOWN_GAPS), so they cannot tell us anything about v2 validation.
    [
        c
        for c in ALL_CASES
        if c.min_format_version == 3 and c.iceberg and not c.name.startswith("geo")
    ],
    ids=lambda c: c.name,
)
def test_v3_types_rejected_on_v2_tables(case, namespace):
    """A v3-only type in a format-v2 table is refused with a validation error.

    Checks:
        * creating the table fails with BadRequest and the catalog's validation message
          (Polaris and Unity: documented deviations).

    Args:
        case: The Iceberg type case (see ``CASES``).
        namespace: Fresh namespace of the test session.
    """
    from pyiceberg.exceptions import BadRequestError

    # Polaris refuses too, but answers HTTP 500 (IllegalStateException) instead of 400.
    # Polaris and Unity refuse too, but answer HTTP 500 instead of 400.
    gap = {
        "polaris": expect_gap(
            "Polaris rejects a v3 type in a v2 table with HTTP 500 instead of 400 (Bad Request)",
            r"IllegalStateException: Invalid schema for v2",
        ),
        "unity": expect_gap(
            "Unity Catalog rejects a v3 type in a v2 table with HTTP 500 (ServiceFailureException) instead of 400 (Bad Request)",
            r"ServiceFailureException: Invalid schema for v2:\s+- Invalid type for v: "
            + re.escape(case.name)
            + " is not supported until v3",
        ),
    }.get(PROFILE.name, expect_gap(None, None))
    # The refusal must be a 400 (BadRequestError); the wording differs per catalog.
    message = {
        "rustfs": r"schema contains unsupported primitive type " + re.escape(case.name),
    }.get(PROFILE.name, r"is not supported until v3 but format version is v2")
    with gap, pytest.raises(BadRequestError, match=message):
        pyiceberg_catalog(WAREHOUSE).create_table(
            f"{namespace}.reject_{case.name}_{uuid.uuid4().hex[:6]}",
            schema=case.schema(),
            properties={"format-version": "2"},
        )
