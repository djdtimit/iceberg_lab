"""Partitioning and evolution across engines: what one engine writes, every engine must read.

Three matrices, each "writer x format version" plus "x reader" for the read side:

Partition transforms (``test_partitioned_write`` / ``test_partitioned_read``)
    A table partitioned by ONE transform (identity, bucket[4], truncate[2], year, month, day,
    hour) is filled by the writer with the same 6 rows (including NULLs and values right at
    period boundaries). Write side: the partition values the writer recorded must equal the
    values Iceberg's transform functions give (computed with PyIceberg). Read side: every
    engine runs a filter on the source column that partition pruning can use - wrong
    partition values (or a wrong pruning implementation) lose or add rows.

Schema evolution (``test_schema_evolution_write`` / ``..._read``)
    The writer writes rows, then the schema changes (rename ``name`` -> ``label``, add
    ``extra``, drop ``amount``, widen ``id`` int -> long), then the writer writes more rows.
    Readers must resolve the OLD files by field id, not by name: the renamed column keeps its
    values, the added column is NULL in old rows, the dropped column is gone.

Partition evolution (``test_partition_evolution_write`` / ``..._read``)
    Unpartitioned -> ``day(ts)`` -> ``day(ts), bucket(4, id)``, with data written under every
    spec; readers must combine files of three specs, filtered and unfiltered.

Tables are created by PyIceberg (identical spec for every writer); the evolution steps are
done with PyIceberg's API, so only the writer's data path and the readers differ. pg_lake
writes only tables it created itself, so it creates them with its own DDL (``partition_by``,
``ALTER TABLE``) - the same spec and the same steps.

A read whose writer did not produce the table is blocked; a writer that failed only after its
content was right (e.g. Spark's stale spec) still hands the table to the readers
(``conftest.keep_if_content_ok``). The same states written by the reference implementation:
tests/engines/test_neutral_source.py.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import operator
import uuid
from typing import NamedTuple

import pyarrow as pa
import pytest
import test_operations as base
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestampType
from pyiceberg.utils.datetime import datetime_to_micros

from iceberg_lab.config import PROFILE
from iceberg_lab.engines import polars_storage_options, pyiceberg_catalog

from conftest import (
    ENGINES,
    engine_param,
    expect_gap,
    fresh,
    keep_if_content_ok,
    skip_if_not_produced,
)

WAREHOUSE = base.WAREHOUSE
FV = pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")


def _ts(s: str) -> dt.datetime:
    """Naive timestamp from ISO text.

    Args:
        s: ISO timestamp text.

    Returns:
        The datetime.
    """
    return dt.datetime.fromisoformat(s)


PSCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "ts", TimestampType(), required=False),
)
#: Values at period boundaries (year/month/day/hour) and a row of NULLs.
PROWS = [
    (1, "alpha", _ts("2025-12-31 23:59:59")),
    (2, "alpine", _ts("2026-01-01 00:00:00")),
    (3, "beta", _ts("2026-01-15 13:30:00")),
    (4, "beta", _ts("2026-02-01 00:00:01")),
    (5, "gamma", _ts("2026-02-28 23:00:00")),
    (6, None, None),
]


class T(NamedTuple):
    """One partition transform: source column, transform, pg_lake spelling, filter, result."""

    source: str
    transform: object
    pg: str
    pred: tuple  # (column, operator, value)
    ids: set


TRANSFORMS = {
    "identity": T("name", IdentityTransform(), "name", ("name", "=", "beta"), {3, 4}),
    "bucket4": T("id", BucketTransform(4), "bucket(4, id)", ("id", "=", 5), {5}),
    "truncate2": T("name", TruncateTransform(2), "truncate(2, name)", ("name", "=", "alpine"), {2}),
    "year": T("ts", YearTransform(), "year(ts)", ("ts", ">=", _ts("2026-01-01")), {2, 3, 4, 5}),
    "month": T("ts", MonthTransform(), "month(ts)", ("ts", ">=", _ts("2026-02-01")), {4, 5}),
    "day": T("ts", DayTransform(), "day(ts)", ("ts", "<", _ts("2026-01-15")), {1, 2}),
    "hour": T("ts", HourTransform(), "hour(ts)", ("ts", ">=", _ts("2026-02-28 23:00")), {5}),
}

OPS = {"=": operator.eq, ">=": operator.ge, "<": operator.lt}


class Gap(NamedTuple):
    """``test``/``writer``/``reader``/``case``/``fv`` globs + regex the error must contain."""

    test: str
    writer: str
    reader: str
    case: str
    fv: str
    match: str
    reason: str
    flaky: bool = False


# Filled from observed failures only; first match wins.
# fmt: off
KNOWN_GAPS: list[Gap] = [
    # Iceberg's SparkCatalog caches loaded tables (cache-enabled=true, 30 s expiry): a change
    # made by another engine stays invisible to Spark until the entry expires.
    Gap("test_schema_evolution_write", "spark", "*", "*", "*", r"name `label` cannot be resolved", "Spark's table cache (cache-enabled=true, 30 s) hides a column renamed by another engine"),
    Gap("test_partition_evolution_write", "spark", "*", "*", "*", r"data files under specs \{0, 1\}", "Spark's table cache (cache-enabled=true, 30 s) keeps writing with the old partition spec after another engine changed it"),
    # Wrong result: pg_lake writes no null_value_counts; Trino then treats "ts < <day boundary>"
    # as fully answered by the day(ts) partition and keeps the NULL partition (row with ts NULL).
    # Not at "<day boundary + 1 s>", not with month(ts), not with ">=" (checked).
    Gap("test_partitioned_read", "pg_lake", "trino", "day", "*", r"got \[1, 2, 6\]", "WRONG RESULT: Trino returns the ts=NULL row for 'ts < <day boundary>' on a day(ts)-partitioned table without null_value_counts (pg_lake writes none)"),
    # pg_lake sends partition fields with BOTH source-id and source-ids; Lakekeeper 0.15 rejects
    # that (422, "mutually exclusive"), pg_lake only warns and fails later
    # (docs/bug-reports/08-pg_lake-partition-field-source-id-and-source-ids.md).
    *([Gap("test_partitioned_write", "pg_lake", "*", "*", "*", r"HTTP request failed \(HTTP 404\)\s+DETAIL:\s+Error getting tabular from catalog", base.PGLAKE_SOURCE_IDS),
       Gap("test_partition_evolution_write", "pg_lake", "*", "*", "*", r"partition spec 1 not found for manifest", base.PGLAKE_SOURCE_IDS)]
      if PROFILE.name == "lakekeeper" else []),
    *(Gap("*_write", e, "*", "*", "3", base.NO_V3_MSG, f"{e}: " + base.PYICEBERG_NO_V3)
      for e in ("pyiceberg", "polars", "daft")),
]
# fmt: on


def _gap(test, writer, fv, reader="-", case="-"):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        test: Test name in ``KNOWN_GAPS``.
        writer: Engine that writes.
        fv: Iceberg format version (2 or 3).
        reader: Engine that reads.
        case: Partition transform / case.

    Returns:
        The context manager.
    """
    for g in KNOWN_GAPS:
        if all(
            fnmatch.fnmatch(v, p)
            for v, p in (
                (test, g.test),
                (writer, g.writer),
                (reader, g.reader),
                (case, g.case),
                (str(fv), g.fv),
            )
        ):
            return expect_gap(g.reason, g.match, flaky=g.flaky)
    return expect_gap(None, None)


# =========================================================================== SQL helpers


def lit(dialect: str, v, kind: str) -> str:
    """SQL literal of ``v`` (kind: int|dbl|str|ts) in ``dialect``; typed NULLs where needed.

    Args:
        dialect: SQL dialect.
        v: Value.
        kind: Value kind (int, dbl, str, ts).

    Returns:
        The literal.
    """
    if v is None:
        if dialect == "flink":
            return {
                "int": "CAST(NULL AS BIGINT)",
                "str": "CAST(NULL AS STRING)",
                "dbl": "CAST(NULL AS DOUBLE)",
            }.get(kind, "CAST(NULL AS TIMESTAMP(6))")
        return "NULL"
    if kind == "int":
        return str(v)
    if kind == "dbl":
        return f"{v!r}E0"
    if kind == "str":
        return "'" + v.replace("'", "''") + "'"
    s = v.isoformat(sep=" ", timespec="microseconds")
    return f"TIMESTAMP_NTZ '{s}'" if dialect == "spark" else f"TIMESTAMP '{s}'"


def insert_sql(adapter, ns, table, cols, kinds, rows) -> str:
    """``INSERT INTO ... VALUES`` for the rows in the adapter's dialect.

    Args:
        adapter: Engine adapter.
        ns: Namespace of the table.
        table: Table name (without namespace).
        cols: Column names.
        kinds: Value kinds of the columns.
        rows: Rows as ``(id, name, amount)`` tuples.

    Returns:
        The statement.
    """
    values = ", ".join(
        "(" + ", ".join(lit(adapter.dialect, v, k) for v, k in zip(r, kinds, strict=True)) + ")"
        for r in rows
    )
    return f"INSERT INTO {adapter.fq(ns, table)} ({', '.join(cols)}) VALUES {values}"


def write_rows(engine, adapter, ns, table, schema: Schema, rows, kinds) -> None:
    """Append ``rows`` (columns of ``schema``) through ``engine``'s own write path.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace of the table.
        table: Table name (without namespace).
        schema: Iceberg schema of the rows.
        rows: Rows as ``(id, name, amount)`` tuples.
        kinds: Value kinds of the columns.
    """
    cols = [f.name for f in schema.fields]
    if engine in ("pyiceberg", "polars", "daft"):
        t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
        data = pa.Table.from_pylist(
            [dict(zip(cols, r, strict=True)) for r in rows],
            schema=schema_to_pyarrow(schema, include_field_ids=False),
        )
        if engine == "pyiceberg":
            t.append(data)
        elif engine == "polars":
            import polars as pl

            pl.from_arrow(data).write_iceberg(t, mode="append")
        else:
            import daft

            daft.from_arrow(data).write_iceberg(t, mode="append")
        return
    adapter.execute(insert_sql(adapter, ns, table, cols, kinds, rows))


def pg_create(adapter, ns, table, ddl_cols: str, options: str = "") -> None:
    """Create a writable pg_lake Iceberg table (pg_lake only writes its own tables).

    Args:
        adapter: Engine adapter.
        ns: Namespace of the table.
        table: Table name (without namespace).
        ddl_cols: Postgres column list.
        options: Further pg_lake table options.
    """
    adapter.execute(f'CREATE SCHEMA IF NOT EXISTS "{ns}"')
    adapter.execute(
        f"CREATE TABLE {adapter.fq(ns, table)} ({ddl_cols}) USING iceberg "
        f"WITH (catalog = '{adapter.server}'{options})"
    )
    adapter.owned.add((ns, table))


def read_cols(reader, adapter, ns, table, cols, pred=None) -> list[tuple]:
    """Rows (``cols``) as read by ``reader``, optionally filtered by ``pred``; sorted.

    Args:
        reader: Engine that reads.
        adapter: Engine adapter.
        ns: Namespace of the table.
        table: Table name (without namespace).
        cols: Column names.
        pred: Filter ``(column, operator, value)``.

    Returns:
        Sorted rows.
    """
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
    if reader == "pyiceberg":
        from pyiceberg.expressions import EqualTo, GreaterThanOrEqual, LessThan

        kw = {}
        if pred:
            c, op, v = pred
            v = v.isoformat() if isinstance(v, dt.datetime) else v
            kw["row_filter"] = {"=": EqualTo, ">=": GreaterThanOrEqual, "<": LessThan}[op](c, v)
        a = t.scan(selected_fields=tuple(cols), **kw).to_arrow()
        return sorted(zip(*(a[c].to_pylist() for c in cols), strict=True))
    if reader == "polars":
        import polars as pl

        lf = pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties))
        if pred:
            c, op, v = pred
            lf = lf.filter(OPS[op](pl.col(c), pl.lit(v)))
        return sorted(lf.select(cols).collect().iter_rows())
    if reader == "daft":
        import daft
        from daft import col

        df = daft.read_iceberg(t)
        if pred:
            c, op, v = pred
            df = df.where(OPS[op](col(c), daft.lit(v)))
        d = df.select(*cols).to_pydict()
        return sorted(zip(*(d[c] for c in cols), strict=True))
    if hasattr(adapter, "_attach"):
        adapter._attach(ns, table)
    sql = f"SELECT {', '.join(cols)} FROM {adapter.fq(ns, table)}"
    if pred:
        c, op, v = pred
        kind = "int" if isinstance(v, int) else "str" if isinstance(v, str) else "ts"
        sql += f" WHERE {c} {op} {lit(adapter.dialect, v, kind)}"
    return sorted(tuple(r) for r in adapter.execute(sql))


def _skip_pg_v3(engine, fv):
    """Skip: pg_lake creates only format v2 tables.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).
    """
    if engine == "pg_lake" and fv == 3:
        pytest.skip("pg_lake creates only format v2 tables")


def _name(prefix, writer, fv, case=""):
    """Unique table name.

    Args:
        prefix: Name prefix.
        writer: Engine that writes.
        fv: Iceberg format version (2 or 3).
        case: Partition transform / case.

    Returns:
        The name.
    """
    return f"{prefix}_{writer}_{case}_v{fv}_{uuid.uuid4().hex[:6]}".replace("__", "_")


def _norm_partition(v):
    """Partition value as reported by PyIceberg -> transform result (days/micros as int).

    Args:
        v: Partition value.

    Returns:
        The comparable value.
    """
    if isinstance(v, dt.datetime):
        return datetime_to_micros(v)
    if isinstance(v, dt.date):
        return (v - dt.date(1970, 1, 1)).days
    return v


def _own_read(writer, engine_pool, ns, table, cols):
    """The writing engine's own read (content check for keep_if_content_ok).

    Args:
        writer: Engine that writes.
        engine_pool: Session pool of engine adapters (conftest fixture).
        ns: Namespace of the table.
        table: Table name (without namespace).
        cols: Column names.

    Returns:
        Sorted rows.
    """
    adapter = None if writer in ("pyiceberg", "polars", "daft") else engine_pool(writer)
    if adapter is not None:
        fresh(adapter, ns, table)
    return read_cols(writer, adapter, ns, table, cols)


# =========================================================================== transforms


@pytest.fixture(scope="module")
def partitioned():
    """(writer, transform, fv) -> (namespace, table) of successfully written tables.

    Returns:
        An empty dict.
    """
    return {}


@pytest.mark.slow
@FV
@pytest.mark.parametrize("case", list(TRANSFORMS))
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_partitioned_write(writer, case, fv, namespace, engine_pool, partitioned):
    """The writer fills a table partitioned by one transform; the partition values are correct.

    Checks:
        * the table has exactly one partition field;
        * PyIceberg reads exactly the written rows (incl. a row of NULLs);
        * the partition value of every data file equals the transform applied to its rows
          (identity, bucket[4], truncate[2], year, month, day, hour).
        If the content is right but a later check fails, the table still goes to the reads.

    Args:
        writer: Engine that writes the table.
        case: Partition transform (see ``TRANSFORMS``).
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        partitioned: ``(writer, case, fv) -> (namespace, table)`` of the written tables (None:
            attempted without a readable result).
    """
    partitioned[(writer, case, fv)] = None  # attempted (see conftest.skip_if_not_produced)
    _skip_pg_v3(writer, fv)
    tr = TRANSFORMS[case]
    table = _name("pt", writer, fv, case)
    cat = pyiceberg_catalog(WAREHOUSE)
    with (
        keep_if_content_ok(
            partitioned,
            (writer, case, fv),
            (namespace, table),
            lambda: _own_read(writer, engine_pool, namespace, table, ["id", "name", "ts"]) == PROWS,
        ),
        _gap("test_partitioned_write", writer, fv, case=case),
    ):
        adapter = engine_pool(writer)
        if writer == "pg_lake":
            pg_create(
                adapter, namespace, table, "id bigint, name text, ts timestamp",
                f", partition_by = '{tr.pg}'",
            )  # fmt: skip
        else:
            src = PSCHEMA.find_field(tr.source)
            spec = PartitionSpec(PartitionField(src.field_id, 1000, tr.transform, f"p_{case}"))
            cat.create_table(
                f"{namespace}.{table}",
                schema=PSCHEMA,
                partition_spec=spec,
                properties={"format-version": str(fv)},
            )
        write_rows(writer, adapter, namespace, table, PSCHEMA, PROWS, ("int", "str", "ts"))

        t = cat.load_table(f"{namespace}.{table}")
        assert len(t.spec().fields) == 1, f"table spec: {t.spec()}"
        got = read_cols("pyiceberg", None, namespace, table, ["id", "name", "ts"])
        assert got == PROWS, got
        idx = {"id": 0, "name": 1, "ts": 2}[tr.source]
        fn = tr.transform.transform(PSCHEMA.find_field(tr.source).field_type)
        expected = {
            fn(datetime_to_micros(r[idx]) if isinstance(r[idx], dt.datetime) else r[idx])
            for r in PROWS
        }
        files = t.inspect.files()
        actual = {
            _norm_partition(next(iter(p.values())))
            for p, c in zip(
                files["partition"].to_pylist(), files["content"].to_pylist(), strict=True
            )
            if c == 0
        }
        assert actual == expected, (
            f"partition values {sorted(actual, key=str)} != {sorted(expected, key=str)}"
        )
    partitioned[(writer, case, fv)] = (namespace, table)


@pytest.mark.slow
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@FV
@pytest.mark.parametrize("case", list(TRANSFORMS))
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_partitioned_read(writer, case, fv, reader, engine_pool, partitioned):
    """The reader filters the writer's partitioned table and gets exactly the matching rows.

    Checks:
        * the filter of the case (e.g. ``ts >= 2026-02-01`` on month(ts)) returns exactly the
          expected ids - no row of a pruned partition missing, no NULL row wrongly kept;
        * no row twice.
        Skipped (blocked) if the writer could not produce the table.

    Args:
        writer: Engine that writes the table.
        case: Partition transform (see ``TRANSFORMS``).
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        partitioned: ``(writer, case, fv) -> (namespace, table)`` of the written tables (None:
            attempted without a readable result).
    """
    ns, table = skip_if_not_produced(partitioned, (writer, case, fv), "test_partitioned_write")
    tr = TRANSFORMS[case]
    with _gap("test_partitioned_read", writer, fv, reader, case):
        rows = read_cols(reader, fresh(engine_pool(reader), ns, table), ns, table, ["id"], tr.pred)
        assert {r[0] for r in rows} == tr.ids, f"filter {tr.pred}: got {sorted(r[0] for r in rows)}"
        assert len(rows) == len(tr.ids), f"duplicates: {rows}"


# =========================================================================== schema evolution

SCHEMA0 = base.SCHEMA  # id int, name string, amount double
SCHEMA1 = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "label", StringType(), required=False),
    NestedField(4, "extra", IntegerType(), required=False),
)
R1 = [(1, "a", 1.0), (2, "b", 2.0)]
R2 = [(3, "c", 30)]
EVOLVED = [(1, "a", None), (2, "b", None), (3, "c", 30)]


@pytest.fixture(scope="module")
def evolved():
    """(writer, fv) -> (namespace, table) after a successful schema-evolution write.

    Returns:
        An empty dict.
    """
    return {}


@pytest.mark.slow
@FV
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_schema_evolution_write(writer, fv, namespace, engine_pool, evolved):
    """Write, evolve the schema (rename, add, drop, widen), write again - with the writer's data path.

    Checks:
        * the schema is ``id (long), label, extra`` afterwards;
        * PyIceberg reads the old rows with NULL in the new column and the new row with its
          value (resolved by field id).

    Args:
        writer: Engine that writes the table.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        evolved: ``(writer, fv) -> (namespace, table)`` of the evolved tables.
    """
    evolved[(writer, fv)] = None  # attempted (see conftest.skip_if_not_produced)
    _skip_pg_v3(writer, fv)
    table = _name("se", writer, fv)
    cat = pyiceberg_catalog(WAREHOUSE)
    cols = ["id", "label", "extra"]
    with (
        keep_if_content_ok(
            evolved,
            (writer, fv),
            (namespace, table),
            lambda: _own_read(writer, engine_pool, namespace, table, cols) == EVOLVED,
        ),
        _gap("test_schema_evolution_write", writer, fv),
    ):
        adapter = engine_pool(writer)
        if writer == "pg_lake":
            pg_create(adapter, namespace, table, "id integer, name text, amount double precision")
            write_rows(writer, adapter, namespace, table, SCHEMA0, R1, ("int", "str", "dbl"))
            fq = adapter.fq(namespace, table)
            for stmt in (
                f"ALTER TABLE {fq} RENAME COLUMN name TO label",
                f"ALTER TABLE {fq} ADD COLUMN extra integer",
                f"ALTER TABLE {fq} DROP COLUMN amount",
                f"ALTER TABLE {fq} ALTER COLUMN id TYPE bigint",
            ):
                adapter.execute(stmt)
        else:
            cat.create_table(
                f"{namespace}.{table}", schema=SCHEMA0, properties={"format-version": str(fv)}
            )
            ctx = base.Ctx(cat, namespace, table, fv, seed_writer=writer)
            base.append_rows(ctx, engine_pool, R1)
            with cat.load_table(f"{namespace}.{table}").update_schema() as u:
                u.rename_column("name", "label")
                u.add_column("extra", IntegerType())
                u.delete_column("amount")
                u.update_column("id", LongType())
        write_rows(writer, adapter, namespace, table, SCHEMA1, R2, ("int", "str", "int"))
        t = cat.load_table(f"{namespace}.{table}")
        assert [f.name for f in t.schema().fields] == ["id", "label", "extra"], t.schema()
        assert read_cols("pyiceberg", None, namespace, table, ["id", "label", "extra"]) == EVOLVED
    evolved[(writer, fv)] = (namespace, table)


@pytest.mark.slow
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@FV
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_schema_evolution_read(writer, fv, reader, engine_pool, evolved):
    """The reader resolves old and new files of the evolved table by field id.

    Checks:
        * the reader returns exactly the evolved content: renamed column with old values, added
          column NULL for old rows, widened id.
        Skipped (blocked) if the writer could not produce the table.

    Args:
        writer: Engine that writes the table.
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        evolved: ``(writer, fv) -> (namespace, table)`` of the evolved tables.
    """
    ns, table = skip_if_not_produced(evolved, (writer, fv), "test_schema_evolution_write")
    with _gap("test_schema_evolution_read", writer, fv, reader):
        got = read_cols(
            reader, fresh(engine_pool(reader), ns, table), ns, table, ["id", "label", "extra"]
        )
        assert got == EVOLVED


# =========================================================================== partition evolution

PE_PRED = ("ts", ">=", _ts("2026-02-01"))
PE_IDS = {4, 5}


@pytest.fixture(scope="module")
def spec_evolved():
    """(writer, fv) -> (namespace, table) after a successful partition-evolution write.

    Returns:
        An empty dict.
    """
    return {}


@pytest.mark.slow
@FV
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_partition_evolution_write(writer, fv, namespace, engine_pool, spec_evolved):
    """Rows under three partition specs: none, ``day(ts)``, ``day(ts), bucket(4, id)``.

    Checks:
        * the table has three specs;
        * its data files lie under all three spec ids (0, 1, 2) - the writer used the current
          spec after each change.

    Args:
        writer: Engine that writes the table.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        spec_evolved: ``(writer, fv) -> (namespace, table)`` of the spec-evolved tables.
    """
    spec_evolved[(writer, fv)] = None  # attempted (see conftest.skip_if_not_produced)
    _skip_pg_v3(writer, fv)
    table = _name("pe", writer, fv)
    cat = pyiceberg_catalog(WAREHOUSE)
    kinds = ("int", "str", "ts")

    def content_ok():
        """The writer's own read returns ids 1..6.

        Returns:
            Whether it does.
        """
        rows = _own_read(writer, engine_pool, namespace, table, ["id"])
        return sorted(r[0] for r in rows) == [1, 2, 3, 4, 5, 6]

    with (
        keep_if_content_ok(spec_evolved, (writer, fv), (namespace, table), content_ok),
        _gap("test_partition_evolution_write", writer, fv),
    ):
        adapter = engine_pool(writer)
        if writer == "pg_lake":
            pg_create(adapter, namespace, table, "id bigint, name text, ts timestamp")
            fq = adapter.fq(namespace, table)
            steps = [
                None,
                f"ALTER FOREIGN TABLE {fq} OPTIONS (ADD partition_by 'day(ts)')",
                f"ALTER FOREIGN TABLE {fq} OPTIONS (SET partition_by 'day(ts), bucket(4, id)')",
            ]
        else:
            cat.create_table(
                f"{namespace}.{table}", schema=PSCHEMA, properties={"format-version": str(fv)}
            )
            steps = [None, "day", "bucket"]
        for step, rows in zip(steps, (PROWS[:3], PROWS[3:5], PROWS[5:]), strict=True):
            if step in ("day", "bucket"):
                with cat.load_table(f"{namespace}.{table}").update_spec() as u:
                    if step == "day":
                        u.add_field("ts", DayTransform(), "ts_day")
                    else:
                        u.add_field("id", BucketTransform(4), "id_bucket")
            elif step:
                adapter.execute(step)
            write_rows(writer, adapter, namespace, table, PSCHEMA, rows, kinds)
        t = cat.load_table(f"{namespace}.{table}")
        files = t.inspect.files()
        assert len(t.specs()) == 3, f"specs: {t.specs()}"
        spec_ids = set(files["spec_id"].to_pylist())
        assert spec_ids == {0, 1, 2}, f"data files under specs {spec_ids}"
    spec_evolved[(writer, fv)] = (namespace, table)


@pytest.mark.slow
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@FV
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_partition_evolution_read(writer, fv, reader, engine_pool, spec_evolved):
    """The reader combines files of three partition specs.

    Checks:
        * reading all rows returns ids 1..6;
        * the filter ``ts >= 2026-02-01`` returns exactly ids 4 and 5, each once.
        Skipped (blocked) if the writer could not produce the table.

    Args:
        writer: Engine that writes the table.
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        spec_evolved: ``(writer, fv) -> (namespace, table)`` of the spec-evolved tables.
    """
    ns, table = skip_if_not_produced(spec_evolved, (writer, fv), "test_partition_evolution_write")
    with _gap("test_partition_evolution_read", writer, fv, reader):
        adapter = fresh(engine_pool(reader), ns, table)
        assert {r[0] for r in read_cols(reader, adapter, ns, table, ["id"])} == {1, 2, 3, 4, 5, 6}
        rows = read_cols(reader, adapter, ns, table, ["id"], PE_PRED)
        assert {r[0] for r in rows} == PE_IDS and len(rows) == len(PE_IDS), rows
