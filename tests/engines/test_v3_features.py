"""Iceberg format v3 features beyond data types.

Deletion vectors
    In v3, row-level deletes are written as *deletion vectors*: one compressed bitmap of
    deleted row positions per data file, stored in a Puffin file. They replace v2's
    position-delete Parquet files. Here Spark, Trino and DuckDB each delete a row in a v3
    table; we check that they really wrote Puffin DVs and that every engine applies them.

Default values
    v3 columns can carry an ``initial-default``: rows in data files written *before* the
    column was added return this value instead of NULL. The column is added with PyIceberg
    (a metadata-only commit, which PyIceberg can do on v3), the data files come from DuckDB.

Row lineage
    v3 tables track ``next-row-id`` in the metadata and expose the hidden columns
    ``_row_id`` (stable id of a logical row, kept across updates) and
    ``_last_updated_sequence_number`` (sequence number of the last change to the row).
    Only Spark exposes them so far.
"""

import uuid

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField

from iceberg_lab.config import WAREHOUSE_STS
from iceberg_lab.engines import polars_storage_options, pyiceberg_catalog

from conftest import ENGINES, engine_param, expect, find_gap, fresh, gap_table, require_engine

WAREHOUSE = WAREHOUSE_STS
#: Seed rows as SQL VALUES, valid in Spark, Trino and DuckDB.
SEED_SQL = "(1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)"

#: Engines that can delete rows in a v3 table, and how.
DV_WRITERS = {
    "spark": "Spark merge-on-read DELETE",
    "trino": "Trino DELETE",
    "duckdb": "DuckDB DELETE",
}

#: Known gaps (only the matching error counts as expected).
# fmt: off
DV_READ_GAPS = gap_table(("writer", "reader"), [
    ("*", "daft", r"not a valid parquet file", "daft-dv-as-parquet"),
    ("*", "pg_lake", r"deletion vectors are not supported", "pglake-no-dv-read"),
])
DEFAULT_READ_GAPS = gap_table(("reader",), [
    # The assertion message shows NULLs where 42 was expected.
    ("daft", r"None, None", "daft-ignores-initial-default"),
    ("flink", r"None, None", "flink-ignores-initial-default"),
    ("pg_lake", r"iceberg column default values are not supported", "pglake-no-initial-default"),
])
MANIFESTS_GAPS = gap_table(("reader",), [
    ("trino", r'"summaries" is null', "duckdb-v3-null-partition-summaries"),
    ("spark", r"ArrayData\.numElements", "duckdb-v3-null-partition-summaries"),
])
# fmt: on


def _fresh(namespace):
    """Unique table name for one v3 test table.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        The name.
    """
    return f"v3_{uuid.uuid4().hex[:8]}"


def _create_v3(engine_pool, writer, namespace, table):
    """Create a v3 table with ``writer``'s own DDL and insert :data:`SEED_SQL`.

    Spark needs ``write.delete.mode=merge-on-read``, otherwise it rewrites data files
    (copy-on-write) and no deletion vector is produced. Trino and DuckDB always write
    delete files for DELETE.

    Args:
        engine_pool: Session pool of engine adapters (conftest fixture).
        writer: Engine that writes.
        namespace: Namespace of the table (the session namespace for fixtures).
        table: Table name (without namespace).

    Returns:
        The table name.
    """
    e = engine_pool(writer)
    fq = e.fq(namespace, table)
    if writer == "spark":
        e.execute(f"""CREATE TABLE {fq} (id INT, name STRING, amount DOUBLE) USING iceberg
                      TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
                                     'write.update.mode'='merge-on-read')""")
    elif writer == "trino":
        e.execute(
            f"CREATE TABLE {fq} (id INTEGER, name VARCHAR, amount DOUBLE) WITH (format_version = 3)"
        )
    else:
        e.execute(
            f"CREATE TABLE {fq} (id INTEGER, name VARCHAR, amount DOUBLE) "
            "WITH ('format-version' = '3')"
        )
    e.execute(f"INSERT INTO {fq} VALUES {SEED_SQL}")
    return e, fq


# =========================================================================== deletion vectors


@pytest.fixture(scope="module")
def dv_tables(namespace, engine_pool):
    """``writer -> table name`` (or the exception), each table with ``id=2`` deleted by writer.

    Built once per module so the reader matrix does not repeat the expensive writes.
    Unavailable writers are left out; their tests are skipped by ``engine_param``.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        Writer -> table name or exception.
    """
    from conftest import AVAILABILITY

    out = {}
    for writer in DV_WRITERS:
        if not AVAILABILITY[writer][0]:
            continue
        table = _fresh(namespace)
        try:
            e, fq = _create_v3(engine_pool, writer, namespace, table)
            e.execute(f"DELETE FROM {fq} WHERE id = 2")
            out[writer] = table
        except Exception as exc:  # noqa: BLE001 - reported by test_v3_delete_writes_deletion_vector
            out[writer] = exc
    return out


@pytest.mark.parametrize("writer", [engine_param(w) for w in DV_WRITERS])
def test_v3_delete_writes_deletion_vector(writer, dv_tables, namespace):
    """A DELETE on a v3 table produces Puffin deletion vectors, not v2 position deletes.

    Checks:
        * the table is v3;
        * every delete file the writer wrote is in Puffin format.

    Args:
        writer: Engine that writes the table.
        dv_tables: ``writer -> table`` (or exception), each with ``id=2`` deleted by the writer.
        namespace: Fresh namespace of the test session.
    """
    table = dv_tables[writer]
    if isinstance(table, Exception):
        raise table
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}")
    assert t.format_version == 3
    formats = {f.file_format.name for task in t.scan().plan_files() for f in task.delete_files}
    assert formats == {"PUFFIN"}, f"{writer} wrote delete files as {formats}"


@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("writer", [engine_param(w) for w in DV_WRITERS])
def test_read_deletion_vectors(writer, reader, dv_tables, namespace, engine_pool):
    """The reader applies the writer's deletion vector: row 2 is gone.

    Checks:
        * the reader returns ids 1 and 3.
        Skipped (blocked) if the writer could not delete on v3.

    Args:
        writer: Engine that writes the table.
        reader: Engine that reads it.
        dv_tables: ``writer -> table`` (or exception), each with ``id=2`` deleted by the writer.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    table = dv_tables[writer]
    if isinstance(table, Exception):
        pytest.skip(f"{writer} could not delete on v3: {table}")
    with expect(find_gap(DV_READ_GAPS, writer=writer, reader=reader)):
        got = fresh(engine_pool(reader), namespace, table).read(namespace, table)
        assert [r[0] for r in got] == [1, 3]


# =========================================================================== default values


@pytest.fixture(scope="module")
def default_table(namespace, engine_pool):
    """v3 table with 3 rows written by DuckDB (else Trino, else Spark - the first that can),
    then column ``extra INT`` added with ``initial-default = 42``. The existing data files do
    not contain ``extra`` at all. If no writer manages, the last error is raised (setup error
    of every reader test - visible, not skipped).

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        The table name.
    """
    table = _fresh(namespace)
    error = None
    for writer in ("duckdb", "trino", "spark"):
        try:
            _create_v3(engine_pool, writer, namespace, table)
            break
        except Exception as exc:  # noqa: BLE001 - try the next writer, keep the error
            error = exc
            table = _fresh(namespace)
    else:
        raise error
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}")
    with t.update_schema() as u:  # metadata-only commit, works on v3
        u.add_column("extra", IntegerType(), default_value=42)
    return table


def _read_extra(engine, namespace, table) -> list:
    """Values of column ``extra`` as read by ``engine``, in row order of ``id``.

    Deliberately NOT ``sorted(values)``: sorting a list containing ``None`` raises a
    ``TypeError`` that would hide the real problem (NULL instead of the default).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        namespace: Namespace of the table (the session namespace for fixtures).
        table: Table name.

    Returns:
        The values.
    """
    if hasattr(engine, "_attach"):  # pg_lake must attach external tables before reading
        engine._attach(namespace, table)
    if engine.dialect:
        return [
            r[0]
            for r in engine.execute(f"SELECT extra FROM {engine.fq(namespace, table)} ORDER BY id")
        ]
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}")
    if engine.name == "polars":
        import polars as pl

        df = pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties)).collect()
        return df.sort("id")["extra"].to_list()
    if engine.name == "daft":
        import daft

        return daft.read_iceberg(t).sort("id").to_pydict()["extra"]
    return t.scan().to_arrow().sort_by("id")["extra"].to_pylist()


def test_initial_default_recorded(default_table, namespace):
    """The default is stored in the table schema (``initial-default``).

    Checks:
        * the added column has initial-default 42.

    Args:
        default_table: v3 table with 3 rows, then column ``extra`` added with initial-default 42.
        namespace: Fresh namespace of the test session.
    """
    field = (
        pyiceberg_catalog(WAREHOUSE)
        .load_table(f"{namespace}.{default_table}")
        .schema()
        .find_field("extra")
    )
    assert field.initial_default == 42


@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
def test_read_initial_default(reader, default_table, namespace, engine_pool):
    """Old rows return the initial default 42, not NULL.

    Checks:
        * the reader returns 42 for the rows written before the column existed.

    Args:
        reader: Engine that reads it.
        default_table: v3 table with 3 rows, then column ``extra`` added with initial-default 42.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    with expect(find_gap(DEFAULT_READ_GAPS, reader=reader)):
        assert _read_extra(
            fresh(engine_pool(reader), namespace, default_table), namespace, default_table
        ) == [42, 42, 42]


# =========================================================================== row lineage


@pytest.mark.slow
def test_row_lineage(namespace, engine_pool):
    """Row ids are unique, survive an UPDATE, and the updated row gets a newer sequence number.

    Checks:
        * next-row-id is set (>= 4);
        * the 4 row ids are unique;
        * the updated row keeps its row id and has a newer last-updated sequence number.

    Args:
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine("spark")
    table = _fresh(namespace)
    e, fq = _create_v3(engine_pool, "spark", namespace, table)
    e.execute(f"INSERT INTO {fq} VALUES (4, 'd', 4.0)")
    e.execute(f"UPDATE {fq} SET amount = 20.0 WHERE id = 2")

    # Table-level counter: next id to assign; 4 rows have been added so far.
    meta = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}").metadata
    assert meta.next_row_id is not None and meta.next_row_id >= 4

    rows = e.execute(f"SELECT id, _row_id, _last_updated_sequence_number FROM {fq} ORDER BY id")
    row_ids = [r[1] for r in rows]
    assert len(set(row_ids)) == 4, "row ids must be unique"
    seq = {r[0]: r[2] for r in rows}
    assert seq[2] > seq[1], "updated row must carry a newer sequence number"
    assert e.execute(f"SELECT _row_id FROM {fq} WHERE id = 2")[0][0] == row_ids[1], (
        "UPDATE must preserve the row id"
    )


# =========================================================================== metadata tables


@pytest.mark.slow
@pytest.mark.parametrize("reader", [engine_param(e) for e in ("spark", "trino")])
def test_manifests_table_of_duckdb_written_v3(reader, namespace, engine_pool):
    """Spark/Trino can query the ``manifests`` metadata table of a v3 table DuckDB wrote.

    Checks:
        * the query returns at least one row (known gap: DuckDB's null partition summaries).

    Args:
        reader: Engine that reads it.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    table = _fresh(namespace)
    pyiceberg_catalog(WAREHOUSE).create_table(
        f"{namespace}.{table}",
        schema=Schema(NestedField(1, "id", IntegerType(), required=False)),
        properties={"format-version": "3"},
    )
    duck = engine_pool("duckdb")
    duck.execute(f"INSERT INTO {duck.fq(namespace, table)} VALUES (1)")
    e = fresh(engine_pool(reader), namespace, table)
    query = (
        f'SELECT * FROM {namespace}."{table}$manifests"'
        if reader == "trino"
        else f"SELECT * FROM {e.fq(namespace, table)}.manifests"
    )
    with expect(find_gap(MANIFESTS_GAPS, reader=reader)):
        assert len(e.execute(query)) >= 1
