"""Data file formats: ORC and Avro besides Parquet - who writes them, who reads them.

The Iceberg spec allows Parquet, ORC and Avro data files (``write.format.default``). Every other
test writes Parquet, so an engine that cannot read an ORC table written by another engine would go
unnoticed. Same pattern as the type matrix: the lab client creates the table
``(id BIGINT, v STRING)`` with ``write.format.default = <format>`` (format v2), the writer inserts
``(1, value)`` and ``(2, NULL)`` through its own path, then

    test_write_format[<writer>-<fmt>]          the data files really are <fmt> (checked in the
                                               table's file list) - an engine that silently
                                               writes Parquet instead fails here
    test_read_format[<writer>-<reader>-<fmt>]  every reader returns exactly those rows

Writes are cached per session (one table per writer and format). pg_lake writes only tables it
created itself, always Parquet ("not offered").
"""

from __future__ import annotations

import uuid

import pytest

from iceberg_lab.config import WAREHOUSE_STS
from iceberg_lab.datatypes import CASES, canonical, normalize
from iceberg_lab.engines import pyiceberg_catalog

from conftest import ENGINES, engine_param, expect, find_gap, fresh, gap_table, offers

WAREHOUSE = WAREHOUSE_STS
FORMATS = ["orc", "avro"]
CASE = next(c for c in CASES if c.name == "string")

# Filled from observed failures only: (op, engine glob, format glob, regex, reason).
# fmt: off
KNOWN_GAPS = gap_table(("op", "engine", "fmt"), [
    ("write", "pyiceberg", "*", r"No writer registered for FileFormat\.(ORC|AVRO)", "pyiceberg-parquet-only-write"),
    ("write", "polars", "*", r"No writer registered for FileFormat\.(ORC|AVRO)", "pyiceberg-parquet-only-write"),
    ("write", "duckdb", "*", r"Cannot write data file with 'write\.format\.default'='(orc|avro)': only parquet is supported", "duckdb-parquet-only-write"),
    ("write", "daft", "*", r"daft wrote \['parquet'\] data files", "daft-ignores-write-format"),
    ("read", "*>pyiceberg", "avro", r"Unsupported file format: FileFormat\.AVRO", "pyiceberg-no-avro-read"),
    ("read", "*>polars", "avro", r"Unsupported file format: FileFormat\.AVRO", "pyiceberg-no-avro-read"),
    ("read", "*>daft", "*", r"FileFormat\.(ORC|AVRO) for iceberg not implemented", "daft-parquet-only-read"),
    ("read", "*>duckdb", "*", r"File format '(ORC|AVRO)' not supported, only supports 'parquet'", "duckdb-parquet-only-read"),
    ("read", "*>pg_lake", "*", r"No magic bytes found at end of file '[^']+\.(orc|avro)'", "pglake-reads-all-as-parquet"),
])
# fmt: on


class NotOffered(Exception):
    """The writer cannot write into a table it did not create (decided before writing)."""


def _gap(op, engine, fmt):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        op: Operation (``write``/``read``/...).
        engine: Engine name (see ``conftest.ENGINES``).
        fmt: Data file format.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, op=op, engine=engine, fmt=fmt))


@pytest.fixture(scope="session")
def written():
    """(writer, fmt) -> (namespace, table) | Exception.

    Returns:
        An empty dict.
    """
    return {}


def _write(writer, fmt, namespace, engine_pool, written):
    """Write the table in the format once per session (cached).

    Args:
        writer: Engine that writes.
        fmt: Data file format.
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).
        written: The write cache.

    Returns:
        ``(namespace, table)``, NotApplicable or the exception.
    """
    key = (writer, fmt)
    if key in written:
        return written[key]
    if not offers(writer, "writing tables it did not create"):
        written[key] = NotOffered(
            f"{writer} has no API for writing tables it did not create (capability registry)"
        )
        return written[key]
    table = f"fmt_{writer}_{fmt}_{uuid.uuid4().hex[:6]}"
    try:
        pyiceberg_catalog(WAREHOUSE).create_table(
            f"{namespace}.{table}",
            schema=CASE.schema(),
            properties={"format-version": "2", "write.format.default": fmt},
        )
        engine_pool(writer).insert_case(namespace, table, CASE)
        written[key] = (namespace, table)
    except Exception as e:  # noqa: BLE001 - recorded, raised by test_write_format
        written[key] = e
    return written[key]


def _formats(namespace, table) -> set[str]:
    """File formats of the table's data files.

    Args:
        namespace: Namespace.
        table: Table name.

    Returns:
        The formats.
    """
    files = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}").inspect.files()
    return {str(f).lower() for f in files.column("file_format").to_pylist()}


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_write_format(writer, fmt, namespace, engine_pool, written):
    """The writer writes data files in the format the table asks for (``write.format.default``).

    Checks:
        * every data file of the written table has the requested format (Parquet, ORC or Avro)
          - an engine that silently writes Parquet instead is a documented gap.
        Writers that cannot write the format at all: not offered.

    Args:
        writer: Engine that writes the table.
        fmt: Data file format (``parquet``, ``orc``, ``avro``).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        written: ``(writer, fmt) -> (namespace, table)`` or the writer's exception.
    """
    result = _write(writer, fmt, namespace, engine_pool, written)
    if isinstance(result, NotOffered):
        pytest.skip(str(result))
    with _gap("write", writer, fmt):
        if isinstance(result, Exception):
            raise result
        found = _formats(*result)
        assert found == {fmt}, (
            f"{writer} wrote {sorted(found)} data files into a table with write.format.default={fmt}"
        )


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_read_format(writer, reader, fmt, namespace, engine_pool, written):
    """The reader returns the rows the writer stored in files of that format.

    Checks:
        * the reader returns exactly the stored value and the NULL row.
        Skipped (blocked) if the writer produced no files of that format.

    Args:
        writer: Engine that writes the table.
        reader: Engine that reads it.
        fmt: Data file format (``parquet``, ``orc``, ``avro``).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        written: ``(writer, fmt) -> (namespace, table)`` or the writer's exception.
    """
    result = _write(writer, fmt, namespace, engine_pool, written)
    if isinstance(result, Exception):
        detail = f": {result}" if isinstance(result, NotOffered) else ""
        pytest.skip(f"not written by {writer}: {type(result).__name__}{detail}")
    ns, table = result
    if _formats(ns, table) != {fmt}:
        pytest.skip(f"not written by {writer}: no {fmt} data files (see test_write_format)")
    with _gap("read", f"{writer}>{reader}", fmt):
        got = [
            (i, normalize(CASE, v))
            for i, v in fresh(engine_pool(reader), ns, table).select(ns, table, CASE)
        ]
        assert got == [(1, canonical(CASE)), (2, None)]
