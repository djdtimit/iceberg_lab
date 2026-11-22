"""File layout per writer: does the engine honour the table's sort order and target file size?

``test_sort_order_honored[writer]``
    The table (created by PyIceberg) has the sort order ``name ASC``. The writer inserts 500
    rows in random order. Every data file must be physically sorted by ``name`` (read back
    with pyarrow directly from RustFS) and record ``sort_order_id = 1`` in the manifest.
``test_target_file_size[writer]``
    Table property ``write.target-file-size-bytes = 1 MiB``; the writer generates 300 000
    rows (~8 MB of Parquet) with its own write path (``tests/lab/datagen.py``). Expected: at least
    3 data files and none larger than 2.5 MiB (writers estimate sizes before compression).

pg_lake writes only its own tables, so the sort order / file size property is set on its
table through the REST catalog (PyIceberg) after creation - whether pg_lake honours an
externally set table property is part of the result.
"""

from __future__ import annotations

import fnmatch
import random
import uuid

import datagen
import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq
import pytest
import test_operations as base
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from iceberg_lab.config import SETTINGS
from iceberg_lab.engines import pyiceberg_catalog

from conftest import ENGINES, engine_param, expect_gap

WAREHOUSE = base.WAREHOUSE
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
)
SORT = SortOrder(SortField(source_id=2, transform=IdentityTransform()))
TARGET = 1024 * 1024

# Filled from observed failures only: (engine, test, regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str]] = [
    *((e, "sort", r"is not sorted by name", f"{e} ignores the table's sort order when writing")
      for e in ("pyiceberg", "polars", "daft", "pg_lake", "flink")),
    ("pg_lake", "file_size", r"1 file\(s\) of", "pg_lake ignores write.target-file-size-bytes (uses its own pg_lake_table.target_file_size_mb, default 512 MB)"),
]
# fmt: on


def _gap(engine, test):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        test: Test name (key in ``KNOWN_GAPS``).

    Returns:
        The context manager.
    """
    for e, t, regex, reason in KNOWN_GAPS:
        if fnmatch.fnmatch(engine, e) and t == test:
            return expect_gap(reason, regex)
    return expect_gap(None, None)


def _s3fs():
    """pyarrow S3 filesystem with the root keys.

    Returns:
        The filesystem.
    """
    return pyarrow.fs.S3FileSystem(
        endpoint_override=SETTINGS.s3_endpoint,
        access_key=SETTINGS.s3_access_key,
        secret_key=SETTINGS.s3_secret_key,
        region=SETTINGS.s3_region,
        scheme="http",
    )


def _create(engine, adapter, ns, table, properties, sort_order=None):
    """Create the table with the properties (and sort order) and fill it through the engine.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.
        table: Table name (without namespace).
        properties: Table properties.
        sort_order: Sort order (None: unsorted).
    """
    cat = pyiceberg_catalog(WAREHOUSE)
    if engine == "pg_lake":
        datagen.create_pg_lake_table(adapter, ns, table)
        t = cat.load_table(f"{ns}.{table}")
        with t.transaction() as tx:
            tx.set_properties(properties)
        if sort_order is not None:
            with t.update_sort_order() as u:
                u.asc("name", IdentityTransform())
    else:
        cat.create_table(
            f"{ns}.{table}",
            schema=SCHEMA,
            properties=properties,
            **({"sort_order": sort_order} if sort_order is not None else {}),
        )


def _data_files(ns, table):
    """Data files of the current snapshot.

    Args:
        ns: Namespace.
        table: Table name.

    Returns:
        ``(path, size)`` entries.
    """
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
    f = t.inspect.files()
    return [
        (p, size, so)
        for p, size, so, c in zip(
            f["file_path"].to_pylist(),
            f["file_size_in_bytes"].to_pylist(),
            f["sort_order_id"].to_pylist(),
            f["content"].to_pylist(),
            strict=True,
        )
        if c == 0
    ]


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_sort_order_honored(engine, namespace, engine_pool):
    """With a sort order on ``name``, data files are physically sorted and marked.

    Checks:
        * data files exist;
        * the rows in each file are sorted by name;
        * the manifests record sort_order_id 1 for them.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    table = f"so_{engine}_{uuid.uuid4().hex[:6]}"
    rows = [(i, datagen.md5(i), i * 0.5) for i in range(500)]
    random.Random(7).shuffle(rows)
    with _gap(engine, "sort"):
        adapter = None if engine in ("pyiceberg", "polars", "daft") else engine_pool(engine)
        _create(engine, adapter, namespace, table, {}, SORT)
        if adapter is None:
            t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}")
            data = pa.Table.from_pylist(
                [dict(zip(("id", "name", "amount"), r, strict=True)) for r in rows],
                schema=pa.schema(
                    [("id", pa.int64()), ("name", pa.string()), ("amount", pa.float64())]
                ),
            )
            if engine == "pyiceberg":
                t.append(data)
            elif engine == "polars":
                import polars as pl

                pl.from_arrow(data).write_iceberg(t, mode="append")
            else:
                import daft

                daft.from_arrow(data).write_iceberg(t, mode="append")
        else:
            values = ", ".join(f"({i}, '{n}', {a!r}E0)" for i, n, a in rows)
            adapter.execute(f"INSERT INTO {adapter.fq(namespace, table)} VALUES {values}")
        fs = _s3fs()
        files = _data_files(namespace, table)
        assert files, "no data files"
        for path, _, so in files:
            names = pq.read_table(path.split("://", 1)[1], filesystem=fs, columns=["name"])[
                "name"
            ].to_pylist()
            assert names == sorted(names), f"{path.rsplit('/', 1)[-1]} is not sorted by name"
        sort_ids = {so for _, _, so in files}
        assert sort_ids == {1}, f"manifest sort_order_id {sort_ids} (expected 1)"


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_target_file_size(engine, namespace, engine_pool):
    """With a 1 MiB target file size, ~8 MB of data is split into files of about that size.

    Checks:
        * at least 3 data files;
        * the largest is at most 2.5 x the target.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    table = f"fs_{engine}_{uuid.uuid4().hex[:6]}"
    with _gap(engine, "file_size"):
        adapter = None if engine in ("pyiceberg", "polars", "daft") else engine_pool(engine)
        _create(engine, adapter, namespace, table, {"write.target-file-size-bytes": str(TARGET)})
        datagen.insert_generated(engine, adapter, WAREHOUSE, namespace, table, 300_000)
        files = _data_files(namespace, table)
        sizes = sorted(s for _, s, _ in files)
        assert len(files) >= 3, (
            f"{len(files)} file(s) of {[round(s / 2**20, 1) for s in sizes]} MiB"
        )
        assert sizes[-1] <= 2.5 * TARGET, f"largest file {sizes[-1] / 2**20:.1f} MiB"
