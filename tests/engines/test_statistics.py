"""Iceberg statistics: who can compute them, and who uses statistics another engine wrote.

Table statistics are Puffin files referenced from the table metadata (``statistics``); the
standard blob is ``apache-datasketches-theta-v1`` with the column's NDV (number of distinct
values) in its properties. Query planners use NDV for join ordering. Partition statistics
(``partition-statistics``) hold per-partition row/file counts.

``test_compute_table_stats[engine]``      engine computes NDV; checked in the metadata
                                          (``id`` -> 100, ``g`` -> 7 distinct values)
``test_trino_uses_table_stats[writer]``   Trino's ``SHOW STATS`` reports the writer's NDV
``test_compute_partition_stats[engine]``  per-partition statistics file in the metadata

No engine in the lab visibly consumes partition statistics files: DuckDB's
``iceberg_partition_stats`` shows the manifest-list partition summaries (one row per
manifest), not the statistics file (checked).

Engines: Spark (``CALL compute_table_stats`` / ``compute_partition_stats``), Trino
(``ANALYZE``). Checked "no API": PyIceberg (``update_statistics`` only registers an existing
Puffin file), DuckDB and Flink (no statistics statement), Polars, Daft. pg_lake's ``ANALYZE``
is tested: it collects Postgres planner statistics, the question is whether anything
reaches the Iceberg metadata.
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

from iceberg_lab.config import WAREHOUSE_STS
from iceberg_lab.engines import SPARK_CATALOGS, pyiceberg_catalog
from iceberg_lab.lab.capabilities import OFFERED

from conftest import ENGINES, engine_param, expect_gap, require, require_engine

WAREHOUSE = WAREHOUSE_STS
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "g", StringType(), required=False),
)
DATA = pa.table({"id": pa.array(range(100), pa.int64()), "g": [f"g{i % 7}" for i in range(100)]})
EXPECTED_NDV = {1: 100, 2: 7}
#: Engines that compute table statistics (capability registry).
STATS_ENGINES = sorted(OFFERED["computing table statistics"])


def _table(namespace, partitioned=False):
    """Table with 100 ids and 7 groups (optionally partitioned).

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
        partitioned: Partition by g.

    Returns:
        The table name.
    """
    cat = pyiceberg_catalog(WAREHOUSE)
    name = f"st_{uuid.uuid4().hex[:8]}"
    spec = (
        PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "g"))
        if partitioned
        else PartitionSpec()
    )
    t = cat.create_table(f"{namespace}.{name}", schema=SCHEMA, partition_spec=spec)
    t.append(DATA)
    return name


def _compute(engine, adapter, ns, name):
    """Compute table statistics with the engine's own API.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.
        name: Name of the table / object.
    """
    if engine == "spark":
        adapter.execute(
            f"CALL {SPARK_CATALOGS[WAREHOUSE]}.system.compute_table_stats(table => '{ns}.{name}')"
        )
    elif engine == "trino":
        adapter.execute(f"ANALYZE {adapter.fq(ns, name)}")
    else:  # pg_lake
        adapter._attach(ns, name)
        adapter.execute(f"ANALYZE {adapter.fq(ns, name)}")


def _ndv(ns, name) -> dict[int, int]:
    """``field id -> NDV`` from the theta blobs of the current snapshot's statistics file.

    Args:
        ns: Namespace.
        name: Table name.

    Returns:
        The NDVs.
    """
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}")
    current = t.current_snapshot().snapshot_id
    out = {}
    for s in t.metadata.statistics:
        if s.snapshot_id == current:
            for b in s.blob_metadata:
                if b.type == "apache-datasketches-theta-v1":
                    out[b.fields[0]] = int(b.properties["ndv"])
    return out


@pytest.fixture(scope="module")
def stats_tables():
    """writer -> (namespace, table) with successfully computed statistics.

    Returns:
        An empty dict.
    """
    return {}


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_compute_table_stats(engine, namespace, engine_pool, stats_tables):
    """The engine computes table statistics; the metadata then holds the correct NDVs.

    Checks:
        * the Iceberg metadata has statistics with NDV values;
        * they equal the true distinct counts (id: 100, g: 7).
        Engines without an API for it: not offered.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        stats_tables: ``writer -> (namespace, table)`` with successfully computed statistics.
    """
    require(engine, "computing table statistics", engine_pool)
    name = _table(namespace)
    gap = (
        expect_gap(
            "pg_lake ANALYZE only collects Postgres planner statistics; nothing is written to "
            "the Iceberg metadata",
            r"no statistics in the Iceberg metadata",
        )
        if engine == "pg_lake"
        else expect_gap(None, None)
    )
    with gap:
        _compute(engine, engine_pool(engine), namespace, name)
        ndv = _ndv(namespace, name)
        assert ndv, "no statistics in the Iceberg metadata"
        assert ndv == EXPECTED_NDV, ndv
    stats_tables[engine] = (namespace, name)


@pytest.mark.slow
@pytest.mark.parametrize("writer", [engine_param(e) for e in STATS_ENGINES])
def test_trino_uses_table_stats(writer, engine_pool, stats_tables):
    """Trino's ``SHOW STATS`` reports the NDVs the writer computed.

    Checks:
        * Trino reports id: 100 and g: 7 distinct values.
        Skipped (blocked) if the writer computed no statistics.

    Args:
        writer: Engine that writes the table.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        stats_tables: ``writer -> (namespace, table)`` with successfully computed statistics.
    """
    require_engine("trino")
    if writer not in stats_tables:
        pytest.skip(f"{writer} computed no statistics (see test_compute_table_stats)")
    ns, name = stats_tables[writer]
    tr = engine_pool("trino")
    rows = {r[0]: r[2] for r in tr.execute(f"SHOW STATS FOR {tr.fq(ns, name)}") if r[0]}
    assert rows == {"id": 100.0, "g": 7.0}, rows


@pytest.fixture(scope="module")
def partition_stats_tables():
    """Reserved for reads of partition statistics.

    Returns:
        An empty dict.
    """
    return {}


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_compute_partition_stats(engine, namespace, engine_pool, partition_stats_tables):
    """The engine writes a partition statistics file for the current snapshot.

    Checks:
        * the metadata lists partition statistics for the current snapshot id.
        Engines without an API for it: not offered, checked against their live API (DuckDB's
        iceberg_partition_stats only reads - conftest.NOT_OFFERED_EXEMPT).

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        partition_stats_tables: Reserved for reads of partition statistics.
    """
    require(engine, "computing partition statistics", engine_pool)
    name = _table(namespace, partitioned=True)
    engine_pool("spark").execute(
        f"CALL {SPARK_CATALOGS[WAREHOUSE]}.system.compute_partition_stats(table => '{namespace}.{name}')"
    )
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{name}")
    current = t.current_snapshot().snapshot_id
    assert any(p.snapshot_id == current for p in t.metadata.partition_statistics), (
        t.metadata.partition_statistics
    )
    partition_stats_tables[engine] = (namespace, name)
