"""Drop and re-create under the same name while an engine has the table open (base, every engine).

Each engine keeps its session (the shared adapter: one connection/SparkSession/catalog client per
engine and run). The engine reads table T; then another client drops T with purge and creates a
new T under the same name with other rows. Without any refresh by the test:

``test_reads_the_recreated_table``
    the engine's next read returns the new table's rows (not the dropped table's, not an error
    about its deleted files);
``test_writes_the_recreated_table``
    the engine's next append lands in the new table - not in the dropped table's location
    (apache/iceberg#18623: Flink's dynamic sink kept writing to the dropped table's location
    because its writer caches are keyed by table name).

Engines may cache table metadata on purpose (Spark: 30 s by default); such a cache showing the
dropped table is recorded as a gap with that reason.
"""

from __future__ import annotations

import uuid

import pytest
import test_concurrency as cc
import test_operations as base

from iceberg_lab.engines import pyiceberg_catalog

from conftest import ENGINES, engine_param, expect_gap, require_engine

WAREHOUSE = base.WAREHOUSE
OLD = [(1, "old", 1.0)]
NEW = [(2, "new", 2.0)]

# Filled from observed results only: (test, engine, regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str]] = [
    ("reads_the_recreated_table", "spark", r"spark read \[\(1, 'old', 1\.0\)\] after the re-create|Location does not exist: s3://\S+\.avro", "WRONG RESULT: Spark's catalog cache (cache-enabled, 30 s) still holds the dropped table after a re-create under the same name: Spark reads its old rows, or fails on its deleted files once they are purged"),
    ("writes_the_recreated_table", "spark", r"Table UUID does not match: current=\S+ != refreshed=", "Spark refuses to write to a table re-created under the same name while its catalog cache holds the dropped one ('Table UUID does not match') - safe, but the new table is not writable until the cache expires (30 s)"),
    *((t, "flink", r"Table UUID does not match: current=\S+ != refreshed=", f"Flink refuses to {w} a table re-created under the same name while its catalog cache holds the dropped one ('Table UUID does not match') - safe, but not usable until the cache expires")
      for t, w in (("reads_the_recreated_table", "read"), ("writes_the_recreated_table", "write"))),
]
# fmt: on


def _gap(test: str, engine: str):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        test: Test name (key in ``KNOWN_GAPS``).
        engine: Engine name (see ``conftest.ENGINES``).

    Returns:
        The context manager.
    """
    for t, e, regex, reason in KNOWN_GAPS:
        if t == test and e == engine:
            return expect_gap(reason, regex)
    return expect_gap(None, None)


def _drop(cat, ident: str) -> None:
    """Purge, else drop (RustFS has no purge).

    Args:
        cat: PyIceberg catalog.
        ident: Table identifier ``namespace.table``.
    """
    try:
        cat.purge_table(ident)
    except Exception:  # noqa: BLE001 - catalogs without purge (RustFS: 406)
        cat.drop_table(ident)


def _open_then_recreate(engine: str, namespace: str, engine_pool) -> tuple[str, str]:
    """The engine reads T (OLD); T is dropped and created again with NEW. Returns
    (table name, location of the dropped table).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        ``(table name, location of the dropped table)``.
    """
    require_engine(engine)
    name = f"rs_{engine}_{uuid.uuid4().hex[:6]}"
    ident = f"{namespace}.{name}"
    cat = pyiceberg_catalog(WAREHOUSE)
    t = cat.create_table(ident, schema=base.SCHEMA)
    t.append(base.arrow(OLD))
    adapter = engine_pool(engine)
    adapter.refresh(namespace, name)  # the session starts from the current table ...
    assert sorted(adapter.read(namespace, name)) == OLD, "setup: first read"
    old_location = t.location()
    _drop(cat, ident)  # ... then it changes behind the session's back
    cat.create_table(ident, schema=base.SCHEMA).append(base.arrow(NEW))
    return name, old_location


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_reads_the_recreated_table(engine, namespace, engine_pool):
    """The engine reads T, T is dropped and re-created with other rows; the next read shows the new T.

    The engine keeps its session; no refresh by the test.

    Checks:
        * the engine's next read returns exactly the new table's rows - not the dropped table's
          rows, not an error about its deleted files (Spark's 30 s catalog cache: documented
          wrong-result gap).

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    name, _ = _open_then_recreate(engine, namespace, engine_pool)
    with _gap("reads_the_recreated_table", engine):
        got = sorted(engine_pool(engine).read(namespace, name))
        assert got == NEW, f"{engine} read {got} after the re-create - the new table has {NEW}"


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_writes_the_recreated_table(engine, namespace, engine_pool):
    """After drop and re-create under the same name, the engine's append lands in the NEW table.

    Checks:
        * the new table holds its rows plus the appended row;
        * no data file of it lies in the dropped table's location (apache/iceberg#18623: Flink
          kept writing there).
        pg_lake: not offered (cannot write tables it did not create).

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    if engine == "pg_lake":
        pytest.skip("pg_lake has no API for writing tables it did not create (read-only attach)")
    name, old_location = _open_then_recreate(engine, namespace, engine_pool)
    with _gap("writes_the_recreated_table", engine):
        adapter = engine_pool(engine)
        cc.append_one(engine, adapter, namespace, name, (3, "w", 3.0))
        t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{name}")
        got = sorted(base.arrow_rows(t.scan().to_arrow()))
        assert got == sorted([*NEW, (3, "w", 3.0)]), f"new table after {engine}'s append: {got}"
        if t.location().rstrip("/") != old_location.rstrip("/"):
            files = [f for f in t.inspect.files().column("file_path").to_pylist()]
            stray = [f for f in files if f.startswith(old_location.rstrip("/") + "/")]
            assert not stray, f"{engine} wrote into the dropped table's location: {stray}"
