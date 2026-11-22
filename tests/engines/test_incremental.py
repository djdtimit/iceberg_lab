"""Incremental reads, changelogs and streaming per engine.

Setup (format v2, written by PyIceberg so every reader gets the same snapshots):
``S1`` appends ids 1-3, ``S2`` appends 4-5, ``S3`` deletes id 2.

``test_incremental_append_read[engine]``  rows appended in (S1, S2] -> {4, 5}
    PyIceberg ``incremental_append_scan``, Polars ``scan_iceberg(from_snapshot_id_exclusive=,
    to_snapshot_id_inclusive=)``, Spark ``start-/end-snapshot-id`` read options,
    Flink ``OPTIONS('start-snapshot-id', 'end-snapshot-id')``, Trino ``table_changes``.
``test_changelog_read[engine]``           net changes in (S2, S3] -> id 2 deleted
    Spark ``create_changelog_view(net_changes => true)``, Trino ``table_changes``.
``test_streaming_write[engine]``          a streaming job writes 500 rows with checkpoints
    Flink (streaming mode, checkpoint every 2 s, bounded datagen source); Spark Structured
    Streaming (``availableNow`` trigger from an Iceberg source table).
``test_streaming_read[engine]``           a streaming reader picks up rows appended later
    Flink (``OPTIONS('streaming'='true')``, unbounded query polled through the gateway);
    Spark (``readStream``, two ``availableNow`` runs on one checkpoint).

Polars streams with ``LazyFrame.sink_iceberg``; whether that commits like a streaming job is
what ``test_streaming_write`` checks. Every other "not offered" is checked at run time against
the engine's live API (``iceberg_lab.lab.capabilities.require``): a matching new function fails the test
instead of hiding behind the skip.
"""

from __future__ import annotations

import fnmatch
import time
import uuid

import pyarrow as pa
import pytest
import test_operations as base

from iceberg_lab.engines import (
    SPARK_CATALOGS,
    FlinkEngine,
    polars_storage_options,
    pyiceberg_catalog,
)

from conftest import ENGINES, engine_param, expect_gap, require, require_engine

WAREHOUSE = base.WAREHOUSE


# Filled from observed failures only: (engine, test, regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str]] = [
    ("polars", "streaming_write", r"expected several streaming commits", "Polars 2.1 sink_iceberg(engine='streaming') writes all 500 rows in one commit at the end - a batch write through the streaming engine, no incremental commits"),
    ("trino", "changelog", r"\(1, 'delete'\)", "Trino table_changes reports copy-on-write carry-over rows as delete + insert (no net changes)"),
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


@pytest.fixture(scope="module")
def history(namespace):
    """``(table name, S1, S2, S3)``.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(table, S1, S2, S3)``.
    """
    cat = pyiceberg_catalog(WAREHOUSE)
    name = f"inc_{uuid.uuid4().hex[:6]}"
    t = cat.create_table(f"{namespace}.{name}", schema=base.SCHEMA)
    t.append(base.arrow(base.SEED))
    s1 = t.current_snapshot().snapshot_id
    t.append(base.arrow([(4, "d", 4.0), (5, "e", 5.0)]))
    s2 = t.current_snapshot().snapshot_id
    t.delete("id == 2")
    s3 = t.refresh().current_snapshot().snapshot_id
    return name, s1, s2, s3


# =========================================================================== incremental


def incremental_ids(engine, adapter, ns, name, s_from, s_to) -> list[int]:
    """Ids appended between two snapshots, read incrementally by the engine.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.
        name: Table name.
        s_from: Start snapshot (exclusive).
        s_to: End snapshot (inclusive).

    Returns:
        Sorted ids.
    """
    if engine == "pyiceberg":
        t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}")
        scan = t.incremental_append_scan(
            from_snapshot_id_exclusive=s_from, to_snapshot_id_inclusive=s_to
        )
        return sorted(scan.to_arrow()["id"].to_pylist())
    if engine == "polars":
        # Polars 2.x: scan_iceberg(from_snapshot_id_exclusive=, to_snapshot_id_inclusive=) - found
        # by the not_offered check, which had claimed "no incremental reads".
        import polars as pl

        t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}")
        lf = pl.scan_iceberg(
            t,
            from_snapshot_id_exclusive=s_from,
            to_snapshot_id_inclusive=s_to,
            storage_options=polars_storage_options(t.io.properties),
        )
        return sorted(lf.select("id").collect()["id"].to_list())
    if engine == "spark":
        df = (
            adapter.spark.read.format("iceberg")
            .option("start-snapshot-id", str(s_from))
            .option("end-snapshot-id", str(s_to))
            .load(adapter.fq(ns, name))
        )
        return sorted(r["id"] for r in df.select("id").collect())
    if engine == "flink":
        rows = adapter.execute(
            f"SELECT id FROM {adapter.fq(ns, name)} "
            f"/*+ OPTIONS('start-snapshot-id'='{s_from}', 'end-snapshot-id'='{s_to}') */"
        )
        return sorted(r[0] for r in rows)
    if engine == "trino":
        rows = adapter.execute(
            f"SELECT id FROM TABLE(system.table_changes('{ns}', '{name}', {s_from}, {s_to})) "
            f"WHERE _change_type = 'insert'"
        )
        return sorted(r[0] for r in rows)
    raise ValueError(engine)


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_incremental_append_read(engine, history, namespace, engine_pool):
    """An incremental read returns only the rows appended between S1 (exclusive) and S2.

    Checks:
        * the engine's incremental read S1 -> S2 returns exactly ids 4 and 5.
        Engines without incremental reads: not offered.

    Args:
        engine: Engine under test.
        history: ``(table, S1, S2, S3)`` - S1: rows 1-3, S2: rows 4-5 appended, S3: row 2 deleted.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require(engine, "incremental reads", engine_pool)
    name, s1, s2, _ = history
    with _gap(engine, "incremental"):
        adapter = None if engine in ("pyiceberg", "polars") else engine_pool(engine)
        assert incremental_ids(engine, adapter, namespace, name, s1, s2) == [4, 5]


def changelog(engine, adapter, ns, name, s_from, s_to) -> list[tuple]:
    """Net changes between two snapshots, read by the engine.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.
        name: Table name.
        s_from: Start snapshot (exclusive).
        s_to: End snapshot (inclusive).

    Returns:
        ``(id, change)`` tuples.
    """
    if engine == "spark":
        cat = SPARK_CATALOGS[WAREHOUSE]
        view = f"chg_{uuid.uuid4().hex[:6]}"
        adapter.execute(
            f"CALL {cat}.system.create_changelog_view(table => '{ns}.{name}', "
            f"options => map('start-snapshot-id', '{s_from}', 'end-snapshot-id', '{s_to}'), "
            f"net_changes => true, changelog_view => '{view}')"
        )
        return sorted(
            (r[0], r[1].lower()) for r in adapter.execute(f"SELECT id, _change_type FROM {view}")
        )
    if engine == "trino":
        rows = adapter.execute(
            f"SELECT id, _change_type FROM TABLE(system.table_changes('{ns}', '{name}', {s_from}, {s_to}))"
        )
        return sorted((r[0], r[1].lower()) for r in rows)
    raise ValueError(engine)


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_changelog_read(engine, history, namespace, engine_pool):
    """The net changes between S2 and S3 are exactly "id 2 deleted".

    Checks:
        * the engine's changelog read S2 -> S3 returns exactly ``(2, delete)``.
        Engines without changelog reads: not offered.

    Args:
        engine: Engine under test.
        history: ``(table, S1, S2, S3)`` - S1: rows 1-3, S2: rows 4-5 appended, S3: row 2 deleted.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require(engine, "changelog reads", engine_pool)
    name, _, s2, s3 = history
    with _gap(engine, "changelog"):
        assert changelog(engine, engine_pool(engine), namespace, name, s2, s3) == [(2, "delete")]


# =========================================================================== streaming


def _flink_streaming():
    """A new Flink session switched to streaming mode with 2 s checkpoints.

    Returns:
        The Flink adapter (streaming mode).
    """
    f = FlinkEngine(WAREHOUSE)
    f.execute("SET 'execution.runtime-mode' = 'streaming'")
    f.execute("SET 'execution.checkpointing.interval' = '2s'")
    return f


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_streaming_write(engine, namespace, engine_pool, tmp_path):
    """A streaming job writes 500 rows; the result is committed in several snapshots.

    Checks:
        * the target table has all 500 rows;
        * it has at least two snapshots (streaming commits, not one batch).
        Engines without streaming writes: not offered.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        tmp_path: Temporary directory (streaming checkpoints).
    """
    require(engine, "streaming writes", engine_pool)
    cat = pyiceberg_catalog(WAREHOUSE)
    name = f"sw_{engine}_{uuid.uuid4().hex[:6]}"
    n = 500
    with _gap(engine, "streaming_write"):
        if engine == "polars":
            # LazyFrame.sink_iceberg: Polars' streaming-engine sink. Whether it commits like a
            # streaming job (several snapshots) or once like a batch write decides the check below.
            import polars as pl

            t = cat.create_table(f"{namespace}.{name}", schema=base.SCHEMA)
            pl.LazyFrame(
                {"id": list(range(1, n + 1)), "name": [str(i) for i in range(1, n + 1)],
                 "amount": [float(i) for i in range(1, n + 1)]},
                schema={"id": pl.Int32, "name": pl.String, "amount": pl.Float64},
            ).sink_iceberg(
                t, mode="append", engine="streaming",
                storage_options=polars_storage_options(t.io.properties),
            )  # fmt: skip
        elif engine == "flink":
            f = _flink_streaming()
            try:
                gen = f"default_catalog.default_database.gen_{name}"
                f.execute(
                    f"CREATE TEMPORARY TABLE {gen} (i BIGINT) WITH ('connector' = 'datagen', "
                    f"'number-of-rows' = '{n}', 'rows-per-second' = '100', "
                    f"'fields.i.kind' = 'sequence', 'fields.i.start' = '1', 'fields.i.end' = '{n}')"
                )
                f.execute(f"CREATE TABLE {f.fq(namespace, name)} (id BIGINT, name STRING)")
                f.execute(
                    f"INSERT INTO {f.fq(namespace, name)} SELECT i, CAST(i AS STRING) FROM {gen}"
                )
            finally:
                f.close()
        else:
            require_engine("spark")
            sp = engine_pool("spark")
            src = f"{name}_src"
            t = cat.create_table(
                f"{namespace}.{src}", schema=pa.schema([("id", pa.int64()), ("name", pa.string())])
            )
            for k in range(5):  # 5 snapshots -> several micro-batches possible
                ids = list(range(k * 100 + 1, k * 100 + 101))
                t.append(pa.table({"id": pa.array(ids, pa.int64()), "name": [str(i) for i in ids]}))
            sp.execute(
                f"CREATE TABLE {sp.fq(namespace, name)} (id BIGINT, name STRING) USING iceberg"
            )
            q = (
                sp.spark.readStream.format("iceberg")
                .option("streaming-max-files-per-micro-batch", "1")
                .load(sp.fq(namespace, src))
                .writeStream.format("iceberg")
                .outputMode("append")
                .option("checkpointLocation", str(tmp_path / "ckpt"))
                .trigger(availableNow=True)
                .toTable(sp.fq(namespace, name))
            )
            q.awaitTermination(300)
        out = cat.load_table(f"{namespace}.{name}")
        assert out.scan().to_arrow().num_rows == n
        assert len(out.snapshots()) >= 2, "expected several streaming commits"


def _flink_stream_ids(f, sql, want: int, timeout: float, after_first=None) -> list[int]:
    """Run an unbounded Flink query and collect ids until ``want`` rows arrived.

    Args:
        f: Flink adapter in streaming mode.
        sql: SQL statement.
        want: Rows to wait for.
        timeout: Seconds to wait at most.
        after_first: Callable run after the first row arrived.

    Returns:
        The ids.
    """
    r = f._http.post(
        f"{f._base}/v1/sessions/{f._session}/statements", json={"statement": sql}, timeout=60
    )
    r.raise_for_status()
    op = r.json()["operationHandle"]
    url = f"{f._base}/v1/sessions/{f._session}/operations/{op}/result/0?rowFormat=JSON"
    ids, fired, deadline = [], False, time.monotonic() + timeout
    try:
        while url and time.monotonic() < deadline and len(ids) < want:
            res = f._http.get(url, timeout=60)
            if not res.ok:
                raise RuntimeError(res.text[:300])
            d = res.json()
            ids += [x["fields"][0] for x in d.get("results", {}).get("data", [])]
            if ids and not fired and after_first:
                after_first()
                fired = True
            nxt = d.get("nextResultUri")
            url = f"{f._base}{nxt}" if nxt else None
            time.sleep(0.5)
    finally:
        f._http.delete(f"{f._base}/v1/sessions/{f._session}/operations/{op}/cancel", timeout=30)
    return sorted(ids)


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_streaming_read(engine, namespace, engine_pool, tmp_path):
    """A streaming read sees the seed rows, then only rows appended afterwards.

    Checks:
        * the first run reads ids 1-3;
        * after an append, the next run reads only the new ids 4 and 5;
        * together exactly 1-5.
        Engines without streaming reads: not offered.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        tmp_path: Temporary directory (streaming checkpoints).
    """
    require(engine, "streaming reads", engine_pool)
    cat = pyiceberg_catalog(WAREHOUSE)
    name = f"sr_{engine}_{uuid.uuid4().hex[:6]}"
    t = cat.create_table(f"{namespace}.{name}", schema=base.SCHEMA)
    t.append(base.arrow(base.SEED))

    def append_more():
        """Append ids 4 and 5 while the stream runs."""
        cat.load_table(f"{namespace}.{name}").append(base.arrow([(4, "d", 4.0), (5, "e", 5.0)]))

    with _gap(engine, "streaming_read"):
        if engine == "flink":
            f = _flink_streaming()
            try:
                ids = _flink_stream_ids(
                    f,
                    f"SELECT id FROM {f.fq(namespace, name)} "
                    f"/*+ OPTIONS('streaming'='true', 'monitor-interval'='1s') */",
                    want=5,
                    timeout=120,
                    after_first=append_more,
                )
            finally:
                f.close()
        else:
            sp = engine_pool("spark")
            ckpt = str(tmp_path / "ckpt")
            ids = []
            for k in range(2):
                batch: list[int] = []
                # foreachBatch supports restarting from the checkpoint (the memory sink does not)
                q = (
                    sp.spark.readStream.format("iceberg")
                    .load(sp.fq(namespace, name))
                    .writeStream.foreachBatch(
                        lambda df, _id, out=batch: out.extend(
                            r["id"] for r in df.select("id").collect()
                        )
                    )
                    .option("checkpointLocation", ckpt)
                    .trigger(availableNow=True)
                    .start()
                )
                q.awaitTermination(300)
                batch.sort()
                ids += batch
                if k == 0:
                    assert batch == [1, 2, 3], f"first run: {batch}"
                    append_more()
            assert ids[3:] == [4, 5], f"second run must read only the new rows: {ids}"
        assert ids == [1, 2, 3, 4, 5], ids
