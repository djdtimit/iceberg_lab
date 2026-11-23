"""Neutral read source for operations, partitions, evolution and maintenance (base, every reader).

The read matrices (``test_operation_cross_read``, ``test_partitioned_read``, the evolution reads,
``test_maintenance_cross_read``) only exist where some engine wrote the state successfully,
and a failing cell does not say whether the writer or the reader is wrong. Here every state is
written ONCE by a writer that is not under test - Iceberg's Java core API, the reference
implementation (``tests/lab/reference_writer.py``) - and every engine reads it:

* neutral row works, matrix cell fails -> the combination (usually the writer's files);
* neutral row fails -> the reader (or the catalog) - also where no engine could write the state.

The states are what the engines' operations leave behind, written the way the spec describes
them, with every kind of delete file:

================================  ==============================================================
operation (``test_read_operation``)
insert, overwrite, ctas           appends / copy-on-write replace / create+fill in ONE commit
delete                            copy-on-write (data file rewritten without the row)
delete_merge_on_read              v2 position delete file, v3 deletion vector (Puffin)
update                            merge-on-read: position delete / DV + the new row
merge                             equality delete (id) + new rows in one commit (Flink's upsert)
partition_evolution, rollback     new identity spec + append / rollback to the seed snapshot
transaction_multi_change          append + copy-on-write delete in one transaction (1 commit)
partition transform               PROWS under identity/bucket/truncate/year/month/day/hour,
(``test_read_partitioned``)       read with the same filters as test_partitioned_read
schema evolution                  rename/add/drop/widen between two writes
partition evolution               rows under three specs (none, day(ts), + bucket(4, id))
maintenance (``test_read_maintained``)
compact_data_files                3 data files -> 1 (RewriteFiles)
compact_delete_files              data file + delete file -> 1 data file (v3: the removed DV
                                  stays listed with status DELETED, as the spec requires)
rewrite_manifests                 3 manifests -> 1
expire_snapshots                  replaced snapshot expired, its files deleted
write after maintenance (``test_write_maintained``)
                                  the four maintained states above (own tables), then every
                                  engine appends - counterpart of test_maintenance_cross_write
file formats (``test_read_format``)
orc, avro                         ``(1, value)``, ``(2, NULL)`` in ORC / Avro data files
time zones (``test_read_tz``)
tz, ntz per zone                  the instant / plain timestamp each zone of test_timezones
                                  stands for (no session zone involved: the expected value)
================================  ==============================================================

``remove_orphan_files`` has no neutral state: removing an unreferenced file changes nothing a
reader sees. Every state is checked at the source first (the reference implementation reads it
back, the intended delete-file kinds are present) - a bad source is a lab error, not a reader's.
Format v3 natively (no upgrade trick); pg_lake reads v3 tables too.
"""

from __future__ import annotations

import time
import uuid

import pytest
import test_evolution as ev
import test_file_formats as ff
import test_maintenance as mt
import test_operations as base
import test_timezones as tz
from pyiceberg.partitioning import PartitionField, PartitionSpec
from reference_writer import ReferenceWriter

from iceberg_lab.datatypes import canonical, normalize

from conftest import ENGINES, engine_param, expect, find_gap, fresh, gap_table, require

WAREHOUSE = base.WAREHOUSE
SEED = base.SEED
FV = pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
READER = pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])

OPERATIONS = base.CONTENT_OPS
TASKS = ["compact_data_files", "compact_delete_files", "rewrite_manifests", "expire_snapshots"]

#: Delete-file kinds every state must have at the source: (content, format) per format version.
DELETE_KINDS = {
    "delete_merge_on_read": {2: {("POSITION_DELETES", "PARQUET")}, 3: {("POSITION_DELETES", "PUFFIN")}},
    "update": {2: {("POSITION_DELETES", "PARQUET")}, 3: {("POSITION_DELETES", "PUFFIN")}},
    "merge": {2: {("EQUALITY_DELETES", "PARQUET")}, 3: {("EQUALITY_DELETES", "PARQUET")}},
}  # fmt: skip


# Filled from observed failures only. Rows that apply to a case are alternatives: the first
# whose regex matches the error names the cause (gap_register.find_gap).
# fmt: off
KNOWN_GAPS = gap_table(("test", "reader", "state", "fv"), [
    *(("read_operation", r, "merge", "*", r"PyIceberg does not yet support equality deletes", "pyiceberg-no-equality-deletes")
      for r in ("pyiceberg", "polars", "daft")),
    *(("read_operation", "daft", op, "3", r"\.puffin is not a valid parquet file", "daft-dv-as-parquet")
      for op in ("delete_merge_on_read", "update")),
    *(("read_operation", "pg_lake", op, "3", r"deletion vectors are not supported", "pglake-no-dv-read")
      for op in ("delete_merge_on_read", "update")),
    ("read_maintained", "pg_lake", "compact_delete_files", "3", r"deletion vectors are not supported", "pglake-rejects-deleted-dv"),
])
# fmt: on


def _gap(test: str, reader: str, state: str, fv: int):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        test: Test name in ``KNOWN_GAPS``.
        reader: Engine that reads.
        state: State name.
        fv: Iceberg format version (2 or 3).

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, test=test, reader=reader, state=state, fv=fv))


# =========================================================================== sources


@pytest.fixture(scope="module")
def ref(spark):
    """The reference writer for the module.

    Args:
        spark: The session's SparkSession.

    Returns:
        The writer.
    """
    return ReferenceWriter(spark, WAREHOUSE)


@pytest.fixture(scope="module")
def sources():
    """(kind, state, fv) -> (namespace, table) or the exception of the source.

    Returns:
        An empty dict.
    """
    return {}


def _source(sources, key, build):
    """Build a neutral state once per session; a failure is re-raised in every reader's test.

    Args:
        sources: The session's source cache.
        key: Cache key ``(kind, state, fv)``.
        build: Callable that writes the state.

    Returns:
        ``(namespace, table)``.
    """
    if key not in sources:
        try:
            sources[key] = build()
        except Exception as e:  # noqa: BLE001 - re-raised (with gap rules) by each reader
            sources[key] = e
    if isinstance(sources[key], Exception):
        raise sources[key]
    return sources[key]


def _name(kind: str, state: str, fv: int) -> str:
    """Unique table name of a neutral state.

    Args:
        kind: Kind of state (op, pt, se, pe, mt).
        state: State name.
        fv: Iceberg format version (2 or 3).

    Returns:
        The name.
    """
    return f"ref_{kind}_{state}_v{fv}_{uuid.uuid4().hex[:6]}"


def _delete_kinds(w: ReferenceWriter, t) -> set[tuple[str, str]]:
    """``(content, format)`` of the live delete files.

    Args:
        w: The reference writer.
        t: PyIceberg table.

    Returns:
        The kinds.
    """
    _, deletes = w.live_files(t)
    return {(d.content().toString(), d.format().toString()) for d in deletes}


def _check(w: ReferenceWriter, t, expected: list[tuple]) -> None:
    """Source check: the reference implementation reads exactly the expected rows.

    Args:
        w: The reference writer.
        t: PyIceberg table.
        expected: Expected rows.
    """
    got = w.read(t)
    assert got == sorted(expected), f"source check: the reference implementation reads {got}"


# =========================================================================== operations


def _operation(w: ReferenceWriter, ns: str, op: str, fv: int) -> tuple[str, str]:
    """Write the state an operation leaves (see the module docstring) and check it.

    Args:
        w: The reference writer.
        ns: Namespace of the table.
        op: Operation.
        fv: Iceberg format version (2 or 3).

    Returns:
        ``(namespace, table)``.
    """
    name = _name("op", op, fv)
    if op == "ctas":  # create and fill in ONE commit, like CREATE TABLE AS SELECT
        txn = w.catalog.newCreateTableTransaction(
            w.ident(ns, name),
            w.ice.SchemaParser.fromJson(base.SCHEMA.model_dump_json()),
            w.ice.PartitionSpec.unpartitioned(),
            _props(w, fv),
        )
        append = txn.newAppend()
        for f in w.data_files(txn.table(), SEED[1:]):
            append.appendFile(f)
        append.commit()
        txn.commitTransaction()
        t = w.load(ns, name)
    else:
        t = w.create(ns, name, base.SCHEMA, fv)
        (seed,) = w.append(t, SEED)
        t.refresh()
        if op == "insert":
            w.append(t, [(4, "d", 4.0)])
        elif op == "overwrite":
            w.replace(t, [seed], [(9, "z", 9.0)])
        elif op == "delete":
            w.replace(t, [seed], [SEED[0], SEED[2]])
        elif op == "delete_merge_on_read":
            w.row_delta(t, [], w.position_deletes(t, seed, [1]))
        elif op == "update":
            w.row_delta(t, [(2, "b", 20.0)], w.position_deletes(t, seed, [1]))
        elif op == "merge":
            # The equality delete has the same sequence number as the new rows: it removes
            # the OLD id 2 only (spec: applies to data files with a lower sequence number).
            w.row_delta(t, [(2, "b2", 20.0), (4, "d", 4.0)], [w.equality_deletes(t, "id", [2, 4])])
        elif op == "partition_evolution":
            t.updateSpec().addField("name").commit()
            t.refresh()
            w.append(t, [(4, "d", 4.0)])
        elif op == "rollback":
            seed_snapshot = t.currentSnapshot().snapshotId()
            w.append(t, [(4, "d", 4.0)])
            t.refresh()
            t.manageSnapshots().rollbackTo(seed_snapshot).commit()
        elif op == "transaction_multi_change":
            added = w.data_files(t, [(4, "d", 4.0)])
            kept = w.data_files(t, [SEED[0], SEED[2]])
            txn = t.newTransaction()
            a = txn.newAppend()
            for f in added:
                a.appendFile(f)
            a.commit()
            o = txn.newOverwrite().deleteFile(seed)
            for f in kept:
                o.addFile(f)
            o.commit()
            txn.commitTransaction()
            t.refresh()
            assert t.currentSnapshot().parentId() is not None
        else:
            raise AssertionError(f"no neutral state for {op}")
        t.refresh()
    _check(w, t, base.EXPECTED_CONTENT[op])
    if op in DELETE_KINDS:
        kinds = _delete_kinds(w, t)
        assert kinds == DELETE_KINDS[op][fv], f"source check: delete files {kinds}"
    if op == "transaction_multi_change":
        log = base.pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}").metadata.metadata_log
        assert len(log) == 2, f"source check: {len(log)} earlier metadata files, expected 2"
    return ns, name


def _props(w: ReferenceWriter, fv: int):
    """Java map with the format version.

    Args:
        w: The reference writer.
        fv: Iceberg format version (2 or 3).

    Returns:
        The map.
    """
    props = w.jvm.java.util.HashMap()
    props.put("format-version", str(fv))
    return props


@READER
@FV
@pytest.mark.parametrize("op", OPERATIONS)
def test_read_operation(op, fv, reader, namespace, engine_pool, ref, sources):
    """The reader reads the state an operation leaves, written by the reference implementation.

    Checks:
        * the source is checked first: the reference implementation reads the expected rows
          back, and the intended delete files exist (v2 position deletes, v3 deletion vectors,
          equality deletes) - a bad source is a lab error, not the reader's;
        * the reader (cache dropped) returns exactly the expected content of the operation.

    Args:
        op: Operation whose resulting state is read (see ``base.EXPECTED_CONTENT``).
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM, ``tests/lab/reference_writer.py``).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception - each
            neutral state is written once per session.
    """
    with _gap("read_operation", reader, op, fv):
        ns, table = _source(sources, ("op", op, fv), lambda: _operation(ref, namespace, op, fv))
        got = fresh(engine_pool(reader), ns, table).read(ns, table)
        assert got == base.EXPECTED_CONTENT[op], f"{reader} reads {got}"


# =========================================================================== partitions


def _partitioned(w: ReferenceWriter, ns: str, case: str, fv: int) -> tuple[str, str]:
    """Write PROWS partitioned by the transform and check it.

    Args:
        w: The reference writer.
        ns: Namespace of the table.
        case: Partition transform.
        fv: Iceberg format version (2 or 3).

    Returns:
        ``(namespace, table)``.
    """
    tr = ev.TRANSFORMS[case]
    src = ev.PSCHEMA.find_field(tr.source)
    spec = PartitionSpec(PartitionField(src.field_id, 1000, tr.transform, f"p_{case}"))
    name = _name("pt", case, fv)
    t = w.create(ns, name, ev.PSCHEMA, fv, spec)
    w.append(t, ev.PROWS)
    _check(w, t, ev.PROWS)
    data, _ = w.live_files(t)
    assert len(data) > 1, "source check: one data file - the rows were not partitioned"
    return ns, name


@READER
@FV
@pytest.mark.parametrize("case", list(ev.TRANSFORMS))
def test_read_partitioned(case, fv, reader, namespace, engine_pool, ref, sources):
    """The reader filters a table partitioned by one transform (reference writer).

    Checks:
        * source: the reference implementation reads all rows and wrote more than one file;
        * the case's filter returns exactly the expected ids, each once.

    Args:
        case: Partition transform (see ``test_evolution.TRANSFORMS``).
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM, ``tests/lab/reference_writer.py``).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception - each
            neutral state is written once per session.
    """
    tr = ev.TRANSFORMS[case]
    with _gap("read_partitioned", reader, case, fv):
        ns, table = _source(
            sources, ("pt", case, fv), lambda: _partitioned(ref, namespace, case, fv)
        )
        adapter = fresh(engine_pool(reader), ns, table)
        rows = ev.read_cols(reader, adapter, ns, table, ["id"], tr.pred)
        assert {r[0] for r in rows} == tr.ids, f"filter {tr.pred}: got {sorted(r[0] for r in rows)}"
        assert len(rows) == len(tr.ids), f"duplicates: {rows}"


# =========================================================================== evolution


def _schema_evolved(w: ReferenceWriter, ns: str, fv: int) -> tuple[str, str]:
    """Write, evolve the schema, write again; check it.

    Args:
        w: The reference writer.
        ns: Namespace of the table.
        fv: Iceberg format version (2 or 3).

    Returns:
        ``(namespace, table)``.
    """
    name = _name("se", "schema", fv)
    t = w.create(ns, name, ev.SCHEMA0, fv)
    w.append(t, ev.R1)
    types = w.ice.types.Types
    (
        t.updateSchema()
        .renameColumn("name", "label")
        .addColumn("extra", types.IntegerType.get())
        .deleteColumn("amount")
        .updateColumn("id", types.LongType.get())
        .commit()
    )
    t.refresh()
    w.append(t, ev.R2)
    _check(w, t, ev.EVOLVED)
    return ns, name


@READER
@FV
def test_read_schema_evolved(fv, reader, namespace, engine_pool, ref, sources):
    """The reader resolves old and new files of an evolved schema by field id (reference writer).

    Checks:
        * the reader returns exactly the evolved content (renamed, added, dropped, widened).

    Args:
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM, ``tests/lab/reference_writer.py``).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception - each
            neutral state is written once per session.
    """
    with _gap("read_schema_evolved", reader, "schema_evolution", fv):
        ns, table = _source(sources, ("se", "-", fv), lambda: _schema_evolved(ref, namespace, fv))
        adapter = fresh(engine_pool(reader), ns, table)
        got = ev.read_cols(reader, adapter, ns, table, ["id", "label", "extra"])
        assert got == ev.EVOLVED, f"{reader} reads {got}"


def _spec_evolved(w: ReferenceWriter, ns: str, fv: int) -> tuple[str, str]:
    """Write rows under three partition specs; check it.

    Args:
        w: The reference writer.
        ns: Namespace of the table.
        fv: Iceberg format version (2 or 3).

    Returns:
        ``(namespace, table)``.
    """
    name = _name("pe", "spec", fv)
    t = w.create(ns, name, ev.PSCHEMA, fv)
    expr = w.ice.expressions.Expressions
    w.append(t, ev.PROWS[:3])
    t.refresh()
    t.updateSpec().addField("ts_day", expr.day("ts")).commit()
    t.refresh()
    w.append(t, ev.PROWS[3:5])
    t.refresh()
    t.updateSpec().addField("id_bucket", expr.bucket("id", 4)).commit()
    t.refresh()
    w.append(t, ev.PROWS[5:])
    _check(w, t, ev.PROWS)
    data, _ = w.live_files(t)
    assert {f.specId() for f in data} == {0, 1, 2}, "source check: data files not under 3 specs"
    return ns, name


@READER
@FV
def test_read_partition_evolved(fv, reader, namespace, engine_pool, ref, sources):
    """The reader combines files of three partition specs (reference writer).

    Checks:
        * source: data files under spec ids 0, 1 and 2;
        * reading all rows returns ids 1..6; the ``ts`` filter returns exactly ids 4 and 5.

    Args:
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM, ``tests/lab/reference_writer.py``).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception - each
            neutral state is written once per session.
    """
    with _gap("read_partition_evolved", reader, "partition_evolution", fv):
        ns, table = _source(sources, ("pe", "-", fv), lambda: _spec_evolved(ref, namespace, fv))
        adapter = fresh(engine_pool(reader), ns, table)
        all_ids = {r[0] for r in ev.read_cols(reader, adapter, ns, table, ["id"])}
        assert all_ids == {1, 2, 3, 4, 5, 6}, f"all rows: {sorted(all_ids)}"
        rows = ev.read_cols(reader, adapter, ns, table, ["id"], ev.PE_PRED)
        assert {r[0] for r in rows} == ev.PE_IDS and len(rows) == len(ev.PE_IDS), rows


# =========================================================================== maintenance


class _Cluster:
    """java.util.function.Function: every manifest into one cluster (rewriteManifests)."""

    def apply(self, data_file):
        """One cluster for every manifest entry.

        Args:
            data_file: Java DataFile (unused).

        Returns:
            The cluster key ``all``.
        """
        return "all"

    class Java:
        """py4j: the Java interface this Python object implements."""

        implements = ["java.util.function.Function"]  # noqa: RUF012 - py4j protocol


def _maintained(w: ReferenceWriter, ns: str, task: str, fv: int) -> tuple[str, str]:
    """Write the state a maintenance task leaves and check it.

    Args:
        w: The reference writer.
        ns: Namespace of the table.
        task: Maintenance task.
        fv: Iceberg format version (2 or 3).

    Returns:
        ``(namespace, table)``.
    """
    name = _name("mt", task, fv)
    t = w.create(ns, name, base.SCHEMA, fv)
    if task in ("compact_data_files", "rewrite_manifests"):
        files = w.append(t, SEED) + w.append(t, [(4, "d", 4.0)]) + w.append(t, [(5, "e", 5.0)])
        t.refresh()
        if task == "compact_data_files":
            rewrite = t.newRewrite()
            for f in files:
                rewrite.deleteFile(f)
            for f in w.data_files(t, mt.EXPECTED[task]):
                rewrite.addFile(f)
            rewrite.commit()
        else:
            from pyspark.java_gateway import ensure_callback_server_started

            ensure_callback_server_started(w.gw)
            t.rewriteManifests().clusterBy(_Cluster()).commit()
    elif task == "compact_delete_files":
        w.append(t, SEED)
        (bulk,) = w.append(t, mt.BULK)
        t.refresh()
        deletes = w.position_deletes(t, bulk, [50 - mt.BULK[0][0]])  # the row with id 50
        w.row_delta(t, [], deletes)
        t.refresh()
        # validateFromSnapshot: the delete file above is known to this rewrite (it removes it).
        rewrite = t.newRewrite().validateFromSnapshot(t.currentSnapshot().snapshotId())
        rewrite.deleteFile(bulk)
        for d in deletes:
            rewrite.deleteFile(d)
        for f in w.data_files(t, [r for r in mt.BULK if r[0] != 50]):
            rewrite.addFile(f)
        rewrite.commit()
    elif task == "expire_snapshots":
        (seed,) = w.append(t, SEED)
        t.refresh()
        old = t.currentSnapshot().snapshotId()
        w.replace(t, [seed], [(7, "g", 7.0)])
        t.refresh()
        time.sleep(0.01)
        t.expireSnapshots().expireOlderThan(int(time.time() * 1000)).retainLast(1).commit()
        t.refresh()
        assert old not in {s.snapshotId() for s in t.snapshots()}, "source check: not expired"
    else:
        raise AssertionError(f"no neutral state for {task}")
    t.refresh()
    _check(w, t, mt.EXPECTED[task])
    data, deletes = w.live_files(t)
    pt = base.pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}")
    if task == "compact_data_files":
        assert len(data) == 1, f"source check: {len(data)} data files"
    elif task == "compact_delete_files":
        assert len(data) == 2 and not deletes, f"source check: {len(data)} data, {deletes}"
    elif task == "rewrite_manifests":
        assert len(pt.inspect.manifests()) == 1, "source check: manifests not rewritten"
    return ns, name


@READER
@FV
@pytest.mark.parametrize("task", TASKS)
def test_read_maintained(task, fv, reader, namespace, engine_pool, ref, sources):
    """The reader reads a table after maintenance by the reference implementation.

    Checks:
        * source: the task's effect (1 data file / no delete files left / 1 manifest / old
          snapshot expired);
        * the reader returns exactly the content the task must leave.

    Args:
        task: Maintenance task whose result is read.
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM, ``tests/lab/reference_writer.py``).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception - each
            neutral state is written once per session.
    """
    with _gap("read_maintained", reader, task, fv):
        ns, table = _source(
            sources, ("mt", task, fv), lambda: _maintained(ref, namespace, task, fv)
        )
        got = fresh(engine_pool(reader), ns, table).read(ns, table)
        assert got == mt.EXPECTED[task], f"{reader} reads {got}"


#: The neutral source's name in the gap tables of the matrices (writer / engine column).
REF = "ref"


@pytest.mark.slow
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
@FV
@pytest.mark.parametrize("task", TASKS)
def test_write_maintained(task, fv, writer, namespace, engine_pool, ref, sources):
    """The writer appends to a table after maintenance by the reference implementation.

    The counterpart of ``test_maintenance_cross_write``, which only has tables where an engine's
    maintenance worked. One table per task and version (not the readers' tables): each writer
    appends its own row (id 100 + its position in ``ENGINES``), so writers do not disturb each
    other's checks.

    Checks:
        * the append works on the maintained table;
        * the lab client then reads every row the task left, the writer's row exactly once, no
          duplicated and no other unexpected row.

    Args:
        task: Maintenance task whose result is written to.
        fv: Iceberg format version of the table (2 or 3).
        writer: Engine that appends.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception.
    """
    require(writer, "writing tables it did not create", engine_pool)
    n = 100 + ENGINES.index(writer)
    row = (n, f"w{n}", float(n))
    with expect(mt._cross_write_gap(REF, writer, task, fv)):
        ns, table = _source(
            sources, ("mtw", task, fv), lambda: _maintained(ref, namespace, task, fv)
        )
        ctx = base.Ctx(base.pyiceberg_catalog(WAREHOUSE), ns, table, fv, seed_writer=writer)
        if writer not in ("pyiceberg", "polars", "daft"):
            fresh(engine_pool(writer), ns, table)
        base.append_rows(ctx, engine_pool, [row])
        got = fresh(engine_pool("pyiceberg"), ns, table).read(ns, table)
        missing = [r for r in mt.EXPECTED[task] if r not in got]
        assert not missing, f"rows the task left are gone after {writer}'s append: {missing}"
        assert got.count(row) == 1, f"{writer}'s row appears {got.count(row)} times"
        assert len(got) == len(set(got)), (
            f"duplicated rows: {sorted(r for r in got if got.count(r) > 1)}"
        )
        unexpected = [r for r in got if r not in mt.EXPECTED[task] and r[0] < 100]
        assert not unexpected, f"rows that should not be there: {unexpected}"


def _format_source(w: ReferenceWriter, ns: str, fmt: str) -> tuple[str, str]:
    """Table ``(id, v string)`` with ``(1, value)``, ``(2, NULL)`` in ``fmt`` data files.

    Args:
        w: The reference writer.
        ns: Namespace of the table.
        fmt: ``orc`` or ``avro``.

    Returns:
        ``(namespace, table)``.
    """
    name = _name("ff", fmt, 2)
    w.write_values(ns, name, "string", 2, [(1, canonical(ff.CASE)), (2, None)], fmt=fmt.upper())
    found = ff._formats(ns, name)
    assert found == {fmt}, f"source check: {sorted(found)} data files"
    return ns, name


@READER
@pytest.mark.parametrize("fmt", ff.FORMATS)
def test_read_format(fmt, reader, namespace, engine_pool, ref, sources):
    """The reader reads ORC / Avro data files the reference implementation wrote.

    The counterpart of ``test_file_formats.test_read_format``, whose cells exist only where an
    engine wrote that format (most write Parquet only).

    Checks:
        * source: every data file has the format;
        * the reader returns exactly the value and the NULL row.

    Args:
        fmt: Data file format (``orc``, ``avro``).
        reader: Engine that reads it.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception.
    """
    with ff._gap("read", f"{REF}>{reader}", fmt):
        ns, table = _source(sources, ("ff", fmt, 2), lambda: _format_source(ref, namespace, fmt))
        got = [
            (i, normalize(ff.CASE, v))
            for i, v in fresh(engine_pool(reader), ns, table).select(ns, table, ff.CASE)
        ]
        assert got == [(1, canonical(ff.CASE)), (2, None)]


def _tz_source(w: ReferenceWriter, ns: str, kind: str, zone: str) -> tuple[str, str]:
    """Table ``(id, v timestamp[tz])`` with the value a zone of test_timezones stands for.

    Args:
        w: The reference writer.
        ns: Namespace of the table.
        kind: ``tz`` or ``ntz``.
        zone: Key of ``test_timezones.ZONES``.

    Returns:
        ``(namespace, table)``.
    """
    case = tz._case(kind, zone, None)
    name = _name("tz", f"{kind}_{zone}", 2)
    type_json = "timestamptz" if kind == "tz" else "timestamp"
    w.write_values(ns, name, type_json, 2, [(1, case.value), (2, None)])
    return ns, name


@READER
@pytest.mark.parametrize("zone", list(tz.ZONES))
@pytest.mark.parametrize("kind", tz.KINDS)
def test_read_tz(kind, zone, reader, namespace, engine_pool, ref, sources):
    """The reader returns a timestamp(tz) the reference implementation wrote, unshifted.

    The counterpart of ``test_timezones.test_tz_read``: the same values (an offset of no whole
    hour, a time right after a DST switch, a date before 1970), stored without any session zone.

    Checks:
        * ``tz``: the instant; ``ntz``: the plain timestamp; row 2 stays NULL.

    Args:
        kind: ``tz`` (timestamptz) or ``ntz`` (timestamp).
        zone: Key of ``test_timezones.ZONES``.
        reader: Engine that reads (in a UTC session).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        ref: The reference writer (Iceberg Java core via PySpark's JVM).
        sources: ``(kind, state, fv) -> (namespace, table)`` or the source's exception.
    """
    case = tz._case(kind, zone, None)
    with tz._gap(REF, reader, kind, zone):
        ns, table = _source(
            sources, ("tz", f"{kind}_{zone}", 2), lambda: _tz_source(ref, namespace, kind, zone)
        )
        got = [
            (i, normalize(case, v))
            for i, v in fresh(engine_pool(reader), ns, table).select(ns, table, case)
        ]
        assert got == [(1, canonical(case)), (2, None)]


def test_states_cover_the_matrices():
    """Guard: every state of the read matrices has a neutral source (or a reason why not).

    Checks:
        * the operation states equal the operation matrix's content states;
        * the maintenance states plus remove_orphan_files (no reader-visible state) equal the
          maintenance matrix's (read and write after maintenance);
        * the file formats and the time zone cells equal those of their matrices (the
          parametrization reads them from there, so this guards against a local copy).
    """
    assert set(OPERATIONS) == set(base.EXPECTED_CONTENT)
    assert set(TASKS) | {"remove_orphan_files"} == set(mt.EXPECTED)
    marks = {m.args[0]: m.args[1] for m in getattr(test_read_tz, "pytestmark", [])}
    assert marks.get("zone") == list(tz.ZONES) and marks.get("kind") == tz.KINDS
    fmts = {m.args[0]: m.args[1] for m in getattr(test_read_format, "pytestmark", [])}
    assert fmts.get("fmt") == ff.FORMATS
