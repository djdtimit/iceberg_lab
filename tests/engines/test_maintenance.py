"""Table maintenance matrix: the same maintenance tasks for every engine, format v2 and v3.

Every task follows the same three steps, for every engine:

1. **Setup through the engine itself** (as in the operation matrix): the engine creates the
   precondition with its own write path - small files, delete files, an old snapshot whose
   data file was replaced. If it cannot (PyIceberg cannot write v3 data, Flink cannot
   DELETE), the test fails with that documented gap; no other engine steps in.
2. **The engine's own maintenance API** runs the task.
3. **Shared verification**, identical for all engines, in this order:
   * the task's effect in the metadata (via PyIceberg): e.g. one live data file left, no
     delete files, one manifest, the old snapshot is gone;
   * content: every file the table still references exists in storage, and the engine reads
     the expected rows - from here on the table goes to the cross-read even if a later check
     fails (``iceberg_lab.lab.results.keep_if_content_ok``);
   * the task's effect in storage (boto3 directly against RustFS): e.g. the expired
     snapshot's data file and manifest list are really deleted, the planted orphan is gone,
     nothing is left after DROP;
   * afterwards every OTHER engine reads the result (``test_maintenance_cross_read``); the
     same states written by the reference implementation: tests/engines/test_neutral_source.py;
   * and every engine, the maintaining one included, appends a row to the maintained table
     (``test_maintenance_cross_write``): writing must still work after another engine's
     compaction, manifest rewrite, snapshot expiry or orphan cleanup.

Tasks (``TASKS``):

========================  ================================================================
compact_data_files        3 small data files -> 1
compact_delete_files      a DELETE left delete files / deletion vectors -> none remain
rewrite_manifests         3 manifests -> 1
expire_snapshots          the snapshot whose data file was replaced is removed - metadata
                          AND files in storage
remove_orphan_files       a file in the table directory that no snapshot references
                          (planted via boto3) is deleted, live files survive
drop_table                after DROP the catalog entry and all files are gone
========================  ================================================================

An engine without an API for a task has no method for it -> "has no API" (not offered).
Every such claim was checked (see the class docstrings).
"""

from __future__ import annotations

import datetime as dt
import time
import uuid

import boto3
import pytest
import test_operations as base
from botocore.config import Config

from iceberg_lab.config import PROFILE, SETTINGS
from iceberg_lab.engines import SPARK_CATALOGS, pyiceberg_catalog

from conftest import (
    ENGINES,
    Gap,
    engine_param,
    expect,
    find_gap,
    fresh,
    gap_table,
    keep_if_content_ok,
    not_offered,
    require,
    require_format,
    skip_if_not_produced,
)

WAREHOUSE = base.WAREHOUSE
SEED = base.SEED

TASKS = [
    "compact_data_files",
    "compact_delete_files",
    "rewrite_manifests",
    "expire_snapshots",
    "remove_orphan_files",
    "drop_table",
]

#: 97 extra rows written in ONE append for compact_delete_files; the DELETE then removes one
#: of them. With only 3 rows pg_lake rewrites the file (copy-on-write) instead of writing a
#: delete file - a 1-in-97 delete gets a position delete file in every engine.
BULK = [(i, f"n{i}", float(i)) for i in range(4, 101)]

#: Table content after each task (``drop_table`` leaves no table).
EXPECTED: dict[str, list[tuple]] = {
    "compact_data_files": SEED + [(4, "d", 4.0), (5, "e", 5.0)],
    "compact_delete_files": SEED + [r for r in BULK if r[0] != 50],
    "rewrite_manifests": SEED + [(4, "d", 4.0), (5, "e", 5.0)],
    "expire_snapshots": [(7, "g", 7.0)],
    "remove_orphan_files": SEED,
}

#: Properties of every setup table. merge-on-read: DELETE must leave delete files behind
#: (Spark would otherwise rewrite the data file, leaving nothing for compact_delete_files).
TABLE_PROPS = {
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}


# Filled from observed failures only. Rows that apply to a case are alternatives: the first
# whose regex matches the error names the cause (gap_register.find_gap).
# fmt: off
KNOWN_GAPS = gap_table(("engine", "task", "fv"), [
    ("pyiceberg", "expire_snapshots", "2", r"expired data file still in storage", "pyiceberg-expire-keeps-files"),
    # Only visible where the catalog deletes files on purge only (Polaris); Lakekeeper's hard
    # delete removes them anyway.
    *([("pg_lake", "drop_table", "*", r"files left after DROP", "pglake-drop-no-purge")] if PROFILE.name != "lakekeeper" else []),
    # Spark is not affected: with a REST catalog it deletes the files itself (Spark actions).
    *((e, "drop_table", "2" if e == "pyiceberg" else "*", r"files left after DROP, e\.g\. s3://unitylab/lab/__unitystorage/", "unity-delayed-file-cleanup")
      for e in ("pyiceberg", "trino", "flink") if "unity" in PROFILE.layers),
    # Not through Gravitino in front of Lakekeeper: the files are gone there (seen 2026-11-25).
    *([("duckdb", "drop_table", "*", r"files left after DROP", "duckdb-drop-no-purge")]
      if PROFILE.name != "gravitino_lakekeeper" else []),
    ("duckdb", "compact_*", "3", r"iceberg_rewrite_data_files does not yet support V3 tables", "duckdb-no-v3-rewrite"),
    # DuckDB 2.0.1 fixed "iceberg_rewrite_data_files keeps the position delete files" (v2).
    ("duckdb", "expire_snapshots", "2", r"expired data file still in storage", "duckdb-expire-keeps-files"),
    ("duckdb", "expire_snapshots", "3", r"iceberg_rewrite_data_files does not yet support V3 tables", "duckdb-no-v3-rewrite"),
    # Two intermittent symptoms (probed by hand): the old snapshot stays in the metadata, or it
    # is removed but its data file stays in storage (also after VACUUM again and
    # lake_engine.flush_deletion_queue()).
    ("pg_lake", "expire_snapshots", "*", r"(old snapshot still in metadata|expired data file still in storage)", "pglake-expiry-unreliable", True),
    ("pg_lake", "remove_orphan_files", "*", r"orphan file still there", "pglake-no-foreign-orphans"),
    ("flink", "compact_delete_files", "*", r"Can't perform delete operation", "flink-no-delete"),
    *(("flink", t, "*", r"getProcedure is not implemented for class org\.apache\.iceberg\.flink\.FlinkCatalog", "flink-no-procedures")
      for t in ("compact_data_files", "rewrite_manifests", "expire_snapshots", "remove_orphan_files")),
    # PyIceberg cannot write v3 data: the setup (its own writes) fails.
    ("pyiceberg", "*", "3", base.NO_V3_MSG, "pyiceberg-v3-data-write"),
])
# fmt: on


def known_gap(engine: str, task: str, fv: int) -> Gap | None:
    """First matching :data:`KNOWN_GAPS` entry, else ``None``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        task: Maintenance task (see ``TASKS``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The gap, or None.
    """
    return find_gap(KNOWN_GAPS, engine=engine, task=task, fv=fv)


# =========================================================================== storage helpers


def _s3():
    """boto3 client with the RustFS root keys - sees the storage independent of any engine.

    Returns:
        The boto3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=SETTINGS.s3_access_key,
        aws_secret_access_key=SETTINGS.s3_secret_key,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def _split(uri: str) -> tuple[str, str]:
    """``s3://bucket/key`` -> ``(bucket, key)``.

    Args:
        uri: S3 URI.

    Returns:
        ``(bucket, key)``.
    """
    bucket, _, key = uri.removeprefix("s3://").removeprefix("s3a://").partition("/")
    return bucket, key


def storage_files(location: str) -> set[str]:
    """All objects below a table location, as ``s3://bucket/key`` URIs.

    Args:
        location: Table location (S3 URI).

    Returns:
        The object URIs.
    """
    bucket, prefix = _split(location.rstrip("/") + "/")
    pages = _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return {f"s3://{bucket}/{o['Key']}" for p in pages for o in p.get("Contents", [])}


def _norm_uri(uri: str) -> str:
    """Normalise ``s3a://`` and other schemes to ``s3://``.

    Args:
        uri: S3 URI.

    Returns:
        The URI with scheme ``s3``.
    """
    return "s3://" + uri.split("://", 1)[1]


# =========================================================================== engines


class Maint:
    """Shared setup steps; subclasses add the engine's maintenance methods.

    Setup goes through the engine's own write path: :func:`test_operations.append_rows`
    for appends, ``delete_one``/``replace_all`` per engine.
    """

    engine_name: str

    def __init__(self, ctx: base.Ctx, pool):
        """Bind the test table and the engine's pooled adapter.

        Args:
            ctx: Context of the test table.
            pool: Engine adapter pool.
        """
        self.c = ctx
        self.pool = pool
        self.e = pool(self.engine_name) if self.engine_name not in ("pyiceberg",) else None

    def fq(self):
        """The test table in the engine's SQL.

        Returns:
            The qualified name.
        """
        return self.e.fq(self.c.ns, self.c.table)

    def q(self, sql):
        """Run SQL through the engine.

        Args:
            sql: SQL statement.

        Returns:
            The result rows.
        """
        return self.e.execute(sql)

    def seed(self):
        """Create the table with :data:`TABLE_PROPS` and write :data:`SEED` with the engine."""
        self.c.catalog.create_table(
            self.c.ident,
            schema=base.SCHEMA,
            properties={"format-version": str(self.c.fv), **TABLE_PROPS},
        )
        base.append_rows(self.c, self.pool, SEED)

    def append(self, rows):
        """Append rows with the engine's own write path (one commit).

        Args:
            rows: Rows ``(id, name, amount)`` to write.
        """
        base.append_rows(self.c, self.pool, rows)

    def values(self, rows) -> str:
        """Rows as a SQL VALUES list (DOUBLE literals for amount).

        Args:
            rows: Rows ``(id, name, amount)`` to write.

        Returns:
            The VALUES list.
        """
        return ", ".join(f"({i}, '{n}', {a!r}E0)" for i, n, a in rows)

    def delete_one(self):
        """Delete the row with id 50 (one row of the bulk append -> a delete file)."""
        self.q(f"DELETE FROM {self.fq()} WHERE id = 50")

    def replace_all(self, rows):
        """Replace the whole content so the seed data file is no longer referenced.

        Args:
            rows: Rows ``(id, name, amount)`` to write.
        """
        self.q(f"DELETE FROM {self.fq()}")
        self.q(f"INSERT INTO {self.fq()} VALUES {self.values(rows)}")

    def plant_orphan(self) -> str:
        """Put an unreferenced Parquet-named object into the table's data directory.

        Returns:
            The orphan's URI.
        """
        uri = f"{self.c.pi_table().location().rstrip('/')}/data/orphan-{uuid.uuid4().hex}.parquet"
        bucket, key = _split(uri)
        _s3().put_object(Bucket=bucket, Key=key, Body=b"PAR1 not referenced PAR1")
        return _norm_uri(uri)

    def drop_table(self):
        """``DROP TABLE`` through the engine."""
        self.q(f"DROP TABLE {self.fq()}")


class PyIcebergMaint(Maint):
    """PyIceberg 0.12: ``table.maintenance.expire_snapshots()`` and ``catalog.purge_table``.

    ``drop_table`` would send ``purgeRequested=false`` and Lakekeeper then keeps the files
    (checked); ``purge_table`` is PyIceberg's API for "drop including data".

    No API (checked: ``Table``/``MaintenanceTable`` members): data file compaction, delete
    file compaction, manifest rewrite, orphan file removal.
    """

    engine_name = "pyiceberg"

    def replace_all(self, rows):
        """Overwrite the table with the rows.

        Args:
            rows: Rows ``(id, name, amount)`` to write.
        """
        self.c.pi_table().overwrite(base.arrow(rows))

    def expire_snapshots(self):
        """Expire every snapshot except the current one (MaintenanceTable API)."""
        t = self.c.pi_table()
        current = t.current_snapshot().snapshot_id
        old = [s.snapshot_id for s in t.snapshots() if s.snapshot_id != current]
        t.maintenance.expire_snapshots().by_ids(old).commit()

    def drop_table(self):
        """Purge the table through the catalog."""
        self.c.catalog.purge_table(self.c.ident)


class SparkMaint(Maint):
    """Spark procedures (``CALL <catalog>.system.*``) - every task has one."""

    engine_name = "spark"

    def drop_table(self):
        """Plain ``DROP TABLE`` on Lakekeeper (it deletes the files itself, and Spark's PURGE
        conflicts with that - see test_spark.py); ``DROP TABLE ... PURGE`` elsewhere."""
        purge = "" if PROFILE.name == "lakekeeper" else " PURGE"
        self.q(f"DROP TABLE {self.fq()}{purge}")

    def call(self, proc, args):
        """``CALL <catalog>.system.<proc>(<args>)``.

        Args:
            proc: Procedure name.
            args: Procedure arguments as SQL.

        Returns:
            The procedure's result rows.
        """
        return self.q(f"CALL {SPARK_CATALOGS[WAREHOUSE]}.system.{proc}({args})")

    def replace_all(self, rows):
        """``INSERT OVERWRITE`` with the rows.

        Args:
            rows: Rows ``(id, name, amount)`` to write.
        """
        self.q(f"INSERT OVERWRITE {self.fq()} VALUES {self.values(rows)}")

    def compact_data_files(self):
        """``rewrite_data_files`` (all small files into one)."""
        self.call(
            "rewrite_data_files",
            f"table => '{self.c.ident}', options => map('min-input-files', '2')",
        )

    def compact_delete_files(self):
        # Rewrite every data file that has a delete file, and drop the then dangling deletes.
        """``rewrite_data_files`` of every file with deletes, then ``rewrite_position_delete_files`` to drop the dangling ones."""
        self.call(
            "rewrite_data_files",
            # use-starting-sequence-number=false: the new file gets a NEWER sequence number
            # than the position delete, so the delete becomes dangling and is removed.
            f"table => '{self.c.ident}', options => map('delete-file-threshold', '1', "
            f"'rewrite-all', 'true', 'remove-dangling-deletes', 'true', "
            f"'use-starting-sequence-number', 'false')",
        )
        # v2 position deletes survive the data rewrite; the dedicated procedure drops the
        # ones that no longer point at a live data file.
        self.call(
            "rewrite_position_delete_files",
            f"table => '{self.c.ident}', options => map('rewrite-all', 'true')",
        )

    def rewrite_manifests(self):
        """``rewrite_manifests``."""
        self.call("rewrite_manifests", f"table => '{self.c.ident}'")

    def expire_snapshots(self):
        """``expire_snapshots`` older than now + 5 s, keeping one."""
        now = (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")
        self.call(
            "expire_snapshots",
            f"table => '{self.c.ident}', older_than => TIMESTAMP '{now}', retain_last => 1",
        )

    def remove_orphan_files(self):
        """Spark's Java action ``deleteOrphanFiles`` (via py4j).

        The SQL procedure refuses intervals under 24 hours ("Cannot remove orphan files with
        an interval less than 24 hours ... use the Action API") and an object's S3
        modification time cannot be backdated, so a freshly planted orphan is only reachable
        through the action - the API Spark's own error message points to.
        """
        spark = self.e.spark
        jvm = spark._jvm
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, f"{SPARK_CATALOGS[WAREHOUSE]}.{self.c.ident}"
        )
        older = int((time.time() + 5) * 1000)
        (
            jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
            .deleteOrphanFiles(table)
            .olderThan(older)
            # list via Iceberg's FileIO; the default Hadoop listing has no s3:// filesystem
            .usePrefixListing(True)
            .execute()
        )


class TrinoMaint(Maint):
    """Trino ``ALTER TABLE ... EXECUTE``: optimize, optimize_manifests, expire_snapshots,
    remove_orphan_files (retentions of ``0s`` are allowed by the catalog properties)."""

    engine_name = "trino"

    def x(self, cmd):
        """``ALTER TABLE ... EXECUTE <cmd>``.

        Args:
            cmd: Trino table procedure with arguments.
        """
        self.q(f"ALTER TABLE {self.fq()} EXECUTE {cmd}")

    def compact_data_files(self):
        """``EXECUTE optimize``."""
        self.x("optimize")

    def compact_delete_files(self):
        """``EXECUTE optimize`` (also applies deletes)."""
        self.x("optimize")

    def rewrite_manifests(self):
        """``EXECUTE optimize_manifests``."""
        self.x("optimize_manifests")

    def expire_snapshots(self):
        """``EXECUTE expire_snapshots`` with retention 0 s."""
        self.x("expire_snapshots(retention_threshold => '0s')")

    def remove_orphan_files(self):
        """``EXECUTE remove_orphan_files`` with retention 0 s."""
        self.x("remove_orphan_files(retention_threshold => '0s')")


class DuckDBMaint(Maint):
    """DuckDB iceberg extension: ``iceberg_rewrite_data_files``, ``iceberg_expire_snapshots``
    (new in DuckDB 2.0.1, found by the engine inventory) and ``DROP TABLE``.

    Manifest rewrite and orphan removal: not offered - checked against DuckDB's live function
    list on every run (capabilities.not_offered).
    """

    engine_name = "duckdb"

    def compact_data_files(self):
        """``iceberg_rewrite_data_files`` (at least 2 input files)."""
        self.q(f"SELECT * FROM iceberg_rewrite_data_files('{self.fq()}', min_input_files => 2)")

    def compact_delete_files(self):
        """``iceberg_rewrite_data_files`` (also single files with deletes)."""
        self.q(f"SELECT * FROM iceberg_rewrite_data_files('{self.fq()}', min_input_files => 1)")

    def replace_all(self, rows):
        """Delete everything, insert the rows, then compact (setup of ``expire_snapshots``).

        DuckDB's ``DELETE FROM t`` and ``TRUNCATE`` both keep the seed data file and add a delete
        file (checked with DuckDB 2.0.1); only ``iceberg_rewrite_data_files`` takes it out of the
        current snapshot.

        Args:
            rows: Rows ``(id, name, amount)`` to write.
        """
        super().replace_all(rows)
        self.compact_delete_files()

    def expire_snapshots(self):
        """``iceberg_expire_snapshots`` older than now + 5 s, keeping one (as Spark)."""
        now = (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S")
        self.q(
            f"SELECT * FROM iceberg_expire_snapshots('{self.fq()}', "
            f"older_than => TIMESTAMP '{now}', retain_last => 1)"
        )


class PgLakeMaint(Maint):
    """pg_lake 3.7 (extension 3.7, release v3.7.0): everything runs through ``VACUUM``
    (docs/findings.md, sections "pg_lake" and "Maintenance").

    ``VACUUM (FULL)`` compacts data files and applies deletes; ``VACUUM`` expires snapshots
    older than the table option ``max_snapshot_age`` and deletes the files in pg_lake's
    deletion queue - also used here as its orphan cleanup. No manifest rewrite API.
    Only tables pg_lake created itself are writable, so it seeds its own (format v2) table.
    """

    engine_name = "pg_lake"

    def seed(self):
        """Create and seed the table through pg_lake (it writes only tables it created)."""
        base.PgLakeOps(self.c, self.pool).setup()

    def replace_all(self, rows):
        """``TRUNCATE`` and insert the rows.

        Args:
            rows: Rows ``(id, name, amount)`` to write.
        """
        self.q(f"TRUNCATE {self.fq()}")
        self.q(f"INSERT INTO {self.fq()} VALUES {self.values(rows)}")

    def compact_data_files(self):
        """``VACUUM (FULL)``."""
        self.q(f"VACUUM (FULL) {self.fq()}")

    def compact_delete_files(self):
        """``VACUUM (FULL)``."""
        self.q(f"VACUUM (FULL) {self.fq()}")

    def expire_snapshots(self):
        """Set ``max_snapshot_age`` 0, then ``VACUUM``."""
        self.q(f"ALTER FOREIGN TABLE {self.fq()} OPTIONS (ADD max_snapshot_age '0')")
        self.q(f"VACUUM {self.fq()}")

    def remove_orphan_files(self):
        """``VACUUM`` (deletes files pg_lake tracked itself)."""
        self.q(f"VACUUM {self.fq()}")


class FlinkMaint(Maint):
    """Flink SQL: only ``CALL`` would offer maintenance, and ``FlinkCatalog`` implements no
    procedures (checked: ``getProcedure is not implemented``). Each task is attempted, so the
    report shows the error. Maintenance exists only as Java API (``TableMaintenance``)."""

    engine_name = "flink"

    def call(self, proc):
        """``CALL <catalog>.system.<proc>('<table>')`` (fails: FlinkCatalog has no procedures).

        Args:
            proc: Procedure name.
        """
        self.q(f"CALL {self.e.catalog}.`system`.{proc}('{self.c.ident}')")

    def replace_all(self, rows):
        """``INSERT OVERWRITE`` with the rows.

        Args:
            rows: Rows ``(id, name, amount)`` to write.
        """
        self.q(f"INSERT OVERWRITE {self.fq()} VALUES {self.values(rows)}")

    def compact_data_files(self):
        """Procedure ``rewrite_data_files``."""
        self.call("rewrite_data_files")

    def compact_delete_files(self):
        """Procedure ``rewrite_data_files``."""
        self.call("rewrite_data_files")

    def rewrite_manifests(self):
        """Procedure ``rewrite_manifests``."""
        self.call("rewrite_manifests")

    def expire_snapshots(self):
        """Procedure ``expire_snapshots``."""
        self.call("expire_snapshots")

    def remove_orphan_files(self):
        """Procedure ``remove_orphan_files``."""
        self.call("remove_orphan_files")


#: Polars and Daft have no maintenance API at all (no entry -> every task "has no API").
MAINT_CLASSES = {
    "pyiceberg": PyIcebergMaint,
    "duckdb": DuckDBMaint,
    "spark": SparkMaint,
    "trino": TrinoMaint,
    "pg_lake": PgLakeMaint,
    "flink": FlinkMaint,
}


# =========================================================================== task runs


def _files(t, content: int | None = None) -> list[str]:
    """Live files of the current snapshot (``content``: 0 data, 1 pos. deletes, 2 eq. deletes).

    Args:
        t: PyIceberg table.
        content: 0 data, 1 position deletes, 2 equality deletes; None: all.

    Returns:
        Normalised URIs of the files.
    """
    df = t.inspect.files()
    return [
        _norm_uri(p)
        for p, c in zip(df["file_path"].to_pylist(), df["content"].to_pylist(), strict=True)
        if content is None or c == content
    ]


def _delete_files(t) -> list[str]:
    """Live delete files of the current snapshot.

    Args:
        t: PyIceberg table.

    Returns:
        Their URIs.
    """
    return [f for f in _files(t) if f not in set(_files(t, 0))]


def _assert_storage_intact(t):
    """Every file the current snapshot references exists in RustFS.

    Args:
        t: PyIceberg table.
    """
    missing = set(_files(t)) - storage_files(t.location())
    assert not missing, f"referenced files missing in storage: {sorted(missing)[:3]}"


def _content(m: Maint, task: str, engine_pool):
    """Shared content check: every referenced file exists, the engine reads the expected rows.

    Args:
        m: The engine's maintenance object.
        task: Maintenance task (see ``TASKS``).
        engine_pool: Session pool of engine adapters (conftest fixture).
    """
    c = m.c
    _assert_storage_intact(c.pi_table())
    got = engine_pool(m.engine_name).read(c.ns, c.table)
    assert got == EXPECTED[task], f"content after {task}: {got}"


def run_task(m: Maint, task: str, engine_pool):
    """Setup, maintenance call and shared verification for one task (see module docstring).

    Order: the task's effect in the metadata, then the shared content check, then the checks of
    the task's effect in storage - so a gap there (files not deleted) cannot hide a broken
    table, and the table still goes to the cross-read (iceberg_lab.lab.results.keep_if_content_ok).

    Args:
        m: The engine's maintenance object.
        task: Maintenance task (see ``TASKS``).
        engine_pool: Session pool of engine adapters (conftest fixture).
    """
    c = m.c
    m.seed()
    if task in ("compact_data_files", "rewrite_manifests"):
        m.append([(4, "d", 4.0)])
        m.append([(5, "e", 5.0)])
        t = c.pi_table()
        if task == "compact_data_files":
            assert len(_files(t, 0)) >= 3, "setup: expected >= 3 data files"
        else:
            assert len(t.inspect.manifests()) >= 3, "setup: expected >= 3 manifests"
        getattr(m, task)()
        _content(m, task, engine_pool)
        t = c.pi_table()
        if task == "compact_data_files":
            assert len(_files(t, 0)) == 1, f"{len(_files(t, 0))} data files after compaction"
        else:
            n = len(t.inspect.manifests())
            assert n == 1, f"{n} manifests after rewrite"
    elif task == "compact_delete_files":
        m.append(BULK)
        m.delete_one()
        assert _delete_files(c.pi_table()), "setup: DELETE wrote no delete file"
        m.compact_delete_files()
        _content(m, task, engine_pool)
        left = _delete_files(c.pi_table())
        assert not left, f"{len(left)} delete files left after compaction"
    elif task == "expire_snapshots":
        t = c.pi_table()
        old_snapshot = t.current_snapshot()
        old_data = set(_files(t, 0))
        m.replace_all([(7, "g", 7.0)])
        assert not old_data & set(_files(c.pi_table())), "setup: seed file still referenced"
        m.expire_snapshots()
        t = c.pi_table()
        assert old_snapshot.snapshot_id not in {s.snapshot_id for s in t.snapshots()}, (
            "old snapshot still in metadata"
        )
        _content(m, task, engine_pool)
        stored = storage_files(t.location())
        left = sorted(old_data & stored)
        list_left = _norm_uri(old_snapshot.manifest_list) in stored
        # One assertion for both, so the message shows everything that was left behind.
        assert not left and not list_left, (
            f"expired data file still in storage: {left}"
            if left
            else "manifest list of the expired snapshot still in storage"
        ) + (" (and its manifest list)" if left and list_left else "")
    elif task == "remove_orphan_files":
        orphan = m.plant_orphan()
        m.remove_orphan_files()
        _content(m, task, engine_pool)
        assert orphan not in storage_files(c.pi_table().location()), "orphan file still there"
    elif task == "drop_table":
        location = c.pi_table().location()
        m.drop_table()
        assert not c.catalog.table_exists(c.ident), "table still in catalog"
        # Lakekeeper deletes the files itself (delete-profile hard), possibly asynchronously.
        deadline = time.monotonic() + 60
        while (left := storage_files(location)) and time.monotonic() < deadline:
            time.sleep(2)
        assert not left, f"{len(left)} files left after DROP, e.g. {min(left)}"


@pytest.fixture(scope="module")
def maint_results():
    """``(engine, task, fv) -> (namespace, table)`` of successful tasks (for the cross-read).

    Returns:
        An empty dict, filled by test_maintenance.
    """
    return {}


@pytest.mark.slow
@pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
@pytest.mark.parametrize("task", TASKS)
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_maintenance(engine, task, fv, namespace, engine_pool, maint_results):
    """The engine runs a maintenance task on its own table; checked in metadata, storage and content.

    Tasks: compact data files (3 -> 1), compact delete files, rewrite manifests (3 -> 1),
    expire snapshots, remove orphan files, drop table.

    Checks (all engines alike):
        * setup by the engine itself (small files, delete files, replaced snapshot, planted
          orphan);
        * after the engine's own maintenance API: every referenced file exists in storage and
          the engine reads the expected rows;
        * the task's effect: file/manifest counts, the expired snapshot gone from metadata and
          its files from storage, the orphan deleted, nothing left after DROP.
        Engines without an API for the task: not offered.

    Args:
        engine: Engine under test.
        task: Maintenance task (see ``TASKS``).
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        maint_results: ``(engine, task, fv) -> (namespace, table)`` of the tables for the
            cross-read (None: attempted without a readable result).
    """
    if task in EXPECTED:
        maint_results[(engine, task, fv)] = (
            None  # attempted (see iceberg_lab.lab.results.skip_if_not_produced)
        )
    cls = MAINT_CLASSES.get(engine)
    if cls is None or not hasattr(cls, task):
        not_offered(engine, task, None, engine_pool)
    require_format(engine, fv)
    ctx = base.Ctx(
        pyiceberg_catalog(WAREHOUSE),
        namespace,
        f"mt_{engine}_{task}_v{fv}_{uuid.uuid4().hex[:6]}",
        fv,
        seed_writer=engine,
    )
    gap = known_gap(engine, task, fv)
    produced = (namespace, ctx.table)

    def content_ok():
        """The engine's own read of the table equals the task's expected content.

        Returns:
            Whether it does.
        """
        got = fresh(engine_pool(engine), *produced).read(*produced)
        return task in EXPECTED and got == EXPECTED[task]

    with (
        keep_if_content_ok(maint_results, (engine, task, fv), produced, content_ok),
        expect(gap),
    ):
        run_task(cls(ctx, engine_pool), task, engine_pool)
    if task in EXPECTED:
        maint_results[(engine, task, fv)] = (namespace, ctx.table)


#: Known cross-read gaps after maintenance: (engine, reader, task, fv) globs, regex, reason.
# fmt: off
CROSS_GAPS = gap_table(("engine", "reader", "task", "fv"), [
    # After compaction the current manifests still list the removed DV as an entry with
    # status DELETED (as the spec requires); no live DV is left. pg_lake rejects it anyway.
    *((w, "pg_lake", "compact_delete_files", "3", r"deletion vectors are not supported", "pglake-rejects-deleted-dv")
      for w in ("spark", "trino")),
    # DuckDB 2.0 cannot compact v3 (documented gap); its table keeps the live deletion vector,
    # which these readers cannot apply (the same gaps as in test_operations / test_neutral_source).
    *(("duckdb", "daft", task, "3", r"\.puffin is not a valid parquet file", "daft-dv-as-parquet")
      for task in ("compact_delete_files", "expire_snapshots")),
    *(("duckdb", "pg_lake", task, "3", r"deletion vectors are not supported", "pglake-no-dv-read")
      for task in ("compact_delete_files", "expire_snapshots")),
])
# fmt: on


def _cross_gap(engine, reader, task, fv):
    """First matching cross-read gap after maintenance.

    Args:
        engine: Engine that ran the task.
        reader: Engine that reads.
        task: Maintenance task (see ``TASKS``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The gap, or None.
    """
    return find_gap(CROSS_GAPS, engine=engine, reader=reader, task=task, fv=fv)


@pytest.mark.slow
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
@pytest.mark.parametrize("task", sorted(EXPECTED))
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_maintenance_cross_read(engine, task, fv, reader, engine_pool, maint_results):
    """After the engine's maintenance, every other engine reads exactly the expected content.

    Checks:
        * the reader (cache dropped) returns exactly the rows the task must leave.
        Skipped (blocked) if the maintenance produced no readable table.

    Args:
        engine: Engine under test.
        task: Maintenance task (see ``TASKS``).
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        maint_results: ``(engine, task, fv) -> (namespace, table)`` of the tables for the
            cross-read (None: attempted without a readable result).
    """
    if reader == engine:
        pytest.skip("own result is verified in test_maintenance")
    result = skip_if_not_produced(maint_results, (engine, task, fv), "test_maintenance")
    gap = _cross_gap(engine, reader, task, fv)
    with expect(gap):
        assert fresh(engine_pool(reader), *result).read(*result) == EXPECTED[task]


#: Known gaps when writing after maintenance: (engine, writer, task, fv) globs, regex, reason,
#: intermittent.
# fmt: off
CROSS_WRITE_GAPS = gap_table(("engine", "writer", "task", "fv"), [
    *(("*", w, "*", "3", r"Cannot write manifest for table version: 3", "pyiceberg-v3-data-write", False)
      for w in ("pyiceberg", "polars", "daft")),
    # Observed 2026-11-23 (pg_lake 3.7): VACUUM queued a manifest for deletion, the next INSERT
    # queues the same path again. Reproducible when run alone (v2, Lakekeeper and Polaris), but
    # not in every full run - intermittent.
    ("pg_lake", "pg_lake", "expire_snapshots", "*", r"duplicate key value violates unique constraint \"deletion_queue_pkey\"", "pglake-deletion-queue-duplicate", True),
])
# fmt: on


def _cross_write_gap(engine, writer, task, fv):
    """First matching gap of :data:`CROSS_WRITE_GAPS`.

    Args:
        engine: Engine that ran the task.
        writer: Engine that writes afterwards.
        task: Maintenance task (see ``TASKS``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The gap, or None.
    """
    return find_gap(CROSS_WRITE_GAPS, engine=engine, writer=writer, task=task, fv=fv)


@pytest.mark.slow
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
@pytest.mark.parametrize("task", sorted(EXPECTED))
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_maintenance_cross_write(engine, task, fv, writer, engine_pool, maint_results):
    """After the engine's maintenance, every engine (itself included) can still append.

    Runs after all cross-reads (the writes change the tables). Each writer appends its own row
    (id 100 + its position in ``ENGINES``), so the writers of one table do not disturb each
    other's checks.

    Checks:
        * the append works on the maintained table (compacted files, rewritten manifests,
          expired snapshots, removed orphans);
        * the lab client then reads every row the task left, the writer's row exactly once, no
          duplicated and no other unexpected row.
        Skipped (blocked) if the maintenance produced no table.

    Args:
        engine: Engine that ran the maintenance.
        task: Maintenance task (see ``TASKS``).
        fv: Iceberg format version of the table (2 or 3).
        writer: Engine that appends afterwards.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        maint_results: ``(engine, task, fv) -> (namespace, table)`` of the maintained tables.
    """
    ns, table = skip_if_not_produced(maint_results, (engine, task, fv), "test_maintenance")
    if writer != engine:
        require(writer, "writing tables it did not create", engine_pool)
    n = 100 + ENGINES.index(writer)
    row = (n, f"w{n}", float(n))
    with expect(_cross_write_gap(engine, writer, task, fv)):
        ctx = base.Ctx(pyiceberg_catalog(WAREHOUSE), ns, table, fv, seed_writer=writer)
        if writer not in ("pyiceberg", "polars", "daft"):
            fresh(engine_pool(writer), ns, table)
        base.append_rows(ctx, engine_pool, [row])
        got = fresh(engine_pool("pyiceberg"), ns, table).read(ns, table)
        missing = [r for r in EXPECTED[task] if r not in got]
        assert not missing, f"rows the task left are gone after {writer}'s append: {missing}"
        assert got.count(row) == 1, f"{writer}'s row appears {got.count(row)} times"
        assert len(got) == len(set(got)), (
            f"duplicated rows: {sorted(r for r in got if got.count(r) > 1)}"
        )
        unexpected = [r for r in got if r not in EXPECTED[task] and r[0] < 100]
        assert not unexpected, f"rows that should not be there: {unexpected}"


def test_maint_classes_define_only_known_tasks():
    """Guard: a method-name typo would silently turn a task into "no API".

    Checks:
        * every public method of each engine's maintenance class is a known task or helper.
    """
    helpers = {
        "seed", "append", "values", "delete_one", "replace_all", "plant_orphan", "fq", "q",
        "call", "x",
    }  # fmt: skip
    for engine, cls in MAINT_CLASSES.items():
        public = {m for m in dir(cls) if not m.startswith("_") and callable(getattr(cls, m))}
        unknown = public - set(TASKS) - helpers
        assert not unknown, f"{engine}: methods that are no maintenance task: {sorted(unknown)}"
