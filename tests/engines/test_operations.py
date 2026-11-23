"""Operation matrix: every data and table operation an engine offers, on format v2 and v3.

Matrix
    engine x operation (:data:`OPERATIONS`) x format version (2, 3) -> one test each,
    e.g. ``test_operation[trino-merge-v3]``. Table maintenance (compaction, expire,
    manifests, orphans, files after DROP) has its own matrix: ``test_maintenance.py``.

Setup per test
    Unless the operation creates its own table (``create_table``), the test starts from a
    table ``(id INT, name STRING, amount DOUBLE)`` with the rows :data:`SEED` committed as one
    snapshot (``ctx.seed_snapshot``, used by time travel / rollback):

    * created by PyIceberg with the requested format version;
    * seeded by the engine under test itself (no substitute engine - if it cannot write the
      format version, the test fails with that documented gap);
    * engines that can only write tables they created themselves (pg_lake) provide a
      ``setup()`` method and create + seed the table on their own.

    The engine under test then performs ONE operation and checks the result, preferably with
    its *own* reader (PyIceberg cannot read every delete-file flavour, e.g. Trino's) and with
    PyIceberg only for metadata (schema, spec, snapshots, files).

Result semantics
    skip   the engine has no API for the operation: the Ops class has no such method, or
           lists it in ``UNSUPPORTED`` (each class docstring names them)
    FAILED the API exists but does not work. Documented cases (:data:`KNOWN_GAPS`) carry
           the label ``[known gap]``; anything else is a new error. ``KNOWN_GAPS=xfail``
           reports documented cases as XFAIL instead (see ``expect_gap`` in iceberg_lab.lab.gaps).
    After every content-changing operation EVERY other engine re-reads the table
    (``test_operation_cross_read``, one test per reader), so an engine whose writes only it
    can read is caught. Setup rows are ALWAYS written by the engine under test
    (:func:`seed_writer_for`); an engine that cannot write the format version fails. If the
    operation's content is right but a later check fails (e.g. no delete file written), the
    table still goes to the cross-read (``iceberg_lab.lab.results.keep_if_content_ok``). The same states
    written by the reference implementation: tests/engines/test_neutral_source.py.

Adding an engine: write an ``<Engine>Ops`` class with one method per supported operation
(subclass :class:`SqlOps` for SQL engines) and register it in :data:`OPS_CLASSES`.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass

import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, IntegerType, LongType, NestedField, StringType

from iceberg_lab.config import PROFILE, WAREHOUSE_STS
from iceberg_lab.engines import SPARK_CATALOGS, polars_storage_options, pyiceberg_catalog

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
    require_format,
    skip_if_not_produced,
)

WAREHOUSE = WAREHOUSE_STS

#: Schema of every operation test table. ``id`` is INT (not BIGINT) so that ``widen_type``
#: can promote it to BIGINT, the most common legal Iceberg type promotion.
SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
)
#: Rows committed as the first snapshot of every table.
SEED = [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)]

#: All operations, in report order. Each is a method name on the Ops classes.
OPERATIONS = [
    "create_table",  # DDL with explicit format version
    "ctas",  # CREATE TABLE ... AS SELECT
    "insert",  # append rows
    "overwrite",  # replace the whole table content
    "delete",  # row-level delete by predicate
    "update",  # row-level update
    "merge",  # upsert: update matched rows, insert new ones
    "add_column",  # schema evolution
    "rename_column",
    "drop_column",
    "widen_type",  # int -> bigint promotion
    "partition_evolution",  # add a partition field to a table that already has data
    "time_travel",  # read an older snapshot
    "rollback",  # make an older snapshot current again
    "branch_and_tag",  # named refs (Iceberg branches/tags)
    "metadata_tables",  # snapshots/files/manifests/... as queryable tables
    "rename_table",
    "drop_table",
    # Base cases that used to be checked for one engine only (docs/test-architecture.md):
    "delete_merge_on_read",  # DELETE writes delete files (position deletes / v3 deletion vectors)
    "transaction_multi_change",  # INSERT + DELETE in one transaction = ONE catalog commit
    "transaction_rollback",  # an aborted transaction leaves no snapshot
    "sees_external_commit",  # a commit by another client is visible without reconnecting
    "identifier_fields",  # declare a primary key / identifier field on a new table
    "create_existing_fails",  # CREATE TABLE of an existing table is an error, not silent
    "filter_aggregate",  # WHERE + count/sum return the right values
    "list_tables",  # the engine lists the namespace's tables (incl. ones other clients made)
]


# PyIceberg 0.12 supports v3 only partly: creating, reading and metadata-only changes work,
# but write_manifest()/write_manifest_list() implement only format v1 and v2
# (cause pyiceberg-v3-data-write). Shared with the other engine matrices.
NO_V3_MSG = r"Cannot write manifest for table version: 3"

# Known gaps of the operation matrix, one per line. First match wins.
# fmt: off
KNOWN_GAPS = gap_table(("engine", "op", "fv"), [
    *([("pg_lake", "partition_evolution", "*", r"partition spec 1 not found for manifest", "pglake-partition-source-ids")]
      if PROFILE.name == "lakekeeper" else []),
    ("trino", "branch_and_tag", "*", r"does not support creating branches", "trino-no-create-branch"),
    ("trino", "overwrite", "*", r"Catalog only supports writes using autocommit", "trino-autocommit-only"),
    ("pg_lake", "rename_table", "*", r"ALTER TABLE RENAME command not supported for pg_lake_iceberg tables", "pglake-no-rename"),
    ("flink", "delete", "*", r"Can't perform delete operation .* has not yet implemented org\.apache\.flink\.table\.connector\.sink\.abilities\.SupportsRowLevelDelete", "flink-no-delete"),
    ("flink", "update", "*", r"Can't perform update operation .* has not yet implemented org\.apache\.flink\.table\.connector\.sink\.abilities\.SupportsRowLevelUpdate", "flink-no-update"),
    *(("flink", op, "*", r"getProcedure is not implemented for class org\.apache\.iceberg\.flink\.FlinkCatalog", "flink-no-procedures")
      for op in ("rollback",)),
    ("flink", "branch_and_tag", "*", r"Flink SQL has no statement to create a tag", "flink-no-create-tag"),
    ("pg_lake", "merge", "*", r"cannot execute MERGE on relation", "pglake-no-merge"),
    # PyIceberg (and Polars/Daft, which write through it) cannot write v3 data. Every test
    # starts by writing its own setup rows, so every v3 operation except create_table fails
    # in the setup - deliberately: no other engine writes the data for them.
    *(("pyiceberg", op, "3", NO_V3_MSG, "pyiceberg-v3-data-write")
      for op in ("insert", "overwrite", "delete", "merge", "add_column", "rename_column",
                 "drop_column", "widen_type", "partition_evolution", "time_travel", "rollback",
                 "branch_and_tag", "metadata_tables", "rename_table",
                 "drop_table", "delete_merge_on_read", "transaction_multi_change",
                 "transaction_rollback", "sees_external_commit", "create_existing_fails",
                 "filter_aggregate", "list_tables")),
    *((eng, op, "3", NO_V3_MSG, "pyiceberg-v3-data-write")
      for eng in ("polars", "daft")
      for op in ("insert", "overwrite", "time_travel", "sees_external_commit", "filter_aggregate", "list_tables")),
    ("polars", "add_column", "3", NO_V3_MSG, "pyiceberg-v3-data-write"),
    # Base cases added 2026-11-15: findings.
    ("spark", "sees_external_commit", "*", r"engine does not see a commit made by another client", "spark-catalog-cache"),
    ("pg_lake", "sees_external_commit", "*", r"engine does not see a commit made by another client", "pglake-no-external-commits"),
    ("pyiceberg", "delete_merge_on_read", "2", r"no delete file written - copy-on-write despite", "pyiceberg-delete-copy-on-write-only"),
    ("pg_lake", "delete_merge_on_read", "2", r"no delete file written - copy-on-write despite", "pglake-delete-copy-on-write"),
    ("duckdb", "identifier_fields", "*", r"Only NOT NULL constraints are supported for Iceberg tables", "duckdb-no-primary-key"),
    ("pg_lake", "identifier_fields", "*", r"primary key constraints are not supported on foreign tables", "pglake-no-primary-key"),
    # Same root causes as above, for the base cases added 2026-11-15.
    ("flink", "delete_merge_on_read", "*", r"Can't perform delete operation .* has not yet implemented org\.apache\.flink\.table\.connector\.sink\.abilities\.SupportsRowLevelDelete", "flink-no-delete"),
    *(("trino", op, "*", r"Catalog only supports writes using autocommit", "trino-autocommit-only")
      for op in ("transaction_multi_change", "transaction_rollback")),
])
# fmt: on


def known_gap(engine: str, op: str, fv: int) -> Gap | None:
    """Return the first matching :data:`KNOWN_GAPS` entry, else ``None``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        op: Operation (see ``OPERATIONS``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The gap, or None.
    """
    return find_gap(KNOWN_GAPS, engine=engine, op=op, fv=fv)


@dataclass
class Ctx:
    """Everything an operation needs to know about its test table.

    Attributes:
        catalog: PyIceberg catalog on :data:`WAREHOUSE`, used for setup and metadata checks.
        ns: Namespace (session namespace from conftest).
        table: Unique table name for this test.
        fv: Format version (2 or 3).
        seed_snapshot: Snapshot id after the seed commit (``None`` for ``create_table``).
        seed_writer: Engine that writes setup rows (see :func:`seed_writer_for`).
    """

    catalog: Catalog
    ns: str
    table: str
    fv: int
    seed_snapshot: int | None = None
    seed_writer: str = "pyiceberg"

    @property
    def ident(self) -> str:
        """``namespace.table`` identifier as PyIceberg and Spark procedures expect it.

        Returns:
            ``namespace.table``.
        """
        return f"{self.ns}.{self.table}"

    def pi_table(self, name: str | None = None):
        """Load the (current state of the) test table, or a sibling table ``name``, via PyIceberg.

        Args:
            name: Sibling table name (None: the test table).

        Returns:
            The PyIceberg table.
        """
        return self.catalog.load_table(f"{self.ns}.{name or self.table}")

    def new_name(self, suffix: str) -> str:
        """Name for an additional table derived from the test table (CTAS/rename targets).

        Args:
            suffix: Suffix of the new name.

        Returns:
            ``<table>_<suffix>``.
        """
        return f"{self.table}_{suffix}"


def seed_writer_for(engine: str, fv: int) -> str:
    """The engine that writes the setup rows of ``engine``'s tests: always the engine itself.

    No substitution: if an engine cannot write the table's format version (PyIceberg, and
    Polars/Daft which write through PyIceberg, cannot write v3 data), its test FAILS in the
    setup - reported as the documented gap, not hidden by letting another engine write.
    pg_lake seeds itself via ``PgLakeOps.setup``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The engine name.
    """
    return engine


def append_rows(ctx: Ctx, pool, rows) -> None:
    """Append ``rows`` to the test table with ``ctx.seed_writer``'s own write path.

    Used for the seed and for "make a second snapshot" steps in time travel/rollback.
    Runs inside the test's ``expect_gap`` block, so a writer that cannot write the format
    version makes the test fail with its documented gap.

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).
        pool: Engine adapter pool (``engine_pool`` fixture).
        rows: Rows ``(id, name, amount)``.
    """
    e = pool(ctx.seed_writer)
    if e.dialect is None:  # DataFrame engine: its own Arrow write path
        e.append_arrow(ctx.ns, ctx.table, arrow(rows))
    else:
        # "<float>E0" is a DOUBLE literal in DuckDB, Spark, Trino and Postgres alike.
        values = ", ".join(f"({i}, '{n}', {a!r}E0)" for i, n, a in rows)
        e.execute(f"INSERT INTO {e.fq(ctx.ns, ctx.table)} VALUES {values}")


def arrow(rows) -> pa.Table:
    """``[(id, name, amount), ...]`` -> Arrow table with the test :data:`SCHEMA`.

    Args:
        rows: Rows ``(id, name, amount)``.

    Returns:
        The Arrow table.
    """
    return pa.Table.from_pylist(
        [dict(zip(("id", "name", "amount"), r)) for r in rows], schema=SCHEMA.as_arrow()
    )


def arrow_rows(t: pa.Table):
    """Iterate an Arrow table as ``(id, name, amount)`` tuples.

    Args:
        t: Arrow table with id, name, amount.

    Returns:
        Iterator of tuples.
    """
    return zip(*(t[c].to_pylist() for c in ("id", "name", "amount")))


def norm(rows) -> list[tuple]:
    """Sort rows and turn engine row objects (Row, list, tuple) into plain tuples.

    Args:
        rows: Engine result rows.

    Returns:
        Sorted tuples.
    """
    return sorted(tuple(r) for r in rows)


#: Table property that asks writers for delete files instead of rewriting data files.
MOR_PROPS = {"write.delete.mode": "merge-on-read", "write.update.mode": "merge-on-read"}
ALREADY_EXISTS = r"(?i)already exists|AlreadyExists"


class _Abort(Exception):
    """Raised inside a transaction to abort it on purpose (transaction_rollback)."""


def set_props(ctx: Ctx, props: dict) -> None:
    """Set table properties through the lab client (setup, not the engine under test).

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).
        props: Table properties to set.
    """
    with ctx.pi_table().transaction() as tx:
        tx.set_properties(props)


def assert_delete_files(ctx: Ctx) -> None:
    """The table now has delete files (position/equality deletes or v3 deletion vectors).

    Read from the file list (content != 0), not from the snapshot summary: not every writer
    fills the summary (pg_lake writes none).

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).
    """
    files = ctx.pi_table().inspect.files()
    content = files["content"].to_pylist()
    assert any(c != 0 for c in content), (
        f"no delete file written - copy-on-write despite {MOR_PROPS} (file contents: {content})"
    )


def commit_count(ctx: Ctx) -> int:
    """Number of catalog commits so far (each commit writes one new metadata file).

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).

    Returns:
        Length of the metadata log.
    """
    return len(ctx.pi_table().metadata.metadata_log)


def assert_seed_unchanged(ctx: Ctx) -> None:
    """No snapshot was added after the seed.

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).
    """
    assert ctx.pi_table().current_snapshot().snapshot_id == ctx.seed_snapshot, (
        "aborted transaction left a snapshot"
    )


def external_append(ctx: Ctx, pool, me: str) -> None:
    """Another client appends row 4 to the test table.

    v2: the lab's PyIceberg client. v3: PyIceberg cannot write v3 data, so the first of
    Spark/Trino/DuckDB that is not the engine under test writes it.

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).
        pool: Engine adapter pool (``engine_pool`` fixture).
        me: Engine under test (the other client must differ).
    """
    if ctx.fv == 2:
        pyiceberg_catalog(WAREHOUSE).load_table(ctx.ident).append(arrow([(4, "d", 4.0)]))
        return
    other = next(w for w in ("spark", "trino", "duckdb") if w != me)
    e = pool(other)
    e.execute(f"INSERT INTO {e.fq(ctx.ns, ctx.table)} VALUES (4, 'd', 4.0E0)")


def expect_already_exists(create) -> None:
    """``create()`` must fail with an "already exists" error.

    Args:
        create: Callable that creates the table again.
    """
    try:
        create()
    except Exception as exc:
        if not re.search(ALREADY_EXISTS, f"{type(exc).__name__}: {exc}"):
            raise
        return
    raise AssertionError("CREATE TABLE of an existing table reported success")


STALE = "engine does not see a commit made by another client (stale metadata)"


def assert_listed(ctx: Ctx, names) -> None:
    """The test table (created by the lab client in the setup) is among the listed names.

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).
        names: Table names as the engine lists them.
    """
    names = {str(n).strip('`"').split(".")[-1] for n in names}
    assert ctx.table in names, f"{ctx.table} not listed: {sorted(names)[:10]}"


# =========================================================================== PyIceberg


class PyIcebergOps:
    """Operations through the PyIceberg Python API (no SQL).

    No API: ``ctas`` and ``update`` (no SQL layer; use ``upsert``/``overwrite`` instead).
    """

    NAME = "pyiceberg"

    def __init__(self, ctx: Ctx, pool):
        """Bind the test table and the engine pool.

        Args:
            ctx: Context of the test table.
            pool: Engine adapter pool.
        """
        self.c = ctx
        self.pool = pool

    def rows(self, name=None):
        """Current table content as sorted tuples.

        Args:
            name: Sibling table name (None: the test table).

        Returns:
            Sorted tuples.
        """
        return norm(arrow_rows(self.c.pi_table(name).scan().to_arrow()))

    def create_table(self):
        """``create_table`` with an explicit ``format-version`` property."""
        t = self.c.catalog.create_table(
            self.c.ident, schema=SCHEMA, properties={"format-version": str(self.c.fv)}
        )
        assert t.format_version == self.c.fv

    def insert(self):
        """``Table.append``: adds a new data file in a new snapshot."""
        self.c.pi_table().append(arrow([(4, "d", 4.0)]))
        assert self.rows() == SEED + [(4, "d", 4.0)]

    def overwrite(self):
        """``Table.overwrite`` without filter: replaces all rows."""
        self.c.pi_table().overwrite(arrow([(9, "z", 9.0)]))
        assert self.rows() == [(9, "z", 9.0)]

    def delete(self):
        """``Table.delete`` with a predicate (copy-on-write: rewrites affected files)."""
        self.c.pi_table().delete("id = 2")
        assert self.rows() == [SEED[0], SEED[2]]

    def merge(self):
        """``Table.upsert`` on key ``id``: row 2 is updated, row 4 is inserted."""
        before = commit_count(self.c)
        self.c.pi_table().upsert(arrow([(2, "b2", 20.0), (4, "d", 4.0)]), join_cols=["id"])
        assert self.rows() == [SEED[0], (2, "b2", 20.0), SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "MERGE was not ONE catalog commit"

    def add_column(self):
        """New column reads as NULL for existing rows (no data rewrite)."""
        t = self.c.pi_table()
        with t.update_schema() as u:
            u.add_column("extra", StringType())
        assert t.scan().to_arrow()["extra"].null_count == 3

    def rename_column(self):
        """Rename is metadata-only: old files keep the old physical name, matched by field id."""
        t = self.c.pi_table()
        with t.update_schema() as u:
            u.rename_column("amount", "value")
        assert t.scan().to_arrow().column_names == ["id", "name", "value"]

    def drop_column(self):
        """Dropped column disappears from reads; its data stays in old files."""
        t = self.c.pi_table()
        with t.update_schema() as u:
            u.delete_column("amount")
        assert t.scan().to_arrow().column_names == ["id", "name"]

    def widen_type(self):
        """int -> long promotion; old files are read with the wider type."""
        t = self.c.pi_table()
        with t.update_schema() as u:
            u.update_column("id", LongType())
        assert t.scan().to_arrow().schema.field("id").type == pa.int64()

    def partition_evolution(self):
        """Add an identity partition on ``name``; old files stay unpartitioned, new are partitioned."""
        t = self.c.pi_table()
        with t.update_spec() as u:
            u.add_identity("name")
        t.append(arrow([(4, "d", 4.0)]))
        assert len(t.refresh().spec().fields) == 1
        assert self.rows() == SEED + [(4, "d", 4.0)]

    def time_travel(self):
        """``scan(snapshot_id=...)`` returns the table as of the seed snapshot."""
        append_rows(self.c, self.pool, [(4, "d", 4.0)])
        t = self.c.pi_table()
        assert norm(arrow_rows(t.scan(snapshot_id=self.c.seed_snapshot).to_arrow())) == SEED

    def rollback(self):
        """``set_current_snapshot`` moves ``main`` back to the seed snapshot."""
        append_rows(self.c, self.pool, [(4, "d", 4.0)])
        t = self.c.pi_table()
        t.manage_snapshots().set_current_snapshot(self.c.seed_snapshot).commit()
        assert self.rows() == SEED

    def branch_and_tag(self):
        """Create a tag and a branch on the seed snapshot in one metadata commit."""
        t = self.c.pi_table()
        (
            t.manage_snapshots()
            .create_tag(self.c.seed_snapshot, "v1")
            .create_branch(self.c.seed_snapshot, "dev")
            .commit()
        )
        refs = t.refresh().metadata.refs
        assert {"v1", "dev", "main"} <= set(refs)

    def metadata_tables(self):
        """``Table.inspect``: snapshots, files, manifests as Arrow tables."""
        t = self.c.pi_table()
        assert t.inspect.snapshots().num_rows == 1
        assert t.inspect.files().num_rows >= 1
        assert t.inspect.manifests().num_rows >= 1

    def rename_table(self):
        """Catalog-level rename; data must be readable under the new name."""
        new = self.c.new_name("renamed")
        self.c.catalog.rename_table(self.c.ident, f"{self.c.ns}.{new}")
        assert self.rows(new) == SEED

    def drop_table(self):
        """``purge_table`` removes the table from the catalog (and its files)."""
        self.c.catalog.purge_table(self.c.ident)
        assert not self.c.catalog.table_exists(self.c.ident)

    def delete_merge_on_read(self):
        """``Table.delete`` with ``write.delete.mode=merge-on-read`` set."""
        set_props(self.c, MOR_PROPS)
        self.c.pi_table().delete("id = 2")
        assert self.rows() == [SEED[0], SEED[2]]
        assert_delete_files(self.c)

    def transaction_multi_change(self):
        """``Table.transaction()``: append + delete, committed once."""
        before = commit_count(self.c)
        with self.c.pi_table().transaction() as tx:
            tx.append(arrow([(4, "d", 4.0)]))
            tx.delete("id = 2")
        assert self.rows() == [SEED[0], SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "more than one catalog commit"

    def transaction_rollback(self):
        """An exception inside ``Table.transaction()`` discards the staged append."""
        try:
            with self.c.pi_table().transaction() as tx:
                tx.append(arrow([(4, "d", 4.0)]))
                raise _Abort
        except _Abort:
            pass
        assert self.rows() == SEED
        assert_seed_unchanged(self.c)

    def sees_external_commit(self):
        """Read, another client appends, read again (``load_table`` each time)."""
        assert self.rows() == SEED
        external_append(self.c, self.pool, self.NAME)
        assert self.rows() == SEED + [(4, "d", 4.0)], STALE

    def identifier_fields(self):
        """``create_table`` with a required ``id`` declared as identifier field."""
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType()),
            NestedField(3, "amount", DoubleType()),
            identifier_field_ids=[1],
        )
        self.c.catalog.create_table(
            self.c.ident, schema=schema, properties={"format-version": str(self.c.fv)}
        )
        s2 = self.c.pi_table().schema()
        assert s2.identifier_field_ids == [s2.find_field("id").field_id]

    def create_existing_fails(self):
        """``create_table`` on an existing name raises ``TableAlreadyExistsError``."""
        expect_already_exists(lambda: self.c.catalog.create_table(self.c.ident, schema=SCHEMA))
        assert self.rows() == SEED

    def list_tables(self):
        """``catalog.list_tables(namespace)``."""
        assert_listed(self.c, [t[-1] for t in self.c.catalog.list_tables(self.c.ns)])

    def filter_aggregate(self):
        """``scan(row_filter=...)`` + Arrow aggregation."""
        import pyarrow.compute as pc

        t = self.c.pi_table().scan(row_filter="id >= 2").to_arrow()
        assert (t.num_rows, pc.sum(t["amount"]).as_py()) == (2, 5.0)


# =========================================================================== SQL engines


class SqlOps:
    """Operations in (mostly) standard SQL; dialect subclasses override where syntax differs.

    Subclasses set ``engine_name`` (key into the engine pool) and may define ``UNSUPPORTED``
    to hide inherited operations the engine does not offer.
    """

    engine_name: str

    def __init__(self, ctx: Ctx, pool):
        """Bind the test table and the engine's pooled adapter.

        Args:
            ctx: Context of the test table.
            pool: Engine adapter pool.
        """
        self.c = ctx
        self.pool = pool
        self.e = pool(self.engine_name)

    @property
    def NAME(self):
        """Engine name (for picking a different "other client" in sees_external_commit).

        Returns:
            The engine name.
        """
        return self.engine_name

    # -- dialect hooks ------------------------------------------------------------------------

    def fq(self, name=None):
        """Fully qualified name of the test table (or sibling ``name``) in this dialect.

        Args:
            name: Sibling table name (None: the test table).

        Returns:
            The qualified name.
        """
        return self.e.fq(self.c.ns, name or self.c.table)

    def q(self, sql):
        """Execute one statement through the engine adapter.

        Args:
            sql: SQL statement.

        Returns:
            The result rows.
        """
        return self.e.execute(sql)

    def rows(self, name=None):
        """Table content as sorted tuples, read by the engine itself.

        Args:
            name: Sibling table name (None: the test table).

        Returns:
            Sorted tuples.
        """
        return norm(self.q(f"SELECT id, name, amount FROM {self.fq(name)}"))

    def rename_target(self, new):
        """How the target of ``ALTER TABLE ... RENAME TO`` must be written (dialect-specific).

        Args:
            new: New table name.

        Returns:
            The target as the dialect expects it.
        """
        return self.fq(new)

    # -- operations ---------------------------------------------------------------------------

    def insert(self):
        """``INSERT INTO ... VALUES``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
        assert self.rows() == SEED + [(4, "d", 4.0)]

    def delete(self):
        """Row-level ``DELETE`` (engine chooses copy-on-write or delete files)."""
        self.q(f"DELETE FROM {self.fq()} WHERE id = 2")
        assert self.rows() == [SEED[0], SEED[2]]

    def update(self):
        """Row-level ``UPDATE`` of a single row."""
        self.q(f"UPDATE {self.fq()} SET amount = 20.0 WHERE id = 2")
        assert self.rows() == [SEED[0], (2, "b", 20.0), SEED[2]]

    def merge(self):
        """Standard ``MERGE INTO``: update row 2, insert row 4."""
        before = commit_count(self.c)
        self.q(f"""
            MERGE INTO {self.fq()} t
            USING (SELECT * FROM (VALUES (2, 'b2', 20.0), (4, 'd', 4.0)) AS s(id, name, amount)) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount
            WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.amount)
        """)
        assert self.rows() == [SEED[0], (2, "b2", 20.0), SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "MERGE was not ONE catalog commit"

    def ctas(self):
        """``CREATE TABLE ... AS SELECT`` from the test table."""
        new = self.c.new_name("ctas")
        self.q(f"CREATE TABLE {self.fq(new)} AS SELECT * FROM {self.fq()} WHERE id > 1")
        assert self.rows(new) == SEED[1:]

    def add_column(self):
        """``ALTER TABLE ADD COLUMN``; checked in the Iceberg schema via PyIceberg."""
        self.q(f"ALTER TABLE {self.fq()} ADD COLUMN extra VARCHAR")
        assert [f.name for f in self.c.pi_table().schema().fields][-1] == "extra"

    def rename_column(self):
        """``ALTER TABLE RENAME COLUMN``."""
        self.q(f"ALTER TABLE {self.fq()} RENAME COLUMN amount TO value")
        assert self.c.pi_table().schema().find_field("value")

    def drop_column(self):
        """``ALTER TABLE DROP COLUMN``."""
        self.q(f"ALTER TABLE {self.fq()} DROP COLUMN amount")
        assert [f.name for f in self.c.pi_table().schema().fields] == ["id", "name"]

    def widen_type(self):
        """``ALTER COLUMN ... SET DATA TYPE BIGINT`` (int -> long promotion)."""
        self.q(f"ALTER TABLE {self.fq()} ALTER COLUMN id SET DATA TYPE BIGINT")
        assert isinstance(self.c.pi_table().schema().find_field("id").field_type, LongType)

    def rename_table(self):
        """``ALTER TABLE ... RENAME TO``; data must be readable under the new name."""
        new = self.c.new_name("renamed")
        self.q(f"ALTER TABLE {self.fq()} RENAME TO {self.rename_target(new)}")
        assert self.rows(new) == SEED

    def drop_table(self):
        """``DROP TABLE``; the table must be gone from the catalog."""
        self.q(f"DROP TABLE {self.fq()}")
        assert not self.c.catalog.table_exists(self.c.ident)

    def delete_merge_on_read(self):
        """``DELETE`` on a table with ``write.delete.mode=merge-on-read``."""
        set_props(self.c, MOR_PROPS)
        self.q(f"DELETE FROM {self.fq()} WHERE id = 2")
        assert self.rows() == [SEED[0], SEED[2]]
        assert_delete_files(self.c)

    def sees_external_commit(self):
        """Read, another client appends, read again in the same engine session."""
        assert self.rows() == SEED
        external_append(self.c, self.pool, self.NAME)
        assert self.rows() == SEED + [(4, "d", 4.0)], STALE

    def create_existing_fails(self):
        """Plain ``CREATE TABLE`` (no IF NOT EXISTS) on an existing name must fail."""
        expect_already_exists(
            lambda: self.q(f"CREATE TABLE {self.fq()} (id INTEGER, name VARCHAR, amount DOUBLE)")
        )
        assert self.rows() == SEED

    def filter_aggregate(self):
        """``count(*)`` and ``sum`` with a ``WHERE`` filter."""
        got = self.q(f"SELECT count(*), sum(amount) FROM {self.fq()} WHERE id >= 2")
        assert tuple(got[0]) == (2, 5.0)

    def list_tables(self):
        """``SHOW TABLES FROM <catalog>.<namespace>`` (Spark, Trino, Flink)."""
        qualifier = self.fq().rsplit(".", 1)[0]
        rows = self.q(f"SHOW TABLES FROM {qualifier}")
        # Spark returns (namespace, tableName, isTemporary); Trino/Flink one column.
        assert_listed(self.c, [r[1] if len(r) == 3 else r[0] for r in rows])


class DuckDBOps(SqlOps):
    """DuckDB iceberg extension (DuckDB 2.0).

    Rollback runs through the table function ``iceberg_rollback_to_snapshot``. No ``INSERT OVERWRITE`` syntax - ``overwrite`` is
    ``TRUNCATE`` + ``INSERT`` in one transaction.
    No API: ``branch_and_tag`` (refs are read-only via ``iceberg_refs``).
    (Verified by listing ``duckdb_functions()``; earlier versions of this file wrongly claimed
    that rollback and compaction were missing.)
    """

    engine_name = "duckdb"

    def create_table(self):
        """Format version is a table option in DuckDB: ``WITH ('format-version' = 'n')``."""
        self.q(f"""CREATE TABLE {self.fq()} (id INTEGER, name VARCHAR, amount DOUBLE)
                   WITH ('format-version' = '{self.c.fv}')""")
        assert self.c.pi_table().format_version == self.c.fv

    def rename_target(self, new):
        """DuckDB: the new name is given without database/schema prefix.

        Args:
            new: New table name.

        Returns:
            The plain table name.
        """
        return new

    def partition_evolution(self):
        """``ALTER TABLE ... SET PARTITIONED BY`` on a table that already has data."""
        self.q(f"ALTER TABLE {self.fq()} SET PARTITIONED BY (name)")
        self.insert()
        assert len(self.c.pi_table().spec().fields) == 1

    def time_travel(self):
        """``AT (VERSION => <snapshot id>)``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
        got = self.q(
            f"SELECT id, name, amount FROM {self.fq()} AT (VERSION => {self.c.seed_snapshot})"
        )
        assert norm(got) == SEED

    def overwrite(self):
        """``TRUNCATE`` + ``INSERT`` inside one transaction (DuckDB has no INSERT OVERWRITE)."""
        self.q("BEGIN TRANSACTION")
        self.q(f"TRUNCATE {self.fq()}")
        self.q(f"INSERT INTO {self.fq()} VALUES (9, 'z', 9.0)")
        self.q("COMMIT")
        assert self.rows() == [(9, "z", 9.0)]

    def rollback(self):
        """Table function ``iceberg_rollback_to_snapshot``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
        self.q(f"SELECT * FROM iceberg_rollback_to_snapshot('{self.fq()}', {self.c.seed_snapshot})")
        assert self.rows() == SEED

    def metadata_tables(self):
        """Table functions ``iceberg_snapshots()`` and ``iceberg_metadata()``."""
        assert len(self.q(f"SELECT * FROM iceberg_snapshots({self.fq()})")) == 1
        assert len(self.q(f"SELECT * FROM iceberg_metadata({self.fq()})")) >= 1

    def list_tables(self):
        """``duckdb_tables()`` of the attached catalog, filtered by schema = namespace."""
        db = self.fq().split(".")[0]
        rows = self.q(
            f"SELECT table_name FROM duckdb_tables() WHERE database_name = '{db}' "
            f"AND schema_name = '{self.c.ns}'"
        )
        assert_listed(self.c, [r[0] for r in rows])

    def transaction_multi_change(self):
        """``BEGIN; INSERT; DELETE; COMMIT``."""
        before = commit_count(self.c)
        self.q("BEGIN TRANSACTION")
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
        self.q(f"DELETE FROM {self.fq()} WHERE id = 2")
        self.q("COMMIT")
        assert self.rows() == [SEED[0], SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "more than one catalog commit"

    def transaction_rollback(self):
        """``BEGIN; INSERT; ROLLBACK``."""
        self.q("BEGIN TRANSACTION")
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
        self.q("ROLLBACK")
        assert self.rows() == SEED
        assert_seed_unchanged(self.c)

    def identifier_fields(self):
        """``PRIMARY KEY`` in ``CREATE TABLE``."""
        self.q(f"""CREATE TABLE {self.fq()} (id INTEGER PRIMARY KEY, name VARCHAR, amount DOUBLE)
                   WITH ('format-version' = '{self.c.fv}')""")
        s2 = self.c.pi_table().schema()
        assert s2.identifier_field_ids == [s2.find_field("id").field_id]


class SparkOps(SqlOps):
    """Spark SQL with the Iceberg extensions - supports every operation in the matrix."""

    engine_name = "spark"

    def sys(self, proc, args):
        """Call an Iceberg stored procedure, e.g. ``CALL sts.system.expire_snapshots(...)``.

        Args:
            proc: Procedure name.
            args: Procedure arguments as SQL.

        Returns:
            The procedure's result rows.
        """
        return self.q(f"CALL {SPARK_CATALOGS[WAREHOUSE]}.system.{proc}({args})")

    def rename_target(self, new):
        """Spark: target without catalog prefix (the catalog cannot change in a rename).

        Args:
            new: New table name.

        Returns:
            ``<namespace>.<new>``.
        """
        return f"{self.c.ns}.{new}"

    def create_table(self):
        """Format version as table property."""
        self.q(f"""CREATE TABLE {self.fq()} (id INT, name STRING, amount DOUBLE) USING iceberg
                   TBLPROPERTIES ('format-version' = '{self.c.fv}')""")
        assert self.c.pi_table().format_version == self.c.fv

    def add_column(self):
        """Spark spells the string type ``STRING``."""
        self.q(f"ALTER TABLE {self.fq()} ADD COLUMN extra STRING")
        assert [f.name for f in self.c.pi_table().schema().fields][-1] == "extra"

    def widen_type(self):
        """Spark syntax: ``ALTER COLUMN ... TYPE``."""
        self.q(f"ALTER TABLE {self.fq()} ALTER COLUMN id TYPE BIGINT")
        assert isinstance(self.c.pi_table().schema().find_field("id").field_type, LongType)

    def overwrite(self):
        """``INSERT OVERWRITE`` on an unpartitioned table replaces all rows."""
        self.q(f"INSERT OVERWRITE {self.fq()} VALUES (9, 'z', 9.0)")
        assert self.rows() == [(9, "z", 9.0)]

    def merge(self):
        """Spark's ``UPDATE SET *`` / ``INSERT *`` shorthand; ``D`` suffix makes doubles."""
        before = commit_count(self.c)
        self.q(f"""
            MERGE INTO {self.fq()} t
            USING (SELECT * FROM VALUES (2, 'b2', 20.0D), (4, 'd', 4.0D) AS s(id, name, amount)) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        assert self.rows() == [SEED[0], (2, "b2", 20.0), SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "MERGE was not ONE catalog commit"

    def partition_evolution(self):
        """``ALTER TABLE ... ADD PARTITION FIELD`` (Iceberg SQL extension)."""
        self.q(f"ALTER TABLE {self.fq()} ADD PARTITION FIELD name")
        self.insert()
        assert len(self.c.pi_table().spec().fields) == 1

    def time_travel(self):
        """``VERSION AS OF <snapshot id>``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
        got = self.q(
            f"SELECT id, name, amount FROM {self.fq()} VERSION AS OF {self.c.seed_snapshot}"
        )
        assert norm(got) == SEED

    def rollback(self):
        """Procedure ``rollback_to_snapshot``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
        self.sys("rollback_to_snapshot", f"'{self.c.ident}', {self.c.seed_snapshot}")
        assert self.rows() == SEED

    def branch_and_tag(self):
        """Create tag + branch, write to the branch only, verify ``main`` is untouched."""
        self.q(f"ALTER TABLE {self.fq()} CREATE TAG v1")
        self.q(f"ALTER TABLE {self.fq()} CREATE BRANCH dev")
        self.q(f"INSERT INTO {self.fq()}.branch_dev VALUES (4, 'd', 4.0)")
        assert self.rows() == SEED  # main untouched
        on_branch = self.q(f"SELECT id, name, amount FROM {self.fq()}.branch_dev")
        assert norm(on_branch) == SEED + [(4, "d", 4.0)]

    def metadata_tables(self):
        """Query every common metadata table (``<table>.<metadata table>``)."""
        for t in ("snapshots", "files", "manifests", "history", "partitions", "refs"):
            self.q(f"SELECT * FROM {self.fq()}.{t}")
        assert self.q(f"SELECT count(*) FROM {self.fq()}.snapshots")[0][0] == 1

    def identifier_fields(self):
        """``ALTER TABLE ... SET IDENTIFIER FIELDS`` on a table with ``id NOT NULL``."""
        self.q(f"""CREATE TABLE {self.fq()} (id INT NOT NULL, name STRING, amount DOUBLE)
                   USING iceberg TBLPROPERTIES ('format-version' = '{self.c.fv}')""")
        self.q(f"ALTER TABLE {self.fq()} SET IDENTIFIER FIELDS id")
        s2 = self.c.pi_table().schema()
        assert s2.identifier_field_ids == [s2.find_field("id").field_id]

    def create_existing_fails(self):
        """Plain ``CREATE TABLE ... USING iceberg`` on an existing name must fail."""
        expect_already_exists(
            lambda: self.q(
                f"CREATE TABLE {self.fq()} (id INT, name STRING, amount DOUBLE) USING iceberg"
            )
        )
        assert self.rows() == SEED

    def drop_table(self):
        """``DROP TABLE`` without ``PURGE``: Lakekeeper deletes the files itself (hard delete).

        Spark's client-side ``PURGE`` conflicts with that (see test_spark.py::
        test_drop_table_purge), so the Lakekeeper-native way is tested here.
        """
        self.q(f"DROP TABLE {self.fq()}")
        assert not self.c.catalog.table_exists(self.c.ident)


class TrinoOps(SqlOps):
    """Trino Iceberg connector.

    ``overwrite`` is attempted atomically as ``TRUNCATE`` + ``INSERT`` in one transaction;
    Trino's Iceberg connector only allows autocommit writes, so this is a known gap (Hive's
    ``insert_existing_partitions_behavior=OVERWRITE`` does not exist for Iceberg - checked).
    Maintenance (``ALTER TABLE ... EXECUTE``) is tested in ``test_maintenance.py``.
    """

    engine_name = "trino"

    def _tx(self, statements, commit=True):
        """Run ``statements`` in ONE explicit (non-autocommit) transaction, then commit/roll back.

        Args:
            statements: SQL statements.
            commit: Commit (True) or roll back (False).
        """
        from trino.transaction import IsolationLevel

        import trino
        from iceberg_lab.config import SETTINGS

        conn = trino.dbapi.connect(
            host=SETTINGS.trino_host,
            port=SETTINGS.trino_port,
            user="tests",
            catalog=self.e.catalog,
            isolation_level=IsolationLevel.SERIALIZABLE,  # = not autocommit
        )
        try:
            cur = conn.cursor()
            for sql in statements:
                cur.execute(sql)
                cur.fetchall()
            conn.commit() if commit else conn.rollback()
        finally:
            conn.close()

    def overwrite(self):
        """Atomic replace: ``TRUNCATE`` + ``INSERT`` inside one explicit transaction."""
        self._tx([f"TRUNCATE TABLE {self.fq()}", f"INSERT INTO {self.fq()} VALUES (9, 'z', 9.0E0)"])
        assert self.rows() == [(9, "z", 9.0)]

    def transaction_multi_change(self):
        """``INSERT`` + ``DELETE`` in one explicit transaction."""
        before = commit_count(self.c)
        self._tx(
            [
                f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0E0)",
                f"DELETE FROM {self.fq()} WHERE id = 2",
            ]
        )
        assert self.rows() == [SEED[0], SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "more than one catalog commit"

    def transaction_rollback(self):
        """``INSERT`` in an explicit transaction, then roll back."""
        self._tx([f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0E0)"], commit=False)
        assert self.rows() == SEED
        assert_seed_unchanged(self.c)

    def meta(self, suffix):
        """Metadata table name, e.g. ``ns."table$snapshots"`` (quoted because of ``$``).

        Args:
            suffix: Metadata table name (snapshots, files, ...).

        Returns:
            The quoted name.
        """
        return f'{self.c.ns}."{self.c.table}${suffix}"'

    def create_table(self):
        """Format version as table property ``format_version``."""
        self.q(f"""CREATE TABLE {self.fq()} (id INTEGER, name VARCHAR, amount DOUBLE)
                   WITH (format_version = {self.c.fv})""")
        assert self.c.pi_table().format_version == self.c.fv

    # Trino types ``4.0`` as DECIMAL; ``4.0E0`` is a DOUBLE literal matching the column type.

    def insert(self):
        """``INSERT`` with DOUBLE literals."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0E0)")
        assert self.rows() == SEED + [(4, "d", 4.0)]

    def update(self):
        """``UPDATE`` - writes position-delete files (v2) or deletion vectors (v3)."""
        self.q(f"UPDATE {self.fq()} SET amount = 20.0E0 WHERE id = 2")
        assert self.rows() == [SEED[0], (2, "b", 20.0), SEED[2]]

    def merge(self):
        """``MERGE`` with a plain ``VALUES`` source."""
        before = commit_count(self.c)
        self.q(f"""
            MERGE INTO {self.fq()} t
            USING (VALUES (2, 'b2', 20.0E0), (4, 'd', 4.0E0)) AS s(id, name, amount)
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount
            WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.amount)
        """)
        assert self.rows() == [SEED[0], (2, "b2", 20.0), SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "MERGE was not ONE catalog commit"

    def partition_evolution(self):
        """Changing the ``partitioning`` table property evolves the partition spec."""
        self.q(f"ALTER TABLE {self.fq()} SET PROPERTIES partitioning = ARRAY['name']")
        self.insert()
        assert len(self.c.pi_table().spec().fields) == 1

    def time_travel(self):
        """``FOR VERSION AS OF <snapshot id>``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0E0)")
        got = self.q(
            f"SELECT id, name, amount FROM {self.fq()} FOR VERSION AS OF {self.c.seed_snapshot}"
        )
        assert norm(got) == SEED

    def rollback(self):
        """``ALTER TABLE ... EXECUTE rollback_to_snapshot``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0E0)")
        self.q(f"ALTER TABLE {self.fq()} EXECUTE rollback_to_snapshot({self.c.seed_snapshot})")
        assert self.rows() == SEED

    def branch_and_tag(self):
        """Branch DDL; Trino 484 rejects it (see KNOWN_GAPS), kept to notice when it lands."""
        self.q(f"CREATE BRANCH dev IN TABLE {self.fq()}")
        self.q(f"INSERT INTO {self.fq()} @ dev VALUES (4, 'd', 4.0E0)")
        assert self.rows() == SEED
        got = self.q(f"SELECT id, name, amount FROM {self.fq()} FOR VERSION AS OF 'dev'")
        assert norm(got) == SEED + [(4, "d", 4.0)]

    def metadata_tables(self):
        """Query every ``$`` metadata table."""
        for t in ("snapshots", "files", "manifests", "history", "partitions", "refs", "properties"):
            self.q(f"SELECT * FROM {self.meta(t)}")
        assert self.q(f"SELECT count(*) FROM {self.meta('snapshots')}")[0][0] == 1


class PgLakeOps(SqlOps):
    """Postgres + pg_lake 3.7.

    Only tables pg_lake created itself are writable, so :meth:`setup` creates and seeds the
    table through pg_lake instead of PyIceberg. New writable tables are always format v2
    (v3 is skipped in the test function).

    Maintenance (``VACUUM``) is tested in ``test_maintenance.py``. Partitioning
    is the table option ``partition_by``; metadata comes from ``lake_iceberg.snapshots()``.
    No API: ``time_travel``, ``rollback``, ``branch_and_tag``. ``MERGE`` is documented as
    unsupported (KNOWN_GAPS). Every other claim here was checked against pg_lake's function
    list and documentation.
    """

    engine_name = "pg_lake"

    def _create(self, name=None):
        """``CREATE TABLE ... USING iceberg WITH (catalog = <server>)`` in schema = namespace.

        Args:
            name: Sibling table name (None: the test table).
        """
        self.q(f'CREATE SCHEMA IF NOT EXISTS "{self.c.ns}"')
        self.q(f"""CREATE TABLE {self.fq(name)} (id integer, name text, amount double precision)
                   USING iceberg WITH (catalog = '{self.e.server}')""")

    def setup(self):
        """Create and seed the test table through pg_lake (replaces the PyIceberg setup)."""
        self._create()
        self.q(f"INSERT INTO {self.fq()} VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)")

    def create_table(self):
        """pg_lake registers the table in Lakekeeper with format v2."""
        self._create()
        assert self.c.pi_table().format_version == 2

    def ctas(self):
        """CTAS needs ``USING iceberg WITH (catalog=...)``, otherwise it creates a heap table."""
        new = self.c.new_name("ctas")
        self.q(f"""CREATE TABLE {self.fq(new)} USING iceberg WITH (catalog = '{self.e.server}')
                   AS SELECT * FROM {self.fq()} WHERE id > 1""")
        assert self.rows(new) == SEED[1:]

    def add_column(self):
        """Postgres spells the string type ``text``."""
        self.q(f"ALTER TABLE {self.fq()} ADD COLUMN extra text")
        assert [f.name for f in self.c.pi_table().schema().fields][-1] == "extra"

    def widen_type(self):
        """Postgres syntax: ``ALTER COLUMN ... TYPE``."""
        self.q(f"ALTER TABLE {self.fq()} ALTER COLUMN id TYPE bigint")
        assert isinstance(self.c.pi_table().schema().find_field("id").field_type, LongType)

    def overwrite(self):
        """``TRUNCATE`` + ``INSERT`` in one Postgres transaction (one atomic Iceberg commit)."""
        with self.e.conn.transaction():
            self.q(f"TRUNCATE {self.fq()}")
            self.q(f"INSERT INTO {self.fq()} VALUES (9, 'z', 9.0)")
        assert self.rows() == [(9, "z", 9.0)]

    def partition_evolution(self):
        """Table option ``partition_by`` added to a table that already has data."""
        self.q(f"ALTER FOREIGN TABLE {self.fq()} OPTIONS (ADD partition_by 'name')")
        self.insert()
        assert len(self.c.pi_table().spec().fields) == 1

    def metadata_tables(self):
        """``lake_iceberg.snapshots()`` / ``files()`` on the table's current metadata file."""
        loc = self.c.pi_table().metadata_location
        assert len(self.q(f"SELECT * FROM lake_iceberg.snapshots('{loc}')")) >= 1
        assert len(self.q(f"SELECT * FROM lake_iceberg.files('{loc}')")) >= 1

    #: pg_lake lists only object-store tables (``lake_iceberg.list_object_store_tables``),
    #: not the tables of a REST catalog (checked in pg_proc) -> "no API".
    UNSUPPORTED = ("list_tables",)

    def transaction_multi_change(self):
        """``INSERT`` + ``DELETE`` in one Postgres transaction."""
        before = commit_count(self.c)
        with self.e.conn.transaction():
            self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
            self.q(f"DELETE FROM {self.fq()} WHERE id = 2")
        assert self.rows() == [SEED[0], SEED[2], (4, "d", 4.0)]
        assert commit_count(self.c) - before == 1, "more than one catalog commit"

    def transaction_rollback(self):
        """``INSERT`` in a Postgres transaction that is rolled back."""
        try:
            with self.e.conn.transaction():
                self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0)")
                raise _Abort
        except _Abort:
            pass
        assert self.rows() == SEED
        assert_seed_unchanged(self.c)

    def identifier_fields(self):
        """``PRIMARY KEY`` on a pg_lake Iceberg table."""
        self.q(f'CREATE SCHEMA IF NOT EXISTS "{self.c.ns}"')
        self.q(f"""CREATE TABLE {self.fq()} (id integer PRIMARY KEY, name text,
                   amount double precision) USING iceberg WITH (catalog = '{self.e.server}')""")
        s2 = self.c.pi_table().schema()
        assert s2.identifier_field_ids == [s2.find_field("id").field_id]

    def create_existing_fails(self):
        """Plain ``CREATE TABLE ... USING iceberg`` on an existing name must fail."""
        expect_already_exists(self._create)
        assert self.rows() == SEED

    def rename_table(self):
        """``ALTER TABLE ... RENAME TO`` (documented for pg_lake; REST tables are checked here)."""
        new = self.c.new_name("renamed")
        self.q(f'ALTER TABLE {self.fq()} RENAME TO "{new}"')
        assert self.c.catalog.table_exists(f"{self.c.ns}.{new}")
        assert self.rows(new) == SEED


# =========================================================================== DataFrame engines


class PolarsOps:
    """Polars DataFrame API: ``insert`` (append), ``overwrite``, ``time_travel``, ``add_column``
    (while writing: ``schema_mode="merge"``).

    Writes go through PyIceberg (``write_iceberg``), reads through Polars' own reader.
    """

    NAME = "polars"

    def __init__(self, ctx: Ctx, pool):
        """Bind the test table.

        Args:
            ctx: Context of the test table.
            pool: Engine adapter pool (unused: Polars reads via PyIceberg).
        """
        self.c = ctx
        self.pool = pool

    def scan(self, **kw):
        """Lazy Polars scan of the test table with the table's S3 access mapped to options.

        Args:
            kw: Scan options (e.g. a filter).

        Returns:
            The LazyFrame.
        """
        import polars as pl

        t = self.c.pi_table()
        return pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties), **kw)

    def rows(self, **kw):
        """Table content as sorted tuples, read by Polars' own reader.

        Args:
            kw: Scan options (e.g. a filter).

        Returns:
            Sorted tuples.
        """
        df = self.scan(**kw).collect()
        return norm(zip(df["id"], df["name"], df["amount"]))

    def insert(self):
        """``write_iceberg(mode="append")``."""
        import polars as pl

        pl.from_arrow(arrow([(4, "d", 4.0)])).write_iceberg(self.c.pi_table(), mode="append")
        assert self.rows() == SEED + [(4, "d", 4.0)]

    def overwrite(self):
        """``write_iceberg(mode="overwrite")``."""
        import polars as pl

        pl.from_arrow(arrow([(9, "z", 9.0)])).write_iceberg(self.c.pi_table(), mode="overwrite")
        assert self.rows() == [(9, "z", 9.0)]

    def time_travel(self):
        """``scan_iceberg(snapshot_id=...)``."""
        append_rows(self.c, self.pool, [(4, "d", 4.0)])
        assert self.rows(snapshot_id=self.c.seed_snapshot) == SEED

    def add_column(self):
        """Polars has no schema DDL: ``sink_iceberg(schema_mode="merge")`` adds the column while
        it appends a row that has it. Existing rows read it as NULL."""
        import polars as pl

        t = self.c.pi_table()
        pl.LazyFrame(
            {"id": [4], "name": ["d"], "amount": [4.0], "extra": ["x"]},
            schema={"id": pl.Int32, "name": pl.String, "amount": pl.Float64, "extra": pl.String},
        ).sink_iceberg(
            t, mode="append", schema_mode="merge",
            storage_options=polars_storage_options(t.io.properties),
        )  # fmt: skip
        assert [f.name for f in self.c.pi_table().schema().fields][-1] == "extra"
        df = self.scan().collect().sort("id")
        assert df["extra"].to_list() == [None, None, None, "x"]

    def sees_external_commit(self):
        """Read, another client appends, read again."""
        assert self.rows() == SEED
        external_append(self.c, self.pool, self.NAME)
        assert self.rows() == SEED + [(4, "d", 4.0)], STALE

    def filter_aggregate(self):
        """Lazy ``filter`` + ``select(len, sum)``."""
        import polars as pl

        got = self.scan().filter(pl.col("id") >= 2).select(pl.len(), pl.col("amount").sum())
        assert got.collect().row(0) == (2, 5.0)


class DaftOps:
    """Daft DataFrame API: ``insert`` (append), ``overwrite``, ``time_travel``."""

    NAME = "daft"

    def __init__(self, ctx: Ctx, pool):
        """Bind the test table.

        Args:
            ctx: Context of the test table.
            pool: Engine adapter pool (unused).
        """
        self.c = ctx
        self.pool = pool

    def rows(self, **kw):
        """Table content as sorted tuples, read by Daft.

        Args:
            kw: Unused (same signature as PolarsOps.rows).

        Returns:
            Sorted tuples.
        """
        import daft

        t = daft.read_iceberg(self.c.pi_table(), **kw).to_arrow()
        return norm(arrow_rows(t))

    def insert(self):
        """``DataFrame.write_iceberg(mode="append")``."""
        import daft

        daft.from_arrow(arrow([(4, "d", 4.0)])).write_iceberg(self.c.pi_table(), mode="append")
        assert self.rows() == SEED + [(4, "d", 4.0)]

    def overwrite(self):
        """``DataFrame.write_iceberg(mode="overwrite")``."""
        import daft

        daft.from_arrow(arrow([(9, "z", 9.0)])).write_iceberg(self.c.pi_table(), mode="overwrite")
        assert self.rows() == [(9, "z", 9.0)]

    def time_travel(self):
        """``read_iceberg(snapshot_id=...)``."""
        append_rows(self.c, self.pool, [(4, "d", 4.0)])
        assert self.rows(snapshot_id=self.c.seed_snapshot) == SEED

    def sees_external_commit(self):
        """Read, another client appends, read again."""
        assert self.rows() == SEED
        external_append(self.c, self.pool, self.NAME)
        assert self.rows() == SEED + [(4, "d", 4.0)], STALE

    def list_tables(self):
        """``daft.catalog.Catalog.from_iceberg(<catalog>).list_tables()``."""
        from daft.catalog import Catalog

        cat = Catalog.from_iceberg(pyiceberg_catalog(WAREHOUSE))
        # The pattern is the namespace (Daft passes it to the catalog's list_tables).
        assert_listed(self.c, [str(i) for i in cat.list_tables(self.c.ns)])

    def filter_aggregate(self):
        """``where`` + ``agg(count, sum)``."""
        import daft

        df = daft.read_iceberg(self.c.pi_table()).where(daft.col("id") >= 2)
        got = df.agg(daft.col("id").count().alias("n"), daft.col("amount").sum().alias("s"))
        d = got.to_pydict()
        assert (d["n"][0], d["s"][0]) == (2, 5.0)


# =========================================================================== cross-check

#: Content-changing operations and the table content every engine must see afterwards.
#: ``ctas`` is checked on the new table ``<table>_ctas``.
EXPECTED_CONTENT: dict[str, list[tuple]] = {
    "insert": SEED + [(4, "d", 4.0)],
    "overwrite": [(9, "z", 9.0)],
    "delete": [SEED[0], SEED[2]],
    "update": [SEED[0], (2, "b", 20.0), SEED[2]],
    "merge": [SEED[0], (2, "b2", 20.0), SEED[2], (4, "d", 4.0)],
    "partition_evolution": SEED + [(4, "d", 4.0)],
    "rollback": SEED,
    "ctas": SEED[1:],
    "delete_merge_on_read": [SEED[0], SEED[2]],
    "transaction_multi_change": [SEED[0], SEED[2], (4, "d", 4.0)],
}
CONTENT_OPS = sorted(EXPECTED_CONTENT)

#: Known cross-read gaps: (writer engine, reader, op, fv) globs + error regex + reason.
#: Filled from observed failures only.
# fmt: off
CROSS_GAPS = gap_table(("engine", "reader", "op", "fv"), [
    # Only operations that delete/replace rows write delete files: on v3 deletion vectors
    # (Puffin), on v2 position deletes. Spark uses copy-on-write by default, so it writes none.
    *((w, "daft", op, "3", r"not a valid parquet file", "daft-dv-as-parquet")
      for w, ops in (("duckdb", ("delete", "update", "merge", "overwrite", "delete_merge_on_read", "transaction_multi_change")), ("trino", ("delete", "update", "merge", "delete_merge_on_read")), ("spark", ("delete_merge_on_read",)))
      for op in ops),
    *((w, "pg_lake", op, "3", r"deletion vectors are not supported", "pglake-no-dv-read")
      for w, ops in (("duckdb", ("delete", "update", "merge", "overwrite", "delete_merge_on_read", "transaction_multi_change")), ("trino", ("delete", "update", "merge", "delete_merge_on_read")), ("spark", ("delete_merge_on_read",)))
      for op in ops),
    # Flink's upsert (= its MERGE) writes EQUALITY deletes; PyIceberg cannot apply them.
    *(("flink", r, "merge", "*", r"PyIceberg does not yet support equality deletes", "pyiceberg-no-equality-deletes")
      for r in ("pyiceberg", "polars", "daft")),
    # Trino's v2 position-delete files use DELTA_LENGTH_BYTE_ARRAY for file_path.
    *(("trino", "pyiceberg", op, "2", r"DeltaLengthByteArrayDecoder", "pyarrow-trino-delta-length-byte-array")
      for op in ("delete", "update", "merge", "delete_merge_on_read")),
])
# fmt: on


def cross_gap(engine: str, reader: str, op: str, fv: int):
    """The first matching :data:`CROSS_GAPS` entry, else ``None``.

    Args:
        engine: Engine that performed the operation.
        reader: Engine that reads.
        op: Operation (see ``OPERATIONS``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The gap, or None.
    """
    return find_gap(CROSS_GAPS, engine=engine, reader=reader, op=op, fv=fv)


class FlinkOps(SqlOps):
    """Apache Flink 2.3 SQL (batch mode) with the Iceberg 1.12 connector, via the SQL Gateway.

    Every claim below was probed against the running gateway:

    * ``DELETE``/``UPDATE`` are inherited and fail: Iceberg's Flink sink does not implement
      ``SupportsRowLevelDelete/Update`` (KNOWN_GAPS).
    * MERGE has no Flink SQL statement; Flink's equivalent is an *upsert* into a table with a
      primary key and ``write.upsert.enabled`` (writes equality deletes) - tested that way.
    * Rollback is attempted with ``CALL``
      and fail: ``FlinkCatalog`` does not implement ``getProcedure`` (KNOWN_GAPS). Iceberg's
      Flink maintenance exists only as a Java API (``TableMaintenance``), not in SQL.
    * Branches: writing with ``OPTIONS('branch'='dev')`` creates and fills the branch, but
      there is no SQL to create a tag (KNOWN_GAPS).
    * Partition evolution: no Flink SQL syntax exists (not offered -> no method).
    """

    engine_name = "flink"

    def meta(self, name):
        """Metadata table ``<table>$<name>`` (needs backticks in Flink SQL).

        Args:
            name: Metadata table name (snapshots, files, ...).

        Returns:
            The quoted name.
        """
        return f"{self.e.catalog}.{self.c.ns}.`{self.c.table}${name}`"

    def call(self, proc, args):
        """Iceberg procedure call; ``system`` is a reserved word in Flink SQL.

        Args:
            proc: Procedure name.
            args: Procedure arguments as SQL.

        Returns:
            The procedure's result rows.
        """
        return self.q(f"CALL {self.e.catalog}.`system`.{proc}({args})")

    def create_table(self):
        """Format version as ``WITH`` option."""
        self.q(f"""CREATE TABLE {self.fq()} (id INT, name STRING, amount DOUBLE)
                   WITH ('format-version' = '{self.c.fv}')""")
        assert self.c.pi_table().format_version == self.c.fv

    def overwrite(self):
        """``INSERT OVERWRITE`` (batch mode) replaces the unpartitioned table."""
        self.q(f"INSERT OVERWRITE {self.fq()} VALUES (9, 'z', 9.0E0)")
        assert self.rows() == [(9, "z", 9.0)]

    def identifier_fields(self):
        """``PRIMARY KEY (id) NOT ENFORCED`` becomes the identifier field."""
        self.q(f"""CREATE TABLE {self.fq()} (id INT NOT NULL, name STRING, amount DOUBLE,
                   PRIMARY KEY (id) NOT ENFORCED) WITH ('format-version' = '{self.c.fv}')""")
        s2 = self.c.pi_table().schema()
        assert s2.identifier_field_ids == [s2.find_field("id").field_id]

    def create_existing_fails(self):
        """Plain ``CREATE TABLE`` on an existing name must fail."""
        expect_already_exists(
            lambda: self.q(f"CREATE TABLE {self.fq()} (id INT, name STRING, amount DOUBLE)")
        )
        assert self.rows() == SEED

    def merge(self):
        """Upsert = Flink's MERGE: PRIMARY KEY table with ``write.upsert.enabled``.

        Needs a primary key (``id NOT NULL``), which the shared setup table lacks, so the test
        table is re-created by Flink under the same name and seeded by Flink.
        """
        self.q(f"DROP TABLE {self.fq()}")
        self.q(f"""CREATE TABLE {self.fq()} (id INT NOT NULL, name STRING, amount DOUBLE,
                   PRIMARY KEY (id) NOT ENFORCED)
                   WITH ('format-version' = '{self.c.fv}', 'write.upsert.enabled' = 'true')""")
        self.q(f"INSERT INTO {self.fq()} VALUES (1, 'a', 1.0E0), (2, 'b', 2.0E0), (3, 'c', 3.0E0)")
        self.q(f"INSERT INTO {self.fq()} VALUES (2, 'b2', 20.0E0), (4, 'd', 4.0E0)")
        assert self.rows() == [SEED[0], (2, "b2", 20.0), SEED[2], (4, "d", 4.0)]

    def add_column(self):
        """Flink syntax ``ALTER TABLE ... ADD <col> <type>``."""
        self.q(f"ALTER TABLE {self.fq()} ADD extra STRING")
        assert [f.name for f in self.c.pi_table().schema().fields][-1] == "extra"

    def rename_column(self):
        """``RENAME <old> TO <new>``; ``value`` is reserved in Flink SQL."""
        self.q(f"ALTER TABLE {self.fq()} RENAME amount TO `value`")
        assert self.c.pi_table().schema().find_field("value")

    def drop_column(self):
        """``ALTER TABLE ... DROP <col>``."""
        self.q(f"ALTER TABLE {self.fq()} DROP amount")
        assert [f.name for f in self.c.pi_table().schema().fields] == ["id", "name"]

    def widen_type(self):
        """``ALTER TABLE ... MODIFY id BIGINT``."""
        self.q(f"ALTER TABLE {self.fq()} MODIFY id BIGINT")
        assert isinstance(self.c.pi_table().schema().find_field("id").field_type, LongType)

    def time_travel(self):
        """Dynamic table option ``snapshot-id``."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0E0)")
        got = self.q(
            f"SELECT id, name, amount FROM {self.fq()} "
            f"/*+ OPTIONS('snapshot-id'='{self.c.seed_snapshot}') */"
        )
        assert norm(got) == SEED

    def rollback(self):
        """No SQL besides ``CALL`` - attempted, fails (no procedures in FlinkCatalog)."""
        self.q(f"INSERT INTO {self.fq()} VALUES (4, 'd', 4.0E0)")
        self.call("rollback_to_snapshot", f"'{self.c.ident}', {self.c.seed_snapshot}")
        assert self.rows() == SEED

    def branch_and_tag(self):
        """Branch via write option (creates it); tag has no SQL -> the test fails there."""
        self.q(f"INSERT INTO {self.fq()} /*+ OPTIONS('branch'='dev') */ VALUES (4, 'd', 4.0E0)")
        assert self.rows() == SEED  # main untouched
        on_branch = self.q(
            f"SELECT id, name, amount FROM {self.fq()} /*+ OPTIONS('branch'='dev') */"
        )
        assert norm(on_branch) == SEED + [(4, "d", 4.0)]
        raise NotImplementedError("Flink SQL has no statement to create a tag")

    def metadata_tables(self):
        """``<table>$snapshots`` etc., every column.

        The SQL Gateway's JSON format cannot encode maps with non-string keys
        (``$files.lower_bounds`` is ``MAP<INT, BYTES>``) - a limit of this lab's client, not
        of Flink - so MAP columns are read as ``CAST(... AS STRING)``.
        """
        for t in ("snapshots", "files", "manifests", "history", "partitions", "refs"):
            cols = [
                f"CAST(`{name}` AS STRING)" if typ.startswith("MAP") else f"`{name}`"
                for name, typ, *_ in self.q(f"DESCRIBE {self.meta(t)}")
            ]
            self.q(f"SELECT {', '.join(cols)} FROM {self.meta(t)}")
        assert self.q(f"SELECT count(*) FROM {self.meta('snapshots')}")[0][0] == 1


@pytest.fixture(scope="session")
def op_results():
    """``(engine, op, fv) -> (namespace, table)`` of successful content operations.

    Returns:
        An empty dict, filled by test_operation.
    """
    return {}


#: Ops implementation per engine name (same names as in conftest.ENGINES).
OPS_CLASSES = {
    "pyiceberg": PyIcebergOps,
    "duckdb": DuckDBOps,
    "spark": SparkOps,
    "trino": TrinoOps,
    "polars": PolarsOps,
    "daft": DaftOps,
    "pg_lake": PgLakeOps,
    "flink": FlinkOps,
}

#: Operations that start without a pre-created table.
CREATES_OWN_TABLE = {"create_table", "identifier_fields"}


# =========================================================================== test


@pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
@pytest.mark.parametrize("op", OPERATIONS)
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_operation(engine, op, fv, namespace, engine_pool, op_results):
    """The engine performs an operation on a format-v2/v3 table and the result is verified.

    The engine writes its own setup rows (no other engine steps in).

    Checks (per operation, in the engine's ``Ops`` class):
        * the content the operation must leave (insert, overwrite, delete, update, merge as
          one commit, CTAS, ...);
        * its metadata effect (schema change, partition spec, snapshot/ref, delete files for
          merge-on-read, one commit per transaction);
        * errors where they must come (create of an existing table).
    If the content is right but a later check fails, the table still goes to the cross-read.
    Engines without an API for the operation: not offered.

    Args:
        engine: Engine under test.
        op: Operation (see ``OPERATIONS``).
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        op_results: ``(engine, op, fv) -> (namespace, table)`` of the tables for the cross-read
            (None: attempted without a readable result).
    """
    if op in EXPECTED_CONTENT:
        op_results[(engine, op, fv)] = (
            None  # attempted (see iceberg_lab.lab.results.skip_if_not_produced)
        )
    cls = OPS_CLASSES.get(engine)
    if cls is None or not hasattr(cls, op) or op in getattr(cls, "UNSUPPORTED", ()):
        not_offered(engine, op, None, engine_pool)
    require_format(engine, fv)
    gap = known_gap(engine, op, fv)

    catalog = pyiceberg_catalog(WAREHOUSE)
    ctx = Ctx(
        catalog,
        namespace,
        f"op_{engine}_{op}_v{fv}_{uuid.uuid4().hex[:6]}",
        fv,
        seed_writer=seed_writer_for(engine, fv),
    )
    ops = cls(ctx, engine_pool)
    produced = (namespace, ctx.new_name("ctas") if op == "ctas" else ctx.table)

    def content_ok():
        """The engine's own read of the produced table equals the operation's expected content.

        Returns:
            Whether it does.
        """
        return (
            op in EXPECTED_CONTENT
            and fresh(engine_pool(engine), *produced).read(*produced) == EXPECTED_CONTENT[op]
        )

    with (
        keep_if_content_ok(op_results, (engine, op, fv), produced, content_ok),
        expect(gap),
    ):
        # Setup is part of the checked block: the engine writes its own setup rows, and if
        # it cannot (e.g. v3), that is the test result.
        if op not in CREATES_OWN_TABLE and hasattr(ops, "setup"):
            ops.setup()
            ctx.seed_snapshot = ctx.pi_table().current_snapshot().snapshot_id
        elif op not in CREATES_OWN_TABLE:
            props = {"format-version": str(fv)}
            if (
                op == "delete_merge_on_read"
            ):  # at creation: engines cache the table from the setup on
                props |= MOR_PROPS
            t = catalog.create_table(ctx.ident, schema=SCHEMA, properties=props)
            append_rows(ctx, engine_pool, SEED)  # written by the engine under test
            ctx.seed_snapshot = t.refresh().current_snapshot().snapshot_id
        getattr(ops, op)()

    # Success: remember the table so every other engine can re-read it (test below).
    if op in EXPECTED_CONTENT:
        op_results[(engine, op, fv)] = produced


@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
@pytest.mark.parametrize("op", CONTENT_OPS)
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_operation_cross_read(engine, op, fv, reader, engine_pool, op_results):
    """After the engine's operation, every other engine reads exactly the expected content.

    Catches engines whose writes (delete files, deletion vectors, rewritten files) only they
    themselves can read.

    Checks:
        * the reader (cache dropped) returns exactly the expected content.
        Skipped (blocked) if the operation produced no readable table.

    Args:
        engine: Engine under test.
        op: Operation (see ``OPERATIONS``).
        fv: Iceberg format version of the table (2 or 3).
        reader: Engine that reads it.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        op_results: ``(engine, op, fv) -> (namespace, table)`` of the tables for the cross-read
            (None: attempted without a readable result).
    """
    if reader == engine:
        pytest.skip("own result is verified in test_operation")
    ns, table = skip_if_not_produced(op_results, (engine, op, fv), "test_operation")
    gap = cross_gap(engine, reader, op, fv)
    with expect(gap):
        assert fresh(engine_pool(reader), ns, table).read(ns, table) == EXPECTED_CONTENT[op]


def test_ops_classes_define_only_known_operations():
    """Guard: a method-name typo would silently turn an operation into "no API".

    Checks:
        * every public method of each engine's ``Ops`` class is a known operation or helper.
    """
    helpers = {"setup", "fq", "q", "rows", "rename_target", "sys", "call", "meta", "scan"}
    for engine, cls in OPS_CLASSES.items():
        public = {m for m in dir(cls) if not m.startswith("_") and callable(getattr(cls, m))}
        unknown = public - set(OPERATIONS) - helpers - set(getattr(cls, "UNSUPPORTED", ()))
        assert not unknown, f"{engine}: methods that are no operation: {sorted(unknown)}"
