"""Concurrent commits: Iceberg's optimistic concurrency, per engine and across engines.

Every Iceberg commit is "swap the metadata pointer if it still points where I started"
(the catalog answers a stale commit with 409 CommitFailed). Engines are expected to retry
non-conflicting commits (appends) and to refuse - not silently lose - conflicting ones.

Tests (each thread uses its OWN adapter instance: own connection, session or catalog client):

``test_concurrent_appends``
    4 threads of the same engine append one row each at the same moment. Expected: no
    error, all 4 rows present (the losers of the race retry).
``test_all_engines_append_concurrently``
    Every available engine appends its own row to ONE shared table at the same moment.
``test_append_during_compaction``
    The engine compacts the table while its own appends keep committing. Expected: no data
    loss, both sides succeed (a rewrite does not conflict with appends).
``test_conflicting_delete_and_update``
    ``DELETE ... WHERE id = 2`` and ``UPDATE ... WHERE id = 2`` at the same moment. Valid
    outcomes: both applied in some order (row 2 gone), or one refused with a conflict.
    Invalid: row 2 still/again present although the DELETE reported success (lost delete),
    or row 2 duplicated.
``test_concurrent_upsert_same_key``
    Two clients upsert (MERGE) the same NEW key at the same moment. Valid: the key exists
    exactly once (both applied in order, or one refused). Invalid: the key twice.
"""

from __future__ import annotations

import re
import threading
import uuid

import pytest
import test_maintenance as maint
import test_operations as base

from iceberg_lab.config import PROFILE
from iceberg_lab.engines import SparkEngine, pyiceberg_catalog

from conftest import (
    ENGINE_CLASSES,
    ENGINES,
    Gap,
    engine_param,
    expect,
    find_gap,
    gap_table,
    not_offered,
    require,
    require_engine,
    require_format,
)

WAREHOUSE = base.WAREHOUSE
N_THREADS = 4


# Filled from observed failures only. Rows that apply to a case are alternatives: the first
# whose regex matches the error names the cause (gap_register.find_gap).
# fmt: off
KNOWN_GAPS = gap_table(("test", "engine", "fv"), [
    # RustFS: an alternative to the 409 entry below (the error picks the row).
    *(("test_append_during_compaction", "duckdb", "2", r"HTTP GET error reading 's3://labtables/\.rustfs-table/\S+/metadata/\d+-[0-9a-f-]+\.metadata\.json' \(HTTP 403 Forbidden\)", "rustfs-deletes-metadata-log")
      for _ in [1] if PROFILE.name == "rustfs"),
    ("test_append_during_compaction", "duckdb", "2", r"Conflict_409", "duckdb-no-retry-on-conflict"),
    ("test_append_during_compaction", "duckdb", "3", r"iceberg_rewrite_data_files does not yet support V3 tables", "duckdb-no-v3-rewrite"),
    ("test_append_during_compaction", "flink", "*", r"getProcedure is not implemented for class org\.apache\.iceberg\.flink\.FlinkCatalog", "flink-no-procedures"),
    # DuckDB does not retry a commit that lost the race; whether it loses depends on timing.
    *((t, "duckdb", "*", r"Conflict_409", "duckdb-no-retry-on-conflict", True)
      for t in ("test_concurrent_appends", "test_all_engines_append_concurrently")),
    # Seen on Unity only: 8 writers race, PyIceberg's 4 default commit retries
    # (commit.retry.num-retries) can all lose - depends on timing.
    *(("test_all_engines_append_concurrently", e, "2", r"CommitFailedException: (Requirement failed: branch main has changed|Metadata location for \S+ changed concurrently)", "pyiceberg-commit-retries-exhausted", True)
      for e in ("pyiceberg", "polars", "daft") if PROFILE.name == "unity"),
    # DuckDB commits through /transactions/commit, where the REST fixture drops conflicting
    # changes and still answers 204; how many get lost depends on timing.
    *(("test_concurrent_appends", "duckdb", "*", r"appends confirmed but lost", "reference-tx-commit-loses-updates", True)
      for _ in [1] if PROFILE.name == "reference"),
    # MERGE's default isolation is serializable (write.merge.isolation-level): a concurrent
    # insert that matches the MERGE condition must make one of them fail.
    ("test_concurrent_upsert_same_key", "pyiceberg", "2", r"key 4 2 times after two concurrent upserts", "pyiceberg-concurrent-upsert-duplicates"),
    # DuckDB 2.0.1 fixed "two concurrent MERGE INTO of the same new key both succeed" (5/5 runs).
    # Setup by the engine itself: PyIceberg (and Polars/Daft through it) cannot write v3 data.
    *(("*", e, "3", base.NO_V3_MSG, "pyiceberg-v3-data-write") for e in ("pyiceberg", "polars", "daft")),
])
# fmt: on


def known_gap(test: str, engine: str, fv: int) -> Gap | None:
    """First matching gap of a concurrency test.

    Args:
        test: Test name in ``KNOWN_GAPS``.
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The gap, or None.
    """
    return find_gap(KNOWN_GAPS, test=test, engine=engine, fv=fv)


def _gap_ctx(test, engine, fv):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        test: Test name in ``KNOWN_GAPS``.
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The context manager.
    """
    return expect(known_gap(test, engine, fv))


# =========================================================================== helpers


@pytest.fixture
def fresh_engine(request):
    """``fresh_engine(name)`` -> a NEW adapter (own connection/session), closed after the test.

    Args:
        request: pytest request of the calling test or fixture.

    Yields:
        ``make(name) -> Engine``.
    """
    made = []

    def make(name):
        """A new adapter for the engine (registered for closing).

        Args:
            name: Engine name.

        Returns:
            The adapter.
        """
        require_engine(name)
        if name == "spark":
            e = SparkEngine(request.getfixturevalue("spark"), WAREHOUSE)
        else:
            e = ENGINE_CLASSES[name](WAREHOUSE)
        made.append(e)
        return e

    yield make
    for e in made:
        e.close()


def append_one(engine: str, adapter, ns: str, table: str, row: tuple) -> None:
    """Append ``row`` with ``engine``'s own write path and a thread-private client.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.
        table: Table name (without namespace).
        row: Row ``(id, name, amount)``.
    """
    if engine in ("pyiceberg", "polars", "daft"):
        t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")  # own catalog client
        data = base.arrow([row])
        if engine == "pyiceberg":
            t.append(data)
        elif engine == "polars":
            import polars as pl

            pl.from_arrow(data).write_iceberg(t, mode="append")
        else:
            import daft

            daft.from_arrow(data).write_iceberg(t, mode="append")
        return
    i, n, a = row
    adapter.execute(f"INSERT INTO {adapter.fq(ns, table)} VALUES ({i}, '{n}', {a!r}E0)")


def run_parallel(jobs) -> list[BaseException | None]:
    """Start all ``jobs`` (callables) at the same moment; return each job's exception or None.

    Args:
        jobs: Callables to start together.

    Returns:
        Per job its exception, or None.
    """
    barrier = threading.Barrier(len(jobs))
    errors: list[BaseException | None] = [None] * len(jobs)

    def wrap(k, job):
        """Wait at the barrier, run the job, store its exception.

        Args:
            k: Index of the job.
            job: The job.
        """
        barrier.wait()
        try:
            job()
        except BaseException as exc:  # noqa: BLE001 - reported per job
            errors[k] = exc

    threads = [threading.Thread(target=wrap, args=(k, j)) for k, j in enumerate(jobs)]
    for t in threads:
        t.start()
    for k, t in enumerate(threads):
        t.join(timeout=600)
        if t.is_alive():  # a hanging job must not count as success
            errors[k] = TimeoutError(f"job {k} still running after 600 s")
    return errors


def raise_first(errors, what: str):
    """Re-raise the first job error (its message is what known gaps match against).

    Args:
        errors: Per-job exception or None.
        what: What the jobs did (for the message).
    """
    for k, e in enumerate(errors):
        if e is not None:
            raise AssertionError(f"{what} job {k} failed: {type(e).__name__}: {e}") from e


def seed_table(engine: str, ctx: base.Ctx, pool) -> None:
    """Create the test table and write :data:`test_operations.SEED` with ``engine`` itself.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        ctx: Context of the test table.
        pool: Engine adapter pool (``engine_pool`` fixture).
    """
    if engine == "pg_lake":
        base.PgLakeOps(ctx, pool).setup()
        return
    ctx.catalog.create_table(
        ctx.ident,
        schema=base.SCHEMA,
        properties={"format-version": str(ctx.fv), **maint.TABLE_PROPS},
    )
    base.append_rows(ctx, pool, base.SEED)


def new_ctx(prefix: str, engine: str, fv: int, namespace: str) -> base.Ctx:
    """Context of a new, unique test table.

    Args:
        prefix: Table name prefix.
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        The context.
    """
    return base.Ctx(
        pyiceberg_catalog(WAREHOUSE),
        namespace,
        f"{prefix}_{engine}_v{fv}_{uuid.uuid4().hex[:6]}",
        fv,
        seed_writer=engine,
    )


FV = pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")


def _flink_merge_refused(engine_pool, ctx) -> None:
    """Flink: try a MERGE on a table without primary key; skip as not offered only when Flink
    refuses the statement itself (its upsert needs a primary key - test_operations: merge).
    If the MERGE works, the claim is outdated and the test fails.

    Args:
        engine_pool: Session pool of engine adapters.
        ctx: Context of the test table.
    """
    ctx.catalog.create_table(
        ctx.ident, schema=base.SCHEMA, properties={"format-version": str(ctx.fv)}
    )
    f = engine_pool("flink")
    fq = f.fq(ctx.ns, ctx.table)
    try:
        f.execute(
            f"MERGE INTO {fq} t USING (SELECT 4 AS id, 'x' AS name, 4.0 AS amount) s "
            "ON t.id = s.id WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.amount)"
        )
    except Exception as e:
        first = str(e).strip().splitlines()[0][:160] if str(e).strip() else type(e).__name__
        # Flink 2.3 parses MERGE INTO but its planner fails validating it ("SQL validation
        # failed", NullPointerException: rowType in FlinkCalciteSqlValidator.validateJoin) -
        # refused before any job runs, like the earlier parse error.
        if re.search(
            r"(?i)merge|parse|encountered|not support|unsupported|validation|rowType", str(e)
        ):
            pytest.skip(f"flink has no API for MERGE (refused: {first})")
        raise
    pytest.fail("[claim outdated?] Flink accepted MERGE INTO - test the concurrent upsert with it")


def _skip_pg_lake_v3(engine, fv):
    """Skip a format-v3 case for an engine that creates no v3 tables (capability registry).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).
    """
    require_format(engine, fv)


# =========================================================================== tests


@pytest.mark.slow
@FV
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_concurrent_appends(engine, fv, namespace, engine_pool, fresh_engine):
    """4 threads of the same engine append one row each at the same time; all rows arrive.

    Each thread uses its own adapter (own connection/session), as separate clients would.

    Checks:
        * after the seed rows and the 4 concurrent appends, the engine reads exactly seed + the
          4 rows (none lost, none twice); a commit conflict the engine does not retry is the
          documented (intermittent) gap.

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        fresh_engine: Factory for a NEW adapter of an engine (own connection), closed afterwards.
    """
    _skip_pg_lake_v3(engine, fv)
    ctx = new_ctx("cc", engine, fv, namespace)
    rows = [(10 + k, f"t{k}", float(10 + k)) for k in range(N_THREADS)]
    with _gap_ctx("test_concurrent_appends", engine, fv):
        seed_table(engine, ctx, engine_pool)
        adapters = [fresh_engine(engine) for _ in rows]
        errors = run_parallel(
            [
                (lambda a=a, r=r: append_one(engine, a, ctx.ns, ctx.table, r))
                for a, r in zip(adapters, rows, strict=True)
            ]
        )
        raise_first(errors, "append")
        got = engine_pool(engine).read(ctx.ns, ctx.table)
        lost = [r for r in rows if r not in got]
        assert not lost, f"appends confirmed but lost: {lost}"
        assert got == base.SEED + rows


@pytest.fixture(scope="module", params=[2, 3], ids=lambda v: f"v{v}")
def shared_append(request, namespace, engine_pool):
    """One table (created by PyIceberg), every available engine appends simultaneously.

    pg_lake is left out: it writes only tables it created itself. Returns
    ``(fv, ns, table, {engine: (row, error)})``.

    Args:
        request: pytest request of the calling test or fixture.
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).

    Yields:
        ``(fv, namespace, table, {engine: (row, error)})``.
    """
    from conftest import AVAILABILITY

    fv = request.param
    writers = [e for e in ENGINES if e != "pg_lake" and AVAILABILITY[e][0]]
    ctx = new_ctx("ccall", "all", fv, namespace)
    ctx.catalog.create_table(ctx.ident, schema=base.SCHEMA, properties={"format-version": str(fv)})
    adapters, setup_errors = {}, {}
    for e in writers:
        try:
            if e == "spark":
                adapters[e] = SparkEngine(request.getfixturevalue("spark"), WAREHOUSE)
            else:
                adapters[e] = ENGINE_CLASSES[e](WAREHOUSE)
        except Exception as exc:  # noqa: BLE001 - reported as this engine's result only
            setup_errors[e] = exc
    rows = {e: (100 + k, e, float(k)) for k, e in enumerate(writers)}
    racing = [e for e in writers if e in adapters]
    errors = run_parallel(
        [(lambda e=e: append_one(e, adapters[e], ctx.ns, ctx.table, rows[e])) for e in racing]
    )
    results = {e: (rows[e], err) for e, err in zip(racing, errors, strict=True)}
    results |= {e: (rows[e], exc) for e, exc in setup_errors.items()}
    yield fv, ctx.ns, ctx.table, results
    for a in adapters.values():
        a.close()


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_all_engines_append_concurrently(engine, shared_append):
    """Every engine appends to ONE shared table at the same time; its row is not lost.

    The shared fixture starts all engines' appends together; this test checks one engine's part.

    Checks:
        * the engine's append did not fail (a conflict it does not retry is a documented,
          intermittent gap);
        * its row is in the table as PyIceberg reads it.
        pg_lake: not offered (writes only tables it created).

    Args:
        engine: Engine under test.
        shared_append: ``(fv, namespace, table, {engine: (row, error)})`` - one table that all
            available engines appended to at the same time (parametrized v2/v3).
    """
    fv, ns, table, results = shared_append
    require(engine, "writing tables it did not create")
    row, err = results[engine]
    with _gap_ctx("test_all_engines_append_concurrently", engine, fv):
        if err is not None:
            raise AssertionError(f"{type(err).__name__}: {err}") from err
        t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
        assert row in set(base.arrow_rows(t.scan().to_arrow())), "row lost"


@pytest.mark.slow
@FV
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_append_during_compaction(engine, fv, namespace, engine_pool, fresh_engine):
    """The engine compacts while its own appends commit; nothing gets lost.

    Checks:
        * compaction (the engine's own API) and appends run in parallel;
        * the engine then reads exactly the expected rows: seed + all appended rows.
        Engines without a compaction API: not offered.

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        fresh_engine: Factory for a NEW adapter of an engine (own connection), closed afterwards.
    """
    cls = maint.MAINT_CLASSES.get(engine)
    if cls is None or not hasattr(cls, "compact_data_files"):
        not_offered(engine, "compact_data_files", None, engine_pool)
    _skip_pg_lake_v3(engine, fv)
    ctx = new_ctx("ccmp", engine, fv, namespace)
    extra = [(20 + k, f"w{k}", float(20 + k)) for k in range(3)]
    with _gap_ctx("test_append_during_compaction", engine, fv):
        seed_table(engine, ctx, engine_pool)
        base.append_rows(ctx, engine_pool, [(4, "d", 4.0)])
        base.append_rows(ctx, engine_pool, [(5, "e", 5.0)])
        writer = fresh_engine(engine)
        compactor = cls(ctx, lambda name: fresh_engine(name))

        def appends():
            for r in extra:
                append_one(engine, writer, ctx.ns, ctx.table, r)

        errors = run_parallel([compactor.compact_data_files, appends])
        raise_first(errors, "compaction/append")
        expected = base.SEED + [(4, "d", 4.0), (5, "e", 5.0)] + extra
        assert engine_pool(engine).read(ctx.ns, ctx.table) == expected


@pytest.mark.slow
@FV
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_conflicting_delete_and_update(engine, fv, namespace, engine_pool, fresh_engine):
    """Simultaneous DELETE and UPDATE of row 2: a serializable outcome, no lost delete.

    Checks:
        * row 2 is at most once in the table, rows 1 and 3 are unchanged;
        * if the DELETE succeeded, row 2 is gone (an UPDATE may not resurrect it);
        * if the DELETE was refused, row 2 shows the UPDATE (amount 20.0);
        * both refused: the test fails with the first error (nothing applied at all).
        Engines without SQL DELETE and UPDATE: not offered.

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        fresh_engine: Factory for a NEW adapter of an engine (own connection), closed afterwards.
    """
    require(engine, "SQL DELETE + UPDATE", engine_pool)
    _skip_pg_lake_v3(engine, fv)
    ctx = new_ctx("ccdu", engine, fv, namespace)
    with _gap_ctx("test_conflicting_delete_and_update", engine, fv):
        seed_table(engine, ctx, engine_pool)
        d, u = fresh_engine(engine), fresh_engine(engine)
        fq = d.fq(ctx.ns, ctx.table)
        errors = run_parallel(
            [
                lambda: d.execute(f"DELETE FROM {fq} WHERE id = 2"),
                lambda: u.execute(f"UPDATE {fq} SET amount = 20.0E0 WHERE id = 2"),
            ]
        )
        delete_err, update_err = errors
        if delete_err and update_err:
            raise_first(errors, "delete/update")  # both refused: nothing was applied at all
        rows = engine_pool(engine).read(ctx.ns, ctx.table)
        row2 = [r for r in rows if r[0] == 2]
        assert len(row2) <= 1, f"row 2 duplicated: {row2}"
        assert [r for r in rows if r[0] != 2] == [base.SEED[0], base.SEED[2]], rows
        if delete_err is None:
            assert not row2, f"lost delete: DELETE succeeded but row 2 is present: {row2}"
        else:
            assert row2 == [(2, "b", 20.0)], f"DELETE refused, so the UPDATE must show: {row2}"


#: MERGE of one row (``row``) on key id, per SQL dialect.
def _merge_sql(engine: str, fq: str, row: tuple) -> str:
    """MERGE (upsert on id) of one row in the engine's dialect.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        fq: Fully qualified table name in the engine's dialect.
        row: Row ``(id, name, amount)``.

    Returns:
        The statement.
    """
    i, n, a = row
    if engine == "spark":
        source = f"SELECT * FROM VALUES ({i}, '{n}', {a}D) AS s(id, name, amount)"
        return (
            f"MERGE INTO {fq} t USING ({source}) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    source = f"(VALUES ({i}, '{n}', {a}E0)) AS s(id, name, amount)"
    if engine != "trino":
        source = f"(SELECT * FROM {source}) s"
    return (
        f"MERGE INTO {fq} t USING {source} ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount "
        "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.amount)"
    )


@pytest.mark.slow
@FV
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_concurrent_upsert_same_key(engine, fv, namespace, engine_pool, fresh_engine):
    """Two clients upsert the same NEW key at the same moment.

    Valid: the key exists exactly once afterwards (both applied in some order, or one refused
    with a conflict). Invalid: the key twice - a duplicate primary key.

    Checks:
        * key 4 is exactly once in the table;
        * the seed rows are unchanged.
        Not offered, each checked: Polars/Daft (no upsert in their live API), pg_lake (its MERGE
        fails in test_operations), Flink (refuses MERGE INTO on a table without primary key).

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        fresh_engine: Factory for a NEW adapter of an engine (own connection), closed afterwards.
    """
    require(engine, "upsert/MERGE", engine_pool)
    _skip_pg_lake_v3(engine, fv)
    ctx = new_ctx("ccup", engine, fv, namespace)
    if engine == "flink":
        _flink_merge_refused(engine_pool, ctx)
    with _gap_ctx("test_concurrent_upsert_same_key", engine, fv):
        seed_table(engine, ctx, engine_pool)
        rows = [(4, "x", 4.0), (4, "y", 5.0)]
        if engine == "pyiceberg":
            tables = [pyiceberg_catalog(WAREHOUSE).load_table(ctx.ident) for _ in rows]
            jobs = [
                lambda t=t, r=r: t.upsert(base.arrow([r]), join_cols=["id"])
                for t, r in zip(tables, rows, strict=True)
            ]
        else:
            adapters = [fresh_engine(engine) for _ in rows]
            fq = adapters[0].fq(ctx.ns, ctx.table)
            jobs = [
                lambda a=a, r=r: a.execute(_merge_sql(engine, fq, r))
                for a, r in zip(adapters, rows, strict=True)
            ]
        errors = run_parallel(jobs)
        if all(errors):
            raise_first(errors, "upsert")  # both refused: nothing was applied at all
        got = engine_pool(engine).read(ctx.ns, ctx.table)
        key4 = [r for r in got if r[0] == 4]
        assert len(key4) == 1, f"key 4 {len(key4)} times after two concurrent upserts: {key4}"
        assert [r for r in got if r[0] != 4] == base.SEED, got
