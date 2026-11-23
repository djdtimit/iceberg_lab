"""Engine-specific: Postgres + pg_lake (not part of the comparison).

Needs: docker compose --profile pglake up -d --build pg-lake. Runs on both warehouses.

pg_lake runs in every comparison matrix as engine ``pg_lake``; this file covers what only
matters for it:
  - the pg_lake extensions are installed;
  - tables other engines created are attached read-only, always show the latest commit and
    refuse writes;
  - an Iceberg table joins with a regular Postgres heap table;
  - an INSERT pg_lake reports as successful is really committed in the catalog (Unity: writes
    silently lost - documented), and a CREATE the catalog refuses fails in pg_lake too;
  - ``lake_iceberg.catalog_stats()`` (new in 3.7) counts pg_lake's REST catalog requests.
"""

# Moved to the base matrices: visibility -> test_interop; DML -> test_operations;
# rollback -> transaction_rollback. Left here: pg_lake's own mechanisms (extension, attached
# read-only tables, join with heap tables) and the two silent-success findings.

import uuid

import psycopg
import pyarrow as pa
import pytest

from iceberg_lab.engines import SCHEMA, PgLakeEngine, PyIcebergEngine

from conftest import expect, require_engine, warehouse_params
from conftest import gap as gap_of


@pytest.fixture(autouse=True)
def _engine_available():
    """Skip (or fail with REQUIRE_ENGINES) every test here if the engine is not available."""
    require_engine("pg_lake")


SEED = [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)]


@pytest.fixture(params=warehouse_params())
def pg(request):
    """pg_lake adapter, once per warehouse (pg_lake works the same on both).

    Args:
        request: pytest request of the calling test or fixture.

    Yields:
        The adapter (closed afterwards).
    """
    engine = PgLakeEngine(request.param)
    yield engine
    engine.close()


@pytest.fixture
def owned(pg, namespace):
    """A table created and seeded by pg_lake itself.

    Args:
        pg: pg_lake adapter.
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        The table name.
    """
    table = f"pg_{uuid.uuid4().hex[:8]}"
    pg.write(
        namespace, table, pa.table(dict(zip(("id", "name", "amount"), zip(*SEED))), schema=SCHEMA)
    )
    return table


def test_extension_installed(pg):
    """The pg_lake extensions are installed in the database.

    Checks:
        * ``pg_lake`` and ``pg_lake_iceberg`` are among the installed extensions.

    Args:
        pg: pg_lake adapter, once per warehouse.
    """
    names = {r[0] for r in pg.execute("SELECT extname FROM pg_extension")}
    assert {"pg_lake", "pg_lake_iceberg"} <= names


def test_attached_table_follows_external_commits(pg, namespace):
    """An attached table always shows the catalog's latest commit, without re-attaching.

    Checks:
        * pg_lake reads the seed rows;
        * after another client appends a row, pg_lake reads 4 rows.

    Args:
        pg: pg_lake adapter, once per warehouse.
        namespace: Fresh namespace of the test session.
    """
    pi = PyIcebergEngine(pg.warehouse)
    table = f"ext_{uuid.uuid4().hex[:8]}"
    data = pa.table(dict(zip(("id", "name", "amount"), zip(*SEED))), schema=SCHEMA)
    pi.write(namespace, table, data)
    assert pg.read(namespace, table) == SEED

    pi.catalog.load_table(f"{namespace}.{table}").append(data.slice(0, 1))
    assert len(pg.read(namespace, table)) == 4  # no re-attach needed


def test_attached_table_is_read_only(pg, namespace):
    """pg_lake refuses writes to tables other engines created.

    Checks:
        * an INSERT into the attached table fails with "foreign table ... does not allow
          inserts".

    Args:
        pg: pg_lake adapter, once per warehouse.
        namespace: Fresh namespace of the test session.
    """
    table = f"ro_{uuid.uuid4().hex[:8]}"
    PyIcebergEngine(pg.warehouse).write(
        namespace, table, pa.table({"id": [1], "name": ["a"], "amount": [1.0]}, schema=SCHEMA)
    )
    pg.read(namespace, table)  # attaches
    with pytest.raises(psycopg.errors.Error, match=r"foreign table .* does not allow inserts"):
        pg.execute(f"INSERT INTO {pg.fq(namespace, table)} VALUES (2, 'b', 2)")


def test_join_iceberg_with_heap_table(pg, owned, namespace):
    """An Iceberg table can be joined with a regular Postgres table.

    Checks:
        * the join returns exactly the two expected rows.

    Args:
        pg: pg_lake adapter, once per warehouse.
        owned: A table created and seeded by pg_lake itself.
        namespace: Fresh namespace of the test session.
    """
    pg.execute("CREATE TABLE IF NOT EXISTS public.labels (id bigint PRIMARY KEY, label text)")
    pg.execute("INSERT INTO public.labels VALUES (1, 'one'), (2, 'two') ON CONFLICT DO NOTHING")
    rows = pg.execute(
        f"SELECT l.label, t.amount FROM {pg.fq(namespace, owned)} t "
        "JOIN public.labels l USING (id) ORDER BY 1"
    )
    assert [tuple(r) for r in rows] == [("one", 1.0), ("two", 2.0)]


def test_insert_commits_to_catalog(pg, namespace):
    """An INSERT that pg_lake reports as successful is committed in the catalog.

    Checks:
        * pg_lake itself counts 2 rows;
        * the catalog has a snapshot and PyIceberg reads 2 rows (Unity: writes silently lost
          - documented data-loss gap).

    Args:
        pg: pg_lake adapter, once per warehouse.
        namespace: Fresh namespace of the test session.
    """
    from iceberg_lab.engines import pyiceberg_catalog

    table = f"pgc_{uuid.uuid4().hex[:8]}"
    pg.execute(f'CREATE SCHEMA IF NOT EXISTS "{namespace}"')
    pg.execute(f"""CREATE TABLE "{namespace}"."{table}" (id bigint)
                   USING iceberg WITH (catalog = '{pg.server}')""")
    pg.owned.add((namespace, table))
    pg.execute_raw(f'INSERT INTO "{namespace}"."{table}" VALUES (1), (2)')
    assert pg.execute_raw(f'SELECT count(*) FROM "{namespace}"."{table}"') == [(2,)]
    # Without /transactions/commit (Unity) pg_lake's INSERT reports success and pg_lake reads
    # its rows, but the catalog never gets the snapshot.
    gap = (
        None
        if pg.tx_commit
        else gap_of("pglake-ignores-refused-commit", r"no snapshot in the catalog")
    )
    with expect(gap):
        t = pyiceberg_catalog(pg.warehouse).load_table(f"{namespace}.{table}")
        assert t.current_snapshot() is not None, "INSERT succeeded, but no snapshot in the catalog"
        assert t.scan().to_arrow().num_rows == 2


def test_create_refused_by_catalog_is_an_error(pg, namespace):
    """A CREATE TABLE that the catalog refuses fails in pg_lake instead of reporting success.

    Checks:
        * either pg_lake reports the refusal (Unity refuses TIME: correct),
        * or the table really exists in the catalog.

    Args:
        pg: pg_lake adapter, once per warehouse.
        namespace: Fresh namespace of the test session.
    """
    from iceberg_lab.config import PROFILE
    from iceberg_lab.engines import pyiceberg_catalog

    table = f"pgr_{uuid.uuid4().hex[:8]}"
    pg.execute(f'CREATE SCHEMA IF NOT EXISTS "{namespace}"')
    gap = (
        gap_of(
            "pglake-ignores-refused-create",
            r"pg_lake reported success, but the table is not in the catalog",
        )
        if PROFILE.name == "unity"
        else None
    )
    with expect(gap):
        try:
            pg.execute(f"""CREATE TABLE "{namespace}"."{table}" (id bigint, v time)
                           USING iceberg WITH (catalog = '{pg.server}')""")
        except psycopg.Error:
            if PROFILE.name == "unity":
                return  # the catalog refuses TIME, and pg_lake says so: correct
            raise
        pg.owned.add((namespace, table))
        assert pyiceberg_catalog(pg.warehouse).table_exists(f"{namespace}.{table}"), (
            "pg_lake reported success, but the table is not in the catalog"
        )


def _catalog_requests(pg) -> dict[tuple[str, str], int]:
    """pg_lake's REST catalog request counters (``lake_iceberg.catalog_stats``).

    Args:
        pg: pg_lake adapter.

    Returns:
        ``(operation, result) -> requests`` summed over pg_lake's catalog groups.
    """
    rows = pg.execute(
        "SELECT operation, result, sum(value)::bigint FROM lake_iceberg.catalog_stats() "
        "WHERE metric = 'requests' GROUP BY operation, result"
    )
    return {(op, res): n for op, res, n in rows}


def test_catalog_stats_count_the_requests(pg, namespace):
    """``lake_iceberg.catalog_stats()`` (pg_lake 3.7) counts the REST catalog requests a
    CREATE TABLE and an INSERT make.

    Checks:
        * ``create_table`` and ``publish_commit`` requests with result ``ok`` each grow by at
          least one;
        * the counters never decrease.

    Args:
        pg: pg_lake adapter, once per warehouse.
        namespace: Fresh namespace of the test session.
    """
    before = _catalog_requests(pg)
    table = f"cs_{uuid.uuid4().hex[:8]}"
    pg.create_owned_table(namespace, table, "id bigint")
    pg.execute(f"INSERT INTO {pg.fq(namespace, table)} VALUES (1)")
    after = _catalog_requests(pg)
    for op in ("create_table", "publish_commit"):
        assert after.get((op, "ok"), 0) > before.get((op, "ok"), 0), (
            f"no new {op} request counted: {before.get((op, 'ok'))} -> {after.get((op, 'ok'))}"
        )
    shrunk = {k: (v, after.get(k, 0)) for k, v in before.items() if after.get(k, 0) < v}
    assert not shrunk, f"counters went down: {shrunk}"
