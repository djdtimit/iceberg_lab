"""Multi-table transactions: one logical change to two tables - all or nothing?

The Iceberg REST spec offers ``POST /v1/{prefix}/transactions/commit``: several table commits
applied atomically (Lakekeeper and Polaris offer it; on a catalog without it a multi-table
transaction cannot be atomic). Whether an engine uses it decides what happens when the commit
of ONE table fails. Runs on every catalog.

Each engine writes one row into table A and one into table B with its own multi-table
construct, through the fault proxy of ``test_robustness.py`` (so every commit request is
seen):

* DuckDB and pg_lake: ``BEGIN; INSERT INTO a ...; INSERT INTO b ...; COMMIT``
* Flink: ``EXECUTE STATEMENT SET BEGIN INSERT ...; INSERT ...; END``
* Trino: an explicit (non-autocommit) transaction
* Spark, PyIceberg, Polars, Daft: no multi-table transaction API (checked) -> "no API"

``test_multi_table_commit``     both rows arrive; the proxy log shows whether ONE
                                ``/transactions/commit`` request was used (atomic) or one
                                commit per table
``test_multi_table_atomicity``  the proxy refuses (503) every commit that touches table B,
                                also inside ``/transactions/commit``. Correct: the whole
                                transaction fails and A is unchanged. Wrong: A committed,
                                B not (a partial transaction became visible).
"""

from __future__ import annotations

import fnmatch
import re
import uuid

import pytest
import test_robustness as rb

from iceberg_lab.config import PROFILE
from iceberg_lab.engines import pyiceberg_catalog

from conftest import ENGINES, engine_param, expect_gap, require

WAREHOUSE = rb.WAREHOUSE
#: The injected refusal: HTTP 503 "ServiceUnavailable: injected" (or the engine's wrapper).
COMMIT_REFUSED = r"(?i)(503|ServiceUnavailable|injected|unavailable)"


@pytest.fixture(scope="module")
def proxy():
    """Recording proxy in front of the catalog.

    Yields:
        The proxy.
    """
    p = rb.Proxy()
    yield p
    p.close()


def _tables(engine, adapter, ns):
    """Create the two tables of the test.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.

    Returns:
        ``(a, b)``.
    """
    names = (f"tx_{engine}_{uuid.uuid4().hex[:6]}_a", f"tx_{engine}_{uuid.uuid4().hex[:6]}_b")
    for n in names:
        if engine == "pg_lake":
            adapter.execute(f'CREATE SCHEMA IF NOT EXISTS "{ns}"')
            adapter.execute(
                f"CREATE TABLE {adapter.fq(ns, n)} (id integer, name text, amount double precision) "
                f"USING iceberg WITH (catalog = '{adapter.server}')"
            )
            adapter.owned.add((ns, n))
        else:
            pyiceberg_catalog(WAREHOUSE).create_table(f"{ns}.{n}", schema=rb.base.SCHEMA)
    return names


def multi_write(engine, adapter, ns, a, b):
    """One row into ``a`` and one into ``b`` as ONE transaction in ``engine``'s own way.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.
        a: First table name.
        b: Second table name.
    """
    ins_a = f"INSERT INTO {adapter.fq(ns, a)} VALUES (1, 'a', 1.0E0)"
    ins_b = f"INSERT INTO {adapter.fq(ns, b)} VALUES (2, 'b', 2.0E0)"
    if engine == "duckdb":
        adapter.execute("BEGIN TRANSACTION")
        try:
            adapter.execute(ins_a)
            adapter.execute(ins_b)
            adapter.execute("COMMIT")
        except Exception:
            try:
                adapter.execute("ROLLBACK")
            except Exception:  # noqa: BLE001, S110 - already aborted
                pass
            raise
    elif engine == "pg_lake":
        with adapter.conn.transaction():
            adapter.execute(ins_a)
            adapter.execute(ins_b)
    elif engine == "flink":
        adapter.execute(f"EXECUTE STATEMENT SET BEGIN {ins_a}; {ins_b}; END")
    elif engine == "trino":
        from trino.transaction import IsolationLevel

        import trino

        conn = trino.dbapi.connect(
            host=rb.SETTINGS.trino_host,
            port=rb.SETTINGS.trino_port,
            user="tests",
            catalog=f"{rb.PROFILE.trino_prefix}_proxy",
            isolation_level=IsolationLevel.READ_COMMITTED,
        )
        cur = conn.cursor()
        cur.execute(ins_a)
        cur.fetchall()
        cur.execute(ins_b)
        cur.fetchall()
        conn.commit()
    else:
        raise ValueError(engine)


def count(ns, table) -> int:
    """Row count as the lab client reads it.

    Args:
        ns: Namespace.
        table: Table name.

    Returns:
        The count.
    """
    return pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}").scan().to_arrow().num_rows


# Filled from observed failures only: (engine, test, regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str]] = [
    # Catalogs without POST /v1/{prefix}/transactions/commit (Unity): DuckDB refuses correctly.
    *([("duckdb", "*", r"cannot commit this transaction atomically because it would require multiple table commit requests without atomic multi-table commit support", f"{PROFILE.name} has no /transactions/commit - no multi-table transaction (DuckDB refuses the COMMIT)")]
      if PROFILE.name == "unity" else []),
    ("trino", "*", r"Catalog only supports writes using autocommit", "Trino Iceberg: no writes inside an explicit transaction (autocommit only), so no multi-table transaction"),
    ("flink", "commit", r"one commit per table instead of one transaction", "Flink STATEMENT SET commits every table separately (no /transactions/commit)"),
    ("flink", "atomicity", r"NOT ATOMIC: A committed, B not", "INCONSISTENT: Flink STATEMENT SET is not atomic across tables: A stays committed when B's commit fails"),
    # Checked by hand: pg_lake shows both rows, the REST catalog (and every other engine)
    # none; no retry, nothing in the Postgres log.
    ("pg_lake", "atomicity", r"no error although B's commit was refused", "DATA LOSS: pg_lake reports COMMIT success although the REST catalog refused the commit: Postgres shows the rows, the catalog and all other engines do not (silent divergence, nothing logged)"),
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
        if fnmatch.fnmatch(engine, e) and fnmatch.fnmatch(test, t):
            return expect_gap(reason, regex)
    return expect_gap(None, None)


def _adapter(engine, request):
    """The engine's adapter through the proxy (skip engines without multi-table transactions).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        request: pytest request of the calling test or fixture.

    Returns:
        The adapter.
    """
    require(engine, "multi-table transactions", request.getfixturevalue("engine_pool"))
    return rb.proxied_engine(engine, request)


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_multi_table_commit(engine, namespace, proxy, request):
    """Both rows arrive, committed with ONE ``/transactions/commit`` request.

    Checks:
        * each of the two tables has its row;
        * the proxy saw a multi-table commit, not one commit per table.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        proxy: Recording HTTP proxy in front of the catalog; it can refuse one table's commit.
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    adapter = _adapter(engine, request)
    try:
        with _gap(engine, "commit"):
            a, b = _tables(engine, adapter, namespace)
            proxy.arm(None)
            multi_write(engine, adapter, namespace, a, b)
            paths = list(proxy.commit_paths)
            assert (count(namespace, a), count(namespace, b)) == (1, 1)
            multi = [p for p in paths if p.endswith("/transactions/commit")]
            assert multi, f"one commit per table instead of one transaction: {paths}"
    finally:
        adapter.close()


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_multi_table_atomicity(engine, namespace, proxy, request):
    """If table B's commit is refused, table A stays unchanged.

    Checks:
        * the engine reports an error that is the refused commit;
        * B has no rows;
        * A has no rows either - else NOT ATOMIC.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        proxy: Recording HTTP proxy in front of the catalog; it can refuse one table's commit.
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    adapter = _adapter(engine, request)
    try:
        with _gap(engine, "atomicity"):
            a, b = _tables(engine, adapter, namespace)
            proxy.arm("fail_table", target=b)
            error = None
            try:
                multi_write(engine, adapter, namespace, a, b)
            except Exception as exc:  # noqa: BLE001 - the outcome is checked below
                error = exc
            paths = list(proxy.commit_paths)
            proxy.arm(None)
            na, nb = count(namespace, a), count(namespace, b)
            assert error is not None, f"no error although B's commit was refused ({paths})"
            assert re.search(COMMIT_REFUSED, f"{type(error).__name__}: {error}"), (
                f"failed for another reason: {type(error).__name__}: {error}"
            )
            assert nb == 0, f"B has {nb} rows although its commit was refused"
            assert na == 0, f"NOT ATOMIC: A committed, B not (commits: {paths})"
    finally:
        proxy.arm(None)
        adapter.close()
