"""Branch isolation (base test, every engine): commits on a branch stay on that branch.

The lab client (PyIceberg) creates a table with two rows on ``main``, creates branch ``audit``
and commits a third row to ``audit`` only. Then

``test_main_ignores_branch_commits``
    every engine reads the table (= ``main``) and must see the two rows, not the branch row
    (readers that take the newest snapshot instead of the one ``main`` points at would show it -
    the class of bug duckdb-iceberg had with branches);
``test_write_to_main_leaves_branch``
    every engine appends one row to the table; ``main`` then has three rows and ``audit`` still
    exactly its own three (an engine that commits on top of the newest snapshot instead of
    ``main``'s would carry the branch row into ``main``).
"""

from __future__ import annotations

import uuid

import pytest
import test_concurrency as cc
import test_operations as base

from iceberg_lab.engines import pyiceberg_catalog

from conftest import ENGINES, engine_param, expect_gap, fresh, require_engine

WAREHOUSE = base.WAREHOUSE
MAIN = [(1, "a", 1.0), (2, "b", 2.0)]
ON_BRANCH = (9, "branch", 9.0)

# Filled from observed results only: (test, engine, regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str]] = []
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


def _branched_table(namespace: str) -> str:
    """Table with MAIN on main and MAIN + ON_BRANCH on branch ``audit``.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        The table name.
    """
    name = f"br_{uuid.uuid4().hex[:6]}"
    cat = pyiceberg_catalog(WAREHOUSE)
    t = cat.create_table(f"{namespace}.{name}", schema=base.SCHEMA)
    t.append(base.arrow(MAIN))
    t.manage_snapshots().create_branch(t.current_snapshot().snapshot_id, "audit").commit()
    t = cat.load_table(f"{namespace}.{name}")
    t.append(base.arrow([ON_BRANCH]), branch="audit")
    return name


def _rows_of(namespace: str, name: str, ref: str) -> list[tuple]:
    """Rows of a ref as the lab client reads them.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
        name: Name of the object.
        ref: Ref name (branch or tag).

    Returns:
        Sorted rows.
    """
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{name}")
    snap = t.metadata.refs[ref].snapshot_id
    return sorted(base.arrow_rows(t.scan(snapshot_id=snap).to_arrow()))


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_main_ignores_branch_commits(engine, namespace, engine_pool):
    """Reading a table shows ``main``, not the newer commit on branch ``audit``.

    Setup (lab client): the table has rows on main and one more commit only on branch audit.

    Checks:
        * the engine's plain read (cache dropped first) returns exactly main's rows - not the
          branch's extra row.

    Args:
        engine: Engine under test (reader).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine(engine)
    name = _branched_table(namespace)
    with _gap("main_ignores_branch_commits", engine):
        got = fresh(engine_pool(engine), namespace, name).read(namespace, name)
        assert sorted(got) == MAIN, f"{engine} read {sorted(got)} - main has {MAIN}"


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_write_to_main_leaves_branch(engine, namespace, engine_pool):
    """An append by the engine lands on ``main`` only; branch ``audit`` keeps its rows.

    Checks (read by the lab client per ref):
        * main has its rows plus the appended row;
        * branch audit has exactly its own rows - unchanged by the append.
        pg_lake: not offered (cannot write tables it did not create).

    Args:
        engine: Engine under test (writer).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine(engine)
    if engine == "pg_lake":
        pytest.skip("pg_lake has no API for writing tables it did not create (read-only attach)")
    name = _branched_table(namespace)
    with _gap("write_to_main_leaves_branch", engine):
        adapter = fresh(engine_pool(engine), namespace, name)
        cc.append_one(engine, adapter, namespace, name, (3, "c", 3.0))
        assert _rows_of(namespace, name, "main") == [*MAIN, (3, "c", 3.0)], "main"
        assert _rows_of(namespace, name, "audit") == sorted([*MAIN, ON_BRANCH]), "branch audit"
