"""Write-Audit-Publish (WAP) per engine: write to a branch, audit it, then publish it to main.

Steps, identical for every engine (v2 and v3):

1. the engine seeds the table itself (as in the operation matrix);
2. **write**: the engine writes a new row into branch ``audit`` (creating it its own way);
3. **audit**: ``main`` still holds only the seed, ``audit`` holds the seed + the new row
   (checked via PyIceberg on the refs);
4. **publish**: the engine moves ``main`` to the audited state; ``main`` then has the row.

Engine ways: Spark ``ALTER TABLE ... CREATE BRANCH`` + ``spark.wap.branch`` + ``CALL
fast_forward``; PyIceberg ``manage_snapshots().create_branch`` + ``append(branch=...)`` +
``set_current_snapshot``; Flink ``OPTIONS('branch'=...)`` (no SQL to publish); Trino
``CREATE BRANCH``. No API (checked): DuckDB and pg_lake (no branches), Polars and Daft
(``write_iceberg`` has no branch parameter).
"""

from __future__ import annotations

import fnmatch
import uuid

import pytest
import test_operations as base

from iceberg_lab.engines import SPARK_CATALOGS, pyiceberg_catalog

from conftest import ENGINES, engine_param, expect_gap

WAREHOUSE = base.WAREHOUSE
ROW = (4, "d", 4.0)
BRANCH = "audit"
WAP_ENGINES = ("pyiceberg", "spark", "flink", "trino")


def write_branch(engine, adapter, ctx):
    """Step 2: create branch ``audit`` and write ROW into it, the engine's own way.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ctx: Context of the test table (``test_operations.Ctx``).
    """
    fq = adapter.fq(ctx.ns, ctx.table) if adapter is not None else None
    if engine == "pyiceberg":
        t = ctx.pi_table()
        t.manage_snapshots().create_branch(t.current_snapshot().snapshot_id, BRANCH).commit()
        t.append(base.arrow([ROW]), branch=BRANCH)
    elif engine == "spark":
        adapter.execute(f"ALTER TABLE {fq} SET TBLPROPERTIES ('write.wap.enabled' = 'true')")
        adapter.execute(f"ALTER TABLE {fq} CREATE BRANCH {BRANCH}")
        spark = adapter.spark
        spark.conf.set("spark.wap.branch", BRANCH)
        try:
            adapter.execute(f"INSERT INTO {fq} VALUES (4, 'd', 4.0D)")
        finally:
            spark.conf.unset("spark.wap.branch")
    elif engine == "flink":
        adapter.execute(
            f"INSERT INTO {fq} /*+ OPTIONS('branch'='{BRANCH}') */ VALUES (4, 'd', 4.0E0)"
        )
    elif engine == "trino":
        adapter.execute(f"ALTER TABLE {fq} CREATE BRANCH {BRANCH}")
        adapter.execute(f"INSERT INTO \"{ctx.table}@{BRANCH}\" VALUES (4, 'd', 4.0E0)")


def publish(engine, adapter, ctx):
    """Step 4: make main point to the audited branch state.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ctx: Context of the test table (``test_operations.Ctx``).
    """
    if engine == "pyiceberg":
        t = ctx.pi_table()
        t.manage_snapshots().set_current_snapshot(t.refs()[BRANCH].snapshot_id).commit()
    elif engine == "spark":
        adapter.execute(
            f"CALL {SPARK_CATALOGS[WAREHOUSE]}.system.fast_forward('{ctx.ident}', 'main', '{BRANCH}')"
        )
    else:
        raise NotImplementedError(f"{engine} SQL has no statement to publish a branch")


def _rows_at(ctx, snapshot_id):
    """Rows of a snapshot as the lab client reads them.

    Args:
        ctx: Context of the test table (``test_operations.Ctx``).
        snapshot_id: Snapshot id.

    Returns:
        Sorted rows.
    """
    t = ctx.pi_table()
    return base.norm(base.arrow_rows(t.scan(snapshot_id=snapshot_id).to_arrow()))


# Filled from observed failures only: (engine, fv, regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str]] = [
    *((e, "3", base.NO_V3_MSG, base.PYICEBERG_NO_V3) for e in ("pyiceberg",)),
    ("trino", "*", r"mismatched input 'CREATE'", "Trino 484 has no SQL to create a branch, so it cannot write-audit-publish"),
    ("flink", "*", r"flink SQL has no statement to publish a branch", "Flink writes and reads branches (OPTIONS('branch')) but has no SQL to publish one to main"),
]
# fmt: on


def _gap(engine, fv):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The context manager.
    """
    for e, f, regex, reason in KNOWN_GAPS:
        if fnmatch.fnmatch(engine, e) and fnmatch.fnmatch(str(fv), f):
            return expect_gap(reason, regex)
    return expect_gap(None, None)


@pytest.mark.slow
@pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_write_audit_publish(engine, fv, namespace, engine_pool):
    """Write-audit-publish: the engine writes to a branch, the branch is audited, then published.

    Checks:
        * the branch exists and holds seed + the new row, while main still holds only the seed;
        * after publishing, main holds seed + the new row.
        Engines without branch writes: not offered.

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    if engine not in WAP_ENGINES:
        pytest.skip(f"{engine} has no API for writing to branches")
    cat = pyiceberg_catalog(WAREHOUSE)
    ctx = base.Ctx(
        cat, namespace, f"wap_{engine}_v{fv}_{uuid.uuid4().hex[:6]}", fv, seed_writer=engine
    )
    with _gap(engine, fv):
        adapter = None if engine == "pyiceberg" else engine_pool(engine)
        cat.create_table(ctx.ident, schema=base.SCHEMA, properties={"format-version": str(fv)})
        base.append_rows(ctx, engine_pool, base.SEED)
        write_branch(engine, adapter, ctx)
        t = ctx.pi_table()
        assert BRANCH in t.refs(), f"no branch {BRANCH}: {list(t.refs())}"
        assert _rows_at(ctx, t.refs()["main"].snapshot_id) == base.SEED, (
            "main changed before publish"
        )
        assert _rows_at(ctx, t.refs()[BRANCH].snapshot_id) == base.SEED + [ROW]
        publish(engine, adapter, ctx)
        t = ctx.pi_table()
        assert _rows_at(ctx, t.refs()["main"].snapshot_id) == base.SEED + [ROW], "not published"
