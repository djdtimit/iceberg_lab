"""Engine-specific: DuckDB's three-part schema names (not part of the comparison).

``CREATE SCHEMA <catalog>."parent"."child"`` must create the nested namespace ``parent.child``
or fail. DuckDB up to 2.0.0 silently created a top-level namespace ``child`` instead; fixed in
2.0.1 - the test stays as a regression check.
"""

# Moved to the base matrices: listing -> test_operations list_tables; aggregation ->
# filter_aggregate; inserts/delete/update (read by every engine) -> insert/delete/update +
# cross reads. Left here: DuckDB's three-part schema name (a DuckDB-only behaviour).

import contextlib

import pytest

from iceberg_lab.config import WAREHOUSE_STS

from conftest import require_engine


@pytest.fixture(autouse=True)
def _engine_available():
    """Skip (or fail with REQUIRE_ENGINES) every test here if the engine is not available."""
    require_engine("duckdb")


@pytest.fixture
def duck(make_engine):
    """DuckDB adapter attached to demo-sts with vended credentials.

    Args:
        make_engine: Factory for engine adapters (conftest fixture).

    Returns:
        The adapter.
    """
    return make_engine("duckdb", WAREHOUSE_STS)


def test_three_part_schema_name_creates_the_right_namespace(duck):
    """``CREATE SCHEMA lk."parent"."child"`` must create ``parent.child`` - or fail.

    DuckDB up to 2.0.0 accepted the statement and silently created a TOP-LEVEL namespace
    ``child`` (the documented form for nested namespaces is ``lk."parent.child"``); fixed in 2.0.1.

    Checks:
        * either the statement fails,
        * or no top-level namespace ``child`` was created and ``parent.child`` exists.

    Args:
        duck: DuckDB adapter attached to the ``sts`` warehouse with vended credentials.
    """
    import uuid

    from iceberg_lab.engines import pyiceberg_catalog

    cat = pyiceberg_catalog(WAREHOUSE_STS)
    parent, child = f"dk3p_{uuid.uuid4().hex[:6]}", f"dk3c_{uuid.uuid4().hex[:6]}"
    cat.create_namespace((parent,))
    try:
        try:
            duck.execute(f'CREATE SCHEMA {duck.alias}."{parent}"."{child}"')
        except Exception:  # noqa: BLE001 - refusing the statement is acceptable
            return
        assert (child,) not in cat.list_namespaces(), f"created top-level namespace {child}"
        assert (parent, child) in cat.list_namespaces((parent,))
    finally:
        for ns in ((parent, child), (child,), (parent,)):
            # Catalogs without nested namespaces (Unity) refuse the nested lookup with 400.
            with contextlib.suppress(Exception):
                if cat.namespace_exists(ns):
                    cat.drop_namespace(ns)
