"""Engine-specific: Flink with both warehouses in one session (not part of the comparison).

Flink runs in every comparison matrix (types, operations, interop, access modes, v3, RustFS)
as engine ``flink``; upserts with equality deletes, time travel and branches are covered
there (test_operations.py). This file keeps what only matters for Flink: one SQL Gateway
session with a catalog per warehouse (``sts`` and ``signing``) joins a table of each.
"""

# Moved to the base matrices: upsert with equality deletes = test_operations merge (Flink)
# + cross reads; identifier_fields; time_travel and branch_and_tag. Left here: a join across
# two warehouses (until every catalog has a second warehouse).

import pytest

from iceberg_lab.config import WAREHOUSE_STS
from iceberg_lab.engines import FlinkEngine

from conftest import require_engine, require_remote_signing, warehouse_params

pytestmark = pytest.mark.slow


@pytest.fixture(params=warehouse_params())
def ctx(request, namespace, table_name):
    """``(flink, warehouse, fully qualified table name)``, once per warehouse.

    Args:
        request: pytest request of the calling test or fixture.
        namespace: Namespace of the table (the session namespace for fixtures).
        table_name: Unique table name for the test.

    Yields:
        ``(flink, warehouse, fq)``.
    """
    require_engine("flink")
    flink = FlinkEngine(request.param)
    flink.execute(f"CREATE DATABASE IF NOT EXISTS {flink.catalog}.{namespace}")
    yield flink, request.param, flink.fq(namespace, table_name)
    flink.close()


def test_join_across_warehouses(namespace, table_name):
    """One Flink session joins a table on the sts warehouse with one on the signing warehouse.

    Checks:
        * the join over both catalogs returns the single expected row ``(sts, signing)``.

    Args:
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
    """
    require_engine("flink")
    require_remote_signing()
    flink = FlinkEngine(WAREHOUSE_STS)
    try:
        for cat in ("sts", "signing"):
            flink.execute(f"CREATE DATABASE IF NOT EXISTS {cat}.{namespace}")
        a, b = f"sts.{namespace}.{table_name}_a", f"signing.{namespace}.{table_name}_b"
        flink.execute(f"CREATE TABLE {a} (id INT, x STRING)")
        flink.execute(f"CREATE TABLE {b} (id INT, y STRING)")
        flink.execute(f"INSERT INTO {a} VALUES (1, 'sts')")
        flink.execute(f"INSERT INTO {b} VALUES (1, 'signing')")
        rows = flink.execute(f"SELECT a.x, b.y FROM {a} a JOIN {b} b ON a.id = b.id")
        assert rows == [["sts", "signing"]]
    finally:
        flink.close()
