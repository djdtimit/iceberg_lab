"""Engine-specific: Spark (not part of the comparison), on both warehouses (``sts``, ``signing``).

Spark runs in every comparison matrix as engine ``spark``; this file keeps ``DROP TABLE ...
PURGE`` through Spark (on Lakekeeper an intermittent conflict with its own hard delete).
"""

# Moved to the base matrices (same checks for every engine, docs/test-architecture.md):
# MERGE -> test_operations merge (incl. ONE commit); partition transforms -> test_evolution;
# time travel / schema evolution -> test_operations time_travel, add_column; merge-on-read
# DELETE read by other engines -> test_operations delete_merge_on_read + cross reads.
# Left here: Spark's own PURGE behaviour on Lakekeeper.

import contextlib

import pytest

from iceberg_lab.config import PROFILE, WAREHOUSE_SIGNING
from iceberg_lab.engines import SPARK_CATALOGS

from conftest import expect_cause, warehouse_params

pytestmark = pytest.mark.slow


@pytest.fixture(params=warehouse_params())
def ctx(request, spark, namespace, table_name):
    """``(spark, warehouse, fully qualified table name)``, once per warehouse.

    Args:
        request: pytest request of the calling test or fixture.
        spark: The session's SparkSession.
        namespace: Namespace of the table (the session namespace for fixtures).
        table_name: Unique table name for the test.

    Returns:
        ``(spark, warehouse, fq)``.
    """
    warehouse = request.param
    fq = f"{SPARK_CATALOGS[warehouse]}.{namespace}.{table_name}"
    return spark, warehouse, fq


@pytest.mark.parametrize("warehouse", warehouse_params())
def test_drop_table_purge(spark, namespace, table_name, warehouse):
    """Spark's ``DROP TABLE ... PURGE`` on the catalog.

    Checks:
        * the drop with purge succeeds and the table is gone (on Lakekeeper a known,
          intermittent conflict with its own hard delete).

    Args:
        spark: The session's SparkSession.
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
        warehouse: ``sts`` or ``signing`` warehouse.
    """
    fq = f"{SPARK_CATALOGS[warehouse]}.{namespace}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {fq} VALUES (1)")
    if warehouse == WAREHOUSE_SIGNING:
        gap = expect_cause(
            "lakekeeper-signer-refuses-purge",
            r"Table does not exist or user does not have permission to view it at location",
        )
    elif PROFILE.name == "lakekeeper":
        # Lakekeeper deletes the files itself; Spark's PURGE can then miss one.
        gap = expect_cause("lakekeeper-hard-delete-race", r"NoSuchKeyException")
    else:
        gap = contextlib.nullcontext()
    with gap:
        spark.sql(f"DROP TABLE {fq} PURGE")
