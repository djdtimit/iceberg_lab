"""Engine-specific: PyIceberg (not part of the comparison), on both warehouses.

* a filter on a day-partitioned table plans only the matching data file (PyIceberg's scan
  planning prunes partitions - no engine-neutral measurement exists for the others);
* known gap: ``upsert`` after ``rename_column`` compares the new column names with the old
  data files.
"""

# Moved to the base matrices: add/rename column, time_travel, delete, overwrite, merge
# (upsert), transaction_multi_change in test_operations. Left here: partition pruning (until a
# engine-neutral measurement exists for all engines) and the upsert-after-rename bug.

from datetime import datetime

import pyarrow as pa
import pytest
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DoubleType, LongType, NestedField, TimestampType

from iceberg_lab.config import WAREHOUSES
from iceberg_lab.engines import pyiceberg_catalog

from conftest import expect_cause


@pytest.fixture(params=WAREHOUSES)
def catalog(request):
    """PyIceberg catalog, once per warehouse (both access modes).

    Args:
        request: pytest request of the calling test or fixture.

    Returns:
        The catalog.
    """
    return pyiceberg_catalog(request.param)


def test_partitioning_prunes_files(catalog, namespace, table_name):
    """Day-partitioned table: a filter on ``ts`` plans only the matching file.

    Checks:
        * without filter the scan plans 3 files;
        * with the ``ts`` filter it plans exactly 1.

    Args:
        catalog: PyIceberg catalog, once per warehouse (both access modes).
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
    """
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "ts", TimestampType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day")
    )
    t = catalog.create_table(f"{namespace}.{table_name}", schema=schema, partition_spec=spec)
    t.append(
        pa.table(
            {
                "id": [1, 2, 3],
                "ts": [datetime(2026, 1, 1, 8), datetime(2026, 1, 2, 9), datetime(2026, 1, 3, 10)],
                "amount": [1.0, 2.0, 3.0],
            },
            schema=schema.as_arrow(),
        )
    )
    all_files = list(t.scan().plan_files())
    pruned = list(t.scan(row_filter=GreaterThanOrEqual("ts", "2026-01-03T00:00:00")).plan_files())
    assert len(all_files) == 3
    assert len(pruned) == 1


def test_upsert_after_rename(catalog, namespace, table_name, sample):
    """Known gap: after ``rename_column``, ``upsert`` compares new names with old files.

    Checks:
        * the upsert of id 2 sets its total to 9.0 (else the documented PyIceberg gap).

    Args:
        catalog: PyIceberg catalog, once per warehouse (both access modes).
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
        sample: Small Arrow table (id, name, amount) that is written and expected back.
    """
    t = catalog.create_table(f"{namespace}.{table_name}", schema=sample.schema)
    t.append(sample)
    with t.update_schema() as u:
        u.rename_column("amount", "total")
    t = t.refresh()
    changes = pa.Table.from_pylist(
        [{"id": 2, "name": "beta", "total": 9.0}], schema=t.schema().as_arrow()
    )
    with expect_cause("pyiceberg-upsert-renamed-columns", r"field names are not matching"):
        t.upsert(changes, join_cols=["id"])
        assert {r["id"]: r["total"] for r in t.scan().to_arrow().to_pylist()}[2] == 9.0
