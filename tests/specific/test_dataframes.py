"""Engine-specific: Polars' lazy API on an Iceberg table (not part of the comparison).

A 100-row table written by PyIceberg on the ``sts`` warehouse; ``pl.scan_iceberg`` with a
filter and ``group_by`` must return the right groups - the filter is pushed into the Iceberg
scan instead of being lost.
"""

# Moved to the base matrices: overwrite and filter_aggregate in test_operations.
# Left here: Polars' lazy API with filter pushdown.

import pyarrow as pa
import pytest

from iceberg_lab.config import WAREHOUSE_STS
from iceberg_lab.engines import PyIcebergEngine, polars_storage_options, pyiceberg_catalog

from conftest import require_engine


@pytest.fixture
def table(namespace, table_name):
    """100-row table (5 distinct names) in demo-sts, written by PyIceberg, returned loaded.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
        table_name: Unique table name for the test.

    Returns:
        The loaded PyIceberg table.
    """
    data = pa.table(
        {
            "id": list(range(100)),
            "name": [f"n{i % 5}" for i in range(100)],
            "amount": [float(i) for i in range(100)],
        }
    )
    PyIcebergEngine(WAREHOUSE_STS).write(namespace, table_name, data)
    return pyiceberg_catalog(WAREHOUSE_STS).load_table(f"{namespace}.{table_name}")


def test_polars_lazy_groupby(table):
    """Polars' lazy scan with filter + group_by on an Iceberg table.

    Checks:
        * the lazy query (filter, group by name, count) returns 5 groups of 10 rows each - the
          filter is applied in the Iceberg scan, not lost.

    Args:
        table: Loaded 100-row table (ids 0..99, 5 distinct names) written by PyIceberg.
    """
    require_engine("polars")
    import polars as pl

    out = (
        pl.scan_iceberg(table, storage_options=polars_storage_options(table.io.properties))
        .filter(pl.col("amount") >= 50)
        .group_by("name")
        .agg(pl.len().alias("n"))
        .sort("name")
        .collect()
    )
    assert out["n"].to_list() == [10] * 5
