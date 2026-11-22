"""Cross-engine interoperability: every engine writes, every engine reads.

Runs on the ``sts`` warehouse of the catalog under test, where all engines work with the
access the catalog provides (vended credentials; on RustFS the engines' own keys).
"""

import pytest

from iceberg_lab.config import WAREHOUSE_STS
from iceberg_lab.engines import rows

from conftest import ENGINES, engine_param, fresh


@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_write_then_read(writer, reader, make_engine, namespace, table_name, sample):
    """The writer creates and fills a table, the reader reads exactly the same rows.

    Checks:
        * the reader (cache dropped) returns exactly the rows of the sample the writer wrote.

    Args:
        writer: Engine that writes the table.
        reader: Engine that reads it.
        make_engine: Factory for an engine adapter on a warehouse and credential mode.
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
        sample: Small Arrow table (id, name, amount) that is written and expected back.
    """
    make_engine(writer, WAREHOUSE_STS).write(namespace, table_name, sample)
    assert fresh(make_engine(reader, WAREHOUSE_STS), namespace, table_name).read(
        namespace, table_name
    ) == rows(sample)
