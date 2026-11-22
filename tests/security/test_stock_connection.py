"""Connection without lab help (base, every engine, every catalog).

Every engine connects with its stock configuration - none of the lab's helps - writes a
small table and reads it back. The other tests run with the lab's helps and count as working
when they work with them; this test is where the gap itself is shown, always without help, so
a release that fixes it shows up as "gap fixed". The helps (``iceberg_lab.config.HELPS``,
central rules in conftest):

* PyIceberg, Polars, Daft on RustFS: the SigV4 adapter sends ``x-amz-content-sha256`` only for
  empty bodies, RustFS refuses every signed POST without it;
* DuckDB on RustFS: the SigV4 service is derived from an AWS host name, ``rustfs:9000`` cannot
  be attached.

Engines without a help on the catalog use their normal adapter.
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from iceberg_lab import engines
from iceberg_lab.config import WAREHOUSE_STS, helped_engines

from conftest import ENGINE_CLASSES, ENGINES, engine_param, require_engine

#: Engines the lab helps over a connection gap on this catalog (iceberg_lab.config.HELPS).
HELPED = helped_engines()
ROWS = [(1, "a", 1.0), (2, "b", 2.0)]


@pytest.fixture
def stock_adapter(engine, monkeypatch, engine_pool):
    """The engine's adapter without any lab workaround.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        monkeypatch: pytest monkeypatch fixture.
        engine_pool: Session pool of engine adapters (conftest fixture).

    Yields:
        The adapter.
    """
    if engine not in HELPED:
        yield engine_pool(engine)
        return
    require_engine(engine)
    monkeypatch.setattr(engines, "WITH_HELP", False)  # DuckDB host, pg_lake stub, header
    monkeypatch.setattr(engines, "LAB_FIXES_ENABLED", False)
    adapter = ENGINE_CLASSES[engine](WAREHOUSE_STS)
    yield adapter
    adapter.close()


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_connects_without_lab_help(engine, namespace, stock_adapter):
    """The engine, configured as documented and without lab help, writes and reads a table.

    The proof for the connection gaps the run "RustFS + help" steps over - always without help,
    also in that run.

    Checks:
        * the engine writes a two-row table and reads exactly these rows back (RustFS:
          PyIceberg/Polars/Daft fail at the missing x-amz-content-sha256 header, DuckDB at the
          host name - documented gaps).

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        stock_adapter: The engine's adapter without any lab workaround.
    """
    table = f"stock_{engine}_{uuid.uuid4().hex[:6]}"
    data = pa.table(
        {
            "id": pa.array([r[0] for r in ROWS], pa.int64()),
            "name": [r[1] for r in ROWS],
            "amount": [r[2] for r in ROWS],
        }
    )
    stock_adapter.write(namespace, table, data)
    assert sorted(stock_adapter.read(namespace, table)) == ROWS
