"""Engine inventory: what each engine offers for Iceberg, read from the engine itself.

Counterpart of the catalog inventory (test_feature_inventory.py, advertised endpoints): a new
engine version that adds or removes Iceberg functions, procedures or options fails here until
the change is classified (tests, docs/engine-inventory.md) and the baseline is re-recorded:

    RECORD_INVENTORY=1 pytest tests/selftest/test_engine_inventory.py

Sources (live, per engine):
    pyiceberg  public methods of Catalog (REST), Table, Transaction, InspectTable,
               ManageSnapshots, UpdateSchema, UpdateSpec, MaintenanceTable
    polars     functions and DataFrame/LazyFrame methods with "iceberg" in the name
    daft       functions and DataFrame methods with "iceberg" in the name
    duckdb     duckdb_functions() with "iceberg" in the name
    spark      Iceberg's Spark procedures (SparkProcedures registry, CALL <catalog>.system.*)
    trino      functions in <catalog>.system, procedures (system.jdbc.procedures) and Iceberg
               table properties (system.metadata.table_properties)
    pg_lake    functions in pg_lake's public schemas (lake, lake_iceberg, lake_file, ...)
    flink      procedures of the Iceberg catalog (SHOW PROCEDURES; none so far - the Iceberg
               Flink catalog does not implement them)

Baselines: docs/api/engine-<engine>.txt. Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import os
import pathlib

import pytest

from conftest import ENGINES, engine_param, require_engine

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _public(prefix: str, obj) -> list[str]:
    """Public callables of an object, prefixed.

    Args:
        prefix: Prefix of the names.
        obj: Class or module.

    Returns:
        ``<prefix>.<name>`` entries.
    """
    return [
        f"{prefix}.{n}"
        for n in dir(obj)
        if not n.startswith("_") and callable(getattr(obj, n, None))
    ]


def _iceberg_names(prefix: str, obj) -> list[str]:
    """Attributes with "iceberg" in the name, prefixed.

    Args:
        prefix: Prefix of the names.
        obj: Class or module.

    Returns:
        ``<prefix>.<name>`` entries.
    """
    return [f"{prefix}.{n}" for n in dir(obj) if "iceberg" in n.lower()]


def offered(engine: str, engine_pool) -> list[str]:
    """What the engine offers for Iceberg, read live (see the module docstring).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        The inventory entries.
    """
    if engine == "pyiceberg":
        from pyiceberg.catalog.rest import RestCatalog
        from pyiceberg.table import InspectTable, Table, Transaction
        from pyiceberg.table.update.schema import UpdateSchema
        from pyiceberg.table.update.snapshot import ManageSnapshots
        from pyiceberg.table.update.spec import UpdateSpec

        names = []
        for prefix, cls in (
            ("Catalog", RestCatalog),
            ("Table", Table),
            ("Transaction", Transaction),
            ("InspectTable", InspectTable),
            ("ManageSnapshots", ManageSnapshots),
            ("UpdateSchema", UpdateSchema),
            ("UpdateSpec", UpdateSpec),
        ):
            names += _public(prefix, cls)
        try:
            from pyiceberg.table.maintenance import MaintenanceTable

            names += _public("MaintenanceTable", MaintenanceTable)
        except ImportError:
            pass
        return names
    if engine == "polars":
        import polars as pl

        return (
            _iceberg_names("pl", pl)
            + _iceberg_names("DataFrame", pl.DataFrame)
            + _iceberg_names("LazyFrame", pl.LazyFrame)
        )
    if engine == "daft":
        import daft

        return _iceberg_names("daft", daft) + _iceberg_names("DataFrame", daft.DataFrame)
    adapter = engine_pool(engine)
    if engine == "duckdb":
        rows = adapter.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() "
            "WHERE function_name ILIKE '%iceberg%'"
        )
        return [f"function:{r[0]}" for r in rows]
    if engine == "spark":
        jvm = adapter.spark._jvm
        loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
        field = loader.loadClass(
            "org.apache.iceberg.spark.procedures.SparkProcedures"
        ).getDeclaredField("BUILDERS")
        field.setAccessible(True)
        return [f"procedure:{n}" for n in field.get(None).keySet()]
    if engine == "trino":
        cat = adapter.catalog
        names = [f"function:{r[0]}" for r in adapter.sql(f"SHOW FUNCTIONS FROM {cat}.system")]
        names += [
            f"procedure:{r[0]}.{r[1]}"
            for r in adapter.sql(
                "SELECT procedure_schem, procedure_name FROM system.jdbc.procedures "
                f"WHERE procedure_cat = '{cat}'"
            )
        ]
        names += [
            f"table_property:{r[0]}"
            for r in adapter.sql(
                "SELECT property_name FROM system.metadata.table_properties "
                f"WHERE catalog_name = '{cat}'"
            )
        ]
        return names
    if engine == "pg_lake":
        rows = adapter.execute(
            "SELECT DISTINCT n.nspname || '.' || p.proname FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE (n.nspname ILIKE '%lake%' OR n.nspname ILIKE '%iceberg%') "
            "AND n.nspname NOT LIKE '\\_\\_%'"
        )
        return [f"function:{r[0]}" for r in rows]
    # flink
    catalog = adapter.fq("x", "y").split(".")[0].strip("`")
    try:
        rows = adapter.execute(f"SHOW PROCEDURES FROM `{catalog}`.`system`")
    except Exception as exc:  # noqa: BLE001 - the catalog has no procedures at all
        return [f"procedures: none ({type(exc).__name__})"]
    return [f"procedure:{r[0]}" for r in rows]


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_engine_inventory_is_recorded(engine, engine_pool):
    """The engine offers exactly the recorded Iceberg functions/procedures/options.

    A change is a new version's new (or dropped) feature and must be classified, then the
    baseline re-recorded (``RECORD_INVENTORY=1``).

    Checks:
        * the live inventory (read from the engine itself) is not empty - else the source broke;
        * it equals ``docs/api/engine-<engine>.txt``: nothing new, nothing gone.

    Args:
        engine: Engine whose Iceberg offering is read.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine(engine)
    live = sorted(set(offered(engine, engine_pool)))
    assert live, f"{engine}: nothing found - the inventory source is broken"
    path = ROOT / "docs" / "api" / f"engine-{engine}.txt"
    if os.getenv("RECORD_INVENTORY") == "1":
        path.write_text("\n".join(live) + "\n")
    recorded = [e for e in (path.read_text().split("\n") if path.exists() else []) if e]
    new, gone = sorted(set(live) - set(recorded)), sorted(set(recorded) - set(live))
    assert not new and not gone, (
        f"{engine} changed - classify it (tests, docs/engine-inventory.md), then "
        f"RECORD_INVENTORY=1 pytest tests/selftest/test_engine_inventory.py:\n  new: {new}\n  gone: {gone}"
    )
