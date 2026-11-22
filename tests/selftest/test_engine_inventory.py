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

Baselines: docs/api/engine-<engine>.txt. Re-recording alone does not make a new name pass:
every entry must be used by a test (its name occurs in a test or the adapters) or be listed in
docs/api/engine-classified.toml as "ignored: <reason>" or "untested: <what>" (offline check
below) - so a new feature is either tested or consciously put on the open list.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import os
import pathlib

import pytest

from iceberg_lab.lab.inventory import CLASSIFIED_FILE, classified, offered, unclassified

from conftest import ENGINES, engine_param, require_engine

ROOT = pathlib.Path(__file__).resolve().parents[2]


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
        f"{engine} changed - test the new names or list them in docs/api/engine-classified.toml, then "
        f"RECORD_INVENTORY=1 pytest tests/selftest/test_engine_inventory.py:\n  new: {new}\n  gone: {gone}"
    )


@pytest.mark.offline
@pytest.mark.parametrize("engine", ENGINES)
def test_every_inventory_entry_is_used_or_classified(engine):
    """Every recorded API name of an engine is used by a test or classified.

    Checks:
        * each name of ``docs/api/engine-<engine>.txt`` occurs in a test / the adapters, or is
          listed in ``docs/api/engine-classified.toml``;
        * listed values are "ignored: <reason>" or "untested: <what>";
        * no listed name is stale: gone from the inventory, or "untested" although a test now
          uses it (then remove the entry).

    Args:
        engine: Engine whose recorded inventory is checked.
    """
    path = ROOT / "docs" / "api" / f"engine-{engine}.txt"
    names = [n for n in path.read_text().split("\n") if n] if path.exists() else []
    problems = {k: v for k, v in unclassified(engine, names).items() if v}
    assert not problems, (
        f"{engine}: {problems} - test the names or classify them in "
        f"{CLASSIFIED_FILE.relative_to(ROOT)}"
    )


@pytest.mark.offline
def test_classification_names_only_lab_engines():
    """The classification file has one section per lab engine at most.

    Checks:
        * every section of ``docs/api/engine-classified.toml`` is a lab engine.
    """
    assert set(classified()) <= set(ENGINES)
