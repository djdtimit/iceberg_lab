"""What each engine offers for Iceberg, read from the engine itself.

Sources (live, per engine): PyIceberg public methods of Catalog (REST), Table, Transaction,
InspectTable, ManageSnapshots, UpdateSchema, UpdateSpec, MaintenanceTable; Polars and Daft
functions and DataFrame/LazyFrame methods with "iceberg" in the name; DuckDB
``duckdb_functions()``; Spark's Iceberg procedures; Trino functions, procedures and table
properties; pg_lake functions in its schemas; Flink procedures of the Iceberg catalog.

Used by the engine-inventory self-test (baselines in docs/api/engine-<engine>.txt) and by
``capabilities.not_offered``.
"""

from __future__ import annotations

import re


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
    if engine == "duckdb":
        import duckdb

        # A connection without an attached catalog: duckdb_functions() also lists the attached
        # catalogs' schemas, so the inventory would depend on (and fail on) their contents.
        con = duckdb.connect()
        try:
            con.execute("LOAD iceberg")
            rows = con.execute(
                "SELECT DISTINCT function_name FROM duckdb_functions() "
                "WHERE function_name ILIKE '%iceberg%'"
            ).fetchall()
        finally:
            con.close()
        return [f"function:{r[0]}" for r in rows]
    adapter = engine_pool(engine)
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


# =========================================================================== classification

_ROOT = __import__("pathlib").Path(__file__).resolve().parents[3]
#: Names no test uses, each with "ignored: <reason>" or "untested: <what>".
CLASSIFIED_FILE = _ROOT / "docs" / "api" / "engine-classified.toml"
_SOURCES: dict[str, str] = {}


def short_name(name: str) -> str:
    """The identifier a test would use for an inventory entry.

    ``function:lake_iceberg.vacuum`` -> ``vacuum``, ``Table.add_files`` -> ``add_files``,
    ``procedure:add_files`` -> ``add_files``.

    Args:
        name: Entry of ``docs/api/engine-<engine>.txt``.

    Returns:
        The last identifier of the name.
    """
    if re.match(r"^\w+:", name) and " " not in name.split(":", 1)[0]:
        name = name.split(":", 1)[1]
    return name.rsplit(".", 1)[-1]


def used_in(name: str) -> list[str]:
    """Files that use an inventory entry: its short name occurs as a whole identifier in a test
    (outside the self-tests) or in the engine adapters.

    Args:
        name: Entry of ``docs/api/engine-<engine>.txt``.

    Returns:
        Repo-relative paths, sorted (empty: no test uses it).
    """
    if not _SOURCES:
        files = [p for p in (_ROOT / "tests").rglob("*.py") if "selftest" not in p.parts]
        files.append(_ROOT / "src" / "iceberg_lab" / "engines.py")
        _SOURCES.update({p.relative_to(_ROOT).as_posix(): p.read_text() for p in files})
    word = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(short_name(name))}(?![A-Za-z0-9_])")
    return sorted(p for p, text in _SOURCES.items() if word.search(text))


def classified() -> dict[str, dict[str, str]]:
    """The classification of the names no test uses (:data:`CLASSIFIED_FILE`).

    Returns:
        ``engine -> {name: "ignored: ..." | "untested: ..."}``.
    """
    import tomllib

    from iceberg_lab.lab.models import Classification

    if not CLASSIFIED_FILE.exists():
        return {}
    return Classification.validate_python(tomllib.loads(CLASSIFIED_FILE.read_text()))


def unclassified(engine: str, names: list[str]) -> dict[str, list[str]]:
    """Problems of an engine's inventory against the classification.

    Args:
        engine: Engine name.
        names: The engine's inventory entries.

    Returns:
        ``{"unclassified": [...], "stale": [...], "bad": [...]}``: names neither used nor
        listed; listed as untested although a test now uses them, or listed but no longer in the
        inventory; entries whose value is neither "ignored: <reason>" nor "untested: <what>".
    """
    listed = classified().get(engine, {})
    out: dict[str, list[str]] = {"unclassified": [], "stale": [], "bad": []}
    for n in names:
        if n not in listed and not used_in(n):
            out["unclassified"].append(n)
    for n, v in listed.items():
        if not re.match(r"^(ignored|untested): \S", v):
            out["bad"].append(n)
        if n not in names or (v.startswith("untested") and used_in(n)):
            out["stale"].append(n)
    return out
