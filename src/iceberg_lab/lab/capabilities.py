"""What an engine offers, and the check behind every "not offered".

``not_offered`` skips a test as "<engine> has no API for <capability>" only after the engine's
live API (``inventory.offered`` plus the parameters of the DataFrame engines' Iceberg functions)
holds no name matching the capability's pattern (:data:`CAPABILITY_API`). A match fails the test:
the engine may have gained the capability. Matches that do not provide it are listed with their
reason in :data:`NOT_OFFERED_EXEMPT`.
"""

from __future__ import annotations

import re

import pytest

_UPDATE = r"(?<![a-z_])update(?![a-z_])"
_SCHEMA_CHANGE = r"update_schema|add_column|drop_column|rename_column|alter_table"
_TRANSACTION = r"(?<![a-z_])transaction(?![a-z_])|begin_transaction"
#: Per capability: case-insensitive regex for API names (functions, procedures, methods,
#: parameters - see _api_inventory) that would provide it. ``not_offered`` checks an engine's
#: live API against it before it skips as "not offered". It covers APIs, not SQL grammar: a new
#: SQL statement (e.g. UPDATE in Flink) is caught by the version pin of test_na_claims instead.
CAPABILITY_API: dict[str, str] = {
    # test_operations
    "create_table": r"create_table",
    "create_existing_fails": r"create_table",
    "ctas": r"create_table_as|ctas|as_select",
    "update": _UPDATE,
    "delete": r"(?<![a-z_])delete(?![a-z_])|delete_where",
    "delete_merge_on_read": r"(?<![a-z_])delete(?![a-z_])|delete_where",
    "merge": r"merge_into|upsert|(?<![a-z_])mode:[^,]*'merge'",
    "add_column": _SCHEMA_CHANGE + r"|schema_mode",
    "drop_column": _SCHEMA_CHANGE,
    "rename_column": _SCHEMA_CHANGE,
    "widen_type": _SCHEMA_CHANGE,
    "drop_table": r"drop_table|purge_table",
    "rename_table": r"rename_table",
    "partition_evolution": r"update_spec|partition_evolution|add_partition_field",
    "identifier_fields": r"identifier_field|primary_key",
    "metadata_tables": r"inspect|metadata_table",
    "time_travel": r"as_of|time_travel|at_snapshot|snapshot_id",
    "rollback": r"rollback|set_current_snapshot|cherry_?pick",
    "branch_and_tag": r"branch|create_tag|(?<![a-z_])tags?(?![a-z_])|manage_snapshots",
    "list_tables": r"list_tables|show_tables",
    "insert": r"append|insert",
    "overwrite": r"overwrite",
    "sees_external_commit": r"refresh",
    "filter_aggregate": r"filter|aggregate|where",
    "transaction_multi_change": _TRANSACTION,
    "transaction_rollback": _TRANSACTION,
    # test_maintenance
    "compact_data_files": r"rewrite_data_files|compact|optimize|bin_?pack",
    "compact_delete_files": r"rewrite_position_delete|rewrite_data_files|compact",
    "expire_snapshots": r"expire",
    "remove_orphan_files": r"orphan",
    "rewrite_manifests": r"rewrite_manifest",
    # other matrices
    "namespaces": r"namespace",
    "namespace properties": r"namespace_propert|schema_propert|update_namespace",
    "multi-table transactions": r"commit_transactions|multi_table",
    "SQL DELETE + UPDATE": _UPDATE,
    "creating views": r"create_view|register_view",
    "querying views": r"load_view|query_view|read_view",
    "computing table statistics": r"compute_table_stats|analyze|compute_stat|update_statistics",
    "computing partition statistics": r"partition_stat|compute_partition",
    "writing to branches": r"branch",
    "incremental reads": (
        r"incremental|change|cdc|append_scan|snapshot_range"
        r"|(?<![a-z_])(start|end|from|to)_snapshot"  # not rollback_to_snapshot
    ),
    "changelog reads": r"change|cdc",
    "streaming writes": r"stream|sink_iceberg|continuous|micro_?batch|subscribe",
    "streaming reads": r"stream|continuous|subscribe|monitor|micro_?batch",
    "upsert/MERGE": r"upsert|merge_into|(?<![a-z_])mode:[^,]*merge",
    "a session time zone": r"time_?zone|session",
    "vended credentials": r"vend|credential|delegation",
    "configuring remote signing": r"remote_sign|signer|signing",
}

#: API names that match a "not offered" probe but do not provide the capability - checked by
#: hand, with the reason. ``(engine, capability) -> {name: reason}``.
NOT_OFFERED_EXEMPT: dict[tuple[str, str], dict[str, str]] = {
    ("duckdb", "computing partition statistics"): {
        "function:iceberg_partition_stats": (
            "a table function that READS partition-level stats from the manifests; it writes no "
            "partition statistics file (checked with DuckDB 2.0.1)"
        ),
    },
    ("pyiceberg", "querying views"): {
        "Catalog.load_view": "returns the view's metadata and SQL text; PyIceberg has no SQL engine to run it",
    },
    ("pyiceberg", "computing table statistics"): {
        n: "registers an existing Puffin file; PyIceberg computes no statistics"
        for n in ("Table.update_statistics", "Transaction.update_statistics")
    },
    ("daft", "branch_and_tag"): {
        n: "read_iceberg reads a branch or tag; Daft cannot create one"
        for n in ("param:read_iceberg.branch:str | None", "param:read_iceberg.tag:str | None")
    },
    ("daft", "writing to branches"): {
        "param:read_iceberg.branch:str | None": "read_iceberg reads a branch; writing goes to main only",
    },
    ("polars", "namespaces"): {
        n: "sink_iceberg only creates a missing namespace while writing; no create/list/drop/property API"
        for n in (
            "param:LazyFrame.sink_iceberg.create_namespace_if_not_exists:bool",
            "param:DataFrame.write_iceberg.create_namespace_if_not_exists:bool",
        )
    },
}
_INVENTORY: dict[str, list[str]] = {}


def _api_inventory(engine: str, engine_pool) -> list[str]:
    """The engine's live Iceberg API (tests/selftest/test_engine_inventory.py ``offered``), plus
    the parameters of the DataFrame engines' Iceberg functions (a new mode or snapshot range
    shows up there, not as a new function). Cached per session.

    Args:
        engine: Engine name.
        engine_pool: Session pool of engine adapters.

    Returns:
        The names.
    """
    if engine not in _INVENTORY:
        import inspect

        from iceberg_lab.lab.inventory import offered

        names = list(offered(engine, engine_pool))
        if engine in ("polars", "daft"):
            mod = __import__(engine)
            fns = [getattr(mod, n) for n in dir(mod) if "iceberg" in n]
            for cls in ("DataFrame", "LazyFrame"):
                c = getattr(mod, cls, None)
                fns += [getattr(c, n) for n in dir(c or object) if "iceberg" in n]
            for fn in fns:
                try:
                    sig = inspect.signature(fn)
                except (TypeError, ValueError):
                    continue
                names += [
                    f"param:{fn.__qualname__}.{p}"
                    + ("" if v.annotation is inspect.Parameter.empty else f":{v.annotation}")
                    for p, v in sig.parameters.items()
                    if p != "self"
                ]
        _INVENTORY[engine] = names
    return _INVENTORY[engine]


def not_offered(engine: str, capability: str, pattern: str | None, engine_pool) -> None:
    """Skip as "<engine> has no API for <capability>" - but only after checking the claim.

    The engine's live API inventory must hold no name matching ``pattern`` (except the
    documented :data:`NOT_OFFERED_EXEMPT`). A match fails the test as a new failure: the engine
    may have gained the capability, so it has to be tested instead of skipped. That way a new
    engine version cannot hide behind an old skip.

    Args:
        engine: Engine name.
        capability: What the engine is said to lack (also the skip reason).
        pattern: Case-insensitive regex for API names that would provide it (None: the entry
            of :data:`CAPABILITY_API`).
        engine_pool: Session pool of engine adapters.
    """
    pattern = pattern or CAPABILITY_API[capability]
    exempt = NOT_OFFERED_EXEMPT.get((engine, capability), {})
    hits = [
        n for n in _api_inventory(engine, engine_pool)
        if re.search(pattern, n, re.IGNORECASE) and n not in exempt
    ]  # fmt: skip
    if hits:
        pytest.fail(
            f"[claim outdated?] {engine} now has {hits[:5]} - test '{capability}' with it, or "
            f"add the name to iceberg_lab.lab.capabilities.NOT_OFFERED_EXEMPT with the reason it does not provide it"
        )
    pytest.skip(f"{engine} has no API for {capability} (checked: no /{pattern}/ in its API)")


# =========================================================================== registry

_ALL = frozenset({"pyiceberg", "duckdb", "polars", "daft", "spark", "trino", "pg_lake", "flink"})

#: Which engines offer a capability that is not simply "the adapter has a method for it". Every
#: test asks :func:`require` instead of keeping its own list; for every other engine the claim
#: "not offered" is checked (:func:`not_offered`) or proven by a test of its own
#: (:data:`PROVEN_BY`). One place to read, one place to change after an engine update.
OFFERED: dict[str, frozenset[str]] = {
    "incremental reads": frozenset({"pyiceberg", "polars", "spark", "flink", "trino"}),
    "changelog reads": frozenset({"spark", "trino"}),
    "streaming writes": frozenset({"flink", "spark", "polars"}),
    "streaming reads": frozenset({"flink", "spark"}),
    "multi-table transactions": frozenset({"duckdb", "pg_lake", "flink", "trino"}),
    # PyIceberg: view metadata with one representation per dialect (test_views).
    "creating views": frozenset({"pyiceberg", "duckdb", "spark", "trino", "flink"}),
    "querying views": frozenset({"duckdb", "spark", "trino", "flink"}),
    "SQL DELETE + UPDATE": frozenset({"duckdb", "spark", "trino", "pg_lake"}),
    "computing table statistics": frozenset({"spark", "trino", "pg_lake"}),
    "computing partition statistics": frozenset({"spark"}),
    "writing to branches": frozenset({"pyiceberg", "spark", "flink", "trino"}),
    "a session time zone": frozenset({"duckdb", "spark", "trino", "flink", "pg_lake"}),
    "format v3 tables": _ALL - {"pg_lake"},
    "writing tables it did not create": _ALL - {"pg_lake"},
    "vended credentials": _ALL - {"pg_lake"},
    # Configurable for it; DuckDB, Daft and the Polars reader then fail with their known gap.
    "configuring remote signing": _ALL - {"pg_lake", "trino"},
    # Flink only with a primary key (upsert); its MERGE INTO is tried in the test itself.
    "upsert/MERGE": frozenset({"pyiceberg", "duckdb", "spark", "trino", "flink"}),
}

#: "Not offered" claims that a test of their own proves (the API check would not see them):
#: ``(engine, capability) -> (skip reason, proving test)``. The proving test fails as soon as the
#: engine gains the capability.
PROVEN_BY: dict[tuple[str, str], tuple[str, str]] = {
    ("pg_lake", "format v3 tables"): (
        "pg_lake creates only format v2 tables",
        "tests/selftest/test_na_claims.py::test_pg_lake_creates_only_v2",
    ),
    ("pg_lake", "upsert/MERGE"): (
        "pg_lake has no API for MERGE on Iceberg tables",
        "tests/engines/test_operations.py::test_operation[pg_lake-merge-v2] (fails with its gap)",
    ),
    ("pg_lake", "writing tables it did not create"): (
        "pg_lake has no API for writing tables it did not create (read-only attach)",
        "tests/specific/test_pglake.py::test_attached_table_is_read_only",
    ),
}


def offers(engine: str, capability: str) -> bool:
    """Whether the registry lists ``engine`` for ``capability``.

    Args:
        engine: Engine name.
        capability: Key of :data:`OFFERED`.

    Returns:
        True if offered.
    """
    return engine in OFFERED[capability]


def require(engine: str, capability: str, engine_pool=None) -> None:
    """Return if ``engine`` offers ``capability``; else skip as not offered - proven
    (:data:`PROVEN_BY`) or checked against the engine's live API (:func:`not_offered`).

    Args:
        engine: Engine name.
        capability: Key of :data:`OFFERED`.
        engine_pool: Session pool of engine adapters (for the API check; not needed for a
            proven claim).
    """
    if offers(engine, capability):
        return
    proof = PROVEN_BY.get((engine, capability))
    if proof:
        pytest.skip(f"{proof[0]} (proven by {proof[1]})")
    not_offered(engine, capability, None, engine_pool)


def require_format(engine: str, fv: int, engine_pool=None) -> None:
    """Skip a format-v3 case for an engine that creates no v3 tables (see :func:`require`).

    Args:
        engine: Engine name.
        fv: Iceberg format version of the case.
        engine_pool: Session pool of engine adapters.
    """
    if fv >= 3:
        require(engine, "format v3 tables", engine_pool)
