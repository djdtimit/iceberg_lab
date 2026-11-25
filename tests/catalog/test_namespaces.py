"""Namespaces per engine: create, nest, properties, refuse dropping a non-empty one, drop.

Every check is verified through PyIceberg (the REST view of the catalog under test), independent of the
engine that made the change. pg_lake, Polars and Daft have no namespace DDL ("no API");
pg_lake's schemas are local Postgres objects.

``create``          ``CREATE NAMESPACE|SCHEMA|DATABASE <ns>``
``nested``          a child namespace ``<parent>.<child>`` (Iceberg allows multi-level names)
``properties``      set a namespace property
``drop_nonempty``   dropping a namespace that still contains a table must be refused and
                    must leave the table intact
``drop``            dropping an empty namespace
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from iceberg_lab.config import WAREHOUSE_STS
from iceberg_lab.engines import pyiceberg_catalog

from conftest import (
    ENGINES,
    drop_namespace_tree,
    engine_param,
    expect,
    find_gap,
    gap_table,
    layer_rows,
    not_offered,
)

WAREHOUSE = WAREHOUSE_STS
#: How engines word "namespace still contains tables" (observed messages).
NOT_EMPTY = (
    r"(?i)(not empty|non-empty|NamespaceNotEmpty|contains? (tables|objects)|has tables|"
    r"cannot (be )?drop|dependent|409|conflict)"
)
TASKS = ["create", "nested", "properties", "drop_nonempty", "drop"]


def _sql(engine, adapter, ns_parts: tuple[str, ...]) -> str:
    """Fully qualified namespace name in ``engine``'s SQL.

    Spark: one identifier per level (``sts.`a`.`b```). DuckDB and Trino address a nested
    namespace as ONE quoted name with dots (``lk."a.b"``; Trino needs
    ``iceberg.rest-catalog.nested-namespace-enabled``). Flink databases are single-level.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns_parts: Namespace levels.

    Returns:
        The name in the engine's SQL.
    """
    if engine == "spark":
        return adapter.catalog + "." + ".".join(f"`{p}`" for p in ns_parts)
    if engine == "flink":
        return adapter.catalog + "." + ".".join(f"`{p}`" for p in ns_parts)
    cat = adapter.alias if engine == "duckdb" else adapter.catalog
    return f'{cat}."{".".join(ns_parts)}"'


class PyIcebergNs:
    """Namespace tasks through PyIceberg's catalog API."""

    def __init__(self, adapter):
        """Open PyIceberg on the warehouse.

        Args:
            adapter: Engine adapter (unused).
        """
        self.cat = pyiceberg_catalog(WAREHOUSE)

    def create(self, ns):
        """Create the namespace.

        Args:
            ns: Namespace (tuple of levels).
        """
        self.cat.create_namespace(ns)

    def set_property(self, ns, k, v):
        """Set one namespace property.

        Args:
            ns: Namespace (tuple of levels).
            k: Property key.
            v: Property value.
        """
        self.cat.update_namespace_properties(ns, updates={k: v})

    def drop(self, ns):
        """Drop the namespace.

        Args:
            ns: Namespace (tuple of levels).
        """
        self.cat.drop_namespace(ns)


class SqlNs:
    """Namespace tasks through SQL DDL (``CREATE/DROP SCHEMA``; subclasses change the keyword)."""

    keyword = "SCHEMA"
    engine = ""

    def __init__(self, adapter):
        """Bind the engine adapter.

        Args:
            adapter: Engine adapter.
        """
        self.e = adapter

    def create(self, ns):
        """``CREATE <keyword> <ns>``.

        Args:
            ns: Namespace (tuple of levels).
        """
        self.e.execute(f"CREATE {self.keyword} {_sql(self.engine, self.e, ns)}")

    def drop(self, ns):
        """``DROP <keyword> <ns>``.

        Args:
            ns: Namespace (tuple of levels).
        """
        self.e.execute(f"DROP {self.keyword} {_sql(self.engine, self.e, ns)}")


class SparkNs(SqlNs):
    """Spark: ``CREATE/DROP NAMESPACE``, properties with ``SET PROPERTIES``."""

    keyword, engine = "NAMESPACE", "spark"

    def set_property(self, ns, k, v):
        """``ALTER NAMESPACE ... SET PROPERTIES``.

        Args:
            ns: Namespace (tuple of levels).
            k: Property key.
            v: Property value.
        """
        self.e.execute(
            f"ALTER NAMESPACE {_sql('spark', self.e, ns)} SET PROPERTIES ('{k}' = '{v}')"
        )


class TrinoNs(SqlNs):
    """Trino: ``CREATE/DROP SCHEMA``; no namespace properties after creation."""

    engine = "trino"


class DuckDBNs(SqlNs):
    """DuckDB: ``CREATE/DROP SCHEMA`` in the attached catalog; properties with the table function
    ``set_iceberg_schema_properties`` (no DDL for it)."""

    engine = "duckdb"

    def set_property(self, ns, k, v):
        """``CALL set_iceberg_schema_properties('<catalog>."<ns>"', {k: v})``.

        Args:
            ns: Namespace (tuple of levels).
            k: Property key.
            v: Property value.
        """
        self.e.execute(
            f"CALL set_iceberg_schema_properties('{_sql('duckdb', self.e, ns)}', {{'{k}': '{v}'}})"
        )


class FlinkNs(SqlNs):
    """Flink: ``CREATE/DROP DATABASE``, properties with ``SET (...)``."""

    keyword, engine = "DATABASE", "flink"

    def set_property(self, ns, k, v):
        """``ALTER DATABASE ... SET ('k' = 'v')``.

        Args:
            ns: Namespace (tuple of levels).
            k: Property key.
            v: Property value.
        """
        self.e.execute(f"ALTER DATABASE {_sql('flink', self.e, ns)} SET ('{k}' = '{v}')")


NS_CLASSES = {
    "pyiceberg": PyIcebergNs,
    "spark": SparkNs,
    "trino": TrinoNs,
    "duckdb": DuckDBNs,
    "flink": FlinkNs,
}

# Filled from observed failures only: (engine, task, regex, reason).
# fmt: off
KNOWN_GAPS = gap_table(("engine", "task"), [
    # Lakekeeper only: Polaris sends the field and PyIceberg works there.
    *layer_rows("lakekeeper", [("pyiceberg", "properties", r"UpdateNamespacePropertiesResponse\s+missing\s+Field required", "lakekeeper-ns-properties-missing-field")]),
    *layer_rows("unity", [("[!f]*", "nested", r"Nested namespaces are not supported: nst_\w+\.child|Invalid schema full name: lab\.nst_\w+\.child|Failed to check namespace 'nst_\w+\.child'|BadRequestException: Malformed request: Bad Request\s+at org\.apache\.iceberg\.rest\.ErrorHandlers\$NamespaceErrorHandler", "unity-no-nested-namespaces")]),
    # Behind the gaps only (DuckDB cannot attach RustFS otherwise): the nested namespace's
    # path contains the separator %1F; RustFS refuses DuckDB's SigV4-signed request.
    # Through Gravitino Unity's refusal comes back as 500.
    *layer_rows("gravitino_unity", [("duckdb", "nested", r"Request returned HTTP 500 for HTTP HEAD to '\S+/namespaces/nst_duckdb_nested_\w+%1Fchild'", "unity-no-nested-namespaces"),
                                    ("spark", "nested", r"ServiceFailureException: Server error: RESTException: Internal Server Error\s+at org\.apache\.iceberg\.rest\.ErrorHandlers\$DefaultErrorHandler\.accept\(ErrorHandlers\.java:\d+\)\s+at org\.apache\.iceberg\.rest\.ErrorHandlers\$NamespaceErrorHandler", "unity-no-nested-namespaces")]),
    *layer_rows("rustfs", [("duckdb", "nested", r"HEAD request to endpoint '\S+/namespaces/nst_duckdb_nested_\w+%1Fchild' returned an error response \(HTTP 403\)", "rustfs-duckdb-nested-namespace-403")]),
    ("flink", "nested", r"create database identifier format error", "flink-no-nested-namespaces"),
])
# fmt: on


def _gap(engine, task):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        task: Namespace task.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, engine=engine, task=task))


@pytest.fixture(autouse=True)
def _cleanup():
    """Drop every namespace a test created (with tables), whatever the outcome.

    Yields:
        A list the test appends its namespaces to.
    """
    yield
    cat = pyiceberg_catalog(WAREHOUSE)
    for ns in cat.list_namespaces():
        if ns[0].startswith("nst_"):
            drop_namespace_tree(cat, ns)


@pytest.mark.parametrize("task", TASKS)
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_namespace(engine, task, engine_pool):
    """The engine performs a namespace task; the result is checked via PyIceberg.

    Checks per task:
        * create: the namespace is listed;
        * nested: ``<ns>.child`` is listed under the parent;
        * properties: the property ``team=lab`` set by the engine is read back;
        * drop_nonempty: dropping a namespace with a table fails with "not empty", and the table
          still exists;
        * drop: the dropped namespace is no longer listed.
        Engines without namespace DDL or properties: not offered.

    Args:
        engine: Engine under test.
        task: ``create``, ``nested``, ``properties``, ``drop_nonempty`` or ``drop``.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    cls = NS_CLASSES.get(engine)
    if cls is None:
        not_offered(engine, "namespaces", None, engine_pool)
    if task == "properties" and not hasattr(cls, "set_property"):
        not_offered(engine, "namespace properties", None, engine_pool)
    cat = pyiceberg_catalog(WAREHOUSE)
    name = f"nst_{engine}_{task}_{uuid.uuid4().hex[:6]}"
    with _gap(engine, task):
        ops = cls(None if engine == "pyiceberg" else engine_pool(engine))
        if task == "create":
            ops.create((name,))
            assert (name,) in cat.list_namespaces()
        elif task == "nested":
            cat.create_namespace((name,))
            ops.create((name, "child"))
            assert (name, "child") in cat.list_namespaces((name,))
        elif task == "properties":
            cat.create_namespace((name,))
            # not "owner": Spark reserves it ("will be set to the current user")
            ops.set_property((name,), "team", "lab")
            assert cat.load_namespace_properties((name,)).get("team") == "lab"
        elif task == "drop_nonempty":
            cat.create_namespace((name,))
            cat.create_table(f"{name}.t", schema=pa.schema([("id", pa.int64())]))
            # The refusal must be about the namespace not being empty - any other error
            # (syntax, connection) would otherwise pass as "correctly refused".
            with pytest.raises(Exception, match=NOT_EMPTY) as exc:
                ops.drop((name,))
            assert cat.table_exists(f"{name}.t"), f"table gone after refused drop: {exc.value}"
        elif task == "drop":
            cat.create_namespace((name,))
            ops.drop((name,))
            assert (name,) not in cat.list_namespaces()
