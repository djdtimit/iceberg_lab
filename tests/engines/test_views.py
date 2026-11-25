"""Iceberg views across engines: who can create one, and who can query whose view?

An Iceberg view stores SQL text per *dialect* (``representations``) plus a default
catalog/namespace. The spec does not require an engine to run another dialect's SQL, so a
view is only portable if readers accept foreign dialects - which is what this matrix shows.

For a fair comparison every creator writes the same, portable SQL: ``SELECT id FROM
<namespace>.<base table> WHERE id > 1`` - without a catalog name. A view also stores the
creator's session catalog as ``default-catalog``, and readers resolve unqualified names in
a catalog of THAT name. So all engines here use the same catalog name (:data:`VIEW_CATALOG`:
``sts`` on Lakekeeper, ``psts`` on Polaris, ``rsts`` on RustFS, ``usts`` on Unity); what happens
when the names differ is shown by ``test_view_needs_matching_catalog_names``.

``test_create_view[creator]``    the view exists in the catalog with the creator's dialect
``test_read_view[creator-reader]`` the reader queries it and gets ``{2, 3}``

PyIceberg has view metadata but no SQL engine, so it has no dialect of its own: it creates the
view with the same SQL in every dialect of :data:`VIEW_ENGINES` (Iceberg allows one
representation per dialect) - the test then shows whether PyIceberg writes view metadata every
engine can run. PyIceberg cannot query views; Polars, Daft and pg_lake have no view support
("no API").
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from iceberg_lab.config import PROFILE, WAREHOUSE_STS
from iceberg_lab.engines import pyiceberg_catalog

from conftest import (
    ENGINES,
    engine_param,
    expect,
    expect_cause,
    find_gap,
    gap_table,
    layer_rows,
    require,
)

WAREHOUSE = WAREHOUSE_STS
#: Engines with SQL views, and the dialect name they store.
VIEW_ENGINES = {"spark": "spark", "trino": "trino", "duckdb": "duckdb", "flink": "flink"}

# Filled from observed failures only: (creator, reader, regex, cause); rows
# that apply are alternatives, the first whose regex matches the error wins.
# fmt: off
KNOWN_GAPS = gap_table(("creator", "reader"), [
    # PyIceberg posts to the views endpoint; Unity answers 404 without a message.
    *layer_rows("unity", [("pyiceberg", "create", r"^RESTError: NotFoundException: Not Found$", "unity-views-read-only"),
       ("*", "create", r"Server does not support endpoint: POST /v1/\{prefix\}/namespaces/\{namespace\}/views|This Iceberg REST catalog server does not support creating views", "unity-views-read-only"),
       # Through Gravitino Trino shows only its generic message.
       ("trino", "create", r"ICEBERG_CATALOG_ERROR, message=\"Failed to create view 'v_\w+'\"", "unity-views-read-only")]),
    # Not for PyIceberg's views: they carry a representation for every dialect.
    *((c, "trino", r"ICEBERG_UNSUPPORTED_VIEW_DIALECT", "trino-view-dialect") for c in VIEW_ENGINES),
    *((c, "duckdb", r"no SQL representation with dialect 'duckdb'", "duckdb-view-dialect") for c in VIEW_ENGINES),
    *layer_rows("rustfs", [(e, "create", r"view schema field id must be positive", "rustfs-view-field-id-0") for e in ("spark", "flink")]),
])
# fmt: on


def _gap(creator, reader):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        creator: Engine that created the view.
        reader: Engine that reads.

    Returns:
        The context manager.
    """
    if creator == reader:  # an engine reading its own view: dialect gaps do not apply
        return expect(None)
    return expect(find_gap(KNOWN_GAPS, creator=creator, reader=reader))


#: Catalog name every view engine uses: ``sts`` under Lakekeeper (Trino:
#: ``trino/catalog/sts.properties``), ``psts`` under Polaris, ``rsts`` under RustFS
#: - Trino catalogs are fixed files, so each catalog profile needs its own name there.
VIEW_CATALOG = PROFILE.views_catalog


def view_adapter(engine, pool):
    """Adapter whose Iceberg catalog is called :data:`VIEW_CATALOG`.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        pool: Engine adapter pool.

    Returns:
        The adapter.
    """
    if engine == "duckdb":
        from iceberg_lab.engines import DuckDBEngine

        class _ViewDuckDB(DuckDBEngine):
            alias = VIEW_CATALOG

        return _ViewDuckDB(WAREHOUSE)
    if engine == "trino":
        import trino
        from iceberg_lab.config import SETTINGS
        from iceberg_lab.engines import TrinoEngine

        e = TrinoEngine(WAREHOUSE)
        e.catalog = VIEW_CATALOG
        e.conn = trino.dbapi.connect(
            host=SETTINGS.trino_host, port=SETTINGS.trino_port, user="tests", catalog=VIEW_CATALOG
        )
        return e
    if engine == "spark":
        from iceberg_lab.engines import SparkEngine

        base = pool("spark")
        if base.catalog != VIEW_CATALOG:
            # Spark loads catalogs lazily from the session conf: register the same catalog
            # (all spark.sql.catalog.<name>.* settings) under the view catalog name.
            conf = base.spark.sparkContext.getConf().getAll()
            prefix = f"spark.sql.catalog.{base.catalog}"
            for k, v in conf:
                if k == prefix or k.startswith(prefix + "."):
                    base.spark.conf.set(f"spark.sql.catalog.{VIEW_CATALOG}{k[len(prefix) :]}", v)
        e = SparkEngine(base.spark, WAREHOUSE)
        e.catalog = VIEW_CATALOG
        return e
    if engine == "flink":
        from iceberg_lab.config import SETTINGS, catalog_auth_props
        from iceberg_lab.engines import FlinkEngine

        e = FlinkEngine(WAREHOUSE)
        if e.catalog != VIEW_CATALOG:
            auth = "".join(f", '{k}'='{v}'" for k, v in catalog_auth_props(SETTINGS).items())
            e.execute(
                f"CREATE CATALOG {VIEW_CATALOG} WITH ('type'='iceberg', 'catalog-type'='rest', "
                f"'uri'='{SETTINGS.catalog_uri}', 'warehouse'='{WAREHOUSE}'{auth})"
            )
            e.catalog = VIEW_CATALOG
        return e
    return pool(engine)


@pytest.fixture(scope="module")
def adapters(engine_pool):
    """engine -> adapter with catalog name :data:`VIEW_CATALOG` (created lazily, closed at module end).

    Args:
        engine_pool: Session pool of engine adapters (conftest fixture).

    Yields:
        ``get(engine) -> adapter``.
    """
    made = {}

    def get(engine):
        """The engine's view adapter (created on first use).

        Args:
            engine: Engine name (see ``conftest.ENGINES``).

        Returns:
            The adapter.
        """
        if engine not in made:
            made[engine] = view_adapter(engine, engine_pool)
        return made[engine]

    yield get
    for name, a in made.items():
        if name in ("duckdb", "trino", "flink"):
            a.close()


def _use(engine, adapter, ns):
    """``(switch to the Iceberg catalog, switch back)`` statements for ``engine``'s session.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.

    Returns:
        ``(use statement, switch-back statement)``.
    """
    if engine == "spark":
        return f"USE {adapter.catalog}", "USE spark_catalog.default"
    if engine == "duckdb":
        return f'USE {adapter.alias}."{ns}"', "USE memory"
    if engine == "flink":
        return f"USE CATALOG {adapter.catalog}", "USE CATALOG default_catalog"
    return None, None  # Trino: the connection's default catalog is already VIEW_CATALOG


def _pyiceberg_view(ns: str, view: str, sql: str) -> None:
    """Create a view through PyIceberg's catalog API with one SQL representation per dialect of
    :data:`VIEW_ENGINES` and :data:`VIEW_CATALOG` as default catalog (as an engine would store
    its session catalog).

    Args:
        ns: Namespace of the view and its base table.
        view: View name.
        sql: View SQL (portable: no catalog name).
    """
    import time

    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    from pyiceberg.view.metadata import SQLViewRepresentation, ViewVersion

    reps = [SQLViewRepresentation(type="sql", sql=sql, dialect=d) for d in VIEW_ENGINES.values()]
    version = ViewVersion(
        version_id=1,
        schema_id=0,
        timestamp_ms=int(time.time() * 1000),
        summary={"engine-name": "pyiceberg"},
        representations=reps,
        default_catalog=VIEW_CATALOG,
        default_namespace=[ns],
    )
    schema = Schema(NestedField(1, "id", LongType(), required=False), schema_id=0)
    pyiceberg_catalog(WAREHOUSE).create_view((ns, view), schema=schema, view_version=version)


@pytest.fixture(scope="module")
def base_table(namespace):
    """``(namespace, table)`` with ids 1..3, created by PyIceberg.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(namespace, table)``.
    """
    name = f"vbase_{uuid.uuid4().hex[:6]}"
    t = pyiceberg_catalog(WAREHOUSE).create_table(
        f"{namespace}.{name}", schema=pa.schema([("id", pa.int64())])
    )
    t.append(pa.table({"id": [1, 2, 3]}))
    return namespace, name


@pytest.fixture(scope="module")
def views():
    """creator -> view name, for successfully created views.

    Returns:
        An empty dict.
    """
    return {}


@pytest.mark.slow
@pytest.mark.parametrize("creator", [engine_param(e) for e in ENGINES])
def test_create_view(creator, base_table, adapters, views, engine_pool):
    """The creator creates a view with portable SQL; the catalog stores it with its dialect.

    Checks:
        * the view exists and one of its representations has the creator's SQL dialect.
        Engines without view DDL: not offered.

    Args:
        creator: Engine that creates the view.
        base_table: ``(namespace, table)`` with ids 1..3.
        adapters: ``engine -> adapter`` with the shared view catalog name.
        views: ``creator -> view name`` of the created views.
        engine_pool: Session pool of engine adapters (for the "not offered" check).
    """
    require(creator, "creating views", engine_pool)
    ns, base = base_table
    view = f"v_{creator}_{uuid.uuid4().hex[:6]}"
    sql = f"SELECT id FROM {ns}.{base} WHERE id > 1"
    with _gap(creator, "create"):
        if creator in VIEW_ENGINES:
            e = adapters(creator)
            use, reset = _use(creator, e, ns)
            if use:
                e.execute(use)
            try:
                e.execute(f"CREATE VIEW {e.fq(ns, view)} AS {sql}")
            finally:
                if reset:
                    e.execute(reset)
            want = {VIEW_ENGINES[creator]}
        else:  # PyIceberg: view metadata only, the same SQL for every dialect
            _pyiceberg_view(ns, view, sql)
            want = set(VIEW_ENGINES.values())
        v = pyiceberg_catalog(WAREHOUSE).load_view((ns, view))
        dialects = {r.root.dialect.lower() for r in v.metadata.versions[-1].representations}
        assert want <= dialects, dialects
    views[creator] = view


@pytest.mark.slow
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("creator", [engine_param(e) for e in ENGINES])
def test_read_view(creator, reader, base_table, adapters, views, engine_pool):
    """The reader queries the creator's view and gets the rows of the view definition.

    Checks:
        * the reader returns ids 2 and 3.
        Readers that cannot query views and creators that cannot create one (Polars, Daft,
        pg_lake): not offered - there is no such case. Skipped (blocked) only if a creator that
        has view DDL did not create its view.

    Args:
        creator: Engine that creates the view.
        reader: Engine that reads it.
        base_table: ``(namespace, table)`` with ids 1..3.
        adapters: ``engine -> adapter`` with the shared view catalog name.
        views: ``creator -> view name`` of the created views.
        engine_pool: Session pool of engine adapters (for the "not offered" check).
    """
    require(reader, "querying views", engine_pool)
    require(creator, "creating views", engine_pool)
    if creator not in views:
        pytest.skip(f"{creator} did not create a view (see test_create_view)")
    ns, _ = base_table
    with _gap(creator, reader):
        e = adapters(reader)
        got = sorted(r[0] for r in e.execute(f"SELECT id FROM {e.fq(ns, views[creator])}"))
        assert got == [2, 3]


@pytest.mark.slow
def test_view_needs_matching_catalog_names(base_table, engine_pool):
    """A Trino view in catalog ``lk_sts`` cannot be resolved by Spark (catalog ``sts``).

    Checks:
        * Trino stores its catalog name as the view's default catalog;
        * Spark's query fails because it cannot resolve that catalog (documented limitation of
          engine-specific catalog names).

    Args:
        base_table: ``(namespace, table)`` with ids 1..3.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    from conftest import require_engine

    trino_cat = f"{PROFILE.trino_prefix}_sts"  # lk_sts / pol_sts
    require_engine("trino")
    require_engine("spark")
    ns, base = base_table
    tr = engine_pool("trino")  # catalog lk_sts / pol_sts
    view = f"v_lk_{uuid.uuid4().hex[:6]}"
    with _gap("trino", "create"):
        tr.execute(f"CREATE VIEW {tr.fq(ns, view)} AS SELECT id FROM {ns}.{base} WHERE id > 1")
    v = pyiceberg_catalog(WAREHOUSE).load_view((ns, view))
    assert v.metadata.versions[-1].default_catalog == trino_cat
    with expect_cause(
        "spark-view-default-catalog", r"REQUIRES_SINGLE_PART_NAMESPACE|TABLE_OR_VIEW_NOT_FOUND"
    ):
        engine_pool("spark").execute(f"SELECT id FROM {engine_pool('spark').fq(ns, view)}")
