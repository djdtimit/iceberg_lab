"""Shared pytest infrastructure: engines, fixtures, known gaps and dependent results.

Engine availability
    Not every engine is always running: Trino, Flink and pg_lake are optional compose profiles,
    Spark needs Java. :data:`AVAILABILITY` is computed once at import time; tests for an
    unavailable engine are *skipped* with a how-to-start message - or fail if the engine is
    listed in ``REQUIRE_ENGINES`` (``all`` in full runs).

Isolation
    Every test session works in its own namespace (``test_<random>``), created in both
    warehouses of the catalog under test (``CATALOG``) and dropped with all tables at the end,
    so runs never see each other's data and can be repeated against the same catalog.

Speed
    A SparkSession costs ~20 s and DuckDB/Trino/Flink connections are not free either.
    ``spark`` and ``engine_pool`` are session-scoped so large matrices reuse them;
    ``make_engine`` creates fresh adapters for tests that need a specific warehouse/mode.

The lab framework (``src/iceberg_lab/lab/``)
    Known gaps (``gaps``: ``expect_gap``, severities, central catalog rules), dependent results
    (``results``: ``skip_if_not_produced``, ``keep_if_content_ok``), what engines offer
    (``capabilities``: the registry, ``require``, the checked ``not_offered``), the report and
    the run manifest. This file wires them into pytest (hooks, fixtures) and re-exports the
    names the tests import from ``conftest``.

Lab helps
    Engines that cannot connect to a catalog with their stock setup get a sensible lab
    workaround (``iceberg_lab.config.HELPS``); a test that works with it counts as working.
    The run without help (``LAB_NO_HELP=1``) selects only the tests of these engines
    (``pytest_collection_modifyitems``) and runs them without it; its report sits next to the
    comparison. The gap itself: tests/security/test_stock_connection.py, always without help.

Markers
    JVM engines (Spark, Trino, Flink) get ``@pytest.mark.slow`` automatically through
    :func:`engine_param`, so ``pytest -m "not slow"`` gives a quick run.
"""

import sys as _sys
from pathlib import Path as _Path

# Tests live in subfolders (engines/, catalog/, security/, storage/, specific/, selftest/, demos/)
# and the lab's helper modules in lab/. They import each other by module name
# (``import test_operations as base``, ``from authz_backends import ...``), so every folder is
# on the import path - module names are unique across folders.
_TESTS = _Path(__file__).resolve().parent
for _d in [_TESTS, *sorted(p for p in _TESTS.iterdir() if p.is_dir() and p.name[0] not in "_.")]:
    if str(_d) not in _sys.path:
        _sys.path.insert(0, str(_d))

import importlib.util
import os
import shutil
import socket
import uuid
import warnings

import pyarrow as pa
import pytest
import requests
from pyiceberg.exceptions import NamespaceNotEmptyError

from iceberg_lab import bootstrap
from iceberg_lab.config import (
    PROFILE,
    REMOTE_SIGNING,
    SETTINGS,
    WAREHOUSE_SIGNING,
    WAREHOUSE_STS,
    WAREHOUSES,
)
from iceberg_lab.engines import (
    SCHEMA,
    DaftEngine,
    DuckDBEngine,
    FlinkEngine,
    PgLakeEngine,
    PolarsEngine,
    PyIcebergEngine,
    SparkEngine,
    TrinoEngine,
    pyiceberg_catalog,
)
from iceberg_lab.lab import gaps, rest_observed

# Re-exported: tests import these from conftest (``from conftest import expect_gap``).
from iceberg_lab.lab.capabilities import (  # noqa: F401
    CAPABILITY_API,
    NOT_OFFERED_EXEMPT,
    not_offered,
    offers,
    require,
    require_format,
)
from iceberg_lab.lab.gap_register import (  # noqa: F401
    Gap,
    expect,
    expect_cause,
    find_gap,
    gap,
    gap_table,
    layer_rows,
    matching,
)
from iceberg_lab.lab.gaps import (  # noqa: F401
    CATALOG_GAPS,
    FLAKY_GAPS,
    GENERIC_ERRORS,
    SEVERITIES,
    GapFixedError,
    KnownGapError,
    expect_gap,
    gap_severity,
)
from iceberg_lab.lab.results import NOT_RUN, keep_if_content_ok, skip_if_not_produced  # noqa: F401

#: Count the catalog's answers to the lab's raw requests per spec operation (docs/rest-conformance.md).
rest_observed.install(SETTINGS.catalog_uri)


def pytest_configure(config):
    """Register the comparison-report plugin (iceberg_lab.lab.reporting) for every run.

    Args:
        config: pytest config.
    """
    from iceberg_lab.lab import reporting

    if not config.pluginmanager.is_registered(reporting):
        config.pluginmanager.register(reporting, "iceberg-lab-reporting")


#: Engines of the per-engine test files (tests/specific/), for the run without help.
_MODULE_ENGINES = {
    "test_pyiceberg": {"pyiceberg"},
    "test_dataframes": {"polars", "daft"},
    "test_duckdb": {"duckdb"},
    "test_pglake": {"pg_lake"},
}


def help_run_involves(item, helped: set[str]) -> bool:
    """Whether a test belongs in the run without help: one of the helped engines takes part.

    An engine takes part when it is one of the parameters (engine, writer, reader, ...) or the
    test file is that engine's own file. Tests without such an engine would only repeat the
    normal run (the lab's own client is the same in both runs).

    Args:
        item: Collected pytest item.
        helped: Engines that get a lab help on the catalog under test.

    Returns:
        True if the test runs in the run with help.
    """
    module = item.nodeid.split("::", 1)[0].rsplit("/", 1)[-1].removesuffix(".py")
    if _MODULE_ENGINES.get(module, set()) & helped:
        return True
    callspec = getattr(item, "callspec", None)
    if callspec is None:
        return False
    parts = set(callspec.id.split("-"))
    parts |= {v for v in callspec.params.values() if isinstance(v, str)}
    return bool(parts & helped)


#: Test files that run against a cloud catalog (``PROFILE.remote``): REST conformance and the
#: engines' read/write behaviour. Not the attack-like security tests (a provider's terms decide
#: whether they are allowed), nothing that rebuilds or disturbs the catalog (fault proxies, IdP
#: outage, the lab's users and roles), nothing that needs the lab's storage (RustFS).
REMOTE_SAFE = (
    "tests/catalog/test_rest_conformance.py",
    "tests/catalog/test_namespaces.py",
    "tests/engines/test_operations.py",
    "tests/engines/test_types.py",
    "tests/engines/test_evolution.py",
    "tests/engines/test_interop.py",
)
#: Engines that reach a cloud catalog from their client settings alone; Trino, Flink and
#: pg_lake are configured in their containers for the lab's catalogs.
REMOTE_ENGINES = {"pyiceberg", "polars", "daft", "duckdb", "spark"}


def item_engines(item) -> set[str]:
    """The engines a test involves: its engine parameters and its file's own engine.

    Args:
        item: Collected pytest item.

    Returns:
        Engine names (empty for a test of the lab's own client).
    """
    module = item.nodeid.split("::", 1)[0].rsplit("/", 1)[-1].removesuffix(".py")
    found = set(_MODULE_ENGINES.get(module, set()))
    callspec = getattr(item, "callspec", None)
    if callspec is not None:
        found |= set(callspec.id.split("-")) | {
            v for v in callspec.params.values() if isinstance(v, str)
        }
    return found & set(AVAILABILITY)


def remote_safe(item) -> bool:
    """Whether a test runs against a cloud catalog (:data:`REMOTE_SAFE`, :data:`REMOTE_ENGINES`).

    Args:
        item: Collected pytest item.

    Returns:
        True if its file is listed and every engine it involves reaches a cloud catalog.
    """
    path = item.nodeid.split("::", 1)[0]
    return path in REMOTE_SAFE and item_engines(item) <= REMOTE_ENGINES


def pytest_collection_modifyitems(config, items):
    """Narrow the session for a cloud catalog and for the run without help.

    A cloud catalog (``PROFILE.remote``) keeps only :func:`remote_safe` tests; the others are
    deselected (not run, not reported as "not offered").

    The run without help (``LAB_NO_HELP=1``) keeps only the tests of the helped engines. Its
    report is shown next to the same tests of the normal run; the other tests would only
    repeat it. A catalog without helps keeps nothing (the run has nothing to show).

    Args:
        config: pytest config.
        items: Collected items, filtered in place.
    """
    from iceberg_lab.config import WITH_HELP, helped_engines

    if PROFILE.remote:
        dropped = [i for i in items if not remote_safe(i)]
        if dropped:
            config.hook.pytest_deselected(items=dropped)
            items[:] = [i for i in items if remote_safe(i)]
    if WITH_HELP:
        return
    helped = helped_engines()
    keep, dropped = [], []
    for i in items:
        (keep if help_run_involves(i, helped) else dropped).append(i)
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = keep


# =========================================================================== availability


def _reachable(host: str, port: int) -> bool:
    """True if a TCP connection to ``host:port`` succeeds within one second.

    Args:
        host: Host name.
        port: TCP port.

    Returns:
        Whether the connection succeeded.
    """
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:  # includes DNS failure when the compose service does not exist
        return False


def _trino_ready() -> bool:
    """True once the Trino coordinator has finished starting.

    Trino accepts HTTP connections long before it can run queries; ``/v1/info`` reports
    ``"starting": true`` until catalogs are loaded, so a plain port check is not enough.

    Returns:
        Whether Trino can run queries.
    """
    try:
        r = requests.get(f"http://{SETTINGS.trino_host}:{SETTINGS.trino_port}/v1/info", timeout=2)
        return r.ok and not r.json().get("starting", True)
    except requests.RequestException:
        return False


def _flink_ready() -> bool:
    """True if the Flink SQL Gateway answers (the cluster starts before the gateway).

    Returns:
        Whether the SQL Gateway answers.
    """
    try:
        return requests.get(f"{SETTINGS.flink_url}/v1/info", timeout=2).ok
    except requests.RequestException:
        return False


def _has(module: str) -> bool:
    """True if ``module`` is importable (its dependency group is installed).

    Args:
        module: Module name.

    Returns:
        Whether it can be imported.
    """
    return importlib.util.find_spec(module) is not None


#: engine name -> (available?, skip reason). Order defines the order in all matrices.
AVAILABILITY = {
    "pyiceberg": (True, ""),
    "duckdb": (_has("duckdb"), "duckdb not installed"),
    "polars": (_has("polars"), "polars not installed"),
    "daft": (_has("daft"), "daft not installed"),
    "spark": (
        _has("pyspark") and shutil.which("java") is not None,
        "pyspark or java missing",
    ),
    "trino": (_trino_ready(), "Trino not running (docker compose --profile trino up -d trino)"),
    "pg_lake": (
        _has("psycopg") and _reachable(SETTINGS.pglake_host, SETTINGS.pglake_port),
        "pg_lake not running (docker compose --profile pglake up -d pg-lake)",
    ),
    "flink": (_flink_ready(), "Flink not running (docker compose --profile flink up -d flink)"),
}

#: Engines that cannot talk to the catalog under test at all (no API for its authentication):
#: skipped as "no API" in every matrix, even with REQUIRE_ENGINES=all - not a missing service.
CATALOG_UNSUPPORTED: dict[str, str] = (
    {
        "pg_lake": "pg_lake has no API for SigV4 catalog authentication "
        "(rest_auth_type: default, oauth2, horizon, none)"
    }
    if PROFILE.auth == "sigv4"
    else {}
)

#: Engines that MUST be available, from ``REQUIRE_ENGINES=trino,spark`` (or ``all``).
#: For these, "not running" fails the test instead of skipping it - so a crashed Trino
#: cannot turn hundreds of tests into silent skips and a green-looking run.
_required = os.getenv("REQUIRE_ENGINES", "")
REQUIRED_ENGINES = (
    set(AVAILABILITY) if _required == "all" else {e for e in _required.split(",") if e}
)
_unknown = REQUIRED_ENGINES - set(AVAILABILITY)
if _unknown:
    raise ValueError(f"REQUIRE_ENGINES contains unknown engines: {sorted(_unknown)}")

#: All engine names, in matrix order.
ENGINES = list(AVAILABILITY)

#: Adapter class per engine. Spark is missing on purpose: its adapter needs the shared
#: SparkSession from the ``spark`` fixture and is built separately.
ENGINE_CLASSES = {
    "pyiceberg": PyIcebergEngine,
    "duckdb": DuckDBEngine,
    "polars": PolarsEngine,
    "daft": DaftEngine,
    "trino": TrinoEngine,
    "pg_lake": PgLakeEngine,
    "flink": FlinkEngine,
}

#: Engines that run on the JVM - slower to start, marked ``slow``.
JVM_ENGINES = {"spark", "trino", "flink"}


def engine_param(name: str, *extra_marks):
    """``pytest.param`` for an engine name, with the right marks attached.

    Adds ``slow`` for JVM engines and a ``skip`` mark (with a how-to-start reason) when the
    engine is unavailable. Use it in every ``@pytest.mark.parametrize`` over engines, so
    skipping behaves the same everywhere::

        @pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])

    Args:
        name: Engine name.
        extra_marks: Further pytest marks.

    Returns:
        The ``pytest.param``.
    """
    marks = list(extra_marks)
    if name in JVM_ENGINES:
        marks.append(pytest.mark.slow)
    if name in CATALOG_UNSUPPORTED:
        marks.append(pytest.mark.skip(reason=CATALOG_UNSUPPORTED[name]))
        return pytest.param(name, marks=marks, id=name)
    ok, reason = AVAILABILITY[name]
    if not ok and name not in REQUIRED_ENGINES:
        marks.append(pytest.mark.skip(reason=reason))
    # Required but unavailable: no skip mark -> require_engine() fails inside the test.
    return pytest.param(name, marks=marks, id=name)


def warehouse_params():
    """``[signing, sts]`` warehouse params; signing is "no API" if the catalog lacks it.

    Returns:
        The two ``pytest.param`` entries.
    """
    no_signing = pytest.mark.skip(reason=f"catalog {PROFILE.name} has no API for remote signing")
    return [
        # Neutral ids (not the catalog's warehouse names): the same case has the same test id
        # on every catalog, so the catalogs line up in the comparison.
        pytest.param(WAREHOUSE_SIGNING, id="signing", marks=() if REMOTE_SIGNING else no_signing),
        pytest.param(WAREHOUSE_STS, id="sts"),
    ]


def require_remote_signing() -> None:
    """Skip a remote-signing test on catalogs that do not offer it ("no API")."""
    if not REMOTE_SIGNING:
        pytest.skip(f"catalog {PROFILE.name} has no API for remote signing")


def require_vended_credentials() -> None:
    """Skip a test about catalog-vended storage credentials on catalogs that vend none."""
    if not PROFILE.vended_credentials:
        pytest.skip(f"catalog {PROFILE.name} has no API for vended storage credentials")


def require_catalog(name: str, why: str) -> None:
    """Skip a test that is specific to one catalog (e.g. its management API).

    Args:
        name: Catalog the test belongs to.
        why: What is specific (shown in the skip).
    """
    if PROFILE.name != name:
        pytest.skip(f"{why} - specific to {name}, not testable on catalog {PROFILE.name}")


def require_engine(name: str) -> None:
    """Skip (or, if listed in ``REQUIRE_ENGINES``, fail) when ``name`` is not available.

    Args:
        name: Engine name.
    """
    if name in CATALOG_UNSUPPORTED:
        pytest.skip(CATALOG_UNSUPPORTED[name])
    ok, reason = AVAILABILITY[name]
    if ok:
        return
    if name in REQUIRED_ENGINES:
        pytest.fail(f"required engine {name!r} unavailable: {reason}")
    pytest.skip(reason)


#: Engines whose writes go through PyIceberg's REST client (SigV4 gap on RustFS).
PYICEBERG_WRITERS = {"pyiceberg", "polars", "daft"}


@pytest.fixture(autouse=True)
def _pyiceberg_engine_role(request):
    """Run without help: turn the lab's PyIceberg workaround off while PyIceberg/Polars/Daft is under test.

    The lab's own PyIceberg client sends ``x-amz-content-sha256`` (see
    ``engines._add_content_sha256``). In the run without help (``LAB_NO_HELP=1``), in a test
    whose writing engine (``engine``, ``writer`` or ``creator``) is PyIceberg, Polars or Daft -
    or in their own test files - every PyIceberg write behaves like stock PyIceberg. Module fixtures run before this one with
    the fix on (it is reset after every test), so shared setup stays unaffected.

    Args:
        request: pytest request of the calling test or fixture.

    Yields:
        Nothing; the test runs in between.
    """
    from iceberg_lab import engines

    callspec = getattr(request.node, "callspec", None)
    roles = {
        v
        for k, v in (callspec.params.items() if callspec else ())
        if k in ("engine", "writer", "creator")
    }
    module = request.node.module.__name__.rsplit(".", 1)[-1]
    under_test = bool(roles & PYICEBERG_WRITERS) or module in ("test_pyiceberg", "test_dataframes")
    # With the lab's help (config.WITH_HELP, the default) the workaround stays on for the
    # engines too; only the run without help (LAB_NO_HELP=1) turns it off for them.
    engines.LAB_FIXES_ENABLED = not under_test or engines.WITH_HELP
    yield
    engines.LAB_FIXES_ENABLED = True


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Label failures matching a central rule (``gaps.FLAKY_GAPS``, ``gaps.CATALOG_GAPS``) as
    known gaps - same rules as ``expect_gap``.

    Args:
        item: The test item.
        call: The call phase info.

    Yields:
        Control to the other hooks (hookwrapper).
    """
    outcome = yield
    report = outcome.get_result()
    if call.when == "call" and report.passed:
        gaps.record_passed(item.nodeid)
    # "setup" too: an engine that cannot connect may fail in a fixture already.
    if call.when not in ("setup", "call") or not report.failed or call.excinfo is None:
        return
    reason = gaps.central_gap(item.nodeid, call.excinfo, PROFILE.name)
    if reason is None:
        return
    if gaps.KNOWN_GAPS_MODE == "xfail":
        report.outcome = "skipped"
        report.wasxfail = reason
    else:
        report.longrepr = f"[known gap] {reason}\n\n{report.longreprtext}"


def pytest_terminal_summary(terminalreporter):
    """Summarise failures split into *new* errors and *known gaps* (``gaps.write_summary``).

    Args:
        terminalreporter: pytest's terminal reporter.
    """
    gaps.write_summary(terminalreporter)


# =========================================================================== fixtures


@pytest.fixture(scope="session", autouse=True)
def _bootstrap(request):
    """Make sure bucket, Lakekeeper bootstrap and warehouses exist before any test runs.

    Aborts the whole session (instead of failing hundreds of tests) when Lakekeeper is not
    reachable, which almost always means the tests were started outside the devcontainer.
    A session of only ``offline`` tests (lab self-tests that need no catalog) skips this, so
    they also run without the lab: ``uv run pytest -m offline``.

    Args:
        request: pytest request (to see the session's tests).
    """
    if all(item.get_closest_marker("offline") for item in request.session.items):
        return
    scheme, _, rest = SETTINGS.catalog_base.partition("//")
    host, _, port = rest.split("/", 1)[0].partition(":")
    if not _reachable(host, int(port or (443 if scheme == "https:" else 80))):
        pytest.exit(f"catalog {PROFILE.name} at {SETTINGS.catalog_base} is not reachable", 2)
    if not PROFILE.remote:  # a cloud catalog and its warehouse exist already
        bootstrap.main()


@pytest.fixture(scope="session")
def namespace():
    """Name of a fresh namespace that exists in BOTH warehouses for this test session.

    Teardown purges every table in it (deletes data files too) and drops the namespace, so
    the bucket does not fill up over many runs. Tables that cannot be purged (e.g. created
    by an engine in an unusual state) are at least dropped from the catalog.

    Yields:
        The namespace name.
    """
    ns = f"test_{uuid.uuid4().hex[:8]}"
    catalogs = {wh: pyiceberg_catalog(wh) for wh in WAREHOUSES}
    for cat in catalogs.values():
        cat.create_namespace(ns)
    yield ns
    for cat in catalogs.values():
        drop_namespace_tree(cat, ns)


def fresh(adapter, namespace: str, table: str):
    """``adapter`` after dropping its cached state of the table (read matrices: a reader must see
    what the catalog holds now - see Engine.refresh).

    Args:
        adapter: Engine adapter.
        namespace: Namespace of the table.
        table: Table name (without namespace).

    Returns:
        The same adapter.
    """
    adapter.refresh(namespace, table)
    return adapter


def drop_namespace_tree(cat, ns) -> None:
    """Cleanup: drop views, tables (purge, else plain drop) and child namespaces, then ``ns``.

    One tolerated leftover: RustFS keeps a hidden entry for the OLD name of a renamed table,
    so the namespace stays "not empty" although nothing is listed. That catalog gap is shown
    by test_catalog_features.py::test_drop_namespace_after_rename; here it would only turn the
    teardown of an unrelated test into an error, so the namespace is left behind with a warning.

    Args:
        cat: PyIceberg catalog.
        ns: Namespace (string or tuple).
    """
    ns = (ns,) if isinstance(ns, str) else tuple(ns)
    for child in cat.list_namespaces(ns):
        drop_namespace_tree(cat, child)
    for view in cat.list_views(ns):  # views block dropping the namespace (409)
        cat.drop_view(view)
    for ident in cat.list_tables(ns):
        try:
            cat.purge_table(ident)
        except Exception:  # noqa: BLE001 - fall back to drop if purge fails (RustFS: 406)
            # A purge can fail after the table is gone (REST fixture: 500 for a missing file
            # of a table a test corrupted on purpose) - then there is nothing left to drop.
            if cat.table_exists(ident):
                cat.drop_table(ident)
    try:
        cat.drop_namespace(ns)
    except NamespaceNotEmptyError:
        if "rustfs" not in PROFILE.layers or cat.list_tables(ns) or cat.list_views(ns):
            raise
        warnings.warn(
            f"RustFS: namespace {'.'.join(ns)} keeps a renamed table's old name", stacklevel=2
        )


@pytest.fixture
def table_name(request) -> str:
    """Unique, readable table name derived from the test id, e.g. ``test_read_duckdb_..._1a2b3c``.

    Engines differ in which identifier characters they accept, so everything that is not
    alphanumeric becomes ``_``; the random suffix keeps parametrised runs apart.

    Args:
        request: pytest request of the calling test or fixture.

    Returns:
        The table name.
    """
    base = "".join(c if c.isalnum() else "_" for c in request.node.name.lower())
    return f"{base[:40]}_{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole test run (see ``iceberg_lab.engines.spark_session``).

    Yields:
        The SparkSession (stopped at the end).
    """
    require_engine("spark")
    from iceberg_lab.engines import spark_session

    session = spark_session()
    yield session
    session.stop()


@pytest.fixture
def make_engine(request):
    """Factory fixture: ``make_engine(name, warehouse, credentials="catalog") -> Engine``.

    Skips the calling test if the engine is unavailable. All adapters created through the
    factory are closed after the test. Use this when a test needs a specific warehouse or
    access mode; matrices over many tests should use ``engine_pool`` instead.

    Args:
        request: pytest request of the calling test or fixture.

    Yields:
        The factory.
    """
    created = []

    def factory(name, warehouse, credentials="catalog"):
        """Create an adapter for ``name`` on ``warehouse``; skip/fail if unavailable.

        Args:
            name: Engine name.
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).

        Returns:
            The adapter.
        """
        require_engine(name)
        if name == "spark":
            engine = SparkEngine(request.getfixturevalue("spark"), warehouse, credentials)
        else:
            engine = ENGINE_CLASSES[name](warehouse, credentials)
        created.append(engine)
        return engine

    yield factory
    for e in created:
        e.close()


@pytest.fixture
def sample() -> pa.Table:
    """Three-row sample table (id, name, amount) with a negative and a fractional amount.

    Returns:
        The Arrow table.
    """
    return pa.table(
        {"id": [1, 2, 3], "name": ["alpha", "beta", "gamma"], "amount": [1.5, 2.25, -3.0]},
        schema=SCHEMA,
    )


@pytest.fixture(scope="session")
def engine_pool(request):
    """Session-wide lazy pool of adapters on ``demo-sts``: ``engine_pool(name) -> Engine``.

    ``demo-sts`` is used because every engine can work there with catalog-vended credentials.
    Each adapter is created on first request and reused for the rest of the session, which
    makes the large type/operation matrices several times faster. Skips the calling test if
    the engine is unavailable.

    Args:
        request: pytest request of the calling test or fixture.

    Yields:
        ``get(name) -> Engine``.
    """
    pool = {}

    def get(name):
        """Return the pooled adapter for ``name``, creating it on first use.

        Args:
            name: Engine name.

        Returns:
            The adapter.
        """
        require_engine(name)
        if name not in pool:
            if name == "spark":
                pool[name] = SparkEngine(request.getfixturevalue("spark"), WAREHOUSE_STS)
            else:
                pool[name] = ENGINE_CLASSES[name](WAREHOUSE_STS)
        return pool[name]

    yield get
    for e in pool.values():
        e.close()
