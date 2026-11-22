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

Known gaps
    ``expect_gap(reason, match)`` turns a failure with exactly the documented error (regex,
    refused if it would also match a generic error) into a known gap and reports a gap that no
    longer occurs as fixed. A reason that sounds like a defect must carry a severity prefix
    (``SEVERITIES``: data loss, corruption, wrong result, ...). Gaps of a whole catalog or of
    intermittent problems are central rules (``CATALOG_GAPS``, ``FLAKY_GAPS``), bound to the
    engines/files they belong to.

Dependent results
    Read matrices depend on producer tests: ``skip_if_not_produced`` tells "producer did not
    run here" (no record) from "producer failed" (blocked); ``keep_if_content_ok`` still hands
    a producer's table to the readers when it failed only after its content was right.

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

import contextlib
import importlib.util
import os
import re
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


def pytest_configure(config):
    """Register the comparison-report plugin (tests/lab/reporting.py) for every run.

    Args:
        config: pytest config.
    """
    import reporting

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


def pytest_collection_modifyitems(config, items):
    """Run without help (``LAB_NO_HELP=1``): keep only the tests of the helped engines.

    Its report is shown next to the same tests of the normal run; the other tests would only
    repeat it. A catalog without helps keeps nothing (the run has nothing to show).

    Args:
        config: pytest config.
        items: Collected items, filtered in place.
    """
    from iceberg_lab.config import WITH_HELP, helped_engines

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


#: How known gaps are reported, from ``KNOWN_GAPS=fail|xfail`` (default ``fail``).
#:
#: ``fail``  (default) a known gap is a real failure: FAILED, message prefixed with
#:           ``[known gap] <reason>`` so it is distinguishable from new errors at a glance.
#: ``xfail`` a known gap is reported as XFAIL (green run) - for CI or when only *new*
#:           errors matter.
#:
#: In BOTH modes:
#: * an error that does not match the gap's expected message is reported as the original,
#:   unlabelled failure - a known gap can never hide a different bug;
#: * a documented gap that no longer reproduces FAILS with ``[gap fixed] <reason>`` and is
#:   listed under "FIXED gaps" in the summary - so an upgrade that fixes something is
#:   impossible to miss, and the KNOWN_GAPS entry gets removed. Exception: gaps marked
#:   ``flaky=True`` (intermittent problems) show up as "not reproduced this run", and after
#:   FLAKY_FIXED_AFTER runs in a row without them as "PROBABLY FIXED".
KNOWN_GAPS_MODE = os.getenv("KNOWN_GAPS", "fail")
if KNOWN_GAPS_MODE not in ("fail", "xfail"):
    raise ValueError(f"KNOWN_GAPS must be 'fail' or 'xfail', not {KNOWN_GAPS_MODE!r}")

#: Collected during the run for the terminal summary: (test id, reason).
_KNOWN_GAP_HITS: list[tuple[str, str]] = []
_FIXED_GAPS: list[tuple[str, str]] = []
_FLAKY_NOT_SEEN: list[tuple[str, str]] = []
_FLAKY_SEEN: list[tuple[str, str]] = []

#: An intermittent gap that has not occurred in this many runs in a row (per catalog, kept in
#: <report dir>/flaky_history.json) is listed as "probably fixed" - check and remove it.
FLAKY_FIXED_AFTER = 5


class KnownGapError(AssertionError):
    """Raised (in ``fail`` mode) when a documented gap reproduces; chains the real error."""


class GapFixedError(AssertionError):
    """Raised when a documented gap no longer reproduces: the KNOWN_GAPS entry is stale."""


#: Errors that have nothing to do with any documented gap: infrastructure down, timeouts,
#: wrong results, plain server errors, lab bugs. A gap regex matching one of them would book
#: such a failure as "known gap" - exactly how a gap hides a bug. ``expect_gap`` refuses it.
GENERIC_ERRORS = [
    "ConnectionError: HTTPConnectionPool(host='catalog', port=8181): Max retries exceeded with url: /v1/config (Caused by NewConnectionError('Connection refused'))",
    "ReadTimeout: HTTPConnectionPool(host='trino', port=8080): Read timed out. (read timeout=30)",
    "TimeoutError: timed out",
    "OSError: [Errno 111] Connection refused",
    "AssertionError: assert 1 == 2",
    "AssertionError: rows differ\nassert [(1, 'a')] == []",
    "KeyError: 'id'",
    "TypeError: 'NoneType' object is not subscriptable",
    "AttributeError: 'NoneType' object has no attribute 'execute'",
    "HTTPError: 500 Server Error: Internal Server Error for url: http://catalog:8181/v1/x/namespaces",
    "Py4JJavaError: An error occurred while calling o123.sql.\n: java.lang.NullPointerException",
    "RuntimeError: Flink has no TaskManager",
    "Exception: error",
    "ValueError: invalid literal for int() with base 10: 'x'",
]


def _require_specific(match: str | None, reason: str) -> None:
    """A documented gap must name its error precisely (see GENERIC_ERRORS).

    Args:
        match: The gap's error pattern.
        reason: The gap's reason (for the message).
    """
    if match is None:
        raise ValueError(f"gap without error pattern (would accept any failure): {reason}")
    generic = [g for g in GENERIC_ERRORS if re.search(match, g, re.IGNORECASE | re.DOTALL)]
    if generic:
        raise ValueError(
            f"gap pattern {match!r} is too generic - it also matches {generic[0]!r}: {reason}"
        )


#: Severity of a gap, written as the first word(s) of its reason. A gap with one of these
#: prefixes is no missing feature but a defect that changes data or results or weakens
#: security - the report lists them separately ("Severe gaps"), they never hide among
#: "not supported".
SEVERITIES = {
    "DATA LOSS:": "Data loss",  # written data is not stored (fully or exactly)
    "DATA CORRUPTION:": "Data corruption",  # the table is broken afterwards
    "INCONSISTENT:": "Inconsistency",  # a commit is applied partly / diverges between views
    "WRONG RESULT:": "Wrong result",  # a read or query returns wrong rows/values
    "SECURITY:": "Security",  # access or credentials reach further than intended
}
#: Words in a gap reason that suggest a severe defect: such a reason must carry a severity.
SEVERE_HINT = re.compile(
    r"(?i)\b(silently|lost|loses|duplicat\w*|truncat\w*|corrupt\w*|stale|not atomic|"
    r"old rows|returns NULL|keeps access)\b|as \[\]|-> \[\]"
)


def gap_severity(reason: str | None) -> str | None:
    """Label of the reason's severity prefix (``None``: a limitation, not a defect).

    Args:
        reason: The gap's reason.

    Returns:
        The label (e.g. "Wrong result"), or None.
    """
    for prefix, label in SEVERITIES.items():
        if (reason or "").startswith(prefix):
            return label
    return None


def _require_severity(reason: str) -> None:
    """A reason that sounds like a defect must say so with a severity prefix.

    Args:
        reason: The gap's reason.
    """
    hint = SEVERE_HINT.search(reason)
    if hint and gap_severity(reason) is None:
        raise ValueError(
            f"gap reason sounds severe ({hint.group(0)!r}) but has no severity prefix "
            f"({', '.join(SEVERITIES)}) - classify it: {reason}"
        )


#: Skip reason prefix of a dependent read test whose producing test did not run in this
#: session (e.g. ``-k reader``). Not a result at all: reporting.py does not record it, so a
#: targeted re-run cannot overwrite the real result of the last full run with "blocked".
NOT_RUN = "[producer not run in this session]"


def skip_if_not_produced(results: dict, key: tuple, producer: str) -> tuple:
    """Return ``results[key]`` for a dependent read test, or skip.

    Producers put ``key`` into ``results`` (value None) as their first step, so "absent" means
    the producer did not run here (no result at all), None that it ran without success
    (blocked).

    Args:
        results: The producer's result dict.
        key: Key of the state to read.
        producer: Name of the producing test (for the skip reason).

    Returns:
        The produced value, e.g. ``(namespace, table)``.
    """
    label = " ".join(f"v{k}" if isinstance(k, int) else str(k) for k in key)
    if key not in results:
        pytest.skip(f"{NOT_RUN} {label} ({producer})")
    if results[key] is None:
        pytest.skip(f"{label} did not succeed in {producer}")
    return results[key]


@contextlib.contextmanager
def keep_if_content_ok(results: dict, key: tuple, produced, content_ok):
    """Producer block of a read matrix: book ``produced`` for the dependent reads also when the
    block fails AFTER the table content was right.

    A gap about a property (no delete file written, files left in storage, wrong partition
    values) must not hide whether OTHER engines can read the table. On a failure (not a skip)
    ``content_ok()`` - the writing engine's own read of the table compared with the expected
    rows - decides; an exception there counts as "not ok". Success is booked by the caller.

    Args:
        results: The producer's result dict.
        key: Key of the state.
        produced: Value to book, e.g. ``(namespace, table)``.
        content_ok: Callable: True if the table's content is right.

    Yields:
        Nothing; the producer block runs inside.
    """
    try:
        yield
    except BaseException as exc:
        if not isinstance(exc, pytest.skip.Exception):
            try:
                ok = bool(content_ok())
            except Exception:  # noqa: BLE001 - table missing/unreadable: nothing to read
                ok = False
            if ok:
                results[key] = produced
        raise


@contextlib.contextmanager
def expect_gap(reason: str | None, match: str | None, flaky: bool = False):
    """Run a block that may hit a *documented* gap (an engine/catalog limitation or bug).

    Behaviour:

    * ``reason is None`` (no gap for this case): the block runs normally.
    * the block raises and ``"<ExceptionType>: <message>"`` matches ``match``
      (regex, case-insensitive): the gap reproduced ->
      ``fail`` mode: :class:`KnownGapError` "[known gap] <reason>" chained to the original
      error (shown as FAILED with full traceback); ``xfail`` mode: ``pytest.xfail(reason)``.
    * the block raises anything else: the original exception propagates unchanged - a
      new, unexpected failure.
    * the block does NOT raise although a gap is documented: the gap is fixed ->
      :class:`GapFixedError` "[gap fixed] <reason>" in both modes (remove the entry).
      With ``flaky=True`` the test passes and the summary lists it as "not reproduced".

    ``match`` must name the error precisely: ``None`` or a pattern that also matches one of
    :data:`GENERIC_ERRORS` is refused (ValueError) - that is exactly what lets gaps hide bugs.

    Args:
        reason: Why the gap exists (None: no gap for this case).
        match: Regex the error must match.
        flaky: Intermittent: a passing run is not "fixed".

    Yields:
        Nothing; the checked block runs inside.
    """
    if reason is None:
        yield
        return
    _require_specific(match, reason)
    _require_severity(reason)
    test_id = os.environ.get("PYTEST_CURRENT_TEST", "?").rsplit(" ", 1)[0]
    try:
        yield
    except Exception as exc:  # re-raised below unless it is the expected one
        text = f"{type(exc).__name__}: {exc}"
        if match is not None and not re.search(match, text, re.IGNORECASE | re.DOTALL):
            raise  # a different error than documented -> real, unlabelled failure
        _KNOWN_GAP_HITS.append((test_id, reason))
        if flaky:
            _FLAKY_SEEN.append((test_id, reason))
        if KNOWN_GAPS_MODE == "xfail":
            pytest.xfail(reason)
        raise KnownGapError(f"[known gap] {reason}") from exc
    if flaky:
        _FLAKY_NOT_SEEN.append((test_id, reason))
        return
    _FIXED_GAPS.append((test_id, reason))
    raise GapFixedError(f"[gap fixed] no longer reproduces - remove the KNOWN_GAPS entry: {reason}")


#: Failures that can hit ANY test on the signing warehouse, recognised centrally instead of wrapping
#: every such test in expect_gap. Entry: (nodeid substring, exception regex, frames that
#: must appear in the traceback, reason). Requiring a PyIceberg frame plus s3fs (the only
#: path that showed the problem; Spark's signer never did) keeps an authorization error from
#: anywhere else on demo-signing reported as a new failure. The PyIceberg frame is generic
#: because manifest writes reach s3fs via pyiceberg/avro/file.py, not pyiceberg/io/fsspec.py.
FLAKY_GAPS = [
    (
        "signing",
        r"^PermissionError: (Forbidden|Access Denied)",
        ("site-packages/pyiceberg/", "s3fs/core.py"),
        "PyIceberg race: re-registering the remote signer on a shared s3fs client leaves a window in which requests go out unsigned (RustFS: anonymous -> 403)",
    ),
]


#: Gaps of one catalog that hit EVERY test touching the affected engine (the engine cannot
#: even connect, or the catalog lacks a whole feature), recognised centrally like FLAKY_GAPS:
#: (catalog ("a|b" = either), nodeid substring, exception regex, frames that must appear in
#: the traceback, reason). The nodeid part names the engines/files the gap belongs to
#: ("a|b" = either), so an engine's failure in shared setup cannot be booked as a gap of
#: ANOTHER engine's test (that hid 258 type checks once). Exact messages only - anything else
#: on that catalog is still a new failure.
CATALOG_GAPS = [
    (
        "unity",
        "spark|flink",
        # Spark/Flink resolve "ns.table.snapshots" by first trying table "snapshots" in
        # namespace "ns.table"; they fall back to the metadata table only on NoSuchTable.
        r"NoSuchNamespaceException: Schema not found: lab\.test_\w+\.\w+",
        (),
        (
            "Unity answers a lookup of table 'ns.t.<x>' with NoSuchNamespace instead of "
            "NoSuchTable - Spark and Flink then do not fall back to metadata tables, branches "
            "or tags (t.snapshots, t.branch_x, t.changes)"
        ),
    ),
    (
        "unity",
        "pg_lake|pglake",
        r"pg_lake commits writes through POST /v1/\{prefix\}/transactions/commit",
        (),
        (
            "DATA LOSS: pg_lake commits every write via /transactions/commit; Unity Catalog does not "
            "offer "
            "it (404), pg_lake ignores that and reports success - writes are silently lost"
        ),
    ),
    (
        "rustfs",
        "pyiceberg|polars|daft|dataframes",
        r"missing header: x-amz-content-sha256",
        # Only without help (LAB_NO_HELP=1, test_stock_connection): the lab's workaround
        # (engines._add_content_sha256) is off while PyIceberg/Polars/Daft is under test.
        ("pyiceberg/catalog/rest",),
        (
            "PyIceberg's SigV4 adapter sends x-amz-content-sha256 only for empty bodies; the "
            "RustFS catalog rejects every signed POST without it (AWS S3 Tables does not)"
        ),
    ),
    (
        "rustfs",
        "duckdb",
        r"Could not parse AWS service from host",
        (),
        (
            "DuckDB derives the SigV4 service from an AWS host name - SigV4 catalogs on other "
            "hosts (RustFS) cannot be attached"
        ),
    ),
    (
        "rustfs",
        "v3",  # -v3 parameters and test_v3_* tests
        r"unsupported Iceberg table format-version: 3",
        (),
        "RustFS catalog does not support format v3 (406 UnsupportedOperationException)",
    ),
    (
        "rustfs",
        "v3",  # -v3 parameters and test_v3_* tests
        r"Failed to create transaction",
        (),
        (
            "RustFS catalog does not support format v3 (Trino only reports 'Failed to create "
            "transaction'; the catalog answers 406 unsupported format-version: 3)"
        ),
    ),
    (
        "rustfs",
        "flink|drop_table",  # drop_table: first seen behind the PyIceberg header gap
        r"purgeRequested=true is not supported",
        (),
        "RustFS catalog rejects DROP with purge (purgeRequested=true -> 406)",
    ),
    (
        "rustfs",
        "drop_table",
        r"Failed to drop table|S3Exception: Forbidden \(Service: S3, Status Code: 403",
        (),
        (
            "DROP on the RustFS catalog: purge requests are rejected (406) and clients may not "
            "delete table files themselves (403) - Spark/Trino DROP fails"
        ),
    ),
    (
        "rustfs",
        "flink",
        r"row-level snapshot operation requires a parent snapshot",
        (),
        (
            "RustFS catalog rejects a row-level commit (equality deletes) on a table without "
            "snapshot - Flink's first upsert into a new table"
        ),
    ),
]


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


def _traceback_files(excinfo) -> list[str]:
    """File paths of all frames of an exception, including chained causes/contexts.

    Uses the exception objects, not the formatted report - so the check does not depend on
    ``--tb=line/short/long``.

    Args:
        excinfo: pytest ExceptionInfo.

    Returns:
        ``<file>::<qualname>`` of every frame.
    """
    files, exc, seen = [], excinfo.value, set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        tb = exc.__traceback__
        while tb is not None:
            code = tb.tb_frame.f_code
            files.append(f"{code.co_filename}::{getattr(code, 'co_qualname', code.co_name)}")
            tb = tb.tb_next
        exc = exc.__cause__ or exc.__context__
    return files


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Label failures matching :data:`FLAKY_GAPS` as known gaps (same rules as expect_gap).

    Args:
        item: The test item.
        call: The call phase info.

    Yields:
        Control to the other hooks (hookwrapper).
    """
    outcome = yield
    report = outcome.get_result()
    # "setup" too: an engine that cannot connect may fail in a fixture already.
    if call.when not in ("setup", "call") or not report.failed or call.excinfo is None:
        return
    text = f"{call.excinfo.type.__name__}: {call.excinfo.value}"
    files = _traceback_files(call.excinfo)
    entries = list(FLAKY_GAPS) + [
        (node, regex, frames, reason)
        for cat, node, regex, frames, reason in CATALOG_GAPS
        if PROFILE.name in cat.split("|")
    ]
    for node_part, regex, frames, reason in entries:
        if (
            any(alt in item.nodeid for alt in node_part.split("|"))
            and re.search(regex, text)
            # each frame entry must appear; "a|b" means either a or b
            and all(any(alt in path for alt in f.split("|") for path in files) for f in frames)
        ):
            _KNOWN_GAP_HITS.append((item.nodeid, reason))
            if KNOWN_GAPS_MODE == "xfail":
                report.outcome = "skipped"
                report.wasxfail = reason
            else:
                report.longrepr = f"[known gap] {reason}\n\n{report.longreprtext}"
            return


def pytest_terminal_summary(terminalreporter):
    """Summarise failures split into *new* errors and *known gaps* at the end of the run.

    Args:
        terminalreporter: pytest's terminal reporter.
    """
    # "error" = failed in setup/teardown (e.g. an engine fixture): those count as failures too.
    failed = terminalreporter.stats.get("failed", []) + terminalreporter.stats.get("error", [])
    known_ids = {tid for tid, _ in _KNOWN_GAP_HITS}
    fixed_ids = {tid for tid, _ in _FIXED_GAPS}
    new_errors = [r.nodeid for r in failed if r.nodeid not in known_ids | fixed_ids]
    tr = terminalreporter
    tr.section("iceberg lab: failure overview")
    tr.write_line(f"known-gap mode: {KNOWN_GAPS_MODE}  (set KNOWN_GAPS=xfail for a green run)")
    tr.write_line(f"NEW / unexpected failures: {len(new_errors)}")
    for nodeid in new_errors[:50]:
        tr.write_line(f"  {nodeid}")
    if len(new_errors) > 50:
        tr.write_line(f"  ... and {len(new_errors) - 50} more")
    by_reason: dict[str, int] = {}
    for _, reason in _KNOWN_GAP_HITS:
        by_reason[reason] = by_reason.get(reason, 0) + 1
    tr.write_line(f"known gaps reproduced: {len(_KNOWN_GAP_HITS)} tests, {len(by_reason)} distinct")
    for reason, n in sorted(by_reason.items(), key=lambda kv: -kv[1]):
        tr.write_line(f"  {n:4d} x {reason}")
    if _FIXED_GAPS:
        tr.write_line(
            f"FIXED gaps - documented but no longer failing, remove the entries: {len(_FIXED_GAPS)}"
        )
        for tid, reason in _FIXED_GAPS:
            tr.write_line(f"  {tid}: {reason}")
    if _FLAKY_NOT_SEEN:
        tr.write_line(f"intermittent gaps not reproduced this run: {len(_FLAKY_NOT_SEEN)}")
        for tid, reason in _FLAKY_NOT_SEEN:
            tr.write_line(f"  {tid}: {reason}")
    stale = _update_flaky_history()
    if stale:
        tr.write_line(
            f"PROBABLY FIXED intermittent gaps - not seen in {FLAKY_FIXED_AFTER}+ runs in a row, "
            f"check and remove the entries: {len(stale)}"
        )
        for tid, reason, n in stale:
            tr.write_line(f"  {tid}: {reason} (not seen in {n} runs)")


def _update_flaky_history() -> list[tuple[str, str, int]]:
    """Count, per intermittent gap and test, the runs in a row in which it did not occur.

    Only gaps declared with ``expect_gap(..., flaky=True)`` are tracked (the central
    FLAKY_GAPS have no per-test "did not occur" signal). Returns the entries at or above
    :data:`FLAKY_FIXED_AFTER`.

    Returns:
        ``(test, reason, runs without occurrence)`` at or above the threshold.
    """
    import json

    import reporting

    if not (_FLAKY_SEEN or _FLAKY_NOT_SEEN):
        return []
    path = reporting.REPORT_DIR / "flaky_history.json"
    try:
        history = json.loads(path.read_text())
    except (OSError, ValueError):
        history = {}
    for tid, reason in _FLAKY_SEEN:
        history[f"{tid} :: {reason}"] = 0
    for tid, reason in _FLAKY_NOT_SEEN:
        key = f"{tid} :: {reason}"
        history[key] = history.get(key, 0) + 1
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history, indent=1, ensure_ascii=False, sort_keys=True))
    return [
        (*key.split(" :: ", 1), n)
        for key, n in sorted(history.items())
        if n >= FLAKY_FIXED_AFTER and key.split(" :: ", 1)[0] in {t for t, _ in _FLAKY_NOT_SEEN}
    ]


# =========================================================================== fixtures


@pytest.fixture(scope="session", autouse=True)
def _bootstrap():
    """Make sure bucket, Lakekeeper bootstrap and warehouses exist before any test runs.

    Aborts the whole session (instead of failing hundreds of tests) when Lakekeeper is not
    reachable, which almost always means the tests were started outside the devcontainer.
    """
    host, _, port = SETTINGS.catalog_base.split("//")[1].partition(":")
    if not _reachable(host, int(port or 80)):
        pytest.exit(f"catalog {PROFILE.name} at {SETTINGS.catalog_base} is not reachable", 2)
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
            cat.drop_table(ident)
    try:
        cat.drop_namespace(ns)
    except NamespaceNotEmptyError:
        if PROFILE.name != "rustfs" or cat.list_tables(ns) or cat.list_views(ns):
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
