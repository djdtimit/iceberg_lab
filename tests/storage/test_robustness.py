"""Robustness against catalog faults: lost commit responses, transient 503, catalog down.

A small fault-injecting HTTP proxy runs inside the test process (port 18181, reachable as
``dev:18181`` from the Trino/Flink/pg_lake containers) and forwards to the catalog under test
(every catalog; SigV4 requests to RustFS keep their headers unchanged, so the signature
stays valid). Every engine talks to
the catalog through it (Trino via catalog ``<prefix>_proxy``, Spark via a runtime catalog
``proxy``, pg_lake via its own proxy server). The catalog's URLs in responses (``/v1/config``
overrides ``uri``) are rewritten to the proxy.

Scenarios, each for every engine's own append path (setup through the proxy, the fault is
armed only for the one commit under test):

``lost_response``  the catalog APPLIES the commit, the proxy drops the response. The client
                   cannot know the outcome. Acceptable: an error (row present once - the
                   commit did happen), or success with the row exactly once. Not acceptable:
                   the row twice (a blind retry re-applied the append), or success without
                   the row. A catalog that supports Idempotency-Key (``idempotency-key-lifetime``
                   in ``/v1/config``, e.g. Lakekeeper) lets a client sending one retry safely.
``transient_503``  the first commit attempt gets ``503`` without reaching the catalog.
                   Acceptable: success with the row exactly once (retried), or an error
                   with the table unchanged.
``catalog_down``   every request is refused. Acceptable: an error within 120 s, or the
                   correct content (Spark's table cache and pg_lake's own metadata in Postgres
                   can still read). Not acceptable: hanging, or wrong content.

After every fault the table's integrity is checked first: every file the current snapshot
references (manifest list, manifests) must exist in RustFS.
"""

from __future__ import annotations

import dataclasses
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pytest
import requests
import test_operations as base

from iceberg_lab import engines
from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSE_STS
from iceberg_lab.engines import pyiceberg_catalog

from conftest import ENGINES, engine_param, expect_gap, require_engine

WAREHOUSE = WAREHOUSE_STS
PORT = 18181
UPSTREAM = SETTINGS.catalog_base  # e.g. http://lakekeeper:8181
COMMIT = re.compile(
    # The prefix may contain slashes (Unity: "catalogs/lab").
    rf"^{re.escape(PROFILE.path)}/v1/(?:.+/)?(namespaces/[^/]+/tables/[^/?]+|transactions/commit)$"
)
SCENARIOS = ["lost_response", "transient_503", "catalog_down"]
#: Errors that plausibly mean "catalog unreachable" (connection refused/reset, no response).
CATALOG_GONE = (
    r"(?i)(connect|connection|refused|reset|closed|unreachable|timed? ?out|remote end|"
    r"max retries|NoHttpResponse|failed to respond|EOF|broken pipe|catalog|metadata|"
    r"RemoteDisconnected|ProtocolError|IO ?Error|HTTP)"
)


class Proxy:
    """Forwarding proxy with one-shot faults on commit requests (and a "down" mode)."""

    def __init__(self):
        #: None | "lost_response" | "transient_503" | "down" | "fail_table" (see ``target``)
        """Start the proxy on port 18181 in a background thread."""
        self.mode: str | None = None
        #: fail_table: table name whose commits (also inside /transactions/commit) get a 503
        self.target: str | None = None
        #: path of every commit request since the last arm() (shows per-table vs. multi-table)
        self.commit_paths: list[str] = []
        self.fired = 0
        self.commits = 0
        self.lock = threading.Lock()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            """Forward to Lakekeeper; apply the armed fault to the next commit."""

            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # silence
                """Silence the request log.

                Args:
                    args: Log arguments (ignored).
                """

            def _handle(self):
                """Forward one request (or fail it per mode), rewrite Lakekeeper URLs to the proxy."""
                if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
                    body = b""  # Java HTTP clients may stream the request body in chunks
                    while (size := int(self.rfile.readline().strip() or b"0", 16)) > 0:
                        body += self.rfile.read(size)
                        self.rfile.readline()
                    self.rfile.readline()
                else:
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length else None
                path = self.path.split("?", 1)[0]
                is_commit = self.command == "POST" and bool(COMMIT.match(path))
                with proxy.lock:
                    mode = proxy.mode
                    if is_commit:
                        proxy.commits += 1
                        proxy.commit_paths.append(path)
                    fire = (mode == "down") or (
                        is_commit and mode in ("lost_response", "transient_503") and not proxy.fired
                    )
                    if is_commit and mode == "fail_table" and proxy.target:
                        names_target = path.endswith(f"/tables/{proxy.target}") or (
                            path.endswith("/transactions/commit")
                            and f'"{proxy.target}"'.encode() in (body or b"")
                        )
                        fire = names_target
                        mode = "transient_503" if names_target else mode
                    if fire and mode != "down":
                        proxy.fired += 1
                if fire and mode == "down":
                    self.close_connection = True
                    self.connection.shutdown(2)
                    return
                if fire and mode == "transient_503":
                    msg = b'{"error":{"message":"injected","type":"ServiceUnavailable","code":503}}'
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(msg)))
                    self.end_headers()
                    self.wfile.write(msg)
                    return
                headers = {
                    k: v
                    for k, v in self.headers.items()
                    if k.lower()
                    not in (
                        "host",
                        "content-length",
                        "transfer-encoding",
                        "expect",
                        "accept-encoding",
                    )
                }
                if PROFILE.auth == "sigv4":
                    # SigV4 signs Host and the other headers as the client sent them: forward
                    # them unchanged, or the catalog rejects the signature.
                    headers["Host"] = self.headers.get("Host", "")
                    if self.headers.get("Accept-Encoding"):
                        headers["Accept-Encoding"] = self.headers["Accept-Encoding"]
                else:
                    # Uncompressed upstream answers: Java clients accept zstd, which requests
                    # cannot decode, and the proxy rewrites URLs in the body.
                    headers["Accept-Encoding"] = "identity"
                r = requests.request(
                    self.command, UPSTREAM + self.path, data=body, headers=headers, timeout=120
                )
                if fire and mode == "lost_response":  # applied upstream, answer lost
                    self.close_connection = True
                    self.connection.shutdown(2)
                    return
                content = r.content.replace(
                    UPSTREAM.encode(), f"http://{proxy.host}:{PORT}".encode()
                )
                self.send_response(r.status_code)
                for k, v in r.headers.items():
                    if k.lower() not in (
                        "content-length",
                        "transfer-encoding",
                        "content-encoding",
                        "connection",
                        "date",
                        "server",
                    ):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            do_GET = do_POST = do_DELETE = do_HEAD = do_PUT = _handle

        self.host = "dev"
        self.server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def arm(self, mode, target=None):
        """Arm a one-shot fault (or a permanent one for ``down``).

        Args:
            mode: Fault mode (lost_response, transient_503, down, fail_table).
            target: Table whose commit is affected (fail_table).
        """
        with self.lock:
            self.mode, self.fired, self.target = mode, 0, target
            self.commit_paths = []

    def close(self):
        """Stop the proxy."""
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture(scope="module")
def proxy():
    """The fault proxy for the module.

    Yields:
        The proxy.
    """
    p = Proxy()
    yield p
    p.close()


PROXY_SETTINGS = dataclasses.replace(SETTINGS, catalog_base=f"http://dev:{PORT}")


def proxied_engine(engine, request):
    """An adapter for ``engine`` whose catalog traffic goes through the proxy.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        request: pytest request of the calling test or fixture.

    Returns:
        The adapter (closed afterwards).
    """
    require_engine(engine)
    s = PROXY_SETTINGS
    if engine in ("pyiceberg", "polars", "daft"):
        return _PyCat(s)
    if engine == "duckdb":
        return engines.DuckDBEngine(WAREHOUSE, s=s)
    if engine == "flink":
        return engines.FlinkEngine(WAREHOUSE, s=s)
    if engine == "pg_lake":
        saved = dict(engines.PGLAKE_SERVERS)
        engines.PGLAKE_SERVERS[WAREHOUSE] = f"{PROFILE.trino_prefix}_proxy"
        try:
            return engines.PgLakeEngine(WAREHOUSE, s=s)
        finally:
            engines.PGLAKE_SERVERS.clear()
            engines.PGLAKE_SERVERS.update(saved)
    if engine == "trino":
        import trino

        e = engines.TrinoEngine(WAREHOUSE)
        e.catalog = f"{PROFILE.trino_prefix}_proxy"
        e.conn = trino.dbapi.connect(
            host=SETTINGS.trino_host, port=SETTINGS.trino_port, user="tests", catalog=e.catalog
        )
        return e
    if engine == "spark":
        spark = request.getfixturevalue("spark")
        src = f"spark.sql.catalog.{engines.SPARK_CATALOGS[WAREHOUSE]}"
        for k, v in spark.sparkContext.getConf().getAll():
            if k == src or k.startswith(src + "."):
                spark.conf.set(k.replace(src, "spark.sql.catalog.proxy", 1), v)
        spark.conf.set("spark.sql.catalog.proxy.uri", PROXY_SETTINGS.catalog_uri)
        e = engines.SparkEngine(spark, WAREHOUSE)
        e.catalog = "proxy"
        return e
    raise ValueError(engine)


class _PyCat:
    """Holder for a proxied PyIceberg catalog (PyIceberg, Polars and Daft write through it)."""

    dialect = None

    def __init__(self, s):
        """Open PyIceberg with the proxied settings.

        Args:
            s: Settings pointing at the proxy.
        """
        self.catalog = engines.pyiceberg_catalog(WAREHOUSE, s=s)

    def close(self):
        """Nothing to close."""


def setup_table(engine, adapter, ns, table):
    """Create the table and write SEED through the proxy, with the engine itself.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace of the table.
        table: Table name (without namespace).
    """
    if engine == "pg_lake":
        adapter.execute(f'CREATE SCHEMA IF NOT EXISTS "{ns}"')
        adapter.execute(
            f"CREATE TABLE {adapter.fq(ns, table)} (id integer, name text, amount double precision) "
            f"USING iceberg WITH (catalog = '{adapter.server}')"
        )
        adapter.owned.add((ns, table))
    else:
        pyiceberg_catalog(WAREHOUSE).create_table(f"{ns}.{table}", schema=base.SCHEMA)
    append(engine, adapter, ns, table, base.SEED)


def append(engine, adapter, ns, table, rows):
    """Append rows with the engine's own write path (through the proxy).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace of the table.
        table: Table name (without namespace).
        rows: Rows to append.
    """
    if engine in ("pyiceberg", "polars", "daft"):
        t = adapter.catalog.load_table(f"{ns}.{table}")
        data = base.arrow(rows)
        if engine == "pyiceberg":
            t.append(data)
        elif engine == "polars":
            import polars as pl

            pl.from_arrow(data).write_iceberg(t, mode="append")
        else:
            import daft

            daft.from_arrow(data).write_iceberg(t, mode="append")
        return
    values = ", ".join(f"({i}, '{n}', {a!r}E0)" for i, n, a in rows)
    adapter.execute(f"INSERT INTO {adapter.fq(ns, table)} VALUES {values}")


def assert_intact(ns, table):
    """The current snapshot's manifest list and manifests must exist in storage.

    Args:
        ns: Namespace of the table.
        table: Table name (without namespace).
    """
    import test_maintenance as mt

    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
    stored = mt.storage_files(t.location())
    ml = mt._norm_uri(t.current_snapshot().manifest_list)
    assert ml in stored, f"TABLE CORRUPTED: manifest list of the current snapshot is missing ({ml})"
    missing = {mt._norm_uri(m.manifest_path) for m in t.current_snapshot().manifests(t.io)} - stored
    assert not missing, f"TABLE CORRUPTED: manifests missing: {sorted(missing)[:2]}"


def direct_rows(ns, table):
    """Table content read directly from Lakekeeper (not through the proxy).

    Args:
        ns: Namespace of the table.
        table: Table name (without namespace).

    Returns:
        Sorted rows.
    """
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
    return sorted(base.arrow_rows(t.scan().to_arrow()))


# Filled from observed failures only: (engine, scenario, regex, reason, flaky).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str, bool]] = [
    # Reproduced 3/3: DuckDB treats the unknown commit outcome as a failure and deletes the
    # commit's files - but Lakekeeper had applied the commit, so the table points at them.
    ("duckdb", "lost_response", r"TABLE CORRUPTED: manifest list of the current snapshot is missing", "DATA CORRUPTION: after a lost commit response DuckDB deletes the files of the commit Lakekeeper already applied (current snapshot -> missing manifest list)", False),
]
# fmt: on


def _gap(engine, scenario):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        scenario: Fault scenario.

    Returns:
        The context manager.
    """
    for e, sc, regex, reason, flaky in KNOWN_GAPS:
        if e == engine and sc == scenario:
            return expect_gap(reason, regex, flaky=flaky)
    return expect_gap(None, None)


@pytest.mark.slow
@pytest.mark.parametrize("scenario", SCENARIOS)
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_catalog_fault(engine, scenario, namespace, proxy, request):
    """The engine's append under a catalog fault (proxy in front of the catalog).

    After every fault the table's integrity is checked first: every file the current snapshot
    references exists.

    Checks per scenario:
        * lost_response (commit applied, response dropped): the fault fired; the row is in the
          table at most once (a blind retry must not append it twice); a reported success
          needs the row to be there;
        * transient_503: success with the row exactly once, or an error with the table
          unchanged;
        * catalog_down: an error within 120 s that names the unreachable catalog, or the
          correct content - never hanging, never wrong content.

    Args:
        engine: Engine under test.
        scenario: ``lost_response``, ``transient_503`` or ``catalog_down``.
        namespace: Fresh namespace of the test session.
        proxy: Fault-injecting HTTP proxy in front of the catalog (port 18181).
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    table = f"rb_{engine}_{scenario}_{uuid.uuid4().hex[:6]}"
    row = (99, "fault", 9.0)
    adapter = None
    proxy.arm(None)
    try:
        with _gap(engine, scenario):
            adapter = proxied_engine(engine, request)
            setup_table(engine, adapter, namespace, table)
            if scenario == "catalog_down":
                proxy.arm("down")
                start = time.monotonic()
                try:
                    if engine in ("pyiceberg", "polars", "daft"):
                        t = adapter.catalog.load_table(f"{namespace}.{table}")
                        n = t.scan().to_arrow().num_rows
                    else:
                        fq = adapter.fq(namespace, table)
                        n = adapter.execute(f"SELECT count(*) FROM {fq}")[0][0]
                except Exception as exc:  # noqa: BLE001 - failing cleanly is acceptable ...
                    # ... but only for a reason that has to do with the missing catalog
                    assert re.search(CATALOG_GONE, f"{type(exc).__name__}: {exc}"), (
                        f"unexpected error while catalog down: {type(exc).__name__}: {exc}"
                    )
                    n = None
                took = time.monotonic() - start
                assert took < 120, f"took {took:.0f}s while the catalog was down"
                assert n in (None, len(base.SEED)), f"wrong content while catalog down: {n} rows"
                return
            proxy.arm(scenario)
            error = None
            try:
                append(engine, adapter, namespace, table, [row])
            except Exception as exc:  # noqa: BLE001 - the outcome is checked below
                error = exc
            fired = proxy.fired
            proxy.arm(None)
            assert fired == 1, "fault was not triggered"
            assert_intact(namespace, table)
            rows = direct_rows(namespace, table)
            n = rows.count(row)
            assert n <= 1, f"row appended {n} times (client error: {error!r})"
            if error is None:
                assert n == 1, "client reported success but the row is missing"
            elif scenario == "transient_503":
                assert n == 0, "client reported an error but the row was committed"
            assert sorted(r for r in rows if r != row) == base.SEED
    finally:
        proxy.arm(None)
        if adapter is not None and engine not in ("spark",):
            adapter.close()


@pytest.mark.slow
def test_proxy_is_transparent(proxy, namespace):
    """Sanity check: without a fault the proxy changes nothing.

    Checks:
        * create, append and read through the proxy give ids 1, 2;
        * the proxy saw at least one commit.

    Args:
        proxy: Fault-injecting HTTP proxy in front of the catalog (port 18181).
        namespace: Fresh namespace of the test session.
    """
    proxy.arm(None)
    cat = engines.pyiceberg_catalog(WAREHOUSE, s=PROXY_SETTINGS)
    name = f"rb_ok_{uuid.uuid4().hex[:6]}"
    t = cat.create_table(f"{namespace}.{name}", schema=pa.schema([("id", pa.int64())]))
    t.append(pa.table({"id": [1, 2]}))
    assert sorted(t.refresh().scan().to_arrow()["id"].to_pylist()) == [1, 2]
    assert proxy.commits >= 1
