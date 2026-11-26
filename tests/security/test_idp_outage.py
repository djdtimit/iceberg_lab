"""Token endpoint outage (base test, catalogs with a login through a token; Python and Java REST
clients).

The identity provider fails while the catalog keeps running. A proxy sits in front of the token
endpoint each client uses for the catalog under test (Keycloak for Lakekeeper, the token shim -
which asks Keycloak - for Unity, Polaris' own token endpoint for Polaris, Keycloak for RustFS,
whose token is exchanged at RustFS' STS for temporary SigV4 keys); the catalog itself is reached
directly. Clients: PyIceberg (Python) and Spark (Iceberg Java's REST client).

Not covered: the catalog's own connection to the identity provider (key set for validating
tokens) - the catalogs reach Keycloak directly, not through the proxy.

``running_session``  a session that got its token before the outage keeps working (list, load,
                     read a table) - nothing asks the identity provider again
``new_session``      a session started during the outage fails within 60 s with an error that
                     names the token request - no hang, no misleading "forbidden"
``recovery``         after the outage a new session works again

The tests never stop a container: the outage is the proxy refusing connections.
"""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import replace
from urllib.parse import urlsplit

import pytest
from authz_backends import Conn, backend, pyiceberg_for
from storage_proxy import StorageProxy

from iceberg_lab.config import PROFILE, lab_port
from iceberg_lab.keycloak import KC

from conftest import expect, find_gap, gap_table

PORT = lab_port(19100)
CLIENTS = ["pyiceberg", "spark"]

# Filled from observed results only: (client, case, regex, cause).
# fmt: off
KNOWN_GAPS = gap_table(("client", "case"), [])
# fmt: on


def _gap(client: str, case: str):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        client: Client under test.
        case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, client=client, case=case))


def _proxy(url) -> StorageProxy:
    """TCP proxy on :data:`PORT` in front of the token endpoint's host.

    Args:
        url: Split URL of the token endpoint.

    Returns:
        The proxy.
    """
    port = url.port or 80
    return StorageProxy(port=PORT, upstream=(url.hostname, port), host=f"{url.hostname}:{port}")


@pytest.fixture(scope="module")
def idp():
    """(backend, namespace with t_select readable by alice, login of alice via the proxy, proxy).

    The login is called once per session: with a bearer token the client itself asks the token
    endpoint (the connection names the proxy); on RustFS the session starts with the Keycloak
    token (through the proxy) and its exchange at RustFS' STS.

    Yields:
        ``(backend, namespace, login () -> Conn, proxy)``.
    """
    b = backend()
    ns = b.setup_namespace()
    if PROFILE.server == "rustfs":
        url = urlsplit(KC)
        proxy = _proxy(url)
        via = f"http://127.0.0.1:{PORT}"
        login = lambda: b.web_identity_conn("rustfs-alice", kc=via)
    else:
        direct = b.conn("alice")
        if direct.token_url is None:
            pytest.skip(f"catalog {PROFILE.name} has no API for logging in with a token")
        url = urlsplit(direct.token_url)
        proxy = _proxy(url)
        conn = replace(direct, token_url=f"http://127.0.0.1:{PORT}{url.path}")
        login = lambda: conn
    yield b, ns, login, proxy
    proxy.close()
    b.drop_namespace(ns)


class _Spark:
    """A new Spark catalog per call (Spark initialises a catalog on first use: own token)."""

    def __init__(self, spark, conn: Conn):
        """Register a new Spark catalog with the connection's properties.

        Args:
            spark: The session's SparkSession.
            conn: Connection of the identity (``authz_backends.Conn``).
        """
        self.spark, self.name = spark, f"idp_{uuid.uuid4().hex[:6]}"
        prefix = f"spark.sql.catalog.{self.name}"
        spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"{prefix}.type", "rest")
        for k, v in conn.java_props().items():
            spark.conf.set(f"{prefix}.{k}", v)

    def reads(self, ns: str) -> int:
        """Row count of t_select through this catalog.

        Args:
            ns: Namespace.

        Returns:
            The count.
        """
        return self.spark.sql(f"SELECT count(*) FROM {self.name}.{ns}.t_select").collect()[0][0]


def _session(client: str, conn: Conn, request):
    """A session of the client on the connection.

    Args:
        client: Client under test.
        conn: Connection of the identity (``authz_backends.Conn``).
        request: pytest request of the calling test or fixture.

    Returns:
        ``read(namespace) -> row count``.
    """
    if client == "pyiceberg":
        cat = pyiceberg_for(conn)
        return lambda ns: cat.load_table(f"{ns}.t_select").scan().to_arrow().num_rows
    s = _Spark(request.getfixturevalue("spark"), conn)
    return s.reads


@pytest.mark.parametrize("client", CLIENTS)
def test_running_session(client, idp, request):
    """A session that got its token before the outage keeps working.

    Checks:
        * with the token endpoint unreachable, the session started before still reads t_select
          with all its rows.

    Args:
        client: Client under test (PyIceberg or Spark).
        idp: ``(backend, namespace, alice's login via the proxy, proxy)`` - a TCP proxy in
            front of the catalog's token endpoint that the test can switch off (the outage).
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    _, ns, login, proxy = idp
    read = _session(client, login(), request)
    expected = read(ns)  # token fetched now, through the proxy
    proxy.arm("down")
    try:
        with _gap(client, "running_session"):
            assert read(ns) == expected, "the running session lost access"
    finally:
        proxy.disarm()


@pytest.mark.parametrize("client", CLIENTS)
def test_new_session(client, idp, request):
    """A session started during the outage fails fast and recognisably.

    Checks:
        * the new session fails;
        * it fails in under 60 s (no endless retries);
        * it did try the token endpoint (proxy hits) - the failure comes from the outage;
        * the error does not claim a missing permission (forbidden / access denied) - it must
          not look like a rights problem.

    Args:
        client: Client under test (PyIceberg or Spark).
        idp: ``(backend, namespace, alice's login via the proxy, proxy)`` - a TCP proxy in
            front of the catalog's token endpoint that the test can switch off (the outage).
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    _, ns, login, proxy = idp
    proxy.arm("down")
    started = time.monotonic()
    try:
        with _gap(client, "new_session"):
            with pytest.raises(Exception) as exc:
                _session(client, login(), request)(ns)
            took = time.monotonic() - started
            text = f"{type(exc.value).__name__}: {exc.value}"
            assert took < 60, f"the failing token request took {took:.0f} s"
            assert proxy.hits, f"the client never asked the token endpoint: {text[:300]}"
            assert not re.search(r"(?i)forbidden|permission|access denied", text), (
                f"the outage looks like a permission problem: {text[:300]}"
            )
    finally:
        proxy.disarm()


@pytest.mark.parametrize("client", CLIENTS)
def test_recovery(client, idp, request):
    """After the outage ends, new sessions work again.

    Checks:
        * a session started after the proxy is back reads t_select with all its rows.

    Args:
        client: Client under test (PyIceberg or Spark).
        idp: ``(backend, namespace, alice's login via the proxy, proxy)`` - a TCP proxy in
            front of the catalog's token endpoint that the test can switch off (the outage).
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    b, ns, login, proxy = idp
    proxy.arm("down")
    proxy.disarm()
    with _gap(client, "recovery"):
        assert _session(client, login(), request)(ns) == b.rows(ns, "t_select")
