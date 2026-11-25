"""Web UI of the catalog (base, every catalog; no click tests).

The official UI of each project, whether the server serves it or the project ships it as a
separate container (third-party UIs do not count):

=============  ================================================  ===============================
catalog        UI (lab address)                                  data API the UI reads
=============  ================================================  ===============================
Lakekeeper     served by the server: lakekeeper-authz:8181/ui/   /management/v1 (same server)
Polaris        Polaris Console, own container (polaris-tools,    Polaris' management API - the
               built by scripts/build_polaris_console.sh):       browser calls Polaris directly
               polaris-console:8080                              (URL in the console's config.js)
RustFS         console served by RustFS: rustfs:9001/rustfs/     signed admin/S3 calls (RustFS)
               console/
Unity          unitycatalog-ui, own container (React dev         /api/2.1/unity-catalog/...
               server): unity-ui:3000                            through the UI's own proxy to
                                                                 unitycatalog-authz
Gravitino      served by the server: gravitino:8090/ (login      /api/metalakes/lab/... (same
               settings from /configs)                           server)
=============  ================================================  ===============================

Authorization on: Lakekeeper and Unity point their UI at the instance with authorization on
(the main instances run open on purpose); Polaris, RustFS and Gravitino always check.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest
import requests

from iceberg_lab.config import PROFILE, SETTINGS

from conftest import expect, find_gap, gap_table

# Filled from observed failures only: (catalog, case, regex, cause).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "case"), [
])
# fmt: on


def _gap(case):
    """``expect`` for a documented gap of this catalog's UI.

    Args:
        case: Case name.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, case=case))


@dataclass
class UI:
    """Where a catalog's UI lives and how to reach the API it reads its data from.

    Attributes:
        page: The UI's entry page.
        data: An API URL the UI reads catalog data from.
        auth: Returns request kwargs (headers) with valid admin credentials - positive control.
        headers: Headers every request to the UI carries (Unity's dev server accepts only the
            Host a browser on the lab host sends).
        wired_to: ``(config URL, regex)``: the UI's runtime configuration must match the regex -
            proof that the UI talks to the lab's catalog (Polaris Console), else ``None``.
    """

    page: str
    data: str
    auth: Callable[[], dict]
    headers: dict = field(default_factory=dict)
    wired_to: tuple[str, str] | None = None
    #: The UI's runtime configuration, which overrides the defaults built into its scripts
    #: (Polaris Console's config.js): the browser check reads only this file then.
    runtime_config: str | None = None
    #: Request kwargs with a token got the way the UI's login in the host's browser gets it
    #: (identity provider under its host address) - for UIs that log in through one.
    browser_login: Callable[[], dict] | None = None


def _lakekeeper_admin() -> dict:
    """Lakekeeper admin headers.

    Returns:
        Request kwargs.
    """
    from authz_backends import LakekeeperAuthz

    return {"headers": LakekeeperAuthz()._h()}


def _lakekeeper_browser_login() -> dict:
    """Token of the UI user as the UI's login gets it: Keycloak as ``localhost:30080``.

    Returns:
        Request kwargs.
    """
    from authz_backends import LakekeeperAuthz

    from iceberg_lab import keycloak as kc

    backend = LakekeeperAuthz()
    backend.ensure()
    backend.ensure_ui_user()
    return {"headers": {"Authorization": f"Bearer {kc.ui_user_token(browser=True)}"}}


def _polaris_admin() -> dict:
    """Polaris root headers.

    Returns:
        Request kwargs.
    """
    from authz_backends import PolarisAuthz

    return {"headers": PolarisAuthz()._h()}


def _unity_admin() -> dict:
    """Unity admin headers plus the browser Host.

    Returns:
        Request kwargs.
    """
    from authz_backends import UnityAuthz

    return {"headers": {**UnityAuthz()._h(), **UNITY_BROWSER}}


def _rustfs_admin() -> dict:
    """SigV4 headers of the admin keys for the GET of the data URL (botocore signs).

    Returns:
        Request kwargs.
    """
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    # RustFS requires x-amz-content-sha256 on every signed request (as AWS S3 does).
    empty = hashlib.sha256(b"").hexdigest()
    req = AWSRequest(method="GET", url=UIS["rustfs"].data, headers={"x-amz-content-sha256": empty})
    creds = Credentials(SETTINGS.s3_access_key, SETTINGS.s3_secret_key)
    SigV4Auth(creds, "s3", SETTINGS.s3_region).add_auth(req)
    return {"headers": dict(req.headers)}


def _gravitino_admin() -> dict:
    """Gravitino service-admin headers (Keycloak token of lab-admin, scope gravitino).

    Returns:
        Request kwargs.
    """
    from authz_backends import GravitinoAuthz

    return {"headers": GravitinoAuthz()._h()}


def _gravitino_browser_login() -> dict:
    """The UI's own login: user ``lab-ui`` through client ``gravitino-ui``, asked as the host's
    browser asks - as ``keycloak:8080`` through keycloak-browser (the issuer Gravitino accepts).

    Returns:
        Request kwargs.
    """
    from iceberg_lab import keycloak as kc
    from iceberg_lab.bootstrap import GRAVITINO_ACCEPT

    return {
        "headers": {
            "Authorization": f"Bearer {kc.gravitino_ui_user_token()}",
            "Accept": GRAVITINO_ACCEPT,
        }
    }


#: The Host a browser on the lab host sends (unity-ui is published as localhost:3000). The
#: dev server's host check stays on; the test behaves like that browser.
UNITY_BROWSER = {"Host": "localhost:3000"}

UIS = {
    "lakekeeper": UI(
        "http://lakekeeper-authz:8181/ui/",
        "http://lakekeeper-authz:8181/management/v1/warehouse",
        _lakekeeper_admin,
        browser_login=_lakekeeper_browser_login,
    ),
    "polaris": UI(
        "http://polaris-console:8080/",
        "http://polaris:8181/api/management/v1/catalogs",
        _polaris_admin,
        # The console runs in the host's browser: Polaris as published on the host (8183).
        wired_to=(
            "http://polaris-console:8080/config.js",
            r"VITE_POLARIS_API_URL: 'http://localhost:8183'",
        ),
        runtime_config="http://polaris-console:8080/config.js",
    ),
    "rustfs": UI(
        f"{SETTINGS.s3_endpoint.replace(':9000', ':9001')}/rustfs/console/",
        f"{SETTINGS.s3_endpoint}/rustfs/admin/v3/info",
        _rustfs_admin,
    ),
    "gravitino": UI(
        "http://gravitino:8090/",
        "http://gravitino:8090/api/metalakes/lab/catalogs",
        _gravitino_admin,
        # The UI reads its login settings (OIDC authority) from the server at runtime.
        runtime_config="http://gravitino:8090/configs",
        browser_login=_gravitino_browser_login,
    ),
    "unity": UI(
        "http://unity-ui:3000/",
        "http://unity-ui:3000/api/2.1/unity-catalog/catalogs",
        _unity_admin,
        headers=UNITY_BROWSER,
    ),
}


def test_catalog_has_ui():
    """The catalog's official web UI is served and belongs to this lab's catalog.

    Checks:
        * the entry page answers 200 with an HTML document (``text/html`` and an ``<html``
          element) - not an error page, not a JSON API answer;
        * where the UI is configured at runtime (Polaris Console's ``config.js``), it points
          to the lab's Polaris - a UI wired to another catalog would prove nothing.
    """
    if PROFILE.server not in UIS:
        pytest.skip(f"catalog {PROFILE.name} has no API for a web UI")
    ui = UIS[PROFILE.server]
    r = requests.get(ui.page, headers=ui.headers, timeout=10)
    assert r.ok, f"{ui.page}: {r.status_code} {r.text[:200]}"
    assert "text/html" in r.headers.get("content-type", "") and "<html" in r.text.lower(), (
        f"{ui.page} is no HTML page: {r.headers.get('content-type')} {r.text[:200]}"
    )
    if ui.wired_to:
        url, pattern = ui.wired_to
        config = requests.get(url, timeout=10).text
        assert re.search(pattern, config), f"UI not wired to the lab's catalog: {config[:300]}"


def test_ui_data_needs_login():
    """The API the UI reads its data from refuses anonymous requests, and works with login.

    Checks:
        * an anonymous GET of the data API answers 401 or 403 - a 200 (or an HTML fallback
          page) would mean anyone who reaches the UI sees the catalog's contents;
        * the same GET with admin credentials answers 2xx (positive control: the endpoint
          exists and works, so the refusal above is a refusal and not a broken URL).
    """
    if PROFILE.server not in UIS:
        pytest.skip(f"catalog {PROFILE.name} has no API for a web UI")
    ui = UIS[PROFILE.server]
    anonymous = requests.get(ui.data, headers=ui.headers, timeout=10)
    assert anonymous.status_code in (401, 403), (
        f"the UI's data API answers an anonymous request with {anonymous.status_code}: "
        f"{anonymous.text[:200]}"
    )
    signed_in = requests.get(ui.data, timeout=10, **ui.auth())
    assert signed_in.ok, (
        f"positive control: with credentials {signed_in.status_code} {signed_in.text[:200]}"
    )


# ---- the UI in the host's browser -------------------------------------------------------------

#: Absolute URLs in what a UI ships to the browser.
_URL = re.compile(r"""https?://([A-Za-z0-9.-]+)(?::(\d+))?([^\s'"`)<>\\]*)""")
#: webpack-dev-server's client entry names the port its live-reload socket connects to.
_DEV_SOCKET = re.compile(r"webpack-dev-server/client/index\.js\?[^\"'\s]*?[?&]port=(\d+)")


def _services() -> dict:
    """The compose services.

    Returns:
        ``{service: definition}`` from ``.devcontainer/docker-compose.yml``.
    """
    import yaml

    from iceberg_lab.lab.manifest import COMPOSE

    return yaml.safe_load(COMPOSE.read_text())["services"]


def _internal_hosts(services: dict) -> set[str]:
    """Host names only the compose network resolves.

    Args:
        services: The compose services.

    Returns:
        Service names and network aliases without a dot (the AWS-like aliases with dots are
        real names a browser resolves - to AWS).
    """
    hosts = set(services)
    for svc in services.values():
        nets = svc.get("networks")
        for net in (nets or {}).values() if isinstance(nets, dict) else ():
            hosts.update(a for a in (net or {}).get("aliases", ()) if "." not in a)
    return hosts


def _published(services: dict) -> dict[int, tuple[str, int]]:
    """Ports published on the host.

    Args:
        services: The compose services.

    Returns:
        ``{host port: (service, container port)}``.
    """
    out = {}
    for name, svc in services.items():
        for spec in svc.get("ports", ()):
            *bind, host, container = str(spec).split(":")
            if bind and bind[0] not in ("127.0.0.1", "0.0.0.0"):
                continue  # another loopback address: a name the host maps (HOST_MAPPED)
            out[int(host)] = (name, int(container))
    return out


#: Compose names the lab host resolves itself (/etc/hosts, docs/setup.md): name:port -> the
#: forwarder service that publishes it on that loopback address. Gravitino accepts one token
#: issuer, http://keycloak:8080/...: its UI must send the browser to exactly that address.
HOST_MAPPED = {"keycloak:8080": ("keycloak-browser", "127.0.0.2")}


def _host_mapped(services: dict) -> set[str]:
    """``name:port`` the host resolves through a published forwarder (:data:`HOST_MAPPED`).

    Args:
        services: The compose services.

    Returns:
        The addresses whose forwarder is published on its loopback address and port.
    """
    out = set()
    for addr, (svc, ip) in HOST_MAPPED.items():
        port = addr.split(":")[1]
        if f"{ip}:{port}:{port}" in [str(s) for s in services.get(svc, {}).get("ports", ())]:
            out.add(addr)
    return out


def _shipped(page: str, headers: dict, runtime_config: str | None) -> str:
    """What the UI hands the browser that decides where it connects to.

    Args:
        page: The UI's entry page (compose address).
        headers: Headers of a browser on the host.
        runtime_config: The UI's runtime configuration, if it has one.

    Returns:
        The runtime configuration, else the page plus every script and module preload it
        references on its own server.
    """
    if runtime_config:
        return requests.get(runtime_config, headers=headers, timeout=10).text
    html = requests.get(page, headers=headers, timeout=10).text
    refs = re.findall(r'<script[^>]*\ssrc="([^"]+)"', html)
    refs += re.findall(r'<link[^>]*rel="modulepreload"[^>]*href="([^"]+)"', html)
    origin = re.match(r"https?://[^/]+", page).group(0)
    texts = [html]
    for ref in dict.fromkeys(refs):
        if ref.startswith(("http://", "https://")):
            continue  # another origin (a CDN): not the UI's own wiring
        url = origin + ref if ref.startswith("/") else page.rstrip("/") + "/" + ref
        texts.append(requests.get(url, headers=headers, timeout=30).text)
    return "\n".join(texts)


def _cors_problems(found: set, origin: str, host_port: int, published: dict) -> list[str]:
    """APIs on another host port whose CORS preflight refuses the UI's origin.

    Args:
        found: ``(host, port, path)`` of the absolute URLs the UI uses.
        origin: The UI's origin in the host's browser.
        host_port: The UI's host port.
        published: ``{host port: (service, container port)}``.

    Returns:
        One line per refused or unpublished API.
    """
    problems = []
    for h, p, path in sorted(found, key=str):
        if h not in ("localhost", "127.0.0.1") or p in (None, host_port):
            continue
        if p not in published:
            problems.append(f"{origin}: calls {h}:{p}, which nothing publishes")
            continue
        svc, cport = published[p]
        r = requests.options(
            f"http://{svc}:{cport}{path or '/'}",
            headers={
                "Origin": origin,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization,content-type",
            },
            timeout=10,
        )
        allowed = r.headers.get("access-control-allow-origin")
        if allowed not in (origin, "*"):
            problems.append(
                f"{origin}: CORS refuses it for {h}:{p}{path} (preflight {r.status_code}, "
                f"Access-Control-Allow-Origin {allowed!r})"
            )
    return problems


def _dev_socket_problems(services: dict, image: str) -> list[str]:
    """Services running a webpack dev server image whose live-reload socket misses its host port.

    Args:
        services: The compose services.
        image: The dev server's image.

    Returns:
        One line per service published on another host port without ``WDS_SOCKET_PORT``
        set to that port.
    """
    problems = []
    for name, svc in services.items():
        if svc.get("image") != image:
            continue
        env = svc.get("environment") or {}
        for spec in svc.get("ports", ()):
            *_, host, container = str(spec).split(":")
            if host != container and str(env.get("WDS_SOCKET_PORT")) != host:
                problems.append(
                    f"{name}: published on {host} (container {container}) but its live-reload "
                    f"socket uses {env.get('WDS_SOCKET_PORT', container)}"
                )
    return problems


def test_ui_works_in_the_host_browser():
    """The UI can reach its data from a browser on the lab host.

    The UI's code runs in that browser, not in the compose network. Three ways that broke on
    2026-11-24 although the page itself loaded ("unreachable", "Network Error", a page that
    reloads every second):

    Checks:
        * the UI is published on the host;
        * nothing it hands the browser (its runtime configuration, else the page and its own
          scripts) points to a host only the compose network resolves (``lakekeeper:8181``,
          ``keycloak:8080``);
        * with a runtime configuration (every URL in it is used): every API on another host
          port allows the UI's origin (CORS preflight, sent to the service behind that port).
          Built scripts are not checked for this - they carry fallbacks they do not use
          (RustFS console's ``localhost:9000``, a default ``icebergCatalogUrl``);
        * a webpack dev server's live-reload socket uses the host port of every service that
          runs its image (compose ``WDS_SOCKET_PORT``) - otherwise the page talks to another
          dev server, misses its hot update and reloads endlessly (unity-ui-main).
    """
    ui = UIS.get(PROFILE.server)
    if ui is None:
        pytest.skip(f"catalog {PROFILE.name} has no API for a web UI")
    services = _services()
    published = _published(services)
    host, port = re.match(r"https?://([^/:]+):(\d+)", ui.page).groups()
    host_port = {target: p for p, target in published.items()}.get((host, int(port)))
    assert host_port is not None, f"{ui.page}: not published on the host"
    origin = f"http://localhost:{host_port}"
    shipped = _shipped(ui.page, {"Host": f"localhost:{host_port}"}, ui.runtime_config)
    found = {(h, int(p) if p else None, path) for h, p, path in _URL.findall(shipped)}
    internal = _internal_hosts(services)
    problems = []
    mapped = _host_mapped(services)
    bad = sorted({f"{h}:{p}" if p else h for h, p, _ in found if h in internal} - mapped)
    if bad:
        problems.append(f"{origin}: points the browser to compose-only hosts {bad}")
    if ui.runtime_config:
        problems += _cors_problems(found, origin, host_port, published)
    for h, p, path in sorted(found, key=str):  # a host-mapped login server: its token endpoint
        if f"{h}:{p}" in mapped and "/realms/" in (path or ""):
            r = requests.options(
                f"http://{h}:{p}{path.rstrip('/')}/protocol/openid-connect/token",
                headers={"Origin": origin, "Access-Control-Request-Method": "POST"},
                timeout=10,
            )
            if r.headers.get("access-control-allow-origin") not in (origin, "*"):
                problems.append(f"{origin}: {h}:{p} refuses the UI's origin (CORS)")
    if _DEV_SOCKET.search(shipped):
        problems += _dev_socket_problems(services, services[host]["image"])
    with _gap("host_browser"):
        assert not problems, "\n".join(problems)


def test_ui_login_reaches_the_data():
    """A login through the UI (identity provider as the host's browser reaches it) is accepted.

    The browser gets its token from the identity provider under the provider's HOST address,
    so the token names another issuer than tokens the lab's clients get inside the compose
    network - the catalog must accept both.

    Checks:
        * a token of the UI's login user, issued for the host's address, reads the UI's data
          API (2xx) - the login in the UI ends in a working session, not in 401.
    """
    ui = UIS.get(PROFILE.server)
    if ui is None:
        pytest.skip(f"catalog {PROFILE.name} has no API for a web UI")
    if ui.browser_login is None:
        pytest.skip(
            f"catalog {PROFILE.name}'s UI logs in with the catalog's own credentials "
            "(test_ui_data_needs_login), not through an identity provider"
        )
    r = requests.get(ui.data, timeout=10, **ui.browser_login())
    with _gap("browser_login"):
        assert r.ok, f"the UI's login is refused: {r.status_code} {r.text[:300]}"
