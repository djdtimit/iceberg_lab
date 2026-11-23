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
=============  ================================================  ===============================

Authorization on: Lakekeeper and Unity point their UI at the instance with authorization on
(the main instances run open on purpose); Polaris and RustFS always check.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest
import requests

from iceberg_lab.config import PROFILE, SETTINGS


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


def _lakekeeper_admin() -> dict:
    """Lakekeeper admin headers.

    Returns:
        Request kwargs.
    """
    from authz_backends import LakekeeperAuthz

    return {"headers": LakekeeperAuthz()._h()}


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


#: The Host a browser on the lab host sends (unity-ui is published as localhost:3000). The
#: dev server's host check stays on; the test behaves like that browser.
UNITY_BROWSER = {"Host": "localhost:3000"}

UIS = {
    "lakekeeper": UI(
        "http://lakekeeper-authz:8181/ui/",
        "http://lakekeeper-authz:8181/management/v1/warehouse",
        _lakekeeper_admin,
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
    ),
    "rustfs": UI(
        f"{SETTINGS.s3_endpoint.replace(':9000', ':9001')}/rustfs/console/",
        f"{SETTINGS.s3_endpoint}/rustfs/admin/v3/info",
        _rustfs_admin,
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
    if PROFILE.name not in UIS:
        pytest.skip(f"catalog {PROFILE.name} has no API for a web UI")
    ui = UIS[PROFILE.name]
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
    if PROFILE.name not in UIS:
        pytest.skip(f"catalog {PROFILE.name} has no API for a web UI")
    ui = UIS[PROFILE.name]
    anonymous = requests.get(ui.data, headers=ui.headers, timeout=10)
    assert anonymous.status_code in (401, 403), (
        f"the UI's data API answers an anonymous request with {anonymous.status_code}: "
        f"{anonymous.text[:200]}"
    )
    signed_in = requests.get(ui.data, timeout=10, **ui.auth())
    assert signed_in.ok, (
        f"positive control: with credentials {signed_in.status_code} {signed_in.text[:200]}"
    )
