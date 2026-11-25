"""The catalog behind Gravitino, used directly (profiles ``gravitino_<backend>``).

Tests that run through Gravitino sometimes leave something Gravitino itself cannot address
any more (a namespace name with a dot: Gravitino refuses every call that names it, and its
listing of the whole catalog fails while it exists). This module reaches the backend on its
own Iceberg REST endpoint, with the lab's admin identity for it, to check or clean up.
"""

from __future__ import annotations

from urllib.parse import quote

from iceberg_lab.config import PROFILE, PROFILES, SETTINGS


def backend_catalog():
    """PyIceberg catalog straight on the backend's Iceberg REST endpoint.

    The backend's own identity for the lab (Polaris: root, Unity: its static token, RustFS:
    SigV4 with the lab's keys, Lakekeeper: open) and the lab's S3 keys for the data files, so
    the result does not depend on what either side vends.

    Returns:
        The catalog.

    Raises:
        RuntimeError: The profile does not front another catalog.
    """
    from pyiceberg.catalog import load_catalog

    if not PROFILE.fronts:
        raise RuntimeError(f"catalog {PROFILE.name} does not front another catalog")
    b = PROFILES[PROFILE.fronts]
    s3 = {
        "s3.endpoint": SETTINGS.s3_endpoint,
        "s3.access-key-id": SETTINGS.s3_access_key,
        "s3.secret-access-key": SETTINGS.s3_secret_key,
        "s3.region": SETTINGS.s3_region,
        "s3.path-style-access": "true",
        "client.region": SETTINGS.s3_region,
        "header.X-Iceberg-Access-Delegation": "",
    }
    auth: dict[str, str] = {}
    if b.auth == "oauth2":
        auth = {
            "credential": b.oauth_credential,
            "scope": b.oauth_scope or "",
            "oauth2-server-uri": b.token_url,
        }
    elif b.auth == "token":
        auth = {"token": b.token or ""}
    elif b.auth == "sigv4":
        auth = {
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3",
            "rest.signing-region": SETTINGS.s3_region,
            "client.access-key-id": SETTINGS.s3_access_key,
            "client.secret-access-key": SETTINGS.s3_secret_key,
        }
    cat = load_catalog(
        f"backend-{b.name}",
        type="rest",
        uri=f"{b.base_url}{b.path}",
        warehouse=b.warehouse_sts,
        **auth,
        **s3,
    )
    if b.auth == "sigv4":
        from iceberg_lab.engines import _add_content_sha256

        _add_content_sha256(cat)
    return cat


def _level(name: str) -> str:
    """One path level for a raw name: percent-encoded, dots as ``%2E`` (no client
    normalises those away, PyIceberg does not split on them).

    Args:
        name: The raw name.

    Returns:
        The encoded level.
    """
    return quote(name, safe="").replace(".", "%2E")


def drop_backend_table(namespace: str, name: str) -> int:
    """Drop (purge where the backend allows it) a table in the backend by its raw name.

    Args:
        namespace: Its top-level namespace.
        name: The table name, exactly as created.

    Returns:
        The HTTP status of the DELETE.
    """
    cat = backend_catalog()
    url = cat.url("namespaces/{ns}/tables/{t}", prefixed=True, ns=_level(namespace), t=_level(name))
    return cat._session.delete(url, timeout=30).status_code


def empty_backend_namespace(namespace: str) -> None:
    """Drop every table of a backend namespace by its raw name, then the namespace.

    Args:
        namespace: The top-level namespace.
    """
    cat = backend_catalog()
    url = cat.url("namespaces/{ns}/tables", prefixed=True, ns=_level(namespace))
    r = cat._session.get(url, timeout=30)
    for ident in r.json().get("identifiers", []) if r.ok else []:
        drop_backend_table(namespace, ident["name"])
    drop_backend_namespace(namespace)


def drop_backend_namespace(name: str) -> int:
    """Drop a top-level namespace in the backend by its raw name (no client-side splitting).

    PyIceberg reads a dot in a name as a level separator, so ``..`` cannot be dropped with
    it; the path here encodes the name as one level (dots as ``%2E``, which no HTTP client
    normalises away).

    Args:
        name: The namespace name, exactly as created.

    Returns:
        The HTTP status of the DELETE (204: dropped, 404: not there).
    """
    cat = backend_catalog()
    url = cat.url("namespaces/{level}", prefixed=True, level=_level(name))
    return cat._session.delete(url, timeout=30).status_code
