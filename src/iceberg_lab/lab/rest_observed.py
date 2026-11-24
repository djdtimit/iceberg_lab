"""Which responses of the Iceberg REST spec the lab's own requests got from the catalog.

The OpenAPI document defines, per operation, the status codes a catalog may answer
(``scripts/iceberg_spec.py`` -> docs/api/iceberg-rest-responses.txt). During a run every answer
the catalog gives to the lab's raw requests (``requests``: the REST client, conformance and
security tests, not the engines' own clients) is mapped to its spec operation and counted:
``<report dir>/rest_observed.json``, ``{"GET /v1/{prefix}/namespaces 404": 3, ...}``.
``scripts/export_reference.py`` keeps it as ``results/<catalog>.rest.json``, and
``scripts/rest_conformance_doc.py`` shows per operation which defined codes some test provoked.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from functools import cache
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[3]
SPEC_RESPONSES = ROOT / "docs" / "api" / "iceberg-rest-responses.txt"
REST_SPEC = ROOT / "docs" / "api" / "iceberg-rest-spec.txt"
#: ``"METHOD template status"`` -> count, for this process.
OBSERVED: Counter[str] = Counter()


@cache
def _templates() -> list[tuple[re.Pattern, str, str]]:
    """The spec's endpoints as path patterns, most specific first.

    Returns:
        ``(pattern, method, template)``; ``{prefix}`` is optional and may hold slashes.
    """
    out = []
    for line in REST_SPEC.read_text().splitlines()[1:]:
        if not line.strip():
            continue
        method, template = line.split(" ", 1)
        rx = re.escape(template).replace(re.escape("/{prefix}"), "(?:/.+?)?")
        rx = re.sub(r"\\\{[a-z-]+\\\}", "[^/]+", rx)
        out.append((re.compile(f"^{rx}$"), method, template))
    return sorted(out, key=lambda t: -len(re.sub(r"\{[^}]+\}", "", t[2])))


def operation(method: str, path: str) -> str | None:
    """The spec operation of a request path, e.g. ``GET /v1/{prefix}/namespaces``.

    Args:
        method: HTTP method.
        path: Path from ``/v1`` on (the catalog's base path removed).

    Returns:
        ``"METHOD template"``, or None for a path the spec does not define.
    """
    for rx, m, template in _templates():
        if m == method.upper() and rx.match(path):
            return f"{m} {template}"
    return None


def record(method: str, url: str, status: int, catalog_uri: str) -> None:
    """Count one answer of the catalog, if it is one of the spec's operations.

    Args:
        method: HTTP method of the request.
        url: Request URL.
        status: HTTP status of the answer.
        catalog_uri: The catalog's REST URI (``SETTINGS.catalog_uri``).
    """
    base = urlsplit(catalog_uri)
    u = urlsplit(url)
    if u.netloc != base.netloc or not u.path.startswith(base.path.rstrip("/") + "/v1"):
        return
    op = operation(method, u.path[len(base.path.rstrip("/")) :])
    if op is not None:
        OBSERVED[f"{op} {status}"] += 1


def install(catalog_uri: str) -> None:
    """Count every answer of the catalog to a ``requests`` call of this process.

    Args:
        catalog_uri: The catalog's REST URI.
    """
    import requests

    if getattr(requests.Session.send, "_lab_observed", False):
        return
    send = requests.Session.send

    def observed_send(self, request, **kw):
        """``Session.send`` that counts the answer (:func:`record`).

        Args:
            self: The session.
            request: The prepared request.
            **kw: Passed through.

        Returns:
            The response.
        """
        response = send(self, request, **kw)
        try:
            record(request.method, request.url, response.status_code, catalog_uri)
        except Exception:  # noqa: BLE001,S110 - counting must never break a test
            pass
        return response

    observed_send._lab_observed = True
    requests.Session.send = observed_send


def write(report_dir: Path) -> None:
    """Add this process's counts to ``<report_dir>/rest_observed.json``.

    The caller holds the report lock (``reporting.report_lock``).

    Args:
        report_dir: The report directory.
    """
    if not OBSERVED:
        return
    path = report_dir / "rest_observed.json"
    try:
        total = Counter(json.loads(path.read_text()))
    except (OSError, ValueError):
        total = Counter()
    total.update(OBSERVED)
    path.write_text(json.dumps(dict(sorted(total.items())), indent=0))
    OBSERVED.clear()


def spec_responses() -> dict[str, list[str]]:
    """The status codes the spec defines per operation (docs/api/iceberg-rest-responses.txt).

    Returns:
        ``{"METHOD template": ["200", "404", ...]}``.
    """
    out: dict[str, list[str]] = {}
    for line in SPEC_RESPONSES.read_text().splitlines()[1:]:
        if line.strip():
            op, code = line.rsplit(" ", 1)
            out.setdefault(op, []).append(code)
    return out
