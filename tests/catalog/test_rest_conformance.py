"""REST conformance (base tests on catalog level, raw Iceberg REST - no engine or client library).

1. Spec endpoints: every endpoint of the Iceberg REST spec gets a valid request on fresh objects.
   The answer classifies it:

       works          2xx
       not offered    404/405/406/501 without a specific Iceberg error type (no such route) ->
                      "catalog X has no API for <endpoint>" - unless /v1/config advertises the
                      endpoint, then it is a failure
       broken         anything else (the endpoint exists, but refuses a valid request)

   An endpoint that works but is missing from the ``endpoints`` list of /v1/config is a gap:
   Iceberg clients only use advertised endpoints (a catalog without the list gets the spec's
   default set, nothing to check there).

2. Behaviour the spec defines beyond "it answers" (issues of Unity Catalog OSS show each one can
   go wrong: unitycatalog#1835, #1800, #1840, #1841):

       error_body       every error is an Iceberg error document {"error": {message, type,
                        code}} with code == HTTP status (missing namespace/table, unknown route,
                        unsupported method)
       head_status      HEAD on an existing namespace/table/view answers 204 without a body,
                        on a missing one 404
       missing_parent   listing namespaces under a parent that does not exist answers 404
       snapshots_refs   loadTable ?snapshots=refs returns only the snapshots refs point at
       snapshots_value  ?snapshots=<undefined value> answers 400
       missing_metadata a table whose metadata file is gone: an error whose type and status agree
                        (not "NotFound" with 500, not a 2xx)

Already elsewhere: ETag/304, pagination, idempotency key (test_catalog_features.py); malformed
bodies and internal names in errors (test_security.py).
"""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa
import pytest
import requests
from rest_client import Rest

from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSE_STS
from iceberg_lab.engines import pyiceberg_catalog

from conftest import expect, find_gap, gap_table

WAREHOUSE = WAREHOUSE_STS
ROWS = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [{"id": 1, "name": "id", "type": "long", "required": False}],
}

# Filled from observed results only: (catalog, case, regex, reason).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "case"), [
    ("lakekeeper", "error_body:unknown_route", r"unknown_route \(HTTP 404\): no Iceberg error document \(None\): ''", "lakekeeper-error-body-empty"),
    ("lakekeeper", "error_body:unsupported_method", r"unsupported_method \(HTTP 405\): no Iceberg error document \(None\): ''", "lakekeeper-error-body-empty"),
    ("reference", "error_body:unsupported_method", r"unsupported_method \(HTTP 501\): no Iceberg error document \(None\): ''", "reference-error-body-empty"),
    ("lakekeeper", "snapshots_value", r"snapshots=some answered 200", "lakekeeper-snapshots-param"),
    ("gravitino", "snapshots_value", r"snapshots=some answered 200", "gravitino-snapshots-param"),
])
# fmt: on


def _gap(case: str):
    """``expect_gap`` for a documented deviation of this catalog.

    Args:
        case: Error case.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, case=case))


@pytest.fixture(scope="module")
def rest():
    """Raw REST client on the STS warehouse.

    Returns:
        The client.
    """
    return Rest(WAREHOUSE)


def _ident(ns: str, name: str) -> dict:
    """Iceberg REST identifier object.

    Args:
        ns: Namespace.
        name: Name.

    Returns:
        ``{namespace, name}``.
    """
    return {"namespace": [ns], "name": name}


def _new(prefix: str) -> str:
    """Unique name with a prefix.

    Args:
        prefix: Name prefix.

    Returns:
        The name.
    """
    return f"{prefix}_{uuid.uuid4().hex[:6]}"


@pytest.fixture
def table(namespace):
    """A table with two snapshots (the first one referenced by no branch or tag).

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(namespace, name)``.
    """
    name = _new("rc")
    t = pyiceberg_catalog(WAREHOUSE).create_table(f"{namespace}.{name}", schema=ROWS.schema)
    t.append(ROWS)
    t.append(ROWS)
    return namespace, name


def _view_body(ns: str, name: str, sql: str = "select 1 as id") -> dict:
    """createView body with one SQL representation.

    Args:
        ns: Namespace.
        name: Name.
        sql: View SQL.

    Returns:
        The body.
    """
    return {
        "name": name,
        "schema": SCHEMA,
        "view-version": {
            "version-id": 1,
            "timestamp-ms": int(time.time() * 1000),
            "schema-id": 0,
            "summary": {"engine-name": "lab"},
            "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}],
            "default-namespace": [ns],
        },
        "properties": {},
    }


def _create_view(rest: Rest, ns: str) -> str:
    """Create a view with a unique name.

    Args:
        rest: Raw REST client.
        ns: Namespace.

    Returns:
        The view name.
    """
    name = _new("rcv")
    r = rest.call("POST", f"namespaces/{ns}/views", json=_view_body(ns, name))
    if not r.ok:
        pytest.skip(f"catalog {PROFILE.name} has no API for views ({r.status_code})")
    return name


# =========================================================================== 1. spec endpoints


@dataclass(frozen=True)
class Endpoint:
    """``spec``: the endpoint as /v1/config lists it; ``send(rest, ns, table)``: a valid request."""

    spec: str
    send: Callable[[Rest, str, str], requests.Response]
    #: Error types that prove the route exists although the request names nothing real
    #: (endpoints whose ids only the server can hand out: plan ids, plan tasks).
    exists_if: tuple[str, ...] = ()


def _rename_table(rest, ns, t):
    """Rename the table into a new name (renameTable).

    Args:
        rest: Raw REST client.
        ns: Namespace.
        t: Table name.

    Returns:
        The response.
    """
    return rest.call(
        "POST",
        "tables/rename",
        json={"source": _ident(ns, t), "destination": _ident(ns, _new("rcr"))},
    )


def _s3():
    """boto3 S3 client with the root keys.

    Returns:
        The client.
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        aws_access_key_id=SETTINGS.s3_access_key,
        aws_secret_access_key=SETTINGS.s3_secret_key,
        region_name=SETTINGS.s3_region,
    )


def _register(rest, ns, t):
    """Adopt a table's metadata under the same name after dropping it without purge (moving a
    table between catalogs). Catalogs that keep a dropped table's location reserved until it is
    purged (Lakekeeper's soft delete: 409, protects the files from the purge) get a copy of the
    metadata at a location of its own instead - same request, nothing overlaps.

    Args:
        rest: Raw REST client.
        ns: Namespace.
        t: Table name.

    Returns:
        The response.
    """
    loaded = rest.call("GET", f"namespaces/{ns}/tables/{t}").json()
    rest.call("DELETE", f"namespaces/{ns}/tables/{t}")
    r = rest.call(
        "POST",
        f"namespaces/{ns}/register",
        json={"name": t, "metadata-location": loaded["metadata-location"]},
    )
    if r.status_code != 409 or "dropped" not in r.text:
        return r
    import json as _json

    md = loaded["metadata"] | {"table-uuid": str(uuid.uuid4())}
    md["location"] = f"{md['location'].rstrip('/')}_reg_{uuid.uuid4().hex[:6]}"
    bucket, prefix = md["location"].removeprefix("s3://").split("/", 1)
    key = f"{prefix}/metadata/00000-{uuid.uuid4()}.metadata.json"
    _s3().put_object(Bucket=bucket, Key=key, Body=_json.dumps(md).encode())
    return rest.call(
        "POST",
        f"namespaces/{ns}/register",
        json={"name": _new("rcg"), "metadata-location": f"s3://{bucket}/{key}"},
    )


def _register_view(rest, ns, _t):
    """Adopt a copy of a fresh view's metadata under a new name (registerView). The copy gets a
    location and uuid of its own, so it overlaps with nothing the catalog already holds.

    Args:
        rest: Raw REST client.
        ns: Namespace.
        _t: Table name (unused).

    Returns:
        The response.
    """
    import json as _json

    v = _create_view(rest, ns)
    md = rest.call("GET", f"namespaces/{ns}/views/{v}").json()["metadata"]
    md = md | {"view-uuid": str(uuid.uuid4())}
    md["location"] = f"{md['location'].rstrip('/')}_reg_{uuid.uuid4().hex[:6]}"
    bucket, prefix = md["location"].removeprefix("s3://").split("/", 1)
    key = f"{prefix}/metadata/00000-{uuid.uuid4()}.metadata.json"
    _s3().put_object(Bucket=bucket, Key=key, Body=_json.dumps(md).encode())
    return rest.call(
        "POST",
        f"namespaces/{ns}/register-view",
        json={"name": _new("rcw"), "metadata-location": f"s3://{bucket}/{key}"},
    )


def _sign(rest, ns, t):
    """Ask the catalog to sign a GET of a file in the table's location (signRequest).

    Args:
        rest: Raw REST client.
        ns: Namespace.
        t: Table name.

    Returns:
        The response.
    """
    loc = rest.call("GET", f"namespaces/{ns}/tables/{t}").json()["metadata"]["location"]
    uri = f"{SETTINGS.s3_endpoint.rstrip('/')}/{loc.removeprefix('s3://').rstrip('/')}/metadata/x"
    body = {"region": SETTINGS.s3_region, "uri": uri, "method": "GET", "headers": {}}
    return rest.call("POST", f"namespaces/{ns}/tables/{t}/sign", json=body)


def _metrics(rest, ns, t):
    """POST a scan report for the table.

    Args:
        rest: Raw REST client.
        ns: Namespace.
        t: Table name.

    Returns:
        The response.
    """
    md = rest.call("GET", f"namespaces/{ns}/tables/{t}").json()["metadata"]
    report = {
        "report-type": "scan-report",
        "table-name": f"{ns}.{t}",
        "snapshot-id": md["current-snapshot-id"],
        "filter": True,
        "schema-id": md["current-schema-id"],
        "projected-field-ids": [1],
        "projected-field-names": ["id"],
        "metrics": {},
    }
    return rest.call("POST", f"namespaces/{ns}/tables/{t}/metrics", json=report)


def _commit_transaction(rest, ns, t):
    """Multi-table commit setting one property.

    Args:
        rest: Raw REST client.
        ns: Namespace.
        t: Table name.

    Returns:
        The response.
    """
    change = {
        "identifier": _ident(ns, t),
        "requirements": [],
        "updates": [{"action": "set-properties", "updates": {"rc": "tx"}}],
    }
    return rest.call("POST", "transactions/commit", json={"table-changes": [change]})


def _view(method: str, suffix: str = "", body=None):
    """Endpoint sender for a view call on a fresh view.

    Args:
        method: HTTP method.
        suffix: Path after the view.
        body: JSON body.

    Returns:
        ``send(rest, ns, t) -> response``.
    """

    def send(rest, ns, _t):
        """Create a view, then call the endpoint on it.

        Args:
            rest: Raw REST client.
            ns: Namespace.
            _t: Table name (unused).

        Returns:
            The response.
        """
        v = _create_view(rest, ns)
        payload = body(ns, v) if body else None
        return rest.call(method, f"namespaces/{ns}/views/{v}{suffix}", json=payload)

    return send


def _replace_view_body(ns, v):
    """replaceView body (new SQL version).

    Args:
        ns: Namespace.
        v: View name.

    Returns:
        The body.
    """
    view = _view_body(ns, v, "select 2 as id")
    return {
        "requirements": [],
        "updates": [
            {
                "action": "add-view-version",
                "view-version": view["view-version"] | {"version-id": 2},
            },
            {"action": "set-current-view-version", "view-version-id": -1},
        ],
    }


def _rename_view(rest, ns, _t):
    """Rename a fresh view.

    Args:
        rest: Raw REST client.
        ns: Namespace.
        _t: Table name (unused).

    Returns:
        The response.
    """
    v = _create_view(rest, ns)
    return rest.call(
        "POST",
        "views/rename",
        json={"source": _ident(ns, v), "destination": _ident(ns, _new("rcv"))},
    )


def _new_namespace(rest, _ns, _t):
    """Create a fresh namespace.

    Args:
        rest: Raw REST client.
        _ns: Namespace (unused).
        _t: Table name (unused).

    Returns:
        The response.
    """
    ns = _new("rcn")
    r = rest.call("POST", "namespaces", json={"namespace": [ns]})
    rest.call("DELETE", f"namespaces/{ns}")
    return r


def _drop_namespace(rest, _ns, _t):
    """Create and drop a fresh namespace.

    Args:
        rest: Raw REST client.
        _ns: Namespace (unused).
        _t: Table name (unused).

    Returns:
        The response.
    """
    ns = _new("rcn")
    rest.call("POST", "namespaces", json={"namespace": [ns]})
    return rest.call("DELETE", f"namespaces/{ns}")


T = "namespaces/{ns}/tables/{t}"
ENDPOINTS = {
    e.spec: e
    for e in [
        Endpoint("GET /v1/{prefix}/namespaces", lambda r, ns, t: r.call("GET", "namespaces")),
        Endpoint("POST /v1/{prefix}/namespaces", _new_namespace),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}",
            lambda r, ns, t: r.call("GET", f"namespaces/{ns}"),
        ),
        Endpoint(
            "HEAD /v1/{prefix}/namespaces/{namespace}",
            lambda r, ns, t: r.call("HEAD", f"namespaces/{ns}"),
        ),
        Endpoint("DELETE /v1/{prefix}/namespaces/{namespace}", _drop_namespace),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/properties",
            lambda r, ns, t: r.call(
                "POST", f"namespaces/{ns}/properties", json={"updates": {"rc": "1"}, "removals": []}
            ),
        ),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            lambda r, ns, t: r.call("GET", f"namespaces/{ns}/tables"),
        ),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/tables",
            lambda r, ns, t: r.call(
                "POST", f"namespaces/{ns}/tables", json={"name": _new("rcc"), "schema": SCHEMA}
            ),
        ),
        Endpoint("POST /v1/{prefix}/namespaces/{namespace}/register", _register),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            lambda r, ns, t: r.call("GET", T.format(ns=ns, t=t)),
        ),
        Endpoint(
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            lambda r, ns, t: r.call("HEAD", T.format(ns=ns, t=t)),
        ),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            lambda r, ns, t: r.call(
                "POST",
                T.format(ns=ns, t=t),
                json={
                    "requirements": [],
                    "updates": [{"action": "set-properties", "updates": {"rc": "1"}}],
                },
            ),
        ),
        Endpoint(
            "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            lambda r, ns, t: r.call("DELETE", T.format(ns=ns, t=t)),
        ),
        Endpoint("POST /v1/{prefix}/tables/rename", _rename_table),
        Endpoint("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics", _metrics),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
            lambda r, ns, t: r.call("GET", T.format(ns=ns, t=t) + "/credentials"),
        ),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
            lambda r, ns, t: r.call("POST", T.format(ns=ns, t=t) + "/plan", json={}),
        ),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
            lambda r, ns, t: r.call("GET", T.format(ns=ns, t=t) + "/plan/no-such-plan"),
            exists_if=("NoSuchPlanIdException",),
        ),
        Endpoint(
            "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
            lambda r, ns, t: r.call("DELETE", T.format(ns=ns, t=t) + "/plan/no-such-plan"),
            exists_if=("NoSuchPlanIdException",),
        ),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
            lambda r, ns, t: r.call(
                "POST", T.format(ns=ns, t=t) + "/tasks", json={"plan-task": "no-such-task"}
            ),
            exists_if=("NoSuchPlanTaskException",),
        ),
        Endpoint("POST /v1/{prefix}/transactions/commit", _commit_transaction),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}/views",
            lambda r, ns, t: r.call("GET", f"namespaces/{ns}/views"),
        ),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/views",
            lambda r, ns, t: r.call(
                "POST", f"namespaces/{ns}/views", json=_view_body(ns, _new("rcv"))
            ),
        ),
        Endpoint("GET /v1/{prefix}/namespaces/{namespace}/views/{view}", _view("GET")),
        Endpoint("HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}", _view("HEAD")),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
            _view("POST", body=_replace_view_body),
        ),
        Endpoint("DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}", _view("DELETE")),
        Endpoint("POST /v1/{prefix}/views/rename", _rename_view),
        Endpoint("POST /v1/{prefix}/namespaces/{namespace}/register-view", _register_view),
        Endpoint(
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister",
            lambda r, ns, t: r.call("POST", T.format(ns=ns, t=t) + "/unregister"),
        ),
        Endpoint("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign", _sign),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}/functions",
            lambda r, ns, t: r.call("GET", f"namespaces/{ns}/functions"),
        ),
        Endpoint(
            "GET /v1/{prefix}/namespaces/{namespace}/functions/{function}",
            lambda r, ns, t: r.call("GET", f"namespaces/{ns}/functions/no_such_function"),
            exists_if=("NoSuchFunctionException",),
        ),
    ]
}

#: Spec endpoints tested elsewhere, not by test_spec_endpoint (checked by tests/selftest/test_iceberg_spec.py).
SPEC_ELSEWHERE = {
    "GET /v1/config": "read by every test (tests/lab/rest_client.py) and test_advertised_endpoints_are_recorded",
    "POST /v1/oauth/tokens": "token exchange of the authz profile (tests/security/test_security.py)",
}

NO_ROUTE = (404, 405, 406, 501)


def _error_type(r: requests.Response) -> str:
    """Iceberg error type of an answer.

    Args:
        r: HTTP response.

    Returns:
        ``error.type``, or empty.
    """
    try:
        return str(r.json().get("error", {}).get("type", ""))
    except ValueError:
        return ""


def _error_message(r: requests.Response) -> str:
    """``error.message`` of an Iceberg error answer.

    Args:
        r: HTTP response.

    Returns:
        The message, or empty.
    """
    try:
        return str(r.json().get("error", {}).get("message", ""))
    except ValueError:
        return ""


def _no_route(r: requests.Response, ep: Endpoint) -> bool:
    """The answer says "no such endpoint", not "no such object".

    Args:
        r: HTTP response.
        ep: The endpoint under test.

    Returns:
        Whether it is a plain "no such route".
    """
    if r.status_code == 400 and re.match(r"No route for request: ", _error_message(r)):
        return True  # the Iceberg REST fixture's (reference implementation's) "no such route"
    if r.status_code not in NO_ROUTE:
        return False
    kind = _error_type(r)
    if any(t in kind for t in ep.exists_if):
        return False
    # A 404 naming a missing object of the request (NoSuchTable...) means the route exists.
    return not (r.status_code == 404 and kind.startswith("NoSuch"))


def _short(spec: str) -> str:
    """Endpoint shortened for ids and messages.

    Args:
        spec: Endpoint as the spec writes it.

    Returns:
        The short form.
    """
    return spec.replace("/v1/{prefix}", "").replace("{namespace}", "{ns}")


@pytest.mark.parametrize("spec", list(ENDPOINTS), ids=[_short(s) for s in ENDPOINTS])
def test_spec_endpoint(spec, rest, table):
    """A spec endpoint answers a valid request with 2xx - or the catalog does not offer it at all.

    Checks:
        * a valid request to the endpoint answers 2xx; "not offered" only if the answer is a
          plain "no such route" (404/405/501 without an Iceberg error), else a failure;
        * a working endpoint is also advertised in ``/v1/config`` (if the catalog advertises
          endpoints at all).

    Args:
        spec: The endpoint as the spec writes it, e.g. ``GET /v1/{prefix}/namespaces``.
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name)`` of a table with two snapshots, the first one referenced by
            no branch or tag.
    """
    ep = ENDPOINTS[spec]
    ns, t = table
    r = ep.send(rest, ns, t)
    advertised = spec in rest.endpoints
    if _no_route(r, ep) and not advertised:
        pytest.skip(f"catalog {PROFILE.name} has no API for {_short(spec)} (HTTP {r.status_code})")
    works = r.ok or any(t in _error_type(r) for t in ep.exists_if)
    with _gap(f"endpoint:{_short(spec)}"):
        assert works, f"{_short(spec)} -> HTTP {r.status_code}: {r.text[:600]}"
        assert advertised or not rest.endpoints, (
            f"{_short(spec)} works but /v1/config does not advertise it"
        )


# =========================================================================== 2. defined behaviour


def _error_document(r: requests.Response) -> str | None:
    """None if ``r`` is a well-formed Iceberg error answer, else what is wrong.

    Args:
        r: HTTP response.

    Returns:
        None, or the problem.
    """
    try:
        err = r.json()["error"]
    except (ValueError, KeyError, TypeError):
        return f"no Iceberg error document ({r.headers.get('content-type')}): {r.text[:200]!r}"
    missing = [k for k in ("message", "type", "code") if k not in err]
    if missing:
        return f"error document without {missing}: {err}"
    if err["code"] != r.status_code:
        return f"error code {err['code']} != HTTP status {r.status_code}"
    return None


ERROR_CASES = {
    "missing_namespace": (
        lambda rest, ns, t: rest.call("GET", f"namespaces/{_new('nx')}"),
        404,
        "NoSuchNamespaceException",
    ),
    "missing_table": (
        lambda rest, ns, t: rest.call("GET", f"namespaces/{ns}/tables/{_new('nx')}"),
        404,
        "NoSuchTableException",
    ),
    "unknown_route": (
        lambda rest, ns, t: rest.call("GET", f"namespaces/{ns}/no-such-route"),
        None,
        None,
    ),
    "unsupported_method": (lambda rest, ns, t: rest.call("PATCH", f"namespaces/{ns}"), None, None),
}


@pytest.mark.parametrize("case", list(ERROR_CASES))
def test_error_body(case, rest, table):
    """Errors are Iceberg error documents with the spec's status and type.

    Checks:
        * the request fails;
        * the body is an Iceberg error document (``error.message``, ``type``, ``code`` equal to
          the HTTP status);
        * status and type are the spec's (e.g. 404 NoSuchTableException).

    Args:
        case: Error case (see ``ERROR_CASES``): missing namespace/table, unknown route, wrong
            method.
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name)`` of a table with two snapshots, the first one referenced by
            no branch or tag.
    """
    send, status, kind = ERROR_CASES[case]
    r = send(rest, *table)
    assert not r.ok, f"{case}: answered {r.status_code}"
    with _gap(f"error_body:{case}"):
        problem = _error_document(r)
        assert problem is None, f"{case} (HTTP {r.status_code}): {problem}"
        if status:
            assert (r.status_code, _error_type(r)) == (status, kind), (
                f"{case}: {r.status_code} {_error_type(r)}, spec: {status} {kind}"
            )


@pytest.mark.parametrize("target", ["namespace", "table", "view"])
def test_head_status(target, rest, table):
    """HEAD answers 204 without a body for an existing object, 404 for a missing one.

    Checks:
        * existing object: 204 and an empty body;
        * missing object: 404.
        Catalogs without HEAD for that object: not offered.

    Args:
        target: ``namespace``, ``table`` or ``view``.
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name)`` of a table with two snapshots, the first one referenced by
            no branch or tag.
    """
    ns, t = table
    path = {
        "namespace": lambda name: f"namespaces/{name}",
        "table": lambda name: f"namespaces/{ns}/tables/{name}",
        "view": lambda name: f"namespaces/{ns}/views/{name}",
    }[target]
    existing = {"namespace": ns, "table": t}.get(target) or _create_view(rest, ns)
    r = rest.call("HEAD", path(existing))
    if _no_route(r, Endpoint("", None)) and r.status_code != 404:
        pytest.skip(f"catalog {PROFILE.name} has no API for HEAD on a {target} ({r.status_code})")
    missing = rest.call("HEAD", path(_new("nx")))
    with _gap(f"head_status:{target}"):
        assert r.status_code == 204, f"existing {target}: HEAD answered {r.status_code}, spec: 204"
        assert not r.content, f"existing {target}: HEAD answer has a body ({len(r.content)} bytes)"
        assert missing.status_code == 404, f"missing {target}: HEAD answered {missing.status_code}"


def test_missing_parent(rest):
    """Listing namespaces under a parent that does not exist answers 404 (NoSuchNamespace).

    Checks:
        * ``GET /namespaces?parent=<missing>`` answers 404.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
    """
    r = rest.call("GET", "namespaces", params={"parent": _new("nx")})
    with _gap("missing_parent"):
        assert r.status_code == 404, f"parent that does not exist: {r.status_code} {r.text[:200]}"


def test_snapshots_refs(rest, table):
    """``?snapshots=refs`` returns only the snapshots a branch or tag points at.

    Checks:
        * the table has 2 snapshots (setup);
        * with ``snapshots=refs`` exactly the referenced snapshots come back.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name)`` of a table with two snapshots, the first one referenced by
            no branch or tag.
    """
    ns, t = table
    every = rest.call("GET", f"namespaces/{ns}/tables/{t}", params={"snapshots": "all"})
    every.raise_for_status()
    md = every.json()["metadata"]
    assert len(md["snapshots"]) == 2, f"setup: {len(md['snapshots'])} snapshots"
    referenced = {ref["snapshot-id"] for ref in md.get("refs", {}).values()}
    r = rest.call("GET", f"namespaces/{ns}/tables/{t}", params={"snapshots": "refs"})
    r.raise_for_status()
    got = {s["snapshot-id"] for s in r.json()["metadata"]["snapshots"]}
    with _gap("snapshots_refs"):
        assert got == referenced, (
            f"snapshots=refs returned {len(got)} snapshots, {len(referenced)} are referenced"
        )


def test_snapshots_value(rest, table):
    """A ``?snapshots`` value the spec does not define is refused with 400.

    Checks:
        * ``snapshots=some`` answers 400 (the spec allows only all | refs).

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name)`` of a table with two snapshots, the first one referenced by
            no branch or tag.
    """
    ns, t = table
    r = rest.call("GET", f"namespaces/{ns}/tables/{t}", params={"snapshots": "some"})
    with _gap("snapshots_value"):
        assert r.status_code == 400, f"snapshots=some answered {r.status_code}, spec: all|refs only"


def test_missing_metadata(rest, table):
    """The current metadata file of a table is deleted from storage; the catalog's answer must fit.

    Checks:
        * RustFS: the metadata file cannot be deleted at all ("reserved for the table catalog")
          - protected, correct;
        * a catalog that serves the table from its database answers 200 with the SAME metadata
          location;
        * otherwise: an Iceberg error document, and a "not found" type never with a 5xx
          status.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name)`` of a table with two snapshots, the first one referenced by
            no branch or tag.
    """
    ns, t = table
    location = rest.call("GET", f"namespaces/{ns}/tables/{t}").json()["metadata-location"]
    bucket, key = location.removeprefix("s3://").split("/", 1)
    s3 = _s3()
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except s3.exceptions.ClientError as exc:
        # RustFS: "Object key is reserved for the table catalog" - the file cannot vanish
        assert "reserved for the table catalog" in str(exc), exc
        return
    r = rest.call("GET", f"namespaces/{ns}/tables/{t}")
    if r.ok:
        assert r.json()["metadata-location"] == location, "served another metadata version"
        return
    with _gap("missing_metadata"):
        problem = _error_document(r)
        assert problem is None, f"HTTP {r.status_code}: {problem}"
        kind = _error_type(r)
        not_found = "NotFound" in kind or kind.startswith("NoSuch")
        assert not (not_found and r.status_code >= 500), (
            f"{r.status_code} with type {kind}: a client reads 'the table does not exist'"
        )
