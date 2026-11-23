"""Catalog features: what does the catalog under test offer beyond the engine matrices?

One fixed list of features, run identically against every catalog (``CATALOG=...``), so
the report shows them side by side. A feature the catalog does not offer is "no API"
(``catalog X has no API for ...``); a feature it offers must actually work - each test
checks the effect, not only that an endpoint answers. Catalog calls are raw REST where the
catalog is judged (a client bug must not count as a catalog gap).

Base (two or more catalogs, or the Iceberg REST spec):
    register_table, rename_across_namespaces, drop_namespace_after_rename, metrics_report,
    load_credentials, pagination, etag_not_modified, idempotency_key, remote_signing,
    generic_tables (Polaris, Unity), external_table_sync (Polaris notifications, RustFS
    operator sync), catalog_functions (Lakekeeper, Unity)

Special features of one catalog (marker ``specific``, area "Catalog-specific", not compared):
    Lakekeeper  soft_delete_and_undrop, drop_protection, tags, warehouse_statistics,
                search_tables, namespace_move, format_version_policy, storage_validation,
                permission_check, projects, maintenance_mode, endpoint_statistics,
                task_queue_config, managed_by, unregister
    Polaris     policies, federation / federation_direct (an external catalog -> Lakekeeper),
                openlineage
    RustFS      table_bucket_lifecycle, compat_endpoint, unknown_table_bucket_is_not_found,
                table_maintenance, refs_api, metadata_location_api, table_admin, catalog_backup
    Unity       volumes, functions, models, temporary_path_credentials, schema_api

Permissions of every catalog: test_authz.py, test_rbac.py, test_privileges.py.
"""

from __future__ import annotations

import json
import uuid

import pyarrow as pa
import pytest
import requests
from rest_client import Rest

from iceberg_lab.config import (
    PROFILE,
    REMOTE_SIGNING,
    SETTINGS,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
)
from iceberg_lab.engines import pyiceberg_catalog

from conftest import drop_namespace_tree, expect, find_gap, gap_table

WAREHOUSE = WAREHOUSE_STS
ROWS = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
#: What a refusal looks like (permission, authentication, or "does not exist" for hidden
#: objects) - any other error must not pass as "correctly refused".
DENIED = (
    r"(?i)(401|403|forbidden|unauthori[sz]ed|not authori[sz]ed|permission|access ?denied|"
    r"ACCESS_DENIED|not allowed|does not exist|not found|NoSuch\w+)"
)

# Filled from observed failures only: (catalog, test, regex, cause).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "test"), [
    ("rustfs", "register_table", r"metadata location must be inside the table metadata directory", "rustfs-own-layout-only"),
    ("rustfs", "drop_namespace_after_rename", r"-> HTTP 409: .*namespace labtables/feat_\w+_r is not empty.*NamespaceNotEmptyException", "rustfs-rename-hidden-entry"),
    ("rustfs", "table_bucket_lifecycle", r"disable: 409 .*table bucket contains reserved metadata", "rustfs-bucket-not-removable"),
    ("rustfs", "rustfs_unknown_bucket", r"GET of a missing bucket: 200", "rustfs-unknown-bucket-200"),
    ("rustfs", "external_table_sync", r"external/sync -> HTTP 400: .*metadata location must be inside the table metadata directory", "rustfs-own-layout-only"),
    ("rustfs", "rustfs_maintenance_metadata", r"/maintenance/metadata -> HTTP 501", "rustfs-routes-501"),
    ("rustfs", "rustfs_rollback", r"/catalog/rollback -> HTTP 400: .*last-sequence-number must not decrease", "rustfs-rollback-refused"),
    ("rustfs", "rustfs_recovery", r"/catalog/recovery -> HTTP 501", "rustfs-routes-501"),
    ("rustfs", "rustfs_backup", r"/catalog/backup -> HTTP 501", "rustfs-routes-501"),
    ("polaris", "remote_signing", r"/tables/\w+/sign -> HTTP 501", "polaris-signing-501"),
    ("lakekeeper", "functions", r"/functions -> HTTP 404", "lakekeeper-functions-unregister-404"),
    ("lakekeeper", "unregister", r"/unregister -> HTTP 404", "lakekeeper-functions-unregister-404"),
    ("polaris", "federation_direct", r"RESTError 500: Received unexpected JSON Payload: \{\"details\":\"Error id", "polaris-federation-brotli"),
    ("unity", "rename_across_namespaces", r"-> HTTP 501: .*Renaming a table into another namespace is not supported", "unity-rename-same-namespace-only"),
    ("unity", "pagination", r"assert \d+ == 1", "unity-ignores-page-size"),
    ("polaris", "rename_across_namespaces", r"-> HTTP 403: .*Invalid locations .* is not in the list of allowed locations", "polaris-rename-across-namespaces"),
])
# fmt: on


def offered_by(*catalogs: str, what: str) -> None:
    """Skip as "no API" on catalogs that do not have this feature at all.

    Args:
        what: The feature (for the skip reason).
        catalogs: Catalogs that offer the feature.
    """
    if PROFILE.name not in catalogs:
        pytest.skip(f"catalog {PROFILE.name} has no API for {what}")


def gap(test: str):
    """``expect_gap`` for a documented gap of the test on this catalog.

    Args:
        test: Gap key.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, test=test))


# ----------------------------------------------------------------------------- helpers


@pytest.fixture(scope="module")
def rest():
    """Raw REST client on the catalog's main warehouse.

    Returns:
        The client.
    """
    return Rest(WAREHOUSE)


@pytest.fixture
def table(namespace, table_name):
    """``(namespace, name, pyiceberg table)`` with ids 1..3.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
        table_name: Unique table name for the test.

    Returns:
        ``(namespace, name, table)``.
    """
    cat = pyiceberg_catalog(WAREHOUSE)
    t = cat.create_table(f"{namespace}.{table_name}", schema=ROWS.schema)
    t.append(ROWS)
    return namespace, table_name, t


@pytest.fixture
def extra_namespace():
    """A second, empty namespace; dropped (with its tables) afterwards.

    Yields:
        The namespace.
    """
    cat = pyiceberg_catalog(WAREHOUSE)
    ns = f"feat_{uuid.uuid4().hex[:8]}"
    cat.create_namespace(ns)
    yield ns
    drop_namespace_tree(cat, ns)


def _drop(cat, ident) -> None:
    """Purge a table; catalogs without purge (RustFS: 406) get a plain drop.

    Args:
        cat: PyIceberg catalog.
        ident: Table identifier ``namespace.table``.
    """
    try:
        cat.purge_table(ident)
    except Exception:  # noqa: BLE001 - cleanup only
        cat.drop_table(ident)


def lk(method: str, path: str, **kw) -> requests.Response:
    """Lakekeeper management API call.

    Args:
        method: HTTP method.
        path: Path relative to the API base.
        kw: Passed to ``requests.request`` (json, params, headers, ...).

    Returns:
        The response.
    """
    return requests.request(
        method,
        f"{SETTINGS.catalog_base.rstrip('/')}/management/v1/{path.lstrip('/')}",
        timeout=30,
        **kw,
    )


def polaris(method: str, path: str, api: str = "management", **kw) -> requests.Response:
    """Polaris management (``/api/management/v1``) or catalog-extension call.

    Args:
        method: HTTP method.
        path: Path relative to the API base.
        api: ``management`` or ``catalog`` (Polaris' catalog extensions).
        kw: Passed to ``requests.request`` (json, params, headers, ...).

    Returns:
        The response.
    """
    root = {
        "management": "/api/management/v1",
        "catalog": "/api/catalog/polaris/v1",
        "iceberg": "/api/catalog/v1",
    }[api]
    headers = (
        catalog_headers(SETTINGS) | {"Content-Type": "application/json"} | kw.pop("headers", {})
    )
    return requests.request(
        method,
        f"{SETTINGS.catalog_base.rstrip('/')}{root}/{path.lstrip('/')}",
        headers=headers,
        timeout=30,
        **kw,
    )


def ok(r: requests.Response, *codes: int) -> requests.Response:
    """Assert the status (2xx by default) and show the server's answer otherwise.

    Args:
        r: HTTP response.
        codes: Status codes that count as success (default: any 2xx).

    Returns:
        The response.
    """
    good = r.status_code in codes if codes else r.ok
    assert good, f"{r.request.method} {r.url} -> HTTP {r.status_code}: {r.text[:1500]}"
    return r


def rows(t) -> list[int]:
    """Sorted ids of a table (refreshed first).

    Args:
        t: PyIceberg table.

    Returns:
        The ids.
    """
    return sorted(t.refresh().scan().to_arrow()["id"].to_pylist())


# ----------------------------------------------------------------- Iceberg REST spec


def test_register_table(rest, namespace, table_name):
    """Adopt a table written by ANOTHER catalog: register its metadata file, read and write it.

    The table is created with a throw-away PyIceberg SQL catalog (in-memory SQLite) directly on
    RustFS inside the warehouse location - the migration case ``register`` exists for.

    Checks:
        * ``POST .../register`` with the foreign metadata location succeeds;
        * the registered table reads exactly ids 1..3;
        * an append through the catalog works (ids 1..3 twice).
        Catalogs without the API (not advertised / no endpoint): not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
    """
    from pyiceberg.catalog.sql import SqlCatalog

    from iceberg_lab.engines import static_s3_props

    new = f"{table_name}_reg"
    location = f"{PROFILE.location}/{namespace}/{new}"
    foreign = SqlCatalog(
        "foreign", uri="sqlite:///:memory:", warehouse=location, **static_s3_props(SETTINGS)
    )
    foreign.create_namespace("x")
    t = foreign.create_table("x.t", schema=ROWS.schema, location=location)
    t.append(ROWS)
    with gap("register_table"):
        if not rest.advertises("POST /v1/{prefix}/namespaces/{namespace}/register"):
            pytest.skip(f"catalog {PROFILE.name} has no API for registering tables")
        ok(
            rest.call(
                "POST",
                f"namespaces/{namespace}/register",
                json={"name": new, "metadata-location": t.metadata_location},
            )
        )
        reg = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{new}")
        assert rows(reg) == [1, 2, 3]
        reg.append(ROWS)  # the adopted table is writable through the catalog
        assert rows(reg) == [1, 1, 2, 2, 3, 3]


def _ident(ns: str, name: str) -> dict:
    """Iceberg REST table identifier object.

    Args:
        ns: Namespace.
        name: Table name.

    Returns:
        ``{namespace, name}``.
    """
    return {"namespace": [ns], "name": name}


def test_rename_across_namespaces(rest, table, extra_namespace):
    """Rename moves a table into another namespace; its data stays readable there.

    Every checked catalog call is raw REST (a client bug must not count as a catalog gap);
    reading the rows needs a client and uses the lab's PyIceberg.

    Checks:
        * ``POST /tables/rename`` to the other namespace answers 2xx;
        * the table under the new name reads ids 1..3;
        * ``HEAD`` of the old name answers 404.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
        extra_namespace: A second, empty namespace; dropped with its tables afterwards.
    """
    ns, name, _ = table
    with gap("rename_across_namespaces"):
        ok(
            rest.call(
                "POST",
                "tables/rename",
                json={"source": _ident(ns, name), "destination": _ident(extra_namespace, name)},
            )
        )
        assert rows(pyiceberg_catalog(WAREHOUSE).load_table(f"{extra_namespace}.{name}")) == [
            1,
            2,
            3,
        ]
        ok(rest.call("HEAD", f"namespaces/{ns}/tables/{name}"), 404)


def test_drop_namespace_after_rename(rest, extra_namespace):
    """After renaming a table and dropping it under its new name, the namespace is empty.

    Checks (raw REST):
        * create namespace and table ``before``, rename to ``after``, drop ``after`` - all 2xx;
        * the namespace can then be dropped (2xx) - no hidden entry of the old name keeps it
          "not empty".

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        extra_namespace: A second, empty namespace; dropped with its tables afterwards.
    """
    ns = f"{extra_namespace}_r"
    schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [{"id": 1, "name": "id", "type": "long", "required": False}],
    }
    ok(rest.call("POST", "namespaces", json={"namespace": [ns]}))
    ok(rest.call("POST", f"namespaces/{ns}/tables", json={"name": "before", "schema": schema}))
    ok(
        rest.call(
            "POST",
            "tables/rename",
            json={"source": _ident(ns, "before"), "destination": _ident(ns, "after")},
        )
    )
    ok(rest.call("DELETE", f"namespaces/{ns}/tables/after"))
    with gap("drop_namespace_after_rename"):
        r = rest.call("DELETE", f"namespaces/{ns}")
        if not r.ok:
            # clear the ghost entry by re-creating the old name, so nothing is left behind
            ok(
                rest.call(
                    "POST", f"namespaces/{ns}/tables", json={"name": "before", "schema": schema}
                )
            )
            ok(rest.call("DELETE", f"namespaces/{ns}/tables/before"))
            ok(rest.call("DELETE", f"namespaces/{ns}"))
        ok(r)


def test_metrics_report(rest, table):
    """The client may POST scan metrics; the catalog accepts a scan report.

    Checks:
        * a spec-conform scan report for the table is accepted with 200 or 204.
        Catalogs without the API (not advertised / no endpoint): not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    ns, name, t = table
    with gap("metrics_report"):
        if not rest.advertises("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"):
            pytest.skip(f"catalog {PROFILE.name} has no API for metrics reports")
        report = {
            "report-type": "scan-report",
            "table-name": f"{ns}.{name}",
            "snapshot-id": t.current_snapshot().snapshot_id,
            "filter": True,
            "schema-id": t.schema().schema_id,
            "projected-field-ids": [1],
            "projected-field-names": ["id"],
            "metrics": {
                "total-planning-duration": {
                    "count": 1,
                    "time-unit": "nanoseconds",
                    "total-duration": 1000,
                }
            },
        }
        ok(rest.call("POST", f"namespaces/{ns}/tables/{name}/metrics", json=report), 200, 204)


def test_load_credentials(rest, table):
    """``GET .../credentials`` (spec 1.8) returns fresh storage credentials for the table.

    Checks:
        * the endpoint answers 2xx;
        * the answer contains at least one storage credential with an ``s3.access-key-id``.
        Catalogs without the API (not advertised / no endpoint): not offered. Catalogs that vend no keys: not testable.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    from conftest import require_vended_credentials

    require_vended_credentials()
    ns, name, _ = table
    with gap("load_credentials"):
        if not rest.advertises(
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"
        ):
            pytest.skip(f"catalog {PROFILE.name} has no API for the loadCredentials endpoint")
        r = ok(
            rest.call(
                "GET",
                f"namespaces/{ns}/tables/{name}/credentials",
                headers={"X-Iceberg-Access-Delegation": "vended-credentials"},
            )
        )
        creds = r.json().get("storage-credentials") or []
        assert creds and any("s3.access-key-id" in c.get("config", {}) for c in creds), r.text[:300]


def test_pagination(rest, namespace, table_name):
    """Listing tables page by page works.

    Checks (two tables in the namespace):
        * ``pageSize=1`` returns exactly one identifier and a ``next-page-token``;
        * the next page (with that token) returns exactly one other identifier.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
    """
    cat = pyiceberg_catalog(WAREHOUSE)
    ns = f"{namespace}"
    for i in (1, 2):
        cat.create_table(f"{ns}.{table_name}_{i}", schema=ROWS.schema)
    with gap("pagination"):
        r = ok(rest.call("GET", f"namespaces/{ns}/tables", params={"pageSize": 1}))
        body = r.json()
        assert len(body["identifiers"]) == 1, body
        assert body.get("next-page-token"), body
        r2 = ok(
            rest.call(
                "GET",
                f"namespaces/{ns}/tables",
                params={"pageSize": 1, "pageToken": body["next-page-token"]},
            )
        )
        assert (
            len(r2.json()["identifiers"]) == 1 and r2.json()["identifiers"] != body["identifiers"]
        )


def test_etag_not_modified(rest, table):
    """loadTable returns an ETag; repeating it with ``If-None-Match`` answers 304.

    Checks:
        * the 2xx loadTable answer carries an ``ETag`` header (else: not offered);
        * the same request with ``If-None-Match: <etag>`` answers 304 Not Modified.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    ns, name, _ = table
    with gap("etag_not_modified"):
        r = ok(rest.call("GET", f"namespaces/{ns}/tables/{name}"))
        etag = r.headers.get("ETag")
        if not etag:
            pytest.skip(f"catalog {PROFILE.name} has no API for ETags on loadTable")
        r2 = rest.call("GET", f"namespaces/{ns}/tables/{name}", headers={"If-None-Match": etag})
        assert r2.status_code == 304, f"HTTP {r2.status_code}"


def test_idempotency_key(rest, namespace):
    """The same ``Idempotency-Key`` twice: the retried create succeeds instead of 409.

    Checks:
        * two identical createTable requests with the same key both answer 2xx.
        Catalogs that advertise no ``idempotency-key-lifetime`` in /config: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        namespace: Fresh namespace of the test session.
    """
    with gap("idempotency_key"):
        if "idempotency-key-lifetime" not in rest.config:
            pytest.skip(f"catalog {PROFILE.name} has no API for idempotency keys")
        key = str(uuid.uuid4())
        body = {
            "name": f"idem_{uuid.uuid4().hex[:6]}",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [{"id": 1, "name": "id", "type": "long", "required": False}],
            },
        }
        for _ in range(2):
            ok(
                rest.call(
                    "POST",
                    f"namespaces/{namespace}/tables",
                    json=body,
                    headers={"Idempotency-Key": key},
                )
            )


def test_remote_signing(rest, table):
    """The catalog signs S3 requests for the client instead of handing out keys.

    Checks:
        * Polaris (endpoint exists, not advertised): its sign endpoint answers 2xx for a GET of
          the table's metadata file;
        * catalogs with a signing warehouse: a client using it reads ids 1..3 and the table's
          FileIO properties name a signer;
        * others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    if PROFILE.name == "polaris":
        # Polaris 1.9 has the Iceberg signing endpoint in its API (not advertised in /config).
        ns, name, t = table
        uri = f"{SETTINGS.s3_endpoint}/{t.metadata_location.removeprefix('s3://')}"
        with gap("remote_signing"):
            body = {"region": SETTINGS.s3_region, "method": "GET", "uri": uri, "headers": {}}
            ok(rest.call("POST", f"namespaces/{ns}/tables/{name}/sign", json=body))
        return
    with gap("remote_signing"):
        if not REMOTE_SIGNING:
            pytest.skip(f"catalog {PROFILE.name} has no API for remote signing")
        from iceberg_lab.config import WAREHOUSE_SIGNING

        cat = pyiceberg_catalog(WAREHOUSE_SIGNING)
        ns, name, _ = table
        cat.create_namespace_if_not_exists(ns)
        t = cat.create_table(f"{ns}.{name}_sig", schema=ROWS.schema)
        t.append(ROWS)
        assert rows(t) == [1, 2, 3]
        assert t.io.properties.get("s3.signer") or t.io.properties.get("s3.remote-signing-enabled")
        cat.purge_table(f"{ns}.{name}_sig")


# ------------------------------------------------------------------- Lakekeeper extras


def _lk_ids(rest, ns: str, name: str | None = None) -> dict:
    """Lakekeeper ids of the warehouse and (optionally) a table.

    Args:
        rest: Raw REST client.
        ns: Namespace.
        name: Table name (None: warehouse only).

    Returns:
        ``{warehouse, table?}``.
    """
    ids = {"warehouse": rest.prefix}
    r = ok(rest.call("GET", f"namespaces/{ns}"))
    ids["namespace"] = r.json()["properties"]["namespace_id"]
    if name:
        r = ok(rest.call("GET", f"namespaces/{ns}/tables/{name}"))
        ids["table"] = r.json()["metadata"]["table-uuid"]
    return ids


@pytest.mark.specific
def test_soft_delete_and_undrop(rest, table):
    """With a soft-delete profile a dropped table lands in a recycle bin and can be undropped.

    Checks:
        * after setting the warehouse's delete profile to soft and dropping the table, it is
          listed in ``deleted-tabulars``;
        * after undrop it reads ids 1..3 again.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("lakekeeper", what="soft delete / undrop")
    ns, name, _ = table
    ids = _lk_ids(rest, ns, name)
    wh = ids["warehouse"]
    with gap("soft_delete_and_undrop"):
        ok(
            lk(
                "POST",
                f"warehouse/{wh}/delete-profile",
                json={"delete-profile": {"type": "soft", "expiration-seconds": 3600}},
            )
        )
        try:
            pyiceberg_catalog(WAREHOUSE).drop_table(f"{ns}.{name}")
            deleted = ok(lk("GET", f"warehouse/{wh}/deleted-tabulars")).json()["tabulars"]
            assert ids["table"] in [d["id"] for d in deleted], deleted
            ok(
                lk(
                    "POST",
                    f"warehouse/{wh}/deleted-tabulars/undrop",
                    json={"targets": [{"type": "table", "id": ids["table"]}]},
                )
            )
            assert rows(pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}")) == [1, 2, 3]
        finally:
            ok(
                lk(
                    "POST",
                    f"warehouse/{wh}/delete-profile",
                    json={"delete-profile": {"type": "hard"}},
                )
            )


@pytest.mark.specific
def test_drop_protection(rest, table):
    """A protected table cannot be dropped; after unprotecting it can.

    Checks:
        * with protection on, the drop answers >= 400 and the table still exists;
        * with protection off, the drop with purge succeeds.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("lakekeeper", what="drop protection")
    ns, name, _ = table
    ids = _lk_ids(rest, ns, name)
    path = f"warehouse/{ids['warehouse']}/table/{ids['table']}/protection"
    with gap("drop_protection"):
        ok(lk("POST", path, json={"protected": True}))
        try:
            r = rest.call("DELETE", f"namespaces/{ns}/tables/{name}")
            assert r.status_code >= 400, f"protected table dropped: HTTP {r.status_code}"
            assert pyiceberg_catalog(WAREHOUSE).table_exists(f"{ns}.{name}")
        finally:
            ok(lk("POST", path, json={"protected": False}))
        ok(rest.call("DELETE", f"namespaces/{ns}/tables/{name}", params={"purgeRequested": "true"}))


@pytest.mark.specific
def test_tags(rest, table):
    """A tag definition with allowed values can be applied to a table and read back.

    Checks:
        * a value outside the allowed values is refused (>= 400);
        * an allowed value is set and read back on the table;
        * the tag can be removed.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("lakekeeper", what="tags on catalog objects")
    ns, name, _ = table
    ids = _lk_ids(rest, ns, name)
    tag = f"lab.class_{uuid.uuid4().hex[:6]}"
    with gap("tags"):
        d = ok(
            lk(
                "POST",
                "tag-definition",
                json={
                    "name": tag,
                    "scope": ["table"],
                    "value-kind": "enumerated",
                    "allowed-values": ["public", "pii"],
                },
            )
        ).json()
        try:
            base = f"warehouse/{ids['warehouse']}/table/{ids['table']}/tags"
            bad = lk("PUT", f"{base}/{tag}", json={"value": "secret"})
            assert bad.status_code >= 400, "value outside allowed-values accepted"
            ok(lk("PUT", f"{base}/{tag}", json={"value": "pii"}))
            got = ok(lk("GET", base)).json()["tags"]
            assert any(g.get("name") == tag and g.get("value") == "pii" for g in got), got
            ok(lk("DELETE", f"{base}/{tag}"))
        finally:
            lk("DELETE", f"tag-definition/{d['id']}")


@pytest.mark.specific
def test_warehouse_statistics(rest, table):
    """The warehouse reports how many tables/views it holds.

    Checks:
        * the statistics endpoint answers with at least one entry counting >= 1 table.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("lakekeeper", what="warehouse statistics")
    with gap("warehouse_statistics"):
        stats = ok(lk("GET", f"warehouse/{rest.prefix}/statistics")).json()["stats"]
        assert stats and stats[0]["number-of-tables"] >= 1, stats


@pytest.mark.specific
def test_search_tables(rest, table):
    """Fuzzy search over table names finds the table.

    Checks:
        * a search for the table's name returns a hit with exactly that name and namespace.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("lakekeeper", what="table search")
    ns, name, _ = table
    with gap("search_tables"):
        hits = ok(
            lk("POST", f"warehouse/{rest.prefix}/search-tabular", json={"search": name[:60]})
        ).json()["tabulars"]
        assert any(h["tabular-name"] == name and h["namespace-name"] == [ns] for h in hits), hits[
            :5
        ]


@pytest.mark.specific
def test_namespace_move(rest):
    """A namespace with a table moves under another parent; the table moves with it.

    Checks:
        * the move call answers 2xx;
        * the table under the new nested namespace reads ids 1..3.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
    """
    offered_by("lakekeeper", what="moving namespaces")
    cat = pyiceberg_catalog(WAREHOUSE)
    a, parent = f"mv_{uuid.uuid4().hex[:6]}", f"mvp_{uuid.uuid4().hex[:6]}"
    cat.create_namespace(a)
    cat.create_namespace(parent)
    cat.create_table(f"{a}.t", schema=ROWS.schema).append(ROWS)
    moved = (parent, a)
    try:
        with gap("namespace_move"):
            ok(
                lk(
                    "POST",
                    f"warehouse/{rest.prefix}/namespace/{_lk_ids(rest, a)['namespace']}/move",
                    json={"destination": list(moved)},
                )
            )
            assert rows(cat.load_table((*moved, "t"))) == [1, 2, 3]
    finally:
        for ns in (moved, (a,)):
            if cat.namespace_exists(ns):
                for ident in cat.list_tables(ns):
                    _drop(cat, ident)
                cat.drop_namespace(ns)
        cat.drop_namespace(parent)


@pytest.mark.specific
def test_format_version_policy(rest, namespace, table_name):
    """A warehouse policy that only allows v3 rejects creating a v2 table.

    Checks:
        * with the policy (allowed: [3]) set, creating a v2 table fails with a format-version
          refusal;
        * the policy is reset to v1-v3 afterwards.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
    """
    offered_by("lakekeeper", what="format-version policies")
    cat = pyiceberg_catalog(WAREHOUSE)
    path = f"warehouse/{rest.prefix}/format-version-policy"
    with gap("format_version_policy"):
        ok(lk("POST", path, json={"allowed-format-versions": [3], "default-format-version": 3}))
        try:
            with pytest.raises(Exception, match=r"(?i)format.?version|not allowed|400"):
                cat.create_table(
                    f"{namespace}.{table_name}",
                    schema=ROWS.schema,
                    properties={"format-version": "2"},
                )
        finally:
            ok(lk("POST", path, json={"allowed-format-versions": [1, 2, 3]}))


# ---------------------------------------------------------------------- Polaris extras


def test_generic_tables(rest, namespace, table_name):
    """Base: a non-Iceberg (Delta) table is registered, loaded with its format, kept apart.

    Polaris: Generic Tables API; Unity: its own table API.

    Checks:
        * the Delta table is created (2xx);
        * loading it returns format ``delta`` and the property set at creation;
        * it is NOT listed among the Iceberg tables of the namespace;
        * it can be dropped.
        Lakekeeper (no create endpoint for generic tables) and RustFS: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
    """
    name = f"{table_name}_delta"
    location = f"{PROFILE.location}/{namespace}/{name}"
    if PROFILE.name == "lakekeeper":
        # Checked, not assumed: the generic-tables create call (the shape Polaris uses) must be
        # refused as an unknown route. Should it ever succeed, the claim is outdated.
        body = {"name": name, "format": "delta", "base-location": location}
        r = rest.call("POST", f"namespaces/{namespace}/generic-tables", json=body)
        if r.ok:
            rest.call("DELETE", f"namespaces/{namespace}/generic-tables/{name}")
            pytest.fail("[claim outdated?] Lakekeeper created a generic table - test it here")
        assert r.status_code in (404, 405, 501), (
            f"unexpected answer: HTTP {r.status_code} {r.text[:300]}"
        )
        pytest.skip(
            f"catalog lakekeeper has no API for creating non-Iceberg tables (generic-tables "
            f"create: HTTP {r.status_code}; only grants/tags/protection endpoints exist)"
        )
    offered_by("polaris", "unity", what="non-Iceberg (generic/Delta) tables")
    with gap("generic_tables"):
        if PROFILE.name == "polaris":
            path = f"{rest.prefix}/namespaces/{namespace}/generic-tables"
            body = {
                "name": name,
                "format": "delta",
                "base-location": location,
                "doc": "lab",
                "properties": {"k": "v"},
            }
            ok(polaris("POST", path, api="catalog", json=body))
            drop = lambda: polaris("DELETE", f"{path}/{name}", api="catalog")
            load = lambda: ok(polaris("GET", f"{path}/{name}", api="catalog")).json()["table"]
            fmt = lambda t: t["format"]
        else:  # unity
            full = f"{WAREHOUSE}.{namespace}.{name}"
            col = {
                "name": "id",
                "type_name": "LONG",
                "type_text": "bigint",
                "position": 0,
                "nullable": True,
                "type_json": '{"name":"id","type":"long","nullable":true,"metadata":{}}',
            }
            body = {
                "name": name,
                "catalog_name": WAREHOUSE,
                "schema_name": namespace,
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": location,
                "columns": [col],
                "properties": {"k": "v"},
            }
            ok(uc("POST", "tables", json=body), 200)
            drop = lambda: uc("DELETE", f"tables/{full}")
            load = lambda: ok(uc("GET", f"tables/{full}"), 200).json()
            fmt = lambda t: t["data_source_format"].lower()
        try:
            got = load()
            assert fmt(got) == "delta" and got.get("properties", {}).get("k") == "v", got
            iceberg = [t[-1] for t in pyiceberg_catalog(WAREHOUSE).list_tables(namespace)]
            assert name not in iceberg, "non-Iceberg table listed as Iceberg table"
        finally:
            drop()


@pytest.mark.specific
def test_policies(rest, table):
    """A compaction policy attached to a namespace applies to (is inherited by) its tables.

    Checks:
        * a policy is created and attached to the namespace (2xx);
        * the applicable policies of the table include it, marked as inherited.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("polaris", what="policies")
    ns, name, _ = table
    pol = f"compact_{uuid.uuid4().hex[:6]}"
    base = f"{rest.prefix}/namespaces/{ns}/policies"
    with gap("policies"):
        ok(
            polaris(
                "POST",
                base,
                api="catalog",
                json={
                    "name": pol,
                    "type": "system.data-compaction",
                    "content": '{"version": "2025-02-03", "enable": true}',
                },
            )
        )
        try:
            ok(
                polaris(
                    "PUT",
                    f"{base}/{pol}/mappings",
                    api="catalog",
                    json={"target": {"type": "namespace", "path": [ns]}},
                ),
                200,
                204,
            )
            r = ok(
                polaris(
                    "GET",
                    f"{rest.prefix}/applicable-policies",
                    api="catalog",
                    params={"namespace": ns, "target-name": name},
                )
            )
            found = r.json().get("applicable-policies", [])
            assert any(p["name"] == pol and p.get("inherited") for p in found), found
        finally:
            polaris(
                "POST",
                f"{base}/{pol}/mappings",
                api="catalog",
                json={"target": {"type": "namespace", "path": [ns]}},
            )
            polaris("DELETE", f"{base}/{pol}", api="catalog", params={"detach-all": "true"})


FED_PORT = 18282  # not 18181: that is test_robustness' fault proxy
LAKEKEEPER = "http://lakekeeper:8181"


@pytest.fixture(scope="module")
def plain_proxy():
    """``http://dev:18282`` -> Lakekeeper with ``Accept-Encoding: identity`` (no compression).

    Polaris' HTTP client asks for brotli, but the Polaris image has no native brotli4j
    library for x86_64, so it cannot read Lakekeeper's compressed answers (see
    ``test_federation_direct``). Through this proxy the federation itself can be tested.

    Yields:
        The proxy URL.
    """
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        """Forward every request to Lakekeeper with ``Accept-Encoding: identity``."""

        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            """Silence the request log.

            Args:
                args: Log arguments (ignored).
            """

        def _handle(self):
            """Forward one request (chunked bodies decoded) and relay the answer."""
            if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
                body = b""
                while (size := int(self.rfile.readline().strip() or b"0", 16)) > 0:
                    body += self.rfile.read(size)
                    self.rfile.readline()
                self.rfile.readline()
            else:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
            skip = {"host", "accept-encoding", "content-length", "transfer-encoding", "connection"}
            headers = {k: v for k, v in self.headers.items() if k.lower() not in skip}
            headers["Accept-Encoding"] = "identity"
            r = requests.request(
                self.command, LAKEKEEPER + self.path, data=body, headers=headers, timeout=60
            )
            self.send_response(r.status_code)
            for k, v in r.headers.items():
                if k.lower() not in (
                    "content-encoding",
                    "content-length",
                    "transfer-encoding",
                    "connection",
                ):
                    self.send_header(k, v)
            # Lakekeeper's /v1/config overrides "uri" with its own address - keep the client
            # on the proxy.
            content = r.content.replace(LAKEKEEPER.encode(), f"http://dev:{FED_PORT}".encode())
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(content)

        do_GET = do_POST = do_DELETE = do_HEAD = do_PUT = _handle

    server = ThreadingHTTPServer(("0.0.0.0", FED_PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://dev:{FED_PORT}"
    server.shutdown()


def _external_catalog(name: str, upstream: str) -> None:
    """Polaris EXTERNAL catalog ``name`` forwarding to Lakekeeper's ``demo-sts`` at ``upstream``.

    Both federation catalogs cover the same location, which Polaris refuses to overlap, so
    any existing one is removed first.

    Args:
        name: Table name.
        upstream: Base URL of Lakekeeper as Polaris reaches it.
    """
    for old in ("fed_lk", "fed_lk_direct"):
        ok(polaris("DELETE", f"catalogs/{old}"), 204, 404)
    ok(
        polaris(
            "POST",
            "catalogs",
            json={
                "catalog": {
                    "name": name,
                    "type": "EXTERNAL",
                    "properties": {"default-base-location": "s3://warehouse/demo-sts"},
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": ["s3://warehouse/demo-sts"],
                        "endpoint": SETTINGS.s3_endpoint,
                        "endpointInternal": SETTINGS.s3_endpoint,
                        "pathStyleAccess": True,
                        "region": SETTINGS.s3_region,
                    },
                    "connectionConfigInfo": {
                        "connectionType": "ICEBERG_REST",
                        "uri": f"{upstream}/catalog",
                        "remoteCatalogName": "demo-sts",
                        # Lakekeeper runs without authentication here; Polaris requires some auth type.
                        "authenticationParameters": {
                            "authenticationType": "BEARER",
                            "bearerToken": "lab",
                        },
                    },
                }
            },
        )
    )
    ok(
        polaris(
            "PUT",
            f"catalogs/{name}/catalog-roles/catalog_admin/grants",
            json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        )
    )


def _federated(name: str):
    """PyIceberg client on Polaris' EXTERNAL catalog.

    Args:
        name: Catalog name.

    Returns:
        The PyIceberg catalog.
    """
    from iceberg_lab.engines import pyiceberg_catalog as _pc

    return _pc(name)  # Polaris profile: OAuth + vended credentials, warehouse = catalog name


@pytest.fixture
def lk_table():
    """A table created directly in Lakekeeper (``demo-sts``): ``(lakekeeper catalog, ns)``.

    Yields:
        ``(Lakekeeper catalog, namespace)``.
    """
    from pyiceberg.catalog import load_catalog

    lk = load_catalog("lk-direct", type="rest", uri=f"{LAKEKEEPER}/catalog", warehouse="demo-sts")
    ns = f"fed_{uuid.uuid4().hex[:6]}"
    lk.create_namespace(ns)
    lk.create_table(f"{ns}.t", schema=ROWS.schema).append(ROWS)
    yield lk, ns
    for ident in lk.list_tables(ns):
        lk.purge_table(ident)
    lk.drop_namespace(ns)


@pytest.mark.specific
def test_federation_direct(lk_table):
    """Polaris federates to Lakekeeper directly: read a Lakekeeper table via Polaris.

    Checks:
        * an EXTERNAL catalog pointing at Lakekeeper is created;
        * the Lakekeeper table read through Polaris returns ids 1..3.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        lk_table: ``(Lakekeeper catalog, namespace)`` - table ``t`` (ids 1..3) created directly
            in Lakekeeper, the federation target.
    """
    offered_by("polaris", what="catalog federation")
    _, ns = lk_table
    with gap("federation_direct"):
        _external_catalog("fed_lk_direct", LAKEKEEPER)
        fed = _federated("fed_lk_direct")
        assert rows(fed.load_table(f"{ns}.t")) == [1, 2, 3]


@pytest.mark.specific
def test_federation(lk_table, plain_proxy):
    """Through Polaris (EXTERNAL catalog -> Lakekeeper): read, append, create and drop.

    Checks (each change verified in Lakekeeper itself):
        * the Lakekeeper namespace is listed through Polaris and the table reads ids 1..3;
        * an append through Polaris shows up in Lakekeeper (ids twice);
        * a table created through Polaris exists in Lakekeeper, and is gone after a drop
          through Polaris.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        lk_table: ``(Lakekeeper catalog, namespace)`` - table ``t`` (ids 1..3) created directly
            in Lakekeeper, the federation target.
        plain_proxy: Proxy in front of Lakekeeper without response compression (Polaris'
            client cannot read Lakekeeper's brotli answers, see test_federation_direct).
    """
    offered_by("polaris", what="catalog federation")
    lk, ns = lk_table
    with gap("federation"):
        _external_catalog("fed_lk", plain_proxy)
        fed = _federated("fed_lk")
        assert ns in [n[0] for n in fed.list_namespaces()]
        t = fed.load_table(f"{ns}.t")
        assert rows(t) == [1, 2, 3]
        t.append(ROWS)
        assert rows(lk.load_table(f"{ns}.t")) == [1, 1, 2, 2, 3, 3]
        fed.create_table(f"{ns}.t2", schema=ROWS.schema)
        assert lk.table_exists(f"{ns}.t2")
        fed.drop_table(f"{ns}.t2")
        assert not lk.table_exists(f"{ns}.t2")


# =========================================================================== RustFS extras


def _rustfs_tables(method: str, path: str) -> requests.Response:
    """Table-bucket API of the RustFS catalog (``/iceberg/v1/buckets/...``), signed as root.

    Args:
        method: HTTP method.
        path: Path relative to the API base.

    Returns:
        The response.
    """
    return requests.request(
        method, f"{SETTINGS.catalog_uri}/v1/buckets/{path}", auth=catalog_auth(), timeout=20
    )


def _s3_root():
    """boto3 S3 client with the RustFS root keys.

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


@pytest.mark.specific
def test_rustfs_table_bucket_lifecycle():
    """An ordinary S3 bucket becomes a catalog warehouse, is used, and is switched off.

    Fixed bucket name: RustFS cannot disable or delete a table bucket that ever held a table
    (see the gap), so a fresh bucket per run would pile up undeletable buckets.

    Checks:
        * enabling answers 2xx and the bucket reports enabled, with itself as warehouse;
        * a table in it reads ids 1..3;
        * disabling answers 2xx and the bucket reports not enabled.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("rustfs", what="table buckets (enable/disable a bucket as warehouse)")
    from botocore.exceptions import ClientError

    bucket = "lab-tb-lifecycle"
    s3 = _s3_root()
    try:
        s3.create_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    r = _rustfs_tables("PUT", bucket)
    assert r.ok, f"enable: {r.status_code} {r.text[:200]}"
    info = _rustfs_tables("GET", bucket).json()
    assert info["enabled"] is True and info["warehouse"] == bucket, info
    cat = pyiceberg_catalog(bucket)
    ns = f"tb_{uuid.uuid4().hex[:6]}"
    cat.create_namespace(ns)
    cat.create_table(f"{ns}.t", schema=ROWS.schema).append(ROWS)
    assert rows(cat.load_table(f"{ns}.t")) == [1, 2, 3]
    cat.drop_table(f"{ns}.t")
    cat.drop_namespace(ns)
    with gap("table_bucket_lifecycle"):
        r = _rustfs_tables("DELETE", bucket)
        assert r.ok, f"disable: {r.status_code} {r.text[:200]}"
        assert _rustfs_tables("GET", bucket).json()["enabled"] is False


@pytest.mark.specific
def test_rustfs_compat_endpoint(namespace):
    """The same catalog under the second endpoint prefix ``/_iceberg/v1``.

    Checks:
        * both ``/iceberg/v1`` and ``/_iceberg/v1`` list the namespaces (2xx);
        * the session namespace is in the compat listing;
        * both list exactly the same namespaces.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        namespace: Fresh namespace of the test session.
    """
    offered_by("rustfs", what="a compatibility endpoint (/_iceberg/v1)")
    root = SETTINGS.catalog_base.rstrip("/")
    wh = WAREHOUSE
    main = requests.get(f"{root}/iceberg/v1/{wh}/namespaces", auth=catalog_auth(), timeout=20)
    compat = requests.get(f"{root}/_iceberg/v1/{wh}/namespaces", auth=catalog_auth(), timeout=20)
    assert main.ok and compat.ok, (main.status_code, compat.status_code, compat.text[:200])
    assert [namespace] in compat.json()["namespaces"]
    assert sorted(map(tuple, main.json()["namespaces"])) == sorted(
        map(tuple, compat.json()["namespaces"])
    )


@pytest.mark.specific
def test_rustfs_unknown_table_bucket_is_not_found():
    """Asking about a bucket that does not exist says so (404), not "disabled".

    Checks:
        * GET of a missing table bucket answers 404;
        * DELETE of a missing table bucket answers 404.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("rustfs", what="table buckets (enable/disable a bucket as warehouse)")
    missing = f"missing-{uuid.uuid4().hex[:8]}"
    with gap("rustfs_unknown_bucket"):
        r = _rustfs_tables("GET", missing)
        assert r.status_code == 404, f"GET of a missing bucket: {r.status_code} {r.text[:160]}"
        r = _rustfs_tables("DELETE", missing)
        assert r.status_code == 404, f"DELETE of a missing bucket: {r.status_code} {r.text[:160]}"


# =========================================================================== Unity extras


def uc(method: str, path: str, **kw) -> requests.Response:
    """Unity Catalog's own REST API (``/api/2.1/unity-catalog``).

    Args:
        method: HTTP method.
        path: Path relative to the API base.
        kw: Passed to ``requests.request`` (json, params, headers, ...).

    Returns:
        The response.
    """
    return requests.request(
        method,
        f"{SETTINGS.catalog_base.rstrip('/')}/api/2.1/unity-catalog/{path.lstrip('/')}",
        headers=catalog_headers(SETTINGS),
        timeout=30,
        **kw,
    )


def _s3_with(creds: dict):
    """boto3 client from Unity's ``aws_temp_credentials``.

    Args:
        creds: ``aws_temp_credentials`` of a Unity answer.

    Returns:
        The client.
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        aws_session_token=creds["session_token"],
    )


def _split(url: str) -> tuple[str, str]:
    """``s3://bucket/key`` -> ``(bucket, key)``.

    Args:
        url: ``s3://bucket/key``.

    Returns:
        ``(bucket, key)``.
    """
    bucket, _, key = url.removeprefix("s3://").partition("/")
    return bucket, key


@pytest.fixture
def uc_schema(namespace):
    """The session namespace as Unity schema ``lab.<namespace>`` (exists already).

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``lab.<namespace>``.
    """
    offered_by("unity", what="Unity's own API (volumes, functions, models, temporary credentials)")
    return f"{WAREHOUSE}.{namespace}"


@pytest.mark.specific
def test_unity_volumes(uc_schema):
    """Managed volume: write with WRITE_VOLUME credentials, read with READ_VOLUME ones.

    Checks:
        * a file written with the volume's WRITE_VOLUME credentials is read back unchanged with
          READ_VOLUME credentials;
        * READ_VOLUME credentials cannot write (403 / AccessDenied).
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        uc_schema: The session namespace as Unity schema ``lab.<namespace>``.
    """
    from botocore.exceptions import ClientError

    cat, schema = uc_schema.split(".")
    name = f"v_{uuid.uuid4().hex[:6]}"
    v = ok(
        uc(
            "POST",
            "volumes",
            json={
                "catalog_name": cat,
                "schema_name": schema,
                "name": name,
                "volume_type": "MANAGED",
            },
        ),
        200,
    ).json()
    try:
        bucket, key = _split(v["storage_location"])
        w = ok(
            uc(
                "POST",
                "temporary-volume-credentials",
                json={"volume_id": v["volume_id"], "operation": "WRITE_VOLUME"},
            ),
            200,
        ).json()
        _s3_with(w["aws_temp_credentials"]).put_object(
            Bucket=bucket, Key=f"{key}/hello.txt", Body=b"hi"
        )
        rd = ok(
            uc(
                "POST",
                "temporary-volume-credentials",
                json={"volume_id": v["volume_id"], "operation": "READ_VOLUME"},
            ),
            200,
        ).json()
        s3r = _s3_with(rd["aws_temp_credentials"])
        assert s3r.get_object(Bucket=bucket, Key=f"{key}/hello.txt")["Body"].read() == b"hi"
        with pytest.raises(ClientError) as exc:
            s3r.put_object(Bucket=bucket, Key=f"{key}/evil.txt", Body=b"x")
        assert exc.value.response["Error"]["Code"] in ("403", "AccessDenied")
    finally:
        uc("DELETE", f"volumes/{uc_schema}.{name}")


@pytest.mark.specific
def test_unity_functions(uc_schema):
    """Register a (Python) function in the catalog; it can be read back and listed.

    Checks:
        * creating the function answers 200;
        * reading it back returns its definition and data type INT;
        * it is in the schema's function list.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        uc_schema: The session namespace as Unity schema ``lab.<namespace>``.
    """
    cat, schema = uc_schema.split(".")
    name = f"f_{uuid.uuid4().hex[:6]}"
    info = {
        "name": name,
        "catalog_name": cat,
        "schema_name": schema,
        "input_params": {
            "parameters": [
                {
                    "name": "x",
                    "type_text": "int",
                    "type_json": '{"name":"x","type":"integer","nullable":false,"metadata":{}}',
                    "type_name": "INT",
                    "position": 0,
                }
            ]
        },
        "data_type": "INT",
        "full_data_type": "INT",
        "return_params": {"parameters": []},
        "routine_body": "EXTERNAL",
        "routine_definition": "return x + 1",
        "parameter_style": "S",
        "is_deterministic": True,
        "sql_data_access": "NO_SQL",
        "is_null_call": False,
        "security_type": "DEFINER",
        "specific_name": name,
        "external_language": "python",
    }
    ok(uc("POST", "functions", json={"function_info": info}), 200)
    try:
        got = ok(uc("GET", f"functions/{uc_schema}.{name}"), 200).json()
        assert got["routine_definition"] == "return x + 1" and got["data_type"] == "INT"
        listed = ok(
            uc("GET", "functions", params={"catalog_name": cat, "schema_name": schema}), 200
        ).json()
        assert name in [f["name"] for f in listed.get("functions", [])]
    finally:
        uc("DELETE", f"functions/{uc_schema}.{name}")


@pytest.mark.specific
def test_unity_models(uc_schema):
    """A registered ML model gets a version that is finalized (PENDING_REGISTRATION -> READY).

    Checks:
        * model and version are created (200);
        * the new version is PENDING_REGISTRATION;
        * after finalizing it is READY.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        uc_schema: The session namespace as Unity schema ``lab.<namespace>``.
    """
    cat, schema = uc_schema.split(".")
    name = f"m_{uuid.uuid4().hex[:6]}"
    ok(uc("POST", "models", json={"catalog_name": cat, "schema_name": schema, "name": name}), 200)
    try:
        v = ok(
            uc(
                "POST",
                "models/versions",
                json={
                    "catalog_name": cat,
                    "schema_name": schema,
                    "model_name": name,
                    "source": "s3://unitylab/x",
                    "run_id": "r1",
                },
            ),
            200,
        ).json()
        assert v["status"] == "PENDING_REGISTRATION"
        f = ok(
            uc(
                "PATCH",
                f"models/{uc_schema}.{name}/versions/{v['version']}/finalize",
                json={"full_name": f"{uc_schema}.{name}", "version": v["version"]},
            ),
            200,
        ).json()
        assert f["status"] == "READY", f
    finally:
        uc("DELETE", f"models/{uc_schema}.{name}", params={"force": "true"})


@pytest.mark.specific
def test_unity_temporary_path_credentials(table):
    """``PATH_READ`` credentials for a table's location read its files but cannot write.

    Checks:
        * Unity issues PATH_READ credentials for the table's location (200);
        * with them a HEAD of the metadata file succeeds;
        * a PUT into the table's data directory is refused (403 / AccessDenied).
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    from botocore.exceptions import ClientError

    offered_by("unity", what="Unity's own API (volumes, functions, models, temporary credentials)")
    *_, t = table
    creds = ok(
        uc(
            "POST",
            "temporary-path-credentials",
            json={"url": t.location(), "operation": "PATH_READ"},
        ),
        200,
    ).json()
    s3 = _s3_with(creds["aws_temp_credentials"])
    bucket, key = _split(t.metadata_location)
    s3.head_object(Bucket=bucket, Key=key)
    with pytest.raises(ClientError) as exc:
        s3.put_object(Bucket=bucket, Key=key.rsplit("/", 2)[0] + "/data/evil.bin", Body=b"x")
    assert exc.value.response["Error"]["Code"] in ("403", "AccessDenied")


# ----------------------------------------------------------------- RustFS: catalog-side table API


def _rfs_table(ns: str, name: str, method: str, path: str, body=None) -> requests.Response:
    """RustFS' own per-table API (``/iceberg/v1/<bucket>/namespaces/<ns>/tables/<t>/...``).

    Args:
        ns: Namespace.
        name: Table name.
        method: HTTP method.
        path: Path relative to the API base.
        body: JSON request body.

    Returns:
        The response.
    """
    return requests.request(
        method,
        f"{SETTINGS.catalog_uri}/v1/{WAREHOUSE}/namespaces/{ns}/tables/{name}{path}",
        json=body,
        auth=catalog_auth(),
        timeout=30,
    )


@pytest.mark.specific
def test_rustfs_table_maintenance(table):
    """Maintenance by the catalog: the per-table configuration can be changed and is kept.

    Checks:
        * the maintenance config is readable, changed (background-enabled) and read back
          changed;
        * the maintenance metadata endpoint answers 2xx (known gap where it does not).
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("rustfs", what="table maintenance run by the catalog")
    ns, name, _ = table
    cfg = ok(_rfs_table(ns, name, "GET", "/maintenance/config")).json()
    ok(_rfs_table(ns, name, "PUT", "/maintenance/config", cfg | {"background-enabled": True}))
    assert (
        ok(_rfs_table(ns, name, "GET", "/maintenance/config")).json()["background-enabled"] is True
    )
    with gap("rustfs_maintenance_metadata"):
        ok(_rfs_table(ns, name, "GET", "/maintenance/metadata"))


@pytest.mark.specific
def test_rustfs_refs_api(table):
    """The refs API reports the table's current snapshot.

    Checks:
        * the refs answer contains the current snapshot id the Iceberg API serves.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("rustfs", what="a refs API per table")
    ns, name, t = table
    refs = ok(_rfs_table(ns, name, "GET", "/refs")).json()
    assert str(t.refresh().current_snapshot().snapshot_id) in json.dumps(refs), refs


@pytest.mark.specific
def test_rustfs_metadata_location_api(table):
    """The metadata location is readable directly, with a version token for safe updates.

    Checks:
        * the answer's metadata location equals the one the Iceberg API serves, and a version
          token is present.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("rustfs", what="reading/setting the metadata location directly")
    ns, name, t = table
    got = ok(_rfs_table(ns, name, "GET", "/metadata-location")).json()
    assert got["metadata-location"] == t.refresh().metadata_location and got["version-token"], got


@pytest.mark.specific
def test_rustfs_table_admin(table):
    """Diagnostics and export describe the table; rollback to older metadata restores it.

    Checks:
        * after an append the table reads ids 1..4;
        * diagnostics and export answers name the table;
        * rollback to the earlier metadata file (with version token) makes it read ids 1..3;
        * the recovery endpoint answers 2xx (known gap where it does not).
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("rustfs", what="table export/diagnostics/rollback")
    ns, name, t = table
    first = t.refresh().metadata_location
    t.append(pa.table({"id": [4]}))
    assert rows(t.refresh()) == [1, 2, 3, 4]
    assert name in json.dumps(ok(_rfs_table(ns, name, "GET", "/catalog/diagnostics")).json())
    assert name in json.dumps(ok(_rfs_table(ns, name, "GET", "/catalog/export")).json())
    token = ok(_rfs_table(ns, name, "GET", "/metadata-location")).json()["version-token"]
    with gap("rustfs_rollback"):
        body = {"metadata-location": first, "version-token": token}
        ok(_rfs_table(ns, name, "POST", "/catalog/rollback", body))
        assert rows(pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}")) == [1, 2, 3]
    with gap("rustfs_recovery"):
        ok(_rfs_table(ns, name, "GET", "/catalog/recovery"))


@pytest.mark.specific
def test_rustfs_catalog_backup():
    """Catalog-wide administration: directory status, index backfill and backup.

    Checks:
        * the directory status answer has a ``phase``;
        * the warehouse-index backfill answers 2xx;
        * a backup can be read (2xx; known gap where not).
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("rustfs", what="catalog backup/restore/migration")
    base = f"{SETTINGS.catalog_uri}/v1/{WAREHOUSE}/catalog"
    assert "phase" in ok(requests.get(f"{base}/directory", auth=catalog_auth(), timeout=30)).json()
    ok(requests.post(f"{base}/warehouse-index/backfill", json={}, auth=catalog_auth(), timeout=60))
    with gap("rustfs_backup"):
        ok(requests.get(f"{base}/backup", auth=catalog_auth(), timeout=30))


# ----------------------------------------------------------------- adopting external metadata


def _foreign_table(location: str):
    """A table written by ANOTHER catalog (throw-away SQL catalog) directly on the storage.

    Args:
        location: Location to write the foreign table to.

    Returns:
        The foreign table (PyIceberg).
    """
    from pyiceberg.catalog.sql import SqlCatalog

    from iceberg_lab.engines import static_s3_props

    foreign = SqlCatalog(
        "foreign", uri="sqlite:///:memory:", warehouse=location, **static_s3_props(SETTINGS)
    )
    foreign.create_namespace("x")
    t = foreign.create_table("x.t", schema=ROWS.schema, location=location)
    t.append(ROWS)
    return t


def test_external_table_sync(namespace):
    """Base: the catalog takes over a table whose metadata another system writes.

    Polaris: notifications to an EXTERNAL catalog; RustFS: operator sync of the metadata pointer.

    Checks (in ``_sync_polaris`` / ``_sync_rustfs``):
        * the catalog adopts the externally written table and serves its rows;
        * after the external system writes a new metadata file, the catalog follows it.
        Lakekeeper and Unity: not offered.

    Args:
        namespace: Fresh namespace of the test session.
    """
    offered_by(
        "polaris", "rustfs", what="adopting tables from external metadata (notifications/sync)"
    )
    if PROFILE.name == "polaris":
        _sync_polaris()
    else:
        _sync_rustfs(namespace)


def _sync_polaris():
    """Polaris notifications: create, then update an EXTERNAL table from foreign metadata; check it follows."""
    import time

    name = f"ext_{uuid.uuid4().hex[:6]}"
    loc = f"s3://{SETTINGS.s3_bucket}/{name}"
    ok(
        polaris(
            "POST",
            "catalogs",
            json={
                "catalog": {
                    "name": name,
                    "type": "EXTERNAL",
                    "properties": {"default-base-location": loc},
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": [loc],
                        "endpoint": SETTINGS.s3_endpoint,
                        "endpointInternal": SETTINGS.s3_endpoint,
                        "pathStyleAccess": True,
                        "region": SETTINGS.s3_region,
                    },
                }
            },
        )
    )
    ok(
        polaris(
            "PUT",
            f"catalogs/{name}/catalog-roles/catalog_admin/grants",
            json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        )
    )
    cat = pyiceberg_catalog(name)
    try:
        cat.create_namespace("n")
        t = _foreign_table(f"{loc}/n/t")
        url = "n/tables/t/notifications"

        def notify(kind, table):
            md = json.loads(table.io.new_input(table.metadata_location).open().read())
            payload = {
                "table-name": "t",
                "timestamp": int(time.time() * 1000),
                "table-uuid": md["table-uuid"],
                "metadata-location": table.metadata_location,
                "metadata": md,
            }
            ok(
                polaris(
                    "POST",
                    f"{name}/namespaces/{url}",
                    api="iceberg",
                    json={"notification-type": kind, "payload": payload},
                )
            )

        with gap("external_table_sync"):
            notify("CREATE", t)
            assert rows(cat.load_table("n.t")) == [1, 2, 3]
            t.append(ROWS)
            notify("UPDATE", t.refresh())
            assert rows(cat.load_table("n.t")) == [1, 1, 2, 2, 3, 3]
    finally:
        for i in cat.list_tables("n"):
            cat.drop_table(i)
        cat.drop_namespace("n")
        polaris("DELETE", f"catalogs/{name}")


def _sync_rustfs(namespace):
    """RustFS operator sync: point the catalog at foreign metadata, then at a newer version; check it follows.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).
    """
    cat = pyiceberg_catalog(WAREHOUSE)
    name = f"sync_{uuid.uuid4().hex[:6]}"
    cat.create_table(f"{namespace}.{name}", schema=ROWS.schema)
    t = _foreign_table(f"{PROFILE.location}/{namespace}/{name}_ext")
    url = f"{SETTINGS.catalog_uri}/v1/{WAREHOUSE}/namespaces/{namespace}/tables/{name}/catalog/external/sync"
    with gap("external_table_sync"):
        body = {
            "catalog": "polaris",
            "external-namespace": "x",
            "external-table": "t",
            "metadata-location": t.metadata_location,
        }
        ok(requests.post(url, json=body, auth=catalog_auth(), timeout=30))
        assert rows(cat.load_table(f"{namespace}.{name}")) == [1, 2, 3]
        t.append(ROWS)
        ok(
            requests.post(
                url,
                json=body | {"metadata-location": t.refresh().metadata_location},
                auth=catalog_auth(),
                timeout=30,
            )
        )
        assert rows(cat.load_table(f"{namespace}.{name}")) == [1, 1, 2, 2, 3, 3]


# ----------------------------------------------------------------- Lakekeeper extras (management)


def _lk_wid(name: str = WAREHOUSE) -> str:
    """Lakekeeper warehouse id by name.

    Args:
        name: Warehouse name.

    Returns:
        The id.
    """
    return {w["name"]: w["id"] for w in ok(lk("GET", "warehouse")).json()["warehouses"]}[name]


@pytest.mark.specific
def test_lakekeeper_storage_validation():
    """A warehouse's storage profile can be validated before use.

    Checks:
        * the warehouse's own (working) profile validates as valid;
        * a profile with a wrong bucket validates as not valid, with a failed check listed.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("lakekeeper", what="validating storage access")
    wid = _lk_wid()
    w = ok(lk("GET", f"warehouse/{wid}")).json()
    from iceberg_lab.bootstrap import warehouse_payload

    cred = warehouse_payload(SETTINGS, "x", True)["storage-credential"]
    path = f"warehouse/{wid}/storage/validate-profile"
    good = ok(
        lk("POST", path, json={"storage-profile": w["storage-profile"], "storage-credential": cred})
    ).json()
    assert good["valid"] is True, good
    bad = w["storage-profile"] | {"bucket": f"missing-{uuid.uuid4().hex[:8]}"}
    res = ok(lk("POST", path, json={"storage-profile": bad, "storage-credential": cred})).json()
    assert res["valid"] is False and any(c["status"] == "failed" for c in res["checks"]), res


@pytest.mark.specific
def test_lakekeeper_permission_check():
    """'What may alice do here?' - batch check on the permission instance.

    Checks (alice has read on t_select only):
        * read_data on t_select: allowed;
        * write_data on t_select: not allowed;
        * read_data on t_none: not allowed.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("lakekeeper", what="checking allowed actions")
    from authz_backends import LakekeeperAuthz

    b = LakekeeperAuthz()
    ns = b.setup_namespace()
    try:
        wid = b.warehouse_ids[b.wh]

        def check(table, action):
            return {
                "id": f"{table}:{action}",
                "operation": {
                    "table": {
                        "warehouse-id": wid,
                        "namespace": [ns],
                        "table": table,
                        "action": {"action": action},  # internally tagged in this API
                    }
                },
            }

        r = requests.post(
            f"{b.base}/management/v1/action/batch-check",
            headers=b.bearer("alice"),
            timeout=20,
            json={
                "checks": [
                    check("t_select", "read_data"),
                    check("t_select", "write_data"),
                    check("t_none", "read_data"),
                ]
            },
        )
        got = {x["id"]: x["allowed"] for x in ok(r).json()["results"]}
        assert got == {
            "t_select:read_data": True,
            "t_select:write_data": False,
            "t_none:read_data": False,
        }, got
    finally:
        b.drop_namespace(ns)


@pytest.mark.specific
def test_lakekeeper_projects():
    """Projects (tenants): create, rename, list, delete.

    Checks:
        * a project is created (201) and renamed; its name reads back renamed;
        * it is in the project list;
        * after deletion it is no longer listed.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("lakekeeper", what="projects (tenants)")
    name = f"lab-proj-{uuid.uuid4().hex[:6]}"
    pid = ok(lk("POST", "project", json={"project-name": name}), 201).json()["project-id"]
    h = {"x-project-id": pid}
    try:
        ok(lk("POST", "project/rename", json={"new-name": name + "-r"}, headers=h))
        assert ok(lk("GET", "project", headers=h)).json()["project-name"] == name + "-r"
        assert pid in [p["project-id"] for p in ok(lk("GET", "project-list")).json()["projects"]]
    finally:
        ok(lk("DELETE", "project", headers=h))
    assert pid not in [p["project-id"] for p in ok(lk("GET", "project-list")).json()["projects"]]


@pytest.mark.specific
def test_lakekeeper_maintenance_mode():
    """Read-only maintenance mode: reads work, writes are refused; switched off, writes work.

    Needs instance-admin rights: runs on the permission instance as lab-admin.

    Checks:
        * in read-only mode listing namespaces works and creating one is refused
          (maintenance / read-only / 503 / 405 / forbidden);
        * after switching it off, creating and dropping a namespace works.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("lakekeeper", what="a server maintenance mode")
    from authz_backends import LakekeeperAuthz, pyiceberg_for

    b = LakekeeperAuthz()
    b.ensure()
    url, h = f"{b.base}/management/v1/server/maintenance-mode", b.bearer("lab-admin")
    cat, ns = pyiceberg_for(b.admin()), f"mm_{uuid.uuid4().hex[:6]}"
    ok(requests.post(url, json={"mode": "read-only"}, headers=h, timeout=20))
    try:
        cat.list_namespaces()
        with pytest.raises(Exception, match=r"(?i)maintenance|read.only|503|405|forbidden"):
            cat.create_namespace(ns)
    finally:
        ok(requests.post(url, json={"mode": "off"}, headers=h, timeout=20))
    cat.create_namespace(ns)
    cat.drop_namespace(ns)


@pytest.mark.specific
def test_lakekeeper_endpoint_statistics(namespace):
    """Call statistics per endpoint include the calls just made.

    Checks:
        * after listing namespaces, the endpoint statistics mention the namespaces endpoint.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        namespace: Fresh namespace of the test session.
    """
    offered_by("lakekeeper", what="endpoint statistics")
    pyiceberg_catalog(WAREHOUSE).list_namespaces()
    stats = ok(
        lk(
            "POST",
            "endpoint-statistics",
            json={"warehouse": {"type": "warehouse-id", "id": _lk_wid()}},
        )
    ).json()
    assert "namespaces" in json.dumps(stats.get("stats", stats)), json.dumps(stats)[:300]


@pytest.mark.specific
def test_lakekeeper_task_queue_config():
    """The purge queue's configuration can be set and read back.

    Checks:
        * a changed max-seconds-since-last-heartbeat (1234) is read back; the previous value is
          restored afterwards.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("lakekeeper", what="task queue configuration")
    wid = _lk_wid()
    path = f"warehouse/{wid}/task-queue/tabular_purge/config"
    before = ok(lk("GET", path)).json()
    try:
        ok(lk("POST", path, json={"queue-config": {}, "max-seconds-since-last-heartbeat": 1234}))
        assert ok(lk("GET", path)).json()["max-seconds-since-last-heartbeat"] == 1234
    finally:
        ok(
            lk(
                "POST",
                path,
                json={
                    "queue-config": {},
                    "max-seconds-since-last-heartbeat": before.get(
                        "max-seconds-since-last-heartbeat"
                    ),
                },
            )
        )


@pytest.mark.specific
def test_lakekeeper_managed_by():
    """A warehouse can be marked as managed by the instance admin (and back).

    Checks:
        * after setting ``managed-by: instance-admin`` the warehouse reports it;
        * setting it back to self-managed answers 2xx.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("lakekeeper", what="marking a warehouse as externally managed")
    from authz_backends import LakekeeperAuthz

    from iceberg_lab.bootstrap import warehouse_payload

    b = LakekeeperAuthz()
    b.ensure()
    api, h = f"{b.base}/management/v1", b.bearer("lab-admin")
    name = f"mb-{uuid.uuid4().hex[:6]}"
    r = requests.post(
        f"{api}/warehouse", json=warehouse_payload(SETTINGS, name, True), headers=h, timeout=20
    )
    wid = ok(r, 201).json()["warehouse-id"]
    try:
        ok(
            requests.post(
                f"{api}/warehouse/{wid}/managed-by",
                json={"managed-by": "instance-admin"},
                headers=h,
                timeout=20,
            )
        )
        assert (
            ok(requests.get(f"{api}/warehouse/{wid}", headers=h, timeout=20))
            .json()
            .get("managed-by")
            == "instance-admin"
        )
        ok(
            requests.post(
                f"{api}/warehouse/{wid}/managed-by",
                json={"managed-by": "self-managed"},
                headers=h,
                timeout=20,
            )
        )
    finally:
        requests.delete(f"{api}/warehouse/{wid}", headers=h, timeout=20)


# ----------------------------------------------------------------- Unity extras


@pytest.mark.specific
def test_unity_schema_api(uc_schema):
    """Unity's own schema API: create, change, list, delete - visible as Iceberg namespace.

    Checks:
        * a schema is created (200) and its comment changed and read back;
        * it is in the schema list and appears as namespace in Iceberg REST;
        * after deletion GET answers 404.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        uc_schema: The session namespace as Unity schema ``lab.<namespace>``.
    """
    cat_name = uc_schema.split(".")[0]
    name = f"us_{uuid.uuid4().hex[:6]}"
    ok(uc("POST", "schemas", json={"name": name, "catalog_name": cat_name}), 200)
    try:
        ok(uc("PATCH", f"schemas/{cat_name}.{name}", json={"comment": "lab"}), 200)
        assert ok(uc("GET", f"schemas/{cat_name}.{name}"), 200).json()["comment"] == "lab"
        assert name in [
            s["name"]
            for s in ok(uc("GET", "schemas", params={"catalog_name": cat_name}), 200).json()[
                "schemas"
            ]
        ]
        assert (name,) in pyiceberg_catalog(WAREHOUSE).list_namespaces()
    finally:
        uc("DELETE", f"schemas/{cat_name}.{name}")
    assert uc("GET", f"schemas/{cat_name}.{name}").status_code == 404


# ----------------------------------------------------------------- functions, unregister, lineage


def test_catalog_functions(rest, namespace):
    """Base: functions are kept in the catalog and can be listed.

    Lakekeeper: Iceberg REST functions endpoint; Unity: its own function API.

    Checks:
        * Lakekeeper: listing the namespace's functions answers 2xx with an identifier list;
        * Unity: a created function is in the schema's function list.
        Polaris and RustFS: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        namespace: Fresh namespace of the test session.
    """
    offered_by("lakekeeper", "unity", what="functions in the catalog")
    with gap("functions"):
        if PROFILE.name == "lakekeeper":
            got = ok(rest.call("GET", f"namespaces/{namespace}/functions")).json()
            assert "identifiers" in got or "functions" in got, got
            return
        name = f"fl_{uuid.uuid4().hex[:6]}"
        info = {
            "name": name,
            "catalog_name": WAREHOUSE,
            "schema_name": namespace,
            "input_params": {"parameters": []},
            "data_type": "INT",
            "full_data_type": "INT",
            "return_params": {"parameters": []},
            "routine_body": "EXTERNAL",
            "routine_definition": "return 1",
            "parameter_style": "S",
            "is_deterministic": True,
            "sql_data_access": "NO_SQL",
            "is_null_call": False,
            "security_type": "DEFINER",
            "specific_name": name,
            "external_language": "python",
        }
        ok(uc("POST", "functions", json={"function_info": info}), 200)
        try:
            listed = ok(
                uc(
                    "GET", "functions", params={"catalog_name": WAREHOUSE, "schema_name": namespace}
                ),
                200,
            ).json()
            assert name in [f["name"] for f in listed.get("functions", [])]
        finally:
            uc("DELETE", f"functions/{WAREHOUSE}.{namespace}.{name}")


def test_unregister(rest, table):
    """Unregister: the table leaves the catalog, its files stay (can be registered again).

    Checks:
        * after unregister the table no longer exists in the catalog;
        * registering its last metadata file again works and the table reads ids 1..3.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.

    Args:
        rest: Raw Iceberg REST client of the catalog under test (``tests/lab/rest_client.py``).
        table: ``(namespace, name, pyiceberg table)`` - a fresh table with ids 1..3.
    """
    offered_by("lakekeeper", "reference", what="unregistering tables")
    ns, name, t = table
    location = t.metadata_location
    with gap("unregister"):
        ok(rest.call("POST", f"namespaces/{ns}/tables/{name}/unregister", json={}))
        assert not pyiceberg_catalog(WAREHOUSE).table_exists(f"{ns}.{name}")
        ok(
            rest.call(
                "POST",
                f"namespaces/{ns}/register",
                json={"name": name, "metadata-location": location},
            )
        )
        assert rows(pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{name}")) == [1, 2, 3]


@pytest.mark.specific
def test_polaris_openlineage():
    """OpenLineage: run events (single and batch) are accepted.

    Checks:
        * a single run event and a batch of events are both accepted with 2xx.
        Only the catalogs named in ``offered_by`` describe it (not part of the REST spec);
        the others: not offered.
    """
    offered_by("polaris", what="receiving OpenLineage events")
    event = {
        "eventType": "COMPLETE",
        "eventTime": "2026-11-15T10:00:00Z",
        "producer": "https://iceberg-lab",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent",
        "run": {"runId": str(uuid.uuid4())},
        "job": {"namespace": "lab", "name": "openlineage-test"},
        "inputs": [],
        "outputs": [],
    }
    url = f"{SETTINGS.catalog_base.rstrip('/')}/api/openlineage/v1/lineage"
    h = catalog_headers(SETTINGS) | {"Content-Type": "application/json"}
    ok(requests.post(url, json=event, headers=h, timeout=20))
    batch = [event | {"run": {"runId": str(uuid.uuid4())}} for _ in range(2)]
    ok(requests.post(f"{url}/batch", json=batch, headers=h, timeout=20))
