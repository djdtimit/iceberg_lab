"""Storage faults seen by the catalog: what does it do when RustFS fails under it?

The counterpart of test_robustness.py (faults in front of the catalog). A fault-injecting proxy
(``storage_proxy.py``, port 19000 + the catalog's offset, ``config.lab_port``) sits between the catalog and RustFS
- in a warehouse/catalog of its own, so no other test is affected:

    Lakekeeper  warehouse ``fault-sts`` with storage and STS endpoint ``http://dev:19000``; the
                engines get the same endpoint, so faults only hit requests from Lakekeeper's IP
    Polaris     catalog ``polaris_fault_<port>``, ``endpointInternal``/``stsEndpoint`` = proxy; the
                engines get RustFS directly
    Unity       reaches RustFS only through the Envoy gateway for the AWS host names - not
                re-routable per catalog in the lab (not testable)
    RustFS      the catalog is the storage (catalog faults: test_robustness.py)

Scenarios (the lab's PyIceberg client writes; the fault hits only the catalog's own requests):

    commit_503    the catalog's metadata write gets 503 SlowDown
    commit_down   the catalog's metadata write gets no answer (connection closed)
    commit_slow   the catalog's metadata write is delayed by 20 s
    load_down     every storage read of the catalog fails while a table is loaded (a catalog
                  that loads without storage - Lakekeeper: metadata in its database - passes)
    vend_down     the catalog's STS AssumeRole fails while keys are issued for a new table
                  (a new one: catalogs cache issued keys per table - Polaris does)

Acceptable: an error within 120 s, or a correct success. Not acceptable: hanging, a success whose
row is missing, a broken table afterwards, a catalog that does not recover once storage is back.
"""

from __future__ import annotations

import socket
import time
import uuid

import pyarrow as pa
import pytest
import requests
from storage_proxy import StorageProxy

from iceberg_lab.config import PROFILE, SETTINGS, catalog_headers, lab_port
from iceberg_lab.engines import pyiceberg_catalog

from conftest import expect, find_gap, gap_table

#: The storage proxy's port for this catalog (Lakekeeper 19000; config.lab_port).
PROXY_PORT = lab_port(19000)
PROXY = f"http://dev:{PROXY_PORT}"
SCENARIOS = ["commit_503", "commit_down", "commit_slow", "load_down", "vend_down"]
SCHEMA = pa.schema([("id", pa.int64())])
LIMIT = 120  # seconds an operation may take under a fault

# Filled from observed results only: (catalog, scenario, regex on the outcome, cause).
KNOWN_GAPS = gap_table(("catalog", "scenario"), [])


def _gap(scenario):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        scenario: Fault scenario.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, scenario=scenario))


def _lakekeeper_warehouse() -> str:
    """Lakekeeper warehouse ``fault-sts`` whose storage runs through the proxy.

    Returns:
        Its name.
    """
    from iceberg_lab.bootstrap import warehouse_payload

    name = "fault-sts"  # created with PROXY (Lakekeeper's port is 19000, offset 0)
    existing = requests.get(f"{SETTINGS.lakekeeper_url}/management/v1/warehouse", timeout=10).json()
    if any(w.get("name") == name for w in existing.get("warehouses", [])):
        return name
    body = warehouse_payload(SETTINGS, name, sts=True)
    body["storage-profile"]["endpoint"] = PROXY
    body["storage-profile"]["sts-endpoint"] = PROXY
    requests.post(
        f"{SETTINGS.lakekeeper_url}/management/v1/warehouse", json=body, timeout=60
    ).raise_for_status()
    return name


def _polaris_catalog() -> str:
    """Polaris catalog ``polaris_fault_<port>`` whose storage runs through the proxy.

    The proxy's port is part of the name: a catalog keeps the endpoint it was created with.

    Returns:
        Its name.
    """
    name, base, headers = f"polaris_fault_{PROXY_PORT}", SETTINGS.catalog_base, catalog_headers()
    location = f"s3://{SETTINGS.s3_bucket}/{name}"
    r = requests.post(
        f"{base}/api/management/v1/catalogs",
        json={
            "catalog": {
                "name": name,
                "type": "INTERNAL",
                "properties": {
                    "default-base-location": location,
                    "polaris.config.drop-with-purge.enabled": "true",
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": [location],
                    "region": SETTINGS.s3_region,
                    "endpoint": SETTINGS.s3_endpoint,
                    "endpointInternal": PROXY,
                    "stsEndpoint": PROXY,
                    "pathStyleAccess": True,
                },
            }
        },
        headers=headers,
        timeout=30,
    )
    if r.status_code not in (201, 409):
        r.raise_for_status()
    requests.put(
        f"{base}/api/management/v1/catalogs/{name}/catalog-roles/catalog_admin/grants",
        json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        headers=headers,
        timeout=10,
    ).raise_for_status()
    return name


@pytest.fixture(scope="module")
def fault_setup():
    """(proxy, warehouse, catalog's source IPs) - the proxy is up before the warehouse exists,
    because the catalog validates its storage when the warehouse is created.

    Yields:
        ``(proxy, warehouse, catalog IPs)``.
    """
    if PROFILE.server not in ("lakekeeper", "polaris"):
        reason = {
            "rustfs": "the catalog is the storage itself (catalog faults: test_robustness.py)",
            "unity": "Unity reaches RustFS only through the gateway for the AWS host names",
            "reference": "the fixture's S3 endpoint is fixed at start (compose environment), "
            "a warehouse cannot point it at the fault proxy",
        }.get(PROFILE.name, "no fault proxy setup for this catalog")
        pytest.skip(f"not testable on {PROFILE.name}: {reason}")
    proxy = StorageProxy(port=PROXY_PORT)
    try:
        if PROFILE.server == "lakekeeper":
            warehouse, sources = _lakekeeper_warehouse(), {socket.gethostbyname("lakekeeper")}
        else:  # only Polaris itself uses the proxy (endpointInternal)
            warehouse, sources = _polaris_catalog(), None
        yield proxy, warehouse, sources
    finally:
        proxy.close()


@pytest.fixture
def table(fault_setup):
    """A fresh namespace and table in the fault warehouse.

    Args:
        fault_setup: The fault setup.

    Yields:
        ``(warehouse, namespace, table)``.
    """
    proxy, warehouse, _ = fault_setup
    cat = pyiceberg_catalog(warehouse)
    ns = f"test_{uuid.uuid4().hex[:8]}"
    cat.create_namespace(ns)
    t = cat.create_table(f"{ns}.t", schema=SCHEMA)
    t.append(pa.table({"id": [1]}))
    yield cat, ns
    proxy.disarm()
    for ident in cat.list_tables(ns):
        try:
            cat.purge_table(ident)
        except Exception:  # noqa: BLE001 - cleanup
            cat.drop_table(ident)
    cat.drop_namespace(ns)


def _rows(warehouse, ns) -> list[int]:
    """The table as a fresh client sees it (storage healthy again).

    Args:
        warehouse: Warehouse (catalog) name.
        ns: Namespace.

    Returns:
        Sorted ids.
    """
    return sorted(
        pyiceberg_catalog(warehouse)
        .load_table(f"{ns}.t")
        .scan()
        .to_arrow()
        .column("id")
        .to_pylist()
    )


@pytest.mark.slow
@pytest.mark.parametrize("scenario", SCENARIOS)
def test_storage_fault(scenario, fault_setup, table):
    """The catalog's behaviour when its storage (RustFS) fails under it.

    The lab's PyIceberg client writes; the fault hits only the catalog's own requests.

    Checks:
        * the table was created with issued keys (setup);
        * the fault really hit a request of the catalog;
        * the operation ends within 120 s;
        * commit faults: success means the table holds both rows, an error means [1] or
          [1, 2] - no broken table;
        * once storage is back, a further commit works (the catalog recovers).
        Unity (not re-routable) and RustFS (is the storage): not testable.

    Args:
        scenario: ``commit_503``, ``commit_down``, ``commit_slow``, ``load_down``, ``vend_down``.
        fault_setup: ``(proxy, warehouse, catalog IPs)`` - a warehouse whose storage runs
            through the fault proxy.
        table: ``(warehouse, namespace, table)`` in that warehouse.
    """
    proxy, warehouse, sources = fault_setup
    cat, ns = table
    match, mode, delay = {
        "commit_503": (r"^PUT .*/metadata/", "503", 0),
        "commit_down": (r"^PUT .*/metadata/", "down", 0),
        "commit_slow": (r"^PUT .*/metadata/", "slow", 20),
        "load_down": (r"^(GET|HEAD) ", "down", 0),
        "vend_down": (r"^POST / .*AssumeRole", "down", 0),
    }[scenario]
    proxy.arm(mode, sources=sources, match=match, delay=delay)
    start = time.monotonic()
    try:
        if scenario.startswith("commit"):
            cat.load_table(f"{ns}.t").append(pa.table({"id": [2]}))
        elif scenario == "load_down":
            pyiceberg_catalog(warehouse).load_table(f"{ns}.t")
        else:  # vend_down: a NEW table - catalogs cache issued keys per table (Polaris does)
            t = pyiceberg_catalog(warehouse).create_table(f"{ns}.t_new", schema=SCHEMA)
            assert "s3.session-token" in t.io.properties, "table created without issued keys"
        outcome = "success"
    except Exception as exc:  # noqa: BLE001 - an error is an acceptable outcome; judged below
        outcome = f"error: {type(exc).__name__}: {str(exc)[:300]}"
    took = time.monotonic() - start
    hits = len(proxy.hits)
    proxy.disarm()
    print(f"[{PROFILE.name} {scenario}] {outcome[:160]} | {took:.1f} s, {hits} faulted requests")

    with _gap(scenario):
        # A catalog that does not touch storage to load a table (Lakekeeper: metadata in its
        # database) passes load_down as long as the load worked; everything else must be hit.
        if not (scenario == "load_down" and outcome == "success"):
            assert hits, f"the fault never hit a request of the catalog ({mode}, {match})"
        assert took < LIMIT, f"took {took:.0f} s under the fault ({outcome})"
        rows = _rows(warehouse, ns)  # the table is intact and readable once storage is back
        if scenario.startswith("commit"):
            if outcome == "success":
                assert rows == [1, 2], f"commit reported success, table holds {rows}"
            else:
                assert rows in ([1], [1, 2]), f"after a failed commit the table holds {rows}"
        # the catalog recovers: the next commit works
        cat.load_table(f"{ns}.t").append(pa.table({"id": [3]}))
        assert 3 in _rows(warehouse, ns)
