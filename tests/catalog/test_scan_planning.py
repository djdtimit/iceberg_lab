"""Capability tests: server-side scan planning and fine-grained (row/column) access control.

Runs on every catalog. State in the lab (Lakekeeper 0.15, Polaris 1.9, RustFS 1.0, Unity 0.7,
Iceberg REST fixture):
  - server-side scan planning only on the REST fixture; the other four advertise
    ``scan-planning-mode=client`` (or nothing), the ``POST .../plan`` endpoint answers 404/501
    and is not listed in ``/v1/config`` (gap table per catalog);
  - no row filters / column masks: the authorizers decide per object (warehouse, namespace,
    table, view), never within a table;
  - the REST spec's ``read-restrictions`` (required row filter / column projection) is not
    emitted by any of them.

The desired capabilities are written as known gaps (causes in docs/gaps.toml, ``expect`` in iceberg_lab.lab.gap_register): they
are reported as failures with the label "[known gap]" until an update adds them.
"""

import uuid

import pyarrow as pa
import pytest
import requests

from iceberg_lab.config import (
    PROFILE,
    SETTINGS,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
    rest_prefix_path,
)
from iceberg_lab.engines import SCHEMA, pyiceberg_catalog

from conftest import expect, find_gap, gap_table

# Filled from observed failures only. The Iceberg REST fixture (reference) implements it.
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "test"), [
    *((c, "advertised", r"AssertionError.*endpoints|assert False", "catalogs-no-scan-planning")
      for c in ("lakekeeper", "polaris", "unity", "rustfs")),
    *((c, "plan", r"plan endpoint answered (404|501)", "catalogs-no-scan-planning")
      for c in ("lakekeeper", "polaris", "unity", "rustfs")),
])
# fmt: on


def _gap(test: str):
    """``expect_gap`` for this catalog's documented gap (none: a plain block).

    Args:
        test: ``advertised`` or ``plan``.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, test=test))


@pytest.fixture(scope="module")
def table_ref(namespace):
    """``(config response, table URL)`` for a small table in demo-sts.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(config, table URL)``.
    """
    table = f"plan_{uuid.uuid4().hex[:6]}"
    # Setup with the lab's PyIceberg client (not an engine under test).
    data = pa.table(
        {"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [1.0, 2.0, 3.0]}, schema=SCHEMA
    )
    pyiceberg_catalog(WAREHOUSE_STS).create_table(f"{namespace}.{table}", schema=SCHEMA).append(
        data
    )
    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    base = f"{SETTINGS.catalog_uri}/v1{rest_prefix_path(cfg)}/namespaces/{namespace}/tables/{table}"
    return cfg, base


def test_tables_advertise_client_side_planning(table_ref):
    """Clients plan scans themselves: ``scan-planning-mode`` is ``client`` or absent.

    Checks:
        * the loadTable config has no other planning mode.

    Args:
        table_ref: ``(config response, table URL)`` for a small table.
    """
    _, base = table_ref
    body = requests.get(base, headers=catalog_headers(), auth=catalog_auth(), timeout=10).json()
    assert body.get("config", {}).get("scan-planning-mode") in (None, "client")


def test_load_table_has_no_read_restrictions(table_ref):
    """Documents today's behaviour: loadTable carries no read restrictions.

    Checks:
        * the loadTable answer has no read-restriction field (with policies and spec support it
          would appear - then this test shows the change).

    Args:
        table_ref: ``(config response, table URL)`` for a small table.
    """
    _, base = table_ref
    assert (
        "read-restrictions"
        not in requests.get(base, headers=catalog_headers(), auth=catalog_auth(), timeout=10).json()
    )


def test_plan_endpoint_is_advertised(table_ref):
    """``/v1/config`` lists the server-side plan endpoint (only the REST fixture does; elsewhere a known gap).

    Checks:
        * an advertised endpoint ends with ``/plan``.

    Args:
        table_ref: ``(config response, table URL)`` for a small table.
    """
    cfg, _ = table_ref
    endpoints = cfg.get("endpoints", [])
    with _gap("advertised"):
        assert any(e.endswith("/plan") for e in endpoints), (
            f"no plan endpoint in endpoints: {endpoints}"
        )


def test_server_side_plan_with_projection_and_filter(table_ref):
    """``POST .../plan`` returns file scan tasks for a projection + filter (elsewhere 404/501).

    Checks:
        * the plan endpoint answers 200 with status completed or submitted.

    Args:
        table_ref: ``(config response, table URL)`` for a small table.
    """
    _, base = table_ref
    r = requests.post(
        f"{base}/plan",
        headers=catalog_headers(),
        auth=catalog_auth(),
        json={
            "select": ["id", "name"],
            "filter": {"type": "gt", "term": "id", "value": 1},
            "case-sensitive": True,
        },
        timeout=10,
    )
    with _gap("plan"):
        assert r.status_code == 200, f"plan endpoint answered {r.status_code}"
    body = r.json()
    assert body["status"] in ("completed", "submitted")
