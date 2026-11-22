"""Helpers for the example notebooks: the same notebooks run against every catalog.

The catalog comes from ``CATALOG`` (see ``iceberg_lab.config``). A cell that shows something
a catalog cannot do wraps it in :func:`catalog_limit`: the notebook keeps running and prints
the exact refusal; tests/demos/test_notebooks.py checks that text per catalog
(``expect_<catalog>`` in the cell metadata), so a limit is still verified, not hidden.
"""

from __future__ import annotations

from contextlib import contextmanager

import requests

from iceberg_lab.config import (
    PROFILE,
    REMOTE_SIGNING,
    SETTINGS,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
)

#: Trino catalog of the main warehouse (``lk_sts``, ``pol_sts``, ``rfs_sts``, ``uc_sts``).
TRINO_STS = f"{PROFILE.trino_prefix}_sts"
#: Trino catalog of the remote-signing warehouse (only where the catalog offers signing).
TRINO_SIGNING = f"{PROFILE.trino_prefix}_signing" if REMOTE_SIGNING else None
#: Bucket of the main warehouse (Lakekeeper/Polaris: ``warehouse``, RustFS: ``labtables``,
#: Unity: ``unitylab``).
BUCKET = (PROFILE.location or f"s3://{SETTINGS.s3_bucket}").removeprefix("s3://").split("/")[0]


def header() -> None:
    """Which catalog this notebook runs against."""
    print(
        f"Catalog: {PROFILE.name}   REST: {SETTINGS.catalog_uri}   Warehouse: {WAREHOUSE_STS}\n"
        f"Remote signing: {'yes' if REMOTE_SIGNING else 'no'}   "
        f"Vended credentials: {'yes' if PROFILE.vended_credentials else 'no'}"
    )


@contextmanager
def catalog_limit(what: str):
    """Run a step that not every catalog/engine combination supports; print the refusal.

    Args:
        what: What the step does (printed with the refusal).

    Yields:
        Nothing; the block runs inside.
    """
    try:
        yield
    except Exception as exc:  # noqa: BLE001 - shown to the reader and checked by the test
        print(f"[{PROFILE.name}] {what} not possible: {type(exc).__name__}: {str(exc)[:400]}")


def describe_catalog() -> None:
    """How the catalog under test is set up (each catalog has its own management API)."""
    base = SETTINGS.catalog_base.rstrip("/")
    if PROFILE.name == "lakekeeper":
        for w in requests.get(f"{base}/management/v1/warehouse", timeout=10).json()["warehouses"]:
            p = w["storage-profile"]
            print(
                f"Warehouse {w['name']:15} bucket={p['bucket']} prefix={p['key-prefix']} "
                f"sts-enabled={p['sts-enabled']}"
            )
    elif PROFILE.name == "polaris":
        c = requests.get(
            f"{base}/api/management/v1/catalogs/{WAREHOUSE_STS}",
            headers=catalog_headers(SETTINGS),
            timeout=10,
        ).json()
        s = c["storageConfigInfo"]
        print(
            f"Catalog {c['name']} ({c['type']}): storage {s['storageType']} "
            f"{s.get('allowedLocations')}  properties {c.get('properties')}"
        )
    elif PROFILE.name == "rustfs":
        b = requests.get(
            f"{SETTINGS.catalog_uri}/v1/buckets/{WAREHOUSE_STS}", auth=catalog_auth(), timeout=10
        ).json()
        print(
            f"Table bucket {b['table-bucket']}: enabled={b['enabled']} "
            f"location={b['warehouse-location']} catalog-uri={b['catalog-uri']}"
        )
    elif PROFILE.name == "unity":
        api = f"{base}/api/2.1/unity-catalog"
        c = requests.get(
            f"{api}/catalogs/{WAREHOUSE_STS}", headers=catalog_headers(SETTINGS), timeout=10
        ).json()
        print(f"Unity catalog {c['name']}: storage_root={c.get('storage_root')}")
        locs = requests.get(
            f"{api}/external-locations", headers=catalog_headers(SETTINGS), timeout=10
        ).json()
        for loc in locs.get("external_locations", []):
            print(
                f"External location {loc['name']}: {loc['url']} (credential {loc['credential_name']})"
            )
    else:
        print(f"Catalog {PROFILE.name}: no description available")


def drop_table(cat, ident: str) -> None:
    """Drop a table including its files where the catalog allows purge (RustFS does not).

    Args:
        cat: PyIceberg catalog.
        ident: Table identifier ``namespace.table``.
    """
    try:
        cat.purge_table(ident)
    except Exception:  # noqa: BLE001 - RustFS answers purge with 406: plain drop
        cat.drop_table(ident)
