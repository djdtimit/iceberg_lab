"""Catalog administration - base capabilities, the same tests on every catalog.

Each catalog has its own management API for the same jobs (docs/feature-inventory.md); an
adapter per catalog translates (pattern of tests/lab/authz_backends.py):

* catalog_lifecycle: create a warehouse/catalog, see it listed, use it, delete it;
* catalog_update: rename it (Lakekeeper, Unity) or change its properties (Polaris);
* catalog_deactivate: switch it off (refused) and on again (Lakekeeper, RustFS);
* storage_config: change how the catalog reaches its storage;
* users: create, list, change, delete a user/principal;
* server_info: version / metastore identity;
* background_tasks: background work can be listed and inspected (Lakekeeper's task queue,
  RustFS' maintenance jobs).

Everything is created under fresh names and removed again; the warehouses the other tests use
are never touched. A catalog without the capability is "no API".
"""

from __future__ import annotations

import json
import re
import tempfile
import uuid

import pytest
import requests

from iceberg_lab.config import PROFILE, SETTINGS, catalog_auth, catalog_headers
from iceberg_lab.engines import pyiceberg_catalog

from conftest import expect, find_gap, gap_table


def ok(r: requests.Response, *codes: int) -> requests.Response:
    """Assert an expected status (message names method, URL and body).

    Args:
        r: HTTP response.
        codes: Status codes that count as success (default 200, 201, 204).

    Returns:
        The response.
    """
    assert r.status_code in (codes or (200, 201, 204)), (
        f"{r.request.method} {r.url}: {r.status_code} {r.text[:300]}"
    )
    return r


def no_api(what: str):
    """Skip as "not offered": the catalog has no API for this.

    Args:
        what: The missing capability (for the skip reason).
    """
    pytest.skip(f"catalog {PROFILE.name} has no API for {what}")


# =========================================================================== adapters


class Lakekeeper:
    """Lakekeeper's management API (warehouses)."""

    base = f"{SETTINGS.catalog_base.rstrip('/')}/management/v1"

    def create(self, name):
        """Create a warehouse on RustFS.

        Args:
            name: Name of the catalog / warehouse.

        Returns:
            The warehouse id.
        """
        from iceberg_lab.bootstrap import warehouse_payload

        return ok(
            requests.post(
                f"{self.base}/warehouse", json=warehouse_payload(SETTINGS, name, True), timeout=20
            )
        ).json()["warehouse-id"]

    def names(self):
        """Names of all warehouses.

        Returns:
            The names.
        """
        return [
            w["name"]
            for w in ok(requests.get(f"{self.base}/warehouse", timeout=20)).json()["warehouses"]
        ]

    def exists(self, handle):
        """Whether the warehouse exists.

        Args:
            handle: Handle as returned by ``create`` (id or name).

        Returns:
            True if GET answers 200.
        """
        return requests.get(f"{self.base}/warehouse/{handle}", timeout=20).status_code == 200

    def delete(self, handle):
        """Delete the warehouse.

        Args:
            handle: Handle as returned by ``create`` (id or name).
        """
        ok(requests.delete(f"{self.base}/warehouse/{handle}", timeout=20))

    def update(self, handle, name):
        """Rename the warehouse (``<name>-renamed``).

        Args:
            handle: Handle as returned by ``create`` (id or name).
            name: Name of the catalog / warehouse.

        Returns:
            The new name.
        """
        ok(
            requests.post(
                f"{self.base}/warehouse/{handle}/rename",
                json={"new-name": name + "-renamed"},
                timeout=20,
            )
        )
        return name + "-renamed"

    def deactivate(self, handle, on: bool):
        """Deactivate or activate the warehouse.

        Args:
            handle: Handle as returned by ``create`` (id or name).
            on: True: switch on, False: switch off.
        """
        ok(
            requests.post(
                f"{self.base}/warehouse/{handle}/{'activate' if on else 'deactivate'}", timeout=20
            )
        )

    def storage(self, handle):
        """Change the storage profile (STS off) and read it back.

        Args:
            handle: Handle as returned by ``create`` (id or name).

        Returns:
            Whether the change is stored.
        """
        w = ok(requests.get(f"{self.base}/warehouse/{handle}", timeout=20)).json()
        profile = w["storage-profile"] | {"sts-enabled": False}
        from iceberg_lab.bootstrap import warehouse_payload

        cred = warehouse_payload(SETTINGS, "x", True)["storage-credential"]
        ok(
            requests.post(
                f"{self.base}/warehouse/{handle}/storage",
                json={"storage-profile": profile, "storage-credential": cred},
                timeout=20,
            )
        )
        return (
            ok(requests.get(f"{self.base}/warehouse/{handle}", timeout=20)).json()[
                "storage-profile"
            ]["sts-enabled"]
            is False
        )

    def server_info(self):
        """Server version from ``/info``.

        Returns:
            The version.
        """
        return ok(requests.get(f"{self.base}/info", timeout=20)).json()["version"]


class Polaris:
    """Polaris' management API (catalogs)."""

    base = f"{SETTINGS.catalog_base.rstrip('/')}/api/management/v1"

    @property
    def h(self):
        """Root principal's headers.

        Returns:
            The headers.
        """
        return catalog_headers(SETTINGS)

    def create(self, name):
        """Create an internal catalog on RustFS and let the root principal manage it.

        Args:
            name: Name of the catalog / warehouse.

        Returns:
            The catalog name.
        """
        loc = f"s3://{SETTINGS.s3_bucket}/{name}"
        ok(
            requests.post(
                f"{self.base}/catalogs",
                json={
                    "catalog": {
                        "name": name,
                        "type": "INTERNAL",
                        "readOnly": False,
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
                headers=self.h,
                timeout=20,
            ),
            201,
        )
        ok(
            requests.put(
                f"{self.base}/catalogs/{name}/catalog-roles/catalog_admin/grants",
                json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
                headers=self.h,
                timeout=20,
            )
        )
        return name

    def names(self):
        """Names of all catalogs.

        Returns:
            The names.
        """
        return [
            c["name"]
            for c in ok(requests.get(f"{self.base}/catalogs", headers=self.h, timeout=20)).json()[
                "catalogs"
            ]
        ]

    def exists(self, handle):
        """Whether the catalog exists.

        Args:
            handle: Handle as returned by ``create`` (id or name).

        Returns:
            True if GET answers 200.
        """
        return (
            requests.get(f"{self.base}/catalogs/{handle}", headers=self.h, timeout=20).status_code
            == 200
        )

    def delete(self, handle):
        """Delete the catalog.

        Args:
            handle: Handle as returned by ``create`` (id or name).
        """
        ok(requests.delete(f"{self.base}/catalogs/{handle}", headers=self.h, timeout=20))

    def _put(self, handle, change):
        """Update the catalog with optimistic locking (current entity version).

        Args:
            handle: Handle as returned by ``create`` (id or name).
            change: Function: current catalog -> fields to change.

        Returns:
            The catalog after the update.
        """
        c = ok(requests.get(f"{self.base}/catalogs/{handle}", headers=self.h, timeout=20)).json()
        body = {"currentEntityVersion": c["entityVersion"]} | change(c)
        ok(requests.put(f"{self.base}/catalogs/{handle}", json=body, headers=self.h, timeout=20))
        return ok(requests.get(f"{self.base}/catalogs/{handle}", headers=self.h, timeout=20)).json()

    def update(self, handle, name):
        """Set a property (Polaris has no rename) and check it is stored.

        Args:
            handle: Handle as returned by ``create`` (id or name).
            name: Name of the catalog / warehouse.

        Returns:
            The unchanged name.
        """
        c = self._put(
            handle, lambda c: {"properties": c["properties"] | {"lab.purpose": "admin-test"}}
        )
        assert c["properties"].get("lab.purpose") == "admin-test", c["properties"]
        return name  # Polaris has no rename: the update is a property change

    def storage(self, handle):
        """Add an allowed location to the storage config.

        Args:
            handle: Handle as returned by ``create`` (id or name).

        Returns:
            Whether it is stored.
        """
        extra = f"s3://{SETTINGS.s3_bucket}/{handle}-extra"
        c = self._put(
            handle,
            lambda c: {
                "storageConfigInfo": c["storageConfigInfo"]
                | {"allowedLocations": c["storageConfigInfo"]["allowedLocations"] + [extra]}
            },
        )
        return extra in c["storageConfigInfo"]["allowedLocations"]


class RustFS:
    """RustFS' table-bucket API (a bucket enabled as warehouse)."""

    def _api(self, method, bucket=""):
        """Table-bucket API call, SigV4-signed.

        Args:
            method: HTTP method.
            bucket: Table bucket (empty: all).

        Returns:
            The response.
        """
        return requests.request(
            method,
            f"{SETTINGS.catalog_uri}/v1/buckets{'/' + bucket if bucket else ''}",
            auth=catalog_auth(),
            timeout=20,
        )

    def _s3(self):
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

    def create(self, name):
        """Create the bucket and enable it as table bucket.

        Args:
            name: Name of the catalog / warehouse.

        Returns:
            The bucket name.
        """
        self._s3().create_bucket(Bucket=name)
        ok(self._api("PUT", name))
        return name

    def names(self):
        """Names of all table buckets.

        Returns:
            The names.
        """
        return [b["table-bucket"] for b in ok(self._api("GET")).json().get("table-buckets", [])]

    def exists(self, handle):
        """Whether the bucket is an enabled table bucket.

        Args:
            handle: Handle as returned by ``create`` (id or name).

        Returns:
            True if enabled.
        """
        r = self._api("GET", handle)
        return r.status_code == 200 and r.json().get("enabled") is True

    def delete(self, handle):
        """Disable the table bucket and delete the bucket.

        Args:
            handle: Handle as returned by ``create`` (id or name).
        """
        ok(self._api("DELETE", handle))
        self._s3().delete_bucket(Bucket=handle)

    def deactivate(self, handle, on: bool):
        """Enable (PUT) or disable (DELETE) the table bucket.

        Args:
            handle: Handle as returned by ``create`` (id or name).
            on: True: switch on, False: switch off.
        """
        ok(self._api("PUT" if on else "DELETE", handle))


class Unity:
    """Unity Catalog's own API (catalogs, credentials, external locations)."""

    base = f"{SETTINGS.catalog_base.rstrip('/')}/api/2.1/unity-catalog"

    @property
    def h(self):
        """Admin headers.

        Returns:
            The headers.
        """
        return catalog_headers(SETTINGS)

    def create(self, name):
        """Create a catalog with managed storage on RustFS.

        Args:
            name: Name of the catalog / warehouse.

        Returns:
            The catalog name.
        """
        bucket = PROFILE.location.removeprefix("s3://").split("/")[0]
        ok(
            requests.post(
                f"{self.base}/catalogs",
                json={"name": name, "storage_root": f"s3://{bucket}/{name}"},
                headers=self.h,
                timeout=20,
            )
        )
        return name

    def names(self):
        """Names of all catalogs.

        Returns:
            The names.
        """
        return [
            c["name"]
            for c in ok(requests.get(f"{self.base}/catalogs", headers=self.h, timeout=20)).json()[
                "catalogs"
            ]
        ]

    def exists(self, handle):
        """Whether the catalog exists.

        Args:
            handle: Handle as returned by ``create`` (id or name).

        Returns:
            True if GET answers 200.
        """
        return (
            requests.get(f"{self.base}/catalogs/{handle}", headers=self.h, timeout=20).status_code
            == 200
        )

    def delete(self, handle):
        """Delete the catalog (force).

        Args:
            handle: Handle as returned by ``create`` (id or name).
        """
        ok(
            requests.delete(
                f"{self.base}/catalogs/{handle}",
                params={"force": "true"},
                headers=self.h,
                timeout=20,
            )
        )

    def update(self, handle, name):
        """Rename the catalog (``<name>_renamed``).

        Args:
            handle: Handle as returned by ``create`` (id or name).
            name: Name of the catalog / warehouse.

        Returns:
            The new name.
        """
        ok(
            requests.patch(
                f"{self.base}/catalogs/{handle}",
                json={"new_name": name + "_renamed"},
                headers=self.h,
                timeout=20,
            )
        )
        return name + "_renamed"

    def storage(self, handle):
        """A storage credential + external location of its own, changed and removed again.

        Args:
            handle: Handle as returned by ``create`` (id or name).

        Returns:
            Whether the change was stored.
        """
        bucket = PROFILE.location.removeprefix("s3://").split("/")[0]
        cred, loc = f"{handle}_cred", f"{handle}_loc"
        ok(
            requests.post(
                f"{self.base}/credentials",
                json={
                    "name": cred,
                    "purpose": "STORAGE",
                    "aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/uc-storage-lab"},
                },
                headers=self.h,
                timeout=20,
            )
        )
        try:
            ok(
                requests.post(
                    f"{self.base}/external-locations",
                    json={
                        "name": loc,
                        "url": f"s3://{bucket}-ext/{handle}/",  # must not overlap the lab location
                        "credential_name": cred,
                    },
                    headers=self.h,
                    timeout=20,
                )
            )
            ok(
                requests.patch(
                    f"{self.base}/external-locations/{loc}",
                    json={"comment": "admin-test"},
                    headers=self.h,
                    timeout=20,
                )
            )
            got = ok(
                requests.get(f"{self.base}/external-locations/{loc}", headers=self.h, timeout=20)
            ).json()
            return got.get("comment") == "admin-test" and got["credential_name"] == cred
        finally:
            requests.delete(
                f"{self.base}/external-locations/{loc}",
                params={"force": "true"},
                headers=self.h,
                timeout=20,
            )
            requests.delete(
                f"{self.base}/credentials/{cred}",
                params={"force": "true"},
                headers=self.h,
                timeout=20,
            )

    def server_info(self):
        """Metastore id from ``metastore_summary``.

        Returns:
            The metastore id.
        """
        return ok(
            requests.get(f"{self.base}/metastore_summary", headers=self.h, timeout=20)
        ).json()["metastore_id"]


class Reference:
    """The Iceberg REST fixture: no management API at all (one fixed warehouse)."""


class Gravitino:
    """Gravitino's own REST API: lakehouse-iceberg catalogs of metalake ``lab`` (one per
    warehouse; the Iceberg REST service serves each under its name)."""

    @property
    def base(self):
        """Metalake URL.

        Returns:
            The URL.
        """
        from iceberg_lab.bootstrap import GRAVITINO_API, GRAVITINO_METALAKE

        return f"{GRAVITINO_API}/api/metalakes/{GRAVITINO_METALAKE}"

    @property
    def h(self):
        """lab-admin's headers (service admin, owner of the metalake).

        Returns:
            The headers.
        """
        from iceberg_lab.bootstrap import GRAVITINO_ACCEPT

        return catalog_headers(SETTINGS) | {"Accept": GRAVITINO_ACCEPT}

    def create(self, name):
        """Create a lakehouse-iceberg catalog like ``gravitino_sts``, data below its own prefix.

        Args:
            name: Name of the catalog / warehouse.

        Returns:
            The catalog name.
        """
        from iceberg_lab.bootstrap import gravitino_catalog_properties

        props = gravitino_catalog_properties(SETTINGS, name, f"s3://{SETTINGS.s3_bucket}/{name}")
        ok(
            requests.post(
                f"{self.base}/catalogs",
                json={
                    "name": name,
                    "type": "RELATIONAL",
                    "provider": "lakehouse-iceberg",
                    "properties": props,
                },
                headers=self.h,
                timeout=60,
            )
        )
        return name

    def names(self):
        """Names of all catalogs of the metalake.

        Returns:
            The names.
        """
        r = ok(requests.get(f"{self.base}/catalogs", headers=self.h, timeout=20))
        return [i["name"] for i in r.json()["identifiers"]]

    def exists(self, handle):
        """Whether the catalog exists.

        Args:
            handle: Catalog name.

        Returns:
            True if GET answers 200.
        """
        r = requests.get(f"{self.base}/catalogs/{handle}", headers=self.h, timeout=20)
        return r.status_code == 200

    def delete(self, handle):
        """Delete the catalog (``force``: also while it is in use).

        Args:
            handle: Catalog name.
        """
        r = requests.delete(
            f"{self.base}/catalogs/{handle}", params={"force": "true"}, headers=self.h, timeout=60
        )
        # 200 with dropped=false for a name that does not exist - a silent leak otherwise
        assert ok(r).json().get("dropped") is True, f"catalog {handle} not dropped: {r.text}"

    def _update(self, handle, *updates):
        """Apply catalog updates (``PUT`` with a list of changes).

        Args:
            handle: Catalog name.
            updates: The changes (``{"@type": ..., ...}``).

        Returns:
            The catalog after the update.
        """
        r = requests.put(
            f"{self.base}/catalogs/{handle}",
            json={"updates": list(updates)},
            headers=self.h,
            timeout=60,
        )
        return ok(r).json()["catalog"]

    def update(self, handle, name):
        """Rename the catalog.

        Args:
            handle: Catalog name.
            name: Name of the catalog / warehouse.

        Returns:
            The new name.
        """
        new = f"{name}-renamed"
        assert self._update(handle, {"@type": "rename", "newName": new})["name"] == new
        return new

    def deactivate(self, handle, on: bool):
        """Switch the catalog off or on (``inUse``).

        Args:
            handle: Catalog name.
            on: True to switch it on.
        """
        ok(
            requests.patch(
                f"{self.base}/catalogs/{handle}", json={"inUse": on}, headers=self.h, timeout=20
            )
        )

    def storage(self, handle):
        """Change the lifetime of the vended STS credentials.

        Args:
            handle: Catalog name.

        Returns:
            Whether it is stored.
        """
        change = {"@type": "setProperty", "property": "s3-token-expire-in-secs", "value": "1800"}
        return self._update(handle, change)["properties"].get("s3-token-expire-in-secs") == "1800"

    def server_info(self):
        """Gravitino's version.

        Returns:
            The version string.
        """
        r = ok(
            requests.get(f"{self.base.split('/api/')[0]}/api/version", headers=self.h, timeout=20)
        )
        return r.json()["version"]["version"]


ADMIN = {
    "lakekeeper": Lakekeeper,
    "polaris": Polaris,
    "rustfs": RustFS,
    "unity": Unity,
    "reference": Reference,
    "gravitino": Gravitino,
}[PROFILE.server]()

# Filled from observed failures only: (catalog, test, regex, reason).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "test"), [
    ("rustfs", "list", r"GET \S+/iceberg/v1/buckets: 501", "rustfs-list-buckets-501"),
    ("unity", "users", r"user still present after DELETE \(active=False\)", "unity-scim-delete-deactivates"),
    ("gravitino", "deactivate", r"reads are still served while switched off; a write refused while switched off took effect anyway \(ServerError: CatalogNotInUseException", "gravitino-disabled-catalog-served"),
])
# fmt: on


def gap(test: str):
    """``expect_gap`` for a documented gap of this test on this catalog.

    Args:
        test: Test name in ``KNOWN_GAPS``.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, test=test))


def fresh_name() -> str:
    # lower-case, letters/digits/dash: valid as Lakekeeper warehouse, Polaris catalog, S3 bucket;
    # Unity catalog names take no dash.
    """A unique catalog name valid on every catalog (Unity: no dash).

    Returns:
        The name.
    """
    n = f"adm-{uuid.uuid4().hex[:8]}"
    return n.replace("-", "_") if PROFILE.name == "unity" else n


def usable(warehouse: str) -> None:
    """The catalog works: a namespace can be created and dropped through Iceberg REST.

    Args:
        warehouse: Warehouse (catalog) name.
    """
    cat = pyiceberg_catalog(warehouse)
    cat.create_namespace("adm_ns")
    assert ("adm_ns",) in cat.list_namespaces()
    cat.drop_namespace("adm_ns")


# =========================================================================== tests


def test_catalog_lifecycle():
    """Create a warehouse/catalog, use it, find it in the list, delete it - and it is gone.

    Checks:
        * after creation the admin API reports it as existing;
        * it is usable through Iceberg REST (create, list and drop a namespace);
        * it appears in the admin API's list;
        * after deletion it no longer exists.
    """
    if not hasattr(ADMIN, "create"):
        no_api("creating a warehouse or catalog")
    name = fresh_name()
    handle = ADMIN.create(name)
    try:
        assert ADMIN.exists(handle)
        usable(name)
        with gap("list"):
            assert name in ADMIN.names()
    finally:
        ADMIN.delete(handle)
    assert not ADMIN.exists(handle)


def test_catalog_update():
    """Change a catalog: rename it (Lakekeeper, Unity) or change a property (Polaris).

    Checks:
        * the update call succeeds;
        * the catalog is usable through Iceberg REST afterwards (under its new name if renamed).
        Catalogs without such an API: not offered.
    """
    if not hasattr(ADMIN, "update"):
        no_api("changing a catalog (rename/properties)")
    name = fresh_name()
    handle = ADMIN.create(name)
    try:
        new = ADMIN.update(handle, name)
        if PROFILE.server in ("unity", "gravitino"):  # addressed by name: the handle moves
            handle = new
        usable(new)
    finally:
        ADMIN.delete(handle)


def test_catalog_deactivate():
    """A switched-off catalog refuses requests; switched on again it works.

    Checks:
        * after deactivation, listing its namespaces fails with "not found / disabled / not in
          use / 403 / 404";
        * creating a namespace fails too - and the refused write did not happen (the namespace
          is absent after reactivation);
        * after reactivation it is usable again (create, list and drop a namespace).
        Catalogs without such an API: not offered.
    """
    if not hasattr(ADMIN, "deactivate"):
        no_api("switching a catalog off and on")
    refused = r"(?i)not found|does not exist|not (table-)?enabled|disabled|not in use|40[34]"
    name = fresh_name()
    handle = ADMIN.create(name)
    try:
        ADMIN.deactivate(handle, on=False)
        problems = []
        try:
            pyiceberg_catalog(name).list_namespaces()
            problems.append("reads are still served while switched off")
        except Exception as e:  # noqa: BLE001 - any refusal; its text is checked
            assert re.search(refused, str(e)), f"read failed for another reason: {e}"
        write_error = None
        try:
            pyiceberg_catalog(name).create_namespace("adm_off")
        except Exception as e:  # noqa: BLE001 - any refusal; its text is checked
            write_error = e
        ADMIN.deactivate(handle, on=True)
        landed = ("adm_off",) in pyiceberg_catalog(name).list_namespaces()
        if write_error is None:
            problems.append("a write was accepted while switched off")
        elif landed:
            problems.append(
                f"a write refused while switched off took effect anyway "
                f"({type(write_error).__name__}: {str(write_error)[:80]})"
            )
        with gap("deactivate"):
            assert not problems, "; ".join(problems)
        if landed:
            pyiceberg_catalog(name).drop_namespace("adm_off")
        usable(name)
    finally:
        ADMIN.delete(handle)


def test_storage_config():
    """Change how the catalog reaches its storage; the change is stored.

    Checks:
        * the catalog-specific storage update (e.g. new credentials or endpoint settings) is
          accepted and reading the configuration back shows it.
        Catalogs without such an API: not offered.
    """
    if not hasattr(ADMIN, "storage"):
        no_api("configuring storage access")
    name = fresh_name()
    handle = ADMIN.create(name)
    try:
        assert ADMIN.storage(handle) is True
    finally:
        ADMIN.delete(handle)


def test_server_info():
    """The server tells which version / metastore it is.

    Checks:
        * the server-information endpoint answers with a non-empty description.
        Catalogs without such an API: not offered.
    """
    if not hasattr(ADMIN, "server_info"):
        no_api("server information")
    assert ADMIN.server_info()


# =========================================================================== users


def _users_lakekeeper(uid):
    """Create, read, rename and delete a Lakekeeper user.

    Args:
        uid: User / principal id.
    """
    base = Lakekeeper.base
    ok(
        requests.post(
            f"{base}/user",
            json={"id": uid, "name": "Admin Test", "user-type": "human", "update-if-exists": False},
            timeout=20,
        ),
        201,
    )
    try:
        assert uid in [
            u["id"] for u in ok(requests.get(f"{base}/user", timeout=20)).json()["users"]
        ]
        ok(
            requests.put(
                f"{base}/user/{uid}",
                json={"name": "Admin Test 2", "user-type": "human"},
                timeout=20,
            )
        )
        assert ok(requests.get(f"{base}/user/{uid}", timeout=20)).json()["name"] == "Admin Test 2"
    finally:
        ok(requests.delete(f"{base}/user/{uid}", timeout=20))
    assert requests.get(f"{base}/user/{uid}", timeout=20).status_code == 404


def _users_polaris(uid):
    """Create, read, change and delete a Polaris principal.

    Args:
        uid: User / principal id.
    """
    p, h = Polaris.base, catalog_headers(SETTINGS)
    ok(
        requests.post(f"{p}/principals", json={"principal": {"name": uid}}, headers=h, timeout=20),
        201,
    )
    try:
        assert uid in [
            x["name"]
            for x in ok(requests.get(f"{p}/principals", headers=h, timeout=20)).json()["principals"]
        ]
        cur = ok(requests.get(f"{p}/principals/{uid}", headers=h, timeout=20)).json()
        ok(
            requests.put(
                f"{p}/principals/{uid}",
                json={"currentEntityVersion": cur["entityVersion"], "properties": {"team": "lab"}},
                headers=h,
                timeout=20,
            )
        )
        assert ok(requests.get(f"{p}/principals/{uid}", headers=h, timeout=20)).json()[
            "properties"
        ] == {"team": "lab"}
    finally:
        ok(requests.delete(f"{p}/principals/{uid}", headers=h, timeout=20))
    assert requests.get(f"{p}/principals/{uid}", headers=h, timeout=20).status_code == 404


def _users_unity(uid):
    """Create, read, change and delete a Unity SCIM user.

    Args:
        uid: User / principal id.
    """
    base, h = (
        f"{SETTINGS.catalog_base.rstrip('/')}/api/1.0/unity-control/scim2/Users",
        catalog_headers(SETTINGS),
    )
    email = f"{uid}@lab.local"
    created = ok(
        requests.post(
            base,
            json={
                "displayName": uid,
                "emails": [{"value": email, "primary": True}],
                "externalId": uid,
            },
            headers=h,
            timeout=20,
        )
    ).json()
    sid = created["id"]
    try:
        assert email in [
            e["value"]
            for u in ok(requests.get(base, headers=h, timeout=20)).json()["Resources"]
            for e in u.get("emails", [])
        ]
        ok(
            requests.put(
                f"{base}/{sid}", json=created | {"displayName": uid + "-2"}, headers=h, timeout=20
            )
        )
        assert (
            ok(requests.get(f"{base}/{sid}", headers=h, timeout=20)).json()["displayName"]
            == uid + "-2"
        )
    finally:
        ok(requests.delete(f"{base}/{sid}", headers=h, timeout=20))
    after = requests.get(f"{base}/{sid}", headers=h, timeout=20)
    assert after.status_code == 404, (
        f"user still present after DELETE (active={after.json().get('active')})"
    )


def _users_rustfs(uid):
    """IAM user via the admin API; checked by its effect (the Python admin client cannot
    decrypt RustFS' encrypted list/info answers): with the policy it can use the catalog,
    after removal it cannot.

    Args:
        uid: User / principal id.
    """
    from authz_backends import Conn, pyiceberg_for
    from minio import MinioAdmin
    from minio.credentials import StaticProvider

    adm = MinioAdmin(
        endpoint=SETTINGS.s3_endpoint.split("//", 1)[1],
        credentials=StaticProvider(SETTINGS.s3_access_key, SETTINGS.s3_secret_key),
        secure=False,
    )
    secret = f"{uid}-secret-1"
    conn = Conn(
        uri=SETTINGS.catalog_uri,
        warehouse=PROFILE.warehouse_sts,
        auth="sigv4",
        keys=(uid, secret),
        vended=False,
    )
    adm.user_add(uid, secret)
    try:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["admin:*"], "Resource": ["arn:aws:s3:::*"]}
            ],
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(policy, f)
        adm.policy_add(f"{uid}-p", f.name)
        adm.policy_set(f"{uid}-p", user=uid)
        pyiceberg_for(conn).list_namespaces()  # the new user works
    finally:
        adm.user_remove(uid)
        adm.policy_remove(f"{uid}-p")
    with pytest.raises(
        Exception,
        match=r"(?i)403|forbidden|access denied|InvalidAccessKeyId|Access Key Id you provided does not exist",
    ):
        pyiceberg_for(conn).list_namespaces()


def _users_gravitino(uid):
    """Create a Gravitino user, grant it a role, delete both.

    Args:
        uid: User / principal id.
    """
    g = Gravitino()
    role = f"{uid}_role"
    ok(requests.post(f"{g.base}/users", json={"name": uid}, headers=g.h, timeout=20))
    try:
        ok(
            requests.post(
                f"{g.base}/roles",
                json={"name": role, "properties": {}, "securableObjects": []},
                headers=g.h,
                timeout=20,
            )
        )
        ok(
            requests.put(
                f"{g.base}/permissions/users/{uid}/grant",
                json={"roleNames": [role]},
                headers=g.h,
                timeout=20,
            )
        )
        user = ok(requests.get(f"{g.base}/users/{uid}", headers=g.h, timeout=20)).json()["user"]
        assert user["roles"] == [role], user
    finally:
        ok(requests.delete(f"{g.base}/users/{uid}", headers=g.h, timeout=20))
        requests.delete(f"{g.base}/roles/{role}", headers=g.h, timeout=20)
    r = requests.get(f"{g.base}/users/{uid}", headers=g.h, timeout=20)
    assert r.status_code == 404, f"user still present after DELETE: {r.status_code}"


USERS = {
    "lakekeeper": _users_lakekeeper,
    "polaris": _users_polaris,
    "unity": _users_unity,
    "rustfs": _users_rustfs,
    "gravitino": _users_gravitino,
}


def test_users():
    """Create a user/principal, find it, change it, delete it (each catalog's user API).

    Checks (per catalog, see ``USERS``):
        * the user is created and can be read back;
        * a change (name or property) is stored;
        * after deletion it is gone.
    """
    uid = (
        f"oidc~admtest{uuid.uuid4().hex[:6]}"
        if PROFILE.name == "lakekeeper"
        else f"admtest{uuid.uuid4().hex[:6]}"
    )
    if PROFILE.server not in USERS:
        no_api("users or principals")
    with gap("users"):
        USERS[PROFILE.server](uid)


# =========================================================================== background tasks


def _table(namespace: str):
    """A small table for the background-task test.

    Args:
        namespace: Namespace to create it in.

    Returns:
        ``(catalog, name, table)``.
    """
    import pyarrow as pa

    cat = pyiceberg_catalog(PROFILE.warehouse_sts)
    name = f"bg_{uuid.uuid4().hex[:6]}"
    t = cat.create_table(f"{namespace}.{name}", schema=pa.schema([("id", pa.int64())]))
    t.append(pa.table({"id": [1, 2]}))
    return cat, name, t


def test_background_tasks(namespace):
    """The catalog's background work can be listed and inspected.

    Checks:
        * Lakekeeper: purging a table creates a task in its queue, findable by the table's id,
          and the task's detail names the queue ``tabular_purge``;
        * RustFS: a triggered maintenance-scheduler run creates a job that can be read back by
          its id;
        * other catalogs: not offered.

    Args:
        namespace: Fresh namespace of the test session.
    """
    if PROFILE.name == "lakekeeper":
        base = Lakekeeper.base
        wid = {
            w["name"]: w["id"]
            for w in ok(requests.get(f"{base}/warehouse", timeout=20)).json()["warehouses"]
        }[PROFILE.warehouse_sts]
        cat, name, t = _table(namespace)
        table_id = str(t.metadata.table_uuid)
        cat.purge_table(f"{namespace}.{name}")
        tasks = ok(
            requests.post(f"{base}/warehouse/{wid}/task/list", json={"page-size": 100}, timeout=20)
        ).json()["tasks"]
        mine = [x for x in tasks if x.get("entity", {}).get("table-id") == table_id]
        assert mine, f"no task for the purged table {table_id}"
        detail = ok(
            requests.get(f"{base}/warehouse/{wid}/task/by-id/{mine[0]['task-id']}", timeout=20)
        ).json()
        assert detail["queue-name"] == "tabular_purge", detail
    elif PROFILE.name == "rustfs":
        _, name, _ = _table(namespace)
        url = f"{SETTINGS.catalog_uri}/v1/{PROFILE.warehouse_sts}/namespaces/{namespace}/tables/{name}/maintenance"
        job = ok(
            requests.post(f"{url}/scheduler/run", json={}, auth=catalog_auth(), timeout=30)
        ).json()["report"]["job"]
        got = ok(
            requests.get(f"{url}/jobs/{job['job_id']}", auth=catalog_auth(), timeout=30)
        ).json()["job"]
        assert got["job_id"] == job["job_id"] and got["table"] == name, got
    else:
        no_api("background tasks of the catalog")
