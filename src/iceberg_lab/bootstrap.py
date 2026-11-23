"""Idempotent setup of the lab for the catalog under test (``CATALOG``).

Run it with::

    uv run python -m iceberg_lab.bootstrap            # CATALOG=lakekeeper by default

The devcontainer runs it on every start (``postStartCommand``) and the test suite once per
session (``tests/conftest.py``). Every step first checks whether its work is already done, so
running it repeatedly is safe and cheap.

Steps (all catalogs share RustFS):
    1. Wait until RustFS answers HTTP and create the S3 bucket if it does not exist.
    2. Per catalog:

       * Lakekeeper: bootstrap once (accepts the terms, creates the default project; a repeat
         answers ``CatalogAlreadyBootstrapped``, treated as success), then the warehouses
         ``demo-signing`` and ``demo-sts`` - Lakekeeper validates storage access on create,
         so a wrong endpoint or key fails here and not later in an engine;
       * Polaris: catalog ``polaris_sts`` with STS on RustFS and the root principal's grants
         (Polaris keeps everything in memory: after a restart this recreates it);
       * RustFS: enable the table bucket ``labtables`` as catalog warehouse;
       * Unity: bucket, storage credential, external location and catalog ``lab`` with
         managed storage (``ensure_unity`` also serves the permission instance).
"""

import time
from dataclasses import replace

import boto3
import requests
from botocore.exceptions import ClientError

from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSE_SIGNING, WAREHOUSE_STS, Settings


def wait_for(url: str, timeout: float = 60) -> None:
    """Block until ``url`` accepts a TCP/HTTP connection, or raise after ``timeout`` seconds.

    Any HTTP status counts as "up" (RustFS answers ``/`` with 403 without credentials); only
    connection errors are retried.

    Raises:
        requests.ConnectionError: if the service is still unreachable after ``timeout``.

    Args:
        url: Service URL to probe.
        timeout: Seconds to wait at most.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            requests.get(url, timeout=2)
            return
        except requests.ConnectionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def ensure_bucket(s: Settings) -> None:
    """Create the warehouse bucket in RustFS with the root credentials, if missing.

    ``head_bucket`` raises ``ClientError`` (404) for a missing bucket; any other error, e.g.
    wrong credentials, will surface again from ``create_bucket`` with a clearer message.

    Args:
        s: Connection settings of the lab.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint,
        aws_access_key_id=s.s3_access_key,
        aws_secret_access_key=s.s3_secret_key,
        region_name=s.s3_region,
    )
    try:
        s3.head_bucket(Bucket=s.s3_bucket)
    except ClientError:
        s3.create_bucket(Bucket=s.s3_bucket)
        print(f"created bucket {s.s3_bucket}")


def ensure_bootstrapped(s: Settings) -> None:
    """Bootstrap Lakekeeper once; a repeated call is detected and ignored.

    Lakekeeper runs without authentication in this lab, so the anonymous caller becomes the
    initial admin and every later request is allowed ("AllowAll" authorizer).

    Raises:
        requests.HTTPError: for any failure other than "already bootstrapped".

    Args:
        s: Connection settings of the lab.
    """
    r = requests.post(
        f"{s.lakekeeper_url}/management/v1/bootstrap",
        json={"accept-terms-of-use": True},
        timeout=10,
    )
    if r.ok:
        print("bootstrapped lakekeeper")
    elif (
        # Lakekeeper answers 400 with error type "CatalogAlreadyBootstrapped" on repeat calls.
        "already bootstrapped" not in r.text.lower() and "CatalogAlreadyBootstrapped" not in r.text
    ):
        r.raise_for_status()


def warehouse_payload(s: Settings, name: str, sts: bool) -> dict:
    """Build the ``POST /management/v1/warehouse`` request body for one warehouse.

    Args:
        s: Connection settings.
        name: Warehouse name; also used as key prefix inside the bucket, so both warehouses
            share one bucket without overlapping.
        sts: ``True`` enables vended credentials via STS ``AssumeRole`` on RustFS;
            ``False`` makes Lakekeeper fall back to remote signing.

    Returns:
        JSON-serialisable payload in Lakekeeper's management API format.
    """
    profile = {
        "type": "s3",
        "bucket": s.s3_bucket,
        "key-prefix": name,
        # Endpoint as seen by the *engines*: Lakekeeper copies it into every loadTable
        # response, so it must be reachable from where the engines run (compose network).
        "endpoint": s.s3_endpoint,
        "region": s.s3_region,
        # RustFS (like MinIO) serves buckets as path (http://host/bucket/key), not as
        # virtual-host subdomains (http://bucket.host/key).
        "path-style-access": True,
        # "s3-compat" = generic S3 implementation; "aws" would enable AWS-only features.
        "flavor": "s3-compat",
        "sts-enabled": sts,
    }
    if sts:
        # RustFS serves STS on the same port as S3 (POST / with Action=AssumeRole).
        # RustFS does NOT implement GetSessionToken, only AssumeRole - which is what
        # Lakekeeper uses for s3-compat storage.
        profile["sts-endpoint"] = s.s3_endpoint
    return {
        "warehouse-name": name,
        "storage-profile": profile,
        # The credential Lakekeeper itself uses: to write table metadata, to sign requests
        # (demo-signing) or to call AssumeRole (demo-sts). Engines never see it.
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": s.s3_access_key,
            "secret-access-key": s.s3_secret_key,
        },
        # Dropping a table deletes its files immediately (default is a soft-delete with
        # expiration), which keeps the lab's bucket from growing during test runs.
        "delete-profile": {"type": "hard"},
    }


def ensure_warehouses(s: Settings) -> None:
    """Create ``demo-signing`` and ``demo-sts`` unless a warehouse with that name exists.

    Existing warehouses are left untouched, even if their configuration differs from
    :func:`warehouse_payload` - delete them in the Lakekeeper UI to re-create them.

    Raises:
        RuntimeError: if Lakekeeper rejects a warehouse (it validates storage access on create).

    Args:
        s: Connection settings of the lab.
    """
    r = requests.get(f"{s.lakekeeper_url}/management/v1/warehouse", timeout=10)
    r.raise_for_status()
    existing = {w["name"] for w in r.json()["warehouses"]}
    for name, sts in ((WAREHOUSE_SIGNING, False), (WAREHOUSE_STS, True)):
        if name in existing:
            continue
        r = requests.post(
            f"{s.lakekeeper_url}/management/v1/warehouse",
            json=warehouse_payload(s, name, sts),
            timeout=30,
        )
        if not r.ok:
            raise RuntimeError(f"creating warehouse {name} failed: {r.status_code} {r.text}")
        print(f"created warehouse {name}")


def ensure_polaris(s: Settings) -> None:
    """Polaris: create catalog ``polaris_sts`` on RustFS and let the root principal use it.

    Idempotent. Polaris runs with in-memory persistence in this lab, so after a restart this
    recreates everything. ``endpoint``/``endpointInternal`` point at RustFS inside the compose
    network; Polaris calls STS AssumeRole there with its own AWS keys (compose environment).
    ``catalog_admin`` (granted to root's ``service_admin``) may manage the catalog but not its
    content - ``CATALOG_MANAGE_CONTENT`` adds tables, data and vended credentials.

    Args:
        s: Connection settings of the lab.
    """
    base = s.catalog_base.rstrip("/")
    client_id, client_secret = PROFILE.oauth_credential.split(":", 1)
    token = requests.post(
        f"{base}{PROFILE.token_path}",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": PROFILE.oauth_scope,
        },
        timeout=10,
    )
    token.raise_for_status()
    headers = {
        "Authorization": f"Bearer {token.json()['access_token']}",
        "Polaris-Realm": "POLARIS",
    }
    name = PROFILE.warehouse_sts
    location = f"s3://{s.s3_bucket}/{name}"
    r = requests.post(
        f"{base}/api/management/v1/catalogs",
        json={
            "catalog": {
                "name": name,
                "type": "INTERNAL",
                "readOnly": False,
                # drop-with-purge: off by default in Polaris; on here so DROP ... PURGE (and
                # engines that always purge) delete the files - like Lakekeeper's hard delete.
                "properties": {
                    "default-base-location": location,
                    "polaris.config.drop-with-purge.enabled": "true",
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": [location],
                    "endpoint": s.s3_endpoint,
                    "endpointInternal": s.s3_endpoint,
                    "pathStyleAccess": True,
                    "region": s.s3_region,
                },
            }
        },
        headers=headers,
        timeout=10,
    )
    if r.status_code not in (201, 409):  # 409: exists already
        r.raise_for_status()
    r = requests.put(
        f"{base}/api/management/v1/catalogs/{name}/catalog-roles/catalog_admin/grants",
        json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        headers=headers,
        timeout=10,
    )
    r.raise_for_status()
    print(f"polaris catalog {name} ready")


def ensure_rustfs_table_bucket(s: Settings) -> None:
    """RustFS catalog: create the table bucket and enable it (``PUT /iceberg/v1/buckets/<b>``).

    Idempotent - enabling an enabled bucket returns its status again. Signed with SigV4 like
    every RustFS catalog call (see :func:`iceberg_lab.config.catalog_auth`).

    Args:
        s: Connection settings of the lab.
    """
    from iceberg_lab.config import catalog_auth

    bucket = PROFILE.warehouse_sts
    s3 = boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint,
        aws_access_key_id=s.s3_access_key,
        aws_secret_access_key=s.s3_secret_key,
        region_name=s.s3_region,
    )
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
    r = requests.put(
        f"{s.catalog_uri}/v1/buckets/{bucket}",
        data="{}",
        headers={"Content-Type": "application/json"},
        auth=catalog_auth(s),
        timeout=10,
    )
    r.raise_for_status()
    assert r.json().get("enabled"), r.text
    print(f"rustfs table bucket {bucket} ready")


def ensure_unity(
    s: Settings = SETTINGS, base: str | None = None, headers: dict | None = None
) -> None:
    """Unity Catalog: bucket, storage credential, external location, catalog with managed storage.

    Idempotent (409 = exists). The storage credential's role ARN is never checked by RustFS'
    STS; Unity assumes it with its master identity (unity/server.properties) through the lab
    gateway and attaches a session policy scoped to each table's path.

    ``base``/``headers``: another Unity instance and its admin credentials (the permission
    instance ``unitycatalog-authz``, tests/lab/authz_backends.py); default the catalog under test.

    Args:
        s: Connection settings of the lab.
        base: Base URL of another Unity instance (default: the lab's main one).
        headers: Admin headers for that instance (default: the main instance's token).
    """
    base = f"{(base or s.catalog_base).rstrip('/')}/api/2.1/unity-catalog"
    bucket = PROFILE.location.removeprefix("s3://").split("/", 1)[0]
    s3 = boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint,
        aws_access_key_id=s.s3_access_key,
        aws_secret_access_key=s.s3_secret_key,
        region_name=s.s3_region,
    )
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
    for path, body in (
        (
            "credentials",
            {
                "name": "lab_storage",
                "purpose": "STORAGE",
                "aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/uc-storage-lab"},
            },
        ),
        (
            "external-locations",
            {"name": "lab", "url": f"s3://{bucket}/", "credential_name": "lab_storage"},
        ),
        ("catalogs", {"name": PROFILE.warehouse_sts, "storage_root": PROFILE.location}),
    ):
        # Check first: with authorization on, Unity answers creating an existing catalog whose
        # storage overlaps a dropped schema's pending cleanup with 403, not "already exists".
        if requests.get(f"{base}/{path}/{body['name']}", headers=headers or {}, timeout=10).ok:
            continue
        r = requests.post(f"{base}/{path}", json=body, headers=headers or {}, timeout=10)
        if r.status_code not in (200, 201, 409) and "ALREADY_EXISTS" not in r.text:
            r.raise_for_status()
    print(f"unity catalog {PROFILE.warehouse_sts} ready")


def main(s: Settings = SETTINGS) -> None:
    """Run all bootstrap steps for the catalog under test (``CATALOG``, see config).

    Args:
        s: Connection settings of the lab.
    """
    wait_for(s.s3_endpoint)
    ensure_bucket(s)
    if PROFILE.name == "rustfs":
        ensure_rustfs_table_bucket(s)
    elif PROFILE.name == "unity":
        wait_for(f"{s.catalog_base}/api/2.1/unity-catalog/catalogs")
        ensure_unity(s)
    elif PROFILE.name == "polaris":
        wait_for(f"{s.catalog_base}/api/catalog/v1/config")
        ensure_polaris(s)
    elif PROFILE.name == "reference":
        # The fixture writes below its CATALOG_WAREHOUSE (s3://reference/wh): only the bucket.
        ensure_bucket(replace(s, s3_bucket=PROFILE.location.split("/")[2]))
        wait_for(f"{s.catalog_base}/v1/config")
    else:
        wait_for(f"{s.lakekeeper_url}/health")
        ensure_bootstrapped(s)
        ensure_warehouses(s)
    print("bootstrap complete")


if __name__ == "__main__":
    main()
