"""How far do vended STS credentials reach? (``sts`` warehouse of every catalog that vends)

The catalog (Lakekeeper, Polaris, Unity) calls STS ``AssumeRole`` on RustFS with a session
policy for the table's location and hands the resulting keys to the engine (``loadTable`` ->
``s3.access-key-id`` ...). RustFS vends no keys ("not offered").
Engines are not involved here: the keys of table A are used directly with boto3, i.e. the
tests show what anyone holding leaked credentials of table A could do.

Expected (least privilege): everything inside A's location, nothing outside it - not table
B, not the bucket root, not the other warehouse, not another bucket, no bucket management.
Plus the credential lifecycle: an expiry time is announced, the REST ``/credentials``
endpoint (``client.refresh-credentials-endpoint``) hands out fresh working keys, and a
tampered session token is rejected.

The existing read checks (A's metadata yes, B's metadata no) are in
``test_access_delegation.py::test_vended_credentials_are_scoped_to_their_table``.
"""

from __future__ import annotations

import time
import uuid
from urllib.parse import urljoin

import boto3
import pytest
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from iceberg_lab.config import (
    SETTINGS,
    WAREHOUSE_SIGNING,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
)
from iceberg_lab.engines import PyIcebergEngine

DENIED = ("403", "AccessDenied")


def _client(access_key, secret, token, retries=1):
    """``retries=1``: calls under test are never retried (a refusal must show as is).

    Args:
        access_key: Access key id.
        secret: Secret key.
        token: Session token.
        retries: Attempts per call.

    Returns:
        The client.
    """
    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        aws_session_token=token,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": retries}),
    )


def _split(uri: str) -> tuple[str, str]:
    """``s3://bucket/key`` -> ``(bucket, key)``.

    Args:
        uri: S3 URI.

    Returns:
        ``(bucket, key)``.
    """
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


@pytest.fixture(scope="module")
def tables(namespace):
    """Tables A and B on demo-sts (A's keys are the ones under test).

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(table A, table B)``.
    """
    from conftest import require_vended_credentials

    require_vended_credentials()
    import pyarrow as pa

    pi = PyIcebergEngine(WAREHOUSE_STS)
    data = pa.table({"id": [1, 2], "name": ["a", "b"], "amount": [1.0, 2.0]})
    name = f"sts_{uuid.uuid4().hex[:6]}"
    pi.write(namespace, f"{name}_a", data)
    pi.write(namespace, f"{name}_b", data)
    a = pi.catalog.load_table(f"{namespace}.{name}_a")
    b = pi.catalog.load_table(f"{namespace}.{name}_b")
    return a, b


@pytest.fixture(scope="module")
def creds(tables):
    """``(boto3 client with A's vended keys, A's properties)``.

    Args:
        tables: Tables A and B.

    Returns:
        ``(client, properties)``.
    """
    a, _ = tables
    p = a.io.properties
    assert "s3.session-token" in p, "expected vended STS credentials"
    return _client(p["s3.access-key-id"], p["s3.secret-access-key"], p["s3.session-token"]), p


def _denied(call):
    """The call is refused by S3 (403 / AccessDenied).

    Args:
        call: Callable that must be refused.
    """
    with pytest.raises(ClientError) as exc:
        call()
    assert exc.value.response["Error"]["Code"] in DENIED, exc.value.response["Error"]


# =========================================================================== inside A


def test_write_and_delete_inside_own_table(tables, creds):
    """Inside A's location the keys may write, read and delete (needed for commits).

    Checks:
        * PUT, GET (same bytes back) and DELETE of a file in A's data directory work.

    Args:
        tables: Tables A and B on the sts warehouse (A's keys are the ones under test).
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
    """
    a, _ = tables
    s3, _ = creds
    bucket, prefix = _split(a.location())
    key = f"{prefix}/data/sts-scope-{uuid.uuid4().hex}.bin"
    s3.put_object(Bucket=bucket, Key=key, Body=b"x")
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"x"
    s3.delete_object(Bucket=bucket, Key=key)


def test_list_own_table_prefix(tables, creds):
    """Listing A's own prefix is allowed (orphan-file removal and purge need it).

    Checks:
        * the listing returns A's files.

    Args:
        tables: Tables A and B on the sts warehouse (A's keys are the ones under test).
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
    """
    a, _ = tables
    s3, _ = creds
    bucket, prefix = _split(a.location())
    keys = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/").get("Contents", [])
    assert keys, "own prefix should list A's files"


# =========================================================================== outside A


@pytest.mark.parametrize(
    "target",
    ["other_table", "bucket_root", "other_warehouse", "sibling_prefix"],
)
def test_write_outside_own_table_is_denied(tables, creds, target):
    """No PUT outside A.

    Checks:
        * a PUT into table B, the bucket root, the signing warehouse or a sibling of A's prefix
          (``<location>-x/``: a policy written as ``<location>*`` would allow it) is denied.

    Args:
        tables: Tables A and B on the sts warehouse (A's keys are the ones under test).
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
        target: Where the PUT goes: other table, bucket root, other warehouse, sibling prefix.
    """
    a, b = tables
    s3, _ = creds
    bucket, a_prefix = _split(a.location())
    keys = {
        "other_table": f"{_split(b.location())[1]}/data/evil.bin",
        "bucket_root": "evil.bin",
        "other_warehouse": f"{WAREHOUSE_SIGNING}/evil.bin",
        "sibling_prefix": f"{a_prefix}-x/evil.bin",
    }
    _denied(lambda: s3.put_object(Bucket=bucket, Key=keys[target], Body=b"x"))


def test_delete_in_other_table_is_denied(tables, creds):
    """A's keys cannot delete B's metadata file.

    Checks:
        * the DELETE is denied.

    Args:
        tables: Tables A and B on the sts warehouse (A's keys are the ones under test).
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
    """
    _, b = tables
    s3, _ = creds
    bucket, key = _split(b.metadata_location)
    # S3 DELETE answers 204 even for missing keys; denial must still be 403.
    _denied(lambda: s3.delete_object(Bucket=bucket, Key=key))
    root = _client(SETTINGS.s3_access_key, SETTINGS.s3_secret_key, None)
    root.head_object(Bucket=bucket, Key=key)  # B's metadata is still there


@pytest.mark.parametrize("prefix", ["", "other_table", "warehouse_prefix"])
def test_list_outside_own_table_is_denied(tables, creds, prefix):
    """Listing outside A is denied.

    Checks:
        * listing the bucket root, B's prefix or the whole warehouse prefix is denied.

    Args:
        tables: Tables A and B on the sts warehouse (A's keys are the ones under test).
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
        prefix: What is listed: bucket root, other table, whole warehouse prefix.
    """
    a, b = tables
    s3, _ = creds
    bucket, _ = _split(a.location())
    p = {
        "": "",
        "other_table": _split(b.location())[1] + "/",
        "warehouse_prefix": f"{WAREHOUSE_STS}/",
    }[prefix]
    _denied(lambda: s3.list_objects_v2(Bucket=bucket, Prefix=p))


def test_bucket_management_is_denied(creds):
    """Other buckets stay invisible and closed; no bucket can be created.

    Checks:
        * creating a bucket is denied;
        * a foreign bucket is not in the bucket list.

    Args:
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
    """
    s3, _ = creds
    # Setup with the root keys may retry: RustFS answers 503 under load (allowed in S3).
    root = _client(SETTINGS.s3_access_key, SETTINGS.s3_secret_key, None, retries=5)
    other = f"sts-scope-{uuid.uuid4().hex[:8]}"
    root.create_bucket(Bucket=other)
    try:
        try:
            names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in DENIED, exc.response["Error"]
            names = []
        assert other not in names, f"foreign bucket visible: {names}"
        _denied(lambda: s3.list_objects_v2(Bucket=other))
        _denied(lambda: s3.put_object(Bucket=other, Key="evil.bin", Body=b"x"))
        _denied(lambda: s3.create_bucket(Bucket=f"evil-{uuid.uuid4().hex[:8]}"))
    finally:
        root.delete_bucket(Bucket=other)


# =========================================================================== lifecycle


def test_expiry_is_announced(creds):
    """``s3.session-token-expires-at-ms`` is in the future and at most a few hours away.

    Checks:
        * 0 < remaining lifetime <= 12 h.

    Args:
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
    """
    _, p = creds
    expires = int(p["s3.session-token-expires-at-ms"]) / 1000
    remaining = expires - time.time()
    assert 0 < remaining <= 12 * 3600, f"{remaining:.0f}s until expiry"


def test_refresh_endpoint_returns_working_credentials(tables, creds):
    """The refresh-credentials endpoint returns new keys that work for A only.

    Checks:
        * the endpoint answers with storage credentials for A's location;
        * they read A's metadata file and are denied on B's.

    Args:
        tables: Tables A and B on the sts warehouse (A's keys are the ones under test).
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
    """
    a, b = tables
    _, p = creds
    # Absolute (Lakekeeper) or relative to the catalog URI (Polaris) - both occur.
    url = urljoin(SETTINGS.catalog_uri.rstrip("/") + "/", p["client.refresh-credentials-endpoint"])
    r = requests.get(url, headers=catalog_headers(), auth=catalog_auth(), timeout=10)
    r.raise_for_status()
    storage = r.json()["storage-credentials"]
    cfg = next(c["config"] for c in storage if a.location().startswith(c["prefix"]))
    s3 = _client(cfg["s3.access-key-id"], cfg["s3.secret-access-key"], cfg["s3.session-token"])
    s3.head_object(Bucket=_split(a.metadata_location)[0], Key=_split(a.metadata_location)[1])
    bucket, key = _split(b.metadata_location)
    _denied(lambda: s3.head_object(Bucket=bucket, Key=key))


def test_tampered_session_token_is_rejected(tables, creds):
    """Keys with a modified session token do not work.

    Checks:
        * a GET with the tampered token is refused (400/403/InvalidToken/AccessDenied).

    Args:
        tables: Tables A and B on the sts warehouse (A's keys are the ones under test).
        creds: ``(boto3 client with A's vended keys, A's FileIO properties)``.
    """
    a, _ = tables
    _, p = creds
    token = p["s3.session-token"]
    bad = token[:-4] + ("AAAA" if not token.endswith("AAAA") else "BBBB")
    s3 = _client(p["s3.access-key-id"], p["s3.secret-access-key"], bad)
    bucket, key = _split(a.metadata_location)
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=bucket, Key=key)
    assert exc.value.response["Error"]["Code"] in ("400", "403", "InvalidToken", "AccessDenied")
