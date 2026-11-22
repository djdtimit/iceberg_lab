"""The storage under every catalog: RustFS's S3 API where Iceberg clients rely on AWS semantics.

Catalog-independent (the same RustFS for all four catalogs; run with every CATALOG so each
report is complete). Expected answers are AWS S3's / STS's:

checksums
    ``crc32_wrong`` / ``md5_wrong``: a PUT whose checksum header does not match the body is refused
    (400 BadDigest / InvalidRequest), nothing is stored; ``crc32_right``: accepted and returned
    by GET with checksum mode.
conditional writes (Iceberg FileIO and catalogs use them for "create only if absent")
    ``if_none_match_existing``: PUT If-None-Match: * on an existing key -> 412, object unchanged;
    ``if_match_wrong``: PUT If-Match: <other ETag> -> 412; ``if_match_right`` -> 200.
multipart uploads (large data files)
    ``wrong_etag``: completing with a part ETag that does not exist -> 400 InvalidPart;
    ``unknown_upload``: completing an upload id that does not exist -> 404 NoSuchUpload;
    ``small_part``: a non-last part below 5 MiB -> 400 EntityTooSmall.
STS session policies (how Lakekeeper, Polaris and Unity scope vended keys)
    ``prefix``: keys from AssumeRole with a session policy for one prefix read there, not
    elsewhere; ``deny``: a Deny statement in the session policy wins over an Allow;
    ``wildcard_action``: ``s3:Get*`` covers GetObject; ``short_duration``: keys asked for 60 s
    are refused (AWS: minimum 900) or expire within 60 s (+30 s tolerance) - never later.
"""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
import zlib

import boto3
import botocore
import pytest
from botocore.exceptions import ClientError

from iceberg_lab.config import SETTINGS

from conftest import expect_gap

BUCKET = SETTINGS.s3_bucket

# Filled from observed results only: case -> (regex, reason).
# fmt: off
KNOWN_GAPS: dict[str, tuple[str, str]] = {
    "sts:short_duration": (r"asked for 60 s, the keys live 0:1[45]:\d\d", "SECURITY: RustFS STS silently raises DurationSeconds=60 to 900 s instead of refusing it (AWS: 400 below the minimum) - a client asking for 1-minute keys gets 15-minute keys"),
    "sts:wildcard_action": (r"Failed to parse session policy: .*invalid action: 's3:Get\*'", "RustFS refuses wildcards in the actions of an STS session policy ('s3:Get*': invalid action) - AWS allows them; Unity's session policies use them (the lab's STS shim expands them)"),
}
# fmt: on


def _gap(case: str):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        case: Case name.

    Returns:
        The context manager.
    """
    entry = KNOWN_GAPS.get(case)
    return expect_gap(entry[1], entry[0]) if entry else expect_gap(None, None)


def _client(**keys):
    """boto3 S3 client on RustFS (path style, one attempt, checksums only when required).

    Args:
        keys: ``access``, ``secret``, ``token`` (default: root keys).

    Returns:
        The client.
    """
    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=keys.get("access", SETTINGS.s3_access_key),
        aws_secret_access_key=keys.get("secret", SETTINGS.s3_secret_key),
        aws_session_token=keys.get("token"),
        config=botocore.config.Config(
            retries={"max_attempts": 1},
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


@pytest.fixture(scope="module")
def s3():
    """S3 client with the root keys.

    Returns:
        The client.
    """
    return _client()


@pytest.fixture
def key():
    """A fresh object key.

    Returns:
        The key.
    """
    return f"lab-s3-tests/{uuid.uuid4().hex}"


def _code(exc: ClientError) -> tuple[int, str]:
    """HTTP status and S3 error code of a ClientError.

    Args:
        exc: boto3 ClientError.

    Returns:
        ``(status, code)``.
    """
    return exc.response["ResponseMetadata"]["HTTPStatusCode"], exc.response["Error"]["Code"]


def _exists(s3, key) -> bool:
    """Whether the object exists.

    Args:
        s3: boto3 S3 client.
        key: S3 object key.

    Returns:
        True if HEAD succeeds.
    """
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError:
        return False


# =========================================================================== checksums


def _crc32(data: bytes) -> str:
    """CRC32 checksum as S3 expects it (base64, big endian).

    Args:
        data: Bytes.

    Returns:
        The checksum.
    """
    return base64.b64encode(zlib.crc32(data).to_bytes(4, "big")).decode()


@pytest.mark.parametrize("case", ["crc32_wrong", "md5_wrong", "crc32_right"])
def test_checksum(case, s3, key):
    """A PUT whose checksum does not match the body is refused; a right one is kept.

    Checks:
        * wrong CRC32 / wrong MD5: refused with 400 (BadDigest / InvalidRequest /
          XAmzContentChecksumMismatch) and nothing is stored;
        * right CRC32: accepted and returned by GET with checksum mode.

    Args:
        case: The scenario (see the module docstring).
        s3: boto3 client with the RustFS root keys.
        key: A fresh object key under ``lab-s3-tests/``.
    """
    body = b"iceberg data file" * 100
    with _gap(f"checksum:{case}"):
        if case == "crc32_right":
            s3.put_object(Bucket=BUCKET, Key=key, Body=body, ChecksumCRC32=_crc32(body))
            got = s3.get_object(Bucket=BUCKET, Key=key, ChecksumMode="ENABLED")
            assert got.get("ChecksumCRC32") == _crc32(body), (
                f"GET checksum: {got.get('ChecksumCRC32')}"
            )
            return
        kw = (
            {"ChecksumCRC32": _crc32(b"something else")}
            if case == "crc32_wrong"
            else {"ContentMD5": base64.b64encode(hashlib.md5(b"other").digest()).decode()}
        )
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=BUCKET, Key=key, Body=body, **kw)
        status, code = _code(exc.value)
        assert status == 400 and code in (
            "BadDigest",
            "InvalidRequest",
            "XAmzContentChecksumMismatch",
        ), f"wrong checksum refused with {status} {code}"
        assert not _exists(s3, key), "the object was stored despite the wrong checksum"


# =========================================================================== conditional writes


@pytest.mark.parametrize("case", ["if_none_match_existing", "if_match_wrong", "if_match_right"])
def test_conditional_write(case, s3, key):
    """Conditional writes as Iceberg uses them for "create only if absent".

    Checks:
        * ``If-None-Match: *`` on an existing key: 412 and the object unchanged;
        * ``If-Match`` with another ETag: 412 and unchanged;
        * ``If-Match`` with the right ETag: written.

    Args:
        case: The scenario (see the module docstring).
        s3: boto3 client with the RustFS root keys.
        key: A fresh object key under ``lab-s3-tests/``.
    """
    first = s3.put_object(Bucket=BUCKET, Key=key, Body=b"first")["ETag"]
    with _gap(f"conditional:{case}"):
        if case == "if_match_right":
            s3.put_object(Bucket=BUCKET, Key=key, Body=b"second", IfMatch=first)
            assert s3.get_object(Bucket=BUCKET, Key=key)["Body"].read() == b"second"
            return
        kw = (
            {"IfNoneMatch": "*"}
            if case == "if_none_match_existing"
            else {"IfMatch": '"' + "0" * 32 + '"'}
        )
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=BUCKET, Key=key, Body=b"second", **kw)
        status, code = _code(exc.value)
        assert status == 412, f"conditional PUT answered {status} {code}, AWS: 412"
        assert s3.get_object(Bucket=BUCKET, Key=key)["Body"].read() == b"first", "object changed"


# =========================================================================== multipart


@pytest.mark.parametrize("case", ["wrong_etag", "unknown_upload", "small_part"])
def test_multipart_errors(case, s3, key):
    """Completing a multipart upload wrongly fails as AWS does.

    Checks:
        * a part ETag that does not exist: 400 InvalidPart;
        * an upload id that does not exist: 404 NoSuchUpload;
        * a non-last part below 5 MiB: 400 EntityTooSmall;
        * no object is created in any case.

    Args:
        case: The scenario (see the module docstring).
        s3: boto3 client with the RustFS root keys.
        key: A fresh object key under ``lab-s3-tests/``.
    """
    expected = {
        "wrong_etag": (400, "InvalidPart"),
        "unknown_upload": (404, "NoSuchUpload"),
        "small_part": (400, "EntityTooSmall"),
    }[case]
    upload = s3.create_multipart_upload(Bucket=BUCKET, Key=key)["UploadId"]
    try:
        part = s3.upload_part(
            Bucket=BUCKET, Key=key, UploadId=upload, PartNumber=1, Body=b"x" * 1024
        )
        parts = [{"PartNumber": 1, "ETag": part["ETag"]}]
        if case == "wrong_etag":
            parts = [{"PartNumber": 1, "ETag": '"' + "0" * 32 + '"'}]
        elif case == "small_part":
            last = s3.upload_part(Bucket=BUCKET, Key=key, UploadId=upload, PartNumber=2, Body=b"y")
            parts.append({"PartNumber": 2, "ETag": last["ETag"]})
        with _gap(f"multipart:{case}"):
            with pytest.raises(ClientError) as exc:
                s3.complete_multipart_upload(
                    Bucket=BUCKET,
                    Key=key,
                    UploadId="no-such-upload" if case == "unknown_upload" else upload,
                    MultipartUpload={"Parts": parts},
                )
            assert _code(exc.value) == expected, f"answered {_code(exc.value)}, AWS: {expected}"
            assert not _exists(s3, key), "an object was created"
    finally:
        try:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload)
        except ClientError:
            pass


# =========================================================================== STS session policies


def _assume(policy: dict | None, duration: int = 900):
    """S3 client with keys from STS AssumeRole with the session policy.

    Args:
        policy: Session policy (None: none).
        duration: Requested lifetime in seconds.

    Returns:
        The client.
    """
    c = _assume_raw(policy, duration)
    return _client(access=c["AccessKeyId"], secret=c["SecretAccessKey"], token=c["SessionToken"])


def _assume_raw(policy: dict | None, duration: int = 900) -> dict:
    """STS AssumeRole (client-side validation off: RustFS must judge the values).

    Args:
        policy: Session policy (None: none).
        duration: Requested lifetime in seconds.

    Returns:
        The Credentials of the answer.
    """
    sts = boto3.client(
        "sts",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=SETTINGS.s3_access_key,
        aws_secret_access_key=SETTINGS.s3_secret_key,
        config=botocore.config.Config(parameter_validation=False),  # RustFS must judge the values
    )
    kw = {"Policy": json.dumps(policy)} if policy else {}
    return sts.assume_role(
        RoleArn="arn:aws:iam::123456789012:role/lab",
        RoleSessionName="lab",
        DurationSeconds=duration,
        **kw,
    )["Credentials"]


def _can_read(client, key) -> bool:
    """Whether the client can GET the object.

    Args:
        client: boto3 S3 client.
        key: S3 object key.

    Returns:
        True if it can.
    """
    try:
        client.get_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError:
        return False


@pytest.mark.parametrize("case", ["prefix", "deny", "wildcard_action", "short_duration"])
def test_sts_session_policy(case, s3):
    """STS session policies scope temporary keys (how catalogs scope vended keys).

    Checks:
        * prefix: keys read inside the policy's prefix, not outside;
        * deny: a Deny in the policy wins over an Allow;
        * wildcard_action: ``s3:Get*`` covers GetObject, nothing outside the prefix;
        * short_duration: keys asked for 60 s are refused (400) or expire within 90 s - never
          later (RustFS raises to 900 s: documented security gap).

    Args:
        case: The scenario (see the module docstring).
        s3: boto3 client with the RustFS root keys.
    """
    run = f"lab-s3-tests/sts-{uuid.uuid4().hex[:8]}"
    inside, outside = f"{run}/inside/f", f"{run}/outside/f"
    for k in (inside, outside):
        s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")
    allow = {
        "Effect": "Allow",
        "Action": ["s3:GetObject"],
        "Resource": [f"arn:aws:s3:::{BUCKET}/{run}/inside/*"],
    }
    with _gap(f"sts:{case}"):
        if case == "short_duration":
            import datetime

            try:
                creds = _assume_raw({"Version": "2012-10-17", "Statement": [allow]}, duration=60)
            except ClientError as exc:  # AWS: below the minimum of 900 s -> 400
                assert _code(exc)[0] == 400, f"DurationSeconds=60 answered {_code(exc)}"
                return
            lives = creds["Expiration"] - datetime.datetime.now(datetime.UTC)
            assert lives.total_seconds() <= 60 + 30, f"asked for 60 s, the keys live {lives}"
            return
        if case == "prefix":
            c = _assume({"Version": "2012-10-17", "Statement": [allow]})
            assert _can_read(c, inside), "cannot read inside the policy's prefix"
            assert not _can_read(c, outside), "reads outside the policy's prefix"
        elif case == "deny":
            deny = {
                "Effect": "Deny",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{BUCKET}/{inside}"],
            }
            c = _assume({"Version": "2012-10-17", "Statement": [allow, deny]})
            assert not _can_read(c, inside), (
                "a Deny in the session policy did not win over the Allow"
            )
        else:  # wildcard_action
            wild = allow | {"Action": ["s3:Get*"]}
            c = _assume({"Version": "2012-10-17", "Statement": [wild]})
            assert _can_read(c, inside), "s3:Get* does not cover GetObject"
            assert not _can_read(c, outside), "reads outside the policy's prefix"
