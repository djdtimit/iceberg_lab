"""Lab self-test: the lab's own adapters between the catalogs and Keycloak / RustFS.

Unity reaches RustFS and Keycloak only through two small lab components (compose profile unity):

    unity/token_shim.py  OAuth client credentials -> Keycloak token -> Unity token exchange
    unity/sts_shim.py    Unity's AssumeRole -> session policy rewritten for RustFS -> re-signed

A finding about Unity is only trustworthy if these components neither open nor close anything
on their own. Checked here:

* token shim: wrong secrets and foreign identities are refused; a token names the right user;
* STS shim (logic, offline): Deny statements are never dropped, wildcards only expand to actions
  they match, a caller's SigV4 signature is verified (valid / tampered / wrong key / too old);
* STS shim (live): unsigned requests and requests without a session policy get no keys; keys
  for one prefix do not reach another.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import os
import re
import sys
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parents[2]
TOKEN_SHIM = "http://unity-token-shim:8080/token"
STS_SHIM = os.environ.get(
    "LAB_STS_SHIM", "http://unity-sts-shim:8080/"
)  # override: test a new build
UNITY = "http://unitycatalog-authz:8080"
CALLER = (
    "rustfsadmin",
    "rustfsadmin",
)  # Unity's aws.accessKey / aws.secretKey (unity/server.properties)


def _up(url: str) -> None:
    """Skip if the service does not answer.

    Args:
        url: Service URL.
    """
    try:
        requests.get(url, timeout=3)
    except requests.RequestException:
        pytest.skip(f"{url} not running (docker compose --profile unity up -d)")


@pytest.fixture(scope="module")
def shim():
    """unity/sts_shim.py imported as a module (its env vars are read at import).

    Returns:
        The module.
    """
    os.environ.setdefault("STS_ACCESS_KEY", CALLER[0])
    os.environ.setdefault("STS_SECRET_KEY", CALLER[1])
    spec = importlib.util.spec_from_file_location("sts_shim", ROOT / "unity" / "sts_shim.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["sts_shim"] = module
    spec.loader.exec_module(module)
    return module


# =========================================================================== token shim


def _token(client: str, secret: str) -> requests.Response:
    """Token request to the Unity token shim.

    Args:
        client: Client under test.
        secret: Secret key.

    Returns:
        The response.
    """
    return requests.post(
        TOKEN_SHIM,
        data={"grant_type": "client_credentials", "client_id": client, "client_secret": secret},
        timeout=20,
    )


@pytest.mark.parametrize(
    ("client", "secret"),
    [
        ("alice", "wrong"),
        ("alice", "bob-secret"),
        ("nobody", "nobody-secret"),
        ("lab-admin", "wrong"),
    ],
    ids=["wrong_secret", "foreign_secret", "unknown_client", "admin_wrong_secret"],
)
def test_token_shim_refuses_bad_credentials(client, secret):
    """The lab's Unity token shim hands out no token for wrong credentials.

    Checks:
        * the client's correct secret gets a token (positive control; the unknown client has
          none);
        * the wrong secret is refused with 400 or 401.

    Args:
        client: Keycloak client the shim exchanges (``alice``, ``bob``, an unknown one).
        secret: The wrong secret sent for it.
    """
    _up(TOKEN_SHIM.rsplit("/", 1)[0])
    assert (
        _token(client, "alice-secret" if client == "alice" else f"{client}-secret").status_code
        == 200
        or client == "nobody"
    )
    r = _token(client, secret)
    assert r.status_code in (400, 401), (
        f"{client} with a bad secret got {r.status_code}: {r.text[:200]}"
    )


def test_token_shim_token_names_the_user():
    """The Unity token minted for alice belongs to alice, not to the shim or the admin.

    Checks:
        * Unity's who-am-I for that token names alice and not admin.
    """
    _up(TOKEN_SHIM.rsplit("/", 1)[0])
    token = _token("alice", "alice-secret").json()["access_token"]
    me = requests.get(
        f"{UNITY}/api/1.0/unity-control/scim2/Me",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    ).json()
    who = " ".join([me.get("externalId") or ""] + [e["value"] for e in me.get("emails", [])])
    assert "alice" in who and "admin" not in who, who


# =========================================================================== STS shim: logic


def test_sts_rewrite_keeps_deny_statements(shim):
    """The STS shim's policy rewrite keeps Deny statements (stricter), drops conditional Allows.

    Checks:
        * a Deny with a kms: condition stays, now unconditional;
        * an Allow of PutObject with such a condition is removed.

    Args:
        shim: The STS shim module (unity/sts_shim.py), imported for unit checks.
    """
    policy = {
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetO*"], "Resource": ["arn:aws:s3:::b/t/*"]},
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject"],
                "Resource": ["arn:aws:s3:::b/t/*"],
                "Condition": {"StringEquals": {"kms:EncryptionContext:x": "y"}},
            },
            {
                "Effect": "Deny",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::b/t/secret/*"],
                "Condition": {"StringNotEquals": {"kms:EncryptionContext:x": "y"}},
            },
        ]
    }
    out = json.loads(shim.rewrite_policy(json.dumps(policy)))["Statement"]
    effects = [(s["Effect"], tuple(s["Action"]), "Condition" in s) for s in out]
    assert ("Deny", ("s3:*",), False) in effects, f"Deny dropped or still conditional: {effects}"
    assert not any(e == "Allow" and "s3:PutObject" in a for e, a, _ in effects), effects


def test_sts_rewrite_only_expands_to_matching_actions(shim):
    """Wildcard actions are expanded only to actions the wildcard matches.

    Checks:
        * for ``s3:GetO*``, ``s3:*Multipart*``, ``s3:List*``, ``s3:Put*`` the rewritten policy
          lists at least one action, and every listed action matches the pattern.

    Args:
        shim: The STS shim module (unity/sts_shim.py), imported for unit checks.
    """
    import fnmatch

    for pattern in ("s3:GetO*", "s3:*Multipart*", "s3:List*", "s3:Put*"):
        out = json.loads(
            shim.rewrite_policy(
                json.dumps(
                    {"Statement": [{"Effect": "Allow", "Action": [pattern], "Resource": ["*"]}]}
                )
            )
        )
        actions = out["Statement"][0]["Action"] if out["Statement"] else []
        assert actions and all(fnmatch.fnmatchcase(a, pattern) for a in actions), (pattern, actions)


def _signed(body: bytes, access=CALLER[0], secret=CALLER[1], at=None) -> dict[str, str]:
    """Headers of an AssumeRole POST signed like an AWS SDK does (botocore).

    Args:
        body: Form-encoded body.
        access: Access key.
        secret: Secret key.
        at: Signing time (default: now).

    Returns:
        The headers.
    """
    import botocore.auth as ba
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    req = AWSRequest(
        method="POST",
        url="https://sts.us-east-1.amazonaws.com/",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
    )
    signer = ba.SigV4Auth(Credentials(access, secret), "sts", "us-east-1")
    if at is None:
        signer.add_auth(req)
    else:
        real = ba.datetime.datetime

        class Fixed(real):
            @classmethod
            def utcnow(cls):
                return at.replace(tzinfo=None)

            @classmethod
            def now(cls, tz=None):
                return at if tz else at.replace(tzinfo=None)

        ba.datetime.datetime = Fixed
        try:
            signer.add_auth(req)
        finally:
            ba.datetime.datetime = real
    headers = {k.lower(): v for k, v in req.headers.items()}
    headers.setdefault("host", "sts.us-east-1.amazonaws.com")
    return headers


BODY = b"Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fx&RoleSessionName=t&Policy=%7B%7D"


def test_sts_verify_accepts_an_sdk_signature(shim):
    """The STS shim accepts a request signed like the AWS SDK signs it.

    Checks:
        * ``verify_caller`` finds no problem with a correctly SigV4-signed AssumeRole request.

    Args:
        shim: The STS shim module (unity/sts_shim.py), imported for unit checks.
    """
    assert shim.verify_caller("POST", "/", _signed(BODY), BODY) is None


@pytest.mark.parametrize(
    "case", ["tampered_body", "wrong_key", "wrong_secret", "too_old", "unsigned"]
)
def test_sts_verify_refuses_bad_signatures(shim, case):
    """The STS shim refuses requests whose signature is not valid.

    Checks:
        * ``verify_caller`` reports a problem for the case.

    Args:
        shim: The STS shim module (unity/sts_shim.py), imported for unit checks.
        case: The bad signature: tampered body, wrong key, wrong secret, older than 15 min,
            unsigned.
    """
    now = datetime.datetime.now(datetime.UTC)
    headers, body = {
        "tampered_body": (_signed(BODY), BODY.replace(b"RoleSessionName=t", b"RoleSessionName=u")),
        "wrong_key": (_signed(BODY, access="other"), BODY),
        "wrong_secret": (_signed(BODY, secret="other"), BODY),
        "too_old": (_signed(BODY, at=now - datetime.timedelta(minutes=16)), BODY),
        "unsigned": ({"host": "sts.us-east-1.amazonaws.com"}, BODY),
    }[case]
    assert shim.verify_caller("POST", "/", headers, body) is not None, case


# =========================================================================== STS shim: live


def _assume(policy: str | None, sign=True) -> requests.Response:
    """AssumeRole against the running STS shim.

    Args:
        policy: Session policy (None: none).
        sign: Sign the request.

    Returns:
        The response.
    """
    form = {
        "Action": "AssumeRole",
        "Version": "2011-06-15",
        "RoleArn": "arn:aws:iam::123456789012:role/x",
        "RoleSessionName": "lab-adapter-test",
        "DurationSeconds": "900",
    }
    if policy is not None:
        form["Policy"] = policy
    from urllib.parse import urlencode

    body = urlencode(form).encode()
    headers = _signed(body) if sign else {"content-type": "application/x-www-form-urlencoded"}
    return requests.post(STS_SHIM, data=body, headers=headers, timeout=20)


def test_sts_shim_refuses_unsigned_requests():
    """The running STS shim answers an unsigned AssumeRole with 403 and no keys.

    Checks:
        * status 403 and no ``<AccessKeyId>`` in the answer.
    """
    _up(STS_SHIM)
    r = _assume('{"Statement": []}', sign=False)
    assert r.status_code == 403 and "<AccessKeyId>" not in r.text, (
        f"{r.status_code}: {r.text[:200]}"
    )


def test_sts_shim_refuses_requests_without_policy():
    """The running STS shim refuses AssumeRole without a session policy.

    Checks:
        * status 403 and no ``<AccessKeyId>`` in the answer (keys without a policy would reach
          the whole bucket).
    """
    _up(STS_SHIM)
    r = _assume(None)
    assert r.status_code == 403 and "<AccessKeyId>" not in r.text, (
        f"{r.status_code}: {r.text[:200]}"
    )


def test_sts_shim_keys_stay_in_their_prefix():
    """Keys issued for one prefix (wildcard actions, as Unity sends them) do not reach another.

    Checks:
        * AssumeRole with a policy for prefix a/ answers 200;
        * the keys read an object under a/;
        * reading under another prefix is refused.
    """
    import boto3
    from botocore.exceptions import ClientError

    _up(STS_SHIM)
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetO*", "s3:PutObject"],
                    "Resource": ["arn:aws:s3:::unitylab/lab-adapter-test/a/*"],
                },
            ],
        }
    )
    r = _assume(policy)
    assert r.status_code == 200, f"{r.status_code}: {r.text[:300]}"
    cred = {
        k: re.search(f"<{k}>([^<]+)</{k}>", r.text).group(1)
        for k in ("AccessKeyId", "SecretAccessKey", "SessionToken")
    }
    s3 = boto3.client(
        "s3",
        endpoint_url="http://rustfs:9000",
        region_name="us-east-1",
        aws_access_key_id=cred["AccessKeyId"],
        aws_secret_access_key=cred["SecretAccessKey"],
        aws_session_token=cred["SessionToken"],
    )
    s3.put_object(Bucket="unitylab", Key="lab-adapter-test/a/x", Body=b"x")
    assert s3.get_object(Bucket="unitylab", Key="lab-adapter-test/a/x")["Body"].read() == b"x"
    with pytest.raises(ClientError):
        s3.put_object(Bucket="unitylab", Key="lab-adapter-test/b/x", Body=b"x")
