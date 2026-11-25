"""Security of the catalogs (base tests, area "Security"): the same attacks on every catalog.

Part 1 - authentication. A catalog must refuse every credential that is not a valid one of its
own, with 401 (Iceberg REST: NotAuthorizedException) - a different code is recorded as a gap,
a request that gets through is a failure:

    garbage        not a token / wrong SigV4 secret
    unknown_key    SigV4 access key that does not exist
    alg_none       the valid token's claims, header {"alg": "none"}, no signature
    alg_confusion  the valid token's claims, signed with HS256 and a guessed key
    tampered       the valid token with a changed claim (signature kept) / request changed after
                   signing
    expired        a really expired token (Keycloak client with 5 s lifetime, used after 70 s)
                   / a SigV4 signature from 16 min ago
    other_issuer   a valid token of another Keycloak realm (master)
    no_audience    a valid token of the right realm without the catalog's audience

Every case has a positive control: the valid credential it was derived from works.

Token lifetime: a token the catalog accepts must not live longer than the identity provider's token
it stems from - otherwise a user disabled in Keycloak keeps access until the longer token expires.

Part 2 - escaping through the storage location. A table whose location (or data path) points into
another table's files would get the catalog's keys for those files - past every table permission.
Each way (same, inner or parent location, moved location, registered metadata of A or from
outside the warehouse, write.data.path) is tried against an existing table A; the catalog must
refuse it, or - if it accepts -
the keys it issues for the new table must not reach A's files. Without issued keys (RustFS) an
accepted overlap is a gap: only the user's own S3 policy is left as a barrier.

Part 3 - secrets. No response (config, table, issued keys, management API) contains a lab secret:
S3 root secret (only counted in a secret-named field: key id and secret are both "rustfsadmin" in
the lab), the catalog's root or database credentials, Keycloak client secrets. Loading a view
returns no storage keys, and Trino's query details (``/v1/query/{id}``) carry no secret.

Part 4 - error responses. A malformed request gets a 4xx (not a server error), and the answer shows
no stack trace, no file system path, no connection string and no internal class names.

Part 5 - names. A table or namespace name with "..", slashes, control characters, encoded dots,
Unicode or 300 characters is either refused (4xx), or accepted and then stays inside the warehouse
and can be loaded and dropped again with a normal client (PyIceberg).

Part 6 - privilege escalation (with the permission instances of tests/lab/authz_backends.py). alice
may only read t_select. She tries to grant herself rights with the catalog's own grant API (the
same call the admin uses), and to drop, rename or change t_select, create a table and drop the
namespace through Iceberg REST. Whatever the API answers, the admin then checks that nothing
changed and that alice still cannot read t_none.

Not testable in the lab: audit logs of refused access - no catalog offers an API to read them, and
the tests have no access to the containers' logs.

How each catalog authenticates in the lab (tests/lab/authz_backends.py):
    Lakekeeper  Keycloak tokens directly (permission instance lakekeeper-authz)
    Polaris     its own token broker (principal az-alice) and Keycloak as external identity
                provider ("mixed"; principal alice, tokens with scope=polaris) - the forged
                cases use Polaris' own token, the identity-provider cases Keycloak's
    Unity       its own tokens, issued by token exchange for Keycloak tokens (unitycatalog-authz);
                the Keycloak cases are checked at the exchange (tokens with scope=unity)
    RustFS      AWS SigV4 (S3 and the Iceberg catalog); Keycloak identities get temporary keys
                via STS AssumeRoleWithWebIdentity (clients rustfs-alice/-bob, policy from the
                token's claim rustfs_policy) - the token cases are checked at the STS
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import json
import re
import time
import uuid

import feature_inventory as inv
import pyarrow as pa
import pytest
import requests

from iceberg_lab import keycloak as kc
from iceberg_lab.config import (
    PROFILE,
    SETTINGS,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
    rest_base,
)
from iceberg_lab.engines import pyiceberg_catalog

from conftest import expect, find_gap, gap_table

KC = "http://keycloak:8080"
LAKEKEEPER = "http://lakekeeper-authz:8181"
POLARIS = "http://polaris:8181"
UNITY = "http://unitycatalog-authz:8080"
UNITY_SHIM = "http://unity-token-shim:8080/token"
RUSTFS = "http://rustfs:9000"

TOKEN_CASES = [
    "garbage",
    "alg_none",
    "alg_confusion",
    "tampered",
    "expired",
    "other_issuer",
    "no_audience",
]
SIGV4_CASES = ["garbage", "unknown_key", "tampered", "expired"]

# Filled from observed results only: (catalog, case, regex on "<status> <message>", cause).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "case"), [
    ("unity", "alg_none", r"not refused with 401: 409 Unsupported RSA algorithm: none", "unity-token-alg-409"),
    ("unity", "alg_confusion", r"not refused with 401: 409 Unsupported RSA algorithm: HS256", "unity-token-alg-409"),
    ("unity", "error_wrong_types", r"internal names in the error: com\.fasterxml\.jackson", "unity-error-internal-names"),
    ("unity", "error_unknown_update", r"server error for a client error: 500 .*Cannot convert metadata update action to json: explode", "unity-unknown-update-500"),
    ("lakekeeper", "name_table_dotdot", r"cannot be loaded again", "lakekeeper-dotdot-table-name"),
    ("unity", "token_lifetime", r"catalog token lives 86400 s, the identity token 3600 s", "unity-token-lifetime"),
    ("rustfs", "token_lifetime", r"catalog token lives 43200 s, the identity token 3600 s", "rustfs-sts-outlives-identity-token"),
    ("rustfs", "write_data_path", r"accepted an overlapping location, no issued keys", "rustfs-write-data-path-overlap"),
    # The REST fixture checks no location at all (and vends no keys: clients use their own).
    *(("reference", c, r"accepted an overlapping location, no issued keys", "reference-no-location-checks")
      for c in ("create_same", "create_inside", "create_parent", "set_location", "register_metadata", "register_outside", "write_data_path")),
    *(("reference", f"error_{c}", r"internal names in the error: org\.apache\.iceberg\.exceptions\.\w+Exception", "reference-error-stack-traces")
      for c in ("missing_table", "huge_name")),
    *(("reference", f"error_{c}", r"server error for a client error: 500 <html>", "reference-client-error-500")
      for c in ("broken_json", "wrong_types", "unknown_update")),
    # Gravitino checks no location against other tables and vends keys for whatever it accepted.
    *(("gravitino", c, r"keys issued for the new table read table A's file", "gravitino-no-location-checks")
      for c in ("create_same", "create_inside", "create_parent", "set_location", "register_metadata", "register_outside", "write_data_path")),
    *(("gravitino", f"error_{c}", r"internal names in the error: org\.(apache|glassfish)\.", "gravitino-error-stack-traces")
      for c in ("missing_table", "broken_json", "wrong_types", "unknown_update", "huge_name")),
    ("gravitino", "error_traversal", r"server error for a client error: 500 .*Authorization failed due to system internal error", "gravitino-dotted-names"),
    ("gravitino", "name_table_backslash", r"refused name 'a\\\\b' was created anyway", "gravitino-backslash-table-refused-created"),
    *(("gravitino", f"name_namespace_{n}", r"refused name .{1,40} (was created anyway|broke the listing)", "gravitino-dotted-names")
      for n in ("dotdot", "traversal")),
    *(("reference", f"name_{n}", r"accepted name .{1,80} cannot be loaded again: (BadRequestError|HTTPError|NoSuchNamespaceError)", "reference-names-unaddressable")
      for n in ("namespace_traversal", "table_slash", "namespace_slash", "table_backslash", "namespace_backslash", "namespace_newline", "table_percent", "namespace_percent")),
])
# fmt: on


def _gap(case):
    """``expect_gap`` for a documented security gap of this catalog.

    Args:
        case: Case name.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, case=case))


# =========================================================================== JWT helpers


def b64(raw: bytes) -> str:
    """base64url without padding (JWT encoding).

    Args:
        raw: Bytes to encode.

    Returns:
        The encoded text.
    """
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def unb64(text: str) -> bytes:
    """Decode base64url, adding the padding JWT leaves out.

    Args:
        text: base64url text (padding optional).

    Returns:
        The bytes.
    """
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _enc(obj) -> str:
    """JSON -> base64url (a JWT part).

    Args:
        obj: JSON answer (dict, list or value).

    Returns:
        The encoded part.
    """
    return b64(json.dumps(obj).encode())


def forge(good: str, case: str) -> str:
    """A bad token derived from the valid ``good`` (cases that need no other token).

    Args:
        good: A valid token of the catalog's identity provider.
        case: Case name.

    Returns:
        The bad token.
    """
    h, p, s = good.split(".")
    header, claims = json.loads(unb64(h)), json.loads(unb64(p))
    if case == "garbage":
        return "not-a-token"
    if case == "alg_none":
        return f"{_enc({**header, 'alg': 'none'})}.{p}."
    if case == "alg_confusion":
        hh = _enc({**header, "alg": "HS256"})
        sig = hmac.new(b"guessed-key", f"{hh}.{p}".encode(), hashlib.sha256).digest()
        return f"{hh}.{p}.{b64(sig)}"
    if case == "tampered":  # another identity, original signature
        return f"{h}.{_enc({**claims, 'sub': 'root', 'azp': 'lab-admin'})}.{s}"
    raise ValueError(case)


# =========================================================================== Keycloak


def keycloak_token(client: str, realm: str = "iceberg", scope: str | None = None) -> str:
    """Client-credentials token of a Keycloak client.

    Args:
        client: Keycloak client id.
        realm: Keycloak realm.
        scope: Optional scopes to request.

    Returns:
        The access token.
    """
    return kc.token(client, scope=scope, realm=realm)


def _keycloak_admin() -> dict:
    """Keycloak master-realm admin headers.

    Returns:
        The headers.
    """
    r = requests.post(
        f"{KC}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": "admin",
            "password": "admin",
        },
        timeout=10,
    )
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


#: Per catalog: (Keycloak client for the valid token, its scope, short-lived client and its
#: token mappers). Each token names only the catalog's own audience (src/iceberg_lab/keycloak.py).
TOKEN_CLIENTS = {
    "lakekeeper": ("alice", None, "az-shortlived", [kc.audience_mapper("lakekeeper")]),
    "unity": ("alice", "unity", "az-shortlived-unity", [kc.audience_mapper("unity")]),
    "polaris": ("alice", "polaris", "az-shortlived-polaris", [kc.audience_mapper("polaris")]),
    "rustfs": (
        "rustfs-alice",
        None,
        "az-shortlived-rustfs",
        [kc.audience_mapper("rustfs"), kc.policy_claim_mapper("az-alice")],
    ),
}


@pytest.fixture(scope="module")
def keycloak():
    """Bad Keycloak tokens for the catalog under test, among them one that is really expired
    (lifespan 5 s, used after 70 s - Lakekeeper tolerates ~60 s of clock skew, measured).

    Returns:
        Case -> bad token.
    """
    if PROFILE.name not in TOKEN_CLIENTS:  # the reference fixture: no authentication
        pytest.skip(f"catalog {PROFILE.name} has no API for token authentication")
    try:
        admin = _keycloak_admin()
    except requests.RequestException:
        pytest.skip("Keycloak not running (docker compose --profile authz up -d)")
    kc.ensure_service_scopes(admin)
    _client, _scope, shortlived, mappers = TOKEN_CLIENTS[PROFILE.name]
    kc.ensure_client(admin, shortlived, mappers, lifespan=5, only_mapped_audiences=True)
    kc.ensure_client(admin, "az-noaudience", [])  # audience "account" only
    expired = keycloak_token(shortlived)
    time.sleep(70)
    master = requests.post(
        f"{KC}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": "admin",
            "password": "admin",
        },
        timeout=10,
    ).json()["access_token"]
    return {
        "expired": expired,
        "other_issuer": master,
        "no_audience": keycloak_token("az-noaudience"),
    }


# =========================================================================== catalogs


def _status(r: requests.Response) -> str:
    """Status and error message of a response (for assertion messages).

    Args:
        r: HTTP response.

    Returns:
        ``<status> <message>``.
    """
    try:
        body = r.json()
        message = body.get("message") or body.get("error", {}).get("message", "")
    except ValueError:
        message = r.text[:200]
    return f"{r.status_code} {message}"


def _lakekeeper(token):
    """Lakekeeper-authz call with a token (list warehouses).

    Args:
        token: Bearer token to send.

    Returns:
        The response.
    """
    return requests.get(
        f"{LAKEKEEPER}/management/v1/whoami",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )


def _polaris_token():
    """Token of Polaris' root principal from its own broker.

    Returns:
        The access token.
    """
    r = requests.post(
        f"{POLARIS}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": "az-alice",
            "client_secret": "alice-secret",
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _polaris(token):
    """Polaris call with a token (list catalogs).

    Args:
        token: Bearer token to send.

    Returns:
        The response.
    """
    return requests.get(
        f"{POLARIS}/api/catalog/v1/config",
        params={"warehouse": "polaris_sts"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )


def _unity_token():
    """Unity token of lab-admin via the token shim.

    Returns:
        The access token.
    """
    r = requests.post(
        UNITY_SHIM,
        data={
            "grant_type": "client_credentials",
            "client_id": "alice",
            "client_secret": "alice-secret",
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _unity(token):
    """Unity-authz call with a token (list catalogs).

    Args:
        token: Bearer token to send.

    Returns:
        The response.
    """
    return requests.get(
        f"{UNITY}/api/2.1/unity-catalog/catalogs",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )


def _unity_exchange(subject_token):
    """Exchange a Keycloak token for a Unity token (RFC 8693).

    Args:
        subject_token: Keycloak token to exchange.

    Returns:
        The response.
    """
    return requests.post(
        f"{UNITY}/api/1.0/unity-control/auth/tokens",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": subject_token,
        },
        timeout=20,
    )


def _require(url):
    """Skip if the service at ``url`` is not running.

    Args:
        url: URL to probe.
    """
    try:
        requests.get(url, timeout=3)
    except requests.RequestException:
        pytest.skip(f"{url} not running (compose profile authz/unity)")


@pytest.mark.parametrize("case", TOKEN_CASES)
def test_bad_token_is_refused(case, request):
    """A forged, tampered, expired or foreign token is refused.

    Checks:
        * positive control: a valid token of the same identity works (200) - so a refusal is a
          refusal;
        * the bad token is refused with 401 (RustFS STS: 400/403 with an STS error code
          InvalidIdentityToken / AccessDenied / ExpiredTokenException).

    Args:
        case: The bad input (token, signature, location escape, error, escalation - see the
            module's case lists).
        request: pytest request (fixtures by name).
    """
    cat = PROFILE.name
    if cat == "polaris":
        from authz_backends import PolarisAuthz

        PolarisAuthz().ensure()  # principal "alice" for Keycloak identities
    via = None
    if cat == "lakekeeper":
        _require(f"{LAKEKEEPER}/health")
        good, probe = keycloak_token("alice"), _lakekeeper
    elif cat == "polaris":  # own tokens; Keycloak tokens (scope=polaris) for the IdP cases
        good, probe = _polaris_token(), _polaris
    elif cat == "rustfs":  # Keycloak identity -> temporary keys (AssumeRoleWithWebIdentity)
        _require(KC)
        good, probe = keycloak_token("rustfs-alice"), _rustfs_sts
    elif cat == "unity":  # own tokens at the API, Keycloak tokens at the exchange
        _require(f"{UNITY}/api/2.1/unity-catalog/catalogs")
        good, probe, via = _unity_token(), _unity, _unity_exchange
    else:
        pytest.skip(f"catalog {cat} has no API for authentication (open catalog)")
    assert probe(good).status_code == 200, f"positive control failed: {_status(probe(good))}"
    if case in ("expired", "other_issuer", "no_audience"):
        bad = request.getfixturevalue("keycloak")[case]
        if cat == "polaris":  # positive control: a valid Keycloak token works at Polaris
            r = _polaris(keycloak_token("alice", scope="polaris"))
            assert r.status_code == 200, f"positive control (Keycloak token): {_status(r)}"
        if via is not None:  # Unity: the Keycloak token is checked when it is exchanged
            assert via(keycloak_token("alice", scope="unity")).status_code == 200, (
                "positive control: exchange of a valid token"
            )
            probe = via
    else:
        bad = forge(good, case)
    r = probe(bad)
    with _gap(case):
        if cat == "rustfs":
            code = re.search(r"<Code>(\w+)</Code>", r.text)
            assert r.status_code in (400, 403) and code and code[1] in STS_REFUSALS, (
                f"not refused by STS: {r.status_code} {r.text[:300]}"
            )
        else:
            assert r.status_code == 401, f"not refused with 401: {_status(r)}"


#: How AWS STS refuses a web identity token (AWS: InvalidIdentityToken 400, AccessDenied 403).
STS_REFUSALS = {"InvalidIdentityToken", "AccessDenied", "ExpiredTokenException"}


def _rustfs_sts(token: str, duration: int = 900) -> requests.Response:
    """RustFS STS AssumeRoleWithWebIdentity with a token.

    Args:
        token: Bearer token to send.
        duration: Requested key lifetime in seconds.

    Returns:
        The response.
    """
    return requests.post(
        f"{RUSTFS}/",
        data={
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "WebIdentityToken": token,
            "DurationSeconds": str(duration),
        },
        timeout=10,
    )


def _sts_keys(token: str) -> tuple[str, str, str]:
    """Temporary keys for a token (asserts STS answers 200).

    Args:
        token: Bearer token to send.

    Returns:
        ``(access key, secret key, session token)``.
    """
    r = _rustfs_sts(token)
    assert r.status_code == 200, f"STS refused: {r.status_code} {r.text[:300]}"
    get = lambda tag: re.search(f"<{tag}>([^<]+)</{tag}>", r.text)[1]
    return get("AccessKeyId"), get("SecretAccessKey"), get("SessionToken")


def _rustfs_catalog(method: str, path: str, keys: tuple[str, str, str], body: dict | None = None):
    """Iceberg catalog call on RustFS, SigV4-signed with temporary keys.

    Args:
        method: HTTP method.
        path: S3 URI to read.
        keys: ``(access key, secret key, session token)``.
        body: JSON request body.

    Returns:
        The response.
    """
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    data = json.dumps(body).encode() if body is not None else b""
    req = AWSRequest(
        method=method,
        url=f"{RUSTFS}/iceberg/v1/{path}",
        data=data,
        headers={
            "x-amz-content-sha256": hashlib.sha256(data).hexdigest(),
            "Content-Type": "application/json",
        },
    )
    SigV4Auth(Credentials(*keys), "s3", "us-east-1").add_auth(req)
    return requests.request(
        method, req.url, data=data, headers=dict(req.headers.items()), timeout=15
    )


def test_identity_provider_policy_applies_on_rustfs():
    """Keycloak -> RustFS: a Keycloak identity gets exactly the rights of the IAM policy in its token.

    Checks:
        * lab setup: the IAM user az-alice reads the config (200) and may not create a
          namespace (403);
        * the Keycloak identity with ``rustfs_policy=az-alice`` gets the same answers (config,
          list/create namespace, S3 listing);
        * an identity naming an unknown policy gets 403 everywhere - no rights at all.
    """
    if PROFILE.name != "rustfs":
        pytest.skip(
            f"catalog {PROFILE.name} has no API for identity-provider STS (AssumeRoleWithWebIdentity)"
        )
    _require(KC)
    import boto3
    from authz_backends import RustFSAuthz

    authz = RustFSAuthz()
    authz.ensure()  # policy az-alice: catalog read rights, no namespace changes, no table data
    kc.ensure_service_scopes()
    ns = f"idp_{uuid.uuid4().hex[:6]}"

    def answers(keys):
        """Status codes of the same calls (config, list/create namespace, S3 list) with these keys.

        Args:
            keys: ``(access key, secret key[, session token])``.

        Returns:
            Call -> status code.
        """
        s3 = boto3.client(
            "s3",
            endpoint_url=RUSTFS,
            aws_access_key_id=keys[0],
            aws_secret_access_key=keys[1],
            aws_session_token=keys[2],
            region_name="us-east-1",
        )
        try:
            s3.list_objects_v2(Bucket="labtables", MaxKeys=1)
            s3_list = 200
        except s3.exceptions.ClientError as exc:
            s3_list = exc.response["ResponseMetadata"]["HTTPStatusCode"]
        return {
            "config": _rustfs_catalog("GET", "config?warehouse=labtables", keys).status_code,
            "list_namespaces": _rustfs_catalog("GET", "labtables/namespaces", keys).status_code,
            "create_namespace": _rustfs_catalog(
                "POST", "labtables/namespaces", keys, {"namespace": [ns]}
            ).status_code,
            "s3_list": s3_list,
        }

    static = answers((*authz.KEYS["alice"], None))
    federated = answers(_sts_keys(keycloak_token("rustfs-alice")))
    assert static["config"] == 200 and static["create_namespace"] == 403, f"lab setup: {static}"
    assert federated == static, f"Keycloak identity {federated} != IAM user {static}"
    r = _rustfs_sts(keycloak_token("rustfs-bob"))  # claim names policy az-bob (does not exist)
    if r.status_code == 200:  # keys issued: they must not open anything
        got = answers(_sts_keys(keycloak_token("rustfs-bob")))
        assert set(got.values()) == {403}, f"a token naming an unknown policy has rights: {got}"


# =========================================================================== RustFS (SigV4)


EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _sigv4_get(url, access="rustfsadmin", secret="rustfsadmin", at=None, tamper=False):
    """GET ``url`` signed with SigV4 (optionally at another time, or changed after signing).

    Args:
        url: URL to probe.
        access: Access key id.
        secret: Secret key.
        at: Signing time (default: now).
        tamper: Change the request after signing.

    Returns:
        The response.
    """
    import botocore.auth as ba
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    req = AWSRequest(method="GET", url=url, headers={"x-amz-content-sha256": EMPTY_SHA256})
    signer = ba.SigV4Auth(Credentials(access, secret), "s3", "us-east-1")
    if at is None:
        signer.add_auth(req)
    else:  # botocore takes the signing time from datetime.datetime.now/utcnow
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
    target = req.url + "&x=1" if tamper else req.url
    return requests.get(target, headers=dict(req.headers.items()), timeout=10)


@pytest.mark.parametrize("case", SIGV4_CASES)
@pytest.mark.parametrize("target", ["catalog", "storage"])
def test_bad_sigv4_is_refused(target, case):
    """RustFS refuses bad SigV4 signatures (catalog and storage).

    Checks:
        * positive control: a correctly signed GET answers 200;
        * a garbage signature, an unknown key, a request changed after signing and a signature
          older than 15 minutes are refused with 403.
        Catalogs with bearer tokens: not offered.

    Args:
        target: ``catalog`` or ``storage`` endpoint.
        case: The bad input (token, signature, location escape, error, escalation - see the
            module's case lists).
    """
    if PROFILE.name != "rustfs":
        pytest.skip(f"catalog {PROFILE.name} has no API for SigV4 request signing (bearer tokens)")
    url = (
        f"{RUSTFS}/iceberg/v1/config?warehouse=labtables"
        if target == "catalog"
        # GetBucketLocation: a signed request RustFS answers at once. A listing (even
        # max-keys=1) takes ~9 s on the table bucket and hit the 10 s timeout under load.
        else f"{RUSTFS}/labtables?location"
    )
    assert _sigv4_get(url).status_code == 200, "positive control failed"
    now = datetime.datetime.now(datetime.UTC)
    r = {
        "garbage": lambda: _sigv4_get(url, secret="wrong"),
        "unknown_key": lambda: _sigv4_get(url, access="nobody", secret="x"),
        "tampered": lambda: _sigv4_get(url, tamper=True),
        "expired": lambda: _sigv4_get(url, at=now - datetime.timedelta(minutes=16)),
    }[case]()
    assert r.status_code == 403, f"not refused: {r.status_code} {r.text[:200]}"


# =========================================================================== location escape

ESCAPES = [
    "create_same",
    "create_inside",
    "create_parent",
    "set_location",
    "register_metadata",
    "register_outside",
    "write_data_path",
]
#: Inventory capability whose endpoint each case needs (the rest: create/commit a table).
ESCAPE_CAPABILITY = {c: "iceberg_tables" for c in ESCAPES} | {
    "register_metadata": "iceberg_register",
    "register_outside": "iceberg_register",
}
SCHEMA = pa.schema([("id", pa.int64()), ("secret", pa.string())])


@pytest.fixture(scope="module")
def victim(namespace):
    """Table A with one data file; returns (catalog, namespace, table, path of A's data file).

    Args:
        namespace: Namespace of the table.

    Returns:
        ``(catalog, namespace, table A, path of A's data file)``.
    """
    cat = pyiceberg_catalog(WAREHOUSE_STS)
    a = cat.create_table(f"{namespace}.victim", schema=SCHEMA)
    a.append(pa.table({"id": [1], "secret": ["geheim"]}))
    a = cat.load_table(f"{namespace}.victim")
    return cat, namespace, a, a.inspect.files().column("file_path").to_pylist()[0]


def _keys_reach(table, path) -> bool | None:
    """Do the keys the catalog issued for ``table`` read ``path``? None: no issued keys.

    Args:
        table: PyIceberg table whose issued keys are tested.
        path: S3 URI to read.

    Returns:
        True if they read it, False if refused, None if there are no keys.
    """
    import boto3
    import botocore
    from botocore.exceptions import ClientError

    p = table.io.properties
    if "s3.session-token" not in p:  # static client keys, not issued by the catalog
        return None
    s3 = boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=p["s3.access-key-id"],
        aws_secret_access_key=p["s3.secret-access-key"],
        aws_session_token=p["s3.session-token"],
        config=botocore.config.Config(retries={"max_attempts": 1}),
    )
    bucket, key = path.removeprefix("s3://").split("/", 1)
    try:
        s3.get_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


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


def _outside(a) -> str:
    """A prefix in the same bucket, but outside the warehouse/catalog location.

    Args:
        a: Table A (PyIceberg table).

    Returns:
        The prefix URI.
    """
    bucket = a.location().removeprefix("s3://").split("/", 1)[0]
    return f"s3://{bucket}/outside_{uuid.uuid4().hex[:6]}"


def _plant_outside(a) -> str:
    """Put a foreign file outside the warehouse (for the register escape).

    Args:
        a: Table A (PyIceberg table).

    Returns:
        Its URI.
    """
    path = f"{_outside(a)}/data/foreign.parquet"
    bucket, key = path.removeprefix("s3://").split("/", 1)
    _s3_root().put_object(Bucket=bucket, Key=key, Body=b"foreign data")
    return path


def _metadata_outside(a, planted: str) -> str:
    """A copy of A's metadata whose table location is the planted file's prefix.

    Args:
        a: Table A (PyIceberg table).
        planted: URI of the planted foreign file.

    Returns:
        URI of the written metadata file.
    """
    location = planted.rsplit("/data/", 1)[0]
    md = json.loads(a.metadata.model_dump_json(by_alias=True))
    md |= {"location": location, "table-uuid": str(uuid.uuid4())}
    bucket, key = f"{location}/metadata/00000-{uuid.uuid4()}.metadata.json".removeprefix(
        "s3://"
    ).split("/", 1)
    _s3_root().put_object(Bucket=bucket, Key=key, Body=json.dumps(md).encode())
    return f"s3://{bucket}/{key}"


@pytest.mark.parametrize("case", ESCAPES)
def test_location_cannot_reach_another_table(case, victim):
    """A new table cannot get at table A's files through its location or metadata.

    Cases: create at A's location, inside it, at its parent; move a table's location there;
    register A's metadata file; register a metadata file outside the warehouse; point
    write.data.path into A.

    Checks:
        * the catalog refuses (the refusal names the location / allowed locations / metadata
          directory),
        * or, if accepted, the new table's issued keys do NOT reach A's data file.
        Catalogs without the API for the case: not offered.

    Args:
        case: The bad input (token, signature, location escape, error, escalation - see the
            module's case lists).
        victim: ``(catalog, namespace, table A, path of A's data file)``.
    """
    from pyiceberg.table.update import AssertTableUUID, SetLocationUpdate

    cat, ns, a, a_file = victim
    name = f"{ns}.esc_{case}"
    loc = a.location().rstrip("/")
    try:
        if case == "create_same":
            b = cat.create_table(name, schema=SCHEMA, location=loc)
        elif case == "create_inside":
            b = cat.create_table(name, schema=SCHEMA, location=f"{loc}/data")
        elif case == "create_parent":
            b = cat.create_table(name, schema=SCHEMA, location=loc.rsplit("/", 1)[0])
        elif case == "set_location":
            b = cat.create_table(name, schema=SCHEMA)
            cat.commit_table(
                b,
                (AssertTableUUID(uuid=b.metadata.table_uuid),),
                (SetLocationUpdate(location=loc),),
            )
        elif case == "register_metadata":
            b = cat.register_table(name, a.metadata_location)
        elif case == "register_outside":
            # Polaris CVE-2026-64640: metadata whose location lies outside every location the
            # catalog allows. The planted file stands for foreign data there.
            a_file = _plant_outside(a)
            b = cat.register_table(name, _metadata_outside(a, a_file))
        else:  # write_data_path
            b = cat.create_table(
                name, schema=SCHEMA, properties={"write.data.path": a_file.rsplit("/", 1)[0]}
            )
    except NotImplementedError as exc:  # PyIceberg: the catalog does not offer the endpoint
        # Only "no API" when the catalog's own API description lacks it too - otherwise the
        # catalog describes the endpoint but does not serve it, and the test fails.
        if PROFILE.name in inv.offered_by(inv.CAPABILITIES[ESCAPE_CAPABILITY[case]]):
            raise
        pytest.skip(f"catalog {PROFILE.name} has no API for this ({exc})")
    except Exception as exc:  # noqa: BLE001 - refused: that is the expected outcome
        if case == "register_outside":  # refused for the location, not for a broken file
            assert re.search(r"(?i)location|allowed|metadata directory", str(exc)), exc
        return
    b = cat.load_table(name)
    reach = _keys_reach(b, a_file)
    with _gap(case):
        assert reach is False, (
            f"keys issued for the new table read table A's file {a_file}"
            if reach
            else f"accepted an overlapping location, no issued keys ({b.location()})"
        )


# =========================================================================== secrets

_SECRET_FIELD = re.compile(r"secret|password|private|token", re.IGNORECASE)


def lab_secrets() -> dict[str, str]:
    """Secrets of the lab that no answer may contain.

    Returns:
        Secret -> what it is.
    """
    found = {
        SETTINGS.s3_secret_key: "S3 root secret",
        "This-is-NOT-Secure!": "Lakekeeper encryption key",
        "postgres:postgres": "database password",
        "lab-admin-secret": "Keycloak client secret",
        "alice-secret": "Keycloak client secret",
        "bob-secret": "Keycloak client secret",
    }
    if PROFILE.oauth_credential:
        found[PROFILE.oauth_credential.split(":", 1)[1]] = "catalog root secret"
    return found


def secrets_in(obj, path="") -> list[str]:
    """Lab secrets in a JSON answer, as "<what> in <field path>".

    Args:
        obj: JSON answer (dict, list or value).
        path: Field path so far (recursion).

    Returns:
        The findings.
    """
    if isinstance(obj, dict):
        return [h for k, v in obj.items() for h in secrets_in(v, f"{path}.{k}")]
    if isinstance(obj, list):
        return [h for i, v in enumerate(obj) for h in secrets_in(v, f"{path}[{i}]")]
    if not isinstance(obj, str):
        return []
    return [
        f"{what} in {path}"
        for value, what in lab_secrets().items()
        if value in obj and (value != SETTINGS.s3_secret_key or _SECRET_FIELD.search(path))
    ]


def test_secret_scan_finds_a_planted_secret():
    """Self-check of the secret scan: a secret in a secret-named field is found, a key id is not.

    Checks:
        * scanning a planted response finds exactly the S3 root secret.
    """
    planted = {
        "config": {
            "s3.access-key-id": SETTINGS.s3_access_key,
            "s3.secret-access-key": SETTINGS.s3_secret_key,
        }
    }
    assert secrets_in(planted) == ["S3 root secret in .config.s3.secret-access-key"]


MANAGEMENT = {
    "lakekeeper": ["{lakekeeper}/management/v1/warehouse"],
    "polaris": ["http://polaris:8181/api/management/v1/catalogs"],
    "unity": [
        "http://unitycatalog:8080/api/2.1/unity-catalog/credentials",
        "http://unitycatalog:8080/api/2.1/unity-catalog/external-locations",
        "http://unitycatalog:8080/api/2.1/unity-catalog/catalogs",
    ],
    "rustfs": [],
}


def _base() -> str:
    """REST base URL of the STS warehouse: ``<catalog>/v1[/<prefix>]`` (``config.rest_base``).

    Returns:
        The URL.
    """
    return rest_base(WAREHOUSE_STS)


def test_responses_contain_no_secrets(victim):
    """Config, table, issued keys and the management API never return a lab secret.

    Checks:
        * each answer (200) contains none of the lab's root secrets or client secrets.

    Args:
        victim: ``(catalog, namespace, table A, path of A's data file)``.
    """
    _, ns, a, _ = victim
    table = f"{_base()}/namespaces/{ns}/tables/{a.name()[-1]}"
    urls = [
        (f"{SETTINGS.catalog_uri}/v1/config?warehouse={WAREHOUSE_STS}", {}),
        (table, {"X-Iceberg-Access-Delegation": "vended-credentials"}),
        (f"{table}/credentials", {}),
    ] + [
        (u.format(lakekeeper=SETTINGS.lakekeeper_url), {}) for u in MANAGEMENT.get(PROFILE.name, [])
    ]
    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    has_credentials = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials" in (
        cfg.get("endpoints") or []
    )
    hits = []
    for url, extra in urls:
        r = requests.get(
            url, headers={**catalog_headers(), **extra}, auth=catalog_auth(), timeout=10
        )
        if url.endswith("/credentials") and not has_credentials and r.status_code in (400, 404):
            continue  # not offered (not advertised, refused): nothing to leak
        assert r.status_code == 200, f"{url}: {r.status_code}"
        hits += [f"{url}: {h}" for h in secrets_in(r.json())]
    assert not hits, "\n".join(hits)


STORAGE_KEYS = ("s3.access-key-id", "s3.secret-access-key", "s3.session-token")


def test_view_responses_carry_no_storage_keys(namespace):
    """Loading a view returns no storage keys, even when asked for vended credentials.

    Checks:
        * the view's create and load answers contain no S3 keys.
        Catalogs without views: not offered.

    Args:
        namespace: Fresh namespace of the test session.
    """
    base = f"{_base()}/namespaces/{namespace}/views"
    name = f"sec_v_{uuid.uuid4().hex[:6]}"
    body = {
        "name": name,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "type": "long", "required": False}],
        },
        "view-version": {
            "version-id": 1,
            "timestamp-ms": int(time.time() * 1000),
            "schema-id": 0,
            "summary": {"engine-name": "lab"},
            "representations": [{"type": "sql", "sql": "select 1 as id", "dialect": "spark"}],
            "default-namespace": [namespace],
        },
        "properties": {},
    }
    headers = {**catalog_headers(), "X-Iceberg-Access-Delegation": "vended-credentials"}
    created = requests.post(base, json=body, headers=headers, auth=catalog_auth(), timeout=15)
    if created.status_code in (404, 405, 501):
        pytest.skip(f"catalog {PROFILE.name} has no API for views ({created.status_code})")
    created.raise_for_status()
    loaded = requests.get(f"{base}/{name}", headers=headers, auth=catalog_auth(), timeout=15)
    loaded.raise_for_status()
    hits = []
    for what, answer in (("create", created.json()), ("load", loaded.json())):
        config = answer.get("config", {}) | {
            k: v
            for c in answer.get("storage-credentials", [])
            for k, v in c.get("config", {}).items()
        }
        hits += [f"{what}: {k}" for k in STORAGE_KEYS if config.get(k)]
        hits += [f"{what}: {h}" for h in secrets_in(answer)]
    with _gap("view_keys"):
        assert not hits, f"storage keys in view responses: {hits}"


def test_trino_query_details_carry_no_secrets(namespace, engine_pool):
    """Trino's query details (REST /v1/query/{id}) contain no secret.

    Checks:
        * after a marker query, its details contain no storage or catalog secret of the lab and
          no vended keys (CVE-2026-34214: Trino showed catalog credentials there).

    Args:
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    from conftest import require_engine

    require_engine("trino")
    trino = engine_pool("trino")
    table = f"sec_tq_{uuid.uuid4().hex[:6]}"
    pyiceberg_catalog(WAREHOUSE_STS).create_table(
        f"{namespace}.{table}", schema=pa.schema([("id", pa.int64())])
    ).append(pa.table({"id": [1]}))
    marker = f"lab_marker_{uuid.uuid4().hex[:8]}"
    trino.sql(f"SELECT count(*) /* {marker} */ FROM {trino.fq(namespace, table)}")
    (query_id,) = trino.sql(
        "SELECT query_id FROM system.runtime.queries "
        f"WHERE query LIKE '%{marker}%' AND query NOT LIKE '%system.runtime%'"
    )[0]
    r = requests.get(
        f"http://{SETTINGS.trino_host}:{SETTINGS.trino_port}/v1/query/{query_id}",
        headers={"X-Trino-User": "tests"},
        timeout=15,
    )
    r.raise_for_status()
    text = r.text
    hits = secrets_in(r.json())
    hits += [
        f"value of {m[0]}"
        for m in re.findall(
            r'"?(s3\.(secret-access-key|session-token)|secret[-_]?key)"?\s*[:=]\s*"([^"]{8,})"',
            text,
        )
    ]
    with _gap("trino_query_details"):
        assert not hits, f"secrets in Trino's query details: {hits}"


# =========================================================================== error responses

_LEAK = re.compile(
    r"(\tat |\bat (org|com|java|io)\.[\w.$]+\(|Traceback|\.java:\d+|\.rs:\d+|/home/|/usr/|/opt/|"
    r"postgres(ql)?://|jdbc:|\b(com|org)\.[a-z]+\.[\w.]+\b)"
)
ERRORS = ["missing_table", "broken_json", "wrong_types", "unknown_update", "huge_name", "traversal"]


@pytest.mark.parametrize("case", ERRORS)
def test_error_response_is_clean(case):
    """A malformed request gets a 4xx whose body reveals no internals.

    Checks:
        * missing table, broken JSON, wrong types, unknown update, huge name, path traversal:
          the answer is a 4xx (not 5xx);
        * the body names no internal classes or stack traces.

    Args:
        case: The bad input (token, signature, location escape, error, escalation - see the
            module's case lists).
    """
    v1 = f"{_base()}"
    missing = f"nope_{uuid.uuid4().hex[:6]}"
    js = {"Content-Type": "application/json"}
    method, url, body, extra = {
        "missing_table": ("GET", f"{v1}/namespaces/{missing}/tables/t", None, {}),
        "broken_json": ("POST", f"{v1}/namespaces", "{not json", js),
        "wrong_types": (
            "POST",
            f"{v1}/namespaces",
            json.dumps({"namespace": 5, "properties": []}),
            js,
        ),
        "unknown_update": (
            "POST",
            f"{v1}/namespaces/{missing}/tables/t",
            json.dumps({"requirements": [], "updates": [{"action": "explode"}]}),
            js,
        ),
        "huge_name": ("GET", f"{v1}/namespaces/{'x' * 5000}", None, {}),
        "traversal": ("GET", f"{v1}/namespaces/..%2F..%2Fetc", None, {}),
    }[case]
    r = requests.request(
        method,
        url,
        data=body,
        headers={**catalog_headers(), **extra},
        auth=catalog_auth(),
        timeout=15,
    )
    with _gap(f"error_{case}"):
        assert 400 <= r.status_code < 500, (
            f"server error for a client error: {r.status_code} {r.text[:200]}"
        )
        leak = _LEAK.search(r.text)
        assert not leak, f"internal names in the error: {leak.group(0)} ({r.text[:200]})"


# =========================================================================== names

NAMES = {
    "dotdot": "..",
    "traversal": "../../escape",
    "slash": "a/b",
    "backslash": "a\\b",
    "nul": "a\x00b",
    "newline": "a\nb",
    "percent": "%2e%2e",
    "unicode": "Grüße_🧊",
    "long": "x" * 300,
}


def _raw_cleanup(parent: str) -> None:
    """Remove a test parent namespace even if a child name cannot be addressed by PyIceberg.

    Args:
        parent: Parent namespace to remove.
    """
    import http.client
    from urllib.parse import quote

    if PROFILE.name == "lakekeeper":  # Lakekeeper extension: recursive drop
        url = f"{_base()}/namespaces/{parent}"
        requests.delete(
            url,
            params={"recursive": "true", "purge": "true"},
            headers=catalog_headers(),
            auth=catalog_auth(),
            timeout=30,
        )
        return
    if PROFILE.name != "polaris":
        return

    def call(method, path):
        """Raw HTTP call without path normalisation (keeps ``..`` and encoded names).

        Args:
            method: HTTP method.
            path: Path as sent.

        Returns:
            The status code.
        """
        conn = http.client.HTTPConnection("polaris", 8181, timeout=20)
        conn.request(method, path, headers=catalog_headers())
        r = conn.getresponse()
        return r.status, r.read()

    base = f"/api/catalog{_base().split('/api/catalog', 1)[1]}/namespaces"  # Polaris
    _, body = call("GET", f"{base}?parent={quote(parent)}")
    for child in json.loads(body or b"{}").get("namespaces", []):
        for enc in (quote(child[-1], safe=""), quote(quote(child[-1], safe=""), safe="")):
            call("DELETE", f"{base}/{quote(parent)}%1F{enc}")
    call("DELETE", f"{base}/{quote(parent)}")


@pytest.fixture
def name_parent():
    """A fresh parent namespace for the name tests; removed afterwards (raw if needed).

    Yields:
        ``(catalog, parent)``.
    """
    cat = pyiceberg_catalog(WAREHOUSE_STS)
    parent = f"test_{uuid.uuid4().hex[:8]}"
    cat.create_namespace(parent)
    yield cat, parent
    try:
        for child in cat.list_namespaces(parent):
            cat.drop_namespace(child)
        for t in cat.list_tables(parent):
            cat.drop_table(t)
        cat.drop_namespace(parent)
    except Exception:  # noqa: BLE001 - a child the client cannot address: remove it raw
        _raw_cleanup(parent)


def _namespace_separator() -> str:
    """The namespace separator the catalog asks for in ``/v1/config`` (``namespace-separator``,
    URL-encoded; default ``%1F``). The REST fixture asks for ``%2E`` - a client must follow it.

    Returns:
        The encoded separator.
    """
    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    sep = {**cfg.get("defaults", {}), **cfg.get("overrides", {})}.get("namespace-separator")
    return sep or "%1F"


def _namespace_roundtrip(ident: tuple[str, ...], nested: bool) -> None:
    """Load a namespace, list its tables (and children), drop it - each with one correct
    encoding (path: parts percent-encoded, joined by the catalog's separator; query: encoded
    once by requests).

    Args:
        ident: Namespace as tuple.
        nested: Also list child namespaces.
    """
    from urllib.parse import quote, unquote

    sep = _namespace_separator()
    base = f"{_base()}/namespaces"
    path = sep.join(quote(part, safe="") for part in ident)
    kw = {"headers": catalog_headers(), "auth": catalog_auth(), "timeout": 15}
    for method, url, params in (
        ("GET", f"{base}/{path}", None),
        ("GET", f"{base}/{path}/tables", None),
        *((("GET", base, {"parent": unquote(sep).join(ident)}),) if nested else ()),
        ("DELETE", f"{base}/{path}", None),
    ):
        r = requests.request(method, url, params=params, **kw)
        r.raise_for_status()


def _refusal_left_nothing(cat, ident: tuple[str, ...], kind: str) -> None:
    """A refused name must not exist anyway, and the listing of its parent must still work.

    Gravitino answers a namespace name with a dot with 400 but creates it nevertheless; every
    top-level listing then fails. Such a leftover is removed from the backend right away, so
    later tests still find a usable catalog.

    Args:
        cat: PyIceberg catalog.
        ident: The refused identifier.
        kind: ``table`` or ``namespace``.
    """
    parent, name = ident[:-1], ident[-1]
    where = ".".join(parent) or "the top level"
    try:
        listed = cat.list_tables(parent) if kind == "table" else cat.list_namespaces(parent)
    except Exception as exc:
        _drop_from_backend(ident, kind)
        raise AssertionError(
            f"refused name {name!r} broke the listing of {where}: {type(exc).__name__}: {exc}"
        ) from exc
    if tuple(ident) in {tuple(x) for x in listed}:
        _drop_from_backend(ident, kind)
        raise AssertionError(f"refused name {name!r} was created anyway in {where}")


def _drop_from_backend(ident: tuple[str, ...], kind: str) -> None:
    """Delete an object the catalog cannot address any more directly in its database.

    Only Gravitino (JDBC backend in Postgres database ``gravitino``, one ``catalog_name`` per
    lab catalog); elsewhere nothing to do here.

    Args:
        ident: The identifier.
        kind: ``table`` or ``namespace``.
    """
    if PROFILE.name != "gravitino":
        return
    import psycopg

    with psycopg.connect(
        host="db", dbname="gravitino", user="postgres", password="postgres", autocommit=True
    ) as conn:
        if kind == "namespace":
            conn.execute(
                "DELETE FROM iceberg_namespace_properties WHERE catalog_name = %s AND namespace = %s",
                (WAREHOUSE_STS, ".".join(ident)),
            )
        else:
            conn.execute(
                "DELETE FROM iceberg_tables WHERE catalog_name = %s AND table_namespace = %s"
                " AND table_name = %s",
                (WAREHOUSE_STS, ".".join(ident[:-1]), ident[-1]),
            )


@pytest.mark.parametrize("kind", ["table", "namespace"])
@pytest.mark.parametrize("label", list(NAMES))
def test_name_is_refused_or_safe(label, kind, name_parent):
    """A dangerous name is refused, or the object stays in the warehouse and stays addressable.

    Names: ``..``, ``../../escape``, slash, backslash, NUL, newline, percent-encoded dots,
    Unicode, 300 characters.

    Checks:
        * either the catalog refuses the name - and then it does not exist anyway and the
      listing of its parent still works,
        * or the object's location stays inside the warehouse (no ``/../``) and it can be loaded
          again under its name.

    Args:
        label: Name of the dangerous name case (see ``NAMES``).
        kind: ``table`` or ``namespace``.
        name_parent: A fresh parent namespace for the name tests.
    """
    cat, parent = name_parent
    name = NAMES[label]
    # Unity and RustFS: one namespace level. Under the parent where nesting exists, so a name the
    # catalog accepts but cannot address afterwards (the REST fixture) stays out of the top level
    # - there it broke every later top-level listing (DuckDB: "empty namespace name").
    nested = PROFILE.name in ("lakekeeper", "polaris", "reference")  # test_namespace[*-nested]
    ident = (parent, name) if kind == "table" or nested else (f"{name}_{uuid.uuid4().hex[:3]}",)
    try:
        if kind == "table":
            t = cat.create_table(ident, schema=SCHEMA)
        else:
            cat.create_namespace(ident)
    except Exception:  # noqa: BLE001 - refused: expected for most of these names
        with _gap(f"name_{kind}_{label}"):
            _refusal_left_nothing(cat, ident, kind)
        return
    with _gap(f"name_{kind}_{label}"):
        if kind == "table":
            root = PROFILE.location.rstrip("/") + "/"
            assert t.location().startswith(root) and "/../" not in t.location() + "/", (
                f"table {name!r} lies outside the warehouse {root}: {t.location()}"
            )
        try:
            if kind == "table":
                cat.load_table(ident)
                cat.drop_table(ident)
            else:  # raw, correctly encoded REST calls - PyIceberg double-encodes ?parent=
                _namespace_roundtrip(ident, nested)
        except Exception as exc:
            raise AssertionError(
                f"accepted name {name!r} cannot be loaded again: {type(exc).__name__}: {exc}"
            ) from exc


# =========================================================================== escalation


@pytest.fixture(scope="module")
def az():
    """Permission backend with its namespace (RustFS: a hidden rename leftover only warns).

    Yields:
        ``(backend, namespace)``.
    """
    from authz_backends import backend

    b = backend()
    ns = b.setup_namespace()
    yield b, ns
    try:
        b.drop_namespace(ns)
    except Exception as exc:  # RustFS: a rename leaves a hidden entry (known gap)
        if PROFILE.name != "rustfs":
            raise
        import warnings

        warnings.warn(
            f"RustFS: namespace {ns} keeps a renamed table's old name ({exc})", stacklevel=2
        )


def _self_grant(b, ns):
    """alice calls the catalog's grant API exactly like the admin does, with her own credentials.

    Args:
        b: Permission backend.
        ns: Namespace of the backend's tables.

    Returns:
        The catalog's answer.
    """
    from authz_backends import pyiceberg_for

    cat = PROFILE.name
    if cat == "lakekeeper":
        t = pyiceberg_for(b.admin()).load_table(f"{ns}.t_none")
        return requests.post(
            f"{b.base}/management/v1/permissions/warehouse/{b.warehouse_ids[b.wh]}/table/{t.metadata.table_uuid}/assignments",
            json={
                "writes": [
                    {"type": "select", "user": b.user_ids["alice"]},
                    {"type": "modify", "user": b.user_ids["alice"]},
                ]
            },
            headers=b._h("alice"),
            timeout=10,
        )
    if cat == "polaris":
        token = requests.post(
            f"{b.base}/api/catalog/v1/oauth/tokens",
            data={
                "grant_type": "client_credentials",
                "client_id": "az-alice",
                "client_secret": "alice-secret",
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            timeout=10,
        ).json()["access_token"]
        return requests.put(
            f"{b.base}/api/management/v1/catalogs/{WAREHOUSE_STS}/catalog-roles/az_alice/grants",
            json={
                "grant": {
                    "type": "table",
                    "namespace": [ns],
                    "tableName": "t_none",
                    "privilege": "TABLE_READ_DATA",
                }
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
    if cat == "unity":
        return requests.patch(
            f"{b.base}/api/2.1/unity-catalog/permissions/table/{b.catalog_name}.{ns}.t_none",
            json={"changes": [{"principal": "alice@lab.local", "add": ["SELECT", "MODIFY"]}]},
            headers=b._h("alice"),
            timeout=10,
        )
    if cat == "gravitino":  # alice grants her own role read + write on t_none
        typ, full = b._object("table", ns, "t_none")
        return requests.put(
            f"{b.api}/permissions/roles/az_alice/{typ}/{full}/grant",
            json={"privileges": b._privs("SELECT_TABLE", "MODIFY_TABLE")},
            headers=b._h("alice"),
            timeout=10,
        )
    # rustfs: rights are IAM policies - alice tries to attach the admin's built-in policy to herself
    from minio import MinioAdmin
    from minio.credentials import StaticProvider

    adm = MinioAdmin(
        endpoint=SETTINGS.s3_endpoint.split("//", 1)[1],
        credentials=StaticProvider(*b.KEYS["alice"]),
        secure=False,
    )
    try:
        adm.policy_set("consoleAdmin", user="az-alice")
    except Exception as exc:  # noqa: BLE001 - refusal is the expected outcome; the effect is checked below
        return exc
    return None


def test_cannot_grant_oneself_rights(az):
    """alice cannot grant herself rights.

    Checks:
        * her call to the grant API is refused (401/403);
        * she still cannot read t_none afterwards.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and a namespace with
            t_select (alice: read), t_modify (alice: read + write), t_none (no grant).
    """
    b, ns = az
    try:
        answer = _self_grant(b, ns)
        try:
            b.catalog("alice").load_table(f"{ns}.t_none").scan().to_arrow()
            read = True
        except Exception:  # noqa: BLE001 - refused, as it must be
            read = False
    finally:
        if PROFILE.name == "rustfs":  # never leave alice escalated: put her lab policy back
            b._apply()
    status = getattr(answer, "status_code", "no error")
    with _gap("self_grant"):
        assert not read, (
            f"alice reads t_none after granting herself rights (grant API answered {status})"
        )
        if isinstance(answer, requests.Response):
            assert answer.status_code in (401, 403), (
                f"grant API accepted alice's call: {answer.status_code} {answer.text[:200]}"
            )


ESCALATIONS = ["drop_table", "rename_table", "set_properties", "create_table", "drop_namespace"]


@pytest.mark.parametrize("case", ESCALATIONS)
def test_reader_cannot_change_the_catalog(case, az):
    """alice (read on t_select only) cannot change the catalog.

    Checks:
        * dropping, renaming, changing properties of t_select, creating a table and dropping the
          namespace are refused;
        * t_select is unchanged afterwards (same metadata location).

    Args:
        case: The bad input (token, signature, location escape, error, escalation - see the
            module's case lists).
        az: ``(backend, namespace)`` - the catalog's permission backend and a namespace with
            t_select (alice: read), t_modify (alice: read + write), t_none (no grant).
    """
    b, ns = az
    alice = b.catalog("alice")
    admin = b.catalog("admin")
    table = f"t_esc_{case}"  # its own table per case: an allowed drop must not break the next case
    admin.create_table(f"{ns}.{table}", schema=SCHEMA).append(
        pa.table({"id": [1], "secret": ["x"]})
    )
    b.grant(ns, table, write=False)
    before = admin.load_table(f"{ns}.{table}")
    try:
        if case == "drop_table":
            alice.drop_table(f"{ns}.{table}")
        elif case == "rename_table":
            alice.rename_table(f"{ns}.{table}", f"{ns}.t_stolen")
        elif case == "set_properties":
            with alice.load_table(f"{ns}.{table}").transaction() as tx:
                tx.set_properties({"owner": "alice"})
        elif case == "create_table":
            alice.create_table(f"{ns}.t_alice", schema=SCHEMA)
        else:  # an empty namespace of its own: "not empty" must not stand in for a refusal
            admin.create_namespace(f"{ns}_empty")
            alice.drop_namespace(f"{ns}_empty")
        done = True
    except Exception as exc:  # noqa: BLE001
        done = False
        refusal = f"{type(exc).__name__}: {exc}"
    still = admin.table_exists(f"{ns}.{table}")
    after = admin.load_table(f"{ns}.{table}") if still else None
    for leftover in ("t_alice", "t_stolen", table):  # undo whatever alice managed to do
        if admin.table_exists(f"{ns}.{leftover}"):
            admin.drop_table(f"{ns}.{leftover}")
    if case == "drop_namespace" and not done:
        admin.drop_namespace(f"{ns}_empty")
    with _gap(f"escalate_{case}"):
        assert not done, f"alice could {case.replace('_', ' ')}"
        assert after is not None and after.metadata_location == before.metadata_location, (
            f"{table} changed"
        )
    assert (
        re.search(
            r"(?i)401|403|forbidden|not authori[sz]ed|permission|access[ _]?denied|not allowed",
            refusal,
        )
        or PROFILE.name == "lakekeeper"
    ), refusal


# =========================================================================== token lifetime


def _claims(token: str) -> dict:
    """Claims of a JWT (unverified).

    Args:
        token: Bearer token to send.

    Returns:
        The claims.
    """
    return json.loads(unb64(token.split(".")[1]))


def test_catalog_token_does_not_outlive_identity_token(request):
    """What the catalog accepts expires no later than the Keycloak token it stems from.

    Checks:
        * the lifetime of the catalog's credential is at most the identity token's (+60 s):
          Lakekeeper and Polaris (``mixed``) take the Keycloak token itself, Unity exchanges it
          for its own token, RustFS exchanges it at its STS for temporary keys (asked for the
          longest lifetime STS allows, 12 h - what a client may ask for).
        Catalogs without authentication: not offered.

    Args:
        request: pytest request (fixtures by name).
    """
    cat = PROFILE.name
    if cat not in ("lakekeeper", "polaris", "unity", "rustfs"):
        pytest.skip(f"catalog {cat} has no API for authentication (open catalog)")
    _require(KC)
    if cat == "rustfs":
        identity = keycloak_token("rustfs-alice")
        r = _rustfs_sts(identity, duration=43200)
        assert r.status_code == 200, f"STS refused: {r.status_code} {r.text[:300]}"
        expires = re.search(r"<Expiration>([^<]+)</Expiration>", r.text)[1]
        stamp = datetime.datetime.fromisoformat(expires).timestamp()
        catalog_life = stamp - time.time()
    else:
        identity = keycloak_token("alice", scope="polaris" if cat == "polaris" else None)
        if cat == "unity":
            _require(f"{UNITY}/api/2.1/unity-catalog/catalogs")
            accepted = (
                _unity_token()
            )  # exchanged through the lab's token shim, as every engine does
        else:
            _require(
                f"{LAKEKEEPER}/health"
                if cat == "lakekeeper"
                else f"{SETTINGS.catalog_uri}/v1/config"
            )
            accepted = identity  # Lakekeeper and Polaris (mixed) take the Keycloak token itself
        catalog_life = _claims(accepted)["exp"] - int(time.time())
    ident = _claims(identity)
    identity_life = ident["exp"] - ident["iat"]
    with _gap("token_lifetime"):
        assert catalog_life <= identity_life + 60, (
            f"catalog token lives {round(catalog_life, -2):.0f} s, the identity token {identity_life} s"
        )
