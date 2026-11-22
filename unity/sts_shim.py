# Adapted from the Unity Catalog docs environment (unitycatalog.io/docs/env/uc-aws.tar.gz,
# modules/rustfs/sts_shim.py), unchanged in behaviour.
"""STS front for RustFS: rewrites session policies RustFS can't parse, then re-signs.

Unity Catalog's AssumeRole session policy uses mid-word action wildcards
(``s3:GetO*``, ``s3:*Multipart*``). AWS accepts them; RustFS only parses exact
action names and ``s3:*``, and rejects the whole request. The body is covered by
the caller's SigV4 signature, so a proxy can't edit it in flight: this shim
accepts the request, expands the wildcards against the actions RustFS knows,
drops statements whose condition keys RustFS rejects, and forwards a freshly
signed AssumeRole using its own RustFS credentials.

Lab hardening (tests/selftest/test_lab_adapters.py):

* the caller's SigV4 signature is verified against CALLER_ACCESS_KEY/CALLER_SECRET_KEY (Unity's
  ``aws.accessKey``/``aws.secretKey``) - before, anyone on the compose network got keys signed
  with the shim's own (root) credentials;
* a request without a session policy is refused - it would yield unrestricted keys;
* Deny statements are never dropped (dropping one would widen the rights): their ``kms:``
  condition keys are removed instead, which makes the Deny unconditional, i.e. stricter.
  Allow statements with ``kms:`` conditions are dropped as before (that only narrows).
"""

from __future__ import annotations

import datetime
import fnmatch
import hashlib
import hmac
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = os.environ.get("STS_UPSTREAM", "http://rustfs:9000")
ACCESS_KEY = os.environ["STS_ACCESS_KEY"]
SECRET_KEY = os.environ["STS_SECRET_KEY"]
REGION = os.environ.get("AWS_REGION", "us-east-1")
CALLER_ACCESS_KEY = os.environ.get("CALLER_ACCESS_KEY", ACCESS_KEY)
CALLER_SECRET_KEY = os.environ.get("CALLER_SECRET_KEY", SECRET_KEY)
MAX_SKEW = datetime.timedelta(minutes=15)

# Every object-level action RustFS 1.0 accepts in a policy (probed one by one;
# e.g. GetObjectTorrent and PutObjectVersionAcl are rejected).
RUSTFS_S3_ACTIONS = [
    "s3:AbortMultipartUpload",
    "s3:DeleteObject",
    "s3:DeleteObjectTagging",
    "s3:DeleteObjectVersion",
    "s3:DeleteObjectVersionTagging",
    "s3:GetObject",
    "s3:GetObjectAcl",
    "s3:GetObjectAttributes",
    "s3:GetObjectLegalHold",
    "s3:GetObjectRetention",
    "s3:GetObjectTagging",
    "s3:GetObjectVersion",
    "s3:GetObjectVersionAttributes",
    "s3:GetObjectVersionForReplication",
    "s3:GetObjectVersionTagging",
    "s3:ListBucket",
    "s3:ListBucketMultipartUploads",
    "s3:ListMultipartUploadParts",
    "s3:PutObject",
    "s3:PutObjectAcl",
    "s3:PutObjectLegalHold",
    "s3:PutObjectRetention",
    "s3:PutObjectTagging",
    "s3:PutObjectVersionTagging",
]


def _expand(action: str) -> list[str]:
    """Concrete S3 actions a wildcard action stands for (RustFS refuses wildcards).

    Args:
        action: IAM action, maybe with a wildcard.

    Returns:
        The actions.
    """
    if "*" not in action or action in ("*", "s3:*"):
        return [action]
    return [a for a in RUSTFS_S3_ACTIONS if fnmatch.fnmatchcase(a, action)]


def _condition_keys(statement: dict) -> set[str]:
    """Condition keys used in a statement.

    Args:
        statement: Policy statement.

    Returns:
        The keys.
    """
    return {k for clause in statement.get("Condition", {}).values() for k in clause}


def rewrite_policy(policy: str) -> str:
    """Make Unity's session policy acceptable to RustFS: expand wildcards, drop conditioned Allows, keep Denys unconditional.

    Args:
        policy: Session policy (JSON text).

    Returns:
        The rewritten policy (JSON text).
    """
    doc = json.loads(policy)
    statements = []
    for st in doc.get("Statement", []):
        # RustFS has no KMS and rejects kms:* condition keys. An Allow with such a condition
        # can only narrow, so dropping it grants nothing extra. A Deny must never be dropped
        # (that would widen the rights): remove its kms:* conditions - an unconditional Deny
        # is stricter.
        if any(k.startswith("kms:") for k in _condition_keys(st)):
            if st.get("Effect") != "Deny":
                continue
            cond = {
                op: {k: v for k, v in clause.items() if not k.startswith("kms:")}
                for op, clause in st.get("Condition", {}).items()
            }
            cond = {op: clause for op, clause in cond.items() if clause}
            if cond:
                st["Condition"] = cond
            else:
                st.pop("Condition", None)
        actions = st.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        st["Action"] = sorted({x for a in actions for x in _expand(a)})
        if st["Action"]:
            statements.append(st)
    doc["Statement"] = statements
    return json.dumps(doc)


def _sign(body: bytes, host: str) -> dict[str, str]:
    """SigV4 headers for a form-encoded STS POST to ``/``.

    Args:
        body: Form-encoded request body.
        host: Host header.

    Returns:
        The headers.
    """
    now = datetime.datetime.now(datetime.UTC)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=utf-8",
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed = ";".join(sorted(headers))
    canonical = "\n".join(
        [
            "POST",
            "/",
            "",
            "".join(f"{k}:{headers[k]}\n" for k in sorted(headers)),
            signed,
            payload_hash,
        ]
    )
    scope = f"{day}/{REGION}/sts/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical.encode()).hexdigest(),
        ]
    )
    key = f"AWS4{SECRET_KEY}".encode()
    for part in (day, REGION, "sts", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature}"
    )
    return headers


def _signing_key(secret: str, day: str, region: str, service: str) -> bytes:
    """SigV4 signing key.

    Args:
        secret: Secret key.
        day: Date ``YYYYMMDD``.
        region: Region.
        service: Service name.

    Returns:
        The key.
    """
    key = f"AWS4{secret}".encode()
    for part in (day, region, service, "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    return key


def verify_caller(
    method: str, path: str, headers: dict[str, str], body: bytes, now=None
) -> str | None:
    """Check the caller's SigV4 signature; return None if valid, else the reason.

    ``headers`` with lower-case names. Generic SigV4 (any signed header set), so it accepts the
    AWS SDKs' signatures (Unity: AWS SDK for Java).

    Args:
        method: HTTP method.
        path: Request path.
        headers: Request headers.
        body: Form-encoded request body.
        now: Current time (tests pass a fixed one).

    Returns:
        None, or the reason.
    """
    auth = headers.get("authorization", "")
    m = re.match(
        r"AWS4-HMAC-SHA256 Credential=([^/]+)/(\d{8})/([^/]+)/([^/]+)/aws4_request,\s*"
        r"SignedHeaders=([^,]+),\s*Signature=([0-9a-f]+)$",
        auth,
    )
    if not m:
        return "no SigV4 signature"
    access, day, region, service, signed, signature = m.groups()
    if access != CALLER_ACCESS_KEY:
        return "unknown access key"
    amz_date = headers.get("x-amz-date", "")
    try:
        at = datetime.datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=datetime.UTC)
    except ValueError:
        return "no x-amz-date"
    if abs((now or datetime.datetime.now(datetime.UTC)) - at) > MAX_SKEW or amz_date[:8] != day:
        return "signature expired or not yet valid"
    payload = hashlib.sha256(body).hexdigest()
    if headers.get("x-amz-content-sha256", payload) not in (payload, "UNSIGNED-PAYLOAD"):
        return "payload hash mismatch"
    names = signed.split(";")
    if "host" not in names or any(n not in headers for n in names):
        return "signed headers missing"
    split = urllib.parse.urlsplit(path)
    query = "&".join(
        sorted(
            f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
            for k, v in urllib.parse.parse_qsl(split.query, keep_blank_values=True)
        )
    )
    canonical = "\n".join(
        [
            method,
            urllib.parse.quote(split.path or "/", safe="/-_.~"),
            query,
            "".join(f"{n}:{' '.join(headers[n].split())}\n" for n in names),
            signed,
            headers.get("x-amz-content-sha256")
            if headers.get("x-amz-content-sha256") == "UNSIGNED-PAYLOAD"
            else payload,
        ]
    )
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            f"{day}/{region}/{service}/aws4_request",
            hashlib.sha256(canonical.encode()).hexdigest(),
        ]
    )
    expected = hmac.new(
        _signing_key(CALLER_SECRET_KEY, day, region, service), to_sign.encode(), hashlib.sha256
    ).hexdigest()
    return None if hmac.compare_digest(expected, signature) else "signature does not match"


class Handler(BaseHTTPRequestHandler):
    """STS endpoint: verify the caller, rewrite the policy, forward AssumeRole to RustFS."""

    def do_GET(self):
        # Health probe only; real STS calls are POSTs.
        """Health probe (200)."""
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        """AssumeRole: refuse unsigned/unpolicied calls, forward the rewritten request to RustFS.

        Returns:
            Nothing (the answer is written to the client).
        """
        raw = self.rfile.read(int(self.headers.get("content-length", 0)))
        reason = verify_caller(
            "POST", self.path, {k.lower(): v for k, v in self.headers.items()}, raw
        )
        form = urllib.parse.parse_qs(raw.decode(), keep_blank_values=True)
        if reason is None and "Policy" not in form:
            reason = "no session policy (the keys would be unrestricted)"
        if reason is not None:
            return self._refuse(reason)
        form["Policy"] = [rewrite_policy(form["Policy"][0])]
        body = urllib.parse.urlencode(form, doseq=True).encode()
        host = urllib.parse.urlsplit(UPSTREAM).netloc
        req = urllib.request.Request(
            f"{UPSTREAM}/", data=body, headers=_sign(body, host), method="POST"
        )
        try:
            with urllib.request.urlopen(req) as resp:
                status, ctype, out = (
                    resp.status,
                    resp.headers.get("content-type"),
                    resp.read(),
                )
        except urllib.error.HTTPError as err:
            status, ctype, out = err.code, err.headers.get("content-type"), err.read()
        self.send_response(status)
        self.send_header("content-type", ctype or "text/xml")
        self.send_header("content-length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def _refuse(self, reason: str) -> None:
        """Answer 403 with an STS error document.

        Args:
            reason: Why the request is refused.
        """
        out = (
            "<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code>"
            f"<Message>sts-shim: {reason}</Message></Error></ErrorResponse>"
        ).encode()
        sys.stderr.write(f"sts-shim: refused: {reason}\n")
        self.send_response(403)
        self.send_header("content-type", "text/xml")
        self.send_header("content-length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, fmt, *args):
        """Log POSTs only (health probes are silent).

        Args:
            fmt: Log format.
            args: Log arguments.
        """
        if self.command == "GET":
            return
        sys.stderr.write(f"sts-shim: {fmt % args}\n")


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
