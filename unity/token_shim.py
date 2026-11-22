"""OAuth2 client-credentials front for the permission instance ``unitycatalog-authz``.

Unity Catalog OSS issues its own tokens only through token exchange (RFC 8693): a client
presents a JWT of an allowed issuer (here Keycloak, realm ``iceberg``) and gets a Unity
token for the user whose external id matches the JWT's client id (``azp``). Iceberg clients
(Spark, Trino, Flink, PyIceberg, DuckDB, pg_lake) only speak client credentials, so this
shim does both steps:

    POST /token  grant_type=client_credentials, client_id, client_secret  (form or Basic)
      lab-admin  -> Unity's own admin token (<conf>/token.txt; secret must match)
      others     -> Keycloak client-credentials token -> Unity token exchange

Wrong credentials are refused by Keycloak (401 passed through); Unity refuses unknown users
itself. Reachable only on the compose network; used on Unity by the permission tests
(test_authz.py, test_rbac.py, test_privileges.py, test_ui.py, test_idp_outage.py).
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

KEYCLOAK = os.environ["KEYCLOAK_TOKEN_URL"]
UNITY = os.environ["UNITY_URL"].rstrip("/")
CONF = os.environ.get("UNITY_CONF", "/conf")
ADMIN_CLIENT = os.environ.get("ADMIN_CLIENT", "lab-admin")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "lab-admin-secret")


def post_form(url: str, fields: dict) -> tuple[int, dict]:
    """POST form fields.

    Args:
        url: URL.
        fields: Form fields.

    Returns:
        ``(status, JSON body)``.
    """
    data = urllib.parse.urlencode(fields).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body or b"{}")
        except ValueError:
            return e.code, {"error": "upstream_error", "error_description": body.decode()[:300]}


class Handler(BaseHTTPRequestHandler):
    """Token endpoint: client credentials -> Keycloak token -> Unity token (token exchange)."""

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        """Log to stderr with a prefix.

        Args:
            fmt: Log format.
            args: Log arguments.
        """
        sys.stderr.write("token-shim: " + (fmt % args) + "\n")

    def reply(self, status: int, body: dict) -> None:
        """Send a JSON answer.

        Args:
            status: HTTP status.
            body: JSON body.
        """
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        """Exchange the client's credentials for a Unity token.

        Returns:
            Nothing (the answer is written to the client).
        """
        n = int(self.headers.get("Content-Length") or 0)
        form = {k: v[0] for k, v in urllib.parse.parse_qs(self.rfile.read(n).decode()).items()}
        auth = self.headers.get("Authorization", "")
        if auth.lower().startswith("basic "):
            cid, _, sec = base64.b64decode(auth[6:]).decode().partition(":")
            form.setdefault("client_id", urllib.parse.unquote(cid))
            form.setdefault("client_secret", urllib.parse.unquote(sec))
        cid, sec = form.get("client_id", ""), form.get("client_secret", "")
        if form.get("grant_type") != "client_credentials" or not cid:
            return self.reply(400, {"error": "unsupported_grant_type"})
        if cid == ADMIN_CLIENT:
            if sec != ADMIN_SECRET:
                return self.reply(401, {"error": "invalid_client"})
            with open(os.path.join(CONF, "token.txt")) as f:
                token = f.read().strip()
            return self.reply(
                200, {"access_token": token, "token_type": "bearer", "expires_in": 3600}
            )
        # scope=unity: a token issued for Unity (audience "unity"); Unity checks it
        # (server.audiences=unity in unity/server-authz.properties).
        status, kc = post_form(
            KEYCLOAK,
            {
                "grant_type": "client_credentials",
                "client_id": cid,
                "client_secret": sec,
                "scope": "unity",
            },
        )
        if status != 200:
            return self.reply(status, kc)
        exchange = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": kc["access_token"],
        }
        status, uc = post_form(f"{UNITY}/api/1.0/unity-control/auth/tokens", exchange)
        if status >= 500:
            # Unity loads the issuer's keys (JWKS) lazily; the very first exchange after a
            # start can fail with a 500 while that happens (observed once) - retry once.
            status, uc = post_form(f"{UNITY}/api/1.0/unity-control/auth/tokens", exchange)
        if status != 200:
            return self.reply(status, uc)
        # Only the standard fields: Unity also returns "scope": null and "refresh_token": null,
        # which the Iceberg Java client and DuckDB reject as non-strings.
        return self.reply(
            200,
            {
                "access_token": uc["access_token"],
                "token_type": "bearer",
                "expires_in": uc.get("expires_in") or 3600,
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
        )


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
