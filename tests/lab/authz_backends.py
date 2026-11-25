"""Permission tests on every catalog: one backend per catalog behind the same interface.

Used by tests/security/test_authz.py (grants of single users), tests/security/test_rbac.py (roles, inherited
grants, a role from the identity provider), tests/security/test_privileges.py (single privileges) and
the permission parts of tests/security/test_security.py, test_ui.py and test_idp_outage.py
(docs/test-architecture.md):

* users ``alice`` (gets grants) and ``bob`` (gets none unless a test grants him one), plus an
  admin for setup and verification;
* test_authz: per module one namespace with tables ``t_select`` (alice: read), ``t_modify``
  (alice: read + write) and ``t_none`` (alice: nothing);
* every engine signs in as alice or bob the way it signs in to that catalog elsewhere;
  :meth:`Backend.rest` gives raw Iceberg REST access as a user (no client library in between).

How each catalog implements this:

* **Lakekeeper** - second instance ``lakekeeper-authz`` (compose profile ``authz``):
  Keycloak (OIDC client credentials) + OpenFGA assignments on tables and namespaces; roles
  with assignee members.
* **Polaris** - the main instance (its RBAC is always on): principals with fixed credentials;
  grants go to catalog roles bound to principal roles (alice: role ``az_alice``; tests create
  roles of their own); Keycloak tokens as a second sign-in (``mixed``).
* **RustFS** - the main instance: IAM users signing with SigV4. Catalog rights are granular
  ``admin:`` table actions (GetTable, CommitTable, ...; ``s3:*`` or ``*`` do not cover them),
  scoped per bucket - not per namespace or table; reading and writing single tables is
  controlled by S3 policies on the tables' data prefixes. Roles are IAM groups; Keycloak
  identities get temporary keys via STS with the policy named in their token.
* **Unity** - second instance ``unitycatalog-authz`` (compose profile ``unity``) with
  authorization on; Keycloak identities exchanged for Unity tokens by ``unity-token-shim``,
  so every engine uses plain OAuth2 client credentials. No roles or groups.
* **Gravitino** - the main instance (authorization on): Keycloak identities (users
  ``service-account-<client>`` in metalake ``lab``); privileges on catalog/schema/table go
  to roles, roles to users.

What a catalog does not offer (roles, namespace grants, single privileges, vended credentials,
remote signing) is reported as "not offered" with a reason the backend checks on every run.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import ClassVar

import pyarrow as pa
import pytest
import requests

from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSE_STS, rest_prefix_path

USERS = ("alice", "bob")
ROWS = 3
TABLES = ("t_select", "t_modify", "t_none")
KC = "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"


@dataclass
class Conn:
    """How a client reaches the catalog as one identity.

    Attributes:
        uri: Iceberg REST base URI (clients append ``/v1``).
        warehouse: Warehouse / catalog name.
        auth: ``oauth2`` (client credentials), ``sigv4`` (access keys) or ``none``.
        credential: ``client_id:client_secret`` (oauth2).
        token_url: Token endpoint (oauth2).
        scope: Token scope (oauth2).
        keys: ``(access key, secret key)`` (sigv4: signing AND data files).
        vended: The catalog hands out storage credentials per table.
        s3_endpoint: Engines must set the S3 endpoint themselves (vended keys without one).
        session_token: Session token of temporary keys (sigv4 with STS keys).
    """

    uri: str
    warehouse: str
    auth: str
    credential: str | None = None
    token_url: str | None = None
    scope: str | None = None
    keys: tuple[str, str] | None = None
    vended: bool = True
    s3_endpoint: bool = False
    session_token: str | None = None

    def java_props(self) -> dict[str, str]:
        """Iceberg Java/PyIceberg REST properties (Spark, Flink, PyIceberg share the keys).

        Returns:
            The REST client properties.
        """
        p = {"uri": self.uri, "warehouse": self.warehouse}
        if self.auth == "oauth2":
            p |= {
                "credential": self.credential,
                "oauth2-server-uri": self.token_url,
                "scope": self.scope or "",
            }
        if self.auth == "sigv4":
            ak, sk = self.keys
            p |= {
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "s3",
                "rest.signing-region": SETTINGS.s3_region,
                "rest.access-key-id": ak,
                "rest.secret-access-key": sk,
                "client.access-key-id": ak,
                "client.secret-access-key": sk,
                "client.region": SETTINGS.s3_region,
            }
        if self.vended:
            p["header.X-Iceberg-Access-Delegation"] = "vended-credentials"
        if self.keys and not self.vended:
            ak, sk = self.keys
            p |= {"s3.access-key-id": ak, "s3.secret-access-key": sk}
        if self.session_token:
            for k in ("rest.session-token", "client.session-token", "s3.session-token"):
                p[k] = self.session_token
        if self.s3_endpoint or not self.vended:
            p |= {
                "s3.endpoint": SETTINGS.s3_endpoint,
                "s3.path-style-access": "true",
                "s3.region": SETTINGS.s3_region,
                "client.region": SETTINGS.s3_region,
            }
        return p


def require(available: bool, msg: str) -> None:
    """Skip - or fail with ``REQUIRE_ENGINES=all`` - when the permission setup is not running.

    Args:
        available: Whether the setup runs.
        msg: Reason shown when it does not.
    """
    if available:
        return
    if os.getenv("REQUIRE_ENGINES", "") == "all":
        pytest.fail(msg)
    pytest.skip(msg)


@dataclass
class Backend:
    """Common interface; subclasses implement the catalog-specific parts."""

    name: str = ""
    #: Trino catalog file per user (trino/catalog/<name>.properties).
    trino: dict[str, str] = field(default_factory=dict)
    #: Feature flags for scenarios that need them (else "no API").
    vended: bool = True
    signing_warehouse: str | None = None

    # -- to implement -------------------------------------------------------------------------
    def ensure(self) -> None:
        """Make users/roles exist (idempotent); skip/fail if the instance is not running."""
        raise NotImplementedError

    def conn(self, user: str) -> Conn:
        """Connection of a lab user.

        Args:
            user: Lab user (``alice`` or ``bob``).
        """
        raise NotImplementedError

    def admin(self) -> Conn:
        """Connection of the admin (setup and verification)."""
        raise NotImplementedError

    def grant(self, ns: str, table: str, write: bool, user: str = "alice", revoke=False):
        """Give (or take) ``user`` read (``write=False``) or read+write on one table.

        Args:
            ns: Namespace of the table.
            table: Table name.
            write: Read + write (True) or read only (False).
            user: Lab user (``alice`` or ``bob``).
            revoke: Take the grant away instead of giving it.
        """
        raise NotImplementedError

    def anonymous_config(self) -> requests.Response:
        """``GET /v1/config`` without any credentials."""
        raise NotImplementedError

    def allow_listing(self, ns: str, user: str = "alice") -> None:
        """Let ``user`` list the tables of ``ns`` (no-op where table grants already allow it).

        Args:
            ns: Namespace of the table.
            user: Lab user (``alice`` or ``bob``).
        """

    #: ``whoami(user) -> str`` (the identity the catalog sees) where the catalog offers it.
    whoami = None

    # -- roles (tests/security/test_rbac.py) ---------------------------------------------------------
    def roles_not_offered(self) -> str | None:
        """Whether the catalog has roles/groups.

        Returns:
            ``None`` if it has, else the checked reason why not.
        """
        return None

    def role_create(self, name: str):
        """Create a role/group.

        Args:
            name: Name of the role / policy.

        Returns:
            The handle the other role methods take.
        """
        raise NotImplementedError

    def role_member(self, role, user: str, remove: bool = False) -> None:
        """Add ``user`` to ``role`` (or remove them).

        Args:
            role: Role handle as returned by ``role_create``.
            user: Lab user (``alice`` or ``bob``).
            remove: Remove instead of add.
        """
        raise NotImplementedError

    def role_grant_table(self, role, ns: str, table: str) -> None:
        """Read on one table for every member of ``role``.

        Args:
            role: Role handle as returned by ``role_create``.
            ns: Namespace of the table.
            table: Table name.
        """
        raise NotImplementedError

    def role_drop(self, role) -> None:
        """Remove the role/group (cleanup).

        Args:
            role: Role handle as returned by ``role_create``.
        """
        raise NotImplementedError

    def idp_role_grant(self, ns: str, table: str) -> None:
        """Read on one table for the role only the identity provider assigns (see
        iceberg_lab.keycloak.IDP_ROLE): the catalog knows the role, nobody is a member.

        Args:
            ns: Namespace of the table.
            table: Table name.
        """
        raise NotImplementedError

    def idp_role_conn(self) -> Conn:
        """Connection of the Keycloak identity whose TOKEN carries that role."""
        raise NotImplementedError

    def idp_role_cleanup(self) -> None:
        """Remove the role and its grants."""

    # -- single privileges (tests/security/test_privileges.py) -----------------------------------------
    def privilege_not_offered(self, priv: str) -> str | None:
        """``None`` if the catalog has a privilege for ``priv``, else the checked reason.

        Args:
            priv: One of :data:`PRIVILEGES`.

        Returns:
            None, or the reason.
        """
        return None

    def privilege_grant(self, priv: str, ns: str, table: str, user: str = "bob"):
        """Give ``user`` exactly the catalog's privilege for ``priv`` (on ``ns`` / ``table``).

        Args:
            priv: One of :data:`PRIVILEGES`.
            ns: Namespace.
            table: Table in it.
            user: Lab user.

        Returns:
            A callable that removes the grant again (cleanup).
        """
        raise NotImplementedError

    def grant_read_as(self, user: str, ns: str, table: str, grantee: str = "alice") -> int:
        """``user`` calls the catalog's grant API to give ``grantee`` read on the table.

        Args:
            user: Lab user who grants.
            ns: Namespace.
            table: Table in it.
            grantee: Lab user who receives the grant.

        Returns:
            The HTTP status of the grant call.
        """
        raise NotImplementedError

    def rest(self, user: str):
        """Raw Iceberg REST access as ``user`` (no client library in between).

        Args:
            user: Lab user.

        Returns:
            ``(requests session with the user's credentials, base URL incl. the prefix)``.
        """
        c = self.conn(user)
        s = requests.Session()
        if c.auth == "oauth2":
            cid, sec = c.credential.split(":", 1)
            data = {"grant_type": "client_credentials", "client_id": cid, "client_secret": sec}
            if c.scope:
                data["scope"] = c.scope
            r = requests.post(c.token_url, data=data, timeout=10)
            r.raise_for_status()
            s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
        elif c.auth == "sigv4":
            s.auth = _SigV4Keys(c.keys)
        cfg = s.get(f"{c.uri}/v1/config", params={"warehouse": c.warehouse}, timeout=10)
        body = cfg.json() if cfg.ok else {}
        # Without /v1/config (no access to the warehouse) the backend knows the prefix.
        prefix = rest_prefix_path(body) if body else "/" + self.rest_prefix(c.warehouse)
        return s, f"{c.uri}/v1{prefix}"

    def rest_prefix(self, warehouse: str) -> str:
        """REST path prefix of a warehouse when the user may not read ``/v1/config``.

        A user without access to the warehouse gets no config (Lakekeeper hides it with 404);
        the raw calls must still go to the real path, so the catalog's own answer (refusal)
        counts - not a 400 for a wrong path.

        Args:
            warehouse: Warehouse name.

        Returns:
            The prefix; by default the warehouse name.
        """
        return warehouse

    def namespace_grant_not_offered(self, ns: str) -> str | None:
        """Whether a read grant on a namespace can cover its tables.

        Args:
            ns: Namespace of the table.

        Returns:
            ``None`` if it can, else the reason why not.
        """
        return None

    def grant_namespace(self, ns: str, user: str) -> None:
        """Read on every table of ``ns`` - existing and future ones - for ``user``.

        Args:
            ns: Namespace of the table.
            user: Lab user (``alice`` or ``bob``).
        """
        raise NotImplementedError

    # -- shared ---------------------------------------------------------------------------------
    def catalog(self, user: str, lab_fixes: bool = True):
        """PyIceberg catalog for ``user`` (``"admin"`` for setup and verification).

        Args:
            user: Lab user, or ``admin``.
            lab_fixes: Apply the lab's SigV4 workaround (RustFS).

        Returns:
            The PyIceberg catalog.
        """
        return pyiceberg_for(self.admin() if user == "admin" else self.conn(user), lab_fixes)

    def setup_namespace(self) -> str:
        """Namespace with the three tables (3 rows each, written by the admin) and grants.

        Returns:
            The new namespace.
        """
        self.ensure()
        ns = f"az_{uuid.uuid4().hex[:6]}"
        cat = self.catalog("admin")
        cat.create_namespace(ns)
        for name in TABLES:
            t = cat.create_table(f"{ns}.{name}", schema=pa.schema([("id", pa.int64())]))
            t.append(pa.table({"id": list(range(1, ROWS + 1))}))
        self.grant(ns, "t_select", write=False)
        self.grant(ns, "t_modify", write=True)
        return ns

    def drop_namespace(self, ns: str) -> None:
        """Purge (else drop) every table of the namespace, then drop it (cleanup).

        Args:
            ns: Namespace of the table.
        """
        cat = self.catalog("admin")
        for t in cat.list_tables(ns):
            try:
                cat.purge_table(t)
            except Exception:  # noqa: BLE001 - cleanup: catalogs without purge (RustFS)
                cat.drop_table(t)
        cat.drop_namespace(ns)

    def rows(self, ns: str, table: str) -> int:
        """Row count of a table as the admin reads it.

        Args:
            ns: Namespace of the table.
            table: Table name.

        Returns:
            The number of rows.
        """
        return self.catalog("admin").load_table(f"{ns}.{table}").scan().to_arrow().num_rows


#: Single privileges every catalog is asked for (tests/security/test_privileges.py).
PRIVILEGES = (
    "describe",
    "list_tables",
    "create_table",
    "alter_table",
    "drop_table",
    "manage_grants",
)


class _SigV4Keys(requests.auth.AuthBase):
    """``requests`` auth: SigV4 with given keys (service s3), incl. ``x-amz-content-sha256``."""

    def __init__(self, keys):
        """Remember the keys.

        Args:
            keys: ``(access key, secret key)``.
        """
        self.keys = keys

    def __call__(self, r):
        """Sign the request.

        Args:
            r: The prepared request.

        Returns:
            The signed request.
        """
        import hashlib

        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        body = r.body or b""
        if isinstance(body, str):
            body = body.encode()
        headers = {
            "x-amz-content-sha256": hashlib.sha256(body).hexdigest(),
            "Content-Type": r.headers.get("Content-Type", "application/json"),
        }
        req = AWSRequest(method=r.method, url=r.url, data=body, headers=headers)
        SigV4Auth(Credentials(*self.keys), "s3", SETTINGS.s3_region).add_auth(req)
        r.headers.update(dict(req.headers))
        return r


def pyiceberg_for(c: Conn, lab_fixes: bool = True):
    """PyIceberg REST catalog for a connection (lab SigV4 workaround only with ``lab_fixes``).

    Args:
        c: Connection of an identity.
        lab_fixes: Apply the lab's SigV4 workaround (RustFS).

    Returns:
        The PyIceberg catalog.
    """
    from pyiceberg.catalog import load_catalog

    from iceberg_lab import engines

    props = c.java_props()
    if c.auth == "sigv4":
        props |= {"s3.region": SETTINGS.s3_region}
    cat = load_catalog(f"az-{uuid.uuid4().hex[:6]}", type="rest", **props)
    if c.auth == "sigv4" and lab_fixes:
        engines._add_content_sha256(cat)
    return cat


# =========================================================================== Lakekeeper


@dataclass
class LakekeeperAuthz(Backend):
    """Lakekeeper permission backend: lakekeeper-authz with Keycloak sign-in and OpenFGA grants."""

    name: str = "lakekeeper"
    base: str = "http://lakekeeper-authz:8181"
    trino: dict = field(default_factory=lambda: {"alice": "lk_authz_alice", "bob": "lk_authz_bob"})
    signing_warehouse: str | None = "authz-signing"
    wh: str = "authz-sts"

    def _token(self, client):
        """Client-credentials token from Keycloak.

        Args:
            client: Keycloak client id.

        Returns:
            The access token.
        """
        r = requests.post(
            KC,
            data={
                "grant_type": "client_credentials",
                "client_id": client,
                "client_secret": f"{client}-secret",
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["access_token"]

    def _h(self, client="lab-admin"):
        """Bearer headers of a Keycloak client (default: lab-admin).

        Args:
            client: Keycloak client id.

        Returns:
            Bearer headers of the identity.
        """
        return {"Authorization": f"Bearer {self._token(client)}"}

    def ensure(self):
        """Bootstrap the instance, create both warehouses and register the users (idempotent)."""
        try:
            ok = requests.get(f"{self.base}/health", timeout=2).ok and bool(self._token("alice"))
        except requests.RequestException:
            ok = False
        require(ok, "lakekeeper-authz not running (docker compose --profile authz up -d)")
        from iceberg_lab.bootstrap import ensure_bucket, warehouse_payload

        s = dataclasses.replace(SETTINGS, lakekeeper_url=self.base)
        ensure_bucket(s)
        r = requests.post(
            f"{self.base}/management/v1/bootstrap",
            json={"accept-terms-of-use": True, "is-operator": True},
            headers=self._h(),
            timeout=10,
        )
        assert r.status_code in (204, 400, 409), r.text  # already bootstrapped is fine
        have = {
            w["name"]: w["id"]
            for w in requests.get(
                f"{self.base}/management/v1/warehouse", headers=self._h(), timeout=10
            ).json()["warehouses"]
        }
        for name, sts in ((self.wh, True), (self.signing_warehouse, False)):
            if name not in have:
                r = requests.post(
                    f"{self.base}/management/v1/warehouse",
                    json=warehouse_payload(s, name, sts),
                    headers=self._h(),
                    timeout=10,
                )
                r.raise_for_status()
                have[name] = r.json()["id"]
        self.warehouse_ids = have
        self.user_ids = {}
        # The admin too: Lakekeeper creates a user record only on this call (needed for whoami).
        requests.post(f"{self.base}/management/v1/user", json={}, headers=self._h(), timeout=10)
        for u in USERS:
            requests.post(
                f"{self.base}/management/v1/user", json={}, headers=self._h(u), timeout=10
            )
            self.user_ids[u] = requests.get(
                f"{self.base}/management/v1/whoami", headers=self._h(u), timeout=10
            ).json()["id"]

    def ensure_ui_user(self) -> str:
        """The UI's login user ``lab-ui``: in Keycloak, registered here, project admin (idempotent).

        Project admin, so the UI shows the identity and role management - the feature the
        permission tests exercise through the API.

        Returns:
            The user's Lakekeeper id.
        """
        from iceberg_lab import keycloak as kc

        kc.ensure_ui_login()
        h = {"Authorization": f"Bearer {kc.ui_user_token()}"}
        requests.post(f"{self.base}/management/v1/user", json={}, headers=h, timeout=10)
        uid = requests.get(f"{self.base}/management/v1/whoami", headers=h, timeout=10).json()["id"]
        r = requests.post(
            f"{self.base}/management/v1/permissions/project/assignments",
            json={"writes": [{"type": "project_admin", "user": uid}]},
            headers=self._h(),
            timeout=10,
        )
        # Writing an assignment that exists already is refused - that is the idempotent case.
        assert r.ok or "exist" in r.text.lower(), (
            f"project_admin for {uid}: {r.status_code} {r.text}"
        )
        return uid

    def rest_prefix(self, warehouse: str) -> str:
        """Lakekeeper's REST prefix is the warehouse id (known from :meth:`ensure`).

        Args:
            warehouse: Warehouse name.

        Returns:
            The warehouse id.
        """
        return self.warehouse_ids[warehouse]

    def _conn(self, client, warehouse=None):
        """Connection of a Keycloak client to a warehouse.

        Args:
            client: Keycloak client id.
            warehouse: Warehouse (default: the backend's STS warehouse).

        Returns:
            The connection.
        """
        return Conn(
            uri=f"{self.base}/catalog",
            warehouse=warehouse or self.wh,
            auth="oauth2",
            credential=f"{client}:{client}-secret",
            token_url=KC,
            scope="openid",
        )

    def conn(self, user, warehouse=None):
        """Connection of a lab user.

        Args:
            user: Lab user (``alice`` or ``bob``).
            warehouse: Warehouse (default: the backend's STS warehouse).

        Returns:
            The connection.
        """
        return self._conn(user, warehouse)

    def admin(self, warehouse=None):
        """Connection of ``lab-admin`` (the instance admin).

        Args:
            warehouse: Warehouse (default: the backend's STS warehouse).

        Returns:
            The connection.
        """
        return self._conn("lab-admin", warehouse)

    def bearer(self, user):
        """Bearer headers of a lab user (raw REST calls).

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            Bearer headers of the identity.
        """
        return self._h(user)

    def whoami(self, user):
        """Lakekeeper's who-am-I for a user.

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            ``<user id> <name>``.
        """
        r = requests.get(f"{self.base}/management/v1/whoami", headers=self._h(user), timeout=10)
        r.raise_for_status()
        me = r.json()
        return (
            f"{me['id']} {me['name']}"  # name: Keycloak service account, e.g. service-account-alice
        )

    def setup_namespace(self):
        """As everywhere, plus the same namespace/tables/grants in the signing warehouse.

        Returns:
            The new namespace.
        """
        ns = super().setup_namespace()
        cat = pyiceberg_for(self.admin(self.signing_warehouse))
        cat.create_namespace(ns)
        t = cat.create_table(f"{ns}.t_select", schema=pa.schema([("id", pa.int64())]))
        t.append(pa.table({"id": list(range(1, ROWS + 1))}))
        self.grant(ns, "t_select", write=False, warehouse=self.signing_warehouse)
        return ns

    def drop_namespace(self, ns):
        """Drop the namespace in both warehouses (cleanup).

        Args:
            ns: Namespace of the table.
        """
        super().drop_namespace(ns)
        cat = pyiceberg_for(self.admin(self.signing_warehouse))
        for t in cat.list_tables(ns):
            cat.purge_table(t)
        cat.drop_namespace(ns)

    def grant(self, ns, table, write, user="alice", revoke=False, warehouse=None):
        """Write (or delete) OpenFGA assignments select/modify of the user on the table.

        Args:
            ns: Namespace of the table.
            table: Table name.
            write: Read + write (True) or read only (False).
            user: Lab user (``alice`` or ``bob``).
            revoke: Take the grant away instead of giving it.
            warehouse: Warehouse (default: the backend's STS warehouse).
        """
        wh = warehouse or self.wh
        t = pyiceberg_for(self.admin(wh)).load_table(f"{ns}.{table}")
        types = ["select", "modify"] if write else ["select"]
        r = requests.post(
            f"{self.base}/management/v1/permissions/warehouse/{self.warehouse_ids[wh]}"
            f"/table/{t.metadata.table_uuid}/assignments",
            json={
                ("deletes" if revoke else "writes"): [
                    {"type": x, "user": self.user_ids[user]} for x in types
                ]
            },
            headers=self._h(),
            timeout=10,
        )
        r.raise_for_status()

    def anonymous_config(self):
        """``GET /v1/config`` without credentials.

        Returns:
            The response.
        """
        return requests.get(
            f"{self.base}/catalog/v1/config", params={"warehouse": self.wh}, timeout=10
        )

    def _post(self, path, body):
        """POST to the management API as lab-admin; asserts success.

        Args:
            path: Path below ``/management/v1/`` (Polaris: ``/api/management/v1/``).
            body: JSON request body.

        Returns:
            The response.
        """
        r = requests.post(
            f"{self.base}/management/v1/{path}", json=body, headers=self._h(), timeout=10
        )
        assert r.ok, f"POST {path}: {r.status_code} {r.text[:300]}"
        return r

    def role_create(self, name):
        """Create a Lakekeeper role.

        Args:
            name: Name of the role / policy.

        Returns:
            The role id.
        """
        return self._post("role", {"name": name}).json()["id"]

    def role_member(self, role, user, remove=False):
        """Add or remove the user as assignee of the role.

        Args:
            role: Role handle as returned by ``role_create``.
            user: Lab user (``alice`` or ``bob``).
            remove: Remove instead of add.
        """
        key = "deletes" if remove else "writes"
        self._post(
            f"permissions/role/{role}/assignments",
            {key: [{"type": "assignee", "user": self.user_ids[user]}]},
        )

    def role_grant_table(self, role, ns, table):
        """Assign select on the table to the role.

        Args:
            role: Role handle as returned by ``role_create``.
            ns: Namespace of the table.
            table: Table name.
        """
        t = pyiceberg_for(self.admin()).load_table(f"{ns}.{table}")
        self._post(
            f"permissions/warehouse/{self.warehouse_ids[self.wh]}/table/{t.metadata.table_uuid}"
            "/assignments",
            {"writes": [{"type": "select", "role": role}]},
        )

    def role_drop(self, role):
        """Delete the role.

        Args:
            role: Role handle as returned by ``role_create``.
        """
        requests.delete(f"{self.base}/management/v1/role/{role}", headers=self._h(), timeout=10)

    def _idp_role_id(self):
        """The role ``oidc~<IDP_ROLE>`` (created if missing).

        Returns:
            The role id.
        """
        from iceberg_lab.keycloak import IDP_ROLE

        body = {"name": IDP_ROLE, "provider-id": "oidc", "source-id": IDP_ROLE}
        r = requests.post(
            f"{self.base}/management/v1/role", json=body, headers=self._h(), timeout=10
        )
        if r.ok:
            return r.json()["id"]
        found = requests.post(
            f"{self.base}/management/v1/search/role",
            json={"search": IDP_ROLE},
            headers=self._h(),
            timeout=10,
        ).json()
        ids = [x["id"] for x in found.get("roles", []) if x.get("source-id") == IDP_ROLE]
        assert ids, f"create role {IDP_ROLE}: {r.status_code} {r.text[:200]}; search: {found}"
        return ids[0]

    def idp_role_grant(self, ns, table):
        """Grant the token-only role select on the table and register the IdP client's user.

        Args:
            ns: Namespace of the table.
            table: Table name.
        """
        from iceberg_lab.keycloak import IDP_CLIENT

        self.idp_role = self._idp_role_id()
        self.role_grant_table(self.idp_role, ns, table)
        # Lakekeeper knows a user only after its first registration call.
        requests.post(
            f"{self.base}/management/v1/user", json={}, headers=self._h(IDP_CLIENT), timeout=10
        )

    def idp_role_conn(self):
        """Connection of the Keycloak client whose token carries the role.

        Returns:
            The connection.
        """
        from iceberg_lab.keycloak import IDP_CLIENT

        return self._conn(IDP_CLIENT)

    def idp_role_cleanup(self):
        """Delete the token-only role."""
        if getattr(self, "idp_role", None):
            self.role_drop(self.idp_role)

    #: privilege -> (object, assignment type)
    PRIV: ClassVar[dict] = {
        "describe": ("table", "describe"),
        "list_tables": ("namespace", "describe"),
        "create_table": ("namespace", "create"),
        "alter_table": ("table", "write"),
        "drop_table": ("table", "modify"),
        "manage_grants": ("table", "manage_grants"),
    }

    def _assignments_path(self, kind, ns, table):
        """Management path of the assignments of a namespace or table.

        Args:
            kind: ``namespace`` or ``table``.
            ns: Namespace.
            table: Table name.

        Returns:
            The path below ``/management/v1/``.
        """
        cat = pyiceberg_for(self.admin())
        if kind == "namespace":
            nsid = cat.load_namespace_properties(ns)["namespace_id"]
            return f"permissions/namespace/{nsid}/assignments"
        tid = cat.load_table(f"{ns}.{table}").metadata.table_uuid
        return f"permissions/warehouse/{self.warehouse_ids[self.wh]}/table/{tid}/assignments"

    def privilege_grant(self, priv, ns, table, user="bob"):
        """OpenFGA assignment of the user on the namespace or table (see ``PRIV``).

        Args:
            priv: Privilege key.
            ns: Namespace.
            table: Table name.
            user: Lab user.

        Returns:
            Cleanup callable (deletes the assignment).
        """
        kind, typ = self.PRIV[priv]
        path = self._assignments_path(kind, ns, table)
        entry = {"type": typ, "user": self.user_ids[user]}
        self._post(path, {"writes": [entry]})

        def cleanup():
            """Delete the assignment (ignored if the object is gone, e.g. after the drop)."""
            with contextlib.suppress(AssertionError):
                self._post(path, {"deletes": [entry]})

        return cleanup

    def grant_read_as(self, user, ns, table, grantee="alice"):
        """POST the table's assignments with the user's own token.

        Args:
            user: Lab user who grants.
            ns: Namespace.
            table: Table name.
            grantee: Lab user who receives the grant.

        Returns:
            The HTTP status.
        """
        r = requests.post(
            f"{self.base}/management/v1/{self._assignments_path('table', ns, table)}",
            json={"writes": [{"type": "select", "user": self.user_ids[grantee]}]},
            headers=self._h(user),
            timeout=10,
        )
        return r.status_code

    def grant_namespace(self, ns, user):
        """Assign select on the namespace to the user (inherited by its tables).

        Args:
            ns: Namespace of the table.
            user: Lab user (``alice`` or ``bob``).
        """
        nsid = pyiceberg_for(self.admin()).load_namespace_properties(ns)["namespace_id"]
        self._post(
            f"permissions/namespace/{nsid}/assignments",
            {"writes": [{"type": "select", "user": self.user_ids[user]}]},
        )


# =========================================================================== Polaris


@dataclass
class PolarisAuthz(Backend):
    """Polaris permission backend: principals with fixed credentials, principal and catalog roles."""

    name: str = "polaris"
    base: str = "http://polaris:8181"
    trino: dict = field(
        default_factory=lambda: {"alice": "pol_authz_alice", "bob": "pol_authz_bob"}
    )

    @property
    def token_url(self):
        """Polaris' own OAuth2 token endpoint.

        Returns:
            The URL.
        """
        return f"{self.base}/api/catalog/v1/oauth/tokens"

    def _h(self):
        """Bearer headers of the root principal.

        Returns:
            Bearer headers of the identity.
        """
        cid, sec = PROFILE.oauth_credential.split(":", 1)
        r = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": cid,
                "client_secret": sec,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            timeout=10,
        )
        r.raise_for_status()
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def _mgmt(self, method, path, body=None, ok=(200, 201, 204)):
        """Management API call as root; asserts an expected status.

        Args:
            method: HTTP method.
            path: Path below ``/management/v1/`` (Polaris: ``/api/management/v1/``).
            body: JSON request body.
            ok: Status codes that count as success.

        Returns:
            The response.
        """
        r = requests.request(
            method,
            f"{self.base}/api/management/v1/{path}",
            json=body,
            headers=self._h(),
            timeout=10,
        )
        assert r.status_code in ok, f"{method} {path}: {r.status_code} {r.text[:300]}"
        return r

    def ensure(self):
        """Create the principals, alice's roles and their binding (idempotent)."""
        try:
            ok = requests.get(f"{self.base}/api/catalog/v1/config", timeout=2).status_code < 500
        except requests.RequestException:
            ok = False
        require(ok, "polaris not running")
        cat = WAREHOUSE_STS
        for u in USERS:
            self._mgmt("POST", "principals", {"principal": {"name": f"az_{u}"}}, ok=(201, 409))
            # Fixed credentials, so Trino's static catalog files can use them.
            self._mgmt(
                "POST",
                f"principals/az_{u}/reset",
                {"clientId": f"az-{u}", "clientSecret": f"{u}-secret"},
            )
        self._mgmt(
            "POST", "principal-roles", {"principalRole": {"name": "az_alice"}}, ok=(201, 409)
        )
        self._mgmt(
            "PUT", "principals/az_alice/principal-roles", {"principalRole": {"name": "az_alice"}}
        )
        # alice signed in with Keycloak (Polaris "mixed" authentication, principal = the token's
        # client id): the same role as her Polaris-issued identity az_alice.
        self._mgmt("POST", "principals", {"principal": {"name": "alice"}}, ok=(201, 409))
        self._mgmt(
            "PUT", "principals/alice/principal-roles", {"principalRole": {"name": "az_alice"}}
        )
        self._mgmt(
            "POST",
            f"catalogs/{cat}/catalog-roles",
            {"catalogRole": {"name": "az_alice"}},
            ok=(201, 409),
        )
        self._mgmt(
            "PUT",
            f"principal-roles/az_alice/catalog-roles/{cat}",
            {"catalogRole": {"name": "az_alice"}},
        )

    def conn(self, user):
        """Connection of a lab user's principal (``az-<user>``).

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            The connection.
        """
        return Conn(
            uri=f"{self.base}/api/catalog",
            warehouse=WAREHOUSE_STS,
            auth="oauth2",
            credential=f"az-{user}:{user}-secret",
            token_url=self.token_url,
            scope="PRINCIPAL_ROLE:ALL",
        )

    def admin(self):
        """Connection of the root principal.

        Returns:
            The connection.
        """
        return Conn(
            uri=f"{self.base}/api/catalog",
            warehouse=WAREHOUSE_STS,
            auth="oauth2",
            credential=PROFILE.oauth_credential,
            token_url=self.token_url,
            scope="PRINCIPAL_ROLE:ALL",
        )

    def grant(self, ns, table, write, user="alice", revoke=False):
        """Grant (or revoke) TABLE_READ_DATA / TABLE_WRITE_DATA to alice's catalog role.

        Args:
            ns: Namespace of the table.
            table: Table name.
            write: Read + write (True) or read only (False).
            user: Lab user (``alice`` or ``bob``).
            revoke: Take the grant away instead of giving it.
        """
        assert user == "alice", "only alice has a role"
        if not revoke:
            # Seeing the namespace (engines like Flink and pg_lake load it before the table).
            self._mgmt(
                "PUT",
                f"catalogs/{WAREHOUSE_STS}/catalog-roles/az_alice/grants",
                {
                    "grant": {
                        "type": "namespace",
                        "namespace": [ns],
                        "privilege": "NAMESPACE_READ_PROPERTIES",
                    }
                },
            )
        privileges = ["TABLE_READ_DATA", "TABLE_WRITE_DATA"] if write else ["TABLE_READ_DATA"]
        for p in privileges:
            self._mgmt(
                "POST" if revoke else "PUT",
                f"catalogs/{WAREHOUSE_STS}/catalog-roles/az_alice/grants",
                {"grant": {"type": "table", "namespace": [ns], "tableName": table, "privilege": p}},
            )

    def anonymous_config(self):
        """``GET /v1/config`` without credentials.

        Returns:
            The response.
        """
        return requests.get(
            f"{self.base}/api/catalog/v1/config", params={"warehouse": WAREHOUSE_STS}, timeout=10
        )

    def role_create(self, name):
        """Principal role + catalog role of the same name, bound together.

        Args:
            name: Name of the role / policy.

        Returns:
            The role name.
        """
        cat = WAREHOUSE_STS
        self._mgmt("POST", "principal-roles", {"principalRole": {"name": name}}, ok=(201,))
        self._mgmt(
            "POST", f"catalogs/{cat}/catalog-roles", {"catalogRole": {"name": name}}, ok=(201,)
        )
        self._mgmt(
            "PUT", f"principal-roles/{name}/catalog-roles/{cat}", {"catalogRole": {"name": name}}
        )
        return name

    def role_member(self, role, user, remove=False):
        """Assign the principal role to the user's principal (or take it away).

        Args:
            role: Role handle as returned by ``role_create``.
            user: Lab user (``alice`` or ``bob``).
            remove: Remove instead of add.
        """
        if remove:
            self._mgmt("DELETE", f"principals/az_{user}/principal-roles/{role}")
        else:
            self._mgmt(
                "PUT", f"principals/az_{user}/principal-roles", {"principalRole": {"name": role}}
            )

    def _role_grant(self, role, grant):
        """Add a grant to the catalog role.

        Args:
            role: Role handle as returned by ``role_create``.
            grant: Polaris grant object (type, namespace, privilege, ...).
        """
        self._mgmt("PUT", f"catalogs/{WAREHOUSE_STS}/catalog-roles/{role}/grants", {"grant": grant})

    def role_grant_table(self, role, ns, table):
        """Grant the role NAMESPACE_READ_PROPERTIES on the namespace and TABLE_READ_DATA on the table.

        Args:
            role: Role handle as returned by ``role_create``.
            ns: Namespace of the table.
            table: Table name.
        """
        self._role_grant(
            role,
            {"type": "namespace", "namespace": [ns], "privilege": "NAMESPACE_READ_PROPERTIES"},
        )
        self._role_grant(
            role,
            {
                "type": "table",
                "namespace": [ns],
                "tableName": table,
                "privilege": "TABLE_READ_DATA",
            },
        )

    def role_drop(self, role):
        """Delete the principal role and the catalog role.

        Args:
            role: Role handle as returned by ``role_create``.
        """
        self._mgmt("DELETE", f"principal-roles/{role}", ok=(204, 404))
        self._mgmt("DELETE", f"catalogs/{WAREHOUSE_STS}/catalog-roles/{role}", ok=(204, 404))

    def idp_role_grant(self, ns, table):
        """Role ``lab_readers`` (scope polaris-role-lab_readers -> PRINCIPAL_ROLE:lab_readers);
        the Keycloak client's principal exists but is assigned NO role in Polaris.

        Args:
            ns: Namespace of the table.
            table: Table name.
        """
        from iceberg_lab.keycloak import IDP_CLIENT

        role = "lab_readers"
        self._mgmt("POST", "principals", {"principal": {"name": IDP_CLIENT}}, ok=(201, 409))
        self._mgmt("POST", "principal-roles", {"principalRole": {"name": role}}, ok=(201, 409))
        self._mgmt(
            "POST",
            f"catalogs/{WAREHOUSE_STS}/catalog-roles",
            {"catalogRole": {"name": role}},
            ok=(201, 409),
        )
        self._mgmt(
            "PUT",
            f"principal-roles/{role}/catalog-roles/{WAREHOUSE_STS}",
            {"catalogRole": {"name": role}},
        )
        self.role_grant_table(role, ns, table)
        self.idp_role = role

    def idp_role_conn(self):
        """Keycloak connection of the IdP client, with the role scope.

        Returns:
            The connection.
        """
        from iceberg_lab.keycloak import IDP_CLIENT, POLARIS_IDP_SCOPE

        return Conn(
            uri=f"{self.base}/api/catalog",
            warehouse=WAREHOUSE_STS,
            auth="oauth2",
            credential=f"{IDP_CLIENT}:{IDP_CLIENT}-secret",
            token_url=KC,
            scope=f"polaris {POLARIS_IDP_SCOPE}",
        )

    def idp_role_cleanup(self):
        """Delete the token-only role."""
        if getattr(self, "idp_role", None):
            self.role_drop(self.idp_role)

    def grant_namespace(self, ns, user):
        """Polaris grants only to catalog roles: a role holding just ``user``.

        Args:
            ns: Namespace of the table.
            user: Lab user (``alice`` or ``bob``).
        """
        role = self.role_create(f"rbac_ns_{user}_{uuid.uuid4().hex[:6]}")
        self.role_member(role, user)
        for privilege in ("NAMESPACE_READ_PROPERTIES", "TABLE_READ_DATA"):
            self._role_grant(role, {"type": "namespace", "namespace": [ns], "privilege": privilege})
        self.namespace_roles = [*getattr(self, "namespace_roles", []), role]

    #: privilege -> (securable type, Polaris privilege)
    PRIV: ClassVar[dict] = {
        "describe": ("table", "TABLE_READ_PROPERTIES"),
        "list_tables": ("namespace", "TABLE_LIST"),
        "create_table": ("namespace", "TABLE_CREATE"),
        "alter_table": ("table", "TABLE_WRITE_PROPERTIES"),
        "drop_table": ("table", "TABLE_DROP"),
        "manage_grants": ("catalog", "CATALOG_MANAGE_ACCESS"),
    }

    def privilege_grant(self, priv, ns, table, user="bob"):
        """A fresh role of the user holding only that privilege (Polaris grants to roles).

        Args:
            priv: Privilege key.
            ns: Namespace.
            table: Table name.
            user: Lab user.

        Returns:
            Cleanup callable (drops the role).
        """
        kind, privilege = self.PRIV[priv]
        role = self.role_create(f"priv_{priv}_{uuid.uuid4().hex[:6]}")
        self.role_member(role, user)
        grant = {"type": kind, "privilege": privilege}
        if kind != "catalog":
            grant["namespace"] = [ns]
        if kind == "table":
            grant["tableName"] = table
        self._role_grant(role, grant)
        return lambda: self.role_drop(role)

    def grant_read_as(self, user, ns, table, grantee="alice"):
        """PUT TABLE_READ_DATA into the grantee's catalog role with the user's own token.

        Args:
            user: Lab user who grants.
            ns: Namespace.
            table: Table name.
            grantee: Lab user who receives the grant (only alice has a role).

        Returns:
            The HTTP status.
        """
        assert grantee == "alice", "only alice has a role"
        r = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": f"az-{user}",
                "client_secret": f"{user}-secret",
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            timeout=10,
        )
        r.raise_for_status()
        grant = {"type": "table", "namespace": [ns], "tableName": table}
        r = requests.put(
            f"{self.base}/api/management/v1/catalogs/{WAREHOUSE_STS}/catalog-roles/az_alice/grants",
            json={"grant": grant | {"privilege": "TABLE_READ_DATA"}},
            headers={"Authorization": f"Bearer {r.json()['access_token']}"},
            timeout=10,
        )
        return r.status_code

    def allow_listing(self, ns, user="alice"):
        """Polaris needs TABLE_LIST on the namespace to list its tables.

        Args:
            ns: Namespace of the table.
            user: Lab user (``alice`` or ``bob``).
        """
        assert user == "alice", "only alice has a role"
        self._mgmt(
            "PUT",
            f"catalogs/{WAREHOUSE_STS}/catalog-roles/az_alice/grants",
            {"grant": {"type": "namespace", "namespace": [ns], "privilege": "TABLE_LIST"}},
        )


# =========================================================================== RustFS


@dataclass
class RustFSAuthz(Backend):
    """RustFS permission backend: IAM users with SigV4 keys, policies on catalog actions and data prefixes."""

    name: str = "rustfs"
    trino: dict = field(
        default_factory=lambda: {"alice": "rfs_authz_alice", "bob": "rfs_authz_bob"}
    )
    vended: bool = False

    #: Fixed keys (Trino's static catalog files use them).
    KEYS: ClassVar[dict] = {
        "alice": ("az-alice", "az-alice-secret"),
        "bob": ("az-bob", "az-bob-secret"),
    }

    def _admin_client(self):
        """MinIO-compatible admin client with the root keys.

        Returns:
            The admin client.
        """
        from minio import MinioAdmin
        from minio.credentials import StaticProvider

        host = SETTINGS.s3_endpoint.split("//", 1)[1]
        return MinioAdmin(
            endpoint=host,
            credentials=StaticProvider(SETTINGS.s3_access_key, SETTINGS.s3_secret_key),
            secure=False,
        )

    def ensure(self):
        """Create the IAM users and apply alice's policy (idempotent)."""
        adm = self._admin_client()
        for u in USERS:
            adm.user_add(*self.KEYS[u])  # idempotent: re-adding sets the same secret
        self.grants: dict[str, set[str]] = {"read": set(), "write": set()}
        self._apply()

    def _apply(self):
        """alice's policy: the catalog actions she needs + S3 rights on the granted tables' data.

        RustFS has granular catalog actions (docs.rustfs.com, S3 Tables; more in the binary). They
        can be scoped per bucket (arn:aws:s3:::<bucket>/*), not per namespace or table - checked.
        Never ``admin:*``: it includes IAM administration (a user could make herself admin).
        """
        adm = self._admin_client()
        bucket = PROFILE.location.removeprefix("s3://").split("/")[0]
        catalog = [f"arn:aws:s3:::{bucket}/*"]
        statements = [
            {
                "Effect": "Allow",
                "Action": ["admin:GetTableCatalog", "admin:GetTableBucket"],
                "Resource": ["arn:aws:s3:::*"],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "admin:GetTableNamespace",
                    "admin:GetTable",
                    "admin:GetTableMetadata",
                    "admin:GetTableMetadataLocation",
                ],
                "Resource": catalog,
            },
        ]
        if self.grants["write"]:  # commits; the S3 rights below limit them to the writable tables
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["admin:CommitTable", "admin:SetTableMetadata"],
                    "Resource": catalog,
                }
            )
        reads = sorted(self.grants["read"] | self.grants["write"])
        if reads:
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{p}/*" for p in reads],
                }
            )
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                }
            )
        if self.grants["write"]:
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:PutObject", "s3:DeleteObject"],
                    "Resource": [
                        f"arn:aws:s3:::{bucket}/{p}/*" for p in sorted(self.grants["write"])
                    ],
                }
            )
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"Version": "2012-10-17", "Statement": statements}, f)
        adm.policy_add("az-alice", f.name)
        adm.policy_set("az-alice", user=self.KEYS["alice"][0])

    def conn(self, user):
        """SigV4 connection with the user's keys.

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            The connection.
        """
        return Conn(
            uri=SETTINGS.catalog_uri,
            warehouse=WAREHOUSE_STS,
            auth="sigv4",
            keys=self.KEYS[user],
            vended=False,
        )

    def admin(self):
        """SigV4 connection with the root keys.

        Returns:
            The connection.
        """
        return Conn(
            uri=SETTINGS.catalog_uri,
            warehouse=WAREHOUSE_STS,
            auth="sigv4",
            keys=(SETTINGS.s3_access_key, SETTINGS.s3_secret_key),
            vended=False,
        )

    def grant(self, ns, table, write, user="alice", revoke=False):
        """Add (or remove) the table's data and metadata prefixes to alice's policy.

        Args:
            ns: Namespace of the table.
            table: Table name.
            write: Read + write (True) or read only (False).
            user: Lab user (``alice`` or ``bob``).
            revoke: Take the grant away instead of giving it.
        """
        assert user == "alice", "only alice has a policy"
        t = pyiceberg_for(self.admin()).load_table(f"{ns}.{table}")
        # Data files live under the table location, the metadata in the catalog's own area
        # (.rustfs-table/warehouses/<wh>/namespaces/<ns>/tables/<t>/metadata) - both needed.
        data = t.location().removeprefix("s3://").split("/", 1)[1]
        meta = t.metadata_location.removeprefix("s3://").split("/", 1)[1].rsplit("/", 2)[0]
        kind = "write" if write else "read"
        for prefix in (data, meta):
            (self.grants[kind].discard if revoke else self.grants[kind].add)(prefix)
        self._apply()

    def anonymous_config(self):
        """``GET /v1/config`` without a signature.

        Returns:
            The response.
        """
        return requests.get(
            f"{SETTINGS.catalog_uri}/v1/config", params={"warehouse": WAREHOUSE_STS}, timeout=10
        )

    # Roles = IAM groups with a group policy (same statements as alice's user policy).
    def _group_members(self, group, members, remove=False):
        """Raw admin call: minio's group_add omits ``groupStatus``, which RustFS requires.

        Args:
            group: IAM group name.
            members: Access keys of the users.
            remove: Remove instead of add.
        """
        from minio.minioadmin import _COMMAND

        body = {"group": group, "members": members, "isRemove": remove, "groupStatus": "enabled"}
        self._admin_client()._url_open(
            method="PUT",
            command=_COMMAND.ADD_UPDATE_REMOVE_GROUP,
            body=json.dumps(body).encode(),
        )

    def _table_prefixes(self, ns, table):
        """Bucket-relative prefixes of a table's data and metadata.

        Args:
            ns: Namespace of the table.
            table: Table name.

        Returns:
            ``(data prefix, metadata prefix)``.
        """
        t = pyiceberg_for(self.admin()).load_table(f"{ns}.{table}")
        data = t.location().removeprefix("s3://").split("/", 1)[1]
        meta = t.metadata_location.removeprefix("s3://").split("/", 1)[1].rsplit("/", 2)[0]
        return data, meta

    def role_create(self, name):
        """Create an empty IAM group.

        Args:
            name: Name of the role / policy.

        Returns:
            The group name.
        """
        self._group_members(name, [])
        self.group_tables = getattr(self, "group_tables", {}) | {name: set()}
        return name

    def role_member(self, role, user, remove=False):
        """Add the user's access key to the group (or remove it).

        Args:
            role: Role handle as returned by ``role_create``.
            user: Lab user (``alice`` or ``bob``).
            remove: Remove instead of add.
        """
        self._group_members(role, [self.KEYS[user][0]], remove)

    def _read_policy(self, name, prefixes, group=None):
        """IAM policy ``name``: the catalog reads alice's user policy needs, plus GetObject on
        ``prefixes`` (table data and metadata); attached to ``group`` if given.

        Args:
            name: Name of the role / policy.
            prefixes: Bucket-relative key prefixes the policy may read.
            group: IAM group name.
        """
        bucket = PROFILE.location.removeprefix("s3://").split("/")[0]
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["admin:GetTableCatalog", "admin:GetTableBucket"],
                    "Resource": ["arn:aws:s3:::*"],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "admin:GetTableNamespace",
                        "admin:GetTable",
                        "admin:GetTableMetadata",
                        "admin:GetTableMetadataLocation",
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{p}/*" for p in sorted(prefixes)],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                },
            ],
        }
        adm = self._admin_client()
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(policy, f)
        adm.policy_add(name, f.name)
        if group:
            adm.policy_set(name, group=group)

    def role_grant_table(self, role, ns, table):
        """Add the table's prefixes to the group's read policy.

        Args:
            role: Role handle as returned by ``role_create``.
            ns: Namespace of the table.
            table: Table name.
        """
        self.group_tables[role] |= set(self._table_prefixes(ns, table))
        self._read_policy(role, self.group_tables[role], group=role)

    def idp_role_grant(self, ns, table):
        """The IAM policy the identity provider names in the token (claim rustfs_policy).

        Args:
            ns: Namespace of the table.
            table: Table name.
        """
        from iceberg_lab.keycloak import RUSTFS_IDP_POLICY

        self._read_policy(RUSTFS_IDP_POLICY, self._table_prefixes(ns, table))

    def idp_role_conn(self):
        """Temporary keys from STS AssumeRoleWithWebIdentity with the Keycloak token.

        Returns:
            SigV4 connection with the temporary keys and session token.
        """
        from iceberg_lab.keycloak import RUSTFS_IDP_CLIENT

        return self.web_identity_conn(RUSTFS_IDP_CLIENT)

    def web_identity_conn(self, client: str, kc: str | None = None):
        """Log in through the identity provider: Keycloak token of ``client``, exchanged at
        RustFS' STS (AssumeRoleWithWebIdentity) for temporary keys.

        Args:
            client: Keycloak client (its token names the IAM policy, claim ``rustfs_policy``).
            kc: Keycloak base URL (default the lab's; a proxy in the outage tests).

        Returns:
            SigV4 connection with the temporary keys and session token.
        """
        import re as _re

        from iceberg_lab.keycloak import KC, token

        r = requests.post(
            SETTINGS.s3_endpoint + "/",
            data={
                "Action": "AssumeRoleWithWebIdentity",
                "Version": "2011-06-15",
                "WebIdentityToken": token(client, base=kc or KC),
                "DurationSeconds": "900",
            },
            timeout=10,
        )
        assert r.status_code == 200, f"STS refused: {r.status_code} {r.text[:300]}"
        get = lambda tag: _re.search(f"<{tag}>([^<]+)</{tag}>", r.text)[1]
        return Conn(
            uri=SETTINGS.catalog_uri,
            warehouse=WAREHOUSE_STS,
            auth="sigv4",
            keys=(get("AccessKeyId"), get("SecretAccessKey")),
            vended=False,
            session_token=get("SessionToken"),
        )

    def idp_role_cleanup(self):
        """Remove the policy the token names."""
        from iceberg_lab.keycloak import RUSTFS_IDP_POLICY

        with contextlib.suppress(Exception):  # cleanup: already gone
            self._admin_client().policy_remove(RUSTFS_IDP_POLICY)

    def role_drop(self, role):
        """Empty and remove the group and its policy (errors ignored: cleanup).

        minio's ``group_remove`` sends no ``members`` field, which RustFS rejects (400); the raw
        call with an empty member list removes the group itself (MinIO semantics). The policy
        can only be removed once no group uses it.

        Args:
            role: Role handle as returned by ``role_create``.
        """
        adm = self._admin_client()
        users = [k for k, _ in self.KEYS.values()]
        for call in (
            lambda: self._group_members(role, users, remove=True),
            lambda: self._group_members(role, [], remove=True),
            lambda: adm.policy_remove(role),
        ):
            with contextlib.suppress(Exception):  # cleanup: already gone
                call()

    #: privilege -> catalog actions (scoped to the bucket; RustFS cannot scope them further)
    PRIV: ClassVar[dict] = {
        "describe": ["admin:GetTableMetadata"],
        "list_tables": ["admin:GetTable"],
        "create_table": ["admin:CreateTable"],
        "alter_table": ["admin:GetTableMetadata", "admin:CommitTable"],
        "drop_table": ["admin:DeleteTable"],
    }

    def privilege_not_offered(self, priv):
        """RustFS: granting is IAM administration and cannot be limited to one table.

        Args:
            priv: Privilege key.

        Returns:
            None, or the reason.
        """
        if priv != "manage_grants":
            return None
        return (
            "catalog rustfs has no API for granting rights on one table: rights are IAM policies, "
            "and changing them (admin:CreatePolicy, admin:AttachUserOrGroupPolicy) is "
            "administration of every user and bucket - no per-table grant option"
        )

    def privilege_grant(self, priv, ns, table, user="bob"):
        """A user policy with only the catalog actions of ``PRIV`` (+ storage write to alter).

        Args:
            priv: Privilege key.
            ns: Namespace.
            table: Table name.
            user: Lab user.

        Returns:
            Cleanup callable (detaches and removes the policy).
        """
        bucket = PROFILE.location.removeprefix("s3://").split("/")[0]
        statements = [
            {
                "Effect": "Allow",
                "Action": ["admin:GetTableCatalog", "admin:GetTableBucket"],
                "Resource": ["arn:aws:s3:::*"],
            },
            {
                "Effect": "Allow",
                "Action": ["admin:GetTableNamespace", *self.PRIV[priv]],
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            },
        ]
        if priv == "alter_table":  # the catalog writes the new metadata with the caller's keys
            prefixes = self._table_prefixes(ns, table)
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{p}/*" for p in prefixes],
                }
            )
        name, adm = f"lab-priv-{user}", self._admin_client()
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"Version": "2012-10-17", "Statement": statements}, f)
        adm.policy_add(name, f.name)
        adm.policy_set(name, user=self.KEYS[user][0])

        def cleanup():
            """Detach and remove the user's policy (errors ignored: already gone)."""
            with contextlib.suppress(Exception):
                adm.policy_unset(name, user=self.KEYS[user][0])
            with contextlib.suppress(Exception):
                adm.policy_remove(name)

        return cleanup

    def namespace_grant_not_offered(self, ns):
        """Checked live: the table's data must lie outside the namespace for the claim to hold.

        Args:
            ns: Namespace of the table.

        Returns:
            The reason (namespace grants are not offered).
        """
        data, _ = self._table_prefixes(ns, "t1")
        assert f"/{ns}/" not in f"/{data}/", f"claim outdated: data under the namespace ({data})"
        return (
            "catalog rustfs has no API for namespace-level data grants: table data lies under "
            f"tables/<uuid> ({data}), not under the namespace, and S3 policies - the only data "
            "rights - cannot follow a namespace; catalog actions can be scoped per bucket only"
        )


# =========================================================================== Unity


@dataclass
class UnityAuthz(Backend):
    """Unity permission backend: unitycatalog-authz, Keycloak identities exchanged by the token shim."""

    name: str = "unity"
    base: str = "http://unitycatalog-authz:8080"
    shim: str = "http://unity-token-shim:8080/token"
    trino: dict = field(default_factory=lambda: {"alice": "uc_authz_alice", "bob": "uc_authz_bob"})
    catalog_name: str = "lab"

    def _h(self, client="lab-admin"):
        """Unity token of a Keycloak client via the token shim.

        Args:
            client: Keycloak client id.

        Returns:
            Bearer headers of the identity.
        """
        r = requests.post(
            self.shim,
            data={
                "grant_type": "client_credentials",
                "client_id": client,
                "client_secret": f"{client}-secret",
            },
            timeout=10,
        )
        r.raise_for_status()
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def ensure(self):
        """Bootstrap the catalog and create the SCIM users (idempotent)."""
        try:
            ok = requests.get(
                f"{self.base}/api/2.1/unity-catalog/catalogs", timeout=2
            ).status_code in (
                200,
                401,
                403,
            ) and bool(self._h())  # admin: alice exists only after the setup below
        except requests.RequestException:
            ok = False
        require(ok, "unitycatalog-authz / unity-token-shim not running (compose profile unity)")
        from iceberg_lab.bootstrap import ensure_unity

        ensure_unity(base=self.base, headers=self._h())
        # Users: the external id is the Keycloak client id (Unity resolves tokens by it).
        for u in USERS:
            r = requests.post(
                f"{self.base}/api/1.0/unity-control/scim2/Users",
                json={
                    "displayName": u,
                    "emails": [{"value": f"{u}@lab.local", "primary": True}],
                    "externalId": u,
                },
                headers=self._h(),
                timeout=10,
            )
            # Unity answers an existing user with HTTP 500 (body: status 409 "already exists").
            assert r.status_code in (201, 409) or "User already exists" in r.text, (
                f"create user {u}: {r.status_code} {r.text[:200]}"
            )

    def whoami(self, user):
        """Unity's SCIM ``/Me`` for a user.

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            External id and e-mail addresses.
        """
        r = requests.get(
            f"{self.base}/api/1.0/unity-control/scim2/Me", headers=self._h(user), timeout=10
        )
        r.raise_for_status()
        me = r.json()
        return " ".join([me.get("externalId") or ""] + [e["value"] for e in me.get("emails", [])])

    def _conn(self, client):
        """OAuth2 connection through the token shim.

        Args:
            client: Keycloak client id.

        Returns:
            The connection.
        """
        return Conn(
            uri=f"{self.base}/api/2.1/unity-catalog/iceberg",
            warehouse=self.catalog_name,
            auth="oauth2",
            credential=f"{client}:{client}-secret",
            token_url=self.shim,
            s3_endpoint=True,
        )

    def conn(self, user):
        """Connection of a lab user.

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            The connection.
        """
        return self._conn(user)

    def admin(self):
        """Connection of ``lab-admin``.

        Returns:
            The connection.
        """
        return self._conn("lab-admin")

    def grant(self, ns, table, write, user="alice", revoke=False):
        """USE CATALOG and USE SCHEMA on the parents, SELECT (+ MODIFY) on the table - or remove the table privileges.

        Args:
            ns: Namespace of the table.
            table: Table name.
            write: Read + write (True) or read only (False).
            user: Lab user (``alice`` or ``bob``).
            revoke: Take the grant away instead of giving it.
        """
        email = f"{user}@lab.local"
        api = f"{self.base}/api/2.1/unity-catalog/permissions"
        key = "remove" if revoke else "add"
        # Using a table needs USE CATALOG and USE SCHEMA on the parents (granted once, kept).
        for kind, full, privs in (
            ("catalog", self.catalog_name, ["USE CATALOG"]),
            ("schema", f"{self.catalog_name}.{ns}", ["USE SCHEMA"]),
            (
                "table",
                f"{self.catalog_name}.{ns}.{table}",
                ["SELECT", "MODIFY"] if write else ["SELECT"],
            ),
        ):
            if revoke and kind != "table":
                continue
            r = requests.patch(
                f"{api}/{kind}/{full}",
                json={"changes": [{"principal": email, key: privs}]},
                headers=self._h(),
                timeout=10,
            )
            assert r.ok, f"grant {kind} {full}: {r.status_code} {r.text[:300]}"

    def roles_not_offered(self):
        """Checked live: SCIM /Groups must still fail for the claim to hold.

        Returns:
            The reason (Unity OSS has no roles or groups).
        """
        r = requests.get(
            f"{self.base}/api/1.0/unity-control/scim2/Groups", headers=self._h(), timeout=10
        )
        assert not r.ok, f"claim outdated: Unity answers /scim2/Groups with {r.status_code}"
        return (
            "catalog unity has no API for roles or groups (Unity Catalog OSS 0.7 describes only "
            f"SCIM /Users and /Me; /Groups answers {r.status_code} {r.text[:80]!r}); privileges "
            "go to single users"
        )

    #: privilege -> (securable type, Unity privilege); the others have no privilege at all
    PRIV: ClassVar[dict] = {"create_table": ("schema", "CREATE TABLE")}
    #: Unity privileges that would grant the missing ones (none of them exists - checked)
    MISSING: ClassVar[dict] = {
        "describe": ("BROWSE", "READ_METADATA", "DESCRIBE"),
        "list_tables": ("BROWSE", "LIST"),
        "alter_table": ("ALTER", "MANAGE", "APPLY_TAG"),
        "drop_table": ("DROP", "MANAGE", "OWNERSHIP"),
        "manage_grants": ("MANAGE", "MANAGE_GRANTS", "OWNERSHIP"),
    }

    def privilege_not_offered(self, priv):
        """Checked against the recorded (and live-verified) privilege list of the server.

        Args:
            priv: Privilege key.

        Returns:
            None, or the reason.
        """
        if priv in self.PRIV:
            return None
        import pathlib

        path = pathlib.Path(__file__).resolve().parents[2] / "docs" / "api" / "privileges-unity.txt"
        known = set(path.read_text().split())
        found = [p for p in self.MISSING[priv] if p in known]
        assert not found, f"claim outdated: Unity knows {found} now"
        return (
            f"catalog unity has no API for {priv.replace('_', ' ')}: Unity Catalog OSS 0.7 has no "
            f"privilege for it (none of {', '.join(self.MISSING[priv])} in its Privilege enum) - "
            "only the owner may do it"
        )

    def privilege_grant(self, priv, ns, table, user="bob"):
        """USE CATALOG, USE SCHEMA and the privilege of ``PRIV`` for the user.

        Args:
            priv: Privilege key.
            ns: Namespace (schema).
            table: Table name.
            user: Lab user.

        Returns:
            Cleanup callable (nothing to do: the schema is dropped with the test).
        """
        kind, privilege = self.PRIV[priv]
        api = f"{self.base}/api/2.1/unity-catalog/permissions"
        full = {"schema": f"{self.catalog_name}.{ns}", "table": f"{self.catalog_name}.{ns}.{table}"}
        changes = [
            ("catalog", self.catalog_name, ["USE CATALOG"]),
            ("schema", full["schema"], ["USE SCHEMA"]),
            (kind, full[kind], [privilege]),
        ]
        for k, name, privs in changes:
            r = requests.patch(
                f"{api}/{k}/{name}",
                json={"changes": [{"principal": f"{user}@lab.local", "add": privs}]},
                headers=self._h(),
                timeout=10,
            )
            assert r.ok, f"grant {k} {name}: {r.status_code} {r.text[:300]}"
        return lambda: None

    def grant_namespace(self, ns, user):
        """USE CATALOG, USE SCHEMA and SELECT on the schema for the user.

        Args:
            ns: Namespace of the table.
            user: Lab user (``alice`` or ``bob``).
        """
        api = f"{self.base}/api/2.1/unity-catalog/permissions"
        for kind, full, privs in (
            ("catalog", self.catalog_name, ["USE CATALOG"]),
            ("schema", f"{self.catalog_name}.{ns}", ["USE SCHEMA", "SELECT"]),
        ):
            r = requests.patch(
                f"{api}/{kind}/{full}",
                json={"changes": [{"principal": f"{user}@lab.local", "add": privs}]},
                headers=self._h(),
                timeout=10,
            )
            assert r.ok, f"grant {kind} {full}: {r.status_code} {r.text[:300]}"

    def anonymous_config(self):
        """``GET /v1/config`` without credentials.

        Returns:
            The response.
        """
        return requests.get(
            f"{self.base}/api/2.1/unity-catalog/iceberg/v1/config",
            params={"warehouse": self.catalog_name},
            timeout=10,
        )


# =========================================================================== Gravitino


@dataclass
class GravitinoAuthz(Backend):
    """Gravitino permission backend: the main instance (authorization on), Keycloak identities.

    A user is the token's ``preferred_username`` (``service-account-<client>``) and must be a
    member of metalake ``lab``; privileges go to roles (one per lab user, ``az_<user>``), roles
    to users. Using a table needs ``USE_CATALOG`` and ``USE_SCHEMA`` on its parents.
    """

    name: str = "gravitino"
    #: In front of RustFS nothing is vended (the clients use the lab's keys).
    vended: bool = field(default_factory=lambda: PROFILE.vended_credentials)
    trino: dict = field(
        default_factory=lambda: {u: f"{PROFILE.trino_prefix}_{u}" for u in ("alice", "bob")}
    )

    @property
    def api(self) -> str:
        """Base URL of metalake ``lab`` in Gravitino's own API.

        Returns:
            The URL.
        """
        from iceberg_lab.bootstrap import GRAVITINO_API, GRAVITINO_METALAKE

        return f"{GRAVITINO_API}/api/metalakes/{GRAVITINO_METALAKE}"

    @staticmethod
    def user(client: str) -> str:
        """Gravitino user name of a Keycloak client (principal field preferred_username).

        Args:
            client: Keycloak client id.

        Returns:
            The user name.
        """
        return f"service-account-{client}"

    def whoami(self, user):
        """Gravitino's ``/api/authn/me`` for a user.

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            The principal (``service-account-<client>``).
        """
        from iceberg_lab.bootstrap import GRAVITINO_API

        r = requests.get(f"{GRAVITINO_API}/api/authn/me", headers=self._h(user), timeout=10)
        r.raise_for_status()
        return r.json()["principal"]

    def _h(self, client="lab-admin"):
        """Headers for Gravitino's API with a Keycloak token (scope gravitino) of ``client``.

        Args:
            client: Keycloak client id.

        Returns:
            The headers.
        """
        from iceberg_lab.bootstrap import GRAVITINO_ACCEPT

        r = requests.post(
            SETTINGS.token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": client,
                "client_secret": f"{client}-secret",
                "scope": PROFILE.oauth_scope,  # + polaris when Gravitino fronts Polaris
            },
            timeout=10,
        )
        r.raise_for_status()
        return {
            "Authorization": f"Bearer {r.json()['access_token']}",
            "Accept": GRAVITINO_ACCEPT,
            "Content-Type": "application/json",
        }

    def _call(self, method, path, body=None, client="lab-admin", ok=(200,)):
        """One call of Gravitino's API; asserts the status.

        Args:
            method: HTTP method.
            path: Path below the metalake.
            body: JSON body.
            client: Keycloak client whose token is sent.
            ok: Accepted status codes.

        Returns:
            The response.
        """
        r = requests.request(
            method, f"{self.api}/{path}", json=body, headers=self._h(client), timeout=15
        )
        assert r.status_code in ok, f"{method} {path}: {r.status_code} {r.text[:300]}"
        return r

    @staticmethod
    def _privs(*names):
        """Privilege list of Gravitino's API (all ALLOW).

        Args:
            *names: Privilege names.

        Returns:
            The list.
        """
        return [{"name": n, "condition": "ALLOW"} for n in names]

    def _object(self, kind, ns=None, table=None):
        """``(type, fullName)`` of a securable object.

        Args:
            kind: ``metalake``, ``catalog``, ``schema`` or ``table``.
            ns: Namespace (schema and table).
            table: Table name (table).

        Returns:
            The pair.
        """
        from iceberg_lab.bootstrap import GRAVITINO_METALAKE

        return kind, {
            "metalake": GRAVITINO_METALAKE,
            "catalog": WAREHOUSE_STS,
            "schema": f"{WAREHOUSE_STS}.{ns}",
            "table": f"{WAREHOUSE_STS}.{ns}.{table}",
        }[kind]

    def _role_privileges(self, role, kind, ns, table, privs, revoke=False, client="lab-admin"):
        """Grant (or revoke) privileges on one object to a role.

        Args:
            role: Role name.
            kind: Object kind (see ``_object``).
            ns: Namespace.
            table: Table name.
            privs: Privilege names.
            revoke: Revoke instead of grant.
            client: Keycloak client that calls.

        Returns:
            The response.
        """
        typ, full = self._object(kind, ns, table)
        return self._call(
            "PUT",
            f"permissions/roles/{role}/{typ}/{full}/{'revoke' if revoke else 'grant'}",
            {"privileges": self._privs(*privs)},
            client=client,
        )

    def _use(self, role, ns):
        """USE_CATALOG and USE_SCHEMA for a role (needed to reach any table of ``ns``).

        Args:
            role: Role name.
            ns: Namespace.
        """
        self._role_privileges(role, "catalog", None, None, ["USE_CATALOG"])
        self._role_privileges(role, "schema", ns, None, ["USE_SCHEMA"])

    def _ensure_user(self, client):
        """Make a Keycloak client's user a member of the metalake (idempotent).

        Args:
            client: Keycloak client id.
        """
        self._call("POST", "users", {"name": self.user(client)}, ok=(200, 409))

    def ensure(self):
        """Users alice and bob in the metalake, each with an (initially empty) role ``az_<user>``."""
        try:
            url = f"{self.api.split('/api/')[0]}/api/version"
            ok = requests.get(url, timeout=3).status_code in (200, 401)  # 401: OAuth on
        except requests.RequestException:
            ok = False
        require(ok, "gravitino not running (compose profile gravitino)")
        for u in USERS:
            self._ensure_user(u)
            self._call(
                "POST",
                "roles",
                {"name": f"az_{u}", "properties": {}, "securableObjects": []},
                ok=(200, 409),
            )
            self._call("PUT", f"permissions/users/{self.user(u)}/grant", {"roleNames": [f"az_{u}"]})

    def _conn(self, client, scope=None):
        """OAuth2 connection of a Keycloak client.

        Args:
            client: Keycloak client id.
            scope: Token scope (default: the profile's - audience gravitino, plus polaris when
                Gravitino fronts Polaris).

        Returns:
            The connection.
        """
        # In front of RustFS nothing is vended: the clients bring the lab's S3 keys, so only
        # the catalog decides what a user may do (as with RustFS's own SigV4 keys).
        keys = None
        if not PROFILE.vended_credentials:
            keys = (SETTINGS.s3_access_key, SETTINGS.s3_secret_key)
        return Conn(
            uri=SETTINGS.catalog_uri,
            warehouse=WAREHOUSE_STS,
            auth="oauth2",
            credential=f"{client}:{client}-secret",
            token_url=SETTINGS.token_endpoint,
            scope=scope or PROFILE.oauth_scope,
            keys=keys,
            vended=PROFILE.vended_credentials,
            s3_endpoint=PROFILE.client_s3_endpoint,  # in front of Unity: keys without endpoint
        )

    def conn(self, user):
        """Connection of a lab user.

        Args:
            user: Lab user (``alice`` or ``bob``).

        Returns:
            The connection.
        """
        return self._conn(user)

    def admin(self):
        """Connection of ``lab-admin`` (the service admin, creator and owner of the objects).

        Returns:
            The connection.
        """
        return self._conn("lab-admin")

    def grant(self, ns, table, write, user="alice", revoke=False):
        """USE_CATALOG/USE_SCHEMA and SELECT_TABLE (+ MODIFY_TABLE) to the user's role.

        Args:
            ns: Namespace of the table.
            table: Table name.
            write: Read + write (True) or read only (False).
            user: Lab user (``alice`` or ``bob``).
            revoke: Take the table privileges away instead of giving them.
        """
        role = f"az_{user}"
        if not revoke:
            self._use(role, ns)
        privs = ["SELECT_TABLE", "MODIFY_TABLE"] if write else ["SELECT_TABLE"]
        self._role_privileges(role, "table", ns, table, privs, revoke=revoke)

    def anonymous_config(self):
        """``GET /v1/config`` without credentials.

        Returns:
            The response.
        """
        return requests.get(
            f"{SETTINGS.catalog_uri}/v1/config", params={"warehouse": WAREHOUSE_STS}, timeout=10
        )

    def role_create(self, name):
        """A role without privileges.

        Args:
            name: Role name.

        Returns:
            The role name.
        """
        self._call("POST", "roles", {"name": name, "properties": {}, "securableObjects": []})
        return name

    def role_member(self, role, user, remove=False):
        """Grant the role to the user (or revoke it).

        Args:
            role: Role name.
            user: Lab user (``alice`` or ``bob``).
            remove: Revoke instead of grant.
        """
        self._call(
            "PUT",
            f"permissions/users/{self.user(user)}/{'revoke' if remove else 'grant'}",
            {"roleNames": [role]},
        )

    def role_grant_table(self, role, ns, table):
        """USE_CATALOG, USE_SCHEMA and SELECT_TABLE on one table to the role.

        Args:
            role: Role name.
            ns: Namespace of the table.
            table: Table name.
        """
        self._use(role, ns)
        self._role_privileges(role, "table", ns, table, ["SELECT_TABLE"])

    def role_drop(self, role):
        """Delete the role.

        Args:
            role: Role name.
        """
        self._call("DELETE", f"roles/{role}")

    def idp_role_grant(self, ns, table):
        """A group named like the token-only role, holding a role with read on the table.

        Gravitino takes a user's groups from the token claim ``lab_roles``
        (``gravitino.authenticator.oauth.groupsFields``); the role is granted to the group,
        nobody is made a member in Gravitino. The IdP client's user is added to the metalake
        (else every call is refused before any role counts) - not to the group or the role.

        Args:
            ns: Namespace of the table.
            table: Table name.
        """
        from iceberg_lab.keycloak import IDP_CLIENT, IDP_ROLE

        self._ensure_user(IDP_CLIENT)
        self._call("POST", "groups", {"name": IDP_ROLE}, ok=(200, 409))
        self._call(
            "POST",
            "roles",
            {"name": IDP_ROLE, "properties": {}, "securableObjects": []},
            ok=(200, 409),
        )
        self.idp_role = IDP_ROLE
        self.role_grant_table(IDP_ROLE, ns, table)
        self._call("PUT", f"permissions/groups/{IDP_ROLE}/grant", {"roleNames": [IDP_ROLE]})

    def idp_role_conn(self):
        """Connection of the Keycloak client whose token carries the role.

        Returns:
            The connection.
        """
        from iceberg_lab.keycloak import IDP_CLIENT

        return self._conn(IDP_CLIENT, scope="openid")  # audience gravitino from its own mapper

    def idp_role_cleanup(self):
        """Delete the token-only role and its group."""
        if getattr(self, "idp_role", None):
            self.role_drop(self.idp_role)
            self._call("DELETE", f"groups/{self.idp_role}", ok=(200, 404))

    #: privilege -> (object kind, Gravitino privilege); always with USE_CATALOG and USE_SCHEMA
    PRIV: ClassVar[dict] = {
        "list_tables": ("schema", "USE_SCHEMA"),
        "create_table": ("schema", "CREATE_TABLE"),
        "alter_table": ("table", "MODIFY_TABLE"),
        "manage_grants": ("metalake", "MANAGE_GRANTS"),
    }
    #: Gravitino privileges that would grant the missing ones (none of them exists - checked)
    MISSING: ClassVar[dict] = {
        "describe": ("DESCRIBE_TABLE", "READ_METADATA", "BROWSE"),
        "drop_table": ("DROP_TABLE", "MANAGE_TABLE", "OWNERSHIP"),
    }

    def privilege_not_offered(self, priv):
        """Checked against the recorded privilege list of the image.

        Args:
            priv: Privilege key.

        Returns:
            None, or the reason.
        """
        if priv in self.PRIV:
            return None
        import pathlib

        path = (
            pathlib.Path(__file__).resolve().parents[2]
            / "docs"
            / "api"
            / "privileges-gravitino.txt"
        )
        known = set(path.read_text().split())
        found = [p for p in self.MISSING[priv] if p in known]
        assert not found, f"claim outdated: Gravitino knows {found} now"
        return (
            f"catalog gravitino has no API for {priv.replace('_', ' ')}: Gravitino 1.3 has no "
            f"privilege for it (none of {', '.join(self.MISSING[priv])} in Privilege.Name) - "
            + (
                "loading a table needs SELECT_TABLE or MODIFY_TABLE, which open the data too"
                if priv == "describe"
                else "only the owner may do it"
            )
        )

    def privilege_grant(self, priv, ns, table, user="bob"):
        """A fresh role of the user holding USE_CATALOG, USE_SCHEMA and the privilege.

        Args:
            priv: Privilege key.
            ns: Namespace.
            table: Table name.
            user: Lab user.

        Returns:
            Cleanup callable (revokes and deletes the role).
        """
        kind, privilege = self.PRIV[priv]
        role = self.role_create(f"priv_{priv}_{uuid.uuid4().hex[:6]}")
        self._use(role, ns)
        if privilege != "USE_SCHEMA":
            self._role_privileges(role, kind, ns, table, [privilege])
        self.role_member(role, user)

        def cleanup():
            """Revoke the role from the user and delete it."""
            with contextlib.suppress(AssertionError):
                self.role_member(role, user, remove=True)
            with contextlib.suppress(AssertionError):
                self.role_drop(role)

        return cleanup

    def grant_read_as(self, user, ns, table, grantee="alice"):
        """``user`` grants read on the table to the grantee's role, with own credentials.

        Args:
            user: Lab user who grants.
            ns: Namespace.
            table: Table in it.
            grantee: Lab user who receives the grant.

        Returns:
            The HTTP status of the grant call.
        """
        # Reading needs USE_CATALOG and USE_SCHEMA as well: all three grants with own credentials.
        headers = self._h(user)
        for kind, priv in (
            ("catalog", "USE_CATALOG"),
            ("schema", "USE_SCHEMA"),
            ("table", "SELECT_TABLE"),
        ):
            typ, full = self._object(kind, ns, table)
            r = requests.put(
                f"{self.api}/permissions/roles/az_{grantee}/{typ}/{full}/grant",
                json={"privileges": self._privs(priv)},
                headers=headers,
                timeout=15,
            )
            if not r.ok:
                break
        return r.status_code

    def grant_namespace(self, ns, user):
        """A fresh role with USE_CATALOG, USE_SCHEMA and SELECT_TABLE on the schema (inherited
        by its tables) for the user; listed in ``namespace_roles`` for cleanup.

        Args:
            ns: Namespace of the table.
            user: Lab user (``alice`` or ``bob``).
        """
        role = self.role_create(f"ns_{user}_{uuid.uuid4().hex[:6]}")
        self._use(role, ns)
        self._role_privileges(role, "schema", ns, None, ["SELECT_TABLE"])
        self.role_member(role, user)
        self.namespace_roles = [*getattr(self, "namespace_roles", []), role]


BACKENDS = {
    "lakekeeper": LakekeeperAuthz,
    "polaris": PolarisAuthz,
    "rustfs": RustFSAuthz,
    "unity": UnityAuthz,
    "gravitino": GravitinoAuthz,
}


def backend() -> Backend:
    """The permission backend of the catalog under test.

    Returns:
        The backend instance.
    """
    if PROFILE.server not in BACKENDS:  # the reference fixture: no authentication at all
        pytest.skip(f"catalog {PROFILE.name} has no API for users and permissions")
    return BACKENDS[PROFILE.server]()
