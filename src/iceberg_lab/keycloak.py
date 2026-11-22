"""Keycloak realm "iceberg": per-service token audiences and identity-provider roles (idempotent).

* Lakekeeper: the lab clients (lab-admin, alice, bob) carry audience "lakekeeper" on every
  token (client mapper, authz/keycloak/realm.json).
* Unity: tokens for the Unity token exchange are requested with ``scope=unity`` (optional
  client scope adding audience "unity"; Unity accepts further audiences).
* Polaris (external identity provider next to its own broker): ``scope=polaris`` adds
  audience "polaris"; the scope itself becomes the role PRINCIPAL_ROLE:ALL, a scope
  ``polaris-role-<name>`` the role PRINCIPAL_ROLE:<name>.
* RustFS: own clients ``rustfs-alice`` / ``rustfs-bob`` with ONLY audience "rustfs" plus the
  IAM policy as claim "rustfs_policy". RustFS rejects a token that also names another
  service (strict OIDC audience check: "`lakekeeper` is not a trusted audience"), and the
  lakekeeper audience cannot be dropped from the lab clients' tokens per request.
* Roles only the identity provider assigns (tests/security/test_rbac.py::test_idp_role_is_a_catalog_role):
  client ``rbac-idp`` (claim ``lab_roles`` for Lakekeeper, scope ``polaris-role-lab_readers``
  for Polaris) and ``rustfs-rbac-idp`` (claim ``rustfs_policy``).

Scopes and these clients are created here, not in realm.json: a realm import that defines
client scopes does not create Keycloak's built-in ones (``basic`` carries ``sub``).
"""

from __future__ import annotations

import requests

KC = "http://keycloak:8080"
LAB_CLIENTS = ("lab-admin", "alice", "bob")
SERVICE_SCOPES = ("unity", "polaris")
#: RustFS identities: client -> IAM policy (claim "rustfs_policy"); only audience "rustfs".
RUSTFS_CLIENTS = {"rustfs-alice": "az-alice", "rustfs-bob": "az-bob"}
#: Roles that only the identity provider assigns (tests/security/test_rbac.py::test_idp_role_is_a_catalog_role):
#: the catalogs know these roles, but no user is a member - the token makes the client one.
IDP_ROLE = "lab-readers"
#: Polaris reads roles from the token's scope: an optional scope with this name -> PRINCIPAL_ROLE.
POLARIS_IDP_SCOPE = "polaris-role-lab_readers"
#: Client whose tokens carry IDP_ROLE (Lakekeeper: claim lab_roles; Polaris: POLARIS_IDP_SCOPE).
IDP_CLIENT = "rbac-idp"
#: RustFS: the identity provider names the IAM policy (claim rustfs_policy).
RUSTFS_IDP_CLIENT, RUSTFS_IDP_POLICY = "rustfs-rbac-idp", "rbac-idp-readers"


def admin_headers() -> dict:
    """Bearer header of Keycloak's master-realm admin (admin-cli).

    Returns:
        ``{"Authorization": "Bearer ..."}``.
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


def audience_mapper(aud: str) -> dict:
    """Token mapper that adds ``aud`` to access tokens.

    Args:
        aud: Audience to add.

    Returns:
        The Keycloak protocol-mapper definition.
    """
    return {
        "name": f"audience-{aud}",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-audience-mapper",
        "config": {
            "included.custom.audience": aud,
            "access.token.claim": "true",
            "id.token.claim": "false",
        },
    }


def policy_claim_mapper(policy: str) -> dict:
    """Hard-coded claim ``rustfs_policy`` (the IAM policy RustFS applies to the identity).

    Args:
        policy: Name of the RustFS IAM policy.

    Returns:
        The Keycloak protocol-mapper definition.
    """
    return {
        "name": "rustfs-policy",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-hardcoded-claim-mapper",
        "config": {
            "claim.name": "rustfs_policy",
            "claim.value": policy,
            "jsonType.label": "String",
            "access.token.claim": "true",
            "id.token.claim": "false",
            "userinfo.token.claim": "false",
        },
    }


def roles_claim_mapper(roles: list[str]) -> dict:
    """Hard-coded JSON array claim ``lab_roles`` (Lakekeeper: LAKEKEEPER__OPENID_ROLES_CLAIM).

    Args:
        roles: Role names to put into the claim.

    Returns:
        The Keycloak protocol-mapper definition.
    """
    import json

    return {
        "name": "lab-roles",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-hardcoded-claim-mapper",
        "config": {
            "claim.name": "lab_roles",
            "claim.value": json.dumps(roles),
            "jsonType.label": "JSON",
            "access.token.claim": "true",
            "id.token.claim": "false",
            "userinfo.token.claim": "false",
        },
    }


def ensure_idp_role_clients(h: dict) -> list[str]:
    """Clients whose roles come from the token only (see IDP_ROLE).

    Args:
        h: Keycloak admin headers.

    Returns:
        What was changed (for the script's output).
    """
    base = f"{KC}/admin/realms/iceberg"
    changed = []
    scopes = {
        s["name"]: s["id"]
        for s in requests.get(f"{base}/client-scopes", headers=h, timeout=10).json()
    }
    if POLARIS_IDP_SCOPE not in scopes:  # no mappers: its name in "scope" is the role
        requests.post(
            f"{base}/client-scopes",
            json={
                "name": POLARIS_IDP_SCOPE,
                "protocol": "openid-connect",
                "attributes": {"include.in.token.scope": "true"},
            },
            headers=h,
            timeout=10,
        ).raise_for_status()
        changed.append(f"scope {POLARIS_IDP_SCOPE}")
        scopes = {
            s["name"]: s["id"]
            for s in requests.get(f"{base}/client-scopes", headers=h, timeout=10).json()
        }
    if ensure_client(
        h, IDP_CLIENT, [audience_mapper("lakekeeper"), roles_claim_mapper([IDP_ROLE])]
    ):
        changed.append(f"client {IDP_CLIENT}")
    (client,) = requests.get(
        f"{base}/clients", params={"clientId": IDP_CLIENT}, headers=h, timeout=10
    ).json()
    url = f"{base}/clients/{client['id']}"
    have = {
        s["name"]
        for s in requests.get(f"{url}/optional-client-scopes", headers=h, timeout=10).json()
    }
    for name in ("polaris", POLARIS_IDP_SCOPE):
        if name not in have:
            requests.put(
                f"{url}/optional-client-scopes/{scopes[name]}", headers=h, timeout=10
            ).raise_for_status()
            changed.append(f"{IDP_CLIENT}: optional scope {name}")
    if ensure_client(
        h,
        RUSTFS_IDP_CLIENT,
        [audience_mapper("rustfs"), policy_claim_mapper(RUSTFS_IDP_POLICY)],
        only_mapped_audiences=True,
    ):
        changed.append(f"client {RUSTFS_IDP_CLIENT}")
    return changed


def ensure_service_scopes(h: dict | None = None) -> list[str]:
    """Create the optional scopes, attach them to the lab clients; returns what changed.

    Args:
        h: Keycloak admin headers (default: fetched).

    Returns:
        What was changed (for the script's output).
    """
    h = h or admin_headers()
    base = f"{KC}/admin/realms/iceberg"
    changed = []
    scopes = {
        s["name"]: s for s in requests.get(f"{base}/client-scopes", headers=h, timeout=10).json()
    }
    for name in SERVICE_SCOPES:
        if name not in scopes:
            requests.post(
                f"{base}/client-scopes",
                json={
                    "name": name,
                    "protocol": "openid-connect",
                    "attributes": {
                        "include.in.token.scope": "true",
                        "display.on.consent.screen": "false",
                    },
                    "protocolMappers": [audience_mapper(name)],
                },
                headers=h,
                timeout=10,
            ).raise_for_status()
            changed.append(f"scope {name}")
    scopes = {
        s["name"]: s["id"]
        for s in requests.get(f"{base}/client-scopes", headers=h, timeout=10).json()
    }
    for cid in LAB_CLIENTS:
        (client,) = requests.get(
            f"{base}/clients", params={"clientId": cid}, headers=h, timeout=10
        ).json()
        url = f"{base}/clients/{client['id']}"
        mappers = {m["name"]: m for m in client.get("protocolMappers", [])}
        for name in ("audience-unity", "audience-rustfs", "rustfs-policy"):  # earlier lab versions
            if name in mappers:
                requests.delete(
                    f"{url}/protocol-mappers/models/{mappers[name]['id']}", headers=h, timeout=10
                ).raise_for_status()
                changed.append(f"{cid}: removed {name}")
        have = {
            s["name"]
            for s in requests.get(f"{url}/optional-client-scopes", headers=h, timeout=10).json()
        }
        for name in SERVICE_SCOPES:
            if name not in have:
                requests.put(
                    f"{url}/optional-client-scopes/{scopes[name]}", headers=h, timeout=10
                ).raise_for_status()
                changed.append(f"{cid}: optional scope {name}")
        for name in (have - set(SERVICE_SCOPES)) & {"rustfs"}:
            requests.delete(
                f"{url}/optional-client-scopes/{scopes[name]}", headers=h, timeout=10
            ).raise_for_status()
            changed.append(f"{cid}: removed optional scope {name}")
    if "rustfs" in scopes:  # earlier lab version: RustFS audience as optional scope
        requests.delete(
            f"{base}/client-scopes/{scopes['rustfs']}", headers=h, timeout=10
        ).raise_for_status()
        changed.append("removed scope rustfs")
    for cid, policy in RUSTFS_CLIENTS.items():
        if ensure_client(
            h,
            cid,
            [audience_mapper("rustfs"), policy_claim_mapper(policy)],
            only_mapped_audiences=True,
        ):
            changed.append(f"client {cid}")
    return changed + ensure_idp_role_clients(h)


def ensure_client(
    h: dict,
    cid: str,
    mappers: list[dict],
    lifespan: int | None = None,
    only_mapped_audiences: bool = False,
) -> bool:
    """Service-account client (secret ``<cid>-secret``) with these token mappers.

    ``only_mapped_audiences``: no role scope (Keycloak's default roles would add audience
    "account") - the token names only the audiences of ``mappers``.

    Args:
        h: Keycloak admin headers.
        cid: Client id.
        mappers: Protocol mappers the client must have.
        lifespan: Access-token lifetime in seconds (None: realm default).
        only_mapped_audiences: Turn off full scope, so only the mappers' audiences are in the token.

    Returns:
        True if the client was created, False if it existed (missing mappers added).
    """
    base = f"{KC}/admin/realms/iceberg"
    found = requests.get(f"{base}/clients", params={"clientId": cid}, headers=h, timeout=10).json()
    if found:
        if only_mapped_audiences and found[0].get("fullScopeAllowed", True):
            requests.put(
                f"{base}/clients/{found[0]['id']}",
                json=found[0] | {"fullScopeAllowed": False},
                headers=h,
                timeout=10,
            ).raise_for_status()
        have = {m["name"] for m in found[0].get("protocolMappers", [])}
        for m in mappers:
            if m["name"] not in have:
                requests.post(
                    f"{base}/clients/{found[0]['id']}/protocol-mappers/models",
                    json=m,
                    headers=h,
                    timeout=10,
                ).raise_for_status()
        return False
    requests.post(
        f"{base}/clients",
        json={
            "clientId": cid,
            "enabled": True,
            "protocol": "openid-connect",
            "publicClient": False,
            "secret": f"{cid}-secret",
            "serviceAccountsEnabled": True,
            "standardFlowEnabled": False,
            "directAccessGrantsEnabled": False,
            "fullScopeAllowed": not only_mapped_audiences,
            "attributes": {"access.token.lifespan": str(lifespan)} if lifespan else {},
            "protocolMappers": mappers,
        },
        headers=h,
        timeout=10,
    ).raise_for_status()
    return True


def token(client: str, scope: str | None = None, realm: str = "iceberg") -> str:
    """Client-credentials token of a lab client (secret ``<client>-secret``).

    Args:
        client: Keycloak client id.
        scope: Optional scopes to request.
        realm: Keycloak realm.

    Returns:
        The access token.
    """
    data = {
        "grant_type": "client_credentials",
        "client_id": client,
        "client_secret": f"{client}-secret",
    }
    if scope:
        data["scope"] = scope
    r = requests.post(f"{KC}/realms/{realm}/protocol/openid-connect/token", data=data, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]
