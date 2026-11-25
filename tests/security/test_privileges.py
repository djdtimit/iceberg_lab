"""Which privileges the catalogs offer, and what each common one allows (base, every catalog).

Inventory (``docs/api/privileges-<catalog>.txt``, written by scripts/privilege_inventory.py from
the running images: Lakekeeper's management OpenAPI, Polaris' and Unity's privilege enums,
RustFS' IAM actions):

``test_privilege_inventory_matches_the_image``
    the recorded list belongs to the image the lab runs - after an update the list must be
    re-recorded (new privileges then show up in the diff);
``test_recorded_privileges_are_accepted``
    every recorded privilege is accepted by the running catalog (granted once and taken away;
    RustFS: accepted in an IAM policy) and an unknown one is refused; Lakekeeper: the live
    OpenAPI lists exactly the recorded ones.

Matrix of the privileges two or more catalogs share (``authz_backends.PRIVILEGES``), each with
exactly one grant to bob (catalog-specific, see the backends' ``PRIV``) and raw REST calls:

``test_privilege_allows_its_action``
    the privilege allows its action - and the actions it does not cover stay refused
    (``NARROW``); ``describe`` additionally must not open the data;
``test_action_needs_its_privilege``
    without the privilege the action is refused.

Only a permission answer counts as refused (401/403; Lakekeeper also hides objects with 404).
"""

from __future__ import annotations

import pathlib
import re
import uuid

import pyarrow as pa
import pytest
import requests
from authz_backends import PRIVILEGES, ROWS, Backend, backend

from iceberg_lab.config import PROFILE

from conftest import expect, find_gap, gap_table

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [{"id": 1, "name": "id", "type": "long", "required": False}],
}
#: Actions a privilege must NOT open (it covers only its own; drop last - it removes the table).
NARROW = {
    "describe": ("alter_table", "create_table", "drop_table"),
    "list_tables": ("alter_table", "create_table", "drop_table"),
    "create_table": ("alter_table", "drop_table"),
    "alter_table": ("create_table", "drop_table"),
    "drop_table": ("create_table",),
    "manage_grants": ("create_table", "drop_table"),
}
# Filled from observed failures only: (catalog, privilege, regex, cause).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "priv", "step"), [
    ("gravitino", "list_tables", "listing", r"listing without the table: \[\]", "gravitino-listing-needs-table-privilege"),
    # Gravitino in front of Polaris: Polaris decides again by its own grants (401 for a user it
    # has no principal for; Gravitino's own grant call loads the object there and answers 500).
    *(("gravitino_polaris", p, "allowed", rf"{p} with the {p} privilege answered (401|500)", "gravitino-rest-backend-grants-not-forwarded")
      for p in ("list_tables", "create_table", "alter_table", "manage_grants")),
])
# fmt: on

#: Compose service of each catalog (the image the inventory belongs to).
SERVICES = {
    "lakekeeper": "lakekeeper-authz",
    "polaris": "polaris",
    "unity": "unitycatalog-authz",
    "rustfs": "rustfs",
    "gravitino": "gravitino",
}


def refused(status: int) -> bool:
    """Whether an answer is a permission refusal.

    Args:
        status: HTTP status.

    Returns:
        True for 401/403 (and 404 on Lakekeeper, which hides objects).
    """
    return status in (401, 403) or (PROFILE.name == "lakekeeper" and status == 404)


def recorded() -> tuple[str, list[str]]:
    """The recorded inventory of the catalog under test.

    Returns:
        ``(image line, privileges)``.
    """
    if PROFILE.server not in SERVICES:
        pytest.skip(f"catalog {PROFILE.name} has no API for privileges")
    lines = (ROOT / "docs" / "api" / f"privileges-{PROFILE.server}.txt").read_text().splitlines()
    return lines[0], [ln for ln in lines[1:] if ln.strip()]


@pytest.fixture
def pv():
    """A fresh namespace with table ``t`` (3 rows) and no grants; grants removed afterwards.

    Yields:
        ``(backend, namespace, cleanups)`` - append the cleanup of every grant.
    """
    b = backend()
    b.ensure()
    cat = b.catalog("admin")
    ns = f"priv_{uuid.uuid4().hex[:6]}"
    cat.create_namespace(ns)
    cat.create_table(f"{ns}.t", schema=pa.schema([("id", pa.int64())])).append(
        pa.table({"id": list(range(1, ROWS + 1))})
    )
    cleanups: list = []
    yield b, ns, cleanups
    for c in cleanups:
        c()
    Backend.drop_namespace(b, ns)


def act(b, user: str, ns: str, action: str) -> int:
    """Run one action as ``user`` with a single raw REST call.

    Args:
        b: Permission backend.
        user: Lab user.
        ns: Namespace with table ``t``.
        action: One of :data:`authz_backends.PRIVILEGES`.

    Returns:
        The HTTP status of the call.
    """
    if action == "manage_grants":
        return b.grant_read_as(user, ns, "t")
    s, base = b.rest(user)
    t = f"{base}/namespaces/{ns}/tables/t"
    return {
        "describe": lambda: s.get(t, timeout=15),
        "list_tables": lambda: s.get(f"{base}/namespaces/{ns}/tables", timeout=15),
        "create_table": lambda: s.post(
            f"{base}/namespaces/{ns}/tables",
            json={"name": f"n_{uuid.uuid4().hex[:6]}", "schema": SCHEMA},
            timeout=15,
        ),
        "alter_table": lambda: s.post(
            t,
            json={
                "requirements": [],
                "updates": [{"action": "set-properties", "updates": {"k": "v"}}],
            },
            timeout=15,
        ),
        "drop_table": lambda: s.delete(t, timeout=15),
    }[action]().status_code


def data_is_closed(b, user: str, ns: str) -> str | None:
    """With ``describe`` only, the user must not get at the table's data.

    Args:
        b: Permission backend.
        user: Lab user.
        ns: Namespace with table ``t``.

    Returns:
        None if the data stays closed, else what opened it.
    """
    import boto3
    from botocore.exceptions import ClientError

    from iceberg_lab.config import SETTINGS

    data = b.catalog("admin").load_table(f"{ns}.t").inspect.files()["file_path"][0].as_py()
    bucket, key = data.removeprefix("s3://").split("/", 1)
    if not b.vended:  # RustFS: the user's own keys must not read the data file
        ak, sk = b.KEYS[user]
        keys, how = (ak, sk, None), "the user's own S3 keys"
    else:
        s, base = b.rest(user)
        r = s.get(
            f"{base}/namespaces/{ns}/tables/t",
            headers={"X-Iceberg-Access-Delegation": "vended-credentials"},
            timeout=15,
        )
        if not r.ok:
            return None  # keys are refused: closed
        config = r.json().get("config", {})
        for c in r.json().get("storage-credentials", []):
            config |= c.get("config", {})
        if "s3.access-key-id" not in config:
            return None  # no keys: closed
        keys = (
            config["s3.access-key-id"],
            config["s3.secret-access-key"],
            config.get("s3.session-token"),
        )
        how = "the keys vended with describe only"
    s3 = boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=keys[0],
        aws_secret_access_key=keys[1],
        aws_session_token=keys[2],
    )
    try:
        s3.get_object(Bucket=bucket, Key=key)
    except ClientError:
        return None
    return f"{how} read the data file {data}"


# =========================================================================== inventory


def test_privilege_inventory_matches_the_image():
    """The recorded privilege list belongs to the image the lab runs.

    Checks:
        * the first line of ``docs/api/privileges-<catalog>.txt`` names the image the compose
          file runs for the catalog - else re-run scripts/privilege_inventory.py (a new
          version's added or removed privileges then show in the diff).
    """
    if PROFILE.server not in SERVICES:
        pytest.skip(f"catalog {PROFILE.name} has no API for privileges")
    head, privileges = recorded()
    compose = (ROOT / ".devcontainer" / "docker-compose.yml").read_text()
    block = compose.split(f"\n  {SERVICES[PROFILE.server]}:\n", 1)[1]
    image = re.search(r"^\s+image:\s*(\S+)", block, re.MULTILINE)[1]
    assert privileges, "empty inventory"
    assert head == f"# image: {image}", f"inventory of {head}, the lab runs {image}"


def test_recorded_privileges_are_accepted():
    """Every recorded privilege is accepted by the running catalog; an unknown one is refused.

    Checks:
        * Lakekeeper: the live management OpenAPI lists exactly the recorded assignment types;
        * Polaris: each recorded catalog/namespace/table privilege can be granted (and is taken
          away again) - views, policies and semantic models are listed but need own objects;
        * Unity: each privilege is accepted on at least one securable (catalog, schema, table,
          metastore);
        * RustFS: an IAM policy with all recorded actions is accepted;
        * Gravitino: each privilege is accepted on the metalake (granted to a probe role and
          revoked again);
        * an unknown privilege is refused (the acceptance above means something).
    """
    _, privileges = recorded()
    b = backend()
    b.ensure()
    getattr(_Accept, PROFILE.server)(b, privileges)


class _Accept:
    """Per catalog: grant every recorded privilege once (see the test above)."""

    @staticmethod
    def lakekeeper(b, privileges):
        """Compare the live OpenAPI with the recorded list.

        Args:
            b: Permission backend.
            privileges: Recorded ``<object>: <type>`` lines.
        """
        spec = requests.get(f"{b.base}/api-docs/management/v1/openapi.json", timeout=20).json()
        live = sorted(
            f"{name.removesuffix('Assignment')}: {t}"
            for name, s in spec["components"]["schemas"].items()
            if name.endswith("Assignment") and "discriminator" in s
            for t in s["discriminator"]["mapping"]
        )
        assert live == sorted(privileges), (
            f"new: {sorted(set(live) - set(privileges))}, gone: {sorted(set(privileges) - set(live))}"
        )
        cat = b.catalog("admin")
        ns = f"inv_{uuid.uuid4().hex[:6]}"
        cat.create_namespace(ns)
        try:
            nsid = cat.load_namespace_properties(ns)["namespace_id"]
            r = requests.post(
                f"{b.base}/management/v1/permissions/namespace/{nsid}/assignments",
                json={"writes": [{"type": "nope", "user": b.user_ids["bob"]}]},
                headers=b._h(),
                timeout=10,
            )
            assert 400 <= r.status_code < 500, f"unknown assignment type: {r.status_code}"
        finally:
            cat.drop_namespace(ns)

    @staticmethod
    def polaris(b, privileges):
        """Grant and revoke each catalog/namespace/table privilege on a probe role.

        Args:
            b: Permission backend.
            privileges: Recorded ``<type>: <privilege>`` lines.
        """
        from iceberg_lab.config import WAREHOUSE_STS

        cat = b.catalog("admin")
        ns = f"inv_{uuid.uuid4().hex[:6]}"
        cat.create_namespace(ns)
        cat.create_table(f"{ns}.t", schema=pa.schema([("id", pa.int64())]))
        role = b.role_create(f"inv_{uuid.uuid4().hex[:6]}")
        url = f"catalogs/{WAREHOUSE_STS}/catalog-roles/{role}/grants"
        target = {
            "Catalog": {},
            "Namespace": {"namespace": [ns]},
            "Table": {"namespace": [ns], "tableName": "t"},
        }
        refused_ = []
        try:
            for line in privileges:
                kind, priv = line.split(": ")
                if kind not in target:
                    continue
                grant = {"type": kind.lower(), "privilege": priv} | target[kind]
                r = requests.put(
                    f"{b.base}/api/management/v1/{url}",
                    json={"grant": grant},
                    headers=b._h(),
                    timeout=10,
                )
                if r.status_code not in (200, 201):
                    refused_.append(f"{line} ({r.status_code})")
                    continue
                requests.post(
                    f"{b.base}/api/management/v1/{url}",
                    json={"grant": grant},
                    headers=b._h(),
                    timeout=10,
                )
            bad = {"type": "table", "privilege": "NOPE"} | target["Table"]
            r = requests.put(
                f"{b.base}/api/management/v1/{url}", json={"grant": bad}, headers=b._h(), timeout=10
            )
            assert r.status_code == 400, f"unknown privilege answered {r.status_code}"
        finally:
            b.role_drop(role)
            Backend.drop_namespace(b, ns)
        assert not refused_, f"recorded privileges the catalog refuses: {refused_}"

    @staticmethod
    def unity(b, privileges):
        """Each privilege is accepted on at least one securable (added for bob, removed again).

        Args:
            b: Permission backend.
            privileges: Recorded enum names.
        """
        cat = b.catalog("admin")
        ns = f"inv_{uuid.uuid4().hex[:6]}"
        cat.create_namespace(ns)
        cat.create_table(f"{ns}.t", schema=pa.schema([("id", pa.int64())]))
        api = f"{b.base}/api/2.1/unity-catalog/permissions"
        securables = [
            ("metastore", "metastore"),
            ("catalog", b.catalog_name),
            ("schema", f"{b.catalog_name}.{ns}"),
            ("table", f"{b.catalog_name}.{ns}.t"),
        ]

        def patch(kind, full, key, priv):
            """Add or remove one privilege for bob.

            Args:
                kind: Securable type.
                full: Full name of the securable.
                key: ``add`` or ``remove``.
                priv: Privilege as the API spells it.

            Returns:
                The HTTP status.
            """
            r = requests.patch(
                f"{api}/{kind}/{full}",
                json={"changes": [{"principal": "bob@lab.local", key: [priv]}]},
                headers=b._h(),
                timeout=10,
            )
            return r.status_code

        refused_ = []
        try:
            for enum in privileges:
                priv = enum.replace("_", " ")
                for kind, full in securables:
                    if patch(kind, full, "add", priv) == 200:
                        patch(kind, full, "remove", priv)
                        break
                else:
                    refused_.append(priv)
            assert patch("catalog", b.catalog_name, "add", "NOPE") == 400, (
                "unknown privilege accepted"
            )
        finally:
            Backend.drop_namespace(b, ns)
        assert not refused_, f"recorded privileges no securable accepts: {refused_}"

    @staticmethod
    def rustfs(b, privileges):
        """An IAM policy with all recorded actions is accepted; an unknown action is refused.

        Args:
            b: Permission backend.
            privileges: Recorded actions.
        """
        import json
        import tempfile

        adm = b._admin_client()

        def add(actions):
            """Try a policy with these actions (one statement per action family).

            Args:
                actions: IAM actions.

            Returns:
                None if accepted, else the error text.
            """
            families = sorted({a.split(":")[0] for a in actions})
            statements = [
                {
                    "Effect": "Allow",
                    "Action": [a for a in actions if a.startswith(f + ":")],
                    "Resource": ["arn:aws:s3:::*"],
                }
                for f in families
            ]
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
                json.dump({"Version": "2012-10-17", "Statement": statements}, f)
            try:
                adm.policy_add("lab-inventory-check", f.name)
            except Exception as exc:  # noqa: BLE001 - the refusal is the result
                return str(exc)[:300]
            return None

        try:
            problem = add(privileges)
            assert problem is None, f"the recorded actions are refused: {problem}"
            assert add(["admin:NoSuchAction"]) is not None, "an unknown action was accepted"
        finally:
            try:
                adm.policy_remove("lab-inventory-check")
            except Exception:  # noqa: BLE001, S110 - nothing to remove
                pass

    # =========================================================================== matrix

    @staticmethod
    def gravitino(b, privileges):
        """Grant and revoke each privilege on the metalake to a probe role.

        Args:
            b: Permission backend.
            privileges: Recorded ``Privilege.Name`` constants.
        """
        role = b.role_create(f"inv_{uuid.uuid4().hex[:6]}")
        refused_ = []
        try:
            for priv in privileges:
                try:
                    b._role_privileges(role, "metalake", None, None, [priv])
                    b._role_privileges(role, "metalake", None, None, [priv], revoke=True)
                except AssertionError as exc:
                    refused_.append(f"{priv} ({str(exc)[:120]})")
            typ, full = b._object("metalake")
            r = requests.put(
                f"{b.api}/permissions/roles/{role}/{typ}/{full}/grant",
                json={"privileges": b._privs("NOPE")},
                headers=b._h(),
                timeout=10,
            )
            assert 400 <= r.status_code < 500, f"unknown privilege answered {r.status_code}"
        finally:
            b.role_drop(role)
        assert not refused_, f"recorded privileges the catalog refuses: {refused_}"


@pytest.mark.parametrize("priv", PRIVILEGES)
def test_privilege_allows_its_action(priv, pv):
    """With only this privilege, bob can do its action - and nothing it does not cover.

    Checks:
        * the action answers 2xx (``list_tables``: the listing names table ``t``;
          ``manage_grants``: bob gives alice read on ``t`` and alice then reads it);
        * ``describe``: the data stays closed (no vended keys, or keys that cannot read a data
          file; RustFS: bob's own keys cannot read it);
        * every action in ``NARROW[priv]`` is refused with a permission answer.
        Catalogs without such a privilege: not offered (reason checked by the backend).

    Args:
        priv: Common privilege (see ``authz_backends.PRIVILEGES``).
        pv: ``(backend, namespace with table t, cleanups)``.
    """
    b, ns, cleanups = pv
    if reason := b.privilege_not_offered(priv):
        pytest.skip(reason)
    cleanups.append(b.privilege_grant(priv, ns, "t"))
    status = act(b, "bob", ns, priv)
    with expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, priv=priv, step="allowed")):
        assert 200 <= status < 300, f"{priv} with the {priv} privilege answered {status}"
    if priv == "list_tables":
        s, base = b.rest("bob")
        names = [
            i["name"]
            for i in s.get(f"{base}/namespaces/{ns}/tables", timeout=15).json()["identifiers"]
        ]
        with expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, priv=priv, step="listing")):
            assert "t" in names, f"listing without the table: {names}"
    if priv == "describe":
        opened = data_is_closed(b, "bob", ns)
        assert opened is None, f"describe opened the data: {opened}"
    if priv == "manage_grants":
        n = b.catalog("alice").load_table(f"{ns}.t").scan().to_arrow().num_rows
        assert n == ROWS, f"alice reads {n} rows after bob's grant"
    wrongly = {a: s for a in NARROW[priv] if not refused(s := act(b, "bob", ns, a))}
    assert not wrongly, f"{priv} also allows {wrongly} (status per action)"


@pytest.mark.parametrize("priv", PRIVILEGES)
def test_action_needs_its_privilege(priv, pv):
    """Without the privilege, bob cannot do the action.

    Checks:
        * the action as bob (no grant on the namespace or table) answers with a permission
          refusal (401/403; Lakekeeper 404);
        * ``manage_grants``: alice still cannot read ``t`` afterwards.
        Catalogs without such a privilege: not offered.

    Args:
        priv: Common privilege (see ``authz_backends.PRIVILEGES``).
        pv: ``(backend, namespace with table t, cleanups)``.
    """
    b, ns, _ = pv
    if reason := b.privilege_not_offered(priv):
        pytest.skip(reason)
    status = act(b, "bob", ns, priv)
    assert refused(status), f"{priv} without the privilege answered {status}"
    if priv == "manage_grants":
        with pytest.raises(Exception):  # noqa: B017 - any refusal: alice has no grant
            b.catalog("alice").load_table(f"{ns}.t").scan().to_arrow()
