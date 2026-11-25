"""Roles and inherited grants - the same scenarios on every catalog (base).

test_authz.py grants single users rights on single tables. Here the layer above that:

``test_role_grant_reaches_every_member``
    a read grant on a ROLE reaches every member (alice and bob), and only for that table;
``test_removed_member_loses_access``
    taking bob out of the role closes the table for him at once - alice keeps it;
``test_roles_add_up``
    alice in two roles reads both roles' tables; bob in one of them only that role's table;
``test_namespace_grant_covers_its_tables``
    a read grant on a namespace covers its tables - also one created after the grant - and
    no table of another namespace;
``test_idp_role_is_a_catalog_role``
    a role that only the identity provider assigns (in the Keycloak token) opens exactly its
    table - the catalog knows the role, nobody is made a member in the catalog.

Roles per catalog (tests/lab/authz_backends.py): Lakekeeper roles with assignee members, Polaris
principal roles bound to catalog roles, RustFS IAM groups with a group policy. Unity Catalog
OSS has no roles or groups and RustFS no namespace-level data rights - both "not offered",
with the claim checked live on every run (the backends assert it still holds).

Every refusal is checked against the admin's view first (the table exists with its rows), and
only a permission error counts (test_authz.DENIED), never an arbitrary failure.
"""

from __future__ import annotations

import re
import uuid

import pyarrow as pa
import pytest
from authz_backends import ROWS, Backend, backend
from test_authz import DENIED

from iceberg_lab.config import PROFILE

from conftest import expect, find_gap, gap_table

# Filled from observed results only: (catalog, case, regex, cause).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "case"), [
    ("lakekeeper", "idp_role", r"the identity provider's role does not open its table: .*(NoSuch\w+|does not exist|403|Forbidden)", "lakekeeper-openfga-ignores-token-roles"),
    ("polaris", "idp_role", r"the identity provider's role does not open its table: .*activated PrincipalRoles '\[\]'", "polaris-idp-roles-narrow-only"),
    # Gravitino in front of Polaris: Polaris decides again by its own grants (the refusal is
    # in the message, see _refusal).
    *(("gravitino_polaris", c, r"(Principal '\w+' with activated PrincipalRoles .* is not authorized|NotAuthorizedException: Not authorized)", "gravitino-rest-backend-grants-not-forwarded")
      for c in ("member_reads", "removed_member", "roles_add_up", "namespace_grant", "idp_role")),
])
# fmt: on


def _gap(case: str):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, case=case))


@pytest.fixture(scope="module")
def module_rb():
    """Backend and two namespaces (A: t1, t2; B: t1) - no grants.

    Yields:
        ``(backend, namespace A, namespace B)``.
    """
    b = backend()
    b.ensure()
    cat = b.catalog("admin")
    a, other = (f"rbac_{x}_{uuid.uuid4().hex[:6]}" for x in ("a", "b"))
    for ns, tables in ((a, ("t1", "t2")), (other, ("t1",))):
        cat.create_namespace(ns)
        for name in tables:
            _table(cat, ns, name)
    yield b, a, other
    for r in getattr(b, "namespace_roles", []):
        b.role_drop(r)
    for ns in (a, other):
        Backend.drop_namespace(b, ns)  # only here (Lakekeeper's also clears its 2nd warehouse)


@pytest.fixture
def rb(module_rb):
    """Per test: the shared namespaces, and every role the test made is dropped afterwards -
    so no test reads through a role another test left behind.

    Args:
        module_rb: The module's backend and namespaces.

    Yields:
        ``(backend, A, B, roles)``.
    """
    roles: list = []
    yield (*module_rb, roles)
    for r in roles:
        module_rb[0].role_drop(r)


def _table(cat, ns, name):
    """Create a table with ids 1..ROWS.

    Args:
        cat: PyIceberg catalog.
        ns: Namespace.
        name: Name of the table / object.
    """
    t = cat.create_table(f"{ns}.{name}", schema=pa.schema([("id", pa.int64())]))
    t.append(pa.table({"id": list(range(1, ROWS + 1))}))


#: The last refusal :func:`reads` saw, for the assertion messages (:func:`_refusal`).
_LAST_REFUSAL = [""]


def _refusal() -> str:
    """The last refusal :func:`reads` saw.

    Returns:
        ``"<ExceptionType>: <message>"`` (shortened).
    """
    return _LAST_REFUSAL[0]


def reads(b, user, ns, table) -> bool:
    """``user`` reads the table's rows (True) or is refused with a permission error (False).

    Args:
        b: Permission backend (``authz_backends``).
        user: Lab user (``alice``, ``bob`` or ``admin``).
        ns: Namespace.
        table: Table name (without namespace).

    Returns:
        True if it reads, False if refused.
    """
    assert b.rows(ns, table) == ROWS, "setup: the admin does not see the table"
    try:
        n = b.catalog(user).load_table(f"{ns}.{table}").scan().to_arrow().num_rows
    except Exception as exc:
        if not re.search(DENIED, f"{type(exc).__name__}: {exc}"):
            raise
        _LAST_REFUSAL[0] = f"{type(exc).__name__}: {exc}"[:300]
        return False
    assert n == ROWS, f"{user} read {n} rows of {ns}.{table}"
    return True


def _roles(rb, *names):
    """Create roles for the test (registered for cleanup); skip where the catalog has none.

    Args:
        rb: The test's rb fixture value.
        names: Role name stems.

    Returns:
        The role handles.
    """
    b, *_, roles = rb
    if reason := b.roles_not_offered():
        pytest.skip(reason)
    made = [b.role_create(f"rbac_{n}_{uuid.uuid4().hex[:6]}") for n in names]
    roles += made
    return made


def test_role_grant_reaches_every_member(rb):
    """A read grant on a role reaches every member - and only for that table.

    Checks:
        * alice and bob (both members) read A.t1;
        * bob is refused on A.t2 (not granted to the role).
        Unity (no roles or groups, checked live): not offered.

    Args:
        rb: ``(backend, namespace A with t1/t2, namespace B with t1, roles made by this test)`` -
            the test's roles are dropped afterwards.
    """
    b, a, _, _ = rb
    (role,) = _roles(rb, "r")
    for user in ("alice", "bob"):
        b.role_member(role, user)
    b.role_grant_table(role, a, "t1")
    with _gap("member_reads"):
        assert reads(b, "alice", a, "t1") and reads(b, "bob", a, "t1"), (
            f"a member cannot read: {_refusal()}"
        )
    assert not reads(b, "bob", a, "t2"), "the role's grant opened another table"


def test_removed_member_loses_access(rb):
    """Taking bob out of the role closes the table for him at once; alice keeps it.

    Checks:
        * bob reads A.t1 through the role (setup);
        * right after leaving the role he is refused;
        * alice still reads it.
        Unity: not offered.

    Args:
        rb: ``(backend, namespace A with t1/t2, namespace B with t1, roles made by this test)`` -
            the test's roles are dropped afterwards.
    """
    b, a, _, _ = rb
    (role,) = _roles(rb, "rm")
    for user in ("alice", "bob"):
        b.role_member(role, user)
    b.role_grant_table(role, a, "t1")
    with _gap("removed_member"):
        assert reads(b, "bob", a, "t1"), f"setup: bob cannot read through the role: {_refusal()}"
    b.role_member(role, "bob", remove=True)
    assert not reads(b, "bob", a, "t1"), "bob still reads after leaving the role"
    assert reads(b, "alice", a, "t1"), "alice lost access when bob left the role"


def test_roles_add_up(rb):
    """Grants of several roles add up, without leaking to non-members.

    Checks:
        * alice (in R1 and R2) reads A.t1 and A.t2;
        * bob (in R1 only) reads A.t1 and is refused on A.t2.
        Unity: not offered.

    Args:
        rb: ``(backend, namespace A with t1/t2, namespace B with t1, roles made by this test)`` -
            the test's roles are dropped afterwards.
    """
    b, a, _, _ = rb
    r1, r2 = _roles(rb, "r1", "r2")
    b.role_member(r1, "alice")
    b.role_member(r2, "alice")
    b.role_member(r1, "bob")
    b.role_grant_table(r1, a, "t1")
    b.role_grant_table(r2, a, "t2")
    with _gap("roles_add_up"):
        assert reads(b, "alice", a, "t1") and reads(b, "alice", a, "t2"), (
            f"roles do not add up: {_refusal()}"
        )
    assert reads(b, "bob", a, "t1"), "bob cannot read his role's table"
    assert not reads(b, "bob", a, "t2"), "bob reads a table of a role he is not in"


def test_namespace_grant_covers_its_tables(rb):
    """A read grant on a namespace covers its tables - also later ones - and no other namespace.

    Checks:
        * bob reads A.t1;
        * bob reads A.t_later, created after the grant;
        * bob is refused on B.t1.
        RustFS (table data outside the namespace, checked live): not offered.

    Args:
        rb: ``(backend, namespace A with t1/t2, namespace B with t1, roles made by this test)`` -
            the test's roles are dropped afterwards.
    """
    b, a, other, _ = rb
    if reason := b.namespace_grant_not_offered(a):
        pytest.skip(reason)
    b.grant_namespace(a, "bob")
    try:
        _table(b.catalog("admin"), a, "t_later")  # created AFTER the grant
        with _gap("namespace_grant"):
            assert reads(b, "bob", a, "t1"), (
                f"the namespace grant does not cover its table: {_refusal()}"
            )
        assert reads(b, "bob", a, "t_later"), "the grant does not cover a table created later"
        assert not reads(b, "bob", other, "t1"), "the grant opened a table of another namespace"
    finally:
        # Where the grant is a role, drop it now: later tests on A must not read through it.
        for r in getattr(b, "namespace_roles", []):
            b.role_drop(r)
        b.namespace_roles = []


def test_idp_role_is_a_catalog_role(rb):
    """A role that only the identity provider assigns opens exactly its table.

    Setup: the catalog knows the role (Lakekeeper: role ``oidc~lab-readers``; Polaris: principal
    role ``lab_readers``; RustFS: IAM policy ``rbac-idp-readers``) and grants it read on A.t1 -
    but no user is made a member in the catalog. The Keycloak client of the test puts the role
    into its token (Lakekeeper: claim ``lab_roles``; Polaris: scope ``polaris-role-lab_readers``;
    RustFS: claim ``rustfs_policy``, exchanged via STS AssumeRoleWithWebIdentity).

    Checks:
        * that identity reads A.t1 - the token alone made it a member;
        * it is refused on A.t2 - the token role grants exactly the role's table;
        * bob (no such role in his token) is refused on A.t1 - the grant is not public.

    Args:
        rb: ``(backend, namespace A with t1/t2, namespace B with t1, roles made by this test)`` -
            the test's roles are dropped afterwards.
    """
    b, a, _, _ = rb
    if reason := b.roles_not_offered():
        pytest.skip(reason)
    # The token-role clients are not in the realm import (authz/keycloak/realm.json): create
    # them here, so the test does not depend on test_security having run first on a fresh
    # Keycloak (idempotent).
    from iceberg_lab import keycloak as kc

    kc.ensure_service_scopes()
    try:
        with _gap("idp_role"):
            b.idp_role_grant(a, "t1")
            conn = b.idp_role_conn()
            ok, refusal = reads_as(b, conn, a, "t1")
            assert ok, f"the identity provider's role does not open its table: {refusal}"
            assert not reads_as(b, conn, a, "t2")[0], "the token role opened another table"
            assert not reads(b, "bob", a, "t1"), "the role's table is open without the role"
    finally:
        b.idp_role_cleanup()


def reads_as(b, conn, ns, table) -> tuple[bool, str]:
    """Like :func:`reads`, for a connection instead of a lab user; also returns the refusal.

    Args:
        b: Permission backend (``authz_backends``).
        conn: Connection of the identity (``authz_backends.Conn``).
        ns: Namespace.
        table: Table name (without namespace).

    Returns:
        ``(read?, refusal text)``.
    """
    from authz_backends import pyiceberg_for

    assert b.rows(ns, table) == ROWS, "setup: the admin does not see the table"
    try:
        n = pyiceberg_for(conn).load_table(f"{ns}.{table}").scan().to_arrow().num_rows
    except Exception as exc:
        if not re.search(DENIED, f"{type(exc).__name__}: {exc}"):
            raise
        return False, f"{type(exc).__name__}: {exc}"
    assert n == ROWS, f"read {n} rows of {ns}.{table}"
    return True, ""
