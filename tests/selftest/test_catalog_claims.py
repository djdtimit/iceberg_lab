"""Lab self-test: no test decides "catalog X has no API" from a list of its own.

Counterpart of ``test_capability_registry.py`` (engines). A catalog offers a capability when its
API description says so (``docs/api/``, ``tests/lab/feature_inventory.py``; the advertised
endpoints are checked live by ``test_feature_inventory.py``). A test that skips a catalog with a
hard-coded name - ``if PROFILE.name != "rustfs": pytest.skip(...)`` - is never re-checked: it
hid that RustFS logs in through Keycloak (identity-provider outage, token lifetime). Here:

* every catalog list a test decides by (``offered_by(...)`` calls, the admin adapters, the
  ``USERS``/``BACKENDS``/``SERVICES``/``UIS`` tables) contains every catalog whose API describes
  the capability;
* every other skip that decides by catalog name alone is listed in :data:`ALLOWED` with the
  reason it is not a claim about the catalog's API.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import ast
from pathlib import Path

import feature_inventory as inv
import pytest

pytestmark = pytest.mark.offline

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
CATALOG_NAMES = set(inv.CATALOGS)

#: Hard-coded catalog skips that are not a claim about the catalog's API, with the reason.
#: ``(file, text in the skip call) -> why`` (a substring, so moving the line does not matter).
ALLOWED = {
    ("tests/conftest.py", "specific to {name}"): (
        "specific_to(): tests of one catalog's special features (marked specific, not compared)"
    ),
    ("tests/catalog/test_catalog_features.py", "non-Iceberg tables (generic-tables"): (
        "asked live just before: Lakekeeper's generic-tables create answered 404/405/501"
    ),
    ("tests/security/test_security.py", "has no API for token authentication"): (
        "TOKEN_CLIENTS: catalogs that check tokens; the reference fixture has no authentication"
    ),
    ("tests/security/test_security.py", "identity-provider STS"): (
        "STS AssumeRoleWithWebIdentity is RustFS' (S3) login; the others have no S3 STS"
    ),
    ("tests/security/test_security.py", "SigV4 request signing (bearer tokens)"): (
        "SigV4-signed catalog requests are RustFS' login; the others take bearer tokens"
    ),
    ("tests/security/test_security.py", "has no API for authentication (open catalog)"): (
        "every catalog with a login is listed; the reference fixture has none"
    ),
    ("tests/storage/test_storage_faults.py", "not testable on"): (
        "lab setup: the fault proxy needs a warehouse whose S3 endpoint can be set at runtime"
    ),
}

#: Catalog lists in the tests -> the capability whose offering catalogs they must contain.
#: ``(file, name) -> capability``; ``name`` is a top-level dict (its keys) or ``ADMIN.<method>``
#: (the catalogs whose admin adapter class defines the method).
LISTS = {
    ("tests/security/test_ui.py", "UIS"): "web_ui",
    ("tests/security/test_privileges.py", "SERVICES"): "grants",
    ("tests/lab/authz_backends.py", "BACKENDS"): "users",
    ("tests/catalog/test_catalog_admin.py", "USERS"): "users",
    ("tests/catalog/test_catalog_admin.py", "ADMIN.create"): "catalog_lifecycle",
    ("tests/catalog/test_catalog_admin.py", "ADMIN.update"): "catalog_update",
    ("tests/catalog/test_catalog_admin.py", "ADMIN.deactivate"): "catalog_deactivate",
    ("tests/catalog/test_catalog_admin.py", "ADMIN.storage"): "storage_config",
    ("tests/catalog/test_catalog_admin.py", "ADMIN.server_info"): "server_info",
}

#: Names in a skip condition that stand for a list checked through :data:`LISTS` or
#: :func:`test_offered_by_names_every_describing_catalog`.
CHECKED_NAMES = {"UIS", "SERVICES", "BACKENDS", "USERS", "catalogs"}


def _is_skip(node) -> bool:
    """Whether ``node`` is ``pytest.skip(...)`` or the admin tests' ``no_api(...)``.

    Args:
        node: AST node.

    Returns:
        True for a skip call.
    """
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    if isinstance(f, ast.Name):
        return f.id == "no_api"
    return (
        isinstance(f, ast.Attribute) and f.attr == "skip" and getattr(f.value, "id", "") == "pytest"
    )


def _by_name_only(test) -> bool:
    """Whether an ``if`` condition decides by the catalog name alone (no live check).

    Args:
        test: AST of the condition.

    Returns:
        True if it reads ``PROFILE.name`` (or a variable ``cat``/``catalog``) and calls nothing.
    """
    nodes = list(ast.walk(test))
    if any(isinstance(n, ast.Call) for n in nodes):
        return False
    return any(
        (
            isinstance(n, ast.Attribute)
            and n.attr == "name"
            and getattr(n.value, "id", "") == "PROFILE"
        )
        or (isinstance(n, ast.Name) and n.id in ("cat", "catalog"))
        for n in nodes
    )


def catalog_skips(file: Path) -> list[tuple[int, str, str]]:
    """Skips in ``file`` under an ``if``/``else`` that decides by catalog name alone.

    Args:
        file: Python source file.

    Returns:
        ``(line, condition, skip call)`` per finding.
    """
    src = file.read_text()
    found = []

    def visit(node, decider) -> None:
        """Walk the tree, remembering the nearest enclosing ``if`` of each statement.

        Args:
            node: AST node.
            decider: Nearest enclosing ``ast.If`` (or None).
        """
        if _is_skip(node) and decider is not None and _by_name_only(decider.test):
            cond = ast.get_source_segment(src, decider.test) or ""
            found.append((node.lineno, cond, ast.get_source_segment(src, node) or ""))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            decider = None  # a function's own body is not decided by an outer if
        for child in ast.iter_child_nodes(node):
            inner = node if isinstance(node, ast.If) and child is not node.test else decider
            visit(child, inner)

    visit(ast.parse(src), None)
    return sorted(set(found))


def test_no_catalog_skips_outside_the_inventory():
    """No test skips a catalog by a name list nobody checks.

    Checks:
        * every skip that decides by catalog name alone either decides by a list checked
          against the feature inventory (:data:`CHECKED_NAMES`) or is in :data:`ALLOWED`.
    """
    bad = []
    for file in sorted(TESTS.rglob("*.py")):
        rel = file.relative_to(ROOT).as_posix()
        if rel.startswith("tests/selftest/"):
            continue
        for line, cond, call in catalog_skips(file):
            names = {
                n.id for n in ast.walk(ast.parse(cond, mode="eval")) if isinstance(n, ast.Name)
            }
            listed = {**ALLOWED, **BRANCHES}
            if names & CHECKED_NAMES or any(rel == f and s in call for f, s in listed):
                continue
            bad.append(f"{rel}:{line}: if {cond}: {call.splitlines()[0]}")
    assert not bad, (
        "catalog skips by a hard-coded name - decide by the feature inventory (a list in LISTS, "
        "offered_by) or a live check, or add the reason to ALLOWED:\n" + "\n".join(bad)
    )


def _module(rel: str) -> ast.Module:
    """Parsed source of a repo file.

    Args:
        rel: Repo-relative path.

    Returns:
        The module AST.
    """
    return ast.parse((ROOT / rel).read_text())


def _defines(classes: dict[str, ast.ClassDef], cls: str, method: str) -> bool:
    """Whether a class or one of its bases in the same file defines a method.

    Args:
        classes: Classes of the file by name.
        cls: Class name.
        method: Method name.

    Returns:
        True if defined.
    """
    node = classes[cls]
    own = {f.name for f in node.body if isinstance(f, ast.FunctionDef)}
    bases = [b.id for b in node.bases if isinstance(b, ast.Name) and b.id in classes]
    return method in own or any(_defines(classes, b, method) for b in bases)


def _listed(rel: str, name: str) -> set[str]:
    """Catalogs a test list names.

    Args:
        rel: Repo-relative file.
        name: Top-level dict name, or ``ADMIN.<method>``.

    Returns:
        The catalog names.
    """
    tree = _module(rel)
    table, _, method = name.partition(".")
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", "") == table for t in node.targets
        ):
            d = node.value
            while isinstance(d, (ast.Call, ast.Subscript)):  # {...}[PROFILE.name]()
                d = d.func if isinstance(d, ast.Call) else d.value
            assert isinstance(d, ast.Dict), f"{rel}: {table} is not a dict"
            keys = {k.value: v for k, v in zip(d.keys, d.values, strict=True)}
            if not method:
                return set(keys)
            classes = {c.name: c for c in tree.body if isinstance(c, ast.ClassDef)}
            return {
                cat
                for cat, v in keys.items()
                if isinstance(v, ast.Name) and _defines(classes, v.id, method)
            }
    raise AssertionError(f"{rel}: no top-level {table}")


@pytest.mark.parametrize(
    ("source", "capability"), LISTS.items(), ids=[f"{f}::{n}" for f, n in LISTS]
)
def test_lists_name_every_describing_catalog(source, capability):
    """A catalog list a test decides by contains every catalog whose API describes it.

    Checks:
        * the inventory's catalogs for the capability are all in the list (a missing one would
          be skipped as "no API" although its API has it).

    Args:
        source: ``(file, name)`` of the list.
        capability: Capability key in the feature inventory.
    """
    listed = _listed(*source)
    describing = set(inv.offered_by(inv.CAPABILITIES[capability]))
    assert describing <= listed, (
        f"{source[1]} in {source[0]} leaves out {sorted(describing - listed)}, whose API "
        f"describes '{capability}' - the test would skip it as 'no API'"
    )


def _offered_by_calls() -> list[tuple[str, int, set[str]]]:
    """``offered_by(...)`` calls of test_catalog_features: test function, line, catalogs.

    Returns:
        ``(function, line, catalogs)`` per call.
    """
    rel = "tests/catalog/test_catalog_features.py"
    out = []
    for fn in _module(rel).body:
        if isinstance(fn, ast.FunctionDef):
            for call in ast.walk(fn):
                if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "offered_by":
                    cats = {a.value for a in call.args if isinstance(a, ast.Constant)}
                    out.append((fn.name, call.lineno, cats))
    return out


def test_offered_by_names_every_describing_catalog():
    """Every catalog whose API describes a capability runs one of its evidence tests.

    Checks:
        * per inventory capability, the catalogs its evidence tests in test_catalog_features
          run on (all catalogs for a test without ``offered_by``, else the named ones) - together
          with those of evidence tests elsewhere (all catalogs) - include every describing
          catalog.
    """
    gated: dict[str, set[str]] = {}
    for fn, _, cats in _offered_by_calls():
        gated.setdefault(fn, set()).update(cats)
    bad = []
    for name, cap in inv.CAPABILITIES.items():
        covered: set[str] = set()
        for test in cap["tests"]:
            module, _, fn = test.partition("::")
            covered |= (
                gated.get(fn, CATALOG_NAMES) if module == "test_catalog_features" else CATALOG_NAMES
            )
        missing = set(inv.offered_by(cap)) - covered
        if missing and cap["tests"]:
            bad.append(f"'{name}': no evidence test runs on {sorted(missing)}")
    assert not bad, "offered_by leaves out catalogs whose API describes it:\n" + "\n".join(bad)


#: Name chains (``if lakekeeper ... elif rustfs ... else: no_api``): the catalogs the chain
#: implements must include every catalog whose API describes the capability.
#: ``(file, text in the skip call) -> (capability, catalogs with a branch)``
BRANCHES = {
    ("tests/catalog/test_catalog_admin.py", "background tasks of the catalog"): (
        "background_tasks",
        {"lakekeeper", "rustfs"},
    ),
}


@pytest.mark.parametrize(("source", "claim"), BRANCHES.items(), ids=[s for _, s in BRANCHES])
def test_branches_cover_every_describing_catalog(source, claim):
    """A name chain has a branch for every catalog whose API describes the capability.

    Checks:
        * the chain's catalogs include the inventory's; the chain's skip is in the file.

    Args:
        source: ``(file, skip text)``.
        claim: ``(capability, catalogs with a branch)``.
    """
    capability, branches = claim
    assert source[1] in (ROOT / source[0]).read_text(), f"{source} not found"
    missing = set(inv.offered_by(inv.CAPABILITIES[capability])) - branches
    assert not missing, f"{source[0]}: no branch for {sorted(missing)} ('{capability}')"


def test_the_finder_finds_a_hard_coded_catalog_skip(tmp_path):
    """The source scan of :func:`test_no_catalog_skips_outside_the_inventory` works.

    Checks:
        * a skip under ``if PROFILE.name != 'rustfs'`` and one in the ``else`` of a name chain
          are found; a skip after a live check (a call in the condition) is not.

    Args:
        tmp_path: Temporary directory (pytest fixture).
    """
    f = tmp_path / "t.py"
    f.write_text(
        "import pytest\n"
        "def test_x(rest):\n"
        "    if PROFILE.name != 'rustfs':\n"
        "        pytest.skip('no API for x')\n"
        "    if PROFILE.name == 'lakekeeper':\n"
        "        pass\n"
        "    else:\n"
        "        no_api('y')\n"
        "    if not rest.advertises('z'):\n"
        "        pytest.skip('no API for z')\n"
    )
    assert [line for line, _, _ in catalog_skips(f)] == [4, 8]
