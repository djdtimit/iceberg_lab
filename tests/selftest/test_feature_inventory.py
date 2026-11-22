"""The feature inventory (tests/lab/feature_inventory.py) is complete and current.

* every endpoint of every catalog (docs/api/) belongs to exactly one capability;
* every test named as evidence exists;
* every capability has tests - or a written reason why none is needed;
* every ◐ (described but not implemented) is backed by a known gap in its test;
* docs/feature-inventory.md is generated from the current inventory;
* a base capability is proven by a base test, and a test that proves only special features of
  one catalog is marked ``specific`` (so it stays out of the comparison);
* the endpoints the catalog advertises in /v1/config are exactly the recorded ones
  (docs/api/advertised-<catalog>.txt) - a new one after an update is a new capability to
  classify (inventory, tests), a missing one a capability the catalog dropped.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import feature_inventory as inv
import pytest

ROOT = Path(__file__).resolve().parents[2]


def _source(stem: str) -> str:
    """Source of a test module by name (tests live in subfolders of tests/).

    Args:
        stem: Module name, e.g. ``test_authz``.

    Returns:
        The file's text.
    """
    (path,) = (ROOT / "tests").rglob(f"{stem}.py")
    return path.read_text()


@pytest.mark.parametrize("catalog", inv.CATALOGS)
def test_every_endpoint_has_exactly_one_capability(catalog):
    """Every endpoint in the catalog's API description belongs to exactly one capability.

    Checks:
        * no endpoint line of ``docs/api/<catalog>-<version>.txt`` is unclassified;
        * none matches two capabilities.

    Args:
        catalog: Catalog whose API description is checked.
    """
    bad = inv.unmapped(catalog)
    assert not bad, "\n".join(
        f"{e} -> {hits or 'no capability'}" for e, hits in sorted(bad.items())
    )


def test_evidence_tests_exist():
    """Every test named as evidence in the inventory exists.

    Checks:
        * each ``file::test`` in a capability's ``tests`` is a test function of that file.
    """
    missing = []
    for name, cap in inv.CAPABILITIES.items():
        for t in cap["tests"]:
            file, func = t.split("::")
            src = _source(file)
            if not re.search(rf"^def {re.escape(func)}\(", src, re.MULTILINE):
                missing.append(f"{name}: {t}")
    assert not missing, "\n".join(missing)


@pytest.mark.parametrize("name", list(inv.CAPABILITIES))
def test_capability_is_tested(name):
    """Every capability that a catalog offers has a test (or a written reason why not).

    Checks:
        * ``tests`` is not empty, or ``untested_reason`` explains the gap.

    Args:
        name: Capability of the inventory.
    """
    cap = inv.CAPABILITIES[name]
    assert cap["tests"] or cap.get("untested_reason"), (
        f"capability '{cap['title']}' ({inv.kind(name, cap)}, offered by "
        f"{', '.join(inv.offered_by(cap))}) has no test yet"
    )


def test_not_implemented_is_proven():
    """Every ◐ (described in the API, not implemented) is proven by a known gap.

    Checks:
        * the ◐ catalog offers the capability at all;
        * the named test documents a known gap whose reason contains the given text - so the
          mark disappears together with the gap.
    """
    bad = []
    for name, cap in inv.CAPABILITIES.items():
        for cat, m in cap.get("not_implemented", {}).items():
            file, func = m["test"].split("::")
            src = _source(file)
            if cat not in inv.offered_by(cap):
                bad.append(f"{name}: {cat} does not offer the capability at all")
            if not re.search(rf"^def {re.escape(func)}\(", src, re.MULTILINE):
                bad.append(f"{name}: Test {m['test']} fehlt")
            if m["gap"] not in src:
                bad.append(f"{name}: known gap '{m['gap']}' is not in {file}.py")
    assert not bad, "\n".join(bad)


def test_overview_is_current():
    """``docs/feature-inventory.md`` is generated from the current inventory.

    Checks:
        * the file exists and equals the freshly rendered overview.
    """
    path = ROOT / "docs" / "feature-inventory.md"
    assert path.exists() and path.read_text() == inv.render(), (
        "docs/feature-inventory.md ist veraltet: cd tests && uv run python -m feature_inventory"
    )


@pytest.mark.parametrize(
    "name", [n for n, c in inv.CAPABILITIES.items() if inv.kind(n, c) == "Base"]
)
def test_base_capability_has_a_base_test(name):
    """A base capability is proven by a test that runs on every catalog.

    Checks:
        * at least one evidence test is a base test (not marked catalog-specific), unless the
          capability has no test yet.

    Args:
        name: Capability of the inventory.
    """
    cap = inv.CAPABILITIES[name]
    base = []
    for t in cap["tests"]:
        file, func = t.split("::")
        src = _source(file)
        if not re.search(rf"@pytest\.mark\.specific\s*\ndef {re.escape(func)}\(", src):
            base.append(t)
    assert base or not cap["tests"], (
        f"base capability '{cap['title']}' is only proven by special-feature tests "
        f"({', '.join(cap['tests'])}) - needs a test for all catalogs"
    )


def _advertised() -> list[str]:
    """Endpoints the catalog advertises in ``/v1/config``.

    Returns:
        The endpoints.
    """
    import requests

    from iceberg_lab.config import SETTINGS, WAREHOUSE_STS, catalog_auth, catalog_headers

    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=20,
    ).json()
    return sorted(" ".join(e.split()) for e in cfg.get("endpoints") or [])


def test_advertised_endpoints_are_recorded():
    """New features show up: the catalog advertises exactly the recorded endpoints.

    Checks:
        * the ``endpoints`` list of the live ``/v1/config`` equals
          ``docs/api/advertised-<catalog>.txt`` - nothing new, nothing gone.
    """
    from iceberg_lab.config import PROFILE

    path = ROOT / "docs" / "api" / f"advertised-{PROFILE.name}.txt"
    live = _advertised()
    if os.getenv("RECORD_ADVERTISED") == "1":  # after classifying the change
        path.write_text("\n".join(live) + "\n")
    recorded = path.read_text().split("\n") if path.exists() else []
    recorded = [e for e in recorded if e]
    new, gone = sorted(set(live) - set(recorded)), sorted(set(recorded) - set(live))
    assert not new and not gone, (
        f"{PROFILE.name} /v1/config changed - classify it (feature_inventory.py, tests), then "
        f"RECORD_ADVERTISED=1 pytest -k advertised:\n  new: {new}\n  gone: {gone}"
    )


def test_specific_only_evidence_is_marked_specific():
    """A test that proves only special features of one catalog is marked ``specific``.

    Checks:
        * every evidence test whose capabilities are all "Specific" carries
          ``@pytest.mark.specific`` - else it would count in the comparison of the catalogs,
          where every other catalog gets "not offered" for it.
    """
    import ast

    proves: dict[str, set[str]] = {}
    for name, cap in inv.CAPABILITIES.items():
        for t in cap["tests"]:
            proves.setdefault(t, set()).add(inv.kind(name, cap))
    marked = {}
    for path in (ROOT / "tests").rglob("test_*.py"):
        tree = ast.parse(path.read_text())
        for n in tree.body:
            if isinstance(n, ast.FunctionDef):
                specific = any("specific" in ast.unparse(d) for d in n.decorator_list)
                marked[f"{path.stem}::{n.name}"] = specific
    wrong = sorted(
        t for t, kinds in proves.items() if kinds == {"Specific"} and not marked.get(t, True)
    )
    assert not wrong, f"special-feature tests without @pytest.mark.specific: {wrong}"
