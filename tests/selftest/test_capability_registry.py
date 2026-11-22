"""Lab self-test: the capability registry is complete, and no test keeps its own engine list.

Every "not offered" skip of an engine goes through ``iceberg_lab.lab.capabilities.require``: the
registry (``OFFERED``) says who offers a capability, and every other engine's claim is either
checked against its live API (``CAPABILITY_API``) or proven by a test of its own (``PROVEN_BY``).
A hard-coded ``if engine == "pg_lake": pytest.skip(...)`` would hide a capability an engine
gains with an update - this test finds such skips in the test sources.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from iceberg_lab.lab.capabilities import (
    _ALL,
    CAPABILITY_API,
    NOT_OFFERED_EXEMPT,
    OFFERED,
    PROVEN_BY,
)

pytestmark = pytest.mark.offline

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"

#: Engine-level skips that are not a registry claim, with the reason. ``(file, line text) -> why``
#: (matched by a substring of the skip call, so moving the line does not break it).
ALLOWED_SKIPS = {
    ("tests/engines/test_concurrency.py", "flink has no API for MERGE (refused"): (
        "Flink was asked: the statement ran and Flink refused it; the test fails once Flink accepts it"
    ),
}


def test_registry_names_only_lab_engines():
    """Every engine in the registry is one of the lab's engines.

    Checks:
        * the registry's engine set equals ``conftest.ENGINES``;
        * OFFERED, PROVEN_BY and NOT_OFFERED_EXEMPT name no other engine.
    """
    from conftest import ENGINES

    assert set(ENGINES) == _ALL
    for capability, engines in OFFERED.items():
        assert engines <= _ALL, f"{capability}: unknown engines {sorted(engines - _ALL)}"
    for engine, capability in [*PROVEN_BY, *NOT_OFFERED_EXEMPT]:
        assert engine in _ALL, f"unknown engine {engine!r} ({capability})"


def test_every_missing_capability_is_checked_or_proven():
    """An engine that does not offer a capability has an API check or a proving test - so
    ``require`` never skips on an unchecked claim.

    Checks:
        * every (engine, capability) missing from OFFERED is in PROVEN_BY or CAPABILITY_API.
    """
    missing = [
        (e, c)
        for c, engines in OFFERED.items()
        for e in sorted(_ALL - engines)
        if (e, c) not in PROVEN_BY and c not in CAPABILITY_API
    ]
    assert not missing, f"neither CAPABILITY_API nor PROVEN_BY: {missing}"


def test_proven_claims_are_not_also_offered():
    """A proven "not offered" contradicts the registry if the engine is listed as offering it.

    Checks:
        * no PROVEN_BY entry names an engine that OFFERED lists for the capability.
    """
    both = [(e, c) for e, c in PROVEN_BY if e in OFFERED[c]]
    assert not both, f"offered and proven not offered: {both}"


@pytest.mark.parametrize("key", sorted(PROVEN_BY), ids=lambda k: f"{k[0]}-{k[1]}")
def test_proving_test_exists(key):
    """The proving test of a ``PROVEN_BY`` entry exists (file and function).

    Checks:
        * the file exists and defines the test function.

    Args:
        key: ``(engine, capability)`` of the PROVEN_BY entry.
    """
    ref = PROVEN_BY[key][1].split(" ")[0]
    path, func = ref.split("::")
    func = func.split("[")[0]
    file = ROOT / path
    assert file.is_file(), f"{key}: {path} does not exist"
    assert re.search(rf"^def {func}\(", file.read_text(), re.MULTILINE), (
        f"{key}: no {func} in {path}"
    )


@pytest.mark.parametrize("capability", sorted({c for _, c in NOT_OFFERED_EXEMPT}))
def test_exemptions_belong_to_known_capabilities(capability):
    """An exemption names a capability that ``not_offered`` checks.

    Checks:
        * the capability is a key of CAPABILITY_API.

    Args:
        capability: Capability of a NOT_OFFERED_EXEMPT entry.
    """
    assert capability in CAPABILITY_API


def _engine_skips(file: Path) -> list[tuple[int, str]]:
    """``pytest.skip`` calls in ``file`` that decide by engine name instead of the registry.

    Found: a skip directly inside ``if <x> == "<engine>"`` / ``if <x> in (<engines>)``, and a
    skip whose message starts with an engine name followed by "has no API" or "not testable".

    Args:
        file: Python source file.

    Returns:
        ``(line, source of the skip call)`` per finding.
    """
    src = file.read_text()
    tree = ast.parse(src)
    found: list[tuple[int, str]] = []

    def is_skip(node) -> bool:
        """Whether ``node`` is a ``pytest.skip(...)`` call.

        Args:
            node: AST node.

        Returns:
            True for a skip call.
        """
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "skip"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "pytest"
        )

    def names_engine(test) -> bool:
        """Whether an ``if`` condition compares with an engine name.

        Args:
            test: AST of the condition.

        Returns:
            True if a string constant in it is an engine name.
        """
        consts = [n.value for n in ast.walk(test) if isinstance(n, ast.Constant)]
        return any(c in _ALL for c in consts if isinstance(c, str))

    for node in ast.walk(tree):
        if isinstance(node, ast.If) and names_engine(node.test):
            for stmt in node.body:
                for call in ast.walk(stmt):
                    if is_skip(call):
                        found.append((call.lineno, ast.get_source_segment(src, call) or ""))
        if is_skip(node) and node.args:
            text = ast.get_source_segment(src, node.args[0]) or ""
            engines = "|".join(sorted(_ALL))
            if re.match(rf"""f?["'](\{{\w+\}}|{engines}) (has no API|not testable)""", text):
                found.append((node.lineno, ast.get_source_segment(src, node) or ""))
    return sorted(set(found))


def test_no_engine_skips_outside_the_registry():
    """No test skips an engine by name: such a skip is never re-checked, so a capability an
    engine gains would stay invisible. Use ``require(engine, capability, engine_pool)``.

    Checks:
        * no skip under an ``if`` on an engine name, and no skip message "<engine> has no API" /
          "<engine> not testable" outside the selftests (except :data:`ALLOWED_SKIPS`).
    """
    bad = []
    for file in sorted(TESTS.rglob("*.py")):
        rel = file.relative_to(ROOT).as_posix()
        if rel.startswith("tests/selftest/"):
            continue
        for line, call in _engine_skips(file):
            if any(rel == f and s in call for f, s in ALLOWED_SKIPS):
                continue
            bad.append(f"{rel}:{line}: {call.splitlines()[0]}")
    assert not bad, "engine skips outside the registry:\n" + "\n".join(bad)


def test_the_finder_finds_a_hard_coded_skip(tmp_path):
    """The source scan of :func:`test_no_engine_skips_outside_the_registry` works.

    Checks:
        * a skip under ``if writer == 'pg_lake'`` and a "{writer} has no API" skip are found;
        * a catalog skip is not.

    Args:
        tmp_path: Temporary directory (pytest fixture).
    """
    f = tmp_path / "t.py"
    f.write_text(
        "import pytest\n"
        "def test_x(writer):\n"
        "    if writer == 'pg_lake':\n"
        "        pytest.skip('it writes only tables it created')\n"
        "    pytest.skip(f'{writer} has no API for x')\n"
        "    pytest.skip('catalog has no API for y')\n"
    )
    assert [line for line, _ in _engine_skips(f)] == [4, 5]
