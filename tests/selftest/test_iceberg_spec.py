"""Lab self-test: everything the Iceberg specification defines has a test.

The REST endpoints and data types of the specification are recorded per Iceberg release
(``scripts/iceberg_spec.py`` -> docs/api/iceberg-rest-spec.txt, docs/api/iceberg-types.txt).
This test checks them against the lab:

* every spec endpoint has a conformance test (``test_rest_conformance.ENDPOINTS``) or is listed
  with the place it is tested (``SPEC_ELSEWHERE``);
* every spec type has a type case (``iceberg_lab.datatypes.ALL_CASES``);
* the recorded release is the lab's Iceberg version (``engines.ICEBERG_VERSION``) - a new
  release (Renovate PR) fails here until the spec is re-recorded, and what it adds then fails
  the two checks above until it is tested.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.offline

ROOT = Path(__file__).resolve().parents[2]
REST_SPEC = ROOT / "docs" / "api" / "iceberg-rest-spec.txt"
TYPES_SPEC = ROOT / "docs" / "api" / "iceberg-types.txt"
RECORD = "uv run python scripts/iceberg_spec.py"


def _recorded(path: Path) -> tuple[str, list[str]]:
    """Release and entries of a recorded spec file.

    Args:
        path: ``docs/api/iceberg-*.txt``.

    Returns:
        ``(release, entries)``, e.g. ``("1.12.0", [...])``.
    """
    lines = [x for x in path.read_text().splitlines() if x]
    return lines[0].removeprefix("# apache-iceberg-"), lines[1:]


def _spec_type(case) -> str:
    """The spec's name for a type case's Iceberg type (``decimal(10,2)`` -> ``decimal``).

    Args:
        case: Type case (``iceberg_lab.datatypes.TypeCase``).

    Returns:
        The type name; ``variant`` for a case PyIceberg cannot model.
    """
    if case.iceberg is None:
        return "variant"
    return re.match(r"[a-z_]+", str(case.iceberg)).group()


@pytest.mark.parametrize("path", [REST_SPEC, TYPES_SPEC], ids=lambda p: p.name)
def test_recorded_spec_is_the_labs_iceberg_release(path):
    """The recorded spec belongs to the Iceberg release the lab runs.

    Checks:
        * the release in the file header equals ``engines.ICEBERG_VERSION``.

    Args:
        path: Recorded spec file.
    """
    from iceberg_lab.engines import ICEBERG_VERSION

    release, entries = _recorded(path)
    assert entries, f"{path.name} is empty"
    assert release == ICEBERG_VERSION, (
        f"{path.name} records apache-iceberg-{release}, the lab runs {ICEBERG_VERSION}: {RECORD}"
    )


def test_every_spec_endpoint_has_a_conformance_test():
    """Every endpoint of the REST spec gets a request in test_spec_endpoint, or names the test
    that covers it.

    Checks:
        * each recorded endpoint is a key of ``ENDPOINTS`` or ``SPEC_ELSEWHERE``;
        * neither list names an endpoint the spec does not have (a renamed route).
    """
    from test_rest_conformance import ENDPOINTS, SPEC_ELSEWHERE

    _, spec = _recorded(REST_SPEC)
    tested = set(ENDPOINTS) | set(SPEC_ELSEWHERE)
    assert not set(spec) - tested, (
        f"spec endpoints without a test: {sorted(set(spec) - tested)} - add an Endpoint to "
        "tests/catalog/test_rest_conformance.py"
    )
    assert not tested - set(spec), f"not in the spec: {sorted(tested - set(spec))}"


def test_every_spec_type_has_a_type_case():
    """Every data type of the format spec is in the type matrix.

    Checks:
        * each recorded type is the Iceberg type of at least one case of ``ALL_CASES``;
        * no case has a type the spec does not define.
    """
    from iceberg_lab.datatypes import ALL_CASES

    _, spec = _recorded(TYPES_SPEC)
    lab = {_spec_type(c) for c in ALL_CASES}
    assert not set(spec) - lab, (
        f"spec types without a type case: {sorted(set(spec) - lab)} - add a TypeCase to "
        "src/iceberg_lab/datatypes.py"
    )
    assert not lab - set(spec), f"type cases outside the spec: {sorted(lab - set(spec))}"
