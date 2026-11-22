"""Execute every example notebook top to bottom on the catalog under test (all four catalogs).

The notebooks connect through the catalog profile (``iceberg_lab.notebook``); expected outputs
may differ per catalog (``expect_<catalog>`` in the cell metadata).
"""

import re
from pathlib import Path

import pytest

from iceberg_lab.config import PROFILE, WITH_HELP

from conftest import expect_gap, require_engine

NOTEBOOKS = sorted((Path(__file__).resolve().parents[2] / "notebooks").glob("*.ipynb"))
#: Notebooks that need an optional engine. Notebook 08 (interop) skips missing engines itself.
NEEDS = {"05_spark": "spark", "06_trino": "trino", "07_pg_lake": "pg_lake", "09_flink": "flink"}


@pytest.mark.slow
@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.stem)
def test_notebook_runs(path: Path):
    """The example notebook runs on this catalog and shows the expected output.

    Checks:
        * every cell runs without error (a documented limitation only with its exact message);
        * the notebook has expected-output checks, and they all hold - running without errors
          alone would not prove the numbers are right.

    Args:
        path: The notebook file.
    """
    if not WITH_HELP:
        pytest.skip("notebooks run with the lab's helps; the run without help is for the engines")
    if path.stem in NEEDS:
        require_engine(NEEDS[path.stem])  # skip, or fail with REQUIRE_ENGINES
    # Plain imports: the notebook dependency group is part of the default install, so a
    # missing package is a broken setup and must fail, not skip.
    """Execute all cells of the notebook in a fresh kernel; any cell error fails the test."""
    import nbclient
    import nbformat

    nb = nbformat.read(path, as_version=4)
    client = nbclient.NotebookClient(
        nb, timeout=600, kernel_name="python3", resources={"metadata": {"path": str(path.parent)}}
    )
    # A notebook the catalog cannot run at all (e.g. DuckDB on RustFS) carries a known gap
    # per catalog in its metadata: {"lab": {"known_gaps": {"<catalog>": {"match", "reason"}}}}.
    known = nb.metadata.get("lab", {}).get("known_gaps", {}).get(PROFILE.name)
    with expect_gap(known["reason"] if known else None, known["match"] if known else None):
        client.execute()

    # Expected-output checks: cells carry {"lab": {"expect": regex, "expect_not": regex}} in
    # their metadata (invisible to readers). Executing without exceptions is not enough - a
    # notebook could run and still show wrong numbers.
    checked = sum(check_cell(idx, cell, PROFILE.name) for idx, cell in enumerate(nb.cells))
    assert checked, f"{path.name} has no expected-output checks"


def check_cell(idx: int, cell, catalog: str) -> bool:
    """Check one executed cell against its expectations; return whether it had any.

    Per-catalog expectations override the general one ("expect_unity" beats "expect"). A cell
    that shows catalog limits (``catalog_limit``) may print "not possible" only where that
    limit is expected for this catalog - anywhere else it is a failure, not a demo.
    (Self-tested in test_lab_selftest.py.)

    Args:
        idx: Cell index.
        cell: Executed cell.
        catalog: Catalog name.

    Returns:
        Whether the cell had checks.
    """
    source = cell.get("source", "")
    lab = cell.get("metadata", {}).get("lab")
    if not lab and "catalog_limit(" in source:
        lab = {}  # still checked: no unexpected "not possible"
    elif not lab:
        return False
    text = _cell_text(cell)
    first_line = (source.splitlines() or [""])[0][:60]
    expect = lab.get(f"expect_{catalog}", lab.get("expect"))
    expect_not = lab.get(f"expect_not_{catalog}", lab.get("expect_not"))
    if "catalog_limit(" in source and f"expect_{catalog}" not in lab:
        expect_not = "|".join(x for x in (expect_not, r"not possible") if x)
    if expect:
        assert re.search(expect, text, re.DOTALL), (
            f"cell {idx} ({first_line!r}): output does not match {expect!r}:\n{text[:800]}"
        )
    if expect_not:
        assert not re.search(expect_not, text, re.DOTALL), (
            f"cell {idx} ({first_line!r}): output matches forbidden {expect_not!r}:\n{text[:800]}"
        )
    return True


def _cell_text(cell) -> str:
    """All textual output of a cell: stdout/stderr streams and text/plain results.

    Args:
        cell: Executed cell.

    Returns:
        The text.
    """
    parts = []
    for out in cell.get("outputs", []):
        if out.get("output_type") == "stream":
            parts.append(out.get("text", ""))
        elif "data" in out:
            parts.append(out["data"].get("text/plain", ""))
    return "\n".join(parts)
