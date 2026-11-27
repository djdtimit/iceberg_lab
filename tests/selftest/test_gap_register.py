"""Self-tests of the gap register (``docs/gaps.toml``, ``iceberg_lab.lab.gap_register``).

Runs without catalog or engines. Covered:

* the register: it loads (pydantic validates every entry), its file is in canonical form,
  every cause is used by a test, a central rule or a notebook, and no test bypasses it
  (``expect_gap`` with a free-text reason);
* the shared ``Gap`` type: an unknown cause, a regex that does not compile or is too generic,
  and a selector the test does not pass are refused; rows for the same case are alternatives;
* versions: every cause was last seen with the version the lab runs now - after an upgrade
  the test lists the causes to check again;
* the references in ``results/``: every reported gap names a cause of the register, and all
  references hold the same test cases (one per catalog, comparable);
* docs/findings.md: its gap list is the one generated from the register.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from iceberg_lab.lab import gap_register as gr
from iceberg_lab.lab import gaps, reporting

#: Needs no catalog or engine: runs without the lab (``uv run pytest -m offline``).
pytestmark = pytest.mark.offline

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "results"
FINDINGS = ROOT / "docs" / "findings.md"


def _sources() -> dict[str, str]:
    """Test files (without the self-tests), the central rules and the notebooks.

    Returns:
        ``{repo-relative path: text}``.
    """
    files = [p for p in (ROOT / "tests").rglob("*.py") if "selftest" not in p.parts]
    files += [ROOT / "src" / "iceberg_lab" / "lab" / "gaps.py"]
    files += sorted((ROOT / "notebooks").glob("*.ipynb"))
    return {p.relative_to(ROOT).as_posix(): p.read_text() for p in files}


def test_register_is_canonical():
    """The register file is in the form ``gap_register.dump`` writes.

    Checks:
        * ``dump(load())`` reproduces the file (so ``scripts/gap_register.py`` can rewrite
          it without unrelated changes).
    """
    assert gr.dump(gr.load()) == gr.REGISTER_FILE.read_text(), (
        "docs/gaps.toml is not canonical - run `uv run python scripts/gap_register.py format`"
    )


def test_every_cause_is_used():
    """No cause is left in the register after its last gap was removed.

    Checks:
        * every cause id occurs (quoted) in a test, a central rule or a notebook.
    """
    text = "\n".join(_sources().values())
    unused = [cid for cid in gr.register() if f'"{cid}"' not in text]
    assert not unused, f"causes no test refers to - remove them from docs/gaps.toml: {unused}"


def test_tests_use_the_register():
    """Known gaps go through the register: no test calls ``expect_gap`` with its own text.

    Checks:
        * no test file outside the self-tests calls ``expect_gap(`` (``expect``,
          ``expect_cause`` and ``find_gap`` take cause ids);
        * every notebook gap names a known cause.
    """
    calls = [p for p, t in _sources().items() if p.startswith("tests/") and "expect_gap(" in t]
    assert not calls, f"use expect/expect_cause with a cause id: {calls}"
    for p in sorted((ROOT / "notebooks").glob("*.ipynb")):
        known = json.loads(p.read_text())["metadata"].get("lab", {}).get("known_gaps", {})
        for entry in known.values():
            gr.cause(entry["cause"])


def test_gap_refuses_unknown_cause_and_bad_regex():
    """The shared ``Gap`` type validates what a test writes.

    Checks:
        * an unknown cause id, a regex that does not compile and a too generic regex are
          refused when the table is built;
        * a selector the test does not pass is an error, not a silent non-match.
    """
    with pytest.raises(ValueError, match="unknown gap cause"):
        gr.gap("no-such-cause", r"some exact error")
    with pytest.raises(ValueError, match="does not compile"):
        gr.gap("flink-no-geo", r"(unclosed")
    with pytest.raises(ValueError, match="too generic"):
        gr.gap("flink-no-geo", r"error")
    g = gr.gap("flink-no-geo", r"Cannot convert unknown type to Flink: geo", engine="flink")
    with pytest.raises(ValueError, match="not passed by the test"):
        gr.find_gap([g], reader="flink")
    assert gr.find_gap([g], engine="flink") is g
    assert gr.find_gap([g], engine="spark") is None


def test_alternative_gaps_pick_the_cause_by_error(monkeypatch):
    """Several rows for the same case are alternatives; the error decides.

    Checks:
        * ``find_gap`` returns all rows that apply, in order;
        * a failure is booked on the row whose regex matches it, not on the first row;
        * an error no row names stays a new failure.

    Args:
        monkeypatch: keeps the recorded hits out of the run's summary.
    """
    for name in ("_KNOWN_GAP_HITS", "_FIXED_GAPS", "_FLAKY_NOT_SEEN", "_FLAKY_SEEN"):
        monkeypatch.setattr(gaps, name, [])
    monkeypatch.setattr(gaps, "KNOWN_GAPS_MODE", "fail")
    a = gr.gap("flink-no-geo", r"Cannot convert unknown type to Flink: geo", engine="*")
    b = gr.gap("flink-no-procedures", r"getProcedure is not implemented", engine="flink")
    both = gr.find_gap([a, b], engine="flink")
    assert both == (a, b)
    with pytest.raises(gaps.KnownGapError, match="procedures"), gr.expect(both):
        raise RuntimeError("getProcedure is not implemented for class FlinkCatalog")
    with pytest.raises(RuntimeError, match="something else"), gr.expect(both):
        raise RuntimeError("something else entirely")


def test_layer_rows_are_inherited_through_gravitino(monkeypatch):
    """``layer_rows``: a catalog's own rows, inherited behind Gravitino, absent elsewhere.

    Checks:
        * on the catalog itself the rows are its own (a passing case reports "gap fixed");
        * on a Gravitino-fronted profile of that backend they are inherited: a matching error
          is booked, a passing case stays silent;
        * on any other catalog there are no rows.

    Args:
        monkeypatch: switches the profile and keeps the recorded hits out of the summary.
    """
    from dataclasses import replace

    from iceberg_lab import config

    for name in ("_KNOWN_GAP_HITS", "_FIXED_GAPS", "_FLAKY_NOT_SEEN", "_FLAKY_SEEN"):
        monkeypatch.setattr(gaps, name, [])
    monkeypatch.setattr(gaps, "KNOWN_GAPS_MODE", "fail")
    row = ("*", r"Iceberg type TIME is not supported by Unity Catalog", "unity-no-v3-types")

    def rows(name: str, fronts: str | None = None) -> list:
        """The gaps of the Unity block for a profile.

        Args:
            name: Profile name.
            fronts: Backend behind Gravitino, or None.

        Returns:
            The gaps.
        """
        monkeypatch.setattr(config, "PROFILE", replace(config.PROFILE, name=name, fronts=fronts))
        return gr.gap_table(("case",), gr.layer_rows("unity", [row]))

    (own,) = rows("unity")
    assert not own.inherited
    with pytest.raises(gaps.GapFixedError), gr.expect(own):
        pass
    (inherited,) = rows("gravitino_unity", fronts="unity")
    assert inherited.inherited
    with pytest.raises(gaps.KnownGapError), gr.expect(inherited):
        raise RuntimeError("Iceberg type TIME is not supported by Unity Catalog")
    with gr.expect(inherited):
        pass
    assert rows("polaris") == []
    assert rows("gravitino_polaris", fronts="polaris") == []


def test_cause_validation():
    """A cause entry is checked when the register is read.

    Checks:
        * a severe-sounding summary needs a severity; the severity is not written into the
          summary; components and bug reports must exist; the id starts with its owner.
    """
    ok = {"summary": "Engine X has no feature Y at all", "seen_at": {"duckdb": "2.0.1"}}
    assert gr.Cause(id="duckdb-x", **ok).reason == ok["summary"]
    with pytest.raises(ValueError, match="sounds severe"):
        gr.Cause(id="duckdb-x", **{**ok, "summary": "Engine X silently drops rows"})
    with pytest.raises(ValueError, match="not in front of the summary"):
        gr.Cause(id="duckdb-x", **{**ok, "summary": "WRONG RESULT: rows come back twice"})
    with pytest.raises(ValueError, match="unknown component"):
        gr.Cause(id="duckdb-x", **{**ok, "seen_at": {"oracle": "1"}})
    with pytest.raises(ValueError, match="does not exist"):
        gr.Cause(id="duckdb-x", **ok, bug_report="99-nothing.md")
    with pytest.raises(ValueError, match="must start with its owner"):
        gr.Cause(id="spark-x", **ok)
    assert gr.Cause(id="engines-x", **ok).owner == "engines"
    sev = gr.Cause(
        id="duckdb-x", **{**ok, "summary": "Engine X silently drops rows"}, severity="DATA LOSS"
    )
    assert sev.reason.startswith("DATA LOSS: ") and gaps.gap_severity(sev.reason) == "Data loss"


def test_causes_seen_with_current_versions():
    """Every cause was last reproduced with the versions the lab runs now.

    After an upgrade this lists the causes to check again: run the affected tests (or a full
    run), then ``scripts/gap_register.py confirm`` moves ``seen_at`` forward for the causes
    that reproduced; the others show up as "gap fixed" in the run.

    Checks:
        * ``seen_at`` equals the current version of every component a cause names.
    """
    stale = gr.outdated(gr.register().values(), gr.current_versions())
    lines = [f"{c.id}: {comp} {seen} -> {now}" for c, comp, seen, now in stale]
    assert not stale, "version changed - check these causes again:\n  " + "\n  ".join(lines)


def test_results_name_known_causes():
    """The references in ``results/`` match the register.

    Checks:
        * every known or fixed gap in a reference names a cause of the register (``cause``);
        * its detail is that cause's current reason (the export writes it - a reworded cause
          needs ``scripts/export_reference.py``, not a rerun).
    """
    causes = gr.register()
    unknown, stale = set(), set()
    for path in sorted(RESULTS.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            row = json.loads(line)
            if row["status"] not in ("gap", "fixed"):
                continue
            cid = row.get("cause", "")
            if cid not in causes:
                unknown.add((path.stem, cid or row["detail"][:120]))
            elif row["detail"] != causes[cid].reason:
                stale.add((path.stem, cid))
    assert not unknown, f"reference gaps without a known cause: {sorted(unknown)[:10]}"
    assert not stale, f"reasons older than the register - re-export: {sorted(stale)[:10]}"


def _base_ids(path: Path) -> set[str]:
    """Test ids of a reference with the catalog-specific tests left out.

    Args:
        path: ``results/<name>.jsonl``.

    Returns:
        The ids of the compared tests.
    """
    ids = set()
    for line in path.read_text().splitlines():
        test = json.loads(line)["test"]
        if "/specific/" not in test:
            ids.add(test)
    return ids


def test_references_hold_the_same_tests():
    """All catalogs ran the same comparison.

    Checks:
        * every full reference in ``results/`` (catalog-specific tests and the ``-nohelp``
          subsets aside) holds the same test ids - a test that only some references have was added or removed between runs.
    """
    # ``<catalog>-nohelp`` runs only the tests of the engines the lab helps (run_all.sh).
    refs = {
        p.stem: _base_ids(p)
        for p in sorted(RESULTS.glob("*.jsonl"))
        if not p.stem.endswith("-nohelp")
    }
    if len(refs) < 2:
        pytest.skip("fewer than two references")
    union = set().union(*refs.values())
    missing = {name: sorted(union - ids)[:5] for name, ids in refs.items() if union - ids}
    assert not missing, f"tests missing from references (re-run them): {missing}"


def test_every_reference_row_has_an_area():
    """No test of a reference falls through the area rules (``reporting.DIMENSIONS``).

    An unassigned test is compared like a base test: a lab self-test parametrized by engine
    gave pg_lake on RustFS (everything else not offered there) "1 of 1 works" - 100 % in the
    dashboard.

    Checks:
        * no row of ``results/<name>.rows.json.gz`` has the area ``Unassigned``.
    """
    unassigned = {
        p.name: sorted({r["test"].split("::")[0] for r in rows if r["area"] == "Unassigned"})
        for p in sorted(RESULTS.glob("*.rows.json.gz"))
        if (rows := json.loads(gzip.decompress(p.read_bytes())))
    }
    unassigned = {k: v for k, v in unassigned.items() if v}
    assert not unassigned, f"tests without an area (reporting.DIMENSIONS): {unassigned}"


def test_every_engine_case_names_its_engine():
    """A compared case whose test id names an engine is charged to an engine.

    Without one it counts at the catalog's own tests: the write cases of
    test_access_delegation and test_idp_outage did, on every catalog.

    Checks:
        * no compared row of ``results/<name>.rows.json.gz`` with an engine among its id's
          parameters has an empty ``engine``.
    """
    engines = set(reporting.ENGINE_ORDER)
    missing = {
        p.name: sorted(
            {
                r["test"].split("[")[0]
                for r in rows
                if r["theme"] != "Not compared"
                and not r["engine"]
                and "[" in r["test"]
                and engines & set(r["test"].split("[", 1)[1].rstrip("]").split("-"))
            }
        )
        for p in sorted(RESULTS.glob("*.rows.json.gz"))
        if (rows := json.loads(gzip.decompress(p.read_bytes())))
    }
    missing = {k: v for k, v in missing.items() if v}
    assert not missing, f"engine cases without an engine (reporting._engine): {missing}"


def test_findings_list_is_generated():
    """docs/findings.md shows the register's gap list.

    Checks:
        * the block between the markers equals the generated one (register + references).
    """
    text = FINDINGS.read_text()
    block = gr.findings_markdown(gr.register(), gr.hits(RESULTS))
    assert gr.FINDINGS_START in text and gr.FINDINGS_END in text, "markers missing"
    assert gr.replace_block(text, block) == text, (
        "docs/findings.md is outdated - run `uv run python scripts/gap_register.py findings`"
    )


def test_references_follow_the_schema():
    """Every reference line and manifest in ``results/`` has the documented form.

    Checks:
        * each line is a :class:`~iceberg_lab.lab.models.ResultRow` (known status, test id);
        * each manifest is a :class:`~iceberg_lab.lab.models.RunManifest`.
    """
    from iceberg_lab.lab.models import ResultRow, RunManifest

    for path in sorted(RESULTS.glob("*.jsonl")):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            try:
                ResultRow.model_validate_json(line)
            except ValueError as e:
                pytest.fail(f"{path.name}:{n}: {e}")
    for path in sorted(RESULTS.glob("*.manifest.json")):
        RunManifest.model_validate_json(path.read_text())
