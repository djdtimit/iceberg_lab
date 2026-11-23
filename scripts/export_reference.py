"""Export a report as the versioned reference in ``results/`` (checked into git).

``reports/`` is a working directory and stays out of git. ``results/`` holds the reference of
the latest full run per catalog, one test per line and sorted, so that ``git diff results/``
after an update shows exactly which tests changed - in both directions:

    results/<name>.jsonl          {"test", "status", "detail"} per test (status "self" left out)
    results/<name>.manifest.json  what the run was produced with (src/iceberg_lab/lab/manifest.py)
    results/<name>.rows.json.gz   the full comparison rows (source of the dashboard data)
    results/<name>.flaky.json     runs in a row without each intermittent gap (kept across runs)

``<name>`` is the catalog (``lakekeeper``, ``polaris``, ``rustfs``, ``unity``, ``reference``) or
``<catalog>-nohelp`` for a run without lab help. ``scripts/run_all.sh`` calls this at the end.

Usage: uv run python scripts/export_reference.py <report dir>   (e.g. reports/polaris)
       uv run python scripts/export_reference.py --run-id <report dir>   (id of its full run)
"""

from __future__ import annotations

import gzip
import json
import re
import sys
from collections import Counter
from pathlib import Path

from iceberg_lab.lab.models import ResultRow, RunManifest

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "results"


def name_of(report_dir: Path) -> str:
    """Reference name of a report directory (``reports`` is Lakekeeper's).

    Args:
        report_dir: The report directory.

    Returns:
        The name, e.g. ``polaris`` or ``rustfs-nohelp``.
    """
    report_dir = report_dir.resolve()
    return "lakekeeper" if report_dir.name == "reports" else report_dir.name


def export(
    rows: list[dict],
    name: str,
    manifest: dict | None,
    out: Path = OUT,
    flaky: dict | None = None,
) -> Path:
    """Write the reference lines (and the manifest, if any) of one report.

    Args:
        rows: Rows with at least ``test``, ``status`` and ``detail``.
        name: Reference name (see :func:`name_of`).
        manifest: The run's manifest, or None.
        out: Target directory.
        flaky: The report's ``flaky_history.json``, or None.

    Returns:
        The written ``.jsonl`` file.
    """
    out.mkdir(parents=True, exist_ok=True)
    lines = sorted(
        json.dumps(
            ResultRow(test=r["test"], status=r["status"], detail=r.get("detail", "")).model_dump(),
            ensure_ascii=False,
        )
        for r in rows
        if r["status"] != "self"
    )
    path = out / f"{name}.jsonl"
    path.write_text("\n".join(lines) + "\n")
    # The full comparison rows (area, capability, run, ...) as well: reports/ is not kept, the
    # dashboard (scripts/dashboard/build_dashboard_data.py) can be rebuilt from these.
    full = json.dumps(sorted(rows, key=lambda r: r["test"]), ensure_ascii=False, sort_keys=True)
    with (out / f"{name}.rows.json.gz").open("wb") as f:
        f.write(gzip.compress(full.encode(), compresslevel=9, mtime=0))
    if (
        flaky is not None
    ):  # runs in a row without each intermittent gap (src/iceberg_lab/lab/gaps.py)
        (out / f"{name}.flaky.json").write_text(json.dumps(flaky, indent=1, sort_keys=True) + "\n")
    if manifest is not None:
        manifest = RunManifest.model_validate(manifest).model_dump()
        (out / f"{name}.manifest.json").write_text(
            json.dumps(manifest, indent=1, sort_keys=True) + "\n"
        )
    return path


def main_manifest(report_dir: Path, rows: list[dict]) -> dict | None:
    """The manifest of the run that produced most of the report's rows.

    ``manifest.json`` is the latest pytest process - after re-running a few tests that is not
    the full run the reference stems from; ``runs.json`` keeps every run's manifest.

    Args:
        report_dir: The report directory.
        rows: The report's rows (each names its run).

    Returns:
        The manifest, or None.
    """
    runs_path, latest = report_dir / "runs.json", report_dir / "manifest.json"
    runs = json.loads(runs_path.read_text()) if runs_path.exists() else {}
    counts = Counter(r.get("run", "") for r in rows if r.get("run", "") in runs)
    if counts:
        return runs[counts.most_common(1)[0][0]]
    return json.loads(latest.read_text()) if latest.exists() else None


def still_exists(test: str) -> bool:
    """Whether the test function of a result row is still in its file.

    A merged rerun after a rename keeps the old name's row in the report; it must not end up
    in the reference.

    Args:
        test: Test id ``tests/<file>.py::<function>[<params>]``.

    Returns:
        True if the file defines the function.
    """
    file, _, rest = test.partition("::")
    path = ROOT / file
    func = rest.split("[")[0].split("::")[-1]
    pattern = rf"^\s*(async )?def {re.escape(func)}\("
    return path.exists() and bool(re.search(pattern, path.read_text(), re.MULTILINE))


def main(report_dir: Path) -> None:
    """Export ``comparison_rows.json`` and ``manifest.json`` of a report directory.

    Args:
        report_dir: The report directory.
    """
    rows = json.loads((report_dir / "comparison_rows.json").read_text())
    gone = [r for r in rows if not still_exists(r["test"])]
    rows = [r for r in rows if still_exists(r["test"])]
    if gone:
        print(f"left out {len(gone)} rows of tests that no longer exist")
    history = report_dir / "flaky_history.json"
    flaky = json.loads(history.read_text()) if history.exists() else None
    path = export(rows, name_of(report_dir), main_manifest(report_dir, rows), flaky=flaky)
    print(f"{path.relative_to(ROOT)}: {len(rows)} tests - `git diff results/` shows the changes")


def run_id(report_dir: Path) -> str:
    """Id of the full run a report holds (the run with most rows; reruns merge into it).

    Args:
        report_dir: The report directory.

    Returns:
        The run id.

    Raises:
        SystemExit: The report has no recorded run.
    """
    rows = json.loads((report_dir / "comparison_rows.json").read_text())
    manifest = main_manifest(report_dir, rows)
    if not manifest:
        raise SystemExit(f"{report_dir}: no recorded run - start with scripts/run_all.sh")
    return manifest["run"]


if __name__ == "__main__":
    if sys.argv[1] == "--run-id":  # used by scripts/rerun.sh
        print(run_id(Path(sys.argv[2])))
    else:
        main(Path(sys.argv[1]))
