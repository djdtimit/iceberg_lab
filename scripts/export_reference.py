"""Export a report as the versioned reference in ``results/`` (checked into git).

``reports/`` is a working directory and stays out of git. ``results/`` holds the reference of
the latest full run per catalog, one test per line and sorted, so that ``git diff results/``
after an update shows exactly which tests changed - in both directions:

    results/<name>.jsonl          {"test", "status", "detail"[, "cause"]} per test (status "self"
                                  left out); a gap's detail is its cause's CURRENT reason in
                                  docs/gaps.toml, so rewording a cause needs an export, no rerun
    results/<name>.manifest.json  what the run was produced with (src/iceberg_lab/lab/manifest.py)
    results/<name>.rows.json.gz   the full comparison rows (source of the dashboard data)
    results/<name>.flaky.json     runs in a row without each intermittent gap (kept across runs)
    results/<name>.rest.json      the spec responses the catalog gave the lab's raw requests
                                  (src/iceberg_lab/lab/rest_observed.py)
    results/history/runs.jsonl    one line per reference and run: status counts and the gaps'
                                  causes (the dashboard's trend); a rerun updates its run's line

``<name>`` is the catalog (``lakekeeper``, ``polaris``, ``rustfs``, ``unity``, ``reference``, ``gravitino``) or
``<catalog>-nohelp`` for a run without lab help. ``scripts/run_all.sh`` calls this at the end.

Usage: uv run python scripts/export_reference.py <report dir>   (e.g. reports/polaris)
       uv run python scripts/export_reference.py --refresh   (after rewording a cause: every
                                                             reference gets the current reasons)
       uv run python scripts/export_reference.py --run-id <report dir>   (id of its full run)
       uv run python scripts/export_reference.py --history   (rebuild results/history/runs.jsonl
                                                             from every committed state of
                                                             results/ and the current one)
"""

from __future__ import annotations

import gzip
import json
import re
import sys
from collections import Counter
from pathlib import Path

from iceberg_lab.lab import gap_register as gr
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


def with_causes(rows: list[dict], known: dict[str, str] | None = None) -> list[dict]:
    """Rows with the gap register cause filled in and a gap's detail set to its current reason.

    Rows recorded before the ``cause`` field existed get it from their reason - or, if the cause
    was reworded since, from the reference's row of the same test (``known``).

    Args:
        rows: Report rows (``status``, ``detail``, optional ``cause``).
        known: ``test id -> cause`` of the current reference.

    Returns:
        New rows.
    """
    causes, out, known = gr.register(), [], known or {}
    for r in rows:
        cid = gr.row_cause(r)
        if cid is None and r.get("status") in ("gap", "fixed"):
            cid = known.get(r.get("test") or r.get("nodeid", ""))
        if cid in causes:
            r = {**r, "cause": cid, "detail": causes[cid].reason}
        out.append(r)
    return out


def _line(r: dict) -> str:
    """One reference line (``cause`` only where there is one, so other lines stay unchanged).

    Args:
        r: A row.

    Returns:
        The JSON line.
    """
    row = ResultRow(
        test=r["test"], status=r["status"], detail=r.get("detail", ""), cause=r.get("cause") or ""
    ).model_dump()
    if not row["cause"]:
        del row["cause"]
    return json.dumps(row, ensure_ascii=False)


HISTORY = OUT / "history" / "runs.jsonl"  # not results/*.jsonl: those are the references


def history_line(name: str, rows: list[dict], manifest: dict | None) -> dict | None:
    """The trend line of one reference: its run, status counts and the gaps' causes.

    Args:
        name: Reference name.
        rows: Its rows (``status``, ``cause``).
        manifest: The run's manifest (``run``, ``started``, ``finished``), or None.

    Returns:
        The line, or None without a recorded run.
    """
    if not manifest or not manifest.get("run"):
        return None
    counts = Counter(r["status"] for r in rows if r["status"] != "self")
    return {
        "name": name,
        "run": manifest["run"],
        "finished": manifest.get("finished") or manifest["run"],
        "counts": dict(sorted(counts.items())),
        "causes": sorted({r["cause"] for r in rows if r.get("cause")}),
    }


def update_history(line: dict | None, path: Path | None = None) -> None:
    """Put a trend line into :data:`HISTORY`, replacing the line of the same reference and run.

    Args:
        line: From :func:`history_line`; None changes nothing.
        path: The history file (default :data:`HISTORY`).
    """
    if line is None:
        return
    path = path or HISTORY
    lines = [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else []
    lines = [x for x in lines if (x["name"], x["run"]) != (line["name"], line["run"])] + [line]
    lines.sort(key=lambda x: (x["name"], x["run"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(x, sort_keys=True) + "\n" for x in lines))


def export(
    rows: list[dict],
    name: str,
    manifest: dict | None,
    out: Path = OUT,
    flaky: dict | None = None,
    rest: dict | None = None,
) -> Path:
    """Write the reference lines (and the manifest, if any) of one report.

    Args:
        rows: Rows with at least ``test``, ``status`` and ``detail``.
        name: Reference name (see :func:`name_of`).
        manifest: The run's manifest, or None.
        out: Target directory.
        flaky: The report's ``flaky_history.json``, or None.
        rest: The report's ``rest_observed.json``, or None.

    Returns:
        The written ``.jsonl`` file.
    """
    out.mkdir(parents=True, exist_ok=True)
    rows = with_causes(rows)
    lines = sorted(_line(r) for r in rows if r["status"] != "self")
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
    if out == OUT:
        update_history(history_line(name, rows, manifest))
    if rest is not None:
        (out / f"{name}.rest.json").write_text(json.dumps(rest, indent=0, sort_keys=True) + "\n")
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
    reference = OUT / f"{name_of(report_dir)}.jsonl"
    known = {}
    if reference.exists():
        for line in reference.read_text().splitlines():
            row = json.loads(line)
            if row.get("cause"):
                known[row["test"]] = row["cause"]
    rows = with_causes(json.loads((report_dir / "comparison_rows.json").read_text()), known)
    # Keep the causes in the report's records too (comparison_rows.json is rebuilt from them
    # by every run): after a rewording a record without its cause could not be matched by text.
    records_path = report_dir / "results.json"
    if records_path.exists():
        records = json.loads(records_path.read_text())
        for rec in records:
            cid = gr.row_cause(rec) or (
                known.get(rec["nodeid"]) if rec.get("status") in ("gap", "fixed") else None
            )
            if cid and not rec.get("cause"):
                rec["cause"] = cid
        records_path.write_text(json.dumps(records, indent=1, ensure_ascii=False, default=str))
    gone = [r for r in rows if not still_exists(r["test"])]
    rows = [r for r in rows if still_exists(r["test"])]
    if gone:
        print(f"left out {len(gone)} rows of tests that no longer exist")
    history = report_dir / "flaky_history.json"
    flaky = json.loads(history.read_text()) if history.exists() else None
    observed = report_dir / "rest_observed.json"
    rest = json.loads(observed.read_text()) if observed.exists() else None
    path = export(
        rows, name_of(report_dir), main_manifest(report_dir, rows), flaky=flaky, rest=rest
    )
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


def refresh(out: Path = OUT) -> None:
    """Rewrite every reference in ``results/`` with the register's current reasons (no report
    needed): what a reworded cause in docs/gaps.toml needs instead of a rerun.

    Args:
        out: The results directory.
    """
    for path in sorted(out.glob("*.jsonl")):
        rows = with_causes([json.loads(line) for line in path.read_text().splitlines()])
        path.write_text("\n".join(sorted(_line(r) for r in rows)) + "\n")
        full_path = out / f"{path.stem}.rows.json.gz"
        if full_path.exists():
            full = with_causes(json.loads(gzip.decompress(full_path.read_bytes())))
            text = json.dumps(
                sorted(full, key=lambda r: r["test"]), ensure_ascii=False, sort_keys=True
            )
            full_path.write_bytes(gzip.compress(text.encode(), compresslevel=9, mtime=0))
        print(f"{path.relative_to(ROOT)}: reasons refreshed from docs/gaps.toml")


def rebuild_history(path: Path | None = None) -> None:
    """Rebuild :data:`HISTORY` from every commit that changed ``results/`` and the files now.

    Each reference with a manifest gives one line per run; a later state of the same run (a
    merged rerun) replaces the earlier one. Old states without cause ids get them from the
    register by reason text, where it still knows the reason.

    Args:
        path: The history file (default :data:`HISTORY`).
    """
    import subprocess

    path = path or HISTORY

    def git(*args: str) -> str:
        """Output of a git command in the repository.

        Args:
            *args: git arguments.

        Returns:
            Its standard output.
        """
        return subprocess.run(
            ["git", *args], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout

    def line_of(name: str, jsonl: str, manifest: str) -> dict | None:
        """The trend line of one reference state.

        Args:
            name: Reference name.
            jsonl: Its ``.jsonl`` text.
            manifest: Its ``.manifest.json`` text.

        Returns:
            The line, or None.
        """
        rows = [json.loads(x) for x in jsonl.splitlines() if x.strip()]
        for r in rows:
            if r["status"] in ("gap", "fixed") and not r.get("cause"):
                r["cause"] = gr.cause_of_reason(r.get("detail", "")) or ""
        return history_line(name, rows, json.loads(manifest))

    path.unlink(missing_ok=True)
    for commit in reversed(git("log", "--format=%H", "--", "results/").split()):
        for f in git("ls-tree", "--name-only", commit, "results/").split():
            if not f.endswith(".jsonl"):  # ls-tree lists results/history/ as one entry
                continue
            name = Path(f).stem
            try:
                manifest = git("show", f"{commit}:results/{name}.manifest.json")
            except subprocess.CalledProcessError:
                continue
            update_history(line_of(name, git("show", f"{commit}:{f}"), manifest), path)
    for f in sorted(OUT.glob("*.jsonl")):
        m = f.with_name(f"{f.stem}.manifest.json")
        if f.name != HISTORY.name and m.exists():
            update_history(line_of(f.stem, f.read_text(), m.read_text()), path)
    print(f"{path.relative_to(ROOT)}: {len(path.read_text().splitlines())} lines")


if __name__ == "__main__":
    if sys.argv[1] == "--run-id":  # used by scripts/rerun.sh
        print(run_id(Path(sys.argv[2])))
    elif sys.argv[1] == "--refresh":
        refresh()
    elif sys.argv[1] == "--history":
        rebuild_history()
    else:
        main(Path(sys.argv[1]))
