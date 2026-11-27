"""Run manifest: what a report's results were produced with.

Every result record carries the id of the run that produced it (``run``). Reports merge results
over runs (a targeted re-run updates single tests), so one report can hold results of several
runs; the manifest of each run says with which versions they came about:

* ``manifest.json`` - the latest run of this report directory
* ``runs.json``     - every run whose manifest was recorded here, by run id

``scripts/run_all.sh`` exports ``LAB_RUN_ID`` so the pytest processes of one full run (one per
test file) share one id; a single pytest run gets its start time as id. ``scripts/rerun.sh``
runs under the full run's id with ``LAB_RERUN=1``: its versions are added to the full run's
``reruns`` and never replace the full run's own.

Contents: run id, start and end, catalog, whether lab helps were on, git commit (and whether the
work tree had changes), versions of the Python engines and clients, the images the compose file
declares and - when the Docker CLI can reach the daemon - the image digests of the running
containers. Everything is best effort: a missing tool leaves its field empty, it never fails a
run.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from iceberg_lab.lab.models import RunManifest

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / ".devcontainer" / "docker-compose.yml"
STARTED = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
#: Shared by all pytest processes of one full run (run_all.sh), else this process's start.
RUN_ID = os.getenv("LAB_RUN_ID") or STARTED
#: Python packages whose version decides results (engines and clients).
PACKAGES = (
    "pyiceberg",
    "pyiceberg-core",
    "pyarrow",
    "duckdb",
    "polars",
    "daft",
    "pyspark",
    "trino",
    "psycopg",
    "boto3",
)


def _run(cmd: list[str]) -> str:
    """Output of a command, or "" if it is missing or fails.

    Args:
        cmd: The command and its arguments.

    Returns:
        The stripped standard output.
    """
    try:
        out = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=15, cwd=ROOT)
    except (OSError, subprocess.SubprocessError):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def declared_images(path: Path = COMPOSE) -> dict[str, str]:
    """Image per service as the compose file declares it (no YAML library needed: services
    are the two-space-indented keys, their ``image:`` the four-space-indented one).

    Args:
        path: The compose file.

    Returns:
        ``{service: image}``.
    """
    images, service = {}, None
    if not path.exists():
        return images
    for line in path.read_text().splitlines():
        if m := re.match(r"^  ([A-Za-z0-9_.-]+):\s*(#.*)?$", line):  # key, maybe a comment
            service = m.group(1)
        elif (m := re.match(r"^    image:\s*(\S+)", line)) and service:
            images[service] = m.group(1)
    return images


def running_images() -> dict[str, str]:
    """Image digest per running lab container: only containers of a service the compose file
    declares - other containers on the host are none of the run's business. Empty without a
    reachable Docker daemon.

    Returns:
        ``{service: "image@digest"}`` (the image name alone where no digest is known).
    """
    services = set(declared_images())
    fmt = '{{.Label "com.docker.compose.service"}}\t{{.Image}}'
    out = {}
    for line in _run(["docker", "ps", "--format", fmt]).splitlines():
        service, _, image = line.partition("\t")
        if service not in services:
            continue
        digest = _run(["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image])
        out[service] = digest or image
    return out


def packages() -> dict[str, str]:
    """Installed versions of :data:`PACKAGES` (missing ones are left out).

    Returns:
        ``{package: version}``.
    """
    found = {}
    for p in PACKAGES:
        try:
            found[p] = importlib.metadata.version(p)
        except importlib.metadata.PackageNotFoundError:
            pass
    return found


def duckdb_extensions() -> dict[str, str]:
    """Versions of the installed DuckDB extensions the lab uses (``iceberg``, ``httpfs``, ...).

    ``INSTALL iceberg`` fetches the extension build that belongs to the DuckDB version, and that
    build can change without a DuckDB release - so the package version alone does not say which
    Iceberg code ran. Reads the local extension directory only (no download).

    Returns:
        ``{extension: version}`` of the installed extensions; empty without DuckDB.
    """
    try:
        import duckdb

        rows = (
            duckdb.connect()
            .execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE installed"
            )
            .fetchall()
        )
    except Exception:  # noqa: BLE001 - best effort, never fails a run
        return {}
    return {name: version or "" for name, version in rows}


def build(catalog: str, with_help: bool) -> dict:
    """The manifest of this run.

    Args:
        catalog: Catalog profile under test.
        with_help: Whether the lab's helps were on (``LAB_NO_HELP`` unset).

    Returns:
        The manifest (validated, :class:`~iceberg_lab.lab.models.RunManifest`).
    """
    return RunManifest(
        run=RUN_ID,
        started=STARTED,
        finished=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        catalog=catalog,
        with_help=with_help,
        git={
            "commit": _run(["git", "rev-parse", "HEAD"]),
            "dirty": bool(_run(["git", "status", "--porcelain"])),
        },
        packages=packages(),
        duckdb_extensions=duckdb_extensions(),
        images_declared=declared_images(),
        images_running=running_images(),
    ).model_dump()


#: What a rerun records of itself (:class:`~iceberg_lab.lab.models.Rerun`), besides its times.
RERUN_FIELDS = ("git", "packages", "duckdb_extensions", "images_declared", "images_running")


def merge_rerun(full: dict, m: dict) -> dict:
    """The full run's manifest with a rerun added to its ``reruns``.

    Its own fields stay as they are: the rows of the full run were produced with them. A
    rerun with the same versions as the one before only extends that one's end.

    Args:
        full: The full run's manifest (from ``runs.json``).
        m: The rerun's manifest (:func:`build`).

    Returns:
        The new manifest of the full run.
    """
    entry = {"started": m["started"], "finished": m["finished"]} | {k: m[k] for k in RERUN_FIELDS}
    reruns = [dict(r) for r in full.get("reruns", [])]
    if reruns and all(reruns[-1][k] == entry[k] for k in RERUN_FIELDS):
        reruns[-1]["finished"] = entry["finished"]
    else:
        reruns.append(entry)
    return {**full, "reruns": reruns}


def write(report_dir: Path, catalog: str, with_help: bool) -> dict:
    """Write ``manifest.json`` and merge this run into ``runs.json``.

    The pytest processes of one full run share the run id: each keeps the earliest start and
    the latest end. A rerun (``LAB_RERUN=1``, scripts/rerun.sh) merges its rows under the full
    run's id too, but its versions go to the full run's ``reruns`` (:func:`merge_rerun`) - it
    once overwrote them, and a reference made with Polars 2.1.0 and pg_lake 3.7.0 said 2.2.0
    and 3.7.1 after a few reruns.

    Args:
        report_dir: The report directory.
        catalog: Catalog profile under test.
        with_help: Whether the lab's helps were on.

    Returns:
        The manifest written to ``manifest.json`` (this process's own).
    """
    m = build(catalog, with_help)
    runs_path = report_dir / "runs.json"
    runs = json.loads(runs_path.read_text()) if runs_path.exists() else {}
    full = runs.get(RUN_ID)
    if full and os.getenv("LAB_RERUN") == "1":
        runs[RUN_ID] = merge_rerun(full, m)
    else:
        if full:
            m["started"] = min(m["started"], full.get("started", m["started"]))
            m["reruns"] = full.get("reruns", [])
        runs[RUN_ID] = m
    runs_path.write_text(json.dumps(runs, indent=1, sort_keys=True))
    (report_dir / "manifest.json").write_text(json.dumps(m, indent=1, sort_keys=True))
    return m
