"""Known gaps: a failure with exactly the documented error is a known gap, nothing else is.

``expect_gap(reason, match)`` turns a failure with exactly the documented error (regex, refused
if it would also match a generic error) into a known gap and reports a gap that no longer
occurs as fixed. A reason that sounds like a defect must carry a severity prefix
(``SEVERITIES``). Gaps of a whole catalog or of intermittent problems are central rules
(``CATALOG_GAPS``, ``FLAKY_GAPS``), applied by ``central_gap`` from conftest's report hook.
Tests do not call ``expect_gap`` themselves: they name a cause of the gap register
(``gap_register.expect``), which supplies reason and severity.

The hit lists below are the run's state; conftest's hooks read them through this module (the
lab self-tests replace them per test).
"""

from __future__ import annotations

import contextlib
import os
import re
from pathlib import Path

import pytest

#: How known gaps are reported, from ``KNOWN_GAPS=fail|xfail`` (default ``fail``).
#:
#: ``fail``  (default) a known gap is a real failure: FAILED, message prefixed with
#:           ``[known gap] <reason>`` so it is distinguishable from new errors at a glance.
#: ``xfail`` a known gap is reported as XFAIL (green run) - for CI or when only *new*
#:           errors matter.
#:
#: In BOTH modes:
#: * an error that does not match the gap's expected message is reported as the original,
#:   unlabelled failure - a known gap can never hide a different bug;
#: * a documented gap that no longer reproduces FAILS with ``[gap fixed] <reason>`` and is
#:   listed under "FIXED gaps" in the summary - so an upgrade that fixes something is
#:   impossible to miss, and the KNOWN_GAPS entry gets removed. Exception: gaps marked
#:   ``flaky=True`` (intermittent problems) show up as "not reproduced this run", and after
#:   FLAKY_FIXED_AFTER runs in a row without them as "PROBABLY FIXED".
KNOWN_GAPS_MODE = os.getenv("KNOWN_GAPS", "fail")
if KNOWN_GAPS_MODE not in ("fail", "xfail"):
    raise ValueError(f"KNOWN_GAPS must be 'fail' or 'xfail', not {KNOWN_GAPS_MODE!r}")

#: Collected during the run for the terminal summary: (test id, reason).
_KNOWN_GAP_HITS: list[tuple[str, str]] = []
_FIXED_GAPS: list[tuple[str, str]] = []
_FLAKY_NOT_SEEN: list[tuple[str, str]] = []
_FLAKY_SEEN: list[tuple[str, str]] = []

#: An intermittent gap that has not occurred in this many runs in a row (per catalog, kept in
#: <report dir>/flaky_history.json) is listed as "probably fixed" - check and remove it.
FLAKY_FIXED_AFTER = 5


class KnownGapError(AssertionError):
    """Raised (in ``fail`` mode) when a documented gap reproduces; chains the real error."""


class GapFixedError(AssertionError):
    """Raised when a documented gap no longer reproduces: the KNOWN_GAPS entry is stale."""


#: Errors that have nothing to do with any documented gap: infrastructure down, timeouts,
#: wrong results, plain server errors, lab bugs. A gap regex matching one of them would book
#: such a failure as "known gap" - exactly how a gap hides a bug. ``expect_gap`` refuses it.
GENERIC_ERRORS = [
    "ConnectionError: HTTPConnectionPool(host='catalog', port=8181): Max retries exceeded with url: /v1/config (Caused by NewConnectionError('Connection refused'))",
    "ReadTimeout: HTTPConnectionPool(host='trino', port=8080): Read timed out. (read timeout=30)",
    "TimeoutError: timed out",
    "OSError: [Errno 111] Connection refused",
    "AssertionError: assert 1 == 2",
    "AssertionError: rows differ\nassert [(1, 'a')] == []",
    "KeyError: 'id'",
    "TypeError: 'NoneType' object is not subscriptable",
    "AttributeError: 'NoneType' object has no attribute 'execute'",
    "HTTPError: 500 Server Error: Internal Server Error for url: http://catalog:8181/v1/x/namespaces",
    "Py4JJavaError: An error occurred while calling o123.sql.\n: java.lang.NullPointerException",
    "RuntimeError: Flink has no TaskManager",
    "Exception: error",
    "ValueError: invalid literal for int() with base 10: 'x'",
]


def _require_specific(match: str | None, reason: str) -> None:
    """A documented gap must name its error precisely (see GENERIC_ERRORS).

    Args:
        match: The gap's error pattern.
        reason: The gap's reason (for the message).

    Raises:
        ValueError: no pattern, or one that also matches a generic error.
    """
    if match is None:
        raise ValueError(f"gap without error pattern (would accept any failure): {reason}")
    generic = [g for g in GENERIC_ERRORS if re.search(match, g, re.IGNORECASE | re.DOTALL)]
    if generic:
        raise ValueError(
            f"gap pattern {match!r} is too generic - it also matches {generic[0]!r}: {reason}"
        )


#: Severity of a gap, written as the first word(s) of its reason. A gap with one of these
#: prefixes is no missing feature but a defect that changes data or results or weakens
#: security - the report lists them separately ("Severe gaps"), they never hide among
#: "not supported".
SEVERITIES = {
    "DATA LOSS:": "Data loss",  # written data is not stored (fully or exactly)
    "DATA CORRUPTION:": "Data corruption",  # the table is broken afterwards
    "INCONSISTENT:": "Inconsistency",  # a commit is applied partly / diverges between views
    "WRONG RESULT:": "Wrong result",  # a read or query returns wrong rows/values
    "SECURITY:": "Security",  # access or credentials reach further than intended
}
#: Words in a gap reason that suggest a severe defect: such a reason must carry a severity.
SEVERE_HINT = re.compile(
    r"(?i)\b(silently|lost|loses|duplicat\w*|truncat\w*|corrupt\w*|stale|not atomic|"
    r"old rows|returns NULL|keeps access)\b|as \[\]|-> \[\]"
)


def gap_severity(reason: str | None) -> str | None:
    """Label of the reason's severity prefix (``None``: a limitation, not a defect).

    Args:
        reason: The gap's reason.

    Returns:
        The label (e.g. "Wrong result"), or None.
    """
    for prefix, label in SEVERITIES.items():
        if (reason or "").startswith(prefix):
            return label
    return None


def _require_severity(reason: str) -> None:
    """A reason that sounds like a defect must say so with a severity prefix.

    Args:
        reason: The gap's reason.

    Raises:
        ValueError: the reason sounds severe but has no severity prefix.
    """
    hint = SEVERE_HINT.search(reason)
    if hint and gap_severity(reason) is None:
        raise ValueError(
            f"gap reason sounds severe ({hint.group(0)!r}) but has no severity prefix "
            f"({', '.join(SEVERITIES)}) - classify it: {reason}"
        )


@contextlib.contextmanager
def expect_gap(reason: str | None, match: str | None, flaky: bool = False):
    """Run a block that may hit a *documented* gap (an engine/catalog limitation or bug).

    Behaviour:

    * ``reason is None`` (no gap for this case): the block runs normally.
    * the block raises and ``"<ExceptionType>: <message>"`` matches ``match``
      (regex, case-insensitive): the gap reproduced ->
      ``fail`` mode: :class:`KnownGapError` "[known gap] <reason>" chained to the original
      error (shown as FAILED with full traceback); ``xfail`` mode: ``pytest.xfail(reason)``.
    * the block raises anything else: the original exception propagates unchanged - a
      new, unexpected failure.
    * the block does NOT raise although a gap is documented: the gap is fixed ->
      :class:`GapFixedError` "[gap fixed] <reason>" in both modes (remove the entry).
      With ``flaky=True`` the test passes, the summary lists it as "not reproduced" and the
      report keeps it as a gap (reporting._classify); flaky_history.json counts the runs.

    ``match`` must name the error precisely: ``None`` or a pattern that also matches one of
    :data:`GENERIC_ERRORS` is refused (ValueError) - that is exactly what lets gaps hide bugs.

    Args:
        reason: Why the gap exists (None: no gap for this case).
        match: Regex the error must match.
        flaky: Intermittent: a passing run is not "fixed".

    Yields:
        Nothing; the checked block runs inside.

    Raises:
        KnownGapError: the documented gap reproduced (``fail`` mode).
        GapFixedError: the documented, not intermittent gap did not occur.
    """
    if reason is None:
        yield
        return
    _require_specific(match, reason)
    _require_severity(reason)
    test_id = os.environ.get("PYTEST_CURRENT_TEST", "?").rsplit(" ", 1)[0]
    try:
        yield
    except Exception as exc:  # re-raised below unless it is the expected one
        text = f"{type(exc).__name__}: {exc}"
        if match is not None and not re.search(match, text, re.IGNORECASE | re.DOTALL):
            raise  # a different error than documented -> real, unlabelled failure
        _KNOWN_GAP_HITS.append((test_id, reason))
        if flaky:
            _FLAKY_SEEN.append((test_id, reason))
        if KNOWN_GAPS_MODE == "xfail":
            pytest.xfail(reason)
        raise KnownGapError(f"[known gap] {reason}") from exc
    if flaky:
        _FLAKY_NOT_SEEN.append((test_id, reason))
        return
    _FIXED_GAPS.append((test_id, reason))
    raise GapFixedError(f"[gap fixed] no longer reproduces - remove the KNOWN_GAPS entry: {reason}")


#: Failures that can hit ANY test on the signing warehouse, recognised centrally instead of wrapping
#: every such test in expect_gap. Entry: (nodeid substring, exception regex, frames that
#: must appear in the traceback, cause id in docs/gaps.toml). Requiring a PyIceberg frame plus
#: s3fs (the only path that showed the problem; Spark's signer never did) keeps an authorization
#: error from anywhere else on demo-signing reported as a new failure. The PyIceberg frame is
#: generic because manifest writes reach s3fs via pyiceberg/avro/file.py, not pyiceberg/io/fsspec.py.
# fmt: off
FLAKY_GAPS = [
    ("signing", r"^PermissionError: (Forbidden|Access Denied)", ("site-packages/pyiceberg/", "s3fs/core.py"), "pyiceberg-signer-race"),
]

#: Gaps of one catalog that hit EVERY test touching the affected engine (the engine cannot
#: even connect, or the catalog lacks a whole feature), recognised centrally like FLAKY_GAPS:
#: (catalog ("a|b" = either), nodeid substring, exception regex, frames that must appear in
#: the traceback, cause id). The nodeid part names the engines/files the gap belongs to
#: ("a|b" = either), so an engine's failure in shared setup cannot be booked as a gap of
#: ANOTHER engine's test (that hid 258 type checks once). Exact messages only - anything else
#: on that catalog is still a new failure.
CATALOG_GAPS = [
    # Spark/Flink resolve "ns.table.snapshots" by first trying table "snapshots" in namespace
    # "ns.table"; they fall back to the metadata table only on NoSuchTable.
    ("unity", "spark|flink", r"NoSuchNamespaceException: Schema not found: lab\.test_\w+\.\w+", (), "unity-nosuchnamespace-for-metadata-tables"),
    ("unity", "pg_lake|pglake", r"pg_lake commits writes through POST /v1/\{prefix\}/transactions/commit", (), "pglake-ignores-refused-commit"),
    # Only without help (LAB_NO_HELP=1, test_stock_connection): the lab's workaround
    # (engines._add_content_sha256) is off while PyIceberg/Polars/Daft is under test.
    ("rustfs", "pyiceberg|polars|daft|dataframes", r"missing header: x-amz-content-sha256", ("pyiceberg/catalog/rest",), "pyiceberg-sigv4-content-sha256"),
    ("rustfs", "duckdb", r"Could not parse AWS service from host", (), "duckdb-sigv4-aws-host"),
    # No option to leave the prefix out (catalog_name must not be empty): pg_lake cannot
    # connect at all.
    ("reference", "pg_lake|pglake", r"No route for request: [A-Z]+ v1/postgres/", (), "pglake-requires-rest-prefix"),
    # node "v3": -v3 parameters and test_v3_* tests
    ("rustfs", "v3", r"unsupported Iceberg table format-version: 3", (), "rustfs-no-v3"),
    ("rustfs", "v3", r"Failed to create transaction", (), "rustfs-no-v3"),
    # drop_table: first seen behind the PyIceberg header gap
    ("rustfs", "flink|drop_table", r"purgeRequested=true is not supported", (), "rustfs-drop-purge-refused"),
    ("rustfs", "drop_table", r"Failed to drop table|S3Exception: Forbidden \(Service: S3, Status Code: 403", (), "rustfs-drop-purge-refused"),
    ("rustfs", "flink", r"row-level snapshot operation requires a parent snapshot", (), "rustfs-row-level-needs-parent"),
]
# fmt: on


def _traceback_files(excinfo) -> list[str]:
    """File paths of all frames of an exception, including chained causes/contexts.

    Uses the exception objects, not the formatted report - so the check does not depend on
    ``--tb=line/short/long``.

    Args:
        excinfo: pytest ExceptionInfo.

    Returns:
        ``<file>::<qualname>`` of every frame.
    """
    files, exc, seen = [], excinfo.value, set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        tb = exc.__traceback__
        while tb is not None:
            code = tb.tb_frame.f_code
            files.append(f"{code.co_filename}::{getattr(code, 'co_qualname', code.co_name)}")
            tb = tb.tb_next
        exc = exc.__cause__ or exc.__context__
    return files


def central_gap(nodeid: str, excinfo, catalog: str) -> str | None:
    """The reason of the first :data:`FLAKY_GAPS` / :data:`CATALOG_GAPS` rule a failure matches.

    A rule matches when the test id contains one of its node parts, the error matches its regex
    and every required frame appears in the traceback ("a|b": either). The hit is recorded.

    Args:
        nodeid: The test's node id.
        excinfo: pytest ExceptionInfo of the failure.
        catalog: Catalog profile under test (CATALOG_GAPS apply only on their catalog).

    Returns:
        The reason, or None.
    """
    text = f"{excinfo.type.__name__}: {excinfo.value}"
    files = _traceback_files(excinfo)
    entries = list(FLAKY_GAPS) + [
        (node, regex, frames, cid)
        for cat, node, regex, frames, cid in CATALOG_GAPS
        if catalog in cat.split("|")
    ]
    from iceberg_lab.lab.gap_register import cause

    for node_part, regex, frames, cid in entries:
        reason = cause(cid).reason
        if (
            any(alt in nodeid for alt in node_part.split("|"))
            and re.search(regex, text)
            and all(any(alt in path for alt in f.split("|") for path in files) for f in frames)
        ):
            _KNOWN_GAP_HITS.append((nodeid, reason))
            return reason
    return None


def write_summary(terminalreporter) -> None:
    """Summarise failures split into *new* errors and *known gaps* at the end of the run
    (called by conftest.pytest_terminal_summary).

    Args:
        terminalreporter: pytest's terminal reporter.
    """
    # "error" = failed in setup/teardown (e.g. an engine fixture): those count as failures too.
    failed = terminalreporter.stats.get("failed", []) + terminalreporter.stats.get("error", [])
    known_ids = {tid for tid, _ in _KNOWN_GAP_HITS}
    fixed_ids = {tid for tid, _ in _FIXED_GAPS}
    new_errors = [r.nodeid for r in failed if r.nodeid not in known_ids | fixed_ids]
    tr = terminalreporter
    tr.section("iceberg lab: failure overview")
    tr.write_line(f"known-gap mode: {KNOWN_GAPS_MODE}  (set KNOWN_GAPS=xfail for a green run)")
    tr.write_line(f"NEW / unexpected failures: {len(new_errors)}")
    for nodeid in new_errors[:50]:
        tr.write_line(f"  {nodeid}")
    if len(new_errors) > 50:
        tr.write_line(f"  ... and {len(new_errors) - 50} more")
    by_reason: dict[str, int] = {}
    for _, reason in _KNOWN_GAP_HITS:
        by_reason[reason] = by_reason.get(reason, 0) + 1
    tr.write_line(f"known gaps reproduced: {len(_KNOWN_GAP_HITS)} tests, {len(by_reason)} distinct")
    for reason, n in sorted(by_reason.items(), key=lambda kv: -kv[1]):
        tr.write_line(f"  {n:4d} x {reason}")
    if _FIXED_GAPS:
        tr.write_line(
            f"FIXED gaps - documented but no longer failing, remove the entries: {len(_FIXED_GAPS)}"
        )
        for tid, reason in _FIXED_GAPS:
            tr.write_line(f"  {tid}: {reason}")
    if _FLAKY_NOT_SEEN:
        tr.write_line(f"intermittent gaps not reproduced this run: {len(_FLAKY_NOT_SEEN)}")
        for tid, reason in _FLAKY_NOT_SEEN:
            tr.write_line(f"  {tid}: {reason}")
    stale = _update_flaky_history()
    if stale:
        tr.write_line(
            f"PROBABLY FIXED intermittent gaps - not seen in {FLAKY_FIXED_AFTER}+ runs in a row, "
            f"check and remove the entries: {len(stale)}"
        )
        for tid, reason, n in stale:
            tr.write_line(f"  {tid}: {reason} (not seen in {n} runs)")


def flaky_not_seen(test_id: str) -> str | None:
    """Reason of the intermittent gap that did not occur in this test (this run).

    Args:
        test_id: Node id of the test.

    Returns:
        The gap's reason, or None.
    """
    return next((reason for tid, reason in _FLAKY_NOT_SEEN if tid == test_id), None)


def _update_flaky_history() -> list[tuple[str, str, int]]:
    """Count, per intermittent gap and test, the runs in a row in which it did not occur.

    Only gaps declared with ``expect_gap(..., flaky=True)`` are tracked (the central
    FLAKY_GAPS have no per-test "did not occur" signal). Returns the entries at or above
    :data:`FLAKY_FIXED_AFTER`.

    Returns:
        ``(test, reason, runs without occurrence)`` at or above the threshold.
    """
    import json

    from iceberg_lab.lab import reporting

    if not (_FLAKY_SEEN or _FLAKY_NOT_SEEN):
        return []
    path = reporting.REPORT_DIR / "flaky_history.json"
    # reports/ is not kept: without it, continue from the copy in results/ (export_reference).
    from iceberg_lab.config import PROFILE, WITH_HELP

    name = PROFILE.name + ("" if WITH_HELP else "-nohelp")
    saved = Path(__file__).resolve().parents[3] / "results" / f"{name}.flaky.json"
    history = {}
    for source in (path, saved):
        try:
            history = json.loads(source.read_text())
            break
        except (OSError, ValueError):
            continue
    # A reason the register no longer has (cause renamed or reworded) would never be seen again
    # and end up "probably fixed" - drop it.
    from iceberg_lab.lab import gap_register

    reasons = {c.reason for c in gap_register.register().values()}
    history = {k: n for k, n in history.items() if k.split(" :: ", 1)[-1] in reasons}
    for tid, reason in _FLAKY_SEEN:
        history[f"{tid} :: {reason}"] = 0
    for tid, reason in _FLAKY_NOT_SEEN:
        key = f"{tid} :: {reason}"
        history[key] = history.get(key, 0) + 1
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history, indent=1, ensure_ascii=False, sort_keys=True))
    return [
        (*key.split(" :: ", 1), n)
        for key, n in sorted(history.items())
        if n >= FLAKY_FIXED_AFTER and key.split(" :: ", 1)[0] in {t for t, _ in _FLAKY_NOT_SEEN}
    ]
