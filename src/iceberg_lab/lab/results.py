"""Dependent results: read matrices depend on producer tests.

``skip_if_not_produced`` tells "producer did not run here" (no record) from "producer failed"
(blocked); ``keep_if_content_ok`` still hands a producer's table to the readers when it failed
only after its content was right.
"""

from __future__ import annotations

import contextlib

import pytest

#: Skip reason prefix of a dependent read test whose producing test did not run in this
#: session (e.g. ``-k reader``). Not a result at all: reporting.py does not record it, so a
#: targeted re-run cannot overwrite the real result of the last full run with "blocked".
NOT_RUN = "[producer not run in this session]"


def skip_if_not_produced(results: dict, key: tuple, producer: str) -> tuple:
    """Return ``results[key]`` for a dependent read test, or skip.

    Producers put ``key`` into ``results`` (value None) as their first step, so "absent" means
    the producer did not run here (no result at all), None that it ran without success
    (blocked).

    Args:
        results: The producer's result dict.
        key: Key of the state to read.
        producer: Name of the producing test (for the skip reason).

    Returns:
        The produced value, e.g. ``(namespace, table)``.
    """
    label = " ".join(f"v{k}" if isinstance(k, int) else str(k) for k in key)
    if key not in results:
        pytest.skip(f"{NOT_RUN} {label} ({producer})")
    if results[key] is None:
        pytest.skip(f"{label} did not succeed in {producer}")
    return results[key]


@contextlib.contextmanager
def keep_if_content_ok(results: dict, key: tuple, produced, content_ok):
    """Producer block of a read matrix: book ``produced`` for the dependent reads also when the
    block fails AFTER the table content was right.

    A gap about a property (no delete file written, files left in storage, wrong partition
    values) must not hide whether OTHER engines can read the table. On a failure (not a skip)
    ``content_ok()`` - the writing engine's own read of the table compared with the expected
    rows - decides; an exception there counts as "not ok". Success is booked by the caller.

    Args:
        results: The producer's result dict.
        key: Key of the state.
        produced: Value to book, e.g. ``(namespace, table)``.
        content_ok: Callable: True if the table's content is right.

    Yields:
        Nothing; the producer block runs inside.
    """
    try:
        yield
    except BaseException as exc:
        if not isinstance(exc, pytest.skip.Exception):
            try:
                ok = bool(content_ok())
            except Exception:  # noqa: BLE001 - table missing/unreadable: nothing to read
                ok = False
            if ok:
                results[key] = produced
        raise
