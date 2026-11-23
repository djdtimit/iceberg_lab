"""Schemas of the lab's data files, checked with pydantic where they are written or read.

* :class:`ResultRow` - one line of a reference in ``results/<name>.jsonl``
* :class:`RunManifest` - ``manifest.json`` of a run (``lab/manifest.py``), also exported as
  ``results/<name>.manifest.json``
* :data:`Classification` - ``docs/api/engine-classified.toml``

The known-gap register has its own models (``lab/gap_register.py``).
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, TypeAdapter

#: Result of one test case (``reporting._classify``); "self" (own result verified elsewhere)
#: never reaches a reference.
Status = Literal["pass", "gap", "new", "fixed", "na", "blocked"]


class ResultRow(BaseModel):
    """One test of a reference: its id, its status and why (reason, error or skip text)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    test: str = Field(pattern=r"^tests/\S+\.py::\S")
    status: Status
    detail: str = ""


class GitState(BaseModel):
    """The commit a run was made with."""

    model_config = ConfigDict(extra="forbid")

    commit: str
    #: The work tree had uncommitted changes.
    dirty: bool


class RunManifest(BaseModel):
    """What a run's results were produced with (best effort: missing tools leave fields empty)."""

    model_config = ConfigDict(extra="forbid")

    run: str
    started: str
    finished: str
    catalog: str
    with_help: bool
    git: GitState
    packages: dict[str, str]
    duckdb_extensions: dict[str, str]
    images_declared: dict[str, str]
    images_running: dict[str, str]


#: ``engine -> {API name: "ignored: <reason>" | "untested: <what>"}``.
Classification = TypeAdapter(
    dict[
        str,
        dict[str, Annotated[str, StringConstraints(pattern=r"^(ignored|untested): \S")]],
    ]
)
