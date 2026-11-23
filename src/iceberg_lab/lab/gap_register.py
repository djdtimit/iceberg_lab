"""Gap register: every known gap names its cause, and each cause is described once.

``docs/gaps.toml`` holds one entry per cause: what goes wrong (``summary``), how bad it is
(``severity``), with which versions it was last seen (``seen_at``), and where it is reported
(``bug_report`` in docs/bug-reports/, ``upstream`` issue). The tests keep their own selectors -
which engine, operation, format version or case a gap applies to - and point to the cause by
id with one shared type, :class:`Gap`:

    KNOWN_GAPS = gap_table(("engine", "op", "fv"), [
        ("pyiceberg", "*", "3", r"Cannot write manifest for table version: 3", "pyiceberg-v3-data-write"),
    ])
    with expect(find_gap(KNOWN_GAPS, engine=engine, op=op, fv=fv)):
        ...

Selectors are shell-style globs (``fnmatch``), matched against the values the test passes;
all entries that match a case are alternatives, and the first whose regex matches the error
names the cause. The register is validated when it is loaded (pydantic): every
regex compiles and names its error precisely, a severe-sounding summary carries a severity,
every ``seen_at`` names a known component, every bug report exists. A test that names an
unknown cause fails at import.

``seen_at`` makes upgrades visible: the self-test ``test_gap_register`` compares it with the
versions the lab runs now and lists the causes to check again after a version change.
``scripts/gap_register.py confirm`` moves ``seen_at`` forward for the causes a full run
reproduced; ``scripts/gap_register.py findings`` writes the gap list in docs/findings.md.
"""

from __future__ import annotations

import fnmatch
import functools
import json
import re
import tomllib
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from iceberg_lab.lab import gaps as _gaps

ROOT = Path(__file__).resolve().parents[3]
REGISTER_FILE = ROOT / "docs" / "gaps.toml"
BUG_REPORTS = ROOT / "docs" / "bug-reports"

#: Severity of a cause (the reason starts with it, see ``gaps.SEVERITIES``); none: a limitation.
Severity = Literal["DATA LOSS", "DATA CORRUPTION", "INCONSISTENT", "WRONG RESULT", "SECURITY"]

#: Components a cause can belong to, and where the lab's current version of each comes from:
#: ``("package", <distribution>)`` or ``("image", <compose service>)``. Spark's version also
#: names the Iceberg runtime (``engines.ICEBERG_VERSION``).
COMPONENTS: dict[str, tuple[str, str]] = {
    "lakekeeper": ("image", "lakekeeper"),
    "polaris": ("image", "polaris"),
    "unity": ("image", "unitycatalog"),
    "rustfs": ("image", "rustfs"),
    "reference": ("image", "reference"),
    "keycloak": ("image", "keycloak"),
    "openfga": ("image", "openfga"),
    "pyiceberg": ("package", "pyiceberg"),
    "pyarrow": ("package", "pyarrow"),
    "polars": ("package", "polars"),
    "daft": ("package", "daft"),
    "duckdb": ("package", "duckdb"),
    "spark": ("package", "pyspark"),
    "trino": ("image", "trino"),
    "flink": ("image", "flink"),
    "pg_lake": ("image", "pg-lake"),
}


#: Owners of causes shared by several catalogs or engines (id prefix ``catalogs-``/``engines-``).
GROUPS = ("catalogs", "engines")
#: Order of the owners in the register file and in docs/findings.md.
OWNERS = (*list(COMPONENTS)[:7], "catalogs", *list(COMPONENTS)[7:], "engines")


class Cause(BaseModel):
    """One cause of known gaps (an entry of ``docs/gaps.toml``)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    #: What goes wrong, without the severity prefix.
    summary: str = Field(min_length=10)
    severity: Severity | None = None
    #: Component -> version with which the cause was last reproduced. The id starts with the
    #: owning component (``pglake-`` for pg_lake) or ``catalogs-``/``engines-``.
    seen_at: dict[str, str] = Field(min_length=1)
    #: Intermittent: a run without it does not mean it is fixed.
    flaky: bool = False
    #: File in docs/bug-reports/.
    bug_report: str | None = None
    #: Upstream issue (``owner/repo#n``) or URL.
    upstream: str | None = None

    @field_validator("seen_at")
    @classmethod
    def _known_components(cls, v: dict[str, str]) -> dict[str, str]:
        unknown = sorted(set(v) - set(COMPONENTS))
        if unknown:
            raise ValueError(f"unknown component(s) {unknown} - known: {sorted(COMPONENTS)}")
        empty = [k for k, ver in v.items() if not ver.strip()]
        if empty:
            raise ValueError(f"no version for {empty}")
        return v

    @field_validator("bug_report")
    @classmethod
    def _report_exists(cls, v: str | None) -> str | None:
        if v is not None and not (BUG_REPORTS / v).is_file():
            raise ValueError(f"docs/bug-reports/{v} does not exist")
        return v

    @model_validator(mode="after")
    def _severity_fits(self) -> Cause:
        if any(self.summary.startswith(p) for p in _gaps.SEVERITIES):
            raise ValueError("write the severity in 'severity', not in front of the summary")
        _gaps._require_severity(self.reason)
        return self

    @model_validator(mode="after")
    def _id_names_owner(self) -> Cause:
        prefix = self.id.split("-", 1)[0]
        if prefix in GROUPS:
            return self
        owners = {c.replace("_", ""): c for c in self.seen_at}
        if prefix not in owners:
            raise ValueError(
                f"id {self.id!r} must start with its owner: one of {sorted(owners)} (a component "
                f"in seen_at) or {sorted(GROUPS)} (shared by several)"
            )
        return self

    @property
    def owner(self) -> str:
        """Where the cause belongs: the component its id starts with, or a shared group."""
        prefix = self.id.split("-", 1)[0]
        if prefix in GROUPS:
            return prefix
        return next(c for c in self.seen_at if c.replace("_", "") == prefix)

    @property
    def reason(self) -> str:
        """The text a known gap reports: ``"<SEVERITY>: <summary>"`` or the summary."""
        return f"{self.severity}: {self.summary}" if self.severity else self.summary


def load(path: Path = REGISTER_FILE) -> dict[str, Cause]:
    """Read and validate a register file.

    Args:
        path: The TOML file.

    Returns:
        ``{id: Cause}`` in file order.
    """
    data = tomllib.loads(path.read_text())
    return {cid: Cause(id=cid, **entry) for cid, entry in data.items()}


@functools.cache
def register() -> dict[str, Cause]:
    """The lab's register (``docs/gaps.toml``), read once.

    Returns:
        ``{id: Cause}``.
    """
    return load()


def cause(cid: str) -> Cause:
    """The cause with this id.

    Args:
        cid: Cause id.

    Returns:
        The cause.

    Raises:
        ValueError: the cause is not in docs/gaps.toml.
    """
    try:
        return register()[cid]
    except KeyError:
        raise ValueError(f"unknown gap cause {cid!r} - add it to docs/gaps.toml") from None


class Gap(BaseModel):
    """A known gap in a test: where it applies, the exact error, and its cause."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Id in docs/gaps.toml.
    cause: str
    #: Regex the error ``"<ExceptionType>: <message>"`` must match (case-insensitive).
    match: str
    #: Selector -> glob; a selector the test does not pass is an error (see :func:`find_gap`).
    where: dict[str, str] = {}
    #: Intermittent here, although the cause is not in general.
    flaky: bool = False

    @field_validator("cause")
    @classmethod
    def _cause_known(cls, v: str) -> str:
        cause(v)
        return v

    @model_validator(mode="after")
    def _match_specific(self) -> Gap:
        try:
            re.compile(self.match)
        except re.error as e:
            raise ValueError(f"gap regex {self.match!r} does not compile: {e}") from None
        _gaps._require_specific(self.match, self.reason)
        return self

    @property
    def reason(self) -> str:
        """The reported text (from the cause)."""
        return cause(self.cause).reason

    @property
    def is_flaky(self) -> bool:
        """Intermittent here or in general."""
        return self.flaky or cause(self.cause).flaky

    def applies(self, dims: dict[str, object]) -> bool:
        """Whether every selector matches the test's values.

        Args:
            dims: The test's values, e.g. ``{"engine": "duckdb", "fv": 3}``.

        Returns:
            True if all selectors match.

        Raises:
            ValueError: the test does not pass every selector of the gap.
        """
        missing = set(self.where) - set(dims)
        if missing:
            raise ValueError(f"gap selector(s) {sorted(missing)} not passed by the test: {self}")
        return all(fnmatch.fnmatchcase(str(dims[k]), p) for k, p in self.where.items())


def gap(cause: str, match: str, *, flaky: bool = False, **where: object) -> Gap:
    """One known gap.

    Args:
        cause: Cause id (docs/gaps.toml).
        match: Regex of the exact error.
        flaky: Intermittent in this test.
        **where: Selector globs (values are turned into strings).

    Returns:
        The gap.
    """
    return Gap(cause=cause, match=match, flaky=flaky, where={k: str(v) for k, v in where.items()})


def gap_table(fields: tuple[str, ...], rows: Iterable[tuple]) -> list[Gap]:
    """Gaps written as table rows: ``(*selectors, match, cause[, flaky])``.

    Args:
        fields: Names of the selector columns.
        rows: The rows.

    Returns:
        The gaps, in row order.

    Raises:
        ValueError: a row has the wrong number of fields.
    """
    out = []
    n = len(fields)
    for row in rows:
        if len(row) not in (n + 2, n + 3):
            raise ValueError(f"gap row needs {fields} + match + cause [+ flaky]: {row!r}")
        flaky = bool(row[n + 2]) if len(row) == n + 3 else False
        out.append(gap(row[n + 1], row[n], flaky=flaky, **dict(zip(fields, row[:n], strict=True))))
    return out


def find_gap(gaps: Iterable[Gap], **dims: object) -> Gap | tuple[Gap, ...] | None:
    """The gaps whose selectors all match.

    Several rows for the same case are alternatives (e.g. a catalog-specific error next to the
    general one): :func:`expect` books the failure on the first row whose regex matches it.

    Args:
        gaps: The test file's gaps.
        **dims: The test's values.

    Returns:
        The gap, the alternatives (in row order) when several rows apply, or None.
    """
    hits = tuple(g for g in gaps if g.applies(dims))
    if not hits:
        return None
    return hits[0] if len(hits) == 1 else hits


def matching(g: Gap | tuple[Gap, ...] | None, error: BaseException) -> Gap | None:
    """The gap (of a :func:`find_gap` result) whose regex names this error.

    For tests that book an error as a gap only when it is the documented one.

    Args:
        g: The gap, the alternatives, or None.
        error: The exception raised.

    Returns:
        The matching gap, or None.
    """
    alternatives = g if isinstance(g, tuple) else (g,) if g is not None else ()
    text = f"{type(error).__name__}: {error}"
    return next(
        (a for a in alternatives if re.search(a.match, text, re.IGNORECASE | re.DOTALL)), None
    )


def expect(g: Gap | tuple[Gap, ...] | None):
    """``expect_gap`` for a gap, alternative gaps (see :func:`find_gap`) or none.

    Args:
        g: The gap, the alternatives, or None (no gap: the block runs normally).

    Returns:
        The context manager.
    """
    if g is None:
        return _gaps.expect_gap(None, None)
    if isinstance(g, tuple):
        return _expect_one_of(g)
    return _gaps.expect_gap(g.reason, g.match, flaky=g.is_flaky)


@contextmanager
def _expect_one_of(alternatives: tuple[Gap, ...]) -> Iterator[None]:
    """``expect`` for alternatives: the error picks the row; no error: the gap is fixed.

    Args:
        alternatives: The rows that apply, in order.

    Yields:
        Nothing; the checked block runs inside.
    """
    try:
        yield
    except Exception as exc:  # booked on the matching row; none matches: a new failure
        g = matching(alternatives, exc) or alternatives[0]
        with expect(g):
            raise
    else:
        # Nothing failed: fixed, unless an alternative is intermittent.
        g = next((a for a in alternatives if a.is_flaky), alternatives[0])
        with expect(g):
            pass


def expect_cause(cid: str, match: str, *, flaky: bool = False):
    """``expect_gap`` for a single gap without a table.

    Args:
        cid: Cause id.
        match: Regex of the exact error.
        flaky: Intermittent in this test.

    Returns:
        The context manager.
    """
    return expect(gap(cid, match, flaky=flaky))


# =========================================================================== versions


def _image_version(ref: str) -> str:
    """Version of an image reference: its tag without a leading ``v``, or the digest's start.

    Args:
        ref: ``repo:tag`` or ``repo@sha256:...``.

    Returns:
        The version.
    """
    if "@" in ref:
        return ref.split("@", 1)[1][:19]  # "sha256:" + 12 hex
    tag = ref.rsplit(":", 1)[1] if ":" in ref.rsplit("/", 1)[-1] else "latest"
    return tag.removeprefix("v")


def _iceberg_java() -> str:
    """The Iceberg runtime Spark loads.

    Returns:
        ``engines.ICEBERG_VERSION``, read from the source ("?" if not found).
    """
    src = (ROOT / "src" / "iceberg_lab" / "engines.py").read_text()
    m = re.search(r'^ICEBERG_VERSION = "([^"]+)"', src, re.MULTILINE)
    return m.group(1) if m else "?"


def versions_of(manifest: dict) -> dict[str, str]:
    """Component versions recorded in a run manifest (``lab/manifest.py``).

    Args:
        manifest: The manifest (``packages`` and ``images_declared`` are used).

    Returns:
        ``{component: version}`` for every component the manifest names.
    """
    pkgs, images = manifest.get("packages", {}), manifest.get("images_declared", {})
    out = {}
    for comp, (kind, name) in COMPONENTS.items():
        if kind == "package" and name in pkgs:
            out[comp] = pkgs[name]
        elif kind == "image" and name in images:
            out[comp] = _image_version(images[name])
    if "spark" in out:
        out["spark"] += f"+iceberg{_iceberg_java()}"
    return out


def current_versions() -> dict[str, str]:
    """Versions the lab runs now (installed packages, images of the compose file).

    Returns:
        ``{component: version}``.
    """
    from iceberg_lab.lab import manifest

    return versions_of(
        {"packages": manifest.packages(), "images_declared": manifest.declared_images()}
    )


def outdated(
    causes: Iterable[Cause], versions: dict[str, str]
) -> list[tuple[Cause, str, str, str]]:
    """Causes last seen with another version than the lab runs now - to check again.

    Args:
        causes: The causes.
        versions: Current versions (:func:`current_versions`).

    Returns:
        ``(cause, component, seen_at, now)`` per differing component.
    """
    return [
        (c, comp, seen, versions[comp])
        for c in causes
        for comp, seen in c.seen_at.items()
        if comp in versions and versions[comp] != seen
    ]


# =========================================================================== reference results


def hits(results_dir: Path) -> dict[str, dict[str, dict[str, int]]]:
    """How often each reason was reported, per reference and status (``gap``/``fixed``).

    Args:
        results_dir: ``results/`` with ``<name>.jsonl``.

    Returns:
        ``{reason: {reference: {status: count}}}``.
    """
    out: dict[str, dict[str, dict[str, int]]] = {}
    for path in sorted(results_dir.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            row = json.loads(line)
            if row["status"] in ("gap", "fixed"):
                per = out.setdefault(row["detail"], {}).setdefault(path.stem, {})
                per[row["status"]] = per.get(row["status"], 0) + 1
    return out


# =========================================================================== findings.md

FINDINGS_START = "<!-- gap-register:start (generated by scripts/gap_register.py findings) -->"
FINDINGS_END = "<!-- gap-register:end -->"


def findings_markdown(causes: dict[str, Cause], hit: dict[str, dict[str, dict[str, int]]]) -> str:
    """The generated gap list of docs/findings.md: per component, severe causes first.

    Args:
        causes: The register.
        hit: :func:`hits` of the reference results.

    Returns:
        Markdown between the markers (markers included).
    """
    lines = [FINDINGS_START, ""]
    by_owner: dict[str, list[Cause]] = {}
    for c in causes.values():
        by_owner.setdefault(c.owner, []).append(c)
    for comp in OWNERS:
        group = by_owner.get(comp)
        if not group:
            continue
        severe = sum(1 for c in group if c.severity)
        n = len(group)
        head = f"**{comp}** ({n} cause{'s' if n != 1 else ''}"
        head += (f", {severe} severe" if severe else "") + ")"
        lines += [head, ""]
        for c in sorted(group, key=lambda c: (c.severity is None, c.severity or "", c.id)):
            text = f"- **{c.severity}** {c.summary}" if c.severity else f"- {c.summary}"
            meta = [", ".join(f"{k} {v}" for k, v in c.seen_at.items())]
            if c.flaky:
                meta.append("intermittent")
            if c.bug_report:
                meta.append(f"[bug report](bug-reports/{c.bug_report})")
            if c.upstream:
                meta.append(f"upstream {c.upstream}")
            where = hit.get(c.reason, {})
            if where:
                meta.append(
                    "hit in " + ", ".join(f"{r} ({sum(s.values())})" for r, s in where.items())
                )
            lines.append(f"{text} — {' · '.join(meta)} `{c.id}`")
        lines.append("")
    lines.append(FINDINGS_END)
    return "\n".join(lines)


def replace_block(text: str, block: str) -> str:
    """Put the generated block into a document (between the markers, which must exist).

    Args:
        text: The document.
        block: :func:`findings_markdown`.

    Returns:
        The new document.
    """
    start, end = text.index(FINDINGS_START), text.index(FINDINGS_END) + len(FINDINGS_END)
    return text[:start] + block + text[end:]


# =========================================================================== writing


def dump(causes: dict[str, Cause]) -> str:
    """The register as TOML, grouped by owner component (the file's canonical form).

    Args:
        causes: The register.

    Returns:
        The file content.
    """
    s = json.dumps
    out = [
        "# Gap register: one entry per cause of a known gap (src/iceberg_lab/lab/gap_register.py).",
        "# Tests point here by id. Fields: summary, severity (DATA LOSS | DATA CORRUPTION |",
        "# INCONSISTENT | WRONG RESULT | SECURITY), seen_at (component -> version last reproduced),",
        "# flaky, bug_report (docs/bug-reports/), upstream. The id starts with the owning component",
        "# (pglake- for pg_lake) or catalogs-/engines- for a cause shared by several.",
        "# `uv run python scripts/gap_register.py confirm` moves seen_at forward after a full run.",
    ]
    by_owner: dict[str, list[Cause]] = {}
    for c in causes.values():
        by_owner.setdefault(c.owner, []).append(c)
    for comp in OWNERS:
        if comp not in by_owner:
            continue
        out += ["", f"# {'=' * 30} {comp}"]
        for c in sorted(by_owner[comp], key=lambda c: c.id):
            out += ["", f"[{c.id}]", f"summary = {s(c.summary, ensure_ascii=False)}"]
            if c.severity:
                out.append(f"severity = {s(c.severity)}")
            seen = ", ".join(f"{k} = {s(v)}" for k, v in c.seen_at.items())
            out.append(f"seen_at = {{ {seen} }}")
            if c.flaky:
                out.append("flaky = true")
            if c.bug_report:
                out.append(f"bug_report = {s(c.bug_report)}")
            if c.upstream:
                out.append(f"upstream = {s(c.upstream)}")
    return "\n".join(out) + "\n"
