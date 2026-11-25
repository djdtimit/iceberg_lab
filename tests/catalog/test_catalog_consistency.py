"""Catalog consistency (base tests on catalog level, raw Iceberg REST - no engine involved).

1. Commit validation: the catalog is the last line of defence against a client sending a commit
   that breaks the table. Each invalid commit below must be refused (4xx) and leave the table
   unchanged. (An issue elsewhere - apache/gravitino#14457 - shows catalogs that accept such
   commits; the engines in the other tests only ever send valid ones.)

       incompatible_type   change an existing column's type long -> string
       reuse_field_id      add a column with the id of a dropped column but another type
       spec_missing_field  partition spec on a field id the schema does not have
       unknown_schema      set-current-schema to an id the table does not have
       unknown_snapshot    set main to a snapshot id the table does not have

2. Lost updates: many clients commit at the same time; every commit the catalog confirms must be
   in the table afterwards (apache/iceberg#18758 shows a REST implementation that confirms and
   loses one). Table appends through the table endpoint and namespace property updates (fixed in
   Lakekeeper 0.15.1, a classic read-modify-write in the server).

3. Re-create at a dropped location: table A is dropped with purge and B is created where A was
   (same name, or A's location given explicitly). A's asynchronous purge must not delete B's
   files - the catalog refuses B, gives it a location of its own, or B stays complete.

4. Metadata history: after commits, the files the table's ``metadata-log`` lists must still
   exist - the spec deletes them only with ``write.metadata.delete-after-commit.enabled``
   (default off). Readers and committers that hold an older metadata file, time travel and
   rollback by metadata file depend on them.

Uses the lab client only to create the tables; every checked call is a raw REST request.
"""

from __future__ import annotations

import copy
import random
import threading
import time
import uuid

import pyarrow as pa
import pytest
import requests

from iceberg_lab.config import (
    PROFILE,
    SETTINGS,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
    rest_base,
)
from iceberg_lab.engines import pyiceberg_catalog

from conftest import expect, find_gap, gap_table

SCHEMA = pa.schema([("id", pa.int64()), ("name", pa.string()), ("gone", pa.int64())])

# Filled from observed results only: (catalog, case, regex, cause[, flaky]).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "case"), [
    *((cat, case, r"invalid commit answered 200", "catalogs-no-schema-compat-check")
      for cat in ("lakekeeper", "polaris", "unity", "gravitino", "reference") for case in ("incompatible_type", "reuse_field_id")),
    # About 1 round in 3 loses one of 8 concurrent updates (confirmed, then gone) - the race
    # Lakekeeper fixed in 0.15.1; the test repeats the round until it shows.
    ("unity", "lost_namespace_property", r"confirmed but lost", "unity-loses-namespace-property"),
    # Through Gravitino the backend's refusals come back as 500 (RustFS: 400 for the commit,
    # 406 for a purge; Unity: 400 for a location in its managed storage).
    *(("gravitino_rustfs", c, r"invalid commit answered 500: .*Malformed request: schema field", "gravitino-rest-backend-client-error-500")
      for c in ("incompatible_type", "reuse_field_id")),
    *(("gravitino_rustfs", f"recreate:{h}", r"drop with purge answered 500", "gravitino-rest-backend-client-error-500")
      for h in ("same_name", "explicit_location")),
    ("gravitino_unity", "recreate:explicit_location", r"server error instead of a refusal: 500 .*contains managed storage prefix", "gravitino-rest-backend-client-error-500"),
    # The server retries a conflicting transaction itself, finds "Nothing to commit" and
    # answers 204: the client's changes are dropped (table endpoint: correct 409).
    ("reference", "lost_transaction_commit", r"confirmed but lost: writers", "reference-tx-commit-loses-updates"),
    ("rustfs", "metadata_log", r"metadata-log lists \d+ files?, missing: \d+", "rustfs-deletes-metadata-log"),
    *(("unity", case, r"invalid commit answered 500: .*ValidationException", "unity-invalid-commit-500")
      for case in ("spec_missing_field", "unknown_snapshot")),
])
# fmt: on


def _gap(case):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        case: Gap case.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, case=case))


def _base() -> str:
    """REST base URL of the STS warehouse: ``<catalog>/v1[/<prefix>]`` (``config.rest_base``).

    Returns:
        The URL.
    """
    return rest_base(WAREHOUSE_STS)


def _url(ns, table=None) -> str:
    """REST URL of a namespace or table.

    Args:
        ns: Namespace.
        table: Table name (None: the namespace URL).

    Returns:
        The URL.
    """
    base = f"{_base()}/namespaces/{ns}"
    return f"{base}/tables/{table}" if table else base


def _load(ns, table) -> dict:
    """Raw loadTable.

    Args:
        ns: Namespace.
        table: Table name (None: the namespace URL).

    Returns:
        The answer (metadata-location, metadata).
    """
    r = requests.get(_url(ns, table), headers=catalog_headers(), auth=catalog_auth(), timeout=15)
    r.raise_for_status()
    return r.json()


def _commit(ns, table, requirements, updates) -> requests.Response:
    """Raw table commit.

    Args:
        ns: Namespace.
        table: Table name (None: the namespace URL).
        requirements: Commit requirements.
        updates: Commit updates.

    Returns:
        The response.
    """
    return requests.post(
        _url(ns, table),
        json={"requirements": requirements, "updates": updates},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=30,
    )


@pytest.fixture
def table(namespace):
    """Table (id long, name string, gone long) with one snapshot, then column ``gone`` dropped.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(namespace, name)``.
    """
    cat = pyiceberg_catalog(WAREHOUSE_STS)
    name = f"cons_{uuid.uuid4().hex[:6]}"
    t = cat.create_table(f"{namespace}.{name}", schema=SCHEMA)
    t.append(pa.table({"id": [1], "name": ["a"], "gone": [7]}, schema=SCHEMA))
    with t.update_schema() as u:
        u.delete_column("gone")
    return namespace, name


def _invalid(case: str, md: dict) -> tuple[list, list]:
    """(requirements, updates) of an invalid commit against the table metadata ``md``.

    Args:
        case: Gap case.
        md: Current table metadata.

    Returns:
        ``(requirements, updates)``.
    """
    uuid_req = [{"type": "assert-table-uuid", "uuid": md["table-uuid"]}]
    schemas = {s["schema-id"]: s for s in md["schemas"]}
    current = copy.deepcopy(schemas[md["current-schema-id"]])
    last_column = md["last-column-id"]
    new_id = max(schemas) + 1
    if case in ("incompatible_type", "reuse_field_id"):
        if case == "incompatible_type":
            for f in current["fields"]:
                if f["name"] == "id":
                    f["type"] = "string"
        else:  # the dropped column "gone" had id 3 (long); bring id 3 back as a string
            dropped = next(f for s in md["schemas"] for f in s["fields"] if f["name"] == "gone")
            current["fields"].append(
                {"id": dropped["id"], "name": "back", "type": "string", "required": False}
            )
        current["schema-id"] = new_id
        return uuid_req + [
            {"type": "assert-current-schema-id", "current-schema-id": md["current-schema-id"]}
        ], [
            {"action": "add-schema", "schema": current, "last-column-id": last_column},
            {"action": "set-current-schema", "schema-id": -1},
        ]
    if case == "spec_missing_field":
        spec_id = max(s["spec-id"] for s in md["partition-specs"]) + 1
        return uuid_req, [
            {
                "action": "add-spec",
                "spec": {
                    "spec-id": spec_id,
                    "fields": [
                        {
                            "source-id": last_column + 50,
                            "field-id": 1500,
                            "name": "ghost",
                            "transform": "identity",
                        }
                    ],
                },
            },
            {"action": "set-default-spec", "spec-id": -1},
        ]
    if case == "unknown_schema":
        return uuid_req, [{"action": "set-current-schema", "schema-id": max(schemas) + 40}]
    return uuid_req, [  # unknown_snapshot
        {
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "type": "branch",
            "snapshot-id": random.randint(10**15, 10**16),
        }
    ]


# Not a case: a snapshot whose parent does not exist - legitimate after snapshot expiry.
CASES = [
    "incompatible_type",
    "reuse_field_id",
    "spec_missing_field",
    "unknown_schema",
    "unknown_snapshot",
]


@pytest.mark.parametrize("case", CASES)
def test_invalid_commit_is_refused(case, table):
    """A commit that would produce invalid table metadata is refused - and changes nothing.

    Cases (raw REST commit with correct requirements): change a column to an incompatible
    type (long -> string); reuse the dropped column's field id with another type; a partition
    spec on a field that does not exist; switch to a schema id that does not exist; point
    ``main`` at a snapshot that does not exist.

    Checks:
        * never hidden by a gap: an error answer while the metadata location changed anyway;
        * the answer is a 4xx;
        * the table's metadata location is unchanged.

    Args:
        case: The invalid change (see ``CASES``).
        table: ``(namespace, name)`` of a table (id long, name string, gone long) with one
            snapshot, after which column ``gone`` (field id 3) was dropped.
    """
    ns, name = table
    before = _load(ns, name)
    requirements, updates = _invalid(case, before["metadata"])
    r = _commit(ns, name, requirements, updates)
    after = _load(ns, name)
    changed = after["metadata-location"] != before["metadata-location"]
    # never hidden by a gap: an error answer while the table was changed anyway
    assert not (r.status_code >= 400 and changed), f"answered {r.status_code} but the table changed"
    with _gap(case):
        assert 400 <= r.status_code < 500, (
            f"invalid commit answered {r.status_code}: {r.text[:200]}"
        )
        assert after["metadata-location"] == before["metadata-location"], "the table changed"


# =========================================================================== lost updates

WRITERS = 8


def _parallel_property_writers(ns, name, commit) -> list[int]:
    """8 clients set their own table property at the same time, retrying on 409.

    Args:
        ns: Namespace.
        name: Table name.
        commit: ``(requirements, updates) -> response`` - the endpoint under test.

    Returns:
        The writers whose commit the catalog confirmed (2xx).
    """
    confirmed, barrier = [], threading.Barrier(WRITERS)

    def writer(i):
        """Set this writer's property, retrying on 409 with fresh metadata.

        Args:
            i: Writer number.
        """
        barrier.wait()
        for _ in range(20):  # optimistic concurrency: reload and retry on conflict
            md = _load(ns, name)["metadata"]
            r = commit(
                [
                    {"type": "assert-table-uuid", "uuid": md["table-uuid"]},
                    {
                        "type": "assert-ref-snapshot-id",
                        "ref": "main",
                        "snapshot-id": md.get("current-snapshot-id"),
                    },
                ],
                [{"action": "set-properties", "updates": {f"writer-{i}": "x", "last": str(i)}}],
            )
            if r.status_code in (200, 204):
                confirmed.append(i)
                return
            if r.status_code != 409:
                return

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(WRITERS)]
    [t.start() for t in threads]
    [t.join(120) for t in threads]
    return confirmed


def test_parallel_table_commits_are_not_lost(table):
    """8 clients set their own table property at the same time - no confirmed update is lost.

    Each client retries on 409 with reloaded metadata, as a client must.

    Checks:
        * at least one commit succeeded;
        * every property whose commit the catalog confirmed (200) is in the table afterwards.

    Args:
        table: ``(namespace, name)`` of a table (id long, name string, gone long) with one
            snapshot, after which column ``gone`` (field id 3) was dropped.
    """
    ns, name = table
    confirmed = _parallel_property_writers(ns, name, lambda req, upd: _commit(ns, name, req, upd))
    props = _load(ns, name)["metadata"].get("properties", {})
    lost = [i for i in confirmed if f"writer-{i}" not in props]
    with _gap("lost_table_commit"):
        assert confirmed, "no commit succeeded at all"
        assert not lost, f"confirmed but lost: writers {lost} (confirmed {sorted(confirmed)})"


def test_parallel_transaction_commits_are_not_lost(table):
    """The same race through ``POST /transactions/commit`` (multi-table commit, used by DuckDB).

    Checks:
        * the catalog advertises the endpoint (else: no API);
        * at least one commit succeeded;
        * every property whose commit the catalog confirmed (204) is in the table afterwards.

    Args:
        table: ``(namespace, name)`` of a table with one snapshot (see the test above).
    """
    ns, name = table
    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    if "POST /v1/{prefix}/transactions/commit" not in (cfg.get("endpoints") or []):
        pytest.skip(f"catalog {PROFILE.name} has no API for multi-table commits (not advertised)")
    ident = {"namespace": [ns], "name": name}

    def commit(req, upd):
        """Commit one table change through ``POST /transactions/commit``.

        Args:
            req: Requirements of the change.
            upd: Updates of the change.

        Returns:
            The HTTP response.
        """
        return requests.post(
            f"{_base()}/transactions/commit",
            json={"table-changes": [{"identifier": ident, "requirements": req, "updates": upd}]},
            headers=catalog_headers(),
            auth=catalog_auth(),
            timeout=30,
        )

    confirmed = _parallel_property_writers(ns, name, commit)
    props = _load(ns, name)["metadata"].get("properties", {})
    lost = [i for i in confirmed if f"writer-{i}" not in props]
    with _gap("lost_transaction_commit"):
        assert confirmed, "no commit succeeded at all"
        assert not lost, f"confirmed but lost: writers {lost} (confirmed {sorted(confirmed)})"


#: Rounds of the namespace race: Unity loses an update in about 1 round in 3, so 15 rounds
#: miss it with probability (2/3)^15 < 0.3 % - a stable gap instead of a flaky one.
ROUNDS = 15


def test_parallel_namespace_property_updates_are_not_lost(namespace):
    """8 clients set their own namespace property at the same time - no confirmed update is lost.

    (Lakekeeper fixed exactly this race in 0.15.1.) The race is repeated up to ``ROUNDS`` times
    and stops at the first loss, so an intermittent loss shows up in (almost) every run.

    Checks:
        * at least one update succeeded;
        * in every round, every property whose update the catalog confirmed (200) is on the
          namespace afterwards.

    Args:
        namespace: Fresh namespace of the test session.
    """
    ns = f"{namespace}_props"
    requests.post(
        f"{_base()}/namespaces",
        json={"namespace": [ns]},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=15,
    ).raise_for_status()

    def race(rnd: int) -> tuple[list[int], list[int]]:
        """One round: 8 writers set ``r<rnd>-writer-<i>`` at once.

        Args:
            rnd: Round number (part of the property names).

        Returns:
            ``(confirmed, lost)`` writer numbers.
        """
        confirmed, barrier = [], threading.Barrier(WRITERS)

        def writer(i):
            """Set this writer's namespace property once.

            Args:
                i: Writer number.
            """
            barrier.wait()
            r = requests.post(
                f"{_url(ns)}/properties",
                json={"updates": {f"r{rnd}-writer-{i}": "x"}, "removals": []},
                headers=catalog_headers(),
                auth=catalog_auth(),
                timeout=30,
            )
            if r.status_code == 200:
                confirmed.append(i)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(WRITERS)]
        [t.start() for t in threads]
        [t.join(60) for t in threads]
        props = (
            requests.get(_url(ns), headers=catalog_headers(), auth=catalog_auth(), timeout=15)
            .json()
            .get("properties", {})
        )
        return confirmed, [i for i in confirmed if f"r{rnd}-writer-{i}" not in props]

    try:
        confirmed, lost, rnd = [], [], 0
        for rnd in range(1, ROUNDS + 1):
            confirmed, lost = race(rnd)
            if lost or not confirmed:
                break
        with _gap("lost_namespace_property"):
            if not confirmed:
                pytest.skip(
                    f"catalog {PROFILE.name} has no API for namespace property updates (none accepted)"
                )
            assert not lost, (
                f"confirmed but lost in round {rnd} of {ROUNDS}: writers {lost} "
                f"(confirmed {sorted(confirmed)})"
            )
    finally:
        requests.delete(_url(ns), headers=catalog_headers(), auth=catalog_auth(), timeout=15)


# =========================================================================== reuse of a location


@pytest.mark.parametrize("how", ["same_name", "explicit_location"])
def test_recreate_at_dropped_location(how, namespace):
    """A table re-created where a purged table was is not damaged by the late purge.

    Table A is dropped with purge and B is created right away where A was (same name -> same
    default location, or B's location set explicitly to A's). Purges run asynchronously: when
    A's purge comes, it must not delete B's files.

    Checks:
        * either the catalog refuses B (4xx, not 5xx) while A's purge may be pending,
        * or B gets a location of its own (nothing can collide - printed for the report),
        * or B lies at A's location and stays complete: its rows are readable for 60 s after
          the drop.
        Catalogs without purge, or without a choosable location (explicit case): not offered.

    Args:
        how: ``same_name`` (default location) or ``explicit_location`` (A's location for B).
        namespace: Fresh namespace of the test session.
    """
    cat = pyiceberg_catalog(WAREHOUSE_STS)
    a = f"reuse_{uuid.uuid4().hex[:6]}"
    t = cat.create_table(f"{namespace}.{a}", schema=SCHEMA)
    t.append(pa.table({"id": [1, 2], "name": ["a", "b"], "gone": [0, 0]}, schema=SCHEMA))
    location = t.location()
    r = requests.delete(
        _url(namespace, a),
        params={"purgeRequested": "true"},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=30,
    )
    if r.status_code in (400, 406, 501) and "purge" in r.text.lower():
        pytest.skip(f"catalog {PROFILE.name} has no API for drop with purge ({r.status_code})")
    if r.status_code >= 500:
        with _gap(f"recreate:{how}"):
            raise AssertionError(f"drop with purge answered {r.status_code}: {r.text[:300]}")
        return
    r.raise_for_status()
    b = a if how == "same_name" else f"{a}_b"
    body = {"name": b, "schema": _iceberg_schema()}
    if how == "explicit_location":
        body["location"] = location
    r = requests.post(
        f"{_base()}/namespaces/{namespace}/tables",
        json=body,
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=30,
    )
    if r.status_code >= 400:  # refused while A's files may still be purged: safe
        print(f"recreate {how}: refused {r.status_code} {r.text[:200]}")
        with _gap(f"recreate:{how}"):
            assert r.status_code < 500, (
                f"server error instead of a refusal: {r.status_code} {r.text[:300]}"
            )
        return
    created = r.json()["metadata"]["location"]
    if created.rstrip("/") != location.rstrip("/"):
        if how == "explicit_location":
            pytest.skip(
                f"catalog {PROFILE.name} has no API for choosing a table location (got {created})"
            )
        print(f"recreate {how}: own location {created} (dropped: {location})")
        return  # the catalog gives the new table a location of its own: nothing to collide
    new = cat.load_table(f"{namespace}.{b}")
    rows = pa.table({"id": [7, 8, 9], "name": ["x", "y", "z"], "gone": [0, 0, 0]}, schema=SCHEMA)
    new.append(rows)
    deadline = time.time() + 60
    lost = None
    while time.time() < deadline:
        try:
            got = sorted(cat.load_table(f"{namespace}.{b}").scan().to_arrow()["id"].to_pylist())
            lost = None if got == [7, 8, 9] else f"rows {got}"
        except Exception as exc:  # noqa: BLE001 - a missing file is exactly the failure
            lost = f"{type(exc).__name__}: {str(exc)[:200]}"
        if lost:
            break
        time.sleep(5)
    with _gap(f"recreate:{how}"):
        assert lost is None, f"the purge of the dropped table destroyed the new one: {lost}"


def _iceberg_schema() -> dict:
    """The table schema in Iceberg REST JSON.

    Returns:
        The schema.
    """
    return {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "type": "long", "required": False},
            {"id": 2, "name": "name", "type": "string", "required": False},
            {"id": 3, "name": "gone", "type": "long", "required": False},
        ],
    }


# =========================================================================== metadata history


def test_metadata_log_files_are_kept(table):
    """After commits, every metadata file in the table's ``metadata-log`` still exists.

    Checks:
        * the table has no ``write.metadata.delete-after-commit.enabled`` (spec default: off);
        * after three more commits the ``metadata-log`` lists earlier files, and each of them
          is still in storage (HEAD with the storage's admin keys).

    Args:
        table: ``(namespace, name)`` of a table with one snapshot and one schema change.
    """
    import boto3
    import botocore

    ns, name = table
    t = pyiceberg_catalog(WAREHOUSE_STS).load_table(f"{ns}.{name}")
    for i in range(3):
        t.append(pa.table({"id": [10 + i], "name": ["x"]}))
    md = _load(ns, name)["metadata"]
    assert md.get("properties", {}).get("write.metadata.delete-after-commit.enabled", "false") in (
        "false",
        "False",
    ), "the catalog set write.metadata.delete-after-commit.enabled - the test needs the default"
    log = [e["metadata-file"] for e in md.get("metadata-log", [])]
    assert log, "metadata-log is empty after 5 commits"
    s3 = boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        aws_access_key_id=SETTINGS.s3_access_key,
        aws_secret_access_key=SETTINGS.s3_secret_key,
        region_name=SETTINGS.s3_region,
    )
    missing = []
    for loc in log:
        bucket, key = loc.split("://", 1)[1].split("/", 1)
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as exc:
            missing.append(f"{key.rsplit('/', 1)[-1]} ({exc.response['Error']['Code']})")
    with _gap("metadata_log"):
        assert not missing, (
            f"metadata-log lists {len(log)} files, missing: {len(missing)} - {missing[:3]}"
        )
