"""Gravitino in front of another catalog: does what one side writes reach the other side?

Profiles ``gravitino_<backend>`` (src/iceberg_lab/config.py ``FRONTED``) run the whole suite
through Gravitino's Iceberg REST service against a lakehouse-iceberg catalog with
``catalog-backend = rest``. That shows what works through Gravitino. These tests add what the
suite cannot see, because it only ever talks to one side: the backend is used directly as well
(its own Iceberg REST endpoint, the lab's admin identity, the lab's S3 keys).

    gravitino_write_is_in_the_backend     written through Gravitino -> the backend has it
    backend_write_is_seen                 written in the backend -> Gravitino serves it, also
                                          after a second commit (no stale metadata)
    backend_drop_is_seen                  dropped in the backend -> gone through Gravitino
    backend_table_in_own_api              a table Gravitino never created: its own API lists
                                          and loads it, and it can be tagged
    staged_create                         a staged creation (CTAS, create and write in one
                                          commit) through Gravitino lands in the backend

Only on the Gravitino-fronted profiles (marker ``specific``); every other catalog: not offered.
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest
import requests
from fronting import backend_catalog

from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSE_STS, catalog_headers
from iceberg_lab.engines import pyiceberg_catalog

from conftest import expect, find_gap, gap_table

pytestmark = pytest.mark.specific

ROWS = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
MORE = pa.table({"id": pa.array([4, 5], pa.int64())})

# Filled from observed failures only: (catalog, test, regex, cause).
# fmt: off
KNOWN_GAPS = gap_table(("catalog", "test"), [
    ("gravitino_lakekeeper", "staged_create_*", r"Cannot assign a new UUID|tabular locations have to be empty", "gravitino-rest-backend-staged-create"),
    ("gravitino_rustfs", "staged_create_*", r"cannot reassign table uuid", "gravitino-rest-backend-staged-create"),
])
# fmt: on


def _gap(test: str):
    """``expect_gap`` for a documented gap of the test on this profile.

    Args:
        test: Gap key.

    Returns:
        The context manager.
    """
    return expect(find_gap(KNOWN_GAPS, catalog=PROFILE.name, test=test))


@pytest.fixture(scope="module")
def sides():
    """``(through Gravitino, backend directly)`` - two PyIceberg catalogs on the same tables.

    Returns:
        The two catalogs.
    """
    if not PROFILE.fronts:
        pytest.skip(f"catalog {PROFILE.name} has no API for fronting another catalog")
    return pyiceberg_catalog(WAREHOUSE_STS), backend_catalog()


@pytest.fixture
def ns(sides):
    """A fresh namespace, created and dropped (with its tables) in the backend.

    Args:
        sides: The two catalogs.

    Yields:
        The namespace name.
    """
    _, backend = sides
    name = f"fr_{uuid.uuid4().hex[:8]}"
    backend.create_namespace(name)
    yield name
    for side in sides:  # RustFS lists a table created through Gravitino only there at times
        try:
            tables = side.list_tables(name)
        except Exception:  # noqa: BLE001, S112 - cleanup: the side cannot list
            continue
        for t in tables:
            try:
                side.drop_table(t)  # not purge: RustFS refuses purgeRequested=true
            except Exception:  # noqa: BLE001, S110 - cleanup: a table the test dropped already
                pass
    backend.drop_namespace(name)


def _ids(cat, ident: str) -> list[int]:
    """Sorted ids of a table, freshly loaded.

    Args:
        cat: PyIceberg catalog.
        ident: ``namespace.table``.

    Returns:
        The ids.
    """
    return sorted(cat.load_table(ident).scan().to_arrow()["id"].to_pylist())


def test_gravitino_write_is_in_the_backend(sides, ns):
    """A table created and written through Gravitino is the backend's table.

    Checks:
        * the backend lists the table and reads ids 1..3;
        * both sides name the same current metadata file (one table, not a copy).

    Args:
        sides: ``(through Gravitino, backend directly)``.
        ns: A fresh namespace in the backend.
    """
    grv, backend = sides
    ident = f"{ns}.t"
    with _gap("gravitino_write_is_in_the_backend"):
        t = grv.create_table(ident, schema=ROWS.schema)
        t.append(ROWS)
        assert (ns, "t") in backend.list_tables(ns)
        assert _ids(backend, ident) == [1, 2, 3]
        assert (
            backend.load_table(ident).metadata_location == grv.load_table(ident).metadata_location
        )


def test_backend_write_is_seen(sides, ns):
    """What the backend commits, Gravitino serves - also after it served the table once.

    Checks:
        * a table created and written directly in the backend reads ids 1..3 through Gravitino;
        * after a second append in the backend, Gravitino serves ids 1..5 and the backend's
          current metadata file (no cached, stale table).

    Args:
        sides: ``(through Gravitino, backend directly)``.
        ns: A fresh namespace in the backend.
    """
    grv, backend = sides
    ident = f"{ns}.t"
    with _gap("backend_write_is_seen"):
        backend.create_table(ident, schema=ROWS.schema).append(ROWS)
        assert _ids(grv, ident) == [1, 2, 3]
        backend.load_table(ident).append(MORE)
        assert _ids(grv, ident) == [1, 2, 3, 4, 5], "Gravitino serves a stale table"
        assert (
            grv.load_table(ident).metadata_location == backend.load_table(ident).metadata_location
        )


def test_backend_drop_is_seen(sides, ns):
    """A table dropped in the backend is gone through Gravitino too.

    Checks:
        * Gravitino served the table before;
        * after the backend dropped it, Gravitino neither lists nor loads it.

    Args:
        sides: ``(through Gravitino, backend directly)``.
        ns: A fresh namespace in the backend.
    """
    grv, backend = sides
    ident = f"{ns}.t"
    with _gap("backend_drop_is_seen"):
        backend.create_table(ident, schema=ROWS.schema).append(ROWS)
        assert _ids(grv, ident) == [1, 2, 3]
        backend.drop_table(ident)
        assert (ns, "t") not in grv.list_tables(ns)
        assert not grv.table_exists(ident), "Gravitino still loads a table the backend dropped"


def test_backend_table_in_own_api(sides, ns):
    """A table Gravitino never created is known to its own API and can carry its metadata.

    Checks:
        * Gravitino's own API lists and loads the table created directly in the backend;
        * a Gravitino tag can be attached to it and is listed on it.

    Args:
        sides: ``(through Gravitino, backend directly)``.
        ns: A fresh namespace in the backend.
    """
    from iceberg_lab.bootstrap import GRAVITINO_ACCEPT, GRAVITINO_API, GRAVITINO_METALAKE

    _, backend = sides
    backend.create_table(f"{ns}.t", schema=ROWS.schema).append(ROWS)
    h = catalog_headers(SETTINGS) | {"Accept": GRAVITINO_ACCEPT}
    api = f"{GRAVITINO_API}/api/metalakes/{GRAVITINO_METALAKE}"
    schema = f"{api}/catalogs/{WAREHOUSE_STS}/schemas/{ns}"
    tag = f"fr_{uuid.uuid4().hex[:6]}"
    with _gap("backend_table_in_own_api"):
        r = requests.get(f"{schema}/tables", headers=h, timeout=30)
        assert r.ok, f"list tables: {r.status_code} {r.text[:300]}"
        assert "t" in [i["name"] for i in r.json()["identifiers"]], r.text[:300]
        r = requests.get(f"{schema}/tables/t", headers=h, timeout=30)
        assert r.ok, f"load table: {r.status_code} {r.text[:300]}"
        requests.post(f"{api}/tags", json={"name": tag, "properties": {}}, headers=h, timeout=30)
        try:
            obj = f"{api}/objects/table/{WAREHOUSE_STS}.{ns}.t/tags"
            r = requests.post(
                obj, json={"tagsToAdd": [tag], "tagsToRemove": []}, headers=h, timeout=30
            )
            assert r.ok, f"attach tag: {r.status_code} {r.text[:300]}"
            assert tag in requests.get(obj, headers=h, timeout=30).json().get("names", [])
        finally:
            requests.post(
                f"{api}/objects/table/{WAREHOUSE_STS}.{ns}.t/tags",
                json={"tagsToAdd": [], "tagsToRemove": [tag]},
                headers=h,
                timeout=30,
            )
            requests.delete(f"{api}/tags/{tag}", headers=h, timeout=30)


@pytest.mark.parametrize("with_data", [False, True], ids=["empty", "with_data"])
def test_staged_create(sides, ns, with_data):
    """A table created in one transaction through Gravitino (stage-create, then the commit with
    assert-create - what CTAS and create-and-write do) lands in the backend.

    Checks:
        * the transaction commits; the table reads 0 rows (empty) or ids 1..3 (with data)
          through Gravitino and in the backend.

    Args:
        sides: ``(through Gravitino, backend directly)``.
        ns: A fresh namespace in the backend.
        with_data: Write data in the same transaction.
    """
    grv, backend = sides
    ident = f"{ns}.t"
    want = [1, 2, 3] if with_data else []
    with _gap(f"staged_create_{'with_data' if with_data else 'empty'}"):
        tx = grv.create_table_transaction(ident, schema=ROWS.schema)
        if with_data:
            tx.append(ROWS)
        tx.commit_transaction()
        assert _ids(grv, ident) == want
        assert _ids(backend, ident) == want
