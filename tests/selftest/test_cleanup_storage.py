"""Self-tests of the storage cleanup before a full run (``scripts/cleanup_storage.py``).

It deletes objects: what it keeps must be right without a live stack. In front of Unity
through Gravitino it once did not protect the managed storage of Unity's other catalogs,
because the check asked ``PROFILE.name == "unity"``.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import types
from pathlib import Path

import pytest

from iceberg_lab.config import PROFILES

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cleanup_storage.py"


@pytest.fixture(scope="module")
def cs():
    """The script as a module (scripts/ is not a package).

    Returns:
        The loaded module.
    """
    spec = importlib.util.spec_from_file_location("cleanup_storage", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


NOW = dt.datetime(2026, 11, 25, 12, tzinfo=dt.UTC)
CUTOFF = NOW - dt.timedelta(hours=3)
OLD = NOW - dt.timedelta(days=1)


def test_decide_keeps_what_is_in_use(cs):
    """Referenced files and everything below a live location are kept, however old.

    Checks:
        * a referenced file in the same bucket is kept, the same key in another bucket not;
        * a key below a live table location is kept, a sibling with the same name prefix
          (``t1`` vs ``t10``) only because the live prefix ends with ``/``;
        * an old unreferenced key is an orphan, a young one is left alone.

    Args:
        cs: The cleanup script.
    """
    files = {("b", "lab/ns/t1/metadata/00001.json")}
    prefixes = [("b", "lab/ns/t1/")]

    def d(key, bucket="b", when=OLD):
        """The verdict for a key with the fixtures of this test.

        Args:
            key: Object key.
            bucket: Its bucket.
            when: Its modification time.

        Returns:
            The verdict.
        """
        return cs.decide(bucket, key, when, files, prefixes, [], CUTOFF)

    assert d("lab/ns/t1/metadata/00001.json") == "keep"
    assert d("lab/ns/t1/metadata/00001.json", bucket="other") == "orphan"
    assert d("lab/ns/t1/data/x.parquet") == "keep"
    assert d("lab/ns/t10/data/x.parquet") == "orphan"
    assert d("lab/ns/gone/data/x.parquet") == "orphan"
    assert d("lab/ns/gone/data/x.parquet", when=NOW) == "young"


def test_decide_protects_other_unity_catalogs(cs):
    """Unity's managed storage of other catalogs is kept; the own catalog's is swept.

    Checks:
        * an old object of another catalog id below ``__unitystorage/catalogs/`` is kept;
        * an old unreferenced object of the own catalog id is an orphan.

    Args:
        cs: The cleanup script.
    """
    managed = "lab/__unitystorage/catalogs/"
    foreign = [f"{managed}own-id/", managed]
    other = cs.decide("b", f"{managed}other-id/t/x.parquet", OLD, set(), [], foreign, CUTOFF)
    own = cs.decide("b", f"{managed}own-id/t/x.parquet", OLD, set(), [], foreign, CUTOFF)
    assert (other, own) == ("keep", "orphan")


def test_split_and_roots(cs, monkeypatch):
    """Storage URIs split into bucket and key; each profile sweeps its own roots.

    Checks:
        * ``s3://bucket/a/b`` -> ``(bucket, a/b)``, a bucket alone -> empty key;
        * Lakekeeper sweeps one root per warehouse, a profile in front of it only its own
          location (never the whole bucket).

    Args:
        cs: The cleanup script.
        monkeypatch: pytest fixture to swap the profile.
    """
    assert cs.split("s3://bucket/a/b") == ("bucket", "a/b")
    assert cs.split("s3://bucket") == ("bucket", "")
    monkeypatch.setattr(cs, "PROFILE", PROFILES["lakekeeper"])
    assert len(cs.roots()) == len(cs.WAREHOUSES)
    assert all(prefix.endswith("/") and prefix != "/" for _, prefix in cs.roots())
    fronted = PROFILES["gravitino_lakekeeper"]
    monkeypatch.setattr(cs, "PROFILE", fronted)
    bucket, prefix = cs.split(fronted.location)
    assert cs.roots() == [(bucket, prefix.rstrip("/") + "/")]


@pytest.mark.parametrize("profile", ["unity", "gravitino_unity"])
def test_foreign_prefixes_ask_unity_itself(cs, monkeypatch, profile):
    """The other Unity catalogs are protected on Unity and in front of it.

    Checks:
        * the catalog id is asked from Unity (not Gravitino), for Unity's own catalog name;
        * the result is the own catalog's managed prefix and the managed root.

    Args:
        cs: The cleanup script.
        monkeypatch: pytest fixture to swap the profile and the HTTP call.
        profile: Unity itself, or Gravitino in front of it.
    """
    asked = []

    def get(url, **kwargs):
        """Fake ``requests.get``: Unity's catalog answer.

        Args:
            url: Requested URL.
            **kwargs: Ignored request options.

        Returns:
            A response with the catalog id.
        """
        asked.append(url)
        return types.SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"id": "own-id"})

    p = PROFILES[profile]
    monkeypatch.setattr(cs, "PROFILE", p)
    monkeypatch.setattr(cs, "catalog_headers", dict)
    monkeypatch.setattr(cs.requests, "get", get)
    unity = PROFILES["unity"]
    _, prefix = cs.split(p.location)
    managed = f"{prefix.rstrip('/')}/__unitystorage/catalogs/"
    assert cs.foreign_prefixes() == [f"{managed}own-id/", managed]
    assert asked[0].endswith(f"/api/2.1/unity-catalog/catalogs/{unity.warehouse_sts}")
    assert "gravitino" not in asked[0]


def test_foreign_prefixes_empty_without_unity(cs, monkeypatch):
    """Profiles without Unity in their layers protect nothing extra (and ask nobody).

    Checks:
        * Lakekeeper and Gravitino in front of Polaris get ``[]`` without an HTTP call.

    Args:
        cs: The cleanup script.
        monkeypatch: pytest fixture to swap the profile and the HTTP call.
    """
    monkeypatch.setattr(cs.requests, "get", lambda *a, **k: pytest.fail("asked over HTTP"))
    for name in ("lakekeeper", "gravitino_polaris"):
        monkeypatch.setattr(cs, "PROFILE", PROFILES[name])
        assert cs.foreign_prefixes() == []
