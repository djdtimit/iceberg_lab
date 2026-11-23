"""Lab self-test: every "not offered" claim is still true.

A known gap reports "fixed" as soon as it stops occurring - a "not offered" skip does not: it is
decided before the attempt, so a capability an update adds would stay invisible (e.g. "pg_lake has
no API for SigV4" alone covers ~1,650 cases on RustFS). Two safeguards:

1. Version tie: each engine's "not offered" claims were checked at the version in ``VERIFIED``.
   Another version fails here until the claims are re-checked and the version is updated.
2. Active probes for the heaviest claims - they try the capability and fail as soon as it works:
   * pg_lake: SigV4 catalog authentication; a way to create format v3 tables;
   * SQL engines: the Iceberg types they supposedly cannot express (``datatypes.TypeCase.sql``
     is None) - each common spelling is tried in a real CREATE TABLE; if the catalog then holds
     the Iceberg type, the claim is wrong;
   * catalogs: vended credentials / remote signing where the catalog claims to have none.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import importlib.metadata
import uuid

import pytest
import requests

from iceberg_lab.config import (
    PROFILE,
    SETTINGS,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
    rest_prefix_path,
)
from iceberg_lab.datatypes import ALL_CASES

from conftest import ENGINES, engine_param, require_engine

#: Version at which each engine's "not offered" claims were last checked (2026-11-17).
VERIFIED = {
    "pyiceberg": "0.12.0",
    "polars": "2.1.0",
    "daft": "0.7.26",
    "duckdb": "2.0.1",
    "spark": "4.1.4",
    "trino": "484",
    "flink": "2.3.0",
    "pg_lake": "3.7",  # the extension's version (pg_extension.extversion); release tag v3.7.0
}


def _version(engine: str, engine_pool) -> str:
    """The engine's running version.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        The version.
    """
    if engine in ("pyiceberg", "polars", "daft", "duckdb"):
        return importlib.metadata.version(engine)
    if engine == "spark":
        return importlib.metadata.version("pyspark")
    if engine == "trino":
        return str(
            engine_pool("trino").execute("SELECT node_version FROM system.runtime.nodes")[0][0]
        )
    if engine == "flink":
        return requests.get(
            f"{SETTINGS.flink_url.rsplit(':', 1)[0]}:8081/config", timeout=10
        ).json()["flink-version"]
    rows = engine_pool("pg_lake").execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_lake'"
    )
    return rows[0][0]


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_claims_were_checked_for_this_version(engine, engine_pool):
    """A new engine version may have gained what its skips claim it lacks.

    Checks:
        * the engine's running version equals the version the "not offered" claims were last
          checked against (``VERIFIED``) - else re-check them and update the entry.

    Args:
        engine: Engine under test.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine(engine)
    current = _version(engine, engine_pool)
    assert current == VERIFIED[engine], (
        f"{engine} is now {current}, its 'not offered' claims were checked at {VERIFIED[engine]}: "
        f"re-check every skip that says '{engine} has no API/SQL type for ...' (grep the tests), "
        f"then update VERIFIED in tests/selftest/test_na_claims.py"
    )


# =========================================================================== pg_lake


def test_pg_lake_has_no_sigv4(engine_pool):
    """Claim (~1,650 skips on RustFS): pg_lake cannot sign catalog requests with SigV4.

    Checks:
        * a pg_lake catalog with ``rest_auth_type 'sigv4'`` is refused, and the refusal is about
          ``rest_auth_type`` - a refusal for another reason does not prove the claim;
        * accepted: the claim is outdated (fails).

    Args:
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine("pg_lake")
    pg = engine_pool("pg_lake")
    try:
        pg.execute(
            "CREATE SERVER na_probe_sigv4 TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog "
            "OPTIONS (rest_endpoint 'http://rustfs:9000/iceberg', rest_auth_type 'sigv4')"
        )
    except Exception as exc:  # noqa: BLE001 - refused: the claim holds
        assert "rest_auth_type" in str(exc), f"refused for another reason: {exc}"
        return
    finally:
        pg.execute("DROP SERVER IF EXISTS na_probe_sigv4 CASCADE")
    pytest.fail("pg_lake accepts rest_auth_type 'sigv4' - the 'no SigV4' claim is outdated")


def test_pg_lake_creates_only_v2(engine_pool):
    """Claim (~800 skips): pg_lake has no way to create format v3 tables.

    Checks:
        * pg_lake has no format-version setting;
        * every table option that could set the version is refused as invalid option.

    Args:
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine("pg_lake")
    pg = engine_pool("pg_lake")
    settings = pg.execute(
        "SELECT name FROM pg_settings WHERE name ILIKE '%format%version%' AND name NOT ILIKE '%parquet%'"
    )
    assert not settings, f"pg_lake has a format-version setting now: {settings}"
    for option in ("format_version", "format-version", "iceberg_format_version"):
        try:
            pg.execute(f"CREATE TABLE na_probe_v3 (id int) USING iceberg WITH (\"{option}\" = '3')")
        except Exception as exc:  # noqa: BLE001
            assert "invalid option" in str(exc), f"{option}: refused for another reason: {exc}"
            continue
        pg.execute("DROP TABLE IF EXISTS na_probe_v3")
        pytest.fail(
            f"pg_lake accepts the table option {option!r} - the 'only v2' claim is outdated"
        )


# =========================================================================== SQL types

#: Common spellings of an Iceberg type in SQL dialects (tried one by one).
SPELLINGS = {
    "time": ["TIME", "TIME(6)"],
    "timestamp_ns": ["TIMESTAMP_NS", "TIMESTAMP(9)", "TIMESTAMP_NTZ(9)"],
    "timestamptz_ns": ["TIMESTAMPTZ_NS", "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP_LTZ(9)"],
    "geometry": ["GEOMETRY"],
    "geography": ["GEOGRAPHY"],
    "variant": ["VARIANT"],
    "uuid": ["UUID"],
    "unknown": ["UNKNOWN"],
}
CLAIMS = [
    (engine, case.name)
    for engine in ("duckdb", "spark", "trino", "flink")
    for case in ALL_CASES
    if case.sql.get(engine) is None and case.name in SPELLINGS
]


def _create(engine, ns, table, sql_type):
    """Try to create a table with a column of the SQL type.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        ns: Namespace.
        table: Table name (without namespace).
        sql_type: SQL type name.
    """
    e = engine
    if e.name == "spark":
        e.execute(
            f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, v {sql_type}) USING iceberg TBLPROPERTIES ('format-version'='3')"
        )
    elif e.name == "trino":
        e.execute(
            f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, v {sql_type}) WITH (format_version = 3)"
        )
    elif e.name == "flink":
        e.execute(
            f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, v {sql_type}) WITH ('format-version'='3')"
        )
    else:
        e.execute(f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, v {sql_type})")


def _iceberg_type(ns, table) -> str:
    """Column v's type as the catalog stores it (raw JSON: PyIceberg cannot parse every v3 type).

    Args:
        ns: Namespace.
        table: Table name.

    Returns:
        The type as JSON text.
    """
    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    prefix = rest_prefix_path(cfg)
    body = requests.get(
        f"{SETTINGS.catalog_uri}/v1{prefix}/namespaces/{ns}/tables/{table}",
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    md = body["metadata"]
    schema = next(s for s in md["schemas"] if s["schema-id"] == md["current-schema-id"])
    return str(next(f["type"] for f in schema["fields"] if f["name"] == "v"))


@pytest.mark.parametrize(("engine", "type_name"), CLAIMS, ids=[f"{e}-{t}" for e, t in CLAIMS])
def test_engine_has_no_sql_type(engine, type_name, namespace, engine_pool):
    """Claim (test_types): the engine has no SQL type for an Iceberg type.

    Checks:
        * no common spelling of the type creates a column of that Iceberg type in the engine's
          DDL - if one does, the claim is outdated (fails).

    Args:
        engine: Engine under test.
        type_name: Iceberg type the engine is claimed to have no SQL type for.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    require_engine(engine)
    e = engine_pool(engine)
    for spelling in SPELLINGS[type_name]:
        table = f"na_{engine}_{type_name}_{uuid.uuid4().hex[:4]}"
        try:
            _create(e, namespace, table, spelling)
        except Exception:  # noqa: BLE001, S112 - refused (engine or catalog): the claim holds for it
            continue
        got = _iceberg_type(namespace, table)
        assert not got.startswith(
            type_name.split("_ns")[0] if type_name != "unknown" else "unknown"
        ) or (type_name.endswith("_ns") and not got.endswith("_ns")), (
            f"{engine} creates an Iceberg {got} column with {spelling!r} - the claim is outdated"
        )


# =========================================================================== catalogs


def _load_config(ns, table, delegation: str) -> dict:
    """loadTable config with the delegation header.

    Args:
        ns: Namespace.
        table: Table name.
        delegation: Value of the X-Iceberg-Access-Delegation header.

    Returns:
        The config.
    """
    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    prefix = rest_prefix_path(cfg)
    r = requests.get(
        f"{SETTINGS.catalog_uri}/v1{prefix}/namespaces/{ns}/tables/{table}",
        headers={**catalog_headers(), "X-Iceberg-Access-Delegation": delegation},
        auth=catalog_auth(),
        timeout=10,
    )
    r.raise_for_status()
    return {
        **r.json().get("config", {}),
        **{
            k: v
            for c in r.json().get("storage-credentials", [])
            for k, v in c.get("config", {}).items()
        },
    }


@pytest.fixture
def probe_table(namespace):
    """A small table to ask loadTable about.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(namespace, table)``.
    """
    import pyarrow as pa

    from iceberg_lab.engines import pyiceberg_catalog

    table = f"na_probe_{uuid.uuid4().hex[:6]}"  # namespace is module-wide: one table per test
    pyiceberg_catalog(WAREHOUSE_STS).create_table(
        f"{namespace}.{table}", schema=pa.schema([("id", pa.int64())])
    )
    return namespace, table


def test_catalog_without_vending_issues_no_keys(probe_table):
    """Claim (RustFS: no vended storage credentials): asked for them, the catalog issues none.

    Checks:
        * a loadTable with ``X-Iceberg-Access-Delegation: vended-credentials`` returns no
          ``s3.access-key-id``. Catalogs that vend: nothing to check (skip).

    Args:
        probe_table: ``(namespace, table)`` of a small table to ask loadTable about.
    """
    if PROFILE.name != "rustfs":
        pytest.skip(f"catalog {PROFILE.name} issues keys - no claim to check")
    config = _load_config(*probe_table, "vended-credentials")
    assert "s3.access-key-id" not in config, "the catalog issues keys now - the claim is outdated"


def test_catalog_without_signing_offers_none(probe_table):
    """Claim (Polaris, RustFS, Unity: no remote signing): asked for it, the catalog offers none.

    Checks:
        * a loadTable asking for remote signing is either refused with a 4xx (not 5xx), or its
          config does not enable remote signing;
        * /v1/config advertises no ``/sign`` endpoint. Lakekeeper (signs): nothing to check.

    Args:
        probe_table: ``(namespace, table)`` of a small table to ask loadTable about.
    """
    if PROFILE.name == "lakekeeper":
        pytest.skip("catalog lakekeeper offers remote signing - no claim to check")
    try:
        config = _load_config(*probe_table, "remote-signing")
    except requests.HTTPError as exc:  # the catalog refuses the delegation mode (Polaris: 400)
        assert exc.response.status_code < 500, f"server error instead of a refusal: {exc}"
        return
    assert str(config.get("s3.remote-signing-enabled", "false")).lower() != "true", (
        "the catalog enables remote signing now - the claim is outdated"
    )
    cfg = requests.get(
        f"{SETTINGS.catalog_uri}/v1/config",
        params={"warehouse": WAREHOUSE_STS},
        headers=catalog_headers(),
        auth=catalog_auth(),
        timeout=10,
    ).json()
    assert not any(e.endswith("/sign") for e in cfg.get("endpoints", [])), (
        "the sign endpoint is advertised now"
    )
