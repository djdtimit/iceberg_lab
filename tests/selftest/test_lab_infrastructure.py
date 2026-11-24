"""Lab self-test: the run infrastructure fits together (ports, cloud profiles, smoke run).

Runs without catalog or engines. Covered:

* every catalog's Trino ``<prefix>_proxy`` catalog names the port of that catalog's fault proxy
  (``config.lab_port``), so parallel runs (``scripts/run_parallel.sh``) reach their own proxy;
* the in-process proxies' ports never collide between catalogs;
* cloud profiles (``config.remote_profile``) exist only when configured, refuse incomplete
  settings and a token endpoint on another host, and pick OAuth or bearer authentication;
* a cloud catalog runs only the listed files with the listed engines (``conftest.remote_safe``);
* every file of the smoke run (``scripts/smoke.sh``) exists.

Lab self-test: reported, not part of the comparison.
"""

from __future__ import annotations

import re
import types
from pathlib import Path

import pytest

from iceberg_lab import config

import conftest

pytestmark = pytest.mark.offline

ROOT = Path(__file__).resolve().parents[2]
LOCAL = [n for n, p in config.PROFILES.items() if not p.remote]
#: Base ports of the in-process proxies (fault, federation, storage, IdP).
PROXY_BASES = (18181, 18282, 19000, 19100)


@pytest.mark.parametrize("catalog", LOCAL)
def test_trino_proxy_catalog_names_the_catalogs_port(catalog):
    """Trino's proxy catalog of a profile points at that profile's fault proxy port.

    Checks:
        * ``trino/catalog/<prefix>_proxy.properties`` exists;
        * every ``dev:<port>`` in it is ``lab_port(18181, catalog)``.

    Args:
        catalog: Local catalog profile.
    """
    path = ROOT / "trino" / "catalog" / f"{config.PROFILES[catalog].trino_prefix}_proxy.properties"
    assert path.is_file(), f"{path} missing"
    ports = {int(p) for p in re.findall(r"dev:(\d+)", path.read_text())}
    assert ports == {config.lab_port(18181, catalog)}, f"{path.name}: {sorted(ports)}"


def test_proxy_ports_do_not_collide():
    """No two catalogs share a proxy port, and no proxy kind runs into another's ports.

    Checks:
        * the ports of all proxies over all profiles are distinct.
    """
    ports = [config.lab_port(b, c) for b in PROXY_BASES for c in config.PROFILES]
    assert len(ports) == len(set(ports)), sorted(ports)


def test_remote_profile_needs_its_uri():
    """Without ``LAB_<NAME>_URI`` a cloud catalog is no profile.

    Checks:
        * ``remote_profile`` returns None for every cloud catalog without variables.
    """
    assert all(config.remote_profile(n, {}) is None for n in config.REMOTE_DEFAULTS)


def test_remote_profile_refuses_incomplete_settings():
    """A URI without warehouse or authentication, or a token endpoint elsewhere, is refused.

    Checks:
        * a missing ``WAREHOUSE`` or ``TOKEN``/``CREDENTIAL`` raises ValueError;
        * a ``TOKEN_URL`` on another host raises ValueError (the secret would go there).
    """
    uri = {"LAB_DATABRICKS_URI": "https://ws.example.com/api/2.1/unity-catalog/iceberg-rest"}
    with pytest.raises(ValueError, match="WAREHOUSE"):
        config.remote_profile("databricks", uri | {"LAB_DATABRICKS_TOKEN": "t"})
    with pytest.raises(ValueError, match="TOKEN or"):
        config.remote_profile("databricks", uri | {"LAB_DATABRICKS_WAREHOUSE": "lab"})
    with pytest.raises(ValueError, match="TOKEN_URL"):
        config.remote_profile(
            "databricks",
            uri
            | {
                "LAB_DATABRICKS_WAREHOUSE": "lab",
                "LAB_DATABRICKS_CREDENTIAL": "id:secret",
                "LAB_DATABRICKS_TOKEN_URL": "https://elsewhere.example.com/token",
            },
        )


def test_remote_profile_bearer_and_oauth():
    """Bearer token and client credentials give the right profile.

    Checks:
        * a token: auth ``token``, URI split into base and path, marked remote;
        * client credentials: auth ``oauth2``, the default scope and token endpoint (Databricks
          ``/oidc/v1/token``; Open Catalog below the REST URI).
    """
    p = config.remote_profile(
        "databricks",
        {
            "LAB_DATABRICKS_URI": "https://ws.example.com/api/2.1/unity-catalog/iceberg-rest/",
            "LAB_DATABRICKS_WAREHOUSE": "lab",
            "LAB_DATABRICKS_TOKEN": "t",
        },
    )
    assert (p.auth, p.token, p.base_url, p.path, p.remote) == (
        "token",
        "t",
        "https://ws.example.com",
        "/api/2.1/unity-catalog/iceberg-rest",
        True,
    )
    p = config.remote_profile(
        "databricks",
        {
            "LAB_DATABRICKS_URI": "https://ws.example.com/api/2.1/unity-catalog/iceberg-rest",
            "LAB_DATABRICKS_WAREHOUSE": "lab",
            "LAB_DATABRICKS_CREDENTIAL": "id:secret",
        },
    )
    assert (p.auth, p.oauth_scope, p.token_url, p.token) == (
        "oauth2",
        "all-apis",
        "https://ws.example.com/oidc/v1/token",
        None,
    )
    p = config.remote_profile(
        "snowflake_open",
        {
            "LAB_SNOWFLAKE_OPEN_URI": "https://acct.snowflakecomputing.com/polaris/api/catalog",
            "LAB_SNOWFLAKE_OPEN_WAREHOUSE": "lab",
            "LAB_SNOWFLAKE_OPEN_CREDENTIAL": "id:secret",
        },
    )
    assert (p.oauth_scope, p.token_url) == (
        "PRINCIPAL_ROLE:ALL",
        "https://acct.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens",
    )


def _item(nodeid: str, **params):
    """A stand-in for a collected item with ``nodeid`` and engine parameters.

    Args:
        nodeid: pytest node id.
        **params: Parametrization (e.g. ``engine="trino"``).

    Returns:
        An object with ``nodeid`` and, if parametrized, ``callspec``.
    """
    item = types.SimpleNamespace(nodeid=nodeid)
    if params:
        item.callspec = types.SimpleNamespace(id="-".join(params.values()), params=params)
    return item


def test_remote_runs_only_listed_files_and_engines():
    """A cloud catalog keeps the listed files with the listed engines only.

    Checks:
        * every file in ``REMOTE_SAFE`` exists;
        * a listed file with a cloud-capable engine is kept, with Trino or pg_lake it is not;
        * a test in an unlisted file (security) is not kept;
        * a writer/reader pair needs both engines to be cloud-capable.
    """
    missing = [f for f in conftest.REMOTE_SAFE if not (ROOT / f).is_file()]
    assert not missing, missing
    ops = "tests/engines/test_operations.py::test_append"
    assert conftest.remote_safe(_item(f"{ops}[duckdb]", engine="duckdb"))
    assert not conftest.remote_safe(_item(f"{ops}[trino]", engine="trino"))
    assert not conftest.remote_safe(_item(f"{ops}[pg_lake]", engine="pg_lake"))
    assert not conftest.remote_safe(_item("tests/security/test_security.py::test_x"))
    pair = "tests/engines/test_interop.py::test_read"
    assert conftest.remote_safe(_item(f"{pair}[spark-polars]", writer="spark", reader="polars"))
    assert not conftest.remote_safe(_item(f"{pair}[flink-polars]", writer="flink", reader="polars"))


def test_smoke_files_exist():
    """Every file or directory the smoke run names exists.

    Checks:
        * each entry of ``SMOKE_FILES`` in ``scripts/smoke.sh`` is a path in the repository.
    """
    text = (ROOT / "scripts" / "smoke.sh").read_text()
    block = re.search(r"SMOKE_FILES=\((.*?)\n\)", text, re.DOTALL).group(1)
    paths = [
        line.split("#")[0].strip() for line in block.splitlines() if line.split("#")[0].strip()
    ]
    assert paths, "no SMOKE_FILES found"
    assert [p for p in paths if not (ROOT / p).exists()] == []


def test_snowflake_horizon_pat_and_role():
    """Horizon: the PAT is a bare client secret, the role builds the scope.

    Checks:
        * ``CREDENTIAL`` without a colon gives no client id and the PAT as secret;
        * ``ROLE`` gives the scope ``session:role:<ROLE>`` and the token endpoint lies below
          the REST URI;
        * without ``ROLE`` (and without ``SCOPE``) the profile is refused.
    """
    env = {
        "LAB_SNOWFLAKE_HORIZON_URI": "https://org-acct.snowflakecomputing.com/polaris/api/catalog",
        "LAB_SNOWFLAKE_HORIZON_WAREHOUSE": "LAB_DB",
        "LAB_SNOWFLAKE_HORIZON_CREDENTIAL": "pat-secret",
    }
    with pytest.raises(ValueError, match="ROLE"):
        config.remote_profile("snowflake_horizon", env)
    p = config.remote_profile("snowflake_horizon", env | {"LAB_SNOWFLAKE_HORIZON_ROLE": "ENGINEER"})
    assert (p.auth, p.oauth_scope, p.token_url, p.warehouse_sts) == (
        "oauth2",
        "session:role:ENGINEER",
        "https://org-acct.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens",
        "LAB_DB",
    )
    assert config.oauth_client(p.oauth_credential) == (None, "pat-secret")
    assert config.oauth_client("root:s3:cr3t") == ("root", "s3:cr3t")


@pytest.mark.parametrize(
    ("method", "path", "expected"),
    [
        ("GET", "/v1/config", "GET /v1/config"),
        ("GET", "/v1/wh-id/namespaces", "GET /v1/{prefix}/namespaces"),
        ("GET", "/v1/namespaces/a", "GET /v1/{prefix}/namespaces/{namespace}"),  # no prefix
        (
            "POST",
            "/v1/catalogs/lab/namespaces/a%1Fb/tables/t",  # Unity: prefix with a slash
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        ),
        (
            "HEAD",
            "/v1/p/namespaces/a/views/v",
            "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
        ),
        ("GET", "/v1/p/not-in-the-spec", None),
    ],
)
def test_rest_observed_maps_paths_to_spec_operations(method, path, expected):
    """A request path is counted under its spec operation, whatever the prefix looks like.

    Checks:
        * prefixed, unprefixed and slash-containing prefixes map to the right template;
        * a path the spec does not define maps to nothing.

    Args:
        method: HTTP method.
        path: Request path from ``/v1`` on.
        expected: The operation, or None.
    """
    from iceberg_lab.lab import rest_observed

    assert rest_observed.operation(method, path) == expected


def test_rest_observed_counts_only_the_catalog_and_merges(tmp_path, monkeypatch):
    """Only the catalog's own answers are counted, and counts add up across processes.

    Checks:
        * a request to another host or outside ``/v1`` is not counted;
        * ``write`` adds this process's counts to the report's file.

    Args:
        tmp_path: pytest's temporary directory (the report directory).
        monkeypatch: pytest monkeypatch (an empty counter).
    """
    from collections import Counter

    from iceberg_lab.lab import rest_observed

    monkeypatch.setattr(rest_observed, "OBSERVED", Counter())
    uri = "http://polaris:8181/api/catalog"
    rest_observed.record("GET", f"{uri}/v1/p/namespaces", 200, uri)
    rest_observed.record("GET", "http://rustfs:9000/bucket", 200, uri)
    rest_observed.record("GET", "http://polaris:8181/api/management/v1/catalogs", 200, uri)
    assert dict(rest_observed.OBSERVED) == {"GET /v1/{prefix}/namespaces 200": 1}
    (tmp_path / "rest_observed.json").write_text('{"GET /v1/{prefix}/namespaces 200": 2}')
    rest_observed.write(tmp_path)
    import json

    assert json.loads((tmp_path / "rest_observed.json").read_text()) == {
        "GET /v1/{prefix}/namespaces 200": 3
    }
