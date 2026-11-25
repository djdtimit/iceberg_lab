"""Central connection settings for the lab.

``CATALOG`` selects one of the catalog profiles (:data:`PROFILES`: lakekeeper, polaris,
rustfs, unity, reference, gravitino; cloud catalogs only when configured, :func:`remote_profile`) - its URL, authentication (open, OAuth2, bearer token or SigV4), warehouses
and storage location. Every other value can be overridden with an environment variable;
inside the devcontainer they are set in ``.devcontainer/docker-compose.yml`` (service
``dev``) and the defaults below match them.

All hostnames are *compose service names* (``rustfs``, ``lakekeeper``, ...). They only resolve
inside the compose network, which is why tests and notebooks run in the devcontainer: the
catalogs hand out those names (e.g. the S3 endpoint stored in Lakekeeper's warehouse storage
profile, see :mod:`iceberg_lab.bootstrap`).

Engines that need a sensible lab workaround to connect (:data:`HELPS`) get it in every run - a
test that works with it counts as working. ``LAB_NO_HELP=1`` (:data:`WITH_HELP` false) runs
their tests once more without it (separate report, shown next to the comparison, not counted).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class CatalogProfile:
    """One Iceberg REST catalog the whole suite can run against (``CATALOG=<name>``).

    Attributes:
        name: Profile name, also the report sub-directory for non-default catalogs.
        base_url: Service base URL (management API, health) inside the compose network.
        path: Path of the Iceberg REST API below ``base_url`` (clients append ``/v1``).
        warehouse_sts: Warehouse whose tables get vended STS credentials.
        warehouse_signing: Warehouse with remote signing, or ``None`` if not offered.
        oauth_credential: ``client_id:client_secret`` if the catalog requires OAuth2.
        oauth_scope: Scope for the client-credentials token request.
        token_path: Token endpoint below ``base_url`` (only with ``oauth_credential``), or an
            absolute URL when an identity provider on another host issues the tokens.
        trino_prefix: Prefix of the static Trino catalog files (``<prefix>_sts`` ...).
        capabilities: What the catalog offers beyond the REST basics; tests for anything
            missing report "no API" (not offered) instead of failing.
    """

    name: str
    base_url: str
    path: str
    warehouse_sts: str
    warehouse_signing: str | None = None
    oauth_credential: str | None = None
    oauth_scope: str | None = None
    token_path: str | None = None
    trino_prefix: str = "lk"
    capabilities: frozenset[str] = field(default_factory=frozenset)
    #: Catalog authentication: "none" (open), "oauth2" (``oauth_credential``) or "sigv4".
    auth: str = "none"
    #: Does the catalog hand out storage credentials? If not, engines use their own S3 keys
    #: (the catalog requires client-provided credentials) and "catalog-only" tests are no API.
    vended_credentials: bool = True
    #: Root location of the main warehouse (where tables and test files live).
    location: str = ""
    #: Catalog name every engine uses for portable views (tests/engines/test_views.py).
    views_catalog: str = "sts"
    #: Static bearer token (``auth="token"``).
    token: str | None = None
    #: The catalog vends keys but no S3 endpoint (it assumes AWS): engines add the RustFS
    #: endpoint themselves.
    client_s3_endpoint: bool = False
    #: A cloud catalog outside the lab (:func:`remote_profile`): only the tests in
    #: ``conftest.REMOTE_SAFE`` run, with the engines in ``conftest.REMOTE_ENGINES``.
    remote: bool = False

    @property
    def token_url(self) -> str | None:
        """Full OAuth2 token endpoint of the catalog, if it has one.

        Returns:
            The token URL, or None.
        """
        if not self.token_path:
            return None
        return self.token_path if "://" in self.token_path else f"{self.base_url}{self.token_path}"


#: Catalogs the suite knows. Lakekeeper is the default and the reference.
PROFILES = {
    "lakekeeper": CatalogProfile(
        name="lakekeeper",
        base_url=os.getenv("LAKEKEEPER_URL", "http://lakekeeper:8181"),
        path="/catalog",
        warehouse_sts="demo-sts",
        warehouse_signing="demo-signing",
        trino_prefix="lk",
        capabilities=frozenset({"remote_signing", "lakekeeper_management_api"}),
        location="s3://warehouse/demo-sts",
    ),
    "polaris": CatalogProfile(
        name="polaris",
        base_url=os.getenv("POLARIS_URL", "http://polaris:8181"),
        path="/api/catalog",
        warehouse_sts="polaris_sts",
        # Polaris' S3 remote signing is experimental and off by default - not offered here.
        oauth_credential="root:s3cr3t",
        oauth_scope="PRINCIPAL_ROLE:ALL",
        token_path="/api/catalog/v1/oauth/tokens",
        trino_prefix="pol",
        auth="oauth2",
        location="s3://warehouse/polaris_sts",
        views_catalog="psts",
    ),
    # The Iceberg REST catalog built into RustFS ("table buckets", like AWS S3 Tables): one
    # table-enabled bucket is one warehouse. SigV4 like S3 itself; no credential vending
    # ("client-provided-s3-credentials-required"), no remote signing.
    "rustfs": CatalogProfile(
        name="rustfs",
        base_url=os.getenv("RUSTFS_URL", "http://rustfs:9000"),
        path="/iceberg",
        warehouse_sts="labtables",
        trino_prefix="rfs",
        auth="sigv4",
        vended_credentials=False,
        location="s3://labtables",
        views_catalog="rsts",
    ),
    # Unity Catalog OSS 0.7.0 (compose profile unity): warehouse = UC catalog "lab", managed
    # storage on RustFS bucket "unitylab". Authorization is disabled in the lab, any bearer
    # token is accepted. Vended STS keys come without an endpoint (Unity assumes AWS).
    "unity": CatalogProfile(
        name="unity",
        base_url=os.getenv("UNITY_URL", "http://unitycatalog:8080"),
        path="/api/2.1/unity-catalog/iceberg",
        warehouse_sts="lab",
        trino_prefix="uc",
        auth="token",
        token="not-used",
        client_s3_endpoint=True,
        location="s3://unitylab/lab",
        views_catalog="usts",
    ),
    # The Apache Iceberg REST fixture (compose profile reference): Iceberg Java's own
    # CatalogHandlers on a JDBC/SQLite catalog - no authentication, no credential vending, one
    # warehouse (it ignores the warehouse name), no management API. The baseline: a gap that
    # also shows here comes from the engine or from Iceberg itself, not from a catalog.
    "reference": CatalogProfile(
        name="reference",
        base_url=os.getenv("REFERENCE_URL", "http://reference:8181"),
        path="",
        warehouse_sts="reference",
        trino_prefix="ref",
        vended_credentials=False,
        location="s3://reference/wh",
        views_catalog="refsts",
    ),
    # Apache Gravitino 1.3 (compose profile gravitino): its Iceberg REST service runs inside the
    # Gravitino server (auxiliary mode) and serves the lakehouse-iceberg catalogs of metalake
    # "lab" - the warehouse names the catalog. Tokens come from Keycloak (client credentials of
    # lab-admin, scope gravitino = audience), so the token endpoint is on another host.
    "gravitino": CatalogProfile(
        name="gravitino",
        base_url=os.getenv("GRAVITINO_URL", "http://gravitino:9001"),
        path="/iceberg",
        warehouse_sts="gravitino_sts",
        oauth_credential="lab-admin:lab-admin-secret",
        oauth_scope="gravitino",
        token_path=os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
        + "/realms/iceberg/protocol/openid-connect/token",
        trino_prefix="grv",
        auth="oauth2",
        location="s3://warehouse/gravitino_sts",
        views_catalog="gsts",
    ),
}

#: Cloud catalogs the lab can run against, with their defaults (docs/cloud-catalogs.md). Each
#: one is a profile only when ``LAB_<NAME>_URI`` is set (environment or ``.env.cloud``).
REMOTE_DEFAULTS: dict[str, dict[str, str]] = {
    # Snowflake Open Catalog (managed Apache Polaris): OAuth client credentials of a service
    # connection, token endpoint below the REST URI, all principal roles.
    "snowflake_open": {"SCOPE": "PRINCIPAL_ROLE:ALL"},
    # Snowflake Horizon Catalog's Iceberg REST endpoint (same path as Open Catalog): the
    # programmatic access token is the client secret (no client id), exchanged at
    # <URI>/v1/oauth/tokens for the scope session:role:<ROLE>; WAREHOUSE is the database name.
    "snowflake_horizon": {"SCOPE_TEMPLATE": "session:role:{ROLE}"},
    # Databricks Unity Catalog (/api/2.1/unity-catalog/iceberg-rest): personal access token, or
    # a service principal's OAuth M2M credentials (token endpoint /oidc/v1/token, all-apis).
    "databricks": {"SCOPE": "all-apis", "TOKEN_PATH": "/oidc/v1/token"},
}
#: File with the cloud catalogs' variables (never committed: .gitignore).
CLOUD_ENV = Path(__file__).resolve().parents[2] / ".env.cloud"


def _cloud_env() -> dict[str, str]:
    """``LAB_*`` variables from :data:`CLOUD_ENV` (``KEY=value`` lines), the environment winning.

    Returns:
        The variables.
    """
    values: dict[str, str] = {}
    if CLOUD_ENV.is_file():
        for line in CLOUD_ENV.read_text().splitlines():
            key, sep, value = line.strip().partition("=")
            if sep and key.startswith("LAB_") and not key.startswith("#"):
                values[key.strip()] = value.strip().strip('"').strip("'")
    return values | {k: v for k, v in os.environ.items() if k.startswith("LAB_")}


def remote_profile(name: str, env: dict[str, str]) -> CatalogProfile | None:
    """The profile of a cloud catalog from its ``LAB_<NAME>_*`` variables, or None if unset.

    Variables: ``URI`` (the Iceberg REST URI clients get, without ``/v1``), ``WAREHOUSE``
    (catalog/warehouse name sent to ``/v1/config``), and either ``TOKEN`` (bearer token) or
    ``CREDENTIAL`` (``client_id:client_secret``, or a bare secret such as Snowflake's
    programmatic access token; OAuth client credentials) with optional ``SCOPE`` (or ``ROLE``
    where the catalog's ``SCOPE_TEMPLATE`` builds it) and ``TOKEN_URL`` (default:
    ``<URI>/v1/oauth/tokens`` or the catalog's ``TOKEN_PATH`` in :data:`REMOTE_DEFAULTS`).

    Args:
        name: Profile name, a key of :data:`REMOTE_DEFAULTS`.
        env: The variables (:func:`_cloud_env`).

    Returns:
        The profile, or None when ``LAB_<NAME>_URI`` is not set.

    Raises:
        ValueError: a variable is missing, or the token endpoint is on another host.
    """
    prefix = f"LAB_{name.upper()}_"
    get = {
        **REMOTE_DEFAULTS[name],
        **{k.removeprefix(prefix): v for k, v in env.items() if k.startswith(prefix)},
    }.get
    if not get("URI"):
        return None
    uri = urlsplit(get("URI").rstrip("/"))
    base = f"{uri.scheme}://{uri.netloc}"
    if not get("WAREHOUSE") or not (get("TOKEN") or get("CREDENTIAL")):
        raise ValueError(
            f"{prefix}URI is set: {prefix}WAREHOUSE and {prefix}TOKEN or "
            f"{prefix}CREDENTIAL are needed too (docs/cloud-catalogs.md)"
        )
    token_path, scope = None, get("SCOPE")
    if get("CREDENTIAL") and not scope and get("SCOPE_TEMPLATE"):
        if not get("ROLE"):
            raise ValueError(f"{prefix}ROLE is needed for the scope {get('SCOPE_TEMPLATE')}")
        scope = get("SCOPE_TEMPLATE").format(ROLE=get("ROLE"))
    if get("CREDENTIAL"):
        token_url = urlsplit(
            get("TOKEN_URL") or f"{base}{get('TOKEN_PATH') or uri.path + '/v1/oauth/tokens'}"
        )
        if token_url.netloc != uri.netloc:
            raise ValueError(f"{prefix}TOKEN_URL must be on {uri.netloc}")
        token_path = token_url.path
    return CatalogProfile(
        name=name,
        base_url=base,
        path=uri.path,
        warehouse_sts=get("WAREHOUSE"),
        oauth_credential=get("CREDENTIAL") or None,
        oauth_scope=scope if get("CREDENTIAL") else None,
        token_path=token_path,
        auth="oauth2" if get("CREDENTIAL") else "token",
        token=None if get("CREDENTIAL") else get("TOKEN"),
        trino_prefix="",
        views_catalog=name,
        remote=True,
    )


for _name in REMOTE_DEFAULTS:
    if (_profile := remote_profile(_name, _cloud_env())) is not None:
        PROFILES[_name] = _profile

CATALOG = os.getenv("CATALOG", "lakekeeper")
if CATALOG not in PROFILES:
    raise ValueError(f"CATALOG={CATALOG!r} unknown; choose one of {sorted(PROFILES)}")
#: The catalog this process runs against.
PROFILE = PROFILES[CATALOG]


#: The in-process proxies of the tests (fault, storage, IdP, federation) listen on a port per
#: catalog, so the runs of several catalogs can share the dev container (scripts/run_parallel.sh).
#: The run without help uses its catalog's ports: it runs after that catalog's run, not beside it.
def lab_port(base: int, catalog: str | None = None) -> int:
    """The port of an in-process proxy for a catalog.

    Args:
        base: The proxy's port for the first catalog (Lakekeeper), e.g. 18181.
        catalog: Profile name; default: this process's catalog.

    Returns:
        ``base`` plus 2 x the profile's position in :data:`PROFILES`.
    """
    return base + 2 * list(PROFILES).index(catalog or CATALOG)


@dataclass(frozen=True)
class Settings:
    """Immutable bundle of all endpoints and credentials used by bootstrap, engines and tests.

    Attributes:
        lakekeeper_url: Base URL of Lakekeeper. The Iceberg REST API lives under ``/catalog``,
            the management API under ``/management`` and the web UI under ``/ui``.
        s3_endpoint: RustFS S3 API endpoint, including the scheme.
        s3_access_key: RustFS root access key. Used for bucket creation, as the storage
            credential Lakekeeper itself uses, and as "static keys" for engines that cannot
            use catalog-provided access (see ``Credentials`` in :mod:`iceberg_lab.engines`).
        s3_secret_key: RustFS root secret key (lab only - never do this in production).
        s3_region: Region string. RustFS ignores it, but every S3 client insists on one.
        s3_bucket: The single bucket that holds both warehouses (under different prefixes).
        trino_host: Hostname of the optional Trino coordinator (compose profile ``trino``).
        trino_port: HTTP port of Trino.
        pglake_host: Hostname of the optional Postgres + pg_lake server (profile ``pglake``).
        pglake_port: Postgres port of pg_lake inside the compose network.
        flink_url: REST endpoint of the optional Flink SQL Gateway (profile ``flink``).
    """

    lakekeeper_url: str = os.getenv("LAKEKEEPER_URL", "http://lakekeeper:8181")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "rustfsadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "rustfsadmin")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")
    s3_bucket: str = os.getenv("S3_BUCKET", "warehouse")  # RustFS table bucket: PROFILE.location
    trino_host: str = os.getenv("TRINO_HOST", "trino")
    trino_port: int = int(os.getenv("TRINO_PORT", "8080"))
    pglake_host: str = os.getenv("PGLAKE_HOST", "pg-lake")
    pglake_port: int = int(os.getenv("PGLAKE_PORT", "5432"))
    flink_url: str = os.getenv("FLINK_SQL_GATEWAY", "http://flink:8083")
    #: Base URL of the catalog under test (Lakekeeper by default, see ``PROFILE``).
    catalog_base: str = PROFILE.base_url

    @property
    def catalog_uri(self) -> str:
        """Iceberg REST catalog base URI, e.g. ``http://lakekeeper:8181/catalog``.

        This is what every engine is configured with (PyIceberg ``uri``, Spark
        ``spark.sql.catalog.<name>.uri``, Trino ``iceberg.rest-catalog.uri``, DuckDB
        ``ENDPOINT``, pg_lake ``rest_endpoint``). The engine then calls ``/v1/config`` with the
        warehouse name and receives the warehouse-specific URL prefix.

        Returns:
            The catalog URI.
        """
        return f"{self.catalog_base}{PROFILE.path}"

    @property
    def token_endpoint(self) -> str | None:
        """OAuth2 token endpoint the engines use: below ``catalog_base`` (so a proxy in front
        of the catalog sees the token request too), or the profile's absolute URL (an identity
        provider on another host, e.g. Keycloak for Gravitino).

        Returns:
            The token URL, or None without OAuth2.
        """
        p = PROFILE.token_path
        if not p:
            return None
        return p if "://" in p else f"{self.catalog_base.rstrip('/')}{p}"

    @property
    def s3_host_port(self) -> str:
        """S3 endpoint without scheme, e.g. ``rustfs:9000``.

        DuckDB's ``CREATE SECRET (TYPE s3, ENDPOINT ...)`` expects host:port only and takes the
        scheme from the separate ``USE_SSL`` flag.

        Returns:
            ``host:port`` of the S3 endpoint.
        """
        return self.s3_endpoint.split("://", 1)[1]


#: Settings instance built from the environment at import time.
SETTINGS = Settings()


@dataclass(frozen=True)
class Help:
    """A lab workaround for an engine that cannot connect to a catalog with its stock setup.

    Attributes:
        name: Short id.
        catalogs: Catalog profiles where the engine needs it.
        engines: Engines that get it.
        gap: The connection gap it steps over (the known gap of the run without help).
        workaround: What the lab does.
    """

    name: str
    catalogs: tuple[str, ...]
    engines: tuple[str, ...]
    gap: str
    workaround: str


#: Every lab help for an engine (the only place that lists them): sensible workarounds a user
#: of the catalog would apply too. The normal run uses them - a test that works with them counts
#: as working (the gap itself is shown once per engine by tests/security/
#: test_stock_connection.py). The run without help (``LAB_NO_HELP=1``) runs only the tests of
#: these engines again without them, reported next to the comparison. Configuration every user
#: of such a catalog needs (e.g. the S3 endpoint for Unity's vended keys) is not a help.
HELPS = (
    Help(
        "sigv4-content-hash",
        ("rustfs",),
        ("pyiceberg", "polars", "daft"),
        "PyIceberg's SigV4 adapter sends x-amz-content-sha256 only for empty bodies; RustFS "
        "rejects every signed POST without it",
        "the lab adds the header to every signed request (engines._add_content_sha256)",
    ),
    Help(
        "duckdb-aws-host",
        ("rustfs",),
        ("duckdb",),
        "DuckDB derives the SigV4 service from an AWS host name; rustfs:9000 cannot be attached",
        "the lab attaches the catalog under an AWS-style alias of RustFS (RUSTFS_AWS_HOST)",
    ),
)


def helped_engines(catalog: str | None = None) -> set[str]:
    """Engines that get a lab help on a catalog (see :data:`HELPS`).

    Args:
        catalog: Catalog profile name (default: the catalog under test).

    Returns:
        The engine names.
    """
    catalog = catalog or PROFILE.name
    return {e for h in HELPS if catalog in h.catalogs for e in h.engines}


#: The engines in :data:`HELPS` get the lab's workaround (default). ``LAB_NO_HELP=1``: the run
#: without help - only the tests of these engines, reported in reports/<catalog>-nohelp/, shown
#: next to the comparison, never counted.
WITH_HELP = os.getenv("LAB_NO_HELP") != "1"
#: Network alias of RustFS (.devcontainer/docker-compose.yml) that DuckDB can parse a SigV4
#: service ("s3") from.
RUSTFS_AWS_HOST = "rustfs-lab.s3.us-east-1.amazonaws.com"

# Two warehouses on the same bucket. They are configured identically except for ONE switch:
# how a query engine obtains access to the data files in S3.
#
# demo-signing: STS disabled -> Lakekeeper answers loadTable with remote-signing config
#               (s3.signer.uri / s3.signer.endpoint). The engine sends every S3 request to
#               Lakekeeper to get it signed and never sees any S3 key.
# demo-sts:     STS enabled  -> Lakekeeper calls AssumeRole on RustFS and hands the engine
#               short-lived credentials (access key, secret, session token) that are scoped
#               to the table's location via a session policy.
WAREHOUSE_STS = PROFILE.warehouse_sts
#: Remote-signing warehouse; for catalogs without remote signing the Lakekeeper name is kept
#: as a placeholder so tests can still be parametrized - they skip via ``REMOTE_SIGNING``.
WAREHOUSE_SIGNING = PROFILE.warehouse_signing or "demo-signing"
REMOTE_SIGNING = PROFILE.warehouse_signing is not None
#: Warehouses that exist in the catalog under test.
WAREHOUSES = (WAREHOUSE_SIGNING, WAREHOUSE_STS) if REMOTE_SIGNING else (WAREHOUSE_STS,)


def catalog_auth_props(s: Settings | None = None) -> dict[str, str]:
    """Iceberg REST client properties for the catalog's authentication (Java and PyIceberg).

    Empty for an open catalog that vends credentials (Lakekeeper here); an open catalog without
    vending (the reference fixture) gets the RustFS keys for the data files (``s3.*``). With
    OAuth2: client credentials, scope, token endpoint and the request for vended credentials
    (Polaris vends only on request). With SigV4 (RustFS): signing on, the RustFS keys for signing
    (Java ``rest.*``, PyIceberg ``client.*``) and - since nothing is vended - the same keys for
    the data files (``s3.*``).

    Args:
        s: Connection settings (default: the lab's).

    Returns:
        Client properties (``uri`` etc. not included).
    """
    s = s or SETTINGS
    if PROFILE.auth == "sigv4":
        return {
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3",
            "rest.signing-region": s.s3_region,
            "rest.access-key-id": s.s3_access_key,
            "rest.secret-access-key": s.s3_secret_key,
            "client.access-key-id": s.s3_access_key,
            "client.secret-access-key": s.s3_secret_key,
            "client.region": s.s3_region,
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.s3_access_key,
            "s3.secret-access-key": s.s3_secret_key,
            "s3.region": s.s3_region,
            "s3.path-style-access": "true",
        }
    if PROFILE.auth == "token":
        props = {
            "token": PROFILE.token or "",
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
        }
        if PROFILE.client_s3_endpoint:
            props |= {
                "s3.endpoint": s.s3_endpoint,
                "s3.path-style-access": "true",
                "s3.region": s.s3_region,
                "client.region": s.s3_region,
            }
        return props
    if not PROFILE.oauth_credential:
        if PROFILE.vended_credentials:
            return {}
        return {
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.s3_access_key,
            "s3.secret-access-key": s.s3_secret_key,
            "s3.region": s.s3_region,
            "s3.path-style-access": "true",
            "client.region": s.s3_region,
        }
    return {
        "credential": PROFILE.oauth_credential,
        "scope": PROFILE.oauth_scope or "",
        "oauth2-server-uri": s.token_endpoint,
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    }


def catalog_auth(s: Settings | None = None):
    """``requests`` auth for raw REST calls to the catalog (``auth=catalog_auth()``).

    SigV4 signs every request (incl. ``x-amz-content-sha256``, which RustFS requires) with the
    standard SigV4 path encoding (each ``%`` encoded again, as PyIceberg and Iceberg Java sign):
    botocore's S3 signer signs the path as sent, which RustFS refuses with 403 for every path
    that holds an encoded character (``%1F`` namespace separator, ``%2F``, non-ASCII names).
    OAuth2/open catalogs return ``None`` and use :func:`catalog_headers` instead.

    Args:
        s: Connection settings (default: the lab's).

    Returns:
        A SigV4 ``requests`` auth object, or None.
    """
    if PROFILE.auth != "sigv4":
        return None
    import requests
    from botocore.auth import S3SigV4Auth, SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    s = s or SETTINGS

    class _CatalogSigner(S3SigV4Auth):
        """S3 signer (payload hash header) with the standard SigV4 path encoding."""

        def _normalize_url_path(self, path):
            """Path as SigV4 (not S3) canonicalises it.

            Args:
                path: URL path.

            Returns:
                The canonical path.
            """
            return SigV4Auth._normalize_url_path(self, path)

    class _SigV4(requests.auth.AuthBase):
        """``requests`` auth that signs each request with the lab's RustFS keys (SigV4, service s3)."""

        def __call__(self, r):
            """Sign the request (with ``x-amz-content-sha256``) and copy the headers onto it.

            Args:
                r: The prepared request.

            Returns:
                The signed request.
            """
            req = AWSRequest(
                method=r.method,
                url=r.url,
                data=r.body or b"",
                headers={
                    k: v
                    for k, v in r.headers.items()
                    if k.lower() not in ("connection", "content-length")
                },
            )
            _CatalogSigner(
                Credentials(s.s3_access_key, s.s3_secret_key), "s3", s.s3_region
            ).add_auth(req)
            r.headers.update(dict(req.headers))
            return r

    return _SigV4()


def oauth_client(credential: str) -> tuple[str | None, str]:
    """Client id and secret of an OAuth ``credential``, as Iceberg clients read it.

    Args:
        credential: ``client_id:client_secret``, or a bare secret (Snowflake Horizon: the
            programmatic access token, sent without a client id).

    Returns:
        ``(client_id or None, client_secret)``.
    """
    client_id, sep, secret = credential.partition(":")
    return (client_id, secret) if sep else (None, credential)


def catalog_headers(s: Settings | None = None) -> dict[str, str]:
    """HTTP headers for raw REST calls to the catalog (a bearer token if it uses OAuth2).

    Args:
        s: Connection settings (default: the lab's).

    Returns:
        The headers (empty for open and SigV4 catalogs).
    """
    if PROFILE.auth == "token":
        return {"Authorization": f"Bearer {PROFILE.token}"}
    if PROFILE.auth != "oauth2":
        return {}
    import requests

    client_id, client_secret = oauth_client(PROFILE.oauth_credential)
    data = {"grant_type": "client_credentials", "client_secret": client_secret}
    if client_id is not None:
        data["client_id"] = client_id
    if PROFILE.oauth_scope:
        data["scope"] = PROFILE.oauth_scope
    url = s.token_endpoint if s else PROFILE.token_url
    r = requests.post(url, data=data, timeout=10)
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def rest_prefix_path(cfg: dict) -> str:
    """The prefix part of REST paths, from a ``/v1/config`` answer.

    The prefix is optional in the REST spec: without one (the Iceberg REST fixture) the paths
    are ``/v1/namespaces/...``, as PyIceberg builds them.

    Args:
        cfg: The JSON body of ``/v1/config``.

    Returns:
        ``"/<prefix>"`` or ``""``.
    """
    prefix = cfg.get("overrides", {}).get("prefix") or cfg.get("defaults", {}).get("prefix")
    return f"/{prefix}" if prefix else ""


def rest_base(warehouse: str, s: Settings | None = None) -> str:
    """Base URL of a warehouse's REST paths: ``<catalog>/v1[/<prefix>]``.

    Args:
        warehouse: Warehouse name (sent as ``?warehouse=`` to ``/v1/config``).
        s: Connection settings (default: the lab's).

    Returns:
        The URL without trailing slash.
    """
    import requests

    s = s or SETTINGS
    r = requests.get(
        f"{s.catalog_uri}/v1/config",
        params={"warehouse": warehouse},
        headers=catalog_headers(s),
        auth=catalog_auth(s),
        timeout=10,
    )
    r.raise_for_status()
    return f"{s.catalog_uri.rstrip('/')}/v1{rest_prefix_path(r.json())}"
