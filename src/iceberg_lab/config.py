"""Central connection settings for the lab.

``CATALOG`` selects one of the catalog profiles (:data:`PROFILES`: lakekeeper, polaris,
rustfs, unity) - its URL, authentication (open, OAuth2, bearer token or SigV4), warehouses
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
        token_path: Token endpoint below ``base_url`` (only with ``oauth_credential``).
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

    @property
    def token_url(self) -> str | None:
        """Full OAuth2 token endpoint of the catalog, if it has one.

        Returns:
            The token URL, or None.
        """
        return f"{self.base_url}{self.token_path}" if self.token_path else None


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
}
CATALOG = os.getenv("CATALOG", "lakekeeper")
if CATALOG not in PROFILES:
    raise ValueError(f"CATALOG={CATALOG!r} unknown; choose one of {sorted(PROFILES)}")
#: The catalog this process runs against.
PROFILE = PROFILES[CATALOG]


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

    Empty for an open catalog (Lakekeeper here). With OAuth2: client credentials, scope,
    token endpoint and the request for vended credentials (Polaris vends only on request).
    With SigV4 (RustFS): signing on, the RustFS keys for signing (Java ``rest.*``, PyIceberg
    ``client.*``) and - since nothing is vended - the same keys for the data files (``s3.*``).

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
        return {}
    base = s.catalog_base.rstrip("/")
    return {
        "credential": PROFILE.oauth_credential,
        "scope": PROFILE.oauth_scope or "",
        "oauth2-server-uri": f"{base}{PROFILE.token_path}",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    }


def catalog_auth(s: Settings | None = None):
    """``requests`` auth for raw REST calls to the catalog (``auth=catalog_auth()``).

    SigV4 signs every request (incl. ``x-amz-content-sha256``, which RustFS requires);
    OAuth2/open catalogs return ``None`` and use :func:`catalog_headers` instead.

    Args:
        s: Connection settings (default: the lab's).

    Returns:
        A SigV4 ``requests`` auth object, or None.
    """
    if PROFILE.auth != "sigv4":
        return None
    import requests
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    s = s or SETTINGS

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
            S3SigV4Auth(Credentials(s.s3_access_key, s.s3_secret_key), "s3", s.s3_region).add_auth(
                req
            )
            r.headers.update(dict(req.headers))
            return r

    return _SigV4()


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

    base = (s.catalog_base if s else PROFILE.base_url).rstrip("/")
    client_id, client_secret = PROFILE.oauth_credential.split(":", 1)
    r = requests.post(
        f"{base}{PROFILE.token_path}",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": PROFILE.oauth_scope,
        },
        timeout=10,
    )
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}
