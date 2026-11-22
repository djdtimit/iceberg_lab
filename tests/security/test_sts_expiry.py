"""Long-lived clients after the vended STS credentials EXPIRED - on every catalog that vends.

Every engine opens its table/session once at t0 and reads; the test then waits until the
vended keys have expired (expiry time taken from the keys themselves) and the same table
object / session reads again. Lifetimes: Lakekeeper 900 s (own warehouse ``demo-sts-short``,
``sts-token-validity-seconds``), Polaris and Unity 1 h (Polaris: only realm-wide
configurable - a catalog property is refused as reserved; Unity: fixed in the code,
``Duration.ofHours(1)``), so the wait is ~16 min resp. ~62 min. RustFS vends nothing ("no
API").

Engines that load the table per query get fresh keys anyway; clients that hold a loaded
table object (PyIceberg, and Polars/Daft reading such an object) keep the old keys unless
they refresh them via ``client.refresh-credentials-endpoint``. pg_lake uses its own static
S3 secret (not applicable). One shared wait for all engines (~16 min, marker ``long``).
"""

from __future__ import annotations

import time
import uuid

import pyarrow as pa
import pytest
import requests

from iceberg_lab import engines
from iceberg_lab.bootstrap import warehouse_payload
from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSE_STS, catalog_auth_props

from conftest import AVAILABILITY, ENGINES, engine_param, expect_gap, require_vended_credentials

#: Lakekeeper: a dedicated short-lived warehouse; elsewhere the main one (1 h keys).
WH = "demo-sts-short" if PROFILE.name == "lakekeeper" else WAREHOUSE_STS
TRINO_CATALOG = "lk_sts_short" if PROFILE.name == "lakekeeper" else f"{PROFILE.trino_prefix}_sts"
VALIDITY = 900
# Polaris and Unity vend 1 h keys: the shared fixture waits ~62 min (global timeout: 30 min).
pytestmark = [pytest.mark.slow, pytest.mark.long, pytest.mark.timeout(5400)]

# Filled from observed failures only: engine -> (regex, reason).
#: How RustFS refuses expired STS keys - observed both: 403 AccessDenied and 400 ExpiredToken.
EXPIRED = (
    r"after expiry: .*(ACCESS_DENIED|ExpiredToken|HTTP status 400|lacked the necessary privileges)"
)

KNOWN_GAPS: dict[str, tuple[str, str]] = {
    "pyiceberg": (
        EXPIRED,
        "PyIceberg 0.12: a loaded table keeps its vended keys - no refresh via client.refresh-credentials-endpoint after expiry",
    ),
    # Polars and Daft read the PyIceberg table object (and its keys) they were given.
    "polars": (
        EXPIRED,
        "Polars reading a held PyIceberg table object: expired vended keys are not refreshed",
    ),
    "daft": (
        EXPIRED,
        "Daft reading a held PyIceberg table object: expired vended keys are not refreshed",
    ),
}


def ensure_warehouse():
    """Create the short-lived-keys warehouse (Lakekeeper only)."""
    if PROFILE.name != "lakekeeper":
        return  # the main warehouse
    s = SETTINGS
    have = requests.get(f"{s.lakekeeper_url}/management/v1/warehouse", timeout=10).json()[
        "warehouses"
    ]
    if WH not in {w["name"] for w in have}:
        p = warehouse_payload(s, WH, True)
        p["storage-profile"]["sts-token-validity-seconds"] = VALIDITY
        requests.post(
            f"{s.lakekeeper_url}/management/v1/warehouse", json=p, timeout=10
        ).raise_for_status()


def open_reader(engine, ns, table, request):
    """Open ``engine`` on the table at t0; return a ``read() -> row count`` closure.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        ns: Namespace.
        table: Table name (without namespace).
        request: pytest request of the calling test or fixture.

    Returns:
        The closure.
    """
    if engine in ("pyiceberg", "polars", "daft"):
        t = engines.pyiceberg_catalog(WH).load_table(f"{ns}.{table}")  # held across the wait
        if engine == "pyiceberg":
            return lambda: t.scan().to_arrow().num_rows
        if engine == "polars":
            import polars as pl

            opts = engines.polars_storage_options(t.io.properties)
            return lambda: pl.scan_iceberg(t, storage_options=opts).collect().height
        import daft

        return lambda: daft.read_iceberg(t).count_rows()
    if engine == "duckdb":
        d = engines.DuckDBEngine(WH)
        return lambda: d.execute(f"SELECT count(*) FROM {d.fq(ns, table)}")[0][0]
    if engine == "spark":
        spark = request.getfixturevalue("spark")
        src = f"spark.sql.catalog.{engines.SPARK_CATALOGS[engines.WAREHOUSE_STS]}"
        for k, v in spark.sparkContext.getConf().getAll():
            if k == src or k.startswith(src + "."):
                spark.conf.set(k.replace(src, "spark.sql.catalog.stsshort", 1), v)
        spark.conf.set("spark.sql.catalog.stsshort.warehouse", WH)
        return lambda: spark.sql(f"SELECT count(*) FROM stsshort.{ns}.{table}").first()[0]
    if engine == "trino":
        import trino

        conn = trino.dbapi.connect(
            host=SETTINGS.trino_host, port=SETTINGS.trino_port, user="tests", catalog=TRINO_CATALOG
        )

        def read():
            cur = conn.cursor()
            cur.execute(f'SELECT count(*) FROM "{ns}"."{table}"')
            return cur.fetchall()[0][0]

        return read
    if engine == "flink":
        f = engines.FlinkEngine(engines.WAREHOUSE_STS)
        auth = "".join(f", '{k}'='{v}'" for k, v in catalog_auth_props(SETTINGS).items())
        f.execute(
            f"CREATE CATALOG stsshort WITH ('type'='iceberg', 'catalog-type'='rest', "
            f"'uri'='{SETTINGS.catalog_uri}', 'warehouse'='{WH}'{auth})"
        )
        return lambda: f.execute(f"SELECT count(*) FROM stsshort.`{ns}`.`{table}`")[0][0]
    raise ValueError(engine)


@pytest.fixture(scope="module")
def after_expiry(request):
    """Open every available engine at t0, read once, wait for expiry; engine -> outcome.

    Args:
        request: pytest request of the calling test or fixture.

    Yields:
        Engine -> outcome.
    """
    require_vended_credentials()
    ensure_warehouse()
    cat = engines.pyiceberg_catalog(WH)
    ns, table = f"exp_{uuid.uuid4().hex[:6]}", "t"
    cat.create_namespace(ns)
    t = cat.create_table(f"{ns}.{table}", schema=pa.schema([("id", pa.int64())]))
    t.append(pa.table({"id": [1, 2, 3]}))
    expires = int(t.io.properties["s3.session-token-expires-at-ms"]) / 1000
    readers, outcome = {}, {}
    for e in ENGINES:
        if e == "pg_lake" or not AVAILABILITY[e][0]:
            continue
        try:
            readers[e] = open_reader(e, ns, table, request)
            outcome[e] = {"before": readers[e]()}
        except Exception as exc:  # noqa: BLE001 - reported by the engine's test
            outcome[e] = {"before": exc}
    # Keys of this load expire at `expires`; engines that loaded slightly later hold keys
    # valid a bit longer, hence the margin.
    time.sleep(max(0.0, expires - time.time()) + 90)
    for e, read in readers.items():
        try:
            outcome[e]["after"] = read()
        except Exception as exc:  # noqa: BLE001
            outcome[e]["after"] = exc
    yield outcome
    for t in cat.list_tables(ns):
        cat.purge_table(t)
    cat.drop_namespace(ns)


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_read_after_credentials_expired(engine, after_expiry):
    """The same table object / session still reads after its vended keys expired.

    Checks:
        * before and after the expiry the engine reads all 3 rows (clients that hold a table
          object must refresh the keys - PyIceberg, Polars, Daft do not: documented gap).
        pg_lake (static key): not offered.

    Args:
        engine: Engine under test.
        after_expiry: ``engine -> outcome`` - every engine opened at t0, read once, then read
            again after the short-lived keys expired.
    """
    if engine == "pg_lake":
        pytest.skip("pg_lake has no API for vended credentials (static S3 secret)")
    res = after_expiry[engine]
    gap = KNOWN_GAPS.get(engine)
    with expect_gap(gap[1] if gap else None, gap[0] if gap else None):
        for phase in ("before", "after"):
            v = res.get(phase)
            if isinstance(v, Exception):
                raise AssertionError(f"{phase} expiry: {type(v).__name__}: {v}") from v  # noqa: TRY004
            assert v == 3, f"{phase} expiry: {v} rows"
