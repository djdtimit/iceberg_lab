"""Authentication and authorization - the same scenarios on every catalog.

Backends (how users, grants and sign-in work per catalog): tests/lab/authz_backends.py. Roles
and single privileges: tests/security/test_rbac.py, tests/security/test_privileges.py.
Identities: ``alice`` gets grants (read on ``t_select``, read+write on ``t_modify``, nothing
on ``t_none``), ``bob`` gets none; an admin sets up and verifies.

Catalog level (PyIceberg as client, engine-independent):
    anonymous requests are rejected; bob is refused; alice reads ``t_select`` but cannot
    append to it; vended keys for a read-only table are read-only (catalogs that vend); the
    remote signer refuses a PUT (catalogs that sign); ``t_none`` stays closed; ``t_modify``
    accepts appends; revoking the read grant takes effect; paging through a listing with
    hidden tables loses and repeats nothing; who-am-I names the signed-in user. Only a
    permission error counts as a refusal (on Lakekeeper also its deliberate "not found").

Engine matrix: every engine signs in as alice and bob with its own configuration (the way
it signs in to that catalog elsewhere): alice reads, bob is refused, alice can append to
``t_modify`` (positive control) but not to ``t_select``.
"""

from __future__ import annotations

import re
import uuid

import pyarrow as pa
import pytest
import requests
from authz_backends import ROWS, Conn, backend, pyiceberg_for

from iceberg_lab.config import PROFILE, SETTINGS

from conftest import ENGINES, engine_param, expect_gap, require_engine

#: What a refusal looks like: a permission or authentication error. Any other error (syntax,
#: connection, driver) must NOT pass as "correctly refused".
PERMISSION = (
    r"401|403|forbidden|unauthori[sz]ed|not authori[sz]ed|permission|access[ _]?denied|"
    r"AccessDenied|not allowed|AuthenticationFailed"
)
#: "Does not exist" counts as a refusal only on catalogs that deliberately hide objects a user
#: may not see (measured: Lakekeeper answers bob with NoSuchWarehouse and alice's table without
#: grant with the same NoSuchTable as a missing table; Polaris, RustFS and Unity answer 403).
#: The refusal tests also check with the admin that the object exists, so a setup error cannot
#: pass as a refusal.
HIDING_CATALOGS = {"lakekeeper"}
HIDDEN = r"does not exist|not found|cannot be found|NoSuch\w+"  # Spark: TABLE_OR_VIEW_NOT_FOUND
DENIED = (
    f"(?i)({PERMISSION}|{HIDDEN})" if PROFILE.name in HIDING_CATALOGS else f"(?i)({PERMISSION})"
)


@pytest.fixture(scope="module")
def az():
    """The catalog's permission backend with a fresh namespace (tables + grants).

    Yields:
        ``(backend, namespace)``.
    """
    b = backend()
    ns = b.setup_namespace()
    yield b, ns
    b.drop_namespace(ns)


def exists(b, ns, table):
    """The admin sees the table with its rows - a refusal below is a refusal, not a typo.

    Args:
        b: Permission backend.
        ns: Namespace.
        table: Table name (without namespace).
    """
    assert b.rows(ns, table) >= 0


# =========================================================================== catalog level


def test_anonymous_request_is_rejected(az):
    """A request without any credentials is refused.

    Checks:
        * ``GET /v1/config`` without credentials answers 401 or 403.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, _ = az
    r = b.anonymous_config()
    assert r.status_code in (401, 403), f"{r.status_code}: {r.text[:200]}"


def test_user_without_grants_is_refused(az):
    """bob (no grant) cannot read a table that exists.

    Checks:
        * the admin sees t_select with its rows (so the refusal is no typo);
        * bob's load and scan of t_select fail with a permission error (``DENIED``: 401/403,
          on Lakekeeper also its deliberate "does not exist").

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    exists(b, ns, "t_select")
    with pytest.raises(Exception, match=DENIED):
        b.catalog("bob").load_table(f"{ns}.t_select").scan().to_arrow()


def test_select_grant_reads(az):
    """alice reads the table she has a read grant on.

    Checks:
        * alice loads and scans t_select and gets all of its rows.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    assert b.catalog("alice").load_table(f"{ns}.t_select").scan().to_arrow().num_rows == ROWS


def test_select_grant_cannot_append(az):
    """A read grant does not allow writing.

    Checks:
        * alice's append to t_select fails with a permission error;
        * the admin still sees exactly the original rows (nothing was committed).

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    t = b.catalog("alice").load_table(f"{ns}.t_select")
    with _gap("catalog", "select_append"), pytest.raises(Exception, match=DENIED):
        t.append(pa.table({"id": [99]}))
    assert b.rows(ns, "t_select") == ROWS


def test_select_grant_vends_read_only_credentials(az):
    """The storage keys vended for a read-only table can read but not write.

    Checks:
        * with the keys from alice's ``loadTable`` of t_select, a HEAD of the metadata file
          succeeds;
        * a PUT of a new file into the table's data directory is refused (403 / AccessDenied).
        Catalogs that vend no keys: not offered.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    import boto3
    from botocore.exceptions import ClientError

    b, ns = az
    if not b.vended:
        pytest.skip(f"catalog {PROFILE.name} has no API for vended storage credentials")
    t = b.catalog("alice").load_table(f"{ns}.t_select")
    p = t.io.properties
    s3 = boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=p["s3.access-key-id"],
        aws_secret_access_key=p["s3.secret-access-key"],
        aws_session_token=p.get("s3.session-token"),
    )
    bucket, key = t.metadata_location.removeprefix("s3://").split("/", 1)
    s3.head_object(Bucket=bucket, Key=key)
    prefix = t.location().removeprefix("s3://").split("/", 1)[1]
    with pytest.raises(ClientError) as exc:
        s3.put_object(Bucket=bucket, Key=f"{prefix}/data/evil.bin", Body=b"x")
    assert exc.value.response["Error"]["Code"] in ("403", "AccessDenied")


def test_signer_refuses_put_for_select_grant(az):
    """Remote signing: alice's GET is signed, her PUT is not.

    Checks:
        * the signer signs a GET of t_select's metadata file for alice;
        * it refuses (401/403) to sign a PUT of a new data file in that table.
        Catalogs without remote signing: not offered.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    if not b.signing_warehouse:
        pytest.skip(f"catalog {PROFILE.name} has no API for remote signing")
    t = pyiceberg_for(b.conn("alice", b.signing_warehouse)).load_table(f"{ns}.t_select")
    p = t.io.properties
    url = p["s3.signer.uri"].rstrip("/") + "/" + p["s3.signer.endpoint"]
    bucket, key = t.metadata_location.removeprefix("s3://").split("/", 1)

    def sign(method, k):
        """Ask the remote signer, as alice, to sign a request.

        Args:
            method: HTTP method to sign.
            k: Object key.

        Returns:
            The signer's response.
        """
        return requests.post(
            url,
            json={
                "region": SETTINGS.s3_region,
                "method": method,
                "uri": f"{SETTINGS.s3_endpoint}/{bucket}/{k}",
                "headers": {},
            },
            headers=b.bearer("alice"),
            timeout=10,
        )

    assert sign("GET", key).ok
    refused = sign("PUT", key.rsplit("/", 2)[0] + "/data/evil.parquet")
    assert refused.status_code in (401, 403), refused.text


def test_table_without_grant_stays_closed(az):
    """alice cannot read a table she has no grant on.

    Checks:
        * the admin sees t_none;
        * alice's load and scan of t_none fail with a permission error.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    exists(b, ns, "t_none")
    with pytest.raises(Exception, match=DENIED):
        b.catalog("alice").load_table(f"{ns}.t_none").scan().to_arrow()


def test_table_without_grant_hides_metadata(az):
    """alice may not even load the metadata (schema, location) of a table without grant.

    Checks:
        * the admin sees t_none;
        * alice's ``loadTable`` of t_none fails, and only with a permission error - a
          successful load is a finding (RustFS: catalog rights per bucket only).

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    exists(b, ns, "t_none")
    with _gap("catalog", "metadata"):
        try:
            b.catalog("alice").load_table(f"{ns}.t_none")
        except Exception as exc:  # noqa: BLE001 - the refusal must be one, not any error
            assert re.search(DENIED, f"{type(exc).__name__}: {exc}"), exc
        else:
            raise AssertionError("alice loaded the metadata of a table she has no grant on")


def _list_pages(cat, ns: str, page_size: int | None) -> list[list[str]]:
    """Table names page by page (raw list call through the user's authenticated session).

    Args:
        cat: The user's PyIceberg catalog.
        ns: Namespace.
        page_size: Page size (None: unpaged).

    Returns:
        Per page the table names.
    """
    prefix = cat.properties.get("prefix")
    url = f"{cat.uri.rstrip('/')}/v1/{prefix + '/' if prefix else ''}namespaces/{ns}/tables"
    pages, token = [], None
    for _ in range(50):
        params = {"pageSize": page_size} if page_size else {}
        if token:
            params["pageToken"] = token
        r = cat._session.get(url, params=params)
        r.raise_for_status()
        body = r.json()
        pages.append([i["name"] for i in body.get("identifiers", [])])
        token = body.get("next-page-token")
        if not token or not page_size:
            return pages
    raise AssertionError("pagination does not end after 50 pages")


def test_listing_with_hidden_tables(az):
    """Paging through a listing with hidden tables loses and repeats nothing.

    Six tables alice may not read sort between the ones she may (Lakekeeper 0.15.1 fixed
    listings that lost entries when some were hidden).

    Checks:
        * alice may list the namespace (else: not testable on this catalog);
        * page size 2: no table twice across the pages;
        * the paged names equal the unpaged list;
        * t_select and t_modify are in it.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    admin = b.catalog("admin")
    hidden = [f"t_h{i}" for i in range(6)]  # sort between t_modify, t_none and t_select
    for name in hidden:
        admin.create_table(f"{ns}.{name}", schema=pa.schema([("id", pa.int64())]))
    try:
        b.allow_listing(ns)
        alice = b.catalog("alice")
        try:
            whole = _list_pages(alice, ns, None)[0]
        except requests.HTTPError as exc:
            assert re.search(DENIED, str(exc)), exc
            pytest.skip(f"catalog {PROFILE.name}: alice may not list the namespace ({exc})")
        pages = _list_pages(alice, ns, 2)
        paged = [n for page in pages for n in page]
        with _gap("catalog", "listing"):
            assert len(paged) == len(set(paged)), f"tables twice: {pages}"
            assert sorted(paged) == sorted(whole), f"paged {sorted(paged)} != whole {sorted(whole)}"
            assert {"t_select", "t_modify"} <= set(whole), f"alice's tables missing: {whole}"
    finally:
        for name in hidden:
            admin.drop_table(f"{ns}.{name}")


def test_modify_grant_allows_append(az):
    """A write grant allows appending (positive control of the refusals).

    Checks:
        * alice appends one row to t_modify and the admin sees one row more.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    t = b.catalog("alice").load_table(f"{ns}.t_modify")
    t.append(pa.table({"id": [4]}))
    assert b.rows(ns, "t_modify") == ROWS + 1


def test_revoke_takes_effect(az):
    """After revoking the read grant, alice can no longer read the table - at once.

    Checks:
        * the admin sees t_select;
        * right after the revoke, alice's load and scan fail with a permission error
          (the grant is restored afterwards for the other tests).

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, ns = az
    exists(b, ns, "t_select")
    b.grant(ns, "t_select", write=False, revoke=True)
    try:
        with pytest.raises(Exception, match=DENIED):
            b.catalog("alice").load_table(f"{ns}.t_select").scan().to_arrow()
    finally:
        b.grant(ns, "t_select", write=False)


def test_whoami(az):
    """The catalog tells a signed-in user who it is.

    Checks:
        * the catalog's who-am-I answer for alice names alice and not bob.
        Catalogs without such an API: not offered.

    Args:
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
    """
    b, _ = az
    if not b.whoami:
        pytest.skip(f"catalog {PROFILE.name} has no API for asking who am I")
    me = b.whoami("alice")
    assert "alice" in me and "bob" not in me, me


# =========================================================================== engines


def engine_read(engine, user, b, ns, table, request) -> int:
    """Row count of ``ns.table`` read by ``engine`` signed in as ``user``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        user: Lab user (``alice``, ``bob`` or ``admin``).
        b: Permission backend.
        ns: Namespace.
        table: Table name (without namespace).
        request: pytest request of the calling test or fixture.

    Returns:
        The row count.
    """
    require_engine(engine)
    if engine in ("pyiceberg", "polars", "daft"):
        t = pyiceberg_for(b.conn(user), lab_fixes=False).load_table(f"{ns}.{table}")
        if engine == "pyiceberg":
            return t.scan().to_arrow().num_rows
        if engine == "polars":
            import polars as pl

            from iceberg_lab.engines import polars_storage_options

            return (
                pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties))
                .collect()
                .height
            )
        import daft

        return daft.read_iceberg(t).count_rows()
    return engine_sql(engine, user, b, request, lambda fq: f"SELECT count(*) FROM {fq(ns, table)}")[
        0
    ][0]


def _duckdb(c: Conn, make_sql):
    """Run the statement in a DuckDB attached as the identity.

    Args:
        c: Connection of the identity.
        make_sql: fq -> SQL statement.

    Returns:
        The result rows.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    mode = "vended_credentials" if c.vended else "none"
    if c.auth == "oauth2":
        cid, sec = c.credential.split(":", 1)
        scope = f", OAUTH2_SCOPE '{c.scope}'" if c.scope else ""
        con.execute(f"""CREATE SECRET az (TYPE iceberg, CLIENT_ID '{cid}', CLIENT_SECRET '{sec}',
            OAUTH2_SERVER_URI '{c.token_url}'{scope})""")
        auth = "SECRET az"
    else:  # sigv4: the S3 secret signs the catalog requests and serves the data files
        ak, sk = c.keys
        con.execute(f"""CREATE SECRET az_s3 (TYPE s3, KEY_ID '{ak}', SECRET '{sk}',
            ENDPOINT '{SETTINGS.s3_host_port}', URL_STYLE 'path', USE_SSL false,
            REGION '{SETTINGS.s3_region}')""")
        auth = "AUTHORIZATION_TYPE 'sigv4', SECRET az_s3"
    if c.s3_endpoint:
        con.execute(f"""SET s3_endpoint = '{SETTINGS.s3_host_port}'; SET s3_url_style = 'path';
            SET s3_use_ssl = false; SET s3_region = '{SETTINGS.s3_region}'""")
    con.execute(f"""ATTACH '{c.warehouse}' AS az (TYPE iceberg, ENDPOINT '{c.uri}', {auth},
        ACCESS_DELEGATION_MODE '{mode}')""")
    try:
        return con.execute(make_sql(lambda n, t: f'az."{n}"."{t}"')).fetchall()
    finally:
        con.close()


#: Iceberg REST client properties of the main catalog that the per-user catalog replaces.
AUTH_KEYS = ("uri", "warehouse", "credential", "scope", "oauth2-server-uri", "token")
AUTH_PREFIXES = ("rest.", "client.", "s3.", "header.")


def _spark(c: Conn, user, request, make_sql):
    """Run the statement in a Spark catalog configured for the identity.

    Args:
        c: Connection of the identity.
        user: Lab user (``alice``, ``bob`` or ``admin``).
        request: pytest request of the calling test or fixture.
        make_sql: fq -> SQL statement.

    Returns:
        The result rows.
    """
    from iceberg_lab.config import WAREHOUSE_STS
    from iceberg_lab.engines import SPARK_CATALOGS

    spark = request.getfixturevalue("spark")
    src, dst = f"spark.sql.catalog.{SPARK_CATALOGS[WAREHOUSE_STS]}", f"spark.sql.catalog.az_{user}"
    for k, v in spark.sparkContext.getConf().getAll():
        if k == src or k.startswith(src + "."):
            sub = k[len(src) + 1 :]
            if sub in AUTH_KEYS or sub.startswith(AUTH_PREFIXES):
                continue
            spark.conf.set(k.replace(src, dst, 1), v)
    for k, v in c.java_props().items():
        spark.conf.set(f"{dst}.{k}", v)
    return [tuple(r) for r in spark.sql(make_sql(lambda n, t: f"az_{user}.{n}.{t}")).collect()]


def _flink(c: Conn, user, make_sql):
    """Run the statement in a Flink catalog configured for the identity.

    Args:
        c: Connection of the identity.
        user: Lab user (``alice``, ``bob`` or ``admin``).
        make_sql: fq -> SQL statement.

    Returns:
        The result rows.
    """
    from iceberg_lab.config import WAREHOUSE_STS
    from iceberg_lab.engines import FlinkEngine

    f = FlinkEngine(WAREHOUSE_STS)
    try:
        props = "".join(f", '{k}'='{v}'" for k, v in c.java_props().items())
        f.execute(f"CREATE CATALOG az_{user} WITH ('type'='iceberg', 'catalog-type'='rest'{props})")
        return f.execute(make_sql(lambda n, t: f"az_{user}.`{n}`.`{t}`"))
    finally:
        f.close()


def _pglake(c: Conn, user, make_sql):
    """Run the statement through a pg_lake server configured for the identity.

    Args:
        c: Connection of the identity.
        user: Lab user (``alice``, ``bob`` or ``admin``).
        make_sql: fq -> SQL statement.

    Returns:
        The result rows.
    """
    from iceberg_lab.config import WAREHOUSE_STS
    from iceberg_lab.engines import PgLakeEngine

    pg = PgLakeEngine(WAREHOUSE_STS)
    server = f"az_{PROFILE.name}_{user}"
    cid, sec = c.credential.split(":", 1)
    scope = f", scope '{c.scope}'" if c.scope else ""
    location = PROFILE.location or f"s3://{SETTINGS.s3_bucket}/{c.warehouse}"
    try:
        pg.execute(f"DROP SERVER IF EXISTS {server} CASCADE")
        pg.execute(f"""CREATE SERVER {server} TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{c.uri}', warehouse '{c.warehouse}',
                     oauth_endpoint '{c.token_url}'{scope},
                     location_prefix '{location}/pg_lake')""")
        pg.execute(f"""CREATE USER MAPPING FOR PUBLIC SERVER {server}
            OPTIONS (client_id '{cid}', client_secret '{sec}')""")

        def fq(n, t):
            local = f"{t}_{user}_{uuid.uuid4().hex[:4]}"
            pg.execute(f'CREATE SCHEMA IF NOT EXISTS "{n}"')
            pg.execute(f"""CREATE TABLE "{n}"."{local}" () USING iceberg
                WITH (catalog = '{server}', read_only = true,
                      catalog_namespace = '{n}', catalog_table_name = '{t}')""")
            return f'"{n}"."{local}"'

        return pg.execute_raw(make_sql(fq))
    finally:
        pg.close()


def engine_sql(engine, user, b, request, make_sql):
    """Run ``make_sql(fq)`` in ``engine`` signed in to the catalog as ``user``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        user: Lab user (``alice``, ``bob`` or ``admin``).
        b: Permission backend.
        request: pytest request of the calling test or fixture.
        make_sql: fq -> SQL statement.

    Returns:
        The result rows.
    """
    c = b.conn(user)
    if engine == "duckdb":
        return _duckdb(c, make_sql)
    if engine == "spark":
        return _spark(c, user, request, make_sql)
    if engine == "flink":
        return _flink(c, user, make_sql)
    if engine == "pg_lake":
        return _pglake(c, user, make_sql)
    if engine == "trino":
        import trino

        cat = b.trino[user]
        conn = trino.dbapi.connect(
            host=SETTINGS.trino_host, port=SETTINGS.trino_port, user="tests", catalog=cat
        )
        cur = conn.cursor()
        cur.execute(make_sql(lambda n, t: f'{cat}."{n}"."{t}"'))
        return cur.fetchall()
    raise ValueError(engine)


# Filled from observed failures only: (catalog glob, engine, user/test, regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple] = [
    # Trino hides the reason of a refusal behind generic messages (seen on every catalog).
    ("*", "trino", "write", r"Error committing write to Parquet file", "Trino reports a refused write (read-only keys) only as 'Error committing write to Parquet file' - the S3 AccessDenied is not shown"),
    # On Lakekeeper Trino shows the refusal; on these catalogs only a generic message:
    *((c, "trino", "bob", r"Failed to load (table|view)\W+t_select|Cannot obtain metadata", "Trino reports a refused table load (403) only as 'Failed to load table' / 'Failed to load view' / 'Cannot obtain metadata' - the refusal is not shown", c == "unity")
      for c in ("polaris", "rustfs", "unity")),  # Unity: Trino sometimes shows the 403 (intermittent)
    # Polaris authorizes /transactions/commit only with TABLE_FULL_METADATA (checked:
    # TABLE_WRITE_DATA, TABLE_WRITE_PROPERTIES, NAMESPACE_READ_PROPERTIES are not enough).
    # RustFS: catalog rights (admin:GetTable, ...) can be scoped per bucket only, not per table;
    # only the S3 prefix policy protects the data, so a reader loads every table's metadata.
    ("rustfs", "catalog", "metadata", r"alice loaded the metadata of a table she has no grant on", "SECURITY: RustFS catalog rights can be scoped per bucket only, not per table: a user who may read one table also loads the metadata (schema, storage location) of every other table in the warehouse - only reading the data is refused by the S3 policy"),
    ("polaris", "duckdb", "write_allowed", r"not authorized for op COMMIT_TRANSACTION", "Polaris requires TABLE_FULL_METADATA for /transactions/commit (TABLE_WRITE_DATA suffices for a normal commit) - DuckDB always commits that way, so a write grant does not let it write"),
]
# fmt: on


def _gap(engine, what):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        what: Gap key (user or case).

    Returns:
        The context manager.
    """
    import fnmatch

    for cat, e, w, regex, reason, *flaky in KNOWN_GAPS:
        if fnmatch.fnmatch(PROFILE.name, cat) and e == engine and w == what:
            return expect_gap(reason, regex, flaky=bool(flaky and flaky[0]))
    return expect_gap(None, None)


@pytest.mark.slow
@pytest.mark.parametrize("user", ["alice", "bob"])
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_engine_reads(engine, user, az, request):
    """Through ``engine``, alice (read grant) reads all rows and bob (no grant) is refused.

    Checks:
        * alice: the engine returns all rows of t_select;
        * bob: the admin sees t_select, and the engine's read fails with a permission error
          (a successful read fails the test with the row count).

    Args:
        engine: Engine under test; it signs in as the user the way it does elsewhere.
        user: ``alice`` (read grant) or ``bob`` (none).
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    b, ns = az
    with _gap(engine, user):
        if user == "alice":
            assert engine_read(engine, user, b, ns, "t_select", request) == ROWS
        else:
            exists(b, ns, "t_select")
            with pytest.raises(Exception, match=DENIED):
                n = engine_read(engine, user, b, ns, "t_select", request)
                pytest.fail(f"bob read {n} rows without any grant")  # pragma: no cover


def engine_append(engine, user, b, ns, table, request):
    """Append one row to ``ns.table`` through ``engine`` signed in as ``user``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        user: Lab user (``alice``, ``bob`` or ``admin``).
        b: Permission backend.
        ns: Namespace.
        table: Table name (without namespace).
        request: pytest request of the calling test or fixture.
    """
    if engine in ("pyiceberg", "polars", "daft"):
        t = pyiceberg_for(b.conn(user), lab_fixes=False).load_table(f"{ns}.{table}")
        data = pa.table({"id": [99]})
        if engine == "pyiceberg":
            t.append(data)
        elif engine == "polars":
            import polars as pl

            pl.from_arrow(data).write_iceberg(t, mode="append")
        else:
            import daft

            daft.from_arrow(data).write_iceberg(t, mode="append")
    else:
        engine_sql(engine, user, b, request, lambda fq: f"INSERT INTO {fq(ns, table)} VALUES (99)")


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_engine_write_allowed_with_modify_grant(engine, az, request):
    """Positive control: the engine's write path works on the table alice may modify.

    Without it, a broken write path (syntax, driver) in the denial test would look like a
    correct refusal.

    Checks:
        * alice appends one row through the engine to t_modify, and the admin sees one row more.
        pg_lake: not offered (cannot write tables it did not create).

    Args:
        engine: Engine under test; it signs in as the user the way it does elsewhere.
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    if engine == "pg_lake":
        pytest.skip("pg_lake has no API for writing tables it did not create")
    require_engine(engine)
    b, ns = az
    before = b.rows(ns, "t_modify")
    with _gap(engine, "write_allowed"):
        engine_append(engine, "alice", b, ns, "t_modify", request)
        assert b.rows(ns, "t_modify") == before + 1


@pytest.mark.slow
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_engine_write_denied_with_select_grant(engine, az, request):
    """alice (read only) cannot append through ``engine``; the table stays unchanged.

    Checks:
        * the engine's append to t_select as alice fails with a permission error;
        * the admin still sees exactly the original rows.
        pg_lake: not offered (cannot write tables it did not create).

    Args:
        engine: Engine under test; it signs in as the user the way it does elsewhere.
        az: ``(backend, namespace)`` - the catalog's permission backend and the module's
            namespace with t_select (alice: read), t_modify (alice: read + write) and t_none
            (no grant); bob has no grant at all.
        request: pytest request, to fetch engine fixtures (SparkSession, ...) lazily.
    """
    if engine == "pg_lake":
        pytest.skip("pg_lake has no API for writing tables it did not create")
    require_engine(engine)
    b, ns = az
    exists(b, ns, "t_select")
    with _gap(engine, "write"):
        with pytest.raises(Exception, match=DENIED):
            engine_append(engine, "alice", b, ns, "t_select", request)
        n = b.rows(ns, "t_select")
        assert n == ROWS, f"table changed: {n} rows"
