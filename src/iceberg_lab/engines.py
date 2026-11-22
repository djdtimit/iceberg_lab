"""Uniform adapters around every query engine of the lab.

Why adapters?
    The tests want to ask questions like "can engine A read what engine B wrote?" for every
    pair of engines. That only works if all engines offer the same small interface, hidden
    behind which each engine uses its *own* native API (SQL dialect, DataFrame API, ...).
    The adapters deliberately stay thin: they translate, they do not work around bugs. A
    limitation of an engine must surface as a failing test, not be papered over here.

Interface (see :class:`Engine`):
    ``write(namespace, table, data)``
        Create ``namespace.table`` with the engine's own DDL/API and insert ``data`` (a
        :class:`pyarrow.Table` with the columns of :data:`SCHEMA`).
    ``read(namespace, table)``
        Read the table back and return the rows as a sorted ``list[tuple]`` of
        ``(id, name, amount)``, independent of how the engine represents results.
    ``insert_case(namespace, table, case)`` / ``select(namespace, table, case)``
        Used by the data-type matrix (``tests/engines/test_types.py``): insert one value of a
        :class:`~iceberg_lab.datatypes.TypeCase` into an existing ``(id, v)`` table, and read
        the raw values back.
    ``fq(namespace, table)`` / ``execute(sql)``
        SQL engines only: fully qualified table name in the engine's dialect, and running a
        statement returning all rows.

Every adapter is bound to ONE warehouse of the catalog under test (``CATALOG``: its ``sts``
warehouse, and on Lakekeeper also ``signing``). The ``credentials`` argument decides how the
engine reaches the data files in S3:

    ``"catalog"``
        Use only what the catalog hands out with ``loadTable``: vended STS credentials
        (``sts``), remote-signing config (Lakekeeper ``signing``), or - on RustFS, which vends
        nothing - the client's own SigV4 keys. This is the mode the access-delegation tests
        examine.
    ``"static"``
        Configure the RustFS root keys directly in the engine. This is the fallback for
        engines that cannot use the warehouse's delegation mode (e.g. DuckDB has no remote
        signing). Convenient, but every engine then has full access to the whole bucket.

The engine adapters get the lab's helps (``config.HELPS``: PyIceberg, Polars, Daft and DuckDB
on RustFS) - except in the run without help
(``config.WITH_HELP`` false, ``LAB_NO_HELP=1``). The lab's own PyIceberg client always has
them (see :func:`pyiceberg_catalog`).
"""

from __future__ import annotations

import pathlib
import re
from typing import Literal

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog

from iceberg_lab.config import (
    PROFILE,
    RUSTFS_AWS_HOST,
    SETTINGS,
    WAREHOUSE_SIGNING,
    WAREHOUSE_STS,
    WAREHOUSES,
    WITH_HELP,
    Settings,
    catalog_auth,
    catalog_auth_props,
    catalog_headers,
)

#: How an engine obtains S3 access - see the module docstring.
Credentials = Literal["catalog", "static"]

#: Schema of the small sample tables used by ``write``/``read`` (interop & delegation tests).
SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("amount", pa.float64()),
    ]
)


def rows(table: pa.Table) -> list[tuple]:
    """Convert a pyarrow table to sorted ``(id, name, amount)`` tuples for comparisons.

    Column order and extra columns of the input do not matter; only the three sample columns
    are selected. Sorting makes results independent of file/partition read order.

    Args:
        table: Arrow table with at least the columns id, name, amount.

    Returns:
        Rows as sorted ``(id, name, amount)`` tuples.
    """
    t = table.select(["id", "name", "amount"])
    return sorted(zip(*(c.to_pylist() for c in t.columns)))


def static_s3_props(s: Settings = SETTINGS) -> dict[str, str]:
    """PyIceberg FileIO properties that point at RustFS with the root keys ("static" mode).

    Args:
        s: Connection settings of the lab.

    Returns:
        FileIO properties (endpoint, root keys, region, path style).
    """
    return {
        "s3.endpoint": s.s3_endpoint,
        "s3.access-key-id": s.s3_access_key,
        "s3.secret-access-key": s.s3_secret_key,
        "s3.region": s.s3_region,
        "s3.path-style-access": "true",
    }


def pyiceberg_catalog(
    warehouse: str,
    credentials: Credentials = "catalog",
    s: Settings = SETTINGS,
    lab_fixes: bool = True,
) -> Catalog:
    """Open a PyIceberg REST catalog on one Lakekeeper warehouse.

    Args:
        warehouse: Lakekeeper warehouse name (``demo-signing`` or ``demo-sts``).
        credentials: ``"catalog"`` relies on what Lakekeeper returns per table;
            ``"static"`` additionally sets the RustFS root keys and sends an empty
            ``X-Iceberg-Access-Delegation`` header (= "I do not need delegated access").
        s: Connection settings.
        lab_fixes: Apply the lab's workarounds (SigV4 content hash on RustFS).

    Note:
        PyIceberg merges the catalog properties with the per-table ``config`` from the
        ``loadTable`` response, and the *server* values win. On ``demo-sts`` the vended
        credentials therefore still override the static keys - which is harmless, both work.

    Returns:
        The PyIceberg REST catalog.
    """
    props = {"type": "rest", "uri": s.catalog_uri, "warehouse": warehouse} | catalog_auth_props(s)
    if credentials == "static":
        props |= static_s3_props(s) | {"header.X-Iceberg-Access-Delegation": ""}
    # The first argument is only a local name for the catalog object.
    cat = load_catalog(warehouse, **props)
    if lab_fixes and PROFILE.auth == "sigv4":
        _add_content_sha256(cat)
    return cat


#: Switch for the lab workaround below; tests/conftest.py turns it off while PyIceberg,
#: Polars or Daft is the engine under test (writer role), so the gap stays visible there.
LAB_FIXES_ENABLED = True


def _add_content_sha256(cat) -> None:
    """Lab workaround: send ``x-amz-content-sha256`` on every SigV4 request (RustFS needs it).

    PyIceberg's SigV4 adapter adds the header only for EMPTY bodies; RustFS rejects a signed
    POST without it ("missing header: x-amz-content-sha256"), AWS does not. The catalog
    PyIceberg uses for test setup and verification gets this fix; the PyIceberg, Polars and
    Daft *engine* adapters do not (``lab_fixes=False``), and while one of them is the engine
    under test the fix is off everywhere (:data:`LAB_FIXES_ENABLED`), so the gap stays visible.

    Args:
        cat: PyIceberg REST catalog with a SigV4 adapter.
    """
    import hashlib

    adapter = cat._session.get_adapter(cat.uri)  # the SigV4Adapter PyIceberg mounted
    sign = adapter.add_headers

    def add_headers(request, **kwargs):
        """Add ``x-amz-content-sha256`` (while the lab fix is on), then sign.

        Args:
            request: The prepared HTTP request.
            kwargs: Passed to PyIceberg's signer.

        Returns:
            Whatever PyIceberg's signer returns.
        """
        if not LAB_FIXES_ENABLED:
            return sign(request, **kwargs)
        body = request.body or b""
        if isinstance(body, str):
            body = body.encode()
        request.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
        return sign(request, **kwargs)

    adapter.add_headers = add_headers


class Engine:
    """Base class of all adapters - defines the shared interface (see module docstring).

    Class attributes:
        name: Short engine name, used in test ids and messages.
        dialect: Key into :attr:`TypeCase.sql <iceberg_lab.datatypes.TypeCase.sql>` for SQL
            engines; ``None`` for DataFrame engines, which write Arrow data instead.

    The default :meth:`insert_case` / :meth:`select` implementations are written for SQL
    engines on top of :meth:`fq` and :meth:`execute`; DataFrame engines override them.
    """

    name: str
    dialect: str | None = None

    def __init__(self, warehouse: str, credentials: Credentials = "catalog"):
        """Remember the warehouse and the access mode.

        Args:
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).
        """
        self.warehouse = warehouse
        self.credentials = credentials

    # -- interop / delegation interface ------------------------------------------------------

    def write(self, namespace: str, table: str, data: pa.Table) -> None:
        """Create ``namespace.table`` with the engine's own DDL/API and insert ``data``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        raise NotImplementedError

    def read(self, namespace: str, table: str) -> list[tuple]:
        """Return all rows as sorted ``(id, name, amount)`` tuples.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
        """
        raise NotImplementedError

    # -- data-type matrix interface ------------------------------------------------------------
    # The table "(id BIGINT, v <type>)" already exists (created by PyIceberg, or by Spark for
    # types PyIceberg cannot model), so the engine's exact *table* type is guaranteed and only
    # the engine's write path for VALUES is tested.

    def insert_case(self, namespace: str, table: str, case) -> None:
        """Insert ``(1, <value>)`` and ``(2, NULL)`` with the engine's own SQL literal.

        Raises:
            NotImplementedError: if the engine has no literal for this type (the test then
                skips: the engine cannot express the type at all).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).
        """
        literal = case.sql.get(self.dialect)
        if literal is None:
            raise NotImplementedError(f"{self.name} has no literal for {case.name}")
        self.execute(f"INSERT INTO {self.fq(namespace, table)} VALUES (1, {literal}), (2, NULL)")

    def select(self, namespace: str, table: str, case=None) -> list[tuple]:
        """Return the raw ``(id, v)`` rows exactly as the engine's client delivers them.

        ``case.read_expr`` may wrap ``v`` (e.g. cast nanosecond timestamps to text, because
        the Python clients would truncate them to microseconds). Normalising the values into
        a comparable form is done by :func:`iceberg_lab.datatypes.normalize`, not here.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case whose ``read_expr`` wraps ``v`` (None: plain ``v``).

        Returns:
            Raw ``(id, v)`` rows, sorted by id.
        """
        expr = case.read_expr(self.dialect) if case else "v"
        return sorted(
            (tuple(r) for r in self.execute(f"SELECT id, {expr} FROM {self.fq(namespace, table)}")),
            key=lambda r: r[0],
        )

    def type_of(self, namespace: str, table: str) -> str:
        """The engine's own type name for column ``v`` (``typeof(v)`` for SQL engines).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The engine's type name of column ``v``.
        """
        return self.execute(f"SELECT typeof(v) FROM {self.fq(namespace, table)} LIMIT 1")[0][0]

    # -- SQL engine helpers --------------------------------------------------------------------

    def fq(self, namespace: str, table: str) -> str:
        """Fully qualified table name in this engine's dialect.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
        """
        raise NotImplementedError

    def execute(self, query: str) -> list:
        """Run one SQL statement and return all result rows (empty list for DDL/DML).

        Args:
            query: SQL statement in the engine's dialect.
        """
        raise NotImplementedError

    def refresh(self, namespace: str, table: str) -> None:
        """Drop cached table state so the next read sees the catalog's current snapshot.

        No-op by default. Spark's Iceberg catalog caches loaded tables (cache-enabled, 30 s):
        the read matrices call this, so a cache can never stand in for lost data there. Where
        the cache behaviour itself is the subject (test_operations ``sees_external_commit``,
        Spark writing with a stale spec), it is not called.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
        """

    def close(self) -> None:
        """Release connections/sessions. Safe to call more than once."""

    def __repr__(self) -> str:
        """Short form for test ids and logs: ``<engine>[<warehouse>,<credentials>]``.

        Returns:
            The short form.
        """
        return f"{self.name}[{self.warehouse},{self.credentials}]"


def _arrow_id_v(t: pa.Table) -> list[tuple]:
    """``(id, v)`` tuples from an Arrow table, sorted by id (type-matrix helper).

    Going through Arrow (instead of e.g. Polars' ``to_list``) keeps nanosecond timestamps as
    ``pandas.Timestamp`` instead of truncating them to ``datetime``.

    Args:
        t: Arrow table with columns id and v.

    Returns:
        Raw ``(id, v)`` rows, sorted by id.
    """
    return sorted(zip(t["id"].to_pylist(), t["v"].to_pylist()), key=lambda r: r[0])


# =========================================================================== PyIceberg-based


class PyIcebergEngine(Engine):
    """PyIceberg: the Python reference implementation of Iceberg.

    Talks to the REST catalog directly and uses its own FileIO for data files: ``PyArrowFileIO``
    for static keys/STS, and ``FsspecFileIO`` (s3fs) when Lakekeeper returns remote-signing
    config - the switch happens automatically via the ``py-io-impl`` property in the
    ``loadTable`` response. PyIceberg is the only Python engine here that can do remote signing.
    """

    name = "pyiceberg"

    def __init__(self, warehouse: str, credentials: Credentials = "catalog"):
        """Open the engine's own PyIceberg catalog (with the lab's help unless LAB_NO_HELP=1).

        Args:
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).
        """
        super().__init__(warehouse, credentials)
        # The engine under test: the lab's help only while config.WITH_HELP (not in the run
        # without help, LAB_NO_HELP=1).
        self.catalog = pyiceberg_catalog(warehouse, credentials, lab_fixes=WITH_HELP)

    def write(self, namespace, table, data):
        # create_table takes the Arrow schema and assigns Iceberg field ids itself.
        """Create the table from the Arrow schema and append the rows.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        t = self.catalog.create_table(f"{namespace}.{table}", schema=data.schema)
        t.append(data)

    def read(self, namespace, table):
        """Scan the whole table.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        return rows(self.catalog.load_table(f"{namespace}.{table}").scan().to_arrow())

    def insert_case(self, namespace, table, case):
        """Append the case's value and NULL row as Arrow data.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).
        """
        self.catalog.load_table(f"{namespace}.{table}").append(case.arrow())

    def select(self, namespace, table, case=None):
        """Scan the table to Arrow.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Unused: PyIceberg reads the column as is.

        Returns:
            Raw ``(id, v)`` rows, sorted by id.
        """
        return _arrow_id_v(self.catalog.load_table(f"{namespace}.{table}").scan().to_arrow())

    def type_of(self, namespace, table):
        """Arrow type of column ``v`` as PyIceberg reads it.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The engine's type name of column ``v``.
        """
        t = self.catalog.load_table(f"{namespace}.{table}")
        return str(t.scan().to_arrow().schema.field("v").type)


def polars_storage_options(io_props: dict[str, str]) -> dict[str, str]:
    """Translate a table's PyIceberg S3 properties into Polars/object_store options.

    Polars uses PyIceberg only to plan the scan; the Parquet files are read by its own Rust
    reader (the ``object_store`` crate), which does **not** look at the table's FileIO
    properties. Without explicit options it falls back to the AWS default chain and hangs on
    the EC2 metadata endpoint (169.254.169.254).

    Works for vended STS credentials and static keys. Remote signing has no equivalent in
    object_store, so on ``demo-signing`` the returned options contain no credentials and the
    read fails - which is exactly what the delegation tests document.

    Args:
        io_props: ``table.io.properties`` of a loaded PyIceberg table.

    Returns:
        Options for ``polars.scan_iceberg(..., storage_options=...)``.
    """
    mapping = {
        "s3.access-key-id": "aws_access_key_id",
        "s3.secret-access-key": "aws_secret_access_key",
        "s3.session-token": "aws_session_token",
        "s3.endpoint": "aws_endpoint_url",
        "s3.region": "aws_region",
    }
    opts = {dst: io_props[src] for src, dst in mapping.items() if src in io_props}
    # object_store refuses plain HTTP unless explicitly allowed.
    if opts.get("aws_endpoint_url", "").startswith("http://"):
        opts["aws_allow_http"] = "true"
    # RustFS needs path-style addressing (http://host/bucket/key).
    if io_props.get("s3.path-style-access") == "true":
        opts["aws_virtual_hosted_style_request"] = "false"
    return opts


class PolarsEngine(PyIcebergEngine):
    """Polars: catalog access and *writes* go through PyIceberg, *reads* through Polars' reader.

    Consequences, all visible in the tests:
      * writing inherits every PyIceberg limitation (e.g. no v3 tables);
      * reading needs :func:`polars_storage_options` and cannot use remote signing;
      * Polars has no ``uuid``/``fixed`` types and a nanosecond ``time``.
    """

    name = "polars"

    def write(self, namespace, table, data):
        """Create the table with PyIceberg, then append with ``write_iceberg``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        import polars as pl

        t = self.catalog.create_table(f"{namespace}.{table}", schema=data.schema)
        pl.from_arrow(data).write_iceberg(t, mode="append")

    def read(self, namespace, table):
        """Read with ``pl.scan_iceberg``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        import polars as pl

        t = self.catalog.load_table(f"{namespace}.{table}")
        df = pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties)).collect()
        return rows(df.to_arrow())

    def insert_case(self, namespace, table, case):
        """Append the case's rows with ``write_iceberg``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).
        """
        import polars as pl

        t = self.catalog.load_table(f"{namespace}.{table}")
        pl.from_arrow(case.arrow()).write_iceberg(t, mode="append")

    def select(self, namespace, table, case=None):
        """Read with ``pl.scan_iceberg`` (via Arrow, keeps nanoseconds).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Unused: Polars reads the column as is.

        Returns:
            Raw ``(id, v)`` rows, sorted by id.
        """
        import polars as pl

        t = self.catalog.load_table(f"{namespace}.{table}")
        df = pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties)).collect()
        return _arrow_id_v(df.to_arrow())  # via Arrow: keeps nanoseconds (pd.Timestamp)

    def type_of(self, namespace, table):
        """Polars dtype of column ``v`` in the lazy scan's schema.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The engine's type name of column ``v``.
        """
        import polars as pl

        t = self.catalog.load_table(f"{namespace}.{table}")
        lf = pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties))
        return str(lf.collect_schema()["v"])


class DaftEngine(PyIcebergEngine):
    """Daft: PyIceberg for the catalog, Daft's own Rust IO layer for reading and writing files.

    Daft converts the table's FileIO properties itself, so vended STS credentials and static
    keys work without extra options; remote signing does not. Writes still commit through
    PyIceberg (no v3 tables).
    """

    name = "daft"

    def write(self, namespace, table, data):
        """Create the table with PyIceberg, then append with Daft's ``write_iceberg``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        import daft

        t = self.catalog.create_table(f"{namespace}.{table}", schema=data.schema)
        daft.from_arrow(data).write_iceberg(t, mode="append")

    def read(self, namespace, table):
        """Read with ``daft.read_iceberg``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        import daft

        t = self.catalog.load_table(f"{namespace}.{table}")
        return rows(daft.read_iceberg(t).to_arrow())

    def insert_case(self, namespace, table, case):
        """Append the case's rows with Daft's ``write_iceberg``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).
        """
        import daft

        t = self.catalog.load_table(f"{namespace}.{table}")
        daft.from_arrow(case.arrow()).write_iceberg(t, mode="append")

    def select(self, namespace, table, case=None):
        """Read with ``daft.read_iceberg``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Unused: Daft reads the column as is.

        Returns:
            Raw ``(id, v)`` rows, sorted by id.
        """
        import daft

        t = self.catalog.load_table(f"{namespace}.{table}")
        return _arrow_id_v(daft.read_iceberg(t).to_arrow())

    def type_of(self, namespace, table):
        """Daft dtype of column ``v``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The engine's type name of column ``v``.
        """
        import daft

        t = self.catalog.load_table(f"{namespace}.{table}")
        return str(daft.read_iceberg(t).schema()["v"].dtype)


# =========================================================================== DuckDB


class DuckDBEngine(Engine):
    """DuckDB with the ``iceberg`` extension, attached to Lakekeeper as database ``lk``.

    ``credentials="catalog"`` uses ``ACCESS_DELEGATION_MODE 'vended_credentials'``: DuckDB asks
    Lakekeeper for STS credentials and creates an S3 secret from them per table. DuckDB has
    no remote-signing support, so on ``demo-signing`` you need ``credentials="static"``, which
    creates a global S3 secret with the RustFS root keys and disables delegation.
    """

    name = "duckdb"
    dialect = "duckdb"
    #: Name under which the warehouse is attached; tables are ``lk.<namespace>.<table>``.
    alias = "lk"

    def __init__(
        self, warehouse: str, credentials: Credentials = "catalog", s: Settings = SETTINGS
    ):
        """Open an in-memory DuckDB, load the iceberg extension and attach the warehouse.

        Args:
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).
            s: Connection settings of the lab.
        """
        import duckdb

        super().__init__(warehouse, credentials)
        self.con = duckdb.connect()  # in-memory database, one per adapter
        # Extensions are downloaded once and cached in ~/.duckdb.
        self.con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
        if credentials == "static":
            self.con.execute(f"""
                CREATE SECRET rustfs (
                    TYPE s3, KEY_ID '{s.s3_access_key}', SECRET '{s.s3_secret_key}',
                    ENDPOINT '{s.s3_host_port}', URL_STYLE 'path', USE_SSL false,
                    REGION '{s.s3_region}'
                )""")
        mode = "vended_credentials" if credentials == "catalog" else "none"
        if PROFILE.auth == "sigv4":
            # SigV4 (RustFS): DuckDB signs with the credentials of an S3 secret; nothing is
            # vended, so the same secret also serves the data files.
            self.con.execute(f"""
                CREATE SECRET IF NOT EXISTS {self.alias}_sigv4 (
                    TYPE s3, KEY_ID '{s.s3_access_key}', SECRET '{s.s3_secret_key}',
                    ENDPOINT '{s.s3_host_port}', URL_STYLE 'path', USE_SSL false,
                    REGION '{s.s3_region}'
                )""")
            auth = f"AUTHORIZATION_TYPE 'sigv4', SECRET {self.alias}_sigv4"
            mode = "none"
        elif PROFILE.auth == "token":
            # Static bearer token (Unity: authorization disabled, any token accepted).
            self.con.execute(f"""
                CREATE SECRET {self.alias}_auth (TYPE iceberg, TOKEN '{PROFILE.token}')""")
            auth = f"SECRET {self.alias}_auth"
        elif PROFILE.oauth_credential:
            # OAuth2 client credentials (e.g. Polaris) through an iceberg secret.
            client_id, client_secret = PROFILE.oauth_credential.split(":", 1)
            self.con.execute(f"""
                CREATE SECRET {self.alias}_auth (
                    TYPE iceberg, CLIENT_ID '{client_id}', CLIENT_SECRET '{client_secret}',
                    OAUTH2_SERVER_URI '{s.catalog_base.rstrip("/")}{PROFILE.token_path}',
                    OAUTH2_SCOPE '{PROFILE.oauth_scope}'
                )""")
            auth = f"SECRET {self.alias}_auth"
        else:
            # AUTHORIZATION_TYPE 'none': Lakekeeper runs without authentication in this lab.
            auth = "AUTHORIZATION_TYPE 'none'"
        if PROFILE.client_s3_endpoint:
            # The catalog vends keys without an endpoint (Unity assumes AWS): point DuckDB's
            # S3 client at RustFS, the vended per-table secrets bring only the keys.
            self.con.execute(f"""
                SET s3_endpoint = '{s.s3_host_port}'; SET s3_url_style = 'path';
                SET s3_use_ssl = false; SET s3_region = '{s.s3_region}'""")
        endpoint = s.catalog_uri
        if WITH_HELP and PROFILE.auth == "sigv4":
            # Lab help "duckdb-aws-host" for "Could not parse AWS service from host".
            endpoint = endpoint.replace(s.s3_host_port.split(":")[0], RUSTFS_AWS_HOST, 1)
        self.con.execute(f"""
            ATTACH '{warehouse}' AS {self.alias} (
                TYPE iceberg,
                ENDPOINT '{endpoint}',
                {auth},
                ACCESS_DELEGATION_MODE '{mode}'
            )""")

    def sql(self, query: str, *params):
        """Run a statement and return the DuckDB result object (for ``fetchone``/``df`` etc.).

        Args:
            query: SQL statement in the engine's dialect.
            params: Bind parameters.

        Returns:
            The DuckDB result object.
        """
        return self.con.execute(query, params or None)

    def fq(self, namespace, table):
        """``<alias>.<namespace>.<table>`` (alias of the attached catalog).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The table name as the engine's SQL addresses it.
        """
        return f"{self.alias}.{namespace}.{table}"

    def execute(self, query):
        """Run one statement and fetch all rows.

        Args:
            query: SQL statement in the engine's dialect.

        Returns:
            All result rows.
        """
        return self.con.execute(query).fetchall()

    def write(self, namespace, table, data):
        """Create the table with DuckDB DDL and insert the Arrow rows (zero-copy view).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        fq = f"{self.alias}.{namespace}.{table}"
        self.con.execute(f"CREATE TABLE {fq} (id BIGINT, name VARCHAR, amount DOUBLE)")
        # Expose the Arrow table as a DuckDB view for INSERT ... SELECT (zero-copy).
        self.con.register("_src", data)
        self.con.execute(f"INSERT INTO {fq} SELECT id, name, amount FROM _src")
        self.con.unregister("_src")

    def read(self, namespace, table):
        """Select all rows.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        return sorted(
            self.con.execute(
                f"SELECT id, name, amount FROM {self.alias}.{namespace}.{table}"
            ).fetchall()
        )

    def close(self):
        """Close the DuckDB connection."""
        self.con.close()


# =========================================================================== Spark

#: Spark catalog name per warehouse (Spark identifiers cannot contain "-").
SPARK_CATALOGS = {wh: ("sts" if wh == WAREHOUSE_STS else "signing") for wh in WAREHOUSES}
#: Iceberg version of the Spark runtime and AWS bundle JARs fetched from Maven Central.
ICEBERG_VERSION = "1.12.0"


def spark_session(s: Settings = SETTINGS):
    """Create (or return the existing) local SparkSession with both warehouses as catalogs.

    * ``iceberg-spark-runtime-<spark minor>_2.13`` provides the Iceberg catalog and SQL
      extensions (MERGE, CALL procedures, branches, ...).
    * ``iceberg-aws-bundle`` provides ``S3FileIO`` including the remote-signing client, so
      Spark works on both warehouses with catalog-provided access only.
    * JARs are downloaded on first use into ``~/.ivy2`` (a named volume in the devcontainer).

    Spark's session time zone is pinned to UTC so that ``TIMESTAMP`` values compare equal
    across engines in the type tests.

    Args:
        s: Connection settings of the lab.

    Returns:
        The SparkSession.
    """
    import pyspark
    from pyspark.sql import SparkSession

    spark_minor = ".".join(pyspark.__version__.split(".")[:2])
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("iceberg-lab-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")  # keep the JVM small; the machine runs a lot more
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.ivy", str(pathlib.Path.home() / ".ivy2"))
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor}_2.13:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )
    for warehouse, cat in SPARK_CATALOGS.items():
        prefix = f"spark.sql.catalog.{cat}"
        builder = (
            builder.config(prefix, "org.apache.iceberg.spark.SparkCatalog")
            .config(f"{prefix}.type", "rest")
            .config(f"{prefix}.uri", s.catalog_uri)
            .config(f"{prefix}.warehouse", warehouse)
            # No io-impl / credentials on purpose: the Iceberg REST client uses whatever S3
            # config the catalog returns per table (STS credentials or remote-signer settings).
        )
        for k, v in catalog_auth_props(s).items():  # OAuth2 for catalogs that need it
            builder = builder.config(f"{prefix}.{k}", v)
    return builder.getOrCreate()


class SparkEngine(Engine):
    """Apache Spark via PySpark - the most complete Iceberg engine of the lab.

    The SparkSession is expensive (JVM start, JAR resolution), so it is created once by the
    ``spark`` test fixture and passed in; the adapter only selects the warehouse's catalog.
    """

    name = "spark"
    dialect = "spark"

    def __init__(self, spark, warehouse: str, credentials: Credentials = "catalog"):
        """Select the warehouse's Spark catalog; only catalog-provided access is supported.

        Raises:
            ValueError: static credentials were asked for.

        Args:
            spark: The session's SparkSession.
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).
        """
        if credentials != "catalog":
            # Spark handles both delegation modes; static keys would need a separate catalog.
            raise ValueError("Spark adapter only uses catalog-provided credentials")
        super().__init__(warehouse, credentials)
        self.spark = spark
        self.catalog = SPARK_CATALOGS[warehouse]

    def write(self, namespace, table, data):
        """Create and fill the table in one commit (DataFrameWriterV2 ``create``).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        df = self.spark.createDataFrame(data.to_pandas())
        # DataFrameWriterV2: CREATE TABLE ... AS from the DataFrame, in one commit.
        df.writeTo(f"{self.catalog}.{namespace}.{table}").using("iceberg").create()

    def read(self, namespace, table):
        """Read the table through Spark.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        pdf = self.spark.table(f"{self.catalog}.{namespace}.{table}").toPandas()
        return rows(pa.Table.from_pandas(pdf, preserve_index=False))

    def refresh(self, namespace, table):
        """``REFRESH TABLE``: drop Spark's cached table state.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
        """
        self.spark.sql(f"REFRESH TABLE {self.fq(namespace, table)}")

    def fq(self, namespace, table):
        """``<catalog>.<namespace>.<table>``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The table name as the engine's SQL addresses it.
        """
        return f"{self.catalog}.{namespace}.{table}"

    def execute(self, query):
        """Run one Spark SQL statement and collect the rows.

        Args:
            query: SQL statement in the engine's dialect.

        Returns:
            All result rows (Spark ``Row`` objects).
        """
        return self.spark.sql(query).collect()


# =========================================================================== Trino

#: Trino catalog per warehouse, defined in trino/catalog/<name>.properties.
#: Static catalog files in trino/catalog/, per catalog profile (``<prefix>_sts`` ...).
TRINO_CATALOGS = {
    wh: f"{PROFILE.trino_prefix}_{'sts' if wh == WAREHOUSE_STS else 'signing'}" for wh in WAREHOUSES
}


class TrinoEngine(Engine):
    """Trino with the Iceberg connector (compose profile ``trino``).

    Trino catalogs are static configuration files, so the access mode is fixed per warehouse:

    * ``lk_sts`` (``demo-sts``): ``iceberg.rest-catalog.vended-credentials-enabled=true``
    * ``lk_signing`` (``demo-signing``): static RustFS keys - Trino cannot do remote signing.

    The constructor rejects a ``credentials`` value that does not match this fixed setup, so a
    test can never silently test something else than it claims.
    """

    name = "trino"
    dialect = "trino"

    def __init__(
        self, warehouse: str, credentials: Credentials = "catalog", s: Settings = SETTINGS
    ):
        """Connect to Trino with the warehouse's catalog as default (static keys on signing).

        Raises:
            ValueError: the access mode does not match the Trino catalog's configuration.

        Args:
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).
            s: Connection settings of the lab.
        """
        import trino

        expected = "static" if warehouse == WAREHOUSE_SIGNING else "catalog"
        if credentials != expected:
            raise ValueError(f"Trino catalog for {warehouse} is configured with {expected!r}")
        super().__init__(warehouse, credentials)
        self.catalog = TRINO_CATALOGS[warehouse]
        # Default catalog = the warehouse, so tables are addressed as "<namespace>.<table>".
        self.conn = trino.dbapi.connect(
            host=s.trino_host, port=s.trino_port, user="tests", catalog=self.catalog
        )

    def sql(self, query: str):
        """Run one statement and return all rows (Trino executes lazily until fetched).

        Args:
            query: SQL statement in the engine's dialect.

        Returns:
            All result rows.
        """
        cur = self.conn.cursor()
        cur.execute(query)
        return cur.fetchall()

    def fq(self, namespace, table):
        """``<namespace>.<table>`` in the connection's default catalog.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The table name as the engine's SQL addresses it.
        """
        return f"{namespace}.{table}"

    execute = sql

    def write(self, namespace, table, data):
        """Create the table with Trino DDL and insert the rows as literals.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        fq = f"{namespace}.{table}"
        self.sql(f"CREATE TABLE {fq} (id BIGINT, name VARCHAR, amount DOUBLE)")
        # Sample data is tiny and trusted, so plain literals are fine here.
        values = ", ".join(f"({i}, '{n}', {a})" for i, n, a in rows(data))
        self.sql(f"INSERT INTO {fq} VALUES {values}")

    def read(self, namespace, table):
        """Select all rows.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        return sorted(
            tuple(r) for r in self.sql(f"SELECT id, name, amount FROM {namespace}.{table}")
        )

    def close(self):
        """Close the Trino connection."""
        self.conn.close()


# =========================================================================== pg_lake (Postgres)

#: Name of the pg_lake catalog server (``CREATE SERVER``) per warehouse.
PGLAKE_SERVERS = {
    wh: f"{PROFILE.trino_prefix}_{'sts' if wh == WAREHOUSE_STS else 'signing'}" for wh in WAREHOUSES
}


class PgLakeEngine(Engine):
    """Postgres with the pg_lake extension (compose profile ``pglake``).

    Architecture: Postgres plans and coordinates; ``pgduck_server`` (an embedded DuckDB in a
    sidecar container, connected via a Unix socket) reads and writes the Parquet files. The
    S3 credentials live in pgduck_server's own DuckDB secret (``pg_lake/init-pgduck-server.sql``)
    - pg_lake never uses catalog-vended credentials for tables it writes, so for this adapter
    ``credentials`` is informational only and both warehouses work the same way.

    Lakekeeper integration:

    * One catalog server per warehouse (``CREATE SERVER <name> TYPE 'rest' FOREIGN DATA
      WRAPPER iceberg_catalog``), created on first use. Catalogs with OAuth2 (Polaris) get
      their real token endpoint and client; open catalogs (Lakekeeper, Unity here) get
      pg_lake's own ``rest_auth_type 'none'`` - no token at all. Those servers carry the
      suffix ``_noauth``: servers persist in Postgres, and earlier lab versions created the
      plain names with a token stub.
    * Tables pg_lake creates itself (``USING iceberg WITH (catalog = '<server>')``) are
      writable; the Postgres schema becomes the Iceberg namespace.
    * Tables created by other engines are attached **read-only**
      (``CREATE TABLE ... () USING iceberg WITH (read_only = true, ...)``) and always show the
      catalog's latest commit. pg_lake cannot write to them.
    """

    name = "pg_lake"
    dialect = "pg_lake"

    def __init__(
        self,
        warehouse: str,
        credentials: Credentials = "static",
        s: Settings = SETTINGS,
    ):
        """Connect to Postgres and prepare the pg_lake catalog server for the warehouse.

        Args:
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).
            s: Connection settings of the lab.
        """
        import psycopg

        super().__init__(warehouse, credentials)
        self.server = PGLAKE_SERVERS[warehouse]
        # autocommit: every statement commits on its own, like the other engines' adapters.
        # Tests that need a transaction use `with conn.transaction(): ...` explicitly.
        self.conn = psycopg.connect(
            host=s.pglake_host,
            port=s.pglake_port,
            user="postgres",
            dbname="postgres",
            autocommit=True,
        )
        # Idempotent: CREATE SERVER has no IF NOT EXISTS together with a user mapping, so use
        # a DO block. Options:
        #   rest_endpoint   - Lakekeeper's REST base URI
        #   warehouse       - sent as ?warehouse= to /v1/config so Lakekeeper returns the prefix
        #   auth            - OAuth2 token endpoint (Polaris), or rest_auth_type 'none' for
        #                     a catalog without authentication (Lakekeeper, Unity here)
        #   location_prefix - where pg_lake asks to place new tables; must be inside the
        #                     warehouse's storage prefix or Lakekeeper rejects the location
        if PROFILE.oauth_credential:
            auth = f"oauth_endpoint '{s.catalog_base.rstrip('/')}{PROFILE.token_path}',"
            client_id, client_secret = PROFILE.oauth_credential.split(":", 1)
            mapping = f"""CREATE USER MAPPING FOR PUBLIC SERVER {self.server}
                  OPTIONS (client_id '{client_id}', client_secret '{client_secret}');"""
        else:
            self.server += "_noauth"
            auth, mapping = "rest_auth_type 'none',", ""
        # New tables must lie inside the warehouse's storage (Unity: its managed bucket).
        location = (
            PROFILE.location
            if warehouse == WAREHOUSE_STS and PROFILE.location
            else f"s3://{s.s3_bucket}/{warehouse}"
        )
        self.execute(f"""
            DO $$ BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = '{self.server}') THEN
                CREATE SERVER {self.server} TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
                  OPTIONS (rest_endpoint '{s.catalog_uri}',
                           warehouse '{warehouse}',
                           {auth}
                           location_prefix '{location}/pg_lake');
                {mapping}
              END IF;
            END $$""")
        # Tables this adapter created (writable); everything else is attached read-only.
        self.owned: set[tuple[str, str]] = set()
        self.tx_commit = self._catalog_has_tx_commit(s)

    #: Set in __init__ from the catalog's /v1/config.
    tx_commit = True
    #: Statements that make pg_lake commit to the catalog.
    _WRITES = re.compile(
        r"^\s*(INSERT|UPDATE|DELETE|MERGE|COPY|TRUNCATE|ALTER\s+TABLE)\b", re.IGNORECASE
    )
    NO_TX_COMMIT = (
        "pg_lake commits writes through POST /v1/{{prefix}}/transactions/commit, which catalog "
        "{catalog} does not offer - pg_lake ignores the 404 and reports success, nothing is "
        "committed (tests/specific/test_pglake.py::test_insert_commits_to_catalog)"
    )

    def _catalog_has_tx_commit(self, s: Settings) -> bool:
        """Does the catalog advertise multi-table commits (``/transactions/commit``)?

        Args:
            s: Connection settings of the lab.

        Returns:
            True if ``/v1/config`` lists the transactions/commit endpoint.
        """
        import requests

        r = requests.get(
            f"{s.catalog_uri}/v1/config",
            params={"warehouse": self.warehouse},
            headers=catalog_headers(s),
            auth=catalog_auth(s),
            timeout=10,
        )
        endpoints = r.json().get("endpoints") if r.ok else None
        return endpoints is None or "POST /v1/{prefix}/transactions/commit" in endpoints

    def execute(self, query: str) -> list:
        """Run a statement; refuse writes where pg_lake would lose them silently.

        Raises:
            RuntimeError: a write on a catalog without ``/transactions/commit`` (see NO_TX_COMMIT).

        Args:
            query: SQL statement in the engine's dialect.

        Returns:
            All result rows (empty list for DDL/DML).
        """
        if self._WRITES.match(query) and not self.tx_commit:
            # Verified behaviour (see NO_TX_COMMIT): refuse instead of losing the write
            # silently, so the matrices show the cause rather than "empty result".
            raise RuntimeError(self.NO_TX_COMMIT.format(catalog=PROFILE.name))
        return self.execute_raw(query)

    def execute_raw(self, query: str) -> list:
        """Run a statement without the guard above (for the test that shows the behaviour).

        Args:
            query: SQL statement in the engine's dialect.

        Returns:
            All result rows (empty list for DDL/DML).
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall() if cur.description else []

    def fq(self, namespace, table):
        # Quote identifiers: Postgres would otherwise fold them to lower case.
        """``"<namespace>"."<table>"`` (quoted: Postgres would fold to lower case).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The table name as the engine's SQL addresses it.
        """
        return f'"{namespace}"."{table}"'

    def _attach(self, namespace, table):
        """Make a table that another engine created visible in Postgres (read-only).

        The column list ``()`` is left empty: pg_lake takes the columns from the catalog.
        ``IF NOT EXISTS`` makes repeated reads cheap; the attached table always follows the
        catalog's current snapshot, so it never needs to be re-created.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
        """
        if (namespace, table) in self.owned:
            return
        self.execute(f'CREATE SCHEMA IF NOT EXISTS "{namespace}"')
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.fq(namespace, table)} () USING iceberg
            WITH (catalog = '{self.server}', read_only = true,
                  catalog_namespace = '{namespace}', catalog_table_name = '{table}')""")

    def write(self, namespace, table, data):
        # Writable REST tables take namespace and name from the Postgres schema and table.
        """Create a pg_lake-owned Iceberg table and insert the rows.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        self.execute(f'CREATE SCHEMA IF NOT EXISTS "{namespace}"')
        self.execute(f"""CREATE TABLE {self.fq(namespace, table)}
                         (id bigint, name text, amount double precision)
                         USING iceberg WITH (catalog = '{self.server}')""")
        self.owned.add((namespace, table))
        values = ", ".join(f"({i}, '{n}', {a})" for i, n, a in rows(data))
        self.execute(f"INSERT INTO {self.fq(namespace, table)} VALUES {values}")

    def read(self, namespace, table):
        """Attach the table (if not owned) and select all rows.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        self._attach(namespace, table)
        return sorted(
            tuple(r)
            for r in self.execute(f"SELECT id, name, amount FROM {self.fq(namespace, table)}")
        )

    def insert_case(self, namespace, table, case):
        # Type-matrix tables are created by PyIceberg, so pg_lake could only attach them
        # read-only - it can never be a *writer* in that matrix.
        """Not possible: pg_lake writes only tables it created.

        Raises:
            NotImplementedError: always.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).
        """
        raise NotImplementedError(
            "pg_lake only writes tables it created itself (not PyIceberg-created)"
        )

    def select(self, namespace, table, case=None):
        """Attach the table and select ``(id, v)``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case whose ``read_expr`` wraps ``v`` (None: plain ``v``).

        Returns:
            Raw ``(id, v)`` rows, sorted by id.
        """
        self._attach(namespace, table)
        # Postgres spells the text type "text", not "VARCHAR".
        expr = case.read_expr("pg_lake").replace("VARCHAR", "text") if case else "v"
        return sorted(
            (tuple(r) for r in self.execute(f"SELECT id, {expr} FROM {self.fq(namespace, table)}")),
            key=lambda r: r[0],
        )

    def type_of(self, namespace, table):
        """``pg_typeof(v)`` of the attached table.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The engine's type name of column ``v``.
        """
        self._attach(namespace, table)
        q = f"SELECT pg_typeof(v)::text FROM {self.fq(namespace, table)} LIMIT 1"
        return self.execute(q)[0][0]

    def close(self):
        """Close the Postgres connection."""
        self.conn.close()


# =========================================================================== Flink

#: Flink catalog name per warehouse (created per SQL Gateway session).
FLINK_CATALOGS = {wh: ("sts" if wh == WAREHOUSE_STS else "signing") for wh in WAREHOUSES}


class FlinkEngine(Engine):
    """Apache Flink 2.3 via its SQL Gateway REST API (compose profile ``flink``).

    PyFlink cannot share the lab's Python environment (it pins pyarrow<21, pandas<2.3 and
    py4j), so this adapter speaks Flink SQL over HTTP: one gateway session per adapter, with
    both warehouses registered as Iceberg REST catalogs ``sts`` and ``signing``. Like Spark,
    Flink's Iceberg S3FileIO uses whatever Lakekeeper hands out (STS or remote signing).

    The session runs in batch mode with ``table.dml-sync`` so an INSERT returns only after
    its job has committed. Results come back as JSON rows; :meth:`select` turns Flink's JSON
    representations (base64 binary, ISO date/time text) back into Python values.
    """

    name = "flink"
    dialect = "flink"

    def __init__(
        self, warehouse: str, credentials: Credentials = "catalog", s: Settings = SETTINGS
    ):
        """Open a SQL Gateway session and create the warehouse's Iceberg catalog in it.

        Raises:
            ValueError: static credentials were asked for.

        Args:
            warehouse: Warehouse (catalog) name.
            credentials: ``"catalog"`` (access the catalog provides) or ``"static"`` (lab root keys).
            s: Connection settings of the lab.
        """
        import requests

        if credentials != "catalog":
            raise ValueError("Flink adapter only uses catalog-provided credentials")
        super().__init__(warehouse, credentials)
        self._http = requests.Session()
        self._base = s.flink_url.rstrip("/")
        self._settings = s
        self.catalog = FLINK_CATALOGS[warehouse]
        self._open_session()

    def _open_session(self):
        """Open a gateway session (batch mode) and register both warehouses as catalogs."""
        s = self._settings
        r = self._http.post(
            f"{self._base}/v1/sessions",
            json={
                "properties": {
                    "execution.runtime-mode": "batch",
                    "table.dml-sync": "true",  # INSERT waits until the job has committed
                    "table.local-time-zone": "UTC",  # TIMESTAMP_LTZ literals/results in UTC
                }
            },
            timeout=30,
        )
        r.raise_for_status()
        self._session = r.json()["sessionHandle"]
        self._check_workers()
        for wh, cat in FLINK_CATALOGS.items():
            auth = "".join(f", '{k}'='{v}'" for k, v in catalog_auth_props(s).items())
            self.execute(
                f"CREATE CATALOG {cat} WITH ('type'='iceberg', 'catalog-type'='rest', "
                f"'uri'='{s.catalog_uri}', 'warehouse'='{wh}'{auth})"
            )

    def _check_workers(self) -> None:
        """Fail fast if the cluster has no TaskManager left.

        A TaskManager that lost its heartbeat (overloaded or suspended host) shuts itself down
        and nothing restarts it; every job then waits for a slot forever and the test hangs
        until the global timeout. The JobManager REST API runs on the gateway host, port 8081.
        """
        jm = re.sub(r":\d+$", ":8081", self._base)
        if self._http.get(f"{jm}/overview", timeout=10).json().get("taskmanagers", 1) == 0:
            raise RuntimeError(
                "Flink cluster has no TaskManager (lost after a heartbeat timeout) - "
                "restart the flink container"
            )

    def execute(self, query: str) -> list:
        """Run one statement through the gateway and return all result rows (lists).

        The gateway's JSON row format cannot encode VARIANT values and maps with non-string
        keys (e.g. ``MAP<INT, BYTES>`` in the ``$files`` metadata table): such queries fail
        with "Could not map response to JSON" (gateway log: "JSON format doesn't support
        non-string as key type of map").

        Args:
            query: SQL statement in the engine's dialect.

        Returns:
            All result rows (lists).
        """
        try:
            return self._execute(query)
        except RuntimeError as exc:
            # The gateway drops idle sessions (idle-timeout); reopen once and retry. Session
            # state (SET, temporary tables) is lost then - statements relying on it fail.
            if "does not exist" not in str(exc) or "Session '" not in str(exc):
                raise
            self._open_session()
            return self._execute(query)

    def _execute(self, query: str) -> list:
        """Submit one statement and page through its results until the end.

        Raises:
            RuntimeError: the gateway reports an error (most specific cause, see ``_flink_error``).

        Args:
            query: SQL statement in the engine's dialect.

        Returns:
            All result rows (lists).
        """
        import time

        r = self._http.post(
            f"{self._base}/v1/sessions/{self._session}/statements",
            json={"statement": query},
            timeout=60,
        )
        if not r.ok:
            raise RuntimeError(_flink_error(r))
        op = r.json()["operationHandle"]
        url = f"{self._base}/v1/sessions/{self._session}/operations/{op}/result/0?rowFormat=JSON"
        rows: list = []
        waited = time.monotonic()
        while url:
            res = self._http.get(url, timeout=300)
            if not res.ok:
                raise RuntimeError(_flink_error(res))
            d = res.json()
            if d["resultType"] == "NOT_READY":
                if time.monotonic() - waited > 30:  # long wait: is there still a worker?
                    self._check_workers()
                    waited = time.monotonic()
                time.sleep(0.2)
                continue
            rows += [x["fields"] for x in d["results"]["data"]]
            nxt = d.get("nextResultUri")
            url = f"{self._base}{nxt}" if nxt and d["resultType"] != "EOS" else None
        return rows

    def fq(self, namespace, table):
        """``<catalog>.<namespace>.<table>``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The table name as the engine's SQL addresses it.
        """
        return f"{self.catalog}.{namespace}.{table}"

    def insert_case(self, namespace, table, case):
        """Like the base version, but Flink rejects an untyped ``NULL`` in VALUES.

        The NULL row is therefore cast to the column type that Flink itself reports.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case of the type matrix (``iceberg_lab.datatypes.TypeCase``).
        """
        literal = case.sql.get(self.dialect)
        if literal is None:
            raise NotImplementedError(f"{self.name} has no literal for {case.name}")
        typ = self.type_of(namespace, table).removesuffix(" NOT NULL")
        null = "NULL" if typ.upper() == "NULL" else f"CAST(NULL AS {typ})"
        self.execute(f"INSERT INTO {self.fq(namespace, table)} VALUES (1, {literal}), (2, {null})")

    def write(self, namespace, table, data):
        """Create the table with Flink DDL and insert the rows (amount cast to DOUBLE).

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            data: Arrow table with the rows to write.
        """
        fq = self.fq(namespace, table)
        self.execute(f"CREATE TABLE {fq} (id BIGINT, name STRING, amount DOUBLE)")
        values = ", ".join(f"({i}, '{n}', CAST({a!r} AS DOUBLE))" for i, n, a in rows(data))
        self.execute(f"INSERT INTO {fq} VALUES {values}")

    def read(self, namespace, table):
        """Select all rows.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            Rows as sorted ``(id, name, amount)`` tuples.
        """
        return sorted(
            tuple(r)
            for r in self.execute(f"SELECT id, name, amount FROM {self.fq(namespace, table)}")
        )

    def type_of(self, namespace, table):
        # Flink SQL has no typeof(); DESCRIBE lists (name, type, null, key, extras, watermark).
        """Type of column ``v`` from ``DESCRIBE`` (Flink SQL has no typeof()).

        Raises:
            LookupError: the table has no column ``v``.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).

        Returns:
            The engine's type name of column ``v``.
        """
        for row in self.execute(f"DESCRIBE {self.fq(namespace, table)}"):
            if row[0] == "v":
                return row[1]
        raise LookupError("column v not found")

    def select(self, namespace, table, case=None):
        """Like the base version, with Flink's JSON values converted back to Python objects.

        Args:
            namespace: Namespace of the table.
            table: Table name (without namespace).
            case: Type case (its Iceberg type guides the conversion).

        Returns:
            Raw ``(id, v)`` rows, sorted by id.
        """
        raw = super().select(namespace, table, case)
        if case is None or case.iceberg is None:
            return raw
        return [(i, _from_flink_json(case.iceberg, v)) for i, v in raw]

    def close(self):
        """Close the gateway session and the HTTP session."""
        try:
            self._http.delete(f"{self._base}/v1/sessions/{self._session}", timeout=10)
        finally:
            self._http.close()


def _flink_error(response) -> str:
    """The most specific error message from a gateway error response (last 'Caused by').

    Args:
        response: Error response of the SQL Gateway.

    Returns:
        The last 'Caused by' line, else the error text.
    """
    try:
        errors = response.json().get("errors", [])
    except ValueError:
        return response.text[:800]
    text = "\n".join(errors)
    causes = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("Caused by:")]
    head = text.strip().splitlines()[0] if text.strip() else ""
    return (causes[-1] if causes else head)[:800] + ("\n" + head[:300] if causes else "")


def _from_flink_json(t, v):
    """Convert Flink's JSON row values back to Python objects, guided by the Iceberg type.

    Args:
        t: Iceberg type of the column.
        v: Value as the gateway's JSON delivers it.

    Returns:
        The value as a Python object comparable with the other engines'.
    """
    import base64
    import datetime as dt
    from decimal import Decimal

    from pyiceberg import types as T

    if v is None:
        return None
    if isinstance(t, (T.BinaryType, T.FixedType, T.UUIDType)) and isinstance(v, str):
        return base64.b64decode(v)
    if isinstance(t, T.DecimalType):
        return Decimal(str(v))
    if isinstance(t, T.DateType) and isinstance(v, str):
        return dt.date.fromisoformat(v)
    if isinstance(t, T.TimeType) and isinstance(v, str):
        return dt.time.fromisoformat(v)
    if isinstance(t, (T.TimestampType, T.TimestamptzType)) and isinstance(v, str):
        ts = dt.datetime.fromisoformat(v)
        return (
            ts.replace(tzinfo=dt.UTC)
            if isinstance(t, T.TimestamptzType) and ts.tzinfo is None
            else ts
        )
    if isinstance(t, T.ListType) and isinstance(v, list):
        return [_from_flink_json(t.element_type, x) for x in v]
    if isinstance(t, T.MapType) and isinstance(v, dict):
        return {k: _from_flink_json(t.value_type, x) for k, x in v.items()}
    if isinstance(t, T.StructType) and isinstance(v, dict):
        return {f.name: _from_flink_json(f.field_type, v.get(f.name)) for f in t.fields}
    return v
