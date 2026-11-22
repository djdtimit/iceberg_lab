"""Which engine works with which access-delegation mode - and how far does it reach?

Background
    An engine needs two things to read an Iceberg table: the *metadata* (from the catalog)
    and access to the *data files* (in S3). A catalog can grant the second in two ways,
    configured per warehouse (see ``iceberg_lab.config``; neutral test ids ``sts`` and
    ``signing``, the same on every catalog):

    ``signing`` - remote signing (Lakekeeper only; elsewhere "not offered")
        The engine never holds S3 keys. For every S3 request it asks the catalog's signer
        endpoint for a signature; the catalog can check each request individually.
    ``sts`` - vended credentials (Lakekeeper, Polaris, Unity; RustFS vends none)
        The catalog calls STS ``AssumeRole`` on RustFS and hands out short-lived keys that a
        session policy restricts to the table's location.

What is tested
    1. Matrix *engine x warehouse x read/write* where the engine uses ONLY what the catalog
       hands out (``credentials="catalog"``); documented gaps only with their exact error
       (``expect_gap`` in conftest).
    2. The static-key fallback for engines without remote signing.
    3. The *scope* of what the catalog hands out: STS keys must not open other tables, and
       the signer must refuse paths that belong to no table.
    4. Remote signing under 30 repeated commits (a PyIceberg race, intermittent).
"""

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

from iceberg_lab.config import SETTINGS, WAREHOUSE_SIGNING, WAREHOUSE_STS
from iceberg_lab.engines import PyIcebergEngine, rows

from conftest import (
    ENGINES,
    engine_param,
    expect_gap,
    require_remote_signing,
    require_vended_credentials,
    warehouse_params,
)

NO_SIGNING = "{engine} does not support Iceberg REST remote signing"
#: What "no signing support" looks like per engine (observed messages): S3 rejects the
#: request sent without valid credentials, or the client hangs on the EC2 metadata endpoint
#: looking for credentials. Deliberately specific, so a 403 for another reason is not hidden.
NO_SIGNING_ERROR = {
    "duckdb": r"HTTPException: HTTP Error: HTTP (GET|PUT) error .*\(HTTP 403 Forbidden\)",
    "daft": r"(DaftCoreException: .*AccessDenied|OSError: .*ACCESS_DENIED during \w+)",
    "polars": r"169\.254\.169\.254",
}

#: Known gaps: (engine, warehouse, "read"|"write") -> reason; checked with expect_gap.
UNSUPPORTED = {
    ("duckdb", WAREHOUSE_SIGNING, "read"): NO_SIGNING,
    ("duckdb", WAREHOUSE_SIGNING, "write"): NO_SIGNING,
    ("daft", WAREHOUSE_SIGNING, "read"): NO_SIGNING,
    ("daft", WAREHOUSE_SIGNING, "write"): NO_SIGNING,
    # Polars writes through PyIceberg (works), but reads with its own object_store reader.
    ("polars", WAREHOUSE_SIGNING, "read"): NO_SIGNING + " in its native reader",
}

#: Combinations that cannot be expressed with catalog-only access at all - a property of
#: the engine's configuration model, not a bug. These are skipped.
NOT_APPLICABLE = {
    (
        "trino",
        WAREHOUSE_SIGNING,
    ): "Trino has no remote signing; catalog lk_signing uses static keys",
    ("pg_lake", WAREHOUSE_SIGNING): "pg_lake always uses pgduck_server's static S3 secret",
    ("pg_lake", WAREHOUSE_STS): "pg_lake always uses pgduck_server's static S3 secret",
}


def _check_applicable(engine, warehouse, op):
    """Skip not-applicable combinations; return the ``expect_gap`` context for this cell.

    Known gaps only count as expected if the error looks like missing signing support
    (:data:`NO_SIGNING_ERROR`); any other error fails the test.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        warehouse: Warehouse (catalog) name.
        op: ``read`` or ``write``.

    Returns:
        The context manager.
    """
    if (engine, warehouse) in NOT_APPLICABLE:
        pytest.skip(NOT_APPLICABLE[(engine, warehouse)])
    reason = UNSUPPORTED.get((engine, warehouse, op))
    return expect_gap(
        reason.format(engine=engine) if reason else None, NO_SIGNING_ERROR.get(engine)
    )


# =========================================================================== matrix


@pytest.mark.parametrize("warehouse", warehouse_params())
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_read(engine, warehouse, make_engine, namespace, table_name, sample):
    """``engine`` reads a table using only the S3 access the catalog provides.

    The table is written by PyIceberg, which supports both access modes, so a failure can only
    come from the reading engine.

    Checks:
        * the engine, configured without own storage keys, reads exactly the rows PyIceberg
          wrote (via vended STS keys on ``sts``, via remote signing on ``signing``);
        * an engine without that access mode fails only with its documented "no signing" error
          (``_check_applicable``), any other error is a new failure.

    Args:
        engine: Engine under test (reader).
        warehouse: Warehouse with the access mode under test (``sts`` or ``signing``).
        make_engine: Factory for an engine adapter on a warehouse and credential mode.
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
        sample: Small Arrow table (id, name, amount) that is written and expected back.
    """
    require_vended_credentials()
    gap = _check_applicable(engine, warehouse, "read")
    PyIcebergEngine(warehouse).write(namespace, table_name, sample)
    with gap:
        assert make_engine(engine, warehouse).read(namespace, table_name) == rows(sample)


@pytest.mark.parametrize("warehouse", warehouse_params())
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_write(engine, warehouse, make_engine, namespace, table_name, sample):
    """``engine`` creates and fills a table using only the S3 access the catalog provides.

    Checks:
        * the engine writes the sample table without own storage keys (vended STS keys on
          ``sts``, remote signing on ``signing``);
        * PyIceberg then reads exactly the sample rows back - verified by another client, so the
          check does not depend on the engine reading its own data;
        * an engine without that access mode fails only with its documented error.

    Args:
        engine: Engine under test (writer).
        warehouse: Warehouse with the access mode under test (``sts`` or ``signing``).
        make_engine: Factory for an engine adapter on a warehouse and credential mode.
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
        sample: Small Arrow table (id, name, amount) that is written and expected back.
    """
    require_vended_credentials()
    gap = _check_applicable(engine, warehouse, "write")
    engine_obj = make_engine(engine, warehouse)
    with gap:
        engine_obj.write(namespace, table_name, sample)
    assert PyIcebergEngine(warehouse).read(namespace, table_name) == rows(sample)


@pytest.mark.parametrize("engine", [engine_param(e) for e in ("duckdb", "polars", "daft")])
def test_static_keys_fallback_on_signing_warehouse(
    engine, make_engine, namespace, table_name, sample
):
    """Engines without remote signing still work on ``signing`` when given static keys.

    The practical workaround - at the price that the engine holds keys for the whole bucket
    instead of per-request signatures.

    Checks:
        * the engine, configured with the lab's static S3 keys on the signing warehouse, writes
          the sample table and reads exactly its rows back.

    Args:
        engine: Engine without remote signing (DuckDB, Polars, Daft).
        make_engine: Factory for an engine adapter on a warehouse and credential mode.
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
        sample: Small Arrow table (id, name, amount) that is written and expected back.
    """
    require_remote_signing()
    e = make_engine(engine, WAREHOUSE_SIGNING, credentials="static")
    e.write(namespace, table_name, sample)
    assert e.read(namespace, table_name) == rows(sample)


# =========================================================================== scoping


def _two_tables(warehouse, namespace, table_name, sample):
    """Create tables ``<name>_a`` and ``<name>_b`` in ``warehouse`` and return both (loaded).

    Args:
        warehouse: Warehouse (catalog) name.
        namespace: Namespace of the table (the session namespace for fixtures).
        table_name: Unique table name for the test.
        sample: Arrow table written into both.

    Returns:
        ``(table A, table B)``.
    """
    pi = PyIcebergEngine(warehouse)
    pi.write(namespace, f"{table_name}_a", sample)
    pi.write(namespace, f"{table_name}_b", sample)
    a = pi.catalog.load_table(f"{namespace}.{table_name}_a")
    b = pi.catalog.load_table(f"{namespace}.{table_name}_b")
    return a, b


def _split(s3_uri: str) -> tuple[str, str]:
    """``s3://bucket/some/key`` -> ``("bucket", "some/key")``.

    Args:
        s3_uri: S3 URI.

    Returns:
        ``(bucket, key)``.
    """
    bucket, key = s3_uri.removeprefix("s3://").split("/", 1)
    return bucket, key


def test_vended_credentials_are_scoped_to_their_table(namespace, table_name, sample):
    """STS keys vended for table A open A's files, but not the files of table B.

    The keys are taken from A's ``loadTable`` response and used directly with boto3, bypassing
    every engine - this tests what a user could do with leaked credentials.

    Checks:
        * A's ``loadTable`` answer contains vended STS keys (with a session token);
        * with them, a HEAD of A's metadata file succeeds;
        * a HEAD of B's metadata file is refused with 403 / AccessDenied.

    Args:
        namespace: Fresh namespace of the test session.
        table_name: Base name; tables A and B are derived from it.
        sample: Small Arrow table written into both tables.
    """
    require_vended_credentials()
    a, b = _two_tables(WAREHOUSE_STS, namespace, table_name, sample)
    p = a.io.properties
    assert "s3.session-token" in p, "expected vended STS credentials"
    s3 = boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        aws_access_key_id=p["s3.access-key-id"],
        aws_secret_access_key=p["s3.secret-access-key"],
        aws_session_token=p["s3.session-token"],
        region_name=SETTINGS.s3_region,
    )
    own_bucket, own_key = _split(a.metadata_location)
    s3.head_object(Bucket=own_bucket, Key=own_key)  # own table: allowed

    other_bucket, other_key = _split(b.metadata_location)
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=other_bucket, Key=other_key)
    # HEAD responses have no body, so botocore only sees the status code "403".
    assert exc.value.response["Error"]["Code"] in ("403", "AccessDenied")


def test_remote_signer_scope(namespace, table_name, sample):
    """The remote signer signs only paths that belong to a table.

    The tabular id in the signer URL is only a hint: Lakekeeper resolves the table from the
    requested location. This lab's main instance runs without authorization, so another table's
    path IS signed; with OpenFGA it would depend on the caller's rights on table B.

    Checks:
        * the signer signs a GET of table A's metadata (2xx);
        * it also signs table B's metadata (expected without authorization, see above);
        * it refuses (4xx) a path in the bucket outside any table, a path in another warehouse's
          prefix and a path in a bucket the catalog does not manage.

    Args:
        namespace: Fresh namespace of the test session.
        table_name: Base name; tables A and B are derived from it.
        sample: Small Arrow table written into both tables.
    """
    require_remote_signing()
    a, b = _two_tables(WAREHOUSE_SIGNING, namespace, table_name, sample)
    p = a.io.properties
    # Signer URL as a client builds it: base URI + per-table endpoint path from loadTable.
    sign_url = p["s3.signer.uri"].rstrip("/") + "/" + p["s3.signer.endpoint"]

    def sign(s3_uri):
        """Ask the signer to sign a GET for ``s3_uri`` (same request body a client sends).

        Args:
            s3_uri: S3 URI.

        Returns:
            The signer's response.
        """
        bucket, key = _split(s3_uri)
        return requests.post(
            sign_url,
            json={
                "region": SETTINGS.s3_region,
                "method": "GET",
                "uri": f"{SETTINGS.s3_endpoint}/{bucket}/{key}",
                "headers": {},
            },
            timeout=10,
        )

    assert sign(a.metadata_location).ok
    assert sign(b.metadata_location).ok  # allowed: no authz in this lab, see docstring

    for foreign in (
        f"s3://{SETTINGS.s3_bucket}/not-a-table/x.parquet",  # same bucket, no table
        f"s3://{SETTINGS.s3_bucket}/{WAREHOUSE_STS}/x.json",  # other warehouse's prefix
        "s3://some-other-bucket/x",  # bucket Lakekeeper does not manage
    ):
        refused = sign(foreign)
        assert 400 <= refused.status_code < 500, (foreign, refused.text)


# =========================================================================== stability


def test_remote_signing_is_reliable_under_repeated_commits(namespace, table_name, sample):
    """30 PyIceberg appends on the signing warehouse must all succeed.

    Root cause of the failures (traced, see README): a race in PyIceberg's
    ``pyiceberg/io/fsspec.py::_s3`` - re-registering the signer on a shared s3fs client leaves
    a window in which a request goes out unsigned (RustFS: anonymous -> 403).

    Checks:
        * 30 appends in a row, every failure counted (not stopped at the first), so the report
          shows the rate;
        * no append got 403 - else the documented intermittent gap (flaky: a clean run does not
          mean it is fixed).

    Args:
        namespace: Fresh namespace of the test session.
        table_name: Unique table name for this test.
        sample: Small Arrow table whose schema the appended rows use.
    """
    import pyarrow as pa

    require_remote_signing()
    pi = PyIcebergEngine(WAREHOUSE_SIGNING)
    t = pi.catalog.create_table(f"{namespace}.{table_name}", schema=sample.schema)
    errors = []
    attempts = 30
    for i in range(attempts):
        try:
            t.append(pa.table({"id": [i], "name": ["x"], "amount": [1.0]}, schema=sample.schema))
        except PermissionError as exc:  # s3fs maps 403 to PermissionError
            errors.append(str(exc))
    with expect_gap(
        "PyIceberg race: re-registering the remote signer on a shared s3fs client leaves a window in which requests go out unsigned (RustFS: anonymous -> 403)",
        r"signed commits got 403",
        flaky=True,  # intermittent: a clean run does not mean it is fixed
    ):
        assert not errors, f"{len(errors)}/{attempts} signed commits got 403: {errors[:3]}"
