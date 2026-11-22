"""RustFS as Iceberg storage: the S3 features engines rely on, and large files per engine.

Part A - storage level (boto3 directly against RustFS, no engine involved)
    The rest of the suite writes tiny files, so several S3 code paths that real Iceberg
    workloads hit were never exercised: multipart uploads, range reads (Parquet reads its
    footer from the end), bulk deletes (purge, orphan removal), listings past 1000 objects
    (pagination) and read-after-write consistency.

Part B - one large table per engine, same structure as the other matrices
    Every engine writes the same 1.5 million rows (~60 MB of Parquet; md5 strings so they do
    not compress away) through its OWN write path, and every engine reads every such table
    and checks count, sum and a filtered count against values computed in Python. Whether an
    engine used a multipart upload is visible in the data file's ETag (``"...-<parts>"``); it
    is recorded in the test output, not required.
"""

from __future__ import annotations

import hashlib
import os
import uuid

import boto3
import pyarrow as pa
import pytest
from botocore.config import Config

from iceberg_lab.config import SETTINGS, WAREHOUSE_STS
from iceberg_lab.engines import polars_storage_options, pyiceberg_catalog

from conftest import ENGINES, engine_param, fresh

# =========================================================================== part A


@pytest.fixture(scope="module")
def s3():
    """boto3 client with the RustFS root keys (path-style, no retries hiding errors).

    Returns:
        The client.
    """
    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        region_name=SETTINGS.s3_region,
        aws_access_key_id=SETTINGS.s3_access_key,
        aws_secret_access_key=SETTINGS.s3_secret_key,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


@pytest.fixture
def prefix(s3):
    """A fresh key prefix inside the bucket; everything under it is deleted afterwards.

    Args:
        s3: boto3 S3 client.

    Yields:
        The prefix.
    """
    p = f"_rustfs_tests/{uuid.uuid4().hex[:10]}"
    yield p
    keys = [
        o["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=SETTINGS.s3_bucket, Prefix=p
        )
        for o in page.get("Contents", [])
    ]
    for i in range(0, len(keys), 1000):
        s3.delete_objects(
            Bucket=SETTINGS.s3_bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]}
        )


def test_multipart_upload_roundtrip(s3, prefix):
    """30 MB in six 5 MB parts; content and ETag form are correct.

    Checks:
        * the object has the full length;
        * its ETag has the multipart form ``...-6``;
        * the downloaded bytes equal the uploaded ones (SHA-256).

    Args:
        s3: boto3 client with the RustFS root keys (path style, no retries).
        prefix: A fresh key prefix in the bucket; everything under it is deleted afterwards.
    """
    key, part_size = f"{prefix}/big.bin", 5 * 1024 * 1024
    data = os.urandom(6 * part_size)
    mpu = s3.create_multipart_upload(Bucket=SETTINGS.s3_bucket, Key=key)
    parts = []
    for n in range(6):
        r = s3.upload_part(
            Bucket=SETTINGS.s3_bucket,
            Key=key,
            UploadId=mpu["UploadId"],
            PartNumber=n + 1,
            Body=data[n * part_size : (n + 1) * part_size],
        )
        parts.append({"PartNumber": n + 1, "ETag": r["ETag"]})
    s3.complete_multipart_upload(
        Bucket=SETTINGS.s3_bucket,
        Key=key,
        UploadId=mpu["UploadId"],
        MultipartUpload={"Parts": parts},
    )
    head = s3.head_object(Bucket=SETTINGS.s3_bucket, Key=key)
    assert head["ContentLength"] == len(data)
    assert head["ETag"].strip('"').endswith("-6"), head["ETag"]  # S3 multipart ETag convention
    body = s3.get_object(Bucket=SETTINGS.s3_bucket, Key=key)["Body"].read()
    assert hashlib.sha256(body).digest() == hashlib.sha256(data).digest()


def test_aborted_multipart_upload_leaves_nothing(s3, prefix):
    """An aborted multipart upload leaves no visible object.

    Checks:
        * after the abort, listing the key finds nothing.

    Args:
        s3: boto3 client with the RustFS root keys (path style, no retries).
        prefix: A fresh key prefix in the bucket; everything under it is deleted afterwards.
    """
    key = f"{prefix}/aborted.bin"
    mpu = s3.create_multipart_upload(Bucket=SETTINGS.s3_bucket, Key=key)
    s3.upload_part(
        Bucket=SETTINGS.s3_bucket,
        Key=key,
        UploadId=mpu["UploadId"],
        PartNumber=1,
        Body=os.urandom(5 * 1024 * 1024),
    )
    s3.abort_multipart_upload(Bucket=SETTINGS.s3_bucket, Key=key, UploadId=mpu["UploadId"])
    listed = s3.list_objects_v2(Bucket=SETTINGS.s3_bucket, Prefix=key).get("KeyCount", 0)
    assert listed == 0


def test_range_reads(s3, prefix):
    """Byte ranges, open-ended ranges and suffix ranges (how Parquet reads its footer).

    Checks:
        * each range returns exactly the expected bytes with status 206.

    Args:
        s3: boto3 client with the RustFS root keys (path style, no retries).
        prefix: A fresh key prefix in the bucket; everything under it is deleted afterwards.
    """
    key = f"{prefix}/range.bin"
    data = os.urandom(3 * 1024 * 1024 + 17)
    s3.put_object(Bucket=SETTINGS.s3_bucket, Key=key, Body=data)
    for rng, expected in (
        ("bytes=0-99", data[:100]),
        ("bytes=1048576-1048675", data[1048576:1048676]),
        (f"bytes={len(data) - 10}-", data[-10:]),
        ("bytes=-8", data[-8:]),  # Parquet: 4-byte footer length + "PAR1"
    ):
        r = s3.get_object(Bucket=SETTINGS.s3_bucket, Key=key, Range=rng)
        assert r["Body"].read() == expected, rng
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 206, rng


def test_bulk_delete_and_paginated_listing(s3, prefix):
    """1500 objects: the listing paginates without loss or duplicates; bulk delete removes all.

    Checks:
        * the listing needs at least 2 pages and returns exactly the 1500 keys;
        * DeleteObjects in batches reports no errors and nothing is left.

    Args:
        s3: boto3 client with the RustFS root keys (path style, no retries).
        prefix: A fresh key prefix in the bucket; everything under it is deleted afterwards.
    """
    keys = [f"{prefix}/many/{i:05d}" for i in range(1500)]
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(16) as ex:
        list(ex.map(lambda k: s3.put_object(Bucket=SETTINGS.s3_bucket, Key=k, Body=b"x"), keys))
    pages = list(
        s3.get_paginator("list_objects_v2").paginate(
            Bucket=SETTINGS.s3_bucket, Prefix=f"{prefix}/many/", PaginationConfig={"PageSize": 1000}
        )
    )
    listed = [o["Key"] for p in pages for o in p.get("Contents", [])]
    assert len(pages) >= 2, "expected the listing to paginate"
    assert sorted(listed) == keys
    errors = []
    for i in range(0, len(keys), 1000):
        r = s3.delete_objects(
            Bucket=SETTINGS.s3_bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]}
        )
        errors += r.get("Errors", [])
    assert not errors, errors[:3]
    assert (
        s3.list_objects_v2(Bucket=SETTINGS.s3_bucket, Prefix=f"{prefix}/many/").get("KeyCount", 0)
        == 0
    )


def test_read_after_write_and_overwrite_consistency(s3, prefix):
    """A new and an overwritten object are visible at once to GET, HEAD and LIST.

    Checks:
        * GET returns the new bytes, HEAD the new length, LIST one entry with that size.

    Args:
        s3: boto3 client with the RustFS root keys (path style, no retries).
        prefix: A fresh key prefix in the bucket; everything under it is deleted afterwards.
    """
    key = f"{prefix}/consistency.json"
    for version in range(20):
        body = f'{{"version": {version}}}'.encode()
        s3.put_object(Bucket=SETTINGS.s3_bucket, Key=key, Body=body)
        assert s3.get_object(Bucket=SETTINGS.s3_bucket, Key=key)["Body"].read() == body
        assert s3.head_object(Bucket=SETTINGS.s3_bucket, Key=key)["ContentLength"] == len(body)
    listed = s3.list_objects_v2(Bucket=SETTINGS.s3_bucket, Prefix=key)["Contents"]
    assert [o["Size"] for o in listed] == [len(body)]


# =========================================================================== part B

ROWS = 1_500_000
WAREHOUSE = WAREHOUSE_STS


def _md5(i: int) -> str:
    """Deterministic name for row ``i``.

    Args:
        i: Row number.

    Returns:
        The hex digest.
    """
    return hashlib.md5(str(i).encode()).hexdigest()


@pytest.fixture(scope="module")
def expected():
    """Reference values for the big table: (count, sum(amount), count of names starting '0').

    Returns:
        ``(count, sum, count of names starting '0')``.
    """
    prefix0 = sum(1 for i in range(ROWS) if _md5(i)[0] == "0")
    return ROWS, 0.5 * ROWS * (ROWS - 1) / 2, prefix0


#: how each engine writes the big table with its own engine (data generated in-engine)
def _write_big(engine_name: str, e, ns: str, table: str) -> None:
    """Write the big table with the engine's own path.

    Args:
        engine_name: Engine.
        e: Its adapter.
        ns: Namespace.
        table: Table name (without namespace).
    """
    n = ROWS
    if engine_name == "duckdb":
        e.execute(f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, name VARCHAR, amount DOUBLE)")
        e.execute(
            f"INSERT INTO {e.fq(ns, table)} SELECT i, md5(CAST(i AS VARCHAR)), i * 0.5 "
            f"FROM range({n}) t(i)"
        )
    elif engine_name == "spark":
        e.execute(
            f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, name STRING, amount DOUBLE) USING iceberg"
        )
        # COALESCE(1): one output file, like the other engines write
        e.execute(
            f"INSERT INTO {e.fq(ns, table)} SELECT /*+ COALESCE(1) */ id, "
            f"md5(CAST(id AS STRING)), id * 0.5D FROM range({n})"
        )
    elif engine_name == "trino":
        e.execute(f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, name VARCHAR, amount DOUBLE)")
        e.execute(
            f"INSERT INTO {e.fq(ns, table)} SELECT i, lower(to_hex(md5(to_utf8(CAST(i AS VARCHAR))))), "
            f"i * 0.5E0 FROM (SELECT a * 10000 + b AS i FROM UNNEST(sequence(0, {n // 10000 - 1})) "
            f"AS x(a) CROSS JOIN UNNEST(sequence(0, 9999)) AS y(b))"
        )
    elif engine_name == "pg_lake":
        e.execute(f'CREATE SCHEMA IF NOT EXISTS "{ns}"')
        e.execute(
            f"CREATE TABLE {e.fq(ns, table)} (id bigint, name text, amount double precision) "
            f"USING iceberg WITH (catalog = '{e.server}')"
        )
        e.owned.add((ns, table))
        e.execute(
            f"INSERT INTO {e.fq(ns, table)} SELECT i, md5(i::text), i * 0.5 "
            f"FROM generate_series(0, {n - 1}) i"
        )
    elif engine_name == "flink":
        # Bounded datagen sequence 0..n-1 as the source (Flink SQL has no range()/generate_series).
        gen = f"default_catalog.default_database.gen_{table}"
        e.execute(
            f"CREATE TEMPORARY TABLE {gen} (i BIGINT) WITH ('connector' = 'datagen', "
            f"'number-of-rows' = '{n}', 'fields.i.kind' = 'sequence', "
            f"'fields.i.start' = '0', 'fields.i.end' = '{n - 1}', "
            # datagen throttles to 10 000 rows/s by default - 1 M rows would take 100 s
            f"'rows-per-second' = '1000000000')"
        )
        e.execute(f"CREATE TABLE {e.fq(ns, table)} (id BIGINT, name STRING, amount DOUBLE)")
        e.execute(
            f"INSERT INTO {e.fq(ns, table)} SELECT i, MD5(CAST(i AS STRING)), i * 0.5E0 FROM {gen}"
        )
    else:  # pyiceberg, polars, daft: Arrow data built in Python, written by the engine
        ids = pa.array(range(n), pa.int64())
        data = pa.table(
            {
                "id": ids,
                "name": pa.array([_md5(i) for i in range(n)], pa.string()),
                "amount": pa.array([i * 0.5 for i in range(n)], pa.float64()),
            }
        )
        t = e.catalog.create_table(f"{ns}.{table}", schema=data.schema)
        if engine_name == "pyiceberg":
            t.append(data)
        elif engine_name == "polars":
            import polars as pl

            pl.from_arrow(data).write_iceberg(t, mode="append")
        else:
            import daft

            daft.from_arrow(data).write_iceberg(t, mode="append")


def _aggregate(engine_name: str, e, ns: str, table: str) -> tuple:
    """(count, sum(amount), count of names starting with '0') read by the engine itself.

    Args:
        engine_name: Engine.
        e: Its adapter.
        ns: Namespace.
        table: Table name (without namespace).

    Returns:
        ``(count, sum, filtered count)``.
    """
    if engine_name in ("duckdb", "spark", "trino", "pg_lake", "flink"):
        if engine_name == "pg_lake":
            e._attach(ns, table)
        like = "name LIKE '0%'"
        row = e.execute(
            f"SELECT count(*), sum(amount), sum(CASE WHEN {like} THEN 1 ELSE 0 END) "
            f"FROM {e.fq(ns, table)}"
        )[0]
        return int(row[0]), float(row[1]), int(row[2])
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
    if engine_name == "polars":
        import polars as pl

        df = (
            pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties))
            .select(pl.len(), pl.col("amount").sum(), pl.col("name").str.starts_with("0").sum())
            .collect()
        )
        return int(df.item(0, 0)), float(df.item(0, 1)), int(df.item(0, 2))
    if engine_name == "daft":
        import daft
        from daft import col

        d = (
            daft.read_iceberg(t)
            .agg(
                col("id").count().alias("n"),
                col("amount").sum().alias("s"),
                col("name").startswith("0").cast(daft.DataType.int64()).sum().alias("p"),
            )
            .to_pydict()
        )
        return int(d["n"][0]), float(d["s"][0]), int(d["p"][0])
    tbl = t.scan(selected_fields=("name", "amount")).to_arrow()
    import pyarrow.compute as pc

    return (
        tbl.num_rows,
        float(pc.sum(tbl["amount"]).as_py()),
        int(pc.sum(pc.cast(pc.starts_with(tbl["name"], "0"), pa.int64())).as_py()),
    )


@pytest.fixture(scope="module")
def big_tables():
    """writer -> (namespace, table) once it has been written successfully.

    Returns:
        An empty dict.
    """
    return {}


@pytest.mark.slow
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_big_write(writer, namespace, engine_pool, big_tables, expected, s3):
    """The engine writes ~60 MB / 1.5 M rows itself; the snapshot reports all rows.

    Checks:
        * the record counts of the planned data files add up to all rows.

    Args:
        writer: Engine that writes the table.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        big_tables: ``writer -> (namespace, table)`` of the big tables written successfully.
        expected: Reference values of the big table: (count, sum(amount), names starting '0').
        s3: boto3 client with the RustFS root keys (path style, no retries).
    """
    table = f"big_{writer}_{uuid.uuid4().hex[:6]}"
    _write_big(writer, engine_pool(writer), namespace, table)
    t = pyiceberg_catalog(WAREHOUSE).load_table(f"{namespace}.{table}")
    # Row count from the data files' metadata ("total-records" in the snapshot summary is
    # optional in the spec, and pg_lake does not write it).
    assert sum(f.file.record_count for f in t.scan().plan_files()) == expected[0]
    big_tables[writer] = (namespace, table)
    # Record (not require) how the data files were uploaded: multipart ETags end in "-<parts>".
    for f in t.inspect.files().to_pylist():
        bucket, key = f["file_path"].removeprefix("s3://").split("/", 1)
        etag = s3.head_object(Bucket=bucket, Key=key)["ETag"].strip('"')
        print(
            f"{writer}: {f['file_size_in_bytes'] / 1e6:.1f} MB, "
            f"{'multipart ' + etag.split('-')[-1] + ' parts' if '-' in etag else 'single PUT'}"
        )


@pytest.mark.slow
@pytest.mark.parametrize("reader", [engine_param(e) for e in ENGINES])
@pytest.mark.parametrize("writer", [engine_param(e) for e in ENGINES])
def test_big_read(writer, reader, engine_pool, big_tables, expected):
    """The reader aggregates the writer's big table exactly.

    Checks:
        * count, sum(amount) (relative 1e-12) and the filtered count equal the reference values.
        Skipped (blocked) if the writer could not write the big table.

    Args:
        writer: Engine that writes the table.
        reader: Engine that reads it.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        big_tables: ``writer -> (namespace, table)`` of the big tables written successfully.
        expected: Reference values of the big table: (count, sum(amount), names starting '0').
    """
    if writer not in big_tables:
        pytest.skip(f"{writer} could not write the big table (see test_big_write)")
    ns, table = big_tables[writer]
    got = _aggregate(reader, fresh(engine_pool(reader), ns, table), ns, table)
    count, total, prefix0 = expected
    assert got[0] == count
    assert got[1] == pytest.approx(total, rel=1e-12)
    assert got[2] == prefix0
