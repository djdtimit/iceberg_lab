"""Files as the Iceberg spec requires them (base test, every engine that writes).

Each engine writes the same table itself (create + rows, as in test_concurrency.seed_table);
then the files are checked as other readers expect them - a reader that relies on the spec
fails on what the writing engine itself happens to tolerate (pg_lake#659: ClickHouse cannot
read pg_lake's manifests, "No partition-spec in iceberg manifest file"):

``test_manifest_metadata``
    every manifest of the current snapshot carries the Avro key-value metadata the spec
    requires ("A manifest file must store the partition spec and other metadata as properties
    in the Avro file's key-value metadata"): schema, partition-spec (all versions) and from v2
    on also partition-spec-id, format-version and content;
``test_manifest_schema_id``
    the same for ``schema-id``, which the spec also requires from v2 on - checked on its own:
    Iceberg Java (the reference implementation, 1.12) does not write it either, so Spark, Trino
    and Flink miss it; a gap here must not hide a missing key that readers depend on;
``test_parquet_field_ids``
    every column of every Parquet data file carries the Iceberg field id ("Parquet files must
    store field ids") - readers match columns by id, not by name.
``test_avro_metadata_reader``
    self-check of the lab's Avro header parser on a hand-built header.
"""

from __future__ import annotations

import io
import json

import pyarrow.parquet as pq
import pytest
import test_concurrency as cc
import test_operations as base

from iceberg_lab.config import SETTINGS

from conftest import ENGINES, engine_param, expect_gap, require_engine, require_format

WAREHOUSE = base.WAREHOUSE
FV = pytest.mark.parametrize("fv", [2, 3], ids=lambda v: f"v{v}")

#: Required manifest key-value metadata per format version (spec, "Manifest File Format"),
#: without ``schema-id`` - see test_manifest_schema_id.
REQUIRED = {
    1: {"schema", "partition-spec"},
    2: {"schema", "partition-spec", "partition-spec-id", "format-version", "content"},
    3: {"schema", "partition-spec", "partition-spec-id", "format-version", "content"},
}

# Filled from observed results only: (test, engine, fv glob "2"/"3"/"*", regex, reason).
# fmt: off
KNOWN_GAPS: list[tuple[str, str, str, str, str]] = [
    *((t, e, "3", base.NO_V3_MSG, f"{e}: " + base.PYICEBERG_NO_V3)
      for t in ("manifest_metadata", "manifest_schema_id", "parquet_field_ids") for e in ("pyiceberg", "polars", "daft")),
    *(("manifest_schema_id", e, "*", r"manifest without schema-id", f"{e} writes manifests without the 'schema-id' key-value metadata the spec requires from v2 on (Iceberg Java, the reference implementation, does not write it either)")
      for e in ("pyiceberg", "polars", "daft", "spark", "trino", "flink")),
]
# fmt: on


def _gap(test: str, engine: str, fv: int):
    """``expect_gap`` for the matching documented gap (none: a plain block).

    Args:
        test: Test name (key in ``KNOWN_GAPS``).
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).

    Returns:
        The context manager.
    """
    for t, e, f, regex, reason in KNOWN_GAPS:
        if t == test and e == engine and f in ("*", str(fv)):
            return expect_gap(reason, regex)
    return expect_gap(None, None)


def _s3():
    """boto3 S3 client with the root keys.

    Returns:
        The client.
    """
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.s3_endpoint,
        aws_access_key_id=SETTINGS.s3_access_key,
        aws_secret_access_key=SETTINGS.s3_secret_key,
        region_name=SETTINGS.s3_region,
    )


def _get(path: str) -> bytes:
    """Bytes of an S3 object.

    Args:
        path: S3 URI of the file.

    Returns:
        The bytes.
    """
    bucket, key = path.removeprefix("s3://").split("/", 1)
    return _s3().get_object(Bucket=bucket, Key=key)["Body"].read()


def _long(buf: io.BytesIO) -> int:
    """Avro long (zig-zag varint).

    Args:
        buf: Byte stream.

    Returns:
        The decoded number.
    """
    shift = result = 0
    while True:
        b = buf.read(1)[0]
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return (result >> 1) ^ -(result & 1)
        shift += 7


def avro_metadata(data: bytes) -> dict[str, bytes]:
    """Key-value metadata of an Avro object container file (header only).

    Args:
        data: Bytes.

    Returns:
        Key -> raw value.
    """
    buf = io.BytesIO(data)
    assert buf.read(4) == b"Obj\x01", "not an Avro object container file"
    meta: dict[str, bytes] = {}
    while True:
        count = _long(buf)
        if count == 0:
            return meta
        if count < 0:  # block with byte size
            count = -count
            _long(buf)
        for _ in range(count):
            key = buf.read(_long(buf)).decode()
            meta[key] = buf.read(_long(buf))


def _written(engine: str, fv: int, namespace: str, engine_pool):
    """The engine creates and fills a table itself; returns the lab client's view of it.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        fv: Iceberg format version (2 or 3).
        namespace: Namespace of the table (the session namespace for fixtures).
        engine_pool: Session pool of engine adapters (conftest fixture).

    Returns:
        The PyIceberg table.
    """
    require_engine(engine)
    require_format(engine, fv)
    ctx = cc.new_ctx("fs", engine, fv, namespace)
    cc.seed_table(engine, ctx, engine_pool)
    return ctx.pi_table()


@FV
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_manifest_metadata(engine, fv, namespace, engine_pool):
    """Every manifest carries the Avro key-value metadata the spec requires.

    The engine writes the table itself (create + rows).

    Checks:
        * at least one manifest exists;
        * each manifest has ``schema`` and ``partition-spec`` (all versions), and from v2 on
          ``partition-spec-id``, ``format-version`` and ``content``;
        * ``format-version`` equals the table's version, ``partition-spec`` is valid JSON.

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    with _gap("manifest_metadata", engine, fv):
        t = _written(engine, fv, namespace, engine_pool)
        manifests = t.current_snapshot().manifests(t.io)
        assert manifests, "no manifest written"
        problems = []
        for m in manifests:
            meta = avro_metadata(_get(m.manifest_path))
            missing = REQUIRED[fv] - meta.keys()
            if missing:
                problems.append(f"{m.manifest_path.rsplit('/', 1)[-1]}: missing {sorted(missing)}")
            elif fv >= 2 and meta["format-version"].decode() != str(fv):
                problems.append(f"format-version {meta['format-version']!r} in a v{fv} table")
            elif "partition-spec" in meta:
                json.loads(meta["partition-spec"])  # must be the spec's JSON
        assert not problems, "manifest key-value metadata: " + "; ".join(problems)


@FV
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_manifest_schema_id(engine, fv, namespace, engine_pool):
    """Every manifest carries ``schema-id`` (required from v2 on), checked on its own.

    Iceberg Java (the reference) does not write it either, so a gap here must not hide another
    missing key.

    Checks:
        * each manifest has a ``schema-id`` and it is a number.

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    with _gap("manifest_schema_id", engine, fv):
        t = _written(engine, fv, namespace, engine_pool)
        for m in t.current_snapshot().manifests(t.io):
            meta = avro_metadata(_get(m.manifest_path))
            assert "schema-id" in meta, f"manifest without schema-id: {m.manifest_path}"
            assert meta["schema-id"].decode().isdigit(), f"schema-id {meta['schema-id']!r}"


@FV
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_parquet_field_ids(engine, fv, namespace, engine_pool):
    """Every column of every Parquet data file carries its Iceberg field id.

    Readers match columns by id, not by name.

    Checks:
        * each column's ``PARQUET:field_id`` equals the field id of the same column in the
          table schema.

    Args:
        engine: Engine under test.
        fv: Iceberg format version of the table (2 or 3).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
    """
    with _gap("parquet_field_ids", engine, fv):
        t = _written(engine, fv, namespace, engine_pool)
        ids = {f.name: f.field_id for f in t.schema().fields}
        problems = []
        for path in t.inspect.files().column("file_path").to_pylist():
            if not path.endswith(".parquet"):
                continue
            schema = pq.read_schema(io.BytesIO(_get(path)))
            for field in schema:
                got = (field.metadata or {}).get(b"PARQUET:field_id")
                if got is None or int(got) != ids.get(field.name):
                    problems.append(f"{path.rsplit('/', 1)[-1]}.{field.name}: field id {got}")
        assert not problems, "Parquet columns without the Iceberg field id: " + "; ".join(problems)


def _zigzag(n: int) -> bytes:
    """Avro long encoding (zig-zag varint) - inverse of ``_long``.

    Args:
        n: Number.

    Returns:
        The bytes.
    """
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def test_avro_metadata_reader():
    """Self-check of the lab's Avro header parser on a hand-built header.

    Checks:
        * a header with one map block (three key/value pairs) and the end marker is parsed into
          exactly these pairs.
    """
    pairs = {"avro.schema": b'"int"', "schema-id": b"0", "content": b"data"}
    header = b"Obj\x01" + _zigzag(len(pairs))
    for k, v in pairs.items():
        header += _zigzag(len(k)) + k.encode() + _zigzag(len(v)) + v
    header += _zigzag(0) + b"\x00" * 16
    assert avro_metadata(header) == pairs
