"""Generate ``n`` rows ``(id, md5(id) as name, id * 0.5 as amount)`` with each engine's OWN write path.

Used by the layout and performance tests. SQL engines generate the rows in-engine (no huge
VALUES lists); PyIceberg, Polars and Daft write an Arrow table built in Python. The target
table must exist with columns ``id BIGINT, name STRING, amount DOUBLE`` - except for
pg_lake, which only writes tables it created itself: use :func:`create_pg_lake_table`.
"""

from __future__ import annotations

import hashlib

import pyarrow as pa

from iceberg_lab.engines import pyiceberg_catalog


def md5(i: int) -> str:
    """Deterministic name for row ``i`` (md5 of its number).

    Args:
        i: Row number.

    Returns:
        The hex digest.
    """
    return hashlib.md5(str(i).encode()).hexdigest()


def arrow_rows(n: int, start: int = 0) -> pa.Table:
    """``n`` generated rows (id, name = md5, amount = id / 2).

    Args:
        n: Number of rows.
        start: First id.

    Returns:
        The Arrow table.
    """
    ids = range(start, start + n)
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "name": pa.array([md5(i) for i in ids], pa.string()),
            "amount": pa.array([i * 0.5 for i in ids], pa.float64()),
        }
    )


def create_pg_lake_table(adapter, ns: str, table: str, options: str = "") -> None:
    """Create a writable pg_lake Iceberg table with the generated schema.

    Args:
        adapter: Engine adapter.
        ns: Namespace.
        table: Table name (without namespace).
        options: Further table options.
    """
    adapter.execute(f'CREATE SCHEMA IF NOT EXISTS "{ns}"')
    adapter.execute(
        f"CREATE TABLE {adapter.fq(ns, table)} (id bigint, name text, amount double precision) "
        f"USING iceberg WITH (catalog = '{adapter.server}'{options})"
    )
    adapter.owned.add((ns, table))


def insert_generated(engine: str, adapter, warehouse: str, ns: str, table: str, n: int) -> None:
    """Append ``n`` generated rows to ``ns.table`` through ``engine``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        warehouse: Warehouse (catalog) name.
        ns: Namespace.
        table: Table name (without namespace).
        n: Number of rows.

    Raises:
        ValueError: an unknown engine.
    """
    fq = adapter.fq(ns, table) if adapter is not None and hasattr(adapter, "fq") else None
    if engine in ("pyiceberg", "polars", "daft"):
        t = pyiceberg_catalog(warehouse).load_table(f"{ns}.{table}")
        data = arrow_rows(n)
        if engine == "pyiceberg":
            t.append(data)
        elif engine == "polars":
            import polars as pl

            pl.from_arrow(data).write_iceberg(t, mode="append")
        else:
            import daft

            daft.from_arrow(data).write_iceberg(t, mode="append")
    elif engine == "duckdb":
        adapter.execute(
            f"INSERT INTO {fq} SELECT i, md5(CAST(i AS VARCHAR)), i * 0.5 FROM range({n}) t(i)"
        )
    elif engine == "spark":
        # COALESCE(1): one writer task, so the file split comes from the target file size
        adapter.execute(
            f"INSERT INTO {fq} SELECT /*+ COALESCE(1) */ id, md5(CAST(id AS STRING)), id * 0.5D "
            f"FROM range({n})"
        )
    elif engine == "trino":
        outer = max(1, n // 10000)
        adapter.execute(
            f"INSERT INTO {fq} SELECT i, lower(to_hex(md5(to_utf8(CAST(i AS VARCHAR))))), i * 0.5E0 "
            f"FROM (SELECT a * 10000 + b AS i FROM UNNEST(sequence(0, {outer - 1})) AS x(a) "
            f"CROSS JOIN UNNEST(sequence(0, {min(n, 10000) - 1})) AS y(b))"
        )
    elif engine == "pg_lake":
        adapter.execute(
            f"INSERT INTO {fq} SELECT i, md5(i::text), i * 0.5 FROM generate_series(0, {n - 1}) i"
        )
    elif engine == "flink":
        gen = f"default_catalog.default_database.gen_{table}"
        adapter.execute(
            f"CREATE TEMPORARY TABLE IF NOT EXISTS {gen} (i BIGINT) WITH ('connector' = 'datagen', "
            f"'number-of-rows' = '{n}', 'fields.i.kind' = 'sequence', "
            f"'fields.i.start' = '0', 'fields.i.end' = '{n - 1}', "
            # datagen throttles to 10 000 rows/s by default - 1 M rows would take 100 s
            f"'rows-per-second' = '1000000000')"
        )
        adapter.execute(f"INSERT INTO {fq} SELECT i, MD5(CAST(i AS STRING)), i * 0.5E0 FROM {gen}")
    else:
        raise ValueError(engine)
