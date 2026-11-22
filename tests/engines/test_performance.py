"""Performance comparison: same workloads, every engine, wall-clock seconds.

Workloads (correctness is asserted too, so a fast wrong answer fails):

``write``      the engine writes 1 000 000 rows into its own new table (``tests/lab/datagen.py``)
``full_scan``  ``count(*), sum(amount)`` over ONE common table (written once by PyIceberg,
               so every engine reads the same files)
``selective``  ``count(*) WHERE id BETWEEN 500000 AND 500999`` on the common table (1 000
               rows; min/max statistics allow skipping data)

Reads are timed 3 times, the median is reported (the first run includes catalog loading and
JIT warm-up). Times include each engine's fixed overhead - e.g. a Flink job submission or a
Spark stage - because that is what a user waits for. The results are written to
``reports/performance.json`` and ``reports/performance.md``. Numbers come from one lab
machine with everything in Docker; compare engines with each other, not with production.
"""

from __future__ import annotations

import json
import statistics
import time
import uuid
from pathlib import Path

import datagen
import pytest
import test_operations as base

from iceberg_lab.engines import polars_storage_options, pyiceberg_catalog

from conftest import ENGINES, engine_param

WAREHOUSE = base.WAREHOUSE
N = 1_000_000
LO, HI = 500_000, 500_999
REPORT = Path(__file__).resolve().parents[2] / "reports"
pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def timings():
    """Collected timings, written to reports/performance.json at the end.

    Yields:
        Engine -> measurement -> seconds.
    """
    out: dict[str, dict[str, float]] = {}
    yield out
    REPORT.mkdir(exist_ok=True)
    (REPORT / "performance.json").write_text(json.dumps(out, indent=2))
    workloads = ["write", "full_scan", "selective"]
    lines = [
        f"# Performance ({N:,} rows; seconds; reads: median of 3 runs)",
        "",
        "| Engine | " + " | ".join(workloads) + " |",
        "|---|" + "---|" * len(workloads),
    ]
    for e in ENGINES:
        if e in out:
            cells = [f"{out[e][w]:.2f}" if w in out[e] else "–" for w in workloads]
            lines.append(f"| {e} | " + " | ".join(cells) + " |")
    (REPORT / "performance.md").write_text("\n".join(lines) + "\n")


@pytest.fixture(scope="module")
def common(namespace):
    """The shared read table: N rows written by PyIceberg.

    Args:
        namespace: Namespace of the table (the session namespace for fixtures).

    Returns:
        ``(namespace, table)``.
    """
    name = f"perf_common_{uuid.uuid4().hex[:6]}"
    t = pyiceberg_catalog(WAREHOUSE).create_table(
        f"{namespace}.{name}", schema=datagen.arrow_rows(1).schema
    )
    t.append(datagen.arrow_rows(N))
    return name


def run_query(engine, adapter, ns, table, selective: bool) -> tuple:
    """``(count, sum(amount))`` or ``(count,)`` for the selective query, read by ``engine``.

    Args:
        engine: Engine name (see ``conftest.ENGINES``).
        adapter: Engine adapter.
        ns: Namespace.
        table: Table name (without namespace).
        selective: Run the selective query (else the full scan).

    Returns:
        The result row.
    """
    if engine in ("pyiceberg", "polars", "daft"):
        t = pyiceberg_catalog(WAREHOUSE).load_table(f"{ns}.{table}")
        if engine == "pyiceberg":
            if selective:
                return (
                    t.scan(row_filter=f"id >= {LO} and id <= {HI}", selected_fields=("id",))
                    .to_arrow()
                    .num_rows,
                )
            a = t.scan(selected_fields=("amount",)).to_arrow()
            import pyarrow.compute as pc

            return a.num_rows, pc.sum(a["amount"]).as_py()
        if engine == "polars":
            import polars as pl

            lf = pl.scan_iceberg(t, storage_options=polars_storage_options(t.io.properties))
            if selective:
                return (
                    lf.filter(pl.col("id").is_between(LO, HI)).select(pl.len()).collect().item(),
                )
            df = lf.select(pl.len(), pl.col("amount").sum()).collect()
            return df.item(0, 0), df.item(0, 1)
        import daft
        from daft import col

        df = daft.read_iceberg(t)
        if selective:
            return (df.where((col("id") >= LO) & (col("id") <= HI)).count_rows(),)
        d = df.agg(col("id").count().alias("n"), col("amount").sum().alias("s")).to_pydict()
        return d["n"][0], d["s"][0]
    if hasattr(adapter, "_attach"):
        adapter._attach(ns, table)
    fq = adapter.fq(ns, table)
    if selective:
        return (
            int(adapter.execute(f"SELECT count(*) FROM {fq} WHERE id BETWEEN {LO} AND {HI}")[0][0]),
        )
    r = adapter.execute(f"SELECT count(*), sum(amount) FROM {fq}")[0]
    return int(r[0]), float(r[1])


def _timed(fn, runs):
    """Run ``fn`` several times.

    Args:
        fn: Callable to run.
        runs: Number of runs.

    Returns:
        ``(median seconds, last result)``.
    """
    ts, result = [], None
    for _ in range(runs):
        t0 = time.perf_counter()
        result = fn()
        ts.append(time.perf_counter() - t0)
    return statistics.median(ts), result


@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_perf_write(engine, namespace, engine_pool, timings):
    """The engine writes N rows with its own write path (timed; correctness only).

    Checks:
        * the table then has exactly N rows. The time goes to reports/performance.json, it is
          not compared.

    Args:
        engine: Engine under test.
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        timings: Collected timings, written to reports/performance.json at the end.
    """
    table = f"perf_w_{engine}_{uuid.uuid4().hex[:6]}"
    adapter = None if engine in ("pyiceberg", "polars", "daft") else engine_pool(engine)
    if engine == "pg_lake":
        datagen.create_pg_lake_table(adapter, namespace, table)
    else:
        pyiceberg_catalog(WAREHOUSE).create_table(
            f"{namespace}.{table}", schema=datagen.arrow_rows(1).schema
        )
    secs, _ = _timed(
        lambda: datagen.insert_generated(engine, adapter, WAREHOUSE, namespace, table, N), 1
    )
    n = (
        pyiceberg_catalog(WAREHOUSE)
        .load_table(f"{namespace}.{table}")
        .scan(selected_fields=("id",))
        .to_arrow()
        .num_rows
    )
    assert n == N
    timings.setdefault(engine, {})["write"] = secs


@pytest.mark.parametrize("workload", ["full_scan", "selective"])
@pytest.mark.parametrize("engine", [engine_param(e) for e in ENGINES])
def test_perf_read(engine, workload, common, namespace, engine_pool, timings):
    """Median of 3 runs of a workload on the common table; the answer must be right.

    Checks:
        * selective: the count of the id range is exact;
        * full scan: count N and the sum of the amount column are right.

    Args:
        engine: Engine under test.
        workload: ``full_scan`` or ``selective``.
        common: The shared read table (N rows written by PyIceberg).
        namespace: Fresh namespace of the test session.
        engine_pool: Session pool of engine adapters on the ``sts`` warehouse (conftest).
        timings: Collected timings, written to reports/performance.json at the end.
    """
    adapter = None if engine in ("pyiceberg", "polars", "daft") else engine_pool(engine)
    selective = workload == "selective"
    secs, result = _timed(lambda: run_query(engine, adapter, namespace, common, selective), 3)
    if selective:
        assert result == (HI - LO + 1,), result
    else:
        assert result[0] == N and abs(result[1] - 0.5 * N * (N - 1) / 2) < 1e-3 * N, result
    timings.setdefault(engine, {})[workload] = secs
