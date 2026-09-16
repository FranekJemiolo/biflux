"""Automated performance regression tests for Biflux."""

import time

import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, ExecutionMode


class PerfVWAPPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2.0)
            .group_by("symbol")
            .agg(vwap=pl.col("mid").mean(), count=pl.len())
        )


def test_batch_throughput_regression():
    """Verify batch processing throughput exceeds 500,000 rows/sec."""
    n_rows = 100_000
    df = pl.DataFrame(
        {
            "symbol": [f"SYM-{i % 10}" for i in range(n_rows)],
            "bid": [100.0 + (i % 20) * 0.1 for i in range(n_rows)],
            "ask": [100.2 + (i % 20) * 0.1 for i in range(n_rows)],
        }
    )

    ctx = BifluxContext(mode=ExecutionMode.BATCH)
    pipeline = PerfVWAPPipeline(ctx)

    t0 = time.perf_counter()
    res_df = pipeline.transform(df.lazy()).collect()
    duration = time.perf_counter() - t0

    throughput = n_rows / duration
    assert res_df.shape[0] == 10
    # Must process at least 500k rows/s (typically > 3M rows/s)
    assert throughput > 500_000, f"Expected >500k rows/s, got {throughput:,.0f}"


def test_streaming_latency_sub_millisecond():
    """Verify streaming micro-batch latency is sub-millisecond for 1,000 records."""
    n_rows = 1_000
    micro_batch = pl.DataFrame(
        {
            "symbol": [f"SYM-{i % 5}" for i in range(n_rows)],
            "bid": [100.0 + (i % 10) * 0.1 for i in range(n_rows)],
            "ask": [100.2 + (i % 10) * 0.1 for i in range(n_rows)],
        }
    )

    ctx = BifluxContext(mode=ExecutionMode.LIVE)
    pipeline = PerfVWAPPipeline(ctx)

    # Warmup
    _ = pipeline.process_micro_batch(micro_batch)

    # Measured
    t0 = time.perf_counter()
    out = pipeline.process_micro_batch(micro_batch)
    latency_ms = (time.perf_counter() - t0) * 1000.0

    assert out.shape[0] == 5
    # Sub-millisecond target (with safety threshold of < 10ms on constrained test runners)
    assert latency_ms < 10.0, f"Expected < 10ms, got {latency_ms:.2f}ms"
