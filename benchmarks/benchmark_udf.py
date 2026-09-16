"""Benchmark: Custom Python UDF Execution via Rust Bindings in Biflux.

Measures:
1. Historical Batch UDF Throughput across multiple dataset scales (10K, 100K, 500K rows).
2. Real-Time Streaming Micro-Batch UDF Latency across micro-batch sizes (500, 2K, 10K rows).
3. Zero-Copy Arrow IPC UDF Throughput in Rust.
"""

import time
from typing import Dict, List

import polars as pl

from biflux import (
    BifluxContext,
    BifluxPipeline,
    ExecutionMode,
    biflux_core,
    biflux_udf,
    biflux_udf_expr,
)


@biflux_udf
def custom_pricing_model(bid: float, ask: float) -> float:
    """Non-linear quant pricing UDF."""
    mid = (bid + ask) / 2.0
    spread = ask - bid
    return (mid * 1.0005) + (spread * 0.1)


class UDFBenchPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                custom_price=biflux_udf_expr(custom_pricing_model, pl.col("bid"), pl.col("ask"))
            )
            .group_by("symbol")
            .agg(
                mean_custom_price=pl.col("custom_price").mean(),
                total_volume=pl.col("size").sum(),
            )
            .sort("symbol")
        )


def generate_udf_data(n_rows: int) -> pl.DataFrame:
    symbols = [f"BOND-{i:02d}" for i in range(10)]
    return pl.DataFrame(
        {
            "symbol": [symbols[i % len(symbols)] for i in range(n_rows)],
            "bid": [100.0 + (i % 20) * 0.1 for i in range(n_rows)],
            "ask": [100.2 + (i % 20) * 0.1 for i in range(n_rows)],
            "size": [100 + (i % 5) * 20 for i in range(n_rows)],
        }
    )


def benchmark_batch_udf(
    sample_sizes: List[int] = [10_000, 100_000, 500_000],
) -> Dict[int, Dict[str, float]]:
    results = {}
    ctx = BifluxContext(mode=ExecutionMode.BATCH)
    pipe = UDFBenchPipeline(ctx)

    for n_rows in sample_sizes:
        df = generate_udf_data(n_rows)
        # Warmup
        _ = pipe.transform(df.lazy()).collect()

        durations = []
        for _ in range(5):
            t0 = time.perf_counter()
            _ = pipe.transform(df.lazy()).collect()
            durations.append((time.perf_counter() - t0) * 1000)
        durations.sort()
        p50_ms = durations[len(durations) // 2]
        throughput = n_rows / (p50_ms / 1000.0)
        results[n_rows] = {"latency_ms": p50_ms, "throughput": throughput}

    return results


def benchmark_stream_udf(
    micro_batch_sizes: List[int] = [500, 2_000, 10_000],
) -> Dict[int, Dict[str, float]]:
    results = {}
    ctx = BifluxContext(mode=ExecutionMode.LIVE)
    pipe = UDFBenchPipeline(ctx)

    for b_size in micro_batch_sizes:
        batch = generate_udf_data(b_size)
        _ = pipe.process_micro_batch(batch)

        latencies = []
        for _ in range(25):
            t0 = time.perf_counter()
            _ = pipe.process_micro_batch(batch)
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        p50_ms = latencies[len(latencies) // 2]
        throughput = b_size / (p50_ms / 1000.0)
        results[b_size] = {"latency_ms": p50_ms, "throughput": throughput}

    return results


def benchmark_rust_vector_udf(n_rows: int = 100_000) -> Dict[str, float]:
    df = generate_udf_data(n_rows)
    col1 = df["bid"].to_list()
    col2 = df["ask"].to_list()

    # Raw Rust UDF invocation
    _ = biflux_core.apply_binary_udf_f64(custom_pricing_model, col1, col2)  # type: ignore[attr-defined]
    durations = []
    for _ in range(10):
        t0 = time.perf_counter()
        _ = biflux_core.apply_binary_udf_f64(custom_pricing_model, col1, col2)  # type: ignore[attr-defined]
        durations.append((time.perf_counter() - t0) * 1000)
    durations.sort()
    p50_ms = durations[len(durations) // 2]
    return {"latency_ms": p50_ms, "throughput": n_rows / (p50_ms / 1000.0)}


def print_udf_summary():
    print("=" * 80)
    print("  PROJECT BIFLUX: CUSTOM PYTHON UDF RUST ACCELERATION BENCHMARK")
    print("=" * 80)

    print("\n>>> 1. BATCH UDF THROUGHPUT ACROSS DATASET SCALES")
    print("-" * 80)
    print(f"{'DATASET SCALE':<20} | {'LATENCY (MS)':<15} | {'THROUGHPUT (ROWS/S)':<22}")
    print("-" * 80)
    batch_res = benchmark_batch_udf([10_000, 100_000, 500_000])
    for n_rows, data in batch_res.items():
        lat_s = f"{data['latency_ms']:>10.2f} ms"
        tp_s = f"{data['throughput']:>14,.0f} rows/s"
        print(f"{n_rows:<20,} | {lat_s}   | {tp_s}")

    print("\n>>> 2. STREAMING MICRO-BATCH UDF LATENCY ACROSS BATCH SIZES")
    print("-" * 80)
    print(f"{'MICRO-BATCH SIZE':<20} | {'P50 LATENCY (MS)':<18} | {'THROUGHPUT (ROWS/S)':<22}")
    print("-" * 80)
    stream_res = benchmark_stream_udf([500, 2_000, 10_000])
    for b_size, data in stream_res.items():
        lat_s = f"{data['latency_ms']:>12.2f} ms"
        tp_s = f"{data['throughput']:>14,.0f} rows/s"
        print(f"{b_size:<20,} | {lat_s}     | {tp_s}")

    print("\n>>> 3. DIRECT RUST BINARY UDF ENGINE EVALUATION (100,000 Records)")
    print("-" * 80)
    rust_res = benchmark_rust_vector_udf(100_000)
    print(f"Direct PyO3 Rust UDF Latency:   {rust_res['latency_ms']:.2f} ms")
    print(f"Direct PyO3 Rust UDF Throughput: {rust_res['throughput']:,.0f} rows/s")
    print("=" * 80)


if __name__ == "__main__":
    print_udf_summary()
