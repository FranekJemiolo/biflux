"""Performance and scalability benchmark suite for Project Biflux.

Measures:
1. Historical batch throughput across 100K, 500K, and 1M record lakehouse tables.
2. Real-time streaming micro-batch latency across various batch sizes.
3. Native Rust Arrow IPC roundtrip throughput and memory transfer rate.
"""

import io
import os
import tempfile
import time
from typing import Dict, List

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc

from biflux import biflux_core
from biflux.core import BifluxContext, BifluxPipeline, ExecutionMode


class BenchmarkVWAPPipeline(BifluxPipeline):
    """High-throughput transformation pipeline for benchmarking."""

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                mid_price=(pl.col("bid") + pl.col("ask")) / 2.0,
                dollar_volume=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
            )
            .group_by("symbol")
            .agg(
                total_volume=pl.col("size").sum(),
                total_dollar_volume=pl.col("dollar_volume").sum(),
                vwap=(pl.col("dollar_volume").sum() / pl.col("size").sum()).round(6),
                ticks_processed=pl.len(),
            )
            .sort("symbol")
        )


def generate_benchmark_data(n_rows: int) -> pl.DataFrame:
    """Generate high-volume quantitative quotes for performance testing."""
    symbols = [f"BOND-TICK-{i:03d}" for i in range(1, 51)]

    # Use Polars native fast generation
    return pl.DataFrame(
        {
            "symbol": [symbols[i % len(symbols)] for i in range(n_rows)],
            "bid": [98.0 + (i % 100) * 0.05 for i in range(n_rows)],
            "ask": [98.1 + (i % 100) * 0.05 for i in range(n_rows)],
            "size": [100 + (i % 10) * 50 for i in range(n_rows)],
            "timestamp": [1_700_000_000_000 + i * 10 for i in range(n_rows)],
        }
    )


def run_batch_benchmarks() -> List[Dict[str, float]]:
    print("-" * 80)
    print(">>> 1. HISTORICAL BATCH BACKTEST THROUGHPUT BENCHMARKS")
    print("-" * 80)

    scales = [100_000, 500_000, 1_000_000]
    results = []

    ctx = BifluxContext(mode=ExecutionMode.BATCH)
    pipeline = BenchmarkVWAPPipeline(ctx)

    for n_rows in scales:
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, f"raw_{n_rows}.parquet")
            sink = os.path.join(tmpdir, f"sink_{n_rows}.parquet")

            df = generate_benchmark_data(n_rows)
            df.write_parquet(src)
            file_size_mb = os.path.getsize(src) / (1024 * 1024)

            # Warmup
            _ = pipeline.run(src, sink, sample_df=df.lazy())

            # Measured runs (3 iterations)
            durations = []
            for _ in range(3):
                t0 = time.perf_counter()
                _ = pipeline.run(src, sink, sample_df=df.lazy())
                durations.append(time.perf_counter() - t0)

            avg_duration = sum(durations) / len(durations)
            rows_per_sec = n_rows / avg_duration
            mb_per_sec = file_size_mb / avg_duration

            print(
                f"[*] Scale: {n_rows:>9,} rows | File: {file_size_mb:>5.1f} MB | "
                f"Time: {avg_duration * 1000:>6.1f} ms | "
                f"Throughput: {rows_per_sec:>11,.0f} rows/s | "
                f"Bandwidth: {mb_per_sec:>6.1f} MB/s"
            )

            results.append(
                {
                    "n_rows": n_rows,
                    "file_size_mb": file_size_mb,
                    "duration_ms": avg_duration * 1000,
                    "rows_per_sec": rows_per_sec,
                    "mb_per_sec": mb_per_sec,
                }
            )

    return results


def run_streaming_benchmarks() -> List[Dict[str, float]]:
    print("\n" + "-" * 80)
    print(">>> 2. REAL-TIME STREAMING MICRO-BATCH LATENCY BENCHMARKS")
    print("-" * 80)

    ctx = BifluxContext(mode=ExecutionMode.LIVE)
    pipeline = BenchmarkVWAPPipeline(ctx)

    batch_sizes = [100, 500, 1_000, 5_000, 10_000, 50_000]
    results = []

    for bs in batch_sizes:
        batch_df = generate_benchmark_data(bs)

        # Warmup
        _ = pipeline.process_micro_batch(batch_df)

        # 20 iterations to compute latency distribution
        latencies = []
        for _ in range(20):
            t0 = time.perf_counter()
            _ = pipeline.process_micro_batch(batch_df)
            latencies.append((time.perf_counter() - t0) * 1000)  # ms

        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p90 = latencies[int(len(latencies) * 0.90)]
        p99 = latencies[int(len(latencies) * 0.99)]
        throughput = bs / (p50 / 1000.0)

        print(
            f"[*] Batch Size: {bs:>6,} rows | P50: {p50:>5.2f} ms | P90: {p90:>5.2f} ms | "
            f"P99: {p99:>5.2f} ms | Throughput: {throughput:>11,.0f} rows/s"
        )

        results.append(
            {
                "batch_size": bs,
                "p50_ms": p50,
                "p90_ms": p90,
                "p99_ms": p99,
                "throughput_rows_sec": throughput,
            }
        )

    return results


def run_arrow_ipc_benchmarks() -> Dict[str, float]:
    print("\n" + "-" * 80)
    print(">>> 3. NATIVE RUST ZERO-COPY ARROW IPC MEMORY BENCHMARK")
    print("-" * 80)

    n_records = 100_000
    schema = pa.schema(
        [
            pa.field("symbol", pa.string()),
            pa.field("bid", pa.float64()),
            pa.field("ask", pa.float64()),
            pa.field("size", pa.int64()),
            pa.field("timestamp", pa.int64()),
        ]
    )

    table = pa.Table.from_arrays(
        [
            pa.array([f"BOND-{i % 20}" for i in range(n_records)]),
            pa.array([99.0 + (i % 50) * 0.1 for i in range(n_records)]),
            pa.array([99.1 + (i % 50) * 0.1 for i in range(n_records)]),
            pa.array([100 + (i % 5) * 50 for i in range(n_records)]),
            pa.array([1_700_000_000 + i for i in range(n_records)]),
        ],
        schema=schema,
    )

    sink = io.BytesIO()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    ipc_bytes = sink.getvalue()
    ipc_size_mb = len(ipc_bytes) / (1024 * 1024)

    # Benchmark Rust FFI roundtrip
    latencies = []
    for _ in range(10):
        t0 = time.perf_counter()
        _ = biflux_core.execute_arrow_ipc(b"plan_bytes", ipc_bytes)
        latencies.append(time.perf_counter() - t0)

    avg_time = sum(latencies) / len(latencies)
    ipc_throughput = ipc_size_mb / avg_time
    records_per_sec = n_records / avg_time

    print(
        f"[*] Arrow IPC Ingest: {n_records:,} records ({ipc_size_mb:.2f} MB) | "
        f"Rust FFI Time: {avg_time * 1000:.2f} ms | "
        f"Bandwidth: {ipc_throughput:,.1f} MB/s | "
        f"Rate: {records_per_sec:,.0f} records/s"
    )

    return {
        "n_records": n_records,
        "ipc_size_mb": ipc_size_mb,
        "time_ms": avg_time * 1000,
        "bandwidth_mb_s": ipc_throughput,
        "records_per_sec": records_per_sec,
    }


def run_all_benchmarks():
    print("=" * 80)
    print("  PROJECT BIFLUX: PERFORMANCE & LATENCY BENCHMARK SUITE")
    print("=" * 80)

    batch_res = run_batch_benchmarks()
    stream_res = run_streaming_benchmarks()
    ipc_res = run_arrow_ipc_benchmarks()

    print("\n" + "=" * 80)
    print(">>> BENCHMARK SUMMARY")
    print("=" * 80)
    print(f"[*] Peak Batch Throughput: {max(r['rows_per_sec'] for r in batch_res):,.0f} rows/s")
    print(f"[*] Sub-millisecond Micro-batch Latency: {stream_res[0]['p50_ms']:.2f} ms (100 rows)")
    print(
        f"[*] Peak Streaming Rate: {max(r['throughput_rows_sec'] for r in stream_res):,.0f} rows/s"
    )
    print(f"[*] Rust Arrow IPC Bandwidth: {ipc_res['bandwidth_mb_s']:,.1f} MB/s")
    print("=" * 80)


if __name__ == "__main__":
    run_all_benchmarks()
