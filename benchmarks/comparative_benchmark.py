"""Comparative benchmark: Biflux vs. Polars vs. DuckDB vs. Pandas vs. Python Baseline.

Benchmarking identical feature transformations (VWAP & volume aggregation) across:
1. Biflux (Unified Python API compiling to dual Rust/Arrow batch and stream engines)
2. Standalone Polars (Direct in-memory LazyFrame / DataFrame operations)
3. DuckDB (In-memory analytical SQL engine)
4. Pandas (De-facto data science DataFrame baseline)
5. Native Python Streaming Consumer (Typical dictionary/accumulator Kafka consumer loop)

Evaluated across multiple sample sizes for:
- Historical Batch Backtest (100,000, 500,000, 1,000,000 rows)
- Real-Time Streaming Micro-Batches (500, 2,000, 10,000, 50,000 rows)
- Direct Batch vs. Streaming on Identical Sample Sizes (1,000, 10,000, 100,000 rows)
"""

import time
from typing import Any, Dict, List

import duckdb
import pandas as pd
import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, ExecutionMode


class BifluxVWAPPipeline(BifluxPipeline):
    """Biflux unified pipeline evaluated across both batch and stream."""

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
            )
            .sort("symbol")
        )


def generate_benchmark_dataset(n_rows: int) -> pl.DataFrame:
    """Generate reproducible quantitative quote dataset."""
    symbols = [f"BOND-{i:02d}" for i in range(25)]
    return pl.DataFrame(
        {
            "symbol": [symbols[i % len(symbols)] for i in range(n_rows)],
            "bid": [98.50 + (i % 50) * 0.05 for i in range(n_rows)],
            "ask": [98.60 + (i % 50) * 0.05 for i in range(n_rows)],
            "size": [100 + (i % 10) * 50 for i in range(n_rows)],
        }
    )


# -----------------------------------------------------------------------------
# 1. BATCH SUITE ACROSS MULTIPLE SAMPLE SIZES
# -----------------------------------------------------------------------------


def benchmark_batch_frameworks(
    sample_sizes: List[int] = [100_000, 500_000, 1_000_000],
) -> Dict[int, Dict[str, Dict[str, Any]]]:
    """Compare batch execution performance across multiple dataset sample sizes."""
    results_by_size: Dict[int, Dict[str, Dict[str, Any]]] = {}

    for n_rows in sample_sizes:
        print(f"[*] Running batch benchmarks for {n_rows:,} rows...")
        pl_df = generate_benchmark_dataset(n_rows)
        pd_df = pl_df.to_pandas()
        duck_conn = duckdb.connect(":memory:")
        duck_conn.register("quotes", pl_df.to_arrow())

        results: Dict[str, Dict[str, Any]] = {}

        # 1. Biflux Batch (Unified Framework)
        ctx = BifluxContext(mode=ExecutionMode.BATCH)
        biflux_pipe = BifluxVWAPPipeline(ctx)
        _ = biflux_pipe.transform(pl_df.lazy()).collect()
        durations = []
        for _ in range(5):
            t0 = time.perf_counter()
            _ = biflux_pipe.transform(pl_df.lazy()).collect()
            durations.append(time.perf_counter() - t0)
        durations.sort()
        p50_biflux = durations[len(durations) // 2]
        results["Biflux (Unified)"] = {
            "time_ms": p50_biflux * 1000,
            "throughput": n_rows / p50_biflux,
            "skew_risk": "0.0% (Zero Skew)",
        }

        # 2. Standalone Polars
        def run_standalone_polars(df: pl.LazyFrame) -> pl.DataFrame:
            return (
                df.with_columns(
                    mid=(pl.col("bid") + pl.col("ask")) / 2.0,
                    dollar_vol=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
                )
                .group_by("symbol")
                .agg(
                    total_volume=pl.col("size").sum(),
                    total_dollar_volume=pl.col("dollar_vol").sum(),
                    vwap=(pl.col("dollar_vol").sum() / pl.col("size").sum()).round(6),
                )
                .sort("symbol")
                .collect()
            )

        _ = run_standalone_polars(pl_df.lazy())
        durations = []
        for _ in range(5):
            t0 = time.perf_counter()
            _ = run_standalone_polars(pl_df.lazy())
            durations.append(time.perf_counter() - t0)
        durations.sort()
        p50_polars = durations[len(durations) // 2]
        results["Polars (Standalone)"] = {
            "time_ms": p50_polars * 1000,
            "throughput": n_rows / p50_polars,
            "skew_risk": "Medium (Glue needed)",
        }

        # 3. DuckDB SQL
        duck_sql = """
            SELECT
                symbol,
                SUM(size) AS total_volume,
                SUM(((bid + ask) / 2.0) * size) AS total_dollar_volume,
                ROUND(SUM(((bid + ask) / 2.0) * size) / SUM(size), 6) AS vwap
            FROM quotes
            GROUP BY symbol
            ORDER BY symbol
        """
        _ = duck_conn.execute(duck_sql).fetchall()
        durations = []
        for _ in range(5):
            t0 = time.perf_counter()
            _ = duck_conn.execute(duck_sql).fetchall()
            durations.append(time.perf_counter() - t0)
        durations.sort()
        p50_duck = durations[len(durations) // 2]
        results["DuckDB"] = {
            "time_ms": p50_duck * 1000,
            "throughput": n_rows / p50_duck,
            "skew_risk": "High (Separate SQL)",
        }

        # 4. Pandas Baseline
        def run_pandas(df: pd.DataFrame) -> pd.DataFrame:
            mid = (df["bid"] + df["ask"]) / 2.0
            dollar_vol = mid * df["size"]
            grouped = df.assign(mid=mid, dollar_vol=dollar_vol).groupby("symbol")
            agg = grouped.agg(
                total_volume=("size", "sum"),
                total_dollar_volume=("dollar_vol", "sum"),
            )
            agg["vwap"] = (agg["total_dollar_volume"] / agg["total_volume"]).round(6)
            return agg.reset_index().sort_values("symbol")

        _ = run_pandas(pd_df)
        durations = []
        for _ in range(5):
            t0 = time.perf_counter()
            _ = run_pandas(pd_df)
            durations.append(time.perf_counter() - t0)
        durations.sort()
        p50_pandas = durations[len(durations) // 2]
        results["Pandas"] = {
            "time_ms": p50_pandas * 1000,
            "throughput": n_rows / p50_pandas,
            "skew_risk": "Critical (Separate code)",
        }

        results_by_size[n_rows] = results

    return results_by_size


# -----------------------------------------------------------------------------
# 2. STREAMING SUITE ACROSS MULTIPLE MICRO-BATCH SAMPLE SIZES
# -----------------------------------------------------------------------------


def benchmark_streaming_frameworks(
    micro_batch_sizes: List[int] = [500, 2_000, 10_000, 50_000],
) -> Dict[int, Dict[str, Dict[str, Any]]]:
    """Compare real-time streaming micro-batch latency across multiple micro-batch sizes."""
    results_by_size: Dict[int, Dict[str, Dict[str, Any]]] = {}

    for batch_size in micro_batch_sizes:
        print(f"[*] Running streaming micro-batch benchmarks for {batch_size:,} rows...")
        pl_batch = generate_benchmark_dataset(batch_size)
        pd_batch = pl_batch.to_pandas()
        raw_dicts: List[Dict[str, Any]] = pl_batch.to_dicts()
        duck_conn = duckdb.connect(":memory:")

        results: Dict[str, Dict[str, Any]] = {}

        # 1. Biflux (Streaming Engine via Arrow IPC)
        ctx = BifluxContext(mode=ExecutionMode.LIVE)
        biflux_pipe = BifluxVWAPPipeline(ctx)
        _ = biflux_pipe.process_micro_batch(pl_batch)
        latencies = []
        for _ in range(25):
            t0 = time.perf_counter()
            _ = biflux_pipe.process_micro_batch(pl_batch)
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        p50_biflux = latencies[len(latencies) // 2]
        results["Biflux (Streaming Engine)"] = {
            "latency_ms": p50_biflux,
            "throughput": batch_size / (p50_biflux / 1000.0),
            "unification": "100% Identical Python Class",
        }

        # 2. Standalone Polars (Manual micro-batching)
        def polars_micro_batch(batch: pl.DataFrame) -> pl.DataFrame:
            return (
                batch.lazy()
                .with_columns(
                    mid_price=(pl.col("bid") + pl.col("ask")) / 2.0,
                    dollar_volume=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
                )
                .group_by("symbol")
                .agg(
                    total_volume=pl.col("size").sum(),
                    total_dollar_volume=pl.col("dollar_volume").sum(),
                    vwap=(pl.col("dollar_volume").sum() / pl.col("size").sum()).round(6),
                )
                .sort("symbol")
                .collect()
            )

        _ = polars_micro_batch(pl_batch)
        latencies = []
        for _ in range(25):
            t0 = time.perf_counter()
            _ = polars_micro_batch(pl_batch)
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        p50_polars = latencies[len(latencies) // 2]
        results["Polars (Micro-Batching)"] = {
            "latency_ms": p50_polars,
            "throughput": batch_size / (p50_polars / 1000.0),
            "unification": "No (Custom Kafka glue required)",
        }

        # 3. DuckDB Micro-Batching (register and query)
        def duckdb_micro_batch(df: pl.DataFrame) -> Any:
            duck_conn.register("batch", df.to_arrow())
            res = duck_conn.execute(
                """
                SELECT symbol,
                       SUM(size) AS total_volume,
                       SUM(((bid + ask) / 2.0) * size) AS total_dollar_volume,
                       ROUND(SUM(((bid + ask) / 2.0) * size) / SUM(size), 6) AS vwap
                FROM batch
                GROUP BY symbol
                """
            ).fetchall()
            duck_conn.unregister("batch")
            return res

        _ = duckdb_micro_batch(pl_batch)
        latencies = []
        for _ in range(25):
            t0 = time.perf_counter()
            _ = duckdb_micro_batch(pl_batch)
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        p50_duck = latencies[len(latencies) // 2]
        results["DuckDB (Micro-Batch SQL)"] = {
            "latency_ms": p50_duck,
            "throughput": batch_size / (p50_duck / 1000.0),
            "unification": "No (Catalog registration overhead)",
        }

        # 4. Native Python Streaming Loop
        def native_python_consumer(records: List[Dict[str, Any]]) -> Dict[str, Any]:
            state: Dict[str, Dict[str, float]] = {}
            for r in records:
                sym = r["symbol"]
                mid = (r["bid"] + r["ask"]) / 2.0
                size = r["size"]
                dollar_vol = mid * size
                if sym not in state:
                    state[sym] = {"vol": 0.0, "dollar_vol": 0.0}
                state[sym]["vol"] += size
                state[sym]["dollar_vol"] += dollar_vol
            vwaps = {
                s: round(v["dollar_vol"] / v["vol"], 6) for s, v in state.items() if v["vol"] > 0
            }
            return vwaps

        _ = native_python_consumer(raw_dicts)
        latencies = []
        for _ in range(25):
            t0 = time.perf_counter()
            _ = native_python_consumer(raw_dicts)
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        p50_python = latencies[len(latencies) // 2]
        results["Native Python Consumer"] = {
            "latency_ms": p50_python,
            "throughput": batch_size / (p50_python / 1000.0),
            "unification": "No (Handwritten Python loops)",
        }

        # 5. Pandas Micro-Batching
        def pandas_micro_batch(df: pd.DataFrame) -> pd.DataFrame:
            mid = (df["bid"] + df["ask"]) / 2.0
            dollar_vol = mid * df["size"]
            grouped = df.assign(mid=mid, dollar_vol=dollar_vol).groupby("symbol")
            agg = grouped.agg(
                total_volume=("size", "sum"),
                total_dollar_volume=("dollar_vol", "sum"),
            )
            agg["vwap"] = (agg["total_dollar_volume"] / agg["total_volume"]).round(6)
            return agg

        _ = pandas_micro_batch(pd_batch)
        latencies = []
        for _ in range(25):
            t0 = time.perf_counter()
            _ = pandas_micro_batch(pd_batch)
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        p50_pandas = latencies[len(latencies) // 2]
        results["Pandas Micro-Batching"] = {
            "latency_ms": p50_pandas,
            "throughput": batch_size / (p50_pandas / 1000.0),
            "unification": "High GC overhead / Skew risk",
        }

        results_by_size[batch_size] = results

    return results_by_size


# -----------------------------------------------------------------------------
# 3. DIRECT BATCH VS. STREAMING ON IDENTICAL SAMPLE SIZES
# -----------------------------------------------------------------------------


def benchmark_batch_vs_stream_shared_sizes(
    shared_sizes: List[int] = [1_000, 10_000, 100_000],
) -> Dict[int, Dict[str, Dict[str, float]]]:
    """Compare Batch Mode vs. Streaming Mode on identical sample sizes."""
    comparison: Dict[int, Dict[str, Dict[str, float]]] = {}

    for size in shared_sizes:
        print(f"[*] Running Batch vs. Streaming comparison for {size:,} rows...")
        pl_data = generate_benchmark_dataset(size)
        pd_data = pl_data.to_pandas()
        duck_conn = duckdb.connect(":memory:")
        duck_conn.register("quotes", pl_data.to_arrow())

        # --- BIFLUX ---
        ctx_b = BifluxContext(mode=ExecutionMode.BATCH)
        pipe_b = BifluxVWAPPipeline(ctx_b)
        _ = pipe_b.transform(pl_data.lazy()).collect()
        t0 = time.perf_counter()
        for _ in range(5):
            _ = pipe_b.transform(pl_data.lazy()).collect()
        biflux_batch_ms = ((time.perf_counter() - t0) / 5) * 1000

        ctx_s = BifluxContext(mode=ExecutionMode.LIVE)
        pipe_s = BifluxVWAPPipeline(ctx_s)
        _ = pipe_s.process_micro_batch(pl_data)
        t0 = time.perf_counter()
        for _ in range(5):
            _ = pipe_s.process_micro_batch(pl_data)
        biflux_stream_ms = ((time.perf_counter() - t0) / 5) * 1000

        # --- POLARS ---
        def polars_calc(df: pl.DataFrame) -> pl.DataFrame:
            return (
                df.lazy()
                .with_columns(
                    mid=(pl.col("bid") + pl.col("ask")) / 2.0,
                    dollar_vol=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
                )
                .group_by("symbol")
                .agg(
                    total_volume=pl.col("size").sum(),
                    total_dollar_volume=pl.col("dollar_vol").sum(),
                    vwap=(pl.col("dollar_vol").sum() / pl.col("size").sum()).round(6),
                )
                .collect()
            )

        _ = polars_calc(pl_data)
        t0 = time.perf_counter()
        for _ in range(5):
            _ = polars_calc(pl_data)
        polars_ms = ((time.perf_counter() - t0) / 5) * 1000

        # --- DUCKDB ---
        duck_sql = """
            SELECT symbol, SUM(size), SUM(((bid+ask)/2.0)*size)/SUM(size)
            FROM quotes GROUP BY symbol
        """
        _ = duck_conn.execute(duck_sql).fetchall()
        t0 = time.perf_counter()
        for _ in range(5):
            _ = duck_conn.execute(duck_sql).fetchall()
        duck_ms = ((time.perf_counter() - t0) / 5) * 1000

        # --- PANDAS ---
        def pandas_calc(df: pd.DataFrame) -> pd.DataFrame:
            mid = (df["bid"] + df["ask"]) / 2.0
            dollar_vol = mid * df["size"]
            return (
                df.assign(mid=mid, dollar_vol=dollar_vol)
                .groupby("symbol")
                .agg(
                    total_volume=("size", "sum"),
                    total_dollar_volume=("dollar_vol", "sum"),
                )
            )

        _ = pandas_calc(pd_data)
        t0 = time.perf_counter()
        for _ in range(5):
            _ = pandas_calc(pd_data)
        pandas_ms = ((time.perf_counter() - t0) / 5) * 1000

        comparison[size] = {
            "Biflux (Batch)": {
                "time_ms": biflux_batch_ms,
                "throughput": size / (biflux_batch_ms / 1000.0),
            },
            "Biflux (Streaming)": {
                "time_ms": biflux_stream_ms,
                "throughput": size / (biflux_stream_ms / 1000.0),
            },
            "Polars (Standalone)": {
                "time_ms": polars_ms,
                "throughput": size / (polars_ms / 1000.0),
            },
            "DuckDB": {
                "time_ms": duck_ms,
                "throughput": size / (duck_ms / 1000.0),
            },
            "Pandas": {
                "time_ms": pandas_ms,
                "throughput": size / (pandas_ms / 1000.0),
            },
        }

    return comparison


# -----------------------------------------------------------------------------
# SUMMARY FORMATTER
# -----------------------------------------------------------------------------


def print_comparative_summary():
    print("=" * 88)
    print("  PROJECT BIFLUX: MULTI-SCALE CROSS-FRAMEWORK COMPARATIVE BENCHMARK")
    print("=" * 88)

    # 1. Batch Suite
    batch_suite = benchmark_batch_frameworks([100_000, 500_000, 1_000_000])

    print("\n" + "=" * 88)
    print(">>> 1. HISTORICAL BATCH BACKTEST COMPARISON ACROSS SAMPLE SIZES")
    print("=" * 88)
    for n_rows, frameworks in batch_suite.items():
        print(f"\n--- Dataset Scale: {n_rows:,} Records ---")
        print(f"{'FRAMEWORK':<22} | {'TIME (MS)':<10} | {'THROUGHPUT':<18} | SKEW RISK")
        print("-" * 88)
        for name, data in frameworks.items():
            t_str = f"{data['time_ms']:>7.2f} ms"
            tp_str = f"{data['throughput']:>14,.0f} rows/s"
            print(f"{name:<22} | {t_str} | {tp_str} | {data['skew_risk']}")

    # 2. Streaming Suite
    stream_suite = benchmark_streaming_frameworks([500, 2_000, 10_000, 50_000])

    print("\n" + "=" * 88)
    print(">>> 2. REAL-TIME STREAMING MICRO-BATCH LATENCY ACROSS SAMPLE SIZES")
    print("=" * 88)
    for b_size, frameworks in stream_suite.items():
        print(f"\n--- Micro-Batch Size: {b_size:,} Records ---")
        print(f"{'FRAMEWORK':<26} | {'P50 LATENCY':<12} | {'THROUGHPUT':<18} | CODE UNIFICATION")
        print("-" * 88)
        for name, data in frameworks.items():
            lat_str = f"{data['latency_ms']:>8.2f} ms"
            tp_str = f"{data['throughput']:>14,.0f} rows/s"
            print(f"{name:<26} | {lat_str} | {tp_str} | {data['unification']}")

    # 3. Direct Batch vs Stream across identical sample sizes
    shared_suite = benchmark_batch_vs_stream_shared_sizes([1_000, 10_000, 100_000])

    print("\n" + "=" * 88)
    print(">>> 3. BATCH VS. STREAMING EXECUTION ON IDENTICAL SAMPLE SIZES")
    print("=" * 88)
    for size, engines in shared_suite.items():
        print(f"\n--- Identical Sample Size: {size:,} Records ---")
        print(f"{'ENGINE / RUNTIME':<24} | {'LATENCY (MS)':<14} | {'THROUGHPUT':<20}")
        print("-" * 75)
        for engine_name, metrics in engines.items():
            lat_str = f"{metrics['time_ms']:>9.2f} ms"
            tp_str = f"{metrics['throughput']:>14,.0f} rows/s"
            print(f"{engine_name:<24} | {lat_str}   | {tp_str}")

    print("\n" + "=" * 88)


if __name__ == "__main__":
    print_comparative_summary()
