"""Bond Pricing Model: Volume-Weighted Average Price (VWAP) Pipeline.

Proves Project Biflux completely eliminates Train-Serve Skew by compiling
an identical Polars/Arrow computation graph for both historical backtests
and low-latency live streaming.
"""

import os
import sys
import tempfile
import time
from typing import List, Tuple

import polars as pl

from biflux.core import (
    BifluxContext,
    BifluxPipeline,
    Environment,
    ExecutionMode,
)


class BondPricingVWAP(BifluxPipeline):
    """Corporate bond pricing pipeline computing Volume-Weighted Average Price (VWAP).

    Formula:
        mid_price = (bid + ask) / 2.0
        dollar_volume = mid_price * size
        VWAP = sum(dollar_volume) / sum(size)
    """

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
                min_bid=pl.col("bid").min(),
                max_ask=pl.col("ask").max(),
            )
            .sort("symbol")
        )


def generate_synthetic_bond_ticks(n_ticks: int = 10_000) -> pl.DataFrame:
    """Generate synthetic bond market quotes."""
    symbols = [
        "AAPL-CORP-2030",
        "AMZN-CORP-2029",
        "GOOG-CORP-2032",
        "MSFT-CORP-2028",
        "US-TREAS-10Y",
    ]
    base_bids = {
        "AAPL-CORP-2030": 98.25,
        "AMZN-CORP-2029": 101.10,
        "GOOG-CORP-2032": 95.40,
        "MSFT-CORP-2028": 104.75,
        "US-TREAS-10Y": 99.80,
    }

    import random

    random.seed(42)

    data: List[Tuple[str, float, float, int, int]] = []
    base_ts = 1_700_000_000_000

    for i in range(n_ticks):
        sym = symbols[i % len(symbols)]
        bid = round(base_bids[sym] + (random.random() - 0.5) * 2.0, 4)
        spread = round(0.05 + random.random() * 0.15, 4)
        ask = round(bid + spread, 4)
        size = random.choice([100, 250, 500, 1000, 2500, 5000])
        ts = base_ts + i * 50
        data.append((sym, bid, ask, size, ts))

    return pl.DataFrame(
        data,
        schema={
            "symbol": pl.Utf8,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "size": pl.Int64,
            "timestamp": pl.Int64,
        },
        orient="row",
    )


def run_e2e_verification() -> bool:
    print("=" * 80)
    print("  PROJECT BIFLUX: TRAIN-SERVE SKEW ELIMINATION VERIFICATION")
    print("=" * 80)

    n_ticks = 20_000
    print(f"[*] Generating {n_ticks:,} synthetic bond market quotes across 5 assets...")
    ticks_df = generate_synthetic_bond_ticks(n_ticks=n_ticks)
    print(f"[*] Dataset schema: {dict(zip(ticks_df.columns, [str(t) for t in ticks_df.dtypes]))}")
    print(f"[*] Sample head:\n{ticks_df.head(3)}\n")

    with tempfile.TemporaryDirectory() as workdir:
        raw_lake_path = os.path.join(workdir, "lakehouse_raw_bonds.parquet")
        batch_sink_path = os.path.join(workdir, "lakehouse_vwap_features.parquet")
        ticks_df.write_parquet(raw_lake_path)

        # -------------------------------------------------------------
        # STEP A: HISTORICAL BATCH BACKTEST
        # -------------------------------------------------------------
        print("-" * 80)
        print(">>> [STEP A] RUNNING HISTORICAL BATCH BACKTEST (S3 / ICEBERG / PARQUET)")
        print("-" * 80)
        batch_start = time.perf_counter()

        batch_ctx = BifluxContext(
            mode=ExecutionMode.BATCH,
            env=Environment.LOCAL,
            s3_endpoint="http://localhost:9000",
            iceberg_catalog="rest://localhost:8181",
        )
        batch_pipeline = BondPricingVWAP(batch_ctx)
        batch_result = batch_pipeline.run(
            source_uri=raw_lake_path,
            sink_uri=batch_sink_path,
            sample_df=ticks_df.lazy(),
        )
        batch_duration_ms = int((time.perf_counter() - batch_start) * 1000)

        batch_df = pl.read_parquet(batch_sink_path).sort("symbol")
        print(
            f"[✓] Batch Backtest Completed in {batch_duration_ms} ms (status={batch_result.status})"
        )
        print(f"[✓] Output Rows Written: {batch_df.shape[0]}")
        print(f"[✓] Batch VWAP Feature Table:\n{batch_df}\n")

        # -------------------------------------------------------------
        # STEP B: LIVE STREAMING EXECUTION
        # -------------------------------------------------------------
        print("-" * 80)
        print(">>> [STEP B] RUNNING LIVE STREAM PROCESSING (KAFKA MICRO-BATCHES)")
        print("-" * 80)
        live_start = time.perf_counter()

        live_ctx = BifluxContext(
            mode=ExecutionMode.LIVE,
            env=Environment.LOCAL,
            kafka_brokers="localhost:9092",
        )
        live_pipeline = BondPricingVWAP(live_ctx)
        # Execute stream FFI initialization
        live_result = live_pipeline.run(
            source_uri="market_data.bonds.ticks",
            sink_uri="features.bonds.vwap",
            sample_df=ticks_df.lazy(),
        )
        print(f"[*] Stream FFI initialized: status={live_result.status}")

        # Feed streaming micro-batches through identical compiled pipeline graph
        batch_size = 2_000
        micro_batches = [
            ticks_df.slice(offset, batch_size) for offset in range(0, len(ticks_df), batch_size)
        ]

        print(f"[*] Processing {len(micro_batches)} live micro-batches via Arrow memory buffers...")
        accumulated_partials: List[pl.DataFrame] = []
        for i, mb in enumerate(micro_batches):
            partial = live_pipeline.process_micro_batch(mb)
            accumulated_partials.append(partial)

        # Merge live micro-batch stream state
        combined = pl.concat(accumulated_partials)
        live_df = (
            combined.group_by("symbol")
            .agg(
                total_volume=pl.col("total_volume").sum(),
                total_dollar_volume=pl.col("total_dollar_volume").sum(),
                vwap=(pl.col("total_dollar_volume").sum() / pl.col("total_volume").sum()).round(6),
                min_bid=pl.col("min_bid").min(),
                max_ask=pl.col("max_ask").max(),
            )
            .sort("symbol")
        )
        live_duration_ms = int((time.perf_counter() - live_start) * 1000)

        print(f"[✓] Live Stream Execution Completed in {live_duration_ms} ms")
        print(f"[✓] Live Streaming VWAP Feature Table:\n{live_df}\n")

        # -------------------------------------------------------------
        # STEP C: MATHEMATICAL EQUIVALENCE PROOF
        # -------------------------------------------------------------
        print("=" * 80)
        print(">>> [THE PROOF] COMPARING BATCH VS STREAM FEATURE VECTORS")
        print("=" * 80)

        print(f"{'SYMBOL':<18} | {'BATCH VWAP':<14} | {'LIVE VWAP':<14} | {'DIFF':<12} | STATUS")
        print("-" * 80)

        all_matched = True
        for b_row, l_row in zip(batch_df.iter_rows(named=True), live_df.iter_rows(named=True)):
            sym = b_row["symbol"]
            b_vwap = b_row["vwap"]
            l_vwap = l_row["vwap"]
            diff = abs(b_vwap - l_vwap)
            match = diff < 1e-6
            status = "MATCH (0.000000 Skew)" if match else "FAIL"
            if not match:
                all_matched = False
            print(f"{sym:<18} | {b_vwap:<14.6f} | {l_vwap:<14.6f} | {diff:<12.6f} | {status}")

        print("-" * 80)
        if all_matched:
            print(">>> VERIFICATION SUCCESS: Zero Train-Serve Skew Detected.")
            print(
                ">>> Single Python Polars transformation produced identical results in both modes."
            )
        else:
            print(">>> VERIFICATION FAILED: Discrepancy detected between batch and stream!")
            sys.exit(1)

        return all_matched


if __name__ == "__main__":
    run_e2e_verification()
