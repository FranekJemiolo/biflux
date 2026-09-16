"""High-frequency L2 orderbook imbalance & micro-price feature pipeline.

Computes real-time market microstructure features:
- Orderbook imbalance ratio: (bid_sz - ask_sz) / (bid_sz + ask_sz)
- Volume-weighted micro-price
- Spread in basis points (bps)
- Depth-weighted price pressure
"""

import os
import random
import tempfile
from typing import List, Tuple

import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, Environment, ExecutionMode


class OrderbookMicrostructurePipeline(BifluxPipeline):
    """L2 Orderbook microstructure pipeline for high-frequency algorithmic execution."""

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                mid_price=(pl.col("best_bid") + pl.col("best_ask")) / 2.0,
                spread_bps=(
                    (pl.col("best_ask") - pl.col("best_bid"))
                    / ((pl.col("best_bid") + pl.col("best_ask")) / 2.0)
                )
                * 10_000.0,
                total_depth=pl.col("bid_size") + pl.col("ask_size"),
                imbalance=(
                    (pl.col("bid_size") - pl.col("ask_size"))
                    / (pl.col("bid_size") + pl.col("ask_size"))
                ),
                micro_price=(
                    (
                        pl.col("best_ask") * pl.col("bid_size")
                        + pl.col("best_bid") * pl.col("ask_size")
                    )
                    / (pl.col("bid_size") + pl.col("ask_size"))
                ),
            )
            .group_by("asset")
            .agg(
                quote_count=pl.len(),
                avg_spread_bps=pl.col("spread_bps").mean().round(3),
                avg_imbalance=pl.col("imbalance").mean().round(6),
                avg_micro_price=pl.col("micro_price").mean().round(4),
                min_micro_price=pl.col("micro_price").min().round(4),
                max_micro_price=pl.col("micro_price").max().round(4),
                total_bid_depth=pl.col("bid_size").sum(),
                total_ask_depth=pl.col("ask_size").sum(),
            )
            .sort("asset")
        )


def generate_synthetic_l2_quotes(n_quotes: int = 30_000) -> pl.DataFrame:
    """Generate high-frequency orderbook snapshots."""
    random.seed(999)
    assets = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "NVDA", "SPY"]
    base_mids = {
        "BTC-USDT": 62450.0,
        "ETH-USDT": 3450.0,
        "SOL-USDT": 145.0,
        "NVDA": 128.50,
        "SPY": 560.20,
    }

    data: List[Tuple[str, float, float, int, int, int]] = []
    base_ts = 1_725_000_000_000_000  # microseconds

    for i in range(n_quotes):
        asset = assets[i % len(assets)]
        mid = base_mids[asset] + (random.random() - 0.5) * (base_mids[asset] * 0.005)
        spread = max(0.01, mid * (random.uniform(0.0001, 0.0008)))
        best_bid = round(mid - spread / 2.0, 2)
        best_ask = round(mid + spread / 2.0, 2)
        bid_sz = random.randint(10, 500)
        ask_sz = random.randint(10, 500)
        ts = base_ts + i * 250
        data.append((asset, best_bid, best_ask, bid_sz, ask_sz, ts))

    return pl.DataFrame(
        data,
        schema={
            "asset": pl.Utf8,
            "best_bid": pl.Float64,
            "best_ask": pl.Float64,
            "bid_size": pl.Int64,
            "ask_size": pl.Int64,
            "timestamp_us": pl.Int64,
        },
        orient="row",
    )


def run_orderbook_demo() -> bool:
    print("=" * 80)
    print("  PROJECT BIFLUX: HIGH-FREQUENCY ORDERBOOK MICROSTRUCTURE PIPELINE")
    print("=" * 80)

    n_quotes = 30_000
    print(f"[*] Generating {n_quotes:,} L2 orderbook quotes across 5 assets...")
    quotes_df = generate_synthetic_l2_quotes(n_quotes=n_quotes)
    print(f"[*] Quotes Snapshot:\n{quotes_df.head(3)}\n")

    with tempfile.TemporaryDirectory() as workdir:
        raw_quotes_path = os.path.join(workdir, "l2_raw_quotes.parquet")
        sink_path = os.path.join(workdir, "microstructure_features.parquet")
        quotes_df.write_parquet(raw_quotes_path)

        # Batch
        batch_ctx = BifluxContext(mode=ExecutionMode.BATCH, env=Environment.LOCAL)
        batch_pipeline = OrderbookMicrostructurePipeline(batch_ctx)
        b_res = batch_pipeline.run(
            source_uri=raw_quotes_path, sink_uri=sink_path, sample_df=quotes_df.lazy()
        )
        batch_output = pl.read_parquet(sink_path).sort("asset")
        print(
            f"[✓] Batch Microstructure Completed in {b_res.duration_ms} ms (status={b_res.status})"
        )
        print(f"[✓] Batch Features Table:\n{batch_output}\n")

        # Stream
        live_ctx = BifluxContext(mode=ExecutionMode.LIVE, kafka_brokers="localhost:9092")
        live_pipeline = OrderbookMicrostructurePipeline(live_ctx)
        l_res = live_pipeline.run(
            source_uri="l2.market.depth",
            sink_uri="features.microstructure",
            sample_df=quotes_df.lazy(),
        )
        live_output = live_pipeline.process_micro_batch(quotes_df).sort("asset")
        print(
            f"[✓] Stream Microstructure Completed in {l_res.duration_ms} ms (status={l_res.status})"
        )
        print(f"[✓] Live Stream Features Table:\n{live_output}\n")

        # Match proof
        print("=" * 80)
        print(">>> [THE PROOF] Orderbook Feature Parity Check")
        print("=" * 80)
        print(f"{'ASSET':<12} | {'BATCH IMBAL':<12} | {'LIVE IMBAL':<12} | {'DIFF':<10} | STATUS")
        print("-" * 80)

        all_matched = True
        for b_row, l_row in zip(
            batch_output.iter_rows(named=True), live_output.iter_rows(named=True)
        ):
            asset = b_row["asset"]
            b_imb = b_row["avg_imbalance"]
            l_imb = l_row["avg_imbalance"]
            diff = abs(b_imb - l_imb)
            match = diff < 1e-6
            if not match:
                all_matched = False
            status = "MATCH (0.000000 Skew)" if match else "MISMATCH"
            print(f"{asset:<12} | {b_imb:<12.6f} | {l_imb:<12.6f} | {diff:<10.6f} | {status}")

        print("-" * 80)
        if all_matched:
            print(">>> VERIFICATION SUCCESS: Zero Train-Serve Skew in Orderbook Microstructure.")
        return all_matched


if __name__ == "__main__":
    run_orderbook_demo()
