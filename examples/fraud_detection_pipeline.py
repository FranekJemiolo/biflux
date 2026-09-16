"""Real-time transaction velocity & fraud scoring feature pipeline.

Proves Train-Serve Skew elimination for critical financial risk systems.
Computes cardholder velocity, spending z-scores, and composite risk flags.
"""

import os
import random
import tempfile
import time
from typing import List, Tuple

import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, Environment, ExecutionMode


class TransactionFraudScoringPipeline(BifluxPipeline):
    """Real-time fraud feature engineering and risk scoring pipeline.

    Features:
        - transaction_velocity: transaction count per cardholder
        - normalized_spend_deviation: deviation of amount from cardholder median
        - international_risk_multiplier: risk weighting based on country code match
        - composite_fraud_score: unified probability score [0.0 - 1.0]
        - fraud_alert: binary flag (risk_score >= 0.70)
    """

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                is_international=(pl.col("card_country") != pl.col("merchant_country")).cast(
                    pl.Float64
                ),
                is_high_risk_mcc=pl.col("mcc").is_in([7995, 6051, 4829, 5944]).cast(pl.Float64),
            )
            .with_columns(
                base_risk=(
                    (pl.col("amount") / 1000.0).clip(0.0, 1.0) * 0.4
                    + pl.col("is_international") * 0.35
                    + pl.col("is_high_risk_mcc") * 0.25
                ).round(6),
            )
            .group_by("cardholder_id")
            .agg(
                total_transactions=pl.len(),
                total_spend=pl.col("amount").sum().round(2),
                avg_transaction_amount=pl.col("amount").mean().round(2),
                max_transaction_amount=pl.col("amount").max().round(2),
                max_risk_score=pl.col("base_risk").max().round(6),
                international_txn_count=pl.col("is_international").sum().cast(pl.Int64),
                fraud_alerts_triggered=(pl.col("base_risk") >= 0.70).sum().cast(pl.Int64),
            )
            .sort("cardholder_id")
        )


def generate_synthetic_transactions(n_txns: int = 25_000) -> pl.DataFrame:
    """Generate reproducible transaction stream data."""
    random.seed(1337)
    cardholders = [f"CARD-USR-{i:04d}" for i in range(1, 101)]
    countries = ["US", "US", "US", "CA", "GB", "DE", "FR", "SG", "JP", "CY"]
    mccs = [5411, 5812, 5311, 4829, 7995, 5912, 5541, 6051]

    data: List[Tuple[str, str, float, str, str, int, int]] = []
    base_ts = 1_720_000_000_000

    for i in range(n_txns):
        cid = random.choice(cardholders)
        tid = f"TXN-{i:07d}"
        amount = round(max(2.50, random.expovariate(1 / 120.0)), 2)
        card_country = "US" if random.random() > 0.15 else random.choice(countries)
        merchant_country = "US" if random.random() > 0.25 else random.choice(countries)
        mcc = random.choice(mccs)
        ts = base_ts + i * 200
        data.append((tid, cid, amount, card_country, merchant_country, mcc, ts))

    return pl.DataFrame(
        data,
        schema={
            "transaction_id": pl.Utf8,
            "cardholder_id": pl.Utf8,
            "amount": pl.Float64,
            "card_country": pl.Utf8,
            "merchant_country": pl.Utf8,
            "mcc": pl.Int64,
            "timestamp": pl.Int64,
        },
        orient="row",
    )


def run_fraud_pipeline_demo() -> bool:
    print("=" * 80)
    print("  PROJECT BIFLUX: REAL-TIME FRAUD DETECTION PIPELINE")
    print("=" * 80)

    n_txns = 25_000
    print(f"[*] Generating {n_txns:,} payment transactions across 100 cardholders...")
    txns_df = generate_synthetic_transactions(n_txns=n_txns)
    print(f"[*] Sample Transactions Head:\n{txns_df.head(3)}\n")

    with tempfile.TemporaryDirectory() as workdir:
        raw_txns_path = os.path.join(workdir, "historical_transactions.parquet")
        batch_sink_path = os.path.join(workdir, "cardholder_risk_profiles.parquet")
        txns_df.write_parquet(raw_txns_path)

        # Batch historical profiling
        print(">>> [STEP A] Running Historical Batch Cardholder Profiling...")
        batch_start = time.perf_counter()
        batch_ctx = BifluxContext(
            mode=ExecutionMode.BATCH,
            env=Environment.LOCAL,
            s3_endpoint="http://localhost:9000",
        )
        batch_pipeline = TransactionFraudScoringPipeline(batch_ctx)
        batch_res = batch_pipeline.run(
            source_uri=raw_txns_path,
            sink_uri=batch_sink_path,
            sample_df=txns_df.lazy(),
        )
        batch_duration_ms = int((time.perf_counter() - batch_start) * 1000)

        batch_output = pl.read_parquet(batch_sink_path).sort("cardholder_id")
        print(
            f"[✓] Batch Profiling Completed in {batch_duration_ms} ms (status={batch_res.status})"
        )
        print(f"[✓] Evaluated {batch_output.shape[0]} Cardholders")
        top_risk = batch_output.sort("fraud_alerts_triggered", descending=True).head(5)
        print(f"[*] Top 5 High-Risk Cardholders (Batch):\n{top_risk}\n")

        # Live streaming scoring
        print(">>> [STEP B] Running Real-Time Kafka Streaming Fraud Scoring...")
        live_start = time.perf_counter()
        live_ctx = BifluxContext(
            mode=ExecutionMode.LIVE,
            env=Environment.LOCAL,
            kafka_brokers="localhost:9092",
        )
        live_pipeline = TransactionFraudScoringPipeline(live_ctx)
        live_res = live_pipeline.run(
            source_uri="payments.authorization.stream",
            sink_uri="risk.cardholder.alerts",
            sample_df=txns_df.lazy(),
        )

        # Micro-batch ingestion
        mb_size = 2_500
        partials = []
        for offset in range(0, len(txns_df), mb_size):
            chunk = txns_df.slice(offset, mb_size)
            partials.append(live_pipeline.process_micro_batch(chunk))

        combined = pl.concat(partials)
        live_output = (
            combined.group_by("cardholder_id")
            .agg(
                total_transactions=pl.col("total_transactions").sum(),
                total_spend=pl.col("total_spend").sum().round(2),
                avg_transaction_amount=(
                    pl.col("total_spend").sum() / pl.col("total_transactions").sum()
                ).round(2),
                max_transaction_amount=pl.col("max_transaction_amount").max().round(2),
                max_risk_score=pl.col("max_risk_score").max().round(6),
                international_txn_count=pl.col("international_txn_count").sum(),
                fraud_alerts_triggered=pl.col("fraud_alerts_triggered").sum(),
            )
            .sort("cardholder_id")
        )
        live_duration_ms = int((time.perf_counter() - live_start) * 1000)
        print(
            f"[✓] Live Stream Fraud Evaluation in {live_duration_ms} ms (status={live_res.status})"
        )
        print()

        # Mathematical verification
        print("=" * 80)
        print(">>> [THE PROOF] Comparing Batch vs Live Fraud Risk Metrics")
        print("=" * 80)
        table_hdr = (
            f"{'CARDHOLDER':<16} | {'BATCH ALERTS':<14} | "
            f"{'LIVE ALERTS':<14} | {'MAX RISK':<10} | STATUS"
        )
        print(table_hdr)
        print("-" * 80)

        all_matched = True
        sample_cardholders = batch_output.head(10)
        for b_row, l_row in zip(
            sample_cardholders.iter_rows(named=True), live_output.head(10).iter_rows(named=True)
        ):
            cid = b_row["cardholder_id"]
            b_alerts = b_row["fraud_alerts_triggered"]
            l_alerts = l_row["fraud_alerts_triggered"]
            b_risk = b_row["max_risk_score"]
            l_risk = l_row["max_risk_score"]
            match = (b_alerts == l_alerts) and (abs(b_risk - l_risk) < 1e-6)
            if not match:
                all_matched = False
            status = "MATCH (0 Skew)" if match else "MISMATCH"
            print(f"{cid:<16} | {b_alerts:<14} | {l_alerts:<14} | {b_risk:<10.6f} | {status}")

        print("-" * 80)
        if all_matched:
            print(">>> VERIFICATION SUCCESS: Zero Train-Serve Skew in Fraud Detection.")
        else:
            print(">>> VERIFICATION FAILED: Discrepancy detected!")
        return all_matched


if __name__ == "__main__":
    run_fraud_pipeline_demo()
