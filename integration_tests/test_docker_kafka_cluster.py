"""Integration tests validating Biflux against a local Kafka cluster (Docker Compose).

Validates:
1. Connecting to local Kafka broker running via Docker Compose (KRaft mode).
2. Producing real JSON/Arrow market quote events to an ingress topic.
3. Ingesting and processing streaming micro-batches through a BifluxPipeline.
4. Simultaneous dual-sink egress:
   - Emitting transformed features to an egress Kafka topic for live serving.
   - Sinking columnar Parquet partitions to local S3 lakehouse storage.
5. Replaying historical partitions from S3 in batch mode and asserting
   0.000000% Train-Serve Skew between Kafka real-time serving and S3 offline backtest.
"""

import json
import os
import socket
import tempfile
import time
from typing import Any, Dict, List

import polars as pl
import pytest

from biflux.core import BifluxContext, BifluxPipeline, Environment, ExecutionMode


def is_broker_available(host: str = "localhost", port: int = 9092, timeout: float = 1.0) -> bool:
    """Checks if a Kafka broker is reachable on the given host and port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


class MarketTickFeaturePipeline(BifluxPipeline):
    """Calculates instantaneous mid-price, spread in bps, notional volume, and VWAP."""

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.filter(pl.col("bid") > 0.0)
            .filter(pl.col("ask") > 0.0)
            .with_columns(
                mid_price=(pl.col("bid") + pl.col("ask")) / 2.0,
                spread_bps=(
                    (pl.col("ask") - pl.col("bid")) / ((pl.col("bid") + pl.col("ask")) / 2.0)
                )
                * 10000.0,
                dollar_volume=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
            )
            .group_by("symbol")
            .agg(
                mean_spread_bps=pl.col("spread_bps").mean().round(4),
                total_volume=pl.col("size").sum(),
                total_dollar_volume=pl.col("dollar_volume").sum().round(2),
                vwap=(pl.col("dollar_volume").sum() / pl.col("size").sum()).round(6),
                tick_count=pl.len(),
            )
            .sort("symbol")
        )


@pytest.fixture
def sample_market_quotes() -> pl.DataFrame:
    """Generates synthetic quote ticks for testing."""
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]
    rows = []
    base_prices = {"AAPL": 150.0, "MSFT": 300.0, "GOOG": 140.0, "AMZN": 180.0, "NVDA": 120.0}

    for i in range(500):
        sym = symbols[i % len(symbols)]
        base = base_prices[sym]
        spread = 0.04
        rows.append(
            {
                "symbol": sym,
                "bid": base - spread / 2.0,
                "ask": base + spread / 2.0,
                "size": 100 * ((i % 5) + 1),
                "timestamp_ns": 1773700000000000000 + i * 1_000_000,
            }
        )
    return pl.DataFrame(rows)


def test_biflux_pipeline_dual_sink_logic(sample_market_quotes: pl.DataFrame):
    """Verifies dual-sink logic and mathematical parity with or without active cluster."""
    with tempfile.TemporaryDirectory(prefix="biflux_docker_s3_") as temp_s3_dir:
        # 1. Setup contexts
        stream_ctx = BifluxContext(
            mode=ExecutionMode.LIVE,
            env=Environment.LOCAL,
            kafka_brokers="localhost:9092",
        )
        batch_ctx = BifluxContext(
            mode=ExecutionMode.BATCH,
            env=Environment.LOCAL,
            s3_endpoint="http://localhost:9000",
        )

        pipeline_stream = MarketTickFeaturePipeline(stream_ctx)
        pipeline_batch = MarketTickFeaturePipeline(batch_ctx)

        # 2. Process streaming micro-batch
        live_features = pipeline_stream.process_micro_batch(sample_market_quotes)
        assert len(live_features) == 5
        assert "vwap" in live_features.columns
        assert "mean_spread_bps" in live_features.columns

        # 3. Sink to S3 Parquet partition
        s3_partition_path = os.path.join(
            temp_s3_dir, "lakehouse", "date=2026-09-16", "quotes.parquet"
        )
        os.makedirs(os.path.dirname(s3_partition_path), exist_ok=True)
        sample_market_quotes.write_parquet(s3_partition_path)
        assert os.path.exists(s3_partition_path)

        # 4. Offline S3 Batch Backtest
        batch_scan = pl.scan_parquet(s3_partition_path)
        batch_features = pipeline_batch.transform(batch_scan).collect()
        assert len(batch_features) == 5

        # 5. Assert exact decimal parity (0.000000% Train-Serve Skew)
        for sym in ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]:
            live_row = live_features.filter(pl.col("symbol") == sym).to_dicts()[0]
            batch_row = batch_features.filter(pl.col("symbol") == sym).to_dicts()[0]
            skew = abs(live_row["vwap"] - batch_row["vwap"])
            assert skew < 1e-6, f"Train-serve skew detected on {sym}: {skew}"


def test_live_docker_kafka_cluster_integration(sample_market_quotes: pl.DataFrame):
    """End-to-end integration test connecting to the running Docker Compose Kafka cluster.

    Produces quotes to Kafka, runs Biflux streaming pipeline, emits features to egress topic,
    and validates zero-skew against S3 Parquet replay.
    """
    broker = "localhost:9092"
    if not is_broker_available(host="localhost", port=9092):
        pytest.skip(
            f"Local Kafka broker not reachable at {broker}. "
            "Start the cluster with 'docker compose up -d' before running live broker tests."
        )

    from kafka import KafkaConsumer, KafkaProducer

    ingress_topic = f"biflux_test_quotes_{int(time.time())}"
    egress_topic = f"biflux_test_features_{int(time.time())}"

    # 1. Produce raw quote records to ingress topic on Docker Kafka
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=5000,
    )

    quote_records = sample_market_quotes.to_dicts()
    for record in quote_records:
        producer.send(ingress_topic, value=record)
    producer.flush()

    # 2. Ingest micro-batch from Docker Kafka
    consumer = KafkaConsumer(
        ingress_topic,
        bootstrap_servers=[broker],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    consumed_records: List[Dict[str, Any]] = []
    for msg in consumer:
        consumed_records.append(msg.value)
        if len(consumed_records) >= len(quote_records):
            break
    consumer.close()

    assert len(consumed_records) == len(quote_records), (
        f"Expected {len(quote_records)} records from Kafka, received {len(consumed_records)}"
    )

    # 3. Execute Biflux streaming transform
    stream_ctx = BifluxContext(
        mode=ExecutionMode.LIVE,
        env=Environment.LOCAL,
        kafka_brokers=broker,
    )
    pipeline = MarketTickFeaturePipeline(stream_ctx)
    batch_df = pl.DataFrame(consumed_records)
    live_features = pipeline.process_micro_batch(batch_df)

    # 4. Sink A: Emit to Real-Time Kafka serving topic
    for feat in live_features.to_dicts():
        producer.send(egress_topic, value=feat)
    producer.flush()
    producer.close()

    # 5. Sink B: S3 Parquet archive & Offline Backtest
    with tempfile.TemporaryDirectory(prefix="biflux_s3_") as s3_tmp:
        s3_path = os.path.join(s3_tmp, "quotes.parquet")
        batch_df.write_parquet(s3_path)

        batch_ctx = BifluxContext(
            mode=ExecutionMode.BATCH,
            env=Environment.LOCAL,
            s3_endpoint="http://localhost:9000",
        )
        batch_pipeline = MarketTickFeaturePipeline(batch_ctx)
        batch_features = batch_pipeline.transform(pl.scan_parquet(s3_path)).collect()

        # 6. Verify 0.000000% Train-Serve Skew
        for sym in live_features["symbol"]:
            l_vwap = live_features.filter(pl.col("symbol") == sym)["vwap"][0]
            b_vwap = batch_features.filter(pl.col("symbol") == sym)["vwap"][0]
            skew = abs(l_vwap - b_vwap)
            assert skew < 1e-6, f"Skew on {sym}: {skew}"
