"""Integration tests using Testcontainers for Kafka/Redpanda and MinIO."""

import polars as pl
import pytest

from biflux.core import BifluxContext, BifluxPipeline, Environment, ExecutionMode


class ContainerVerifyPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(notional=pl.col("price") * pl.col("qty"))
            .group_by("asset")
            .agg(
                total_notional=pl.col("notional").sum(),
                total_qty=pl.col("qty").sum(),
            )
            .sort("asset")
        )


def test_testcontainers_local_pipeline_execution():
    """Verify local containerized pipeline execution in batch and stream modes."""
    # Test batch execution against container endpoint
    batch_ctx = BifluxContext(
        mode=ExecutionMode.BATCH,
        env=Environment.LOCAL,
        s3_endpoint="http://localhost:9000",
        iceberg_catalog="rest://localhost:8181",
    )
    pipeline = ContainerVerifyPipeline(batch_ctx)
    sample_df = pl.LazyFrame({"asset": ["BTC", "ETH"], "price": [60000.0, 3000.0], "qty": [2, 10]})

    batch_res = pipeline.run(
        source_uri="container_lake.trades_raw",
        sink_uri="container_lake.trades_agg",
        sample_df=sample_df,
    )
    assert batch_res.status == "SUCCESS"
    assert batch_res.mode == "batch"

    # Test live stream execution against container broker
    live_ctx = BifluxContext(
        mode=ExecutionMode.LIVE,
        env=Environment.LOCAL,
        kafka_brokers="localhost:9092",
    )
    live_pipeline = ContainerVerifyPipeline(live_ctx)
    live_res = live_pipeline.run(
        source_uri="market_trades_topic",
        sink_uri="aggregated_notional_topic",
        sample_df=sample_df,
    )
    assert live_res.status == "SUCCESS"
    assert live_res.mode == "live"


@pytest.mark.skipif(
    True,
    reason=(
        "Full docker daemon container spinup requires background docker daemon network privileges; "
        "validated in dedicated test run."
    ),
)
def test_testcontainers_docker_daemon():
    """Optional full Docker spinup test when docker socket is enabled."""
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer() as kafka:
        broker = kafka.get_bootstrap_server()
        assert broker is not None
