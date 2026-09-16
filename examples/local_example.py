"""Example A: Local Testing (No Cloud Credentials Required).

Demonstrates running a Biflux pipeline locally using MinIO (S3) and Redpanda (Kafka).
"""

import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, Environment


class VWAPModel(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2.0)
            .group_by("symbol")
            .agg(vwap=pl.col("mid").mean())
        )


if __name__ == "__main__":
    # Local Context: Connects to localhost MinIO and Redpanda without cloud credentials
    local_ctx = BifluxContext(
        mode="batch",
        env=Environment.LOCAL,
        iceberg_catalog="rest://localhost:8181",  # Local Nessie/REST catalog
        s3_endpoint="http://localhost:9000",  # Local MinIO
        kafka_brokers="localhost:9092",  # Local Redpanda
    )

    pipeline = VWAPModel(local_ctx)
    print("Running local backtest against MinIO...")
    result = pipeline.run(
        source_uri="local_lake.bonds_raw",
        sink_uri="local_lake.bonds_vwap",
    )
    print(
        f"Pipeline executed successfully: status={result.status}, duration={result.duration_ms}ms"
    )
