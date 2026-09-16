"""Example B: Cloud Execution (Requires Credentials & Guardrails).

Demonstrates running a Biflux pipeline against AWS S3, Glue, and MSK.
"""

import os

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
    # 1. Credential Guardrail (Enforced automatically by Biflux or explicitly confirmed)
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        confirm = input("⚠️ WARNING: Attempting to run against AWS. Enter AWS_ACCESS_KEY_ID: ")
        os.environ["AWS_ACCESS_KEY_ID"] = confirm
        os.environ["AWS_SECRET_ACCESS_KEY"] = input("Enter AWS_SECRET_ACCESS_KEY: ")

    # 2. Cloud Context: Connects to AWS Glue and MSK
    cloud_ctx = BifluxContext(
        mode="live",
        env=Environment.CLOUD,
        iceberg_catalog="glue://aws_account_id",  # AWS Glue Catalog
        kafka_brokers="b-1.my-msk.aws.com:9092",  # AWS MSK
    )

    pipeline = VWAPModel(cloud_ctx)
    print("Starting live stream processing on AWS MSK...")
    result = pipeline.run(
        source_uri="prod_market_data",
        sink_uri="prod_vwap_features",
    )
    print(f"Cloud stream initialized: status={result.status}")
