"""Integration tests for live cloud endpoints and confirmation guardrails."""

import os
import sys

import polars as pl
import pytest

from biflux.core import BifluxContext, BifluxPipeline, Credentials, Environment


class CloudTestPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.with_columns(score=pl.col("val") * 10)


@pytest.fixture(autouse=True)
def check_cloud_environment_gate():
    """Gate cloud integration tests behind RUN_CLOUD_INTEGRATION secret or interactive prompt."""
    # 1. CI check: If RUN_CLOUD_INTEGRATION is not explicitly set, skip in non-interactive CI
    run_cloud = os.environ.get("RUN_CLOUD_INTEGRATION", "").lower() in ("true", "1")

    if not run_cloud:
        # 2. Local check: If running interactively, prompt the developer
        if sys.stdin and sys.stdin.isatty():
            try:
                msg = (
                    "\n⚠️ You are about to execute tests against a live AWS environment "
                    "resulting in potential charges. Proceed? [y/N]: "
                )
                response = input(msg).strip().lower()
                if response != "y":
                    pytest.skip("Gracefully skipped: Developer declined cloud execution charges.")
            except (EOFError, KeyboardInterrupt):
                pytest.skip("Gracefully skipped: Cloud test execution prompt aborted.")
        else:
            pytest.skip(
                "Skipping live cloud tests: RUN_CLOUD_INTEGRATION secret not set "
                "and not in interactive session."
            )


def test_cloud_environment_endpoint_execution(monkeypatch):
    """Test cloud endpoint execution with confirmed credentials."""
    creds = Credentials(
        aws_access_key_id="CONFIRMED_CLOUD_KEY",
        aws_secret_access_key="CONFIRMED_CLOUD_SECRET",
    )
    ctx = BifluxContext(
        mode="batch",
        env=Environment.CLOUD,
        iceberg_catalog="glue://123456789012",
        credentials=creds,
    )
    pipeline = CloudTestPipeline(ctx)
    sample_df = pl.LazyFrame({"val": [1, 2, 3]})

    result = pipeline.run(
        source_uri="glue_db.table_in",
        sink_uri="glue_db.table_out",
        sample_df=sample_df,
        interactive_prompt=False,
    )
    assert result.status == "SUCCESS"
    assert result.mode == "batch"
