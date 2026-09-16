"""Unit tests for BifluxPipeline abstract base class and validation."""

import polars as pl
import pytest

from biflux.core import (
    BifluxContext,
    BifluxPipeline,
    Environment,
    ExecutionMode,
    PlanValidationError,
)


class ValidTestPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.with_columns(doubled=pl.col("val") * 2)


class InvalidReturnDataFramePipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # Invalid: Returns DataFrame instead of LazyFrame
        return df.collect()  # type: ignore


class InvalidReturnNonePipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # Invalid: Returns None
        return None  # type: ignore


def test_valid_pipeline_plan_compilation():
    pipeline = ValidTestPipeline()
    sample_df = pl.LazyFrame({"val": [1, 2, 3]})
    plan_bytes = pipeline.compile_plan(sample_df)
    assert isinstance(plan_bytes, bytes)
    assert len(plan_bytes) > 0


def test_pipeline_raises_on_dataframe_return():
    pipeline = InvalidReturnDataFramePipeline()
    sample_df = pl.LazyFrame({"val": [1, 2, 3]})
    with pytest.raises(
        PlanValidationError, match="Expected transform\\(\\) to return a polars.LazyFrame"
    ):
        pipeline.validate_plan(sample_df)


def test_pipeline_raises_on_none_return():
    pipeline = InvalidReturnNonePipeline()
    with pytest.raises(
        PlanValidationError, match="Expected transform\\(\\) to return a polars.LazyFrame"
    ):
        pipeline.validate_plan()


def test_pipeline_batch_dispatch():
    ctx = BifluxContext(
        mode=ExecutionMode.BATCH,
        env=Environment.LOCAL,
        s3_endpoint="http://localhost:9000",
    )
    pipeline = ValidTestPipeline(ctx)
    sample_df = pl.LazyFrame({"val": [10, 20]})

    result = pipeline.run(
        source_uri="s3://lake/raw_vals/",
        sink_uri="s3://lake/doubled_vals/",
        sample_df=sample_df,
    )

    assert result.status == "SUCCESS"
    assert result.mode == "batch"
    assert result.source_uri == "s3://lake/raw_vals/"
    assert result.sink_uri == "s3://lake/doubled_vals/"
    assert result.duration_ms >= 0


def test_pipeline_stream_dispatch():
    ctx = BifluxContext(
        mode=ExecutionMode.LIVE,
        env=Environment.LOCAL,
        kafka_brokers="localhost:9092",
    )
    pipeline = ValidTestPipeline(ctx)
    sample_df = pl.LazyFrame({"val": [10, 20]})

    result = pipeline.run(
        source_uri="input_ticks",
        sink_uri="output_ticks",
        sample_df=sample_df,
    )

    assert result.status == "SUCCESS"
    assert result.mode == "live"
    assert result.source_uri == "input_ticks"
    assert result.sink_uri == "output_ticks"
    assert result.duration_ms >= 0
