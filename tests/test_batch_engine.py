"""Integration tests for batch engine using physical parquet tables."""

import os
import tempfile

import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, Environment, ExecutionMode


class BatchAggPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2.0)
            .group_by("symbol")
            .agg(
                avg_mid=pl.col("mid").mean().round(4),
                total_volume=pl.col("size").sum(),
            )
            .sort("symbol")
        )


def test_batch_parquet_execution():
    with tempfile.TemporaryDirectory() as tmpdir:
        source_parquet = os.path.join(tmpdir, "raw_ticks.parquet")
        sink_parquet = os.path.join(tmpdir, "aggregated.parquet")

        # Create input dataset
        raw_df = pl.DataFrame(
            {
                "symbol": ["BOND_A", "BOND_A", "BOND_B", "BOND_B"],
                "bid": [99.0, 99.5, 101.0, 101.2],
                "ask": [100.0, 100.5, 102.0, 102.2],
                "size": [100, 200, 300, 400],
            }
        )
        raw_df.write_parquet(source_parquet)

        ctx = BifluxContext(
            mode=ExecutionMode.BATCH,
            env=Environment.LOCAL,
            s3_endpoint="http://localhost:9000",
        )
        pipeline = BatchAggPipeline(ctx)
        result = pipeline.run(
            source_uri=source_parquet,
            sink_uri=sink_parquet,
            sample_df=raw_df.lazy(),
        )

        assert result.status == "SUCCESS"
        assert result.mode == "batch"
        assert result.rows_processed == 2
        assert os.path.exists(sink_parquet)

        # Assert output content
        output_df = pl.read_parquet(sink_parquet)
        assert output_df.shape == (2, 3)

        bond_a = output_df.filter(pl.col("symbol") == "BOND_A")
        assert bond_a["total_volume"][0] == 300
        # mid_1 = 99.5, mid_2 = 100.0 -> mean = 99.75
        assert bond_a["avg_mid"][0] == 99.75

        bond_b = output_df.filter(pl.col("symbol") == "BOND_B")
        assert bond_b["total_volume"][0] == 700
        # mid_1 = 101.5, mid_2 = 101.7 -> mean = 101.6
        assert bond_b["avg_mid"][0] == 101.6
