"""Example C: Apache Airflow Orchestration Wrapper.

Demonstrates orchestrating a Biflux batch execution pipeline in a production DAG.
"""

from datetime import datetime

import polars as pl

from biflux.core import BifluxContext, BifluxPipeline, Environment


class VWAPModel(BifluxPipeline):
    """User-defined feature pipeline executed on historical Iceberg lakehouse."""

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2.0)
            .group_by("symbol")
            .agg(vwap=pl.col("mid").mean())
        )


def run_biflux_batch():
    """Callable executed by Airflow PythonOperator."""
    # Airflow triggers the batch mode nightly
    ctx = BifluxContext(
        mode="batch",
        env=Environment.CLOUD,
        iceberg_catalog="glue://aws_account_id",
    )
    pipeline = VWAPModel(ctx)
    result = pipeline.run(
        source_uri="raw_bonds",
        sink_uri="daily_vwap",
        interactive_prompt=False,
    )
    print(f"Airflow batch task finished: {result.status}, {result.duration_ms}ms")
    return result.status


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        "biflux_daily_vwap",
        start_date=datetime(2026, 9, 16),
        schedule_interval="@daily",
        catchup=False,
    ) as dag:
        run_batch = PythonOperator(
            task_id="compute_iceberg_vwap",
            python_callable=run_biflux_batch,
        )
except ImportError:
    # Allows module import in environments where apache-airflow is not installed
    dag = None  # type: ignore
