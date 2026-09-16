# Tutorials & Quantitative Examples

## The Bond Pricing VWAP Model

Corporate bond pricing models compute the **Volume-Weighted Average Price (VWAP)** using bid and ask quotes and trade sizes.

### Formula

$$\text{Mid Price} = \frac{\text{Bid} + \text{Ask}}{2}$$

$$\text{VWAP} = \frac{\sum (\text{Mid Price} \times \text{Size})}{\sum \text{Size}}$$

### Python Pipeline Implementation

```python
import polars as pl
from biflux.core import BifluxPipeline, BifluxContext, Environment

class BondVWAPPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                mid_price=(pl.col("bid") + pl.col("ask")) / 2.0,
                dollar_volume=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
            )
            .group_by("symbol")
            .agg(
                vwap=(pl.col("dollar_volume").sum() / pl.col("size").sum()).round(6),
                total_volume=pl.col("size").sum(),
            )
            .sort("symbol")
        )
```

---

## Airflow Orchestration

Biflux pipelines integrate into scheduled orchestrators like Apache Airflow:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from biflux.core import BifluxContext, Environment

def run_batch_vwap():
    ctx = BifluxContext(
        mode="batch",
        env=Environment.CLOUD,
        iceberg_catalog="glue://aws_account_id",
    )
    pipeline = BondVWAPPipeline(ctx)
    pipeline.run(
        source_uri="s3://lake/market_data/corporate_bonds/",
        sink_uri="s3://lake/features/corporate_bonds_vwap/",
    )

with DAG(
    "biflux_daily_vwap",
    start_date=datetime(2026, 9, 16),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    compute_task = PythonOperator(
        task_id="compute_iceberg_vwap",
        python_callable=run_batch_vwap,
    )
```
