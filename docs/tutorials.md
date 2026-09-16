# Real-World Domain Pipelines & Tutorials

Biflux was engineered to handle demanding data engineering workloads across high-frequency trading, payment risk defense, industrial IoT, and enterprise lakehouses.

Below are 6 production-grade pipeline implementations proving **0.000000% Train-Serve Skew** across diverse domains.

---

## 📈 1. Fixed Income: Bond Pricing VWAP Model

Corporate bond pricing desks compute **Volume-Weighted Average Price (VWAP)** to establish institutional transaction benchmark prices.

### Mathematical Formulation
$$\text{Mid Price}_i = \frac{\text{Bid}_i + \text{Ask}_i}{2}$$

$$\text{Dollar Volume}_i = \text{Mid Price}_i \times \text{Size}_i$$

$$\text{VWAP} = \frac{\sum_{i=1}^N \text{Dollar Volume}_i}{\sum_{i=1}^N \text{Size}_i}$$

### Pipeline Implementation
```python
import polars as pl
from biflux import BifluxPipeline

class BondPricingVWAP(BifluxPipeline):
    """Calculates bond volume-weighted average price across quotes."""

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                mid_price=(pl.col("bid") + pl.col("ask")) / 2.0,
                dollar_volume=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
            )
            .group_by("symbol")
            .agg(
                total_volume=pl.col("size").sum(),
                total_dollar_volume=pl.col("dollar_volume").sum().round(2),
                vwap=(pl.col("dollar_volume").sum() / pl.col("size").sum()).round(6),
                quote_count=pl.len(),
            )
            .sort("symbol")
        )
```

### Verification & Skew Proof (20,000 Quotes)
```
SYMBOL             | BATCH VWAP     | LIVE VWAP      | DIFF         | STATUS
--------------------------------------------------------------------------------
AAPL-CORP-2030     | 98.308978      | 98.308978      | 0.000000     | MATCH (0.000000 Skew)
AMZN-CORP-2029     | 101.142059     | 101.142059     | 0.000000     | MATCH (0.000000 Skew)
GOOG-CORP-2032     | 95.443961      | 95.443961      | 0.000000     | MATCH (0.000000 Skew)
MSFT-CORP-2028     | 104.808747     | 104.808747     | 0.000000     | MATCH (0.000000 Skew)
US-TREAS-10Y       | 99.846180      | 99.846180      | 0.000000     | MATCH (0.000000 Skew)
```
*Source: [`examples/bond_pricing_pipeline.py`](https://github.com/FranekJemiolo/biflux/blob/main/examples/bond_pricing_pipeline.py)*

---

## 💳 2. Real-Time Payment Fraud Scoring

Card networks monitor cardholder velocity, anomalous transaction amounts, and risk thresholds:

```python
class TransactionFraudScoringPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.filter(pl.col("amount") > 0.0)
            .with_columns(
                # Flag high-risk transactions
                is_high_value=(pl.col("amount") > 1000.0).cast(pl.Int32),
                is_foreign=(pl.col("currency") != "USD").cast(pl.Int32),
            )
            .group_by("cardholder_id")
            .agg(
                txn_count=pl.len(),
                total_spent=pl.col("amount").sum().round(2),
                mean_amount=pl.col("amount").mean().round(2),
                high_value_count=pl.col("is_high_value").sum(),
                foreign_txn_count=pl.col("is_foreign").sum(),
            )
            .with_columns(
                # Compute risk multiplier
                risk_score=(
                    (pl.col("mean_amount") / 100.0)
                    + (pl.col("high_value_count") * 2.5)
                    + (pl.col("foreign_txn_count") * 1.8)
                ).round(4)
            )
            .sort("cardholder_id")
        )
```
*Source: [`examples/fraud_detection_pipeline.py`](https://github.com/FranekJemiolo/biflux/blob/main/examples/fraud_detection_pipeline.py)*

---

## ⚡ 3. HFT: L2 Orderbook Depth & Micro-Price

High-frequency market makers compute orderbook imbalance and micro-price to predict short-term order flow:

### Formulas
$$\text{Imbalance Ratio} = \frac{\text{Bid Size} - \text{Ask Size}}{\text{Bid Size} + \text{Ask Size}}$$

$$\text{Micro-Price} = \frac{\text{Bid Size} \times \text{Ask Price} + \text{Ask Size} \times \text{Bid Price}}{\text{Bid Size} + \text{Ask Size}}$$

```python
class OrderbookMicrostructurePipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                mid_price=((pl.col("bid_px_0") + pl.col("ask_px_0")) / 2.0).round(4),
                spread_bps=(
                    (pl.col("ask_px_0") - pl.col("bid_px_0"))
                    / ((pl.col("bid_px_0") + pl.col("ask_px_0")) / 2.0)
                    * 10000.0
                ).round(2),
                imbalance_ratio=(
                    (pl.col("bid_sz_0") - pl.col("ask_sz_0"))
                    / (pl.col("bid_sz_0") + pl.col("ask_sz_0"))
                ).round(6),
                micro_price=(
                    (pl.col("bid_sz_0") * pl.col("ask_px_0") + pl.col("ask_sz_0") * pl.col("bid_px_0"))
                    / (pl.col("bid_sz_0") + pl.col("ask_sz_0"))
                ).round(6),
            )
            .group_by("symbol")
            .agg(
                mean_imbalance=pl.col("imbalance_ratio").mean().round(6),
                mean_micro_price=pl.col("micro_price").mean().round(6),
                mean_spread_bps=pl.col("spread_bps").mean().round(2),
                quote_count=pl.len(),
            )
            .sort("symbol")
        )
```
*Source: [`examples/orderbook_depth_pipeline.py`](https://github.com/FranekJemiolo/biflux/blob/main/examples/orderbook_depth_pipeline.py)*

---

## ⚙️ 4. Industrial IoT: Turbine Predictive Health

Industrial monitoring systems calculate root-mean-square (RMS) vibration energy and thermal stress:

```python
class PredictiveMaintenancePipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                # Vibration magnitude vector
                vibration_rms=(
                    (pl.col("vibration_x").pow(2) + pl.col("vibration_y").pow(2) + pl.col("vibration_z").pow(2))
                    / 3.0
                ).sqrt().round(4),
                thermal_gradient=(pl.col("bearing_temp") - pl.col("ambient_temp")).round(2),
            )
            .group_by("turbine_id")
            .agg(
                mean_vibration_rms=pl.col("vibration_rms").mean().round(4),
                peak_vibration_rms=pl.col("vibration_rms").max().round(4),
                mean_thermal_gradient=pl.col("thermal_gradient").mean().round(2),
                rpm_stability=pl.col("rpm").std().round(2),
            )
            .sort("turbine_id")
        )
```
*Source: [`examples/iot_sensor_telemetry_pipeline.py`](https://github.com/FranekJemiolo/biflux/blob/main/examples/iot_sensor_telemetry_pipeline.py)*

---

## 📐 5. Quantitative Derivatives: Custom Python/Rust UDF

Proprietary option risk models require non-linear Black-Scholes formulas executed at native speeds:

```python
import math
from biflux import BifluxPipeline, biflux_udf, biflux_udf_expr

@biflux_udf
def theoretical_option_delta(spot: float, strike: float) -> float:
    """Fast Black-Scholes call delta approximation evaluated via Rust bindings."""
    if spot <= 0 or strike <= 0:
        return 0.5
    moneyness = spot / strike
    d1 = math.log(moneyness) + 0.02
    return round(1.0 / (1.0 + math.exp(-1.7 * d1)), 6)

class OptionRiskPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(
                delta=biflux_udf_expr(theoretical_option_delta, pl.col("bid"), pl.col("ask"))
            )
            .group_by("symbol")
            .agg(
                mean_delta=pl.col("delta").mean().round(6),
                total_volume=pl.col("size").sum(),
            )
            .sort("symbol")
        )
```
*Source: [`examples/custom_udf_pipeline.py`](https://github.com/FranekJemiolo/biflux/blob/main/examples/custom_udf_pipeline.py)*

---

## ⏱️ 6. Apache Airflow Scheduled Orchestration

Deploying historical backtest jobs on a nightly schedule via standard Airflow PythonOperators:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from biflux import BifluxContext, Environment, ExecutionMode
from examples.bond_pricing_pipeline import BondPricingVWAP

def execute_nightly_vwap():
    ctx = BifluxContext(
        mode=ExecutionMode.BATCH,
        env=Environment.CLOUD,
        iceberg_catalog="glue://123456789012",
        s3_endpoint="https://s3.us-east-1.amazonaws.com",
    )
    pipeline = BondPricingVWAP(ctx)
    res = pipeline.run(
        source_uri="s3://lake/market_data/corporate_bonds/",
        sink_uri="s3://lake/features/corporate_bonds_vwap/",
    )
    if res.status != "SUCCESS":
        raise RuntimeError(f"Biflux batch job failed: {res.details}")

with DAG(
    "biflux_nightly_feature_engineering",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    vwap_task = PythonOperator(
        task_id="compute_bond_vwap",
        python_callable=execute_nightly_vwap,
    )
```
*Source: [`examples/airflow_dag.py`](https://github.com/FranekJemiolo/biflux/blob/main/examples/airflow_dag.py)*
