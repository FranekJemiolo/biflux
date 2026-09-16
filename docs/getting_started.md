# Getting Started with Biflux

This guide walks you through installing Biflux, understanding its core abstractions, authoring your first unified feature pipeline, and running it in both **Batch Backtest** and **Live Streaming** modes.

---

## 📦 1. Installation

### Install from GitHub
```bash
pip install "git+https://github.com/FranekJemiolo/biflux.git"
```

### From Source (Local Development)
Building locally requires Python 3.10+ and the Rust toolchain (1.75+):

```bash
git clone https://github.com/FranekJemiolo/biflux.git
cd biflux
pip install maturin
maturin develop --uv
```

Verify your installation:
```bash
python -c "import biflux; print(f'Biflux version: {biflux.__version__}')"
```

---

## 🧱 2. Core Concepts & Abstractions

Biflux is built around two primary classes and execution modes:

| Abstraction | Module | Role |
| :--- | :--- | :--- |
| **`BifluxPipeline`** | `biflux.core.pipeline` | Abstract Base Class where developers implement `transform(df: pl.LazyFrame) -> pl.LazyFrame`. |
| **`BifluxContext`** | `biflux.core.context` | Runtime environment configuration holding execution mode, credentials, and catalog endpoints. |
| **`ExecutionMode`** | `biflux.core.models` | Enum: `BATCH` (Lakehouse S3/Iceberg) or `LIVE` (Real-Time Kafka). |
| **`Environment`** | `biflux.core.models` | Enum: `LOCAL` (MinIO/Redpanda without credentials) or `CLOUD` (AWS MSK/Glue with strict validation). |

---

## ✍️ 3. Step 1: Write Your Transformation Once

Every Biflux pipeline subclasses `BifluxPipeline`. The `transform` method receives a Polars `LazyFrame` and must return a Polars `LazyFrame` defining the computational graph.

> [!IMPORTANT]
> **Do NOT call `.collect()` inside `transform()`.** Returning a `pl.LazyFrame` allows Biflux to inspect, optimize, serialize, and route the plan to the Rust Arrow execution engine.

```python
import polars as pl
from biflux import BifluxPipeline

class MarketSpreadPipeline(BifluxPipeline):
    """Calculates instantaneous mid-price, spread in basis points, and volume."""

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.filter(pl.col("bid") > 0.0)
            .filter(pl.col("ask") > 0.0)
            .with_columns(
                mid_price=(pl.col("bid") + pl.col("ask")) / 2.0,
                spread_bps=((pl.col("ask") - pl.col("bid")) / ((pl.col("bid") + pl.col("ask")) / 2.0)) * 10000.0,
                dollar_volume=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
            )
            .group_by("symbol")
            .agg(
                mean_spread_bps=pl.col("spread_bps").mean().round(4),
                total_dollar_volume=pl.col("dollar_volume").sum().round(2),
                quote_count=pl.len(),
            )
            .sort("symbol")
        )
```

---

## 💾 4. Step 2: Run a Local Historical Backtest

When running historical backtests over local Parquet files, MinIO, or an Apache Iceberg REST catalog:

```python
from biflux import BifluxContext, Environment, ExecutionMode

# 1. Instantiate local batch context
batch_ctx = BifluxContext(
    mode=ExecutionMode.BATCH,
    env=Environment.LOCAL,
    s3_endpoint="http://localhost:9000",
    iceberg_catalog="rest://localhost:8181",
)

# 2. Bind pipeline to context
pipeline = MarketSpreadPipeline(batch_ctx)

# 3. Execute backtest over S3 or local Parquet tables
result = pipeline.run(
    source_uri="s3://lake/market_data/ticks_20260916.parquet",
    sink_uri="s3://lake/features/spreads_daily/",
)

print(f"Status: {result.status}")
print(f"Duration: {result.duration_ms} ms")
print(f"Rows Processed: {result.rows_processed}")
```

---

## ⚡ 5. Step 3: Run Real-Time Live Streaming

To switch to online real-time stream processing, simply change the context `mode="live"`. The pipeline transformation logic remains 100% identical:

```python
# 1. Instantiate live streaming context
live_ctx = BifluxContext(
    mode=ExecutionMode.LIVE,
    env=Environment.LOCAL,
    kafka_brokers="localhost:9092",
    kafka_config={"group.id": "spread_feature_engine", "auto.offset.reset": "latest"},
)

# 2. Bind the exact same pipeline class
pipeline = MarketSpreadPipeline(live_ctx)

# 3. Execute stream engine subscribing to Kafka topics
result = pipeline.run(
    source_uri="market_ticks_live",
    sink_uri="market_spreads_live",
)

print(f"Stream status: {result.status}")
```

### In-Memory Streaming Micro-Batches
For custom Kafka consumers or unit testing in CI, you can feed micro-batches directly into `process_micro_batch`:

```python
# Pass in a streaming micro-batch (DataFrame or Arrow IPC bytes)
micro_batch_df = pl.DataFrame({
    "symbol": ["AAPL", "AAPL", "MSFT"],
    "bid": [150.0, 150.1, 300.0],
    "ask": [150.2, 150.3, 300.4],
    "size": [100, 200, 50],
})

# Evaluates identical transformation with sub-millisecond latency
output_df = pipeline.process_micro_batch(micro_batch_df)
print(output_df)
```

---

## 🛡️ 6. Step 4: Production Cloud Guardrails

When deploying to production AWS/GCP lakehouses (`env=Environment.CLOUD`):
- Biflux enforces strict credential validation.
- Missing credentials immediately raise `BifluxCredentialError` rather than silently timing out or charging unexpected cloud resources.

```python
from biflux import BifluxContext, Environment, ExecutionMode, Credentials

prod_ctx = BifluxContext(
    mode=ExecutionMode.BATCH,
    env=Environment.CLOUD,
    iceberg_catalog="glue://123456789012",
    credentials=Credentials(
        aws_access_key_id="AKIA...",
        aws_secret_access_key="wJalr...",
        aws_region="us-east-1",
    ),
)
```

---

## 🔬 7. Step 5: Asserting Zero Train-Serve Skew in Tests

To guarantee exact mathematical parity between your offline backtests and live streaming models in your test suite:

```python
def test_zero_skew():
    sample_data = generate_sample_quotes(10_000)

    # 1. Run in batch mode
    batch_res = MarketSpreadPipeline(BifluxContext(mode=ExecutionMode.BATCH)).transform(sample_data.lazy()).collect()

    # 2. Run in streaming mode
    live_res = MarketSpreadPipeline(BifluxContext(mode=ExecutionMode.LIVE)).process_micro_batch(sample_data)

    # 3. Mathematically assert zero skew
    for sym in batch_res["symbol"]:
        b_val = batch_res.filter(pl.col("symbol") == sym)["mean_spread_bps"][0]
        l_val = live_res.filter(pl.col("symbol") == sym)["mean_spread_bps"][0]
        assert abs(b_val - l_val) < 1e-6, f"Skew detected on {sym}!"
```

---

## 📖 Next Steps

Explore production domain tutorials:
- [Fixed Income: Bond Pricing VWAP](tutorials.md#1-fixed-income-bond-pricing-vwap-model)
- [Financial Fraud: Transaction Velocity Scoring](tutorials.md#2-real-time-payment-fraud-scoring)
- [High-Frequency Trading: L2 Orderbook Depth](tutorials.md#3-hft-l2-orderbook-depth-micro-price)
- [Industrial IoT: Predictive Maintenance](tutorials.md#4-industrial-iot-turbine-predictive-health)
- [Custom Python UDFs](udf.md): Accelerating proprietary mathematical functions with Rust bindings.
