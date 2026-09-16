<div align="center">

# Biflux

**Unified Data Engineering Framework Eliminating Train-Serve Skew**

[![CI](https://github.com/FranekJemiolo/biflux/actions/workflows/ci.yml/badge.svg)](https://github.com/FranekJemiolo/biflux/actions/workflows/ci.yml)
[![Docs](https://github.com/FranekJemiolo/biflux/actions/workflows/docs.yml/badge.svg)](https://franekjemiolo.github.io/biflux/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Rust](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org/)

</div>

---

## ⚡ The Train-Serve Skew Problem

In production machine learning and quantitative systems, **Train-Serve Skew** is one of the most persistent sources of catastrophic failures:
- **Offline Backtests / Training**: Engineers write SQL, PySpark, DuckDB, or Polars scripts querying historical data lakes (S3, Apache Iceberg, Delta Lake).
- **Online Inference / Serving**: Systems engineers rewrite the feature pipeline in C++, Java (Flink), or Go consuming from real-time message buses (Apache Kafka, Redpanda).

Over time, discrepancies in timestamp truncation, floating-point rounding, window boundaries, null handling, or subtle mathematical logic creep into both codebases. Models fail silently in production because the real-time feature vector deviates from the historical training distribution.

## 🚀 The Biflux Solution

**Biflux** compiles data transformations authored once in a high-level Python API (using Polars) into a single, unified Rust/Apache Arrow execution plan. That identical plan is conditionally routed to:
1. **Batch Mode**: High-throughput distributed backtests and lakehouse ETL over S3 and Apache Iceberg.
2. **Live Mode**: Sub-millisecond real-time stream processing over Apache Kafka with zero-copy Arrow memory buffers.

Zero logic duplication. Zero mathematical divergence. Exact parity down to the decimal point.

```mermaid
graph TD
    subgraph Developer API
        Code["Pipeline Definition: transform(df: pl.LazyFrame)"]
        Ctx["BifluxContext(mode='batch' | 'live', env=Environment.LOCAL | CLOUD)"]
        Code --> Ctx
    end

    subgraph Biflux Runtime
        Ctx --> Guardrail{"Cloud Credential Guardrail"}
        Guardrail -- Validated --> RustCore["biflux-core (Rust/PyO3 Engine)"]
        RustCore --> PlanEngine["Single Arrow Execution Plan"]
    end

    subgraph Batch Execution Path
        PlanEngine -- "mode=batch" --> S3Reader["S3 / MinIO Parquet & Iceberg"]
        S3Reader --> BatchExec["Arrow Batch Processor"]
        BatchExec --> S3Sink["S3 / Iceberg Sink"]
    end

    subgraph Streaming Execution Path
        PlanEngine -- "mode=live" --> KafkaConsumer["Kafka / Redpanda Consumer"]
        KafkaConsumer --> StreamExec["Arrow Micro-Batch Buffer"]
        StreamExec --> KafkaSink["Kafka Output Topic"]
    end

    style Code fill:#2563eb,stroke:#1e40af,color:#fff
    style RustCore fill:#d97706,stroke:#b45309,color:#fff
    style PlanEngine fill:#059669,stroke:#047857,color:#fff
```

---

## 📦 Installation

Install the pre-compiled wheel via pip:

```bash
pip install biflux
```

Or build locally from source (requires Rust toolchain):

```bash
git clone https://github.com/FranekJemiolo/biflux.git
cd biflux
pip install maturin
maturin develop
```

---

## 💡 Quickstart: One Pipeline, Two Worlds

Define your pipeline once:

```python
import polars as pl
from biflux.core import BifluxPipeline, BifluxContext, Environment

class VWAPModel(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # Single transformation graph used across both batch and live streams
        return (
            df.with_columns(
                mid_price=(pl.col("bid") + pl.col("ask")) / 2.0,
                dollar_volume=((pl.col("bid") + pl.col("ask")) / 2.0) * pl.col("size"),
            )
            .group_by("symbol")
            .agg(
                vwap=(pl.col("dollar_volume").sum() / pl.col("size").sum()),
                total_volume=pl.col("size").sum(),
            )
        )
```

### Side-by-Side Execution

=== "Batch Backtest (Historical Lakehouse)"
    ```python
    # 1. Historical Context: Reads historical Parquet/Iceberg tables from S3
    batch_ctx = BifluxContext(
        mode="batch",
        env=Environment.LOCAL,
        iceberg_catalog="rest://localhost:8181",
        s3_endpoint="http://localhost:9000",
    )

    pipeline = VWAPModel(batch_ctx)
    result = pipeline.run(
        source_uri="s3://market-data/raw_ticks/",
        sink_uri="s3://features/vwap_daily/",
    )
    print(f"Batch completed: {result.duration_ms}ms, status={result.status}")
    ```

=== "Live Stream (Real-Time Kafka)"
    ```python
    # 2. Live Context: Subscribes to real-time tick stream on Kafka
    live_ctx = BifluxContext(
        mode="live",
        env=Environment.LOCAL,
        kafka_brokers="localhost:9092",
    )

    pipeline = VWAPModel(live_ctx)
    result = pipeline.run(
        source_uri="market_ticks_topic",
        sink_uri="vwap_realtime_topic",
    )
    print(f"Streaming initialized: {result.duration_ms}ms, status={result.status}")
    ```

---

## 🛡️ Enterprise Guardrails

Biflux includes built-in safety mechanisms:
- **Cloud vs. Local Parity**: Prevents accidental AWS/GCP access during local unit testing.
- **Strict Credential Validation**: When `env=Environment.CLOUD`, credentials must be explicitly confirmed or present, otherwise `BifluxCredentialError` is raised.
- **Airflow & Orchestration Ready**: First-class support for Airflow DAGs and Prefect tasks.

---

## 📖 Documentation

Comprehensive guides, tutorials, and architectural deep-dives are available on our [Documentation Site](https://franekjemiolo.github.io/biflux/):
- [Architecture & Zero-Copy Memory Model](https://franekjemiolo.github.io/biflux/architecture/)
- [E2E Bond Pricing VWAP Tutorial](https://franekjemiolo.github.io/biflux/tutorials/)
- [Apache Airflow Orchestration Guide](https://franekjemiolo.github.io/biflux/tutorials/#airflow)

---

## 📜 License

Project Biflux is licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.
