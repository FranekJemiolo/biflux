# Project Biflux

**Unified Data Engineering Framework Eliminating Train-Serve Skew**

---

## What is Train-Serve Skew?

Train-Serve Skew occurs when the feature engineering pipeline used during **offline backtesting/training** differs in subtle or significant ways from the feature pipeline running in **online inference/serving**:

1. **Language Mismatch**: Offline code is often written in Python (Polars, Pandas, PySpark, DuckDB) by data scientists, while online streaming systems are often rewritten in Java (Flink), C++, or Go by production engineers.
2. **Semantics Drift**: Slight discrepancies in window edge behavior, timestamp truncation, floating-point order of operations, or null coalescing cause production features to diverge from the distributions models were trained on.
3. **Maintenance Overhead**: Every change to an offline feature requires a parallel, manual rewrite of the online feature, causing dual-maintenance burdens and bugs.

---

## The Biflux Solution

**Biflux** fundamentally rethinks this architecture:

> **Write once in Python (Polars), compile into an Apache Arrow execution plan, run identically in batch and stream.**

```mermaid
flowchart LR
    subgraph Authoring
        A["Python API\n(Polars LazyFrame)"]
    end
    subgraph Biflux Engine
        B["Compiled Arrow Execution Plan"]
    end
    subgraph Batch Run
        C["S3 / Apache Iceberg"]
    end
    subgraph Live Stream
        D["Apache Kafka"]
    end

    A --> B
    B -->|BifluxContext(mode='batch')| C
    B -->|BifluxContext(mode='live')| D
```

### Key Capabilities

- **Single Transformation Graph**: Authors express transformations using familiar Polars syntax (`df.with_columns(...)`, `df.group_by(...)`, etc.).
- **Dual Routing**: Instantiating with `mode="batch"` queries historical parquet/Iceberg lakehouses. Instantiating with `mode="live"` attaches to Kafka micro-batches.
- **Zero-Copy Arrow IPC**: Low-latency Rust engine processes micro-batches in-memory using Apache Arrow record batches without serialization penalties.
- **Strict Cloud Guardrails**: Built-in safety toggles prevent unexpected cloud costs or accidental testing against production AWS/GCP clusters.
