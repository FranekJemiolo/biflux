# Biflux System Architecture & Technical Design

Project Biflux is engineered as a unified, dual-runtime execution engine that bridges offline analytical lakehouses with real-time streaming buses through a single Apache Arrow compilation graph.

---

## 🏛️ System Overview & Component Topology

```mermaid
graph TD
    subgraph Python API Layer ["1. Developer Interface (Python API)"]
        UserClass["User Pipeline: class MyModel(BifluxPipeline)"]
        TransformFn["transform(df: pl.LazyFrame) -> pl.LazyFrame"]
        CtxInject["BifluxContext(mode, env, catalog, kafka_brokers)"]
        Guardrails["Cloud Guardrails (Credential Validator)"]

        UserClass --> TransformFn
        UserClass --> CtxInject
        CtxInject --> Guardrails
    end

    subgraph Compiler Layer ["2. Plan Compilation & Serialization"]
        LogicalPlan["Polars Logical Plan IR"]
        PlanSer["Plan Serializer (Protobuf / Arrow Schema / DSL)"]
        Guardrails -- Validated --> LogicalPlan
        TransformFn --> LogicalPlan
        LogicalPlan --> PlanSer
    end

    subgraph FFI Boundary ["3. PyO3 Native FFI Bridge"]
        RustBridge["biflux-core (Rust cdylib)"]
        BatchFFI["biflux_core.batch_execute()"]
        StreamFFI["biflux_core.stream_execute()"]
        IpcFFI["biflux_core.execute_arrow_ipc()"]

        PlanSer --> RustBridge
        RustBridge --> BatchFFI
        RustBridge --> StreamFFI
        RustBridge --> IpcFFI
    end

    subgraph Execution Engines ["4. Dual-Runtime Execution Engines"]
        subgraph Batch Path ["Batch Engine Path (S3 / Iceberg)"]
            IcebergCatalog["Iceberg / Glue / Nessie Catalog"]
            PartitionPruner["File Manifest Partition Pruning"]
            ParquetScanner["Parallel Parquet / S3 Arrow Scanner"]
            BatchExec["Arrow Execution Plan"]
            S3Sink["S3 / MinIO Parquet Sink"]

            BatchFFI --> IcebergCatalog
            IcebergCatalog --> PartitionPruner
            PartitionPruner --> ParquetScanner
            ParquetScanner --> BatchExec
            BatchExec --> S3Sink
        end

        subgraph Streaming Path ["Stream Engine Path (Kafka / Redpanda)"]
            KafkaConsumer["rdkafka High-Throughput Consumer"]
            ArrowBuffer["Zero-Copy Arrow IPC Micro-Batch Buffer"]
            StreamExec["Arrow Execution Plan (Identical IR)"]
            KafkaProducer["rdkafka Sink Producer"]

            StreamFFI --> KafkaConsumer
            KafkaConsumer --> ArrowBuffer
            ArrowBuffer --> StreamExec
            StreamExec --> KafkaProducer
        end
    end

    classDef highlight fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#1e40af;
    classDef success fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;
    classDef rust fill:#ffedd5,stroke:#ea580c,stroke-width:2px,color:#9a3412;
    classDef arrow fill:#ecfdf5,stroke:#059669,stroke-width:2px,color:#065f46;
    classDef neutral fill:#f8fafc,stroke:#64748b,stroke-width:1.5px,color:#1e293b;

    class UserClass,TransformFn,CtxInject,Guardrails highlight;
    class LogicalPlan,PlanSer success;
    class RustBridge,BatchFFI,StreamFFI,IpcFFI rust;
    class BatchExec,StreamExec,ArrowBuffer arrow;
    class IcebergCatalog,PartitionPruner,ParquetScanner,S3Sink,KafkaConsumer,KafkaProducer neutral;
```

---

## ⚡ Zero-Copy Arrow Memory Model

In conventional architectures, real-time message streams undergo severe serialization penalties:

```
[Kafka TCP Stream]
       │
       ▼ (Wire deserialization)
[JSON / Avro Strings]
       │
       ▼ (Python heap allocation)
[Python Dicts & Objects]
       │
       ▼ (DataFrame creation & copy)
[Pandas / Arrow Table]
       │
       ▼ (Feature computation)
[Output DataFrame]
       │
       ▼ (JSON serialization)
[Kafka Producer Socket]
```

### The Biflux In-Memory Pathway

Biflux eliminates intermediate string and dictionary allocations by operating directly on **pre-allocated Apache Arrow `RecordBatch` buffers**:

```mermaid
sequenceDiagram
    autonumber
    participant K as Apache Kafka / Network
    participant R as rdkafka (Rust Crate)
    participant A as Arrow IPC Buffer
    participant P as Polars / Arrow Execution Plan
    participant S as Sink Broker / S3

    K->>R: Raw Wire TCP Stream
    Note over R,A: Zero-copy pointer wrapping into Arrow RecordBatch
    R->>A: Append to contiguous ChunkedArray
    A->>P: Pass Arrow pointer table (O(1) memory transfer)
    Note over P: Executes compiled LogicalPlan expressions in parallel SIMD
    P->>A: Result RecordBatch
    A->>S: Transmit Arrow IPC bytes / JSON messages
```

### Memory Safety & Rust Invariants

1. **Foreign Function Interface (PyO3)**: Memory blocks allocated in Rust are tracked by `Arc<arrow::array::RecordBatch>`. Python accesses the memory through Arrow PyCapsule / IPC streams without duplicating underlying memory arrays.
2. **SIMD Vectorization**: Numerical computations (`mid = (bid + ask) / 2.0`, `vwap = sum(dollar_vol) / sum(vol)`) compile directly to AVX-512 / ARM Neon vector instructions.
3. **Partition Pruning**: In batch mode, Iceberg manifest files are scanned in Rust to eliminate non-matching partitions before reading Parquet byte ranges from S3.

---

## 🔀 Batch vs. Stream Execution Routing

```mermaid
flowchart TD
    Start["User invokes pipeline.run(source_uri, sink_uri)"]
    CheckCreds{"env == Environment.CLOUD?"}
    CredGuard["Validate AWS/GCP Credentials & Safety Prompts"]
    Compile["Compile Polars LazyFrame Plan IR"]
    CheckMode{"context.mode"}

    Start --> CheckCreds
    CheckCreds -- Yes --> CredGuard
    CheckCreds -- No --> Compile
    CredGuard -- Validated --> Compile

    Compile --> CheckMode

    CheckMode -- "mode == 'batch'" --> BatchRoute["Route to _execute_batch()"]
    CheckMode -- "mode == 'live'" --> StreamRoute["Route to _execute_stream()"]

    subgraph Batch Flow
        BatchRoute --> ResolveFiles["Resolve Iceberg / Parquet URI partitions"]
        ResolveFiles --> ScanParquet["Parallel Parquet Read into Arrow"]
        ScanParquet --> ExecBatch["Execute Logical Graph on Full Partitions"]
        ExecBatch --> WriteParquet["Write Result Parquet to Sink S3"]
    end

    subgraph Stream Flow
        StreamRoute --> InitKafka["Initialize rdkafka Consumer & Producer"]
        InitKafka --> MicroBatch["Accumulate Arrow Micro-Batches"]
        MicroBatch --> ExecStream["Execute Identical Logical Graph on Batches"]
        ExecStream --> EmitKafka["Emit Transformed Batches to Sink Topic"]
    end

    WriteParquet --> Summary["Return ExecutionResult(status, duration_ms, rows)"]
    EmitKafka --> Summary
```

---

## 🛡️ Cloud Guardrail Architecture

To protect engineering organizations against catastrophic AWS/GCP cost overruns during local testing or CI runs, Biflux implements a two-tier gate:

1. **Environment Separation**: `Environment.LOCAL` (default) explicitly targets `localhost` MinIO and Redpanda endpoints, bypassing cloud credential checks.
2. **Cloud Gating**: `Environment.CLOUD` strictly checks for active credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or `GOOGLE_APPLICATION_CREDENTIALS`). If missing in an interactive session, it prompts the developer with explicit cost warnings; in automated non-interactive runs, it halts immediately with `BifluxCredentialError`.
