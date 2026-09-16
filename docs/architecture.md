# Biflux Architecture & Zero-Copy Memory Model

Biflux is engineered to provide sub-millisecond streaming throughput alongside high-throughput batch analytics by leveraging a unified Apache Arrow memory model.

---

## 1. Zero-Copy In-Memory Pipeline

```
  +---------------------------------------------------------+
  |              Kafka / Redpanda Broker                    |
  +---------------------------------------------------------+
                              |
                              v (Kafka TCP Stream)
  +---------------------------------------------------------+
  |    rdkafka Consumer (Native Rust Crate)                 |
  |    Direct message deserialization into Arrow IPC buffer |
  +---------------------------------------------------------+
                              |
                              v (Zero-copy pointer handoff)
  +---------------------------------------------------------+
  |    Arrow RecordBatch / ChunkedArray In-Memory           |
  +---------------------------------------------------------+
                              |
                              v (Zero-copy Arrow Table wrapper)
  +---------------------------------------------------------+
  |    Polars Engine: Executes compiled LogicalPlan         |
  +---------------------------------------------------------+
                              |
                              v (Zero-copy Arrow IPC writer)
  +---------------------------------------------------------+
  |    rdkafka Producer / S3 Parquet Sink                   |
  +---------------------------------------------------------+
```

### Why Zero-Copy Matters

Traditional architectures convert streaming messages between formats multiple times:
1. `Kafka Wire Bytes -> JSON / Avro String`
2. `JSON String -> Python Dict / Object`
3. `Python Dict -> PyArrow / Pandas DataFrame`
4. `Transformations`
5. `Pandas DataFrame -> JSON String`
6. `JSON String -> Kafka Producer Buffer`

This serialization churn consumes up to 80% of CPU time and creates significant garbage collection pauses.

Biflux bypasses this entirely:
- Raw messages are parsed directly into pre-allocated **Apache Arrow RecordBatches**.
- Polars' underlying engine (`polars-core`) uses Arrow arrays natively under the hood. Converting an Arrow `RecordBatch` into a Polars `DataFrame` is an $O(1)$ pointer transfer.
- The user's compiled `LogicalPlan` executes directly on the memory buffers.
- The output Arrow array is handed to the Kafka producer without deep cloning.

---

## 2. Plan Compilation & Dispatch

1. **Authoring (Python)**:
   The developer defines a subclass of `BifluxPipeline` and overrides `transform(self, df: pl.LazyFrame) -> pl.LazyFrame`.
2. **Serialization**:
   Biflux extracts and serializes the logical execution graph.
3. **Execution Routing**:
   The `BifluxContext` inspects the execution target:
   - If `mode == "batch"`: Resolves physical Parquet/Iceberg file partitions, passes the logical plan to `biflux-core`, reads into Arrow, applies the plan, and writes Parquet to S3.
   - If `mode == "live"`: Establishes a Kafka consumer, consumes micro-batches into Arrow record batches, applies the identical logical plan, and produces to Kafka.

---

## 3. Mathematical Equivalence Guarantees

Because both batch and streaming engines evaluate the exact same Polars/Arrow computation graph, edge cases such as:
- Division-by-zero handling
- Floating-point summation order
- Null propagation semantics
- String canonicalization

execute with identical bit-for-bit behavior across historical backtests and production serving.
