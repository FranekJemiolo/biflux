# Performance & Latency Benchmarks

Biflux is engineered for low latency and high analytical throughput by compiling Python Polars logical plans into a unified Apache Arrow in-memory engine.

---

## 🚀 Benchmark Highlights

- **Peak Batch Throughput**: **4,129,685 rows/sec** (1,000,000 rows scanned, aggregated, and written to Parquet in 242 ms).
- **Sub-Millisecond Streaming Latency**: **0.24 ms** P50 latency on 100-row micro-batches; **0.25 ms** on 1,000-row micro-batches.
- **Peak Streaming Micro-batch Throughput**: Up to **88,862,511 rows/sec**.
- **Rust Arrow IPC Memory Transfer Bandwidth**: **668.4 MB/s** (16,487,434 records/sec zero-copy in-memory roundtrip).

---

## 1. Historical Batch Engine Throughput

Evaluated on VWAP aggregation over synthetic bond quote tables in Apache Parquet format:

| Scale (Records) | Dataset Size | Execution Time | Processing Throughput |
| :--- | :--- | :--- | :--- |
| **100,000 rows** | 0.1 MB | **24.5 ms** | **4,075,519 rows/s** |
| **500,000 rows** | 0.5 MB | **132.1 ms** | **3,786,130 rows/s** |
| **1,000,000 rows** | 1.1 MB | **242.1 ms** | **4,129,685 rows/s** |

---

## 2. Real-Time Streaming Micro-Batch Latency

Evaluated over 20 iterations per batch size using native in-memory Apache Arrow record batches:

| Micro-Batch Size | P50 Latency | P90 Latency | P99 Latency | Effective Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **100 rows** | **0.24 ms** | 0.41 ms | 0.44 ms | 408,510 rows/s |
| **500 rows** | **0.25 ms** | 0.29 ms | 0.32 ms | 1,979,876 rows/s |
| **1,000 rows** | **0.25 ms** | 0.32 ms | 0.34 ms | 4,022,801 rows/s |
| **5,000 rows** | **0.31 ms** | 0.35 ms | 0.44 ms | 16,382,270 rows/s |
| **10,000 rows** | **0.31 ms** | 0.38 ms | 0.39 ms | 32,318,952 rows/s |
| **50,000 rows** | **0.56 ms** | 0.68 ms | 0.98 ms | 88,862,511 rows/s |

---

## 3. Rust Zero-Copy Arrow IPC Engine

Direct memory evaluation between the Python API and `biflux-core` Rust engine via PyO3:

| Metric | Result |
| :--- | :--- |
| **Record Batch Size** | 100,000 records |
| **In-Memory Buffer Size** | 4.05 MB |
| **FFI Roundtrip Duration** | **6.07 ms** |
| **Arrow Memory Bandwidth** | **668.4 MB/s** |
| **FFI Record Ingestion Rate**| **16,487,434 records/s** |

---

## Reproducing Benchmarks

Run the benchmark suite locally:

```bash
python benchmarks/benchmark_throughput.py
```
