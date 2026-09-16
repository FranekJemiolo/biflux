# Performance & Latency Benchmarks

Biflux is engineered for low latency and high analytical throughput by compiling Python Polars logical plans into a unified Apache Arrow in-memory engine.

---

## 🚀 Benchmark Highlights

- **Peak Batch Throughput**: **4,129,685 rows/sec** (1,000,000 rows scanned, aggregated, and written to Parquet in 242 ms).
- **Sub-Millisecond Streaming Latency**: **0.24 ms** P50 latency on 100-row micro-batches; **0.25 ms** on 1,000-row micro-batches.
- **Peak Streaming Micro-batch Throughput**: Up to **88,862,511 rows/sec**.
- **Rust Arrow IPC Memory Transfer Bandwidth**: **668.4 MB/s** (16,487,434 records/sec zero-copy in-memory roundtrip).

---

## 📊 Cross-Framework Multi-Scale Comparison (Biflux vs. Polars, DuckDB, Pandas)

### 1. Batch Backtest Scaling Across Sample Sizes

| Scale | Biflux (Unified) | Polars (Standalone) | DuckDB | Pandas |
| :--- | :--- | :--- | :--- | :--- |
| **100K rows** | **0.96 ms** (104.2M/s) | 0.91 ms (110.4M/s) | 1.35 ms (74.1M/s) | 2.52 ms (39.7M/s) |
| **500K rows** | **4.28 ms** (116.9M/s) | 4.11 ms (121.6M/s) | 5.01 ms (99.8M/s) | 7.76 ms (64.5M/s) |
| **1M rows** | **9.56 ms** (104.6M/s) | 8.90 ms (112.4M/s) | 9.81 ms (101.9M/s) | 14.70 ms (68.0M/s) |

### 2. Streaming Micro-Batch Latency (P50) Across Sample Sizes

| Micro-Batch Size | Biflux (Streaming) | Polars (Micro-Batch) | DuckDB (Micro-Batch) | Native Python | Pandas |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **500 rows** | **0.18 ms** (2.7M/s) | 0.19 ms (2.6M/s) | 0.58 ms (0.86M/s) | 0.06 ms (9.1M/s) | 1.20 ms (0.42M/s) |
| **2,000 rows** | **0.26 ms** (7.6M/s) | 0.25 ms (8.0M/s) | 0.57 ms (3.5M/s) | 0.22 ms (9.1M/s) | 1.23 ms (1.6M/s) |
| **10,000 rows** | **0.30 ms** (33.5M/s) | 0.30 ms (33.1M/s) | 0.63 ms (15.8M/s) | 1.08 ms (9.3M/s) | 1.39 ms (7.2M/s) |
| **50,000 rows** | **0.62 ms** (80.6M/s) | 0.58 ms (85.7M/s) | 1.17 ms (42.8M/s) | 5.50 ms (9.1M/s) | 1.78 ms (28.1M/s) |

### 3. Batch vs. Streaming on Identical Sample Sizes

| Sample Size | Biflux (Batch) | Biflux (Streaming) | Polars (Standalone) | DuckDB | Pandas |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1,000 rows** | **0.22 ms** | **0.18 ms** | 0.16 ms | 0.45 ms | 1.14 ms |
| **10,000 rows** | **0.28 ms** | **0.29 ms** | 0.34 ms | 0.55 ms | 1.22 ms |
| **100,000 rows**| **0.88 ms** | **0.83 ms** | 0.79 ms | 1.30 ms | 2.36 ms |

*For complete comparative methodology and train-serve skew elimination analysis, see [Comparative Analysis](comparative_analysis.md).*

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

## 4. All Domain Pipeline Examples Empirical Benchmark

Evaluating all production domain pipelines across batch and live streaming modes (from `benchmarks/benchmark_all_examples.py`):

| Pipeline Example | Domain Application | Batch Latency (50K) | Batch Throughput | Streaming P50 (1K) | Streaming Throughput | Max Train-Serve Skew |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Bond Pricing VWAP** | Fixed Income Analytics | **0.67 ms** | **74.7M rows/s** | **0.26 ms** | **3.9M rows/s** | **0.000000% (Exact Parity)** |
| **Fraud Risk Scoring** | Payment Fraud Systems | **1.38 ms** | **36.1M rows/s** | **0.41 ms** | **2.4M rows/s** | **0.000000% (Exact Parity)** |
| **L2 Orderbook Depth** | HFT Microstructure | **0.77 ms** | **64.6M rows/s** | **0.30 ms** | **3.3M rows/s** | **0.000000% (Exact Parity)** |
| **IoT Predictive Health**| Industrial Turbines | **1.00 ms** | **49.8M rows/s** | **0.38 ms** | **2.7M rows/s** | **0.000000% (Exact Parity)** |
| **Option Risk (Rust UDF)**| Options Pricing & Greeks | **71.54 ms** | **698.9K rows/s** | **1.71 ms** | **585.2K rows/s** | **0.000000% (Exact Parity)** |

---

## 5. Custom Python UDF Acceleration Engine

Evaluating custom Python mathematical functions executed via Rust/PyO3 bindings (from `benchmarks/benchmark_udf.py`):

### Batch Mode UDF Scaling
| Dataset Scale | Execution Time (ms) | Processing Throughput |
| :--- | :--- | :--- |
| **10,000 records** | **5.29 ms** | **1,890,166 rows/s** |
| **100,000 records** | **49.58 ms** | **2,016,836 rows/s** |
| **500,000 records** | **251.33 ms** | **1,989,432 rows/s** |

### Streaming Micro-Batch UDF Latency
| Micro-Batch Size | P50 Latency (ms) | Streaming Throughput |
| :--- | :--- | :--- |
| **500 records** | **0.54 ms** | **934,071 rows/s** |
| **2,000 records** | **1.49 ms** | **1,342,057 rows/s** |
| **10,000 records** | **5.43 ms** | **1,842,610 rows/s** |

- **Direct PyO3 Rust UDF Vector Latency**: **46.84 ms** for 100,000 records (**2,134,967 rows/s**).

---

## Reproducing Benchmarks

Run the benchmark suites locally:

```bash
# Internal throughput and Arrow IPC
python benchmarks/benchmark_throughput.py

# Multi-scale cross-framework comparison (Biflux vs Polars, DuckDB, Pandas)
python benchmarks/comparative_benchmark.py

# All domain pipeline examples
python benchmarks/benchmark_all_examples.py

# Custom Python UDF acceleration engine
python benchmarks/benchmark_udf.py
```
