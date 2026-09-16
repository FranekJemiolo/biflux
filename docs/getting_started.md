# Getting Started with Biflux

## Installation

Install via pip:

```bash
pip install biflux
```

Or build locally with `maturin`:

```bash
cd biflux
maturin develop --uv
```

---

## Defining a Pipeline

Every Biflux pipeline subclasses `BifluxPipeline` and implements `transform()`:

```python
import polars as pl
from biflux.core import BifluxPipeline

class CleanTradesPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.filter(pl.col("price") > 0.0)
            .filter(pl.col("volume") > 0)
            .with_columns(
                notional=pl.col("price") * pl.col("volume")
            )
        )
```

---

## Configuring the Context

The `BifluxContext` determines the runtime environment:

### Local Batch Backtest

```python
from biflux.core import BifluxContext, Environment

ctx = BifluxContext(
    mode="batch",
    env=Environment.LOCAL,
    s3_endpoint="http://localhost:9000",
)

pipeline = CleanTradesPipeline(ctx)
result = pipeline.run(
    source_uri="s3://lake/trades/raw/",
    sink_uri="s3://lake/trades/cleaned/",
)
```

### Local Live Stream

```python
ctx = BifluxContext(
    mode="live",
    env=Environment.LOCAL,
    kafka_brokers="localhost:9092",
)

pipeline = CleanTradesPipeline(ctx)
result = pipeline.run(
    source_uri="trades_raw_topic",
    sink_uri="trades_clean_topic",
)
```
