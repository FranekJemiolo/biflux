"""Integration tests for streaming micro-batch processing."""

import io

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc

from biflux import biflux_core
from biflux.core import BifluxContext, BifluxPipeline, ExecutionMode


class StreamingTestPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.with_columns(spread=pl.col("ask") - pl.col("bid"))
            .filter(pl.col("spread") > 0.0)
            .sort("symbol")
        )


def test_stream_micro_batch_from_dataframe():
    ctx = BifluxContext(mode=ExecutionMode.LIVE, kafka_brokers="localhost:9092")
    pipeline = StreamingTestPipeline(ctx)

    micro_batch = pl.DataFrame(
        {
            "symbol": ["BOND_X", "BOND_Y"],
            "bid": [98.50, 101.20],
            "ask": [99.00, 101.80],
        }
    )

    result_df = pipeline.process_micro_batch(micro_batch)
    assert result_df.shape == (2, 4)
    assert "spread" in result_df.columns
    assert round(result_df["spread"][0], 2) == 0.50
    assert round(result_df["spread"][1], 2) == 0.60


def test_stream_micro_batch_from_arrow_ipc():
    ctx = BifluxContext(mode=ExecutionMode.LIVE)
    pipeline = StreamingTestPipeline(ctx)

    # Build an Arrow RecordBatch and serialize to IPC stream bytes
    schema = pa.schema(
        [
            pa.field("symbol", pa.string()),
            pa.field("bid", pa.float64()),
            pa.field("ask", pa.float64()),
        ]
    )
    arrow_table = pa.Table.from_arrays(
        [
            pa.array(["BOND_M", "BOND_N"]),
            pa.array([95.0, 105.0]),
            pa.array([96.0, 105.5]),
        ],
        schema=schema,
    )

    sink = io.BytesIO()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_table(arrow_table)
    ipc_bytes = sink.getvalue()

    # Pass IPC stream bytes to process_micro_batch
    result_df = pipeline.process_micro_batch(ipc_bytes)
    assert result_df.shape == (2, 4)
    assert result_df["symbol"].to_list() == ["BOND_M", "BOND_N"]
    assert result_df["spread"].to_list() == [1.0, 0.5]


def test_rust_execute_arrow_ipc_ffi():
    # Test low-level Rust FFI Arrow IPC roundtrip
    schema = pa.schema(
        [
            pa.field("symbol", pa.string()),
            pa.field("bid", pa.float64()),
            pa.field("ask", pa.float64()),
        ]
    )
    arrow_table = pa.Table.from_arrays(
        [
            pa.array(["BOND_1"]),
            pa.array([100.0]),
            pa.array([101.0]),
        ],
        schema=schema,
    )

    sink = io.BytesIO()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_table(arrow_table)
    ipc_bytes = sink.getvalue()

    output_ipc = biflux_core.execute_arrow_ipc(b"dummy_plan", ipc_bytes)
    assert len(output_ipc) > 0

    reader = ipc.open_stream(io.BytesIO(output_ipc))
    restored_table = reader.read_all()
    assert restored_table.num_rows == 1
    assert restored_table.num_columns == 3
