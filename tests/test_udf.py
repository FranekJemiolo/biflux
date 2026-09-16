"""Unit and regression tests for custom Python UDFs with Rust bindings."""

import io

import polars as pl
import pyarrow.ipc as ipc
import pytest

from biflux import (
    BifluxContext,
    BifluxPipeline,
    ExecutionMode,
    apply_udf,
    biflux_core,
    biflux_udf,
    biflux_udf_expr,
)


@biflux_udf
def square_val(x: float) -> float:
    return x * x


@biflux_udf
def weighted_mid(bid: float, ask: float) -> float:
    return (bid * 0.4) + (ask * 0.6)


@biflux_udf
def tri_sum(a: float, b: float, c: float) -> float:
    return a + b + c


class UDFPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.with_columns(
            sq=biflux_udf_expr(square_val, pl.col("bid")),
            mid=biflux_udf_expr(weighted_mid, pl.col("bid"), pl.col("ask")),
        )


def test_biflux_core_scalar_and_binary_udf():
    # Unary f64
    res1 = biflux_core.apply_udf_f64(lambda x: x * 3.0, [1.0, 2.0, 3.0])
    assert res1 == [3.0, 6.0, 9.0]

    # Binary f64
    res2 = biflux_core.apply_binary_udf_f64(lambda x, y: x - y, [10.0, 20.0], [3.0, 5.0])
    assert res2 == [7.0, 15.0]


def test_biflux_core_arrow_ipc_udf():
    df = pl.DataFrame({"x": [2.0, 4.0], "y": [10.0, 20.0]})
    buf = io.BytesIO()
    with ipc.new_stream(buf, df.to_arrow().schema) as writer:
        writer.write_table(df.to_arrow())
    in_ipc = buf.getvalue()

    out_ipc = biflux_core.apply_udf_arrow_ipc(lambda x, y: x * y, in_ipc, ["x", "y"], "product")
    out_table = ipc.open_stream(io.BytesIO(out_ipc)).read_all()
    out_df = pl.from_arrow(out_table)
    assert "product" in out_df.columns
    assert out_df["product"].to_list() == [20.0, 80.0]


def test_biflux_udf_decorator_and_expression():
    df = pl.DataFrame({"bid": [10.0, 20.0], "ask": [12.0, 22.0], "c": [1.0, 2.0]})

    # Unary
    res1 = df.lazy().with_columns(sq=square_val(pl.col("bid"))).collect()
    assert res1["sq"].to_list() == [100.0, 400.0]

    # Binary
    res2 = df.lazy().with_columns(mid=weighted_mid(pl.col("bid"), pl.col("ask"))).collect()
    assert res2["mid"].to_list() == [11.2, 21.2]

    # N-ary (3 columns)
    res3 = df.lazy().with_columns(tri=tri_sum(pl.col("bid"), pl.col("ask"), pl.col("c"))).collect()
    assert res3["tri"].to_list() == [23.0, 44.0]


def test_apply_udf_helper():
    df = pl.DataFrame({"bid": [5.0, 8.0], "ask": [6.0, 10.0]})

    # LazyFrame
    res_lazy = apply_udf(df.lazy(), weighted_mid, ["bid", "ask"], "out").collect()
    assert res_lazy["out"].to_list() == [5.6, 9.2]

    # DataFrame
    res_df = apply_udf(df, weighted_mid, ["bid", "ask"], "out")
    assert isinstance(res_df, pl.DataFrame)
    assert res_df["out"].to_list() == [5.6, 9.2]

    # Arrow IPC bytes
    buf = io.BytesIO()
    with ipc.new_stream(buf, df.to_arrow().schema) as writer:
        writer.write_table(df.to_arrow())
    in_ipc = buf.getvalue()

    out_ipc = apply_udf(in_ipc, weighted_mid, ["bid", "ask"], "out")
    assert isinstance(out_ipc, bytes)
    out_table = ipc.open_stream(io.BytesIO(out_ipc)).read_all()
    assert "out" in out_table.column_names


def test_udf_pipeline_batch_and_streaming_parity():
    df = pl.DataFrame({"bid": [100.0, 200.0, 300.0], "ask": [105.0, 205.0, 305.0]})

    batch_ctx = BifluxContext(mode=ExecutionMode.BATCH)
    batch_pipe = UDFPipeline(batch_ctx)
    batch_res = batch_pipe.transform(df.lazy()).collect()

    live_ctx = BifluxContext(mode=ExecutionMode.LIVE)
    live_pipe = UDFPipeline(live_ctx)
    live_res = live_pipe.process_micro_batch(df)

    assert batch_res["sq"].to_list() == live_res["sq"].to_list()
    assert batch_res["mid"].to_list() == live_res["mid"].to_list()


def test_udf_exception_handling():
    @biflux_udf
    def failing_fn(x: float) -> float:
        if x < 0:
            raise ValueError("Negative number not allowed")
        return x

    df = pl.DataFrame({"bid": [-1.0, 2.0]})
    with pytest.raises(Exception):
        _ = df.lazy().with_columns(res=failing_fn(pl.col("bid"))).collect()
