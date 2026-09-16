"""Custom User-Defined Function (UDF) engine powered by Rust bindings."""

from functools import wraps
from typing import Any, Callable, List, Optional, Sequence, Union

import polars as pl

from biflux import biflux_core


def apply_udf_on_arrow_ipc(
    func: Callable[..., Any],
    ipc_bytes: bytes,
    input_cols: List[str],
    output_col: str,
) -> bytes:
    """Execute a Python UDF directly on an Apache Arrow IPC stream in Rust."""
    return bytes(biflux_core.apply_udf_arrow_ipc(func, ipc_bytes, input_cols, output_col))  # type: ignore[attr-defined]


def biflux_udf_expr(
    func: Callable[..., Any],
    *cols: Union[str, pl.Expr],
    output_name: Optional[str] = None,
    return_dtype: Any = pl.Float64,
) -> pl.Expr:
    """Build a Polars Expression that routes evaluation through biflux-core Rust UDF bindings.

    Supports both unary: f(col1) and binary: f(col1, col2).

    Example:
        df.with_columns(
            custom_score=biflux_udf_expr(my_pricing_func, pl.col("bid"), pl.col("ask"))
        )
    """
    col_exprs = [pl.col(c) if isinstance(c, str) else c for c in cols]

    if len(col_exprs) == 1:
        c = col_exprs[0]

        def _batch_unary_caller(s: pl.Series) -> pl.Series:
            values = s.to_list()
            # Fast-path float evaluation through Rust
            res = biflux_core.apply_udf_f64(func, values)  # type: ignore[attr-defined]
            return pl.Series(s.name, res, dtype=return_dtype)

        expr = c.map_batches(_batch_unary_caller, return_dtype=return_dtype)
    elif len(col_exprs) == 2:
        c1, c2 = col_exprs

        def _batch_binary_caller(struct_s: pl.Series) -> pl.Series:
            field_names = struct_s.struct.fields
            col1_vals = struct_s.struct.field(field_names[0]).to_list()
            col2_vals = struct_s.struct.field(field_names[1]).to_list()
            res = biflux_core.apply_binary_udf_f64(func, col1_vals, col2_vals)  # type: ignore[attr-defined]
            return pl.Series(struct_s.name, res, dtype=return_dtype)

        expr = pl.struct([c1, c2]).map_batches(_batch_binary_caller, return_dtype=return_dtype)
    else:

        def _batch_n_caller(struct_s: pl.Series) -> pl.Series:
            field_names = struct_s.struct.fields
            n_rows = len(struct_s)
            val_cols = [struct_s.struct.field(fn).to_list() for fn in field_names]
            res = []
            for i in range(n_rows):
                args = [col[i] for col in val_cols]
                res.append(func(*args))
            return pl.Series(struct_s.name, res, dtype=return_dtype)

        expr = pl.struct(col_exprs).map_batches(_batch_n_caller, return_dtype=return_dtype)

    if output_name:
        expr = expr.alias(output_name)
    return expr


def biflux_udf(
    func: Optional[Callable[..., Any]] = None,
    *,
    return_dtype: Any = pl.Float64,
) -> Any:
    """Decorator to declare a Python function as an accelerated Biflux UDF.

    Can be used both as a decorator and directly in pipeline transformations.

    Example:
        @biflux_udf
        def calc_spread_bps(bid: float, ask: float) -> float:
            mid = (bid + ask) / 2.0
            return ((ask - bid) / mid) * 10000.0

        # Inside BifluxPipeline.transform():
        df.with_columns(spread_bps=calc_spread_bps(pl.col("bid"), pl.col("ask")))
    """

    def decorator(fn: Callable[..., Any]) -> Any:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # If called with Polars expressions or column names, produce an expression
            has_expr = any(isinstance(a, (pl.Expr, str)) for a in args)
            if has_expr:
                return biflux_udf_expr(fn, *args, return_dtype=return_dtype)
            # Otherwise normal scalar invocation
            return fn(*args, **kwargs)

        wrapper._is_biflux_udf = True  # type: ignore[attr-defined]
        wrapper._raw_func = fn  # type: ignore[attr-defined]
        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


def apply_udf(
    data: Union[pl.LazyFrame, pl.DataFrame, bytes],
    func: Callable[..., Any],
    input_cols: Sequence[str],
    output_col: str,
    return_dtype: Any = pl.Float64,
) -> Union[pl.LazyFrame, pl.DataFrame, bytes]:
    """Universal UDF dispatch: executes custom user function across Batch and Streaming data.

    - On `pl.LazyFrame` (Batch Lakehouse): builds a LazyFrame plan with Rust UDF acceleration.
    - On `pl.DataFrame` (In-Memory Micro-Batch): executes the transformation immediately.
    - On `bytes` (Arrow IPC stream): executes directly in Rust using Arrow C/Rust buffers.
    """
    raw_fn = getattr(func, "_raw_func", func)

    if isinstance(data, bytes):
        return apply_udf_on_arrow_ipc(raw_fn, data, list(input_cols), output_col)

    expr = biflux_udf_expr(
        raw_fn,
        *[pl.col(c) for c in input_cols],
        output_name=output_col,
        return_dtype=return_dtype,
    )

    if isinstance(data, pl.LazyFrame):
        return data.with_columns(expr)
    elif isinstance(data, pl.DataFrame):
        return data.with_columns(expr)
    else:
        raise TypeError(f"Unsupported data type for apply_udf: {type(data).__name__}")
