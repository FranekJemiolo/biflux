"""Biflux: Unified Data Engineering Framework Eliminating Train-Serve Skew."""

from biflux.core import (
    BifluxContext,
    BifluxCredentialError,
    BifluxError,
    BifluxExecutionError,
    BifluxPipeline,
    BifluxPipelineError,
    Credentials,
    Environment,
    ExecutionMode,
    ExecutionResult,
    PlanValidationError,
    apply_udf,
    biflux_udf,
    biflux_udf_expr,
)

# Attempt to import Rust backend biflux_core
try:
    from . import biflux_core
except ImportError:
    biflux_core = None  # type: ignore

__version__ = "0.1.0"

__all__ = [
    "BifluxPipeline",
    "BifluxContext",
    "Environment",
    "ExecutionMode",
    "Credentials",
    "ExecutionResult",
    "BifluxError",
    "BifluxCredentialError",
    "BifluxPipelineError",
    "BifluxExecutionError",
    "PlanValidationError",
    "biflux_udf",
    "biflux_udf_expr",
    "apply_udf",
    "biflux_core",
    "__version__",
]
