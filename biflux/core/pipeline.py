"""Abstract base class and execution dispatch for Biflux pipelines."""

import json
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

import polars as pl

from biflux.core.context import BifluxContext
from biflux.core.errors import PlanValidationError
from biflux.core.models import Environment, ExecutionMode, ExecutionResult


class BifluxPipeline(ABC):
    """Abstract Base Class for Biflux unified feature engineering pipelines.

    Developers implement `transform(self, df: pl.LazyFrame) -> pl.LazyFrame`.
    The identical transformation plan is executed identically in both:
    1. Batch mode over S3/Iceberg tables.
    2. Streaming mode over Apache Kafka topics.
    """

    def __init__(self, context: Optional[BifluxContext] = None):
        self.context = context or BifluxContext()

    @abstractmethod
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """User-defined transformation graph.

        Args:
            df: An input Polars LazyFrame representing the streaming or batch schema.

        Returns:
            A Polars LazyFrame containing the transformation expressions.
        """
        raise NotImplementedError("Subclasses must implement transform(df: pl.LazyFrame)")

    def validate_plan(self, sample_df: Optional[pl.LazyFrame] = None) -> pl.LazyFrame:
        """Validates that transform() returns a valid Polars LazyFrame."""
        if sample_df is None:
            sample_df = pl.LazyFrame()

        result = self.transform(sample_df)
        if not isinstance(result, pl.LazyFrame):
            raise PlanValidationError(
                f"Expected transform() to return a polars.LazyFrame, got {type(result).__name__}. "
                "Ensure you do not call .collect() inside transform()."
            )
        return result

    def compile_plan(self, sample_df: Optional[pl.LazyFrame] = None) -> bytes:
        """Validates and compiles the transformation into serialized plan bytes."""
        lazy_plan = self.validate_plan(sample_df)
        if hasattr(lazy_plan, "serialize"):
            return lazy_plan.serialize()
        return b"polars_plan_bytes"

    def run(
        self,
        source_uri: str,
        sink_uri: str,
        sample_df: Optional[pl.LazyFrame] = None,
        interactive_prompt: bool = True,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Executes the pipeline in the configured context mode.

        Args:
            source_uri: Source Iceberg table, S3 Parquet URI, or Kafka topic.
            sink_uri: Sink Iceberg table, S3 Parquet URI, or Kafka topic.
            sample_df: Optional sample schema LazyFrame for plan validation.
            interactive_prompt: Prompt CLI user if cloud credentials missing.
            **kwargs: Extra execution options.

        Returns:
            ExecutionResult with execution metrics and status.
        """
        # 1. Enforce strict cloud credential guardrails
        self.context.validate_cloud_guardrails(interactive_prompt=interactive_prompt)

        # 2. Validate user transform and serialize logical plan
        plan_bytes = self.compile_plan(sample_df)

        # 3. Route execution based on context mode
        if self.context.mode == ExecutionMode.BATCH:
            return self._execute_batch(plan_bytes, source_uri, sink_uri, **kwargs)
        elif self.context.mode == ExecutionMode.LIVE:
            return self._execute_stream(plan_bytes, source_uri, sink_uri, **kwargs)
        else:
            raise ValueError(f"Unknown execution mode: {self.context.mode}")

    def _execute_batch(
        self,
        plan_bytes: bytes,
        source_uri: str,
        sink_uri: str,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Batch execution dispatch calling biflux-core Rust engine."""
        start_time = time.perf_counter()
        env_val = (
            self.context.env.value
            if isinstance(self.context.env, Environment)
            else str(self.context.env)
        )
        options = {
            "s3_endpoint": self.context.s3_endpoint,
            "iceberg_catalog": self.context.iceberg_catalog,
            "env": env_val,
            **self.context.options,
            **kwargs,
        }
        options_json = json.dumps(options)

        # Import Rust extension
        from biflux import biflux_core

        raw_summary = biflux_core.batch_execute(
            plan_bytes,
            source_uri,
            sink_uri,
            options_json,
        )
        parsed = json.loads(raw_summary)

        # If source_uri points to an existing file or local parquet path, execute physical plan
        rows_processed = None
        import os

        if os.path.exists(source_uri) or source_uri.endswith(".parquet"):
            try:
                input_lf = pl.scan_parquet(source_uri)
                output_df = self.transform(input_lf).collect()
                rows_processed = len(output_df)
                if sink_uri.endswith(".parquet"):
                    os.makedirs(os.path.dirname(os.path.abspath(sink_uri)) or ".", exist_ok=True)
                    output_df.write_parquet(sink_uri)
            except Exception:
                pass

        duration_ms = int((time.perf_counter() - start_time) * 1000)

        return ExecutionResult(
            status=parsed.get("status", "SUCCESS"),
            mode="batch",
            source_uri=source_uri,
            sink_uri=sink_uri,
            duration_ms=duration_ms,
            rows_processed=rows_processed,
            details=parsed.get("details", "Batch executed via biflux-core"),
            metadata=parsed,
        )

    def _execute_stream(
        self,
        plan_bytes: bytes,
        source_uri: str,
        sink_uri: str,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Stream execution dispatch calling biflux-core Rust engine."""
        start_time = time.perf_counter()
        broker = self.context.kafka_brokers or "localhost:9092"
        env_val = (
            self.context.env.value
            if isinstance(self.context.env, Environment)
            else str(self.context.env)
        )
        options = {
            "env": env_val,
            **self.context.kafka_config,
            **self.context.options,
            **kwargs,
        }
        options_json = json.dumps(options)

        # Import Rust extension
        from biflux import biflux_core

        raw_summary = biflux_core.stream_execute(
            plan_bytes,
            broker,
            source_uri,
            sink_uri,
            options_json,
        )
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        parsed = json.loads(raw_summary)

        return ExecutionResult(
            status=parsed.get("status", "SUCCESS"),
            mode="live",
            source_uri=source_uri,
            sink_uri=sink_uri,
            duration_ms=duration_ms,
            details=parsed.get("details", "Stream executed via biflux-core"),
            metadata=parsed,
        )

    def process_micro_batch(
        self,
        batch: Any,
    ) -> pl.DataFrame:
        """Process an in-memory streaming micro-batch using the identical transform logic."""
        if isinstance(batch, bytes):
            import io

            import pyarrow.ipc as ipc

            reader = ipc.open_stream(io.BytesIO(batch))
            table = reader.read_all()
            df = pl.from_arrow(table)
            if isinstance(df, pl.DataFrame):
                lf = df.lazy()
            else:
                lf = pl.DataFrame(df).lazy()
        elif isinstance(batch, pl.DataFrame):
            lf = batch.lazy()
        elif isinstance(batch, pl.LazyFrame):
            lf = batch
        else:
            raise TypeError(f"Unsupported micro-batch type: {type(batch).__name__}")

        transformed_lf = self.transform(lf)
        return transformed_lf.collect()
