"""Execution context and configuration for Biflux pipelines."""

import os
import sys
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, field_validator

from biflux.core.errors import BifluxCredentialError
from biflux.core.models import Credentials, Environment, ExecutionMode


class BifluxContext(BaseModel):
    """Execution context injected into a BifluxPipeline.

    Controls whether the pipeline runs in batch (S3/Iceberg) or live (Kafka),
    and whether it runs locally or against cloud infrastructure.
    """

    mode: Union[ExecutionMode, str] = Field(
        default=ExecutionMode.BATCH,
        description="Execution mode: 'batch' or 'live'",
    )
    env: Union[Environment, str] = Field(
        default=Environment.LOCAL,
        description="Environment: 'local' (MinIO/Redpanda) or 'cloud' (AWS/GCP)",
    )
    kafka_brokers: Optional[str] = Field(
        default=None,
        description="Kafka broker addresses (e.g. 'localhost:9092' or MSK endpoint)",
    )
    kafka_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional Kafka consumer/producer configurations",
    )
    iceberg_catalog: Optional[str] = Field(
        default=None,
        description="Iceberg catalog URI (e.g. 'rest://localhost:8181' or 'glue://...')",
    )
    iceberg_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional Iceberg catalog properties",
    )
    s3_endpoint: Optional[str] = Field(
        default=None,
        description="S3 endpoint URL (e.g. 'http://localhost:9000' for MinIO)",
    )
    credentials: Optional[Credentials] = Field(
        default=None,
        description="Cloud credentials for AWS or GCP",
    )
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Engine options (batch_size, compression, etc.)",
    )

    @field_validator("mode", mode="before")
    @classmethod
    def parse_mode(cls, v: Union[str, ExecutionMode]) -> ExecutionMode:
        if isinstance(v, str):
            v_lower = v.lower()
            if v_lower in ("batch", "offline", "backtest"):
                return ExecutionMode.BATCH
            if v_lower in ("live", "stream", "streaming", "realtime"):
                return ExecutionMode.LIVE
            raise ValueError(f"Invalid mode '{v}'. Must be 'batch' or 'live'.")
        return v

    @field_validator("env", mode="before")
    @classmethod
    def parse_env(cls, v: Union[str, Environment]) -> Environment:
        if isinstance(v, str):
            v_lower = v.lower()
            if v_lower in ("local", "test", "dev"):
                return Environment.LOCAL
            if v_lower in ("cloud", "prod", "production", "aws"):
                return Environment.CLOUD
            raise ValueError(f"Invalid environment '{v}'. Must be 'local' or 'cloud'.")
        return v

    def validate_cloud_guardrails(self, interactive_prompt: bool = True) -> None:
        """Strict guardrail preventing unauthenticated cloud execution.

        Checks environment variables and explicit credentials.
        Prompts interactive CLI user or raises BifluxCredentialError.
        """
        if self.env != Environment.CLOUD:
            return

        has_creds = False
        if self.credentials and (
            self.credentials.has_aws_credentials() or self.credentials.has_gcp_credentials()
        ):
            has_creds = True
        elif os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
            has_creds = True
        elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            has_creds = True

        if not has_creds:
            # Check if running in an interactive terminal and interactive prompting is enabled
            if interactive_prompt and sys.stdin and sys.stdin.isatty():
                try:
                    prompt = "⚠️ WARNING: Attempting to run against AWS. Enter AWS_ACCESS_KEY_ID: "
                    key_id = input(prompt).strip()
                    if not key_id:
                        raise BifluxCredentialError("Aborted: Empty AWS_ACCESS_KEY_ID provided.")
                    secret = input("Enter AWS_SECRET_ACCESS_KEY: ").strip()
                    if not secret:
                        raise BifluxCredentialError(
                            "Aborted: Empty AWS_SECRET_ACCESS_KEY provided."
                        )

                    os.environ["AWS_ACCESS_KEY_ID"] = key_id
                    os.environ["AWS_SECRET_ACCESS_KEY"] = secret
                    self.credentials = Credentials(
                        aws_access_key_id=key_id,
                        aws_secret_access_key=secret,
                    )
                    return
                except (EOFError, KeyboardInterrupt):
                    raise BifluxCredentialError(
                        "Cloud execution aborted by user credential prompt."
                    )
            else:
                raise BifluxCredentialError()
