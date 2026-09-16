"""Data models and enums for Biflux."""

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class Environment(str, Enum):
    """Execution environment toggle."""

    LOCAL = "local"
    CLOUD = "cloud"


class ExecutionMode(str, Enum):
    """Pipeline execution mode."""

    BATCH = "batch"
    LIVE = "live"


class Credentials(BaseModel):
    """Cloud credentials model for AWS and GCP environments."""

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_region: str = "us-east-1"
    gcp_project_id: Optional[str] = None
    gcp_credentials_path: Optional[str] = None

    def has_aws_credentials(self) -> bool:
        """Check if AWS credentials are provided."""
        return bool(self.aws_access_key_id and self.aws_secret_access_key)

    def has_gcp_credentials(self) -> bool:
        """Check if GCP credentials are provided."""
        return bool(self.gcp_project_id or self.gcp_credentials_path)


class ExecutionResult(BaseModel):
    """Execution summary returned by pipeline.run()."""

    status: str = "SUCCESS"
    mode: str
    source_uri: str
    sink_uri: str
    duration_ms: int = 0
    rows_processed: Optional[int] = None
    messages_processed: Optional[int] = None
    details: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)
