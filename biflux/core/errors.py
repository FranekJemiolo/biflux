"""Exception hierarchy for Biflux."""

from typing import Optional


class BifluxError(Exception):
    """Base exception for all Biflux errors."""


class BifluxCredentialError(BifluxError):
    """Raised when required cloud credentials are missing or invalid."""

    def __init__(self, message: Optional[str] = None):
        default_msg = (
            "⚠️ Cloud credentials missing for Environment.CLOUD execution.\n"
            "Please provide AWS credentials via environment variables:\n"
            "  export AWS_ACCESS_KEY_ID=...\n"
            "  export AWS_SECRET_ACCESS_KEY=...\n"
            "or instantiate BifluxContext with explicit Credentials(\n"
            "  aws_access_key_id=..., aws_secret_access_key=...\n"
            ")."
        )
        super().__init__(message or default_msg)


class PlanValidationError(BifluxError):
    """Raised when a pipeline's transform method fails validation."""


class BifluxPipelineError(BifluxError):
    """Raised when a pipeline encounters a configuration or runtime error."""


class BifluxExecutionError(BifluxError):
    """Raised when the underlying execution engine fails."""
