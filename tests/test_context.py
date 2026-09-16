"""Unit tests for BifluxContext and configuration."""

import pytest

from biflux.core import BifluxContext, Credentials, Environment, ExecutionMode


def test_default_context():
    ctx = BifluxContext()
    assert ctx.mode == ExecutionMode.BATCH
    assert ctx.env == Environment.LOCAL
    assert ctx.kafka_brokers is None
    assert ctx.iceberg_catalog is None
    assert ctx.s3_endpoint is None
    assert ctx.credentials is None


def test_context_custom_values():
    ctx = BifluxContext(
        mode="live",
        env="cloud",
        kafka_brokers="b-1.msk.aws.com:9092",
        iceberg_catalog="glue://123456789012",
        s3_endpoint="http://localhost:9000",
    )
    assert ctx.mode == ExecutionMode.LIVE
    assert ctx.env == Environment.CLOUD
    assert ctx.kafka_brokers == "b-1.msk.aws.com:9092"
    assert ctx.iceberg_catalog == "glue://123456789012"
    assert ctx.s3_endpoint == "http://localhost:9000"


def test_context_case_insensitivity():
    ctx_batch = BifluxContext(mode="BATCH", env="LOCAL")
    assert ctx_batch.mode == ExecutionMode.BATCH
    assert ctx_batch.env == Environment.LOCAL

    ctx_stream = BifluxContext(mode="stream")
    assert ctx_stream.mode == ExecutionMode.LIVE


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="Invalid mode"):
        BifluxContext(mode="invalid_mode")


def test_invalid_env_raises():
    with pytest.raises(ValueError, match="Invalid environment"):
        BifluxContext(env="unsupported_env")


def test_credentials_model():
    creds = Credentials(
        aws_access_key_id="AKIAEXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        aws_region="us-west-2",
    )
    assert creds.has_aws_credentials() is True
    assert creds.has_gcp_credentials() is False
