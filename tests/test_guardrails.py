"""Unit tests for strict cloud credential guardrails."""

import os
from unittest.mock import patch

import polars as pl
import pytest

from biflux.core import (
    BifluxContext,
    BifluxCredentialError,
    BifluxPipeline,
    Credentials,
    Environment,
)


class DummyPipeline(BifluxPipeline):
    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df


def test_local_mode_does_not_require_credentials():
    ctx = BifluxContext(env=Environment.LOCAL)
    pipeline = DummyPipeline(ctx)
    # Should run without error even without any AWS credentials
    result = pipeline.run(source_uri="local_src", sink_uri="local_sink")
    assert result.status == "SUCCESS"


def test_cloud_mode_raises_when_credentials_missing(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    ctx = BifluxContext(env=Environment.CLOUD)
    pipeline = DummyPipeline(ctx)

    with pytest.raises(BifluxCredentialError, match="Cloud credentials missing"):
        pipeline.run(
            source_uri="cloud_src",
            sink_uri="cloud_sink",
            interactive_prompt=False,
        )


def test_cloud_mode_succeeds_with_explicit_credentials():
    creds = Credentials(
        aws_access_key_id="TEST_KEY",
        aws_secret_access_key="TEST_SECRET",
    )
    ctx = BifluxContext(env=Environment.CLOUD, credentials=creds)
    pipeline = DummyPipeline(ctx)
    result = pipeline.run(source_uri="cloud_src", sink_uri="cloud_sink")
    assert result.status == "SUCCESS"


def test_cloud_mode_succeeds_with_env_variables(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "MOCK_KEY_ID")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "MOCK_SECRET_KEY")

    ctx = BifluxContext(env=Environment.CLOUD)
    pipeline = DummyPipeline(ctx)
    result = pipeline.run(source_uri="cloud_src", sink_uri="cloud_sink")
    assert result.status == "SUCCESS"


def test_cloud_mode_interactive_prompt_input(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    ctx = BifluxContext(env=Environment.CLOUD)
    pipeline = DummyPipeline(ctx)

    with patch("sys.stdin.isatty", return_value=True):
        with patch("builtins.input", side_effect=["PROMPT_KEY", "PROMPT_SECRET"]):
            result = pipeline.run(
                source_uri="cloud_src",
                sink_uri="cloud_sink",
                interactive_prompt=True,
            )
            assert result.status == "SUCCESS"
            assert os.environ.get("AWS_ACCESS_KEY_ID") == "PROMPT_KEY"
            assert os.environ.get("AWS_SECRET_ACCESS_KEY") == "PROMPT_SECRET"
