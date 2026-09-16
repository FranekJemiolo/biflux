"""Type stubs for biflux_core Rust PyO3 extension module."""

from typing import Optional

def version() -> str: ...
def batch_execute(
    plan_bytes: bytes,
    source_uri: str,
    sink_uri: str,
    options: Optional[str] = None,
) -> str: ...
def stream_execute(
    plan_bytes: bytes,
    broker: str,
    input_topic: str,
    output_topic: str,
    options: Optional[str] = None,
) -> str: ...
def execute_arrow_ipc(
    plan_bytes: bytes,
    input_ipc: bytes,
) -> bytes: ...
