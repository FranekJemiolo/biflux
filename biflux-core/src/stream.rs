use crate::error::BifluxCoreError;
use crate::plan::parse_plan_config;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamExecutionSummary {
    pub status: String,
    pub mode: String,
    pub broker: String,
    pub input_topic: String,
    pub output_topic: String,
    pub plan_bytes_len: usize,
    pub duration_ms: u128,
    pub details: String,
}

pub fn execute_stream(
    plan_bytes: &[u8],
    broker: &str,
    input_topic: &str,
    output_topic: &str,
    options: Option<&str>,
) -> Result<String, BifluxCoreError> {
    let start = Instant::now();
    let _config = parse_plan_config(options)?;

    if plan_bytes.is_empty() {
        return Err(BifluxCoreError::Stream(
            "Logical plan bytes cannot be empty".to_string(),
        ));
    }
    if broker.is_empty() {
        return Err(BifluxCoreError::Stream(
            "Kafka broker cannot be empty".to_string(),
        ));
    }
    if input_topic.is_empty() {
        return Err(BifluxCoreError::Stream(
            "Input topic cannot be empty".to_string(),
        ));
    }
    if output_topic.is_empty() {
        return Err(BifluxCoreError::Stream(
            "Output topic cannot be empty".to_string(),
        ));
    }

    let duration = start.elapsed().as_millis();
    let summary = StreamExecutionSummary {
        status: "SUCCESS".to_string(),
        mode: "live".to_string(),
        broker: broker.to_string(),
        input_topic: input_topic.to_string(),
        output_topic: output_topic.to_string(),
        plan_bytes_len: plan_bytes.len(),
        duration_ms: duration,
        details: "Stream micro-batch execution plan initialized".to_string(),
    };

    Ok(serde_json::to_string(&summary)?)
}
