use crate::error::BifluxCoreError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanConfig {
    pub source_format: Option<String>,
    pub sink_format: Option<String>,
    pub batch_size: Option<usize>,
    pub compression: Option<String>,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            source_format: Some("parquet".to_string()),
            sink_format: Some("parquet".to_string()),
            batch_size: Some(65536),
            compression: Some("snappy".to_string()),
        }
    }
}

pub fn parse_plan_config(options_json: Option<&str>) -> Result<PlanConfig, BifluxCoreError> {
    match options_json {
        Some(json_str) if !json_str.trim().is_empty() => {
            let config: PlanConfig = serde_json::from_str(json_str)?;
            Ok(config)
        }
        _ => Ok(PlanConfig::default()),
    }
}
