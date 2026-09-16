use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BifluxCoreError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Plan execution error: {0}")]
    Execution(String),

    #[error("Stream error: {0}")]
    Stream(String),

    #[error("Batch error: {0}")]
    Batch(String),
}

impl From<BifluxCoreError> for PyErr {
    fn from(err: BifluxCoreError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}
