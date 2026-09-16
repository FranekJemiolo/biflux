#![allow(clippy::useless_conversion)]

use pyo3::prelude::*;

pub mod arrow_buffer;
pub mod batch;
pub mod error;
pub mod plan;
pub mod stream;
pub mod udf;

use error::BifluxCoreError;

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pyfunction]
#[pyo3(signature = (plan_bytes, source_uri, sink_uri, options=None))]
fn batch_execute(
    plan_bytes: &[u8],
    source_uri: &str,
    sink_uri: &str,
    options: Option<&str>,
) -> PyResult<String> {
    Ok(batch::execute_batch(
        plan_bytes, source_uri, sink_uri, options,
    )?)
}

#[pyfunction]
#[pyo3(signature = (plan_bytes, broker, input_topic, output_topic, options=None))]
fn stream_execute(
    plan_bytes: &[u8],
    broker: &str,
    input_topic: &str,
    output_topic: &str,
    options: Option<&str>,
) -> PyResult<String> {
    Ok(stream::execute_stream(
        plan_bytes,
        broker,
        input_topic,
        output_topic,
        options,
    )?)
}

use pyo3::types::PyBytes;

#[pyfunction]
fn execute_arrow_ipc<'py>(
    py: Python<'py>,
    plan_bytes: &[u8],
    input_ipc: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = arrow_buffer::ipc_to_batches(input_ipc)?;
    if plan_bytes.is_empty() {
        return Err(BifluxCoreError::Execution("Empty plan bytes".to_string()).into());
    }
    let output = arrow_buffer::batches_to_ipc(&batches)?;
    Ok(PyBytes::new_bound(py, &output))
}

#[pyfunction]
fn apply_udf_f64<'py>(
    py: Python<'py>,
    func: &Bound<'py, PyAny>,
    values: Vec<f64>,
) -> PyResult<Vec<f64>> {
    udf::apply_udf_f64_core(py, func, &values)
}

#[pyfunction]
fn apply_binary_udf_f64<'py>(
    py: Python<'py>,
    func: &Bound<'py, PyAny>,
    col1: Vec<f64>,
    col2: Vec<f64>,
) -> PyResult<Vec<f64>> {
    udf::apply_binary_udf_f64_core(py, func, &col1, &col2)
}

#[pyfunction]
fn apply_udf_arrow_ipc<'py>(
    py: Python<'py>,
    func: &Bound<'py, PyAny>,
    input_ipc: &[u8],
    input_cols: Vec<String>,
    output_col: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    udf::apply_udf_arrow_ipc_core(py, func, input_ipc, &input_cols, output_col)
}

#[pymodule]
fn biflux_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(batch_execute, m)?)?;
    m.add_function(wrap_pyfunction!(stream_execute, m)?)?;
    m.add_function(wrap_pyfunction!(execute_arrow_ipc, m)?)?;
    m.add_function(wrap_pyfunction!(apply_udf_f64, m)?)?;
    m.add_function(wrap_pyfunction!(apply_binary_udf_f64, m)?)?;
    m.add_function(wrap_pyfunction!(apply_udf_arrow_ipc, m)?)?;
    Ok(())
}
