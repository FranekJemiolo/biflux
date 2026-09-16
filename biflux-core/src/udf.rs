use std::sync::Arc;

use arrow::array::{Array, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::arrow_buffer::{batches_to_ipc, ipc_to_batches};

/// Apply a unary Python callable f(x: f64) -> f64 over a slice of f64 values.
pub fn apply_udf_f64_core<'py>(
    _py: Python<'py>,
    func: &Bound<'py, PyAny>,
    values: &[f64],
) -> PyResult<Vec<f64>> {
    let mut out = Vec::with_capacity(values.len());
    for &val in values {
        let res: f64 = func.call1((val,))?.extract()?;
        out.push(res);
    }
    Ok(out)
}

/// Apply a binary Python callable f(x: f64, y: f64) -> f64 over two slices of f64 values.
pub fn apply_binary_udf_f64_core<'py>(
    _py: Python<'py>,
    func: &Bound<'py, PyAny>,
    col1: &[f64],
    col2: &[f64],
) -> PyResult<Vec<f64>> {
    let len = col1.len().min(col2.len());
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let res: f64 = func.call1((col1[i], col2[i]))?.extract()?;
        out.push(res);
    }
    Ok(out)
}

/// Apply a Python callable over specific numeric columns in an Arrow IPC buffer,
/// appending a new Float64 column to each batch and returning the updated Arrow IPC buffer.
pub fn apply_udf_arrow_ipc_core<'py>(
    py: Python<'py>,
    func: &Bound<'py, PyAny>,
    input_ipc: &[u8],
    input_cols: &[String],
    output_col: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    let batches = ipc_to_batches(input_ipc)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    if batches.is_empty() {
        return Ok(PyBytes::new_bound(py, &[]));
    }

    let mut transformed_batches = Vec::with_capacity(batches.len());

    for batch in batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        // Extract input columns
        let mut col_arrays = Vec::new();
        for col_name in input_cols {
            let col_idx = schema.index_of(col_name).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "Column {} not found in Arrow batch: {}",
                    col_name, e
                ))
            })?;
            let col_arr = batch.column(col_idx);
            let f64_arr = col_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Column {} is not Float64",
                        col_name
                    ))
                })?;
            col_arrays.push(f64_arr);
        }

        // Compute output column values
        let mut result_values = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            match col_arrays.len() {
                1 => {
                    let val = col_arrays[0].value(row_idx);
                    let res: f64 = func.call1((val,))?.extract()?;
                    result_values.push(res);
                }
                2 => {
                    let v1 = col_arrays[0].value(row_idx);
                    let v2 = col_arrays[1].value(row_idx);
                    let res: f64 = func.call1((v1, v2))?.extract()?;
                    result_values.push(res);
                }
                _ => {
                    let mut args = Vec::with_capacity(col_arrays.len());
                    for arr in &col_arrays {
                        args.push(arr.value(row_idx));
                    }
                    let tuple = pyo3::types::PyTuple::new_bound(py, args);
                    let res: f64 = func.call1(tuple)?.extract()?;
                    result_values.push(res);
                }
            }
        }

        // Build new Arrow schema with appended output column
        let mut fields: Vec<Arc<Field>> = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(output_col, DataType::Float64, false)));
        let new_schema = Arc::new(Schema::new(fields));

        // Build new column list
        let mut new_columns = batch.columns().to_vec();
        new_columns.push(Arc::new(Float64Array::from(result_values)));

        let new_batch = RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        transformed_batches.push(new_batch);
    }

    let output_ipc = batches_to_ipc(&transformed_batches)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Ok(PyBytes::new_bound(py, &output_ipc))
}
