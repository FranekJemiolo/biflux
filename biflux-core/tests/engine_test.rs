use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use biflux_core::arrow_buffer::{batches_to_ipc, ipc_to_batches};
use biflux_core::batch::execute_batch;
use biflux_core::plan::parse_plan_config;
use biflux_core::stream::execute_stream;
use std::sync::Arc;

#[test]
fn test_arrow_ipc_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("size", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["US-CORP-BOND-A", "US-CORP-BOND-B"])),
            Arc::new(Float64Array::from(vec![98.50, 102.10])),
            Arc::new(Float64Array::from(vec![99.10, 102.70])),
            Arc::new(Int64Array::from(vec![1000, 2500])),
        ],
    )
    .expect("Failed to create RecordBatch");

    let ipc_bytes =
        batches_to_ipc(std::slice::from_ref(&batch)).expect("Serialization to IPC failed");
    assert!(!ipc_bytes.is_empty(), "IPC bytes should not be empty");

    let restored_batches = ipc_to_batches(&ipc_bytes).expect("Deserialization from IPC failed");
    assert_eq!(restored_batches.len(), 1);
    assert_eq!(restored_batches[0].num_rows(), 2);
    assert_eq!(restored_batches[0].num_columns(), 4);
}

#[test]
fn test_batch_execute_validates_inputs() {
    let plan_bytes = b"mock_polars_plan";

    // Valid call
    let result = execute_batch(
        plan_bytes,
        "s3://lake/source",
        "s3://lake/sink",
        Some(r#"{"batch_size": 10000}"#),
    );
    assert!(result.is_ok());
    let json_str = result.unwrap();
    assert!(json_str.contains("SUCCESS"));
    assert!(json_str.contains("batch"));

    // Empty plan bytes error
    let err_plan = execute_batch(b"", "s3://lake/source", "s3://lake/sink", None);
    assert!(err_plan.is_err());

    // Empty source URI error
    let err_src = execute_batch(plan_bytes, "", "s3://lake/sink", None);
    assert!(err_src.is_err());

    // Empty sink URI error
    let err_sink = execute_batch(plan_bytes, "s3://lake/source", "", None);
    assert!(err_sink.is_err());
}

#[test]
fn test_stream_execute_validates_inputs() {
    let plan_bytes = b"mock_polars_plan";

    // Valid call
    let result = execute_stream(
        plan_bytes,
        "localhost:9092",
        "input_ticks",
        "output_ticks",
        None,
    );
    assert!(result.is_ok());
    let json_str = result.unwrap();
    assert!(json_str.contains("SUCCESS"));
    assert!(json_str.contains("live"));

    // Empty broker error
    let err_broker = execute_stream(plan_bytes, "", "input_ticks", "output_ticks", None);
    assert!(err_broker.is_err());
}

#[test]
fn test_plan_config_parsing() {
    let config = parse_plan_config(Some(r#"{"compression": "zstd", "batch_size": 32768}"#))
        .expect("Parsing failed");
    assert_eq!(config.compression.unwrap(), "zstd");
    assert_eq!(config.batch_size.unwrap(), 32768);

    // Default parsing
    let default_config = parse_plan_config(None).expect("Default parsing failed");
    assert_eq!(default_config.batch_size.unwrap(), 65536);
}
