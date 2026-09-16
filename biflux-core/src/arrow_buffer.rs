use crate::error::BifluxCoreError;
use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::io::Cursor;

/// Reads Arrow IPC stream bytes into a vector of RecordBatch
pub fn ipc_to_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>, BifluxCoreError> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result?);
    }
    Ok(batches)
}

/// Writes a slice of RecordBatch into an Arrow IPC stream byte vector
pub fn batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, BifluxCoreError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(output)
}
