//! Arrow IPC stream ingestion (D-VZ-1 lane V1's third charter-named source,
//! behind the optional `arrow-ipc` feature). A default `viz-columnstore` build
//! links none of this — only a caller who enables `arrow-ipc` pulls the `arrow`
//! crate's array/buffer/schema/ipc family.
//!
//! Reads a real Arrow **IPC stream** (`arrow::ipc::reader::StreamReader`, the
//! wire format `pyarrow.ipc.new_stream`/`RecordBatchStreamWriter` produce) and
//! ingests every batch's `Float64`/`Int64`/`Utf8`/`Boolean` columns into a
//! [`crate::store::ColumnStore`] dataset via the SAME [`ColumnStore::ingest_columns`]
//! entry point every other ingest path uses — Arrow is a source format, not a
//! second storage representation.

use std::io::Cursor;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use crate::error::ColumnStoreError;
use crate::store::ColumnStore;
use crate::types::{ColumnData, ColumnInput};

fn ipc_err(e: impl std::fmt::Display) -> ColumnStoreError {
    ColumnStoreError::ArrowIpc(e.to_string())
}

/// Ingest every record batch in `stream_bytes` (a complete Arrow IPC stream) as
/// one dataset under `dataset_ref`. Multiple batches with the same schema
/// concatenate row-wise, matching how a streamed query result arrives in pages.
pub fn ingest_stream(
    store: &mut ColumnStore,
    dataset_ref: &str,
    stream_bytes: &[u8],
) -> Result<String, ColumnStoreError> {
    let cursor = Cursor::new(stream_bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(ipc_err)?;
    let schema = reader.schema();

    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(ipc_err)?);
    }
    if batches.is_empty() {
        return Err(ColumnStoreError::ArrowIpc(
            "empty Arrow IPC stream: no record batches".to_string(),
        ));
    }

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name().as_str();
        let input = match field.data_type() {
            DataType::Float64 => ingest_numeric(&batches, name, |a: &Float64Array, i| a.value(i))?,
            DataType::Int64 => {
                ingest_numeric_as(&batches, name, |a: &Int64Array, i| a.value(i) as f64)?
            }
            DataType::Utf8 => ingest_utf8(&batches, name)?,
            DataType::Boolean => ingest_bool(&batches, name)?,
            other => {
                return Err(ColumnStoreError::ArrowIpc(format!(
                    "column `{name}`: unsupported Arrow DataType {other:?} (supported: Float64, Int64, Utf8, Boolean)"
                )))
            }
        };
        columns.push(input);
    }

    store.ingest_columns(dataset_ref, columns)
}

fn column_array<'a, A: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a A, ColumnStoreError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ColumnStoreError::ArrowIpc(format!("missing column `{name}`")))?
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| {
            ColumnStoreError::ArrowIpc(format!("column `{name}` has an unexpected array type"))
        })
}

fn ingest_numeric(
    batches: &[RecordBatch],
    name: &str,
    value_at: impl Fn(&Float64Array, usize) -> f64,
) -> Result<ColumnInput, ColumnStoreError> {
    let mut values = Vec::new();
    let mut validity = Vec::new();
    let mut any_null = false;
    for batch in batches {
        let arr: &Float64Array = column_array(batch, name)?;
        for i in 0..arr.len() {
            let valid = arr.is_valid(i);
            any_null |= !valid;
            values.push(if valid { value_at(arr, i) } else { 0.0 });
            validity.push(valid);
        }
    }
    let mut input = ColumnInput::new(name, ColumnData::F64(values));
    if any_null {
        input = input.nullable(validity);
    }
    Ok(input)
}

fn ingest_numeric_as(
    batches: &[RecordBatch],
    name: &str,
    value_at: impl Fn(&Int64Array, usize) -> f64,
) -> Result<ColumnInput, ColumnStoreError> {
    let mut values = Vec::new();
    let mut validity = Vec::new();
    let mut any_null = false;
    for batch in batches {
        let arr: &Int64Array = column_array(batch, name)?;
        for i in 0..arr.len() {
            let valid = arr.is_valid(i);
            any_null |= !valid;
            values.push(if valid { value_at(arr, i) } else { 0.0 });
            validity.push(valid);
        }
    }
    let mut input = ColumnInput::new(name, ColumnData::F64(values));
    if any_null {
        input = input.nullable(validity);
    }
    Ok(input)
}

fn ingest_utf8(batches: &[RecordBatch], name: &str) -> Result<ColumnInput, ColumnStoreError> {
    let mut values = Vec::new();
    let mut validity = Vec::new();
    let mut any_null = false;
    for batch in batches {
        let arr: &StringArray = column_array(batch, name)?;
        for i in 0..arr.len() {
            let valid = arr.is_valid(i);
            any_null |= !valid;
            values.push(if valid {
                arr.value(i).to_string()
            } else {
                String::new()
            });
            validity.push(valid);
        }
    }
    let mut input = ColumnInput::new(name, ColumnData::Utf8(values));
    if any_null {
        input = input.nullable(validity);
    }
    Ok(input)
}

fn ingest_bool(batches: &[RecordBatch], name: &str) -> Result<ColumnInput, ColumnStoreError> {
    let mut values = Vec::new();
    let mut validity = Vec::new();
    let mut any_null = false;
    for batch in batches {
        let arr: &BooleanArray = column_array(batch, name)?;
        for i in 0..arr.len() {
            let valid = arr.is_valid(i);
            any_null |= !valid;
            values.push(valid && arr.value(i));
            validity.push(valid);
        }
    }
    let mut input = ColumnInput::new(name, ColumnData::Bool(values));
    if any_null {
        input = input.nullable(validity);
    }
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Float64Array as F64Arr, Int64Array as I64Arr, StringArray as Utf8Arr,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use std::sync::Arc;

    fn build_stream_bytes() -> Vec<u8> {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("n", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        let x: ArrayRef = Arc::new(F64Arr::from(vec![1.0, 2.0, 3.0]));
        let n: ArrayRef = Arc::new(I64Arr::from(vec![10, 20, 30]));
        let label: ArrayRef = Arc::new(Utf8Arr::from(vec!["a", "b", "c"]));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![x, n, label]).unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        buffer
    }

    #[test]
    fn ingests_a_real_arrow_ipc_stream_end_to_end() {
        let bytes = build_stream_bytes();
        let mut store = ColumnStore::new();
        ingest_stream(&mut store, "ds:arrow", &bytes).unwrap();

        use eg_viz_core::ColumnStoreIngest;
        assert_eq!(ColumnStoreIngest::row_count(&store, "ds:arrow").unwrap(), 3);
        assert_eq!(
            store.materialize_f64("ds:arrow", "x").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
        assert_eq!(
            store.materialize_f64("ds:arrow", "n").unwrap(),
            vec![10.0, 20.0, 30.0]
        );
        assert_eq!(
            store.materialize_utf8("ds:arrow", "label").unwrap(),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }
}
