//! Parquet materialization + read-back (CONCEPT:EG-317).
//!
//! Transcodes a [`LakeBatch`] (the engine's columnar/table data, neutralized in
//! `schema.rs`) into a single Parquet file's bytes, and reads Parquet bytes back into
//! a [`LakeBatch`]. This is the ONLY module that pulls arrow + parquet, so it is the
//! ONLY thing the `lake` feature gates — the Delta/Iceberg logs, the catalog and the
//! LSN snapshot are pure JSON and compile without it.
//!
//! The Arrow type map is fixed so the file an external engine (Spark/Trino/DuckDB)
//! reads carries exactly the logical types the Delta/Iceberg schema advertises:
//! `Long → Int64`, `Double → Float64`, `Bool → Boolean`, `String → Utf8`,
//! `Timestamp → Timestamp(Microsecond, UTC)`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};

/// The Arrow [`DataType`] a [`LakeType`] maps to (CONCEPT:EG-317). A UTC-zoned
/// micro-timestamp is what Delta/Iceberg's `timestamptz` expects.
fn arrow_type(ty: LakeType) -> DataType {
    match ty {
        LakeType::Long => DataType::Int64,
        LakeType::Double => DataType::Float64,
        LakeType::Bool => DataType::Boolean,
        LakeType::String => DataType::Utf8,
        LakeType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    }
}

/// Build the Arrow [`Schema`] mirroring a [`LakeSchema`] (CONCEPT:EG-317).
fn arrow_schema(schema: &LakeSchema) -> Schema {
    let fields: Vec<Field> = schema
        .fields
        .iter()
        .map(|f| Field::new(&f.name, arrow_type(f.ty), f.nullable))
        .collect();
    Schema::new(fields)
}

/// Explode one column (column index `col`) of a row-major batch into a typed Arrow
/// array, honoring nulls. A cell whose runtime variant disagrees with the schema type
/// is treated as null for that column (schema-on-read tolerance, CONCEPT:EG-317).
fn build_column(batch: &LakeBatch, col: usize, ty: LakeType) -> ArrayRef {
    match ty {
        LakeType::Long => {
            let it = batch.rows.iter().map(|r| match &r[col] {
                CellValue::Long(v) => Some(*v),
                _ => None,
            });
            Arc::new(Int64Array::from_iter(it))
        }
        LakeType::Double => {
            let it = batch.rows.iter().map(|r| match &r[col] {
                CellValue::Double(v) => Some(*v),
                _ => None,
            });
            Arc::new(Float64Array::from_iter(it))
        }
        LakeType::Bool => {
            let it = batch.rows.iter().map(|r| match &r[col] {
                CellValue::Bool(v) => Some(*v),
                _ => None,
            });
            Arc::new(BooleanArray::from_iter(it))
        }
        LakeType::String => {
            let vals: Vec<Option<String>> = batch
                .rows
                .iter()
                .map(|r| match &r[col] {
                    CellValue::String(v) => Some(v.clone()),
                    _ => None,
                })
                .collect();
            Arc::new(StringArray::from(vals))
        }
        LakeType::Timestamp => {
            let it = batch.rows.iter().map(|r| match &r[col] {
                CellValue::Timestamp(v) => Some(*v),
                _ => None,
            });
            let arr = TimestampMicrosecondArray::from_iter(it).with_timezone("UTC");
            Arc::new(arr)
        }
    }
}

/// Transcode a [`LakeBatch`] into one Parquet file's bytes (CONCEPT:EG-317).
///
/// This is the core materialization primitive — `materialize_table(rows) -> parquet
/// bytes`. The bytes are a self-describing Parquet file (schema in the footer) that
/// any lakehouse reader opens directly; the Delta/Iceberg log then points at it.
pub fn materialize_batch(batch: &LakeBatch) -> Result<Vec<u8>, String> {
    let schema = Arc::new(arrow_schema(&batch.schema));
    let columns: Vec<ArrayRef> = batch
        .schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, f)| build_column(batch, i, f.ty))
        .collect();
    let record = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("arrow record batch: {e}"))?;

    let mut buf: Vec<u8> = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
        .map_err(|e| format!("parquet writer: {e}"))?;
    writer
        .write(&record)
        .map_err(|e| format!("parquet write: {e}"))?;
    writer.close().map_err(|e| format!("parquet close: {e}"))?;
    Ok(buf)
}

/// The Parquet physical footprint of a materialized batch: byte length + row count
/// (CONCEPT:EG-317). Fed straight into the Delta `add` action / Iceberg data-file
/// entry so an external planner sees accurate file stats without opening the file.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ParquetStats {
    pub size_bytes: u64,
    pub num_rows: u64,
}

/// Materialize and return the bytes plus their [`ParquetStats`] (CONCEPT:EG-317).
pub fn materialize_with_stats(batch: &LakeBatch) -> Result<(Vec<u8>, ParquetStats), String> {
    let bytes = materialize_batch(batch)?;
    let stats = ParquetStats {
        size_bytes: bytes.len() as u64,
        num_rows: batch.num_rows() as u64,
    };
    Ok((bytes, stats))
}

/// Map an Arrow [`DataType`] back to a [`LakeType`] on read (CONCEPT:EG-317).
fn lake_type(dt: &DataType) -> Result<LakeType, String> {
    Ok(match dt {
        DataType::Int64 => LakeType::Long,
        DataType::Float64 => LakeType::Double,
        DataType::Boolean => LakeType::Bool,
        DataType::Utf8 => LakeType::String,
        DataType::Timestamp(TimeUnit::Microsecond, _) => LakeType::Timestamp,
        other => return Err(format!("unsupported Arrow type on read: {other:?}")),
    })
}

/// Read Parquet bytes back into a [`LakeBatch`] (CONCEPT:EG-317). The inverse of
/// [`materialize_batch`]; used by the round-trip correctness test and by any in-engine
/// consumer that wants to re-read a materialized file.
pub fn read_parquet(bytes: &[u8]) -> Result<LakeBatch, String> {
    // `bytes::Bytes` is a parquet `ChunkReader`; own the input so the reader is 'static.
    let owned = bytes::Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(owned)
        .map_err(|e| format!("parquet reader: {e}"))?;
    let arrow_schema = builder.schema().clone();

    let fields: Vec<LakeField> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            Ok(LakeField {
                name: f.name().clone(),
                ty: lake_type(f.data_type())?,
                nullable: f.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let schema = LakeSchema::new(fields);

    let reader = builder.build().map_err(|e| format!("parquet build: {e}"))?;
    let mut rows: Vec<Vec<CellValue>> = Vec::new();
    for rb in reader {
        let rb = rb.map_err(|e| format!("parquet batch: {e}"))?;
        let ncols = rb.num_columns();
        for r in 0..rb.num_rows() {
            let mut row = Vec::with_capacity(ncols);
            for c in 0..ncols {
                row.push(cell_at(rb.column(c), r)?);
            }
            rows.push(row);
        }
    }
    LakeBatch::new(schema, rows)
}

/// Extract one cell from an Arrow column at row `r` as a [`CellValue`] (CONCEPT:EG-317).
fn cell_at(col: &ArrayRef, r: usize) -> Result<CellValue, String> {
    use arrow::array::Array;
    if col.is_null(r) {
        return Ok(CellValue::Null);
    }
    let v = match col.data_type() {
        DataType::Int64 => {
            let a = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("downcast Int64")?;
            CellValue::Long(a.value(r))
        }
        DataType::Float64 => {
            let a = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("downcast Float64")?;
            CellValue::Double(a.value(r))
        }
        DataType::Boolean => {
            let a = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("downcast Boolean")?;
            CellValue::Bool(a.value(r))
        }
        DataType::Utf8 => {
            let a = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("downcast Utf8")?;
            CellValue::String(a.value(r).to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or("downcast Timestamp")?;
            CellValue::Timestamp(a.value(r))
        }
        other => return Err(format!("unsupported Arrow type on read: {other:?}")),
    };
    Ok(v)
}
