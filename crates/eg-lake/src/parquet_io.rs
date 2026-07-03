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
use parquet::format::FileMetaData;

use crate::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
use crate::snapshot::ColumnStat;

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

/// Transcode a [`LakeBatch`] into one Parquet file's bytes AND the Parquet
/// [`FileMetaData`] the close returns (CONCEPT:EG-317). The metadata carries the
/// per-column-chunk compressed sizes the Iceberg `column_sizes` stat needs (EG-350).
fn materialize_batch_meta(batch: &LakeBatch) -> Result<(Vec<u8>, FileMetaData), String> {
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
    let meta = writer.close().map_err(|e| format!("parquet close: {e}"))?;
    Ok((buf, meta))
}

/// Transcode a [`LakeBatch`] into one Parquet file's bytes (CONCEPT:EG-317).
///
/// This is the core materialization primitive — `materialize_table(rows) -> parquet
/// bytes`. The bytes are a self-describing Parquet file (schema in the footer) that
/// any lakehouse reader opens directly; the Delta/Iceberg log then points at it.
pub fn materialize_batch(batch: &LakeBatch) -> Result<Vec<u8>, String> {
    Ok(materialize_batch_meta(batch)?.0)
}

/// Whether a cell's runtime variant matches the column's declared [`LakeType`]
/// (CONCEPT:EG-350). A mismatching cell is materialized as null by the Parquet path
/// ([`build_column`]), so the stats treat it as null too, staying file-exact.
fn cell_matches(cell: &CellValue, ty: LakeType) -> bool {
    matches!(
        (cell, ty),
        (CellValue::Long(_), LakeType::Long)
            | (CellValue::Double(_), LakeType::Double)
            | (CellValue::Bool(_), LakeType::Bool)
            | (CellValue::String(_), LakeType::String)
            | (CellValue::Timestamp(_), LakeType::Timestamp)
    )
}

/// Strict `a < b` for two same-typed, non-null, non-NaN cells (CONCEPT:EG-350). Used to
/// fold a column's min/max. String ordering is UTF-8 byte order (matches Iceberg's
/// binary string ordering); bool orders `false < true`.
fn cell_lt(a: &CellValue, b: &CellValue) -> bool {
    match (a, b) {
        (CellValue::Long(x), CellValue::Long(y)) => x < y,
        (CellValue::Timestamp(x), CellValue::Timestamp(y)) => x < y,
        (CellValue::Double(x), CellValue::Double(y)) => x < y,
        (CellValue::Bool(x), CellValue::Bool(y)) => !x & y,
        (CellValue::String(x), CellValue::String(y)) => x < y,
        _ => false,
    }
}

/// Compute per-column min/max/null/nan over a batch (CONCEPT:EG-350). A pure row walk —
/// no arrow needed; bounds stay neutral [`CellValue`]s the Iceberg manifest writer later
/// serializes to Iceberg single-value binary. `column_size` is filled from the Parquet
/// metadata by [`materialize_with_column_stats`], so it is left `None` here.
fn compute_column_stats(batch: &LakeBatch) -> Vec<ColumnStat> {
    let nrows = batch.num_rows() as i64;
    batch
        .schema
        .fields
        .iter()
        .enumerate()
        .map(|(c, f)| {
            let mut null_count = 0i64;
            let mut nan_count = 0i64;
            let mut lower: Option<CellValue> = None;
            let mut upper: Option<CellValue> = None;
            for row in &batch.rows {
                let cell = &row[c];
                if cell.is_null() || !cell_matches(cell, f.ty) {
                    null_count += 1;
                    continue;
                }
                // NaN is excluded from bounds per the Iceberg spec; counted separately.
                if let CellValue::Double(v) = cell {
                    if v.is_nan() {
                        nan_count += 1;
                        continue;
                    }
                }
                if lower.as_ref().map(|lo| cell_lt(cell, lo)).unwrap_or(true) {
                    lower = Some(cell.clone());
                }
                if upper.as_ref().map(|hi| cell_lt(hi, cell)).unwrap_or(true) {
                    upper = Some(cell.clone());
                }
            }
            ColumnStat {
                field_id: (c + 1) as i32,
                value_count: nrows,
                null_count,
                nan_count,
                column_size: None,
                lower,
                upper,
            }
        })
        .collect()
}

/// Per-column total compressed byte size, keyed by leaf column name, read back from the
/// Parquet [`FileMetaData`] (CONCEPT:EG-350) — the source for Iceberg `column_sizes`.
fn column_sizes_by_name(meta: &FileMetaData) -> std::collections::HashMap<String, i64> {
    let mut sizes: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for rg in &meta.row_groups {
        for col in &rg.columns {
            if let Some(cm) = &col.meta_data {
                if let Some(name) = cm.path_in_schema.first() {
                    *sizes.entry(name.clone()).or_insert(0) += cm.total_compressed_size;
                }
            }
        }
    }
    sizes
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

/// Materialize a batch AND gather full per-column Iceberg stats (CONCEPT:EG-350).
///
/// Returns the Parquet bytes, the file-level [`ParquetStats`], and one [`ColumnStat`]
/// per column carrying `value_count` / `null_count` / `nan_count` / `column_size` and
/// typed min/max `lower`/`upper` bounds. This is the LTAP tier's stats-bearing
/// materialize path (EG-317) — [`crate::LakeTable::materialize`] feeds the returned
/// stats to the snapshot so the Iceberg Avro manifest (CONCEPT:EG-333) can emit the
/// predicate-pushdown maps a Spark/Trino reader uses to skip files.
pub fn materialize_with_column_stats(
    batch: &LakeBatch,
) -> Result<(Vec<u8>, ParquetStats, Vec<ColumnStat>), String> {
    let (bytes, meta) = materialize_batch_meta(batch)?;
    let stats = ParquetStats {
        size_bytes: bytes.len() as u64,
        num_rows: batch.num_rows() as u64,
    };
    let mut col_stats = compute_column_stats(batch);
    let sizes = column_sizes_by_name(&meta);
    for (c, cs) in col_stats.iter_mut().enumerate() {
        cs.column_size = sizes.get(&batch.schema.fields[c].name).copied();
    }
    Ok((bytes, stats, col_stats))
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
