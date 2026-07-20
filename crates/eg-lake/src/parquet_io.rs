//! Parquet materialization + read-back (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
//!
//! Transcodes a [`LakeBatch`] (the engine's columnar/table data, neutralized in
//! `schema.rs`) into a single Parquet file's bytes, and reads Parquet bytes back into
//! a [`LakeBatch`]. This is the ONLY module that pulls the Polars Parquet codec, so it is the
//! ONLY thing the `lake` feature gates — the Delta/Iceberg logs, the catalog and the
//! LSN snapshot are pure JSON and compile without it.
//!
//! The logical type map is fixed so the file an external engine (Spark/Trino/DuckDB)
//! reads carries exactly the logical types the Delta/Iceberg schema advertises:
//! `Long → Int64`, `Double → Float64`, `Bool → Boolean`, `String → String`,
//! `Timestamp → Datetime(Microseconds, UTC)`.

use polars_core::prelude::{AnyValue, Column, DataFrame, DataType, TimeUnit, TimeZone};
use polars_io::prelude::{
    FileMetadata, KeyValueMetadata, ParquetCompression, ParquetReader, ParquetWriter, SerReader,
};

use crate::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};
use crate::snapshot::ColumnStat;

const LAKE_SCHEMA_METADATA_KEY: &str = "epistemic_graph.lake_schema.v1";

/// The Polars [`DataType`] a [`LakeType`] maps to. A UTC-zoned micro-timestamp
/// is what Delta/Iceberg's `timestamptz` expects.
fn parquet_type(ty: LakeType) -> DataType {
    match ty {
        LakeType::Long => DataType::Int64,
        LakeType::Double => DataType::Float64,
        LakeType::Bool => DataType::Boolean,
        LakeType::String => DataType::String,
        LakeType::Timestamp => DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
    }
}

/// Explode one column of a row-major batch into a typed Polars column. A cell whose
/// runtime variant disagrees with the schema is materialized as null.
fn build_column(batch: &LakeBatch, col: usize, field: &LakeField) -> Result<Column, String> {
    let name = field.name.clone().into();
    Ok(match field.ty {
        LakeType::Long => {
            let values = batch
                .rows
                .iter()
                .map(|r| match &r[col] {
                    CellValue::Long(v) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Column::new(name, values)
        }
        LakeType::Double => {
            let values = batch
                .rows
                .iter()
                .map(|r| match &r[col] {
                    CellValue::Double(v) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Column::new(name, values)
        }
        LakeType::Bool => {
            let values = batch
                .rows
                .iter()
                .map(|r| match &r[col] {
                    CellValue::Bool(v) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Column::new(name, values)
        }
        LakeType::String => {
            let values: Vec<Option<&str>> = batch
                .rows
                .iter()
                .map(|r| match &r[col] {
                    CellValue::String(v) => Some(v.as_str()),
                    _ => None,
                })
                .collect();
            Column::new(name, values)
        }
        LakeType::Timestamp => {
            let values = batch
                .rows
                .iter()
                .map(|r| match &r[col] {
                    CellValue::Timestamp(v) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Column::new(name, values)
                .cast(&parquet_type(LakeType::Timestamp))
                .map_err(|e| format!("timestamp column {}: {e}", field.name))?
        }
    })
}

/// Transcode a [`LakeBatch`] into one Parquet file's bytes AND the Parquet
/// [`FileMetadata`] parsed from the completed file. The metadata carries the
/// per-column-chunk compressed sizes the Iceberg `column_sizes` stat needs (EG-350).
fn materialize_batch_meta(batch: &LakeBatch) -> Result<(Vec<u8>, FileMetadata), String> {
    let columns: Vec<Column> = batch
        .schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| build_column(batch, i, field))
        .collect::<Result<_, _>>()?;
    let mut frame = DataFrame::new(batch.num_rows(), columns)
        .map_err(|e| format!("parquet data frame: {e}"))?;

    let mut buf: Vec<u8> = Vec::new();
    let schema_json = serde_json::to_string(&batch.schema)
        .map_err(|e| format!("serialize lake schema metadata: {e}"))?;
    ParquetWriter::new(&mut buf)
        .with_compression(ParquetCompression::Uncompressed)
        .set_parallel(false)
        .with_key_value_metadata(Some(KeyValueMetadata::from_static(vec![(
            LAKE_SCHEMA_METADATA_KEY.to_string(),
            schema_json,
        )])))
        .finish(&mut frame)
        .map_err(|e| format!("parquet write: {e}"))?;

    let mut reader = ParquetReader::new(std::io::Cursor::new(buf.as_slice()));
    let meta = reader
        .get_metadata()
        .map_err(|e| format!("parquet metadata: {e}"))?
        .as_ref()
        .clone();
    Ok((buf, meta))
}

/// Transcode a [`LakeBatch`] into one Parquet file's bytes (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
///
/// This is the core materialization primitive — `materialize_table(rows) -> parquet
/// bytes`. The bytes are a self-describing Parquet file (schema in the footer) that
/// any lakehouse reader opens directly; the Delta/Iceberg log then points at it.
pub fn materialize_batch(batch: &LakeBatch) -> Result<Vec<u8>, String> {
    Ok(materialize_batch_meta(batch)?.0)
}

/// Whether a cell's runtime variant matches the column's declared [`LakeType`]
/// (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries). A mismatching cell is materialized as null by the Parquet path
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

/// Strict `a < b` for two same-typed, non-null, non-NaN cells (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries). Used to
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

/// Compute per-column min/max/null/nan over a batch (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries). A pure row walk —
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
/// Parquet [`FileMetadata`] — the source for Iceberg `column_sizes`.
fn column_sizes_by_name(meta: &FileMetadata) -> std::collections::HashMap<String, i64> {
    let mut sizes: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for rg in &meta.row_groups {
        for col in rg.parquet_columns() {
            if let Some(name) = col.descriptor().path_in_schema.first() {
                *sizes.entry(name.as_str().to_string()).or_insert(0) += col.compressed_size();
            }
        }
    }
    sizes
}

/// The Parquet physical footprint of a materialized batch: byte length + row count
/// (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Fed straight into the Delta `add` action / Iceberg data-file
/// entry so an external planner sees accurate file stats without opening the file.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ParquetStats {
    pub size_bytes: u64,
    pub num_rows: u64,
}

/// Materialize and return the bytes plus their [`ParquetStats`] (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
pub fn materialize_with_stats(batch: &LakeBatch) -> Result<(Vec<u8>, ParquetStats), String> {
    let bytes = materialize_batch(batch)?;
    let stats = ParquetStats {
        size_bytes: bytes.len() as u64,
        num_rows: batch.num_rows() as u64,
    };
    Ok((bytes, stats))
}

/// Materialize a batch AND gather full per-column Iceberg stats (CONCEPT:EG-KG.storage.iceberg-avro-manifest-carries).
///
/// Returns the Parquet bytes, the file-level [`ParquetStats`], and one [`ColumnStat`]
/// per column carrying `value_count` / `null_count` / `nan_count` / `column_size` and
/// typed min/max `lower`/`upper` bounds. This is the LTAP tier's stats-bearing
/// materialize path (EG-317) — [`crate::LakeTable::materialize`] feeds the returned
/// stats to the snapshot so the Iceberg Avro manifest (CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest) can emit the
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

/// Map a Polars [`DataType`] back to a [`LakeType`] on read.
fn lake_type(dt: &DataType) -> Result<LakeType, String> {
    Ok(match dt {
        DataType::Int64 => LakeType::Long,
        DataType::Float64 => LakeType::Double,
        DataType::Boolean => LakeType::Bool,
        DataType::String => LakeType::String,
        DataType::Datetime(TimeUnit::Microseconds, _) => LakeType::Timestamp,
        other => return Err(format!("unsupported Parquet type on read: {other:?}")),
    })
}

fn embedded_lake_schema(meta: &FileMetadata) -> Result<Option<LakeSchema>, String> {
    let Some(items) = meta.key_value_metadata.as_ref() else {
        return Ok(None);
    };
    let Some(value) = items
        .iter()
        .find(|item| item.key == LAKE_SCHEMA_METADATA_KEY)
        .and_then(|item| item.value.as_deref())
    else {
        return Ok(None);
    };
    serde_json::from_str(value)
        .map(Some)
        .map_err(|e| format!("invalid embedded lake schema: {e}"))
}

/// Read Parquet bytes back into a [`LakeBatch`] (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). The inverse of
/// [`materialize_batch`]; used by the round-trip correctness test and by any in-engine
/// consumer that wants to re-read a materialized file.
pub fn read_parquet(bytes: &[u8]) -> Result<LakeBatch, String> {
    let mut reader = ParquetReader::new(std::io::Cursor::new(bytes));
    let meta = reader
        .get_metadata()
        .map_err(|e| format!("parquet metadata: {e}"))?
        .clone();
    let embedded_schema = embedded_lake_schema(meta.as_ref())?;
    let frame = reader.finish().map_err(|e| format!("parquet read: {e}"))?;

    let schema = match embedded_schema {
        Some(schema) => schema,
        None => LakeSchema::new(
            frame
                .columns()
                .iter()
                .map(|column| {
                    Ok(LakeField {
                        name: column.name().as_str().to_string(),
                        ty: lake_type(column.dtype())?,
                        // Polars exposes logical dtypes rather than Parquet repetition at
                        // this layer. Conservatively keep externally-authored columns nullable.
                        nullable: true,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
    };
    if schema.fields.len() != frame.width() {
        return Err("embedded lake schema width does not match Parquet data".to_string());
    }

    let mut rows: Vec<Vec<CellValue>> = Vec::with_capacity(frame.height());
    for row_index in 0..frame.height() {
        let mut row = Vec::with_capacity(frame.width());
        for (column, field) in frame.columns().iter().zip(&schema.fields) {
            row.push(cell_at(column, row_index, field.ty)?);
        }
        rows.push(row);
    }
    LakeBatch::new(schema, rows)
}

/// Extract one typed cell from a Polars column.
fn cell_at(col: &Column, row: usize, expected: LakeType) -> Result<CellValue, String> {
    let value = col
        .get(row)
        .map_err(|e| format!("read column {} row {row}: {e}", col.name()))?;
    match (expected, value) {
        (_, AnyValue::Null) => Ok(CellValue::Null),
        (LakeType::Long, AnyValue::Int64(value)) => Ok(CellValue::Long(value)),
        (LakeType::Double, AnyValue::Float64(value)) => Ok(CellValue::Double(value)),
        (LakeType::Bool, AnyValue::Boolean(value)) => Ok(CellValue::Bool(value)),
        (LakeType::String, AnyValue::String(value)) => Ok(CellValue::String(value.to_string())),
        (LakeType::String, AnyValue::StringOwned(value)) => {
            Ok(CellValue::String(value.as_str().to_string()))
        }
        (LakeType::Timestamp, AnyValue::Datetime(value, TimeUnit::Microseconds, _))
        | (LakeType::Timestamp, AnyValue::DatetimeOwned(value, TimeUnit::Microseconds, _)) => {
            Ok(CellValue::Timestamp(value))
        }
        (expected, actual) => Err(format!(
            "column {} row {row}: expected {expected:?}, found {actual:?}",
            col.name()
        )),
    }
}
