//! Arrow materialization of a user table (CONCEPT:EG-KG.query.register-user-tables-alongside) so it registers as a
//! DataFusion `TableProvider` ALONGSIDE the graph's `nodes`/`edges` tables — the
//! unified-engine payoff: a single SELECT can JOIN a user table to the graph.
//!
//! Each user table is scanned out of the redb store into ONE Arrow `RecordBatch`
//! whose schema is derived from the catalog [`TableSchema`] (so the column types are
//! the declared types, not schema-on-read inference). The batch is wrapped in a
//! DataFusion `MemTable` and registered under the table's name. Predicate pushdown
//! is left to DataFusion's `Filter` above the in-memory scan for this first
//! increment (correct, and JOINs/compose work); a PK/secondary-index pushdown
//! provider is an explicit follow-up.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int64Builder,
    ListBuilder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::schema::{Cell, ColumnType, TableSchema};

/// The Arrow `DataType` a [`ColumnType`] materializes as. Coarse + pg-mappable: every
/// type lands on one of `Int64`/`Float64`/`Utf8`/`Boolean`/`Binary`, which the exec
/// path's `PgColType` mapping already covers, so a user-table column is never
/// lossy-dropped over the wire.
pub fn arrow_type(ty: ColumnType) -> DataType {
    match ty {
        ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp => DataType::Int64,
        ColumnType::Float | ColumnType::Double => DataType::Float64,
        ColumnType::Text | ColumnType::Json => DataType::Utf8,
        ColumnType::Bool => DataType::Boolean,
        ColumnType::Bytes => DataType::Binary,
        // CONCEPT:EG-KG.query.pgvector-binary-wire — a pgvector column materializes as `List<Float32>`; the exec
        // path's `pg_col_type` maps a Float32-element list to the wire vector type.
        ColumnType::Vector(_) => {
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        }
    }
}

/// The Arrow [`SchemaRef`] for a user table's catalog schema.
pub fn arrow_schema(schema: &TableSchema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .columns()
        .iter()
        .map(|c| Field::new(&c.name, arrow_type(c.ty), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Materialize `(schema, rows)` into an Arrow `(SchemaRef, RecordBatch)` — the shape
/// the exec path registers as a DataFusion table. Each row is a schema-aligned
/// `Vec<Cell>` (already NULL-padded by the store scan).
pub fn materialize(
    schema: &TableSchema,
    rows: &[Vec<Cell>],
) -> Result<(SchemaRef, RecordBatch), String> {
    let arrow = arrow_schema(schema);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.columns().len());

    for (ci, col) in schema.columns().iter().enumerate() {
        let array: ArrayRef = match arrow_type(col.ty) {
            DataType::Int64 => {
                let mut b = Int64Builder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Int(i)) | Some(Cell::Timestamp(i)) => b.append_value(*i),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Float(f)) => b.append_value(*f),
                        Some(Cell::Int(i)) => b.append_value(*i as f64),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Bool(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Bytes(bytes)) => b.append_value(bytes),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            // CONCEPT:EG-KG.query.pgvector-binary-wire — a vector column → a `List<Float32>` array (one list per row).
            DataType::List(_) => {
                let mut b = ListBuilder::new(Float32Builder::new());
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Vector(v)) => {
                            for f in v {
                                b.values().append_value(*f);
                            }
                            b.append(true);
                        }
                        _ => b.append(false),
                    }
                }
                Arc::new(b.finish())
            }
            // Utf8 (Text + Json). Json cells render as their canonical JSON text.
            _ => {
                let mut b = StringBuilder::new();
                for row in rows {
                    match row.get(ci) {
                        Some(Cell::Text(s)) => b.append_value(s),
                        Some(Cell::Json(v)) => b.append_value(v.to_string()),
                        Some(Cell::Null) | None => b.append_null(),
                        // A scalar in a text column (shouldn't happen post-coerce) →
                        // its JSON rendering, never a hard error.
                        Some(other) => b.append_value(other.to_json().to_string()),
                    }
                }
                Arc::new(b.finish())
            }
        };
        columns.push(array);
    }

    let batch = RecordBatch::try_new(arrow.clone(), columns)
        .map_err(|e| format!("user table batch: {e}"))?;
    Ok((arrow, batch))
}
