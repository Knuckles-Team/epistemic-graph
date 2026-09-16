//! Exact source-cell interpretation lowered onto the existing SQL Insert path.

use super::super::row_insert::insert_rows_in;
use super::super::{
    fill_omitted_insert_cells, get_schema_in, resolve_targets, schema_version_in,
    validate_column_checks_in, Cell, Column, ColumnType, SqlWrite, TableSchema,
};
use eg_types::storage_wire::{SqlSourceBatchRequest, SqlSourceCell, SqlSourceVector};

pub(super) fn apply(
    write: &SqlWrite<'_>,
    tenant: &str,
    request: &SqlSourceBatchRequest,
) -> Result<usize, String> {
    let submitted = request.as_batch();
    let name = submitted.table.as_str();
    let schema = get_schema_in(write, name)?
        .ok_or_else(|| format!("SQL source target table `{name}` does not exist"))?;
    let actual_digest = schema.schema_digest()?;
    if schema_version_in(write, tenant, name)? != submitted.expected_schema_version
        || actual_digest != submitted.expected_schema_digest.to_hex()
    {
        return Err("SQL source target schema compare-and-swap conflict".into());
    }
    let columns: Vec<_> = submitted
        .columns
        .iter()
        .map(|name| name.as_str().to_string())
        .collect();
    let targets = resolve_targets(&schema, name, &columns)?;
    admit_materialization(&schema, &targets, request)?;
    let inserted = insert_rows_in(
        write,
        tenant,
        name,
        schema,
        submitted.rows.as_slice(),
        |schema, row, rowid| build_source_cells(schema, &targets, row.as_slice(), rowid),
        |cells| eg_storage::encode_bounded(cells, "SQL source row"),
    )?;
    Ok(inserted.len())
}

/// Place the explicitly typed cells, then reuse ordinary INSERT's omitted
/// columns and column checks. JSON null is a Json cell; SQL NULL is a Null cell.
fn build_source_cells(
    schema: &TableSchema,
    targets: &[usize],
    row: &[SqlSourceCell],
    rowid: u64,
) -> Result<Vec<Cell>, String> {
    let width = schema.columns().len();
    let mut cells = vec![Cell::Null; width];
    let mut supplied = vec![false; width];
    for (value, &index) in row.iter().zip(targets) {
        cells[index] = lower_cell(value, &schema.columns()[index])?;
        supplied[index] = true;
    }
    fill_omitted_insert_cells(schema, &mut cells, &supplied, rowid)?;
    validate_column_checks_in(schema, &cells)?;
    Ok(cells)
}

fn lower_cell(cell: &SqlSourceCell, column: &Column) -> Result<Cell, String> {
    match cell {
        SqlSourceCell::Null => lower_null(column),
        SqlSourceCell::Int(value) => {
            require_type(column, &[ColumnType::Int, ColumnType::BigInt])?;
            Ok(Cell::Int(*value))
        }
        SqlSourceCell::FiniteFloat(value) => {
            require_type(column, &[ColumnType::Float, ColumnType::Double])?;
            Ok(Cell::Float(value.get()))
        }
        SqlSourceCell::Text(value) => {
            require_type(column, &[ColumnType::Text])?;
            Ok(Cell::Text(value.as_str().to_string()))
        }
        SqlSourceCell::Bool(value) => {
            require_type(column, &[ColumnType::Bool])?;
            Ok(Cell::Bool(*value))
        }
        SqlSourceCell::Timestamp(value) => {
            require_type(column, &[ColumnType::Timestamp, ColumnType::TimestampTz])?;
            Ok(Cell::Timestamp(*value))
        }
        SqlSourceCell::Bytes(value) => {
            require_type(column, &[ColumnType::Bytes])?;
            Ok(Cell::Bytes(value.as_slice().to_vec()))
        }
        SqlSourceCell::Json(value) => lower_json(value, column),
        SqlSourceCell::FiniteVector(value) => lower_vector(value, column),
    }
}

fn require_type(column: &Column, allowed: &[ColumnType]) -> Result<(), String> {
    if !allowed.contains(&column.ty) {
        return Err(format!(
            "SQL source cell type differs from column `{}`",
            column.name
        ));
    }
    Ok(())
}

fn lower_null(column: &Column) -> Result<Cell, String> {
    if !column.nullable {
        return Err(format!(
            "SQL source NULL violates NOT NULL column `{}`",
            column.name
        ));
    }
    Ok(Cell::Null)
}

fn lower_json(
    value: &eg_types::storage_wire::SqlSourceJson,
    column: &Column,
) -> Result<Cell, String> {
    require_type(column, &[ColumnType::Json])?;
    Ok(Cell::Json(value.value()?))
}

fn lower_vector(vector: &SqlSourceVector, column: &Column) -> Result<Cell, String> {
    let ColumnType::Vector(dimensions) = column.ty else {
        return Err(format!(
            "SQL source vector type differs from column `{}`",
            column.name
        ));
    };
    if dimensions.is_some_and(|expected| expected != vector.as_slice().len()) {
        return Err("SQL source vector dimension differs from target column".into());
    }
    Ok(Cell::Vector(vector.as_slice().to_vec()))
}

// Bounds are conservative codec/structural reservations, not exact allocator
// RSS. The whole batch is admitted before DEFAULT coercion, typed-cell copies,
// rowid allocation or publication. Encoded bytes and decoded structural nodes
// are bounded by the reservation and existing kernel codec/collection limits.
const MATERIALIZED_BYTES: usize = eg_types::storage_wire::source_batch::MAX_SQL_SOURCE_BATCH_BYTES;
const MATERIALIZED_ITEMS: usize = eg_types::msgpack::MAX_PROPERTY_ITEMS;

#[derive(Clone, Copy)]
struct Expansion {
    bytes: usize,
    items: usize,
}

impl Expansion {
    fn fixed(bytes: usize, items: usize) -> Self {
        Self { bytes, items }
    }

    fn encoded_scale(self, multiplier: usize) -> Result<Self, String> {
        Ok(Self {
            bytes: self
                .bytes
                .checked_mul(multiplier)
                .ok_or_else(materialization_error)?,
            items: self.items,
        })
    }

    fn scaled(self, multiplier: usize) -> Result<Self, String> {
        Ok(Self {
            bytes: self
                .bytes
                .checked_mul(multiplier)
                .ok_or_else(materialization_error)?,
            items: self
                .items
                .checked_mul(multiplier)
                .ok_or_else(materialization_error)?,
        })
    }
}

#[derive(Default)]
struct Materialization {
    bytes: usize,
    items: usize,
    collection: eg_storage::CollectionBudget,
}

impl Materialization {
    fn reserve(&mut self, expansion: Expansion) -> Result<(), String> {
        self.bytes = self
            .bytes
            .checked_add(expansion.bytes)
            .filter(|total| *total <= MATERIALIZED_BYTES)
            .ok_or_else(|| {
                "SQL source materialization exceeds aggregate codec budget".to_string()
            })?;
        self.items = self
            .items
            .checked_add(expansion.items)
            .filter(|total| *total <= MATERIALIZED_ITEMS)
            .ok_or_else(|| {
                "SQL source materialization exceeds aggregate structural budget".to_string()
            })?;
        Ok(())
    }
}

fn materialization_error() -> String {
    "SQL source materialization exceeds aggregate codec or structural budget".into()
}

fn admit_materialization(
    schema: &TableSchema,
    targets: &[usize],
    request: &SqlSourceBatchRequest,
) -> Result<(), String> {
    let rows = request.as_batch().rows.as_slice();
    let mut budget = Materialization::default();
    // Each native row-array header is at most five bytes. Reserve omitted
    // SERIAL/default/NULL columns across ALL rows before touching their values.
    budget.reserve(Expansion::fixed(5, 1).scaled(rows.len())?)?;
    for (index, column) in schema.columns().iter().enumerate() {
        if !targets.contains(&index) {
            budget.reserve(default_expansion(column)?.scaled(rows.len())?)?;
        }
    }
    let implicit_per_row = budget.bytes / rows.len();
    for row in rows {
        let before = budget.bytes;
        for cell in row {
            budget.reserve(source_expansion(cell)?)?;
        }
        // The storage kernel's standard collection policy also applies. The
        // source-specific 16MiB/item ceilings above are deliberately stricter.
        budget.collection.account(
            implicit_per_row
                .checked_add(budget.bytes - before)
                .ok_or_else(materialization_error)?,
        )?;
    }
    Ok(())
}

fn source_expansion(cell: &SqlSourceCell) -> Result<Expansion, String> {
    let expansion = match cell {
        SqlSourceCell::Null => Expansion::fixed(8, 3),
        SqlSourceCell::Int(_) | SqlSourceCell::FiniteFloat(_) | SqlSourceCell::Timestamp(_) => {
            Expansion::fixed(32, 3)
        }
        SqlSourceCell::Bool(_) => Expansion::fixed(16, 3),
        SqlSourceCell::Text(value) => {
            Expansion::fixed(value.as_str().len(), 0).with_overhead(32, 3)?
        }
        SqlSourceCell::Bytes(value) => {
            Expansion::fixed(value.as_slice().len(), value.as_slice().len())
                .encoded_scale(2)?
                .with_overhead(32, 3)?
        }
        SqlSourceCell::Json(value) => {
            Expansion::fixed(value.canonical_bytes().len(), value.canonical_bytes().len())
                .encoded_scale(9)?
                .with_overhead(128, 4)?
        }
        SqlSourceCell::FiniteVector(value) => {
            Expansion::fixed(value.as_slice().len(), value.as_slice().len())
                .encoded_scale(5)?
                .with_overhead(32, 3)?
        }
    };
    Ok(expansion)
}

impl Expansion {
    fn with_overhead(self, bytes: usize, items: usize) -> Result<Self, String> {
        Ok(Self {
            bytes: self
                .bytes
                .checked_add(bytes)
                .ok_or_else(materialization_error)?,
            items: self
                .items
                .checked_add(items)
                .ok_or_else(materialization_error)?,
        })
    }
}

fn default_expansion(column: &Column) -> Result<Expansion, String> {
    if column.serial {
        return Ok(Expansion::fixed(32, 3));
    }
    let Some(default) = &column.default else {
        return Ok(Expansion::fixed(8, 3));
    };
    match default {
        serde_json::Value::Null => Ok(Expansion::fixed(8, 3)),
        serde_json::Value::String(text) => string_default_expansion(text, column.ty),
        value => structured_default_expansion(value),
    }
}

fn string_default_expansion(text: &str, ty: ColumnType) -> Result<Expansion, String> {
    match ty {
        ColumnType::Text | ColumnType::Json => Expansion::fixed(text.len(), 0).with_overhead(32, 3),
        ColumnType::Bytes => Expansion::fixed(text.len(), text.len())
            .encoded_scale(2)?
            .with_overhead(32, 3),
        // A textual vector/array can become one element per input byte. The
        // factor includes f32 encoding and normalized scalar-array elements.
        ColumnType::Vector(_) | ColumnType::Array(_) => Expansion::fixed(text.len(), text.len())
            .encoded_scale(9)?
            .with_overhead(128, 4),
        // NUMERIC scale is capped at 38; UUID/timestamp/scalar conversions
        // cannot expand past the original string length plus this allowance.
        _ => Expansion::fixed(text.len(), 0).with_overhead(128, 4),
    }
}

fn structured_default_expansion(value: &serde_json::Value) -> Result<Expansion, String> {
    let mut counter = BorrowedDefaultBytes::default();
    serde_json::to_writer(&mut counter, value).map_err(|_| materialization_error())?;
    let mut nodes = 0;
    count_default_nodes(value, 0, &mut nodes)?;
    // Covers enum tags, native numeric/vector width and normalized array
    // cells. It is an upper bound computed without coercing or cloning JSON.
    Expansion::fixed(counter.bytes, nodes)
        .encoded_scale(9)?
        .with_overhead(128, 4)
}

#[derive(Default)]
struct BorrowedDefaultBytes {
    bytes: usize,
}

impl std::io::Write for BorrowedDefaultBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|total| *total <= MATERIALIZED_BYTES)
            .ok_or_else(|| {
                std::io::Error::other("SQL source default serialization budget exceeded")
            })?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn count_default_nodes(
    value: &serde_json::Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), String> {
    if depth > eg_types::msgpack::DEFAULT_MAX_DEPTH {
        return Err(materialization_error());
    }
    *nodes = nodes
        .checked_add(1)
        .filter(|total| *total <= MATERIALIZED_ITEMS)
        .ok_or_else(materialization_error)?;
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                count_default_nodes(value, depth + 1, nodes)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                // A map key is another structural scalar in native MessagePack.
                *nodes = nodes
                    .checked_add(1)
                    .filter(|total| *total <= MATERIALIZED_ITEMS)
                    .ok_or_else(materialization_error)?;
                count_default_nodes(value, depth + 1, nodes)?;
            }
        }
        _ => {}
    }
    Ok(())
}
