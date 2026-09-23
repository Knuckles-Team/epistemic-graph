//! `information_schema.eg_index_status` (EH-352): one row per managed index the
//! reading store serves — its family, typed target, lifecycle state
//! (`requested | backfilling | active | blocked`), live generation, lag and
//! typed block diagnostic. Synthesized per statement like the other system
//! catalogs, and only from rows the reading store may show: a served read runs
//! over a per-caller projection that holds exactly the managed indexes of the
//! tables the caller may `SELECT` (there is no separate wire operation).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use eg_core::index::ManagedIndexStatus;

use super::store_catalog::INDEX_STATUS_RELATION;

/// `(column, type, nullable)`, in relation order.
const COLUMNS: [(&str, DataType, bool); 13] = [
    ("index_name", DataType::Utf8, false),
    ("family", DataType::Utf8, false),
    ("target_kind", DataType::Utf8, false),
    ("relation_name", DataType::Utf8, false),
    ("attribute_name", DataType::Utf8, false),
    ("state", DataType::Utf8, false),
    ("generation", DataType::Int64, true),
    ("built_version", DataType::Int64, true),
    ("change_version", DataType::Int64, false),
    ("lag", DataType::Int64, false),
    ("indexed", DataType::Int64, true),
    ("block_reason", DataType::Utf8, true),
    ("block_detail", DataType::Utf8, true),
];

type TextColumn = fn(&ManagedIndexStatus) -> Option<String>;
type NumberColumn = fn(&ManagedIndexStatus) -> Option<u64>;

/// The text columns, in relation order before the numbers.
const LEADING_TEXT: [TextColumn; 6] = [
    |status| Some(status.name.clone()),
    |status| Some(status.family.as_str().to_string()),
    |status| Some(status.target.kind().to_string()),
    |status| Some(status.target.relation().to_string()),
    |status| Some(status.target.attribute().to_string()),
    |status| Some(status.state.as_str().to_string()),
];
const NUMBERS: [NumberColumn; 5] = [
    |status| status.generation,
    |status| status.built_version,
    |status| Some(status.change_version),
    |status| Some(status.lag),
    |status| status.indexed.map(|rows| rows as u64),
];
const TRAILING_TEXT: [TextColumn; 2] = [
    |status| {
        status
            .block
            .as_ref()
            .map(|block| block.reason.as_str().to_string())
    },
    |status| status.block.as_ref().map(|block| block.detail.clone()),
];

/// Register `statuses` as `information_schema.eg_index_status` in `ctx`, whose
/// `information_schema` must already be registered.
pub(crate) fn register(
    ctx: &SessionContext,
    statuses: &[ManagedIndexStatus],
) -> Result<(), String> {
    let information_schema = ctx
        .catalog("datafusion")
        .and_then(|catalog| catalog.schema("information_schema"))
        .ok_or_else(|| "information_schema is not registered".to_string())?;
    let batch = status_batch(statuses)?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
        .map_err(|error| format!("{INDEX_STATUS_RELATION} memtable: {error}"))?;
    information_schema
        .register_table(INDEX_STATUS_RELATION.to_string(), Arc::new(table))
        .map_err(|error| format!("register {INDEX_STATUS_RELATION}: {error}"))?;
    Ok(())
}

fn status_batch(statuses: &[ManagedIndexStatus]) -> Result<RecordBatch, String> {
    let text = |column: &TextColumn| -> ArrayRef {
        Arc::new(StringArray::from(
            statuses.iter().map(column).collect::<Vec<_>>(),
        ))
    };
    let number = |column: &NumberColumn| -> ArrayRef {
        Arc::new(Int64Array::from(
            statuses
                .iter()
                .map(|status| column(status).map(saturating))
                .collect::<Vec<_>>(),
        ))
    };
    let columns: Vec<ArrayRef> = LEADING_TEXT
        .iter()
        .map(text)
        .chain(NUMBERS.iter().map(number))
        .chain(TRAILING_TEXT.iter().map(text))
        .collect();
    RecordBatch::try_new(schema(), columns)
        .map_err(|error| format!("information_schema.{INDEX_STATUS_RELATION} batch: {error}"))
}

/// A `u64` count as the relation's `bigint`, saturating.
fn saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|(name, kind, nullable)| Field::new(*name, kind.clone(), *nullable))
            .collect::<Vec<_>>(),
    ))
}
