//! `DecisionLog.query`: a read-only SQL view over the caller's visible
//! decision log (EH-066).
//!
//! Decision records live in the Agent Library control owner, not in a graph,
//! so there is no graph row for SPARQL, GraphQL or pgwire to project. The view
//! is three relations built per request from ONLY the entries, evaluations and
//! resolutions the caller may read -- `decisions`, `evaluations`,
//! `resolutions` -- and one read-only SQL statement over them. Visibility is
//! applied before a row exists, so no SQL can reach a row the caller could not
//! `DecisionLog.get`.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// Most rows one view answer carries; a larger answer is refused, not cut.
pub const MAX_VIEW_ROWS: usize = 10_000;
/// Longest SQL text a view request may carry.
pub const MAX_VIEW_SQL_BYTES: usize = 16_384;

/// One cell. Floats are carried as their decimal text, never as a float.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cell", content = "value", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ViewCell {
    Null,
    Bool(bool),
    Int(i64),
    Text(String),
}

/// The answer to one view query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionLogRows {
    pub schema_version: u16,
    pub columns: BoundedVec<String, 64>,
    pub rows: BoundedVec<BoundedVec<ViewCell, 64>, 10_000>,
}
