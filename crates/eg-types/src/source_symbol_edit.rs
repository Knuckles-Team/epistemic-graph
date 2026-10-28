//! Graph-native code edits (EG-SCM-R013.1): an edit names a symbol and its new
//! text instead of a file diff. The projection to file bytes and one changeset
//! lives in `eg-compute`.

use serde::{Deserialize, Serialize};

/// What an op does to its symbol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolEditKind {
    ReplaceSymbol,
    ReplaceBody,
    InsertBefore,
    InsertAfter,
    Delete,
}

/// One op against one symbol, guarded by the digest of the span it expects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolEditOp {
    pub kind: SymbolEditKind,
    pub symbol_id: String,
    pub anchor_span_digest: String,
    pub new_text: String,
}

/// An edit of one branch at one base revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolEdit {
    pub repo: String,
    pub branch: String,
    pub base_rev: String,
    pub ops: Vec<SymbolEditOp>,
    pub message: String,
    pub run_trace_id: String,
}
