//! User-managed indexes on the wire (EH-351 / EH-352).
//!
//! The lifecycle vocabulary every managed index family reports in —
//! `requested -> backfilling -> active | blocked`, the typed target (a table
//! column or the edges of a graph), typed bounded diagnostics and the status
//! row — lives here, so the engine's `IndexManager`, the SQL status relation
//! (`information_schema.eg_index_status`), the capability descriptor and every
//! generated client carry the SAME types. `eg-core` re-exports them.
//!
//! The edge-index operations: `EdgeIndex` creates, refreshes, drops and lists
//! the edge-native indexes of the request graph; `EdgeSearch` searches one.
//! An edge is returned as an edge — its endpoints and its position among the
//! parallel edges of that pair — never reified as a node.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// Longest diagnostic detail a status row carries, in bytes.
pub const MAX_BLOCK_DETAIL_BYTES: usize = 256;

/// Where a managed index is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ManagedIndexState {
    Requested,
    Backfilling,
    Active,
    Blocked,
}

impl ManagedIndexState {
    /// The one lifecycle rule every family applies: a live generation is
    /// active; without one, a recorded failure blocks, a running build
    /// backfills, and otherwise the index is only requested.
    pub fn of(serving: bool, building: bool, block: Option<&IndexBlock>) -> Self {
        match (serving, block.is_some(), building) {
            (true, _, _) => Self::Active,
            (false, true, _) => Self::Blocked,
            (false, false, true) => Self::Backfilling,
            (false, false, false) => Self::Requested,
        }
    }

    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 4] = ["requested", "backfilling", "active", "blocked"];
        NAMES[self as usize]
    }
}

/// What a managed index answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ManagedIndexFamily {
    /// Nearest-neighbour search over a vector property.
    Vector,
    /// BM25 search over a text property.
    Text,
}

impl ManagedIndexFamily {
    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 2] = ["vector", "text"];
        NAMES[self as usize]
    }
}

/// The typed target of a managed index. Graph edges are a native target: an
/// edge is indexed and returned as an edge, never reified as a node (EH-351).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagedIndexTarget {
    /// One column of one SQL table.
    TableColumn { table: String, column: String },
    /// One property of every edge of the graph.
    GraphEdges { property: String },
}

impl ManagedIndexTarget {
    /// The target kind's stable name.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::TableColumn { .. } => "table_column",
            Self::GraphEdges { .. } => "graph_edges",
        }
    }

    /// The relation the index covers: the table, or `edges`.
    pub fn relation(&self) -> &str {
        match self {
            Self::TableColumn { table, .. } => table,
            Self::GraphEdges { .. } => "edges",
        }
    }

    /// The column or property the index covers.
    pub fn attribute(&self) -> &str {
        match self {
            Self::TableColumn { column, .. } => column,
            Self::GraphEdges { property } => property,
        }
    }
}

/// Why a managed index is blocked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum IndexBlockReason {
    /// The target cannot be indexed (missing, or not of the family's type).
    NotIndexable,
    /// The source exceeds the build's resource bound.
    BuildBound,
    /// A build failed for another reason.
    BuildFailed,
    /// The persisted generation could not be restored.
    RestoreFailed,
    /// A live generation could not be persisted.
    PersistFailed,
}

impl IndexBlockReason {
    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 5] = [
            "not_indexable",
            "build_bound",
            "build_failed",
            "restore_failed",
            "persist_failed",
        ];
        NAMES[self as usize]
    }
}

/// A typed, bounded diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndexBlock {
    pub reason: IndexBlockReason,
    /// At most [`MAX_BLOCK_DETAIL_BYTES`], cut on a character boundary.
    pub detail: String,
}

impl IndexBlock {
    pub fn new(reason: IndexBlockReason, detail: &str) -> Self {
        let mut end = detail.len().min(MAX_BLOCK_DETAIL_BYTES);
        while !detail.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            reason,
            detail: detail[..end].to_string(),
        }
    }
}

/// One managed index's status: one row of the SQL status relation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ManagedIndexStatus {
    pub name: String,
    pub family: ManagedIndexFamily,
    pub target: ManagedIndexTarget,
    pub state: ManagedIndexState,
    /// The live generation.
    pub generation: Option<u64>,
    /// The source version (SQL epoch / graph version) the live generation was
    /// built from.
    pub built_version: Option<u64>,
    /// The source version of the target's last change.
    pub change_version: u64,
    /// `change_version - built_version`; the whole change version when nothing
    /// serves.
    pub lag: u64,
    /// Entries the live generation indexes; `None` when nothing serves or the
    /// reader may not learn the count (row-level security hides rows from it).
    pub indexed: Option<usize>,
    pub block: Option<IndexBlock>,
}

/// Most managed indexes one status view lists.
pub const MAX_INDEX_STATUSES: usize = 1_024;
/// Most edges one search returns.
pub const MAX_EDGE_SEARCH_HITS: usize = 1_000;
/// Most property-equality filters on one search.
pub const MAX_EDGE_FILTERS: usize = 16;
/// Widest query vector.
pub const MAX_EDGE_QUERY_DIM: usize = 8_192;

/// The distance an edge vector index ranks by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EdgeVectorMetric {
    L2,
    Cosine,
    InnerProduct,
}

/// What an edge index searches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EdgeIndexKind {
    Vector { metric: EdgeVectorMetric },
    Text,
}

/// One edge index a caller creates. The tenant is the verified caller's; the
/// graph is the request graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EdgeIndexDefinition {
    pub name: String,
    /// The edge property indexed.
    pub property: String,
    pub kind: EdgeIndexKind,
    /// The purpose the index serves; a search must present the same.
    pub purpose: String,
}

/// Create, refresh, drop or list the edge indexes of the request graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EdgeIndexOp {
    /// Register the index durably and build its first generation.
    Create { definition: EdgeIndexDefinition },
    /// Build and activate the next generation.
    Refresh { name: String },
    /// Drop the index, its registration and every generation (fenced).
    Drop { name: String },
    /// Every edge index of the request graph.
    Status,
}

impl EdgeIndexOp {
    /// Only the status view reads; every other op writes the tenant catalog.
    pub fn is_mutation(&self) -> bool {
        !matches!(self, Self::Status)
    }

    /// The RBAC action: edge indexes are semantic retrieval indexes, governed
    /// by the semantic-binding actions identities already hold.
    pub fn authz_action(&self) -> &'static str {
        if self.is_mutation() {
            return "semantic:binding-write";
        }
        "semantic:binding-read"
    }
}

/// What an edge search looks for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EdgeSearchQuery {
    Vector {
        vector: BoundedVec<f32, MAX_EDGE_QUERY_DIM>,
    },
    Text {
        text: String,
    },
}

/// One property an admitted edge must carry with exactly this value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EdgePropertyEquals {
    pub property: String,
    pub value: serde_json::Value,
}

/// Search one edge index of the request graph. Row-level security, the edge
/// type and the property filters are applied inside the index walk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EdgeSearchRequest {
    pub index: String,
    pub purpose: String,
    pub query: EdgeSearchQuery,
    pub k: u32,
    /// Only edges whose `type` property equals this.
    #[serde(default)]
    pub edge_type: Option<String>,
    #[serde(default)]
    pub property_equals: BoundedVec<EdgePropertyEquals, MAX_EDGE_FILTERS>,
}

/// The edge indexes of one graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EdgeIndexStatusView {
    pub indexes: BoundedVec<ManagedIndexStatus, MAX_INDEX_STATUSES>,
}

/// One result edge, identified as an edge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EdgeSearchHit {
    pub source: String,
    pub target: String,
    /// The edge's position among the parallel edges of `(source, target)`.
    pub ordinal: u32,
    /// The distance (vector: smaller is nearer) or BM25 score (text).
    pub score: f64,
}

/// The answer to one edge search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EdgeSearchView {
    pub hits: BoundedVec<EdgeSearchHit, MAX_EDGE_SEARCH_HITS>,
    /// The generation that served, when the maintained index did.
    #[serde(default)]
    pub generation: Option<u64>,
    /// Why the bounded exact path answered instead, when it did.
    #[serde(default)]
    pub fallback: Option<String>,
}
