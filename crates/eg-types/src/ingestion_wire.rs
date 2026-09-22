//! Pure-data result bodies of the `ingestion` contract domain: the source-code parse
//! and repository-index graphs, the screen-observation graph, and discovery hits.
//!
//! They live here, at the bottom of the crate DAG, so the result contract
//! (`result_contract::ingestion`) can name the exact body a handler encodes; the
//! parsers and the screen enrichment (`eg-compute`) re-export them.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

pub const MAX_INDEX_DIAGNOSTICS_PER_FILE: usize = 8;

/// Per-file disposition produced by [`IndexResult`].  It is deliberately
/// separate from `ParseResult`: an unsupported parser capability and a source
/// file containing no declarations are not the same outcome.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IndexFileStatus {
    Success,
    Unsupported,
    Error,
}

/// One bounded, machine-readable diagnostic for a repository-index input.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndexDiagnostic {
    pub code: String,
    pub message: String,
}

/// The exact outcome of one input file in an `IndexRepository` batch.
///
/// Outcomes are returned one-for-one and in the same order as the submitted
/// files. Both digests use the canonical `sha256:<lowercase-hex>` spelling.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndexFileOutcome {
    pub file_path: String,
    pub status: IndexFileStatus,
    pub content_digest: String,
    pub parser_capability_digest: String,
    /// Bounded by the engine. The current parser emits at most one diagnostic
    /// for an input, while the vector leaves room for richer parsers without a
    /// wire-shape change.
    pub diagnostics: BoundedVec<IndexDiagnostic, MAX_INDEX_DIAGNOSTICS_PER_FILE>,
}

/// An extracted graph node: the shape the AST and screen enrichments share, so the
/// caller's persist path is one.
#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExtractedNode {
    pub node_id: String,
    pub node_type: String,
    pub properties: HashMap<String, String>,
}

/// An extracted graph edge.
#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExtractedEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: HashMap<String, String>,
}

/// Result of `Method::ParseFile`, and one entry of `Method::ParseFiles`: the raw,
/// unresolved symbol graph of one source file.
#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ParseResult {
    pub nodes: Vec<ExtractedNode>,
    pub edges: Vec<ExtractedEdge>,
    pub symbols_extracted: usize,
}

/// Resolved, cross-file symbol graph for a batch of files — the response shape
/// of the `IndexRepository` RPC. Unlike `ParseFiles` (one raw `ParseResult` per
/// file), this is a SINGLE merged graph whose `calls`/`inherits`/`realizes`/
/// `depends_on` edges point at real node ids.
#[derive(Serialize, Deserialize, Debug, Default)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IndexResult {
    /// Every SYMBOL node across all files — ONE row per declaration site, never
    /// deduplicated. The doc used to claim "deduplicated by node id" and
    /// `collect_result_nodes` never did it; the DOC was the wrong half. Under the
    /// old content-addressed id, deduplicating would have been a data-loss bug:
    /// byte-identical declarations shared an id but not their facts (`file_path`,
    /// `line`, byte range), so collapsing them would have thrown away every
    /// occurrence but the first. Ids are now per-occurrence
    /// (CONCEPT:EG-KG.compute.symbol-occurrence-id), so uniqueness holds by
    /// construction and there is nothing left to dedupe — provided the batch
    /// carries each file path once, which is the caller's contract (a path
    /// repeated in `files` is parsed twice and would repeat its ids). The
    /// internal `call_sites` resolution-input property is stripped before return.
    pub nodes: Vec<ExtractedNode>,
    /// `IMPLEMENTS` (file→symbol) + resolved `calls` (symbol→symbol) + `inherits`/
    /// `realizes` (class→class) + resolved `depends_on` (file→file). Raw unresolved
    /// `calls_raw`/`depends_on_raw` edges are dropped — they're superseded here.
    pub edges: Vec<ExtractedEdge>,
    pub symbols_extracted: usize,
    /// Successfully parsed inputs. This is not the submitted batch size: see
    /// `file_outcomes` for unsupported and failed inputs.
    pub files_parsed: usize,
    /// Exactly one outcome per submitted file, in input order. An unsupported
    /// extension is explicit and never represented as an empty success.
    pub file_outcomes: Vec<IndexFileOutcome>,
    /// Call sites bound to a definition (numerator of call-resolution coverage).
    pub calls_resolved: usize,
    /// Call sites seen but not bound (external/stdlib/ambiguous) — the remainder.
    pub calls_unresolved: usize,
    /// Of `calls_resolved`, those bound by receiver/class scope (CONCEPT:EG-KG.compute.type-scope-resolved-call).
    pub calls_scope_resolved: usize,
    /// Of `calls_resolved`, those disambiguated by argument-count match.
    pub calls_type_resolved: usize,
    /// Class→base `inherits` edges emitted.
    pub inherits_edges: usize,
    /// Class→interface `realizes` edges emitted.
    pub realizes_edges: usize,
    /// Model-free `similar_to` edges emitted (CONCEPT:EG-KG.compute.model-free-similar-code).
    pub similar_edges: usize,
    /// Import statements bound to an in-batch file.
    pub imports_resolved: usize,
    /// Import statements seen but not bound (external packages, unknown layout).
    pub imports_unresolved: usize,
}

/// Result of `Method::ObserveScreen`: one captured frame as session/frame/UI-element
/// graph entities.
#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScreenObservationResult {
    /// The session node + the frame node + one node per UI element.
    pub nodes: Vec<ExtractedNode>,
    /// session-`hasObservation`->frame, frame-`hasElement`->element, and
    /// prevframe-`succeededBy`->frame (only when the frame actually changed).
    pub edges: Vec<ExtractedEdge>,
    pub frame_id: String,
    pub width: u32,
    pub height: u32,
    /// FNV-1a hash of the PNG bytes — the caller passes it back as `prev_hash`.
    pub hash: u64,
    /// False when the frame is byte-identical to the previous one (no visual change).
    pub changed: bool,
    pub element_count: usize,
}

/// One ranked node of `Method::Discover`, hydrated with its human-readable text.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DiscoverHit {
    pub id: String,
    /// The node's `name`, or its id when it has none.
    pub name: String,
    pub description: String,
    #[serde(rename = "type")]
    pub node_type: String,
    /// Combined keyword + semantic score.
    pub score: f32,
}
