//! The ShEx validation report (CONCEPT:EG-133) — a serde-serializable result of
//! validating a shape map (focus node → shape label) against a data graph.
//!
//! Terms (the focus node) are carried as their canonical N-Triples lexical form
//! (`<iri>` / `_:b` / `"lit"^^<dt>`) so the report is a flat, transport-friendly JSON
//! value the engine handler returns verbatim — no oxrdf types leak across the wire. This
//! mirrors the ShEx *result shape map*: for each (node, shape) pair, does the node
//! `conform` to the shape, and if not, a human `reason`.

use serde::{Deserialize, Serialize};

/// One entry of the result shape map — the outcome of testing one focus node against one
/// shape label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeResult {
    /// The focus node tested (N-Triples term form).
    pub node: String,
    /// The shape label the node was tested against (an IRI, or `START` for the schema's
    /// start shape).
    pub shape: String,
    /// Whether the node conforms to the shape.
    pub conforms: bool,
    /// A human explanation when `conforms` is false (the first failing reason).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A ShEx validation report — the aggregate over a whole shape map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShexReport {
    /// `true` iff EVERY tested (node, shape) pair conforms.
    pub conforms: bool,
    /// One [`NodeResult`] per shape-map entry, in input order.
    pub results: Vec<NodeResult>,
}

impl ShexReport {
    /// Build a report from its per-node results, deriving `conforms` (true iff every
    /// result conforms).
    pub fn from_results(results: Vec<NodeResult>) -> Self {
        let conforms = results.iter().all(|r| r.conforms);
        ShexReport { conforms, results }
    }
}
