//! Step 1 of the Q7/Q11 pipeline: pull a NISQ-sized candidate node set via UQL and
//! materialize the induced subgraph over it.
//!
//! This is deliberately a THIN wrapper over the engine's EXISTING query surface
//! (`eg_plan::uql::parse` + `eg_plan::execute`) — no new query language, no new
//! executor. A caller hands a UQL string (e.g. `"MATCH (:Concept) LIMIT 12"`); this
//! module runs it against one `GraphCore::analysis_snapshot()` (an OCC-consistent
//! point-in-time view, per `eg-plan`'s own snapshot-isolation guarantee) and reads
//! back which of the SAME snapshot's edges connect two selected nodes.
//!
//! Max-Cut treats the graph as undirected and unweighted-by-default: two candidate
//! nodes are "connected" for the cost Hamiltonian if EITHER direction carries an
//! edge in the snapshot, deduplicated to one logical edge. A `weight` property (if
//! present as a JSON number on any edge blob between the pair) overrides the default
//! weight of `1.0`; multiple parallel edges between the same pair sum their weights.

use std::collections::{BTreeMap, HashSet};

use eg_core::graph::{GraphCore, GraphView};
use eg_core::compute::semantic::SemanticStore;
use eg_plan::exec::PlanCtx;

/// The induced subgraph over a UQL-selected candidate node set: node ids in
/// selection order (order matters — it fixes the qubit index each node maps to
/// throughout the rest of the pipeline) and the deduplicated undirected weighted
/// edge list among them.
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateSubgraph {
    /// Candidate node ids, in the order the UQL query returned them. `node_ids[i]`
    /// is qubit `i` for the rest of the pipeline (`circuit.rs`).
    pub node_ids: Vec<String>,
    /// Deduplicated undirected edges among `node_ids`, as `(qubit_i, qubit_j,
    /// weight)` with `qubit_i < qubit_j` — already resolved to qubit INDICES (not
    /// node ids), since that is the only thing `circuit.rs`/`hamiltonian.rs` need.
    pub edges: Vec<(u32, u32, f64)>,
}

impl CandidateSubgraph {
    pub fn n_qubits(&self) -> u32 {
        self.node_ids.len() as u32
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubgraphError {
    #[error("UQL parse error: {0}")]
    Parse(String),
    #[error("UQL execution error: {0}")]
    Exec(String),
    #[error("candidate set has {n} nodes, which exceeds max_qubits={max}; narrow the UQL query (e.g. a tighter WHERE or LIMIT)")]
    TooManyQubits { n: usize, max: u32 },
    #[error("candidate set is empty — UQL query `{uql}` selected no nodes")]
    Empty { uql: String },
}

/// Pull a candidate node set via `uql` and materialize the induced subgraph, capped
/// at `max_qubits` (pass the target backend's `max_qubits_statevector`, e.g.
/// `StateVectorSimulator::capabilities().max_qubits_statevector`, so an
/// over-large selection fails fast here rather than deep inside the simulator).
pub fn pull_candidate_subgraph(
    core: &GraphCore,
    uql: &str,
    max_qubits: u32,
) -> Result<CandidateSubgraph, SubgraphError> {
    let plan = eg_plan::uql::parse(uql).map_err(|e| SubgraphError::Parse(e.render(uql)))?;
    let view: GraphView = core.analysis_snapshot();
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic);
    let rowset = eg_plan::execute(&plan, &ctx).map_err(SubgraphError::Exec)?;

    let node_ids = rowset.ids();
    if node_ids.is_empty() {
        return Err(SubgraphError::Empty {
            uql: uql.to_string(),
        });
    }
    if node_ids.len() as u32 > max_qubits {
        return Err(SubgraphError::TooManyQubits {
            n: node_ids.len(),
            max: max_qubits,
        });
    }

    Ok(materialize_induced_subgraph(&view, node_ids))
}

/// Build the induced-subgraph edge list directly from an already-held `GraphView` +
/// an already-known candidate id list — split out from [`pull_candidate_subgraph`]
/// so a caller that already ran its OWN UQL/Cypher/SQL selection (or a hand-built
/// id list, e.g. in a benchmark) can reuse the SAME induction logic without a
/// second UQL round trip.
pub fn materialize_induced_subgraph(view: &GraphView, node_ids: Vec<String>) -> CandidateSubgraph {
    let index_of: BTreeMap<&str, u32> = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i as u32))
        .collect();
    let candidate_set: HashSet<&str> = node_ids.iter().map(String::as_str).collect();

    // (qubit_i, qubit_j) with i < j -> summed weight. A BTreeMap keeps edge order
    // deterministic (qubit-index order), which keeps the built QuantumProgram —
    // and therefore its `circuit_hash` — reproducible for the SAME candidate set.
    let mut weights: BTreeMap<(u32, u32), f64> = BTreeMap::new();
    for ((source, target), blobs) in &view.edge_properties {
        if !candidate_set.contains(source.as_str()) || !candidate_set.contains(target.as_str()) {
            continue;
        }
        if source == target {
            continue; // no self-loops in a Max-Cut instance
        }
        let a = index_of[source.as_str()];
        let b = index_of[target.as_str()];
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        for blob in blobs {
            let weight = eg_types::msgpack::decode_property_value(blob)
                .ok()
                .and_then(|v| v.get("weight").and_then(|w| w.as_f64()))
                .unwrap_or(1.0);
            *weights.entry((lo, hi)).or_insert(0.0) += weight;
        }
    }

    // NOTE on directionality: a pair connected by edge records in BOTH directions
    // (e.g. two separate directed relationship facts between the same nodes) sums
    // BOTH weights into one undirected Max-Cut edge — that is a deliberate modeling
    // choice (two distinct edge records really do carry twice the "connectedness"),
    // not a double-count bug; a single-direction pair (the common case for this
    // engine's directed relationship edges) is summed exactly once, from its one
    // direction's blob(s).
    let edges = weights
        .into_iter()
        .map(|((i, j), w)| (i, j, w))
        .collect();

    CandidateSubgraph { node_ids, edges }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn build_triangle() -> GraphCore {
        let core = GraphCore::new();
        for id in ["a", "b", "c", "d"] {
            core.add_node(id.into(), blob(json!({ "type": "Concept" })));
        }
        for (s, t) in [("a", "b"), ("b", "c"), ("c", "a")] {
            core.add_edge(s.into(), t.into(), blob(json!({ "relationship": "RELATED_TO" })))
                .unwrap();
        }
        core
    }

    #[test]
    fn pulls_candidate_set_and_induces_subgraph() {
        let core = build_triangle();
        let sub = pull_candidate_subgraph(&core, "MATCH (:Concept) LIMIT 10", 24).unwrap();
        assert_eq!(sub.node_ids.len(), 4);
        // Triangle a-b-c has 3 edges; "d" is isolated, contributes none.
        assert_eq!(sub.edges.len(), 3);
        for (i, j, w) in &sub.edges {
            assert!(*i < *j);
            assert_eq!(*w, 1.0);
        }
    }

    #[test]
    fn too_many_qubits_is_a_typed_error() {
        let core = build_triangle();
        let err = pull_candidate_subgraph(&core, "MATCH (:Concept) LIMIT 10", 2).unwrap_err();
        assert!(matches!(err, SubgraphError::TooManyQubits { n: 4, max: 2 }));
    }

    #[test]
    fn empty_selection_is_a_typed_error() {
        let core = build_triangle();
        let err = pull_candidate_subgraph(&core, "MATCH (:NoSuchLabel) LIMIT 10", 24).unwrap_err();
        assert!(matches!(err, SubgraphError::Empty { .. }));
    }
}
