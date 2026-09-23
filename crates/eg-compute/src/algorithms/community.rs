//! Compatibility facade for the canonical graph-algorithm Leiden kernel.
//!
//! EH-282: this used to route through the sole Louvain implementation in
//! [`crate::graph_algos`]. Louvain's own local-moving phase can, purely from
//! the order nodes happen to be visited in one sweep, leave a returned
//! community internally DISCONNECTED (Traag, Waltman & van Eck 2019 measure up
//! to ~25% badly connected, ~16% fully disconnected on real networks) — a
//! correctness bug once a community becomes a module an agent trusts. Leiden's
//! refinement phase (`graph_algos::leiden`) makes connectivity a structural
//! guarantee instead of a typical outcome, sharing Louvain's own
//! `aggregate`/`modularity_of`/multilevel driver rather than re-deriving them
//! (see that module's doc), so this switch is wiring, not new implementation.

use std::collections::HashMap;
use std::time::Duration;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::GraphView;
use crate::graph_algos::{leiden, AdjacencyGraph, LeidenConfig, QualityFunction};

/// Detect communities through the sole Leiden implementation in
/// [`crate::graph_algos`]. The adapter preserves isolated nodes and translates
/// the engine's directed topology into the generic graph-algorithm value; the
/// canonical kernel owns symmetrisation, modularity, ordering, connectivity,
/// and work caps.
pub fn community_detection(core: &GraphView, resolution: f64) -> Vec<Vec<String>> {
    let mut node_ids: Vec<String> = core.node_map.keys().cloned().collect();
    node_ids.sort_unstable();
    let adjacency = weighted_adjacency(core, &node_ids);
    let graph = AdjacencyGraph::from_adjacency(node_ids.into_iter().zip(adjacency));
    // Budget: 15s — the exact wall-clock bound `COMMUNITY_DETECTION_BUDGET`
    // imposed on this same path before commit `a14b9c28` removed it along with
    // the duplicate kernel. This facade serves BOTH request-reachable
    // handlers (`Method::CommunityDetection` and the caller-sized
    // `Method::CommunityDetectEphemeral`), so it gets the interactive budget:
    // a request must not be able to occupy a compute thread indefinitely.
    leiden(
        &graph,
        &LeidenConfig {
            resolution,
            budget: Duration::from_secs(15),
            ..LeidenConfig::default()
        },
    )
    .communities
}

/// EH-314: stateless community detection over an explicitly WEIGHTED,
/// caller-supplied call graph — the `CommunityDetectEphemeral` wire method's
/// own per-edge weight and quality-function selector, rather than
/// [`community_detection`]'s persisted-topology `edge_properties` blob.
/// Building the [`AdjacencyGraph`] directly from the supplied `(source,
/// target, weight)` triples — instead of round-tripping through a scratch
/// [`GraphView`] the way the ephemeral handler used to — is what makes a
/// weight/quality slot possible on this wire method at all: a throwaway
/// `GraphView` has no `edge_properties` to read a weight back out of unless
/// the caller also fabricates a properties blob per edge, which is exactly
/// the round-trip EH-314 exists to avoid.
///
/// A node id in `node_ids` that never appears in `edges` is preserved as an
/// isolated (zero-degree) node, matching [`community_detection`]'s own
/// contract. An edge endpoint absent from `node_ids` is silently dropped —
/// the caller is expected to pass the exact node set the edges range over,
/// as `enrichment/features.py::cluster_features` already does.
pub fn community_detection_weighted(
    mut node_ids: Vec<String>,
    edges: Vec<(String, String, f64)>,
    resolution: f64,
    quality: QualityFunction,
) -> Vec<Vec<String>> {
    node_ids.sort_unstable();
    node_ids.dedup();
    let index: HashMap<&str, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();

    let mut adjacency: Vec<Vec<(String, f64)>> = vec![Vec::new(); node_ids.len()];
    for (source, target, weight) in &edges {
        // Same `weight > 0.0` admission [`weighted_adjacency`] applies below —
        // a zero/negative weight (never sent by a well-behaved caller, but not
        // ruled out by the wire schema) contributes nothing to Leiden's
        // objective either way, so dropping it up front keeps the two
        // adjacency builders' admission rule identical.
        if *weight <= 0.0 {
            continue;
        }
        if let (Some(&si), Some(&ti)) = (index.get(source.as_str()), index.get(target.as_str())) {
            adjacency[si].push((node_ids[ti].clone(), *weight));
        }
    }

    let graph = AdjacencyGraph::from_adjacency(node_ids.into_iter().zip(adjacency));
    // Same 15s interactive budget as `community_detection` — this facade
    // serves the SAME caller-sized `Method::CommunityDetectEphemeral` request
    // that function used to, just with an explicit weight/quality slot.
    leiden(
        &graph,
        &LeidenConfig {
            resolution,
            quality,
            budget: Duration::from_secs(15),
            ..LeidenConfig::default()
        },
    )
    .communities
}

/// EH-284: the weighted adjacency list [`community_detection`] clusters
/// over. Collects the unique topology-confirmed `(source, target)` pairs
/// first — not from `core.edge_properties` directly — so this stays scoped
/// to exactly what the live petgraph snapshot carries (e.g. whatever
/// row-visibility filtering already ran on `core.graph`), then asks
/// `edge_properties` how much each pair topology already confirmed is an
/// edge should weigh, via [`resolver_confidence_weight`] — which folds every
/// PARALLEL typed edge between the same pair into ONE weight, rather than
/// one unweighted adjacency-list entry per topology edge instance the way
/// this facade used to. Extracted out of `community_detection` so that
/// function keeps its own pre-EH-284 shape (this is where EH-284's new
/// branching lives instead).
fn weighted_adjacency(core: &GraphView, node_ids: &[String]) -> Vec<Vec<(String, f64)>> {
    let indexes: HashMap<&str, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, node_id)| (node_id.as_str(), index))
        .collect();

    let mut pairs: std::collections::BTreeSet<(usize, usize)> = std::collections::BTreeSet::new();
    for edge in core.graph.edge_references() {
        let source = core.graph[edge.source()].as_str();
        let target = core.graph[edge.target()].as_str();
        if let (Some(&source_index), Some(&target_index)) =
            (indexes.get(source), indexes.get(target))
        {
            pairs.insert((source_index, target_index));
        }
    }
    let mut adjacency = vec![Vec::new(); node_ids.len()];
    for (source_index, target_index) in pairs {
        let weight =
            resolver_confidence_weight(core, &node_ids[source_index], &node_ids[target_index]);
        if weight > 0.0 {
            adjacency[source_index].push((node_ids[target_index].clone(), weight));
        }
    }
    adjacency
}

/// EH-284: the total community-detection weight of every typed edge this
/// snapshot carries between `source` and `target`, summed once per unique
/// pair — so three parallel `calls` edges of confidence 0.9 each contribute
/// 2.7 ("three separate pieces of resolver evidence"), not three identical
/// unweighted hops. Falls back to `1.0` (today's pre-EH-284 behaviour) when
/// the pair carries no decodable properties at all, so an edge from a caller
/// that predates this convention counts exactly as it always did.
fn resolver_confidence_weight(core: &GraphView, source: &str, target: &str) -> f64 {
    match core
        .edge_properties
        .get(&(source.to_string(), target.to_string()))
    {
        Some(blobs) if !blobs.is_empty() => blobs
            .iter()
            .filter_map(|blob| eg_types::msgpack::decode_property_value(blob).ok())
            .map(|properties| edge_weight(&properties))
            .sum(),
        _ => 1.0,
    }
}

/// Decode one edge-properties blob into its EH-284 community-detection
/// weight.
///
/// A MinHash `similar_to` edge (`parser::resolve`'s LSH-banded clone
/// detector) is a RESEMBLANCE signal — two symbols merely look alike — not a
/// structural one, so per the EH-284 ruling it is EXCLUDED outright (weight
/// `0.0`) rather than allowed to vote on module boundaries: two similarly
/// shaped utility files are not a module. Every other (structural:
/// `calls`/`inherits`/`realizes`/...) edge counts at its resolver confidence —
/// `parser::resolve::resolve_site`'s 0.95 for a scoped self/receiver bind down
/// to 0.6 for a same-name-unique guess — read from the `confidence` property,
/// the repo-wide edge-type convention already used by
/// `reasoning::edge_relationship_facts` for `relationship`. Missing
/// `confidence` defaults to `1.0` (an edge with no recorded confidence, or
/// from a persist path that predates this convention, counts exactly as it
/// always did).
fn edge_weight(properties: &serde_json::Value) -> f64 {
    if properties.get("relationship").and_then(|v| v.as_str()) == Some("similar_to") {
        return 0.0;
    }
    decoded_f64(properties, "confidence").unwrap_or(1.0)
}

/// Read a numeric property that may have been persisted as a JSON number OR
/// as a numeric STRING. `parser::resolve::record_call_resolution` itself
/// writes `confidence` as `format!("{confidence:.2}")` (a string); a
/// downstream persist path (outside this crate) is free to re-encode it as a
/// number instead, so both forms must decode.
fn decoded_f64(properties: &serde_json::Value, key: &str) -> Option<f64> {
    let value = properties.get(key)?;
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(json: serde_json::Value) -> serde_json::Value {
        json
    }

    #[test]
    fn similar_to_edge_is_excluded() {
        assert_eq!(
            edge_weight(&props(
                serde_json::json!({"relationship": "similar_to", "score": "0.90"})
            )),
            0.0
        );
    }

    #[test]
    fn structural_edge_uses_string_confidence() {
        assert_eq!(
            edge_weight(&props(
                serde_json::json!({"relationship": "calls", "confidence": "0.60"})
            )),
            0.60
        );
    }

    #[test]
    fn structural_edge_uses_numeric_confidence() {
        assert_eq!(
            edge_weight(&props(
                serde_json::json!({"relationship": "calls", "confidence": 0.95})
            )),
            0.95
        );
    }

    #[test]
    fn missing_confidence_defaults_to_one() {
        assert_eq!(
            edge_weight(&props(serde_json::json!({"relationship": "inherits"}))),
            1.0
        );
    }

    #[test]
    fn edge_with_no_relationship_key_defaults_to_full_weight() {
        // A caller that predates the `relationship`/`confidence` convention
        // (or any edge the KG stores without them) must see NO regression:
        // exactly the old uniform weight of 1.0.
        assert_eq!(edge_weight(&props(serde_json::json!({}))), 1.0);
    }
}
