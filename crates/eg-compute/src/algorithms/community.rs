//! Compatibility facade for the canonical graph-algorithm Louvain kernel.

use std::collections::HashMap;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::GraphView;
use crate::graph_algos::{louvain, AdjacencyGraph, LouvainConfig};

/// Detect communities through the sole Louvain implementation in
/// [`crate::graph_algos`]. The adapter preserves isolated nodes and translates
/// the engine's directed topology into the generic graph-algorithm value; the
/// canonical kernel owns symmetrisation, modularity, ordering, and work caps.
pub fn community_detection(core: &GraphView, resolution: f64) -> Vec<Vec<String>> {
    let mut node_ids: Vec<String> = core.node_map.keys().cloned().collect();
    node_ids.sort_unstable();
    let indexes: HashMap<&str, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, node_id)| (node_id.as_str(), index))
        .collect();
    let mut adjacency = vec![Vec::new(); node_ids.len()];
    for edge in core.graph.edge_references() {
        let source = core.graph[edge.source()].as_str();
        let target = core.graph[edge.target()].as_str();
        if let (Some(&source_index), Some(&target_index)) =
            (indexes.get(source), indexes.get(target))
        {
            adjacency[source_index].push((node_ids[target_index].clone(), 1.0));
        }
    }
    let graph = AdjacencyGraph::from_adjacency(node_ids.into_iter().zip(adjacency));
    louvain(
        &graph,
        &LouvainConfig {
            resolution,
            ..LouvainConfig::default()
        },
    )
    .communities
}
