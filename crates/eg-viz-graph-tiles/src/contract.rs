//! The shared VIZ-1/VIZ-2/VIZ-3 graph-tile contract, as plain Rust/serde types.
//!
//! This mirrors the JSON shape the three lanes agreed on:
//!
//! ```text
//! clusters(graph, level, parent_cluster_id?) ->
//!   { level, clusters: [ {id, label, node_count, edge_count, centroid?, top_node_types} ],
//!     inter_cluster_edges: [ {src_idx, dst_idx, weight} ] }
//! expand(graph, cluster_id) ->
//!   { nodes: [...], edges: [ {src_idx, dst_idx, type} ], child_clusters: [...] }
//! ```
//!
//! [`ClusterLevel`] is the wire type for `clusters(...)`, [`ClusterExpansion`] for
//! `expand(...)`. [`crate::wire`] is the binary encoding of exactly these two
//! types (plus a streaming frame envelope); both types also derive
//! `serde::Serialize`, so plain `serde_json::to_vec` on the SAME values is the
//! JSON form used only for the size/time comparison the lane exists to justify
//! -- never a second wire format callers actually choose between.
//!
//! [`GraphSource`] is the trait boundary VIZ-1's real GraphCore-backed
//! hierarchical clustering plugs into. Until that lane merges, [`crate::demo`]
//! provides a deterministic, seeded, in-memory implementation -- the SAME
//! "engine-side generated, clearly labeled, never a fabricated real dataset"
//! idiom `eg_types::viz::VizDatasetSource::SyntheticGraph` already uses in
//! production for the (nodes-only) `MarkKind::Graph` static-export demo path.
//! Swapping in the real implementation later changes only which `GraphSource`
//! this crate's callers construct -- neither the contract types nor the wire
//! encoding change.

use serde::{Deserialize, Serialize};

/// One cluster in a [`ClusterLevel`] response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterSummary {
    pub id: u64,
    pub label: String,
    pub node_count: u32,
    pub edge_count: u32,
    /// `None` when the source has no meaningful 2D placement for this cluster
    /// yet (e.g. a level computed before layout ran) -- never a fabricated
    /// `(0, 0)`.
    pub centroid: Option<(f32, f32)>,
    /// The most common node types inside this cluster, most-common first.
    /// Bounded by the `GraphSource` (see [`crate::wire::MAX_TOP_TYPES_PER_CLUSTER`]).
    pub top_node_types: Vec<String>,
}

/// One inter-cluster edge in a [`ClusterLevel`] response. `src_idx`/`dst_idx`
/// index into THIS response's own `clusters` array (array-index-local, per the
/// shared contract) -- never a cluster id, and never a node id.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct InterClusterEdge {
    pub src_idx: u32,
    pub dst_idx: u32,
    pub weight: f32,
}

/// The wire type for `clusters(graph, level, parent_cluster_id?)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterLevel {
    pub level: u32,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent_cluster_id: Option<u64>,
    pub clusters: Vec<ClusterSummary>,
    pub inter_cluster_edges: Vec<InterClusterEdge>,
}

/// One node in a [`ClusterExpansion`] response. Carries its real string id --
/// unlike edges, a bounded-size cluster's worth of node ids is not the
/// "string ids at a million nodes are the payload" cost the contract exists to
/// avoid; only EDGES (which outnumber nodes several-fold in a typical graph)
/// pay that cost, so edges below reference nodes by index instead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TileNode {
    pub id: String,
    #[serde(default)]
    pub label: String,
    pub node_type: String,
    /// `None` when the source has no layout position for this node yet.
    pub pos: Option<(f32, f32)>,
}

/// One edge in a [`ClusterExpansion`] response. `src_idx`/`dst_idx` index into
/// THIS response's own `nodes` array -- the contract's core cost-saving move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TileEdge {
    pub src_idx: u32,
    pub dst_idx: u32,
    #[serde(rename = "type")]
    pub edge_type: String,
}

/// A child cluster reference in a [`ClusterExpansion`] response -- enough for a
/// client to draw a placeholder / decide whether to descend further, without a
/// second round trip through `clusters(...)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildClusterRef {
    pub id: u64,
    pub label: String,
    pub node_count: u32,
}

/// The wire type for `expand(graph, cluster_id)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterExpansion {
    pub cluster_id: u64,
    pub nodes: Vec<TileNode>,
    pub edges: Vec<TileEdge>,
    pub child_clusters: Vec<ChildClusterRef>,
}

/// The trait boundary a real clustering engine (VIZ-1) or this crate's own
/// [`crate::demo`] generator implements. Read-only, synchronous, and
/// deliberately graph-agnostic (no `GraphCore` dependency here -- see the
/// crate doc) so this crate stays a leaf next to `eg-viz-core`, exactly like
/// `eg-viz-columnstore`/`eg-viz-kernels`.
pub trait GraphSource {
    /// `parent` narrows to the children of one cluster from the previous
    /// level; `None` asks for the top-level clustering.
    fn clusters(&self, level: u32, parent: Option<u64>) -> ClusterLevel;
    /// Full node/edge detail for one cluster, plus its immediate children (if
    /// a finer level exists below it).
    fn expand(&self, cluster_id: u64) -> ClusterExpansion;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_level_json_round_trips_and_omits_absent_parent() {
        let level = ClusterLevel {
            level: 0,
            parent_cluster_id: None,
            clusters: vec![ClusterSummary {
                id: 1,
                label: "Person".to_string(),
                node_count: 100,
                edge_count: 50,
                centroid: Some((0.5, 0.5)),
                top_node_types: vec!["Person".to_string()],
            }],
            inter_cluster_edges: vec![InterClusterEdge {
                src_idx: 0,
                dst_idx: 0,
                weight: 1.0,
            }],
        };
        let json = serde_json::to_string(&level).unwrap();
        assert!(
            !json.contains("parent_cluster_id"),
            "absent parent must not appear on the wire"
        );
        let restored: ClusterLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, level);
    }

    #[test]
    fn cluster_expansion_json_round_trips() {
        let expansion = ClusterExpansion {
            cluster_id: 7,
            nodes: vec![TileNode {
                id: "n:1".to_string(),
                label: "Alice".to_string(),
                node_type: "Person".to_string(),
                pos: Some((0.1, 0.2)),
            }],
            edges: vec![TileEdge {
                src_idx: 0,
                dst_idx: 0,
                edge_type: "knows".to_string(),
            }],
            child_clusters: vec![ChildClusterRef {
                id: 8,
                label: "Sub".to_string(),
                node_count: 3,
            }],
        };
        let json = serde_json::to_string(&expansion).unwrap();
        let restored: ClusterExpansion = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, expansion);
    }
}
