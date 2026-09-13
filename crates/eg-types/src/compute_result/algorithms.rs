//! Results of the always-built graph algorithms and the VIZ-1 cluster hierarchy.

use serde::{Deserialize, Serialize};

/// Materialized result of a bounded VF2 subgraph-isomorphism search.
///
/// `truncated` is true when either the requested match limit or search-step
/// budget stopped the search before the candidate space was exhausted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Vf2MatchResult {
    pub matches: Vec<std::collections::HashMap<String, String>>,
    pub truncated: bool,
}

/// One capability term found in a query by the ontology lexical gate
/// (CONCEPT:EG-ORCH.routing.lexical-capability-escalation). `term` is the matched alias/name, `node_type` its capability
/// class (Tool/Skill/MCPServer/…), `label` the owning node's display name,
/// `mcp_server` the owning fleet server (so a caller can bind that server's
/// toolset directly), and `score` the matched term's character length.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyMatch {
    pub term: String,
    pub node_type: String,
    pub label: String,
    pub mcp_server: String,
    pub score: f64,
}

/// `ClusterHierarchyRefresh`: what was computed and whether it was cached durably.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterHierarchySummary {
    pub graph: String,
    /// Number of computed levels (level 1 is the finest).
    pub levels: usize,
    pub base_node_count: usize,
    pub base_edge_count: usize,
    /// Cluster count of the coarsest level; 0 when no level was computed.
    pub top_level_clusters: usize,
    /// `false` when the backend has no persistence to cache the hierarchy in.
    pub cached: bool,
}

/// One cluster as served to a visualization client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterSummary {
    /// Stable, globally-addressable id: `"L{level}-{local_index}"`.
    pub id: String,
    pub label: String,
    pub node_count: usize,
    /// Sum of internal-edge weight (directed).
    pub edge_count: f64,
    /// Up to 5 most common node types among the members, descending.
    pub top_node_types: Vec<(String, usize)>,
}

/// An edge between two clusters of one served level, by array-local index into
/// that response's `clusters`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InterClusterEdge {
    pub src_idx: u32,
    pub dst_idx: u32,
    pub weight: f64,
}

/// `ClusterHierarchyClusters`: one level of the cached hierarchy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterLevelView {
    pub level: usize,
    pub clusters: Vec<ClusterSummary>,
    pub inter_cluster_edges: Vec<InterClusterEdge>,
}

/// A member node of a level-1 cluster, read live off the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterMemberNode {
    pub id: String,
    /// The node's stored properties, decoded; `null` when undecodable.
    pub properties: serde_json::Value,
}

/// An edge between two member nodes of a level-1 cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterMemberEdge {
    pub src_id: String,
    pub dst_id: String,
    /// The edge's `relationship` property, or `"_"` when it has none.
    #[serde(rename = "type")]
    pub relationship: String,
}

/// `ClusterHierarchyExpand`: a level-1 cluster expands to its member nodes and edges
/// (`child_clusters` empty); a coarser cluster expands to its children one level
/// finer (`nodes` and `edges` empty).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterExpansion {
    pub nodes: Vec<ClusterMemberNode>,
    pub edges: Vec<ClusterMemberEdge>,
    pub child_clusters: Vec<ClusterSummary>,
}
