//! Stable facade for the native graph-compute operations.
//!
//! Implementations live in purpose modules so traversal, community, hierarchy,
//! finance, similarity/resolution, batch, and metrics work can evolve without
//! recreating one high-contention monolith. The re-exports preserve the public
//! `crate::algorithms::*` contract used by server and embedded callers.

#[path = "algorithms/batch.rs"]
pub mod batch;
#[path = "algorithms/community.rs"]
pub mod community;
#[path = "algorithms/finance.rs"]
pub mod finance;
#[path = "algorithms/graph_traversal.rs"]
pub mod graph_traversal;
#[path = "algorithms/hierarchy.rs"]
pub mod hierarchy;
#[path = "algorithms/metrics.rs"]
pub mod metrics;
#[path = "algorithms/similarity_resolution.rs"]
pub mod similarity_resolution;

#[cfg(test)]
mod tests;

pub use batch::{
    batch_update, batch_update_preview, decode_batch_operations, merge_batch_node_properties,
    BatchOperation,
};
pub use community::community_detection;
pub use finance::{
    compute_exponential_decay, compute_rolling_mean, compute_rolling_std, compute_rolling_zscore,
    simulate_order_matching,
};
pub use graph_traversal::{
    betweenness_centrality, compute_degree_centrality, connected_components, degree_centrality_all,
    find_cycle, get_blast_radius, get_shortest_path, graph_coloring, minimum_spanning_tree,
    pagerank, strongly_connected_components, topological_sort,
};
pub use hierarchy::{
    cluster_hierarchy, format_cluster_id, parse_cluster_id, ClusterHierarchyResult,
    ClusterLevelResult, ClusterMeta,
};
pub use metrics::{compute_metrics, get_context_view, personalized_pagerank, prune_by_lifecycle};
pub use similarity_resolution::{compute_similarity_edges, resolve_candidates, MergeProposal};
