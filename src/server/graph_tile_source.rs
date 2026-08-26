//! VIZ-1/VIZ-2 bridge: [`RealClusterSource`], the real `GraphCore`-backed
//! `eg_viz_graph_tiles::contract::GraphSource` VIZ-2's `graph_tile_server`
//! serves once a graph has a cached cluster hierarchy
//! (`Method::ClusterHierarchyRefresh`) — replacing the `DemoGraph` placeholder
//! VIZ-2 shipped with pending this lane. See `eg_viz_graph_tiles::contract`'s
//! own module doc: "Swapping in the real implementation later changes only
//! which `GraphSource` this crate's callers construct — neither the contract
//! types nor the wire encoding change." This is that swap; the contract types
//! and wire encoding are untouched.
//!
//! `GraphSource`'s methods are deliberately SYNCHRONOUS (a leaf, graph-agnostic
//! trait with no `async` in its signature — see the crate's own doc on why it
//! stays free of a `GraphCore` dependency). The async work (resolving
//! `graph_name` -> `Arc<GraphCore>` off the registry, and loading the
//! persisted hierarchy blob off `PersistenceBackend`) happens in
//! `graph_tile_server`'s route handlers BEFORE constructing this struct — by
//! the time `clusters`/`expand` run, everything they need is already
//! in-memory, so the trait's sync signature is never a problem.
//!
//! ## Cluster id encoding
//!
//! The contract's `ClusterSummary.id`/`expand(cluster_id)` are `u64`, but this
//! lane's own `Method::ClusterHierarchy*` RPC surface (`handlers::graph_ops.rs`,
//! `eg_compute::algorithms::ClusterMeta::id`) already has a stable STRING id,
//! `"L{level}-{local_index}"` (`crate::algorithms::format_cluster_id`/
//! `parse_cluster_id` — the single source of truth for that shape). Rather
//! than inventing a second identity scheme, this module packs the SAME
//! `(level, local_index)` pair into a `u64` (`level << 32 | local_index`) —
//! [`cluster_id_u64`]/[`decode_cluster_id_u64`] — so a tile server cluster id
//! and an RPC cluster id always name the same cluster, just in two wire
//! encodings for two different transports.
//!
//! ## Level numbering (a contract convention this lane pins, not a divergence)
//!
//! The shared contract does not specify whether `level` counts up from the
//! leaves or down from the root. This lane uses level 1 = the finest computed
//! level (matching `Method::ClusterHierarchyClusters`'s own already-shipped
//! convention) — so a client driving BOTH transports against the same graph
//! sees identical level numbers either way.
//!
//! ## No RLS projection here — a deliberate, pre-existing property of this listener
//!
//! `viz_interactive`'s loopback HTTP listener (127.0.0.1-only, no auth token,
//! no `verified_context`) already serves every OTHER route
//! (`/tile`/`/graph_tile/*` demo data) with no row-level-security filtering —
//! it is not on the authenticated RPC transport at all. This module reads the
//! raw `Arc<GraphCore>` off the registry, unfiltered, matching that existing
//! security posture rather than inventing a stricter one for just this route;
//! if this listener is ever exposed beyond loopback, that decision needs
//! revisiting for the WHOLE module, not patched here alone.

use std::collections::HashMap;
use std::sync::Arc;

use eg_viz_graph_tiles::contract::{
    ChildClusterRef, ClusterExpansion, ClusterLevel, ClusterSummary, GraphSource,
    InterClusterEdge, TileEdge, TileNode,
};

use crate::algorithms::{format_cluster_id, ClusterHierarchyResult};
use crate::graph::GraphCore;

/// Cap on `top_node_types` handed to the wire encoder — the CONTRACT's own
/// bound (`eg_viz_graph_tiles::wire::MAX_TOP_TYPES_PER_CLUSTER`), enforced
/// here rather than assumed: `cluster_hierarchy` already caps at 5 (well
/// under this), but this is the wire's actual limit, not an assumption about
/// the compute side staying under it forever.
const MAX_TOP_TYPES: usize = eg_viz_graph_tiles::wire::MAX_TOP_TYPES_PER_CLUSTER;

/// Pack `(level, local_index)` into the contract's `u64` cluster id — see the
/// module doc's "Cluster id encoding" section.
fn cluster_id_u64(level: usize, local_index: usize) -> u64 {
    ((level as u64) << 32) | (local_index as u64 & 0xFFFF_FFFF)
}

/// Inverse of [`cluster_id_u64`].
fn decode_cluster_id_u64(id: u64) -> (usize, usize) {
    ((id >> 32) as usize, (id & 0xFFFF_FFFF) as usize)
}

/// `usize`/`f64` -> the wire's `u32` counters: saturating, never panicking on
/// an oversized value — a pathological graph should degrade to a clamped
/// count, not crash the tile server.
fn as_u32_saturating_usize(value: usize) -> u32 {
    value.min(u32::MAX as usize) as u32
}

fn as_u32_saturating_f64(value: f64) -> u32 {
    if value.is_finite() && value > 0.0 {
        value.min(u32::MAX as f64).round() as u32
    } else {
        0
    }
}

fn cap_top_types(top: &[(String, usize)]) -> Vec<String> {
    top.iter()
        .take(MAX_TOP_TYPES)
        .map(|(t, _)| t.clone())
        .collect()
}

/// The real, `GraphCore`-backed [`GraphSource`] (VIZ-1 supplying VIZ-2's
/// contract). Holds an ALREADY-LOADED, decoded hierarchy (no I/O inside the
/// trait methods) plus the live `Arc<GraphCore>` `expand()`'s level-1 branch
/// reads real node/edge content off — mirrors `Method::ClusterHierarchyExpand`'s
/// own handler in `handlers::graph_ops.rs` (same source of truth, same
/// "membership is frozen at refresh time, content is always live" split).
pub struct RealClusterSource {
    hierarchy: Arc<ClusterHierarchyResult>,
    core: Arc<GraphCore>,
}

impl RealClusterSource {
    pub fn new(hierarchy: Arc<ClusterHierarchyResult>, core: Arc<GraphCore>) -> Self {
        RealClusterSource { hierarchy, core }
    }

    /// A source with no cached hierarchy and no graph — every `clusters`/
    /// `expand` call degrades to an honest empty tile via the SAME
    /// out-of-range/unknown-id paths a populated source uses, rather than a
    /// second "is this source real" branch at every call site. Used by
    /// `graph_tile_server::resolve_source` when the requested graph doesn't
    /// exist, this build has no persistence backend, or that graph has no
    /// cached hierarchy yet (call `ClusterHierarchyRefresh` first).
    pub fn empty() -> Self {
        RealClusterSource {
            hierarchy: Arc::new(ClusterHierarchyResult {
                levels: Vec::new(),
                leaf_membership: Vec::new(),
                base_node_count: 0,
                base_edge_count: 0,
            }),
            core: Arc::new(GraphCore::new()),
        }
    }
}

impl GraphSource for RealClusterSource {
    fn clusters(&self, level: u32, parent: Option<u64>) -> ClusterLevel {
        let level_usize = level as usize;
        let Some(level_data) = level_usize
            .checked_sub(1)
            .and_then(|idx| self.hierarchy.levels.get(idx))
        else {
            // Out-of-range level: an honest empty tile, never a panic — this
            // trait's methods are infallible by contract, so "nothing at this
            // level" is the degrade-honestly answer (mirrors
            // `Method::ClusterHierarchyClusters`'s own out-of-range error,
            // just expressed as an empty tile instead of a wire error).
            return ClusterLevel {
                level,
                parent_cluster_id: parent,
                clusters: Vec::new(),
                inter_cluster_edges: Vec::new(),
            };
        };
        let parent_string = parent.map(|p| {
            let (pl, pidx) = decode_cluster_id_u64(p);
            format_cluster_id(pl, pidx)
        });

        let mut remap: HashMap<usize, u32> = HashMap::new();
        let mut clusters = Vec::new();
        for (i, c) in level_data.clusters.iter().enumerate() {
            if let Some(pid) = &parent_string {
                if c.parent_id.as_deref() != Some(pid.as_str()) {
                    continue;
                }
            }
            remap.insert(i, clusters.len() as u32);
            clusters.push(ClusterSummary {
                id: cluster_id_u64(level_usize, i),
                label: c.label.clone(),
                node_count: as_u32_saturating_usize(c.node_count),
                edge_count: as_u32_saturating_f64(c.edge_count),
                // No layout computed by this lane — never a fabricated (0, 0).
                // See the contract's own doc on this field.
                centroid: None,
                top_node_types: cap_top_types(&c.top_node_types),
            });
        }
        let inter_cluster_edges = level_data
            .inter_cluster_edges
            .iter()
            .filter_map(|&(s, d, w)| {
                let ls = *remap.get(&(s as usize))?;
                let ld = *remap.get(&(d as usize))?;
                Some(InterClusterEdge {
                    src_idx: ls,
                    dst_idx: ld,
                    weight: w as f32,
                })
            })
            .collect();

        ClusterLevel {
            level,
            parent_cluster_id: parent,
            clusters,
            inter_cluster_edges,
        }
    }

    fn expand(&self, cluster_id: u64) -> ClusterExpansion {
        let (level, local_index) = decode_cluster_id_u64(cluster_id);
        let empty = |nodes, edges, child_clusters| ClusterExpansion {
            cluster_id,
            nodes,
            edges,
            child_clusters,
        };
        if level == 1 {
            // Finest computed level: drill all the way to real graph nodes,
            // read LIVE off the graph rather than any stale snapshot inside
            // the cache — membership is what's frozen until the next
            // refresh, never the member nodes' own content.
            let member_ids: Vec<String> = self
                .hierarchy
                .leaf_membership
                .iter()
                .filter(|(_, idx)| *idx as usize == local_index)
                .map(|(id, _)| id.clone())
                .collect();
            if member_ids.is_empty() {
                return empty(Vec::new(), Vec::new(), Vec::new());
            }
            let sub = self.core.get_subgraph(&member_ids);
            let mut node_index: HashMap<String, u32> = HashMap::new();
            let mut nodes = Vec::with_capacity(sub.node_properties.len());
            for (id, blob) in &sub.node_properties {
                let props = eg_types::msgpack::decode_property_value(blob).ok();
                let node_type = props
                    .as_ref()
                    .and_then(|v| {
                        v.get("type")
                            .or_else(|| v.get("node_type"))
                            .and_then(|t| t.as_str())
                    })
                    .unwrap_or("_")
                    .to_string();
                node_index.insert(id.clone(), nodes.len() as u32);
                nodes.push(TileNode {
                    id: id.clone(),
                    label: id.clone(),
                    node_type,
                    // No layout computed by this lane — see `clusters`'
                    // `centroid` doc; the same honesty applies per-node here.
                    pos: None,
                });
            }
            let mut edges = Vec::new();
            for ((src, tgt), blobs) in &sub.edge_properties {
                let (Some(&si), Some(&ti)) = (node_index.get(src), node_index.get(tgt)) else {
                    continue;
                };
                for blob in blobs {
                    let props = eg_types::msgpack::decode_property_value(blob).ok();
                    let edge_type = props
                        .as_ref()
                        .and_then(|v| v.get("relationship").and_then(|t| t.as_str()))
                        .unwrap_or("_")
                        .to_string();
                    edges.push(TileEdge {
                        src_idx: si,
                        dst_idx: ti,
                        edge_type,
                    });
                }
            }
            empty(nodes, edges, Vec::new())
        } else {
            // A coarser cluster: drill down ONE level at a time — hand back
            // its children (from `level - 1`) rather than raw nodes, matching
            // "expand-on-demand" (the caller `expand`s again on one child to
            // go further, instead of every level materializing every node) —
            // mirrors `Method::ClusterHierarchyExpand`'s own non-level-1 arm.
            let this_id = format_cluster_id(level, local_index);
            let Some(child_level) = level
                .checked_sub(2)
                .and_then(|idx| self.hierarchy.levels.get(idx))
            else {
                return empty(Vec::new(), Vec::new(), Vec::new());
            };
            let child_clusters = child_level
                .clusters
                .iter()
                .enumerate()
                .filter(|(_, c)| c.parent_id.as_deref() == Some(this_id.as_str()))
                .map(|(child_idx, c)| ChildClusterRef {
                    id: cluster_id_u64(level - 1, child_idx),
                    label: c.label.clone(),
                    node_count: as_u32_saturating_usize(c.node_count),
                })
                .collect();
            empty(Vec::new(), Vec::new(), child_clusters)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::cluster_hierarchy;
    use crate::graph::GraphCore;

    fn p(node_type: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": node_type})).unwrap()
    }

    /// Build a real `GraphCore` with two bridged 4-cliques (the same fixture
    /// `eg-compute`'s own `cluster_hierarchy` tests use), refresh a hierarchy
    /// over it, and wrap both in a `RealClusterSource` — the round-trip this
    /// whole module exists for.
    fn two_bridged_cliques_source() -> (RealClusterSource, Vec<String>) {
        let core = GraphCore::new();
        let clique_a = ["a0", "a1", "a2", "a3"];
        let clique_b = ["b0", "b1", "b2", "b3"];
        for id in clique_a.iter().chain(clique_b.iter()) {
            core.add_node((*id).to_string(), p("Doc"));
        }
        for clique in [&clique_a, &clique_b] {
            for i in 0..clique.len() {
                for j in (i + 1)..clique.len() {
                    core.add_edge(clique[i].to_string(), clique[j].to_string(), p("_"))
                        .unwrap();
                }
            }
        }
        core.add_edge("a3".to_string(), "b0".to_string(), p("_"))
            .unwrap();
        let core = Arc::new(core);
        let snap = core.analysis_snapshot();
        let result = cluster_hierarchy(&snap, None, 1.0, 0);
        let all_ids: Vec<String> = clique_a
            .iter()
            .chain(clique_b.iter())
            .map(|s| s.to_string())
            .collect();
        (RealClusterSource::new(Arc::new(result), core), all_ids)
    }

    #[test]
    fn cluster_id_round_trips_through_the_u64_packing() {
        assert_eq!(decode_cluster_id_u64(cluster_id_u64(1, 0)), (1, 0));
        assert_eq!(decode_cluster_id_u64(cluster_id_u64(3, 12345)), (3, 12345));
    }

    #[test]
    fn clusters_at_level_1_covers_every_node_exactly_once() {
        let (source, all_ids) = two_bridged_cliques_source();
        let level1 = source.clusters(1, None);
        assert_eq!(level1.level, 1);
        assert_eq!(level1.clusters.len(), 2, "{:?}", level1.clusters);
        let total: u32 = level1.clusters.iter().map(|c| c.node_count).sum();
        assert_eq!(total as usize, all_ids.len());
        for c in &level1.clusters {
            assert!(c.centroid.is_none(), "no layout computed -- must never fabricate one");
            assert_eq!(c.top_node_types, vec!["Doc".to_string()]);
        }
    }

    #[test]
    fn out_of_range_level_returns_an_empty_tile_not_a_panic() {
        let (source, _) = two_bridged_cliques_source();
        let level = source.clusters(999, None);
        assert_eq!(level.level, 999);
        assert!(level.clusters.is_empty());
        assert!(level.inter_cluster_edges.is_empty());
    }

    #[test]
    fn expand_level_1_cluster_returns_real_member_nodes_confined_to_one_clique() {
        let (source, _) = two_bridged_cliques_source();
        let level1 = source.clusters(1, None);
        let first = &level1.clusters[0];
        let expansion = source.expand(first.id);
        assert!(expansion.child_clusters.is_empty());
        assert!(!expansion.nodes.is_empty());
        let ids: std::collections::BTreeSet<&str> =
            expansion.nodes.iter().map(|n| n.id.as_str()).collect();
        let clique_a: std::collections::BTreeSet<&str> =
            ["a0", "a1", "a2", "a3"].into_iter().collect();
        let clique_b: std::collections::BTreeSet<&str> =
            ["b0", "b1", "b2", "b3"].into_iter().collect();
        assert!(
            ids.is_subset(&clique_a) || ids.is_subset(&clique_b),
            "leiden must not split a complete clique across two clusters: {ids:?}"
        );
        // Every edge index must be in-bounds for THIS expansion's own nodes array.
        for e in &expansion.edges {
            assert!((e.src_idx as usize) < expansion.nodes.len());
            assert!((e.dst_idx as usize) < expansion.nodes.len());
        }
    }

    #[test]
    fn empty_source_degrades_honestly_at_every_call() {
        let source = RealClusterSource::empty();
        let level = source.clusters(1, None);
        assert!(level.clusters.is_empty());
        assert!(level.inter_cluster_edges.is_empty());
        let expansion = source.expand(cluster_id_u64(1, 0));
        assert!(expansion.nodes.is_empty());
        assert!(expansion.edges.is_empty());
        assert!(expansion.child_clusters.is_empty());
    }

    #[test]
    fn expand_unknown_cluster_returns_empty_not_a_panic() {
        let (source, _) = two_bridged_cliques_source();
        let expansion = source.expand(cluster_id_u64(1, 9999));
        assert!(expansion.nodes.is_empty());
        assert!(expansion.edges.is_empty());
        assert!(expansion.child_clusters.is_empty());
    }
}
