//! Hierarchical Leiden projection and stable cluster identities.

use std::collections::HashMap;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::GraphView;

// ── VIZ-1: hierarchical Leiden clustering for graph visualization ──────────
//
// This delegates to `crate::graph_algos::leiden_hierarchy`, alongside the
// `community_detection` facade's delegation to canonical
// `crate::graph_algos::louvain`. It also reads `node_properties` (which the
// topology-only Louvain facade never needs) for label filtering and the
// `top_node_types` summary, so it takes the SAME `GraphView` shape
// `ComputeSimilarityEdges`/MST already read (`analysis_snapshot`, not
// `topology_snapshot`).

/// One cluster at one level of a computed [`ClusterHierarchyResult`]
/// (CONCEPT:EG-KG.compute.leiden-hierarchy, VIZ-1). `Serialize`/`Deserialize` so the whole
/// result can be MessagePack-encoded directly into
/// `server::persistence::cluster_hierarchy_store` — the wire/persisted shape
/// IS the compute shape, no separate DTO layer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterMeta {
    /// Stable, globally-addressable id: `"L{level}-{local_index}"`.
    pub id: String,
    pub label: String,
    pub node_count: usize,
    /// Sum of internal-edge weight (directed, NOT symmetrized — the graph's
    /// own edge direction, so this is directly comparable to a live
    /// `GetEdges` count, unlike a modularity-style doubled undirected count).
    pub edge_count: f64,
    /// This cluster's parent id at `level + 1`. `None` only at the root level.
    pub parent_id: Option<String>,
    /// Up to 5 most common node types among this cluster's members, descending.
    pub top_node_types: Vec<(String, usize)>,
}

/// One level of a [`ClusterHierarchyResult`]. `inter_cluster_edges` indices are
/// LOCAL to `clusters` (array-local, per the VIZ-1 program contract).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterLevelResult {
    pub level: usize,
    pub clusters: Vec<ClusterMeta>,
    pub inter_cluster_edges: Vec<(u32, u32, f64)>,
}

/// Full computed hierarchy (CONCEPT:EG-KG.compute.leiden-hierarchy, VIZ-1). `levels[0]` is
/// level 1 (finest). `leaf_membership` maps every clustered node id to its
/// LOCAL index into `levels[0].clusters` — the lookup `expand` needs to answer
/// "which level-1 cluster is node X in" without re-running Leiden.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterHierarchyResult {
    pub levels: Vec<ClusterLevelResult>,
    pub leaf_membership: Vec<(String, u32)>,
    pub base_node_count: usize,
    pub base_edge_count: usize,
}

/// Format a VIZ-1 cluster id — the single source of truth for `ClusterMeta::id`'s
/// `"L{level}-{local_index}"` shape, shared by every consumer (the
/// `Method::ClusterHierarchy*` RPC handlers AND VIZ-2's `graph_tile_server`,
/// which packs the same `(level, idx)` pair into its own `u64` cluster id --
/// see `server::graph_tile_source`) so the format can never drift between them.
pub fn format_cluster_id(level: usize, idx: usize) -> String {
    format!("L{level}-{idx}")
}

/// Parse a VIZ-1 cluster id of the form [`format_cluster_id`] produces, back
/// into `(level, local_index)`. Returns `None` for anything else — a
/// caller-supplied cluster_id is untrusted input, so this never panics on a
/// malformed string.
pub fn parse_cluster_id(id: &str) -> Option<(usize, usize)> {
    let rest = id.strip_prefix('L')?;
    let (level_str, idx_str) = rest.split_once('-')?;
    let level: usize = level_str.parse().ok()?;
    let idx: usize = idx_str.parse().ok()?;
    if level == 0 {
        return None;
    }
    Some((level, idx))
}

/// Top-`limit` `(type, count)` pairs by descending count, ties broken by type
/// name for determinism.
fn top_types(counts: &HashMap<String, usize>, limit: usize) -> Vec<(String, usize)> {
    let mut v: Vec<(String, usize)> = counts.iter().map(|(k, &c)| (k.clone(), c)).collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    v.truncate(limit);
    v
}

/// Compute the hierarchical Leiden cluster tree over `view` (CONCEPT:EG-KG.compute.leiden-hierarchy,
/// VIZ-1) — the engine-side half of "server-side hierarchical clustering with
/// expand-on-demand": a client renders `levels[k].clusters` (a few thousand
/// nodes even for a million-node graph) instead of every node, and calls
/// `ClusterHierarchyExpand` to drill into one level-1 cluster's real members.
///
/// `label` optionally restricts the clustered projection to one node type
/// (mirrors `MineCommunity`/`community_detection`'s own `label` filter).
/// `resolution`/`seed` are Leiden's own knobs (`graph_algos::LeidenConfig`).
///
/// A graph too small/sparse to coarsen at all (few nodes, or Leiden's local-
/// moving never improves) still gets a level 1 — synthesized as one singleton
/// cluster per node — so `ClusterHierarchyClusters`/`Expand` always have
/// something to serve rather than erroring on a small graph.
/// Project the filtered node id list + directed adjacency over dense indices.
/// Split out of `cluster_hierarchy` step 1 (extract-method, cx/wD8) — same
/// terms, same order as before.
fn project_cluster_graph(
    view: &GraphView,
    label: Option<&str>,
) -> (
    Vec<String>,
    Vec<String>,
    crate::graph_algos::AdjacencyGraph<usize>,
    usize,
) {
    let mut ids: Vec<String> = view.node_map.keys().cloned().collect();
    if let Some(want) = label {
        ids.retain(|id| {
            view.node_properties
                .get(id)
                .and_then(|b| crate::node_labels::node_type_label(b))
                .as_deref()
                == Some(want)
        });
    }
    ids.sort_unstable();
    let index: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    let node_types: Vec<String> = ids
        .iter()
        .map(|id| {
            view.node_properties
                .get(id)
                .and_then(|b| crate::node_labels::node_type_label(b))
                .unwrap_or_else(|| "_".to_string())
        })
        .collect();

    let mut adjacency: Vec<Vec<(usize, f64)>> = vec![Vec::new(); ids.len()];
    let mut base_edge_count = 0usize;
    for e in view.graph.edge_references() {
        let s = view.graph[e.source()].as_str();
        let t = view.graph[e.target()].as_str();
        if let (Some(&si), Some(&ti)) = (index.get(s), index.get(t)) {
            adjacency[si].push((ti, 1.0));
            base_edge_count += 1;
        }
    }
    let graph: crate::graph_algos::AdjacencyGraph<usize> =
        crate::graph_algos::AdjacencyGraph::from_adjacency(adjacency.into_iter().enumerate());
    (ids, node_types, graph, base_edge_count)
}

/// Build one cluster-hierarchy level's result + this level's node->cluster
/// membership. Split out of `cluster_hierarchy`'s per-level loop body
/// (extract-method, cx/wD8) — same terms, same arithmetic order as before
/// (the `internal_weight`/`inter` accumulation loop is untouched).
type ClusterLevelInput<'a> = (
    usize,
    &'a [Vec<usize>],
    &'a [Option<usize>],
    &'a crate::graph_algos::AdjacencyGraph<usize>,
    &'a [String],
    usize,
);

fn build_cluster_level(input: ClusterLevelInput<'_>) -> (ClusterLevelResult, Vec<u32>) {
    let (level, communities, parent, graph, node_types, base_node_count) = input;
    // Local orig_idx -> this-level-cluster-local-idx, for the O(E) edge pass.
    let mut membership: Vec<u32> = vec![0u32; base_node_count];
    for (c, members) in communities.iter().enumerate() {
        for &m in members {
            membership[m] = c as u32;
        }
    }

    let mut internal_weight = vec![0.0f64; communities.len()];
    let mut inter: HashMap<(u32, u32), f64> = HashMap::new();
    for i in 0..base_node_count {
        let ci = membership[i];
        for &(j, w) in graph.out_edges(i) {
            let cj = membership[j];
            if ci == cj {
                internal_weight[ci as usize] += w;
            } else {
                *inter.entry((ci, cj)).or_insert(0.0) += w;
            }
        }
    }
    let mut inter_cluster_edges: Vec<(u32, u32, f64)> =
        inter.into_iter().map(|((s, d), w)| (s, d, w)).collect();
    inter_cluster_edges.sort_unstable_by_key(|&(s, d, _)| (s, d));

    let clusters: Vec<ClusterMeta> = communities
        .iter()
        .enumerate()
        .map(|(c, members)| {
            let mut type_counts: HashMap<String, usize> = HashMap::new();
            for &m in members {
                *type_counts.entry(node_types[m].clone()).or_insert(0) += 1;
            }
            let top = top_types(&type_counts, 5);
            let id = format_cluster_id(level, c);
            let label = top
                .first()
                .map(|(t, _)| format!("{t} cluster ({c})"))
                .unwrap_or_else(|| id.clone());
            ClusterMeta {
                id,
                label,
                node_count: members.len(),
                edge_count: internal_weight[c],
                parent_id: parent[c].map(|p| format_cluster_id(level + 1, p)),
                top_node_types: top,
            }
        })
        .collect();

    (
        ClusterLevelResult {
            level,
            clusters,
            inter_cluster_edges,
        },
        membership,
    )
}

pub fn cluster_hierarchy(
    view: &GraphView,
    label: Option<&str>,
    resolution: f64,
    seed: u64,
) -> ClusterHierarchyResult {
    // 1) Project: filtered node id list + directed adjacency over dense indices.
    let (ids, node_types, graph, base_edge_count) = project_cluster_graph(view, label);
    let base_node_count = ids.len();

    // 2) Cluster: the tested, connectivity-guaranteeing hierarchical kernel.
    let cfg = crate::graph_algos::LeidenConfig {
        resolution: if resolution > 0.0 { resolution } else { 1.0 },
        seed: Some(seed),
        ..Default::default()
    };
    let raw = crate::graph_algos::leiden_hierarchy(&graph, &cfg);

    // Fall back to singleton level 1 when Leiden found no coarsening at all
    // (too few/sparse nodes) — see the function doc.
    let synthetic_singleton_level = raw.levels.is_empty() && base_node_count > 0;
    let level_count = if synthetic_singleton_level {
        1
    } else {
        raw.levels.len()
    };

    let mut levels: Vec<ClusterLevelResult> = Vec::with_capacity(level_count);
    let mut leaf_membership: Vec<(String, u32)> = Vec::new();

    for level_idx in 0..level_count {
        let level = level_idx + 1;
        let (communities, parent): (Vec<Vec<usize>>, Vec<Option<usize>>) =
            if synthetic_singleton_level {
                (
                    (0..base_node_count).map(|i| vec![i]).collect(),
                    vec![None; base_node_count],
                )
            } else {
                (
                    raw.levels[level_idx].communities.clone(),
                    raw.levels[level_idx].parent.clone(),
                )
            };

        let (level_result, membership) = build_cluster_level((
            level,
            &communities,
            &parent,
            &graph,
            &node_types,
            base_node_count,
        ));
        if level == 1 {
            leaf_membership = ids
                .iter()
                .enumerate()
                .map(|(i, id)| (id.clone(), membership[i]))
                .collect();
        }
        levels.push(level_result);
    }

    ClusterHierarchyResult {
        levels,
        leaf_membership,
        base_node_count,
        base_edge_count,
    }
}
