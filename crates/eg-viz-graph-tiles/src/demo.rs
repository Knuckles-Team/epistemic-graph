//! A deterministic, seeded, in-memory [`GraphSource`] — the SAME idiom
//! `eg_types::viz::VizDatasetSource::SyntheticGraph` already uses in
//! production (engine-side generated, clearly labeled, never a fabricated
//! real dataset): it exists so the binary tile protocol has a real caller and
//! real measurements TODAY, without waiting on VIZ-1's GraphCore-backed
//! hierarchical clustering to merge. Swapping in the real implementation
//! later means constructing a different `GraphSource` — [`crate::wire`] and
//! [`crate::contract`] do not change.
//!
//! ## Shape
//!
//! Two levels: `node_count` nodes are hash-bucketed into `top_clusters`
//! top-level clusters (level 0), and each top cluster's nodes are further
//! hash-bucketed into `sub_clusters_per_top` sub-clusters (level 1, reached
//! via `clusters(1, Some(top_cluster_id))`). Edges are generated with an
//! intra-cluster bias (most edges land inside one top cluster, matching how
//! a real graph clusters) so `inter_cluster_edges` is non-trivial rather than
//! uniformly random. Everything is a pure function of `(node_count,
//! edge_count, seed, top_clusters, sub_clusters_per_top)` — the same inputs
//! always produce the byte-identical graph, positions included, exactly like
//! `eg_viz_export::graph_layout::layout`'s determinism contract.

use std::collections::HashMap;

use crate::contract::{
    ChildClusterRef, ClusterExpansion, ClusterLevel, ClusterSummary, GraphSource, InterClusterEdge,
    TileEdge, TileNode,
};

/// Bound on `node_count` a [`DemoGraph`] will generate — this is a demo/proof
/// generator, not a production dataset ingest path; it must stay fast enough
/// to build synchronously inside one request handler. 2,000,000 matches this
/// program's stated scale target.
pub const MAX_DEMO_NODE_COUNT: u64 = 2_000_000;
/// Bound on `edge_count`, independent of `node_count` (a caller can still ask
/// for a dense small graph).
pub const MAX_DEMO_EDGE_COUNT: u64 = 6_000_000;
/// Bound on how many nodes a single [`DemoGraph::expand`] call returns.
/// [`expand`] is a TILE, not "the whole graph" — a cluster larger than this
/// is truncated (deterministically: the lowest node indices win), exactly
/// the same "bounded, documented, never silently unbounded" posture
/// `MAX_TILE_ROWS`/`MAX_SYNTHETIC_SCATTER_ROWS` already take elsewhere in
/// this codebase.
pub const MAX_EXPAND_NODES: usize = 50_000;

const NODE_TYPES: [&str; 6] = [
    "Person",
    "Organization",
    "Document",
    "Event",
    "Location",
    "Concept",
];
const EDGE_TYPES: [&str; 4] = ["relatesTo", "knows", "partOf", "mentions"];

/// splitmix64 — see `eg_viz_export::graph_layout::SplitMix64`'s doc for the
/// provenance of this exact construction; reimplemented here (not imported)
/// because this crate deliberately does not depend on `eg-viz-export` (a
/// leaf-crate DAG position, see this crate's `Cargo.toml`).
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn mix(seed: u64, salt: u64, i: u64) -> u64 {
    SplitMix64::new(seed ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i)).next_u64()
}

/// Parameters for a [`DemoGraph`]. `top_clusters`/`sub_clusters_per_top` are
/// deliberately small integers (a real clustering picks cluster COUNT from
/// the data; this demo just needs enough clusters to exercise the
/// level/parent addressing scheme).
#[derive(Debug, Clone, Copy)]
pub struct DemoParams {
    pub node_count: u64,
    pub edge_count: u64,
    pub seed: u64,
    pub top_clusters: u32,
    pub sub_clusters_per_top: u32,
}

impl Default for DemoParams {
    fn default() -> Self {
        Self {
            node_count: 1_000,
            edge_count: 3_000,
            seed: 42,
            top_clusters: 8,
            sub_clusters_per_top: 4,
        }
    }
}

impl DemoParams {
    /// Clamp every field to this module's `MAX_*` bounds and at least 1
    /// cluster, so a caller-supplied (e.g. HTTP query-param-derived) set of
    /// params can never force unbounded generation.
    pub fn clamped(mut self) -> Self {
        self.node_count = self.node_count.min(MAX_DEMO_NODE_COUNT);
        self.edge_count = self.edge_count.min(MAX_DEMO_EDGE_COUNT);
        self.top_clusters = self.top_clusters.max(1);
        self.sub_clusters_per_top = self.sub_clusters_per_top.max(1);
        self
    }
}

struct Edge {
    src: u32,
    dst: u32,
    type_idx: u8,
}

/// A deterministic in-memory demo graph implementing [`GraphSource`]. See the
/// module doc.
pub struct DemoGraph {
    params: DemoParams,
    node_type_idx: Vec<u8>,
    pos: Vec<(f32, f32)>,
    top_cluster_of: Vec<u32>,
    sub_cluster_of: Vec<u32>,
    edges: Vec<Edge>,
}

/// Cluster ids `1..=top_clusters` are top-level clusters; sub-cluster ids
/// start immediately after, encoded as `sub_id_base + c * sub_clusters_per_top
/// + s`. This keeps every cluster id globally unique across both levels
/// (required: `expand(cluster_id)` takes no level/parent, so the id alone
/// must disambiguate which level it names) while staying trivially
/// invertible for [`DemoGraph::expand`].
impl DemoGraph {
    fn sub_id_base(&self) -> u64 {
        self.params.top_clusters as u64 + 1
    }
    fn top_cluster_id(&self, c: u32) -> u64 {
        c as u64 + 1
    }
    fn sub_cluster_id(&self, c: u32, s: u32) -> u64 {
        self.sub_id_base() + (c as u64) * (self.params.sub_clusters_per_top as u64) + s as u64
    }
    /// Decode a cluster id into `Top(c)` or `Sub(c, s)`, or `None` if it
    /// names neither — an out-of-range id is a normal "unknown cluster"
    /// case, not a panic.
    fn decode_cluster_id(&self, id: u64) -> Option<ClusterRef> {
        if id == 0 {
            return None;
        }
        if id <= self.params.top_clusters as u64 {
            return Some(ClusterRef::Top((id - 1) as u32));
        }
        let sub_offset = id.checked_sub(self.sub_id_base())?;
        let sub_clusters_per_top = self.params.sub_clusters_per_top as u64;
        let c = sub_offset / sub_clusters_per_top;
        let s = sub_offset % sub_clusters_per_top;
        if c < self.params.top_clusters as u64 {
            Some(ClusterRef::Sub(c as u32, s as u32))
        } else {
            None
        }
    }

    /// Build a graph deterministically from `params` (clamped to this
    /// module's bounds first).
    pub fn build(params: DemoParams) -> Self {
        let params = params.clamped();
        let n = params.node_count as usize;
        let mut node_type_idx = Vec::with_capacity(n);
        let mut pos = Vec::with_capacity(n);
        let mut top_cluster_of = Vec::with_capacity(n);
        let mut sub_cluster_of = Vec::with_capacity(n);

        for i in 0..n as u64 {
            let type_idx = (mix(params.seed, 0x1, i) % NODE_TYPES.len() as u64) as u8;
            let x = (mix(params.seed, 0x2, i) >> 11) as f64 / (1u64 << 53) as f64;
            let y = (mix(params.seed, 0x3, i) >> 11) as f64 / (1u64 << 53) as f64;
            let top = (mix(params.seed, 0x4, i) % params.top_clusters as u64) as u32;
            let sub = (mix(params.seed, 0x5, i) % params.sub_clusters_per_top as u64) as u32;
            node_type_idx.push(type_idx);
            pos.push((x as f32, y as f32));
            top_cluster_of.push(top);
            sub_cluster_of.push(sub);
        }

        let mut edges = Vec::with_capacity(params.edge_count as usize);
        if n > 0 {
            for e in 0..params.edge_count {
                let src = (mix(params.seed, 0x6, e) % n as u64) as u32;
                let want_intra = mix(params.seed, 0x7, e) % 100 < 80; // 80% intra-cluster bias
                let mut dst = (mix(params.seed, 0x8, e) % n as u64) as u32;
                if want_intra && n > 1 {
                    // Bounded retry: reroll `dst` toward the same top cluster
                    // as `src`, giving up (and keeping whatever `dst` landed
                    // on) after a few tries rather than looping unboundedly.
                    for retry in 0..4u64 {
                        if top_cluster_of[dst as usize] == top_cluster_of[src as usize] {
                            break;
                        }
                        dst = (mix(params.seed, 0x9 + retry, e) % n as u64) as u32;
                    }
                }
                if dst == src {
                    dst = ((src as u64 + 1) % n as u64) as u32;
                }
                let type_idx = (mix(params.seed, 0xA, e) % EDGE_TYPES.len() as u64) as u8;
                edges.push(Edge { src, dst, type_idx });
            }
        }

        Self {
            params,
            node_type_idx,
            pos,
            top_cluster_of,
            sub_cluster_of,
            edges,
        }
    }

    pub fn node_count(&self) -> usize {
        self.node_type_idx.len()
    }
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    fn top_node_types_for<I: Iterator<Item = usize>>(&self, node_indices: I) -> Vec<String> {
        let mut counts: HashMap<u8, u32> = HashMap::new();
        for idx in node_indices {
            *counts.entry(self.node_type_idx[idx]).or_default() += 1;
        }
        let mut by_count: Vec<(u8, u32)> = counts.into_iter().collect();
        by_count.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        by_count
            .into_iter()
            .take(crate::wire::MAX_TOP_TYPES_PER_CLUSTER)
            .map(|(t, _)| NODE_TYPES[t as usize].to_string())
            .collect()
    }

    fn centroid_of<I: Iterator<Item = usize> + Clone>(
        &self,
        node_indices: I,
    ) -> Option<(f32, f32)> {
        let mut sum = (0.0f64, 0.0f64);
        let mut count = 0u64;
        for idx in node_indices {
            sum.0 += self.pos[idx].0 as f64;
            sum.1 += self.pos[idx].1 as f64;
            count += 1;
        }
        if count == 0 {
            None
        } else {
            Some(((sum.0 / count as f64) as f32, (sum.1 / count as f64) as f32))
        }
    }

    fn expand_from_membership(
        &self,
        cluster_id: u64,
        member: impl Fn(usize) -> bool,
    ) -> ClusterExpansion {
        let mut included: Vec<usize> = (0..self.node_count()).filter(|&i| member(i)).collect();
        included.truncate(MAX_EXPAND_NODES);
        let mut local_idx: HashMap<u32, u32> = HashMap::with_capacity(included.len());
        let mut nodes = Vec::with_capacity(included.len());
        for (local, &global) in included.iter().enumerate() {
            local_idx.insert(global as u32, local as u32);
            nodes.push(TileNode {
                id: format!("n:{global}"),
                label: format!("Node {global}"),
                node_type: NODE_TYPES[self.node_type_idx[global] as usize].to_string(),
                pos: Some(self.pos[global]),
            });
        }
        let mut edges = Vec::new();
        for e in &self.edges {
            if let (Some(&s), Some(&d)) = (local_idx.get(&e.src), local_idx.get(&e.dst)) {
                edges.push(TileEdge {
                    src_idx: s,
                    dst_idx: d,
                    edge_type: EDGE_TYPES[e.type_idx as usize].to_string(),
                });
            }
        }
        ClusterExpansion {
            cluster_id,
            nodes,
            edges,
            child_clusters: Vec::new(),
        }
    }
}

enum ClusterRef {
    Top(u32),
    Sub(u32, u32),
}

impl GraphSource for DemoGraph {
    fn clusters(&self, level: u32, parent: Option<u64>) -> ClusterLevel {
        if level == 0 {
            let mut clusters = Vec::with_capacity(self.params.top_clusters as usize);
            for c in 0..self.params.top_clusters {
                let members: Vec<usize> = (0..self.node_count())
                    .filter(|&i| self.top_cluster_of[i] == c)
                    .collect();
                let node_count = members.len() as u32;
                let edge_count = self
                    .edges
                    .iter()
                    .filter(|e| {
                        self.top_cluster_of[e.src as usize] == c
                            && self.top_cluster_of[e.dst as usize] == c
                    })
                    .count() as u32;
                clusters.push(ClusterSummary {
                    id: self.top_cluster_id(c),
                    label: format!("cluster-{c}"),
                    node_count,
                    edge_count,
                    centroid: self.centroid_of(members.iter().copied()),
                    top_node_types: self.top_node_types_for(members.iter().copied()),
                });
            }
            // src_idx/dst_idx are positions in the `clusters` array above,
            // which is built in `c` order 0..top_clusters -- so `c` IS the
            // index, no separate lookup needed.
            let mut agg: HashMap<(u32, u32), (u32, f32)> = HashMap::new();
            for e in &self.edges {
                let a = self.top_cluster_of[e.src as usize];
                let b = self.top_cluster_of[e.dst as usize];
                if a != b {
                    let entry = agg.entry((a, b)).or_insert((0, 0.0));
                    entry.0 += 1;
                    entry.1 += 1.0;
                }
            }
            let mut inter_cluster_edges: Vec<InterClusterEdge> = agg
                .into_iter()
                .map(|((a, b), (_, weight))| InterClusterEdge {
                    src_idx: a,
                    dst_idx: b,
                    weight,
                })
                .collect();
            inter_cluster_edges.sort_by_key(|e| (e.src_idx, e.dst_idx));
            ClusterLevel {
                level: 0,
                parent_cluster_id: None,
                clusters,
                inter_cluster_edges,
            }
        } else {
            let Some(ClusterRef::Top(c)) = parent.and_then(|p| self.decode_cluster_id(p)) else {
                // Unknown/out-of-range/wrong-kind parent: an empty level, not
                // an error -- "degrade honestly" per this crate's own doc.
                return ClusterLevel {
                    level,
                    parent_cluster_id: parent,
                    clusters: Vec::new(),
                    inter_cluster_edges: Vec::new(),
                };
            };
            let mut clusters = Vec::with_capacity(self.params.sub_clusters_per_top as usize);
            for s in 0..self.params.sub_clusters_per_top {
                let members: Vec<usize> = (0..self.node_count())
                    .filter(|&i| self.top_cluster_of[i] == c && self.sub_cluster_of[i] == s)
                    .collect();
                let node_count = members.len() as u32;
                let edge_count = self
                    .edges
                    .iter()
                    .filter(|e| {
                        self.top_cluster_of[e.src as usize] == c
                            && self.top_cluster_of[e.dst as usize] == c
                            && self.sub_cluster_of[e.src as usize] == s
                            && self.sub_cluster_of[e.dst as usize] == s
                    })
                    .count() as u32;
                clusters.push(ClusterSummary {
                    id: self.sub_cluster_id(c, s),
                    label: format!("cluster-{c}-{s}"),
                    node_count,
                    edge_count,
                    centroid: self.centroid_of(members.iter().copied()),
                    top_node_types: self.top_node_types_for(members.iter().copied()),
                });
            }
            let mut agg: HashMap<(u32, u32), (u32, f32)> = HashMap::new();
            for e in &self.edges {
                let (src_c, src_s) = (
                    self.top_cluster_of[e.src as usize],
                    self.sub_cluster_of[e.src as usize],
                );
                let (dst_c, dst_s) = (
                    self.top_cluster_of[e.dst as usize],
                    self.sub_cluster_of[e.dst as usize],
                );
                if src_c == c && dst_c == c && src_s != dst_s {
                    let entry = agg.entry((src_s, dst_s)).or_insert((0, 0.0));
                    entry.0 += 1;
                    entry.1 += 1.0;
                }
            }
            let mut inter_cluster_edges: Vec<InterClusterEdge> = agg
                .into_iter()
                .map(|((a, b), (_, weight))| InterClusterEdge {
                    src_idx: a,
                    dst_idx: b,
                    weight,
                })
                .collect();
            inter_cluster_edges.sort_by_key(|e| (e.src_idx, e.dst_idx));
            ClusterLevel {
                level,
                parent_cluster_id: parent,
                clusters,
                inter_cluster_edges,
            }
        }
    }

    fn expand(&self, cluster_id: u64) -> ClusterExpansion {
        match self.decode_cluster_id(cluster_id) {
            Some(ClusterRef::Top(c)) => {
                let mut expansion =
                    self.expand_from_membership(cluster_id, |i| self.top_cluster_of[i] == c);
                let sub_level = self.clusters(1, Some(cluster_id));
                expansion.child_clusters = sub_level
                    .clusters
                    .into_iter()
                    .map(|cs| ChildClusterRef {
                        id: cs.id,
                        label: cs.label,
                        node_count: cs.node_count,
                    })
                    .collect();
                expansion
            }
            Some(ClusterRef::Sub(c, s)) => self.expand_from_membership(cluster_id, |i| {
                self.top_cluster_of[i] == c && self.sub_cluster_of[i] == s
            }),
            None => ClusterExpansion {
                cluster_id,
                nodes: Vec::new(),
                edges: Vec::new(),
                child_clusters: Vec::new(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_params() -> DemoParams {
        DemoParams {
            node_count: 500,
            edge_count: 1500,
            seed: 7,
            top_clusters: 5,
            sub_clusters_per_top: 3,
        }
    }

    #[test]
    fn build_is_deterministic_for_the_same_params() {
        let a = DemoGraph::build(small_params());
        let b = DemoGraph::build(small_params());
        assert_eq!(a.pos, b.pos);
        assert_eq!(a.top_cluster_of, b.top_cluster_of);
        assert_eq!(a.edges.len(), b.edges.len());
    }

    #[test]
    fn level_0_clusters_partition_every_node_exactly_once() {
        let g = DemoGraph::build(small_params());
        let level = g.clusters(0, None);
        assert_eq!(level.clusters.len(), 5);
        let total: u32 = level.clusters.iter().map(|c| c.node_count).sum();
        assert_eq!(total, 500);
    }

    #[test]
    fn inter_cluster_edge_indices_are_positions_in_the_returned_clusters_array() {
        let g = DemoGraph::build(small_params());
        let level = g.clusters(0, None);
        for e in &level.inter_cluster_edges {
            assert!((e.src_idx as usize) < level.clusters.len());
            assert!((e.dst_idx as usize) < level.clusters.len());
            assert_ne!(
                e.src_idx, e.dst_idx,
                "an inter-cluster edge must cross clusters"
            );
        }
    }

    #[test]
    fn expand_top_cluster_child_clusters_match_level_1_clusters_call() {
        let g = DemoGraph::build(small_params());
        let top = &g.clusters(0, None).clusters[0];
        let expansion = g.expand(top.id);
        let level1 = g.clusters(1, Some(top.id));
        assert_eq!(expansion.child_clusters.len(), level1.clusters.len());
        for (child, summary) in expansion.child_clusters.iter().zip(level1.clusters.iter()) {
            assert_eq!(child.id, summary.id);
            assert_eq!(child.label, summary.label);
            assert_eq!(child.node_count, summary.node_count);
        }
    }

    #[test]
    fn expand_edges_reference_only_nodes_present_in_the_same_response() {
        let g = DemoGraph::build(small_params());
        let top_id = g.clusters(0, None).clusters[0].id;
        let expansion = g.expand(top_id);
        for e in &expansion.edges {
            assert!((e.src_idx as usize) < expansion.nodes.len());
            assert!((e.dst_idx as usize) < expansion.nodes.len());
        }
    }

    #[test]
    fn expand_of_an_unknown_cluster_id_is_empty_not_a_panic() {
        let g = DemoGraph::build(small_params());
        let expansion = g.expand(999_999_999);
        assert!(expansion.nodes.is_empty());
        assert!(expansion.edges.is_empty());
    }

    #[test]
    fn clusters_of_an_unknown_parent_is_an_empty_level_not_a_panic() {
        let g = DemoGraph::build(small_params());
        let level = g.clusters(1, Some(999_999_999));
        assert!(level.clusters.is_empty());
    }

    #[test]
    fn params_are_clamped_to_the_documented_bounds() {
        let params = DemoParams {
            node_count: MAX_DEMO_NODE_COUNT + 1000,
            edge_count: MAX_DEMO_EDGE_COUNT + 1000,
            seed: 1,
            top_clusters: 4,
            sub_clusters_per_top: 2,
        }
        .clamped();
        assert_eq!(params.node_count, MAX_DEMO_NODE_COUNT);
        assert_eq!(params.edge_count, MAX_DEMO_EDGE_COUNT);
    }

    #[test]
    fn a_cluster_larger_than_the_expand_cap_is_truncated_not_unbounded() {
        let params = DemoParams {
            node_count: 200,
            edge_count: 100,
            seed: 3,
            top_clusters: 1, // force every node into one cluster
            sub_clusters_per_top: 1,
        };
        let g = DemoGraph::build(params);
        let expansion = g.expand(1);
        assert!(expansion.nodes.len() <= MAX_EXPAND_NODES);
        assert_eq!(expansion.nodes.len(), 200);
    }
}
