// CONCEPT:EG-KG.compute.graph-compute-engine - High-Performance Graph Algorithms
//
// PageRank, centrality, community detection, BFS/DFS traversals,
// connected components — all operating on GraphCore.

use eg_core::compute::semantic::MAX_EMBEDDING_DIMENSION;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{Bfs, EdgeRef, IntoEdgeReferences};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::graph::{GraphCore, GraphView};

/// Hard wall-clock budget for one [`community_detection`] call. Louvain is
/// pass- and level-capped, but a very large or adversarial graph can make each
/// pass expensive; this deadline guarantees the call always returns a valid
/// partition in bounded time instead of appearing to hang. (CONCEPT:EG-KG.compute.graph-compute-engine)
const COMMUNITY_DETECTION_BUDGET: Duration = Duration::from_secs(15);

// ── Traversal Algorithms ─────────────────────────────────────────────────

/// Topological sort of the graph. Returns PyErr if cycles exist.
pub fn topological_sort(core: &GraphView) -> Result<Vec<String>, String> {
    match petgraph::algo::toposort(&core.graph, None) {
        Ok(indices) => {
            let sorted: Vec<String> = indices.iter().map(|&idx| core.graph[idx].clone()).collect();
            Ok(sorted)
        }
        Err(_) => Err("Graph contains cycles".to_string()),
    }
}

/// Detect a cycle via DFS coloring. Returns the cycle path if found.
pub fn find_cycle(core: &GraphView) -> Option<Vec<String>> {
    let mut visited: HashMap<NodeIndex, i32> = HashMap::new();
    let mut parent: HashMap<NodeIndex, NodeIndex> = HashMap::new();

    for node in core.graph.node_indices() {
        visited.insert(node, 0);
    }

    for node in core.graph.node_indices() {
        if visited[&node] == 0 {
            let mut path = Vec::new();
            if dfs_find_cycle(core, node, &mut visited, &mut parent, &mut path) {
                return Some(path);
            }
        }
    }
    None
}

fn dfs_find_cycle(
    core: &GraphView,
    node: NodeIndex,
    visited: &mut HashMap<NodeIndex, i32>,
    parent: &mut HashMap<NodeIndex, NodeIndex>,
    path: &mut Vec<String>,
) -> bool {
    visited.insert(node, 1); // visiting

    for neighbor in core.graph.neighbors(node) {
        if visited[&neighbor] == 1 {
            // Cycle detected — reconstruct path
            let mut curr = node;
            let mut temp_path = Vec::new();
            while curr != neighbor {
                temp_path.push(core.graph[curr].clone());
                curr = parent[&curr];
            }
            temp_path.push(core.graph[neighbor].clone());
            temp_path.reverse();
            if let Some(first) = temp_path.first().cloned() {
                temp_path.push(first);
            }
            *path = temp_path;
            return true;
        } else if visited[&neighbor] == 0 {
            parent.insert(neighbor, node);
            if dfs_find_cycle(core, neighbor, visited, parent, path) {
                return true;
            }
        }
    }
    visited.insert(node, 2); // visited
    false
}

/// BFS shortest path between two nodes.
pub fn get_shortest_path(
    core: &GraphView,
    source_id: &str,
    target_id: &str,
) -> Option<Vec<String>> {
    let src_idx = *core.node_map.get(source_id)?;
    let tgt_idx = *core.node_map.get(target_id)?;

    let mut bfs = Bfs::new(&core.graph, src_idx);
    let mut path_predecessor: HashMap<NodeIndex, NodeIndex> = HashMap::new();

    while let Some(nx) = bfs.next(&core.graph) {
        for neighbor in core.graph.neighbors(nx) {
            if !path_predecessor.contains_key(&neighbor) && neighbor != src_idx {
                path_predecessor.insert(neighbor, nx);
                if neighbor == tgt_idx {
                    break;
                }
            }
        }
    }

    if path_predecessor.contains_key(&tgt_idx) {
        let mut path = Vec::new();
        let mut curr = tgt_idx;
        while curr != src_idx {
            path.push(core.graph[curr].clone());
            curr = path_predecessor[&curr];
        }
        path.push(source_id.to_string());
        path.reverse();
        Some(path)
    } else {
        None
    }
}

/// BFS blast radius — all nodes reachable within `max_depth` hops.
pub fn get_blast_radius(core: &GraphView, node_id: &str, max_depth: usize) -> Vec<String> {
    let start_idx = match core.node_map.get(node_id) {
        Some(&idx) => idx,
        None => return Vec::new(),
    };

    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();

    queue.push_back((start_idx, 0));
    visited.insert(start_idx);

    let mut blast_nodes = Vec::new();

    while let Some((curr, depth)) = queue.pop_front() {
        if curr != start_idx {
            blast_nodes.push(core.graph[curr].clone());
        }
        if depth < max_depth {
            for neighbor in core.graph.neighbors(curr) {
                if visited.insert(neighbor) {
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }
    }
    blast_nodes
}

// ── Centrality Algorithms ────────────────────────────────────────────────

/// Degree centrality for a single node: (in + out) / (n - 1).
pub fn compute_degree_centrality(core: &GraphView, node_id: &str) -> Result<f64, String> {
    let idx = core
        .node_map
        .get(node_id)
        .ok_or_else(|| format!("Node '{}' not found", node_id))?;
    let n = core.node_map.len();
    if n <= 1 {
        return Ok(0.0);
    }
    let in_deg = core
        .graph
        .edges_directed(*idx, petgraph::Direction::Incoming)
        .count();
    let out_deg = core
        .graph
        .edges_directed(*idx, petgraph::Direction::Outgoing)
        .count();
    Ok((in_deg + out_deg) as f64 / (n - 1) as f64)
}

/// Degree centrality for ALL nodes. Returns Vec<(node_id, centrality)>.
pub fn degree_centrality_all(core: &GraphView) -> Vec<(String, f64)> {
    let n = core.node_map.len();
    if n <= 1 {
        return core.node_map.keys().map(|k| (k.clone(), 0.0)).collect();
    }
    let denom = (n - 1) as f64;

    core.node_map
        .iter()
        .map(|(node_id, &idx)| {
            let in_deg = core
                .graph
                .edges_directed(idx, petgraph::Direction::Incoming)
                .count();
            let out_deg = core
                .graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .count();
            (node_id.clone(), (in_deg + out_deg) as f64 / denom)
        })
        .collect()
}

/// Betweenness centrality via Brandes' algorithm.
///
/// Brandes accumulates an independent single-source contribution per node, so the
/// expensive O(V·E) outer loop is parallelized across source nodes with rayon
/// (Phase C-D). Each source's contribution is computed in parallel; the partials
/// are then summed back in SOURCE ORDER, so the floating-point result is bit-for-bit
/// identical to the sequential version (determinism preserved).
/// Single-source BFS shortest-path counting (the forward pass of Brandes'
/// algorithm). Split out of `betweenness_centrality`'s `source_contribution`
/// closure (extract-method, cx/wD8) — same terms, same arithmetic order as
/// before. Returns the visit order stack, the predecessor DAG, and the
/// shortest-path counts `sigma`.
fn bfs_shortest_path_counts(
    core: &GraphView,
    nodes: &[NodeIndex],
    source: NodeIndex,
) -> (
    Vec<NodeIndex>,
    HashMap<NodeIndex, Vec<NodeIndex>>,
    HashMap<NodeIndex, f64>,
) {
    let mut stack = Vec::new();
    let mut predecessors: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();
    let mut sigma: HashMap<NodeIndex, f64> = HashMap::new();
    let mut dist: HashMap<NodeIndex, i64> = HashMap::new();

    for &v in nodes {
        predecessors.insert(v, Vec::new());
        sigma.insert(v, 0.0);
        dist.insert(v, -1);
    }
    sigma.insert(source, 1.0);
    dist.insert(source, 0);

    let mut queue = VecDeque::new();
    queue.push_back(source);

    while let Some(v) = queue.pop_front() {
        stack.push(v);
        let v_dist = dist[&v];
        bfs_relax_neighbors(
            core,
            v,
            v_dist,
            &mut queue,
            &mut dist,
            &mut sigma,
            &mut predecessors,
        );
    }
    (stack, predecessors, sigma)
}

/// Relax `v`'s out-neighbors for one BFS-frontier step. Split out of
/// `bfs_shortest_path_counts` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before.
#[allow(clippy::too_many_arguments)]
fn bfs_relax_neighbors(
    core: &GraphView,
    v: NodeIndex,
    v_dist: i64,
    queue: &mut VecDeque<NodeIndex>,
    dist: &mut HashMap<NodeIndex, i64>,
    sigma: &mut HashMap<NodeIndex, f64>,
    predecessors: &mut HashMap<NodeIndex, Vec<NodeIndex>>,
) {
    for neighbor in core.graph.neighbors(v) {
        if dist[&neighbor] < 0 {
            queue.push_back(neighbor);
            dist.insert(neighbor, v_dist + 1);
        }
        if dist[&neighbor] == v_dist + 1 {
            let sigma_v = sigma[&v];
            if let Some(s) = sigma.get_mut(&neighbor) {
                *s += sigma_v;
            }
            if let Some(p) = predecessors.get_mut(&neighbor) {
                p.push(v);
            }
        }
    }
}

/// Single-source dependency accumulation (the backward pass of Brandes'
/// algorithm). Split out of `betweenness_centrality`'s `source_contribution`
/// closure (extract-method, cx/wD8) — same terms, same arithmetic order as
/// before, including the exact `(sigma[v] / sigma[w]) * (1.0 + delta[w])`
/// evaluation order.
fn accumulate_betweenness_dependencies(
    nodes: &[NodeIndex],
    mut stack: Vec<NodeIndex>,
    predecessors: &HashMap<NodeIndex, Vec<NodeIndex>>,
    sigma: &HashMap<NodeIndex, f64>,
    source: NodeIndex,
) -> Vec<(NodeIndex, f64)> {
    let mut delta: HashMap<NodeIndex, f64> = HashMap::new();
    for &v in nodes {
        delta.insert(v, 0.0);
    }

    let mut contrib = Vec::new();
    while let Some(w) = stack.pop() {
        if sigma[&w] > 0.0 {
            for &v in &predecessors[&w] {
                let d = (sigma[&v] / sigma[&w]) * (1.0 + delta[&w]);
                if let Some(dv) = delta.get_mut(&v) {
                    *dv += d;
                }
            }
        }
        if w != source && delta[&w] != 0.0 {
            contrib.push((w, delta[&w]));
        }
    }
    contrib
}

pub fn betweenness_centrality(core: &GraphView) -> Vec<(String, f64)> {
    use rayon::prelude::*;

    let nodes: Vec<NodeIndex> = core.graph.node_indices().collect();
    let n = nodes.len();

    // One independent single-source dependency accumulation. Returns (w, delta[w])
    // for every w != source — the partial betweenness this source contributes.
    let source_contribution =
        |source: NodeIndex| -> Vec<(NodeIndex, f64)> {
            let (stack, predecessors, sigma) = bfs_shortest_path_counts(core, &nodes, source);
            accumulate_betweenness_dependencies(&nodes, stack, &predecessors, &sigma, source)
        };

    // Compute every source's contribution in parallel; collect preserves source
    // order so the sequential reduction below is order-stable.
    let partials: Vec<Vec<(NodeIndex, f64)>> = nodes
        .par_iter()
        .map(|&source| source_contribution(source))
        .collect();

    let mut centrality: HashMap<NodeIndex, f64> = nodes.iter().map(|&v| (v, 0.0)).collect();
    for partial in &partials {
        for &(w, dw) in partial {
            if let Some(c) = centrality.get_mut(&w) {
                *c += dw;
            }
        }
    }

    // Normalize
    let norm = if n > 2 {
        1.0 / ((n - 1) as f64 * (n - 2) as f64)
    } else {
        1.0
    };

    centrality
        .into_iter()
        .map(|(idx, val)| (core.graph[idx].clone(), val * norm))
        .collect()
}

/// PageRank via power iteration (CONCEPT:EG-KG.compute.pagerank-sparse-csr).
///
/// Delegates to the sparse, CSR-adjacency-list, memory-bounded implementation in
/// [`crate::graph_algos::pagerank`] (the same engine `CALL gds.pageRank` in
/// Cypher already uses) instead of maintaining a second, independently-written
/// implementation directly over the live petgraph structure.
///
/// **Why this changed.** The prior version allocated a fresh
/// `HashMap<NodeIndex, f64>` of size `n` on EVERY iteration (`new_scores`) and
/// resolved each node's in/out edges via per-node `edges_directed` lookups. On a
/// large graph (~139k nodes) that per-iteration HashMap churn — hashing +
/// rehashing + heap allocation, repeated `iterations` times, never reused — OOM-
/// killed the engine on an unbounded whole-graph PageRank call. The sparse path
/// here builds ONE flat CSR-style adjacency (`Vec<Vec<(usize, f64)>>`, via
/// [`crate::graph_algos::graph::AdjacencyGraph`]) once, up front, and reuses TWO
/// `Vec<f64>` score buffers across every iteration (swapped, never reallocated) —
/// `O(V+E)` working memory, bounded regardless of `iterations`, with no per-
/// iteration allocation at all. It also converges early once the L1 tolerance is
/// reached, rather than always spending the full iteration budget.
///
/// **Correctness parity, one intentional improvement.** The computation itself —
/// distributing each node's rank across its out-edges, weighted by damping, plus
/// a uniform teleport term — is the SAME power iteration the prior
/// implementation ran (pull-from-incoming vs. push-to-outgoing are the same
/// arithmetic, just iterated from opposite ends: see
/// `pagerank_matches_prior_dense_implementation_on_a_small_graph` for the
/// differential proof on a small graph with no dangling nodes). Every node
/// (including one with zero edges) is still scored: `node_indices()` seeds the
/// adjacency with an explicit empty out-list rather than only registering nodes
/// that appear in an edge. Unlike the prior version, a dangling node (no
/// out-edges) now redistributes its rank uniformly instead of leaking it, so
/// total rank mass is properly conserved at 1.0 — the prior implementation did
/// not conserve mass on a graph with dangling nodes, which is a correctness
/// improvement, not a behavior this delegation is obligated to reproduce.
pub fn pagerank(core: &GraphView, damping: f64, iterations: usize) -> Vec<(String, f64)> {
    let adjacency: Vec<(String, Vec<(String, f64)>)> = core
        .graph
        .node_indices()
        .map(|idx| {
            let id = core.graph[idx].clone();
            let out_neighbors: Vec<(String, f64)> = core
                .graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| (core.graph[e.target()].clone(), 1.0))
                .collect();
            (id, out_neighbors)
        })
        .collect();
    let adj = crate::graph_algos::graph::AdjacencyGraph::from_adjacency(adjacency);
    let config = crate::graph_algos::pagerank::PageRankConfig {
        damping,
        tolerance: 1e-10,
        max_iterations: iterations.max(1),
    };
    crate::graph_algos::pagerank::pagerank(&adj, &config).scores
}

// ── Component / Community Algorithms ─────────────────────────────────────

/// Weakly connected components (treats directed edges as undirected).
/// BFS-collect the weakly-connected component containing `start`, marking
/// every visited node in `visited`. Split out of `connected_components`
/// (extract-method, cx/wD8) — same terms, same order as before.
fn collect_weakly_connected_component(
    core: &GraphView,
    start: NodeIndex,
    visited: &mut HashSet<NodeIndex>,
) -> Vec<String> {
    let mut component = Vec::new();
    let mut queue = VecDeque::new();
    queue.push_back(start);
    visited.insert(start);

    while let Some(curr) = queue.pop_front() {
        component.push(core.graph[curr].clone());
        // Traverse both directions (weakly connected)
        for edge in core
            .graph
            .edges_directed(curr, petgraph::Direction::Outgoing)
        {
            let neighbor = edge.target();
            if visited.insert(neighbor) {
                queue.push_back(neighbor);
            }
        }
        for edge in core
            .graph
            .edges_directed(curr, petgraph::Direction::Incoming)
        {
            let neighbor = edge.source();
            if visited.insert(neighbor) {
                queue.push_back(neighbor);
            }
        }
    }
    component
}

pub fn connected_components(core: &GraphView) -> Vec<Vec<String>> {
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut components: Vec<Vec<String>> = Vec::new();

    for &start in core.node_map.values() {
        if visited.contains(&start) {
            continue;
        }
        components.push(collect_weakly_connected_component(core, start, &mut visited));
    }

    components
}

/// Strongly connected components via Tarjan's algorithm.
///
/// CONCEPT:EG-KG.compute.graph-compute-engine — Unlike weakly connected components which treat edges as
/// undirected, SCC respects edge direction. Two nodes are in the same SCC iff
/// there is a directed path from each to the other. This is critical for
/// belief cluster detection where causal direction matters.
pub fn strongly_connected_components(core: &GraphView) -> Vec<Vec<String>> {
    let sccs = petgraph::algo::tarjan_scc(&core.graph);
    sccs.into_iter()
        .map(|component| {
            component
                .into_iter()
                .map(|idx| core.graph[idx].clone())
                .collect()
        })
        .collect()
}

/// Minimum spanning tree via Kruskal's algorithm.
///
/// CONCEPT:EG-KG.compute.graph-compute-engine — Returns the MST edges as `(source, target, weight)` tuples.
/// Edge weights are extracted from the `weight` field of edge properties JSON.
/// Edges without a weight field default to 1.0. Useful for argument coherence
/// analysis — the MST reveals the minimum-cost skeleton connecting all beliefs.
pub fn minimum_spanning_tree(core: &GraphView) -> Vec<(String, String, f64)> {
    use petgraph::data::FromElements;
    use petgraph::stable_graph::StableGraph;

    // Build a weighted undirected graph for MST computation
    let mut undirected: petgraph::Graph<String, f64, petgraph::Undirected> =
        petgraph::Graph::new_undirected();

    // Map node indices from core to undirected graph
    let mut idx_map: HashMap<NodeIndex, petgraph::graph::NodeIndex> = HashMap::new();
    for &idx in core.node_map.values() {
        let new_idx = undirected.add_node(core.graph[idx].clone());
        idx_map.insert(idx, new_idx);
    }

    // Add edges with weights
    for edge_ref in core.graph.edge_references() {
        let src = edge_ref.source();
        let tgt = edge_ref.target();
        if let (Some(&u_src), Some(&u_tgt)) = (idx_map.get(&src), idx_map.get(&tgt)) {
            let src_id = &core.graph[src];
            let tgt_id = &core.graph[tgt];
            // Extract weight from edge properties
            let weight = core
                .edge_properties
                .get(&(src_id.clone(), tgt_id.clone()))
                .and_then(|props| props.first())
                .and_then(|json_str| serde_json::from_slice::<serde_json::Value>(json_str).ok())
                .and_then(|v| v.get("weight").and_then(|w| w.as_f64()))
                .unwrap_or(1.0);
            undirected.add_edge(u_src, u_tgt, weight);
        }
    }

    // Compute MST using petgraph's built-in min_spanning_tree
    let mst_graph = StableGraph::<String, f64, petgraph::Undirected>::from_elements(
        petgraph::algo::min_spanning_tree(&undirected),
    );

    // Extract edges from MST
    mst_graph
        .edge_references()
        .map(|e| {
            let src_id = mst_graph[e.source()].clone();
            let tgt_id = mst_graph[e.target()].clone();
            let weight = *e.weight();
            (src_id, tgt_id, weight)
        })
        .collect()
}

/// Community detection via **coloring-parallel, multi-level modularity
/// optimization** (Louvain) — Phase C-D.
///
/// This replaces the previous label-propagation heuristic with a parallel,
/// deterministic, modularity-optimizing algorithm — strictly better on all three
/// axes:
///
/// * **Parallel** without the oscillation hazard. The classic obstacle to parallel
///   community detection is that two *adjacent* nodes moving community in the same
///   round can race/oscillate (the reason naive synchronous label propagation is
///   unstable). We dissolve it with **graph coloring**: color the graph so adjacent
///   nodes differ, then move one color class at a time. Within a class every node
///   is mutually non-adjacent, so their moves are independent — safe to evaluate in
///   parallel (rayon) with no interaction. This is the established parallel-Louvain
///   approach (Grappolo / NetworKit PLM).
/// * **Deterministic.** Fixed node order, fixed color order, and a smallest-
///   community-id tie-break make every run bit-identical (the determinism the
///   regression tests assert).
/// * **Higher quality.** Greedy modularity gain + multi-level aggregation finds the
///   community structure Louvain is known for, not LPA's coarse approximation.
///
/// `resolution` (γ) scales the modularity null model (higher ⇒ more, smaller
/// communities). Bounded by a wall-clock deadline so it always returns in bounded
/// time (CONCEPT:EG-KG.compute.graph-compute-engine).
pub fn community_detection(core: &GraphView, resolution: f64) -> Vec<Vec<String>> {
    let resolution = if resolution > 0.0 { resolution } else { 1.0 };

    // Stable node order → deterministic compact indexing.
    let mut node_ids: Vec<String> = core.node_map.keys().cloned().collect();
    if node_ids.is_empty() {
        return Vec::new();
    }
    node_ids.sort_unstable();
    let n = node_ids.len();
    let index: HashMap<&str, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();

    // Build an undirected weighted adjacency: parallel edges and the two directions
    // of each edge sum into a single symmetric weight; self-loops kept once.
    let mut maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n];
    for e in core.graph.edge_references() {
        let s = core.graph[e.source()].as_str();
        let t = core.graph[e.target()].as_str();
        if let (Some(&si), Some(&ti)) = (index.get(s), index.get(t)) {
            if si == ti {
                *maps[si].entry(si).or_insert(0.0) += 1.0;
            } else {
                *maps[si].entry(ti).or_insert(0.0) += 1.0;
                *maps[ti].entry(si).or_insert(0.0) += 1.0;
            }
        }
    }
    let adjacency: Vec<Vec<(usize, f64)>> = maps
        .into_iter()
        .map(|m| {
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_unstable_by_key(|x| x.0);
            v
        })
        .collect();

    let deadline = Instant::now() + COMMUNITY_DETECTION_BUDGET;
    let node_to_comm = louvain(&adjacency, resolution, deadline);

    // Group by community, deterministic order (members sorted; communities by their
    // smallest member) so callers + tests are stable.
    let mut groups: std::collections::BTreeMap<usize, Vec<String>> =
        std::collections::BTreeMap::new();
    for (i, id) in node_ids.into_iter().enumerate() {
        groups.entry(node_to_comm[i]).or_default().push(id);
    }
    let mut out: Vec<Vec<String>> = groups
        .into_values()
        .map(|mut members| {
            members.sort_unstable();
            members
        })
        .collect();
    out.sort_unstable_by(|a, b| a[0].cmp(&b[0]));
    out
}

// ── VIZ-1: hierarchical Leiden clustering for graph visualization ──────────
//
// UNLIKE `community_detection` above (a hand-rolled Louvain kept for its own
// call sites), this delegates to `crate::graph_algos::leiden_hierarchy` — the
// tested, GDS-parity, multi-level kernel — rather than duplicating it. It also
// reads `node_properties` (which `community_detection` never needs, since it's
// topology-only) for label filtering and the `top_node_types` summary, so it
// takes the SAME `GraphView` shape `ComputeSimilarityEdges`/MST already read
// (`analysis_snapshot`, not `topology_snapshot`).

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

/// Extract a node's type/label from its property blob — same key precedence
/// as `server::handlers::mining::node_type_label` (kept as an independent
/// small copy: that one reads a `GraphCore` node blob directly under the
/// server crate, this one reads a `GraphView::node_properties` blob from
/// eg-compute; duplicating a 6-line lookup is cheaper than a new cross-crate
/// dependency for it).
fn node_type_label(blob: &[u8]) -> Option<String> {
    let val = eg_types::msgpack::decode_property_value(blob).ok()?;
    for key in ["type", "node_type", "label"] {
        if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
    }
    None
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
                .and_then(|b| node_type_label(b))
                .as_deref()
                == Some(want)
        });
    }
    ids.sort_unstable();
    let index: HashMap<&str, usize> = ids.iter().enumerate().map(|(i, s)| (s.as_str(), i)).collect();
    let node_types: Vec<String> = ids
        .iter()
        .map(|id| {
            view.node_properties
                .get(id)
                .and_then(|b| node_type_label(b))
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
#[allow(clippy::too_many_arguments)]
fn build_cluster_level(
    level: usize,
    communities: &[Vec<usize>],
    parent: &[Option<usize>],
    graph: &crate::graph_algos::AdjacencyGraph<usize>,
    node_types: &[String],
    base_node_count: usize,
) -> (ClusterLevelResult, Vec<u32>) {
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
                ((0..base_node_count).map(|i| vec![i]).collect(), vec![None; base_node_count])
            } else {
                (
                    raw.levels[level_idx].communities.clone(),
                    raw.levels[level_idx].parent.clone(),
                )
            };

        let (level_result, membership) = build_cluster_level(
            level,
            &communities,
            &parent,
            &graph,
            &node_types,
            base_node_count,
        );
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

/// Weighted degree of each node (a self-loop counts twice, per the modularity
/// convention), and `2m = Σ degrees`.
fn weighted_degrees(adjacency: &[Vec<(usize, f64)>]) -> Vec<f64> {
    adjacency
        .iter()
        .enumerate()
        .map(|(i, nbrs)| {
            nbrs.iter()
                .map(|&(j, w)| if j == i { 2.0 * w } else { w })
                .sum()
        })
        .collect()
}

/// Multi-level Louvain: local-move → aggregate → repeat until no community merges
/// or the deadline hits. Returns, for each ORIGINAL node, its final community id.
fn louvain(adjacency0: &[Vec<(usize, f64)>], resolution: f64, deadline: Instant) -> Vec<usize> {
    let n0 = adjacency0.len();
    let mut node_to_comm: Vec<usize> = (0..n0).collect();
    let mut adjacency: Vec<Vec<(usize, f64)>> = adjacency0.to_vec();

    loop {
        let n = adjacency.len();
        let degrees = weighted_degrees(&adjacency);
        let two_m: f64 = degrees.iter().sum();
        if two_m <= 0.0 {
            break;
        }

        let raw = local_moving(&adjacency, &degrees, two_m, resolution, deadline);
        let (local, k) = renumber(&raw);

        // Lift the original-node mapping through this level's relabeling.
        for c in node_to_comm.iter_mut() {
            *c = local[*c];
        }

        // No community merged this level (k == n) → modularity converged.
        if k == n {
            break;
        }
        adjacency = aggregate(&adjacency, &local, k);
        if Instant::now() >= deadline {
            break;
        }
    }
    node_to_comm
}

/// One Louvain local-moving phase, parallelized by graph coloring. Returns the
/// (sparse) community label per node.
fn local_moving(
    adjacency: &[Vec<(usize, f64)>],
    degrees: &[f64],
    two_m: f64,
    resolution: f64,
    deadline: Instant,
) -> Vec<usize> {
    use rayon::prelude::*;

    let n = adjacency.len();
    let mut comm: Vec<usize> = (0..n).collect();
    let mut comm_tot: Vec<f64> = degrees.to_vec();
    let color_classes = color_classes(adjacency);

    // Each color class is an independent set, so the best move of every node in it
    // is computed from the SAME frozen (comm, comm_tot) with no cross-interaction —
    // evaluate the class in parallel, then apply the decided moves in node order.
    let max_passes = 50;
    for _ in 0..max_passes {
        if Instant::now() >= deadline {
            break;
        }
        let mut changed = false;
        for class in &color_classes {
            let moves: Vec<(usize, usize)> = class
                .par_iter()
                .map(|&i| {
                    (
                        i,
                        best_community(i, adjacency, degrees, &comm, &comm_tot, two_m, resolution),
                    )
                })
                .collect();
            for (i, target) in moves {
                if target != comm[i] {
                    comm_tot[comm[i]] -= degrees[i];
                    comm_tot[target] += degrees[i];
                    comm[i] = target;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    comm
}

/// Best community for node `i` by modularity gain: remove `i` from its community,
/// then pick the neighboring community maximizing `w(i,C) - γ·Σtot(C)·k_i/2m`.
/// Only a STRICT improvement over staying moves it (convergence); ties resolve to
/// the smallest community id (determinism).
fn best_community(
    i: usize,
    adjacency: &[Vec<(usize, f64)>],
    degrees: &[f64],
    comm: &[usize],
    comm_tot: &[f64],
    two_m: f64,
    resolution: f64,
) -> usize {
    let ci = comm[i];
    let ki = degrees[i];

    // Weight from i to each neighboring community (self-loop excluded).
    let mut w_to: HashMap<usize, f64> = HashMap::new();
    for &(j, w) in &adjacency[i] {
        if j != i {
            *w_to.entry(comm[j]).or_insert(0.0) += w;
        }
    }

    // Baseline: re-adding i to its own community (Σtot already had i removed).
    let stay_gain =
        w_to.get(&ci).copied().unwrap_or(0.0) - resolution * (comm_tot[ci] - ki) * ki / two_m;

    let mut best_c = ci;
    let mut best_gain = stay_gain;
    let mut cands: Vec<usize> = w_to.keys().copied().collect();
    cands.sort_unstable(); // smallest-id tie-break
    for c in cands {
        if c == ci {
            continue;
        }
        let gain = w_to[&c] - resolution * comm_tot[c] * ki / two_m;
        if gain > best_gain + 1e-12 {
            best_gain = gain;
            best_c = c;
        }
    }
    best_c
}

/// Greedy proper coloring (smallest available color, fixed node order) → the
/// color classes (independent sets), in ascending color order.
fn color_classes(adjacency: &[Vec<(usize, f64)>]) -> Vec<Vec<usize>> {
    let n = adjacency.len();
    let mut color = vec![usize::MAX; n];
    let mut max_color = 0;
    for i in 0..n {
        let mut used: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for &(j, _) in &adjacency[i] {
            if j != i && color[j] != usize::MAX {
                used.insert(color[j]);
            }
        }
        let mut c = 0;
        while used.contains(&c) {
            c += 1;
        }
        color[i] = c;
        max_color = max_color.max(c);
    }
    let mut classes: Vec<Vec<usize>> = vec![Vec::new(); max_color + 1];
    for (i, &c) in color.iter().enumerate() {
        classes[c].push(i);
    }
    classes
}

/// Compact a sparse labeling to dense `0..k` (first-seen order). Returns the dense
/// labels and `k`.
fn renumber(comm: &[usize]) -> (Vec<usize>, usize) {
    let mut map: HashMap<usize, usize> = HashMap::new();
    let mut dense = Vec::with_capacity(comm.len());
    for &c in comm {
        let next = map.len();
        dense.push(*map.entry(c).or_insert(next));
    }
    let k = map.len();
    (dense, k)
}

/// Contract each community (dense label `0..k`) into a super-node; inter-community
/// edge weights sum, intra-community edges become the super-node's self-loop.
fn aggregate(adjacency: &[Vec<(usize, f64)>], comm: &[usize], k: usize) -> Vec<Vec<(usize, f64)>> {
    let mut maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); k];
    for (i, nbrs) in adjacency.iter().enumerate() {
        let ci = comm[i];
        for &(j, w) in nbrs {
            *maps[ci].entry(comm[j]).or_insert(0.0) += w;
        }
    }
    maps.into_iter()
        .map(|m| {
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_unstable_by_key(|x| x.0);
            v
        })
        .collect()
}

// ── Quant epistemic-graph Algorithms ─────────────────────────────────────────────────

/// Compute rolling mean over a sliding window.
pub fn compute_rolling_mean(values: &[f64], window: usize) -> Vec<f64> {
    if window == 0 || values.is_empty() {
        return vec![0.0; values.len()];
    }
    let mut result = vec![0.0; values.len()];
    for i in 0..values.len() {
        let start = if i >= window - 1 { i + 1 - window } else { 0 };
        let slice = &values[start..=i];
        result[i] = slice.iter().sum::<f64>() / slice.len() as f64;
    }
    result
}

/// Compute rolling standard deviation over a sliding window.
pub fn compute_rolling_std(values: &[f64], window: usize) -> Vec<f64> {
    if window == 0 || values.is_empty() {
        return vec![0.0; values.len()];
    }
    let mut result = vec![0.0; values.len()];
    for (i, res_val) in result.iter_mut().enumerate() {
        let (_, variance) = window_stats(values, i, window);
        *res_val = variance.sqrt();
    }
    result
}

/// Compute rolling z-score over a sliding window.
pub fn compute_rolling_zscore(values: &[f64], window: usize) -> Vec<f64> {
    if window == 0 || values.is_empty() {
        return vec![0.0; values.len()];
    }
    let mut result = vec![0.0; values.len()];
    for i in 0..values.len() {
        let (mean, variance) = window_stats(values, i, window);
        let std = variance.sqrt();
        result[i] = if std > 0.0 {
            (values[i] - mean) / std
        } else {
            0.0
        };
    }
    result
}

/// Exponential decay (EMA) over a series.
pub fn compute_exponential_decay(values: &[f64], alpha: f64) -> Vec<f64> {
    if values.is_empty() {
        return vec![];
    }
    let mut result = vec![0.0; values.len()];
    result[0] = values[0];
    for i in 1..values.len() {
        result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1];
    }
    result
}

/// Order book matching simulation.
/// Match a buy order against the resting ask book. Split out of
/// `simulate_order_matching` (extract-method, cx/wD8) — same terms, same
/// fill-volume arithmetic order (`remaining_vol.min(ask.1)` then subtract
/// from both sides) as before.
fn match_order_against_asks(
    ask_book: &mut [(f64, f64)],
    order_id: &str,
    price: f64,
    mut remaining_vol: f64,
) -> Vec<HashMap<String, String>> {
    let mut matches = Vec::new();
    for ask in ask_book {
        let ask_price = ask.0;
        if ask_price <= price && remaining_vol > 0.0 && ask.1 > 0.0 {
            let fill_vol = remaining_vol.min(ask.1);
            remaining_vol -= fill_vol;
            ask.1 -= fill_vol;

            let mut m = HashMap::new();
            m.insert("order_id".to_string(), order_id.to_string());
            m.insert("match_price".to_string(), ask_price.to_string());
            m.insert("match_volume".to_string(), fill_vol.to_string());
            matches.push(m);
        }
    }
    matches
}

/// Match a sell order against the resting bid book. Split out of
/// `simulate_order_matching` (extract-method, cx/wD8) — same terms, same
/// fill-volume arithmetic order as before.
fn match_order_against_bids(
    bid_book: &mut [(f64, f64)],
    order_id: &str,
    price: f64,
    mut remaining_vol: f64,
) -> Vec<HashMap<String, String>> {
    let mut matches = Vec::new();
    for bid in bid_book {
        let bid_price = bid.0;
        if bid_price >= price && remaining_vol > 0.0 && bid.1 > 0.0 {
            let fill_vol = remaining_vol.min(bid.1);
            remaining_vol -= fill_vol;
            bid.1 -= fill_vol;

            let mut m = HashMap::new();
            m.insert("order_id".to_string(), order_id.to_string());
            m.insert("match_price".to_string(), bid_price.to_string());
            m.insert("match_volume".to_string(), fill_vol.to_string());
            matches.push(m);
        }
    }
    matches
}

pub fn simulate_order_matching(
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    orders: Vec<(String, String, f64, f64)>,
) -> Vec<HashMap<String, String>> {
    let mut matches = Vec::new();
    let mut bid_book = bids;
    let mut ask_book = asks;

    bid_book.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    ask_book.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    for (order_id, side, price, volume) in orders {
        if side.to_lowercase() == "buy" {
            matches.extend(match_order_against_asks(
                &mut ask_book,
                &order_id,
                price,
                volume,
            ));
        } else {
            matches.extend(match_order_against_bids(
                &mut bid_book,
                &order_id,
                price,
                volume,
            ));
        }
    }

    matches
}

// ── Internal Helpers ─────────────────────────────────────────────────────

/// Window statistics helper: returns (mean, variance) for the window ending at index `i`.
fn window_stats(values: &[f64], i: usize, window: usize) -> (f64, f64) {
    let start = if i >= window - 1 { i + 1 - window } else { 0 };
    let slice = &values[start..=i];
    let n = slice.len() as f64;
    let mean = slice.iter().sum::<f64>() / n;
    let variance = if n > 1.0 {
        slice.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)
    } else {
        0.0
    };
    (mean, variance)
}

// ── New Algorithms (Phase 1 Expansion) ───────────────────────────────────

/// Greedy graph coloring — assigns colors so no two adjacent nodes share a color.
///
/// Uses a sequential greedy algorithm. The number of colors used is at most
/// Δ(G) + 1 where Δ is the maximum degree.
pub fn graph_coloring(core: &GraphView) -> Vec<(String, usize)> {
    let nodes: Vec<String> = core.node_map.keys().cloned().collect();
    let mut colors: HashMap<String, usize> = HashMap::new();

    for node_id in &nodes {
        let idx = match core.node_map.get(node_id) {
            Some(&i) => i,
            None => continue,
        };

        // Collect colors of all neighbors
        let mut neighbor_colors: HashSet<usize> = HashSet::new();
        for edge in core
            .graph
            .edges_directed(idx, petgraph::Direction::Outgoing)
        {
            let neighbor_id = &core.graph[edge.target()];
            if let Some(&c) = colors.get(neighbor_id) {
                neighbor_colors.insert(c);
            }
        }
        for edge in core
            .graph
            .edges_directed(idx, petgraph::Direction::Incoming)
        {
            let neighbor_id = &core.graph[edge.source()];
            if let Some(&c) = colors.get(neighbor_id) {
                neighbor_colors.insert(c);
            }
        }

        // Find smallest color not used by neighbors
        let mut color = 0;
        while neighbor_colors.contains(&color) {
            color += 1;
        }
        colors.insert(node_id.clone(), color);
    }

    colors.into_iter().collect()
}

/// Compute similarity edges between nodes using cosine similarity on embeddings.
///
/// Only considers nodes that have embeddings stored in their properties JSON
/// as an "embedding" field. Uses rayon for parallel pairwise comparison.
pub fn compute_similarity_edges(core: &GraphView, threshold: f64) -> Vec<(String, String, f64)> {
    use rayon::prelude::*;

    // Extract nodes with embeddings
    let nodes_with_emb: Vec<(String, Vec<f64>)> = core
        .node_properties
        .iter()
        .filter_map(|(node_id, props_json)| {
            let val: serde_json::Value = serde_json::from_slice(props_json).ok()?;
            let emb = val.get("embedding")?;
            let vec: Vec<f64> = serde_json::from_value(emb.clone()).ok()?;
            if vec.is_empty() {
                return None;
            }
            Some((node_id.clone(), vec))
        })
        .collect();

    if nodes_with_emb.len() < 2 {
        return Vec::new();
    }

    // Parallel pairwise cosine similarity
    let results: Vec<(String, String, f64)> = nodes_with_emb
        .par_iter()
        .enumerate()
        .flat_map(|(i, (id_a, emb_a))| {
            let mut local_edges = Vec::new();
            for (id_b, emb_b) in nodes_with_emb.iter().skip(i + 1) {
                let sim = cosine_similarity(emb_a, emb_b);
                if sim >= threshold {
                    local_edges.push((id_a.clone(), id_b.clone(), sim));
                }
            }
            local_edges
        })
        .collect();

    results
}

/// One proposed entity-resolution action emitted by [`resolve_candidates`].
///
/// `kind` is `"same_as"` (a true-duplicate cluster — safe to merge onto
/// `canonical`) or `"extends"` (a subtype/version relationship between distinct
/// entities — link, don't merge). The op is **read/propose only**: it never
/// mutates the graph, so the client decides whether to apply via `BatchUpdate`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MergeProposal {
    pub canonical: String,
    pub members: Vec<String>,
    pub score: f64,
    pub kind: String,
}

/// Native entity-resolution candidate generator (CONCEPT:AU-KG.compute.when-exposes-native).
///
/// Composes the existing embedding + clustering primitives into ONE server-side
/// read op so the agent-utilities dedup ladder's residual escalates here instead
/// of an O(N²) client-side embedding pass:
///   1. collect entity nodes carrying an embedding (optionally filtered by type);
///   2. all-pairs cosine ≥ `sim_threshold` (rayon, off-lock by the caller);
///   3. union-find clusters over SAME-TYPE pairs ≥ `merge_threshold` → `same_as`
///      proposals (canonical = highest-degree member, sift-kg `resolver.py:190`);
///   4. high-sim pairs across DIFFERENT types → `extends` proposals (the
///      duplicates-vs-variants split; the OWL subclass refinement happens in the
///      ontology layer downstream).
///
/// Returns proposals only — applying them is the client's decision.
pub fn resolve_candidates(
    core: &GraphView,
    sim_threshold: f64,
    merge_threshold: f64,
    node_type: Option<&str>,
) -> Vec<MergeProposal> {
    use rayon::prelude::*;

    // (id, embedding, node_type) for embedded nodes, optionally type-filtered.
    let nodes: Vec<(String, Vec<f64>, String)> = core
        .node_properties
        .iter()
        .filter_map(|(node_id, props_json)| {
            let val: serde_json::Value = serde_json::from_slice(props_json).ok()?;
            let vec: Vec<f64> = serde_json::from_value(val.get("embedding")?.clone()).ok()?;
            if vec.is_empty() {
                return None;
            }
            let nt = val
                .get("node_type")
                .or_else(|| val.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(filter) = node_type {
                if nt != filter {
                    return None;
                }
            }
            Some((node_id.clone(), vec, nt))
        })
        .collect();

    if nodes.len() < 2 {
        return Vec::new();
    }

    // All-pairs cosine ≥ sim_threshold (the candidate floor).
    let pairs: Vec<(usize, usize, f64)> = (0..nodes.len())
        .into_par_iter()
        .flat_map(|i| {
            let mut local = Vec::new();
            for j in (i + 1)..nodes.len() {
                let s = cosine_similarity(&nodes[i].1, &nodes[j].1);
                if s >= sim_threshold {
                    local.push((i, j, s));
                }
            }
            local
        })
        .collect();
    if pairs.is_empty() {
        return Vec::new();
    }

    // Union-find over SAME-TYPE pairs ≥ merge_threshold (the same_as bar).
    let mut parent: Vec<usize> = (0..nodes.len()).collect();
    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }
    let mut degree = vec![0usize; nodes.len()];
    let mut extends_pairs: Vec<(usize, usize, f64)> = Vec::new();
    for &(i, j, s) in &pairs {
        degree[i] += 1;
        degree[j] += 1;
        if s >= merge_threshold && nodes[i].2 == nodes[j].2 {
            let (ri, rj) = (find(&mut parent, i), find(&mut parent, j));
            if ri != rj {
                parent[ri] = rj;
            }
        } else {
            // weak (sim below merge bar) OR cross-type high-sim → variant link
            extends_pairs.push((i, j, s));
        }
    }

    // same_as clusters
    let mut clusters: std::collections::HashMap<usize, Vec<usize>> =
        std::collections::HashMap::new();
    for idx in 0..nodes.len() {
        let root = find(&mut parent, idx);
        clusters.entry(root).or_default().push(idx);
    }
    let mut proposals: Vec<MergeProposal> = Vec::new();
    let mut clustered: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for members in clusters.values() {
        if members.len() < 2 {
            continue;
        }
        for &m in members {
            clustered.insert(m);
        }
        // canonical = highest-degree member (most corroborating similar neighbours)
        let canonical_idx = *members
            .iter()
            .max_by_key(|&&m| degree[m])
            .unwrap_or(&members[0]);
        let score = pairs
            .iter()
            .filter(|(i, j, _)| members.contains(i) && members.contains(j))
            .map(|(_, _, s)| *s)
            .fold(0.0_f64, f64::max);
        proposals.push(MergeProposal {
            canonical: nodes[canonical_idx].0.clone(),
            members: members.iter().map(|&m| nodes[m].0.clone()).collect(),
            score,
            kind: "same_as".to_string(),
        });
    }

    // extends proposals (cross-type / weak high-sim pairs not already merged)
    for &(i, j, s) in &extends_pairs {
        if find(&mut parent, i) == find(&mut parent, j) {
            continue; // already in one same_as cluster
        }
        proposals.push(MergeProposal {
            canonical: nodes[i].0.clone(),
            members: vec![nodes[i].0.clone(), nodes[j].0.clone()],
            score: s,
            kind: "extends".to_string(),
        });
    }

    proposals
}

/// Cosine similarity between two vectors.
fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let norm_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}

/// Lifecycle-aware pruning: remove nodes past max_age or below min_score.
///
/// Examines node properties for `created_at` (epoch seconds) and `score` fields.
/// Nodes older than `max_age_secs` or with score below `min_score` are removed.
pub fn prune_by_lifecycle(
    core: &GraphCore,
    max_age_secs: u64,
    min_score: f64,
) -> crate::types::PruneStats {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut to_remove: Vec<String> = Vec::new();
    let mut archived = 0usize;

    for entry in core.node_properties.iter() {
        let (node_id, props_json) = (entry.key(), entry.value());
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(props_json.as_slice()) {
            let created_at = val.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0);
            let score = val.get("score").and_then(|v| v.as_f64()).unwrap_or(1.0);
            let lifecycle = val
                .get("lifecycle_state")
                .and_then(|v| v.as_str())
                .unwrap_or("active");

            // Skip already archived/compacted
            if lifecycle == "archived" || lifecycle == "compacted" {
                continue;
            }

            let age = if created_at > 0 { now - created_at } else { 0 };

            if (max_age_secs > 0 && age > max_age_secs) || score < min_score {
                to_remove.push(node_id.clone());
                archived += 1;
            }
        }
    }

    let nodes_removed = to_remove.len();
    let mut edges_removed = 0usize;

    for node_id in &to_remove {
        // Count edges that will be removed
        let edge_keys: Vec<(String, String)> = core
            .edge_properties
            .iter()
            .map(|e| e.key().clone())
            .filter(|(src, tgt)| src == node_id || tgt == node_id)
            .collect();
        edges_removed += edge_keys.len();
        core.remove_node(node_id.clone());
    }

    crate::types::PruneStats {
        nodes_removed,
        edges_removed,
        nodes_archived: archived,
    }
}

/// Get an optimized context view for an agent within a token budget.
///
/// Traverses the graph from the agent node via BFS, collecting relevant
/// nodes and edges up to the token budget (estimated at ~4 chars per token).
pub fn get_context_view(
    core: &GraphView,
    agent_id: &str,
    max_tokens: u32,
) -> crate::types::ContextView {
    let chars_per_token = 4u32;
    let max_chars = max_tokens * chars_per_token;
    let mut used_chars = 0u32;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    // BFS from agent_id
    let start_idx = match core.node_map.get(agent_id) {
        Some(&idx) => idx,
        None => {
            return crate::types::ContextView {
                agent_id: agent_id.to_string(),
                ..Default::default()
            }
        }
    };

    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut queue: VecDeque<NodeIndex> = VecDeque::new();
    queue.push_back(start_idx);
    visited.insert(start_idx);

    while let Some(curr) = queue.pop_front() {
        let node_id = core.graph[curr].clone();
        let props = core
            .node_properties
            .get(&node_id)
            .cloned()
            .unwrap_or_default();
        let node_chars = (node_id.len() + props.len()) as u32;

        if used_chars + node_chars > max_chars {
            break;
        }
        used_chars += node_chars;
        nodes.push(node_id.clone());

        // Collect edges and queue neighbors
        for edge in core
            .graph
            .edges_directed(curr, petgraph::Direction::Outgoing)
        {
            let target = edge.target();
            let target_id = core.graph[target].clone();
            let edge_props = core
                .edge_properties
                .get(&(node_id.clone(), target_id.clone()))
                .and_then(|v| v.first())
                .map(|a| (**a).clone())
                .unwrap_or_default();
            edges.push((node_id.clone(), target_id.clone(), edge_props));

            if visited.insert(target) {
                queue.push_back(target);
            }
        }
        for edge in core
            .graph
            .edges_directed(curr, petgraph::Direction::Incoming)
        {
            let source = edge.source();
            if visited.insert(source) {
                queue.push_back(source);
            }
        }
    }

    crate::types::ContextView {
        agent_id: agent_id.to_string(),
        nodes,
        edges,
        budget_used: used_chars / chars_per_token,
        budget_max: max_tokens,
    }
}

/// One validated operation in the public `BatchUpdate` wire contract.
///
/// This type is shared with the redb row adapter so RAM execution, WAL replay,
/// embedded mode, and authoritative persistence cannot drift onto different field
/// names. The public keys are deliberately `id`, `source`, and `target`.
#[derive(Debug, Clone, PartialEq)]
pub enum BatchOperation {
    AddNode {
        id: String,
        properties_msgpack: Vec<u8>,
        upsert: bool,
    },
    RemoveNode {
        id: String,
    },
    AddEdge {
        source: String,
        target: String,
        properties_msgpack: Vec<u8>,
        upsert: bool,
    },
    RemoveEdge {
        source: String,
        target: String,
    },
    AddEmbedding {
        id: String,
        embedding: Vec<f32>,
    },
}

const MAX_BATCH_UPDATE_BYTES: usize = 32 * 1024 * 1024;
const MAX_BATCH_UPDATE_ITEMS: usize = 500_000;
const MAX_BATCH_OPERATIONS: usize = 50_000;
const MAX_BATCH_ID_BYTES: usize = 4_096;
const MAX_BATCH_PROPERTIES_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Default, serde::Serialize)]
struct BatchUpdateSummary {
    added_nodes: u32,
    upserted_nodes: u32,
    removed_nodes: u32,
    added_edges: u32,
    upserted_edges: u32,
    removed_edges: u32,
    added_embeddings: u32,
    errors: Vec<String>,
}

fn required_batch_id(
    operation: &serde_json::Value,
    index: usize,
    key: &str,
) -> Result<String, String> {
    let value = operation
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("BatchUpdate op[{index}] requires a non-empty string '{key}'"))?;
    if value.len() > MAX_BATCH_ID_BYTES || value.chars().any(char::is_control) {
        return Err(format!(
            "BatchUpdate op[{index}] '{key}' exceeds the identifier policy"
        ));
    }
    Ok(value.to_owned())
}

fn batch_properties(operation: &serde_json::Value, index: usize) -> Result<Vec<u8>, String> {
    let value = operation
        .get("properties")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    if !value.is_object() {
        return Err(format!(
            "BatchUpdate op[{index}] 'properties' must be an object"
        ));
    }
    let encoded = rmp_serde::to_vec_named(&value)
        .map_err(|_| format!("BatchUpdate op[{index}] properties encode failed"))?;
    if encoded.len() > MAX_BATCH_PROPERTIES_BYTES {
        return Err(format!(
            "BatchUpdate op[{index}] properties exceed the resource limit"
        ));
    }
    Ok(encoded)
}

/// Merge the supplied top-level fields into one existing node property object.
///
/// Both the RAM executor and the authoritative redb row adapter call this exact
/// routine so `upsert_node` cannot drift between resident and durable state.
/// Nested values are replaced as complete top-level fields; this is deliberately
/// not a recursive JSON merge.
pub fn merge_batch_node_properties(current: &[u8], updates: &[u8]) -> Result<Vec<u8>, String> {
    let current = eg_types::msgpack::decode_property_value(current)
        .map_err(|_| "existing node properties are not a valid object".to_string())?;
    let updates = eg_types::msgpack::decode_property_value(updates)
        .map_err(|_| "upsert properties are not a valid object".to_string())?;
    let serde_json::Value::Object(mut current) = current else {
        return Err("existing node properties are not a valid object".to_string());
    };
    let serde_json::Value::Object(updates) = updates else {
        return Err("upsert properties are not a valid object".to_string());
    };
    current.extend(updates);
    rmp_serde::to_vec_named(&serde_json::Value::Object(current)).map_err(|error| error.to_string())
}

/// Decode and validate the public `BatchUpdate` schema without mutating state.
///
/// Malformed MessagePack, missing fields, unknown operations, non-object properties,
/// and invalid embeddings are terminal errors. Callers must never reinterpret an
/// opaque or partially decoded payload as an empty successful batch.
pub fn decode_batch_operations(operations_msgpack: &[u8]) -> Result<Vec<BatchOperation>, String> {
    let operations: Vec<serde_json::Value> = eg_types::msgpack::decode_bounded(
        operations_msgpack,
        eg_types::msgpack::MsgpackLimits::new(MAX_BATCH_UPDATE_BYTES, MAX_BATCH_UPDATE_ITEMS, 64),
    )
    .map_err(|_| "[EpistemicGraph::batch_update] invalid or over-complex MsgPack".to_string())?;
    if operations.len() > MAX_BATCH_OPERATIONS {
        return Err("BatchUpdate operation count exceeds the resource limit".to_string());
    }
    let mut decoded = Vec::with_capacity(operations.len());
    for (index, operation) in operations.iter().enumerate() {
        let kind = operation
            .get("op")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("BatchUpdate op[{index}] requires string 'op'"))?;
        let decoded_operation = match kind {
            "add_node" | "upsert_node" => BatchOperation::AddNode {
                id: required_batch_id(operation, index, "id")?,
                properties_msgpack: batch_properties(operation, index)?,
                upsert: kind == "upsert_node",
            },
            "remove_node" => BatchOperation::RemoveNode {
                id: required_batch_id(operation, index, "id")?,
            },
            "add_edge" | "upsert_edge" => BatchOperation::AddEdge {
                source: required_batch_id(operation, index, "source")?,
                target: required_batch_id(operation, index, "target")?,
                properties_msgpack: batch_properties(operation, index)?,
                upsert: kind == "upsert_edge",
            },
            "remove_edge" => BatchOperation::RemoveEdge {
                source: required_batch_id(operation, index, "source")?,
                target: required_batch_id(operation, index, "target")?,
            },
            "add_embedding" => {
                let id = required_batch_id(operation, index, "id")?;
                let values = operation
                    .get("embedding")
                    .and_then(serde_json::Value::as_array)
                    .filter(|values| !values.is_empty())
                    .ok_or_else(|| {
                        format!(
                            "BatchUpdate op[{index}] 'embedding' must be a non-empty number array"
                        )
                    })?;
                if values.len() > MAX_EMBEDDING_DIMENSION {
                    return Err(format!(
                        "BatchUpdate op[{index}] embedding exceeds the dimension limit"
                    ));
                }
                let mut embedding = Vec::with_capacity(values.len());
                for value in values {
                    let number = value.as_f64().ok_or_else(|| {
                        format!("BatchUpdate op[{index}] embedding contains a non-number")
                    })?;
                    let component = number as f32;
                    if !number.is_finite() || !component.is_finite() {
                        return Err(format!(
                            "BatchUpdate op[{index}] embedding contains a non-finite component"
                        ));
                    }
                    embedding.push(component);
                }
                BatchOperation::AddEmbedding { id, embedding }
            }
            _ => return Err(format!("BatchUpdate op[{index}] has an unknown operation")),
        };
        decoded.push(decoded_operation);
    }
    Ok(decoded)
}

fn prepare_batch_operations_with(
    operations: &mut [BatchOperation],
    mut node_exists: impl FnMut(&str) -> bool,
    mut node_properties: impl FnMut(&str) -> Option<Vec<u8>>,
    // CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007). The store's CURRENT established
    // embedding dimension (`0` = store empty / unset). Every `AddEmbedding` op in
    // this batch is validated against it (and against each other, once the batch
    // itself establishes a dimension for an empty store) BEFORE `batch_update`
    // applies a single one to the LIVE `semantic_store` — that apply loop mutates
    // the resident store directly (no rollback), so a mixed-dimension batch MUST be
    // rejected here, whole, or it would partially apply.
    store_dim: usize,
) -> Result<(), String> {
    // Track only ids touched by this batch. The property image is needed so two
    // ordered upserts merge cumulatively before the first RAM mutation occurs.
    let mut node_state = HashMap::<String, Option<Vec<u8>>>::new();
    let mut expected_embedding_dim = store_dim;
    for (index, operation) in operations.iter_mut().enumerate() {
        match operation {
            BatchOperation::AddNode {
                id,
                properties_msgpack,
                upsert,
            } => {
                if *upsert {
                    let current = match node_state.get(id) {
                        Some(properties) => properties.clone(),
                        None => match node_properties(id) {
                            Some(properties) => Some(properties),
                            None if node_exists(id) => {
                                return Err(format!(
                                    "BatchUpdate op[{index}] node '{id}' has no property document"
                                ));
                            }
                            None => None,
                        },
                    };
                    if let Some(current) = current {
                        *properties_msgpack = merge_batch_node_properties(
                            &current,
                            properties_msgpack,
                        )
                        .map_err(|reason| {
                            format!("BatchUpdate op[{index}] cannot upsert node '{id}': {reason}")
                        })?;
                    }
                }
                node_state.insert(id.clone(), Some(properties_msgpack.clone()));
            }
            BatchOperation::RemoveNode { id } => {
                node_state.insert(id.clone(), None);
            }
            BatchOperation::AddEdge { source, target, .. } => {
                let source_exists = node_state
                    .get(source)
                    .map(Option::is_some)
                    .unwrap_or_else(|| node_exists(source));
                let target_exists = node_state
                    .get(target)
                    .map(Option::is_some)
                    .unwrap_or_else(|| node_exists(target));
                if !source_exists || !target_exists {
                    return Err(format!(
                        "BatchUpdate op[{index}] edge endpoints must exist at that point in the batch"
                    ));
                }
            }
            BatchOperation::RemoveEdge { .. } => {}
            BatchOperation::AddEmbedding { id, embedding } => {
                let node_exists = node_state
                    .get(id)
                    .map(Option::is_some)
                    .unwrap_or_else(|| node_exists(id));
                if !node_exists {
                    return Err(format!(
                        "BatchUpdate op[{index}] embedding node '{id}' does not exist"
                    ));
                }
                // Same guard as the arena/map chokepoint (`SemanticStore::add_embedding`),
                // run for EVERY embedding op in the batch up front so a
                // mixed-dimension OR non-finite-component batch (e.g. op[2] matches
                // the store, op[5] doesn't, or op[5] contains a NaN) is rejected as a
                // whole, before `batch_update` applies op[0]/op[1] to the live store.
                expected_embedding_dim = eg_core::compute::semantic::check_embedding_dimension(
                    embedding,
                    expected_embedding_dim,
                )
                .map_err(|error| format!("BatchUpdate op[{index}] {error}"))?;
            }
        }
    }
    Ok(())
}

fn prepare_batch_operations(
    core: &GraphCore,
    operations: &mut [BatchOperation],
) -> Result<(), String> {
    prepare_batch_operations_with(
        operations,
        |id| core.has_node(id),
        |id| core.get_node_properties(id),
        core.semantic_store.read().dim(),
    )
}

fn batch_summary(operations: &[BatchOperation]) -> BatchUpdateSummary {
    let mut summary = BatchUpdateSummary::default();
    for operation in operations {
        match operation {
            BatchOperation::AddNode { upsert, .. } => {
                summary.added_nodes += 1;
                summary.upserted_nodes += u32::from(*upsert);
            }
            BatchOperation::RemoveNode { .. } => summary.removed_nodes += 1,
            BatchOperation::AddEdge { upsert, .. } => {
                summary.added_edges += 1;
                summary.upserted_edges += u32::from(*upsert);
            }
            BatchOperation::RemoveEdge { .. } => summary.removed_edges += 1,
            BatchOperation::AddEmbedding { .. } => summary.added_embeddings += 1,
        }
    }
    summary
}

fn encode_batch_summary(summary: &BatchUpdateSummary) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(summary).map_err(|error| error.to_string())
}

/// Validate a batch against the current graph and return its deterministic success
/// payload without applying it. The authoritative mutation gateway uses this to put
/// valid batches on the durable-before-RAM row path.
pub fn batch_update_preview(
    core: &GraphCore,
    operations_msgpack: &[u8],
) -> Result<Vec<u8>, String> {
    let mut operations = decode_batch_operations(operations_msgpack)?;
    prepare_batch_operations(core, &mut operations)?;
    encode_batch_summary(&batch_summary(&operations))
}

/// Apply a validated collection of graph/vector operations atomically from the
/// caller's perspective. Structural rows share one topology transaction; semantic
/// actions execute in wire order while that topology guard is still held. The same
/// decoded operations are used by redb, so replay and restart preserve RAM behavior.
///
/// Supported operations are `add_node`, `upsert_node`, `remove_node`, `add_edge`,
/// `upsert_edge`, `remove_edge`, and `add_embedding`. Nodes use `id`; edges use
/// `source` and `target`; embeddings use `id` plus a non-empty `embedding` array.
pub fn batch_update(core: &GraphCore, operations_msgpack: &[u8]) -> Result<Vec<u8>, String> {
    let mut operations = decode_batch_operations(operations_msgpack)?;
    let summary = batch_summary(&operations);
    let capture_content = core.wants_change_content();
    let mut node_upserts = BTreeMap::<String, Vec<u8>>::new();
    let mut node_removals = BTreeSet::<String>::new();
    let mut change = eg_core::index::ChangeSet::new();

    enum SemanticAction {
        Upsert(String, Vec<f32>),
        Remove(String),
    }
    let mut semantic_actions = Vec::new();

    // ONE topology guard makes every structural operation atomic to graph readers.
    let mut txn = core.txn();
    // Revalidate against the state protected by this exact guard. Validation must
    // finish before the first mutation so a concurrent removal between preview and
    // execution cannot turn an otherwise valid batch into a partial write.
    prepare_batch_operations_with(
        &mut operations,
        |id| txn.has_node(id),
        |id| txn.get_node_properties(id),
        // Embeddings are NEVER staged through `txn` (that guard covers node/edge
        // topology only) — the apply loop below mutates `core.semantic_store`
        // directly, live, regardless of txn state. So the dimension to validate
        // against here is the SAME live store `prepare_batch_operations` reads,
        // not anything txn-scoped; there is no separate "staged" dimension to be
        // consistent with (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard, BUG-007).
        core.semantic_store.read().dim(),
    )?;
    let source_version = core.version();
    for operation in operations {
        match operation {
            BatchOperation::AddNode {
                id,
                properties_msgpack,
                ..
            } => {
                if capture_content {
                    node_removals.remove(&id);
                    node_upserts.insert(id.clone(), properties_msgpack.clone());
                } else {
                    node_removals.remove(&id);
                    node_upserts.insert(id.clone(), Vec::new());
                }
                txn.add_node(id, properties_msgpack);
            }
            BatchOperation::RemoveNode { id } => {
                node_upserts.remove(&id);
                node_removals.insert(id.clone());
                txn.remove_node(id.clone());
                semantic_actions.push(SemanticAction::Remove(id));
            }
            BatchOperation::AddEdge {
                source,
                target,
                properties_msgpack,
                upsert,
            } => {
                if upsert {
                    txn.remove_edge(source.clone(), target.clone());
                    change.record_remove_edge(source.clone(), target.clone());
                }
                txn.add_edge(source.clone(), target.clone(), properties_msgpack)?;
                change.record_add_edge(source, target);
            }
            BatchOperation::RemoveEdge { source, target } => {
                txn.remove_edge(source.clone(), target.clone());
                change.record_remove_edge(source, target);
            }
            BatchOperation::AddEmbedding { id, embedding } => {
                semantic_actions.push(SemanticAction::Upsert(id, embedding));
            }
        }
    }

    // Preserve operation order: remove→re-add→embedding and embedding→remove
    // reach the same final semantic state after RAM execution and durable replay.
    if !semantic_actions.is_empty() {
        let mut semantic = core.semantic_store.write();
        for action in semantic_actions {
            match action {
                // `prepare_batch_operations` already validated every embedding's
                // dimension against the store and against the rest of this batch
                // (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard, BUG-007), so this should never
                // observe a mismatch — propagated rather than `.expect()`-panicked
                // so a genuine surprise (e.g. a concurrent mutation between
                // validation and apply) fails closed instead of crashing the
                // dispatcher.
                SemanticAction::Upsert(id, embedding) => semantic
                    .add_embedding(id, embedding)
                    .map_err(|error| error.to_string())?,
                SemanticAction::Remove(id) => {
                    semantic.remove_embedding(&id);
                }
            }
        }
    }

    for (id, properties) in node_upserts {
        if capture_content {
            change
                .added_nodes
                .push(eg_core::index::NodeChange::with_properties(id, properties));
        } else {
            change.record_add_node(id);
        }
    }
    for id in node_removals {
        change.record_remove_node(id);
    }
    let node_count = txn.node_count();
    let edge_count = txn.edge_count();
    core.maintain_indexes_at(
        &change,
        source_version.saturating_add(1),
        node_count,
        edge_count,
    );
    drop(txn);

    encode_batch_summary(&summary)
}

/// Compute runtime metrics for observability.
pub fn compute_metrics(core: &GraphView) -> crate::types::GraphMetrics {
    let mut active = 0usize;
    let mut compacted = 0usize;
    let mut archived = 0usize;

    for props_json in core.node_properties.values() {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(props_json) {
            match val
                .get("lifecycle_state")
                .and_then(|v| v.as_str())
                .unwrap_or("active")
            {
                "compacted" => compacted += 1,
                "archived" => archived += 1,
                _ => active += 1,
            }
        } else {
            active += 1;
        }
    }

    crate::types::GraphMetrics {
        node_count: core.node_map.len(),
        edge_count: core.edge_properties.values().map(|v| v.len()).sum(),
        // The ledger is not part of the read view; the caller (server Metrics
        // handler) captures the live ledger length and overwrites this field.
        total_mutations: 0,
        last_prune_removed: 0,
        active_nodes: active,
        compacted_nodes: compacted,
        archived_nodes: archived,
    }
}

/// Personalized PageRank with seed (teleport) nodes.
///
/// Similar to standard PageRank but the random walker teleports to seed
/// nodes weighted by their seed score instead of uniformly.
pub fn personalized_pagerank(
    core: &GraphView,
    seed_nodes: &[(String, f64)],
    damping: f64,
    iterations: usize,
) -> Vec<(String, f64)> {
    let nodes: Vec<NodeIndex> = core.graph.node_indices().collect();
    let n = nodes.len();
    if n == 0 {
        return Vec::new();
    }

    let initial = 1.0 / n as f64;
    let mut scores: HashMap<NodeIndex, f64> = HashMap::new();
    for &node in &nodes {
        scores.insert(node, initial);
    }

    // Build teleport vector
    let mut teleport: HashMap<NodeIndex, f64> = HashMap::new();
    let total_seed_weight: f64 = seed_nodes.iter().map(|(_, w)| w).sum();

    if total_seed_weight > 0.0 {
        for (seed_id, weight) in seed_nodes {
            if let Some(&idx) = core.node_map.get(seed_id) {
                teleport.insert(idx, weight / total_seed_weight);
            }
        }
    } else {
        // Uniform teleport if no seeds
        let uniform = 1.0 / n as f64;
        for &node in &nodes {
            teleport.insert(node, uniform);
        }
    }

    // Pre-compute out-degree
    let mut out_degree: HashMap<NodeIndex, usize> = HashMap::new();
    for &node in &nodes {
        out_degree.insert(
            node,
            core.graph
                .edges_directed(node, petgraph::Direction::Outgoing)
                .count(),
        );
    }

    for _ in 0..iterations {
        let mut new_scores: HashMap<NodeIndex, f64> = HashMap::new();

        for &node in &nodes {
            let mut rank_sum = 0.0;
            for edge in core
                .graph
                .edges_directed(node, petgraph::Direction::Incoming)
            {
                let src = edge.source();
                let src_out = *out_degree.get(&src).unwrap_or(&1);
                if src_out > 0 {
                    rank_sum += scores[&src] / src_out as f64;
                }
            }
            let tp = teleport.get(&node).copied().unwrap_or(0.0);
            new_scores.insert(node, (1.0 - damping) * tp + damping * rank_sum);
        }

        scores = new_scores;
    }

    scores
        .into_iter()
        .map(|(idx, score)| (core.graph[idx].clone(), score))
        .collect()
}

#[cfg(test)]
mod community_tests {
    use super::*;

    fn p() -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": "Code"})).unwrap()
    }

    fn build(nodes: &[&str], edges: &[(&str, &str)]) -> GraphView {
        let g = GraphCore::new();
        for n in nodes {
            g.add_node((*n).to_string(), p());
        }
        for (s, t) in edges {
            g.add_edge((*s).to_string(), (*t).to_string(), p()).unwrap();
        }
        g.analysis_snapshot()
    }

    #[test]
    fn community_detection_separates_two_blocks() {
        // Mirrors the CommunityDetectEphemeral handler: build a graph from inline
        // nodes+edges (two dense triangles joined by one bridge) and assert
        // detection separates them into distinct communities.
        let nodes: Vec<String> = (0..8).map(|i| format!("n{i}")).collect();
        let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
        let edges = [
            ("n0", "n1"),
            ("n1", "n2"),
            ("n2", "n0"),
            ("n4", "n5"),
            ("n5", "n6"),
            ("n6", "n4"),
            ("n2", "n4"), // single bridge between the two blocks
        ];
        let g = build(&node_refs, &edges);
        let comms = community_detection(&g, 1.0);
        assert!(
            comms.len() >= 2,
            "expected >=2 communities for two bridged blocks, got {}",
            comms.len()
        );
    }

    #[test]
    fn empty_graph_returns_empty() {
        let g = GraphCore::new().analysis_snapshot();
        assert!(community_detection(&g, 1.0).is_empty());
    }

    #[test]
    fn deterministic_across_runs() {
        // A symmetric structure with label ties is exactly what made the old
        // HashMap-order tie-break non-deterministic. The hardened version must
        // return the SAME partition every call.
        let nodes = ["a", "b", "c", "d", "e", "f"];
        let edges = [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"), // triangle 1
            ("d", "e"),
            ("e", "f"),
            ("f", "d"), // triangle 2
        ];
        let g = build(&nodes, &edges);
        let first = community_detection(&g, 1.0);
        for _ in 0..20 {
            assert_eq!(community_detection(&g, 1.0), first, "result must be stable");
        }
        // Output is sorted (communities by first member, members sorted).
        for community in &first {
            let mut sorted = community.clone();
            sorted.sort_unstable();
            assert_eq!(community, &sorted);
        }
    }

    #[test]
    fn separates_two_disconnected_cliques() {
        let g = build(
            &["a", "b", "c", "x", "y", "z"],
            &[
                ("a", "b"),
                ("b", "c"),
                ("c", "a"),
                ("x", "y"),
                ("y", "z"),
                ("z", "x"),
            ],
        );
        let communities = community_detection(&g, 1.0);
        // Every node assigned exactly once; the two cliques never merge.
        let total: usize = communities.iter().map(|c| c.len()).sum();
        assert_eq!(total, 6);
        for c in &communities {
            let has_first = c.iter().any(|n| ["a", "b", "c"].contains(&n.as_str()));
            let has_second = c.iter().any(|n| ["x", "y", "z"].contains(&n.as_str()));
            assert!(!(has_first && has_second), "cliques must not merge: {c:?}");
        }
    }

    #[test]
    fn terminates_quickly_on_dense_graph() {
        // A dense graph with many ties is the oscillation-prone case. With the
        // wall-clock budget + deterministic tie-break it must finish well under
        // the budget and partition all nodes.
        let ids: Vec<String> = (0..120).map(|i| format!("n{i:03}")).collect();
        let g = GraphCore::new();
        for id in &ids {
            g.add_node(id.clone(), p());
        }
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                g.add_edge(ids[i].clone(), ids[j].clone(), p()).unwrap();
            }
        }
        let view = g.analysis_snapshot();
        let start = Instant::now();
        let communities = community_detection(&view, 1.0);
        assert!(
            start.elapsed() < COMMUNITY_DETECTION_BUDGET + Duration::from_secs(2),
            "must respect the wall-clock budget"
        );
        let total: usize = communities.iter().map(|c| c.len()).sum();
        assert_eq!(total, ids.len(), "every node must be assigned a community");
    }

    #[test]
    fn batch_update_stores_msgpack_readable_properties() {
        // Regression: batch_update used to store JSON-string bytes, which the
        // read path (msgpack) couldn't decode → batch-written nodes looked empty.
        let g = GraphCore::new();
        let ops = serde_json::json!([
            {"op": "add_node", "id": "code:A", "properties": {"type": "Code", "language": "java", "name": "Widget"}},
            {"op": "add_node", "id": "code:B", "properties": {"type": "Code", "language": "rust"}},
            {"op": "add_edge", "source": "code:A", "target": "code:B", "properties": {"relationship": "CALLS"}},
        ]);
        let ops_mp = rmp_serde::to_vec_named(&ops).unwrap();
        let res_mp = batch_update(&g, &ops_mp).unwrap();
        let res: serde_json::Value = rmp_serde::from_slice(&res_mp).unwrap();
        assert_eq!(res["added_nodes"], 2);
        assert_eq!(res["added_edges"], 1);
        assert_eq!(g.node_count(), 2);

        // The stored property bytes MUST decode as MsgPack (not JSON bytes) and
        // round-trip the values — exactly what the Python client expects.
        let raw = g.get_node_properties("code:A").expect("node A present");
        let props: serde_json::Value = rmp_serde::from_slice(&raw).expect("props are msgpack");
        assert_eq!(props["language"], "java");
        assert_eq!(props["name"], "Widget");
    }

    #[test]
    fn upsert_node_merges_top_level_fields_and_creates_missing_node() {
        let g = GraphCore::new();
        g.add_node(
            "existing".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "retained": "yes",
                "overwritten": "old",
                "nested": {"left": 1, "right": 2}
            }))
            .unwrap(),
        );
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {
                "op": "upsert_node",
                "id": "existing",
                "properties": {
                    "overwritten": "new",
                    "added": true,
                    "nested": {"left": 9}
                }
            },
            {"op": "upsert_node", "id": "existing", "properties": {"last": true}},
            {"op": "upsert_node", "id": "created", "properties": {"created": true}}
        ]))
        .unwrap();

        let preview = batch_update_preview(&g, &operations).unwrap();
        let applied = batch_update(&g, &operations).unwrap();

        assert_eq!(preview, applied);
        let existing: serde_json::Value =
            rmp_serde::from_slice(&g.get_node_properties("existing").unwrap()).unwrap();
        assert_eq!(existing["retained"], "yes");
        assert_eq!(existing["overwritten"], "new");
        assert_eq!(existing["added"], true);
        assert_eq!(existing["last"], true);
        assert_eq!(existing["nested"], serde_json::json!({"left": 9}));
        let created: serde_json::Value =
            rmp_serde::from_slice(&g.get_node_properties("created").unwrap()).unwrap();
        assert_eq!(created, serde_json::json!({"created": true}));
    }

    #[test]
    fn invalid_existing_upsert_fails_before_any_ram_mutation() {
        let g = GraphCore::new();
        let invalid = rmp_serde::to_vec_named(&serde_json::json!("not-an-object")).unwrap();
        g.add_node("invalid".to_string(), invalid.clone());
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "would-have-been-partial", "properties": {}},
            {"op": "upsert_node", "id": "invalid", "properties": {"field": "value"}}
        ]))
        .unwrap();

        let error = batch_update(&g, &operations).unwrap_err();

        assert!(error.contains("cannot upsert"));
        assert!(!g.has_node("would-have-been-partial"));
        assert_eq!(g.get_node_properties("invalid"), Some(invalid));
    }

    #[test]
    fn batch_update_preview_matches_ram_upsert_vector_and_tombstone() {
        let g = GraphCore::new();
        let operations = serde_json::json!([
            {"op": "add_node", "id": "a", "properties": {"text": "old body"}},
            {"op": "add_node", "id": "b", "properties": {"text": "peer"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "old"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "also old"}},
            {"op": "upsert_edge", "source": "a", "target": "b", "properties": {"kind": "new"}},
            {"op": "add_embedding", "id": "a", "embedding": [0.25, 0.75]}
        ]);
        let bytes = rmp_serde::to_vec_named(&operations).unwrap();
        let preview = batch_update_preview(&g, &bytes).unwrap();
        let applied = batch_update(&g, &bytes).unwrap();
        assert_eq!(
            preview, applied,
            "durable prediction and RAM result drifted"
        );
        assert_eq!(g.edge_count(), 1, "upsert_edge must replace parallel rows");
        let edge: serde_json::Value =
            rmp_serde::from_slice(&g.get_edges()[0].2).expect("edge properties");
        assert_eq!(edge["kind"], "new");
        assert_eq!(
            g.semantic_store.read().get_embedding("a"),
            Some(vec![0.25, 0.75])
        );

        let remove = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "remove_node", "id": "a"}
        ]))
        .unwrap();
        batch_update(&g, &remove).unwrap();
        assert!(!g.has_node("a"));
        assert_eq!(g.edge_count(), 0, "node removal must drop incident edges");
        assert_eq!(g.semantic_store.read().get_embedding("a"), None);
    }

    #[test]
    fn malformed_batch_fails_before_any_ram_mutation() {
        let g = GraphCore::new();
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "would-have-been-partial", "properties": {}},
            {"op": "add_edge", "source": "would-have-been-partial"}
        ]))
        .unwrap();
        let error = batch_update(&g, &operations).unwrap_err();
        assert!(error.contains("target"));
        assert_eq!(g.node_count(), 0, "validation must precede the write txn");
        assert!(
            batch_update(&g, &[0xc1]).is_err(),
            "opaque MsgPack must fail"
        );
    }

    /// BUG-007 neighbouring hostile input: a batch where some `add_embedding` ops
    /// match the store's dimension and others don't must be rejected AS A WHOLE —
    /// none of it applies, including the structural `add_node` ops sharing the same
    /// batch. `batch_update` mutates `core.semantic_store` and the node/edge topology
    /// directly (no rollback), so this depends on `prepare_batch_operations`'s
    /// upfront validation pass running before any apply.
    #[test]
    fn mixed_dimension_batch_is_rejected_without_partial_mutation() {
        let g = GraphCore::new();
        // Establish the store's dimension at 2.
        g.add_node("a".into(), p());
        g.semantic_store
            .write()
            .add_embedding("a".into(), vec![1.0, 0.0])
            .unwrap();
        let before = g.semantic_store.read().embeddings_snapshot();

        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "b", "properties": {}},
            {"op": "add_embedding", "id": "b", "embedding": [0.0, 1.0]},
            {"op": "add_node", "id": "c", "properties": {}},
            // Mismatched: 3 components instead of the batch/store's 2.
            {"op": "add_embedding", "id": "c", "embedding": [1.0, 2.0, 3.0]}
        ]))
        .unwrap();

        let error = batch_update(&g, &operations).unwrap_err();
        assert!(
            error.contains("dimension"),
            "error must name the dimension problem: {error}"
        );

        // NOTHING from the batch applied — not "b" (which would have been valid
        // alone), not "c", not even the add_node rows sharing the batch.
        assert!(!g.has_node("b"), "no partial node application");
        assert!(!g.has_node("c"), "no partial node application");
        assert_eq!(
            g.semantic_store.read().embeddings_snapshot(),
            before,
            "the pre-existing embedding corpus must be untouched (BUG-007)"
        );
        assert_eq!(g.semantic_store.read().len(), 1);
    }

    /// GOC-08: `decode_batch_operations` already rejects a non-finite `embedding`
    /// component at DECODE time (`"embedding contains a non-finite component"`,
    /// above `prepare_batch_operations_with` in this file) — this pins that
    /// existing behaviour so a future refactor can't silently drop it, and
    /// documents why `mixed_dimension_batch_is_rejected_without_partial_mutation`'s
    /// sibling test isn't `check_embedding_dimension`-shaped here: the JSON `json!`
    /// macro used by these tests converts `f64::NAN`/`f64::INFINITY` to JSON `null`
    /// (`serde_json::Value::from(f64)` maps non-finite to `Value::Null`, matching
    /// the JSON spec, which has no NaN/Infinity literal), so a non-finite float
    /// can only be exercised through this decoder via an explicit `null` — never a
    /// literal NaN — proving the DECODER's existing guard, not
    /// `check_embedding_dimension` (which guards the non-JSON callers: WAL replay,
    /// `redb_store.rs`, `graph_delta.rs`, `mutation_apply.rs`, the structural
    /// `graphlearn::embeddings` writer, and every direct `SemanticStore::add_embedding`
    /// caller — none of which round-trip through this JSON batch wire format).
    #[test]
    fn null_embedding_component_in_batch_is_rejected_without_partial_mutation() {
        let g = GraphCore::new();
        g.add_node("a".into(), p());
        g.semantic_store
            .write()
            .add_embedding("a".into(), vec![1.0, 0.0])
            .unwrap();
        let before = g.semantic_store.read().embeddings_snapshot();

        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "b", "properties": {}},
            {"op": "add_embedding", "id": "b", "embedding": [0.0, 1.0]},
            {"op": "add_node", "id": "c", "properties": {}},
            {"op": "add_embedding", "id": "c", "embedding": [1.0, null]}
        ]))
        .unwrap();

        let error = batch_update(&g, &operations).unwrap_err();
        assert!(
            error.contains("non-number"),
            "error must name the decode-time problem: {error}"
        );

        assert!(!g.has_node("b"), "no partial node application");
        assert!(!g.has_node("c"), "no partial node application");
        assert_eq!(
            g.semantic_store.read().embeddings_snapshot(),
            before,
            "the pre-existing embedding corpus must be untouched"
        );
        assert_eq!(g.semantic_store.read().len(), 1);
    }

    /// BUG-007 neighbouring hostile input: an empty batch (zero operations) is a
    /// safe no-op, not an error and not a crash.
    #[test]
    fn empty_batch_is_a_safe_noop() {
        let g = GraphCore::new();
        let operations = rmp_serde::to_vec_named(&serde_json::json!([])).unwrap();
        let applied = batch_update(&g, &operations).unwrap();
        let preview = batch_update_preview(&g, &operations).unwrap();
        assert_eq!(applied, preview);
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.semantic_store.read().len(), 0);
    }

    /// BUG-007 neighbouring hostile input: a zero-length embedding in a batch is
    /// rejected at decode time, before any operation in the batch applies.
    #[test]
    fn zero_dimension_embedding_in_batch_is_rejected() {
        let g = GraphCore::new();
        g.add_node("a".into(), p());
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "should-not-land", "properties": {}},
            {"op": "add_embedding", "id": "a", "embedding": []}
        ]))
        .unwrap();
        assert!(batch_update(&g, &operations).is_err());
        assert!(!g.has_node("should-not-land"));
    }

    /// BUG-007 neighbouring hostile input: an embedding beyond the maximum
    /// dimension is rejected at decode time, before any operation in the batch
    /// applies.
    #[test]
    fn oversized_embedding_dimension_in_batch_is_rejected() {
        let g = GraphCore::new();
        g.add_node("a".into(), p());
        let oversized = vec![0.0f64; MAX_EMBEDDING_DIMENSION + 1];
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "should-not-land", "properties": {}},
            {"op": "add_embedding", "id": "a", "embedding": oversized}
        ]))
        .unwrap();
        assert!(batch_update(&g, &operations).is_err());
        assert!(!g.has_node("should-not-land"));
    }

    #[test]
    fn batch_decode_rejects_nested_allocation_bombs_and_oversized_ids() {
        assert!(decode_batch_operations(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
        let oversized = rmp_serde::to_vec_named(&serde_json::json!([{
            "op": "add_node",
            "id": "x".repeat(MAX_BATCH_ID_BYTES + 1),
            "properties": {}
        }]))
        .unwrap();
        assert!(decode_batch_operations(&oversized).is_err());
    }

    #[test]
    fn louvain_splits_connected_graph_at_weak_bridge_deterministically() {
        // Two 5-cliques joined by a SINGLE bridge edge — a connected graph. Naive
        // label propagation tends to collapse this into one community; modularity
        // optimization (Phase C-D) keeps each dense clique whole and cuts the weak
        // bridge → exactly two communities, identically across many parallel runs.
        let mut node_strs: Vec<String> = Vec::new();
        for c in 0..2 {
            for i in 0..5 {
                node_strs.push(format!("c{c}n{i}"));
            }
        }
        let node_refs: Vec<&str> = node_strs.iter().map(|s| s.as_str()).collect();

        let mut edge_strs: Vec<(String, String)> = Vec::new();
        for c in 0..2 {
            for i in 0..5 {
                for j in (i + 1)..5 {
                    edge_strs.push((format!("c{c}n{i}"), format!("c{c}n{j}")));
                }
            }
        }
        edge_strs.push(("c0n0".to_string(), "c1n0".to_string())); // the lone bridge
        let edge_refs: Vec<(&str, &str)> = edge_strs
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();

        let g = build(&node_refs, &edge_refs);
        let first = community_detection(&g, 1.0);
        assert_eq!(
            first.len(),
            2,
            "two cliques + one bridge must yield 2 communities, got {first:?}"
        );
        // Each community is exactly one clique (5 members).
        assert!(first.iter().all(|c| c.len() == 5), "got {first:?}");
        // Coloring-parallel result is deterministic across runs.
        for _ in 0..10 {
            assert_eq!(community_detection(&g, 1.0), first);
        }
    }

    #[test]
    fn parallel_betweenness_is_deterministic_and_finds_cut_vertex() {
        // Phase C-D: Brandes parallelized over source nodes. On the path
        // b—a—hub—d—e the centre "hub" lies on the most shortest paths, so it must
        // have the maximum betweenness — and the parallel result must be identical
        // across runs (source-ordered reduction preserves the sequential value).
        let g = build(
            &["a", "b", "hub", "d", "e"],
            &[
                ("a", "b"),
                ("b", "a"),
                ("a", "hub"),
                ("hub", "a"),
                ("hub", "d"),
                ("d", "hub"),
                ("d", "e"),
                ("e", "d"),
            ],
        );
        let mut r1 = betweenness_centrality(&g);
        let mut r2 = betweenness_centrality(&g);
        r1.sort_by(|a, b| a.0.cmp(&b.0));
        r2.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(r1, r2, "parallel betweenness must be deterministic");

        let top = r1
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .unwrap();
        assert_eq!(top.0, "hub", "the cut vertex must have the max betweenness");
    }
}

#[cfg(test)]
mod cluster_hierarchy_tests {
    use super::*;

    fn p(node_type: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": node_type})).unwrap()
    }

    fn build_typed(nodes: &[(&str, &str)], edges: &[(&str, &str)]) -> GraphView {
        let g = GraphCore::new();
        for (id, ty) in nodes {
            g.add_node((*id).to_string(), p(ty));
        }
        for (s, t) in edges {
            g.add_edge((*s).to_string(), (*t).to_string(), p("_")).unwrap();
        }
        g.analysis_snapshot()
    }

    #[test]
    fn two_bridged_cliques_yield_two_level1_clusters_with_no_parent_at_the_root() {
        let mut nodes: Vec<(&str, &str)> = Vec::new();
        let mut edges: Vec<(&str, &str)> = Vec::new();
        for c in [["a", "b", "c", "d"], ["w", "x", "y", "z"]] {
            for id in c {
                nodes.push((id, "Doc"));
            }
            for i in 0..c.len() {
                for j in (i + 1)..c.len() {
                    edges.push((c[i], c[j]));
                }
            }
        }
        edges.push(("d", "w"));
        let g = build_typed(&nodes, &edges);
        let result = cluster_hierarchy(&g, None, 1.0, 0);

        assert_eq!(result.base_node_count, 8);
        let level1 = &result.levels[0];
        assert_eq!(level1.level, 1);
        assert_eq!(level1.clusters.len(), 2, "{:?}", level1.clusters);
        let total_members: usize = level1.clusters.iter().map(|c| c.node_count).sum();
        assert_eq!(total_members, 8);
        for c in &level1.clusters {
            assert_eq!(c.top_node_types, vec![("Doc".to_string(), c.node_count)]);
        }
        // The root level's clusters must all have no parent.
        let root = result.levels.last().unwrap();
        assert!(root.clusters.iter().all(|c| c.parent_id.is_none()));
        // Every non-root cluster's parent must be a real cluster id at the next level.
        for w in result.levels.windows(2) {
            let (lower, upper) = (&w[0], &w[1]);
            let upper_ids: std::collections::BTreeSet<&str> =
                upper.clusters.iter().map(|c| c.id.as_str()).collect();
            for c in &lower.clusters {
                let parent = c.parent_id.as_deref().expect("non-root cluster needs a parent");
                assert!(upper_ids.contains(parent), "dangling parent {parent}");
            }
        }
        // leaf_membership covers every base node exactly once, into a valid
        // level-1 cluster local index.
        assert_eq!(result.leaf_membership.len(), 8);
        for (_, idx) in &result.leaf_membership {
            assert!((*idx as usize) < level1.clusters.len());
        }
    }

    #[test]
    fn label_filter_restricts_the_clustered_projection() {
        let nodes = [("a", "Doc"), ("b", "Doc"), ("c", "Doc"), ("z", "Other")];
        let edges = [("a", "b"), ("b", "c"), ("a", "c"), ("a", "z")];
        let g = build_typed(&nodes, &edges);
        let result = cluster_hierarchy(&g, Some("Doc"), 1.0, 0);
        assert_eq!(result.base_node_count, 3);
        assert_eq!(
            result.leaf_membership.len(),
            3,
            "the Other-typed node must be excluded"
        );
        assert!(result.leaf_membership.iter().all(|(id, _)| id != "z"));
    }

    #[test]
    fn single_edge_merges_into_one_level1_cluster() {
        // A single edge DOES give local-moving one improving merge (both nodes
        // sharing a community beats two singletons), so this is one real level
        // with one 2-member cluster -- not the singleton-fallback path (that
        // path is exercised below, on nodes with no edges at all).
        let g = build_typed(&[("a", "Doc"), ("b", "Doc")], &[("a", "b")]);
        let result = cluster_hierarchy(&g, None, 1.0, 0);
        assert_eq!(result.levels.len(), 1);
        assert_eq!(result.levels[0].clusters.len(), 1);
        assert_eq!(result.levels[0].clusters[0].node_count, 2);
        assert!(result.levels[0].clusters[0].parent_id.is_none());
    }

    #[test]
    fn edgeless_graph_gets_a_synthesized_singleton_level_1() {
        // No edges at all ⇒ `leiden_hierarchy` returns zero levels (nothing to
        // coarsen) ⇒ `cluster_hierarchy` synthesizes one singleton cluster per
        // node so callers always have a level 1 to serve.
        let g = build_typed(&[("a", "Doc"), ("b", "Doc"), ("c", "Doc")], &[]);
        let result = cluster_hierarchy(&g, None, 1.0, 0);
        assert_eq!(result.levels.len(), 1);
        assert_eq!(result.levels[0].clusters.len(), 3);
        assert!(result.levels[0]
            .clusters
            .iter()
            .all(|c| c.node_count == 1 && c.parent_id.is_none() && c.edge_count == 0.0));
        assert_eq!(result.leaf_membership.len(), 3);
    }

    #[test]
    fn empty_graph_yields_no_levels() {
        let g = GraphCore::new().analysis_snapshot();
        let result = cluster_hierarchy(&g, None, 1.0, 0);
        assert!(result.levels.is_empty());
        assert!(result.leaf_membership.is_empty());
        assert_eq!(result.base_node_count, 0);
    }

    #[test]
    fn deterministic_across_runs() {
        let mut nodes: Vec<(&str, &str)> = Vec::new();
        let mut edges: Vec<(&str, &str)> = Vec::new();
        for c in [["a1", "a2", "a3"], ["b1", "b2", "b3"], ["c1", "c2", "c3"]] {
            for id in c {
                nodes.push((id, "Doc"));
            }
            edges.push((c[0], c[1]));
            edges.push((c[1], c[2]));
            edges.push((c[0], c[2]));
        }
        edges.push(("a3", "b1"));
        edges.push(("b3", "c1"));
        edges.push(("c3", "a1"));
        let g = build_typed(&nodes, &edges);
        let r1 = cluster_hierarchy(&g, None, 1.0, 7);
        let r2 = cluster_hierarchy(&g, None, 1.0, 7);
        let ids = |r: &ClusterHierarchyResult| -> Vec<Vec<String>> {
            r.levels
                .iter()
                .map(|l| l.clusters.iter().map(|c| c.id.clone()).collect())
                .collect()
        };
        assert_eq!(ids(&r1), ids(&r2));
        assert_eq!(r1.leaf_membership, r2.leaf_membership);
    }
}

#[cfg(test)]
mod resolve_candidates_tests {
    use super::*;
    use crate::graph::GraphCore;

    fn pe(emb: &[f64], ntype: &str) -> Vec<u8> {
        // JSON-encoded props (resolve_candidates reads embedding via serde_json,
        // matching compute_similarity_edges).
        serde_json::to_vec(&serde_json::json!({"type": ntype, "embedding": emb})).unwrap()
    }

    #[test]
    fn same_type_near_duplicates_propose_same_as() {
        let g = GraphCore::new();
        g.add_node("a".into(), pe(&[1.0, 0.0, 0.0], "Concept"));
        g.add_node("b".into(), pe(&[0.99, 0.01, 0.0], "Concept")); // ~dup of a
        g.add_node("c".into(), pe(&[0.0, 0.0, 1.0], "Concept")); // distinct
        let snap = g.analysis_snapshot();

        let proposals = resolve_candidates(&snap, 0.8, 0.95, None);
        let same_as: Vec<_> = proposals.iter().filter(|p| p.kind == "same_as").collect();
        assert_eq!(same_as.len(), 1, "a~b should form one same_as cluster");
        let members: std::collections::HashSet<&str> =
            same_as[0].members.iter().map(|s| s.as_str()).collect();
        assert!(members.contains("a") && members.contains("b"));
        assert!(!members.contains("c"), "distinct node must not be merged");
    }

    #[test]
    fn cross_type_similarity_proposes_extends_not_merge() {
        let g = GraphCore::new();
        g.add_node("base".into(), pe(&[1.0, 0.0, 0.0], "Model"));
        g.add_node("variant".into(), pe(&[0.99, 0.01, 0.0], "ModelVersion")); // similar, other type
        let snap = g.analysis_snapshot();

        let proposals = resolve_candidates(&snap, 0.8, 0.95, None);
        // cross-type high similarity → extends, never same_as
        assert!(proposals.iter().all(|p| p.kind == "extends"));
        assert_eq!(proposals.len(), 1);
        let m: std::collections::HashSet<&str> =
            proposals[0].members.iter().map(|s| s.as_str()).collect();
        assert!(m.contains("base") && m.contains("variant"));
    }

    #[test]
    fn node_type_filter_restricts_candidates() {
        let g = GraphCore::new();
        g.add_node("a".into(), pe(&[1.0, 0.0, 0.0], "Concept"));
        g.add_node("b".into(), pe(&[0.99, 0.01, 0.0], "Concept"));
        g.add_node("x".into(), pe(&[1.0, 0.0, 0.0], "Other"));
        let snap = g.analysis_snapshot();

        let proposals = resolve_candidates(&snap, 0.8, 0.95, Some("Concept"));
        // only the Concept nodes are considered
        for p in &proposals {
            for m in &p.members {
                assert!(m == "a" || m == "b", "Other-typed node must be excluded");
            }
        }
    }

    #[test]
    fn empty_when_too_few_nodes() {
        let g = GraphCore::new();
        g.add_node("only".into(), pe(&[1.0, 0.0], "Concept"));
        let snap = g.analysis_snapshot();
        assert!(resolve_candidates(&snap, 0.8, 0.95, None).is_empty());
    }
}

/// Engine follow-up B (CONCEPT:EG-KG.compute.pagerank-sparse-csr): proves the new sparse-CSR
/// `pagerank` (delegating to `graph_algos::pagerank`) is numerically equivalent
/// to the PRIOR dense, per-iteration-`HashMap` implementation it replaced — not
/// just "produces *a* score", but the same score, to floating-point tolerance.
#[cfg(test)]
mod pagerank_tests {
    use super::*;
    use crate::graph::GraphCore;

    fn p() -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": "Doc"})).unwrap()
    }

    fn build(nodes: &[&str], edges: &[(&str, &str)]) -> GraphView {
        let g = GraphCore::new();
        for n in nodes {
            g.add_node((*n).to_string(), p());
        }
        for (s, t) in edges {
            g.add_edge((*s).to_string(), (*t).to_string(), p()).unwrap();
        }
        g.topology_snapshot()
    }

    /// The EXACT prior implementation (verbatim, kept ONLY here as the oracle for
    /// this differential test) — HashMap-per-iteration, pull-from-incoming-edges.
    /// Always runs the full `iterations` count (no early-convergence exit), which
    /// is why the real `pagerank` under test is driven with a tolerance tight
    /// enough (`1e-10`) that it won't converge early either, over the fixed
    /// iteration count used below — an apples-to-apples comparison.
    fn dense_pagerank_oracle(
        core: &GraphView,
        damping: f64,
        iterations: usize,
    ) -> Vec<(String, f64)> {
        use petgraph::stable_graph::NodeIndex;
        let nodes: Vec<NodeIndex> = core.graph.node_indices().collect();
        let n = nodes.len();
        if n == 0 {
            return Vec::new();
        }
        let initial = 1.0 / n as f64;
        let mut scores: HashMap<NodeIndex, f64> = HashMap::new();
        for &node in &nodes {
            scores.insert(node, initial);
        }
        let mut out_degree: HashMap<NodeIndex, usize> = HashMap::new();
        for &node in &nodes {
            out_degree.insert(
                node,
                core.graph
                    .edges_directed(node, petgraph::Direction::Outgoing)
                    .count(),
            );
        }
        for _ in 0..iterations {
            let mut new_scores: HashMap<NodeIndex, f64> = HashMap::new();
            let teleport = (1.0 - damping) / n as f64;
            for &node in &nodes {
                let mut rank_sum = 0.0;
                for edge in core
                    .graph
                    .edges_directed(node, petgraph::Direction::Incoming)
                {
                    let src = edge.source();
                    let src_out = *out_degree.get(&src).unwrap_or(&1);
                    if src_out > 0 {
                        rank_sum += scores[&src] / src_out as f64;
                    }
                }
                new_scores.insert(node, teleport + damping * rank_sum);
            }
            scores = new_scores;
        }
        scores
            .into_iter()
            .map(|(idx, score)| (core.graph[idx].clone(), score))
            .collect()
    }

    /// A small graph with NO dangling nodes (every node has ≥1 out-edge — a
    /// directed cycle plus a chord) so the two implementations' only difference
    /// (dangling-mass redistribution) never triggers: this isolates the proof to
    /// "same core power-iteration arithmetic, same answer".
    fn no_dangling_fixture() -> GraphView {
        build(
            &["a", "b", "c", "d"],
            &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "a"), ("a", "c")],
        )
    }

    #[test]
    fn pagerank_matches_prior_dense_implementation_on_a_small_graph() {
        let g = no_dangling_fixture();
        let iterations = 50;
        let damping = 0.85;

        let oracle = dense_pagerank_oracle(&g, damping, iterations);
        let sparse = pagerank(&g, damping, iterations);

        let oracle_map: HashMap<&str, f64> = oracle.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let sparse_map: HashMap<&str, f64> = sparse.iter().map(|(k, v)| (k.as_str(), *v)).collect();

        assert_eq!(oracle_map.len(), sparse_map.len(), "same node set scored");
        for (id, oracle_score) in &oracle_map {
            let sparse_score = sparse_map
                .get(id)
                .unwrap_or_else(|| panic!("sparse pagerank missing node {id}"));
            assert!(
                (oracle_score - sparse_score).abs() < 1e-6,
                "node {id}: oracle={oracle_score} sparse={sparse_score} — must match \
                 the prior dense implementation to floating-point tolerance"
            );
        }
    }

    /// A node with zero edges (neither source nor target of any edge) must still
    /// be scored — the sparse rewrite must not silently drop isolated nodes just
    /// because they never appear in an edge list.
    #[test]
    fn pagerank_scores_isolated_nodes() {
        let g = build(&["a", "b", "isolated"], &[("a", "b")]);
        let scores = pagerank(&g, 0.85, 20);
        assert_eq!(scores.len(), 3, "isolated node must still be scored");
        let map: HashMap<&str, f64> = scores.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        assert!(map.contains_key("isolated"));
        assert!(
            map["isolated"] > 0.0,
            "isolated node still gets teleport mass"
        );
    }

    /// Mass conservation on a graph WITH a dangling node (b has no out-edges) —
    /// the one place the sparse implementation is intentionally MORE correct than
    /// the prior dense one (which leaked dangling mass instead of redistributing
    /// it). Total rank must still sum to ~1.0.
    #[test]
    fn pagerank_conserves_mass_with_a_dangling_node() {
        let g = build(&["a", "b"], &[("a", "b")]); // b is dangling (no out-edges)
        let scores = pagerank(&g, 0.85, 50);
        let total: f64 = scores.iter().map(|(_, v)| v).sum();
        assert!(
            (total - 1.0).abs() < 1e-6,
            "rank mass must be conserved at 1.0, got {total}"
        );
    }

    #[test]
    fn pagerank_empty_graph_returns_empty() {
        let g = GraphCore::new().topology_snapshot();
        assert!(pagerank(&g, 0.85, 20).is_empty());
    }
}
