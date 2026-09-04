//! Graph traversal, centrality, connectivity, and coloring algorithms.

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{Bfs, EdgeRef, IntoEdgeReferences};
use std::collections::{HashMap, HashSet, VecDeque};

use crate::graph::GraphView;

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
/// (visit-order stack, predecessor DAG, shortest-path counts `sigma`) — the
/// result of one BFS shortest-path count pass. The alias keeps the helper's
/// structured result readable at its call sites (cx/wD8).
type BfsShortestPathCounts = (
    Vec<NodeIndex>,
    HashMap<NodeIndex, Vec<NodeIndex>>,
    HashMap<NodeIndex, f64>,
);

/// Single-source BFS shortest-path counting (the forward pass of Brandes'
/// algorithm). Split out of `betweenness_centrality`'s `source_contribution`
/// closure (extract-method, cx/wD8) — same terms, same arithmetic order as
/// before. Returns the visit order stack, the predecessor DAG, and the
/// shortest-path counts `sigma`.
fn bfs_shortest_path_counts(
    core: &GraphView,
    nodes: &[NodeIndex],
    source: NodeIndex,
) -> BfsShortestPathCounts {
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
            (&mut queue, &mut dist, &mut sigma, &mut predecessors),
        );
    }
    (stack, predecessors, sigma)
}

/// Relax `v`'s out-neighbors for one BFS-frontier step. Split out of
/// `bfs_shortest_path_counts` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before.
type BfsRelaxState<'a> = (
    &'a mut VecDeque<NodeIndex>,
    &'a mut HashMap<NodeIndex, i64>,
    &'a mut HashMap<NodeIndex, f64>,
    &'a mut HashMap<NodeIndex, Vec<NodeIndex>>,
);

fn bfs_relax_neighbors(core: &GraphView, v: NodeIndex, v_dist: i64, state: BfsRelaxState<'_>) {
    let (queue, dist, sigma, predecessors) = state;
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
    let source_contribution = |source: NodeIndex| -> Vec<(NodeIndex, f64)> {
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
        components.push(collect_weakly_connected_component(
            core,
            start,
            &mut visited,
        ));
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
