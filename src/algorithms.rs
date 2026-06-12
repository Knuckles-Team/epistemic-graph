// CONCEPT:KG-2.16 - High-Performance Graph Algorithms
//
// PageRank, centrality, community detection, BFS/DFS traversals,
// connected components — all operating on GraphCore.

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{Bfs, EdgeRef, IntoEdgeReferences};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::graph::{GraphCore, GraphView};

/// Hard wall-clock budget for one [`community_detection`] call. Louvain is
/// pass- and level-capped, but a very large or adversarial graph can make each
/// pass expensive; this deadline guarantees the call always returns a valid
/// partition in bounded time instead of appearing to hang. (CONCEPT:KG-2.16)
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
pub fn betweenness_centrality(core: &GraphView) -> Vec<(String, f64)> {
    use rayon::prelude::*;

    let nodes: Vec<NodeIndex> = core.graph.node_indices().collect();
    let n = nodes.len();

    // One independent single-source dependency accumulation. Returns (w, delta[w])
    // for every w != source — the partial betweenness this source contributes.
    let source_contribution = |source: NodeIndex| -> Vec<(NodeIndex, f64)> {
        let mut stack = Vec::new();
        let mut predecessors: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();
        let mut sigma: HashMap<NodeIndex, f64> = HashMap::new();
        let mut dist: HashMap<NodeIndex, i64> = HashMap::new();

        for &v in &nodes {
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

        let mut delta: HashMap<NodeIndex, f64> = HashMap::new();
        for &v in &nodes {
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

/// PageRank via power iteration method.
pub fn pagerank(core: &GraphView, damping: f64, iterations: usize) -> Vec<(String, f64)> {
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

    // Pre-compute out-degree for each node
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
            // Sum contributions from all predecessors
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

// ── Component / Community Algorithms ─────────────────────────────────────

/// Weakly connected components (treats directed edges as undirected).
pub fn connected_components(core: &GraphView) -> Vec<Vec<String>> {
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut components: Vec<Vec<String>> = Vec::new();

    for &start in core.node_map.values() {
        if visited.contains(&start) {
            continue;
        }

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

        components.push(component);
    }

    components
}

/// Strongly connected components via Tarjan's algorithm.
///
/// CONCEPT:KG-2.16 — Unlike weakly connected components which treat edges as
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
/// CONCEPT:KG-2.16 — Returns the MST edges as `(source, target, weight)` tuples.
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
/// time (CONCEPT:KG-2.16).
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
        let mut remaining_vol = volume;

        if side.to_lowercase() == "buy" {
            for ask in &mut ask_book {
                let ask_price = ask.0;
                if ask_price <= price && remaining_vol > 0.0 && ask.1 > 0.0 {
                    let fill_vol = remaining_vol.min(ask.1);
                    remaining_vol -= fill_vol;
                    ask.1 -= fill_vol;

                    let mut m = HashMap::new();
                    m.insert("order_id".to_string(), order_id.clone());
                    m.insert("match_price".to_string(), ask_price.to_string());
                    m.insert("match_volume".to_string(), fill_vol.to_string());
                    matches.push(m);
                }
            }
        } else {
            for bid in &mut bid_book {
                let bid_price = bid.0;
                if bid_price >= price && remaining_vol > 0.0 && bid.1 > 0.0 {
                    let fill_vol = remaining_vol.min(bid.1);
                    remaining_vol -= fill_vol;
                    bid.1 -= fill_vol;

                    let mut m = HashMap::new();
                    m.insert("order_id".to_string(), order_id.clone());
                    m.insert("match_price".to_string(), bid_price.to_string());
                    m.insert("match_volume".to_string(), fill_vol.to_string());
                    matches.push(m);
                }
            }
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

/// Batch update: apply multiple graph operations in a single epistemic-graph crossing.
///
/// Accepts a JSON array of operations:
/// - {"op": "add_node", "id": "...", "properties": "..."}
/// - {"op": "remove_node", "id": "..."}
/// - {"op": "add_edge", "source": "...", "target": "...", "properties": "..."}
/// - {"op": "remove_edge", "source": "...", "target": "..."}
pub fn batch_update(core: &GraphCore, operations_msgpack: &[u8]) -> Result<Vec<u8>, String> {
    let ops: Vec<serde_json::Value> = rmp_serde::from_slice(operations_msgpack)
        .map_err(|e| format!("[EpistemicGraph::batch_update] invalid MsgPack: {e}"))?;

    // The whole batch runs under ONE write transaction so it is atomic w.r.t.
    // concurrent readers/writers — no other operation observes a half-applied
    // batch (CONCEPT:KG-2.16, Phase C-B).
    let mut txn = core.txn();

    let mut added_nodes = 0u32;
    let mut removed_nodes = 0u32;
    let mut added_edges = 0u32;
    let mut removed_edges = 0u32;
    let mut errors = Vec::new();

    for (i, op) in ops.iter().enumerate() {
        let op_type = op.get("op").and_then(|v| v.as_str()).unwrap_or("");
        match op_type {
            "add_node" => {
                let id = op.get("id").and_then(|v| v.as_str()).unwrap_or("");
                if !id.is_empty() {
                    // Store properties as MsgPack (NOT json.to_string().into_bytes()).
                    // GraphCore stores raw property bytes and the Python client reads
                    // them with `msgpack.unpackb` — JSON-string bytes were unreadable,
                    // so batch-written nodes looked empty/absent. Match the single
                    // `AddNode` op, which stores `properties_msgpack`.
                    let props_val = op
                        .get("properties")
                        .cloned()
                        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
                    let props_mp = rmp_serde::to_vec_named(&props_val).unwrap_or_default();
                    txn.add_node(id.to_string(), props_mp);
                    added_nodes += 1;
                }
            }
            "remove_node" => {
                let id = op.get("id").and_then(|v| v.as_str()).unwrap_or("");
                if !id.is_empty() {
                    txn.remove_node(id.to_string());
                    removed_nodes += 1;
                }
            }
            "add_edge" => {
                let src = op.get("source").and_then(|v| v.as_str()).unwrap_or("");
                let tgt = op.get("target").and_then(|v| v.as_str()).unwrap_or("");
                if !src.is_empty() && !tgt.is_empty() {
                    // MsgPack props — same read-compatibility fix as add_node above.
                    let props_val = op
                        .get("properties")
                        .cloned()
                        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
                    let props_mp = rmp_serde::to_vec_named(&props_val).unwrap_or_default();
                    if let Err(e) = txn.add_edge(src.to_string(), tgt.to_string(), props_mp) {
                        errors.push(format!("op[{i}]: {e}"));
                    } else {
                        added_edges += 1;
                    }
                }
            }
            "remove_edge" => {
                let src = op.get("source").and_then(|v| v.as_str()).unwrap_or("");
                let tgt = op.get("target").and_then(|v| v.as_str()).unwrap_or("");
                if !src.is_empty() && !tgt.is_empty() {
                    txn.remove_edge(src.to_string(), tgt.to_string());
                    removed_edges += 1;
                }
            }
            _ => {
                errors.push(format!("op[{i}]: unknown operation '{op_type}'"));
            }
        }
    }

    let result = serde_json::json!({
        "added_nodes": added_nodes,
        "removed_nodes": removed_nodes,
        "added_edges": added_edges,
        "removed_edges": removed_edges,
        "errors": errors,
    });
    rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())
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
            {"op": "add_edge", "source": "code:A", "target": "code:B", "properties": {"rel_type": "CALLS"}},
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
