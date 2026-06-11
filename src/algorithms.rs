// CONCEPT:KG-2.16 - High-Performance Graph Algorithms
//
// PageRank, centrality, community detection, BFS/DFS traversals,
// connected components — all operating on GraphCore.

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{Bfs, EdgeRef, IntoEdgeReferences};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::graph::GraphCore;

/// Hard wall-clock budget for one [`community_detection`] call. Label
/// propagation is already iteration-capped, but a very large or adversarial
/// call graph can make each pass expensive; this deadline guarantees the call
/// always returns a valid partition in bounded time instead of appearing to
/// hang. (CONCEPT:KG-2.16)
const COMMUNITY_DETECTION_BUDGET: Duration = Duration::from_secs(15);

// ── Traversal Algorithms ─────────────────────────────────────────────────

/// Topological sort of the graph. Returns PyErr if cycles exist.
pub fn topological_sort(core: &GraphCore) -> Result<Vec<String>, String> {
    match petgraph::algo::toposort(&core.graph, None) {
        Ok(indices) => {
            let sorted: Vec<String> = indices.iter().map(|&idx| core.graph[idx].clone()).collect();
            Ok(sorted)
        }
        Err(_) => Err("Graph contains cycles".to_string()),
    }
}

/// Detect a cycle via DFS coloring. Returns the cycle path if found.
pub fn find_cycle(core: &GraphCore) -> Option<Vec<String>> {
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
    core: &GraphCore,
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
    core: &GraphCore,
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
pub fn get_blast_radius(core: &GraphCore, node_id: &str, max_depth: usize) -> Vec<String> {
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
pub fn compute_degree_centrality(core: &GraphCore, node_id: &str) -> Result<f64, String> {
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
pub fn degree_centrality_all(core: &GraphCore) -> Vec<(String, f64)> {
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
pub fn betweenness_centrality(core: &GraphCore) -> Vec<(String, f64)> {
    let nodes: Vec<NodeIndex> = core.graph.node_indices().collect();
    let n = nodes.len();
    let mut centrality: HashMap<NodeIndex, f64> = HashMap::new();
    for &node in &nodes {
        centrality.insert(node, 0.0);
    }

    for &source in &nodes {
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

        while let Some(w) = stack.pop() {
            if sigma[&w] > 0.0 {
                for &v in &predecessors[&w] {
                    let d = (sigma[&v] / sigma[&w]) * (1.0 + delta[&w]);
                    if let Some(dv) = delta.get_mut(&v) {
                        *dv += d;
                    }
                }
            }
            if w != source {
                if let Some(c) = centrality.get_mut(&w) {
                    *c += delta[&w];
                }
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
pub fn pagerank(core: &GraphCore, damping: f64, iterations: usize) -> Vec<(String, f64)> {
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
pub fn connected_components(core: &GraphCore) -> Vec<Vec<String>> {
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
pub fn strongly_connected_components(core: &GraphCore) -> Vec<Vec<String>> {
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
pub fn minimum_spanning_tree(core: &GraphCore) -> Vec<(String, String, f64)> {
    use petgraph::data::FromElements;
    use petgraph::stable_graph::StableGraph;

    // Build a weighted undirected graph for MST computation
    let mut undirected: petgraph::Graph<String, f64, petgraph::Undirected> =
        petgraph::Graph::new_undirected();

    // Map node indices from core to undirected graph
    let mut idx_map: HashMap<NodeIndex, petgraph::graph::NodeIndex> = HashMap::new();
    for (&ref _id, &idx) in &core.node_map {
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

/// Simple community detection via label propagation (Louvain-inspired).
///
/// Each node starts with its own label. Iteratively, each node adopts the
/// most common label among its neighbors. `resolution` controls sensitivity
/// (higher = more communities). Converges when no labels change.
pub fn community_detection(core: &GraphCore, _resolution: f64) -> Vec<Vec<String>> {
    // Deterministic node order. The previous version iterated `node_map`'s
    // HashMap keys in arbitrary order and broke label ties via `max_by_key`
    // (also order-dependent), so the algorithm could *oscillate* between
    // equivalent labelings and never reach the `changed == false` early-exit —
    // burning all 100 iterations every run and producing a different result
    // each time. Sorting both the node sweep and the tie-break makes
    // propagation stable (reproducible) and lets it converge early.
    let mut nodes: Vec<String> = core.node_map.keys().cloned().collect();
    if nodes.is_empty() {
        return Vec::new();
    }
    nodes.sort_unstable();

    // Initialize: each node is its own community
    let mut labels: HashMap<String, usize> = HashMap::new();
    for (i, node_id) in nodes.iter().enumerate() {
        labels.insert(node_id.clone(), i);
    }

    // Bounded by BOTH an iteration cap and a wall-clock deadline, so a large or
    // adversarial graph can never make this call hang (CONCEPT:KG-2.16).
    let max_iterations = 100;
    let deadline = Instant::now() + COMMUNITY_DETECTION_BUDGET;
    for _ in 0..max_iterations {
        if Instant::now() >= deadline {
            break;
        }
        let mut changed = false;

        for node_id in &nodes {
            let idx = match core.node_map.get(node_id) {
                Some(&i) => i,
                None => continue,
            };

            // Count neighbor labels
            let mut label_counts: HashMap<usize, usize> = HashMap::new();
            for edge in core
                .graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
            {
                let neighbor_id = &core.graph[edge.target()];
                if let Some(&lbl) = labels.get(neighbor_id) {
                    *label_counts.entry(lbl).or_insert(0) += 1;
                }
            }
            for edge in core
                .graph
                .edges_directed(idx, petgraph::Direction::Incoming)
            {
                let neighbor_id = &core.graph[edge.source()];
                if let Some(&lbl) = labels.get(neighbor_id) {
                    *label_counts.entry(lbl).or_insert(0) += 1;
                }
            }

            // Most common neighbor label; break ties deterministically toward
            // the SMALLEST label so propagation is stable and convergent.
            let best = label_counts
                .iter()
                .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
                .map(|(&lbl, _)| lbl);
            if let Some(best_label) = best {
                let current = labels[node_id];
                if best_label != current {
                    labels.insert(node_id.clone(), best_label);
                    changed = true;
                }
            }
        }

        if !changed {
            break;
        }
    }

    // Group nodes by label, returning a deterministic order (communities sorted
    // by their smallest member; members sorted) so callers + tests are stable.
    let mut communities: HashMap<usize, Vec<String>> = HashMap::new();
    for (node_id, label) in &labels {
        communities.entry(*label).or_default().push(node_id.clone());
    }
    let mut out: Vec<Vec<String>> = communities
        .into_values()
        .map(|mut members| {
            members.sort_unstable();
            members
        })
        .collect();
    out.sort_unstable_by(|a, b| a[0].cmp(&b[0]));
    out
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
pub fn graph_coloring(core: &GraphCore) -> Vec<(String, usize)> {
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
pub fn compute_similarity_edges(core: &GraphCore, threshold: f64) -> Vec<(String, String, f64)> {
    use rayon::prelude::*;

    // Extract nodes with embeddings
    let nodes_with_emb: Vec<(String, Vec<f64>)> = core
        .node_properties
        .iter()
        .filter_map(|(node_id, props_json)| {
            let val: serde_json::Value = serde_json::from_slice(&props_json).ok()?;
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
    core: &mut GraphCore,
    max_age_secs: u64,
    min_score: f64,
) -> crate::types::PruneStats {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut to_remove: Vec<String> = Vec::new();
    let mut archived = 0usize;

    for (node_id, props_json) in &core.node_properties {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&props_json) {
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
            .keys()
            .filter(|(src, tgt)| src == node_id || tgt == node_id)
            .cloned()
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
    core: &GraphCore,
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
                .cloned()
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
pub fn batch_update(core: &mut GraphCore, operations_msgpack: &[u8]) -> Result<Vec<u8>, String> {
    let ops: Vec<serde_json::Value> = rmp_serde::from_slice(operations_msgpack)
        .map_err(|e| format!("[EpistemicGraph::batch_update] invalid MsgPack: {e}"))?;

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
                    core.add_node(id.to_string(), props_mp);
                    added_nodes += 1;
                }
            }
            "remove_node" => {
                let id = op.get("id").and_then(|v| v.as_str()).unwrap_or("");
                if !id.is_empty() {
                    core.remove_node(id.to_string());
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
                    if let Err(e) = core.add_edge(src.to_string(), tgt.to_string(), props_mp) {
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
                    core.remove_edge(src.to_string(), tgt.to_string());
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
pub fn compute_metrics(core: &GraphCore) -> crate::types::GraphMetrics {
    let mut active = 0usize;
    let mut compacted = 0usize;
    let mut archived = 0usize;

    for props_json in core.node_properties.values() {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&props_json) {
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
        total_mutations: core.ledger.len() as u64,
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
    core: &GraphCore,
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

    fn build(nodes: &[&str], edges: &[(&str, &str)]) -> GraphCore {
        let mut g = GraphCore::new();
        for n in nodes {
            g.add_node((*n).to_string(), p());
        }
        for (s, t) in edges {
            g.add_edge((*s).to_string(), (*t).to_string(), p()).unwrap();
        }
        g
    }

    #[test]
    fn empty_graph_returns_empty() {
        let g = GraphCore::new();
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
        let mut g = GraphCore::new();
        for id in &ids {
            g.add_node(id.clone(), p());
        }
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                g.add_edge(ids[i].clone(), ids[j].clone(), p()).unwrap();
            }
        }
        let start = Instant::now();
        let communities = community_detection(&g, 1.0);
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
        let mut g = GraphCore::new();
        let ops = serde_json::json!([
            {"op": "add_node", "id": "code:A", "properties": {"type": "Code", "language": "java", "name": "Widget"}},
            {"op": "add_node", "id": "code:B", "properties": {"type": "Code", "language": "rust"}},
            {"op": "add_edge", "source": "code:A", "target": "code:B", "properties": {"rel_type": "CALLS"}},
        ]);
        let ops_mp = rmp_serde::to_vec_named(&ops).unwrap();
        let res_mp = batch_update(&mut g, &ops_mp).unwrap();
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
}
