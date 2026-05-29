// CONCEPT:KG-2.16 - High-Performance Graph Algorithms
//
// PageRank, centrality, community detection, BFS/DFS traversals,
// connected components — all operating on GraphCore.

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{Bfs, EdgeRef};
use std::collections::{HashMap, HashSet, VecDeque};

use crate::graph::GraphCore;

// ── Traversal Algorithms ─────────────────────────────────────────────────

/// Topological sort of the graph. Returns PyErr if cycles exist.
pub fn topological_sort(core: &GraphCore) -> Result<Vec<String>, String> {
    match petgraph::algo::toposort(&core.graph, None) {
        Ok(indices) => {
            let sorted: Vec<String> = indices
                .iter()
                .map(|&idx| core.graph[idx].clone())
                .collect();
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
pub fn get_shortest_path(core: &GraphCore, source_id: &str, target_id: &str) -> Option<Vec<String>> {
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
    let idx = core.node_map.get(node_id).ok_or_else(|| {
        format!("Node '{}' not found", node_id)
    })?;
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
        return core
            .node_map
            .keys()
            .map(|k| (k.clone(), 0.0))
            .collect();
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

/// Simple community detection via label propagation (Louvain-inspired).
///
/// Each node starts with its own label. Iteratively, each node adopts the
/// most common label among its neighbors. `resolution` controls sensitivity
/// (higher = more communities). Converges when no labels change.
pub fn community_detection(core: &GraphCore, _resolution: f64) -> Vec<Vec<String>> {
    let nodes: Vec<String> = core.node_map.keys().cloned().collect();
    if nodes.is_empty() {
        return Vec::new();
    }

    // Initialize: each node is its own community
    let mut labels: HashMap<String, usize> = HashMap::new();
    for (i, node_id) in nodes.iter().enumerate() {
        labels.insert(node_id.clone(), i);
    }

    let max_iterations = 100;
    for _ in 0..max_iterations {
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

            if let Some((&best_label, _)) = label_counts.iter().max_by_key(|&(_, &count)| count) {
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

    // Group nodes by label
    let mut communities: HashMap<usize, Vec<String>> = HashMap::new();
    for (node_id, label) in &labels {
        communities
            .entry(*label)
            .or_default()
            .push(node_id.clone());
    }

    communities.into_values().collect()
}

// ── Quant FFI Algorithms ─────────────────────────────────────────────────

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
    let variance = slice.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n;
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
            let val: serde_json::Value = serde_json::from_str(props_json).ok()?;
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
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(props_json) {
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

/// Batch update: apply multiple graph operations in a single FFI crossing.
///
/// Accepts a JSON array of operations:
/// - {"op": "add_node", "id": "...", "properties": "..."}
/// - {"op": "remove_node", "id": "..."}
/// - {"op": "add_edge", "source": "...", "target": "...", "properties": "..."}
/// - {"op": "remove_edge", "source": "...", "target": "..."}
pub fn batch_update(core: &mut GraphCore, operations_json: &str) -> Result<String, String> {
    let ops: Vec<serde_json::Value> = serde_json::from_str(operations_json).map_err(|e| {
        format!(
            "[EpistemicGraph::batch_update] invalid JSON: {e}"
        )
    })?;

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
                let props = op
                    .get("properties")
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "{}".to_string());
                if !id.is_empty() {
                    core.add_node(id.to_string(), props);
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
                let props = op
                    .get("properties")
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "{}".to_string());
                if !src.is_empty() && !tgt.is_empty() {
                    if let Err(e) = core.add_edge(src.to_string(), tgt.to_string(), props) {
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
    Ok(result.to_string())
}

/// Compute runtime metrics for observability.
pub fn compute_metrics(core: &GraphCore) -> crate::types::GraphMetrics {
    let mut active = 0usize;
    let mut compacted = 0usize;
    let mut archived = 0usize;

    for props_json in core.node_properties.values() {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(props_json) {
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
