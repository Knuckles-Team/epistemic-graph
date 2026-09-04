//! Lifecycle maintenance, context views, and runtime metrics.

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::graph::{GraphCore, GraphView};

/// Lifecycle-aware pruning: remove nodes past max_age or below min_score.
///
/// Examines node properties for `created_at` (epoch seconds) and `score` fields.
/// Nodes older than `max_age_secs` or with score below `min_score` are removed.
/// Collect the node ids past `max_age_secs` or below `min_score` (skipping
/// already archived/compacted nodes). Split out of `prune_by_lifecycle`
/// (extract-method, cx/wD8) — same terms, same order as before. Returns
/// (to_remove, archived_count).
/// Whether one node's decoded properties make it eligible for lifecycle
/// pruning. Split out of `collect_lifecycle_removals` (extract-method,
/// cx/wD8) — same terms, same order as before.
fn node_should_be_pruned(
    val: &serde_json::Value,
    now: u64,
    max_age_secs: u64,
    min_score: f64,
) -> bool {
    let created_at = val.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0);
    let score = val.get("score").and_then(|v| v.as_f64()).unwrap_or(1.0);
    let lifecycle = val
        .get("lifecycle_state")
        .and_then(|v| v.as_str())
        .unwrap_or("active");

    // Skip already archived/compacted
    if lifecycle == "archived" || lifecycle == "compacted" {
        return false;
    }

    let age = if created_at > 0 { now - created_at } else { 0 };
    (max_age_secs > 0 && age > max_age_secs) || score < min_score
}

fn collect_lifecycle_removals(
    core: &GraphCore,
    now: u64,
    max_age_secs: u64,
    min_score: f64,
) -> (Vec<String>, usize) {
    let mut to_remove: Vec<String> = Vec::new();
    let mut archived = 0usize;

    for entry in core.node_properties.iter() {
        let (node_id, props_json) = (entry.key(), entry.value());
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(props_json.as_slice()) {
            if node_should_be_pruned(&val, now, max_age_secs, min_score) {
                to_remove.push(node_id.clone());
                archived += 1;
            }
        }
    }
    (to_remove, archived)
}

/// Remove every node in `to_remove` from `core`, counting the edges dropped
/// along with them. Split out of `prune_by_lifecycle` (extract-method,
/// cx/wD8) — same terms, same order as before.
fn remove_pruned_nodes(core: &GraphCore, to_remove: &[String]) -> usize {
    let mut edges_removed = 0usize;
    for node_id in to_remove {
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
    edges_removed
}

pub fn prune_by_lifecycle(
    core: &GraphCore,
    max_age_secs: u64,
    min_score: f64,
) -> crate::types::PruneStats {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let (to_remove, archived) = collect_lifecycle_removals(core, now, max_age_secs, min_score);
    let nodes_removed = to_remove.len();
    let edges_removed = remove_pruned_nodes(core, &to_remove);

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
/// Build the teleport vector: seed-weighted if any seed carries positive
/// weight, else uniform. Split out of `personalized_pagerank`
/// (extract-method, cx/wD8) — same terms, same arithmetic order as before.
fn build_pagerank_teleport(
    core: &GraphView,
    seed_nodes: &[(String, f64)],
    nodes: &[NodeIndex],
    n: usize,
) -> HashMap<NodeIndex, f64> {
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
        for &node in nodes {
            teleport.insert(node, uniform);
        }
    }
    teleport
}

/// One personalized-PageRank power-iteration step. Split out of
/// `personalized_pagerank` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before, including the exact
/// `(1.0 - damping) * tp + damping * rank_sum` evaluation order.
fn pagerank_iteration(
    core: &GraphView,
    nodes: &[NodeIndex],
    scores: &HashMap<NodeIndex, f64>,
    teleport: &HashMap<NodeIndex, f64>,
    out_degree: &HashMap<NodeIndex, usize>,
    damping: f64,
) -> HashMap<NodeIndex, f64> {
    let mut new_scores: HashMap<NodeIndex, f64> = HashMap::new();

    for &node in nodes {
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
    new_scores
}

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

    let teleport = build_pagerank_teleport(core, seed_nodes, &nodes, n);

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
        scores = pagerank_iteration(core, &nodes, &scores, &teleport, &out_degree, damping);
    }

    scores
        .into_iter()
        .map(|(idx, score)| (core.graph[idx].clone(), score))
        .collect()
}
