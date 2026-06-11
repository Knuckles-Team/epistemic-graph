// CONCEPT:KG-2.16 - Core Graph Storage Module
//
// Core petgraph DiGraph CRUD operations, node/edge storage,
// serialization, ledger, and repository parsing.

use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;
use std::collections::HashMap;
use std::io::Read;

/// Core graph storage — encapsulates petgraph and all CRUD operations.
///
/// This struct is NOT exposed to Python directly. `EpistemicGraph` in `lib.rs`
/// wraps it and delegates through ``.
#[derive(Debug)]
pub struct GraphCore {
    pub graph: StableDiGraph<String, String>,
    pub node_map: HashMap<String, NodeIndex>,
    pub node_properties: HashMap<String, Vec<u8>>,
    pub edge_properties: HashMap<(String, String), Vec<Vec<u8>>>,
    pub ledger: Vec<String>,
    pub semantic_store: crate::compute::semantic::SemanticStore,
}

impl GraphCore {
    pub fn new() -> Self {
        GraphCore {
            graph: StableDiGraph::new(),
            node_map: HashMap::new(),
            node_properties: HashMap::new(),
            edge_properties: HashMap::new(),
            ledger: Vec::new(),
            semantic_store: crate::compute::semantic::SemanticStore::new(),
        }
    }

    // ── Node CRUD ────────────────────────────────────────────────────────

    pub fn add_node(&mut self, node_id: String, properties_msgpack: Vec<u8>) {
        let _idx = if let Some(&existing_idx) = self.node_map.get(&node_id) {
            existing_idx
        } else {
            let new_idx = self.graph.add_node(node_id.clone());
            self.node_map.insert(node_id.clone(), new_idx);
            new_idx
        };
        self.node_properties
            .insert(node_id.clone(), properties_msgpack.clone());
        let log = format!("ADD_NODE|{}|{}", node_id, hex::encode(&properties_msgpack));
        self.ledger.push(log);
        if self.ledger.len() > 100_000 {
            self.ledger.drain(0..50_000);
        }
    }

    pub fn remove_node(&mut self, node_id: String) {
        if let Some(idx) = self.node_map.remove(&node_id) {
            self.graph.remove_node(idx);
            self.node_properties.remove(&node_id);
            self.edge_properties
                .retain(|(src, tgt), _| src != &node_id && tgt != &node_id);
            let log = format!("REMOVE_NODE|{}", node_id);
            self.ledger.push(log);
            if self.ledger.len() > 100_000 {
                self.ledger.drain(0..50_000);
            }
        }
    }

    pub fn has_node(&self, node_id: &str) -> bool {
        self.node_map.contains_key(node_id)
    }

    pub fn get_nodes(&self) -> Vec<(String, Vec<u8>)> {
        self.node_properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn get_node_properties(&self, node_id: &str) -> Option<Vec<u8>> {
        self.node_properties.get(node_id).cloned()
    }

    pub fn node_count(&self) -> usize {
        self.node_map.len()
    }

    /// Return all node IDs without properties (lightweight enumeration).
    pub fn node_ids(&self) -> Vec<String> {
        self.node_map.keys().cloned().collect()
    }

    // ── Edge CRUD ────────────────────────────────────────────────────────

    pub fn add_edge(
        &mut self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        let source_idx = match self.node_map.get(&source_id) {
            Some(&idx) => idx,
            None => return Err(format!("Source node '{}' not found", source_id)),
        };
        let target_idx = match self.node_map.get(&target_id) {
            Some(&idx) => idx,
            None => return Err(format!("Target node '{}' not found", target_id)),
        };

        self.graph.add_edge(
            source_idx,
            target_idx,
            format!("{}:{}", source_id, target_id),
        );
        self.edge_properties
            .entry((source_id.clone(), target_id.clone()))
            .or_default()
            .push(properties_msgpack.clone());
        let log = format!(
            "ADD_EDGE|{}|{}|{}",
            source_id,
            target_id,
            hex::encode(&properties_msgpack)
        );
        self.ledger.push(log);
        if self.ledger.len() > 100_000 {
            self.ledger.drain(0..50_000);
        }
        Ok(())
    }

    pub fn remove_edge(&mut self, source_id: String, target_id: String) {
        if let (Some(&src_idx), Some(&tgt_idx)) =
            (self.node_map.get(&source_id), self.node_map.get(&target_id))
        {
            if let Some(edge_idx) = self.graph.find_edge(src_idx, tgt_idx) {
                self.graph.remove_edge(edge_idx);
            }
            self.edge_properties
                .remove(&(source_id.clone(), target_id.clone()));
            let log = format!("REMOVE_EDGE|{}|{}", source_id, target_id);
            self.ledger.push(log);
            if self.ledger.len() > 100_000 {
                self.ledger.drain(0..50_000);
            }
        }
    }

    pub fn has_edge(&self, source_id: &str, target_id: &str) -> bool {
        if let (Some(&src_idx), Some(&tgt_idx)) =
            (self.node_map.get(source_id), self.node_map.get(target_id))
        {
            self.graph.find_edge(src_idx, tgt_idx).is_some()
        } else {
            false
        }
    }

    pub fn get_edges(&self) -> Vec<(String, String, Vec<u8>)> {
        let mut res = Vec::new();
        for ((src, tgt), props_list) in &self.edge_properties {
            for props in props_list {
                res.push((src.clone(), tgt.clone(), props.clone()));
            }
        }
        res
    }

    pub fn get_edge_properties(&self, source_id: &str, target_id: &str) -> Vec<Vec<u8>> {
        self.edge_properties
            .get(&(source_id.to_string(), target_id.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    pub fn edge_count(&self) -> usize {
        self.edge_properties.values().map(|v| v.len()).sum()
    }

    /// In-degree count for a specific node.
    pub fn in_degree(&self, node_id: &str) -> Result<usize, String> {
        let idx = self
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        Ok(self
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
            .count())
    }

    /// Out-degree count for a specific node.
    pub fn out_degree(&self, node_id: &str) -> Result<usize, String> {
        let idx = self
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        Ok(self
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
            .count())
    }

    // ── Neighbor Queries ─────────────────────────────────────────────────

    /// Incoming neighbors (predecessors).
    pub fn get_predecessors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let idx = self
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        let preds: Vec<String> = self
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
            .map(|e| self.graph[e.source()].clone())
            .collect();
        Ok(preds)
    }

    /// Outgoing neighbors (successors).
    pub fn get_successors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let idx = self
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        let succs: Vec<String> = self
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
            .map(|e| self.graph[e.target()].clone())
            .collect();
        Ok(succs)
    }

    /// All neighbors (both directions, deduplicated).
    pub fn get_neighbors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let idx = self
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        let mut neighbors = std::collections::HashSet::new();
        for e in self
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
        {
            neighbors.insert(self.graph[e.source()].clone());
        }
        for e in self
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
        {
            neighbors.insert(self.graph[e.target()].clone());
        }
        Ok(neighbors.into_iter().collect())
    }

    // ── Serialization ────────────────────────────────────────────────────

    pub fn to_msgpack(&self) -> Result<Vec<u8>, String> {
        let mut graph_map = HashMap::new();
        let nodes = self.get_nodes();
        let edges = self.get_edges();
        graph_map.insert(
            "nodes".to_string(),
            serde_json::to_value(nodes).map_err(|e| e.to_string())?,
        );
        graph_map.insert(
            "edges".to_string(),
            serde_json::to_value(edges).map_err(|e| e.to_string())?,
        );
        graph_map.insert(
            "ledger".to_string(),
            serde_json::to_value(&self.ledger).map_err(|e| e.to_string())?,
        );
        graph_map.insert(
            "semantic_store".to_string(),
            serde_json::to_value(&self.semantic_store).map_err(|e| e.to_string())?,
        );

        rmp_serde::to_vec_named(&graph_map).map_err(|e| e.to_string())
    }

    pub fn clear(&mut self) {
        self.graph.clear();
        self.node_map.clear();
        self.node_properties.clear();
        self.edge_properties.clear();
        self.ledger.clear();
        self.semantic_store = crate::compute::semantic::SemanticStore::new();
    }

    pub fn from_msgpack(&mut self, msgpack: &[u8]) -> Result<(), String> {
        let graph_map: HashMap<String, serde_json::Value> =
            rmp_serde::from_slice(msgpack).map_err(|e| e.to_string())?;

        // Reset state
        self.graph.clear();
        self.node_map.clear();
        self.node_properties.clear();
        self.edge_properties.clear();
        self.ledger.clear();

        if let Some(nodes_val) = graph_map.get("nodes") {
            let nodes: Vec<(String, Vec<u8>)> =
                serde_json::from_value(nodes_val.clone()).map_err(|e| e.to_string())?;
            for (node_id, props) in nodes {
                self.add_node(node_id, props);
            }
        }

        if let Some(edges_val) = graph_map.get("edges") {
            let edges: Vec<(String, String, Vec<u8>)> =
                serde_json::from_value(edges_val.clone()).map_err(|e| e.to_string())?;
            for (src, tgt, props) in edges {
                let _ = self.add_edge(src, tgt, props);
            }
        }

        if let Some(ledger_val) = graph_map.get("ledger") {
            let ledger: Vec<String> =
                serde_json::from_value(ledger_val.clone()).map_err(|e| e.to_string())?;
            self.ledger = ledger;
        }

        if let Some(store_val) = graph_map.get("semantic_store") {
            let store: crate::compute::semantic::SemanticStore =
                serde_json::from_value(store_val.clone()).map_err(|e| e.to_string())?;
            self.semantic_store = store;
        }

        Ok(())
    }

    // ── Ledger Operations ────────────────────────────────────────────────

    pub fn get_ledger(&self) -> Vec<String> {
        self.ledger.clone()
    }

    pub fn clear_ledger(&mut self) {
        self.ledger.clear();
    }

    pub fn apply_ledger(&mut self, transactions: Vec<String>) -> Result<(), String> {
        for tx in transactions {
            let parts: Vec<&str> = tx.split('|').collect();
            if parts.is_empty() {
                continue;
            }
            match parts[0] {
                "ADD_NODE" if parts.len() >= 3 => {
                    self.add_node(parts[1].to_string(), parts[2].as_bytes().to_vec());
                }
                "ADD_EDGE" if parts.len() >= 4 => {
                    let _ = self.add_edge(
                        parts[1].to_string(),
                        parts[2].to_string(),
                        parts[3].as_bytes().to_vec(),
                    );
                }
                "REMOVE_NODE" if parts.len() >= 2 => {
                    self.remove_node(parts[1].to_string());
                }
                "REMOVE_EDGE" if parts.len() >= 3 => {
                    self.remove_edge(parts[1].to_string(), parts[2].to_string());
                }
                _ => {}
            }
        }
        Ok(())
    }

    // ── Subgraph Extraction ──────────────────────────────────────────────

    /// Extract a subgraph containing only the specified node IDs.
    pub fn get_subgraph(&self, node_ids: &[String]) -> GraphCore {
        let mut sub = GraphCore::new();
        let id_set: std::collections::HashSet<&String> = node_ids.iter().collect();

        // Copy matching nodes
        for nid in node_ids {
            if let Some(props) = self.node_properties.get(nid) {
                sub.add_node(nid.clone(), props.clone());
            }
        }

        // Copy edges where both endpoints are in the subgraph
        for ((src, tgt), props_list) in &self.edge_properties {
            if id_set.contains(src) && id_set.contains(tgt) {
                for props in props_list {
                    let _ = sub.add_edge(src.clone(), tgt.clone(), props.clone());
                }
            }
        }

        sub
    }

    // ── Read-Only Compute Snapshots (CONCEPT:KG-2.51) ────────────────────
    // CPU-heavy read-only algorithms must not run while holding the per-graph
    // RwLock — they would starve every writer on that graph for the whole
    // computation. These snapshots take a cheap O(V+E) structural memcpy under
    // the read lock so the algorithm can run on the blocking pool with the
    // lock already released. The ledger and embedding store are never copied —
    // the graph algorithms do not read them.

    /// Topology-only snapshot: petgraph structure + id↔index map. For
    /// algorithms that read only the graph shape (PageRank, betweenness
    /// centrality, community detection, graph coloring, …).
    pub fn topology_snapshot(&self) -> GraphCore {
        GraphCore {
            graph: self.graph.clone(),
            node_map: self.node_map.clone(),
            node_properties: HashMap::new(),
            edge_properties: HashMap::new(),
            ledger: Vec::new(),
            semantic_store: crate::compute::semantic::SemanticStore::new(),
        }
    }

    /// Topology + property-blob snapshot (still no ledger / embedding store).
    /// For algorithms that also read node/edge property blobs: MST edge
    /// weights, VF2 matching, similarity edges, lifecycle metrics.
    pub fn analysis_snapshot(&self) -> GraphCore {
        GraphCore {
            graph: self.graph.clone(),
            node_map: self.node_map.clone(),
            node_properties: self.node_properties.clone(),
            edge_properties: self.edge_properties.clone(),
            ledger: Vec::new(),
            semantic_store: crate::compute::semantic::SemanticStore::new(),
        }
    }

    // ── Graph Forking ────────────────────────────────────────────────────

    pub fn fork(&self) -> GraphCore {
        GraphCore {
            graph: self.graph.clone(),
            node_map: self.node_map.clone(),
            node_properties: self.node_properties.clone(),
            edge_properties: self.edge_properties.clone(),
            ledger: self.ledger.clone(),
            semantic_store: self.semantic_store.clone(),
        }
    }

    pub fn diff_against(&self, other: &GraphCore) -> String {
        let self_nodes: std::collections::HashSet<&String> = self.node_map.keys().collect();
        let other_nodes: std::collections::HashSet<&String> = other.node_map.keys().collect();

        let added: Vec<&String> = other_nodes.difference(&self_nodes).cloned().collect();
        let removed: Vec<&String> = self_nodes.difference(&other_nodes).cloned().collect();

        let mut modified: Vec<&String> = Vec::new();
        for node_id in self_nodes.intersection(&other_nodes) {
            let self_props = self.node_properties.get(*node_id);
            let other_props = other.node_properties.get(*node_id);
            if self_props != other_props {
                modified.push(node_id);
            }
        }

        let self_edges: std::collections::HashSet<&(String, String)> =
            self.edge_properties.keys().collect();
        let other_edges: std::collections::HashSet<&(String, String)> =
            other.edge_properties.keys().collect();
        let edges_added: Vec<&(String, String)> =
            other_edges.difference(&self_edges).cloned().collect();
        let edges_removed: Vec<&(String, String)> =
            self_edges.difference(&other_edges).cloned().collect();

        let diff = serde_json::json!({
            "nodes_added": added,
            "nodes_removed": removed,
            "nodes_modified": modified,
            "edges_added": edges_added,
            "edges_removed": edges_removed,
        });
        diff.to_string()
    }

    // ── Compaction ───────────────────────────────────────────────────────

    pub fn compact_nodes_by_type(&mut self, node_type: &str, threshold: usize) -> Vec<String> {
        let mut candidates: Vec<String> = Vec::new();
        for (node_id, props_json) in &self.node_properties {
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(props_json) {
                if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
                    if t == node_type {
                        candidates.push(node_id.clone());
                    }
                }
            }
        }

        if candidates.len() <= threshold {
            return Vec::new();
        }

        let summary_id = format!("summary:{}:{}", node_type, candidates.len());
        let summary_props = serde_json::json!({
            "type": format!("{}_summary", node_type),
            "compacted_count": candidates.len(),
            "original_type": node_type,
        });
        self.add_node(summary_id.clone(), summary_props.to_string().into_bytes());

        let mut removed = Vec::new();
        for node_id in &candidates {
            self.remove_node(node_id.clone());
            removed.push(node_id.clone());
        }
        removed
    }

    // ── Repository Parsing ───────────────────────────────────────────────

    pub fn parse_repository(&mut self, root_path: &str) -> Result<(), String> {
        let root = std::path::Path::new(root_path);
        if !root.exists() {
            return Err(format!("Path '{}' does not exist", root_path));
        }
        let mut files = Vec::new();
        walk_dir_recursive(root, &mut files);

        for path in files {
            if let Ok(relative) = path.strip_prefix(root) {
                let rel_str = relative.to_string_lossy().to_string();

                let file_props = format!("{{\"type\": \"file\", \"path\": \"{}\"}}", rel_str);
                self.add_node(rel_str.clone(), file_props.into_bytes());

                if let Ok(mut file) = std::fs::File::open(&path) {
                    let mut content = String::new();
                    if file.read_to_string(&mut content).is_ok() {
                        let lines: Vec<&str> = content.lines().collect();
                        for (idx, line) in lines.iter().enumerate() {
                            let trimmed = line.trim();
                            self.parse_code_line(trimmed, &rel_str, idx + 1);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn parse_code_line(&mut self, trimmed: &str, rel_str: &str, line_num: usize) {
        // Python/JS class definition
        if trimmed.starts_with("class ") {
            if let Some(class_name) = trimmed.split_whitespace().nth(1) {
                let clean_name = class_name
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !clean_name.is_empty() {
                    let node_id = format!("{}::{}", rel_str, clean_name);
                    let props = format!(
                        "{{\"type\": \"class\", \"file\": \"{}\", \"line\": {}}}",
                        rel_str, line_num
                    );
                    self.add_node(node_id.clone(), props.into_bytes());
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props.into_bytes());
                }
            }
        }

        // Python function definition
        if trimmed.starts_with("def ") {
            if let Some(func_name) = trimmed.split_whitespace().nth(1) {
                let clean_name = func_name
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !clean_name.is_empty() {
                    let node_id = format!("{}::{}", rel_str, clean_name);
                    let props = format!(
                        "{{\"type\": \"function\", \"file\": \"{}\", \"line\": {}}}",
                        rel_str, line_num
                    );
                    self.add_node(node_id.clone(), props.into_bytes());
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props.into_bytes());
                }
            }
        }

        // JavaScript/TypeScript function
        if trimmed.starts_with("function ") {
            if let Some(func_name) = trimmed.split_whitespace().nth(1) {
                let clean_name = func_name.split('(').next().unwrap_or("").trim();
                if !clean_name.is_empty() {
                    let node_id = format!("{}::{}", rel_str, clean_name);
                    let props = format!(
                        "{{\"type\": \"function\", \"file\": \"{}\", \"line\": {}}}",
                        rel_str, line_num
                    );
                    self.add_node(node_id.clone(), props.into_bytes());
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props.into_bytes());
                }
            }
        }
    }

    // ── VF2 Subgraph Matching ────────────────────────────────────────────

    pub fn vf2_subgraph_match(&self, pattern: &GraphCore) -> Vec<HashMap<String, String>> {
        let mut matches = Vec::new();
        let pattern_nodes: Vec<String> = pattern.node_map.keys().cloned().collect();
        if pattern_nodes.is_empty() {
            return matches;
        }
        let mut current_mapping = HashMap::new();
        let mut mapped_targets = std::collections::HashSet::new();

        backtrack_match(
            self,
            0,
            &pattern_nodes,
            &mut current_mapping,
            &mut mapped_targets,
            pattern,
            &mut matches,
        );
        matches
    }

    /// Evict nodes down to `max_nodes` by removing the least-recently-added.
    ///
    /// CONCEPT:KG-2.16 — Memory pressure defense. When the in-memory graph
    /// grows beyond `max_nodes`, this method removes the oldest nodes (by
    /// insertion order in `node_map`) until the count is at or below the cap.
    /// Returns the number of evicted nodes.
    pub fn evict_lru(&mut self, max_nodes: usize) -> usize {
        let current = self.node_map.len();
        if current <= max_nodes {
            return 0;
        }
        let to_evict = current - max_nodes;

        // Collect node IDs to evict — nodes with the lowest NodeIndex values
        // were inserted earliest, so they approximate LRU.
        let mut indexed: Vec<(String, NodeIndex)> =
            self.node_map.iter().map(|(k, &v)| (k.clone(), v)).collect();
        indexed.sort_by_key(|(_, idx)| *idx);

        let evict_ids: Vec<String> = indexed
            .into_iter()
            .take(to_evict)
            .map(|(id, _)| id)
            .collect();

        for node_id in &evict_ids {
            self.remove_node(node_id.clone());
        }

        evict_ids.len()
    }

    // ── Ebbinghaus Temporal Decay (CONCEPT:KG-2.16) ──────────────────────

    /// Apply an Ebbinghaus forgetting-curve decay to every node's and edge's
    /// belief `confidence`, then optionally prune anything below `floor`.
    ///
    /// Retention follows `R = 0.5^(Δt / half_life)` where `Δt` is the seconds
    /// elapsed since the item's `last_access` (falling back to `updated_at` →
    /// `created_at` → `now`, so a freshly-stamped item never decays on its first
    /// sweep). The decayed confidence is persisted and `last_access` advanced to
    /// `now`, so repeated sweeps compound exactly: `R(Δt₁)·R(Δt₂) = R(Δt₁+Δt₂)`.
    /// A per-item `half_life` property overrides `default_half_life` when present
    /// and positive. Properties are read/written as MessagePack (the wire/storage
    /// format produced by `client.nodes.add`).
    pub fn decay_sweep(
        &mut self,
        now: u64,
        default_half_life: f64,
        floor: f64,
        prune: bool,
    ) -> crate::types::DecayStats {
        let mut stats = crate::types::DecayStats::default();

        // ── Nodes ──
        let node_ids: Vec<String> = self.node_properties.keys().cloned().collect();
        let mut node_prune: Vec<String> = Vec::new();
        for nid in node_ids {
            if let Some(bytes) = self.node_properties.get(&nid).cloned() {
                if let Ok(mut val) = rmp_serde::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(obj) = val.as_object_mut() {
                        let (new_conf, changed) = apply_decay(obj, now, default_half_life);
                        if changed {
                            stats.nodes_decayed += 1;
                            if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                                self.node_properties.insert(nid.clone(), reenc);
                            }
                        }
                        if prune && new_conf < floor {
                            node_prune.push(nid.clone());
                        }
                    }
                }
            }
        }

        // ── Edges ── (edge_properties: (src,tgt) -> Vec<Vec<u8>> parallel edges)
        let edge_keys: Vec<(String, String)> = self.edge_properties.keys().cloned().collect();
        let mut edge_prune: Vec<(String, String)> = Vec::new();
        for key in edge_keys {
            let mut min_conf = 1.0f64;
            if let Some(blobs) = self.edge_properties.get_mut(&key) {
                for b in blobs.iter_mut() {
                    if let Ok(mut val) = rmp_serde::from_slice::<serde_json::Value>(b) {
                        if let Some(obj) = val.as_object_mut() {
                            let (new_conf, changed) = apply_decay(obj, now, default_half_life);
                            if changed {
                                stats.edges_decayed += 1;
                                if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                                    *b = reenc;
                                }
                            }
                            if new_conf < min_conf {
                                min_conf = new_conf;
                            }
                        }
                    }
                }
            }
            if prune && min_conf < floor {
                edge_prune.push(key);
            }
        }

        // ── Prune below floor ──
        for (s, t) in &edge_prune {
            self.remove_edge(s.clone(), t.clone());
            stats.edges_pruned += 1;
        }
        for nid in &node_prune {
            self.remove_node(nid.clone());
            stats.nodes_pruned += 1;
        }
        stats
    }

    /// Refresh the given nodes on access (spaced-repetition reset): stamp
    /// `last_access = now` and restore `confidence = 1.0` so the forgetting
    /// clock restarts. Call when an agent actually reads/uses a fact. Returns
    /// the number of nodes touched.
    pub fn touch_nodes(&mut self, node_ids: &[String], now: u64) -> usize {
        let mut touched = 0usize;
        for nid in node_ids {
            if let Some(bytes) = self.node_properties.get(nid).cloned() {
                if let Ok(mut val) = rmp_serde::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(obj) = val.as_object_mut() {
                        obj.insert("last_access".to_string(), serde_json::json!(now));
                        obj.insert("confidence".to_string(), serde_json::json!(1.0_f64));
                        if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                            self.node_properties.insert(nid.clone(), reenc);
                            touched += 1;
                        }
                    }
                }
            }
        }
        touched
    }
}

// ── Free Functions (non-method helpers) ──────────────────────────────────

/// Apply the Ebbinghaus retention curve to a single property map in place.
///
/// Reads `confidence` (default 1.0), `last_access` (→ `updated_at` → `created_at`
/// → `now`) and an optional per-item `half_life`. Writes the decayed
/// `confidence` and advances `last_access` to `now`. Returns `(new_confidence,
/// changed)`; `changed` is false when no time elapsed (fresh item) so callers can
/// skip a re-encode. `last_access` is always stamped so the next sweep has an
/// anchor.
fn apply_decay(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    now: u64,
    default_half_life: f64,
) -> (f64, bool) {
    let confidence = obj
        .get("confidence")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    let last_access = obj
        .get("last_access")
        .and_then(|v| v.as_u64())
        .or_else(|| obj.get("updated_at").and_then(|v| v.as_u64()))
        .or_else(|| obj.get("created_at").and_then(|v| v.as_u64()))
        .unwrap_or(now);
    let half_life = obj
        .get("half_life")
        .and_then(|v| v.as_f64())
        .filter(|h| *h > 0.0)
        .unwrap_or(default_half_life);

    if now <= last_access || half_life <= 0.0 {
        // Nothing to decay yet; ensure there is an anchor for the next sweep.
        obj.insert("last_access".to_string(), serde_json::json!(now));
        return (confidence, false);
    }

    let dt = (now - last_access) as f64;
    let retention = 0.5_f64.powf(dt / half_life);
    let new_conf = (confidence * retention).clamp(0.0, 1.0);
    obj.insert("confidence".to_string(), serde_json::json!(new_conf));
    obj.insert("last_access".to_string(), serde_json::json!(now));
    (new_conf, true)
}

pub fn walk_dir_recursive(dir: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if !name.starts_with('.') && name != "node_modules" && name != "target" {
                    walk_dir_recursive(&path, files);
                }
            } else {
                let ext = path.extension().unwrap_or_default().to_string_lossy();
                if ext == "py"
                    || ext == "js"
                    || ext == "ts"
                    || ext == "rs"
                    || ext == "go"
                    || ext == "tsx"
                    || ext == "jsx"
                    || ext == "mjs"
                {
                    files.push(path);
                }
            }
        }
    }
}

fn backtrack_match(
    host: &GraphCore,
    pattern_node_idx: usize,
    pattern_nodes: &[String],
    current_mapping: &mut HashMap<String, String>,
    mapped_targets: &mut std::collections::HashSet<String>,
    pattern: &GraphCore,
    matches: &mut Vec<HashMap<String, String>>,
) {
    if pattern_node_idx == pattern_nodes.len() {
        matches.push(current_mapping.clone());
        return;
    }

    let p_node = &pattern_nodes[pattern_node_idx];

    for t_node in host.node_map.keys() {
        if mapped_targets.contains(t_node) {
            continue;
        }

        if check_match(host, p_node, t_node, current_mapping, pattern) {
            current_mapping.insert(p_node.clone(), t_node.clone());
            mapped_targets.insert(t_node.clone());

            backtrack_match(
                host,
                pattern_node_idx + 1,
                pattern_nodes,
                current_mapping,
                mapped_targets,
                pattern,
                matches,
            );

            current_mapping.remove(p_node);
            mapped_targets.remove(t_node);
        }
    }
}

fn check_match(
    host: &GraphCore,
    p_node: &str,
    t_node: &str,
    current_mapping: &HashMap<String, String>,
    pattern: &GraphCore,
) -> bool {
    let p_props = pattern
        .node_properties
        .get(p_node)
        .map(|s| s.as_slice())
        .unwrap_or(b"{}");
    let t_props = host
        .node_properties
        .get(t_node)
        .map(|s| s.as_slice())
        .unwrap_or(b"{}");

    if !match_props(p_props, t_props) {
        return false;
    }

    let p_idx = match pattern.node_map.get(p_node) {
        Some(&idx) => idx,
        None => return false,
    };

    // In-edges
    for in_edge in pattern
        .graph
        .edges_directed(p_idx, petgraph::Direction::Incoming)
    {
        let p_src = &pattern.graph[in_edge.source()];
        if let Some(t_src) = current_mapping.get(p_src) {
            if !host.has_edge(t_src, t_node) {
                return false;
            }
            if !check_edge_props(host, pattern, p_src, p_node, t_src, t_node) {
                return false;
            }
        }
    }

    // Out-edges
    for out_edge in pattern
        .graph
        .edges_directed(p_idx, petgraph::Direction::Outgoing)
    {
        let p_tgt = &pattern.graph[out_edge.target()];
        if let Some(t_tgt) = current_mapping.get(p_tgt) {
            if !host.has_edge(t_node, t_tgt) {
                return false;
            }
            if !check_edge_props(host, pattern, p_node, &p_tgt.clone(), t_node, t_tgt) {
                return false;
            }
        }
    }

    true
}

fn check_edge_props(
    host: &GraphCore,
    pattern: &GraphCore,
    p_src: &str,
    p_tgt: &str,
    t_src: &str,
    t_tgt: &str,
) -> bool {
    if let Some(p_props_list) = pattern
        .edge_properties
        .get(&(p_src.to_string(), p_tgt.to_string()))
    {
        if let Some(t_props_list) = host
            .edge_properties
            .get(&(t_src.to_string(), t_tgt.to_string()))
        {
            for p_edge_props in p_props_list {
                let mut matched = false;
                for t_edge_props in t_props_list {
                    if match_props(p_edge_props, t_edge_props) {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return false;
                }
            }
        } else {
            return false;
        }
    }
    true
}

pub fn match_props(p_msgpack: &[u8], t_msgpack: &[u8]) -> bool {
    let p_val: serde_json::Value = match rmp_serde::from_slice(p_msgpack) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let t_val: serde_json::Value = match rmp_serde::from_slice(t_msgpack) {
        Ok(v) => v,
        Err(_) => return false,
    };

    if let (Some(p_obj), Some(t_obj)) = (p_val.as_object(), t_val.as_object()) {
        for (k, v) in p_obj {
            if let Some(t_v) = t_obj.get(k) {
                if v != t_v {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    } else {
        p_val == t_val
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn props(map: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&map).unwrap()
    }

    fn confidence_of(core: &GraphCore, id: &str) -> f64 {
        let bytes = core.get_node_properties(id).expect("node exists");
        let v: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        v.get("confidence").and_then(|c| c.as_f64()).unwrap()
    }

    #[test]
    fn decay_halves_confidence_at_one_half_life() {
        let mut g = GraphCore::new();
        let now = 1_000_000u64;
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": now - 100})),
        );
        let stats = g.decay_sweep(now, 100.0, 0.0, false);
        assert_eq!(stats.nodes_decayed, 1);
        let c = confidence_of(&g, "n1");
        assert!((c - 0.5).abs() < 1e-9, "expected ~0.5, got {c}");
    }

    #[test]
    fn fresh_node_does_not_decay() {
        let mut g = GraphCore::new();
        let now = 1_000_000u64;
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": now})),
        );
        let stats = g.decay_sweep(now, 100.0, 0.0, false);
        assert_eq!(stats.nodes_decayed, 0);
        assert!((confidence_of(&g, "n1") - 1.0).abs() < 1e-12);
    }

    #[test]
    fn decay_compounds_across_sweeps() {
        // R(Δt₁)·R(Δt₂) must equal R(Δt₁+Δt₂): two one-half-life sweeps → 0.25.
        let mut g = GraphCore::new();
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": 1000u64})),
        );
        g.decay_sweep(1100, 100.0, 0.0, false);
        g.decay_sweep(1200, 100.0, 0.0, false);
        let c = confidence_of(&g, "n1");
        assert!((c - 0.25).abs() < 1e-9, "expected ~0.25, got {c}");
    }

    #[test]
    fn touch_resets_confidence_and_clock() {
        let mut g = GraphCore::new();
        let now = 5000u64;
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 0.3, "last_access": 1000u64})),
        );
        assert_eq!(g.touch_nodes(&["n1".to_string()], now), 1);
        assert!((confidence_of(&g, "n1") - 1.0).abs() < 1e-12);
        // Immediately after touch, a sweep at the same instant must not decay.
        assert_eq!(g.decay_sweep(now, 100.0, 0.0, false).nodes_decayed, 0);
    }

    #[test]
    fn prune_removes_below_floor() {
        let mut g = GraphCore::new();
        let now = 1_000_000u64;
        // ~4 half-lives elapsed → retention ≈ 0.0625, below the 0.1 floor.
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": now - 400})),
        );
        let stats = g.decay_sweep(now, 100.0, 0.1, true);
        assert_eq!(stats.nodes_pruned, 1);
        assert!(!g.has_node("n1"));
    }
}
