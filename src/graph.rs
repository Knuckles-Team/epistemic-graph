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
    pub node_properties: HashMap<String, String>,
    pub edge_properties: HashMap<(String, String), Vec<String>>,
    pub ledger: Vec<String>,
}

impl GraphCore {
    pub fn new() -> Self {
        GraphCore {
            graph: StableDiGraph::new(),
            node_map: HashMap::new(),
            node_properties: HashMap::new(),
            edge_properties: HashMap::new(),
            ledger: Vec::new(),
        }
    }

    // ── Node CRUD ────────────────────────────────────────────────────────

    pub fn add_node(&mut self, node_id: String, properties_json: String) {
        let _idx = if let Some(&existing_idx) = self.node_map.get(&node_id) {
            existing_idx
        } else {
            let new_idx = self.graph.add_node(node_id.clone());
            self.node_map.insert(node_id.clone(), new_idx);
            new_idx
        };
        self.node_properties
            .insert(node_id.clone(), properties_json.clone());
        let log = format!("ADD_NODE|{}|{}", node_id, properties_json);
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

    pub fn get_nodes(&self) -> Vec<(String, String)> {
        self.node_properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn get_node_properties(&self, node_id: &str) -> Option<String> {
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
        properties_json: String,
    ) -> Result<(), String> {
        let source_idx = match self.node_map.get(&source_id) {
            Some(&idx) => idx,
            None => {
                return Err(format!(
                    "Source node '{}' not found",
                    source_id
                ))
            }
        };
        let target_idx = match self.node_map.get(&target_id) {
            Some(&idx) => idx,
            None => {
                return Err(format!(
                    "Target node '{}' not found",
                    target_id
                ))
            }
        };

        self.graph.add_edge(
            source_idx,
            target_idx,
            format!("{}:{}", source_id, target_id),
        );
        self.edge_properties
            .entry((source_id.clone(), target_id.clone()))
            .or_default()
            .push(properties_json.clone());
        let log = format!("ADD_EDGE|{}|{}|{}", source_id, target_id, properties_json);
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

    pub fn get_edges(&self) -> Vec<(String, String, String)> {
        let mut res = Vec::new();
        for ((src, tgt), props_list) in &self.edge_properties {
            for props in props_list {
                res.push((src.clone(), tgt.clone(), props.clone()));
            }
        }
        res
    }

    pub fn get_edge_properties(&self, source_id: &str, target_id: &str) -> Vec<String> {
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
        let idx = self.node_map.get(node_id).ok_or_else(|| {
            format!("Node '{}' not found", node_id)
        })?;
        Ok(self
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
            .count())
    }

    /// Out-degree count for a specific node.
    pub fn out_degree(&self, node_id: &str) -> Result<usize, String> {
        let idx = self.node_map.get(node_id).ok_or_else(|| {
            format!("Node '{}' not found", node_id)
        })?;
        Ok(self
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
            .count())
    }

    // ── Neighbor Queries ─────────────────────────────────────────────────

    /// Incoming neighbors (predecessors).
    pub fn get_predecessors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let idx = self.node_map.get(node_id).ok_or_else(|| {
            format!("Node '{}' not found", node_id)
        })?;
        let preds: Vec<String> = self
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
            .map(|e| self.graph[e.source()].clone())
            .collect();
        Ok(preds)
    }

    /// Outgoing neighbors (successors).
    pub fn get_successors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let idx = self.node_map.get(node_id).ok_or_else(|| {
            format!("Node '{}' not found", node_id)
        })?;
        let succs: Vec<String> = self
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
            .map(|e| self.graph[e.target()].clone())
            .collect();
        Ok(succs)
    }

    /// All neighbors (both directions, deduplicated).
    pub fn get_neighbors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let idx = self.node_map.get(node_id).ok_or_else(|| {
            format!("Node '{}' not found", node_id)
        })?;
        let mut neighbors = std::collections::HashSet::new();
        for e in self.graph.edges_directed(*idx, petgraph::Direction::Incoming) {
            neighbors.insert(self.graph[e.source()].clone());
        }
        for e in self.graph.edges_directed(*idx, petgraph::Direction::Outgoing) {
            neighbors.insert(self.graph[e.target()].clone());
        }
        Ok(neighbors.into_iter().collect())
    }

    // ── Serialization ────────────────────────────────────────────────────

    pub fn to_json(&self) -> Result<String, String> {
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
            serde_json::to_value(&self.ledger)
                .map_err(|e| e.to_string())?,
        );

        serde_json::to_string(&graph_map).map_err(|e| e.to_string())
    }

    pub fn clear(&mut self) {
        self.graph.clear();
        self.node_map.clear();
        self.node_properties.clear();
        self.edge_properties.clear();
        self.ledger.clear();
    }

    pub fn from_json(&mut self, json_str: &str) -> Result<(), String> {
        let graph_map: HashMap<String, serde_json::Value> =
            serde_json::from_str(json_str).map_err(|e| e.to_string())?;

        // Reset state
        self.graph.clear();
        self.node_map.clear();
        self.node_properties.clear();
        self.edge_properties.clear();
        self.ledger.clear();

        if let Some(nodes_val) = graph_map.get("nodes") {
            let nodes: Vec<(String, String)> = serde_json::from_value(nodes_val.clone())
                .map_err(|e| e.to_string())?;
            for (node_id, props) in nodes {
                self.add_node(node_id, props);
            }
        }

        if let Some(edges_val) = graph_map.get("edges") {
            let edges: Vec<(String, String, String)> =
                serde_json::from_value(edges_val.clone())
                    .map_err(|e| e.to_string())?;
            for (src, tgt, props) in edges {
                let _ = self.add_edge(src, tgt, props);
            }
        }

        if let Some(ledger_val) = graph_map.get("ledger") {
            let ledger: Vec<String> = serde_json::from_value(ledger_val.clone())
                .map_err(|e| e.to_string())?;
            self.ledger = ledger;
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
                    self.add_node(parts[1].to_string(), parts[2].to_string());
                }
                "ADD_EDGE" if parts.len() >= 4 => {
                    let _ = self.add_edge(
                        parts[1].to_string(),
                        parts[2].to_string(),
                        parts[3].to_string(),
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

    // ── Graph Forking ────────────────────────────────────────────────────

    pub fn fork(&self) -> GraphCore {
        GraphCore {
            graph: self.graph.clone(),
            node_map: self.node_map.clone(),
            node_properties: self.node_properties.clone(),
            edge_properties: self.edge_properties.clone(),
            ledger: self.ledger.clone(),
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
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(props_json) {
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
        self.add_node(summary_id.clone(), summary_props.to_string());

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
            return Err(format!(
                "Path '{}' does not exist",
                root_path
            ));
        }
        let mut files = Vec::new();
        walk_dir_recursive(root, &mut files);

        for path in files {
            if let Ok(relative) = path.strip_prefix(root) {
                let rel_str = relative.to_string_lossy().to_string();

                let file_props = format!("{{\"type\": \"file\", \"path\": \"{}\"}}", rel_str);
                self.add_node(rel_str.clone(), file_props);

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
                    self.add_node(node_id.clone(), props);
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props);
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
                    self.add_node(node_id.clone(), props);
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props);
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
                    self.add_node(node_id.clone(), props);
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props);
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
}

// ── Free Functions (non-method helpers) ──────────────────────────────────

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
                if ext == "py" || ext == "js" || ext == "ts" {
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
        .map(|s| s.as_str())
        .unwrap_or("{}");
    let t_props = host
        .node_properties
        .get(t_node)
        .map(|s| s.as_str())
        .unwrap_or("{}");

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

pub fn match_props(p_json: &str, t_json: &str) -> bool {
    let p_val: serde_json::Value = match serde_json::from_str(p_json) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let t_val: serde_json::Value = match serde_json::from_str(t_json) {
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
