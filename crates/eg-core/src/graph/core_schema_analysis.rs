use super::*;

impl GraphCore {
    // ── A18 TBox/ABox RLS distinction (BUG A3, 2026-08-12) ─────────────────
    // See `schema_refs`'s own field doc for the full history. `eg-rdf`'s SPARQL
    // UPDATE insert/delete path is the ONLY caller: it marks/unmarks in the
    // SAME transaction as the triple write/delete that makes/unmakes `id` the
    // subject or object of a schema-defining triple.

    /// Record one more LIVE schema-defining triple naming `id` (BUG A3). Call
    /// once per schema-defining triple INSERTED that names `id` as subject or
    /// object, in the same mutation as the triple write.
    pub fn mark_schema_ref(&self, id: &str) {
        *self.schema_refs.entry(id.to_string()).or_insert(0) += 1;
    }

    /// Release one LIVE schema-defining triple naming `id` (BUG A3). Call once
    /// per schema-defining triple REMOVED that names `id` as subject or
    /// object, in the same mutation as the triple delete. Saturating: never
    /// underflows past 0 — a defensive floor, not something callers should
    /// rely on for correctness (mark/unmark are expected to stay symmetric per
    /// triple).
    pub fn unmark_schema_ref(&self, id: &str) {
        if let Some(mut count) = self.schema_refs.get_mut(id) {
            *count = count.saturating_sub(1);
        }
    }

    /// Is `id` CURRENTLY ontology schema (TBox)? DERIVED from the live reverse
    /// index (BUG A3) — true iff at least one schema-defining triple currently
    /// names `id` as subject or object. Never cached; a node whose last
    /// schema-defining triple was deleted answers `false` again immediately,
    /// with no separate "clear the marker" step.
    pub fn is_schema_node(&self, id: &str) -> bool {
        self.schema_refs.get(id).is_some_and(|c| *c > 0)
    }

    /// The full set of node ids CURRENTLY schema (BUG A3) — for snapshotting
    /// into a [`GraphView`] (`GraphView::schema_node_ids`) so
    /// `IsolationLayer::filter_view` can consult it per row without needing a
    /// live `&GraphCore` reference. `O(schema size)`, not `O(graph size)` — an
    /// ontology's class/property/axiom count, never the whole ABox.
    pub(super) fn live_schema_node_ids(&self) -> std::collections::HashSet<String> {
        self.schema_refs
            .iter()
            .filter(|e| *e.value() > 0)
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn diff_against(&self, other: &GraphView) -> String {
        let topo = self.topo.read();
        let self_nodes: std::collections::HashSet<&String> = topo.node_map.keys().collect();
        let other_nodes: std::collections::HashSet<&String> = other.node_map.keys().collect();

        let added: Vec<&String> = other_nodes.difference(&self_nodes).cloned().collect();
        let removed: Vec<&String> = self_nodes.difference(&other_nodes).cloned().collect();

        let mut modified: Vec<&String> = Vec::new();
        for node_id in self_nodes.intersection(&other_nodes) {
            let self_props = self.node_properties.get(*node_id).map(|a| a.clone());
            let other_props = other.node_properties.get(*node_id).cloned();
            if self_props != other_props {
                modified.push(node_id);
            }
        }

        let self_edges: std::collections::HashSet<(String, String)> = self
            .edge_properties
            .iter()
            .map(|e| e.key().clone())
            .collect();
        let other_edges: std::collections::HashSet<&(String, String)> =
            other.edge_properties.keys().collect();
        let edges_added: Vec<&(String, String)> = other_edges
            .iter()
            .filter(|k| !self_edges.contains(**k))
            .cloned()
            .collect();
        let edges_removed: Vec<&(String, String)> = self_edges
            .iter()
            .filter(|k| !other_edges.contains(k))
            .collect();

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

    pub fn compact_nodes_by_type(&self, node_type: &str, threshold: usize) -> Vec<String> {
        let mut candidates: Vec<String> = Vec::new();
        for entry in self.node_properties.iter() {
            let (node_id, props_json) = (entry.key(), entry.value());
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(props_json.as_slice()) {
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

    // ── VF2 Subgraph Matching ────────────────────────────────────────────

    /// VF2 subgraph isomorphism match against a consistent read view so the
    /// worst-case-exponential backtracking never holds a live lock. `max_results`/
    /// `max_steps` (`0` ⇒ [`DEFAULT_VF2_MAX_RESULTS`]/[`DEFAULT_VF2_MAX_STEPS`]) bound
    /// the search; the returned `bool` is `true` when it stopped early against
    /// either budget rather than exhausting the search space.
    pub fn vf2_subgraph_match(
        &self,
        pattern: &GraphView,
        max_results: usize,
        max_steps: usize,
    ) -> (Vec<HashMap<String, String>>, bool) {
        let host = self.analysis_snapshot();
        vf2_match_views(&host, pattern, max_results, max_steps)
    }

    /// The least-recently-added node ids that would be evicted to bring the graph
    /// down to `max_nodes` — the same set (and order) `evict_lru` removes, but
    /// WITHOUT dropping them (CONCEPT:EG-KG.storage.read-through-seam-exercised). Used by the authoritative
    /// eviction path, which must confirm each candidate is durable BEFORE
    /// dropping it (commit-before-ack makes that the common case; the check is the
    /// no-data-loss guarantee). Empty when the graph is at/under the cap.
    pub fn lru_eviction_candidates(&self, max_nodes: usize) -> Vec<String> {
        let mut indexed: Vec<(String, NodeIndex)> = {
            let topo = self.topo.read();
            if topo.node_map.len() <= max_nodes {
                return Vec::new();
            }
            topo.node_map.iter().map(|(k, &v)| (k.clone(), v)).collect()
        };
        let to_evict = indexed.len() - max_nodes;
        // Nodes with the lowest NodeIndex were inserted earliest → approximate LRU.
        // Partition in expected O(N); a full O(N log N) ordering is unnecessary
        // because eviction consumes the set as one batch.
        if to_evict < indexed.len() {
            indexed.select_nth_unstable_by_key(to_evict, |(_, index)| *index);
            indexed.truncate(to_evict);
        }
        indexed.into_iter().map(|(id, _)| id).collect()
    }

    /// Evict nodes down to `max_nodes` by removing the least-recently-added.
    ///
    /// CONCEPT:EG-KG.compute.graph-compute-engine — Memory pressure defense. When the in-memory graph
    /// grows beyond `max_nodes`, this method removes the oldest nodes (by
    /// insertion order in `node_map`) until the count is at or below the cap.
    /// Returns the number of evicted nodes.
    pub fn evict_lru(&self, max_nodes: usize) -> usize {
        let evict_ids = self.lru_eviction_candidates(max_nodes);
        self.evict_resident_nodes(&evict_ids)
    }
}
