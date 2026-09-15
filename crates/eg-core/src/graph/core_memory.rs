use super::*;

impl GraphCore {
    // ── Agent-native memory primitives — one-shot wrappers + queries ──────────
    //     (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg hierarchical summary tier / EG-221 consolidation)

    /// One-shot [`GraphTxn::create_summary_node`] (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg): the whole
    /// create-node + link-children runs under ONE topology write guard, then the
    /// lazy secondary indexes are invalidated via `mark_dirty` so a subsequent
    /// `summaries_at_level` sees the new node. Returns the summary node id.
    pub fn create_summary_node(
        &self,
        level: u32,
        child_ids: &[String],
        props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        let id = self.txn().create_summary_node(level, child_ids, props);
        self.mark_dirty();
        id
    }

    /// One-shot [`GraphTxn::consolidate`] (CONCEPT:EG-KG.compute.consolidate-cluster): the whole
    /// create-semantic + redirect-edges + provenance + mark-episodics runs under ONE
    /// topology write guard (atomic + localized — no global reindex), then the lazy
    /// secondary indexes are invalidated. Returns the semantic node id.
    pub fn consolidate(
        &self,
        episodic_ids: &[String],
        semantic_props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        let id = self.txn().consolidate(episodic_ids, semantic_props);
        self.mark_dirty();
        id
    }

    /// Does a `source → target` edge carrying `relationship` exist? (Read helper for
    /// the summary/consolidation queries — CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg.) Matches on the edge
    /// blob's canonical `relationship` field.
    pub(super) fn edge_has_relationship(
        &self,
        source_id: &str,
        target_id: &str,
        relationship: &str,
    ) -> bool {
        edge_declares_relationship(&self.edge_properties, source_id, target_id, relationship)
    }

    /// The sorted, deduplicated targets of `id`'s outgoing edges that declare
    /// `relationship`. Empty when the node is absent or has no such edge.
    pub(super) fn related_targets(&self, id: &str, relationship: &str) -> Vec<String> {
        let targets: Vec<String> = {
            let topo = self.topo.read();
            let Some(&idx) = topo.node_map.get(id) else {
                return Vec::new();
            };
            topo.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| topo.graph[e.target()].clone())
                .collect()
        };
        let mut out: Vec<String> = targets
            .into_iter()
            .filter(|t| self.edge_has_relationship(id, t, relationship))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg — the direct children of summary node `id`: the targets of its
    /// outgoing `SUMMARIZES` edges. Returns ids sorted + deduped. Empty if the node
    /// is absent or has no summary children.
    pub fn summary_children(&self, id: &str) -> Vec<String> {
        self.related_targets(id, "SUMMARIZES")
    }

    /// CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg — all summary node ids at abstraction `level` (nodes typed
    /// `SummaryNode` whose `summary_level` equals `level`). Returns ids sorted +
    /// deduped. Uses the lazy label index (`get_nodes_by_label`) so the `SummaryNode`
    /// scan is O(matches) once the index is warm.
    pub fn summaries_at_level(&self, level: u32) -> Vec<String> {
        let mut out: Vec<String> = self
            .get_nodes_by_label("SummaryNode", 0)
            .into_iter()
            .filter_map(|(id, blob)| {
                let val = decode_property_value(&blob).ok()?;
                let lvl = val.get("summary_level").and_then(|v| v.as_u64())?;
                (lvl == level as u64).then_some(id)
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    // ── Memory maintenance — one-shot wrappers (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive) ────────────────
    //     (decay + reinforcement; the AU maintenance loop schedules these)

    /// One-shot [`GraphTxn::reinforce`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): bump access/recency +
    /// importance under ONE topology write guard, then invalidate the lazy secondary
    /// indexes. Returns whether the node existed.
    pub fn reinforce(&self, id: &str, now_ms: u64, weight: f64) -> bool {
        let ok = self.txn().reinforce(id, now_ms, weight);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::decay_node`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive). Returns whether it
    /// decayed/stamped the node.
    pub fn decay_node(&self, id: &str, now_ms: u64, half_life_ms: u64) -> bool {
        let ok = self.txn().decay_node(id, now_ms, half_life_ms);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::decay_memories`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): the whole working-set
    /// decay runs under ONE topology write guard (localized — no global scan). Returns
    /// the number of nodes decayed.
    pub fn decay_memories(&self, now_ms: u64, half_life_ms: u64, ids: &[String]) -> usize {
        let n = self.txn().decay_memories(now_ms, half_life_ms, ids);
        if n > 0 {
            self.mark_dirty();
        }
        n
    }

    /// One-shot [`GraphTxn::forget`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive). `delete == false` marks
    /// `forgotten` (provenance-preserving default); `true` hard-removes. Returns
    /// whether it acted.
    pub fn forget(&self, id: &str, delete: bool) -> bool {
        let ok = self.txn().forget(id, delete);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::evict_below`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): prune the sub-threshold
    /// members of the working set under ONE topology write guard. Returns the pruned
    /// ids (sorted).
    pub fn evict_below(&self, ids: &[String], threshold: f64, delete: bool) -> Vec<String> {
        let pruned = self.txn().evict_below(ids, threshold, delete);
        if !pruned.is_empty() {
            self.mark_dirty();
        }
        pruned
    }

    /// One-shot [`GraphTxn::maintain`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): decay-then-evict the working
    /// set under ONE topology write guard (atomic + localized — no global reindex) —
    /// the primitive the AU maintenance loop schedules. Returns `(decayed, pruned_ids)`.
    pub fn maintain(
        &self,
        ids: &[String],
        now_ms: u64,
        half_life_ms: u64,
        evict_threshold: f64,
        delete: bool,
    ) -> (usize, Vec<String>) {
        let out = self
            .txn()
            .maintain(ids, now_ms, half_life_ms, evict_threshold, delete);
        self.mark_dirty();
        out
    }
}
