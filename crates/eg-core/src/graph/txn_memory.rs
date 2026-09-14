use super::*;

impl<'a> GraphTxn<'a> {
    // ── Agent-native memory primitives (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg / EG-221) ─────────────
    //
    // The DETERMINISTIC engine substrate for the agent-native memory tier from the
    // "Are We Ready For An Agent-Native Memory System?" survey (arXiv 2606.24775).
    // The memory LIFECYCLE — deciding *when* to summarize/consolidate, and the
    // LLM-produced summary TEXT — lives in agent-utilities; the engine only stores
    // + links what AU hands it. No clock/RNG is read internally: any node id is
    // derived deterministically from the (sorted) inputs, and any timestamp is
    // taken from the caller-supplied property blobs. Every method here runs under
    // the single held topology write guard, so it is atomic w.r.t. other writers
    // and replays identically from the WAL / on a Raft follower.

    /// Decode a node's stored property blob into its raw property object (no
    /// synthetic `id` injection, unlike [`GraphTxn::node_row_map`]). `None` if the
    /// node is absent or its blob is not a decodable object. Read under the held
    /// write guard so the read-modify-write stays atomic (CONCEPT:EG-KG.compute.consolidate-cluster).
    pub(super) fn node_object(
        &self,
        node_id: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let bytes = self.node_properties.get(node_id)?.value().clone();
        match decode_property_value(&bytes) {
            Ok(serde_json::Value::Object(o)) => Some(o),
            _ => None,
        }
    }

    /// Unconditionally merge `updates` into a node's property object under the held
    /// write guard (CONCEPT:EG-KG.compute.consolidate-cluster). Like [`GraphTxn::compare_and_set_fields`] with
    /// no conditions. A missing/undecodable node is a no-op returning `false`.
    pub(super) fn merge_fields(
        &mut self,
        node_id: &str,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        let empty = serde_json::Map::new();
        self.compare_and_set_fields(node_id, &empty, updates)
    }

    /// Does a `source → target` edge carrying `relationship` already exist under
    /// the held guard? Used to keep provenance-edge creation idempotent so a
    /// re-run of `create_summary_node` / `consolidate` does not add parallel edges.
    pub(super) fn has_relationship_edge(
        &self,
        source_id: &str,
        target_id: &str,
        relationship: &str,
    ) -> bool {
        edge_declares_relationship(self.edge_properties, source_id, target_id, relationship)
    }

    /// Deterministic id for a summary node over `(level, sorted child ids)`
    /// (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg). Same inputs ⇒ same id, so re-summarizing a cluster at a
    /// level UPSERTS the same node rather than spawning a duplicate. No RNG.
    pub(super) fn derive_summary_id(level: u32, sorted_children: &[String]) -> String {
        use std::hash::{Hash, Hasher};
        // `DefaultHasher::new()` is SipHash with FIXED (zero) keys — deterministic
        // across processes (unlike `RandomState`), so the id replays identically.
        let mut h = std::collections::hash_map::DefaultHasher::new();
        level.hash(&mut h);
        for c in sorted_children {
            c.hash(&mut h);
        }
        format!("summary:L{}:{:016x}", level, h.finish())
    }

    /// Deterministic id for a consolidated semantic node over its `(sorted episodic
    /// ids)` cluster (CONCEPT:EG-KG.compute.consolidate-cluster). No RNG.
    pub(super) fn derive_semantic_id(sorted_cluster: &[String]) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for c in sorted_cluster {
            c.hash(&mut h);
        }
        format!("semantic:{:016x}", h.finish())
    }

    /// CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg — create (or UPSERT) a hierarchical summary node at abstraction
    /// `level`, linked to each of `child_ids` via a `SUMMARIZES` provenance edge, so a
    /// multi-level abstraction ladder can be built (a level-2 summary's children may
    /// themselves be level-1 summary nodes). The LLM-produced summary TEXT is passed
    /// in `props` by the caller (AU); the engine stores it verbatim and only injects
    /// the structural markers `type = "SummaryNode"`, `summary_level`, and
    /// `summary_child_count`. If `props` carries an `id` string it is honoured (and
    /// stripped from the stored blob, since a node id is not a property); otherwise a
    /// deterministic id over `(level, sorted children)` is used. Returns the id.
    ///
    /// Deterministic + idempotent: re-running with the same inputs upserts the same
    /// node and does NOT duplicate the `SUMMARIZES` edges. Children that do not exist
    /// are skipped (no dangling edges). Runs under the held write guard.
    pub fn create_summary_node(
        &mut self,
        level: u32,
        child_ids: &[String],
        props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        // Canonicalize children (sorted + deduped) for deterministic id + ledger.
        let mut children: Vec<String> = child_ids.to_vec();
        children.sort();
        children.dedup();

        let id = match props.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => Self::derive_summary_id(level, &children),
        };

        // Merge onto any existing node object (upsert) then apply caller props +
        // structural markers.
        let mut obj = self.node_object(&id).unwrap_or_default();
        for (k, v) in props {
            if k == "id" {
                continue; // the node id, not a stored property
            }
            obj.insert(k, v);
        }
        obj.entry("type".to_string())
            .or_insert_with(|| serde_json::json!("SummaryNode"));
        obj.insert("summary_level".to_string(), serde_json::json!(level));
        obj.insert(
            "summary_child_count".to_string(),
            serde_json::json!(children.len()),
        );
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            self.add_node(id.clone(), blob);
        }

        // Link to existing children via idempotent `SUMMARIZES` provenance edges.
        for child in &children {
            if child == &id {
                continue; // never summarize self
            }
            if !self.topo.node_map.contains_key(child) {
                continue; // skip absent child — no dangling edge
            }
            if self.has_relationship_edge(&id, child, "SUMMARIZES") {
                continue; // already linked (idempotent re-run)
            }
            if let Ok(eprops) =
                rmp_serde::to_vec_named(&serde_json::json!({"relationship": "SUMMARIZES"}))
            {
                let _ = self.add_edge(id.clone(), child.clone(), eprops);
            }
        }
        id
    }

    /// CONCEPT:EG-KG.compute.consolidate-cluster — consolidate a cluster of `episodic_ids` memory nodes into ONE
    /// semantic node (episodic → semantic consolidation), the paper's key
    /// LOCALIZED-maintenance finding: nothing outside the cluster + its immediate
    /// neighbours is touched, and there is NO global reindex.
    ///
    /// What it does, all atomically under the held write guard:
    /// * Create/UPDATE the semantic node with the caller-merged `semantic_props`
    ///   (AU/LLM produce the merged content; the engine stores it). `type` defaults
    ///   to `"SemanticMemory"`; `consolidated_from_count` is recorded. If
    ///   `semantic_props` carries an `id` it is honoured, else a deterministic id
    ///   over the sorted cluster is used.
    /// * Preserve BITEMPORAL validity: the consolidated node's `tx_from` spans the
    ///   MIN of the children's `tx_from`, and `tx_to` the MAX of the children's
    ///   `tx_to` — UNLESS any child is still open (no `tx_to`), in which case the
    ///   consolidated node is left open too. Caller-supplied `tx_from`/`tx_to`
    ///   override the computed span.
    /// * COPY each episodic's EXTERNAL edges (endpoint outside the cluster) onto the
    ///   semantic node, re-pointed to it — so the semantic node inherits the
    ///   cluster's connectivity. Intra-cluster edges are subsumed (skipped). The
    ///   originals are preserved on the episodics (provenance/history intact).
    /// * Add a `CONSOLIDATES` provenance edge semantic → each episodic.
    /// * Mark each episodic `consolidated = true` + `consolidated_into = <id>`
    ///   WITHOUT deleting it (bitemporal history preserved).
    ///
    /// Deterministic (inputs sorted; copied edges collected + sorted before apply)
    /// and idempotent (provenance edges guarded). Returns the semantic node id.
    pub fn consolidate(
        &mut self,
        episodic_ids: &[String],
        semantic_props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        // Canonical cluster (sorted + deduped) for a deterministic id + ledger.
        let mut cluster: Vec<String> = episodic_ids.to_vec();
        cluster.sort();
        cluster.dedup();
        let cluster_set: std::collections::HashSet<&str> =
            cluster.iter().map(|s| s.as_str()).collect();

        let semantic_id = match semantic_props.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => Self::derive_semantic_id(&cluster),
        };

        let obj = self.build_semantic_object(&cluster, &semantic_id, semantic_props);
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            self.add_node(semantic_id.clone(), blob);
        }

        // Collect EXTERNAL edges to copy onto the semantic node. Gather first (no
        // mutation during the DashMap scan), then sort + dedup for a deterministic
        // ledger, then apply after the iterator guard drops.
        let to_add = self.collect_external_edges(&cluster_set, &semantic_id);
        self.apply_redirected_edges(to_add);

        // Provenance + mark episodics consolidated (localized — cluster only).
        self.mark_cluster_consolidated(&cluster, &semantic_id);
        semantic_id
    }

    /// Compute the consolidated node's BITEMPORAL span over `cluster`: the MIN of
    /// the children's `tx_from` and the MAX of their `tx_to`, plus whether any
    /// child is still open (no `tx_to`) — an open child leaves the span open
    /// (CONCEPT:EG-KG.compute.preserved/2.250 preserved).
    pub(super) fn consolidated_tx_span(
        &self,
        cluster: &[String],
    ) -> (Option<u64>, Option<u64>, bool) {
        let mut tx_from_min: Option<u64> = None;
        let mut tx_to_max: Option<u64> = None;
        let mut any_open = false;
        for epi in cluster {
            let Some(obj) = self.node_object(epi) else {
                continue;
            };
            if let Some(f) = obj.get("tx_from").and_then(|v| v.as_u64()) {
                tx_from_min = Some(tx_from_min.map_or(f, |m| m.min(f)));
            }
            match obj.get("tx_to").and_then(|v| v.as_u64()) {
                Some(t) => tx_to_max = Some(tx_to_max.map_or(t, |m| m.max(t))),
                None => any_open = true, // an open child ⇒ the span stays open
            }
        }
        (tx_from_min, tx_to_max, any_open)
    }

    /// Build the semantic node object: merge onto any existing node (upsert),
    /// then caller props, then the computed markers (caller values win).
    pub(super) fn build_semantic_object(
        &self,
        cluster: &[String],
        semantic_id: &str,
        semantic_props: serde_json::Map<String, serde_json::Value>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let (tx_from_min, tx_to_max, any_open) = self.consolidated_tx_span(cluster);
        let mut obj = self.node_object(semantic_id).unwrap_or_default();
        for (k, v) in semantic_props {
            if k == "id" {
                continue;
            }
            obj.insert(k, v);
        }
        obj.entry("type".to_string())
            .or_insert_with(|| serde_json::json!("SemanticMemory"));
        obj.insert(
            "consolidated_from_count".to_string(),
            serde_json::json!(cluster.len()),
        );
        if !obj.contains_key("tx_from") {
            if let Some(f) = tx_from_min {
                obj.insert("tx_from".to_string(), serde_json::json!(f));
            }
        }
        if !obj.contains_key("tx_to") && !any_open {
            if let Some(t) = tx_to_max {
                obj.insert("tx_to".to_string(), serde_json::json!(t));
            }
        }
        obj
    }

    /// Gather the cluster's EXTERNAL edges (exactly one endpoint inside the
    /// cluster) re-pointed onto the semantic node. Intra-cluster edges are
    /// subsumed and fully-external edges are unrelated, so both are skipped.
    /// Read-only: nothing is mutated while the DashMap iterator guard is alive.
    /// Sorted + deduped for a deterministic ledger.
    pub(super) fn collect_external_edges(
        &self,
        cluster_set: &std::collections::HashSet<&str>,
        semantic_id: &str,
    ) -> Vec<(String, String, Vec<u8>)> {
        let mut to_add: Vec<(String, String, Vec<u8>)> = Vec::new();
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            let src_in = cluster_set.contains(src.as_str());
            let tgt_in = cluster_set.contains(tgt.as_str());
            if src_in == tgt_in {
                // both in-cluster (subsumed) or both external (unrelated) — skip.
                continue;
            }
            for blob in entry.value() {
                if src_in {
                    if tgt.as_str() != semantic_id {
                        to_add.push((semantic_id.to_string(), tgt.clone(), (**blob).clone()));
                    }
                } else if src.as_str() != semantic_id {
                    to_add.push((src.clone(), semantic_id.to_string(), (**blob).clone()));
                }
            }
        }
        to_add.sort();
        to_add.dedup();
        to_add
    }

    /// Apply the redirected external edges collected by
    /// [`GraphTxn::collect_external_edges`]. An identical-relationship edge that
    /// already connects the endpoints is skipped, so a re-run does not stack
    /// duplicate redirected edges.
    pub(super) fn apply_redirected_edges(&mut self, to_add: Vec<(String, String, Vec<u8>)>) {
        for (src, tgt, blob) in to_add {
            let rel = decode_property_value(&blob).ok().and_then(|v| {
                v.as_object()
                    .and_then(|o| o.get("relationship"))
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string())
            });
            if let Some(r) = &rel {
                if self.has_relationship_edge(&src, &tgt, r) {
                    continue;
                }
            }
            let _ = self.add_edge(src, tgt, blob);
        }
    }

    /// Add the `CONSOLIDATES` provenance edge semantic → each episodic (guarded,
    /// so it is idempotent) and mark each episodic `consolidated = true` +
    /// `consolidated_into = <semantic_id>` WITHOUT deleting it (bitemporal
    /// history preserved). Localized — touches the cluster only.
    pub(super) fn mark_cluster_consolidated(&mut self, cluster: &[String], semantic_id: &str) {
        for epi in cluster {
            if epi.as_str() == semantic_id || !self.topo.node_map.contains_key(epi) {
                continue;
            }
            self.link_consolidates(semantic_id, epi);
            let mut mark = serde_json::Map::new();
            mark.insert("consolidated".to_string(), serde_json::json!(true));
            mark.insert(
                "consolidated_into".to_string(),
                serde_json::json!(semantic_id),
            );
            self.merge_fields(epi, &mark);
        }
    }

    /// Add the guarded `CONSOLIDATES` provenance edge `semantic_id → episodic_id`.
    pub(super) fn link_consolidates(&mut self, semantic_id: &str, episodic_id: &str) {
        if self.has_relationship_edge(semantic_id, episodic_id, "CONSOLIDATES") {
            return;
        }
        if let Ok(eprops) =
            rmp_serde::to_vec_named(&serde_json::json!({"relationship": "CONSOLIDATES"}))
        {
            let _ = self.add_edge(semantic_id.to_string(), episodic_id.to_string(), eprops);
        }
    }
}
