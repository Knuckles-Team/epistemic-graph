use super::*;

impl GraphCore {
    // ── Ebbinghaus Temporal Decay (CONCEPT:EG-KG.compute.graph-compute-engine) ──────────────────────

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
        &self,
        now: u64,
        default_half_life: f64,
        floor: f64,
        prune: bool,
    ) -> crate::types::DecayStats {
        let mut stats = crate::types::DecayStats::default();
        let node_prune: Vec<String>;
        let edge_prune: Vec<(String, String)>;

        // The property re-encode runs under the topology READ lock: it excludes
        // structural writers (add/remove go through a write txn), so a node can't
        // be concurrently removed while we re-insert its decayed properties (which
        // would resurrect it). Reads/other property updates still proceed.
        {
            let _topo = self.topo.read();
            node_prune = self.decay_all_nodes(now, default_half_life, floor, prune, &mut stats);
            edge_prune = self.decay_all_edges(now, default_half_life, floor, prune, &mut stats);
        }

        // ── Prune below floor (each removal takes its own write txn) ──
        for (s, t) in &edge_prune {
            self.remove_edge(s.clone(), t.clone());
            stats.edges_pruned += 1;
        }
        for nid in &node_prune {
            self.remove_node(nid.clone());
            stats.nodes_pruned += 1;
        }
        // Decay/prune mutated persistent state → the next checkpoint must rewrite
        // this graph (Phase C-C). The background sweep does not go through dispatch,
        // so it marks dirty here directly.
        if stats.nodes_decayed > 0
            || stats.edges_decayed > 0
            || stats.nodes_pruned > 0
            || stats.edges_pruned > 0
        {
            self.mark_dirty();
        }
        stats
    }

    /// Decay every node's belief `confidence` in place, bumping `stats`.
    ///
    /// The caller MUST already hold the topology READ guard — this only touches
    /// `node_properties`, never the topology, and must not take a lock itself.
    /// Returns the ids that fell below `floor` (empty unless `prune`).
    pub(super) fn decay_all_nodes(
        &self,
        now: u64,
        default_half_life: f64,
        floor: f64,
        prune: bool,
        stats: &mut crate::types::DecayStats,
    ) -> Vec<String> {
        let mut node_prune: Vec<String> = Vec::new();
        let node_ids: Vec<String> = self
            .node_properties
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for nid in node_ids {
            let Some(bytes) = self.node_properties.get(&nid).map(|r| r.value().clone()) else {
                continue;
            };
            let Ok(mut val) = decode_property_value(&bytes) else {
                continue;
            };
            let Some(obj) = val.as_object_mut() else {
                continue;
            };
            let (new_conf, changed) = apply_decay(obj, now, default_half_life);
            if changed {
                stats.nodes_decayed += 1;
                if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                    self.node_properties.insert(nid.clone(), Arc::new(reenc));
                }
            }
            if prune && new_conf < floor {
                node_prune.push(nid.clone());
            }
        }
        node_prune
    }

    /// Decay every parallel edge blob in place, bumping `stats`.
    ///
    /// The caller MUST already hold the topology READ guard (see
    /// [`GraphCore::decay_all_nodes`]). `edge_properties` maps `(src, tgt)` to the
    /// parallel-edge blobs. Returns the keys whose WEAKEST parallel edge fell
    /// below `floor` (empty unless `prune`).
    pub(super) fn decay_all_edges(
        &self,
        now: u64,
        default_half_life: f64,
        floor: f64,
        prune: bool,
        stats: &mut crate::types::DecayStats,
    ) -> Vec<(String, String)> {
        let mut edge_prune: Vec<(String, String)> = Vec::new();
        let edge_keys: Vec<(String, String)> = self
            .edge_properties
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for key in edge_keys {
            let mut min_conf = 1.0f64;
            if let Some(mut blobs) = self.edge_properties.get_mut(&key) {
                for b in blobs.iter_mut() {
                    let new_conf = Self::decay_edge_blob(b, now, default_half_life, stats);
                    if new_conf < min_conf {
                        min_conf = new_conf;
                    }
                }
            }
            if prune && min_conf < floor {
                edge_prune.push(key);
            }
        }
        edge_prune
    }

    /// Decay ONE edge property blob in place, bumping `stats.edges_decayed`.
    ///
    /// Returns the blob's confidence after decay, or `1.0` when it is undecodable
    /// or not a property object — matching the original sweep, which left
    /// `min_conf` untouched in exactly those cases.
    pub(super) fn decay_edge_blob(
        blob: &mut Arc<Vec<u8>>,
        now: u64,
        default_half_life: f64,
        stats: &mut crate::types::DecayStats,
    ) -> f64 {
        let Ok(mut val) = decode_property_value(blob.as_slice()) else {
            return 1.0;
        };
        let Some(obj) = val.as_object_mut() else {
            return 1.0;
        };
        let (new_conf, changed) = apply_decay(obj, now, default_half_life);
        if changed {
            stats.edges_decayed += 1;
            if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                *blob = Arc::new(reenc);
            }
        }
        new_conf
    }

    /// Refresh the given nodes on access (spaced-repetition reset): stamp
    /// `last_access = now` and restore `confidence = 1.0` so the forgetting
    /// clock restarts. Call when an agent actually reads/uses a fact. Returns
    /// the number of nodes touched.
    pub fn touch_nodes(&self, node_ids: &[String], now: u64) -> usize {
        let _topo = self.topo.read();
        let mut touched = 0usize;
        for nid in node_ids {
            if let Some(bytes) = self.node_properties.get(nid).map(|a| (**a).clone()) {
                if let Ok(mut val) = decode_property_value(&bytes) {
                    if let Some(obj) = val.as_object_mut() {
                        obj.insert("last_access".to_string(), serde_json::json!(now));
                        obj.insert("confidence".to_string(), serde_json::json!(1.0_f64));
                        if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                            self.node_properties.insert(nid.clone(), Arc::new(reenc));
                            touched += 1;
                        }
                    }
                }
            }
        }
        touched
    }
}
