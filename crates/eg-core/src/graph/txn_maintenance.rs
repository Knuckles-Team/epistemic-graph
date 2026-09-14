use super::*;

impl<'a> GraphTxn<'a> {
    // ── Memory maintenance — decay + reinforcement (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive) ────────────
    //
    // Module 4 of the agent-native memory tier ("Are We Ready For An Agent-Native
    // Memory System?", arXiv 2606.24775): the paper's finding that "localized
    // maintenance is more cost-efficient than global reorganization". The caller (the
    // AU maintenance loop) picks a WORKING SET of memory ids; the engine only touches
    // those nodes — there is NO global scan or reindex. No clock/RNG is read
    // internally: `now_ms`, `half_life_ms`, `weight`, and `threshold` are all supplied
    // by the caller, so a run replays identically from the WAL / on a Raft follower.
    // Every method runs under the single held topology write guard, so it is atomic
    // w.r.t. other writers.
    //
    // A memory node carries these memory-value fields in its property blob:
    //   * `importance`     (f64) — retrieval-priority weight; decays over time and is
    //                              boosted on access.
    //   * `access_count`   (u64) — how many times it has been reinforced (retrieved).
    //   * `last_access_ms` (u64) — wall-clock of the last reinforcement (recency).
    //   * `last_decay_ms`  (u64) — decay's OWN clock, so a maintenance sweep never
    //                              masquerades as an access.
    //   * `forgotten`      (bool)— set true when evicted via the mark path (provenance
    //                              preserved; the node + its edges + bitemporal history
    //                              stay intact).
    // Every memory participating in maintenance carries an explicit finite
    // `importance`. Missing or non-numeric values are not interpreted as an older
    // schema: that node is outside the maintenance operation and must be migrated by
    // its writer before reinforcement, decay, or eviction.

    /// Read the mandatory current-schema `importance` value.
    pub(super) fn memory_importance(
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<f64> {
        obj.get("importance")
            .and_then(|v| v.as_f64())
            .filter(|importance| importance.is_finite())
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — REINFORCE a memory on retrieval: bump `access_count`, refresh
    /// `last_access_ms` to `now_ms` (recency), and raise `importance` by `weight`
    /// (retrieval strengthens a memory). The current memory schema requires an explicit
    /// finite `importance`; a node without it is rejected from this operation. A memory
    /// previously marked `forgotten` is REVIVED (`forgotten = false`) — a
    /// re-accessed memory is live again.
    ///
    /// Deterministic (no clock/RNG — `now_ms`/`weight` are caller-supplied). This is an
    /// accumulator, so it is intentionally NOT idempotent: each call models a distinct
    /// retrieval event. A missing/undecodable node is a no-op returning `false`. Runs
    /// under the held write guard.
    pub fn reinforce(&mut self, id: &str, now_ms: u64, weight: f64) -> bool {
        let Some(obj) = self.node_object(id) else {
            return false;
        };
        let Some(current_importance) = Self::memory_importance(&obj) else {
            return false;
        };
        let importance = current_importance + weight;
        let access_count = obj
            .get("access_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            + 1;
        let mut updates = serde_json::Map::new();
        updates.insert("importance".to_string(), serde_json::json!(importance));
        updates.insert("access_count".to_string(), serde_json::json!(access_count));
        updates.insert("last_access_ms".to_string(), serde_json::json!(now_ms));
        if obj.get("forgotten").and_then(|v| v.as_bool()) == Some(true) {
            updates.insert("forgotten".to_string(), serde_json::json!(false));
        }
        self.merge_fields(id, &updates)
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — apply time-based EXPONENTIAL decay to a memory's `importance`
    /// from the time elapsed since it was last touched: `importance *= 0.5 ^ (elapsed /
    /// half_life_ms)`, i.e. importance halves every `half_life_ms` of inactivity. The
    /// elapsed clock is measured from `last_decay_ms` if present, else `last_access_ms`
    /// (recency); the method advances `last_decay_ms` — NOT `last_access_ms` — so a
    /// maintenance sweep never masquerades as an access.
    ///
    /// A node carrying no current-schema `importance` field is rejected from the
    /// maintenance operation (returns `false`). A node with importance but no decay reference just stamps `last_decay_ms = now_ms` (a
    /// baseline; no decay this pass). Deterministic (no clock/RNG) and IDEMPOTENT at a
    /// fixed `now_ms`: because it advances `last_decay_ms` to `now_ms`, a second call
    /// with the same `now_ms` sees zero elapsed and leaves importance unchanged; and
    /// successive passes compose exactly (`0.5^(Δ₁/h)·0.5^(Δ₂/h) = 0.5^((Δ₁+Δ₂)/h)`).
    /// `half_life_ms == 0` is treated as no-decay (only the baseline stamp). Runs under
    /// the held write guard.
    pub fn decay_node(&mut self, id: &str, now_ms: u64, half_life_ms: u64) -> bool {
        let Some(obj) = self.node_object(id) else {
            return false;
        };
        // Untouched: no memory-value `importance` ⇒ not a decayable memory node.
        let Some(importance) = obj.get("importance").and_then(|v| v.as_f64()) else {
            return false;
        };
        let reference = obj
            .get("last_decay_ms")
            .and_then(|v| v.as_u64())
            .or_else(|| obj.get("last_access_ms").and_then(|v| v.as_u64()));
        let mut updates = serde_json::Map::new();
        if let Some(ref_ms) = reference {
            let elapsed = now_ms.saturating_sub(ref_ms);
            if elapsed > 0 && half_life_ms > 0 {
                let factor = 0.5_f64.powf(elapsed as f64 / half_life_ms as f64);
                updates.insert(
                    "importance".to_string(),
                    serde_json::json!(importance * factor),
                );
            }
        }
        // Advance decay's own clock — makes the pass idempotent + composable.
        updates.insert("last_decay_ms".to_string(), serde_json::json!(now_ms));
        self.merge_fields(id, &updates)
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — batch [`GraphTxn::decay_node`] over a caller-supplied working
    /// set `ids` (LOCALIZED — the caller picks the set; the engine never scans the
    /// whole store). Returns the number of nodes actually decayed/stamped. The effect
    /// is order-independent (each node is decayed against its own clock). Runs under
    /// the held write guard.
    pub fn decay_memories(&mut self, now_ms: u64, half_life_ms: u64, ids: &[String]) -> usize {
        let mut n = 0;
        for id in ids {
            if self.decay_node(id, now_ms, half_life_ms) {
                n += 1;
            }
        }
        n
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — FORGET a single memory `id` locally. With `delete == false`
    /// (the default, provenance-preserving path) the node is MARKED `forgotten = true`
    /// — its bitemporal history + edges stay intact for audit / recall-on-reinforce.
    /// With `delete == true` the node (and its edges) are hard-removed. A missing node
    /// is a no-op returning `false`. Deterministic; marking is idempotent. Runs under
    /// the held write guard.
    pub fn forget(&mut self, id: &str, delete: bool) -> bool {
        if delete {
            if !self.topo.node_map.contains_key(id) {
                return false;
            }
            self.remove_node(id.to_string());
            true
        } else {
            let mut updates = serde_json::Map::new();
            updates.insert("forgotten".to_string(), serde_json::json!(true));
            self.merge_fields(id, &updates)
        }
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — EVICT every memory in the working set `ids` whose (already-
    /// decayed) `importance` has fallen strictly BELOW `threshold`, pruning it via
    /// [`GraphTxn::forget`] (mark when `delete == false`, hard-remove when `true`). A
    /// node carrying no current-schema `importance` is skipped rather than assigned a
    /// synthetic score. A node already marked `forgotten = true` is skipped (not re-pruned). LOCALIZED (only
    /// `ids` are considered — no full scan) and deterministic; returns the pruned ids,
    /// sorted + deduped. Callers typically [`GraphTxn::decay_memories`] first. Runs
    /// under the held write guard.
    pub fn evict_below(&mut self, ids: &[String], threshold: f64, delete: bool) -> Vec<String> {
        let mut pruned: Vec<String> = Vec::new();
        for id in ids {
            let Some(obj) = self.node_object(id) else {
                continue; // absent/undecodable — nothing to evict.
            };
            if obj.get("forgotten").and_then(|v| v.as_bool()) == Some(true) {
                continue; // already forgotten — idempotent skip.
            }
            if Self::memory_importance(&obj).is_some_and(|importance| importance < threshold)
                && self.forget(id, delete)
            {
                pruned.push(id.clone());
            }
        }
        pruned.sort();
        pruned.dedup();
        pruned
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — the combined maintenance primitive the AU maintenance loop
    /// schedules over a working set: DECAY every node in `ids` (against its own clock),
    /// then EVICT those that fell below `evict_threshold`. LOCALIZED (only `ids`),
    /// atomic (one held write guard), deterministic + idempotent at a fixed `now_ms`.
    /// `delete` selects mark (`false`, the default, provenance-preserving) vs
    /// hard-remove (`true`) for eviction. Returns `(decayed_count, pruned_ids)`. Runs
    /// under the held write guard.
    pub fn maintain(
        &mut self,
        ids: &[String],
        now_ms: u64,
        half_life_ms: u64,
        evict_threshold: f64,
        delete: bool,
    ) -> (usize, Vec<String>) {
        let decayed = self.decay_memories(now_ms, half_life_ms, ids);
        let pruned = self.evict_below(ids, evict_threshold, delete);
        (decayed, pruned)
    }
}
