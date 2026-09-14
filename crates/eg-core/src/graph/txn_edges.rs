use super::*;

impl<'a> GraphTxn<'a> {
    // ── Edge CRUD (under the held topology write guard) ──────────────────

    pub fn add_edge(
        &mut self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        let source_idx = match self.topo.node_map.get(&source_id) {
            Some(&idx) => idx,
            None => return Err(edge_endpoint_not_found("Source", &source_id)),
        };
        let target_idx = match self.topo.node_map.get(&target_id) {
            Some(&idx) => idx,
            None => return Err(edge_endpoint_not_found("Target", &target_id)),
        };
        self.topo.graph.add_edge(
            source_idx,
            target_idx,
            format!("{}:{}", source_id, target_id),
        );
        let log = format!(
            "ADD_EDGE|{}|{}|{}",
            source_id,
            target_id,
            HexLedger(&properties_msgpack)
        );
        self.edge_properties
            .entry((source_id.clone(), target_id.clone()))
            .or_default()
            .push(Arc::new(properties_msgpack));
        self.push_ledger(log);
        Ok(())
    }

    pub fn remove_edge(&mut self, source_id: String, target_id: String) {
        if let (Some(&src_idx), Some(&tgt_idx)) = (
            self.topo.node_map.get(&source_id),
            self.topo.node_map.get(&target_id),
        ) {
            // Edge properties are stored per endpoint pair, so removing only one
            // topology edge while dropping the whole property vector left hidden
            // parallel edges behind. Pair removal (and `upsert_edge`) replaces all.
            let edge_indices: Vec<_> = self
                .topo
                .graph
                .edges_connecting(src_idx, tgt_idx)
                .map(|edge| edge.id())
                .collect();
            for edge_idx in edge_indices {
                self.topo.graph.remove_edge(edge_idx);
            }
            self.edge_properties
                .remove(&(source_id.clone(), target_id.clone()));
            self.push_ledger(format!("REMOVE_EDGE|{}|{}", source_id, target_id));
        }
    }

    /// Atomic compare-and-set on a node's property blob (CONCEPT:EG-KG.compute.backend backend-
    /// agnostic atomic claim). Runs entirely under the held topology write guard
    /// (decode → check → merge → re-encode → write), so the read-modify-write is
    /// atomic w.r.t. other writers. For every `(field, expected)` in `conditions`
    /// the node's current value must equal `expected`, treating a MISSING field as
    /// `null` (so a condition of `null` means "absent or null"). If ALL conditions
    /// hold, every `(field, value)` from `updates` is merged into the object and
    /// `true` is returned. If the node is absent, any condition fails, or the
    /// current blob fails to decode, the node is left untouched and `false` is
    /// returned.
    pub fn compare_and_set_fields(
        &mut self,
        node_id: &str,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        let bytes = match self.node_properties.get(node_id) {
            Some(b) => b.value().clone(),
            None => return false,
        };
        let mut val = match decode_property_value(&bytes) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let obj = match val.as_object_mut() {
            Some(o) => o,
            None => return false,
        };
        // Check every condition against the current value (missing reads as null).
        for (field, expected) in conditions {
            let current = obj.get(field).unwrap_or(&serde_json::Value::Null);
            if current != expected {
                return false;
            }
        }
        // All conditions held — merge updates and write the blob back.
        for (field, value) in updates {
            obj.insert(field.clone(), value.clone());
        }
        let reenc = match rmp_serde::to_vec_named(&val) {
            Ok(b) => b,
            Err(_) => return false,
        };
        let log = format!("CAS_NODE|{}|{}", node_id, HexLedger(&reenc));
        self.node_properties
            .insert(node_id.to_string(), Arc::new(reenc));
        self.push_ledger(log);
        true
    }

    /// Serializable gated compare-and-set (CONCEPT:EG-KG.txn.serializable-mutation-gate). Like
    /// [`GraphTxn::compare_and_set_fields`] but FIRST re-evaluates `predicate`
    /// against the node's CURRENT row (decoded under the held write guard, with the
    /// synthetic `id` column injected). If the predicate no longer holds the node is
    /// left untouched and `false` is returned — this is the serializable re-check for
    /// a compound `UPDATE … WHERE <predicate>` whose candidate ids were resolved by an
    /// earlier (lock-free) read. When the predicate holds, the usual `conditions`
    /// check + `updates` merge run atomically under the same guard.
    pub fn compare_and_set_fields_if(
        &mut self,
        node_id: &str,
        predicate: &eg_types::RowPredicate,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        match self.node_row_map(node_id) {
            Some(map) if predicate.eval(&map) => {}
            _ => return false,
        }
        self.compare_and_set_fields(node_id, conditions, updates)
    }

    /// Non-destructively CLOSE the temporal windows of a contradicted edge
    /// (CONCEPT:AU-KG.ingest.list-durable-media). Sets the matching edge's `valid_until = invalid_at`
    /// (event-time close) and `tx_to = tx_now` (belief retracted) — it does NOT
    /// remove the edge, so an `AS OF` before `invalid_at` still sees the fact and the
    /// `AS OF TX` history of what-we-believed is preserved. Matches the edge(s)
    /// between `(source_id, target_id)` whose canonical `relationship` field equals
    /// `relationship`
    /// and that are not already closed at or before `invalid_at`. Returns how many
    /// edge blobs were updated. Deterministic in its args, so it replays identically
    /// from the WAL / on a Raft follower.
    pub fn invalidate_edge(
        &mut self,
        source_id: &str,
        target_id: &str,
        relationship: &str,
        invalid_at: u64,
        tx_now: u64,
    ) -> usize {
        let key = (source_id.to_string(), target_id.to_string());
        let mut entry = match self.edge_properties.get_mut(&key) {
            Some(e) => e,
            None => return 0,
        };
        let mut updated = 0usize;
        for blob in entry.value_mut().iter_mut() {
            let Ok(mut val) = decode_property_value(blob.as_slice()) else {
                continue;
            };
            let Some(obj) = val.as_object_mut() else {
                continue;
            };
            let rel_matches =
                obj.get("relationship").and_then(|v| v.as_str()) == Some(relationship);
            if !rel_matches {
                continue;
            }
            // Skip an edge already closed at or before this instant (idempotent).
            let already_closed = obj
                .get("valid_until")
                .and_then(|v| v.as_u64())
                .is_some_and(|vu| vu <= invalid_at);
            if already_closed {
                continue;
            }
            obj.insert("valid_until".into(), serde_json::json!(invalid_at));
            obj.insert("tx_to".into(), serde_json::json!(tx_now));
            if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                *blob = Arc::new(reenc);
                updated += 1;
            }
        }
        if updated > 0 {
            self.push_ledger(format!(
                "INVALIDATE_EDGE|{}|{}|{}|{}|{}",
                source_id, target_id, relationship, invalid_at, tx_now
            ));
        }
        updated
    }

    /// Atomically SUPERSEDE a prior edge with a new one (CONCEPT:AU-KG.ingest.list-durable-media) under the
    /// single held write guard: close the prior edge's validity window
    /// (`valid_until = valid_at`, `tx_to = tx_now`) and insert the new edge — never
    /// deleting the prior, so the full history survives. The new edge's blob is
    /// supplied fully-formed by the caller (it should carry `valid_from = valid_at`
    /// and a `supersedes` provenance pointer). Returns `Ok(())` once the new edge is
    /// added (endpoints must exist), after invalidating the prior.
    // The nine args are three irreducible groups of distinct primitives — the new
    // edge (source, target, properties), the prior edge to close (source, target,
    // relationship) and the two bitemporal timestamps (valid_at, tx_now). Bundling
    // them into a struct would add ceremony at every call site without making any
    // group clearer, so a scoped allow is the right call here.
    #[allow(clippy::too_many_arguments)]
    pub fn supersede_edge(
        &mut self,
        new_source: String,
        new_target: String,
        new_properties_msgpack: Vec<u8>,
        prior_source: &str,
        prior_target: &str,
        prior_relationship: &str,
        valid_at: u64,
        tx_now: u64,
    ) -> Result<(), String> {
        self.invalidate_edge(
            prior_source,
            prior_target,
            prior_relationship,
            valid_at,
            tx_now,
        );
        self.add_edge(new_source, new_target, new_properties_msgpack)
    }
}
