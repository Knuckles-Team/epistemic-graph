use super::*;

impl GraphCore {
    pub fn clear(&self) {
        // One write txn freezes structure; properties cleared under it so no reader
        // sees a half-cleared graph.
        let mut topo = self.topo.write();
        topo.graph.clear();
        topo.node_map.clear();
        self.node_properties.clear();
        self.edge_properties.clear();
        self.ledger.lock().clear();
        *self.semantic_store.write() = crate::compute::semantic::SemanticStore::new();
        // Drop every cached query result (CONCEPT:EG-KG.coordination.distributed-cache-coherence): `clear` (and `hibernate`,
        // which reuses it) wipes the graph WITHOUT bumping `version`, so the
        // version-keyed cache must be invalidated directly or a post-wipe lookup at the
        // unchanged version could serve a stale result.
        #[cfg(feature = "result-cache")]
        {
            self.result_cache.invalidate_all();
            // Wiping the graph also retires the dependency clock's per-dimension history (W1.6/P7).
            self.dep_clock.reset();
        }
        // U-142/U-143/U-145 (BUG-130): same reasoning as `result_cache` above, applied
        // to the per-actor RLS projection cache — `clear`/`hibernate` wipe the image
        // WITHOUT bumping `version`, so `(actor, version)` alone would keep serving a
        // pre-wipe projection as "current". See `Self::invalidate_projection_cache`.
        #[cfg(feature = "security")]
        self.invalidate_projection_cache();
        // perf/cold-query-floor-analysis (UNCOMPILED proposal): same reasoning, applied
        // to the filtered-view cache.
        #[cfg(feature = "security")]
        self.invalidate_filtered_view_cache();
    }

    /// Hibernate this graph's in-memory state (CONCEPT:EG-KG.storage.100m-tenant — cold-tenant
    /// hibernation). Drops the whole in-RAM topology / node+edge properties /
    /// semantic vectors — exactly what [`Self::clear`] frees — to reclaim a COLD
    /// tenant's memory, WITHOUT touching the durable redb tier (the caller guarantees
    /// the graph is durable first, the SAME durability-gate eviction uses). The
    /// `read_through` seam is left INTACT, so an evicted node's properties still read
    /// from redb on a RAM miss; a full topology/edge view requires a rehydrate.
    /// Reuses `clear`'s single-write-txn atomicity so no reader sees a half-state.
    /// Returns the node count freed (for observability).
    pub fn hibernate(&self) -> usize {
        let freed = self.node_count();
        self.clear();
        freed
    }

    /// Offload this graph's whole in-RAM state to a cold tier (CONCEPT:EG-KG.coordination.distributed-cache-coherence),
    /// then hibernate the RAM. Serializes the graph (`to_msgpack`), pushes it to the
    /// cold store, and only then drops the RAM — so the bytes are safely in the cold
    /// tier before RAM is freed. Returns the node count freed. The NEXT access calls
    /// [`Self::rehydrate_from_cold`] to bring it back. Read-mostly cold tenants thus
    /// spill RAM→redb→object-store and back without data loss.
    #[cfg(feature = "cold-tier")]
    pub fn offload_to_cold(
        &self,
        graph_name: &str,
        tier: &dyn crate::cold_tier::ColdTier,
    ) -> Result<usize, String> {
        let bytes = self.to_msgpack()?;
        tier.offload(graph_name, &bytes)?;
        Ok(self.hibernate())
    }

    /// Rehydrate this graph from the cold tier (CONCEPT:EG-KG.coordination.distributed-cache-coherence): fetch its
    /// offloaded blob and reload it via `from_msgpack`, then drop the cold copy. A
    /// no-op returning `Ok(false)` when the graph was not offloaded (already hot or
    /// never cold). On success the graph's full topology/edges/properties/vectors are
    /// back in RAM, exactly as before the offload.
    #[cfg(feature = "cold-tier")]
    pub fn rehydrate_from_cold(
        &self,
        graph_name: &str,
        tier: &dyn crate::cold_tier::ColdTier,
    ) -> Result<bool, String> {
        match tier.rehydrate(graph_name)? {
            Some(bytes) => {
                self.from_msgpack(&bytes)?;
                tier.remove(graph_name)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    // ── Ledger Operations ────────────────────────────────────────────────
    //
    // BUG A1 follow-up (2026-08-12): `ledger` is a purely IN-MEMORY,
    // ephemeral buffer -- it is NOT part of the durable path. `GetLedger`
    // reproduced returning `[]` after real, durably-committed mutations
    // because this crate's `security`-active RLS projection served an
    // unrelated no-ledger detached core (see `GetLedger`'s server handler
    // and `access::build_projection`'s doc for that root cause, fixed
    // separately) -- but even with that fixed, `ledger` itself can still be
    // emptied or truncated while the underlying mutations remain fully
    // durable in redb: cold-tenant idle offload/hibernate,
    // `MAX_RESIDENT_GRAPHS` eviction + lazy rehydrate, a process restart, or
    // simply exceeding `LEDGER_CAP` (below) all drop history this buffer
    // cannot recover. There is no query fix for that -- it is a property of
    // what this buffer IS (an audit/debug trail, not a change-data-capture
    // log; `CdcHub`, `src/server/cdc.rs`, is a SEPARATE bounded in-memory
    // ring with the identical ephemerality). The honest fix is to make that
    // fact OBSERVABLE rather than silent: `push_ledger` counts every
    // capacity-driven drop, `ledger_watermark` exposes the running total, and
    // `Method::GetLedger`'s response carries it on every read so a caller can
    // DETECT truncation (watermark increased since its last read) instead of
    // inferring completeness from a merely-nonzero read.

    pub fn get_ledger(&self) -> Vec<String> {
        self.ledger.lock().clone()
    }

    /// The 0-based sequence of the OLDEST ledger entry [`Self::get_ledger`]
    /// can currently vouch for (BUG A1 follow-up) -- i.e. how many entries
    /// have been permanently dropped from the front of this `GraphCore`
    /// instance's in-memory ledger by `LEDGER_CAP`'s trim policy. `0` means
    /// nothing has been dropped BY THIS INSTANCE -- it does not by itself
    /// prove no eviction/rehydrate/restart occurred (a freshly rehydrated or
    /// restarted graph's ledger legitimately starts at `0` again, with no
    /// memory that it was ever nonzero); the actionable signal for a caller
    /// is the watermark DECREASING or FAILING TO ADVANCE the way it expects
    /// across reads it knows straddled real mutations, not a bare snapshot
    /// value in isolation.
    pub fn ledger_watermark(&self) -> u64 {
        self.ledger_dropped_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[doc(hidden)]
    pub fn ledger_len(&self) -> usize {
        self.ledger.lock().len()
    }

    /// Replace only the changed suffix of an authenticated staged ledger image.
    /// The graph-row delta validates its source length before any topology write.
    #[doc(hidden)]
    pub fn replace_ledger_suffix(&self, retain: usize, append: &[String]) -> Result<(), String> {
        let mut ledger = self.ledger.lock();
        if retain > ledger.len() {
            return Err("graph row delta ledger prefix is invalid".to_string());
        }
        ledger.truncate(retain);
        ledger.extend_from_slice(append);
        Ok(())
    }

    pub fn clear_ledger(&self) {
        self.ledger.lock().clear();
    }

    pub fn apply_ledger(&self, transactions: Vec<String>) -> Result<(), String> {
        // Replay the whole batch under one write txn (atomic + no per-op re-lock).
        //
        // A ledger entry's property segment is HEX-ENCODED (`HexLedger`, this
        // module, "byte-identical to hex::encode") -- so replaying it requires
        // `hex::decode` back to the original msgpack property bytes, not a raw
        // `.as_bytes()` reinterpretation of the hex TEXT itself (which just
        // re-encodes the ASCII hex-digit characters as a bogus, undecodable
        // "property blob", silently corrupting every node/edge `ApplyLedger`
        // ever replayed -- caught by the durable commit's own bounded-msgpack
        // decode rejecting it as "invalid or exceeds resource limits" the
        // moment the corrupted blob is durably validated).
        let mut txn = self.txn();
        for tx in transactions {
            let parts: Vec<&str> = tx.split('|').collect();
            if parts.is_empty() {
                continue;
            }
            match parts[0] {
                "ADD_NODE" if parts.len() >= 3 => {
                    if let Ok(props) = hex::decode(parts[2]) {
                        txn.add_node(parts[1].to_string(), props);
                    }
                }
                "ADD_EDGE" if parts.len() >= 4 => {
                    if let Ok(props) = hex::decode(parts[3]) {
                        let _ = txn.add_edge(parts[1].to_string(), parts[2].to_string(), props);
                    }
                }
                "REMOVE_NODE" if parts.len() >= 2 => {
                    txn.remove_node(parts[1].to_string());
                }
                "REMOVE_EDGE" if parts.len() >= 3 => {
                    txn.remove_edge(parts[1].to_string(), parts[2].to_string());
                }
                _ => {}
            }
        }
        Ok(())
    }
}
