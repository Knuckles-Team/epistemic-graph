use super::*;

impl GraphCore {
    pub fn changes(&self) -> &ChangeNotifier {
        &self.changes
    }

    /// Attach a durable read-through (CONCEPT:EG-KG.storage.read-through-seam-exercised). Called once at startup
    /// so a node evicted from RAM is still
    /// served from redb on a RAM miss. A `GraphCore` with no read-through behaves
    /// exactly as before — a miss is a genuine absence.
    pub fn set_read_through(&self, rt: Arc<dyn crate::read_through::ReadThrough>) {
        *self.read_through.write() = Some(rt);
    }

    /// Mark this graph as changed since its last checkpoint (Phase C-C). Called by
    /// the dispatch after any successful write op and by the background decay sweep.
    pub fn mark_dirty(&self) {
        self.mark_dirty_inner(true);
    }

    /// Mark a committed write while retaining node-derived lazy indexes that a
    /// preceding incremental maintenance step proved unaffected (for example a
    /// pure edge batch). This is hidden infrastructure API: callers that do not
    /// carry a complete [`crate::index::ChangeSet`] must use [`mark_dirty`].
    #[doc(hidden)]
    pub fn mark_dirty_preserving_indexes(&self) {
        self.mark_dirty_inner(false);
    }

    pub(super) fn mark_dirty_inner(&self, invalidate_node_indexes: bool) {
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        // Bump the OCC write-version (CONCEPT:EG-KG.txn.occ-graph-core): every committed write —
        // single-op, coalesced batch, and (via the commit path) a multi-op txn —
        // flows through `mark_dirty`, so an in-flight staged transaction's validate
        // step observes any concurrent write that landed since it began. AcqRel so
        // the bump is visible to a commit that reads `version()` under the topo
        // write lock.
        let new_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        // Retire the lazy secondary indexes made stale by this write — but only the ones a
        // preceding `maintain_indexes` did NOT already bring current (W1.6/P7,
        // CONCEPT:EG-KG.storage.incremental-index-stamp). A node add/remove that was incrementally
        // applied to the postings stamps its index at (at least) this version, so
        // `invalidate_node_indexes_if_stale` preserves it instead of dropping + rebuilding it;
        // a write that bypassed maintenance (a stale stamp) still drops it. This is what makes
        // the index-rebuild count under continuous ingest ~0 instead of one-per-write.
        if invalidate_node_indexes {
            self.invalidate_node_indexes_if_stale(new_version);
        }
        // Unconditional (unlike the node-derived caches above): a pure edge batch
        // is exactly the case `mark_dirty_preserving_indexes` exists for, and it
        // is exactly the case that changes this index.
        self.invalidate_edge_key_index();
        // CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation (W1.6/P7) — record the
        // version bump on the dependency clock. It floors (coarsely invalidates every
        // dependency-scoped result-cache entry) ONLY when this version was not already accounted
        // for by a `maintain_indexes` footprint — i.e. only for a write that bypassed the
        // change-set path (a follower's replicated apply). A footprinted write does not floor.
        #[cfg(feature = "result-cache")]
        self.dep_clock.note_version_bump(new_version);
        // CONCEPT:EG-KG.compute.cdc-event-emit — fan out a change notification (post-write version) to any
        // live subscribers (the GraphQL subscription carrier). A single relaxed
        // atomic load when there are none, so this is off the write hot path.
        self.changes.emit(new_version);
    }

    /// Drop each lazy node-derived cache whose STAMP is stale relative to `new_version`
    /// (W1.6/P7, CONCEPT:EG-KG.storage.incremental-index-stamp) — the stamp-aware replacement for the
    /// unconditional `invalidate_indexes` the node-write `mark_dirty` used to call. A cache whose
    /// `maintain_indexes` step already stamped it at `>= new_version` (an incrementally applied
    /// add/remove/CAS) is preserved; a cache left stale by a maintenance-bypassing write is
    /// dropped so the next read rebuilds it. The stamp check runs UNDER the cache's own write lock
    /// so it is atomic with the maintainer's stamp store (no torn stamp-vs-content read).
    pub(super) fn invalidate_node_indexes_if_stale(&self, new_version: u64) {
        Self::drop_if_stale(&self.label_index, &self.index_stamps.label, new_version);
        Self::drop_if_stale(&self.node_id_index, &self.index_stamps.node_id, new_version);
        Self::drop_if_stale(
            &self.property_index,
            &self.index_stamps.property,
            new_version,
        );
        Self::drop_if_stale(&self.path_index, &self.index_stamps.path, new_version);
        // perf/row-visibility-index: same stamp-aware preserve-or-drop treatment as
        // the four caches above — a write whose `invalidate_indexes_for_change` step
        // already incrementally re-filed the touched node(s) (stamp >= new_version)
        // is preserved; a write that bypassed maintenance is dropped.
        #[cfg(feature = "security")]
        Self::drop_if_stale(
            &self.visibility_index,
            &self.index_stamps.visibility,
            new_version,
        );
    }
}
