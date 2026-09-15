use super::*;

impl GraphCore {
    pub(super) fn drop_if_stale<T>(
        cache: &RwLock<Option<T>>,
        stamp: &std::sync::atomic::AtomicU64,
        new_version: u64,
    ) {
        let mut guard = cache.write();
        if guard.is_some() && stamp.load(std::sync::atomic::Ordering::Acquire) < new_version {
            *guard = None;
        }
    }

    /// Invalidate cached query results for a CHANGE that landed elsewhere
    /// (CONCEPT:EG-KG.coordination.distributed-cache-coherence — distributed cache coherence). A replica tailing the CDC
    /// feed calls this when it observes a REMOTE write for this graph: it bumps the
    /// local `version` (so any cached result keyed on the old version becomes
    /// unreachable) AND drops the cache directly (belt-and-braces — the local data
    /// itself may be rehydrated separately). Unlike `mark_dirty`, this is for a write
    /// that did NOT flow through this node's local write path, so the version must be
    /// advanced explicitly to retire stale cached reads.
    #[cfg(feature = "result-cache")]
    pub fn invalidate_for_remote_change(&self) {
        let new_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        self.result_cache.invalidate_all();
        // The dependency clock backs the SAME result cache; a remote change is un-attributable
        // here (its footprint landed on another node), so floor every dependency dimension at the
        // new version — no dependency-scoped entry may survive it (W1.6/P7).
        self.dep_clock.invalidate_all(new_version);
        // The label/property/path caches are derived state; retire them too so a
        // subsequent local read after a remote-applied change rebuilds them. Same
        // carved block as `mark_dirty` (CONCEPT:EG-KG.storage.index-manager-seam).
        self.invalidate_indexes();
        self.invalidate_edge_key_index();
        // CONCEPT:EG-KG.compute.cdc-event-emit — a replicated write must also wake local live-query
        // subscribers, so a subscription reflects remote writes, not just local ones.
        self.changes.emit(new_version);
    }

    /// Current OCC write-version (CONCEPT:EG-KG.txn.occ-graph-core). A staged transaction snapshots
    /// this at begin and an OCC commit re-reads it under the topology write lock to
    /// detect concurrent writes. Cheap atomic load — never on a read hot path.
    pub fn version(&self) -> u64 {
        self.version.load(std::sync::atomic::Ordering::Acquire)
    }

    /// D-OP-1 / D-OB-20 — look up a cached RLS projection for `actor`, valid only if
    /// it was built at exactly `current_version`. `None` on a cold miss or a stale
    /// (version-mismatched) entry; the caller then builds fresh and stores via
    /// [`Self::put_cached_projection`]. See `crate::rls_projection_cache` for why the
    /// (expensive) build must never happen while this lookup's lock is held.
    ///
    /// `pub` (not `pub(crate)`) so `GraphReadAuthority::project_core`, which lives in
    /// the served-facade crate above `eg-core`, can reach it through the re-exported
    /// `graph` module (`src/lib.rs`'s `pub use eg_core::{..., graph, ...}`) — not part
    /// of a stable external API, just this crate's own internal facade boundary.
    #[cfg(feature = "security")]
    pub fn cached_projection(&self, actor: &str, current_version: u64) -> Option<Arc<GraphCore>> {
        self.rls_projection_cache.get(actor, current_version)
    }

    /// U-142/U-143/U-145 (BUG-130) — the current whole-image generation of the
    /// projection cache. A caller MUST capture this immediately before starting an
    /// (expensive, unlocked) [`Self::put_cached_projection`] build and pass the same
    /// value back to it, so a whole-image replacement/clear that lands mid-build
    /// (`Self::invalidate_projection_cache`, called from `replace_snapshot`/`clear`)
    /// discards that build's now-stale result instead of publishing it. See
    /// `crate::rls_projection_cache`'s `generation` field doc for the full rationale.
    #[cfg(feature = "security")]
    pub fn projection_cache_generation(&self) -> u64 {
        self.rls_projection_cache.generation()
    }

    /// D-OP-1 / D-OB-20 — store a freshly built RLS projection for `actor` at
    /// `version`. Call this AFTER the projection is fully built, never while
    /// holding any lock the build itself needed. See [`Self::cached_projection`]'s
    /// doc for why this is `pub`. `generation` must be the value
    /// [`Self::projection_cache_generation`] returned right before the build started
    /// (U-145) — a mismatch at store time means a whole-image replacement invalidated
    /// the cache while this build was in flight, and the result is dropped rather
    /// than published.
    #[cfg(feature = "security")]
    pub fn put_cached_projection(
        &self,
        actor: String,
        version: u64,
        generation: u64,
        projected: Arc<GraphCore>,
    ) {
        self.rls_projection_cache
            .put(actor, version, generation, projected);
    }

    /// U-142/U-143/U-145 (BUG-130) — advance the projection cache's whole-image
    /// generation and drop every cached entry. Called from every whole-image
    /// transition that does not already imply a `version()` bump the cache's
    /// (actor, version) key would itself catch: [`Self::replace_snapshot`]
    /// (same-version resident-image replacement/reconciliation) and [`Self::clear`]
    /// (and [`Self::hibernate`], which reuses it) — a wipe that intentionally does
    /// not bump `version` (see that method's own doc). Mirrors the sibling
    /// `result_cache.invalidate_all()` call already present at both sites; unlike
    /// that cache, this one is per-actor and `security`-only, hence the separate
    /// entry point.
    #[cfg(feature = "security")]
    pub(super) fn invalidate_projection_cache(&self) {
        self.rls_projection_cache.invalidate_all();
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — look up a cached
    /// RLS-filtered `GraphView` for `actor`, valid only at exactly `current_version`.
    /// Mirrors [`Self::cached_projection`] exactly, one layer lighter. See
    /// `crate::rls_view_cache` for the full rationale.
    #[cfg(feature = "security")]
    pub fn cached_filtered_view(
        &self,
        actor: &str,
        current_version: u64,
    ) -> Option<Arc<GraphView>> {
        self.rls_view_cache.get(actor, current_version)
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — the filtered-view cache's
    /// current whole-image generation. Capture BEFORE starting an unlocked
    /// `analysis_snapshot_versioned` + `filter_view` build and hand the same value back
    /// to [`Self::put_cached_filtered_view`]. Mirrors [`Self::projection_cache_generation`].
    #[cfg(feature = "security")]
    pub fn filtered_view_cache_generation(&self) -> u64 {
        self.rls_view_cache.generation()
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — store a freshly filtered
    /// view for `actor` at `version`, only if `generation` is still current. Mirrors
    /// [`Self::put_cached_projection`] exactly.
    #[cfg(feature = "security")]
    pub fn put_cached_filtered_view(
        &self,
        actor: String,
        version: u64,
        generation: u64,
        view: Arc<GraphView>,
    ) {
        self.rls_view_cache.put(actor, version, generation, view);
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — advance the filtered-view
    /// cache's whole-image generation and drop every cached entry. Called alongside
    /// [`Self::invalidate_projection_cache`] at every one of its call sites (same
    /// same-version whole-image transitions apply identically to this cache).
    #[cfg(feature = "security")]
    pub(super) fn invalidate_filtered_view_cache(&self) {
        self.rls_view_cache.invalidate_all();
    }

    /// Atomically read-and-clear the dirty flag. The checkpoint calls this BEFORE
    /// snapshotting, so a mutation that races the checkpoint re-marks the graph
    /// dirty and is captured by the NEXT checkpoint rather than being lost.
    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    /// Open a write transaction: acquires the topology write lock and borrows the
    /// property maps + ledger. A sequence of mutations under one txn is atomic
    /// w.r.t. other writers (and excludes graph-traversal readers) until it drops.
    /// Single-op convenience methods below open a one-shot txn; multi-op callers
    /// (batch_update, reasoning) hold one txn so the whole batch is atomic.
    pub fn txn(&self) -> GraphTxn<'_> {
        GraphTxn {
            topo: self.topo.write(),
            node_properties: &self.node_properties,
            edge_properties: &self.edge_properties,
            ledger: &self.ledger,
            node_bloom: &self.node_bloom,
            ledger_dropped_total: &self.ledger_dropped_total,
        }
    }
}
