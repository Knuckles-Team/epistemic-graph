use super::*;

impl GraphCore {
    pub fn invalidate_indexes(&self) {
        *self.label_index.write() = None;
        *self.node_id_index.write() = None;
        *self.property_index.write() = None;
        *self.path_index.write() = None;
        // perf/row-visibility-index: a wholesale content replacement (e.g.
        // `replace_snapshot`) invalidates the visibility index exactly like the
        // other node-derived caches above — the warm map would otherwise keep
        // serving decisions for a now-superseded image.
        #[cfg(feature = "security")]
        {
            *self.visibility_index.write() = None;
        }
    }

    /// Invalidate the edge-key keyset-scan cache (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation). Kept
    /// separate from [`Self::invalidate_indexes`] (the NODE-derived caches) because
    /// it must fire on EVERY committed write that touches edges, including the
    /// "pure edge batch" path that deliberately skips the node-derived caches via
    /// [`Self::mark_dirty_preserving_indexes`].
    pub(super) fn invalidate_edge_key_index(&self) {
        *self.edge_key_index.write() = None;
    }

    /// Incrementally maintain the lazy node-derived caches for a committed write batch
    /// (CONCEPT:EG-KG.storage.incremental-index-stamp, W1.6/P7) and — under `result-cache` — record
    /// the batch's dependency footprint on the [`crate::dep_scope::DepClock`] that backs
    /// dependency-scoped result-cache invalidation.
    ///
    /// Where the previous behavior DROPPED the entire label / property index on any node add or
    /// remove (forcing a full O(V) rebuild on the next label/property query — pathological under
    /// continuous ingest), this updates the affected postings in place:
    ///   * ADD — file the new id under every label it carries and every indexed property key it has;
    ///   * REMOVE — unfile the id (from its captured labels/keys, or by scanning the warm postings);
    ///   * UPDATE (CAS) — re-file the id for exactly the changed label / property fields.
    ///
    /// Only a WARM (already-built) cache is touched; a cold cache stays cold and builds on demand.
    /// An un-attributable change — an update with unknown `changed_fields`, or an add whose blob
    /// will not decode — still DROPS the affected cache (the sound conservative fallback) and
    /// coarsely floors the dependency clock. Each cache left warm is STAMPED with `target_version`
    /// so `mark_dirty` recognizes it as already-current and preserves it instead of nuking it. The
    /// JSONPath path-index is not incrementally maintained (arbitrary-path resolution); a node add
    /// or remove drops any warm path-index so the next JSON filter rebuilds it — its pre-W1.6
    /// behavior, and the documented coarse-fallback shape for that cache.
    pub(super) fn invalidate_indexes_for_change(
        &self,
        change: &crate::index::ChangeSet,
        target_version: u64,
    ) {
        #[cfg(feature = "result-cache")]
        let mut footprint = crate::dep_scope::WriteFootprint::default();
        #[cfg(feature = "result-cache")]
        if Self::change_touches_edges(change) {
            // The node-derived caches are untouched by a pure edge change, but a traversal query
            // depends on the edge set, so the clock must learn the edge dimension moved.
            footprint.edge_changed = true;
        }

        if change.has_node_changes() {
            self.maintain_node_id_index(change, target_version);
        }

        // ── ADDS: file the new id into the warm postings it belongs to. ──
        for nc in &change.added_nodes {
            let val = self.node_props_value(&nc.id, nc.properties_msgpack.as_deref());
            self.file_added_node(nc, val.as_ref(), target_version);
            #[cfg(feature = "result-cache")]
            Self::note_added_node(&mut footprint, val.as_ref());
        }

        // ── REMOVES: unfile the id from the warm postings. ──
        for id in &change.removed_nodes {
            let captured = change
                .removed_node_props
                .get(id)
                .and_then(|blob| decode_property_value(blob).ok());
            self.unfile_removed_node(id, captured.as_ref(), target_version);
            #[cfg(feature = "result-cache")]
            Self::note_removed_node(&mut footprint, captured.as_ref());
        }

        // ── UPDATES (CAS): re-file the id for the changed label / property fields only. ──
        for nc in &change.updated_nodes {
            #[cfg(feature = "result-cache")]
            {
                footprint.node_changed = true;
            }
            let Some(fields) = nc.changed_fields.as_ref() else {
                // Unknown-scope update ⇒ drop the node-derived caches (fallback) + floor.
                self.drop_node_derived_caches();
                #[cfg(feature = "result-cache")]
                {
                    footprint.coarse_node = true;
                }
                continue;
            };
            let current = self.node_props_value(&nc.id, None);
            self.refile_updated_node(nc, fields, current.as_ref(), target_version);
            #[cfg(feature = "result-cache")]
            Self::note_updated_node(&mut footprint, fields, current.as_ref());
        }

        #[cfg(feature = "result-cache")]
        self.dep_clock.note_footprint(&footprint, target_version);
    }

    /// Did this change touch the edge set at all?
    #[cfg(feature = "result-cache")]
    pub(super) fn change_touches_edges(change: &crate::index::ChangeSet) -> bool {
        !change.added_edges.is_empty() || !change.removed_edges.is_empty()
    }

    /// File ONE added node into the warm node-derived postings. `val` is its
    /// decoded blob, or `None` when it could not be read — in which case the
    /// label/property postings cannot be targeted and are dropped wholesale.
    pub(super) fn file_added_node(
        &self,
        nc: &crate::index::NodeChange,
        val: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        // perf/row-visibility-index: unlike the label/property postings below,
        // `row_visibility` tolerates an undecodable blob internally (it returns
        // `RowVisibility::default_public()`, the SAME value `can_see_node` would
        // have produced for that node before this index existed) — so there is
        // no "cannot target its postings" failure mode here and no need to drop
        // the whole index on a decode failure. File unconditionally.
        #[cfg(feature = "security")]
        self.visibility_index_set(&nc.id, nc.properties_msgpack.as_deref(), target_version);
        match val {
            Some(val) => {
                self.label_index_add(&nc.id, val, target_version);
                self.property_index_add(&nc.id, val, target_version);
            }
            None => {
                // Cannot read the added node's content ⇒ cannot target its postings; drop.
                *self.label_index.write() = None;
                *self.property_index.write() = None;
            }
        }
        *self.path_index.write() = None;
    }

    /// The dependency-clock footprint of ONE added node.
    #[cfg(feature = "result-cache")]
    pub(super) fn note_added_node(
        footprint: &mut crate::dep_scope::WriteFootprint,
        val: Option<&serde_json::Value>,
    ) {
        footprint.node_changed = true;
        match val {
            Some(val) => collect_dep_footprint(footprint, val),
            None => footprint.coarse_node = true,
        }
    }

    /// Unfile ONE removed node from the warm node-derived postings. `captured` is
    /// the blob the change carried, or `None` for an uncaptured removal — the
    /// maintainers then scan the warm postings for the id (sound; no blob decode).
    pub(super) fn unfile_removed_node(
        &self,
        id: &str,
        captured: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        // perf/row-visibility-index: a flat `id -> RowVisibility` map needs no
        // captured value to unfile — unlike the label/property postings (which
        // need to know WHICH postings to remove `id` from), removal here is a
        // single O(1) delete regardless of what the node's blob contained.
        #[cfg(feature = "security")]
        self.visibility_index_remove(id, target_version);
        self.label_index_remove(id, captured, target_version);
        self.property_index_remove(id, captured, target_version);
        *self.path_index.write() = None;
    }

    /// The dependency-clock footprint of ONE removed node. An uncaptured removal
    /// coarsely floors the clock (its labels/keys are unknown).
    #[cfg(feature = "result-cache")]
    pub(super) fn note_removed_node(
        footprint: &mut crate::dep_scope::WriteFootprint,
        captured: Option<&serde_json::Value>,
    ) {
        footprint.node_changed = true;
        match captured {
            Some(val) => collect_dep_footprint(footprint, val),
            None => footprint.coarse_node = true,
        }
    }
}
