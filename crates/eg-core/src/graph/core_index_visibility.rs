use super::*;

impl GraphCore {
    // ── perf/row-visibility-index: write-time-maintained RLS visibility cache ──

    /// (Re)file `id`'s RLS visibility in the WARM `visibility_index` from its
    /// CURRENT property blob — the shared operation an ADD and an RLS-key-touching
    /// CAS refile both need (compute-and-insert; there is no separate "refile"
    /// shape the way label/property postings need, since this is a flat
    /// `id -> RowVisibility` map, not a `value -> ids` posting list). No-op when
    /// cold (mirrors every other node-derived cache: a cold cache stays cold and
    /// builds on demand via [`Self::live_visibility_index`]). Stamped.
    ///
    /// `blob` is the freshly-written content when the caller already has it (an
    /// ADD's captured `properties_msgpack`); when `None`, the CURRENT
    /// `node_properties` entry (post-write — mirrors [`Self::node_props_value`]'s
    /// identical fallback chain) is read instead, used by the CAS-refile call site
    /// which does not carry a captured blob.
    ///
    /// Correctness: every entry is produced by calling
    /// [`crate::isolation::row_visibility`] on the RAW blob bytes — the EXACT
    /// function [`crate::isolation::IsolationLayer::can_see_node`] called per-node,
    /// per-query before this index existed. This function never reimplements or
    /// approximates that decode; it only memoizes calls to it, so a warm entry is
    /// byte-for-byte identical to what a cold decode of the same blob would
    /// produce. A node with no readable blob at all (should not occur for an
    /// add/update whose node still exists post-write) resolves to
    /// `RowVisibility::default_public()` — the SAME fallback `can_see_node` used
    /// for a missing blob (`view.node_properties.get(id)` returning `None`), not a
    /// new case.
    #[cfg(feature = "security")]
    pub(super) fn visibility_index_set(&self, id: &str, blob: Option<&[u8]>, target_version: u64) {
        let mut guard = self.visibility_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        let vis = match blob {
            Some(b) => crate::isolation::row_visibility(b),
            None => match self.node_properties.get(id) {
                Some(props) => crate::isolation::row_visibility(props.value().as_slice()),
                None => crate::isolation::RowVisibility::default_public(),
            },
        };
        index.insert(id.to_string(), vis);
        self.index_stamps
            .visibility
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Unfile `id` from the WARM `visibility_index`. No-op when cold. Stamped.
    #[cfg(feature = "security")]
    pub(super) fn visibility_index_remove(&self, id: &str, target_version: u64) {
        let mut guard = self.visibility_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        index.remove(id);
        self.index_stamps
            .visibility
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Lazily build-and-cache [`Self::visibility_index`], returning an owned copy
    /// for a [`GraphView`] snapshot to carry (`GraphView::visibility_index`).
    /// Mirrors [`Self::get_nodes_by_label_page`]'s consult-then-build-once idiom
    /// exactly: a warm index is returned via a cheap clone (cloning `RowVisibility`
    /// structs — small, no allocation-heavy tree walk — not re-decoding); a cold
    /// index pays [`Self::build_visibility_index`]'s ONE full
    /// `row_visibility`-per-node decode pass, stamps itself at the version it
    /// scanned, and publishes the result so every later call (from ANY caller,
    /// building ANY kind of snapshot) sees it warm. From then on the index is kept
    /// current in place by [`Self::invalidate_indexes_for_change`]
    /// (`visibility_index_set`/`_remove`) on every write, so a write-driven
    /// `FilteredViewCache`/`rls_projection_cache` invalidation (which amortize the
    /// FILTERED result, not the per-node decode) is followed by an O(1)-lookup
    /// rebuild here, not a re-decode — this is what makes the query immediately
    /// after a write cheap too, not just the query after that.
    #[cfg(feature = "security")]
    pub(super) fn live_visibility_index(&self) -> HashMap<String, crate::isolation::RowVisibility> {
        {
            let guard = self.visibility_index.read();
            if let Some(idx) = guard.as_ref() {
                return idx.clone();
            }
        }
        let built_at = self.version();
        let built = self.build_visibility_index();
        {
            let mut guard = self.visibility_index.write();
            self.index_stamps
                .visibility
                .store(built_at, std::sync::atomic::Ordering::Release);
            *guard = Some(built.clone());
        }
        built
    }

    /// Scan the node store once and build the RLS visibility index — the O(V)
    /// `row_visibility`-per-node decode pass this index's incremental maintenance
    /// (`visibility_index_set`/`_remove`) exists to pay AT MOST ONCE under
    /// continuous ingest (mirrors [`Self::build_label_index`]'s identical
    /// "rebuild count under ingest ~1" observability shape, tracked separately via
    /// [`Self::visibility_index_rebuilds`] rather than the pre-existing
    /// [`Self::index_rebuilds`] counter so the two signals are never conflated).
    #[cfg(feature = "security")]
    pub(super) fn build_visibility_index(
        &self,
    ) -> HashMap<String, crate::isolation::RowVisibility> {
        let n = self
            .visibility_index_rebuilds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::debug!(
            target: "epistemic_graph::index_rebuild",
            index = "visibility",
            nodes = self.node_properties.len(),
            total_rebuilds = n,
            "full RLS-visibility-index rebuild (cold cache); warm ingest maintains it incrementally"
        );
        self.node_properties
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    crate::isolation::row_visibility(entry.value().as_slice()),
                )
            })
            .collect()
    }

    /// Count of FULL rebuilds of [`Self::visibility_index`] (perf/row-visibility-index).
    /// Observability + test-assertion hook ("the fast path is actually taken") —
    /// never on a read hot path.
    #[cfg(feature = "security")]
    pub fn visibility_index_rebuilds(&self) -> u64 {
        self.visibility_index_rebuilds
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
