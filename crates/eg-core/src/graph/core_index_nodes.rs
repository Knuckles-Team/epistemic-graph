use super::*;

impl GraphCore {
    pub(super) fn drop_node_derived_caches(&self) {
        *self.label_index.write() = None;
        *self.property_index.write() = None;
        *self.path_index.write() = None;
        // perf/row-visibility-index: an unknown-scope CAS could have touched
        // an RLS key just as easily as any other — same conservative
        // whole-index drop as the other node-derived caches above.
        #[cfg(feature = "security")]
        {
            *self.visibility_index.write() = None;
        }
    }

    /// Re-file ONE known-scope CAS into the warm node-derived postings.
    pub(super) fn refile_updated_node(
        &self,
        nc: &crate::index::NodeChange,
        fields: &[String],
        current: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        if Self::changes_label_fields(fields) {
            self.label_index_refile(&nc.id, current, target_version);
        }
        self.property_index_refile(&nc.id, fields, current, target_version);
        self.path_index_invalidate_for_fields(fields, target_version);
        // perf/row-visibility-index: re-file ONLY when a changed field is one of
        // the RLS keys `row_visibility` actually reads (EITHER naming
        // convention — `crate::isolation::RowVisibility`'s doc) — a CAS that
        // left every RLS key untouched cannot have changed the node's
        // visibility decision, so the warm entry stays valid and is left alone
        // (unlike label/property refiling above, which always re-files on ANY
        // known field set because it must recompute regardless).
        #[cfg(feature = "security")]
        if Self::changes_rls_keys(fields) {
            self.visibility_index_set(&nc.id, None, target_version);
        }
    }

    /// The dependency-clock footprint of ONE known-scope CAS. `footprint` is a
    /// pure accumulator consumed once at the end of the sweep, so recording it
    /// after the index re-file (rather than interleaved with it) is equivalent.
    #[cfg(feature = "result-cache")]
    pub(super) fn note_updated_node(
        footprint: &mut crate::dep_scope::WriteFootprint,
        fields: &[String],
        current: Option<&serde_json::Value>,
    ) {
        if Self::changes_label_fields(fields) {
            if let Some(val) = current {
                footprint.labels.extend(labels_of(val));
            }
        }
        footprint.keys.extend(fields.iter().cloned());
    }

    /// Does this CAS touch a field the LABEL index is derived from? Any of them
    /// forces a label re-file (the index cannot know which naming convention the
    /// writer used).
    pub(super) fn changes_label_fields(fields: &[String]) -> bool {
        fields
            .iter()
            .any(|f| matches!(f.as_str(), "type" | "node_type" | "label" | "labels"))
    }

    /// Does this CAS touch any RLS key that `row_visibility` actually reads
    /// (EITHER naming convention — `crate::isolation::RowVisibility`'s doc)? A CAS
    /// that left every RLS key untouched cannot have changed the node's visibility
    /// decision, so its warm entry stays valid.
    #[cfg(feature = "security")]
    pub(super) fn changes_rls_keys(fields: &[String]) -> bool {
        fields.iter().any(|f| {
            f == crate::isolation::RLS_OWNER_KEY
                || f == crate::isolation::RLS_VISIBILITY_KEY
                || f == crate::isolation::RLS_GRANTS_KEY
                || f == crate::isolation::RLS_OWNER_ID_KEY
                || f == crate::isolation::RLS_SHARED_SCOPE_KEY
        })
    }

    /// Decode a node's CURRENT property blob (present after an add/update) to JSON, falling back
    /// to a `fallback` blob captured on the change (used for an add whose node the coalescer
    /// already committed). `None` when neither is present or decodable — the drop-and-rebuild
    /// fallback signal for the incremental maintainers.
    pub(super) fn node_props_value(
        &self,
        id: &str,
        fallback: Option<&[u8]>,
    ) -> Option<serde_json::Value> {
        if let Some(props) = self.node_properties.get(id) {
            if let Ok(val) = decode_property_value(props.value().as_slice()) {
                return Some(val);
            }
        }
        fallback.and_then(|blob| decode_property_value(blob).ok())
    }

    /// Keep the warm unlabeled-scan id cache current in place: insert added ids, remove removed
    /// ids (a CAS leaves the id SET unchanged). Cold ⇒ no-op (builds on demand). Stamped so
    /// `mark_dirty` preserves it.
    pub(super) fn maintain_node_id_index(
        &self,
        change: &crate::index::ChangeSet,
        target_version: u64,
    ) {
        if change.added_nodes.is_empty() && change.removed_nodes.is_empty() {
            return;
        }
        let mut guard = self.node_id_index.write();
        let Some(ids) = guard.as_mut() else {
            return;
        };
        for nc in &change.added_nodes {
            insert_sorted(ids, &nc.id);
        }
        for id in &change.removed_nodes {
            remove_sorted(ids, id);
        }
        self.index_stamps
            .node_id
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// File `id` under every label it carries in the WARM label index (no-op when cold). Stamped.
    pub(super) fn label_index_add(&self, id: &str, val: &serde_json::Value, target_version: u64) {
        let mut guard = self.label_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for label in labels_of(val) {
            insert_sorted(index.entry(label).or_default(), id);
        }
        self.index_stamps
            .label
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Unfile `id` from the WARM label index. With `val` (captured pre-removal properties) only its
    /// own label postings are touched; without it, every posting is scanned for the id (sound but
    /// O(#labels)). No-op when cold. Stamped.
    pub(super) fn label_index_remove(
        &self,
        id: &str,
        val: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        let mut guard = self.label_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        match val {
            Some(val) => {
                for label in labels_of(val) {
                    if let Some(ids) = index.get_mut(&label) {
                        remove_sorted(ids, id);
                    }
                }
            }
            None => {
                for ids in index.values_mut() {
                    remove_sorted(ids, id);
                }
            }
        }
        self.index_stamps
            .label
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Re-file `id` after a label-field CAS: remove it from every posting, then re-add it under
    /// its CURRENT labels. Bounded by the label cardinality. No-op when cold. Stamped.
    pub(super) fn label_index_refile(
        &self,
        id: &str,
        current: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        let mut guard = self.label_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for ids in index.values_mut() {
            remove_sorted(ids, id);
        }
        if let Some(val) = current {
            for label in labels_of(val) {
                insert_sorted(index.entry(label).or_default(), id);
            }
        }
        self.index_stamps
            .label
            .store(target_version, std::sync::atomic::Ordering::Release);
    }
}
