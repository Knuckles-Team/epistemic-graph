use super::*;

impl GraphCore {
    fn path_is_affected(path: &str, changed_fields: &[String]) -> bool {
        match crate::jsonpath::parse_path(path).and_then(|parts| parts.into_iter().next()) {
            Some(crate::jsonpath::Segment::Key(key)) => changed_fields.iter().any(|f| f == &key),
            _ => true,
        }
    }

    fn path_index_is_affected(index: &PathIndex, changed_fields: &[String]) -> bool {
        index
            .by_value
            .keys()
            .any(|path| Self::path_is_affected(path, changed_fields))
    }

    /// The pre-W1.6 scoped path-index invalidation for a CAS: drop a warm JSONPath index only if
    /// a changed field could feed one of its indexed paths (an untargeted / root path observes any
    /// top-level change). The path index is not incrementally maintained; this preserves its
    /// unaffected-retention behavior for updates.
    pub(super) fn path_index_invalidate_for_fields(
        &self,
        changed_fields: &[String],
        target_version: u64,
    ) {
        let mut index = self.path_index.write();
        if index.is_none() {
            return;
        }
        let affected = index
            .as_ref()
            .is_some_and(|index| Self::path_index_is_affected(index, changed_fields));
        if affected {
            *index = None;
        } else {
            // The CAS could not feed any indexed path — the warm path index stays valid. Stamp it
            // forward so `mark_dirty` recognizes it as current and preserves it (W1.6/P7); without
            // the stamp, the stamp-aware nuke would drop this deliberately-retained cache.
            self.index_stamps
                .path
                .store(target_version, std::sync::atomic::Ordering::Release);
        }
    }

    /// Maintain the secondary indexes after a committed write batch
    /// (CONCEPT:EG-KG.storage.write-changeset). This convenience is for callers
    /// outside a topology guard. Lock-owning compound paths call
    /// [`Self::maintain_indexes_at`] with counts read from their [`GraphTxn`], so a
    /// concurrent hybrid read never observes a torn index and completeness
    /// publication never recursively acquires the topology lock.
    ///
    /// Route `change` through the [`IndexManager`] seam —
    ///    each heavy index applies its own delta (the vector store tombstones the
    ///    embeddings of removed nodes, CONCEPT:EG-KG.storage.incremental-ann) — then invalidate only
    ///    lazy label/property/path caches covered by the exact node-field change.
    pub fn maintain_indexes(
        &self,
        change: &crate::index::ChangeSet,
    ) -> crate::index::BatchMaintenance {
        let outcome = self.index_manager.commit_batch(self, change);
        // Label/property/JSON-path postings derive only from nodes. Pure edge
        // batches preserve those potentially expensive warm caches. This convenience runs
        // outside a topology guard, so it predicts the batch's committed version as the next
        // one — an under-estimate is safe (it only risks an extra rebuild, never a stale read).
        self.invalidate_indexes_for_change(change, self.version().saturating_add(1));
        outcome
    }

    /// Maintain indexes for one compound mutation whose externally visible graph
    /// version advances once, regardless of its internal operation count.
    /// `node_count` and `edge_count` come from the caller's held [`GraphTxn`], so
    /// publishing completeness never recursively acquires the topology lock.
    pub fn maintain_indexes_at(
        &self,
        change: &crate::index::ChangeSet,
        target_version: u64,
        node_count: usize,
        edge_count: usize,
    ) -> crate::index::BatchMaintenance {
        let outcome = self.index_manager.commit_batch_at(
            self,
            change,
            target_version,
            node_count as u64,
            edge_count as u64,
        );
        self.invalidate_indexes_for_change(change, target_version);
        outcome
    }
}
