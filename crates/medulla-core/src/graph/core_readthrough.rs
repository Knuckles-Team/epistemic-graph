use super::*;

impl GraphCore {
    pub fn get_nodes_arc(&self) -> Vec<(String, Arc<Vec<u8>>)> {
        self.node_properties
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    pub fn get_node_properties(&self, node_id: &str) -> Option<Vec<u8>> {
        if let Some(a) = self.node_properties.get(node_id) {
            return Some((**a).clone());
        }
        // A durable node may have been
        // evicted from RAM to bound memory (CONCEPT:MEDULLA-KG.storage.read-through-seam-exercised); fetch its stored
        // blob from the durable tier so an evicted node still reads back correctly.
        // Without an installed read-through this is a genuine absence.
        self.read_through_get(node_id)
    }

    /// Consult the durable read-through on a RAM miss (CONCEPT:MEDULLA-KG.storage.read-through-seam-exercised). Returns
    /// the node's stored property blob from the durable tier, or `None` when no
    /// read-through is attached (default model) or the node is genuinely absent.
    /// Kept lock-scoped: the read-through guard is cloned out and released BEFORE
    /// the (possibly blocking) durable point-read so it never holds the lock across
    /// I/O. Serve-only — it does NOT repopulate RAM, so a full scan over evicted
    /// nodes cannot re-grow the resident set past the cap (memory stays bounded).
    pub(super) fn read_through_get(&self, node_id: &str) -> Option<Vec<u8>> {
        let rt = self.read_through.read().clone()?;
        // CONCEPT:MEDULLA-KG.storage.bloom-negative-lookup-guard — only trust a bloom "no"
        // once `node_bloom` reflects the COMPLETE durable node-id set (never during
        // a paged-lazy-open still in progress: CONCEPT:MEDULLA-KG.sharding.paged-lazy-open); a not-yet-
        // paged-in-but-durable node would otherwise be wrongly reported absent. When
        // incomplete this is a no-op guard and behavior is byte-for-byte the
        // pre-bloom original: every miss falls through to the durable point-read.
        if self
            .bloom_complete
            .load(std::sync::atomic::Ordering::Acquire)
            && !self.node_bloom.read().might_contain(node_id)
        {
            return None;
        }
        rt.read_node_blob(node_id)
    }

    /// Mark `node_bloom` as reflecting the COMPLETE known durable node-id set for
    /// this graph (CONCEPT:MEDULLA-KG.storage.bloom-negative-lookup-guard). Called by the registry once a
    /// full/eager load or the FINAL page of a paged-lazy-open has replayed every
    /// durable node through `add_node` — before that point `read_through_get`
    /// never gates on the filter, so an in-progress paged open cannot regress.
    #[doc(hidden)]
    pub fn mark_bloom_complete(&self) {
        self.bloom_complete
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn node_count(&self) -> usize {
        self.topo.read().node_map.len()
    }

    /// Return all node IDs without properties (lightweight enumeration).
    pub fn node_ids(&self) -> Vec<String> {
        self.topo.read().node_map.keys().cloned().collect()
    }

    // ── Edge CRUD ────────────────────────────────────────────────────────
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, Weak};

    #[derive(Debug)]
    struct LockCheckingStore {
        core: Weak<GraphCore>,
        calls: Mutex<Vec<String>>,
    }

    impl crate::read_through::ReadThrough for LockCheckingStore {
        fn read_node_blob(&self, node_id: &str) -> Option<Vec<u8>> {
            let core = self.core.upgrade().expect("graph remains alive");
            assert!(
                core.read_through.try_write().is_some(),
                "the seam lock must be released before durable access"
            );
            self.calls.lock().unwrap().push(node_id.to_owned());
            match node_id {
                "cold" => Some(vec![0, 255, 7]),
                "not-yet-loaded" => Some(vec![9, 0, 128]),
                _ => None,
            }
        }
    }

    #[test]
    fn evicted_reads_release_the_seam_and_never_repopulate_residency() {
        let core = Arc::new(GraphCore::new());
        core.add_node("hot".to_owned(), vec![1, 2]);
        core.add_node("cold".to_owned(), vec![0, 255, 7]);
        let store = Arc::new(LockCheckingStore {
            core: Arc::downgrade(&core),
            calls: Mutex::new(Vec::new()),
        });
        core.set_read_through(store.clone());
        assert_eq!(core.get_node_properties("hot"), Some(vec![1, 2]));
        assert!(store.calls.lock().unwrap().is_empty());

        assert_eq!(core.evict_resident_nodes(&["cold".to_owned()]), 1);
        let resident_count = core.node_properties.len();
        assert_eq!(resident_count, 1);
        for _ in 0..3 {
            assert_eq!(core.get_node_properties("cold"), Some(vec![0, 255, 7]));
            assert_eq!(core.node_properties.len(), resident_count);
            assert!(!core.node_properties.contains_key("cold"));
        }

        // An incomplete identity set cannot suppress a durable read for a node
        // not yet added to the Bloom filter during paged recovery.
        assert_eq!(
            core.get_node_properties("not-yet-loaded"),
            Some(vec![9, 0, 128])
        );
        assert_eq!(core.node_properties.len(), resident_count);
        assert_eq!(core.get_node_properties("absent"), None);
        // Recovery now accounts for the remaining durable identity before
        // declaring the Bloom set complete; the projection stays evicted.
        core.add_node("not-yet-loaded".to_owned(), vec![9, 0, 128]);
        assert_eq!(core.evict_resident_nodes(&["not-yet-loaded".to_owned()]), 1);
        assert_eq!(core.node_properties.len(), resident_count);
        core.mark_bloom_complete();
        assert_eq!(core.get_node_properties("cold"), Some(vec![0, 255, 7]));
        assert_eq!(core.get_node_properties("absent"), None);
        assert_eq!(core.node_properties.len(), resident_count);
        assert_eq!(
            *store.calls.lock().unwrap(),
            ["cold", "cold", "cold", "not-yet-loaded", "absent", "cold"]
        );
    }
}
