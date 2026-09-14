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
        // evicted from RAM to bound memory (CONCEPT:EG-KG.storage.read-through-seam-exercised); fetch its stored
        // blob from the durable tier so an evicted node still reads back correctly.
        // Without an installed read-through this is a genuine absence.
        self.read_through_get(node_id)
    }

    /// Consult the durable read-through on a RAM miss (CONCEPT:EG-KG.storage.read-through-seam-exercised). Returns
    /// the node's stored property blob from the durable tier, or `None` when no
    /// read-through is attached (default model) or the node is genuinely absent.
    /// Kept lock-scoped: the read-through guard is cloned out and released BEFORE
    /// the (possibly blocking) durable point-read so it never holds the lock across
    /// I/O. Serve-only — it does NOT repopulate RAM, so a full scan over evicted
    /// nodes cannot re-grow the resident set past the cap (memory stays bounded).
    pub(super) fn read_through_get(&self, node_id: &str) -> Option<Vec<u8>> {
        let rt = self.read_through.read().clone()?;
        // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — only trust a bloom "no"
        // once `node_bloom` reflects the COMPLETE durable node-id set (never during
        // a paged-lazy-open still in progress: CONCEPT:EG-KG.sharding.paged-lazy-open); a not-yet-
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
    /// this graph (CONCEPT:EG-KG.storage.bloom-negative-lookup-guard). Called by the registry once a
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
