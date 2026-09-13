use super::*;

impl GraphCore {
    // ── Graph Forking ────────────────────────────────────────────────────

    /// Deep-clone into a new, independent LIVE graph (fresh locks).
    pub fn fork(&self) -> GraphCore {
        let topo = self.topo.read();
        let mut fork = Self::empty();
        self.copy_fork_payload(&mut fork, &topo);
        self.copy_fork_bloom(&mut fork);
        self.copy_fork_schema(&mut fork);
        fork.ledger_dropped_total = std::sync::atomic::AtomicU64::new(
            self.ledger_dropped_total
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        fork
    }

    fn copy_fork_payload(&self, fork: &mut GraphCore, topo: &Topology) {
        fork.topo = RwLock::new(topo.clone());
        fork.node_properties = self
            .node_properties
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        fork.edge_properties = self
            .edge_properties
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        fork.ledger = Mutex::new(self.ledger.lock().clone());
        fork.semantic_store = RwLock::new(self.semantic_store.read().clone());
        fork.integrity_policy = RwLock::new(self.integrity_policy.read().clone());
    }

    fn copy_fork_bloom(&self, fork: &mut GraphCore) {
        let filter = crate::bloom::NodeBloomFilter::new(self.node_properties.len(), 0.01);
        for id in self.node_properties.iter().map(|entry| entry.key().clone()) {
            filter.insert(&id);
        }
        fork.node_bloom = RwLock::new(filter);
        fork.bloom_complete = std::sync::atomic::AtomicBool::new(true);
    }

    fn copy_fork_schema(&self, fork: &mut GraphCore) {
        fork.schema_refs = self
            .schema_refs
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
    }
}
