use super::*;

impl GraphCore {
    // ── Node CRUD (one-shot convenience over `txn`) ──────────────────────

    pub fn add_node(&self, node_id: String, properties_msgpack: Vec<u8>) {
        self.txn().add_node(node_id, properties_msgpack);
    }

    /// Insert a node WITHOUT appending a ledger record (D-EGP-1, t1-grounding-0802).
    ///
    /// `add_node`'s ledger write hex-encodes the ENTIRE property blob byte-by-byte
    /// (`HexLedger`'s `Display` impl does one `write!` per byte) into a `String`,
    /// then pushes it under `self.ledger`'s mutex — real, non-trivial cost when
    /// called thousands of times. This is exactly what
    /// `GraphReadAuthority::build_projection` (`src/server/access.rs`) does when the
    /// RLS projection cache misses: it builds a throwaway `GraphCore` one node/edge
    /// at a time via the ordinary `add_node`/`add_edge`, then immediately
    /// `ledger.lock().clear()`s the whole thing, because the synthesized
    /// `ADD_NODE|...` records are never real source history. Every one of those
    /// records was therefore formatted, hex-encoded, and locked into a `Vec` purely
    /// to be thrown away — on a 27,969-node graph this is tens of thousands of
    /// wasted per-byte `write!` calls on every single cache miss, and was found to
    /// be the dominant cost of the ~2.4-15s rebuild time the miss-cost histogram
    /// (`epistemic_graph_projection_cache_miss_build_seconds`) measured live.
    ///
    /// Use ONLY for synthetic/throwaway construction whose ledger is discarded
    /// before ever being read (`build_projection` today) — never for a mutation
    /// whose ledger must reflect real history. Functionally identical to
    /// `add_node` otherwise (same node_map/graph/node_properties/bloom updates).
    pub fn add_node_no_ledger(&self, node_id: String, properties_msgpack: Vec<u8>) {
        {
            let mut topo = self.topo.write();
            if !topo.node_map.contains_key(&node_id) {
                let new_idx = topo.graph.add_node(node_id.clone());
                topo.node_map.insert(node_id.clone(), new_idx);
            }
        }
        self.node_bloom.read().insert(&node_id);
        self.node_properties
            .insert(node_id, Arc::new(properties_msgpack));
    }

    /// Atomically create `node_id` without overwriting an existing node.
    /// Returns `true` only for the writer that inserted it.
    pub fn create_node_if_absent(&self, node_id: String, properties_msgpack: Vec<u8>) -> bool {
        self.txn()
            .create_node_if_absent(node_id, properties_msgpack)
    }

    pub fn remove_node(&self, node_id: String) {
        self.txn().remove_node(node_id.clone());
        self.semantic_store.write().remove_embedding(&node_id);
    }

    /// Bulk memory-only eviction. Unlike a logical delete this does not append a
    /// `REMOVE_NODE` ledger record or dirty the graph; durable state remains the
    /// authority. Semantic entries are removed under one lock so the projection
    /// releases their resident bytes without lock-per-node overhead.
    pub fn evict_resident_nodes(&self, node_ids: &[String]) -> usize {
        let removed = self.txn().evict_resident_nodes(node_ids);
        if removed > 0 {
            let mut semantic = self.semantic_store.write();
            for node_id in node_ids {
                semantic.remove_embedding(node_id);
            }
        }
        removed
    }
}
