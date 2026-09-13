use super::*;

impl<'a> GraphTxn<'a> {
    /// Push one entry onto the ledger, applying `LEDGER_CAP`'s drop-oldest-
    /// half policy (BUG A1 follow-up, 2026-08-12). Every production mutation
    /// (`add_node`/`remove_node`/`add_edge`/`remove_edge`/... below) records
    /// through here via `push_ledger_impl`, so the cap + drop-accounting
    /// policy lives in exactly one place.
    pub(super) fn push_ledger(&self, entry: String) {
        push_ledger_impl(self.ledger, self.ledger_dropped_total, entry);
    }

    /// Test node membership through the already-held topology guard. Compound
    /// mutations use this to validate their complete state transition before the
    /// first row is changed, without a second (recursive) graph lock.
    pub fn has_node(&self, node_id: &str) -> bool {
        self.topo.node_map.contains_key(node_id)
    }

    /// Clone one resident node property blob without reacquiring the topology lock.
    /// Compound mutations use this accessor while this transaction already holds
    /// the graph's write guard.
    pub fn get_node_properties(&self, node_id: &str) -> Option<Vec<u8>> {
        self.node_properties
            .get(node_id)
            .map(|properties| (**properties).clone())
    }

    /// Node count from the already-held topology guard. Index maintenance uses
    /// this instead of recursively acquiring `GraphCore`'s topology lock.
    pub fn node_count(&self) -> usize {
        self.topo.graph.node_count()
    }

    /// Edge count from the already-held topology guard. See [`Self::node_count`].
    pub fn edge_count(&self) -> usize {
        self.topo.graph.edge_count()
    }

    // ── Node CRUD (under the held topology write guard) ──────────────────

    pub fn add_node(&mut self, node_id: String, properties_msgpack: Vec<u8>) {
        if !self.topo.node_map.contains_key(&node_id) {
            let new_idx = self.topo.graph.add_node(node_id.clone());
            self.topo.node_map.insert(node_id.clone(), new_idx);
        }
        let log = format!("ADD_NODE|{}|{}", node_id, HexLedger(&properties_msgpack));
        self.node_properties
            .insert(node_id.clone(), Arc::new(properties_msgpack));
        // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — record every id ever
        // added so a later durable-read-through miss can trust a bloom "no".
        self.node_bloom.read().insert(&node_id);
        self.push_ledger(log);
    }

    /// Insert a node only when its id is absent from the current topology.
    ///
    /// Membership testing and insertion share this transaction's topology write
    /// guard, so two writers racing on the same id cannot both observe absence.
    /// The losing writer leaves both the properties and ledger untouched.
    pub fn create_node_if_absent(&mut self, node_id: String, properties_msgpack: Vec<u8>) -> bool {
        if self.topo.node_map.contains_key(&node_id) {
            return false;
        }
        self.add_node(node_id, properties_msgpack);
        true
    }

    pub fn remove_node(&mut self, node_id: String) {
        if let Some(idx) = self.topo.node_map.get(&node_id).copied() {
            // Derive the exact endpoint pairs from petgraph's adjacency lists before
            // removing the node.  The old `DashMap::retain` walked EVERY edge pair
            // for every single-node delete (O(E)); adjacency makes the work
            // proportional to this node's degree.  A set collapses parallel edges
            // and the incoming/outgoing duplicate of a self-loop.
            let mut incident = std::collections::HashSet::new();
            for edge in self
                .topo
                .graph
                .edges_directed(idx, petgraph::Direction::Incoming)
                .chain(
                    self.topo
                        .graph
                        .edges_directed(idx, petgraph::Direction::Outgoing),
                )
            {
                incident.insert((
                    self.topo.graph[edge.source()].clone(),
                    self.topo.graph[edge.target()].clone(),
                ));
            }
            // Properties first, then topology: a crash mid-remove can never leave
            // a live node index whose properties already vanished (which on reload
            // would resurrect a half-deleted node). Topology is the source of truth.
            self.node_properties.remove(&node_id);
            for key in incident {
                self.edge_properties.remove(&key);
            }
            self.topo.node_map.remove(&node_id);
            self.topo.graph.remove_node(idx);
            self.push_ledger(format!("REMOVE_NODE|{}", node_id));
        }
    }

    /// Drop a set of nodes from the resident projection without recording logical
    /// delete mutations. This is the memory-pressure counterpart of `remove_node`:
    /// authoritative rows remain in the durable store and can be read through or
    /// re-materialized later. Incident edge properties are filtered once for the
    /// whole batch. Endpoint pairs come from the selected nodes' adjacency lists,
    /// avoiding both the old O(evictions × edges) repeated-delete behavior and
    /// an O(all edges) projection scan when only a small resident set is evicted.
    pub fn evict_resident_nodes(&mut self, node_ids: &[String]) -> usize {
        if node_ids.is_empty() {
            return 0;
        }
        let selected: std::collections::HashSet<&str> =
            node_ids.iter().map(String::as_str).collect();
        let selected_nodes: Vec<(&str, NodeIndex)> = selected
            .into_iter()
            .filter_map(|node_id| {
                self.topo
                    .node_map
                    .get(node_id)
                    .copied()
                    .map(|index| (node_id, index))
            })
            .collect();
        let mut incident = std::collections::HashSet::new();
        for (_, index) in &selected_nodes {
            for edge in self
                .topo
                .graph
                .edges_directed(*index, petgraph::Direction::Incoming)
                .chain(
                    self.topo
                        .graph
                        .edges_directed(*index, petgraph::Direction::Outgoing),
                )
            {
                incident.insert((
                    self.topo.graph[edge.source()].clone(),
                    self.topo.graph[edge.target()].clone(),
                ));
            }
        }
        for (node_id, index) in &selected_nodes {
            self.node_properties.remove(*node_id);
            self.topo.node_map.remove(*node_id);
            self.topo.graph.remove_node(*index);
        }
        for key in incident {
            self.edge_properties.remove(&key);
        }
        selected_nodes.len()
    }

    /// Serializable gated remove (CONCEPT:EG-KG.txn.serializable-mutation-gate). Decodes the node's CURRENT
    /// property blob to a row map UNDER the held write guard, re-evaluates
    /// `predicate`, and removes the node only if it still matches — so a compound
    /// `DELETE … WHERE <predicate>` cannot delete a row that a concurrent writer
    /// changed out from under the candidate-id scan. Returns whether it removed.
    /// A missing/undecodable node, or a predicate that no longer holds, is a no-op
    /// returning `false`.
    pub fn remove_node_if(&mut self, node_id: &str, predicate: &eg_types::RowPredicate) -> bool {
        let map = match self.node_row_map(node_id) {
            Some(m) => m,
            None => return false,
        };
        if !predicate.eval(&map) {
            return false;
        }
        self.remove_node(node_id.to_string());
        true
    }

    /// Decode a node's stored property blob into a `col -> value` row map for
    /// predicate evaluation (CONCEPT:EG-KG.txn.serializable-mutation-gate). The synthetic `id` column is injected
    /// (the blob stores only properties, not the node id) so a predicate may
    /// reference `id` alongside property columns. `None` if absent/undecodable.
    pub(super) fn node_row_map(
        &self,
        node_id: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let bytes = self.node_properties.get(node_id)?.value().clone();
        let val = decode_property_value(&bytes).ok()?;
        let mut map = match val {
            serde_json::Value::Object(o) => o,
            _ => return None,
        };
        map.entry("id".to_string())
            .or_insert_with(|| serde_json::Value::String(node_id.to_string()));
        Some(map)
    }
}
