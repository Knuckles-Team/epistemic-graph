use super::*;

impl GraphCore {
    pub fn memory_estimate(&self) -> u64 {
        // Per-node structural overhead: a petgraph node + a node_map entry (the id
        // String is counted via its bytes below) + a node_properties slot. Per-edge:
        // a petgraph edge + an edge_properties Vec slot. Calibrated constants, not a
        // measurement — keep them modest so the blob bytes dominate the estimate.
        const NODE_OVERHEAD: u64 = 64;
        const EDGE_OVERHEAD: u64 = 48;

        let mut bytes: u64 = 0;

        // Node property blobs + their id-string bytes.
        for entry in self.node_properties.iter() {
            bytes += entry.key().len() as u64;
            bytes += entry.value().len() as u64;
            bytes += NODE_OVERHEAD;
        }

        // Edge property blobs + the (src, tgt) key bytes.
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            let key_bytes = (src.len() + tgt.len()) as u64;
            for props in entry.value() {
                bytes += key_bytes + props.len() as u64 + EDGE_OVERHEAD;
            }
        }

        // Embedding vectors: sum of each live vector's `len × 4` bytes (f32).
        bytes += self.semantic_store.read().embedding_bytes();

        bytes
    }

    /// In-degree count for a specific node.
    pub fn in_degree(&self, node_id: &str) -> Result<usize, String> {
        self.directed_degree(node_id, petgraph::Direction::Incoming)
    }

    /// How many of `node_id`'s edges run in `direction`. `Err` when the node is unknown.
    pub(super) fn directed_degree(
        &self,
        node_id: &str,
        direction: petgraph::Direction,
    ) -> Result<usize, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        Ok(topo.graph.edges_directed(*idx, direction).count())
    }

    /// The ids at the far end of `node_id`'s edges in `direction`, in edge order and with
    /// duplicates kept (a parallel edge yields its neighbour twice, as it always has).
    /// `Err` when the node is unknown.
    pub(super) fn directed_neighbors(
        &self,
        node_id: &str,
        direction: petgraph::Direction,
    ) -> Result<Vec<String>, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        Ok(topo
            .graph
            .edges_directed(*idx, direction)
            .map(|e| {
                let far = match direction {
                    petgraph::Direction::Incoming => e.source(),
                    petgraph::Direction::Outgoing => e.target(),
                };
                topo.graph[far].clone()
            })
            .collect())
    }

    /// Out-degree count for a specific node.
    pub fn out_degree(&self, node_id: &str) -> Result<usize, String> {
        self.directed_degree(node_id, petgraph::Direction::Outgoing)
    }

    // ── Neighbor Queries ─────────────────────────────────────────────────

    /// Incoming neighbors (predecessors).
    pub fn get_predecessors(&self, node_id: &str) -> Result<Vec<String>, String> {
        self.directed_neighbors(node_id, petgraph::Direction::Incoming)
    }

    /// Outgoing neighbors (successors).
    pub fn get_successors(&self, node_id: &str) -> Result<Vec<String>, String> {
        self.directed_neighbors(node_id, petgraph::Direction::Outgoing)
    }

    /// All neighbors (both directions, deduplicated).
    pub fn get_neighbors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        let mut neighbors = std::collections::HashSet::new();
        for e in topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
        {
            neighbors.insert(topo.graph[e.source()].clone());
        }
        for e in topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
        {
            neighbors.insert(topo.graph[e.target()].clone());
        }
        Ok(neighbors.into_iter().collect())
    }

    /// Batch form of [`Self::get_neighbors`] (D-DPF-1): neighbor ids for MANY
    /// nodes under ONE `topo.read()` acquisition instead of one lock
    /// acquisition per node. A node absent from the graph yields an empty
    /// neighbor list at its position rather than failing the whole batch —
    /// callers that need to distinguish "absent" from "no neighbors" should
    /// pair this with `has_batch`. Order matches `node_ids`.
    pub fn get_neighbors_batch(&self, node_ids: Vec<String>) -> Vec<(String, Vec<String>)> {
        let topo = self.topo.read();
        node_ids
            .into_iter()
            .map(|node_id| {
                let neighbors = match topo.node_map.get(&node_id) {
                    Some(idx) => {
                        let mut set = std::collections::HashSet::new();
                        for e in topo
                            .graph
                            .edges_directed(*idx, petgraph::Direction::Incoming)
                        {
                            set.insert(topo.graph[e.source()].clone());
                        }
                        for e in topo
                            .graph
                            .edges_directed(*idx, petgraph::Direction::Outgoing)
                        {
                            set.insert(topo.graph[e.target()].clone());
                        }
                        set.into_iter().collect()
                    }
                    None => Vec::new(),
                };
                (node_id, neighbors)
            })
            .collect()
    }
}
