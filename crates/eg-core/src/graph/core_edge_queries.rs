use super::*;

impl GraphCore {
    // ── Edge CRUD (one-shot convenience over `txn`) ──────────────────────

    pub fn add_edge(
        &self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        self.txn()
            .add_edge(source_id, target_id, properties_msgpack)
    }

    /// Edge sibling of [`Self::add_node_no_ledger`] (D-EGP-1, t1-grounding-0802) —
    /// same rationale: `build_projection` inserts every visible edge into a
    /// throwaway `GraphCore` whose ledger is cleared before it is ever read, so
    /// `add_edge`'s per-call `format!("ADD_EDGE|...", HexLedger(...))` + ledger
    /// mutex push is pure waste. Functionally identical to `add_edge` otherwise.
    pub fn add_edge_no_ledger(
        &self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        {
            let mut topo = self.topo.write();
            let source_idx = *topo
                .node_map
                .get(&source_id)
                .ok_or_else(|| edge_endpoint_not_found("Source", &source_id))?;
            let target_idx = *topo
                .node_map
                .get(&target_id)
                .ok_or_else(|| edge_endpoint_not_found("Target", &target_id))?;
            topo.graph.add_edge(
                source_idx,
                target_idx,
                format!("{}:{}", source_id, target_id),
            );
        }
        self.edge_properties
            .entry((source_id, target_id))
            .or_default()
            .push(Arc::new(properties_msgpack));
        Ok(())
    }

    pub fn remove_edge(&self, source_id: String, target_id: String) {
        self.txn().remove_edge(source_id, target_id);
    }

    pub fn has_edge(&self, source_id: &str, target_id: &str) -> bool {
        let topo = self.topo.read();
        if let (Some(&src_idx), Some(&tgt_idx)) =
            (topo.node_map.get(source_id), topo.node_map.get(target_id))
        {
            topo.graph.find_edge(src_idx, tgt_idx).is_some()
        } else {
            false
        }
    }

    /// Unconditionally materializes EVERY edge's property blob (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation) — the
    /// full-graph-dump primitive behind the served `GetEdges` RPC, which guards
    /// this with an edge-count cap before calling it (`oversize_edge_dump_error`
    /// in `graph_ops.rs`) precisely because it has no bound of its own. Callers
    /// that don't need a full-graph snapshot/export/mining pass should prefer
    /// [`Self::get_edges_page`] (bounded, keyset-paginated) instead of adding a
    /// new unguarded caller here.
    pub fn get_edges(&self) -> Vec<(String, String, Vec<u8>)> {
        let mut res = Vec::new();
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            for props in entry.value() {
                res.push((src.clone(), tgt.clone(), (**props).clone()));
            }
        }
        res
    }

    /// Like `get_edges` but clones the Arc pointers — snapshot hot path (C-A).
    /// Same unbounded-materialization caveat; prefer [`Self::get_edges_page`]
    /// for a bounded read.
    pub fn get_edges_arc(&self) -> Vec<(String, String, Arc<Vec<u8>>)> {
        let mut res = Vec::new();
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            for props in entry.value() {
                res.push((src.clone(), tgt.clone(), props.clone()));
            }
        }
        res
    }

    /// Deterministic keyset page over the edge store (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation — the edge
    /// sibling of `get_nodes_by_label_page`'s unlabeled scan). Returns at most
    /// `limit` edges ordered by `(source, target, ordinal)` — `ordinal` is the
    /// index among parallel edges stored under the same `(source, target)` pair,
    /// mirroring the durable `(graph, src, tgt, ordinal)` row key
    /// (`redb_store::read_graph_dump_page` / `scan_next_edge_ordinal`). `after` is
    /// an EXCLUSIVE `(source, target, ordinal)` cursor (`None` starts at the first
    /// edge); a caller advances it to the last row returned. `limit == 0` means no
    /// cap.
    ///
    /// Unlike `get_edges`/`get_edges_arc` (which clone EVERY edge's property
    /// blob), this clones/decodes properties ONLY for the requested page — the
    /// full scan is over the lightweight `(source, target)` key pairs (cached
    /// after the first call, invalidated on any write), never the property bytes.
    ///
    /// This is a live committed-state scan rather than a cross-request snapshot
    /// (same caveat as `get_nodes_by_label_page`): a concurrent writer can insert
    /// or remove an edge between two page calls; the cursor is not a MVCC
    /// snapshot handle.
    pub fn get_edges_page(
        &self,
        after: Option<(&str, &str, u32)>,
        limit: usize,
    ) -> Vec<(String, String, u32, Vec<u8>)> {
        {
            let guard = self.edge_key_index.read();
            if let Some(keys) = guard.as_ref() {
                return Self::collect_edge_page(keys, &self.edge_properties, after, limit);
            }
        }
        let mut keys: Vec<(String, String)> = self
            .edge_properties
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        keys.sort_unstable();
        let out = Self::collect_edge_page(&keys, &self.edge_properties, after, limit);
        *self.edge_key_index.write() = Some(keys);
        out
    }

    /// Materialize one page of `(source, target, ordinal, properties)` rows from a
    /// SORTED `(source, target)` key list, honouring `limit` (`0` = uncapped) and
    /// the exclusive `after` cursor. Skips a key that has since been removed from
    /// `edge_properties` (defensive against an in-flight removal that hasn't yet
    /// invalidated the cache) — mirrors `GraphCore::collect_by_label`'s identical
    /// defensiveness for the node-label keyset scan.
    pub(super) fn collect_edge_page(
        keys: &[(String, String)],
        edge_properties: &DashMap<(String, String), Vec<Arc<Vec<u8>>>>,
        after: Option<(&str, &str, u32)>,
        limit: usize,
    ) -> Vec<(String, String, u32, Vec<u8>)> {
        let start = match after {
            Some((s, t, _)) => keys.partition_point(|(ks, kt)| (ks.as_str(), kt.as_str()) < (s, t)),
            None => 0,
        };
        let mut out: Vec<(String, String, u32, Vec<u8>)> =
            Vec::with_capacity(if limit == 0 { 0 } else { limit });
        for (src, tgt) in keys.iter().skip(start) {
            let Some(props_list) = edge_properties.get(&(src.clone(), tgt.clone())) else {
                continue; // removed since the key list was built; skip defensively.
            };
            if !Self::push_edge_rows(&mut out, src, tgt, props_list.value(), after, limit) {
                break; // page full — stop scanning keys entirely.
            }
        }
        out
    }

    /// Append ONE endpoint pair's parallel edges to `out`, honouring the exclusive
    /// `after` cursor and `limit` (`0` = uncapped). Returns `false` once the page
    /// is full, which is the caller's signal to stop scanning keys.
    pub(super) fn push_edge_rows(
        out: &mut Vec<(String, String, u32, Vec<u8>)>,
        src: &str,
        tgt: &str,
        props_list: &[Arc<Vec<u8>>],
        after: Option<(&str, &str, u32)>,
        limit: usize,
    ) -> bool {
        for (ordinal, props) in props_list.iter().enumerate() {
            let ordinal = ordinal as u32;
            if Self::edge_row_at_or_before_cursor(src, tgt, ordinal, after) {
                continue;
            }
            if limit != 0 && out.len() >= limit {
                return false;
            }
            out.push((src.to_string(), tgt.to_string(), ordinal, (**props).clone()));
        }
        true
    }

    /// Is this `(source, target, ordinal)` row at or before the EXCLUSIVE cursor?
    /// Only rows on the cursor's own endpoint pair can be, since the key list is
    /// sorted and the scan already started at that pair.
    pub(super) fn edge_row_at_or_before_cursor(
        src: &str,
        tgt: &str,
        ordinal: u32,
        after: Option<(&str, &str, u32)>,
    ) -> bool {
        let Some((after_src, after_tgt, after_ordinal)) = after else {
            return false;
        };
        src == after_src && tgt == after_tgt && ordinal <= after_ordinal
    }

    pub fn get_edge_properties(&self, source_id: &str, target_id: &str) -> Vec<Vec<u8>> {
        self.edge_properties
            .get(&(source_id.to_string(), target_id.to_string()))
            .map(|v| v.iter().map(|a| (**a).clone()).collect())
            .unwrap_or_default()
    }

    pub fn edge_count(&self) -> usize {
        // Topology and edge-property rows are updated in the same GraphTxn. The
        // StableGraph maintains its cardinality, so this is O(1) instead of walking
        // every endpoint pair and parallel-edge property vector.
        self.topo.read().graph.edge_count()
    }
}
