use super::*;

impl GraphCore {
    pub fn has_node(&self, node_id: &str) -> bool {
        self.topo.read().node_map.contains_key(node_id)
    }

    pub fn get_nodes(&self) -> Vec<(String, Vec<u8>)> {
        self.node_properties
            .iter()
            .map(|e| (e.key().clone(), (**e.value()).clone()))
            .collect()
    }

    /// Return at most `limit` nodes (id, properties) whose `type`/`label`/`labels`
    /// matches `label`; `limit == 0` means no cap. Rows are ordered by node id.
    /// Scans in-engine but bounds the returned payload, so a
    /// `MATCH (n:Label) … LIMIT k` caller no longer materializes every node's
    /// properties over the wire.
    ///
    /// An EMPTY `label` (CONCEPT:EG-KG.query.unlabeled-scan-limit-pushdown) means "no label filter" — a bounded scan of
    /// the WHOLE node store, still honouring `limit`. This gives an unlabeled
    /// `MATCH (n) … LIMIT k` a genuinely bounded RPC to call instead of falling
    /// back to the unbounded `GetNodes` dump (which trips the `RESULT_TOO_LARGE`
    /// overload guard on a large graph even when the caller only wanted `k` rows).
    pub fn get_nodes_by_label(&self, label: &str, limit: usize) -> Vec<(String, Vec<u8>)> {
        self.get_nodes_by_label_page(label, None, limit)
    }

    /// Deterministic keyset page over [`Self::get_nodes_by_label`]. `after` is an
    /// exclusive node-id cursor. This is a live committed-state scan rather than
    /// a cross-request snapshot; synchronizers must serialize a reconcile pass
    /// with writers that could insert an older id.
    pub fn get_nodes_by_label_page(
        &self,
        label: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        if label.is_empty() {
            return self.collect_unlabeled(after, limit);
        }
        // CONCEPT:EG-KG.compute.consult-lazy — consult the lazy `label → ids` index so a label
        // lookup is an O(1) map hit instead of a full DashMap scan that
        // deserializes every node. The cached map is invalidated by `mark_dirty()`
        // after any successful write (the same dirty flag the checkpoint uses), so
        // it never serves a stale view across a mutation.
        {
            let guard = self.label_index.read();
            if let Some(idx) = guard.as_ref() {
                return Self::collect_by_label(idx, &self.node_properties, label, after, limit);
            }
        }
        // Stamp the freshly built index at the version it reflects (W1.6/P7,
        // CONCEPT:EG-KG.storage.incremental-index-stamp). The scan reads the LIVE `node_properties`, so
        // the built map is at least as current as `built_at`; a later write's `mark_dirty` sees
        // the stamp predates its new version and drops the now-stale index. Captured before the
        // scan so the stamp is a safe lower bound (an under-estimate only risks a rebuild).
        let built_at = self.version();
        let built = self.build_label_index();
        let out = Self::collect_by_label(&built, &self.node_properties, label, after, limit);
        {
            let mut guard = self.label_index.write();
            self.index_stamps
                .label
                .store(built_at, std::sync::atomic::Ordering::Release);
            *guard = Some(built);
        }
        out
    }

    /// The unlabeled-scan leg of [`Self::get_nodes_by_label`] (CONCEPT:EG-KG.query.unlabeled-scan-limit-pushdown):
    /// every node in deterministic id order, honouring `limit` (`0` = uncapped).
    /// Only ids are sorted; property blobs are cloned for the requested page.
    pub(super) fn collect_unlabeled(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        // Build and publish while holding the topology read guard. Every
        // structural writer takes the matching write guard, so it cannot slip a
        // node add/remove between this snapshot and publication; its later
        // mark_dirty invalidates the cache before the next committed-state read.
        if self.node_id_index.read().is_none() {
            let built_at = self.version();
            let topo = self.topo.read();
            let mut cache = self.node_id_index.write();
            if cache.is_none() {
                let mut ids: Vec<String> = topo.node_map.keys().cloned().collect();
                ids.sort_unstable();
                // Stamp at the built version (W1.6/P7) so `mark_dirty` preserves this scan cache
                // when a later add/remove is incrementally applied, and drops it otherwise.
                self.index_stamps
                    .node_id
                    .store(built_at, std::sync::atomic::Ordering::Release);
                *cache = Some(ids);
            }
        }
        let cache = self.node_id_index.read();
        let ids = cache.as_deref().unwrap_or_default();
        let start = after.map_or(0, |cursor| ids.partition_point(|id| id.as_str() <= cursor));
        let capacity = if limit == 0 {
            ids.len().saturating_sub(start)
        } else {
            limit.min(ids.len().saturating_sub(start))
        };
        let mut out: Vec<(String, Vec<u8>)> = Vec::with_capacity(capacity);
        for id in ids.iter().skip(start) {
            if limit != 0 && out.len() >= limit {
                break;
            }
            if let Some(props) = self.node_properties.get(id) {
                out.push((id.clone(), (**props.value()).clone()));
            }
        }
        out
    }

    /// Materialize the `(id, properties)` rows for `label` from a built index,
    /// honouring `limit` (`0` = uncapped). Skips ids that have since been removed
    /// from the property store (defensive against an in-flight removal that hasn't
    /// yet invalidated the index).
    pub(super) fn collect_by_label(
        index: &HashMap<String, Vec<String>>,
        node_properties: &DashMap<String, Arc<Vec<u8>>>,
        label: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        let Some(ids) = index.get(label) else {
            return Vec::new();
        };
        let start = after.map_or(0, |cursor| ids.partition_point(|id| id.as_str() <= cursor));
        // Sized allocation instead of doubling growth, mirroring
        // `collect_unlabeled`'s identical capacity computation two functions
        // above — the caller-requested page size upper-bounds the true count
        // (some ids may since have been removed from `node_properties`).
        let capacity = if limit == 0 {
            ids.len().saturating_sub(start)
        } else {
            limit.min(ids.len().saturating_sub(start))
        };
        let mut out: Vec<(String, Vec<u8>)> = Vec::with_capacity(capacity);
        for id in ids.iter().skip(start) {
            if limit != 0 && out.len() >= limit {
                break;
            }
            if let Some(props) = node_properties.get(id) {
                out.push((id.clone(), (**props.value()).clone()));
            }
        }
        out
    }

    /// Scan the node store once and build the secondary label index
    /// (CONCEPT:EG-KG.compute.consult-lazy): each node id is filed under EVERY label it carries.
    /// The label set is read from exactly the fields `get_nodes_by_label` matched
    /// before this index existed, so the two never diverge:
    ///   * `type` (canonical), `node_type` (the field the Python client writes —
    ///     graph_compute normalises `type` ⇒ `node_type` on read-back, so the
    ///     index MUST honour both or a label-scoped MATCH under-returns every
    ///     node_type-keyed node), `label`, and the multi-valued `labels` array.
    pub(super) fn build_label_index(&self) -> HashMap<String, Vec<String>> {
        // O(V) full rebuild — the exact cost W1.6/P7 incremental maintenance exists to avoid.
        // Counting + logging it is the "rebuild count under ingest ~0" observability signal.
        let n = self
            .index_rebuilds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::debug!(
            target: "epistemic_graph::index_rebuild",
            index = "label",
            nodes = self.node_properties.len(),
            total_rebuilds = n,
            "full label-index rebuild (cold cache); warm ingest maintains it incrementally"
        );
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            Self::file_node_labels(entry.key(), &val, &mut index);
        }
        Self::dedup_label_postings(&mut index);
        index
    }

    /// File ONE node id under every label it carries: the single-valued `type`,
    /// `node_type` and `label` fields, plus every entry of the multi-valued
    /// `labels` array.
    pub(super) fn file_node_labels(
        id: &str,
        val: &serde_json::Value,
        index: &mut HashMap<String, Vec<String>>,
    ) {
        for key in ["type", "node_type", "label"] {
            if let Some(lbl) = val.get(key).and_then(|v| v.as_str()) {
                index
                    .entry(lbl.to_string())
                    .or_default()
                    .push(id.to_string());
            }
        }
        let Some(arr) = val.get("labels").and_then(|v| v.as_array()) else {
            return;
        };
        for x in arr {
            if let Some(lbl) = x.as_str() {
                index
                    .entry(lbl.to_string())
                    .or_default()
                    .push(id.to_string());
            }
        }
    }

    /// A node carrying the same value on two of {type,node_type,label} (or a
    /// duplicated `labels` entry) would otherwise be listed twice for that
    /// label; dedup so the returned rows match the pre-index 1-node-1-row scan.
    pub(super) fn dedup_label_postings(index: &mut HashMap<String, Vec<String>>) {
        for ids in index.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
    }
}
