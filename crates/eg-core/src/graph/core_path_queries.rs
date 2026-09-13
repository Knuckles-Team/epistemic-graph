use super::*;

impl GraphCore {
    // ── inverted JSONPath path-index (CONCEPT:EG-KG.compute.json-deep-indexing) ─────────────────────────

    /// Node ids whose JSONPath `path` resolves to a scalar equal to `value`, via the
    /// bounded, demand-driven inverted path-index (CONCEPT:EG-KG.compute.json-deep-indexing). This is the
    /// index-accelerated backing for `WHERE props->>'k' = 'v'` (and `props->'k' = …`).
    /// `value` is compared against the canonical string form of the stored scalar (see
    /// [`crate::jsonpath::canonical_scalar`]), so a numeric `30` matches `"30"`.
    ///
    /// Returns `None` when `path` is not (and cannot be) indexed under the bound — the
    /// caller must then full-scan. Returns `Some(vec)` (possibly empty) when the path IS
    /// indexed. The cache is invalidated by `mark_dirty()` after any write, so it never
    /// serves a consistent-stale view across a mutation.
    pub fn nodes_by_json_path(&self, path: &str, value: &str) -> Option<Vec<String>> {
        {
            let guard = self.path_index.read();
            if let Some(idx) = guard.as_ref() {
                if let Some(by_value) = idx.by_value.get(path) {
                    return Some(by_value.get(value).cloned().unwrap_or_default());
                }
            }
        }
        let built_at = self.version();
        let mut guard = self.path_index.write();
        let idx = guard.get_or_insert_with(PathIndex::default);
        let indexed = self.ensure_json_path_indexed(idx, path);
        // Stamp the (re)built path index at the pre-build version (W1.6/P7) so `mark_dirty`
        // preserves it across a CAS that could not feed any indexed path.
        self.index_stamps
            .path
            .fetch_max(built_at, std::sync::atomic::Ordering::AcqRel);
        indexed?;
        Some(
            idx.by_value
                .get(path)
                .and_then(|by_value| by_value.get(value).cloned())
                .unwrap_or_default(),
        )
    }

    /// Node ids for which JSONPath `path` resolves to ANY value — the EXISTENCE set
    /// (CONCEPT:EG-KG.compute.json-deep-indexing), the index-accelerated backing for `props @> …` / `jsonb_path_query`
    /// existence and a selectivity estimate for the planner. Returns `None` when `path`
    /// is not (and cannot be) indexed under the bound (full-scan fallback).
    pub fn nodes_with_json_path(&self, path: &str) -> Option<Vec<String>> {
        {
            let guard = self.path_index.read();
            if let Some(idx) = guard.as_ref() {
                if let Some(ids) = idx.present.get(path) {
                    return Some(ids.clone());
                }
            }
        }
        let built_at = self.version();
        let mut guard = self.path_index.write();
        let idx = guard.get_or_insert_with(PathIndex::default);
        let indexed = self.ensure_json_path_indexed(idx, path);
        self.index_stamps
            .path
            .fetch_max(built_at, std::sync::atomic::Ordering::AcqRel);
        indexed?;
        Some(idx.present.get(path).cloned().unwrap_or_default())
    }

    /// Ensure `path` is present in the JSONPath index, honouring the bound
    /// (CONCEPT:EG-KG.compute.json-deep-indexing). `Some(())` if the path is (now) indexed, `None` if the cap is
    /// full and the path is not already present (caller full-scans). Pre-seeds any paths
    /// named in `EPISTEMIC_GRAPH_INDEXED_JSON_PATHS` on the first build.
    #[allow(clippy::map_entry)]
    pub(super) fn ensure_json_path_indexed(&self, idx: &mut PathIndex, path: &str) -> Option<()> {
        let cap = Self::max_indexed_json_paths();
        // Track whether we built anything, so a durable store (CONCEPT:EG-KG.storage.path-index-store) is
        // written through exactly once per demand-driven (re)build — not on a pure
        // cache hit where the path was already indexed.
        let mut changed = false;
        if idx.by_value.is_empty() && idx.present.is_empty() {
            for seed in Self::seed_indexed_json_paths() {
                if idx.by_value.len() >= cap || idx.by_value.contains_key(&seed) {
                    continue;
                }
                let (by_value, present) = self.build_json_path_maps(&seed);
                idx.by_value.insert(seed.clone(), by_value);
                idx.present.insert(seed, present);
                changed = true;
            }
        }
        if idx.by_value.contains_key(path) {
            // CONCEPT:EG-KG.storage.path-index-store — a rehydrated seed set (or an earlier same-guard build)
            // already covers `path`; persist the seed build if it happened, then hit.
            if changed {
                self.persist_path_index(idx);
            }
            return Some(());
        }
        if idx.by_value.len() >= cap {
            if changed {
                self.persist_path_index(idx);
            }
            return None;
        }
        let (by_value, present) = self.build_json_path_maps(path);
        idx.by_value.insert(path.to_string(), by_value);
        idx.present.insert(path.to_string(), present);
        // CONCEPT:EG-KG.storage.path-index-store — write the freshly (re)built index through to the durable
        // store so a restart rehydrates it instead of rescanning every node. A no-op
        // when no store is attached (the default), so the in-memory path is unchanged.
        self.persist_path_index(idx);
        Some(())
    }

    /// Attach a durable JSONPath-index store (CONCEPT:EG-KG.storage.path-index-store). Called once at startup
    /// (only when a persist dir is configured): thereafter every demand-driven
    /// (re)build of the path-index is written through, and [`rehydrate_path_index`]
    /// can warm the index from the store at boot. A `GraphCore` with no store attached
    /// behaves exactly as before — the path-index is fully in-memory.
    ///
    /// [`rehydrate_path_index`]: GraphCore::rehydrate_path_index
    pub fn set_path_index_store(&self, store: Arc<dyn crate::path_persist::PathIndexPersistence>) {
        *self.path_index_store.write() = Some(store);
    }

    /// Rehydrate the persisted JSONPath index into memory at boot (CONCEPT:EG-KG.storage.path-index-store).
    /// Loads the durable snapshot (if a store is attached and non-empty) and adopts it
    /// as the live `path_index`, so the FIRST JSON filter after a restart hits the
    /// warm index instead of paying a full node rescan. Returns the number of distinct
    /// JSONPaths adopted (0 when no store is attached / the store is empty). A
    /// subsequent write invalidates the index via `mark_dirty` as usual, and the next
    /// query rebuilds-on-miss (and re-persists) — so rehydration never pins a stale
    /// view across a mutation.
    pub fn rehydrate_path_index(&self) -> usize {
        let Some(store) = self.path_index_store.read().clone() else {
            return 0;
        };
        let Some(snap) = store.load() else {
            return 0;
        };
        if snap.is_empty() {
            return 0;
        }
        let idx = PathIndex::from_persisted(&snap);
        let adopted = idx.by_value.len();
        let mut guard = self.path_index.write();
        // Stamp the rehydrated index at the current version (W1.6/P7) so the stamp-aware
        // `mark_dirty` treats it as current until the first write that could affect a path.
        self.index_stamps
            .path
            .fetch_max(self.version(), std::sync::atomic::Ordering::AcqRel);
        *guard = Some(idx);
        adopted
    }

    /// Write the current in-memory path-index through to the durable store, stamped
    /// with the graph's OCC `version()` (CONCEPT:EG-KG.storage.path-index-store). A no-op when no store is
    /// attached (the default), so the fully-in-memory path is byte-for-byte unchanged.
    /// Best-effort: a persistence error is swallowed by the store impl (the index is
    /// always rebuildable on demand), so it can never fail the query that triggered it.
    pub(super) fn persist_path_index(&self, idx: &PathIndex) {
        let Some(store) = self.path_index_store.read().clone() else {
            return;
        };
        let snap = idx.to_persisted(self.version());
        store.save(&snap);
    }

    /// JSONPath filter selectivity for the planner cost `Stats` (CONCEPT:EG-KG.storage.path-index-store —
    /// hooking the EG-084 inverted-index id counts into the cross-modal cost model,
    /// CONCEPT:EG-KG.query.concept-14). Returns the fraction of property-bearing nodes that pass the
    /// filter, in `[0,1]` — exactly the `filter_selectivity` a planner feeds
    /// `eg_plan::cost::Stats::estimate` to order a Filter/Rank pair:
    ///   * `value = Some(v)` — an EQUALITY filter (`props->>'k' = 'v'`): the count from
    ///     [`nodes_by_json_path`](GraphCore::nodes_by_json_path) over `|nodes|`;
    ///   * `value = None` — an EXISTENCE/`@>` filter: the count from
    ///     [`nodes_with_json_path`](GraphCore::nodes_with_json_path) over `|nodes|`.
    ///
    /// Returns `None` when `path` cannot be indexed under the bound (the planner then
    /// falls back to its default estimate) — the SAME bound the two count methods use,
    /// so the selectivity source never triggers a build the query itself wouldn't.
    pub fn json_path_selectivity(&self, path: &str, value: Option<&str>) -> Option<f64> {
        let total = self.node_properties.len();
        if total == 0 {
            return Some(0.0);
        }
        let matched = match value {
            Some(v) => self.nodes_by_json_path(path, v)?.len(),
            None => self.nodes_with_json_path(path)?.len(),
        };
        Some((matched as f64 / total as f64).clamp(0.0, 1.0))
    }

    /// Scan the node store once and build, for one JSONPath, both the `value → ids`
    /// equality map (over the canonical scalar form of each matched leaf) and the
    /// existence `ids` set (CONCEPT:EG-KG.compute.json-deep-indexing). A malformed path yields empty maps.
    pub(super) fn build_json_path_maps(
        &self,
        path: &str,
    ) -> (HashMap<String, Vec<String>>, Vec<String>) {
        let mut by_value: HashMap<String, Vec<String>> = HashMap::new();
        let mut present: Vec<String> = Vec::new();
        let Some(segs) = crate::jsonpath::parse_path(path) else {
            return (by_value, present);
        };
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            let matches = crate::jsonpath::eval(&val, &segs);
            if matches.is_empty() {
                continue;
            }
            present.push(entry.key().clone());
            for m in matches {
                if let Some(vk) = crate::jsonpath::canonical_scalar(m) {
                    by_value.entry(vk).or_default().push(entry.key().clone());
                }
            }
        }
        for ids in by_value.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
        present.sort_unstable();
        present.dedup();
        (by_value, present)
    }

    /// Cap on the number of distinct JSONPaths ever indexed
    /// (`EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS`, default 64) (CONCEPT:EG-KG.compute.json-deep-indexing).
    pub(super) fn max_indexed_json_paths() -> usize {
        std::env::var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_INDEXED_JSON_PATHS)
    }

    /// JSONPaths to pre-seed into the index on first build
    /// (`EPISTEMIC_GRAPH_INDEXED_JSON_PATHS`, comma-separated) (CONCEPT:EG-KG.compute.json-deep-indexing).
    pub(super) fn seed_indexed_json_paths() -> Vec<String> {
        Self::seed_indexed_values("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS")
    }
}
