use super::*;

impl GraphCore {
    // ── secondary property index (CONCEPT:EG-KG.query.concept-12) ──────────────────────────

    /// Node ids whose property `key` equals `value`, via the bounded, demand-driven
    /// secondary property index (CONCEPT:EG-KG.query.concept-12). Equality only. The first call
    /// for a given `key` indexes it (subject to the configured cap) and caches the
    /// `value → ids` map; subsequent calls are an O(1) map hit. The cache is
    /// invalidated by `mark_dirty()` after any write, so it never serves a stale
    /// view across a mutation.
    ///
    /// `value` is compared against the canonical string form of the stored property
    /// (see [`Self::property_value_key`]): strings match their text, scalars their
    /// `to_string()`. Returns ids sorted + deduped (one id per node).
    ///
    /// Returns `None` when `key` is NOT (and cannot be) indexed under the bound —
    /// the caller must then full-scan. Returns `Some(vec)` (possibly empty) when the
    /// key IS indexed: an empty vec means "indexed, no node has that value".
    pub fn nodes_by_property(&self, key: &str, value: &str) -> Option<Vec<String>> {
        // Fast path: key already indexed in the cached map.
        {
            let guard = self.property_index.read();
            if let Some(idx) = guard.as_ref() {
                if let Some(by_value) = idx.keys.get(key) {
                    return Some(by_value.get(value).cloned().unwrap_or_default());
                }
            }
        }
        // Slow path: build/extend the index to cover `key` (bounded), then answer. Stamp the
        // index at the pre-build version (W1.6/P7) so `mark_dirty` preserves it across a
        // subsequently incrementally-maintained write. `fetch_max` never regresses a stamp a
        // concurrent maintenance already advanced (the newly-scanned key reflects live data, so
        // the pre-build version is a safe lower bound).
        let built_at = self.version();
        let mut guard = self.property_index.write();
        let idx = guard.get_or_insert_with(PropertyIndex::default);
        let indexed = self.ensure_key_indexed(idx, key);
        self.index_stamps
            .property
            .fetch_max(built_at, std::sync::atomic::Ordering::AcqRel);
        indexed?;
        Some(
            idx.keys
                .get(key)
                .and_then(|by_value| by_value.get(value).cloned())
                .unwrap_or_default(),
        )
    }

    /// Composite equality lookup (CONCEPT:EG-KG.query.concept-12): node ids matching EVERY
    /// `(key, value)` pair, as the intersection of the per-key equality sets. Used
    /// for pushdown of multiple `col = literal` predicates ANDed together. Returns
    /// `None` if ANY key is not (and cannot be) indexed under the bound — the caller
    /// then full-scans the whole predicate; partial pushdown would risk divergence.
    pub fn nodes_by_properties(&self, pairs: &[(&str, &str)]) -> Option<Vec<String>> {
        if pairs.is_empty() {
            return None;
        }
        // Resolve each predicate via the single-key path (each ensures its key is
        // indexed); intersect the resulting id sets. Start from the smallest set.
        let mut sets: Vec<Vec<String>> = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            sets.push(self.nodes_by_property(k, v)?);
        }
        sets.sort_by_key(|s| s.len());
        let mut acc = sets.remove(0);
        for s in &sets {
            Self::intersect_sorted_ids(&mut acc, s);
            if acc.is_empty() {
                break;
            }
        }
        Some(acc)
    }

    /// Intersect `acc` IN PLACE with `other`.
    ///
    /// Every property posting is already sorted + deduplicated when built, so this
    /// is the classic two-pointer merge rather than a fresh `HashSet` per
    /// predicate: deterministic O(a+b) time with O(1) scratch (beyond the reused
    /// result vector).
    pub(super) fn intersect_sorted_ids(acc: &mut Vec<String>, other: &[String]) {
        let mut write = 0usize;
        let mut left = 0usize;
        let mut right = 0usize;
        while left < acc.len() && right < other.len() {
            match acc[left].cmp(&other[right]) {
                std::cmp::Ordering::Less => left += 1,
                std::cmp::Ordering::Greater => right += 1,
                std::cmp::Ordering::Equal => {
                    if write != left {
                        acc.swap(write, left);
                    }
                    write += 1;
                    left += 1;
                    right += 1;
                }
            }
        }
        acc.truncate(write);
    }

    /// Ensure `key` is present in the property index, honouring the bound. Returns
    /// `Some(())` if the key is (now) indexed, `None` if the cap is full and the key
    /// is not already present (so the caller must full-scan). Pre-seeds any keys
    /// named in `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` on the first build.
    // `map_entry` would have us use `entry().or_insert_with`, but the bounded cap
    // means we must NOT insert when the map is full — `entry` always inserts, so the
    // explicit `contains_key`/`len`-then-`insert` is the correct shape here.
    #[allow(clippy::map_entry)]
    pub(super) fn ensure_key_indexed(&self, idx: &mut PropertyIndex, key: &str) -> Option<()> {
        let cap = Self::max_indexed_properties();
        // First-build pre-seed: index every env-named key (subject to the cap).
        if idx.keys.is_empty() {
            for seed in Self::seed_indexed_properties() {
                if idx.keys.len() >= cap || idx.keys.contains_key(&seed) {
                    continue;
                }
                let map = self.build_property_value_map(&seed);
                idx.keys.insert(seed, map);
            }
        }
        if idx.keys.contains_key(key) {
            return Some(());
        }
        if idx.keys.len() >= cap {
            // Cap reached and this key isn't indexed — refuse (full-scan fallback).
            return None;
        }
        let map = self.build_property_value_map(key);
        idx.keys.insert(key.to_string(), map);
        Some(())
    }

    /// Scan the node store once and build the `value → node ids` map for one
    /// property `key`. Each node is filed under the canonical string form of its
    /// `key` value (if present and a scalar). Ids are sorted + deduped.
    pub(super) fn build_property_value_map(&self, key: &str) -> HashMap<String, Vec<String>> {
        let n = self
            .index_rebuilds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::debug!(
            target: "epistemic_graph::index_rebuild",
            index = "property",
            key,
            nodes = self.node_properties.len(),
            total_rebuilds = n,
            "full property-value-map rebuild (cold key); warm ingest maintains it incrementally"
        );
        let mut by_value: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            if let Some(vk) = val.get(key).and_then(Self::property_value_key) {
                by_value.entry(vk).or_default().push(entry.key().clone());
            }
        }
        for ids in by_value.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
        by_value
    }

    /// Canonical string key for an equality-indexable property value: strings index
    /// under their text, bools/numbers under their `to_string()`. Arrays/objects/
    /// null are NOT equality-indexable (return `None`) — they fall to a full scan,
    /// matching how an equality predicate on such a column behaves.
    ///
    /// `pub` (CONCEPT:EG-KG.storage.index-manager-seam) so a caller resolving a WHERE/inline-prop
    /// literal to a `Predicate::PropertyEq` value (eg-query's Cypher executor) canonicalizes
    /// EXACTLY the same way this index was built — the single source of truth, not a
    /// second implementation a future edit here could silently drift out of sync with.
    pub fn property_value_key(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }

    /// Read a positive bounded-index limit from an environment variable, falling
    /// back when it is unset, malformed, or zero. The query crate uses this same
    /// policy so its relational pushdown cannot drift from the graph indexes.
    pub fn configured_index_limit(env_key: &str, default: usize) -> usize {
        let raw = match std::env::var(env_key) {
            Ok(raw) => raw,
            Err(_) => return default,
        };
        match raw.trim().parse::<usize>() {
            Ok(value) if value > 0 => value,
            _ => default,
        }
    }

    /// Cap on the number of distinct property keys ever indexed
    /// (`EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES`, default 32). `0` is treated as the
    /// default rather than "disable" so a misconfigured empty value can't silently
    /// turn the index off.
    pub fn max_indexed_properties() -> usize {
        Self::configured_index_limit(
            "EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES",
            DEFAULT_MAX_INDEXED_PROPERTIES,
        )
    }

    /// Property keys to pre-seed into the index on first build
    /// (`EPISTEMIC_GRAPH_INDEXED_PROPERTIES`, comma-separated). Empty when unset.
    pub(super) fn seed_indexed_properties() -> Vec<String> {
        Self::indexed_properties_from_env()
    }

    /// Parse the shared property-index seed setting for GraphCore and query providers.
    pub fn indexed_properties_from_env() -> Vec<String> {
        Self::seed_indexed_values("EPISTEMIC_GRAPH_INDEXED_PROPERTIES")
    }

    pub(super) fn seed_indexed_values(env_key: &str) -> Vec<String> {
        std::env::var(env_key)
            .ok()
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    }
}
