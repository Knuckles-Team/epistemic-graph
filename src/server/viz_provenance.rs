//! Durable render provenance (D-VZ-1 lane V4, "provenance inherited from the
//! producing job").
//!
//! A rendered view is not a durable graph mutation — it has no `GraphCore` to
//! attach a `:ToolCall`/`RunTrace` node to (viz is explicitly NOT graph-scoped;
//! see `handlers::viz`'s module doc). It still needs an auditable record of
//! WHAT was rendered, from WHAT data, and HOW, that survives a restart. Rather
//! than adopting the full `eg-jobs` `AnalyticsJob`/`JobStore` machinery (built
//! for graph-scoped, worker-claimed, async-executed jobs — a real mismatch for
//! a synchronous request/response render with no `GraphCore` to snapshot and
//! no worker pool to claim it), this module identifies a record by
//! `handlers::viz::provenance_result_ref` — a content-addressed id derived
//! from the SAME `render_cache_key` the render cache itself keys on, so
//! provenance and cache entries for one render always agree on "which render
//! this is" without a second, independently-computed identity scheme. (An
//! `eg_jobs`-shaped binding for this data — `AlgoVersion`/
//! `InputSnapshotHandle`/`compute_result_ref` — is proven interop-compatible
//! at the `eg-viz-core::job` layer's own dev-test, should a later lane want to
//! surface a render through the shared analytics-job plane; this store does
//! not require adopting that machinery to be useful today.) It persists ONE
//! record per distinct `result_ref` in a small durable side-store — the SAME
//! shape `persistence::tenant_catalog::TenantCatalog` already establishes for
//! a non-graph durable map (`catalog.redb`): an in-memory authoritative view,
//! optionally backed by its own small redb file, durability strictly opt-in
//! and never required for this module to function.
//!
//! **Idempotent, not append-only.** `result_ref` is already content-derived
//! (a render of byte-identical spec+dataset+canvas+format always computes the
//! SAME `result_ref`), so recording is `put_if_absent`: the first time a given
//! view is actually produced, its provenance is written once; a later cache
//! HIT of the same `result_ref` (see `viz_engine::RenderCache`) needs no new
//! entry — the view's provenance has not changed, only its serving cost has.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[cfg(feature = "redb")]
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

#[cfg(feature = "redb")]
const PROVENANCE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("viz_provenance");

const MAX_ENTRIES: usize = 100_000;
const MAX_RESULT_REF_BYTES: usize = 512;

/// One durable record of a produced render (D-VZ-1 lane V4). Kept intentionally
/// small and flat — this is an audit/debugging record, not a copy of the
/// rendered bytes (those live in [`super::viz_engine::RenderCache`], which is
/// bounded/evictable; provenance is meant to outlive an evicted cache entry).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct VizProvenanceRecord {
    /// `eg_jobs::compute_result_ref(&InputSnapshotHandle, &AlgoVersion)` — the
    /// SAME content-derived identity scheme the durable analytics-job plane
    /// uses, so a viz render's provenance sits in the same identity space as
    /// every other computed result this engine produces.
    pub result_ref: String,
    /// `eg_viz_core::job::query_hash(spec, dataset_ref, content_fingerprint)` —
    /// what [`super::viz_engine`]'s render cache itself keys on (folded with
    /// canvas size/format there); recorded here too so a caller can go
    /// `query_hash -> result_ref` without recomputing it.
    pub query_hash: String,
    pub dataset_ref: String,
    /// `eg_viz_columnstore::ColumnStore::content_fingerprint` at render time —
    /// the exact "what changed" scope this record is pinned to.
    pub content_fingerprint: u64,
    pub algo_family: String,
    pub algo_name: String,
    pub lod_tier: String,
    pub exact: bool,
    pub row_count: u64,
    pub width_px: u32,
    pub height_px: u32,
    pub format: String,
    pub wall_time_ms: u64,
    pub produced_at_unix_ms: i64,
}

fn validate_key(key: &str) -> Result<(), String> {
    if key.is_empty() || key.len() > MAX_RESULT_REF_BYTES || key.contains('\0') {
        Err("viz provenance result_ref exceeds resource limits".to_string())
    } else {
        Ok(())
    }
}

/// Durable (optionally) render-provenance store. Mirrors
/// `persistence::tenant_catalog::TenantCatalog`'s "in-memory authoritative view,
/// optional redb backing" shape, but does not live under `persistence/` — that
/// module is themed around the authoritative GRAPH store, which viz explicitly
/// is not (see `handlers::viz`'s "NOT graph-scoped" doc).
pub(crate) struct VizProvenanceStore {
    entries: RwLock<HashMap<String, VizProvenanceRecord>>,
    #[cfg(feature = "redb")]
    db: Option<Database>,
}

impl VizProvenanceStore {
    /// Non-durable store (no persist dir configured, or a build without the
    /// `redb` feature — `viz-static-export` does not itself imply `redb`).
    /// Mutations are visible immediately but do not survive a restart.
    pub(crate) fn in_memory() -> Self {
        VizProvenanceStore {
            entries: RwLock::new(HashMap::new()),
            #[cfg(feature = "redb")]
            db: None,
        }
    }

    /// Open (or create) a durable store at `viz_provenance.redb` under
    /// `persist_dir` and load every existing record into memory. Only
    /// available with the `redb` feature; callers without it (or without a
    /// configured persist dir) use [`Self::in_memory`] instead.
    #[cfg(feature = "redb")]
    pub(crate) fn open(persist_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let path = std::path::Path::new(persist_dir).join("viz_provenance.redb");
        let db = Database::create(&path).map_err(|e| e.to_string())?;
        {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            wtx.open_table(PROVENANCE_TABLE)
                .map_err(|e| e.to_string())?;
            wtx.commit().map_err(|e| e.to_string())?;
        }
        let mut entries = HashMap::new();
        {
            let rtx = db.begin_read().map_err(|e| e.to_string())?;
            let table = rtx
                .open_table(PROVENANCE_TABLE)
                .map_err(|e| e.to_string())?;
            for row in table.iter().map_err(|e| e.to_string())? {
                if entries.len() >= MAX_ENTRIES {
                    return Err("viz provenance store exceeds resource limits".to_string());
                }
                let (k, v) = row.map_err(|e| e.to_string())?;
                validate_key(k.value())?;
                let record: VizProvenanceRecord = eg_types::msgpack::decode_bounded(
                    v.value(),
                    eg_types::msgpack::MsgpackLimits::new(
                        2048,
                        32,
                        eg_types::msgpack::DEFAULT_MAX_DEPTH,
                    ),
                )
                .map_err(|_| {
                    "viz provenance row is invalid or exceeds resource limits".to_string()
                })?;
                entries.insert(k.value().to_string(), record);
            }
        }
        Ok(VizProvenanceStore {
            entries: RwLock::new(entries),
            db: Some(db),
        })
    }

    /// Record `record` durably IFF `record.result_ref` is not already known —
    /// a render's provenance is written once (see module doc: idempotent, not
    /// append-only). Returns `true` if this call actually inserted a new
    /// record, `false` if it was already present (a cache-hit re-serve, or a
    /// concurrent duplicate submit — both harmless no-ops here).
    pub(crate) fn put_if_absent(&self, record: VizProvenanceRecord) -> Result<bool, String> {
        validate_key(&record.result_ref)?;
        {
            let entries = self.entries.read();
            if entries.contains_key(&record.result_ref) {
                return Ok(false);
            }
        }
        #[cfg(feature = "redb")]
        if let Some(db) = &self.db {
            let bytes = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut table = wtx
                    .open_table(PROVENANCE_TABLE)
                    .map_err(|e| e.to_string())?;
                table
                    .insert(record.result_ref.as_str(), bytes.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            wtx.commit().map_err(|e| e.to_string())?;
        }
        let mut entries = self.entries.write();
        if entries.len() >= MAX_ENTRIES && !entries.contains_key(&record.result_ref) {
            return Err("viz provenance store exceeds resource limits".to_string());
        }
        let inserted = entries.insert(record.result_ref.clone(), record).is_none();
        Ok(inserted)
    }

    pub(crate) fn get(&self, result_ref: &str) -> Option<VizProvenanceRecord> {
        self.entries.read().get(result_ref).cloned()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(result_ref: &str) -> VizProvenanceRecord {
        VizProvenanceRecord {
            result_ref: result_ref.to_string(),
            query_hash: "eg:viz_query:abc".to_string(),
            dataset_ref: "ds:1".to_string(),
            content_fingerprint: 42,
            algo_family: "viz.render".to_string(),
            algo_name: "line".to_string(),
            lod_tier: "direct".to_string(),
            exact: true,
            row_count: 100,
            width_px: 800,
            height_px: 600,
            format: "png".to_string(),
            wall_time_ms: 5,
            produced_at_unix_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn put_then_get_round_trips_in_memory() {
        let store = VizProvenanceStore::in_memory();
        assert!(store.put_if_absent(sample("r1")).unwrap());
        let got = store.get("r1").unwrap();
        assert_eq!(got, sample("r1"));
    }

    #[test]
    fn put_if_absent_is_idempotent() {
        let store = VizProvenanceStore::in_memory();
        assert!(store.put_if_absent(sample("r1")).unwrap());
        assert!(
            !store.put_if_absent(sample("r1")).unwrap(),
            "a repeat put for the same result_ref must be a no-op, not overwrite/duplicate"
        );
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn get_of_unknown_result_ref_is_none() {
        let store = VizProvenanceStore::in_memory();
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn empty_result_ref_is_rejected() {
        let store = VizProvenanceStore::in_memory();
        let err = store.put_if_absent(sample("")).unwrap_err();
        assert!(err.contains("resource limits"));
    }

    #[cfg(feature = "redb")]
    #[test]
    fn durable_store_survives_a_reopen() {
        let dir = tempfile_dir();
        {
            let store = VizProvenanceStore::open(&dir).unwrap();
            assert!(store.put_if_absent(sample("r1")).unwrap());
        }
        {
            let reopened = VizProvenanceStore::open(&dir).unwrap();
            assert_eq!(reopened.get("r1").unwrap(), sample("r1"));
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "redb")]
    fn tempfile_dir() -> String {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "eg-viz-provenance-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        dir.to_string_lossy().to_string()
    }
}
