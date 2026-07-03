//! CONCEPT:EG-308 — durable JSONPath index persistence.
//!
//! CONCEPT:EG-084 built an in-memory inverted JSONPath index (`path → value → ids`
//! for equality + `path → ids` for existence) on [`crate::graph::GraphCore`], built
//! demand-driven under the write guard and invalidated by `mark_dirty()`, exactly
//! like the KG-2.199 property index. EG-308 makes THAT derived state **durable**: the
//! path-index is written through to a redb table on each (re)build and rehydrated at
//! boot, so a restart skips the full node rescan the demand-driven build would
//! otherwise pay on the first JSON filter after a cold start.
//!
//! Design (mirrors the redb-backed RBAC store, CONCEPT:EG-303, and the dep-light
//! `ColdTier`/`ReadThrough` seams):
//!   * The persistence **seam** — [`PathIndexPersistence`] + [`PersistedPathIndex`] —
//!     is defined DEP-FREE here in eg-core, so `GraphCore` depends only on a trait
//!     object (`Option<Arc<dyn PathIndexPersistence>>`) and the default/Pi build links
//!     nothing extra. An [`InMemoryPathIndexStore`] default carries non-durable use +
//!     the round-trip tests; the real durable [`RedbPathIndexStore`] is gated behind
//!     the `path-persist` feature (which pulls `redb`, the SAME 4.1 the durable tiers
//!     link), exactly like EG-303 gates its redb store behind `security`.
//!   * The persisted form uses `BTreeMap`s so the bytes are **deterministic** (stable
//!     key order) — a save→reopen always restores the identical logical index.
//!   * A [`PersistedPathIndex::stamp`] carries the graph's OCC `version()` at persist
//!     time, so a rehydrated index can be reasoned about against the loaded graph.
//!   * NO store attached (the default) ⇒ the path-index stays fully in-memory and
//!     every write-through is a no-op — byte-for-byte the pre-EG-308 behavior.

use std::collections::BTreeMap;

/// A serializable snapshot of the demand-driven JSONPath index (CONCEPT:EG-308).
///
/// Mirrors the two inverted maps the in-memory `PathIndex` (CONCEPT:EG-084) holds,
/// but in `BTreeMap` form for a **deterministic** byte serialization (stable key
/// order), so persisting the same logical index twice yields identical bytes:
///   * `by_value`: `jsonpath → (canonical scalar value → node ids)` (equality/`->>`);
///   * `present` : `jsonpath → node ids` for which the path resolves to ANY value.
///
/// `stamp` records the source graph's OCC `version()` at persist time.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PersistedPathIndex {
    /// `jsonpath → (canonical scalar value → node ids)` for equality lookups.
    pub by_value: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    /// `jsonpath → node ids` for which the path resolves to ANY value (existence).
    pub present: BTreeMap<String, Vec<String>>,
    /// The source graph's OCC `version()` at the moment this snapshot was persisted
    /// (CONCEPT:EG-308 / KG-2.180). A warm-start reader can compare it against the
    /// live graph version to reason about freshness.
    pub stamp: u64,
}

impl PersistedPathIndex {
    /// How many distinct JSONPaths this snapshot indexes (the demanded-so-far set).
    pub fn path_count(&self) -> usize {
        self.by_value.len()
    }

    /// Is this an empty snapshot (nothing built yet)? An empty snapshot rehydrates to
    /// exactly the cold in-memory default (CONCEPT:EG-308).
    pub fn is_empty(&self) -> bool {
        self.by_value.is_empty() && self.present.is_empty()
    }
}

/// The durable-persistence seam for the JSONPath index (CONCEPT:EG-308). Defined
/// DEP-FREE in eg-core (like [`crate::cold_tier::ColdTier`] /
/// [`crate::read_through::ReadThrough`]) so a `GraphCore` holds only a trait object
/// and no build links a persistence backend it did not ask for.
///
/// Both methods are **best-effort**: `save` swallows its own errors (a failed
/// write-through must never fail the graph write that triggered it — the index is
/// rebuildable), and `load` returns `None` on any error/absence (the caller then
/// rebuilds on demand, exactly the pre-EG-308 path).
pub trait PathIndexPersistence: std::fmt::Debug + Send + Sync {
    /// Load the persisted snapshot, or `None` when nothing is stored / on error.
    fn load(&self) -> Option<PersistedPathIndex>;
    /// Write the snapshot through (best-effort; errors are swallowed).
    fn save(&self, idx: &PersistedPathIndex);
}

/// A dep-free, in-process [`PathIndexPersistence`] (CONCEPT:EG-308). Sharing ONE
/// `Arc<InMemoryPathIndexStore>` across two `GraphCore`s simulates a save→reopen
/// (the second core rehydrates what the first persisted), which is what the default
/// (`cargo test -p eg-core`, no redb) round-trip test exercises. Also the natural
/// zero-durability backend for an embedded caller that wants warm-start reuse within
/// a single process without linking redb.
#[derive(Debug, Default)]
pub struct InMemoryPathIndexStore {
    inner: parking_lot::Mutex<Option<PersistedPathIndex>>,
}

impl InMemoryPathIndexStore {
    /// A fresh, empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl PathIndexPersistence for InMemoryPathIndexStore {
    fn load(&self) -> Option<PersistedPathIndex> {
        self.inner.lock().clone()
    }

    fn save(&self, idx: &PersistedPathIndex) {
        *self.inner.lock() = Some(idx.clone());
    }
}

#[cfg(feature = "path-persist")]
pub use redb_store::{PathPersistError, RedbPathIndexStore};

/// The real durable, redb-backed [`PathIndexPersistence`] (CONCEPT:EG-308, feature
/// `path-persist`). Mirrors the EG-303 [`crate::rbac_persist::RbacStore`]: ONE redb
/// table in `{persist_dir}/path_index.redb`, a single well-known key holding the
/// serde-json bytes of the whole [`PersistedPathIndex`], written in one durable
/// (immediate-fsync) transaction so a reopen restores the identical index.
#[cfg(feature = "path-persist")]
mod redb_store {
    use super::{PathIndexPersistence, PersistedPathIndex};
    use std::fmt;
    use std::path::Path;
    use std::sync::Arc;

    use redb::{Database, ReadableDatabase, TableDefinition};

    /// `key → serde_json bytes`. One table, one well-known key (`snapshot`), written
    /// in a single durable transaction (CONCEPT:EG-308).
    const PATH_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("path_index_v1");
    const SNAPSHOT_KEY: &str = "snapshot";

    /// Errors from the durable path-index store (CONCEPT:EG-308). Flattened to a
    /// message string (matching the EG-303 / cold-tier convention); io + serde carry
    /// their native errors so callers can inspect them.
    #[derive(Debug)]
    pub enum PathPersistError {
        /// Creating the persist dir / opening the redb file failed.
        Io(std::io::Error),
        /// (De)serializing the persisted index failed.
        Serde(serde_json::Error),
        /// A redb transaction/table/storage/commit operation failed.
        Redb(String),
    }

    impl fmt::Display for PathPersistError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                PathPersistError::Io(e) => write!(f, "path persist io error: {e}"),
                PathPersistError::Serde(e) => write!(f, "path persist serde error: {e}"),
                PathPersistError::Redb(e) => write!(f, "path persist redb error: {e}"),
            }
        }
    }

    impl std::error::Error for PathPersistError {}

    impl From<std::io::Error> for PathPersistError {
        fn from(e: std::io::Error) -> Self {
            PathPersistError::Io(e)
        }
    }

    impl From<serde_json::Error> for PathPersistError {
        fn from(e: serde_json::Error) -> Self {
            PathPersistError::Serde(e)
        }
    }

    /// A durable, redb-backed JSONPath-index store (CONCEPT:EG-308). Cheap to `clone`
    /// (shares one `Arc<Database>`).
    pub struct RedbPathIndexStore {
        db: Arc<Database>,
    }

    impl fmt::Debug for RedbPathIndexStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RedbPathIndexStore").finish_non_exhaustive()
        }
    }

    impl RedbPathIndexStore {
        /// Open (or create) `{dir}/path_index.redb` and ensure the table exists
        /// (CONCEPT:EG-308). The dir is created if absent; opening validates the store
        /// is writable up front, so subsequent write-throughs are best-effort.
        pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, PathPersistError> {
            std::fs::create_dir_all(dir.as_ref())?;
            let path = dir.as_ref().join("path_index.redb");
            let db = Database::create(&path).map_err(|e| PathPersistError::Redb(e.to_string()))?;
            let wtx = db
                .begin_write()
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            wtx.open_table(PATH_TABLE)
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            wtx.commit()
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            Ok(Self { db: Arc::new(db) })
        }

        /// Fallible load — the typed backing of the trait's best-effort `load`.
        pub fn try_load(&self) -> Result<Option<PersistedPathIndex>, PathPersistError> {
            let rtx = self
                .db
                .begin_read()
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            let t = rtx
                .open_table(PATH_TABLE)
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            match t
                .get(SNAPSHOT_KEY)
                .map_err(|e| PathPersistError::Redb(e.to_string()))?
            {
                Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
                None => Ok(None),
            }
        }

        /// Fallible save — the typed backing of the trait's best-effort `save`. Writes
        /// the whole snapshot in ONE durable (immediate-fsync) transaction.
        pub fn try_save(&self, idx: &PersistedPathIndex) -> Result<(), PathPersistError> {
            let bytes = serde_json::to_vec(idx)?;
            let mut wtx = self
                .db
                .begin_write()
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            wtx.set_durability(redb::Durability::Immediate)
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            {
                let mut t = wtx
                    .open_table(PATH_TABLE)
                    .map_err(|e| PathPersistError::Redb(e.to_string()))?;
                t.insert(SNAPSHOT_KEY, bytes.as_slice())
                    .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            }
            wtx.commit()
                .map_err(|e| PathPersistError::Redb(e.to_string()))?;
            Ok(())
        }
    }

    impl PathIndexPersistence for RedbPathIndexStore {
        fn load(&self) -> Option<PersistedPathIndex> {
            self.try_load().ok().flatten()
        }

        fn save(&self, idx: &PersistedPathIndex) {
            // Best-effort: a failed write-through must never fail the graph write that
            // triggered it — the index is always rebuildable on demand (CONCEPT:EG-308).
            let _ = self.try_save(idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PersistedPathIndex {
        let mut by_value: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        let mut lang = BTreeMap::new();
        lang.insert("rust".to_string(), vec!["a".to_string(), "c".to_string()]);
        lang.insert("go".to_string(), vec!["b".to_string()]);
        by_value.insert("$.meta.lang".to_string(), lang);
        let mut present: BTreeMap<String, Vec<String>> = BTreeMap::new();
        present.insert("$.tags".to_string(), vec!["a".to_string(), "b".to_string()]);
        PersistedPathIndex {
            by_value,
            present,
            stamp: 7,
        }
    }

    #[test]
    fn eg308_in_memory_store_round_trips_snapshot() {
        // save → load through the dep-free in-memory store restores the identical
        // logical index (CONCEPT:EG-308).
        let store = InMemoryPathIndexStore::new();
        assert!(store.load().is_none(), "cold store is empty");
        let snap = sample();
        store.save(&snap);
        let got = store.load().expect("loads the persisted snapshot");
        assert_eq!(got, snap);
        assert_eq!(got.path_count(), 1);
        assert_eq!(got.stamp, 7);
    }

    #[test]
    fn eg308_persisted_bytes_are_deterministic() {
        // The BTreeMap containers give a stable byte serialization: two builds of the
        // same logical index (inserted in different orders) serialize identically
        // (CONCEPT:EG-308, mirrors EG-303's deterministic identity bytes).
        let a = sample();
        let mut b_by_value: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        let mut lang = BTreeMap::new();
        // Insert the value keys in the OPPOSITE order — BTreeMap normalizes.
        lang.insert("go".to_string(), vec!["b".to_string()]);
        lang.insert("rust".to_string(), vec!["a".to_string(), "c".to_string()]);
        b_by_value.insert("$.meta.lang".to_string(), lang);
        let mut present: BTreeMap<String, Vec<String>> = BTreeMap::new();
        present.insert("$.tags".to_string(), vec!["a".to_string(), "b".to_string()]);
        let b = PersistedPathIndex {
            by_value: b_by_value,
            present,
            stamp: 7,
        };
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap()
        );
    }

    #[cfg(feature = "path-persist")]
    #[test]
    fn eg308_redb_store_round_trips_through_save_reopen() {
        // The real durable path: persist to a redb file, then REOPEN the same dir in a
        // fresh store and load — the index survives the "process restart" (CONCEPT:EG-308).
        let dir = std::env::temp_dir().join(format!(
            "eg308-redb-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let snap = sample();
        {
            let store = RedbPathIndexStore::open(&dir).unwrap();
            assert!(store.load().is_none(), "cold redb store is empty");
            store.save(&snap);
        }
        // Reopen the SAME dir — durable across store lifetimes.
        let store = RedbPathIndexStore::open(&dir).unwrap();
        let got = store
            .load()
            .expect("loads the persisted snapshot from redb");
        assert_eq!(got, snap);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "path-persist")]
    #[test]
    fn eg308_redb_absent_store_loads_none() {
        let dir = std::env::temp_dir().join(format!(
            "eg308-redb-absent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let store = RedbPathIndexStore::open(&dir).unwrap();
        assert!(store.load().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
