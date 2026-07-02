//! The COLD tier backing store (CONCEPT:EG-185).
//!
//! The COLD tier is where the cache offloads bytes it can no longer keep in RAM — the
//! "survive OOM by spilling to disk" rung. [`ColdStore`] is the pluggable seam:
//!
//! * [`MemoryColdStore`] — always available, dependency-free. Holds the spilled blobs
//!   in a separate `HashMap` so the tier machinery (and all its tests) work in a pure
//!   RAM build. It models the tier boundary without a durable device.
//! * [`RedbColdStore`] — behind the `durable` feature. A crash-safe, single-writer
//!   redb blob store, reusing the engine's KG-2.195 durability tier, so demoted KV
//!   bytes SURVIVE an OOM / process restart. This is the true offload target.
//!
//! `ColdStore` is intentionally object-safe (`Box<dyn ColdStore<K>>`) and moves only
//! bytes — the cache owns all per-key metadata (score, compression flag, sizes), so a
//! backend is a dumb blob device.

use std::io;

/// A pluggable COLD-tier blob device the tiered cache spills bytes into (CONCEPT:EG-185).
///
/// Object-safe by design: keys and values cross as borrows / owned `Vec<u8>`, so it can
/// be held as `Box<dyn ColdStore<K>>`. The cache keeps the per-key metadata; a store
/// only persists / returns opaque blobs.
pub trait ColdStore<K> {
    /// Write (or overwrite) the blob for `key`.
    fn put(&mut self, key: &K, bytes: &[u8]) -> io::Result<()>;
    /// Read the blob for `key`, if present.
    fn get(&self, key: &K) -> io::Result<Option<Vec<u8>>>;
    /// Remove the blob for `key` (no-op if absent).
    fn remove(&mut self, key: &K) -> io::Result<()>;
    /// A short label for diagnostics / stats.
    fn kind(&self) -> &'static str;
}

/// An in-RAM COLD store (CONCEPT:EG-185) — always available, no dependency.
///
/// It keeps spilled blobs in a *separate* map from the HOT/WARM tiers, so demotion to
/// COLD is a real tier transition (bytes leave the HOT/WARM budgets) even in a pure-RAM
/// build. It is NOT durable — use [`RedbColdStore`] (feature `durable`) for
/// offload-that-survives-a-restart.
#[derive(Debug, Default)]
pub struct MemoryColdStore<K> {
    blobs: std::collections::HashMap<K, Vec<u8>>,
}

impl<K: std::hash::Hash + Eq + Clone> MemoryColdStore<K> {
    /// A fresh, empty in-RAM cold store.
    pub fn new() -> Self {
        MemoryColdStore {
            blobs: std::collections::HashMap::new(),
        }
    }
}

impl<K: std::hash::Hash + Eq + Clone> ColdStore<K> for MemoryColdStore<K> {
    fn put(&mut self, key: &K, bytes: &[u8]) -> io::Result<()> {
        self.blobs.insert(key.clone(), bytes.to_vec());
        Ok(())
    }
    fn get(&self, key: &K) -> io::Result<Option<Vec<u8>>> {
        Ok(self.blobs.get(key).cloned())
    }
    fn remove(&mut self, key: &K) -> io::Result<()> {
        self.blobs.remove(key);
        Ok(())
    }
    fn kind(&self) -> &'static str {
        "memory"
    }
}

/// A key that the durable ([`RedbColdStore`]) tier can render to stable bytes
/// (CONCEPT:EG-185). Only the durable tier needs this; the RAM tiers are content with
/// `Hash + Eq`.
#[cfg(feature = "durable")]
pub trait ColdKey {
    /// Stable byte encoding of the key (used as the redb primary key).
    fn cold_key(&self) -> Vec<u8>;
}

#[cfg(feature = "durable")]
impl ColdKey for String {
    fn cold_key(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

#[cfg(feature = "durable")]
impl ColdKey for Vec<u8> {
    fn cold_key(&self) -> Vec<u8> {
        self.clone()
    }
}

#[cfg(feature = "durable")]
impl ColdKey for u64 {
    fn cold_key(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

/// A durable, crash-safe COLD store backed by redb (CONCEPT:EG-185, feature `durable`).
///
/// Demoted KV bytes are written under the key's [`ColdKey::cold_key`] encoding into a
/// single redb table and committed (fsync), so they SURVIVE an OOM / process restart —
/// the true "offload to disk to survive OOM" behavior. Reuses the engine's KG-2.195
/// redb durability tier; a build without `durable` never links redb.
#[cfg(feature = "durable")]
pub struct RedbColdStore {
    db: redb::Database,
}

#[cfg(feature = "durable")]
const COLD: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("eg_kvcache_cold");

#[cfg(feature = "durable")]
impl RedbColdStore {
    /// Open (creating if absent) a durable cold store at `path`.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> io::Result<Self> {
        let db = redb::Database::create(path).map_err(|e| io::Error::other(e.to_string()))?;
        // Ensure the table exists so a first `get` before any `put` succeeds.
        let wtxn = db
            .begin_write()
            .map_err(|e| io::Error::other(e.to_string()))?;
        {
            wtxn.open_table(COLD)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        wtxn.commit().map_err(|e| io::Error::other(e.to_string()))?;
        Ok(RedbColdStore { db })
    }
}

#[cfg(feature = "durable")]
impl<K: ColdKey> ColdStore<K> for RedbColdStore {
    fn put(&mut self, key: &K, bytes: &[u8]) -> io::Result<()> {
        let wtxn = self
            .db
            .begin_write()
            .map_err(|e| io::Error::other(e.to_string()))?;
        {
            let mut t = wtxn
                .open_table(COLD)
                .map_err(|e| io::Error::other(e.to_string()))?;
            t.insert(key.cold_key().as_slice(), bytes)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        wtxn.commit().map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    fn get(&self, key: &K) -> io::Result<Option<Vec<u8>>> {
        use redb::ReadableDatabase;
        let rtxn = self
            .db
            .begin_read()
            .map_err(|e| io::Error::other(e.to_string()))?;
        let t = rtxn
            .open_table(COLD)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let v = t
            .get(key.cold_key().as_slice())
            .map_err(|e| io::Error::other(e.to_string()))?
            .map(|g| g.value().to_vec());
        Ok(v)
    }

    fn remove(&mut self, key: &K) -> io::Result<()> {
        let wtxn = self
            .db
            .begin_write()
            .map_err(|e| io::Error::other(e.to_string()))?;
        {
            let mut t = wtxn
                .open_table(COLD)
                .map_err(|e| io::Error::other(e.to_string()))?;
            t.remove(key.cold_key().as_slice())
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        wtxn.commit().map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "redb"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONCEPT:EG-185 — the RAM cold store round-trips blobs (models the tier boundary).
    #[test]
    fn eg185_memory_cold_store_roundtrips() {
        let mut cs: MemoryColdStore<String> = MemoryColdStore::new();
        cs.put(&"k".to_string(), b"hello").unwrap();
        assert_eq!(
            cs.get(&"k".to_string()).unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        cs.remove(&"k".to_string()).unwrap();
        assert_eq!(cs.get(&"k".to_string()).unwrap(), None);
    }

    /// CONCEPT:EG-185 — the durable redb cold tier persists across a store reopen
    /// (offload survives a restart).
    #[cfg(feature = "durable")]
    #[test]
    fn eg185_durable_redb_cold_store_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold.redb");
        {
            let mut cs = RedbColdStore::open(&path).unwrap();
            ColdStore::<String>::put(&mut cs, &"tok".to_string(), b"kvbytes").unwrap();
        }
        // Reopen a fresh handle — the bytes must still be there.
        let cs = RedbColdStore::open(&path).unwrap();
        assert_eq!(
            ColdStore::<String>::get(&cs, &"tok".to_string())
                .unwrap()
                .as_deref(),
            Some(&b"kvbytes"[..]),
            "durable cold tier must survive a reopen (OOM/restart offload)"
        );
    }
}
