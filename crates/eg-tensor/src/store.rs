//! Content-addressed, restart-durable tensor persistence (CONCEPT:EG-085) — the
//! CAS-persist follow-up to the in-memory [`Tensor`] value + byte-blob codec.
//!
//! [`TensorStore`] keeps tensors as content-addressed compact byte blobs (the
//! `crate::blob` codec — [`Tensor::to_blob`]) keyed by a deterministic FNV-1a-128
//! [`content_hash`] of those bytes, so an identical tensor DEDUPS to one blob (the same
//! content always yields the same address). [`TensorStore::persist`] writes every blob
//! to a directory as a hash-named `<hash>.tsr` file and [`TensorStore::load`] rehydrates
//! the store from such a directory — so a tensor store SURVIVES a restart, mirroring how
//! the engine's blob CAS / eg-tsdb persist content-addressed payloads to durable storage
//! (redb / disk). The filename IS the content address, so `load` is self-verifying
//! (bit-rot / tampered blobs are skipped).
//!
//! Pure-Rust, std-only (`std::fs` + `std::collections`) — NO new dependency, so eg-tensor
//! stays the serde-only leaf crate. The FNV-1a-128 hash is portable + stable across
//! processes/platforms (unlike `std`'s `DefaultHasher`), which a content-addressed store
//! REQUIRES: an address computed before a restart must still resolve the same blob after.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use crate::tensor::Tensor;

/// File extension for a persisted content-addressed tensor blob.
const BLOB_EXT: &str = "tsr";

/// A content-addressed, restart-durable store of [`Tensor`] blobs (CONCEPT:EG-085).
///
/// Each tensor is stored as its compact byte blob ([`Tensor::to_blob`]) keyed by
/// [`content_hash`] of those bytes; the SAME tensor always yields the SAME key
/// (deterministic FNV-1a-128), so re-[`put`](TensorStore::put)ting an identical tensor is
/// an idempotent dedup. [`persist`](TensorStore::persist) / [`load`](TensorStore::load)
/// make the store survive a restart.
#[derive(Clone, Debug, Default)]
pub struct TensorStore {
    /// content-hash (lowercase hex) → compact tensor byte blob.
    blobs: HashMap<String, Vec<u8>>,
}

impl TensorStore {
    /// A fresh, empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Content-address a tensor: store its compact blob under its content hash and
    /// return that hash. Idempotent — an identical tensor maps to the same key, so a
    /// re-`put` neither grows the store nor rewrites the blob.
    pub fn put(&mut self, t: &Tensor) -> String {
        let bytes = t.to_blob();
        let key = content_hash(&bytes);
        self.blobs.entry(key.clone()).or_insert(bytes);
        key
    }

    /// Rehydrate the tensor stored under `hash`, if present (and it decodes).
    pub fn get(&self, hash: &str) -> Option<Tensor> {
        Tensor::from_blob(self.blobs.get(hash)?).ok()
    }

    /// Whether a blob is held under `hash`.
    pub fn contains(&self, hash: &str) -> bool {
        self.blobs.contains_key(hash)
    }

    /// Number of DISTINCT blobs held (post-dedup).
    pub fn len(&self) -> usize {
        self.blobs.len()
    }

    /// Whether the store holds no blobs.
    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    /// All content hashes currently held (unordered).
    pub fn hashes(&self) -> impl Iterator<Item = &String> {
        self.blobs.keys()
    }

    /// Persist every blob to `dir` (created if absent) as a hash-named `<hash>.tsr`
    /// file. The filename IS the content address, so a later [`load`](Self::load) is
    /// self-verifying. Overwriting an existing `<hash>.tsr` is a no-op in content (same
    /// bytes), so re-persisting is idempotent.
    pub fn persist(&self, dir: &Path) -> io::Result<()> {
        fs::create_dir_all(dir)?;
        for (hash, bytes) in &self.blobs {
            fs::write(dir.join(format!("{hash}.{BLOB_EXT}")), bytes)?;
        }
        Ok(())
    }

    /// Rebuild a store from a directory previously written by [`persist`](Self::persist).
    /// Only `*.tsr` files are read; a file whose CONTENT no longer hashes to its NAME
    /// (bit rot / tampering) or that does not decode as a tensor blob is SKIPPED — so
    /// load is integrity-checked, and a `persist` → `load` round-trip restores exactly
    /// the tensors that were stored. A missing `dir` yields an empty store (not an error).
    pub fn load(dir: &Path) -> io::Result<Self> {
        let mut store = TensorStore::new();
        if !dir.exists() {
            return Ok(store);
        }
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some(BLOB_EXT) {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let bytes = fs::read(&path)?;
            // Integrity: the filename must equal the recomputed content hash AND the
            // bytes must decode as a valid tensor blob. Either failing ⇒ skip.
            if content_hash(&bytes) != name {
                continue;
            }
            if Tensor::from_blob(&bytes).is_err() {
                continue;
            }
            store.blobs.insert(name.to_string(), bytes);
        }
        Ok(store)
    }
}

/// Deterministic 128-bit FNV-1a content hash of `bytes`, rendered as 32-char lowercase
/// hex (CONCEPT:EG-085). Portable + stable across processes/platforms — the property a
/// content-addressed store needs so an address computed before a restart still resolves
/// the same blob after. NOT cryptographic; it is a dedup / integrity address within the
/// engine trust boundary. Pure-Rust `u128` arithmetic, no dependency.
pub fn content_hash(bytes: &[u8]) -> String {
    // 128-bit FNV-1a constants (the standard offset basis + prime).
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let mut h = OFFSET;
    for &b in bytes {
        h ^= b as u128;
        h = h.wrapping_mul(PRIME);
    }
    format!("{h:032x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tensor::Buffer;
    use std::path::PathBuf;

    fn sample_tensors() -> Vec<Tensor> {
        vec![
            Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.5, 3.0, 4.0, 5.0, 6.0])).unwrap(),
            Tensor::new(vec![3], Buffer::I64(vec![i64::MIN, 0, i64::MAX])).unwrap(),
            Tensor::new(vec![2, 2, 2], Buffer::U8((0..8).collect())).unwrap(),
        ]
    }

    /// A unique scratch directory under the OS temp dir, removed on drop.
    struct TmpDir(PathBuf);
    impl TmpDir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static CTR: AtomicU64 = AtomicU64::new(0);
            let n = CTR.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir()
                .join(format!("eg-tensor-cas-{tag}-{}-{n}", std::process::id()));
            TmpDir(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// CONCEPT:EG-085 — a tensor store round-trips across a persist → load (restart)
    /// cycle: every stored tensor is byte-for-byte recovered under the SAME content hash.
    #[test]
    fn cas_persist_load_round_trip_survives_restart() {
        let dir = TmpDir::new("roundtrip");
        let mut store = TensorStore::new();
        let mut keys = Vec::new();
        for t in sample_tensors() {
            keys.push(store.put(&t));
        }
        assert_eq!(store.len(), 3);
        store.persist(dir.path()).unwrap();

        // A brand-new store (a "restart") rehydrated purely from disk.
        let loaded = TensorStore::load(dir.path()).unwrap();
        assert_eq!(loaded.len(), store.len());
        for (t, key) in sample_tensors().into_iter().zip(&keys) {
            assert!(loaded.contains(key), "hash {key} missing after load");
            assert_eq!(loaded.get(key), Some(t), "tensor differs after round-trip");
        }
    }

    /// CONCEPT:EG-085 — content addressing is deterministic + dedups: the SAME tensor
    /// yields the SAME hash, and re-putting it does not grow the store.
    #[test]
    fn cas_content_hash_is_deterministic_and_dedups() {
        let t = Tensor::new(vec![2, 2], Buffer::F64(vec![1.0, 2.0, 3.0, 4.0])).unwrap();
        let mut store = TensorStore::new();
        let k1 = store.put(&t);
        let k2 = store.put(&t.clone());
        assert_eq!(k1, k2, "same content must address to the same hash");
        assert_eq!(store.len(), 1, "an identical tensor must dedup");
        // The hash is a stable pure function of the blob bytes.
        assert_eq!(content_hash(&t.to_blob()), k1);
        // A different tensor gets a different address.
        let other = Tensor::new(vec![2, 2], Buffer::F64(vec![1.0, 2.0, 3.0, 5.0])).unwrap();
        assert_ne!(store.put(&other), k1);
        assert_eq!(store.len(), 2);
    }

    /// CONCEPT:EG-085 — load is integrity-checked: a `.tsr` file whose bytes no longer
    /// hash to its filename is skipped, so corruption cannot smuggle a wrong tensor in.
    #[test]
    fn cas_load_skips_corrupt_and_foreign_files() {
        let dir = TmpDir::new("integrity");
        let t = Tensor::new(vec![2], Buffer::I32(vec![7, 9])).unwrap();
        let mut store = TensorStore::new();
        let key = store.put(&t);
        store.persist(dir.path()).unwrap();

        // A tampered blob under a valid-looking (wrong) name → skipped.
        fs::write(dir.path().join(format!("{key}.{BLOB_EXT}.bak")), b"junk").unwrap();
        fs::write(
            dir.path().join("deadbeefdeadbeefdeadbeefdeadbeef.tsr"),
            b"not a tensor blob",
        )
        .unwrap();
        // A non-.tsr sidecar → ignored by extension.
        fs::write(dir.path().join("README.txt"), b"hello").unwrap();

        let loaded = TensorStore::load(dir.path()).unwrap();
        assert_eq!(loaded.len(), 1, "only the intact blob should load");
        assert_eq!(loaded.get(&key), Some(t));
    }

    /// CONCEPT:EG-085 — loading a directory that was never written yields an empty
    /// store rather than an error.
    #[test]
    fn cas_load_missing_dir_is_empty() {
        let dir = TmpDir::new("missing");
        let loaded = TensorStore::load(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }
}
