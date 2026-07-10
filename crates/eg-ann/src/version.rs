//! Embedding versioning — tag stored vectors with the model/version that produced
//! them (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed depth: per-modality vector versioning).
//!
//! `eg-ann` stores compressed PQ/SQ8 codes keyed by an external `u64` id (see
//! [`crate::ivfpq::IvfPq`]); it does NOT track which embedding model (or which
//! version of that model) produced each stored vector. That provenance matters the
//! moment a KG re-embeds a corpus with a new model: mixed-version rows in the same
//! index silently degrade recall (old and new vectors are not comparable), and
//! nothing today can even tell you it happened.
//!
//! [`EmbeddingVersion`] is the minimal tag (`model_id` + a monotonic `model_version`
//! integer); [`EmbeddingVersionStore`] is a side-table `id -> EmbeddingVersion`,
//! persisted independently of the PQ codes (bincode, same codec [`crate::persist`]
//! already uses) so tagging is fully additive — an index built before this feature
//! existed just has an empty version store.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// The model identity + version that produced one stored embedding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingVersion {
    /// The embedding model's identifier (e.g. `"text-embedding-3-large"`).
    pub model_id: String,
    /// A monotonic version counter for that model (bump on any re-embed that
    /// changes the vector space — a fine-tune, a dimension change, a provider
    /// upgrade). Caller-assigned; this crate only compares/stores it.
    pub model_version: u32,
}

impl EmbeddingVersion {
    pub fn new(model_id: impl Into<String>, model_version: u32) -> Self {
        Self {
            model_id: model_id.into(),
            model_version,
        }
    }
}

/// A side-table mapping stored embedding id → the [`EmbeddingVersion`] that
/// produced it. Independent of the PQ/SQ8 code buffers — tagging (or re-tagging)
/// never touches the vector data itself.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EmbeddingVersionStore {
    tags: HashMap<u64, EmbeddingVersion>,
}

impl EmbeddingVersionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Tag `id` with `version` (overwrites any prior tag for that id).
    pub fn tag(&mut self, id: u64, version: EmbeddingVersion) {
        self.tags.insert(id, version);
    }

    /// Tag a whole batch with the SAME version — the common re-embed-a-corpus case.
    pub fn tag_batch(&mut self, ids: &[u64], version: &EmbeddingVersion) {
        for &id in ids {
            self.tags.insert(id, version.clone());
        }
    }

    pub fn version_of(&self, id: u64) -> Option<&EmbeddingVersion> {
        self.tags.get(&id)
    }

    pub fn remove(&mut self, id: u64) -> Option<EmbeddingVersion> {
        self.tags.remove(&id)
    }

    pub fn len(&self) -> usize {
        self.tags.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Every stored id currently tagged with exactly `model_id`/`model_version`.
    pub fn ids_for_version(&self, model_id: &str, model_version: u32) -> Vec<u64> {
        self.tags
            .iter()
            .filter(|(_, v)| v.model_id == model_id && v.model_version == model_version)
            .map(|(&id, _)| id)
            .collect()
    }

    /// The distinct `(model_id, model_version)` pairs present — a quick "is this
    /// index mixed-version?" check (`distinct_versions().len() > 1`).
    pub fn distinct_versions(&self) -> Vec<(String, u32)> {
        let mut out: Vec<(String, u32)> = self
            .tags
            .values()
            .map(|v| (v.model_id.clone(), v.model_version))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        out.sort();
        out
    }

    /// Persist the tag table (bincode) — the same codec [`crate::persist`] uses for
    /// the index's own `meta.bin`, so this drops beside it in an index directory.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let bytes = bincode::serialize(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, bytes)
    }

    /// Reopen a persisted tag table. A missing file is NOT an error — it means "no
    /// tags yet" (an index built before versioning existed), so this returns an
    /// empty store rather than failing.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = fs::read(path)?;
        bincode::deserialize(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_and_lookup_roundtrip() {
        let mut store = EmbeddingVersionStore::new();
        let v1 = EmbeddingVersion::new("text-embedding-3-large", 1);
        store.tag(10, v1.clone());
        assert_eq!(store.version_of(10), Some(&v1));
        assert_eq!(store.version_of(999), None);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn tag_batch_applies_same_version_to_all_ids() {
        let mut store = EmbeddingVersionStore::new();
        let v = EmbeddingVersion::new("m1", 2);
        store.tag_batch(&[1, 2, 3], &v);
        assert_eq!(store.len(), 3);
        assert!([1u64, 2, 3].iter().all(|&id| store.version_of(id) == Some(&v)));
    }

    #[test]
    fn ids_for_version_filters_correctly_on_reembed() {
        let mut store = EmbeddingVersionStore::new();
        store.tag_batch(&[1, 2], &EmbeddingVersion::new("m1", 1));
        store.tag_batch(&[3, 4], &EmbeddingVersion::new("m1", 2)); // re-embedded
        let old = store.ids_for_version("m1", 1);
        let new = store.ids_for_version("m1", 2);
        assert_eq!(old.len(), 2);
        assert_eq!(new.len(), 2);
        assert!(old.iter().all(|id| [1u64, 2].contains(id)));
        assert!(new.iter().all(|id| [3u64, 4].contains(id)));
    }

    #[test]
    fn distinct_versions_detects_mixed_version_index() {
        let mut store = EmbeddingVersionStore::new();
        assert!(store.distinct_versions().is_empty());
        store.tag_batch(&[1], &EmbeddingVersion::new("m1", 1));
        assert_eq!(store.distinct_versions(), vec![("m1".to_string(), 1)]);
        store.tag_batch(&[2], &EmbeddingVersion::new("m1", 2));
        assert_eq!(
            store.distinct_versions(),
            vec![("m1".to_string(), 1), ("m1".to_string(), 2)],
            "a re-embedded-but-not-yet-migrated index is detectably mixed-version"
        );
    }

    #[test]
    fn save_open_roundtrip() {
        let mut store = EmbeddingVersionStore::new();
        store.tag_batch(&[1, 2, 3], &EmbeddingVersion::new("m1", 5));
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("versions.bin");
        store.save(&path).unwrap();
        let reopened = EmbeddingVersionStore::open(&path).unwrap();
        assert_eq!(reopened.len(), 3);
        assert_eq!(
            reopened.version_of(2),
            Some(&EmbeddingVersion::new("m1", 5))
        );
    }

    #[test]
    fn open_missing_file_yields_empty_store_not_error() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("does-not-exist.bin");
        let store = EmbeddingVersionStore::open(&path).unwrap();
        assert!(store.is_empty());
    }
}
