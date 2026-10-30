//! Bounded low-storage ANN manifest and byte accounting
//! (EG-FEDERATED-QUERY-R040.5.1), beside HNSW and IVF-PQ.
//!
//! The manifest is the admission record of a low-storage artifact: it binds the
//! source identity, embedding space, encoding policy and generation, and carries
//! the byte accounting that proves the artifact stays under its ceiling. It
//! references source text by digest and never copies it.

use serde::{Deserialize, Serialize};

/// Byte components of one artifact. The total is always checked addition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAccounting {
    pub graph_bytes: u64,
    pub code_bytes: u64,
    pub metadata_bytes: u64,
    pub source_reference_bytes: u64,
    pub cache_bytes: u64,
    pub scratch_bytes: u64,
    /// Ceiling for the persisted total (everything except scratch).
    pub ceiling_bytes: u64,
    /// Ceiling for scratch at its peak.
    pub scratch_ceiling_bytes: u64,
}

impl StorageAccounting {
    /// Persisted bytes: graph, code, metadata, source reference and cache. `None` on overflow.
    pub fn persisted_total(&self) -> Option<u64> {
        self.graph_bytes
            .checked_add(self.code_bytes)?
            .checked_add(self.metadata_bytes)?
            .checked_add(self.source_reference_bytes)?
            .checked_add(self.cache_bytes)
    }

    /// Sum of every component, scratch included. `None` on overflow.
    pub fn total(&self) -> Option<u64> {
        self.persisted_total()?.checked_add(self.scratch_bytes)
    }

    pub fn validate(&self) -> Result<(), String> {
        let persisted = self
            .persisted_total()
            .ok_or("storage accounting overflows")?;
        self.total().ok_or("storage accounting overflows")?;
        if self.ceiling_bytes == 0 || self.scratch_ceiling_bytes == 0 {
            return Err("storage accounting requires non-zero ceilings".into());
        }
        if persisted > self.ceiling_bytes {
            return Err(format!(
                "persisted bytes {persisted} exceed ceiling {}",
                self.ceiling_bytes
            ));
        }
        if self.scratch_bytes > self.scratch_ceiling_bytes {
            return Err("scratch peak exceeds its ceiling".into());
        }
        Ok(())
    }
}

/// The admission manifest of one low-storage ANN generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LowStorageAnnManifest {
    pub tenant: String,
    pub source_snapshot: String,
    pub source_id: String,
    pub source_revision: String,
    /// Digest of the referenced source text; the text itself is never copied.
    pub text_digest: String,
    pub embedding_space: String,
    pub encoding_policy: String,
    pub generation: u64,
    pub build_policy: String,
    pub row_count: u64,
    pub accounting: StorageAccounting,
}

impl LowStorageAnnManifest {
    /// Admission check, run BEFORE publication. Refuses missing identity and
    /// over-budget accounting.
    pub fn admit(&self) -> Result<(), String> {
        let identity = [
            ("tenant", &self.tenant),
            ("source snapshot", &self.source_snapshot),
            ("source id", &self.source_id),
            ("source revision", &self.source_revision),
            ("text digest", &self.text_digest),
            ("embedding space", &self.embedding_space),
            ("encoding policy", &self.encoding_policy),
            ("build policy", &self.build_policy),
        ];
        for (name, value) in identity {
            if value.trim().is_empty() {
                return Err(format!("manifest is missing {name}"));
            }
        }
        if self.row_count == 0 {
            return Err("manifest row count must be non-zero".into());
        }
        if self.accounting.source_reference_bytes == 0 {
            return Err("manifest must reference its source".into());
        }
        self.accounting.validate()
    }
}

/// Publishes manifests; a refused manifest leaves the active generation unchanged.
#[derive(Debug, Default)]
pub struct ManifestRegistry {
    active: Option<LowStorageAnnManifest>,
}

impl ManifestRegistry {
    pub fn active(&self) -> Option<&LowStorageAnnManifest> {
        self.active.as_ref()
    }

    /// Admit then publish; mixed build or encoding policies against the active
    /// generation of the same source are refused.
    pub fn publish(&mut self, manifest: LowStorageAnnManifest) -> Result<(), String> {
        manifest.admit()?;
        if let Some(active) = &self.active {
            if active.source_id == manifest.source_id
                && (active.build_policy != manifest.build_policy
                    || active.encoding_policy != manifest.encoding_policy)
            {
                return Err("manifest mixes policies with the active generation".into());
            }
            if manifest.generation <= active.generation && active.source_id == manifest.source_id {
                return Err("manifest generation must advance".into());
            }
        }
        self.active = Some(manifest);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> LowStorageAnnManifest {
        LowStorageAnnManifest {
            tenant: "t".into(),
            source_snapshot: "snap1".into(),
            source_id: "src".into(),
            source_revision: "r1".into(),
            text_digest: "sha256:aa".into(),
            embedding_space: "space1".into(),
            encoding_policy: "enc1".into(),
            generation: 1,
            build_policy: "build1".into(),
            row_count: 100,
            accounting: StorageAccounting {
                graph_bytes: 10,
                code_bytes: 20,
                metadata_bytes: 3,
                source_reference_bytes: 4,
                cache_bytes: 5,
                scratch_bytes: 6,
                ceiling_bytes: 100,
                scratch_ceiling_bytes: 10,
            },
        }
    }

    // spec: EG-FEDERATED-QUERY-R040.5.1
    #[test]
    fn manifest_round_trips_sums_independently_and_refuses_bad_input() {
        let m = manifest();
        let bytes = postcard::to_stdvec(&m).unwrap();
        let back: LowStorageAnnManifest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, m);
        assert_eq!(m.accounting.persisted_total(), Some(10 + 20 + 3 + 4 + 5));
        assert_eq!(m.accounting.total(), Some(48));
        m.admit().unwrap();

        let mut reg = ManifestRegistry::default();
        reg.publish(m.clone()).unwrap();
        let mut over = manifest();
        over.generation = 2;
        over.accounting.cache_bytes = 1000;
        assert!(reg.publish(over).is_err());
        let mut overflow = manifest();
        overflow.generation = 2;
        overflow.accounting.graph_bytes = u64::MAX;
        assert!(reg.publish(overflow).is_err());
        let mut nosrc = manifest();
        nosrc.generation = 2;
        nosrc.accounting.source_reference_bytes = 0;
        assert!(reg.publish(nosrc).is_err());
        let mut mixed = manifest();
        mixed.generation = 2;
        mixed.build_policy = "other".into();
        assert!(reg.publish(mixed).is_err());
        let mut noid = manifest();
        noid.generation = 2;
        noid.embedding_space.clear();
        assert!(reg.publish(noid).is_err());
        let mut scratch = manifest();
        scratch.generation = 2;
        scratch.accounting.scratch_bytes = 11;
        assert!(reg.publish(scratch).is_err());
        // every refusal left the active generation unchanged
        assert_eq!(reg.active(), Some(&m));
    }
}
