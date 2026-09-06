//! In-memory durable-code artifact for the IVF-PQ index
//! (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed).
//!
//! The index's durable image is exactly three opaque buffers: the versioned
//! metadata record, the PQ codes, and the SQ8 refine codes. This module owns
//! the encode/decode of those buffers — the format knowledge belongs to the
//! crate that defines the format — and owns **no storage**. It opens no file,
//! no database and no transaction: a caller hands the artifact to whatever
//! durable authority it serves under, and hands it back to rebuild the index
//! with the same no-rebuild property the mmap reader has (posting lists are
//! rebuilt in one integer pass; no k-means, no f32 reconstruction).
//!
//! This replaces `redb_store.rs`, which opened its own `redb::Database` and so
//! made this crate a second physical authority (RF-RULING-004). It also could
//! not hold two index generations: it wrote the fixed keys `meta`/`codes`/
//! `refine` into one table, so building a new generation overwrote the one
//! being served. Both are fixed by the storage side owning the keying — see
//! `eg_storage::ANN_CODES`, keyed `(tenant, binding, generation, part)`.

use crate::ivfpq::IvfPq;

/// The three buffers that make one index generation durable, in the order a
/// consumer must store and restore them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnCodeArtifact {
    /// Versioned metadata record (centroids, codebooks, shape, row ids).
    pub meta: Vec<u8>,
    /// PQ codes, `m` bytes per indexed row.
    pub codes: Vec<u8>,
    /// SQ8 refine codes, `dim` bytes per indexed row.
    pub refine: Vec<u8>,
}

/// Encode a validated index into its three durable buffers.
pub fn encode(idx: &IvfPq) -> std::io::Result<AnnCodeArtifact> {
    crate::persist::validate_index(idx)?;
    let meta = crate::codec::serialize(&crate::persist::Meta::from_index(idx))
        .map_err(crate::persist::invalid_data)?;
    if meta.len() as u64 > crate::persist::MAX_METADATA_BYTES {
        return Err(crate::persist::invalid_data(
            "ANN metadata exceeds its safety bound",
        ));
    }
    Ok(AnnCodeArtifact {
        meta,
        codes: idx.codes.clone(),
        refine: idx.sq_codes.clone(),
    })
}

/// Rebuild an index from its durable buffers WITHOUT retraining. Every length
/// is checked against the metadata before a single row is interpreted.
pub fn decode(artifact: &AnnCodeArtifact) -> std::io::Result<IvfPq> {
    if artifact.meta.len() as u64 > crate::persist::MAX_METADATA_BYTES {
        return Err(crate::persist::invalid_data(
            "ANN metadata exceeds its safety bound",
        ));
    }
    let meta: crate::persist::Meta =
        crate::codec::deserialize(&artifact.meta).map_err(crate::persist::invalid_data)?;
    let (codes_len, refine_len) = meta.expected_code_lengths()?;
    if artifact.codes.len() != codes_len {
        return Err(crate::persist::invalid_data(
            "ANN PQ-code length does not match metadata",
        ));
    }
    if artifact.refine.len() != refine_len {
        return Err(crate::persist::invalid_data(
            "ANN refine-code length does not match metadata",
        ));
    }
    let mut idx = meta.into_index(artifact.codes.clone(), artifact.refine.clone())?;
    idx.rebuild_postings();
    Ok(idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivfpq::{IvfPqParams, SearchParams};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn trained_index() -> (IvfPq, Vec<Vec<f32>>) {
        let dim = 32;
        let mut rng = ChaCha8Rng::seed_from_u64(5);
        let centers: Vec<Vec<f32>> = (0..20)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect())
            .collect();
        let data: Vec<Vec<f32>> = (0..3000)
            .map(|_| {
                let c = &centers[rng.gen_range(0..centers.len())];
                (0..dim)
                    .map(|j| c[j] + (rng.gen::<f32>() - 0.5) * 0.2)
                    .collect()
            })
            .collect();
        let params = IvfPqParams {
            dim,
            nlist: 48,
            m: 8,
            kmeans_iters: 12,
            opq_iters: 4,
            seed: 1,
        };
        let mut idx = IvfPq::train(&params, &data);
        let items: Vec<(u64, Vec<f32>)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.clone()))
            .collect();
        idx.add(&items);
        (idx, data)
    }

    #[test]
    fn artifact_round_trips_without_rebuild() {
        let (idx, data) = trained_index();
        let sp = SearchParams::default();
        let before = idx.search(&data[200], 10, sp);

        let artifact = encode(&idx).unwrap();
        let restored = decode(&artifact).unwrap();
        let after = restored.search(&data[200], 10, sp);
        assert_eq!(
            before.iter().map(|r| r.id).collect::<Vec<_>>(),
            after.iter().map(|r| r.id).collect::<Vec<_>>(),
            "artifact-restored (no-rebuild) results must match"
        );
    }

    /// Every buffer's length is checked against the metadata before a row is
    /// interpreted, so a partial store cannot produce a partial index.
    #[test]
    fn a_truncated_buffer_fails_closed() {
        let (idx, _) = trained_index();
        for corrupt in 0..3 {
            let mut artifact = encode(&idx).unwrap();
            match corrupt {
                0 => artifact.codes.pop(),
                1 => artifact.refine.pop(),
                _ => artifact.meta.pop(),
            };
            assert!(
                decode(&artifact).is_err(),
                "a truncated buffer {corrupt} must not decode"
            );
        }
    }

    /// Negative test layer (a) of RF-RULING-007, keyed on the **dependency
    /// edge** rather than on a marker string: a marker can be moved by
    /// extraction, a dependency cannot.
    ///
    /// `eg-ann` declares no `redb` and no storage-kernel dependency at all, so
    /// no code in this crate can name a `Database`, a `TableDefinition` or a
    /// transaction. The durable authority for these buffers is the storage
    /// kernel, reached by a consumer that holds one — never by this crate.
    #[test]
    fn eg_ann_declares_no_physical_storage_dependency() {
        let manifest = include_str!("../Cargo.toml");
        let mut in_dependencies = false;
        let mut declared = Vec::new();
        for line in manifest.lines() {
            let line = line.trim();
            if line.starts_with('[') {
                in_dependencies = matches!(line, "[dependencies]" | "[dev-dependencies]");
                continue;
            }
            if !in_dependencies || line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((name, _)) = line.split_once('=') {
                declared.push(name.trim().to_string());
            }
        }
        assert!(
            declared.contains(&"serde".to_string()),
            "the manifest scan found no dependencies at all: {declared:?}"
        );
        for forbidden in ["redb", "eg-storage", "eg-transaction"] {
            assert!(
                !declared.iter().any(|name| name == forbidden),
                "eg-ann must not depend on `{forbidden}`: {declared:?}"
            );
        }
    }
}
