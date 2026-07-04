//! Optional redb-durable codes (feature `redb`, CONCEPT:AU-KG.backend.backend-modes tier reuse).
//!
//! The default persistence is the mmap file format (`persist.rs`) — fast reopen,
//! zero deps beyond memmap2. When the engine is already running its redb durable
//! store, the ANN index can instead live in a redb table so its codes get the same
//! crash-safe, single-writer durability as the graph. Same no-rebuild property:
//! the meta blob + code buffers are read back, posting lists rebuilt in one pass.

use crate::ivfpq::IvfPq;
use redb::{Database, ReadableDatabase, TableDefinition};
use std::path::Path;

const ANN: TableDefinition<&str, &[u8]> = TableDefinition::new("eg_ann");

/// Persist the index into a redb database file under three keys: `meta`, `codes`,
/// `refine`. Atomic + durable via redb's transaction commit (fsync).
pub fn save_redb(idx: &IvfPq, db_path: &Path) -> Result<(), redb::Error> {
    let db = Database::create(db_path)?;
    let meta = bincode::serialize(&MetaBlob::from(idx)).expect("serialize meta");
    let wtxn = db.begin_write()?;
    {
        let mut t = wtxn.open_table(ANN)?;
        t.insert("meta", meta.as_slice())?;
        t.insert("codes", idx.codes.as_slice())?;
        t.insert("refine", idx.sq_codes.as_slice())?;
    }
    wtxn.commit()?;
    Ok(())
}

/// Open WITHOUT rebuild from a redb database: read the three blobs, rebuild
/// posting lists in one integer pass.
pub fn open_redb(db_path: &Path) -> Result<IvfPq, redb::Error> {
    let db = Database::open(db_path)?;
    let rtxn = db.begin_read()?;
    let t = rtxn.open_table(ANN)?;
    let meta: MetaBlob = bincode::deserialize(
        t.get("meta")?
            .ok_or_else(|| {
                redb::Error::from(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "meta missing",
                ))
            })?
            .value(),
    )
    .map_err(|e| redb::Error::from(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
    let codes = t
        .get("codes")?
        .map(|v| v.value().to_vec())
        .unwrap_or_default();
    let sq_codes = t
        .get("refine")?
        .map(|v| v.value().to_vec())
        .unwrap_or_default();
    drop(t);

    let mut idx = meta.into_index(codes, sq_codes);
    idx.rebuild_postings();
    Ok(idx)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MetaBlob {
    dim: usize,
    nlist: usize,
    m: usize,
    dsub: usize,
    rotation: Vec<f32>,
    coarse_centroids: Vec<f32>,
    pq_centroids: Vec<f32>,
    sq_min: Vec<f32>,
    sq_scale: Vec<f32>,
    ids: Vec<u64>,
    list_of: Vec<u32>,
    deleted: Vec<u8>,
}

impl MetaBlob {
    fn from(idx: &IvfPq) -> Self {
        Self {
            dim: idx.dim,
            nlist: idx.nlist,
            m: idx.m,
            dsub: idx.dsub,
            rotation: idx.rotation.clone(),
            coarse_centroids: idx.coarse_centroids.clone(),
            pq_centroids: idx.pq_centroids.clone(),
            sq_min: idx.sq_min.clone(),
            sq_scale: idx.sq_scale.clone(),
            ids: idx.ids.clone(),
            list_of: idx.list_of.clone(),
            deleted: idx.deleted.clone(),
        }
    }
    fn into_index(self, codes: Vec<u8>, sq_codes: Vec<u8>) -> IvfPq {
        IvfPq {
            dim: self.dim,
            nlist: self.nlist,
            m: self.m,
            dsub: self.dsub,
            rotation: self.rotation,
            coarse_centroids: self.coarse_centroids,
            pq_centroids: self.pq_centroids,
            codes,
            sq_codes,
            sq_min: self.sq_min,
            sq_scale: self.sq_scale,
            ids: self.ids,
            list_of: self.list_of,
            deleted: self.deleted,
            postings: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivfpq::{IvfPqParams, SearchParams};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn redb_roundtrip_no_rebuild() {
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

        let sp = SearchParams::default();
        let before = idx.search(&data[200], 10, sp);

        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("ann.redb");
        save_redb(&idx, &db).unwrap();
        let reopened = open_redb(&db).unwrap();
        let after = reopened.search(&data[200], 10, sp);
        assert_eq!(
            before.iter().map(|r| r.id).collect::<Vec<_>>(),
            after.iter().map(|r| r.id).collect::<Vec<_>>(),
            "redb-reopened (no-rebuild) results must match"
        );
    }
}
