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
    crate::persist::validate_index(idx).map_err(redb_error)?;
    let db = Database::create(db_path)?;
    let meta =
        crate::codec::serialize(&crate::persist::Meta::from_index(idx)).map_err(redb_error)?;
    if meta.len() as u64 > crate::persist::MAX_METADATA_BYTES {
        return Err(redb_error("ANN metadata exceeds its safety bound"));
    }
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
    let meta_guard = t.get("meta")?.ok_or_else(|| {
        redb::Error::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "ANN metadata is missing",
        ))
    })?;
    if meta_guard.value().len() as u64 > crate::persist::MAX_METADATA_BYTES {
        return Err(redb_error("ANN metadata exceeds its safety bound"));
    }
    let meta: crate::persist::Meta =
        crate::codec::deserialize(meta_guard.value()).map_err(redb_error)?;
    let (codes_len, refine_len) = meta.expected_code_lengths().map_err(redb_error)?;
    let codes_guard = t
        .get("codes")?
        .ok_or_else(|| redb_error("ANN PQ codes are missing"))?;
    if codes_guard.value().len() != codes_len {
        return Err(redb_error("ANN PQ-code length does not match metadata"));
    }
    let refine_guard = t
        .get("refine")?
        .ok_or_else(|| redb_error("ANN refine codes are missing"))?;
    if refine_guard.value().len() != refine_len {
        return Err(redb_error("ANN refine-code length does not match metadata"));
    }
    let codes = codes_guard.value().to_vec();
    let sq_codes = refine_guard.value().to_vec();
    drop(meta_guard);
    drop(codes_guard);
    drop(refine_guard);
    drop(t);

    let mut idx = meta.into_index(codes, sq_codes).map_err(redb_error)?;
    idx.rebuild_postings();
    Ok(idx)
}

fn redb_error(error: impl std::fmt::Display) -> redb::Error {
    redb::Error::from(crate::persist::invalid_data(error))
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
