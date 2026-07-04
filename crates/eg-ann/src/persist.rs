//! Persistent on-disk format — the no-rebuild-on-load win (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed).
//!
//! An index directory holds three files:
//!   * `meta.bin` — bincode of everything EXCEPT the two bulk code buffers: the
//!     OPQ rotation, coarse + PQ centroids, ids, `list_of`, tombstones, per-row
//!     SQ8 min/scale, and params.
//!   * `codes.bin` — `N*m` u8 PQ codes (the IVF-PQ bulk), mmapped on open.
//!   * `refine.bin` — `N*dim` u8 SQ8 refine codes, mmapped on open.
//!
//! `open()` bincode-loads `meta.bin`, mmaps the two code files (copying the bytes
//! into the `IvfPq` so it stays one owned struct — still NO f32 reconstruction, NO
//! k-means, NO graph build), and rebuilds posting lists with one O(N) integer
//! pass. Reopen does no vector arithmetic — the expensive structure (rotation +
//! codebooks + cell assignment) is read back, not recomputed. This is the precise
//! contrast with `SemanticStore`'s HNSW, which is `#[serde(skip)]` and rebuilds
//! insert-by-insert on first search after load.

use crate::ivfpq::IvfPq;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const META_FILE: &str = "meta.bin";
const CODES_FILE: &str = "codes.bin";
const REFINE_FILE: &str = "refine.bin";

#[derive(Serialize, Deserialize)]
struct Meta {
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
    n: usize,
}

/// Atomically write the index to `dir` (write-to-temp + rename per file).
pub fn save(idx: &IvfPq, dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    let meta = Meta {
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
        n: idx.ids.len(),
    };
    write_atomic(
        &dir.join(META_FILE),
        &bincode::serialize(&meta).expect("serialize meta"),
    )?;
    write_atomic(&dir.join(CODES_FILE), &idx.codes)?;
    write_atomic(&dir.join(REFINE_FILE), &idx.sq_codes)?;
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

/// Open WITHOUT rebuilding from raw f32. mmaps the two code files, loads meta,
/// rebuilds posting lists (integer pass only). Ready to query.
pub fn open(dir: &Path) -> std::io::Result<IvfPq> {
    let mbytes = fs::read(dir.join(META_FILE))?;
    let meta: Meta = bincode::deserialize(&mbytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let codes = mmap_to_vec(&dir.join(CODES_FILE))?;
    let sq_codes = mmap_to_vec(&dir.join(REFINE_FILE))?;

    let mut idx = IvfPq {
        dim: meta.dim,
        nlist: meta.nlist,
        m: meta.m,
        dsub: meta.dsub,
        rotation: meta.rotation,
        coarse_centroids: meta.coarse_centroids,
        pq_centroids: meta.pq_centroids,
        codes,
        sq_codes,
        sq_min: meta.sq_min,
        sq_scale: meta.sq_scale,
        ids: meta.ids,
        list_of: meta.list_of,
        deleted: meta.deleted,
        postings: Vec::new(),
    };
    idx.rebuild_postings(); // O(N) integer pass — no vector math
    Ok(idx)
}

fn mmap_to_vec(path: &Path) -> std::io::Result<Vec<u8>> {
    let f = File::open(path)?;
    let len = f.metadata()?.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    let mmap = unsafe { Mmap::map(&f)? };
    Ok(mmap.to_vec())
}

/// Compaction / VACUUM: rewrite the index dropping all tombstoned rows, WITHOUT
/// retraining (rotation + codebooks are kept; only the row buffers are rebuilt).
/// This reclaims posting-list bloat from accumulated deletes — the per-list
/// compaction the spike flagged as a hard problem. Returns a fresh compacted
/// `IvfPq`; the codebooks/rotation are shared by clone (cheap relative to the data).
pub fn compact(idx: &IvfPq) -> IvfPq {
    let dim = idx.dim;
    let m = idx.m;
    let live: Vec<usize> = (0..idx.ids.len())
        .filter(|&r| idx.deleted[r] == 0)
        .collect();
    let n = live.len();

    let mut codes = Vec::with_capacity(n * m);
    let mut sq_codes = Vec::with_capacity(n * dim);
    let mut sq_min = Vec::with_capacity(n);
    let mut sq_scale = Vec::with_capacity(n);
    let mut ids = Vec::with_capacity(n);
    let mut list_of = Vec::with_capacity(n);
    for &row in &live {
        codes.extend_from_slice(&idx.codes[row * m..(row + 1) * m]);
        sq_codes.extend_from_slice(&idx.sq_codes[row * dim..(row + 1) * dim]);
        sq_min.push(idx.sq_min[row]);
        sq_scale.push(idx.sq_scale[row]);
        ids.push(idx.ids[row]);
        list_of.push(idx.list_of[row]);
    }
    let deleted = vec![0u8; n];

    let mut out = IvfPq {
        dim,
        nlist: idx.nlist,
        m,
        dsub: idx.dsub,
        rotation: idx.rotation.clone(),
        coarse_centroids: idx.coarse_centroids.clone(),
        pq_centroids: idx.pq_centroids.clone(),
        codes,
        sq_codes,
        sq_min,
        sq_scale,
        ids,
        list_of,
        deleted,
        postings: Vec::new(),
    };
    out.rebuild_postings();
    out
}
