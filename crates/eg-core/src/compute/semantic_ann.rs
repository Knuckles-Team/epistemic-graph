//! eg-ann IVF-PQ index backend for `SemanticStore` (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed, feature `ann`).
//!
//! Wraps `eg_ann::IvfPq` with the bookkeeping `SemanticStore` needs:
//!   * a `String` node-id ↔ `u64` row-id map (eg-ann is integer-keyed),
//!   * cosine-via-normalised-L2: vectors are L2-normalised before indexing, so the
//!     IVF-PQ squared-L2 distance `d` ranks identically to cosine and the cosine
//!     similarity is recovered as `1 − d/2`,
//!   * lazy build: the index trains on the resident embeddings the first time the
//!     store crosses `ANN_BUILD_THRESHOLD`, then encodes them; below that the store
//!     uses brute force (no index).
//!
//! The index is built from the in-RAM embeddings on first use, BUT once persisted
//! (`save`) it reopens via `eg_ann::open` WITHOUT rebuilding from raw vectors — the
//! no-rebuild win that distinguishes this from the hnsw_rs path.

use eg_ann::{IvfPq, IvfPqParams, SearchParams};
use std::collections::HashMap;
use std::path::Path;

/// Train + switch to the IVF-PQ index once the store holds at least this many
/// embeddings. Below it, brute-force cosine is both faster and exact.
pub const ANN_BUILD_THRESHOLD: usize = 4096;

/// Target IVF cells ≈ √N, clamped to a sane range for small/medium stores.
fn nlist_for(n: usize) -> usize {
    ((n as f64).sqrt() as usize).clamp(16, 65_536)
}

/// PQ subquantizer count: largest divisor of `dim` giving `dsub` in `[4, 16]`
/// (more subquantizers ⇒ finer codes ⇒ better recall, bounded by code size).
fn m_for(dim: usize) -> usize {
    for dsub in [4usize, 6, 8, 12, 16] {
        if dim.is_multiple_of(dsub) {
            return dim / dsub;
        }
    }
    // Fallback: any divisor.
    (1..=dim)
        .rev()
        .find(|d| dim.is_multiple_of(*d))
        .unwrap_or(1)
}

/// L2-normalise (returns the input unchanged if it is all-zero).
fn normalize(v: &[f32]) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        return v.to_vec();
    }
    v.iter().map(|x| x / norm).collect()
}

/// The eg-ann index plus the id bookkeeping `SemanticStore` needs.
pub struct AnnIndex {
    index: IvfPq,
    /// row id (u64) → node id.
    row_to_id: Vec<String>,
    /// node id → its CURRENT live row id (latest insert wins).
    id_to_row: HashMap<String, u64>,
    dim: usize,
}

impl AnnIndex {
    /// Train on a sample of the embeddings and encode them all. CONCEPT:EG-KG.storage.arena-row-append — the
    /// store now holds embeddings in ONE contiguous row-major `data` buffer (`dim`
    /// floats per row) with a parallel `ids` table, so the index builds by streaming
    /// `data.chunks_exact(dim)` instead of iterating a scattered `HashMap`. `ids` must
    /// be non-empty and `data.len() == ids.len() * dim` (the resident arena).
    pub fn build(ids: &[String], data: &[f32], dim: usize) -> Option<Self> {
        if dim == 0 || ids.is_empty() || data.len() < dim {
            return None;
        }
        let n = ids.len();

        // Train on a sample (≈ nlist×40 vectors, capped) — never the full set at
        // scale. The arena is RAM-resident so we sample contiguous rows by stride.
        let nlist = nlist_for(n);
        let sample_target = (nlist * 40).clamp(1, n);
        let stride = (n / sample_target).max(1);
        let sample: Vec<Vec<f32>> = data
            .chunks_exact(dim)
            .step_by(stride)
            .map(normalize)
            .collect();

        let params = IvfPqParams {
            dim,
            nlist,
            m: m_for(dim),
            kmeans_iters: 25,
            opq_iters: 8,
            seed: 42,
        };
        let mut index = IvfPq::train(&params, &sample);

        let mut row_to_id = Vec::with_capacity(n);
        let mut id_to_row = HashMap::with_capacity(n);
        let items: Vec<(u64, Vec<f32>)> = data
            .chunks_exact(dim)
            .zip(ids.iter())
            .enumerate()
            .map(|(row, (v, id))| {
                id_to_row.insert(id.clone(), row as u64);
                row_to_id.push(id.clone());
                (row as u64, normalize(v))
            })
            .collect();
        index.add(&items);

        Some(Self {
            index,
            row_to_id,
            id_to_row,
            dim,
        })
    }

    /// Incrementally insert/overwrite one embedding. An overwrite tombstones the
    /// node's previous row, so search always returns the latest vector. Returns
    /// `false` (no-op) if the embedding's dim doesn't match the index.
    pub fn add(&mut self, node_id: &str, embedding: &[f32]) -> bool {
        if embedding.len() != self.dim {
            return false;
        }
        if let Some(&old) = self.id_to_row.get(node_id) {
            self.index.delete(old);
        }
        let row = self.index.len() as u64;
        let v = normalize(embedding);
        self.index.add(&[(row, v)]);
        self.row_to_id.push(node_id.to_string());
        self.id_to_row.insert(node_id.to_string(), row);
        true
    }

    /// Incrementally tombstone `node_id`'s live row (CONCEPT:EG-KG.storage.incremental-ann) so
    /// search no longer returns it — no rebuild. Returns `true` if a row was
    /// tombstoned, `false` if the id was not indexed. The caller compacts once the
    /// tombstone ratio crosses the threshold.
    pub fn remove(&mut self, node_id: &str) -> bool {
        match self.id_to_row.remove(node_id) {
            Some(old) => {
                self.index.delete(old);
                true
            }
            None => false,
        }
    }

    /// kNN cosine search. Returns `(node_id, cosine_similarity)` descending.
    pub fn search(&self, query: &[f32], n_results: usize) -> Vec<(String, f32)> {
        self.search_filtered(query, n_results, |_| true)
    }

    /// kNN cosine search with a node-id metadata pre-filter (CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter). `allow`
    /// is pushed INTO the eg-ann scan (translated node-id → external row id), so the
    /// returned top-k already satisfies the predicate rather than being over-fetched
    /// and post-filtered. Returns `(node_id, cosine_similarity)` descending.
    pub fn search_filtered(
        &self,
        query: &[f32],
        n_results: usize,
        allow: impl Fn(&str) -> bool,
    ) -> Vec<(String, f32)> {
        if query.len() != self.dim {
            return Vec::new();
        }
        let q = normalize(query);
        let sp = SearchParams {
            nprobe: 32,
            refine: true,
            refine_factor: 16,
        };
        // eg-ann is integer-keyed; the external id equals the `row_to_id` index (ids are
        // assigned densely and re-densified on compaction), so map id → node-id → test.
        let pred = |ext_id: u64| -> bool {
            self.row_to_id
                .get(ext_id as usize)
                .map(|id| allow(id.as_str()))
                .unwrap_or(false)
        };
        self.index
            .search_filtered(&q, n_results, sp, Some(&pred))
            .into_iter()
            .filter_map(|r| {
                self.row_to_id
                    .get(r.id as usize)
                    // squared-L2 of unit vectors = 2(1 − cos) ⇒ cos = 1 − d/2.
                    .map(|id| (id.clone(), 1.0 - r.distance / 2.0))
            })
            .collect()
    }

    /// Drop tombstones via the eg-ann compaction (VACUUM) and re-derive the id map.
    pub fn compact(&mut self) {
        let compacted = eg_ann::compact(&self.index);
        // Rebuild the id maps over the surviving rows (compaction renumbers rows).
        let mut row_to_id = Vec::with_capacity(compacted.len());
        let mut id_to_row = HashMap::with_capacity(compacted.len());
        for (new_row, &id_u64) in compacted.ids.iter().enumerate() {
            // `ids` carries the OLD row id we assigned; map it back to the node id.
            if let Some(node) = self.row_to_id.get(id_u64 as usize) {
                id_to_row.insert(node.clone(), new_row as u64);
                row_to_id.push(node.clone());
            } else {
                row_to_id.push(String::new());
            }
        }
        // Re-key the compacted index rows to dense 0..n so future ids stay unique.
        let mut renumbered = compacted;
        for (i, slot) in renumbered.ids.iter_mut().enumerate() {
            *slot = i as u64;
        }
        self.index = renumbered;
        self.row_to_id = row_to_id;
        self.id_to_row = id_to_row;
    }

    pub fn live_len(&self) -> usize {
        self.index.live_len()
    }

    /// Tombstone fraction (drives the deferred-compaction trigger).
    pub fn tombstone_ratio(&self) -> f32 {
        self.index.tombstone_ratio()
    }

    /// Persist the index (codes + meta) to `dir`. Reopen is `load` — NO rebuild.
    pub fn save(&self, dir: &Path) -> std::io::Result<()> {
        eg_ann::save(&self.index, dir)?;
        // The String id map lives beside the codes (eg-ann is integer-keyed).
        let map_bytes = bincode_serialize_ids(&self.row_to_id);
        std::fs::write(dir.join("ids.bin"), map_bytes)
    }

    /// Reopen a persisted index WITHOUT rebuilding from raw vectors.
    pub fn load(dir: &Path) -> std::io::Result<Self> {
        let index = eg_ann::open(dir)?;
        let dim = index.dim;
        let row_to_id = bincode_deserialize_ids(&std::fs::read(dir.join("ids.bin"))?)?;
        let mut id_to_row = HashMap::with_capacity(row_to_id.len());
        for (row, id) in row_to_id.iter().enumerate() {
            if !id.is_empty() {
                id_to_row.insert(id.clone(), row as u64);
            }
        }
        Ok(Self {
            index,
            row_to_id,
            id_to_row,
            dim,
        })
    }
}

fn bincode_serialize_ids(ids: &[String]) -> Vec<u8> {
    // Hand-rolled length-prefixed encoding (no extra dep): u64 count, then each
    // u32 len + utf8 bytes.
    let mut out = Vec::new();
    out.extend_from_slice(&(ids.len() as u64).to_le_bytes());
    for s in ids {
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    out
}

fn bincode_deserialize_ids(bytes: &[u8]) -> std::io::Result<Vec<String>> {
    let err = || std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt ids.bin");
    if bytes.len() < 8 {
        return Err(err());
    }
    let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(count);
    let mut off = 8usize;
    for _ in 0..count {
        if off + 4 > bytes.len() {
            return Err(err());
        }
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + len > bytes.len() {
            return Err(err());
        }
        out.push(String::from_utf8_lossy(&bytes[off..off + len]).into_owned());
        off += len;
    }
    Ok(out)
}
