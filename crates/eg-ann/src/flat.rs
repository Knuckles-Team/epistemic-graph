//! FlatIndex — an exact (brute-force) kNN index + an ANN-candidate rerank stage
//! (CONCEPT:EG-297).
//!
//! The [`IvfPq`](crate::IvfPq) index is *approximate*: it probes `nprobe` cells and
//! scores candidates from lossy PQ/SQ8 codes, so its top-k can miss a true
//! neighbour. This module adds the exact counterpart alongside it (it does NOT
//! change IVF-PQ):
//!
//!   * [`FlatIndex`] — stores the FULL f32 vectors and answers `search(query, k,
//!     metric)` by scanning every live row and returning the exact top-k by true
//!     distance. This is the correctness ground truth for small/medium sets and the
//!     reference the recall harness measures ANN against.
//!   * [`FlatIndex::rerank`] / [`FlatIndex::refine_ann`] — the standard
//!     "ANN recall + exact rerank = high precision" pattern: the ANN over-fetches
//!     `>k` candidate ids cheaply, then we recompute EXACT distances on the full
//!     vectors and return the precise top-k. ADC/SQ8 find the right neighbourhood;
//!     the exact rerank orders it perfectly.
//!
//! Three metrics are supported ([`Metric`]); every one is expressed so that a
//! SMALLER [`SearchResult::distance`] means NEARER — identical to the ordering
//! convention IVF-PQ, `merge_topk`, and the refine tier already use — so flat and
//! ANN results are directly comparable and mergeable.

use crate::ivfpq::SearchResult;
use crate::kmeans::sq_dist;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The distance/similarity metric a [`FlatIndex`] search scores by (CONCEPT:EG-297).
///
/// All three are normalised to a "smaller = nearer" scalar so they sort ascending
/// like every other distance in the crate:
///   * [`Metric::L2`] — squared Euclidean distance (matches `kmeans::sq_dist`, the
///     metric the IVF-PQ tier itself minimises, so it is the right ground truth for
///     ANN recall).
///   * [`Metric::Cosine`] — cosine *distance* `1 − cos(a,b)` in `[0, 2]`; a zero
///     vector has undefined direction and is treated as maximally far (`1.0`).
///   * [`Metric::InnerProduct`] — the NEGATED inner product `−⟨a,b⟩`, so a larger
///     (more positive) dot product yields a smaller distance (maximum-inner-product
///     search under the same ascending order).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Metric {
    L2,
    Cosine,
    InnerProduct,
}

impl Default for Metric {
    fn default() -> Self {
        Metric::L2
    }
}

impl Metric {
    /// True "smaller = nearer" distance between two equal-length vectors under this
    /// metric (CONCEPT:EG-297). Panics only on a length mismatch (a programming
    /// error), never on data values.
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "metric operands must share dimension");
        match self {
            Metric::L2 => sq_dist(a, b),
            Metric::Cosine => {
                let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
                for (x, y) in a.iter().zip(b.iter()) {
                    dot += x * y;
                    na += x * x;
                    nb += y * y;
                }
                let denom = na.sqrt() * nb.sqrt();
                if denom <= 0.0 {
                    // A zero-norm vector has no direction — treat as fully dissimilar.
                    1.0
                } else {
                    1.0 - dot / denom
                }
            }
            Metric::InnerProduct => {
                let mut dot = 0.0f32;
                for (x, y) in a.iter().zip(b.iter()) {
                    dot += x * y;
                }
                -dot
            }
        }
    }
}

/// A brute-force EXACT kNN index over full f32 vectors (CONCEPT:EG-297).
///
/// Every vector is stored verbatim in one contiguous `N*dim` buffer (row `i` is
/// `vectors[i*dim .. (i+1)*dim]`, external id `ids[i]`). Rows are append-only; a
/// tombstone byte per row supports exclude-on-delete without reshuffling. Search is
/// an O(N·dim) scan — the exact ground truth, not a production ANN. Byte-accounted
/// via [`FlatIndex::byte_size`] and serde-serializable for a no-recompute reload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlatIndex {
    /// Embedding dimension.
    pub dim: usize,
    /// External ids, parallel to the row blocks in `vectors`.
    pub ids: Vec<u64>,
    /// Full vectors, row-major `N*dim` f32 (the exact payload — no quantization).
    pub vectors: Vec<f32>,
    /// Tombstones: 1 byte/row (1 = deleted, excluded from search).
    pub deleted: Vec<u8>,
}

impl FlatIndex {
    /// An empty index for `dim`-dimensional vectors.
    pub fn new(dim: usize) -> Self {
        assert!(dim > 0, "dim must be positive");
        Self {
            dim,
            ids: Vec::new(),
            vectors: Vec::new(),
            deleted: Vec::new(),
        }
    }

    /// Append `(id, vector)` rows. Each vector's length must equal `dim`.
    pub fn add(&mut self, items: &[(u64, Vec<f32>)]) {
        self.vectors.reserve(items.len() * self.dim);
        self.ids.reserve(items.len());
        self.deleted.reserve(items.len());
        for (id, v) in items {
            assert_eq!(v.len(), self.dim, "vector length must equal dim");
            self.ids.push(*id);
            self.vectors.extend_from_slice(v);
            self.deleted.push(0);
        }
    }

    /// Number of stored rows (including tombstoned ones).
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether the index holds no rows.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Count of live (non-tombstoned) rows.
    pub fn live_len(&self) -> usize {
        self.deleted.iter().filter(|&&d| d == 0).count()
    }

    /// Tombstone every row carrying `id`. Returns the number tombstoned.
    pub fn delete(&mut self, id: u64) -> usize {
        let mut n = 0;
        for (row, &x) in self.ids.iter().enumerate() {
            if x == id && self.deleted[row] == 0 {
                self.deleted[row] = 1;
                n += 1;
            }
        }
        n
    }

    /// The stored vector for row `i` (`i*dim .. (i+1)*dim`).
    #[inline]
    fn row(&self, i: usize) -> &[f32] {
        &self.vectors[i * self.dim..(i + 1) * self.dim]
    }

    /// Look up the FIRST live row's vector for external `id`, if present.
    pub fn vector_of(&self, id: u64) -> Option<&[f32]> {
        (0..self.ids.len())
            .find(|&i| self.ids[i] == id && self.deleted[i] == 0)
            .map(|i| self.row(i))
    }

    /// EXACT top-k: scan every live row, score by `metric`, return the k nearest as
    /// [`SearchResult`]s sorted nearest-first (CONCEPT:EG-297). Ties break by id so
    /// the order is deterministic. This is the ground truth ANN recall is measured
    /// against.
    pub fn search(&self, query: &[f32], k: usize, metric: Metric) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim, "query length must equal dim");
        if k == 0 {
            return Vec::new();
        }
        let mut scored: Vec<SearchResult> = (0..self.ids.len())
            .filter(|&i| self.deleted[i] == 0)
            .map(|i| SearchResult {
                id: self.ids[i],
                distance: metric.distance(query, self.row(i)),
            })
            .collect();
        sort_nearest_first(&mut scored);
        scored.truncate(k);
        scored
    }

    /// Rerank ANN candidate ids by EXACT distance (CONCEPT:EG-297). Given the
    /// (over-fetched) candidate `candidate_ids` an ANN returned, recompute the true
    /// `metric` distance on each candidate's FULL stored vector and return the
    /// precise top-k, nearest-first. Ids absent from the index (or tombstoned) are
    /// skipped; a duplicate id is scored once. This is the exact half of the
    /// "ANN recall + exact rerank = high precision" pipeline — the input order is
    /// irrelevant, the output is exactly ordered.
    pub fn rerank(&self, query: &[f32], candidate_ids: &[u64], k: usize) -> Vec<SearchResult> {
        self.rerank_metric(query, candidate_ids, k, Metric::L2)
    }

    /// [`FlatIndex::rerank`] under an explicit [`Metric`].
    pub fn rerank_metric(
        &self,
        query: &[f32],
        candidate_ids: &[u64],
        k: usize,
        metric: Metric,
    ) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim, "query length must equal dim");
        if k == 0 {
            return Vec::new();
        }
        // Map external id → first live row once, so rerank is O(candidates) lookups
        // plus one O(N) build (cheap next to the exact distance recompute).
        let mut row_of: HashMap<u64, usize> = HashMap::with_capacity(self.ids.len());
        for (i, &id) in self.ids.iter().enumerate() {
            if self.deleted[i] == 0 {
                row_of.entry(id).or_insert(i);
            }
        }
        let mut seen: HashMap<u64, ()> = HashMap::with_capacity(candidate_ids.len());
        let mut scored: Vec<SearchResult> = Vec::with_capacity(candidate_ids.len());
        for &id in candidate_ids {
            if seen.insert(id, ()).is_some() {
                continue; // score each distinct candidate once
            }
            if let Some(&i) = row_of.get(&id) {
                scored.push(SearchResult {
                    id,
                    distance: metric.distance(query, self.row(i)),
                });
            }
        }
        sort_nearest_first(&mut scored);
        scored.truncate(k);
        scored
    }

    /// Refine an ANN result list into a high-precision top-k (CONCEPT:EG-297).
    /// A convenience over [`FlatIndex::rerank_metric`] that takes the ANN's
    /// [`SearchResult`]s directly (the over-fetched `refine_factor*k` candidates),
    /// discards their approximate distances, and re-scores them exactly. The
    /// canonical "ANN over-fetch → exact rerank" call site.
    pub fn refine_ann(
        &self,
        query: &[f32],
        ann: &[SearchResult],
        k: usize,
        metric: Metric,
    ) -> Vec<SearchResult> {
        let ids: Vec<u64> = ann.iter().map(|r| r.id).collect();
        self.rerank_metric(query, &ids, k, metric)
    }

    /// Heap-byte footprint of the index (CONCEPT:EG-297) — the exact tier is the
    /// expensive one (full f32), so it is byte-accounted like the code buffers:
    /// `struct + vectors(4B) + ids(8B) + tombstones(1B)`, counting reserved
    /// capacity (what is actually resident).
    pub fn byte_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.vectors.capacity() * std::mem::size_of::<f32>()
            + self.ids.capacity() * std::mem::size_of::<u64>()
            + self.deleted.capacity()
    }
}

/// Sort [`SearchResult`]s nearest-first (ascending distance), breaking ties by id
/// for a deterministic, reproducible order. NaN distances sort last.
#[inline]
fn sort_nearest_first(v: &mut [SearchResult]) {
    v.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
}
