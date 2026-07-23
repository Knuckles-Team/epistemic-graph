//! FlatIndex — an exact (brute-force) kNN index + an ANN-candidate rerank stage
//! (CONCEPT:EG-KG.query.concept-5).
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
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

/// The distance/similarity metric a [`FlatIndex`] search scores by (CONCEPT:EG-KG.query.concept-5).
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Metric {
    #[default]
    L2,
    Cosine,
    InnerProduct,
}

impl Metric {
    /// True "smaller = nearer" distance between two equal-length vectors under this
    /// metric (CONCEPT:EG-KG.query.concept-5). Panics only on a length mismatch (a programming
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

/// A brute-force EXACT kNN index over full f32 vectors (CONCEPT:EG-KG.query.concept-5).
///
/// Every vector is stored verbatim in one contiguous `N*dim` buffer (row `i` is
/// `vectors[i*dim .. (i+1)*dim]`, external id `ids[i]`). Rows are append-only; a
/// tombstone byte per row supports exclude-on-delete without reshuffling. Search is
/// an O(N·dim) scan — the exact ground truth, not a production ANN. Byte-accounted
/// via [`FlatIndex::byte_size`] and serde-serializable for a no-recompute reload.
#[derive(Debug, Serialize, Deserialize)]
pub struct FlatIndex {
    /// Embedding dimension.
    pub dim: usize,
    /// External ids, parallel to the row blocks in `vectors`.
    pub ids: Vec<u64>,
    /// Full vectors, row-major `N*dim` f32 (the exact payload — no quantization).
    pub vectors: Vec<f32>,
    /// Tombstones: 1 byte/row (1 = deleted, excluded from search).
    pub deleted: Vec<u8>,
    /// Derived external-id -> row offsets directory. It is intentionally omitted
    /// from the wire image and rebuilt lazily after a reload. Keeping every row
    /// offset preserves the historical duplicate-id behavior: `vector_of` returns
    /// the first live row and `delete` tombstones every live occurrence.
    #[serde(skip, default)]
    rows_by_id: OnceLock<HashMap<u64, Vec<usize>>>,
}

impl Clone for FlatIndex {
    fn clone(&self) -> Self {
        Self {
            dim: self.dim,
            ids: self.ids.clone(),
            vectors: self.vectors.clone(),
            deleted: self.deleted.clone(),
            // A lookup directory is derived and cheap to rebuild on first use.
            // Keeping clones cold avoids copying O(N) auxiliary allocations.
            rows_by_id: OnceLock::new(),
        }
    }
}

impl PartialEq for FlatIndex {
    fn eq(&self, other: &Self) -> bool {
        self.dim == other.dim
            && self.ids == other.ids
            && self.vectors == other.vectors
            && self.deleted == other.deleted
    }
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
            rows_by_id: OnceLock::new(),
        }
    }

    /// Append `(id, vector)` rows. Each vector's length must equal `dim`.
    pub fn add(&mut self, items: &[(u64, Vec<f32>)]) {
        self.vectors.reserve(items.len() * self.dim);
        self.ids.reserve(items.len());
        self.deleted.reserve(items.len());
        for (id, v) in items {
            assert_eq!(v.len(), self.dim, "vector length must equal dim");
            let row = self.ids.len();
            self.ids.push(*id);
            self.vectors.extend_from_slice(v);
            self.deleted.push(0);
            if let Some(index) = self.rows_by_id.get_mut() {
                index.entry(*id).or_default().push(row);
            }
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
        let index = self
            .rows_by_id
            .get_or_init(|| build_row_directory(&self.ids));
        let deleted = &mut self.deleted;
        for &row in index.get(&id).into_iter().flatten() {
            if deleted[row] == 0 {
                deleted[row] = 1;
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
        self.rows_by_id()
            .get(&id)?
            .iter()
            .copied()
            .find(|&row| self.deleted[row] == 0)
            .map(|row| self.row(row))
    }

    /// Lazily build the derived external-id directory once. A loaded index pays
    /// one O(N) pass on its first point lookup/rerank/delete; all later lookups are
    /// expected O(1) plus the number of duplicate rows for that id.
    fn rows_by_id(&self) -> &HashMap<u64, Vec<usize>> {
        self.rows_by_id
            .get_or_init(|| build_row_directory(&self.ids))
    }

    /// EXACT top-k: scan every live row, score by `metric`, return the k nearest as
    /// [`SearchResult`]s sorted nearest-first (CONCEPT:EG-KG.query.concept-5). Ties break by id so
    /// the order is deterministic. This is the ground truth ANN recall is measured
    /// against.
    pub fn search(&self, query: &[f32], k: usize, metric: Metric) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim, "query length must equal dim");
        if k == 0 || self.ids.is_empty() {
            return Vec::new();
        }
        // Batch-score every row on the ACTIVE distance backend (CONCEPT:EG-KG.compute.gpu-distance-seam): the
        // GPU kernel when `gpu-cuda` is built + a device is present, else the pure-Rust
        // CPU backend — byte-for-byte identical results either way. Tombstoned rows are
        // filtered AFTER scoring (the batch is over the contiguous `vectors` buffer).
        let dists = crate::distance::batch_distances(query, &self.vectors, self.dim, metric);
        let mut scored: Vec<SearchResult> = (0..self.ids.len())
            .filter(|&i| self.deleted[i] == 0)
            .map(|i| SearchResult {
                id: self.ids[i],
                distance: dists[i],
            })
            .collect();
        truncate_nearest_first(&mut scored, k);
        scored
    }

    /// Rerank ANN candidate ids by EXACT distance (CONCEPT:EG-KG.query.concept-5). Given the
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
        // Reuse the lazy id directory instead of rebuilding an O(N) map for every
        // rerank request. Duplicate candidate ids and duplicate stored rows retain
        // the historical first-live-row semantics.
        let row_of = self.rows_by_id();
        let mut seen: HashSet<u64> = HashSet::with_capacity(candidate_ids.len());
        let mut scored: Vec<SearchResult> = Vec::with_capacity(candidate_ids.len());
        for &id in candidate_ids {
            if !seen.insert(id) {
                continue; // score each distinct candidate once
            }
            if let Some(i) = row_of
                .get(&id)
                .and_then(|rows| rows.iter().copied().find(|&row| self.deleted[row] == 0))
            {
                scored.push(SearchResult {
                    id,
                    distance: metric.distance(query, self.row(i)),
                });
            }
        }
        truncate_nearest_first(&mut scored, k);
        scored
    }

    /// Refine an ANN result list into a high-precision top-k (CONCEPT:EG-KG.query.concept-5).
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

    /// Heap-byte footprint of the index (CONCEPT:EG-KG.query.concept-5) — the exact tier is the
    /// expensive one (full f32), so it is byte-accounted like the code buffers:
    /// `struct + vectors(4B) + ids(8B) + tombstones(1B)`, counting reserved
    /// capacity (what is actually resident).
    pub fn byte_size(&self) -> usize {
        let directory_bytes = self.rows_by_id.get().map_or(0, |index| {
            index.capacity() * (std::mem::size_of::<u64>() + std::mem::size_of::<Vec<usize>>())
                + index
                    .values()
                    .map(|rows| rows.capacity() * std::mem::size_of::<usize>())
                    .sum::<usize>()
        });
        std::mem::size_of::<Self>()
            + self.vectors.capacity() * std::mem::size_of::<f32>()
            + self.ids.capacity() * std::mem::size_of::<u64>()
            + self.deleted.capacity()
            + directory_bytes
    }
}

fn build_row_directory(ids: &[u64]) -> HashMap<u64, Vec<usize>> {
    let mut index = HashMap::with_capacity(ids.len());
    for (row, &id) in ids.iter().enumerate() {
        index.entry(id).or_insert_with(Vec::new).push(row);
    }
    index
}

/// Keep and order the exact nearest `k` without sorting every scored row. Selection
/// is linear and only the retained prefix is sorted, reducing a flat exact search
/// from O(N log N) to O(N + k log k), with O(1) selection scratch.
#[inline]
fn truncate_nearest_first(v: &mut Vec<SearchResult>, k: usize) {
    if k == 0 {
        v.clear();
        return;
    }
    if v.len() > k {
        v.select_nth_unstable_by(k, nearest_cmp);
        v.truncate(k);
    }
    v.sort_unstable_by(nearest_cmp);
}

/// Total nearest-first order: finite distance ascending, id ascending on ties,
/// and NaN after every finite/infinite value. A total comparator is required by
/// partial selection and matches the public deterministic ordering contract.
#[inline]
fn nearest_cmp(a: &SearchResult, b: &SearchResult) -> std::cmp::Ordering {
    match (a.distance.is_nan(), b.distance.is_nan()) {
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        _ => a
            .distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny hand-checkable index: 2-D points at known coordinates.
    fn idx() -> FlatIndex {
        let mut f = FlatIndex::new(2);
        f.add(&[
            (10, vec![0.0, 0.0]),
            (20, vec![1.0, 0.0]),
            (30, vec![0.0, 2.0]),
            (40, vec![3.0, 4.0]),
        ]);
        f
    }

    #[test]
    fn eg297_flat_exact_topk_l2_matches_hand_computed() {
        // Squared-L2 from the origin: 10→0, 20→1, 30→4, 40→25.
        let f = idx();
        let res = f.search(&[0.0, 0.0], 3, Metric::L2);
        assert_eq!(
            res.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(res[0].distance, 0.0);
        assert_eq!(res[1].distance, 1.0);
        assert_eq!(res[2].distance, 4.0);
    }

    #[test]
    fn eg297_flat_exact_topk_cosine_matches_hand_computed() {
        // Query along +x. cosine distance = 1 − cos.
        //   20 [1,0] cos=1     → 0.0     (nearest)
        //   40 [3,4] cos=3/5   → 0.4
        //   30 [0,2] cos=0     → 1.0
        //   10 [0,0] zero-norm → 1.0     (tie, id-broken after 30)
        let f = idx();
        let res = f.search(&[1.0, 0.0], 2, Metric::Cosine);
        assert_eq!(res[0].id, 20);
        assert!(
            (res[0].distance - 0.0).abs() < 1e-6,
            "d={}",
            res[0].distance
        );
        assert_eq!(res[1].id, 40);
        assert!(
            (res[1].distance - 0.4).abs() < 1e-6,
            "d={}",
            res[1].distance
        );
    }

    #[test]
    fn eg297_flat_exact_topk_inner_product_is_max_dot() {
        // Inner product distance = −⟨q,x⟩, so the largest dot is nearest.
        // q=[1,0]: 40 dot 3 → −3, 20 dot 1 → −1, 10/30 dot 0 → 0.
        let f = idx();
        let res = f.search(&[1.0, 0.0], 2, Metric::InnerProduct);
        assert_eq!(res[0].id, 40);
        assert_eq!(res[0].distance, -3.0);
        assert_eq!(res[1].id, 20);
        assert_eq!(res[1].distance, -1.0);
    }

    #[test]
    fn eg297_metric_distance_symmetric_and_zero_norm_cosine() {
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, -1.0, 0.5];
        for m in [Metric::L2, Metric::Cosine, Metric::InnerProduct] {
            assert!((m.distance(&a, &b) - m.distance(&b, &a)).abs() < 1e-6);
        }
        // A zero vector is maximally far under cosine (no direction).
        assert_eq!(Metric::Cosine.distance(&[0.0, 0.0], &[1.0, 1.0]), 1.0);
        // Identical direction → cosine distance 0.
        assert!(Metric::Cosine.distance(&[2.0, 0.0], &[5.0, 0.0]).abs() < 1e-6);
    }

    #[test]
    fn eg297_rerank_recovers_exact_order_from_shuffled_overfetch() {
        // An ANN over-fetches candidates in an arbitrary (wrong) order and may
        // include ids not in the index; exact rerank must recover the true top-k.
        let f = idx();
        let shuffled_overfetch = vec![40u64, 30, 20, 10, 99_999];
        let res = f.rerank(&[0.0, 0.0], &shuffled_overfetch, 3);
        assert_eq!(
            res.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert!(
            !res.iter().any(|r| r.id == 99_999),
            "absent id must be skipped"
        );
        // Distances are the true (recomputed) ones, not any ANN estimate.
        assert_eq!(res[0].distance, 0.0);
        assert_eq!(res[1].distance, 1.0);
    }

    #[test]
    fn eg297_rerank_dedups_repeated_candidates() {
        let f = idx();
        let res = f.rerank(&[0.0, 0.0], &[20, 20, 10, 10, 30], 5);
        assert_eq!(
            res.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
    }

    #[test]
    fn eg297_refine_ann_reorders_ann_result_by_exact_distance() {
        // The ANN's approximate distances are deliberately in the WRONG order;
        // refine_ann discards them and re-scores exactly.
        let f = idx();
        let ann = vec![
            SearchResult {
                id: 40,
                distance: 0.01,
            },
            SearchResult {
                id: 30,
                distance: 0.02,
            },
            SearchResult {
                id: 10,
                distance: 0.03,
            },
            SearchResult {
                id: 20,
                distance: 0.04,
            },
        ];
        let res = f.refine_ann(&[0.0, 0.0], &ann, 2, Metric::L2);
        assert_eq!(res.iter().map(|r| r.id).collect::<Vec<_>>(), vec![10, 20]);
    }

    #[test]
    fn eg297_delete_excluded_from_search_and_rerank() {
        let mut f = idx();
        assert_eq!(f.delete(10), 1);
        assert_eq!(f.live_len(), 3);
        let res = f.search(&[0.0, 0.0], 4, Metric::L2);
        assert!(
            !res.iter().any(|r| r.id == 10),
            "tombstoned id must not appear"
        );
        let rr = f.rerank(&[0.0, 0.0], &[10, 20, 30], 4);
        assert!(!rr.iter().any(|r| r.id == 10));
        assert!(f.vector_of(10).is_none());
        assert!(f.vector_of(20).is_some());
    }

    #[test]
    fn point_directory_preserves_duplicate_id_semantics_and_warm_adds() {
        let mut f = FlatIndex::new(2);
        f.add(&[(7, vec![1.0, 0.0]), (7, vec![2.0, 0.0])]);

        // The first point lookup builds the directory. Appending afterwards must
        // maintain that warm directory incrementally rather than make it stale.
        assert_eq!(f.vector_of(7), Some([1.0, 0.0].as_slice()));
        f.add(&[(9, vec![9.0, 0.0]), (7, vec![3.0, 0.0])]);
        assert_eq!(f.vector_of(9), Some([9.0, 0.0].as_slice()));

        assert_eq!(f.delete(7), 3, "every duplicate row is tombstoned");
        assert!(f.vector_of(7).is_none());
        assert_eq!(f.delete(7), 0, "repeated delete is idempotent");
    }

    #[test]
    fn derived_point_directory_is_not_part_of_equality_or_wire_image() {
        let f = idx();
        assert!(f.vector_of(20).is_some(), "warm the derived directory");
        let bytes = crate::codec::serialize(&f).expect("serialize FlatIndex");
        let back: FlatIndex = crate::codec::deserialize(&bytes).expect("deserialize FlatIndex");
        assert_eq!(back, f);
        assert!(back.rows_by_id.get().is_none(), "reload starts cache-cold");
        assert_eq!(back.vector_of(20), Some([1.0, 0.0].as_slice()));
    }

    #[test]
    fn eg297_search_k_zero_and_empty_index() {
        let f = idx();
        assert!(f.search(&[0.0, 0.0], 0, Metric::L2).is_empty());
        let empty = FlatIndex::new(2);
        assert!(empty.search(&[0.0, 0.0], 5, Metric::L2).is_empty());
        assert!(empty.is_empty());
    }

    #[test]
    fn partial_topk_matches_total_full_sort_and_places_nan_last() {
        let input = vec![
            SearchResult {
                id: 9,
                distance: 3.0,
            },
            SearchResult {
                id: 5,
                distance: f32::NAN,
            },
            SearchResult {
                id: 4,
                distance: 1.0,
            },
            SearchResult {
                id: 3,
                distance: 1.0,
            },
            SearchResult {
                id: 2,
                distance: -1.0,
            },
            SearchResult {
                id: 1,
                distance: f32::INFINITY,
            },
        ];
        let mut expected = input.clone();
        expected.sort_unstable_by(nearest_cmp);
        expected.truncate(4);

        let mut got = input;
        truncate_nearest_first(&mut got, 4);

        assert_eq!(
            got.iter().map(|row| row.id).collect::<Vec<_>>(),
            expected.iter().map(|row| row.id).collect::<Vec<_>>()
        );
        assert!(got.iter().all(|row| !row.distance.is_nan()));
    }

    #[test]
    fn eg297_byte_size_accounts_vectors_ids_tombstones() {
        // Each added row costs dim*4 (f32) + 8 (id) + 1 (tombstone) bytes minimum.
        let mut f = FlatIndex::new(4);
        let base = f.byte_size();
        f.add(&[(1, vec![1.0; 4]), (2, vec![2.0; 4])]);
        let grew = f.byte_size();
        let per_row = 4 * std::mem::size_of::<f32>() + std::mem::size_of::<u64>() + 1;
        assert!(
            grew >= base + 2 * per_row,
            "byte_size {grew} must account >= {} added bytes (base {base})",
            2 * per_row
        );
    }

    #[test]
    fn eg297_serde_round_trip_preserves_vectors_and_search() {
        let f = idx();
        let before = f.search(&[0.5, 0.5], 4, Metric::L2);
        let bytes = crate::codec::serialize(&f).expect("serialize FlatIndex");
        let back: FlatIndex = crate::codec::deserialize(&bytes).expect("deserialize FlatIndex");
        assert_eq!(back.dim, f.dim);
        assert_eq!(back.ids, f.ids);
        assert_eq!(back.vectors, f.vectors);
        assert_eq!(back.deleted, f.deleted);
        let after = back.search(&[0.5, 0.5], 4, Metric::L2);
        assert_eq!(
            before, after,
            "search must be identical after a serde round-trip"
        );
    }

    #[test]
    fn eg297_metric_serde_round_trip() {
        for m in [Metric::L2, Metric::Cosine, Metric::InnerProduct] {
            let s = crate::codec::serialize(&m).unwrap();
            let back: Metric = crate::codec::deserialize(&s).unwrap();
            assert_eq!(m, back);
        }
    }
}
