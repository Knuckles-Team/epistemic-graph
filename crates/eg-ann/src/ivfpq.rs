//! IVF-PQ index with an OPQ rotation and an SQ8 refine tier (CONCEPT:KG-2.207).
//!
//! Pipeline (the production recall recipe the spike deferred):
//!   1. **OPQ rotation** `R` — an orthogonal `dim*dim` matrix learned at train
//!      time that re-aligns the embedding axes so product-quantization distortion
//!      is minimised. Every vector is rotated by `R` before coarse-assignment and
//!      PQ encoding; queries are rotated the same way. This is the single biggest
//!      lever on raw-PQ candidate quality.
//!   2. **IVF coarse quantizer** — `nlist` k-means cells; a vector lands in its
//!      nearest cell's posting list. Search probes the `nprobe` nearest cells.
//!   3. **PQ codes** — the rotated residual (vector − cell centroid) is split into
//!      `m` subspaces, each encoded to one of 256 codebook entries → `m` bytes per
//!      vector. Search scores candidates with an Asymmetric Distance Computation
//!      (ADC) table: integer code → precomputed sub-distance, summed. No decode.
//!   4. **SQ8 refine tier** — a scalar-quantized (1 byte/dim) copy of each rotated
//!      vector. Search over-fetches `refine_factor*k` ADC candidates, then re-ranks
//!      them with the near-exact SQ8 distance. THIS is what recovers recall to the
//!      production target: ADC finds the right neighbourhood cheaply, SQ8 orders it
//!      accurately. The SQ8 tier costs `dim` bytes/vec (≈ the codes again), kept as
//!      a separate mmappable buffer so it can live on disk.

use crate::kmeans::{kmeans, nearest_centroid, sq_dist};
use crate::linalg::{identity, orthogonal_polar, rotate, transpose};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

/// PQ codebook entries per subspace (8-bit codes).
pub const PQ_KSUB: usize = 256;

/// One encoded row produced by `add`: (id, ivf-cell, PQ code, SQ8 code, sq_min, sq_scale).
type EncodedRow = (u64, u32, Vec<u8>, Vec<u8>, f32, f32);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IvfPqParams {
    /// Embedding dimension.
    pub dim: usize,
    /// Number of IVF (coarse) cells. Rule of thumb ≈ √N.
    pub nlist: usize,
    /// Number of PQ subquantizers (`dim` must be divisible by `m`).
    pub m: usize,
    /// Lloyd iterations for the coarse + PQ k-means.
    pub kmeans_iters: usize,
    /// OPQ outer iterations (rotation ↔ PQ-retrain alternations). 0 ⇒ identity
    /// rotation (plain IVF-PQ, no OPQ).
    pub opq_iters: usize,
    /// Train RNG seed.
    pub seed: u64,
}

impl Default for IvfPqParams {
    fn default() -> Self {
        Self {
            dim: 768,
            nlist: 1024,
            m: 96,
            kmeans_iters: 25,
            opq_iters: 8,
            seed: 42,
        }
    }
}

/// A single hit: external id + (approximate, then refined) distance — smaller is
/// nearer. The semantic layer converts to a cosine-like similarity.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
    pub id: u64,
    pub distance: f32,
}

/// Per-search tuning. `nprobe` cells probed; `refine_factor*k` ADC candidates are
/// over-fetched and re-ranked by the SQ8 tier (set `refine = false` to skip it).
#[derive(Clone, Copy, Debug)]
pub struct SearchParams {
    pub nprobe: usize,
    pub refine: bool,
    pub refine_factor: usize,
}

impl Default for SearchParams {
    fn default() -> Self {
        Self {
            nprobe: 16,
            refine: true,
            refine_factor: 8,
        }
    }
}

/// The trained, persistable IVF-PQ+OPQ index with its SQ8 refine tier.
#[derive(Serialize, Deserialize)]
pub struct IvfPq {
    pub dim: usize,
    pub nlist: usize,
    pub m: usize,
    pub dsub: usize,
    /// OPQ rotation, row-major `dim*dim`. Identity when `opq_iters == 0`.
    pub rotation: Vec<f32>,
    /// IVF coarse centroids in the **rotated** space (`nlist*dim`).
    pub coarse_centroids: Vec<f32>,
    /// PQ codebooks in the **rotated residual** space (`m*256*dsub`).
    pub pq_centroids: Vec<f32>,
    /// PQ codes, `N*m` u8 — the bulk (mmappable on disk).
    pub codes: Vec<u8>,
    /// SQ8 refine tier: per-vector min/scale + `N*dim` u8 quantized rotated
    /// vectors. `sq_min`/`sq_scale` are per-row so each vector quantizes tightly.
    pub sq_codes: Vec<u8>,
    pub sq_min: Vec<f32>,
    pub sq_scale: Vec<f32>,
    /// External ids, parallel to rows.
    pub ids: Vec<u64>,
    /// Which IVF cell each row landed in (lets us rebuild postings in one pass).
    pub list_of: Vec<u32>,
    /// Tombstones: 1 byte/row (1 = deleted).
    pub deleted: Vec<u8>,
    /// Live posting lists, rebuilt from `list_of` on load (never serialized).
    #[serde(skip)]
    pub postings: Vec<Vec<u32>>,
}

impl IvfPq {
    /// Train the OPQ rotation + coarse + PQ codebooks on a representative sample.
    /// Returns an empty (no rows) index ready for `add`.
    // CONCEPT:EG-011 — span ANN index-build (train) throughput; no-op without a subscriber.
    #[tracing::instrument(
        skip(params, training),
        fields(n_training = training.len(), nlist = params.nlist, m = params.m)
    )]
    pub fn train(params: &IvfPqParams, training: &[Vec<f32>]) -> Self {
        let dim = params.dim;
        assert!(
            params.m > 0 && dim.is_multiple_of(params.m),
            "dim must be divisible by m"
        );
        assert!(!training.is_empty(), "training set must be non-empty");
        let dsub = dim / params.m;
        let mut rng = ChaCha8Rng::seed_from_u64(params.seed);

        // ---- OPQ: alternate (rotate data → train PQ → update rotation). The
        // coarse quantizer is trained ONCE on the rotated data after R converges;
        // the PQ codebooks here are bootstrap copies refined inside the loop.
        let mut rotation = identity(dim);
        let mut pq_centroids = vec![0.0f32; params.m * PQ_KSUB * dsub];

        for it in 0..params.opq_iters.max(1) {
            // Rotate the (whole-vector) training set by the current R.
            let rotated: Vec<Vec<f32>> = training
                .par_iter()
                .map(|v| rotate(&rotation, v, dim))
                .collect();

            // Train PQ subspace codebooks directly on the rotated vectors (OPQ
            // optimises the joint rotation+quantizer; the coarse residual is folded
            // in once R is fixed, below).
            pq_centroids = train_pq(
                &rotated,
                dim,
                params.m,
                dsub,
                params.kmeans_iters,
                params.seed,
            );

            // On the last OPQ iteration we keep R fixed; otherwise update it.
            if params.opq_iters == 0 || it + 1 == params.opq_iters.max(1) {
                break;
            }

            // Reconstruct each rotated vector from its PQ codes, then update R to
            // the orthogonal matrix best mapping originals → reconstructions:
            //   R = polar(  Σ x_i · x̂_iᵀ )  where x̂ is the PQ reconstruction.
            // Accumulate M = Σ recon · originalᵀ (rotated-space recon vs raw input).
            let m_acc = training
                .par_iter()
                .zip(rotated.par_iter())
                .map(|(orig, rot)| {
                    let recon = pq_reconstruct(rot, &pq_centroids, params.m, dsub);
                    outer_product(&recon, orig, dim)
                })
                .reduce(
                    || vec![0.0f64; dim * dim],
                    |mut a, b| {
                        for i in 0..a.len() {
                            a[i] += b[i];
                        }
                        a
                    },
                );
            let m_f32: Vec<f32> = m_acc.iter().map(|&x| x as f32).collect();
            // R = U Vᵀ from SVD(M); apply as the new rotation (Rᵀ maps raw→rotated).
            rotation = transpose(&orthogonal_polar(&m_f32, dim), dim);
        }

        // ---- Coarse quantizer over the ROTATED training vectors.
        let rotated: Vec<Vec<f32>> = training
            .par_iter()
            .map(|v| rotate(&rotation, v, dim))
            .collect();
        let coarse_centroids = kmeans(&rotated, dim, params.nlist, params.kmeans_iters, &mut rng);

        // ---- Re-train PQ on the rotated RESIDUALS (vector − its coarse centroid),
        // which is what `add` encodes — the final production codebooks.
        let residuals: Vec<Vec<f32>> = rotated
            .par_iter()
            .map(|rv| {
                let c = nearest_centroid(rv, &coarse_centroids, dim, params.nlist);
                let base = c * dim;
                (0..dim)
                    .map(|d| rv[d] - coarse_centroids[base + d])
                    .collect()
            })
            .collect();
        let pq_centroids = train_pq(
            &residuals,
            dim,
            params.m,
            dsub,
            params.kmeans_iters,
            params.seed,
        );

        IvfPq {
            dim,
            nlist: params.nlist,
            m: params.m,
            dsub,
            rotation,
            coarse_centroids,
            pq_centroids,
            codes: Vec::new(),
            sq_codes: Vec::new(),
            sq_min: Vec::new(),
            sq_scale: Vec::new(),
            ids: Vec::new(),
            list_of: Vec::new(),
            deleted: Vec::new(),
            postings: vec![Vec::new(); params.nlist],
        }
    }

    /// Encode + append a batch of `(id, vector)`. Append-only IVF-list insert — no
    /// rebuild. Both the PQ code and the SQ8 refine row are written.
    // CONCEPT:EG-011 — span ANN incremental-build (add) throughput; no-op without a subscriber.
    #[tracing::instrument(skip(self, items), fields(n_items = items.len(), dim = self.dim))]
    pub fn add(&mut self, items: &[(u64, Vec<f32>)]) {
        let dim = self.dim;
        let encoded: Vec<EncodedRow> = items
            .par_iter()
            .map(|(id, v)| {
                let rv = rotate(&self.rotation, v, dim);
                let c = nearest_centroid(&rv, &self.coarse_centroids, dim, self.nlist);
                let base = c * dim;
                let resid: Vec<f32> = (0..dim)
                    .map(|d| rv[d] - self.coarse_centroids[base + d])
                    .collect();
                let code = self.encode_residual(&resid);
                let (sq, mn, sc) = sq8_encode(&rv);
                (*id, c as u32, code, sq, mn, sc)
            })
            .collect();

        for (id, cell, code, sq, mn, sc) in encoded {
            let row = self.ids.len() as u32;
            self.ids.push(id);
            self.list_of.push(cell);
            self.deleted.push(0);
            self.codes.extend_from_slice(&code);
            self.sq_codes.extend_from_slice(&sq);
            self.sq_min.push(mn);
            self.sq_scale.push(sc);
            self.postings[cell as usize].push(row);
        }
    }

    /// Tombstone every row carrying `id` (an overwrite re-adds then deletes the old
    /// row). Returns the number tombstoned.
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

    fn encode_residual(&self, resid: &[f32]) -> Vec<u8> {
        let dsub = self.dsub;
        (0..self.m)
            .map(|sq| {
                let sv = &resid[sq * dsub..(sq + 1) * dsub];
                let book_base = sq * PQ_KSUB * dsub;
                let mut best = 0u8;
                let mut bestd = f32::MAX;
                for k in 0..PQ_KSUB {
                    let cb = book_base + k * dsub;
                    let mut d = 0.0f32;
                    for (a, b) in sv.iter().zip(&self.pq_centroids[cb..cb + dsub]) {
                        let diff = a - b;
                        d += diff * diff;
                    }
                    if d < bestd {
                        bestd = d;
                        best = k as u8;
                    }
                }
                best
            })
            .collect()
    }

    /// Rebuild in-RAM posting lists from `list_of` (after a no-rebuild mmap load).
    /// One O(N) integer pass — NO vector math, NO k-means, NO graph build.
    pub fn rebuild_postings(&mut self) {
        let mut postings = vec![Vec::new(); self.nlist];
        for (row, &cell) in self.list_of.iter().enumerate() {
            postings[cell as usize].push(row as u32);
        }
        self.postings = postings;
    }

    /// kNN search: probe `nprobe` cells, ADC-score the candidates, then (if
    /// `refine`) re-rank the top `refine_factor*k` with the SQ8 tier for recall.
    /// A thin wrapper over [`Self::search_filtered`] with no candidate pre-filter.
    pub fn search(&self, query: &[f32], k: usize, sp: SearchParams) -> Vec<SearchResult> {
        self.search_filtered(query, k, sp, None)
    }

    /// kNN search with an optional metadata pre-filter (CONCEPT:EG-070). `allow`, when
    /// present, is tested against each candidate's EXTERNAL id (`self.ids[row]`) DURING
    /// the ADC probe/scan, so disallowed rows never enter the candidate heap and the
    /// returned top-k already satisfies the predicate — no over-fetch-then-post-filter.
    /// `allow == None` is exactly the unfiltered `search`. The refine tier re-ranks only
    /// the rows that already passed the filter, so recall is preserved over the allowed
    /// subset.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        sp: SearchParams,
        allow: Option<&dyn Fn(u64) -> bool>,
    ) -> Vec<SearchResult> {
        let dim = self.dim;
        let dsub = self.dsub;
        let rq = rotate(&self.rotation, query, dim);

        // 1. nprobe nearest coarse cells (in rotated space).
        let mut cell_d: Vec<(usize, f32)> = (0..self.nlist)
            .map(|c| {
                (
                    c,
                    sq_dist(&rq, &self.coarse_centroids[c * dim..(c + 1) * dim]),
                )
            })
            .collect();
        cell_d.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        cell_d.truncate(sp.nprobe.max(1));

        // 2. ADC over each probed cell's posting list.
        let mut cands: Vec<(usize, f32)> = Vec::new(); // (row, adc_dist)
        for &(cell, _) in &cell_d {
            let base = cell * dim;
            let qresid: Vec<f32> = (0..dim)
                .map(|d| rq[d] - self.coarse_centroids[base + d])
                .collect();
            // ADC table: m x 256 squared sub-distances.
            let mut table = vec![0.0f32; self.m * PQ_KSUB];
            for sq in 0..self.m {
                let qs = &qresid[sq * dsub..(sq + 1) * dsub];
                let book_base = sq * PQ_KSUB * dsub;
                for kc in 0..PQ_KSUB {
                    let cb = book_base + kc * dsub;
                    let mut d = 0.0f32;
                    for (a, b) in qs.iter().zip(&self.pq_centroids[cb..cb + dsub]) {
                        let diff = a - b;
                        d += diff * diff;
                    }
                    table[sq * PQ_KSUB + kc] = d;
                }
            }
            for &row in &self.postings[cell] {
                let row = row as usize;
                if self.deleted[row] == 1 {
                    continue;
                }
                // CONCEPT:EG-070 — metadata pre-filter DURING the scan: skip rows whose
                // external id is not permitted, so the ADC/refine top-k is built only
                // over the allowed subset.
                if let Some(allow) = allow {
                    if !allow(self.ids[row]) {
                        continue;
                    }
                }
                let cbase = row * self.m;
                let mut dist = 0.0f32;
                for sq in 0..self.m {
                    let code = self.codes[cbase + sq] as usize;
                    dist += table[sq * PQ_KSUB + code];
                }
                cands.push((row, dist));
            }
        }

        if !sp.refine || self.sq_codes.is_empty() {
            cands.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            cands.truncate(k);
            return cands
                .into_iter()
                .map(|(row, d)| SearchResult {
                    id: self.ids[row],
                    distance: d,
                })
                .collect();
        }

        // 3. Refine: keep the best refine_factor*k ADC candidates, re-rank by SQ8
        // distance (near-exact in the rotated space).
        let keep = (sp.refine_factor.max(1) * k).min(cands.len());
        if keep < cands.len() {
            cands.select_nth_unstable_by(keep.saturating_sub(1), |a, b| {
                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            cands.truncate(keep);
        }
        let mut refined: Vec<SearchResult> = cands
            .into_iter()
            .map(|(row, _)| SearchResult {
                id: self.ids[row],
                distance: self.sq8_dist(&rq, row),
            })
            .collect();
        refined.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        refined.truncate(k);
        refined
    }

    /// Squared distance between a rotated query and row's SQ8-decoded rotated vec.
    #[inline]
    fn sq8_dist(&self, rq: &[f32], row: usize) -> f32 {
        let dim = self.dim;
        let mn = self.sq_min[row];
        let sc = self.sq_scale[row];
        let base = row * dim;
        let mut d = 0.0f32;
        for (q, code) in rq.iter().zip(&self.sq_codes[base..base + dim]) {
            let val = mn + (*code as f32) * sc;
            let diff = q - val;
            d += diff * diff;
        }
        d
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
    /// Count of live (non-tombstoned) rows.
    pub fn live_len(&self) -> usize {
        self.deleted.iter().filter(|&&d| d == 0).count()
    }
    /// Tombstone fraction in `[0,1]` (drives compaction).
    pub fn tombstone_ratio(&self) -> f32 {
        if self.ids.is_empty() {
            return 0.0;
        }
        let dead = self.deleted.iter().filter(|&&d| d == 1).count();
        dead as f32 / self.ids.len() as f32
    }
}

/// Merge per-shard kNN result lists into ONE global top-k (CONCEPT:EG-069). This is
/// the gather half of a cross-shard scatter-gather: each shard returns its LOCAL
/// top-k [`SearchResult`]s (smaller `distance` = nearer) over a SHARED id space, and
/// this reduces them to the globally-nearest `k`. A bounded binary-heap keeps only the
/// `k` best seen so far — the heap root is the CURRENT worst kept hit (largest
/// distance), so a new candidate nearer than the root evicts it. O(total · log k) time,
/// O(k) memory: the full cross-shard candidate list is never materialized. The result
/// is sorted nearest-first, identical to what a single combined index would return.
pub fn merge_topk(shards: &[Vec<SearchResult>], k: usize) -> Vec<SearchResult> {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    if k == 0 {
        return Vec::new();
    }

    // Max-heap on distance: `Ord` puts the LARGEST distance at the root so it is the
    // first evicted once the heap holds `k`. NaN distances sort as "worst" (evicted
    // first), never poisoning the ordering.
    struct Worst(SearchResult);
    impl PartialEq for Worst {
        fn eq(&self, o: &Self) -> bool {
            self.0.distance == o.0.distance
        }
    }
    impl Eq for Worst {}
    impl PartialOrd for Worst {
        fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Worst {
        fn cmp(&self, o: &Self) -> Ordering {
            self.0
                .distance
                .partial_cmp(&o.0.distance)
                .unwrap_or(Ordering::Equal)
        }
    }

    let mut heap: BinaryHeap<Worst> = BinaryHeap::with_capacity(k + 1);
    for shard in shards {
        for r in shard {
            if heap.len() < k {
                heap.push(Worst(r.clone()));
            } else if let Some(top) = heap.peek() {
                if r.distance < top.0.distance {
                    heap.pop();
                    heap.push(Worst(r.clone()));
                }
            }
        }
    }
    let mut out: Vec<SearchResult> = heap.into_iter().map(|w| w.0).collect();
    out.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(Ordering::Equal)
    });
    out
}

/// Deterministic gather for the cross-shard scatter (CONCEPT:EG-319). Like
/// [`merge_topk`] but under a TOTAL order — distance ascending, then id ascending —
/// so ties resolve to the SMALLER id regardless of which shard delivered a hit or in
/// what order shards answered. This is what makes a scattered kNN reproducible: two
/// candidates at the same distance can straddle the k-boundary, and without an id
/// tiebreak which one is kept would depend on shard arrival order. Same bounded-heap
/// budget as [`merge_topk`]: O(total · log k) time, O(k) memory; the output order is
/// identical to a single combined index that sorts by `(distance, id)` (the
/// convention [`crate::FlatIndex`] uses for its ground-truth top-k).
pub fn merge_topk_stable(shards: &[Vec<SearchResult>], k: usize) -> Vec<SearchResult> {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    if k == 0 {
        return Vec::new();
    }

    // Total "worseness" order: a is worse (evicted sooner) if it is farther, or — at
    // equal distance — has the LARGER id. A NaN distance is treated as the worst.
    #[inline]
    fn worse_than(a: &SearchResult, b: &SearchResult) -> Ordering {
        match a.distance.partial_cmp(&b.distance) {
            Some(Ordering::Equal) | None => a.id.cmp(&b.id),
            Some(o) => o,
        }
    }

    // Max-heap on the total order: the root is the CURRENT worst kept hit, so a
    // candidate that is strictly better (by `(distance, id)`) than the root evicts it.
    struct Worst(SearchResult);
    impl PartialEq for Worst {
        fn eq(&self, o: &Self) -> bool {
            self.0.id == o.0.id && self.0.distance == o.0.distance
        }
    }
    impl Eq for Worst {}
    impl PartialOrd for Worst {
        fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Worst {
        fn cmp(&self, o: &Self) -> Ordering {
            worse_than(&self.0, &o.0)
        }
    }

    let mut heap: BinaryHeap<Worst> = BinaryHeap::with_capacity(k + 1);
    for shard in shards {
        for r in shard {
            if heap.len() < k {
                heap.push(Worst(r.clone()));
            } else if let Some(top) = heap.peek() {
                // Keep the candidate iff it is strictly better than the worst kept.
                if worse_than(r, &top.0) == Ordering::Less {
                    heap.pop();
                    heap.push(Worst(r.clone()));
                }
            }
        }
    }
    let mut out: Vec<SearchResult> = heap.into_iter().map(|w| w.0).collect();
    out.sort_by(|a, b| worse_than(a, b));
    out
}

/// Train `m` PQ subspace codebooks (each 256 entries of `dsub`) over `vectors`.
fn train_pq(
    vectors: &[Vec<f32>],
    dim: usize,
    m: usize,
    dsub: usize,
    iters: usize,
    seed: u64,
) -> Vec<f32> {
    let mut out = vec![0.0f32; m * PQ_KSUB * dsub];
    let books: Vec<Vec<f32>> = (0..m)
        .into_par_iter()
        .map(|sq| {
            let sub: Vec<Vec<f32>> = vectors
                .iter()
                .map(|v| v[sq * dsub..(sq + 1) * dsub].to_vec())
                .collect();
            let mut srng =
                ChaCha8Rng::seed_from_u64(seed ^ (0x9E3779B9u64.wrapping_mul(sq as u64 + 1)));
            kmeans(&sub, dsub, PQ_KSUB, iters, &mut srng)
        })
        .collect();
    for (sq, book) in books.into_iter().enumerate() {
        let base = sq * PQ_KSUB * dsub;
        out[base..base + book.len()].copy_from_slice(&book);
    }
    let _ = dim;
    out
}

/// Reconstruct a vector from its nearest PQ codewords (per subspace).
fn pq_reconstruct(v: &[f32], pq: &[f32], m: usize, dsub: usize) -> Vec<f32> {
    let mut out = vec![0.0f32; m * dsub];
    for sq in 0..m {
        let sv = &v[sq * dsub..(sq + 1) * dsub];
        let book_base = sq * PQ_KSUB * dsub;
        let mut best = 0usize;
        let mut bestd = f32::MAX;
        for k in 0..PQ_KSUB {
            let cb = book_base + k * dsub;
            let mut d = 0.0f32;
            for j in 0..dsub {
                let diff = sv[j] - pq[cb + j];
                d += diff * diff;
            }
            if d < bestd {
                bestd = d;
                best = k;
            }
        }
        let cb = book_base + best * dsub;
        out[sq * dsub..(sq + 1) * dsub].copy_from_slice(&pq[cb..cb + dsub]);
    }
    out
}

/// `Σ a · bᵀ` accumulated in f64 (row-major `dim*dim`), for the OPQ R update.
fn outer_product(a: &[f32], b: &[f32], dim: usize) -> Vec<f64> {
    let mut m = vec![0.0f64; dim * dim];
    for i in 0..dim {
        let ai = a[i] as f64;
        if ai == 0.0 {
            continue;
        }
        let row = &mut m[i * dim..(i + 1) * dim];
        for j in 0..dim {
            row[j] += ai * b[j] as f64;
        }
    }
    m
}

/// SQ8-encode a vector: per-row min/scale, each dim → u8. Returns `(codes,min,scale)`.
fn sq8_encode(v: &[f32]) -> (Vec<u8>, f32, f32) {
    let mut mn = f32::MAX;
    let mut mx = f32::MIN;
    for &x in v {
        if x < mn {
            mn = x;
        }
        if x > mx {
            mx = x;
        }
    }
    let range = mx - mn;
    let scale = if range > 0.0 { range / 255.0 } else { 1.0 };
    let inv = if range > 0.0 { 255.0 / range } else { 0.0 };
    let codes = v
        .iter()
        .map(|&x| ((x - mn) * inv).round().clamp(0.0, 255.0) as u8)
        .collect();
    (codes, mn, scale)
}
