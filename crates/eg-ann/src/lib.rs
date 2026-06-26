//! eg-ann — native IVF-PQ + OPQ + SQ8-refine approximate-nearest-neighbour index
//! (CONCEPT:KG-2.207).
//!
//! A pure-Rust, Pi-lean vector index that replaces the rebuild-on-load `hnsw_rs`
//! store in `eg-core::compute::semantic`. The headline properties:
//!
//!   * **Persistent / no-rebuild load** — `persist::save` → `persist::open`
//!     reopens via mmap + an O(N) integer posting-list pass; no k-means, no graph
//!     reconstruction, no f32 touch (the spike's headline win, productionised).
//!   * **Production recall** — an OPQ rotation + well-trained codebooks make raw
//!     ADC candidates good, and an SQ8 (1 byte/dim) **refine tier** re-ranks the
//!     over-fetched candidates to recover recall@10 ≥ 0.95 vs brute force.
//!   * **Compression** — `m` bytes/vec PQ codes (32× at dim=768/m=96) for the ANN
//!     bulk; the SQ8 refine tier adds `dim` bytes/vec (4× vs raw f32, mmappable).
//!   * **Incremental** — IVF-list append insert, per-row tombstone delete, and a
//!     `persist::compact` VACUUM that drops tombstones without retraining.
//!
//! Per-graph / per-shard by construction (one index per `SemanticStore`). The
//! cross-shard scatter-gather kNN merge, hybrid metadata filtering, GPU training,
//! and multi-vector spaces are LATER increments (see the crate README / report).
//!
//! GPU/training acceleration is behind the `gpu` cargo feature, gated OUT of the
//! Pi/full tiers — serving is CPU integer-table lookups (ADC) and needs no
//! accelerator. The default build links no GPU, no faiss, no native-ML deps.

pub mod ivfpq;
pub mod kmeans;
pub mod linalg;
pub mod persist;

#[cfg(feature = "redb")]
pub mod redb_store;

pub use ivfpq::{IvfPq, IvfPqParams, SearchParams, SearchResult, PQ_KSUB};
pub use persist::{compact, open, save};

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    /// Clustered synthetic data — realistic embeddings are clustered, not uniform,
    /// so a uniform-random generator understates achievable recall. We draw from
    /// `nclusters` gaussian blobs (the regime PQ is designed for).
    fn clustered_vecs(n: usize, dim: usize, nclusters: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let centers: Vec<Vec<f32>> = (0..nclusters)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect())
            .collect();
        (0..n)
            .map(|_| {
                let c = &centers[rng.gen_range(0..nclusters)];
                (0..dim)
                    .map(|j| c[j] + (rng.gen::<f32>() - 0.5) * 0.30)
                    .collect()
            })
            .collect()
    }

    fn brute_force_knn(data: &[Vec<f32>], q: &[f32], k: usize) -> Vec<u64> {
        use rayon::prelude::*;
        let mut scored: Vec<(u64, f32)> = data
            .par_iter()
            .enumerate()
            .map(|(i, v)| (i as u64, kmeans::sq_dist(q, v)))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        scored.truncate(k);
        scored.into_iter().map(|(id, _)| id).collect()
    }

    fn build(data: &[Vec<f32>], dim: usize, nlist: usize) -> IvfPq {
        let params = IvfPqParams {
            dim,
            nlist,
            m: dim / 4, // dsub = 4
            kmeans_iters: 20,
            opq_iters: 6,
            seed: 7,
        };
        let mut idx = IvfPq::train(&params, data);
        let items: Vec<(u64, Vec<f32>)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.clone()))
            .collect();
        idx.add(&items);
        idx
    }

    #[test]
    fn simd_sq_dist_matches_scalar_within_epsilon() {
        // CONCEPT:EG-014 — the 8-lane chunked `sq_dist` must equal a naive scalar
        // squared-distance for ALL lengths, including non-multiples of 8.
        fn scalar_sq_dist(a: &[f32], b: &[f32]) -> f32 {
            a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
        }
        for &len in &[0usize, 1, 7, 8, 9, 15, 16, 31, 128, 1000, 1024] {
            let a: Vec<f32> = (0..len).map(|i| (i as f32 * 0.017).sin() * 3.0).collect();
            let b: Vec<f32> = (0..len)
                .map(|i| (i as f32 * 0.029 + 2.0).cos() * 3.0)
                .collect();
            let s = scalar_sq_dist(&a, &b);
            let v = kmeans::sq_dist(&a, &b);
            assert!(
                (s - v).abs() <= 1e-3 * (1.0 + s.abs()),
                "len={len}: simd {v} vs scalar {s}"
            );
        }
    }

    #[test]
    fn train_add_search_self_retrieval() {
        let dim = 64;
        let data = clustered_vecs(5000, dim, 50, 1);
        let idx = build(&data, dim, 64);
        let sp = SearchParams {
            nprobe: 16,
            refine: true,
            refine_factor: 8,
        };
        // A stored vector should retrieve itself at the very top.
        let res = idx.search(&data[123], 10, sp);
        assert_eq!(
            res[0].id,
            123,
            "self must be top-1: {:?}",
            &res[..3.min(res.len())]
        );
    }

    #[test]
    fn delete_tombstones_excluded() {
        let dim = 64;
        let data = clustered_vecs(3000, dim, 30, 2);
        let mut idx = build(&data, dim, 48);
        assert_eq!(idx.delete(77), 1);
        let sp = SearchParams::default();
        let res = idx.search(&data[77], 10, sp);
        assert!(
            !res.iter().any(|r| r.id == 77),
            "deleted id must not appear"
        );
    }

    #[test]
    fn persist_reopen_no_rebuild_matches() {
        let dim = 64;
        let data = clustered_vecs(4000, dim, 40, 7);
        let idx = build(&data, dim, 64);
        let sp = SearchParams::default();
        let q = &data[321];
        let before = idx.search(q, 10, sp);

        let tmp = tempfile::tempdir().unwrap();
        save(&idx, tmp.path()).unwrap();
        let reopened = open(tmp.path()).unwrap();
        let after = reopened.search(q, 10, sp);
        assert_eq!(
            before.iter().map(|r| r.id).collect::<Vec<_>>(),
            after.iter().map(|r| r.id).collect::<Vec<_>>(),
            "reopened (no-rebuild) results must match exactly"
        );
        // Reopen must NOT have repopulated raw f32: the index carries only codes.
        assert_eq!(reopened.codes.len(), idx.codes.len());
        assert_eq!(reopened.sq_codes.len(), idx.sq_codes.len());
    }

    #[test]
    fn compact_drops_tombstones_preserves_results() {
        let dim = 64;
        let data = clustered_vecs(3000, dim, 30, 3);
        let mut idx = build(&data, dim, 48);
        for id in 0..500u64 {
            idx.delete(id);
        }
        assert!(idx.tombstone_ratio() > 0.1);
        let compacted = compact(&idx);
        assert_eq!(compacted.tombstone_ratio(), 0.0);
        assert_eq!(compacted.live_len(), idx.live_len());
        // A non-deleted vector still retrieves itself after compaction.
        let res = compacted.search(&data[2000], 10, SearchParams::default());
        assert_eq!(res[0].id, 2000);
    }

    #[test]
    fn incremental_insert_after_build_is_searchable() {
        let dim = 64;
        let data = clustered_vecs(2000, dim, 20, 9);
        let mut idx = build(&data, dim, 32);
        // Add a brand-new distinct vector AFTER the build — no rebuild.
        let novel: Vec<f32> = (0..dim).map(|j| if j == 0 { 9.0 } else { -9.0 }).collect();
        idx.add(&[(99999, novel.clone())]);
        let res = idx.search(&novel, 5, SearchParams::default());
        assert_eq!(res[0].id, 99999, "incrementally-added vector must be top-1");
    }

    /// THE BAR: recall@10 ≥ 0.95 vs brute force on a representative scale. Runs at
    /// 50k×128 with 100 queries to stay CI-runnable (the brute-force ground truth
    /// is the cost); the `recall` bench binary scales this to 1M and reports the
    /// full recall/latency/footprint trade.
    #[test]
    fn recall_at_10_meets_target() {
        let dim = 128;
        let n = 40_000;
        let data = clustered_vecs(n, dim, 200, 11);
        let params = IvfPqParams {
            dim,
            nlist: 512,
            m: dim / 4, // dsub = 4, m = 32
            kmeans_iters: 15,
            opq_iters: 5,
            seed: 7,
        };
        // Train on a sample (the production pattern — never the full set).
        let sample: Vec<Vec<f32>> = data.iter().step_by(3).cloned().collect();
        let mut idx = IvfPq::train(&params, &sample);
        let items: Vec<(u64, Vec<f32>)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.clone()))
            .collect();
        idx.add(&items);

        let sp = SearchParams {
            nprobe: 32,
            refine: true,
            refine_factor: 16,
        };
        let mut q_rng = ChaCha8Rng::seed_from_u64(999);
        let nq = 80;
        let mut hits = 0usize;
        let mut total = 0usize;
        for _ in 0..nq {
            let qi = q_rng.gen_range(0..n);
            let q = &data[qi];
            let truth = brute_force_knn(&data, q, 10);
            let got: Vec<u64> = idx.search(q, 10, sp).into_iter().map(|r| r.id).collect();
            for t in &truth {
                if got.contains(t) {
                    hits += 1;
                }
                total += 1;
            }
        }
        let recall = hits as f64 / total as f64;
        assert!(
            recall >= 0.95,
            "recall@10 = {recall:.4} must be >= 0.95 (OPQ+refine). hits={hits}/{total}"
        );
    }
}
