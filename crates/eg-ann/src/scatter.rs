//! Cross-shard scatter-gather kNN (CONCEPT:EG-319).
//!
//! `eg-ann` is per-graph / per-shard by construction — one [`crate::IvfPq`] (or
//! [`crate::FlatIndex`] / [`crate::HnswIndex`]) per `SemanticStore`. [`merge_topk`]
//! (CONCEPT:EG-069) is the GATHER leaf: it reduces per-shard local top-k lists into a
//! global top-k. What was missing is the SCATTER: fan one query vector to EVERY
//! shard's index, over-fetch per shard, collect the per-shard [`SearchResult`] lists,
//! and merge them. This module wires that end to end.
//!
//! ## Correctness
//!
//! For an EXACT global top-k it is not enough for each shard to return its top-1: the
//! global k-th nearest neighbour could be a shard's own k-th, so each shard must
//! over-fetch at least `k` (all `k` could live on one shard). [`scatter_knn`] fetches
//! `k * over_fetch` (min `k`) per shard, then reduces with [`merge_topk_stable`] so the
//! result is byte-for-byte what a single index over the UNION of all shards would
//! return, ties broken by id (deterministic regardless of shard arrival order).
//!
//! ## Fault tolerance
//!
//! A shard that is missing, unreachable, or too slow returns `None` from
//! [`ShardIndex::shard_search`] (the caller wraps its RPC/timeout there); scatter skips
//! it, records its index in [`ScatterKnn::shards_skipped`], and still returns the best
//! global top-k over the shards that DID answer — a degraded-but-useful result rather
//! than a hard failure.
//!
//! ## Router hook (deferred — CONCEPT:EG-319 follow-up)
//!
//! In cluster mode a graph's vectors would be sharded across multi-Raft groups
//! (`src/raft/multi.rs`, `GroupRouter`). The wiring point is the cluster vector-search
//! path in `src/server/handlers/query.rs`: when `GroupRouter::group_ring()` resolves a
//! semantic query to more than one group, build one [`ShardIndex`] per group (each an
//! adapter that issues the per-group `SemanticStore` search — remote via
//! `network::PeerPool`, local for the owning group), then call [`scatter_knn`] and hand
//! the merged top-k back to the ranker. Today `GroupRouter` maps every graph to ONE
//! group (CONCEPT:KG-2.207), so a single graph's vectors are never split across groups
//! and the scatter degenerates to a one-shard gather — the core here is ready for the
//! moment per-graph vector resharding lands. Kept out of the router for now to avoid
//! touching the cross-shard txn / pgwire paths; the seam is [`ShardIndex`].

use crate::ivfpq::{merge_topk_stable, SearchParams, SearchResult};

/// One shard's ANN index, as seen by the cross-shard scatter (CONCEPT:EG-319).
///
/// The scatter never assumes an index type — a shard is anything that can answer
/// "your `k` nearest to this query", or say it can't. Implemented in-crate for
/// [`crate::IvfPq`] and [`crate::FlatIndex`]; a cluster router would implement it over
/// a remote per-group `SemanticStore` handle (the RPC + timeout live inside
/// [`Self::shard_search`], which returns `None` on failure so the scatter can skip it).
pub trait ShardIndex {
    /// Return up to `k` nearest hits to `query` from THIS shard, sorted nearest-first,
    /// or `None` if the shard is unavailable/slow (so scatter skips + notes it).
    fn shard_search(&self, query: &[f32], k: usize) -> Option<Vec<SearchResult>>;
}

/// Outcome of a [`scatter_knn`] fan-out (CONCEPT:EG-319).
#[derive(Clone, Debug)]
pub struct ScatterKnn {
    /// The global top-k over every shard that answered, nearest-first, id-tiebroken.
    pub results: Vec<SearchResult>,
    /// How many shards the query was scattered to.
    pub shards_total: usize,
    /// How many shards returned a (possibly empty) result list.
    pub shards_answered: usize,
    /// Indexes of shards that were skipped (returned `None` — missing/slow).
    pub shards_skipped: Vec<usize>,
}

/// Scatter a kNN query to every shard, gather, and merge to a global top-k
/// (CONCEPT:EG-319).
///
/// Each shard over-fetches `(k * over_fetch).max(k)` (an `over_fetch` of 1 fetches
/// exactly `k` per shard — the minimum for an exact merge; a larger factor buys ANN
/// recall margin before the merge). Shards are queried in parallel; a shard returning
/// `None` is skipped and noted in [`ScatterKnn::shards_skipped`]. The gather uses
/// [`merge_topk_stable`], so the returned order is deterministic (distance, then id)
/// and equals a single index over the union of all answering shards.
pub fn scatter_knn<S>(shards: &[S], query: &[f32], k: usize, over_fetch: usize) -> ScatterKnn
where
    S: ShardIndex + Sync,
{
    use rayon::prelude::*;

    let per_shard = k.saturating_mul(over_fetch.max(1)).max(k);

    // Fan out in parallel, preserving shard index alongside each answer so skips are
    // reported deterministically regardless of completion order.
    let answers: Vec<(usize, Option<Vec<SearchResult>>)> = shards
        .par_iter()
        .enumerate()
        .map(|(i, s)| (i, s.shard_search(query, per_shard)))
        .collect();

    let mut lists: Vec<Vec<SearchResult>> = Vec::with_capacity(answers.len());
    let mut shards_skipped: Vec<usize> = Vec::new();
    let mut shards_answered = 0usize;
    for (i, ans) in answers {
        match ans {
            Some(list) => {
                shards_answered += 1;
                lists.push(list);
            }
            None => shards_skipped.push(i),
        }
    }
    shards_skipped.sort_unstable();

    ScatterKnn {
        results: merge_topk_stable(&lists, k),
        shards_total: shards.len(),
        shards_answered,
        shards_skipped,
    }
}

/// [`IvfPq`] as a scatter shard — always available, uses default search tuning.
///
/// [`IvfPq`]: crate::IvfPq
impl ShardIndex for crate::IvfPq {
    fn shard_search(&self, query: &[f32], k: usize) -> Option<Vec<SearchResult>> {
        Some(self.search(query, k, SearchParams::default()))
    }
}

/// [`FlatIndex`] as a scatter shard — exact per-shard top-k under L2 (the ground-truth
/// leaf), always available.
///
/// [`FlatIndex`]: crate::FlatIndex
impl ShardIndex for crate::FlatIndex {
    fn shard_search(&self, query: &[f32], k: usize) -> Option<Vec<SearchResult>> {
        Some(self.search(query, k, crate::Metric::L2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlatIndex, Metric};

    /// A mock shard: an exact FlatIndex over an assigned slice of the corpus, with a
    /// switch to simulate being missing/slow (returns `None`).
    struct MockShard {
        index: FlatIndex,
        available: bool,
    }

    impl ShardIndex for MockShard {
        fn shard_search(&self, query: &[f32], k: usize) -> Option<Vec<SearchResult>> {
            if !self.available {
                return None;
            }
            Some(self.index.search(query, k, Metric::L2))
        }
    }

    fn corpus(n: usize, dim: usize, seed: u64) -> Vec<(u64, Vec<f32>)> {
        // Deterministic pseudo-random vectors; ids are the global row index.
        let mut state = seed.wrapping_add(0x9E3779B97F4A7C15);
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f32 / (1u64 << 53) as f32
        };
        (0..n)
            .map(|i| (i as u64, (0..dim).map(|_| next() * 2.0 - 1.0).collect()))
            .collect()
    }

    /// Round-robin the corpus across `n_shards` FlatIndexes (ids preserved globally).
    fn shard_corpus(data: &[(u64, Vec<f32>)], dim: usize, n_shards: usize) -> Vec<MockShard> {
        let mut buckets: Vec<Vec<(u64, Vec<f32>)>> = vec![Vec::new(); n_shards];
        for (row, item) in data.iter().enumerate() {
            buckets[row % n_shards].push(item.clone());
        }
        buckets
            .into_iter()
            .map(|items| {
                let mut idx = FlatIndex::new(dim);
                idx.add(&items);
                MockShard {
                    index: idx,
                    available: true,
                }
            })
            .collect()
    }

    fn single_index(data: &[(u64, Vec<f32>)], dim: usize) -> FlatIndex {
        let mut idx = FlatIndex::new(dim);
        idx.add(data);
        idx
    }

    #[test]
    fn eg319_scatter_over_n_shards_equals_single_index_over_union() {
        // CONCEPT:EG-319 — scattering a kNN query to N shards and merging equals the
        // top-k a single index over the UNION would return (ids AND nearest-first).
        let dim = 24;
        let data = corpus(600, dim, 1);
        let reference = single_index(&data, dim);
        let query = &data[137].1.clone();

        for &n_shards in &[1usize, 2, 3, 5, 8] {
            let shards = shard_corpus(&data, dim, n_shards);
            let k = 10;
            let out = scatter_knn(&shards, query, k, 1);
            let truth = reference.search(query, k, Metric::L2);

            assert_eq!(out.shards_total, n_shards);
            assert_eq!(out.shards_answered, n_shards);
            assert!(out.shards_skipped.is_empty());
            assert_eq!(
                out.results.iter().map(|r| r.id).collect::<Vec<_>>(),
                truth.iter().map(|r| r.id).collect::<Vec<_>>(),
                "EG-319 scatter over {n_shards} shards must equal single-index top-k"
            );
            for (g, t) in out.results.iter().zip(truth.iter()) {
                assert_eq!(g.distance, t.distance, "distances must carry through");
            }
        }
    }

    #[test]
    fn eg319_missing_shard_degrades_gracefully() {
        // CONCEPT:EG-319 — a missing/slow shard is skipped + noted, and the result is
        // the exact top-k over the shards that DID answer (not a hard failure).
        let dim = 16;
        let data = corpus(400, dim, 2);
        let query = &data[3].1.clone();
        let k = 8;

        let mut shards = shard_corpus(&data, dim, 4);
        shards[1].available = false; // shard 1 is down/slow

        let out = scatter_knn(&shards, query, k, 1);
        assert_eq!(out.shards_total, 4);
        assert_eq!(out.shards_answered, 3);
        assert_eq!(out.shards_skipped, vec![1]);

        // Reference: a single index over ONLY the surviving shards' rows (rows whose
        // global index is not congruent to 1 mod 4).
        let survivors: Vec<(u64, Vec<f32>)> = data
            .iter()
            .enumerate()
            .filter(|(row, _)| row % 4 != 1)
            .map(|(_, item)| item.clone())
            .collect();
        let reference = single_index(&survivors, dim);
        let truth = reference.search(query, k, Metric::L2);

        assert_eq!(
            out.results.iter().map(|r| r.id).collect::<Vec<_>>(),
            truth.iter().map(|r| r.id).collect::<Vec<_>>(),
            "EG-319 degraded scatter must equal top-k over the surviving shards"
        );
        // No skipped shard's id (row % 4 == 1) leaks into the result.
        assert!(
            out.results.iter().all(|r| r.id % 4 != 1),
            "a skipped shard's ids must not appear"
        );
    }

    #[test]
    fn eg319_k_greater_than_total_returns_all_sorted() {
        // CONCEPT:EG-319 — asking for more neighbours than exist across all shards
        // returns every point, still globally sorted (distance, id) and de-duplicated
        // by construction (ids are shard-disjoint).
        let dim = 8;
        let data = corpus(20, dim, 3);
        let query = &data[0].1.clone();
        let shards = shard_corpus(&data, dim, 4);

        let out = scatter_knn(&shards, query, 1000, 1);
        assert_eq!(
            out.results.len(),
            data.len(),
            "k>total must return all points"
        );
        assert_eq!(out.shards_answered, 4);

        // Globally sorted nearest-first with id tiebreak.
        for w in out.results.windows(2) {
            let ok = w[0].distance < w[1].distance
                || (w[0].distance == w[1].distance && w[0].id <= w[1].id);
            assert!(ok, "result must be sorted by (distance, id)");
        }
        let reference = single_index(&data, dim);
        let truth = reference.search(query, 1000, Metric::L2);
        assert_eq!(
            out.results.iter().map(|r| r.id).collect::<Vec<_>>(),
            truth.iter().map(|r| r.id).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn eg319_scatter_is_deterministic_across_repeats() {
        // CONCEPT:EG-319 — repeated scatters (and a reversed shard order) yield the
        // IDENTICAL global top-k: the merge's (distance, id) total order removes any
        // dependence on shard arrival / ordering.
        let dim = 12;
        let data = corpus(500, dim, 4);
        let query = &data[200].1.clone();
        let k = 16;

        let shards = shard_corpus(&data, dim, 6);
        let first = scatter_knn(&shards, query, k, 1).results;
        for _ in 0..5 {
            let again = scatter_knn(&shards, query, k, 1).results;
            assert_eq!(first, again, "scatter must be deterministic across repeats");
        }

        // Reversing shard order must not change the merged result.
        let mut reversed = shard_corpus(&data, dim, 6);
        reversed.reverse();
        let rev = scatter_knn(&reversed, query, k, 1).results;
        assert_eq!(
            first, rev,
            "EG-319 merge must be independent of shard ordering"
        );
    }

    #[test]
    fn eg319_over_fetch_factor_preserves_exact_topk() {
        // CONCEPT:EG-319 — a larger per-shard over-fetch never changes the exact top-k
        // for exact (FlatIndex) shards; it only widens the ANN recall margin.
        let dim = 20;
        let data = corpus(700, dim, 9);
        let query = &data[42].1.clone();
        let k = 10;
        let shards = shard_corpus(&data, dim, 5);

        let of1 = scatter_knn(&shards, query, k, 1).results;
        let of4 = scatter_knn(&shards, query, k, 4).results;
        assert_eq!(
            of1.iter().map(|r| r.id).collect::<Vec<_>>(),
            of4.iter().map(|r| r.id).collect::<Vec<_>>(),
            "over-fetch must not change the exact top-k"
        );
    }
}
