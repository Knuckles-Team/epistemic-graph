//! # eg-text — embedded BM25 full-text search over node text fields (CONCEPT:KG-2.215)
//!
//! An inverted-index full-text search the engine holds beside the graph: index a
//! `(node_id, text)` pair, query with a natural-language string, get the BM25 top-k
//! `Vec<TextHit>` — `(id, score)`, exactly the shape the vector `Rank` produces, so
//! it drops into the [`eg_plan`] RowSet algebra as a lexical `Rank` (a sibling of the
//! vector kNN).
//!
//! ## Tantivy vs a hand-rolled BM25 (the decision)
//!
//! This increment uses **Tantivy** (the Rust Lucene), HARD feature-gated behind the
//! crate's `tantivy` feature. Tantivy gives — for free, battle-tested — exactly the
//! four things the spec demands and a hand-rolled BM25 would each have to reinvent:
//!
//!  * **Persistent, no-rebuild-on-load.** Tantivy writes immutable mmap'd segments to
//!    a directory; reopening an index is `Index::open(dir)` — it does NOT re-tokenize
//!    or rebuild postings from raw text (mirrors the eg-ann KG-2.207 "reopen without
//!    rebuild" contract, which a naive in-RAM BM25 map cannot do without persisting +
//!    replaying every document).
//!  * **Incremental add/delete.** `IndexWriter::add_document` / `delete_term` +
//!    `commit` mutate the index in place (new segments + a tombstone), so re-indexing
//!    one changed `:Doc` is O(1 doc), not a full rebuild.
//!  * **BM25 scoring + tokenization/stemming.** Tantivy's `BM25` weighting and its
//!    default `en_stem` tokenizer (lower-case → stop-ish → Porter stem) are what a
//!    correct BM25 *needs*; hand-rolling tokenization/stemming is the bug-prone part.
//!
//! The cost is footprint: Tantivy pulls ≈140 transitive crates including `zstd-sys`
//! (a `cc`-compiled C library). That is why the gate is **HARD**: the `tantivy`
//! feature is folded by the facade into `text`/`node`/`cluster`/`full` only — NEVER
//! `pi`/`default`. A default/Pi build links no Tantivy and no zstd-sys (the Pi
//! contract, asserted by `cargo tree`). If that footprint were unacceptable even
//! gated, the fallback is a hand-rolled BM25 (an in-RAM `HashMap<term, postings>` +
//! the Robertson/Spärck-Jones BM25 formula + a serde snapshot for persistence) — see
//! the crate report; we chose Tantivy because the gate fully isolates the weight and
//! the no-rebuild/incremental/stemming machinery is non-trivial to get right.
//!
//! ## Public surface
//!
//! [`TextHit`] is the dep-free result row (always available — the planner names it in
//! a default build even though the *index* needs `tantivy`). [`TextIndex`] is the
//! Tantivy-backed index, behind `#[cfg(feature = "tantivy")]`.

use serde::{Deserialize, Serialize};

/// One BM25 hit: a node id and its BM25 relevance score (higher = more relevant).
/// The SAME `(id, score)` shape the vector `Rank` produces, so the lexical rank op
/// fuses with the vector rank op over the shared RowSet currency.
///
/// Dep-free and ALWAYS compiled (no feature gate) so [`eg_plan`]'s `RankText` op can
/// name the result type in a default build; only building/querying an actual index
/// needs the `tantivy` feature.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextHit {
    pub id: String,
    pub score: f32,
}

#[cfg(feature = "tantivy")]
mod index;
#[cfg(feature = "tantivy")]
pub use index::TextIndex;

/// Reciprocal-Rank Fusion (RRF) of several ranked id lists into one — the modern
/// hybrid-retrieval primitive, dep-free so it ships in EVERY build (a default build
/// can fuse two ranked lists even though it cannot itself build a text index).
///
/// RRF score of an id = Σ over each input list `1 / (k + rank)` where `rank` is the
/// id's 0-based position in that list (only lists that contain the id contribute).
/// The constant `k` (Cormack et al. 2009 use 60) damps the top-rank dominance so a
/// document strong in BOTH lists out-ranks one that merely tops a single list — which
/// is exactly why a fused query beats either alone.
///
/// Input: each `&[String]` is one ranked list, most-relevant FIRST (e.g. the BM25
/// top-k ids and the vector top-k ids). Output: ids sorted by descending fused score
/// (ties broken by id for determinism), paired with that score.
///
/// Rank-based (not score-based) fusion is deliberate: BM25 scores and cosine
/// similarities live on incomparable scales, so fusing their RANKS sidesteps any
/// score-normalization choice — the property that makes RRF robust across modalities.
pub fn rrf_fuse(lists: &[&[String]], k: f32) -> Vec<(String, f32)> {
    use std::collections::HashMap;
    let mut acc: HashMap<&str, f32> = HashMap::new();
    for list in lists {
        for (rank, id) in list.iter().enumerate() {
            *acc.entry(id.as_str()).or_insert(0.0) += 1.0 / (k + rank as f32);
        }
    }
    let mut out: Vec<(String, f32)> = acc.into_iter().map(|(id, s)| (id.to_string(), s)).collect();
    // Descending fused score; deterministic id tie-break.
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    out
}

/// The conventional RRF damping constant (Cormack, Clarke & Buettcher, SIGIR 2009).
pub const RRF_K: f32 = 60.0;

#[cfg(test)]
mod rrf_tests {
    use super::*;

    /// RRF rewards agreement: an id present in BOTH lists beats an id that merely tops
    /// ONE list — the property that makes a fused query beat either alone. "c" is #2 in
    /// each list (score 1/61+1/61 ≈ 0.0328), out-scoring "a" (#1 in lexical only, 1/60 ≈
    /// 0.0167) and "b" (#1 in vector only, 1/60).
    #[test]
    fn rrf_rewards_cross_list_agreement() {
        let lexical = vec!["a".to_string(), "c".to_string()];
        let vector = vec!["b".to_string(), "c".to_string()];
        let fused = rrf_fuse(&[&lexical, &vector], RRF_K);
        assert_eq!(
            fused[0].0, "c",
            "the cross-list agreement winner ranks first"
        );
        // And it strictly beats both single-list toppers.
        assert!(fused[0].1 > fused[1].1, "agreement score strictly highest");
    }

    /// An id present in only one list still scores; a single-list fuse is identity
    /// order (rank-monotone).
    #[test]
    fn rrf_single_list_is_rank_order() {
        let only = vec!["x".to_string(), "y".to_string(), "z".to_string()];
        let fused = rrf_fuse(&[&only], RRF_K);
        let ids: Vec<&str> = fused.iter().map(|(i, _)| i.as_str()).collect();
        assert_eq!(ids, vec!["x", "y", "z"]);
    }
}
