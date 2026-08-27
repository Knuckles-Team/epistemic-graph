//! Characterization tests for `knn_similarity` (CX-EG-02,
//! `crates/eg-compute/src/graph_algos/similarity.rs`).
//!
//! Pins observed behaviour of the public per-node top-k similarity sweep
//! ahead of a pure extract-method refactor (CCN 11 -> target <=10): the
//! Cosine metric branch (existing in-file unit tests only cover Jaccard),
//! the `cutoff` filter, the `select_nth_unstable_by` truncation path taken
//! when a node has more than `top_k` candidates scoring above cutoff, and
//! the final descending-score / ascending-id sort order.

use eg_compute::graph_algos::{knn_similarity, AdjacencyGraph, Direction, Metric};

#[test]
fn cosine_metric_ranks_by_cosine_score_not_jaccard() {
    // a -> x:1,y:2 ; b -> x:2,y:4 (proportional weights: cosine == 1.0,
    // jaccard on the *set* {x,y}∩{x,y}/{x,y}∪{x,y} would ALSO be 1.0, so
    // pick a third node c whose weights make cosine and jaccard disagree
    // in ranking to prove the Cosine arm of the `match metric` is what's
    // driving the result, not an accidental jaccard fallback).
    // a -> x:1,y:1 (unit); b -> x:1,y:1 (unit, identical) => cosine 1.0
    // c -> x:1,y:0.001 (almost only x) => cosine(a,c) is close to but
    // below cosine(a,b), while jaccard(a,c) == jaccard(a,b) == 1.0 (same
    // *set* of neighbours x,y for all three). Only Cosine's weight-aware
    // formula can separate b (best) from c (second) for node a.
    let g = AdjacencyGraph::from_edges([
        ("a", "x", 1.0),
        ("a", "y", 1.0),
        ("b", "x", 1.0),
        ("b", "y", 1.0),
        ("c", "x", 1.0),
        ("c", "y", 0.001),
    ]);
    let pairs = knn_similarity(&g, Metric::Cosine, Direction::Out, 1, 0.0);
    let ab = pairs
        .iter()
        .find(|p| (p.a == "a" && p.b == "b") || (p.a == "b" && p.b == "a"));
    assert!(
        ab.is_some(),
        "a's top-1 under Cosine must be b (identical weighted vector), got {pairs:?}"
    );
    assert!((ab.unwrap().score - 1.0).abs() < 1e-9);
    // OBSERVED (known-bad-checked below): c's own top-1 resolves to "a", not
    // "b" -- cosine(c,a) and cosine(c,b) are numerically tied (~0.70781...,
    // since a and b have identical weighted vectors), and the ascending-id
    // tiebreak in `neighbor_score_cmp` picks the lower-index node, which is
    // "a" (nodes are assigned dense indices in sorted-N order: a=0,b=1,c=2).
    // So the pair (a, c) DOES appear -- but (b, c) must NOT, since b's own
    // top-1 is a (score 1.0), strictly beating c (score ~0.708). This is
    // exactly what proves the Cosine arm ran: under Jaccard, a/b/c all tie
    // at 1.0 (same neighbour *set* {x,y} for all three) and the exclusion of
    // (b,c) would not hold.
    let ac = pairs
        .iter()
        .find(|p| (p.a == "a" && p.b == "c") || (p.a == "c" && p.b == "a"));
    assert!(ac.is_some(), "expected (a,c) pair, got {pairs:?}");
    assert!(
        (ac.unwrap().score - 0.7078135340610553).abs() < 1e-9,
        "got {:?}",
        ac.unwrap().score
    );
    assert!(
        !pairs
            .iter()
            .any(|p| (p.a == "b" && p.b == "c") || (p.a == "c" && p.b == "b")),
        "b's top-1 is a (score 1.0), strictly beating c (~0.708) under Cosine; \
         (b,c) appearing would mean this ran as Jaccard instead. got {pairs:?}"
    );
}

#[test]
fn cutoff_excludes_scores_at_or_below_it() {
    // a -> {x} (1 neighbour), b -> {x,y} (2 neighbours): intersection {x}=1,
    // union {x,y}=2 => jaccard == 0.5 exactly. With cutoff exactly at 0.5
    // the pair must be EXCLUDED (`s > cutoff`, strict).
    let g = AdjacencyGraph::from_edges([("a", "x", 1.0), ("b", "x", 1.0), ("b", "y", 1.0)]);
    let at_cutoff = knn_similarity(&g, Metric::Jaccard, Direction::Out, 5, 0.5);
    assert!(
        at_cutoff.is_empty(),
        "score exactly at cutoff must be excluded (strict >), got {at_cutoff:?}"
    );
    let below_cutoff = knn_similarity(&g, Metric::Jaccard, Direction::Out, 5, 0.4);
    assert_eq!(below_cutoff.len(), 1, "got {below_cutoff:?}");
    assert!((below_cutoff[0].score - 0.5).abs() < 1e-9);
}

#[test]
fn truncates_to_top_k_when_more_candidates_than_k_score_above_cutoff() {
    // Hub node "h" points at 4 leaves with distinct weights so each leaf's
    // jaccard-with-h is distinct (share exactly {h's target itself has no
    // effect}; use per-leaf extra fan-out to vary the overlap). Build so
    // leaves l1..l4 have descending similarity to h, then ask for top_k=2
    // and confirm only the 2 highest survive -- this exercises the
    // `scored.len() > k` -> `select_nth_unstable_by` + truncate branch.
    let g = AdjacencyGraph::from_edges([
        ("h", "s", 1.0),
        ("h", "t", 1.0),
        ("h", "u", 1.0),
        ("h", "v", 1.0),
        // l1 shares 3/4 of h's targets (jaccard 3/4)
        ("l1", "s", 1.0),
        ("l1", "t", 1.0),
        ("l1", "u", 1.0),
        // l2 shares 2/4 (jaccard 2/5: {s,t} ∩ / {s,t,u,v,w} ∪)
        ("l2", "s", 1.0),
        ("l2", "t", 1.0),
        ("l2", "w", 1.0),
        // l3 shares 1/4
        ("l3", "s", 1.0),
        ("l3", "p", 1.0),
        ("l3", "q", 1.0),
        // l4 shares 1/4, different disjoint extras (lower jaccard than l3)
        ("l4", "s", 1.0),
        ("l4", "m", 1.0),
        ("l4", "n", 1.0),
        ("l4", "o", 1.0),
    ]);
    let pairs = knn_similarity(&g, Metric::Jaccard, Direction::Out, 2, 0.0);
    let h_pairs: Vec<&str> = pairs
        .iter()
        .filter(|p| p.a == "h" || p.b == "h")
        .map(|p| if p.a == "h" { p.b } else { p.a })
        .collect();
    assert_eq!(
        h_pairs.len(),
        2,
        "h must keep exactly top_k=2 neighbours, got {h_pairs:?} from {pairs:?}"
    );
    assert!(h_pairs.contains(&"l1"), "{h_pairs:?}");
    assert!(h_pairs.contains(&"l2"), "{h_pairs:?}");
}

#[test]
fn results_sorted_descending_score_then_ascending_ids() {
    let g = AdjacencyGraph::from_edges([
        ("a", "x", 1.0),
        ("a", "y", 1.0),
        ("b", "x", 1.0),
        ("b", "y", 1.0),
        ("c", "p", 1.0),
        ("c", "q", 1.0),
        ("d", "p", 1.0),
        ("d", "q", 1.0),
    ]);
    let pairs = knn_similarity(&g, Metric::Jaccard, Direction::Out, 5, 0.0);
    // (a,b) and (c,d) both score 1.0; ascending-id tiebreak means (a,b)
    // (lexicographically smaller pair) must come first.
    assert_eq!(pairs.len(), 2);
    assert_eq!((pairs[0].a, pairs[0].b), ("a", "b"));
    assert_eq!((pairs[1].a, pairs[1].b), ("c", "d"));
    for w in pairs.windows(2) {
        assert!(w[0].score >= w[1].score);
    }
}

#[test]
fn empty_graph_yields_empty_result() {
    let g: AdjacencyGraph<String> =
        AdjacencyGraph::from_adjacency(Vec::<(String, Vec<(String, f64)>)>::new());
    assert!(knn_similarity(&g, Metric::Jaccard, Direction::Out, 5, 0.0).is_empty());
    assert!(knn_similarity(&g, Metric::Cosine, Direction::Out, 5, 0.0).is_empty());
}
