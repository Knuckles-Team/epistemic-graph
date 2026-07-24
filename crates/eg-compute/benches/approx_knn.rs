//! Approximate `gds.knn` (NN-descent) quality + speed bench
//! (CONCEPT:EG-KG.compute.node-similarity, W4.3/E11).
//!
//! Not a criterion bench — a plain `main` that builds a clustered similarity graph
//! and compares the seeded NN-descent (`knn_similarity_approx`) against the exact
//! `O(V²·d̄)` sweep (`knn_similarity`), asserting the acceptance bars and logging the
//! numbers:
//!
//!   * approximate quality WITHIN 5% of exact — pair-set recall ≥ 0.95;
//!   * approximate speed ≥ 10× the exact sweep at scale.
//!
//! Run:
//!   cargo bench -p eg-compute --bench approx_knn
//!   cargo bench -p eg-compute --bench approx_knn -- 20000 12 10 0.5

use eg_compute::graph_algos::{
    knn_similarity, knn_similarity_approx, AdjacencyGraph, Direction, Metric, SimilarityPair,
};
use std::collections::HashSet;
use std::time::Instant;

/// A ring similarity graph with a smooth gradient (the canonical NN-descent
/// setting): node `i` (of `n`) points at the `window` successor nodes
/// `n_{(i+j) mod n}` (`j = 1..=window`), so Jaccard similarity between two nodes
/// decays cleanly with their ring distance — `sim(i, i±d) = (window−d)/(window+d)`
/// for `0 < d < window`, else 0. Each node's exact top-`k` is therefore its `k`
/// nearest ring positions with DISTINCT, well-ordered scores (no massive score ties
/// to make the top-`k` — and thus the recall measurement — ambiguous, which a
/// clustered block graph would suffer). Every node is a real participant (no inert
/// target-only nodes), so `V = n`.
fn ring_graph(n: usize, window: usize) -> AdjacencyGraph<String> {
    let w = window.min(n.saturating_sub(1)).max(1);
    let mut edges: Vec<(String, String, f64)> = Vec::with_capacity(n * w);
    for i in 0..n {
        let src = format!("n{i}");
        for j in 1..=w {
            edges.push((src.clone(), format!("n{}", (i + j) % n), 1.0));
        }
    }
    AdjacencyGraph::from_edges(edges)
}

fn pair_set(pairs: &[SimilarityPair<String>]) -> HashSet<(String, String)> {
    pairs
        .iter()
        .map(|p| {
            if p.a <= p.b {
                (p.a.clone(), p.b.clone())
            } else {
                (p.b.clone(), p.a.clone())
            }
        })
        .collect()
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let n_sources: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let window: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(12);
    let k: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
    let sample_rate: f64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(0.5);

    let g = ring_graph(n_sources, window);

    println!(
        "eg-compute approx-knn (NN-descent) bench: sources={n_sources} \
         (ring, window={window}) k={k} sampleRate={sample_rate}"
    );

    // Exact sweep.
    let t = Instant::now();
    let exact = knn_similarity(&g, Metric::Jaccard, Direction::Out, k, 0.0);
    let exact_ms = t.elapsed().as_secs_f64() * 1000.0;

    // Approximate NN-descent.
    let t = Instant::now();
    let approx = knn_similarity_approx(
        &g,
        Metric::Jaccard,
        Direction::Out,
        k,
        0.0,
        sample_rate,
        100,
        0.001,
        42,
    );
    let approx_ms = t.elapsed().as_secs_f64() * 1000.0;

    let (se, sa) = (pair_set(&exact), pair_set(&approx));
    let recovered = se.intersection(&sa).count();
    let recall = recovered as f64 / se.len().max(1) as f64;
    let speedup = exact_ms / approx_ms;

    println!(
        "\n{:>14} | {:>12} | {:>10}",
        "variant", "time (ms)", "pairs"
    );
    println!("{}", "-".repeat(42));
    println!(
        "{:>14} | {:>12.2} | {:>10}",
        "exact O(V^2)",
        exact_ms,
        se.len()
    );
    println!(
        "{:>14} | {:>12.2} | {:>10}",
        "nn-descent",
        approx_ms,
        sa.len()
    );

    println!("\n--- acceptance ---");
    let quality_ok = recall >= 0.95;
    let speed_ok = speedup >= 10.0;
    println!(
        "  quality: pair-recall = {recall:.4} (recovered {recovered}/{})  [{}]",
        se.len(),
        if quality_ok {
            "PASS within 5%"
        } else {
            "FAIL > 5% loss"
        }
    );
    println!(
        "  speed  : {speedup:.1}x exact ({exact_ms:.1} ms -> {approx_ms:.1} ms)  [{}]",
        if speed_ok {
            "PASS >= 10x"
        } else {
            "FAIL < 10x"
        }
    );
    let ok = quality_ok && speed_ok;
    println!("\nRESULT: {}", if ok { "PASS" } else { "FAIL" });
    assert!(ok, "approx-knn acceptance failed (see table above)");
}
