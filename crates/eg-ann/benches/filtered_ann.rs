//! Filtered-ANN recall + sub-linear-scaling bench for the HNSW index
//! (CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter, W4.3/E10).
//!
//! Not a criterion bench — a plain `main` that builds an `HnswIndex` at several
//! scales, applies a ~1% metadata predicate, and compares the PUSHED-DOWN filtered
//! graph walk (`HnswIndex::search_filtered`) against a post-filter brute force. It
//! asserts the two acceptance bars and logs the numbers:
//!
//!   * filtered recall@10 ≥ 0.9 at 1% selectivity (vs exact filtered brute force);
//!   * filtered latency SUB-LINEAR in |V| — as |V| grows k× the HNSW query time grows
//!     far slower than k× (while brute-force post-filter grows ~linearly), so the
//!     filtered walk explores ~ef/selectivity nodes regardless of graph size.
//!
//! Run:
//!   cargo bench -p eg-ann --bench filtered_ann
//!   cargo bench -p eg-ann --bench filtered_ann -- 20000 40000 80000 32 128 100 100
//!   #                                              N-scales...    dim ef nq  sel⁻¹

use eg_ann::{HnswIndex, Metric};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::Instant;

fn random_vecs(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    (0..n)
        .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect())
        .collect()
}

/// POST-FILTER brute force — the baseline the pushed-down filter must beat on
/// SCALING: it scores EVERY vector (the predicate is applied AFTER the distance,
/// exactly what a system without predicate push-down pays), then keeps the allowed
/// top-`k`. `O(|V|)` distance work per query — linear in the graph size. Also the
/// exact ground truth for recall.
fn brute_filtered(data: &[Vec<f32>], q: &[f32], k: usize, allow: &dyn Fn(u64) -> bool) -> Vec<u64> {
    let mut scored: Vec<(f32, u64)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (Metric::L2.distance(q, v), i as u64))
        .filter(|(_, id)| allow(*id))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.truncate(k);
    scored.into_iter().map(|(_, id)| id).collect()
}

fn recall_at(got: &[u64], truth: &[u64]) -> f64 {
    if truth.is_empty() {
        return 1.0;
    }
    let hit = got.iter().filter(|g| truth.contains(g)).count();
    hit as f64 / truth.len() as f64
}

struct ScaleResult {
    n: usize,
    recall: f64,
    hnsw_ms: f64,
    brute_ms: f64,
}

fn main() {
    // Leading args that parse as usize >= 1000 are the |V| scales; everything after
    // the first non-scale arg is [dim, ef, nq, sel_inv]. No args ⇒ all defaults.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut scales: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].parse::<usize>() {
            Ok(v) if v >= 1000 => {
                scales.push(v);
                i += 1;
            }
            _ => break,
        }
    }
    if scales.is_empty() {
        scales = vec![20_000, 40_000, 80_000];
    }
    let params: Vec<usize> = args[i..].iter().filter_map(|s| s.parse().ok()).collect();
    let dim = params.first().copied().unwrap_or(32);
    let ef = params.get(1).copied().unwrap_or(128);
    let nq = params.get(2).copied().unwrap_or(100);
    let sel_inv = params.get(3).copied().unwrap_or(100);
    let k = 10;
    let selectivity = 1.0 / sel_inv as f64;

    println!(
        "eg-ann filtered-ANN bench: scales={scales:?} dim={dim} ef={ef} k={k} nq={nq} \
         selectivity={:.3}% (allow id %{sel_inv}==0)",
        selectivity * 100.0
    );
    println!(
        "\n{:>8} | {:>9} | {:>13} | {:>13} | {:>8}",
        "N", "recall@10", "hnsw ms/q", "brute ms/q", "speedup"
    );
    println!("{}", "-".repeat(66));

    let allow = move |id: u64| id % sel_inv as u64 == 0;
    let mut results: Vec<ScaleResult> = Vec::new();

    for &n in &scales {
        let data = random_vecs(n, dim, 33);
        let mut hnsw = HnswIndex::new(dim, Metric::L2, 16, 200, 7);
        for (i, v) in data.iter().enumerate() {
            hnsw.insert(i as u64, v.clone());
        }
        let mut qr = ChaCha8Rng::seed_from_u64(2025);
        let queries: Vec<Vec<f32>> = (0..nq)
            .map(|_| (0..dim).map(|_| qr.gen::<f32>() * 2.0 - 1.0).collect())
            .collect();

        // Filtered HNSW: recall vs exact filtered brute force + latency.
        let mut recall_sum = 0.0f64;
        let t = Instant::now();
        let mut got_all: Vec<Vec<u64>> = Vec::with_capacity(nq);
        for q in &queries {
            let got: Vec<u64> = hnsw
                .search_filtered(q, k, ef, Some(&allow))
                .into_iter()
                .map(|r| r.id)
                .collect();
            got_all.push(got);
        }
        let hnsw_ms = t.elapsed().as_secs_f64() * 1000.0 / nq as f64;

        // Brute-force post-filter baseline: latency + the ground truth for recall.
        let t = Instant::now();
        for (q, got) in queries.iter().zip(&got_all) {
            let truth = brute_filtered(&data, q, k, &allow);
            recall_sum += recall_at(got, &truth);
        }
        let brute_ms = t.elapsed().as_secs_f64() * 1000.0 / nq as f64;
        let recall = recall_sum / nq as f64;

        println!(
            "{n:>8} | {recall:>9.4} | {hnsw_ms:>13.3} | {brute_ms:>13.3} | {:>7.1}x",
            brute_ms / hnsw_ms
        );
        results.push(ScaleResult {
            n,
            recall,
            hnsw_ms,
            brute_ms,
        });
    }

    // ── acceptance checks (log + assert) ──────────────────────────────────────
    println!("\n--- acceptance ---");
    let mut ok = true;
    for r in &results {
        let pass = r.recall >= 0.9;
        ok &= pass;
        println!(
            "  N={:>7}: filtered recall@10 = {:.4}  [{}]",
            r.n,
            r.recall,
            if pass { "PASS >= 0.9" } else { "FAIL < 0.9" }
        );
    }
    if results.len() >= 2 {
        let first = &results[0];
        let last = results.last().unwrap();
        let n_growth = last.n as f64 / first.n as f64;
        let hnsw_growth = last.hnsw_ms / first.hnsw_ms;
        let brute_growth = last.brute_ms / first.brute_ms;
        // Sub-linear: HNSW query time grows far slower than |V| (well under the
        // linear factor), while the brute-force post-filter tracks |V| roughly 1:1.
        let sublinear = hnsw_growth < n_growth * 0.6;
        ok &= sublinear;
        println!(
            "  |V| grew {n_growth:.1}x  ->  hnsw latency {hnsw_growth:.2}x  (brute {brute_growth:.2}x)  [{}]",
            if sublinear {
                "PASS sub-linear"
            } else {
                "FAIL not sub-linear"
            }
        );
    }
    println!("\nRESULT: {}", if ok { "PASS" } else { "FAIL" });
    assert!(ok, "filtered-ANN acceptance failed (see table above)");
}
