//! Recall / latency / footprint bench for eg-ann (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed).
//!
//! Not a criterion bench — a plain `main` that builds an IVF-PQ+OPQ index at a
//! given scale, measures brute-force vs ADC-only vs ADC+refine recall@10, query
//! latency, and the index footprint. Run:
//!
//!   cargo bench -p eg-ann --bench recall -- 100000 128 1024 32 32 200
//!   #                                        N      dim  nlist m nprobe queries
//!
//! Scales to 1M; defaults are CI-sane.

use eg_ann::ivfpq::{IvfPq, IvfPqParams, SearchParams};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::Instant;

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

fn brute_knn(data: &[Vec<f32>], q: &[f32], k: usize) -> Vec<u64> {
    let mut s: Vec<(u64, f32)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let mut d = 0.0f32;
            for j in 0..v.len() {
                let df = q[j] - v[j];
                d += df * df;
            }
            (i as u64, d)
        })
        .collect();
    s.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    s.truncate(k);
    s.into_iter().map(|(id, _)| id).collect()
}

fn recall_at(
    idx: &IvfPq,
    data: &[Vec<f32>],
    queries: &[usize],
    k: usize,
    sp: SearchParams,
) -> (f64, f64) {
    let mut hits = 0usize;
    let mut total = 0usize;
    let t = Instant::now();
    for &qi in queries {
        let q = &data[qi];
        let truth = brute_knn(data, q, k);
        let got: Vec<u64> = idx.search(q, k, sp).into_iter().map(|r| r.id).collect();
        for tr in &truth {
            if got.contains(tr) {
                hits += 1;
            }
            total += 1;
        }
    }
    let recall = hits as f64 / total as f64;
    let ms = t.elapsed().as_secs_f64() * 1000.0 / queries.len() as f64;
    (recall, ms)
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let n: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let dim: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(128);
    let nlist: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(1024);
    let m: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(dim / 4);
    let nprobe: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(32);
    let nq: usize = a.get(6).and_then(|s| s.parse().ok()).unwrap_or(200);

    println!("eg-ann recall bench: N={n} dim={dim} nlist={nlist} m={m} nprobe={nprobe} q={nq}");
    let data = clustered_vecs(n, dim, (n / 250).max(64), 11);

    let params = IvfPqParams {
        dim,
        nlist,
        m,
        kmeans_iters: 25,
        opq_iters: 8,
        seed: 7,
    };
    let sample: Vec<Vec<f32>> = data.iter().step_by((n / 60_000).max(1)).cloned().collect();
    println!("training on {} sample vectors...", sample.len());
    let t = Instant::now();
    let mut idx = IvfPq::train(&params, &sample);
    println!("  trained in {:.1}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    let items: Vec<(u64, Vec<f32>)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64, v.clone()))
        .collect();
    idx.add(&items);
    println!(
        "  encoded+inserted {n} in {:.1}s",
        t.elapsed().as_secs_f64()
    );

    let mut q_rng = ChaCha8Rng::seed_from_u64(999);
    let queries: Vec<usize> = (0..nq).map(|_| q_rng.gen_range(0..n)).collect();

    let raw = recall_at(
        idx_ref(&idx),
        &data,
        &queries,
        10,
        SearchParams {
            nprobe,
            refine: false,
            refine_factor: 1,
        },
    );
    let refined = recall_at(
        &idx,
        &data,
        &queries,
        10,
        SearchParams {
            nprobe,
            refine: true,
            refine_factor: 16,
        },
    );

    let codes_b = idx.codes.len();
    let refine_b = idx.sq_codes.len();
    let raw_b = n * dim * 4;
    println!("\n--- results ---");
    println!("raw f32 footprint     : {:.3} GB", raw_b as f64 / 1e9);
    println!(
        "PQ codes footprint    : {:.3} GB ({:.0}x compression)",
        codes_b as f64 / 1e9,
        raw_b as f64 / codes_b as f64
    );
    println!(
        "SQ8 refine footprint  : {:.3} GB ({:.0}x vs raw)",
        refine_b as f64 / 1e9,
        raw_b as f64 / refine_b as f64
    );
    println!(
        "ADC-only   recall@10  : {:.4}   {:.2} ms/query",
        raw.0, raw.1
    );
    println!(
        "ADC+refine recall@10  : {:.4}   {:.2} ms/query",
        refined.0, refined.1
    );
}

fn idx_ref(i: &IvfPq) -> &IvfPq {
    i
}
