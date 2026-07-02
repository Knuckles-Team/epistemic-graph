//! Massive-scale engine micro-benchmark harness (CONCEPT:EG-096).
//!
//! The standing "how fast is the core?" harness for the three hot substrates the
//! whole engine is built on. It is DEV-only (criterion), never linked into the
//! server binary, and runs entirely IN-PROCESS against the pure `eg-core`
//! `GraphCore` + the pure `eg-ann` IVF-PQ index — NO socket, NO Tokio, NO durable
//! tier — so it isolates the compute cost from any transport/fsync noise. It is
//! deliberately feature-light (default features only), so a plain
//! `cargo bench --no-run` compiles it while the redb/server benches skip.
//!
//! Three groups (see benches/README.md for methodology + interpretation):
//!   * `eg096_node_write` — AddNode throughput into ONE graph (elements/sec).
//!   * `eg096_edge_write` — AddEdge throughput over a pre-populated node set.
//!   * `eg096_query`      — in-process read latency: single-hop `get_neighbors`
//!     and a `get_nodes_by_label` index lookup.
//!   * `eg096_ann_knn`    — IVF-PQ kNN search latency swept over `nprobe`.
//!
//! Run:  cargo bench --bench eg096_massive_scale_bench
//!
//! CONCEPT:EG-096 — massive-scale benchmark harness.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use eg_ann::{IvfPq, IvfPqParams, SearchParams};
use epistemic_graph::graph::GraphCore;

// ── Deterministic, dependency-free PRNG ──────────────────────────────────
// splitmix64: keeps the harness reproducible across runs and avoids pulling a
// `rand` dev-dep into the facade crate (feature-light by design).
struct SplitMix64(u64);
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f32(&mut self) -> f32 {
        // 24-bit mantissa → [0,1)
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
    fn range(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

fn node_props(k: usize, label: &str) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "k": k, "label": label })).unwrap()
}

// ── Group 1: node write throughput ───────────────────────────────────────
fn bench_node_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("eg096_node_write");
    for &n in &[10_000usize, 50_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let core = GraphCore::new();
                for i in 0..n {
                    core.add_node(format!("n{i}"), node_props(i, "Doc"));
                }
                black_box(core.node_count())
            });
        });
    }
    group.finish();
}

// ── Group 2: edge write throughput ───────────────────────────────────────
fn bench_edge_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("eg096_edge_write");
    const NODES: usize = 20_000;
    for &e in &[20_000usize, 100_000] {
        group.throughput(Throughput::Elements(e as u64));
        group.bench_with_input(BenchmarkId::from_parameter(e), &e, |b, &e| {
            b.iter_batched(
                || {
                    // Setup (not timed): a graph with NODES nodes to link.
                    let core = GraphCore::new();
                    for i in 0..NODES {
                        core.add_node(format!("n{i}"), node_props(i, "Doc"));
                    }
                    core
                },
                |core| {
                    let mut rng = SplitMix64::new(7);
                    for _ in 0..e {
                        let s = rng.range(NODES);
                        let t = rng.range(NODES);
                        let _ = core.add_edge(format!("n{s}"), format!("n{t}"), Vec::new());
                    }
                    black_box(core.edge_count())
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ── Group 3: in-process query latency ────────────────────────────────────
fn build_query_graph(nodes: usize, edges: usize) -> GraphCore {
    let core = GraphCore::new();
    for i in 0..nodes {
        // Two labels so the label-index lookup returns a bounded slice.
        let label = if i % 2 == 0 { "Doc" } else { "Chunk" };
        core.add_node(format!("n{i}"), node_props(i, label));
    }
    let mut rng = SplitMix64::new(11);
    for _ in 0..edges {
        let s = rng.range(nodes);
        let t = rng.range(nodes);
        let _ = core.add_edge(format!("n{s}"), format!("n{t}"), Vec::new());
    }
    core
}

fn bench_query(c: &mut Criterion) {
    const NODES: usize = 50_000;
    const EDGES: usize = 200_000;
    let core = build_query_graph(NODES, EDGES);
    // Warm the lazy label index once so the bench measures the steady-state hit.
    let _ = core.get_nodes_by_label("Doc", 1);

    let mut group = c.benchmark_group("eg096_query");
    let mut rng = SplitMix64::new(23);

    group.bench_function("get_neighbors_1hop", |b| {
        b.iter(|| {
            let id = format!("n{}", rng.range(NODES));
            black_box(core.get_neighbors(&id).unwrap_or_default())
        });
    });

    group.bench_function("get_nodes_by_label_limit100", |b| {
        b.iter(|| black_box(core.get_nodes_by_label("Doc", 100)));
    });

    group.finish();
}

// ── Group 4: ANN kNN search latency ──────────────────────────────────────
fn synth_vecs(n: usize, dim: usize, nclusters: usize, seed: u64) -> Vec<Vec<f32>> {
    // Clustered (gaussian-ish blobs) — the regime PQ is designed for; uniform
    // random understates achievable recall/latency realism.
    let mut rng = SplitMix64::new(seed);
    let centers: Vec<Vec<f32>> = (0..nclusters)
        .map(|_| (0..dim).map(|_| rng.next_f32() * 2.0 - 1.0).collect())
        .collect();
    (0..n)
        .map(|_| {
            let c = &centers[rng.range(nclusters)];
            (0..dim)
                .map(|j| c[j] + (rng.next_f32() - 0.5) * 0.30)
                .collect()
        })
        .collect()
}

fn bench_ann_knn(c: &mut Criterion) {
    const DIM: usize = 128;
    const N: usize = 20_000;
    let params = IvfPqParams {
        dim: DIM,
        nlist: 128,
        m: 16,
        kmeans_iters: 10,
        opq_iters: 2,
        seed: 42,
    };
    let vecs = synth_vecs(N, DIM, 64, 1);
    // Train on a representative sample, then add the whole corpus.
    let train: Vec<Vec<f32>> = vecs.iter().take(4_000).cloned().collect();
    let mut index = IvfPq::train(&params, &train);
    let items: Vec<(u64, Vec<f32>)> = vecs
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64, v.clone()))
        .collect();
    index.add(&items);

    let queries = synth_vecs(256, DIM, 64, 99);

    let mut group = c.benchmark_group("eg096_ann_knn");
    group.throughput(Throughput::Elements(1));
    let mut qi = 0usize;
    for &nprobe in &[8usize, 16, 32] {
        let sp = SearchParams {
            nprobe,
            refine: true,
            refine_factor: 8,
        };
        group.bench_with_input(BenchmarkId::from_parameter(nprobe), &nprobe, |b, _| {
            b.iter(|| {
                let q = &queries[qi % queries.len()];
                qi += 1;
                black_box(index.search(q, 10, sp))
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_node_write,
    bench_edge_write,
    bench_query,
    bench_ann_knn
);
criterion_main!(benches);
