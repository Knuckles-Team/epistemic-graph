//! Write-coalescer `__commons__` contention reproduction (CONCEPT:EG-KG.txn.write-path-benchmarks).
//!
//! This is the standing micro-reproduction of the live H1 contention: under the
//! `__commons__` ingestion firehose a read (semantic_search) goes 0.02s idle → 14s,
//! because every structural write serializes on that one graph's topology write lock.
//! Here N concurrent producers fan single-op writes into ONE coalesced graph; the
//! batch window is swept so the contention curve — lock-acquisitions (∝ 1/max_batch)
//! vs throughput — is visible. `max_batch == 1` is the pre-coalescer baseline (one
//! lock acquisition per op); larger windows amortize the lock.
//!
//! Run:        cargo bench --features server --bench write_coalescer_bench
//! Flamegraph: cargo flamegraph --features server --bench write_coalescer_bench
//!
//! Gated to `--features server` (the coalescer is Tokio-based and lives behind that
//! feature); a default `cargo check --benches` skips it.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use epistemic_graph::graph::GraphCore;
use epistemic_graph::write_coalescer::{fan_in_add_nodes, CoalescerConfig, GraphWriter};
use tokio::runtime::Builder;

const GRAPH: &str = "__commons__";

fn node_props(k: i64) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "k": k })).unwrap()
}

/// Fan `n` AddNode writes from `producers` concurrent tasks into ONE graph through
/// the coalescer at `max_batch`, using the coalescer's own pipelined fan-in driver
/// (producers fire without blocking on each reply; a full bounded queue is retried
/// after draining a reply; no unordered inline path is used).
async fn fan_in(n: usize, producers: usize, max_batch: usize) {
    let core = Arc::new(GraphCore::new());
    let cfg = CoalescerConfig {
        max_batch,
        queue_capacity: (max_batch * 4).max(1024),
        // max_batch == 1 is the faithful one-lock-per-op baseline: no linger so a lone
        // write is not coalesced with a follower.
        max_linger: if max_batch == 1 {
            Duration::ZERO
        } else {
            Duration::from_micros(100)
        },
    };
    let writer = GraphWriter::spawn(GRAPH.into(), core, cfg);
    fan_in_add_nodes(&writer, n, producers, node_props).await;
}

fn bench_contention(c: &mut Criterion) {
    // A multi-thread runtime so the producers and the drain worker actually contend.
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");

    const N: usize = 20_000;
    const PRODUCERS: usize = 64;

    let mut group = c.benchmark_group("write_coalescer_fan_in");
    group.throughput(Throughput::Elements(N as u64));
    // Sweep the batch window: 1 (pre-coalescer, one lock/op) → 256 (amortized).
    for &max_batch in &[1usize, 8, 32, 128, 256] {
        group.bench_with_input(
            BenchmarkId::from_parameter(max_batch),
            &max_batch,
            |b, &mb| {
                b.iter(|| rt.block_on(fan_in(N, PRODUCERS, mb)));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_contention);
criterion_main!(benches);
