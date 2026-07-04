//! Redb durable group-commit micro-linger reproduction (CONCEPT:EG-KG.backend.adaptive-linger-coalesce).
//!
//! Live profiling showed the single `eg-redb-writer` thread pinned ~100% on ext4
//! writeback (disk ~83% util, ~50ms write latency, queue depth 42) while every tokio
//! worker sat idle — it is the ingestion write ceiling. The writer already
//! group-commits, but because every authoritative write carries a commit-before-ack
//! `done` oneshot, the barrier is ALWAYS set, so it commits the instant the channel
//! drains ⇒ with serial in-flight writes the batch is ~1 op ⇒ ~1 fsync/write.
//!
//! This bench fans N concurrent `record_durable` (authoritative, awaited) writes into
//! ONE redb backend and sweeps the linger knob:
//!   * `linger_us = 0`  → BASELINE: commit-on-drain, no accumulation window.
//!   * `linger_us > 0`  → AFTER: a bounded micro-linger lets concurrent writers
//!     coalesce into one fsync.
//!
//! It prints `commits / ops / avg_batch` per setting so the ops-per-fsync win is
//! visible alongside criterion's throughput (elements/sec).
//!
//! Run: cargo bench --features full --bench redb_group_commit_bench
//!
//! Gated to `--features full` (needs the redb backend + tokio server runtime); a
//! default `cargo check --benches` skips it.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use epistemic_graph::wal_service::FsyncPolicy;
use tokio::runtime::Builder;

const GRAPH: &str = "__commons__";

fn node_props(k: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "k": k })).unwrap()
}

/// Fan `n` authoritative (commit-before-ack) writes from `producers` concurrent
/// tasks into ONE redb backend. Producers PIPELINE — each fires its own
/// `record_durable` task and they all await together — so the writer sees concurrent
/// in-flight barrier writes within the micro-linger window (the case the linger
/// targets). The bench starts at offset `base` so successive criterion iterations
/// write distinct node ids (no upsert short-circuit skewing the I/O).
async fn fan_in(backend: &Arc<RedbBackend>, n: usize, base: usize) {
    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let b = backend.clone();
        let id = base + i;
        tasks.push(tokio::spawn(async move {
            b.record_durable(
                GRAPH,
                &Method::AddNode {
                    node_id: format!("n{id}"),
                    properties_msgpack: node_props(id),
                },
            )
            .await
        }));
    }
    for t in tasks {
        t.await.unwrap().expect("durable commit ok");
    }
}

fn bench_group_commit(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");

    const N: usize = 2_000;

    let mut group = c.benchmark_group("redb_group_commit_fan_in");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(20);

    // Sweep the linger knob: 0 = baseline (commit-on-drain), >0 = micro-linger on.
    for &linger_us in &[0u64, 500, 1000, 2000] {
        // The writer reads the knob once at open, so set it BEFORE opening, and open a
        // fresh backend (its own dir) per setting.
        std::env::set_var(
            "EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US",
            linger_us.to_string(),
        );
        std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW", "512");
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-gc-bench-{}-{linger_us}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = Arc::new(
            RedbBackend::open(
                dir.to_string_lossy().to_string(),
                // Long interval so ONLY the barrier path (+ linger) commits a batch,
                // never the timer — isolates the linger's effect on batch size.
                FsyncPolicy::Interval(Duration::from_millis(500)),
                4096,
            )
            .expect("open redb backend"),
        );
        std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US");
        std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW");

        let stats = backend.commit_stats();
        let counter = std::cell::Cell::new(0usize);
        group.bench_with_input(
            BenchmarkId::from_parameter(linger_us),
            &linger_us,
            |b, _| {
                b.iter(|| {
                    let base = counter.get();
                    counter.set(base + N);
                    rt.block_on(fan_in(&backend, N, base));
                });
            },
        );
        eprintln!(
            "[EG-024] linger_us={linger_us:<5} commits={:<8} ops={:<8} avg_batch={:.2} lingered={}",
            stats.commits(),
            stats.ops(),
            stats.avg_batch(),
            stats.lingered(),
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
    group.finish();
}

criterion_group!(benches, bench_group_commit);
criterion_main!(benches);
