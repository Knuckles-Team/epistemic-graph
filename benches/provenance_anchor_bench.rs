//! Provenance-anchoring write-path overhead (provenance anchoring, CONCEPT:EG-KG.sharding.row-level-security):
//! measures the write-throughput delta a running anchor sweep adds to the
//! ordinary durable write path, so the <1% overhead budget is a measured
//! number, not an assumption.
//!
//! Fans `N` concurrent `record_durable` (authoritative, awaited) `AddNode`
//! writes into ONE redb backend — the SAME `fan_in` harness
//! `redb_group_commit_bench.rs` uses — under two arms:
//!   * `anchoring_off` — BASELINE: no anchor activity at all.
//!   * `anchoring_on`  — a background task repeatedly mutates one node in a
//!     synthetic `:ToolCall` window (so the window's Merkle root changes EVERY
//!     tick — a real durable commit every time, never the free
//!     unchanged-root skip path) and re-anchors it on a DELIBERATELY
//!     aggressive fixed interval, far tighter than any real deployment's
//!     `EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS` (seconds, not milliseconds).
//!     This is a stress-test upper bound on overhead, not a realistic
//!     cadence — if overhead stays under budget here, it is strictly lower
//!     at a real deployment's cadence.
//!
//! Criterion's OWN printed `time: [lo mid hi]` per arm is the authoritative
//! comparison (its `measurement_time` targets a fixed WALL-CLOCK budget per
//! arm and adapts the iteration count to fit it, so a wall-clock span
//! measured outside `bench_with_input` is NOT a valid per-iteration-cost
//! proxy -- an earlier version of this file computed one and it was
//! materially skewed by this box's concurrent-build contention). Also prints
//! each arm's commit/batch stats (counts, not timing) for corroboration.
//!
//! Run: cargo bench --features security --bench provenance_anchor_bench

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use epistemic_graph::audit;
use epistemic_graph::durability::DurabilityPolicy;
use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use tokio::runtime::Builder;

const GRAPH: &str = "__commons__";
const ANCHOR_WINDOW: usize = 500;

fn node_props(k: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "k": k })).unwrap()
}

fn tool_call_props(tick: u64) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "node_type": "ToolCall", "tick": tick })).unwrap()
}

/// Fan `n` authoritative (commit-before-ack) writes from `producers` concurrent
/// tasks into ONE redb backend. Identical harness to
/// `redb_group_commit_bench.rs::fan_in` — see that file for the pipelining
/// rationale.
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

/// Seed `ANCHOR_WINDOW` synthetic `:ToolCall` nodes for the anchor loop to
/// repeatedly re-anchor.
async fn seed_anchor_window(backend: &Arc<RedbBackend>) -> Vec<String> {
    let ids: Vec<String> = (0..ANCHOR_WINDOW)
        .map(|i| format!("toolcall-{i}"))
        .collect();
    for id in &ids {
        backend
            .record_durable(
                GRAPH,
                &Method::AddNode {
                    node_id: id.clone(),
                    properties_msgpack: tool_call_props(0),
                },
            )
            .await
            .expect("seed durable commit ok");
    }
    ids
}

/// The aggressive anchor loop: every `interval`, mutate one anchored node (so
/// the window's root always changes -- a REAL commit every tick, never the
/// free unchanged-root skip path), hash the window off-thread, then commit the
/// anchor. Mirrors `server::persistence::provenance_anchor::sweep`'s two-step
/// shape (off-writer-thread hash, then a small writer-thread commit) but
/// against a fixed synthetic window instead of a live `GraphCore` label index,
/// so this bench needs no `ServerState`/registry.
fn spawn_anchor_loop(
    rt: &tokio::runtime::Runtime,
    backend: Arc<RedbBackend>,
    ids: Vec<String>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    // `rt.spawn` (not the bare `tokio::spawn`) because this is called from
    // criterion's plain synchronous benchmark-setup code, OUTSIDE any
    // currently-executing async task -- the bare form panics with "there is
    // no reactor running" without an ambient runtime context to spawn onto.
    rt.spawn(async move {
        let tick = AtomicU64::new(1);
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let t = tick.fetch_add(1, Ordering::Relaxed);
            let mutated_id = &ids[(t as usize) % ids.len()];
            let _ = backend
                .record_durable(
                    GRAPH,
                    &Method::AddNode {
                        node_id: mutated_id.clone(),
                        properties_msgpack: tool_call_props(t),
                    },
                )
                .await;
            let members = match backend.provenance_leaf_hashes_blocking(GRAPH, &ids) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if members.is_empty() {
                continue;
            }
            let hashes: Vec<audit::Hash> = members.iter().map(|(_, h)| *h).collect();
            let root = audit::mth_from_hashes(&hashes);
            let _ = backend.provenance_anchor_commit_blocking(GRAPH, root, members);
        }
    })
}

fn bench_provenance_anchor_overhead(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");

    const N: usize = 1_000;
    // Deliberately aggressive vs. any real `EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS`
    // (seconds) -- a stress-test upper bound, not a realistic cadence.
    const ANCHOR_INTERVAL: Duration = Duration::from_millis(10);

    let mut group = c.benchmark_group("provenance_anchor_write_overhead");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    for &anchoring_on in &[false, true] {
        let dir = std::env::temp_dir().join(format!(
            "eg-provenance-bench-{}-{anchoring_on}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = Arc::new(
            RedbBackend::open(
                dir.to_string_lossy().to_string(),
                // Long interval so ONLY the barrier path commits a batch, never
                // the timer -- matches `redb_group_commit_bench.rs`'s rationale.
                DurabilityPolicy::Interval(Duration::from_millis(500)),
                4096,
            )
            .expect("open redb backend"),
        );

        let anchor_task = if anchoring_on {
            let ids = rt.block_on(seed_anchor_window(&backend));
            Some(spawn_anchor_loop(
                &rt,
                backend.clone(),
                ids,
                ANCHOR_INTERVAL,
            ))
        } else {
            None
        };

        let counter = std::cell::Cell::new(0usize);
        // Criterion (not a wall-clock wrapper around `bench_with_input`) is the
        // authoritative timing comparison: it targets a fixed `measurement_time`
        // per arm and adapts its OWN internal iteration count to fit it, so a
        // wall-clock span measured OUTSIDE `bench_with_input` is not a valid
        // apples-to-apples proxy for per-iteration cost -- the two arms' outer
        // wall-clock spans can differ even when their per-iteration means are
        // close, purely from how many iterations criterion chose to run under
        // momentary external scheduling noise (this box runs many concurrent
        // wave-agent builds). Read the printed `time: [lo mid hi]` per arm below
        // (and `docs/benchmarks.md`'s recorded run) for the real comparison.
        group.bench_with_input(
            BenchmarkId::from_parameter(if anchoring_on {
                "anchoring_on"
            } else {
                "anchoring_off"
            }),
            &anchoring_on,
            |b, _| {
                b.iter(|| {
                    let base = counter.get();
                    counter.set(base + N);
                    rt.block_on(fan_in(&backend, N, base));
                });
            },
        );

        let stats = backend.commit_stats();
        eprintln!(
            "[provenance-anchor] anchoring={:<12} commits={:<8} ops={:<8} avg_batch={:.2} lingered={}",
            if anchoring_on { "ON" } else { "OFF" },
            stats.commits(),
            stats.ops(),
            stats.avg_batch(),
            stats.lingered(),
        );

        if let Some(task) = anchor_task {
            task.abort();
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
    group.finish();
}

criterion_group!(benches, bench_provenance_anchor_overhead);
criterion_main!(benches);
