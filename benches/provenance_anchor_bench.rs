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
//! Both arms process the IDENTICAL amount of fan-in work (same `N`, same
//! `sample_size`), so the total wall-clock ratio between arms is a direct,
//! fair write-throughput overhead measurement. Prints commit/batch stats
//! alongside criterion's own per-iteration timing so the delta is visible
//! from both angles.
//!
//! Run: cargo bench --features security --bench provenance_anchor_bench

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    backend: Arc<RedbBackend>,
    ids: Vec<String>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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

    let mut totals: Vec<(bool, Duration, u64, u64)> = Vec::new();

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
            Some(spawn_anchor_loop(backend.clone(), ids, ANCHOR_INTERVAL))
        } else {
            None
        };

        let counter = std::cell::Cell::new(0usize);
        let started = Instant::now();
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
        let elapsed = started.elapsed();

        let stats = backend.commit_stats();
        eprintln!(
            "[provenance-anchor] anchoring={:<12} total_bench_wall={:>9.4}s commits={:<8} ops={:<8} avg_batch={:.2} lingered={}",
            if anchoring_on { "ON" } else { "OFF" },
            elapsed.as_secs_f64(),
            stats.commits(),
            stats.ops(),
            stats.avg_batch(),
            stats.lingered(),
        );
        totals.push((anchoring_on, elapsed, stats.commits(), stats.ops()));

        if let Some(task) = anchor_task {
            task.abort();
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
    group.finish();

    if let (Some((_, off, _, _)), Some((_, on, _, _))) = (
        totals.iter().find(|(a, ..)| !a),
        totals.iter().find(|(a, ..)| *a),
    ) {
        let off_s = off.as_secs_f64();
        let on_s = on.as_secs_f64();
        let overhead_pct = (on_s - off_s) / off_s * 100.0;
        eprintln!(
            "[provenance-anchor] SUMMARY: off={off_s:.4}s on={on_s:.4}s overhead={overhead_pct:+.3}% \
             (budget: <1%; anchor_interval={:?} is a deliberate worst-case stress cadence)",
            ANCHOR_INTERVAL
        );
    }
}

criterion_group!(benches, bench_provenance_anchor_overhead);
criterion_main!(benches);
