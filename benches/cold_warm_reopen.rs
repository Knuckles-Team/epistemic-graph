//! Cold-open vs warm-read latency of the redb durable tier (Phase-2 T-E1).
//!
//! Times the two legs a persistent engine pays differently for the SAME point-read:
//!   * `cold_reopen_read` — reopen a FRESH `RedbBackend` over a populated persist dir and
//!     serve the FIRST read (file-lock acquire + table open + first `begin_read()` MVCC
//!     snapshot on the target shard). The reopen cost is amortised across the shard.
//!   * `warm_read` — a subsequent point-read on the already-open backend (handle +
//!     snapshot path warm).
//!
//! The cold/warm p50 delta is the reopen amortization cost. Uses the durable reopen
//! fixture pattern from `tests/graphql_crossmodal_durable.rs` (unique pid+nanos persist
//! dir, `RedbBackend::open(dir, DurabilityPolicy::Each, 8192)`, write, `shutdown`, reopen).
//!
//! Run: `cargo bench --features full --bench cold_warm_reopen`
//! Gated to `--features full` (needs the redb backend + tokio server runtime); a default
//! `cargo check --benches` skips it.

use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use epistemic_graph::durability::DurabilityPolicy;
use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use tokio::runtime::Builder;

const GRAPH: &str = "__commons__";
const NODES: usize = 2_000;
const READ_ID: &str = "n1000";

fn node_props(k: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "k": k, "type": "Doc" })).unwrap()
}

/// A unique, self-cleaning persist dir under the system temp dir (no tempfile dep) — the
/// `graphql_crossmodal_durable` fixture pattern (pid + nanos).
fn unique_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "eg-coldwarm-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_string_lossy().to_string()
}

/// Populate a persist dir with `NODES` durable (commit-before-ack) nodes and shut the
/// writer down (releasing the per-file lock) so it can be COLD-reopened.
fn populate(rt: &tokio::runtime::Runtime, dir: &str) {
    let backend = Arc::new(
        RedbBackend::open(dir.to_string(), DurabilityPolicy::Each, 8192).expect("open redb"),
    );
    rt.block_on(async {
        for i in 0..NODES {
            backend
                .record_durable(
                    GRAPH,
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: node_props(i),
                    },
                )
                .await
                .expect("durable commit");
        }
    });
    backend.shutdown();
}

fn bench_cold_warm(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    // ── COLD: reopen a populated dir + first read, per iteration ──
    let cold_dir = unique_dir("cold");
    populate(&rt, &cold_dir);
    c.bench_function("cold_reopen_read", |b| {
        b.iter(|| {
            let backend = RedbBackend::open(cold_dir.clone(), DurabilityPolicy::Each, 8192)
                .expect("reopen redb");
            let got = rt
                .block_on(backend.read_node(GRAPH, READ_ID))
                .expect("read");
            // Release the file lock for the next cold reopen (the next iteration).
            backend.shutdown();
            black_box(got)
        });
    });

    // ── WARM: a populated backend opened ONCE, repeated reads on the warm handle ──
    let warm_dir = unique_dir("warm");
    populate(&rt, &warm_dir);
    let warm =
        RedbBackend::open(warm_dir.clone(), DurabilityPolicy::Each, 8192).expect("open warm");
    let _ = rt.block_on(warm.read_node(GRAPH, READ_ID)); // warm the handle/snapshot (untimed)
    c.bench_function("warm_read", |b| {
        b.iter(|| black_box(rt.block_on(warm.read_node(GRAPH, READ_ID)).expect("read")));
    });
    warm.shutdown();

    let _ = std::fs::remove_dir_all(&cold_dir);
    let _ = std::fs::remove_dir_all(&warm_dir);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = bench_cold_warm
}
criterion_main!(benches);
