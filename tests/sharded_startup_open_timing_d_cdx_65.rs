//! D-CDX-65 startup-readiness instrumentation proof (CONCEPT:EG-KG.backend.sharded-k-way-durable).
//!
//! The live incident this item tracks: a served rollout acquired the single-writer
//! lock, logged "K=4 graph-<n>.redb files", then emitted NOTHING for 5m27s before the
//! serving port opened — while loading an 11 GB persist dir (four shards: 532 MB, 5.0
//! GB, 4.0 GB, 1.2 GB). Operators saw silence, not a stuck-vs-progressing signal.
//!
//! `RedbBackend::open_with_shards` used to open its K shards in a plain sequential
//! `for` loop with NO log line between shards, so the entire multi-minute cost was one
//! silent gap. The fix (this same lane, `src/server/persistence/redb_backend.rs`)
//! opens all K shards CONCURRENTLY on their own OS threads and logs each shard's
//! start (with its on-disk size) and completion (with duration) individually.
//!
//! This is a `#[ignore]`d, on-demand timing measurement (not a CI gate — wall-clock
//! assertions in the shared gate would be flaky, matching this repo's existing
//! `#[ignore]`d timing tests in `tests/advanced_crossmodal_roundtrip.rs` /
//! `tests/pgwire_roundtrip.rs`). It builds a REPRESENTATIVE synthetic K=4 store (not a
//! copy of the literal 11 GB production store — that never leaves the cluster) and
//! reports two real, directly comparable numbers from the SAME open call:
//!
//!   * `after`  — actual wall-clock time of the new concurrent `open_with_shards`.
//!   * `before` (reconstructed, not re-run) — the sum of the same call's own per-shard
//!     durations, i.e. exactly what the OLD sequential loop would have cost, since each
//!     shard's `Database::create` does the same on-disk work whether it runs alongside
//!     the others or strictly after them.
//!
//! Run: `cargo test --features full --test sharded_startup_open_timing_d_cdx_65 -- \
//!   --ignored --nocapture`

#![cfg(feature = "redb")]

use std::sync::{Arc, Mutex};

use epistemic_graph::durability::DurabilityPolicy;
use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;

/// Node count per graph and payload size, tuned to produce multi-hundred-MB shard
/// files in a few minutes of setup time — representative in SHAPE (multiple
/// differently-sized shards opened at once) but smaller in absolute bytes than the
/// production 11 GB store measured in the incident. Sample counts are printed by the
/// test itself so the report never has to guess the scale actually exercised.
const NODES_PER_GRAPH: usize = 8_000;
const PROPERTY_BYTES: usize = 6_000;
/// Writes in flight at once per graph — real bulk-load traffic is concurrent, and the
/// redb writer's group-commit batching (`RedbGroupCommitConfig`) only coalesces
/// multiple PENDING writes into one fsync, so a single in-flight write per graph (as a
/// naive one-at-a-time await loop would produce) defeats it and measures fsync latency
/// per record instead of realistic bulk-load throughput.
const CONCURRENT_WRITES: usize = 64;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-dcdx65-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn big_props(k: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({
        "k": k,
        "padding": "x".repeat(PROPERTY_BYTES),
    }))
    .unwrap()
}

/// A tracing subscriber that just collects every formatted event line so this test can
/// pull the per-shard "opened in Xms" lines back out without depending on log parsing
/// elsewhere in the crate.
struct LineCollector(Arc<Mutex<Vec<String>>>);

impl<S> tracing_subscriber::Layer<S> for LineCollector
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut v = Visitor(String::new());
        event.record(&mut v);
        self.0.lock().unwrap().push(v.0);
    }
}

#[test]
#[ignore = "on-demand timing measurement for D-CDX-65 — expensive multi-hundred-MB \
            synthetic store build; run explicitly, not part of the default suite"]
fn measure_concurrent_vs_sequential_shard_open_d_cdx_65() {
    use tracing_subscriber::layer::SubscriberExt;

    std::env::set_var("EPISTEMIC_GRAPH_REDB_SHARDS", "4");

    // The synthetic-store build (hundreds of MB of durable writes) is the expensive
    // part of this measurement (real elapsed time, not the open call under test); allow
    // reusing an already-built store from a prior run via EG_DCDX65_REUSE_DIR so a
    // subscriber-wiring fix doesn't force paying for it twice.
    let reuse = std::env::var("EG_DCDX65_REUSE_DIR").ok();
    let dir = reuse
        .as_ref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| unique_dir("sharded"));
    let dir_s = dir.to_string_lossy().to_string();
    let built_fresh = reuse.is_none();

    // ── Build a K=4 synthetic store: four differently-sized graphs so EG-026 hash
    // routing spreads them (unevenly, like production's 532MB/5GB/4GB/1.2GB) across
    // the four shard files. ──
    let build_start = std::time::Instant::now();
    let sizes = [1usize, 3, 2, 1]; // relative multiplier per graph -> uneven shard sizes
    if built_fresh {
        let backend = Arc::new(
            RedbBackend::open(
                dir_s.clone(),
                DurabilityPolicy::Interval(std::time::Duration::from_millis(50)),
                8192,
            )
            .expect("build store"),
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();
        let mut total_nodes = 0usize;
        for (gi, mult) in sizes.iter().enumerate() {
            let graph = Arc::new(format!("startup-bench-graph-{gi}"));
            let n = NODES_PER_GRAPH * mult;
            total_nodes += n;
            rt.block_on(async {
                use futures::stream::{self, StreamExt};
                stream::iter(0..n)
                    .for_each_concurrent(CONCURRENT_WRITES, |i| {
                        let backend = backend.clone();
                        let graph = graph.clone();
                        async move {
                            backend
                                .record_durable(
                                    &graph,
                                    &Method::AddNode {
                                        node_id: format!("n{i}"),
                                        properties_msgpack: big_props(i),
                                    },
                                )
                                .await
                                .expect("durable write");
                        }
                    })
                    .await;
            });
        }
        backend.shutdown();
        println!(
            "[D-CDX-65] built synthetic store: {total_nodes} nodes total across {} graphs, \
             {PROPERTY_BYTES} bytes/property, {CONCURRENT_WRITES}-way concurrent writes, in {:?}",
            sizes.len(),
            build_start.elapsed()
        );
    }

    let mut on_disk_total = 0u64;
    let mut shard_files = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        let meta = entry.metadata().unwrap();
        if meta.is_file() {
            on_disk_total += meta.len();
            shard_files.push((entry.file_name().to_string_lossy().to_string(), meta.len()));
        }
    }
    shard_files.sort();
    println!("[D-CDX-65] on-disk shard files (bytes): {shard_files:?}");
    println!(
        "[D-CDX-65] total on-disk size: {on_disk_total} bytes ({:.1} MB)",
        on_disk_total as f64 / 1_048_576.0
    );

    // ── Reopen: capture the new concurrent open's own per-shard log lines AND its
    // real wall-clock duration in the same call. MUST be a process-GLOBAL subscriber
    // (not `with_default`, which is thread-local): `open_with_shards` opens each shard
    // on its OWN OS thread via `std::thread::scope`, and a freshly spawned OS thread
    // does not inherit the parent thread's thread-local tracing dispatcher — only a
    // global default reaches it.
    let lines = Arc::new(Mutex::new(Vec::<String>::new()));
    let collector = LineCollector(lines.clone());
    let subscriber = tracing_subscriber::registry().with(collector);
    let _ = tracing::subscriber::set_global_default(subscriber);
    let t0 = std::time::Instant::now();
    let backend =
        RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192).expect("reopen store");
    let reopen_wall_clock = t0.elapsed();
    backend.shutdown();

    let per_shard_ms: Vec<f64> = lines
        .lock()
        .unwrap()
        .iter()
        .filter_map(|line| {
            // Lines look like: "redb: shard 2/4 open finished in 123.456ms"
            let marker = "open finished in ";
            let idx = line.find(marker)?;
            let rest = &line[idx + marker.len()..];
            let numeric: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            numeric.parse::<f64>().ok().map(|ms| {
                // Duration::Debug prints e.g. "123.456ms" or "1.234s" — normalize to ms.
                if rest.trim_start_matches(&numeric).starts_with("s")
                    && !rest.trim_start_matches(&numeric).starts_with("ms")
                {
                    ms * 1000.0
                } else {
                    ms
                }
            })
        })
        .collect();

    let sequential_proxy_ms: f64 = per_shard_ms.iter().sum();
    println!(
        "[D-CDX-65] captured {} per-shard open-duration log line(s): {:?} ms",
        per_shard_ms.len(),
        per_shard_ms
    );
    println!(
        "[D-CDX-65] BEFORE (reconstructed sequential sum of the real per-shard costs): \
         {sequential_proxy_ms:.1} ms"
    );
    println!(
        "[D-CDX-65] AFTER  (actual concurrent open_with_shards wall clock):            \
         {:.1} ms",
        reopen_wall_clock.as_secs_f64() * 1000.0
    );
    if !per_shard_ms.is_empty() {
        println!(
            "[D-CDX-65] speedup (before/after): {:.2}x over {} shard(s), {:.1} MB total",
            sequential_proxy_ms / reopen_wall_clock.as_secs_f64().max(0.000_001) / 1000.0,
            per_shard_ms.len(),
            on_disk_total as f64 / 1_048_576.0
        );
    }

    assert!(
        !per_shard_ms.is_empty(),
        "expected per-shard open-duration log lines from the new instrumentation — none \
         were captured, so the tracing wiring in this test (or the log format in \
         redb_backend.rs) has drifted"
    );

    let _ = std::fs::remove_dir_all(&dir);
    std::env::remove_var("EPISTEMIC_GRAPH_REDB_SHARDS");
}
