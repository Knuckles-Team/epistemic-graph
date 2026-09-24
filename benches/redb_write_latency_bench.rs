//! Per-write redb latency, decomposed by commit phase (EH-290).
//!
//! EH-290 measured one durable write transaction at a ~1.3s median in a loaded
//! deployment. This bench reproduces the shape -- ONE awaited write at a time,
//! so every write pays its own commit -- through the engine's real storage path
//! (`RedbBackend::record_durable` -> shard writer thread -> `commit_ops` ->
//! the transaction kernel -> redb), and prints p50/p95/p99 of the end-to-end
//! latency beside the same percentiles of every `commit_phase` tracing span the
//! commit path emits (`CommitPhaseTimer` in the engine, the storage kernel's
//! `redb_begin_write` / `write_authority_validation` / `redb_commit`).
//!
//! Two raw-redb floors run first on the same directory: one-row commits at
//! `Durability::Immediate` (the fsync floor of this disk) and at
//! `Durability::None` (the same page writes without the sync), so the engine's
//! numbers can be read against what the medium itself costs.
//!
//! The engine scenario is swept across graph sizes because a per-commit cost
//! that grows with the graph is invisible on a fresh store.
//!
//! Deterministic: fixed payloads, fixed node ids, no RNG. Configuration (env):
//!   * `EG_WRITE_LATENCY_DIR`   -- store directory (default: the OS temp dir;
//!     point it at the disk under test, never at tmpfs)
//!   * `EG_WRITE_LATENCY_N`     -- measured writes per scenario (default 400)
//!   * `EG_WRITE_LATENCY_SIZES` -- comma-separated graph sizes (default
//!     `0,10000,50000`)
//!
//! Run: cargo bench --features full --bench redb_write_latency_bench

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use redb::{Database, Durability, TableDefinition};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

const GRAPH: &str = "latency_graph";
const PAYLOAD_BYTES: usize = 256;
const SEED_WAVE: usize = 1_000;
const RAW_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raw_latency");

// ── phase collector ──────────────────────────────────────────────────────

/// Every closed `commit_phase` span's wall time, keyed by its `phase` field.
#[derive(Clone, Default)]
struct PhaseSamples(Arc<Mutex<BTreeMap<String, Vec<f64>>>>);

impl PhaseSamples {
    fn take(&self) -> BTreeMap<String, Vec<f64>> {
        std::mem::take(&mut *self.0.lock().expect("phase samples"))
    }
}

struct PhaseStart {
    phase: String,
    at: Instant,
}

#[derive(Default)]
struct PhaseField(Option<String>);

impl Visit for PhaseField {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "phase" {
            self.0 = Some(value.to_string());
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "phase" {
            self.0 = Some(format!("{value:?}").trim_matches('"').to_string());
        }
    }
}

impl<S> Layer<S> for PhaseSamples
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().name() != "commit_phase" {
            return;
        }
        let mut field = PhaseField::default();
        attrs.record(&mut field);
        if let (Some(phase), Some(span)) = (field.0, ctx.span(id)) {
            span.extensions_mut().insert(PhaseStart {
                phase,
                at: Instant::now(),
            });
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let extensions = span.extensions();
        let Some(start) = extensions.get::<PhaseStart>() else {
            return;
        };
        let seconds = start.at.elapsed().as_secs_f64();
        self.0
            .lock()
            .expect("phase samples")
            .entry(start.phase.clone())
            .or_default()
            .push(seconds);
    }
}

// ── statistics ───────────────────────────────────────────────────────────

struct Summary {
    count: usize,
    mean: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
}

fn summarize(samples: &[f64]) -> Summary {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mean = sorted.iter().sum::<f64>() / sorted.len().max(1) as f64;
    Summary {
        count: sorted.len(),
        mean,
        p50: percentile(&sorted, 0.50),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        max: sorted.last().copied().unwrap_or(0.0),
    }
}

fn ms(seconds: f64) -> f64 {
    seconds * 1_000.0
}

fn print_summary(label: &str, samples: &[f64]) -> Summary {
    let s = summarize(samples);
    println!(
        "{label:<44} n={:<6} p50={:>9.3}ms p95={:>9.3}ms p99={:>9.3}ms max={:>9.3}ms mean={:>9.3}ms",
        s.count,
        ms(s.p50),
        ms(s.p95),
        ms(s.p99),
        ms(s.max),
        ms(s.mean)
    );
    s
}

/// The phase table for one scenario: each phase's time per measured write,
/// as a share of the end-to-end mean write latency. Phases nest
/// (`commit_ops` contains the rest), so shares do not sum to 100%.
fn print_phases(phases: &BTreeMap<String, Vec<f64>>, writes: usize, e2e_mean: f64) {
    for (phase, samples) in phases {
        let s = print_summary(&format!("  phase {phase}"), samples);
        let per_write = s.mean * s.count as f64 / writes.max(1) as f64;
        println!(
            "  {:<42} per_write={:>9.3}ms share_of_e2e_mean={:>6.1}%",
            "",
            ms(per_write),
            100.0 * per_write / e2e_mean.max(1e-12)
        );
    }
}

// ── raw redb floor ───────────────────────────────────────────────────────

fn raw_redb(dir: &std::path::Path, durability: Durability, label: &str, n: usize) {
    let path = dir.join(format!("raw-{label}.redb"));
    let _ = std::fs::remove_file(&path);
    let db = Database::create(&path).expect("create raw redb");
    let payload = vec![7u8; PAYLOAD_BYTES];
    let mut samples = Vec::with_capacity(n);
    for key in 0..n as u64 {
        let started = Instant::now();
        let mut txn = db.begin_write().expect("begin raw write");
        txn.set_durability(durability).expect("durability");
        txn.open_table(RAW_TABLE)
            .expect("open raw table")
            .insert(key, payload.as_slice())
            .expect("raw insert");
        txn.commit().expect("raw commit");
        samples.push(started.elapsed().as_secs_f64());
    }
    print_summary(&format!("raw redb one-row commit [{label}]"), &samples);
    drop(db);
    let _ = std::fs::remove_file(&path);
}

// ── engine path ──────────────────────────────────────────────────────────

fn add_node(id: usize) -> Method {
    let props = serde_json::json!({ "k": id, "pad": "x".repeat(PAYLOAD_BYTES) });
    Method::AddNode {
        node_id: format!("n{id:08}"),
        properties_msgpack: rmp_serde::to_vec_named(&props).expect("encode props"),
    }
}

/// Grow the graph to `target` nodes with concurrent waves, so seeding rides
/// group commit and costs a few commits rather than `target` of them.
async fn seed(backend: &Arc<RedbBackend>, from: usize, target: usize) {
    let mut next = from;
    while next < target {
        let wave_end = target.min(next + SEED_WAVE);
        let tasks: Vec<_> = (next..wave_end)
            .map(|id| {
                let backend = Arc::clone(backend);
                tokio::spawn(async move { backend.record_durable(GRAPH, &add_node(id)).await })
            })
            .collect();
        for task in tasks {
            task.await.expect("seed task").expect("seed write");
        }
        next = wave_end;
    }
}

/// `n` strictly sequential awaited writes: each is its own commit.
async fn measure(backend: &RedbBackend, from: usize, n: usize) -> Vec<f64> {
    let mut samples = Vec::with_capacity(n);
    for id in from..from + n {
        let method = add_node(id);
        let started = Instant::now();
        backend
            .record_durable(GRAPH, &method)
            .await
            .expect("durable write");
        samples.push(started.elapsed().as_secs_f64());
    }
    samples
}

fn print_trend(samples: &[f64]) {
    let tenth = (samples.len() / 10).max(1);
    let first = summarize(&samples[..tenth]);
    let last = summarize(&samples[samples.len() - tenth..]);
    println!(
        "  trend: first-10% p50={:.3}ms  last-10% p50={:.3}ms",
        ms(first.p50),
        ms(last.p50)
    );
}

struct EngineRun<'a> {
    backend: &'a Arc<RedbBackend>,
    phases: &'a PhaseSamples,
    n: usize,
}

async fn engine_scenario(run: &EngineRun<'_>, size: usize, written: usize) -> usize {
    seed(run.backend, written, size.max(written)).await;
    let start = size.max(written);
    let before = run.backend.commit_stats();
    let (commits, ops) = (before.commits(), before.ops());
    run.phases.take();
    let samples = measure(run.backend, start, run.n).await;
    let phases = run.phases.take();
    println!(
        "\nengine record_durable, graph size {start} -> {}",
        start + run.n
    );
    let e2e = print_summary("  end-to-end write", &samples);
    print_trend(&samples);
    println!(
        "  commits={} ops={}",
        before.commits() - commits,
        before.ops() - ops
    );
    print_phases(&phases, run.n, e2e.mean);
    start + run.n
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn sizes() -> Vec<usize> {
    std::env::var("EG_WRITE_LATENCY_SIZES")
        .unwrap_or_else(|_| "0,10000,50000".to_string())
        .split(',')
        .filter_map(|size| size.trim().parse().ok())
        .collect()
}

fn main() {
    let phases = PhaseSamples::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(phases.clone()))
        .expect("install phase collector");
    let n = env_usize("EG_WRITE_LATENCY_N", 400);
    let root = std::env::var("EG_WRITE_LATENCY_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let dir = root.join(format!("eg-write-latency-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("bench dir");
    println!("EH-290 write latency bench: dir={} n={n}", dir.display());

    raw_redb(&dir, Durability::Immediate, "immediate", n);
    raw_redb(&dir, Durability::None, "none", n);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let backend = Arc::new(
        RedbBackend::open(dir.join("engine").to_string_lossy().to_string(), 4096)
            .expect("open redb backend"),
    );
    let run = EngineRun {
        backend: &backend,
        phases: &phases,
        n,
    };
    let mut written = 0;
    for size in sizes() {
        written = runtime.block_on(engine_scenario(&run, size, written));
    }
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
