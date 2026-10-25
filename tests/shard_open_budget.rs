//! EG-DURABLE-KERNEL-R006.2: startup open has a time budget that holds on a
//! loaded host. One scenario, two checks:
//!
//! * The REGRESSION GATE (the default tests): one process, run serially, with
//!   limits about twice the measured single-process R820 numbers, while 16
//!   fsync-writer and 32 CPU-burn threads run alongside.
//! * The STRESS CHECK (`#[ignore]`d, run explicitly with `--ignored`): ten
//!   copies of the same scenario in parallel child processes. Each copy starts
//!   its own load threads, so the host carries ten times that load, and the
//!   limits are loose. It is not a regression gate.
//!
//! A tracing subscriber captures the per-store open phase lines (file open,
//! table validation, recovery scan or skip, bind, ledger). A missed limit
//! prints them, so a slow open is diagnosable from the test output.
//!
//! The directory defaults to the system temp dir; set `EG_BUDGET_DIR` to a
//! real-disk path (tmpfs fsyncs are free and would hide the cost under test).
#![cfg(feature = "redb")]

#[path = "../src/test_rendezvous.rs"]
#[allow(dead_code)]
mod test_rendezvous;

use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const SHARDS: usize = 8;
const FSYNC_WRITERS: usize = 16;
const CPU_BURNERS: usize = 32;
const STRESS_COPIES: usize = 10;
const STRESS_CHILD: &str = "EG_SHARD_OPEN_STRESS_CHILD";

// Each scenario owns the process-wide tracing buffer and its background load.
static SCENARIO_LOCK: Mutex<()> = Mutex::new(());

/// Wall-clock limits for one run of the scenario.
struct Limits {
    label: &'static str,
    fresh: Duration,
    small_reopen: Duration,
    populated_marker_reopen: Duration,
}

/// Regression gate: about 2x the single-process R820 numbers under the load
/// above (fresh 1.55-1.82 s, small reopen 0.61-0.83 s, populated marker
/// reopen 1.00-1.12 s). Measurements: plans/eg-shard-open-latency-2026-10-23.md.
const GATE: Limits = Limits {
    label: "gate",
    fresh: Duration::from_secs(4),
    small_reopen: Duration::from_secs(2),
    populated_marker_reopen: Duration::from_millis(2500),
};

/// Stress check: ten parallel copies. Worst R820 cases were fresh 37.8 s and
/// reopen 24.1 s.
const STRESS: Limits = Limits {
    label: "stress",
    fresh: Duration::from_secs(60),
    small_reopen: Duration::from_secs(45),
    populated_marker_reopen: Duration::from_secs(45),
};

struct Load {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Load {
    fn start(base: &std::path::Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        for _ in 0..CPU_BURNERS {
            let stop = Arc::clone(&stop);
            threads.push(std::thread::spawn(move || {
                let mut x = 1u64;
                while !stop.load(Ordering::Relaxed) {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                    std::hint::black_box(x);
                }
            }));
        }
        for i in 0..FSYNC_WRITERS {
            let stop = Arc::clone(&stop);
            let file = base.join(format!("fsync-writer-{i}"));
            threads.push(std::thread::spawn(move || {
                use std::io::Write;
                let mut handle = std::fs::File::create(&file).expect("fsync writer file");
                while !stop.load(Ordering::Relaxed) {
                    handle.write_all(&[7u8; 4096]).expect("fsync writer write");
                    handle.sync_data().expect("fsync writer sync");
                }
                let _ = std::fs::remove_file(&file);
            }));
        }
        Self { stop, threads }
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            test_rendezvous::join_bounded(thread, "shard open budget load thread");
        }
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        // An open or assertion can panic before stop; end the load on that path.
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Captured tracing output. The subscriber is process-global because the
/// shards open on their own threads.
#[derive(Clone, Default)]
struct PhaseLog(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for PhaseLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl PhaseLog {
    fn get() -> &'static PhaseLog {
        static LOG: OnceLock<PhaseLog> = OnceLock::new();
        LOG.get_or_init(|| {
            let log = PhaseLog::default();
            let writer = log.clone();
            let _ = tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
                .try_init();
            log
        })
    }

    /// Drain everything captured so far and keep only the store-open lines.
    fn take(&self) -> String {
        let mut bytes = self.0.lock().unwrap();
        let text = String::from_utf8_lossy(&bytes).into_owned();
        bytes.clear();
        let lines: Vec<&str> = text
            .lines()
            .filter(|line| line.contains("redb:") && line.contains("open"))
            .collect();
        if lines.is_empty() {
            "(no store-open phase lines captured)".to_string()
        } else {
            lines.join("\n")
        }
    }
}

/// Count the captured per-store lines that ran the full recovery scan.
fn scanned(phases: &str) -> usize {
    phases
        .lines()
        .filter(|line| line.contains("recovery_scan_skipped=false"))
        .count()
}

fn check(failures: &mut Vec<String>, what: &str, took: Duration, limit: Duration, phases: &str) {
    if took > limit {
        failures.push(format!(
            "{what} took {took:?}, limit {limit:?}\n--- per-store open phases ---\n{phases}"
        ));
    }
}

fn finish(limits: &Limits, failures: Vec<String>) {
    if !failures.is_empty() {
        for failure in &failures {
            println!("[{} MISS] {failure}", limits.label);
        }
        panic!(
            "{} of the {} limits missed:\n{}",
            failures.len(),
            limits.label,
            failures.join("\n\n")
        );
    }
}

fn scratch(prefix: &str) -> tempfile::TempDir {
    let base = std::env::var("EG_BUDGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    std::fs::create_dir_all(&base).unwrap();
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(&base)
        .unwrap()
}

fn record(backend: &RedbBackend, graphs: usize, nodes: usize, payload: &str) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for graph in 0..graphs {
            for node in 0..nodes {
                backend
                    .record_durable(
                        &format!("g{graph}"),
                        &Method::AddNode {
                            node_id: format!("n{node}"),
                            properties_msgpack: rmp_serde::to_vec_named(
                                &serde_json::json!({"k": node, "p": payload}),
                            )
                            .unwrap(),
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    });
}

/// Fresh K=8 open, then a reopen of the small store, both under load.
fn small_scenario(limits: &Limits) -> Vec<String> {
    let _scenario = SCENARIO_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let phase_log = PhaseLog::get();
    let scratch = scratch("shard-open-budget-");
    let dir = scratch.path().join("store").to_string_lossy().to_string();
    std::env::set_var("EPISTEMIC_GRAPH_REDB_SHARDS", SHARDS.to_string());

    let load = Load::start(scratch.path());

    phase_log.take();
    let started = std::time::Instant::now();
    let backend = RedbBackend::open(dir.clone(), 8192).expect("fresh open");
    let fresh = started.elapsed();
    let fresh_phases = phase_log.take();

    record(&backend, 16, 20, "");
    backend.shutdown();
    drop(backend);

    phase_log.take();
    let started = std::time::Instant::now();
    let backend = RedbBackend::open(dir, 8192).expect("reopen");
    let reopen = started.elapsed();
    let reopen_phases = phase_log.take();
    backend.shutdown();
    drop(backend);
    load.stop();

    println!(
        "[BUDGET {}] fresh={fresh:?} (<= {:?}) reopen={reopen:?} (<= {:?}) reopen_scanned_stores={}",
        limits.label,
        limits.fresh,
        limits.small_reopen,
        scanned(&reopen_phases)
    );
    let mut failures = Vec::new();
    check(
        &mut failures,
        &format!("fresh K={SHARDS} open"),
        fresh,
        limits.fresh,
        &fresh_phases,
    );
    check(
        &mut failures,
        &format!("small reopen K={SHARDS}"),
        reopen,
        limits.small_reopen,
        &reopen_phases,
    );
    failures
}

/// Reopen of a populated (~300 MB) K=8 store under load, first on the
/// clean-shutdown markers, then with the markers removed (full scan, no limit:
/// it proves the fallback still opens the store).
fn populated_scenario(limits: &Limits) -> Vec<String> {
    let _scenario = SCENARIO_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let phase_log = PhaseLog::get();
    let scratch = scratch("shard-open-populated-");
    let dir = scratch.path().join("store");
    let dir_str = dir.to_string_lossy().to_string();

    let backend = RedbBackend::open_with_shards(dir_str.clone(), 8192, SHARDS).expect("open");
    record(&backend, 32, 50, &"x".repeat(100_000));
    backend.shutdown();
    drop(backend);

    let bytes: u64 = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".redb"))
        .map(|e| e.metadata().unwrap().len())
        .sum();
    assert!(
        bytes > 100 * 1024 * 1024,
        "populated store is only {bytes} bytes"
    );

    let load = Load::start(scratch.path());
    phase_log.take();
    let started = std::time::Instant::now();
    let backend = RedbBackend::open_with_shards(dir_str.clone(), 8192, SHARDS).expect("reopen");
    let fast = started.elapsed();
    let fast_phases = phase_log.take();
    backend.shutdown();
    drop(backend);

    // Forced fallback: no marker means the full recovery scan runs.
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        if entry.file_name().to_string_lossy().ends_with(".redb.clean") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    phase_log.take();
    let started = std::time::Instant::now();
    let backend = RedbBackend::open_with_shards(dir_str, 8192, SHARDS).expect("full-scan reopen");
    let full = started.elapsed();
    let full_phases = phase_log.take();
    backend.shutdown();
    drop(backend);
    load.stop();

    println!(
        "[BUDGET {}] populated bytes={bytes} marker_reopen={fast:?} (<= {:?}) marker_scanned_stores={} full_scan_reopen={full:?} full_scanned_stores={}",
        limits.label,
        limits.populated_marker_reopen,
        scanned(&fast_phases),
        scanned(&full_phases)
    );
    let mut failures = Vec::new();
    check(
        &mut failures,
        "populated marker reopen",
        fast,
        limits.populated_marker_reopen,
        &fast_phases,
    );
    failures
}

// spec: EG-DURABLE-KERNEL-R006.2
#[test]
fn shard_open_stays_within_budget_under_load() {
    finish(&GATE, small_scenario(&GATE));
}

/// R006.3: reopening a populated store (~300 MB) takes the clean-shutdown
/// marker fast path and stays inside the gate under load; with the markers
/// removed the full scan still runs and the store still opens.
// spec: EG-DURABLE-KERNEL-R006.3
// spec: EG-DURABLE-KERNEL-R006.2
#[test]
fn populated_reopen_hits_the_marker_fast_path() {
    finish(&GATE, populated_scenario(&GATE));
}

/// R006.1: the sidecar stores open with the shards, and a refused open leaves
/// the persist dir without any sidecar file that attempt created.
// spec: EG-DURABLE-KERNEL-R006.1
#[test]
fn sidecars_open_with_the_shards_and_a_refusal_leaves_none() {
    let scratch = tempfile::tempdir().unwrap();
    let sidecars = [
        "admin-mutations.redb",
        "node_info.redb",
        "cluster_hierarchy.redb",
    ];

    let good = scratch.path().join("good");
    let backend =
        RedbBackend::open_with_shards(good.to_string_lossy().to_string(), 64, 2).expect("open");
    for name in sidecars {
        assert!(good.join(name).exists(), "{name} must exist after open");
    }
    backend.shutdown();
    drop(backend);

    let bad = scratch.path().join("bad");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("graph-0.redb"), b"not a redb file").unwrap();
    assert!(RedbBackend::open_with_shards(bad.to_string_lossy().to_string(), 64, 1).is_err());
    for name in sidecars {
        for artifact in [name.to_string(), format!("{name}.clean")] {
            assert!(
                !bad.join(&artifact).exists(),
                "refused open must not leave {artifact}"
            );
        }
    }
}

/// A sidecar failure after the admin store opened must clean that store and
/// its close marker, while preserving every pre-existing sidecar artifact.
// spec: EG-DURABLE-KERNEL-R006.1
#[test]
fn sidecar_failure_cleans_new_artifacts_and_preserves_existing_markers() {
    let scratch = tempfile::tempdir().unwrap();
    let bad = scratch.path();
    let existing_marker = bad.join("admin-mutations.redb.clean");
    std::fs::write(&existing_marker, b"pre-existing marker").unwrap();
    let invalid = bad.join("node_info.redb");
    std::fs::write(&invalid, b"not a redb file").unwrap();
    assert!(RedbBackend::open_with_shards(bad.to_string_lossy().to_string(), 64, 1).is_err());
    assert!(!bad.join("admin-mutations.redb").exists());
    assert!(existing_marker.is_file());
    assert_eq!(std::fs::read(invalid).unwrap(), b"not a redb file");
    assert!(!bad.join("cluster_hierarchy.redb").exists());
    assert!(!bad.join("cluster_hierarchy.redb.clean").exists());
}

/// A competing backend startup must refuse before its cleanup snapshot can
/// mistake another opener's newly created sidecar for its own.
// spec: EG-DURABLE-KERNEL-R006.1
#[test]
fn competing_backend_startup_preserves_the_other_openers_files() {
    let scratch = tempfile::tempdir().unwrap();
    let startup_lock = std::fs::File::open(scratch.path()).unwrap();
    startup_lock.try_lock().unwrap();
    let other_sidecar = scratch.path().join("admin-mutations.redb");
    std::fs::write(&other_sidecar, b"another opener owns this").unwrap();
    let refused =
        RedbBackend::open_with_shards(scratch.path().to_string_lossy().to_string(), 64, 1);
    assert!(refused
        .err()
        .unwrap()
        .contains("startup directory is locked"));
    assert_eq!(
        std::fs::read(&other_sidecar).unwrap(),
        b"another opener owns this"
    );
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 1);
}

// spec: EG-DURABLE-KERNEL-R006.1
#[test]
fn backend_startup_coexists_with_the_engine_lifetime_lock() {
    let scratch = tempfile::tempdir().unwrap();
    let directory = scratch.path().to_string_lossy().to_string();
    let lifetime_lock = eg_core::persist_lock::acquire(&directory).unwrap();
    let backend = RedbBackend::open_with_shards(directory, 64, 1).unwrap();
    backend.shutdown();
    drop(backend);
    drop(lifetime_lock);
}

/// Child half of the stress check: runs only when the parent sets
/// `EG_SHARD_OPEN_STRESS_CHILD`, so the default run treats it as a no-op.
#[test]
fn shard_open_stress_child() {
    if std::env::var_os(STRESS_CHILD).is_none() {
        return;
    }
    let mut failures = small_scenario(&STRESS);
    failures.extend(populated_scenario(&STRESS));
    finish(&STRESS, failures);
}

/// STRESS CHECK, not the regression gate: ten copies of the scenario run in
/// parallel child processes, each with its own load threads, under the loose
/// stress limits.
// spec: EG-DURABLE-KERNEL-R006.2
#[test]
#[ignore = "stress check, not the regression gate: ten parallel copies, about 1 h on R820; run with --ignored"]
fn shard_open_stress_ten_parallel_copies() {
    let exe = std::env::current_exe().unwrap();
    let logs = tempfile::tempdir().unwrap();
    let children: Vec<_> = (0..STRESS_COPIES)
        .map(|copy| {
            let log = logs.path().join(format!("copy-{copy}.log"));
            let file = std::fs::File::create(&log).unwrap();
            let child = std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "shard_open_stress_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(STRESS_CHILD, "1")
                .stdout(file.try_clone().unwrap())
                .stderr(file)
                .spawn()
                .unwrap();
            (copy, log, child)
        })
        .collect();
    let mut failed = Vec::new();
    for (copy, log, mut child) in children {
        let status = child.wait().unwrap();
        let text = std::fs::read_to_string(&log).unwrap_or_default();
        for line in text.lines().filter(|line| line.contains("[BUDGET")) {
            println!("copy {copy}: {line}");
        }
        if !status.success() {
            println!("--- stress copy {copy} missed a limit ---\n{text}");
            failed.push(copy);
        }
    }
    assert!(
        failed.is_empty(),
        "stress copies {failed:?} of {STRESS_COPIES} missed the stress limits"
    );
}
