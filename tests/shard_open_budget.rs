//! EG-DURABLE-KERNEL-R006.2: startup open has a time budget that holds on a
//! loaded host. A fresh K=8 store and a reopened small K=8 store must each open
//! within budget while fsync-writer and CPU-burn threads run alongside.
//!
//! The directory defaults to the system temp dir; set `EG_BUDGET_DIR` to a
//! real-disk path (tmpfs fsyncs are free and would hide the cost under test).
#![cfg(feature = "redb")]

use epistemic_graph::protocol::Method;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SHARDS: usize = 8;
const FSYNC_WRITERS: usize = 16;
const CPU_BURNERS: usize = 32;
/// Fresh K=8 open budget, wall clock, under the load above.
const FRESH_BUDGET: Duration = Duration::from_secs(5);
/// Reopen of a small K=8 store, wall clock, under the load above.
const REOPEN_BUDGET: Duration = Duration::from_secs(2);

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

    fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads {
            thread.join().expect("load thread");
        }
    }
}

// spec: EG-DURABLE-KERNEL-R006.2
#[test]
fn shard_open_stays_within_budget_under_load() {
    let base = std::env::var("EG_BUDGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    std::fs::create_dir_all(&base).unwrap();
    let scratch = tempfile::Builder::new()
        .prefix("shard-open-budget-")
        .tempdir_in(&base)
        .unwrap();
    let dir = scratch.path().join("store").to_string_lossy().to_string();
    std::env::set_var("EPISTEMIC_GRAPH_REDB_SHARDS", SHARDS.to_string());

    let load = Load::start(scratch.path());

    let started = std::time::Instant::now();
    let backend = RedbBackend::open(dir.clone(), 8192).expect("fresh open");
    let fresh = started.elapsed();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for graph in 0..16 {
            for node in 0..20 {
                backend
                    .record_durable(
                        &format!("g{graph}"),
                        &Method::AddNode {
                            node_id: format!("n{node}"),
                            properties_msgpack: rmp_serde::to_vec_named(
                                &serde_json::json!({"k": node}),
                            )
                            .unwrap(),
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    });
    backend.shutdown();
    drop(backend);

    let started = std::time::Instant::now();
    let backend = RedbBackend::open(dir, 8192).expect("reopen");
    let reopen = started.elapsed();
    backend.shutdown();
    drop(backend);
    load.stop();

    println!(
        "[BUDGET] fresh={fresh:?} (<= {FRESH_BUDGET:?}) reopen={reopen:?} (<= {REOPEN_BUDGET:?})"
    );
    assert!(
        fresh <= FRESH_BUDGET,
        "fresh K={SHARDS} open took {fresh:?}, budget {FRESH_BUDGET:?}"
    );
    assert!(
        reopen <= REOPEN_BUDGET,
        "reopen K={SHARDS} took {reopen:?}, budget {REOPEN_BUDGET:?}"
    );
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
        assert!(
            !bad.join(name).exists(),
            "refused open must not leave {name}"
        );
    }
}

/// R006.3: reopening a populated store (~160 MB) takes the clean-shutdown
/// marker fast path and stays inside the reopen budget under load; with the
/// markers removed the full scan still runs and the store still opens.
// spec: EG-DURABLE-KERNEL-R006.3
// spec: EG-DURABLE-KERNEL-R006.2
#[test]
fn populated_reopen_hits_the_marker_fast_path() {
    let base = std::env::var("EG_BUDGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    std::fs::create_dir_all(&base).unwrap();
    let scratch = tempfile::Builder::new()
        .prefix("shard-open-populated-")
        .tempdir_in(&base)
        .unwrap();
    let dir = scratch.path().join("store");
    let dir_str = dir.to_string_lossy().to_string();

    let backend = RedbBackend::open_with_shards(dir_str.clone(), 8192, SHARDS).expect("open");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let payload = "x".repeat(100_000);
    runtime.block_on(async {
        for graph in 0..32 {
            for node in 0..50 {
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
    let started = std::time::Instant::now();
    let backend = RedbBackend::open_with_shards(dir_str.clone(), 8192, SHARDS).expect("reopen");
    let fast = started.elapsed();
    backend.shutdown();
    drop(backend);

    // Forced fallback: no marker means the full recovery scan runs.
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        if entry.file_name().to_string_lossy().ends_with(".redb.clean") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    let started = std::time::Instant::now();
    let backend = RedbBackend::open_with_shards(dir_str, 8192, SHARDS).expect("full-scan reopen");
    let full = started.elapsed();
    backend.shutdown();
    drop(backend);
    load.stop();

    println!("[BUDGET] populated bytes={bytes} marker_reopen={fast:?} (<= {REOPEN_BUDGET:?}) full_scan_reopen={full:?}");
    assert!(
        fast <= REOPEN_BUDGET,
        "marker reopen took {fast:?}, budget {REOPEN_BUDGET:?}"
    );
}
