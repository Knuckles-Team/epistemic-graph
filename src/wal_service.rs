//! Off-reactor WAL writer with group-commit fsync (CONCEPT:KG-2.8, Phase B3).
//!
//! Previously each durable mutation appended to its graph's WAL with a blocking
//! `write_all` executed inline on the dispatch path — i.e. on a Tokio worker
//! thread. Under write load that stalls the async reactor. This service moves all
//! WAL file I/O onto ONE dedicated OS thread:
//!
//!   * the dispatch path serializes the method to bytes (cheap CPU, on-reactor)
//!     and `try_send`s them to the writer thread — never touching a file;
//!   * a single consumer thread owns every graph's [`WalWriter`], applies appends
//!     in receive order (preserving per-graph ordering), and `fsync`s on a
//!     group-commit cadence so thousands of appends cost a handful of fsyncs.
//!
//! Durability tiers (unchanged contract): an append is a `write` (page cache) so a
//! PROCESS crash keeps it; `fsync` is batched per [`FsyncPolicy`] so a hard POWER
//! loss is bounded by the group-commit interval (default) rather than the whole
//! checkpoint interval. `Off` keeps the previous behavior (fsync only at
//! checkpoint truncation); `Each` fsyncs after every drained batch.
//!
//! Backpressure: the channel is BOUNDED. The WAL is a best-effort crash-window
//! narrowing layer (the durable backend is the system-of-record), so a full
//! channel does NOT block the reactor and does NOT fail the already-applied
//! mutation — it drops the append and increments a LOUD counter + warns, so a
//! saturated writer is observable instead of a silent reactor stall.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::wal::{wal_path, WalWriter};

/// When the writer thread `fsync`s the files it has appended to.
#[derive(Debug, Clone, Copy)]
pub enum FsyncPolicy {
    /// Never fsync on the timer; durability comes only from the page cache
    /// (process-crash safe) plus the checkpoint truncation fsync. Matches the
    /// pre-Phase-B3 behavior.
    Off,
    /// Group-commit: fsync every dirty WAL once per interval. Bounds hard-power
    /// loss to the interval while amortizing fsync cost across many appends.
    Interval(Duration),
    /// fsync after every drained batch — strongest durability, lowest throughput.
    Each,
}

impl FsyncPolicy {
    /// Parse `EPISTEMIC_GRAPH_WAL_FSYNC`: `off` | `each` | `<ms>` | `interval`
    /// (default 100ms). Anything unrecognized falls back to the 100ms interval.
    pub fn from_env(raw: Option<&str>) -> Self {
        match raw.map(|s| s.trim().to_ascii_lowercase()) {
            Some(s) if s == "off" => FsyncPolicy::Off,
            Some(s) if s == "each" => FsyncPolicy::Each,
            Some(s) if s == "interval" || s.is_empty() => {
                FsyncPolicy::Interval(Duration::from_millis(100))
            }
            Some(s) => match s.parse::<u64>() {
                Ok(ms) if ms > 0 => FsyncPolicy::Interval(Duration::from_millis(ms)),
                _ => FsyncPolicy::Interval(Duration::from_millis(100)),
            },
            None => FsyncPolicy::Interval(Duration::from_millis(100)),
        }
    }

    fn tick(&self) -> Duration {
        match self {
            FsyncPolicy::Interval(d) => *d,
            // No timer fsync needed; still wake periodically so a Disconnected
            // channel is noticed promptly on shutdown.
            _ => Duration::from_millis(1000),
        }
    }
}

enum Cmd {
    Append {
        fname: String,
        bytes: Vec<u8>,
    },
    /// Current logical WAL length for a graph (0 if it has no WAL), used by the
    /// checkpoint to capture a truncation point. Processed in order, so it
    /// reflects exactly the appends enqueued before it.
    Position {
        fname: String,
        reply: Sender<u64>,
    },
    Truncate {
        fname: String,
        prefix: u64,
        reply: Sender<std::io::Result<()>>,
    },
    Shutdown {
        reply: Sender<()>,
    },
}

/// Handle to the background WAL writer thread. Cloneable via `Arc`.
pub struct WalService {
    tx: SyncSender<Cmd>,
    dropped: Arc<AtomicU64>,
    handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl WalService {
    /// Spawn the writer thread for `persist_dir` with the given fsync policy and
    /// channel capacity (bounded backpressure).
    pub fn spawn(persist_dir: String, policy: FsyncPolicy, capacity: usize) -> Arc<Self> {
        let (tx, rx) = sync_channel::<Cmd>(capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        let handle = std::thread::Builder::new()
            .name("eg-wal-writer".into())
            .spawn(move || run(rx, persist_dir, policy))
            .expect("spawn WAL writer thread");
        Arc::new(Self {
            tx,
            dropped,
            handle: parking_lot::Mutex::new(Some(handle)),
        })
    }

    /// Enqueue a pre-serialized append. Non-blocking: on a full channel the append
    /// is dropped (LOUD — counter + warn), never blocking the reactor. Safe because
    /// the mutation is already applied in memory and will be captured by the next
    /// checkpoint; the durable backend remains the system-of-record.
    pub fn append(&self, fname: String, bytes: Vec<u8>) {
        match self.tx.try_send(Cmd::Append { fname, bytes }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                crate::metrics::wal_append_dropped();
                tracing::warn!(
                    "WAL writer saturated: dropped append (total dropped={}); \
                     data is in memory and will checkpoint, but the crash-recovery \
                     window has widened — raise EPISTEMIC_GRAPH_WAL_QUEUE or check disk",
                    n
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                tracing::warn!("WAL writer thread is gone; append not logged");
            }
        }
    }

    /// Logical WAL length for `fname` after all prior appends are processed.
    pub fn position(&self, fname: &str) -> u64 {
        let (reply, rx) = std::sync::mpsc::channel();
        if self
            .tx
            .send(Cmd::Position {
                fname: fname.to_string(),
                reply,
            })
            .is_err()
        {
            return 0;
        }
        rx.recv().unwrap_or(0)
    }

    /// Drop the WAL prefix a durable snapshot now supersedes.
    pub fn truncate(&self, fname: &str, prefix: u64) -> std::io::Result<()> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::Truncate {
                fname: fname.to_string(),
                prefix,
                reply,
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "WAL writer gone"))?;
        rx.recv().unwrap_or_else(|_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL writer dropped truncate reply",
            ))
        })
    }

    /// Total appends dropped due to channel saturation (observability).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Flush and stop the writer thread, fsyncing all dirty WALs. Idempotent.
    pub fn shutdown(&self) {
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            let (reply, rx) = std::sync::mpsc::channel();
            if self.tx.send(Cmd::Shutdown { reply }).is_ok() {
                let _ = rx.recv();
            }
            let _ = handle.join();
        }
    }
}

fn run(rx: Receiver<Cmd>, dir: String, policy: FsyncPolicy) {
    let mut writers: HashMap<String, WalWriter> = HashMap::new();
    let mut dirty: HashSet<String> = HashSet::new();
    let tick = policy.tick();
    loop {
        match rx.recv_timeout(tick) {
            Ok(cmd) => {
                if handle_cmd(cmd, &dir, &mut writers, &mut dirty) {
                    break; // shutdown
                }
                // Drain whatever else is queued before considering an fsync, so a
                // burst coalesces into a single group commit.
                while let Ok(cmd) = rx.try_recv() {
                    if handle_cmd(cmd, &dir, &mut writers, &mut dirty) {
                        fsync_dirty(&mut writers, &mut dirty);
                        return;
                    }
                }
                if matches!(policy, FsyncPolicy::Each) {
                    fsync_dirty(&mut writers, &mut dirty);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if matches!(policy, FsyncPolicy::Interval(_)) {
                    fsync_dirty(&mut writers, &mut dirty);
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                fsync_dirty(&mut writers, &mut dirty);
                break;
            }
        }
    }
}

/// Returns true if the writer should stop (shutdown received).
fn handle_cmd(
    cmd: Cmd,
    dir: &str,
    writers: &mut HashMap<String, WalWriter>,
    dirty: &mut HashSet<String>,
) -> bool {
    match cmd {
        Cmd::Append { fname, bytes } => {
            if let Some(w) = ensure_writer(writers, dir, &fname, true) {
                if let Err(e) = w.append_bytes(&bytes) {
                    tracing::warn!("WAL append failed for '{}': {}", fname, e);
                } else {
                    dirty.insert(fname);
                }
            }
        }
        Cmd::Position { fname, reply } => {
            // Only open an existing WAL — don't create empty files for graphs that
            // never logged a durable mutation (e.g. decay-only dirty graphs).
            let pos = ensure_writer(writers, dir, &fname, false)
                .map(|w| w.position())
                .unwrap_or(0);
            let _ = reply.send(pos);
        }
        Cmd::Truncate {
            fname,
            prefix,
            reply,
        } => {
            let res = match ensure_writer(writers, dir, &fname, false) {
                Some(w) => {
                    let r = w.truncate_prefix(prefix);
                    dirty.remove(&fname); // truncate_prefix already fsync'd
                    r
                }
                None => Ok(()),
            };
            let _ = reply.send(res);
        }
        Cmd::Shutdown { reply } => {
            fsync_dirty(writers, dirty);
            let _ = reply.send(());
            return true;
        }
    }
    false
}

/// Get the writer for `fname`, opening it lazily. When `create` is false the
/// writer is only returned if the `.wal` file already exists on disk.
fn ensure_writer<'a>(
    writers: &'a mut HashMap<String, WalWriter>,
    dir: &str,
    fname: &str,
    create: bool,
) -> Option<&'a mut WalWriter> {
    if !writers.contains_key(fname) {
        let path = wal_path(dir, fname);
        if !create && !path.exists() {
            return None;
        }
        match WalWriter::open(&path) {
            Ok(w) => {
                writers.insert(fname.to_string(), w);
            }
            Err(e) => {
                tracing::warn!("WAL open failed for '{}': {}", fname, e);
                return None;
            }
        }
    }
    writers.get_mut(fname)
}

fn fsync_dirty(writers: &mut HashMap<String, WalWriter>, dirty: &mut HashSet<String>) {
    for fname in dirty.drain() {
        if let Some(w) = writers.get_mut(&fname) {
            if let Err(e) = w.sync() {
                tracing::warn!("WAL fsync failed for '{}': {}", fname, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Method;

    fn append_bytes(m: &Method) -> Vec<u8> {
        rmp_serde::to_vec_named(m).unwrap()
    }

    #[test]
    fn append_truncate_position_roundtrip() {
        let dir = std::env::temp_dir().join(format!("eg-walsvc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let svc = WalService::spawn(dir.to_str().unwrap().to_string(), FsyncPolicy::Each, 16);
        for i in 0..3 {
            svc.append(
                "g".into(),
                append_bytes(&Method::AddNode {
                    node_id: format!("n{i}"),
                    properties_msgpack: append_bytes(&Method::ClearGraph),
                }),
            );
        }
        let pos = svc.position("g");
        assert!(pos > 0, "position should reflect the 3 appends");
        // Truncate the whole prefix → empty WAL.
        svc.truncate("g", pos).unwrap();
        assert_eq!(svc.position("g"), 0, "WAL empty after full truncation");
        assert_eq!(svc.dropped(), 0);
        svc.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn position_zero_for_unlogged_graph() {
        let dir = std::env::temp_dir().join(format!("eg-walsvc-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let svc = WalService::spawn(dir.to_str().unwrap().to_string(), FsyncPolicy::Off, 16);
        // No append → no file created, position 0, truncate is a no-op.
        assert_eq!(svc.position("never"), 0);
        svc.truncate("never", 0).unwrap();
        assert!(!wal_path(dir.to_str().unwrap(), "never").exists());
        svc.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
