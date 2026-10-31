//! Bounded thread fan-out and start gates for medulla-core's concurrency tests.
//!
//! A concurrency test that spawns workers and then waits for them has the same
//! hazard `clippy.toml` bans `JoinHandle::join` and `Barrier::wait` for: a worker
//! that deadlocks in the code under test takes the waiting test with it, and
//! `cargo test` cannot tell a wedged binary from a slow one. The tests using
//! this module exist precisely to catch deadlocks, so the wait must be the thing
//! that fails.
//!
//! The root crate's `test_rendezvous` cannot be reached from here (medulla-core sits
//! below it in the crate DAG), and this module does not wrap a join at all:
//! every worker reports its own outcome over a bounded channel, and the
//! collector waits with `recv_timeout`.
//!
//! The hang guard measures progress, not elapsed time. A fixed wall-clock
//! deadline cannot tell a wedged fan-out from one that is merely starved on a
//! loaded build host, so it fails slow runs as deadlocks. Here a wait only
//! fails once a whole [`WEDGE_WINDOW`] passes in which the test process
//! consumed no CPU at all: deadlocked threads are parked and burn nothing,
//! while a slow-but-live run (including a rayon pool working for a blocked
//! caller) keeps accruing CPU however contended the host is.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Condvar, Mutex};
use std::thread::Result as ThreadResult;
use std::time::Duration;

/// One observation window of the hang guard. A wait fails only after a full
/// window in which the process made no CPU progress.
pub(crate) const WEDGE_WINDOW: Duration = Duration::from_secs(60);

/// Process-wide CPU time consumed so far (user + system, all threads), or
/// `None` where the platform does not expose it.
#[cfg(target_os = "linux")]
fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the parenthesised command name start at field 3 (`state`);
    // `utime` and `stime` are fields 14 and 15.
    let mut fields = stat
        .get(stat.rfind(')')? + 1..)?
        .split_whitespace()
        .skip(11);
    let utime: u64 = fields.next()?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some(utime + stime)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ticks() -> Option<u64> {
    None
}

/// Tracks whether the process made progress during the current window.
struct WedgeGuard {
    cpu_at_window_start: Option<u64>,
}

impl WedgeGuard {
    fn start() -> Self {
        Self {
            cpu_at_window_start: process_cpu_ticks(),
        }
    }

    /// Called when a window elapsed without the awaited event. Returns `true`
    /// when the process was idle for that whole window (wedged), otherwise
    /// opens a new window and returns `false`. Without a CPU clock, an elapsed
    /// window counts as wedged.
    fn window_was_idle(&mut self) -> bool {
        let now = process_cpu_ticks();
        let idle = match (self.cpu_at_window_start, now) {
            (Some(before), Some(after)) => after <= before,
            _ => true,
        };
        self.cpu_at_window_start = now;
        idle
    }
}

type Outcome<T> = (usize, ThreadResult<T>);

/// A set of worker threads whose results are collected within one deadline.
pub(crate) struct BoundedThreads<T> {
    what: &'static str,
    finished: SyncSender<Outcome<T>>,
    outcomes: Receiver<Outcome<T>>,
    spawned: usize,
}

impl<T: Send + 'static> BoundedThreads<T> {
    /// `capacity` is the number of workers expected. Senders never block below it.
    pub(crate) fn new(what: &'static str, capacity: usize) -> Self {
        let (finished, outcomes) = sync_channel(capacity);
        Self {
            what,
            finished,
            outcomes,
            spawned: 0,
        }
    }

    /// Run `work` on a new thread. Its return value or its panic is reported.
    pub(crate) fn spawn(&mut self, work: impl FnOnce() -> T + Send + 'static) {
        let index = self.spawned;
        self.spawned += 1;
        let finished = self.finished.clone();
        std::thread::spawn(move || {
            let outcome = catch_unwind(AssertUnwindSafe(work));
            let _ = finished.send((index, outcome));
        });
    }

    /// Every worker's result in spawn order. Re-raises a worker's panic, and
    /// panics naming the fan-out if any worker misses the deadline.
    pub(crate) fn finish(self) -> Vec<T> {
        let Self {
            what,
            finished,
            outcomes,
            spawned,
        } = self;
        drop(finished);
        let mut guard = WedgeGuard::start();
        let mut results: Vec<Option<T>> = (0..spawned).map(|_| None).collect();
        let mut reported = 0;
        while reported < spawned {
            match outcomes.recv_timeout(WEDGE_WINDOW) {
                Ok((index, Ok(value))) => {
                    results[index] = Some(value);
                    reported += 1;
                }
                Ok((_, Err(payload))) => resume_unwind(payload),
                Err(RecvTimeoutError::Timeout) => assert!(
                    !guard.window_was_idle(),
                    "{what}: no worker finished and the process used no CPU for {}s -- it is deadlocked or wedged",
                    WEDGE_WINDOW.as_secs()
                ),
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("{what}: a worker exited without reporting its outcome")
                }
            }
        }
        results
            .into_iter()
            .map(|result| result.expect("every worker index reports exactly once"))
            .collect()
    }
}

/// A start line that many threads wait at, released at once, with a deadline.
#[derive(Default)]
pub(crate) struct StartGate {
    opened: Mutex<bool>,
    changed: Condvar,
}

impl StartGate {
    /// Release every thread waiting at the gate.
    pub(crate) fn open(&self) {
        *self.opened.lock().expect("start gate lock") = true;
        self.changed.notify_all();
    }

    /// Wait until the gate opens, or panic naming `what`.
    pub(crate) fn wait(&self, what: &str) {
        let mut guard = WedgeGuard::start();
        let mut opened = self.opened.lock().expect("start gate lock");
        while !*opened {
            let (next, timeout) = self
                .changed
                .wait_timeout_while(opened, WEDGE_WINDOW, |opened| !*opened)
                .expect("start gate lock");
            opened = next;
            assert!(
                *opened || !timeout.timed_out() || !guard.window_was_idle(),
                "{what}: the start gate was never opened and the process used no CPU for {}s",
                WEDGE_WINDOW.as_secs()
            );
        }
    }
}
