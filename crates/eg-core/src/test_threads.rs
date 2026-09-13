//! Bounded thread fan-out and start gates for eg-core's concurrency tests.
//!
//! A concurrency test that spawns workers and then waits for them has the same
//! hazard `clippy.toml` bans `JoinHandle::join` and `Barrier::wait` for: a worker
//! that deadlocks in the code under test takes the waiting test with it, and
//! `cargo test` cannot tell a wedged binary from a slow one. The tests using
//! this module exist precisely to catch deadlocks, so the wait must be the thing
//! that fails.
//!
//! The root crate's `test_rendezvous` cannot be reached from here (eg-core sits
//! below it in the crate DAG), and this module does not wrap a join at all:
//! every worker reports its own outcome over a bounded channel, and the
//! collector's deadline is a `recv_timeout`.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Condvar, Mutex};
use std::thread::Result as ThreadResult;
use std::time::{Duration, Instant};

/// How long a fan-out may run before it counts as wedged. This is a generous
/// wedge detector for shared, loaded build hosts, not a latency budget.
pub(crate) const THREAD_TEST_TIMEOUT: Duration = Duration::from_secs(120);

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
        let deadline = Instant::now() + THREAD_TEST_TIMEOUT;
        let mut results: Vec<Option<T>> = (0..spawned).map(|_| None).collect();
        for _ in 0..spawned {
            match outcomes.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok((index, Ok(value))) => results[index] = Some(value),
                Ok((_, Err(payload))) => resume_unwind(payload),
                Err(RecvTimeoutError::Timeout) => panic!(
                    "{what}: a worker did not finish within {}s -- it is deadlocked or wedged",
                    THREAD_TEST_TIMEOUT.as_secs()
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
        let opened = self.opened.lock().expect("start gate lock");
        let (opened, _) = self
            .changed
            .wait_timeout_while(opened, THREAD_TEST_TIMEOUT, |opened| !*opened)
            .expect("start gate lock");
        assert!(
            *opened,
            "{what}: the start gate was never opened within {}s",
            THREAD_TEST_TIMEOUT.as_secs()
        );
    }
}
