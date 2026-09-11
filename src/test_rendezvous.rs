//! Bounded rendezvous and joins for concurrency tests.
//!
//! A concurrency test pairs threads at a meeting point: one parks until the
//! other reaches a hook, so the test can observe an interleaving that is
//! otherwise unobservable. `std::sync::Barrier` is the natural primitive and it
//! has no timeout, so when the other party never arrives -- it returned early
//! down an error path, it panicked, it never reached its hook because a
//! refactor moved the hook's trigger -- the waiting thread blocks for the life
//! of the process.
//!
//! That failure mode is strictly worse than the bug it was written to catch. A
//! test that FAILS reports one name; a test that HANGS takes the whole binary
//! with it, and `cargo test` cannot distinguish a wedged binary from a slow
//! one. This repository learned that the expensive way: a single unbounded
//! rendezvous in the shard graft tests silently truncated every full workspace
//! test run, so runs were read as complete when they had stopped early.
//!
//! So: meet, or fail by name. Every helper here is bounded, and every timeout
//! says which rendezvous was missed rather than reporting a bare deadline.

#![cfg(test)]

use std::sync::mpsc::{sync_channel, RecvTimeoutError};
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::time::Duration;

/// How long a rendezvous may wait before it is a failure rather than a wait.
///
/// Generous on purpose: these tests drive real redb transactions on shared
/// build hosts under parallel load, so the bound exists to catch a party that
/// is never coming, not to police latency.
pub(crate) const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(120);

/// Meet the other party at `barrier`, or panic naming `what`.
///
/// The wait happens on a helper thread so the deadline is enforceable --
/// `Barrier::wait` itself cannot be interrupted. If the other party arrives
/// later, that helper simply completes and exits.
pub(crate) fn meet(barrier: &Arc<Barrier>, what: &str) {
    let other = Arc::clone(barrier);
    let (arrived, waiting) = sync_channel(1);
    std::thread::spawn(move || {
        other.wait();
        let _ = arrived.send(());
    });
    match waiting.recv_timeout(RENDEZVOUS_TIMEOUT) {
        Ok(()) => {}
        Err(RecvTimeoutError::Timeout) => panic!(
            "{what}: the other party never reached this rendezvous within {}s -- it \
             returned early, panicked, or never reached its hook",
            RENDEZVOUS_TIMEOUT.as_secs()
        ),
        Err(RecvTimeoutError::Disconnected) => {
            panic!("{what}: the waiting thread died before the rendezvous")
        }
    }
}

/// Join `handle`, or panic naming `what` rather than blocking forever.
///
/// `JoinHandle::join` is unbounded for the same reason `Barrier::wait` is, and
/// a worker that deadlocked upstream takes its joiner down with it.
pub(crate) fn join_bounded<T: Send + 'static>(handle: JoinHandle<T>, what: &str) -> T {
    let (finished, waiting) = sync_channel(1);
    let joiner = std::thread::spawn(move || {
        let value = handle.join();
        let _ = finished.send(());
        value
    });
    match waiting.recv_timeout(RENDEZVOUS_TIMEOUT) {
        Ok(()) => match joiner.join() {
            Ok(Ok(value)) => value,
            Ok(Err(payload)) => std::panic::resume_unwind(payload),
            Err(payload) => std::panic::resume_unwind(payload),
        },
        Err(_) => panic!(
            "{what}: the worker did not finish within {}s",
            RENDEZVOUS_TIMEOUT.as_secs()
        ),
    }
}
