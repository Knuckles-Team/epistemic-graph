//! A per-process cap on concurrently running raft cluster tests (EH-534, from the
//! EH-287 evidence).
//!
//! `cargo test --lib raft::` runs every cluster test of the binary as a thread of
//! ONE process, up to one per core: ~159 three-node clusters at once, each with its
//! own election timers and heartbeats. The suite starved itself -- heartbeats
//! overran their 1 s timeout and followers elected -- while every test passed run
//! alone (10/10 unthrottled). Each cluster test now claims a slot when it builds
//! its first node; the slot is held by the test's thread and released when the
//! test (its thread) ends. Under nextest (one process per test) a slot is always
//! free, so this only shapes the in-process runner.

use std::cell::RefCell;
use std::sync::{Condvar, Mutex, OnceLock};

use crate::lock_recovery::LockRecovery;

/// Concurrent cluster tests per process: a quarter of the cores' worth of
/// three-node clusters, never fewer than two.
fn capacity() -> usize {
    let cores = std::thread::available_parallelism().map_or(4, usize::from);
    (cores / 8).max(2)
}

struct Slots {
    used: Mutex<usize>,
    freed: Condvar,
    capacity: usize,
}

fn slots() -> &'static Slots {
    static SLOTS: OnceLock<Slots> = OnceLock::new();
    SLOTS.get_or_init(|| Slots {
        used: Mutex::new(0),
        freed: Condvar::new(),
        capacity: capacity(),
    })
}

/// One claimed slot; dropping it (the test thread ending) frees it.
struct Held;

impl Drop for Held {
    fn drop(&mut self) {
        let slots = slots();
        let mut used = slots.used.lock_recovering("raft harness cluster slots");
        *used = used.saturating_sub(1);
        slots.freed.notify_one();
    }
}

thread_local! {
    static HELD: RefCell<Option<Held>> = const { RefCell::new(None) };
}

/// Claim a cluster slot for the calling test thread, waiting for one to free.
/// Idempotent per thread: a test building several nodes holds one slot.
pub(crate) fn claim() {
    if HELD.with(|held| held.borrow().is_some()) {
        return;
    }
    let slots = slots();
    let mut used = slots.used.lock_recovering("raft harness cluster slots");
    while *used >= slots.capacity {
        used = slots
            .freed
            .wait(used)
            .unwrap_or_else(|_| panic!("raft harness cluster slots: a holder panicked mid-count"));
    }
    *used += 1;
    drop(used);
    HELD.with(|held| *held.borrow_mut() = Some(Held));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thread_holds_one_slot_until_it_ends() {
        let used = || *slots().used.lock_recovering("raft harness cluster slots");
        let inner = std::thread::spawn(move || {
            claim();
            let after_first = used();
            claim();
            (after_first, used())
        });
        let (after_first, after_second) =
            crate::test_rendezvous::join_bounded(inner, "cluster slot claim thread");
        assert_eq!(
            after_first, after_second,
            "a second claim on one thread is free"
        );
        assert!(slots().capacity >= 2);
    }
}
