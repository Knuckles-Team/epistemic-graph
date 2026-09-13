//! Taking a lock whose previous holder panicked, without turning one bug into
//! a permanent outage.
//!
//! `Mutex::lock` returns a `Result` because a thread that panics while holding
//! the guard leaves the data at an arbitrary point in whatever update it was
//! making. Rust hands that decision to the caller, and `.unwrap()` is the reflex
//! answer: propagate the panic.
//!
//! For a long-lived server that reflex is usually the wrong trade. Poisoning is
//! sticky -- once a mutex is poisoned it stays poisoned for the life of the
//! process -- so a single panic on one request converts a recoverable fault into
//! every subsequent caller of that lock panicking too. A cache, a registry or a
//! connection map that could simply have been rebuilt instead takes the process
//! down. That is a much larger blast radius than the original bug.
//!
//! The opposite reflex is no better: silently calling `into_inner()` everywhere
//! hides the fact that an invariant may be broken, which is precisely what
//! poisoning exists to tell you.
//!
//! So this module makes the choice explicit and visible, once, instead of
//! implicit and repeated 71 times:
//!
//! * [`LockRecovery::lock_recovering`] recovers the guard AND records that it
//!   did, naming the lock. Use it for state that is rebuildable or
//!   self-correcting -- caches, indexes, registries, metrics -- where continuing
//!   is strictly better than refusing to serve.
//! * [`LockRecovery::lock_or_panic`] keeps the panic, but names the lock and
//!   says why the state cannot be trusted after a poisoned hold. Use it for
//!   authority and durability state, where serving on possibly-broken invariants
//!   is worse than failing.
//!
//! Both are deliberate. A bare `.unwrap()` is neither, which is the thing being
//! removed: not the panic, the absence of a decision.

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Record that a poisoned lock was recovered.
///
/// Deliberately observable: recovering silently would hide a panic that already
/// happened somewhere else, and the whole point of poisoning is to surface it.
fn note_recovered(what: &str) {
    // `eprintln!` rather than the tracing macros so this module stays usable
    // from every layer, including those that must not depend on the observability
    // stack.
    eprintln!("lock recovered after a poisoned hold: {what}");
}

/// Take a lock whose previous holder may have panicked.
pub(crate) trait LockRecovery<'a, T> {
    /// The guard, recovering from poisoning and recording that it happened.
    ///
    /// For state that can be rebuilt or that self-corrects. Continuing to serve
    /// is better than refusing to, and the recovery is reported rather than
    /// swallowed.
    fn lock_recovering(&'a self, what: &str) -> T;

    /// The guard, or panic naming the lock and why its state is untrustworthy.
    ///
    /// For authority and durability state, where serving on possibly-broken
    /// invariants is worse than failing. This is the same outcome `.unwrap()`
    /// gives, with the reasoning attached.
    fn lock_or_panic(&'a self, what: &str) -> T;
}

impl<'a, T> LockRecovery<'a, MutexGuard<'a, T>> for Mutex<T> {
    fn lock_recovering(&'a self, what: &str) -> MutexGuard<'a, T> {
        // The one sanctioned call site: `into_inner` is disallowed so that
        // recovery is never silent, and this function IS the reporting
        // recovery the rule points every other caller at.
        // Invariant `reporting-recovery`: docs/architecture/liveness_invariants.md.
        #[allow(clippy::disallowed_methods)]
        self.lock().unwrap_or_else(|poison| {
            note_recovered(what);
            poison.into_inner()
        })
    }

    fn lock_or_panic(&'a self, what: &str) -> MutexGuard<'a, T> {
        self.lock().unwrap_or_else(|_: PoisonError<_>| {
            panic!("{what}: a holder panicked, so this state cannot be trusted")
        })
    }
}

impl<'a, T> LockRecovery<'a, RwLockReadGuard<'a, T>> for RwLock<T> {
    fn lock_recovering(&'a self, what: &str) -> RwLockReadGuard<'a, T> {
        // See the `Mutex` impl above: this is the reporting recovery the
        // `disallowed_methods` entry exists to route callers to.
        // Invariant `reporting-recovery`: docs/architecture/liveness_invariants.md.
        #[allow(clippy::disallowed_methods)]
        self.read().unwrap_or_else(|poison| {
            note_recovered(what);
            poison.into_inner()
        })
    }

    fn lock_or_panic(&'a self, what: &str) -> RwLockReadGuard<'a, T> {
        self.read().unwrap_or_else(|_: PoisonError<_>| {
            panic!("{what}: a writer panicked, so this state cannot be trusted")
        })
    }
}

/// Write access is a separate trait rather than a second method, because a
/// single type cannot implement the same trait for both guard types.
pub(crate) trait WriteRecovery<'a, T> {
    fn write_recovering(&'a self, what: &str) -> RwLockWriteGuard<'a, T>;
    fn write_or_panic(&'a self, what: &str) -> RwLockWriteGuard<'a, T>;
}

impl<'a, T> WriteRecovery<'a, T> for RwLock<T> {
    fn write_recovering(&'a self, what: &str) -> RwLockWriteGuard<'a, T> {
        // See the `Mutex` impl above: this is the reporting recovery the
        // `disallowed_methods` entry exists to route callers to.
        // Invariant `reporting-recovery`: docs/architecture/liveness_invariants.md.
        #[allow(clippy::disallowed_methods)]
        self.write().unwrap_or_else(|poison| {
            note_recovered(what);
            poison.into_inner()
        })
    }

    fn write_or_panic(&'a self, what: &str) -> RwLockWriteGuard<'a, T> {
        self.write().unwrap_or_else(|_: PoisonError<_>| {
            panic!("{what}: a writer panicked, so this state cannot be trusted")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn a_recovering_lock_still_serves_after_a_holder_panics() {
        let cache = Arc::new(Mutex::new(vec![1_u8, 2, 3]));
        let poisoner = Arc::clone(&cache);
        // `JoinHandle::join` is a `disallowed_methods` entry and
        // `test_rendezvous::join_bounded` cannot stand in for it here: that
        // helper re-raises the worker's panic payload, and the panic IS this
        // fixture -- it is how the mutex gets poisoned. The wait also cannot
        // hang: the closure panics unconditionally, with nothing between spawn
        // and `panic!` that could block.
        // Invariant `panic-fixture-join`: docs/architecture/liveness_invariants.md.
        #[allow(clippy::disallowed_methods)]
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("holder panics while holding the guard");
        })
        .join();
        assert!(cache.lock().is_err(), "the mutex must be poisoned");

        // The rebuildable case: the data is still there and serving continues.
        let guard = cache.lock_recovering("test cache");
        assert_eq!(&*guard, &[1, 2, 3]);
    }

    #[test]
    fn an_authority_lock_refuses_after_a_holder_panics() {
        let authority = Arc::new(Mutex::new(7_u32));
        let poisoner = Arc::clone(&authority);
        // `JoinHandle::join` is a `disallowed_methods` entry and
        // `test_rendezvous::join_bounded` cannot stand in for it here: that
        // helper re-raises the worker's panic payload, and the panic IS this
        // fixture -- it is how the mutex gets poisoned. The wait also cannot
        // hang: the closure panics unconditionally, with nothing between spawn
        // and `panic!` that could block.
        // Invariant `panic-fixture-join`: docs/architecture/liveness_invariants.md.
        #[allow(clippy::disallowed_methods)]
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("holder panics while holding the guard");
        })
        .join();

        let refused = std::panic::catch_unwind(|| {
            // Bind the guard: `let _ =` on a lock drops it immediately, which
            // the compiler denies precisely because it is almost always a bug.
            let _guard = authority.lock_or_panic("test authority");
        });
        assert!(
            refused.is_err(),
            "authority state must refuse rather than serve a possibly-broken invariant"
        );
    }
}
