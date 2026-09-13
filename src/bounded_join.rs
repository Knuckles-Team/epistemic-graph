//! Bounded thread joins on production paths.
//!
//! `JoinHandle::join` is a `disallowed_methods` entry because it has no
//! deadline: a worker that deadlocked upstream takes its joiner down with it.
//! [`crate::test_rendezvous::join_bounded`] is the bounded answer for tests, but
//! it is `#![cfg(test)]` and it re-raises the worker's panic payload, so it
//! cannot serve a production caller that has to keep running.
//!
//! This is the production counterpart: the join is confined to a throwaway
//! helper thread, the caller's deadline is a `recv_timeout`, and a worker that
//! overruns is reported by NAME instead of hanging its joiner. The overrunning
//! thread is left alone rather than being detached-and-forgotten silently —
//! there is no way to cancel an OS thread, so saying so is the honest outcome.
use std::sync::mpsc::sync_channel;
use std::thread::JoinHandle;
use std::time::Duration;

/// The helper thread's body: the worker's join result.
type HelperBody<T> = Box<dyn FnOnce() -> std::thread::Result<T> + Send + 'static>;

/// Starts the helper thread. Production uses [`spawn_named_helper`]; tests
/// substitute a spawner that fails, which is otherwise not reproducible.
type HelperSpawner<T> = fn(HelperBody<T>) -> std::io::Result<JoinHandle<std::thread::Result<T>>>;

/// Join `handle` within `timeout`, or report which worker overran.
///
/// `Err` carries a message naming `what`; `Ok(Err(()))` is not modelled because
/// a panicking worker and an overrunning one are the same thing to every caller
/// here: the result is unavailable and the reason belongs in the message.
pub(crate) fn join_within<T: Send + 'static>(
    handle: JoinHandle<T>,
    what: &str,
    timeout: Duration,
) -> Result<T, String> {
    join_within_using(handle, what, timeout, spawn_named_helper::<T>)
}

fn spawn_named_helper<T: Send + 'static>(
    body: HelperBody<T>,
) -> std::io::Result<JoinHandle<std::thread::Result<T>>> {
    std::thread::Builder::new()
        .name("eg-bounded-join".to_string())
        .spawn(body)
}

fn join_within_using<T: Send + 'static>(
    handle: JoinHandle<T>,
    what: &str,
    timeout: Duration,
    spawn_helper: HelperSpawner<T>,
) -> Result<T, String> {
    let (finished, waiting) = sync_channel(1);
    // The unbounded join is confined to this helper thread; the caller's
    // deadline is the `recv_timeout` below. This IS the bounded join the
    // `JoinHandle::join` entry points callers at. Invariant
    // `helper-confined-wait`: docs/architecture/liveness_invariants.md.
    //
    // `Builder::spawn` rather than `thread::spawn` because this runs on
    // shutdown and from `Drop`, where `thread::spawn`'s panic-on-failure would
    // turn an exhausted thread budget into an abort. If the helper cannot be
    // spawned there is no way to enforce a deadline at all, so this reports
    // that by name instead. `handle` is dropped with the closure, which
    // DETACHES the worker rather than joining it -- the one outcome available
    // here, and still preferable to aborting the process from a destructor.
    let joiner = match spawn_helper(Box::new(move || {
        // Invariant `helper-confined-wait`: docs/architecture/liveness_invariants.md.
        #[allow(clippy::disallowed_methods)]
        let value = handle.join();
        let _ = finished.send(());
        value
    })) {
        Ok(joiner) => joiner,
        Err(error) => {
            return Err(format!(
                "{what}: could not spawn a bounded-join helper ({error}), so the join \
                 could not be given a deadline"
            ));
        }
    };
    match waiting.recv_timeout(timeout) {
        Ok(()) => {
            // The helper already signalled, so this join cannot block.
            // Invariant `helper-confined-wait`: docs/architecture/liveness_invariants.md.
            #[allow(clippy::disallowed_methods)]
            match joiner.join() {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(_)) => Err(format!("{what} panicked")),
                Err(_) => Err(format!("{what}: the joining helper thread panicked")),
            }
        }
        // `{timeout:?}`, not whole seconds: a sub-second bound must not read "0s".
        Err(_) => Err(format!(
            "{what} did not finish within {timeout:?} and is still running"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bound for waits that must NOT expire in a passing run. Every worker below
    /// is released by the test itself; nothing depends on scheduling speed.
    const GENEROUS: Duration = Duration::from_secs(30);

    #[test]
    fn a_worker_that_finishes_within_the_bound_returns_its_value() {
        let worker = std::thread::spawn(|| 42_u32);
        assert_eq!(join_within(worker, "quick worker", GENEROUS), Ok(42));
    }

    #[test]
    fn a_worker_that_overruns_the_bound_is_reported_by_name_and_left_running() {
        // The worker cannot finish until the test releases it, so the 50ms bound
        // expires deterministically rather than by winning a sleep race.
        let (release, released) = sync_channel::<()>(1);
        let (exited, exit_seen) = sync_channel::<bool>(1);
        let worker = std::thread::spawn(move || {
            let was_released = released.recv_timeout(GENEROUS).is_ok();
            let _ = exited.send(was_released);
            7_u32
        });

        let outcome = join_within(worker, "wedged worker", Duration::from_millis(50));

        assert_eq!(
            outcome,
            Err("wedged worker did not finish within 50ms and is still running".to_string())
        );
        assert!(
            exit_seen.try_recv().is_err(),
            "an overrun is reported while the worker is still running, not after it exits"
        );
        release
            .send(())
            .expect("the overrunning worker is still waiting");
        assert_eq!(exit_seen.recv_timeout(GENEROUS), Ok(true));
    }

    #[test]
    fn a_worker_that_panics_is_reported_as_panicked_not_propagated() {
        let worker = std::thread::spawn(|| -> u32 { panic!("worker fixture panics") });
        assert_eq!(
            join_within(worker, "panicking worker", GENEROUS),
            Err("panicking worker panicked".to_string())
        );
    }

    fn refuse_to_spawn<T>(
        _body: HelperBody<T>,
    ) -> std::io::Result<JoinHandle<std::thread::Result<T>>> {
        Err(std::io::Error::other("thread budget exhausted"))
    }

    #[test]
    fn a_helper_that_cannot_be_spawned_is_reported_without_joining_the_worker() {
        // The worker stays blocked, so a fallback to an unbounded join would hang here.
        let (release, released) = sync_channel::<()>(1);
        let worker = std::thread::spawn(move || released.recv_timeout(GENEROUS).is_ok());

        let outcome = join_within_using(
            worker,
            "unjoinable worker",
            GENEROUS,
            refuse_to_spawn::<bool>,
        );

        assert_eq!(
            outcome,
            Err(
                "unjoinable worker: could not spawn a bounded-join helper (thread budget \
                 exhausted), so the join could not be given a deadline"
                    .to_string()
            )
        );
        let _ = release.send(());
    }

    fn spawn_helper_that_panics_after_signalling<T: Send + 'static>(
        body: HelperBody<T>,
    ) -> std::io::Result<JoinHandle<std::thread::Result<T>>> {
        std::thread::Builder::new().spawn(move || {
            let _joined = body();
            panic!("helper fixture panics after signalling completion")
        })
    }

    #[test]
    fn a_helper_that_panics_is_reported_as_a_helper_failure() {
        let worker = std::thread::spawn(|| 1_u8);
        assert_eq!(
            join_within_using(
                worker,
                "helper-panic worker",
                GENEROUS,
                spawn_helper_that_panics_after_signalling::<u8>,
            ),
            Err("helper-panic worker: the joining helper thread panicked".to_string())
        );
    }
}
