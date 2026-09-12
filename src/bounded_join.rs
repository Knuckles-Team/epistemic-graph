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
    let (finished, waiting) = sync_channel(1);
    // The unbounded join is confined to this helper thread; the caller's
    // deadline is the `recv_timeout` below. This IS the bounded join the
    // `JoinHandle::join` entry points callers at.
    //
    // `Builder::spawn` rather than `thread::spawn` because this runs on
    // shutdown and from `Drop`, where `thread::spawn`'s panic-on-failure would
    // turn an exhausted thread budget into an abort. If the helper cannot be
    // spawned there is no way to enforce a deadline at all, so this reports
    // that by name instead. `handle` is dropped with the closure, which
    // DETACHES the worker rather than joining it -- the one outcome available
    // here, and still preferable to aborting the process from a destructor.
    let joiner = match std::thread::Builder::new()
        .name("eg-bounded-join".to_string())
        .spawn(move || {
            #[allow(clippy::disallowed_methods)]
            let value = handle.join();
            let _ = finished.send(());
            value
        }) {
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
            #[allow(clippy::disallowed_methods)]
            match joiner.join() {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(_)) => Err(format!("{what} panicked")),
                Err(_) => Err(format!("{what}: the joining helper thread panicked")),
            }
        }
        Err(_) => Err(format!(
            "{what} did not finish within {}s and is still running",
            timeout.as_secs()
        )),
    }
}
