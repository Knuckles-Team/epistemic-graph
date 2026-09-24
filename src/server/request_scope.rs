//! The cancellation scope of one served native request (EH-536).
//!
//! A client that gives up on a request — its own deadline expired, so it closed
//! the connection — used to leave the engine running the abandoned work to the
//! end: a ConnectorPack import kept its tenant's pack lock and every later pack
//! operation for that tenant queued behind it. The transport now gives every
//! dispatched request a [`RequestCancel`] and trips it when the connection's
//! peer closes or the server dispatch deadline expires; long-running handlers
//! read it with [`current`] and stop:
//!
//! * waits (a pack lock) race it and give up with [`CANCELLED`];
//! * CPU-bound work runs through [`interruptible`], which installs it as the
//!   `eg_core::interrupt` probe the reasoning step loops poll, and discards the
//!   result of an interrupted run.

use std::future::Future;
use std::sync::Arc;

/// Error prefix a cancelled request reports (nobody is normally left to read it).
pub(crate) const CANCELLED: &str =
    "CANCELLED: the request was abandoned (client disconnected or deadline expired)";

/// One request's cancellation flag, awaitable and cheap to clone.
#[derive(Clone)]
pub(crate) struct RequestCancel(Arc<tokio::sync::watch::Sender<bool>>);

impl RequestCancel {
    pub(crate) fn new() -> Self {
        Self(Arc::new(tokio::sync::watch::channel(false).0))
    }

    pub(crate) fn cancel(&self) {
        self.0.send_replace(true);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolves once [`Self::cancel`] has been called (immediately if it was).
    pub(crate) async fn cancelled(&self) {
        let mut watch = self.0.subscribe();
        // The sender lives in `self`, so the channel cannot close under us.
        let _ = watch.wait_for(|cancelled| *cancelled).await;
    }
}

tokio::task_local! {
    static CURRENT: RequestCancel;
}

/// Run one request's dispatch future with `cancel` as its [`current`] scope.
pub(crate) async fn scope<F: Future>(cancel: RequestCancel, dispatch: F) -> F::Output {
    CURRENT.scope(cancel, dispatch).await
}

/// The running request's cancellation; a never-cancelled one outside a served
/// request (in-process callers, background tasks).
pub(crate) fn current() -> RequestCancel {
    CURRENT
        .try_with(RequestCancel::clone)
        .unwrap_or_else(|_| RequestCancel::new())
}

/// Run blocking `work` interruptibly on behalf of `cancel`'s request. An
/// interrupted run's result is partial by construction, so once `cancel` has
/// tripped the result is discarded and [`CANCELLED`] is returned instead.
pub(crate) fn interruptible<T>(
    cancel: &RequestCancel,
    work: impl FnOnce() -> T,
) -> Result<T, String> {
    let probe = cancel.clone();
    let output = eg_core::interrupt::scoped(move || probe.is_cancelled(), work);
    if cancel.is_cancelled() {
        return Err(CANCELLED.to_string());
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_resolves_for_a_cancel_before_or_after_the_wait() {
        let early = RequestCancel::new();
        early.cancel();
        early.cancelled().await;

        let late = RequestCancel::new();
        let waiter = tokio::spawn({
            let late = late.clone();
            async move { late.cancelled().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        late.cancel();
        waiter.await.unwrap();
        assert!(late.is_cancelled());
    }

    #[tokio::test]
    async fn current_is_the_scoped_request_and_fresh_outside_one() {
        assert!(!current().is_cancelled());
        let cancel = RequestCancel::new();
        cancel.cancel();
        assert!(scope(cancel, async { current().is_cancelled() }).await);
    }

    #[test]
    fn an_interrupted_run_reports_cancelled_not_its_partial_result() {
        let cancel = RequestCancel::new();
        assert_eq!(interruptible(&cancel, || 7), Ok(7));
        let tripped = cancel.clone();
        let outcome = interruptible(&cancel, move || {
            tripped.cancel();
            eg_core::interrupt::requested()
        });
        assert_eq!(outcome, Err(CANCELLED.to_string()));
    }
}
