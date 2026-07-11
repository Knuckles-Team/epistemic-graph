//! Request-scoped cancellation registry (CONCEPT:EG-KG.query.streaming-spillable-collect, L36).
//!
//! `eg_query::sql::exec::collect_streaming` (the batch-at-a-time SQL collect path,
//! EG-P1-4) already checks a [`eg_query::CancellationToken`] between batches — but
//! before this module every served `Method::Sql` call built a FRESH, never-cancelled
//! token internally (`exec_sql_typed_with_tables`), so a client cancel or a server-side
//! timeout had no live token to trip: cancellation was wired end-to-end EXCEPT for the
//! one hop that matters, the wire protocol to a live request.
//!
//! This module closes that hop: [`register`] hands the SQL handler a REAL token and
//! remembers it under the request's `req_id` for the lifetime of the call (an RAII
//! [`RequestCancelGuard`] removes the entry on drop — success, error, or panic-unwind,
//! so the registry never accumulates stale entries); [`cancel`] trips the token for a
//! given `req_id` — driven by [`crate::protocol::Method::CancelRequest`] (an explicit
//! client cancel) or by [`spawn_timeout`] (a per-request deadline). Either trip is
//! observed by `collect_streaming` at its NEXT batch boundary and stops the stream
//! short — chunk-granular, never mid-batch, exactly as `CancellationToken`'s own docs
//! describe.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use eg_query::CancellationToken;

fn registry() -> &'static Mutex<HashMap<u64, CancellationToken>> {
    static REG: OnceLock<Mutex<HashMap<u64, CancellationToken>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

/// RAII handle for one registered, cancellable request. Holding this alive is what
/// keeps `req_id`'s token reachable to [`cancel`]; dropping it (the handler's `?`
/// early-return, its normal return, OR an unwinding panic) removes the entry so a
/// completed/aborted request's id is never left cancellable — a `CancelRequest` racing
/// a request's own completion harmlessly reports "not found" rather than reaching a
/// stale or reused token.
pub struct RequestCancelGuard {
    req_id: u64,
}

impl Drop for RequestCancelGuard {
    fn drop(&mut self) {
        registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.req_id);
    }
}

/// Register `token` as the live cancellation handle for `req_id`, returning the guard
/// that keeps it registered until dropped. Call this BEFORE dispatching the
/// cancellable work (e.g. before `compute_off_lock`'s `spawn_blocking`) and keep the
/// guard alive for exactly as long as that work can still observe `token`.
pub fn register(req_id: u64, token: CancellationToken) -> RequestCancelGuard {
    registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(req_id, token);
    RequestCancelGuard { req_id }
}

/// Trip the cancellation token registered for `req_id`, if a cancellable request is
/// currently live under that id. Returns `true` iff a live token was found and
/// cancelled — `false` for an unknown, already-finished, or never-cancellable
/// `req_id` (never an error: cancelling a request that already completed, or that was
/// never cancellable to begin with, is a harmless no-op).
pub fn cancel(req_id: u64) -> bool {
    match registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&req_id)
    {
        Some(tok) => {
            tok.cancel();
            true
        }
        None => false,
    }
}

/// Spawn a background task that cancels `token` after `EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS`
/// milliseconds — a server-side per-request DEADLINE that trips the SAME cooperative
/// cancellation an explicit client `Method::CancelRequest` does, so a runaway SQL scan
/// is bounded even when no client ever cancels it. Returns `None` (spawns nothing) when
/// the env var is unset, non-numeric, or `0` — the default, so a served query has no
/// timeout unless the operator opts in. The caller aborts the returned handle once its
/// own work finishes (cancelled or not), so a completed request never leaves a stray
/// sleeping task behind.
pub fn spawn_timeout(token: CancellationToken) -> Option<tokio::task::JoinHandle<()>> {
    let ms = std::env::var("EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS")
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|&n| n > 0)?;
    Some(tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        token.cancel();
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `cancel` is a no-op (`false`) for an id nothing ever registered.
    #[test]
    fn cancel_unknown_id_is_a_harmless_noop() {
        assert!(!cancel(u64::MAX - 4242));
    }

    /// `register` makes the token reachable by `cancel`; the SAME token (cloned) observes
    /// the trip.
    #[test]
    fn register_then_cancel_trips_the_same_token() {
        let tok = CancellationToken::new();
        let guard = register(9001, tok.clone());
        assert!(!tok.is_cancelled(), "fresh token starts uncancelled");
        assert!(cancel(9001), "a live registered id must be found");
        assert!(tok.is_cancelled(), "cancel() must trip the SAME token");
        drop(guard);
    }

    /// Dropping the guard removes the entry — a later `cancel` for the same id (now
    /// stale/reused) reports "not found" rather than reaching a dangling token.
    #[test]
    fn drop_removes_the_registry_entry() {
        let tok = CancellationToken::new();
        {
            let _guard = register(9002, tok.clone());
            assert!(cancel(9002), "registered while the guard is alive");
        }
        // guard dropped here
        assert!(
            !cancel(9002),
            "cancel after the guard drops must find nothing (no stale entry)"
        );
    }

    /// Two DIFFERENT req_ids never cross-cancel each other's token.
    #[test]
    fn distinct_req_ids_do_not_cross_cancel() {
        let tok_a = CancellationToken::new();
        let tok_b = CancellationToken::new();
        let _ga = register(9101, tok_a.clone());
        let _gb = register(9102, tok_b.clone());
        assert!(cancel(9101));
        assert!(tok_a.is_cancelled());
        assert!(!tok_b.is_cancelled(), "cancelling 9101 must not touch 9102's token");
    }

    /// `EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS` is process-global; serialize the two
    /// tests below against each other (mirrors `redb_backend`'s `LINGER_ENV_LOCK`
    /// precedent) so a parallel run can't have one observe the other's value.
    static TIMEOUT_ENV_LOCK: Mutex<()> = Mutex::new(());

    /// `spawn_timeout` is a no-op (spawns nothing) when the env var is unset/invalid —
    /// the default posture: no server-side deadline unless explicitly configured.
    #[tokio::test]
    async fn spawn_timeout_noop_when_unset() {
        let _env = TIMEOUT_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS");
        let tok = CancellationToken::new();
        assert!(spawn_timeout(tok).is_none());
    }

    /// `spawn_timeout` trips the token after the configured deadline. The env-var lock
    /// is scoped to a plain block (dropped before the `.await` below) — `spawn_timeout`
    /// reads the env var synchronously, at spawn time, so releasing the lock right after
    /// spawning is sound: the spawned task no longer touches the env var, only its
    /// already-resolved millisecond deadline.
    #[tokio::test]
    async fn spawn_timeout_trips_after_deadline() {
        let tok = CancellationToken::new();
        let handle = {
            let _env = TIMEOUT_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            std::env::set_var("EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS", "10");
            let handle = spawn_timeout(tok.clone());
            std::env::remove_var("EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS");
            handle
            // `_env` drops here, BEFORE the `.await` below.
        }
        .expect("configured ⇒ Some");
        assert!(!tok.is_cancelled(), "must not fire before the deadline");
        handle.await.unwrap();
        assert!(tok.is_cancelled(), "must fire once the deadline elapses");
    }
}
