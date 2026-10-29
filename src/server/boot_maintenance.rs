//! Post-bind boot maintenance (MEDULLA-DURABLE-KERNEL-R006.4, G9): the boot work
//! that is RAM re-hydration / crash-window residual closing, NOT a serving
//! precondition — the materialized-view reload and the time-series startup
//! reconciliation.
//!
//! Pre-G9 both ran on the pre-listener critical path (`start_raft_and_
//! matview_reload`, the tail of `recover_durable_catalog`): there they queued
//! behind background maintenance holding the `ServerState` write lock (the
//! connector-pack-projection drain opened the agent-library store under it)
//! and made the listeners — the only thing that marks the engine up — wait
//! minutes on a large store, with nothing logged for the whole interval.
//! They now run as background tasks spawned AFTER the primary listeners have
//! bound (`server_startup::serve_transports` places the hook right after the
//! bind), so the pre-listener path never waits on them, and each logs its
//! start and completion (counts + elapsed) — a multi-minute pass must never
//! again be invisible in the log.

use std::sync::Arc;
#[cfg(any(
    feature = "compute-dist",
    feature = "matview",
    all(feature = "redb", feature = "tsdb"),
))]
use std::time::Instant;

use tokio::sync::RwLock;

use crate::server::state::ServerState;

/// Spawn the post-bind maintenance tasks. Called from `serve_transports`
/// after the primary listener(s) have bound — the one boot point where
/// "the engine is up" is already true.
pub async fn spawn_post_bind_maintenance(state: Arc<RwLock<ServerState>>) {
    // Both tasks are feature-gated; without either there is nothing to do.
    let _ = &state;
    #[cfg(any(feature = "compute-dist", feature = "matview"))]
    spawn_matview_reload(state.clone());
    #[cfg(all(feature = "redb", feature = "tsdb"))]
    spawn_time_series_reconcile(state).await;
}

/// Re-hydrate the in-RAM materialized-view index from the durable tier as a
/// background task (CONCEPT:EG-KG.storage.feature). No correctness
/// requirement to precede serving, and it must never hold the pre-listener
/// path up (MEDULLA-DURABLE-KERNEL-R006.4).
#[cfg(any(feature = "compute-dist", feature = "matview"))]
fn spawn_matview_reload(state: Arc<RwLock<ServerState>>) {
    tokio::spawn(async move {
        let started = Instant::now();
        tracing::info!("post-bind maintenance: materialized-view reload started");
        match crate::server::reload_matviews(&state).await {
            Ok(0) => {
                tracing::info!(
                    "post-bind maintenance: materialized-view reload finished in {:.3}s \
                     (0 view(s) reloaded)",
                    started.elapsed().as_secs_f64()
                )
            }
            Ok(n) => {
                tracing::info!(
                    "post-bind maintenance: reloaded {n} materialized view(s) from redb in \
                     {:.3}s (CONCEPT:EG-KG.storage.feature)",
                    started.elapsed().as_secs_f64()
                )
            }
            Err(e) => tracing::warn!("materialized-view reload skipped: {e}"),
        }
    });
}

/// Close the time-series crash-window residual as a background task
/// (CONCEPT:EG-KG.backend.ts-startup-reconcile, L16): a crash strictly
/// BETWEEN the two EG-P0-4 commits can leave the authoritative shard ahead
/// of the served `series.redb`. That is residual closing, not a serving
/// precondition — run it after the listeners bind (MEDULLA-DURABLE-KERNEL-R006.4).
/// The shard scan is synchronous redb I/O, so it runs off the reactor through
/// the usual blocking-task boundary.
#[cfg(all(feature = "redb", feature = "tsdb"))]
async fn spawn_time_series_reconcile(state: Arc<RwLock<ServerState>>) {
    // Snapshot the backend and served store under the read lock; the pass
    // itself needs no state at all.
    let (backend, series) = {
        let guard = state.read().await;
        (guard.persistence.clone(), guard.tsdb_store.clone())
    };
    let Some(backend) = backend else {
        return;
    };
    let Some(series) = series else {
        return;
    };
    tokio::spawn(async move {
        let started = Instant::now();
        tracing::info!(
            "post-bind maintenance: time-series startup reconciliation started \
             (CONCEPT:EG-KG.backend.ts-startup-reconcile)"
        );
        if backend.as_redb().is_none() {
            tracing::info!(
                "post-bind maintenance: time-series startup reconciliation \
                 skipped (no redb backend)"
            );
            return;
        }
        // The redb lookup is repeated INSIDE the blocking closure (it borrows
        // from the owned backend, which must be moved into the 'static
        // closure); the `None` arm is unreachable after the check above.
        let report = match crate::server::dispatch::blocking_task(
            "time-series startup reconciliation",
            move || {
                let Some(redb) = backend.as_redb() else {
                    return Err("no redb persistence backend".to_string());
                };
                redb.reconcile_time_series_sync(&series)
            },
        )
        .await
        {
            Ok(report) => report,
            Err(error) => {
                tracing::error!(
                    code = "TS_STARTUP_RECONCILE_FAILED",
                    %error,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "time-series startup reconciliation failed; the crash-window residual \
                     stays open until the next boot"
                );
                return;
            }
        };
        // Logged even when the work was zero — a zero-work boot used to log
        // nothing at all, and a slow one used to log nothing either
        // (MEDULLA-DURABLE-KERNEL-R006.4/.5).
        tracing::info!(
            series_examined = report.series_examined,
            series_reconciled = report.series_reconciled,
            points_replayed = report.points_replayed,
            projection_cursors_written = report.projection_cursors_written,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "post-bind maintenance: time-series startup reconciliation complete"
        );
    });
}
