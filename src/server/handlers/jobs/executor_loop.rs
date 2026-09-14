//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

/// Start the optional bounded colocated executor pool. Setting the count to zero
/// leaves execution to authenticated remote workers using the coordinator ops.
pub(super) fn ensure_job_workers(state: Arc<RwLock<ServerState>>, store: Arc<JobStore>) {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        // Gauge state must advance when leases expire even if no worker or
        // client RPC arrives. This coordinator task is deliberately independent
        // of EG_ANALYTICS_WORKERS so remote-only deployments remain autoscalable.
        let metric_store = store.clone();
        tokio::spawn(async move {
            loop {
                refresh_job_metrics(&metric_store);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let workers = std::env::var("EG_ANALYTICS_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1)
            .min(32);
        for slot in 0..workers {
            tokio::spawn(worker_loop(state.clone(), store.clone(), slot));
        }
    });
}

pub(super) fn refresh_job_metrics(store: &JobStore) {
    if let Ok((ready, active, publishing)) = store.metric_counts(unix_ms()) {
        crate::metrics::set_analytics_job_counts(ready, active, publishing);
    }
}

pub(super) async fn worker_loop(
    state: Arc<RwLock<ServerState>>,
    store: Arc<JobStore>,
    slot: usize,
) {
    let worker_ref = crate::server::mutation_batch::opaque_coordinator_key(
        "analytics-worker",
        "native",
        &slot.to_string(),
    );
    let mut capabilities = vec![
        opaque_worker_capability("mining.association"),
        opaque_worker_capability("pool:default"),
    ];
    #[cfg(feature = "program-optimization")]
    capabilities.push(opaque_worker_capability("program.optimization"));
    let quota = tenant_worker_quota();
    loop {
        match store.claim_next(&worker_ref, &capabilities, unix_ms(), 60_000, quota) {
            Ok(Some(claim)) => {
                let job_id = claim.job.job_id.clone();
                let lease = claim.lease.clone();
                if execute_claim(&state, &store, claim).await.is_err() {
                    // Compute failures consume the current attempt and obey durable
                    // retry backoff. Publication failures retain the complete result
                    // and release only ownership so another replica can replay it.
                    if let Ok(current) = store.get(&job_id) {
                        match current.state {
                            JobState::Running { .. } => {
                                let _ = store.fail_attempt_fenced(
                                    &job_id,
                                    &lease.worker_ref,
                                    lease.epoch,
                                    "worker_execution_failed",
                                    unix_ms(),
                                );
                            }
                            JobState::Publishing { .. } => {
                                let _ = store.release_publication_lease_fenced(
                                    &job_id,
                                    &lease.worker_ref,
                                    lease.epoch,
                                    unix_ms(),
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}
