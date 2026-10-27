//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_std::*;
use super::*;

const DEFAULT_ANALYTICS_LEASE_MS: u64 = 60_000;
const MIN_ANALYTICS_LEASE_MS: u64 = 5_000;
const MAX_ANALYTICS_LEASE_MS: u64 = 300_000;
const DEFAULT_ANALYTICS_POLL_MS: u64 = 250;
const MIN_ANALYTICS_POLL_MS: u64 = 50;
const MAX_ANALYTICS_POLL_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AnalyticsWorkerTiming {
    lease_ms: u64,
    poll_ms: u64,
}

fn parse_bounded_millis(
    name: &str,
    raw: Option<&str>,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64, String> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let value = raw
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an integer between {min} and {max} milliseconds"))?;
    if !(min..=max).contains(&value) {
        return Err(format!(
            "{name} must be between {min} and {max} milliseconds"
        ));
    }
    Ok(value)
}

fn analytics_worker_timing() -> Result<AnalyticsWorkerTiming, String> {
    Ok(AnalyticsWorkerTiming {
        lease_ms: parse_bounded_millis(
            "EG_ANALYTICS_LEASE_MS",
            std::env::var("EG_ANALYTICS_LEASE_MS").ok().as_deref(),
            DEFAULT_ANALYTICS_LEASE_MS,
            MIN_ANALYTICS_LEASE_MS,
            MAX_ANALYTICS_LEASE_MS,
        )?,
        poll_ms: parse_bounded_millis(
            "EG_ANALYTICS_POLL_MS",
            std::env::var("EG_ANALYTICS_POLL_MS").ok().as_deref(),
            DEFAULT_ANALYTICS_POLL_MS,
            MIN_ANALYTICS_POLL_MS,
            MAX_ANALYTICS_POLL_MS,
        )?,
    })
}

/// Start the optional bounded colocated executor pool. Setting the count to zero
/// leaves execution to authenticated remote workers using the coordinator ops.
pub(super) fn ensure_job_workers(
    state: Arc<RwLock<ServerState>>,
    store: Arc<JobStore>,
) -> Result<(), String> {
    let timing = analytics_worker_timing()?;
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
            tokio::spawn(worker_loop(state.clone(), store.clone(), slot, timing));
        }
    });
    Ok(())
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
    timing: AnalyticsWorkerTiming,
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
        match store.claim_next(
            &worker_ref,
            &capabilities,
            unix_ms(),
            timing.lease_ms,
            quota,
        ) {
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
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_millis(timing.poll_ms)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analytics_worker_timing_uses_defaults_and_accepts_range_ends() {
        assert_eq!(
            parse_bounded_millis(
                "EG_ANALYTICS_LEASE_MS",
                None,
                DEFAULT_ANALYTICS_LEASE_MS,
                MIN_ANALYTICS_LEASE_MS,
                MAX_ANALYTICS_LEASE_MS,
            ),
            Ok(DEFAULT_ANALYTICS_LEASE_MS)
        );
        assert_eq!(
            parse_bounded_millis(
                "EG_ANALYTICS_LEASE_MS",
                Some("5000"),
                DEFAULT_ANALYTICS_LEASE_MS,
                MIN_ANALYTICS_LEASE_MS,
                MAX_ANALYTICS_LEASE_MS,
            ),
            Ok(MIN_ANALYTICS_LEASE_MS)
        );
        assert_eq!(
            parse_bounded_millis(
                "EG_ANALYTICS_LEASE_MS",
                Some("300000"),
                DEFAULT_ANALYTICS_LEASE_MS,
                MIN_ANALYTICS_LEASE_MS,
                MAX_ANALYTICS_LEASE_MS,
            ),
            Ok(MAX_ANALYTICS_LEASE_MS)
        );
        assert_eq!(
            parse_bounded_millis(
                "EG_ANALYTICS_POLL_MS",
                None,
                DEFAULT_ANALYTICS_POLL_MS,
                MIN_ANALYTICS_POLL_MS,
                MAX_ANALYTICS_POLL_MS,
            ),
            Ok(DEFAULT_ANALYTICS_POLL_MS)
        );
        assert_eq!(
            parse_bounded_millis(
                "EG_ANALYTICS_POLL_MS",
                Some("50"),
                DEFAULT_ANALYTICS_POLL_MS,
                MIN_ANALYTICS_POLL_MS,
                MAX_ANALYTICS_POLL_MS,
            ),
            Ok(MIN_ANALYTICS_POLL_MS)
        );
        assert_eq!(
            parse_bounded_millis(
                "EG_ANALYTICS_POLL_MS",
                Some("30000"),
                DEFAULT_ANALYTICS_POLL_MS,
                MIN_ANALYTICS_POLL_MS,
                MAX_ANALYTICS_POLL_MS,
            ),
            Ok(MAX_ANALYTICS_POLL_MS)
        );
    }

    #[test]
    fn analytics_worker_timing_refuses_out_of_range_or_malformed_values() {
        for (name, raw, default, min, max) in [
            (
                "EG_ANALYTICS_LEASE_MS",
                "4999",
                DEFAULT_ANALYTICS_LEASE_MS,
                MIN_ANALYTICS_LEASE_MS,
                MAX_ANALYTICS_LEASE_MS,
            ),
            (
                "EG_ANALYTICS_LEASE_MS",
                "300001",
                DEFAULT_ANALYTICS_LEASE_MS,
                MIN_ANALYTICS_LEASE_MS,
                MAX_ANALYTICS_LEASE_MS,
            ),
            (
                "EG_ANALYTICS_POLL_MS",
                "49",
                DEFAULT_ANALYTICS_POLL_MS,
                MIN_ANALYTICS_POLL_MS,
                MAX_ANALYTICS_POLL_MS,
            ),
            (
                "EG_ANALYTICS_POLL_MS",
                "30001",
                DEFAULT_ANALYTICS_POLL_MS,
                MIN_ANALYTICS_POLL_MS,
                MAX_ANALYTICS_POLL_MS,
            ),
            (
                "EG_ANALYTICS_POLL_MS",
                "not-a-number",
                DEFAULT_ANALYTICS_POLL_MS,
                MIN_ANALYTICS_POLL_MS,
                MAX_ANALYTICS_POLL_MS,
            ),
            (
                "EG_ANALYTICS_LEASE_MS",
                "-1",
                DEFAULT_ANALYTICS_LEASE_MS,
                MIN_ANALYTICS_LEASE_MS,
                MAX_ANALYTICS_LEASE_MS,
            ),
        ] {
            assert!(parse_bounded_millis(name, Some(raw), default, min, max).is_err());
        }
    }
}
