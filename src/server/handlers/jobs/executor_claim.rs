//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_std::*;
use super::*;

pub(super) async fn execute_claim(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    claim: WorkerClaim,
) -> Result<(), String> {
    let worker_ref = claim.lease.worker_ref.clone();
    let epoch = claim.lease.epoch;
    let job_id = claim.job.job_id.clone();
    let staged = if matches!(&claim.job.state, JobState::Publishing { .. }) {
        claim.job
    } else {
        let payload = claim
            .job
            .input_payload
            .clone()
            .ok_or_else(|| "analytics input payload is unavailable".to_string())?;
        let kind: JobKind = eg_types::msgpack::decode_bounded(
            &payload,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_JOB_INPUT_BYTES,
                MAX_JOB_INPUT_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "analytics input decoding failed".to_string())?;
        let (transactions, min_support, min_confidence, algorithm) = match kind {
            JobKind::MineAssociate {
                transactions,
                min_support,
                min_confidence,
                algorithm,
            } => (transactions, min_support, min_confidence, algorithm),
            #[cfg(feature = "program-optimization")]
            JobKind::ProgramOptimize { request_msgpack } => {
                return execute_program_claim(
                    state,
                    store,
                    &claim.job,
                    &worker_ref,
                    epoch,
                    payload,
                    request_msgpack,
                )
                .await;
            }
        };
        let algorithm = parse_algorithm(&algorithm)?;
        store
            .checkpoint_fenced(
                &job_id,
                &worker_ref,
                epoch,
                Checkpoint {
                    progress: 0.1,
                    stage: "mining".to_string(),
                    state_blob: Some(payload),
                    updated_at_ms: unix_ms(),
                },
                unix_ms(),
            )
            .map_err(|error| error.to_string())?;

        let cancellation = Arc::new(AtomicBool::new(false));
        let kernel_token = cancellation.clone();
        let mut kernel = tokio::task::spawn_blocking(move || {
            association::mine_labeled_cancellable(
                &transactions,
                min_support,
                min_confidence,
                algorithm,
                kernel_token,
            )
        });
        let began = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        let mut stop_reason: Option<&'static str> = None;
        let rules = loop {
            tokio::select! {
                joined = &mut kernel => {
                    match joined {
                        Ok(Ok(rules)) if stop_reason.is_none() => break rules,
                        Ok(Err(_)) | Ok(Ok(_)) => {
                            match stop_reason {
                                Some("cancelled") => {
                                    let _ = store.mark_cancelled_fenced(
                                        &job_id, &worker_ref, epoch, unix_ms(),
                                    );
                                }
                                Some(reason) => {
                                    let _ = store.fail_attempt_fenced(
                                        &job_id, &worker_ref, epoch, reason, unix_ms(),
                                    );
                                }
                                None => {
                                    let _ = store.fail_attempt_fenced(
                                        &job_id, &worker_ref, epoch, "kernel_cancelled", unix_ms(),
                                    );
                                }
                            }
                            return Ok(());
                        }
                        Err(_) => {
                            let _ = store.fail_attempt_fenced(
                                &job_id, &worker_ref, epoch, "kernel_failure", unix_ms(),
                            );
                            return Ok(());
                        }
                    }
                }
                _ = ticker.tick() => {
                    let now = unix_ms();
                    let current = match store.get(&job_id) {
                        Ok(job) => job,
                        Err(_) => {
                            cancellation.store(true, Ordering::Relaxed);
                            stop_reason = Some("lease_lost");
                            continue;
                        }
                    };
                    let cpu_exceeded = current
                        .policy
                        .resources
                        .cpu_ms
                        .or(current.policy.quota_cpu_ms)
                        .is_some_and(|limit| began.elapsed().as_millis() as u64 >= limit);
                    stop_reason = if current.cancel_requested {
                        Some("cancelled")
                    } else if current.deadline_exceeded(now) {
                        Some("deadline_exceeded")
                    } else if cpu_exceeded {
                        Some("cpu_budget_exceeded")
                    } else {
                        None
                    };
                    if stop_reason.is_some() {
                        cancellation.store(true, Ordering::Relaxed);
                    } else if store
                        .renew_lease(&job_id, &worker_ref, epoch, now, 60_000)
                        .is_err()
                    {
                        cancellation.store(true, Ordering::Relaxed);
                        stop_reason = Some("lease_lost");
                    }
                }
            }
        };
        let now = unix_ms();
        store
            .checkpoint_fenced(
                &job_id,
                &worker_ref,
                epoch,
                Checkpoint {
                    progress: 0.9,
                    stage: "computed".to_string(),
                    state_blob: None,
                    updated_at_ms: now,
                },
                now,
            )
            .map_err(|error| error.to_string())?;
        let result = typed_association_result(&claim.job, &rules)?;
        store
            .stage_result_fenced(&job_id, &worker_ref, epoch, result, unix_ms())
            .map_err(|error| error.to_string())?
    };

    publish_staged_result(state, store, staged, &worker_ref, epoch).await
}
