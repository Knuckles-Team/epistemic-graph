//! Tenant-bound, fenced execution of a Decision walk-forward replay.

use super::prelude_jobs::*;
use super::prelude_std::*;
use super::*;

#[cfg(all(feature = "decide", feature = "finance"))]
fn stable_replay_failure_code(error: &str) -> &'static str {
    eg_types::decision::statistical::StatisticalErrorCode::ALL
        .iter()
        .find(|code| {
            error.starts_with(code.as_str())
                && error.as_bytes().get(code.as_str().len()) == Some(&b':')
        })
        .map(|code| code.as_str())
        .unwrap_or("DECISION_REPLAY_WORKER_FAILED")
}

#[cfg(all(feature = "decide", feature = "finance"))]
pub(super) async fn execute_replay_claim(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    claim: WorkerClaim,
    payload: Vec<u8>,
    request_ref: String,
) -> Result<(), String> {
    use eg_types::decision::replay::EvalMode;
    use eg_types::decision::DecisionEvalRequest;
    use sha2::{Digest, Sha256};

    let job = &claim.job;
    let payload_digest = hex::encode(Sha256::digest(&payload));
    if payload_digest != job.input_snapshot.content_digest
        || job.input_snapshot.dataset_ref != format!("eg:job_input:{payload_digest}")
        || job.algo.family != "decision.replay"
        || job.algo.algorithm != "walk_forward"
        || job.algo.params_digest
            != eg_jobs::digest_params(&serde_json::json!({
                "params": {"request_ref": request_ref},
                "input_content_digest": payload_digest,
            }))
    {
        return Err("decision replay job lineage is invalid".to_string());
    }
    let library = state.write().await.ensure_agent_library()?;
    let bytes = library
        .decision_artifact(&job.policy.tenant, &request_ref)?
        .ok_or_else(|| "decision replay request artifact is unavailable".to_string())?;
    if bytes.len() > MAX_JOB_INPUT_BYTES {
        return Err("decision replay request artifact exceeds worker bounds".to_string());
    }
    let request: DecisionEvalRequest = crate::server::persistence::decision_jobs::decode_artifact(
        &bytes,
        "decision replay request",
    )?;
    if request.tenant_id.trim().is_empty()
        || crate::server::access::verified_tenant_scope(&request.tenant_id) != job.policy.tenant
        || !matches!(&request.mode, EvalMode::Replay { .. })
        || crate::server::handlers::decide::replay_request_ref(&request) != request_ref
    {
        return Err(
            "decision replay request does not match the pinned tenant and reference".to_string(),
        );
    }

    let worker_ref = claim.lease.worker_ref.clone();
    let epoch = claim.lease.epoch;
    let job_id = job.job_id.clone();
    let jobs = Arc::clone(store);
    let mut work = tokio::task::spawn_blocking(move || {
        crate::server::handlers::decide::run_replay_job(&library, &jobs, &claim, &request)
    });
    let began = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    let run = loop {
        tokio::select! {
            joined = &mut work => match joined {
                Ok(Ok(run)) => break run,
                Ok(Err(error)) => {
                    store.fail_attempt_fenced(&job_id, &worker_ref, epoch, stable_replay_failure_code(&error), unix_ms())
                        .map_err(|error| error.to_string())?;
                    return Ok(());
                }
                Err(_) => {
                    store.fail_attempt_fenced(&job_id, &worker_ref, epoch, "DECISION_REPLAY_WORKER_FAILED", unix_ms())
                        .map_err(|error| error.to_string())?;
                    return Ok(());
                }
            },
            _ = ticker.tick() => {
                let now = unix_ms();
                let current = store.get(&job_id).map_err(|error| error.to_string())?;
                let cpu_exceeded = current.policy.resources.cpu_ms
                    .or(current.policy.quota_cpu_ms)
                    .is_some_and(|limit| began.elapsed().as_millis() as u64 >= limit);
                if current.cancel_requested || current.deadline_exceeded(now) || cpu_exceeded {
                    if current.cancel_requested {
                        let _ = store.mark_cancelled_fenced(&job_id, &worker_ref, epoch, now);
                    } else {
                        let reason = if cpu_exceeded { "cpu_budget_exceeded" }
                            else { "deadline_exceeded" };
                        let _ = store.fail_attempt_fenced(&job_id, &worker_ref, epoch, reason, now);
                    }
                    return Ok(());
                }
                store.renew_lease(&job_id, &worker_ref, epoch, now, 60_000)
                    .map_err(|error| error.to_string())?;
            }
        }
    };
    let result = typed_replay_result(
        &store.get(&job_id).map_err(|error| error.to_string())?,
        &request_ref,
        &run,
    )?;
    let staged = store
        .stage_result_fenced(&job_id, &worker_ref, epoch, result, unix_ms())
        .map_err(|error| error.to_string())?;
    publish_staged_result(state, store, staged, &worker_ref, epoch).await
}

#[cfg(all(test, feature = "decide", feature = "finance"))]
mod tests {
    use super::stable_replay_failure_code;

    #[test]
    fn worker_keeps_only_closed_refusal_code() {
        assert_eq!(
            stable_replay_failure_code("LOOK_AHEAD: secret row detail"),
            "LOOK_AHEAD"
        );
        assert_eq!(
            stable_replay_failure_code("REPLAY_SPEC_INVALID: malformed folds"),
            "REPLAY_SPEC_INVALID"
        );
        assert_eq!(
            stable_replay_failure_code("LOOK_AHEAD_UNSAFE: forged"),
            "DECISION_REPLAY_WORKER_FAILED"
        );
        assert_eq!(
            stable_replay_failure_code("io: /private/path"),
            "DECISION_REPLAY_WORKER_FAILED"
        );
    }
}
