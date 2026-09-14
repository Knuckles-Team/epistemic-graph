//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_worker_stage(
    store: &JobStore,
    req_id: u64,
    caller: Option<&str>,
    verified_worker_context: bool,
    worker_instance: &str,
    job_id: &str,
    lease_epoch: u64,
    result: JobResult,
) -> Response {
    let worker_ref = match worker_ref(caller, verified_worker_context, worker_instance) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    let result = typed_result_from_wire(result);
    if let Err(error) = validate_remote_result_privacy(&result) {
        return Response::err(req_id, error);
    }
    let current = match store.get(job_id) {
        Ok(job) => job,
        Err(error) => return Response::err(req_id, error.to_string()),
    };
    #[cfg(feature = "knowledge-batch")]
    if let Err(error) = validate_native_job_result(&current, &result) {
        return Response::err(req_id, error);
    }
    if worker_stage_already_matches(&current, &worker_ref, lease_epoch, &result) {
        return job_response::<eg_types::result_contract::coordination::JobWorkerStage>(
            req_id, &current,
        );
    }
    match store.stage_result_fenced(job_id, &worker_ref, lease_epoch, result, unix_ms()) {
        Ok(job) => {
            job_response::<eg_types::result_contract::coordination::JobWorkerStage>(req_id, &job)
        }
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

/// Whether a worker's re-submitted stage result is byte-identical to an
/// already-`Succeeded` job for the SAME worker/lease — an idempotent retry
/// that should replay the existing job rather than re-stage.
pub(super) fn worker_stage_already_matches(
    current: &eg_jobs::AnalyticsJob,
    worker_ref: &str,
    lease_epoch: u64,
    result: &TypedJobResult,
) -> bool {
    matches!(&current.state, JobState::Succeeded { .. })
        && current.last_worker_ref == worker_ref
        && current.lease_epoch == lease_epoch
        && current.output.as_ref() == Some(result)
}

pub(super) async fn handle_worker_publish(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    ctx: WorkerRequestCtx<'_>,
    job_id: &str,
    lease_epoch: u64,
) -> Response {
    let WorkerRequestCtx {
        req_id,
        caller,
        verified_worker_context,
        worker_instance,
    } = ctx;
    let worker_ref = match worker_ref(caller, verified_worker_context, worker_instance) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    let job = match store.get(job_id) {
        Ok(job) => job,
        Err(error) => return Response::err(req_id, error.to_string()),
    };
    if worker_publish_already_succeeded(&job, &worker_ref, lease_epoch) {
        return job_response::<eg_types::result_contract::coordination::JobWorkerPublish>(
            req_id, &job,
        );
    }
    let job = match require_publishing_lease(store, req_id, job_id, &worker_ref, lease_epoch) {
        Ok(job) => job,
        Err(response) => return response,
    };
    #[cfg(feature = "raft")]
    if let Some(response) =
        publish_via_consensus_if_replicated(state, req_id, &job, &worker_ref, lease_epoch).await
    {
        return response;
    }
    finalize_local_publish(state, store, job, &worker_ref, lease_epoch, job_id, req_id).await
}

/// Publish a job's staged result locally (non-consensus path) and reload the
/// job record for the response.
#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_local_publish(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    job: eg_jobs::AnalyticsJob,
    worker_ref: &str,
    lease_epoch: u64,
    job_id: &str,
    req_id: u64,
) -> Response {
    match publish_staged_result(state, store, job, worker_ref, lease_epoch).await {
        Ok(()) => match store.get(job_id) {
            Ok(job) => job_response::<eg_types::result_contract::coordination::JobWorkerPublish>(
                req_id, &job,
            ),
            Err(error) => Response::err(req_id, error.to_string()),
        },
        Err(error) => Response::err(req_id, error),
    }
}

/// Whether the job is already `Succeeded` for this SAME worker/lease — an
/// idempotent retry that should replay the existing job rather than re-verify
/// the lease and republish.
pub(super) fn worker_publish_already_succeeded(
    job: &eg_jobs::AnalyticsJob,
    worker_ref: &str,
    lease_epoch: u64,
) -> bool {
    matches!(&job.state, JobState::Succeeded { .. })
        && job.last_worker_ref == worker_ref
        && job.lease_epoch == lease_epoch
}

/// Verify the worker's fenced lease and require the job be in `Publishing`
/// state — the only state a publish may proceed from.
pub(super) fn require_publishing_lease(
    store: &JobStore,
    req_id: u64,
    job_id: &str,
    worker_ref: &str,
    lease_epoch: u64,
) -> Result<eg_jobs::AnalyticsJob, Response> {
    match store.verify_lease(job_id, worker_ref, lease_epoch, unix_ms()) {
        Ok(job) if matches!(&job.state, JobState::Publishing { .. }) => Ok(job),
        Ok(job) => Err(Response::err(
            req_id,
            format!(
                "worker publication requires Publishing, got {}",
                job.state.label()
            ),
        )),
        Err(error) => Err(Response::err(req_id, error.to_string())),
    }
}

/// If this process is applying replicated Raft entries, prepare the
/// consensus job-publication command instead of publishing locally.
/// `None` means the caller should proceed with the local publish path.
#[cfg(feature = "raft")]
pub(super) async fn publish_via_consensus_if_replicated(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    job: &eg_jobs::AnalyticsJob,
    worker_ref: &str,
    lease_epoch: u64,
) -> Option<Response> {
    if !crate::server::dispatch::is_replicated_apply() {
        return None;
    }
    Some(
        match prepare_consensus_job_publication(state, job, worker_ref, lease_epoch).await {
            Ok(prepared) => Response::ok(req_id, ResultPayload::Raw(prepared)),
            Err(error) => Response::err(req_id, error),
        },
    )
}

pub(super) fn handle_worker_cancel(
    store: &JobStore,
    req_id: u64,
    caller: Option<&str>,
    verified_worker_context: bool,
    worker_instance: &str,
    job_id: &str,
    lease_epoch: u64,
) -> Response {
    let worker_ref = match worker_ref(caller, verified_worker_context, worker_instance) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    if let Ok(job) = store.get(job_id) {
        if matches!(&job.state, JobState::Cancelled { .. })
            && job.last_worker_ref == worker_ref
            && job.lease_epoch == lease_epoch
        {
            return job_response::<eg_types::result_contract::coordination::JobWorkerCancel>(
                req_id, &job,
            );
        }
    }
    match store.mark_cancelled_fenced(job_id, &worker_ref, lease_epoch, unix_ms()) {
        Ok(job) => {
            job_response::<eg_types::result_contract::coordination::JobWorkerCancel>(req_id, &job)
        }
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_worker_fail(
    store: &JobStore,
    req_id: u64,
    caller: Option<&str>,
    verified_worker_context: bool,
    worker_instance: &str,
    job_id: &str,
    lease_epoch: u64,
    reason_code: &str,
) -> Response {
    let worker_ref = match worker_ref(caller, verified_worker_context, worker_instance) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    if !matches!(
        reason_code,
        "kernel_cancelled"
            | "kernel_failure"
            | "deadline_exceeded"
            | "cpu_budget_exceeded"
            | "invalid_payload"
    ) {
        return Response::err(req_id, "analytics worker failure code is invalid");
    }
    if let Ok(job) = store.get(job_id) {
        if matches!(&job.state, JobState::Submitted | JobState::Failed { .. })
            && job.last_worker_ref == worker_ref
            && job.lease_epoch == lease_epoch
        {
            return job_response::<eg_types::result_contract::coordination::JobWorkerFail>(
                req_id, &job,
            );
        }
    }
    match store.fail_attempt_fenced(job_id, &worker_ref, lease_epoch, reason_code, unix_ms()) {
        Ok(job) => {
            job_response::<eg_types::result_contract::coordination::JobWorkerFail>(req_id, &job)
        }
        Err(error) => Response::err(req_id, error.to_string()),
    }
}
