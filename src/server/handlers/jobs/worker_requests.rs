//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::*;

/// The verified worker-identity fields shared by the `handle_worker_*` request
/// handlers, bundled so functions with several additional parameters of their
/// own (e.g. `handle_worker_renew`, `handle_worker_publish`) stay under the
/// clippy argument-count ceiling.
pub(super) struct WorkerRequestCtx<'a> {
    pub(super) req_id: u64,
    pub(super) caller: Option<&'a str>,
    pub(super) verified_worker_context: bool,
    pub(super) worker_instance: &'a str,
}

pub(super) fn worker_ref(
    caller: Option<&str>,
    verified_worker_context: bool,
    worker_instance: &str,
) -> Result<String, String> {
    if !verified_worker_context {
        return Err("analytics worker operations require a verified v2 RequestContext".to_string());
    }
    let principal = caller
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "analytics worker operations require verified request identity".to_string()
        })?;
    let instance = worker_instance.trim();
    if instance.is_empty() || instance.len() > 256 {
        return Err("analytics worker_instance must be a bounded opaque value".to_string());
    }
    Ok(native_opaque_ref(
        "analytics_worker",
        &format!("{principal}\0{instance}"),
    ))
}

pub(super) fn bounded_lease_ms(value: u64) -> u64 {
    value.clamp(1_000, 300_000)
}

pub(super) fn tenant_worker_quota() -> TenantJobQuota {
    TenantJobQuota {
        max_active: std::env::var("EG_ANALYTICS_TENANT_ACTIVE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2),
        max_reserved_cpu_ms: std::env::var("EG_ANALYTICS_TENANT_CPU_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(u64::MAX),
    }
}

pub(super) fn worker_claim_response(req_id: u64, claim: &WorkerClaim) -> Response {
    Response::ok(
        req_id,
        worker_claim_payload(claim).map_err(|_| "worker claim serialization failed".to_string()),
    )
}

pub(super) fn worker_claim_payload(claim: &WorkerClaim) -> Result<ResultPayload, String> {
    let (record, input_payload) = job_record(&claim.job)?;
    ResultPayload::of::<eg_types::result_contract::coordination::JobWorkerClaim>(Some(
        eg_types::result_contract::coordination::WorkerJobClaim {
            job: eg_types::result_contract::coordination::ClaimedAnalyticsJob {
                record,
                input_payload,
            },
            lease: worker_lease(&claim.lease),
        },
    ))
}

pub(super) fn worker_lease(
    lease: &eg_jobs::model::WorkerLease,
) -> eg_types::result_contract::coordination::JobWorkerLease {
    eg_types::result_contract::coordination::JobWorkerLease {
        worker_ref: lease.worker_ref.clone(),
        epoch: lease.epoch,
        acquired_at_ms: lease.acquired_at_ms,
        expires_at_ms: lease.expires_at_ms,
    }
}

pub(super) fn handle_worker_claim(
    store: &JobStore,
    req_id: u64,
    caller: Option<&str>,
    verified_worker_context: bool,
    worker_instance: &str,
    capabilities: Vec<String>,
    lease_ms: u64,
) -> Response {
    let worker_ref = match worker_ref(caller, verified_worker_context, worker_instance) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    if capabilities.len() > 128
        || capabilities.iter().any(|value| {
            value.is_empty() || value.len() > 128 || value.chars().any(char::is_control)
        })
    {
        return Response::err(req_id, "analytics worker capabilities are invalid");
    }
    let capabilities: Vec<String> = capabilities
        .iter()
        .map(|value| opaque_worker_capability(value))
        .collect();
    match store.claim_next(
        &worker_ref,
        &capabilities,
        unix_ms(),
        bounded_lease_ms(lease_ms),
        tenant_worker_quota(),
    ) {
        Ok(Some(claim)) => worker_claim_response(req_id, &claim),
        Ok(None) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::coordination::JobWorkerClaim>(None),
        ),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

pub(super) fn handle_worker_renew(
    store: &JobStore,
    ctx: WorkerRequestCtx<'_>,
    job_id: &str,
    lease_epoch: u64,
    lease_ms: u64,
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
    match store.renew_lease(
        job_id,
        &worker_ref,
        lease_epoch,
        unix_ms(),
        bounded_lease_ms(lease_ms),
    ) {
        Ok(lease) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::coordination::JobWorkerRenew>(
                worker_lease(&lease),
            ),
        ),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_worker_checkpoint(
    store: &JobStore,
    ctx: WorkerRequestCtx<'_>,
    job_id: &str,
    lease_epoch: u64,
    progress: f64,
    stage: String,
    state_ref: Option<String>,
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
    let valid_stage = matches!(
        stage.as_str(),
        "leased" | "mining" | "optimizing" | "computed" | "publishing" | "published"
    );
    let valid_state = state_ref
        .as_ref()
        .is_none_or(|value| is_opaque_result_ref(value));
    if !progress.is_finite() || !valid_stage || !valid_state {
        return Response::err(req_id, "analytics worker checkpoint is invalid");
    }
    let now = unix_ms();
    match store.checkpoint_fenced(
        job_id,
        &worker_ref,
        lease_epoch,
        Checkpoint {
            progress: progress.clamp(0.0, 1.0),
            stage,
            state_blob: state_ref.map(String::into_bytes),
            updated_at_ms: now,
        },
        now,
    ) {
        Ok(job) => job_response::<eg_types::result_contract::coordination::JobWorkerCheckpoint>(
            req_id, &job,
        ),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}
