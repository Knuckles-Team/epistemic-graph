//! Deterministic, verified submit of a Decision replay to the durable job plane.

use super::prelude_jobs::*;
use super::*;
use crate::server::auth::VerifiedRequestContext;
use sha2::{Digest, Sha256};

/// Enqueue a previously persisted, content-addressed replay request. The
/// Decision job identity is stable across retries even when the transport
/// request ID, mutation version, or wall clock changes.
pub(crate) async fn submit_decision_replay(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    decision_identity_key: &str,
    request_ref: &str,
    graph: &str,
) -> Result<eg_jobs::AnalyticsJob, String> {
    if decision_identity_key.is_empty() || decision_identity_key.len() > 256 {
        return Err("decision replay identity is invalid".to_string());
    }
    let authority = CarrierAuthority::from_verified(verified)?;
    if !verified.allows_action("admin:decision-eval") && !verified.allows_action("kg:admin") {
        return Err("ACCESS_DENIED: decision replay requires DecisionEval authority".to_string());
    }
    let core = resolve_submit_graph_core(state, req_id, &authority, graph)
        .await
        .map_err(|response| {
            response
                .error
                .unwrap_or_else(|| "decision replay graph is unavailable".to_string())
        })?;
    let store = resolve_job_store(state, req_id).await.map_err(|response| {
        response
            .error
            .unwrap_or_else(|| "analytics job store is unavailable".to_string())
    })?;
    let mut required_capabilities = Vec::new();
    let (kind, family, algorithm, params) =
        submit_job_decision_replay(req_id, request_ref.to_string(), &mut required_capabilities)
            .map_err(|response| {
                response
                    .error
                    .unwrap_or_else(|| "decision replay submit is invalid".to_string())
            })?;
    let spec = SubmitJobSpec {
        graph: graph.to_string(),
        tenant: String::new(),
        actor: String::new(),
        purpose: "decision.replay".to_string(),
        priority: 0,
        deadline_unix_ms: None,
        quota_cpu_ms: Some(300_000),
        memory_bytes: Some(512 * 1024 * 1024),
        io_bytes: None,
        output_bytes: Some(16 * 1024 * 1024),
        worker_pool: String::new(),
        worker_region: String::new(),
        required_capabilities,
        max_attempts: 3,
        backoff_ms: 1_000,
        kind: kind.clone(),
    };
    let method = Method::AnalyticsJob {
        op: JobOp::Submit(spec.clone()),
    };
    let scope = authority.namespace("analytics-jobs", "control");
    let identity = eg_jobs::analytics_job_scope_identity().map_err(|error| error.to_string())?;
    let identity_tenant = identity.tenant().as_str().to_string();
    let expected = store
        .mutation_version(authority.tenant_scope(), &scope)
        .map_err(|error| error.to_string())?;
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "decision-replay-job",
        authority.tenant_scope(),
        decision_identity_key,
    );
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_method_in_scope(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: 0,
            attempt_nonce: None,
            principal: Some(authority.actor_scope()),
            tenant: &identity_tenant,
            graph: &scope,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Job,
            authoritative_state: None,
        },
        &method,
        MutationSurface::Job,
        DurabilityDomain::AnalyticsJob,
        "analytics_job_operation",
        Some(identity),
    )?;
    let policy_fingerprint = batch
        .envelope
        .operation()
        .map(|operation| operation.authority.policy_revision.as_str().to_string())
        .unwrap_or_else(|| "policy:unversioned".to_string());
    let input_payload = encode_job_input(&kind, req_id).map_err(|response| {
        response
            .error
            .unwrap_or_else(|| "decision replay input encoding failed".to_string())
    })?;
    let input_digest = hex::encode(Sha256::digest(&input_payload));
    let snapshot = InputSnapshotHandle::new(native_opaque_ref("graph", graph), core.version())
        .with_dataset(format!("eg:job_input:{input_digest}"), input_digest.clone());
    let params_digest = eg_jobs::digest_params(&serde_json::json!({
        "params": params,
        "input_content_digest": input_digest,
    }));
    let submitted = build_submit_spec(
        &authority,
        SubmitBuild {
            spec,
            input_snapshot: snapshot,
            algorithm_family: family,
            algorithm,
            params_digest,
            input_payload: input_payload.clone(),
            policy_fingerprint,
        },
    );
    let (job, _) = store
        .submit_batch(submitted, &batch, now)
        .map_err(|error| error.to_string())?;
    if job.policy.tenant != authority.tenant_scope()
        || job.policy.actor != authority.actor_scope()
        || job.input_snapshot.graph != native_opaque_ref("graph", graph)
        || job.input_payload.as_deref() != Some(input_payload.as_slice())
        || job.algo.family != "decision.replay"
    {
        return Err("decision replay identity replays a different analytics job".to_string());
    }
    Ok(job)
}
