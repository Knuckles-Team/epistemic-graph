//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

/// Lazily-opened local projection of the consensus-owned analytics scheduler. A
/// served process must have the configured persistence root; there is no process-temp
/// scheduler authority that can disappear or diverge during coordinator failover.
pub(super) fn job_store(persist_dir: Option<&str>) -> Result<Arc<JobStore>, String> {
    static STORE: OnceLock<Result<Arc<JobStore>, String>> = OnceLock::new();
    let persist_path = job_persistence_path(persist_dir)?;
    STORE.get_or_init(|| open_job_store(persist_path)).clone()
}

fn job_persistence_path(persist_dir: Option<&str>) -> Result<&Path, String> {
    persist_dir
        .filter(|value| !value.is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            "analytics jobs require a configured durable persistence directory".to_string()
        })
}

fn open_job_store(persist_path: &Path) -> Result<Arc<JobStore>, String> {
    let authority = crate::store_authority::process_authority();
    let proof = authority.proof();
    JobStore::open_in_dir(
        persist_path,
        authority.as_ref(),
        authority.principal(),
        &proof,
    )
    .map(Arc::new)
    .map_err(|_| "analytics job projection is unavailable".to_string())
}

pub(super) fn reproducibility_manifest(
    snapshot: &InputSnapshotHandle,
    algorithm: &AlgoVersion,
    policy: &JobPolicy,
) -> ReproducibilityManifest {
    let mut manifest = ReproducibilityManifest::default();
    manifest.input_dataset_ref.clone_from(&snapshot.dataset_ref);
    manifest
        .input_content_digest
        .clone_from(&snapshot.content_digest);
    manifest.input_snapshot_version = snapshot.version;
    manifest.algorithm_ref = format!("{}:{}", algorithm.family, algorithm.algorithm);
    manifest.params_digest.clone_from(&algorithm.params_digest);
    manifest
        .implementation_version
        .clone_from(&algorithm.code_version);
    manifest
        .environment_version
        .clone_from(&algorithm.env_version);
    manifest
        .policy_fingerprint
        .clone_from(&policy.policy_fingerprint);
    manifest
}

pub(super) fn parse_algorithm(name: &str) -> Result<Algorithm, String> {
    match name.to_ascii_lowercase().as_str() {
        "apriori" => Ok(Algorithm::Apriori),
        "fpgrowth" | "fp-growth" | "fp_growth" => Ok(Algorithm::FpGrowth),
        "eclat" => Ok(Algorithm::Eclat),
        other => Err(format!(
            "unknown MineAssociate job algorithm '{other}' (expected apriori|fpgrowth|eclat)"
        )),
    }
}

pub(super) fn owned_job(
    store: &JobStore,
    authority: &CarrierAuthority,
    job_id: &str,
) -> Result<AnalyticsJob, String> {
    let job = store
        .get(job_id)
        .map_err(|_| "analytics job not found or not owned by caller".to_string())?;
    if authority.owns(&job.policy.tenant, &job.policy.actor) {
        Ok(job)
    } else {
        crate::metrics::access_denied();
        Err("analytics job not found or not owned by caller".to_string())
    }
}

pub(super) fn compile_job_batch(
    store: &JobStore,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
) -> Result<(MutationBatch, u64), String> {
    // The caller's own namespace still keys the BATCH -- so two tenants' job
    // requests never collide on a batch id or an idempotency key -- but it must
    // NOT key the mutation SCOPE. `JobStore` is one process-wide native store
    // whose owner scope is bound once at `open()` to
    // `analytics_job_scope_identity()`, and `verify_scope` refuses any batch
    // naming a different identity. Deriving the identity from
    // `authority.tenant_scope()` / `authority.namespace(..)` (plus the generic
    // `COMPILED_BATCH_INCARNATION`) therefore mismatched on all three fields and
    // failed EVERY call with "jobs redb error: mutation capability does not serve
    // this scope" -- the whole `Method::AnalyticsJob` wire surface was
    // uncommittable. `JobStore::mutation_version` already ignores both of its
    // arguments for the same reason (one store, one scope, one version).
    //
    // Per-caller isolation is unaffected: it is enforced by
    // `authority.owns(&job.policy.tenant, &job.policy.actor)` on the job row
    // (see `owned_job` above), which is where it belongs -- not by the physical
    // scope identity of a store that has exactly one.
    let scope = authority.namespace("analytics-jobs", "control");
    let identity = eg_jobs::analytics_job_scope_identity().map_err(|error| error.to_string())?;
    let expected = store
        .mutation_version(authority.tenant_scope(), &scope)
        .map_err(|error| error.to_string())?;
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("analytics-job", &scope, req_id, method);
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_method_in_scope(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: req_id,
            attempt_nonce,
            principal: Some(authority.actor_scope()),
            tenant: identity.tenant().as_str(),
            graph: &scope,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Job,
            authoritative_state: None,
        },
        method,
        MutationSurface::Job,
        DurabilityDomain::AnalyticsJob,
        "analytics_job_operation",
        Some(identity.clone()),
    )?;
    Ok((batch, now))
}

pub(super) fn job_response<M>(req_id: u64, job: &eg_jobs::AnalyticsJob) -> Response
where
    M: eg_types::result_contract::MethodResult<
        Body = eg_types::result_contract::coordination::AnalyticsJobRecord,
    >,
{
    match job_result_payload::<M>(job) {
        Ok(result) => Response::ok(req_id, result),
        Err(e) => Response::err(req_id, format!("job serialization failed: {e}")),
    }
}

pub(super) fn job_result_payload<M>(job: &eg_jobs::AnalyticsJob) -> Result<ResultPayload, String>
where
    M: eg_types::result_contract::MethodResult<
        Body = eg_types::result_contract::coordination::AnalyticsJobRecord,
    >,
{
    let (record, _input_payload) = job_record(job)?;
    ResultPayload::of::<M>(record)
}

/// Project a durable job onto its declared wire record, strictly, split from its
/// executor payload. Executor payloads are durable implementation detail: even
/// governed, pseudonymized inputs are only ever handed to the claiming worker.
pub(super) fn job_record(
    job: &eg_jobs::AnalyticsJob,
) -> Result<
    (
        eg_types::result_contract::coordination::AnalyticsJobRecord,
        Option<Vec<u8>>,
    ),
    String,
> {
    let mut value = serde_json::to_value(job).map_err(|error| error.to_string())?;
    if let Some(object) = value.as_object_mut() {
        object.remove("input_payload");
    }
    let record = serde_json::from_value(value)
        .map_err(|error| format!("job record does not match its declared projection: {error}"))?;
    Ok((record, job.input_payload.clone()))
}

/// Resolve a completed typed job result for the shared KnowledgeBatch stream.
/// The graph already passed graph ACL and placement checks before this helper is
/// called; matching the immutable input graph prevents a job id from becoming a
/// cross-tenant read handle.
#[cfg(feature = "knowledge-batch")]
pub(crate) fn knowledge_stream_result(
    persist_dir: &Option<String>,
    graph: &str,
    job_id: &str,
) -> Result<(eg_jobs::AnalyticsJob, TypedJobResult), String> {
    let store = job_store(persist_dir.as_deref())?;
    let job = store.get(job_id).map_err(|error| error.to_string())?;
    if job.input_snapshot.graph != native_opaque_ref("graph", graph) {
        return Err("analytics result does not belong to the authorized graph".to_string());
    }
    if !matches!(job.state, eg_jobs::JobState::Succeeded { .. }) {
        return Err("analytics result is not committed".to_string());
    }
    let result = job
        .output
        .clone()
        .ok_or_else(|| "analytics result is missing".to_string())?;
    result.validate()?;
    Ok((job, result))
}
