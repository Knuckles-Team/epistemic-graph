//! Private implementation module for the analytics job handler.

use super::prelude_jobs::*;
use super::prelude_server::*;
use super::prelude_std::*;
use super::*;

pub(super) struct SubmitBuild {
    pub(super) spec: SubmitJobSpec,
    pub(super) input_snapshot: InputSnapshotHandle,
    pub(super) algorithm_family: String,
    pub(super) algorithm: String,
    pub(super) params_digest: String,
    pub(super) input_payload: Vec<u8>,
    pub(super) policy_fingerprint: String,
}

fn build_submit_policy(
    authority: &CarrierAuthority,
    spec: &SubmitJobSpec,
    policy_fingerprint: &str,
) -> JobPolicy {
    let tenant = authority.tenant_scope().to_string();
    let actor = authority.actor_scope().to_string();
    let purpose = crate::server::mutation_batch::opaque_coordinator_key(
        "job-purpose",
        "native",
        &spec.purpose,
    );
    let worker_pool = opaque_placement_value("job_pool", &spec.worker_pool);
    let worker_region = opaque_placement_value("job_region", &spec.worker_region);
    let required_capabilities = spec
        .required_capabilities
        .iter()
        .map(|value| opaque_placement_value("job_capability", value))
        .collect();
    JobPolicy {
        tenant,
        actor,
        purpose,
        priority: spec.priority,
        quota_cpu_ms: spec.quota_cpu_ms,
        deadline_unix_ms: spec.deadline_unix_ms,
        policy_fingerprint: policy_fingerprint.to_string(),
        resources: eg_jobs::ResourceBudget {
            cpu_ms: spec.quota_cpu_ms,
            memory_bytes: spec.memory_bytes,
            io_bytes: spec.io_bytes,
            output_bytes: spec.output_bytes,
        },
        placement: eg_jobs::JobPlacement {
            pool: worker_pool,
            region: worker_region,
            required_capabilities,
        },
    }
}

pub(super) fn build_submit_spec(authority: &CarrierAuthority, build: SubmitBuild) -> SubmitSpec {
    let SubmitBuild {
        spec,
        input_snapshot,
        algorithm_family,
        algorithm,
        params_digest,
        input_payload,
        policy_fingerprint,
    } = build;
    SubmitSpec {
        input_snapshot,
        policy: build_submit_policy(authority, &spec, &policy_fingerprint),
        algo: AlgoVersion {
            family: algorithm_family,
            algorithm,
            params_digest,
            code_version: CODE_VERSION.to_string(),
            env_version: ENV_VERSION.to_string(),
        },
        input_payload: Some(input_payload),
        max_attempts: spec.max_attempts,
        backoff_ms: spec.backoff_ms,
    }
}

#[cfg(feature = "program-optimization")]
pub(super) fn verified_program_policy(
    authority: &CarrierAuthority,
    policy_fingerprint: &str,
    purpose: &str,
) -> Result<PolicyEnvelope, String> {
    let opaque = |namespace: &str, value: &str| {
        OpaqueRef::new(native_opaque_ref(namespace, value)).map_err(|error| error.to_string())
    };
    let purpose_refs = (!purpose.trim().is_empty())
        .then(|| opaque("purpose", purpose))
        .transpose()?
        .into_iter()
        .collect();
    Ok(PolicyEnvelope {
        tenant_ref: opaque("tenant", authority.tenant_scope())?,
        access_policy_ref: opaque("policy", policy_fingerprint)?,
        classification: Classification::Internal,
        retention_policy_ref: opaque("retention", "engine-governed")?,
        deletion_policy_ref: opaque("deletion", "engine-governed")?,
        legal_hold_ref: None,
        purpose_refs,
    })
}
/// The `JobOp::Submit` arm of [`handle`]: compile the durable batch for the
/// submit op, then run [`handle_submit`].
pub(super) async fn handle_submit_op(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    spec: SubmitJobSpec,
) -> Response {
    let method = Method::AnalyticsJob {
        op: JobOp::Submit(spec.clone()),
    };
    let (batch, now) = match compile_job_batch(store, req_id, authority, attempt_nonce, &method) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    handle_submit(state, store, req_id, authority, spec, batch, now).await
}

/// The `JobOp::Cancel` arm of [`handle`]: verify ownership, compile the
/// durable batch for the cancel op, then apply it.
pub(super) fn handle_cancel_op(
    store: &Arc<JobStore>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    job_id: String,
) -> Response {
    if let Err(error) = owned_job(store, authority, &job_id) {
        return Response::err(req_id, error);
    }
    let method = Method::AnalyticsJob {
        op: JobOp::Cancel {
            job_id: job_id.clone(),
        },
    };
    let (batch, now) = match compile_job_batch(store, req_id, authority, attempt_nonce, &method) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    match store.request_cancel_batch(&job_id, &batch, now) {
        Ok((job, _)) => {
            job_response::<eg_types::result_contract::coordination::JobCancel>(req_id, &job)
        }
        Err(e) => Response::err(req_id, e.to_string()),
    }
}

/// The `JobOp::Resume` arm of [`handle`]: verify ownership, compile the
/// durable batch for the resume op, then run [`handle_resume`].
pub(super) async fn handle_resume_op(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    job_id: String,
) -> Response {
    if let Err(error) = owned_job(store, authority, &job_id) {
        return Response::err(req_id, error);
    }
    let method = Method::AnalyticsJob {
        op: JobOp::Resume {
            job_id: job_id.clone(),
        },
    };
    let (batch, now) = match compile_job_batch(store, req_id, authority, attempt_nonce, &method) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    handle_resume(state, store, req_id, &job_id, batch, now).await
}

/// Resolve this process's `JobStore` off `state`'s configured persistence
/// root, and (outside clustered/Raft mode) ensure the colocated worker pool
/// is running. Clustered mode disables automatic per-replica workers: every
/// scheduler transition there must arrive as an authenticated Worker*
/// command and cross Raft.
pub(super) async fn resolve_job_store(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
) -> Result<Arc<JobStore>, Response> {
    let (persist_dir, clustered) = {
        let current = state.read().await;
        (current.persist_dir.clone(), {
            #[cfg(feature = "raft")]
            {
                current.multi_raft.is_some()
            }
            #[cfg(not(feature = "raft"))]
            {
                false
            }
        })
    };
    let store = job_store(persist_dir.as_deref()).map_err(|error| Response::err(req_id, error))?;
    if !clustered {
        ensure_job_workers(state.clone(), store.clone());
    }
    Ok(store)
}
/// Resolve the target graph's `GraphCore` for a `Submit`, checking Read
/// access (the RLS boundary for anything the job might later read).
pub(super) async fn resolve_submit_graph_core(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    graph: &str,
) -> Result<Arc<crate::graph::GraphCore>, Response> {
    let s = state.read().await;
    let Some(entry) = s.registry.get(graph) else {
        return Err(Response::err(req_id, format!("unknown graph '{graph}'")));
    };
    if let Err(error) = check_graph_access(
        &s.isolation,
        Some(authority.agent_id()),
        graph,
        entry.graph_type,
        entry.owner.as_deref(),
        AccessLevel::Read,
    ) {
        return Err(Response::err(req_id, error));
    }
    Ok(entry.core.clone())
}

/// Whether a submit's worker-pool/region/capability placement constraints
/// are outside native bounds.
pub(super) fn submit_placement_invalid(
    worker_pool: &str,
    worker_region: &str,
    required_capabilities: &[String],
) -> bool {
    let invalid_placement_value =
        |value: &str| value.len() > 128 || value.chars().any(char::is_control);
    invalid_placement_value(worker_pool)
        || invalid_placement_value(worker_region)
        || required_capabilities.len() > 64
        || required_capabilities
            .iter()
            .any(|value| value.is_empty() || invalid_placement_value(value))
}

/// The `JobKind::MineAssociate` arm of [`handle_submit`]'s kind-governance
/// match: pseudonymize transaction items, validate thresholds/cardinality,
/// and validate the algorithm name.
pub(super) fn submit_job_mine_associate(
    authority: &CarrierAuthority,
    req_id: u64,
    transactions: Vec<Vec<String>>,
    min_support: f64,
    min_confidence: f64,
    algorithm: String,
) -> Result<(JobKind, String, String, serde_json::Value), Response> {
    // Durable worker input contains opaque item references only. Source
    // labels and personal identifiers never enter jobs.redb.
    let transactions: Vec<Vec<String>> = transactions
        .into_iter()
        .map(|transaction| {
            transaction
                .into_iter()
                .map(|item| {
                    native_opaque_ref(
                        "analytics_item",
                        &format!("{}\0{item}", authority.owner_scope()),
                    )
                })
                .collect()
        })
        .collect();
    let distinct_items = transactions
        .iter()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if !min_support.is_finite()
        || !(0.0..=1.0).contains(&min_support)
        || !min_confidence.is_finite()
        || !(0.0..=1.0).contains(&min_confidence)
        || distinct_items > 31
    {
        return Err(Response::err(
            req_id,
            "association job thresholds or distinct-item cardinality are invalid",
        ));
    }
    if let Err(error) = parse_algorithm(&algorithm) {
        return Err(Response::err(req_id, error));
    }
    let params = serde_json::json!({
        "min_support": min_support,
        "min_confidence": min_confidence,
        "algorithm": algorithm.clone(),
    });
    Ok((
        JobKind::MineAssociate {
            transactions,
            min_support,
            min_confidence,
            algorithm: algorithm.clone(),
        },
        "mining.association".to_string(),
        algorithm,
        params,
    ))
}

/// The `JobKind::ProgramOptimize` arm of [`handle_submit`]'s kind-governance
/// match: bounded-decode the request, rebind its policy to the verified
/// caller, re-encode it, and ensure the native capability is required.
#[cfg(feature = "program-optimization")]
pub(super) fn submit_job_program_optimize(
    authority: &CarrierAuthority,
    req_id: u64,
    policy_fingerprint: &str,
    purpose: &str,
    request_msgpack: Vec<u8>,
    required_capabilities: &mut Vec<String>,
) -> Result<(JobKind, String, String, serde_json::Value), Response> {
    let request: OptimizationRequest = match eg_types::msgpack::decode_bounded(
        &request_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_INPUT_BYTES,
            MAX_JOB_INPUT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    ) {
        Ok(request) => request,
        Err(_) => {
            return Err(Response::err(
                req_id,
                "program optimization request failed bounded decoding",
            ));
        }
    };
    let verified_policy = match verified_program_policy(authority, policy_fingerprint, purpose) {
        Ok(policy) => policy,
        Err(error) => return Err(Response::err(req_id, error)),
    };
    let request = match request.rebind_program_policy(verified_policy) {
        Ok(request) => request,
        Err(error) => {
            return Err(Response::err(
                req_id,
                format!("program optimization request is invalid: {error}"),
            ));
        }
    };
    let optimizer = request.optimizer.as_str().to_string();
    let execution = request.optimizer.execution().as_str().to_string();
    let request_msgpack = match rmp_serde::to_vec_named(&request) {
        Ok(payload) => payload,
        Err(error) => {
            return Err(Response::err(
                req_id,
                format!("program optimization encoding failed: {error}"),
            ));
        }
    };
    ensure_program_optimization_capability(req_id, required_capabilities)?;
    let params = serde_json::json!({
        "optimizer": optimizer,
        "execution": execution,
        "request_ref": request.request_ref.as_str(),
        "corpus_ref": request.corpus.corpus_ref.as_str(),
        "snapshot_version": request.corpus.snapshot_version,
    });
    Ok((
        JobKind::ProgramOptimize { request_msgpack },
        "program.optimization".to_string(),
        optimizer,
        params,
    ))
}

/// Ensure `required_capabilities` names `program.optimization`, appending it
/// if there is room (native cap: 64 entries).
#[cfg(feature = "program-optimization")]
pub(super) fn ensure_program_optimization_capability(
    req_id: u64,
    required_capabilities: &mut Vec<String>,
) -> Result<(), Response> {
    if required_capabilities
        .iter()
        .any(|capability| capability == "program.optimization")
    {
        return Ok(());
    }
    if required_capabilities.len() == 64 {
        return Err(Response::err(
            req_id,
            "analytics job has no room for its required native capability",
        ));
    }
    required_capabilities.push("program.optimization".to_string());
    Ok(())
}
