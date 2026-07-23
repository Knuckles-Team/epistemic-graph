//! The durable analytics-job plane's protocol surface (CONCEPT:INT-P2-1, feature
//! `jobs`): `Method::AnalyticsJob { op }` — submit/status/cancel/resume over
//! `eg-jobs`'s redb-backed `AnalyticsJob` state machine.
//!
//! NOT graph-scoped: jobs are keyed by `job_id` in the consensus-owned analytics
//! control range and projected into local `jobs.redb`, so this module self-routes
//! `dispatch.rs`'s top-level match, ahead of the per-graph `dispatch_graph_op`
//! chain — see that module's doc + `src/server/mutation.rs`'s native-coordinator
//! inventory.
//!
//! ## Why this file never calls `GraphReadAuthority::filter_view`/`project_core`
//!
//! `handle_submit` resolves the target graph's `Arc<GraphCore>` and calls
//! `check_graph_access(..., AccessLevel::Read)` — a COARSE, graph-LEVEL ACL
//! check ("does this caller have any access to this graph at all?") — plus
//! `core.version()`, used ONLY to stamp the job's immutable input-snapshot
//! handle (CONCEPT:INT-P2-1: a client cannot forge which graph-version a job
//! ran against). Neither is a per-row decision, and today neither NEEDS to be:
//! [`reads_graph_rows_server_side`] is an exhaustive, no-wildcard match proving
//! (at compile time — a new `JobKind` variant without an arm here fails to
//! build) that no shipped `JobKind` reads a node/edge property from `core`.
//! `MineAssociate` mines only caller-supplied `transactions`; `ProgramOptimize`
//! submits an opaque request a REMOTE WORKER later claims and executes under
//! its OWN independently authenticated session (see "Distributed execution
//! contract" below) — this handler never touches the worker's read path.
//! `handle_submit` also runtime-checks this classification (fail-closed, not a
//! debug-only assert) before ever destructuring `kind`, so a FUTURE
//! graph-reading `JobKind` that is marked `true` here but not yet wired
//! through `GraphReadAuthority::project_core` is refused rather than silently
//! served unfiltered.
//!
//! ## Distributed execution contract
//!
//! A bounded worker pool claims durable work by renewable lease and monotonically
//! increasing fencing epoch. The association kernel observes cooperative
//! cancellation inside its inner loops. Every clustered worker transition crosses
//! Raft, including claims, renewals, checkpoints, staging and publication. Complete
//! output is staged as a typed
//! KnowledgeBatch result before evidence-bearing claims are committed through the
//! universal MutationBatch gateway; only that successful publication can move a
//! job from `Publishing` to terminal `Succeeded`. Expired compute leases consume
//! retry attempts, whereas an expired publication lease safely replays the same
//! deterministic claim batch without recomputing or discarding the staged result.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use eg_compute::mining::association::{self, Algorithm};
use eg_core::graph::GraphCore;
use eg_jobs::model::{
    AlgoVersion, AnalyticsJob, Checkpoint, InputSnapshotHandle, JobPolicy, JobState,
};
use eg_jobs::store::{JobStore, SubmitSpec, TenantJobQuota, WorkerClaim};
use eg_jobs::{ReproducibilityManifest, ResultColumn, TypedJobResult};
use eg_types::jobs::{JobKind, JobOp, JobResult, SubmitJobSpec};

#[cfg(feature = "program-optimization")]
use eg_modality::{Classification, OpaqueRef, PolicyEnvelope};
#[cfg(feature = "program-optimization")]
use eg_program::{NativeCompiler, OptimizationRequest, ProgramModality};

use crate::isolation::AccessLevel;
use crate::mutation_batch::{MutationBatch, MutationDomain, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::{check_graph_access, CarrierAuthority};
use crate::server::state::ServerState;

/// The engine build that ran a job (CONCEPT:INT-P2-1 lineage) — `CARGO_PKG_VERSION`
/// of the `epistemic-graph` facade crate itself.
const CODE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Stable runtime/feature contract recorded in result reproducibility lineage.
const ENV_VERSION: &str = "eg-jobs-v1";
const MAX_JOB_INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_JOB_INPUT_ITEMS: usize = 1_000_000;
#[cfg(feature = "raft")]
const JOB_PUBLICATION_PLAN_VERSION: u16 = 1;
#[cfg(feature = "raft")]
const MAX_JOB_PUBLICATION_PLAN_BYTES: usize = 16 * 1024 * 1024;

/// Transient scheduler-group PREPARE result. It is returned only to the trusted
/// coordinator; every subsequent Raft command carries it AEAD-sealed.
#[cfg(feature = "raft")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedJobPublication {
    schema_version: u16,
    pub(crate) coordinator_id: String,
    pub(crate) target_graph: String,
    pub(crate) target_graph_type: crate::protocol::GraphType,
    job_id: String,
    worker_ref: String,
    lease_epoch: u64,
    result_ref: String,
    principal_ref: String,
    batch_id: String,
    claim_id: String,
    dataset_ref: String,
    methods: Vec<Method>,
}

/// Target-group COMMIT plan. Placement authority is frozen before it is sealed,
/// preventing a coordinator retry from silently changing participants.
#[cfg(feature = "raft")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutedJobPublication {
    schema_version: u16,
    prepared: PreparedJobPublication,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
}

/// Scheduler-group FINALIZE receipt. It contains no claim payload; the target
/// commit's success is represented by the fact this sealed command was proposed.
#[cfg(feature = "raft")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FinalizeJobPublication {
    schema_version: u16,
    coordinator_id: String,
    job_id: String,
    worker_ref: String,
    lease_epoch: u64,
    result_ref: String,
}

/// Lazily-opened local projection of the consensus-owned analytics scheduler. A
/// served process must have the configured persistence root; there is no process-temp
/// scheduler authority that can disappear or diverge during coordinator failover.
fn job_store(persist_dir: Option<&str>) -> Result<Arc<JobStore>, String> {
    static STORE: OnceLock<Result<Arc<JobStore>, String>> = OnceLock::new();
    let persist_dir = persist_dir
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "analytics jobs require a configured durable persistence directory".to_string()
        })?;
    STORE
        .get_or_init(|| {
            JobStore::open_in_dir(Path::new(persist_dir))
                .map(Arc::new)
                .map_err(|_| "analytics job projection is unavailable".to_string())
        })
        .clone()
}

#[cfg(feature = "raft")]
fn valid_publication_scope(value: &str) -> bool {
    value.rsplit_once(':').is_some_and(|(namespace, digest)| {
        !namespace.is_empty()
            && digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

#[cfg(feature = "raft")]
impl PreparedJobPublication {
    fn validate(&self) -> Result<(), String> {
        let coordinator_material = format!(
            "{}\0{}\0{}\0{}",
            self.job_id, self.worker_ref, self.lease_epoch, self.result_ref
        );
        let expected_coordinator = crate::server::mutation_batch::opaque_coordinator_key(
            "job-publication",
            &self.target_graph,
            &coordinator_material,
        );
        let batch_material = format!("{}\0{}", self.result_ref, self.job_id);
        let expected_batch = crate::server::mutation_batch::opaque_coordinator_key(
            "job-result",
            &self.target_graph,
            &batch_material,
        );
        if self.schema_version != JOB_PUBLICATION_PLAN_VERSION {
            return Err("unsupported job publication plan version".to_string());
        }
        if self.target_graph.is_empty()
            || self.target_graph.len() > 4_096
            || self.target_graph.chars().any(char::is_control)
            || self.job_id.is_empty()
            || self.job_id.len() > 256
            || self.job_id.chars().any(char::is_control)
            || self.lease_epoch == 0
            || !valid_publication_scope(&self.coordinator_id)
            || !valid_publication_scope(&self.worker_ref)
            || !valid_publication_scope(&self.principal_ref)
            || !valid_publication_scope(&self.batch_id)
            || self.coordinator_id != expected_coordinator
            || self.batch_id != expected_batch
            || !is_opaque_result_ref(&self.result_ref)
            || !is_opaque_result_ref(&self.dataset_ref)
            || self.claim_id != eg_jobs::claim::claim_node_id(&self.result_ref)
            || self.methods.is_empty()
            || self.methods.len() > 64
            || self
                .methods
                .iter()
                .any(|method| !matches!(method, Method::AddNode { .. } | Method::AddEdge { .. }))
        {
            return Err("job publication plan is invalid".to_string());
        }
        let bytes = rmp_serde::to_vec_named(self).map_err(|error| error.to_string())?;
        if bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
            return Err("job publication plan exceeds resource limits".to_string());
        }
        Ok(())
    }
}

#[cfg(feature = "raft")]
impl RoutedJobPublication {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != JOB_PUBLICATION_PLAN_VERSION {
            return Err("unsupported routed job publication plan version".to_string());
        }
        self.prepared.validate()?;
        if self
            .fencing_token
            .is_some_and(|token| token != self.group_id)
        {
            return Err("job publication placement fence is invalid".to_string());
        }
        Ok(())
    }
}

#[cfg(feature = "raft")]
impl FinalizeJobPublication {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != JOB_PUBLICATION_PLAN_VERSION
            || !valid_publication_scope(&self.coordinator_id)
            || self.job_id.is_empty()
            || self.job_id.len() > 256
            || !valid_publication_scope(&self.worker_ref)
            || self.lease_epoch == 0
            || !is_opaque_result_ref(&self.result_ref)
        {
            return Err("job publication finalize receipt is invalid".to_string());
        }
        Ok(())
    }
}

fn parse_algorithm(name: &str) -> Result<Algorithm, String> {
    match name.to_ascii_lowercase().as_str() {
        "apriori" => Ok(Algorithm::Apriori),
        "fpgrowth" | "fp-growth" | "fp_growth" => Ok(Algorithm::FpGrowth),
        "eclat" => Ok(Algorithm::Eclat),
        other => Err(format!(
            "unknown MineAssociate job algorithm '{other}' (expected apriori|fpgrowth|eclat)"
        )),
    }
}

#[cfg(feature = "program-optimization")]
fn verified_program_policy(
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

/// Handle `Method::AnalyticsJob { op }` (CONCEPT:INT-P2-1). Self-contained: resolves
/// its own `JobStore` + (for `Submit`/`Resume`) the target `GraphCore` off `state`,
/// so the dispatch shell can call this directly with no per-graph routing.
pub(crate) async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    verified_worker_context: bool,
    op: JobOp,
) -> Response {
    let caller = Some(authority.actor_scope());
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
    let store = match job_store(persist_dir.as_deref()) {
        Ok(store) => store,
        Err(error) => return Response::err(req_id, error),
    };
    // Colocated workers mutate the native scheduler directly. In clustered mode
    // all scheduler transitions must arrive as authenticated Worker* commands and
    // cross Raft, so automatic per-replica workers stay disabled.
    if !clustered {
        ensure_job_workers(state.clone(), store.clone());
    }

    let response = match op {
        JobOp::Submit(spec) => {
            let method = Method::AnalyticsJob {
                op: JobOp::Submit(spec.clone()),
            };
            let (batch, now) = match compile_job_batch(&store, req_id, authority, &method) {
                Ok(value) => value,
                Err(error) => return Response::err(req_id, error),
            };
            handle_submit(state, &store, req_id, authority, spec, batch, now).await
        }
        JobOp::Status { job_id } => match owned_job(&store, authority, &job_id) {
            Ok(job) => job_response(req_id, &job),
            Err(e) => Response::err(req_id, e),
        },
        JobOp::Cancel { job_id } => {
            if let Err(error) = owned_job(&store, authority, &job_id) {
                return Response::err(req_id, error);
            }
            let method = Method::AnalyticsJob {
                op: JobOp::Cancel {
                    job_id: job_id.clone(),
                },
            };
            let (batch, now) = match compile_job_batch(&store, req_id, authority, &method) {
                Ok(value) => value,
                Err(error) => return Response::err(req_id, error),
            };
            match store.request_cancel_batch(&job_id, &batch, now) {
                Ok((job, _)) => job_response(req_id, &job),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        JobOp::Resume { job_id } => {
            if let Err(error) = owned_job(&store, authority, &job_id) {
                return Response::err(req_id, error);
            }
            let method = Method::AnalyticsJob {
                op: JobOp::Resume {
                    job_id: job_id.clone(),
                },
            };
            let (batch, now) = match compile_job_batch(&store, req_id, authority, &method) {
                Ok(value) => value,
                Err(error) => return Response::err(req_id, error),
            };
            handle_resume(state, &store, req_id, &job_id, batch, now).await
        }
        JobOp::WorkerClaim {
            worker_instance,
            capabilities,
            lease_ms,
        } => handle_worker_claim(
            &store,
            req_id,
            caller,
            verified_worker_context,
            &worker_instance,
            capabilities,
            lease_ms,
        ),
        JobOp::WorkerRenew {
            job_id,
            worker_instance,
            lease_epoch,
            lease_ms,
        } => handle_worker_renew(
            &store,
            WorkerRequestCtx {
                req_id,
                caller,
                verified_worker_context,
                worker_instance: &worker_instance,
            },
            &job_id,
            lease_epoch,
            lease_ms,
        ),
        JobOp::WorkerCheckpoint {
            job_id,
            worker_instance,
            lease_epoch,
            progress,
            stage,
            state_ref,
        } => handle_worker_checkpoint(
            &store,
            req_id,
            caller,
            verified_worker_context,
            &worker_instance,
            &job_id,
            lease_epoch,
            progress,
            stage,
            state_ref,
        ),
        JobOp::WorkerStage {
            job_id,
            worker_instance,
            lease_epoch,
            result,
        } => handle_worker_stage(
            &store,
            req_id,
            caller,
            verified_worker_context,
            &worker_instance,
            &job_id,
            lease_epoch,
            result,
        ),
        JobOp::WorkerPublish {
            job_id,
            worker_instance,
            lease_epoch,
        } => {
            handle_worker_publish(
                state,
                &store,
                WorkerRequestCtx {
                    req_id,
                    caller,
                    verified_worker_context,
                    worker_instance: &worker_instance,
                },
                &job_id,
                lease_epoch,
            )
            .await
        }
        JobOp::WorkerCancel {
            job_id,
            worker_instance,
            lease_epoch,
        } => handle_worker_cancel(
            &store,
            req_id,
            caller,
            verified_worker_context,
            &worker_instance,
            &job_id,
            lease_epoch,
        ),
        JobOp::WorkerFail {
            job_id,
            worker_instance,
            lease_epoch,
            reason_code,
        } => handle_worker_fail(
            &store,
            req_id,
            caller,
            verified_worker_context,
            &worker_instance,
            &job_id,
            lease_epoch,
            &reason_code,
        ),
    };
    refresh_job_metrics(&store);
    response
}

fn owned_job(
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

fn compile_job_batch(
    store: &JobStore,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
) -> Result<(MutationBatch, u64), String> {
    let scope = authority.namespace("analytics-jobs", "control");
    let expected = store
        .mutation_version(authority.tenant_scope(), &scope)
        .map_err(|error| error.to_string())?;
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("analytics-job", &scope, req_id, method);
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: req_id,
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
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
        MutationDomain::AnalyticsJob,
        "analytics_job_operation",
    )?;
    Ok((batch, now))
}

fn job_response(req_id: u64, job: &eg_jobs::AnalyticsJob) -> Response {
    match job_result_payload(job) {
        Ok(result) => Response::ok(req_id, result),
        Err(e) => Response::err(req_id, format!("job serialization failed: {e}")),
    }
}

fn job_result_payload(job: &eg_jobs::AnalyticsJob) -> Result<ResultPayload, String> {
    let mut value = serde_json::to_value(job).map_err(|error| error.to_string())?;
    // Executor payloads are durable implementation detail. Even governed,
    // pseudonymized inputs are not echoed through status responses.
    if let Some(object) = value.as_object_mut() {
        object.remove("input_payload");
    }
    Ok(ResultPayload::Json(value))
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

/// The verified worker-identity fields shared by the `handle_worker_*` request
/// handlers, bundled so functions with several additional parameters of their
/// own (e.g. `handle_worker_renew`, `handle_worker_publish`) stay under the
/// clippy argument-count ceiling.
struct WorkerRequestCtx<'a> {
    req_id: u64,
    caller: Option<&'a str>,
    verified_worker_context: bool,
    worker_instance: &'a str,
}

fn worker_ref(
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

fn bounded_lease_ms(value: u64) -> u64 {
    value.clamp(1_000, 300_000)
}

fn tenant_worker_quota() -> TenantJobQuota {
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

fn worker_claim_response(req_id: u64, claim: &WorkerClaim) -> Response {
    match (
        serde_json::to_value(&claim.job),
        serde_json::to_value(&claim.lease),
    ) {
        (Ok(job), Ok(lease)) => Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({"job": job, "lease": lease})),
        ),
        _ => Response::err(req_id, "worker claim serialization failed"),
    }
}

fn handle_worker_claim(
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
        Ok(None) => Response::ok(req_id, ResultPayload::Json(serde_json::Value::Null)),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

fn handle_worker_renew(
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
        Ok(lease) => match serde_json::to_value(lease) {
            Ok(value) => Response::ok(req_id, ResultPayload::Json(value)),
            Err(_) => Response::err(req_id, "worker lease serialization failed"),
        },
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_worker_checkpoint(
    store: &JobStore,
    req_id: u64,
    caller: Option<&str>,
    verified_worker_context: bool,
    worker_instance: &str,
    job_id: &str,
    lease_epoch: u64,
    progress: f64,
    stage: String,
    state_ref: Option<String>,
) -> Response {
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
        Ok(job) => job_response(req_id, &job),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

fn typed_result_from_wire(result: JobResult) -> TypedJobResult {
    TypedJobResult {
        schema_version: result.schema_version,
        dataset_ref: result.dataset_ref,
        content_digest: result.content_digest,
        schema: result
            .schema
            .into_iter()
            .map(|column| ResultColumn {
                name: column.name,
                logical_type: column.logical_type,
                nullable: column.nullable,
            })
            .collect(),
        rows: result.rows,
        evidence_refs: result.evidence_refs,
        counterexample_refs: result.counterexample_refs,
        uncertainty: result.uncertainty,
        calibration: result.calibration,
        reproducibility: ReproducibilityManifest {
            input_dataset_ref: result.reproducibility.input_dataset_ref,
            input_content_digest: result.reproducibility.input_content_digest,
            input_snapshot_version: result.reproducibility.input_snapshot_version,
            algorithm_ref: result.reproducibility.algorithm_ref,
            params_digest: result.reproducibility.params_digest,
            implementation_version: result.reproducibility.implementation_version,
            environment_version: result.reproducibility.environment_version,
            policy_fingerprint: result.reproducibility.policy_fingerprint,
        },
    }
}

fn is_opaque_result_ref(value: &str) -> bool {
    let mut parts = value.split(':');
    let scheme = parts.next();
    let namespace = parts.next();
    let digest = parts.next();
    scheme == Some("eg")
        && namespace.is_some_and(|value| {
            !value.is_empty()
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        })
        && digest.is_some_and(|value| {
            matches!(value.len(), 32 | 64)
                && value.chars().all(|character| character.is_ascii_hexdigit())
        })
        && parts.next().is_none()
}

fn json_refs(value: Option<&serde_json::Value>, allow_empty: bool) -> bool {
    value
        .and_then(serde_json::Value::as_array)
        .is_some_and(|values| {
            (allow_empty || !values.is_empty())
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(is_opaque_result_ref))
        })
}

fn association_rule_id(
    antecedent: &[String],
    consequent: &[String],
    support: f64,
    confidence: f64,
    lift: f64,
) -> String {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    digest.update(b"eg-jobs.association-rule.v1\0");
    for values in [antecedent, consequent] {
        digest.update((values.len() as u64).to_le_bytes());
        for value in values {
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
    }
    for value in [support, confidence, lift] {
        digest.update(value.to_bits().to_le_bytes());
    }
    format!("eg:rule:{}", hex::encode(digest.finalize()))
}

fn association_rule_id_from_row(row: &BTreeMap<String, serde_json::Value>) -> Option<String> {
    let strings = |field: &str| {
        row.get(field)?
            .as_array()?
            .iter()
            .map(|value| value.as_str().map(str::to_string))
            .collect::<Option<Vec<_>>>()
    };
    Some(association_rule_id(
        &strings("antecedent")?,
        &strings("consequent")?,
        row.get("support")?.as_f64()?,
        row.get("confidence")?.as_f64()?,
        row.get("lift")?.as_f64()?,
    ))
}

/// The only shipped remote kernel is association mining. Fail closed on extra
/// free-text fields so a compromised worker cannot use result rows as a durable
/// PII, prompt, endpoint, or local-path channel.
fn validate_remote_result_privacy(result: &TypedJobResult) -> Result<(), String> {
    let expected = std::collections::BTreeSet::from([
        "id",
        "kind",
        "confidence",
        "evidence_refs",
        "source_refs",
        "proof_ids",
        "contradiction_ids",
        "antecedent",
        "consequent",
        "support",
        "lift",
    ]);
    let actual = result
        .schema
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let valid_columns = result.schema.iter().all(|column| {
        !column.nullable
            && matches!(
                (column.name.as_str(), column.logical_type.as_str()),
                ("id" | "kind", "string")
                    | ("confidence" | "support" | "lift", "float64")
                    | (
                        "evidence_refs"
                            | "source_refs"
                            | "proof_ids"
                            | "contradiction_ids"
                            | "antecedent"
                            | "consequent",
                        "list<string>"
                    )
            )
    });
    if actual != expected
        || !valid_columns
        || !result
            .evidence_refs
            .iter()
            .all(|value| is_opaque_result_ref(value))
        || !result
            .counterexample_refs
            .iter()
            .all(|value| is_opaque_result_ref(value))
    {
        return Err("remote analytics result schema/references are not governed".to_string());
    }
    for row in &result.rows {
        let expected_rule_id = association_rule_id_from_row(row);
        if row
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>()
            != expected
            || row.get("kind").and_then(serde_json::Value::as_str) != Some("association_rule")
            || !row
                .get("id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value.starts_with("eg:rule:") && is_opaque_result_ref(value))
            || row.get("id").and_then(serde_json::Value::as_str) != expected_rule_id.as_deref()
            || !json_refs(row.get("evidence_refs"), false)
            || !json_refs(row.get("source_refs"), false)
            || !json_refs(row.get("proof_ids"), true)
            || !json_refs(row.get("contradiction_ids"), true)
            || !json_refs(row.get("antecedent"), false)
            || !json_refs(row.get("consequent"), false)
            || !row
                .get("confidence")
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|value| (0.0..=1.0).contains(&value))
            || !row
                .get("support")
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|value| (0.0..=1.0).contains(&value))
            || !row
                .get("lift")
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|value| value.is_finite() && value >= 0.0)
        {
            return Err("remote analytics result contains non-governed row data".to_string());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_worker_stage(
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
    if matches!(&current.state, JobState::Succeeded { .. })
        && current.last_worker_ref == worker_ref
        && current.lease_epoch == lease_epoch
        && current.output.as_ref() == Some(&result)
    {
        return job_response(req_id, &current);
    }
    match store.stage_result_fenced(job_id, &worker_ref, lease_epoch, result, unix_ms()) {
        Ok(job) => job_response(req_id, &job),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

async fn handle_worker_publish(
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
    if matches!(&job.state, JobState::Succeeded { .. })
        && job.last_worker_ref == worker_ref
        && job.lease_epoch == lease_epoch
    {
        return job_response(req_id, &job);
    }
    let job = match store.verify_lease(job_id, &worker_ref, lease_epoch, unix_ms()) {
        Ok(job) if matches!(&job.state, JobState::Publishing { .. }) => job,
        Ok(job) => {
            return Response::err(
                req_id,
                format!(
                    "worker publication requires Publishing, got {}",
                    job.state.label()
                ),
            )
        }
        Err(error) => return Response::err(req_id, error.to_string()),
    };
    #[cfg(feature = "raft")]
    if crate::server::dispatch::is_replicated_apply() {
        return match prepare_consensus_job_publication(state, &job, &worker_ref, lease_epoch).await
        {
            Ok(prepared) => Response::ok(req_id, ResultPayload::Raw(prepared)),
            Err(error) => Response::err(req_id, error),
        };
    }
    match publish_staged_result(state, store, job, &worker_ref, lease_epoch).await {
        Ok(()) => match store.get(job_id) {
            Ok(job) => job_response(req_id, &job),
            Err(error) => Response::err(req_id, error.to_string()),
        },
        Err(error) => Response::err(req_id, error),
    }
}

fn handle_worker_cancel(
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
            return job_response(req_id, &job);
        }
    }
    match store.mark_cancelled_fenced(job_id, &worker_ref, lease_epoch, unix_ms()) {
        Ok(job) => job_response(req_id, &job),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_worker_fail(
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
            return job_response(req_id, &job);
        }
    }
    match store.fail_attempt_fenced(job_id, &worker_ref, lease_epoch, reason_code, unix_ms()) {
        Ok(job) => job_response(req_id, &job),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

async fn resolve_core(state: &Arc<RwLock<ServerState>>, graph: &str) -> Option<Arc<GraphCore>> {
    state
        .read()
        .await
        .registry
        .get(graph)
        .map(|e| e.core.clone())
}

async fn resolve_core_ref(
    state: &Arc<RwLock<ServerState>>,
    graph_ref: &str,
) -> Option<(String, crate::protocol::GraphType, Arc<GraphCore>)> {
    let s = state.read().await;
    resolve_opaque_graph_ref(&s.registry, graph_ref)
}

/// Process-wide `native_opaque_ref("graph", name) -> name` reverse index for
/// [`resolve_opaque_graph_ref`]. A small accessor (rather than an inline
/// function-local `static`, the pattern this file's own `job_store` and
/// `query.rs`'s `TENSOR_STORE` otherwise use) so the architecture test in
/// `jobs_read_rls_architecture.rs`/`resolve_core_ref_tests` below can exercise
/// cache-hit, cache-miss, and stale/poisoned-entry self-healing directly.
fn opaque_graph_ref_index() -> &'static std::sync::RwLock<HashMap<String, String>> {
    static INDEX: OnceLock<std::sync::RwLock<HashMap<String, String>>> = OnceLock::new();
    INDEX.get_or_init(|| std::sync::RwLock::new(HashMap::new()))
}

/// [`resolve_core_ref`]'s actual lookup, decoupled from `ServerState`/
/// `tokio::sync::RwLock` so it is directly unit-testable against a bare
/// `GraphRegistry`.
///
/// This used to be a straight `all_entries().into_iter().find(...)` — an
/// O(resident-graphs) SHA-256 digest + string-compare on EVERY call — and sits
/// on the job-publication hot path (`prepare_consensus_job_publication`/
/// `publish_staged_result` each call it once per completed job under the
/// `raft` cluster tier), so cost scaled with (jobs completed) x (resident
/// graphs). A cache HIT below costs one read-lock + one hashmap get + one
/// direct-by-name `registry.get` (already O(1)) — no hashing, no scan.
///
/// A cache MISS (the first-ever lookup for this digest, or a stale hit whose
/// cached name the live registry no longer backs with a matching digest)
/// falls back to the original full scan — unchanged worst-case cost — but
/// that scan now populates the index for EVERY entry it visits, not just the
/// match, so any of those OTHER resident graphs' next lookup is also O(1)
/// instead of paying its own O(n) scan later.
///
/// Correctness never depends on the cache being fresh: the entry returned
/// always comes from a LIVE `registry.get(&name)` call, and its digest is
/// re-verified against `graph_ref` before use, cache-hit or not — so a stale,
/// deleted, renamed, or even directly-poisoned cache entry can only ever cost
/// a wasted rescan, never resolve to the wrong graph (see
/// `resolve_core_ref_tests::a_poisoned_cache_entry_can_only_waste_a_rescan_never_resolve_the_wrong_graph`
/// and `..._a_deleted_graphs_stale_cache_entry_resolves_to_none_not_a_wrong_graph`).
fn resolve_opaque_graph_ref(
    registry: &crate::registry::GraphRegistry,
    graph_ref: &str,
) -> Option<(String, crate::protocol::GraphType, Arc<GraphCore>)> {
    let index = opaque_graph_ref_index();

    let cached_name = index
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(graph_ref)
        .cloned();
    if let Some(name) = cached_name {
        if let Some(entry) = registry.get(&name) {
            if native_opaque_ref("graph", &entry.name) == graph_ref {
                return Some((entry.name.clone(), entry.graph_type, entry.core.clone()));
            }
        }
        // Stale: the live registry disagrees with the cached digest (the
        // graph was deleted, or the cache entry was never trustworthy in the
        // first place) — fall through to the authoritative rescan rather than
        // returning `None` or the stale name outright.
    }

    let entries = registry.all_entries();
    let mut fresh = HashMap::with_capacity(entries.len());
    let mut found = None;
    for entry in entries {
        let opaque = native_opaque_ref("graph", &entry.name);
        if opaque == graph_ref {
            found = Some((entry.name.clone(), entry.graph_type, entry.core.clone()));
        }
        fresh.insert(opaque, entry.name.clone());
    }
    *index.write().unwrap_or_else(|e| e.into_inner()) = fresh;
    found
}

#[cfg(test)]
mod resolve_core_ref_tests {
    use super::*;
    use crate::protocol::GraphType;
    use crate::registry::GraphRegistry;

    /// A unique-enough name per call so parallel `cargo test` threads sharing
    /// the ONE process-wide `opaque_graph_ref_index()` singleton never collide
    /// on the same digest.
    fn unique_name(label: &str) -> String {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("w1b-resolve-core-ref-test-{label}-{n}-{nanos}")
    }

    #[test]
    fn resolves_every_resident_graph_by_its_opaque_ref_after_a_cold_scan() {
        let mut registry = GraphRegistry::new();
        let a = unique_name("a");
        let b = unique_name("b");
        registry.create_graph(&a, GraphType::Agent, None).unwrap();
        registry.create_graph(&b, GraphType::Team, None).unwrap();

        let ref_a = native_opaque_ref("graph", &a);
        let ref_b = native_opaque_ref("graph", &b);

        let (name, kind, _core) = resolve_opaque_graph_ref(&registry, &ref_a).unwrap();
        assert_eq!(name, a);
        assert_eq!(kind, GraphType::Agent);

        let (name, kind, _core) = resolve_opaque_graph_ref(&registry, &ref_b).unwrap();
        assert_eq!(name, b);
        assert_eq!(kind, GraphType::Team);

        assert!(resolve_opaque_graph_ref(&registry, "eg:graph:not-a-real-digest").is_none());
    }

    #[test]
    fn a_warm_cache_hit_still_returns_the_live_registry_entry() {
        let mut registry = GraphRegistry::new();
        let name = unique_name("warm");
        registry
            .create_graph(&name, GraphType::Agent, None)
            .unwrap();
        let graph_ref = native_opaque_ref("graph", &name);

        // Cold lookup: falls back to the full scan and populates the index.
        let first = resolve_opaque_graph_ref(&registry, &graph_ref).unwrap();
        assert_eq!(first.0, name);

        // Warm lookup: same digest, same registry -- must take the cache-hit
        // path and still resolve to the live entry.
        let second = resolve_opaque_graph_ref(&registry, &graph_ref).unwrap();
        assert_eq!(second.0, name);
        assert_eq!(second.1, GraphType::Agent);
    }

    #[test]
    fn a_poisoned_cache_entry_can_only_waste_a_rescan_never_resolve_the_wrong_graph() {
        let mut registry = GraphRegistry::new();
        let real_name = unique_name("real");
        registry
            .create_graph(&real_name, GraphType::Agent, None)
            .unwrap();
        let real_ref = native_opaque_ref("graph", &real_name);

        // Directly poison the shared process-wide index with a WRONG name for
        // this exact digest, simulating a stale or corrupted cache entry --
        // e.g. left over from a deleted graph whose name got reused for a
        // digest collision class this test forces by hand.
        opaque_graph_ref_index()
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(real_ref.clone(), "not-the-real-graph-name".to_string());

        // The live registry has no graph by that poisoned name, so the digest
        // re-verification must reject the cache hit and fall back to a fresh
        // scan -- resolving to the REAL graph, never the poisoned name and
        // never `None`.
        let resolved = resolve_opaque_graph_ref(&registry, &real_ref);
        assert_eq!(resolved.map(|(name, _, _)| name), Some(real_name));
    }

    #[test]
    fn a_deleted_graphs_stale_cache_entry_resolves_to_none_not_a_wrong_graph() {
        let mut registry = GraphRegistry::new();
        let name = unique_name("deleted");
        registry
            .create_graph(&name, GraphType::Agent, None)
            .unwrap();
        let graph_ref = native_opaque_ref("graph", &name);

        // Populate the cache while the graph is still live.
        assert!(resolve_opaque_graph_ref(&registry, &graph_ref).is_some());

        registry.delete_graph(&name).unwrap();

        // The index may still hold `graph_ref -> name`, but the live registry
        // no longer backs it -- must resolve to `None`, never a stale core.
        assert!(resolve_opaque_graph_ref(&registry, &graph_ref).is_none());
    }
}

#[cfg(test)]
#[path = "jobs_read_rls_architecture.rs"]
mod jobs_read_rls_architecture;

/// L-RLS-2 (§9 #10 next-level-analysis): does executing `kind` read graph
/// node/edge property data server-side, and therefore need its `GraphCore`
/// routed through [`GraphReadAuthority::project_core`]/`filter_view`
/// (`crate::server::access`, backed by
/// `crates/eg-core/src/isolation.rs::can_see_row`) before anything downstream
/// inspects a row?
///
/// Exhaustive match, deliberately NO wildcard arm: a new `JobKind` variant
/// that isn't given an arm here is a compile error (`E0004`), so a future
/// graph-reading job kind cannot silently ship without an explicit decision.
///
/// Both current kinds read ZERO graph node/edge data server-side:
/// - `MineAssociate` mines only the `transactions` the CALLER supplied inline
///   in the request — each item is opaque-ref-hashed under the caller's own
///   `owner_scope` before it ever reaches durable storage (see this file's
///   `handle_submit`, the `JobKind::MineAssociate` arm).
/// - `ProgramOptimize` submits an opaque `OptimizationRequest`; the actual
///   optimization work is claimed and executed later by a remote worker under
///   its OWN independently authenticated session (see the module doc's
///   "Distributed execution contract") — this handler never reads graph rows
///   on that worker's behalf.
///
/// `handle_submit` calls this BEFORE it does anything else with `kind`, and
/// fails closed (an error response, not a debug-only assert) if a future
/// variant is marked `true` here without also being wired through
/// `project_core` — see that call site.
fn reads_graph_rows_server_side(kind: &JobKind) -> bool {
    match kind {
        JobKind::MineAssociate { .. } => false,
        #[cfg(feature = "program-optimization")]
        JobKind::ProgramOptimize { .. } => false,
    }
}

#[cfg(test)]
mod read_rls_tests {
    use super::*;

    /// Locks in TODAY's classification for both currently-shipped kinds, so a
    /// future change that flips one to graph-row-reading is a visible,
    /// intentional diff here — not just a silent behavior change caught only
    /// by `reads_graph_rows_server_side`'s own compile-time exhaustiveness.
    #[test]
    fn no_shipped_job_kind_reads_graph_rows_server_side_today() {
        assert!(!reads_graph_rows_server_side(&JobKind::MineAssociate {
            transactions: vec![],
            min_support: 0.1,
            min_confidence: 0.5,
            algorithm: "fpgrowth".to_string(),
        }));
        #[cfg(feature = "program-optimization")]
        assert!(!reads_graph_rows_server_side(&JobKind::ProgramOptimize {
            request_msgpack: Vec::new(),
        }));
    }
}

async fn handle_submit(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    req_id: u64,
    authority: &CarrierAuthority,
    spec: SubmitJobSpec,
    batch: MutationBatch,
    committed_at_ms: u64,
) -> Response {
    let SubmitJobSpec {
        graph,
        tenant,
        actor,
        purpose,
        priority,
        deadline_unix_ms,
        quota_cpu_ms,
        memory_bytes,
        io_bytes,
        output_bytes,
        worker_pool,
        worker_region,
        mut required_capabilities,
        max_attempts,
        backoff_ms,
        kind,
    } = spec;

    let core = {
        let s = state.read().await;
        let Some(entry) = s.registry.get(&graph) else {
            return Response::err(req_id, format!("unknown graph '{graph}'"));
        };
        if let Err(error) = check_graph_access(
            &s.isolation,
            Some(authority.agent_id()),
            &graph,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        ) {
            return Response::err(req_id, error);
        }
        entry.core.clone()
    };
    // The immutable input-snapshot handle is stamped by the SERVER from the live
    // graph's OCC version, never accepted from the caller (CONCEPT:INT-P2-1: a
    // client cannot forge which graph-version a job ran against).
    let snapshot_version = core.version();

    // L-RLS-2 (§9 #10 next-level-analysis): fail closed, not merely document,
    // if a future JobKind is classified as graph-row-reading. No shipped kind
    // reaches this branch today (see `reads_graph_rows_server_side`'s own
    // exhaustive match); a kind that DOES need row data must be wired through
    // `GraphReadAuthority::project_core` before this point, then this function
    // updated to return `true` for it — never the reverse order.
    if reads_graph_rows_server_side(&kind) {
        return Response::err(
            req_id,
            "INTERNAL: this JobKind is classified as reading graph rows server-side, \
             but handle_submit has no per-row RLS projection wired for it yet",
        );
    }

    let invalid_placement_value =
        |value: &str| value.len() > 128 || value.chars().any(char::is_control);
    if invalid_placement_value(&worker_pool)
        || invalid_placement_value(&worker_region)
        || required_capabilities.len() > 64
        || required_capabilities
            .iter()
            .any(|value| value.is_empty() || invalid_placement_value(value))
    {
        return Response::err(req_id, "analytics job placement constraints are invalid");
    }
    let policy_fingerprint = batch
        .context
        .policy_fingerprint
        .clone()
        .unwrap_or_else(|| "policy:unversioned".to_string());
    let (governed_kind, algorithm_family, algorithm, params) = match kind {
        JobKind::MineAssociate {
            transactions,
            min_support,
            min_confidence,
            algorithm,
        } => {
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
                return Response::err(
                    req_id,
                    "association job thresholds or distinct-item cardinality are invalid",
                );
            }
            if let Err(error) = parse_algorithm(&algorithm) {
                return Response::err(req_id, error);
            }
            let params = serde_json::json!({
                "min_support": min_support,
                "min_confidence": min_confidence,
                "algorithm": algorithm.clone(),
            });
            (
                JobKind::MineAssociate {
                    transactions,
                    min_support,
                    min_confidence,
                    algorithm: algorithm.clone(),
                },
                "mining.association".to_string(),
                algorithm,
                params,
            )
        }
        #[cfg(feature = "program-optimization")]
        JobKind::ProgramOptimize { request_msgpack } => {
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
                    return Response::err(
                        req_id,
                        "program optimization request failed bounded decoding",
                    )
                }
            };
            let verified_policy =
                match verified_program_policy(authority, &policy_fingerprint, &purpose) {
                    Ok(policy) => policy,
                    Err(error) => return Response::err(req_id, error),
                };
            let request = match request.rebind_program_policy(verified_policy) {
                Ok(request) => request,
                Err(error) => {
                    return Response::err(
                        req_id,
                        format!("program optimization request is invalid: {error}"),
                    )
                }
            };
            let optimizer = request.optimizer.as_str().to_string();
            let execution = request.optimizer.execution().as_str().to_string();
            let request_msgpack = match rmp_serde::to_vec_named(&request) {
                Ok(payload) => payload,
                Err(error) => {
                    return Response::err(
                        req_id,
                        format!("program optimization encoding failed: {error}"),
                    )
                }
            };
            if !required_capabilities
                .iter()
                .any(|capability| capability == "program.optimization")
            {
                if required_capabilities.len() == 64 {
                    return Response::err(
                        req_id,
                        "analytics job has no room for its required native capability",
                    );
                }
                required_capabilities.push("program.optimization".to_string());
            }
            let params = serde_json::json!({
                "optimizer": optimizer,
                "execution": execution,
                "request_ref": request.request_ref.as_str(),
                "corpus_ref": request.corpus.corpus_ref.as_str(),
                "snapshot_version": request.corpus.snapshot_version,
            });
            (
                JobKind::ProgramOptimize { request_msgpack },
                "program.optimization".to_string(),
                optimizer,
                params,
            )
        }
    };
    let input_payload = match rmp_serde::to_vec_named(&governed_kind) {
        Ok(payload) => payload,
        Err(error) => return Response::err(req_id, format!("job input encoding failed: {error}")),
    };
    use sha2::{Digest, Sha256};
    let input_digest = hex::encode(Sha256::digest(&input_payload));
    let input_snapshot =
        InputSnapshotHandle::new(native_opaque_ref("graph", &graph), snapshot_version)
            .with_dataset(format!("eg:job_input:{input_digest}"), input_digest.clone());
    let params_digest = eg_jobs::digest_params(&serde_json::json!({
        "params": params,
        "input_content_digest": input_digest,
    }));

    // Persist only pseudonyms for identity/free-text policy fields. The verified
    // transport caller wins over the self-reported actor when present.
    // Body tenant/actor are descriptive, never authority. Persist only verified
    // tenant/principal ownership so Status/Cancel/Resume can enforce it exactly.
    let _ = (tenant, actor);
    let actor = authority.actor_scope().to_string();
    let tenant = authority.tenant_scope().to_string();
    let purpose =
        crate::server::mutation_batch::opaque_coordinator_key("job-purpose", "native", &purpose);
    let worker_pool = opaque_placement_value("job_pool", &worker_pool);
    let worker_region = opaque_placement_value("job_region", &worker_region);
    let required_capabilities = required_capabilities
        .iter()
        .map(|value| opaque_placement_value("job_capability", value))
        .collect();
    let submit_spec = SubmitSpec {
        input_snapshot,
        policy: JobPolicy {
            tenant,
            actor,
            purpose,
            priority,
            quota_cpu_ms,
            deadline_unix_ms,
            policy_fingerprint,
            resources: eg_jobs::ResourceBudget {
                cpu_ms: quota_cpu_ms,
                memory_bytes,
                io_bytes,
                output_bytes,
            },
            placement: eg_jobs::JobPlacement {
                pool: worker_pool,
                region: worker_region,
                required_capabilities,
            },
        },
        algo: AlgoVersion {
            family: algorithm_family,
            algorithm,
            params_digest,
            code_version: CODE_VERSION.to_string(),
            env_version: ENV_VERSION.to_string(),
        },
        input_payload: Some(input_payload),
        max_attempts,
        backoff_ms,
    };

    let (job, replayed) = match store.submit_batch(submit_spec, &batch, committed_at_ms) {
        Ok(value) => value,
        Err(e) => return Response::err(req_id, e.to_string()),
    };

    let _replayed = replayed;

    job_response(req_id, &job)
}

async fn handle_resume(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    req_id: u64,
    job_id: &str,
    batch: MutationBatch,
    committed_at_ms: u64,
) -> Response {
    let (job, replayed) = match store.resume_batch(job_id, &batch, committed_at_ms) {
        Ok(value) => value,
        Err(e) => return Response::err(req_id, e.to_string()),
    };

    let _ = (state, store);
    let _replayed = replayed;

    job_response(req_id, &job)
}

/// Start the optional bounded colocated executor pool. Setting the count to zero
/// leaves execution to authenticated remote workers using the coordinator ops.
fn ensure_job_workers(state: Arc<RwLock<ServerState>>, store: Arc<JobStore>) {
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

fn refresh_job_metrics(store: &JobStore) {
    if let Ok((ready, active, publishing)) = store.metric_counts(unix_ms()) {
        crate::metrics::set_analytics_job_counts(ready, active, publishing);
    }
}

async fn worker_loop(state: Arc<RwLock<ServerState>>, store: Arc<JobStore>, slot: usize) {
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

async fn execute_claim(
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

#[cfg(feature = "program-optimization")]
#[allow(clippy::too_many_arguments)]
async fn execute_program_claim(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    job: &eg_jobs::AnalyticsJob,
    worker_ref: &str,
    epoch: u64,
    payload: Vec<u8>,
    request_msgpack: Vec<u8>,
) -> Result<(), String> {
    let request: OptimizationRequest = eg_types::msgpack::decode_bounded(
        &request_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_INPUT_BYTES,
            MAX_JOB_INPUT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "program optimization input decoding failed".to_string())?;
    store
        .checkpoint_fenced(
            &job.job_id,
            worker_ref,
            epoch,
            Checkpoint {
                progress: 0.1,
                stage: "optimizing".to_string(),
                state_blob: Some(payload),
                updated_at_ms: unix_ms(),
            },
            unix_ms(),
        )
        .map_err(|error| error.to_string())?;

    let cancellation = Arc::new(AtomicBool::new(false));
    let kernel_token = cancellation.clone();
    let mut kernel = tokio::task::spawn_blocking(move || {
        NativeCompiler::compile_cancellable(&request, &kernel_token)
    });
    let began = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    let mut stop_reason: Option<&'static str> = None;
    let optimization = loop {
        tokio::select! {
            joined = &mut kernel => {
                match joined {
                    Ok(Ok(result)) if stop_reason.is_none() => break result,
                    Ok(Ok(_)) | Ok(Err(eg_program::ProgramError::Cancelled)) => {
                        match stop_reason {
                            Some("cancelled") => {
                                let _ = store.mark_cancelled_fenced(
                                    &job.job_id, worker_ref, epoch, unix_ms(),
                                );
                            }
                            Some(reason) => {
                                let _ = store.fail_attempt_fenced(
                                    &job.job_id, worker_ref, epoch, reason, unix_ms(),
                                );
                            }
                            None => {
                                let _ = store.fail_attempt_fenced(
                                    &job.job_id,
                                    worker_ref,
                                    epoch,
                                    "kernel_cancelled",
                                    unix_ms(),
                                );
                            }
                        }
                        return Ok(());
                    }
                    Ok(Err(_)) | Err(_) => {
                        let _ = store.fail_attempt_fenced(
                            &job.job_id, worker_ref, epoch, "kernel_failure", unix_ms(),
                        );
                        return Ok(());
                    }
                }
            }
            _ = ticker.tick() => {
                let now = unix_ms();
                let current = match store.get(&job.job_id) {
                    Ok(current) => current,
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
                    .renew_lease(&job.job_id, worker_ref, epoch, now, 60_000)
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
            &job.job_id,
            worker_ref,
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
    let result = typed_program_result(job, &optimization)?;
    let staged = store
        .stage_result_fenced(&job.job_id, worker_ref, epoch, result, unix_ms())
        .map_err(|error| error.to_string())?;
    publish_staged_result(state, store, staged, worker_ref, epoch).await
}

#[cfg(feature = "raft")]
async fn prepare_consensus_job_publication(
    state: &Arc<RwLock<ServerState>>,
    job: &eg_jobs::AnalyticsJob,
    worker_ref: &str,
    lease_epoch: u64,
) -> Result<Vec<u8>, String> {
    let (target_graph, target_graph_type, _core) =
        resolve_core_ref(state, &job.input_snapshot.graph)
            .await
            .ok_or_else(|| "analytics target graph is unavailable".to_string())?;
    let (confidence, calibration) = result_quality(job);
    let plan = eg_jobs::plan_result_claim(job, confidence, calibration)?;
    let result_ref = job.result_ref();
    let coordinator_material = format!(
        "{}\0{}\0{}\0{}",
        job.job_id, worker_ref, lease_epoch, result_ref
    );
    let coordinator_id = crate::server::mutation_batch::opaque_coordinator_key(
        "job-publication",
        &target_graph,
        &coordinator_material,
    );
    let batch_material = format!("{}\0{}", result_ref, job.job_id);
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "job-result",
        &target_graph,
        &batch_material,
    );
    let dataset_ref = job
        .output
        .as_ref()
        .map(|output| output.dataset_ref.clone())
        .ok_or_else(|| "staged analytics result is missing".to_string())?;
    let prepared = PreparedJobPublication {
        schema_version: JOB_PUBLICATION_PLAN_VERSION,
        coordinator_id,
        target_graph,
        target_graph_type,
        job_id: job.job_id.clone(),
        worker_ref: worker_ref.to_string(),
        lease_epoch,
        result_ref,
        principal_ref: job.policy.actor.clone(),
        batch_id,
        claim_id: plan.claim_id,
        dataset_ref,
        methods: plan.methods,
    };
    prepared.validate()?;
    rmp_serde::to_vec_named(&prepared).map_err(|error| error.to_string())
}

#[cfg(feature = "raft")]
pub(crate) fn decode_prepared_job_publication(
    bytes: &[u8],
) -> Result<PreparedJobPublication, String> {
    if bytes.is_empty() || bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
        return Err("job publication prepare result exceeds resource limits".to_string());
    }
    let prepared: PreparedJobPublication = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_PUBLICATION_PLAN_BYTES,
            1_000_000,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "job publication prepare result is invalid".to_string())?;
    prepared.validate()?;
    Ok(prepared)
}

#[cfg(feature = "raft")]
pub(crate) fn build_job_publication_commands(
    prepared: PreparedJobPublication,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    prepared.validate()?;
    let finalize = FinalizeJobPublication {
        schema_version: JOB_PUBLICATION_PLAN_VERSION,
        coordinator_id: prepared.coordinator_id.clone(),
        job_id: prepared.job_id.clone(),
        worker_ref: prepared.worker_ref.clone(),
        lease_epoch: prepared.lease_epoch,
        result_ref: prepared.result_ref.clone(),
    };
    finalize.validate()?;
    let routed = RoutedJobPublication {
        schema_version: JOB_PUBLICATION_PLAN_VERSION,
        prepared,
        group_id,
        placement_epoch,
        fencing_token,
    };
    routed.validate()?;
    let commit = rmp_serde::to_vec_named(&routed).map_err(|error| error.to_string())?;
    let finalize = rmp_serde::to_vec_named(&finalize).map_err(|error| error.to_string())?;
    if commit.len() > MAX_JOB_PUBLICATION_PLAN_BYTES
        || finalize.len() > MAX_JOB_PUBLICATION_PLAN_BYTES
    {
        return Err("job publication command exceeds resource limits".to_string());
    }
    Ok((commit, finalize))
}

#[cfg(feature = "raft")]
fn decode_routed_job_publication(bytes: &[u8]) -> Result<RoutedJobPublication, String> {
    if bytes.is_empty() || bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
        return Err("job publication commit plan exceeds resource limits".to_string());
    }
    let plan: RoutedJobPublication = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_PUBLICATION_PLAN_BYTES,
            1_000_000,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "job publication commit plan is invalid".to_string())?;
    plan.validate()?;
    Ok(plan)
}

#[cfg(feature = "raft")]
fn decode_finalize_job_publication(bytes: &[u8]) -> Result<FinalizeJobPublication, String> {
    if bytes.is_empty() || bytes.len() > MAX_JOB_PUBLICATION_PLAN_BYTES {
        return Err("job publication finalize receipt exceeds resource limits".to_string());
    }
    let receipt: FinalizeJobPublication = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_JOB_PUBLICATION_PLAN_BYTES,
            64,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "job publication finalize receipt is invalid".to_string())?;
    receipt.validate()?;
    Ok(receipt)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_job_publication_commit(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    authority: &crate::raft::RaftMutationContext,
    applying_group: crate::raft::GroupId,
    expected_coordinator_id: &str,
    plan_bytes: &[u8],
) -> Result<bool, String> {
    let plan = decode_routed_job_publication(plan_bytes)?;
    if plan.prepared.coordinator_id != expected_coordinator_id
        || plan.group_id != applying_group
        || plan.placement_epoch != authority.placement_epoch
        || plan.fencing_token != authority.fencing_token
    {
        return Err("job publication commit reached the wrong authority".to_string());
    }
    let (multi, core, persistence) = {
        let current = state.read().await;
        let multi = current
            .multi_raft
            .clone()
            .ok_or_else(|| "job publication lost placement authority".to_string())?;
        let entry = current
            .registry
            .get(&plan.prepared.target_graph)
            .ok_or_else(|| "analytics target graph is unavailable".to_string())?;
        if entry.graph_type != plan.prepared.target_graph_type {
            return Err("analytics target graph type changed".to_string());
        }
        (multi, entry.core.clone(), current.persistence.clone())
    };
    let route = multi.route_graph(&plan.prepared.target_graph).await;
    if route.group != plan.group_id
        || route.epoch != plan.placement_epoch
        || route.placed.then_some(route.fencing_token()) != plan.fencing_token
    {
        return Err("job publication target placement changed".to_string());
    }
    let result = ResultPayload::Json(serde_json::json!({
        "claim_id": plan.prepared.claim_id.clone(),
        "dataset_ref": plan.prepared.dataset_ref.clone(),
    }));
    crate::server::mutation_batch::commit_internal_graph_methods(
        persistence.as_ref(),
        &core,
        request_id,
        Some(&plan.prepared.principal_ref),
        &plan.prepared.target_graph,
        &plan.prepared.batch_id,
        plan.prepared.methods,
        &result,
    )
    .await?;
    Ok(true)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_job_publication_finalize(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    expected_coordinator_id: &str,
    receipt_bytes: &[u8],
) -> Result<ResultPayload, String> {
    let receipt = decode_finalize_job_publication(receipt_bytes)?;
    if receipt.coordinator_id != expected_coordinator_id {
        return Err("job publication finalize coordinator changed".to_string());
    }
    let persist_dir = state.read().await.persist_dir.clone();
    let store = job_store(persist_dir.as_deref())?;
    let committed_at_ms = i64::try_from(committed_at_ms)
        .map_err(|_| "job publication timestamp exceeds resource limits".to_string())?;
    let job = store
        .complete_publication_prepared(
            &receipt.job_id,
            &receipt.worker_ref,
            receipt.lease_epoch,
            &receipt.result_ref,
            committed_at_ms,
        )
        .map_err(|error| error.to_string())?;
    job_result_payload(&job)
}

async fn publish_staged_result(
    state: &Arc<RwLock<ServerState>>,
    store: &JobStore,
    job: eg_jobs::AnalyticsJob,
    worker_ref: &str,
    epoch: u64,
) -> Result<(), String> {
    let (target_graph, _target_graph_type, core) =
        resolve_core_ref(state, &job.input_snapshot.graph)
            .await
            .ok_or_else(|| "analytics target graph is unavailable".to_string())?;
    let persistence = state.read().await.persistence.clone();
    let (confidence, calibration) = result_quality(&job);
    let plan = eg_jobs::plan_result_claim(&job, confidence, calibration)?;
    let coordinator = format!("{}\0{}", job.result_ref(), job.job_id);
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "job-result",
        &target_graph,
        &coordinator,
    );
    let dataset_ref = job
        .output
        .as_ref()
        .map(|output| output.dataset_ref.clone())
        .unwrap_or_default();
    let result = ResultPayload::Json(serde_json::json!({
        "claim_id": plan.claim_id.clone(),
        "dataset_ref": dataset_ref,
    }));
    crate::server::mutation_batch::commit_internal_graph_methods(
        persistence.as_ref(),
        &core,
        0,
        Some(&job.policy.actor),
        &target_graph,
        &batch_id,
        plan.methods,
        &result,
    )
    .await?;
    let completed = store
        .complete_publication_fenced(&job.job_id, worker_ref, epoch, unix_ms())
        .map_err(|error| error.to_string())?;
    let _ = completed;
    Ok(())
}

fn typed_association_result(
    job: &eg_jobs::AnalyticsJob,
    rules: &[association::LabeledRule],
) -> Result<TypedJobResult, String> {
    let rows = rules
        .iter()
        .map(|rule| {
            let id = association_rule_id(
                &rule.antecedent,
                &rule.consequent,
                rule.support,
                rule.confidence,
                rule.lift,
            );
            BTreeMap::from([
                ("id".to_string(), serde_json::json!(id)),
                ("kind".to_string(), serde_json::json!("association_rule")),
                ("confidence".to_string(), serde_json::json!(rule.confidence)),
                (
                    "evidence_refs".to_string(),
                    serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
                ),
                (
                    "source_refs".to_string(),
                    serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
                ),
                ("proof_ids".to_string(), serde_json::json!([])),
                ("contradiction_ids".to_string(), serde_json::json!([])),
                ("antecedent".to_string(), serde_json::json!(rule.antecedent)),
                ("consequent".to_string(), serde_json::json!(rule.consequent)),
                ("support".to_string(), serde_json::json!(rule.support)),
                ("lift".to_string(), serde_json::json!(rule.lift)),
            ])
        })
        .collect::<Vec<_>>();
    let scores = rules
        .iter()
        .map(|rule| rule.support * rule.confidence)
        .collect::<Vec<_>>();
    let calibration = (!scores.is_empty()).then(|| {
        (
            scores.iter().copied().fold(f64::INFINITY, f64::min),
            scores.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    });
    let result = TypedJobResult::new(
        [
            ("id", "string", false),
            ("kind", "string", false),
            ("confidence", "float64", false),
            ("evidence_refs", "list<string>", false),
            ("source_refs", "list<string>", false),
            ("proof_ids", "list<string>", false),
            ("contradiction_ids", "list<string>", false),
            ("antecedent", "list<string>", false),
            ("consequent", "list<string>", false),
            ("support", "float64", false),
            ("lift", "float64", false),
        ]
        .into_iter()
        .map(|(name, logical_type, nullable)| ResultColumn {
            name: name.to_string(),
            logical_type: logical_type.to_string(),
            nullable,
        })
        .collect(),
        rows,
        vec![job.input_snapshot.dataset_ref.clone()],
        Vec::new(),
        (!rules.is_empty()).then(|| {
            rules.iter().map(|rule| 1.0 - rule.confidence).sum::<f64>() / rules.len() as f64
        }),
        calibration,
        ReproducibilityManifest {
            input_dataset_ref: job.input_snapshot.dataset_ref.clone(),
            input_content_digest: job.input_snapshot.content_digest.clone(),
            input_snapshot_version: job.input_snapshot.version,
            algorithm_ref: format!("{}:{}", job.algo.family, job.algo.algorithm),
            params_digest: job.algo.params_digest.clone(),
            implementation_version: job.algo.code_version.clone(),
            environment_version: job.algo.env_version.clone(),
            policy_fingerprint: job.policy.policy_fingerprint.clone(),
        },
    )?;
    #[cfg(feature = "knowledge-batch")]
    validate_native_job_result(job, &result)?;
    Ok(result)
}

#[cfg(feature = "program-optimization")]
fn typed_program_result(
    job: &eg_jobs::AnalyticsJob,
    optimization: &eg_program::OptimizationResult,
) -> Result<TypedJobResult, String> {
    let mut rows = optimization
        .candidates
        .iter()
        .map(|candidate| {
            let evidence_refs = candidate
                .evaluation
                .as_ref()
                .map(|evaluation| {
                    evaluation
                        .evidence_refs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
                .filter(|references| !references.is_empty())
                .unwrap_or_else(|| vec![job.input_snapshot.dataset_ref.clone()]);
            let confidence = candidate
                .evaluation
                .as_ref()
                .map(|evaluation| evaluation.aggregate_score)
                .unwrap_or(0.0);
            BTreeMap::from([
                (
                    "id".to_string(),
                    serde_json::json!(candidate.candidate_ref.as_str()),
                ),
                ("kind".to_string(), serde_json::json!("program_candidate")),
                ("confidence".to_string(), serde_json::json!(confidence)),
                (
                    "evidence_refs".to_string(),
                    serde_json::json!(evidence_refs),
                ),
                (
                    "source_refs".to_string(),
                    serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
                ),
                ("proof_ids".to_string(), serde_json::json!([])),
                ("contradiction_ids".to_string(), serde_json::json!([])),
                (
                    "program_ref".to_string(),
                    serde_json::json!(candidate.program_ref.as_str()),
                ),
                (
                    "optimizer".to_string(),
                    serde_json::json!(candidate.optimizer.as_str()),
                ),
                (
                    "execution".to_string(),
                    serde_json::json!(candidate.optimizer.execution().as_str()),
                ),
                (
                    "candidate_role".to_string(),
                    serde_json::json!(candidate.role.as_str()),
                ),
                (
                    "demonstration_refs".to_string(),
                    serde_json::json!(candidate
                        .demonstration_refs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()),
                ),
                (
                    "artifact_refs".to_string(),
                    serde_json::json!(candidate
                        .artifact_refs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()),
                ),
                (
                    "composition_refs".to_string(),
                    serde_json::json!(candidate
                        .composition_refs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()),
                ),
                (
                    "instruction_ref".to_string(),
                    candidate
                        .instruction_ref
                        .as_ref()
                        .map_or(serde_json::Value::Null, |reference| {
                            serde_json::json!(reference.as_str())
                        }),
                ),
                (
                    "tool_policy_ref".to_string(),
                    candidate
                        .tool_policy_ref
                        .as_ref()
                        .map_or(serde_json::Value::Null, |reference| {
                            serde_json::json!(reference.as_str())
                        }),
                ),
                (
                    "model_profile_ref".to_string(),
                    candidate
                        .model_profile_ref
                        .as_ref()
                        .map_or(serde_json::Value::Null, |reference| {
                            serde_json::json!(reference.as_str())
                        }),
                ),
                (
                    "modalities".to_string(),
                    serde_json::json!(candidate
                        .modalities
                        .iter()
                        .map(|modality| modality.as_str())
                        .collect::<Vec<_>>()),
                ),
                ("plan_ref".to_string(), serde_json::Value::Null),
                ("plan_step_kinds".to_string(), serde_json::json!([])),
                ("plan_executors".to_string(), serde_json::json!([])),
                ("plan_input_refs".to_string(), serde_json::json!([])),
                ("plan_output_refs".to_string(), serde_json::json!([])),
                ("plan_depends_on".to_string(), serde_json::json!([])),
                ("max_operations".to_string(), serde_json::Value::Null),
                (
                    "selected".to_string(),
                    serde_json::json!(
                        optimization.selected_candidate_ref.as_ref()
                            == Some(&candidate.candidate_ref)
                    ),
                ),
            ])
        })
        .collect::<Vec<_>>();
    rows.extend(optimization.plans.iter().flat_map(|plan| {
        plan.steps.iter().map(move |step| {
            BTreeMap::from([
                ("id".to_string(), serde_json::json!(step.step_ref.as_str())),
                (
                    "kind".to_string(),
                    serde_json::json!("program_optimization_plan_step"),
                ),
                ("confidence".to_string(), serde_json::json!(0.0)),
                (
                    "evidence_refs".to_string(),
                    serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
                ),
                (
                    "source_refs".to_string(),
                    serde_json::json!([job.input_snapshot.dataset_ref.clone()]),
                ),
                ("proof_ids".to_string(), serde_json::json!([])),
                ("contradiction_ids".to_string(), serde_json::json!([])),
                (
                    "program_ref".to_string(),
                    serde_json::json!(optimization.program_ref.as_str()),
                ),
                (
                    "optimizer".to_string(),
                    serde_json::json!(plan.optimizer.as_str()),
                ),
                (
                    "execution".to_string(),
                    serde_json::json!(plan.optimizer.execution().as_str()),
                ),
                ("candidate_role".to_string(), serde_json::Value::Null),
                ("demonstration_refs".to_string(), serde_json::json!([])),
                ("artifact_refs".to_string(), serde_json::json!([])),
                ("composition_refs".to_string(), serde_json::json!([])),
                ("instruction_ref".to_string(), serde_json::Value::Null),
                ("tool_policy_ref".to_string(), serde_json::Value::Null),
                ("model_profile_ref".to_string(), serde_json::Value::Null),
                (
                    "modalities".to_string(),
                    serde_json::json!(step
                        .modalities
                        .iter()
                        .map(|modality| modality.as_str())
                        .collect::<Vec<_>>()),
                ),
                (
                    "plan_ref".to_string(),
                    serde_json::json!(plan.plan_ref.as_str()),
                ),
                (
                    "plan_step_kinds".to_string(),
                    serde_json::json!([step.kind.as_str()]),
                ),
                (
                    "plan_executors".to_string(),
                    serde_json::json!([step.executor.as_str()]),
                ),
                (
                    "plan_input_refs".to_string(),
                    serde_json::json!(step
                        .input_refs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()),
                ),
                (
                    "plan_output_refs".to_string(),
                    serde_json::json!(step
                        .output_refs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()),
                ),
                (
                    "plan_depends_on".to_string(),
                    serde_json::json!(step
                        .depends_on
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()),
                ),
                (
                    "max_operations".to_string(),
                    serde_json::json!(step.max_operations),
                ),
                ("selected".to_string(), serde_json::json!(false)),
            ])
        })
    }));
    let evaluated_scores = optimization
        .candidates
        .iter()
        .filter_map(|candidate| {
            candidate
                .evaluation
                .as_ref()
                .map(|evaluation| evaluation.aggregate_score)
        })
        .collect::<Vec<_>>();
    let calibration = (!evaluated_scores.is_empty()).then(|| {
        (
            evaluated_scores
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min),
            evaluated_scores
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max),
        )
    });
    let result = TypedJobResult::new(
        [
            ("id", "string", false),
            ("kind", "string", false),
            ("confidence", "float64", false),
            ("evidence_refs", "list<string>", false),
            ("source_refs", "list<string>", false),
            ("proof_ids", "list<string>", false),
            ("contradiction_ids", "list<string>", false),
            ("program_ref", "string", false),
            ("optimizer", "string", false),
            ("execution", "string", false),
            ("candidate_role", "string", true),
            ("demonstration_refs", "list<string>", false),
            ("artifact_refs", "list<string>", false),
            ("composition_refs", "list<string>", false),
            ("instruction_ref", "string", true),
            ("tool_policy_ref", "string", true),
            ("model_profile_ref", "string", true),
            ("modalities", "list<string>", false),
            ("plan_ref", "string", true),
            ("plan_step_kinds", "list<string>", false),
            ("plan_executors", "list<string>", false),
            ("plan_input_refs", "list<string>", false),
            ("plan_output_refs", "list<string>", false),
            ("plan_depends_on", "list<string>", false),
            ("max_operations", "uint64", true),
            ("selected", "bool", false),
        ]
        .into_iter()
        .map(|(name, logical_type, nullable)| ResultColumn {
            name: name.to_string(),
            logical_type: logical_type.to_string(),
            nullable,
        })
        .collect(),
        rows,
        vec![job.input_snapshot.dataset_ref.clone()],
        Vec::new(),
        (!evaluated_scores.is_empty()).then(|| {
            evaluated_scores
                .iter()
                .map(|score| 1.0 - score)
                .sum::<f64>()
                / evaluated_scores.len() as f64
        }),
        calibration,
        ReproducibilityManifest {
            input_dataset_ref: job.input_snapshot.dataset_ref.clone(),
            input_content_digest: job.input_snapshot.content_digest.clone(),
            input_snapshot_version: job.input_snapshot.version,
            algorithm_ref: format!("{}:{}", job.algo.family, job.algo.algorithm),
            params_digest: job.algo.params_digest.clone(),
            implementation_version: job.algo.code_version.clone(),
            environment_version: job.algo.env_version.clone(),
            policy_fingerprint: job.policy.policy_fingerprint.clone(),
        },
    )?;
    validate_program_result_privacy(&result)?;
    #[cfg(feature = "knowledge-batch")]
    validate_native_job_result(job, &result)?;
    Ok(result)
}

#[cfg(feature = "program-optimization")]
fn validate_program_result_privacy(result: &TypedJobResult) -> Result<(), String> {
    let expected = std::collections::BTreeSet::from([
        "id",
        "kind",
        "confidence",
        "evidence_refs",
        "source_refs",
        "proof_ids",
        "contradiction_ids",
        "program_ref",
        "optimizer",
        "execution",
        "candidate_role",
        "demonstration_refs",
        "artifact_refs",
        "composition_refs",
        "instruction_ref",
        "tool_policy_ref",
        "model_profile_ref",
        "modalities",
        "plan_ref",
        "plan_step_kinds",
        "plan_executors",
        "plan_input_refs",
        "plan_output_refs",
        "plan_depends_on",
        "max_operations",
        "selected",
    ]);
    let actual = result
        .schema
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let governed_ref = |value: &str| OpaqueRef::new(value.to_string()).is_ok();
    let modalities = ProgramModality::ALL
        .into_iter()
        .map(ProgramModality::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let optimizers = eg_program::OptimizerKind::ALL
        .into_iter()
        .map(eg_program::OptimizerKind::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let executions = eg_program::OptimizerKind::ALL
        .into_iter()
        .map(|optimizer| optimizer.execution().as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let candidate_roles = ["proposal", "ensemble_member", "ensemble"]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let plan_step_kinds = eg_program::PlanStepKind::ALL
        .into_iter()
        .map(eg_program::PlanStepKind::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let plan_executors = eg_program::PlanExecutor::ALL
        .into_iter()
        .map(eg_program::PlanExecutor::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    if actual != expected
        || !result.evidence_refs.iter().all(|value| governed_ref(value))
        || !result
            .counterexample_refs
            .iter()
            .all(|value| governed_ref(value))
    {
        return Err("program result schema/references are not governed".to_string());
    }
    for row in &result.rows {
        let row_fields = row
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        let valid_modalities = row
            .get("modalities")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|values| {
                !values.is_empty()
                    && values.iter().all(|value| {
                        value
                            .as_str()
                            .is_some_and(|value| modalities.contains(value))
                    })
            });
        let valid_refs = |field: &str, allow_empty: bool| {
            row.get(field)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|values| {
                    (allow_empty || !values.is_empty())
                        && values
                            .iter()
                            .all(|value| value.as_str().is_some_and(&governed_ref))
                })
        };
        let valid_optional_ref = |field: &str| {
            row.get(field)
                .is_some_and(|value| value.is_null() || value.as_str().is_some_and(&governed_ref))
        };
        let empty_list = |field: &str| {
            row.get(field)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|values| values.is_empty())
        };
        let single_label = |field: &str, allowed: &std::collections::BTreeSet<&str>| {
            row.get(field)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|values| {
                    values.len() == 1
                        && values[0]
                            .as_str()
                            .is_some_and(|value| allowed.contains(value))
                })
        };
        let kind = row.get("kind").and_then(serde_json::Value::as_str);
        let selected = row.get("selected").and_then(serde_json::Value::as_bool);
        let candidate_role = row
            .get("candidate_role")
            .and_then(serde_json::Value::as_str);
        let valid_candidate_role = match candidate_role {
            Some("proposal") => empty_list("composition_refs"),
            Some("ensemble_member") => empty_list("composition_refs") && selected == Some(false),
            Some("ensemble") => valid_refs("composition_refs", false),
            _ => false,
        };
        let plan_step_kind = row
            .get("plan_step_kinds")
            .and_then(serde_json::Value::as_array)
            .and_then(|values| values.first())
            .and_then(serde_json::Value::as_str);
        let plan_executor = row
            .get("plan_executors")
            .and_then(serde_json::Value::as_array)
            .and_then(|values| values.first())
            .and_then(serde_json::Value::as_str);
        let valid_step_executor = matches!(
            (plan_step_kind, plan_executor),
            (Some("query_similarity"), Some("graph_similarity"))
                | (
                    Some(
                        "propose_instruction"
                            | "compare_tool_use"
                            | "propose_rules"
                            | "reflect_on_trace"
                            | "pareto_reflect"
                    ),
                    Some("model_transport")
                )
                | (Some("compose_programs"), Some("native_kernel"))
                | (Some("train_weights"), Some("trainer"))
                | (Some("evaluate_candidates"), Some("evaluator"))
        );
        let valid_tool_policy_binding =
            match row.get("optimizer").and_then(serde_json::Value::as_str) {
                Some("avatar") => {
                    row.get("tool_policy_ref")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|reference| {
                            governed_ref(reference)
                                && row
                                    .get("artifact_refs")
                                    .and_then(serde_json::Value::as_array)
                                    .is_some_and(|values| {
                                        values.iter().any(|value| value.as_str() == Some(reference))
                                    })
                        })
                        && row
                            .get("instruction_ref")
                            .is_some_and(serde_json::Value::is_null)
                }
                Some(_) => row
                    .get("tool_policy_ref")
                    .is_some_and(serde_json::Value::is_null),
                None => false,
            };
        let candidate_shape = kind == Some("program_candidate")
            && candidate_role.is_some_and(|value| candidate_roles.contains(value))
            && valid_candidate_role
            && valid_tool_policy_binding
            && row.get("plan_ref").is_some_and(serde_json::Value::is_null)
            && valid_refs("demonstration_refs", false)
            && valid_refs("artifact_refs", true)
            && valid_refs("composition_refs", true)
            && empty_list("plan_step_kinds")
            && empty_list("plan_executors")
            && empty_list("plan_input_refs")
            && empty_list("plan_output_refs")
            && empty_list("plan_depends_on")
            && row
                .get("max_operations")
                .is_some_and(serde_json::Value::is_null);
        let plan_shape = kind == Some("program_optimization_plan_step")
            && row
                .get("candidate_role")
                .is_some_and(serde_json::Value::is_null)
            && row
                .get("plan_ref")
                .and_then(serde_json::Value::as_str)
                .is_some_and(&governed_ref)
            && row
                .get("instruction_ref")
                .is_some_and(serde_json::Value::is_null)
            && row
                .get("tool_policy_ref")
                .is_some_and(serde_json::Value::is_null)
            && row
                .get("model_profile_ref")
                .is_some_and(serde_json::Value::is_null)
            && empty_list("demonstration_refs")
            && empty_list("artifact_refs")
            && empty_list("composition_refs")
            && single_label("plan_step_kinds", &plan_step_kinds)
            && single_label("plan_executors", &plan_executors)
            && valid_step_executor
            && valid_refs("plan_input_refs", false)
            && valid_refs("plan_output_refs", false)
            && valid_refs("plan_depends_on", true)
            && row
                .get("max_operations")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|value| value > 0)
            && selected == Some(false);
        if row_fields != expected
            || (!candidate_shape && !plan_shape)
            || !row
                .get("id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(&governed_ref)
            || !row
                .get("program_ref")
                .and_then(serde_json::Value::as_str)
                .is_some_and(&governed_ref)
            || !row
                .get("optimizer")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| optimizers.contains(value))
            || !row
                .get("execution")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| executions.contains(value))
            || !valid_optional_ref("instruction_ref")
            || !valid_optional_ref("tool_policy_ref")
            || !valid_optional_ref("model_profile_ref")
            || !row
                .get("confidence")
                .and_then(serde_json::Value::as_f64)
                .is_some_and(|value| (0.0..=1.0).contains(&value))
            || selected.is_none()
            || !valid_refs("evidence_refs", false)
            || !valid_refs("source_refs", false)
            || !valid_refs("proof_ids", true)
            || !valid_refs("contradiction_ids", true)
            || !valid_modalities
        {
            return Err("program result contains non-governed row data".to_string());
        }
    }
    Ok(())
}

#[cfg(feature = "knowledge-batch")]
fn validate_native_job_result(
    job: &eg_jobs::AnalyticsJob,
    result: &TypedJobResult,
) -> Result<(), String> {
    use eg_modality::{
        ArtifactId, DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId, OpaqueRef,
        ResourceId,
    };
    use eg_plan::{job_result_stream, KnowledgeBatchRow, KnowledgeStreamContext};

    let opaque = |namespace: &str, value: &str| {
        OpaqueRef::new(native_opaque_ref(namespace, value)).map_err(|error| error.to_string())
    };
    let rows = result
        .rows
        .iter()
        .map(|row| -> Result<KnowledgeBatchRow, String> {
            let id = row
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let locus = EvidenceLocus {
                id: EvidenceLocusId::from_opaque(opaque("locus", &format!("{}:{id}", job.job_id))?)
                    .map_err(|error| error.to_string())?,
                subject: ResourceId::Artifact(
                    ArtifactId::from_opaque(opaque("artifact", &job.input_snapshot.dataset_ref)?)
                        .map_err(|error| error.to_string())?,
                ),
                address: EvidenceAddress::RowVersion {
                    row_ref: opaque("row", &id)?,
                    version: job.input_snapshot.version,
                },
                policy_ref: opaque("policy", &job.policy.policy_fingerprint)?,
                derivation_ref: DerivationId::from_opaque(opaque("derivation", &job.result_ref())?)
                    .map_err(|error| error.to_string())?,
            };
            Ok(KnowledgeBatchRow {
                id: id.clone(),
                kind: row
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("analytics_result")
                    .to_string(),
                scores: vec![
                    (
                        "support".to_string(),
                        row.get("support")
                            .and_then(serde_json::Value::as_f64)
                            .map(|value| value as f32),
                    ),
                    (
                        "lift".to_string(),
                        row.get("lift")
                            .and_then(serde_json::Value::as_f64)
                            .map(|value| value as f32),
                    ),
                ],
                confidence: row
                    .get("confidence")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0),
                evidence_refs: vec![locus],
                source_refs: vec![job.input_snapshot.dataset_ref.clone()],
                proof_ids: json_string_list(row.get("proof_ids")),
                contradiction_ids: json_string_list(row.get("contradiction_ids")),
                ..KnowledgeBatchRow::default()
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let context = KnowledgeStreamContext {
        tenant_ref: opaque("tenant", &job.policy.tenant)?,
        access_policy_ref: opaque("policy", &job.policy.policy_fingerprint)?,
        placement_ref: opaque("placement", &job.input_snapshot.graph)?,
        snapshot_ref: OpaqueRef::new(job.input_snapshot.dataset_ref.clone())
            .map_err(|error| error.to_string())?,
        query_ref: opaque("query", &job.algo.params_digest)?,
        derivation_ref: opaque("derivation", &job.result_ref())?,
        evidence_set_ref: opaque("evidence_set", &result.dataset_ref)?,
    };
    let mut stream = job_result_stream(
        context,
        vec!["support".to_string(), "lift".to_string()],
        rows,
        256,
    )
    .map_err(|error| error.to_string())?;
    while stream
        .next_batch()
        .map_err(|error| error.to_string())?
        .is_some()
    {}
    Ok(())
}

#[cfg(feature = "knowledge-batch")]
fn json_string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn native_opaque_ref(namespace: &str, value: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "eg:{namespace}:{}",
        hex::encode(Sha256::digest(value.as_bytes()))
    )
}

fn opaque_placement_value(namespace: &str, value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return String::new();
    }
    let prefix = format!("eg:{namespace}:");
    if value.starts_with(&prefix) {
        return value.to_string();
    }
    native_opaque_ref(namespace, value)
}

fn opaque_worker_capability(value: &str) -> String {
    let value = value.trim();
    if let Some(pool) = value.strip_prefix("pool:") {
        return format!("pool:{}", opaque_placement_value("job_pool", pool));
    }
    if let Some(region) = value.strip_prefix("region:") {
        return format!("region:{}", opaque_placement_value("job_region", region));
    }
    opaque_placement_value("job_capability", value)
}

fn result_quality(job: &eg_jobs::AnalyticsJob) -> (f64, Option<eg_jobs::CalibrationInput>) {
    let Some(output) = &job.output else {
        return (0.0, None);
    };
    let scores = output
        .rows
        .iter()
        .filter_map(|row| {
            if row.get("kind").and_then(serde_json::Value::as_str)
                == Some("program_optimization_plan_step")
            {
                return None;
            }
            let confidence = row.get("confidence")?.as_f64()?;
            let support = row
                .get("support")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(1.0);
            Some(support * confidence)
        })
        .collect::<Vec<_>>();
    if scores.is_empty() {
        return (0.0, None);
    }
    let confidence = scores.iter().sum::<f64>() / scores.len() as f64;
    let interval = output.calibration.unwrap_or_else(|| {
        (
            scores.iter().copied().fold(f64::INFINITY, f64::min),
            scores.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    });
    (
        confidence,
        Some(eg_jobs::CalibrationInput {
            interval,
            level: 0.95,
            evidence_count: scores.len(),
        }),
    )
}

fn unix_ms() -> i64 {
    crate::server::dispatch::authoritative_now_ms() as i64
}

#[cfg(test)]
mod privacy_tests {
    use super::{native_opaque_ref, opaque_placement_value, opaque_worker_capability};

    #[test]
    fn job_placement_values_are_opaque_and_idempotent() {
        let capability = opaque_worker_capability("accelerator");
        let pool = opaque_worker_capability("pool:interactive");
        let region = opaque_worker_capability("region:zone-a");
        assert!(capability.starts_with("eg:job_capability:"));
        assert!(pool.starts_with("pool:eg:job_pool:"));
        assert!(region.starts_with("region:eg:job_region:"));
        assert!(!capability.contains("accelerator"));
        assert_eq!(
            opaque_placement_value("job_capability", &capability),
            capability
        );
    }

    #[test]
    fn authorized_graph_comparison_uses_the_persisted_graph_reference() {
        let stored = native_opaque_ref("graph", "authorized-graph");
        assert_eq!(stored, native_opaque_ref("graph", "authorized-graph"));
        assert_ne!(stored, "authorized-graph");
    }
}
