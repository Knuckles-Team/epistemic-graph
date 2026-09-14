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
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

use eg_core::graph::GraphCore;
use eg_jobs::model::InputSnapshotHandle;
use eg_jobs::store::{JobStore, SubmitSpec};
use eg_types::contract::Nonce;
use eg_types::jobs::{JobKind, JobOp, SubmitJobSpec};

#[cfg(feature = "program-optimization")]
use eg_modality::{OpaqueRef, PolicyEnvelope};
#[cfg(feature = "program-optimization")]
use eg_program::ProgramRevisionIdentity;

use crate::lock_recovery::{LockRecovery, WriteRecovery};
use crate::mutation_batch::MutationBatch;
use crate::protocol::Response;
use crate::server::access::CarrierAuthority;
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

mod core;
mod executor_claim;
mod executor_loop;
mod executor_program;
#[cfg(test)]
mod graph_tests;
mod helpers;
mod native_validation;
mod prelude_jobs;
mod prelude_server;
mod prelude_std;
mod program_validation;
mod publication;
mod results_association;
mod results_program;
mod submit;
mod worker_publish;
mod worker_requests;
mod worker_validation;

pub(crate) use core::knowledge_stream_result;
use core::{
    compile_job_batch, job_record, job_response, job_result_payload, job_store, owned_job,
    parse_algorithm,
};
use executor_claim::*;
use executor_loop::*;
use executor_program::*;
use helpers::*;
use native_validation::*;
use program_validation::*;
pub(crate) use publication::{
    apply_consensus_job_publication_commit, apply_consensus_job_publication_finalize,
    build_job_publication_commands, decode_prepared_job_publication,
    validate_job_publication_commit, PreparedJobPublication,
};
use publication::{prepare_consensus_job_publication, publish_staged_result};
use results_association::*;
use results_program::*;
use submit::*;
use worker_publish::*;
use worker_requests::*;
use worker_validation::*;

/// Handle `Method::AnalyticsJob { op }` (CONCEPT:INT-P2-1). Self-contained: resolves
/// its own `JobStore` + (for `Submit`/`Resume`) the target `GraphCore` off `state`,
/// so the dispatch shell can call this directly with no per-graph routing.
pub(crate) async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    verified_worker_context: bool,
    op: JobOp,
) -> Response {
    let caller = Some(authority.actor_scope());
    let store = match resolve_job_store(state, req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };

    let response = match op {
        JobOp::Submit(spec) => {
            handle_submit_op(state, &store, req_id, authority, attempt_nonce, spec).await
        }
        JobOp::Status { job_id } => handle_status_op(&store, req_id, authority, &job_id),
        JobOp::Cancel { job_id } => {
            handle_cancel_op(&store, req_id, authority, attempt_nonce, job_id)
        }
        JobOp::Resume { job_id } => {
            handle_resume_op(state, &store, req_id, authority, attempt_nonce, job_id).await
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
            WorkerRequestCtx {
                req_id,
                caller,
                verified_worker_context,
                worker_instance: &worker_instance,
            },
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

fn handle_status_op(
    store: &JobStore,
    req_id: u64,
    authority: &CarrierAuthority,
    job_id: &str,
) -> Response {
    match owned_job(store, authority, job_id) {
        Ok(job) => job_response::<eg_types::result_contract::coordination::JobStatus>(req_id, &job),
        Err(error) => Response::err(req_id, error),
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
        .lock_recovering("opaque graph-ref cache")
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
    *index.write_recovering("opaque graph-ref cache") = fresh;
    found
}

#[cfg(test)]
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
    let core = match resolve_submit_graph_core(state, req_id, authority, &spec.graph).await {
        Ok(core) => core,
        Err(response) => return response,
    };
    // L-RLS-2 (§9 #10 next-level-analysis): fail closed, not merely document,
    // if a future JobKind is classified as graph-row-reading. No shipped kind
    // reaches this branch today (see `reads_graph_rows_server_side`'s own
    // exhaustive match); a kind that DOES need row data must be wired through
    // `GraphReadAuthority::project_core` before this point, then this function
    // updated to return `true` for it — never the reverse order.
    //
    // `kind` is cloned here (rather than borrowed as `&spec.kind`) solely so
    // this guard's own source text names a bare `kind` local: the
    // `jobs_read_rls_architecture` textual architecture test scans this
    // function's source for the literal `reads_graph_rows_server_side(&kind)`
    // call to prove the fail-closed check runs before `kind` is otherwise
    // used. `spec` (including its own `kind` field) is still passed on to
    // `finalize_submit` unchanged below; this is a one-time, bounded-size
    // clone per Submit call, not a hot-path cost.
    let kind = spec.kind.clone();
    if reads_graph_rows_server_side(&kind) {
        return Response::err(
            req_id,
            "INTERNAL: this JobKind is classified as reading graph rows server-side, \
             but handle_submit has no per-row RLS projection wired for it yet",
        );
    }
    if submit_placement_invalid(
        &spec.worker_pool,
        &spec.worker_region,
        &spec.required_capabilities,
    ) {
        return Response::err(req_id, "analytics job placement constraints are invalid");
    }
    // The immutable input-snapshot handle is stamped by the SERVER from the live
    // graph's OCC version, never accepted from the caller (CONCEPT:INT-P2-1: a
    // client cannot forge which graph-version a job ran against).
    let snapshot_version = core.version();
    finalize_submit(
        store,
        req_id,
        authority,
        batch,
        committed_at_ms,
        snapshot_version,
        spec,
    )
    .await
}

#[cfg(feature = "program-optimization")]
fn staged_program_promotion(
    job: &eg_jobs::AnalyticsJob,
) -> Result<Option<ProgramRevisionIdentity>, String> {
    let output = job
        .output
        .as_ref()
        .ok_or_else(|| "program publication has no staged result".to_string())?;
    let mut selected = false;
    let mut identity = None;
    for row in &output.rows {
        let row_selected = row
            .get("selected")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let value = row
            .get("promotion_identity")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        if !row_selected {
            if !value.is_null() {
                return Err(
                    "program promotion identity is attached to an unselected row".to_string(),
                );
            }
            continue;
        }
        if selected {
            return Err("program result has multiple selected rows".to_string());
        }
        selected = true;
        if value.is_null() {
            return Err("selected program row is missing promotion identity".to_string());
        }
        identity = Some(staged_program_identity(row, value)?);
    }
    Ok(identity)
}

#[cfg(feature = "program-optimization")]
fn staged_program_identity(
    row: &BTreeMap<String, serde_json::Value>,
    value: serde_json::Value,
) -> Result<ProgramRevisionIdentity, String> {
    let parsed: ProgramRevisionIdentity = serde_json::from_value(value)
        .map_err(|error| format!("program promotion identity is invalid: {error}"))?;
    parsed
        .validate()
        .map_err(|error| format!("program promotion identity is invalid: {error}"))?;
    if row.get("id").and_then(serde_json::Value::as_str) != Some(parsed.candidate_ref.as_str())
        || row.get("program_ref").and_then(serde_json::Value::as_str)
            != Some(parsed.program_ref.as_str())
    {
        return Err("program promotion identity does not match its selected row".to_string());
    }
    let row_policy = row
        .get("policy")
        .cloned()
        .ok_or_else(|| "selected program row is missing policy binding".to_string())?;
    let row_policy: PolicyEnvelope = serde_json::from_value(row_policy)
        .map_err(|error| format!("selected program policy binding is invalid: {error}"))?;
    let row_tool_policy_ref = row
        .get("tool_policy_ref")
        .and_then(serde_json::Value::as_str);
    let row_model_profile_ref = row
        .get("model_profile_ref")
        .and_then(serde_json::Value::as_str);
    if row_policy != parsed.policy
        || row_tool_policy_ref != parsed.tool_policy_ref.as_ref().map(OpaqueRef::as_str)
        || row_model_profile_ref != parsed.model_profile_ref.as_ref().map(OpaqueRef::as_str)
    {
        return Err(
            "program promotion identity does not match its policy/model bindings".to_string(),
        );
    }
    Ok(parsed)
}
fn govern_job_kind(
    authority: &CarrierAuthority,
    req_id: u64,
    policy_fingerprint: &str,
    purpose: &str,
    kind: JobKind,
    required_capabilities: &mut Vec<String>,
) -> Result<(JobKind, String, String, serde_json::Value), Response> {
    match kind {
        JobKind::MineAssociate {
            transactions,
            min_support,
            min_confidence,
            algorithm,
        } => submit_job_mine_associate(
            authority,
            req_id,
            transactions,
            min_support,
            min_confidence,
            algorithm,
        ),
        #[cfg(feature = "program-optimization")]
        JobKind::ProgramOptimize { request_msgpack } => submit_job_program_optimize(
            authority,
            req_id,
            policy_fingerprint,
            purpose,
            request_msgpack,
            required_capabilities,
        ),
    }
}

/// The rest of a Submit, once the target graph/access and placement
/// constraints have checked out: govern the job kind, encode/hash the input,
/// build the durable SubmitSpec, and apply the batch.
async fn finalize_submit(
    store: &Arc<JobStore>,
    req_id: u64,
    authority: &CarrierAuthority,
    batch: MutationBatch,
    committed_at_ms: u64,
    snapshot_version: u64,
    spec: SubmitJobSpec,
) -> Response {
    let mut spec = spec;
    // The policy this job was admitted under. The envelope carries the real
    // revision, which is also inside the stable replay identity.
    let policy_fingerprint = batch
        .envelope
        .operation()
        .map(|operation| operation.authority.policy_revision.as_str().to_string())
        .unwrap_or_else(|| "policy:unversioned".to_string());
    let (governed_kind, algorithm_family, algorithm, params) = match govern_job_kind(
        authority,
        req_id,
        &policy_fingerprint,
        &spec.purpose,
        spec.kind.clone(),
        &mut spec.required_capabilities,
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input_payload = match encode_job_input(&governed_kind, req_id) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    use sha2::{Digest, Sha256};
    let input_digest = hex::encode(Sha256::digest(&input_payload));
    let input_snapshot =
        InputSnapshotHandle::new(native_opaque_ref("graph", &spec.graph), snapshot_version)
            .with_dataset(format!("eg:job_input:{input_digest}"), input_digest.clone());
    let params_digest = eg_jobs::digest_params(&serde_json::json!({
        "params": params,
        "input_content_digest": input_digest,
    }));
    let submit_spec = build_submit_spec(
        authority,
        SubmitBuild {
            spec,
            input_snapshot,
            algorithm_family,
            algorithm,
            params_digest,
            input_payload,
            policy_fingerprint,
        },
    );
    finish_job_submit(store, req_id, submit_spec, &batch, committed_at_ms)
}

/// Encode a submit's governed `JobKind` as the durable input payload.
fn encode_job_input(governed_kind: &JobKind, req_id: u64) -> Result<Vec<u8>, Response> {
    rmp_serde::to_vec_named(governed_kind)
        .map_err(|error| Response::err(req_id, format!("job input encoding failed: {error}")))
}

/// Apply the durably-compiled submit batch and respond with the resulting job.
fn finish_job_submit(
    store: &Arc<JobStore>,
    req_id: u64,
    submit_spec: SubmitSpec,
    batch: &MutationBatch,
    committed_at_ms: u64,
) -> Response {
    match store.submit_batch(submit_spec, batch, committed_at_ms) {
        Ok((job, _replayed)) => {
            job_response::<eg_types::result_contract::coordination::JobSubmit>(req_id, &job)
        }
        Err(e) => Response::err(req_id, e.to_string()),
    }
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

    job_response::<eg_types::result_contract::coordination::JobResume>(req_id, &job)
}
