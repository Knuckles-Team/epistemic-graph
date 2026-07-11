//! The durable analytics-job plane's protocol surface (CONCEPT:INT-P2-1, feature
//! `jobs`): `Method::AnalyticsJob { op }` — submit/status/cancel/resume over
//! `eg-jobs`'s redb-backed `AnalyticsJob` state machine.
//!
//! NOT graph-scoped: jobs are keyed by `job_id` in their own `jobs.redb` (mirrors
//! `TsAppend`/`Kv*`), so this module self-routes directly off `ServerState` in
//! `dispatch.rs`'s top-level match, ahead of the per-graph `dispatch_graph_op`
//! chain — see that module's doc + `src/server/mutation.rs`'s `JUSTIFIED_NA` entry.
//!
//! ## The reference job kind: `MineAssociate`
//!
//! V1 wires ONE concrete analytics computation end to end — association-rule
//! mining over EXPLICIT transactions, reusing the exact
//! `eg_compute::mining::association::mine_labeled` kernel the synchronous
//! `Method::MineAssociate` writeback already calls — to prove the full job-plane
//! mechanics: async execution off the request, checkpointed progress, cooperative
//! cancellation, crash-orphan resume, and a deterministic/idempotent result commit
//! as a provenance'd `:Claim`/`:Evidence` pair (`eg_jobs::claim::commit_result_claim`).
//!
//! `mine_labeled` is an ATOMIC (non-incremental) kernel, so this job kind's
//! checkpoint granularity is coarse (`queued -> running -> computed -> committed`)
//! rather than fine-grained chunk-level resume — a `resume` re-runs the WHOLE
//! computation from the ORIGINAL request (preserved in the checkpoint's
//! `state_blob`, since explicit `transactions` are request data, not something the
//! `InputSnapshotHandle` itself retains). This is safe BECAUSE the computation is
//! deterministic and the result commit is idempotent (re-deriving the same
//! `result_ref` and finding the claim already committed is a documented no-op) —
//! the durability/lineage/idempotency guarantees this crate adds do not depend on
//! any one kernel being incrementally resumable.
//!
//! ## Wave-2 follow-up
//!
//! Additional mining families (clustering, anomaly, sequence, …) as more
//! `JobKind` variants; graph-derived transaction sources (today's `MineAssociate`
//! job kind only accepts explicit `transactions`); an AU-side feature/model/
//! experiment REGISTRY that indexes committed claims by `AlgoVersion` lineage
//! across jobs (this crate records the lineage on every claim — see `claim.rs` —
//! but building a queryable registry ON TOP of that is out of scope here).

use std::path::Path;
use std::sync::{Arc, OnceLock};

use tokio::sync::RwLock;

use eg_compute::mining::association::{self, Algorithm};
use eg_core::graph::GraphCore;
use eg_jobs::model::{AlgoVersion, InputSnapshotHandle, JobPolicy};
use eg_jobs::store::{JobStore, SubmitSpec};
use eg_types::jobs::{JobKind, JobOp, SubmitJobSpec};

use crate::protocol::{Response, ResultPayload};
use crate::server::state::ServerState;

/// The engine build that ran a job (CONCEPT:INT-P2-1 lineage) — `CARGO_PKG_VERSION`
/// of the `epistemic-graph` facade crate itself.
const CODE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// A stable label for the runtime/feature-set this job kind ran under. A full
/// fingerprint (feature-flag hash) is a Wave-2 refinement; this is enough to
/// distinguish "the reference `jobs` build" from a future differently-shaped one.
const ENV_VERSION: &str = "eg-jobs-v1";

/// Lazily-opened, process-wide job store, keyed by the server's OWN
/// `persist_dir` (an EXISTING `ServerState` field — no new field threaded through
/// the ~30 `ServerState` struct-literal construction sites across the codebase).
/// One store per process (persist_dir is fixed for the process lifetime), mirroring
/// the `OnceLock` env-read pattern `server::state::max_response_nodes` already uses.
fn job_store(persist_dir: &Option<String>) -> Arc<JobStore> {
    static STORE: OnceLock<Arc<JobStore>> = OnceLock::new();
    STORE
        .get_or_init(|| {
            let store = match persist_dir {
                Some(dir) => JobStore::open_in_dir(Path::new(dir)),
                None => {
                    // In-memory-only deployment (no persist dir configured): a
                    // process-scoped temp location, mirroring `eg-tsdb`'s own
                    // temp-file fallback for a no-persist-dir server — durable for
                    // the life of the process, not across a real restart (there is
                    // no "graph.redb" sibling to live beside in this mode either).
                    let dir = std::env::temp_dir().join(format!("eg-jobs-{}", std::process::id()));
                    JobStore::open_in_dir(&dir)
                }
            };
            Arc::new(store.expect("open jobs.redb (durable analytics-job store)"))
        })
        .clone()
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

/// Handle `Method::AnalyticsJob { op }` (CONCEPT:INT-P2-1). Self-contained: resolves
/// its own `JobStore` + (for `Submit`/`Resume`) the target `GraphCore` off `state`,
/// so the dispatch shell can call this directly with no per-graph routing.
pub(crate) async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    op: JobOp,
) -> Response {
    let persist_dir = state.read().await.persist_dir.clone();
    let store = job_store(&persist_dir);

    match op {
        JobOp::Submit(spec) => handle_submit(state, &store, req_id, spec).await,
        JobOp::Status { job_id } => match store.get(&job_id) {
            Ok(job) => job_response(req_id, &job),
            Err(e) => Response::err(req_id, e.to_string()),
        },
        JobOp::Cancel { job_id } => match store.request_cancel(&job_id) {
            Ok(job) => job_response(req_id, &job),
            Err(e) => Response::err(req_id, e.to_string()),
        },
        JobOp::Resume { job_id } => handle_resume(state, &store, req_id, &job_id).await,
    }
}

fn job_response(req_id: u64, job: &eg_jobs::AnalyticsJob) -> Response {
    match serde_json::to_value(job) {
        Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
        Err(e) => Response::err(req_id, format!("job serialization failed: {e}")),
    }
}

async fn resolve_core(state: &Arc<RwLock<ServerState>>, graph: &str) -> Option<Arc<GraphCore>> {
    state.read().await.registry.get(graph).map(|e| e.core.clone())
}

async fn handle_submit(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    req_id: u64,
    spec: SubmitJobSpec,
) -> Response {
    let SubmitJobSpec {
        graph,
        tenant,
        actor,
        purpose,
        priority,
        deadline_unix_ms,
        quota_cpu_ms,
        max_attempts,
        backoff_ms,
        kind,
    } = spec;

    let Some(core) = resolve_core(state, &graph).await else {
        return Response::err(req_id, format!("unknown graph '{graph}'"));
    };
    // The immutable input-snapshot handle is stamped by the SERVER from the live
    // graph's OCC version, never accepted from the caller (CONCEPT:INT-P2-1: a
    // client cannot forge which graph-version a job ran against).
    let input_snapshot = InputSnapshotHandle::new(graph, core.version());

    let JobKind::MineAssociate {
        transactions,
        min_support,
        min_confidence,
        algorithm,
    } = kind.clone();
    let parsed_algo = match parse_algorithm(&algorithm) {
        Ok(a) => a,
        Err(e) => return Response::err(req_id, e),
    };
    let params_digest = eg_jobs::digest_params(&serde_json::json!({
        "min_support": min_support,
        "min_confidence": min_confidence,
        "algorithm": algorithm,
    }));

    let submit_spec = SubmitSpec {
        input_snapshot,
        policy: JobPolicy {
            tenant,
            actor,
            purpose,
            priority,
            quota_cpu_ms,
            deadline_unix_ms,
        },
        algo: AlgoVersion {
            family: "mining.association".to_string(),
            algorithm,
            params_digest,
            code_version: CODE_VERSION.to_string(),
            env_version: ENV_VERSION.to_string(),
        },
        max_attempts,
        backoff_ms,
    };

    let job = match store.submit(submit_spec) {
        Ok(j) => j,
        Err(e) => return Response::err(req_id, e.to_string()),
    };

    spawn_mine_associate(
        store.clone(),
        core,
        job.job_id.clone(),
        kind,
        transactions,
        min_support,
        min_confidence,
        parsed_algo,
    );

    job_response(req_id, &job)
}

async fn handle_resume(
    state: &Arc<RwLock<ServerState>>,
    store: &Arc<JobStore>,
    req_id: u64,
    job_id: &str,
) -> Response {
    let job = match store.resume(job_id) {
        Ok(j) => j,
        Err(e) => return Response::err(req_id, e.to_string()),
    };

    // Re-extract the original request from the preserved checkpoint so the
    // (coarse-grained, job-level) resume can actually re-drive the computation —
    // see the module doc on why this kernel resumes by re-running deterministically
    // rather than continuing a fine-grained chunk cursor.
    let kind = job
        .state
        .checkpoint()
        .and_then(|c| c.state_blob.as_ref())
        .and_then(|blob| rmp_serde::from_slice::<JobKind>(blob).ok());

    if let Some(JobKind::MineAssociate {
        transactions,
        min_support,
        min_confidence,
        algorithm,
    }) = kind.clone()
    {
        if let Ok(parsed_algo) = parse_algorithm(&algorithm) {
            if let Some(core) = resolve_core(state, &job.input_snapshot.graph).await {
                spawn_mine_associate(
                    store.clone(),
                    core,
                    job.job_id.clone(),
                    kind.expect("checked Some above"),
                    transactions,
                    min_support,
                    min_confidence,
                    parsed_algo,
                );
            }
        }
    }

    job_response(req_id, &job)
}

/// Spawn the actual analytics work OFF the request (CONCEPT:INT-P2-1: "runs async").
/// Owns the checkpoint/cancel/succeed/claim-commit lifecycle for one run.
#[allow(clippy::too_many_arguments)]
fn spawn_mine_associate(
    store: Arc<JobStore>,
    core: Arc<GraphCore>,
    job_id: String,
    kind: JobKind,
    transactions: Vec<Vec<String>>,
    min_support: f64,
    min_confidence: f64,
    algorithm: Algorithm,
) {
    tokio::spawn(async move {
        if store.start_running(&job_id).is_err() {
            return;
        }
        // The checkpoint's `state_blob` preserves the ORIGINAL request so a later
        // `resume` (after an orphaning crash) can re-derive it — see `handle_resume`.
        let kind_blob = rmp_serde::to_vec_named(&kind).ok();
        if store
            .checkpoint(&job_id, 0.1, "mining", kind_blob)
            .is_err()
        {
            return;
        }

        if job_cancelled(&store, &job_id) {
            return;
        }

        let rules = match tokio::task::spawn_blocking(move || {
            association::mine_labeled(&transactions, min_support, min_confidence, algorithm)
        })
        .await
        {
            Ok(rules) => rules,
            Err(e) => {
                let _ = store.fail(&job_id, format!("mining task panicked: {e}"));
                return;
            }
        };

        let _ = store.checkpoint(&job_id, 0.9, "computed", None);
        if job_cancelled(&store, &job_id) {
            return;
        }

        let job = match store.get(&job_id) {
            Ok(j) => j,
            Err(_) => return,
        };
        let result_ref = job.result_ref();
        let job = match store.succeed(&job_id, result_ref) {
            Ok(j) => j,
            Err(e) => {
                let _ = store.fail(&job_id, e.to_string());
                return;
            }
        };

        // Quality score seeds the claim's confidence — mean `support * confidence`
        // over the mined rules (the SAME convention `mining.rs`'s synchronous
        // `as_claim` writeback uses), `0.0` for an empty result.
        let confidence = if rules.is_empty() {
            0.0
        } else {
            rules.iter().map(|r| r.support * r.confidence).sum::<f64>() / rules.len() as f64
        };
        // A commit failure here (e.g. a transient graph-write error) leaves the job
        // durably `Succeeded` with its `result_ref` intact; the caller can retry the
        // commit later (it is idempotent) without re-running the computation.
        let _ = eg_jobs::commit_result_claim(&core, &job, confidence);
    });
}

/// Cooperative-cancel check-and-finalize (CONCEPT:INT-P2-1): if the job's
/// `cancel_requested` flag is set, transition it to `Cancelled` and report `true`
/// (the caller should stop); otherwise `false`.
fn job_cancelled(store: &JobStore, job_id: &str) -> bool {
    match store.get(job_id) {
        Ok(job) if job.cancel_requested => {
            let _ = store.mark_cancelled(job_id);
            true
        }
        _ => false,
    }
}
