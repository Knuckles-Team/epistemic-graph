//! Lane Q5 — quantum as a durable `eg-jobs` job, NOT a synchronous `Op` (PROGRAM.md +
//! `quantum-external-providers.md` addendum §1.1). Three plain functions mirror the
//! eventual native `Op::SubmitQuantum` / `Op::JoinQuantumResults` shape WITHOUT
//! forking eg-jobs or adding a wire `Op` variant — this is staging-discipline step 1,
//! "a tool pulls a subgraph, runs the algorithm, writes results back as proposals.
//! Works today":
//!
//! - [`submit_quantum_job`] — takes the candidate set (ids + the induced subgraph's
//!   edges) as the SAME input a future `Op::SubmitQuantum` would take, builds the
//!   [`eg_quantum_core::ir::QuantumProgram`] via [`crate::circuit`], and calls
//!   `JobStore::submit` — the REAL `eg-jobs` entry point, not a parallel one.
//! - [`claim_and_run_quantum_job`] — the worker side: `JobStore::claim_next` (the
//!   REAL fenced-lease claim), execute against a caller-supplied
//!   [`eg_quantum_core::backend::QuantumBackend`] set chosen by the SAME planner rules
//!   R0-R5 every other quantum lane uses, then `checkpoint_fenced` /
//!   `stage_result_fenced` / `complete_publication_fenced` — the REAL state machine,
//!   not a shortcut around it.
//! - [`join_quantum_result_rows`] — the read side: fetch the durable `TypedJobResult`
//!   and project it to plain `(id, score)` pairs, exactly what a future
//!   `Op::JoinQuantumResults` would emit as RowSet rows. Kept `eg-plan`-free here
//!   (`Vec<(String, f32)>` — every RowSet in this workspace can already build from
//!   that shape via `RowSet::from_scored`); [`crate::rowset_bridge`] (feature
//!   `federation-bridge`) adds the actual `RowSet` + `ForeignSource` wrapper, step 2.
//!
//! ★ Step 3 (a native `Op::SubmitQuantum`/`Op::JoinQuantumResults` in
//! `eg-types::wire::Op`) is DELIBERATELY NOT implemented in this crate. The
//! addendum's staging discipline is explicit: "Do not skip to (3)... Deliver the
//! native Op only if steps 1-2 prove the pattern" with "a real workload" at each step.
//! This crate's own test suite (`tests/`) is that first real workload for step 1+2;
//! it is not, by itself, the "proves the pattern in production" bar step 3 requires.

use std::collections::BTreeMap;

use eg_jobs::{
    digest_params, AlgoVersion, AnalyticsJob, Checkpoint, InputSnapshotHandle, JobError, JobId,
    JobPolicy, JobState, JobStore, ReproducibilityManifest, ResultColumn, SubmitSpec,
    TenantJobQuota, TypedJobResult,
};
use eg_quantum_core::backend::{BackendDescriptor, BackendError, BackendId, QuantumBackend, RunOptions};
use eg_quantum_core::estimate::{estimate, EstimateOptions};
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::planner::{select_backend, PlannerDecision, PlannerOptions};
use eg_quantum_core::result::{Outcome, QuantumResult};
use eg_numeric::error::NumericError;
use serde::{Deserialize, Serialize};

use crate::circuit;

/// The `AnalyticsJob::algo.family` every job this module submits carries. A worker
/// claiming from a `JobStore` shared with OTHER job kinds must check this before
/// treating a claimed job as quantum work (this module does, in
/// [`claim_and_run_quantum_job`]) — `JobStore::claim_next` itself is family-agnostic
/// by design (CONCEPT:INT-P2-1), exactly like every other analytics-job kind.
pub const QUANTUM_ALGO_FAMILY: &str = "quantum.circuit";
/// The one workload this crate ships (see `circuit.rs`'s module doc for why).
pub const GHZ_RANKING_ALGORITHM: &str = "induced_subgraph_ghz";
/// `eg-jobs`' own build-lineage placeholder — mirrors the pattern other in-tree job
/// producers use (a real deployment stamps this from `CARGO_PKG_VERSION`/a git sha at
/// build time; this crate is a library, not a binary, so it names itself instead).
const CODE_VERSION: &str = concat!("eg-quantum-jobs/", env!("CARGO_PKG_VERSION"));
const ENV_VERSION: &str = "eg-quantum-sim";

#[derive(Debug, thiserror::Error)]
pub enum QuantumJobError {
    #[error(transparent)]
    Job(#[from] JobError),
    #[error("quantum job payload codec error: {0}")]
    Codec(String),
    #[error("planner could not select a backend: {0}")]
    Planner(#[from] eg_quantum_core::planner::PlannerError),
    #[error("no registered backend descriptor matches the planner's chosen id '{0}'")]
    BackendNotRegistered(BackendId),
    #[error("backend execution failed: {0}")]
    Backend(#[from] BackendError),
    #[error("quantum result numeric bridge failed: {0}")]
    Numeric(#[from] NumericError),
    #[error("typed job result failed validation: {0}")]
    ResultInvalid(String),
    #[error("job '{0}' was claimed but its algo.family is '{1}', not '{QUANTUM_ALGO_FAMILY}' -- this JobStore is shared with a non-quantum job producer")]
    WrongFamily(JobId, String),
    #[error("candidate_ids.len()={candidates} does not match program.n_qubits={qubits}")]
    CandidateQubitMismatch { candidates: usize, qubits: u32 },
}

/// Everything a submitted quantum job carries beyond what `AnalyticsJob` already
/// stores structurally — the program, its run/estimate/planner options, and the
/// ordered candidate ids (qubit `i` <-> `candidate_ids[i]`). Serialized into
/// `AnalyticsJob::input_payload` (an opaque blob to `eg-jobs` itself, per its own
/// contract) so a worker process that only has a `job_id` can reconstruct exactly
/// what to run without a second side-channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantumJobPayload {
    pub candidate_ids: Vec<String>,
    /// The induced subgraph's edges, as qubit indices into `candidate_ids` (already
    /// reduced to a spanning forest by [`circuit::spanning_forest`] at submit time —
    /// stored post-reduction so a worker never re-derives a DIFFERENT forest from the
    /// same edge list under a different tie-breaking rule).
    pub forest_edges: Vec<(u32, u32)>,
    pub program: QuantumProgram,
    pub run_options: RunOptions,
    pub estimate_options: EstimateOptionsDto,
    pub planner_options: PlannerOptionsDto,
}

/// A serde-friendly mirror of [`EstimateOptions`] ([`BackendId`] round-trips fine but
/// the struct itself has no `Serialize`/`Deserialize` derive upstream — Q0 shipped it
/// as a planner-input value type, not a wire type). Converts losslessly both ways.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EstimateOptionsDto {
    pub noise_model_id: Option<String>,
    pub noise_non_clifford: bool,
    pub want_exact_density_matrix: bool,
    pub shots: Option<u64>,
    pub memory_bound_bytes: Option<u64>,
    pub backend_id_override: Option<String>,
}

impl From<&EstimateOptions> for EstimateOptionsDto {
    fn from(o: &EstimateOptions) -> Self {
        EstimateOptionsDto {
            noise_model_id: o.noise.as_ref().and_then(|n| n.model_id.clone()),
            noise_non_clifford: o.noise.as_ref().map(|n| n.non_clifford).unwrap_or(false),
            want_exact_density_matrix: o.want_exact_density_matrix,
            shots: o.shots,
            memory_bound_bytes: o.memory_bound_bytes,
            backend_id_override: o.backend_id_override.as_ref().map(|b| b.0.clone()),
        }
    }
}

impl From<&EstimateOptionsDto> for EstimateOptions {
    fn from(d: &EstimateOptionsDto) -> Self {
        EstimateOptions {
            noise: d.noise_model_id.as_ref().map(|id| {
                eg_quantum_core::estimate::NoiseRequest {
                    model_id: Some(id.clone()),
                    non_clifford: d.noise_non_clifford,
                }
            }),
            want_exact_density_matrix: d.want_exact_density_matrix,
            shots: d.shots,
            memory_bound_bytes: d.memory_bound_bytes,
            backend_id_override: d.backend_id_override.as_deref().map(BackendId::from),
        }
    }
}

/// A serde-friendly mirror of [`PlannerOptions`] (same reason as
/// [`EstimateOptionsDto`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlannerOptionsDto {
    pub want_hardware: bool,
    pub backend_id_override: Option<String>,
}

impl From<&PlannerOptions> for PlannerOptionsDto {
    fn from(o: &PlannerOptions) -> Self {
        PlannerOptionsDto {
            want_hardware: o.want_hardware,
            backend_id_override: o.backend_id_override.as_ref().map(|b| b.0.clone()),
        }
    }
}

impl From<&PlannerOptionsDto> for PlannerOptions {
    fn from(d: &PlannerOptionsDto) -> Self {
        PlannerOptions {
            want_hardware: d.want_hardware,
            backend_id_override: d.backend_id_override.as_deref().map(BackendId::from),
        }
    }
}

/// Step 1a — SUBMIT. Builds the GHZ-ranking [`QuantumProgram`] over `candidate_ids`
/// (one qubit per id, in order) and `edges` (indices into `candidate_ids`), then
/// submits it through the REAL `eg-jobs` entry point (`JobStore::submit`). Returns the
/// durable `AnalyticsJob` (its server-issued `job_id` is what [`join_quantum_result_rows`]
/// later reads back).
#[allow(clippy::too_many_arguments)]
pub fn submit_quantum_job(
    store: &JobStore,
    input_snapshot: InputSnapshotHandle,
    policy: JobPolicy,
    candidate_ids: Vec<String>,
    edges: Vec<(u32, u32)>,
    run_options: RunOptions,
    estimate_options: EstimateOptions,
    planner_options: PlannerOptions,
    max_attempts: u32,
    backoff_ms: u64,
) -> Result<AnalyticsJob, QuantumJobError> {
    let n_qubits = u32::try_from(candidate_ids.len()).unwrap_or(u32::MAX);
    let forest_edges = circuit::spanning_forest(n_qubits, &edges);
    let program = circuit::induced_subgraph_ghz_program(n_qubits, &forest_edges);

    let payload = QuantumJobPayload {
        candidate_ids,
        forest_edges,
        program,
        run_options,
        estimate_options: (&estimate_options).into(),
        planner_options: (&planner_options).into(),
    };
    let payload_bytes =
        serde_json::to_vec(&payload).map_err(|e| QuantumJobError::Codec(e.to_string()))?;
    let params_digest = digest_params(&serde_json::to_value(&payload).unwrap_or_default());

    let spec = SubmitSpec {
        input_snapshot,
        policy,
        algo: AlgoVersion {
            family: QUANTUM_ALGO_FAMILY.to_string(),
            algorithm: GHZ_RANKING_ALGORITHM.to_string(),
            params_digest,
            code_version: CODE_VERSION.to_string(),
            env_version: ENV_VERSION.to_string(),
        },
        input_payload: Some(payload_bytes),
        max_attempts,
        backoff_ms,
    };
    Ok(store.submit(spec)?)
}

/// A minimal, job-plane-scoped set of live backend instances (NOT the general Q3/Q4
/// backend registry `eg_quantum_core::backend`'s own docs flag as future/out-of-scope
/// — this is just enough to run the planner's chosen [`BackendId`] against a REAL
/// backend after `select_backend` picks it structurally).
pub struct BackendSet<'a> {
    backends: Vec<&'a dyn QuantumBackend>,
}

impl<'a> BackendSet<'a> {
    pub fn new(backends: Vec<&'a dyn QuantumBackend>) -> Self {
        BackendSet { backends }
    }

    pub fn descriptors(&self) -> Vec<BackendDescriptor> {
        self.backends
            .iter()
            .map(|b| BackendDescriptor {
                id: b.backend_id(),
                family: b.family(),
                capabilities: b.capabilities(),
            })
            .collect()
    }

    pub fn get(&self, id: &BackendId) -> Option<&'a dyn QuantumBackend> {
        self.backends
            .iter()
            .copied()
            .find(|b| &b.backend_id() == id)
    }
}

/// Step 1b — RUN (the worker side). Claims the next ready job via the REAL
/// `JobStore::claim_next` fenced-lease path, runs it against whichever backend in
/// `backends` the SAME planner rules R0-R5 select, and drives the job through
/// `checkpoint_fenced` -> `stage_result_fenced` -> `complete_publication_fenced` (or
/// `fail_attempt_fenced` on error) — every transition the real `eg-jobs` state machine
/// requires, none skipped. Returns `Ok(None)` if no quantum job is currently ready
/// (mirrors `claim_next`'s own `Ok(None)` "nothing to do" signal).
pub fn claim_and_run_quantum_job(
    store: &JobStore,
    worker_ref: &str,
    backends: &BackendSet<'_>,
    quota: TenantJobQuota,
    now_ms: i64,
    lease_ms: u64,
) -> Result<Option<AnalyticsJob>, QuantumJobError> {
    let Some(claim) = store.claim_next(worker_ref, &[], now_ms, lease_ms, quota)? else {
        return Ok(None);
    };
    let job = claim.job;
    let epoch = claim.lease.epoch;

    if job.algo.family != QUANTUM_ALGO_FAMILY {
        return Err(QuantumJobError::WrongFamily(
            job.job_id.clone(),
            job.algo.family.clone(),
        ));
    }

    match run_claimed_job(store, &job, worker_ref, epoch, backends, now_ms) {
        Ok(finished) => Ok(Some(finished)),
        Err(err) => {
            // Best-effort: return the job to Submitted (if retries remain) or Failed,
            // per the REAL eg-jobs retry policy -- never leave a claimed lease
            // dangling on our own execution error.
            let _ = store.fail_attempt_fenced(&job.job_id, worker_ref, epoch, err.to_string(), now_ms);
            Err(err)
        }
    }
}

fn run_claimed_job(
    store: &JobStore,
    job: &AnalyticsJob,
    worker_ref: &str,
    epoch: u64,
    backends: &BackendSet<'_>,
    now_ms: i64,
) -> Result<AnalyticsJob, QuantumJobError> {
    let payload: QuantumJobPayload = job
        .input_payload
        .as_ref()
        .ok_or_else(|| QuantumJobError::Codec("quantum job has no input_payload".to_string()))
        .and_then(|bytes| {
            serde_json::from_slice(bytes).map_err(|e| QuantumJobError::Codec(e.to_string()))
        })?;

    if payload.candidate_ids.len() != payload.program.n_qubits as usize {
        return Err(QuantumJobError::CandidateQubitMismatch {
            candidates: payload.candidate_ids.len(),
            qubits: payload.program.n_qubits,
        });
    }

    store.checkpoint_fenced(
        &job.job_id,
        worker_ref,
        epoch,
        Checkpoint {
            progress: 0.25,
            stage: "planning".to_string(),
            state_blob: None,
            updated_at_ms: now_ms,
        },
        now_ms,
    )?;

    let estimate_opts: EstimateOptions = (&payload.estimate_options).into();
    let planner_opts: PlannerOptions = (&payload.planner_options).into();
    let est = estimate(&payload.program, &estimate_opts);
    let descriptors = backends.descriptors();
    let decision: PlannerDecision = select_backend(&est, &descriptors, &planner_opts)?;
    let backend = backends
        .get(&decision.chosen)
        .ok_or_else(|| QuantumJobError::BackendNotRegistered(decision.chosen.clone()))?;

    store.checkpoint_fenced(
        &job.job_id,
        worker_ref,
        epoch,
        Checkpoint {
            progress: 0.5,
            stage: format!("executing on {}", decision.chosen),
            state_blob: None,
            updated_at_ms: now_ms,
        },
        now_ms,
    )?;

    let result = backend.run(&payload.program, &payload.run_options)?;
    // ★ Addendum §0 / §1.4: quantum subroutines PROPOSE, never commit as a hard
    // constraint from this job-plane path -- even an exact GHZ result stays a
    // Proposal all the way into the TypedJobResult below. `into_hard_constraint()`
    // is never called here.
    let proposal = result.into_proposal();
    let outcome = proposal.result().outcome.clone();

    let scores = consistency_scores(&outcome, payload.program.n_qubits, &payload.forest_edges)?;
    let marginals =
        crate::numeric_bridge::marginal_probabilities(&outcome, payload.program.n_qubits)?;

    let typed_result = build_typed_result(
        job,
        proposal.result(),
        &payload.candidate_ids,
        &scores,
        &marginals,
    )
    .map_err(QuantumJobError::ResultInvalid)?;

    // `stage_result_fenced` moves Running -> Publishing without touching the lease
    // (see its own doc), so the SAME fencing epoch still owns the job here.
    store.stage_result_fenced(&job.job_id, worker_ref, epoch, typed_result, now_ms)?;
    let finished = store.complete_publication_fenced(&job.job_id, worker_ref, epoch, now_ms)?;
    Ok(finished)
}

/// Per-candidate "entanglement consistency" score: the fraction of shots (weighted by
/// `Outcome::Counts`) in which candidate `i`'s measured bit equals the MAJORITY bit
/// within its own connected component (`forest`) for that shot's bitstring. A perfect
/// noiseless GHZ component scores every member `1.0` (every shot, every member of the
/// component agrees) — genuinely distinguishing entangled candidates from independent
/// ones, which would average close to `0.5`. Bounded `[0,1]` by construction (a
/// fraction of shots), satisfying `TypedJobResult`'s `confidence` contract directly.
pub fn consistency_scores(
    outcome: &Outcome,
    n_qubits: u32,
    forest: &[(u32, u32)],
) -> Result<Vec<f64>, NumericError> {
    let Outcome::Counts(counts) = outcome else {
        return Err(NumericError::Shape(
            "consistency_scores requires Outcome::Counts".to_string(),
        ));
    };
    let total: u64 = counts.values().sum();
    if total == 0 {
        return Err(NumericError::Shape(
            "consistency_scores: zero total shots".to_string(),
        ));
    }
    let comps = circuit::components(n_qubits, forest);
    let mut agree = vec![0u64; n_qubits as usize];
    for (bitstring, &count) in counts {
        let bits: Vec<u8> = bitstring
            .chars()
            .map(|c| if c == '1' { 1u8 } else { 0u8 })
            .collect();
        if bits.len() != n_qubits as usize {
            return Err(NumericError::Shape(format!(
                "consistency_scores: outcome bitstring '{bitstring}' has {} bits, expected {n_qubits}",
                bits.len()
            )));
        }
        for comp in &comps {
            let ones = comp.iter().filter(|&&q| bits[q as usize] == 1).count();
            let majority: u8 = if ones * 2 >= comp.len() { 1 } else { 0 };
            for &q in comp {
                if bits[q as usize] == majority {
                    agree[q as usize] += count;
                }
            }
        }
    }
    Ok(agree.into_iter().map(|a| a as f64 / total as f64).collect())
}

fn reproducibility_for(job: &AnalyticsJob) -> ReproducibilityManifest {
    ReproducibilityManifest {
        input_dataset_ref: job.input_snapshot.dataset_ref.clone(),
        input_content_digest: job.input_snapshot.content_digest.clone(),
        input_snapshot_version: job.input_snapshot.version,
        algorithm_ref: format!("{}:{}", job.algo.family, job.algo.algorithm),
        params_digest: job.algo.params_digest.clone(),
        implementation_version: job.algo.code_version.clone(),
        environment_version: job.algo.env_version.clone(),
        policy_fingerprint: job.policy.policy_fingerprint.clone(),
    }
}

fn build_typed_result(
    job: &AnalyticsJob,
    result: &QuantumResult,
    candidate_ids: &[String],
    scores: &[f64],
    marginals: &ndarray::Array1<f64>,
) -> Result<TypedJobResult, String> {
    let circuit_evidence = format!("eg:quantum_circuit:{}", result.circuit_hash);
    let job_source = format!("eg:job:{}", job.job_id);

    let schema = vec![
        ResultColumn {
            name: "id".to_string(),
            logical_type: "string".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "kind".to_string(),
            logical_type: "string".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "confidence".to_string(),
            logical_type: "f64".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "evidence_refs".to_string(),
            logical_type: "list<string>".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "source_refs".to_string(),
            logical_type: "list<string>".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "proof_ids".to_string(),
            logical_type: "list<string>".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "contradiction_ids".to_string(),
            logical_type: "list<string>".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "quantum_marginal".to_string(),
            logical_type: "f64".to_string(),
            nullable: false,
        },
        ResultColumn {
            name: "quantum_exact".to_string(),
            logical_type: "bool".to_string(),
            nullable: false,
        },
    ];

    let mut rows = Vec::with_capacity(candidate_ids.len());
    for (i, id) in candidate_ids.iter().enumerate() {
        let mut row: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        row.insert("id".to_string(), serde_json::json!(id));
        row.insert(
            "kind".to_string(),
            serde_json::json!("quantum_ghz_consistency_proposal"),
        );
        row.insert("confidence".to_string(), serde_json::json!(scores[i]));
        row.insert(
            "evidence_refs".to_string(),
            serde_json::json!([circuit_evidence.clone()]),
        );
        row.insert(
            "source_refs".to_string(),
            serde_json::json!([job_source.clone()]),
        );
        row.insert("proof_ids".to_string(), serde_json::json!([] as [(); 0]));
        row.insert(
            "contradiction_ids".to_string(),
            serde_json::json!([] as [(); 0]),
        );
        row.insert(
            "quantum_marginal".to_string(),
            serde_json::json!(marginals[i]),
        );
        row.insert("quantum_exact".to_string(), serde_json::json!(result.is_exact()));
        rows.push(row);
    }

    TypedJobResult::new(
        schema,
        rows,
        vec![circuit_evidence],
        vec![],
        None,
        None,
        reproducibility_for(job),
    )
}

/// Step 1c — JOIN. Reads back a `Succeeded` job's durable `TypedJobResult` and
/// projects it to plain `(id, score)` pairs (`score` = `confidence`), exactly what a
/// future `Op::JoinQuantumResults` would emit as RowSet rows — this function IS that
/// materialization step, just callable directly today. Errs cleanly (never panics) if
/// the job has not reached `Succeeded` yet, matching the async-job contract: a caller
/// must poll/check state before joining, exactly like every other `eg-jobs` consumer.
pub fn join_quantum_result_rows(
    store: &JobStore,
    job_id: &str,
) -> Result<Vec<(String, f32)>, QuantumJobError> {
    let job = store.get(job_id)?;
    if !matches!(&job.state, JobState::Succeeded { .. }) {
        return Err(QuantumJobError::Job(JobError::InvalidTransition {
            job_id: job_id.to_string(),
            state: job.state.label(),
            reason: "join requires a Succeeded job",
        }));
    };
    // `AnalyticsJob::output` retains the FULL typed result after success (see its own
    // doc: "analytics output is never discarded") -- read it straight off the job
    // record rather than a second `get_result` round trip. This ALSO sidesteps a
    // subtlety worth naming: `JobState::Succeeded.result_ref` is
    // `AnalyticsJob::result_ref()`, a hash of `(input_snapshot, algo)` used as the
    // store's IDEMPOTENCY key (`COMMITTED_RESULTS`/`mark_result_committed`) -- it is
    // NOT the `RESULTS` table's storage key, which is the typed result's OWN content
    // hash (`TypedJobResult::dataset_ref`). `JobStore::get_result` takes THAT key, not
    // the state's `result_ref` field; reading `job.output` directly avoids relying on
    // a caller getting that distinction right.
    let result = job.output.clone().ok_or_else(|| {
        QuantumJobError::Codec(format!(
            "job '{job_id}' is Succeeded but carries no output (should be impossible)"
        ))
    })?;
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let id = row
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| QuantumJobError::Codec("result row missing 'id'".to_string()))?
            .to_string();
        let score = row
            .get("confidence")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| QuantumJobError::Codec("result row missing 'confidence'".to_string()))?
            as f32;
        out.push((id, score));
    }
    Ok(out)
}
