//! `DecisionFit` and `DecisionEval`: the two admin jobs, served (EH-062,
//! EH-040, EH-016, EH-022, EH-026).
//!
//! A job is a bounded, deterministic computation, so it runs to a terminal
//! state inside its submit and its row is written once, with its receipt or
//! draft, in one Agent Library control-owner transaction. The job id is
//! derived from `(kind, tenant, idempotency_key)`: a retried submit of the
//! same request is a replay that returns the stored row, and the same key with
//! a different request is refused.

use std::sync::Arc;
use std::time::Instant;

use tracing::Instrument;

use eg_numeric::decision::admission::{admit, AdmissionRules, Admitted, Regime};
use eg_numeric::decision::evaluate::{evaluate, EvalSpec};
use eg_numeric::decision::fit::{fit, FitSpec};
use eg_types::agent_component::AgentComponentKind;
use eg_types::decision::digest::digest_text;
use eg_types::decision::jobs::{DatasetSource, LabelRegime};
#[cfg(feature = "jobs")]
use eg_types::decision::replay::EvaluationRun;
use eg_types::decision::replay::{EvalMode, ReplaySpec};
use eg_types::decision::statistical::body::{canonical_body_bytes, content_digest_of};
use eg_types::decision::statistical::dataset::LabelledDataset;
use eg_types::decision::statistical::features::FeatureSchemaBody;
use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::{
    DecisionEvalOp, DecisionEvalRequest, DecisionFitOp, DecisionFitRequest, DecisionJobKind,
    DecisionJobOutput, DecisionJobRecord, DecisionJobState, EvalCandidate,
    DECISION_JOB_SCHEMA_VERSION,
};

use super::stat_log::{logged_dataset, LogReader};
use super::stat_support::{pinned_body, refusal, resolve_policy, ResolvedPolicy};
use super::telemetry;
use super::SharedState;
use crate::protocol::{Response, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::{
    decode_artifact, draft_key, encode_artifact, job_key, receipt_key,
};

/// The label a failed blocking store task names.
const BLOCKING_TASK: &str = "decision job";

/// Domain of a job id.
const JOB_ID_DOMAIN: &str = "eg/decision-job-id/v1";
/// Domain of a job's request digest.
const JOB_REQUEST_DOMAIN: &str = "eg/decision-job-request/v1";

/// The digest a full-label regime pins its gold set by: the content digest of
/// the dataset's canonical JSON, the same scheme component bodies use.
pub(super) fn dataset_digest(dataset: &LabelledDataset) -> Result<String, String> {
    Ok(content_digest_of(&canonical_body_bytes(dataset)?))
}

fn job_id(kind: DecisionJobKind, tenant_id: &str, idempotency_key: &str) -> String {
    let digest = digest_text(JOB_ID_DOMAIN, &(kind, tenant_id, idempotency_key));
    format!("decision-job:{}", digest.trim_start_matches("sha256:"))
}

struct JobIdentity {
    kind: DecisionJobKind,
    tenant_id: String,
    job_id: String,
    request_digest: String,
    now_ms: u64,
}

impl JobIdentity {
    fn record(&self, state: DecisionJobState) -> DecisionJobRecord {
        DecisionJobRecord {
            schema_version: DECISION_JOB_SCHEMA_VERSION,
            job_id: self.job_id.clone(),
            kind: self.kind,
            tenant_id: self.tenant_id.clone(),
            request_digest: self.request_digest.clone(),
            submitted_at_ms: self.now_ms,
            state,
        }
    }
}

fn stored_job(
    store: &AgentLibraryStore,
    tenant_id: &str,
    job_id: &str,
) -> Result<Option<DecisionJobRecord>, String> {
    store
        .decision_artifact(tenant_id, &job_key(job_id))?
        .map(|bytes| decode_artifact(&bytes, "decision job"))
        .transpose()
}

/// A stored job with the same identity is a replay; another request under the
/// same key is refused.
fn replayed(
    store: &AgentLibraryStore,
    identity: &JobIdentity,
) -> Result<Option<DecisionJobRecord>, String> {
    match stored_job(store, &identity.tenant_id, &identity.job_id)? {
        Some(job) if job.request_digest == identity.request_digest => Ok(Some(job)),
        Some(_) => Err(refusal(
            StatisticalErrorCode::IdempotencyConflict,
            "this idempotency key already names a different job request",
        )),
        None => Ok(None),
    }
}

fn checked_dataset(
    dataset: &LabelledDataset,
    schema_digest: &str,
    schema: &FeatureSchemaBody,
) -> Result<(), String> {
    let invalid = |detail: &str| refusal(StatisticalErrorCode::DatasetInvalid, detail);
    dataset
        .clone()
        .checked()
        .map_err(|detail| invalid(&detail))?;
    if dataset.feature_schema_digest != schema_digest {
        return Err(invalid(
            "the dataset was computed under a different feature schema",
        ));
    }
    if dataset.feature_names.as_slice() != schema.names().as_slice() {
        return Err(invalid(
            "the dataset's feature columns do not match the schema",
        ));
    }
    Ok(())
}

fn rules<'a>(
    regime: Regime,
    window: eg_types::decision::RecordWindow,
    policy: &ResolvedPolicy,
    approved: &'a [String],
) -> AdmissionRules<'a> {
    AdmissionRules {
        regime,
        window,
        fidelity_floor: policy.statistical.min_outcome_fidelity,
        approved_principals: approved,
    }
}

fn fit_regime(request: &DecisionFitRequest, dataset: &LabelledDataset) -> Result<Regime, String> {
    match &request.label_regime {
        LabelRegime::FullLabel { gold_set_digest } => {
            if !is_inline(&request.source) || *gold_set_digest != dataset_digest(dataset)? {
                return Err(refusal(
                    StatisticalErrorCode::DatasetInvalid,
                    "gold_set_digest does not pin this dataset",
                ));
            }
            Ok(Regime::FullLabel)
        }
        LabelRegime::BanditLabel => Ok(Regime::BanditLabel),
    }
}

/// Decision-artifact rows to commit with a job: `(key, encoded bytes)`.
pub(super) type ArtifactRows = Vec<(String, Vec<u8>)>;
/// What a job run yields: its output and the artifact rows it commits.
pub(super) type JobRun = Result<(DecisionJobOutput, ArtifactRows), String>;

fn is_inline(source: &DatasetSource) -> bool {
    matches!(source, DatasetSource::Inline { .. })
}

/// The labelled items a job reads: the submitted dataset, or the decision
/// log's executed and evaluated records of one question the caller may read.
fn resolve_dataset(
    store: &AgentLibraryStore,
    reader: &LogReader,
    source: &DatasetSource,
    schema_digest: &str,
) -> Result<LabelledDataset, String> {
    match source {
        DatasetSource::Inline { dataset } => Ok(dataset.as_ref().clone()),
        DatasetSource::Logged { question_id } => {
            logged_dataset(store, reader, question_id, schema_digest)
        }
    }
}

/// Everything a fit produces: the job output and the draft row.
fn run_fit(store: &AgentLibraryStore, reader: &LogReader, request: &DecisionFitRequest) -> JobRun {
    let (schema, schema_digest) = pinned_body::<FeatureSchemaBody>(
        store,
        &request.tenant_id,
        &request.feature_schema,
        AgentComponentKind::FeatureSchema,
        StatisticalErrorCode::FeatureSchemaInvalid,
    )?;
    let dataset = resolve_dataset(store, reader, &request.source, &schema_digest)?;
    checked_dataset(&dataset, &schema_digest, &schema)?;
    let policy = resolve_policy(store, &request.tenant_id, &request.policy)?;
    let regime = fit_regime(request, &dataset)?;
    let approved = policy.statistical.approved_commit_principals.as_slice();
    let admitted = admit(&dataset, &rules(regime, request.window, &policy, approved));
    let spec = FitSpec {
        head_kind: request.head_kind,
        regime,
        optimiser: request.optimiser,
        feature_schema_digest: &schema_digest,
        statistical: &policy.statistical,
    };
    let head = fit(&dataset, &admitted.items, &spec).map_err(|r| r.render())?;
    let bytes = canonical_body_bytes(&head)?;
    let sha = content_digest_of(&bytes);
    let row = (draft_key(&sha), encode_artifact(&head)?);
    Ok((
        DecisionJobOutput::Fit {
            draft_sha256: sha.clone(),
            draft_length: bytes.len() as u64,
            head_digest: sha,
            training_records_digest: head.training_records_digest.clone(),
            synthetic: head.synthetic,
            draft: Box::new(head),
            exclusions: admitted.exclusions,
        },
        vec![row],
    ))
}

pub(super) fn candidate_head(
    store: &AgentLibraryStore,
    tenant_id: &str,
    candidate: &EvalCandidate,
) -> Result<DecisionHeadBody, String> {
    let head = match candidate {
        EvalCandidate::DraftArtifact { sha256, length } => {
            let bytes = store
                .decision_artifact(tenant_id, &draft_key(sha256))?
                .ok_or_else(|| {
                    refusal(
                        StatisticalErrorCode::EvalCandidateNotFound,
                        format!("no draft {sha256}"),
                    )
                })?;
            let head: DecisionHeadBody = decode_artifact(&bytes, "decision head draft")?;
            let canonical = canonical_body_bytes(&head)?;
            if content_digest_of(&canonical) != *sha256 || canonical.len() as u64 != *length {
                return Err(refusal(
                    StatisticalErrorCode::EvalCandidateNotFound,
                    "the draft does not match its pin",
                ));
            }
            head
        }
        EvalCandidate::PublishedHead { head } => {
            pinned_body::<DecisionHeadBody>(
                store,
                tenant_id,
                head,
                AgentComponentKind::DecisionHead,
                StatisticalErrorCode::HeadInvalid,
            )?
            .0
        }
    };
    head.checked()
        .map_err(|detail| refusal(StatisticalErrorCode::HeadInvalid, detail))
}

fn eval_regime(request: &DecisionEvalRequest, dataset: &LabelledDataset) -> Result<Regime, String> {
    let inline = is_inline(&request.source);
    match &request.gold_set_digest {
        Some(digest) if !inline || *digest != dataset_digest(dataset)? => Err(refusal(
            StatisticalErrorCode::DatasetInvalid,
            "gold_set_digest does not pin this dataset",
        )),
        Some(_) => Ok(Regime::FullLabel),
        None => Ok(Regime::BanditLabel),
    }
}

/// What both evaluation modes read: the candidate head, the dataset it is
/// evaluated on, the resolved policy and the label regime.
pub(super) struct EvalInputs {
    pub(super) head: DecisionHeadBody,
    pub(super) dataset: LabelledDataset,
    pub(super) policy: ResolvedPolicy,
    pub(super) regime: Regime,
}

impl EvalInputs {
    /// The dataset's items admitted under this job's window and policy.
    pub(super) fn admitted(&self, window: eg_types::decision::RecordWindow) -> Admitted<'_> {
        let approved = self
            .policy
            .statistical
            .approved_commit_principals
            .as_slice();
        admit(
            &self.dataset,
            &rules(self.regime, window, &self.policy, approved),
        )
    }
}

fn eval_inputs(
    store: &AgentLibraryStore,
    reader: &LogReader,
    request: &DecisionEvalRequest,
) -> Result<EvalInputs, String> {
    let head = candidate_head(store, &request.tenant_id, &request.candidate)?;
    let dataset = resolve_dataset(store, reader, &request.source, &head.feature_schema_digest)?;
    eval_inputs_with_dataset(store, request, head, dataset)
}

fn eval_inputs_with_dataset(
    store: &AgentLibraryStore,
    request: &DecisionEvalRequest,
    head: DecisionHeadBody,
    dataset: LabelledDataset,
) -> Result<EvalInputs, String> {
    if dataset.feature_schema_digest != head.feature_schema_digest {
        return Err(refusal(
            StatisticalErrorCode::DatasetInvalid,
            "the dataset and the head read different feature schemas",
        ));
    }
    dataset
        .clone()
        .checked()
        .map_err(|detail| refusal(StatisticalErrorCode::DatasetInvalid, detail))?;
    let policy = resolve_policy(store, &request.tenant_id, &request.policy)?;
    let regime = eval_regime(request, &dataset)?;
    Ok(EvalInputs {
        head,
        dataset,
        policy,
        regime,
    })
}

/// Immutable, tenant-scoped Agent Library key for the worker's pinned replay
/// request. No labelled data is copied into the analytics-job control row.
#[cfg(feature = "jobs")]
pub(crate) fn replay_request_ref(request: &DecisionEvalRequest) -> String {
    let digest = digest_text("eg/decision/replay-request/v1", request);
    format!("replay-request:{}", digest.trim_start_matches("sha256:"))
}

/// Persist the complete replay request in the Agent Library before queuing a
/// worker. Repeated identical submissions converge on the same artifact.
#[cfg(feature = "jobs")]
pub(crate) fn persist_replay_request(
    store: &AgentLibraryStore,
    request: &DecisionEvalRequest,
) -> Result<String, String> {
    if !matches!(&request.mode, EvalMode::Replay { .. }) || request.tenant_id.is_empty() {
        return Err(refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            "a replay worker requires a tenant-scoped replay request",
        ));
    }
    let DatasetSource::Inline { dataset } = &request.source else {
        return Err(refusal(
            StatisticalErrorCode::ReplayPolicyDependent,
            "replay requires an inline, pinned full-label dataset",
        ));
    };
    eval_regime(request, dataset)?;
    let key = replay_request_ref(request);
    store.put_decision_artifacts(
        &request.tenant_id,
        &[(key.clone(), encode_artifact(request)?)],
    )?;
    Ok(key)
}

/// Execute a claimed replay from its pinned request and durable fold prefix.
/// The worker validates the request reference, tenant and job lineage before
/// entering here. The sealed run is idempotently written to Agent Library;
/// its typed job result is staged separately under the worker's fenced lease.
#[cfg(feature = "jobs")]
pub(crate) fn run_replay_job(
    store: &AgentLibraryStore,
    jobs: &eg_jobs::JobStore,
    claim: &eg_jobs::store::WorkerClaim,
    request: &DecisionEvalRequest,
) -> Result<EvaluationRun, String> {
    let EvalMode::Replay { spec } = &request.mode else {
        return Err(refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            "the claimed request is not a replay evaluation",
        ));
    };
    let DatasetSource::Inline { dataset } = &request.source else {
        return Err(refusal(
            StatisticalErrorCode::ReplayPolicyDependent,
            "replay requires an inline, pinned full-label dataset",
        ));
    };
    let head = candidate_head(store, &request.tenant_id, &request.candidate)?;
    let inputs = eval_inputs_with_dataset(store, request, head, dataset.as_ref().clone())?;
    #[cfg(not(feature = "finance"))]
    {
        let _ = (store, jobs, claim, &inputs, spec);
        return Err(refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            "replay evaluation requires the `finance` feature (its validation kernels)",
        ));
    }
    #[cfg(feature = "finance")]
    {
        let (output, rows) =
            super::stat_walk_forward::run_fenced(store, &inputs, request, spec, jobs, claim)?;
        let DecisionJobOutput::Replay { run } = output else {
            return Err(refusal(
                StatisticalErrorCode::ReplaySpecInvalid,
                "the replay worker produced a different result kind",
            ));
        };
        store.put_decision_artifacts(&request.tenant_id, &rows)?;
        Ok(*run)
    }
}

fn run_eval(inputs: &EvalInputs, request: &DecisionEvalRequest) -> JobRun {
    let admitted = inputs.admitted(request.window);
    let head_digest = content_digest_of(&canonical_body_bytes(&inputs.head)?);
    let spec = EvalSpec {
        regime: inputs.regime,
        statistical: &inputs.policy.statistical,
        estimators: request.estimators.as_slice(),
        head_digest: &head_digest,
        policy_digest: &inputs.policy.digest,
    };
    let receipt = evaluate(
        &inputs.head,
        &inputs.dataset,
        &admitted.items,
        admitted.exclusions,
        &spec,
    )
    .map_err(|r| r.render())?;
    let row = (
        receipt_key(&receipt.receipt_digest),
        encode_artifact(&receipt)?,
    );
    Ok((
        DecisionJobOutput::Eval {
            receipt: Box::new(receipt),
        },
        vec![row],
    ))
}

/// A replay evaluation (EH-528). Its overfitting statistics are the finance
/// validation kernels', so a build without `finance` refuses it by name.
#[cfg(feature = "finance")]
fn run_replay(
    store: &AgentLibraryStore,
    inputs: &EvalInputs,
    request: &DecisionEvalRequest,
    spec: &ReplaySpec,
) -> JobRun {
    super::stat_walk_forward::run(store, inputs, request, spec)
}

#[cfg(not(feature = "finance"))]
fn run_replay(
    _store: &AgentLibraryStore,
    _inputs: &EvalInputs,
    _request: &DecisionEvalRequest,
    _spec: &ReplaySpec,
) -> JobRun {
    Err(refusal(
        StatisticalErrorCode::ReplaySpecInvalid,
        "replay evaluation requires the `finance` feature (its validation kernels)",
    ))
}

/// Run one evaluation job in the mode its request names.
fn run_eval_job(
    store: &AgentLibraryStore,
    reader: &LogReader,
    request: &DecisionEvalRequest,
) -> JobRun {
    let inputs = eval_inputs(store, reader, request)?;
    match &request.mode {
        EvalMode::OffPolicy => run_eval(&inputs, request),
        EvalMode::Replay { spec } => run_replay(store, &inputs, request, spec),
    }
}

fn terminal(outcome: JobRun) -> (DecisionJobState, ArtifactRows) {
    match outcome {
        Ok((output, rows)) => (
            DecisionJobState::Succeeded {
                output: Box::new(output),
            },
            rows,
        ),
        Err(error) => (DecisionJobState::Failed { code: error }, Vec::new()),
    }
}

/// Run a job to its terminal state and persist it with its artifacts.
fn submit_job(
    store: &AgentLibraryStore,
    identity: &JobIdentity,
    run: impl FnOnce() -> JobRun,
) -> Result<DecisionJobRecord, String> {
    if let Some(job) = replayed(store, identity)? {
        return Ok(job);
    }
    let started = Instant::now();
    let (state, mut rows) = terminal(run());
    let job = identity.record(state);
    match &job.state {
        DecisionJobState::Succeeded { output } => match output.as_ref() {
            DecisionJobOutput::Eval { receipt } => telemetry::evaluated(receipt, started),
            DecisionJobOutput::Replay { run } => telemetry::replayed(run, started),
            DecisionJobOutput::Fit { draft, .. } => {
                telemetry::fitted(draft.n_training, draft.calibration.is_some(), started)
            }
        },
        DecisionJobState::Failed { code } => telemetry::refused("DecisionJob", code),
        DecisionJobState::Queued | DecisionJobState::Running | DecisionJobState::Cancelled => {}
    }
    rows.push((job_key(&identity.job_id), encode_artifact(&job)?));
    store.put_decision_artifacts(&identity.tenant_id, &rows)?;
    Ok(job)
}

async fn store_and_now(state: &SharedState) -> Result<(Arc<AgentLibraryStore>, u64), String> {
    let store = state.write().await.ensure_agent_library()?;
    Ok((store, crate::server::dispatch::authoritative_now_ms()))
}

fn identity<T: serde::Serialize>(
    kind: DecisionJobKind,
    tenant_id: &str,
    key: &str,
    request: &T,
    now_ms: u64,
) -> JobIdentity {
    JobIdentity {
        kind,
        tenant_id: tenant_id.to_string(),
        job_id: job_id(kind, tenant_id, key),
        request_digest: digest_text(JOB_REQUEST_DOMAIN, request),
        now_ms,
    }
}

async fn serve_fit(
    state: &SharedState,
    reader: LogReader,
    op: DecisionFitOp,
) -> Result<ResultPayload, String> {
    use eg_types::result_contract::coordination::{DecisionFitStatus, DecisionFitSubmit};
    let (store, now_ms) = store_and_now(state).await?;
    match op {
        DecisionFitOp::Submit { request } => {
            let id = identity(
                DecisionJobKind::Fit,
                &request.tenant_id,
                &request.idempotency_key,
                &request,
                now_ms,
            );
            let job = crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                submit_job(&store, &id, || run_fit(&store, &reader, &request))
            })
            .await?;
            ResultPayload::of::<DecisionFitSubmit>(job)
        }
        DecisionFitOp::Status { request } => ResultPayload::of::<DecisionFitStatus>(stored_job(
            &store,
            &request.tenant_id,
            &request.job_id,
        )?),
    }
}

async fn serve_eval(
    state: &SharedState,
    reader: LogReader,
    op: DecisionEvalOp,
) -> Result<ResultPayload, String> {
    use eg_types::result_contract::coordination::{DecisionEvalStatus, DecisionEvalSubmit};
    let (store, now_ms) = store_and_now(state).await?;
    match op {
        DecisionEvalOp::Submit { request } => {
            let id = identity(
                DecisionJobKind::Eval,
                &request.tenant_id,
                &request.idempotency_key,
                &request,
                now_ms,
            );
            let job = crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                submit_job(&store, &id, || run_eval_job(&store, &reader, &request))
            })
            .await?;
            ResultPayload::of::<DecisionEvalSubmit>(job)
        }
        DecisionEvalOp::Status { request } => ResultPayload::of::<DecisionEvalStatus>(stored_job(
            &store,
            &request.tenant_id,
            &request.job_id,
        )?),
    }
}

fn respond(
    req_id: u64,
    method: &'static str,
    result: Result<ResultPayload, String>,
) -> crate::protocol::Response {
    match result {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => {
            telemetry::refused(method, &error);
            Response::err(req_id, error)
        }
    }
}

fn tenant_refusal(method: &str) -> String {
    format!("ACCESS_DENIED: {method} tenant must match the verified request tenant")
}

/// Serve one `DecisionFit` op.
pub(super) async fn handle_fit(
    state: &SharedState,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: DecisionFitOp,
) -> Response {
    if op.tenant_id() != verified.tenant() {
        return Response::err(req_id, tenant_refusal("DecisionFit"));
    }
    let span = telemetry::span("DecisionFit", verified.tenant());
    respond(
        req_id,
        "DecisionFit",
        serve_fit(state, LogReader::served(state, verified).await, op)
            .instrument(span)
            .await,
    )
}

/// Serve one `DecisionEval` op.
pub(super) async fn handle_eval(
    state: &SharedState,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: DecisionEvalOp,
) -> Response {
    if op.tenant_id() != verified.tenant() {
        return Response::err(req_id, tenant_refusal("DecisionEval"));
    }
    let span = telemetry::span("DecisionEval", verified.tenant());
    respond(
        req_id,
        "DecisionEval",
        serve_eval(state, LogReader::served(state, verified).await, op)
            .instrument(span)
            .await,
    )
}
