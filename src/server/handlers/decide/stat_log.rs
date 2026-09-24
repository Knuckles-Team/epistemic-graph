//! `DecisionLog`, served (EH-060, EH-061, EH-012), and the logged-dataset
//! builder `DecisionFit`/`DecisionEval` read labels from.
//!
//! Every read filters by each record's own visibility before a record, an
//! evaluation, a count or a rate is formed: tenant-wide for library-sourced
//! records, the committing principal only for graph-sourced ones.

use std::collections::BTreeMap;
use std::time::Instant;

use tracing::Instrument;

use eg_numeric::decision::aggregate::{aggregate, AggregateRules, JoinedRecord};
use eg_numeric::decision::candidate::CandidateView;
use eg_numeric::decision::features::outcome_rate_key;
use eg_types::agent_component::AgentComponentKind;
use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::dataset::{
    ItemLabel, LabelledDataset, LabelledItem, LoggedOutcome, OutcomeEvaluation, PropensitySource,
    LABELLED_DATASET_SCHEMA_VERSION,
};
use eg_types::decision::statistical::features::{FeatureKind, FeatureSchemaBody};
use eg_types::decision::statistical::log::{
    DecisionLogCommitted, DecisionLogEntry, DecisionLogOp, DecisionOutcomeEvaluation, EntryInputs,
    OutcomeAggregate, OutcomeAggregateRequest, RecordVisibility, StoredEvaluation,
    DECISION_LOG_SCHEMA_VERSION,
};
use eg_types::decision::statistical::{
    FeatureMatrixRef, StatisticalDecisionRecord, StatisticalErrorCode, StatisticalOutcome,
};
use eg_types::decision::{CandidateSourceRecord, QuantScaleTag, RecordWindow};

use super::stat_executor::ExecutionContext;
use super::stat_replay::replay;
use super::stat_resolve::resolve;
use super::stat_retention::{compact, full_record, verify, Retention};
use super::stat_slate::{evaluated_slates, slate_of, SLATE_QUESTION};
use super::stat_support::{default_statistical_policy, pinned_entry, refusal};
use super::telemetry;
use super::SharedState;
use crate::protocol::{Response, ResultPayload};
use crate::server::{
    auth::VerifiedRequestContext,
    persistence::{
        agent_library::AgentLibraryStore,
        decision_jobs::{decode_artifact, encode_artifact, evaluation_key, record_key},
    },
};

/// Most log rows one read walks before refusing as unbounded.
pub(super) const MAX_LOG_ROWS: usize = 100_000;

/// Who is reading or writing the log.
pub(super) struct LogReader {
    pub(super) tenant_id: String,
    pub(super) principal: String,
    /// How compacted inputs are restored when a read needs them.
    pub(super) retention: Retention,
}

impl LogReader {
    /// A reader that needs no compacted inputs (aggregates, visibility).
    pub(super) fn of(verified: &VerifiedRequestContext) -> Self {
        Self {
            tenant_id: verified.tenant().to_string(),
            principal: verified.principal_persistence_id(),
            retention: Retention::none(),
        }
    }

    /// A reader that restores compacted inputs from the served Blob CAS.
    pub(super) async fn served(state: &SharedState, verified: &VerifiedRequestContext) -> Self {
        let retention = Retention::of(&*state.read().await, verified);
        Self {
            retention,
            ..Self::of(verified)
        }
    }

    fn sees(&self, entry: &DecisionLogEntry) -> bool {
        match &entry.visibility {
            RecordVisibility::Tenant => true,
            RecordVisibility::Principal { principal } => *principal == self.principal,
        }
    }
}

pub(super) fn executed_option(outcome: &StatisticalOutcome) -> Option<&str> {
    match outcome {
        StatisticalOutcome::Acted { option_id, .. }
        | StatisticalOutcome::Explored { option_id, .. } => Some(option_id),
        StatisticalOutcome::Advisory { .. } | StatisticalOutcome::Abstained { .. } => None,
    }
}

pub(super) fn visible_entries(
    store: &AgentLibraryStore,
    reader: &LogReader,
) -> Result<Vec<DecisionLogEntry>, String> {
    store
        .decision_artifacts_with_prefix(&reader.tenant_id, "record:", MAX_LOG_ROWS)?
        .into_iter()
        .map(|(_, bytes)| decode_artifact::<DecisionLogEntry>(&bytes, "decision log entry"))
        .filter(|entry| !matches!(entry, Ok(e) if !reader.sees(e)))
        .collect()
}

pub(super) fn visible_entry(
    store: &AgentLibraryStore,
    reader: &LogReader,
    record_id: &str,
) -> Result<Option<DecisionLogEntry>, String> {
    let Some(bytes) = store.decision_artifact(&reader.tenant_id, &record_key(record_id))? else {
        return Ok(None);
    };
    let entry: DecisionLogEntry = decode_artifact(&bytes, "decision log entry")?;
    Ok(reader.sees(&entry).then_some(entry))
}

pub(super) fn evaluations_of(
    store: &AgentLibraryStore,
    tenant_id: &str,
    record_id: &str,
) -> Result<Vec<StoredEvaluation>, String> {
    store
        .decision_artifacts_with_prefix(
            tenant_id,
            &format!("evaluation:{record_id}:"),
            MAX_LOG_ROWS,
        )?
        .into_iter()
        .map(|(_, bytes)| decode_artifact(&bytes, "decision outcome evaluation"))
        .collect()
}

fn visibility_of(record: &StatisticalDecisionRecord, principal: &str) -> RecordVisibility {
    match record.candidate_source {
        CandidateSourceRecord::AgentLibrary { .. } => RecordVisibility::Tenant,
        CandidateSourceRecord::Graph { .. } | CandidateSourceRecord::Declared { .. } => {
            RecordVisibility::Principal {
                principal: principal.to_string(),
            }
        }
    }
}

fn commit(
    ctx: &ExecutionContext,
    reader: &LogReader,
    record: &StatisticalDecisionRecord,
    evaluator: Option<&eg_types::decision::statistical::log::NamedEvaluator>,
) -> Result<DecisionLogCommitted, String> {
    if record.caller_principal != reader.principal {
        return Err(
            "ACCESS_DENIED: a decision is logged by the principal that made it".to_string(),
        );
    }
    let logged = replay(ctx, record)?;
    let key = record_key(&logged.record_id);
    let committed = |replayed| DecisionLogCommitted {
        schema_version: DECISION_LOG_SCHEMA_VERSION,
        record_id: logged.record_id.clone(),
        record_digest: logged.record_digest.clone(),
        replayed,
    };
    if let Some(bytes) = ctx.store.decision_artifact(ctx.tenant_id, &key)? {
        let existing: DecisionLogEntry = decode_artifact(&bytes, "decision log entry")?;
        if existing.record.record_digest == logged.record_digest {
            return Ok(committed(true));
        }
        return Err(refusal(
            StatisticalErrorCode::IdempotencyConflict,
            "the record id is logged with other content",
        ));
    }
    let entry = DecisionLogEntry {
        schema_version: DECISION_LOG_SCHEMA_VERSION,
        visibility: visibility_of(&logged, &reader.principal),
        record: Box::new(logged.clone()),
        committed_by: reader.principal.clone(),
        committed_at_ms: ctx.now_ms,
        inputs: EntryInputs::Inline,
    };
    // EH-395: a named evaluator's grant is written in the same transaction.
    let mut rows =
        super::stat_evaluator::grant_rows(ctx, &logged.record_id, &reader.principal, evaluator)?;
    rows.push((key, encode_artifact(&entry)?));
    ctx.store.put_decision_artifacts(ctx.tenant_id, &rows)?;
    Ok(committed(false))
}

/// An evaluation joins an executed statistical record, or a committed solved
/// assembly (its slate, EH-012); anything else has nothing to evaluate.
fn check_evaluable(
    ctx: &ExecutionContext,
    reader: &LogReader,
    record_id: &str,
) -> Result<(), String> {
    let invalid = |detail: &str| refusal(StatisticalErrorCode::ParameterInvalid, detail);
    match super::stat_evaluator::evaluable_entry(ctx, reader, record_id)? {
        Some(entry) if executed_option(&entry.record.outcome).is_none() => {
            Err(invalid("the record executed no option"))
        }
        Some(_) => Ok(()),
        None => match slate_of(ctx.store, ctx.tenant_id, record_id)? {
            Some(_) => Ok(()),
            None => Err(invalid("no committed record with that id is visible")),
        },
    }
}

fn evaluate(
    ctx: &ExecutionContext,
    reader: &LogReader,
    evaluation: DecisionOutcomeEvaluation,
) -> Result<StoredEvaluation, String> {
    check_evaluable(ctx, reader, &evaluation.record_id)?;
    let key = evaluation_key(&evaluation.record_id, &evaluation.evaluation_id);
    let stored = StoredEvaluation {
        evaluation,
        producer: reader.principal.clone(),
        recorded_at_ms: ctx.now_ms,
    };
    store_once(ctx, key, "evaluation", stored, |existing, fresh| {
        existing.evaluation == fresh.evaluation && existing.producer == fresh.producer
    })
}

/// Store `fresh` under `key` exactly once. A re-send with the same content
/// from the same producer answers the stored row (idempotent replay);
/// anything else under that key is a conflict, never an overwrite.
pub(super) fn store_once<T: serde::Serialize + serde::de::DeserializeOwned>(
    ctx: &ExecutionContext,
    key: String,
    noun: &str,
    fresh: T,
    same: impl Fn(&T, &T) -> bool,
) -> Result<T, String> {
    if let Some(bytes) = ctx.store.decision_artifact(ctx.tenant_id, &key)? {
        let existing: T = decode_artifact(&bytes, noun)?;
        if same(&existing, &fresh) {
            return Ok(existing);
        }
        return Err(refusal(
            StatisticalErrorCode::IdempotencyConflict,
            format!("the {noun} id is recorded with other content"),
        ));
    }
    ctx.store
        .put_decision_artifacts(ctx.tenant_id, &[(key, encode_artifact(&fresh)?)])?;
    Ok(fresh)
}

/// Visible, executed records of `question_id` (or of every question) inside
/// the window, each with its evaluations.
fn joined(
    store: &AgentLibraryStore,
    reader: &LogReader,
    question_id: Option<&str>,
    window: Option<RecordWindow>,
) -> Result<Vec<(DecisionLogEntry, Vec<StoredEvaluation>)>, String> {
    let mut out = Vec::new();
    for entry in visible_entries(store, reader)? {
        let record = &entry.record;
        let in_question = question_id.is_none_or(|q| q == record.question.question_id);
        let in_window =
            window.is_none_or(|w| (w.from_ms..=w.to_ms).contains(&record.created_at_ms));
        if !in_question || !in_window || executed_option(&record.outcome).is_none() {
            continue;
        }
        let evaluations = evaluations_of(store, &reader.tenant_id, &record.record_id)?;
        out.push((entry, evaluations));
    }
    Ok(out)
}

/// The outcome aggregate the caller may read.
pub(super) fn aggregate_log(
    store: &AgentLibraryStore,
    reader: &LogReader,
    request: &OutcomeAggregateRequest,
) -> Result<OutcomeAggregate, String> {
    let rows = joined(
        store,
        reader,
        request.question_id.as_deref(),
        Some(request.window),
    )?;
    let records: Vec<JoinedRecord> = rows
        .iter()
        .filter_map(|(entry, evaluations)| {
            Some(JoinedRecord {
                option_id: executed_option(&entry.record.outcome)?,
                question_id: &entry.record.question.question_id,
                policy_digest: &entry.record.inputs.policy_digest,
                decider: &entry.record.caller_principal,
                evaluations,
            })
        })
        .collect();
    let slates = evaluated_slates(
        store,
        &reader.tenant_id,
        request.question_id.as_deref(),
        (request.window.from_ms, request.window.to_ms),
        MAX_LOG_ROWS,
    )?;
    let records: Vec<JoinedRecord> = records
        .into_iter()
        .chain(slates.iter().map(|(slate, evaluations)| JoinedRecord {
            option_id: &slate.option_id,
            question_id: SLATE_QUESTION,
            policy_digest: &slate.policy_digest,
            decider: &slate.decider,
            evaluations,
        }))
        .collect();
    let policy = default_statistical_policy();
    let rules = AggregateRules {
        min_support: policy.min_support,
        fidelity_floor: policy.min_outcome_fidelity,
        cross_question: request.question_id.is_none(),
    };
    Ok(OutcomeAggregate {
        schema_version: DECISION_LOG_SCHEMA_VERSION,
        min_support: policy.min_support,
        rows: aggregate(&records, &rules).map_err(|r| r.render())?,
    })
}

fn logged_item(
    entry: &DecisionLogEntry,
    record: &StatisticalDecisionRecord,
    stored: &StoredEvaluation,
) -> Option<LabelledItem> {
    let FeatureMatrixRef::Inline {
        candidate_ids,
        values,
        ..
    } = &record.inputs.feature_matrix
    else {
        return None;
    };
    let aligned = !values.is_empty() && record.logging_propensities.len() == candidate_ids.len();
    let executed = executed_option(&record.outcome).filter(|_| aligned)?;
    let e = &stored.evaluation;
    Some(LabelledItem {
        item_id: record.record_id.clone(),
        recorded_at_ms: record.created_at_ms,
        class_key: record.question.question_id.clone(),
        candidate_ids: candidate_ids.clone(),
        features: values.clone(),
        label: ItemLabel::Logged(Box::new(LoggedOutcome {
            executed: executed.to_string(),
            logging_propensities: record.logging_propensities.clone(),
            propensity_source: PropensitySource::ExecutedPolicy,
            pinned: false,
            commit_principal: entry.committed_by.clone(),
            evaluation: OutcomeEvaluation {
                evaluation_id: e.evaluation_id.clone(),
                class: e.class,
                producer: stored.producer.clone(),
                selected_agent: e.selected_agent.clone(),
                lease_holder: e.lease_holder.clone(),
                fidelity: e.fidelity,
                success: e.success,
            },
        })),
        audit_inclusion: record
            .audit
            .filter(|a| a.sampled)
            .map(|a| a.inclusion_probability),
    })
}

/// The bandit-label dataset of one question, read from the decision log:
/// every visible, executed record computed under the feature schema whose
/// content digest is `schema_digest`, joined with its first evaluation.
pub(super) fn logged_dataset(
    store: &AgentLibraryStore,
    reader: &LogReader,
    question_id: &str,
    schema_digest: &str,
) -> Result<LabelledDataset, String> {
    let mut items = Vec::new();
    let mut names = Vec::new();
    let mut synthetic = false;
    for (entry, evaluations) in joined(store, reader, Some(question_id), None)? {
        let pin = &entry.record.inputs.feature_schema;
        let schema = pinned_entry(
            store,
            &reader.tenant_id,
            pin,
            AgentComponentKind::FeatureSchema,
        )?;
        let (Some(stored), true) = (evaluations.first(), schema.content_digest == schema_digest)
        else {
            continue;
        };
        let Some(record) = full_record(&reader.retention, &reader.tenant_id, &entry)? else {
            continue;
        };
        if let FeatureMatrixRef::Inline { feature_names, .. } = &record.inputs.feature_matrix {
            names = feature_names.iter().cloned().collect();
        }
        synthetic |= record.synthetic_evidence;
        items.extend(logged_item(&entry, &record, stored));
    }
    if items.is_empty() {
        return Err(refusal(
            StatisticalErrorCode::NoAdmissibleLabels,
            format!("no logged, evaluated decision of {question_id}"),
        ));
    }
    let invalid = |detail: String| refusal(StatisticalErrorCode::DatasetInvalid, detail);
    Ok(LabelledDataset {
        schema_version: LABELLED_DATASET_SCHEMA_VERSION,
        feature_schema_digest: schema_digest.to_string(),
        feature_names: BoundedVec::new(names).map_err(invalid)?,
        scale: QuantScaleTag::Q32,
        items: BoundedVec::new(items).map_err(invalid)?,
        synthetic,
    })
}

fn dispatch(
    ctx: &ExecutionContext,
    reader: &LogReader,
    op: DecisionLogOp,
    inputs: Option<&super::stat_learning::LearningInputs>,
) -> Result<ResultPayload, String> {
    use eg_types::result_contract::coordination::{
        DecisionLogAggregate, DecisionLogCommit, DecisionLogCompact, DecisionLogEvaluate,
        DecisionLogGet, DecisionLogResolve, DecisionLogVerify,
    };
    match op {
        DecisionLogOp::Commit { record, evaluator } => ResultPayload::of::<DecisionLogCommit>(
            commit(ctx, reader, &record, evaluator.as_ref())?,
        ),
        DecisionLogOp::Evaluate { evaluation, .. } => {
            ResultPayload::of::<DecisionLogEvaluate>(evaluate(ctx, reader, evaluation)?)
        }
        DecisionLogOp::Get { record_id, .. } => {
            ResultPayload::of::<DecisionLogGet>(visible_entry(ctx.store, reader, &record_id)?)
        }
        DecisionLogOp::Aggregate { request } => {
            ResultPayload::of::<DecisionLogAggregate>(aggregate_log(ctx.store, reader, &request)?)
        }
        DecisionLogOp::Compact { policy, limit, .. } => ResultPayload::of::<DecisionLogCompact>(
            compact(ctx, &reader.retention, &policy, limit)?,
        ),
        DecisionLogOp::Resolve { resolution, .. } => {
            ResultPayload::of::<DecisionLogResolve>(resolve(ctx, reader, resolution)?)
        }
        DecisionLogOp::Learn { write, .. } => {
            ResultPayload::of::<eg_types::result_contract::coordination::DecisionLogLearn>(
                super::stat_learning::dispatch(ctx, reader, write, inputs)?,
            )
        }
        DecisionLogOp::Verify { record_id, .. } => ResultPayload::of::<DecisionLogVerify>(verify(
            ctx,
            reader,
            &reader.retention,
            &record_id,
        )?),
    }
}

async fn serve(
    state: &SharedState,
    verified: &VerifiedRequestContext,
    op: DecisionLogOp,
) -> Result<ResultPayload, String> {
    if op.tenant_id() != verified.tenant() {
        return Err(
            "ACCESS_DENIED: DecisionLog tenant must match the verified request tenant".to_string(),
        );
    }
    let (store, secret) = {
        let mut guard = state.write().await;
        (guard.ensure_agent_library()?, guard.auth_secret.clone())
    };
    let reader = LogReader::served(state, verified).await;
    let inputs = super::stat_learning::prepare(state, verified, &op).await?;
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let ctx = ExecutionContext {
            store: store.as_ref(),
            tenant_id: &reader.tenant_id,
            now_ms,
            server_secret: secret.as_bytes(),
        };
        dispatch(&ctx, &reader, op, inputs.as_ref())
    })
    .await
    .map_err(|error| format!("DecisionLog task failed: {error}"))?;
    telemetry::logged(result.is_ok(), started);
    result
}

/// Serve one `DecisionLog` op.
pub(super) async fn handle_log(
    state: &SharedState,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: DecisionLogOp,
) -> Response {
    let span = telemetry::span("DecisionLog", verified.tenant());
    match serve(state, verified, op).instrument(span).await {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => {
            telemetry::refused("DecisionLog", &error);
            Response::err(req_id, error)
        }
    }
}

/// Fill every `OutcomeRate` feature's numeric fact from the log's pooled
/// rates (§6.1 outcome statistics, EH-063). Those rates summarise other
/// principals' runs, so they are readable only when the policy declares
/// cross-principal features tenant-public; an option below `min_support`
/// gets no fact and falls to the feature's declared missing-value rule.
pub(super) fn fill_outcome_rates(
    store: &AgentLibraryStore,
    reader: &LogReader,
    schema: &FeatureSchemaBody,
    tenant_public: bool,
    views: &mut [CandidateView],
) -> Result<(), String> {
    for spec in &schema.features {
        let FeatureKind::OutcomeRate { question_id } = &spec.kind else {
            continue;
        };
        if !tenant_public {
            return Err(refusal(
                StatisticalErrorCode::ParameterInvalid,
                "an outcome-rate feature reads other principals' runs; the policy must declare \
                 tenant_public_features",
            ));
        }
        let request = OutcomeAggregateRequest {
            tenant_id: reader.tenant_id.clone(),
            question_id: Some(question_id.clone()),
            window: RecordWindow {
                from_ms: 0,
                to_ms: u64::MAX,
            },
        };
        let key = outcome_rate_key(question_id);
        let mut best: BTreeMap<String, (u64, i64)> = BTreeMap::new();
        for row in aggregate_log(store, reader, &request)?.rows.iter() {
            let Some(rate) = row.pooled_rate else {
                continue;
            };
            let slot = best.entry(row.option_id.clone()).or_insert((0, rate.value));
            if row.trials > slot.0 {
                *slot = (row.trials, rate.value);
            }
        }
        for view in views.iter_mut() {
            if let Some((_, value)) = best.get(&view.id) {
                view.numbers.insert(key.clone(), *value);
            }
        }
    }
    Ok(())
}
