//! `Method::Decide`: the statistical executor, served (EH-056, EH-059).
//!
//! Evaluate-only: it reads the visible candidates, the pinned feature schema,
//! head and policy, runs the ladder and answers a `DecisionBatch` of one
//! sealed record. It commits nothing, so the exploration seed's reveal is
//! left to the commit that makes the record durable.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;
use tracing::Instrument;

use eg_types::contract::BoundedVec;
use eg_types::decision::digest::{digest_text, statistical_record_digest};
use eg_types::decision::statistical::keyed::seed_commitment;
use eg_types::decision::statistical::nl::{NlBinding, NlChoiceSource};
use eg_types::decision::statistical::{
    CandidateSource, DecideRequest, DecisionBatch, ExplorationRecord, FeatureMatrixRef,
    ShortlistProvenance, StatisticalDecisionRecord, StatisticalErrorCode, StatisticalInputs,
    StatisticalOutcome,
};
use eg_types::decision::{
    EvidenceClass, PremiseClass, PremiseProvenance, PremiseRef, QuantScaleTag, ResolutionKind,
    TraceFidelity, STATISTICAL_DECISION_RECORD_SCHEMA_VERSION,
};

use eg_numeric::decision::ladder::LadderResult;

use super::candidates::{read_candidates, ReadCandidates};
use super::stat_belief::{live_slices, Slices};
use super::stat_executor::{
    execute, pinned_inputs, recorded_explanation, Executed, ExecutionContext, Pinned,
};
use super::stat_log::{fill_outcome_rates, LogReader};
use super::stat_nl::binding;
use super::stat_support::{refusal, resolve_policy, ResolvedPolicy};
use super::stat_tool_subset::tool_subset_outcome;
use super::telemetry;
use crate::protocol::{Response, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

/// Domain of a statistical record's input digest.
pub(super) const STATISTICAL_INPUTS_DOMAIN: &str = "eg/decision-statistical-inputs/v1";

/// Answer one statistical decision; commit nothing.
pub(super) async fn handle_decide(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: DecideRequest,
) -> Response {
    let span = telemetry::span("Decide", verified.tenant());
    let result = serve(state, verified, request).instrument(span).await;
    match result.and_then(ResultPayload::of::<eg_types::result_contract::query::Decide>) {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => {
            telemetry::refused("Decide", &error);
            Response::err(req_id, error)
        }
    }
}

async fn serve(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    request: DecideRequest,
) -> Result<DecisionBatch, String> {
    if request.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: Decide tenant must match the verified request tenant".to_string(),
        );
    }
    if request.max_records == Some(0) {
        return Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "max_records must be at least 1",
        ));
    }
    let (store, secret) = {
        let mut guard = state.write().await;
        (guard.ensure_agent_library()?, guard.auth_secret.clone())
    };
    let graph_view =
        super::candidates::filtered_graph_view(state, verified.agent_id(), &request.candidates)
            .await?;
    let principal = verified.principal_persistence_id();
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let started = Instant::now();
    tokio::task::spawn_blocking(move || {
        let ctx = ExecutionContext {
            store: store.as_ref(),
            tenant_id: &request.tenant_id,
            now_ms,
            server_secret: secret.as_bytes(),
        };
        decide_blocking(&ctx, &request, graph_view.as_ref(), principal, started)
    })
    .await
    .map_err(|error| format!("Decide task failed: {error}"))?
}

fn decide_blocking(
    ctx: &ExecutionContext,
    request: &DecideRequest,
    graph_view: Option<&eg_core::graph::GraphView>,
    principal: String,
    started: Instant,
) -> Result<DecisionBatch, String> {
    let mut candidates = read_candidates(
        ctx.store,
        ctx.tenant_id,
        &request.candidates,
        graph_view,
        &principal,
        request.question.kind,
    )?;
    let pinned = pinned_inputs(ctx, &request.feature_schema, request.head.as_ref())?;
    let policy = resolve_policy(ctx.store, ctx.tenant_id, &request.policy)?;
    let reader = LogReader {
        tenant_id: ctx.tenant_id.to_string(),
        principal: principal.clone(),
        roles: Vec::new(),
        retention: super::stat_retention::Retention::none(),
    };
    fill_outcome_rates(
        ctx.store,
        &reader,
        &pinned.schema,
        policy.statistical.tenant_public_features,
        &mut candidates.views,
    )?;
    let executed = match tool_subset_outcome(request) {
        Some(outcome_result) => Executed {
            matrix: None,
            ladder: LadderResult {
                outcome: outcome_result?,
                calibration: None,
                explained: None,
                audit: None,
                logging: Vec::new(),
            },
            reading: None,
            seed: [0u8; 32],
        },
        None => execute(ctx, request, &pinned, &policy, &candidates.views)?,
    };
    let slices = live_slices(ctx, request, &pinned, &candidates.views)?;
    let nl = binding(request, &executed.ladder.outcome, &candidates.entries)?;
    let record = seal(
        Sealing {
            ctx,
            request,
            pinned: &pinned,
            policy: &policy,
            candidates: &candidates,
            principal,
            slices: &slices,
        },
        &executed,
        nl,
    )?;
    telemetry::decided(&record, candidates.views.len(), started);
    let records = BoundedVec::new(vec![record])
        .map_err(|detail| refusal(StatisticalErrorCode::ParameterInvalid, detail))?;
    Ok(DecisionBatch {
        schema_version: STATISTICAL_DECISION_RECORD_SCHEMA_VERSION,
        inputs_digest: records.as_slice()[0].inputs_digest.clone(),
        records,
    })
}

/// Everything sealing reads besides the executed answer.
struct Sealing<'a> {
    ctx: &'a ExecutionContext<'a>,
    request: &'a DecideRequest,
    pinned: &'a Pinned,
    policy: &'a ResolvedPolicy,
    candidates: &'a ReadCandidates,
    principal: String,
    slices: &'a Slices,
}

fn bounded<T, const N: usize>(values: Vec<T>) -> Result<BoundedVec<T, N>, String> {
    BoundedVec::new(values)
        .map_err(|detail| refusal(StatisticalErrorCode::CandidateSetTooLarge, detail))
}

fn feature_matrix(
    views: &[eg_numeric::decision::candidate::CandidateView],
    pinned: &Pinned,
    executed: &Executed,
) -> Result<FeatureMatrixRef, String> {
    let (ids, values) = match &executed.matrix {
        Some(matrix) => (matrix.candidate_ids.clone(), matrix.values.clone()),
        None => (views.iter().map(|v| v.id.clone()).collect(), Vec::new()),
    };
    Ok(FeatureMatrixRef::Inline {
        candidate_ids: bounded(ids)?,
        feature_names: bounded(pinned.schema.names())?,
        scale: QuantScaleTag::Q32,
        values: bounded(values)?,
    })
}

fn exploration(sealing: &Sealing, executed: &Executed) -> Option<ExplorationRecord> {
    let eg_types::decision::ColdStart::Explore { budget } = &sealing.policy.policy.cold_start
    else {
        return None;
    };
    Some(ExplorationRecord {
        seed_commitment: seed_commitment(&executed.seed),
        revealed_seed: None,
        budget_digest: digest_text("eg/decide-exploration-budget/v1", budget),
    })
}

fn premise(
    subject: &str,
    fact: &str,
    class: PremiseClass,
    provenance: PremiseProvenance,
) -> PremiseRef {
    PremiseRef {
        subject: subject.to_string(),
        fact: fact.to_string(),
        class,
        provenance,
    }
}

/// One CLAIM premise per rule-derived class (EH-200): a derivation over a
/// publisher's declared facts is only as strong as those facts.
fn derived_premises<'a>(sealing: &'a Sealing) -> impl Iterator<Item = PremiseRef> + 'a {
    sealing.candidates.entries.iter().flat_map(move |entry| {
        let classes = sealing
            .candidates
            .derived
            .by_component
            .get(&entry.component_id);
        classes.into_iter().flatten().map(move |(class, rule)| {
            premise(
                &entry.component_id,
                &format!("derived:{class} by {rule}"),
                PremiseClass::Claim,
                PremiseProvenance::Publisher {
                    component_id: entry.component_id.clone(),
                    definition_digest: entry.definition_digest.clone(),
                },
            )
        })
    })
}

/// One CLAIM premise per caller-declared option: the caller vouches for its
/// facts, nobody else does.
fn declared_premises(request: &DecideRequest) -> Vec<PremiseRef> {
    let CandidateSource::Declared { options } = &request.candidates else {
        return Vec::new();
    };
    options
        .iter()
        .map(|option| {
            premise(
                &option.option_id,
                "declared_option",
                PremiseClass::Claim,
                PremiseProvenance::Request {
                    field: "candidates.options".to_string(),
                },
            )
        })
        .collect()
}

fn premises(sealing: &Sealing, nl: Option<&NlBinding>) -> Vec<PremiseRef> {
    let schema = &sealing.request.feature_schema;
    let mut out = vec![
        premise(
            &schema.component_id,
            "feature_schema",
            PremiseClass::Definition,
            PremiseProvenance::Publisher {
                component_id: schema.component_id.clone(),
                definition_digest: schema.definition_digest.clone(),
            },
        ),
        premise(
            "policy",
            "decision_policy",
            PremiseClass::Definition,
            PremiseProvenance::Policy {
                policy_digest: sealing.policy.digest.clone(),
            },
        ),
    ];
    if let (Some(pin), Some((head, _))) = (&sealing.request.head, &sealing.pinned.head) {
        let class = if head.synthetic {
            PremiseClass::Claim
        } else {
            PremiseClass::Observation
        };
        out.push(premise(
            &pin.component_id,
            "decision_head",
            class,
            PremiseProvenance::Publisher {
                component_id: pin.component_id.clone(),
                definition_digest: pin.definition_digest.clone(),
            },
        ));
    }
    out.extend(derived_premises(sealing));
    out.extend(declared_premises(sealing.request));
    if let Some(NlBinding {
        source:
            NlChoiceSource::LlmProposal {
                producer,
                prompt_digest,
            },
        template,
        ..
    }) = nl
    {
        out.push(premise(
            &template.component_id,
            "llm_template_proposal",
            PremiseClass::Claim,
            PremiseProvenance::ClaimedMapping {
                text_digest: prompt_digest.clone(),
                producer: producer.clone(),
            },
        ));
    }
    out
}

fn premise_evidence(class: PremiseClass) -> EvidenceClass {
    match class {
        PremiseClass::Definition | PremiseClass::Proof => EvidenceClass::Proof,
        PremiseClass::Observation => EvidenceClass::Observation,
        PremiseClass::Claim => EvidenceClass::Claim,
    }
}

/// The weakest premise, and never stronger than a claim for an answer no
/// calibration licenses (advisory scores and exploration draws).
fn evidence_class(premises: &[PremiseRef], outcome: &StatisticalOutcome) -> EvidenceClass {
    let weakest = premises
        .iter()
        .map(|p| premise_evidence(p.class))
        .max()
        .unwrap_or(EvidenceClass::Proof);
    match outcome {
        StatisticalOutcome::Advisory { .. } | StatisticalOutcome::Explored { .. } => {
            EvidenceClass::Claim
        }
        StatisticalOutcome::Acted { .. } | StatisticalOutcome::Abstained { .. } => weakest,
    }
}

fn resolution(outcome: &StatisticalOutcome) -> ResolutionKind {
    match outcome {
        StatisticalOutcome::Abstained { .. } => ResolutionKind::Abstention,
        StatisticalOutcome::Acted { .. }
        | StatisticalOutcome::Explored { .. }
        | StatisticalOutcome::Advisory { .. } => ResolutionKind::Statistical,
    }
}

fn seal(
    sealing: Sealing,
    executed: &Executed,
    nl: Option<NlBinding>,
) -> Result<StatisticalDecisionRecord, String> {
    let inputs = StatisticalInputs {
        feature_schema: sealing.request.feature_schema.clone(),
        head: sealing.request.head.clone(),
        policy: sealing.request.policy.clone(),
        policy_digest: sealing.policy.digest.clone(),
        params: sealing.request.params.clone(),
        classification_rules: sealing.candidates.derived.rules.clone(),
        feature_matrix: feature_matrix(&sealing.candidates.views, sealing.pinned, executed)?,
        shortlist: ShortlistProvenance {
            ann_recall_mode: None,
            now_ms: sealing.ctx.now_ms,
            embedder_model_digest: None,
        },
        exploration: exploration(&sealing, executed),
        belief_slices: bounded(sealing.slices.inputs.clone())?,
    };
    let inputs_digest = digest_text(
        STATISTICAL_INPUTS_DOMAIN,
        &(&sealing.request.question, &inputs),
    );
    let premise_list = premises(&sealing, nl.as_ref());
    let outcome = executed.ladder.outcome.clone();
    let synthetic = sealing
        .pinned
        .head
        .as_ref()
        .is_some_and(|(head, _)| head.synthetic);
    let mut record = StatisticalDecisionRecord {
        schema_version: STATISTICAL_DECISION_RECORD_SCHEMA_VERSION,
        record_id: format!("decision:{}", inputs_digest.trim_start_matches("sha256:")),
        tenant_id: sealing.ctx.tenant_id.to_string(),
        caller_principal: sealing.principal,
        created_at_ms: sealing.ctx.now_ms,
        question: sealing.request.question.clone(),
        candidate_source: sealing.candidates.record.clone(),
        inputs,
        inputs_digest,
        resolution_kind: resolution(&outcome),
        evidence_class: evidence_class(&premise_list, &outcome),
        trace_fidelity: TraceFidelity::FullStep,
        premises: BoundedVec::new(premise_list)
            .map_err(|detail| refusal(StatisticalErrorCode::ParameterInvalid, detail))?,
        outcome,
        calibration: executed.ladder.calibration,
        explanation: recorded_explanation(sealing.pinned, executed)?,
        audit: executed.ladder.audit,
        logging_propensities: bounded(executed.ladder.logging.clone())?,
        nl_binding: nl,
        belief: bounded(sealing.slices.points.clone())?,
        synthetic_evidence: synthetic,
        record_digest: String::new(),
    };
    record.record_digest = statistical_record_digest(&record);
    Ok(record)
}
