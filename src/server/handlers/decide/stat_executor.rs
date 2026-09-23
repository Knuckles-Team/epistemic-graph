//! The decision executor behind `Method::Decide` (EH-059, EH-005/006/023–025).
//!
//! It never builds a `RowSet` and never touches `wire::Op`: the candidates are
//! already the visible options, the feature schema and head are pinned
//! component bodies, and the whole answer is one schematised record. Order of
//! work, per the composition rule (§3.4): visibility (the candidate read) →
//! features over exactly those options → the head's advisory reading → the
//! act/abstain rule and any budgeted exploration inside the legal set.

use eg_numeric::decision::candidate::CandidateView;
use eg_numeric::decision::exploration::permit;
use eg_numeric::decision::features::{feature_matrix, FeatureInputs, FeatureMatrix, MatrixOutcome};
use eg_numeric::decision::head_eval::{
    check_compatible, explanation, read_head, Evaluated, HeadReading,
};
use eg_numeric::decision::ladder::{decide, LadderInputs, LadderResult};
use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::features::FeatureSchemaBody;
use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::statistical::keyed::{decision_seed, exploration_key};
use eg_types::decision::statistical::{
    DecideRequest, StatisticalErrorCode, StatisticalOutcome, StatisticalQuestion, TypedParam,
};
use eg_types::decision::AbstainReason;

use super::stat_support::{pinned_body, refusal, ResolvedPolicy};
use crate::server::persistence::agent_library::AgentLibraryStore;

/// What the executor needs besides the request.
pub(super) struct ExecutionContext<'a> {
    pub(super) store: &'a AgentLibraryStore,
    pub(super) tenant_id: &'a str,
    pub(super) now_ms: u64,
    pub(super) server_secret: &'a [u8],
}

/// The pinned inputs of one decision.
pub(super) struct Pinned {
    pub(super) schema: FeatureSchemaBody,
    pub(super) schema_digest: String,
    pub(super) head: Option<(DecisionHeadBody, String)>,
}

/// What the executor concluded, before it is sealed into a record.
pub(super) struct Executed {
    pub(super) matrix: Option<FeatureMatrix>,
    pub(super) ladder: LadderResult,
    pub(super) reading: Option<Evaluated>,
    pub(super) seed: [u8; 32],
}

/// Read and validate the feature schema and head a decision pins.
pub(super) fn pinned_inputs(
    ctx: &ExecutionContext,
    schema_pin: &ComponentDependency,
    head_pin: Option<&ComponentDependency>,
) -> Result<Pinned, String> {
    let (schema, schema_digest) = pinned_body::<FeatureSchemaBody>(
        ctx.store,
        ctx.tenant_id,
        schema_pin,
        AgentComponentKind::FeatureSchema,
        StatisticalErrorCode::FeatureSchemaInvalid,
    )?;
    let schema = schema
        .checked()
        .map_err(|detail| refusal(StatisticalErrorCode::FeatureSchemaInvalid, detail))?;
    let head = head_pin
        .map(|pin| {
            let (body, digest) = pinned_body::<DecisionHeadBody>(
                ctx.store,
                ctx.tenant_id,
                pin,
                AgentComponentKind::DecisionHead,
                StatisticalErrorCode::HeadInvalid,
            )?;
            let body = body
                .checked()
                .map_err(|detail| refusal(StatisticalErrorCode::HeadInvalid, detail))?;
            Ok::<_, String>((body, digest))
        })
        .transpose()?;
    Ok(Pinned {
        schema,
        schema_digest,
        head,
    })
}

fn abstained(reason: AbstainReason) -> LadderResult {
    LadderResult {
        outcome: StatisticalOutcome::Abstained {
            reasons: BoundedVec::new(vec![reason]).expect("one reason is inside the bound"),
        },
        calibration: None,
        explained: None,
        audit: None,
        logging: Vec::new(),
    }
}

fn head_reading(
    pinned: &Pinned,
    matrix: &FeatureMatrix,
) -> Result<Option<Result<Evaluated, AbstainReason>>, String> {
    let Some((head, _)) = &pinned.head else {
        return Ok(None);
    };
    check_compatible(head, &pinned.schema_digest, matrix).map_err(|r| r.render())?;
    Ok(Some(
        match read_head(head, matrix).map_err(|r| r.render())? {
            HeadReading::InDistribution(evaluated) => Ok(evaluated),
            HeadReading::OutOfDistribution { .. } => Err(AbstainReason::InsufficientConfidence),
        },
    ))
}

/// The state digest the exploration seed is keyed on: everything the decision
/// read, so a caller cannot change the draw without changing the inputs.
pub(super) fn state_digest(
    question: &StatisticalQuestion,
    params: &[TypedParam],
    matrix: &FeatureMatrix,
    policy: &ResolvedPolicy,
    now_ms: u64,
) -> String {
    eg_types::decision::digest::digest_text(
        "eg/decide-state/v1",
        &(
            question,
            Some((&matrix.candidate_ids, &matrix.values)),
            &policy.digest,
            params,
            now_ms,
        ),
    )
}

/// What the decision function reads once the matrix exists. `execute` builds
/// it from live candidates; the log's verify-replay builds it from a record.
pub(super) struct MatrixInputs<'a> {
    pub(super) question: &'a StatisticalQuestion,
    pub(super) params: &'a [TypedParam],
    pub(super) now_ms: u64,
    pub(super) server_secret: &'a [u8],
}

/// The decision function over a complete matrix: head reading, drift check,
/// keyed seed and the act/abstain ladder.
pub(super) fn run_on_matrix(
    inputs: &MatrixInputs,
    pinned: &Pinned,
    policy: &ResolvedPolicy,
    matrix: FeatureMatrix,
) -> Result<Executed, String> {
    let exploration = permit(&policy.policy, inputs.question).map_err(|r| r.render())?;
    let digest = state_digest(
        inputs.question,
        inputs.params,
        &matrix,
        policy,
        inputs.now_ms,
    );
    let seed = decision_seed(
        &exploration_key(inputs.server_secret),
        &digest,
        &inputs.question.question_id,
    );
    let reading = match head_reading(pinned, &matrix)? {
        Some(Err(reason)) => {
            return Ok(Executed {
                matrix: Some(matrix),
                ladder: abstained(reason),
                reading: None,
                seed,
            });
        }
        Some(Ok(evaluated)) => Some(evaluated),
        None => None,
    };
    let ladder = decide(&LadderInputs {
        candidate_ids: &matrix.candidate_ids,
        head: pinned.head.as_ref().map(|(head, _)| head),
        reading: reading.as_ref(),
        policy: &policy.policy,
        statistical: &policy.statistical,
        permit: exploration,
        seed: &seed,
    })
    .map_err(|r| r.render())?;
    Ok(Executed {
        matrix: Some(matrix),
        ladder,
        reading,
        seed,
    })
}

/// Run the ladder over the candidates.
pub(super) fn execute(
    ctx: &ExecutionContext,
    request: &DecideRequest,
    pinned: &Pinned,
    policy: &ResolvedPolicy,
    views: &[CandidateView],
) -> Result<Executed, String> {
    permit(&policy.policy, &request.question).map_err(|r| r.render())?;
    let features = FeatureInputs {
        params: request.params.as_slice(),
        now_ms: ctx.now_ms,
    };
    let matrix = match feature_matrix(&pinned.schema, views, &features).map_err(|r| r.render())? {
        MatrixOutcome::Complete(matrix) => matrix,
        MatrixOutcome::UnknownFact {
            component_id,
            field,
        } => {
            return Ok(Executed {
                matrix: None,
                ladder: abstained(AbstainReason::UnknownFact {
                    component_id,
                    field,
                }),
                reading: None,
                seed: [0u8; 32],
            });
        }
    };
    let inputs = MatrixInputs {
        question: &request.question,
        params: request.params.as_slice(),
        now_ms: ctx.now_ms,
        server_secret: ctx.server_secret,
    };
    run_on_matrix(&inputs, pinned, policy, matrix)
}

/// The recorded explanation of the explained option, for a linear head.
pub(super) fn recorded_explanation(
    pinned: &Pinned,
    executed: &Executed,
) -> Result<Option<eg_types::decision::statistical::LinearExplanation>, String> {
    let (Some((head, _)), Some(reading), Some(index), Some(matrix)) = (
        &pinned.head,
        &executed.reading,
        executed.ladder.explained,
        &executed.matrix,
    ) else {
        return Ok(None);
    };
    explanation(
        head,
        &matrix.candidate_ids[index],
        &reading.standardised[index],
    )
    .map(Some)
    .map_err(|r| r.render())
}
