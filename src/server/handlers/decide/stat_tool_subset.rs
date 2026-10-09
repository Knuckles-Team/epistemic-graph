//! EG-DECISION-ENGINE-R045.2: wires the typed tool-subset selection
//! (`tool_subset::smallest_covering_subset`, R045.1) into the `ToolSubset`
//! `Decide` question kind.
//!
//! The caller declares each tool candidate (`CandidateSource::Declared`,
//! classification = the capabilities it covers, a `token_cost` declared
//! number) and names the required capabilities and context budget as typed
//! request params. The selected minimal covering subset is sealed as an
//! `Advisory` outcome: one `ScoredOption` per selected tool at full (Q32
//! `1.0`) uncalibrated score. `resolution_kind`/`evidence_class` are still
//! computed generically from this outcome by `stat_decide`, exactly as for
//! every other question kind -- this module decides only which tools were
//! selected.

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::declared::DeclaredOption;
use eg_types::decision::statistical::tool_subset::{
    smallest_covering_subset, ToolCoverageCandidate, ToolSubsetRefusal, ToolSubsetRequestBody,
};
use eg_types::decision::statistical::{
    CandidateSource, DecideRequest, QuestionKind, ScoredOption, StatisticalErrorCode,
    StatisticalOutcome, TypedValue,
};
use eg_types::decision::{QuantScaleTag, QuantisedValue};

use super::stat_support::refusal;

/// The Q32 fixed-point value for "selected" (uncalibrated full weight).
const SELECTED_SCORE: i64 = 1i64 << 32;

/// `Some` only for a `ToolSubset` question: the outcome to seal in place of
/// the ordinary scored ladder.
pub(super) fn tool_subset_outcome(
    request: &DecideRequest,
) -> Option<Result<StatisticalOutcome, String>> {
    (request.question.kind == QuestionKind::ToolSubset).then(|| compute(request))
}

fn required_capabilities(request: &DecideRequest) -> Result<Vec<String>, String> {
    for param in request.params.iter() {
        if param.name == "required_capabilities" {
            return match &param.value {
                TypedValue::IriList(list) => Ok(list.iter().cloned().collect()),
                _ => Err(refusal(
                    StatisticalErrorCode::ParameterInvalid,
                    "required_capabilities must be an iri_list",
                )),
            };
        }
    }
    Err(refusal(
        StatisticalErrorCode::ParameterInvalid,
        "a tool_subset question requires a required_capabilities (iri_list) param",
    ))
}

fn context_budget_tokens(request: &DecideRequest) -> Result<u32, String> {
    for param in request.params.iter() {
        if param.name == "context_budget_tokens" {
            return match &param.value {
                TypedValue::Int(tokens) if *tokens >= 0 => Ok(*tokens as u32),
                TypedValue::Int(_) => Err(refusal(
                    StatisticalErrorCode::ParameterInvalid,
                    "context_budget_tokens must not be negative",
                )),
                _ => Err(refusal(
                    StatisticalErrorCode::ParameterInvalid,
                    "context_budget_tokens must be an int",
                )),
            };
        }
    }
    Err(refusal(
        StatisticalErrorCode::ParameterInvalid,
        "a tool_subset question requires a context_budget_tokens (int) param",
    ))
}

fn declared_candidates(request: &DecideRequest) -> Result<&[DeclaredOption], String> {
    match &request.candidates {
        CandidateSource::Declared { options } => Ok(options.as_slice()),
        _ => Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "a tool_subset question declares its own tool candidates",
        )),
    }
}

/// The declared `token_cost` number, or 0 when the caller did not declare
/// one (a free tool).
fn token_cost(option: &DeclaredOption) -> u32 {
    option
        .numbers
        .iter()
        .find(|number| number.key == "token_cost")
        .map(|number| number.q32.max(0) as u32)
        .unwrap_or(0)
}

fn request_body(request: &DecideRequest) -> Result<ToolSubsetRequestBody, String> {
    let required = required_capabilities(request)?;
    let context_budget_tokens = context_budget_tokens(request)?;
    let options = declared_candidates(request)?;
    let mut candidates = Vec::with_capacity(options.len());
    for option in options {
        candidates.push(ToolCoverageCandidate {
            tool_id: option.option_id.clone(),
            capabilities: BoundedVec::new(option.classification.iter().cloned().collect())
                .map_err(|detail| refusal(StatisticalErrorCode::CandidateSetTooLarge, detail))?,
            token_cost: token_cost(option),
        });
    }
    Ok(ToolSubsetRequestBody {
        required_capabilities: BoundedVec::new(required)
            .map_err(|detail| refusal(StatisticalErrorCode::ParameterInvalid, detail))?,
        context_budget_tokens,
        candidates: BoundedVec::new(candidates)
            .map_err(|detail| refusal(StatisticalErrorCode::CandidateSetTooLarge, detail))?,
    })
}

fn refusal_reason(kind: ToolSubsetRefusal) -> &'static str {
    match kind {
        ToolSubsetRefusal::CapabilityUncoverable => {
            "no candidate subset covers every required capability"
        }
        ToolSubsetRefusal::BudgetExceeded => "every covering subset exceeds the context budget",
    }
}

fn compute(request: &DecideRequest) -> Result<StatisticalOutcome, String> {
    let body = request_body(request)?;
    let chosen = smallest_covering_subset(&body)
        .map_err(|kind| refusal(StatisticalErrorCode::ParameterInvalid, refusal_reason(kind)))?;
    let scores: Vec<ScoredOption> = chosen
        .into_iter()
        .map(|tool_id| ScoredOption {
            option_id: tool_id,
            score: QuantisedValue {
                scale: QuantScaleTag::Q32,
                value: SELECTED_SCORE,
            },
            probability: None,
        })
        .collect();
    Ok(StatisticalOutcome::Advisory {
        scores: BoundedVec::new(scores)
            .map_err(|detail| refusal(StatisticalErrorCode::CandidateSetTooLarge, detail))?,
        calibrated: false,
    })
}
