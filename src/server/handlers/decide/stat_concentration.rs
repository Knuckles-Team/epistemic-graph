//! EG-DECISION-ENGINE-R173.3/.5: the served `EditConcentration` arm.
//!
//! The question names the evidence graph (`graph`, text) and the candidate
//! edit (`candidate_edit`, iri). The arm reads the caller's RLS-filtered
//! snapshot of that graph, projects it to RDF and judges the edit with
//! `eg_shacl::concentration::judge_edit` -- the same function the native
//! `kg:editConcentration` SHACL constraint runs, so `Decide` and
//! `ShaclValidate` give the same verdict over the same graph. The policy node
//! the edit names (`kg:governedByConcentrationPolicy`) supplies the threshold.
//!
//! A verdict seals as an uncalibrated `Advisory` outcome with one option,
//! `keep` or `revert`, at full weight; an abstention seals as `Abstained`
//! with its typed reason. The premises list the policy (threshold, window,
//! revision), the window bounds and every counted edit id.

use eg_shacl::concentration::{judge_edit_iri, EditJudgement, JudgeError};
use eg_types::contract::BoundedVec;
use eg_types::decision::concentration::{ConcentrationAbstention, ConcentrationAction};
use eg_types::decision::digest::digest_text;
use eg_types::decision::statistical::{
    DecideRequest, QuestionKind, ScoredOption, StatisticalOutcome, TypedValue,
};
use eg_types::decision::{
    AbstainReason, PremiseClass, PremiseProvenance, PremiseRef, QuantScaleTag, QuantisedValue,
};

use super::stat_support::{filtered_view, invalid_parameter};

const PARAM_GRAPH: &str = "graph";
const PARAM_CANDIDATE: &str = "candidate_edit";
/// The Q32 fixed-point value for the chosen action.
const CHOSEN_SCORE: i64 = 1i64 << 32;

fn param<'a>(request: &'a DecideRequest, name: &str) -> Result<&'a str, String> {
    request
        .params
        .iter()
        .find(|param| param.name == name)
        .and_then(|param| match &param.value {
            TypedValue::Text(text) if !text.trim().is_empty() => Some(text.as_str()),
            TypedValue::Iri(iri) if !iri.trim().is_empty() => Some(iri.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            invalid_parameter(format!(
                "an edit_concentration question requires a {name} param"
            ))
        })
}

/// Open the evidence graph named by the `graph` param, RLS-filtered.
pub(super) async fn concentration_view(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::state::ServerState>>,
    agent_id: &str,
    request: &DecideRequest,
) -> Result<eg_core::graph::GraphView, String> {
    filtered_view(state, agent_id, param(request, PARAM_GRAPH)?).await
}

/// The judged answer: the outcome to seal and the premises behind it.
pub(super) struct Concentration {
    pub(super) outcome: StatisticalOutcome,
    pub(super) premises: Vec<PremiseRef>,
}

/// `Some` only for an `EditConcentration` question.
pub(super) fn concentration_outcome(
    request: &DecideRequest,
    view: Option<&eg_core::graph::GraphView>,
) -> Option<Result<Concentration, String>> {
    (request.question.kind == QuestionKind::EditConcentration).then(|| compute(request, view))
}

fn compute(
    request: &DecideRequest,
    view: Option<&eg_core::graph::GraphView>,
) -> Result<Concentration, String> {
    let candidate = param(request, PARAM_CANDIDATE)?;
    let view = view.ok_or_else(|| invalid_parameter("edit_concentration reads a graph"))?;
    let triples = eg_rdf::mapping::export_view_triples(view)?;
    let mut graph = eg_shacl::Graph::new();
    for triple in &triples {
        graph.insert(triple);
    }
    match judge_edit_iri(&graph, candidate) {
        Ok(judged) => Ok(Concentration {
            outcome: verdict_outcome(&judged)?,
            premises: premises(&judged),
        }),
        Err(JudgeError::Abstained(abstention)) => Ok(Concentration {
            outcome: StatisticalOutcome::Abstained {
                reasons: BoundedVec::new(vec![abstain_reason(abstention)?])
                    .map_err(invalid_parameter)?,
            },
            premises: vec![PremiseRef {
                subject: candidate.to_string(),
                fact: "candidate_edit".to_string(),
                class: PremiseClass::Claim,
                provenance: PremiseProvenance::Request {
                    field: format!("params.{PARAM_CANDIDATE}"),
                },
            }],
        }),
        Err(JudgeError::MalformedEdit(detail)) => Err(invalid_parameter(detail)),
    }
}

fn abstain_reason(abstention: ConcentrationAbstention) -> Result<AbstainReason, String> {
    Ok(match abstention {
        ConcentrationAbstention::NoConcentrationPolicy => AbstainReason::NoConcentrationPolicy,
        ConcentrationAbstention::MixedPolicyRevision { revisions } => {
            AbstainReason::MixedPolicyRevision {
                revisions: BoundedVec::new(revisions).map_err(invalid_parameter)?,
            }
        }
    })
}

fn verdict_outcome(judged: &EditJudgement) -> Result<StatisticalOutcome, String> {
    let action = match judged.verdict.action {
        ConcentrationAction::Keep => "keep",
        ConcentrationAction::Revert => "revert",
    };
    Ok(StatisticalOutcome::Advisory {
        scores: BoundedVec::new(vec![ScoredOption {
            option_id: action.to_string(),
            score: QuantisedValue {
                scale: QuantScaleTag::Q32,
                value: CHOSEN_SCORE,
            },
            probability: None,
        }])
        .map_err(invalid_parameter)?,
        calibrated: false,
    })
}

/// R173.5: the policy (threshold and revision), the window bounds and one
/// premise per counted edit id, the candidate last.
fn premises(judged: &EditJudgement) -> Vec<PremiseRef> {
    let evidence = &judged.evidence;
    let policy_digest = digest_text("eg/concentration-policy/v1", evidence);
    let mut out = vec![
        PremiseRef {
            subject: evidence.policy_iri.clone(),
            fact: format!(
                "concentration_policy min_shipped={} revision={}",
                evidence.min_shipped, evidence.policy_revision
            ),
            class: PremiseClass::Definition,
            provenance: PremiseProvenance::Policy {
                policy_digest: policy_digest.clone(),
            },
        },
        PremiseRef {
            subject: judged.verdict.dimension_iri.clone(),
            fact: format!(
                "concentration_window first_round={} last_round={} count={}",
                evidence.window_start, evidence.window_end, judged.verdict.count
            ),
            class: PremiseClass::Definition,
            provenance: PremiseProvenance::Policy { policy_digest },
        },
    ];
    out.extend(evidence.counted_edit_ids.iter().map(|edit_id| PremiseRef {
        subject: edit_id.clone(),
        fact: "counted_edit".to_string(),
        class: PremiseClass::Observation,
        provenance: PremiseProvenance::Observation {
            evaluation_id: edit_id.clone(),
        },
    }));
    out
}
