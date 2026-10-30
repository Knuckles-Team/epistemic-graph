//! EG-DECISION-ENGINE-R045.2: wires the typed tool-subset selection
//! (`tool_subset::smallest_covering_subset`, R045.1) into the `ToolSubset`
//! `Decide` question kind.
//!
//! The caller declares each tool candidate (`CandidateSource::Declared`,
//! classification = the capabilities it covers, a `schema_tokens` declared
//! number) and names the required capabilities and context budget as typed
//! request params. The selected minimal covering subset is sealed as an
//! `Advisory` outcome: one `ScoredOption` per selected tool at full (Q32
//! `1.0`) uncalibrated score. `resolution_kind`/`evidence_class` are still
//! computed generically from this outcome by `stat_decide`, exactly as for
//! every other question kind -- this module decides only which tools were
//! selected.
//!
//! EG-DECISION-ENGINE-R056.2.2: the selection is an exact statistical-path
//! solve. The covering problem is lowered to a bounded 0-1 programme (one
//! covering row per required capability, one budget row, a lexicographic
//! objective: fewest tools, then fewest tokens) and solved by `eg-compute`'s
//! deterministic branch-and-bound. The certificate is re-checked by the
//! independent verifier and reported through
//! `StatisticalSolverCertificate::solved` under the solver's own node budget;
//! a certificate that fails either check refuses the question.

use eg_compute::solve::{solve, verify, Model, SolveStatus, SolverConfig};
use eg_types::contract::BoundedVec;
use eg_types::decision::rapid_r056::StatisticalSolverCertificate;
use eg_types::decision::statistical::declared::DeclaredOption;
use eg_types::decision::statistical::tool_subset::{
    ToolCoverageCandidate, ToolSubsetRefusal, ToolSubsetRequestBody,
};
use eg_types::decision::statistical::{
    CandidateSource, DecideRequest, QuestionKind, ScoredOption, StatisticalErrorCode,
    StatisticalOutcome, TypedValue,
};
use eg_types::decision::{QuantScaleTag, QuantisedValue};
use eg_types::solve::{
    Coefficient, ConstraintBody, ConstraintSpec, ModelSpec, ObjectiveLevelSpec, ObjectiveTerm,
    Relation, Term, VarId,
};

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

/// The declared schema size, if the caller supplied one.
fn token_cost(option: &DeclaredOption) -> Option<u32> {
    option
        .numbers
        .iter()
        .find(|number| number.key == "schema_tokens")
        .map(|number| number.q32.max(0) as u32)
}

fn request_body(request: &DecideRequest) -> Result<ToolSubsetRequestBody, String> {
    let required = required_capabilities(request)?;
    let context_budget_tokens = context_budget_tokens(request)?;
    let options = declared_candidates(request)?;
    let mut candidates = Vec::with_capacity(options.len());
    for option in options {
        let token_cost = token_cost(option).ok_or_else(|| {
            refusal(
                StatisticalErrorCode::ParameterInvalid,
                "a tool_subset candidate must declare schema_tokens",
            )
        })?;
        candidates.push(ToolCoverageCandidate {
            tool_id: option.option_id.clone(),
            capabilities: BoundedVec::new(option.classification.iter().cloned().collect())
                .map_err(|detail| refusal(StatisticalErrorCode::CandidateSetTooLarge, detail))?,
            token_cost,
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

/// The exact covering subset and the statistical-path certificate it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SolvedSubset {
    pub(super) chosen: Vec<String>,
    pub(super) certificate: StatisticalSolverCertificate,
    pub(super) solver_certificate_digest: String,
}

fn var(index: usize) -> VarId {
    VarId(index as u32)
}

/// The bounded 0-1 programme of a covering request: `x_i` selects tool `i`.
pub(super) fn covering_model(body: &ToolSubsetRequestBody) -> Result<ModelSpec, ToolSubsetRefusal> {
    let mut constraints = Vec::new();
    for capability in body.required_capabilities.iter() {
        let covering: Vec<VarId> = body
            .candidates
            .iter()
            .enumerate()
            .filter(|(_, c)| c.capabilities.iter().any(|cap| cap == capability))
            .map(|(index, _)| var(index))
            .collect();
        if covering.is_empty() {
            return Err(ToolSubsetRefusal::CapabilityUncoverable);
        }
        constraints.push(ConstraintSpec {
            label: format!("cover:{capability}"),
            body: ConstraintBody::AtLeast {
                vars: covering,
                k: 1,
            },
        });
    }
    constraints.push(ConstraintSpec {
        label: "context_budget_tokens".to_string(),
        body: ConstraintBody::Linear {
            terms: body
                .candidates
                .iter()
                .enumerate()
                .map(|(index, c)| Term {
                    var: var(index),
                    coefficient: i64::from(c.token_cost),
                })
                .collect(),
            relation: Relation::LessEqual,
            rhs: i64::from(body.context_budget_tokens),
        },
    });
    let level = |label: &str, weight: &dyn Fn(&ToolCoverageCandidate) -> i64| ObjectiveLevelSpec {
        label: label.to_string(),
        terms: body
            .candidates
            .iter()
            .enumerate()
            .map(|(index, c)| ObjectiveTerm {
                var: var(index),
                coefficient: Coefficient::Known(weight(c)),
            })
            .collect(),
    };
    Ok(ModelSpec {
        variables: body.candidates.iter().map(|c| c.tool_id.clone()).collect(),
        constraints,
        objective: vec![
            level("tool_count", &|_| 1),
            level("schema_tokens", &|c| i64::from(c.token_cost)),
        ],
    })
}

fn solver_refusal(detail: impl std::fmt::Display) -> String {
    refusal(
        StatisticalErrorCode::ParameterInvalid,
        format!("statistical solve refused: {detail}"),
    )
}

/// Solve the covering request exactly and certify the statistical path.
pub(super) fn solved_covering_subset(body: &ToolSubsetRequestBody) -> Result<SolvedSubset, String> {
    if body.required_capabilities.is_empty() {
        return Err(solver_refusal("no required capability to cover"));
    }
    let spec = covering_model(body)
        .map_err(|kind| refusal(StatisticalErrorCode::ParameterInvalid, refusal_reason(kind)))?;
    let model = Model::try_from(spec).map_err(solver_refusal)?;
    let config = SolverConfig::default();
    let certificate = solve(&model, &config);
    verify(&model, &certificate).map_err(solver_refusal)?;
    let model_digest = String::from(model.digest());
    let statistical = StatisticalSolverCertificate::solved(
        model_digest.clone(),
        certificate.nodes_expanded,
        config.node_budget(),
    )
    .map_err(|refused| solver_refusal(format!("{refused:?}")))?;
    statistical
        .verify(&String::from(certificate.model_digest))
        .map_err(|refused| solver_refusal(format!("{refused:?}")))?;
    let incumbent = match (&certificate.status, &certificate.incumbent) {
        (SolveStatus::Optimal | SolveStatus::OptimalByDeterministicSearch { .. }, Some(i)) => i,
        (
            SolveStatus::Infeasible { .. } | SolveStatus::InfeasibleByDeterministicSearch { .. },
            _,
        ) => {
            return Err(refusal(
                StatisticalErrorCode::ParameterInvalid,
                refusal_reason(ToolSubsetRefusal::BudgetExceeded),
            ))
        }
        (status, _) => return Err(solver_refusal(format!("no proven optimum: {status:?}"))),
    };
    let chosen = body
        .candidates
        .iter()
        .zip(&incumbent.selected)
        .filter(|(_, selected)| **selected)
        .map(|(c, _)| c.tool_id.clone())
        .collect();
    Ok(SolvedSubset {
        chosen,
        certificate: statistical,
        solver_certificate_digest: String::from(certificate.digest()),
    })
}

fn compute(request: &DecideRequest) -> Result<StatisticalOutcome, String> {
    let body = request_body(request)?;
    let chosen = solved_covering_subset(&body)?.chosen;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, caps: &[&str], cost: u32) -> ToolCoverageCandidate {
        ToolCoverageCandidate {
            tool_id: id.to_string(),
            capabilities: BoundedVec::new(caps.iter().map(|s| s.to_string()).collect()).unwrap(),
            token_cost: cost,
        }
    }

    fn body(
        required: &[&str],
        budget: u32,
        candidates: Vec<ToolCoverageCandidate>,
    ) -> ToolSubsetRequestBody {
        ToolSubsetRequestBody {
            required_capabilities: BoundedVec::new(
                required.iter().map(|s| s.to_string()).collect(),
            )
            .unwrap(),
            context_budget_tokens: budget,
            candidates: BoundedVec::new(candidates).unwrap(),
        }
    }

    /// A set-cover instance the greedy density heuristic gets wrong: greedy
    /// takes the dense `wide` tool first and then needs two more, while the
    /// exact optimum is the pair `left` + `right`.
    fn greedy_trap() -> ToolSubsetRequestBody {
        body(
            &["a", "b", "c", "d", "e", "f"],
            1_000,
            vec![
                candidate("wide", &["b", "c", "d", "e"], 4),
                candidate("left", &["a", "b", "c"], 5),
                candidate("right", &["d", "e", "f"], 5),
            ],
        )
    }

    // spec: EG-DECISION-ENGINE-R056.2.2
    #[test]
    fn the_statistical_solve_calls_the_exact_solver_and_certifies_deterministically() {
        let request = greedy_trap();
        let first = solved_covering_subset(&request).expect("solved");
        assert_eq!(first.chosen, vec!["left".to_string(), "right".to_string()]);

        // Certificate verification: the statistical certificate names the
        // model the exact solver ran on and refuses any other model.
        let model = Model::try_from(covering_model(&request).unwrap()).unwrap();
        let digest = String::from(model.digest());
        assert!(first.certificate.verify(&digest).is_ok());
        assert!(first.certificate.verify("another-model").is_err());
        assert!(first.certificate.branch_count <= SolverConfig::default().node_budget());

        // Deterministic branching: an identical solve explores the same
        // branches and yields a byte-identical solver certificate.
        let second = solved_covering_subset(&request).expect("solved again");
        assert_eq!(first, second);

        // A changed programme changes the certified model.
        let mut tighter = greedy_trap();
        tighter.context_budget_tokens = 9;
        let refused = solved_covering_subset(&tighter).unwrap_err();
        assert!(refused.contains("context budget"), "{refused}");
    }
}
