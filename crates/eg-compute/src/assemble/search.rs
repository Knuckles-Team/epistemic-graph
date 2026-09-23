//! The solve with its no-good loop (DECIDE-LAYER-DESIGN §7.3, EH-072).
//!
//! Validators the solver does not encode -- the agent-library entry and
//! agent-graph checks every answer must pass -- run after each solve. A
//! refused answer is cut off with a no-good row (`Σ_{selected} x ≤ |S| − 1`)
//! and the model is solved again, at most `max_nogood_rounds` times; then the
//! decision abstains, naming each refusal. The final model, cuts included, is
//! the one the record's certificate covers, so replay reproduces it exactly.

use eg_types::agent_component::AgentComponentKind;
use eg_types::agent_graph::AgentGraphDraft;
use eg_types::agent_library::AgentLibraryEntryDraft;
use eg_types::contract::BoundedVec;
use eg_types::decision::request::effective_solver_budget;
use eg_types::decision::{
    digest, AbstainReason, CandidateFacts, DecisionErrorCode, DecisionInputs, TemplateFacts,
};
use eg_types::solve::{
    Certificate, ConstraintBody, ConstraintSpec, ModelSpec, Relation, Scalar, SolveStatus,
    SolverConfig, SolverConfigSpec, Term, VarId, DEFAULT_CERTIFICATE_LEAVES,
};

use super::model::Built;
use super::AssembleError;
use crate::solve::{solve, Model};

/// Every agent an answer assembles, and the graph that runs them.
pub(super) type Drafts = (Vec<AgentLibraryEntryDraft>, AgentGraphDraft);

/// The validators the solver does not encode, run on a candidate answer:
/// each chosen candidate with the slot it was placed in.
pub(super) type Validator<'v> = dyn Fn(&[(usize, &CandidateFacts)]) -> Result<Drafts, String> + 'v;

/// What every search step shares.
pub(super) struct SolveContext<'a> {
    pub inputs: &'a DecisionInputs,
    pub built: Built,
    pub config: SolverConfig,
    pub inputs_digest: String,
    pub template: Option<&'a TemplateFacts>,
}

impl<'a> SolveContext<'a> {
    pub(super) fn new(
        inputs: &'a DecisionInputs,
        built: Built,
        template: Option<&'a TemplateFacts>,
    ) -> Result<Self, AssembleError> {
        let budget = effective_solver_budget(&inputs.policy, &inputs.request);
        let config = SolverConfig::try_from(SolverConfigSpec {
            node_budget: budget.node_budget,
            max_certificate_leaves: DEFAULT_CERTIFICATE_LEAVES,
            bound_denominator: 1,
            accepted_gap: inputs.policy.accepted_gap,
        })
        .map_err(|error| {
            AssembleError::new(DecisionErrorCode::AssemblyInputsInvalid, error.to_string())
        })?;
        Ok(Self {
            inputs,
            built,
            config,
            inputs_digest: digest::inputs_digest(inputs),
            template,
        })
    }

    pub(super) fn candidate(&self, var: VarId) -> &'a CandidateFacts {
        &self.inputs.candidates.as_slice()[self.built.vars[var.index()].candidate]
    }

    pub(super) fn kind_of(&self, var: VarId) -> AgentComponentKind {
        self.candidate(var).kind
    }

    /// The chosen candidates with their slots, in variable order.
    pub(super) fn chosen(&self, selected: &[bool]) -> Vec<(usize, &'a CandidateFacts)> {
        (0..selected.len())
            .filter(|&position| selected[position])
            .map(|position| {
                let var = VarId(position as u32);
                (self.built.vars[position].slot, self.candidate(var))
            })
            .collect()
    }

    fn labels(&self, spec: &ModelSpec, rows: impl Iterator<Item = usize>) -> Vec<String> {
        rows.filter_map(|row| spec.constraints.get(row).map(|c| c.label.clone()))
            .take(64)
            .collect()
    }
}

/// A validated answer: the final model, its certificate and the drafts.
pub(super) struct Answer {
    pub spec: ModelSpec,
    pub model: Model,
    pub certificate: Certificate,
    pub selected: Vec<bool>,
    pub drafts: Drafts,
}

pub(super) fn model_invalid(error: crate::solve::ModelError) -> AssembleError {
    AssembleError::new(DecisionErrorCode::AssemblyModelInvalid, error.to_string())
}

/// Solve, validate the answer, and cut it off and re-solve when a validator
/// refuses it -- at most `max_nogood_rounds` times.
pub(super) fn search(
    context: &SolveContext<'_>,
    validate: &Validator<'_>,
) -> Result<Result<Answer, Vec<AbstainReason>>, AssembleError> {
    let mut spec = context.built.spec.clone();
    let mut refused = Vec::new();
    for round in 0..=context.inputs.policy.max_nogood_rounds {
        let model = Model::try_from(spec.clone()).map_err(model_invalid)?;
        let certificate = solve(&model, &context.config);
        let selected = match classify(context, &spec, &certificate) {
            Ok(selected) => selected,
            Err(reasons) => return Ok(Err(reasons)),
        };
        match validate(&context.chosen(&selected)) {
            Ok(drafts) => {
                return Ok(Ok(Answer {
                    spec,
                    model,
                    certificate,
                    selected,
                    drafts,
                }))
            }
            Err(refusal) => {
                let code = refusal
                    .split(':')
                    .next()
                    .unwrap_or("VALIDATION")
                    .to_string();
                refused.push(format!("nogood:{round}:{code}"));
                spec.constraints.push(nogood(round, &selected));
            }
        }
    }
    let constraints =
        BoundedVec::new(refused.into_iter().take(64).collect()).expect("taken to the bound");
    Ok(Err(vec![AbstainReason::Infeasible { constraints }]))
}

/// `Σ_{selected} x ≤ |selected| − 1`: this exact selection is refused.
fn nogood(round: u8, selected: &[bool]) -> ConstraintSpec {
    let terms: Vec<Term> = (0..selected.len())
        .filter(|&position| selected[position])
        .map(|position| Term {
            var: VarId(position as u32),
            coefficient: 1,
        })
        .collect();
    let rhs = terms.len() as i64 - 1;
    ConstraintSpec {
        label: format!("nogood:{round}"),
        body: ConstraintBody::Linear {
            terms,
            relation: Relation::LessEqual,
            rhs,
        },
    }
}

/// The incumbent selection of a solved certificate, or the typed reasons an
/// unsolved one abstains with.
fn classify(
    context: &SolveContext<'_>,
    spec: &ModelSpec,
    certificate: &Certificate,
) -> Result<Vec<bool>, Vec<AbstainReason>> {
    let reason = match &certificate.status {
        SolveStatus::Optimal
        | SolveStatus::OptimalByDeterministicSearch { .. }
        | SolveStatus::FeasibleWithGap { .. } => {
            return certificate
                .incumbent
                .as_ref()
                .map(|incumbent| incumbent.selected.clone())
                .ok_or_else(Vec::new)
        }
        SolveStatus::Infeasible { core } => {
            infeasible(context.labels(spec, core.iter().map(|row| row.index())))
        }
        SolveStatus::InfeasibleByDeterministicSearch { .. } => {
            infeasible(context.labels(spec, 0..spec.constraints.len()))
        }
        SolveStatus::BudgetExhausted => budget_exhausted(context, certificate),
    };
    Err(vec![reason])
}

fn infeasible(labels: Vec<String>) -> AbstainReason {
    AbstainReason::Infeasible {
        constraints: BoundedVec::new(labels).expect("labels are taken to the bound"),
    }
}

/// Every objective coefficient is non-negative, so zero is a valid lower
/// bound when the search closed no bound leaf.
fn budget_exhausted(context: &SolveContext<'_>, certificate: &Certificate) -> AbstainReason {
    let incumbent = certificate.incumbent.as_ref().map(|incumbent| {
        let ids: Vec<&str> = context
            .chosen(&incumbent.selected)
            .iter()
            .map(|(_, candidate)| candidate.component_id.as_str())
            .collect();
        digest::digest_text("eg/decision-incumbent/v1", &ids)
    });
    AbstainReason::BudgetExhausted {
        incumbent,
        lower_bound: certificate.lower_bound.unwrap_or(Scalar::new(0)),
    }
}
