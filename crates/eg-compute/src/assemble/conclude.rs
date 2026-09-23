//! Running the ladder and turning its result into a record's conclusion.

use eg_types::agent_component::ComponentDependency;
use eg_types::agent_graph::AgentGraphDraft;
use eg_types::agent_library::AgentLibraryEntryDraft;
use eg_types::contract::BoundedVec;
use eg_types::decision::derivation::{
    coverage_derivation, required_capabilities, weakest, RequiredCapability,
};
use eg_types::decision::request::effective_solver_budget;
use eg_types::decision::{
    AbstainReason, CandidateFacts, CoverageDerivation, DecisionErrorCode, DecisionInputs,
    DecisionOutcome, Elimination, EvidenceClass, PremiseRef, ResolutionKind, SlotAssignment,
    TemplateFacts, Violation, WhyNot, MAX_ASSEMBLY_REQUIRED_CAPABILITIES,
};
use eg_types::solve::ModelSpec;

use super::eliminate::Screened;
use super::model::Building;
use super::search::{search, Answer, SolveContext};
use super::{agent, eliminate, facts, model, template, why_not, AssembleError};
use crate::solve::Verdict;

/// Every record field the decision function computes.
pub(super) struct Decided {
    pub resolution_kind: ResolutionKind,
    pub evidence_class: EvidenceClass,
    pub premises: Vec<PremiseRef>,
    pub eliminated: Vec<Elimination>,
    pub derivations: Vec<CoverageDerivation>,
    pub outcome: DecisionOutcome,
    pub why_not: Vec<WhyNot>,
    pub model: Option<ModelSpec>,
    pub agents: Vec<AgentLibraryEntryDraft>,
    pub graph: Option<AgentGraphDraft>,
}

/// Whether a verified certificate supports a `Solved` outcome.
pub(super) fn verdict_supports_solved(verdict: &Verdict) -> bool {
    match verdict {
        Verdict::ProvenOptimal { .. }
        | Verdict::ProvenGap { .. }
        | Verdict::OptimalityRequiresReplay { .. } => true,
        Verdict::ProvenInfeasible { .. }
        | Verdict::InfeasibilityRequiresReplay
        | Verdict::Unresolved { .. } => false,
    }
}

/// What every assembly over one input set shares, template or not.
pub(super) struct Ladder<'a> {
    pub inputs: &'a DecisionInputs,
    pub required: Vec<RequiredCapability>,
    pub screened: Screened,
    pub currency: Option<String>,
}

pub(super) fn decide(inputs: &DecisionInputs) -> Result<Decided, AssembleError> {
    let budget = inputs.request.requirements.constraints.cost_budget.as_ref();
    let currency = facts::objective_currency(
        inputs.candidates.as_slice(),
        budget.map(|budget| budget.currency.as_str()),
    );
    let screened = eliminate::screen(inputs, currency.as_deref());
    let required =
        match required_capabilities(&inputs.request.requirements, &inputs.ontology_digest) {
            Ok(required) => required,
            Err(reasons) => return Ok(abstained(&[], screened.eliminated, reasons)),
        };
    if required.len() > MAX_ASSEMBLY_REQUIRED_CAPABILITIES {
        return Err(AssembleError::new(
            DecisionErrorCode::RecordTooLarge,
            format!("more than {MAX_ASSEMBLY_REQUIRED_CAPABILITIES} required capabilities"),
        ));
    }
    if let Some(reasons) = pinned_ineligible(inputs, &screened.eliminated) {
        return Ok(abstained(&required, screened.eliminated, reasons));
    }
    let ladder = Ladder {
        inputs,
        required,
        screened,
        currency,
    };
    match inputs.templates.is_empty() {
        true => decide_one(&ladder, None),
        false => template::enumerate(&ladder),
    }
}

/// Decide over one topology: the one-agent graph, or one template.
pub(super) fn decide_one(
    ladder: &Ladder<'_>,
    template: Option<&TemplateFacts>,
) -> Result<Decided, AssembleError> {
    let inputs = ladder.inputs;
    let eliminated = ladder.screened.eliminated.clone();
    let slots = template.map_or(1, |template| template.slot_nodes().len());
    let legal = (ladder.screened.legal.as_slice(), slots);
    let built = match model::build(inputs, &ladder.required, legal, ladder.currency.as_deref())? {
        Building::Ready(built) => built,
        Building::Abstain(reasons) => return Ok(abstained(&ladder.required, eliminated, reasons)),
    };
    let context = SolveContext::new(inputs, built, template)?;
    let validator = |chosen: &[(usize, &CandidateFacts)]| {
        agent::drafts(inputs, &context.inputs_digest, template, chosen)
    };
    match search(&context, &validator)? {
        Ok(answer) => Ok(solved(&context, &ladder.required, eliminated, answer)),
        Err(reasons) => Ok(abstained(&ladder.required, eliminated, reasons)),
    }
}

/// An external agent the request pinned but step 1b had to refuse is named as
/// such, rather than as an anonymous infeasible pin.
fn pinned_ineligible(
    inputs: &DecisionInputs,
    eliminated: &[Elimination],
) -> Option<Vec<AbstainReason>> {
    let reasons: Vec<AbstainReason> = inputs
        .request
        .requirements
        .pins
        .iter()
        .filter(|pin| {
            eliminated.iter().any(|elimination| {
                elimination.component_id == pin.component_id
                    && elimination.violation == Violation::IneligibleExternalAgent
            })
        })
        .map(|pin| AbstainReason::IneligibleExternalAgent {
            component_id: pin.component_id.clone(),
        })
        .collect();
    (!reasons.is_empty()).then_some(reasons)
}

/// A record conclusion that is an abstention.
pub(super) fn abstained(
    required: &[RequiredCapability],
    eliminated: Vec<Elimination>,
    mut reasons: Vec<AbstainReason>,
) -> Decided {
    reasons.truncate(64);
    let premises: Vec<PremiseRef> = required.iter().map(|r| r.because.clone()).collect();
    let derivations: Vec<CoverageDerivation> = reasons
        .iter()
        .filter_map(|reason| {
            let AbstainReason::UncoveredCapability { iri } = reason else {
                return None;
            };
            coverage_derivation(iri, None)
        })
        .collect();
    Decided {
        resolution_kind: ResolutionKind::Abstention,
        evidence_class: weakest(premises.iter().map(|premise| premise.class)),
        premises,
        eliminated,
        derivations,
        outcome: DecisionOutcome::Abstained {
            reasons: BoundedVec::new(reasons).expect("truncated to the bound"),
        },
        why_not: Vec::new(),
        model: None,
        agents: Vec::new(),
        graph: None,
    }
}

/// Each chosen candidate once, in candidate order.
fn distinct<'a>(chosen: &[(usize, &'a CandidateFacts)]) -> Vec<&'a CandidateFacts> {
    let mut out: Vec<&CandidateFacts> = chosen.iter().map(|(_, candidate)| *candidate).collect();
    out.sort_by(|a, b| a.component_id.cmp(&b.component_id));
    out.dedup_by(|a, b| a.component_id == b.component_id);
    out
}

fn solved(
    context: &SolveContext<'_>,
    required: &[RequiredCapability],
    eliminated: Vec<Elimination>,
    answer: Answer,
) -> Decided {
    let chosen = context.chosen(&answer.selected);
    let components = distinct(&chosen);
    let derivations: Vec<CoverageDerivation> = required
        .iter()
        .filter_map(|requirement| {
            let covering = components
                .iter()
                .find(|candidate| candidate.classified_under(&requirement.iri))?;
            coverage_derivation(&requirement.iri, Some(covering))
        })
        .collect();
    let mut premises: Vec<PremiseRef> = required.iter().map(|r| r.because.clone()).collect();
    premises.extend(
        components
            .iter()
            .flat_map(|candidate| candidate.fact_premises.iter().cloned()),
    );
    let edge_classes = derivations
        .iter()
        .flat_map(|d| d.chain.iter().map(|edge| edge.class));
    let evidence_class = weakest(
        premises
            .iter()
            .map(|premise| premise.class)
            .chain(edge_classes),
    );
    let limit = effective_solver_budget(&context.inputs.policy, &context.inputs.request)
        .max_why_not_per_slot;
    let why_not = why_not::explain(context, &answer.model, &answer.selected, limit);
    let (agents, graph) = answer.drafts;
    let slots = chosen
        .iter()
        .map(|(slot, candidate)| slot_assignment(context.template, *slot, candidate))
        .collect();
    Decided {
        resolution_kind: ResolutionKind::Optimization,
        evidence_class,
        premises,
        eliminated,
        derivations,
        outcome: DecisionOutcome::Solved {
            graph_digest: eg_types::agent_graph::draft_definition_digest(&graph),
            slots: BoundedVec::new(slots).expect("a selection fits the slot bound"),
            certificate: Box::new(answer.certificate),
        },
        why_not,
        model: Some(answer.spec),
        agents,
        graph: Some(graph),
    }
}

/// The slot a chosen candidate fills: its kind, qualified by the template
/// node it was placed in when the answer is a template graph.
pub(super) fn slot_label(template: Option<&TemplateFacts>, slot: usize, kind: &str) -> String {
    match template.and_then(|template| template.slot_nodes().get(slot).copied()) {
        Some(node) => format!("{}/{kind}", node.node_id),
        None => kind.to_string(),
    }
}

fn slot_assignment(
    template: Option<&TemplateFacts>,
    slot: usize,
    candidate: &CandidateFacts,
) -> SlotAssignment {
    SlotAssignment {
        slot: slot_label(template, slot, agent::slot_name(candidate.kind)),
        component: ComponentDependency {
            component_id: candidate.component_id.clone(),
            kind: candidate.kind,
            definition_digest: candidate.definition_digest.clone(),
        },
    }
}
