//! Step 1b: remove every candidate a hard rule excludes, and say which rule.
//!
//! The rules are a table, applied in order; the first that fires is the
//! recorded violation. A candidate no rule removes is LEGAL, and the legal set
//! is the only thing any later step reads -- which is what makes the ladder
//! monotone: nothing after this can reinstate an option removed here.

use eg_types::agent_component::AgentComponentKind;
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::agent_ontology::satisfies;
use eg_types::decision::{
    AssemblyConstraints, CandidateFacts, DecisionInputs, Elimination, ObjectiveLevelKind, Violation,
};

use super::facts::{self, Declared};

/// The legal remainder (indices into the inputs' candidates, ascending) and
/// every elimination, in candidate order.
pub(super) struct Screened {
    pub legal: Vec<usize>,
    pub eliminated: Vec<Elimination>,
}

/// What every rule may read besides the candidate.
struct Rules<'a> {
    inputs: &'a DecisionInputs,
    constraints: &'a AssemblyConstraints,
    currency: Option<&'a str>,
}

type Rule = fn(&CandidateFacts, &Rules<'_>) -> Option<Violation>;

/// Applied in this order; the first violation found is the one recorded.
const RULES: &[Rule] = &[
    denied,
    lifecycle,
    external_agent,
    context_window,
    tool_support,
    structured_output,
    modality,
    prompt_budget,
    cost_budget,
    latency_budget,
];

pub(super) fn screen(inputs: &DecisionInputs, currency: Option<&str>) -> Screened {
    let rules = Rules {
        inputs,
        constraints: &inputs.request.requirements.constraints,
        currency,
    };
    let mut screened = Screened {
        legal: Vec::new(),
        eliminated: Vec::new(),
    };
    for (index, candidate) in inputs.candidates.iter().enumerate() {
        match RULES.iter().find_map(|rule| rule(candidate, &rules)) {
            Some(violation) => screened.eliminated.push(Elimination {
                component_id: candidate.component_id.clone(),
                violation,
            }),
            None => screened.legal.push(index),
        }
    }
    screened
}

fn denied(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    rules
        .inputs
        .request
        .requirements
        .denies
        .iter()
        .any(|denied| *denied == candidate.component_id)
        .then_some(Violation::Denied)
}

fn lifecycle(candidate: &CandidateFacts, _: &Rules<'_>) -> Option<Violation> {
    match candidate.lifecycle {
        AgentLibraryLifecycle::Published => None,
        AgentLibraryLifecycle::Retired => Some(Violation::Retired),
        AgentLibraryLifecycle::Withdrawn => Some(Violation::Withdrawn),
    }
}

/// RF-ADR-010 §4: an external agent card is a claim about someone else's
/// agent. Option A reads no L5 observation that could qualify one, so under a
/// policy that requires an observation a card is never selectable.
fn external_agent(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let external = candidate.kind == AgentComponentKind::A2aAgentCard;
    (external && rules.inputs.policy.a2a_requires_observation)
        .then_some(Violation::IneligibleExternalAgent)
}

fn context_window(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let model = facts::model_facts(candidate)?;
    let required = rules.constraints.context_budget_tokens?;
    let available = u64::from(model.context_window_tokens);
    (available < required).then_some(Violation::ContextWindowTooSmall {
        required,
        available,
    })
}

fn tool_support(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let model = facts::model_facts(candidate)?;
    (rules.constraints.require_tools && !model.supports_tools)
        .then_some(Violation::MissingToolSupport)
}

fn structured_output(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let model = facts::model_facts(candidate)?;
    (rules.constraints.require_structured_output && !model.supports_structured_output)
        .then_some(Violation::MissingStructuredOutput)
}

/// Every required modality must be satisfied (by subsumption) by one the
/// model declares, in the same direction.
fn modality(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let model = facts::model_facts(candidate)?;
    let wanted = [
        (&rules.constraints.modalities_in, model.input),
        (&rules.constraints.modalities_out, model.output),
    ];
    wanted.into_iter().find_map(|(required, declared)| {
        required
            .iter()
            .find(|iri| !declared.iter().any(|term| satisfies(term, iri)))
            .map(|iri| Violation::MissingModality { iri: iri.clone() })
    })
}

fn prompt_budget(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let tokens = u64::from(facts::prompt_tokens(candidate)?);
    let available = rules.constraints.context_budget_tokens?;
    (tokens > available).then_some(Violation::ContextWindowTooSmall {
        required: tokens,
        available,
    })
}

fn cost_budget(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let budget = rules.constraints.cost_budget.as_ref()?;
    match facts::per_call_cost(candidate, rules.currency) {
        Declared::None => None,
        Declared::Known(micros) => {
            (micros.unsigned_abs() > budget.max_micros).then_some(Violation::OverBudget {
                level: ObjectiveLevelKind::DeclaredCost,
            })
        }
        Declared::Unknown => budget
            .strict
            .then_some(Violation::UnknownCostUnderStrictBudget),
    }
}

fn latency_budget(candidate: &CandidateFacts, rules: &Rules<'_>) -> Option<Violation> {
    let ceiling = rules.constraints.max_p95_latency_ms?;
    match facts::p95_latency(candidate) {
        Declared::None => None,
        Declared::Known(p95) => (p95 > i64::from(ceiling)).then_some(Violation::OverBudget {
            level: ObjectiveLevelKind::DeclaredP95Latency,
        }),
        Declared::Unknown => Some(Violation::UnknownLatencyUnderBudget),
    }
}
