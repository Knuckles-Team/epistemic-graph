//! Reading the integer facts a candidate declares, with "unknown" kept apart
//! from zero.
//!
//! Only model profiles and tools carry an invocation price or a latency. Every
//! other kind an agent is assembled from (a prompt, a toolset grouping, a
//! skill, an ontology) is invoked by nothing and so contributes nothing by
//! definition -- that is [`Declared::None`], which is NOT the same as a
//! selectable kind that simply did not declare the fact ([`Declared::Unknown`]).

use eg_types::agent_component::{AgentComponentFacts, CostFacts, DeclaredLatency};
use eg_types::decision::CandidateFacts;

use crate::solve::model::MAX_ABS_COEFFICIENT;

/// One integer fact of one candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Declared {
    /// The kind carries no such fact: it is never invoked on its own.
    None,
    /// Declared, and representable as a model coefficient.
    Known(i64),
    /// Absent, in another currency, or too large to represent exactly.
    Unknown,
}

fn selection_facts(
    candidate: &CandidateFacts,
) -> Option<(Option<&CostFacts>, Option<&DeclaredLatency>)> {
    match &candidate.facts {
        AgentComponentFacts::ModelProfile {
            cost,
            latency_declared,
            ..
        }
        | AgentComponentFacts::Tool {
            cost,
            latency_declared,
            ..
        } => Some((cost.as_ref(), latency_declared.as_ref())),
        AgentComponentFacts::SystemPrompt { .. }
        | AgentComponentFacts::Toolset { .. }
        | AgentComponentFacts::Opaque => None,
    }
}

fn representable(value: u64) -> Declared {
    match i64::try_from(value) {
        Ok(value) if value <= MAX_ABS_COEFFICIENT => Declared::Known(value),
        _ => Declared::Unknown,
    }
}

/// The per-call price in `currency`, in micros. A price in another currency
/// is unknown: the engine never converts.
pub(super) fn per_call_cost(candidate: &CandidateFacts, currency: Option<&str>) -> Declared {
    let Some((cost, _)) = selection_facts(candidate) else {
        return Declared::None;
    };
    let known = cost
        .filter(|cost| Some(cost.declared.currency.as_str()) == currency)
        .and_then(|cost| cost.declared.per_call_micros);
    known.map_or(Declared::Unknown, representable)
}

/// The declared p95 latency in milliseconds.
pub(super) fn p95_latency(candidate: &CandidateFacts) -> Declared {
    let Some((_, latency)) = selection_facts(candidate) else {
        return Declared::None;
    };
    latency.map_or(Declared::Unknown, |latency| {
        representable(u64::from(latency.p95_ms))
    })
}

/// The one currency the cost level is measured in: the budget's when there is
/// one, otherwise the smallest currency any candidate declares, so the choice
/// is a function of the inputs alone. Costs in any other currency are unknown.
pub(super) fn objective_currency(
    candidates: &[CandidateFacts],
    budget_currency: Option<&str>,
) -> Option<String> {
    if let Some(currency) = budget_currency {
        return Some(currency.to_string());
    }
    candidates
        .iter()
        .filter_map(|candidate| selection_facts(candidate).and_then(|(cost, _)| cost))
        .map(|cost| cost.declared.currency.clone())
        .min()
}

/// A model profile's hard facts, when the candidate is one.
pub(super) struct ModelFacts<'a> {
    pub model_identity: &'a str,
    pub context_window_tokens: u32,
    pub supports_tools: bool,
    pub supports_structured_output: bool,
    pub input: &'a [String],
    pub output: &'a [String],
}

pub(super) fn model_facts(candidate: &CandidateFacts) -> Option<ModelFacts<'_>> {
    let AgentComponentFacts::ModelProfile {
        model_identity,
        context_window_tokens,
        supports_tools,
        supports_structured_output,
        modalities,
        ..
    } = &candidate.facts
    else {
        return None;
    };
    Some(ModelFacts {
        model_identity,
        context_window_tokens: *context_window_tokens,
        supports_tools: *supports_tools,
        supports_structured_output: *supports_structured_output,
        input: &modalities.input,
        output: &modalities.output,
    })
}

/// A system prompt's token estimate, when the candidate is one.
pub(super) fn prompt_tokens(candidate: &CandidateFacts) -> Option<u32> {
    let AgentComponentFacts::SystemPrompt { token_estimate, .. } = &candidate.facts else {
        return None;
    };
    Some(*token_estimate)
}
