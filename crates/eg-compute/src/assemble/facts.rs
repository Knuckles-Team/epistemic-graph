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

/// EG-DECISION-ENGINE-R061: the exact-solver derivation layer here
/// (`per_call_cost`/`p95_latency`, over `CandidateFacts`) and the statistical
/// candidate path (`eg-numeric`'s `CandidateView::from_component`, over the
/// same `AgentComponentEntry`) read the same declared cost and latency.
///
/// `decide-stats` links `eg-numeric` only for this cross-path proof; neither
/// `per_call_cost` nor `p95_latency` depends on it.
#[cfg(all(test, feature = "decide-stats"))]
mod cross_path_consistency_tests {
    use super::{p95_latency, per_call_cost, Declared};
    use eg_numeric::decision::candidate::CandidateView;
    use eg_types::decision::CandidateFacts;
    use eg_types::test_support::decision::tool_entry_with_cost_latency;

    // spec: EG-DECISION-ENGINE-R061.1
    #[test]
    fn the_exact_solver_path_and_the_statistical_path_read_the_same_declared_values() {
        let entry = tool_entry_with_cost_latency(
            "fixture-tool",
            &["eg:capability/retrieval"],
            "USD",
            7_500,
            320,
        );
        let solver_facts =
            CandidateFacts::from_entry(&entry).expect("fixture entry is well-formed");
        let statistical_view = CandidateView::from_component(&entry);

        let Declared::Known(solver_cost) = per_call_cost(&solver_facts, Some("USD")) else {
            panic!("the fixture declares a representable USD cost");
        };
        assert_eq!(
            solver_cost as u64,
            statistical_view
                .cost_micros
                .expect("statistical view reads the same cost"),
            "both paths must read the same declared per-call cost"
        );

        let Declared::Known(solver_p95) = p95_latency(&solver_facts) else {
            panic!("the fixture declares a representable p95 latency");
        };
        assert_eq!(
            solver_p95 as u32,
            statistical_view
                .p95_ms
                .expect("statistical view reads the same latency"),
            "both paths must read the same declared p95 latency"
        );
    }
}
