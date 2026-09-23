//! The held-out routing fixture (QUALITY.md, LEDGER RF-019) with
//! acceptability sets. Deterministic items (a unique acceptable answer) must
//! select exactly; every other item must answer inside its acceptable set or
//! abstain with the typed reason it names.

use std::collections::BTreeSet;

use eg_types::decision::{
    AbstainReason, CostBudget, DecisionOutcome, EvidenceClass, ResolutionKind,
};

use super::super::{assemble, replay_check, Assembly};
use super::fixture::*;

/// What an item accepts.
enum Accept {
    /// Any one of these exact selections.
    OneOf(Vec<Vec<&'static str>>),
    /// An abstention whose reasons satisfy the predicate.
    Abstain(fn(&AbstainReason) -> bool),
}

struct Item {
    name: &'static str,
    request: eg_types::decision::AssemblyRequest,
    library: Vec<eg_types::decision::CandidateFacts>,
    accept: Accept,
}

fn selection(assembly: &Assembly) -> Option<Vec<String>> {
    let DecisionOutcome::Solved { slots, .. } = &assembly.record.outcome else {
        return None;
    };
    Some(
        slots
            .iter()
            .map(|slot| slot.component.component_id.clone())
            .collect(),
    )
}

fn items() -> Vec<Item> {
    let mut denied = request(&["eg:task/research"], &[]);
    denied.requirements.denies = bounded(vec!["tool-swiss".to_string()]);
    let mut budget = request(&["eg:task/research"], &[]);
    budget.requirements.constraints.cost_budget = Some(CostBudget {
        currency: "USD".to_string(),
        max_micros: 25,
        strict: true,
    });
    let mut unavailable = research_library();
    unavailable.retain(|c| !c.component_id.starts_with("tool-"));
    let mut small_window = request(&["eg:task/research"], &[]);
    small_window.requirements.constraints.context_budget_tokens = Some(150_000);
    vec![
        Item {
            name: "research: fewest components wins over cheapest tools",
            request: request(&["eg:task/research"], &[]),
            library: research_library(),
            accept: Accept::OneOf(vec![vec![
                "ontology-a",
                "skill-a",
                "model-cheap",
                "prompt-research",
                "tool-swiss",
            ]]),
        },
        Item {
            name: "research with the all-in-one tool denied: cheapest pair",
            request: denied,
            library: research_library(),
            accept: Accept::OneOf(vec![vec![
                "ontology-a",
                "skill-a",
                "model-cheap",
                "prompt-research",
                "tool-summarize",
                "tool-vector",
            ]]),
        },
        Item {
            name: "cost budget excludes the all-in-one tool",
            request: budget,
            library: research_library(),
            accept: Accept::OneOf(vec![vec![
                "ontology-a",
                "skill-a",
                "model-cheap",
                "prompt-research",
                "tool-summarize",
                "tool-vector",
            ]]),
        },
        Item {
            name: "context budget forces the large-window model",
            request: small_window,
            library: research_library(),
            accept: Accept::OneOf(vec![vec![
                "ontology-a",
                "skill-a",
                "model-dear",
                "prompt-research",
                "tool-swiss",
            ]]),
        },
        Item {
            name: "no tool available: retrieval is uncovered",
            request: request(&["eg:task/research"], &[]),
            library: unavailable,
            accept: Accept::Abstain(|reason| {
                matches!(reason, AbstainReason::UncoveredCapability { .. })
            }),
        },
        Item {
            name: "ambiguous general retrieval: either retrieval tool is acceptable",
            request: request(&[], &["eg:capability/retrieval"]),
            library: research_library(),
            accept: Accept::OneOf(vec![
                vec![
                    "ontology-a",
                    "skill-a",
                    "model-cheap",
                    "prompt-research",
                    "tool-vector",
                ],
                vec![
                    "ontology-a",
                    "skill-a",
                    "model-cheap",
                    "prompt-research",
                    "tool-web",
                ],
            ]),
        },
        Item {
            name: "no-match capability",
            request: request(&[], &["eg:capability/action/process-exec"]),
            library: research_library(),
            accept: Accept::Abstain(|reason| {
                matches!(reason, AbstainReason::UncoveredCapability { .. })
            }),
        },
    ]
}

#[test]
fn every_routing_item_answers_inside_its_acceptability_set() {
    for item in items() {
        let assembly = assemble(inputs(item.request, item.library), identity())
            .unwrap_or_else(|error| panic!("{}: {error}", item.name));
        replay_check(&assembly.record)
            .unwrap_or_else(|error| panic!("{}: replay {error}", item.name));
        match (&item.accept, selection(&assembly)) {
            (Accept::OneOf(sets), Some(chosen)) => {
                let chosen: BTreeSet<&str> = chosen.iter().map(String::as_str).collect();
                assert!(
                    sets.iter()
                        .any(|set| set.iter().copied().collect::<BTreeSet<_>>() == chosen),
                    "{}: chose {chosen:?}",
                    item.name
                );
                assert_eq!(
                    assembly.record.resolution_kind,
                    ResolutionKind::Optimization
                );
                assert!(
                    assembly.graph.is_some() && assembly.agents.len() == 1,
                    "{}",
                    item.name
                );
            }
            (Accept::Abstain(expected), None) => {
                let DecisionOutcome::Abstained { reasons } = &assembly.record.outcome else {
                    unreachable!("no selection means abstained");
                };
                assert!(reasons.iter().any(expected), "{}: {reasons:?}", item.name);
                assert_eq!(assembly.record.resolution_kind, ResolutionKind::Abstention);
            }
            (_, chosen) => panic!("{}: unexpected answer {chosen:?}", item.name),
        }
    }
}

#[test]
fn a_solved_record_carries_premises_derivations_and_a_claim_class() {
    let assembly = assemble(
        inputs(request(&["eg:task/research"], &[]), research_library()),
        identity(),
    )
    .expect("assembles");
    let record = &assembly.record;
    // Every coverage rests on a publisher's classification: a claim.
    assert_eq!(record.evidence_class, EvidenceClass::Claim);
    assert_eq!(
        record.derivations.len(),
        3,
        "one derivation per required capability"
    );
    eg_types::decision::derivation::verify_record(record).expect("derivations re-check");
    assert!(record.record_id.starts_with("decision:"));
    assert_eq!(
        record.record_digest,
        eg_types::decision::digest::record_digest(record)
    );
    let graph = assembly.graph.expect("solved answers a graph");
    graph
        .validate()
        .expect("the one-agent graph is structurally valid");
    let agent = assembly
        .agents
        .first()
        .cloned()
        .expect("solved answers an agent");
    assert_eq!(
        graph.shape.pinned_agents(),
        vec![(
            agent.agent_id.as_str(),
            eg_types::agent_library::draft_definition_digest(&agent).as_str()
        )]
    );
    assert!(!record.why_not.is_empty(), "excluded options are explained");
}
