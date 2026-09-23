//! Planted bad inputs (DECIDE-LAYER-DESIGN §11.1). Each must be refused, or
//! answered with the typed abstention or violation it names -- never with an
//! assembly. The catalog-side plants (a stale catalog digest, a fabricated
//! record whose facts differ from the published revision, a component from
//! another tenant) need the store and live with `DecisionCommit`'s tests.

use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::decision::derivation::verify_record;
use eg_types::decision::{
    AbstainReason, CostBudget, DecisionErrorCode, DecisionOutcome, EvidenceClass, PremiseClass,
    SolverBudget, Violation,
};

use super::super::model::{build, Building};
use super::super::search::{search, SolveContext};
use super::super::{assemble, replay_check, Assembly};
use super::fixture::*;

fn decide(
    request: eg_types::decision::AssemblyRequest,
    library: Vec<eg_types::decision::CandidateFacts>,
) -> Assembly {
    assemble(inputs(request, library), identity()).expect("the inputs are consistent")
}

fn reasons(assembly: &Assembly) -> Vec<AbstainReason> {
    match &assembly.record.outcome {
        DecisionOutcome::Abstained { reasons } => reasons.as_slice().to_vec(),
        DecisionOutcome::Solved { .. } => panic!("a planted input must not be solved"),
    }
}

fn eliminated(assembly: &Assembly, component_id: &str) -> Option<Violation> {
    assembly
        .record
        .eliminated
        .iter()
        .find(|elimination| elimination.component_id == component_id)
        .map(|elimination| elimination.violation.clone())
}

#[test]
fn unknown_cost_under_a_strict_budget_is_excluded_never_zero() {
    let mut library = research_library();
    library.push(tool(
        "tool-free?",
        &[
            "eg:capability/retrieval/web-search",
            "eg:capability/analysis/summarize",
        ],
        None,
    ));
    let mut strict = request(&["eg:task/research"], &[]);
    strict.requirements.constraints.cost_budget = Some(CostBudget {
        currency: "USD".to_string(),
        max_micros: 1_000,
        strict: true,
    });
    let assembly = decide(strict, library.clone());
    assert_eq!(
        eliminated(&assembly, "tool-free?"),
        Some(Violation::UnknownCostUnderStrictBudget)
    );
    // Without the budget the unknown-cost tool is legal but ranks below every
    // known cost at the same component count: the priced all-in-one wins.
    let lenient = decide(request(&["eg:task/research"], &[]), library);
    let DecisionOutcome::Solved { slots, .. } = &lenient.record.outcome else {
        panic!("solves")
    };
    assert!(slots
        .iter()
        .any(|slot| slot.component.component_id == "tool-swiss"));
}

#[test]
fn infeasible_constraints_abstain_with_the_rows_that_conflict() {
    let mut tight = request(&["eg:task/research"], &[]);
    tight.requirements.constraints.max_components = Some(2);
    let found = reasons(&decide(tight, research_library()));
    assert!(
        found
            .iter()
            .any(|reason| matches!(reason, AbstainReason::Infeasible { .. })),
        "{found:?}"
    );
}

#[test]
fn an_unmapped_free_text_task_abstains_by_digest() {
    let mut unmapped = request(&[], &["eg:capability/retrieval"]);
    unmapped.requirements.unmapped_task_digests = bounded(vec![digest_of("do the thing")]);
    let found = reasons(&decide(unmapped, research_library()));
    assert_eq!(
        found,
        vec![AbstainReason::UnmappedTask {
            text_digest: digest_of("do the thing")
        }]
    );
}

#[test]
fn an_external_agent_card_without_observations_is_ineligible_and_cannot_be_pinned() {
    let mut library = research_library();
    let card = candidate(
        "a2a-agent",
        AgentComponentKind::A2aAgentCard,
        &["eg:capability/retrieval"],
    );
    library.push(card.clone());
    let assembly = decide(request(&[], &["eg:capability/retrieval"]), library.clone());
    assert_eq!(
        eliminated(&assembly, "a2a-agent"),
        Some(Violation::IneligibleExternalAgent)
    );
    let mut pinned = request(&[], &["eg:capability/retrieval"]);
    pinned.requirements.pins = bounded(vec![ComponentDependency {
        component_id: card.component_id.clone(),
        kind: card.kind,
        definition_digest: card.definition_digest.clone(),
    }]);
    let found = reasons(&decide(pinned, library));
    assert_eq!(
        found,
        vec![AbstainReason::IneligibleExternalAgent {
            component_id: "a2a-agent".to_string()
        }]
    );
}

#[test]
fn retired_and_withdrawn_and_denied_candidates_are_eliminated_with_their_rule() {
    let mut library = research_library();
    library
        .iter_mut()
        .for_each(|candidate| match candidate.component_id.as_str() {
            "tool-web" => candidate.lifecycle = AgentLibraryLifecycle::Retired,
            "tool-vector" => candidate.lifecycle = AgentLibraryLifecycle::Withdrawn,
            _ => {}
        });
    let mut denied = request(&["eg:task/research"], &[]);
    denied.requirements.denies = bounded(vec!["tool-swiss".to_string()]);
    let assembly = decide(denied, library);
    assert_eq!(eliminated(&assembly, "tool-web"), Some(Violation::Retired));
    assert_eq!(
        eliminated(&assembly, "tool-vector"),
        Some(Violation::Withdrawn)
    );
    assert_eq!(eliminated(&assembly, "tool-swiss"), Some(Violation::Denied));
    // Nothing legal covers retrieval any more: monotone safety means no later
    // step can bring an eliminated option back to cover it.
    assert!(reasons(&assembly).iter().any(|reason| *reason
        == AbstainReason::UncoveredCapability {
            iri: "eg:capability/retrieval".to_string()
        }));
}

#[test]
fn a_request_that_loosens_the_policy_is_refused() {
    let mut loose = request(&["eg:task/research"], &[]);
    loose.solver = Some(SolverBudget {
        node_budget: 10_000_000,
        max_why_not_per_slot: 1,
    });
    let error = assemble(inputs(loose, research_library()), identity()).expect_err("loosening");
    assert_eq!(error.code, DecisionErrorCode::PolicyLoosening);
}

#[test]
fn a_pack_annotated_capability_is_shown_as_a_claim() {
    let assembly = decide(request(&["eg:task/research"], &[]), research_library());
    assert_eq!(assembly.record.evidence_class, EvidenceClass::Claim);
    for derivation in &assembly.record.derivations {
        assert_eq!(derivation.chain.as_slice()[0].class, PremiseClass::Claim);
    }
    let mut promoted = assembly.record.clone();
    promoted.evidence_class = EvidenceClass::Proof;
    assert!(
        verify_record(&promoted).is_err(),
        "a claim displayed as a proof is refused"
    );
}

#[test]
fn an_option_outside_the_candidate_universe_or_a_tampered_certificate_cannot_commit() {
    let assembly = decide(request(&["eg:task/research"], &[]), research_library());
    let mut foreign = assembly.record.clone();
    let DecisionOutcome::Solved { slots, .. } = &mut foreign.outcome else {
        panic!("solves")
    };
    let mut forged = slots.as_slice().to_vec();
    forged[0].component.component_id = "tool-from-elsewhere".to_string();
    *slots = bounded(forged);
    assert_eq!(
        replay_check(&foreign).expect_err("forged").code,
        DecisionErrorCode::DecisionReplayMismatch
    );

    let mut tampered = assembly.record.clone();
    let DecisionOutcome::Solved { certificate, .. } = &mut tampered.outcome else {
        panic!("solves")
    };
    certificate.nodes_expanded += 1;
    assert!(
        replay_check(&tampered).is_err(),
        "a tampered certificate is refused"
    );
    // And the independent verifier refuses it on its own, without replay.
    let model =
        crate::solve::Model::try_from(assembly.model.clone().expect("model")).expect("valid");
    let DecisionOutcome::Solved { certificate, .. } = &tampered.outcome else {
        unreachable!()
    };
    let mut lying = (**certificate).clone();
    if let Some(incumbent) = lying.incumbent.as_mut() {
        incumbent.selected.iter_mut().for_each(|on| *on = !*on);
    }
    assert!(crate::solve::verify(&model, &lying).is_err());
}

#[test]
fn a_result_the_deterministic_search_would_not_reach_is_not_committable() {
    // A wall-clock abort is the only way a solver could reach an answer the
    // node budget does not reproduce. Whatever produced it, a record whose
    // outcome differs from the deterministic replay is refused.
    let assembly = decide(request(&["eg:task/research"], &[]), research_library());
    let mut aborted = assembly.record.clone();
    aborted.outcome = DecisionOutcome::Abstained {
        reasons: bounded(vec![AbstainReason::BudgetExhausted {
            incumbent: None,
            lower_bound: eg_types::solve::Scalar::new(0),
        }]),
    };
    aborted.resolution_kind = eg_types::decision::ResolutionKind::Abstention;
    aborted.why_not = Default::default();
    assert!(replay_check(&aborted).is_err());
}

#[test]
fn budget_exhaustion_is_a_typed_abstention_or_a_verified_answer() {
    let mut starved = request(&["eg:task/research"], &[]);
    starved.solver = Some(SolverBudget {
        node_budget: 1,
        max_why_not_per_slot: 0,
    });
    let assembly = decide(starved, research_library());
    replay_check(&assembly.record).expect("either way the record replays");
    if let DecisionOutcome::Abstained { reasons } = &assembly.record.outcome {
        assert!(reasons
            .iter()
            .all(|reason| matches!(reason, AbstainReason::BudgetExhausted { .. })));
    }
}

#[test]
fn the_nogood_loop_terminates_after_its_bounded_rounds() {
    let inputs = inputs(request(&["eg:task/research"], &[]), research_library());
    let legal: Vec<usize> = (0..inputs.candidates.len()).collect();
    let Building::Ready(built) =
        build(&inputs, &required(&inputs), (&legal, 1), Some("USD")).expect("builds")
    else {
        panic!("the research library is buildable")
    };
    let context = SolveContext::new(&inputs, built, None).expect("context");
    let refuse_all = |_: &[(usize, &eg_types::decision::CandidateFacts)]| {
        Err("TEMPLATE_REFUSED: every answer".to_string())
    };
    let Err(reasons) = search(&context, &refuse_all).expect("decides") else {
        panic!("every answer was refused, so the search must abstain")
    };
    let [AbstainReason::Infeasible { constraints }] = reasons.as_slice() else {
        panic!("{reasons:?}")
    };
    // Either the rounds run out, or the cuts exhaust every feasible
    // selection first; both end in a typed abstention, never a loop.
    let rounds = usize::from(inputs.policy.max_nogood_rounds) + 1;
    let cuts = constraints
        .iter()
        .filter(|label| label.starts_with("nogood:"))
        .count();
    assert!(cuts >= 1 && cuts <= rounds, "{constraints:?}");
}

fn required(
    inputs: &eg_types::decision::DecisionInputs,
) -> Vec<eg_types::decision::derivation::RequiredCapability> {
    eg_types::decision::derivation::required_capabilities(
        &inputs.request.requirements,
        &inputs.ontology_digest,
    )
    .expect("resolves")
}
