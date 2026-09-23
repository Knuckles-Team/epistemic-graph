use super::*;
use crate::agent_component::AgentComponentKind;
use crate::decision::request::{ClaimProvenance, ClaimedTaskMapping};

const ONTOLOGY: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn bounded<T, const N: usize>(values: Vec<T>) -> BoundedVec<T, N> {
    BoundedVec::new(values).expect("test values fit their bound")
}

fn candidate(component_id: &str, classification: &[&str]) -> CandidateFacts {
    crate::test_support::decision::published_candidate(
        component_id,
        AgentComponentKind::Tool,
        ONTOLOGY,
        classification,
    )
}

fn requirements(tasks: &[&str], capabilities: &[&str]) -> AssemblyRequirements {
    AssemblyRequirements {
        tasks: bounded(tasks.iter().map(|t| t.to_string()).collect()),
        capabilities: bounded(capabilities.iter().map(|t| t.to_string()).collect()),
        ..AssemblyRequirements::default()
    }
}

fn mapping(task: &str) -> ClaimedTaskMapping {
    ClaimedTaskMapping {
        text_digest: ONTOLOGY.to_string(),
        task_iris: bounded(vec![task.to_string()]),
        provenance: ClaimProvenance {
            producer: "au-planner".to_string(),
            model_profile: None,
            prompt_digest: None,
        },
    }
}

#[test]
fn a_task_closes_over_its_native_requirements_as_definitions() {
    let required = required_capabilities(&requirements(&["eg:task/research"], &[]), ONTOLOGY)
        .expect("a native task resolves");
    let iris: Vec<&str> = required.iter().map(|r| r.iri.as_str()).collect();
    assert_eq!(
        iris,
        vec![
            "eg:capability/analysis/summarize",
            "eg:capability/reasoning/plan",
            "eg:capability/retrieval",
        ]
    );
    assert!(required
        .iter()
        .all(|r| r.because.class == PremiseClass::Definition
            && r.because.fact == "required_by:eg:task/research"));
}

#[test]
fn a_claimed_mapping_is_a_claim_unless_something_stronger_requires_the_same_iri() {
    let mut request = requirements(&[], &["eg:capability/retrieval"]);
    request.task_mappings = bounded(vec![mapping("eg:task/research")]);
    let required = required_capabilities(&request, ONTOLOGY).expect("mapping resolves");
    let class_of = |iri: &str| {
        required
            .iter()
            .find(|r| r.iri == iri)
            .map(|r| r.because.class)
            .expect("required")
    };
    // Named directly: the claim adds nothing to it.
    assert_eq!(
        class_of("eg:capability/retrieval"),
        PremiseClass::Definition
    );
    // Only the claim requires these.
    assert_eq!(
        class_of("eg:capability/reasoning/plan"),
        PremiseClass::Claim
    );
    assert_eq!(
        class_of("eg:capability/analysis/summarize"),
        PremiseClass::Claim
    );
}

#[test]
fn unknown_terms_and_unmapped_text_are_typed_abstentions() {
    let mut request = requirements(
        &["eg:capability/retrieval"],
        &["acme:thing", "eg:task/review"],
    );
    request.unmapped_task_digests = bounded(vec![ONTOLOGY.to_string()]);
    let reasons = required_capabilities(&request, ONTOLOGY).expect_err("nothing here resolves");
    assert_eq!(
        reasons,
        vec![
            AbstainReason::UnresolvedCapabilityIri {
                iri: "acme:thing".to_string()
            },
            AbstainReason::UnresolvedCapabilityIri {
                iri: "eg:task/review".to_string()
            },
            AbstainReason::UnresolvedCapabilityIri {
                iri: "eg:capability/retrieval".to_string()
            },
            AbstainReason::UnmappedTask {
                text_digest: ONTOLOGY.to_string()
            },
        ]
    );
}

#[test]
fn the_coverage_chain_is_the_shortest_and_re_checks() {
    let web = candidate(
        "tool-web",
        &[
            "eg:capability/retrieval/web-search",
            "eg:capability/retrieval",
        ],
    );
    let chain = coverage_chain("eg:capability/retrieval", &web).expect("covers");
    assert_eq!(chain.len(), 1, "the direct classification is the shortest");
    let derivation = coverage_derivation("eg:capability", Some(&web)).expect("covers the root");
    assert_eq!(derivation.chain.len(), 2);
    verify_coverage(&derivation, std::slice::from_ref(&web)).expect("re-checks");
    assert!(
        coverage_chain(
            "eg:capability/retrieval/web-search",
            &candidate("x", &["eg:capability/retrieval"])
        )
        .is_none(),
        "claiming the parent is not evidence of the child"
    );
}

fn tampered(edit: impl FnOnce(&mut Vec<DerivationEdge>)) -> CoverageDerivation {
    let web = candidate("tool-web", &["eg:capability/retrieval/web-search"]);
    let mut chain = coverage_chain("eg:capability", &web).expect("covers");
    edit(&mut chain);
    CoverageDerivation {
        required: "eg:capability".to_string(),
        covered_by: Some("tool-web".to_string()),
        chain: bounded(chain),
    }
}

#[test]
fn a_tampered_chain_is_refused_at_the_step_that_lies() {
    let web = candidate("tool-web", &["eg:capability/retrieval/web-search"]);
    let candidates = std::slice::from_ref(&web);
    let promoted = tampered(|chain| chain[0].class = PremiseClass::Definition);
    assert!(matches!(
        verify_coverage(&promoted, candidates),
        Err(DerivationDefect::NotRootedInComponent { .. })
    ));
    let invented = tampered(|chain| chain[1].broader = "eg:capability/action".to_string());
    assert!(matches!(
        verify_coverage(&invented, candidates),
        Err(DerivationDefect::EdgeNotInOntology { index: 1, .. })
    ));
    let short = tampered(|chain| {
        chain.pop();
    });
    assert!(matches!(
        verify_coverage(&short, candidates),
        Err(DerivationDefect::MissesRequirement { .. })
    ));
    assert!(matches!(
        verify_coverage(&promoted, &[]),
        Err(DerivationDefect::UnknownCoveringComponent { .. })
    ));
}

#[test]
fn the_weakest_premise_classifies_and_a_definition_never_weakens() {
    assert_eq!(weakest([]), EvidenceClass::Proof);
    assert_eq!(weakest([PremiseClass::Definition]), EvidenceClass::Proof);
    assert_eq!(
        weakest([PremiseClass::Proof, PremiseClass::Observation]),
        EvidenceClass::Observation
    );
    assert_eq!(
        weakest([
            PremiseClass::Definition,
            PremiseClass::Observation,
            PremiseClass::Claim
        ]),
        EvidenceClass::Claim
    );
}
