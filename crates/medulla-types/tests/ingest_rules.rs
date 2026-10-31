//! Refusal and behavior rules of the ingest worker's typed records.

use std::collections::{BTreeMap, BTreeSet};

use medulla_types::extracted_fact::{EvidenceSpan, ExtractedFact, GroundingResult};
use medulla_types::ingest_rules::{
    check_access_target, check_atom_provenance, check_container_tree, check_group_size,
    check_row_version, ground_fact, judge_proposal, plan_reconcile, select_atoms, FactDeduper,
    GroundingCache, IngestRefusal, RowVersionDecision, MAX_CONTAINER_DEPTH, MAX_GROUP_MEMBERS,
};
use medulla_types::knowledge_atom::{AtomKind, KnowledgeAtom};
use medulla_types::ontology_proposal::{
    OntologyProposal, ProposalCheck, ProposalProvenance, ProposalState, ProposalThresholds,
    ProposedClass,
};
use medulla_types::source_governance::{RecordGovernance, SourceContainer, SourceGroup};
use serde_json::json;

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| item.to_string()).collect()
}

fn governance(classification: &str, public: bool, retention: Option<u64>) -> RecordGovernance {
    serde_json::from_value(json!({
        "classification": classification,
        "retention": retention,
        "access_public": public,
    }))
    .unwrap()
}

// spec: MEDULLA-INGEST-WORKER-R900.4
#[test]
fn conflicting_or_retention_less_governance_is_refused() {
    let conflict = governance("restricted", true, Some(1))
        .admit(false)
        .unwrap_err();
    assert_eq!(conflict.code(), "SOURCE_GOVERNANCE_CONFLICT");
    let also_conflict = governance("public", false, Some(1))
        .admit(false)
        .unwrap_err();
    assert_eq!(also_conflict.code(), "SOURCE_GOVERNANCE_CONFLICT");
    let missing = governance("internal", false, None).admit(true).unwrap_err();
    assert_eq!(missing.code(), "SOURCE_RETENTION_MISSING");
    assert_eq!(governance("public", true, None).admit(false), Ok(()));
    assert_eq!(governance("internal", false, None).admit(false), Ok(()));
    assert_eq!(governance("internal", false, Some(5)).admit(true), Ok(()));
}

// spec: MEDULLA-INGEST-WORKER-R901.4
#[test]
fn non_integer_or_decreasing_row_versions_are_refused() {
    assert_eq!(
        check_row_version(None, &json!(3)),
        Ok(RowVersionDecision::First)
    );
    assert_eq!(
        check_row_version(Some(3), &json!(4)),
        Ok(RowVersionDecision::Supersedes)
    );
    assert_eq!(
        check_row_version(Some(3), &json!(3)),
        Ok(RowVersionDecision::Replay)
    );
    for bad in [json!("4"), json!(1.5), json!(-1), json!(null)] {
        let error = check_row_version(Some(3), &bad).unwrap_err();
        assert_eq!(error.code(), "ROW_VERSION_INVALID", "{bad}");
    }
    let decreasing = check_row_version(Some(3), &json!(2)).unwrap_err();
    assert_eq!(decreasing.code(), "ROW_VERSION_INVALID");
}

// spec: MEDULLA-INGEST-WORKER-R902.4
#[test]
fn unknown_records_and_oversized_groups_are_refused() {
    let held = set(&["page-1"]);
    assert_eq!(check_access_target(&held, "page-1"), Ok(()));
    assert_eq!(
        check_access_target(&held, "page-2").unwrap_err().code(),
        "SOURCE_RECORD_UNKNOWN"
    );
    let group = |count: usize| SourceGroup {
        source: "s".into(),
        group_id: "g".into(),
        members: (0..count).map(|i| format!("u{i}")).collect(),
        synced_at: 0,
    };
    assert_eq!(check_group_size(&group(MAX_GROUP_MEMBERS)), Ok(()));
    assert_eq!(
        check_group_size(&group(MAX_GROUP_MEMBERS + 1))
            .unwrap_err()
            .code(),
        "SOURCE_GROUP_TOO_LARGE"
    );
}

// spec: MEDULLA-INGEST-WORKER-R903.4
#[test]
fn incomplete_enumerations_never_delete_and_mass_deletes_are_guarded() {
    let held: BTreeSet<String> = (0..10).map(|i| format!("r{i}")).collect();
    // Incomplete: an empty enumeration deletes nothing.
    assert_eq!(
        plan_reconcile(&held, &BTreeSet::new(), false, false),
        Ok(set(&[]))
    );
    // Complete but empty: 100 percent of the source, refused.
    let guarded = plan_reconcile(&held, &BTreeSet::new(), true, false).unwrap_err();
    assert_eq!(guarded.code(), "SOURCE_MASS_DELETE_GUARD");
    // An admin override lifts the guard.
    assert_eq!(
        plan_reconcile(&held, &BTreeSet::new(), true, true),
        Ok(held.clone())
    );
    // Exactly 20 percent (2 of 10) is allowed; 3 of 10 is not.
    let mut seen = held.clone();
    seen.remove("r0");
    seen.remove("r1");
    assert_eq!(
        plan_reconcile(&held, &seen, true, false),
        Ok(set(&["r0", "r1"]))
    );
    seen.remove("r2");
    assert!(matches!(
        plan_reconcile(&held, &seen, true, false),
        Err(IngestRefusal::MassDeleteGuard {
            missing: 3,
            held: 10
        })
    ));
}

fn container(id: &str, parent: Option<&str>) -> SourceContainer {
    SourceContainer {
        source: "s".into(),
        container_id: id.into(),
        parent_id: parent.map(str::to_string),
        title: id.into(),
    }
}

// spec: MEDULLA-INGEST-WORKER-R904.4
#[test]
fn container_cycles_and_excess_depth_are_refused() {
    let tree = [
        container("a", None),
        container("b", Some("a")),
        container("c", Some("b")),
    ];
    assert_eq!(check_container_tree(&tree), Ok(()));
    let cycle = [
        container("a", Some("c")),
        container("b", Some("a")),
        container("c", Some("b")),
    ];
    assert_eq!(
        check_container_tree(&cycle).unwrap_err().code(),
        "SOURCE_CONTAINER_CYCLE"
    );

    let chain = |depth: usize| -> Vec<SourceContainer> {
        (0..depth)
            .map(|i| {
                let parent = (i > 0).then(|| format!("n{}", i - 1));
                container(&format!("n{i}"), parent.as_deref())
            })
            .collect()
    };
    assert_eq!(check_container_tree(&chain(MAX_CONTAINER_DEPTH)), Ok(()));
    assert_eq!(
        check_container_tree(&chain(MAX_CONTAINER_DEPTH + 1))
            .unwrap_err()
            .code(),
        "SOURCE_CONTAINER_TOO_DEEP"
    );
}

fn fact(subject: &str, predicate: &str, object: &str) -> ExtractedFact {
    ExtractedFact {
        subject: subject.into(),
        predicate: predicate.into(),
        object: object.into(),
        evidence: EvidenceSpan::new("doc:1", 0, 10).unwrap(),
        confidence: 0.9,
    }
}

fn cache() -> GroundingCache {
    GroundingCache {
        predicates: BTreeMap::from([(
            "authorOf".to_string(),
            ("Person".to_string(), "Work".to_string()),
        )]),
        classes: BTreeMap::from([
            ("Ada".to_string(), "Person".to_string()),
            ("Note G".to_string(), "Work".to_string()),
        ]),
    }
}

// spec: MEDULLA-INGEST-WORKER-R701.2
#[test]
fn grounding_repairs_direction_and_deduper_scales_linearly() {
    let cache = cache();
    let straight = ground_fact(fact("Ada", "authorOf", "Note G"), &cache);
    assert!(matches!(
        straight.grounding,
        GroundingResult::Grounded {
            direction_repaired: false,
            ..
        }
    ));
    let swapped = ground_fact(fact("Note G", "authorOf", "Ada"), &cache);
    match swapped.grounding {
        GroundingResult::Grounded {
            subject_class,
            object_class,
            direction_repaired,
            ..
        } => {
            assert_eq!(
                (subject_class.as_str(), object_class.as_str()),
                ("Person", "Work")
            );
            assert!(direction_repaired);
        }
        other => panic!("expected a repaired grounding, got {other:?}"),
    }
    for size in [10usize, 1_000, 100_000] {
        let mut deduper = FactDeduper::default();
        let admitted = (0..size * 2)
            .filter(|i| deduper.admit(&fact(&format!("s{}", i % size), "p", "o")))
            .count();
        assert_eq!(admitted, size, "each distinct triple is admitted once");
    }
}

// spec: MEDULLA-INGEST-WORKER-R701.4
#[test]
fn a_fact_without_evidence_is_refused_and_an_unfit_fact_stays_an_ungrounded_claim() {
    let without_evidence = json!({
        "subject": "Ada", "predicate": "authorOf", "object": "Note G", "confidence": 0.5
    });
    assert!(serde_json::from_value::<ExtractedFact>(without_evidence).is_err());
    let empty_span = json!({
        "subject": "Ada", "predicate": "authorOf", "object": "Note G", "confidence": 0.5,
        "evidence": {"source_ref": "doc:1", "start": 4, "end": 4}
    });
    assert!(serde_json::from_value::<ExtractedFact>(empty_span).is_err());

    let cache = cache();
    for unfit in [
        fact("Ada", "unknownPredicate", "Note G"),
        fact("Ada", "authorOf", "Ada"),
        fact("Nobody", "authorOf", "Note G"),
    ] {
        let claim = ground_fact(unfit.clone(), &cache);
        assert_eq!(claim.grounding, GroundingResult::Ungrounded);
        assert_eq!(
            claim.fact, unfit,
            "the claim keeps the extracted fact intact"
        );
    }
}

fn proposal(properties: &[&str]) -> OntologyProposal {
    OntologyProposal::new(
        "p1",
        vec![ProposedClass {
            name: "Person".into(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
        }],
        vec![],
        ProposalProvenance {
            source_ref: "doc:1".into(),
            induced_by: "induction-v1".into(),
        },
    )
}

// spec: MEDULLA-INGEST-WORKER-R002.4
#[test]
fn a_failing_proposal_is_refused_stays_draft_and_allows_no_instances() {
    let observed: Vec<String> = ["name", "born", "employer", "title", "city"]
        .iter()
        .map(|n| n.to_string())
        .collect();
    let thresholds = ProposalThresholds::default();

    let thin = proposal(&["name"]);
    let refused = judge_proposal(&thin, &observed, &thresholds).unwrap_err();
    assert_eq!(
        refused,
        IngestRefusal::ProposalNotAdmitted(ProposalCheck::Coverage)
    );
    assert_eq!(refused.code(), "PROPOSAL_NOT_ADMITTED");
    assert_eq!(
        thin.state,
        ProposalState::Draft,
        "a refused proposal stays a draft"
    );

    let full = proposal(&["name", "born", "employer", "title", "city"]);
    let decision = judge_proposal(&full, &observed, &thresholds).unwrap();
    assert_eq!(decision.state, ProposalState::Admitted);
    assert!(decision.instances_allowed);
}

fn atom(id: &str, text: &str, provenance: &str) -> KnowledgeAtom {
    KnowledgeAtom {
        atom_id: id.into(),
        kind: AtomKind::Fact,
        text: text.into(),
        provenance_key: provenance.into(),
        tenant: "t1".into(),
    }
}

// spec: MEDULLA-INGEST-WORKER-R003.4
#[test]
fn an_atom_without_provenance_is_refused_and_no_relevant_atom_gives_an_empty_context() {
    let refused = check_atom_provenance(&atom("a1", "restart the worker", "  ")).unwrap_err();
    assert_eq!(refused.code(), "ATOM_PROVENANCE_MISSING");
    assert_eq!(
        check_atom_provenance(&atom("a2", "restart the worker", "doc:1#p1")),
        Ok(())
    );

    let atoms = vec![
        atom("a2", "restart the worker after upgrade", "doc:1#p1"),
        atom("a3", "rotate the signing key", "doc:1#p2"),
    ];
    assert!(select_atoms("quantum cryptography", &atoms, 100).is_empty());
    let chosen = select_atoms("restart worker", &atoms, 100);
    assert_eq!(chosen.len(), 1);
    assert_eq!(chosen[0].atom_id, "a2");
}
