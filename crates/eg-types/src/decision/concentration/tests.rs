use super::*;

const DIMENSION: &str = "urn:harness:dimension:context";

fn policy() -> ConcentrationPolicy {
    ConcentrationPolicy {
        policy_iri: "urn:policy:concentration".into(),
        min_shipped: 3,
        window_rounds: 3,
        revision: 7,
    }
}

fn edit(id: &str, round: u32, status: EditStatus) -> EditFact {
    EditFact {
        edit_id: id.into(),
        dimension_iri: DIMENSION.into(),
        round,
        status,
        policy_revision: 7,
    }
}

// spec: MEDULLA-DECISION-ENGINE-R173.1
#[test]
fn policy_round_trips() {
    let value = serde_json::to_value(policy()).unwrap();
    assert_eq!(
        serde_json::from_value::<ConcentrationPolicy>(value.clone()).unwrap(),
        policy()
    );
    let mut zero = value.clone();
    zero["window_rounds"] = serde_json::json!(0);
    assert!(serde_json::from_value::<ConcentrationPolicy>(zero).is_err());
    let mut unknown = value;
    unknown["extra"] = serde_json::json!(1);
    assert!(serde_json::from_value::<ConcentrationPolicy>(unknown).is_err());
    let fact = edit("e1", 2, EditStatus::Shipped);
    let wire = serde_json::to_value(&fact).unwrap();
    assert_eq!(wire["status"], "shipped");
    assert_eq!(serde_json::from_value::<EditFact>(wire).unwrap(), fact);
    let verdict =
        concentration_verdict(&[], &edit("e1", 2, EditStatus::Proposed), Some(&policy())).unwrap();
    let wire = serde_json::to_value(&verdict).unwrap();
    assert_eq!(wire["action"], "keep");
    assert_eq!(
        serde_json::from_value::<ConcentrationVerdict>(wire).unwrap(),
        verdict
    );
    assert_eq!(
        serde_json::to_value(crate::decision::statistical::QuestionKind::EditConcentration)
            .unwrap(),
        "edit_concentration"
    );
}

// spec: MEDULLA-DECISION-ENGINE-R173.2
#[test]
fn third_edit_in_window_reverts_and_two_keep() {
    let facts = [
        edit("e2", 2, EditStatus::Shipped),
        edit("e3", 3, EditStatus::Shipped),
    ];
    let third = concentration_verdict(
        &facts,
        &edit("e4", 4, EditStatus::Proposed),
        Some(&policy()),
    )
    .unwrap();
    assert_eq!(
        (
            third.count,
            third.first_round,
            third.last_round,
            third.action
        ),
        (3, 2, 4, ConcentrationAction::Revert)
    );
    let two = concentration_verdict(
        &facts[..1],
        &edit("e4", 4, EditStatus::Proposed),
        Some(&policy()),
    )
    .unwrap();
    assert_eq!((two.count, two.action), (2, ConcentrationAction::Keep));
    // Outside the window, reverted edits and other dimensions do not count.
    let mut other = edit("o", 3, EditStatus::Shipped);
    other.dimension_iri = "urn:harness:dimension:tools".into();
    let mixed = [
        edit("old", 1, EditStatus::Shipped),
        edit("rev", 3, EditStatus::Reverted),
        other,
        edit("e3", 3, EditStatus::Shipped),
    ];
    let kept = concentration_verdict(
        &mixed,
        &edit("e4", 4, EditStatus::Proposed),
        Some(&policy()),
    )
    .unwrap();
    assert_eq!((kept.count, kept.first_round), (2, 3));
    assert_eq!(kept.action, ConcentrationAction::Keep);
}

// spec: MEDULLA-DECISION-ENGINE-R173.2
#[test]
fn ledger_evicts_rounds_outside_the_window() {
    let mut ledger = ConcentrationLedger::new(3);
    for round in 1..=6 {
        ledger.ship(DIMENSION, round);
    }
    assert_eq!(ledger.window(DIMENSION, 6), [4, 5, 6]);
    assert_eq!(ledger.window(DIMENSION, 8), [6]);
    assert!(ledger.window("urn:none", 8).is_empty());
}

// spec: MEDULLA-DECISION-ENGINE-R173.4
#[test]
fn missing_policy_and_mixed_revisions_abstain() {
    let candidate = edit("e4", 4, EditStatus::Proposed);
    assert_eq!(
        concentration_verdict(&[], &candidate, None).unwrap_err(),
        ConcentrationAbstention::NoConcentrationPolicy
    );
    let mut stale = edit("e2", 2, EditStatus::Shipped);
    stale.policy_revision = 6;
    let refusal = concentration_verdict(&[stale], &candidate, Some(&policy())).unwrap_err();
    assert_eq!(
        refusal,
        ConcentrationAbstention::MixedPolicyRevision {
            revisions: vec![6, 7]
        }
    );
    assert_eq!(
        serde_json::to_value(&refusal).unwrap()["reason"],
        "mixed_policy_revision"
    );
    assert_eq!(
        serde_json::to_value(ConcentrationAbstention::NoConcentrationPolicy).unwrap()["reason"],
        "no_concentration_policy"
    );
}
