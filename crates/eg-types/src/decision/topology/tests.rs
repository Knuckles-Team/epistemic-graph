use super::*;
use crate::decision::record::schema_version_for;
use crate::test_support::contract_wave::decision as wave;
use crate::test_support::topology::{facts, requirements, slot};

#[test]
fn a_request_may_only_tighten_the_policy_caps() {
    let policy = TopologyPolicy::engine_default().caps;
    let mut request = requirements(4).caps;
    assert_eq!(request.loosened_field(&policy), None);
    request.max_width = policy.max_width + 1;
    assert_eq!(request.loosened_field(&policy), Some("max_width"));
    request.max_width = 2;
    request.max_tokens = Some(policy.max_tokens.unwrap() + 1);
    assert_eq!(request.loosened_field(&policy), Some("max_tokens"));
    request.max_tokens = None;
    let effective = policy.tightened_by(&request);
    assert_eq!(effective.max_width, 2);
    assert_eq!(effective.max_tokens, policy.max_tokens);
}

#[test]
fn stop_rules_and_slots_are_range_checked() {
    assert!(StopRule::Quorum { k: 3, n: 2 }.validate().is_err());
    assert!(StopRule::Quorum { k: 2, n: 3 }.validate().is_ok());
    assert!(StopRule::MaxRounds { n: 0 }.validate().is_err());
    let mut wide = facts();
    wide.slots = BoundedVec::new(vec![slot("w", SlotRole::Child, (1, 9), None)]).unwrap();
    assert!(wide.validate().unwrap_err().contains("choices"));
    let mut inverted = facts();
    inverted.slots = BoundedVec::new(vec![slot("w", SlotRole::Child, (3, 2), None)]).unwrap();
    assert!(inverted.validate().is_err());
    let mut repeated = facts();
    repeated.slots = BoundedVec::new(vec![
        slot("w", SlotRole::Child, (1, 2), None),
        slot("w", SlotRole::Peer, (1, 1), None),
    ])
    .unwrap();
    assert!(repeated.validate().unwrap_err().contains("repeated"));
    facts().validate().expect("the fixture is valid");
}

#[test]
fn the_facts_digest_moves_with_every_fact() {
    let base = facts_digest(&facts());
    let mut moved = facts();
    moved.slots = BoundedVec::new(vec![slot("lead", SlotRole::Parent, (1, 2), Some(100))]).unwrap();
    assert_ne!(facts_digest(&moved), base);
    let mut stop = facts();
    stop.stop = StopRule::Budget;
    assert_ne!(facts_digest(&stop), base);
}

/// A v1 record carries no topology key anywhere, so its bytes -- and every
/// committed v1 digest -- are exactly what they were before topology existed.
// spec: EG-CONTRACT-R020
#[test]
fn a_plain_record_serializes_no_topology_key_and_stays_at_base_version() {
    for outcome in wave::every_decision_outcome() {
        let record = wave::record(outcome);
        let json = serde_json::to_string(&record).unwrap();
        assert!(!json.contains("\"topology\""), "{json}");
        assert_eq!(schema_version_for(&record.inputs.request), 1);
        record.clone().checked().expect("a v1 record stays usable");
    }
}

#[test]
fn a_topology_request_is_sealed_as_the_topology_version() {
    let mut record = wave::record(wave::every_decision_outcome().remove(1));
    record.inputs.request.requirements.topology = Some(requirements(4));
    assert_eq!(
        schema_version_for(&record.inputs.request),
        crate::decision::TOPOLOGY_DECISION_RECORD_SCHEMA_VERSION
    );
    assert_eq!(
        record.clone().checked(),
        Err(crate::decision::DecisionErrorCode::DecisionRecordVersionUnsupported),
        "a v1 header over a topology request is refused, typed"
    );
    record.schema_version = crate::decision::TOPOLOGY_DECISION_RECORD_SCHEMA_VERSION;
    let round_tripped: crate::decision::DecisionRecord =
        serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
    assert_eq!(round_tripped.checked().unwrap(), record);
}
