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
// spec: EG-DECISION-ENGINE-R107
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

// spec: EG-DECISION-ENGINE-R107
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

/// Every schema version this build supports still decodes: the committed v1
/// golden vectors decode and replay-check clean, a record sealed at the
/// topology version round-trips and stays usable, and a version this build
/// does not recognise is refused by a typed error rather than guessed at.
// spec: EG-DECISION-ENGINE-R067
#[test]
fn every_supported_decision_record_version_decodes_and_an_unknown_version_is_refused() {
    let golden_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/decision/assembly_golden.json");
    let committed = std::fs::read_to_string(&golden_path).expect("the golden file is committed");
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(&committed).expect("the golden file decodes as JSON");
    assert!(!cases.is_empty(), "the golden file carries at least one v1 vector");
    for case in cases {
        let record: crate::decision::DecisionRecord =
            serde_json::from_value(case["record"].clone()).expect("a golden v1 record decodes");
        assert_eq!(record.schema_version, 1, "the golden file holds base-version records");
        record.checked().expect("a committed v1 vector stays usable");
    }

    let mut topology_record = wave::record(wave::every_decision_outcome().remove(0));
    topology_record.inputs.request.requirements.topology = Some(requirements(4));
    topology_record.schema_version = crate::decision::TOPOLOGY_DECISION_RECORD_SCHEMA_VERSION;
    let round_tripped: crate::decision::DecisionRecord =
        serde_json::from_str(&serde_json::to_string(&topology_record).unwrap()).unwrap();
    round_tripped
        .checked()
        .expect("a v3 (topology) vector stays usable after a round trip");

    let mut unrecognised = wave::record(wave::every_decision_outcome().remove(0));
    unrecognised.schema_version = u16::MAX;
    assert_eq!(
        unrecognised.checked(),
        Err(crate::decision::DecisionErrorCode::DecisionRecordVersionUnsupported),
        "a version this build does not recognise must be a typed refusal"
    );
}
