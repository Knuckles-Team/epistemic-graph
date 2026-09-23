use serde_json::json;

use super::*;

fn stored_claim() -> Value {
    json!({
        "type": "Claim",
        "family": "association_rules",
        "about": "rule:a->b",
        "confidence": 0.8,
        "validation_state": "unvalidated",
        "input_snapshot_version": 7,
        "algo_code_version": "2.27.0",
        "calibration": null,
        "invalidation_deps": ["rule:a->b", "evidence:1"],
    })
}

fn stored_evidence() -> Value {
    json!({
        "type": "Evidence",
        "family": "association_rules",
        "about": "rule:a->b",
        "provenance": "explicit",
        "confidence": 0.8,
        "validation_state": "unvalidated",
        "shots": 1024,
    })
}

#[test]
fn a_stored_claim_decodes_and_re_encodes_to_the_same_object() {
    let stored = stored_claim();
    let claim = Claim::from_properties(&stored).expect("valid claim");
    assert_eq!(claim.about, "rule:a->b");
    assert_eq!(claim.calibration, None);
    assert_eq!(claim.invalidation_deps, vec!["rule:a->b", "evidence:1"]);
    assert_eq!(claim.attributes["input_snapshot_version"], json!(7));
    assert_eq!(claim.to_properties().expect("encodes"), stored);
}

#[test]
fn a_stored_evidence_node_decodes_and_re_encodes_to_the_same_object() {
    let stored = stored_evidence();
    let evidence = Evidence::from_properties(&stored).expect("valid evidence");
    assert_eq!(evidence.provenance, "explicit");
    assert_eq!(evidence.attributes["shots"], json!(1024));
    assert_eq!(evidence.to_properties().expect("encodes"), stored);
}

#[test]
fn a_calibrated_claim_carries_its_typed_interval() {
    let mut stored = stored_claim();
    stored["calibration"] = json!({"interval": [0.6, 0.9], "level": 0.95, "evidence_count": 3});
    let claim = Claim::from_properties(&stored).expect("valid claim");
    let calibration = claim.calibration.expect("calibrated");
    assert_eq!(calibration.interval, (0.6, 0.9));
    assert_eq!(calibration.evidence_count, 3);
    assert_eq!(claim.to_properties().expect("encodes"), stored);
}

#[test]
fn the_dispatching_decoder_names_each_kind_and_ignores_other_nodes() {
    let claim = EpistemicNode::from_properties(&stored_claim()).expect("decodes");
    assert_eq!(
        claim.map(|node| node.kind()),
        Some(EpistemicNodeKind::Claim)
    );
    let evidence = EpistemicNode::from_properties(&stored_evidence()).expect("decodes");
    assert_eq!(
        evidence.map(|node| node.kind()),
        Some(EpistemicNodeKind::Evidence)
    );
    let other = EpistemicNode::from_properties(&json!({"type": "Activity"})).expect("decodes");
    assert_eq!(other, None);
    assert_eq!(
        EpistemicNode::from_properties(&json!(42)).expect("decodes"),
        None
    );
}

#[test]
fn a_claim_with_a_missing_core_field_is_malformed_not_skipped() {
    let mut stored = stored_claim();
    stored.as_object_mut().expect("object").remove("about");
    assert!(matches!(
        Claim::from_properties(&stored),
        Err(EpistemicNodeError::Malformed { .. })
    ));
    assert!(EpistemicNode::from_properties(&stored).is_err());
}

#[test]
fn empty_text_and_out_of_range_confidence_are_refused() {
    let mut empty = stored_evidence();
    empty["provenance"] = json!("  ");
    assert_eq!(
        Evidence::from_properties(&empty),
        Err(EpistemicNodeError::EmptyField {
            field: "provenance".into()
        })
    );
    let mut high = stored_claim();
    high["confidence"] = json!(1.5);
    assert_eq!(
        Claim::from_properties(&high),
        Err(EpistemicNodeError::ConfidenceOutOfRange)
    );
}

#[test]
fn decoding_one_kind_as_the_other_names_both() {
    assert_eq!(
        Evidence::from_properties(&stored_claim()),
        Err(EpistemicNodeError::WrongKind {
            expected: EpistemicNodeKind::Evidence,
            found: Some("Claim".into()),
        })
    );
    assert_eq!(
        Claim::from_properties(&json!([])),
        Err(EpistemicNodeError::NotAnObject)
    );
}

#[test]
fn an_attribute_cannot_shadow_the_node_label() {
    let mut claim = Claim::from_properties(&stored_claim()).expect("valid claim");
    claim.attributes.insert("type".into(), json!("Evidence"));
    assert!(matches!(
        claim.to_properties(),
        Err(EpistemicNodeError::Unencodable { .. })
    ));
}

#[test]
fn every_kind_label_round_trips() {
    for (kind, label) in KIND_LABELS {
        assert_eq!(kind.label(), label);
        assert_eq!(EpistemicNodeKind::from_label(label), Some(kind));
    }
    assert_eq!(EpistemicNodeKind::from_label("claim"), None);
}

#[test]
fn a_built_claim_encodes_to_the_stored_convention() {
    let built = Claim::new("association_rules", "rule:a->b", 0.8, "unvalidated")
        .with_invalidation_deps(["rule:a->b", "evidence:1"])
        .with_attributes(json!({"input_snapshot_version": 7, "algo_code_version": "2.27.0"}))
        .expect("object attributes");
    assert_eq!(built.to_properties().expect("encodes"), stored_claim());
}

#[test]
fn a_built_evidence_node_encodes_to_the_stored_convention() {
    let built = Evidence::new(
        "association_rules",
        "rule:a->b",
        "explicit",
        0.8,
        "unvalidated",
    )
    .with_attributes(json!({"shots": 1024}))
    .expect("object attributes");
    assert_eq!(built.to_properties().expect("encodes"), stored_evidence());
}

#[test]
fn attributes_must_be_an_object_and_must_not_shadow_a_typed_field() {
    let base = || Claim::new("f", "a", 0.5, "unvalidated");
    assert!(matches!(
        base().with_attributes(json!([1, 2])),
        Err(EpistemicNodeError::Malformed { .. })
    ));
    let shadowing = base()
        .with_attributes(json!({"confidence": 0.99}))
        .expect("object attributes");
    assert!(matches!(
        shadowing.to_properties(),
        Err(EpistemicNodeError::Unencodable { .. })
    ));
}

#[test]
fn an_invalid_built_claim_is_refused_at_encode_time() {
    let claim = Claim::new("f", "", 0.5, "unvalidated");
    assert_eq!(
        claim.to_properties(),
        Err(EpistemicNodeError::EmptyField {
            field: "about".into()
        })
    );
}
