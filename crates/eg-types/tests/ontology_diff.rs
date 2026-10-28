use eg_types::ontology_diff::{ChangeImpact, OntologyDiff};

#[test]
fn diff_round_trips() {
    let diff = OntologyDiff {
        from_rev: "urn:example:revision:1".into(),
        to_rev: "urn:example:revision:2".into(),
        added: vec!["class:Invoice".into()],
        removed: vec!["property:legacyCode".into()],
        changed: vec!["property:total.range".into()],
        deprecated: vec!["class:OldInvoice".into()],
    };
    let impact = ChangeImpact {
        instances_invalid: 2,
        shapes_broken: 1,
        mappings_broken: 3,
        queries_broken: 4,
    };

    assert_eq!(
        serde_json::from_str::<OntologyDiff>(&serde_json::to_string(&diff).unwrap()).unwrap(),
        diff
    );
    assert_eq!(
        serde_json::from_str::<ChangeImpact>(&serde_json::to_string(&impact).unwrap()).unwrap(),
        impact
    );
}
