//! Ontology diff and change-impact records (EG-TYPED-PACKS-R804.1).

use eg_types::ontology_diff::{
    ChangeImpact, ImpactCount, OntologyDiff, OntologyRevisionRef, MAX_IMPACT_ITEMS,
    ONTOLOGY_DIFF_IRI_MISMATCH,
};

fn rev(iri: &str, revision: &str) -> OntologyRevisionRef {
    OntologyRevisionRef {
        ontology_iri: iri.into(),
        revision: revision.into(),
    }
}

fn diff() -> OntologyDiff {
    OntologyDiff {
        from_rev: rev("http://example.org/sales", "1"),
        to_rev: rev("http://example.org/sales", "2"),
        added: vec!["http://example.org/sales#Refund".into()],
        removed: vec!["http://example.org/sales#Coupon".into()],
        changed: vec![
            "http://example.org/sales#Order".into(),
            "http://example.org/sales#total".into(),
        ],
        deprecated: vec!["http://example.org/sales#legacyId".into()],
    }
}

// spec: EG-TYPED-PACKS-R804.1
#[test]
fn diff_round_trips() {
    let diff = diff();
    diff.validate().unwrap();
    let json: OntologyDiff = serde_json::from_value(serde_json::to_value(&diff).unwrap()).unwrap();
    assert_eq!(json, diff);
    let packed: OntologyDiff =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&diff).unwrap()).unwrap();
    assert_eq!(packed, diff);

    let impact = ChangeImpact {
        instances_invalid: ImpactCount {
            count: 3,
            first: vec!["order:1".into(), "order:2".into(), "order:3".into()],
        },
        shapes_broken: ImpactCount {
            count: 1,
            first: vec!["shape:CouponShape".into()],
        },
        mappings_broken: ImpactCount::default(),
        queries_broken: ImpactCount {
            count: 250,
            first: (0..MAX_IMPACT_ITEMS).map(|i| format!("query:{i}")).collect(),
        },
    };
    impact.validate().unwrap();
    assert!(impact.is_breaking());
    assert!(!ChangeImpact::default().is_breaking());
    let json: ChangeImpact =
        serde_json::from_value(serde_json::to_value(&impact).unwrap()).unwrap();
    assert_eq!(json, impact);
}

// spec: EG-TYPED-PACKS-R804.1
#[test]
fn invalid_records_are_refused() {
    let across = OntologyDiff {
        to_rev: rev("http://example.org/other", "2"),
        ..diff()
    };
    assert_eq!(
        across.validate().unwrap_err().code,
        ONTOLOGY_DIFF_IRI_MISMATCH
    );
    let unsorted = OntologyDiff {
        changed: vec!["b".into(), "a".into()],
        ..diff()
    };
    assert_eq!(unsorted.validate().unwrap_err().field, "changed");
    let mut extra = serde_json::to_value(diff()).unwrap();
    extra["note"] = "x".into();
    assert!(serde_json::from_value::<OntologyDiff>(extra).is_err());
    let overlong = ChangeImpact {
        mappings_broken: ImpactCount {
            count: 1_000,
            first: (0..=MAX_IMPACT_ITEMS).map(|i| format!("m:{i}")).collect(),
        },
        ..ChangeImpact::default()
    };
    assert_eq!(overlong.validate().unwrap_err().field, "mappings_broken");
    let miscounted = ChangeImpact {
        shapes_broken: ImpactCount {
            count: 0,
            first: vec!["shape:a".into()],
        },
        ..ChangeImpact::default()
    };
    assert_eq!(miscounted.validate().unwrap_err().field, "shapes_broken");
}
