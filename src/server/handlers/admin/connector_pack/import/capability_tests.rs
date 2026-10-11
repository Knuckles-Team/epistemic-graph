//! Native G12 checks for both capability annotation fields.

use super::*;
use eg_types::contract::BoundedVec;

fn annotated(field: &str, terms: &[&str]) -> PackAnnotations {
    serde_json::from_value(serde_json::json!({field: terms})).unwrap()
}

// spec: EG-TYPED-PACKS-R042.1
#[test]
fn both_capability_fields_accept_native_terms_and_round_trip() {
    for field in ["provides", "requires_capabilities"] {
        let annotations = annotated(field, &["eg:capability/retrieval", "eg:capability/action"]);
        assert_eq!(
            check_annotations(PackEntryKind::Tool, &annotations),
            AnnotationFindings::default(),
            "{field}"
        );
        let wire = serde_json::to_vec(&annotations).unwrap();
        assert_eq!(
            serde_json::from_slice::<PackAnnotations>(&wire).unwrap(),
            annotations
        );
    }
}

// spec: EG-TYPED-PACKS-R042.1
#[test]
fn both_capability_fields_refuse_native_typos_roots_and_wrong_kinds() {
    for field in ["provides", "requires_capabilities"] {
        for iri in [
            "eg:capability/retrieval/web-searc",
            "eg:capability",
            "eg:modality/text",
        ] {
            let found = check_annotations(PackEntryKind::Tool, &annotated(field, &[iri]));
            assert_eq!(found.violations.len(), 1, "{field}: {iri}");
            assert_eq!(
                found.violations[0].0,
                PackViolationCode::UnknownCapabilityIri
            );
            assert!(found.warnings.is_empty());
        }
    }
}

// spec: EG-TYPED-PACKS-R042.1
#[test]
fn both_capability_fields_refuse_relative_and_invalid_scheme_claims() {
    for field in ["provides", "requires_capabilities"] {
        for iri in [
            "capability/search",
            "1vendor:search",
            "+vendor:search",
            ".vendor:search",
            "https:",
            "https://example.org/a b",
        ] {
            let found = check_annotations(PackEntryKind::Tool, &annotated(field, &[iri]));
            assert_eq!(found.violations.len(), 1, "{field}: {iri}");
            assert_eq!(found.violations[0].0, PackViolationCode::InvalidAnnotation);
            assert!(found.warnings.is_empty());
        }
    }
}

// spec: EG-TYPED-PACKS-R042.1
#[test]
fn foreign_terms_warn_and_stay_only_in_the_declared_partition() {
    let native = "eg:capability/retrieval";
    for field in ["provides", "requires_capabilities"] {
        for foreign in [
            "https://example.org/cap/search",
            "urn:vendor:search",
            "vendor+v1.2-x:search",
        ] {
            let annotations = annotated(field, &[native, foreign]);
            let found = check_annotations(PackEntryKind::Tool, &annotations);
            assert!(found.violations.is_empty(), "{field}: {foreign}");
            assert_eq!(
                found.warnings,
                vec![(
                    PackWarningCode::UnresolvedCapabilityIri,
                    format!("{foreign} is a declared claim outside the native vocabulary")
                )]
            );
            let terms = if field == "provides" {
                annotations.provides
            } else {
                annotations.requires_capabilities
            };
            let (classified, declared) = partition_capabilities(terms.as_slice());
            assert_eq!(classified, [native]);
            assert_eq!(declared, [foreign]);
            assert!(!eg_types::agent_ontology::is_capability(&declared[0]));
        }
    }
}

// spec: EG-TYPED-PACKS-R042.1
#[test]
fn capability_fields_remain_bounded_typed_string_lists() {
    for field in ["provides", "requires_capabilities"] {
        for value in [
            serde_json::json!("eg:capability/retrieval"),
            serde_json::json!([1]),
            serde_json::json!(vec!["eg:capability/retrieval"; 65]),
        ] {
            assert!(
                serde_json::from_value::<PackAnnotations>(serde_json::json!({field: value}))
                    .is_err()
            );
        }
    }
    assert!(BoundedVec::<String, 64>::new(vec![String::new(); 64]).is_ok());
}
