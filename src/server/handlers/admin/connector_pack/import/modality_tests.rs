//! R043 modality regressions. Full served acceptance remains open in the owning spec.

use super::*;
use medulla_types::connector_pack::digest::annotations_digest;

fn annotated(field: &str, terms: &[&str]) -> PackAnnotations {
    serde_json::from_value(serde_json::json!({field: terms})).unwrap()
}

// spec: MEDULLA-TYPED-PACKS-R043
#[test]
fn modalities_accept_native_terms_for_tools_prompts_and_skills() {
    for field in ["modalities_in", "modalities_out"] {
        let annotations = annotated(field, &["eg:modality/text", "eg:modality/image"]);
        for kind in [
            PackEntryKind::Tool,
            PackEntryKind::Prompt,
            PackEntryKind::Skill,
        ] {
            assert_eq!(
                check_annotations(kind, &annotations),
                AnnotationFindings::default()
            );
        }
        assert_eq!(
            serde_json::from_slice::<PackAnnotations>(&serde_json::to_vec(&annotations).unwrap())
                .unwrap(),
            annotations
        );
    }
}

// spec: MEDULLA-TYPED-PACKS-R043
#[test]
fn modalities_refuse_foreign_roots_typos_and_wrong_kinds() {
    for field in ["modalities_in", "modalities_out"] {
        for iri in [
            "https://example.org/modality/text",
            "eg:modality",
            "eg:modality/typo",
            "eg:capability/retrieval",
            "text",
            "",
        ] {
            for kind in [
                PackEntryKind::Tool,
                PackEntryKind::Prompt,
                PackEntryKind::Skill,
            ] {
                let found = check_annotations(kind, &annotated(field, &[iri]));
                assert_eq!(found.violations.len(), 1, "{field}: {iri}");
                assert_eq!(
                    found.violations[0].0,
                    PackViolationCode::UnknownCapabilityIri
                );
                assert!(found.warnings.is_empty());
            }
        }
    }
}

// spec: MEDULLA-TYPED-PACKS-R043
#[test]
fn modality_lists_enforce_wire_types_and_both_size_edges() {
    for field in ["modalities_in", "modalities_out"] {
        for count in [0, 64] {
            let annotations = annotated(field, &vec!["eg:modality/text"; count]);
            assert_eq!(
                check_annotations(PackEntryKind::Tool, &annotations),
                AnnotationFindings::default()
            );
        }
        for value in [
            serde_json::json!(null),
            serde_json::json!("eg:modality/text"),
            serde_json::json!([1]),
            serde_json::json!(vec!["eg:modality/text"; 65]),
        ] {
            assert!(
                serde_json::from_value::<PackAnnotations>(serde_json::json!({field: value}))
                    .is_err(),
                "{field}"
            );
        }
    }
}

// spec: MEDULLA-TYPED-PACKS-R043
#[test]
fn modality_digest_replay_is_canonical_and_direction_sensitive() {
    let annotations = medulla_types::test_support::contract_wave::pack::annotations();
    assert_eq!(
        annotations_digest(&annotations).unwrap().to_hex(),
        // The golden digest Python's connector-pack client must also produce.
        "2184e78879af1f369b47adac45d9b847eb9c0f9f3f1f87fe53df73140eb0fe82"
    );
    assert_eq!(
        annotations_digest(&PackAnnotations::default())
            .unwrap()
            .to_hex(),
        "0df97c2f617f661b8c75e150998cc346613a374329f484701a7ab813a6fde073"
    );
    for field in ["modalities_in", "modalities_out"] {
        let ordered = annotated(field, &["eg:modality/image", "eg:modality/text"]);
        let duplicate = annotated(
            field,
            &["eg:modality/text", "eg:modality/image", "eg:modality/text"],
        );
        assert_eq!(
            annotations_digest(&ordered).unwrap(),
            annotations_digest(&duplicate).unwrap()
        );
        assert_ne!(
            annotations_digest(&ordered).unwrap(),
            annotations_digest(&annotated(field, &["eg:modality/text"])).unwrap()
        );
    }
    assert_ne!(
        annotations_digest(&annotated("modalities_in", &["eg:modality/text"])).unwrap(),
        annotations_digest(&annotated("modalities_out", &["eg:modality/text"])).unwrap()
    );
}
