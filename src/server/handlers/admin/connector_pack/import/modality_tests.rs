//! R043 modality regressions. Full served acceptance remains open in the owning spec.

use super::*;
use eg_types::connector_pack::digest::annotations_digest;

fn annotated(field: &str, terms: &[&str]) -> PackAnnotations {
    serde_json::from_value(serde_json::json!({field: terms})).unwrap()
}

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

#[test]
fn modality_digest_replay_is_canonical_and_direction_sensitive() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contract/fixtures/connector_pack_digest_vectors.json"
    )))
    .unwrap();
    let full = vectors["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "annotations_digest/full")
        .unwrap();
    let annotations = eg_types::test_support::contract_wave::pack::annotations();
    assert_eq!(
        annotations_digest(&annotations).unwrap().to_hex(),
        full["sha256"].as_str().unwrap()
    );
    let empty = vectors["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "annotations_digest/empty")
        .unwrap();
    assert_eq!(
        annotations_digest(&PackAnnotations::default())
            .unwrap()
            .to_hex(),
        empty["sha256"].as_str().unwrap()
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
