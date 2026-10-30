//! EG-MEDIA-VOCAB-R650.1: the media memory types are declared in the generated
//! contract, and a field change that was not regenerated fails here.

#![cfg(feature = "contract")]

use std::collections::BTreeSet;
use std::path::PathBuf;

use eg_types::media_memory::{MediaDerivation, MediaMemory, MediaRecall};

fn contract(path: &str) -> serde_json::Value {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join(path);
    serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap()
}

fn rust_fields(value: serde_json::Value) -> BTreeSet<String> {
    value
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect()
}

fn schema_fields(defs: &serde_json::Value, name: &str) -> BTreeSet<String> {
    defs[name]["properties"]
        .as_object()
        .unwrap_or_else(|| panic!("{name} is not declared in the contract schemas"))
        .keys()
        .cloned()
        .collect()
}

// spec: EG-MEDIA-VOCAB-R650.1
#[test]
fn media_memory_types_are_declared_in_the_contract_with_their_exact_fields() {
    let methods = contract("contract/methods.json");
    let ids: BTreeSet<&str> = methods["methods"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["id"].as_str())
        .collect();
    assert!(ids.contains("MediaRemember") && ids.contains("MediaRecall"));

    let results = contract("contract/schemas/result.storage.json");
    let requests = contract("contract/schemas/method.request.json");
    let memory = MediaMemory {
        blob_id: "b".into(),
        mime: "image/png".into(),
        transcript_ref: None,
        source_message: "m".into(),
    };
    let recall = MediaRecall {
        blob_id: "b".into(),
        mime: "image/png".into(),
        size: 1,
        derivation_ids: vec![],
        citing_memory: "c".into(),
    };
    let derivation = MediaDerivation {
        derivation_id: "d".into(),
        text: "t".into(),
    };
    assert_eq!(schema_fields(&results["$defs"], "MediaMemory"), rust_fields(serde_json::to_value(&memory).unwrap()));
    assert_eq!(schema_fields(&results["$defs"], "MediaRecall"), rust_fields(serde_json::to_value(&recall).unwrap()));
    assert_eq!(
        schema_fields(&requests["$defs"], "MediaDerivation"),
        rust_fields(serde_json::to_value(&derivation).unwrap())
    );
    // Serde-stable: an undeclared field is refused on the wire.
    let mut extra = serde_json::to_value(&memory).unwrap();
    extra["undeclared"] = serde_json::json!(1);
    assert!(serde_json::from_value::<MediaMemory>(extra).is_err());
}
