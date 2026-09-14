//! The committed contract IS what the registry generates.
//!
//! `gen_contract --check` is the same assertion as a shell command; running it here as
//! well means a stale `contract/`, `docs/capabilities.generated.md` or
//! `epistemic_graph/generated/**` fails `cargo test`, not only the pre-push hook.
//!
//! It needs the schemas, so it can only exist under the generator's own profile:
//! `cargo test -p eg-capabilities --features contract`. A plain `cargo test
//! -p eg-capabilities` compiles this file away entirely and reports green having run
//! none of it -- which is why `--features contract` is the command the pre-commit hook
//! and release.yml both run, and the one to use locally.

#![cfg(all(feature = "canonical-ledger", feature = "contract-schema"))]

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/eg-capabilities is two levels below the repo root")
        .to_path_buf()
}

#[test]
fn committed_contract_artifacts_match_the_registry() {
    match eg_capabilities::contract::check(&repo_root()) {
        Ok(count) => assert!(count > 0, "the generator produced no artifacts"),
        Err(drift) => panic!(
            "the committed engine contract is STALE -- regenerate with `cargo run \
             -p eg-capabilities --features contract --bin gen_contract`:\n{}",
            drift.join("\n")
        ),
    }
}

fn committed_json(path: &str) -> serde_json::Value {
    let text = std::fs::read_to_string(repo_root().join(path))
        .unwrap_or_else(|error| panic!("cannot read {path}: {error}"));
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("{path} is not JSON: {error}"))
}

/// The contract profile turns on `eg-types/timeseries` (via `canonical-ledger`), so the
/// `Op` variants that feature gates are part of the published request schema. `Op` is
/// externally tagged: a variant's name is its subschema's single required key.
#[test]
fn request_schema_covers_the_timeseries_op_variants() {
    let document = committed_json("contract/schemas/method.request.json");
    let variants: std::collections::BTreeSet<String> = document["$defs"]["Op"]["oneOf"]
        .as_array()
        .expect("`Op` is a oneOf over its variants")
        .iter()
        .filter_map(|variant| variant["required"].as_array()?.first()?.as_str())
        .map(str::to_string)
        .collect();
    for gated in ["SensorAlign", "SensorFuse", "TsScan"] {
        assert!(
            variants.contains(gated),
            "`Op::{gated}` is missing from method.request.json -- the contract profile no \
             longer enables `eg-types/timeseries`"
        );
    }
}

/// A result-type change must move a digest: every declared result points into a
/// schema document that is an `artifact_digests` entry, and the receipt's classification
/// accounts for every method exactly once.
#[test]
fn every_declared_result_schema_is_digested() {
    let receipt = committed_json("contract/receipt.json");
    let digests = receipt["artifact_digests"]
        .as_object()
        .expect("the receipt carries artifact_digests");
    let methods = committed_json("contract/methods.json");
    let methods = methods["methods"]
        .as_array()
        .expect("methods.json lists methods");
    let mut declared = 0;
    for method in methods {
        let result = &method["result_schema"];
        if result["kind"] == "unclassified" {
            continue;
        }
        declared += 1;
        let document = result["schema"]
            .as_str()
            .and_then(|pointer| pointer.split('#').next())
            .expect("a declared result names its schema document");
        assert!(
            digests.contains_key(document),
            "{document} is referenced by methods.json but not digested"
        );
    }
    let classification = receipt["result_classification"]
        .as_object()
        .expect("the receipt classifies results");
    let total: u64 = classification.values().filter_map(|n| n.as_u64()).sum();
    assert_eq!(total as usize, methods.len());
    assert_eq!(
        classification["unclassified"].as_u64(),
        Some((methods.len() - declared) as u64)
    );
}

#[test]
fn split_transaction_result_marker_is_collected_by_the_generator() {
    let artifacts = eg_capabilities::contract::render_all(&repo_root());
    let methods = artifacts
        .iter()
        .find(|artifact| artifact.path == "contract/methods.json")
        .expect("the generator renders methods.json");
    let methods: serde_json::Value =
        serde_json::from_slice(&methods.bytes).expect("the generated methods artifact is JSON");
    let method = methods["methods"]
        .as_array()
        .and_then(|methods| {
            methods
                .iter()
                .find(|method| method["id"] == "ApplyMultisigMutation")
        })
        .expect("the transaction descriptor is present");
    assert_eq!(method["result_schema"]["kind"], "declared");
    assert_eq!(
        method["result_schema"]["bodies"]["result"]["encoding"],
        "Json"
    );

    let transactions = artifacts
        .iter()
        .find(|artifact| artifact.path == "epistemic_graph/generated/transactions.py")
        .expect("the generator renders the transactions client");
    let transactions = String::from_utf8_lossy(&transactions.bytes);
    assert!(transactions.contains(
        "Result: ResultPayload::Json \
         (contract/schemas/result.transactions.json#/methods/ApplyMultisigMutation)."
    ));
}

#[test]
fn every_declared_format_identity_exists_in_the_tree() {
    let root = repo_root();
    let collected: std::collections::BTreeSet<String> =
        eg_capabilities::contract::collect_format_identities(&root)
            .into_iter()
            .map(|identity| identity.name)
            .collect();
    let mut missing = Vec::new();
    for descriptor in eg_capabilities::method_descriptors() {
        for name in descriptor.format_identities {
            if !collected.contains(*name) {
                missing.push(format!("{}: {name}", descriptor.id.as_str()));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "a descriptor names a storage-format identity that no constant in the tree \
         declares -- it was renamed or removed:\n{}",
        missing.join("\n")
    );
}
