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

/// A result DTO change must move a digest: every bound result-body schema is an
/// `artifact_digests` entry, and every descriptor's pointer resolves to one.
#[test]
fn every_result_body_schema_is_digested() {
    let receipt = committed_json("contract/receipt.json");
    let digests = receipt["artifact_digests"]
        .as_object()
        .expect("the receipt carries artifact_digests");
    let methods = committed_json("contract/methods.json");
    let bound: Vec<&str> = methods["methods"]
        .as_array()
        .expect("methods.json lists methods")
        .iter()
        .filter_map(|method| method["result_body_schema"].as_str())
        .collect();
    assert!(!bound.is_empty(), "no method binds a result-body schema");
    for path in bound {
        assert!(
            digests.contains_key(path),
            "{path} is referenced by methods.json but not digested"
        );
    }
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
