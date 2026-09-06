//! The committed contract IS what the registry generates.
//!
//! `gen_contract --check` is the same assertion as a shell command; running it here as
//! well means a stale `contract/` or `docs/capabilities.generated.md` fails the ordinary
//! `cargo test -p eg-capabilities` a developer already runs, not only the pre-push hook.

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
