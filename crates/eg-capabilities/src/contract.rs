//! The one engine-contract generator (RF-RULING-003).
//!
//! Renders every committed artifact under `contract/` plus `docs/capabilities.generated.md`
//! from [`crate::method_descriptors`] — the single hand-authored registry. Nothing here
//! reads a second source, so a generated file can never become a rival source of truth.
//! [`check`] regenerates in memory and byte-diffs the committed copies; it replaces the
//! deleted `tests/test_protocol_parity.py` + `protocol_unbound_baseline.txt` ratchet and
//! `consistency.rs::generated_ledger_is_not_stale`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::{ConsumerProfile, MethodDescriptor, Stability};

mod format_identity;
mod python;
mod python_render;
mod results;
mod schema;
mod vectors;

use results::{Catalog, ResultClass};

pub use format_identity::{collect_format_identities, FormatIdentity};

/// One generated file: repo-relative path plus its exact bytes.
pub struct Artifact {
    pub path: String,
    pub bytes: Vec<u8>,
}

/// The HAND-WRITTEN inputs the contract is derived from, digested into
/// `receipt.json.source_tree_oid`.
///
/// `epistemic_graph/client.py` is here because it is the transport every one of
/// agent-utilities' import lines actually consumes (`SyncEpistemicGraphClient`, framing,
/// auth, pooling). It is NOT generated, so nothing else would bind it, and a
/// transport-breaking edit must move the digest AU pins. `pyproject.toml` is here because
/// the generated client's `pydantic` dependency is part of what the wheel promises.
///
/// The GENERATED half -- `contract/**` and `epistemic_graph/generated/**` -- is bound by
/// `artifact_digests` (an exact sha256 per file, which is strictly stronger than folding
/// them into one rolled-up input) and both halves are folded into the single
/// `contract_digest` below. Digesting a generated file as a *source input* would be
/// self-referential and would not reproduce from a clean checkout.
const HAND_WRITTEN_INPUTS: &[&str] = &[
    "crates/eg-capabilities/src",
    "crates/eg-capabilities/Cargo.toml",
    "crates/eg-types/src",
    "crates/eg-types/Cargo.toml",
    "epistemic_graph/client.py",
    "pyproject.toml",
];

/// The optional surfaces this build selected, in declaration order.
fn feature_profile() -> Vec<&'static str> {
    [
        ("contract", cfg!(feature = "contract")),
        ("canonical-ledger", cfg!(feature = "canonical-ledger")),
        ("contract-schema", cfg!(feature = "contract-schema")),
        ("jobs", cfg!(feature = "jobs")),
        ("statechart", cfg!(feature = "statechart")),
        ("knowledge-batch", cfg!(feature = "knowledge-batch")),
        ("modality-serving", cfg!(feature = "modality-serving")),
        ("quantum", cfg!(feature = "quantum")),
        ("asr-native", cfg!(feature = "asr-native")),
        ("viz", cfg!(feature = "viz")),
    ]
    .into_iter()
    .filter(|(_, on)| *on)
    .map(|(name, _)| name)
    .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// A method's declared result, as `contract/methods.json` records it.
fn result_schema_json(d: &MethodDescriptor, catalog: &Catalog) -> serde_json::Value {
    let id = d.id.as_str();
    let Some(declared) = catalog.methods.get(id) else {
        return serde_json::json!({ "kind": "unclassified" });
    };
    let bodies: BTreeMap<&str, serde_json::Value> = declared
        .bodies
        .iter()
        .map(|(key, body)| {
            (
                *key,
                serde_json::json!({
                    "encoding": body.encoding,
                    "dynamic": body.dynamic.map(|reason| reason.as_str()),
                }),
            )
        })
        .collect();
    serde_json::json!({
        "kind": "declared",
        "schema": format!("{}#/methods/{id}", schema::result_document_path(d.domain)),
        "selected_by": declared.by_op.then_some("op"),
        "bodies": bodies,
    })
}

fn descriptor_json(d: &MethodDescriptor, catalog: &Catalog) -> serde_json::Value {
    let id = d.id.as_str();
    serde_json::json!({
        "id": id,
        "domain": d.domain,
        "request_schema": {
            "kind": "named",
            "schema": format!("contract/schemas/method.request.json#/methods/{id}"),
        },
        "result_schema": result_schema_json(d, catalog),
        "error_set": d.error_set,
        "policy": {
            "mutates": d.policy.mutates,
            "durability_domain": format!("{:?}", d.policy.durability_domain),
            "authz_action": d.policy.authz_action,
            "idempotent": d.policy.idempotent,
            "audited": d.policy.audited,
            "emits_cdc": d.policy.emits_cdc,
            "txn_participation": format!("{:?}", d.policy.txn_participation),
        },
        "replay_class": format!("{:?}", d.replay_class),
        "consumer_profiles": d.consumer_profiles.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
        "stability": d.stability.as_str(),
        "format_identities": d.format_identities,
        "note": d.note,
    })
}

/// `contract/methods.json` — the complete registry, deterministic domain order.
fn methods_json(catalog: &Catalog) -> Vec<u8> {
    let methods: Vec<_> = crate::method_descriptors()
        .map(|d| descriptor_json(&d, catalog))
        .collect();
    let doc = serde_json::json!({
        "contract_version": 1,
        "generator": "eg-capabilities/gen_contract",
        "method_count": methods.len(),
        "methods": methods,
    });
    pretty(&doc)
}

fn pretty(value: &serde_json::Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).expect("contract JSON is serializable");
    bytes.push(b'\n');
    bytes
}

/// Read every contract source input and digest it, without git: the receipt must stay
/// stable across the commit that lands the generated outputs it describes.
fn source_tree_oid(root: &Path) -> String {
    let mut files: Vec<PathBuf> = Vec::new();
    for input in HAND_WRITTEN_INPUTS {
        collect_files(&root.join(input), &mut files);
    }
    files.sort();
    let mut hasher = Sha256::new();
    for path in files {
        let rel = path.strip_prefix(root).unwrap_or(path.as_path());
        hasher.update(rel.to_string_lossy().as_bytes());
        hasher.update([0u8]);
        hasher.update(std::fs::read(&path).unwrap_or_default());
        hasher.update([0u8]);
    }
    hex::encode(hasher.finalize())
}

pub(crate) fn collect_files(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_file() {
        out.push(path.to_path_buf());
        return;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        collect_files(&entry.path(), out);
    }
}

/// Per method: every body schematized, every body declared caller-shaped, a mix of the
/// two across ops, or no declared result at all.
fn result_classification(catalog: &Catalog) -> serde_json::Value {
    let mut counts: BTreeMap<&str, usize> = [
        ("declared_dynamic", 0),
        ("mixed", 0),
        ("schematized", 0),
        ("unclassified", 0),
    ]
    .into_iter()
    .collect();
    for descriptor in crate::method_descriptors() {
        let key = match catalog
            .methods
            .get(descriptor.id.as_str())
            .map(|d| d.class())
        {
            Some(ResultClass::Schematized) => "schematized",
            Some(ResultClass::Dynamic) => "declared_dynamic",
            Some(ResultClass::Mixed) => "mixed",
            None => "unclassified",
        };
        *counts.entry(key).or_insert(0) += 1;
    }
    serde_json::json!(counts)
}

fn consumer_census() -> (usize, usize) {
    let python = crate::method_descriptors()
        .filter(|d| d.serves(ConsumerProfile::PythonClient))
        .count();
    let internal = crate::method_descriptors()
        .filter(|d| matches!(d.stability, Stability::Internal))
        .count();
    (python, internal)
}

/// The ONE digest a consumer pins: the hand-written source digest folded together with
/// every generated artifact's digest, in deterministic path order.
fn contract_digest(source_tree_oid: &str, digests: &BTreeMap<&str, String>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source_tree_oid.as_bytes());
    hasher.update([0u8]);
    for (path, digest) in digests {
        hasher.update(path.as_bytes());
        hasher.update([0u8]);
        hasher.update(digest.as_bytes());
        hasher.update([0u8]);
    }
    hex::encode(hasher.finalize())
}

/// `contract/receipt.json` — what AU pins.
fn receipt_json(root: &Path, artifacts: &[Artifact], catalog: &Catalog) -> Vec<u8> {
    let lock = std::fs::read(root.join("Cargo.lock")).unwrap_or_default();
    let (python, internal) = consumer_census();
    let digests: BTreeMap<&str, String> = artifacts
        .iter()
        .map(|a| (a.path.as_str(), sha256_hex(&a.bytes)))
        .collect();
    let identities: BTreeMap<String, serde_json::Value> = collect_format_identities(root)
        .into_iter()
        .map(|identity| {
            let sites: Vec<serde_json::Value> = identity
                .sites
                .iter()
                .map(|site| {
                    serde_json::json!({
                        "file": site.file,
                        "scope": site.scope,
                        "value": site.value,
                    })
                })
                .collect();
            (identity.name, serde_json::json!(sites))
        })
        .collect();
    let source_oid = source_tree_oid(root);
    pretty(&serde_json::json!({
        "contract_version": 1,
        "generator": "eg-capabilities/gen_contract",
        "contract_digest": contract_digest(&source_oid, &digests),
        "feature_profile": feature_profile(),
        "source_tree_oid": source_oid,
        "cargo_lock_sha256": sha256_hex(&lock),
        "method_count": crate::method_descriptors().count(),
        "result_classification": result_classification(catalog),
        "python_client_methods": python,
        "internal_only_methods": internal,
        "format_identities": identities,
        "artifact_digests": digests,
    }))
}

/// Trim per-line trailing whitespace and end with exactly one newline, so the
/// `trailing-whitespace`/`end-of-file-fixer` hooks and `--check` can never fight over
/// the same generated file (the precedent `gen_ledger` already set for the ledger).
pub(crate) fn normalize(text: String) -> Vec<u8> {
    let mut out: String = text
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    while out.ends_with('\n') {
        out.pop();
    }
    out.push('\n');
    out.into_bytes()
}

/// Every generated artifact except the receipt, which digests them.
fn body_artifacts(catalog: &Catalog) -> Vec<Artifact> {
    let mut out = vec![
        Artifact {
            path: "contract/methods.json".to_string(),
            bytes: methods_json(catalog),
        },
        Artifact {
            path: "docs/capabilities.generated.md".to_string(),
            bytes: normalize(crate::gen_ledger()),
        },
    ];
    out.push(Artifact {
        path: "crates/eg-capabilities/generated/method_catalog.rs".to_string(),
        bytes: schema::method_catalog_source(),
    });
    out.extend(schema::artifacts(catalog));
    out.extend(python::artifacts(catalog));
    out.extend(vectors::artifacts());
    out
}

/// Render every committed contract artifact, receipt last.
///
/// The receipt is written TWICE, byte-identically: once at the repo root and once inside
/// the published package as `epistemic_graph/contract/receipt.json`, which is the only
/// copy a `pip install`ed consumer can read. Neither copy appears in `artifact_digests`
/// (a digest cannot contain itself); `--check` compares both against the tree, so the
/// shipped copy can never drift from the root one.
pub fn render_all(root: &Path) -> Vec<Artifact> {
    let catalog = Catalog::collect();
    let mut artifacts = body_artifacts(&catalog);
    let receipt = receipt_json(root, &artifacts, &catalog);
    let digest = receipt_contract_digest(&receipt);
    artifacts.push(Artifact {
        path: "contract/receipt.json".to_string(),
        bytes: receipt.clone(),
    });
    artifacts.push(Artifact {
        path: "epistemic_graph/contract/receipt.json".to_string(),
        bytes: receipt,
    });
    // Written LAST and outside `artifact_digests`, exactly like the receipt it
    // copies, because a digest of every generated artifact cannot itself live
    // inside one of them and still have a fixpoint. `check` compares it like any
    // other artifact, so it cannot drift from the receipt.
    artifacts.push(Artifact {
        path: "crates/eg-capabilities/generated/catalog_digest.rs".to_string(),
        bytes: catalog_digest_source(&digest),
    });
    artifacts
}

/// The `contract_digest` field of a rendered receipt.
fn receipt_contract_digest(receipt: &[u8]) -> String {
    let text = String::from_utf8_lossy(receipt);
    text.split_once("\"contract_digest\": \"")
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(digest, _)| digest.to_string())
        .expect("a rendered receipt declares its contract digest")
}

/// `crates/eg-capabilities/generated/catalog_digest.rs`.
fn catalog_digest_source(digest: &str) -> Vec<u8> {
    normalize(
        [
            "// @generated by `cargo run -p eg-capabilities --features contract --bin gen_contract`.\n",
            "// DO NOT EDIT: `gen_contract --check` byte-diffs this file.\n",
            "//\n",
            "// The engine contract's one pinned digest, compiled in so the admission path\n",
            "// can bind it into every replay identity without reading `contract/receipt.json`\n",
            "// at run time -- a deployment-editable file cannot be an identity input.\n",
            "\n",
            "/// `contract/receipt.json`'s `contract_digest`.\n",
            "pub const CONTRACT_CATALOG_DIGEST: &str = \"",
        ]
        .concat()
            + digest
            + "\";\n",
    )
}

/// Write every artifact to disk, creating parent directories.
pub fn write_all(root: &Path) -> std::io::Result<usize> {
    let artifacts = render_all(root);
    for artifact in &artifacts {
        let path = root.join(&artifact.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &artifact.bytes)?;
    }
    Ok(artifacts.len())
}

/// Directories whose every file is a generated artifact, so a file there that the
/// generator no longer renders is drift -- a retired schema must not linger as if current.
const GENERATED_DIRS: &[&str] = &[
    "contract/fixtures",
    "contract/schemas",
    "epistemic_graph/generated",
];

/// Files under [`GENERATED_DIRS`] that no artifact renders.
fn orphaned_files(root: &Path, artifacts: &[Artifact]) -> Vec<String> {
    let rendered: std::collections::BTreeSet<&str> =
        artifacts.iter().map(|a| a.path.as_str()).collect();
    let mut files = Vec::new();
    for dir in GENERATED_DIRS {
        collect_files(&root.join(dir), &mut files);
    }
    let mut orphans: Vec<String> = files
        .iter()
        .filter_map(|path| path.strip_prefix(root).ok())
        .map(|path| path.to_string_lossy().to_string())
        .filter(|path| !path.contains("__pycache__") && !rendered.contains(path.as_str()))
        .collect();
    orphans.sort();
    orphans
}

/// Describe one artifact's drift by CONTENT, not length: two byte strings of equal
/// length are exactly the case a length-only message cannot distinguish from "identical"
/// (a digest change at constant length, e.g. `catalog_digest.rs`, is the whole EH-266/
/// EH-324 failure mode this program has now been misled by twice). Report both digests
/// and the first byte at which the two disagree, so a reader who sees matching lengths
/// still sees the files are different and roughly where.
fn describe_drift(path: &str, committed: &[u8], generated: &[u8]) -> String {
    let first_diff = committed
        .iter()
        .zip(generated.iter())
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| committed.len().min(generated.len()));
    format!(
        "{path}: committed {} bytes, sha256 {} -- generated {} bytes, sha256 {} -- first differing byte at offset {first_diff}",
        committed.len(),
        sha256_hex(committed),
        generated.len(),
        sha256_hex(generated),
    )
}

/// Byte-diff every artifact against the committed tree. `Ok(())` means no drift.
pub fn check(root: &Path) -> Result<usize, Vec<String>> {
    let artifacts = render_all(root);
    let mut drift: Vec<String> = orphaned_files(root, &artifacts)
        .into_iter()
        .map(|path| format!("{path}: no longer generated -- delete it"))
        .collect();
    for artifact in &artifacts {
        let committed = std::fs::read(root.join(&artifact.path)).unwrap_or_default();
        if committed != artifact.bytes {
            drift.push(describe_drift(&artifact.path, &committed, &artifact.bytes));
        }
    }
    if drift.is_empty() {
        Ok(artifacts.len())
    } else {
        Err(drift)
    }
}

#[cfg(test)]
mod drift_message_tests {
    use super::describe_drift;

    /// A gate you add must catch a known-bad input (BUILD-CONTRACT §3): feed
    /// `describe_drift` two equal-length, unequal-content byte strings -- exactly the
    /// EH-324 shape ("15945 vs 15945") -- and prove the message no longer reads as if
    /// nothing differs.
    #[test]
    fn equal_length_unequal_content_is_distinguishable() {
        let committed = b"pub const CONTRACT_CATALOG_DIGEST: &str = \"aaaa\";\n";
        let generated = b"pub const CONTRACT_CATALOG_DIGEST: &str = \"bbbb\";\n";
        assert_eq!(committed.len(), generated.len());
        let message = describe_drift("crates/.../catalog_digest.rs", committed, generated);
        assert!(
            message.contains("sha256"),
            "message must carry a content digest, not just a length: {message}"
        );
        let digest_committed = super::sha256_hex(committed);
        let digest_generated = super::sha256_hex(generated);
        assert_ne!(digest_committed, digest_generated);
        assert!(message.contains(&digest_committed));
        assert!(message.contains(&digest_generated));
        assert!(message.contains("first differing byte at offset"));
    }
}
