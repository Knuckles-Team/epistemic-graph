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

mod errors;
mod format_identity;
mod method_bodies;
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

/// The HAND-WRITTEN client surfaces consumers depend on, digested into
/// `receipt.json.source_tree_oid`.
///
/// `epistemic_graph/client.py` is the transport every one of agent-utilities'
/// import lines consumes (`SyncEpistemicGraphClient`, framing, auth, pooling), and
/// `connector_pack.py` implements the Rust-owned ConnectorPack framed identity and
/// replay key the SDK consumes. Neither is generated, so nothing else would bind
/// them, and a transport-breaking edit must move the digest AU pins.
///
/// The Rust sources the contract is generated FROM are deliberately not listed:
/// every effect they have on the contract shows up in the generated half --
/// `contract/**` and `epistemic_graph/generated/**` -- which `artifact_digests`
/// binds file by file. Hashing their raw bytes (or `pyproject.toml`, or
/// `Cargo.lock`) only made comment edits and dependency bumps look like
/// contract changes.
const HAND_WRITTEN_INPUTS: &[&str] = &[
    "epistemic_graph/client.py",
    "epistemic_graph/connector_pack.py",
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
        ("decide", cfg!(feature = "decide")),
    ]
    .into_iter()
    .filter(|(_, on)| *on)
    .map(|(name, _)| name)
    .collect()
}

/// A build-selected feature this freeze must name but does not (EG-DECISION-ENGINE-R078).
///
/// `feature_profile` is a hand-maintained enumeration: a feature can ship without ever
/// being added to it, so a reissued freeze silently drops it. This type makes that gap
/// a typed, testable refusal instead of a manual review step.
#[derive(Debug, PartialEq, Eq)]
pub struct MissingFrozenFeature(pub &'static str);

impl std::fmt::Display for MissingFrozenFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "frozen feature profile omits enabled feature {:?}", self.0)
    }
}

/// Refuse a frozen profile that omits a feature the current build enabled.
fn require_feature_frozen(
    profile: &[&'static str],
    name: &'static str,
    enabled: bool,
) -> Result<(), MissingFrozenFeature> {
    if enabled && !profile.contains(&name) {
        return Err(MissingFrozenFeature(name));
    }
    Ok(())
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
    // The source registry declares whether a method is published to a served
    // consumer. check_contract_method_reachability.py independently proves
    // every published row reaches a Response-producing dispatch arm and is
    // not a refusal-only stub; release qualification must run that gate.
    let is_wire_callable = d.stability != Stability::Internal && !d.consumer_profiles.is_empty();
    serde_json::json!({
        "id": id,
        "domain": d.domain,
        "is_wire_callable": is_wire_callable,
        "request_schema": {
            "kind": "named",
            "schema": format!("contract/schemas/method.request.json#/methods/{id}"),
        },
        "result_schema": result_schema_json(d, catalog),
        "error_set": errors::method_error_set(d),
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
            path: "contract/errors.json".to_string(),
            bytes: errors::catalog_json(),
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
    out.extend(method_bodies::artifacts());
    let scopes = scopes_json();
    out.push(Artifact {
        path: "contract/scopes.json".to_string(),
        bytes: scopes.clone(),
    });
    // The packaged copy is what agent-utilities generates its session-scope
    // allowlist from; both are digested, so neither can drift.
    out.push(Artifact {
        path: "epistemic_graph/contract/scopes.json".to_string(),
        bytes: scopes,
    });
    out.extend(package_contract_artifacts(&out));
    out
}

/// Mirror the canonical discovery surface without rewriting schema references or
/// method policy. Consumers resolve `contract/schemas/...` within the package.
/// Scopes are mirrored separately in `body_artifacts` from the authoritative
/// `crate::scopes` registry.
fn package_contract_artifacts(artifacts: &[Artifact]) -> Vec<Artifact> {
    artifacts
        .iter()
        .filter(|artifact| {
            matches!(
                artifact.path.as_str(),
                "contract/methods.json" | "contract/errors.json"
            ) || artifact.path.starts_with("contract/schemas/")
        })
        .map(|artifact| Artifact {
            path: format!("epistemic_graph/{}", artifact.path),
            bytes: artifact.bytes.clone(),
        })
        .collect()
}

/// `contract/scopes.json` -- the scope registry, sorted by scope.
fn scopes_json() -> Vec<u8> {
    let scopes: Vec<serde_json::Value> = crate::scopes::SCOPES
        .iter()
        .map(|entry| {
            let approver_group = crate::scopes::APPROVER_GROUPS
                .iter()
                .find(|(scope, _)| *scope == entry.scope)
                .map(|(_, group)| *group);
            serde_json::json!({
                "scope": entry.scope,
                "class": crate::scopes::class_name(entry.class),
                "owner": entry.owner,
                "approver_group": approver_group,
            })
        })
        .collect();
    pretty(&serde_json::json!({
        "registry_version": 1,
        "generator": "eg-capabilities/gen_contract",
        "scopes": scopes,
    }))
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
            "pub const CONTRACT_CATALOG_DIGEST: &str =\n    \"",
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
    "epistemic_graph/contract/schemas",
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
mod decide_feature_freeze_tests {
    use super::*;

    #[test]
    fn refuses_a_profile_missing_an_enabled_feature() {
        let err = require_feature_frozen(&["contract", "jobs"], "decide", true).unwrap_err();
        assert_eq!(err, MissingFrozenFeature("decide"));
        assert!(err.to_string().contains("decide"));
    }

    #[test]
    fn allows_a_profile_missing_a_disabled_feature() {
        assert!(require_feature_frozen(&["contract", "jobs"], "decide", false).is_ok());
    }

    #[test]
    fn allows_a_profile_already_naming_an_enabled_feature() {
        assert!(require_feature_frozen(&["contract", "decide"], "decide", true).is_ok());
    }

    /// Wiring proof for R078: this build's own `feature_profile()` must satisfy the
    /// same refusal when `decide` is compiled in, not just the synthetic cases above.
    #[cfg(feature = "decide")]
    #[test]
    fn compiled_decide_feature_is_reflected_in_the_frozen_profile() {
        require_feature_frozen(&feature_profile(), "decide", true)
            .expect("decide is compiled in but feature_profile() omits it");
    }
}

#[cfg(test)]
mod package_projection_tests {
    use super::*;

    #[test]
    fn package_surface_is_complete_identical_and_receipt_bound() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let artifacts = render_all(&root);
        let by_path: BTreeMap<_, _> = artifacts
            .iter()
            .map(|artifact| (artifact.path.as_str(), artifact.bytes.as_slice()))
            .collect();
        assert_eq!(by_path.len(), artifacts.len(), "duplicate artifact path");
        let receipt: serde_json::Value =
            serde_json::from_slice(by_path["contract/receipt.json"]).unwrap();
        let mut expected = vec!["contract/methods.json", "contract/errors.json"];
        expected.extend(
            by_path
                .keys()
                .copied()
                .filter(|path| path.starts_with("contract/schemas/")),
        );
        assert!(expected.len() > 2, "schema projection cannot be empty");
        for path in expected {
            let packaged = format!("epistemic_graph/{path}");
            assert_eq!(by_path[path], by_path[packaged.as_str()], "{path}");
            for bound in [path, packaged.as_str()] {
                assert_eq!(
                    receipt["artifact_digests"][bound].as_str(),
                    Some(sha256_hex(by_path[bound]).as_str()),
                    "receipt must bind {bound}"
                );
            }
        }
        assert_eq!(
            by_path["contract/receipt.json"],
            by_path["epistemic_graph/contract/receipt.json"]
        );
    }

    #[test]
    fn projection_does_not_publish_unowned_contract_surfaces() {
        let artifacts: Vec<_> = [
            "contract/scopes.json",
            "contract/receipt.json",
            "contract/fixtures/example.json",
            "epistemic_graph/contract/methods.json",
            "docs/capabilities.generated.md",
        ]
        .into_iter()
        .map(|path| Artifact {
            path: path.to_string(),
            bytes: Vec::new(),
        })
        .collect();
        assert!(package_contract_artifacts(&artifacts).is_empty());
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
