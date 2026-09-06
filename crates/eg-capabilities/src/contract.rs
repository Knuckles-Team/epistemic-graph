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

use crate::{ConsumerProfile, MethodDescriptor, SchemaRef, Stability};

mod format_identity;
mod python;
mod schema;

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

fn schema_ref_json(schema_ref: SchemaRef, id: &str) -> serde_json::Value {
    match schema_ref {
        SchemaRef::MethodVariant => serde_json::json!({
            "kind": "named",
            "schema": format!("contract/schemas/method.request.json#/methods/{id}"),
        }),
        SchemaRef::Payload(shape) => serde_json::json!({
            "kind": "named",
            "schema": format!("contract/schemas/result.{}.json", shape.as_str()),
        }),
        SchemaRef::Opaque(kind) => serde_json::json!({
            "kind": "opaque",
            "payload": kind.as_str(),
        }),
    }
}

fn descriptor_json(d: &MethodDescriptor) -> serde_json::Value {
    let id = d.id.as_str();
    serde_json::json!({
        "id": id,
        "domain": d.domain,
        "request_schema": schema_ref_json(d.request_schema, id),
        "result_schema": schema_ref_json(d.result_schema, id),
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
fn methods_json() -> Vec<u8> {
    let methods: Vec<_> = crate::method_descriptors()
        .map(|d| descriptor_json(&d))
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

fn result_census() -> (usize, usize) {
    let typed = crate::method_descriptors()
        .filter(|d| d.result_schema.is_typed())
        .count();
    (typed, crate::method_descriptors().count() - typed)
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
fn receipt_json(root: &Path, artifacts: &[Artifact]) -> Vec<u8> {
    let lock = std::fs::read(root.join("Cargo.lock")).unwrap_or_default();
    let (typed, opaque) = result_census();
    let (python, internal) = consumer_census();
    let digests: BTreeMap<&str, String> = artifacts
        .iter()
        .map(|a| (a.path.as_str(), sha256_hex(&a.bytes)))
        .collect();
    let identities: BTreeMap<String, serde_json::Value> = collect_format_identities(root)
        .into_iter()
        .map(|i| {
            (
                i.name.clone(),
                serde_json::json!({"value": i.value, "sites": i.sites}),
            )
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
        "typed_result_methods": typed,
        "opaque_result_methods": opaque,
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
fn body_artifacts(root: &Path) -> Vec<Artifact> {
    let mut out = vec![
        Artifact {
            path: "contract/methods.json".to_string(),
            bytes: methods_json(),
        },
        Artifact {
            path: "docs/capabilities.generated.md".to_string(),
            bytes: normalize(crate::gen_ledger()),
        },
    ];
    out.extend(schema::artifacts());
    out.extend(python::artifacts());
    let _ = root;
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
    let mut artifacts = body_artifacts(root);
    let receipt = receipt_json(root, &artifacts);
    artifacts.push(Artifact {
        path: "contract/receipt.json".to_string(),
        bytes: receipt.clone(),
    });
    artifacts.push(Artifact {
        path: "epistemic_graph/contract/receipt.json".to_string(),
        bytes: receipt,
    });
    artifacts
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

/// Byte-diff every artifact against the committed tree. `Ok(())` means no drift.
pub fn check(root: &Path) -> Result<usize, Vec<String>> {
    let artifacts = render_all(root);
    let mut drift = Vec::new();
    for artifact in &artifacts {
        let committed = std::fs::read(root.join(&artifact.path)).unwrap_or_default();
        if committed != artifact.bytes {
            drift.push(format!(
                "{}: committed {} bytes, generated {} bytes",
                artifact.path,
                committed.len(),
                artifact.bytes.len()
            ));
        }
    }
    if drift.is_empty() {
        Ok(artifacts.len())
    } else {
        Err(drift)
    }
}
