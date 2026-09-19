//! The engine contract is a BIJECTION with the `Method` enum (RF-RULING-003).
//!
//! `tests/invariants.rs` can only check the registry against itself: a real `Method`
//! value cannot be constructed generically for every variant (most carry required,
//! non-`Default` fields), so no runtime enumeration exists. This file closes the loop
//! the way the deleted `tests/test_protocol_parity.py` did — by reading the wire truth,
//! `crates/eg-types/src/protocol.rs`, as text — but with NO baseline file: a variant
//! without a descriptor, or a descriptor without a variant, is a hard failure.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// Names enforced ABSENT from the wire: they were retired, and re-adding one silently
/// would restore a surface an earlier decision removed.
const RETIRED_METHODS: &[&str] = &[
    "BatchCosineSimilarity",
    "SpectralCluster",
    "HypergraphEncodeInteraction",
    "FindSimilarPairs",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/eg-capabilities is two levels below the repo root")
        .to_path_buf()
}

/// The chunk files that build `pub enum Method { .. }`, discovered from
/// `crates/eg-types/src/protocol/method/mod.rs` rather than hardcoded.
///
/// `crates/eg-types/src/protocol.rs` is now a facade (`mod method;`): the enum is
/// assembled by a `__eg_method_chunk_0..N` macro chain, each chunk file appending its
/// own variants to an `@acc` token-tree accumulator before handing off to the next, with
/// `__eg_method_finish` wrapping the final accumulator in `pub enum Method { $($variants)* }`
/// (`crates/eg-types/src/protocol/method/method_finish.rs`). That means the literal text
/// `pub enum Method {` still exists, but its body there is only the macro placeholder
/// `$($variants)*` -- reading just that file finds the wrapper and none of the variants.
/// Discovering the chunk list from `mod.rs`'s own `mod method_NN;` declarations (instead
/// of hardcoding `method_00..method_10`) means a future chunk added to the chain is
/// picked up automatically rather than silently skipped.
fn method_chunk_files() -> Vec<PathBuf> {
    let dir = repo_root().join("crates/eg-types/src/protocol/method");
    let mod_path = dir.join("mod.rs");
    let text = std::fs::read_to_string(&mod_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", mod_path.display()));
    let files: Vec<PathBuf> = text
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("mod ")?.strip_suffix(';'))
        .filter(|name| {
            name.strip_prefix("method_").is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
            })
        })
        .map(|name| dir.join(format!("{name}.rs")))
        .collect();
    assert!(
        !files.is_empty(),
        "could not locate any `mod method_<N>;` declaration in {} -- the Method enum's \
         macro chain moved again and this gate needs to follow it there",
        mod_path.display()
    );
    files
}

/// Top-level variant identifiers of `pub enum Method { .. }`, walked across its chunk
/// files (see [`method_chunk_files`]).
///
/// Within each chunk file, variants sit at exactly 4-space indent; struct fields are
/// deeper, and attributes, doc comments and the macro's own syntax (`macro_rules!`,
/// `(@acc [$($variants:tt)*])`, the `$($variants)*` re-embed of earlier chunks) start
/// with something other than an uppercase ASCII letter, so the same column-based filter
/// that used to read the literal enum body reads a chunk body just as well.
fn wire_method_variants() -> BTreeSet<String> {
    method_chunk_files()
        .iter()
        .flat_map(|path| chunk_file_variants(path))
        .collect()
}

/// The `Method` variants one chunk file contributes (see [`wire_method_variants`]).
///
/// Fails loudly, naming the file, on either way this can go silently wrong: the file no
/// longer defining a chunk macro at all (the chain restructured again), or defining one
/// that -- at exactly 4-space indent, uppercase-starting -- contributes zero variants
/// (the accumulator's layout changed under this filter).
fn chunk_file_variants(path: &std::path::Path) -> BTreeSet<String> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    assert!(
        text.contains("macro_rules! __eg_method_chunk_"),
        "{} no longer defines an `__eg_method_chunk_*` macro -- the Method enum's macro \
         chain moved again and this gate needs to follow it there",
        path.display()
    );
    let found: BTreeSet<String> = text
        .lines()
        .filter_map(variant_name_at_enum_indent)
        .collect();
    assert!(
        !found.is_empty(),
        "{} contributed no Method variants -- confirm its variant lines are still at \
         4-space indent",
        path.display()
    );
    found
}

/// A variant name if `line` is a top-level variant row (exactly 4-space indent,
/// uppercase-starting) -- `None` for struct fields (deeper), attributes, doc comments,
/// and the macro's own syntax (`macro_rules!`, `(@acc [$($variants:tt)*])`, the
/// `$($variants)*` re-embed of earlier chunks), none of which starts this way.
fn variant_name_at_enum_indent(line: &str) -> Option<String> {
    let rest = line.strip_prefix("    ")?;
    if !rest.starts_with(|c: char| c.is_ascii_uppercase()) {
        return None;
    }
    Some(
        rest.chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect(),
    )
}

fn descriptor_ids() -> Vec<String> {
    eg_capabilities::method_descriptors()
        .map(|d| d.id.as_str().to_string())
        .collect()
}

/// The bijection can only hold under the CANONICAL profile, and that is a property of the
/// two sides, not a convenience: the text scan is cfg-BLIND (it sees all 413 variants,
/// 139 of them behind a `#[cfg(feature = ...)]`) while `method_descriptors()` is
/// cfg-CONDITIONAL (7 rows behind `jobs`/`statechart`/`knowledge-batch`/
/// `modality-serving`/`quantum`/`asr-native`/`viz`). `canonical-ledger` selects exactly
/// those 7 and eg-capabilities force-enables every other Method-gating `eg-types`
/// feature, so the two sides can agree. A variant added behind a NEW feature that
/// `canonical-ledger` does not select fails this test loudly, which is the correct
/// outcome -- the canonical ledger would no longer be the complete inventory.
#[cfg(feature = "canonical-ledger")]
#[test]
fn every_wire_variant_has_exactly_one_descriptor_and_vice_versa() {
    let ids = descriptor_ids();
    let unique: BTreeSet<String> = ids.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "the contract registry declares a duplicate method id"
    );
    let variants = wire_method_variants();
    let missing: Vec<_> = variants.difference(&unique).collect();
    let extra: Vec<_> = unique.difference(&variants).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "contract registry ⇄ `Method` enum are not a bijection\n\
         missing descriptors ({}): {missing:?}\n\
         descriptors with no wire variant ({}): {extra:?}",
        missing.len(),
        extra.len(),
    );
    // 410 -> 413: the three RF-ADR-008 agent-hierarchy variants beyond
    // `AgentLibrary` -- `AgentGraph` and `AgentComponent` (landed 2026-09-10
    // without this count being raised) and `AgentTemplate` (item C). This is
    // the same census `scripts/method_policy_inventory.py` counts from the
    // descriptor side; the two must agree.
    // 413 -> 414: RF-019's `SemanticIndex`, the S1-S6 tiered semantic ingestion
    // queue -- the first wire surface over the semantic stage queue rather than
    // over semantic CONTENT.
    // 414 -> 415: `SqlSourceBatch` (SQL source ingestion through the native SQL
    // owner).
    // 415 -> 425: the 2.27.x contract wave's ten new methods -- `Decide`,
    // `ConnectorPack`, `GraphSchema`, `GraphSchemaList`, `MutationOutbox`,
    // `Solve`, `AgentAssemble`, `DecisionCommit`, `DecisionEval`, `DecisionFit`.
    assert_eq!(
        variants.len(),
        425,
        "the wire method census changed; update this exact count deliberately"
    );
}

#[test]
fn retired_methods_never_reappear() {
    let variants = wire_method_variants();
    let ids: BTreeSet<String> = descriptor_ids().into_iter().collect();
    for retired in RETIRED_METHODS {
        assert!(
            !variants.contains(*retired),
            "{retired} was retired from the wire but is back in `Method`"
        );
        assert!(
            !ids.contains(*retired),
            "{retired} was retired from the wire but is back in the contract registry"
        );
    }
}

#[test]
fn every_descriptor_names_a_domain_and_an_authz_scope() {
    for descriptor in eg_capabilities::method_descriptors() {
        let id = descriptor.id.as_str();
        assert!(
            !descriptor.domain.is_empty(),
            "{id}: descriptor has no domain"
        );
        assert!(
            descriptor.policy.authz_action.contains(':'),
            "{id}: authz_action must be `primitive:verb`"
        );
        assert!(
            !descriptor.error_set.is_empty(),
            "{id}: every method declares at least one error code"
        );
    }
}

#[test]
fn internal_methods_declare_no_consumer_and_stable_methods_declare_one() {
    for descriptor in eg_capabilities::method_descriptors() {
        let id = descriptor.id.as_str();
        match descriptor.stability {
            eg_capabilities::Stability::Internal => assert!(
                descriptor.consumer_profiles.is_empty(),
                "{id}: an internal method must generate no consumer surface"
            ),
            _ => assert!(
                !descriptor.consumer_profiles.is_empty(),
                "{id}: a published method must name at least one consumer profile"
            ),
        }
    }
}
