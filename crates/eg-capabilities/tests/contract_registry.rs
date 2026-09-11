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

/// Top-level variant identifiers of `pub enum Method { .. }`.
///
/// Variants sit at exactly 4-space indent; struct fields are deeper and attributes or
/// doc comments start with `#` or `/`.
fn wire_method_variants() -> BTreeSet<String> {
    let path = repo_root().join("crates/eg-types/src/protocol.rs");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let start = text
        .find("pub enum Method {")
        .expect("could not locate `pub enum Method {` in protocol.rs")
        + "pub enum Method {".len();
    // The item ends at the first line that is exactly `}` in column 0. Counting braces
    // instead would run through doc comments and string literals: a single `}` inside a
    // doc comment on a variant would truncate the scan and silently drop every variant
    // after it, and only the hardcoded census below would notice.
    let body = text[start..]
        .split_once("\n}\n")
        .map(|(body, _)| body)
        .unwrap_or_else(|| {
            panic!("`pub enum Method` is not terminated by a column-0 `}}` in protocol.rs")
        });
    body.lines()
        .filter_map(|line| line.strip_prefix("    "))
        .filter(|rest| rest.starts_with(|c: char| c.is_ascii_uppercase()))
        .map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect::<String>()
        })
        .collect()
}

fn descriptor_ids() -> Vec<String> {
    eg_capabilities::method_descriptors()
        .map(|d| d.id.as_str().to_string())
        .collect()
}

/// The bijection can only hold under the CANONICAL profile, and that is a property of the
/// two sides, not a convenience: the text scan is cfg-BLIND (it sees all 410 variants,
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
    assert_eq!(
        variants.len(),
        410,
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
        assert_eq!(
            descriptor.request_schema,
            eg_capabilities::SchemaRef::MethodVariant,
            "{id}: a request schema is always the method's own variant subschema"
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
