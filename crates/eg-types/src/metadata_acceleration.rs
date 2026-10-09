//! Typed model for metadata materialization and selective acceleration
//! (EG-UNIFIED-DATA-PLANE-R038): the versioned, hashed `MetadataContract`
//! every registered source kind's discovery writes (metadata only, never
//! copied entity rows), and the `AccelerationPolicy` that alone decides
//! whether a mapping's query fragment may run from a copy instead of live.
//! This is the typed-model slice (`.1`): the models plus the refusal of a
//! policy that declares a hot-subset predicate for a source with no `copy`
//! capability, and `AccelerationPolicy::resolve_mode`, which falls back to
//! `Live` whenever the copy capability, the predicate, or the freshness
//! watermark is missing — a missing watermark disables the copied route
//! rather than silently serving stale rows. Real discovery, the copy
//! pipeline, and EXPLAIN's per-fragment live/copy reporting are later
//! children.

use serde::{Deserialize, Serialize};

/// A versioned, hashed metadata-discovery result for one registered
/// source. Carries no entity row data — only the discovered shape and its
/// content hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataContract {
    pub source_kind: String,
    pub source_name: String,
    pub schema_version: u32,
    pub content_hash: String,
}

/// Whether a mapping's query fragment is permitted to run from a copy of
/// its hot subset, or must run live against the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FragmentExecutionMode {
    Live,
    Copy,
}

/// The acceleration policy for one mapping: whether its source declares the
/// `copy` capability, the hot-subset predicate it would copy, and the
/// freshness watermark that bounds how stale a copy may be.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccelerationPolicy {
    pub source_supports_copy: bool,
    pub hot_subset_predicate: String,
    pub freshness_watermark: Option<String>,
}

impl AccelerationPolicy {
    /// Resolve how a fragment under this policy must run: `Copy` only when
    /// the source supports copying, a hot-subset predicate is declared, and
    /// a freshness watermark is present; `Live` otherwise. A missing
    /// watermark disables the copied route — it never silently serves a
    /// copy of unknown freshness.
    pub fn resolve_mode(&self) -> FragmentExecutionMode {
        if self.source_supports_copy
            && !self.hot_subset_predicate.is_empty()
            && self.freshness_watermark.is_some()
        {
            FragmentExecutionMode::Copy
        } else {
            FragmentExecutionMode::Live
        }
    }
}

/// A `MetadataContract` or `AccelerationPolicy` failed validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidMetadataContract(pub String);

impl std::fmt::Display for InvalidMetadataContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid metadata contract: {}", self.0)
    }
}

impl std::error::Error for InvalidMetadataContract {}

/// Confirm a `MetadataContract` names both its source kind and source name.
pub fn validate_contract(contract: &MetadataContract) -> Result<(), InvalidMetadataContract> {
    if contract.source_kind.is_empty() || contract.source_name.is_empty() {
        return Err(InvalidMetadataContract(
            "source_kind and source_name must both be non-empty".to_string(),
        ));
    }
    Ok(())
}

/// Confirm an `AccelerationPolicy` is internally consistent: a hot-subset
/// predicate may only be declared for a source that actually supports
/// copying. Refuses rather than admitting a policy that claims
/// acceleration the source cannot provide.
pub fn validate_policy(policy: &AccelerationPolicy) -> Result<(), InvalidMetadataContract> {
    if !policy.hot_subset_predicate.is_empty() && !policy.source_supports_copy {
        return Err(InvalidMetadataContract(
            "hot-subset predicate declared but source has no copy capability".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract() -> MetadataContract {
        MetadataContract {
            source_kind: "postgres".to_string(),
            source_name: "app-primary".to_string(),
            schema_version: 1,
            content_hash: "deadbeef".to_string(),
        }
    }

    fn policy(copy: bool, predicate: &str, watermark: Option<&str>) -> AccelerationPolicy {
        AccelerationPolicy {
            source_supports_copy: copy,
            hot_subset_predicate: predicate.to_string(),
            freshness_watermark: watermark.map(str::to_string),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn well_formed_contract_validates() {
        assert_eq!(validate_contract(&contract()), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn contract_with_empty_source_kind_is_refused() {
        let mut bad = contract();
        bad.source_kind = String::new();
        assert!(validate_contract(&bad).is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn fully_declared_policy_resolves_to_copy() {
        let p = policy(
            true,
            "updated_at > now() - interval '1 day'",
            Some("lsn:42"),
        );
        assert!(validate_policy(&p).is_ok());
        assert_eq!(p.resolve_mode(), FragmentExecutionMode::Copy);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn missing_watermark_falls_back_to_live() {
        let p = policy(true, "updated_at > now() - interval '1 day'", None);
        assert_eq!(p.resolve_mode(), FragmentExecutionMode::Live);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn cold_mapping_with_no_predicate_is_live() {
        let p = policy(true, "", None);
        assert_eq!(p.resolve_mode(), FragmentExecutionMode::Live);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn predicate_without_copy_capability_is_refused() {
        let p = policy(
            false,
            "updated_at > now() - interval '1 day'",
            Some("lsn:42"),
        );
        let Err(error) = validate_policy(&p) else {
            panic!("a predicate declared without copy capability must be refused");
        };
        assert!(error.0.contains("copy capability"), "{error}");
    }
}
