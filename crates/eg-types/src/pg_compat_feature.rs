//! Typed model for the Postgres compatibility-gap inventory
//! (EG-UNIFIED-DATA-PLANE-R027): a feature is fixed only once captured
//! application traffic proves it is needed, with a test shown failing
//! before the fix and passing after. This is the typed-model slice (`.1`):
//! the closed feature inventory and the refusal to mark a feature fixed
//! without its full proof (captured traffic plus the failing/passing test
//! pair). Implementing each proven feature is later children.

use serde::{Deserialize, Serialize};

/// One Postgres compatibility feature in EG's tracked inventory. Closed —
/// per the spec's defined list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgCompatFeature {
    BTreeIndexes,
    Collation,
    Regex,
    Bytea,
    Sequences,
    Savepoints,
    Locks,
    Notify,
    TriggersAndFunctions,
    Jsonb,
    Isolation,
    Startup,
    SqlState,
    Schemas,
    Constraints,
    Types,
    MaterializedViews,
    LargeObjects,
    Timezone,
}

/// Proof that captured application traffic needs a feature, and that a test
/// demonstrates the gap before the fix and its closure after.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureFixProof {
    /// A reference (capture file, trace id) to the application traffic that
    /// proved the need.
    pub captured_traffic_ref: String,
    /// The committed test name/path, shown failing before the fix.
    pub failing_test_ref: String,
    /// The same test, shown passing after the fix.
    pub passing_test_ref: String,
}

/// One feature's fix record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureFixRecord {
    pub feature: PgCompatFeature,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof: Option<FeatureFixProof>,
}

/// A record claimed `fixed` without a complete, non-empty proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidFeatureFix {
    NoProof,
    IncompleteProof,
}

impl std::fmt::Display for InvalidFeatureFix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoProof => write!(f, "feature fix has no captured-traffic/test proof"),
            Self::IncompleteProof => write!(f, "feature fix proof has an empty field"),
        }
    }
}

impl std::error::Error for InvalidFeatureFix {}

impl FeatureFixRecord {
    /// Confirm this record is either unfixed (no proof, which is fine: it
    /// is simply not yet proven needed) or fixed with a complete proof.
    /// Refuses a proof with any empty field rather than accepting a
    /// half-recorded fix.
    pub fn validate(&self) -> Result<(), InvalidFeatureFix> {
        let Some(proof) = &self.proof else {
            return Ok(());
        };
        if proof.captured_traffic_ref.trim().is_empty()
            || proof.failing_test_ref.trim().is_empty()
            || proof.passing_test_ref.trim().is_empty()
        {
            return Err(InvalidFeatureFix::IncompleteProof);
        }
        Ok(())
    }

    /// Whether this feature is confirmed fixed: a complete, valid proof.
    pub fn is_fixed(&self) -> bool {
        self.proof.is_some() && self.validate().is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proof() -> FeatureFixProof {
        FeatureFixProof {
            captured_traffic_ref: "capture-0042".to_string(),
            failing_test_ref: "tests::jsonb::path_query_fails_before_fix".to_string(),
            passing_test_ref: "tests::jsonb::path_query_passes_after_fix".to_string(),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.1
    #[test]
    fn unfixed_feature_with_no_proof_validates() {
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Jsonb,
            proof: None,
        };
        assert_eq!(record.validate(), Ok(()));
        assert!(!record.is_fixed());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.1
    #[test]
    fn fixed_feature_with_complete_proof_validates() {
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Jsonb,
            proof: Some(proof()),
        };
        assert_eq!(record.validate(), Ok(()));
        assert!(record.is_fixed());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.1
    #[test]
    fn proof_with_empty_traffic_ref_is_refused() {
        let mut bad_proof = proof();
        bad_proof.captured_traffic_ref = String::new();
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Savepoints,
            proof: Some(bad_proof),
        };
        assert_eq!(record.validate(), Err(InvalidFeatureFix::IncompleteProof));
        assert!(!record.is_fixed());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.1
    #[test]
    fn proof_with_empty_failing_test_ref_is_refused() {
        let mut bad_proof = proof();
        bad_proof.failing_test_ref = "  ".to_string();
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Locks,
            proof: Some(bad_proof),
        };
        assert_eq!(record.validate(), Err(InvalidFeatureFix::IncompleteProof));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.1
    #[test]
    fn record_serializes_round_trip() {
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Timezone,
            proof: Some(proof()),
        };
        let encoded = serde_json::to_string(&record).unwrap();
        let decoded: FeatureFixRecord = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, record);
    }
}
