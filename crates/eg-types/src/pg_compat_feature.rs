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

/// A source of captured application traffic for one feature, pluggable so
/// the capture runner (`EG-UNIFIED-DATA-PLANE-R027.2.1`) needs no live
/// candidate application. Needs no live service: driven against a fake
/// capture source in tests.
pub trait TrafficCaptureSource {
    /// This feature's captured-traffic proof, if traffic captured so far
    /// proves the feature is needed. `None` means not yet proven.
    fn capture(&self, feature: PgCompatFeature) -> Option<FeatureFixProof>;
}

/// `capture_feature_fix` could not produce a validated, proven fix record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureProofError {
    /// The capture source has not yet observed traffic proving the need.
    NotProven,
    /// The source supplied a proof, but it is incomplete.
    Invalid(InvalidFeatureFix),
}

impl std::fmt::Display for CaptureProofError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotProven => write!(f, "capture source has not proven this feature needed"),
            Self::Invalid(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for CaptureProofError {}

/// Assemble one feature's `FeatureFixRecord` from a pluggable
/// `TrafficCaptureSource`, refusing to mark it fixed unless the source
/// supplies a complete, valid proof (`EG-UNIFIED-DATA-PLANE-R027.2.1`).
pub fn capture_feature_fix(
    source: &dyn TrafficCaptureSource,
    feature: PgCompatFeature,
) -> Result<FeatureFixRecord, CaptureProofError> {
    let proof = source
        .capture(feature)
        .ok_or(CaptureProofError::NotProven)?;
    let record = FeatureFixRecord {
        feature,
        proof: Some(proof),
    };
    record.validate().map_err(CaptureProofError::Invalid)?;
    Ok(record)
}

/// A fix was applied against a record not yet proven fixed -- a fix must
/// follow its captured-traffic proof, never precede it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixNotProven {
    pub feature: PgCompatFeature,
}

impl std::fmt::Display for FixNotProven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} has no proven fix to apply", self.feature)
    }
}

impl std::error::Error for FixNotProven {}

/// Apply a feature's fix only once its record carries a complete, valid
/// proof (`EG-UNIFIED-DATA-PLANE-R027.3.1`); refuses -- without invoking
/// `apply` -- to apply a fix the record does not yet prove needed. Needs no
/// live Postgres-compatible target: the caller's actual fix is injected as
/// `apply`, called only after the proof check passes.
pub fn apply_proven_fix<F: FnOnce(PgCompatFeature)>(
    record: &FeatureFixRecord,
    apply: F,
) -> Result<(), FixNotProven> {
    if !record.is_fixed() {
        return Err(FixNotProven {
            feature: record.feature,
        });
    }
    apply(record.feature);
    Ok(())
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

    #[test]
    fn unfixed_feature_with_no_proof_validates() {
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Jsonb,
            proof: None,
        };
        assert_eq!(record.validate(), Ok(()));
        assert!(!record.is_fixed());
    }

    #[test]
    fn fixed_feature_with_complete_proof_validates() {
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Jsonb,
            proof: Some(proof()),
        };
        assert_eq!(record.validate(), Ok(()));
        assert!(record.is_fixed());
    }

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

    struct FakeCaptureSource {
        proof: Option<FeatureFixProof>,
    }

    impl TrafficCaptureSource for FakeCaptureSource {
        fn capture(&self, _feature: PgCompatFeature) -> Option<FeatureFixProof> {
            self.proof.clone()
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.2.1
    #[test]
    fn capture_with_complete_proof_produces_a_valid_fixed_record() {
        let source = FakeCaptureSource {
            proof: Some(proof()),
        };
        let record = capture_feature_fix(&source, PgCompatFeature::Jsonb).unwrap();
        assert!(record.is_fixed());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.2.1
    #[test]
    fn capture_with_no_traffic_yet_is_not_proven() {
        let source = FakeCaptureSource { proof: None };
        let err = capture_feature_fix(&source, PgCompatFeature::Jsonb).unwrap_err();
        assert_eq!(err, CaptureProofError::NotProven);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.2.1
    #[test]
    fn capture_with_incomplete_proof_is_refused() {
        let mut bad_proof = proof();
        bad_proof.passing_test_ref = String::new();
        let source = FakeCaptureSource {
            proof: Some(bad_proof),
        };
        let err = capture_feature_fix(&source, PgCompatFeature::Jsonb).unwrap_err();
        assert_eq!(
            err,
            CaptureProofError::Invalid(InvalidFeatureFix::IncompleteProof)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.3.1
    #[test]
    fn apply_proven_fix_invokes_apply_exactly_once_when_proven() {
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Jsonb,
            proof: Some(proof()),
        };
        let mut calls = 0;
        apply_proven_fix(&record, |feature| {
            calls += 1;
            assert_eq!(feature, PgCompatFeature::Jsonb);
        })
        .unwrap();
        assert_eq!(calls, 1);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R027.3.1
    #[test]
    fn apply_proven_fix_refuses_an_unproven_record_without_calling_apply() {
        let record = FeatureFixRecord {
            feature: PgCompatFeature::Savepoints,
            proof: None,
        };
        let mut calls = 0;
        let err = apply_proven_fix(&record, |_| calls += 1).unwrap_err();
        assert_eq!(err.feature, PgCompatFeature::Savepoints);
        assert_eq!(calls, 0);
    }
}
