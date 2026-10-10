//! Typed model for a shared-platform restore-drill result (EG-UNIFIED-DATA-
//! PLANE-R023.3): the typed record of one independent restore-drill run and
//! its refusal path. This is the first slice (`.1`): `RestoreDrillResult`/
//! `validate` refuse a result whose `data_matched` flag disagrees with
//! whether its pre- and post-restore digests are actually equal -- a drill
//! can never be marked matched (or mismatched) by anything other than the
//! actual digest comparison. No restore is actually run yet, and no live
//! cluster or object-storage backup is read; automating the real restore
//! and taking the real pre/post digests is a later child.

use serde::{Deserialize, Serialize};

use crate::shared_db_platform::SharedPlatformEngine;

/// One independent restore-drill run: a cluster group's PITR backup is
/// recovered into a scratch cluster and its data is digested before and
/// after, to confirm the drill is independent of the source cluster still
/// being up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreDrillResult {
    pub engine: SharedPlatformEngine,
    pub cluster_group: String,
    /// A content digest of the pre-restore cluster's data (the state the
    /// backup is expected to recover).
    pub pre_restore_digest: String,
    /// A content digest of the same data read back from the independently
    /// restored scratch cluster.
    pub post_restore_digest: String,
    /// Whether the drill is reported as a data match. Always derived from
    /// comparing the two digests -- never hand-set independently of them.
    pub data_matched: bool,
}

/// A restore-drill result failed a platform invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidRestoreDrillResult {
    /// `data_matched` was `true` while the digests actually differ.
    MatchedDespiteDifferingDigests,
    /// `data_matched` was `false` while the digests actually agree -- a
    /// drill never reports a spurious mismatch either.
    MismatchedDespiteEqualDigests,
    /// Either digest was blank -- a drill that never actually took a digest
    /// cannot report a match or mismatch.
    BlankDigest,
}

impl std::fmt::Display for InvalidRestoreDrillResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::MatchedDespiteDifferingDigests => {
                "data_matched=true but pre/post-restore digests differ"
            }
            Self::MismatchedDespiteEqualDigests => {
                "data_matched=false but pre/post-restore digests are equal"
            }
            Self::BlankDigest => "pre- or post-restore digest is blank",
        };
        write!(f, "{message}")
    }
}

impl std::error::Error for InvalidRestoreDrillResult {}

impl RestoreDrillResult {
    /// Confirm `data_matched` actually agrees with comparing the two
    /// digests, and that neither digest is blank. Refuses rather than
    /// trusting a hand-set (or buggy) `data_matched` flag.
    pub fn validate(&self) -> Result<(), InvalidRestoreDrillResult> {
        if self.pre_restore_digest.trim().is_empty() || self.post_restore_digest.trim().is_empty() {
            return Err(InvalidRestoreDrillResult::BlankDigest);
        }
        let digests_equal = self.pre_restore_digest == self.post_restore_digest;
        if self.data_matched && !digests_equal {
            return Err(InvalidRestoreDrillResult::MatchedDespiteDifferingDigests);
        }
        if !self.data_matched && digests_equal {
            return Err(InvalidRestoreDrillResult::MismatchedDespiteEqualDigests);
        }
        Ok(())
    }

    /// Build a result with `data_matched` always derived from comparing the
    /// two digests -- the only way this slice lets a caller construct one,
    /// so a drill can never set the flag independently of the comparison.
    pub fn from_digests(
        engine: SharedPlatformEngine,
        cluster_group: impl Into<String>,
        pre_restore_digest: impl Into<String>,
        post_restore_digest: impl Into<String>,
    ) -> Self {
        let pre_restore_digest = pre_restore_digest.into();
        let post_restore_digest = post_restore_digest.into();
        let data_matched = pre_restore_digest == post_restore_digest;
        Self {
            engine,
            cluster_group: cluster_group.into(),
            pre_restore_digest,
            post_restore_digest,
            data_matched,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R023.3.1
    #[test]
    fn matching_digests_built_via_from_digests_validate() {
        let result = RestoreDrillResult::from_digests(
            SharedPlatformEngine::CloudNativePg,
            "gramps",
            "abc123",
            "abc123",
        );
        assert!(result.data_matched);
        assert_eq!(result.validate(), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.3.1
    #[test]
    fn differing_digests_built_via_from_digests_validate_as_mismatch() {
        let result = RestoreDrillResult::from_digests(
            SharedPlatformEngine::MariaDb,
            "twenty",
            "abc123",
            "def456",
        );
        assert!(!result.data_matched);
        assert_eq!(result.validate(), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.3.1
    #[test]
    fn matched_flag_despite_differing_digests_is_refused() {
        let result = RestoreDrillResult {
            engine: SharedPlatformEngine::CloudNativePg,
            cluster_group: "gramps".to_string(),
            pre_restore_digest: "abc123".to_string(),
            post_restore_digest: "def456".to_string(),
            data_matched: true,
        };
        assert_eq!(
            result.validate(),
            Err(InvalidRestoreDrillResult::MatchedDespiteDifferingDigests)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.3.1
    #[test]
    fn mismatched_flag_despite_equal_digests_is_refused() {
        let result = RestoreDrillResult {
            engine: SharedPlatformEngine::CloudNativePg,
            cluster_group: "gramps".to_string(),
            pre_restore_digest: "abc123".to_string(),
            post_restore_digest: "abc123".to_string(),
            data_matched: false,
        };
        assert_eq!(
            result.validate(),
            Err(InvalidRestoreDrillResult::MismatchedDespiteEqualDigests)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.3.1
    #[test]
    fn blank_digest_is_refused() {
        let result = RestoreDrillResult {
            engine: SharedPlatformEngine::CloudNativePg,
            cluster_group: "gramps".to_string(),
            pre_restore_digest: String::new(),
            post_restore_digest: "abc123".to_string(),
            data_matched: false,
        };
        assert_eq!(
            result.validate(),
            Err(InvalidRestoreDrillResult::BlankDigest)
        );
    }
}
