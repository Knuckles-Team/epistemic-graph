//! EG-UNIFIED-DATA-PLANE-R026.2.1 — adapter skeleton for the Gramps pilot's
//! P0 baseline: a typed baseline-digest record and its mapping to the P0
//! phase exit artifact, plus the refusal path before any phase is marked
//! closed. Running the real baseline capture against a live Gramps export,
//! the Postgres control, and the unmodified corpus replay are
//! EG-UNIFIED-DATA-PLANE-R026.2.2+.

use crate::gramps_pilot_phase::{GrampsPilotPhase, InvalidPhaseExit, PhaseExitArtifact};
use serde::{Deserialize, Serialize};

/// A content digest captured for one baseline record set, before any live
/// Postgres control or replay runs against it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrampsBaselineDigest {
    pub record_count: u64,
    pub digest_hex: String,
}

/// Why a [`GrampsBaselineDigest`] was refused before it closed P0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrampsBaselineDigestRefusal {
    ZeroRecordCount,
    EmptyDigest,
    PhaseExit(InvalidPhaseExit),
}

impl std::fmt::Display for GrampsBaselineDigestRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRecordCount => write!(f, "baseline digest covers zero records"),
            Self::EmptyDigest => write!(f, "baseline digest is empty"),
            Self::PhaseExit(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for GrampsBaselineDigestRefusal {}

impl GrampsBaselineDigest {
    /// Validates this digest and, if well-formed, maps it to the P0 phase
    /// exit artifact. Performs NO live baseline capture itself: a refusal
    /// here never closes P0.
    pub fn to_phase_exit(
        &self,
        reviewed_by: &str,
    ) -> Result<PhaseExitArtifact, GrampsBaselineDigestRefusal> {
        if self.record_count == 0 {
            return Err(GrampsBaselineDigestRefusal::ZeroRecordCount);
        }
        if self.digest_hex.trim().is_empty() {
            return Err(GrampsBaselineDigestRefusal::EmptyDigest);
        }
        let artifact = PhaseExitArtifact {
            phase: GrampsPilotPhase::P0Baseline,
            artifact_kind: GrampsPilotPhase::P0Baseline
                .required_artifact_kind()
                .to_string(),
            reviewed_by: reviewed_by.to_string(),
        };
        artifact
            .validate()
            .map_err(GrampsBaselineDigestRefusal::PhaseExit)?;
        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> GrampsBaselineDigest {
        GrampsBaselineDigest {
            record_count: 196,
            digest_hex: "a1b2c3d4".to_string(),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.2.1
    #[test]
    fn well_formed_digest_maps_to_the_p0_phase_exit() {
        let exit = valid()
            .to_phase_exit("audel")
            .expect("well-formed digest maps");
        assert_eq!(exit.phase, GrampsPilotPhase::P0Baseline);
        assert_eq!(exit.artifact_kind, "content_digest");
        assert_eq!(exit.reviewed_by, "audel");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.2.1
    #[test]
    fn refuses_zero_record_count() {
        let mut digest = valid();
        digest.record_count = 0;
        assert_eq!(
            digest.to_phase_exit("audel"),
            Err(GrampsBaselineDigestRefusal::ZeroRecordCount)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.2.1
    #[test]
    fn refuses_empty_digest_hex() {
        let mut digest = valid();
        digest.digest_hex = "  ".to_string();
        assert_eq!(
            digest.to_phase_exit("audel"),
            Err(GrampsBaselineDigestRefusal::EmptyDigest)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.2.1
    #[test]
    fn refuses_an_unreviewed_exit() {
        let err = valid().to_phase_exit("   ").unwrap_err();
        assert_eq!(
            err,
            GrampsBaselineDigestRefusal::PhaseExit(InvalidPhaseExit::NotReviewed)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.2.1
    #[test]
    fn digest_serializes_round_trip() {
        let digest = valid();
        let encoded = serde_json::to_string(&digest).unwrap();
        let decoded: GrampsBaselineDigest = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, digest);
    }
}
