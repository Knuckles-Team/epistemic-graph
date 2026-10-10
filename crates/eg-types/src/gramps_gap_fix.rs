//! EG-UNIFIED-DATA-PLANE-R026.3.1 — adapter skeleton for the Gramps pilot's
//! P3 proven-gap fixes: a typed fix record that must name a failing test
//! that then passes, its mapping to the P3 phase exit artifact, and the
//! refusal path before any phase is marked closed. Running the real restore
//! verification (P4) and the schema-understanding demonstration (P5) are
//! EG-UNIFIED-DATA-PLANE-R026.3.2+.

use crate::gramps_pilot_phase::{GrampsPilotPhase, InvalidPhaseExit, PhaseExitArtifact};
use serde::{Deserialize, Serialize};

/// One gap fixed because a captured driver statement proved a real
/// discrepancy, with a failing test written before the fix and the same
/// test passing after it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrampsProvenGapFix {
    pub description: String,
    pub failing_test_name: String,
    pub passing_test_name: String,
}

/// Why a [`GrampsProvenGapFix`] was refused before it closed P3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrampsProvenGapFixRefusal {
    EmptyDescription,
    EmptyFailingTestName,
    EmptyPassingTestName,
    /// The same named test cannot stand as both the pre-fix failing proof
    /// and the post-fix passing proof: that proves nothing changed.
    SameTestBeforeAndAfter,
    PhaseExit(InvalidPhaseExit),
}

impl std::fmt::Display for GrampsProvenGapFixRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyDescription => write!(f, "gap fix names no description"),
            Self::EmptyFailingTestName => write!(f, "gap fix names no failing test"),
            Self::EmptyPassingTestName => write!(f, "gap fix names no passing test"),
            Self::SameTestBeforeAndAfter => {
                write!(f, "failing and passing test names must differ")
            }
            Self::PhaseExit(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for GrampsProvenGapFixRefusal {}

impl GrampsProvenGapFix {
    /// Validates this fix and, if well-formed, maps it to the P3 phase exit
    /// artifact. Performs NO live test run itself: a refusal here never
    /// closes P3.
    pub fn to_phase_exit(
        &self,
        reviewed_by: &str,
    ) -> Result<PhaseExitArtifact, GrampsProvenGapFixRefusal> {
        if self.description.trim().is_empty() {
            return Err(GrampsProvenGapFixRefusal::EmptyDescription);
        }
        if self.failing_test_name.trim().is_empty() {
            return Err(GrampsProvenGapFixRefusal::EmptyFailingTestName);
        }
        if self.passing_test_name.trim().is_empty() {
            return Err(GrampsProvenGapFixRefusal::EmptyPassingTestName);
        }
        if self.failing_test_name == self.passing_test_name {
            return Err(GrampsProvenGapFixRefusal::SameTestBeforeAndAfter);
        }
        let artifact = PhaseExitArtifact {
            phase: GrampsPilotPhase::P3ProvenGapFixes,
            artifact_kind: GrampsPilotPhase::P3ProvenGapFixes
                .required_artifact_kind()
                .to_string(),
            reviewed_by: reviewed_by.to_string(),
        };
        artifact
            .validate()
            .map_err(GrampsProvenGapFixRefusal::PhaseExit)?;
        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> GrampsProvenGapFix {
        GrampsProvenGapFix {
            description: "pgoutput missed a jsonb null".to_string(),
            failing_test_name: "capture_jsonb_null_before_fix".to_string(),
            passing_test_name: "capture_jsonb_null_after_fix".to_string(),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.3.1
    #[test]
    fn well_formed_fix_maps_to_the_p3_phase_exit() {
        let exit = valid()
            .to_phase_exit("audel")
            .expect("well-formed fix maps");
        assert_eq!(exit.phase, GrampsPilotPhase::P3ProvenGapFixes);
        assert_eq!(exit.artifact_kind, "failing_then_passing_test");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.3.1
    #[test]
    fn refuses_empty_description() {
        let mut fix = valid();
        fix.description = "".to_string();
        assert_eq!(
            fix.to_phase_exit("audel"),
            Err(GrampsProvenGapFixRefusal::EmptyDescription)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.3.1
    #[test]
    fn refuses_empty_failing_test_name() {
        let mut fix = valid();
        fix.failing_test_name = "  ".to_string();
        assert_eq!(
            fix.to_phase_exit("audel"),
            Err(GrampsProvenGapFixRefusal::EmptyFailingTestName)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.3.1
    #[test]
    fn refuses_the_same_test_before_and_after() {
        let mut fix = valid();
        fix.passing_test_name = fix.failing_test_name.clone();
        assert_eq!(
            fix.to_phase_exit("audel"),
            Err(GrampsProvenGapFixRefusal::SameTestBeforeAndAfter)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.3.1
    #[test]
    fn refuses_an_unreviewed_exit() {
        let err = valid().to_phase_exit("").unwrap_err();
        assert_eq!(
            err,
            GrampsProvenGapFixRefusal::PhaseExit(InvalidPhaseExit::NotReviewed)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R026.3.1
    #[test]
    fn fix_serializes_round_trip() {
        let fix = valid();
        let encoded = serde_json::to_string(&fix).unwrap();
        let decoded: GrampsProvenGapFix = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, fix);
    }
}
