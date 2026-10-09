//! Typed model for the Gramps production cutover go-ahead
//! (EG-UNIFIED-DATA-PLANE-R033, pilot phase P6): the explicit, separately
//! recorded approval that must follow the P0-P5 pilot evidence
//! (`crate::gramps_pilot_phase`) before production moves to EG-backed
//! storage, and the minimum fallback window that approval must declare.
//! This is the typed-model slice (`.1`): the go-ahead model and its
//! refusal — citing anything but a valid P5 schema-understanding exit,
//! naming no approver, or declaring a fallback window shorter than the
//! required 30 days. The real cutover (Postgres replica, nightly XML
//! export, and the rehearsed failover test) is a later child.

use serde::{Deserialize, Serialize};

use crate::gramps_pilot_phase::{GrampsPilotPhase, PhaseExitArtifact};

/// The minimum read-only SQLite fallback window the spec requires.
pub const MINIMUM_FALLBACK_WINDOW_DAYS: u32 = 30;

/// The recorded go-ahead for the Gramps production cutover.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrampsCutoverGoAhead {
    pub p5_exit: PhaseExitArtifact,
    pub approved_by: String,
    pub fallback_window_days: u32,
}

/// A cutover go-ahead failed a gate rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidCutoverGoAhead {
    WrongExitPhase(GrampsPilotPhase),
    InvalidP5Exit(String),
    NotApproved,
    FallbackWindowTooShort(u32),
}

impl std::fmt::Display for InvalidCutoverGoAhead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongExitPhase(phase) => {
                write!(f, "go-ahead cites phase {phase:?}, not P5SchemaUnderstanding")
            }
            Self::InvalidP5Exit(reason) => write!(f, "cited P5 exit is invalid: {reason}"),
            Self::NotApproved => write!(f, "go-ahead names no approver"),
            Self::FallbackWindowTooShort(days) => write!(
                f,
                "fallback window of {days} day(s) is shorter than the required {MINIMUM_FALLBACK_WINDOW_DAYS}"
            ),
        }
    }
}

impl std::error::Error for InvalidCutoverGoAhead {}

/// Confirm a cutover go-ahead is well-formed: it cites a valid P5
/// schema-understanding exit, names an approver, and declares a fallback
/// window of at least 30 days. Refuses rather than admitting a cutover with
/// missing pilot evidence or an unsafe fallback window.
pub fn validate_go_ahead(go_ahead: &GrampsCutoverGoAhead) -> Result<(), InvalidCutoverGoAhead> {
    if go_ahead.p5_exit.phase != GrampsPilotPhase::P5SchemaUnderstanding {
        return Err(InvalidCutoverGoAhead::WrongExitPhase(
            go_ahead.p5_exit.phase,
        ));
    }
    go_ahead
        .p5_exit
        .validate()
        .map_err(|error| InvalidCutoverGoAhead::InvalidP5Exit(error.to_string()))?;
    if go_ahead.approved_by.trim().is_empty() {
        return Err(InvalidCutoverGoAhead::NotApproved);
    }
    if go_ahead.fallback_window_days < MINIMUM_FALLBACK_WINDOW_DAYS {
        return Err(InvalidCutoverGoAhead::FallbackWindowTooShort(
            go_ahead.fallback_window_days,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p5_exit() -> PhaseExitArtifact {
        PhaseExitArtifact {
            phase: GrampsPilotPhase::P5SchemaUnderstanding,
            artifact_kind: GrampsPilotPhase::P5SchemaUnderstanding
                .required_artifact_kind()
                .to_string(),
            reviewed_by: "alice".to_string(),
        }
    }

    fn go_ahead() -> GrampsCutoverGoAhead {
        GrampsCutoverGoAhead {
            p5_exit: p5_exit(),
            approved_by: "bob".to_string(),
            fallback_window_days: 30,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R002.3, EG-UNIFIED-DATA-PLANE-R033.1, EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn well_formed_go_ahead_validates() {
        assert_eq!(validate_go_ahead(&go_ahead()), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R002.3, EG-UNIFIED-DATA-PLANE-R033.1, EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn go_ahead_citing_wrong_phase_is_refused() {
        let mut bad = go_ahead();
        bad.p5_exit.phase = GrampsPilotPhase::P4RestoreVerification;
        bad.p5_exit.artifact_kind = GrampsPilotPhase::P4RestoreVerification
            .required_artifact_kind()
            .to_string();
        assert!(matches!(
            validate_go_ahead(&bad),
            Err(InvalidCutoverGoAhead::WrongExitPhase(_))
        ));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R002.3, EG-UNIFIED-DATA-PLANE-R033.1, EG-UNIFIED-DATA-PLANE-R038.1
    #[test]
    fn go_ahead_citing_unreviewed_p5_exit_is_refused() {
        let mut bad = go_ahead();
        bad.p5_exit.reviewed_by = String::new();
        assert!(matches!(
            validate_go_ahead(&bad),
            Err(InvalidCutoverGoAhead::InvalidP5Exit(_))
        ));
    }

    #[test]
    fn go_ahead_with_no_approver_is_refused() {
        let mut bad = go_ahead();
        bad.approved_by = "   ".to_string();
        assert_eq!(
            validate_go_ahead(&bad),
            Err(InvalidCutoverGoAhead::NotApproved)
        );
    }

    #[test]
    fn go_ahead_with_short_fallback_window_is_refused() {
        let mut bad = go_ahead();
        bad.fallback_window_days = 14;
        assert_eq!(
            validate_go_ahead(&bad),
            Err(InvalidCutoverGoAhead::FallbackWindowTooShort(14))
        );
    }

    #[test]
    fn go_ahead_serializes_round_trip() {
        let original = go_ahead();
        let encoded = serde_json::to_string(&original).unwrap();
        let decoded: GrampsCutoverGoAhead = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, original);
    }
}
