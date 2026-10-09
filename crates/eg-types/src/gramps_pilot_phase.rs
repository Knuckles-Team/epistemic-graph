//! Typed model for the Gramps native-hosting pilot's phase gate
//! (EG-UNIFIED-DATA-PLANE-R026): phases P0 through P5, each gated on its own
//! exit artifact kind, reviewed before the next phase begins. This is the
//! typed-model slice (`.1`): the closed phase/artifact-kind vocabulary and
//! the refusal to advance without the phase's required, reviewed exit
//! artifact. Running each phase (the baseline digest, the Postgres control,
//! the unmodified corpus replay, proven-gap fixes, restore verification, and
//! the schema-understanding demonstration) is later children.

use serde::{Deserialize, Serialize};

/// One pilot phase, in required order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrampsPilotPhase {
    /// Records a baseline content digest.
    P0Baseline,
    /// Establishes a real-Postgres control with a captured driver statement
    /// corpus.
    P1PostgresControl,
    /// Runs that corpus unmodified against EG.
    P2UnmodifiedReplay,
    /// Fixes only proven gaps, each with a failing test first.
    P3ProvenGapFixes,
    /// Verifies restore paths.
    P4RestoreVerification,
    /// Demonstrates schema understanding through an approved mapping and
    /// query.
    P5SchemaUnderstanding,
}

impl GrampsPilotPhase {
    /// Every phase in required order.
    pub const ORDER: [Self; 6] = [
        Self::P0Baseline,
        Self::P1PostgresControl,
        Self::P2UnmodifiedReplay,
        Self::P3ProvenGapFixes,
        Self::P4RestoreVerification,
        Self::P5SchemaUnderstanding,
    ];

    /// The exit-artifact kind this phase's spec defines, by name — used to
    /// refuse a phase closed with the wrong artifact.
    pub const fn required_artifact_kind(self) -> &'static str {
        match self {
            Self::P0Baseline => "content_digest",
            Self::P1PostgresControl => "statement_corpus",
            Self::P2UnmodifiedReplay => "replay_report",
            Self::P3ProvenGapFixes => "failing_then_passing_test",
            Self::P4RestoreVerification => "restore_report",
            Self::P5SchemaUnderstanding => "approved_mapping_and_query",
        }
    }

    fn position(self) -> usize {
        Self::ORDER.iter().position(|phase| *phase == self).unwrap()
    }
}

/// One phase's exit artifact, reviewed before the pilot advances.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseExitArtifact {
    pub phase: GrampsPilotPhase,
    pub artifact_kind: String,
    pub reviewed_by: String,
}

/// An exit artifact failed a gate rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidPhaseExit {
    WrongArtifactKind { expected: &'static str, got: String },
    NotReviewed,
}

impl std::fmt::Display for InvalidPhaseExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongArtifactKind { expected, got } => {
                write!(f, "phase requires artifact kind {expected:?}, got {got:?}")
            }
            Self::NotReviewed => write!(f, "exit artifact names no reviewer"),
        }
    }
}

impl std::error::Error for InvalidPhaseExit {}

impl PhaseExitArtifact {
    /// Confirm this exit artifact's kind matches its phase's required kind
    /// and names a reviewer. Refuses rather than accepting any artifact as
    /// closing any phase.
    pub fn validate(&self) -> Result<(), InvalidPhaseExit> {
        let expected = self.phase.required_artifact_kind();
        if self.artifact_kind != expected {
            return Err(InvalidPhaseExit::WrongArtifactKind {
                expected,
                got: self.artifact_kind.clone(),
            });
        }
        if self.reviewed_by.trim().is_empty() {
            return Err(InvalidPhaseExit::NotReviewed);
        }
        Ok(())
    }
}

/// The pilot's recorded exit history. `next_phase` and `admits` are the
/// entry points a runner uses instead of inspecting `exits` directly.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrampsPilotProgress {
    pub exits: Vec<PhaseExitArtifact>,
}

impl GrampsPilotProgress {
    /// The next phase this pilot may enter: the first phase in order with
    /// no recorded, valid exit yet. `None` once every phase has exited.
    pub fn next_phase(&self) -> Option<GrampsPilotPhase> {
        GrampsPilotPhase::ORDER
            .into_iter()
            .find(|phase| !self.exited(*phase))
    }

    fn exited(&self, phase: GrampsPilotPhase) -> bool {
        self.exits
            .iter()
            .any(|exit| exit.phase == phase && exit.validate().is_ok())
    }

    /// Whether `phase` may be entered now: every earlier phase in order has
    /// already exited. Refuses skipping ahead.
    pub fn admits(&self, phase: GrampsPilotPhase) -> bool {
        GrampsPilotPhase::ORDER[..phase.position()]
            .iter()
            .all(|earlier| self.exited(*earlier))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exit(phase: GrampsPilotPhase) -> PhaseExitArtifact {
        PhaseExitArtifact {
            phase,
            artifact_kind: phase.required_artifact_kind().to_string(),
            reviewed_by: "alice".to_string(),
        }
    }

    #[test]
    fn well_formed_exit_validates() {
        assert_eq!(exit(GrampsPilotPhase::P0Baseline).validate(), Ok(()));
    }

    #[test]
    fn wrong_artifact_kind_is_refused() {
        let mut artifact = exit(GrampsPilotPhase::P0Baseline);
        artifact.artifact_kind = "replay_report".to_string();
        assert!(matches!(
            artifact.validate(),
            Err(InvalidPhaseExit::WrongArtifactKind { .. })
        ));
    }

    #[test]
    fn unreviewed_exit_is_refused() {
        let mut artifact = exit(GrampsPilotPhase::P0Baseline);
        artifact.reviewed_by = "  ".to_string();
        assert_eq!(artifact.validate(), Err(InvalidPhaseExit::NotReviewed));
    }

    #[test]
    fn fresh_pilot_admits_only_p0() {
        let progress = GrampsPilotProgress::default();
        assert!(progress.admits(GrampsPilotPhase::P0Baseline));
        assert!(!progress.admits(GrampsPilotPhase::P1PostgresControl));
        assert_eq!(progress.next_phase(), Some(GrampsPilotPhase::P0Baseline));
    }

    #[test]
    fn skipping_ahead_without_prior_exit_is_refused() {
        let progress = GrampsPilotProgress {
            exits: vec![exit(GrampsPilotPhase::P0Baseline)],
        };
        assert!(!progress.admits(GrampsPilotPhase::P2UnmodifiedReplay));
        assert!(progress.admits(GrampsPilotPhase::P1PostgresControl));
    }

    #[test]
    fn invalid_exit_does_not_advance_progress() {
        let mut bad_exit = exit(GrampsPilotPhase::P0Baseline);
        bad_exit.reviewed_by = String::new();
        let progress = GrampsPilotProgress {
            exits: vec![bad_exit],
        };
        assert_eq!(progress.next_phase(), Some(GrampsPilotPhase::P0Baseline));
    }

    #[test]
    fn full_order_completes_pilot() {
        let progress = GrampsPilotProgress {
            exits: GrampsPilotPhase::ORDER.into_iter().map(exit).collect(),
        };
        assert_eq!(progress.next_phase(), None);
    }
}
