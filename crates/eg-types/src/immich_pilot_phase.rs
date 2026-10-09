//! Typed model for the Immich connector pilot's phase gate
//! (EG-UNIFIED-DATA-PLANE-R032): phases I0 through I6, each gated on its own
//! exit artifact kind, reviewed before the next phase begins. This is the
//! typed-model slice (`.1`): the closed phase/artifact-kind vocabulary and
//! the refusal to advance without the phase's required, reviewed exit
//! artifact. Running each phase (the OpenAPI client generation, the
//! read-first/approval-gated MCP tools, catalog registration, incremental
//! ingest with no-op replay, face-cluster naming, cross-app Person link
//! proposals, and the health/lag/upgrade-digest checks) is later children.

use serde::{Deserialize, Serialize};

/// One pilot phase, in required order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImmichPilotPhase {
    /// Generates a version-pinned API client from a checked-in OpenAPI
    /// fixture and records its digest.
    I0ClientGeneration,
    /// Provides read-first domain MCP tools and approval-gated writes.
    I1ReadFirstTools,
    /// Registers the connector in every required release catalog and
    /// validates catalog parity.
    I2CatalogRegistration,
    /// Ingests assets, albums, and per-user people incrementally, proving a
    /// second run is a no-op and one new upload appears.
    I3IncrementalIngest,
    /// Names representative face clusters through the application UI.
    I4FaceClusterNaming,
    /// Emits cross-application Person links as proposals and approves
    /// selected links before a cross-app SPARQL question.
    I5CrossAppLinkApproval,
    /// Adds health, lag, and upgrade-digest checks.
    I6HealthChecks,
}

impl ImmichPilotPhase {
    /// Every phase in required order.
    pub const ORDER: [Self; 7] = [
        Self::I0ClientGeneration,
        Self::I1ReadFirstTools,
        Self::I2CatalogRegistration,
        Self::I3IncrementalIngest,
        Self::I4FaceClusterNaming,
        Self::I5CrossAppLinkApproval,
        Self::I6HealthChecks,
    ];

    /// The exit-artifact kind this phase's spec defines, by name — used to
    /// refuse a phase closed with the wrong artifact.
    pub const fn required_artifact_kind(self) -> &'static str {
        match self {
            Self::I0ClientGeneration => "client_digest",
            Self::I1ReadFirstTools => "approval_gated_tool_spec",
            Self::I2CatalogRegistration => "catalog_parity_report",
            Self::I3IncrementalIngest => "noop_replay_report",
            Self::I4FaceClusterNaming => "face_cluster_naming_report",
            Self::I5CrossAppLinkApproval => "approved_link_set",
            Self::I6HealthChecks => "health_lag_digest_report",
        }
    }

    fn position(self) -> usize {
        Self::ORDER.iter().position(|phase| *phase == self).unwrap()
    }
}

/// One phase's exit artifact, reviewed before the pilot advances.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseExitArtifact {
    pub phase: ImmichPilotPhase,
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
pub struct ImmichPilotProgress {
    pub exits: Vec<PhaseExitArtifact>,
}

impl ImmichPilotProgress {
    /// The next phase this pilot may enter: the first phase in order with
    /// no recorded, valid exit yet. `None` once every phase has exited.
    pub fn next_phase(&self) -> Option<ImmichPilotPhase> {
        ImmichPilotPhase::ORDER
            .into_iter()
            .find(|phase| !self.exited(*phase))
    }

    fn exited(&self, phase: ImmichPilotPhase) -> bool {
        self.exits
            .iter()
            .any(|exit| exit.phase == phase && exit.validate().is_ok())
    }

    /// Whether `phase` may be entered now: every earlier phase in order has
    /// already exited. Refuses skipping ahead.
    pub fn admits(&self, phase: ImmichPilotPhase) -> bool {
        ImmichPilotPhase::ORDER[..phase.position()]
            .iter()
            .all(|earlier| self.exited(*earlier))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exit(phase: ImmichPilotPhase) -> PhaseExitArtifact {
        PhaseExitArtifact {
            phase,
            artifact_kind: phase.required_artifact_kind().to_string(),
            reviewed_by: "alice".to_string(),
        }
    }

    #[test]
    fn well_formed_exit_validates() {
        assert_eq!(
            exit(ImmichPilotPhase::I0ClientGeneration).validate(),
            Ok(())
        );
    }

    #[test]
    fn wrong_artifact_kind_is_refused() {
        let mut artifact = exit(ImmichPilotPhase::I0ClientGeneration);
        artifact.artifact_kind = "noop_replay_report".to_string();
        assert!(matches!(
            artifact.validate(),
            Err(InvalidPhaseExit::WrongArtifactKind { .. })
        ));
    }

    #[test]
    fn unreviewed_exit_is_refused() {
        let mut artifact = exit(ImmichPilotPhase::I0ClientGeneration);
        artifact.reviewed_by = "  ".to_string();
        assert_eq!(artifact.validate(), Err(InvalidPhaseExit::NotReviewed));
    }

    #[test]
    fn fresh_pilot_admits_only_i0() {
        let progress = ImmichPilotProgress::default();
        assert!(progress.admits(ImmichPilotPhase::I0ClientGeneration));
        assert!(!progress.admits(ImmichPilotPhase::I1ReadFirstTools));
        assert_eq!(
            progress.next_phase(),
            Some(ImmichPilotPhase::I0ClientGeneration)
        );
    }

    #[test]
    fn skipping_ahead_without_prior_exit_is_refused() {
        let progress = ImmichPilotProgress {
            exits: vec![exit(ImmichPilotPhase::I0ClientGeneration)],
        };
        assert!(!progress.admits(ImmichPilotPhase::I2CatalogRegistration));
        assert!(progress.admits(ImmichPilotPhase::I1ReadFirstTools));
    }

    #[test]
    fn invalid_exit_does_not_advance_progress() {
        let mut bad_exit = exit(ImmichPilotPhase::I0ClientGeneration);
        bad_exit.reviewed_by = String::new();
        let progress = ImmichPilotProgress {
            exits: vec![bad_exit],
        };
        assert_eq!(
            progress.next_phase(),
            Some(ImmichPilotPhase::I0ClientGeneration)
        );
    }

    #[test]
    fn full_order_completes_pilot() {
        let progress = ImmichPilotProgress {
            exits: ImmichPilotPhase::ORDER.into_iter().map(exit).collect(),
        };
        assert_eq!(progress.next_phase(), None);
    }
}
