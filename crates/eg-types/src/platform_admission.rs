//! Typed model for admitting one application to the shared database
//! platform (EG-UNIFIED-DATA-PLANE-R024): the admission stage machine that
//! refuses retiring an application's prior ingest connector before it is
//! fully attached with parity. This is the typed-model slice (`.1`): the
//! stage enum and its allowed-transition refusal. The real admission
//! runner, the tested rollback path, and the per-application admission test
//! are later children.

use serde::{Deserialize, Serialize};

/// One application's admission progress on the shared platform. Ordered:
/// each stage names the stages it may advance to next.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionStage {
    /// Not yet attached to the shared platform; the prior connector is the
    /// only live write path.
    Pending,
    /// Attached to the shared platform; the prior connector still writes.
    Admitted,
    /// The attached copy has been confirmed at parity with the prior
    /// connector's data.
    ParityConfirmed,
    /// The prior ingest connector has stopped writing; the shared platform
    /// is now authoritative.
    ConnectorRetired,
    /// Rolled back to `Pending`: the shared-platform attachment was
    /// abandoned and the prior connector remains authoritative.
    RolledBack,
}

/// An admission attempted a transition its current stage does not allow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DisallowedTransition {
    pub from: AdmissionStage,
    pub to: AdmissionStage,
}

impl std::fmt::Display for DisallowedTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot advance admission from {:?} to {:?}",
            self.from, self.to
        )
    }
}

impl std::error::Error for DisallowedTransition {}

impl AdmissionStage {
    /// Whether advancing from `self` to `next` is an allowed transition.
    /// `ConnectorRetired` is reachable ONLY from `ParityConfirmed` --
    /// the acceptance rule this type exists to enforce. Rollback is allowed
    /// from `Admitted` or `ParityConfirmed` (abandon before the connector is
    /// retired), never from `ConnectorRetired` (no connector remains to roll
    /// back to) or `RolledBack` itself.
    pub fn allows(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Admitted)
                | (Self::Admitted, Self::ParityConfirmed)
                | (Self::ParityConfirmed, Self::ConnectorRetired)
                | (Self::Admitted, Self::RolledBack)
                | (Self::ParityConfirmed, Self::RolledBack)
        )
    }
}

/// One application's admission record: its name and current stage.
/// `advance` is the one entry point that moves it; a direct field write
/// would bypass the transition rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationAdmission {
    pub application: String,
    pub stage: AdmissionStage,
}

impl ApplicationAdmission {
    pub fn new(application: impl Into<String>) -> Self {
        Self {
            application: application.into(),
            stage: AdmissionStage::Pending,
        }
    }

    /// Advance to `next`, refusing a transition `self.stage` does not allow
    /// rather than silently jumping the application's recorded stage.
    pub fn advance(&mut self, next: AdmissionStage) -> Result<(), DisallowedTransition> {
        if self.stage.allows(next) {
            self.stage = next;
            Ok(())
        } else {
            Err(DisallowedTransition {
                from: self.stage,
                to: next,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_admission_path_advances_in_order() {
        let mut admission = ApplicationAdmission::new("gramps");
        admission.advance(AdmissionStage::Admitted).unwrap();
        admission.advance(AdmissionStage::ParityConfirmed).unwrap();
        admission.advance(AdmissionStage::ConnectorRetired).unwrap();
        assert_eq!(admission.stage, AdmissionStage::ConnectorRetired);
    }

    #[test]
    fn retiring_connector_before_parity_is_refused() {
        let mut admission = ApplicationAdmission::new("gramps");
        admission.advance(AdmissionStage::Admitted).unwrap();
        let err = admission
            .advance(AdmissionStage::ConnectorRetired)
            .unwrap_err();
        assert_eq!(err.from, AdmissionStage::Admitted);
        assert_eq!(admission.stage, AdmissionStage::Admitted);
    }

    #[test]
    fn retiring_connector_from_pending_is_refused() {
        let mut admission = ApplicationAdmission::new("twenty");
        assert!(admission.advance(AdmissionStage::ConnectorRetired).is_err());
        assert_eq!(admission.stage, AdmissionStage::Pending);
    }

    #[test]
    fn rollback_is_allowed_before_connector_retirement() {
        let mut admission = ApplicationAdmission::new("firefly");
        admission.advance(AdmissionStage::Admitted).unwrap();
        admission.advance(AdmissionStage::ParityConfirmed).unwrap();
        admission.advance(AdmissionStage::RolledBack).unwrap();
        assert_eq!(admission.stage, AdmissionStage::RolledBack);
    }

    #[test]
    fn rollback_after_connector_retirement_is_refused() {
        let mut admission = ApplicationAdmission::new("immich");
        admission.advance(AdmissionStage::Admitted).unwrap();
        admission.advance(AdmissionStage::ParityConfirmed).unwrap();
        admission.advance(AdmissionStage::ConnectorRetired).unwrap();
        assert!(admission.advance(AdmissionStage::RolledBack).is_err());
        assert_eq!(admission.stage, AdmissionStage::ConnectorRetired);
    }

    #[test]
    fn stage_serializes_round_trip() {
        let admission = ApplicationAdmission::new("gramps");
        let encoded = serde_json::to_string(&admission).unwrap();
        let decoded: ApplicationAdmission = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, admission);
    }
}
