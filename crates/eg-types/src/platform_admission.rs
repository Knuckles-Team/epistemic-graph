//! Typed model for admitting one application to the shared database
//! platform (EG-UNIFIED-DATA-PLANE-R024): the admission stage machine that
//! refuses retiring an application's prior ingest connector before it is
//! fully attached with parity. This is the typed-model slice (`.1`): the
//! stage enum and its allowed-transition refusal. `.2.1` adds the admission
//! runner and its tested rollback path against a pluggable connector, and
//! `.3.1` adds the per-candidate-application admission tests that confirm
//! rollback and that a retired connector stops writing -- both driven
//! against a fake in-process connector so they need no live candidate
//! application or database. The live per-application runs against the real
//! MariaDB-backed candidates remain later children.

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

/// An application's prior ingest connector: the live write path the shared
/// platform admission is meant to retire only after confirmed parity.
/// Abstracted as a trait so the admission runner and its rollback path are
/// unit-tested against a fake in-process connector, with no live candidate
/// application or database required.
pub trait PriorIngestConnector {
    /// Whether this connector currently accepts writes. `true` until
    /// retirement; the runner must never report a retirement that leaves
    /// this `true`, and must never retire while rollback is still possible.
    fn is_writing(&self) -> bool;

    /// Stop accepting writes. Called only once the runner has confirmed
    /// `ParityConfirmed`.
    fn retire(&mut self);
}

/// An error the admission runner refuses to proceed past.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionRunError {
    /// The underlying stage transition was disallowed.
    Transition(DisallowedTransition),
    /// Retirement was attempted while the connector still reports writing
    /// after `retire()` ran -- the runner refuses to record
    /// `ConnectorRetired` on a connector that did not actually stop.
    ConnectorStillWriting,
}

impl std::fmt::Display for AdmissionRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transition(err) => write!(f, "{err}"),
            Self::ConnectorStillWriting => {
                write!(f, "refusing to record retirement: connector still writing")
            }
        }
    }
}

impl std::error::Error for AdmissionRunError {}

impl From<DisallowedTransition> for AdmissionRunError {
    fn from(err: DisallowedTransition) -> Self {
        Self::Transition(err)
    }
}

/// Drives one application's `ApplicationAdmission` through the shared
/// platform admission lifecycle against its prior ingest connector,
/// including the tested rollback path. This is the runner slice
/// (`EG-UNIFIED-DATA-PLANE-R024.2.1`): it needs no live candidate
/// application, only an implementation of `PriorIngestConnector`.
pub struct AdmissionRunner<C: PriorIngestConnector> {
    pub admission: ApplicationAdmission,
    pub connector: C,
}

impl<C: PriorIngestConnector> AdmissionRunner<C> {
    pub fn new(application: impl Into<String>, connector: C) -> Self {
        Self {
            admission: ApplicationAdmission::new(application),
            connector,
        }
    }

    /// Attach the application to the shared platform. The prior connector
    /// keeps writing.
    pub fn admit(&mut self) -> Result<(), AdmissionRunError> {
        self.admission.advance(AdmissionStage::Admitted)?;
        Ok(())
    }

    /// Confirm the attached copy is at parity with the prior connector.
    pub fn confirm_parity(&mut self) -> Result<(), AdmissionRunError> {
        self.admission.advance(AdmissionStage::ParityConfirmed)?;
        Ok(())
    }

    /// Retire the prior connector. Refused by the underlying stage machine
    /// before `ParityConfirmed`, and refused here if the connector reports
    /// it is still writing after `retire()` runs.
    pub fn retire_connector(&mut self) -> Result<(), AdmissionRunError> {
        self.admission.advance(AdmissionStage::ConnectorRetired)?;
        self.connector.retire();
        if self.connector.is_writing() {
            return Err(AdmissionRunError::ConnectorStillWriting);
        }
        Ok(())
    }

    /// Abandon the shared-platform attachment: roll back to `Pending`. The
    /// prior connector was never retired, so it is still the live write
    /// path -- the runner does not touch it. Refused once the connector has
    /// already been retired (the same-named `advance` refusal).
    pub fn rollback(&mut self) -> Result<(), AdmissionRunError> {
        self.admission.advance(AdmissionStage::RolledBack)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake prior ingest connector: tracks whether it is writing in
    /// memory, so admission-runner tests need no live candidate application
    /// or database.
    struct FakeConnector {
        writing: bool,
    }

    impl FakeConnector {
        fn new() -> Self {
            Self { writing: true }
        }
    }

    impl PriorIngestConnector for FakeConnector {
        fn is_writing(&self) -> bool {
            self.writing
        }

        fn retire(&mut self) {
            self.writing = false;
        }
    }

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

    // spec: EG-UNIFIED-DATA-PLANE-R024.2.1
    #[test]
    fn runner_retires_connector_only_after_parity_and_stops_its_writes() {
        let mut runner = AdmissionRunner::new("gramps", FakeConnector::new());
        runner.admit().unwrap();
        runner.confirm_parity().unwrap();
        assert!(runner.connector.is_writing());
        runner.retire_connector().unwrap();
        assert_eq!(runner.admission.stage, AdmissionStage::ConnectorRetired);
        assert!(!runner.connector.is_writing());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R024.2.1
    #[test]
    fn runner_rollback_restores_prior_state_and_connector_keeps_writing() {
        let mut runner = AdmissionRunner::new("immich", FakeConnector::new());
        runner.admit().unwrap();
        runner.confirm_parity().unwrap();
        runner.rollback().unwrap();
        assert_eq!(runner.admission.stage, AdmissionStage::RolledBack);
        // Rollback never touches the prior connector: it was never retired,
        // so it remains the live write path.
        assert!(runner.connector.is_writing());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R024.2.1
    #[test]
    fn runner_rollback_is_refused_once_connector_is_retired() {
        let mut runner = AdmissionRunner::new("firefly", FakeConnector::new());
        runner.admit().unwrap();
        runner.confirm_parity().unwrap();
        runner.retire_connector().unwrap();
        assert!(runner.rollback().is_err());
        assert_eq!(runner.admission.stage, AdmissionStage::ConnectorRetired);
    }

    /// One admission test per candidate application
    /// (`EG-UNIFIED-DATA-PLANE-R024.3.1`): confirms rollback restores the
    /// prior state and that a retired connector no longer writes, against
    /// a fake connector so no live candidate application is required.
    fn per_application_rollback_and_retirement_case(application: &str) {
        let mut rolled_back = AdmissionRunner::new(application, FakeConnector::new());
        rolled_back.admit().unwrap();
        rolled_back.rollback().unwrap();
        assert_eq!(rolled_back.admission.stage, AdmissionStage::RolledBack);
        assert!(
            rolled_back.connector.is_writing(),
            "{application}: rollback must leave the prior connector writing"
        );

        let mut retired = AdmissionRunner::new(application, FakeConnector::new());
        retired.admit().unwrap();
        retired.confirm_parity().unwrap();
        retired.retire_connector().unwrap();
        assert!(
            !retired.connector.is_writing(),
            "{application}: retired connector must stop writing"
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R024.3.1
    #[test]
    fn per_application_rollback_and_retirement() {
        for application in ["gramps", "immich", "firefly"] {
            per_application_rollback_and_retirement_case(application);
        }
    }
}
