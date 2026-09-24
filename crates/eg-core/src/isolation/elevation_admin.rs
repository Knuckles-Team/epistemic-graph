//! Just-in-time elevation transitions (EH-404) over the RBAC policy image.
//!
//! Each transition is applied to the in-memory ledger, written through with
//! the whole authorization image, and rolled back if the write fails -- the
//! same discipline as every other RBAC mutation in `policy_admin`. A refused
//! transition changes nothing, not even the ledger's expiry bookkeeping.
#![cfg(feature = "security")]

use eg_types::rbac_elevation::{
    ElevationActor, ElevationApproval, ElevationLease, ElevationLedger, ElevationRefusal,
    ElevationRequest, ElevationRevoke,
};

use super::IsolationLayer;

/// A refused elevation, or a persistence failure after an accepted one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElevationError {
    Refused(ElevationRefusal),
    Persist(String),
}

impl std::fmt::Display for ElevationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => refusal.fmt(f),
            Self::Persist(error) => f.write_str(error),
        }
    }
}

impl IsolationLayer {
    /// Record `actor`'s request. The requester must be a registered identity.
    pub fn try_request_elevation(
        &mut self,
        actor: &ElevationActor,
        request: &ElevationRequest,
        now_ms: u64,
    ) -> Result<ElevationLease, ElevationError> {
        self.require_registered(actor)?;
        self.apply_elevation(|ledger| ledger.request(actor, request, now_ms))
    }

    /// Approve a request. The approver must be a registered identity; the
    /// ledger enforces the two-person, direct-approval and replay rules.
    pub fn try_approve_elevation(
        &mut self,
        actor: &ElevationActor,
        approval: &ElevationApproval,
        now_ms: u64,
    ) -> Result<ElevationLease, ElevationError> {
        self.require_registered(actor)?;
        self.apply_elevation(|ledger| ledger.approve(actor, approval, now_ms))
    }

    /// Revoke an elevation now. Revocation only narrows, so it is not gated on
    /// registration: a de-registered grantee can still end its own lease.
    pub fn try_revoke_elevation(
        &mut self,
        actor: &ElevationActor,
        revoke: &ElevationRevoke,
        now_ms: u64,
    ) -> Result<ElevationLease, ElevationError> {
        self.apply_elevation(|ledger| ledger.revoke(actor, revoke, now_ms))
    }

    /// The elevations `actor` may see, with their status at `now_ms`.
    pub fn elevations_visible_to(
        &self,
        actor: &ElevationActor,
        now_ms: u64,
    ) -> Vec<ElevationLease> {
        self.rbac.elevations().visible_to(actor, now_ms)
    }

    fn require_registered(&self, actor: &ElevationActor) -> Result<(), ElevationError> {
        if self.agents.contains_key(&actor.agent_id) {
            Ok(())
        } else {
            Err(ElevationError::Refused(ElevationRefusal::UnknownActor))
        }
    }

    fn apply_elevation(
        &mut self,
        transition: impl FnOnce(&mut ElevationLedger) -> Result<ElevationLease, ElevationRefusal>,
    ) -> Result<ElevationLease, ElevationError> {
        let previous = self.rbac.clone();
        let lease = match transition(self.rbac.elevations_mut()) {
            Ok(lease) => lease,
            Err(refusal) => {
                self.rbac = previous;
                return Err(ElevationError::Refused(refusal));
            }
        };
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            return Err(ElevationError::Persist(error));
        }
        Ok(lease)
    }
}
