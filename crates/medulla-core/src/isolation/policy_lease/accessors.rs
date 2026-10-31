use super::{AccessLevel, PolicyDecisionBasis, PolicyDecisionLease, PolicySnapshot};

impl PolicyDecisionLease {
    pub fn access(&self) -> AccessLevel {
        self.access
    }

    pub fn originating_principal(&self) -> &str {
        &self.originating_principal
    }

    pub fn effective_actor(&self) -> &str {
        &self.effective_actor
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn resource(&self) -> &str {
        &self.resource
    }

    pub fn policy_snapshot(&self) -> &PolicySnapshot {
        &self.policy_snapshot
    }

    pub fn decision_basis(&self) -> PolicyDecisionBasis {
        self.decision_basis
    }

    pub fn row_policy_identity_digest(&self) -> &str {
        &self.row_policy_identity_digest
    }
}
