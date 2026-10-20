//! Immutable evidence of a coordinator decision, bound to one complete intent.

use serde::{Deserialize, Serialize};

/// Inputs fixed before the participating owners prepare their state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorDecisionBinding {
    pub transaction_id: String,
    pub tenant: String,
    pub coordinator_scope: String,
    pub request_digest: [u8; 32],
    pub authority_deadline_ms: u64,
}

impl CoordinatorDecisionBinding {
    pub fn validate(&self) -> Result<(), String> {
        for value in [&self.transaction_id, &self.tenant, &self.coordinator_scope] {
            if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
                return Err("invalid coordinator decision identity".into());
            }
        }
        if self.request_digest == [0; 32] || self.authority_deadline_ms == 0 {
            return Err("missing coordinator decision intent or deadline".into());
        }
        Ok(())
    }
}

/// The storage owner mints this only beside the durable terminal outcome.
/// Decoding this value alone is not proof: consumers must read their trusted owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorDecisionCertificate {
    pub version: u8,
    pub binding: CoordinatorDecisionBinding,
    pub commit: bool,
    pub decided_at_ms: u64,
}

impl CoordinatorDecisionCertificate {
    pub fn validate(&self) -> Result<(), String> {
        self.binding.validate()?;
        if self.version != 1 || self.decided_at_ms == 0 {
            return Err("invalid coordinator decision certificate".into());
        }
        if self.commit && self.decided_at_ms >= self.binding.authority_deadline_ms {
            return Err("coordinator decision authority expired".into());
        }
        Ok(())
    }

    pub fn matches(
        &self,
        binding: &CoordinatorDecisionBinding,
        commit: bool,
    ) -> Result<(), String> {
        self.validate()?;
        binding.validate()?;
        if &self.binding != binding || self.commit != commit {
            return Err("coordinator decision intent conflict".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn certificate(commit: bool, decided_at_ms: u64) -> CoordinatorDecisionCertificate {
        CoordinatorDecisionCertificate {
            version: 1,
            binding: CoordinatorDecisionBinding {
                transaction_id: "txn".into(),
                tenant: "tenant".into(),
                coordinator_scope: "graph-incarnation".into(),
                request_digest: [1; 32],
                authority_deadline_ms: 100,
            },
            commit,
            decided_at_ms,
        }
    }

    #[test]
    fn expiry_is_exclusive_and_abort_can_resolve_after_expiry() {
        assert!(certificate(true, 99).validate().is_ok());
        assert!(certificate(true, 100).validate().is_err());
        assert!(certificate(false, 101).validate().is_ok());
    }

    #[test]
    fn replay_binds_all_intent_fields_and_original_timestamp() {
        let proof = certificate(true, 99);
        assert!(proof.matches(&proof.binding, true).is_ok());
        let mut changed = proof.binding.clone();
        changed.request_digest = [2; 32];
        assert!(proof.matches(&changed, true).is_err());
        changed = proof.binding.clone();
        changed.authority_deadline_ms += 1;
        assert!(proof.matches(&changed, true).is_err());
        assert!(proof.matches(&proof.binding, false).is_err());
    }
}
