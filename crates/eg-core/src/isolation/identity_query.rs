use super::*;

impl IsolationLayer {
    #[cfg(feature = "security")]
    pub fn rbac(&self) -> &crate::rbac::RbacPolicy {
        &self.rbac
    }

    #[cfg(feature = "security")]
    pub fn identity_bootstrap_pending(&self) -> bool {
        self.identity_bootstrap == crate::rbac_persist::IdentityBootstrapState::Pending
            && self.agents.is_empty()
            && self.rbac.roles().next().is_none()
            && self.rbac.grants().is_empty()
    }

    #[cfg(not(feature = "security"))]
    pub fn identity_bootstrap_pending(&self) -> bool {
        false
    }

    pub fn has_rules(&self) -> bool {
        !self.agents.is_empty()
    }

    pub fn is_registered(&self, agent_id: &str) -> bool {
        self.agents.contains_key(agent_id)
    }

    pub fn get_identity(&self, agent_id: &str) -> Option<AgentIdentity> {
        self.agents.get(agent_id).cloned()
    }
}
