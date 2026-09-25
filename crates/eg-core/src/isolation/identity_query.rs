use super::*;

impl IsolationLayer {
    #[cfg(feature = "security")]
    pub fn rbac(&self) -> &crate::rbac::RbacPolicy {
        &self.rbac
    }

    /// The engine's System identity has not bootstrapped yet, and the policy
    /// holds nothing but the identity store's SEED: initializing the store
    /// keeps the bootstrap open, while any real principal, credential or grant
    /// made through it (first-admin setup, a claim, SCIM/LDAP provisioning)
    /// closes it.
    #[cfg(feature = "security")]
    pub fn identity_bootstrap_pending(&self) -> bool {
        self.identity_bootstrap == crate::rbac_persist::IdentityBootstrapState::Pending
            && super::layer_store::holds_only_identity_seed(&self.rbac, self.agents.keys())
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
