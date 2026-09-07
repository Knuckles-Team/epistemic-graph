use super::*;

impl IsolationLayer {
    #[cfg(feature = "security")]
    pub fn try_bootstrap_system_identity(&mut self, identity: AgentIdentity) -> Result<(), String> {
        if !self.identity_bootstrap_pending() {
            return Err("ACCESS_DENIED: identity bootstrap is not pending".to_string());
        }
        if identity.agent_id.trim().is_empty()
            || !matches!(&identity.role, AgentRole::System)
            || !identity.teams.is_empty()
            || !identity.roles.is_empty()
        {
            return Err(
                "ACCESS_DENIED: bootstrap requires a non-empty System identity with no teams or roles"
                    .to_string(),
            );
        }
        let agent_id = identity.agent_id.clone();
        self.agents.insert(agent_id.clone(), identity);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.agents.remove(&agent_id);
            self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Pending;
            return Err(error);
        }
        Ok(())
    }

    #[cfg(not(feature = "security"))]
    pub fn try_bootstrap_system_identity(
        &mut self,
        _identity: AgentIdentity,
    ) -> Result<(), String> {
        Err("ACCESS_DENIED: identity bootstrap requires the security feature".to_string())
    }

    pub fn register_agent(&mut self, identity: AgentIdentity) {
        let _ = self.try_register_agent(identity);
    }

    pub fn try_register_agent(&mut self, identity: AgentIdentity) -> Result<(), String> {
        let agent_id = identity.agent_id.clone();
        let previous = self.agents.insert(agent_id.clone(), identity);
        #[cfg(feature = "security")]
        {
            let previous_bootstrap = self.identity_bootstrap;
            self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
            if let Err(error) = self.persist_state() {
                match previous {
                    Some(identity) => {
                        self.agents.insert(agent_id, identity);
                    }
                    None => {
                        self.agents.remove(&agent_id);
                    }
                }
                self.identity_bootstrap = previous_bootstrap;
                return Err(error);
            }
        }
        #[cfg(not(feature = "security"))]
        let _ = previous;
        Ok(())
    }

    pub fn try_register_agent_from_request(
        &mut self,
        identity: AgentIdentity,
    ) -> Result<(), String> {
        if self.identity_bootstrap_pending() {
            return Err(
                "ACCESS_DENIED: identity registration requires the dedicated bootstrap path"
                    .to_string(),
            );
        }
        if matches!(&identity.role, AgentRole::System) {
            return Err(
                "ACCESS_DENIED: System identities require the dedicated bootstrap path".to_string(),
            );
        }
        self.try_register_agent(identity)
    }

    pub fn unregister_agent(&mut self, agent_id: &str) {
        let _ = self.try_unregister_agent(agent_id);
    }

    pub fn try_unregister_agent(&mut self, agent_id: &str) -> Result<bool, String> {
        let previous = self.agents.remove(agent_id);
        let removed = previous.is_some();
        #[cfg(feature = "security")]
        if removed {
            if let Err(error) = self.persist_state() {
                if let Some(identity) = previous {
                    self.agents.insert(agent_id.to_string(), identity);
                }
                return Err(error);
            }
        }
        #[cfg(not(feature = "security"))]
        let _ = removed;
        Ok(removed)
    }
}
