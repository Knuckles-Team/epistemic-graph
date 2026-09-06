use super::*;

impl IsolationLayer {
    #[cfg(feature = "security")]
    pub fn add_role(&mut self, role: crate::acl::Role) {
        let _ = self.try_add_role(role);
    }

    #[cfg(feature = "security")]
    pub fn try_add_role(&mut self, role: crate::acl::Role) -> Result<(), String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        self.rbac.add_role(role);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            self.identity_bootstrap = previous_bootstrap;
            return Err(error);
        }
        Ok(())
    }

    #[cfg(feature = "security")]
    pub fn remove_role(&mut self, name: &str) {
        let _ = self.try_remove_role(name);
    }

    #[cfg(feature = "security")]
    pub fn try_remove_role(&mut self, name: &str) -> Result<(), String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        self.rbac.remove_role(name);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            self.identity_bootstrap = previous_bootstrap;
            return Err(error);
        }
        Ok(())
    }

    #[cfg(feature = "security")]
    pub fn add_grant(&mut self, grant: crate::acl::Grant) {
        let _ = self.try_add_grant(grant);
    }

    #[cfg(feature = "security")]
    pub fn try_add_grant(&mut self, grant: crate::acl::Grant) -> Result<(), String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        self.rbac.add_grant(grant);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            self.identity_bootstrap = previous_bootstrap;
            return Err(error);
        }
        Ok(())
    }

    /// Provision the one tenant role and its read/write graph-pattern grants.
    #[cfg(feature = "security")]
    pub fn provision_tenant_graph_access(
        &mut self,
        graph_name: &str,
        creator_agent_id: Option<&str>,
    ) -> Result<(), String> {
        let Some(tenant_slug) = tenant_slug_from_graph_name(graph_name) else {
            return Ok(());
        };
        let role_name = format!("tenant:{tenant_slug}");
        let pattern = format!("tenant__{tenant_slug}__*");
        self.try_add_role(crate::acl::Role::new(role_name.clone()))?;
        self.try_add_grant(crate::acl::Grant {
            role: role_name.clone(),
            resource: crate::acl::ResourceSelector::Pattern(pattern.clone()),
            action: crate::acl::RbacAction::Read,
            effect: crate::acl::GrantEffect::Allow,
        })?;
        self.try_add_grant(crate::acl::Grant {
            role: role_name.clone(),
            resource: crate::acl::ResourceSelector::Pattern(pattern),
            action: crate::acl::RbacAction::Write,
            effect: crate::acl::GrantEffect::Allow,
        })?;
        if let Some(identity) = creator_agent_id.and_then(|id| self.agents.get(id)) {
            if !identity.roles.contains(&role_name) {
                let mut updated = identity.clone();
                updated.roles.push(role_name);
                self.try_register_agent(updated)?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "security")]
    pub fn remove_grant(&mut self, grant: &crate::acl::Grant) -> bool {
        self.try_remove_grant(grant).unwrap_or(false)
    }

    #[cfg(feature = "security")]
    pub fn try_remove_grant(&mut self, grant: &crate::acl::Grant) -> Result<bool, String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        let removed = self.rbac.remove_grant(grant);
        if removed {
            self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
            if let Err(error) = self.persist_state() {
                self.rbac = previous;
                self.identity_bootstrap = previous_bootstrap;
                return Err(error);
            }
        }
        Ok(removed)
    }
}
