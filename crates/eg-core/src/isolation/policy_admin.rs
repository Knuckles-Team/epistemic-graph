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

    /// Atomically add the tenant role to an ordinary identity.
    ///
    /// The role and both graph grants must already exist in the authoritative
    /// RBAC policy. Unlike a caller-side GetIdentity/RegisterIdentity pair,
    /// this reads and updates the complete identity under one isolation lock
    /// and one durable policy save, preserving unrelated teams and roles. An
    /// absent identity is created from the explicit initial role and teams.
    #[cfg(feature = "security")]
    pub fn try_admit_tenant_principal(
        &mut self,
        agent_id: &str,
        tenant_slug: &str,
        initial_role: AgentRole,
        initial_teams: Vec<String>,
    ) -> Result<bool, String> {
        if agent_id.trim().is_empty()
            || tenant_slug.is_empty()
            || !tenant_slug.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || byte == b'_'
                    || byte == b'-'
                    || byte == b'.'
            })
        {
            return Err("ACCESS_DENIED: invalid tenant admission target".to_string());
        }
        if matches!(&initial_role, AgentRole::System)
            || initial_teams.iter().any(|team| team.trim().is_empty())
            || initial_teams
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != initial_teams.len()
        {
            return Err("ACCESS_DENIED: invalid initial tenant identity".to_string());
        }
        let role_name = format!("tenant:{tenant_slug}");
        let pattern = crate::acl::ResourceSelector::Pattern(format!("tenant__{tenant_slug}__*"));
        let has_grant = |action| {
            self.rbac.grants().iter().any(|grant| {
                grant.role == role_name
                    && grant.resource == pattern
                    && grant.action == action
                    && grant.effect == crate::acl::GrantEffect::Allow
            })
        };
        if !self.rbac.roles().any(|role| role.name == role_name)
            || !has_grant(crate::acl::RbacAction::Read)
            || !has_grant(crate::acl::RbacAction::Write)
        {
            return Err("ACCESS_DENIED: tenant role is not provisioned".to_string());
        }
        let identity = self
            .agents
            .get(agent_id)
            .cloned()
            .unwrap_or_else(|| AgentIdentity {
                agent_id: agent_id.to_string(),
                role: initial_role,
                teams: initial_teams,
                roles: Vec::new(),
            });
        if matches!(&identity.role, AgentRole::System) {
            return Err(
                "ACCESS_DENIED: System identity is outside tenant role admission".to_string(),
            );
        }
        if identity.roles.contains(&role_name) {
            return Ok(false);
        }
        let mut updated = identity;
        updated.roles.push(role_name);
        self.try_register_agent(updated)?;
        Ok(true)
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

#[cfg(all(test, feature = "security"))]
mod tenant_admission_tests {
    use super::*;

    #[test]
    fn atomic_admission_preserves_existing_identity_and_is_idempotent() {
        let mut layer = IsolationLayer::new();
        layer
            .try_register_agent(AgentIdentity {
                agent_id: "creator".into(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec![],
            })
            .unwrap();
        layer
            .provision_tenant_graph_access("tenant__acme__default", Some("creator"))
            .unwrap();
        let role = AgentRole::Manager {
            subordinates: vec!["worker".into()],
        };
        layer
            .try_register_agent(AgentIdentity {
                agent_id: "reader".into(),
                role: role.clone(),
                teams: vec!["support".into()],
                roles: vec!["code-reader".into()],
            })
            .unwrap();

        assert_eq!(
            layer.try_admit_tenant_principal("reader", "acme", AgentRole::Agent, vec![]),
            Ok(true)
        );
        assert_eq!(
            layer.try_admit_tenant_principal("reader", "acme", AgentRole::Agent, vec![]),
            Ok(false)
        );
        let identity = layer.get_identity("reader").unwrap();
        assert_eq!(identity.role, role);
        assert_eq!(identity.teams, vec!["support"]);
        assert_eq!(identity.roles, vec!["code-reader", "tenant:acme"]);
    }

    #[test]
    fn admission_refuses_unprovisioned_role_and_creates_absent_identity_atomically() {
        let mut layer = IsolationLayer::new();
        layer
            .try_register_agent(AgentIdentity {
                agent_id: "reader".into(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec!["code-reader".into()],
            })
            .unwrap();
        assert!(layer
            .try_admit_tenant_principal("reader", "acme", AgentRole::Agent, vec![])
            .is_err());
        layer
            .provision_tenant_graph_access("tenant__acme__default", None)
            .unwrap();
        assert_eq!(
            layer.try_admit_tenant_principal(
                "new",
                "acme",
                AgentRole::Agent,
                vec!["engineering".into()],
            ),
            Ok(true)
        );
        let created = layer.get_identity("new").unwrap();
        assert_eq!(created.teams, vec!["engineering"]);
        assert_eq!(created.roles, vec!["tenant:acme"]);
        assert_eq!(
            layer.get_identity("reader").unwrap().roles,
            vec!["code-reader"]
        );
    }

    #[test]
    fn admission_refuses_system_and_noncanonical_tenant_slug() {
        let mut layer = IsolationLayer::new();
        layer
            .try_register_agent(AgentIdentity {
                agent_id: "root".into(),
                role: AgentRole::System,
                teams: vec![],
                roles: vec![],
            })
            .unwrap();
        layer
            .provision_tenant_graph_access("tenant__acme__default", None)
            .unwrap();
        assert!(layer
            .try_admit_tenant_principal("root", "acme", AgentRole::Agent, vec![])
            .is_err());
        assert!(layer
            .try_admit_tenant_principal("root", "Acme", AgentRole::Agent, vec![])
            .is_err());
        assert!(layer
            .try_admit_tenant_principal("root", "acme:*", AgentRole::Agent, vec![])
            .is_err());
        assert!(layer
            .try_admit_tenant_principal("new", "acme", AgentRole::System, vec![])
            .is_err());
    }

    #[test]
    fn admission_accepts_canonical_dotted_graph_slug() {
        let mut layer = IsolationLayer::new();
        layer
            .provision_tenant_graph_access("tenant__acme.io__default", None)
            .unwrap();
        assert_eq!(
            layer.try_admit_tenant_principal("reader", "acme.io", AgentRole::Agent, vec![]),
            Ok(true)
        );
        assert_eq!(
            layer.get_identity("reader").unwrap().roles,
            vec!["tenant:acme.io"]
        );
        layer
            .provision_tenant_graph_access("tenant__-lab__default", None)
            .unwrap();
        assert_eq!(
            layer.try_admit_tenant_principal("reader", "-lab", AgentRole::Agent, vec![]),
            Ok(true)
        );
        assert_eq!(
            layer.get_identity("reader").unwrap().roles,
            vec!["tenant:acme.io", "tenant:-lab"]
        );
    }
}
