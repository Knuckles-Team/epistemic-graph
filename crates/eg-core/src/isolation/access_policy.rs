use super::*;
#[cfg(feature = "security")]
use std::collections::HashSet;

impl IsolationLayer {
    pub fn check_access(
        &self,
        agent_id: &str,
        graph_name: &str,
        graph_type: GraphType,
        graph_owner: Option<&str>,
        access: AccessLevel,
    ) -> bool {
        let Some(identity) = self.agents.get(agent_id) else {
            return false;
        };
        identity.role == AgentRole::System
            || self.check_non_system_access(
                identity,
                agent_id,
                graph_name,
                graph_type,
                graph_owner,
                access,
            )
    }

    #[cfg(feature = "security")]
    fn check_non_system_access(
        &self,
        identity: &AgentIdentity,
        _agent_id: &str,
        graph_name: &str,
        _graph_type: GraphType,
        _graph_owner: Option<&str>,
        access: AccessLevel,
    ) -> bool {
        // Mandatory RBAC means no pre-RBAC ACL fall-through on empty/no-match.
        let context = crate::acl::ResourceContext::graph(graph_name);
        let action = match access {
            AccessLevel::Read => crate::acl::RbacAction::Read,
            AccessLevel::Write => crate::acl::RbacAction::Write,
        };
        matches!(
            self.rbac.evaluate(&identity.roles, &context, action),
            Some(crate::acl::GrantEffect::Allow)
        )
    }

    #[cfg(not(feature = "security"))]
    fn check_non_system_access(
        &self,
        identity: &AgentIdentity,
        agent_id: &str,
        graph_name: &str,
        graph_type: GraphType,
        graph_owner: Option<&str>,
        access: AccessLevel,
    ) -> bool {
        match graph_type {
            GraphType::Commons => true,
            GraphType::Global => access == AccessLevel::Read,
            GraphType::Agent => {
                graph_owner == Some(agent_id)
                    || graph_owner.is_some_and(|owner| self.is_manager_of(agent_id, owner))
            }
            GraphType::Team => {
                let team_name = graph_name.strip_prefix("team:").unwrap_or(graph_name);
                identity.teams.contains(&team_name.to_string())
                    && (access == AccessLevel::Read
                        || matches!(identity.role, AgentRole::Manager { .. }))
            }
        }
    }

    fn is_manager_of(&self, agent_id: &str, subordinate_id: &str) -> bool {
        self.agents.get(agent_id).is_some_and(|identity| {
            matches!(
                &identity.role,
                AgentRole::Manager { subordinates }
                    if subordinates.iter().any(|id| id == subordinate_id)
            )
        })
    }

    #[cfg(feature = "security")]
    pub fn is_system(&self, agent_id: &str) -> bool {
        self.agents
            .get(agent_id)
            .is_some_and(|identity| identity.role == AgentRole::System)
    }

    #[cfg(feature = "security")]
    pub fn can_see_row(&self, agent_id: &str, visibility: &RowVisibility) -> bool {
        let Some(owner) = visibility.owner.as_deref() else {
            return self.is_system(agent_id)
                || visibility.schema
                || (visibility.tagged && visibility.public);
        };
        self.is_system(agent_id)
            || visibility.schema
            || visibility.public
            || owner == agent_id
            || visibility.grants.iter().any(|grant| grant == agent_id)
            || self.is_manager_of(agent_id, owner)
    }

    #[cfg(feature = "security")]
    pub fn can_see_node(&self, agent_id: &str, view: &crate::graph::GraphView, id: &str) -> bool {
        let mut visibility = view
            .visibility_index
            .get(id)
            .cloned()
            .or_else(|| {
                view.node_properties
                    .get(id)
                    .map(|blob| row_visibility(blob))
            })
            .unwrap_or_else(RowVisibility::default_public);
        visibility.schema = view.schema_node_ids.contains(id);
        self.can_see_row(agent_id, &visibility)
    }

    #[cfg(feature = "security")]
    pub fn filter_view(&self, agent_id: &str, view: &mut crate::graph::GraphView) {
        let hidden: HashSet<String> = view
            .node_map
            .keys()
            .filter(|id| !self.can_see_node(agent_id, view, id))
            .cloned()
            .collect();
        for id in &hidden {
            if let Some(index) = view.node_map.remove(id) {
                view.graph.remove_node(index);
            }
            view.node_properties.remove(id);
        }
        view.edge_properties
            .retain(|(source, target), _| !hidden.contains(source) && !hidden.contains(target));
    }

    pub fn has_admin_capability(&self, agent_id: &str) -> bool {
        let Some(identity) = self.agents.get(agent_id) else {
            return false;
        };
        if identity.role == AgentRole::System {
            return true;
        }
        #[cfg(feature = "security")]
        {
            let context = crate::acl::ResourceContext::graph("__admin__");
            matches!(
                self.rbac
                    .evaluate(&identity.roles, &context, crate::acl::RbacAction::Admin,),
                Some(crate::acl::GrantEffect::Allow)
            )
        }
        #[cfg(not(feature = "security"))]
        false
    }
}
