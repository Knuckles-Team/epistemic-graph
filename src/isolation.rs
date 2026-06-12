// CONCEPT:KG-2.19 — Graph Access Control / Isolation Layer
//
// Enforces ACL rules for multi-tenant graph access:
// 1. Peer isolation: Agent graphs invisible to peer agents
// 2. Hierarchical access: Managers have full access to subordinate graphs
// 3. Commons is public: __commons__ readable/writable by all authenticated agents
// 4. Team scoping: Read for members, R/W for manager
// 5. Global read-only: System-managed, agent-readable

use crate::protocol::GraphType;
use std::collections::{HashMap, HashSet};

/// Access level for a graph operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLevel {
    Read,
    Write,
}

/// `AgentRole` / `AgentIdentity` are defined in `eg-types::acl` (the `protocol`
/// enum's `RegisterIdentity` carries `AgentRole` over the wire, and `protocol`
/// sits below `isolation` in the DAG); re-exported here so `IsolationLayer` and
/// every call site reference `crate::isolation::AgentRole` unchanged.
pub use crate::acl::{AgentIdentity, AgentRole};

/// Isolation policy engine.
pub struct IsolationLayer {
    /// Known agent identities for ACL resolution.
    agents: HashMap<String, AgentIdentity>,
}

impl Default for IsolationLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl IsolationLayer {
    pub fn new() -> Self {
        IsolationLayer {
            agents: HashMap::new(),
        }
    }

    /// Register or update an agent identity.
    pub fn register_agent(&mut self, identity: AgentIdentity) {
        self.agents.insert(identity.agent_id.clone(), identity);
    }

    /// Remove an agent identity.
    pub fn unregister_agent(&mut self, agent_id: &str) {
        self.agents.remove(agent_id);
    }

    /// True once any identity has been registered. While no rules exist the
    /// server skips ACL checks entirely (single-tenant back-compat); the first
    /// `RegisterIdentity` switches graph-targeted dispatch to enforcing mode.
    pub fn has_rules(&self) -> bool {
        !self.agents.is_empty()
    }

    /// Check if an agent has the requested access level to a graph.
    pub fn check_access(
        &self,
        agent_id: &str,
        graph_name: &str,
        graph_type: GraphType,
        graph_owner: Option<&str>,
        access: AccessLevel,
    ) -> bool {
        // System agents bypass all checks.
        if let Some(identity) = self.agents.get(agent_id) {
            if identity.role == AgentRole::System {
                return true;
            }
        }

        match graph_type {
            // Bus: all authenticated agents have full access.
            GraphType::Commons => true,

            // Global: read-only for all agents.
            GraphType::Global => access == AccessLevel::Read,

            // Agent graph: owner has full access, manager of owner has full access,
            // all others denied.
            GraphType::Agent => {
                // Owner always has access.
                if graph_owner == Some(agent_id) {
                    return true;
                }
                // Check if requester is a manager of the owner.
                if let Some(owner_id) = graph_owner {
                    if self.is_manager_of(agent_id, owner_id) {
                        return true;
                    }
                }
                false
            }

            // Team graph: members read, manager R/W.
            GraphType::Team => {
                let team_name = graph_name.strip_prefix("team:").unwrap_or(graph_name);
                let identity = match self.agents.get(agent_id) {
                    Some(id) => id,
                    None => return false,
                };

                // Check membership.
                let is_member = identity.teams.contains(&team_name.to_string());
                if !is_member {
                    return false;
                }

                match access {
                    AccessLevel::Read => true,
                    AccessLevel::Write => {
                        // Only managers can write to team graphs.
                        matches!(identity.role, AgentRole::Manager { .. })
                    }
                }
            }
        }
    }

    /// Check if `agent_id` is a manager of `subordinate_id`.
    fn is_manager_of(&self, agent_id: &str, subordinate_id: &str) -> bool {
        if let Some(identity) = self.agents.get(agent_id) {
            if let AgentRole::Manager { subordinates } = &identity.role {
                return subordinates.contains(&subordinate_id.to_string());
            }
        }
        false
    }

    /// Get all agent IDs that a given agent can access.
    pub fn accessible_graphs(&self, agent_id: &str) -> HashSet<String> {
        let mut accessible = HashSet::new();
        accessible.insert("__commons__".to_string());

        if let Some(identity) = self.agents.get(agent_id) {
            // Own agent graph.
            accessible.insert(format!("agent:{}", agent_id));

            // Team graphs (read).
            for team in &identity.teams {
                accessible.insert(format!("team:{}", team));
            }

            // Subordinate graphs (if manager).
            if let AgentRole::Manager { subordinates } = &identity.role {
                for sub in subordinates {
                    accessible.insert(format!("agent:{}", sub));
                }
            }
        }

        accessible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> IsolationLayer {
        let mut layer = IsolationLayer::new();
        layer.register_agent(AgentIdentity {
            agent_id: "manager".to_string(),
            role: AgentRole::Manager {
                subordinates: vec!["worker1".to_string(), "worker2".to_string()],
            },
            teams: vec!["alpha".to_string()],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "worker1".to_string(),
            role: AgentRole::Agent,
            teams: vec!["alpha".to_string()],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "worker2".to_string(),
            role: AgentRole::Agent,
            teams: vec!["alpha".to_string()],
        });
        layer
    }

    #[test]
    fn test_bus_access_for_all() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "__commons__",
            GraphType::Commons,
            None,
            AccessLevel::Write
        ));
        assert!(layer.check_access(
            "manager",
            "__commons__",
            GraphType::Commons,
            None,
            AccessLevel::Read
        ));
    }

    #[test]
    fn test_agent_graph_owner_access() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "agent:worker1",
            GraphType::Agent,
            Some("worker1"),
            AccessLevel::Write
        ));
    }

    #[test]
    fn test_agent_graph_peer_denied() {
        let layer = setup();
        assert!(!layer.check_access(
            "worker2",
            "agent:worker1",
            GraphType::Agent,
            Some("worker1"),
            AccessLevel::Read
        ));
    }

    #[test]
    fn test_manager_access_to_subordinate() {
        let layer = setup();
        assert!(layer.check_access(
            "manager",
            "agent:worker1",
            GraphType::Agent,
            Some("worker1"),
            AccessLevel::Write
        ));
    }

    #[test]
    fn test_team_member_read_only() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "team:alpha",
            GraphType::Team,
            None,
            AccessLevel::Read
        ));
        assert!(!layer.check_access(
            "worker1",
            "team:alpha",
            GraphType::Team,
            None,
            AccessLevel::Write
        ));
    }

    #[test]
    fn test_team_manager_can_write() {
        let layer = setup();
        assert!(layer.check_access(
            "manager",
            "team:alpha",
            GraphType::Team,
            None,
            AccessLevel::Write
        ));
    }

    #[test]
    fn test_global_read_only() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "global:ontology",
            GraphType::Global,
            None,
            AccessLevel::Read
        ));
        assert!(!layer.check_access(
            "worker1",
            "global:ontology",
            GraphType::Global,
            None,
            AccessLevel::Write
        ));
    }
}
