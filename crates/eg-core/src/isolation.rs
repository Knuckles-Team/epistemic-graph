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

/// Per-row owner/visibility derived from a node's property blob (CONCEPT:KG-2.231).
/// The reserved property keys (`_owner` / `_visibility` / `_grants`) form the RLS
/// convention enforced by [`IsolationLayer::filter_view`].
#[cfg(feature = "security")]
#[derive(Debug, Clone, Default)]
pub struct RowVisibility {
    /// Owning agent_id (`_owner`); `None` ⇒ unowned ⇒ visible to all.
    pub owner: Option<String>,
    /// `true` when `_visibility` is absent OR `"public"`; `false` for `"private"`.
    pub public: bool,
    /// Agent_ids explicitly granted read (`_grants`, comma-separated).
    pub grants: Vec<String>,
}

/// Reserved RLS property keys.
#[cfg(feature = "security")]
pub const RLS_OWNER_KEY: &str = "_owner";
#[cfg(feature = "security")]
pub const RLS_VISIBILITY_KEY: &str = "_visibility";
#[cfg(feature = "security")]
pub const RLS_GRANTS_KEY: &str = "_grants";

/// Parse a node's msgpack property blob into its [`RowVisibility`]. A blob that
/// can't be decoded as a string-keyed map (or lacks the keys) is treated as an
/// unowned PUBLIC row — RLS only ever HIDES rows that explicitly mark themselves
/// owned+private, so undecodable/legacy data is never accidentally hidden.
#[cfg(feature = "security")]
pub fn row_visibility(blob: &[u8]) -> RowVisibility {
    use serde_json::Value;
    let map: std::collections::BTreeMap<String, Value> = match rmp_serde::from_slice(blob) {
        Ok(m) => m,
        Err(_) => return RowVisibility::default_public(),
    };
    let owner = map
        .get(RLS_OWNER_KEY)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let public = match map.get(RLS_VISIBILITY_KEY).and_then(|v| v.as_str()) {
        Some(v) => !v.eq_ignore_ascii_case("private"),
        None => true,
    };
    let grants = map
        .get(RLS_GRANTS_KEY)
        .and_then(|v| v.as_str())
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    RowVisibility {
        owner,
        public,
        grants,
    }
}

#[cfg(feature = "security")]
impl RowVisibility {
    fn default_public() -> Self {
        RowVisibility {
            owner: None,
            public: true,
            grants: Vec::new(),
        }
    }
}

/// `AgentRole` / `AgentIdentity` are defined in `eg-types::acl` (the `protocol`
/// enum's `RegisterIdentity` carries `AgentRole` over the wire, and `protocol`
/// sits below `isolation` in the DAG); re-exported here so `IsolationLayer` and
/// every call site reference `crate::isolation::AgentRole` unchanged.
pub use crate::acl::{AgentIdentity, AgentRole};

/// Isolation policy engine.
#[derive(Clone)]
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

    /// Per-agent Row-Level Security (CONCEPT:KG-2.231): may `agent_id` SEE one
    /// node, given that node's owner + visibility convention?
    ///
    /// Visibility convention (carried in the node's property blob; read by
    /// [`row_visibility`]):
    /// * `_owner`      — the owning agent_id (absent ⇒ unowned/legacy ⇒ visible to all).
    /// * `_visibility` — `"public"` (default when absent) or `"private"`.
    /// * `_grants`     — optional comma-separated agent_ids explicitly granted read.
    ///
    /// Decision: an agent may see a row when ANY holds —
    /// 1. it is unowned (no `_owner`), 2. it is public, 3. the agent IS the owner,
    /// 4. the agent is explicitly granted, 5. the agent is a manager of the owner,
    /// 6. the agent is a `System` role. Otherwise the row is hidden.
    #[cfg(feature = "security")]
    pub fn can_see_row(&self, agent_id: &str, vis: &RowVisibility) -> bool {
        // System role sees everything.
        if let Some(identity) = self.agents.get(agent_id) {
            if identity.role == AgentRole::System {
                return true;
            }
        }
        let owner = match &vis.owner {
            // Unowned row — visible to all (legacy/shared data).
            None => return true,
            Some(o) => o.as_str(),
        };
        if vis.public {
            return true;
        }
        if owner == agent_id {
            return true;
        }
        if vis.grants.iter().any(|g| g == agent_id) {
            return true;
        }
        if self.is_manager_of(agent_id, owner) {
            return true;
        }
        false
    }

    /// Filter a [`GraphView`](crate::graph::GraphView) IN-PLACE down to only the
    /// rows `agent_id` may see (CONCEPT:KG-2.231 — RLS in the read/plan path).
    ///
    /// This runs on the owned, off-lock snapshot the query planner (SQL / Cypher /
    /// SPARQL / unified) consumes — NOT at the graph boundary — so NO query surface
    /// can exfiltrate a forbidden row: a hidden node is removed from the view's
    /// topology, node-map, and property map, and every edge incident to a removed
    /// node is dropped too (an edge to an invisible node would otherwise leak its
    /// existence). When the layer has no registered identities the filter is a
    /// no-op (single-tenant back-compat, matching `check_access`).
    #[cfg(feature = "security")]
    pub fn filter_view(&self, agent_id: &str, view: &mut crate::graph::GraphView) {
        if !self.has_rules() {
            return;
        }
        // Decide visibility per node from its property blob.
        let hidden: HashSet<String> = view
            .node_properties
            .iter()
            .filter_map(|(id, blob)| {
                let vis = row_visibility(blob);
                if self.can_see_row(agent_id, &vis) {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .collect();
        // A node present in topology but with NO property blob is unowned ⇒ visible;
        // so `hidden` is exactly the set to drop.
        if hidden.is_empty() {
            return;
        }
        // Drop hidden nodes from the petgraph topology (StableDiGraph keeps other
        // indices valid) + the node_map + node_properties.
        for id in &hidden {
            if let Some(idx) = view.node_map.remove(id) {
                view.graph.remove_node(idx);
            }
            view.node_properties.remove(id);
        }
        // Drop any edge touching a hidden endpoint (do not leak its existence).
        view.edge_properties
            .retain(|(s, t), _| !hidden.contains(s) && !hidden.contains(t));
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

    // ── Per-agent Row-Level Security (CONCEPT:KG-2.231) ──────────────────
    #[cfg(feature = "security")]
    mod rls {
        use super::*;
        use crate::graph::GraphView;

        /// Build a node property blob from a list of (key,value) string pairs.
        fn props(pairs: &[(&str, &str)]) -> std::sync::Arc<Vec<u8>> {
            let map: std::collections::BTreeMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            std::sync::Arc::new(rmp_serde::to_vec_named(&map).unwrap())
        }

        /// A 3-node view: B's private node owned by worker2, a public node, an
        /// unowned node. (Topology only carries the node ids; properties carry RLS.)
        fn view() -> GraphView {
            let mut v = GraphView::default();
            for id in ["b_private", "shared_public", "legacy_unowned"] {
                let idx = v.graph.add_node(id.to_string());
                v.node_map.insert(id.to_string(), idx);
            }
            v.node_properties.insert(
                "b_private".to_string(),
                props(&[("_owner", "worker2"), ("_visibility", "private")]),
            );
            v.node_properties.insert(
                "shared_public".to_string(),
                props(&[("_owner", "worker2"), ("_visibility", "public")]),
            );
            v.node_properties
                .insert("legacy_unowned".to_string(), props(&[("name", "x")]));
            // An edge from B's private node to the public node — must be dropped for A.
            v.edge_properties
                .insert(("b_private".into(), "shared_public".into()), vec![]);
            v
        }

        #[test]
        fn agent_a_cannot_see_agent_b_private_node() {
            let layer = setup();
            let mut va = view();
            layer.filter_view("worker1", &mut va);
            // worker1 sees the PUBLIC node + the UNOWNED node, NOT worker2's private one.
            assert!(!va.node_properties.contains_key("b_private"));
            assert!(va.node_properties.contains_key("shared_public"));
            assert!(va.node_properties.contains_key("legacy_unowned"));
            assert!(va.node_map.get("b_private").is_none());
            // The edge touching the hidden node is dropped (no existence leak).
            assert!(!va
                .edge_properties
                .contains_key(&("b_private".to_string(), "shared_public".to_string())));
        }

        #[test]
        fn owner_b_sees_own_private_node() {
            let layer = setup();
            let mut vb = view();
            layer.filter_view("worker2", &mut vb);
            assert!(vb.node_properties.contains_key("b_private"));
            assert!(vb.node_properties.contains_key("shared_public"));
        }

        #[test]
        fn manager_sees_subordinate_private_node() {
            let layer = setup();
            let mut vm = view();
            layer.filter_view("manager", &mut vm);
            // manager manages worker2 ⇒ sees its private node.
            assert!(vm.node_properties.contains_key("b_private"));
        }

        #[test]
        fn explicit_grant_is_honored() {
            let mut layer = setup();
            layer.register_agent(AgentIdentity {
                agent_id: "auditor".to_string(),
                role: AgentRole::Agent,
                teams: vec![],
            });
            let mut v = GraphView::default();
            let idx = v.graph.add_node("g".to_string());
            v.node_map.insert("g".to_string(), idx);
            v.node_properties.insert(
                "g".to_string(),
                props(&[
                    ("_owner", "worker2"),
                    ("_visibility", "private"),
                    ("_grants", "auditor, someone_else"),
                ]),
            );
            layer.filter_view("auditor", &mut v);
            assert!(v.node_properties.contains_key("g"), "grant ignored");
        }

        #[test]
        fn no_rules_is_noop() {
            let layer = IsolationLayer::new(); // no identities
            let mut v = view();
            let before = v.node_properties.len();
            layer.filter_view("anyone", &mut v);
            assert_eq!(v.node_properties.len(), before);
        }
    }
}
