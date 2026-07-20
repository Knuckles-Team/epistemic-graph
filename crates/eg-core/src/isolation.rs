// CONCEPT:EG-KG.txn.access-control-isolation — Graph Access Control / Isolation Layer
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

/// Per-row owner/visibility derived from a node's property blob (CONCEPT:EG-KG.sharding.row-level-security).
/// The reserved property keys (`_owner` / `_visibility` / `_grants`) form the RLS
/// convention enforced by [`IsolationLayer::filter_view`].
#[cfg(feature = "security")]
#[derive(Debug, Clone, Default)]
pub struct RowVisibility {
    /// Owning agent_id (`_owner`); `None` requires both [`Self::tagged`] and
    /// [`Self::public`] under the default-deny posture.
    pub owner: Option<String>,
    /// `true` when `_visibility` is absent OR `"public"`; `false` for `"private"`.
    pub public: bool,
    /// Agent_ids explicitly granted read (`_grants`, comma-separated).
    pub grants: Vec<String>,
    /// Whether this row carried ANY explicit RLS metadata at all — i.e. the decoded
    /// property blob contained at least one of `_owner` / `_visibility` / `_grants`.
    /// `false` for an undecodable blob OR a blob that decoded fine but declares none
    /// of the three keys. Untagged rows fail the default-deny decision.
    pub tagged: bool,
}

/// Reserved RLS property keys.
#[cfg(feature = "security")]
pub const RLS_OWNER_KEY: &str = "_owner";
#[cfg(feature = "security")]
pub const RLS_VISIBILITY_KEY: &str = "_visibility";
#[cfg(feature = "security")]
pub const RLS_GRANTS_KEY: &str = "_grants";

/// Parse a node's msgpack property blob into its [`RowVisibility`].
///
/// A blob that cannot be decoded as a string-keyed map, or that declares none of
/// the RLS keys, yields `tagged = false` and is denied. Explicit ownership,
/// visibility, and grants remain the only row authorization inputs.
#[cfg(feature = "security")]
pub fn row_visibility(blob: &[u8]) -> RowVisibility {
    use serde_json::Value;
    let map: std::collections::BTreeMap<String, Value> = match eg_types::msgpack::decode_bounded(
        blob,
        eg_types::msgpack::MsgpackLimits::new(
            eg_types::msgpack::MAX_PROPERTY_BYTES,
            eg_types::msgpack::MAX_PROPERTY_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    ) {
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
    let tagged = map.contains_key(RLS_OWNER_KEY)
        || map.contains_key(RLS_VISIBILITY_KEY)
        || map.contains_key(RLS_GRANTS_KEY);
    RowVisibility {
        owner,
        public,
        grants,
        tagged,
    }
}

#[cfg(feature = "security")]
impl RowVisibility {
    fn default_public() -> Self {
        RowVisibility {
            owner: None,
            public: true,
            grants: Vec::new(),
            tagged: false,
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
    /// RBAC policy (CONCEPT:EG-KG.compute.feature): the mandatory authorization
    /// decision for every non-System graph access. An empty policy denies all.
    #[cfg(feature = "security")]
    rbac: crate::rbac::RbacPolicy,
    /// Durable one-time bootstrap lifecycle. This is independent of the current
    /// identity count so deleting identities can never reopen first-run authority.
    #[cfg(feature = "security")]
    identity_bootstrap: crate::rbac_persist::IdentityBootstrapState,
    /// RBAC/identity persistence handle
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Current layers always
    /// carry a store: embedded layers use the atomic memory store and served layers
    /// load redb. `Arc` keeps [`IsolationLayer`] `Clone`.
    #[cfg(feature = "security")]
    persist: Option<std::sync::Arc<dyn crate::rbac_persist::RbacPolicyStore>>,
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
            #[cfg(feature = "security")]
            rbac: crate::rbac::RbacPolicy::new(),
            #[cfg(feature = "security")]
            identity_bootstrap: crate::rbac_persist::IdentityBootstrapState::Pending,
            #[cfg(feature = "security")]
            persist: Some(std::sync::Arc::new(
                crate::rbac_persist::MemoryRbacStore::new(),
            )),
        }
    }

    /// Open an [`IsolationLayer`] backed by a durable redb store at `dir`
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Any previously-persisted RBAC policy + registered agent
    /// identities are LOADED at boot; every subsequent `add_role`/`remove_role`/
    /// `add_grant`/`remove_grant`/`register_agent`/`unregister_agent` mutation is
    /// written through to redb. A new store carries an explicit current empty,
    /// default-deny bootstrap image; a partial image fails boot.
    #[cfg(feature = "security")]
    pub fn with_persist_dir<P: AsRef<std::path::Path>>(
        dir: P,
    ) -> Result<Self, crate::rbac_persist::RbacPersistError> {
        let store = crate::rbac_persist::RbacStore::open(dir)?;
        let (rbac, identities, identity_bootstrap) = store.load()?;
        if identity_bootstrap == crate::rbac_persist::IdentityBootstrapState::Pending
            && (!identities.is_empty()
                || rbac.roles().next().is_some()
                || !rbac.grants().is_empty())
        {
            return Err(crate::rbac_persist::RbacPersistError::IncompleteState(
                "pending identity bootstrap requires an empty policy and identity map",
            ));
        }
        let agents: HashMap<String, AgentIdentity> = identities.into_iter().collect();
        Ok(IsolationLayer {
            agents,
            rbac,
            identity_bootstrap,
            persist: Some(std::sync::Arc::new(store)),
        })
    }

    /// Write through the FULL RBAC state (policy + identities) to the configured
    /// store. Absence is an invalid internal state. Secure request handlers
    /// use the fallible `try_*` mutation methods below and roll in-memory state
    /// back if this save fails, so policy changes are never acknowledged unless
    /// their durable representation committed.
    ///
    /// [`with_persist_dir`]: IsolationLayer::with_persist_dir
    #[cfg(feature = "security")]
    fn persist_state(&self) -> Result<(), String> {
        let store = self
            .persist
            .as_ref()
            .ok_or_else(|| "identity/RBAC policy store is not bound".to_string())?;
        let identities: std::collections::BTreeMap<String, AgentIdentity> = self
            .agents
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        store
            .save(&self.rbac, &identities, self.identity_bootstrap)
            .map_err(|e| format!("identity/RBAC policy save failed: {e}"))
    }

    /// Add/replace an RBAC role definition (CONCEPT:EG-KG.compute.feature); written through to the
    /// durable store when configured (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
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

    /// Remove an RBAC role definition (CONCEPT:EG-KG.compute.feature); written through (EG-303).
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

    /// Add an RBAC grant (CONCEPT:EG-KG.compute.feature); written through (EG-303).
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

    /// Remove an RBAC grant (CONCEPT:EG-KG.compute.feature). Returns true when one was removed;
    /// written through (EG-303).
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

    /// Read-only access to the RBAC policy (for admin `List` / persistence).
    #[cfg(feature = "security")]
    pub fn rbac(&self) -> &crate::rbac::RbacPolicy {
        &self.rbac
    }

    /// Whether this layer still represents the exact pristine first-run image.
    /// Bootstrap state is durable and independent from identity count, so an
    /// administrator cannot recreate first-run authority by removing identities.
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

    /// Atomically consume first-run authority and persist its sole System identity.
    /// Request-envelope, graph, self-registration, and signature checks live at the
    /// served boundary; this layer independently enforces the identity shape and
    /// one-time durable transition while holding the caller's state write lock.
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

    /// Register or update an agent identity; written through to the durable store
    /// when configured (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    pub fn register_agent(&mut self, identity: AgentIdentity) {
        let _ = self.try_register_agent(identity);
    }

    /// Fallible registration used by secure handlers. When a durable adapter is
    /// configured, an unsuccessful commit restores the prior in-memory identity.
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

    /// Remove an agent identity; written through (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
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

    /// True once any identity has been registered. Served graph access requires
    /// provisioned identities; an empty policy is rejected at the boundary.
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
        // Served access is current-policy-only: an actor must resolve to a
        // provisioned durable identity before any graph-type rule is evaluated.
        let Some(identity) = self.agents.get(agent_id) else {
            return false;
        };
        if identity.role == AgentRole::System {
            return true;
        }

        // RBAC (CONCEPT:EG-KG.compute.feature) is the mandatory current decision for
        // non-System identities. Missing roles, an empty policy, or no matching grant
        // are all default-deny; there is no pre-RBAC ACL fall-through.
        #[cfg(feature = "security")]
        {
            let _ = (graph_type, graph_owner);
            let ctx = crate::acl::ResourceContext::graph(graph_name);
            let action = match access {
                AccessLevel::Read => crate::acl::RbacAction::Read,
                AccessLevel::Write => crate::acl::RbacAction::Write,
            };
            return matches!(
                self.rbac.evaluate(&identity.roles, &ctx, action),
                Some(crate::acl::GrantEffect::Allow)
            );
        }

        #[cfg(not(feature = "security"))]
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

    /// Per-agent Row-Level Security (CONCEPT:EG-KG.sharding.row-level-security): may `agent_id` SEE one
    /// node, given that node's owner + visibility convention?
    ///
    /// Visibility convention (carried in the node's property blob; read by
    /// [`row_visibility`]):
    /// * `_owner`      — the owning agent_id (absent requires explicit public visibility).
    /// * `_visibility` — `"public"` (default when absent) or `"private"`.
    /// * `_grants`     — optional comma-separated agent_ids explicitly granted read.
    ///
    /// Default-deny decision (CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6):
    /// an unowned row is visible only when it explicitly carries
    /// `_visibility: "public"` metadata
    /// ([`RowVisibility::tagged`] `&&` [`RowVisibility::public`]); an unowned row
    /// that is undecodable or declares no RLS metadata is denied. Owner, grant,
    /// manager, and System rules remain explicit authorization paths.
    #[cfg(feature = "security")]
    pub fn can_see_row(&self, agent_id: &str, vis: &RowVisibility) -> bool {
        // System role sees everything.
        if let Some(identity) = self.agents.get(agent_id) {
            if identity.role == AgentRole::System {
                return true;
            }
        }
        let owner = match &vis.owner {
            None => return vis.tagged && vis.public,
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
    /// rows `agent_id` may see (CONCEPT:EG-KG.sharding.row-level-security — RLS in the read/plan path).
    ///
    /// This runs on the owned, off-lock snapshot the query planner (SQL / Cypher /
    /// SPARQL / unified) consumes — NOT at the graph boundary — so NO query surface
    /// can exfiltrate a forbidden row: a hidden node is removed from the view's
    /// topology, node-map, and property map, and every edge incident to a removed
    /// node is dropped too (an edge to an invisible node would otherwise leak its
    /// existence). Default-deny remains active even before identities are provisioned.
    #[cfg(feature = "security")]
    pub fn filter_view(&self, agent_id: &str, view: &mut crate::graph::GraphView) {
        // Decide visibility for EVERY topology node. A topology row with no
        // property blob is untagged and must be hidden like an undecodable row.
        let hidden: HashSet<String> = view
            .node_map
            .keys()
            .filter_map(|id| {
                let vis = view
                    .node_properties
                    .get(id)
                    .map(|blob| row_visibility(blob))
                    .unwrap_or_else(RowVisibility::default_public);
                if self.can_see_row(agent_id, &vis) {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .collect();
        // `hidden` includes topology-only rows under strict/default-deny, not just
        // rows represented in the property map.
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

    /// Does `agent_id` hold ADMIN capability (CONCEPT:EG-KG.compute.feature, EG-P0-6)?
    ///
    /// Used to gate the system-wide administrative methods (`RegisterIdentity`,
    /// `RbacAdmin`, `ApplyMultisigMutation`, the M3 reshard/rebalance/catalog
    /// family, backup/restore) — see `server::access::require_admin_capability`,
    /// which drives WHICH methods this applies to from `eg_capabilities::policy`'s
    /// `authz_action` rather than a second hardcoded list.
    ///
    /// `System` role always qualifies. Otherwise, under the `security` feature, an
    /// explicit RBAC grant of `RbacAction::Admin` for one of the agent's roles
    /// (typically scoped `ResourceSelector::All`, since admin actions are not
    /// graph-scoped) — evaluated against a fixed, non-graph resource context so a
    /// grant written for a specific graph never accidentally satisfies a global
    /// admin check. Without `security` compiled in there is no RBAC evaluator to
    /// consult, so only `System` qualifies.
    pub fn has_admin_capability(&self, agent_id: &str) -> bool {
        if let Some(identity) = self.agents.get(agent_id) {
            if identity.role == AgentRole::System {
                return true;
            }
        }
        #[cfg(feature = "security")]
        {
            if let Some(identity) = self.agents.get(agent_id) {
                let ctx = crate::acl::ResourceContext::graph("__admin__");
                return matches!(
                    self.rbac
                        .evaluate(&identity.roles, &ctx, crate::acl::RbacAction::Admin),
                    Some(crate::acl::GrantEffect::Allow)
                );
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
            roles: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "worker1".to_string(),
            role: AgentRole::Agent,
            teams: vec!["alpha".to_string()],
            roles: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "worker2".to_string(),
            role: AgentRole::Agent,
            teams: vec!["alpha".to_string()],
            roles: vec![],
        });
        layer
    }

    #[test]
    #[cfg(not(feature = "security"))]
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
    #[cfg(not(feature = "security"))]
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
    #[cfg(not(feature = "security"))]
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
    #[cfg(not(feature = "security"))]
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
    #[cfg(not(feature = "security"))]
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
    #[cfg(not(feature = "security"))]
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

    // ── Per-agent Row-Level Security (CONCEPT:EG-KG.sharding.row-level-security) ──────────────────
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
            for id in ["b_private", "shared_public", "untagged"] {
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
                .insert("untagged".to_string(), props(&[("name", "x")]));
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
            // worker1 sees the explicitly public node, not the private or untagged rows.
            assert!(!va.node_properties.contains_key("b_private"));
            assert!(va.node_properties.contains_key("shared_public"));
            assert!(!va.node_properties.contains_key("untagged"));
            assert!(!va.node_map.contains_key("b_private"));
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
                roles: vec![],
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
        fn no_identities_still_enforces_row_tags() {
            let layer = IsolationLayer::new(); // no identities
            let mut v = view();
            layer.filter_view("anyone", &mut v);
            assert_eq!(v.node_properties.len(), 1);
            assert!(v.node_properties.contains_key("shared_public"));
        }

        // ── RLS default-deny posture (CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6) ──
        mod default_deny {
            use super::*;

            #[test]
            fn untagged_unowned_row_is_hidden() {
                let layer = setup();
                let mut v = view();
                layer.filter_view("worker1", &mut v);
                assert!(
                    !v.node_properties.contains_key("untagged"),
                    "default-deny must hide an untagged row"
                );
            }

            #[test]
            fn no_identities_hides_topology_only_row() {
                let layer = IsolationLayer::new();
                assert!(!layer.has_rules());
                let mut v = GraphView::default();
                let idx = v.graph.add_node("topology_only".to_string());
                v.node_map.insert("topology_only".to_string(), idx);

                layer.filter_view("unregistered", &mut v);

                assert!(!v.node_map.contains_key("topology_only"));
                assert_eq!(v.graph.node_count(), 0);
            }

            #[test]
            fn undecodable_blob_is_hidden() {
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("garbage".to_string());
                v.node_map.insert("garbage".to_string(), idx);
                v.node_properties.insert(
                    "garbage".to_string(),
                    std::sync::Arc::new(vec![0xFF, 0x00, 0x01]),
                );
                layer.filter_view("worker1", &mut v);
                assert!(
                    !v.node_properties.contains_key("garbage"),
                    "default-deny must reject an undecodable blob"
                );
            }

            #[test]
            fn explicitly_public_unowned_row_is_visible() {
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("shared".to_string());
                v.node_map.insert("shared".to_string(), idx);
                v.node_properties
                    .insert("shared".to_string(), props(&[("_visibility", "public")]));
                layer.filter_view("worker1", &mut v);
                assert!(
                    v.node_properties.contains_key("shared"),
                    "an explicitly public unowned row must remain visible"
                );
            }

            #[test]
            fn owner_and_manager_rules_remain_explicit() {
                let layer = setup();
                let mut vb = view();
                layer.filter_view("worker2", &mut vb);
                assert!(vb.node_properties.contains_key("b_private"));
                assert!(vb.node_properties.contains_key("shared_public"));

                let mut vm = view();
                layer.filter_view("manager", &mut vm);
                assert!(vm.node_properties.contains_key("b_private"));
            }

            #[test]
            fn direct_row_decision_requires_explicit_public_tag() {
                let layer = setup();
                let untagged = RowVisibility {
                    owner: None,
                    public: true,
                    grants: Vec::new(),
                    tagged: false,
                };
                assert!(
                    !layer.can_see_row("worker1", &untagged),
                    "untagged rows remain denied even when decoded visibility defaults public"
                );

                let explicit_public = RowVisibility {
                    owner: None,
                    public: true,
                    grants: Vec::new(),
                    tagged: true,
                };
                assert!(
                    layer.can_see_row("worker1", &explicit_public),
                    "explicit public metadata grants visibility"
                );
            }
        }
    }

    // ── RBAC-at-scale layered on check_access (CONCEPT:EG-KG.compute.feature) ───────────
    #[cfg(feature = "security")]
    mod rbac_access {
        use super::*;
        use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};

        /// Register an agent holding `roles`.
        fn with_roles(layer: &mut IsolationLayer, id: &str, roles: Vec<String>) {
            layer.register_agent(AgentIdentity {
                agent_id: id.to_string(),
                role: AgentRole::Agent,
                teams: vec![],
                roles,
            });
        }

        #[test]
        fn empty_policy_is_default_deny() {
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
        fn rbac_grant_allows_access_acl_would_deny() {
            // worker2 normally can't read worker1's agent graph. An RBAC grant on the
            // "auditor" role for that graph flips it to Allow.
            let mut layer = IsolationLayer::new();
            with_roles(&mut layer, "worker2", vec!["auditor".into()]);
            layer.add_grant(Grant {
                role: "auditor".into(),
                resource: ResourceSelector::Graph("agent:worker1".into()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            assert!(layer.check_access(
                "worker2",
                "agent:worker1",
                GraphType::Agent,
                Some("worker1"),
                AccessLevel::Read
            ));
        }

        #[test]
        fn rbac_deny_overrides_base_allow() {
            // Owner would normally get full access to its own graph; an explicit RBAC
            // Deny on the owner's role revokes write.
            let mut layer = IsolationLayer::new();
            with_roles(&mut layer, "worker1", vec!["frozen".into()]);
            layer.add_grant(Grant {
                role: "frozen".into(),
                resource: ResourceSelector::Graph("agent:worker1".into()),
                action: RbacAction::Write,
                effect: GrantEffect::Deny,
            });
            assert!(!layer.check_access(
                "worker1",
                "agent:worker1",
                GraphType::Agent,
                Some("worker1"),
                AccessLevel::Write
            ));
        }

        #[test]
        fn rbac_hierarchy_inherited_grant_honored_by_check_access() {
            // "senior" inherits "reader"; a reader Read grant on the graph lets a
            // senior-only agent read it through check_access.
            let mut layer = IsolationLayer::new();
            with_roles(&mut layer, "sam", vec!["senior".into()]);
            layer.add_role(Role::new("reader"));
            layer.add_role(Role::with_parents("senior", vec!["reader".into()]));
            layer.add_grant(Grant {
                role: "reader".into(),
                resource: ResourceSelector::Graph("agent:other".into()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            assert!(layer.check_access(
                "sam",
                "agent:other",
                GraphType::Agent,
                Some("other"),
                AccessLevel::Read
            ));
        }

        #[test]
        fn rbac_no_applicable_grant_is_denied() {
            let mut layer = setup();
            layer.add_grant(Grant {
                role: "auditor".into(),
                resource: ResourceSelector::Graph("agent:worker1".into()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            assert!(!layer.check_access(
                "worker1",
                "__commons__",
                GraphType::Commons,
                None,
                AccessLevel::Write
            ));
        }

        #[test]
        fn system_role_still_bypasses_rbac_deny() {
            // System bypass precedes RBAC — a Deny cannot lock out System.
            let mut layer = IsolationLayer::new();
            layer.register_agent(AgentIdentity {
                agent_id: "root".to_string(),
                role: AgentRole::System,
                teams: vec![],
                roles: vec!["frozen".into()],
            });
            layer.add_grant(Grant {
                role: "frozen".into(),
                resource: ResourceSelector::All,
                action: RbacAction::Write,
                effect: GrantEffect::Deny,
            });
            assert!(layer.check_access(
                "root",
                "agent:anything",
                GraphType::Agent,
                Some("someone"),
                AccessLevel::Write
            ));
        }
    }

    // ── Durable RBAC/identity persistence (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence) ───────────────
    #[cfg(feature = "security")]
    mod eg303_persist {
        use super::*;
        use crate::acl::{Grant, GrantEffect, RbacAction, ResourceContext, ResourceSelector, Role};
        use crate::rbac_persist::{RbacPersistError, RbacPolicyStore};
        use std::collections::BTreeMap;

        struct FailingPolicyStore;

        impl RbacPolicyStore for FailingPolicyStore {
            fn load(
                &self,
            ) -> Result<
                (
                    crate::rbac::RbacPolicy,
                    BTreeMap<String, AgentIdentity>,
                    crate::rbac_persist::IdentityBootstrapState,
                ),
                RbacPersistError,
            > {
                Ok((
                    crate::rbac::RbacPolicy::new(),
                    BTreeMap::new(),
                    crate::rbac_persist::IdentityBootstrapState::Pending,
                ))
            }

            fn save(
                &self,
                _policy: &crate::rbac::RbacPolicy,
                _identities: &BTreeMap<String, AgentIdentity>,
                _bootstrap: crate::rbac_persist::IdentityBootstrapState,
            ) -> Result<(), RbacPersistError> {
                Err(RbacPersistError::Redb("injected commit failure".into()))
            }
        }

        /// A unique temp dir per test invocation (no external dev-dep needed).
        fn tmp_dir(tag: &str) -> std::path::PathBuf {
            std::env::temp_dir().join(format!(
                "eg303-iso-{}-{}-{}",
                tag,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            ))
        }

        #[test]
        fn eg303_roles_grants_and_identities_round_trip_through_redb_reopen() {
            let dir = tmp_dir("round-trip");
            {
                let mut layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                layer.add_role(Role::new("reader"));
                layer.add_role(Role::with_parents("editor", vec!["reader".into()]));
                layer.add_grant(Grant {
                    role: "editor".into(),
                    resource: ResourceSelector::Label("Doc".into()),
                    action: RbacAction::Write,
                    effect: GrantEffect::Allow,
                });
                layer.register_agent(AgentIdentity {
                    agent_id: "sam".into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                    roles: vec!["editor".into()],
                });
            }
            // Reopen the SAME dir — a fresh layer restores policy + identities from redb.
            let layer = IsolationLayer::with_persist_dir(&dir).unwrap();
            // Identity survived (has_rules + accessible graphs reflect the registration).
            assert!(layer.has_rules());
            assert!(layer.accessible_graphs("sam").contains("agent:sam"));
            assert!(layer.accessible_graphs("sam").contains("team:alpha"));
            // Roles/grants survived — the inherited grant still evaluates.
            assert_eq!(layer.rbac().grants().len(), 1);
            assert!(layer.rbac().is_allowed(
                &["editor"],
                &ResourceContext {
                    graph: "g".into(),
                    label: Some("Doc".into())
                },
                RbacAction::Write
            ));
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn eg303_mutation_write_through_visible_on_reopen() {
            let dir = tmp_dir("write-through");
            // 1) Add a grant, then reopen: the grant is present.
            {
                let mut layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                layer.add_grant(Grant {
                    role: "r".into(),
                    resource: ResourceSelector::All,
                    action: RbacAction::Read,
                    effect: GrantEffect::Allow,
                });
            }
            {
                let layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                assert_eq!(layer.rbac().grants().len(), 1);
            }
            // 2) Remove that grant + register/unregister an identity, then reopen: the
            //    removals are durable too (write-through fires on every mutation).
            {
                let mut layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                assert!(layer.remove_grant(&Grant {
                    role: "r".into(),
                    resource: ResourceSelector::All,
                    action: RbacAction::Read,
                    effect: GrantEffect::Allow,
                }));
                layer.register_agent(AgentIdentity {
                    agent_id: "tmp".into(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    roles: vec![],
                });
                layer.unregister_agent("tmp");
            }
            let layer = IsolationLayer::with_persist_dir(&dir).unwrap();
            assert_eq!(layer.rbac().grants().len(), 0);
            assert!(!layer.has_rules());
            assert!(!layer.identity_bootstrap_pending());
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn exact_system_bootstrap_is_atomic_and_one_time() {
            let mut layer = IsolationLayer::new();
            assert!(layer.identity_bootstrap_pending());
            layer
                .try_bootstrap_system_identity(AgentIdentity {
                    agent_id: "root".into(),
                    role: AgentRole::System,
                    teams: vec![],
                    roles: vec![],
                })
                .unwrap();
            assert!(!layer.identity_bootstrap_pending());
            assert!(layer
                .try_bootstrap_system_identity(AgentIdentity {
                    agent_id: "second".into(),
                    role: AgentRole::System,
                    teams: vec![],
                    roles: vec![],
                })
                .is_err());
        }

        #[test]
        fn eg303_embedded_layer_uses_atomic_memory_policy_store() {
            let mut layer = IsolationLayer::new();
            assert!(layer.persist.is_some());
            layer.add_grant(Grant {
                role: "r".into(),
                resource: ResourceSelector::All,
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            layer.register_agent(AgentIdentity {
                agent_id: "x".into(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec!["r".into()],
            });
            assert!(layer.persist.is_some());
            assert_eq!(layer.rbac().grants().len(), 1);
            assert!(layer.has_rules());
        }

        #[test]
        fn durable_policy_failure_rolls_back_identity_and_rbac_mutations() {
            let mut layer = IsolationLayer::new();
            layer.persist = Some(std::sync::Arc::new(FailingPolicyStore));

            assert!(layer
                .try_register_agent(AgentIdentity {
                    agent_id: "not-committed".into(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    roles: vec![],
                })
                .is_err());
            assert!(!layer.has_rules());

            assert!(layer
                .try_add_grant(Grant {
                    role: "r".into(),
                    resource: ResourceSelector::All,
                    action: RbacAction::Admin,
                    effect: GrantEffect::Allow,
                })
                .is_err());
            assert!(layer.rbac().grants().is_empty());
        }
    }
}
