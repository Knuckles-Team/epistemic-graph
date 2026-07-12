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
    /// Owning agent_id (`_owner`); `None` ⇒ unowned. Under the PERMISSIVE (default)
    /// posture this is visible to all; under the STRICT default-deny posture
    /// (CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6) it is visible to all only when [`Self::tagged`]
    /// AND [`Self::public`] — see [`IsolationLayer::can_see_row`].
    pub owner: Option<String>,
    /// `true` when `_visibility` is absent OR `"public"`; `false` for `"private"`.
    pub public: bool,
    /// Agent_ids explicitly granted read (`_grants`, comma-separated).
    pub grants: Vec<String>,
    /// Whether this row carried ANY explicit RLS metadata at all — i.e. the decoded
    /// property blob contained at least one of `_owner` / `_visibility` / `_grants`.
    /// `false` for an undecodable blob OR a blob that decoded fine but declares none
    /// of the three keys (a genuinely untagged/legacy row). Consulted ONLY by the
    /// STRICT default-deny posture (EG-P0-6): the permissive (default, back-compat)
    /// posture never reads this field, so existing behavior is byte-for-byte
    /// unchanged when the flag is off.
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
/// Under the PERMISSIVE (default, back-compat) posture, a blob that can't be
/// decoded as a string-keyed map (or that decodes but declares none of the RLS
/// keys) is treated as an unowned PUBLIC row: RLS only ever HIDES rows that
/// explicitly mark themselves owned+private, so undecodable/legacy data is never
/// accidentally hidden. The returned [`RowVisibility::tagged`] flag records whether
/// the row actually carried explicit RLS metadata; [`IsolationLayer::can_see_row`]
/// consults it ONLY under the STRICT default-deny posture (EG-P0-6), where an
/// untagged/undecodable row is denied instead of defaulted open — see that
/// function's docs for the migration implication.
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

/// Resolve the RLS default-deny posture from the raw
/// `EPISTEMIC_GRAPH_RLS_DEFAULT_DENY` env value (CONCEPT:EG-KG.sharding.row-level-security,
/// EG-P0-6). **Default (the env var unset) is `true` (STRICT/secure-by-default)** for a
/// fresh/greenfield deployment — flipped from the pre-WS-1b permissive default per this
/// repo's no-back-compat policy (`AGENTS.md` "No Legacy"): there is no external caller
/// pinned to the old posture, so the secure posture ships as the default rather than as
/// an opt-in. A deployment that still wants the permissive/back-compat posture opts out
/// explicitly with `0`/`false`/`no`/`off` (case-insensitive, whitespace-trimmed); any
/// other value (including an unrecognized one) resolves to the strict posture, so a typo
/// fails closed rather than silently falling back to permissive. Server startup
/// (`src/main.rs`) calls this with `std::env::var("EPISTEMIC_GRAPH_RLS_DEFAULT_DENY").ok()`;
/// pulled out as a pure function so the resolution itself is unit-testable without a
/// process-wide env var.
pub fn resolve_rls_default_deny(raw: Option<&str>) -> bool {
    match raw {
        Some(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        None => true,
    }
}

/// Isolation policy engine.
#[derive(Clone)]
pub struct IsolationLayer {
    /// Known agent identities for ACL resolution.
    agents: HashMap<String, AgentIdentity>,
    /// RBAC policy (CONCEPT:EG-KG.compute.feature): roles + grants layered on top of the per-agent
    /// ACL/RLS. An EMPTY policy leaves every existing decision unchanged.
    #[cfg(feature = "security")]
    rbac: crate::rbac::RbacPolicy,
    /// Durable RBAC/identity persistence handle (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). `None` ⇒ fully
    /// in-memory (today's default): every write-through is a no-op. `Some` ⇒ the
    /// policy + identities were LOADED from redb at boot and are written through on
    /// every RBAC/identity mutation. `Arc` keeps [`IsolationLayer`] `Clone`.
    #[cfg(feature = "security")]
    persist: Option<std::sync::Arc<crate::rbac_persist::RbacStore>>,
    /// RLS default-deny / strict-isolation posture (CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6).
    /// `false` is the PERMISSIVE/back-compat posture: an unowned, undecodable, or
    /// untagged-legacy row is visible to all. `true` is the SECURE/target posture:
    /// such a row is DENIED unless it explicitly declares `_visibility: "public"`
    /// (or an `_owner` a rule already grants). See [`Self::can_see_row`] for the
    /// exact decision and [`Self::set_rls_default_deny`] / [`Self::with_rls_default_deny`]
    /// for how to set it.
    ///
    /// **Struct-level default (`IsolationLayer::new()`) stays `false`** — this is the
    /// bare-library builder default for callers (tests/harnesses/the embedded
    /// in-process API) who construct an `IsolationLayer` directly with no config
    /// resolution of their own. **The server's fresh-deploy default is `true`**
    /// (secure-by-default): `src/main.rs` resolves the actual posture via
    /// [`resolve_rls_default_deny`] over `EPISTEMIC_GRAPH_RLS_DEFAULT_DENY` and calls
    /// [`Self::with_rls_default_deny`] with the result — so a deployed server is
    /// strict unless it explicitly opts out.
    ///
    /// **Migration implication:** flipping this to `true` in a deployment that has
    /// legacy rows written before RLS tagging existed makes those rows INVISIBLE to
    /// every non-System agent until they are explicitly re-tagged with an `_owner`
    /// or `_visibility: "public"` property. Recommended rollout: run a backfill that
    /// tags existing rows (owner from provenance, or an explicit `_visibility:
    /// "public"` for genuinely shared data) BEFORE enabling strict mode in
    /// production, or accept that untagged legacy data becomes temporarily
    /// unreadable until backfilled.
    #[cfg(feature = "security")]
    rls_default_deny: bool,
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
            persist: None,
            #[cfg(feature = "security")]
            rls_default_deny: false,
        }
    }

    /// Enable/disable the RLS default-deny (strict-isolation) posture in place. See
    /// the `rls_default_deny` field docs for the exact semantics + migration
    /// implication. Server startup wires this from the `EPISTEMIC_GRAPH_RLS_DEFAULT_DENY`
    /// env var; tests/callers may also flip it directly.
    #[cfg(feature = "security")]
    pub fn set_rls_default_deny(&mut self, deny: bool) {
        self.rls_default_deny = deny;
    }

    /// Builder-style variant of [`Self::set_rls_default_deny`] for chaining at
    /// construction time (e.g. `IsolationLayer::new().with_rls_default_deny(true)`).
    #[cfg(feature = "security")]
    pub fn with_rls_default_deny(mut self, deny: bool) -> Self {
        self.rls_default_deny = deny;
        self
    }

    /// Whether the strict RLS default-deny posture is active.
    #[cfg(feature = "security")]
    pub fn rls_default_deny(&self) -> bool {
        self.rls_default_deny
    }

    /// Open an [`IsolationLayer`] backed by a durable redb store at `dir`
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Any previously-persisted RBAC policy + registered agent
    /// identities are LOADED at boot; every subsequent `add_role`/`remove_role`/
    /// `add_grant`/`remove_grant`/`register_agent`/`unregister_agent` mutation is
    /// written through to redb. An EMPTY/absent store yields the exact in-memory
    /// default — identical to [`IsolationLayer::new`] — so this is fully
    /// backward-compatible; the only difference is that state now survives a restart.
    #[cfg(feature = "security")]
    pub fn with_persist_dir<P: AsRef<std::path::Path>>(
        dir: P,
    ) -> Result<Self, crate::rbac_persist::RbacPersistError> {
        let store = crate::rbac_persist::RbacStore::open(dir)?;
        let (rbac, identities) = store.load()?;
        let agents: HashMap<String, AgentIdentity> = identities.into_iter().collect();
        Ok(IsolationLayer {
            agents,
            rbac,
            persist: Some(std::sync::Arc::new(store)),
            rls_default_deny: false,
        })
    }

    /// Best-effort write-through of the FULL RBAC state (policy + identities) to the
    /// durable store (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). A NO-OP when no persist dir is configured
    /// (in-memory default). Errors are swallowed: [`with_persist_dir`] already
    /// validated the store is writable at boot, and the mutation entry points
    /// (`RbacAdmin`, `register_identity`) are infallible by contract.
    ///
    /// [`with_persist_dir`]: IsolationLayer::with_persist_dir
    #[cfg(feature = "security")]
    fn persist_state(&self) {
        if let Some(store) = &self.persist {
            let identities: std::collections::BTreeMap<String, AgentIdentity> = self
                .agents
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let _ = store.save(&self.rbac, &identities);
        }
    }

    /// Add/replace an RBAC role definition (CONCEPT:EG-KG.compute.feature); written through to the
    /// durable store when configured (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    #[cfg(feature = "security")]
    pub fn add_role(&mut self, role: crate::acl::Role) {
        self.rbac.add_role(role);
        self.persist_state();
    }

    /// Remove an RBAC role definition (CONCEPT:EG-KG.compute.feature); written through (EG-303).
    #[cfg(feature = "security")]
    pub fn remove_role(&mut self, name: &str) {
        self.rbac.remove_role(name);
        self.persist_state();
    }

    /// Add an RBAC grant (CONCEPT:EG-KG.compute.feature); written through (EG-303).
    #[cfg(feature = "security")]
    pub fn add_grant(&mut self, grant: crate::acl::Grant) {
        self.rbac.add_grant(grant);
        self.persist_state();
    }

    /// Remove an RBAC grant (CONCEPT:EG-KG.compute.feature). Returns true when one was removed;
    /// written through (EG-303).
    #[cfg(feature = "security")]
    pub fn remove_grant(&mut self, grant: &crate::acl::Grant) -> bool {
        let removed = self.rbac.remove_grant(grant);
        if removed {
            self.persist_state();
        }
        removed
    }

    /// Read-only access to the RBAC policy (for admin `List` / persistence).
    #[cfg(feature = "security")]
    pub fn rbac(&self) -> &crate::rbac::RbacPolicy {
        &self.rbac
    }

    /// Register or update an agent identity; written through to the durable store
    /// when configured (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    pub fn register_agent(&mut self, identity: AgentIdentity) {
        self.agents.insert(identity.agent_id.clone(), identity);
        #[cfg(feature = "security")]
        self.persist_state();
    }

    /// Remove an agent identity; written through (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    pub fn unregister_agent(&mut self, agent_id: &str) {
        let removed = self.agents.remove(agent_id).is_some();
        #[cfg(feature = "security")]
        if removed {
            self.persist_state();
        }
        #[cfg(not(feature = "security"))]
        let _ = removed;
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

        // RBAC (CONCEPT:EG-KG.compute.feature): consult roles/grants layered on top of the ACL.
        // Backward-compatible: an EMPTY policy is skipped entirely, so the existing
        // GraphType decision below is returned unchanged. When grants exist, an
        // explicit Deny for the agent's roles wins (deny overrides), an explicit
        // Allow grants access the base ACL would otherwise deny, and NO applicable
        // grant falls through to the base ACL decision (additive, not a lockdown).
        #[cfg(feature = "security")]
        {
            if !self.rbac.is_empty() {
                if let Some(identity) = self.agents.get(agent_id) {
                    let ctx = crate::acl::ResourceContext::graph(graph_name);
                    let action = match access {
                        AccessLevel::Read => crate::acl::RbacAction::Read,
                        AccessLevel::Write => crate::acl::RbacAction::Write,
                    };
                    match self.rbac.evaluate(&identity.roles, &ctx, action) {
                        Some(crate::acl::GrantEffect::Deny) => return false,
                        Some(crate::acl::GrantEffect::Allow) => return true,
                        None => {}
                    }
                }
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

    /// Per-agent Row-Level Security (CONCEPT:EG-KG.sharding.row-level-security): may `agent_id` SEE one
    /// node, given that node's owner + visibility convention?
    ///
    /// Visibility convention (carried in the node's property blob; read by
    /// [`row_visibility`]):
    /// * `_owner`      — the owning agent_id (absent ⇒ unowned/legacy ⇒ visible to all).
    /// * `_visibility` — `"public"` (default when absent) or `"private"`.
    /// * `_grants`     — optional comma-separated agent_ids explicitly granted read.
    ///
    /// Decision (PERMISSIVE / default posture, `rls_default_deny == false` — matches
    /// every pre-EG-P0-6 behavior byte-for-byte): an agent may see a row when ANY
    /// holds — 1. it is unowned (no `_owner`), 2. it is public, 3. the agent IS the
    /// owner, 4. the agent is explicitly granted, 5. the agent is a manager of the
    /// owner, 6. the agent is a `System` role. Otherwise the row is hidden.
    ///
    /// Decision (STRICT / default-deny posture, `rls_default_deny == true`,
    /// CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6): identical EXCEPT case 1 — an unowned row is visible
    /// only when it also explicitly carries `_visibility: "public"` metadata
    /// ([`RowVisibility::tagged`] `&&` [`RowVisibility::public`]); an unowned row
    /// that is undecodable OR declares no RLS metadata at all is now DENIED instead
    /// of defaulted open. **Migration implication:** this hides legacy/untagged rows
    /// written before RLS existed — see the `rls_default_deny` field docs for the
    /// recommended backfill-before-enable rollout. Owner/grant/manager/System rules
    /// (cases 2-6) are completely unaffected by this flag.
    #[cfg(feature = "security")]
    pub fn can_see_row(&self, agent_id: &str, vis: &RowVisibility) -> bool {
        // System role sees everything.
        if let Some(identity) = self.agents.get(agent_id) {
            if identity.role == AgentRole::System {
                return true;
            }
        }
        let owner = match &vis.owner {
            None => {
                if self.rls_default_deny {
                    // Strict posture: only an EXPLICIT public tag opens an unowned
                    // row; an undecodable/untagged legacy row is denied.
                    return vis.tagged && vis.public;
                }
                // Permissive (default) posture: unowned — visible to all
                // (legacy/shared data), unchanged from pre-EG-P0-6 behavior.
                return true;
            }
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
        fn no_rules_is_noop() {
            let layer = IsolationLayer::new(); // no identities
            let mut v = view();
            let before = v.node_properties.len();
            layer.filter_view("anyone", &mut v);
            assert_eq!(v.node_properties.len(), before);
        }

        // ── RLS default-deny / strict-isolation posture (CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6) ──
        mod default_deny {
            use super::*;

            #[test]
            fn permissive_default_still_shows_legacy_unowned_row() {
                // `rls_default_deny` defaults to false: byte-for-byte pre-EG-P0-6
                // behavior — a genuinely untagged legacy row stays visible to all.
                let layer = setup();
                assert!(!layer.rls_default_deny());
                let mut v = view();
                layer.filter_view("worker1", &mut v);
                assert!(v.node_properties.contains_key("legacy_unowned"));
            }

            #[test]
            fn strict_mode_hides_legacy_unowned_row() {
                // Flipping the flag hides the SAME untagged row — the migration
                // implication: legacy data with no RLS tag becomes invisible.
                let mut layer = setup();
                layer.set_rls_default_deny(true);
                let mut v = view();
                layer.filter_view("worker1", &mut v);
                assert!(
                    !v.node_properties.contains_key("legacy_unowned"),
                    "strict mode must deny an untagged/legacy row by default"
                );
            }

            #[test]
            fn strict_mode_hides_undecodable_blob() {
                let mut layer = setup();
                layer.set_rls_default_deny(true);
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
                    "strict mode must deny an undecodable blob, not default it open"
                );
            }

            #[test]
            fn strict_mode_still_shows_explicitly_public_unowned_row() {
                // An unowned row that EXPLICITLY opts into `_visibility: public` is
                // still visible under strict mode — only the untagged default flips.
                let mut layer = setup();
                layer.set_rls_default_deny(true);
                let mut v = GraphView::default();
                let idx = v.graph.add_node("shared".to_string());
                v.node_map.insert("shared".to_string(), idx);
                v.node_properties
                    .insert("shared".to_string(), props(&[("_visibility", "public")]));
                layer.filter_view("worker1", &mut v);
                assert!(
                    v.node_properties.contains_key("shared"),
                    "an explicitly public unowned row must stay visible under strict mode"
                );
            }

            #[test]
            fn strict_mode_owner_and_grant_rules_unaffected() {
                // Owner/grant/manager rules are completely orthogonal to the flag:
                // owner_b_sees_own_private_node / manager_sees_subordinate_private_node
                // / explicit_grant_is_honored all still hold under strict mode.
                let mut layer = setup();
                layer.set_rls_default_deny(true);
                let mut vb = view();
                layer.filter_view("worker2", &mut vb);
                assert!(vb.node_properties.contains_key("b_private"));
                assert!(vb.node_properties.contains_key("shared_public"));

                let mut vm = view();
                layer.filter_view("manager", &mut vm);
                assert!(vm.node_properties.contains_key("b_private"));
            }

            #[test]
            fn can_see_row_direct_strict_vs_permissive() {
                let mut layer = setup();
                let untagged = RowVisibility {
                    owner: None,
                    public: true,
                    grants: Vec::new(),
                    tagged: false,
                };
                assert!(
                    layer.can_see_row("worker1", &untagged),
                    "permissive default allows untagged"
                );
                layer.set_rls_default_deny(true);
                assert!(
                    !layer.can_see_row("worker1", &untagged),
                    "strict mode denies untagged even though `public` defaulted true"
                );

                let explicit_public = RowVisibility {
                    owner: None,
                    public: true,
                    grants: Vec::new(),
                    tagged: true,
                };
                assert!(
                    layer.can_see_row("worker1", &explicit_public),
                    "strict mode allows explicitly-tagged public"
                );
            }
        }

        // ── EPISTEMIC_GRAPH_RLS_DEFAULT_DENY env resolution (secure-by-default, WS-1b) ──
        mod env_resolution {
            use super::super::resolve_rls_default_deny;

            #[test]
            fn unset_env_resolves_strict_secure_by_default() {
                assert!(
                    resolve_rls_default_deny(None),
                    "a fresh/greenfield deployment (no env var set) must default to \
                     the STRICT/secure-by-default RLS posture"
                );
            }

            #[test]
            fn explicit_opt_out_values_resolve_permissive() {
                for v in ["0", "false", "False", "FALSE", "no", "off", "  false  "] {
                    assert!(
                        !resolve_rls_default_deny(Some(v)),
                        "{v:?} must resolve to the permissive opt-out posture"
                    );
                }
            }

            #[test]
            fn truthy_and_unrecognized_values_resolve_strict() {
                for v in ["1", "true", "TRUE", "yes", "on", "garbage", ""] {
                    assert!(
                        resolve_rls_default_deny(Some(v)),
                        "{v:?} must resolve to the strict posture (fail-closed on typos)"
                    );
                }
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
        fn empty_policy_leaves_acl_unchanged() {
            // No grants ⇒ the existing peer-isolation ACL result stands: worker2 may
            // NOT read worker1's private agent graph.
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
        fn rbac_no_applicable_grant_falls_through_to_acl() {
            // Policy is non-empty but no grant matches THIS (resource,action) for the
            // agent ⇒ the base ACL still decides (commons stays writable to all).
            let mut layer = setup();
            layer.add_grant(Grant {
                role: "auditor".into(),
                resource: ResourceSelector::Graph("agent:worker1".into()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            assert!(layer.check_access(
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
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn eg303_no_persist_dir_is_in_memory_no_op() {
            // A default layer has NO store: mutations behave exactly as pre-EG-303
            // (write-through is a no-op) and nothing is persisted anywhere.
            let mut layer = IsolationLayer::new();
            assert!(layer.persist.is_none());
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
            // Still no store; in-memory state is exactly what was set.
            assert!(layer.persist.is_none());
            assert_eq!(layer.rbac().grants().len(), 1);
            assert!(layer.has_rules());
        }
    }
}
