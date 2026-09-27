//! Graph access for an engine-owned background writer: one fixed service
//! principal, one engine-provisioned role per graph granting Read and Write on
//! exactly that graph (never a pattern). The writer then goes through the same
//! mutation gateway and RBAC check as any caller — no bypass, and not a System
//! identity. Used by the connector-pack projection worker (PB1) and the standing
//! impact watches (EH-526).
//!
//! An empty policy awaiting its signer-backed System bootstrap is never
//! touched: registering an identity there would consume the bootstrap.

use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};

/// One service writer's per-graph grant: `actor` holds role `<role_prefix>:<graph>`.
pub(crate) struct ServiceGraphGrant<'a> {
    pub(crate) actor: &'a str,
    pub(crate) role_prefix: &'a str,
    /// The refusal code prefix when the policy is not bootstrapped yet.
    pub(crate) unbootstrapped: &'a str,
}

impl ServiceGraphGrant<'_> {
    fn role(&self, graph: &str) -> String {
        format!("{}:{graph}", self.role_prefix)
    }

    /// Let the actor read and write exactly `graph`. Idempotent.
    pub(crate) fn ensure(&self, isolation: &mut IsolationLayer, graph: &str) -> Result<(), String> {
        if isolation.identity_bootstrap_pending() {
            return Err(format!(
                "{}: the identity policy awaits its System bootstrap; service graph access \
                 is provisioned after it",
                self.unbootstrapped
            ));
        }
        let role = self.role(graph);
        ensure_exact_graph_grant(isolation, graph, self.actor, role)
    }
}

/// Install an exact-graph read/write role and attach it to one service actor.
/// Callers retain their own policy-bootstrap refusal before entering here.
pub(crate) fn ensure_exact_graph_grant(
    isolation: &mut IsolationLayer,
    graph: &str,
    actor: &str,
    role: String,
) -> Result<(), String> {
    for action in [RbacAction::Read, RbacAction::Write] {
        let grant = Grant {
            role: role.clone(),
            resource: ResourceSelector::Graph(graph.to_string()),
            action,
            effect: GrantEffect::Allow,
        };
        if !isolation.rbac().grants().contains(&grant) {
            isolation.try_add_role(Role::new(role.clone()))?;
            isolation.try_add_grant(grant)?;
        }
    }
    let mut identity = isolation
        .get_identity(actor)
        .unwrap_or_else(|| AgentIdentity {
            agent_id: actor.to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
    if identity.roles.contains(&role) {
        return Ok(());
    }
    identity.roles.push(role);
    isolation.try_register_agent(identity)
}
