//! The projection actor's graph access: exactly one pack graph per bound
//! connector (PB1).
//!
//! The worker writes each connector's `pack__<digest>` graph as the fixed
//! service principal [`PROJECTION_ACTOR`], through the same mutation gateway
//! and RBAC check as any caller -- no bypass, and not a System identity. What
//! it may write is one engine-provisioned role per pack graph,
//! `pack-projection:<graph>`, granting Read and Write on that exact graph
//! (never a `pack__*` pattern). The grant is asserted by the worker for the
//! connector of the outbox row it is projecting, and only once that
//! connector has a committed head -- i.e. only after an authorized import --
//! so there is no window between import and first projection to lose. It
//! mirrors the tenant-graph provisioning the engine already does at graph
//! creation (`provision_tenant_graph_access`). The principal that holds the
//! grant is the principal the worker's writes are verified as.
//!
//! An empty policy awaiting its signer-backed System bootstrap is never
//! touched: registering an identity there would consume the bootstrap.

use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};

/// The fixed service principal the projection worker writes as.
pub(crate) const PROJECTION_ACTOR: &str = "service:connector-pack-projection";

fn role_for(graph: &str) -> String {
    format!("pack-projection:{graph}")
}

fn grants_for(graph: &str) -> [Grant; 2] {
    [RbacAction::Read, RbacAction::Write].map(|action| Grant {
        role: role_for(graph),
        resource: ResourceSelector::Graph(graph.to_string()),
        action,
        effect: GrantEffect::Allow,
    })
}

/// Let [`PROJECTION_ACTOR`] read and write exactly `graph`. Idempotent: an
/// already-provisioned grant writes nothing.
pub(crate) fn ensure(isolation: &mut IsolationLayer, graph: &str) -> Result<(), String> {
    if isolation.identity_bootstrap_pending() {
        return Err(
            "PACK_PROJECTION_POLICY_UNBOOTSTRAPPED: the identity policy awaits its System \
             bootstrap; pack projection access is provisioned after it"
                .to_string(),
        );
    }
    let role = role_for(graph);
    for grant in grants_for(graph) {
        if !isolation.rbac().grants().contains(&grant) {
            isolation.try_add_role(Role::new(role.clone()))?;
            isolation.try_add_grant(grant)?;
        }
    }
    let mut identity = isolation
        .get_identity(PROJECTION_ACTOR)
        .unwrap_or_else(|| AgentIdentity {
            agent_id: PROJECTION_ACTOR.to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::isolation::AccessLevel;
    use crate::protocol::GraphType;

    fn policy() -> IsolationLayer {
        crate::server::state::ServerState::test_isolation("pack-grant-admin")
    }

    fn may_write(isolation: &IsolationLayer, graph: &str) -> bool {
        isolation.check_access(
            PROJECTION_ACTOR,
            graph,
            GraphType::Global,
            None,
            AccessLevel::Write,
        )
    }

    #[test]
    fn the_projection_actor_writes_only_the_pack_graph_it_was_granted() {
        let mut isolation = policy();
        ensure(&mut isolation, "pack__aaaa").unwrap();
        ensure(&mut isolation, "pack__aaaa").unwrap();
        assert!(may_write(&isolation, "pack__aaaa"));
        assert!(!may_write(&isolation, "pack__bbbb"), "another pack's graph");
        assert!(!may_write(&isolation, "__commons__"));
        assert_eq!(
            isolation.get_identity(PROJECTION_ACTOR).unwrap().roles,
            ["pack-projection:pack__aaaa"]
        );
    }

    #[test]
    fn the_granted_principal_is_the_principal_the_worker_signs_as() {
        let signer =
            crate::server::auth::VerifiedRequestContext::authenticated_fixed_service_actor(
                super::super::worker::CONSUMER,
                &["kg:write"],
            )
            .unwrap();
        assert_eq!(signer.agent_id(), PROJECTION_ACTOR);
    }

    #[test]
    fn an_unbootstrapped_policy_is_never_consumed() {
        let mut isolation = IsolationLayer::new();
        assert!(isolation.identity_bootstrap_pending());
        let error = ensure(&mut isolation, "pack__aaaa").unwrap_err();
        assert!(
            error.starts_with("PACK_PROJECTION_POLICY_UNBOOTSTRAPPED"),
            "{error}"
        );
        assert!(isolation.identity_bootstrap_pending());
    }
}
