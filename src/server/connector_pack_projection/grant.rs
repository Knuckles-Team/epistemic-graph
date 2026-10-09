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

use crate::isolation::IsolationLayer;
use crate::server::service_grant::ServiceGraphGrant;

/// The fixed service principal the projection worker writes as.
pub(crate) const PROJECTION_ACTOR: &str = "service:connector-pack-projection";

const GRANT: ServiceGraphGrant<'static> = ServiceGraphGrant {
    actor: PROJECTION_ACTOR,
    role_prefix: "pack-projection",
    unbootstrapped: "PACK_PROJECTION_POLICY_UNBOOTSTRAPPED",
};

/// Let [`PROJECTION_ACTOR`] read and write exactly `graph`. Idempotent: an
/// already-provisioned grant writes nothing.
pub(crate) fn ensure(isolation: &mut IsolationLayer, graph: &str) -> Result<(), String> {
    GRANT.ensure(isolation, graph)
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

    // spec: EG-TYPED-PACKS-R074, EG-TYPED-PACKS-R096
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

    // spec: EG-TYPED-PACKS-R074, EG-TYPED-PACKS-R096
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

    // spec: EG-TYPED-PACKS-R074, EG-TYPED-PACKS-R096
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
