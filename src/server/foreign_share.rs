//! Explicit sharing of an owner-scoped foreign source through RBAC grants
//! (CONCEPT:EG-KG.query.query-federation, EH-378).
//!
//! A registered foreign source belongs to its owner (tenant+principal, EH-373) and only
//! the owner may use it by its plain name. Sharing reuses the engine's existing RBAC
//! grant mechanism, the same pattern ConnectorPack uses for `pack-projection:<graph>`:
//!
//! * registration provisions ONE engine-owned role per source,
//!   `foreign-source-use:<owner agent>/<name>`, holding exactly one grant — `Read` on the
//!   reserved resource `foreign-source:<owner agent>/<name>`. The role is assigned to
//!   nobody; the registering principal needs no grant (ownership is its use right).
//! * an administrator shares the source by assigning that role to another principal
//!   with the existing identity/role administration, and revokes it by removing the role
//!   (or the grant). The registry is rebuilt per request, so a revocation takes effect on
//!   the next query.
//! * a grantee addresses a shared source by its qualified name `<owner agent>/<name>`, so
//!   two owners' same-named sources never collide and a grantee's own plain names are
//!   never shadowed.
//!
//! Only an EXACT grant on the reserved resource counts. A wildcard or `All` grant (for
//! example a broad reader role) never conveys use of a credential-bearing source, and a
//! same-specificity `Deny` still wins. Without a grant a shared name resolves exactly
//! like an unregistered name, so the refusal reveals nothing.

use crate::isolation::IsolationLayer;

/// The name a grantee uses for `owner_agent`'s source `name`.
pub(crate) fn shared_name(owner_agent: &str, name: &str) -> String {
    format!("{owner_agent}/{name}")
}

/// The reserved RBAC resource a share grant names. Its prefix is reserved in graph-name
/// validation (`registry::validate_graph_name`), so no graph can ever carry this name
/// and a graph grant can never convey use of a source.
pub(crate) fn share_resource(owner_agent: &str, name: &str) -> String {
    format!(
        "{}{}",
        crate::registry::FOREIGN_SOURCE_RESOURCE_PREFIX,
        shared_name(owner_agent, name)
    )
}

/// The engine-provisioned role that conveys use of `owner_agent`'s source `name`.
pub(crate) fn share_role(owner_agent: &str, name: &str) -> String {
    format!("foreign-source-use:{}", shared_name(owner_agent, name))
}

#[cfg(feature = "security")]
fn share_grant(owner_agent: &str, name: &str) -> crate::acl::Grant {
    crate::acl::Grant {
        role: share_role(owner_agent, name),
        resource: crate::acl::ResourceSelector::Graph(share_resource(owner_agent, name)),
        action: crate::acl::RbacAction::Read,
        effect: crate::acl::GrantEffect::Allow,
    }
}

/// Provision the share role + its single exact grant for a newly registered source.
/// Idempotent. Assigns the role to nobody. Never touches a policy still awaiting its
/// System bootstrap (registering a role there would consume the bootstrap).
#[cfg(feature = "security")]
pub(crate) fn provision_share_role(
    isolation: &mut IsolationLayer,
    owner_agent: &str,
    name: &str,
) -> Result<(), String> {
    if isolation.identity_bootstrap_pending() {
        return Err(
            "FOREIGN_SOURCE_POLICY_UNBOOTSTRAPPED: the identity policy awaits its System \
             bootstrap; foreign sources are registered after it"
                .to_string(),
        );
    }
    let grant = share_grant(owner_agent, name);
    if isolation.rbac().grants().contains(&grant) {
        return Ok(());
    }
    isolation.try_add_role(crate::acl::Role::new(share_role(owner_agent, name)))?;
    isolation.try_add_grant(grant)
}

#[cfg(not(feature = "security"))]
pub(crate) fn provision_share_role(
    _isolation: &mut IsolationLayer,
    _owner_agent: &str,
    _name: &str,
) -> Result<(), String> {
    Ok(())
}

/// May `caller_agent` use the shared source behind `resource`? Only an exact `Read`
/// allow grant on `resource`, held through the caller's (expanded) roles, counts, and
/// the RBAC evaluator must still allow it (a same-specificity deny wins).
#[cfg(feature = "security")]
pub(crate) fn may_use_shared(
    isolation: &IsolationLayer,
    caller_agent: &str,
    resource: &str,
) -> bool {
    let Some(identity) = isolation.get_identity(caller_agent) else {
        return false;
    };
    let policy = isolation.rbac();
    let roles = policy.expand_roles(&identity.roles);
    let exact = crate::acl::ResourceSelector::Graph(resource.to_string());
    let granted = policy.grants().iter().any(|grant| {
        grant.resource == exact
            && grant.action == crate::acl::RbacAction::Read
            && grant.effect == crate::acl::GrantEffect::Allow
            && roles.contains(&grant.role)
    });
    granted
        && policy.is_allowed(
            &identity.roles,
            &crate::acl::ResourceContext::graph(resource),
            crate::acl::RbacAction::Read,
        )
}

#[cfg(not(feature = "security"))]
pub(crate) fn may_use_shared(
    _isolation: &IsolationLayer,
    _caller_agent: &str,
    _resource: &str,
) -> bool {
    false
}

#[cfg(all(test, feature = "security"))]
mod tests {
    //! EH-378 grant / use / revoke at the catalog chokepoint. The served proof lives in
    //! `server::tests::foreign_tenancy::shared_source_needs_an_explicit_grant`.
    use super::*;
    use crate::isolation::{AgentIdentity, AgentRole};
    use crate::server::access::CarrierAuthority;
    use crate::server::foreign_catalog::ForeignSourceCatalog;
    use eg_types::wire::ForeignSourceSpec;

    fn carrier(agent: &str) -> CarrierAuthority {
        CarrierAuthority::verified_for_test(agent)
    }

    fn spec() -> ForeignSourceSpec {
        ForeignSourceSpec::HttpJson {
            url: "http://a.invalid/".into(),
            json_path: "data".into(),
            field_map: eg_types::wire::HttpFieldMap {
                id: "id".into(),
                score: None,
                columns: Default::default(),
            },
        }
    }

    fn set_roles(isolation: &mut IsolationLayer, agent: &str, roles: Vec<String>) {
        isolation
            .try_register_agent(AgentIdentity {
                agent_id: agent.into(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                roles,
            })
            .expect("register test identity");
    }

    /// Does `agent`'s registry resolve `name`? (Also returns its cache salt.)
    fn resolves(
        catalog: &ForeignSourceCatalog,
        isolation: &IsolationLayer,
        agent: &str,
        name: &str,
    ) -> (bool, String) {
        let scoped = catalog.registry_for(&carrier(agent), isolation);
        let found = scoped.registry().get(name).is_some();
        (found, scoped.cache_salt().to_string())
    }

    #[test]
    fn a_shared_source_needs_an_exact_grant_and_revocation_stops_use() {
        let mut isolation = crate::server::state::ServerState::test_isolation("share-admin");
        let catalog = ForeignSourceCatalog::default();
        catalog
            .register(&carrier("agent-a"), "src".into(), spec())
            .expect("registering the shared source succeeds");
        provision_share_role(&mut isolation, "agent-a", "src").unwrap();
        provision_share_role(&mut isolation, "agent-a", "src").unwrap();
        set_roles(&mut isolation, "agent-b", Vec::new());
        let qualified = shared_name("agent-a", "src");
        assert!(
            resolves(&catalog, &isolation, "agent-a", "src").0,
            "owner uses its own"
        );

        // No grant: not found under either name.
        let (found, before) = resolves(&catalog, &isolation, "agent-b", &qualified);
        assert!(!found && !resolves(&catalog, &isolation, "agent-b", "src").0);

        // A broad wildcard reader role never conveys use of a credential.
        isolation
            .try_add_role(crate::acl::Role::new("reader"))
            .unwrap();
        isolation
            .try_add_grant(crate::acl::Grant {
                role: "reader".into(),
                resource: crate::acl::ResourceSelector::All,
                action: crate::acl::RbacAction::Read,
                effect: crate::acl::GrantEffect::Allow,
            })
            .unwrap();
        set_roles(&mut isolation, "agent-b", vec!["reader".into()]);
        assert!(!resolves(&catalog, &isolation, "agent-b", &qualified).0);

        // Grant: the engine-provisioned role makes the qualified name resolve.
        set_roles(
            &mut isolation,
            "agent-b",
            vec![share_role("agent-a", "src")],
        );
        let (found, granted) = resolves(&catalog, &isolation, "agent-b", &qualified);
        assert!(found, "granted principal uses the shared source");
        assert_ne!(before, granted, "a grant changes the result-cache salt");
        assert!(
            !resolves(&catalog, &isolation, "agent-b", "src").0,
            "only qualified"
        );

        // Revoke: removing the role stops use on the very next resolution.
        set_roles(&mut isolation, "agent-b", Vec::new());
        let (found, revoked) = resolves(&catalog, &isolation, "agent-b", &qualified);
        assert!(!found, "revocation stops use");
        assert_eq!(before, revoked);
    }
}
