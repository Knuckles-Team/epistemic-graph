use super::{AccessLevel, AgentIdentity, AgentRole, IsolationLayer};
use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
use crate::protocol::GraphType;

#[test]
fn lease_is_opaque_non_clone_and_has_one_mint_authority() {
    let source = include_str!("policy_lease.rs");
    assert_eq!(
        source.matches("pub fn mint_policy_decision_lease(").count(),
        1,
        "the isolation owner must expose exactly one lease-mint operation"
    );
    assert_eq!(
        source.matches("Ok(PolicyDecisionLease {").count(),
        1,
        "the opaque lease must have exactly one construction site"
    );
    assert!(
        source.contains("#[cfg(feature = \"security\")]\npub struct PolicyDecisionLease {")
            && !source.contains("impl Clone for PolicyDecisionLease"),
        "the lease declaration must remain free of Clone derives and implementations"
    );

    let declaration = source
        .split_once("pub struct PolicyDecisionLease {")
        .expect("lease declaration remains present")
        .1
        .split_once("\n}")
        .expect("lease declaration remains a named-field struct")
        .0;
    assert!(
        declaration
            .lines()
            .filter(|line| line.contains(':'))
            .all(|line| !line.trim_start().starts_with("pub ")),
        "every lease field must remain private"
    );
    assert!(declaration.contains("originating_principal: String"));
    assert!(declaration.contains("effective_actor: String"));
    assert!(declaration.contains("resource: String"));
    assert!(!source.contains("GraphPolicyLease"));
    assert!(!source.contains("fn actor(&self)"));
    assert!(!source.contains("fn graph(&self)"));
}

#[test]
fn persistence_modules_cannot_mint_policy_leases() {
    let sources = [
        include_str!("../rbac_persist.rs"),
        include_str!("../rbac_persist/durable_write.rs"),
        include_str!("../rbac_persist/memory_store.rs"),
    ];
    for source in sources {
        assert!(!source.contains("fn mint_policy_decision_lease"));
        assert!(!source.contains("PolicyDecisionLease {"));
    }

    let module_root = include_str!("../rbac_persist.rs");
    assert!(module_root.contains("mod memory_store;"));
    assert!(!module_root.contains("pub mod memory_store;"));
    assert!(module_root.contains("mod durable_write;"));
    assert!(!module_root.contains("pub mod durable_write;"));

    let isolation_root = include_str!("../isolation.rs");
    assert!(!isolation_root.contains("fn accessible_graphs"));
}

#[test]
fn lease_decision_and_freshness_use_only_atomic_authority_snapshots() {
    let lease_source = include_str!("policy_lease.rs");
    let mint = lease_source
        .split_once("pub fn mint_policy_decision_lease(")
        .expect("sole mint function exists")
        .1;
    assert_eq!(mint.matches(".authority_snapshot()").count(), 1);
    assert!(!mint.contains("store.load()"));
    assert!(!mint.contains("store.current_version()"));
    assert!(!mint.contains("self.agents"));
    assert!(!mint.contains("self.rbac"));

    let freshness = lease_source
        .split_once("fn reload_and_verify_fresh(")
        .expect("freshness function exists")
        .1
        .split_once("pub fn validate_before(")
        .expect("freshness function has a bounded body")
        .0;
    assert_eq!(freshness.matches(".authority_snapshot()").count(), 1);
    assert!(!freshness.contains(".load()"));
    assert!(!freshness.contains(".current_version()"));

    let persist_source = include_str!("../rbac_persist.rs");
    let snapshot = persist_source
        .split_once("pub struct RbacAuthoritySnapshot {")
        .expect("atomic authority snapshot exists")
        .1
        .split_once("\n}")
        .expect("snapshot remains a named-field struct")
        .0;
    for field in [
        "revision: u64",
        "policy_digest: String",
        "identity_digest: String",
        "policy: RbacPolicy",
        "identities: BTreeMap<String, AgentIdentity>",
    ] {
        assert!(snapshot.contains(field), "missing atomic field: {field}");
    }

    let durable_snapshot = persist_source
        .split_once("pub fn authority_snapshot(&self)")
        .expect("durable atomic snapshot reader exists")
        .1
        .split_once("pub fn save(")
        .expect("durable snapshot reader has a bounded body")
        .0;
    // The invariant is unchanged -- the image and its revision come from ONE
    // store snapshot -- but the snapshot is now the storage kernel's scoped
    // read rather than a private `Database::begin_read()`. Two `scoped_read()`
    // calls in this body would be two snapshots, which is what this rejects.
    assert_eq!(durable_snapshot.matches("self.scoped_read()").count(), 1);
    assert!(!durable_snapshot.contains(".begin_read()"));
    assert!(durable_snapshot.contains("read_authority_image(&read)"));
    assert!(durable_snapshot.contains("eg_transaction::version(&read)"));

    let memory_source = include_str!("../rbac_persist/memory_store.rs");
    assert!(memory_source.contains("IdentityBootstrapState,\n        u64,"));
    assert!(memory_source.contains("let state = self.state.read();"));
    assert!(!memory_source.contains("AtomicU64"));
}

#[test]
fn server_binding_keeps_originating_and_effective_identities_separate() {
    let source = include_str!("../../../../src/server/handlers/knowledge_stream/mod.rs");
    assert!(source.contains("lease.originating_principal() == claims.principal.as_str()"));
    assert!(source.contains("lease.effective_actor() == claims.agent_id.as_str()"));
    assert!(source.contains("carrier.agent_id() == claims.agent_id.as_str()"));
    assert!(source.contains("lease.effective_actor() == effective_actor"));
    assert!(source.contains("\"policy-originating-principal\""));
    assert!(source.contains("\"policy-effective-actor\""));
    assert!(source.contains("let actor_scope = carrier.actor_scope()"));
}

#[test]
fn tenant_rbac_isolation_remains_default_deny() {
    let mut layer = IsolationLayer::new();
    layer.add_role(Role::new("tenant:alpha"));
    layer.add_grant(Grant {
        role: "tenant:alpha".into(),
        resource: ResourceSelector::Graph("tenant__alpha____commons__".into()),
        action: RbacAction::Read,
        effect: GrantEffect::Allow,
    });
    layer.register_agent(AgentIdentity {
        agent_id: "alpha-reader".into(),
        role: AgentRole::Agent,
        teams: vec![],
        roles: vec!["tenant:alpha".into()],
    });
    layer.register_agent(AgentIdentity {
        agent_id: "beta-reader".into(),
        role: AgentRole::Agent,
        teams: vec![],
        roles: vec![],
    });

    assert!(layer.check_access(
        "alpha-reader",
        "tenant__alpha____commons__",
        GraphType::Agent,
        None,
        AccessLevel::Read,
    ));
    assert!(!layer.check_access(
        "beta-reader",
        "tenant__alpha____commons__",
        GraphType::Agent,
        None,
        AccessLevel::Read,
    ));
    assert!(!layer.check_access(
        "alpha-reader",
        "tenant__beta____commons__",
        GraphType::Agent,
        None,
        AccessLevel::Read,
    ));
}
