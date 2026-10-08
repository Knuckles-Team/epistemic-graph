use super::*;

const TENANT_GRAPH: &str = "tenant__homelab__default";

fn tenant_access(layer: &IsolationLayer) -> bool {
    layer.check_access(
        "usr:alice",
        TENANT_GRAPH,
        GraphType::Agent,
        None,
        AccessLevel::Write,
    )
}

fn provision(layer: &mut IsolationLayer) {
    layer
        .provision_tenant_graph_access(TENANT_GRAPH, Some("usr:alice"))
        .unwrap();
}

fn refresh(layer: &mut IsolationLayer) {
    apply(
        layer,
        reports_role(Vec::new()),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
}

/// Apply `op` as an identity administrator, then re-project the store.
fn admin_then_refresh(layer: &mut IsolationLayer, op: IdentityOp) {
    apply(layer, op, &stamp(IDENTITY_ADMIN_SCOPE)).unwrap();
    refresh(layer);
}

/// Alice has no tenant access, and provisioning cannot restore it.
fn assert_tenant_closed(layer: &mut IsolationLayer) {
    assert!(!tenant_access(layer));
    assert!(layer
        .provision_tenant_graph_access(TENANT_GRAPH, Some("usr:alice"))
        .is_err());
}

/// The saved image, after checking that the saved identity of `principal`
/// equals the store's projection of it.
fn saved_principal(layer: &IsolationLayer, principal: &str) -> (RbacPolicy, AgentIdentity) {
    let (policy, identities, _) = layer.policy_store().unwrap().load().unwrap();
    assert_eq!(
        serde_json::to_value(&policy.identity_store().rbac_projection().identities[principal])
            .unwrap(),
        serde_json::to_value(&identities[principal]).unwrap()
    );
    let identity = identities[principal].clone();
    (policy, identity)
}

#[test]
fn managed_tenant_binding_survives_projection_and_is_in_the_saved_store() {
    let mut layer = seeded();
    provision(&mut layer);
    assert!(tenant_access(&layer));
    refresh(&mut layer);
    assert!(tenant_access(&layer));
    let (_, alice) = saved_principal(&layer, "usr:alice");
    assert!(alice.roles.contains(&"idm:tenant:homelab".to_string()));
    assert!(!alice.roles.contains(&"tenant:homelab".to_string()));
}

#[test]
fn tenant_binding_revocation_and_disable_survive_later_projection() {
    let mut layer = seeded();
    provision(&mut layer);
    apply(
        &mut layer,
        IdentityOp::Access(AccessOp::ChangeUserRole {
            request: UserRoleChange {
                principal_id: "usr:alice".to_string(),
                role_id: "tenant:homelab".to_string(),
                change: BindingChange::Remove,
            },
        }),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    refresh(&mut layer);
    assert!(!tenant_access(&layer));
    provision(&mut layer);
    admin_then_refresh(
        &mut layer,
        IdentityOp::User(UserOp::SetStatus {
            request: UserStatusChange {
                principal_id: "usr:alice".to_string(),
                status: UserStatus::Disabled,
            },
        }),
    );
    assert_tenant_closed(&mut layer);
}

#[test]
fn managed_tenant_provisioning_does_not_import_existing_projected_roles() {
    let mut layer = seeded();
    let mut forged = layer.get_identity("usr:alice").unwrap().clone();
    forged.roles.push("unrelated-admin".to_string());
    layer.try_register_agent(forged).unwrap();
    provision(&mut layer);
    assert!(!layer
        .get_identity("usr:alice")
        .unwrap()
        .roles
        .contains(&"unrelated-admin".to_string()));
}

#[test]
fn conflicting_tenant_role_cannot_smuggle_scopes_into_a_creator() {
    let mut layer = seeded();
    apply(
        &mut layer,
        IdentityOp::Access(AccessOp::UpsertRole {
            request: RoleUpsert {
                role_id: "tenant:homelab".to_string(),
                name: "collision".to_string(),
                description: None,
                scopes: BTreeSet::from(["identity:admin".to_string()]),
                graph_grants: Vec::new(),
            },
        }),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    let before = serde_json::to_string(layer.rbac()).unwrap();
    assert!(layer
        .provision_tenant_graph_access(TENANT_GRAPH, Some("usr:alice"))
        .is_err());
    assert_eq!(serde_json::to_string(layer.rbac()).unwrap(), before);
    assert!(!tenant_access(&layer));
}

#[test]
fn failed_managed_tenant_save_rolls_back_binding_and_projection() {
    let mut layer = seeded();
    let before = serde_json::to_string(layer.rbac()).unwrap();
    let identity = layer.get_identity("usr:alice").unwrap().clone();
    layer.persist = Some(Arc::new(FailingStore));
    assert!(layer
        .provision_tenant_graph_access(TENANT_GRAPH, Some("usr:alice"))
        .is_err());
    assert_eq!(serde_json::to_string(layer.rbac()).unwrap(), before);
    assert_eq!(
        serde_json::to_value(layer.get_identity("usr:alice").unwrap()).unwrap(),
        serde_json::to_value(identity).unwrap()
    );
    assert!(!tenant_access(&layer));
}

fn absent_grant() -> crate::acl::Grant {
    crate::acl::Grant {
        role: "absent".to_string(),
        resource: ResourceSelector::Graph("absent".to_string()),
        action: RbacAction::Read,
        effect: GrantEffect::Allow,
    }
}

#[test]
fn no_op_grant_removal_saves_its_audit_entry() {
    let mut layer = seeded();
    let store = layer.policy_store().unwrap();
    let version = store.current_version().unwrap();
    assert!(!layer
        .try_rbac_admin_audited(RbacAdminOp::RemoveGrant(absent_grant()), actor())
        .unwrap());
    assert_eq!(store.current_version().unwrap(), version + 1);
    let (saved, _, _) = store.load().unwrap();
    assert_eq!(saved.identity_store(), layer.rbac().identity_store());
    assert_eq!(
        saved
            .identity_store()
            .audit_trail()
            .entries()
            .last()
            .unwrap()
            .target
            .as_deref(),
        Some("absent")
    );
}

#[test]
fn failed_no_op_grant_audit_save_rolls_back_the_audit_entry() {
    let mut layer = seeded();
    let before = serde_json::to_string(layer.rbac()).unwrap();
    layer.persist = Some(Arc::new(FailingStore));
    assert!(layer
        .try_rbac_admin_audited(RbacAdminOp::RemoveGrant(absent_grant()), actor())
        .is_err());
    assert_eq!(serde_json::to_string(layer.rbac()).unwrap(), before);
}

#[test]
fn tenant_role_removal_does_not_reappear_on_unrelated_identity_writes() {
    let mut layer = seeded();
    provision(&mut layer);
    apply(
        &mut layer,
        IdentityOp::Access(AccessOp::RemoveRole {
            request: ObjectRef {
                id: "tenant:homelab".to_string(),
            },
        }),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    refresh(&mut layer);
    assert!(!tenant_access(&layer));
}

#[test]
fn revoked_tenant_grants_are_not_recreated_or_overwritten_by_provisioning() {
    let mut layer = seeded();
    provision(&mut layer);
    admin_then_refresh(
        &mut layer,
        IdentityOp::Access(AccessOp::UpsertRole {
            request: RoleUpsert {
                role_id: "tenant:homelab".to_string(),
                name: "tenant:homelab".to_string(),
                description: None,
                scopes: BTreeSet::new(),
                graph_grants: Vec::new(),
            },
        }),
    );
    assert_tenant_closed(&mut layer);
    assert!(!tenant_access(&layer));
}

#[test]
fn managed_service_tenant_binding_survives_projection_and_persistence() {
    const SERVICE: &str = "svc:tenant-worker";
    let mut layer = seeded();
    apply(
        &mut layer,
        IdentityOp::User(UserOp::Create {
            request: CreateUserRequest {
                username: "tenant-worker".to_string(),
                kind: UserKind::Service,
                principal_id: Some(SERVICE.to_string()),
                display_name: None,
                email: None,
                roles: BTreeSet::new(),
                groups: BTreeSet::new(),
                password: Secret::default(),
                must_change: false,
            },
        }),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    layer
        .provision_tenant_graph_access(TENANT_GRAPH, Some(SERVICE))
        .unwrap();
    refresh(&mut layer);
    assert!(layer.check_access(
        SERVICE,
        TENANT_GRAPH,
        GraphType::Agent,
        None,
        AccessLevel::Write,
    ));
    assert!(!layer.check_access(
        SERVICE,
        "tenant__other__default",
        GraphType::Agent,
        None,
        AccessLevel::Write,
    ));
    let (policy, service) = saved_principal(&layer, SERVICE);
    let store = policy.identity_store();
    assert_eq!(store.kind_of(SERVICE), Some(UserKind::Service));
    assert_eq!(service.roles, vec!["idm:tenant:homelab".to_string()]);
    assert!(store.resolve(SERVICE, &Registry).unwrap().scopes.is_empty());
}

fn assert_named_tenant_access(layer: &IsolationLayer, graph: &str, expected: bool) {
    assert_eq!(
        layer.check_access(
            "usr:alice",
            graph,
            GraphType::Agent,
            None,
            AccessLevel::Write
        ),
        expected,
        "unexpected tenant access for {graph}"
    );
}

fn check_encoded_tenant_role(slug: &str, role_id: &str) {
    let mut layer = seeded();
    let graph = format!("tenant__{slug}__default");
    layer
        .provision_tenant_graph_access(&graph, Some("usr:alice"))
        .unwrap();
    refresh(&mut layer);
    assert_named_tenant_access(&layer, &graph, true);
    assert_named_tenant_access(&layer, &format!("tenant__{slug}-other__default"), false);
    assert_named_tenant_access(&layer, "tenant__acme__default", false);
    let (policy, alice) = saved_principal(&layer, "usr:alice");
    assert!(alice.roles.contains(&format!("idm:{role_id}")));
    assert_exact_saved_tenant_grants(policy.identity_store(), slug, role_id);
    apply(
        &mut layer,
        IdentityOp::Access(AccessOp::ChangeUserRole {
            request: UserRoleChange {
                principal_id: "usr:alice".to_string(),
                role_id: role_id.to_string(),
                change: BindingChange::Remove,
            },
        }),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    refresh(&mut layer);
    assert_named_tenant_access(&layer, &graph, false);
    layer
        .provision_tenant_graph_access(&graph, Some("usr:alice"))
        .unwrap();
    assert_named_tenant_access(&layer, &graph, true);
    apply(
        &mut layer,
        IdentityOp::Access(AccessOp::RemoveRole {
            request: ObjectRef {
                id: role_id.to_string(),
            },
        }),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    refresh(&mut layer);
    assert_named_tenant_access(&layer, &graph, false);
}

#[test]
fn engine_tenant_names_outside_identity_id_syntax_remain_scoped_and_revocable() {
    for (slug, role_id) in [
        (
            "Acme".to_string(),
            "tenant-sha256:37036cd8f9746d335038eca92f8a73ae5f1bca4779a1e55e5812e37743b2f5bf",
        ),
        (
            "café".to_string(),
            "tenant-sha256:850f7dc43910ff890f8879c0ed26fe697c93a067ad93a7d50f466a7028a9bf4e",
        ),
        (
            "cafe\u{301}".to_string(),
            "tenant-sha256:81ef060bcd98adc7824eb5c1ada83c32491b16018e11e79f00ab9d09e04b015a",
        ),
        (
            "x".repeat(250),
            "tenant-sha256:086d4a1c293bde318dc1fec9a21b9d828ba7637bcbdc5cdb42662fd84b733e9f",
        ),
    ] {
        check_encoded_tenant_role(&slug, role_id);
    }
}

#[test]
fn hashed_tenant_namespace_cannot_alias_a_preserved_tenant_role() {
    const HASHED_ROLE: &str =
        "tenant-sha256:37036cd8f9746d335038eca92f8a73ae5f1bca4779a1e55e5812e37743b2f5bf";
    let mut layer = seeded();
    let ordinary_graph = format!("tenant__{HASHED_ROLE}__default");
    for graph in ["tenant__Acme__default", ordinary_graph.as_str()] {
        layer
            .provision_tenant_graph_access(graph, Some("usr:alice"))
            .unwrap();
    }
    refresh(&mut layer);
    let roles = &layer.get_identity("usr:alice").unwrap().roles;
    assert!(roles.contains(&format!("idm:{HASHED_ROLE}")));
    assert!(roles.contains(&format!("idm:tenant:{HASHED_ROLE}")));
    apply(
        &mut layer,
        IdentityOp::Access(AccessOp::RemoveRole {
            request: ObjectRef {
                id: HASHED_ROLE.to_string(),
            },
        }),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    refresh(&mut layer);
    assert_named_tenant_access(&layer, "tenant__Acme__default", false);
    assert_named_tenant_access(&layer, &ordinary_graph, true);
}

fn assert_exact_saved_tenant_grants(store: &IdentityStore, slug: &str, role_id: &str) {
    let saved_projection = store.rbac_projection();
    for action in [RbacAction::Read, RbacAction::Write] {
        assert!(saved_projection.grants.contains(&crate::acl::Grant {
            role: format!("idm:{role_id}"),
            resource: ResourceSelector::Pattern(format!("tenant__{slug}__*")),
            action,
            effect: GrantEffect::Allow,
        }));
    }
}
