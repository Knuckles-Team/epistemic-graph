//! Class invariants (IDM-05) and the RBAC projection (IDM-03).

use super::*;
use crate::acl::{GrantEffect, RbacAction, ResourceSelector};

fn role(role_id: &str, scopes: &[&str]) -> IdentityOp {
    IdentityOp::Access(AccessOp::UpsertRole {
        request: RoleUpsert {
            role_id: role_id.to_string(),
            name: role_id.to_string(),
            description: None,
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            graph_grants: vec![RoleGraphGrant {
                resource: ResourceSelector::Graph(format!("g-{role_id}")),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            }],
        },
    })
}

fn bind(principal: &str, role_id: &str, change: BindingChange) -> IdentityOp {
    IdentityOp::Access(AccessOp::ChangeUserRole {
        request: UserRoleChange {
            principal_id: principal.to_string(),
            role_id: role_id.to_string(),
            change,
        },
    })
}

fn join(principal: &str, group: &str) -> IdentityOp {
    IdentityOp::Access(AccessOp::ChangeMembership {
        request: GroupMembershipChange {
            group_id: group.to_string(),
            principal_id: principal.to_string(),
            change: BindingChange::Add,
        },
    })
}

/// Creates `alice` in `store` and binds her to a fresh `reader` role granting
/// `kg:read` -- the RBAC-projection and SQL-dump fixtures' shared setup.
fn bind_alice_as_reader(store: &mut IdentityStore) -> String {
    apply_kept(store, &role("reader", &["kg:read"]), &admin(), NOW).unwrap();
    let alice = create(store, "alice", UserKind::Human).unwrap();
    apply_kept(
        store,
        &bind(&alice, "reader", BindingChange::Add),
        &admin(),
        NOW,
    )
    .unwrap();
    alice
}

#[test]
fn an_unregistered_scope_cannot_be_put_on_a_role() {
    let mut store = store_in(AuthMode::Local);
    assert_eq!(
        apply_kept(&mut store, &role("r", &["made:up"]), &admin(), NOW),
        Err(IdentityRefusal::UnknownScope)
    );
    assert!(apply_kept(&mut store, &role("r", &["kg:read"]), &admin(), NOW).is_ok());
}

#[test]
fn a_service_only_scope_reaches_a_service_and_never_a_human() {
    let mut store = store_in(AuthMode::Local);
    apply_kept(
        &mut store,
        &role("throttler", &["capacity:throttle"]),
        &admin(),
        NOW,
    )
    .unwrap();
    let service = create(&mut store, "graph-os", UserKind::Service).unwrap();
    let human = create(&mut store, "alice", UserKind::Human).unwrap();
    assert!(apply_kept(
        &mut store,
        &bind(&service, "throttler", BindingChange::Add),
        &admin(),
        NOW
    )
    .is_ok());
    assert_eq!(
        apply_kept(
            &mut store,
            &bind(&human, "throttler", BindingChange::Add),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::ClassViolation)
    );
}

#[test]
fn an_approver_scope_is_held_only_through_its_built_in_group_by_a_human() {
    let mut store = store_in(AuthMode::Local);
    let human = create(&mut store, "alice", UserKind::Human).unwrap();
    let service = create(&mut store, "svc", UserKind::Service).unwrap();
    assert_eq!(
        apply_kept(
            &mut store,
            &bind(&human, ELEVATION_APPROVER_ROLE, BindingChange::Add),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::ClassViolation),
        "never granted directly"
    );
    assert_eq!(
        apply_kept(
            &mut store,
            &join(&service, ELEVATION_APPROVERS_GROUP),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::ClassViolation),
        "never held by a service"
    );
    let other_group = IdentityOp::Access(AccessOp::UpsertGroup {
        request: GroupUpsert {
            group_id: "helpers".to_string(),
            name: "helpers".to_string(),
            roles: [ELEVATION_APPROVER_ROLE.to_string()].into(),
            mfa_required: false,
        },
    });
    assert_eq!(
        apply_kept(&mut store, &other_group, &admin(), NOW),
        Err(IdentityRefusal::ClassViolation),
        "never through another group"
    );
    apply_kept(
        &mut store,
        &join(&human, ELEVATION_APPROVERS_GROUP),
        &admin(),
        NOW,
    )
    .unwrap();
    let resolved = store.resolve(&human, &TestRegistry).unwrap();
    assert!(resolved.scopes.contains("rbac:approve-elevation"));
    assert!(!resolved.scopes.contains("finance:approve-live-order"));
}

#[test]
fn a_built_in_group_keeps_its_roles_and_a_built_in_role_cannot_be_removed() {
    let mut store = store_in(AuthMode::Local);
    let rewire = IdentityOp::Access(AccessOp::UpsertGroup {
        request: GroupUpsert {
            group_id: ELEVATION_APPROVERS_GROUP.to_string(),
            name: "x".to_string(),
            roles: BTreeSet::new(),
            mfa_required: true,
        },
    });
    assert_eq!(
        apply_kept(&mut store, &rewire, &admin(), NOW),
        Err(IdentityRefusal::BuiltIn)
    );
    let remove = IdentityOp::Access(AccessOp::RemoveRole {
        request: ObjectRef {
            id: ADMIN_ROLE.to_string(),
        },
    });
    assert_eq!(
        apply_kept(&mut store, &remove, &admin(), NOW),
        Err(IdentityRefusal::BuiltIn)
    );
    let require_mfa = IdentityOp::Access(AccessOp::UpsertGroup {
        request: GroupUpsert {
            group_id: ELEVATION_APPROVERS_GROUP.to_string(),
            name: ELEVATION_APPROVERS_GROUP.to_string(),
            roles: [ELEVATION_APPROVER_ROLE.to_string()].into(),
            mfa_required: true,
        },
    });
    assert!(apply_kept(&mut store, &require_mfa, &admin(), NOW).is_ok());
}

#[test]
fn the_projection_is_the_full_rbac_identity_and_follows_every_removal() {
    let mut store = store_in(AuthMode::Local);
    let alice = bind_alice_as_reader(&mut store);
    let projection = store.rbac_projection();
    let identity = &projection.identities[&alice];
    assert!(identity.roles.contains(&rbac_role_name("reader")));
    assert!(projection
        .grants
        .iter()
        .any(|grant| grant.role == "idm:reader"));
    apply_kept(
        &mut store,
        &bind(&alice, "reader", BindingChange::Remove),
        &admin(),
        NOW,
    )
    .unwrap();
    let projection = store.rbac_projection();
    assert!(!projection.identities[&alice]
        .roles
        .contains(&rbac_role_name("reader")));
    let disable = IdentityOp::User(UserOp::SetStatus {
        request: UserStatusChange {
            principal_id: alice.clone(),
            status: UserStatus::Disabled,
        },
    });
    apply_kept(&mut store, &disable, &admin(), NOW).unwrap();
    let projection = store.rbac_projection();
    assert!(
        !projection.identities.contains_key(&alice),
        "an inactive principal holds nothing"
    );
    assert!(
        projection.managed.contains(&alice),
        "but stays owned by the store"
    );
}

#[test]
fn the_last_active_administrator_cannot_be_taken_out_of_service() {
    let mut store = store_in(AuthMode::Local);
    let disable = |principal: &str| {
        IdentityOp::User(UserOp::SetStatus {
            request: UserStatusChange {
                principal_id: principal.to_string(),
                status: UserStatus::Disabled,
            },
        })
    };
    assert_eq!(
        apply_kept(&mut store, &disable(BOOTSTRAP_PRINCIPAL), &admin(), NOW),
        Err(IdentityRefusal::PreconditionFailed)
    );
    let second = create(&mut store, "second-admin", UserKind::Human).unwrap();
    apply_kept(
        &mut store,
        &join(&second, ADMINISTRATORS_GROUP),
        &admin(),
        NOW,
    )
    .unwrap();
    assert!(apply_kept(&mut store, &disable(BOOTSTRAP_PRINCIPAL), &admin(), NOW).is_ok());
}

#[test]
fn admin_ops_need_the_exact_admin_scope() {
    let mut store = store_in(AuthMode::Local);
    let read_only = IdentityStamp::for_actor(actor("usr:x", &[IDENTITY_READ_SCOPE]));
    assert_eq!(
        apply_kept(&mut store, &role("r", &["kg:read"]), &read_only, NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
    let list = IdentityOp::Access(AccessOp::ListRoles);
    assert!(apply_kept(&mut store, &list, &read_only, NOW).is_ok());
    let nothing = IdentityStamp::for_actor(actor("usr:x", &["kg:admin"]));
    assert_eq!(
        apply_kept(&mut store, &list, &nothing, NOW),
        Err(IdentityRefusal::NotAuthorized),
        "kg:admin does not stand in for an identity scope"
    );
}

#[test]
fn a_privileged_mapping_rule_must_say_so() {
    let mut store = store_in(AuthMode::Local);
    let idp = |privileged| {
        IdentityOp::Idp(IdpOp::Upsert {
            request: IdpConfig {
                idp_id: "keycloak".to_string(),
                kind: IdpKind::Oidc,
                display_name: "Keycloak".to_string(),
                enabled: true,
                config_json: "{\"issuer\":\"https://kc.example\"}".to_string(),
                secret_ref: None,
                jit_policy: JitPolicy::Create,
                email_domains: Vec::new(),
                order: 0,
                rules: vec![MappingRule {
                    rule_id: "admins".to_string(),
                    claim_path: "groups".to_string(),
                    match_kind: "equals".to_string(),
                    value: "platform-admins".to_string(),
                    target: format!("group:{ADMINISTRATORS_GROUP}"),
                    privileged,
                }],
            },
        })
    };
    assert_eq!(
        apply_kept(&mut store, &idp(false), &admin(), NOW),
        Err(IdentityRefusal::ClassViolation)
    );
    assert!(apply_kept(&mut store, &idp(true), &admin(), NOW).is_ok());
}

#[test]
fn the_sql_relations_carry_no_secret_and_a_dump_restores_the_structure() {
    let mut store = store_in(AuthMode::Local);
    with_admin_session(&mut store);
    let alice = bind_alice_as_reader(&mut store);
    let text = serde_json::to_string(
        &store
            .sql_relations()
            .iter()
            .map(|relation| &relation.rows)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert!(
        !text.contains("argon2"),
        "a relation leaked a password hash"
    );
    assert!(
        !text.contains(ADMIN_SESSION),
        "a relation leaked a session id"
    );
    let export = IdentityOp::Config(ConfigOp::ExportSql);
    let IdentityReply::Sql(dump) = apply_kept(&mut store, &export, &admin(), NOW).unwrap() else {
        panic!("export answers SQL");
    };
    assert!(dump.contains("CREATE TABLE identity.users"));
    assert!(!dump.contains("argon2") && !dump.contains("identity.sessions"));
    let mut restored = store_in(AuthMode::Local);
    let import = IdentityOp::Config(ConfigOp::ImportSql {
        request: SqlDump { sql: dump.clone() },
    });
    let reader_stamp = IdentityStamp::for_actor(actor("usr:x", &[IDENTITY_READ_SCOPE]));
    assert_eq!(
        apply_kept(&mut restored, &import, &reader_stamp, NOW),
        Err(IdentityRefusal::NotAuthorized),
        "import is administrator-only"
    );
    apply_kept(&mut restored, &import, &admin(), NOW).unwrap();
    let alice_back = restored.resolve(&alice, &TestRegistry).unwrap();
    assert!(alice_back.roles.contains("reader"));
    assert_eq!(
        alice_back.status,
        UserStatus::PendingReset,
        "no credential was carried"
    );
    assert!(restored.credential_of(&alice).is_none());
}

#[test]
fn a_dump_cannot_smuggle_a_forbidden_binding_or_foreign_sql() {
    let mut store = store_in(AuthMode::Local);
    let smuggle = "INSERT INTO identity.users (principal_id, username, kind, status, source) VALUES ('usr:svc', 'svc', 'service', 'active', 'local');\n\
                   INSERT INTO identity.group_members (group_id, principal_id, source) VALUES ('elevation-approvers', 'usr:svc', 'local');\n";
    let import = |sql: &str| {
        IdentityOp::Config(ConfigOp::ImportSql {
            request: SqlDump {
                sql: sql.to_string(),
            },
        })
    };
    assert_eq!(
        apply_kept(&mut store, &import(smuggle), &admin(), NOW),
        Err(IdentityRefusal::ClassViolation)
    );
    assert!(!store.manages("usr:svc"), "the whole import was refused");
    assert_eq!(
        apply_kept(
            &mut store,
            &import("DROP TABLE identity.users;"),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::InvalidRequest)
    );
}
