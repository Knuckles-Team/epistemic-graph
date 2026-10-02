//! The identity store as read-only SQL relations in the one
//! authorized projection -- visible only to exact identity readers, secrets
//! never projected, the namespace reserved.

use super::*;
use crate::server::auth::VerifiedRequestContext;
use crate::server::sql_tables::test_persist_dir;
use eg_types::identity::*;

const TENANT: &str = "tenant-identity-view";

fn authority(scopes: &[&str]) -> CarrierAuthority {
    authority_of("ops", scopes)
}

/// A carrier for the principal `principal:<agent>`.
fn authority_of(agent: &str, scopes: &[&str]) -> CarrierAuthority {
    CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_with_scopes(
        agent, TENANT, scopes,
    ))
    .unwrap()
}

/// A local-mode store whose bootstrap administrator has a password hash.
fn store() -> IdentityStore {
    let mut store = IdentityStore::default();
    let mut stamp = IdentityStamp::for_actor(IdentityActor {
        principal_id: "svc:graph-os".to_string(),
        delegated: false,
        scopes: [IDENTITY_AUTHENTICATE_SCOPE.to_string()].into(),
    });
    stamp.password_hash = Some("$argon2id$v=19$m=1024,t=1,p=1$c2FsdA$aGFzaA".to_string());
    let op = eg_types::test_support::identity::bootstrap_local_op();
    let ctx = ApplyContext {
        now_ms: 1_800_000_000_000,
        classifier: &eg_capabilities::scopes::ScopeRegistry,
    };
    store.apply(&op, &stamp, &ctx).unwrap();
    store
}

fn no_graph() -> super::super::RequestGraph {
    super::super::RequestGraph {
        name: "identity-view-test-graph".to_string(),
        core: std::sync::Arc::new(crate::graph::GraphCore::new()),
    }
}

fn query(dir: &std::path::Path, scopes: &[&str], sql: &str) -> Result<Vec<Vec<String>>, String> {
    query_as(dir, &authority(scopes), sql)
}

fn query_as(
    dir: &std::path::Path,
    authority: &CarrierAuthority,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let projection =
        super::super::authorized_read_store_for_query(authority, dir, sql, None, &no_graph())?;
    let view = crate::graph::GraphView::default();
    let typed = eg_query::exec_sql_typed_with_tables(&view, projection.store(), sql)?;
    Ok(typed
        .rows
        .iter()
        .map(|row| row.iter().map(|cell| format!("{cell}")).collect())
        .collect())
}

#[test]
fn an_identity_reader_selects_users_and_nobody_else_sees_the_relations() {
    let dir = test_persist_dir();
    std::fs::create_dir_all(&dir).unwrap();
    crate::server::identity_view::publish(dir.to_str(), &store());
    let sql = "SELECT principal_id, username, has_password FROM __identity__users";
    let rows = query(&dir, &["identity:read"], sql).unwrap();
    assert_eq!(
        rows,
        vec![vec![
            "\"usr:bootstrap\"".to_string(),
            "\"root\"".to_string(),
            "true".to_string(),
        ]]
    );
    for outsider in [&["kg:admin"][..], &["*"], &["kg:read"], &["identity:self"]] {
        assert!(
            query(&dir, outsider, sql).is_err(),
            "{outsider:?} must not see identity relations"
        );
    }
}

/// `store()` plus `ann` (`principal:ann`), an administrator.
fn store_with_ann() -> IdentityStore {
    let mut store = store();
    let admin = IdentityStamp::for_actor(IdentityActor {
        principal_id: BOOTSTRAP_PRINCIPAL.to_string(),
        delegated: false,
        scopes: [IDENTITY_ADMIN_SCOPE.to_string()].into(),
    });
    let create = IdentityOp::User(UserOp::Create {
        request: CreateUserRequest {
            username: "ann".to_string(),
            kind: UserKind::Human,
            principal_id: Some("principal:ann".to_string()),
            display_name: None,
            email: None,
            roles: std::collections::BTreeSet::new(),
            groups: [ADMINISTRATORS_GROUP.to_string()].into(),
            password: Secret::default(),
            must_change: false,
        },
    });
    apply(&mut store, &create, &admin);
    store
}

fn apply(store: &mut IdentityStore, op: &IdentityOp, stamp: &IdentityStamp) {
    let ctx = ApplyContext {
        now_ms: 1_800_000_000_000,
        classifier: &eg_capabilities::scopes::ScopeRegistry,
    };
    store.apply(op, stamp, &ctx).unwrap();
}

#[test]
fn a_reader_the_store_no_longer_backs_sees_no_identity_relations() {
    let dir = test_persist_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let mut store = store_with_ann();
    crate::server::identity_view::publish(dir.to_str(), &store);
    let sql = "SELECT principal_id FROM __identity__users";
    let ann = authority_of("ann", &["identity:read"]);
    assert_eq!(query_as(&dir, &ann, sql).unwrap().len(), 2);
    let admin = IdentityStamp::for_actor(IdentityActor {
        principal_id: BOOTSTRAP_PRINCIPAL.to_string(),
        delegated: false,
        scopes: [IDENTITY_ADMIN_SCOPE.to_string()].into(),
    });
    let disable = IdentityOp::User(UserOp::SetStatus {
        request: UserStatusChange {
            principal_id: "principal:ann".to_string(),
            status: UserStatus::Disabled,
        },
    });
    apply(&mut store, &disable, &admin);
    crate::server::identity_view::publish(dir.to_str(), &store);
    assert!(
        query_as(&dir, &ann, sql).is_err(),
        "a disabled reader's previous token still reads the identity relations"
    );
}

#[test]
fn no_relation_projects_a_secret() {
    let dir = test_persist_dir();
    std::fs::create_dir_all(&dir).unwrap();
    crate::server::identity_view::publish(dir.to_str(), &store());
    for relation in store().sql_relations() {
        let sql = format!("SELECT * FROM {IDENTITY_RELATION_PREFIX}{}", relation.name);
        let rows = query(&dir, &["identity:admin"], &sql).unwrap();
        let text = format!("{rows:?}");
        assert!(!text.contains("argon2"), "{} leaked a hash", relation.name);
    }
}

#[test]
fn a_tenant_table_cannot_take_the_reserved_namespace() {
    assert!(refuse_reserved_name("__identity__users").is_err());
    assert!(refuse_reserved_name("__IDENTITY__users").is_err());
    assert!(refuse_reserved_name("identity_users").is_ok());
}
