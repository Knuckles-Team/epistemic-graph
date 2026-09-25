//! IDM-01: the identity store as read-only SQL relations in the one
//! authorized projection -- visible only to exact identity readers, secrets
//! never projected, the namespace reserved.

use super::*;
use crate::server::auth::VerifiedRequestContext;
use crate::server::sql_tables::test_persist_dir;
use eg_types::identity::*;

const TENANT: &str = "tenant-identity-view";

fn authority(scopes: &[&str]) -> CarrierAuthority {
    CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_with_scopes(
        "ops", TENANT, scopes,
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
    let op = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode: AuthMode::Local,
            admin_username: Some("root".to_string()),
            admin_password: Secret::default(),
        },
    });
    let ctx = ApplyContext {
        now_ms: 1_800_000_000_000,
        classifier: &eg_capabilities::scopes::ScopeRegistry,
    };
    store.apply(&op, &stamp, &ctx).unwrap();
    store
}

fn query(dir: &std::path::Path, scopes: &[&str], sql: &str) -> Result<Vec<Vec<String>>, String> {
    let projection = super::super::authorized_read_store_for_query(&authority(scopes), dir, sql)?;
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
