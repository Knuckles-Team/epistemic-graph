//! A token outlives the authority it was minted with. For a principal the
//! store owns, the store -- not the token -- says what it holds now: a
//! disabled, deprovisioned or demoted administrator's unexpired token is
//! refused, for mutations and for reads.

use super::*;

/// A second administrator, `ann`, and the token she was issued while one.
fn second_administrator(store: &mut IdentityStore) -> (String, IdentityStamp) {
    let ann = create(store, "ann", UserKind::Human).unwrap();
    apply_kept(store, &membership(&ann, BindingChange::Add), &admin(), NOW).unwrap();
    let token = IdentityStamp::for_actor(actor(
        &ann,
        &[
            IDENTITY_ADMIN_SCOPE,
            IDENTITY_READ_SCOPE,
            IDENTITY_SELF_SCOPE,
        ],
    ));
    (ann, token)
}

fn membership(principal: &str, change: BindingChange) -> IdentityOp {
    IdentityOp::Access(AccessOp::ChangeMembership {
        request: GroupMembershipChange {
            group_id: ADMINISTRATORS_GROUP.to_string(),
            principal_id: principal.to_string(),
            change,
        },
    })
}

fn status(principal: &str, status: UserStatus) -> IdentityOp {
    IdentityOp::User(UserOp::SetStatus {
        request: UserStatusChange {
            principal_id: principal.to_string(),
            status,
        },
    })
}

fn list_users() -> IdentityOp {
    IdentityOp::User(UserOp::List {
        request: ListQuery {
            after: None,
            limit: 10,
        },
    })
}

/// What `token` can still do: `(a mutation, a read)`.
fn standing(store: &mut IdentityStore, token: &IdentityStamp, ann: &str) -> (bool, bool) {
    let mutation = apply_kept(store, &status(ann, UserStatus::Active), token, NOW);
    let read = apply_kept(store, &list_users(), token, NOW);
    for refused in [mutation.as_ref().err(), read.as_ref().err()] {
        assert!(matches!(
            refused,
            None | Some(IdentityRefusal::NotAuthorized)
        ));
    }
    (mutation.is_ok(), read.is_ok())
}

#[test]
fn a_disabled_administrators_token_cannot_re_enable_itself_or_read() {
    let mut store = store_in(AuthMode::Local);
    let (ann, token) = second_administrator(&mut store);
    assert_eq!(standing(&mut store, &token, &ann), (true, true));
    apply_kept(
        &mut store,
        &status(&ann, UserStatus::Disabled),
        &admin(),
        NOW,
    )
    .unwrap();
    assert_eq!(
        standing(&mut store, &token, &ann),
        (false, false),
        "a disabled administrator's unexpired token still administers"
    );
    assert_eq!(store.user(&ann).unwrap().status, UserStatus::Disabled);
    apply_kept(&mut store, &status(&ann, UserStatus::Active), &admin(), NOW).unwrap();
    assert_eq!(standing(&mut store, &token, &ann), (true, true));
}

#[test]
fn a_deprovisioned_or_demoted_administrators_token_holds_nothing() {
    let mut store = store_in(AuthMode::Local);
    let (ann, token) = second_administrator(&mut store);
    apply_kept(
        &mut store,
        &membership(&ann, BindingChange::Remove),
        &admin(),
        NOW,
    )
    .unwrap();
    assert_eq!(
        standing(&mut store, &token, &ann),
        (false, false),
        "a token keeps the administrator scope its principal no longer resolves to"
    );
    apply_kept(
        &mut store,
        &membership(&ann, BindingChange::Add),
        &admin(),
        NOW,
    )
    .unwrap();
    assert_eq!(standing(&mut store, &token, &ann), (true, true));
    let gone = status(&ann, UserStatus::Deprovisioned);
    apply_kept(&mut store, &gone, &admin(), NOW).unwrap();
    assert_eq!(standing(&mut store, &token, &ann), (false, false));
    assert_eq!(store.user(&ann).unwrap().status, UserStatus::Deprovisioned);
}

#[test]
fn a_token_is_necessary_even_when_the_store_backs_the_scope() {
    let mut store = store_in(AuthMode::Local);
    let (ann, _) = second_administrator(&mut store);
    let self_only = IdentityStamp::for_actor(actor(&ann, &[IDENTITY_SELF_SCOPE]));
    assert_eq!(
        apply_kept(&mut store, &list_users(), &self_only, NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
}

#[test]
fn first_run_and_the_broker_do_not_need_a_store_record() {
    let mut store = IdentityStore::default();
    let operator = IdentityStamp::for_actor(actor("usr:operator", &[IDENTITY_ADMIN_SCOPE]));
    let mut first_run = operator.clone();
    first_run.password_hash = Some("$argon2id$bootstrap".to_string());
    let initialize = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode: AuthMode::Local,
            admin_username: Some("root-admin".to_string()),
            admin_password: Secret::default(),
        },
    });
    store
        .apply(&initialize, &first_run, &ctx_at(NOW))
        .expect("an empty store is initialized by a token holder");
    let get = IdentityOp::Config(ConfigOp::Get);
    apply_kept(&mut store, &get, &broker(), NOW).expect("the broker owns no store record");
    assert!(store.holds_now(&operator.actor, IDENTITY_ADMIN_SCOPE, &TestRegistry));
    assert!(!store.holds_now(&broker().actor, IDENTITY_ADMIN_SCOPE, &TestRegistry));
}
