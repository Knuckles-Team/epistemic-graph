//! Admin reads and single-session revocation (MCPI-10).

use super::*;

#[test]
fn search_users_pages_in_principal_order_and_checks_read_scope() {
    let mut store = store_in(AuthMode::Local);
    create(&mut store, "alice", UserKind::Human).unwrap();
    create(&mut store, "alicia", UserKind::Human).unwrap();
    create(&mut store, "bob", UserKind::Human).unwrap();
    let op = IdentityOp::User(UserOp::Search {
        request: UserSearch {
            query: "ALI".to_string(),
            after: None,
            limit: 1,
        },
    });
    let reader = IdentityStamp::for_actor(actor("usr:reader", &[IDENTITY_READ_SCOPE]));
    let IdentityReply::Users(first) = apply_kept(&mut store, &op, &reader, NOW).unwrap() else {
        panic!("expected user list")
    };
    assert_eq!(first[0].username, "alice");
    let next = IdentityOp::User(UserOp::Search {
        request: UserSearch {
            query: "ali".to_string(),
            after: Some(first[0].principal_id.clone()),
            limit: 1,
        },
    });
    let IdentityReply::Users(second) = apply_kept(&mut store, &next, &reader, NOW).unwrap() else {
        panic!("expected user list")
    };
    assert_eq!(second[0].username, "alicia");
    assert_eq!(
        apply_kept(&mut store, &op, &broker(), NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
}

#[test]
fn revoke_one_session_uses_public_handle_and_requires_direct_admin() {
    let mut store = store_in(AuthMode::Local);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    open_session(&mut store, "alice", &alice, "alice-one");
    open_session(&mut store, "alice", &alice, "alice-two");
    let list = IdentityOp::Session(SessionOp::List {
        request: ObjectRef { id: alice.clone() },
    });
    let IdentityReply::Sessions(sessions) = apply_kept(&mut store, &list, &admin(), NOW).unwrap()
    else {
        panic!("expected sessions")
    };
    assert_eq!(sessions.len(), 2);
    let op = IdentityOp::Session(SessionOp::RevokeOne {
        request: ObjectRef {
            id: sessions[0].handle.clone(),
        },
    });
    let mut delegated = admin();
    delegated.actor.delegated = true;
    assert_eq!(
        apply_kept(&mut store, &op, &delegated, NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
    assert_eq!(
        apply_kept(&mut store, &op, &admin(), NOW),
        Ok(IdentityReply::Done { changed: true })
    );
    assert_eq!(
        apply_kept(&mut store, &op, &admin(), NOW),
        Ok(IdentityReply::Done { changed: false })
    );
    let IdentityReply::Sessions(after) = apply_kept(&mut store, &list, &admin(), NOW).unwrap()
    else {
        panic!("expected sessions")
    };
    assert_eq!(after.iter().filter(|session| session.revoked).count(), 1);
}
