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
fn list_service_accounts_excludes_humans_before_paging() {
    let mut store = store_in(AuthMode::Local);
    create(&mut store, "human", UserKind::Human).unwrap();
    create(&mut store, "svc-a", UserKind::Service).unwrap();
    create(&mut store, "svc-b", UserKind::Service).unwrap();
    let op = IdentityOp::User(UserOp::ListServiceAccounts {
        request: ListQuery {
            after: None,
            limit: 1,
        },
    });
    let IdentityReply::Users(first) = apply_kept(&mut store, &op, &admin(), NOW).unwrap() else {
        panic!("expected service accounts")
    };
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].kind, UserKind::Service);
    let next = IdentityOp::User(UserOp::ListServiceAccounts {
        request: ListQuery {
            after: Some(first[0].principal_id.clone()),
            limit: 1,
        },
    });
    let IdentityReply::Users(second) = apply_kept(&mut store, &next, &admin(), NOW).unwrap() else {
        panic!("expected service accounts")
    };
    assert_eq!(second.len(), 1);
    assert_ne!(first[0].principal_id, second[0].principal_id);
}

#[test]
fn self_reads_use_only_the_stamped_principal() {
    let mut store = store_in(AuthMode::Local);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let bob = create(&mut store, "bob", UserKind::Human).unwrap();
    open_session(&mut store, "alice", &alice, "alice-session");
    open_session(&mut store, "bob", &bob, "bob-session");
    let self_stamp = IdentityStamp::for_actor(actor(&alice, &[IDENTITY_SELF_SCOPE]));
    let resolution = IdentityOp::User(UserOp::ResolveSelf);
    let IdentityReply::Resolution(who) =
        apply_kept(&mut store, &resolution, &self_stamp, NOW).unwrap()
    else {
        panic!("expected self resolution")
    };
    assert_eq!(who.principal_id, alice);
    let sessions = IdentityOp::Session(SessionOp::ListOwn);
    let IdentityReply::Sessions(rows) =
        apply_kept(&mut store, &sessions, &self_stamp, NOW).unwrap()
    else {
        panic!("expected own sessions")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].principal_id, alice);
    let status = IdentityOp::Mfa(MfaOp::Status);
    assert_eq!(
        apply_kept(&mut store, &status, &self_stamp, NOW),
        Ok(IdentityReply::MfaStatus(MfaStatusView {
            totp_enrolled: false,
            webauthn_credentials: 0,
            recovery_codes_left: 0,
        }))
    );
    assert_eq!(
        apply_kept(&mut store, &sessions, &broker(), NOW),
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

#[test]
fn audit_export_is_bounded_and_verify_detects_a_broken_chain() {
    let mut store = store_in(AuthMode::Local);
    create(&mut store, "alice", UserKind::Human).unwrap();
    let reader = IdentityStamp::for_actor(actor("usr:reader", &[IDENTITY_READ_SCOPE]));
    let export = IdentityOp::Config(ConfigOp::ExportAudit {
        request: ListQuery {
            after: None,
            limit: 1,
        },
    });
    let IdentityReply::Audit(rows) = apply_kept(&mut store, &export, &reader, NOW).unwrap() else {
        panic!("expected audit rows")
    };
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].chain.is_empty());
    let verify = IdentityOp::Config(ConfigOp::VerifyAudit);
    assert_eq!(
        apply_kept(&mut store, &verify, &reader, NOW),
        Ok(IdentityReply::AuditVerification {
            valid: true,
            first_broken_seq: None
        })
    );
    let mut raw = serde_json::to_value(&store).unwrap();
    raw["audit"]["entries"][0]["chain"] = serde_json::json!("tampered");
    let mut corrupt: IdentityStore = serde_json::from_value(raw).unwrap();
    assert_eq!(
        apply_kept(&mut corrupt, &verify, &reader, NOW),
        Ok(IdentityReply::AuditVerification {
            valid: false,
            first_broken_seq: Some(rows[0].seq)
        })
    );
}
