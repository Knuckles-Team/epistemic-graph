//! WebAuthn second factors (IDM-09) and the signed-out password reset
//! (IDM-19). Every refusal is tested both ways.

use super::auth::{outcome, sign_in, verdict, with_password};
use super::*;

const SESSION: &str = "alice-session";
const CREDENTIAL: &str = "Y3JlZC1hbGljZS0x";

fn with_session(stamp_session: &str) -> IdentityStamp {
    let mut stamp = broker();
    stamp.token_hashes = vec![stamp_session.to_string()];
    stamp
}

fn register(credential_id: &str, sign_count: u32) -> IdentityOp {
    IdentityOp::Mfa(MfaOp::RegisterWebauthn {
        request: WebauthnCredential {
            session_token: Secret::default(),
            credential_id: credential_id.to_string(),
            public_key_cose: "pQECAyYgASFYIA".to_string(),
            sign_count,
            aaguid: None,
            transports: vec!["usb".to_string(), "internal".to_string()],
            name: "YubiKey".to_string(),
        },
    })
}

fn verify(credential_id: &str, new_sign_count: u32) -> IdentityOp {
    IdentityOp::Mfa(MfaOp::VerifyWebauthn {
        request: WebauthnUse {
            session_token: Secret::default(),
            credential_id: credential_id.to_string(),
            new_sign_count,
        },
    })
}

fn credentials() -> IdentityOp {
    IdentityOp::Mfa(MfaOp::WebauthnCredentials {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    })
}

/// Alice with a registered credential (counter 4), signed in again: her
/// new session owes its second factor.
fn alice_pending_webauthn(store: &mut IdentityStore) -> String {
    let alice = with_password(store, "alice");
    open_session(store, "alice", &alice, SESSION);
    apply_kept(store, &register(CREDENTIAL, 4), &with_session(SESSION), NOW).unwrap();
    let again = apply_kept(
        store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "mfa"),
        NOW,
    );
    assert_eq!(
        outcome(again.unwrap()).outcome,
        AuthenticateOutcome::MfaRequired,
        "a WebAuthn credential is an enrolled second factor"
    );
    alice
}

#[test]
fn a_credential_needs_a_live_session_and_a_well_formed_body() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    assert_eq!(
        apply_kept(
            &mut store,
            &register(CREDENTIAL, 0),
            &with_session("no-session"),
            NOW
        ),
        Err(IdentityRefusal::NotAuthorized)
    );
    open_session(&mut store, "alice", &alice, SESSION);
    assert_eq!(
        apply_kept(
            &mut store,
            &register("not base64url!", 0),
            &with_session(SESSION),
            NOW
        ),
        Err(IdentityRefusal::InvalidRequest)
    );
    let self_scope = IdentityStamp::for_actor(actor(&alice, &[IDENTITY_SELF_SCOPE]));
    assert_eq!(
        apply_kept(&mut store, &register(CREDENTIAL, 0), &self_scope, NOW),
        Err(IdentityRefusal::NotAuthorized),
        "registration is a broker op"
    );
    apply_kept(
        &mut store,
        &register(CREDENTIAL, 0),
        &with_session(SESSION),
        NOW,
    )
    .unwrap();
    assert_eq!(
        apply_kept(
            &mut store,
            &register(CREDENTIAL, 0),
            &with_session(SESSION),
            NOW
        ),
        Err(IdentityRefusal::Collision)
    );
}

#[test]
fn a_pending_session_completes_only_with_a_forward_counter_on_its_own_credential() {
    let mut store = store_in(AuthMode::Local);
    alice_pending_webauthn(&mut store);
    let listed = apply_kept(&mut store, &credentials(), &with_session("mfa"), NOW).unwrap();
    assert!(
        matches!(listed, IdentityReply::WebauthnCredentials(ref found) if found.len() == 1 && found[0].sign_count == 4),
        "the pending session reads the keys its assertion is checked against: {listed:?}"
    );
    let unknown = apply_kept(
        &mut store,
        &verify("dW5rbm93bg", 9),
        &with_session("mfa"),
        NOW,
    );
    assert_eq!(outcome(unknown.unwrap()).outcome, AuthenticateOutcome::Bad);
    assert_eq!(
        apply_kept(
            &mut store,
            &verify(CREDENTIAL, 4),
            &with_session("mfa"),
            NOW
        ),
        Err(IdentityRefusal::Replay),
        "a counter that does not move forward is a cloned authenticator"
    );
    let ok = apply_kept(
        &mut store,
        &verify(CREDENTIAL, 5),
        &with_session("mfa"),
        NOW,
    );
    assert_eq!(outcome(ok.unwrap()).outcome, AuthenticateOutcome::Ok);
    assert_eq!(
        apply_kept(
            &mut store,
            &verify(CREDENTIAL, 6),
            &with_session("mfa"),
            NOW
        ),
        Err(IdentityRefusal::NotFound),
        "a completed session owes nothing"
    );
}

#[test]
fn a_credential_is_removed_by_its_owner_or_an_administrator_only() {
    let mut store = store_in(AuthMode::Local);
    let alice = alice_pending_webauthn(&mut store);
    let bob = create(&mut store, "bob", UserKind::Human).unwrap();
    let remove = IdentityOp::Mfa(MfaOp::RemoveWebauthn {
        request: ObjectRef {
            id: CREDENTIAL.to_string(),
        },
    });
    let as_bob = IdentityStamp::for_actor(actor(&bob, &[IDENTITY_SELF_SCOPE]));
    assert_eq!(
        apply_kept(&mut store, &remove, &as_bob, NOW),
        Err(IdentityRefusal::NotFound),
        "another principal's credential is never confirmed"
    );
    let mut delegated_admin = admin();
    delegated_admin.actor.delegated = true;
    assert_eq!(
        apply_kept(&mut store, &remove, &delegated_admin, NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
    let as_alice = IdentityStamp::for_actor(actor(&alice, &[IDENTITY_SELF_SCOPE]));
    let mut kept = store.clone();
    apply_kept(&mut kept, &remove, &as_alice, NOW).unwrap();
    assert!(!kept.has_webauthn(&alice));
    apply_kept(&mut store, &remove, &admin(), NOW).unwrap();
    assert!(!store.has_webauthn(&alice));
}

fn reset(username: &str, token_hash: &str) -> (IdentityOp, IdentityStamp) {
    let op = IdentityOp::Token(TokenOp::IssuePasswordReset {
        request: PasswordResetIssue {
            username: username.to_string(),
            token: Secret::default(),
            ttl_ms: 3_600_000,
        },
    });
    (op, with_session(token_hash))
}

fn delivered(store: &mut IdentityStore, username: &str, token_hash: &str) -> Option<String> {
    let (op, stamp) = reset(username, token_hash);
    match apply_kept(store, &op, &stamp, NOW).unwrap() {
        IdentityReply::ResetDelivery(delivery) => delivery.email,
        other => panic!("expected a reset delivery, got {other:?}"),
    }
}

fn with_email(store: &mut IdentityStore, principal: &str) {
    let op = IdentityOp::User(UserOp::Update {
        request: UserUpdate {
            principal_id: principal.to_string(),
            username: None,
            display_name: None,
            email: Some(format!("{principal}@example.org").replace(':', "-")),
        },
    });
    apply_kept(store, &op, &admin(), NOW).unwrap();
}

#[test]
fn a_reset_link_answers_uniformly_and_redeems_once() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let _mailless = with_password(&mut store, "carol");
    assert_eq!(
        delivered(&mut store, "alice", "tok-no-mail"),
        None,
        "no e-mail"
    );
    with_email(&mut store, &alice);
    assert_eq!(
        delivered(&mut store, "nobody", "tok-unknown"),
        None,
        "unknown"
    );
    assert_eq!(delivered(&mut store, "carol", "tok-carol"), None);
    let email = delivered(&mut store, "Alice", "tok-alice");
    assert_eq!(email.as_deref(), Some("usr-alice@example.org"));
    let mut redeem = with_session("tok-alice");
    redeem.password_hash = Some("$argon2id$reset".to_string());
    let op = IdentityOp::Token(TokenOp::RedeemOneTime {
        request: TokenRedeem {
            purpose: TokenPurpose::PasswordReset,
            token: Secret::default(),
            new_password: Secret::default(),
            link: None,
        },
    });
    assert!(apply_kept(&mut store, &op, &redeem, NOW).is_ok());
    assert_eq!(
        apply_kept(&mut store, &op, &redeem, NOW),
        Err(IdentityRefusal::TokenSpent)
    );
    let disable = IdentityOp::User(UserOp::SetStatus {
        request: UserStatusChange {
            principal_id: alice,
            status: UserStatus::Disabled,
        },
    });
    apply_kept(&mut store, &disable, &admin(), NOW).unwrap();
    assert_eq!(
        delivered(&mut store, "alice", "tok-disabled"),
        None,
        "disabled"
    );
}

#[test]
fn reset_requests_are_throttled_and_broker_only() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    with_email(&mut store, &alice);
    for attempt in 0..5 {
        let hash = format!("tok-{attempt}");
        assert!(
            delivered(&mut store, "alice", &hash).is_some(),
            "attempt {attempt}"
        );
    }
    assert_eq!(
        delivered(&mut store, "alice", "tok-throttled"),
        None,
        "a throttled account answers like an unknown one"
    );
    let (op, _) = reset("alice", "tok-x");
    let reader = IdentityStamp::for_actor(actor("usr:x", &[IDENTITY_ADMIN_SCOPE]));
    assert_eq!(
        apply_kept(&mut store, &op, &reader, NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
    let (mut bad_ttl, stamp) = reset("alice", "tok-y");
    if let IdentityOp::Token(TokenOp::IssuePasswordReset { request }) = &mut bad_ttl {
        request.ttl_ms = 0;
    }
    assert_eq!(
        apply_kept(&mut store, &bad_ttl, &stamp, NOW + 3_600_000),
        Err(IdentityRefusal::InvalidRequest)
    );
}
