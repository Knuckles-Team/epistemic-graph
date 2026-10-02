//! Recovery codes stand in for a second factor the principal has and cannot
//! present. They are never a factor of their own: a session that still owes
//! its FIRST enrolment can neither issue itself codes nor complete with one.

use super::auth::{apply_verdict, join_ops, outcome, require_mfa_in_ops, sign_in, with_password};
use super::*;

const CODES: [&str; RECOVERY_CODES_PER_SET] = [
    "code-0", "code-1", "code-2", "code-3", "code-4", "code-5", "code-6", "code-7", "code-8",
    "code-9",
];

fn touch() -> SessionTouch {
    SessionTouch {
        session_token: Secret::default(),
        code: Secret::default(),
    }
}

/// `set_recovery_codes` as the boundary stamps it: the session hash, then
/// one hash per code.
fn issue_codes(session: &str) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = std::iter::once(session)
        .chain(CODES)
        .map(str::to_string)
        .collect();
    let op = IdentityOp::Mfa(MfaOp::SetRecoveryCodes {
        request: RecoveryCodesSet {
            session_token: Secret::default(),
            codes: Vec::new(),
        },
    });
    (op, stamp)
}

fn spend(session: &str, code: &str) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = vec![session.to_string(), code.to_string()];
    let op = IdentityOp::Mfa(MfaOp::ConsumeRecoveryCode { request: touch() });
    (op, stamp)
}

fn run(
    store: &mut IdentityStore,
    (op, stamp): (IdentityOp, IdentityStamp),
) -> Result<IdentityReply, IdentityRefusal> {
    apply_kept(store, &op, &stamp, NOW)
}

/// Whether `session` still owes its second factor.
fn owes_second_factor(store: &mut IdentityStore, session: &str) -> bool {
    let mut stamp = broker();
    stamp.token_hashes = vec![session.to_string()];
    let op = IdentityOp::Session(SessionOp::Resolve { request: touch() });
    match apply_kept(store, &op, &stamp, NOW) {
        Ok(IdentityReply::Resolution(resolution)) => resolution.session_mfa_pending,
        other => panic!("expected a resolution, got {other:?}"),
    }
}

/// `alice`, in a group that requires a second factor.
fn alice_in_a_group_requiring_mfa(store: &mut IdentityStore) -> String {
    let alice = with_password(store, "alice");
    require_mfa_in_ops(store);
    join_ops(store, &alice);
    alice
}

/// Register a WebAuthn credential for the principal of the completed
/// `session` (a confirmed second factor).
fn enrol_factor(store: &mut IdentityStore, session: &str) {
    let mut stamp = broker();
    stamp.token_hashes = vec![session.to_string()];
    let op = IdentityOp::Mfa(MfaOp::RegisterWebauthn {
        request: WebauthnCredential {
            session_token: Secret::default(),
            credential_id: "Y3JlZC1hbGljZS0x".to_string(),
            public_key_cose: "pQECAyYgASFYIA".to_string(),
            sign_count: 0,
            aaguid: None,
            transports: Vec::new(),
            name: "key".to_string(),
        },
    });
    apply_kept(store, &op, &stamp, NOW).unwrap();
}

#[test]
fn recovery_codes_cannot_bootstrap_a_session_past_a_required_second_factor() {
    let mut store = store_in(AuthMode::Local);
    let alice = alice_in_a_group_requiring_mfa(&mut store);
    let first = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "enrol-only"),
        NOW,
    );
    assert_eq!(
        outcome(first.unwrap()).outcome,
        AuthenticateOutcome::MfaEnrollmentRequired
    );
    let issued = run(&mut store, issue_codes("enrol-only"));
    let spent = run(&mut store, spend("enrol-only", CODES[0]));
    assert!(
        owes_second_factor(&mut store, "enrol-only"),
        "the password alone completed a session in a group that requires a second factor \
         (issued: {issued:?}, spent: {spent:?})"
    );
    assert_eq!(issued, Err(IdentityRefusal::NotAuthorized));
    assert_eq!(spent, Err(IdentityRefusal::PreconditionFailed));
}

#[test]
fn a_principal_without_a_confirmed_factor_is_issued_no_recovery_codes() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    open_session(&mut store, "alice", &alice, "complete");
    assert_eq!(
        run(&mut store, issue_codes("complete")),
        Err(IdentityRefusal::PreconditionFailed),
        "there is no factor to recover"
    );
    enrol_factor(&mut store, "complete");
    run(&mut store, issue_codes("complete")).unwrap();
}

#[test]
fn a_lost_factor_is_recovered_once_with_a_previously_issued_code() {
    let mut store = store_in(AuthMode::Local);
    let alice = alice_in_a_group_requiring_mfa(&mut store);
    let first = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "enrol"),
        NOW,
    );
    assert_eq!(
        outcome(first.unwrap()).outcome,
        AuthenticateOutcome::MfaEnrollmentRequired
    );
    enrol_factor(&mut store, "enrol");
    assert_eq!(
        run(&mut store, issue_codes("enrol")),
        Err(IdentityRefusal::NotAuthorized),
        "a session that still owes its factor issues nothing"
    );
    complete_with_the_factor(&mut store, "enrol");
    run(&mut store, issue_codes("enrol")).unwrap();

    open_session_owing_a_factor(&mut store, &alice, "lost-device");
    let wrong = run(&mut store, spend("lost-device", "not-a-code")).unwrap();
    assert_eq!(outcome(wrong).outcome, AuthenticateOutcome::Bad);
    let recovered = run(&mut store, spend("lost-device", CODES[3])).unwrap();
    assert_eq!(outcome(recovered).outcome, AuthenticateOutcome::Ok);
    assert!(!owes_second_factor(&mut store, "lost-device"));

    open_session_owing_a_factor(&mut store, &alice, "again");
    let reused = run(&mut store, spend("again", CODES[3])).unwrap();
    assert_eq!(
        outcome(reused).outcome,
        AuthenticateOutcome::Bad,
        "a code is spent once"
    );
}

/// Sign `alice` in again: she has a confirmed factor, so the session owes it.
fn open_session_owing_a_factor(store: &mut IdentityStore, alice: &str, session: &str) {
    let reply = apply_verdict(store, &sign_in("alice"), (Some(alice), true, session), NOW);
    assert_eq!(
        outcome(reply.unwrap()).outcome,
        AuthenticateOutcome::MfaRequired
    );
}

/// Complete `session` with the registered WebAuthn credential.
fn complete_with_the_factor(store: &mut IdentityStore, session: &str) {
    let mut stamp = broker();
    stamp.token_hashes = vec![session.to_string()];
    let op = IdentityOp::Mfa(MfaOp::VerifyWebauthn {
        request: WebauthnUse {
            session_token: Secret::default(),
            credential_id: "Y3JlZC1hbGljZS0x".to_string(),
            new_sign_count: 0,
        },
    });
    let reply = apply_kept(store, &op, &stamp, NOW).unwrap();
    assert_eq!(outcome(reply).outcome, AuthenticateOutcome::Ok);
}

#[test]
fn codes_that_outlive_their_factor_complete_nothing() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    open_session(&mut store, "alice", &alice, "complete");
    enrol_factor(&mut store, "complete");
    run(&mut store, issue_codes("complete")).unwrap();
    let remove = IdentityOp::Mfa(MfaOp::RemoveWebauthn {
        request: ObjectRef {
            id: "Y3JlZC1hbGljZS0x".to_string(),
        },
    });
    apply_kept(&mut store, &remove, &admin(), NOW).unwrap();
    require_mfa_in_ops(&mut store);
    join_ops(&mut store, &alice);
    let reply = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "later"),
        NOW,
    );
    assert_eq!(
        outcome(reply.unwrap()).outcome,
        AuthenticateOutcome::MfaEnrollmentRequired
    );
    let spent = run(&mut store, spend("later", CODES[0]));
    assert!(
        owes_second_factor(&mut store, "later"),
        "a leftover code completed a session with no factor: {spent:?}"
    );
    assert_eq!(spent, Err(IdentityRefusal::PreconditionFailed));
}
