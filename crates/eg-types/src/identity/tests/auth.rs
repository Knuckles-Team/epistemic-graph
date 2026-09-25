//! Sign-in, the throttle, sessions and second factors.

use super::*;

pub(super) fn sign_in(username: &str) -> IdentityOp {
    sign_in_from(username, Some("10.0.0.0/24"))
}

fn sign_in_from(username: &str, ip_prefix: Option<&str>) -> IdentityOp {
    IdentityOp::Credential(CredentialOp::Authenticate {
        request: AuthenticateRequest {
            username: username.to_string(),
            password: Secret::default(),
            session_token: Secret::default(),
            ip_prefix: ip_prefix.map(str::to_string),
            new_password: Secret::default(),
        },
    })
}

pub(super) fn verdict(principal: Option<&str>, matched: bool, session: &str) -> IdentityStamp {
    let mut stamp = broker();
    stamp.password_check = Some(PasswordCheck {
        principal_id: principal.map(str::to_string),
        matched,
        rehash: None,
    });
    stamp.token_hashes = vec![session.to_string()];
    stamp
}

pub(super) fn outcome(reply: IdentityReply) -> AuthenticateResult {
    match reply {
        IdentityReply::Authenticate(result) => result,
        other => panic!("expected an authenticate reply, got {other:?}"),
    }
}

pub(super) fn with_password(store: &mut IdentityStore, username: &str) -> String {
    let principal = create(store, username, UserKind::Human).unwrap();
    let mut stamp = admin();
    stamp.password_hash = Some("$argon2id$user".to_string());
    let op = IdentityOp::Credential(CredentialOp::SetPassword {
        request: PasswordSet {
            principal_id: principal.clone(),
            password: Secret::default(),
            must_change: false,
        },
    });
    apply_kept(store, &op, &stamp, NOW).unwrap();
    principal
}

#[test]
fn a_matched_password_opens_a_session_and_a_wrong_one_does_not() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let bad = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), false, "s0"),
        NOW,
    );
    assert_eq!(outcome(bad.unwrap()).outcome, AuthenticateOutcome::Bad);
    assert!(store.session_principal("s0", NOW).is_none());
    let good = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "s1"),
        NOW,
    );
    let good = outcome(good.unwrap());
    assert_eq!(good.outcome, AuthenticateOutcome::Ok);
    assert_eq!(good.principal_id.as_deref(), Some(alice.as_str()));
    assert_eq!(store.session_principal("s1", NOW), Some(alice.as_str()));
}

#[test]
fn an_unknown_user_answers_exactly_like_a_wrong_password() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let unknown = apply_kept(
        &mut store,
        &sign_in("nobody"),
        &verdict(None, false, "s"),
        NOW,
    )
    .unwrap();
    let wrong = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), false, "s"),
        NOW,
    )
    .unwrap();
    assert_eq!(unknown, wrong);
}

#[test]
fn a_verdict_stamped_for_another_principal_is_refused() {
    let mut store = store_in(AuthMode::Local);
    with_password(&mut store, "alice");
    assert_eq!(
        apply_kept(
            &mut store,
            &sign_in("alice"),
            &verdict(Some("usr:mallory"), true, "s"),
            NOW
        ),
        Err(IdentityRefusal::Unstamped)
    );
}

#[test]
fn the_lockout_is_atomic_a_correct_guess_after_it_engaged_is_throttled() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    for attempt in 0..5 {
        let stamp = verdict(Some(&alice), false, &format!("s{attempt}"));
        apply_kept(&mut store, &sign_in("alice"), &stamp, NOW).unwrap();
    }
    let correct = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "ok"),
        NOW,
    );
    let correct = outcome(correct.unwrap());
    assert_eq!(correct.outcome, AuthenticateOutcome::Throttled);
    assert!(correct.retry_after_ms.unwrap() > 0);
    assert!(store.session_principal("ok", NOW).is_none());
    let later = NOW + 16 * 60 * 1000;
    let after = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "ok"),
        later,
    );
    assert_eq!(outcome(after.unwrap()).outcome, AuthenticateOutcome::Ok);
}

#[test]
fn an_administrator_can_unlock_a_throttled_account() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    for attempt in 0..6 {
        let stamp = verdict(Some(&alice), false, &format!("s{attempt}"));
        apply_kept(&mut store, &sign_in("alice"), &stamp, NOW).unwrap();
    }
    let unlock = IdentityOp::User(UserOp::Unlock {
        request: ObjectRef { id: alice.clone() },
    });
    let still = apply_kept(
        &mut store,
        &sign_in_from("alice", None),
        &verdict(Some(&alice), true, "t"),
        NOW,
    );
    assert_eq!(
        outcome(still.unwrap()).outcome,
        AuthenticateOutcome::Throttled
    );
    apply_kept(&mut store, &unlock, &admin(), NOW).unwrap();
    let ok = apply_kept(
        &mut store,
        &sign_in_from("alice", None),
        &verdict(Some(&alice), true, "t"),
        NOW,
    );
    let ok = outcome(ok.unwrap());
    assert_eq!(ok.outcome, AuthenticateOutcome::Ok, "{ok:?}");
}

#[test]
fn external_mode_signs_in_only_the_break_glass_administrator_locally() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let mut json = serde_json::to_value(&store).unwrap();
    json["config"]["mode"] = serde_json::json!("external");
    store = serde_json::from_value(json).unwrap();
    let user = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "a"),
        NOW,
    );
    assert_eq!(outcome(user.unwrap()).outcome, AuthenticateOutcome::Bad);
    let admin_sign_in = verdict(Some(BOOTSTRAP_PRINCIPAL), true, "b");
    let admin_reply = apply_kept(&mut store, &sign_in("root-admin"), &admin_sign_in, NOW);
    assert_eq!(
        outcome(admin_reply.unwrap()).outcome,
        AuthenticateOutcome::Ok
    );
}

#[test]
fn a_forced_change_opens_no_session_until_a_new_password_is_supplied() {
    let mut store = store_in(AuthMode::Local);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let mut reset = admin();
    reset.password_hash = Some("$argon2id$temp".to_string());
    let op = IdentityOp::Credential(CredentialOp::SetPassword {
        request: PasswordSet {
            principal_id: alice.clone(),
            password: Secret::default(),
            must_change: true,
        },
    });
    apply_kept(&mut store, &op, &reset, NOW).unwrap();
    let first = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "p"),
        NOW,
    );
    assert_eq!(
        outcome(first.unwrap()).outcome,
        AuthenticateOutcome::PasswordChangeRequired
    );
    assert!(store.session_principal("p", NOW).is_none());
    let mut with_new = verdict(Some(&alice), true, "q");
    with_new.password_hash = Some("$argon2id$chosen".to_string());
    let second = apply_kept(&mut store, &sign_in("alice"), &with_new, NOW);
    assert_eq!(outcome(second.unwrap()).outcome, AuthenticateOutcome::Ok);
    assert!(!store.credential_of(&alice).unwrap().must_change);
}

fn totp_session(store: &mut IdentityStore, alice: &str) {
    open_session(store, "alice", alice, "enrol-session");
    let mut enroll = broker();
    enroll.token_hashes = vec!["enrol-session".to_string()];
    enroll.sealed_secret = Some("sealed".to_string());
    let op = IdentityOp::Mfa(MfaOp::EnrollTotp {
        request: TotpEnroll {
            session_token: Secret::default(),
            secret_base32: Secret::default(),
        },
    });
    apply_kept(store, &op, &enroll, NOW).unwrap();
    let mut confirm = broker();
    confirm.token_hashes = vec!["enrol-session".to_string()];
    confirm.totp_step = Some(100);
    let op = IdentityOp::Mfa(MfaOp::ConfirmTotp {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    apply_kept(store, &op, &confirm, NOW).unwrap();
}

fn verify(step: Option<u64>) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = vec!["mfa-session".to_string()];
    stamp.totp_step = step;
    let op = IdentityOp::Mfa(MfaOp::VerifyTotp {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    (op, stamp)
}

#[test]
fn an_enrolled_factor_holds_the_session_until_a_fresh_step_verifies() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    totp_session(&mut store, &alice);
    let first = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "mfa-session"),
        NOW,
    );
    assert_eq!(
        outcome(first.unwrap()).outcome,
        AuthenticateOutcome::MfaRequired
    );
    let (op, replayed) = verify(Some(100));
    assert_eq!(
        apply_kept(&mut store, &op, &replayed, NOW),
        Err(IdentityRefusal::Replay),
        "the enrolment step cannot be replayed"
    );
    let (op, wrong) = verify(None);
    assert_eq!(
        outcome(apply_kept(&mut store, &op, &wrong, NOW).unwrap()).outcome,
        AuthenticateOutcome::Bad
    );
    let (op, fresh) = verify(Some(101));
    assert_eq!(
        outcome(apply_kept(&mut store, &op, &fresh, NOW).unwrap()).outcome,
        AuthenticateOutcome::Ok
    );
    let mut broker_stamp = broker();
    broker_stamp.token_hashes = vec!["mfa-session".to_string()];
    let resolve = IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    let IdentityReply::Resolution(session) =
        apply_kept(&mut store, &resolve, &broker_stamp, NOW).unwrap()
    else {
        panic!("expected session resolution")
    };
    assert_eq!(session.session_mfa_at_ms, Some(NOW));
    assert!(!session.session_mfa_pending);
    let principal = IdentityOp::User(UserOp::Resolve {
        request: ObjectRef { id: alice.clone() },
    });
    let IdentityReply::Resolution(plain) =
        apply_kept(&mut store, &principal, &broker(), NOW).unwrap()
    else {
        panic!("expected principal resolution")
    };
    assert_eq!(plain.session_mfa_at_ms, None);
}

#[test]
fn a_group_can_require_mfa_for_its_members() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let group = IdentityOp::Access(AccessOp::UpsertGroup {
        request: GroupUpsert {
            group_id: "ops".to_string(),
            name: "ops".to_string(),
            roles: BTreeSet::new(),
            mfa_required: true,
        },
    });
    apply_kept(&mut store, &group, &admin(), NOW).unwrap();
    let before = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "x"),
        NOW,
    );
    assert_eq!(outcome(before.unwrap()).outcome, AuthenticateOutcome::Ok);
    let join = IdentityOp::Access(AccessOp::ChangeMembership {
        request: GroupMembershipChange {
            group_id: "ops".to_string(),
            principal_id: alice.clone(),
            change: BindingChange::Add,
        },
    });
    apply_kept(&mut store, &join, &admin(), NOW).unwrap();
    let after = apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "y"),
        NOW,
    );
    assert_eq!(
        outcome(after.unwrap()).outcome,
        AuthenticateOutcome::MfaEnrollmentRequired
    );
}

#[test]
fn a_revoked_session_resolves_to_nothing() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    apply_kept(
        &mut store,
        &sign_in("alice"),
        &verdict(Some(&alice), true, "live"),
        NOW,
    )
    .unwrap();
    let mut touch = broker();
    touch.token_hashes = vec!["live".to_string()];
    let resolve = IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    assert!(apply_kept(&mut store, &resolve, &touch, NOW).is_ok());
    let revoke = IdentityOp::Session(SessionOp::Revoke {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    apply_kept(&mut store, &revoke, &touch, NOW).unwrap();
    assert_eq!(
        apply_kept(&mut store, &resolve, &touch, NOW),
        Err(IdentityRefusal::NotFound)
    );
}
