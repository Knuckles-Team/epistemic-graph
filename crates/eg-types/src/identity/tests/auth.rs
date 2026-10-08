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

pub(super) fn verdict(
    store: &IdentityStore,
    principal: Option<&str>,
    matched: bool,
    session: &str,
) -> IdentityStamp {
    let mut stamp = broker();
    stamp.password_check = Some(check_for(store, principal, matched));
    stamp.token_hashes = vec![session.to_string()];
    stamp
}

/// Apply `op` under the verdict the boundary would stamp against the
/// store's CURRENT credential: `(principal, matched, session)`.
pub(super) fn apply_verdict(
    store: &mut IdentityStore,
    op: &IdentityOp,
    (principal, matched, session): (Option<&str>, bool, &str),
    now_ms: u64,
) -> Result<IdentityReply, IdentityRefusal> {
    let stamp = verdict(store, principal, matched, session);
    apply_kept(store, op, &stamp, now_ms)
}

pub(super) fn outcome(reply: IdentityReply) -> AuthenticateResult {
    match reply {
        IdentityReply::Authenticate(result) => result,
        other => panic!("expected an authenticate reply, got {other:?}"),
    }
}

/// An administrator's `SetPassword` for `principal`, stamped with `hash`.
pub(super) fn set_password_op(
    store: &IdentityStore,
    principal: &str,
    hash: &str,
) -> (IdentityOp, IdentityStamp) {
    let mut stamp = admin();
    stamp.password_hash = Some(hash.to_string());
    stamp.password_check = Some(check_for(store, Some(principal), false));
    let op = IdentityOp::Credential(CredentialOp::SetPassword {
        request: PasswordSet {
            principal_id: principal.to_string(),
            password: Secret::default(),
            must_change: false,
        },
    });
    (op, stamp)
}

/// An administrator replaces `principal`'s password with `hash`.
pub(super) fn replace_password(store: &mut IdentityStore, principal: &str, hash: &str) {
    let (op, stamp) = set_password_op(store, principal, hash);
    apply_kept(store, &op, &stamp, NOW).unwrap();
}

/// Optionally replace `principal`'s password a second time, then prove that
/// `stale` is refused as stale and changes nothing. Returns the unchanged store.
pub(super) fn assert_stale_after_replace(
    store: &mut IdentityStore,
    principal: &str,
    replace_twice: bool,
    (op, stale): (&IdentityOp, &IdentityStamp),
) -> IdentityStore {
    if replace_twice {
        replace_password(store, principal, "synthetic-password-hash-p2");
    }
    let before = store.clone();
    assert_eq!(
        store.apply(op, stale, &ctx_at(NOW)),
        Err(IdentityRefusal::StaleCredential)
    );
    assert_eq!(*store, before, "a stale verdict mutates nothing");
    before
}

/// Re-check `fresh` as a reused password; the op is refused and the store
/// still equals `before`.
pub(super) fn assert_reuse_refused(
    store: &mut IdentityStore,
    principal: &str,
    (op, fresh): (&IdentityOp, &mut IdentityStamp),
    before: &IdentityStore,
) {
    fresh.password_check = Some(check_for(store, Some(principal), true));
    assert_eq!(
        store.apply(op, fresh, &ctx_at(NOW)),
        Err(IdentityRefusal::PasswordReused)
    );
    assert_eq!(store, before);
}

pub(super) fn with_password(store: &mut IdentityStore, username: &str) -> String {
    let principal = create(store, username, UserKind::Human).unwrap();
    replace_password(store, &principal, "$argon2id$user");
    principal
}

#[test]
fn a_matched_password_opens_a_session_and_a_wrong_one_does_not() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    let bad = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), false, "s0"),
        NOW,
    );
    assert_eq!(outcome(bad.unwrap()).outcome, AuthenticateOutcome::Bad);
    assert!(store.session_principal("s0", NOW).is_none());
    let good = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "s1"),
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
    let unknown = apply_verdict(&mut store, &sign_in("nobody"), (None, false, "s"), NOW).unwrap();
    let wrong = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), false, "s"),
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
        apply_verdict(
            &mut store,
            &sign_in("alice"),
            (Some("usr:mallory"), true, "s"),
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
        let stamp = verdict(&store, Some(&alice), false, &format!("s{attempt}"));
        apply_kept(&mut store, &sign_in("alice"), &stamp, NOW).unwrap();
    }
    let correct = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "ok"),
        NOW,
    );
    let correct = outcome(correct.unwrap());
    assert_eq!(correct.outcome, AuthenticateOutcome::Throttled);
    assert!(correct.retry_after_ms.unwrap() > 0);
    assert!(store.session_principal("ok", NOW).is_none());
    let later = NOW + 16 * 60 * 1000;
    let after = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "ok"),
        later,
    );
    assert_eq!(outcome(after.unwrap()).outcome, AuthenticateOutcome::Ok);
}

#[test]
fn an_administrator_can_unlock_a_throttled_account() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    for attempt in 0..6 {
        let stamp = verdict(&store, Some(&alice), false, &format!("s{attempt}"));
        apply_kept(&mut store, &sign_in("alice"), &stamp, NOW).unwrap();
    }
    let unlock = IdentityOp::User(UserOp::Unlock {
        request: ObjectRef { id: alice.clone() },
    });
    let still = apply_verdict(
        &mut store,
        &sign_in_from("alice", None),
        (Some(&alice), true, "t"),
        NOW,
    );
    assert_eq!(
        outcome(still.unwrap()).outcome,
        AuthenticateOutcome::Throttled
    );
    apply_kept(&mut store, &unlock, &admin(), NOW).unwrap();
    let ok = apply_verdict(
        &mut store,
        &sign_in_from("alice", None),
        (Some(&alice), true, "t"),
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
    let user = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "a"),
        NOW,
    );
    assert_eq!(outcome(user.unwrap()).outcome, AuthenticateOutcome::Bad);
    let admin_sign_in = verdict(&store, Some(BOOTSTRAP_PRINCIPAL), true, "b");
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
    reset.password_check = Some(check_for(&store, Some(&alice), false));
    let op = IdentityOp::Credential(CredentialOp::SetPassword {
        request: PasswordSet {
            principal_id: alice.clone(),
            password: Secret::default(),
            must_change: true,
        },
    });
    apply_kept(&mut store, &op, &reset, NOW).unwrap();
    let first = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "p"),
        NOW,
    );
    assert_eq!(
        outcome(first.unwrap()).outcome,
        AuthenticateOutcome::CredentialChangeRequired
    );
    assert!(store.session_principal("p", NOW).is_none());
    let mut with_new = verdict(&store, Some(&alice), true, "q");
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
    confirm.sealed_secret = Some("sealed".to_string());
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
    stamp.sealed_secret = Some("sealed".to_string());
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
    let first = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "mfa-session"),
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
}

/// Create (or keep) the group `ops`, which requires a second factor of its
/// members.
pub(super) fn require_mfa_in_ops(store: &mut IdentityStore) {
    let group = IdentityOp::Access(AccessOp::UpsertGroup {
        request: GroupUpsert {
            group_id: "ops".to_string(),
            name: "ops".to_string(),
            roles: BTreeSet::new(),
            mfa_required: true,
        },
    });
    apply_kept(store, &group, &admin(), NOW).unwrap();
}

/// Make `principal` a member of `ops`.
pub(super) fn join_ops(store: &mut IdentityStore, principal: &str) {
    let join = IdentityOp::Access(AccessOp::ChangeMembership {
        request: GroupMembershipChange {
            group_id: "ops".to_string(),
            principal_id: principal.to_string(),
            change: BindingChange::Add,
        },
    });
    apply_kept(store, &join, &admin(), NOW).unwrap();
}

#[test]
fn a_group_can_require_mfa_for_its_members() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    require_mfa_in_ops(&mut store);
    let before = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "x"),
        NOW,
    );
    assert_eq!(outcome(before.unwrap()).outcome, AuthenticateOutcome::Ok);
    join_ops(&mut store, &alice);
    let after = apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "y"),
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
    apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "live"),
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

#[test]
fn a_totp_verdict_cannot_confirm_a_replacement_secret() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    open_session(&mut store, "alice", &alice, "enrol-session");
    let enroll = IdentityOp::Mfa(MfaOp::EnrollTotp {
        request: TotpEnroll {
            session_token: Secret::default(),
            secret_base32: Secret::default(),
        },
    });
    let mut stamp = broker();
    stamp.token_hashes = vec!["enrol-session".to_string()];
    stamp.sealed_secret = Some("sealed-original".to_string());
    apply_kept(&mut store, &enroll, &stamp, NOW).unwrap();
    let mut old_verdict = stamp.clone();
    old_verdict.totp_step = Some(100);
    stamp.sealed_secret = Some("sealed-replacement".to_string());
    apply_kept(&mut store, &enroll, &stamp, NOW).unwrap();
    let confirm = IdentityOp::Mfa(MfaOp::ConfirmTotp {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    let before = serde_json::to_value(&store).unwrap();
    assert_eq!(
        store.apply(&confirm, &old_verdict, &ctx_at(NOW)),
        Err(IdentityRefusal::Unstamped)
    );
    assert_eq!(serde_json::to_value(&store).unwrap(), before);
    stamp.totp_step = Some(100);
    apply_kept(&mut store, &confirm, &stamp, NOW).unwrap();
    assert!(store.mfa_enrolled(&alice));
}

#[test]
fn totp_verification_requires_the_verified_secret_snapshot() {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    totp_session(&mut store, &alice);
    apply_verdict(
        &mut store,
        &sign_in("alice"),
        (Some(&alice), true, "mfa-session"),
        NOW,
    )
    .unwrap();
    let (op, mut stamp) = verify(Some(101));
    for sealed in [None, Some("other-secret".to_string())] {
        stamp.sealed_secret = sealed;
        let before = serde_json::to_value(&store).unwrap();
        assert_eq!(
            store.apply(&op, &stamp, &ctx_at(NOW)),
            Err(IdentityRefusal::Unstamped)
        );
        assert_eq!(serde_json::to_value(&store).unwrap(), before);
    }
    stamp.sealed_secret = Some("sealed".to_string());
    assert_eq!(
        outcome(apply_kept(&mut store, &op, &stamp, NOW).unwrap()).outcome,
        AuthenticateOutcome::Ok
    );
}
