//! The boundary verifies a password off the engine's write lock. Each test
//! holds one derivation after it has computed its verdict, replaces the
//! credential through the served boundary, releases the derivation, and
//! requires that the verdict on the replaced password did nothing.

use super::*;
use crate::test_rendezvous::RENDEZVOUS_TIMEOUT;

const REPLACEMENT: &str = "replacement horse battery staple";
const CHOSEN_BY_OLD_HOLDER: &str = "old holder horse battery staple";
const LATER_SESSION: &str = "sess-later-0123456789abcdefghijklmnop";

fn administrator() -> VerifiedRequestContext {
    context(BOOTSTRAP_PRINCIPAL, &[IDENTITY_ADMIN_SCOPE])
}

fn replace_password() -> IdentityOp {
    set_password_value(REPLACEMENT)
}

fn set_password_value(password: &str) -> IdentityOp {
    IdentityOp::Credential(CredentialOp::SetPassword {
        request: PasswordSet {
            principal_id: BOOTSTRAP_PRINCIPAL.to_string(),
            password: Secret::new(password),
            must_change: false,
        },
    })
}

fn change_own_password() -> IdentityOp {
    IdentityOp::Credential(CredentialOp::ChangePassword {
        request: PasswordChange {
            current: Secret::new(PASSWORD),
            new: Secret::new(CHOSEN_BY_OLD_HOLDER),
        },
    })
}

/// Send `op` as `context`, hold it once its stamp is derived, replace the
/// bootstrap administrator's password, then let the held op apply.
async fn raced_past_a_replacement(
    state: &Arc<RwLock<ServerState>>,
    context: VerifiedRequestContext,
    op: IdentityOp,
) -> Response {
    let held = pause::arm(state);
    let racing = tokio::spawn({
        let state = Arc::clone(state);
        async move { send(&state, context, op).await }
    });
    tokio::task::block_in_place(|| held.derived());
    let replaced = send(state, administrator(), replace_password()).await;
    assert!(replaced.error.is_none(), "{:?}", replaced.error);
    held.release();
    tokio::time::timeout(RENDEZVOUS_TIMEOUT, racing)
        .await
        .expect("the held op finishes once released")
        .expect("the held op did not panic")
}

async fn session_is_live(state: &Arc<RwLock<ServerState>>, session: &str) -> bool {
    let hash = secrets::token_hash(session);
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let guard = state.read().await;
    let store = guard.isolation.rbac().identity_store();
    store.session_principal(&hash, now_ms).is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sign_in_verified_against_a_replaced_password_opens_no_session() {
    let state = state();
    let ready = send(&state, broker(), initialize(PASSWORD)).await;
    assert!(ready.error.is_none(), "{:?}", ready.error);
    let raced = raced_past_a_replacement(&state, broker(), sign_in(PASSWORD, SESSION)).await;
    assert!(
        !session_is_live(&state, SESSION).await,
        "the replaced password opened a session: {raced:?}"
    );
    assert_eq!(raced.error.as_deref(), Some("IDENTITY_STALE_CREDENTIAL"));
    let old = send(&state, broker(), sign_in(PASSWORD, SESSION)).await;
    assert_eq!(authenticate_outcome(&old), AuthenticateOutcome::Bad);
    let new = send(&state, broker(), sign_in(REPLACEMENT, LATER_SESSION)).await;
    assert_eq!(authenticate_outcome(&new), AuthenticateOutcome::Ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_password_change_verified_against_a_replaced_password_changes_nothing() {
    let state = state();
    let ready = send(&state, broker(), initialize(PASSWORD)).await;
    assert!(ready.error.is_none(), "{:?}", ready.error);
    let own = context(BOOTSTRAP_PRINCIPAL, &[IDENTITY_SELF_SCOPE]);
    let raced = raced_past_a_replacement(&state, own, change_own_password()).await;
    let taken = send(&state, broker(), sign_in(CHOSEN_BY_OLD_HOLDER, SESSION)).await;
    assert_eq!(
        authenticate_outcome(&taken),
        AuthenticateOutcome::Bad,
        "the replaced password set a new one: {raced:?}"
    );
    assert_eq!(raced.error.as_deref(), Some("IDENTITY_STALE_CREDENTIAL"));
    let kept = send(&state, broker(), sign_in(REPLACEMENT, LATER_SESSION)).await;
    assert_eq!(authenticate_outcome(&kept), AuthenticateOutcome::Ok);
}

fn enroll_totp(secret: &str) -> IdentityOp {
    IdentityOp::Mfa(MfaOp::EnrollTotp {
        request: TotpEnroll {
            session_token: Secret::new(SESSION),
            secret_base32: Secret::new(secret),
        },
    })
}

fn current_totp(secret: &str) -> String {
    let key = secrets::base32_decode(secret).unwrap();
    secrets::totp_code_for_test(
        &key,
        crate::server::dispatch::authoritative_now_ms() / 1_000,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_totp_confirmation_verified_before_secret_replacement_is_refused() {
    // Deliberately synthetic base32 keys, constructed only for this test.
    let original = "A".repeat(32);
    let replacement_secret = "B".repeat(32);
    let state = state();
    for op in [
        initialize(PASSWORD),
        sign_in(PASSWORD, SESSION),
        enroll_totp(&original),
    ] {
        let response = send(&state, broker(), op).await;
        assert!(response.error.is_none(), "{:?}", response.error);
    }
    let held = pause::arm(&state);
    let op = IdentityOp::Mfa(MfaOp::ConfirmTotp {
        request: SessionTouch {
            session_token: Secret::new(SESSION),
            code: Secret::new(current_totp(&original)),
        },
    });
    let racing = tokio::spawn({
        let state = Arc::clone(&state);
        async move { send(&state, broker(), op).await }
    });
    tokio::task::block_in_place(|| held.derived());
    let replaced = send(&state, broker(), enroll_totp(&replacement_secret)).await;
    assert!(replaced.error.is_none(), "{:?}", replaced.error);
    let before =
        serde_json::to_value(state.read().await.isolation.rbac().identity_store()).unwrap();
    held.release();
    let raced = tokio::time::timeout(RENDEZVOUS_TIMEOUT, racing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raced.error.as_deref(), Some("IDENTITY_UNSTAMPED"));
    assert_eq!(
        serde_json::to_value(state.read().await.isolation.rbac().identity_store()).unwrap(),
        before
    );
    let fresh = IdentityOp::Mfa(MfaOp::ConfirmTotp {
        request: SessionTouch {
            session_token: Secret::new(SESSION),
            code: Secret::new(current_totp(&replacement_secret)),
        },
    });
    let accepted = send(&state, broker(), fresh).await;
    assert!(accepted.error.is_none(), "{:?}", accepted.error);
}

#[tokio::test]
async fn stamped_totp_methods_keep_only_the_sealed_binding_and_clear_plaintext() {
    let state = state();
    let secret = "A".repeat(32);
    for op in [
        initialize(PASSWORD),
        sign_in(PASSWORD, SESSION),
        enroll_totp(&secret),
    ] {
        let response = send(&state, broker(), op).await;
        assert!(response.error.is_none(), "{:?}", response.error);
    }
    let sealed = state
        .read()
        .await
        .isolation
        .rbac()
        .identity_store()
        .sealed_totp_of(BOOTSTRAP_PRINCIPAL)
        .unwrap()
        .to_string();
    for confirm in [true, false] {
        let code = current_totp(&secret);
        let request = SessionTouch {
            session_token: Secret::new(SESSION),
            code: Secret::new(&code),
        };
        let op = if confirm {
            MfaOp::ConfirmTotp { request }
        } else {
            MfaOp::VerifyTotp { request }
        };
        let mut method = Method::Identity {
            op: IdentityOp::Mfa(op),
            stamp: None,
        };
        stamp_identity(
            &state,
            &mut method,
            &broker(),
            ElevationStampAuthority::External,
        )
        .await
        .unwrap();
        let Method::Identity {
            op: IdentityOp::Mfa(op),
            stamp: Some(stamp),
        } = &method
        else {
            panic!("expected a stamped MFA method");
        };
        let request = match op {
            MfaOp::ConfirmTotp { request } | MfaOp::VerifyTotp { request } => request,
            _ => panic!("expected the original TOTP operation"),
        };
        assert!(request.session_token.is_empty());
        assert!(request.code.is_empty());
        assert!(stamp.totp_step.is_some());
        assert_eq!(stamp.sealed_secret.as_deref(), Some(sealed.as_str()));
        let replicated = serde_json::to_string(&method).unwrap();
        assert!(!replicated.contains(SESSION));
        assert!(!replicated.contains(&serde_json::to_string(&code).unwrap()));
        assert!(!replicated.contains(&secret));
        assert!(replicated.contains(&sealed));
    }
}

/// Both contenders derive from P0. The held P1 verdict must be refused
/// whether the winner leaves P1 current or moves it into history via P2.
async fn race_password_policy(
    state: &Arc<RwLock<ServerState>>,
    context: VerifiedRequestContext,
    op: IdentityOp,
    replace_twice: bool,
) {
    let held = pause::arm(state);
    let racing = tokio::spawn({
        let state = Arc::clone(state);
        async move { send(&state, context, op).await }
    });
    tokio::task::block_in_place(|| held.derived());
    let first = send(state, administrator(), replace_password()).await;
    assert!(first.error.is_none(), "{:?}", first.error);
    if replace_twice {
        let second = send(
            state,
            administrator(),
            set_password_value(CHOSEN_BY_OLD_HOLDER),
        )
        .await;
        assert!(second.error.is_none(), "{:?}", second.error);
    }
    let before = state.read().await.isolation.rbac().identity_store().clone();
    held.release();
    let raced = tokio::time::timeout(RENDEZVOUS_TIMEOUT, racing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raced.error.as_deref(), Some("IDENTITY_STALE_CREDENTIAL"));
    assert_eq!(
        state.read().await.isolation.rbac().identity_store(),
        &before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn administrative_replacement_cannot_apply_a_stale_password_history_check() {
    for replace_twice in [false, true] {
        let state = state();
        assert!(send(&state, broker(), initialize(PASSWORD))
            .await
            .error
            .is_none());
        race_password_policy(&state, administrator(), replace_password(), replace_twice).await;
        let reused = send(&state, administrator(), replace_password()).await;
        assert_eq!(reused.error.as_deref(), Some("IDENTITY_PASSWORD_REUSED"));
        let fresh = send(
            &state,
            administrator(),
            set_password_value("fresh synthetic replacement password"),
        )
        .await;
        assert!(fresh.error.is_none(), "{:?}", fresh.error);
    }
}

fn reset_password(purpose: TokenPurpose, token: &str, password: &str) -> IdentityOp {
    IdentityOp::Token(TokenOp::RedeemOneTime {
        request: TokenRedeem {
            purpose,
            token: Secret::new(token),
            new_password: Secret::new(password),
            link: None,
        },
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_replacement_keeps_its_token_when_password_history_changes() {
    for purpose in [TokenPurpose::AdminReset, TokenPurpose::CredentialReset] {
        for replace_twice in [false, true] {
            let state = state();
            for op in [initialize(PASSWORD), sign_in(PASSWORD, SESSION)] {
                let response = send(&state, broker(), op).await;
                assert!(response.error.is_none(), "{:?}", response.error);
            }
            let token = "synthetic-reset-token-".repeat(3);
            let issue = IdentityOp::Token(TokenOp::IssueOneTime {
                request: OneTimeTokenIssue {
                    session_token: Secret::new(SESSION),
                    purpose,
                    principal_id: Some(BOOTSTRAP_PRINCIPAL.to_string()),
                    token: Secret::new(&token),
                    ttl_ms: 60_000,
                },
            });
            let issued = send(&state, broker(), issue).await;
            assert!(issued.error.is_none(), "{:?}", issued.error);
            race_password_policy(
                &state,
                broker(),
                reset_password(purpose, &token, REPLACEMENT),
                replace_twice,
            )
            .await;
            let reused = send(
                &state,
                broker(),
                reset_password(purpose, &token, REPLACEMENT),
            )
            .await;
            assert_eq!(reused.error.as_deref(), Some("IDENTITY_PASSWORD_REUSED"));
            let fresh_password = "fresh synthetic reset password";
            let fresh = send(
                &state,
                broker(),
                reset_password(purpose, &token, fresh_password),
            )
            .await;
            assert!(fresh.error.is_none(), "{:?}", fresh.error);
            let spent = send(
                &state,
                broker(),
                reset_password(purpose, &token, "another synthetic reset password"),
            )
            .await;
            assert_eq!(spent.error.as_deref(), Some("IDENTITY_TOKEN_SPENT"));
        }
    }
}
