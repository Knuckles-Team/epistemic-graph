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
    IdentityOp::Credential(CredentialOp::SetPassword {
        request: PasswordSet {
            principal_id: BOOTSTRAP_PRINCIPAL.to_string(),
            password: Secret::new(REPLACEMENT),
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
