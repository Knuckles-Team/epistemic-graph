//! Durable revocation at the served boundary: administrators A (`ann`) and B
//! (the bootstrap administrator). A's token works; B disables, demotes or
//! deprovisions A; A's PREVIOUS token -- the same claims under a fresh nonce
//! -- is then refused for identity mutations and reads, because the store no
//! longer backs the scope it carries. First-run setup needs no store record.

use super::*;

const ANN: &str = "usr:ann";

fn b_token() -> VerifiedRequestContext {
    context(BOOTSTRAP_PRINCIPAL, &[IDENTITY_ADMIN_SCOPE])
}

/// A's token: the claims it was issued with while an administrator. Every
/// call is the same token presented under a fresh nonce.
fn a_token() -> VerifiedRequestContext {
    context(ANN, &[IDENTITY_ADMIN_SCOPE, IDENTITY_READ_SCOPE])
}

fn create_ann() -> IdentityOp {
    eg_types::test_support::identity::create_administrator_op("ann", ANN)
}

fn set_status(status: UserStatus) -> IdentityOp {
    IdentityOp::User(UserOp::SetStatus {
        request: UserStatusChange {
            principal_id: ANN.to_string(),
            status,
        },
    })
}

fn administrators(change: BindingChange) -> IdentityOp {
    IdentityOp::Access(AccessOp::ChangeMembership {
        request: GroupMembershipChange {
            group_id: ADMINISTRATORS_GROUP.to_string(),
            principal_id: ANN.to_string(),
            change,
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

/// An engine with administrators A and B, A's token proven to work.
async fn two_administrators() -> Arc<RwLock<ServerState>> {
    let state = state();
    for (context, op) in [
        (broker(), initialize(PASSWORD)),
        (b_token(), create_ann()),
        (a_token(), set_status(UserStatus::Active)),
        (a_token(), list_users()),
    ] {
        let response = send(&state, context, op).await;
        assert!(response.error.is_none(), "{:?}", response.error);
    }
    state
}

async fn as_b(state: &Arc<RwLock<ServerState>>, op: IdentityOp) {
    let response = send(state, b_token(), op).await;
    assert!(response.error.is_none(), "{:?}", response.error);
}

/// A's durable standing: `(status, whether her roles resolve to administrator)`.
async fn ann(state: &Arc<RwLock<ServerState>>) -> (UserStatus, bool) {
    let guard = state.read().await;
    let store = guard.isolation.rbac().identity_store();
    let resolution = store
        .resolve(ANN, &eg_capabilities::scopes::ScopeRegistry)
        .expect("ann exists");
    let administers = resolution.scopes.contains(IDENTITY_ADMIN_SCOPE);
    (resolution.status, administers)
}

fn not_authorized(response: &Response) -> bool {
    response.error.as_deref() == Some("IDENTITY_NOT_AUTHORIZED")
}

#[tokio::test]
async fn a_disabled_administrators_previous_token_is_refused() {
    let state = two_administrators().await;
    as_b(&state, set_status(UserStatus::Disabled)).await;
    let replayed = send(&state, a_token(), set_status(UserStatus::Active)).await;
    assert_eq!(
        ann(&state).await.0,
        UserStatus::Disabled,
        "a disabled administrator re-enabled itself with its previous token: {replayed:?}"
    );
    assert!(not_authorized(&replayed), "{replayed:?}");
    let read = send(&state, a_token(), list_users()).await;
    assert!(not_authorized(&read), "{read:?}");
    as_b(&state, set_status(UserStatus::Active)).await;
    let restored = send(&state, a_token(), list_users()).await;
    assert!(restored.error.is_none(), "{:?}", restored.error);
}

#[tokio::test]
async fn a_removed_administrators_previous_token_is_refused() {
    let state = two_administrators().await;
    as_b(&state, administrators(BindingChange::Remove)).await;
    let replayed = send(&state, a_token(), administrators(BindingChange::Add)).await;
    assert!(
        !ann(&state).await.1,
        "a removed administrator restored itself with its previous token: {replayed:?}"
    );
    assert!(not_authorized(&replayed), "{replayed:?}");
    let read = send(&state, a_token(), list_users()).await;
    assert!(not_authorized(&read), "{read:?}");

    as_b(&state, administrators(BindingChange::Add)).await;
    as_b(&state, set_status(UserStatus::Deprovisioned)).await;
    let replayed = send(&state, a_token(), set_status(UserStatus::Active)).await;
    assert_eq!(
        ann(&state).await.0,
        UserStatus::Deprovisioned,
        "a deprovisioned administrator restored itself with its previous token: {replayed:?}"
    );
    assert!(not_authorized(&replayed), "{replayed:?}");
}

#[tokio::test]
async fn first_administrator_setup_needs_no_store_record() {
    for first in [broker(), context("usr:operator", &[IDENTITY_ADMIN_SCOPE])] {
        let state = state();
        let response = send(&state, first, initialize(PASSWORD)).await;
        assert!(response.error.is_none(), "{:?}", response.error);
        let signed_in = send(&state, broker(), sign_in(PASSWORD, SESSION)).await;
        assert_eq!(authenticate_outcome(&signed_in), AuthenticateOutcome::Ok);
    }
}
