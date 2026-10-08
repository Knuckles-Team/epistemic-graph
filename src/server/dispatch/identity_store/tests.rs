//! The identity store through the real request boundary: the actor is stamped from the
//! verified context, exact identity scopes are required, every secret is
//! hashed and cleared before apply, and the floors refuse weak secrets.

use super::*;
use eg_types::identity::*;

const PASSWORD: &str = "correct horse battery staple";
const SESSION: &str = "sess-0123456789abcdefghijklmnopqrstuv";

use super::super::test_support::{
    bootstrapped_state as state, send as send_method, state as unbootstrapped_state, verified,
};

mod refusal_contract;
mod revocation;
mod stale_verdict;

fn context(principal: &str, scopes: &[&str]) -> VerifiedRequestContext {
    verified(principal, scopes, &[])
}

fn broker() -> VerifiedRequestContext {
    context("svc:graph-os", &[IDENTITY_AUTHENTICATE_SCOPE])
}

async fn send_stamped(
    state: &Arc<RwLock<ServerState>>,
    context: VerifiedRequestContext,
    op: IdentityOp,
    forged: Option<IdentityStamp>,
) -> Response {
    send_method(state, context, Method::Identity { op, stamp: forged }).await
}

async fn send(
    state: &Arc<RwLock<ServerState>>,
    context: VerifiedRequestContext,
    op: IdentityOp,
) -> Response {
    send_stamped(state, context, op, None).await
}

fn initialize(password: &str) -> IdentityOp {
    IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode: AuthMode::Local,
            admin_username: Some("root".to_string()),
            admin_password: Secret::new(password),
        },
    })
}

fn sign_in(password: &str, session: &str) -> IdentityOp {
    IdentityOp::Credential(CredentialOp::Authenticate {
        request: AuthenticateRequest {
            username: "root".to_string(),
            password: Secret::new(password),
            session_token: Secret::new(session),
            ip_prefix: None,
            new_password: Secret::default(),
        },
    })
}

fn authenticate_outcome(response: &Response) -> AuthenticateOutcome {
    let reply: IdentityReply = match response.result.as_ref() {
        Some(ResultPayload::Json(value)) => serde_json::from_value(value.clone()).expect("reply"),
        other => panic!("expected a JSON identity reply, got {other:?}"),
    };
    match reply {
        IdentityReply::Authenticate(result) => result.outcome,
        other => panic!("expected an authenticate reply, got {other:?}"),
    }
}

#[tokio::test]
async fn a_password_is_hashed_at_the_boundary_and_signs_in() {
    let state = state();
    let response = send(&state, broker(), initialize(PASSWORD)).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let stored = state
        .read()
        .await
        .isolation
        .rbac()
        .identity_store()
        .credential_of(BOOTSTRAP_PRINCIPAL)
        .map(|credential| credential.hash.clone())
        .expect("a credential");
    assert!(stored.starts_with("$argon2id$"));
    assert!(!stored.contains(PASSWORD));
    let image = serde_json::to_string(state.read().await.isolation.rbac()).unwrap();
    assert!(
        !image.contains(PASSWORD),
        "no plaintext in the durable image"
    );
    assert!(!image.contains(SESSION));
    let bad = send(
        &state,
        broker(),
        sign_in("wrong horse battery staple", SESSION),
    )
    .await;
    assert_eq!(authenticate_outcome(&bad), AuthenticateOutcome::Bad);
    let good = send(&state, broker(), sign_in(PASSWORD, SESSION)).await;
    assert_eq!(authenticate_outcome(&good), AuthenticateOutcome::Ok);
}

#[tokio::test]
async fn weak_passwords_and_short_tokens_are_refused_at_the_boundary() {
    let state = state();
    let weak = send(&state, broker(), initialize("short")).await;
    assert!(weak
        .error
        .as_deref()
        .unwrap_or("")
        .contains("IDENTITY_WEAK_PASSWORD"));
    let ok = send(&state, broker(), initialize(PASSWORD)).await;
    assert!(ok.error.is_none(), "{:?}", ok.error);
    let short = send(&state, broker(), sign_in(PASSWORD, "tiny")).await;
    assert!(short
        .error
        .as_deref()
        .unwrap_or("")
        .contains("IDENTITY_INVALID"));
}

#[tokio::test]
async fn identity_authority_is_exact_and_never_implied_by_kg_admin() {
    let state = state();
    let as_kg_admin = send(
        &state,
        context("usr:ops", &["kg:admin"]),
        initialize(PASSWORD),
    )
    .await;
    assert!(
        as_kg_admin
            .error
            .as_deref()
            .unwrap_or("")
            .contains("IDENTITY_NOT_AUTHORIZED"),
        "{:?}",
        as_kg_admin.error
    );
    let wildcard = send(
        &state,
        context("usr:ops", &["identity:*"]),
        initialize(PASSWORD),
    )
    .await;
    assert!(wildcard
        .error
        .as_deref()
        .unwrap_or("")
        .contains("IDENTITY_NOT_AUTHORIZED"));
    let broker_ok = send(&state, broker(), initialize(PASSWORD)).await;
    assert!(broker_ok.error.is_none());
}

#[tokio::test]
async fn a_forged_stamp_in_the_body_is_overwritten() {
    let state = state();
    let forged = IdentityStamp::for_actor(IdentityActor {
        principal_id: "usr:ops".to_string(),
        delegated: false,
        scopes: [IDENTITY_AUTHENTICATE_SCOPE.to_string()].into(),
    });
    let mut forged_with_hash = forged.clone();
    forged_with_hash.password_hash = Some("$argon2id$attacker".to_string());
    let response = send_stamped(
        &state,
        context("usr:ops", &["kg:admin"]),
        initialize(PASSWORD),
        Some(forged_with_hash),
    )
    .await;
    assert!(response
        .error
        .as_deref()
        .unwrap_or("")
        .contains("IDENTITY_NOT_AUTHORIZED"));
    assert!(state
        .read()
        .await
        .isolation
        .rbac()
        .identity_store()
        .config()
        .is_none());
}

#[tokio::test]
async fn stamping_clears_every_secret_from_the_op() {
    let state = state();
    let mut method = Method::Identity {
        op: initialize(PASSWORD),
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
    let Method::Identity { op, stamp } = method else {
        unreachable!("the method stays an identity op")
    };
    let IdentityOp::Config(ConfigOp::Initialize { request }) = op else {
        unreachable!("the op is unchanged")
    };
    assert!(request.admin_password.is_empty());
    let stamp = stamp.expect("stamped");
    assert!(stamp.password_hash.unwrap().starts_with("$argon2id$"));
    let replicated = serde_json::to_string(&Method::Identity {
        op: IdentityOp::Config(ConfigOp::Initialize { request }),
        stamp: None,
    })
    .unwrap();
    assert!(!replicated.contains(PASSWORD));
}

fn scim_idp() -> IdentityOp {
    IdentityOp::Idp(IdpOp::Upsert {
        request: IdpConfig {
            idp_id: "okta-scim".to_string(),
            kind: IdpKind::Scim,
            display_name: "Okta".to_string(),
            enabled: true,
            config_json: "{\"provisioner\":\"svc:scim\"}".to_string(),
            secret_ref: None,
            jit_policy: JitPolicy::Deny,
            email_domains: Vec::new(),
            order: 0,
            rules: Vec::new(),
        },
    })
}

fn provision_ann() -> IdentityOp {
    IdentityOp::Idp(IdpOp::Provision {
        request: ProvisionSubject {
            idp_id: "okta-scim".to_string(),
            subject: "00u-ann".to_string(),
            username: "ann".to_string(),
            display_name: None,
            email: None,
            active: true,
            claims: std::collections::BTreeMap::new(),
        },
    })
}

#[tokio::test]
async fn the_broker_reads_idps_and_a_bound_provisioner_provisions_through_the_boundary() {
    let state = state();
    assert!(send(&state, broker(), initialize(PASSWORD))
        .await
        .error
        .is_none());
    let admin = context("usr:bootstrap", &[IDENTITY_ADMIN_SCOPE]);
    let upserted = send(&state, admin, scim_idp()).await;
    assert!(upserted.error.is_none(), "{:?}", upserted.error);
    let listed = send(&state, broker(), IdentityOp::Idp(IdpOp::List)).await;
    assert!(
        listed.error.is_none(),
        "the sign-in page lists IdPs: {:?}",
        listed.error
    );
    let outsider = send(
        &state,
        context("usr:x", &[IDENTITY_SELF_SCOPE]),
        IdentityOp::Idp(IdpOp::List),
    )
    .await;
    assert!(
        outsider.error.is_some(),
        "identity:self does not read the IdP directory"
    );
    let stranger = send(
        &state,
        context("svc:other", &[IDENTITY_PROVISION_SCOPE]),
        provision_ann(),
    )
    .await;
    assert!(
        stranger
            .error
            .as_deref()
            .unwrap_or("")
            .contains("IDENTITY_NOT_AUTHORIZED"),
        "{:?}",
        stranger.error
    );
    let bound = send(
        &state,
        context("svc:scim", &[IDENTITY_PROVISION_SCOPE]),
        provision_ann(),
    )
    .await;
    assert!(bound.error.is_none(), "{:?}", bound.error);
    let store = state.read().await.isolation.rbac().identity_store().clone();
    let (principal, _) = store.sign_in_target("ann");
    let principal = principal.expect("ann was provisioned");
    assert!(
        principal.starts_with("usr:"),
        "the boundary minted the principal id"
    );
    assert_eq!(
        store.user(principal).map(|user| user.source.as_str()),
        Some("scim:okta-scim")
    );
}

fn forgot(token: &str) -> IdentityOp {
    IdentityOp::Token(TokenOp::IssuePasswordReset {
        request: PasswordResetIssue {
            username: "nobody".to_string(),
            token: Secret::new(token),
            ttl_ms: 3_600_000,
        },
    })
}

#[tokio::test]
async fn a_signed_out_reset_request_is_broker_only_and_its_token_meets_the_floor() {
    let state = state();
    assert!(send(&state, broker(), initialize(PASSWORD))
        .await
        .error
        .is_none());
    let weak = send(&state, broker(), forgot("short")).await;
    assert!(
        weak.error
            .as_deref()
            .unwrap_or("")
            .contains("IDENTITY_INVALID"),
        "{:?}",
        weak.error
    );
    let outsider = send(
        &state,
        context("usr:x", &[IDENTITY_SELF_SCOPE]),
        forgot(SESSION),
    )
    .await;
    assert!(
        outsider.error.is_some(),
        "only the broker issues reset links"
    );
    let uniform = send(&state, broker(), forgot(SESSION)).await;
    assert!(uniform.error.is_none(), "{:?}", uniform.error);
    let image = serde_json::to_string(state.read().await.isolation.rbac()).unwrap();
    assert!(
        !image.contains(SESSION),
        "the reset token never reaches the image"
    );
}

#[tokio::test]
async fn first_admin_setup_before_the_system_bootstrap_is_refused_at_the_boundary() {
    let early = unbootstrapped_state();
    let refused = send(&early, broker(), initialize(PASSWORD)).await;
    assert!(
        refused
            .error
            .as_deref()
            .unwrap_or("")
            .contains("SYSTEM_BOOTSTRAP_PENDING"),
        "{:?}",
        refused.error
    );
    assert!(early.read().await.isolation.identity_bootstrap_pending());
    let ready = state();
    let accepted = send(&ready, broker(), initialize(PASSWORD)).await;
    assert!(accepted.error.is_none(), "{:?}", accepted.error);
}
