//! Source regressions only: these fixtures do not qualify a deployed broker,
//! credential authority, generated client or mounted serving surface.
use super::*;
use eg_types::identity::{
    ApiKeyUse, ObjectRef, Secret, SessionTouch, UserKind, UserStatus, IDENTITY_AUTHENTICATE_SCOPE,
};
use serde_json::json;

fn policy() -> RequestContextPolicy {
    RequestContextPolicy {
        expected_tenant: "tenant".into(),
        expected_audience: "engine".into(),
        expected_policy_version: "policy-7".into(),
    }
}

fn resolution() -> PrincipalResolution {
    PrincipalResolution {
        request_context: (),
        principal_id: "usr:alice".into(),
        username: "alice".into(),
        kind: UserKind::Human,
        status: UserStatus::Active,
        is_bootstrap: false,
        roles: ["kg:write".into()].into(),
        groups: Default::default(),
        scopes: ["kg:read".into()].into(),
        mfa_required: false,
        mfa_enrolled: false,
        session_mfa_pending: false,
    }
}

fn session_op() -> IdentityOp {
    IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    })
}

fn context() -> VerifiedRequestContext {
    let mut claims = compose_resolution(resolution(), &policy())
        .unwrap()
        .request_context;
    claims.principal = "svc:broker".into();
    claims.agent_id = claims.principal.clone();
    claims.scopes = vec![IDENTITY_AUTHENTICATE_SCOPE.into()];
    VerifiedRequestContext::from_verified_claims(claims, "test-only".into())
}

fn broker_store(status: &str, scopes: &[&str]) -> IdentityStore {
    serde_json::from_value(json!({
        "users": {"svc:broker": {
            "principal_id":"svc:broker", "username":"broker", "kind":"service",
            "status":status, "source":"local", "roles":["broker"], "created_at_ms":1
        }},
        "roles": {"broker": {"role_id":"broker", "name":"broker", "scopes":scopes}}
    }))
    .unwrap()
}

fn admission(store: &IdentityStore, ctx: &VerifiedRequestContext) -> Result<(), String> {
    authorize_resolution(
        store,
        &session_op(),
        &IdentityStamp::for_actor(identity_actor(ctx)),
        ctx,
        ElevationStampAuthority::External,
        &policy(),
    )
}

#[test]
fn preserves_narrow_scopes_without_role_union() {
    let value = compose_resolution(resolution(), &policy()).unwrap();
    assert_eq!(value.request_context.scopes, vec!["kg:read"]);
    assert_eq!(value.request_context.roles, vec!["kg:write"]);
    assert_eq!(value.scopes, ["kg:read".into()].into());
    assert_eq!(value.request_context.principal, "usr:alice");
    assert_eq!(value.request_context.agent_id, "usr:alice");
    assert!(value.request_context.delegation.is_empty());
    assert_eq!(value.request_context.policy_version, "policy-7");
}

#[test]
fn inactive_and_pending_mfa_refuse() {
    for status in [
        UserStatus::Disabled,
        UserStatus::Deprovisioned,
        UserStatus::PendingReset,
        UserStatus::PendingVerification,
    ] {
        let mut raw = resolution();
        raw.status = status;
        assert!(compose_resolution(raw, &policy()).is_err());
    }
    let mut raw = resolution();
    raw.session_mfa_pending = true;
    assert!(compose_resolution(raw, &policy()).is_err());
    let mut raw = resolution();
    raw.mfa_required = true;
    assert!(compose_resolution(raw, &policy()).is_err());
}

#[test]
fn broker_lookup_is_not_credential_provenance() {
    for id in ["usr:alice", "usr:bob", "svc:broker"] {
        let op = IdentityOp::User(UserOp::Resolve {
            request: ObjectRef { id: id.into() },
        });
        assert!(!is_credential_resolution(&op));
        let ctx = context();
        assert!(authorize_resolution(
            &broker_store("active", &[IDENTITY_AUTHENTICATE_SCOPE]),
            &op,
            &IdentityStamp::for_actor(identity_actor(&ctx)),
            &ctx,
            ElevationStampAuthority::External,
            &policy()
        )
        .is_err());
    }
}

#[test]
fn api_key_is_a_credential_operation_but_lookup_is_not() {
    assert!(is_credential_resolution(&session_op()));
    assert!(is_credential_resolution(&IdentityOp::Token(
        TokenOp::VerifyApiKey {
            request: ApiKeyUse {
                key_id: "key".into(),
                secret: Secret::default()
            },
        }
    )));
}

#[test]
fn unmanaged_broker_has_no_current_authority() {
    assert!(admission(&IdentityStore::default(), &context()).is_err());
}

#[test]
fn managed_broker_requires_exact_current_scope_and_active_status() {
    assert!(admission(
        &broker_store("active", &[IDENTITY_AUTHENTICATE_SCOPE]),
        &context()
    )
    .is_ok());
    for scopes in [
        vec![],
        vec!["identity:admin"],
        vec!["identity:read"],
        vec!["identity:*"],
    ] {
        assert!(admission(&broker_store("active", &scopes), &context()).is_err());
    }
    for status in ["disabled", "deprovisioned"] {
        assert!(admission(
            &broker_store(status, &[IDENTITY_AUTHENTICATE_SCOPE]),
            &context()
        )
        .is_err());
    }
}

#[test]
fn roles_only_and_unsigned_scope_widening_do_not_authorize_broker() {
    let mut claims = context().claims().clone();
    claims.scopes.clear();
    claims.roles = vec![IDENTITY_AUTHENTICATE_SCOPE.into()];
    let ctx = VerifiedRequestContext::from_verified_claims(claims, "test".into());
    assert!(admission(
        &broker_store("active", &[IDENTITY_AUTHENTICATE_SCOPE]),
        &ctx
    )
    .is_err());
}

#[test]
fn delegated_or_mismatched_agent_broker_refuses() {
    for chain in [vec![], vec!["svc:broker".into(), "svc:other".into()]] {
        let mut claims = context().claims().clone();
        claims.agent_id = "svc:other".into();
        claims.delegation = chain;
        let ctx = VerifiedRequestContext::from_verified_claims(claims, "test".into());
        assert!(admission(
            &broker_store("active", &[IDENTITY_AUTHENTICATE_SCOPE]),
            &ctx
        )
        .is_err());
    }
}

#[test]
fn forged_stamp_and_replicated_context_cannot_issue() {
    let ctx = context();
    let store = broker_store("active", &[IDENTITY_AUTHENTICATE_SCOPE]);
    let mut stamp = IdentityStamp::for_actor(identity_actor(&ctx));
    stamp.actor.principal_id = "usr:alice".into();
    assert!(authorize_resolution(
        &store,
        &session_op(),
        &stamp,
        &ctx,
        ElevationStampAuthority::External,
        &policy()
    )
    .is_err());
    assert!(authorize_resolution(
        &store,
        &session_op(),
        &IdentityStamp::for_actor(identity_actor(&ctx)),
        &ctx,
        ElevationStampAuthority::Replicated,
        &policy()
    )
    .is_err());
}

#[test]
fn deployment_binding_rejects_missing_and_mismatched_values() {
    for field in ["tenant", "audience", "policy_version"] {
        let original = serde_json::to_value(context().claims()).unwrap();
        for replacement in ["", "wrong"] {
            let mut value = original.clone();
            value[field] = json!(replacement);
            let claims: RequestContextClaims = serde_json::from_value(value).unwrap();
            assert!(validate_policy(&claims, &policy()).is_err());
        }
    }
    let mut missing = policy();
    missing.expected_policy_version.clear();
    assert!(compose_resolution(resolution(), &missing).is_err());
}

#[test]
fn public_wire_requires_nonnull_strict_context() {
    let public = IdentityReply::Resolution(compose_resolution(resolution(), &policy()).unwrap());
    let wire = serde_json::to_value(public).unwrap();
    assert!(serde_json::from_value::<IdentityReply<RequestContextClaims>>(wire.clone()).is_ok());
    for field in [
        "principal",
        "tenant",
        "audience",
        "agent_id",
        "roles",
        "scopes",
        "policy_version",
        "delegation",
    ] {
        let mut bad = wire.clone();
        bad["value"]["request_context"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(serde_json::from_value::<IdentityReply<RequestContextClaims>>(bad).is_err());
    }
    let mut missing = wire.clone();
    missing["value"]
        .as_object_mut()
        .unwrap()
        .remove("request_context");
    assert!(serde_json::from_value::<IdentityReply<RequestContextClaims>>(missing).is_err());
    let mut null = wire.clone();
    null["value"]["request_context"] = json!(null);
    assert!(serde_json::from_value::<IdentityReply<RequestContextClaims>>(null).is_err());
    let mut extra = wire;
    extra["value"]["request_context"]["oidc_token"] = json!("not-a-context-field");
    assert!(serde_json::from_value::<IdentityReply<RequestContextClaims>>(extra).is_err());
}

#[test]
fn nonresolution_reply_does_not_request_authority() {
    let raw: IdentityReply = IdentityReply::Done { changed: false };
    let public: Result<IdentityReply<RequestContextClaims>, String> =
        raw.try_with_request_context(|_| panic!("nonresolution must not enter issuance"));
    assert!(matches!(
        public.unwrap(),
        IdentityReply::Done { changed: false }
    ));
}

/// Boots a fresh locked engine fixture, a broker context and a pre-mutation
/// identity-store snapshot. Shared by refusal tests that assert the store is
/// untouched after a rejected operation.
async fn locked_fixture_snapshot() -> (
    Arc<RwLock<ServerState>>,
    IdentityStore,
    VerifiedRequestContext,
) {
    let state = crate::server::dispatch::test_support::bootstrapped_state();
    let before = state.read().await.isolation.rbac().identity_store().clone();
    (state, before, context())
}

#[tokio::test]
async fn arbitrary_broker_lookup_refuses_before_store_change() {
    let (state, before, ctx) = locked_fixture_snapshot().await;
    let mut state = state.write().await;
    let op = IdentityOp::User(UserOp::Resolve {
        request: ObjectRef {
            id: "usr:alice".into(),
        },
    });
    let result = apply_and_compose(
        &mut state,
        &op,
        &IdentityStamp::for_actor(identity_actor(&ctx)),
        &ctx,
        ElevationStampAuthority::External,
        100,
    );
    assert_eq!(result.unwrap_err(), NO_PROVENANCE.to_string());
    assert_eq!(state.isolation.rbac().identity_store(), &before);
}

#[tokio::test]
async fn unauthorized_broker_cannot_use_subject_credential() {
    let (state, before, ctx) = locked_fixture_snapshot().await;
    let mut state = state.write().await;
    let result = apply_and_compose(
        &mut state,
        &session_op(),
        &IdentityStamp::for_actor(identity_actor(&ctx)),
        &ctx,
        ElevationStampAuthority::External,
        100,
    );
    assert!(result.is_err());
    assert_eq!(state.isolation.rbac().identity_store(), &before);
}

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::dispatch::test_support::{bootstrapped_state, send, verified};
use eg_types::identity::{
    AccessOp, AuthMode, AuthenticateRequest, ConfigOp, CredentialOp, InitializeRequest, MfaOp,
    BOOTSTRAP_PRINCIPAL, IDENTITY_ADMIN_SCOPE,
};
use std::sync::Arc;
use tokio::sync::RwLock;

const PASSWORD: &str = "source regression password phrase";
const SESSION: &str = "source-session-0123456789abcdefghijklmnopqrstuv";

fn live_broker() -> VerifiedRequestContext {
    verified("svc:broker", &[IDENTITY_AUTHENTICATE_SCOPE], &[])
}

async fn send_op(
    state: &Arc<RwLock<ServerState>>,
    ctx: VerifiedRequestContext,
    op: IdentityOp,
) -> Response {
    send(state, ctx, Method::Identity { op, stamp: None }).await
}

fn session_request() -> IdentityOp {
    IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
            session_token: Secret::new(SESSION),
            code: Secret::default(),
        },
    })
}

/// Test-only in-memory engine, initialized through the existing boundary.
/// This creates no live grants and is not an installed-authority qualification.
async fn ready_engine() -> Arc<RwLock<ServerState>> {
    let state = bootstrapped_state();
    let initialized = send_op(
        &state,
        live_broker(),
        IdentityOp::Config(ConfigOp::Initialize {
            request: InitializeRequest {
                mode: AuthMode::Local,
                admin_username: Some("operator".into()),
                admin_password: Secret::new(PASSWORD),
            },
        }),
    )
    .await;
    assert!(initialized.error.is_none(), "{initialized:?}");
    let admin = || verified(BOOTSTRAP_PRINCIPAL, &[IDENTITY_ADMIN_SCOPE], &[]);
    let role = send_op(
        &state,
        admin(),
        IdentityOp::Access(AccessOp::UpsertRole {
            request: serde_json::from_value(json!({"role_id":"broker", "name":"broker",
            "scopes":[IDENTITY_AUTHENTICATE_SCOPE]}))
            .unwrap(),
        }),
    )
    .await;
    assert!(role.error.is_none(), "{role:?}");
    let user = send_op(
        &state,
        admin(),
        IdentityOp::User(UserOp::Create {
            request: serde_json::from_value(json!({"username":"broker", "kind":"service",
            "principal_id":"svc:broker", "roles":["broker"]}))
            .unwrap(),
        }),
    )
    .await;
    assert!(user.error.is_none(), "{user:?}");
    let login = send_op(
        &state,
        live_broker(),
        IdentityOp::Credential(CredentialOp::Authenticate {
            request: AuthenticateRequest {
                username: "operator".into(),
                password: Secret::new(PASSWORD),
                session_token: Secret::new(SESSION),
                ip_prefix: None,
                new_password: Secret::default(),
            },
        }),
    )
    .await;
    assert!(login.error.is_none(), "{login:?}");
    let wire = serde_json::to_value(&login).unwrap();
    assert_eq!(wire["result"]["value"]["outcome"], "ok");
    state
}

fn resolved(response: &Response) -> PrincipalResolution<RequestContextClaims> {
    assert!(response.error.is_none(), "{response:?}");
    let Some(ResultPayload::Json(value)) = &response.result else {
        panic!("missing JSON response")
    };
    match serde_json::from_value::<IdentityReply<RequestContextClaims>>(value.clone()).unwrap() {
        IdentityReply::Resolution(value) => value,
        _ => panic!("expected authoritative resolution"),
    }
}

fn assert_refusal(response: &Response, expected: IdentityRefusal) {
    let wire = serde_json::to_value(response).unwrap();
    assert_eq!(wire["error"], expected.to_string());
    assert!(wire.get("result").is_none());
    assert_ne!(wire["error_detail"], "unclassified engine refusal");
}

#[tokio::test]
async fn successful_session_resolution_runs_boundary_store_and_public_response() {
    let state = ready_engine().await;
    let response = send_op(&state, live_broker(), session_request()).await;
    let value = resolved(&response);
    assert_eq!(value.principal_id, BOOTSTRAP_PRINCIPAL);
    assert_eq!(value.request_context.principal, BOOTSTRAP_PRINCIPAL);
    assert_eq!(value.request_context.agent_id, BOOTSTRAP_PRINCIPAL);
    assert!(value.request_context.delegation.is_empty());
    assert_eq!(
        value.request_context.scopes,
        value.scopes.iter().cloned().collect::<Vec<_>>()
    );
    assert_eq!(
        value.request_context.roles,
        value.roles.iter().cloned().collect::<Vec<_>>()
    );
    assert_eq!(
        value.request_context.policy_version,
        request_context_policy().unwrap().expected_policy_version
    );
}

#[test]
fn all_producer_refusals_survive_final_response_classification() {
    let ctx = context();
    let stamp = IdentityStamp::for_actor(identity_actor(&ctx));
    let mut bad_policy = policy();
    bad_policy.expected_policy_version.clear();
    let mut pending = resolution();
    pending.session_mfa_pending = true;
    let cases = [
        (NO_PROVENANCE.to_string(), IdentityRefusal::NotAuthorized),
        (
            admission(&IdentityStore::default(), &ctx).unwrap_err(),
            IdentityRefusal::NotAuthorized,
        ),
        (
            validate_policy(ctx.claims(), &bad_policy).unwrap_err(),
            IdentityRefusal::PreconditionFailed,
        ),
        (
            compose_resolution(pending, &policy()).unwrap_err(),
            IdentityRefusal::NotAuthorized,
        ),
    ];
    for (error, expected) in cases {
        let response = super::super::respond(9, &session_op(), &stamp, Err(error));
        assert_refusal(&response, expected);
    }
}

/// Edit an in-memory fixture image through the policy-store adapter, then load
/// it normally. Used only to put session time/MFA at precise test boundaries.
fn edit_fixture_identity(state: &mut ServerState, edit: impl FnOnce(&mut serde_json::Value)) {
    let adapter = state.isolation.policy_store().unwrap();
    let (policy, identities, bootstrap) = adapter.load().unwrap();
    let mut value = serde_json::to_value(policy).unwrap();
    edit(&mut value["identity"]);
    let policy = serde_json::from_value(value).unwrap();
    adapter.save(&policy, &identities, bootstrap).unwrap();
    state.isolation = crate::isolation::IsolationLayer::with_policy_store(adapter).unwrap();
}

#[tokio::test]
async fn credential_expiring_while_waiting_for_write_lock_refuses_without_touch() {
    use std::future::Future;
    use std::task::Poll;
    let state = ready_engine().await;
    let ctx = live_broker();
    let mut method = Method::Identity {
        op: session_request(),
        stamp: None,
    };
    super::super::stamp_identity(&state, &mut method, &ctx, ElevationStampAuthority::External)
        .await
        .unwrap();
    let Method::Identity { op, stamp } = method else {
        unreachable!()
    };
    let mut guard = state.write().await;
    let hash = super::super::secrets::token_hash(SESSION);
    let before_poll = crate::server::dispatch::authoritative_now_ms();
    let expires = before_poll + 200;
    let old_touch = before_poll - 120_000;
    edit_fixture_identity(&mut guard, |identity| {
        identity["sessions"][&hash]["idle_expires_at_ms"] = json!(expires);
        identity["sessions"][&hash]["last_seen_at_ms"] = json!(old_touch);
    });
    let before = guard.isolation.rbac().identity_store().clone();
    let mut blocked = Box::pin(super::super::dispatch_identity(
        &state,
        10,
        &ctx,
        ElevationStampAuthority::External,
        (op, stamp),
    ));
    // Poll once while holding the lock: the old implementation samples its
    // stale timestamp here; the fixed implementation must still await the lock.
    std::future::poll_fn(|cx| {
        assert!(blocked.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        crate::server::dispatch::authoritative_now_ms() < expires,
        "fixture must start live"
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while crate::server::dispatch::authoritative_now_ms() < expires {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bounded expiry wait");
    drop(guard);
    let response = blocked.await;
    assert_refusal(&response, IdentityRefusal::NotFound);
    assert_eq!(
        state.read().await.isolation.rbac().identity_store(),
        &before
    );
}

#[tokio::test]
async fn pending_mfa_refusal_preserves_a_completable_ceremony_after_touch() {
    let state = ready_engine().await;
    let hash = super::super::secrets::token_hash(SESSION);
    let code = "recovery-0123456789abcdefghijklmnop";
    let code_hash = super::super::secrets::token_hash(code);
    let now = crate::server::dispatch::authoritative_now_ms();
    {
        let mut guard = state.write().await;
        edit_fixture_identity(&mut guard, |identity| {
            identity["sessions"][&hash]["mfa_pending"] = json!(true);
            identity["sessions"][&hash]["last_seen_at_ms"] = json!(now - 120_000);
            identity["totp"] = json!({(BOOTSTRAP_PRINCIPAL): {
                "sealed_secret":"fixture-unused-by-recovery", "confirmed_at_ms":1, "last_step":0
            }});
            identity["recovery"] = json!({(BOOTSTRAP_PRINCIPAL): [{
                "code_hash":code_hash, "used_at_ms":null
            }]});
        });
    }
    let refused = send_op(&state, live_broker(), session_request()).await;
    assert_refusal(&refused, IdentityRefusal::NotAuthorized);
    let stored =
        serde_json::to_value(state.read().await.isolation.rbac().identity_store()).unwrap();
    assert_eq!(stored["sessions"][&hash]["mfa_pending"], true);
    assert!(
        stored["sessions"][&hash]["last_seen_at_ms"]
            .as_u64()
            .unwrap()
            >= now
    );
    assert!(stored["recovery"][BOOTSTRAP_PRINCIPAL][0]["used_at_ms"].is_null());
    let completed = send_op(
        &state,
        live_broker(),
        IdentityOp::Mfa(MfaOp::ConsumeRecoveryCode {
            request: SessionTouch {
                session_token: Secret::new(SESSION),
                code: Secret::new(code),
            },
        }),
    )
    .await;
    assert!(completed.error.is_none(), "{completed:?}");
    assert_eq!(
        serde_json::to_value(&completed).unwrap()["result"]["value"]["outcome"],
        "ok"
    );
    let response = send_op(&state, live_broker(), session_request()).await;
    assert!(!resolved(&response).session_mfa_pending);
}
