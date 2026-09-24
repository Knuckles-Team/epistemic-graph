//! IDM-02 through verification: a token is routed to exactly one trusted
//! issuer; an issuer restricted to service accounts vouches for a principal
//! only when the identity store owns it as an active service.

use super::*;
use crate::server::oidc::{JwtValidator, TrustedIssuer};
use eg_types::identity::*;

const SERVICE_ISSUER: &str = "https://keycloak.example.test/realms/homelab";

struct TrustGuard;

impl Drop for TrustGuard {
    fn drop(&mut self) {
        TEST_OIDC_TRUST.with(|cell| cell.set(&[]));
    }
}

fn install_service_issuer() -> TrustGuard {
    let mut keys = HashMap::new();
    let n = hex::decode(TEST_RSA_MODULUS_HEX).unwrap();
    let e = hex::decode(TEST_RSA_EXPONENT_HEX).unwrap();
    keys.insert(
        OIDC_KID.to_string(),
        jsonwebtoken::DecodingKey::from_rsa_raw_components(&n, &e),
    );
    let trusted: &'static [TrustedIssuer] = Box::leak(Box::new([TrustedIssuer {
        validator: JwtValidator::from_parts(SERVICE_ISSUER, OIDC_AUDIENCE, keys),
        restriction: Some(UserKind::Service),
    }]));
    TEST_OIDC_TRUST.with(|cell| cell.set(trusted));
    TrustGuard
}

fn service_token(sub: &str) -> String {
    let mut claims = oidc_claims(sub, "tenant-a", &["kg:read"], "kg:read");
    claims["iss"] = serde_json::json!(SERVICE_ISSUER);
    sign(&claims)
}

fn claims_for(principal: &str) -> RequestContextClaims {
    RequestContextClaims {
        principal: principal.into(),
        agent_id: principal.into(),
        ..matching_claims()
    }
}

/// A persist dir whose published store owns `svc:alloy` (service) and
/// `usr:alice` (human).
fn published_dir() -> String {
    let dir = crate::server::sql_tables::test_persist_dir();
    let mut store = IdentityStore::default();
    let ctx = ApplyContext {
        now_ms: 1_800_000_000_000,
        classifier: &eg_capabilities::scopes::ScopeRegistry,
    };
    let broker = IdentityStamp::for_actor(IdentityActor {
        principal_id: "svc:graph-os".into(),
        delegated: false,
        scopes: [IDENTITY_AUTHENTICATE_SCOPE.to_string()].into(),
    });
    let init = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode: AuthMode::None,
            admin_username: None,
            admin_password: Secret::default(),
        },
    });
    store.apply(&init, &broker, &ctx).unwrap();
    let admin = IdentityStamp::for_actor(IdentityActor {
        principal_id: "usr:bootstrap".into(),
        delegated: false,
        scopes: [IDENTITY_ADMIN_SCOPE.to_string()].into(),
    });
    for (principal, kind) in [("svc:alloy", UserKind::Service), ("usr:alice", UserKind::Human)] {
        let create = IdentityOp::User(UserOp::Create {
            request: CreateUserRequest {
                username: principal.replace(':', "-"),
                kind,
                principal_id: Some(principal.to_string()),
                display_name: None,
                email: None,
                roles: Default::default(),
                groups: Default::default(),
                password: Secret::default(),
                must_change: false,
            },
        });
        store.apply(&create, &admin, &ctx).unwrap();
    }
    let dir = dir.to_string_lossy().to_string();
    crate::server::identity_view::publish(Some(&dir), &store);
    dir
}

fn verify_as(principal: &str, dir: Option<&str>, nonce: &str) -> Result<VerifiedRequestContext, String> {
    let token = service_token(principal);
    let req = envelope_request(801, nonce, claims_for(principal), Some(&token));
    let context = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay())?;
    enforce_issuer_kind(&context, dir)?;
    Ok(context)
}

#[test]
fn a_service_only_issuer_vouches_for_a_store_service_and_nobody_else() {
    let _primary = install_test_validator();
    let _trust = install_service_issuer();
    let dir = published_dir();
    assert!(verify_as("svc:alloy", Some(&dir), "svc-ok").is_ok());
    let human = verify_as("usr:alice", Some(&dir), "svc-human").unwrap_err();
    assert!(human.contains("IDENTITY_ISSUER_KIND_REFUSED"), "{human}");
    let unknown = verify_as("svc:ghost", Some(&dir), "svc-ghost").unwrap_err();
    assert!(unknown.contains("IDENTITY_ISSUER_KIND_REFUSED"), "{unknown}");
    let no_store = verify_as("svc:alloy", None, "svc-nostore").unwrap_err();
    assert!(no_store.contains("IDENTITY_ISSUER_KIND_REFUSED"), "fails closed: {no_store}");
}

#[test]
fn the_primary_issuer_keeps_vouching_for_anyone_and_an_unknown_issuer_is_refused() {
    let _primary = install_test_validator();
    let _trust = install_service_issuer();
    let token = sign(&oidc_claims("agent:planner", "tenant-a", &["kg:read"], "kg:read"));
    let req = envelope_request(802, "primary-any", matching_claims(), Some(&token));
    let context = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay()).unwrap();
    assert_eq!(context.issuer_kind(), None);
    let mut claims = oidc_claims("agent:planner", "tenant-a", &["kg:read"], "kg:read");
    claims["iss"] = serde_json::json!("https://rogue.example.test");
    let rogue = sign(&claims);
    let req = envelope_request(803, "rogue", matching_claims(), Some(&rogue));
    let error = verify_envelope_v2_with(SECRET, &req, &verified_policy(), &memory_replay()).unwrap_err();
    assert!(error.contains("not trusted"), "{error}");
}
