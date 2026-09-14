//! Authenticated saga key and async scope isolation.
use super::*;
use crate::server::auth::VerifiedRequestContext;

fn authority(actor: &str, tenant: &str, key: &str) -> CarrierAuthority {
    let mut claims = crate::acl::RequestContextClaims::default();
    claims.principal = actor.to_string();
    claims.agent_id = actor.to_string();
    claims.tenant = tenant.to_string();
    claims.audience = "epistemic-graph".into();
    claims.policy_version = "test".into();
    CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
        claims,
        key.into(),
    ))
    .unwrap()
}

#[tokio::test]
async fn request_scopes_are_isolated_and_missing_authority_fails_closed() {
    assert!(current_admin_saga_authority().is_err());
    let first = authority("alice", "a", "key-a");
    let second = authority("bob", "b", "key-b");
    let (a, b) = tokio::join!(
        scope_admin_saga_authority(first, async {
            tokio::task::yield_now().await;
            assert!(
                tokio::spawn(async { current_admin_saga_authority().is_err() })
                    .await
                    .unwrap()
            );
            current_admin_saga_authority()
                .unwrap()
                .idempotency_key()
                .to_string()
        }),
        scope_admin_saga_authority(second, async {
            tokio::task::yield_now().await;
            current_admin_saga_authority()
                .unwrap()
                .idempotency_key()
                .to_string()
        }),
    );
    assert_eq!((a.as_str(), b.as_str()), ("key-a", "key-b"));
    assert!(current_admin_saga_authority().is_err());
}

#[test]
fn saga_keys_separate_tenant_principal_and_authenticated_idempotency() {
    let original = authenticated_admin_saga_id(&authority("alice", "a", "key"));
    for different in [
        authority("bob", "a", "key"),
        authority("alice", "b", "key"),
        authority("alice", "a", "other"),
    ] {
        assert_ne!(original, authenticated_admin_saga_id(&different));
    }
    assert_eq!(
        original,
        authenticated_admin_saga_id(&authority("alice", "a", "key"))
    );
}

#[test]
fn admin_replay_rejects_wrong_json_body_and_scalar_arm() {
    let method = Method::Reshard {
        graph: "g".into(),
        to_shard: 1,
    };
    assert!(validate_admin_method_result(
        &method,
        &crate::protocol::ResultPayload::Json(serde_json::json!({"unrelated": true}))
    )
    .is_err());
    assert!(
        validate_admin_method_result(&method, &crate::protocol::ResultPayload::Bool(true)).is_err()
    );
}
