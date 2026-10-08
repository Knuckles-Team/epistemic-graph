//! The refusals the older identity writer serves when it is turned away from
//! the identity store's namespace are the ones its contract declares. The
//! served boundary replaces any undeclared code with `INTERNAL`, so each case
//! goes through the full signed dispatch.

use super::*;
use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
use crate::protocol::Request;
use crate::server::auth::sign_current_test_request;

const SECRET: &str = "refusal-contract-test-secret";

/// An engine whose System identity `root` is a trusted signer, with an
/// initialized identity store (so `usr:bootstrap` is store-managed).
fn engine() -> Arc<RwLock<ServerState>> {
    let mut isolation = IsolationLayer::new();
    isolation
        .try_bootstrap_system_identity(AgentIdentity {
            agent_id: "root".to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        })
        .expect("a fresh layer bootstraps its System identity");
    let mut stamp = IdentityStamp::for_actor(IdentityActor {
        principal_id: "svc:graph-os".to_string(),
        delegated: false,
        scopes: [IDENTITY_AUTHENTICATE_SCOPE.to_string()].into(),
    });
    stamp.password_hash = Some("$argon2id$bootstrap".to_string());
    isolation
        .try_apply_identity(
            &eg_types::test_support::identity::bootstrap_local_op(),
            &stamp,
            1_800_000_000_000,
            &eg_capabilities::scopes::ScopeRegistry,
        )
        .expect("the store initializes");
    Arc::new(RwLock::new(ServerState::new_for_test(SECRET, isolation)))
}

/// `root` registers `agent_id` with `roles` through the signed dispatch.
async fn register(state: &Arc<RwLock<ServerState>>, agent_id: &str, roles: &[&str]) -> Response {
    let request = sign_current_test_request(
        SECRET,
        Request {
            id: 31,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some("root".to_string()),
            method: Method::RegisterIdentity {
                agent_id: agent_id.to_string(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                signature: String::new(),
                roles: roles.iter().map(|role| role.to_string()).collect(),
            },
        },
    );
    Box::pin(crate::server::dispatch::dispatch(state, request)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn register_identity_serves_its_declared_store_refusals() {
    let state = engine();
    let accepted = register(&state, "carol", &["auditor"]).await;
    assert!(accepted.error.is_none(), "{:?}", accepted.error);
    for (agent_id, roles, code) in [
        ("dave", &["idm:admin"][..], "IDENTITY_STORE_NAMESPACE"),
        (BOOTSTRAP_PRINCIPAL, &[][..], "IDENTITY_STORE_MANAGED"),
    ] {
        let refused = register(&state, agent_id, roles).await;
        assert_eq!(
            refused.error.as_deref(),
            Some(code),
            "RegisterIdentity for {agent_id} served {refused:?}"
        );
        assert!(eg_capabilities::error_routing::method_allows_error(
            "RegisterIdentity",
            code
        ));
    }
}
