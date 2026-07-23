use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use epistemic_graph::acl::{AgentIdentity, AgentRole, RequestContextClaims};
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::protocol::{build_context_operation_signature_bytes, Method, Request};
use epistemic_graph::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub const TEST_AGENT: &str = "integration-test-agent";
const TEST_AUDIENCE: &str = "epistemic-graph-integration-tests";
const TEST_TENANT: &str = "integration-test-tenant";
const TEST_POLICY_VERSION: &str = "integration-test-policy-v1";
const TEST_AGENT_SIGNER_KEY: &str = "integration-test-agent-signing-key";
const ROOT_SIGNER_KEY: &str = "integration-test-root-signing-key";

static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static SECURITY_STATE_DIR: OnceLock<String> = OnceLock::new();

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the Unix epoch")
        .as_secs()
}

fn security_state_dir() -> &'static str {
    SECURITY_STATE_DIR
        .get_or_init(|| {
            let epoch_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the system clock is after the Unix epoch")
                .as_nanos();
            std::env::temp_dir()
                .join(format!(
                    "epistemic-graph-integration-auth-{}-{epoch_nanos}",
                    std::process::id()
                ))
                .to_string_lossy()
                .into_owned()
        })
        .as_str()
}

pub fn configure_authority() {
    // Integration-test crates link the library without cfg(test), so they get
    // require_oidc()'s production fail-closed default instead of the library
    // unit tests' thread-local shield. These tests exercise the HMAC envelope
    // path deliberately; OIDC-enforcement coverage lives in the library's own
    // auth tests.
    std::env::set_var("EPISTEMIC_GRAPH_REQUIRE_OIDC", "false");
    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", TEST_AUDIENCE);
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", TEST_TENANT);
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", TEST_POLICY_VERSION);
    std::env::set_var("EPISTEMIC_GRAPH_SECURITY_STATE_DIR", security_state_dir());
    std::env::set_var(
        "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON",
        format!(r#"{{"{TEST_AGENT}":"{TEST_AGENT_SIGNER_KEY}","root":"{ROOT_SIGNER_KEY}"}}"#),
    );
}

fn context_for(agent_id: &str) -> RequestContextClaims {
    RequestContextClaims {
        principal: agent_id.to_string(),
        tenant: TEST_TENANT.to_string(),
        audience: TEST_AUDIENCE.to_string(),
        agent_id: agent_id.to_string(),
        roles: Vec::new(),
        scopes: vec!["*".to_string()],
        policy_version: TEST_POLICY_VERSION.to_string(),
        delegation: Vec::new(),
        node: None,
    }
}

#[allow(dead_code)]
fn signer_key(agent_id: &str) -> &'static str {
    match agent_id {
        TEST_AGENT => TEST_AGENT_SIGNER_KEY,
        "root" => ROOT_SIGNER_KEY,
        other => panic!("no integration-test signer key is provisioned for {other}"),
    }
}

pub fn current_isolation() -> IsolationLayer {
    let mut isolation = IsolationLayer::new();
    isolation.register_agent(AgentIdentity {
        agent_id: TEST_AGENT.to_string(),
        role: AgentRole::System,
        teams: Vec::new(),
        roles: Vec::new(),
    });
    isolation
}

pub fn signed_request(secret: &str, id: u64, graph: &str, method: Method) -> Request {
    signed_request_as(secret, id, graph, TEST_AGENT, method)
}

pub fn signed_request_as(
    secret: &str,
    id: u64,
    graph: &str,
    agent_id: &str,
    method: Method,
) -> Request {
    configure_authority();
    let context = context_for(agent_id);
    let mut request = Request {
        id,
        graph: graph.to_string(),
        auth_token: String::new(),
        agent_id: Some(agent_id.to_string()),
        method,
    };
    let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nonce = format!("integration-{}-{id}-{sequence}", std::process::id());
    let idempotency_key = format!("integration-request-{}-{id}-{sequence}", std::process::id());
    request.auth_token = compute_verified_envelope_token(
        secret,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: now_secs(),
            nonce: &nonce,
            idempotency_key: &idempotency_key,
        },
    );
    request
}

/// Node-bound variant of [`signed_request`] (ADR-3 / W1.9,
/// `reports/wave1/ADR-scale-trio.md`): mints the SAME kind of request but
/// with an explicit `node` claim, so a caller can exercise node-binding
/// enforcement through the real dispatch path rather than only the `auth`
/// module's own unit tests. `node: None` mints a request with no claim at
/// all -- an "old client" that predates node binding.
#[allow(dead_code)]
pub fn signed_request_with_node(
    secret: &str,
    id: u64,
    graph: &str,
    method: Method,
    node: Option<&str>,
) -> Request {
    configure_authority();
    let mut context = context_for(TEST_AGENT);
    context.node = node.map(str::to_string);
    let mut request = Request {
        id,
        graph: graph.to_string(),
        auth_token: String::new(),
        agent_id: Some(TEST_AGENT.to_string()),
        method,
    };
    let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nonce = format!("integration-{}-{id}-{sequence}", std::process::id());
    let idempotency_key = format!("integration-request-{}-{id}-{sequence}", std::process::id());
    request.auth_token = compute_verified_envelope_token(
        secret,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: now_secs(),
            nonce: &nonce,
            idempotency_key: &idempotency_key,
        },
    );
    request
}

/// Bundled arguments for [`signed_register_identity_request`], kept under the
/// clippy argument-count ceiling without dropping any of the fields the
/// signed-envelope construction needs.
#[allow(dead_code)]
pub struct SignedRegisterIdentity<'a> {
    pub secret: &'a str,
    pub id: u64,
    pub graph: &'a str,
    pub actor: &'a str,
    pub registered_agent: &'a str,
    pub role: AgentRole,
    pub teams: Vec<String>,
    pub roles: Vec<String>,
}

/// A unique tempdir-backed authoritative redb persistence handle for served-dispatch
/// tests whose mutations must reach the durable `MutationBatch` gateway. Since the
/// assimilation of the gateway persistence requirement, a `persistence: None` state
/// makes the gateway reject every routed mutation, so a served test that stages a
/// write then reads it back never reaches its assertions. This mirrors
/// `src/raft/xshard_harness.rs`'s setup: open one authoritative `RedbBackend` over a
/// per-call unique dir (so parallel tests never contend redb's exclusive per-file
/// lock) and hand back the `(persist_dir, persistence)` pair the `ServerState` literal
/// needs. On a build without `redb` the durable gateway does not exist, so this yields
/// the pre-existing `(None, None)` and the caller compiles unchanged.
#[cfg(feature = "redb")]
#[allow(dead_code)]
pub fn tempdir_persistence() -> (
    Option<String>,
    Option<std::sync::Arc<dyn epistemic_graph::server::persistence::PersistenceBackend>>,
) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("eg-test-persist-{}-{seq}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test persist dir");
    let dir_s = dir.to_string_lossy().into_owned();
    let backend: std::sync::Arc<dyn epistemic_graph::server::persistence::PersistenceBackend> =
        std::sync::Arc::new(
            epistemic_graph::server::persistence::redb_backend::RedbBackend::open(
                dir_s.clone(),
                epistemic_graph::durability::DurabilityPolicy::Each,
                4096,
            )
            .expect("open tempdir redb backend"),
        );
    (Some(dir_s), Some(backend))
}

/// `redb`-off fallback: no durable gateway exists, so served tests keep the
/// pre-existing behavior (they cannot reach the durable-write assertions, but they
/// compile). See the `redb`-on variant above.
#[cfg(not(feature = "redb"))]
#[allow(dead_code)]
pub fn tempdir_persistence() -> (
    Option<String>,
    Option<std::sync::Arc<dyn epistemic_graph::server::persistence::PersistenceBackend>>,
) {
    (None, None)
}

#[allow(dead_code)]
pub fn signed_register_identity_request(args: SignedRegisterIdentity<'_>) -> Request {
    let SignedRegisterIdentity {
        secret,
        id,
        graph,
        actor,
        registered_agent,
        role,
        teams,
        roles,
    } = args;
    configure_authority();
    let context = context_for(actor);
    let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nonce = format!("integration-{}-{id}-{sequence}", std::process::id());
    let idempotency_key = format!("integration-request-{}-{id}-{sequence}", std::process::id());

    let unsigned_operation = Method::RegisterIdentity {
        agent_id: registered_agent.to_string(),
        role: role.clone(),
        teams: teams.clone(),
        signature: String::new(),
        roles: roles.clone(),
    };
    let operation_bytes = build_context_operation_signature_bytes(
        "eg-register-identity-v2",
        &context,
        &idempotency_key,
        graph,
        &unsigned_operation.canonical_body_bytes(),
    );
    let digest = Sha256::digest(operation_bytes);
    let mut mac = Hmac::<Sha256>::new_from_slice(signer_key(actor).as_bytes())
        .expect("HMAC accepts any signing-key length");
    mac.update(&digest);
    let signature = format!("{actor}:{}", hex::encode(mac.finalize().into_bytes()));

    let mut request = Request {
        id,
        graph: graph.to_string(),
        auth_token: String::new(),
        agent_id: Some(actor.to_string()),
        method: Method::RegisterIdentity {
            agent_id: registered_agent.to_string(),
            role,
            teams,
            signature,
            roles,
        },
    };
    request.auth_token = compute_verified_envelope_token(
        secret,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: now_secs(),
            nonce: &nonce,
            idempotency_key: &idempotency_key,
        },
    );
    request
}
