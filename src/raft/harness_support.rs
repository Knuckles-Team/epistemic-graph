//! Shared setup for in-process Raft harnesses.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::BasicNode;
use tokio::sync::RwLock;

use super::multi::MultiRaft;
use super::{AppCtx, GroupId, NodeId};
use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
use crate::isolation::IsolationLayer;
use crate::protocol::{Method, Request};
use crate::server::persistence::PersistenceBackend;
use crate::server::{compute_verified_envelope_token, ServerState, VerifiedEnvelopeParams};

static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Build a harness `ServerState` over an already-open persistence backend.
///
/// The production baseline ([`ServerState::new`]) with the harness's own
/// settings: the given persistence, wider admission (64 in flight, 16 per
/// graph) and no CDC hub unless a harness asks for one
/// ([`make_state_with_cdc`]). Starting from the baseline keeps a newly added
/// server field from needing a second hand-written default here.
pub(crate) async fn make_state(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
    isolation: IsolationLayer,
    auth_secret: &str,
) -> Arc<RwLock<ServerState>> {
    let mut state = ServerState::new(auth_secret, isolation);
    state.persist_dir = Some(dir.to_string());
    state.persistence = Some(backend);
    state.max_in_flight = Arc::new(tokio::sync::Semaphore::new(64));
    state.read_admission = Arc::new(tokio::sync::Semaphore::new(64));
    state.per_graph_inflight_limit = 16;
    #[cfg(feature = "streaming")]
    {
        state.cdc = None;
    }
    Arc::new(RwLock::new(state))
}

/// Build the state shape used by tests that exercise the in-memory CDC side effect.
pub(crate) async fn make_state_with_cdc(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
    isolation: IsolationLayer,
    auth_secret: &str,
) -> Arc<RwLock<ServerState>> {
    let state = make_state(dir, backend, isolation, auth_secret).await;
    #[cfg(feature = "streaming")]
    {
        state.write().await.cdc = Some(Arc::new(crate::server::cdc::CdcHub::new()));
    }
    state
}

/// Build the explicit System-role identity used by handler-facing harnesses.
pub(crate) fn current_isolation(agent_id: &str) -> IsolationLayer {
    let identity = AgentIdentity {
        agent_id: agent_id.to_owned(),
        role: AgentRole::System,
        teams: Vec::new(),
        roles: Vec::new(),
    };
    let mut isolation = IsolationLayer::new();
    isolation.register_agent(identity);
    isolation
}

/// The four per-harness labels a signed test request is stamped with.
///
/// Bundled because they vary together -- every harness picks one agent and
/// names its nonce, idempotency key and security-state directory after itself --
/// and because eight positional `&str`s at one call site is exactly the shape
/// that silently swaps two of them.
pub(crate) struct HarnessLabels<'a> {
    pub(crate) agent_id: &'a str,
    pub(crate) nonce: &'a str,
    pub(crate) idempotency: &'a str,
    pub(crate) security_state: &'a str,
}

/// Mint a verified request envelope with the harness's test deployment claims.
pub(crate) fn signed_request(
    id: u64,
    graph: &str,
    method: Method,
    secret: &str,
    labels: HarnessLabels<'_>,
) -> Request {
    let HarnessLabels {
        agent_id,
        nonce: nonce_label,
        idempotency: idempotency_label,
        security_state: security_state_label,
    } = labels;
    for (key, value) in [
        ("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test"),
        ("EPISTEMIC_GRAPH_TENANT", "tenant-shared"),
        ("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test"),
    ] {
        std::env::set_var(key, value);
    }
    let security_state_dir =
        std::env::temp_dir().join(format!("{security_state_label}-{}", std::process::id()));
    std::env::set_var("EPISTEMIC_GRAPH_SECURITY_STATE_DIR", security_state_dir);

    let principal = agent_id.to_owned();
    let context = RequestContextClaims {
        principal: principal.clone(),
        tenant: "tenant-shared".to_string(),
        audience: "epistemic-graph-test".to_string(),
        agent_id: principal.clone(),
        scopes: vec!["*".to_string()],
        policy_version: "policy-test".to_string(),
        ..Default::default()
    };
    let mut request = Request {
        id,
        graph: graph.to_owned(),
        auth_token: String::new(),
        agent_id: Some(principal),
        method,
    };
    let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the Unix epoch");
    let nonce = format!(
        "{nonce_label}-{}-{id}-{sequence}-{}",
        std::process::id(),
        issued_at.as_nanos()
    );
    let idempotency_key = format!("{idempotency_label}-{id}-{sequence}");
    let token_params = VerifiedEnvelopeParams {
        context: &context,
        timestamp: issued_at.as_secs(),
        nonce: &nonce,
        idempotency_key: &idempotency_key,
    };
    request.auth_token = compute_verified_envelope_token(secret, &request, &token_params);
    request
}

async fn wait_for_leader(multi: &Arc<MultiRaft>, gid: GroupId, node_id: NodeId) {
    let group = multi.group(gid).await.expect("group exists");
    let elected = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if group.current_leader().await == Some(node_id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
    })
    .await;
    if elected.is_err() {
        panic!("group {gid} must elect a leader");
    }
}

/// Start a one-node cluster, create its requested groups, and wait for each leader.
/// [`start_single_node_groups`], but RECOVERING the durable graph image into the
/// serving projection first -- the order a restarting process uses.
///
/// `EgStore::open`'s own doc states the contract: "the graph DATA is recovered
/// separately by the M2 `load_all` path BEFORE Raft starts, so on boot the
/// applied pointers and the on-disk graph data agree." A harness that brings the
/// groups up and only then calls `load_all` inverts that, and is not simulating
/// the restart it claims to.
pub(crate) async fn start_recovered_single_node_groups(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
    isolation: IsolationLayer,
    auth_secret: &str,
    group_ids: &[GroupId],
) -> (Arc<MultiRaft>, Arc<RwLock<ServerState>>) {
    let state = make_state(dir, backend.clone(), isolation, auth_secret).await;
    backend
        .load_all(&state)
        .await
        .expect("recover the durable graph image before Raft starts");
    start_groups_on_state(backend, group_ids, state).await
}

pub(crate) async fn start_single_node_groups(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
    isolation: IsolationLayer,
    auth_secret: &str,
    group_ids: &[GroupId],
) -> (Arc<MultiRaft>, Arc<RwLock<ServerState>>) {
    let state = make_state(dir, backend.clone(), isolation, auth_secret).await;
    start_groups_on_state(backend, group_ids, state).await
}

/// Bind the harness listener, start `MultiRaft` over `state`, and bring every
/// requested group to a leader. Shared by both starters above, which differ only
/// in whether the durable image is recovered into `state` first.
async fn start_groups_on_state(
    backend: Arc<dyn PersistenceBackend>,
    group_ids: &[GroupId],
    state: Arc<RwLock<ServerState>>,
) -> (Arc<MultiRaft>, Arc<RwLock<ServerState>>) {
    let ctx = AppCtx {
        state: state.clone(),
        router: None,
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind harness listener");
    let port = listener
        .local_addr()
        .expect("read harness listener address")
        .port();
    drop(listener);
    let node_id: NodeId = 1;
    let bind_addr = format!("127.0.0.1:{port}");
    let peers: BTreeMap<NodeId, BasicNode> = [(node_id, BasicNode::new(bind_addr.clone()))].into();
    let multi = MultiRaft::start(node_id, bind_addr, backend, ctx)
        .await
        .expect("start multi");

    for &gid in group_ids {
        if gid == super::DEFAULT_GROUP {
            multi.ensure_group(gid).await.unwrap();
        } else {
            multi.create_group(gid, peers.clone(), true).await.unwrap();
        }
    }
    for &gid in group_ids {
        wait_for_leader(&multi, gid, node_id).await;
    }
    (multi, state)
}
