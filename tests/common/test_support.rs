// Each integration-test crate compiles this shared fixture module independently
// and intentionally uses only the helpers needed by that target.
#![allow(dead_code)]

use std::sync::Arc;

use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::protocol::{Method, Request};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::persistence::PersistenceBackend;
use epistemic_graph::server::ServerState;
use tokio::sync::RwLock;

pub type SharedPersistence = Arc<dyn PersistenceBackend>;
pub type SharedState = Arc<RwLock<ServerState>>;

fn make_state(
    auth_secret: &str,
    isolation: IsolationLayer,
    persist_dir: Option<String>,
    persistence: Option<SharedPersistence>,
) -> ServerState {
    let mut state = ServerState::new_for_test(auth_secret.to_owned(), isolation);
    state.persist_dir = persist_dir;
    state.persistence = persistence;
    state
}

pub fn state_with_registry(
    auth_secret: &str,
    isolation: IsolationLayer,
    registry: GraphRegistry,
    persist_dir: Option<String>,
    persistence: Option<SharedPersistence>,
) -> SharedState {
    let mut state = ServerState::new_for_test(auth_secret.to_owned(), isolation);
    state.registry = registry;
    state.persist_dir = persist_dir;
    state.persistence = persistence;
    Arc::new(RwLock::new(state))
}

pub fn state_with(
    auth_secret: &str,
    isolation: IsolationLayer,
    persist_dir: Option<String>,
    persistence: Option<SharedPersistence>,
) -> SharedState {
    state_with_registry(
        auth_secret,
        isolation,
        GraphRegistry::new(),
        persist_dir,
        persistence,
    )
}

pub fn durable_state(auth_secret: &str, isolation: IsolationLayer) -> SharedState {
    let (persist_dir, persistence) = crate::common::tempdir_persistence();
    state_with(auth_secret, isolation, persist_dir, persistence)
}

#[cfg(feature = "tsdb")]
pub fn state_with_tsdb(
    auth_secret: &str,
    isolation: IsolationLayer,
    persist_dir: Option<String>,
    persistence: Option<SharedPersistence>,
    tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>>,
) -> SharedState {
    let mut state = make_state(auth_secret, isolation, persist_dir, persistence);
    state.tsdb_store = tsdb_store;
    Arc::new(RwLock::new(state))
}

#[cfg(feature = "tsdb")]
pub fn temporary_series(label: &str) -> Arc<eg_tsdb::store::SeriesStore> {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);
    let path = std::env::temp_dir().join(format!(
        "eg-{label}-series-{}-{}.redb",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    Arc::new(eg_tsdb::store::SeriesStore::open(path).expect("open temporary series store"))
}

pub fn request(auth_secret: &str, id: u64, graph: &str, method: Method) -> Request {
    crate::common::signed_request(auth_secret, id, graph, method)
}

pub fn commons_request(auth_secret: &str, id: u64, method: Method) -> Request {
    request(auth_secret, id, "__commons__", method)
}

pub fn sql_test_persist_dir(label: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);
    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-test");
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
    std::env::temp_dir()
        .join(format!(
            "epistemic-graph-{label}-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
        .to_string_lossy()
        .into_owned()
}

#[cfg(feature = "security")]
pub fn wire_isolation(users: &[&str]) -> IsolationLayer {
    use epistemic_graph::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
    use epistemic_graph::isolation::{AgentIdentity, AgentRole};

    let mut isolation = IsolationLayer::new();
    isolation.add_role(Role::new("commons-user"));
    for action in [RbacAction::Read, RbacAction::Write] {
        isolation.add_grant(Grant {
            role: "commons-user".to_string(),
            resource: ResourceSelector::Graph("__commons__".to_string()),
            action,
            effect: GrantEffect::Allow,
        });
    }
    for user in users {
        isolation.register_agent(AgentIdentity {
            agent_id: (*user).to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: vec!["commons-user".to_string()],
        });
    }
    isolation
}

#[cfg(not(feature = "security"))]
pub fn wire_isolation(_users: &[&str]) -> IsolationLayer {
    IsolationLayer::new()
}

pub fn seeded_wire_state(
    auth_secret: &str,
    user: &str,
    persist_dir: Option<String>,
    persistence: Option<SharedPersistence>,
) -> SharedState {
    let mut state = make_state(
        auth_secret,
        wire_isolation(&[user]),
        persist_dir,
        persistence,
    );
    seed_wire_nodes(&state.registry, user);
    #[cfg(feature = "tsdb")]
    {
        state.tsdb_store = Some(temporary_series("wire"));
    }
    Arc::new(RwLock::new(state))
}

pub fn seed_wire_nodes(registry: &GraphRegistry, user: &str) {
    let core = registry.get("__commons__").unwrap().core.clone();
    for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
        let properties = serde_json::json!({
            "type": ty,
            "rank": rank,
            "_visibility": "public",
            "_owner": user,
        });
        core.add_node(
            id.to_string(),
            rmp_serde::to_vec_named(&properties).expect("encode seeded node"),
        );
    }
}

pub async fn dispatch(
    state: &SharedState,
    request: Request,
) -> epistemic_graph::protocol::Response {
    Box::pin(epistemic_graph::server::dispatch(state, request)).await
}

pub fn json_bytes(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).expect("encode JSON test value")
}

pub fn edge_properties(tag: &str) -> Vec<u8> {
    json_bytes(serde_json::json!({"tag": tag}))
}
