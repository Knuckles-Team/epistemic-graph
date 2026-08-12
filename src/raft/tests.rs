//! 3-node Raft cluster test (CONCEPT:AU-KG.ingest.source-sync-canonical).
//!
//! Spins three in-process Raft nodes on three local TCP ports, each with its own
//! redb-AUTHORITATIVE persistence dir. Verifies the full HA contract:
//!
//! 1. a leader is elected,
//! 2. a batch of writes routed through the leader's `client_write` replicates,
//! 3. the writes are READABLE from a FOLLOWER (read replica),
//! 4. KILLING the leader (shutdown Raft + stop its RPC listener) triggers a NEW
//!    leader election among the survivors,
//! 5. EVERY committed write survives on the new leader (no data loss, no
//!    split-brain — the survivors form a quorum of 2/3).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::sync::RwLock;

use super::config::RaftClusterConfig;
use super::node::{self, StartedNode};
use super::{NodeId, RaftRequest};
use crate::channels::ChannelManager;
use crate::durability::DurabilityPolicy;
use crate::isolation::IsolationLayer;
use crate::protocol::{GraphType, Method};
use crate::registry::GraphRegistry;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;

/// Build a ServerState with a redb-AUTHORITATIVE backend rooted at `dir`.
async fn make_state(dir: &str) -> Arc<RwLock<ServerState>> {
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.to_string(), DurabilityPolicy::Each, 4096).expect("open redb"),
    );
    make_state_with_backend(dir, backend).await
}

/// Build a ServerState over an ALREADY-OPEN backend (redb is single-handle-per-
/// process, so a test that needs the concrete backend handle must SHARE it with the
/// state, never open a second one over the same dir).
async fn make_state_with_backend(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
) -> Arc<RwLock<ServerState>> {
    backend
        .register_graph("__commons__", "__commons__", GraphType::Commons)
        .await
        .expect("register mandatory commons graph");
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            crate::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "raft-test".to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend),
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(64)),
        read_admission: Arc::new(tokio::sync::Semaphore::new(64)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 16,
        write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
        open_txns: Arc::new(dashmap::DashMap::new()),
        txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "streaming")]
        cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: std::sync::Arc::new(parking_lot::Mutex::new(
            crate::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
    }))
}

fn peer_map(ports: &[u16]) -> BTreeMap<NodeId, BasicNode> {
    ports
        .iter()
        .enumerate()
        .map(|(i, p)| ((i + 1) as NodeId, BasicNode::new(format!("127.0.0.1:{p}"))))
        .collect()
}

fn cluster_cfg(node_id: NodeId, ports: &[u16]) -> RaftClusterConfig {
    cluster_cfg_with_groups(node_id, ports, 1)
}

/// [`cluster_cfg`] with an explicit group count (DIST-P2-2 multi-group startup test).
fn cluster_cfg_with_groups(node_id: NodeId, ports: &[u16], groups: u64) -> RaftClusterConfig {
    let peers = peer_map(ports);
    let bind_addr = peers.get(&node_id).unwrap().addr.clone();
    RaftClusterConfig {
        node_id,
        peers: peers.clone(),
        bind_addr,
        // ADR-1 / W1.1: a distinct per-node advertised client address so
        // `ClusterMembers`/`PlacementRoute.endpoints` are exercisable against
        // this real (loopback) multi-node cluster.
        advertised_client_addr: format!("tcp://127.0.0.1:{}", 30_000 + node_id),
        advertised_tls_server_name: None,
        is_bootstrap: peers.keys().next() == Some(&node_id),
        groups,
        transport_secret: Some(
            super::config::RaftTransportSecret::from_material(&[0x5a; 32]).unwrap(),
        ),
    }
}

/// Pick three currently-free localhost ports by binding then dropping.
fn free_ports(n: usize) -> Vec<u16> {
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..n {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        ports.push(l.local_addr().unwrap().port());
        listeners.push(l);
    }
    // Listeners drop here, freeing the ports for the Raft listeners to claim.
    ports
}

/// A read-the-graph helper: total node count in the named graph on a node's state.
async fn node_count(state: &Arc<RwLock<ServerState>>, graph: &str) -> usize {
    let s = state.read().await;
    s.registry
        .get(graph)
        .map(|e| e.core.node_count())
        .unwrap_or(0)
}

/// BUG-044-class: keep the full (`--features full`) dispatcher's state machine
/// behind one heap indirection. `dispatch()` bottoms out in `dispatch_inner`
/// (`src/server/dispatch.rs`), one very large async fn whose generated future is
/// enormous; awaiting it inline inside a `#[tokio::test]`'s default 2 MiB worker
/// stack can exhaust it before the first request is even polled, SIGABRTing the
/// whole test binary. Mirrors `server::mod::tests::dispatch_on_heap` (8e00e0b).
/// (`placement_admin_wire_rpc`'s tests are exempt: they run on their own
/// explicitly-sized `ENGINE_WORKER_STACK_BYTES` runtime instead, see its module doc.)
fn dispatch_on_heap<'a>(
    state: &'a Arc<RwLock<ServerState>>,
    request: crate::protocol::Request,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::protocol::Response> + Send + 'a>> {
    Box::pin(crate::server::dispatch(state, request))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_replicates_and_survives_leader_failover() {
    let tmp = std::env::temp_dir().join(format!("eg-raft-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let dirs: Vec<String> = (1..=3)
        .map(|i| {
            let d = tmp.join(format!("node{i}"));
            std::fs::create_dir_all(&d).unwrap();
            d.to_string_lossy().to_string()
        })
        .collect();
    let ports = free_ports(3);

    // ── Start three nodes ───────────────────────────────────────────────
    let mut states = Vec::new();
    let mut nodes: BTreeMap<NodeId, StartedNode> = BTreeMap::new();
    for i in 1..=3u64 {
        let state = make_state(&dirs[(i - 1) as usize]).await;
        let started = node::start(cluster_cfg(i, &ports), state.clone())
            .await
            .expect("start raft node");
        state.write().await.raft = Some(started.handle.clone());
        states.push(state);
        nodes.insert(i, started);
    }

    // ── 1. A leader is elected ──────────────────────────────────────────
    // Bootstrap node (id 1) initializes; wait until SOME node reports a leader.
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(15))
        .await
        .expect("a leader must be elected");
    assert!((1..=3).contains(&leader_id));

    // ── 2. Replicate a batch of writes through the leader ───────────────
    let graph = "__commons__";
    let n_writes = 25u64;
    let leader = nodes.get(&leader_id).unwrap().handle.clone();
    for k in 0..n_writes {
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(graph),
            graph_name: graph.to_string(),
            graph_type: GraphType::Commons,
            committed_at_ms: 0,
            mutation: super::RaftMutationContext::internal(
                "raft-test",
                graph,
                &format!("cluster-write-{k}"),
                k,
                0,
            ),
            command: super::ReplicatedMutation::graph(
                Method::AddNode {
                    node_id: format!("n{k}"),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"k": k}))
                        .unwrap(),
                },
                "raft-test",
            )
            .unwrap(),
        };
        leader
            .client_write(req)
            .await
            .unwrap_or_else(|e| panic!("write {k} via leader failed: {e}"));
    }

    // ── 3. Read the writes back from a FOLLOWER (read replica) ──────────
    let follower_id = (1..=3u64).find(|&i| i != leader_id).unwrap();
    let follower_state = states[(follower_id - 1) as usize].clone();
    wait_until(Duration::from_secs(10), || {
        let st = follower_state.clone();
        async move { node_count(&st, graph).await as u64 == n_writes }
    })
    .await
    .expect("all writes must replicate to the follower");
    assert_eq!(node_count(&follower_state, graph).await as u64, n_writes);

    // ── 4. KILL the leader: shut down its Raft + stop its RPC listener ──
    let killed = nodes.remove(&leader_id).unwrap();
    killed.multi.stop_listener();
    let _ = killed.handle.raft.shutdown().await;

    // ── 5. A NEW leader is elected among the survivors, with NO data loss ─
    let new_leader = wait_for_leader_excluding(&nodes, leader_id, Duration::from_secs(20))
        .await
        .expect("a new leader must be elected after the old one is killed");
    assert_ne!(new_leader, leader_id, "must not be the dead leader");

    // All committed writes survive on the new leader's state machine.
    let new_leader_state = states[(new_leader - 1) as usize].clone();
    assert_eq!(
        node_count(&new_leader_state, graph).await as u64,
        n_writes,
        "every committed write must survive the failover (no data loss)"
    );

    // And a write through the NEW leader still commits (cluster is live, not split).
    let post = RaftRequest {
        graph_fname: crate::persist::sanitize(graph),
        graph_name: graph.to_string(),
        graph_type: GraphType::Commons,
        committed_at_ms: 0,
        mutation: super::RaftMutationContext::internal(
            "raft-test",
            graph,
            "after-failover",
            n_writes,
            0,
        ),
        command: super::ReplicatedMutation::graph(
            Method::AddNode {
                node_id: "after-failover".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"post": true}))
                    .unwrap(),
            },
            "raft-test",
        )
        .unwrap(),
    };
    nodes
        .get(&new_leader)
        .unwrap()
        .handle
        .client_write(post)
        .await
        .expect("the new leader must accept writes (cluster is live)");
    assert_eq!(
        node_count(&new_leader_state, graph).await as u64,
        n_writes + 1
    );

    // ── Cleanup ─────────────────────────────────────────────────────────
    for (_, n) in nodes {
        n.multi.stop_listener();
        let _ = n.handle.raft.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&tmp);
}

/// ADR-1 / W1.1 (CONCEPT:EG-KG.sharding.cluster-topology, `reports/wave1/ADR-scale-trio.md` §ADR-1) —
/// the dynamic client-topology-discovery counterpart to
/// [`three_node_cluster_replicates_and_survives_leader_failover`] above, proven
/// through the REAL served `dispatch()` entrypoint (like
/// `wire_raft_add_learner_and_change_membership_resolve_through_dispatch`), not
/// just the in-process `MultiRaft` API.
///
/// Proves, in order: (a) `Method::ClusterMembers`, answered from a FOLLOWER (not
/// just the leader, unlike `PlacementRoute`), returns the correct topology --
/// one group, all three self-reported members, correct roles, and each
/// member's `client_endpoint` matching its configured
/// `EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR`; (b) the `cluster:topology-read`
/// grant genuinely works -- an envelope asserting ONLY that scope (no
/// `admin:*`, no `*`) succeeds, while an unrelated scope is denied, proving
/// this is NOT gated `admin:cluster-read`; (c) `PlacementRoute.endpoints`
/// echoes the same leader-first member list; (d) after killing the leader and
/// a new election, `ClusterMembers` queried from a SURVIVOR reflects the NEW
/// leader -- the engine-side half of "kill leader -> client re-routes with
/// zero config edits" (the client-side half is proven in AU's
/// `graph_compute.py`/`placement_catalog.py` reconnect tests).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_members_reports_topology_and_tracks_leader_failover() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::protocol::{Request, ResultPayload};
    use crate::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};

    const TEST_AGENT: &str = "cluster-members-wire-test-agent";
    const SECRET: &str = "raft-test"; // matches make_state's ServerState.auth_secret
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
    std::env::set_var(
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
        std::env::temp_dir().join(format!(
            "eg-cluster-members-wire-auth-{}",
            std::process::id()
        )),
    );

    fn signed_request(id: u64, scopes: Vec<String>, method: Method) -> Request {
        let context = RequestContextClaims {
            principal: TEST_AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: TEST_AGENT.to_string(),
            roles: Vec::new(),
            scopes,
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "cluster-members-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("cluster-members-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    let root = std::env::temp_dir().join(format!("eg-wire-cluster-members-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let dirs: Vec<String> = (1..=3)
        .map(|i| {
            let d = root.join(format!("node{i}"));
            std::fs::create_dir_all(&d).unwrap();
            d.to_string_lossy().to_string()
        })
        .collect();
    let ports = free_ports(3);
    let gid = super::DEFAULT_GROUP;

    // ── Start three nodes through the REAL production path (`node::start`),
    // so ADR-1's self-report (`raft::node::start` -> `MultiRaft::commit_node_info`)
    // runs exactly as it would in a real deployment. ──────────────────────
    let mut states = Vec::new();
    let mut nodes: BTreeMap<NodeId, StartedNode> = BTreeMap::new();
    for i in 1..=3u64 {
        let state = make_state(&dirs[(i - 1) as usize]).await;
        state.write().await.isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let started = node::start(cluster_cfg(i, &ports), state.clone())
            .await
            .expect("start raft node");
        state.write().await.raft = Some(started.handle.clone());
        state.write().await.multi_raft = Some(started.multi.clone());
        states.push(state);
        nodes.insert(i, started);
    }

    let leader_id = wait_for_leader(&nodes, Duration::from_secs(15))
        .await
        .expect("a leader must be elected");
    let follower_id = (1..=3u64).find(|&i| i != leader_id).unwrap();
    let follower_state = states[(follower_id - 1) as usize].clone();

    let expected_endpoint = |id: NodeId| format!("tcp://127.0.0.1:{}", 30_000 + id);

    // ── (a) ClusterMembers from a FOLLOWER reflects all 3 self-reported
    // members once the background self-report tasks have converged. ──────
    let mut req_id = 100u64;
    wait_until(Duration::from_secs(20), || {
        req_id += 1;
        let follower_state = follower_state.clone();
        let request = signed_request(
            req_id,
            vec!["cluster:topology-read".to_string()],
            Method::ClusterMembers,
        );
        async move {
            let resp = dispatch_on_heap(&follower_state, request).await;
            matches!(
                resp.result,
                Some(ResultPayload::Json(serde_json::Value::Object(ref map)))
                    if map.get("groups")
                        .and_then(|g| g.as_array())
                        .and_then(|groups| groups.first())
                        .and_then(|g| g.get("members"))
                        .and_then(|m| m.as_array())
                        .is_some_and(|members| members.len() == 3)
            )
        }
    })
    .await
    .expect("ClusterMembers must report all 3 self-reported members from a follower");

    req_id += 1;
    let resp = dispatch_on_heap(
        &follower_state,
        signed_request(
            req_id,
            vec!["cluster:topology-read".to_string()],
            Method::ClusterMembers,
        ),
    )
    .await;
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    let Some(ResultPayload::Json(value)) = resp.result else {
        panic!(
            "expected a JSON ClusterMembers result, got {:?}",
            resp.result
        );
    };
    let groups = value["groups"].as_array().expect("groups array");
    assert_eq!(groups.len(), 1, "single-group deployment -> one group");
    assert_eq!(groups[0]["group_id"].as_u64(), Some(gid));
    let members = groups[0]["members"].as_array().expect("members array");
    assert_eq!(members.len(), 3);
    let mut seen_leaders = 0;
    for member in members {
        let node_id = member["node_id"].as_u64().expect("node_id");
        let role = member["role"].as_str().expect("role");
        let endpoint = member["client_endpoint"].as_str().expect("client_endpoint");
        assert_eq!(endpoint, expected_endpoint(node_id));
        if node_id == leader_id {
            assert_eq!(role, "leader");
            seen_leaders += 1;
        } else {
            assert_eq!(role, "follower");
        }
    }
    assert_eq!(
        seen_leaders, 1,
        "exactly one member must be reported leader"
    );

    // ── (b) The `cluster:topology-read` grant genuinely works: an envelope
    // asserting ONLY that scope succeeds; an unrelated scope is denied --
    // proving this is NOT gated behind `admin:cluster-read`. ─────────────
    req_id += 1;
    let denied = dispatch_on_heap(
        &follower_state,
        signed_request(
            req_id,
            vec!["irrelevant:scope".to_string()],
            Method::ClusterMembers,
        ),
    )
    .await;
    assert!(
        denied
            .error
            .as_deref()
            .is_some_and(|e| e.contains("ACCESS_DENIED")),
        "an unrelated scope must be denied, got {:?}",
        denied.error
    );

    // ── (c) PlacementRoute.endpoints echoes the SAME leader-first list.
    // Unlike ClusterMembers, PlacementRoute remains LEADER-ONLY (pre-existing,
    // unchanged by ADR-1 -- a follower answers OPERATION_REDIRECTED), so this
    // leg is issued against the leader's own state. ──────────────────────
    let leader_state = states[(leader_id - 1) as usize].clone();
    req_id += 1;
    let route_resp = dispatch_on_heap(
        &leader_state,
        signed_request(
            req_id,
            vec!["*".to_string()],
            Method::PlacementRoute {
                request: crate::epistemic_operations::PlacementRouteRequest {
                    schema_version:
                        crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                    tenant_ref: "adr1-endpoints-check".to_string(),
                    partition_ref: "adr1-endpoints-check".to_string(),
                    client_epoch: 0,
                },
            },
        ),
    )
    .await;
    assert!(
        route_resp.error.is_none(),
        "route error: {:?}",
        route_resp.error
    );
    let Some(ResultPayload::Raw(bytes)) = route_resp.result else {
        panic!(
            "expected a raw PlacementRoute result, got {:?}",
            route_resp.result
        );
    };
    let route: serde_json::Value = rmp_serde::from_slice(&bytes).expect("decode PlacementRoute");
    let endpoints = route["endpoints"].as_array().expect("endpoints array");
    assert!(
        !endpoints.is_empty(),
        "endpoints must be non-empty once nodes have self-reported"
    );
    assert_eq!(
        endpoints[0].as_str(),
        Some(expected_endpoint(leader_id).as_str()),
        "leader-first ordering"
    );

    // ── (d) Kill the leader; a new one is elected; ClusterMembers queried
    // from a SURVIVOR reflects the NEW leader. ────────────────────────────
    let killed = nodes.remove(&leader_id).unwrap();
    killed.multi.stop_listener();
    let _ = killed.handle.raft.shutdown().await;

    let new_leader = wait_for_leader_excluding(&nodes, leader_id, Duration::from_secs(20))
        .await
        .expect("a new leader must be elected after the old one is killed");
    assert_ne!(new_leader, leader_id);

    let survivor_id = (1..=3u64)
        .find(|&i| i != leader_id && i != new_leader)
        .unwrap();
    let survivor_state = states[(survivor_id - 1) as usize].clone();

    req_id += 1;
    wait_until(Duration::from_secs(20), || {
        req_id += 1;
        let survivor_state = survivor_state.clone();
        let request = signed_request(
            req_id,
            vec!["cluster:topology-read".to_string()],
            Method::ClusterMembers,
        );
        async move {
            let resp = dispatch_on_heap(&survivor_state, request).await;
            let Some(ResultPayload::Json(value)) = resp.result else {
                return false;
            };
            value["groups"][0]["members"]
                .as_array()
                .is_some_and(|members| {
                    members.iter().any(|m| {
                        m["node_id"].as_u64() == Some(new_leader)
                            && m["role"].as_str() == Some("leader")
                    })
                })
        }
    })
    .await
    .expect("ClusterMembers must reflect the NEW leader after failover, from a survivor");

    // ── Cleanup ─────────────────────────────────────────────────────────
    for (_, n) in nodes {
        n.multi.stop_listener();
        let _ = n.handle.raft.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// Wait until any node reports a current leader; returns its id.
async fn wait_for_leader(
    nodes: &BTreeMap<NodeId, StartedNode>,
    timeout: Duration,
) -> Option<NodeId> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for n in nodes.values() {
            if let Some(l) = n.handle.raft.current_leader().await {
                return Some(l);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// Wait until a leader OTHER than `excluded` is reported by a surviving node.
async fn wait_for_leader_excluding(
    nodes: &BTreeMap<NodeId, StartedNode>,
    excluded: NodeId,
    timeout: Duration,
) -> Option<NodeId> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for n in nodes.values() {
            if let Some(l) = n.handle.raft.current_leader().await {
                if l != excluded {
                    return Some(l);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// Poll an async predicate until it is true or the timeout elapses.
async fn wait_until<F, Fut>(timeout: Duration, mut pred: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if pred().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    Err(())
}

// ─────────────────────────────────────────────────────────────────────────
// DIST-P2-2: multi-group PRODUCTION startup — `node::start` stands up N groups
// from config (not just a test harness's manual `create_group` calls), and a
// default (unconfigured) deploy stays single-group, byte-for-byte.
// ─────────────────────────────────────────────────────────────────────────

/// `cfg.groups > 1` (DIST-P2-2, `EPISTEMIC_GRAPH_RAFT_GROUPS`) makes PRODUCTION
/// `node::start` stand up every group `0..groups` on this node and configure the
/// tenant-range ring across all of them — the same [`super::multi::MultiRaft`]
/// machinery the placement/xshard test harnesses already exercise, now reachable from
/// the real boot path instead of only from a test's manual `create_group` calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_group_startup_creates_n_groups_from_config() {
    let dir = fresh_dir("multigroup-startup");
    let state = make_state(&dir).await;
    let ports = free_ports(1);
    let cfg = cluster_cfg_with_groups(1, &ports, 4);

    let started = node::start(cfg, state.clone())
        .await
        .expect("start raft node with 4 groups");

    // The tenant-range ring now spans all 4 groups, and each is actually running.
    assert_eq!(started.multi.router().group_ring(), vec![0, 1, 2, 3]);
    for gid in 0..4u64 {
        assert!(
            started.multi.group(gid).await.is_some(),
            "group {gid} must be running on this node after multi-group startup"
        );
    }

    started.multi.stop_listener();
    let _ = started.handle.raft.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A graph with no explicit [`super::placement::PlacementCatalog`] entry hash-spreads
/// across the configured ring (DIST-P2-2) — deterministically, so the SAME graph name
/// always lands on the SAME group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_routes_to_its_ring_assigned_group_after_multi_group_startup() {
    let dir = fresh_dir("multigroup-routing");
    let state = make_state(&dir).await;
    let ports = free_ports(1);
    let cfg = cluster_cfg_with_groups(1, &ports, 3);

    let started = node::start(cfg, state.clone())
        .await
        .expect("start raft node with 3 groups");

    let route = started.multi.route_graph("acme:ws1").await;
    assert!(
        (0..3).contains(&route.group),
        "graph must route into the ring"
    );
    assert_eq!(route.epoch, 0, "an unplaced route has epoch 0");
    assert!(!route.placed);
    // Deterministic: routing the same graph again resolves to the SAME group.
    let route_again = started.multi.route_graph("acme:ws1").await;
    assert_eq!(route.group, route_again.group);

    started.multi.stop_listener();
    let _ = started.handle.raft.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The DEFAULT (unconfigured, `groups <= 1`) production boot is BYTE-FOR-BYTE the
/// pre-existing single-group path: no extra group, an empty ring, every graph on
/// [`super::DEFAULT_GROUP`] — the guardrail this whole feature must not regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_startup_stays_single_group_unchanged() {
    let dir = fresh_dir("multigroup-default");
    let state = make_state(&dir).await;
    let ports = free_ports(1);
    let cfg = cluster_cfg(1, &ports); // groups: 1 — the unconfigured default.

    let started = node::start(cfg, state.clone())
        .await
        .expect("start raft node with the default (single-group) config");

    assert!(
        started.multi.router().group_ring().is_empty(),
        "no ring configured by default"
    );
    assert!(started.multi.group(super::DEFAULT_GROUP).await.is_some());
    assert!(
        started.multi.group(1).await.is_none(),
        "no extra group should exist without EPISTEMIC_GRAPH_RAFT_GROUPS"
    );
    let route = started.multi.route_graph("any-tenant:ws1").await;
    assert_eq!(
        route.group,
        super::DEFAULT_GROUP,
        "every unplaced graph must be authoritatively routed to the default group"
    );
    assert_eq!(route.epoch, 0);

    started.multi.stop_listener();
    let _ = started.handle.raft.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// DIST-P2-5: the placement-catalog ADMIN wire RPC (`Method::PlacementAdmin`,
// ops `assign`/`move`/`abort_move`), proven end-to-end against a REAL
// three-node cluster (three independent tokio-spawned nodes on three
// independent TCP ports + persist dirs, real openraft consensus — the SAME
// topology `three_node_cluster_replicates_and_survives_leader_failover`
// above proves HA with) rather than the single-node/two-group simplification
// the `placement_harness` (harness-feature-gated) suite uses. This is the
// external-caller seam: before this `Method` variant existed, the
// `PlacementCatalog`/`TenantManager` admin machinery was reachable ONLY from
// in-process Rust, even on a real multi-node cluster — there was no wire RPC
// to trigger a placement decision or an online move from outside the engine.
//
// Proves, through the REAL served `dispatch()` entrypoint (signed `eg2.`
// envelopes, not a raw `client_write`), driven from a DIFFERENT physical node
// at each step to rule out any same-process shortcut:
//   1. The `assign` op (the placement DECISION) lands and is visible from
//      every node.
//   2. Data written after the decision replicates to a genuinely different
//      node and is read back over the wire (`GetNodeProperties`).
//   3. The `move` op (PLAN -> EXECUTE -> CATALOG UPDATE) relocates the
//      tenant's partition to the other group; the SAME data is still present
//      and wire-readable from yet another node after the move — proving
//      placement, not merely a function returning `Ok`.
//   4. `PlacementRoute`, queried from a third node with the PRE-move epoch,
//      is flagged stale and redirected to the new group/epoch — the fenced
//      cutover is cluster-wide, not node-local.
//   5. A post-move write lands and is readable, proving the new owner truly
//      serves the partition going forward.
// ─────────────────────────────────────────────────────────────────────────

mod placement_admin_wire_rpc {
    use super::*;
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::protocol::{Response, ResultPayload};
    use crate::server::{compute_verified_envelope_token, dispatch, VerifiedEnvelopeParams};

    const TEST_AGENT: &str = "placement-admin-wire-test-agent";
    const SECRET: &str = "raft-test"; // matches `make_state`'s `auth_secret`.
    const TENANT: &str = "acme";
    static NONCE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    async fn register_admin_agent(state: &Arc<RwLock<ServerState>>) {
        // Called right after `make_state`, before any node is started, so
        // every node's isolation layer trusts the SAME test identity.
        state.write().await.isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
    }

    fn signed_request(id: u64, method: Method) -> crate::protocol::Request {
        signed_request_for_graph(id, "__commons__", method)
    }

    /// Like [`signed_request`], but signs the envelope over the REAL target
    /// `graph` from the start. The envelope token covers the whole `Request`
    /// (including `graph`), so setting `req.graph` AFTER signing invalidates the
    /// signature ("Authentication failed") — this is the one constructor path
    /// for a non-`__commons__` request.
    fn signed_request_for_graph(id: u64, graph: &str, method: Method) -> crate::protocol::Request {
        std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
        std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
        std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
        std::env::set_var(
            "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
            std::env::temp_dir().join(format!(
                "epistemic-graph-placement-wire-auth-{}",
                std::process::id()
            )),
        );
        let context = RequestContextClaims {
            principal: TEST_AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: TEST_AGENT.to_string(),
            roles: Vec::new(),
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = crate::protocol::Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let issued_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "placement-wire-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("placement-wire-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    async fn add_node_via(
        state: &Arc<RwLock<ServerState>>,
        req_id: u64,
        graph: &str,
        node_id: &str,
    ) -> Response {
        let req = signed_request_for_graph(
            req_id,
            graph,
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"id": node_id}))
                    .unwrap(),
            },
        );
        dispatch(state, req).await
    }

    async fn read_node_via(
        state: &Arc<RwLock<ServerState>>,
        req_id: u64,
        graph: &str,
        node_id: &str,
    ) -> Option<serde_json::Value> {
        let req = signed_request_for_graph(
            req_id,
            graph,
            Method::GetNodeProperties {
                node_id: node_id.to_string(),
            },
        );
        let resp = dispatch(state, req).await;
        assert!(
            resp.error.is_none(),
            "GetNodeProperties failed: {:?}",
            resp.error
        );
        match resp.result {
            Some(ResultPayload::PropertiesMsgpack(bytes)) => {
                Some(rmp_serde::from_slice(&bytes).expect("typed node properties"))
            }
            _ => None,
        }
    }

    async fn wait_for_group_leader(
        nodes: &BTreeMap<NodeId, StartedNode>,
        gid: super::super::GroupId,
        timeout: Duration,
    ) -> Option<NodeId> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            for n in nodes.values() {
                if let Some(group) = n.multi.group(gid).await {
                    if let Some(l) = group.current_leader().await {
                        return Some(l);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        None
    }

    /// A plain `#[test]` (not `#[tokio::test]`) driving its own runtime on an
    /// explicitly sized worker stack via `crate::server::spawn_engine_driver` +
    /// `ENGINE_WORKER_STACK_BYTES` — the SAME established pattern
    /// `tests/external_compute_e2e.rs` uses for any test that calls the real served
    /// `dispatch()` entrypoint. `dispatch()`/`dispatch_graph_op_inner` are themselves
    /// enormous async state machines (matching over the whole `Method` enum), and
    /// three concurrent two-group Raft nodes plus that call depth overflows Tokio's
    /// 2 MiB default test-worker stack; `#[tokio::test]`'s default runtime does not
    /// apply the engine's own `thread_stack_size`, so this test builds its own.
    #[test]
    fn placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster() {
        let driver = crate::server::spawn_engine_driver(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(crate::server::ENGINE_WORKER_STACK_BYTES)
                .enable_all()
                .build()
                .expect("build engine-contract runtime");
            runtime.block_on(
                placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster_body(),
            );
        });
        driver
            .expect("spawn the engine test driver thread")
            .join()
            .expect("engine driver thread must not panic");
    }

    async fn placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster_body() {
        use super::super::DEFAULT_GROUP;
        const TARGET_GROUP: super::super::GroupId = 1;

        let tmp =
            std::env::temp_dir().join(format!("eg-placement-wire-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let dirs: Vec<String> = (1..=3)
            .map(|i| {
                let d = tmp.join(format!("node{i}"));
                std::fs::create_dir_all(&d).unwrap();
                d.to_string_lossy().to_string()
            })
            .collect();
        let ports = free_ports(3);

        // ── Start three REAL, independent nodes with a 2-group ring ─────────
        let mut states: Vec<Arc<RwLock<ServerState>>> = Vec::new();
        let mut nodes: BTreeMap<NodeId, StartedNode> = BTreeMap::new();
        for i in 1..=3u64 {
            let state = make_state(&dirs[(i - 1) as usize]).await;
            register_admin_agent(&state).await;
            let started = node::start(cluster_cfg_with_groups(i, &ports, 2), state.clone())
                .await
                .expect("start raft node");
            {
                let mut s = state.write().await;
                s.raft = Some(started.handle.clone());
                s.multi_raft = Some(started.multi.clone());
            }
            states.push(state);
            nodes.insert(i, started);
        }
        let node_state = |id: NodeId| states[(id - 1) as usize].clone();

        wait_for_group_leader(&nodes, DEFAULT_GROUP, Duration::from_secs(15))
            .await
            .expect("the DEFAULT (placement-control) group must elect a leader");
        wait_for_group_leader(&nodes, TARGET_GROUP, Duration::from_secs(15))
            .await
            .expect("the target group must elect a leader");

        // ── 1. The `assign` op (the DECISION leg), dispatched via the wire ──
        let control_leader = wait_for_group_leader(&nodes, DEFAULT_GROUP, Duration::from_secs(5))
            .await
            .unwrap();
        let control_state = node_state(control_leader);
        let assign_resp = dispatch(
            &control_state,
            signed_request(
                1,
                Method::PlacementAdmin {
                    op: crate::protocol::PlacementAdminOp::Assign {
                        tenant: TENANT.to_string(),
                        group: DEFAULT_GROUP,
                    },
                },
            ),
        )
        .await;
        assert!(
            assign_resp.error.is_none(),
            "PlacementAdmin assign failed: {:?}",
            assign_resp.error
        );
        let epoch0 = match assign_resp.result {
            Some(ResultPayload::Json(v)) => v["epoch"].as_u64().expect("epoch in response"),
            other => panic!("expected a JSON epoch payload, got {other:?}"),
        };
        assert_eq!(epoch0, 1, "the first placement decision is epoch 1");

        // ── 2. Write data, then read it back from a DIFFERENT physical node ─
        let graph = format!("{TENANT}:ws1");
        let create_resp = dispatch(
            &control_state,
            signed_request(
                5,
                Method::CreateGraph {
                    graph_name: graph.clone(),
                    graph_type: GraphType::Global,
                },
            ),
        )
        .await;
        assert!(
            create_resp.error.is_none(),
            "CreateGraph failed: {:?}",
            create_resp.error
        );
        let n_nodes = 6usize;
        for k in 0..n_nodes {
            let resp = add_node_via(&control_state, 10 + k as u64, &graph, &format!("m{k}")).await;
            assert!(
                resp.error.is_none(),
                "AddNode m{k} failed: {:?}",
                resp.error
            );
        }
        // Pick a node that is NOT the one we wrote through, to prove real
        // cross-node replication (not a same-process shortcut).
        let reader_id = (1..=3u64).find(|&i| i != control_leader).unwrap();
        let reader_state = node_state(reader_id);
        wait_until(Duration::from_secs(10), || {
            let state = reader_state.clone();
            let graph = graph.clone();
            async move { node_count(&state, &graph).await == n_nodes }
        })
        .await
        .expect("all 6 pre-move nodes must replicate to a follower node");
        for k in 0..n_nodes {
            let val = read_node_via(&reader_state, 100 + k as u64, &graph, &format!("m{k}"))
                .await
                .unwrap_or_else(|| panic!("m{k} must be wire-readable from node {reader_id}"));
            assert_eq!(val["id"], serde_json::json!(format!("m{k}")));
        }

        // ── 3. The `move` op (PLAN -> EXECUTE -> CATALOG UPDATE) over the wire ──
        let move_resp = dispatch(
            &control_state,
            signed_request(
                2,
                Method::PlacementAdmin {
                    op: crate::protocol::PlacementAdminOp::Move {
                        tenant: TENANT.to_string(),
                        range_start: 0,
                        range_end: u64::MAX,
                        target: TARGET_GROUP,
                    },
                },
            ),
        )
        .await;
        assert!(
            move_resp.error.is_none(),
            "PlacementAdmin move failed: {:?}",
            move_resp.error
        );
        let (moved_epoch, moved_nodes_transferred) = match move_resp.result {
            Some(ResultPayload::Json(v)) => (
                v["epoch"].as_u64().expect("epoch in move report"),
                v["graphs"][0]["nodes_transferred"]
                    .as_u64()
                    .expect("nodes_transferred in move report"),
            ),
            other => panic!("expected a JSON PlacementMoveReport payload, got {other:?}"),
        };
        assert!(
            moved_epoch > epoch0,
            "the fenced cutover strictly bumps the epoch"
        );
        assert_eq!(
            moved_nodes_transferred, n_nodes as u64,
            "the move report must account for every pre-move node"
        );

        // ── 4a. PlacementRoute is a placement-CONTROL-group-leader-only read
        //      (`handlers::placement::handle_route`, unchanged by this seam's
        //      work): a THIRD, non-leader node must redirect, not answer locally
        //      — proving the "only the leader answers" invariant is enforced
        //      cluster-wide, not merely present. ──
        let route_checker_id = (1..=3u64)
            .find(|&i| i != control_leader && i != reader_id)
            .unwrap();
        let placement_route_request = |client_epoch: u64| Method::PlacementRoute {
            request: crate::epistemic_operations::PlacementRouteRequest {
                schema_version: crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                tenant_ref: TENANT.to_string(),
                partition_ref: "ws1".to_string(),
                client_epoch,
            },
        };
        let non_leader_resp = dispatch(
            &node_state(route_checker_id),
            signed_request(3, placement_route_request(epoch0)),
        )
        .await;
        assert_eq!(
            non_leader_resp.error.as_deref(),
            Some("OPERATION_REDIRECTED"),
            "a non-leader node must redirect a PlacementRoute query, not answer it locally"
        );
        let redirect: crate::epistemic_operations::OperationResult = match non_leader_resp.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected a structured redirect, got {other:?}"),
        };
        assert!(
            redirect.redirect.is_some(),
            "the redirect must carry a target for the caller to retry against"
        );

        // ── 4b. PlacementRoute from the ACTUAL placement-control leader,
        //      presenting the PRE-move epoch, must be flagged stale and
        //      redirected to the new group. ──
        let route_resp = dispatch(
            &control_state,
            signed_request(4, placement_route_request(epoch0)),
        )
        .await;
        assert!(
            route_resp.error.is_none(),
            "PlacementRoute failed: {:?}",
            route_resp.error
        );
        // A-W1.2-2: the route response is the ADR-1 wire superset (extra
        // `endpoints` key); the canonical deny_unknown_fields DTO rejects it,
        // so the wire type is the ONLY correct reader for a route response.
        let route: crate::server::handlers::placement::PlacementRouteWire = match route_resp.result
        {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected a typed PlacementRouteWire, got {other:?}"),
        };
        assert!(route.placed);
        assert_eq!(
            route.group, TARGET_GROUP,
            "the cutover is visible cluster-wide"
        );
        assert_eq!(route.epoch, moved_epoch);
        assert!(
            route.stale,
            "a caller on the pre-move epoch must be redirected"
        );

        // ── 5. Post-move: the SAME data is still wire-readable, and a NEW
        //      write through the target group's leader lands. Data is placed
        //      AND readable after the reshard — not merely `Ok`. ──
        let post_move_leader = wait_for_group_leader(&nodes, TARGET_GROUP, Duration::from_secs(10))
            .await
            .expect("the target group must have a leader after cutover");
        let post_move_state = node_state(post_move_leader);
        for k in 0..n_nodes {
            let val = read_node_via(&post_move_state, 200 + k as u64, &graph, &format!("m{k}"))
                .await
                .unwrap_or_else(|| panic!("m{k} must survive the move and be wire-readable"));
            assert_eq!(val["id"], serde_json::json!(format!("m{k}")));
        }
        let post_resp = add_node_via(&post_move_state, 300, &graph, "post-move").await;
        assert!(
            post_resp.error.is_none(),
            "post-move AddNode failed: {:?}",
            post_resp.error
        );
        // Read the post-move write back from yet another node to prove it
        // replicated on the NEW owning group, not merely landed locally.
        let final_reader = node_state(reader_id);
        wait_until(Duration::from_secs(10), || {
            let state = final_reader.clone();
            let graph = graph.clone();
            async move { node_count(&state, &graph).await == n_nodes + 1 }
        })
        .await
        .expect("the post-move write must replicate under the NEW owning group");
        let val = read_node_via(&final_reader, 301, &graph, "post-move")
            .await
            .expect("post-move node must be wire-readable from a third node");
        assert_eq!(val["id"], serde_json::json!("post-move"));

        // ── Cleanup ───────────────────────────────────────────────────────
        for (_, n) in nodes {
            n.multi.stop_listener();
            let _ = n.handle.raft.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(&tmp);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// KG-2.204: durable redb Raft log — replay after restart + fault injection
// ─────────────────────────────────────────────────────────────────────────

use super::store::EgStore;
use super::AppCtx;
// openraft 0.10 v2 split-storage traits (CONCEPT:AU-KG.backend.authority-has-already-acked): the combined `RaftStorage`
// is gone — log ops live on `RaftLogStorage` + its super-trait `RaftLogReader`, and
// `append` signals durability through an `IOFlushed` callback.
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::type_config::alias::EntryOf;

/// A fresh temp dir for one test, removed first.
fn fresh_dir(tag: &str) -> String {
    let d = std::env::temp_dir().join(format!("eg-raft-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d.to_string_lossy().to_string()
}

/// Encode a Normal log `Entry` for group `gid` at `index`/`term` carrying an AddNode.
///
/// openraft 0.10: `Entry` is no longer `Entry<C>` — we build it via the generic
/// `RaftEntry::new_normal` over the concrete `EntryOf<TypeConfig>`, with the advanced
/// committed-leader id (`LeaderId { term, node_id }`) the default type config uses.
fn make_log_entry(index: u64, term: u64, node_id: &str) -> EntryOf<super::TypeConfig> {
    use openraft::impls::leader_id_adv::LeaderId;
    use openraft::LogId;
    let log_id = LogId::new(
        LeaderId {
            term,
            node_id: 1u64,
        },
        index,
    );
    EntryOf::<super::TypeConfig>::new_normal(
        log_id,
        RaftRequest {
            graph_fname: crate::persist::sanitize("__commons__"),
            graph_name: "__commons__".to_string(),
            graph_type: GraphType::Commons,
            committed_at_ms: 0,
            mutation: super::RaftMutationContext::internal(
                "raft-log-test",
                "__commons__",
                &format!("{term}:{index}:{node_id}"),
                index,
                0,
            ),
            command: super::ReplicatedMutation::graph(
                Method::AddNode {
                    node_id: node_id.to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(
                        &serde_json::json!({"id": node_id}),
                    )
                    .unwrap(),
                },
                "raft-test",
            )
            .unwrap(),
        },
    )
}

/// KG-2.204: append log entries → DROP the store → RE-OPEN over the SAME redb →
/// the log replays from disk with NO leader/network (the in-memory log could not).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_log_replays_from_redb_after_restart() {
    let dir = fresh_dir("logreplay");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx {
        state,
        router: None,
    };

    // ── Append 12 entries through the real RaftStorage::append_to_log path ──
    {
        let mut store = EgStore::open(super::DEFAULT_GROUP, backend.clone(), ctx.clone()).unwrap();
        let entries: Vec<_> = (1..=12)
            .map(|i| make_log_entry(i, 1, &format!("n{i}")))
            .collect();
        store
            .append(entries, IOFlushed::noop())
            .await
            .expect("append");
        // Drop the store (simulate process exit). The redb backend stays open here,
        // but the EgStore's in-RAM log state (if any) is gone — the next open must
        // recover the tail PURELY from redb.
        drop(store);
    }

    // ── Re-open the store over the SAME redb; no leader, no network ─────────
    let mut store2 = EgStore::open(super::DEFAULT_GROUP, backend.clone(), ctx.clone()).unwrap();
    let log_state = store2.get_log_state().await.expect("get_log_state");
    assert_eq!(
        log_state.last_log_id.map(|l| l.index),
        Some(12),
        "the restarted store must recover its log tail from redb (index 12)"
    );
    let replayed = store2
        .try_get_log_entries(1..=12)
        .await
        .expect("read log range");
    assert_eq!(replayed.len(), 12, "all 12 entries replay from redb");
    for (k, e) in replayed.iter().enumerate() {
        assert_eq!(e.log_id.index, (k + 1) as u64);
    }

    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// KG-2.204 fault injection: append committed log entries, then KILL the writer
/// MID-LIFE (drop the backend → its writer thread joins, flushing) and RE-OPEN a
/// brand-new backend over the same files. No committed (fsynced) entry is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fault_injection_no_committed_log_entry_lost_on_restart() {
    let dir = fresh_dir("faultlog");
    // DurabilityPolicy::Each = a committed (awaited) append is fsynced before the await
    // returns, so anything we observe as Ok IS on disk.
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx {
        state: state.clone(),
        router: None,
    };

    {
        let mut store = EgStore::open(super::DEFAULT_GROUP, backend.clone(), ctx.clone()).unwrap();
        // Append 8 entries; each append_to_log awaits the durable group-commit fsync
        // (commit-before-ack), so all 8 are provably on disk when this returns Ok.
        for i in 1..=8u64 {
            store
                .append(
                    vec![make_log_entry(i, 1, &format!("k{i}"))],
                    IOFlushed::noop(),
                )
                .await
                .expect("durable append");
        }
        drop(store);
    }
    // "Kill": drop ALL handles to the backend so its writer thread shuts down + the
    // single-process redb file lock releases (the closest in-process analog to losing
    // the process — the on-disk redb is exactly what a kill -9 would leave, since
    // every append was fsynced). The state holds a backend Arc too: release it first.
    state.write().await.persistence = None;
    backend.shutdown();
    drop(backend);

    // Restart: brand-new backend + store over the SAME files. Every fsynced entry
    // must still be present — no lost committed entry.
    let backend2: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("reopen redb"),
    );
    let mut store2 = EgStore::open(super::DEFAULT_GROUP, backend2.clone(), ctx).unwrap();
    let entries = store2
        .try_get_log_entries(1..=8)
        .await
        .expect("read after restart");
    assert_eq!(
        entries.len(),
        8,
        "every fsynced (committed) log entry must survive the kill+restart"
    );

    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// KG-2.205: multi-group isolation — composite-key log rows for different groups
// do not collide, and groups commit independently.
// ─────────────────────────────────────────────────────────────────────────

/// Two stores for DIFFERENT group ids share ONE redb DB. Their logs are keyed by
/// `(group_id, index)`, so writing group A does not touch group B's log even at the
/// same index — proving the shared-DB composite-key layout isolates groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_group_logs_isolate_on_shared_redb() {
    let dir = fresh_dir("multigroup");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx {
        state,
        router: None,
    };

    let mut g7 = EgStore::open(7, backend.clone(), ctx.clone()).unwrap();
    let mut g9 = EgStore::open(9, backend.clone(), ctx.clone()).unwrap();

    // Group 7 gets indices 1..=5; group 9 gets indices 1..=3 — same index range,
    // DIFFERENT groups, ONE redb DB.
    g7.append(
        (1..=5)
            .map(|i| make_log_entry(i, 1, &format!("g7-{i}")))
            .collect::<Vec<_>>(),
        IOFlushed::noop(),
    )
    .await
    .unwrap();
    g9.append(
        (1..=3)
            .map(|i| make_log_entry(i, 1, &format!("g9-{i}")))
            .collect::<Vec<_>>(),
        IOFlushed::noop(),
    )
    .await
    .unwrap();

    // Each group sees ONLY its own entries.
    assert_eq!(
        g7.get_log_state()
            .await
            .unwrap()
            .last_log_id
            .map(|l| l.index),
        Some(5)
    );
    assert_eq!(
        g9.get_log_state()
            .await
            .unwrap()
            .last_log_id
            .map(|l| l.index),
        Some(3)
    );
    let e7 = g7.try_get_log_entries(1..=10).await.unwrap();
    let e9 = g9.try_get_log_entries(1..=10).await.unwrap();
    assert_eq!(e7.len(), 5, "group 7 has exactly its 5 entries");
    assert_eq!(e9.len(), 3, "group 9 has exactly its 3 entries");

    // Truncating group 7 does NOT affect group 9 (isolation under mutation). openraft
    // 0.10's `truncate_after(Some(id))` deletes entries STRICTLY AFTER `id`; passing the
    // index-2 log id removes indices 3,4,5 and leaves 1,2.
    g7.truncate_after(Some(make_log_entry(2, 1, "x").log_id()))
        .await
        .unwrap();
    assert_eq!(g7.try_get_log_entries(1..=10).await.unwrap().len(), 2);
    assert_eq!(
        g9.try_get_log_entries(1..=10).await.unwrap().len(),
        3,
        "group 9's log is untouched by group 7's truncation"
    );

    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// KG-2.205: a live two-group cluster (one node, two groups on the SHARED listener)
/// — each group elects its own leader and commits independently, demuxed by group id
/// over ONE TCP listener, all over ONE shared redb DB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_groups_one_node_commit_independently() {
    use super::multi::MultiRaft;

    let dir = fresh_dir("twogroups");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx {
        state: state.clone(),
        router: None,
    };
    let port = free_ports(1)[0];
    let node_id: NodeId = 1;
    let peers: BTreeMap<NodeId, BasicNode> =
        [(node_id, BasicNode::new(format!("127.0.0.1:{port}")))].into();

    let multi = MultiRaft::start(node_id, format!("127.0.0.1:{port}"), backend.clone(), ctx)
        .await
        .expect("start multi");
    // Two groups on the SAME node + SAME shared listener + SAME redb DB.
    multi.create_group(100, peers.clone(), true).await.unwrap();
    multi.create_group(200, peers.clone(), true).await.unwrap();

    // Each group elects its own (single-node) leader.
    for gid in [100u64, 200u64] {
        let g = multi.group(gid).await.expect("group exists");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(node_id) }
        })
        .await
        .unwrap_or_else(|_| panic!("group {gid} must elect a leader"));
    }

    // Route a write to each group; both commit independently.
    multi.router().assign("graphA", 100);
    multi.router().assign("graphB", 200);
    for (graph, gid) in [("graphA", 100u64), ("graphB", 200u64)] {
        let g = multi.group_for_graph(graph).await.expect("group for graph");
        assert_eq!(multi.router().group_of(graph), gid);
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(graph),
            graph_name: graph.to_string(),
            graph_type: GraphType::Global,
            committed_at_ms: 0,
            mutation: super::RaftMutationContext::internal(
                "raft-group-test",
                graph,
                "first-write",
                gid,
                0,
            ),
            command: super::ReplicatedMutation::graph(
                Method::AddNode {
                    node_id: format!("{graph}-n1"),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"g": graph}))
                        .unwrap(),
                },
                "raft-test",
            )
            .unwrap(),
        };
        g.client_write(req)
            .await
            .unwrap_or_else(|e| panic!("write to group {gid} failed: {e}"));
    }

    // Both graphs materialized — independent commits on two groups, one node.
    let s = state.read().await;
    assert_eq!(
        s.registry.get("graphA").map(|e| e.core.node_count()),
        Some(1)
    );
    assert_eq!(
        s.registry.get("graphB").map(|e| e.core.node_count()),
        Some(1)
    );
    drop(s);

    // Group lifecycle: close one group; the other is unaffected.
    multi.close_group(100).await.unwrap();
    assert!(multi.group(100).await.is_none(), "group 100 closed");
    assert!(multi.group(200).await.is_some(), "group 200 still running");

    multi.close_group(200).await.unwrap();
    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// M2 hardening (CONCEPT:AU-KG.ontology.manage-arbitrary / KG-2.266 / KG-2.267)
// ─────────────────────────────────────────────────────────────────────────

/// KG-2.265: the per-peer connection pool REUSES a warm connection across
/// sequential RPCs — three round-trips to one peer pay exactly ONE TCP connect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_pool_reuses_warm_connection() {
    use super::network::{read_frame, write_frame, PeerPool};

    // A tiny echo server speaking the Raft frame protocol: read a frame, write it back.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            tokio::spawn(async move {
                while let Ok(buf) = read_frame(&mut s).await {
                    if write_frame(&mut s, &buf).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let pool = PeerPool::with_capacity(4);
    // First round-trip opens a fresh connection; the next two must REUSE it.
    assert_eq!(pool.round_trip(&addr, b"ping-1").await.unwrap(), b"ping-1");
    assert_eq!(pool.round_trip(&addr, b"ping-2").await.unwrap(), b"ping-2");
    assert_eq!(pool.round_trip(&addr, b"ping-3").await.unwrap(), b"ping-3");
    assert_eq!(
        pool.opens(),
        1,
        "three sequential RPCs to one peer must pay exactly one TCP connect"
    );
    assert!(
        pool.reuses() >= 2,
        "later RPCs must reuse the warm connection (reuses={})",
        pool.reuses()
    );
}

/// KG-2.265: a stale idle connection (peer closed it) is transparently retried on a
/// FRESH connection — the reuse optimization never surfaces a spurious failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_pool_retries_on_stale_connection() {
    use super::network::{read_frame, write_frame, PeerPool};

    // Echo server that serves EXACTLY ONE frame per connection, then closes — so any
    // connection returned to the pool is stale for the next round-trip.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            tokio::spawn(async move {
                if let Ok(buf) = read_frame(&mut s).await {
                    let _ = write_frame(&mut s, &buf).await;
                }
                // connection drops here → the pooled copy is now stale.
            });
        }
    });

    let pool = PeerPool::with_capacity(4);
    assert_eq!(pool.round_trip(&addr, b"a").await.unwrap(), b"a");
    // The pooled connection is stale; the next call must reconnect and still succeed.
    assert_eq!(pool.round_trip(&addr, b"b").await.unwrap(), b"b");
    assert_eq!(
        pool.opens(),
        2,
        "a stale reuse must force a fresh reconnect, not a hard error"
    );
}

/// KG-2.266: the tenant-range ring distributes un-pinned graphs across multiple
/// groups, explicit overrides win, and an empty ring is the single-group default.
#[test]
fn group_router_distributes_tenants_across_ring() {
    use super::multi::GroupRouter;

    let r = GroupRouter::new();
    // Default (no ring): every graph routes to DEFAULT_GROUP — scaffold behavior.
    for g in ["a", "b", "__commons__", "tenant-42"] {
        assert_eq!(r.group_of(g), super::DEFAULT_GROUP);
    }

    // Configure a 4-group tenant-range ring (sorted + de-duplicated).
    r.set_group_ring(&[3, 1, 0, 2, 1]);
    assert_eq!(r.group_ring(), vec![0, 1, 2, 3]);

    // Many distinct tenants spread across MORE THAN ONE group.
    let mut seen = std::collections::BTreeSet::new();
    for i in 0..200 {
        let g = r.group_of(&format!("tenant-{i}"));
        assert!((0..4).contains(&g));
        seen.insert(g);
    }
    assert!(
        seen.len() >= 2,
        "tenants must spread across multiple groups, got {seen:?}"
    );

    // Deterministic + stable: same name → same group on repeat (and on any node).
    assert_eq!(r.group_of("tenant-7"), r.group_of("tenant-7"));

    // An explicit override beats the ring (the reshard / pin path).
    r.assign("tenant-7", 3);
    assert_eq!(r.group_of("tenant-7"), 3);

    // Cross-shard span detection now works across real tenant ranges.
    r.assign("x", 0);
    r.assign("y", 1);
    assert!(r.is_cross_shard(["x", "y"]));
    assert!(!r.is_cross_shard(["x", "x"]));

    // Collapse back to the single-group default.
    r.set_group_ring(&[]);
    assert_eq!(r.group_of("an-unpinned-graph"), super::DEFAULT_GROUP);
}

/// KG-2.267: a group's snapshot dump is SCOPED to its own tenant-range graphs — a
/// graph pinned to another group never bleeds into this group's snapshot. A store
/// opened WITHOUT a router (the unscoped scaffold path) still dumps the whole registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_snapshot_is_scoped_to_its_tenant_range() {
    use super::multi::GroupRouter;

    let dir = fresh_dir("scopedsnap");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;

    // Two graphs, pinned to two DIFFERENT non-default groups via the router. Neither
    // is DEFAULT_GROUP (0), which already owns the bootstrap `__commons__` graph — so
    // a pinned graph never shares a group with the un-pinned default tenant.
    let router = Arc::new(GroupRouter::new());
    router.assign("graphA", 3);
    router.assign("graphB", 5);

    // Snapshot membership comes from the durable graph identity authority. The
    // registry is only the resident projection used to select this group's rows.
    backend
        .register_graph("graphA", "graphA", GraphType::Global)
        .await
        .unwrap();
    backend
        .register_graph("graphB", "graphB", GraphType::Global)
        .await
        .unwrap();
    {
        let mut s = state.write().await;
        let _ = s.registry.create_graph("graphA", GraphType::Global, None);
        let _ = s.registry.create_graph("graphB", GraphType::Global, None);
        assert!(s.registry.evict_resident("graphB"));
    }

    let ctx = AppCtx {
        state: state.clone(),
        router: Some(router.clone()),
    };
    let g0 = EgStore::open(0, backend.clone(), ctx.clone()).unwrap();
    let g3 = EgStore::open(3, backend.clone(), ctx.clone()).unwrap();
    let g5 = EgStore::open(5, backend.clone(), ctx.clone()).unwrap();

    // Group 3's snapshot carries ONLY resident graphA; group 5's ONLY catalog-only
    // graphB — proving eviction cannot remove durable authority from a snapshot. No
    // graph bleeds across groups, and the DEFAULT group (0) owns only `__commons__`.
    assert_eq!(
        g3.scoped_snapshot_graph_names().await,
        vec!["graphA".to_string()]
    );
    assert_eq!(
        g5.scoped_snapshot_graph_names().await,
        vec!["graphB".to_string()]
    );
    assert_eq!(
        g0.scoped_snapshot_graph_names().await,
        vec!["__commons__".to_string()]
    );

    // A store WITHOUT a router (the unscoped scaffold path) dumps the WHOLE registry.
    let ctx_global = AppCtx {
        state: state.clone(),
        router: None,
    };
    let g_global = EgStore::open(0, backend.clone(), ctx_global).unwrap();
    assert_eq!(
        g_global.scoped_snapshot_graph_names().await,
        vec![
            "__commons__".to_string(),
            "graphA".to_string(),
            "graphB".to_string()
        ]
    );

    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// M2 remaining (CONCEPT:EG-KG.storage.kg-kg-2 / KG-2.270 / KG-2.271)
//   R3 multi-node membership join · R1 leader balancing · R2 heartbeat coalescing
// ─────────────────────────────────────────────────────────────────────────

/// An empty-entries `AppendEntries` for group `gid` — exactly what openraft sends as a
/// HEARTBEAT (the case R2 coalesces).
fn heartbeat_req() -> openraft::raft::AppendEntriesRequest<super::TypeConfig> {
    openraft::raft::AppendEntriesRequest {
        vote: openraft::Vote::new(1, 1),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    }
}

/// A log-bearing `AppendEntries` (NON-heartbeat) — must NOT coalesce.
fn append_with_entry() -> openraft::raft::AppendEntriesRequest<super::TypeConfig> {
    openraft::raft::AppendEntriesRequest {
        vote: openraft::Vote::new(1, 1),
        prev_log_id: None,
        entries: vec![make_log_entry(1, 1, "x")],
        leader_commit: None,
    }
}

/// KG-2.270: the round-robin leader-target function spreads leadership across all voters
/// deterministically — the property that makes the cooperative balancer converge.
#[test]
fn desired_leader_round_robin_spreads_across_voters() {
    use super::multi::desired_leader;

    // Empty voter set ⇒ no target.
    assert_eq!(desired_leader(0, &[]), None);
    // Single voter ⇒ always that voter.
    assert_eq!(desired_leader(7, &[5]), Some(5));

    // Three voters: gid % 3 indexes the SORTED set, so consecutive groups round-robin.
    let voters = [1u64, 2, 3];
    assert_eq!(desired_leader(0, &voters), Some(1));
    assert_eq!(desired_leader(1, &voters), Some(2));
    assert_eq!(desired_leader(2, &voters), Some(3));
    assert_eq!(desired_leader(7, &voters), Some(2)); // 7 % 3 == 1 → voters[1]

    // Over many groups every voter is chosen (leadership actually spreads), and the
    // mapping is deterministic (identical on every node — no coordination needed).
    let mut seen = std::collections::BTreeSet::new();
    for gid in 0..30u64 {
        let t = desired_leader(gid, &voters).unwrap();
        assert_eq!(desired_leader(gid, &voters), Some(t), "deterministic");
        seen.insert(t);
    }
    assert_eq!(
        seen,
        [1, 2, 3].into_iter().collect(),
        "every voter must be a leader target for some group"
    );
}

/// KG-2.271: the heartbeat coalescer buckets heartbeats BY PEER, passes non-heartbeats
/// through, and drains one batch per peer (the batch-construction logic).
#[test]
fn heartbeat_coalescer_batches_per_peer() {
    use super::network::{GroupRpc, HeartbeatCoalescer};

    let c = HeartbeatCoalescer::new();

    // A log-bearing append is NOT a heartbeat — refused (caller sends it directly).
    assert!(!HeartbeatCoalescer::is_heartbeat(&GroupRpc::Append(
        1,
        append_with_entry()
    )));
    assert!(!c.offer("peerA", GroupRpc::Append(1, append_with_entry())));
    assert_eq!(c.pending_for("peerA"), 0);

    // Heartbeats to two peers: 2 → peerA, 1 → peerB.
    assert!(c.offer("peerA", GroupRpc::Append(1, heartbeat_req())));
    assert!(c.offer("peerA", GroupRpc::Append(2, heartbeat_req())));
    assert!(c.offer("peerB", GroupRpc::Append(3, heartbeat_req())));
    assert_eq!(c.pending_for("peerA"), 2);
    assert_eq!(c.pending_for("peerB"), 1);

    // Drain → exactly one batch per peer, preserving per-peer membership.
    let mut batches = c.drain_batches();
    batches.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].0, "peerA");
    assert_eq!(batches[0].1.len(), 2, "peerA's two heartbeats coalesce");
    assert_eq!(batches[1].0, "peerB");
    assert_eq!(batches[1].1.len(), 1);

    // Counters + buffer emptied.
    assert_eq!(c.coalesced(), 3, "three heartbeats folded into batches");
    assert_eq!(c.flushes(), 1);
    assert_eq!(c.pending_for("peerA"), 0);

    // An empty drain is a no-op (no extra flush counted).
    assert!(c.drain_batches().is_empty());
    assert_eq!(c.flushes(), 1);
}

/// KG-2.271: a coalesced heartbeat BATCH rides ONE pooled connection to the shared
/// listener, which demuxes each tagged sub-RPC to its group and replies in order — so
/// N group heartbeats to one peer cost ONE round-trip, not N.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coalesced_batch_round_trips_on_one_connection() {
    use super::multi::MultiRaft;
    use super::network::{GroupRpc, GroupRpcReply, HeartbeatCoalescer, PeerPool};

    let dir = fresh_dir("hbbatch");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx {
        state,
        router: None,
    };
    let port = free_ports(1)[0];
    let addr = format!("127.0.0.1:{port}");
    // A node with its shared listener up but NO groups: each demuxed sub-RPC gets a
    // "no group here" reply — enough to prove the batch envelope demuxes + replies in
    // order over ONE connection (the transport contract, independent of election).
    let multi = MultiRaft::start(1, addr.clone(), backend.clone(), ctx)
        .await
        .expect("start multi");

    let pool = PeerPool::with_capacity(4);
    let batch = vec![
        GroupRpc::Append(100, heartbeat_req()),
        GroupRpc::Append(200, heartbeat_req()),
        GroupRpc::Append(300, heartbeat_req()),
    ];
    let replies = HeartbeatCoalescer::send_batch(&pool, &addr, batch)
        .await
        .expect("send coalesced batch");
    assert_eq!(
        replies.len(),
        3,
        "one reply per coalesced heartbeat, in order"
    );
    for r in &replies {
        assert!(matches!(r, GroupRpcReply::Append(_)));
    }
    assert_eq!(
        pool.opens(),
        1,
        "the whole batch must ride exactly ONE TCP connect"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// KG-2.268 + KG-2.270: a group is grown from a single-node bootstrap to a 3-VOTER group
/// spanning three nodes (add_learner → change_membership), a write replicates to the new
/// voters, and the leader balancer then MOVES leadership to the round-robin target node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_node_group_join_then_leader_rebalance() {
    use super::multi::{desired_leader, MultiRaft};
    use super::xread::{ReadPageRequest, RouteToken};

    let root = std::env::temp_dir().join(format!("eg-mnjoin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ports = free_ports(3);
    let addr = |i: usize| format!("127.0.0.1:{}", ports[i - 1]);
    let gid = 7u64;

    // ── Start three nodes; node 1 single-member bootstraps group 7, nodes 2/3 join EMPTY.
    let mut multis: Vec<(NodeId, Arc<MultiRaft>, Arc<RwLock<ServerState>>)> = Vec::new();
    for i in 1..=3u64 {
        let dir = root.join(format!("node{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"),
        );
        let state = make_state_with_backend(&dir, backend.clone()).await;
        let ctx = AppCtx {
            state: state.clone(),
            router: None,
        };
        let multi = MultiRaft::start(i, addr(i as usize), backend, ctx)
            .await
            .expect("start multi");
        if i == 1 {
            let peers: BTreeMap<NodeId, BasicNode> = [(1u64, BasicNode::new(addr(1)))].into();
            multi.create_group(gid, peers, true).await.unwrap();
        } else {
            // Empty, non-bootstrapping member ready to receive replication.
            multi.join_group(gid, BTreeMap::new()).await.unwrap();
        }
        multis.push((i, multi, state));
    }

    // ── Node 1 (the single-member bootstrap) becomes leader of group 7.
    let leader = multis[0].1.clone();
    {
        let g = leader.group(gid).await.expect("group on node 1");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(1) }
        })
        .await
        .expect("node 1 must lead the single-member group 7");
    }

    // ── R3: add nodes 2 and 3 as VOTERS from the leader (add_learner → change_membership).
    leader.add_group_member(gid, 2, addr(2)).await.unwrap();
    leader.add_group_member(gid, 3, addr(3)).await.unwrap();
    for (_, multi, _) in &multis {
        multi.router().assign("graph7", gid);
    }
    assert_eq!(
        leader.group_membership(gid).await,
        Some(vec![1, 2, 3]),
        "the group must now have all three nodes as voters"
    );

    // ── A write through the leader replicates to the freshly-joined voters.
    {
        let g = leader
            .group_for_graph("graph7")
            .await
            .expect("group for graph7");
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize("graph7"),
            graph_name: "graph7".to_string(),
            graph_type: GraphType::Global,
            committed_at_ms: 0,
            mutation: super::RaftMutationContext::internal(
                "raft-membership-test",
                "graph7",
                "joined-write",
                gid,
                0,
            ),
            command: super::ReplicatedMutation::graph(
                Method::AddNode {
                    node_id: "joined-write".to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"ok": true}))
                        .unwrap(),
                },
                "raft-test",
            )
            .unwrap(),
        };
        g.client_write(req).await.expect("write via leader");
    }
    // Both followers (nodes 2 and 3) must apply the replicated write.
    for idx in [1usize, 2] {
        let st = multis[idx].2.clone();
        wait_until(Duration::from_secs(10), || {
            let st = st.clone();
            async move { node_count(&st, "graph7").await == 1 }
        })
        .await
        .unwrap_or_else(|_| panic!("node {} must apply the replicated write", idx + 1));
    }

    // ── R1: the round-robin target for group 7 over [1,2,3] is node 2 (7 % 3 == 1).
    let target = desired_leader(gid, &[1, 2, 3]).unwrap();
    assert_eq!(target, 2, "round-robin target for group 7 is node 2");

    // EVERY node runs a balancing pass (as a real cluster does). openraft 0.10
    // (CONCEPT:AU-KG.backend.authority-has-already-acked): node 1 (the incumbent leader) issues the NATIVE
    // `trigger().transfer_leader(2)` — a graceful, near-instant handoff — because the
    // round-robin target for group 7 is node 2. No cooperative heartbeat-yield is
    // involved any more, and node 2 (a follower) does NOT campaign on its own.
    let node1 = multis[0].1.clone();
    let node2 = multis[1].1.clone();
    let node3 = multis[2].1.clone();
    let r1 = node1.rebalance_leaders().await;
    let r2 = node2.rebalance_leaders().await;
    let _r3 = node3.rebalance_leaders().await;
    assert_eq!(r2.targets.get(&gid), Some(&2));
    assert!(
        r1.transferred.contains(&gid),
        "node 1 (incumbent leader, target elsewhere) must transfer group 7 to node 2"
    );
    assert!(
        r2.transferred.is_empty(),
        "node 2 (a follower) does not transfer anything — only the leader hands off"
    );

    // Leadership converges to node 2 via the native transfer. Keep driving periodic
    // passes (a real periodic balancer); node 1's per-group transfer cooldown means it
    // re-issues at most once per window, which is plenty for the handoff to settle.
    let start = std::time::Instant::now();
    let mut converged = false;
    while start.elapsed() < Duration::from_secs(30) {
        if node2.group(gid).await.unwrap().current_leader().await == Some(2) {
            converged = true;
            break;
        }
        node1.rebalance_leaders().await;
        node2.rebalance_leaders().await;
        node3.rebalance_leaders().await;
        tokio::time::sleep(Duration::from_millis(800)).await;
    }
    assert!(
        converged,
        "leadership must converge to the round-robin target (node 2) via transfer_leader"
    );

    // A request initiated on node 1 now routes to node 2's leader over the same
    // authenticated/group-multiplexed PeerPool as Raft consensus traffic.
    let page = node1
        .read_page_group(
            gid,
            ReadPageRequest {
                graph_name: "graph7".to_string(),
                route: RouteToken {
                    group: gid,
                    epoch: 0,
                },
                after_node_id: None,
                expected_snapshot_version: None,
                limit: 16,
                max_bytes: 1024 * 1024,
            },
        )
        .await
        .expect("remote leader read page");
    assert_eq!(page.nodes.len(), 1);
    assert_eq!(page.nodes[0].0, "joined-write");
    assert!(page.raft_barrier_index > 0);

    // Already balanced → an extra pass anywhere transfers nothing (idempotent): node 2
    // now leads (target==self, no-op) and node 1 no longer leads group 7.
    let report2 = node2.rebalance_leaders().await;
    let report1 = node1.rebalance_leaders().await;
    assert!(
        report2.transferred.is_empty() && report1.transferred.is_empty(),
        "an already-balanced cluster must not issue further transfers (idempotent)"
    );

    // ── Cleanup.
    for (_, multi, _) in &multis {
        multi.stop_listener();
        let _ = multi.close_group(gid).await;
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// CONCEPT:EG-KG.storage.kg-kg-2 — `cluster_deployment.md` §5 item 2: a learner can be
/// attached to a LIVE Raft group WITHOUT changing the voter set (the gap the M2 soak
/// found — `MultiRaft::add_group_learner`/`change_group_voters`, split out of the
/// pre-existing bundled `add_group_member`, had no external caller). Proves the split
/// end-to-end at the `MultiRaft` API level: node 2 joins group 8 as a non-voting
/// learner, is OBSERVED in the leader's committed membership as a learner (not a
/// voter) via `group_learners`, then is promoted via `change_group_voters` and
/// observed moving into the voter set via `group_membership`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_add_group_learner_attaches_non_voting_learner_then_promotes() {
    use super::multi::MultiRaft;

    let root = std::env::temp_dir().join(format!("eg-mnlearner-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ports = free_ports(2);
    let addr = |i: usize| format!("127.0.0.1:{}", ports[i - 1]);
    let gid = 8u64;

    let mut multis: Vec<(NodeId, Arc<MultiRaft>)> = Vec::new();
    for i in 1..=2u64 {
        let dir = root.join(format!("node{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"),
        );
        let state = make_state_with_backend(&dir, backend.clone()).await;
        let ctx = super::AppCtx {
            state,
            router: None,
        };
        let multi = MultiRaft::start(i, addr(i as usize), backend, ctx)
            .await
            .expect("start multi");
        if i == 1 {
            let peers: BTreeMap<NodeId, BasicNode> = [(1u64, BasicNode::new(addr(1)))].into();
            multi.create_group(gid, peers, true).await.unwrap();
        } else {
            // Empty, non-bootstrapping member ready to receive replication — exactly
            // the "boots straight into learner-ready" state a real node 2 reaches
            // with `EPISTEMIC_GRAPH_RAFT_NODE_ID`/`_PEERS` set but `is_bootstrap`
            // false (see `cluster_deployment.md` §2b/§5).
            multi.join_group(gid, BTreeMap::new()).await.unwrap();
        }
        multis.push((i, multi));
    }

    let leader = multis[0].1.clone();
    let follower = multis[1].1.clone();
    {
        let g = leader.group(gid).await.expect("group on node 1");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(1) }
        })
        .await
        .expect("node 1 must lead the single-member group 8");
    }

    // Before the learner is attached, membership is just the bootstrap voter and
    // there are no learners on either side.
    assert_eq!(leader.group_membership(gid).await, Some(vec![1]));
    assert_eq!(leader.group_learners(gid).await, Some(vec![]));

    // ── Attach node 2 as a NON-VOTING LEARNER. Real openraft add_learner: it blocks
    // until node 2's log is caught up, so this is a genuine catch-up, not a stub.
    leader
        .add_group_learner(gid, 2, addr(2))
        .await
        .expect("add_group_learner must succeed against the leader");

    // ── Observe the REAL committed membership: node 2 is a learner, NOT a voter.
    assert_eq!(
        leader.group_membership(gid).await,
        Some(vec![1]),
        "attaching a learner must NOT change the voter set"
    );
    assert_eq!(
        leader.group_learners(gid).await,
        Some(vec![2]),
        "node 2 must appear as a non-voting learner in the leader's committed membership"
    );
    // The follower's own view (once it has applied the membership entry) agrees —
    // this is REPLICATED state, not a leader-local fiction.
    wait_until(Duration::from_secs(10), || {
        let follower = follower.clone();
        async move { follower.group_learners(gid).await == Some(vec![2]) }
    })
    .await
    .expect("node 2 must observe itself as a learner in the replicated membership");

    // A second `add_group_learner` for the SAME node is idempotent (re-confirms the
    // same membership, does not error).
    leader
        .add_group_learner(gid, 2, addr(2))
        .await
        .expect("re-adding an existing learner must be idempotent");
    assert_eq!(leader.group_learners(gid).await, Some(vec![2]));

    // ── Promote the learner to a voter via change_group_voters (a SEPARATE admin
    // step from add_group_learner — the whole point of the split).
    let mut voters: BTreeSet<NodeId> = BTreeSet::new();
    voters.insert(1);
    voters.insert(2);
    leader
        .change_group_voters(gid, voters)
        .await
        .expect("change_group_voters must promote the caught-up learner");
    assert_eq!(
        leader.group_membership(gid).await,
        Some(vec![1, 2]),
        "node 2 must now be a voter"
    );
    assert_eq!(
        leader.group_learners(gid).await,
        Some(vec![]),
        "a promoted node is no longer listed as a learner"
    );

    // change_group_voters refuses to produce an empty voter set.
    let err = leader
        .change_group_voters(gid, BTreeSet::new())
        .await
        .expect_err("an empty voter set must be refused");
    assert!(err.contains("empty voter set"), "unexpected error: {err}");

    // ── Cleanup.
    for (_, multi) in &multis {
        multi.stop_listener();
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// CONCEPT:EG-KG.storage.kg-kg-2 — the SAME gap, exercised through the REAL served
/// dispatch path (`Method::RaftAddLearner`/`Method::RaftChangeMembership`,
/// `src/server/handlers/raft_admin.rs`), not just the in-process `MultiRaft` API the
/// test above drives directly. This is the external-caller seam
/// `epistemic_graph.client`'s `raft_admin` namespace drives. Proves, in order: (a)
/// a request against the LEADER actually attaches node 2 as a learner (real
/// execution, not a stub); (b) a `RaftChangeMembership` request against that now
/// genuinely-attached FOLLOWER is redirected to the leader (`OPERATION_REDIRECTED`
/// with the real observed `leader_ref`, mirroring `PlacementRoute`'s stale-route
/// shape) and is NOT silently mis-served or applied locally; (c) the SAME request
/// against the LEADER actually promotes node 2 to a voter; and (d) an engine with
/// no live `MultiRaft` answers a clean typed error rather than a silent no-op. (a)
/// runs before (b) deliberately: openraft's `current_leader()` is honest local
/// knowledge learned only from real AppendEntries/vote traffic, so the redirect is
/// proven against a follower that has actually observed the leader through
/// replication, not one the test simply asserts knows something it was never told.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_raft_add_learner_and_change_membership_resolve_through_dispatch() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::multi::MultiRaft;
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::protocol::{Request, ResultPayload};
    use crate::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};

    const TEST_AGENT: &str = "raft-admin-wire-test-agent";
    const SECRET: &str = "raft-test";
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
    std::env::set_var(
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
        std::env::temp_dir().join(format!("eg-raft-admin-wire-auth-{}", std::process::id())),
    );

    fn signed_request(id: u64, method: Method) -> Request {
        let context = RequestContextClaims {
            principal: TEST_AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: TEST_AGENT.to_string(),
            roles: Vec::new(),
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "raft-admin-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("raft-admin-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    let root = std::env::temp_dir().join(format!("eg-wire-raft-admin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ports = free_ports(2);
    let addr = |i: usize| format!("127.0.0.1:{}", ports[i - 1]);
    let gid = super::DEFAULT_GROUP;

    let mut multis: Vec<(NodeId, Arc<MultiRaft>, Arc<RwLock<ServerState>>)> = Vec::new();
    for i in 1..=2u64 {
        let dir = root.join(format!("node{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 4096).expect("open redb"),
        );
        let state = make_state_with_backend(&dir, backend.clone()).await;
        state.write().await.isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let ctx = super::AppCtx {
            state: state.clone(),
            router: None,
        };
        let multi = MultiRaft::start(i, addr(i as usize), backend, ctx)
            .await
            .expect("start multi");
        if i == 1 {
            let peers: BTreeMap<NodeId, BasicNode> = [(1u64, BasicNode::new(addr(1)))].into();
            multi.create_group(gid, peers, true).await.unwrap();
        } else {
            multi.join_group(gid, BTreeMap::new()).await.unwrap();
        }
        // The real served `Method::RaftAddLearner`/`RaftChangeMembership` handler
        // (`handlers/raft_admin.rs`) resolves `MultiRaft` off `state.multi_raft`,
        // exactly like `handlers/placement.rs` does — wire it here so this test
        // exercises the SAME lookup a real server does.
        state.write().await.multi_raft = Some(multi.clone());
        multis.push((i, multi, state));
    }

    let leader_multi = multis[0].1.clone();
    let leader_state = multis[0].2.clone();
    let follower_state = multis[1].2.clone();
    {
        let g = leader_multi.group(gid).await.expect("group on node 1");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(1) }
        })
        .await
        .expect("node 1 must lead the default group");
    }

    // (a) A request against the LEADER actually attaches node 2 as a learner.
    // Deliberately proven FIRST: an openraft node's `current_leader()` is honest
    // LOCAL knowledge learned only through real AppendEntries/vote traffic — a
    // node that has never been contacted (like node 2 immediately after its own
    // empty, non-bootstrapping `join_group`) has no leader to report and no
    // basis to fabricate one. Attaching it as a learner is the real replication
    // event that gives it that knowledge, exactly like a real operator's first
    // admin call against a live cluster would.
    let resp = dispatch_on_heap(
        &leader_state,
        signed_request(
            1,
            Method::RaftAddLearner {
                group: None,
                node_id: 2,
                addr: addr(2),
            },
        ),
    )
    .await;
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    assert!(matches!(resp.result, Some(ResultPayload::Bool(true))));
    assert_eq!(leader_multi.group_learners(gid).await, Some(vec![2]));
    assert_eq!(leader_multi.group_membership(gid).await, Some(vec![1]));

    // Node 2 is now a genuinely attached, caught-up learner (openraft's
    // `add_learner(.., blocking=true)` does not return until the learner's log
    // is caught up) -- it has observed node 1 as leader through REAL replicated
    // traffic, not a value the test injects. Confirm that before relying on it.
    let follower_multi = multis[1].1.clone();
    {
        let g = follower_multi.group(gid).await.expect("group on node 2");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(1) }
        })
        .await
        .expect("node 2 must observe node 1 as leader after being attached as a learner");
    }

    // (b) A membership-admin request against this now-attached FOLLOWER is
    // redirected to the leader, not silently mis-served, mis-applied locally, or
    // panicking -- proven with the follower's REAL observed leader, not merely
    // that the `OPERATION_REDIRECTED` constant exists somewhere in the source.
    let resp = dispatch_on_heap(
        &follower_state,
        signed_request(
            2,
            Method::RaftChangeMembership {
                group: None,
                voters: vec![1, 2],
            },
        ),
    )
    .await;
    assert_eq!(resp.error.as_deref(), Some("OPERATION_REDIRECTED"));
    match resp.result {
        Some(ResultPayload::Raw(bytes)) => {
            let detail: crate::epistemic_operations::OperationResult =
                rmp_serde::from_slice(&bytes).expect("typed OperationResult");
            let redirect = detail.redirect.expect("redirect detail present");
            assert_eq!(redirect.leader_ref.as_deref(), Some("node:1"));
        }
        other => panic!("expected a typed redirect result, got {other:?}"),
    }
    // The redirected request must NOT have been applied anywhere: membership is
    // unchanged (still just the learner from step (a), no promotion happened).
    assert_eq!(leader_multi.group_membership(gid).await, Some(vec![1]));
    assert_eq!(leader_multi.group_learners(gid).await, Some(vec![2]));

    // (c) The SAME `RaftChangeMembership` issued against the LEADER actually
    // promotes node 2 -- real execution, not just a redirect-shaped stub.
    let resp = dispatch_on_heap(
        &leader_state,
        signed_request(
            3,
            Method::RaftChangeMembership {
                group: None,
                voters: vec![1, 2],
            },
        ),
    )
    .await;
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    assert!(matches!(resp.result, Some(ResultPayload::Bool(true))));
    assert_eq!(leader_multi.group_membership(gid).await, Some(vec![1, 2]));
    assert_eq!(leader_multi.group_learners(gid).await, Some(vec![]));

    // (c) An engine with no live MultiRaft answers a clean typed error, never a
    // silent no-op or a panic.
    let unclustered_dir = root.join("unclustered");
    std::fs::create_dir_all(&unclustered_dir).unwrap();
    let unclustered_backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(
            unclustered_dir.to_string_lossy().to_string(),
            DurabilityPolicy::Each,
            4096,
        )
        .expect("open redb"),
    );
    let unclustered_state = make_state_with_backend(
        &unclustered_dir.to_string_lossy(),
        unclustered_backend.clone(),
    )
    .await;
    unclustered_state
        .write()
        .await
        .isolation
        .register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
    let resp = dispatch_on_heap(
        &unclustered_state,
        signed_request(
            4,
            Method::RaftAddLearner {
                group: None,
                node_id: 9,
                addr: "127.0.0.1:1".to_string(),
            },
        ),
    )
    .await;
    assert_eq!(
        resp.error.as_deref(),
        Some("RAFT_NOT_CONFIGURED: this node is not running a Raft cluster")
    );
    unclustered_backend.shutdown();

    // ── Cleanup.
    for (_, multi, _) in &multis {
        multi.stop_listener();
    }
    let _ = std::fs::remove_dir_all(&root);
}

// ── Distributed graph compute (CONCEPT:EG-KG.storage.feature) ─────────────────────────────
//
// These prove the cross-shard Pregel engine produces the SAME result as the
// single-graph algorithm run over the UNION graph: a graph split across two shard
// graphs, computed distributed, equals the whole graph computed in one core. Plus the
// incremental connected-components variant equals a from-scratch run on a delta. They
// build a minimal ServerState (no Raft groups needed — the engine reads each shard
// graph's snapshot from the registry) over an in-memory-only backend.
#[cfg(feature = "compute-dist")]
mod dist_compute {
    use super::*;
    use crate::isolation::{AgentIdentity, AgentRole};
    use crate::protocol::{DistAlgo, GraphType};
    use crate::raft::pregel::{self, DistResult};
    use crate::server::access::GraphReadAuthority;

    fn props(n: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": "N", "id": n})).unwrap()
    }

    /// Build a state with two shard graphs `shA`/`shB`, the union's nodes partitioned
    /// across them (a node lives in the shard named in `placement`), and EVERY edge
    /// added to the shard that owns its SOURCE. Returns the state + a single-graph
    /// `union` core holding the WHOLE graph for the reference comparison.
    async fn two_shard_state(
        nodes: &[&str],
        edges: &[(&str, &str)],
        placement: &dyn Fn(&str) -> &'static str,
    ) -> (
        Arc<RwLock<ServerState>>,
        std::sync::Arc<crate::graph::GraphCore>,
    ) {
        let dir = std::env::temp_dir().join(format!(
            "eg-pregel-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(
                dir.to_string_lossy().to_string(),
                DurabilityPolicy::Each,
                256,
            )
            .expect("open redb"),
        );
        let state = make_state_with_backend(&dir.to_string_lossy(), backend).await;
        {
            let mut s = state.write().await;
            s.isolation.register_agent(AgentIdentity {
                agent_id: "pregel-test-authority".to_string(),
                role: AgentRole::System,
                teams: Vec::new(),
                roles: Vec::new(),
            });
            s.registry
                .create_graph("shA", GraphType::Global, None)
                .unwrap();
            s.registry
                .create_graph("shB", GraphType::Global, None)
                .unwrap();
            for n in nodes {
                let g = placement(n);
                s.registry
                    .get(g)
                    .unwrap()
                    .core
                    .add_node(n.to_string(), props(n));
            }
            for (u, v) in edges {
                // The edge's endpoints must both exist in the owning shard for petgraph
                // to add it, so we ensure the target node exists in the source's shard
                // too (a cross-shard edge's far endpoint is mirrored as a bare node).
                let g = placement(u);
                let core = &s.registry.get(g).unwrap().core;
                if !core.has_node(v) {
                    core.add_node(v.to_string(), props(v));
                }
                core.add_edge(u.to_string(), v.to_string(), props(u))
                    .unwrap();
            }
        }

        // The single-graph reference: the WHOLE union in ONE core.
        let union = std::sync::Arc::new(crate::graph::GraphCore::new());
        for n in nodes {
            union.add_node(n.to_string(), props(n));
        }
        for (u, v) in edges {
            union
                .add_edge(u.to_string(), v.to_string(), props(u))
                .unwrap();
        }
        (state, union)
    }

    async fn read_authority(state: &Arc<RwLock<ServerState>>) -> GraphReadAuthority {
        let context =
            crate::server::auth::VerifiedRequestContext::verified_for_test("pregel-test-authority");
        let state = state.read().await;
        GraphReadAuthority::from_verified(&context, &state.isolation).unwrap()
    }

    /// Round a score map for tolerant float comparison.
    fn score_map(rows: &[(String, f64)]) -> std::collections::BTreeMap<String, i64> {
        rows.iter()
            .map(|(k, v)| (k.clone(), (v * 1e6).round() as i64))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_shard_pagerank_matches_single_graph() {
        // A union graph split across two shards by a parity rule. Distributed PageRank
        // over the two shards must match the single-graph power iteration on the union.
        let nodes = ["a", "b", "c", "d", "e", "f"];
        let edges = [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("c", "d"), // cross-shard edge (c in shA, d in shB)
            ("d", "e"),
            ("e", "f"),
            ("f", "d"),
        ];
        let place = |n: &str| -> &'static str {
            if matches!(n, "a" | "b" | "c") {
                "shA"
            } else {
                "shB"
            }
        };
        let (state, union) = two_shard_state(&nodes, &edges, &place).await;
        let authority = read_authority(&state).await;

        let dist = pregel::run_distributed(
            &state,
            &["shA".into(), "shB".into()],
            &DistAlgo::PageRank {
                damping: 0.85,
                iterations: 50,
            },
            &authority,
        )
        .await
        .unwrap();
        let DistResult::Scores(dist_scores) = dist else {
            panic!("expected scores")
        };

        let single = crate::algorithms::pagerank(&union.topology_snapshot(), 0.85, 50);
        assert_eq!(
            score_map(&dist_scores),
            score_map(&single),
            "cross-shard PageRank must equal single-graph PageRank on the union"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_shard_connected_components_matches_single_graph() {
        // Two components: {a,b,c} (in shA) and {d,e} (split shA/shB) — a cross-shard
        // component. Distributed CC must produce the SAME partition as single-graph CC.
        let nodes = ["a", "b", "c", "d", "e"];
        let edges = [("a", "b"), ("b", "c"), ("d", "e")];
        let place = |n: &str| -> &'static str {
            if matches!(n, "a" | "b" | "d") {
                "shA"
            } else {
                "shB"
            }
        };
        let (state, union) = two_shard_state(&nodes, &edges, &place).await;
        let authority = read_authority(&state).await;

        let dist = pregel::run_distributed(
            &state,
            &["shA".into(), "shB".into()],
            &DistAlgo::ConnectedComponents,
            &authority,
        )
        .await
        .unwrap();
        let DistResult::Labels(dist_labels) = dist else {
            panic!("expected labels")
        };

        // Reference: single-graph CC partition. Compare as "same-component" equivalence
        // (the label VALUE differs by indexing, so compare which nodes share a label).
        let single = crate::algorithms::connected_components(&union.topology_snapshot());
        let mut single_comp: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (ci, comp) in single.iter().enumerate() {
            for n in comp {
                single_comp.insert(n.clone(), ci);
            }
        }
        let dist_map: std::collections::HashMap<String, i64> = dist_labels.into_iter().collect();

        // Two nodes share a distributed label IFF they share a single-graph component.
        for i in &nodes {
            for j in &nodes {
                let same_dist = dist_map[*i] == dist_map[*j];
                let same_single = single_comp[*i] == single_comp[*j];
                assert_eq!(
                    same_dist, same_single,
                    "cross-shard CC partition must match single-graph for ({i},{j})"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn incremental_cc_equals_from_scratch() {
        // Start with two separate components, compute CC, then add a cross-shard edge
        // that MERGES them. The incremental recompute (seeded from the prior labeling,
        // re-propagating only the affected region) must equal a from-scratch CC.
        let nodes = ["a", "b", "c", "d"];
        let edges = [("a", "b"), ("c", "d")]; // two components {a,b}, {c,d}
        let place = |n: &str| -> &'static str {
            if matches!(n, "a" | "c") {
                "shA"
            } else {
                "shB"
            }
        };
        let (state, _union) = two_shard_state(&nodes, &edges, &place).await;
        let authority = read_authority(&state).await;

        let graphs = ["shA".to_string(), "shB".to_string()];
        let prior = match pregel::run_distributed(
            &state,
            &graphs,
            &DistAlgo::ConnectedComponents,
            &authority,
        )
        .await
        .unwrap()
        {
            DistResult::Labels(l) => l,
            _ => panic!("labels"),
        };

        // DELTA: add edge b→c, merging the two components. b lives in shB; add there.
        {
            let s = state.read().await;
            let core = &s.registry.get("shB").unwrap().core;
            if !core.has_node("c") {
                core.add_node("c".to_string(), props("c"));
            }
            core.add_edge("b".to_string(), "c".to_string(), props("b"))
                .unwrap();
        }

        // Incremental: only b and c are affected by the new edge.
        let affected: std::collections::HashSet<String> =
            ["b".to_string(), "c".to_string()].into_iter().collect();
        let incr = pregel::incremental_connected_components(
            &state, &graphs, &prior, &affected, &authority,
        )
        .await
        .unwrap();

        // From scratch over the same (post-delta) graphs.
        let scratch = match pregel::run_distributed(
            &state,
            &graphs,
            &DistAlgo::ConnectedComponents,
            &authority,
        )
        .await
        .unwrap()
        {
            DistResult::Labels(l) => l,
            _ => panic!("labels"),
        };

        // Compare partitions (same-component equivalence — label values are stable here
        // because both seed from the same vertex indexing, but compare structurally to
        // be robust).
        let im: std::collections::HashMap<String, i64> = incr.into_iter().collect();
        let sm: std::collections::HashMap<String, i64> = scratch.into_iter().collect();
        for i in &nodes {
            for j in &nodes {
                assert_eq!(
                    im[*i] == im[*j],
                    sm[*i] == sm[*j],
                    "incremental CC partition must equal from-scratch for ({i},{j})"
                );
            }
        }
        // All four now in one component (the merge took effect).
        assert!(
            nodes.iter().all(|n| sm[*n] == sm["a"]),
            "the cross-shard merge must unite all four nodes"
        );
    }
}

// ── Materialized view durability (CONCEPT:EG-KG.storage.feature) ──────────────────────────
//
// A materialized view round-trips through the redb durable tier: the handler computes
// + persists it, and a fresh scan (the boot reload path) recovers the SAME view. Tests
// the redb matview_put/matview_scan + the MatView serde, the durability half of the
// matview lifecycle.
#[cfg(feature = "compute-dist")]
mod matview {
    use super::*;
    use crate::protocol::DistAlgo;
    use crate::raft::pregel::{DistResult, MatView};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn matview_persists_and_reloads_from_redb() {
        let dir = std::env::temp_dir().join(format!(
            "eg-matview-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let backend = RedbBackend::open(
            dir.to_string_lossy().to_string(),
            DurabilityPolicy::Each,
            256,
        )
        .unwrap();

        let view = MatView {
            name: "ranks".into(),
            graphs: vec!["shA".into(), "shB".into()],
            algo: DistAlgo::PageRank {
                damping: 0.85,
                iterations: 20,
            },
            result: DistResult::Scores(vec![("a".into(), 0.5), ("b".into(), 0.3)]),
        };
        let blob = rmp_serde::to_vec_named(&view).unwrap();
        backend.matview_put("ranks", blob).await.unwrap();

        // The boot reload path: scan recovers the SAME view, byte-for-byte.
        let scanned = backend.matview_scan().unwrap();
        assert_eq!(scanned.len(), 1, "exactly one matview persisted");
        let (name, recovered_blob) = &scanned[0];
        assert_eq!(name, "ranks");
        let recovered: MatView = rmp_serde::from_slice(recovered_blob).unwrap();
        assert_eq!(
            recovered, view,
            "matview must round-trip through redb unchanged"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// ADR-2 / W1.2: shard-aligned multi-raft-group write scaling
// (`reports/wave1/ADR-scale-trio.md` §ADR-2). Group g owns redb shard g, so HA
// (raft) and write-parallelism (K shards) coexist instead of the M2 spike's K=1.
// ─────────────────────────────────────────────────────────────────────────

/// The ADR-2 alignment invariant, proven WITHOUT a cluster: with the production ring
/// `0..N`, `GroupRouter::group_of(name)` returns EXACTLY the durable shard
/// `shard_index(sanitize(name), N)` the graph's data lands in — so "raft group g owns
/// redb shard g" holds for every graph, INCLUDING names the durable key `sanitize`s
/// (`a:b` → `a~3ab`), which the pre-ADR-2 raw-name ring hash would have mis-aligned
/// against the sanitized storage hash.
#[test]
fn group_of_equals_durable_shard_index_under_production_ring() {
    use super::multi::GroupRouter;
    use crate::server::persistence::redb_backend::shard_index;

    let mut names: Vec<String> = vec![
        "__commons__",
        "agent:planner",
        "acme:ws1",
        "tenant/hot",
        "a:b",
        "a/b",
        "ZZZ",
        "g-xyz",
        "graph-a",
        "深い",
        "emoji-🚀",
        "under_score.dot",
        "space here",
        "n%23",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    names.push("x".repeat(300)); // long name → `~h<sha256>` sanitize path

    for n in [2usize, 3, 4, 6, 8, 16] {
        let router = GroupRouter::new();
        let ring: Vec<u64> = (0..n as u64).collect();
        router.set_group_ring(&ring);
        for name in &names {
            let g = router.group_of(name) as usize;
            let s = shard_index(&crate::persist::sanitize(name), n);
            assert_eq!(
                g, s,
                "group_of must equal the durable shard for {name:?} at N={n} \
                 (ADR-2: raft group g owns redb shard g)"
            );
        }
    }
}

/// A replicated `AddNode` for `graph`, uniquely keyed by `node_id` (its coordinator key,
/// so distinct ids never collide as an idempotent replay). ~512-byte payload so each
/// durable commit does real work — the per-shard writer, not client plumbing, is the cost.
fn scale_add_node_req(graph: &str, node_id: &str, seq: u64) -> RaftRequest {
    let filler = "x".repeat(2048);
    RaftRequest {
        graph_fname: crate::persist::sanitize(graph),
        graph_name: graph.to_string(),
        graph_type: GraphType::Global,
        committed_at_ms: 0,
        mutation: super::RaftMutationContext::internal("w12-scale", graph, node_id, seq, 0),
        // The native command is AEAD-sealed with the node's server secret and unsealed at
        // apply; this MUST match `make_state_with_backend`'s `auth_secret` ("raft-test")
        // or apply fails "native Raft command authentication failed".
        command: super::ReplicatedMutation::graph(
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(
                    &serde_json::json!({"seq": seq, "pad": filler}),
                )
                .unwrap(),
            },
            "raft-test",
        )
        .unwrap(),
    }
}

/// Write one `AddNode` for `graph` (seq-keyed) through `node`'s local group handle.
/// `node` must currently lead `graph`'s group, else the local `client_write` is rejected.
async fn write_via_node(
    nodes: &BTreeMap<NodeId, StartedNode>,
    node: NodeId,
    graph: &str,
    seq: u64,
) -> Result<(), String> {
    let g = nodes[&node]
        .multi
        .group_for_graph(graph)
        .await
        .ok_or_else(|| format!("no local group for {graph} on node {node}"))?;
    let id = format!("{graph}-n{seq}");
    g.client_write(scale_add_node_req(graph, &id, seq))
        .await
        .map(|_| ())
}

/// Resolve, per group `0..n_groups`, the `MultiRaft` of the node that currently leads it.
async fn map_group_leaders(
    nodes: &BTreeMap<NodeId, StartedNode>,
    n_groups: u64,
    timeout: Duration,
) -> BTreeMap<u64, std::sync::Arc<super::multi::MultiRaft>> {
    let mut map = BTreeMap::new();
    for gid in 0..n_groups {
        let start = std::time::Instant::now();
        loop {
            let mut found = None;
            for n in nodes.values() {
                if let Some(g) = n.multi.group(gid).await {
                    if let Some(leader) = g.current_leader().await {
                        if let Some(ln) = nodes.get(&leader) {
                            found = Some(ln.multi.clone());
                            break;
                        }
                    }
                }
            }
            if let Some(m) = found {
                map.insert(gid, m);
                break;
            }
            if start.elapsed() > timeout {
                panic!("group {gid} never elected a discoverable leader");
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }
    map
}

/// Start a 3-node cluster with `n_groups` groups AND `n_groups` durable shards (K == N,
/// ADR-2), run a fixed concurrent write workload spread across `n_graphs` graphs, and
/// return `(writes_per_second, total_writes)`. `open_with_shards` forces K == N because
/// `resolve_shard_count()` returns 1 under `cfg(test)` (the raft env var is unset in tests).
async fn run_group_write_workload(
    tag: &str,
    n_groups: u64,
    n_graphs: usize,
    writes_per_graph: u64,
) -> (f64, u64) {
    let root = std::env::temp_dir().join(format!("eg-w12-scale-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ports = free_ports(3);
    let mut nodes: BTreeMap<NodeId, StartedNode> = BTreeMap::new();
    for i in 1..=3u64 {
        let dir = root.join(format!("node{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open_with_shards(
                dir.clone(),
                DurabilityPolicy::Each,
                4096,
                n_groups as usize,
            )
            .expect("open K==N sharded redb"),
        );
        assert_eq!(backend.as_redb().unwrap().shard_count(), n_groups as usize);
        let state = make_state_with_backend(&dir, backend).await;
        let started = node::start(cluster_cfg_with_groups(i, &ports, n_groups), state.clone())
            .await
            .expect("start raft node");
        state.write().await.raft = Some(started.handle.clone());
        nodes.insert(i, started);
    }

    // Every group elects a leader; resolve each group's leader node.
    let leaders = map_group_leaders(&nodes, n_groups, Duration::from_secs(20)).await;
    let router = nodes.values().next().unwrap().multi.router();

    // Deterministic graph set; each routes to its group's leader (group_of == shard_index).
    let graphs: Vec<String> = (0..n_graphs).map(|i| format!("w12-{tag}-g{i}")).collect();
    let mut assignments: Vec<(String, super::multi::Group)> = Vec::with_capacity(graphs.len());
    for graph in &graphs {
        let gid = router.group_of(graph);
        let multi = leaders.get(&gid).expect("leader multi for group").clone();
        let group = multi.group_for_graph(graph).await.expect("group for graph");
        assignments.push((graph.clone(), group));
    }

    // Timed section: one task per graph, all concurrent, each doing `writes_per_graph`
    // durable replicated writes through its group's leader.
    let t0 = std::time::Instant::now();
    let mut handles = Vec::new();
    for (graph, group) in assignments {
        handles.push(tokio::spawn(async move {
            for seq in 0..writes_per_graph {
                let node_id = format!("{graph}-n{seq}");
                let req = scale_add_node_req(&graph, &node_id, seq);
                group
                    .client_write(req)
                    .await
                    .map_err(|e| format!("{graph} seq {seq}: {e}"))?;
            }
            Ok::<(), String>(())
        }));
    }
    for h in handles {
        h.await.unwrap().expect("workload write must commit");
    }
    let elapsed = t0.elapsed();
    let total = n_graphs as u64 * writes_per_graph;
    let wps = total as f64 / elapsed.as_secs_f64();

    for (_, n) in nodes {
        n.multi.stop_listener();
        let _ = n.handle.raft.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&root);
    (wps, total)
}

/// ACCEPTANCE (ADR-2 §Acceptance): a 3-node cluster with N groups (== N durable shards)
/// sustains aggregate write throughput ≥ 2.5× the single-group (K=1) baseline — N parallel
/// per-node durable writers vs one. Both runs execute the SAME workload in ONE test; the
/// ratio is asserted and the absolute writes/sec logged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multi_group_write_throughput_scales_vs_single_group() {
    const N_GROUPS: u64 = 8;
    const N_GRAPHS: usize = 24;
    const WRITES_PER_GRAPH: u64 = 10;

    // Multi-group (K == N == 6): 6 parallel durable shard writers.
    let (multi_wps, total) =
        run_group_write_workload("multi", N_GROUPS, N_GRAPHS, WRITES_PER_GRAPH).await;
    // Single-group baseline (K == 1): one serialized durable writer, SAME workload.
    let (single_wps, _) = run_group_write_workload("single", 1, N_GRAPHS, WRITES_PER_GRAPH).await;

    let ratio = multi_wps / single_wps;
    tracing::info!(
        n_groups = N_GROUPS,
        total_writes = total,
        multi_writes_per_sec = multi_wps,
        single_writes_per_sec = single_wps,
        ratio,
        "ADR-2 W1.2 write-scaling: N-group vs single-group aggregate throughput"
    );
    println!(
        "ADR-2 W1.2 write-scaling: multi(K={N_GROUPS})={multi_wps:.0} w/s, single(K=1)={single_wps:.0} w/s, ratio={ratio:.2}x ({total} writes each)"
    );
    assert!(
        ratio >= 2.5,
        "N={N_GROUPS}-group aggregate write throughput ({multi_wps:.0} w/s) must be ≥2.5× the \
         single-group baseline ({single_wps:.0} w/s); measured {ratio:.2}×"
    );
}

/// ACCEPTANCE (ADR-2 §Acceptance): per-group failover independence — killing ONE group's
/// leader must not interrupt writes to a DIFFERENT group. Leaders are spread across nodes
/// (round-robin `desired_leader`), so group 1's leader (node 2) and group 2's leader
/// (node 3) live on distinct nodes; killing node 2 leaves group 2 writing uninterrupted
/// while group 1 independently re-elects.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn per_group_leader_failover_is_independent() {
    use super::multi::desired_leader;

    let root = std::env::temp_dir().join(format!("eg-w12-failover-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ports = free_ports(3);
    let n_groups = 3u64;
    let mut nodes: BTreeMap<NodeId, StartedNode> = BTreeMap::new();
    for i in 1..=3u64 {
        let dir = root.join(format!("node{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open_with_shards(
                dir.clone(),
                DurabilityPolicy::Each,
                4096,
                n_groups as usize,
            )
            .expect("open K==N sharded redb"),
        );
        let state = make_state_with_backend(&dir, backend).await;
        let started = node::start(cluster_cfg_with_groups(i, &ports, n_groups), state.clone())
            .await
            .expect("start raft node");
        state.write().await.raft = Some(started.handle.clone());
        nodes.insert(i, started);
    }

    // Wait for every group to elect, then rebalance so group g is led by node (g%3)+1:
    // desired_leader(1,[1,2,3]) == node 2, desired_leader(2,[1,2,3]) == node 3.
    map_group_leaders(&nodes, n_groups, Duration::from_secs(20)).await;
    assert_eq!(desired_leader(1, &[1, 2, 3]), Some(2));
    assert_eq!(desired_leader(2, &[1, 2, 3]), Some(3));
    let converged = wait_until(Duration::from_secs(30), || async {
        for n in nodes.values() {
            n.multi.rebalance_leaders().await;
        }
        let g1 = nodes[&2]
            .multi
            .group(1)
            .await
            .unwrap()
            .current_leader()
            .await
            == Some(2);
        let g2 = nodes[&3]
            .multi
            .group(2)
            .await
            .unwrap()
            .current_leader()
            .await
            == Some(3);
        g1 && g2
    })
    .await;
    converged.expect("groups 1/2 must converge to leaders on nodes 2/3");

    let router = nodes[&1].multi.router();
    // Pick a graph in group 1 (leader node 2) and one in group 2 (leader node 3).
    let pick = |want: u64| -> String {
        (0..10_000)
            .map(|i| format!("w12-fo-{i}"))
            .find(|g| router.group_of(g) == want)
            .expect("a graph routing to the wanted group")
    };
    let graph_g1 = pick(1);
    let graph_g2 = pick(2);

    // Baseline: both groups accept writes.
    write_via_node(&nodes, 2, &graph_g1, 0)
        .await
        .expect("baseline group-1 write");
    write_via_node(&nodes, 3, &graph_g2, 0)
        .await
        .expect("baseline group-2 write");

    // KILL group 1's leader node (node 2) fully — stop its listener AND shut down EVERY
    // group's raft on it (not just DEFAULT_GROUP via `handle`), else node 2 keeps
    // heart-beating as group 1's leader and its followers never time out. Group 2's
    // leader (node 3) is a different node, untouched.
    let killed = nodes.remove(&2).unwrap();
    killed.multi.stop_listener();
    for gid in killed.multi.known_groups().await {
        let _ = killed.multi.close_group(gid).await;
    }
    let _ = killed.handle.raft.shutdown().await;

    // ── KEY ASSERTION: group 2 keeps committing writes UNINTERRUPTED right through
    // group 1's failover — its leader (node 3) and quorum {1,3} are untouched.
    let g2_start = std::time::Instant::now();
    for seq in 1..=15u64 {
        write_via_node(&nodes, 3, &graph_g2, seq)
            .await
            .unwrap_or_else(|e| {
                panic!("group-2 write {seq} must proceed while group 1 fails over: {e}")
            });
    }
    let g2_elapsed = g2_start.elapsed();
    assert!(
        g2_elapsed < Duration::from_secs(10),
        "15 group-2 writes stalled ({g2_elapsed:?}) — group 1's failover leaked into group 2"
    );

    // Corroborate the killed group DOES recover independently: group 1 re-elects among
    // {1,3} and accepts a write on its NEW leader.
    let new_g1_leader = wait_until(Duration::from_secs(25), || async {
        for n in nodes.values() {
            if let Some(g) = n.multi.group(1).await {
                if matches!(g.current_leader().await, Some(l) if l != 2) {
                    return true;
                }
            }
        }
        false
    })
    .await;
    new_g1_leader.expect("group 1 must re-elect a surviving leader after node 2 dies");
    let mut recovered = false;
    for _ in 0..40 {
        let leader_node = {
            let mut found = None;
            for (nid, n) in nodes.iter() {
                if let Some(g) = n.multi.group(1).await {
                    if g.current_leader().await == Some(*nid) {
                        found = Some(*nid);
                        break;
                    }
                }
            }
            found
        };
        if let Some(node) = leader_node {
            if write_via_node(&nodes, node, &graph_g1, 1).await.is_ok() {
                recovered = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        recovered,
        "group 1 must accept writes again after independent failover"
    );

    tracing::info!(
        ?g2_elapsed,
        "ADR-2 W1.2 per-group failover independence verified"
    );
    for (_, n) in nodes {
        n.multi.stop_listener();
        let _ = n.handle.raft.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&root);
}
