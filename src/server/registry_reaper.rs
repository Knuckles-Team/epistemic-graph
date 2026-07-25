//! Stale-lease reaper for the fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5).
//!
//! `Method::RegisterServer` (`dispatch.rs`) writes/renews a `:Server` node in
//! `__commons__` carrying a `lease_expires_at_ms` the SERVER computed from its
//! own clock. A fleet server that stops renewing — it crashed, was killed, or
//! lost connectivity — leaves a stale row behind forever unless something
//! expires it. This sweep is that something: periodically (the engine's
//! existing interval-task cadence, `main.rs`, alongside cold-offload/memcap/
//! budget-enforcer — NO new daemon) it finds every `:Server` node whose lease
//! has lapsed and durably removes it, emitting the SAME `RemoveNode` CDC event
//! a client's own `Method::RemoveNode` call would (feeding the incident brain
//! within one sweep interval of the lease actually expiring).
//!
//! ## Why this bypasses the client-facing gateway
//!
//! This is engine-internal system maintenance, not a client request — no
//! caller identity, no signed envelope. It follows the SAME precedent as the
//! cold-offload sweep and the per-graph memory-cap eviction (`persist::
//! evict_oversized_all`): it reads `ServerState`'s registry/persistence
//! directly rather than minting a synthetic signed `Request` and re-entering
//! `dispatch()`. Unlike those two (which only ever evict resident RAM for
//! data ALREADY durable — never a new durable write), reaping a lease is a
//! genuine new durable mutation, so this module reproduces the commit
//! ordering `server::mutation::commit_finalize` uses for a REAL `RemoveNode`
//! (apply in-memory, durably record, then CDC-emit) — reusing `RemoveNode`'s
//! ALREADY-correct `mutation_apply`/`cdc` classification byte-for-byte, just
//! DURABLE-FIRST: unlike the client gateway (which applies in-memory before
//! the durable record, tolerable there because a failed durable commit
//! surfaces as an error response the caller can retry), this sweep has no
//! caller to report a partial failure to, so it durably records FIRST and
//! only applies in-memory / emits CDC once that durably succeeds — a durable
//! failure leaves BOTH images (memory and disk) untouched, self-healing on
//! the next sweep instead of risking divergence.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Method;
use crate::server::ServerState;

/// Default sweep interval, seconds (`EPISTEMIC_GRAPH_SERVER_REGISTRY_REAP_SECS`
/// overrides). Short enough that even the minimum `RegisterServer.ttl_secs`
/// lease (1s) is reaped within a small, bounded number of sweeps — the cost is
/// one bounded scan of `__commons__`'s resident `:Server` nodes (never the
/// whole graph), so a short interval is cheap.
const DEFAULT_REAP_INTERVAL_SECS: u64 = 15;

/// The configured positive sweep interval.
pub fn reap_interval_secs() -> u64 {
    std::env::var("EPISTEMIC_GRAPH_SERVER_REGISTRY_REAP_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REAP_INTERVAL_SECS)
}

/// One `:Server` node's lease state, decoded from its property blob.
fn is_expired_server_node(blob: &[u8], now_ms: u64) -> Option<bool> {
    let value = eg_types::msgpack::decode_property_value(blob).ok()?;
    if value.get("node_type").and_then(|v| v.as_str()) != Some("Server") {
        return None; // not a fleet-registry row -- never a reap candidate
    }
    let lease_expires_at_ms = value.get("lease_expires_at_ms").and_then(|v| v.as_u64())?;
    Some(lease_expires_at_ms <= now_ms)
}

/// Sweep `__commons__` for `:Server` nodes whose lease has lapsed at/before
/// `now_ms`, durably removing each and emitting a `RemoveNode` CDC event.
/// Returns the number reaped. Best-effort + fail-safe by construction: a
/// non-`Server` node, or a `Server` row with no/unparseable lease field (e.g.
/// hand-corrupted by an operator), is never touched here — repairing a
/// malformed row is the au reconciler's job, not this sweep's. A durable
/// write failure for one node is logged and skipped; the next sweep retries.
pub async fn reap_expired_servers(state: &Arc<RwLock<ServerState>>, now_ms: u64) -> u64 {
    let (core, backend) = {
        let s = state.read().await;
        let Some(entry) = s.registry.get("__commons__") else {
            return 0;
        };
        (entry.core.clone(), s.persistence.clone())
    };
    let Some(backend) = backend else {
        return 0; // no durable backend attached -- nothing to reap durably
    };

    let candidates: Vec<String> = core
        .get_nodes()
        .into_iter()
        .filter_map(|(node_id, _)| {
            if !node_id.starts_with("srv:") {
                return None;
            }
            let blob = core.get_node_properties(&node_id)?;
            is_expired_server_node(&blob, now_ms)
                .unwrap_or(false)
                .then_some(node_id)
        })
        .collect();
    if candidates.is_empty() {
        return 0;
    }

    #[cfg(feature = "streaming")]
    let hub = { state.read().await.cdc.clone() };
    let fname = crate::persist::sanitize("__commons__");
    let mut reaped = 0u64;
    for node_id in candidates {
        // Serialize against a concurrent RegisterServer renewal of the SAME node
        // (the same per-graph lane every routed mutation takes) and re-check
        // under the lock: a heartbeat may have renewed the lease since the scan.
        let _guard = crate::server::mutation_batch::lock_graph("__commons__").await;
        let still_expired = core
            .get_node_properties(&node_id)
            .and_then(|blob| is_expired_server_node(&blob, now_ms))
            .unwrap_or(false);
        if !still_expired {
            continue;
        }

        let method = Method::RemoveNode {
            node_id: node_id.clone(),
        };
        #[cfg(feature = "streaming")]
        let pre = crate::server::cdc::capture_before(&core, &method);

        // Durable-first (see module docs): only touch the in-memory core / emit
        // CDC once the durable record is confirmed.
        match backend.record_durable(&fname, &method).await {
            Ok(()) => {
                core.remove_node(node_id.clone());
                core.mark_dirty();
                #[cfg(feature = "streaming")]
                if let Some(hub) = &hub {
                    crate::server::cdc::emit_for_method(hub, &core, "__commons__", &method, pre);
                }
                reaped += 1;
                tracing::info!(
                    node_id = %node_id,
                    "stale-lease reaper expired fleet server registration (CONCEPT:EG-KG.sharding.server-registry, W2.5)"
                );
            }
            Err(error) => {
                tracing::warn!(
                    node_id = %node_id,
                    %error,
                    "stale-lease reaper failed to durably expire fleet server registration; retrying next sweep"
                );
            }
        }
    }
    reaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::channels::ChannelManager;
    use crate::durability::DurabilityPolicy;
    use crate::isolation::IsolationLayer;
    use crate::protocol::Request;
    use crate::registry::GraphRegistry;
    use crate::server::persistence::read_through::{
        BackendGraphMaterializer, BackendReadThroughFactory,
    };
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use crate::server::{compute_verified_envelope_token, dispatch, VerifiedEnvelopeParams};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::Semaphore;

    const SECRET: &str = "registry-reaper-test";
    const TEST_AGENT: &str = "unit-test-agent";
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn current_isolation() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation
    }

    async fn redb_state(dir_s: &str) -> Arc<RwLock<ServerState>> {
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir_s.to_string(), DurabilityPolicy::Each, 64).expect("open"),
        );
        let state = Arc::new(RwLock::new(ServerState {
            cold_tracker: Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.to_string()),
            persistence: Some(backend.clone()),
            max_in_flight: Arc::new(Semaphore::new(64)),
            read_admission: Arc::new(Semaphore::new(64)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 32,
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
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }));
        {
            let mut s = state.write().await;
            let rt_factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
            s.registry.set_read_through_factory(rt_factory);
            let materializer = Arc::new(BackendGraphMaterializer::new(backend.clone()));
            s.registry.set_materializer(materializer);
            // `GraphRegistry::new()` already seeds `__commons__` in-memory; wiring
            // the read-through/materializer above is what makes it durable too.
        }
        state
    }

    fn req(id: u64, graph: &str, method: Method) -> Request {
        std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
        std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
        std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
        std::env::set_var(
            "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
            std::env::temp_dir().join(format!(
                "epistemic-graph-registry-reaper-unit-auth-{}",
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
        let mut request = Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "registry-reaper-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("registry-reaper-request-{id}-{sequence}");
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

    async fn register(state: &Arc<RwLock<ServerState>>, id: u64, name: &str, ttl_secs: u64) {
        let r = dispatch(
            state,
            req(
                id,
                "__commons__",
                Method::RegisterServer {
                    name: name.to_string(),
                    url: "mcp-ref://deadbeef".to_string(),
                    resources_json: String::new(),
                    ttl_secs,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "register {name}: {:?}", r.error);
    }

    /// A registration whose lease has NOT lapsed is left untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_lease_is_not_reaped() {
        let dir = std::env::temp_dir().join(format!("eg-reaper-live-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        register(&state, 1, "alive-server", 3_600).await;

        let now_ms = crate::server::txn::now_ms();
        let reaped = reap_expired_servers(&state, now_ms).await;
        assert_eq!(reaped, 0, "a live lease must never be reaped");

        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(
            core.get_node_properties("srv:alive-server").is_some(),
            "the node must still exist"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A registration whose TTL has elapsed is durably removed and a CDC
    /// `RemoveNode` event is observable on `__commons__`'s feed -- the
    /// acceptance-criteria proof for "killing one surfaces a lease-expiry CDC
    /// event within TTL".
    #[tokio::test(flavor = "multi_thread")]
    async fn expired_lease_is_reaped_and_emits_cdc() {
        let dir = std::env::temp_dir().join(format!("eg-reaper-expired-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        // The minimum allowed TTL (1s) -- register, then sweep "later".
        register(&state, 1, "dead-server", 1).await;
        let cdc_seq_before = {
            let s = state.read().await;
            s.cdc.as_ref().unwrap().head_seq("__commons__")
        };

        // Simulate the lease having elapsed (a real deployment just waits;
        // here we sweep against a synthetic "now" past the 1s TTL instead of
        // sleeping, keeping the test fast and deterministic).
        let now_ms = crate::server::txn::now_ms() + 2_000;
        let reaped = reap_expired_servers(&state, now_ms).await;
        assert_eq!(reaped, 1, "the single expired registration must be reaped");

        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(
            core.get_node_properties("srv:dead-server").is_none(),
            "the expired node must be durably removed"
        );

        // CDC proof: a RemoveNode event landed on __commons__'s feed after the sweep.
        let events = {
            let s = state.read().await;
            s.cdc
                .as_ref()
                .unwrap()
                .read("__commons__", cdc_seq_before, 0)
                .expect("cdc read")
        };
        assert!(
            events
                .iter()
                .any(|e| e.node_id == "srv:dead-server"
                    && e.kind == crate::wire::CdcKind::RemoveNode),
            "a RemoveNode CDC event for the expired server must be observable: {events:?}"
        );

        // Durability proof: the AUTHORITATIVE durable backend (not the in-memory
        // core) confirms the row is gone on disk too -- read directly off the
        // live backend (the same `durable_node_presence` check
        // `offload_graph_core`/eviction rely on), rather than a full
        // process-level close+reopen (which races the writer thread's own
        // async shutdown outside this single-threaded test's control).
        let backend = { state.read().await.persistence.clone().unwrap() };
        let presence = backend
            .durable_node_presence(
                &crate::persist::sanitize("__commons__"),
                &["srv:dead-server".to_string()],
            )
            .expect("presence check");
        assert_eq!(
            presence,
            vec![false],
            "the durable removal must be confirmed by the authoritative backend"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `:Server` row whose lease has lapsed but that was ALSO just renewed
    /// (a heartbeat racing the sweep's own decision) survives -- the guard's
    /// re-check under the per-graph lock, not the initial scan, is
    /// authoritative.
    #[tokio::test(flavor = "multi_thread")]
    async fn renewal_racing_the_sweep_is_not_reaped() {
        let dir = std::env::temp_dir().join(format!("eg-reaper-race-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        register(&state, 1, "racy-server", 1).await;
        // Renew BEFORE the sweep runs (simulates a heartbeat landing first) --
        // this call rewrites `lease_expires_at_ms` off the SAME "now".
        register(&state, 2, "racy-server", 3_600).await;

        let now_ms = crate::server::txn::now_ms() + 2_000;
        let reaped = reap_expired_servers(&state, now_ms).await;
        assert_eq!(reaped, 0, "the renewed lease must not be reaped");

        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(core.get_node_properties("srv:racy-server").is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A hand-corrupted `:Server` row (missing/invalid `lease_expires_at_ms`)
    /// is left untouched by the reaper -- repairing it is the au reconciler's
    /// job, never a false reap.
    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_lease_field_is_never_reaped() {
        let dir = std::env::temp_dir().join(format!("eg-reaper-malformed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        let r = dispatch(
            &state,
            req(
                1,
                "__commons__",
                Method::AddNode {
                    node_id: "srv:hand-broken".to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "node_type": "Server",
                        "name": "hand-broken",
                        // lease_expires_at_ms deliberately absent (operator wiped it).
                    }))
                    .unwrap(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none());

        let now_ms = crate::server::txn::now_ms() + 1_000;
        let reaped = reap_expired_servers(&state, now_ms).await;
        assert_eq!(reaped, 0, "a malformed lease row must never be reaped");

        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(core.get_node_properties("srv:hand-broken").is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Acceptance-criteria proof (W2.5): "several simulated servers register +
    /// heartbeat; killing one surfaces a lease-expiry CDC event within TTL" --
    /// multiple INDEPENDENT servers (no shared identity, no au/graph-os
    /// config-sync involved) register directly against the engine; letting
    /// exactly one server's lease lapse (simulating it being killed and never
    /// heartbeating again) reaps ONLY that one, leaves the still-heartbeating
    /// others live, and the CDC event is scoped to the dead server's node id.
    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_independent_servers_one_kill_reaps_only_that_one() {
        let dir = std::env::temp_dir().join(format!("eg-reaper-multi-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        // Three independent fleet servers self-register, each with its own
        // name/TTL -- exactly as `mcp/server_factory.py`'s lifespan wires every
        // server to do, with ZERO shared coordination between them.
        register(&state, 1, "graph-os", 3_600).await;
        register(&state, 2, "portainer-agent-mcp", 3_600).await;
        register(&state, 3, "about-to-die-mcp", 1).await; // the one that gets "killed"

        let cdc_seq_before = {
            let s = state.read().await;
            s.cdc.as_ref().unwrap().head_seq("__commons__")
        };

        // "about-to-die-mcp" is killed -- it never renews. The other two keep
        // heartbeating well inside their TTL (unaffected by the sweep below).
        let now_ms = crate::server::txn::now_ms() + 2_000;
        let reaped = reap_expired_servers(&state, now_ms).await;
        assert_eq!(reaped, 1, "exactly the one dead server must be reaped");

        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(
            core.get_node_properties("srv:graph-os").is_some(),
            "a live, unrelated server must survive another server's expiry"
        );
        assert!(
            core.get_node_properties("srv:portainer-agent-mcp")
                .is_some(),
            "a live, unrelated server must survive another server's expiry"
        );
        assert!(
            core.get_node_properties("srv:about-to-die-mcp").is_none(),
            "the killed server must be gone"
        );

        // The CDC event is scoped to EXACTLY the dead server -- the incident
        // brain learns which server died, not merely that a reap happened.
        let events = {
            let s = state.read().await;
            s.cdc
                .as_ref()
                .unwrap()
                .read("__commons__", cdc_seq_before, 0)
                .expect("cdc read")
        };
        let removed: Vec<&str> = events
            .iter()
            .filter(|e| e.kind == crate::wire::CdcKind::RemoveNode)
            .map(|e| e.node_id.as_str())
            .collect();
        assert_eq!(
            removed,
            vec!["srv:about-to-die-mcp"],
            "exactly one RemoveNode event, for the killed server only: {events:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Acceptance-criteria proof (W2.5): "the reconciler repairs a hand-broken
    /// registry entry." `RegisterServer` is the SAME idempotent-upsert RPC
    /// both a self-registering server's heartbeat AND au's
    /// `_reconcile_server_registration` call — this proves calling it again
    /// for a name whose row was hand-corrupted (wrong url, stale/molested
    /// fields, as an operator directly poking the graph might do) fully
    /// restores a correct row, not a partial merge of old-broken +
    /// new-correct fields.
    #[tokio::test(flavor = "multi_thread")]
    async fn register_server_repairs_a_hand_broken_entry() {
        let dir = std::env::temp_dir().join(format!("eg-reaper-repair-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        // A legitimate registration first ...
        register(&state, 1, "graph-os", 3_600).await;

        // ... then an operator (or a bug) hand-corrupts the row directly: wrong
        // url, a garbage lease far in the past, and a bogus extra field.
        let r = dispatch(
            &state,
            req(
                2,
                "__commons__",
                Method::CompareAndSetNodeFields {
                    node_id: "srv:graph-os".to_string(),
                    conditions_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
                    updates_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "url": "http://hand-edited-wrong-endpoint.invalid",
                        "lease_expires_at_ms": 1u64,
                        "hand_broken_marker": "should not survive repair",
                    }))
                    .unwrap(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "corrupt the row: {:?}", r.error);
        let broken = {
            let s = state.read().await;
            s.registry
                .get("__commons__")
                .unwrap()
                .core
                .get_node_properties("srv:graph-os")
                .and_then(|blob| eg_types::msgpack::decode_property_value(&blob).ok())
                .expect("broken row exists")
        };
        assert_eq!(
            broken.get("url").and_then(|v| v.as_str()),
            Some("http://hand-edited-wrong-endpoint.invalid")
        );

        // The reconciler repairs it -- a plain re-`register()` call, exactly
        // what `_reconcile_server_registration` (au) issues.
        register(&state, 3, "graph-os", 3_600).await;

        let repaired = {
            let s = state.read().await;
            s.registry
                .get("__commons__")
                .unwrap()
                .core
                .get_node_properties("srv:graph-os")
                .and_then(|blob| eg_types::msgpack::decode_property_value(&blob).ok())
                .expect("repaired row exists")
        };
        assert_eq!(
            repaired.get("url").and_then(|v| v.as_str()),
            Some("mcp-ref://deadbeef"),
            "the corrupted url must be fully overwritten, not merged"
        );
        assert!(
            repaired.get("hand_broken_marker").is_none(),
            "a corrupted extra field must not survive repair"
        );
        let lease_expires_at_ms = repaired
            .get("lease_expires_at_ms")
            .and_then(|v| v.as_u64())
            .expect("lease field present");
        assert!(
            lease_expires_at_ms > crate::server::txn::now_ms(),
            "the repaired lease must be live again, not the hand-corrupted past timestamp"
        );

        // And the repaired row is no longer a reap candidate.
        let now_ms = crate::server::txn::now_ms();
        let reaped = reap_expired_servers(&state, now_ms).await;
        assert_eq!(reaped, 0, "a freshly-repaired row must not be reaped");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
