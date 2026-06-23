// CONCEPT:KG-2.19 — Tokio Service Server
//
// Long-running Tokio server that holds the GraphRegistry in memory
// and serves requests over UDS or TCP with HMAC-SHA256 authentication.

mod access;
mod auth;
mod compute;
mod dispatch;
mod handlers;
pub mod persistence;
mod state;
mod transport;

// External path surface — `server::ServerState`, `server::MAX_BATCH_IDS`,
// `server::compute_auth_token`, `server::dispatch`, and
// `server::{handle_connection,serve_uds,serve_tcp}` — used by main.rs/persist.rs/tests.
pub use auth::compute_auth_token;
pub use dispatch::dispatch;
pub use persistence::PersistenceBackend;
pub use state::{ServerState, MAX_BATCH_IDS};
pub use transport::{handle_connection, serve_tcp};
// serve_uds is Unix-only (UnixListener); on Windows the server uses serve_tcp,
// so gate the re-export to keep the windows-msvc wheel building (main.rs already
// guards its serve_uds call with #[cfg(unix)]).
#[cfg(unix)]
pub use transport::serve_uds;

#[cfg(test)]
mod tests {
    use super::compute::weight_semantic_results;
    use super::*;
    use crate::channels::ChannelManager;
    use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
    use crate::protocol::{GraphType, Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "dispatch-test-secret";

    fn test_state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
        }))
    }

    /// State with worker1/worker2 (team alpha) + their manager registered, and
    /// per-agent + team graphs created with real owners.
    async fn multi_tenant_state() -> Arc<RwLock<ServerState>> {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.isolation.register_agent(AgentIdentity {
                agent_id: "manager".into(),
                role: AgentRole::Manager {
                    subordinates: vec!["worker1".into(), "worker2".into()],
                },
                teams: vec!["alpha".into()],
            });
            for w in ["worker1", "worker2"] {
                s.isolation.register_agent(AgentIdentity {
                    agent_id: w.into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                });
            }
            s.registry
                .create_graph("agent:worker1", GraphType::Agent, Some("worker1".into()))
                .unwrap();
            s.registry
                .create_graph("team:alpha", GraphType::Team, Some("manager".into()))
                .unwrap();
            s.registry
                .create_graph("global:ontology", GraphType::Global, None)
                .unwrap();
        }
        state
    }

    fn request(id: u64, graph: &str, agent_id: Option<&str>, method: Method) -> Request {
        Request {
            id,
            graph: graph.to_string(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: agent_id.map(str::to_string),
            method,
        }
    }

    fn add_node(node_id: &str) -> Method {
        Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
        }
    }

    fn assert_denied(resp: &Response) {
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.starts_with("ACCESS_DENIED"),
            "expected ACCESS_DENIED, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    fn assert_ok(resp: &Response) {
        assert!(
            resp.error.is_none(),
            "expected success, got error: {:?}",
            resp.error
        );
    }

    // ── Cross-graph union reads (CONCEPT:KG-2.171) ──────────────────────

    #[tokio::test]
    async fn test_union_read_across_graphs() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("__ingest__", GraphType::Global, None)
                .unwrap();
        }
        // Node A lives in __commons__, node B lives ONLY in __ingest__.
        let mk = |id: &str, name: &str| Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec(
                &serde_json::json!({"type": "Doc", "name": name}),
            )
            .unwrap(),
        };
        assert_ok(&dispatch(&state, request(1, "__commons__", None, mk("A", "alpha"))).await);
        assert_ok(&dispatch(&state, request(2, "__ingest__", None, mk("B", "beta"))).await);

        let graphs = vec!["__commons__".to_string(), "__ingest__".to_string()];

        // A single-graph read of __commons__ does NOT see B (proves the union does work).
        let single = dispatch(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::GetNodeProperties {
                    node_id: "B".into(),
                },
            ),
        )
        .await;
        assert!(
            matches!(
                single.result,
                Some(ResultPayload::Json(serde_json::Value::Null))
            ),
            "B must be absent from __commons__ alone, got {:?}",
            single.result
        );

        // Union point read finds B (which lives only in __ingest__).
        let up = dispatch(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::UnionGetNodeProperties {
                    graphs: graphs.clone(),
                    node_id: "B".into(),
                },
            ),
        )
        .await;
        assert_ok(&up);
        assert!(
            matches!(up.result, Some(ResultPayload::PropertiesMsgpack(_))),
            "union point read must find B across graphs, got {:?}",
            up.result
        );

        // Union label scan sees BOTH graphs, deduped by id.
        let ul = dispatch(
            &state,
            request(
                5,
                "__commons__",
                None,
                Method::UnionGetNodesByLabel {
                    graphs: graphs.clone(),
                    label: "Doc".into(),
                    limit: 0,
                },
            ),
        )
        .await;
        assert_ok(&ul);
        match ul.result {
            Some(ResultPayload::NodeList(nodes)) => {
                let ids: std::collections::HashSet<String> =
                    nodes.iter().map(|(k, _)| k.clone()).collect();
                assert!(
                    ids.contains("A") && ids.contains("B"),
                    "union label scan must union both graphs, got {:?}",
                    ids
                );
            }
            other => panic!("expected NodeList, got {:?}", other),
        }

        // A missing lane graph in the set is skipped (no error), still returns __commons__'s A.
        let with_missing = vec![
            "__commons__".to_string(),
            "__ingest_does_not_exist__".to_string(),
        ];
        let um = dispatch(
            &state,
            request(
                6,
                "__commons__",
                None,
                Method::UnionGetNodeProperties {
                    graphs: with_missing,
                    node_id: "A".into(),
                },
            ),
        )
        .await;
        assert_ok(&um);
        assert!(matches!(
            um.result,
            Some(ResultPayload::PropertiesMsgpack(_))
        ));
    }

    #[tokio::test]
    async fn test_bad_auth_token_rejected() {
        let state = test_state();
        let mut req = request(1, "__commons__", None, Method::Ping);
        req.auth_token = "bogus".to_string();
        let resp = dispatch(&state, req).await;
        assert_eq!(resp.error.as_deref(), Some("Authentication failed"));
    }

    /// Feature-gating contract: a gated-out domain's Method variant still exists
    /// in the wire enum, but with the feature off its handler arm is compiled
    /// away — the request must hit the explicit "not available in this build"
    /// catch-all, never panic or silently route elsewhere. `reasoning` is off in
    /// this build, so `RunDatalogReasoning` exercises the gate. (Compiled out
    /// when `reasoning` is enabled, where the real handler answers instead.)
    #[cfg(not(feature = "reasoning"))]
    #[tokio::test]
    async fn test_gated_out_method_returns_not_built() {
        let state = multi_tenant_state().await;
        let method = Method::RunDatalogReasoning {
            subclass_relations: vec![],
            subproperty_relations: vec![],
            symmetric_properties: vec![],
            transitive_properties: vec![],
            inverse_properties: vec![],
            domain_rules: vec![],
            range_rules: vec![],
            property_chains: vec![],
        };
        let resp = dispatch(&state, request(1, "agent:worker1", Some("worker1"), method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    #[tokio::test]
    async fn incremental_checkpoint_skips_clean_graphs() {
        // Phase C-C: checkpoint_all rewrites only graphs dirtied since the last
        // checkpoint. Return value is the number WRITTEN (clean graphs skipped).
        let dir = std::env::temp_dir().join(format!("eg-cc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir.to_string_lossy().to_string()),
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
        }));

        // __commons__ starts dirty → the first checkpoint writes exactly it.
        assert_eq!(
            crate::persist::checkpoint_all(&state, None).await.unwrap(),
            1
        );
        // Nothing changed → the next checkpoint writes nothing (all clean/skipped).
        assert_eq!(
            crate::persist::checkpoint_all(&state, None).await.unwrap(),
            0
        );

        // A successful write through dispatch marks the graph dirty (no isolation
        // rules registered → back-compat permits the write).
        assert_ok(&dispatch(&state, request(1, "__commons__", None, add_node("x"))).await);
        assert_eq!(
            crate::persist::checkpoint_all(&state, None).await.unwrap(),
            1
        );
        assert_eq!(
            crate::persist::checkpoint_all(&state, None).await.unwrap(),
            0
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn wal_service_logs_dispatch_then_checkpoint_truncates() {
        // Phase B3 end-to-end: a durable mutation dispatched through the server is
        // appended to the WAL by the OFF-REACTOR writer (no file I/O on this task),
        // replays into a fresh graph, and a checkpoint truncates the WAL prefix it
        // supersedes.
        let dir = std::env::temp_dir().join(format!("eg-b3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();
        let svc = crate::wal_service::WalService::spawn(
            dir_s.clone(),
            crate::wal_service::FsyncPolicy::Each,
            64,
        );
        // The default backend owns the WAL writer; keep a clone of `svc` here to
        // assert directly on the off-reactor writer's position/dropped counters.
        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> = Arc::new(
            crate::server::persistence::snapshot_wal::SnapshotWalBackend::new(Some(svc.clone())),
        );
        let state = Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.clone()),
            persistence: Some(backend.clone()),
            max_in_flight: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
        }));

        assert_ok(&dispatch(&state, request(1, "__commons__", None, add_node("x"))).await);
        // position() is processed in-order AFTER the append by the single writer
        // thread, so a non-zero result proves the off-reactor append landed.
        assert!(
            svc.position("__commons__") > 0,
            "dispatch should have logged to WAL"
        );
        assert_eq!(svc.dropped(), 0);

        // The WAL alone recovers the mutation into a fresh graph.
        let fresh = crate::graph::GraphCore::new();
        let replayed = crate::wal::replay(&fresh, &crate::wal::wal_path(&dir_s, "__commons__"));
        assert_eq!(replayed, 1);
        assert!(fresh.get_node_properties("x").is_some());

        // Checkpoint writes the snapshot and truncates the WAL prefix it covers.
        assert_eq!(backend.checkpoint_all(&state).await.unwrap(), 1);
        assert_eq!(
            svc.position("__commons__"),
            0,
            "WAL truncated after checkpoint"
        );
        assert!(std::path::Path::new(&dir_s).join("__commons__.mp").exists());

        svc.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn legacy_bus_snapshot_migrates_to_commons() {
        // C3: an engine that persisted the old `__bus__` commons graph must load it
        // under the new `__commons__` name via the one-time on-disk migration.
        let dir = std::env::temp_dir().join(format!("eg-busmig-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();

        // Forge a legacy snapshot: a `__bus__.mp` + a manifest naming it `__bus__`
        // with the old `"Bus"` graph_type.
        let legacy = crate::graph::GraphCore::new();
        legacy.add_node(
            "legacy_node".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"type": "Code"})).unwrap(),
        );
        let bytes = legacy.snapshot().to_msgpack().unwrap();
        std::fs::write(dir.join("__bus__.mp"), &bytes).unwrap();
        std::fs::write(
            dir.join("manifest.json"),
            br#"{"__bus__":{"name":"__bus__","graph_type":"Bus"}}"#,
        )
        .unwrap();

        let state = Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.clone()),
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
        }));

        crate::persist::load_all(&state, None).await.unwrap();

        // The legacy data now lives under __commons__; the old files are gone.
        let resp = dispatch(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::HasNode {
                    node_id: "legacy_node".into(),
                },
            ),
        )
        .await;
        assert!(
            matches!(resp.result, Some(ResultPayload::Bool(true))),
            "legacy node must be present under __commons__: {:?}",
            resp.error
        );
        assert!(
            !dir.join("__bus__.mp").exists(),
            "legacy snapshot renamed away"
        );
        assert!(
            dir.join("__commons__.mp").exists(),
            "migrated snapshot present"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn memory_cap_evicts_graphs_over_cap() {
        // E3: a graph above the per-graph cap is evicted (LRU) back down to it;
        // under the cap, or cap 0, is a no-op.
        let state = test_state();
        for i in 0..6 {
            assert_ok(
                &dispatch(
                    &state,
                    request(1, "__commons__", None, add_node(&format!("n{i}"))),
                )
                .await,
            );
        }
        assert_eq!(
            crate::persist::evict_oversized_all(&state, 4).await,
            2,
            "6 nodes capped at 4 -> evict 2"
        );
        assert_eq!(crate::persist::evict_oversized_all(&state, 4).await, 0);
        assert_eq!(crate::persist::evict_oversized_all(&state, 100).await, 0);
        assert_eq!(crate::persist::evict_oversized_all(&state, 0).await, 0);
    }

    #[tokio::test]
    async fn batch_node_reads_collapse_round_trips() {
        // A2: GetNodePropertiesBatch / HasNodesBatch fetch N nodes in one request.
        let state = test_state();
        for (id, k) in [("a", 1), ("b", 2)] {
            let m = Method::AddNode {
                node_id: id.into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({ "k": k }))
                    .unwrap(),
            };
            assert_ok(&dispatch(&state, request(1, "__commons__", None, m)).await);
        }

        let resp = dispatch(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::GetNodePropertiesBatch {
                    node_ids: vec!["a".into(), "missing".into(), "b".into()],
                },
            ),
        )
        .await;
        let raw = match resp.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?} (err={:?})", resp.error),
        };
        let rows: Vec<(String, Option<serde_bytes::ByteBuf>)> =
            rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "a");
        assert!(rows[0].1.is_some(), "present node returns properties");
        assert_eq!(rows[1].0, "missing");
        assert!(rows[1].1.is_none(), "absent id returns nil");
        let a_props: serde_json::Value =
            rmp_serde::from_slice(rows[0].1.as_ref().unwrap()).unwrap();
        assert_eq!(a_props["k"], 1);

        let resp = dispatch(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::HasNodesBatch {
                    node_ids: vec!["a".into(), "missing".into()],
                },
            ),
        )
        .await;
        let raw = match resp.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let flags: Vec<bool> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(flags, vec![true, false]);

        // Oversize batches are rejected, not truncated (OOM guard).
        let resp = dispatch(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::GetNodePropertiesBatch {
                    node_ids: vec!["x".to_string(); MAX_BATCH_IDS + 1],
                },
            ),
        )
        .await;
        assert!(resp.error.is_some(), "oversize batch must be rejected");
    }

    #[tokio::test]
    async fn per_graph_backpressure_isolates_tenants() {
        // Phase C-D: a hot graph that has exhausted its per-graph in-flight cap is
        // shed with BUSY, but OTHER graphs keep being served from the (ample) global
        // pool — one tenant cannot starve the rest.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn round_trip(s: &mut tokio::io::DuplexStream, req: &Request) -> Response {
            let payload = rmp_serde::to_vec_named(req).unwrap();
            s.write_all(&(payload.len() as u32).to_be_bytes())
                .await
                .unwrap();
            s.write_all(&payload).await.unwrap();
            let mut lb = [0u8; 4];
            s.read_exact(&mut lb).await.unwrap();
            let n = u32::from_be_bytes(lb) as usize;
            let mut buf = vec![0u8; n];
            s.read_exact(&mut buf).await.unwrap();
            rmp_serde::from_slice(&buf).unwrap()
        }

        let state = Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(64)), // global: ample
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 1, // any one graph: a single slot
        }));

        // Pre-seed g_hot's per-graph semaphore and hold its only permit, simulating
        // an op already in flight on that graph.
        let hot_sem = Arc::new(Semaphore::new(1));
        state
            .read()
            .await
            .per_graph_inflight
            .insert("g_hot".into(), hot_sem.clone());
        let _held = hot_sem.try_acquire_owned().unwrap();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let st = state.clone();
        let handle = tokio::spawn(async move { handle_connection(server, st).await });

        // g_hot is saturated → BUSY at the graph level.
        let r_hot = round_trip(&mut client, &request(1, "g_hot", None, Method::Ping)).await;
        assert!(
            r_hot
                .error
                .as_deref()
                .unwrap_or("")
                .contains("graph at capacity"),
            "hot graph must be shed, got {:?}",
            r_hot
        );

        // g_cold is independent → served normally despite g_hot being saturated.
        let r_cold = round_trip(&mut client, &request(2, "g_cold", None, Method::Ping)).await;
        assert!(
            r_cold.error.is_none(),
            "cold graph must NOT be starved by the hot graph, got {:?}",
            r_cold
        );

        drop(client);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_no_isolation_rules_is_back_compat() {
        // No identities registered → no rules → any caller (even anonymous)
        // can write to any graph, exactly as before enforcement existed.
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:worker1", GraphType::Agent, Some("worker1".into()))
                .unwrap();
        }
        let resp = dispatch(&state, request(1, "agent:worker1", None, add_node("n1"))).await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(2, "agent:worker1", Some("worker2"), add_node("n2")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_owner_can_write_own_graph() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "agent:worker1", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_peer_denied_read_and_write() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "agent:worker1", Some("worker2"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
        let resp = dispatch(
            &state,
            request(2, "agent:worker1", Some("worker2"), Method::GetNodes),
        )
        .await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_anonymous_denied_when_rules_exist() {
        let state = multi_tenant_state().await;
        let resp = dispatch(&state, request(1, "agent:worker1", None, Method::GetNodes)).await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_manager_reaches_subordinate_graph() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "agent:worker1", Some("manager"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_team_member_read_only() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "team:alpha", Some("worker1"), Method::GetNodes),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(2, "team:alpha", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
        let resp = dispatch(
            &state,
            request(3, "team:alpha", Some("manager"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_global_graph_read_only() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "global:ontology", Some("worker1"), Method::GetNodes),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(2, "global:ontology", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_bus_stays_open_to_all() {
        let state = multi_tenant_state().await;
        for (id, agent) in [(1, Some("worker1")), (2, Some("worker2")), (3, None)] {
            let resp = dispatch(
                &state,
                request(id, "__commons__", agent, add_node(&format!("n{}", id))),
            )
            .await;
            assert_ok(&resp);
        }
    }

    #[tokio::test]
    async fn test_create_graph_records_caller_as_owner() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(
                1,
                "__commons__",
                Some("worker2"),
                Method::CreateGraph {
                    graph_name: "agent:worker2".to_string(),
                    graph_type: GraphType::Agent,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        {
            let s = state.read().await;
            assert_eq!(
                s.registry.get("agent:worker2").unwrap().owner.as_deref(),
                Some("worker2")
            );
        }
        // Owner writes fine; the peer is denied.
        let resp = dispatch(
            &state,
            request(2, "agent:worker2", Some("worker2"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(3, "agent:worker2", Some("worker1"), add_node("n2")),
        )
        .await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_delete_graph_requires_write_access() {
        let state = multi_tenant_state().await;
        let del = || Method::DeleteGraph {
            graph_name: "agent:worker1".to_string(),
        };
        let resp = dispatch(&state, request(1, "__commons__", Some("worker2"), del())).await;
        assert_denied(&resp);
        let resp = dispatch(&state, request(2, "__commons__", Some("worker1"), del())).await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_channel_operations_unaffected_by_rules() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(
                1,
                "__commons__",
                Some("worker1"),
                Method::CreateChannel {
                    channel_id: "channel:p2p:worker1:worker2".to_string(),
                    channel_type: crate::protocol::ChannelType::PeerToPeer,
                    creator: "worker1".to_string(),
                    initial_members: vec!["worker1".to_string(), "worker2".to_string()],
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(
                2,
                "__commons__",
                Some("worker2"),
                Method::SendMessage {
                    channel_id: "channel:p2p:worker1:worker2".to_string(),
                    sender: "worker2".to_string(),
                    payload: "hello".to_string(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
    }

    // ── Lock-free compute (CONCEPT:KG-2.51) ─────────────────────────────

    fn json_props(val: serde_json::Value) -> Option<Vec<u8>> {
        // SemanticSearch's decay path reads properties as a UTF-8 JSON string
        // (NodeData::from_json_props), so encode candidates the same way.
        Some(val.to_string().into_bytes())
    }

    #[test]
    fn test_weight_semantic_results_orders_decays_and_truncates() {
        let now = 100_000_000u64;
        let thirty_days = 30 * 86_400u64;
        let candidates = vec![
            // Fresh fact: confidence 1.0, no decay → keeps raw similarity.
            (
                "fresh".to_string(),
                0.8f32,
                json_props(serde_json::json!({"type": "Fact", "valid_from": now})),
            ),
            // One half-life old: 0.9 similarity decays to ~0.45 → ranks below.
            (
                "aged".to_string(),
                0.9f32,
                json_props(serde_json::json!({"type": "Fact", "valid_from": now - thirty_days})),
            ),
            // Validity window closed → filtered out entirely.
            (
                "stale".to_string(),
                0.99f32,
                json_props(serde_json::json!({"type": "Fact", "valid_until": now - 1})),
            ),
            // No properties → similarity passes through unweighted.
            ("bare".to_string(), 0.5f32, None),
        ];

        let out = weight_semantic_results(candidates, now, 10);
        let ids: Vec<&str> = out.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["fresh", "bare", "aged"]);
        let aged_score = out[2].1;
        assert!(
            (aged_score - 0.45).abs() < 0.01,
            "expected ~0.45 after one half-life, got {aged_score}"
        );

        // Truncation honors n_results after re-ranking.
        let top1 = weight_semantic_results(
            vec![
                ("a".to_string(), 0.4f32, None),
                ("b".to_string(), 0.7f32, None),
            ],
            now,
            1,
        );
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].0, "b");
    }

    /// Writers must keep making progress while a large semantic search (HNSW
    /// path, index rebuilt per query) runs concurrently on the same graph.
    /// Before KG-2.51 the search held the graph read lock for its whole
    /// duration; now it only memcpys the embedding store under the lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_writers_not_starved_by_large_semantic_search() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:busy", GraphType::Agent, None)
                .unwrap();
        }
        // Seed enough embeddings to take the HNSW path (>= brute-force
        // threshold) and make the search non-trivial.
        {
            let s = state.read().await;
            let core = s.registry.get("agent:busy").unwrap().core.clone();
            drop(s);
            let g = &*core;
            for i in 0..2_000u32 {
                let id = format!("n{}", i);
                g.add_node(
                    id.clone(),
                    rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                );
                let emb: Vec<f32> = (0..64).map(|d| ((i + d) % 97) as f32 / 97.0).collect();
                g.semantic_store.write().add_embedding(id, emb);
            }
        }

        let search_state = state.clone();
        let search = tokio::spawn(async move {
            dispatch(
                &search_state,
                request(
                    1,
                    "agent:busy",
                    None,
                    Method::SemanticSearch {
                        query_embedding: vec![0.5f32; 64],
                        n_results: 25,
                    },
                ),
            )
            .await
        });

        // Interleave writes while the search task runs; each must complete
        // promptly instead of queueing behind a long-held read lock.
        for i in 0..50u64 {
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                dispatch(
                    &state,
                    request(100 + i, "agent:busy", None, add_node(&format!("w{}", i))),
                ),
            )
            .await
            .expect("writer starved during semantic search");
            assert_ok(&resp);
            tokio::task::yield_now().await;
        }

        let resp = search.await.expect("search task panicked");
        assert_ok(&resp);
        // Compact encoding (Phase C-D): the weighted result is a Raw msgpack blob.
        assert!(matches!(resp.result, Some(ResultPayload::Raw(_))));
    }

    #[tokio::test]
    async fn test_offloaded_algorithms_round_trip() {
        // Snapshot+spawn_blocking arms must preserve result semantics.
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:algo", GraphType::Agent, None)
                .unwrap();
        }
        for (id, m) in [
            (1, add_node("a")),
            (2, add_node("b")),
            (3, add_node("c")),
            (
                4,
                Method::AddEdge {
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: rmp_serde::to_vec(&serde_json::json!({"weight": 2.0}))
                        .unwrap(),
                },
            ),
            (
                5,
                Method::AddEdge {
                    source_id: "b".into(),
                    target_id: "c".into(),
                    properties_msgpack: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                },
            ),
        ] {
            assert_ok(&dispatch(&state, request(id, "agent:algo", None, m)).await);
        }

        let pagerank = dispatch(
            &state,
            request(
                10,
                "agent:algo",
                None,
                Method::PageRank {
                    damping: 0.85,
                    iterations: 20,
                },
            ),
        )
        .await;
        assert_ok(&pagerank);
        // Compact encoding (Phase C-D): pagerank returns a Raw msgpack blob that
        // decodes to the exact same typed result.
        let Some(ResultPayload::Raw(bytes)) = pagerank.result else {
            panic!("expected Raw pagerank result");
        };
        let scores: Vec<(String, f64)> = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(scores.len(), 3);

        let communities = dispatch(
            &state,
            request(
                11,
                "agent:algo",
                None,
                Method::CommunityDetection { resolution: 1.0 },
            ),
        )
        .await;
        assert_ok(&communities);

        let metrics = dispatch(&state, request(12, "agent:algo", None, Method::Metrics)).await;
        assert_ok(&metrics);
        let Some(ResultPayload::Json(m)) = metrics.result else {
            panic!("expected JSON metrics result");
        };
        assert_eq!(m.get("node_count").and_then(|v| v.as_u64()), Some(3));
        assert_eq!(m.get("edge_count").and_then(|v| v.as_u64()), Some(2));
        // total_mutations comes from the ledger length captured under-lock
        // (3 adds + 2 edges) — the snapshot itself carries no ledger.
        assert_eq!(m.get("total_mutations").and_then(|v| v.as_u64()), Some(5));
    }

    #[tokio::test]
    async fn test_diff_against_gates_other_graph() {
        let state = multi_tenant_state().await;
        // worker2 owns nothing here; create their graph for the diff source.
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:worker2", GraphType::Agent, Some("worker2".into()))
                .unwrap();
        }
        // worker2 may read its own graph but NOT diff it against worker1's.
        let resp = dispatch(
            &state,
            request(
                1,
                "agent:worker2",
                Some("worker2"),
                Method::DiffAgainst {
                    other_graph: "agent:worker1".to_string(),
                },
            ),
        )
        .await;
        assert_denied(&resp);
    }
}
