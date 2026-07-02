// CONCEPT:KG-2.19 — Tokio Service Server
//
// Long-running Tokio server that holds the GraphRegistry in memory
// and serves requests over UDS or TCP with HMAC-SHA256 authentication.

mod access;
mod auth;
// Streamed content-addressed BLOB substrate (CONCEPT:KG-2.206). Facade-only,
// behind the `blob` cargo feature. Default/server-only builds compile NONE of it;
// the Blob* methods then fall to the dispatch "not available" catch-all.
#[cfg(feature = "blob")]
pub mod blob;
// Generic namespaced Key→Value surface (CONCEPT:EG-022). Self-routing (NOT graph-
// scoped) like blob/tsdb, behind the `kv` cargo feature. A build without it compiles
// none of it; the Kv* methods then fall to the dispatch "not available" catch-all.
#[cfg(feature = "kv")]
pub mod kv;
// Change-Data-Capture hub + continuous queries + subscriptions/triggers
// (CONCEPT:KG-2.229/230). Facade-only, behind the `streaming` cargo feature (no heavy
// dep, folds into pi/node/cluster/full). A build without it compiles none of it.
#[cfg(feature = "streaming")]
pub mod cdc;
// Distributed result-cache coherence over the CDC feed (CONCEPT:KG-2.233): a replica
// tailing CDC invalidates its local version-keyed result cache on a remote write.
// Needs BOTH the cache (`result-cache`) and the CDC feed (`streaming`).
#[cfg(all(feature = "result-cache", feature = "streaming"))]
pub mod cache_coherence;
// Facade-side ColdTier impls (CONCEPT:KG-2.233): redb-durable default + S3 behind
// `cold-tier-s3`. The seam + in-memory impl live in eg-core; this needs redb.
#[cfg(all(feature = "cold-tier", feature = "redb"))]
pub mod cold_tier_impl;
mod compute;
mod dispatch;
pub(crate) mod handlers;
pub mod persistence;
// Wire-agnostic SQL execution core (CONCEPT:EG-074) — the multi-wire keystone. The
// wire-NEUTRAL `classify → dispatch → exec` pipeline + per-connection session/txn
// state that EVERY wire (Postgres today; SQLite/MySQL/MSSQL Phase J; AMQP Phase Y)
// reuses. Behind the `wire` facade feature (pulled in by `pgwire`; a future wire's
// feature pulls it in too). Kept OUT of `node`/`full` — the orchestrator folds it in.
#[cfg(feature = "wire")]
pub mod wire;
// Postgres wire-protocol shim (CONCEPT:KG-2.189). Facade-only, behind the `pgwire`
// cargo feature (cluster tier). The FIRST `wire::WireProtocol` adapter (CONCEPT:EG-074).
// Default/pi/node builds compile NONE of it.
#[cfg(feature = "pgwire")]
pub mod pgwire;
// SQLite-compatible served surface (CONCEPT:EG-075) — Phase J. SQLite has NO client/
// server wire protocol, so this is a lightweight NDJSON-over-TCP endpoint that accepts
// SQLite-dialect SQL, rewrites the SQLite-isms (AUTOINCREMENT / INTEGER PRIMARY KEY /
// PRAGMA no-ops) and runs them through the shared `WireSession` (CONCEPT:EG-074). The
// SECOND `wire` consumer after pgwire; behind the `sqlite-wire` feature (pulls in
// `wire`). Pure-Rust — NO C-linked sqlite. Kept OUT of node/full — the orchestrator folds it.
#[cfg(feature = "sqlite-wire")]
pub mod sqlite_wire;
/// W3C SPARQL 1.1 Protocol HTTP endpoint (CONCEPT:EG-017, feature `sparql-http`).
#[cfg(feature = "sparql-http")]
pub mod sparql_http;
/// GraphQL real subscriptions over Server-Sent Events (CONCEPT:EG-064, feature
/// `graphql`): a live query re-resolved on every eg-core change and pushed as
/// `text/event-stream` frames over the same hand-rolled tokio HTTP idiom.
#[cfg(feature = "graphql")]
pub mod graphql_sub;
/// Observability log ingestion + Parquet segment substrate (CONCEPT:EG-160/161,
/// feature `obs`): a hand-rolled HTTP listener accepting OTLP/HTTP, Elasticsearch
/// `_bulk`/`_doc` and JSON-lines log records, landing them in eg-tsdb series +
/// eg-text full-text indices and rolling Parquet-on-blob-CAS segments — the first
/// slice of Phase T (surpass OpenObserve). Self-contained (its own `ObsState`), not
/// tied to the graph `ServerState`.
#[cfg(feature = "obs")]
pub mod obs;
// Process-wide user-defined relational table store (CONCEPT:EG-018/EG-023): the ONE
// `eg_query::TableStore` (redb permits a single handle per file per process) shared by
// BOTH the wire `Method::Sql` DDL/DML path and the pgwire shim, so a table created via
// one surface is visible to the other. Behind `query` (TableStore needs eg-query/sql).
#[cfg(feature = "query")]
pub mod sql_tables;
mod state;
mod transport;
// Server-staged OCC ACID transactions (CONCEPT:KG-2.180). `txn` holds the staged
// transaction state + id source; `handlers::txn` owns the Txn* methods.
pub mod txn;

// External path surface — `server::ServerState`, `server::MAX_BATCH_IDS`,
// `server::compute_auth_token`, `server::dispatch`, and
// `server::{handle_connection,serve_uds,serve_tcp}` — used by main.rs/persist.rs/tests.
pub use auth::compute_auth_token;
pub use dispatch::dispatch;
// Distributed-compute materialized-view boot reload (CONCEPT:KG-2.227): the binary
// calls this on startup to repopulate the in-RAM matview index from redb.
#[cfg(feature = "compute-dist")]
pub use handlers::dist_compute::reload_matviews;
pub use persistence::PersistenceBackend;
pub use state::{txn_limits_from_env, ServerState, MAX_BATCH_IDS};
pub use transport::{handle_connection, run_idle_watcher, serve_tcp, ShutdownCoordinator};
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
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
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
            // A real per-test temp series store so the `Ts*` handler round-trips
            // exercise the actual store (a fresh, uniquely-named redb file — redb
            // holds an exclusive per-process file lock, so each test gets its own).
            #[cfg(feature = "tsdb")]
            tsdb_store: Some(Arc::new(
                eg_tsdb::store::SeriesStore::open(&std::env::temp_dir().join(format!(
                        "eg-tsdb-test-{}-{}.redb",
                        std::process::id(),
                        std::sync::atomic::AtomicU64::new(0)
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            + std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_nanos() as u64)
                                .unwrap_or(0)
                    )))
                .expect("open test series store"),
            )),
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
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
                roles: vec![],
            });
            for w in ["worker1", "worker2"] {
                s.isolation.register_agent(AgentIdentity {
                    agent_id: w.into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                    roles: vec![],
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

    // ── SQL query surface (CONCEPT:KG-2.178) ────────────────────────────

    /// End-to-end: add nodes, then route `Method::Sql` through the full dispatch
    /// chain and decode the `Raw(QueryResult)` payload back to rows. Proves the
    /// query handler is wired before graph_ops and returns rows. (query feature)
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_sql_select_returns_rows() {
        let state = test_state();
        let mk = |id: &str, ty: &str, rank: i64| Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(
                &serde_json::json!({"type": ty, "rank": rank}),
            )
            .unwrap(),
        };
        for (i, (id, ty, rank)) in [("n1", "Agent", 1), ("n2", "Agent", 2), ("n3", "Tool", 3)]
            .iter()
            .enumerate()
        {
            assert_ok(
                &dispatch(
                    &state,
                    request(i as u64 + 1, "__commons__", None, mk(id, ty, *rank)),
                )
                .await,
            );
        }

        let resp = dispatch(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::Sql {
                    query: "SELECT id FROM nodes WHERE rank >= 2 ORDER BY id LIMIT 5".into(),
                    params_msgpack: Vec::new(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let raw = match resp.result {
            Some(ResultPayload::Raw(bytes)) => bytes,
            other => panic!("expected Raw(QueryResult), got {:?}", other),
        };
        let qr: crate::protocol::QueryResult = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(qr.columns, vec!["id".to_string()]);
        let ids: Vec<String> = qr
            .rows
            .iter()
            .map(|blob| {
                let cells: Vec<serde_json::Value> = rmp_serde::from_slice(blob).unwrap();
                cells[0].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(ids, vec!["n2".to_string(), "n3".to_string()]);
    }

    // ── Unified cross-modal query (CONCEPT:KG-2.208/209) ────────────────

    /// Build the canonical cross-modal fixture in `__commons__` via the full
    /// dispatch chain: Doc nodes with a `year`, CITES/MENTIONS edges, and an
    /// embedding per Doc. Mirrors eg-plan's test fixture so the dispatched plan and
    /// the in-crate oracle operate on identical data.
    #[cfg(feature = "query")]
    async fn build_unified_fixture(state: &Arc<RwLock<ServerState>>) {
        let mut id = 1u64;
        let mut send = |m: Method| {
            let r = request(id, "__commons__", None, m);
            id += 1;
            r
        };
        for (nid, ty, year) in [
            ("d1", "Doc", 2025),
            ("d2", "Doc", 2025),
            ("d3", "Doc", 2023),
            ("d4", "Doc", 2024),
            ("d5", "Doc", 2025),
            ("old", "Doc", 2020),
            ("t1", "Tool", 2025),
        ] {
            let m = Method::AddNode {
                node_id: nid.into(),
                properties_msgpack: rmp_serde::to_vec_named(
                    &serde_json::json!({"type": ty, "year": year}),
                )
                .unwrap(),
            };
            assert_ok(&dispatch(state, send(m)).await);
        }
        for (s, t, rel) in [
            ("d1", "d2", "CITES"),
            ("d2", "d3", "CITES"),
            ("d1", "d4", "CITES"),
            ("d2", "d5", "MENTIONS"),
        ] {
            let m = Method::AddEdge {
                source_id: s.into(),
                target_id: t.into(),
                properties_msgpack: rmp_serde::to_vec_named(
                    &serde_json::json!({"relationship": rel}),
                )
                .unwrap(),
            };
            assert_ok(&dispatch(state, send(m)).await);
        }
        for (nid, emb) in [
            ("d1", vec![0.2f32, 0.9, 0.0, 0.0]),
            ("d2", vec![0.98, 0.20, 0.0, 0.0]),
            ("d3", vec![0.80, 0.60, 0.0, 0.0]),
            ("d4", vec![0.90, 0.44, 0.0, 0.0]),
            ("d5", vec![0.0, 0.0, 1.0, 0.0]),
            ("old", vec![0.0, 1.0, 0.0, 0.0]),
        ] {
            let m = Method::AddEmbedding {
                node_id: nid.into(),
                embedding: emb,
            };
            assert_ok(&dispatch(state, send(m)).await);
        }
    }

    /// Decode a `UnifiedQuery` response (`Raw([(id, score?)])`) to its id list.
    #[cfg(feature = "query")]
    fn unified_ids(resp: &crate::protocol::Response) -> Vec<String> {
        let raw = match &resp.result {
            Some(ResultPayload::Raw(bytes)) => bytes,
            other => panic!("expected Raw rows, got {:?}", other),
        };
        let rows: Vec<(String, Option<f32>)> = rmp_serde::from_slice(raw).unwrap();
        rows.into_iter().map(|(id, _)| id).collect()
    }

    /// THE oracle proof, end-to-end through the SERVED surface: run the unified plan
    /// `Method::UnifiedQuery` over the full dispatch chain, then run the SAME query
    /// the siloed way via `eg_plan::oracle::separate_surfaces` over the graph's
    /// snapshot, and assert the served result is byte-identical. (CONCEPT:KG-2.208)
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_unified_query_matches_separate_surfaces_oracle() {
        use eg_plan::{Op, Pred};
        let state = test_state();
        build_unified_fixture(&state).await;

        let plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::Limit { k: 10 },
        ];
        let resp = dispatch(
            &state,
            request(
                100,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(plan),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let served_ids = unified_ids(&resp);

        // The siloed oracle over the same snapshot.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let view = core.analysis_snapshot();
        let semantic = core.semantic_store.read().clone();
        // The oracle's FILTER leg drives DataFusion via a current-thread runtime
        // (eg_query::exec_sql), which cannot nest inside this #[tokio::test] reactor —
        // run it off-reactor, exactly as the served handler does via spawn_blocking.
        let oracle = tokio::task::spawn_blocking(move || {
            eg_plan::oracle::separate_surfaces(
                &view,
                &semantic,
                "Doc",
                &[Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
                "CITES",
                1,
                2,
                &[1.0, 0.0, 0.0, 0.0],
                10,
            )
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            served_ids,
            oracle.ids(),
            "served unified plan must equal the separate-surfaces oracle"
        );
        assert_eq!(
            served_ids,
            vec!["d2", "d4", "d3"],
            "expected ranked order d2 > d4 > d3"
        );
    }

    /// UQL e2e (CONCEPT:KG-2.214): the SAME query written as a UQL TEXT string, served
    /// via `Method::UnifiedQueryText`, returns the BYTE-IDENTICAL result to (a) the
    /// hand-built structured `Method::UnifiedQuery` plan AND (b) the separate-surfaces
    /// oracle. This is the proof the text front-end is faithful: text → Plan → the
    /// SAME run_unified executor, no new execution path.
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_uql_text_equals_structured_plan_and_oracle() {
        use eg_plan::{Op, Pred};
        let state = test_state();
        build_unified_fixture(&state).await;

        // (1) The structured plan (the existing surface).
        let plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::Limit { k: 10 },
        ];
        let structured = dispatch(
            &state,
            request(
                300,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(plan),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await;
        assert_ok(&structured);
        let structured_ids = unified_ids(&structured);

        // (2) The SAME query as a UQL text string, served via the text surface.
        let uql = "MATCH (:Doc) WHERE year > 2024 \
                   |> TRAVERSE -[:CITES]->{1,2} \
                   |> RANK BY ~[1.0, 0.0, 0.0, 0.0] \
                   |> LIMIT 10";
        let textq = dispatch(
            &state,
            request(
                301,
                "__commons__",
                None,
                Method::UnifiedQueryText {
                    text: uql.into(),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await;
        assert_ok(&textq);
        let text_ids = unified_ids(&textq);

        // (3) The siloed oracle over the same snapshot.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let view = core.analysis_snapshot();
        let semantic = core.semantic_store.read().clone();
        let oracle = tokio::task::spawn_blocking(move || {
            eg_plan::oracle::separate_surfaces(
                &view,
                &semantic,
                "Doc",
                &[Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
                "CITES",
                1,
                2,
                &[1.0, 0.0, 0.0, 0.0],
                10,
            )
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            text_ids, structured_ids,
            "UQL text result must equal the structured plan result"
        );
        assert_eq!(
            text_ids,
            oracle.ids(),
            "UQL text result must equal the separate-surfaces oracle"
        );
    }

    /// A malformed UQL string returns a CLEAR error Response (caret diagnostic), not a
    /// panic and not a wrong result. (CONCEPT:KG-2.214)
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_uql_text_bad_syntax_is_clear_error() {
        let state = test_state();
        build_unified_fixture(&state).await;
        let resp = dispatch(
            &state,
            request(
                302,
                "__commons__",
                None,
                Method::UnifiedQueryText {
                    text: "MATCH (:Doc) |> FROBNICATE".into(),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await;
        let err = resp.error.expect("malformed UQL must error");
        assert!(
            err.contains("UQL parse error") && err.contains("pipeline stage"),
            "expected a clear UQL parse error, got: {err}"
        );
    }

    /// Cost-reorder e2e (CONCEPT:KG-2.209): the SAME plan with a selective vs a broad
    /// `reorder_filter_selectivity` (which flip filter-first ↔ vector-first) returns
    /// the IDENTICAL result set through the served surface — the reorder is cost-only.
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_unified_query_cost_reorder_same_result() {
        use eg_plan::{Op, Pred};
        let state = test_state();
        build_unified_fixture(&state).await;

        // Scan'd seed feeding a commuting (Filter, Rank) pair.
        let plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2022.0,
                }],
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
        ];

        let run = |sel: f64, rid: u64| {
            let plan = plan.clone();
            let state = state.clone();
            async move {
                let resp = dispatch(
                    &state,
                    request(
                        rid,
                        "__commons__",
                        None,
                        Method::UnifiedQuery {
                            plan: eg_plan::Plan::new(plan),
                            reorder_filter_selectivity: Some(sel),
                        },
                    ),
                )
                .await;
                assert_ok(&resp);
                let mut ids = unified_ids(&resp);
                ids.sort();
                ids
            }
        };

        let selective = run(0.01, 200).await; // filter-first
        let broad = run(0.98, 201).await; // vector-first
        assert_eq!(
            selective, broad,
            "cost reorder must not change the result set"
        );
        assert!(!selective.is_empty(), "fixture yields a non-empty result");
    }

    // ── Query federation / foreign sources (CONCEPT:KG-2.232, Lane P) ───────

    /// THE federation compose proof through TWO in-process engines: a LOCAL engine A
    /// runs a `UnifiedQuery` whose plan `ForeignScan`s a REMOTE engine B (served over
    /// TCP, queried with the engine's own length-prefixed-MessagePack + HMAC transport),
    /// JOINS B's rows with A's local graph, ranks, and limits. The fused result equals
    /// the MANUAL join done by hand. This is the cross-engine federation seam: ONE plan,
    /// TWO engines, no Python round-trip. (CONCEPT:KG-2.232)
    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn test_federated_query_two_engines_equals_manual_join() {
        use eg_plan::{Op, Pred};

        // ── engine B (the REMOTE), served over TCP ──
        let remote = test_state();
        build_unified_fixture(&remote).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote_addr = listener.local_addr().unwrap().to_string();
        let remote_for_serve = remote.clone();
        tokio::spawn(async move {
            // One connection is enough for the single foreign round-trip the test makes.
            if let Ok((stream, _)) = listener.accept().await {
                handle_connection(stream, remote_for_serve).await;
            }
        });

        // ── engine A (the LOCAL) ──
        let local = test_state();
        build_unified_fixture(&local).await;

        // The remote returns the ids of Docs it CITES-reaches from the year>2024 seed (a
        // remote graph traversal): UQL `MATCH (:Doc) WHERE year>2024 |> TRAVERSE
        // -[:CITES]->{1,2}` → over B's fixture the seed is {d1,d2,d5} (years 2025) and
        // CITES-reaching 1..2 hops gives {d2,d3,d4} (d1→d2→d3, d1→d4). The foreign source
        // pulls those ids; A then JOINS them with its OWN local filter `year > 2023`.
        let foreign = eg_types::wire::ForeignSourceSpec::RemoteEngine {
            endpoint: remote_addr,
            graph: "__commons__".into(),
            secret: SECRET.into(),
            uql: "MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2}".into(),
            cypher: String::new(),
            id_field: String::new(),
        };

        let query = vec![1.0f32, 0.0, 0.0, 0.0]; // ranks d2 > d4 > d3
        let plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2023.0,
                }],
            },
            Op::ForeignScan {
                source: foreign,
                join: true,
            },
            Op::Rank {
                query: query.clone(),
            },
            Op::Limit { k: 10 },
        ];
        let resp = dispatch(
            &local,
            request(
                500,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(plan),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let fused = unified_ids(&resp);

        // ── the manual join (the oracle) ──
        // A's local filter `year > 2023` → {d1, d2, d4, d5}. The remote's
        // CITES-traversal set → {d2, d3, d4}. Join (intersection) → {d2, d4}; ranked by
        // `[1,0,0,0]` → d2 then d4.
        let local_filtered: std::collections::HashSet<&str> =
            ["d1", "d2", "d4", "d5"].into_iter().collect();
        let remote_reached: std::collections::HashSet<&str> =
            ["d2", "d3", "d4"].into_iter().collect();
        let joined: std::collections::HashSet<String> = local_filtered
            .intersection(&remote_reached)
            .map(|s| s.to_string())
            .collect();
        let core = {
            let s = local.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let semantic = core.semantic_store.read().clone();
        let ranked = tokio::task::spawn_blocking(move || semantic.semantic_search(&query, 32))
            .await
            .unwrap();
        let manual: Vec<String> = ranked
            .into_iter()
            .map(|(id, _)| id)
            .filter(|id| joined.contains(id))
            .collect();

        assert_eq!(
            fused, manual,
            "federated two-engine plan must equal the manual join"
        );
        assert_eq!(fused, vec!["d2", "d4"], "ranked: d2 (closest) then d4");
    }

    /// `RegisterForeignSource` is served and recorded on `ServerState`. (CONCEPT:KG-2.232)
    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn test_register_foreign_source_served() {
        let state = test_state();
        let resp = dispatch(
            &state,
            request(
                600,
                "__commons__",
                None,
                Method::RegisterForeignSource {
                    name: "papers_api".into(),
                    source: eg_types::wire::ForeignSourceSpec::HttpJson {
                        url: "http://example.invalid/papers".into(),
                        json_path: "data".into(),
                        field_map: eg_types::wire::HttpFieldMap {
                            id: "id".into(),
                            score: None,
                        },
                    },
                },
            ),
        )
        .await;
        assert_ok(&resp);
        match resp.result {
            Some(ResultPayload::String(name)) => assert_eq!(name, "papers_api"),
            other => panic!("expected the registered name, got {other:?}"),
        }
        let s = state.read().await;
        assert!(
            s.foreign_sources.contains_key("papers_api"),
            "the source must be recorded on ServerState"
        );
    }

    // ── Cypher query surface (CONCEPT:KG-2.179) ─────────────────────────

    /// End-to-end: add nodes + a KNOWS edge, route `Method::CypherQuery` through
    /// the FULL dispatch chain, and decode the `Raw(QueryResult)` rows. Proves the
    /// dep-free Cypher handler is wired before graph_ops in a no-DataFusion build.
    /// (cypher feature)
    #[cfg(feature = "cypher")]
    #[tokio::test]
    async fn test_cypher_match_returns_rows() {
        let state = test_state();
        let add = |id: u64, node_id: &str, ty: &str, name: &str| {
            request(
                id,
                "__commons__",
                None,
                Method::AddNode {
                    node_id: node_id.to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(
                        &serde_json::json!({"type": ty, "name": name}),
                    )
                    .unwrap(),
                },
            )
        };
        assert_ok(&dispatch(&state, add(1, "alice", "Person", "Alice")).await);
        assert_ok(&dispatch(&state, add(2, "bob", "Person", "Bob")).await);
        assert_ok(
            &dispatch(
                &state,
                request(
                    3,
                    "__commons__",
                    None,
                    Method::AddEdge {
                        source_id: "alice".into(),
                        target_id: "bob".into(),
                        properties_msgpack: rmp_serde::to_vec_named(
                            &serde_json::json!({"relationship": "KNOWS"}),
                        )
                        .unwrap(),
                    },
                ),
            )
            .await,
        );

        // Single-node label MATCH → label index.
        let resp = dispatch(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::CypherQuery {
                    query: "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a".into(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let qr = match resp.result {
            Some(ResultPayload::Raw(bytes)) => {
                rmp_serde::from_slice::<crate::protocol::QueryResult>(&bytes).unwrap()
            }
            other => panic!("expected Raw(QueryResult), got {:?}", other),
        };
        assert_eq!(qr.columns, vec!["a".to_string()]);
        let cells: Vec<serde_json::Value> = rmp_serde::from_slice(&qr.rows[0]).unwrap();
        assert_eq!(cells[0].as_str(), Some("alice"));

        // 2-node typed-edge MATCH → VF2.
        let resp2 = dispatch(
            &state,
            request(
                11,
                "__commons__",
                None,
                Method::CypherQuery {
                    query: "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b".into(),
                },
            ),
        )
        .await;
        assert_ok(&resp2);
        let qr2 = match resp2.result {
            Some(ResultPayload::Raw(bytes)) => {
                rmp_serde::from_slice::<crate::protocol::QueryResult>(&bytes).unwrap()
            }
            other => panic!("expected Raw(QueryResult), got {:?}", other),
        };
        assert_eq!(qr2.columns, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(qr2.rows.len(), 1);
        let pair: Vec<serde_json::Value> = rmp_serde::from_slice(&qr2.rows[0]).unwrap();
        assert_eq!(pair[0].as_str(), Some("alice"));
        assert_eq!(pair[1].as_str(), Some("bob"));
    }

    /// Feature-gating contract for the Cypher surface (CONCEPT:KG-2.179): with the
    /// `cypher` feature off, `Method::CypherQuery`'s handler arm is compiled away
    /// and the request must hit the not-built catch-all. (Compiled out when
    /// `cypher` is on, where the real handler answers instead.)
    #[cfg(not(feature = "cypher"))]
    #[tokio::test]
    async fn test_cypher_gated_out_returns_not_built() {
        let state = test_state();
        let method = Method::CypherQuery {
            query: "MATCH (a:Person) RETURN a".into(),
        };
        let resp = dispatch(&state, request(1, "__commons__", None, method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    /// End-to-end (CONCEPT:KG-2.235): add the SAME alice-KNOWS->bob graph the Cypher
    /// test builds, route `Method::GraphQl` through the FULL dispatch chain, and PROVE
    /// the GraphQL result is the expected node/field set. When `cypher` is ALSO built,
    /// cross-check that the GraphQL KNOWS traversal equals the served Cypher result for
    /// the same question — the GraphQL==Cypher equivalence over the served surface.
    /// (graphql feature)
    #[cfg(feature = "graphql")]
    #[tokio::test]
    async fn test_graphql_routes_and_equals_cypher() {
        let state = test_state();
        let add = |id: u64, node_id: &str, ty: &str, name: &str| {
            request(
                id,
                "__commons__",
                None,
                Method::AddNode {
                    node_id: node_id.to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(
                        &serde_json::json!({"type": ty, "name": name}),
                    )
                    .unwrap(),
                },
            )
        };
        assert_ok(&dispatch(&state, add(1, "alice", "Person", "Alice")).await);
        assert_ok(&dispatch(&state, add(2, "bob", "Person", "Bob")).await);
        assert_ok(
            &dispatch(
                &state,
                request(
                    3,
                    "__commons__",
                    None,
                    Method::AddEdge {
                        source_id: "alice".into(),
                        target_id: "bob".into(),
                        properties_msgpack: rmp_serde::to_vec_named(
                            &serde_json::json!({"relationship": "KNOWS"}),
                        )
                        .unwrap(),
                    },
                ),
            )
            .await,
        );

        // GraphQL: Alice + her KNOWS targets' names.
        let gql = dispatch(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::GraphQl {
                    query: r#"{ Person(name: "Alice") { name KNOWS { name } } }"#.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert_ok(&gql);
        let value: serde_json::Value = match gql.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected Raw(json), got {:?}", other),
        };
        let alice = &value["data"]["Person"][0];
        assert_eq!(alice["name"].as_str(), Some("Alice"));
        let gql_knows: Vec<String> = alice["KNOWS"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap().to_string())
            .collect();
        // alice KNOWS bob.
        assert_eq!(gql_knows, vec!["Bob".to_string()]);

        // When cypher is ALSO built: prove GraphQL == Cypher over the SAME served
        // dispatch for the same question (the equivalence proof, served form).
        #[cfg(feature = "cypher")]
        {
            let cy = dispatch(
                &state,
                request(
                    11,
                    "__commons__",
                    None,
                    Method::CypherQuery {
                        query: "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' \
                                RETURN b.name"
                            .into(),
                    },
                ),
            )
            .await;
            assert_ok(&cy);
            let qr = match cy.result {
                Some(ResultPayload::Raw(bytes)) => {
                    rmp_serde::from_slice::<crate::protocol::QueryResult>(&bytes).unwrap()
                }
                other => panic!("expected Raw(QueryResult), got {:?}", other),
            };
            let cy_knows: Vec<String> = qr
                .rows
                .iter()
                .map(|b| {
                    let cells: Vec<serde_json::Value> = rmp_serde::from_slice(b).unwrap();
                    cells[0].as_str().unwrap().to_string()
                })
                .collect();
            assert_eq!(
                gql_knows, cy_knows,
                "GraphQL KNOWS traversal must equal the served Cypher result"
            );
        }
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

    /// Same feature-gating contract for the SQL surface (CONCEPT:KG-2.178): with
    /// the `query` feature off, `Method::Sql`'s handler arm is compiled away and
    /// the request must hit the not-built catch-all. (Compiled out when `query` is
    /// on, where the real handler answers instead.)
    #[cfg(not(feature = "query"))]
    #[tokio::test]
    async fn test_sql_gated_out_returns_not_built() {
        let state = test_state();
        let method = Method::Sql {
            query: "SELECT id FROM nodes".into(),
            params_msgpack: Vec::new(),
        };
        let resp = dispatch(&state, request(1, "__commons__", None, method)).await;
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
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir.to_string_lossy().to_string()),
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
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
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
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
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.clone()),
            persistence: Some(backend.clone()),
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
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
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
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
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.clone()),
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
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
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
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
        // A hot graph that has exhausted its per-graph in-flight cap sheds WRITES with
        // BUSY, but OTHER graphs keep being served from the (ample) global pool — one
        // tenant cannot starve the rest. Per-graph backpressure is a WRITE property:
        // reads bypass the per-graph cap via the reserved read lane (CONCEPT:EG-044),
        // so both probes here are writes.
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
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(64)), // global: ample
            read_admission: Arc::new(Semaphore::new(64)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 1, // any one graph: a single slot
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
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
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
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

        // g_cold's write must reach dispatch, so the graph has to exist. (g_hot's write
        // is shed at admission, BEFORE dispatch, so g_hot needs no registry entry.)
        state
            .write()
            .await
            .registry
            .create_graph("g_cold", GraphType::Agent, None)
            .unwrap();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let st = state.clone();
        let handle = tokio::spawn(async move { handle_connection(server, st).await });

        // g_hot is saturated → its WRITE is shed BUSY at the per-graph cap.
        let r_hot = round_trip(&mut client, &request(1, "g_hot", None, add_node("h1"))).await;
        assert!(
            r_hot.error.as_deref().unwrap_or("").contains("at capacity"),
            "hot graph write must be shed BUSY, got {:?}",
            r_hot
        );

        // g_cold is independent → its WRITE is served normally despite g_hot saturation.
        let r_cold = round_trip(&mut client, &request(2, "g_cold", None, add_node("c1"))).await;
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

    /// CONCEPT:KG-2.51 — Per-graph write isolation (parallel writers).
    ///
    /// Writers to DIFFERENT graphs must never serialize on a global/registry lock:
    /// `dispatch_graph_op` only takes the global `ServerState` lock as a SHARED
    /// reader, clones the target graph's `Arc<GraphCore>`, and releases it before
    /// any mutation — so the only write lock taken is `GraphCore::topo`, which is
    /// per-graph. This reproduces the starvation scenario: a long-running write
    /// txn on graph A (a stand-in for sustained ingestion holding A's write lock)
    /// must NOT block writers targeting graph B.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_writers_to_distinct_graphs_do_not_serialize() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:ingest", GraphType::Agent, None)
                .unwrap();
            s.registry
                .create_graph("agent:control", GraphType::Agent, None)
                .unwrap();
        }

        // Grab graph A's core and open a write txn, HOLDING its topology write lock
        // for the duration — exactly what sustained ingestion does to one graph.
        let ingest_core = {
            let s = state.read().await;
            s.registry.get("agent:ingest").unwrap().core.clone()
        };
        let _held_txn = ingest_core.txn(); // holds agent:ingest topo.write()

        // With A's write lock held, writers to B (the control plane) must still
        // complete promptly. If anything serialized writes across graphs (a global
        // write lock, or lazy-create under a registry write lock), every one of
        // these would deadlock against `_held_txn` and the timeout would fire.
        for i in 0..25u64 {
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                dispatch(
                    &state,
                    request(200 + i, "agent:control", None, add_node(&format!("c{}", i))),
                ),
            )
            .await
            .expect("control-plane writer starved by ingestion holding another graph's lock");
            assert_ok(&resp);
        }

        // The held graph took no control-plane writes; the control graph took all.
        drop(_held_txn);
        assert_eq!(ingest_core.node_count(), 0);
        let control_core = {
            let s = state.read().await;
            s.registry.get("agent:control").unwrap().core.clone()
        };
        assert_eq!(control_core.node_count(), 25);
    }

    /// CONCEPT:KG-2.182 — per-graph write coalescer, end-to-end through dispatch.
    ///
    /// Many concurrent writers to ONE hot graph (the `__commons__` firehose) must
    /// (a) ALL land via the dispatch path (no lost writes — the coalescer is not a
    /// drop point) and (b) be applied in FEWER topology-lock acquisitions than ops,
    /// proving the batching win on the live serving path (not just the unit level).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatch_coalesces_concurrent_writes_to_one_graph() {
        let state = test_state();

        const N: u64 = 400;
        let mut handles = Vec::with_capacity(N as usize);
        for i in 0..N {
            let st = state.clone();
            handles.push(tokio::spawn(async move {
                dispatch(
                    &st,
                    request(i, "__commons__", None, add_node(&format!("n{i}"))),
                )
                .await
            }));
        }
        for h in handles {
            assert_ok(&h.await.unwrap());
        }

        // Every write landed exactly once on the live path.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert_eq!(
            core.node_count() as u64,
            N,
            "all {N} dispatched writes land"
        );

        // The win: the coalescer applied them in fewer lock acquisitions than ops.
        let (batches, ops) = {
            let s = state.read().await;
            let w = s
                .write_coalescer
                .writer_for("__commons__", &core)
                .expect("commons writer");
            (w.stats().batches(), w.stats().ops())
        };
        assert_eq!(ops, N, "stats account for every dispatched write");
        assert!(
            batches < ops,
            "dispatch path should batch: {ops} ops in {batches} lock acquisitions",
        );
    }

    /// CAS exactly-once is preserved through the dispatch coalescer: concurrent
    /// claimers of one node via `CompareAndSetNodeFields` yield exactly one winner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatch_cas_exactly_once_under_coalescing() {
        let state = test_state();

        // Seed the task node with owner=null.
        let seed = Method::AddNode {
            node_id: "task".into(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"owner": null}))
                .unwrap(),
        };
        assert_ok(&dispatch(&state, request(0, "__commons__", None, seed)).await);

        const C: u64 = 40;
        let mut handles = Vec::with_capacity(C as usize);
        for i in 0..C {
            let st = state.clone();
            handles.push(tokio::spawn(async move {
                let conditions_msgpack =
                    rmp_serde::to_vec_named(&serde_json::json!({"owner": null})).unwrap();
                let updates_msgpack =
                    rmp_serde::to_vec_named(&serde_json::json!({"owner": format!("w{i}")}))
                        .unwrap();
                let m = Method::CompareAndSetNodeFields {
                    node_id: "task".into(),
                    conditions_msgpack,
                    updates_msgpack,
                };
                let resp = dispatch(&st, request(100 + i, "__commons__", None, m)).await;
                matches!(resp.result, Some(ResultPayload::Bool(true)))
            }));
        }
        let mut winners = 0;
        for h in handles {
            if h.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one CAS claimer wins through dispatch");
    }

    // ── Multi-op OCC ACID transactions (CONCEPT:KG-2.180) ───────────────

    /// Open a txn on `graph` and return its server-issued id.
    async fn begin_txn(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str) -> String {
        begin_txn_iso(state, id, graph, None).await
    }

    /// Open a txn on `graph` with an explicit isolation hint (CONCEPT:KG-2.183) and
    /// return its server-issued id.
    async fn begin_txn_iso(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        graph: &str,
        isolation: Option<&str>,
    ) -> String {
        let resp = dispatch(
            state,
            request(
                id,
                graph,
                None,
                Method::BeginTxn {
                    graph: None,
                    isolation: isolation.map(str::to_string),
                },
            ),
        )
        .await;
        match resp.result {
            Some(ResultPayload::String(txn_id)) => txn_id,
            other => panic!(
                "BeginTxn must return a txn id, got {other:?} (err={:?})",
                resp.error
            ),
        }
    }

    fn node_props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// (a) Happy path: begin → stage two nodes + one edge → commit → all present.
    #[tokio::test]
    async fn txn_commit_applies_staged_writes() {
        let state = test_state();
        let txn = begin_txn(&state, 1, "__commons__").await;

        for (i, nid) in ["a", "b"].iter().enumerate() {
            let r = dispatch(
                &state,
                request(
                    10 + i as u64,
                    "__commons__",
                    None,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: nid.to_string(),
                        properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(
                matches!(r.result, Some(ResultPayload::Bool(true))),
                "stage node {nid}"
            );
        }
        let r = dispatch(
            &state,
            request(
                20,
                "__commons__",
                None,
                Method::TxnAddEdge {
                    txn_id: txn.clone(),
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: node_props(serde_json::json!({})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "stage edge"
        );

        // Nothing applied until commit: the nodes are absent pre-commit.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(
            !core.has_node("a"),
            "staged node must NOT exist before commit"
        );

        // Commit → Bool(true), all present.
        let r = dispatch(
            &state,
            request(
                30,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: txn.clone(),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "commit ok: {:?}",
            r.error
        );
        assert!(
            core.has_node("a") && core.has_node("b"),
            "committed nodes present"
        );
        assert!(core.has_edge("a", "b"), "committed edge present");
        // The txn id is consumed.
        let s = state.read().await;
        assert!(s.open_txns.get(&txn).is_none(), "committed txn removed");
    }

    /// (b) Rollback: begin → stage → rollback → graph unchanged, nothing persisted.
    #[tokio::test]
    async fn txn_rollback_applies_nothing() {
        let state = test_state();
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let v0 = core.version();
        let txn = begin_txn(&state, 1, "__commons__").await;
        let r = dispatch(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::TxnAddNode {
                    txn_id: txn.clone(),
                    node_id: "ghost".into(),
                    properties_msgpack: node_props(serde_json::json!({})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

        let r = dispatch(
            &state,
            request(
                20,
                "__commons__",
                None,
                Method::Rollback {
                    txn_id: txn.clone(),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "rollback ok"
        );
        assert!(!core.has_node("ghost"), "rolled-back node must be absent");
        assert_eq!(
            core.version(),
            v0,
            "rollback bumps no version (nothing applied)"
        );
        assert_eq!(core.node_count(), 0, "graph unchanged after rollback");
    }

    /// (c) OCC conflict: two txns read-modify the SAME node; the first commits, the
    /// second's commit returns Bool(false) (a true rollback — nothing applied).
    #[tokio::test]
    async fn txn_occ_conflict_second_commit_fails() {
        let state = test_state();
        // Seed the contended node.
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "k".into(),
                        properties_msgpack: node_props(serde_json::json!({"v": 0})),
                    },
                ),
            )
            .await,
        );

        // Both transactions open and stage a CAS-like overwrite of node "k"; both
        // fingerprint "k" at its CURRENT (v=0) state.
        let t1 = begin_txn(&state, 2, "__commons__").await;
        let t2 = begin_txn(&state, 3, "__commons__").await;
        for (rid, txn, val) in [(10u64, &t1, 1), (11, &t2, 2)] {
            let r = dispatch(
                &state,
                request(
                    rid,
                    "__commons__",
                    None,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: "k".into(),
                        properties_msgpack: node_props(serde_json::json!({"v": val})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(matches!(r.result, Some(ResultPayload::Bool(true))));
        }

        // First commit wins.
        let r1 = dispatch(
            &state,
            request(
                20,
                "__commons__",
                None,
                Method::Commit { txn_id: t1.clone() },
            ),
        )
        .await;
        assert!(
            matches!(r1.result, Some(ResultPayload::Bool(true))),
            "t1 commits"
        );

        // Second commit conflicts (node "k" changed since t2 began) → Bool(false).
        let r2 = dispatch(
            &state,
            request(
                21,
                "__commons__",
                None,
                Method::Commit { txn_id: t2.clone() },
            ),
        )
        .await;
        assert!(
            matches!(r2.result, Some(ResultPayload::Bool(false))),
            "t2 must conflict, got {:?} err={:?}",
            r2.result,
            r2.error
        );

        // t1's value won; t2 applied nothing.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let props: serde_json::Value =
            rmp_serde::from_slice(&core.get_node_properties("k").unwrap()).unwrap();
        assert_eq!(
            props["v"], 1,
            "first committer's write survives, not the conflicted one"
        );
    }

    /// (d) Abandoned txn auto-rolls-back after the TTL (drive the sweep directly).
    #[tokio::test]
    async fn txn_ttl_sweep_reclaims_idle() {
        use crate::server::txn::{now_ms, sweep_expired_txns};
        let state = test_state();
        let txn = begin_txn(&state, 1, "__commons__").await;
        assert!(state.read().await.open_txns.get(&txn).is_some());

        // A sweep with a future "now" (TTL elapsed) reclaims the idle txn.
        let future = now_ms() + 10 * 60 * 1000; // 10 min later
        let reclaimed = sweep_expired_txns(&state, 300, future);
        assert_eq!(reclaimed, 1, "idle txn past TTL is swept");
        assert!(
            state.read().await.open_txns.get(&txn).is_none(),
            "swept txn removed"
        );
        // Committing a swept txn is now an unknown-id error (true rollback occurred).
        let r = dispatch(
            &state,
            request(2, "__commons__", None, Method::Commit { txn_id: txn }),
        )
        .await;
        assert!(r.error.is_some(), "committing a swept txn errors");
    }

    /// (e) Regression: standalone single-op CAS still works (degenerate 1-op
    /// auto-commit) and is untouched by the txn machinery.
    #[tokio::test]
    async fn standalone_cas_still_works() {
        let state = test_state();
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "task".into(),
                        properties_msgpack: node_props(serde_json::json!({"owner": null})),
                    },
                ),
            )
            .await,
        );
        let r = dispatch(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::CompareAndSetNodeFields {
                    node_id: "task".into(),
                    conditions_msgpack: node_props(serde_json::json!({"owner": null})),
                    updates_msgpack: node_props(serde_json::json!({"owner": "w1"})),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "CAS claims"
        );
        // A second CAS with the same condition fails (already claimed).
        let r = dispatch(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::CompareAndSetNodeFields {
                    node_id: "task".into(),
                    conditions_msgpack: node_props(serde_json::json!({"owner": null})),
                    updates_msgpack: node_props(serde_json::json!({"owner": "w2"})),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(false))),
            "second CAS rejected"
        );
    }

    // ── Transaction isolation levels (CONCEPT:KG-2.183 — M6b) ────────────

    /// Commit `txn` and return the Bool payload (true=committed, false=conflict).
    async fn commit_bool(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        graph: &str,
        txn: &str,
    ) -> bool {
        let r = dispatch(
            state,
            request(
                id,
                graph,
                None,
                Method::Commit {
                    txn_id: txn.to_string(),
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::Bool(b)) => b,
            other => panic!("Commit must return Bool, got {other:?} (err={:?})", r.error),
        }
    }

    /// Stage an AddNode into `txn` (used by the phantom scenarios).
    async fn stage_add(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        graph: &str,
        txn: &str,
        node_id: &str,
        props: serde_json::Value,
    ) {
        let r = dispatch(
            state,
            request(
                id,
                graph,
                None,
                Method::TxnAddNode {
                    txn_id: txn.to_string(),
                    node_id: node_id.to_string(),
                    properties_msgpack: node_props(props),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))), "stage");
    }

    /// (a-serializable) Phantom: under `serializable:label=Doc`, txn A declares a
    /// label-scan read-set, txn B inserts a matching `Doc` and commits, then A's
    /// commit returns Bool(false) — the phantom is rejected.
    #[tokio::test]
    async fn txn_serializable_rejects_phantom() {
        let state = test_state();
        // Seed one Doc so the label set is non-empty at begin (not required, but
        // mirrors a real range read).
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "d0".into(),
                        properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                    },
                ),
            )
            .await,
        );

        // Txn A reads the `Doc` label set (declared via the isolation hint) and stages
        // an unrelated write so it has something to commit.
        let a = begin_txn_iso(&state, 2, "__commons__", Some("serializable:label=Doc")).await;
        stage_add(
            &state,
            3,
            "__commons__",
            &a,
            "a_marker",
            serde_json::json!({"type": "Marker"}),
        )
        .await;

        // Txn B inserts a NEW matching Doc and commits — a phantom for A's read-set.
        let b = begin_txn_iso(&state, 4, "__commons__", Some("snapshot")).await;
        stage_add(
            &state,
            5,
            "__commons__",
            &b,
            "d_phantom",
            serde_json::json!({"type": "Doc"}),
        )
        .await;
        assert!(
            commit_bool(&state, 6, "__commons__", &b).await,
            "B (phantom inserter) commits"
        );

        // A's commit must now conflict: the Doc label set changed under it.
        assert!(
            !commit_bool(&state, 7, "__commons__", &a).await,
            "serializable A must reject the phantom (Bool(false))"
        );
        // A applied nothing — its unrelated marker is absent.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(!core.has_node("a_marker"), "conflicted A applied nothing");
    }

    /// (a-snapshot) The SAME phantom scenario under `snapshot` ALLOWS A to commit —
    /// proving the levels differ. A touches no node B touched, so the per-node OCC
    /// read-set sees no conflict and snapshot does not watch the label predicate.
    #[tokio::test]
    async fn txn_snapshot_allows_phantom() {
        let state = test_state();
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "d0".into(),
                        properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                    },
                ),
            )
            .await,
        );

        // A under SNAPSHOT (the default). A label predicate is meaningless here and
        // omitted; A simply stages an unrelated write.
        let a = begin_txn_iso(&state, 2, "__commons__", Some("snapshot")).await;
        stage_add(
            &state,
            3,
            "__commons__",
            &a,
            "a_marker",
            serde_json::json!({"type": "Marker"}),
        )
        .await;

        // B inserts a matching Doc and commits (the phantom).
        let b = begin_txn(&state, 4, "__commons__").await;
        stage_add(
            &state,
            5,
            "__commons__",
            &b,
            "d_phantom",
            serde_json::json!({"type": "Doc"}),
        )
        .await;
        assert!(commit_bool(&state, 6, "__commons__", &b).await, "B commits");

        // Under snapshot, A commits successfully despite the phantom.
        assert!(
            commit_bool(&state, 7, "__commons__", &a).await,
            "snapshot A is allowed to commit through the phantom (Bool(true))"
        );
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(core.has_node("a_marker"), "snapshot A applied its write");
    }

    /// A serializable txn whose predicate set is UNCHANGED still commits (the level
    /// rejects only real anomalies, not every concurrent write).
    #[tokio::test]
    async fn txn_serializable_commits_when_predicate_unchanged() {
        let state = test_state();
        let a = begin_txn_iso(&state, 1, "__commons__", Some("serializable:label=Doc")).await;
        stage_add(
            &state,
            2,
            "__commons__",
            &a,
            "m1",
            serde_json::json!({"type": "Marker"}),
        )
        .await;
        // A concurrent commit that does NOT touch the Doc label set.
        let b = begin_txn(&state, 3, "__commons__").await;
        stage_add(
            &state,
            4,
            "__commons__",
            &b,
            "m2",
            serde_json::json!({"type": "Other"}),
        )
        .await;
        assert!(commit_bool(&state, 5, "__commons__", &b).await, "B commits");
        assert!(
            commit_bool(&state, 6, "__commons__", &a).await,
            "serializable A commits when its Doc predicate set is unchanged"
        );
    }

    /// (c) An unknown isolation value is rejected at BeginTxn (no txn opened).
    #[tokio::test]
    async fn txn_unknown_isolation_rejected() {
        let state = test_state();
        let resp = dispatch(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::BeginTxn {
                    graph: None,
                    isolation: Some("read-committed".into()),
                },
            ),
        )
        .await;
        assert!(
            resp.error.is_some() && resp.result.is_none(),
            "unknown isolation must error, got ok={:?} err={:?}",
            resp.result,
            resp.error
        );
        assert!(
            resp.error.as_deref().unwrap_or("").contains("isolation"),
            "error should name the isolation problem: {:?}",
            resp.error
        );
        // No transaction was registered.
        assert_eq!(
            state.read().await.open_txns.len(),
            0,
            "rejected BeginTxn opens no txn"
        );
    }

    // ── Time-series (CONCEPT:KG-2.210/211) round-trips through full dispatch ──
    #[cfg(feature = "tsdb")]
    const TS_NS: i64 = 1_000_000_000;

    #[cfg(feature = "tsdb")]
    fn ts_points(pts: &[(i64, Vec<f64>)]) -> Vec<u8> {
        rmp_serde::to_vec(&pts.to_vec()).unwrap()
    }

    #[cfg(feature = "tsdb")]
    #[tokio::test]
    async fn ts_append_then_range_via_dispatch() {
        let state = test_state();
        let pts: Vec<(i64, Vec<f64>)> = (0..10).map(|i| (i * TS_NS, vec![i as f64])).collect();
        let r = dispatch(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::TsAppend {
                    series_id: "s".into(),
                    n_fields: 1,
                    bucket_ns: 100 * TS_NS as u64,
                    field_names: vec!["v".into()],
                    points_msgpack: ts_points(&pts),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Count(10))),
            "{:?}",
            r
        );

        let r = dispatch(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::TsRange {
                    series_id: "s".into(),
                    from: 2 * TS_NS,
                    to: 5 * TS_NS,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let got: Vec<(i64, Vec<f64>)> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, 2 * TS_NS);
        assert_eq!(got[2].1[0], 4.0);
    }

    #[cfg(feature = "tsdb")]
    #[tokio::test]
    async fn ts_asof_window_gapfill_via_dispatch() {
        let state = test_state();
        let ticks: Vec<(i64, Vec<f64>)> = (0..20)
            .map(|i| (i * TS_NS, vec![100.0 + i as f64]))
            .collect();
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::TsAppend {
                        series_id: "px".into(),
                        n_fields: 1,
                        bucket_ns: 100 * TS_NS as u64,
                        field_names: vec!["px".into()],
                        points_msgpack: ts_points(&ticks),
                    },
                ),
            )
            .await,
        );

        // ASOF: out-of-order left events, results returned in caller order.
        let left_ts: Vec<i64> = vec![7 * TS_NS, 3 * TS_NS];
        let r = dispatch(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::TsAsofJoin {
                    series_id: "px".into(),
                    left_ts_msgpack: rmp_serde::to_vec(&left_ts).unwrap(),
                    tolerance: -1,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let got: Vec<Option<f64>> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(got, vec![Some(107.0), Some(103.0)]);

        // WINDOW: 10s mean buckets.
        let r = dispatch(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::TsWindow {
                    series_id: "px".into(),
                    from: 0,
                    to: 20 * TS_NS,
                    width: 10 * TS_NS,
                    agg: "mean".into(),
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let bars: Vec<(i64, f64, usize)> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(bars.len(), 2);
        assert!((bars[0].1 - 104.5).abs() < 1e-9);

        // GAP-FILL on a 5s grid.
        let r = dispatch(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::TsGapFill {
                    series_id: "px".into(),
                    from: 0,
                    to: 20 * TS_NS,
                    step: 5 * TS_NS,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let grid: Vec<(i64, f64, bool)> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(grid.len(), 4);
    }

    // ── RDF/SPARQL Method round-trips through dispatch (CONCEPT:KG-2.217/218) ──

    /// AddTriples → GetRdf round-trips through the dispatch chain: Turtle in, the
    /// graph populated, N-Triples out reparses to the same triple set (xsd + @lang).
    #[cfg(feature = "rdf")]
    #[tokio::test]
    async fn test_add_triples_then_get_rdf_round_trips() {
        let state = test_state();
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:knows ex:bob .
ex:bob   a ex:Person ; ex:name "Bob"@en .
"#;
        let r = dispatch(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::AddTriples {
                    turtle: ttl.into(),
                    ntriples: String::new(),
                },
            ),
        )
        .await;
        assert_ok(&r);
        let report: eg_rdf::mapping::LoadReport = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(LoadReport), got {other:?}"),
        };
        assert_eq!(report.triples, 6);
        assert_eq!(report.multivalue, 0);

        let r2 = dispatch(&state, request(2, "__commons__", None, Method::GetRdf)).await;
        assert_ok(&r2);
        let nt: String = match r2.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(String), got {other:?}"),
        };
        let parsed_in = eg_rdf::mapping::parse_turtle(ttl).unwrap();
        let parsed_out = eg_rdf::mapping::parse_ntriples(&nt).unwrap();
        assert_eq!(
            eg_rdf::mapping::triple_set_key(&parsed_in),
            eg_rdf::mapping::triple_set_key(&parsed_out),
            "AddTriples→GetRdf must round-trip the triple set"
        );
    }

    /// Sparql Method round-trips through dispatch: a BGP+FILTER over a loaded graph.
    #[cfg(feature = "sparql")]
    #[tokio::test]
    async fn test_sparql_method_round_trips() {
        let state = test_state();
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:knows ex:bob .
ex:bob   a ex:Person ; ex:name "Bob"   ; ex:age "25"^^xsd:integer .
ex:carol a ex:Person ; ex:name "Carol" ; ex:age "40"^^xsd:integer ; ex:knows ex:alice .
"#;
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddTriples {
                        turtle: ttl.into(),
                        ntriples: String::new(),
                    },
                ),
            )
            .await,
        );
        let r = dispatch(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::Sparql {
                    query: r#"
                        PREFIX ex: <http://example.org/>
                        SELECT ?name WHERE {
                          ?p a ex:Person . ?p ex:name ?name . ?p ex:age ?age .
                          ?p ex:knows ?o . FILTER (?age > 28)
                        }"#
                    .into(),
                    base_iri: String::new(),
                    type_convention: String::new(),
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::SparqlResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(SparqlResult), got {other:?}"),
        };
        let name_idx = res.vars.iter().position(|v| v == "name").unwrap();
        let mut names: Vec<String> = res
            .rows
            .iter()
            .filter_map(|row| row[name_idx].clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice".to_string(), "Carol".to_string()]);
    }

    /// OwlReason Method round-trips through dispatch: an EL existential-restriction
    /// subsumption + an inferred instance membership the property-graph stored no
    /// explicit type edge for, plus a consistency verdict (CONCEPT:KG-2.219).
    #[cfg(feature = "owl")]
    #[tokio::test]
    async fn test_owl_reason_method_round_trips() {
        let state = test_state();
        // TBox + one individual, loaded as RDF; HumanHeart ⊑ HumanComponent is derived
        // through ∃partOf.Body on the LHS — RL cannot reach it.
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Heart rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ] .
[ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ] rdfs:subClassOf ex:HumanComponent .
ex:HumanHeart rdfs:subClassOf ex:Heart .
ex:myHeart a ex:HumanHeart .
"#;
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddTriples {
                        turtle: ttl.into(),
                        ntriples: String::new(),
                    },
                ),
            )
            .await,
        );
        let r = dispatch(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::OwlReason {
                    ontology: String::new(),
                    target_class: "http://example.org/HumanComponent".into(),
                    min_confidence: 0.0,
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::OwlReasonResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlReasonResult), got {other:?}"),
        };
        assert!(res.consistent, "ontology is consistent");
        // Confidence is aligned + present (hard ontology ⇒ all 1.0).
        assert_eq!(res.subclasses.len(), res.subclass_conf.len());
        assert_eq!(res.instances.len(), res.instance_conf.len());
        // The EL-derived subsumption is in the hierarchy.
        assert!(res.subclasses.contains(&(
            "<http://example.org/HumanHeart>".into(),
            "<http://example.org/HumanComponent>".into()
        )));
        // myHeart is an INFERRED HumanComponent (no explicit type edge for it).
        assert!(res.instances.contains(&(
            "<http://example.org/myHeart>".into(),
            "<http://example.org/HumanComponent>".into()
        )));
    }

    /// DISTRIBUTED OwlReason over TWO graphs derives the SAME entailment a single graph
    /// would (CONCEPT:KG-2.236). The shared TBox + p1 live in graph A; p2 lives in graph
    /// B; `OwlReasonDistributed{[A,B]}` unions them and infers p2 ⊑ ScholarlyWork — an
    /// entailment NEITHER shard alone reaches (B has no axioms).
    #[cfg(feature = "owl")]
    #[tokio::test]
    async fn test_owl_reason_distributed_two_graphs() {
        let state = test_state();
        // Graph A: the TBox + individual p1.
        let tbox = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .
ex:p1 a ex:Paper .
"#;
        for (g, doc) in [
            ("__commons__", tbox),
            (
                "shard:b",
                "@prefix ex: <http://example.org/> .\nex:p2 a ex:Article .\n",
            ),
        ] {
            if g == "shard:b" {
                assert_ok(
                    &dispatch(
                        &state,
                        request(
                            10,
                            "__commons__",
                            None,
                            Method::CreateGraph {
                                graph_name: "shard:b".into(),
                                graph_type: GraphType::Commons,
                            },
                        ),
                    )
                    .await,
                );
            }
            assert_ok(
                &dispatch(
                    &state,
                    request(
                        11,
                        g,
                        None,
                        Method::AddTriples {
                            turtle: doc.into(),
                            ntriples: String::new(),
                        },
                    ),
                )
                .await,
            );
        }

        let r = dispatch(
            &state,
            request(
                12,
                "__commons__",
                None,
                Method::OwlReasonDistributed {
                    graphs: vec!["__commons__".into(), "shard:b".into()],
                    ontology: String::new(),
                    target_class: "http://example.org/ScholarlyWork".into(),
                    min_confidence: 0.0,
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::OwlReasonResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlReasonResult), got {other:?}"),
        };
        // p2 (Article, on shard B) is inferred ScholarlyWork via the TBox on shard A —
        // ONLY the union reaches it.
        assert!(
            res.instances.contains(&(
                "<http://example.org/p2>".into(),
                "<http://example.org/ScholarlyWork>".into()
            )),
            "distributed union must infer p2 ⊑ ScholarlyWork; instances={:?}",
            res.instances
        );
        assert_eq!(res.instances.len(), res.instance_conf.len());
        assert!(res.consistent);
    }

    // ── Streaming / CDC / subscriptions / triggers (CONCEPT:KG-2.229/230) ──
    // End-to-end through the FULL dispatch path (the emit hook fires from the
    // write-side-effect block, NOT a direct hub call).

    #[cfg(feature = "streaming")]
    fn doc_node(id: &str, label: &str) -> Method {
        Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type": label}))
                .unwrap(),
        }
    }

    #[cfg(feature = "streaming")]
    fn cdc_events(resp: &Response) -> Vec<crate::wire::CdcEvent> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(b).unwrap(),
            other => panic!("expected Raw(Vec<CdcEvent>), got {other:?}"),
        }
    }

    /// A write through dispatch lands in the CDC feed in order; re-reading from a
    /// later cursor skips what was already seen (CONCEPT:KG-2.229).
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_cdc_ordered_read_from_cursor() {
        let state = test_state();
        assert_ok(
            &dispatch(
                &state,
                request(1, "__commons__", None, doc_node("n1", "Doc")),
            )
            .await,
        );
        assert_ok(
            &dispatch(
                &state,
                request(2, "__commons__", None, doc_node("n2", "Doc")),
            )
            .await,
        );

        // Read from the start: both, in order.
        let r = dispatch(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        let events = cdc_events(&r);
        assert_eq!(events.len(), 2, "two writes → two CDC events");
        assert_eq!(events[0].seq, 0);
        assert_eq!(events[0].node_id, "n1");
        assert_eq!(events[0].label, "Doc");
        assert!(matches!(events[0].kind, crate::wire::CdcKind::AddNode));
        assert_eq!(events[1].seq, 1);
        assert_eq!(events[1].node_id, "n2");

        // Re-read from one past the last seen cursor → empty (skips seen).
        let cursor = events.last().unwrap().seq + 1;
        let r2 = dispatch(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: cursor,
                    limit: 0,
                },
            ),
        )
        .await;
        assert!(cdc_events(&r2).is_empty(), "cursor past head sees nothing");

        // A new write then read-from-cursor returns ONLY it.
        assert_ok(
            &dispatch(
                &state,
                request(
                    5,
                    "__commons__",
                    None,
                    Method::RemoveNode {
                        node_id: "n1".into(),
                    },
                ),
            )
            .await,
        );
        let r3 = dispatch(
            &state,
            request(
                6,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: cursor,
                    limit: 0,
                },
            ),
        )
        .await;
        let tail = cdc_events(&r3);
        assert_eq!(tail.len(), 1);
        assert!(matches!(tail[0].kind, crate::wire::CdcKind::RemoveNode));
        assert_eq!(tail[0].node_id, "n1");

        // ClearGraph through dispatch RESETS the feed (CONCEPT:KG-2.229): the seq
        // rewinds to 0 and the ring empties, so a consumer re-seeds from 0. (This is
        // what gives a wiped/cleared graph a clean change feed.)
        assert_ok(&dispatch(&state, request(7, "__commons__", None, Method::ClearGraph)).await);
        let after_clear = dispatch(
            &state,
            request(
                8,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        assert!(
            cdc_events(&after_clear).is_empty(),
            "ClearGraph resets the CDC feed to empty"
        );
        // A post-clear write is seq 0 again.
        assert_ok(
            &dispatch(
                &state,
                request(9, "__commons__", None, doc_node("fresh", "Doc")),
            )
            .await,
        );
        let reseeded = dispatch(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        let ev = cdc_events(&reseeded);
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].seq, 0,
            "feed rewound — first post-clear change is seq 0"
        );
        assert_eq!(ev[0].node_id, "fresh");
    }

    /// A continuous query maintained incrementally off the CDC feed equals a full
    /// re-run over the final graph state (CONCEPT:KG-2.229).
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_continuous_query_incremental_equals_full_rerun() {
        let state = test_state();
        // Register a Count CQ over label "Doc" BEFORE any writes.
        let spec = crate::wire::ContinuousQuerySpec {
            graph: "__commons__".into(),
            label: "Doc".into(),
            agg: crate::wire::ContinuousAgg::Count,
        };
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::RegisterContinuousQuery {
                        name: "doc_count".into(),
                        spec_msgpack: rmp_serde::to_vec_named(&spec).unwrap(),
                    },
                ),
            )
            .await,
        );

        // Mutations: 3 Doc adds, 1 Other add, 1 Doc remove → 2 live Docs.
        let mut id = 10u64;
        for n in ["a", "b", "c"] {
            assert_ok(
                &dispatch(&state, request(id, "__commons__", None, doc_node(n, "Doc"))).await,
            );
            id += 1;
        }
        assert_ok(
            &dispatch(
                &state,
                request(id, "__commons__", None, doc_node("x", "Other")),
            )
            .await,
        );
        id += 1;
        assert_ok(
            &dispatch(
                &state,
                request(
                    id,
                    "__commons__",
                    None,
                    Method::RemoveNode {
                        node_id: "a".into(),
                    },
                ),
            )
            .await,
        );
        id += 1;

        // Read the incrementally-maintained CQ value.
        let r = dispatch(
            &state,
            request(
                id,
                "__commons__",
                None,
                Method::ReadContinuousQuery {
                    name: "doc_count".into(),
                },
            ),
        )
        .await;
        let cq: crate::wire::ContinuousQueryResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(ContinuousQueryResult), got {other:?}"),
        };

        // ORACLE: full re-run = count nodes with label "Doc" in the final graph.
        let full_rerun = {
            let s = state.read().await;
            let core = s.registry.get("__commons__").unwrap().core.clone();
            core.get_nodes_by_label("Doc", 0).len() as f64
        };
        assert_eq!(
            cq.value, full_rerun,
            "incremental CQ must equal the full re-run"
        );
        assert_eq!(cq.value, 2.0);
    }

    /// A `Watch` long-poll delivers a change to a subscriber, and a registered trigger
    /// fires its action on a matching change (CONCEPT:KG-2.230).
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_watch_and_trigger_delivery() {
        let state = test_state();

        // Register a trigger: any "Alert"-labelled node add records an action.
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::RegisterTrigger {
                        name: "on_alert".into(),
                        graph: "__commons__".into(),
                        label: "Alert".into(),
                        op: "add".into(),
                        action_msgpack: rmp_serde::to_vec_named(
                            &serde_json::json!({"topic": "ops"}),
                        )
                        .unwrap(),
                    },
                ),
            )
            .await,
        );

        // A non-matching write (Doc) does NOT fire the trigger.
        assert_ok(
            &dispatch(
                &state,
                request(2, "__commons__", None, doc_node("d1", "Doc")),
            )
            .await,
        );
        // A matching write (Alert) DOES — and is delivered to a Watch subscriber.
        assert_ok(
            &dispatch(
                &state,
                request(3, "__commons__", None, doc_node("a1", "Alert")),
            )
            .await,
        );

        // Watch from the start, filtered to "Alert": must see exactly the Alert change.
        let w = dispatch(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::Watch {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    label: "Alert".into(),
                    timeout_ms: 0,
                },
            ),
        )
        .await;
        let batch: crate::wire::WatchBatch = match w.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(WatchBatch), got {other:?}"),
        };
        assert_eq!(
            batch.events.len(),
            1,
            "watch delivers only the Alert change"
        );
        assert_eq!(batch.events[0].node_id, "a1");
        assert_eq!(batch.next_seq, batch.events[0].seq + 1);

        // The trigger fired exactly once; poll the fired log for its action.
        let f = dispatch(
            &state,
            request(
                5,
                "__commons__",
                None,
                Method::FiredTriggers {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        let fired: Vec<crate::wire::FiredAction> = match f.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(Vec<FiredAction>), got {other:?}"),
        };
        assert_eq!(fired.len(), 1, "exactly one firing (only the Alert add)");
        assert_eq!(fired[0].trigger, "on_alert");
        assert_eq!(fired[0].node_id, "a1");
        let action: serde_json::Value = rmp_serde::from_slice(&fired[0].action).unwrap();
        assert_eq!(action["topic"], "ops");
    }

    /// `Watch` long-poll wakes when a change lands DURING the poll window
    /// (subscription push semantics over the long-poll transport).
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_watch_long_poll_wakes_on_write() {
        let state = test_state();
        let st2 = state.clone();
        // Spawn a writer that lands a change shortly after the watch begins.
        let writer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let _ = dispatch(
                &st2,
                request(9, "__commons__", None, doc_node("late", "Doc")),
            )
            .await;
        });
        // Watch with a generous timeout; it should return the change once it lands.
        let w = dispatch(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::Watch {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    label: String::new(),
                    timeout_ms: 2000,
                },
            ),
        )
        .await;
        writer.await.unwrap();
        let batch: crate::wire::WatchBatch = match w.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(WatchBatch), got {other:?}"),
        };
        assert_eq!(
            batch.events.len(),
            1,
            "long-poll woke on the in-window write"
        );
        assert_eq!(batch.events[0].node_id, "late");
    }

    /// End-to-end WASM UDF through the SERVER dispatch (CONCEPT:KG-2.228): RegisterUdf
    /// compiles+caches a sandboxed module, then RunUdf runs it over a payload and the
    /// output round-trips — AND an infinite-loop UDF registered the same way is
    /// FUEL-KILLED (a trap error response), never a hang. Proves the Method surface +
    /// the off-reactor sandboxed execution path, not just the eg-wasm unit tests.
    #[cfg(feature = "wasm-udf")]
    #[tokio::test]
    async fn run_udf_through_dispatch_runs_sandboxed_and_fuel_kills_infinite_loop() {
        let state = test_state();

        // An identity UDF (echoes its input bytes) and an infinite-loop UDF.
        let identity = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (global $n (mut i32) (i32.const 1024))
                (func (export "alloc") (param $l i32) (result i32)
                    (local $p i32) (local.set $p (global.get $n))
                    (global.set $n (i32.add (global.get $n) (local.get $l))) (local.get $p))
                (func (export "udf") (param $p i32) (param $l i32) (result i64)
                    (i64.or (i64.shl (i64.extend_i32_u (local.get $p)) (i64.const 32))
                            (i64.extend_i32_u (local.get $l)))))"#,
        )
        .unwrap();
        let infinite = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "alloc") (param $l i32) (result i32) (i32.const 1024))
                (func (export "udf") (param $p i32) (param $l i32) (result i64)
                    (loop $f (br $f)) (i64.const 0)))"#,
        )
        .unwrap();

        // Register both (process-global; the request graph is just the ACL anchor).
        assert_ok(
            &dispatch(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::RegisterUdf {
                        id: "echo".into(),
                        wasm: identity,
                    },
                ),
            )
            .await,
        );
        assert_ok(
            &dispatch(
                &state,
                request(
                    2,
                    "__commons__",
                    None,
                    Method::RegisterUdf {
                        id: "spin".into(),
                        wasm: infinite,
                    },
                ),
            )
            .await,
        );

        // RunUdf "echo" over a payload → the SAME bytes back (sandboxed round-trip).
        let payload = b"rows-over-the-wire".to_vec();
        let resp = dispatch(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::RunUdf {
                    id: "echo".into(),
                    input: payload.clone(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        match resp.result {
            Some(ResultPayload::Raw(out)) => assert_eq!(out, payload, "identity UDF echoes input"),
            other => panic!("expected Raw output, got {other:?}"),
        }

        // RunUdf "spin" → the infinite loop is FUEL-KILLED: an error response, not a hang.
        let start = std::time::Instant::now();
        let resp = dispatch(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::RunUdf {
                    id: "spin".into(),
                    input: b"x".to_vec(),
                },
            ),
        )
        .await;
        assert!(
            resp.error.is_some(),
            "infinite-loop UDF must be killed (error), got ok: {:?}",
            resp.result
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "fuel kill must be fast, not a hang"
        );
    }
}
