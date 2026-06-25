//! Read-only query handler. Owns BOTH query methods (one module per domain, per
//! the dispatch conventions — `Sql` + `CypherQuery` are the one `// ── Query ──`
//! protocol section):
//!   * `Method::Sql` (CONCEPT:KG-2.178, feature `query`) — `SELECT … FROM nodes …`
//!     over ONE graph via DataFusion (eg-query::exec_sql).
//!   * `Method::CypherQuery` (CONCEPT:KG-2.179, feature `cypher`) — `MATCH … RETURN
//!     …` over ONE graph, DEP-FREE (eg-query::exec_cypher; label index / VF2 / BFS,
//!     no DataFusion). This is the lean-Pi query path.
//!
//! Both are read-only — they cannot mutate — so the centralized write side-effects
//! (dirty/WAL/gauge) in the dispatch shell never fire for them.
//!
//! Off-lock execution: take the owned `analysis_snapshot()` (a GraphView that
//! shares property bytes by Arc) under a brief read lock, then run on the blocking
//! pool via `compute_off_lock` — the VF2/algorithm idiom, so no query work runs on
//! a reactor worker or under the graph lock.
//!
//! Each arm is gated on ITS feature and returns `Err(method)` when its feature is
//! off, so a method whose feature is absent falls through to the graph_ops
//! "not available in this build" catch-all (never a panic, never a mis-route).

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use super::super::compute::compute_off_lock;
use crate::graph::GraphCore;
use crate::protocol::Method;
#[cfg(any(feature = "query", feature = "cypher"))]
use crate::protocol::{Response, ResultPayload};
#[cfg(feature = "result-cache")]
use eg_core::result_cache::ResultCache;

/// Handle `Method::Sql` / `Method::CypherQuery`. `Err(method)` hands a non-query
/// method (or a query method whose feature is off) back to the dispatcher
/// (routing fall-through). (CONCEPT:KG-2.19 — server dispatch convention)
pub(crate) async fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        #[cfg(feature = "query")]
        Method::Sql { query, .. } => {
            // Version-keyed result cache (CONCEPT:KG-2.233): a repeated identical SQL
            // on an UNCHANGED graph serves the cached bytes without re-running
            // DataFusion; any write bumped `version()` so the next lookup misses.
            // Snapshot + version are taken atomically so the cached bytes are stored
            // under exactly the version the snapshot reflects.
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let hash = ResultCache::hash_query("sql", query.as_bytes());
                let (snap, version) = core.analysis_snapshot_versioned();
                if let Some(bytes) = core.result_cache().get(hash, version) {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = core.analysis_snapshot();
            let resp =
                match compute_off_lock(req_id, move || eg_query::exec_sql(&snap, &query)).await {
                    Ok(Ok(result)) => {
                        let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                        #[cfg(feature = "result-cache")]
                        core.result_cache().put(hash, version, bytes.clone());
                        Response::ok(req_id, ResultPayload::Raw(bytes))
                    }
                    Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
                    Err(resp) => resp,
                };
            Ok(resp)
        }
        #[cfg(feature = "query")]
        Method::UnifiedQuery {
            plan,
            reorder_filter_selectivity,
        } => {
            // ONE cross-modal plan (CONCEPT:KG-2.208/209): filter (DataFusion) →
            // traverse (BFS) → rank (kNN) over ONE consistent off-lock snapshot. Take
            // BOTH the GraphView (topology + property blobs) and a SemanticStore clone
            // under a brief read each — same point-in-time, so the cross-modal read is
            // snapshot-isolated — then run the whole pipeline on the blocking pool.
            // Version-keyed result cache (CONCEPT:KG-2.233): key on the plan bytes +
            // the reorder flag. The plan + semantic store both reflect `version`, so a
            // write retires the entry.
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let mut payload = rmp_serde::to_vec_named(&plan).unwrap_or_default();
                payload.extend(reorder_filter_selectivity.unwrap_or(f64::NAN).to_le_bytes());
                let hash = ResultCache::hash_query("unified", &payload);
                let (snap, version) = core.analysis_snapshot_versioned();
                if let Some(bytes) = core.result_cache().get(hash, version) {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = core.analysis_snapshot();
            let semantic = core.semantic_store.read().clone();
            let resp = match compute_off_lock(req_id, move || {
                run_unified(plan, reorder_filter_selectivity, &snap, &semantic)
            })
            .await
            {
                Ok(Ok(rows)) => {
                    let bytes = rmp_serde::to_vec_named(&rows).unwrap_or_default();
                    #[cfg(feature = "result-cache")]
                    core.result_cache().put(hash, version, bytes.clone());
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "query")]
        Method::UnifiedQueryText {
            text,
            reorder_filter_selectivity,
        } => {
            // UQL (CONCEPT:KG-2.214): parse the TEXT query into the SAME `wire::Plan`
            // `UnifiedQuery` carries, then run the IDENTICAL `run_unified` executor —
            // a pure front-end, no new execution path. A parse error is a clear,
            // caret-annotated error Response (never a panic).
            // Version-keyed result cache (CONCEPT:KG-2.233): key on the TEXT + reorder
            // flag (the parse is deterministic, so caching pre-parse is sound and
            // skips the parse on a hit too).
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let mut payload = text.clone().into_bytes();
                payload.extend(reorder_filter_selectivity.unwrap_or(f64::NAN).to_le_bytes());
                let hash = ResultCache::hash_query("unified-text", &payload);
                let (snap, version) = core.analysis_snapshot_versioned();
                if let Some(bytes) = core.result_cache().get(hash, version) {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                (snap, version, hash)
            };
            let plan = match eg_plan::uql::parse(&text) {
                Ok(plan) => plan,
                Err(e) => return Ok(Response::err(req_id, e.render(&text))),
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = core.analysis_snapshot();
            let semantic = core.semantic_store.read().clone();
            let resp = match compute_off_lock(req_id, move || {
                run_unified(plan, reorder_filter_selectivity, &snap, &semantic)
            })
            .await
            {
                Ok(Ok(rows)) => {
                    let bytes = rmp_serde::to_vec_named(&rows).unwrap_or_default();
                    #[cfg(feature = "result-cache")]
                    core.result_cache().put(hash, version, bytes.clone());
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "cypher")]
        Method::CypherQuery { query } => {
            // Same off-lock snapshot + blocking-pool idiom as SQL — but DEP-FREE
            // (label index / VF2 / BFS), so it runs in a no-DataFusion Pi build.
            // Version-keyed result cache (CONCEPT:KG-2.233) wraps it identically; this
            // is the lean-Pi cached query path.
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let hash = ResultCache::hash_query("cypher", query.as_bytes());
                let (snap, version) = core.analysis_snapshot_versioned();
                if let Some(bytes) = core.result_cache().get(hash, version) {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = core.analysis_snapshot();
            let resp = match compute_off_lock(req_id, move || eg_query::exec_cypher(&snap, &query))
                .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    #[cfg(feature = "result-cache")]
                    core.result_cache().put(hash, version, bytes.clone());
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        other => Err(other),
    }
}

/// Execute a unified cross-modal plan (CONCEPT:KG-2.208/209) over one off-lock
/// snapshot and return the result rows as `[id, score|nil]`. When
/// `reorder_filter_selectivity` is set, the cost model reorders an adjacent
/// (Filter, Rank) pair before execution (CONCEPT:KG-2.209). Synchronous — runs on
/// the blocking pool via `compute_off_lock`, like the SQL/Cypher legs.
#[cfg(feature = "query")]
fn run_unified(
    plan: eg_plan::Plan,
    reorder_filter_selectivity: Option<f64>,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<Vec<(String, Option<f32>)>, String> {
    use eg_plan::{CostModel, Op, PlanCtx, Stats};

    // Optional cost-based reorder of the adjacent (Filter, Rank) pair. The final
    // top-k requested by a trailing Limit drives the cost asymmetry; default to the
    // seed size if there is no Limit. Seed/embedding counts come straight from the
    // snapshot, so the decision is fed by derivable stats (CONCEPT:KG-2.209).
    let ops = match reorder_filter_selectivity {
        Some(sel) => {
            let seed_rows = view.node_properties.len();
            let top_k = plan
                .ops
                .iter()
                .rev()
                .find_map(|o| match o {
                    Op::Limit { k } => Some(*k),
                    _ => None,
                })
                .unwrap_or(seed_rows.max(1));
            let stats = Stats::estimate(seed_rows, sel, top_k, semantic.len());
            CostModel::reorder_filter_rank(plan.ops, &stats)
        }
        None => plan.ops,
    };

    // `PlanCtx::new` defaults the (feature-gated) text index to `None`, so a
    // `RankText`/`FuseRrf` op served today degrades to no lexical hits rather than
    // erroring. Threading a live BM25 `TextIndex` into `ServerState` (index-on-write +
    // an `AddText`/`IndexText` Method + a persist dir beside graph.redb) is the
    // explicit follow-up integration (CONCEPT:KG-2.215 increment 2); the algebra +
    // index crate land + are proven here.
    let ctx = PlanCtx::new(view, semantic);
    let result = eg_plan::execute(&eg_plan::Plan::new(ops), &ctx)?;
    Ok(result
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect())
}

// ── Version-keyed result cache, end-to-end through dispatch (CONCEPT:KG-2.233) ──
//
// Proves the cache over the REAL `dispatch` entrypoint (auth → routing → handler →
// cache → Cypher), on the lean Pi path (cypher, NO DataFusion):
//   1. the SAME query twice on an UNCHANGED graph HITS (didn't recompute, proven by
//      the cache hit counter) and returns identical bytes;
//   2. a WRITE bumps `version()` → the next identical query MISSES and recomputes a
//      CORRECT (changed) result;
//   3. the CDC feed invalidates a SECOND instance's cache for that graph
//      (cross-replica coherence): a write on A, replayed as a CDC event to B,
//      retires B's cached result so B recomputes.
#[cfg(all(
    test,
    feature = "result-cache",
    feature = "cypher",
    feature = "streaming"
))]
mod result_cache_dispatch_tests {
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::auth::compute_auth_token;
    use crate::server::dispatch;
    use crate::server::state::ServerState;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "result-cache-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
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
            cdc: Some(Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        Request {
            id,
            graph: "__commons__".into(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        }
    }

    async fn add_node(state: &Arc<RwLock<ServerState>>, id: u64, node: &str, label: &str) {
        let props = serde_json::json!({ "type": label });
        let bytes = rmp_serde::to_vec_named(&props).unwrap();
        let r = dispatch(
            state,
            req(
                id,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: bytes,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddNode failed: {:?}", r.error);
    }

    fn raw(resp: &Response) -> Vec<u8> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?}"),
        }
    }

    const Q: &str = "MATCH (n:Person) RETURN n";

    fn cache_stats(core: &Arc<crate::graph::GraphCore>) -> (u64, u64) {
        core.result_cache().stats()
    }

    async fn core_of(state: &Arc<RwLock<ServerState>>) -> Arc<crate::graph::GraphCore> {
        state
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone()
    }

    #[tokio::test]
    async fn hit_on_unchanged_then_write_invalidates() {
        let state = state();
        add_node(&state, 1, "p1", "Person").await;
        add_node(&state, 2, "p2", "Person").await;

        let core = core_of(&state).await;
        let (h0, m0) = cache_stats(&core);

        // First query: cold MISS, computes + caches.
        let r1 = dispatch(&state, req(10, Method::CypherQuery { query: Q.into() })).await;
        assert!(r1.error.is_none());
        let bytes1 = raw(&r1);
        let (h1, m1) = cache_stats(&core);
        assert_eq!((h1 - h0, m1 - m0), (0, 1), "first query is a miss");

        // Second identical query on the UNCHANGED graph: HIT, identical bytes, no
        // recompute (the hit counter moved, the miss counter did not).
        let r2 = dispatch(&state, req(11, Method::CypherQuery { query: Q.into() })).await;
        assert_eq!(raw(&r2), bytes1, "cached bytes identical to computed");
        let (h2, m2) = cache_stats(&core);
        assert_eq!((h2 - h1, m2 - m1), (1, 0), "second query hit the cache");

        // A WRITE bumps version → the cached entry is now unreachable.
        let v_before = core.version();
        add_node(&state, 3, "p3", "Person").await;
        assert_ne!(core.version(), v_before, "write must bump version");

        // Same query again: MISS (recompute), and the result is CORRECT (now 3 rows).
        let r3 = dispatch(&state, req(12, Method::CypherQuery { query: Q.into() })).await;
        assert!(r3.error.is_none());
        let bytes3 = raw(&r3);
        assert_ne!(
            bytes3, bytes1,
            "result changed after the write (recomputed)"
        );
        let (h3, m3) = cache_stats(&core);
        assert_eq!(
            (h3 - h2, m3 - m2),
            (0, 1),
            "post-write query missed + recomputed"
        );

        // And it is cached again at the new version: the next identical query HITS.
        let r4 = dispatch(&state, req(13, Method::CypherQuery { query: Q.into() })).await;
        assert_eq!(raw(&r4), bytes3);
        let (h4, _m4) = cache_stats(&core);
        assert_eq!(h4 - h3, 1, "post-write result is itself cached + re-hit");
    }

    #[tokio::test]
    async fn cdc_drives_cross_instance_invalidation() {
        // Two independent in-process instances A and B (separate registries/caches),
        // each holding the SAME logical graph + the SAME data.
        let a = state();
        let b = state();
        for (id, n) in [(1u64, "p1"), (2, "p2")] {
            add_node(&a, id, n, "Person").await;
            add_node(&b, id, n, "Person").await;
        }
        let core_b = core_of(&b).await;

        // Warm B's cache: query B once (miss) then again (hit) — B is now serving a
        // cached result for the graph.
        let _ = dispatch(&b, req(20, Method::CypherQuery { query: Q.into() })).await;
        let r_hit = dispatch(&b, req(21, Method::CypherQuery { query: Q.into() })).await;
        let (h_before, _m) = cache_stats(&core_b);
        assert!(h_before >= 1, "B should have a warm cache hit");
        let bytes_b_old = raw(&r_hit);

        // A WRITE lands on A. A emits a CDC event into its feed (the dispatch shell
        // does this for every durable mutation). Grab A's CDC feed.
        add_node(&a, 3, "p3", "Person").await;
        let hub_a = {
            let s = a.read().await;
            s.cdc.clone().unwrap()
        };
        // A produced at least one CDC event for the AddNode.
        let events = hub_a.read("__commons__", 0, 0).unwrap();
        assert!(!events.is_empty(), "A's write must emit a CDC event");

        // COHERENCE: drain A's feed into B → B invalidates its local cache for the
        // graph (bumps B's version). This is the cross-replica invalidation signal.
        let v_b_before = core_b.version();
        let next =
            crate::server::cache_coherence::drain_and_invalidate(&b, &hub_a, "__commons__", 0, 0)
                .await
                .unwrap();
        assert!(next > 0, "drained at least one event");
        assert_ne!(
            core_b.version(),
            v_b_before,
            "B's version bumped on the remote change"
        );

        // B's previously-cached result is now unreachable: the SAME query MISSES.
        let (h2, m2) = cache_stats(&core_b);
        let r_after = dispatch(&b, req(22, Method::CypherQuery { query: Q.into() })).await;
        let (h3, m3) = cache_stats(&core_b);
        assert_eq!(
            (h3 - h2, m3 - m2),
            (0, 1),
            "after CDC invalidation B recomputes (miss), not a stale hit"
        );
        // B still has its OWN old data (we only invalidated the cache, didn't
        // replicate the row), so the recomputed bytes equal the pre-write bytes — the
        // point proven is the INVALIDATION fired, not data replication.
        assert_eq!(raw(&r_after), bytes_b_old);
    }
}
