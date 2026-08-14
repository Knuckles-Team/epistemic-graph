//! Governed query handler. Owns BOTH query methods (one module per domain, per
//! the dispatch conventions — `Sql` + `CypherQuery` are the one `// ── Query ──`
//! protocol section):
//!   * `Method::Sql` (CONCEPT:EG-KG.query.read-only-sql-query, feature `query`) — `SELECT … FROM nodes …`
//!     over ONE graph via DataFusion (eg-query::exec_sql).
//!   * `Method::CypherQuery` (CONCEPT:EG-KG.query.dep-free-behind, feature `cypher`) — `MATCH … RETURN
//!     …` over ONE graph, DEP-FREE (eg-query::exec_cypher; label index / VF2 / BFS,
//!     no DataFusion). This is the lean-Pi query path.
//!
//! SQL and Cypher reads use detached, policy-filtered snapshots. Query-language
//! writes are staged and committed by the centralized MutationBatch boundary.
//! Cypher's explicit wire mode is verified against the native parser before
//! admission, authorization, or persistence selection.
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

use tokio::sync::RwLock;

use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::graph::GraphCore;
use crate::protocol::Method;
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
use crate::protocol::{Response, ResultPayload};
use crate::server::access::GraphReadAuthority;
#[cfg(feature = "result-cache")]
use eg_core::result_cache::ResultCache;

/// Verify that Cypher's explicit wire mode agrees with the native parser.
///
/// The mode is an authorization and durability claim, not a parser hint. Callers
/// must reject a mismatch before choosing a read lane or MutationBatch path.
#[cfg(feature = "cypher")]
pub(crate) fn validate_cypher_mode(method: &Method) -> Result<(), String> {
    let Method::CypherQuery { query, mode } = method else {
        return Ok(());
    };
    let parsed_mode = match eg_query::classify_cypher(query) {
        Ok(eg_query::CypherStatementKind::Read) => crate::protocol::CypherMode::Read,
        Ok(eg_query::CypherStatementKind::Write) => crate::protocol::CypherMode::Write,
        Err(message) => return Err(format!("Cypher error: {message}")),
    };
    if &parsed_mode != mode {
        return Err("Cypher error: declared mode does not match the parsed statement".to_string());
    }
    Ok(())
}

/// Process-wide GraphQL cross-modal transaction registry (CONCEPT:EG-KG.query.facade-reconcile-hook). Holds staged
/// multi-request cross-modal txns (`beginTransaction` … `stage*` … `commitTransaction`)
/// across GraphQL requests — GraphQL over the RPC transport has no per-connection session,
/// and txn ids are process-unique, so ONE shared registry is the carrier (the `OnceLock`
/// idiom the UQL text-embedder seam uses).
#[cfg(feature = "graphql")]
fn graphql_crossmodal_registry() -> &'static eg_graphql::CrossModalTxnRegistry {
    use std::sync::OnceLock;
    static REG: OnceLock<eg_graphql::CrossModalTxnRegistry> = OnceLock::new();
    REG.get_or_init(eg_graphql::CrossModalTxnRegistry::new)
}

#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) fn plan_needs_tsdb(ops: &[eg_plan::Op]) -> bool {
    ops.iter().any(|op| match op {
        eg_plan::Op::TsScan { .. } => true,
        #[cfg(feature = "text")]
        eg_plan::Op::FuseRrf { branches, .. } => {
            branches.iter().any(|branch| plan_needs_tsdb(branch))
        }
        _ => false,
    })
}

/// Resolve an actor-owned storage namespace before a served plan can touch the
/// committed TSDB. Graph ACL/RLS actor identity alone is not a tenant carrier.
/// `pub(crate)`: also the single source of truth `handlers::mining`'s plan-sourced
/// `TsScan` leg reuses (CONCEPT:EG-KG.mining.tsdb-typed-absent) rather than
/// re-deriving the same tenant/namespace scope a second time.
#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) fn served_tsdb_scope(
    plan: &eg_plan::Plan,
    graph: &str,
    read_authority: Option<&GraphReadAuthority>,
) -> Result<Option<(String, String)>, String> {
    if !plan_needs_tsdb(&plan.ops) {
        return Ok(None);
    }
    let carrier = read_authority
        .and_then(GraphReadAuthority::carrier)
        .ok_or_else(|| {
            crate::metrics::access_denied();
            "ACCESS_DENIED: TsScan requires a verified tenant+actor carrier".to_string()
        })?;
    Ok(Some((
        carrier.tenant_scope().to_string(),
        carrier.namespace("timeseries-graph", graph),
    )))
}

/// Handle `Method::Sql` / `Method::CypherQuery`. `Err(method)` hands a non-query
/// method (or a query method whose feature is off) back to the dispatcher
/// (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention — server dispatch convention)
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    ctx: super::TryHandleContext<'_>,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    let super::TryHandleContext {
        req_id,
        graph_name,
        read_authority,
        caller,
    } = ctx;
    #[cfg(not(feature = "security"))]
    let _ = caller;
    // `state` is consumed only by the `query`-gated in-txn cross-modal RYOW arms
    // (CONCEPT:EG-KG.query.txn-cross-modal-ryow — TxnUnifiedQuery{,Text}); keep it referenced in a
    // cypher/graphql-only build (no `query`) so no dead-param warning fires.
    #[cfg(not(feature = "query"))]
    let _ = (state, read_authority);
    // `graph_name` is consumed only by the `graphql`-gated cross-modal durable commit
    // (CONCEPT:EG-KG.query.facade-reconcile-hook); keep it referenced in a query/cypher-only build so no dead-param
    // warning fires.
    #[cfg(not(feature = "graphql"))]
    let _ = graph_name;
    match method {
        #[cfg(feature = "query")]
        Method::Sql {
            query,
            params_msgpack,
        } => {
            let sql_method = Method::Sql {
                query: query.clone(),
                params_msgpack,
            };
            // CONCEPT:EG-KG.query.mirrors-pgwire — `Method::Sql` now routes BOTH reads AND writes (was
            // SELECT-only). Classify the statement with the SAME `eg_query::classify`
            // the pgwire shim uses, then:
            //   * a write (graph-node DML on `nodes`, or user-table DDL/DML) → the
            //     classify+execute path pgwire uses (graph core writes + the shared
            //     `TableStore`), so agent-utilities `graph_table`/`sql_exec` writes
            //     LAND over the wire — see `exec_sql_write`;
            //   * a read / transaction-control / unparseable statement → the
            //     DataFusion read path, run tables-aware (`exec_sql_typed_with_tables`)
            //     so a `SELECT` sees BOTH the graph AND user tables in one plan.
            //
            // SQL catalogs are owned by the current verified tenant+principal. Reads
            // and writes resolve the same owner store; there is no shared catalog or
            // unsigned lookup path.
            let Some(read_authority) = read_authority else {
                crate::metrics::access_denied();
                return Ok(Response::err(
                    req_id,
                    "ACCESS_DENIED: current signed tenant authority is required".to_string(),
                ));
            };
            let Some(authority) = read_authority.carrier() else {
                crate::metrics::access_denied();
                return Ok(Response::err(
                    req_id,
                    "ACCESS_DENIED: current signed tenant authority is required".to_string(),
                ));
            };
            let persist_dir = state.read().await.persist_dir.clone();
            let store = match crate::server::sql_tables::user_table_store(
                authority,
                persist_dir.as_deref().map(std::path::Path::new),
            ) {
                Ok(s) => s,
                Err(e) => return Ok(Response::err(req_id, format!("SQL error: {e}"))),
            };
            match eg_query::classify(&query) {
                Ok(kind) if !matches!(kind, eg_query::StatementKind::Read) => Ok(exec_sql_write(
                    req_id,
                    SqlWriteScope {
                        graph_name,
                        tenant_scope: authority.tenant_scope(),
                        caller: Some(authority.actor_scope()),
                    },
                    read_authority,
                    sql_method,
                    &core,
                    &store,
                    kind,
                )
                .await),
                _ => {
                    // Read (or an unparseable statement — exec surfaces the precise
                    // parse error). RLS-filter the off-lock snapshot to the caller's
                    // visible rows BEFORE execution so a SELECT cannot exfiltrate a
                    // forbidden row. `analysis_snapshot_versioned` (not the bare
                    // `analysis_snapshot`) so the OCC version used to key the served
                    // context cache below is taken ATOMICALLY with the snapshot it
                    // describes — they can never drift apart.
                    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
                    let (mut snap, graph_version) = core.analysis_snapshot_versioned();
                    // W1.6/P7 site 3: the node epoch gates the SQL-context node-batch sub-cache so a
                    // pure-edge / catalog-only write reuses the O(V) node scan. The dependency clock
                    // folds the coarse floor into it, keeping it sound for bypass writes; without
                    // result-cache, fall back to graph_version (correct, no reuse).
                    #[cfg(feature = "result-cache")]
                    let node_epoch = core.dep_clock().node_epoch();
                    #[cfg(not(feature = "result-cache"))]
                    let node_epoch = graph_version;
                    #[cfg(feature = "security")]
                    rls.filter_view(caller, &mut snap);
                    // CONCEPT:EG-KG.query.served-context-cache — the whole-`SessionContext` cache (UDFs,
                    // durable views, synthesized system catalogs), amortized across every
                    // served SQL read for this owner. One instance PER owner-scoped SQL
                    // catalog (the same registry key `user_table_store` resolves `store`
                    // by), so repeated calls from the SAME tenant+actor actually reuse
                    // it — not just within one request.
                    let context_cache = match crate::server::sql_tables::sql_context_cache(
                        authority,
                        persist_dir.as_deref().map(std::path::Path::new),
                    ) {
                        Ok(c) => c,
                        Err(e) => return Ok(Response::err(req_id, format!("SQL error: {e}"))),
                    };
                    let tenant_scope = authority.tenant_scope().to_string();
                    let graph_name_owned = graph_name.to_string();
                    let caller_owned = caller.to_string();
                    // L36 (CONCEPT:EG-KG.query.streaming-spillable-collect) — a REAL, request-scoped
                    // `CancellationToken`: registered under THIS request's `req_id` for the
                    // duration of the call so an explicit client `Method::CancelRequest` or a
                    // server-side per-request deadline (`EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS`,
                    // via `spawn_timeout`) can trip it — observed by `collect_streaming` at its
                    // NEXT batch boundary, stopping the stream short instead of the always-fresh,
                    // never-cancelled token this handler built internally before this fix. The
                    // guard removes the registry entry when this arm returns (success, error, or
                    // panic-unwind), so a completed request is never left cancellable.
                    let cancel = eg_query::CancellationToken::new();
                    let _cancel_guard =
                        crate::server::request_cancel::register(req_id, cancel.clone());
                    let timeout_task = crate::server::request_cancel::spawn_timeout(cancel.clone());
                    let cancel_for_task = cancel.clone();
                    let resp = match compute_off_lock(req_id, move || {
                        eg_query::exec_sql_typed_with_tables_cached_cancellable(
                            &snap,
                            graph_version,
                            node_epoch,
                            &tenant_scope,
                            &graph_name_owned,
                            &caller_owned,
                            &store,
                            &context_cache,
                            &query,
                            &cancel_for_task,
                        )
                    })
                    .await
                    {
                        Ok(Ok(typed)) => {
                            let result = crate::protocol::QueryResult {
                                columns: typed.columns.iter().map(|c| c.name.clone()).collect(),
                                rows: typed
                                    .rows
                                    .iter()
                                    .map(|r| rmp_serde::to_vec_named(r).unwrap_or_default())
                                    .collect(),
                            };
                            let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                            Response::ok(req_id, ResultPayload::Raw(bytes))
                        }
                        Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
                        Err(resp) => resp,
                    };
                    if let Some(t) = timeout_task {
                        t.abort();
                    }
                    Ok(resp)
                }
            }
        }
        #[cfg(feature = "query")]
        Method::UnifiedQuery { plan } => {
            #[cfg(feature = "tsdb")]
            let tsdb_scope = match served_tsdb_scope(&plan, graph_name, read_authority) {
                Ok(scope) => scope,
                Err(denied) => return Ok(Response::err(req_id, denied)),
            };
            // ONE cross-modal plan (CONCEPT:AU-KG.compute.vector/209): filter (DataFusion) →
            // traverse (BFS) → rank (kNN) over ONE consistent off-lock snapshot. Take
            // BOTH the GraphView (topology + property blobs) and a SemanticStore clone
            // under a brief read each — same point-in-time, so the cross-modal read is
            // snapshot-isolated — then run the whole pipeline on the blocking pool.
            // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): key
            // on the plan bytes + the caller's RLS context. The plan + semantic store
            // both reflect `version`, so a write retires the entry; the
            // RLS-context salt keeps agent A's fused result out of agent B's lookups.
            // Dependency-scoped invalidation (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation,
            // W1.6/P7): a plan reducible to a bounded node read (Scan/Filter/Limit) is cached in
            // the dependency-scoped namespace, so it survives every write DISJOINT from its
            // labels; any other plan shape keeps the coarse version-keyed path unchanged.
            #[cfg(feature = "result-cache")]
            let dep = plan_dependency_set(&plan);
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let mut payload = rmp_serde::to_vec_named(&plan).unwrap_or_default();
                #[cfg(feature = "tsdb")]
                if let Some((tenant, graph)) = tsdb_scope.as_ref() {
                    payload.extend_from_slice(tenant.as_bytes());
                    payload.extend_from_slice(graph.as_bytes());
                }
                let hash = rls_cache_hash(
                    "unified",
                    &payload,
                    #[cfg(feature = "security")]
                    caller,
                    #[cfg(feature = "security")]
                    rls,
                );
                let (mut snap, version) = core.analysis_snapshot_versioned();
                let cached = match &dep {
                    Some(_) => core.result_cache().get_dep(hash, 0, core.dep_clock()),
                    None => core.result_cache().get(hash, version),
                };
                if let Some(bytes) = cached {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                #[cfg(feature = "security")]
                rls.filter_view(caller, &mut snap);
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            // CONCEPT:EG-KG.query.served-vector-index-binding / served-text-index-binding — push the
            // vector kNN AND lexical legs down into the LIVE persistent indexes instead
            // of cloning/rebuilding them per request: `core` (an `Arc`, cheap to clone)
            // is moved into the off-lock closure so the `SemanticStore` read guard is
            // taken THERE, on the blocking pool, never here on the async task — see
            // `run_unified`'s new `served_text` param doc for why this replaces the old
            // `core.semantic_store.read().clone()` (which forced a full HNSW rebuild on
            // the clone's first search under the default backend).
            let core_for_ctx = core.clone();
            // RECONCILE (CONCEPT:EG-KG.query.native-time-series): committed tsdb store for `Op::TsScan` fusion.
            #[cfg(feature = "tsdb")]
            let tsdb = if tsdb_scope.is_some() {
                state.read().await.tsdb_store.clone()
            } else {
                None
            };
            #[cfg(feature = "tsdb")]
            let (tsdb_tenant, tsdb_graph) = match tsdb_scope {
                Some((tenant, graph)) => (Some(tenant), Some(graph)),
                None => (None, None),
            };
            let resp = match compute_off_lock(req_id, move || {
                #[cfg(feature = "text")]
                let served_text =
                    crate::server::secondary_indexes::ServedTextIndex::new(core_for_ctx.clone());
                #[cfg(feature = "geo")]
                let served_spatial =
                    crate::server::secondary_indexes::ServedSpatialIndex::new(core_for_ctx.clone());
                let semantic_guard = core_for_ctx.semantic_store.read();
                run_unified(
                    plan,
                    &snap,
                    &semantic_guard,
                    ServedIndexes {
                        #[cfg(feature = "text")]
                        text: Some(&served_text),
                        #[cfg(feature = "geo")]
                        spatial: Some(&served_spatial),
                        #[cfg(not(any(feature = "text", feature = "geo")))]
                        _marker: std::marker::PhantomData,
                    },
                    #[cfg(feature = "tsdb")]
                    TsdbLegBind {
                        tsdb: tsdb.as_deref(),
                        tsdb_tenant: tsdb_tenant.as_deref(),
                        tsdb_graph: tsdb_graph.as_deref(),
                        // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                        staged_series: None,
                    },
                )
            })
            .await
            {
                Ok(Ok(rows)) => {
                    let bytes = rmp_serde::to_vec_named(&rows).unwrap_or_default();
                    #[cfg(feature = "result-cache")]
                    match &dep {
                        // Dependency-scoped store: computed against `version`, tagged with the
                        // dependency set the plan read, so a disjoint write leaves it valid (W1.6/P7).
                        Some(deps) => core.result_cache().put_dep(
                            hash,
                            0,
                            version,
                            deps.clone(),
                            bytes.clone(),
                        ),
                        None => core.result_cache().put(hash, version, bytes.clone()),
                    }
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "query")]
        Method::UnifiedQueryText { text } => {
            let plan = match eg_plan::uql::parse(&text) {
                Ok(plan) => plan,
                Err(e) => return Ok(Response::err(req_id, e.render(&text))),
            };
            #[cfg(feature = "tsdb")]
            let tsdb_scope = match served_tsdb_scope(&plan, graph_name, read_authority) {
                Ok(scope) => scope,
                Err(denied) => return Ok(Response::err(req_id, denied)),
            };
            // UQL (CONCEPT:AU-KG.query.top-nodes-by-degree): parse the TEXT query into the SAME `wire::Plan`
            // `UnifiedQuery` carries, then run the IDENTICAL `run_unified` executor —
            // a pure front-end, no new execution path. A parse error is a clear,
            // caret-annotated error Response (never a panic).
            // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): key
            // on the text + the caller's RLS context (the parse is deterministic, so
            // caching pre-parse is sound and skips the parse on a hit too). The
            // RLS-context salt keeps agent A's result out of agent B's lookups.
            // Dependency-scoped invalidation (W1.6/P7): identical to the `UnifiedQuery` arm — a
            // Scan/Filter/Limit plan is cached in the dependency-scoped namespace (survives
            // disjoint writes); any other shape keeps the version-keyed path.
            #[cfg(feature = "result-cache")]
            let dep = plan_dependency_set(&plan);
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let mut payload = text.clone().into_bytes();
                #[cfg(feature = "tsdb")]
                if let Some((tenant, graph)) = tsdb_scope.as_ref() {
                    payload.extend_from_slice(tenant.as_bytes());
                    payload.extend_from_slice(graph.as_bytes());
                }
                let hash = rls_cache_hash(
                    "unified-text",
                    &payload,
                    #[cfg(feature = "security")]
                    caller,
                    #[cfg(feature = "security")]
                    rls,
                );
                let (mut snap, version) = core.analysis_snapshot_versioned();
                let cached = match &dep {
                    Some(_) => core.result_cache().get_dep(hash, 0, core.dep_clock()),
                    None => core.result_cache().get(hash, version),
                };
                if let Some(bytes) = cached {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                #[cfg(feature = "security")]
                rls.filter_view(caller, &mut snap);
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            // See the `UnifiedQuery` arm above: push the vector + lexical legs down
            // into the live persistent indexes via a guard taken INSIDE the off-lock
            // closure, instead of pre-cloning the whole `SemanticStore` here.
            let core_for_ctx = core.clone();
            // RECONCILE (CONCEPT:EG-KG.query.native-time-series): committed tsdb store for `Op::TsScan` fusion.
            #[cfg(feature = "tsdb")]
            let tsdb = if tsdb_scope.is_some() {
                state.read().await.tsdb_store.clone()
            } else {
                None
            };
            #[cfg(feature = "tsdb")]
            let (tsdb_tenant, tsdb_graph) = match tsdb_scope {
                Some((tenant, graph)) => (Some(tenant), Some(graph)),
                None => (None, None),
            };
            let resp = match compute_off_lock(req_id, move || {
                #[cfg(feature = "text")]
                let served_text =
                    crate::server::secondary_indexes::ServedTextIndex::new(core_for_ctx.clone());
                #[cfg(feature = "geo")]
                let served_spatial =
                    crate::server::secondary_indexes::ServedSpatialIndex::new(core_for_ctx.clone());
                let semantic_guard = core_for_ctx.semantic_store.read();
                run_unified(
                    plan,
                    &snap,
                    &semantic_guard,
                    ServedIndexes {
                        #[cfg(feature = "text")]
                        text: Some(&served_text),
                        #[cfg(feature = "geo")]
                        spatial: Some(&served_spatial),
                        #[cfg(not(any(feature = "text", feature = "geo")))]
                        _marker: std::marker::PhantomData,
                    },
                    #[cfg(feature = "tsdb")]
                    TsdbLegBind {
                        tsdb: tsdb.as_deref(),
                        tsdb_tenant: tsdb_tenant.as_deref(),
                        tsdb_graph: tsdb_graph.as_deref(),
                        // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                        staged_series: None,
                    },
                )
            })
            .await
            {
                Ok(Ok(rows)) => {
                    let bytes = rmp_serde::to_vec_named(&rows).unwrap_or_default();
                    #[cfg(feature = "result-cache")]
                    match &dep {
                        // Dependency-scoped store: computed against `version`, tagged with the
                        // dependency set the plan read, so a disjoint write leaves it valid (W1.6/P7).
                        Some(deps) => core.result_cache().put_dep(
                            hash,
                            0,
                            version,
                            deps.clone(),
                            bytes.clone(),
                        ),
                        None => core.result_cache().put(hash, version, bytes.clone()),
                    }
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // ── EXPLAIN surfaces (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────────
        #[cfg(feature = "query")]
        Method::ExplainPlan { plan } => {
            let snap = explain_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            // L34: reuse the served `UnifiedQuery` idiom instead of a per-request
            // `SemanticStore` clone — move the cheap `Arc<GraphCore>` clone into the
            // off-lock closure and take the read guard THERE, on the blocking pool. This
            // diagnostic surface is low-traffic, but the fix costs nothing (same clone
            // count: an `Arc` clone instead of a whole-store clone) so there is no reason
            // to keep the heavier path.
            let core_for_ctx = core.clone();
            let resp = match compute_off_lock(req_id, move || {
                let semantic = core_for_ctx.semantic_store.read();
                explain_plan(plan, &snap, &semantic)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("ExplainPlan error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "query")]
        Method::ExplainProvenance { plan } => {
            let snap = explain_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            // L34: see `ExplainPlan` above — clone the cheap `Arc<GraphCore>` into the
            // closure and take the `SemanticStore` read guard inside it, instead of
            // cloning the whole store per request.
            let core_for_ctx = core.clone();
            let resp = match compute_off_lock(req_id, move || {
                let semantic = core_for_ctx.semantic_store.read();
                explain_provenance(req_id, plan, &snap, &semantic)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("ExplainProvenance error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of `ExplainProvenance`: same
        // per-row epistemic wire shape, no `Op` plan needed.
        #[cfg(feature = "query")]
        Method::ExplainProvenanceByIds { ids } => {
            let snap = explain_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let core_for_ctx = core.clone();
            let resp = match compute_off_lock(req_id, move || {
                let semantic = core_for_ctx.semantic_store.read();
                explain_provenance_by_ids(req_id, ids, &snap, &semantic)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => {
                    Response::err(req_id, format!("ExplainProvenanceByIds error: {msg}"))
                }
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "query")]
        Method::ExplainPolicy { plan } => {
            // BOTH the unfiltered snapshot and the caller's RLS-filtered one, so the
            // diagnostic can report exactly which rows the policy denied (reuses the
            // SAME `IsolationLayer::filter_view` every other read path applies).
            let full = core.analysis_snapshot();
            let filtered = explain_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            // L34: see `ExplainPlan` above — clone the cheap `Arc<GraphCore>` into the
            // closure and take the `SemanticStore` read guard inside it, instead of
            // cloning the whole store per request.
            let core_for_ctx = core.clone();
            let resp = match compute_off_lock(req_id, move || {
                let semantic = core_for_ctx.semantic_store.read();
                explain_policy(plan, &full, &filtered, &semantic)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("ExplainPolicy error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // L51: redaction-aware arm. Handles BOTH `disclosure_level: None` (byte-for-
        // byte the classic path below) AND `Some(_)` (routes through
        // `eg_epistemic::redact::explain_belief_redacted_capped` under the caller's
        // own RLS actor). Mutually exclusive with the arm below via `cfg` — exactly
        // one of the two is ever compiled for a given `Method::ExplainBelief` pattern.
        #[cfg(feature = "epistemic-redaction")]
        Method::ExplainBelief {
            node_id,
            disclosure_level,
        } => {
            let snap = core.analysis_snapshot();
            let caller_id = caller.to_string();
            let rls = rls.clone();
            let resp = match disclosure_level {
                None => {
                    match compute_off_lock(req_id, move || explain_belief(&node_id, &snap)).await {
                        Ok(result) => {
                            let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                            Response::ok(req_id, ResultPayload::Raw(bytes))
                        }
                        Err(resp) => resp,
                    }
                }
                Some(cap) => {
                    match compute_off_lock(req_id, move || {
                        explain_belief_redacted_wire(&node_id, &snap, cap, &rls, &caller_id)
                    })
                    .await
                    {
                        Ok(result) => {
                            let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                            Response::ok(req_id, ResultPayload::Raw(bytes))
                        }
                        Err(resp) => resp,
                    }
                }
            };
            Ok(resp)
        }
        // Classic-only arm: compiled when `epistemic` is on but `epistemic-redaction`
        // is off. `disclosure_level: Some(_)` gets an explicit error — never a silent
        // fall-back to the un-redacted tree, which would leak exactly what redaction
        // exists to hide.
        #[cfg(all(feature = "epistemic", not(feature = "epistemic-redaction")))]
        Method::ExplainBelief {
            node_id,
            disclosure_level,
        } => {
            if disclosure_level.is_some() {
                return Ok(Response::err(
                    req_id,
                    "ExplainBelief.disclosure_level requires the epistemic-redaction \
                     feature, not enabled in this build"
                        .to_string(),
                ));
            }
            let snap = core.analysis_snapshot();
            let resp = match compute_off_lock(req_id, move || explain_belief(&node_id, &snap)).await
            {
                Ok(result) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // L53 (EPI-P3-5): the acceptance capstone.
        #[cfg(feature = "epistemic-tms")]
        Method::EpistemicStatus { node_id } => {
            let snap = core.analysis_snapshot();
            let resp = match compute_off_lock(req_id, move || {
                epistemic_status_wire(&node_id, &snap)
            })
            .await
            {
                Ok(result) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // L53 (EPI-P3-5): the one facet not subsumed by `EpistemicStatus` — a
        // whole-graph bitemporal diff between two transaction times.
        #[cfg(feature = "epistemic-tms")]
        Method::WhatChanged { tx_from, tx_to } => {
            let snap = core.analysis_snapshot();
            let resp =
                match compute_off_lock(req_id, move || what_changed_wire(&snap, tx_from, tx_to))
                    .await
                {
                    Ok(result) => {
                        let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                        Response::ok(req_id, ResultPayload::Raw(bytes))
                    }
                    Err(resp) => resp,
                };
            Ok(resp)
        }
        // Fenced recompute/writeback against the durable per-graph projection. The
        // graph snapshot supplies current provenance; request data cannot choose the
        // dependency set or generator persisted by the projection.
        #[cfg(feature = "epistemic-tms")]
        Method::RecomputeMaterialization {
            derived_id,
            expected_source_graph_version,
        } => {
            #[cfg(feature = "raft")]
            if crate::server::dispatch::is_replicated_apply() {
                let method = Method::RecomputeMaterialization {
                    derived_id: derived_id.clone(),
                    expected_source_graph_version,
                };
                let persistence = state.read().await.persistence.clone();
                let Some(persistence) = persistence else {
                    return Ok(Response::err(
                        req_id,
                        "reasoning recompute requires an authoritative MutationBatch backend",
                    ));
                };
                let graph_fname = crate::persist::sanitize(graph_name);
                let authoritative_graph_version =
                    match crate::server::mutation_batch::authoritative_graph_version(
                        &persistence,
                        &graph_fname,
                        &core,
                    )
                    .await
                    {
                        Ok(version) => version,
                        Err(error) => return Ok(Response::err(req_id, error)),
                    };
                if authoritative_graph_version != expected_source_graph_version {
                    return Ok(Response::err(
                        req_id,
                        "STALE_RECOMPUTE_FENCE: authoritative graph version changed",
                    ));
                }
                let Some(target_graph_version) = expected_source_graph_version.checked_add(1)
                else {
                    return Ok(Response::err(
                        req_id,
                        "reasoning recompute graph version exhausted",
                    ));
                };
                let result = crate::protocol::RecomputeMaterializationResult {
                    id: eg_epistemic::projection_identity(&derived_id),
                    depends_on: Vec::new(),
                    generating_activity: None,
                    status: "Queued".to_string(),
                    source_graph_version: target_graph_version,
                    fence_epoch: 0,
                    projection_pending: true,
                };
                let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                let payload = ResultPayload::Raw(bytes.clone());
                let batch_id = crate::server::mutation_batch::opaque_request_key(
                    "reasoning-recompute",
                    graph_name,
                    req_id,
                    &method,
                );
                if let Err(error) = crate::server::mutation_batch::commit_internal_graph_methods(
                    Some(&persistence),
                    &core,
                    req_id,
                    Some(caller),
                    graph_name,
                    &batch_id,
                    vec![method],
                    &payload,
                )
                .await
                {
                    return Ok(Response::err(req_id, error));
                }
                return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
            }
            let authoritative_graph_version = core.version();
            let snap = core.analysis_snapshot();
            let persist_dir = state.read().await.persist_dir.clone();
            let (materialization, fence_epoch) =
                match crate::server::reasoning_projection::recompute_materialization(
                    persist_dir.as_deref(),
                    graph_name,
                    &snap,
                    authoritative_graph_version,
                    &derived_id,
                    expected_source_graph_version,
                ) {
                    Ok(value) => value,
                    Err(error) => return Ok(Response::err(req_id, error)),
                };
            let result = crate::protocol::RecomputeMaterializationResult {
                id: materialization.materialization_ref,
                depends_on: materialization.dependency_refs,
                generating_activity: materialization.generator_ref,
                status: format!("{:?}", materialization.status),
                source_graph_version: materialization.source_graph_version,
                fence_epoch,
                projection_pending: false,
            };
            let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
            Ok(Response::ok(req_id, ResultPayload::Raw(bytes)))
        }
        // Read-only status lookup on the durable per-graph projection.
        #[cfg(feature = "epistemic-tms")]
        Method::MaterializationStatus { id } => {
            let persist_dir = state.read().await.persist_dir.clone();
            let (status, source_graph_version) =
                match crate::server::reasoning_projection::materialization_status(
                    persist_dir.as_deref(),
                    graph_name,
                    &id,
                ) {
                    Ok(value) => value,
                    Err(error) => return Ok(Response::err(req_id, error)),
                };
            let result = crate::protocol::MaterializationStatusResult {
                status: status.map(|status| format!("{status:?}")),
                source_graph_version,
            };
            let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
            Ok(Response::ok(req_id, ResultPayload::Raw(bytes)))
        }
        // Bulk "what's stale" read on the same durable per-graph projection.
        #[cfg(feature = "epistemic-tms")]
        Method::StaleMaterializations => {
            let persist_dir = state.read().await.persist_dir.clone();
            let (ids, source_graph_version) =
                match crate::server::reasoning_projection::stale_materializations(
                    persist_dir.as_deref(),
                    graph_name,
                ) {
                    Ok(value) => value,
                    Err(error) => return Ok(Response::err(req_id, error)),
                };
            let result = crate::protocol::StaleMaterializationsResult {
                ids,
                source_graph_version,
            };
            let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
            Ok(Response::ok(req_id, ResultPayload::Raw(bytes)))
        }
        // EPI-P3-7 (gap-fill): standalone Dung argumentation conflict resolution. A
        // pure classification over a `BeliefGraph` snapshot — no writes. Gated
        // `epistemic-tms` at the handler (same fallback convention as
        // `EpistemicStatus`/`WhatChanged`). L-RLS-1 (RLS_ROUTED): `node_ids` are
        // caller-supplied, and the grounded/preferred/stable extension is computed
        // over the WHOLE argumentation graph before `node_ids` is partitioned against
        // it — an RLS-invisible node's own status would otherwise be classified
        // directly, and its attack/support edges would still shape OTHER (visible)
        // nodes' computed status. `rls.filter_view` the snapshot first, the SAME
        // idiom every other read arm in this file applies, so an invisible node (and
        // its edges) never enters `BeliefGraph::from_graph_view` at all.
        #[cfg(feature = "epistemic-tms")]
        Method::ResolveConflict {
            node_ids,
            semantics,
        } => {
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let mut snap = core.analysis_snapshot();
            #[cfg(feature = "security")]
            rls.filter_view(caller, &mut snap);
            let resp = match compute_off_lock(req_id, move || {
                resolve_conflict_wire(&node_ids, &semantics, &snap)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("ResolveConflict error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // X-1 (CONCEPT:EG-X1): the multimodal-evidence citation resolver. Gated
        // `evidence-graph` at the handler; the wire `Method` variant itself is gated
        // only `epistemic` (see its doc comment), so a build with `epistemic` but not
        // `evidence-graph` falls through to the not-built catch-all. SURPASS
        // gap-closure ("unify the two evidence resolvers"): with `alignment` ALSO
        // compiled in, resolve the configured blob store (if any) BEFORE entering
        // the off-lock closure (needs `state.read().await`, not available inside a
        // `spawn_blocking` closure) and thread it through so citations carry REAL
        // resolved content, not just locus metadata. L-RLS-1 (RLS_ROUTED): `node_id`
        // is caller-supplied and `evidence_citations` walks the graph's incoming
        // support/attack edges transitively — an unfiltered snapshot would resolve
        // (and return) an RLS-invisible evidence node's citation. `rls.filter_view`
        // the snapshot first, the SAME idiom every other read arm in this file
        // applies, so a hidden node (and the edges naming it) never enters
        // `BeliefGraph::from_graph_view` at all.
        #[cfg(all(feature = "evidence-graph", feature = "alignment"))]
        Method::ExplainEvidence { node_id } => {
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let mut snap = core.analysis_snapshot();
            #[cfg(feature = "security")]
            rls.filter_view(caller, &mut snap);
            let blob_store = state.read().await.blob.as_ref().map(|b| b.store.clone());
            let resp = match compute_off_lock(req_id, move || {
                explain_evidence_wire(&node_id, &snap, blob_store)
            })
            .await
            {
                Ok(result) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(all(feature = "evidence-graph", not(feature = "alignment")))]
        Method::ExplainEvidence { node_id } => {
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let mut snap = core.analysis_snapshot();
            #[cfg(feature = "security")]
            rls.filter_view(caller, &mut snap);
            let resp = match compute_off_lock(req_id, move || {
                explain_evidence_wire(&node_id, &snap)
            })
            .await
            {
                Ok(result) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // EPI-P3-3/P3-6: do-calculus intervention OR observational conditioning
        // (selected by `mode`) over a request-carried SCM. A pure function over
        // `variables`/`do_values`/`mode` — no graph snapshot needed. Gated
        // `epistemic-causal` at the handler (same fallback convention as above).
        #[cfg(feature = "epistemic-causal")]
        Method::CausalEstimate {
            variables,
            do_values,
            mode,
        } => {
            let resp = match compute_off_lock(req_id, move || {
                causal_estimate_wire(&variables, &do_values, mode)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("CausalEstimate error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // EPI-P3-6: Pearl's point-counterfactual recipe over a request-carried SCM +
        // a fully-observed `actual` unit. A pure function over request-carried
        // inputs — no graph snapshot needed. Gated `epistemic-causal` at the
        // handler (same fallback convention as `CausalEstimate`).
        #[cfg(feature = "epistemic-causal")]
        Method::CausalCounterfactual {
            variables,
            actual,
            do_values,
        } => {
            let resp = match compute_off_lock(req_id, move || {
                causal_counterfactual_wire(&variables, &actual, &do_values)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("CausalCounterfactual error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // EPI-P3-3: provenance-aware retrieval ranking. A pure function over
        // request-carried inputs — no graph snapshot needed. Gated `epistemic-causal`
        // at the handler (same fallback convention as above).
        #[cfg(feature = "epistemic-causal")]
        Method::RankByProvenance {
            candidates,
            weights,
        } => {
            let resp = match compute_off_lock(req_id, move || {
                rank_by_provenance_wire(&candidates, weights)
            })
            .await
            {
                Ok(result) => {
                    let bytes = rmp_serde::to_vec_named(&result).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Err(resp) => resp,
            };
            Ok(resp)
        }
        // ── In-transaction cross-modal read-your-own-writes (CONCEPT:EG-KG.query.txn-cross-modal-ryow) ──
        // Run the SAME unified cross-modal plan as `UnifiedQuery`, but over a
        // snapshot OVERLAID with the open txn's staged (uncommitted) write-set +
        // staged embeddings, so a node/edge/vector the txn itself staged is visible
        // to THIS query before commit and invisible off-txn until commit. Reuses the
        // EG-049 overlay generalized cross-modal (graph + semantic). No result cache
        // on this path (staged writes don't bump `version()`), exactly like the
        // committed SQL read path. RLS applies to the committed base snapshot.
        #[cfg(feature = "query")]
        Method::TxnUnifiedQuery { txn_id, plan } => Ok(run_unified_overlaid(
            state,
            req_id,
            &txn_id,
            plan,
            read_authority,
            caller,
            #[cfg(feature = "security")]
            rls,
        )
        .await),
        #[cfg(feature = "query")]
        Method::TxnUnifiedQueryText { txn_id, text } => {
            // UQL front-end: parse to the SAME `wire::Plan`, then run the IDENTICAL
            // overlaid in-txn executor. A parse error is a caret-annotated Response.
            let plan = match eg_plan::uql::parse(&text) {
                Ok(p) => p,
                Err(e) => return Ok(Response::err(req_id, e.render(&text))),
            };
            Ok(run_unified_overlaid(
                state,
                req_id,
                &txn_id,
                plan,
                read_authority,
                caller,
                #[cfg(feature = "security")]
                rls,
            )
            .await)
        }
        #[cfg(feature = "nl-query")]
        Method::NlQuery { text, graph } => {
            // CONCEPT:EG-KG.query.core-query-input/EG-080 — natural-language → executable query → rows. Resolve
            // the configured/injected `NlPlanner`, turn the NL into a UQL query STRING,
            // then run it through the IDENTICAL `UnifiedQueryText` pipeline
            // (`eg_plan::uql::parse` + `run_unified`). NO LLM in the engine core and NO
            // new execution path — the produced query rides the deterministic pipeline.
            // The graph was already used for routing; the handler runs against `core`.
            let _ = graph;
            let planner = match crate::server::nl::resolve_planner() {
                Some(p) => p,
                None => {
                    return Ok(Response::err(
                        req_id,
                        "NlQuery: no NL planner configured — set an OpenAI-compatible \
                         endpoint in agent-utilities config.json (or \
                         EPISTEMIC_GRAPH_NL_ENDPOINT), or inject one via \
                         server::set_nl_planner"
                            .to_string(),
                    ))
                }
            };
            let hint = nl_schema_hint(&core);
            let uql = match planner.plan(&text, &hint) {
                Ok(q) => q,
                Err(e) => return Ok(Response::err(req_id, format!("NlQuery planner error: {e}"))),
            };
            let plan = match eg_plan::uql::parse(&uql) {
                Ok(p) => p,
                Err(e) => {
                    return Ok(Response::err(
                        req_id,
                        format!("NlQuery produced invalid UQL: {}", e.render(&uql)),
                    ))
                }
            };
            #[cfg(feature = "tsdb")]
            let tsdb_scope = match served_tsdb_scope(&plan, graph_name, read_authority) {
                Ok(scope) => scope,
                Err(denied) => return Ok(Response::err(req_id, denied)),
            };
            // RLS-filtered off-lock snapshot, exactly like the Sql/UnifiedQueryText reads.
            // NOT result-cached: an LLM plan is non-deterministic, so keying a cache on the
            // NL text would risk serving a stale/foreign result.
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let mut snap = core.analysis_snapshot();
            #[cfg(feature = "security")]
            rls.filter_view(caller, &mut snap);
            // See the `UnifiedQuery` arm: push vector + lexical legs into the live
            // persistent indexes via a guard taken INSIDE the off-lock closure.
            let core_for_ctx = core.clone();
            // RECONCILE (CONCEPT:EG-KG.query.native-time-series): committed tsdb store for `Op::TsScan` fusion.
            #[cfg(feature = "tsdb")]
            let tsdb = if tsdb_scope.is_some() {
                state.read().await.tsdb_store.clone()
            } else {
                None
            };
            #[cfg(feature = "tsdb")]
            let (tsdb_tenant, tsdb_graph) = match tsdb_scope {
                Some((tenant, graph)) => (Some(tenant), Some(graph)),
                None => (None, None),
            };
            let resp = match compute_off_lock(req_id, move || {
                #[cfg(feature = "text")]
                let served_text =
                    crate::server::secondary_indexes::ServedTextIndex::new(core_for_ctx.clone());
                #[cfg(feature = "geo")]
                let served_spatial =
                    crate::server::secondary_indexes::ServedSpatialIndex::new(core_for_ctx.clone());
                let semantic_guard = core_for_ctx.semantic_store.read();
                run_unified(
                    plan,
                    &snap,
                    &semantic_guard,
                    ServedIndexes {
                        #[cfg(feature = "text")]
                        text: Some(&served_text),
                        #[cfg(feature = "geo")]
                        spatial: Some(&served_spatial),
                        #[cfg(not(any(feature = "text", feature = "geo")))]
                        _marker: std::marker::PhantomData,
                    },
                    #[cfg(feature = "tsdb")]
                    TsdbLegBind {
                        tsdb: tsdb.as_deref(),
                        tsdb_tenant: tsdb_tenant.as_deref(),
                        tsdb_graph: tsdb_graph.as_deref(),
                        // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                        staged_series: None,
                    },
                )
            })
            .await
            {
                Ok(Ok(rows)) => {
                    let bytes = rmp_serde::to_vec_named(&rows).unwrap_or_default();
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("NlQuery error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "graphql")]
        Method::GraphQl { query, variables } => {
            // GraphQL WRITE surface (CONCEPT:EG-KG.query.mutation/EG-023): a `mutation { … }` document
            // maps onto eg-core's native write ops over the LIVE `GraphCore` via
            // `execute_mutation` (which bumps the OCC version / `mark_dirty` once it
            // lands). NOT cached (it is a write) and NOT RLS pre-filtered (writes are
            // graph-ACL-gated in `dispatch_graph_op` — this method classified Write).
            if super::super::access::graphql_is_mutation(&query) {
                let carrier = match read_authority.and_then(GraphReadAuthority::carrier) {
                    Some(carrier) => carrier,
                    None => {
                        crate::metrics::access_denied();
                        return Ok(Response::err(
                            req_id,
                            "ACCESS_DENIED: GraphQL mutation requires verified tenant+actor authority",
                        ));
                    }
                };
                // Cross-modal transaction routing (CONCEPT:EG-KG.query.eg-9/419). A GraphQL mutation
                // is one of three shapes: a `commitTransaction` — landed DURABLY via
                // `commit_cross_modal_txn` (ONE redb WriteTransaction across graph + vector
                // + tsdb + axioms), exactly as pgwire's commit path; a begin/stage/read/
                // rollback cross-modal verb — run in-memory over the process-wide
                // `CrossModalTxnRegistry` (staging + read-your-own-writes, no durable side
                // effect until commit); or an ordinary mutation — the native `execute_mutation`
                // write path. `classify_crossmodal` picks the route with ONE parse.
                match eg_graphql::classify_crossmodal(&query) {
                    eg_graphql::CrossModalRoute::Commit(txn_id) => {
                        let committed = super::txn::commit_graphql_cross_modal(
                            state,
                            req_id,
                            graph_name,
                            &core,
                            graphql_crossmodal_registry(),
                            &txn_id,
                            carrier,
                        )
                        .await;
                        let resp = match committed {
                            Ok(committed) => Response::ok(
                                req_id,
                                ResultPayload::Raw(
                                    rmp_serde::to_vec_named(&serde_json::json!({
                                        "data": {"commitTransaction": {"committed": committed}}
                                    }))
                                    .unwrap_or_default(),
                                ),
                            ),
                            Err(msg) => Response::err(
                                req_id,
                                format!("GraphQL commitTransaction error: {msg}"),
                            ),
                        };
                        return Ok(resp);
                    }
                    eg_graphql::CrossModalRoute::Staging => {
                        let core_w = read_authority
                            .expect("GraphQL mutation authority checked above")
                            .project_core(&core);
                        let owner_scope = carrier.owner_scope().to_string();
                        let reg = graphql_crossmodal_registry();
                        let resp = match compute_off_lock(req_id, move || {
                            eg_graphql::execute_crossmodal(&core_w, reg, &owner_scope, &query)
                        })
                        .await
                        {
                            Ok(Ok(value)) => Response::ok(
                                req_id,
                                ResultPayload::Raw(
                                    rmp_serde::to_vec_named(&value).unwrap_or_default(),
                                ),
                            ),
                            Ok(Err(msg)) => {
                                Response::err(req_id, format!("GraphQL cross-modal error: {msg}"))
                            }
                            Err(resp) => resp,
                        };
                        return Ok(resp);
                    }
                    eg_graphql::CrossModalRoute::Invalid(message) => {
                        return Ok(Response::err(req_id, message));
                    }
                    eg_graphql::CrossModalRoute::NotCrossModal => {
                        let core_w = core.clone();
                        let resp = match compute_off_lock(req_id, move || {
                            eg_graphql::execute_mutation(&core_w, &query)
                        })
                        .await
                        {
                            Ok(Ok(value)) => Response::ok(
                                req_id,
                                ResultPayload::Raw(
                                    rmp_serde::to_vec_named(&value).unwrap_or_default(),
                                ),
                            ),
                            Ok(Err(msg)) => {
                                Response::err(req_id, format!("GraphQL mutation error: {msg}"))
                            }
                            Err(resp) => resp,
                        };
                        return Ok(resp);
                    }
                }
            }
            // A `subscription { … }` is a read-only POLL of the current matches (a full
            // push transport is a documented eg-graphql deferral); a `query { … }` is the
            // ordinary read. Both run over the SAME RLS-filtered off-lock snapshot below.
            let is_subscription = matches!(
                eg_graphql::parse_operation(&query),
                Ok(eg_graphql::Operation::Subscription(_))
            );
            // GraphQL READ surface (CONCEPT:EG-KG.query.sparql-completeness): compile the GraphQL query to
            // scans + BFS over the SAME off-lock snapshot the Cypher path uses, via the
            // pure-Rust eg-graphql resolver (NO async-graphql / DataFusion). The result
            // is the GraphQL `{"data": …}` JSON, returned via `ResultPayload::Raw`.
            //
            // GraphQL runs under the SAME version-keyed, RLS-aware result cache the
            // SQL/Cypher/SPARQL paths do (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): the cache KEY
            // folds in the caller's RLS context so agent A's filtered `{data}` is NEVER
            // served to agent B for the same GraphQL query text, and the snapshot is
            // RLS-FILTERED to the caller's visible rows BEFORE the resolver runs — a
            // GraphQL read cannot leak rows across agents any more than a Cypher read.
            // Bind the request's GraphQL `$variables` (task #23): a `query { … }` runs
            // through `execute_with_variables` so `$var` args + `@skip`/`@include`
            // resolve (CONCEPT:EG-KG.query.fragments-variables-directives); absent ⇒ an empty object, byte-identical to the
            // no-vars path. (A `subscription { … }` stays a poll of the current matches.)
            let vars = variables.unwrap_or_else(|| serde_json::json!({}));
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                // Fold the bound variables INTO the cache key: the same query text with
                // different `$variables` can produce different `{data}`, so the key must
                // distinguish them or a variables-bound read would serve a stale result.
                // An empty `{}` serializes to `{}` — byte-stable for the no-vars path.
                let mut key_payload = query.as_bytes().to_vec();
                key_payload.push(0);
                key_payload.extend_from_slice(&serde_json::to_vec(&vars).unwrap_or_default());
                let hash = rls_cache_hash(
                    "graphql",
                    &key_payload,
                    #[cfg(feature = "security")]
                    caller,
                    #[cfg(feature = "security")]
                    rls,
                );
                let (mut snap, version) = core.analysis_snapshot_versioned();
                if let Some(bytes) = core.result_cache().get(hash, version) {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                #[cfg(feature = "security")]
                rls.filter_view(caller, &mut snap);
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let resp = match compute_off_lock(req_id, move || {
                if is_subscription {
                    eg_graphql::subscribe(&snap, &query)
                } else {
                    eg_graphql::execute_with_variables(&snap, &query, &vars)
                }
            })
            .await
            {
                Ok(Ok(value)) => {
                    let bytes = rmp_serde::to_vec_named(&value).unwrap_or_default();
                    #[cfg(feature = "result-cache")]
                    core.result_cache().put(hash, version, bytes.clone());
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("GraphQL error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "cypher")]
        Method::CypherQuery { query, mode } => {
            // Cypher WRITE surface (CONCEPT:EG-KG.query.register-each-user-table/EG-023): a `CREATE`/`MERGE`/`SET`/
            // `DELETE`/`REMOVE` statement is applied to the LIVE `GraphCore` via
            // `exec_cypher_write` (native eg-core write ops — NO DataFusion; it calls
            // `mark_dirty` once after the mutation). NOT cached, NOT RLS pre-filtered
            // (writes are graph-ACL-gated upstream — this method classified Write). A
            // read falls through to the RLS-aware cached snapshot path below.
            let validation_method = Method::CypherQuery {
                query: query.clone(),
                mode,
            };
            if let Err(error) = validate_cypher_mode(&validation_method) {
                return Ok(Response::err(req_id, error));
            }
            if matches!(mode, crate::protocol::CypherMode::Write) {
                let core_w = core.clone();
                let resp = match compute_off_lock(req_id, move || {
                    eg_query::exec_cypher_write(&core_w, &query)
                })
                .await
                {
                    Ok(Ok(result)) => Response::ok(
                        req_id,
                        ResultPayload::Raw(rmp_serde::to_vec_named(&result).unwrap_or_default()),
                    ),
                    Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
                    Err(resp) => resp,
                };
                return Ok(resp);
            }
            // Same off-lock snapshot + blocking-pool idiom as SQL — but DEP-FREE
            // (label index / VF2 / BFS), so it runs in a no-DataFusion Pi build.
            // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231) wraps
            // it identically; this is the lean-Pi cached query path. The cache KEY folds
            // in the caller's RLS context so agent A's filtered rows are never served to
            // agent B, and the snapshot is RLS-filtered before execution.
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let hash = rls_cache_hash(
                    "cypher",
                    query.as_bytes(),
                    #[cfg(feature = "security")]
                    caller,
                    #[cfg(feature = "security")]
                    rls,
                );
                let (mut snap, version) = core.analysis_snapshot_versioned();
                if let Some(bytes) = core.result_cache().get(hash, version) {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                #[cfg(feature = "security")]
                rls.filter_view(caller, &mut snap);
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
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

/// The process-wide server-side text→vector embedder for the UQL `RANK BY ~ "text"`
/// (`Op::RankEmbed`) NL→vector seam (CONCEPT:EG-KG.query.bind-server-side-text / EG-411) — the FACADE injection point.
/// Returns the bound embedder, or `None` when none is configured (an `Op::RankEmbed` then
/// errors cleanly). The engine stores embeddings but produces them client-side today, so no
/// in-process model ships and the default is `None`; `EG_UQL_TEXT_EMBEDDER=hash` binds the
/// deterministic `HashEmbedder` fallback (offline/testing — arbitrary ranking). A real
/// embedding model (an ONNX/remote-service impl of `eg_plan::TextEmbedder`) is wired in HERE.
#[cfg(feature = "query")]
fn uql_text_embedder() -> Option<&'static dyn eg_plan::TextEmbedder> {
    use std::sync::OnceLock;
    static EMBEDDER: OnceLock<Option<eg_plan::HashEmbedder>> = OnceLock::new();
    EMBEDDER
        .get_or_init(
            || match std::env::var("EG_UQL_TEXT_EMBEDDER").ok().as_deref() {
                Some("hash") => Some(eg_plan::HashEmbedder::default()),
                _ => None,
            },
        )
        .as_ref()
        .map(|e| e as &dyn eg_plan::TextEmbedder)
}

/// Compute a SOUND dependency set for a UQL/`UnifiedQuery` plan
/// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7), or `None` when the plan's
/// shape cannot be reduced to one — the caller then uses the coarse version-keyed result-cache
/// path (unchanged). ONLY a plan built from pure node-relational ops is dependency-scoped:
///   * `Scan { label }` — the sole source: a labeled scan depends on that label, an unlabeled scan
///     on the whole node set (both tracked by the [`eg_core::dep_scope::DepClock`]);
///   * `Filter` / `Limit` — RowSet-narrowing transforms over the ALREADY-sourced rows, adding no
///     graph dependency beyond the source's (they read only the scanned rows' own properties,
///     which the source's label/all-nodes dimension already covers).
///
/// ANY other op — a `Traverse` (edges + arbitrary reached nodes), a vector/lexical `Rank`, a
/// temporal `AsOf`, a reasoner/SPARQL/federation/tensor/spatial/tsdb/epistemic leg — reads state
/// the clock does not model, so the WHOLE plan falls back to coarse invalidation. That
/// conservative boundary is what makes a stale hit impossible: a dependency set is only ever
/// returned when it PROVABLY captures everything the query reads.
#[cfg(feature = "result-cache")]
fn plan_dependency_set(plan: &eg_plan::Plan) -> Option<eg_core::dep_scope::DepSet> {
    use eg_core::dep_scope::{DepSet, Dim};
    let mut dims: Vec<Dim> = Vec::new();
    let mut has_source = false;
    for op in &plan.ops {
        match op {
            eg_plan::Op::Scan { label } => {
                has_source = true;
                if label.is_empty() {
                    dims.push(Dim::AllNodes);
                } else {
                    dims.push(Dim::Label(label.clone()));
                }
            }
            eg_plan::Op::Filter { .. } | eg_plan::Op::Limit { .. } => {}
            // Any op reading state outside the dependency clock's model ⇒ coarse fallback.
            _ => return None,
        }
    }
    // A plan with no graph SOURCE op (e.g. a pure federation/tsdb seed) is not a bounded node
    // read — fall back rather than claim an empty dependency set.
    if has_source {
        Some(DepSet::new(dims))
    } else {
        None
    }
}

/// Does `ops` reference a lexical text op — an `Op::RankText` at the top level or nested
/// inside an `Op::FuseRrf` branch (CONCEPT:EG-KG.query.served-text-index-binding)? Drives whether
/// `run_unified` builds+binds a served text index at all, so a non-text plan pays nothing.
#[cfg(all(feature = "query", feature = "text"))]
fn plan_needs_text(ops: &[eg_plan::Op]) -> bool {
    ops.iter().any(|op| match op {
        eg_plan::Op::RankText { .. } => true,
        eg_plan::Op::FuseRrf { branches, .. } => branches.iter().any(|b| plan_needs_text(b)),
        _ => false,
    })
}

/// Does `ops` reference `Op::SpatialScan` — at the top level or nested inside an
/// `Op::FuseRrf` branch (CONCEPT:EG-KG.storage.incremental-spatial, L37, mirroring `plan_needs_text`)?
/// Drives whether `run_unified` binds the served spatial index at all, so a non-spatial
/// plan pays nothing.
#[cfg(all(feature = "query", feature = "geo"))]
fn plan_needs_spatial(ops: &[eg_plan::Op]) -> bool {
    ops.iter().any(|op| match op {
        eg_plan::Op::SpatialScan { .. } => true,
        eg_plan::Op::FuseRrf { branches, .. } => branches.iter().any(|b| plan_needs_spatial(b)),
        _ => false,
    })
}

/// Build a BM25 [`eg_text::TextIndex`] from a graph snapshot's node blobs
/// (CONCEPT:EG-KG.query.served-text-index-binding) — the served lexical index for `Op::RankText` /
/// `Op::FuseRrf`. Each node's indexable text is the concatenation of every STRING leaf in its
/// JSON property blob (so `name` / `description` / `text` / `content` / `title` / … are all
/// searchable, matching the human-readable fields `Discover` hydrates), keyed by node id. An
/// in-memory index (no persist dir needed — it is rebuilt per served text query off the exact
/// queried snapshot, so it is always current with the read). Returns `None` on a build/commit
/// error (the plan then degrades to no lexical hits — never errs), mirroring an absent index.
#[cfg(all(feature = "query", feature = "text"))]
fn build_text_index_from_view(view: &crate::graph::GraphView) -> Option<eg_text::TextIndex> {
    /// Append every string leaf in `v` (recursing objects/arrays) to `out`, space-separated.
    fn collect_strings(v: &serde_json::Value, out: &mut String) {
        match v {
            serde_json::Value::String(s) => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(s);
            }
            serde_json::Value::Array(a) => a.iter().for_each(|e| collect_strings(e, out)),
            serde_json::Value::Object(o) => o.values().for_each(|e| collect_strings(e, out)),
            _ => {}
        }
    }
    let mut index = eg_text::TextIndex::in_memory().ok()?;
    for (id, blob) in &view.node_properties {
        let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
            continue;
        };
        let mut text = String::new();
        collect_strings(&v, &mut text);
        if !text.is_empty() {
            index.upsert(id, &text);
        }
    }
    index.commit().ok()?;
    Some(index)
}

/// Bundles the served-adapter modality indexes `run_unified` pushes a plan's legs down
/// into (CONCEPT:EG-KG.query.served-text-index-binding / CONCEPT:EG-KG.storage.incremental-spatial) — ONE parameter
/// rather than one per modality, so `run_unified`'s argument count stays sane as more
/// served-index modalities are added over time (L37 added spatial beside text; a future
/// modality adds a field here, not a new top-level parameter). Each field is `Some` and
/// `.available()` ⇒ the matching op searches the MAINTAINED persistent index directly, NO
/// per-query rebuild; otherwise `run_unified` falls back to the prior behavior (a
/// snapshot-derived index for text, an unbound `PlanCtx::spatial` — ephemeral R-tree — for
/// spatial), exactly as if this bundle were never passed.
#[cfg(feature = "query")]
#[derive(Default)]
pub(crate) struct ServedIndexes<'a> {
    #[cfg(feature = "text")]
    pub text: Option<&'a crate::server::secondary_indexes::ServedTextIndex>,
    #[cfg(feature = "geo")]
    pub spatial: Option<&'a crate::server::secondary_indexes::ServedSpatialIndex>,
    // Keeps `'a` used even when neither `text` nor `geo` is built, so `ServedIndexes<'_>`
    // stays a valid (zero-field-active) type in every feature combination.
    #[cfg(not(any(feature = "text", feature = "geo")))]
    _marker: std::marker::PhantomData<&'a ()>,
}

/// Bundles `run_unified`'s tsdb `Op::TsScan` leg-binding parameters — ONE
/// parameter rather than four, so the function's argument count stays under the
/// clippy ceiling (mirrors the `ServedIndexes` rationale above). Only exists
/// under the `tsdb` feature; a non-`tsdb` build omits the parameter entirely.
#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) struct TsdbLegBind<'a> {
    /// The committed native tsdb `SeriesStore` backing `Op::TsScan`, threaded in
    /// so a UQL plan fuses its time-series leg with the graph/vector/relational
    /// legs. `None` ⇒ a `TsScan` yields no rows (degrade, never err).
    pub tsdb: Option<&'a eg_tsdb::store::SeriesStore>,
    /// Verified tenant + actor-owned graph policy context for committed TsScan
    /// reads. Served callers bind both or omit the store entirely.
    pub tsdb_tenant: Option<&'a str>,
    pub tsdb_graph: Option<&'a str>,
    /// In-txn tsdb read-your-own-writes (CONCEPT:EG-KG.query.txn-tsdb-read-your): the resolved txn's OWN staged,
    /// uncommitted series points, overlaid onto `Op::TsScan` BEFORE the committed store so
    /// an in-txn UQL reads its own measurements. `None` off-txn ⇒ committed series only.
    pub staged_series: Option<&'a eg_plan::StagedSeries>,
}

/// Execute a unified cross-modal plan (CONCEPT:AU-KG.compute.vector/209) over one off-lock
/// snapshot and return the result rows as `[id, score|nil]`. The plan is routed through the
/// full cost optimizer by `eg_plan::execute` (CONCEPT:EG-KG.query.served-plan-optimize-routing); a
/// lexical `Op::RankText`/`Op::FuseRrf` leg is served over the MAINTAINED persistent BM25
/// index when one is registered, falling back to a snapshot-derived index otherwise
/// (CONCEPT:EG-KG.query.served-text-index-binding). Synchronous — runs on the blocking pool via
/// `compute_off_lock`, like the SQL/Cypher legs.
#[cfg(feature = "query")]
pub(crate) fn run_unified(
    plan: eg_plan::Plan,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
    served: ServedIndexes<'_>,
    #[cfg(feature = "tsdb")] tsdb_ctx: TsdbLegBind<'_>,
) -> Result<Vec<(String, Option<f32>)>, String> {
    #[cfg(feature = "tsdb")]
    let TsdbLegBind {
        tsdb,
        tsdb_tenant,
        tsdb_graph,
        staged_series,
    } = tsdb_ctx;
    #[cfg(feature = "text")]
    let served_text = served.text;
    #[cfg(feature = "geo")]
    let served_spatial = served.spatial;
    use eg_plan::PlanCtx;

    // CONCEPT:EG-KG.query.served-plan-optimize-routing — the served path hands the plan
    // directly to `eg_plan::execute`, which applies the complete cost optimizer using
    // snapshot-derived cardinality and cost statistics. Optimizer rules are
    // answer-preserving within the EG-405 non-empty guard.
    let ops = plan.ops;

    // CONCEPT:EG-KG.query.served-text-index-binding — bind a live BM25 lexical search surface into the
    // served `PlanCtx` so a served `UnifiedQuery`/`UnifiedQueryText` whose plan carries
    // `Op::RankText` or an `Op::FuseRrf` text branch gets REAL lexical scores (it
    // previously always rebuilt a throwaway index from the queried snapshot on EVERY
    // request — the EG-P1-4 gap). Preference order:
    //   1. the MAINTAINED persistent per-graph `GraphTextIndex`, via `served_text`, when
    //      one is registered — no per-query rebuild, and it reflects every committed
    //      write incrementally (CONCEPT:EG-KG.storage.incremental-text);
    //   2. a snapshot-derived `eg_text::TextIndex` built from `view` (the PRIOR
    //      behavior), for a graph with no `ServerIndexFactory` installed (a bare test
    //      harness, or a graph that predates the factory).
    // Built/bound ONLY when the plan actually references a text op, so a non-text
    // served query pays nothing either way.
    let ctx = PlanCtx::new(view, semantic);
    #[cfg(feature = "text")]
    let need_text = plan_needs_text(&ops);
    #[cfg(feature = "text")]
    let persistent_text = served_text.filter(|st| st.available());
    #[cfg(feature = "text")]
    let snapshot_text_index: Option<eg_text::TextIndex> = if need_text && persistent_text.is_none()
    {
        build_text_index_from_view(view)
    } else {
        None
    };
    #[cfg(feature = "text")]
    let ctx = if !need_text {
        ctx
    } else if let Some(served) = persistent_text {
        ctx.with_text(served)
    } else if let Some(index) = snapshot_text_index.as_ref() {
        ctx.with_text(index)
    } else {
        ctx
    };
    // CONCEPT:EG-KG.storage.incremental-spatial, L37 — bind a persistent spatial index into the served `PlanCtx` so a
    // served `Op::SpatialScan` gets pushed into the MAINTAINED per-graph
    // `GraphSpatialIndex` instead of rebuilding a throwaway packed Hilbert R-tree on
    // EVERY request. `None` (no factory installed, or the plan has no spatial op) keeps
    // `spatial_scan`'s prior ephemeral-build fallback — byte-for-byte the old behavior.
    #[cfg(feature = "geo")]
    let ctx = if plan_needs_spatial(&ops) {
        match served_spatial.filter(|s| s.available()) {
            Some(served) => ctx.with_spatial(served),
            None => ctx,
        }
    } else {
        ctx
    };
    // CONCEPT:EG-KG.query.bind-server-side-text — bind the server-side text→vector embedder so a UQL `RANK BY ~ "text"`
    // (`Op::RankEmbed`) resolves its query vector at exec time (the NL→vector seam,
    // EG-411). This is the facade INJECTION POINT: the engine stores embeddings but
    // produces them CLIENT-side today (no in-process model), so a real embedding model — an
    // ONNX/remote embedding-service impl of `eg_plan::TextEmbedder` producing vectors in the
    // graph's embedding space — is bound HERE. Absent a bound model an `Op::RankEmbed` is a
    // clean typed error (never a panic), exactly the documented unbound behavior. The
    // deterministic `HashEmbedder` fallback is opt-in via `EG_UQL_TEXT_EMBEDDER=hash` so the
    // seam is exercisable end-to-end offline (its ranking is deterministic but semantically
    // arbitrary — never the production default).
    let ctx = match uql_text_embedder() {
        Some(embedder) => ctx.with_embedder(embedder),
        None => ctx,
    };
    // RECONCILE (CONCEPT:EG-KG.query.native-time-series): attach the committed
    // store and its ownership scope atomically. A partial/missing scope never
    // leaves a raw store reachable through `TsScan`.
    #[cfg(feature = "tsdb")]
    let ctx = match (tsdb, tsdb_tenant, tsdb_graph) {
        (Some(store), Some(tenant), Some(graph)) => {
            ctx.with_tsdb(store).with_tsdb_scope(tenant, graph)
        }
        _ => ctx,
    };
    // CONCEPT:EG-KG.query.txn-tsdb-read-your: attach the txn's staged-series overlay so an in-txn `Op::TsScan`
    // reads its own uncommitted points (RYOW). Absent overlay ⇒ committed series only.
    #[cfg(feature = "tsdb")]
    let ctx = match staged_series {
        Some(staged) => ctx.with_staged_series(staged),
        None => ctx,
    };
    // CONCEPT:EG-KG.storage.derived-tensor-writeback-sink — bind the tensor CAS
    // write-back sink so a served `Op::TensorOp` actually runs instead of its
    // documented-but-unreachable "TensorOp requires a bound tensor store" error.
    // `Op::TensorScan`/`Op::TensorOp` read their INPUT tensor directly off the
    // queried `GraphView`'s node properties (`eg_plan::exec::row_tensor`) — this
    // store is purely the write-back destination for a TensorOp's DERIVED output,
    // so a process-wide singleton (not threaded through `ServerState`/callers) is
    // sufficient and still gives real content-address dedup across requests,
    // unlike a fresh store per call. In-memory only for now — `TensorStore::
    // persist`/`load` (disk durability across restarts) is a follow-up, tracked
    // the same way `tsdb_store` earned its own dedicated `ServerState` wiring.
    #[cfg(feature = "tensor")]
    let ctx = {
        static TENSOR_STORE: std::sync::OnceLock<std::sync::Mutex<eg_tensor::TensorStore>> =
            std::sync::OnceLock::new();
        let store =
            TENSOR_STORE.get_or_init(|| std::sync::Mutex::new(eg_tensor::TensorStore::new()));
        ctx.with_tensor_store(store)
    };
    let result = eg_plan::execute(&eg_plan::Plan::new(ops), &ctx)?;
    Ok(result
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect())
}

/// CONCEPT:EG-KG.storage.derived-tensor-writeback-sink — served-path proof that
/// `run_unified` (not just `eg-plan`'s own internal executor, already proven by
/// `crates/eg-plan/src/tensor_tests.rs`) now binds a tensor store: an
/// `Op::TensorScan` + `Op::TensorOp` plan run through the SAME entry point every
/// `UnifiedQuery`/`UnifiedQueryText` request uses now executes and returns rows
/// instead of the "TensorOp requires a bound tensor store" error `run_unified`
/// deterministically returned before the `.with_tensor_store(...)` binding was
/// added.
#[cfg(all(test, feature = "tensor"))]
mod tensor_served_round_trip_tests {
    use super::*;
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_tensor::{Buffer, Tensor};
    use eg_types::wire::{TensorOpKind, TensorReduceKind};

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// A `Frame` layer of three nodes each holding the same dense 2×3 tensor in
    /// their conventional `tensor` property, mirroring
    /// `eg_plan::tensor_tests::frames()`.
    fn frames_view() -> crate::graph::GraphView {
        let core = GraphCore::new();
        let t = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap();
        let tv = serde_json::to_value(&t).unwrap();
        for id in ["F1", "F2", "F3"] {
            core.add_node(
                id.into(),
                blob(serde_json::json!({ "type": "Frame", "tensor": tv })),
            );
        }
        core.analysis_snapshot()
    }

    fn served_indexes() -> ServedIndexes<'static> {
        ServedIndexes {
            #[cfg(feature = "text")]
            text: None,
            #[cfg(feature = "geo")]
            spatial: None,
            #[cfg(not(any(feature = "text", feature = "geo")))]
            _marker: std::marker::PhantomData,
        }
    }

    fn call_run_unified(plan: eg_plan::Plan) -> Result<Vec<(String, Option<f32>)>, String> {
        let view = frames_view();
        let semantic = SemanticStore::new();
        run_unified(
            plan,
            &view,
            &semantic,
            served_indexes(),
            #[cfg(feature = "tsdb")]
            TsdbLegBind {
                tsdb: None,
                tsdb_tenant: None,
                tsdb_graph: None,
                staged_series: None,
            },
        )
    }

    #[test]
    fn served_tensor_scan_and_op_executes_instead_of_erroring() {
        let plan = eg_plan::Plan::new(vec![
            eg_plan::Op::TensorScan {
                layer: "Frame".into(),
            },
            eg_plan::Op::TensorOp {
                kind: TensorOpKind::Reduce {
                    axis: 1,
                    kind: TensorReduceKind::Mean,
                },
            },
        ]);
        let rows = call_run_unified(plan).expect(
            "served TensorOp must execute now that run_unified binds a tensor store, \
             not error with 'TensorOp requires a bound tensor store'",
        );
        let mut ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["F1", "F2", "F3"]);
    }

    /// Before the fix, `run_unified` had no `tensor_store` binding at all, so this
    /// exact plan deterministically failed with "TensorOp requires a bound tensor
    /// store" regardless of input — the gap this test closes.
    #[test]
    fn served_tensor_op_without_the_fix_would_have_errored() {
        let plan = eg_plan::Plan::new(vec![
            eg_plan::Op::TensorScan {
                layer: "Frame".into(),
            },
            eg_plan::Op::TensorOp {
                kind: TensorOpKind::Elementwise {
                    op: eg_types::wire::TensorElementwiseOp::Mul,
                    scalar: 2.0,
                },
            },
        ]);
        assert!(
            call_run_unified(plan).is_ok(),
            "TensorOp over the served path must not deterministically error"
        );
    }
}

// ── EXPLAIN surfaces (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────────────────

/// The RLS-filtered snapshot [`rls_snapshot`] builds, without that fn's `result-cache`
/// gating (the EXPLAIN surfaces are diagnostics-only and never participate in the
/// version-keyed result cache, so they need this unconditionally rather than
/// duplicating the inline result-cache-aware snapshot each `UnifiedQuery`-family arm
/// builds when `result-cache` is on).
#[cfg(feature = "query")]
fn explain_snapshot(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> crate::graph::GraphView {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    snap
}

/// `EXPLAIN PLAN` — serialize `plan` as a `PlanDag` before/after the DAG-aware cost
/// optimizer (`eg_plan::optimize_dag`), plus the active rule set
/// (`eg_plan::cost_opt_rule_names()`). Pure diagnostics — no execution.
#[cfg(feature = "query")]
fn explain_plan(
    plan: eg_plan::Plan,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::protocol::ExplainPlanResult, String> {
    use crate::protocol::{ExplainNodeWire, ExplainPlanResult};
    use eg_plan::PlanCtx;

    fn to_wire(dag: &eg_plan::PlanDag) -> Vec<ExplainNodeWire> {
        dag.nodes
            .iter()
            .enumerate()
            .map(|(id, node)| ExplainNodeWire {
                id,
                op: format!("{:?}", node.op),
                inputs: node.inputs.clone(),
            })
            .collect()
    }

    let ctx = PlanCtx::new(view, semantic);
    let before = eg_plan::PlanDag::from(plan);
    let after = eg_plan::optimize_dag(&before, &ctx);
    Ok(ExplainPlanResult {
        before: to_wire(&before),
        after: to_wire(&after),
        applied_rules: eg_plan::cost_opt_rule_names()
            .into_iter()
            .map(str::to_string)
            .collect(),
    })
}

/// Project the governed locus onto its DAG-safe wire mirror.
#[cfg(feature = "epistemic")]
fn evidence_locus_wire(locus: &eg_modality::EvidenceLocus) -> crate::protocol::EvidenceLocusWire {
    use crate::protocol::{EvidenceAddressWire, EvidenceLocusWire, EvidenceResourceWire};
    use eg_modality::{EvidenceAddress, ResourceId};

    let subject = match &locus.subject {
        ResourceId::Artifact(id) => EvidenceResourceWire::Artifact(id.as_ref().to_string()),
        ResourceId::Occurrence(id) => EvidenceResourceWire::Occurrence(id.as_ref().to_string()),
        ResourceId::Rendition(id) => EvidenceResourceWire::Rendition(id.as_ref().to_string()),
        ResourceId::Segment(id) => EvidenceResourceWire::Segment(id.as_ref().to_string()),
        ResourceId::Feature(id) => EvidenceResourceWire::Feature(id.as_ref().to_string()),
        ResourceId::EvidenceLocus(id) => {
            EvidenceResourceWire::EvidenceLocus(id.as_ref().to_string())
        }
    };
    let address = match &locus.address {
        EvidenceAddress::CharacterRange { start, end } => EvidenceAddressWire::CharacterRange {
            start: *start,
            end: *end,
        },
        EvidenceAddress::TableCellRange {
            row_start,
            row_end,
            col_start,
            col_end,
        } => EvidenceAddressWire::TableCellRange {
            row_start: *row_start,
            row_end: *row_end,
            col_start: *col_start,
            col_end: *col_end,
        },
        EvidenceAddress::ImageRegion {
            x,
            y,
            width,
            height,
        } => EvidenceAddressWire::ImageRegion {
            x: *x,
            y: *y,
            width: *width,
            height: *height,
        },
        EvidenceAddress::PageRegion {
            page,
            x,
            y,
            width,
            height,
        } => EvidenceAddressWire::PageRegion {
            page: *page,
            x: *x,
            y: *y,
            width: *width,
            height: *height,
        },
        EvidenceAddress::AudioRange { start_ms, end_ms } => EvidenceAddressWire::AudioRange {
            start_ms: *start_ms,
            end_ms: *end_ms,
        },
        EvidenceAddress::VideoTimeRange { start_ms, end_ms } => {
            EvidenceAddressWire::VideoTimeRange {
                start_ms: *start_ms,
                end_ms: *end_ms,
            }
        }
        EvidenceAddress::FrameRange {
            start_frame,
            end_frame,
        } => EvidenceAddressWire::FrameRange {
            start_frame: *start_frame,
            end_frame: *end_frame,
        },
        EvidenceAddress::MetricWindow { start_ms, end_ms } => EvidenceAddressWire::MetricWindow {
            start_ms: *start_ms,
            end_ms: *end_ms,
        },
        EvidenceAddress::Point { x, y } => EvidenceAddressWire::Point { x: *x, y: *y },
        EvidenceAddress::RowVersion { row_ref, version } => EvidenceAddressWire::RowVersion {
            row_ref: row_ref.to_string(),
            version: *version,
        },
        EvidenceAddress::CodeSymbol {
            revision_ref,
            symbol_ref,
            start_line,
            end_line,
        } => EvidenceAddressWire::CodeSymbol {
            revision_ref: revision_ref.to_string(),
            symbol_ref: symbol_ref.to_string(),
            start_line: *start_line,
            end_line: *end_line,
        },
        EvidenceAddress::TraceSpan {
            trace_ref,
            span_ref,
        } => EvidenceAddressWire::TraceSpan {
            trace_ref: trace_ref.to_string(),
            span_ref: span_ref.to_string(),
        },
    };
    EvidenceLocusWire {
        id: locus.id.as_ref().to_string(),
        subject,
        address,
        policy_ref: locus.policy_ref.to_string(),
        derivation_ref: locus.derivation_ref.as_ref().to_string(),
    }
}

/// `EXPLAIN PROVENANCE` — run `plan` and, for each result row, resolve its EVIDENCE-FOR
/// provenance over the `KnowledgeSet` (E3) row shape (CONCEPT:EG-KG.query.knowledge-set),
/// reusing the SAME belief-substrate resolution `Op::EvidenceFor` runs, PLUS (X1,
/// CONCEPT:E4) the row's own located `evidence_refs` `KnowledgeSet::from_rowset`
/// already resolved. With `epistemic` off, every row's `source_refs`/`evidence_loci`
/// are empty and `resolved` is `false` — the documented "no epistemic resolution ran"
/// `KnowledgeSet` v1 default.
#[cfg(feature = "query")]
fn explain_provenance(
    request_id: u64,
    plan: eg_plan::Plan,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::epistemic_operations::EvidenceBundle, String> {
    use eg_plan::PlanCtx;

    let ctx = PlanCtx::new(view, semantic);
    let rs = eg_plan::execute(&plan, &ctx)?;
    let ks = eg_plan::KnowledgeSet::from_rowset(&rs, view, &[]);
    Ok(explain_provenance_result(request_id, &ks, &ctx))
}

/// `EXPLAIN PROVENANCE BY IDS` (CONCEPT:EG-KB-CURRENCY) — the ID-seeded sibling of
/// [`explain_provenance`]: builds the `KnowledgeSet` straight from an explicit id list
/// (`RowSet::from_ids`, deduplicated/first-occurrence, unranked — no `Op` plan/executor
/// involved) instead of running a `Plan`, then resolves the IDENTICAL per-row epistemic
/// columns via [`explain_provenance_result`]. The seam a caller with ids from ANY other
/// read path (Cypher, SQL, a prior `UnifiedQuery`) uses to fetch calibrated/cited/
/// time-versioned rows for exactly those ids.
#[cfg(feature = "query")]
fn explain_provenance_by_ids(
    request_id: u64,
    ids: Vec<String>,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::epistemic_operations::EvidenceBundle, String> {
    use eg_plan::PlanCtx;

    let ctx = PlanCtx::new(view, semantic);
    let rs = eg_plan::RowSet::from_ids(ids);
    let ks = eg_plan::KnowledgeSet::from_rowset(&rs, view, &[]);
    Ok(explain_provenance_result(request_id, &ks, &ctx))
}

/// Shared row-resolution core of [`explain_provenance`]/[`explain_provenance_by_ids`]
/// (CONCEPT:EG-KB-CURRENCY): map an already-built `KnowledgeSet`'s rows onto the wire
/// shape, widened beyond id/kind/source_refs/evidence_loci to also carry `score`/
/// `confidence`/`valid_time`/`tx_time`/`policy_labels` — straight field copies off each
/// `KnowledgeRow` (populated by `KnowledgeSet::from_rowset` regardless of `epistemic`
/// for score/confidence/valid_time/tx_time; `epistemic`-gated for
/// source_refs/policy_labels/evidence_loci exactly as before this widening).
#[cfg(feature = "query")]
fn explain_provenance_result(
    request_id: u64,
    ks: &eg_plan::KnowledgeSet,
    ctx: &eg_plan::PlanCtx<'_>,
) -> crate::epistemic_operations::EvidenceBundle {
    use crate::epistemic_operations::{
        EvidenceBundle, EvidenceBundleSchemaVersion, EvidenceClaim, EvidenceTimeRange,
    };

    #[cfg(feature = "epistemic")]
    let claims: Vec<EvidenceClaim> = ks
        .rows
        .iter()
        .map(|row| {
            // Reuse the SAME `Op::EvidenceFor` resolution the plan-Op surface runs, as
            // its own tiny one-op plan — no private eg-plan access needed.
            let evidence_plan = eg_plan::Plan::new(vec![eg_plan::Op::EvidenceFor {
                claim_id: row.id.clone(),
            }]);
            let source_refs = eg_plan::execute(&evidence_plan, ctx)
                .map(|r| r.ids())
                .unwrap_or_default();
            // X1: the row's own located evidence, already resolved by
            // `KnowledgeSet::from_rowset` — just map it onto the wire shape.
            let evidence_locus_refs = row
                .evidence_refs
                .iter()
                .map(|locus| locus.id.as_ref().to_string())
                .collect();
            EvidenceClaim {
                claim_ref: row.id.clone(),
                kind: row.kind.clone(),
                score: row.score.map(f64::from),
                confidence: row.confidence,
                valid_time: EvidenceTimeRange {
                    start_ms: row.valid_time.0,
                    end_ms: row.valid_time.1,
                },
                transaction_time: EvidenceTimeRange {
                    start_ms: row.tx_time.0,
                    end_ms: row.tx_time.1,
                },
                source_refs,
                evidence_locus_refs,
                contradiction_refs: row.contradiction_ids.clone(),
                proof_refs: row.proof_ids.clone(),
                policy_labels: row.policy_labels.clone(),
            }
        })
        .collect();
    #[cfg(not(feature = "epistemic"))]
    let claims: Vec<EvidenceClaim> = ks
        .rows
        .iter()
        .map(|row| EvidenceClaim {
            claim_ref: row.id.clone(),
            kind: row.kind.clone(),
            score: row.score.map(f64::from),
            confidence: row.confidence,
            valid_time: EvidenceTimeRange {
                start_ms: row.valid_time.0,
                end_ms: row.valid_time.1,
            },
            transaction_time: EvidenceTimeRange {
                start_ms: row.tx_time.0,
                end_ms: row.tx_time.1,
            },
            source_refs: Vec::new(),
            evidence_locus_refs: Vec::new(),
            contradiction_refs: Vec::new(),
            proof_refs: Vec::new(),
            policy_labels: Vec::new(),
        })
        .collect();

    EvidenceBundle {
        schema_version: EvidenceBundleSchemaVersion::V1,
        bundle_id: format!("request:{request_id}"),
        resolved: cfg!(feature = "epistemic"),
        answer_ref: None,
        claims,
        policy_exclusions: Vec::new(),
        next_action_refs: Vec::new(),
    }
}

/// `EXPLAIN POLICY` — run `plan` against BOTH the unfiltered snapshot and the caller's
/// RLS-filtered one (reusing the SAME `IsolationLayer::filter_view` every read path
/// already applies before this fn is ever reached), reporting which result ids the
/// policy denied. With no filtering applied (no `security` feature, or no caller/RLS on
/// this connection) `full_view` and `filtered_view` are the identical snapshot, so
/// `policy_denied_ids` is always empty.
#[cfg(feature = "query")]
fn explain_policy(
    plan: eg_plan::Plan,
    full_view: &crate::graph::GraphView,
    filtered_view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
) -> Result<crate::protocol::ExplainPolicyResult, String> {
    use crate::protocol::ExplainPolicyResult;
    use eg_plan::PlanCtx;

    let full_ctx = PlanCtx::new(full_view, semantic);
    let filtered_ctx = PlanCtx::new(filtered_view, semantic);
    let full_ids: std::collections::HashSet<String> = eg_plan::execute(&plan, &full_ctx)?
        .ids()
        .into_iter()
        .collect();
    let visible_ids: Vec<String> = eg_plan::execute(&plan, &filtered_ctx)?.ids();
    let visible_set: std::collections::HashSet<&str> =
        visible_ids.iter().map(String::as_str).collect();
    let mut policy_denied_ids: Vec<String> = full_ids
        .iter()
        .filter(|id| !visible_set.contains(id.as_str()))
        .cloned()
        .collect();
    policy_denied_ids.sort();
    Ok(ExplainPolicyResult {
        visible_ids,
        policy_denied_ids,
    })
}

/// Count `DerivedSupport`/`DerivedContradiction` nodes in an already-computed proof
/// tree (CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span
/// attributes, WS-1b): `(supporting, contradicting)`. A cheap walk over data
/// `explain_belief_tree`/`epistemic_status` already built above — not a new epistemic
/// computation — feeding `eg_epistemic::classify_policy_labels` for the
/// `epistemic.policy_labels` span attribute.
#[cfg(feature = "epistemic")]
fn count_tree_rules(node: &eg_epistemic::ProofNode) -> (usize, usize) {
    let (mut supporting, mut contradicting) = match node.rule {
        eg_epistemic::JustRule::DerivedSupport => (1, 0),
        eg_epistemic::JustRule::DerivedContradiction => (0, 1),
        eg_epistemic::JustRule::Asserted | eg_epistemic::JustRule::BayesianUpdate => (0, 0),
    };
    for p in &node.premises {
        let (s, c) = count_tree_rules(p);
        supporting += s;
        contradicting += c;
    }
    (supporting, contradicting)
}

/// Wire-project one [`eg_epistemic::ProofNode`] (recursively) — shared by the classic
/// `explain_belief` below AND the L53 `epistemic_status_wire` capstone, so both surfaces
/// render a proof tree identically.
#[cfg(feature = "epistemic")]
fn proof_node_wire(node: &eg_epistemic::ProofNode) -> crate::protocol::JustificationNodeWire {
    crate::protocol::JustificationNodeWire {
        claim: node.claim.clone(),
        rule: format!("{:?}", node.rule),
        confidence: node.confidence,
        premises: node.premises.iter().map(proof_node_wire).collect(),
    }
}

/// `EXPLAIN BELIEF <node_id>` — the FULL, un-flattened E1 justification tree
/// (`eg_epistemic::JustificationGraph`, via `eg_plan::explain_belief_tree`), wire-projected
/// recursively (mirroring `Method::OwlExplain`'s `ProofNodeWire`).
#[cfg(feature = "epistemic")]
fn explain_belief(
    node_id: &str,
    view: &crate::graph::GraphView,
) -> crate::protocol::ExplainBeliefResult {
    // A minimal semantic store: `explain_belief_tree` only reads `ctx.view` +
    // `ctx.belief_policy` (default, unbound on this path — no facade caller binds a
    // tenant-specific policy today, matching every other served epistemic op).
    let semantic = eg_core::compute::semantic::SemanticStore::new();
    let ctx = eg_plan::PlanCtx::new(view, &semantic);
    let tree = eg_plan::explain_belief_tree(&ctx, node_id);

    // CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b).
    // Mirrors the `write_coalescer.apply_batch`/`ann_index_build` span idiom (a plain
    // `tracing::debug_span!(...).entered()` guard over a sync block): additive-only,
    // exported by the SAME `tracing-opentelemetry` layer `otel.rs` installs when built
    // `--features otel` with `EPISTEMIC_GRAPH_OTLP_ENDPOINT` set, a complete no-op
    // otherwise. Every value is read off `tree`, already computed above — no new
    // epistemic computation.
    let (supporting, contradicting) = count_tree_rules(&tree.root);
    let policy_labels =
        eg_epistemic::classify_policy_labels(supporting, contradicting, 0).join(",");
    let _span = tracing::debug_span!(
        "epistemic.explain_belief",
        epistemic.confidence = tree.root.confidence,
        epistemic.status = tracing::field::debug(tree.root.rule),
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    crate::protocol::ExplainBeliefResult {
        root: proof_node_wire(&tree.root),
    }
}

/// Redacted-tree sibling of [`count_tree_rules`] — same walk, over
/// `eg_epistemic::RedactedProofNode` instead of `ProofNode` (a redaction preserves
/// `rule`/`confidence`/`premises` per that type's own doc comment, so the walk is
/// identical; the two types just aren't unified by a shared trait).
#[cfg(feature = "epistemic-redaction")]
fn count_redacted_tree_rules(node: &eg_epistemic::RedactedProofNode) -> (usize, usize) {
    let (mut supporting, mut contradicting) = match node.rule {
        eg_epistemic::JustRule::DerivedSupport => (1, 0),
        eg_epistemic::JustRule::DerivedContradiction => (0, 1),
        eg_epistemic::JustRule::Asserted | eg_epistemic::JustRule::BayesianUpdate => (0, 0),
    };
    for p in &node.premises {
        let (s, c) = count_redacted_tree_rules(p);
        supporting += s;
        contradicting += c;
    }
    (supporting, contradicting)
}

/// L51 — the redaction-aware sibling of [`explain_belief`]: builds a [`BeliefGraph`]
/// straight off `view` (populating `node_visibility` from the SAME per-node RLS blob
/// `filter_view` reads elsewhere, since `epistemic-redaction` turns that decode on in
/// `BeliefGraph::from_graph_view`) and routes through
/// `eg_epistemic::explain_belief_redacted_capped` under `actor_id`. `cap` is the
/// wire-requested `DisclosureLevelWire`, converted 1:1 to `eg_epistemic::DisclosureLevel`
/// (never a grant — see that fn's doc comment).
#[cfg(feature = "epistemic-redaction")]
fn explain_belief_redacted_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
    cap: crate::protocol::DisclosureLevelWire,
    isolation: &crate::isolation::IsolationLayer,
    actor_id: &str,
) -> crate::protocol::ExplainBeliefRedactedResult {
    use crate::protocol::{
        DisclosureLevelWire, ExistenceSignalWire, ExplainBeliefRedactedResult,
        RedactedJustificationNodeWire,
    };

    fn redacted_node_wire(node: &eg_epistemic::RedactedProofNode) -> RedactedJustificationNodeWire {
        RedactedJustificationNodeWire {
            claim: node.claim.clone(),
            redaction_label: node.redaction_label.clone(),
            rule: format!("{:?}", node.rule),
            confidence: node.confidence,
            premises: node.premises.iter().map(redacted_node_wire).collect(),
        }
    }

    let cap = match cap {
        DisclosureLevelWire::Full => eg_epistemic::DisclosureLevel::Full,
        DisclosureLevelWire::Skeleton => eg_epistemic::DisclosureLevel::Skeleton,
        DisclosureLevelWire::ExistenceOnly => eg_epistemic::DisclosureLevel::ExistenceOnly,
    };

    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let policy = eg_epistemic::AuthorityPolicy::default();
    let redacted = eg_epistemic::explain_belief_redacted_capped(
        &bg,
        node_id,
        &policy,
        isolation,
        actor_id,
        Some(cap),
    );

    let level = match redacted.level {
        eg_epistemic::DisclosureLevel::Full => DisclosureLevelWire::Full,
        eg_epistemic::DisclosureLevel::Skeleton => DisclosureLevelWire::Skeleton,
        eg_epistemic::DisclosureLevel::ExistenceOnly => DisclosureLevelWire::ExistenceOnly,
    };
    let existence = match redacted.existence {
        eg_epistemic::ExistenceSignal::Supported => ExistenceSignalWire::Supported,
        eg_epistemic::ExistenceSignal::Contradicted => ExistenceSignalWire::Contradicted,
        eg_epistemic::ExistenceSignal::Uncertain => ExistenceSignalWire::Uncertain,
    };

    // CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b),
    // same idiom as `explain_belief` above. `redacted.existence` (Supported/Contradicted/
    // Uncertain) is exactly the "status" concept for a redaction-capped read — the
    // caller's RLS actor may not see enough of the tree to justify a finer label.
    // `root` is `None` at `ExistenceOnly` (no structure rendered at all, by design), so
    // confidence/contradiction_count/policy_labels are only recorded when a tree is
    // actually present — never fabricated.
    let (confidence, contradicting, policy_labels) = match &redacted.root {
        Some(root) => {
            let (supporting, contradicting) = count_redacted_tree_rules(root);
            (
                Some(root.confidence),
                contradicting,
                eg_epistemic::classify_policy_labels(supporting, contradicting, 0).join(","),
            )
        }
        None => (None, 0, String::new()),
    };
    let _span = tracing::debug_span!(
        "epistemic.explain_belief_redacted",
        epistemic.confidence = tracing::field::Empty,
        epistemic.status = tracing::field::debug(existence),
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();
    // `confidence` is `Option<f64>` (unavailable at `ExistenceOnly`, see above) —
    // `tracing::field::Value` has no blanket `Option` impl, so record it after span
    // creation only when present, leaving the field `Empty` otherwise (never a
    // fabricated `0.0`).
    if let Some(c) = confidence {
        tracing::Span::current().record("epistemic.confidence", c);
    }

    ExplainBeliefRedactedResult {
        level,
        existence,
        root: redacted.root.as_ref().map(redacted_node_wire),
    }
}

/// L53 — wire-project an `eg_epistemic::query::WhyNot` (see `WhyNotWire` docs).
#[cfg(feature = "epistemic-tms")]
fn why_not_wire(wn: &eg_epistemic::WhyNot) -> crate::protocol::WhyNotWire {
    use crate::protocol::WhyNotWire;
    let (reason, blockers, competing) = match &wn.reason {
        eg_epistemic::WhyNotReason::Unknown => ("Unknown", Vec::new(), Vec::new()),
        eg_epistemic::WhyNotReason::InsufficientConfidence => {
            ("InsufficientConfidence", Vec::new(), Vec::new())
        }
        eg_epistemic::WhyNotReason::Contradicted { blockers } => {
            ("Contradicted", blockers.clone(), Vec::new())
        }
        eg_epistemic::WhyNotReason::Undecided { competing } => {
            ("Undecided", Vec::new(), competing.clone())
        }
    };
    WhyNotWire {
        claim: wn.claim.clone(),
        reason: reason.to_string(),
        blockers,
        competing,
        confidence: wn.confidence,
    }
}

/// L53 — wire-project an `eg_epistemic::MinimalFlipSet` ("what would invalidate it").
#[cfg(feature = "epistemic-tms")]
fn minimal_flip_set_wire(f: &eg_epistemic::MinimalFlipSet) -> crate::protocol::MinimalFlipSetWire {
    crate::protocol::MinimalFlipSetWire {
        claim: f.claim.clone(),
        believed_now: f.believed_now,
        evidence_ids: f.evidence_ids.iter().cloned().collect(),
        believed_after: f.believed_after,
    }
}

/// `Method::EpistemicStatus` (EPI-P3-5, L53) — the Phase-3 acceptance capstone: build a
/// [`BeliefGraph`] off `view` and run `eg_epistemic::epistemic_status` under the
/// default `AuthorityPolicy` (no facade caller binds a tenant-specific policy today,
/// matching `explain_belief`'s own posture), wire-projecting every facet.
#[cfg(feature = "epistemic-tms")]
fn epistemic_status_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
) -> crate::protocol::EpistemicStatusResult {
    use crate::protocol::{AuthorityPolicyWire, EpistemicStatusResult, EpistemicStatusWire};

    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let policy = eg_epistemic::AuthorityPolicy::default();
    let status = eg_epistemic::epistemic_status(&bg, node_id, &policy);

    // CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b),
    // same idiom as `explain_belief`/`explain_belief_redacted_wire` above. `EpistemicStatus`
    // is the richest already-computed source of the four (`believed`/`confidence`/
    // `contradicting`/`evidence`/`attacking` are all fields `eg_epistemic::epistemic_status`
    // already populated above) — no new epistemic computation, just reading them before
    // they're moved into the wire struct below.
    let policy_labels = eg_epistemic::classify_policy_labels(
        status.evidence.len(),
        status.contradicting.len(),
        status.attacking.len(),
    )
    .join(",");
    let _span = tracing::debug_span!(
        "epistemic.epistemic_status",
        epistemic.confidence = status.confidence,
        epistemic.status = if status.believed { "believed" } else { "not_believed" },
        epistemic.contradiction_count = status.contradicting.len(),
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    EpistemicStatusResult {
        status: EpistemicStatusWire {
            claim: status.claim,
            believed: status.believed,
            confidence: status.confidence,
            uncertainty: status.uncertainty,
            proof: proof_node_wire(&status.proof.root),
            why_not: status.why_not.as_ref().map(why_not_wire),
            evidence: status.evidence,
            contradicting: status.contradicting,
            attacking: status.attacking,
            authority: AuthorityPolicyWire {
                source_reliability: status.authority.source_reliability,
                attack_multiplier: status.authority.attack_multiplier,
                prior_strength: status.authority.prior_strength,
            },
            valid_time: status.valid_time,
            tx_time: status.tx_time,
            what_would_invalidate: status
                .what_would_invalidate
                .as_ref()
                .map(minimal_flip_set_wire),
        },
    }
}

/// `Method::ResolveConflict` (EPI-P3-7, gap-fill) — build a `BeliefGraph` off `view`
/// and run the Dung argumentation semantics `semantics` names
/// (`eg_epistemic::tms::{grounded_extension,preferred_extensions,stable_extensions}`
/// — reused as-is, never reimplemented), then partition `node_ids` into
/// surviving/defeated/undecided against the computed extension(s):
///
/// * `"grounded"`: the unique extension only ever gives the IN (accepted) set
///   directly (`grounded_extension`), so OUT vs UNDECIDED is recovered from the
///   SAME public API rather than needing the crate's private 3-way labelling: an id
///   NOT in the extension is `defeated` (OUT) iff at least one of its augmented
///   (bipolar-closed) attackers IS in the extension (`augmented_attackers`) —
///   otherwise it is `undecided` (caught in an unresolved/paraconsistent conflict,
///   e.g. an odd attack cycle, matching `eg_epistemic::tms`'s own module docs).
/// * `"preferred"`/`"stable"`: potentially several credulous extensions. An id in
///   EVERY extension `survives` (unanimous across every admissible "side"); an id in
///   NONE is `defeated` (never credulously acceptable); anything in SOME but not all
///   is `undecided` (contested). When `semantics` legitimately yields NO extension
///   at all (a real `stable` result over e.g. an odd cycle, or the crate's own
///   NP-hardness argument-count/search-budget caps firing — see `tms` module docs),
///   every requested id is reported `undecided` rather than fabricating a verdict.
///
/// An id in `node_ids` that names no argument anywhere in the graph degrades to
/// `undecided` (never fabricated as surviving/defeated) — the same "no signal ⇒ safe
/// default" convention every other epistemic op follows.
#[cfg(feature = "epistemic-tms")]
fn resolve_conflict_wire(
    node_ids: &[String],
    semantics: &str,
    view: &crate::graph::GraphView,
) -> Result<crate::protocol::ResolveConflictResult, String> {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);

    let (extension_sets, surviving, defeated, undecided): (
        Vec<std::collections::BTreeSet<String>>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
    ) = match semantics {
        "grounded" => {
            let grounded = eg_epistemic::grounded_extension(&bg);
            let mut surviving = Vec::new();
            let mut defeated = Vec::new();
            let mut undecided = Vec::new();
            for id in node_ids {
                if grounded.contains(id) {
                    surviving.push(id.clone());
                } else if eg_epistemic::augmented_attackers(&bg, id)
                    .iter()
                    .any(|a| grounded.contains(a))
                {
                    defeated.push(id.clone());
                } else {
                    undecided.push(id.clone());
                }
            }
            (vec![grounded], surviving, defeated, undecided)
        }
        "preferred" | "stable" => {
            let extensions = if semantics == "preferred" {
                eg_epistemic::preferred_extensions(&bg)
            } else {
                eg_epistemic::stable_extensions(&bg)
            };
            let mut surviving = Vec::new();
            let mut defeated = Vec::new();
            let mut undecided = Vec::new();
            for id in node_ids {
                if extensions.is_empty() {
                    undecided.push(id.clone());
                } else if extensions.iter().all(|e| e.contains(id)) {
                    surviving.push(id.clone());
                } else if extensions.iter().all(|e| !e.contains(id)) {
                    defeated.push(id.clone());
                } else {
                    undecided.push(id.clone());
                }
            }
            (extensions, surviving, defeated, undecided)
        }
        other => {
            return Err(format!(
                "ResolveConflict: unknown semantics '{other}' (expected \
                 grounded|preferred|stable)"
            ))
        }
    };

    Ok(crate::protocol::ResolveConflictResult {
        semantics: semantics.to_string(),
        surviving,
        defeated,
        undecided,
        extension_sets: extension_sets
            .into_iter()
            .map(|e| e.into_iter().collect())
            .collect(),
    })
}

/// `Method::WhatChanged` (EPI-P3-5, L53) — between two transaction times, which beliefs
/// changed and why, over the WHOLE graph (`eg_epistemic::what_changed`).
#[cfg(feature = "epistemic-tms")]
fn what_changed_wire(
    view: &crate::graph::GraphView,
    tx_from: u64,
    tx_to: u64,
) -> crate::protocol::WhatChangedResult {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let policy = eg_epistemic::AuthorityPolicy::default();
    let changed = eg_epistemic::what_changed(&bg, tx_from, tx_to, &policy);
    crate::protocol::WhatChangedResult {
        changed: changed
            .iter()
            .map(|c| crate::protocol::ChangedBeliefWire {
                id: c.id.clone(),
                believed_before: c.believed_before,
                believed_after: c.believed_after,
                confidence_before: c.confidence_before,
                confidence_after: c.confidence_after,
                evidence_added: c.evidence_added.clone(),
                evidence_removed: c.evidence_removed.clone(),
                reason: c.reason.clone(),
            })
            .collect(),
    }
}

/// SURPASS gap-closure ("unify the two evidence resolvers"): map an
/// `eg_alignment::ResolvedArtifact` onto its wire twin.
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
fn resolved_artifact_wire(
    r: eg_alignment::ResolvedArtifact,
) -> crate::protocol::ResolvedArtifactWire {
    match r {
        eg_alignment::ResolvedArtifact::Text {
            subject_ref,
            excerpt,
        } => crate::protocol::ResolvedArtifactWire {
            kind: "text".to_string(),
            subject_ref,
            excerpt: Some(excerpt),
            blob_ref: None,
            note: None,
            reason: None,
        },
        eg_alignment::ResolvedArtifact::Blob {
            subject_ref,
            blob_ref,
            note,
        } => crate::protocol::ResolvedArtifactWire {
            kind: "blob".to_string(),
            subject_ref,
            excerpt: None,
            blob_ref: Some(blob_ref),
            note: Some(note),
            reason: None,
        },
        eg_alignment::ResolvedArtifact::Unresolved {
            subject_ref,
            reason,
        } => crate::protocol::ResolvedArtifactWire {
            kind: "unresolved".to_string(),
            subject_ref,
            excerpt: None,
            blob_ref: None,
            note: None,
            reason: Some(unresolved_reason_wire(reason).to_string()),
        },
    }
}

/// Stable, machine-readable string form of `eg_alignment::UnresolvedReason` for
/// `ResolvedArtifactWire.reason` — the resolver reason-code catalog GOC-05's
/// acceptance gates require ("Attach exact resolver outputs and every
/// unresolved reason code").
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
fn unresolved_reason_wire(reason: eg_alignment::UnresolvedReason) -> &'static str {
    match reason {
        eg_alignment::UnresolvedReason::MissingRendition => "missing_rendition",
        eg_alignment::UnresolvedReason::CodecUnavailable => "codec_unavailable",
        eg_alignment::UnresolvedReason::PolicyDenied => "policy_denied",
        eg_alignment::UnresolvedReason::CorruptBytes => "corrupt_bytes",
        eg_alignment::UnresolvedReason::OutOfRange => "out_of_range",
    }
}

/// X-1 (CONCEPT:EG-X1) — wire-project an `eg_epistemic::EvidenceCitation`. `kind`
/// renders the `EdgeKind` via `Debug` (the SAME flat-string convention
/// `JustificationNodeWire::rule` uses for `JustRule`); `locus` reuses the
/// `evidence_locus_wire` mapper already defined above for `ExplainProvenance`.
/// SURPASS gap-closure ("unify the two evidence resolvers"): when `resolver` is
/// `Some` (the `alignment` feature is compiled in AND a blob store is configured —
/// see `explain_evidence_wire`), the citation's `locus` is ALSO resolved through it
/// (`eg_alignment::EvidenceResolver::resolve`), attaching the REAL excerpt/blob
/// digest as `resolved` instead of leaving the caller with locus metadata alone.
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
fn evidence_citation_wire(
    c: &eg_epistemic::EvidenceCitation,
    resolver: Option<&crate::server::blob::cas_resolver::CasEvidenceResolver<'_>>,
) -> crate::protocol::EvidenceCitationWire {
    let resolved = resolver
        .and_then(|resolver| eg_alignment::EvidenceResolver::resolve(resolver, &c.locus))
        .map(resolved_artifact_wire);
    crate::protocol::EvidenceCitationWire {
        evidence_id: c.evidence_id.clone(),
        kind: format!("{:?}", c.kind),
        locus: evidence_locus_wire(&c.locus),
        resolved,
    }
}

/// The `alignment`-less counterpart of the dual-gated `evidence_citation_wire`
/// above: `resolved` is always `None` (no `CasEvidenceResolver` exists to call in
/// this build) — same wire shape, honest absence rather than a fabricated
/// resolution.
#[cfg(all(feature = "evidence-graph", not(feature = "alignment")))]
fn evidence_citation_wire(
    c: &eg_epistemic::EvidenceCitation,
) -> crate::protocol::EvidenceCitationWire {
    crate::protocol::EvidenceCitationWire {
        evidence_id: c.evidence_id.clone(),
        kind: format!("{:?}", c.kind),
        locus: evidence_locus_wire(&c.locus),
        resolved: None,
    }
}

/// `Method::ExplainEvidence` (CONCEPT:EG-X1) — build a `BeliefGraph` off `view` and
/// resolve `node_id`'s cited multimodal evidence (`eg_epistemic::evidence_citations`).
/// SURPASS gap-closure ("unify the two evidence resolvers"): `blob_store`, when
/// `Some`, backs a `CasEvidenceResolver` over the SAME `view` snapshot so every
/// citation ALSO carries its resolved content — see `evidence_citation_wire`.
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
fn explain_evidence_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
    blob_store: Option<std::sync::Arc<dyn crate::server::blob::ChunkStore>>,
) -> crate::protocol::ExplainEvidenceResult {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let citations = eg_epistemic::evidence_citations(&bg, node_id);

    // CONCEPT:EG-OS.observability.slow-query-descriptor — OTEL epistemic span attributes (WS-1b),
    // same idiom as the other epistemic handlers above. `ExplainEvidence` runs no belief
    // propagation (no posterior confidence is computed here), so `epistemic.status` is a
    // fixed descriptor rather than a believed/contested verdict; `contradiction_count`/
    // `policy_labels` come from a cheap local classification of `bg.in_edges` (already
    // loaded by `from_graph_view` above) by `EdgeKind` — not a new propagation pass.
    let ins = bg.in_edges.get(node_id).map(Vec::as_slice).unwrap_or(&[]);
    let supporting = ins
        .iter()
        .filter(|(_, k)| *k == eg_epistemic::EdgeKind::Supports)
        .count();
    let contradicting = ins
        .iter()
        .filter(|(_, k)| {
            matches!(
                k,
                eg_epistemic::EdgeKind::Contradicts | eg_epistemic::EdgeKind::Attacks
            )
        })
        .count();
    let policy_labels =
        eg_epistemic::classify_policy_labels(supporting, contradicting, 0).join(",");
    let _span = tracing::debug_span!(
        "epistemic.explain_evidence",
        epistemic.status = "cited",
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    let resolver = blob_store
        .map(|store| crate::server::blob::cas_resolver::CasEvidenceResolver::new(view, store));

    crate::protocol::ExplainEvidenceResult {
        citations: citations
            .iter()
            .map(|c| evidence_citation_wire(c, resolver.as_ref()))
            .collect(),
    }
}

/// The `alignment`-less counterpart of the dual-gated `explain_evidence_wire`
/// above: byte-for-byte the pre-existing behavior (locus metadata only, no
/// resolved content — there is no `CasEvidenceResolver` to build without the
/// `alignment` feature).
#[cfg(all(feature = "evidence-graph", not(feature = "alignment")))]
fn explain_evidence_wire(
    node_id: &str,
    view: &crate::graph::GraphView,
) -> crate::protocol::ExplainEvidenceResult {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let citations = eg_epistemic::evidence_citations(&bg, node_id);

    let ins = bg.in_edges.get(node_id).map(Vec::as_slice).unwrap_or(&[]);
    let supporting = ins
        .iter()
        .filter(|(_, k)| *k == eg_epistemic::EdgeKind::Supports)
        .count();
    let contradicting = ins
        .iter()
        .filter(|(_, k)| {
            matches!(
                k,
                eg_epistemic::EdgeKind::Contradicts | eg_epistemic::EdgeKind::Attacks
            )
        })
        .count();
    let policy_labels =
        eg_epistemic::classify_policy_labels(supporting, contradicting, 0).join(",");
    let _span = tracing::debug_span!(
        "epistemic.explain_evidence",
        epistemic.status = "cited",
        epistemic.contradiction_count = contradicting,
        epistemic.policy_labels = %policy_labels,
    )
    .entered();

    crate::protocol::ExplainEvidenceResult {
        citations: citations.iter().map(evidence_citation_wire).collect(),
    }
}

/// `Method::CausalEstimate` (EPI-P3-3/P3-6) — build an `eg_epistemic::CausalGraph`
/// from the request-carried `variables` (rejecting an out-of-topological-order
/// parent as an explicit error, never a panic — mirrors
/// `CausalGraph::add_variable`'s own contract), then run whichever of the crate's
/// two non-counterfactual queries `mode` selects over `do_values`: the do-calculus
/// intervention (`CausalGraph::intervene`, `mode: Intervene` — the default) or the
/// observational conditioning query (`CausalGraph::observe`, `mode: Observe`).
/// Results are re-ordered to match the request's `variables` order (a `HashMap`
/// iteration order is not itself meaningful).
#[cfg(feature = "epistemic-causal")]
fn causal_estimate_wire(
    variables: &[crate::protocol::StructuralEquationWire],
    do_values: &std::collections::BTreeMap<String, f64>,
    mode: crate::protocol::CausalQueryModeWire,
) -> Result<crate::protocol::CausalEstimateResult, String> {
    let mut g = eg_epistemic::CausalGraph::new();
    for v in variables {
        let parents: Vec<(&str, f64)> = v.parents.iter().map(|(p, w)| (p.as_str(), *w)).collect();
        g.add_variable(v.id.clone(), parents, v.bias, v.noise_var)?;
    }
    let values: std::collections::HashMap<String, f64> =
        do_values.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let estimates = match mode {
        crate::protocol::CausalQueryModeWire::Intervene => g.intervene(&values)?,
        crate::protocol::CausalQueryModeWire::Observe => g.observe(&values)?,
    };

    let ordered = variables
        .iter()
        .map(|v| {
            let est = estimates.get(&v.id).copied().unwrap_or_else(|| {
                unreachable!("intervene()/observe() return an estimate for every declared variable")
            });
            (
                v.id.clone(),
                crate::protocol::CausalEstimateWire {
                    mean: est.mean,
                    variance: est.variance,
                    interval: est.interval,
                    level: est.level,
                },
            )
        })
        .collect();
    Ok(crate::protocol::CausalEstimateResult { estimates: ordered })
}

/// `Method::CausalCounterfactual` (EPI-P3-6) — build an `eg_epistemic::CausalGraph`
/// exactly as `causal_estimate_wire` does, then run Pearl's point-counterfactual
/// recipe (`CausalGraph::counterfactual`) over the request's fully-observed
/// `actual` unit and `do_values` intervention. Results (one point value per
/// variable) are re-ordered to match the request's `variables` order.
#[cfg(feature = "epistemic-causal")]
fn causal_counterfactual_wire(
    variables: &[crate::protocol::StructuralEquationWire],
    actual: &std::collections::BTreeMap<String, f64>,
    do_values: &std::collections::BTreeMap<String, f64>,
) -> Result<crate::protocol::CausalCounterfactualResult, String> {
    let mut g = eg_epistemic::CausalGraph::new();
    for v in variables {
        let parents: Vec<(&str, f64)> = v.parents.iter().map(|(p, w)| (p.as_str(), *w)).collect();
        g.add_variable(v.id.clone(), parents, v.bias, v.noise_var)?;
    }
    let actual: std::collections::HashMap<String, f64> =
        actual.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let do_: std::collections::HashMap<String, f64> =
        do_values.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let cf = g.counterfactual(&actual, &do_)?;

    let ordered = variables
        .iter()
        .map(|v| {
            let val = cf.get(&v.id).copied().unwrap_or_else(|| {
                unreachable!("counterfactual() returns a value for every declared variable")
            });
            (v.id.clone(), val)
        })
        .collect();
    Ok(crate::protocol::CausalCounterfactualResult { values: ordered })
}

/// `Method::RankByProvenance` (EPI-P3-3) — map the request-carried
/// `RetrievalCandidateWire`s onto `eg_epistemic::RetrievalCandidate` and run
/// `eg_epistemic::rank` under the request's `RankWeightsWire`.
#[cfg(feature = "epistemic-causal")]
fn rank_by_provenance_wire(
    candidates: &[crate::protocol::RetrievalCandidateWire],
    weights: crate::protocol::RankWeightsWire,
) -> crate::protocol::RankByProvenanceResult {
    let candidates: Vec<eg_epistemic::RetrievalCandidate> = candidates
        .iter()
        .map(|c| eg_epistemic::RetrievalCandidate {
            id: c.id.clone(),
            similarity: c.similarity,
            source_reliability: c.source_reliability,
            freshness: c.freshness,
            calibration: c.calibration.map(|cal| eg_epistemic::Calibration {
                interval: cal.interval,
                level: cal.level,
                evidence_count: cal.evidence_count,
            }),
        })
        .collect();
    let weights = eg_epistemic::RankWeights {
        similarity: weights.similarity,
        evidence_quality: weights.evidence_quality,
    };
    let ranked = eg_epistemic::rank(&candidates, weights);
    crate::protocol::RankByProvenanceResult {
        ranked: ranked
            .into_iter()
            .map(|r| crate::protocol::RankedResultWire {
                id: r.id,
                score: r.score,
                similarity: r.similarity,
                evidence_quality: r.evidence_quality,
            })
            .collect(),
    }
}

/// Resolve an OPEN txn, build a snapshot OVERLAID with its staged write-set +
/// embeddings, and run a unified cross-modal plan over it with read-your-own-writes
/// (CONCEPT:EG-KG.query.txn-cross-modal-ryow). The overlay is built under the (brief) state read + per-txn
/// lock, then the CPU-heavy plan runs OFF-lock on the blocking pool — the same
/// off-lock idiom as `run_unified`. Not result-cached (staged writes don't bump
/// `version()`). RLS filters the committed base snapshot to the caller's visible
/// rows BEFORE the txn's own staged writes are overlaid, so the txn always reads its
/// own writes while committed data stays isolation-scoped.
#[cfg(feature = "query")]
async fn run_unified_overlaid(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    plan: eg_plan::Plan,
    read_authority: Option<&GraphReadAuthority>,
    caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Response {
    #[cfg(not(feature = "security"))]
    let _ = caller;
    // Resolve the txn's target core + snapshot its staged write-set/embeddings while
    // holding only the cheap state read + per-txn lock; everything moved into the
    // off-lock closure is OWNED, so no lock is held across the compute.
    // `core` itself (an `Arc`) is threaded OUT of this block too — CONCEPT:EG-KG.query.served-vector-index-binding
    // / served-text-index-binding: the committed `SemanticStore`/text index are pushed
    // down via a guard taken INSIDE the off-lock closure below (not cloned here), so
    // `committed_semantic` is no longer materialized eagerly — see the closure.
    let (mut view, write_set, vectors, core, tsdb_graph) = {
        let s = state.read().await;
        let entry = match s.open_txns.get(txn_id) {
            Some(e) => e,
            None => {
                return Response::err(req_id, format!("unknown transaction '{}'", txn_id));
            }
        };
        let guard = entry.value().lock();
        let Some(expected_owner) = read_authority
            .and_then(GraphReadAuthority::carrier)
            .map(crate::server::access::CarrierAuthority::owner_scope)
        else {
            crate::metrics::access_denied();
            return Response::err(
                req_id,
                "ACCESS_DENIED: transaction read requires verified owner authority",
            );
        };
        if guard.agent != expected_owner {
            crate::metrics::access_denied();
            return Response::err(req_id, "ACCESS_DENIED: transaction is not owned by caller");
        }
        let core = match s.registry.get(&guard.graph) {
            Some(g) => g.core.clone(),
            None => {
                return Response::err(req_id, format!("Graph '{}' not found", guard.graph));
            }
        };
        // Committed base snapshot (O(V+E) structural copy), taken at ONE point in time
        // so the cross-modal read is snapshot-isolated.
        #[cfg_attr(not(feature = "security"), allow(unused_mut))]
        let mut view = core.analysis_snapshot();
        #[cfg(feature = "security")]
        rls.filter_view(caller, &mut view);
        (
            view,
            guard.write_set.clone(),
            guard.vectors.clone(),
            core,
            guard.graph.clone(),
        )
        // `guard` + `s` drop here — no lock held across the compute below.
    };
    #[cfg(feature = "tsdb")]
    let tsdb_scope = match served_tsdb_scope(&plan, &tsdb_graph, read_authority) {
        Ok(scope) => scope,
        Err(denied) => return Response::err(req_id, denied),
    };
    // RECONCILE (CONCEPT:EG-KG.query.native-time-series): the committed tsdb `SeriesStore` for `Op::TsScan`
    // fusion inside the txn, so an in-txn UQL reads COMMITTED series.
    #[cfg(feature = "tsdb")]
    let tsdb = if tsdb_scope.is_some() {
        state.read().await.tsdb_store.clone()
    } else {
        None
    };
    #[cfg(feature = "tsdb")]
    let (tsdb_tenant, tsdb_graph_scope) = match tsdb_scope {
        Some((tenant, graph)) => (Some(tenant), Some(graph)),
        None => (None, None),
    };
    // CONCEPT:EG-KG.query.txn-tsdb-read-your — the in-txn tsdb read-your-own-writes overlay: seed a `StagedSeries`
    // from the txn's OWN staged, uncommitted `GraphTxnState.measurements` so an in-txn
    // `Op::TsScan` sees its own points (merged BEFORE the committed store), while an
    // off-txn read (no overlay) still sees committed only. `SeriesStore` is redb-file-
    // backed with no in-memory overlay, so this dep-free map is the RYOW source.
    #[cfg(feature = "tsdb")]
    let staged_series = {
        let s = state.read().await;
        let mut staged = eg_plan::StagedSeries::new();
        if let Some(entry) = s.open_txns.get(txn_id) {
            let guard = entry.value().lock();
            for m in &guard.measurements {
                let series = eg_tsdb::store::SeriesKey::decode(&m.series)
                    .map(|key| key.series)
                    .unwrap_or_else(|| m.series.clone());
                staged.push_points(&series, m.points.iter().cloned());
            }
        }
        staged
    };
    // Overlay the txn's staged graph writes onto the RLS-filtered committed snapshot.
    overlay_write_set(&mut view, &write_set);
    // CONCEPT:EG-KG.query.overlay-leg-rls-filter — RLS on the STAGED-OVERLAY leg too. The committed base was
    // `filter_view`d above, but the staged write-set is overlaid AFTER that filter, so a
    // staged node the caller may not see (an owned+private `_owner`/`_visibility` blob)
    // would otherwise leak through an in-txn fused read. Re-filter the overlaid view so
    // BOTH the committed and the staged legs of a fused `Reason → Rank` honor per-agent
    // row visibility. A no-op when no RLS rules are registered (`has_rules()==false`
    // short-circuits `filter_view`), so the single-tenant RYOW path is byte-for-byte
    // unchanged.
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut view);
    match compute_off_lock(req_id, move || {
        #[cfg(feature = "text")]
        let served_text = crate::server::secondary_indexes::ServedTextIndex::new(core.clone());
        #[cfg(feature = "geo")]
        let served_spatial =
            crate::server::secondary_indexes::ServedSpatialIndex::new(core.clone());
        // Fast path (CONCEPT:EG-KG.query.served-vector-index-binding): no staged embeddings this txn ⇒
        // search the COMMITTED store directly through a guard — no clone, no forced
        // HNSW rebuild. Only when the txn actually staged embeddings do we need a
        // MUTATED overlay copy for read-your-own-writes (`semantic_overlay` always
        // clones its input, so it is worth paying only when there is something to
        // overlay).
        if vectors.is_empty() {
            let semantic_guard = core.semantic_store.read();
            run_unified(
                plan,
                &view,
                &semantic_guard,
                ServedIndexes {
                    #[cfg(feature = "text")]
                    text: Some(&served_text),
                    #[cfg(feature = "geo")]
                    spatial: Some(&served_spatial),
                    #[cfg(not(any(feature = "text", feature = "geo")))]
                    _marker: std::marker::PhantomData,
                },
                #[cfg(feature = "tsdb")]
                TsdbLegBind {
                    tsdb: tsdb.as_deref(),
                    tsdb_tenant: tsdb_tenant.as_deref(),
                    tsdb_graph: tsdb_graph_scope.as_deref(),
                    staged_series: Some(&staged_series),
                },
            )
        } else {
            let committed = core.semantic_store.read().clone();
            let semantic = eg_core::compute::semantic::semantic_overlay(committed, &vectors);
            run_unified(
                plan,
                &view,
                &semantic,
                ServedIndexes {
                    #[cfg(feature = "text")]
                    text: Some(&served_text),
                    #[cfg(feature = "geo")]
                    spatial: Some(&served_spatial),
                    #[cfg(not(any(feature = "text", feature = "geo")))]
                    _marker: std::marker::PhantomData,
                },
                #[cfg(feature = "tsdb")]
                TsdbLegBind {
                    tsdb: tsdb.as_deref(),
                    tsdb_tenant: tsdb_tenant.as_deref(),
                    tsdb_graph: tsdb_graph_scope.as_deref(),
                    staged_series: Some(&staged_series),
                },
            )
        }
    })
    .await
    {
        Ok(Ok(rows)) => {
            let bytes = rmp_serde::to_vec_named(&rows).unwrap_or_default();
            Response::ok(req_id, ResultPayload::Raw(bytes))
        }
        Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
        Err(resp) => resp,
    }
}

/// Replay a txn's staged durable-mutation `write_set` onto a cloned `GraphView` as an
/// overlay (CONCEPT:EG-KG.query.txn-cross-modal-ryow — in-txn cross-modal RYOW). Mirrors `handlers::txn::
/// apply_staged`, but against a view's overlay ops (no ledger/durability): so the
/// Filter (node props) and Traverse (BFS over staged edges) legs of an in-txn unified
/// query observe the txn's own uncommitted graph writes. Only the durable-mutation set
/// is ever staged (the protocol restricts `Txn*` to it); any other variant is a no-op.
#[cfg(feature = "query")]
pub(crate) fn overlay_write_set(view: &mut crate::graph::GraphView, write_set: &[Method]) {
    for m in write_set {
        match m {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => view.overlay_add_node(node_id.clone(), properties_msgpack.clone()),
            Method::RemoveNode { node_id } => view.overlay_remove_node(node_id),
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => {
                view.overlay_add_edge(
                    source_id.clone(),
                    target_id.clone(),
                    properties_msgpack.clone(),
                );
            }
            Method::RemoveEdge {
                source_id,
                target_id,
            } => view.overlay_remove_edge(source_id, target_id),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            } => {
                // A decode failure is a no-op overlay (mirrors `apply_staged`).
                if let (Ok(conditions), Ok(updates)) = (
                    eg_types::msgpack::decode_property_object(conditions_msgpack),
                    eg_types::msgpack::decode_property_object(updates_msgpack),
                ) {
                    view.overlay_compare_and_set_fields(node_id, &conditions, &updates);
                }
            }
            _ => {}
        }
    }
}

/// Execute a classified `Method::Sql` WRITE (CONCEPT:EG-KG.query.mirrors-pgwire). Mirrors the pgwire shim's
/// classify→execute write path, reusing the SAME engine primitives:
///   * graph-node DML (`INSERT`/`UPDATE`/`DELETE` on `nodes`) → the live `GraphCore`
///     write ops (`add_node` / `compare_and_set_fields` / `remove_node`) — the dispatch
///     shell then `mark_dirty`s the graph (this method classified Write) so the next
///     checkpoint persists it;
///   * user-table DDL/DML → the shared durable `TableStore`, where rows/catalog and
///     universal batch status/fence/idempotency/outbox share one commit-before-ack
///     redb transaction.
///
/// Blocking work (redb commits, the node scan) runs on the blocking pool via
/// `compute_off_lock`. Returns a `QueryResult`-shaped ack (`[tag]` column, one
/// rows-affected row) so the client decodes a write response exactly like a read.
/// The verified actor/scope fields for [`exec_sql_write`], bundled so the
/// function stays under the clippy argument-count ceiling.
#[cfg(feature = "query")]
struct SqlWriteScope<'a> {
    graph_name: &'a str,
    tenant_scope: &'a str,
    caller: Option<&'a str>,
}

#[cfg(feature = "query")]
async fn exec_sql_write(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    read_authority: &GraphReadAuthority,
    sql_method: Method,
    core: &Arc<GraphCore>,
    store: &eg_query::TableStore,
    kind: eg_query::StatementKind,
) -> Response {
    let SqlWriteScope {
        graph_name,
        tenant_scope,
        caller,
    } = scope;
    use eg_query::StatementKind as K;
    let read_core = read_authority.project_core(core);
    match kind {
        K::InsertNodes(ins) => {
            let core = core.clone();
            let visible = read_core.clone();
            let r = compute_off_lock(req_id, move || {
                let mut n = 0usize;
                for node in ins.rows {
                    if core.has_node(&node.node_id) && !visible.has_node(&node.node_id) {
                        crate::metrics::access_denied();
                        return Err("ACCESS_DENIED: node write is outside the visible row scope"
                            .to_string());
                    }
                    let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(node.properties))
                        .map_err(|e| format!("encode node properties: {e}"))?;
                    core.add_node(node.node_id, blob);
                    n += 1;
                }
                Ok::<usize, String>(n)
            })
            .await;
            sql_write_ack(req_id, "INSERT", r)
        }
        K::UpdateNodes(upd) => {
            let core = core.clone();
            let r = compute_off_lock(req_id, move || {
                let ids = matched_node_ids(&read_core, &upd.selector);
                let conditions = serde_json::Map::new();
                // CONCEPT:EG-KG.query.compound-predicate-decode — re-check a compound predicate under the write guard.
                let pred = match &upd.selector {
                    eg_query::WhereEq::Predicate { pred, .. } => Some(pred.clone()),
                    eg_query::WhereEq::Id(_) => None,
                };
                let mut n = 0usize;
                for id in ids {
                    let applied = match &pred {
                        Some(p) => core.compare_and_set_fields_if(&id, p, &conditions, &upd.set),
                        None => core.compare_and_set_fields(&id, &conditions, &upd.set),
                    };
                    if applied {
                        n += 1;
                    }
                }
                Ok::<usize, String>(n)
            })
            .await;
            sql_write_ack(req_id, "UPDATE", r)
        }
        K::DeleteNodes(del) => {
            let core = core.clone();
            let r = compute_off_lock(req_id, move || {
                let ids = matched_node_ids(&read_core, &del.selector);
                // CONCEPT:EG-KG.query.compound-predicate-decode — re-check a compound predicate under the write guard.
                let pred = match &del.selector {
                    eg_query::WhereEq::Predicate { pred, .. } => Some(pred.clone()),
                    eg_query::WhereEq::Id(_) => None,
                };
                let mut n = 0usize;
                for id in ids {
                    let removed = match &pred {
                        Some(p) => core.remove_node_if(&id, p),
                        None => {
                            core.remove_node(id);
                            true
                        }
                    };
                    if removed {
                        n += 1;
                    }
                }
                Ok::<usize, String>(n)
            })
            .await;
            sql_write_ack(req_id, "DELETE", r)
        }
        K::CreateTable(plan) => {
            let columns = match to_store_columns(&plan.columns) {
                Ok(columns) => columns,
                Err(error) => return Response::err(req_id, format!("SQL error: {error}")),
            };
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::CreateTable {
                schema: eg_query::TableSchema::new(plan.name, columns),
                if_not_exists: plan.if_not_exists,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "CREATE TABLE",
            )
            .await
        }
        K::DropTable(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::DropTable {
                name: plan.name,
                if_exists: plan.if_exists,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "DROP TABLE",
            )
            .await
        }
        // CONCEPT:EG-KG.query.register-user-tables-alongside ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog the rest — one dispatch helper.
        K::AlterTable(plan) => {
            let op = match alter_table_txn_op(plan) {
                Ok(op) => op,
                Err(error) => return Response::err(req_id, format!("SQL error: {error}")),
            };
            let mut txn = eg_query::TableTxn::new();
            txn.push(op);
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "ALTER TABLE",
            )
            .await
        }
        K::InsertTable(ins) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::Insert {
                table: ins.table,
                col_order: ins.columns,
                rows: ins.rows,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "INSERT",
            )
            .await
        }
        K::InsertSelect(ins) => {
            // The SELECT half runs through the SAME tables-aware DataFusion path (so it
            // can JOIN user tables AND the graph); its projected rows are then durably
            // inserted. Column COUNT must match the insert column list.
            let eg_query::InsertSelect {
                table,
                columns,
                select_sql,
            } = ins;
            let read_store = store.clone();
            let snap = read_core.analysis_snapshot();
            let expected_columns = columns.len();
            let r = compute_off_lock(req_id, move || {
                let read =
                    eg_query::exec_sql_typed_with_tables(&snap, &read_store, &select_sql)?;
                if read.columns.len() != expected_columns {
                    return Err(format!(
                        "INSERT … SELECT column count mismatch: {} target columns, {} selected",
                        expected_columns,
                        read.columns.len()
                    ));
                }
                Ok::<_, String>(read.rows)
            })
            .await;
            let rows = match r {
                Ok(Ok(rows)) => rows,
                Ok(Err(error)) => return Response::err(req_id, format!("SQL error: {error}")),
                Err(response) => return response,
            };
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::Insert {
                table,
                col_order: columns,
                rows,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "INSERT",
            )
            .await
        }
        K::UpdateTable(upd) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::Update {
                table: upd.table,
                set: upd.set,
                selector: upd.selector.pred,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "UPDATE",
            )
            .await
        }
        K::DeleteTable(del) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::Delete {
                table: del.table,
                selector: del.selector.pred,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "DELETE",
            )
            .await
        }
        // Method::Sql is request-scoped and therefore cannot honestly represent
        // connection-scoped transaction control. Fail closed instead of pretending
        // BEGIN/COMMIT/ROLLBACK succeeded while committing each request separately.
        K::Begin | K::Commit | K::Rollback => Response::err(
            req_id,
            "SQL error: transaction control requires a stateful SQL wire connection"
                .to_string(),
        ),
        // CONCEPT:EG-KG.query.insert-into-nodes-select — INSERT INTO nodes … SELECT over the RPC wire (write-ack; no RETURNING).
        K::InsertNodesSelect(ins) => {
            let core = core.clone();
            let store = store.clone();
            let visible = read_core.clone();
            let snap = visible.analysis_snapshot();
            let r = compute_off_lock(req_id, move || {
                let read = eg_query::exec_sql_typed_with_tables(&snap, &store, &ins.select_sql)?;
                if read.columns.len() != ins.columns.len() {
                    return Err(format!(
                        "INSERT INTO nodes … SELECT column count mismatch: {} target columns, {} selected",
                        ins.columns.len(),
                        read.columns.len()
                    ));
                }
                let id_pos = ins
                    .columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case("id"))
                    .ok_or("INSERT INTO nodes … SELECT must include the `id` column")?;
                let empty = serde_json::Map::new();
                let mut n = 0usize;
                for row in read.rows {
                    let node_id = cell_to_node_id(&row[id_pos])?;
                    let mut props = serde_json::Map::new();
                    for (i, col) in ins.columns.iter().enumerate() {
                        if i != id_pos {
                            props.insert(col.clone(), row[i].clone());
                        }
                    }
                    if core.has_node(&node_id) && !visible.has_node(&node_id) {
                        crate::metrics::access_denied();
                        return Err(
                            "ACCESS_DENIED: node write is outside the visible row scope"
                                .to_string(),
                        );
                    }
                    if visible.has_node(&node_id) {
                        match ins.on_conflict.as_ref().map(|oc| &oc.action) {
                            Some(eg_query::OnConflictAction::DoNothing) => continue,
                            Some(eg_query::OnConflictAction::DoUpdate(set)) => {
                                core.compare_and_set_fields(&node_id, &empty, set);
                                n += 1;
                                continue;
                            }
                            None => {}
                        }
                    }
                    let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(props))
                        .map_err(|e| format!("encode node properties: {e}"))?;
                    core.add_node(node_id, blob);
                    n += 1;
                }
                Ok::<usize, String>(n)
            })
            .await;
            sql_write_ack(req_id, "INSERT", r)
        }
        // CONCEPT:EG-KG.query.update-delete-from — UPDATE nodes … FROM … over the RPC wire.
        K::UpdateNodesJoin(upd) => {
            let core = core.clone();
            let store = store.clone();
            let snap = read_core.analysis_snapshot();
            let r = compute_off_lock(req_id, move || {
                let read = eg_query::exec_sql_typed_with_tables(&snap, &store, &upd.resolve_sql)?;
                if read.columns.len() != upd.set_targets.len() + 1 {
                    return Err(format!(
                        "UPDATE … FROM resolution shape mismatch: expected id + {} set columns, got {}",
                        upd.set_targets.len(),
                        read.columns.len()
                    ));
                }
                let empty = serde_json::Map::new();
                let mut seen = std::collections::HashSet::new();
                let mut n = 0usize;
                for row in read.rows {
                    let id = cell_to_node_id(&row[0])?;
                    if !seen.insert(id.clone()) {
                        continue;
                    }
                    let mut updates = serde_json::Map::new();
                    for (i, col) in upd.set_targets.iter().enumerate() {
                        updates.insert(col.clone(), row[i + 1].clone());
                    }
                    if core.compare_and_set_fields(&id, &empty, &updates) {
                        n += 1;
                    }
                }
                Ok::<usize, String>(n)
            })
            .await;
            sql_write_ack(req_id, "UPDATE", r)
        }
        // CONCEPT:EG-KG.query.update-delete-from — DELETE FROM nodes … USING … over the RPC wire.
        K::DeleteNodesJoin(del) => {
            let core = core.clone();
            let store = store.clone();
            let snap = read_core.analysis_snapshot();
            let r = compute_off_lock(req_id, move || {
                let read = eg_query::exec_sql_typed_with_tables(&snap, &store, &del.resolve_sql)?;
                let mut seen = std::collections::HashSet::new();
                let mut n = 0usize;
                for row in read.rows {
                    let id = cell_to_node_id(&row[0])?;
                    if !seen.insert(id.clone()) {
                        continue;
                    }
                    core.remove_node(id);
                    n += 1;
                }
                Ok::<usize, String>(n)
            })
            .await;
            sql_write_ack(req_id, "DELETE", r)
        }
        // CONCEPT:EG-KG.query.create-drop-view — CREATE/DROP VIEW over the RPC wire.
        K::CreateView(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::CreateView {
                name: plan.name,
                select_sql: plan.select_sql,
                or_replace: plan.or_replace,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "CREATE VIEW",
            )
            .await
        }
        K::DropView(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::DropView {
                name: plan.name,
                if_exists: plan.if_exists,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "DROP VIEW",
            )
            .await
        }
        // CONCEPT:EG-KG.query.create-drop-extension-over — CREATE/DROP EXTENSION over the RPC wire.
        K::CreateExtension {
            name,
            if_not_exists,
        } => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::CreateExtension {
                name,
                if_not_exists,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "CREATE EXTENSION",
            )
            .await
        }
        K::DropExtension { name, if_exists } => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::DropExtension { name, if_exists });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "DROP EXTENSION",
            )
            .await
        }
        // CONCEPT:EG-KG.query.create-drop-function — CREATE/DROP FUNCTION over the RPC wire.
        K::CreateFunction(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::CreateFunction {
                function: plan.func,
                or_replace: plan.or_replace,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "CREATE FUNCTION",
            )
            .await
        }
        K::DropFunction(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::DropFunction {
                name: plan.name,
                if_exists: plan.if_exists,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "DROP FUNCTION",
            )
            .await
        }
        // ── Postgres-family extension parity (wave 19) ──────────────────────────
        // CONCEPT:EG-KG.query.postgres-family-extension-plan — Apache AGE cypher() is a read; run it + project the agtype
        // result onto the AS columns, returning a result set (like the read path).
        K::CypherCall(plan) => {
            #[cfg(feature = "cypher")]
            {
                let core = read_core.clone();
                let r = compute_off_lock(req_id, move || {
                    let snap = core.analysis_snapshot();
                    let result = eg_query::exec_cypher(&snap, &plan.cypher)?;
                    let typed = eg_query::project_cypher_rows(
                        &result,
                        &plan.columns,
                        plan.projection.as_deref(),
                    )?;
                    Ok::<_, String>(crate::protocol::QueryResult {
                        columns: typed.columns.iter().map(|c| c.name.clone()).collect(),
                        rows: typed
                            .rows
                            .iter()
                            .map(|row| rmp_serde::to_vec_named(row).unwrap_or_default())
                            .collect(),
                    })
                })
                .await;
                match r {
                    Ok(Ok(result)) => Response::ok(
                        req_id,
                        ResultPayload::Raw(rmp_serde::to_vec_named(&result).unwrap_or_default()),
                    ),
                    Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
                    Err(resp) => resp,
                }
            }
            #[cfg(not(feature = "cypher"))]
            {
                let _ = plan;
                Response::err(
                    req_id,
                    "SQL error: cypher() (Apache AGE) requires the engine's `cypher` feature"
                        .to_string(),
                )
            }
        }
        // CONCEPT:EG-KG.query.real-ann-top-k — persist the pgvector ANN index used
        // by the native eg-ann pushdown planner.
        K::CreateAnnIndex(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::PutAnnIndex { plan });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "CREATE INDEX",
            )
            .await
        }
        // CONCEPT:EG-KG.query.continuous-aggregate-lowering — validate and persist
        // the native hypertable declaration through the SQL MutationBatch kernel.
        K::CreateHypertable(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::PutHypertable { plan });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "CREATE TABLE",
            )
            .await
        }
        // CONCEPT:EG-KG.query.continuous-aggregate-lowering — lower the continuous aggregate onto the durable view catalog.
        K::CreateContinuousAggregate(plan) => {
            let mut txn = eg_query::TableTxn::new();
            txn.push(eg_query::TxnOp::CreateView {
                name: plan.name,
                select_sql: plan.select_sql,
                or_replace: true,
            });
            commit_sql_catalog_txn(
                req_id,
                SqlWriteScope {
                    graph_name,
                    tenant_scope,
                    caller,
                },
                sql_method,
                store,
                txn,
                "CREATE MATERIALIZED VIEW",
            )
            .await
        }
        // `COPY … FROM STDIN` is a streamed, connection-stateful pgwire op (rows arrive
        // as CopyData frames), with no single-request wire form.
        K::CopyIn(_) => Response::err(
            req_id,
            "SQL error: COPY … FROM STDIN is a streaming pgwire operation, not available over Method::Sql".to_string(),
        ),
        // The caller only routes non-`Read` statements here.
        K::Read => Response::err(req_id, "SQL error: read routed to write path".to_string()),
    }
}

/// A scalar cell (from a resolved SELECT row) coerced to the string node-id form the
/// engine stores (CONCEPT:EG-KG.query.insert-into-nodes-select/047).
#[cfg(feature = "query")]
fn cell_to_node_id(v: &serde_json::Value) -> Result<String, String> {
    match v {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Bool(b) => Ok(b.to_string()),
        serde_json::Value::Null => Err("resolved a NULL `id` for a node write".to_string()),
        other => Err(format!("`id` must be a scalar, got {other}")),
    }
}

/// Build a `QueryResult`-shaped write ack and map the off-lock outcome (CONCEPT:EG-KG.query.mirrors-pgwire).
#[cfg(feature = "query")]
fn sql_write_ack(
    req_id: u64,
    tag: &str,
    outcome: Result<Result<usize, String>, Response>,
) -> Response {
    match outcome {
        Ok(Ok(n)) => {
            let result = crate::protocol::QueryResult {
                columns: vec![tag.to_string()],
                rows: vec![
                    rmp_serde::to_vec_named(&vec![serde_json::Value::from(n as u64)])
                        .unwrap_or_default(),
                ],
            };
            Response::ok(
                req_id,
                ResultPayload::Raw(rmp_serde::to_vec_named(&result).unwrap_or_default()),
            )
        }
        Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
        Err(resp) => resp,
    }
}

/// SQL catalog/table native coordinator. The user-table rows/catalog, terminal
/// MutationBatch record, SQL-domain OCC/fence, idempotency result and outbox land
/// in one owner-scoped SQL-catalog transaction. Query text and parameters are represented
/// only by a SHA-256 operation digest in durable metadata.
#[cfg(feature = "query")]
async fn commit_sql_catalog_txn(
    req_id: u64,
    scope: SqlWriteScope<'_>,
    sql_method: Method,
    store: &eg_query::TableStore,
    txn: eg_query::TableTxn,
    tag: &'static str,
) -> Response {
    let SqlWriteScope {
        graph_name,
        tenant_scope,
        caller,
    } = scope;
    let tenant_scope = tenant_scope.to_string();
    let graph_name = graph_name.to_string();
    let caller = caller.map(ToOwned::to_owned);
    let store = store.clone();
    let outcome = compute_off_lock(req_id, move || {
        let batch_id = crate::server::mutation_batch::opaque_request_key(
            "sql-catalog",
            &graph_name,
            req_id,
            &sql_method,
        );
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        // Recovery after commit-before-ack rebuilds the exact proposed batch with
        // the stored OCC observation. `commit_txn_batch` then verifies every
        // identity byte (including principal + operation digest) before returning
        // the result without applying `txn` again.
        let expected_version = match store.mutation_batch(&batch_id)? {
            Some(record) => record
                .batch
                .expected_graph_version
                .ok_or_else(|| "committed SQL MutationBatch has no OCC version".to_string())?,
            None => store.mutation_version(&tenant_scope, &graph_name)?,
        };
        let batch = crate::server::mutation_batch::compile_opaque_method(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &batch_id,
                request_id: req_id,
                principal: caller.as_deref(),
                tenant: &tenant_scope,
                graph: &graph_name,
                placement_epoch: 0,
                idempotency_key: &batch_id,
                expected_graph_version: Some(expected_version),
                fencing_token: None,
                created_at_ms,
                default_surface: crate::mutation_batch::MutationSurface::Query,
                authoritative_state: None,
            },
            &sql_method,
            crate::mutation_batch::MutationSurface::Query,
            crate::mutation_batch::MutationDomain::SqlCatalog,
            "sql_catalog_operation",
        )?;
        let committed = store.commit_txn_batch(&txn, &batch, created_at_ms)?;
        let bytes = committed
            .record
            .result_msgpack
            .as_deref()
            .ok_or_else(|| "committed SQL MutationBatch has no result".to_string())?;
        eg_types::msgpack::decode_bounded::<usize>(
            bytes,
            eg_types::msgpack::MsgpackLimits::new(64, 1, 1),
        )
        .map_err(|_| "committed SQL result is corrupt".to_string())
    })
    .await;
    sql_write_ack(req_id, tag, outcome)
}

/// Resolve the node ids a WHERE selects (CONCEPT:EG-KG.query.mirrors-pgwire). `Id` is the fast path (the
/// node if it exists); `Predicate` (CONCEPT:EG-KG.query.compound-predicate-decode) scans the node store once,
/// decodes each blob to a row map (with the synthetic `id` column injected) and
/// evaluates the compound predicate. The matched ids are re-checked under the write
/// guard by the caller (`compare_and_set_fields_if`/`remove_node_if`).
#[cfg(feature = "query")]
fn matched_node_ids(core: &GraphCore, selector: &eg_query::WhereEq) -> Vec<String> {
    match selector {
        eg_query::WhereEq::Id(id) => {
            if core.has_node(id) {
                vec![id.clone()]
            } else {
                Vec::new()
            }
        }
        eg_query::WhereEq::Predicate { pred, .. } => {
            let mut out = Vec::new();
            for (id, blob) in core.get_nodes() {
                if let Ok(mut obj) = eg_types::msgpack::decode_property_object(&blob) {
                    obj.entry("id".to_string())
                        .or_insert_with(|| serde_json::Value::String(id.clone()));
                    if pred.eval(&obj) {
                        out.push(id);
                    }
                }
            }
            out
        }
    }
}

/// Resolve classify `ColumnDef`s (raw SQL type spellings) into store `Column`s
/// (CONCEPT:EG-KG.query.mirrors-pgwire — mirrors the pgwire `to_store_columns`).
#[cfg(feature = "query")]
fn to_store_columns(cols: &[eg_query::ColumnDef]) -> Result<Vec<eg_query::Column>, String> {
    cols.iter()
        .map(|c| {
            let ty = eg_query::ColumnType::parse(&c.type_name)?;
            Ok(eg_query::Column {
                name: c.name.clone(),
                ty,
                nullable: c.nullable,
                primary_key: c.primary_key,
                unique: c.unique,
                serial: c.serial,
                default: c.default.clone(),
                check: c.check.clone(),
            })
        })
        .collect()
}

/// Lower a decoded `ALTER TABLE` action into the shared transactional table-store
/// operation. The SQL native coordinator can then apply the catalog change and its
/// MutationBatch metadata in one redb transaction.
#[cfg(feature = "query")]
fn alter_table_txn_op(plan: eg_query::AlterTablePlan) -> Result<eg_query::TxnOp, String> {
    use eg_query::AlterTableAction as A;
    match plan.action {
        A::AddColumn(col) => {
            let columns = to_store_columns(std::slice::from_ref(&col))?;
            let column = columns.into_iter().next().ok_or("ALTER TABLE: no column")?;
            Ok(eg_query::TxnOp::AddColumn {
                table: plan.name,
                column,
            })
        }
        A::DropColumn { column, if_exists } => Ok(eg_query::TxnOp::DropColumn {
            table: plan.name,
            column,
            if_exists,
        }),
        A::RenameColumn { from, to } => Ok(eg_query::TxnOp::RenameColumn {
            table: plan.name,
            from,
            to,
        }),
        A::RenameTable { new_name } => Ok(eg_query::TxnOp::RenameTable {
            table: plan.name,
            new_name,
        }),
        A::AlterColumnType { column, new_type } => {
            let ty = eg_query::ColumnType::parse(&new_type)?;
            Ok(eg_query::TxnOp::AlterColumnType {
                table: plan.name,
                column,
                new_type: ty,
            })
        }
        A::DropConstraint {
            constraint,
            if_exists,
        } => Ok(eg_query::TxnOp::DropConstraint {
            table: plan.name,
            constraint,
            if_exists,
        }),
    }
}

/// Produce the off-lock `GraphView` the query planner consumes, with per-agent
/// Row-Level Security applied IN the read/plan path (CONCEPT:EG-KG.sharding.row-level-security). Under the
/// `security` feature the owned snapshot is filtered down to the rows `caller` may
/// see BEFORE it reaches any query surface (SQL/Cypher/unified), so no surface can
/// exfiltrate a forbidden row. Without the feature this is exactly
/// `core.analysis_snapshot()` (zero overhead, behavior unchanged). Used on the
/// `not(result-cache)` path; with the result cache the same `filter_view` is applied
/// inline on the versioned snapshot so the version pairs atomically with the filter.
/// CONCEPT:EG-KG.query.fence-stripper — build the `schema_hint` fed to the NL planner: the distinct node
/// LABELS present in the target graph (capped so a huge graph stays cheap), so the model
/// targets real labels. Scans up to a bound of the snapshot's node blobs for their
/// `type`/`node_type`/`label` field (mirroring `get_nodes_by_label`). Best-effort — an
/// empty hint (no labels found) is fine; the planner's system prompt carries the grammar.
#[cfg(feature = "nl-query")]
fn nl_schema_hint(core: &Arc<GraphCore>) -> String {
    use std::collections::BTreeSet;
    const SCAN_CAP: usize = 512;
    let snap = core.analysis_snapshot();
    let mut labels: BTreeSet<String> = BTreeSet::new();
    for blob in snap.node_properties.values().take(SCAN_CAP) {
        if let Ok(v) = eg_types::msgpack::decode_property_value(blob) {
            for key in ["type", "node_type", "label"] {
                if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
                    labels.insert(s.to_string());
                    break;
                }
            }
        }
        if labels.len() >= 64 {
            break;
        }
    }
    if labels.is_empty() {
        "Available node labels: (none discovered)".to_string()
    } else {
        format!(
            "Available node labels: {}",
            labels.into_iter().collect::<Vec<_>>().join(", ")
        )
    }
}

#[cfg(all(
    any(feature = "query", feature = "cypher", feature = "graphql"),
    not(feature = "result-cache")
))]
fn rls_snapshot(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> crate::graph::GraphView {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    snap
}

/// ⚠ THE RLS-AWARE RESULT-CACHE KEY (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231 — the headline
/// reconciliation). RLS makes a query's RESULT agent-specific: agent A and agent B
/// running the SAME query text see DIFFERENT rows (A cannot see B's private nodes).
/// The result cache is keyed by `(query-hash, version)`; if that hash ignored the
/// caller, agent A's cached (A-filtered) result could be served to agent B for the
/// same query text — a cross-agent data leak.
///
/// The caller's RLS context is always folded into the hash
/// `kind` so a different caller keys to a different cache slot. The agent_id IS the
/// complete RLS visibility key: `IsolationLayer::filter_view`/`can_see_row` resolve
/// a row's visibility for a caller PURELY from that caller's agent_id against the
/// registered identities (owner / explicit grants / manager-of / System role), so
/// two requests with the same agent_id always get the byte-identical filtered view,
/// and two with different agent_ids may not — exactly the cache-key equivalence we
/// need. A build without the `security` feature uses the plain `(kind, payload)`.
#[cfg(all(
    feature = "result-cache",
    any(feature = "query", feature = "cypher", feature = "graphql")
))]
fn rls_cache_hash(
    kind: &str,
    payload: &[u8],
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] _rls: &Arc<crate::isolation::IsolationLayer>,
) -> u128 {
    #[cfg(feature = "security")]
    {
        let salted_kind = format!("rls:{caller}:{kind}");
        ResultCache::hash_query(&salted_kind, payload)
    }
    #[cfg(not(feature = "security"))]
    {
        ResultCache::hash_query(kind, payload)
    }
}

#[cfg(test)]
mod current_auth_test_support {
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request};
    use crate::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEST_AGENT: &str = "unit-test-agent";
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    pub(super) fn current_isolation() -> IsolationLayer {
        current_isolation_with_agents(&[])
    }

    pub(super) fn current_isolation_with_agents(agent_ids: &[&str]) -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        for agent_id in agent_ids {
            isolation.register_agent(AgentIdentity {
                agent_id: (*agent_id).to_string(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                roles: Vec::new(),
            });
        }
        isolation
    }

    pub(super) fn current_request(secret: &str, id: u64, graph: &str, method: Method) -> Request {
        current_request_as(secret, id, graph, TEST_AGENT, method)
    }

    pub(super) fn current_request_as(
        secret: &str,
        id: u64,
        graph: &str,
        agent_id: &str,
        method: Method,
    ) -> Request {
        std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
        std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
        std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
        std::env::set_var(
            "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
            std::env::temp_dir().join(format!("epistemic-graph-unit-auth-{}", std::process::id())),
        );
        let context = RequestContextClaims {
            principal: agent_id.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: agent_id.to_string(),
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
            agent_id: Some(agent_id.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "query-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("query-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            secret,
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
}

#[cfg(all(test, feature = "security", feature = "query", feature = "cypher"))]
mod rls_no_exfiltrate_tests {
    //! Proof (CONCEPT:EG-KG.sharding.row-level-security): RLS filters the read/plan-path snapshot so neither
    //! SQL nor Cypher can exfiltrate a forbidden row. Agent A's query MUST exclude
    //! agent B's private node; a public node is visible to both.
    use crate::graph::GraphView;
    use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
    use std::sync::Arc;

    fn node_blob(pairs: &[(&str, &str)]) -> Arc<Vec<u8>> {
        let m: std::collections::BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Arc::new(rmp_serde::to_vec_named(&m).unwrap())
    }

    /// Three nodes: B's private, an explicitly public one, and an untagged one.
    fn seeded_view() -> GraphView {
        let mut v = GraphView::default();
        for id in ["secret_b", "public_x", "untagged_z"] {
            let idx = v.graph.add_node(id.to_string());
            v.node_map.insert(id.to_string(), idx);
        }
        v.node_properties.insert(
            "secret_b".to_string(),
            node_blob(&[
                ("type", "Secret"),
                ("_owner", "bob"),
                ("_visibility", "private"),
            ]),
        );
        v.node_properties.insert(
            "public_x".to_string(),
            node_blob(&[
                ("type", "Public"),
                ("_owner", "bob"),
                ("_visibility", "public"),
            ]),
        );
        v.node_properties
            .insert("untagged_z".to_string(), node_blob(&[("type", "Untagged")]));
        v
    }

    fn isolation() -> IsolationLayer {
        let mut layer = IsolationLayer::new();
        layer.register_agent(AgentIdentity {
            agent_id: "alice".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "bob".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec![],
        });
        layer
    }

    fn sql_ids(view: &GraphView) -> Vec<String> {
        let r = eg_query::exec_sql(
            view,
            "SELECT id FROM nodes",
            &eg_query::CancellationToken::new(),
        )
        .expect("sql");
        r.rows
            .iter()
            .filter_map(|blob| {
                let cells: Vec<serde_json::Value> = rmp_serde::from_slice(blob).ok()?;
                cells
                    .first()
                    .and_then(|c| c.as_str())
                    .map(|s| s.to_string())
            })
            .collect()
    }

    #[test]
    fn sql_excludes_other_agents_private_node() {
        let layer = isolation();
        // Alice's filtered view: NO secret_b.
        let mut va = seeded_view();
        layer.filter_view("alice", &mut va);
        let ids = sql_ids(&va);
        assert!(
            !ids.contains(&"secret_b".to_string()),
            "exfiltration: {ids:?}"
        );
        assert!(
            ids.contains(&"public_x".to_string()),
            "public hidden: {ids:?}"
        );
        assert!(!ids.contains(&"untagged_z".to_string()));

        // Bob (owner) sees his own private node.
        let mut vb = seeded_view();
        layer.filter_view("bob", &mut vb);
        let ids_b = sql_ids(&vb);
        assert!(
            ids_b.contains(&"secret_b".to_string()),
            "owner blocked: {ids_b:?}"
        );
    }

    #[test]
    fn cypher_excludes_other_agents_private_node() {
        let layer = isolation();
        let mut va = seeded_view();
        layer.filter_view("alice", &mut va);
        let r = eg_query::exec_cypher(&va, "MATCH (n) RETURN n").expect("cypher");
        // The cypher result must not reference the hidden node id anywhere.
        let any_secret = r
            .rows
            .iter()
            .any(|blob| String::from_utf8_lossy(blob).contains("secret_b"));
        assert!(!any_secret, "cypher exfiltrated secret_b");
    }
}

// ── Version-keyed result cache, end-to-end through dispatch (CONCEPT:EG-KG.coordination.distributed-cache-coherence) ──
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
    use super::current_auth_test_support::{current_isolation, current_request};
    use crate::channels::ChannelManager;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::dispatch;
    use crate::server::state::ServerState;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "result-cache-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        // Post-FLIP every dispatch-served mutation is authoritative
        // (commit-before-ack), so `AddNode` through `dispatch` REQUIRES a
        // persistence backend — a backendless fixture rejects the write
        // ("authoritative MutationBatch commit requires a persistence
        // backend") before the cache paths under test are ever reached.
        static DIR_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir()
            .join(format!(
                "eg-result-cache-dispatch-{}-{}",
                std::process::id(),
                DIR_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
            .to_string_lossy()
            .into_owned();
        std::fs::create_dir_all(&dir).expect("create test persist dir");
        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> = Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(
                dir.clone(),
                crate::durability::DurabilityPolicy::Each,
                4096,
            )
            .expect("open test redb backend"),
        );
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir),
            persistence: Some(backend),
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
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
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        current_request(SECRET, id, "__commons__", method)
    }

    /// Keep the full (`--features full`) dispatcher's state machine behind one heap
    /// indirection — mirrors `src/cost.rs`'s `dispatch_on_heap`. `dispatch()` bottoms
    /// out in `dispatch_inner` (`src/server/dispatch.rs`), a single ~8k-line async fn
    /// whose generated `Future` is sized to the UNION of every feature-gated
    /// `Method` match arm; under `full` every arm is compiled in, so that future is
    /// large. Awaiting it INLINE (never boxed) embeds the whole thing in the
    /// caller's own generated state machine, and nesting a couple of calls deep (a
    /// helper awaiting `dispatch` inside a test awaiting the helper) can exhaust the
    /// test harness thread's stack before the first request is even polled — this is
    /// exactly what crashed `hit_on_unchanged_then_write_invalidates` with a stack
    /// overflow (SIGABRT) in CI. Route every call in this module through here.
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    async fn add_node(state: &Arc<RwLock<ServerState>>, id: u64, node: &str, label: &str) {
        let props = serde_json::json!({ "node_type": label });
        let bytes = rmp_serde::to_vec_named(&props).unwrap();
        let r = dispatch_on_heap(
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
        let r1 = dispatch_on_heap(
            &state,
            req(
                10,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(r1.error.is_none());
        let bytes1 = raw(&r1);
        let (h1, m1) = cache_stats(&core);
        assert_eq!((h1 - h0, m1 - m0), (0, 1), "first query is a miss");

        // Second identical query on the UNCHANGED graph: HIT, identical bytes, no
        // recompute (the hit counter moved, the miss counter did not).
        let r2 = dispatch_on_heap(
            &state,
            req(
                11,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert_eq!(raw(&r2), bytes1, "cached bytes identical to computed");
        let (h2, m2) = cache_stats(&core);
        assert_eq!((h2 - h1, m2 - m1), (1, 0), "second query hit the cache");

        // A WRITE bumps version → the cached entry is now unreachable.
        let v_before = core.version();
        add_node(&state, 3, "p3", "Person").await;
        assert_ne!(core.version(), v_before, "write must bump version");

        // Same query again: MISS (recompute), and the result is CORRECT (now 3 rows).
        let r3 = dispatch_on_heap(
            &state,
            req(
                12,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
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
        let r4 = dispatch_on_heap(
            &state,
            req(
                13,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
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
        let _ = dispatch_on_heap(
            &b,
            req(
                20,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        let r_hit = dispatch_on_heap(
            &b,
            req(
                21,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
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
        let r_after = dispatch_on_heap(
            &b,
            req(
                22,
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        let (h3, m3) = cache_stats(&core_b);
        assert_eq!(
            (h3 - h2, m3 - m2),
            (0, 1),
            "after CDC invalidation B recomputes (miss), not a stale hit"
        );
        // The recompute SUCCEEDS over B's own (unreplicated) data and is non-empty —
        // proving the post-invalidation read recomputes a valid result rather than
        // erroring or serving a stale hit. (Cypher row ORDER isn't stable, so we don't
        // byte-compare to the pre-invalidation bytes; the load-bearing proof is the
        // miss counter above — invalidation fired.)
        assert!(r_after.error.is_none());
        assert!(!raw(&r_after).is_empty());
        assert!(!bytes_b_old.is_empty());
    }
}

// ── ⚠ RLS × result-cache: NO cross-agent leak (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231) ──
//
// THE headline reconciliation proof. With RLS ACTIVE, agent A and agent B run the
// SAME query text against the SAME graph and the SAME version, yet:
//   * A's cached (A-filtered) result is NEVER served to B — B's identical query is a
//     cache MISS (the rls-aware key differs by agent_id) and recomputes B's OWN view;
//   * the bytes B receives do NOT contain A's private node — no exfiltration through
//     the cache;
//   * a SECOND A query IS a hit (A's key is stable), proving caching still works
//     per-agent (we didn't just disable the cache under RLS).
#[cfg(all(
    test,
    feature = "result-cache",
    feature = "cypher",
    feature = "security"
))]
mod rls_aware_cache_no_cross_agent_leak {
    use super::current_auth_test_support::{current_isolation_with_agents, current_request_as};
    use crate::channels::ChannelManager;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::dispatch;
    use crate::server::state::ServerState;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "rls-cache-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation_with_agents(&["alice", "bob"]),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
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
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    /// A request as `agent_id`.
    fn req_as(id: u64, agent_id: &str, method: Method) -> Request {
        current_request_as(SECRET, id, "__commons__", agent_id, method)
    }

    fn raw(resp: &Response) -> Vec<u8> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?}"),
        }
    }

    /// Add a node with RLS owner/visibility props (the `_owner`/`_visibility`
    /// convention `IsolationLayer::filter_view` reads).
    async fn add_rls_node(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        node: &str,
        label: &str,
        owner: &str,
        visibility: &str,
    ) {
        let props = serde_json::json!({
            "node_type": label,
            "_owner": owner,
            "_visibility": visibility,
        });
        let bytes = rmp_serde::to_vec_named(&props).unwrap();
        let r = dispatch(
            state,
            req_as(
                id,
                owner,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: bytes,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddNode failed: {:?}", r.error);
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

    // The SAME query text both agents run. It matches BOTH nodes by label, so RLS is
    // the only thing that differentiates their result sets.
    const Q: &str = "MATCH (n:Secret) RETURN n";

    #[tokio::test]
    async fn agent_a_cached_result_is_not_served_to_agent_b() {
        let state = state();
        // The fixture provisions two peer agents with no manager/grant between them.
        // A private :Secret node owned by alice (bob must NEVER see it), plus a public
        // :Secret node both can see — so neither agent's result is empty.
        add_rls_node(&state, 10, "alice_secret", "Secret", "alice", "private").await;
        add_rls_node(&state, 11, "shared", "Secret", "alice", "public").await;

        let core = core_of(&state).await;

        // ── Alice queries Q: cold MISS, computes + caches under alice's rls-key. Her
        //    filtered view sees BOTH nodes (she owns the private one + the public one).
        let (h0, m0) = core.result_cache().stats();
        let ra1 = dispatch(
            &state,
            req_as(
                20,
                "alice",
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(ra1.error.is_none());
        let alice_bytes = raw(&ra1);
        let (h1, m1) = core.result_cache().stats();
        assert_eq!(
            (h1 - h0, m1 - m0),
            (0, 1),
            "alice's first query is a cold miss"
        );
        let alice_str = String::from_utf8_lossy(&alice_bytes);
        assert!(
            alice_str.contains("alice_secret"),
            "alice must see her own private node: {alice_str}"
        );

        // ── Bob runs the IDENTICAL query text on the UNCHANGED graph (same version).
        //    If the cache were NOT rls-aware, bob would HIT alice's entry and receive
        //    `alice_secret` — a cross-agent leak. With the rls-aware key it MISSES
        //    (different agent_id ⇒ different key) and recomputes BOB's filtered view.
        let rb = dispatch(
            &state,
            req_as(
                21,
                "bob",
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(rb.error.is_none());
        let bob_bytes = raw(&rb);
        let (h2, m2) = core.result_cache().stats();
        assert_eq!(
            (h2 - h1, m2 - m1),
            (0, 1),
            "bob's identical query MISSES the rls-aware cache (no cross-agent hit)"
        );
        // THE leak assertions: bob's bytes must NOT differ-by-leak — no alice_secret.
        let bob_str = String::from_utf8_lossy(&bob_bytes);
        assert!(
            !bob_str.contains("alice_secret"),
            "EXFILTRATION via cache: bob received alice's private node: {bob_str}"
        );
        assert!(
            bob_str.contains("shared"),
            "bob must still see the public node: {bob_str}"
        );
        assert_ne!(
            bob_bytes, alice_bytes,
            "bob's RLS-filtered result must differ from alice's (no shared cache slot)"
        );

        // ── Alice repeats Q: now it HITS her own entry (caching still works per-agent;
        //    we didn't simply disable the cache under RLS), and serves her bytes back.
        let ra2 = dispatch(
            &state,
            req_as(
                22,
                "alice",
                Method::CypherQuery {
                    query: Q.into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        let (h3, m3) = core.result_cache().stats();
        assert_eq!(
            (h3 - h2, m3 - m2),
            (1, 0),
            "alice's repeat query HITS her own rls-aware cache slot"
        );
        assert_eq!(raw(&ra2), alice_bytes, "alice's cached bytes are her own");
    }

    // The SAME GraphQL query both agents run. It selects every `:Secret` node's id, so
    // — exactly like the Cypher case — RLS is the ONLY thing differentiating the two
    // agents' result sets. Locks in reconciliation #1: a GraphQL read must go through
    // the SAME RLS-aware result-cache compose and never leak across agents.
    #[cfg(feature = "graphql")]
    const GQL: &str = "{ Secret { id } }";

    #[cfg(feature = "graphql")]
    #[tokio::test]
    async fn agent_a_graphql_cached_result_is_not_served_to_agent_b() {
        let state = state();
        add_rls_node(&state, 10, "alice_secret", "Secret", "alice", "private").await;
        add_rls_node(&state, 11, "shared", "Secret", "alice", "public").await;

        let core = core_of(&state).await;

        // Alice: cold MISS, cached under alice's rls-key; her view sees both nodes.
        let (h0, m0) = core.result_cache().stats();
        let ra1 = dispatch(
            &state,
            req_as(
                20,
                "alice",
                Method::GraphQl {
                    query: GQL.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(ra1.error.is_none(), "alice GraphQL failed: {:?}", ra1.error);
        let alice_bytes = raw(&ra1);
        let (h1, m1) = core.result_cache().stats();
        assert_eq!(
            (h1 - h0, m1 - m0),
            (0, 1),
            "alice's first GraphQL query is a cold miss"
        );
        let alice_str = String::from_utf8_lossy(&alice_bytes);
        assert!(
            alice_str.contains("alice_secret"),
            "alice must see her own private node via GraphQL: {alice_str}"
        );

        // Bob: IDENTICAL GraphQL text on the UNCHANGED graph. An rls-UNAWARE cache would
        // serve him alice's entry (leak). The rls-aware key MISSES (different agent_id)
        // and recomputes BOB's filtered view.
        let rb = dispatch(
            &state,
            req_as(
                21,
                "bob",
                Method::GraphQl {
                    query: GQL.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(rb.error.is_none(), "bob GraphQL failed: {:?}", rb.error);
        let bob_bytes = raw(&rb);
        let (h2, m2) = core.result_cache().stats();
        assert_eq!(
            (h2 - h1, m2 - m1),
            (0, 1),
            "bob's identical GraphQL query MISSES the rls-aware cache (no cross-agent hit)"
        );
        let bob_str = String::from_utf8_lossy(&bob_bytes);
        assert!(
            !bob_str.contains("alice_secret"),
            "EXFILTRATION via GraphQL cache: bob received alice's private node: {bob_str}"
        );
        assert!(
            bob_str.contains("shared"),
            "bob must still see the public node via GraphQL: {bob_str}"
        );
        assert_ne!(
            bob_bytes, alice_bytes,
            "bob's RLS-filtered GraphQL result must differ from alice's (no shared slot)"
        );

        // Alice repeats: HITS her own per-agent slot (caching still works under RLS).
        let ra2 = dispatch(
            &state,
            req_as(
                22,
                "alice",
                Method::GraphQl {
                    query: GQL.into(),
                    variables: None,
                },
            ),
        )
        .await;
        let (h3, m3) = core.result_cache().stats();
        assert_eq!(
            (h3 - h2, m3 - m2),
            (1, 0),
            "alice's repeat GraphQL query HITS her own rls-aware cache slot"
        );
        assert_eq!(
            raw(&ra2),
            alice_bytes,
            "alice's cached GraphQL bytes are her own"
        );
    }
}

// ── Server-dispatch WRITE wiring (CONCEPT:EG-KG.query.mirrors-pgwire) ─────────────────────────────────
//
// Proves the five EG-023 wirings land THROUGH the real `dispatch()` entrypoint a wire
// request hits (auth → routing → handler → write): a GraphQL mutation creates a node a
// later query sees; a Cypher CREATE is then visible to a MATCH; a wire `Sql`
// CREATE TABLE + INSERT + SELECT round-trips; an `INSERT INTO nodes` is visible to a
// SELECT; and the read paths still work.
#[cfg(all(test, feature = "query", feature = "cypher", feature = "graphql"))]
mod dispatch_write_tests {
    use super::current_auth_test_support::{current_isolation, current_request};
    use crate::channels::ChannelManager;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::dispatch;
    use crate::server::state::ServerState;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "dispatch-write-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(
                crate::server::sql_tables::test_persist_dir()
                    .to_string_lossy()
                    .into_owned(),
            ),
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
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
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        current_request(SECRET, id, "__commons__", method)
    }

    fn raw(resp: &Response) -> Vec<u8> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?} / err {:?}", resp.error),
        }
    }

    /// Decode a `Raw(QueryResult)` write/read response into `(columns, rows)` where each
    /// row is the decoded `Vec<serde_json::Value>` cell list.
    fn query_result(resp: &Response) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let qr: crate::protocol::QueryResult =
            rmp_serde::from_slice(&raw(resp)).expect("QueryResult");
        let rows = qr
            .rows
            .iter()
            .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).expect("row cells"))
            .collect();
        (qr.columns, rows)
    }

    /// THE GraphQL write→read proof (CONCEPT:EG-KG.query.mutation/EG-023): a `mutation { createNode … }`
    /// dispatched over the wire creates a node a subsequent GraphQL query SEES.
    #[tokio::test]
    async fn graphql_mutation_creates_node_via_dispatch() {
        let state = state();
        let m = dispatch(
            &state,
            req(
                1,
                Method::GraphQl {
                    query: r#"mutation { createNode(label: "Person", id: "dave", props: {name: "Dave", age: 50}) { id name } }"#.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(m.error.is_none(), "mutation failed: {:?}", m.error);
        let v: serde_json::Value = rmp_serde::from_slice(&raw(&m)).unwrap();
        assert_eq!(v["data"]["createNode"]["id"], serde_json::json!("dave"));

        // A fresh GraphQL query over the post-write graph sees Dave.
        let q = dispatch(
            &state,
            req(
                2,
                Method::GraphQl {
                    query: r#"{ Person(name: "Dave") { name age } }"#.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert!(q.error.is_none(), "query failed: {:?}", q.error);
        let qv: serde_json::Value = rmp_serde::from_slice(&raw(&q)).unwrap();
        let people = qv["data"]["Person"].as_array().unwrap();
        assert_eq!(people.len(), 1);
        assert_eq!(people[0]["name"], serde_json::json!("Dave"));
        assert_eq!(people[0]["age"], serde_json::json!(50));
    }

    /// THE Cypher write→read proof (CONCEPT:EG-KG.query.register-each-user-table/EG-023): a `CREATE` dispatched over the
    /// wire is then visible to a `MATCH` (which still runs the read path).
    #[tokio::test]
    async fn cypher_create_then_match_via_dispatch() {
        let state = state();
        let c = dispatch(
            &state,
            req(
                1,
                Method::CypherQuery {
                    query: "CREATE (n:Widget {name: 'gizmo', qty: 7})".into(),
                    mode: crate::protocol::CypherMode::Write,
                },
            ),
        )
        .await;
        assert!(c.error.is_none(), "cypher CREATE failed: {:?}", c.error);

        let r = dispatch(
            &state,
            req(
                2,
                Method::CypherQuery {
                    query: "MATCH (n:Widget) RETURN n.name".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "cypher MATCH failed: {:?}", r.error);
        let (_cols, rows) = query_result(&r);
        let names: Vec<String> = rows
            .iter()
            .map(|cells| cells[0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["gizmo"], "MATCH must see the CREATEd node");
    }

    #[tokio::test]
    async fn cypher_declared_mode_must_match_native_parser() {
        let state = state();
        let disguised_write = dispatch(
            &state,
            req(
                1,
                Method::CypherQuery {
                    query: "CREATE (n:Forbidden {name: 'write-through-read'})".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(
            disguised_write
                .error
                .as_deref()
                .is_some_and(|message| message.contains("declared mode")),
            "write declared as read must fail closed: {:?}",
            disguised_write.error
        );

        let mislabeled_read = dispatch(
            &state,
            req(
                2,
                Method::CypherQuery {
                    query: "MATCH (n) RETURN n".into(),
                    mode: crate::protocol::CypherMode::Write,
                },
            ),
        )
        .await;
        assert!(
            mislabeled_read
                .error
                .as_deref()
                .is_some_and(|message| message.contains("declared mode")),
            "read declared as write must fail closed: {:?}",
            mislabeled_read.error
        );
    }

    /// THE wire-SQL DDL/DML round-trip (CONCEPT:EG-KG.query.mirrors-pgwire): `CREATE TABLE` + `INSERT` + a
    /// `SELECT` that reads the user table back, all over `Method::Sql` through dispatch.
    #[tokio::test]
    async fn wire_sql_create_insert_select_round_trips() {
        let state = state();
        // Unique table name + DROP-IF-EXISTS so the process-global store is clean even on
        // a re-run against a persisted temp store.
        let table = format!("eg023_kv_{}", std::process::id());

        let sql = |q: String| Method::Sql {
            query: q,
            params_msgpack: Vec::new(),
        };

        let d = dispatch(&state, req(1, sql(format!("DROP TABLE IF EXISTS {table}")))).await;
        assert!(d.error.is_none(), "DROP failed: {:?}", d.error);

        let c = dispatch(
            &state,
            req(2, sql(format!("CREATE TABLE {table} (k TEXT, v BIGINT)"))),
        )
        .await;
        assert!(c.error.is_none(), "CREATE TABLE failed: {:?}", c.error);

        let i = dispatch(
            &state,
            req(
                3,
                sql(format!(
                    "INSERT INTO {table} (k, v) VALUES ('a', 1), ('b', 2)"
                )),
            ),
        )
        .await;
        assert!(i.error.is_none(), "INSERT failed: {:?}", i.error);

        let s = dispatch(
            &state,
            req(4, sql(format!("SELECT k, v FROM {table} ORDER BY k"))),
        )
        .await;
        assert!(s.error.is_none(), "SELECT failed: {:?}", s.error);
        let (cols, rows) = query_result(&s);
        assert_eq!(cols, vec!["k", "v"]);
        assert_eq!(
            rows.len(),
            2,
            "two rows round-tripped through the table store"
        );
        assert_eq!(rows[0][0], serde_json::json!("a"));
        assert_eq!(rows[0][1], serde_json::json!(1));
        assert_eq!(rows[1][0], serde_json::json!("b"));
        assert_eq!(rows[1][1], serde_json::json!(2));

        // cleanup
        let _ = dispatch(&state, req(5, sql(format!("DROP TABLE IF EXISTS {table}")))).await;
    }

    /// `INSERT INTO nodes` over the wire lands in the graph core and a `SELECT` sees it —
    /// the agent-utilities `graph_table`/`sql_exec` node-write path (CONCEPT:EG-KG.query.mirrors-pgwire).
    #[tokio::test]
    async fn wire_sql_insert_node_then_select_via_dispatch() {
        let state = state();
        let sql = |q: &str| Method::Sql {
            query: q.into(),
            params_msgpack: Vec::new(),
        };

        let i = dispatch(
            &state,
            req(
                1,
                sql("INSERT INTO nodes (id, type, name) VALUES ('sqlnode', 'Gadget', 'Zed')"),
            ),
        )
        .await;
        assert!(i.error.is_none(), "INSERT INTO nodes failed: {:?}", i.error);

        // SELECT over the graph projection sees the new node.
        let s = dispatch(
            &state,
            req(2, sql("SELECT id FROM nodes WHERE id = 'sqlnode'")),
        )
        .await;
        assert!(s.error.is_none(), "SELECT failed: {:?}", s.error);
        let (_c, rows) = query_result(&s);
        assert_eq!(
            rows.len(),
            1,
            "the SQL-inserted node is visible to a SELECT"
        );
        assert_eq!(rows[0][0], serde_json::json!("sqlnode"));

        // And a Cypher read sees it too (cross-surface).
        let cy = dispatch(
            &state,
            req(
                3,
                Method::CypherQuery {
                    query: "MATCH (n:Gadget) RETURN n.name".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert!(cy.error.is_none(), "cypher read failed: {:?}", cy.error);
        let (_c2, rows2) = query_result(&cy);
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][0], serde_json::json!("Zed"));
    }
}

// ── In-transaction cross-modal read-your-own-writes (CONCEPT:EG-KG.query.txn-cross-modal-ryow) ──────────
// End-to-end dispatch tests for the `TxnUnifiedQuery{,Text}` overlay path: a txn's
// STAGED (uncommitted) node + embedding + edge are visible to a unified cross-modal
// query issued INSIDE that txn (RYOW), while an identical OFF-txn query sees nothing
// until COMMIT. Drives the real `dispatch` shell, so it exercises begin → stage →
// overlaid query → commit exactly as a client would.
#[cfg(all(test, feature = "query"))]
mod txn_ryow_dispatch_tests {
    use super::current_auth_test_support::{current_isolation, current_request};
    use crate::channels::ChannelManager;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::dispatch;
    use crate::server::state::ServerState;
    use dashmap::DashMap;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "txn-ryow-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
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
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        current_request(SECRET, id, "__commons__", method)
    }

    fn pack(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Decode a unified-query response into its result node ids.
    fn unified_ids(resp: &Response) -> Vec<String> {
        assert!(
            resp.error.is_none(),
            "unified query error: {:?}",
            resp.error
        );
        let bytes = match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?}"),
        };
        let rows: Vec<(String, Option<f32>)> = rmp_serde::from_slice(&bytes).unwrap();
        rows.into_iter().map(|(id, _)| id).collect()
    }

    async fn begin(state: &Arc<RwLock<ServerState>>, id: u64) -> String {
        let r = dispatch(
            state,
            req(
                id,
                Method::BeginTxn {
                    graph: None,
                    isolation: None,
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("BeginTxn failed: {:?} / {other:?}", r.error),
        }
    }

    async fn ok(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) {
        let r = dispatch(state, req(id, method)).await;
        assert!(r.error.is_none(), "stage op {id} failed: {:?}", r.error);
    }

    // Cross-modal RYOW: staged node + embedding rank in-txn; staged edge is BFS-
    // reachable in-txn; an identical OFF-txn query sees none of it.
    #[tokio::test]
    async fn in_txn_cross_modal_ryow() {
        let state = state();
        let txn = begin(&state, 1).await;
        // Stage: node `sn` (Widget) + its embedding, node `tn` (Gadget), edge sn→tn.
        ok(
            &state,
            2,
            Method::TxnAddNode {
                txn_id: txn.clone(),
                node_id: "sn".into(),
                properties_msgpack: pack(json!({"type": "Widget"})),
                graph: None,
            },
        )
        .await;
        ok(
            &state,
            3,
            Method::TxnAddEmbedding {
                txn_id: txn.clone(),
                node_id: "sn".into(),
                embedding: vec![1.0, 0.0],
                graph: None,
            },
        )
        .await;
        ok(
            &state,
            4,
            Method::TxnAddNode {
                txn_id: txn.clone(),
                node_id: "tn".into(),
                properties_msgpack: pack(json!({"type": "Gadget"})),
                graph: None,
            },
        )
        .await;
        ok(
            &state,
            5,
            Method::TxnAddEdge {
                txn_id: txn.clone(),
                source_id: "sn".into(),
                target_id: "tn".into(),
                properties_msgpack: pack(json!({"relationship": "LINKS"})),
                graph: None,
            },
        )
        .await;

        // IN-TXN cross-modal (graph label Scan fused with staged-vector Rank): sees sn.
        let vec_q = "MATCH (:Widget) |> RANK BY ~[1.0,0.0] |> LIMIT 5";
        let in_txn = dispatch(
            &state,
            req(
                6,
                Method::TxnUnifiedQueryText {
                    txn_id: txn.clone(),
                    text: vec_q.into(),
                },
            ),
        )
        .await;
        assert_eq!(
            unified_ids(&in_txn),
            vec!["sn".to_string()],
            "staged node + embedding must be visible to the in-txn cross-modal query"
        );

        // IN-TXN traverse over the STAGED edge: reaches tn.
        let trav_q = "MATCH (:Widget) |> TRAVERSE -[:LINKS]->{1,1} |> LIMIT 5";
        let trav = dispatch(
            &state,
            req(
                7,
                Method::TxnUnifiedQueryText {
                    txn_id: txn.clone(),
                    text: trav_q.into(),
                },
            ),
        )
        .await;
        assert!(
            unified_ids(&trav).contains(&"tn".to_string()),
            "staged edge must make tn BFS-reachable in-txn"
        );

        // OFF-TXN identical query: empty — staged writes are invisible before commit.
        let off = dispatch(
            &state,
            req(8, Method::UnifiedQueryText { text: vec_q.into() }),
        )
        .await;
        assert!(
            unified_ids(&off).is_empty(),
            "off-txn query must see none of the txn's uncommitted writes"
        );
    }

    // The "until COMMIT" half: a graph-only txn (no vectors → commits in-memory with
    // no persistence backend) is invisible off-txn before commit, visible after.
    #[tokio::test]
    async fn commit_makes_txn_writes_visible_off_txn() {
        let state = state();
        let txn = begin(&state, 1).await;
        ok(
            &state,
            2,
            Method::TxnAddNode {
                txn_id: txn.clone(),
                node_id: "cn".into(),
                properties_msgpack: pack(json!({"type": "Committed"})),
                graph: None,
            },
        )
        .await;
        let q = "MATCH (:Committed) |> LIMIT 5";

        // Before commit: off-txn empty, in-txn sees it (RYOW).
        let before = dispatch(&state, req(3, Method::UnifiedQueryText { text: q.into() })).await;
        assert!(
            unified_ids(&before).is_empty(),
            "off-txn empty before commit"
        );
        let in_txn = dispatch(
            &state,
            req(
                4,
                Method::TxnUnifiedQueryText {
                    txn_id: txn.clone(),
                    text: q.into(),
                },
            ),
        )
        .await;
        assert_eq!(unified_ids(&in_txn), vec!["cn".to_string()], "RYOW in-txn");

        // Commit, then the same OFF-txn query now sees the committed node.
        let c = dispatch(
            &state,
            req(
                5,
                Method::Commit {
                    txn_id: txn.clone(),
                },
            ),
        )
        .await;
        assert!(
            matches!(c.result, Some(ResultPayload::Bool(true))),
            "commit must succeed: {:?}",
            c.error
        );
        let after = dispatch(&state, req(6, Method::UnifiedQueryText { text: q.into() })).await;
        assert_eq!(
            unified_ids(&after),
            vec!["cn".to_string()],
            "committed node must be visible off-txn after commit"
        );
    }
}

// ── SURPASS gap-closure: "unify the two evidence resolvers" ──────────────────────
// `explain_evidence_wire`'s `alignment`-gated variant must ACTUALLY call
// `CasEvidenceResolver` and attach the resolved content to each citation — before
// this, `CasEvidenceResolver` had zero served-RPC call sites (only its own unit
// tests in `src/server/blob/cas_resolver.rs` exercised it). These tests drive
// `explain_evidence_wire` directly (the same function `Method::ExplainEvidence`'s
// handler arm calls), over a real `RedbChunkStore`, so a regression that silently
// drops `resolved` back to always-`None` fails here, not just in the resolver's own
// isolated unit tests.
#[cfg(all(test, feature = "evidence-graph", feature = "alignment"))]
mod evidence_resolver_wiring_tests {
    use super::explain_evidence_wire;
    use crate::graph::GraphCore;
    use crate::server::blob::stream::stream_blob_put;
    use crate::server::blob::{ChunkStore, RedbChunkStore};
    use serde_json::json;
    use std::sync::Arc;

    fn node_blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn edge_blob(relationship: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&json!({ "relationship": relationship })).unwrap()
    }

    const SUBJECT: &str = "eg:artifact:0000000000000002";

    fn locus(address: serde_json::Value) -> serde_json::Value {
        json!({
            "id": "eg:locus:0000000000000001",
            "subject": { "kind": "artifact", "id": SUBJECT },
            "address": address,
            "policy_ref": "eg:policy:0000000000000003",
            "derivation_ref": "eg:derivation:0000000000000004"
        })
    }

    /// A `CharacterRange` citation resolves to the REAL text excerpt read back out of
    /// the blob CAS -- `explain_evidence_wire` must thread the configured blob store
    /// all the way through to `EvidenceCitationWire::resolved`, not just return the
    /// locus metadata `evidence_citation_wire`'s `alignment`-less twin would.
    #[tokio::test]
    async fn explain_evidence_wire_attaches_a_real_resolved_excerpt() {
        let cas: Arc<dyn ChunkStore> = Arc::new(RedbChunkStore::open_temp().unwrap());
        let committed = stream_blob_put(cas.as_ref(), "hello world".as_bytes(), 0).unwrap();

        let core = GraphCore::new();
        core.add_node(
            SUBJECT.into(),
            node_blob(json!({ "node_type": "Document", "blob_ref": committed.digest })),
        );
        core.add_node(
            "claim1".into(),
            node_blob(json!({ "type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            node_blob(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "evidence_locus": locus(json!({
                    "kind": "character_range", "start": 0, "end": 5
                })),
            })),
        );
        core.add_edge("evidence1".into(), "claim1".into(), edge_blob("SUPPORTS"))
            .unwrap();

        let view = core.analysis_snapshot();
        let result = explain_evidence_wire("claim1", &view, Some(cas));

        assert_eq!(result.citations.len(), 1);
        let citation = &result.citations[0];
        assert_eq!(citation.evidence_id, "evidence1");
        let resolved = citation
            .resolved
            .as_ref()
            .expect("character-range citation must resolve through the configured blob store");
        assert_eq!(resolved.kind, "text");
        assert_eq!(resolved.subject_ref, SUBJECT);
        assert_eq!(resolved.excerpt.as_deref(), Some("hello"));
    }

    /// No blob store configured (`None`, e.g. an in-memory/no-persist-dir deployment)
    /// -- `resolved` stays `None` on every citation, exactly the pre-existing
    /// locus-only behavior. Never an error, never a fabricated resolution.
    #[tokio::test]
    async fn explain_evidence_wire_leaves_resolved_none_without_a_blob_store() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            node_blob(json!({ "type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            node_blob(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "evidence_locus": locus(json!({
                    "kind": "character_range", "start": 0, "end": 5
                })),
            })),
        );
        core.add_edge("evidence1".into(), "claim1".into(), edge_blob("SUPPORTS"))
            .unwrap();

        let view = core.analysis_snapshot();
        let result = explain_evidence_wire("claim1", &view, None);

        assert_eq!(result.citations.len(), 1);
        assert_eq!(result.citations[0].resolved, None);
    }

    /// A `CodeSymbol` citation resolves to the REAL line-range excerpt -- proving
    /// the wiring covers the newly-added `CodeSymbol` codec path too, not just
    /// `CharacterRange`.
    #[tokio::test]
    async fn explain_evidence_wire_attaches_a_real_code_symbol_excerpt() {
        let cas: Arc<dyn ChunkStore> = Arc::new(RedbChunkStore::open_temp().unwrap());
        let source = "fn a() {}\nfn b() {\n    1 + 1\n}\n";
        let committed = stream_blob_put(cas.as_ref(), source.as_bytes(), 0).unwrap();

        let core = GraphCore::new();
        core.add_node(
            SUBJECT.into(),
            node_blob(json!({ "node_type": "Code", "blob_ref": committed.digest })),
        );
        core.add_node(
            "claim1".into(),
            node_blob(json!({ "type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            node_blob(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "evidence_locus": locus(json!({
                        "kind": "code_symbol",
                        "revision_ref": "eg:revision:0000000000000005",
                        "symbol_ref": "eg:symbol:0000000000000006",
                        "start_line": 1,
                        "end_line": 4
                })),
            })),
        );
        core.add_edge("evidence1".into(), "claim1".into(), edge_blob("SUPPORTS"))
            .unwrap();

        let view = core.analysis_snapshot();
        let result = explain_evidence_wire("claim1", &view, Some(cas));

        let resolved = result.citations[0]
            .resolved
            .as_ref()
            .expect("CodeSymbol citation must resolve");
        assert_eq!(resolved.kind, "text");
        assert_eq!(resolved.excerpt.as_deref(), Some("fn b() {\n    1 + 1\n}"));
    }
}
