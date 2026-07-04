//! Read-only query handler. Owns BOTH query methods (one module per domain, per
//! the dispatch conventions — `Sql` + `CypherQuery` are the one `// ── Query ──`
//! protocol section):
//!   * `Method::Sql` (CONCEPT:EG-KG.query.read-only-sql-query, feature `query`) — `SELECT … FROM nodes …`
//!     over ONE graph via DataFusion (eg-query::exec_sql).
//!   * `Method::CypherQuery` (CONCEPT:EG-KG.query.dep-free-behind, feature `cypher`) — `MATCH … RETURN
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

use tokio::sync::RwLock;

use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::graph::GraphCore;
use crate::protocol::Method;
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
use crate::protocol::{Response, ResultPayload};
#[cfg(feature = "result-cache")]
use eg_core::result_cache::ResultCache;

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

/// Handle `Method::Sql` / `Method::CypherQuery`. `Err(method)` hands a non-query
/// method (or a query method whose feature is off) back to the dispatcher
/// (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention — server dispatch convention)
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    // `state` is consumed only by the `query`-gated in-txn cross-modal RYOW arms
    // (CONCEPT:EG-KG.query.txn-cross-modal-ryow — TxnUnifiedQuery{,Text}); keep it referenced in a
    // cypher/graphql-only build (no `query`) so no dead-param warning fires.
    #[cfg(not(feature = "query"))]
    let _ = state;
    // `graph_name` is consumed only by the `graphql`-gated cross-modal durable commit
    // (CONCEPT:EG-KG.query.facade-reconcile-hook); keep it referenced in a query/cypher-only build so no dead-param
    // warning fires.
    #[cfg(not(feature = "graphql"))]
    let _ = graph_name;
    match method {
        #[cfg(feature = "query")]
        Method::Sql { query, .. } => {
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
            // The shared process-wide table store is resolved once here (cheap clone of
            // the singleton). The read path is NOT version-keyed cached (a user-table
            // write does not bump the graph `version()`, so caching it would risk
            // staleness); the graph-only Cypher/SPARQL/GraphQL reads keep their caches.
            let store = match crate::server::sql_tables::user_table_store() {
                Ok(s) => s,
                Err(e) => return Ok(Response::err(req_id, format!("SQL error: {e}"))),
            };
            match eg_query::classify(&query) {
                Ok(kind) if !matches!(kind, eg_query::StatementKind::Read) => {
                    Ok(exec_sql_write(req_id, &core, &store, kind).await)
                }
                _ => {
                    // Read (or an unparseable statement — exec surfaces the precise
                    // parse error). RLS-filter the off-lock snapshot to the caller's
                    // visible rows BEFORE execution so a SELECT cannot exfiltrate a
                    // forbidden row.
                    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
                    let mut snap = core.analysis_snapshot();
                    #[cfg(feature = "security")]
                    rls.filter_view(caller.unwrap_or(""), &mut snap);
                    let resp = match compute_off_lock(req_id, move || {
                        eg_query::exec_sql_typed_with_tables(&snap, &store, &query)
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
                    Ok(resp)
                }
            }
        }
        #[cfg(feature = "query")]
        Method::UnifiedQuery {
            plan,
            reorder_filter_selectivity,
        } => {
            // ONE cross-modal plan (CONCEPT:AU-KG.compute.vector/209): filter (DataFusion) →
            // traverse (BFS) → rank (kNN) over ONE consistent off-lock snapshot. Take
            // BOTH the GraphView (topology + property blobs) and a SemanticStore clone
            // under a brief read each — same point-in-time, so the cross-modal read is
            // snapshot-isolated — then run the whole pipeline on the blocking pool.
            // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): key
            // on the plan bytes + the reorder flag + the caller's RLS context. The plan
            // + semantic store both reflect `version`, so a write retires the entry; the
            // RLS-context salt keeps agent A's fused result out of agent B's lookups.
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let mut payload = rmp_serde::to_vec_named(&plan).unwrap_or_default();
                payload.extend(reorder_filter_selectivity.unwrap_or(f64::NAN).to_le_bytes());
                let hash = rls_cache_hash(
                    "unified",
                    &payload,
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
                rls.filter_view(caller.unwrap_or(""), &mut snap);
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
            let semantic = core.semantic_store.read().clone();
            // RECONCILE (CONCEPT:EG-KG.query.native-time-series): committed tsdb store for `Op::TsScan` fusion.
            #[cfg(feature = "tsdb")]
            let tsdb = state.read().await.tsdb_store.clone();
            let resp = match compute_off_lock(req_id, move || {
                run_unified(
                    plan,
                    reorder_filter_selectivity,
                    &snap,
                    &semantic,
                    #[cfg(feature = "tsdb")]
                    tsdb.as_deref(),
                    // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                    #[cfg(feature = "tsdb")]
                    None,
                )
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
            // UQL (CONCEPT:AU-KG.query.top-nodes-by-degree): parse the TEXT query into the SAME `wire::Plan`
            // `UnifiedQuery` carries, then run the IDENTICAL `run_unified` executor —
            // a pure front-end, no new execution path. A parse error is a clear,
            // caret-annotated error Response (never a panic).
            // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): key
            // on the TEXT + reorder flag + the caller's RLS context (the parse is
            // deterministic, so caching pre-parse is sound and skips the parse on a hit
            // too). The RLS-context salt keeps agent A's result out of agent B's lookups.
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let mut payload = text.clone().into_bytes();
                payload.extend(reorder_filter_selectivity.unwrap_or(f64::NAN).to_le_bytes());
                let hash = rls_cache_hash(
                    "unified-text",
                    &payload,
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
                rls.filter_view(caller.unwrap_or(""), &mut snap);
                (snap, version, hash)
            };
            let plan = match eg_plan::uql::parse(&text) {
                Ok(plan) => plan,
                Err(e) => return Ok(Response::err(req_id, e.render(&text))),
            };
            #[cfg(not(feature = "result-cache"))]
            let snap = rls_snapshot(
                &core,
                #[cfg(feature = "security")]
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let semantic = core.semantic_store.read().clone();
            // RECONCILE (CONCEPT:EG-KG.query.native-time-series): committed tsdb store for `Op::TsScan` fusion.
            #[cfg(feature = "tsdb")]
            let tsdb = state.read().await.tsdb_store.clone();
            let resp = match compute_off_lock(req_id, move || {
                run_unified(
                    plan,
                    reorder_filter_selectivity,
                    &snap,
                    &semantic,
                    #[cfg(feature = "tsdb")]
                    tsdb.as_deref(),
                    // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                    #[cfg(feature = "tsdb")]
                    None,
                )
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
        // ── In-transaction cross-modal read-your-own-writes (CONCEPT:EG-KG.query.txn-cross-modal-ryow) ──
        // Run the SAME unified cross-modal plan as `UnifiedQuery`, but over a
        // snapshot OVERLAID with the open txn's staged (uncommitted) write-set +
        // staged embeddings, so a node/edge/vector the txn itself staged is visible
        // to THIS query before commit and invisible off-txn until commit. Reuses the
        // EG-049 overlay generalized cross-modal (graph + semantic). No result cache
        // on this path (staged writes don't bump `version()`), exactly like the
        // committed SQL read path. RLS applies to the committed base snapshot.
        #[cfg(feature = "query")]
        Method::TxnUnifiedQuery {
            txn_id,
            plan,
            reorder_filter_selectivity,
        } => Ok(run_unified_overlaid(
            state,
            req_id,
            &txn_id,
            plan,
            reorder_filter_selectivity,
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            rls,
        )
        .await),
        #[cfg(feature = "query")]
        Method::TxnUnifiedQueryText {
            txn_id,
            text,
            reorder_filter_selectivity,
        } => {
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
                reorder_filter_selectivity,
                #[cfg(feature = "security")]
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
            // RLS-filtered off-lock snapshot, exactly like the Sql/UnifiedQueryText reads.
            // NOT result-cached: an LLM plan is non-deterministic, so keying a cache on the
            // NL text would risk serving a stale/foreign result.
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let mut snap = core.analysis_snapshot();
            #[cfg(feature = "security")]
            rls.filter_view(caller.unwrap_or(""), &mut snap);
            let semantic = core.semantic_store.read().clone();
            // RECONCILE (CONCEPT:EG-KG.query.native-time-series): committed tsdb store for `Op::TsScan` fusion.
            #[cfg(feature = "tsdb")]
            let tsdb = state.read().await.tsdb_store.clone();
            let resp = match compute_off_lock(req_id, move || {
                run_unified(
                    plan,
                    None,
                    &snap,
                    &semantic,
                    #[cfg(feature = "tsdb")]
                    tsdb.as_deref(),
                    // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                    #[cfg(feature = "tsdb")]
                    None,
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
                            graph_name,
                            &core,
                            graphql_crossmodal_registry(),
                            &txn_id,
                            #[cfg(feature = "security")]
                            caller,
                            #[cfg(not(feature = "security"))]
                            None,
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
                        let core_w = core.clone();
                        let reg = graphql_crossmodal_registry();
                        let resp = match compute_off_lock(req_id, move || {
                            eg_graphql::execute_crossmodal(&core_w, reg, &query)
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
                rls.filter_view(caller.unwrap_or(""), &mut snap);
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
        Method::CypherQuery { query } => {
            // Cypher WRITE surface (CONCEPT:EG-KG.query.register-each-user-table/EG-023): a `CREATE`/`MERGE`/`SET`/
            // `DELETE`/`REMOVE` statement is applied to the LIVE `GraphCore` via
            // `exec_cypher_write` (native eg-core write ops — NO DataFusion; it calls
            // `mark_dirty` once after the mutation). NOT cached, NOT RLS pre-filtered
            // (writes are graph-ACL-gated upstream — this method classified Write). A
            // read falls through to the RLS-aware cached snapshot path below.
            if super::super::access::cypher_is_write(&query) {
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
                rls.filter_view(caller.unwrap_or(""), &mut snap);
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
        let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
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

/// Execute a unified cross-modal plan (CONCEPT:AU-KG.compute.vector/209) over one off-lock
/// snapshot and return the result rows as `[id, score|nil]`. The plan is routed through the
/// full cost optimizer by `eg_plan::execute` (CONCEPT:EG-KG.query.served-plan-optimize-routing); a
/// lexical `Op::RankText`/`Op::FuseRrf` leg is served over a snapshot-derived BM25 index
/// (CONCEPT:EG-KG.query.served-text-index-binding). Synchronous — runs on the blocking pool via
/// `compute_off_lock`, like the SQL/Cypher legs.
#[cfg(feature = "query")]
pub(crate) fn run_unified(
    plan: eg_plan::Plan,
    reorder_filter_selectivity: Option<f64>,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
    // RECONCILE (Lane C tsdb-in-plan, CONCEPT:EG-KG.query.native-time-series): the committed native tsdb
    // `SeriesStore` backing `Op::TsScan`, threaded in so a UQL plan fuses its
    // time-series leg with the graph/vector/relational legs. `None` ⇒ a `TsScan`
    // yields no rows (degrade, never err). Only exists under the `tsdb` feature.
    #[cfg(feature = "tsdb")] tsdb: Option<&eg_tsdb::store::SeriesStore>,
    // In-txn tsdb read-your-own-writes (CONCEPT:EG-KG.query.txn-tsdb-read-your): the resolved txn's OWN staged,
    // uncommitted series points, overlaid onto `Op::TsScan` BEFORE the committed store so
    // an in-txn UQL reads its own measurements. `None` off-txn ⇒ committed series only.
    #[cfg(feature = "tsdb")] staged_series: Option<&eg_plan::StagedSeries>,
) -> Result<Vec<(String, Option<f32>)>, String> {
    use eg_plan::PlanCtx;

    // CONCEPT:EG-KG.query.served-plan-optimize-routing — No-Legacy migration (handoff-1). The
    // served path NO LONGER runs the bespoke single `CostModel::reorder_filter_rank` swap.
    // Instead the plan is handed to `eg_plan::execute` UNCHANGED, and `execute` routes it
    // through the FULL cost optimizer (`plan_optimize` → `eg_plan::optimizer::optimize`) — so a
    // served `UnifiedQuery` now gets EVERY optimizer rule (filter/AsOf-before-Rank, Reason↔Rank
    // reorder, FuseRrf branch reorder), not just the one legacy reorder. The optimizer folds
    // the legacy `reorder_filter_rank` in as its `FilterAsOfBeforeRank` rule
    // (CONCEPT:EG-KG.query.filter-pushdown-rule), driving the decision off the SAME snapshot-derived
    // cardinality/`CostModel::order` primitives, and every rule is answer-preserving within the
    // EG-405 non-empty guard (proven by `tests/differential_oracle.rs` + the plan snapshots), so
    // served-plan RESULTS are unchanged on the covered cases. The runtime kill-switch
    // `EPISTEMIC_GRAPH_COST_OPT=0` makes `plan_optimize` an identity passthrough (the reorder is
    // pure performance, never correctness). `reorder_filter_selectivity` is now a legacy no-op
    // HINT the optimizer's own derived selectivity supersedes — kept in the signature/wire DTO
    // for backward compatibility (a caller may still pass it; it no longer changes the plan).
    let _ = reorder_filter_selectivity;
    let ops = plan.ops;

    // CONCEPT:EG-KG.query.served-text-index-binding — bind a live BM25 `TextIndex` into the served
    // `PlanCtx` so a served `UnifiedQuery`/`UnifiedQueryText` whose plan carries `Op::RankText`
    // or an `Op::FuseRrf` text branch gets REAL lexical scores (it previously degraded to ZERO
    // lexical hits — the EG-KG.query.served-text-index-unbound-finding seam gap). The index is
    // derived from the SAME off-lock snapshot the rest of the plan reads (`view`), so the lexical
    // leg is snapshot-consistent with the vector/graph/temporal legs of the fused query — the
    // BM25 result over a document set is deterministic, so this yields byte-identical lexical
    // ranking to a persistent index over the same documents. Built ONLY when the plan actually
    // references a text op, so a non-text served query pays nothing. (A persistent index-on-write
    // in `ServerState` beside `graph.redb` is a pure performance follow-up — it does not change
    // the answer, only avoids the per-query build.)
    let ctx = PlanCtx::new(view, semantic);
    #[cfg(feature = "text")]
    let served_text_index = plan_needs_text(&ops).then(|| build_text_index_from_view(view));
    #[cfg(feature = "text")]
    let ctx = match served_text_index.as_ref().and_then(|r| r.as_ref()) {
        Some(index) => ctx.with_text(index),
        None => ctx,
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
    // RECONCILE (CONCEPT:EG-KG.query.native-time-series): attach the committed tsdb store so `Op::TsScan`
    // sources real series (tsdb-in-plan fusion). Absent store ⇒ ctx unchanged.
    #[cfg(feature = "tsdb")]
    let ctx = match tsdb {
        Some(store) => ctx.with_tsdb(store),
        None => ctx,
    };
    // CONCEPT:EG-KG.query.txn-tsdb-read-your: attach the txn's staged-series overlay so an in-txn `Op::TsScan`
    // reads its own uncommitted points (RYOW). Absent overlay ⇒ committed series only.
    #[cfg(feature = "tsdb")]
    let ctx = match staged_series {
        Some(staged) => ctx.with_staged_series(staged),
        None => ctx,
    };
    let result = eg_plan::execute(&eg_plan::Plan::new(ops), &ctx)?;
    Ok(result
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect())
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
    reorder_filter_selectivity: Option<f64>,
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Response {
    // Resolve the txn's target core + snapshot its staged write-set/embeddings while
    // holding only the cheap state read + per-txn lock; everything moved into the
    // off-lock closure is OWNED, so no lock is held across the compute.
    let (mut view, write_set, committed_semantic, vectors) = {
        let s = state.read().await;
        let entry = match s.open_txns.get(txn_id) {
            Some(e) => e,
            None => {
                return Response::err(req_id, format!("unknown transaction '{}'", txn_id));
            }
        };
        let guard = entry.value().lock();
        let core = match s.registry.get(&guard.graph) {
            Some(g) => g.core.clone(),
            None => {
                return Response::err(req_id, format!("Graph '{}' not found", guard.graph));
            }
        };
        // Committed base snapshot (O(V+E) structural copy) + committed embedding store,
        // taken at ONE point in time so the cross-modal read is snapshot-isolated.
        #[cfg_attr(not(feature = "security"), allow(unused_mut))]
        let mut view = core.analysis_snapshot();
        #[cfg(feature = "security")]
        rls.filter_view(caller.unwrap_or(""), &mut view);
        let committed_semantic = core.semantic_store.read().clone();
        (
            view,
            guard.write_set.clone(),
            committed_semantic,
            guard.vectors.clone(),
        )
        // `guard` + `s` drop here — no lock held across the compute below.
    };
    // RECONCILE (CONCEPT:EG-KG.query.native-time-series): the committed tsdb `SeriesStore` for `Op::TsScan`
    // fusion inside the txn, so an in-txn UQL reads COMMITTED series.
    #[cfg(feature = "tsdb")]
    let tsdb = state.read().await.tsdb_store.clone();
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
                staged.push_points(&m.series, m.points.iter().cloned());
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
    rls.filter_view(caller.unwrap_or(""), &mut view);
    let semantic = eg_core::compute::semantic::semantic_overlay(committed_semantic, &vectors);
    match compute_off_lock(req_id, move || {
        run_unified(
            plan,
            reorder_filter_selectivity,
            &view,
            &semantic,
            #[cfg(feature = "tsdb")]
            tsdb.as_deref(),
            #[cfg(feature = "tsdb")]
            Some(&staged_series),
        )
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
                    rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                        conditions_msgpack,
                    ),
                    rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                        updates_msgpack,
                    ),
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
///   * user-table DDL/DML → the shared durable `TableStore` (redb commit-before-ack,
///     self-durable).
///
/// Blocking work (redb commits, the node scan) runs on the blocking pool via
/// `compute_off_lock`. Returns a `QueryResult`-shaped ack (`[tag]` column, one
/// rows-affected row) so the client decodes a write response exactly like a read.
#[cfg(feature = "query")]
async fn exec_sql_write(
    req_id: u64,
    core: &Arc<GraphCore>,
    store: &eg_query::TableStore,
    kind: eg_query::StatementKind,
) -> Response {
    use eg_query::StatementKind as K;
    match kind {
        K::InsertNodes(ins) => {
            let core = core.clone();
            let r = compute_off_lock(req_id, move || {
                let mut n = 0usize;
                for node in ins.rows {
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
                let ids = matched_node_ids(&core, &upd.selector);
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
                let ids = matched_node_ids(&core, &del.selector);
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
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                let columns = to_store_columns(&plan.columns)?;
                let schema = eg_query::TableSchema {
                    name: plan.name,
                    columns,
                };
                store
                    .create_table(&schema, plan.if_not_exists)
                    .map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "CREATE TABLE", r)
        }
        K::DropTable(plan) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.drop_table(&plan.name, plan.if_exists).map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "DROP TABLE", r)
        }
        // CONCEPT:EG-KG.query.register-user-tables-alongside ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog the rest — one dispatch helper.
        K::AlterTable(plan) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || apply_alter_table(&store, plan).map(|_| 0usize)).await;
            sql_write_ack(req_id, "ALTER TABLE", r)
        }
        K::InsertTable(ins) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.insert_rows(&ins.table, &ins.columns, &ins.rows)
            })
            .await;
            sql_write_ack(req_id, "INSERT", r)
        }
        K::InsertSelect(ins) => {
            // The SELECT half runs through the SAME tables-aware DataFusion path (so it
            // can JOIN user tables AND the graph); its projected rows are then durably
            // inserted. Column COUNT must match the insert column list.
            let store = store.clone();
            let snap = core.analysis_snapshot();
            let r = compute_off_lock(req_id, move || {
                let read = eg_query::exec_sql_typed_with_tables(&snap, &store, &ins.select_sql)?;
                if read.columns.len() != ins.columns.len() {
                    return Err(format!(
                        "INSERT … SELECT column count mismatch: {} target columns, {} selected",
                        ins.columns.len(),
                        read.columns.len()
                    ));
                }
                store.insert_rows(&ins.table, &ins.columns, &read.rows)
            })
            .await;
            sql_write_ack(req_id, "INSERT", r)
        }
        K::UpdateTable(upd) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                // CONCEPT:EG-KG.query.compound-predicate-decode — the store evaluates the compound predicate per row.
                store.update_where(&upd.table, &upd.set, &upd.selector.pred)
            })
            .await;
            sql_write_ack(req_id, "UPDATE", r)
        }
        K::DeleteTable(del) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.delete_where(&del.table, &del.selector.pred)
            })
            .await;
            sql_write_ack(req_id, "DELETE", r)
        }
        // A single wire request cannot hold a multi-statement transaction across
        // requests (the pgwire shim does — it is connection-stateful). These are
        // accepted as benign no-op acks (Postgres-compatible) rather than errored, so a
        // client that brackets statements in BEGIN/COMMIT over the wire still succeeds
        // (each non-txn statement is already durably applied as it lands).
        K::Begin => sql_write_ack(req_id, "BEGIN", Ok(Ok(0))),
        K::Commit => sql_write_ack(req_id, "COMMIT", Ok(Ok(0))),
        K::Rollback => sql_write_ack(req_id, "ROLLBACK", Ok(Ok(0))),
        // CONCEPT:EG-KG.query.insert-into-nodes-select — INSERT INTO nodes … SELECT over the RPC wire (write-ack; no RETURNING).
        K::InsertNodesSelect(ins) => {
            let core = core.clone();
            let store = store.clone();
            let snap = core.analysis_snapshot();
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
                    if core.has_node(&node_id) {
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
            let snap = core.analysis_snapshot();
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
            let snap = core.analysis_snapshot();
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
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store
                    .create_view(&plan.name, &plan.select_sql, plan.or_replace)
                    .map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "CREATE VIEW", r)
        }
        K::DropView(plan) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.drop_view(&plan.name, plan.if_exists).map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "DROP VIEW", r)
        }
        // CONCEPT:EG-KG.query.create-drop-extension-over — CREATE/DROP EXTENSION over the RPC wire.
        K::CreateExtension {
            name,
            if_not_exists,
        } => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.create_extension(&name, if_not_exists).map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "CREATE EXTENSION", r)
        }
        K::DropExtension { name, if_exists } => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.drop_extension(&name, if_exists).map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "DROP EXTENSION", r)
        }
        // CONCEPT:EG-KG.query.create-drop-function — CREATE/DROP FUNCTION over the RPC wire.
        K::CreateFunction(plan) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store
                    .create_function(&plan.func, plan.or_replace)
                    .map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "CREATE FUNCTION", r)
        }
        K::DropFunction(plan) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.drop_function(&plan.name, plan.if_exists).map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "DROP FUNCTION", r)
        }
        // ── Postgres-family extension parity (wave 19) ──────────────────────────
        // CONCEPT:EG-KG.query.postgres-family-extension-plan — Apache AGE cypher() is a read; run it + project the agtype
        // result onto the AS columns, returning a result set (like the read path).
        K::CypherCall(plan) => {
            #[cfg(feature = "cypher")]
            {
                let core = core.clone();
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
        // CONCEPT:EG-KG.query.real-ann-top-k — acknowledge the pgvector ANN index (brute-force EG-115 still
        // serves NN queries; durable catalog + eg-ann pushdown is a follow-up).
        K::CreateAnnIndex(_) => sql_write_ack(req_id, "CREATE INDEX", Ok(Ok(0))),
        // CONCEPT:EG-KG.query.continuous-aggregate-lowering — accept the hypertable declaration (metadata durability is a
        // follow-up).
        K::CreateHypertable(_) => sql_write_ack(req_id, "CREATE TABLE", Ok(Ok(0))),
        // CONCEPT:EG-KG.query.continuous-aggregate-lowering — lower the continuous aggregate onto the durable view catalog.
        K::CreateContinuousAggregate(plan) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                store.create_view(&plan.name, &plan.select_sql, true).map(|_| 0usize)
            })
            .await;
            sql_write_ack(req_id, "CREATE MATERIALIZED VIEW", r)
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
                if let Ok(serde_json::Value::Object(mut obj)) =
                    rmp_serde::from_slice::<serde_json::Value>(&blob)
                {
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

/// Route a decoded `ALTER TABLE` action to the matching durable `TableStore` mutation
/// (CONCEPT:EG-KG.query.register-user-tables-alongside ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog DROP/RENAME COLUMN, RENAME TABLE, ALTER
/// COLUMN TYPE, DROP CONSTRAINT). Mirrors the embedded/pgwire dispatch.
#[cfg(feature = "query")]
fn apply_alter_table(
    store: &eg_query::TableStore,
    plan: eg_query::AlterTablePlan,
) -> Result<(), String> {
    use eg_query::AlterTableAction as A;
    match plan.action {
        A::AddColumn(col) => {
            let columns = to_store_columns(std::slice::from_ref(&col))?;
            let column = columns.into_iter().next().ok_or("ALTER TABLE: no column")?;
            store.add_column(&plan.name, column)
        }
        A::DropColumn { column, if_exists } => store.drop_column(&plan.name, &column, if_exists),
        A::RenameColumn { from, to } => store.rename_column(&plan.name, &from, &to),
        A::RenameTable { new_name } => store.rename_table(&plan.name, &new_name),
        A::AlterColumnType { column, new_type } => {
            let ty = eg_query::ColumnType::parse(&new_type)?;
            store.alter_column_type(&plan.name, &column, ty)
        }
        A::DropConstraint {
            constraint,
            if_exists,
        } => store.drop_constraint(&plan.name, &constraint, if_exists),
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
        if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob) {
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
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> crate::graph::GraphView {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller.unwrap_or(""), &mut snap);
    snap
}

/// ⚠ THE RLS-AWARE RESULT-CACHE KEY (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231 — the headline
/// reconciliation). RLS makes a query's RESULT agent-specific: agent A and agent B
/// running the SAME query text see DIFFERENT rows (A cannot see B's private nodes).
/// The result cache is keyed by `(query-hash, version)`; if that hash ignored the
/// caller, agent A's cached (A-filtered) result could be served to agent B for the
/// same query text — a cross-agent data leak.
///
/// Fix (option a, "include the caller's RLS-key in the cache key"): when RLS is
/// ACTIVE (`rls.has_rules()` — at least one identity registered, i.e. the engine is
/// in multi-tenant enforcing mode), we fold the caller's RLS context into the hash
/// `kind` so a different caller keys to a different cache slot. The agent_id IS the
/// complete RLS visibility key: `IsolationLayer::filter_view`/`can_see_row` resolve
/// a row's visibility for a caller PURELY from that caller's agent_id against the
/// registered identities (owner / explicit grants / manager-of / System role), so
/// two requests with the same agent_id always get the byte-identical filtered view,
/// and two with different agent_ids may not — exactly the cache-key equivalence we
/// need. When RLS is INACTIVE (single-tenant, `has_rules()==false`, or the `security`
/// feature is off) the salt is empty and the key is the plain `(kind, payload)` —
/// zero behavior change from the cache-only branch.
#[cfg(all(
    feature = "result-cache",
    any(feature = "query", feature = "cypher", feature = "graphql")
))]
fn rls_cache_hash(
    kind: &str,
    payload: &[u8],
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> u128 {
    #[cfg(feature = "security")]
    {
        // RLS active ⇒ namespace the hash by the caller's visibility key (agent_id).
        // The `rls:` prefix keeps it distinct from any other kind. RLS inactive ⇒
        // fall through to the plain key so single-tenant caching is byte-identical.
        if rls.has_rules() {
            let salted_kind = format!("rls:{}:{}", caller.unwrap_or(""), kind);
            return ResultCache::hash_query(&salted_kind, payload);
        }
    }
    ResultCache::hash_query(kind, payload)
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

    /// Three nodes: B's private, a public one, an unowned legacy one.
    fn seeded_view() -> GraphView {
        let mut v = GraphView::default();
        for id in ["secret_b", "public_x", "legacy_z"] {
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
            .insert("legacy_z".to_string(), node_blob(&[("type", "Legacy")]));
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
        let r = eg_query::exec_sql(view, "SELECT id FROM nodes").expect("sql");
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
        assert!(ids.contains(&"legacy_z".to_string()));

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
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
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
    use crate::channels::ChannelManager;
    use crate::isolation::{AgentRole, IsolationLayer};
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::auth::compute_auth_token;
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
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
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
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
        }))
    }

    /// A request as `agent_id`.
    fn req_as(id: u64, agent_id: &str, method: Method) -> Request {
        Request {
            id,
            graph: "__commons__".into(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: Some(agent_id.into()),
            method,
        }
    }

    fn raw(resp: &Response) -> Vec<u8> {
        match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw result, got {other:?}"),
        }
    }

    async fn register(state: &Arc<RwLock<ServerState>>, id: u64, agent: &str) {
        let r = dispatch(
            state,
            req_as(
                id,
                agent,
                Method::RegisterIdentity {
                    agent_id: agent.into(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    signature: String::new(),
                    roles: vec![],
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "RegisterIdentity failed: {:?}", r.error);
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
            "type": label,
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
        // Activate RLS: register two peer agents (no manager/grant between them).
        register(&state, 1, "alice").await;
        register(&state, 2, "bob").await;
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
            req_as(20, "alice", Method::CypherQuery { query: Q.into() }),
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
            req_as(21, "bob", Method::CypherQuery { query: Q.into() }),
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
            req_as(22, "alice", Method::CypherQuery { query: Q.into() }),
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
        register(&state, 1, "alice").await;
        register(&state, 2, "bob").await;
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

    const SECRET: &str = "dispatch-write-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
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
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
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
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::auth::compute_auth_token;
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
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
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
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
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
                    reorder_filter_selectivity: None,
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
                    reorder_filter_selectivity: None,
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
            req(
                8,
                Method::UnifiedQueryText {
                    text: vec_q.into(),
                    reorder_filter_selectivity: None,
                },
            ),
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
        let before = dispatch(
            &state,
            req(
                3,
                Method::UnifiedQueryText {
                    text: q.into(),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await;
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
                    reorder_filter_selectivity: None,
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
        let after = dispatch(
            &state,
            req(
                6,
                Method::UnifiedQueryText {
                    text: q.into(),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await;
        assert_eq!(
            unified_ids(&after),
            vec!["cn".to_string()],
            "committed node must be visible off-txn after commit"
        );
    }
}
