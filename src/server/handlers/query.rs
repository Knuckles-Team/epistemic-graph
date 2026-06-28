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
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
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
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    match method {
        #[cfg(feature = "query")]
        Method::Sql { query, .. } => {
            // CONCEPT:EG-023 — `Method::Sql` now routes BOTH reads AND writes (was
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
            // ONE cross-modal plan (CONCEPT:KG-2.208/209): filter (DataFusion) →
            // traverse (BFS) → rank (kNN) over ONE consistent off-lock snapshot. Take
            // BOTH the GraphView (topology + property blobs) and a SemanticStore clone
            // under a brief read each — same point-in-time, so the cross-modal read is
            // snapshot-isolated — then run the whole pipeline on the blocking pool.
            // Version-keyed, RLS-aware result cache (CONCEPT:KG-2.233 × KG-2.231): key
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
            // Version-keyed, RLS-aware result cache (CONCEPT:KG-2.233 × KG-2.231): key
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
        #[cfg(feature = "graphql")]
        Method::GraphQl { query } => {
            // GraphQL WRITE surface (CONCEPT:EG-019/EG-023): a `mutation { … }` document
            // maps onto eg-core's native write ops over the LIVE `GraphCore` via
            // `execute_mutation` (which bumps the OCC version / `mark_dirty` once it
            // lands). NOT cached (it is a write) and NOT RLS pre-filtered (writes are
            // graph-ACL-gated in `dispatch_graph_op` — this method classified Write).
            if super::super::access::graphql_is_mutation(&query) {
                let core_w = core.clone();
                let resp = match compute_off_lock(req_id, move || {
                    eg_graphql::execute_mutation(&core_w, &query)
                })
                .await
                {
                    Ok(Ok(value)) => {
                        Response::ok(req_id, ResultPayload::Raw(rmp_serde::to_vec_named(&value).unwrap_or_default()))
                    }
                    Ok(Err(msg)) => Response::err(req_id, format!("GraphQL mutation error: {msg}")),
                    Err(resp) => resp,
                };
                return Ok(resp);
            }
            // A `subscription { … }` is a read-only POLL of the current matches (a full
            // push transport is a documented eg-graphql deferral); a `query { … }` is the
            // ordinary read. Both run over the SAME RLS-filtered off-lock snapshot below.
            let is_subscription = matches!(
                eg_graphql::parse_operation(&query),
                Ok(eg_graphql::Operation::Subscription(_))
            );
            // GraphQL READ surface (CONCEPT:KG-2.235): compile the GraphQL query to
            // scans + BFS over the SAME off-lock snapshot the Cypher path uses, via the
            // pure-Rust eg-graphql resolver (NO async-graphql / DataFusion). The result
            // is the GraphQL `{"data": …}` JSON, returned via `ResultPayload::Raw`.
            //
            // GraphQL runs under the SAME version-keyed, RLS-aware result cache the
            // SQL/Cypher/SPARQL paths do (CONCEPT:KG-2.233 × KG-2.231): the cache KEY
            // folds in the caller's RLS context so agent A's filtered `{data}` is NEVER
            // served to agent B for the same GraphQL query text, and the snapshot is
            // RLS-FILTERED to the caller's visible rows BEFORE the resolver runs — a
            // GraphQL read cannot leak rows across agents any more than a Cypher read.
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                let hash = rls_cache_hash(
                    "graphql",
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
            let resp = match compute_off_lock(req_id, move || {
                if is_subscription {
                    eg_graphql::subscribe(&snap, &query)
                } else {
                    eg_graphql::execute(&snap, &query)
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
            // Cypher WRITE surface (CONCEPT:EG-020/EG-023): a `CREATE`/`MERGE`/`SET`/
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
                    Ok(Ok(result)) => {
                        Response::ok(req_id, ResultPayload::Raw(rmp_serde::to_vec_named(&result).unwrap_or_default()))
                    }
                    Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
                    Err(resp) => resp,
                };
                return Ok(resp);
            }
            // Same off-lock snapshot + blocking-pool idiom as SQL — but DEP-FREE
            // (label index / VF2 / BFS), so it runs in a no-DataFusion Pi build.
            // Version-keyed, RLS-aware result cache (CONCEPT:KG-2.233 × KG-2.231) wraps
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

/// Execute a classified `Method::Sql` WRITE (CONCEPT:EG-023). Mirrors the pgwire shim's
/// classify→execute write path, reusing the SAME engine primitives:
///   * graph-node DML (`INSERT`/`UPDATE`/`DELETE` on `nodes`) → the live `GraphCore`
///     write ops (`add_node` / `compare_and_set_fields` / `remove_node`) — the dispatch
///     shell then `mark_dirty`s the graph (this method classified Write) so the next
///     checkpoint persists it;
///   * user-table DDL/DML → the shared durable `TableStore` (redb commit-before-ack,
///     self-durable).
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
                let mut n = 0usize;
                for id in ids {
                    if core.compare_and_set_fields(&id, &conditions, &upd.set) {
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
                let mut n = 0usize;
                for id in ids {
                    core.remove_node(id);
                    n += 1;
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
        K::AlterTable(plan) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                let columns = to_store_columns(std::slice::from_ref(&plan.add_column))?;
                let column = columns.into_iter().next().ok_or("ALTER TABLE: no column")?;
                store.add_column(&plan.name, column).map(|_| 0usize)
            })
            .await;
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
                let selector = eg_query::ColEq {
                    column: upd.selector.column,
                    value: upd.selector.value,
                };
                store.update_where(&upd.table, &upd.set, &selector)
            })
            .await;
            sql_write_ack(req_id, "UPDATE", r)
        }
        K::DeleteTable(del) => {
            let store = store.clone();
            let r = compute_off_lock(req_id, move || {
                let selector = eg_query::ColEq {
                    column: del.selector.column,
                    value: del.selector.value,
                };
                store.delete_where(&del.table, &selector)
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

/// Build a `QueryResult`-shaped write ack and map the off-lock outcome (CONCEPT:EG-023).
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
                rows: vec![rmp_serde::to_vec_named(&vec![serde_json::Value::from(n as u64)])
                    .unwrap_or_default()],
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

/// Resolve the node ids a simple-equality WHERE selects (CONCEPT:EG-023). Mirrors the
/// pgwire `matched_ids`: `Id` is the fast path (the node if it exists); `Property`
/// scans the node store once and matches the column's current value.
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
        eg_query::WhereEq::Property { column, value } => {
            let mut out = Vec::new();
            for (id, blob) in core.get_nodes() {
                if let Ok(serde_json::Value::Object(obj)) =
                    rmp_serde::from_slice::<serde_json::Value>(&blob)
                {
                    if obj.get(column).unwrap_or(&serde_json::Value::Null) == value {
                        out.push(id);
                    }
                }
            }
            out
        }
    }
}

/// Resolve classify `ColumnDef`s (raw SQL type spellings) into store `Column`s
/// (CONCEPT:EG-023 — mirrors the pgwire `to_store_columns`).
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

/// Produce the off-lock `GraphView` the query planner consumes, with per-agent
/// Row-Level Security applied IN the read/plan path (CONCEPT:KG-2.231). Under the
/// `security` feature the owned snapshot is filtered down to the rows `caller` may
/// see BEFORE it reaches any query surface (SQL/Cypher/unified), so no surface can
/// exfiltrate a forbidden row. Without the feature this is exactly
/// `core.analysis_snapshot()` (zero overhead, behavior unchanged). Used on the
/// `not(result-cache)` path; with the result cache the same `filter_view` is applied
/// inline on the versioned snapshot so the version pairs atomically with the filter.
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

/// ⚠ THE RLS-AWARE RESULT-CACHE KEY (CONCEPT:KG-2.233 × KG-2.231 — the headline
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
    //! Proof (CONCEPT:KG-2.231): RLS filters the read/plan-path snapshot so neither
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
        });
        layer.register_agent(AgentIdentity {
            agent_id: "bob".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
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

// ── ⚠ RLS × result-cache: NO cross-agent leak (CONCEPT:KG-2.233 × KG-2.231) ──
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
            req_as(20, "alice", Method::GraphQl { query: GQL.into() }),
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
            req_as(21, "bob", Method::GraphQl { query: GQL.into() }),
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
            req_as(22, "alice", Method::GraphQl { query: GQL.into() }),
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

// ── Server-dispatch WRITE wiring (CONCEPT:EG-023) ─────────────────────────────────
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
        let qr: crate::protocol::QueryResult = rmp_serde::from_slice(&raw(resp)).expect("QueryResult");
        let rows = qr
            .rows
            .iter()
            .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).expect("row cells"))
            .collect();
        (qr.columns, rows)
    }

    /// THE GraphQL write→read proof (CONCEPT:EG-019/EG-023): a `mutation { createNode … }`
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
            req(2, Method::GraphQl { query: r#"{ Person(name: "Dave") { name age } }"#.into() }),
        )
        .await;
        assert!(q.error.is_none(), "query failed: {:?}", q.error);
        let qv: serde_json::Value = rmp_serde::from_slice(&raw(&q)).unwrap();
        let people = qv["data"]["Person"].as_array().unwrap();
        assert_eq!(people.len(), 1);
        assert_eq!(people[0]["name"], serde_json::json!("Dave"));
        assert_eq!(people[0]["age"], serde_json::json!(50));
    }

    /// THE Cypher write→read proof (CONCEPT:EG-020/EG-023): a `CREATE` dispatched over the
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

    /// THE wire-SQL DDL/DML round-trip (CONCEPT:EG-023): `CREATE TABLE` + `INSERT` + a
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
                sql(format!("INSERT INTO {table} (k, v) VALUES ('a', 1), ('b', 2)")),
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
        assert_eq!(rows.len(), 2, "two rows round-tripped through the table store");
        assert_eq!(rows[0][0], serde_json::json!("a"));
        assert_eq!(rows[0][1], serde_json::json!(1));
        assert_eq!(rows[1][0], serde_json::json!("b"));
        assert_eq!(rows[1][1], serde_json::json!(2));

        // cleanup
        let _ = dispatch(&state, req(5, sql(format!("DROP TABLE IF EXISTS {table}")))).await;
    }

    /// `INSERT INTO nodes` over the wire lands in the graph core and a `SELECT` sees it —
    /// the agent-utilities `graph_table`/`sql_exec` node-write path (CONCEPT:EG-023).
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
        assert_eq!(rows.len(), 1, "the SQL-inserted node is visible to a SELECT");
        assert_eq!(rows[0][0], serde_json::json!("sqlnode"));

        // And a Cypher read sees it too (cross-surface).
        let cy = dispatch(
            &state,
            req(3, Method::CypherQuery { query: "MATCH (n:Gadget) RETURN n.name".into() }),
        )
        .await;
        assert!(cy.error.is_none(), "cypher read failed: {:?}", cy.error);
        let (_c2, rows2) = query_result(&cy);
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][0], serde_json::json!("Zed"));
    }
}
