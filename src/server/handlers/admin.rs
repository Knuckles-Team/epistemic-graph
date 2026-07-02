//! M3 catalog-driven resharding ADMIN RPC (CONCEPT:EG-038).
//!
//! The wire surface that DRIVES the M3 ops the engine already has the building blocks for:
//! online single-node resharding (CONCEPT:EG-032 `RedbBackend::reshard_graph`), the durable
//! tenant catalog (CONCEPT:EG-031 `RedbBackend::catalog`), the rebalancing planner
//! (CONCEPT:EG-035 `rebalance::plan_rebalance`), and its execution (CONCEPT:EG-039
//! `RedbBackend::rebalance_execute`). These CALL the existing persistence APIs — they do not
//! reimplement them.
//!
//! All are durable-redb-only. The module is always declared so the dispatch routing chain is
//! identical across builds; in a build WITHOUT `redb` the catalog/reshard/planner don't
//! exist, so the handler returns a clean "not available in this build" error.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Method, Response};
use crate::server::state::ServerState;

/// Route an M3 admin method (CONCEPT:EG-038). `Ok(resp)` = handled; `Err(method)` = not an
/// admin method (unreachable — the dispatch arm only routes admin variants here).
#[cfg(feature = "redb")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    use crate::protocol::ResultPayload;
    use crate::server::persistence::rebalance::{
        plan_rebalance, shard_loads_from_catalog, shard_loads_from_graph_loads,
    };

    // Resolve the concrete redb backend once (every admin op needs it). The owning Arc
    // (`backend_arc`) is kept alive for the whole fn so the `&RedbBackend` borrow stays
    // valid across the async reshard/rebalance awaits.
    let backend_arc = { state.read().await.persistence.clone() };
    let backend = match backend_arc.as_ref().and_then(|p| p.as_redb()) {
        Some(b) => b,
        None => {
            return Ok(Response::err(
                req_id,
                "M3 resharding admin requires a durable redb backend (no persist dir / not a \
                 redb build)",
            ))
        }
    };

    match method {
        Method::Reshard { graph, to_shard } => {
            let fname = crate::persist::sanitize(&graph);
            match backend.reshard_graph(&fname, to_shard).await {
                Ok(report) => Ok(Response::ok(
                    req_id,
                    ResultPayload::Json(report_json(&report)),
                )),
                Err(e) => Ok(Response::err(req_id, format!("Reshard failed: {e}"))),
            }
        }
        Method::CatalogAssign { graph, shard, node } => Ok(with_catalog(req_id, backend, |cat| {
            cat.assign(&crate::persist::sanitize(&graph), shard, node)
        })),
        Method::CatalogReassign { graph, shard } => Ok(with_catalog(req_id, backend, |cat| {
            cat.reassign(&crate::persist::sanitize(&graph), shard)
        })),
        Method::CatalogRemove { graph } => Ok(with_catalog(req_id, backend, |cat| {
            cat.remove(&crate::persist::sanitize(&graph))
        })),
        Method::CatalogList => {
            let Some(cat) = backend.catalog() else {
                return Ok(no_catalog(req_id));
            };
            let entries: Vec<serde_json::Value> = cat
                .entries()
                .into_iter()
                .map(|(graph, a)| {
                    serde_json::json!({"graph": graph, "shard": a.shard, "node": a.node})
                })
                .collect();
            Ok(Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({"placements": entries})),
            ))
        }
        Method::RebalancePlan {
            tolerance,
            max_moves,
        } => {
            let (loads, k) = live_graph_loads(state).await;
            let shards = match backend.catalog() {
                Some(cat) => shard_loads_from_catalog(&cat, &loads, k),
                None => {
                    // No catalog ⇒ pure EG-026 routing for the placement view.
                    let routed: Vec<(String, u32, u64)> = loads
                        .iter()
                        .map(|(g, l)| {
                            (
                                g.clone(),
                                crate::server::persistence::redb_backend::shard_index(g, k) as u32,
                                *l,
                            )
                        })
                        .collect();
                    shard_loads_from_graph_loads(&routed, k)
                }
            };
            let plan = plan_rebalance(&shards, rebalance_opts(tolerance, max_moves));
            Ok(Response::ok(
                req_id,
                ResultPayload::Json(plan_json(&plan, &shards)),
            ))
        }
        Method::RebalanceExecute {
            tolerance,
            max_moves,
        } => {
            let Some(cat) = backend.catalog() else {
                return Ok(no_catalog(req_id));
            };
            let (loads, k) = live_graph_loads(state).await;
            let shards = shard_loads_from_catalog(&cat, &loads, k);
            let plan = plan_rebalance(&shards, rebalance_opts(tolerance, max_moves));
            match backend.rebalance_execute(&plan).await {
                Ok(reports) => {
                    let moves: Vec<serde_json::Value> = reports.iter().map(report_json).collect();
                    Ok(Response::ok(
                        req_id,
                        ResultPayload::Json(serde_json::json!({"executed": moves})),
                    ))
                }
                Err(e) => Ok(Response::err(
                    req_id,
                    format!("RebalanceExecute failed: {e}"),
                )),
            }
        }
        // ── Online backup / restore (CONCEPT:EG-090) ─────────────────
        Method::Backup { destination, label } => {
            // ONLINE, no quiesce: per-shard begin_read() MVCC snapshot streamed verbatim.
            // The engine version + wall-clock timestamp are supplied HERE (application
            // code) — the library `backup` fn never reads the clock.
            let ts = now_secs();
            match backend.backup(
                std::path::Path::new(&destination),
                env!("CARGO_PKG_VERSION"),
                ts,
                label.as_deref().unwrap_or(""),
            ) {
                Ok(r) => Ok(Response::ok(
                    req_id,
                    ResultPayload::Json(serde_json::json!({
                        "destination": destination,
                        "label": label.unwrap_or_default(),
                        "timestamp": ts,
                        "engine_version": env!("CARGO_PKG_VERSION"),
                        "shards": r.shards,
                        "graphs": r.graphs,
                        "nodes": r.nodes,
                        "edges": r.edges,
                        "ledger": r.ledger,
                        "semantic": r.semantic,
                        "audit": r.audit,
                        "global": r.global,
                    })),
                )),
                Err(e) => Ok(Response::err(req_id, format!("Backup failed: {e}"))),
            }
        }
        Method::Restore { source } => {
            // The running engine holds an exclusive lock on its live persist dir, so an
            // in-place restore is offline-only (use the `restore` CLI). Over the wire we
            // STAGE the rebuilt copy in a sibling dir for the operator to swap in.
            let Some(persist_dir) = backend.persist_dir() else {
                return Ok(Response::err(
                    req_id,
                    "cannot resolve the engine persist dir for a staged restore",
                ));
            };
            let stage = persist_dir.with_file_name(format!(
                "{}.restored-{}",
                persist_dir
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "eg".to_string()),
                now_secs()
            ));
            match crate::server::persistence::backup::restore_bundle(
                std::path::Path::new(&source),
                &stage,
                None,
            ) {
                Ok(r) => Ok(Response::ok(
                    req_id,
                    ResultPayload::Json(serde_json::json!({
                        "source": source,
                        "staged_dir": stage.display().to_string(),
                        "note": "restored into a sibling dir; stop the engine and swap it \
                                 into the persist dir to activate (in-place restore uses \
                                 the offline `restore` CLI)",
                        "restored_shards": r.restored_shards,
                        "bundle_engine_version": r.manifest.engine_version,
                        "bundle_timestamp": r.manifest.timestamp,
                        "bundle_label": r.manifest.label,
                        "graphs": r.migration.graphs,
                        "nodes": r.migration.nodes,
                        "edges": r.migration.edges,
                        "ledger": r.migration.ledger,
                        "semantic": r.migration.semantic,
                        "audit": r.migration.audit,
                        "global": r.migration.global,
                    })),
                )),
                Err(e) => Ok(Response::err(req_id, format!("Restore failed: {e}"))),
            }
        }
        other => Err(other),
    }
}

/// Wall-clock Unix seconds for the backup/restore RPC (CONCEPT:EG-090). Lives in the
/// HANDLER (application code), never in the library `backup`/`restore_bundle` fns.
#[cfg(feature = "redb")]
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Non-redb build: every admin method returns a clean "not available" error.
#[cfg(not(feature = "redb"))]
pub(crate) async fn try_handle(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::CatalogList
        | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. }
        | Method::Backup { .. }
        | Method::Restore { .. } => Ok(Response::err(
            req_id,
            "M3 resharding admin is not available in this build (requires the `redb` feature)",
        )),
        other => Err(other),
    }
}

#[cfg(feature = "redb")]
fn report_json(
    report: &crate::server::persistence::online_reshard::ReshardReport,
) -> serde_json::Value {
    serde_json::json!({
        "graph": report.graph,
        "from_shard": report.from_shard,
        "to_shard": report.to_shard,
        "nodes": report.nodes,
        "edges": report.edges,
        "ledger": report.ledger,
        "semantic": report.semantic,
        "audit": report.audit,
        "delta_nodes": report.delta_nodes,
        "delta_edges": report.delta_edges,
        "no_op": report.no_op,
    })
}

#[cfg(feature = "redb")]
fn plan_json(
    plan: &crate::server::persistence::rebalance::RebalancePlan,
    shards: &[crate::server::persistence::rebalance::ShardLoad],
) -> serde_json::Value {
    let moves: Vec<serde_json::Value> = plan
        .moves
        .iter()
        .map(|m| {
            serde_json::json!({
                "graph": m.graph,
                "from_shard": m.from_shard,
                "to_shard": m.to_shard,
            })
        })
        .collect();
    let loads: Vec<serde_json::Value> = shards
        .iter()
        .map(
            |s| serde_json::json!({"shard": s.shard, "total": s.total(), "graphs": s.graphs.len()}),
        )
        .collect();
    serde_json::json!({"moves": moves, "shards": loads})
}

/// Run a catalog mutation, returning `Bool(true)` on success or a clean error when no
/// catalog is attached / the write fails.
#[cfg(feature = "redb")]
fn with_catalog(
    req_id: u64,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    f: impl FnOnce(&crate::server::persistence::tenant_catalog::TenantCatalog) -> Result<(), String>,
) -> Response {
    use crate::protocol::ResultPayload;
    let Some(cat) = backend.catalog() else {
        return no_catalog(req_id);
    };
    match f(&cat) {
        Ok(()) => Response::ok(req_id, ResultPayload::Bool(true)),
        Err(e) => Response::err(req_id, format!("catalog write failed: {e}")),
    }
}

#[cfg(feature = "redb")]
fn no_catalog(req_id: u64) -> Response {
    Response::err(
        req_id,
        "no tenant catalog attached (set EPISTEMIC_GRAPH_TENANT_CATALOG=1 and restart)",
    )
}

#[cfg(feature = "redb")]
fn rebalance_opts(
    tolerance: Option<f64>,
    max_moves: Option<usize>,
) -> crate::server::persistence::rebalance::RebalanceOptions {
    let mut opts = crate::server::persistence::rebalance::RebalanceOptions::default();
    if let Some(t) = tolerance {
        opts.tolerance = t;
    }
    if let Some(m) = max_moves {
        opts.max_moves = m;
    }
    opts
}

/// Live per-graph load `(sanitized_fname, resident_node_count)` over the registry + the
/// shard count K (CONCEPT:EG-035 integration). Resident node count is the KG-2.51 per-graph
/// size dimension — a cheap, available balance metric. `__commons__` is included like any
/// other graph. Returns `(loads, k)`.
#[cfg(feature = "redb")]
async fn live_graph_loads(state: &Arc<RwLock<ServerState>>) -> (Vec<(String, u64)>, usize) {
    let s = state.read().await;
    let loads: Vec<(String, u64)> = s
        .registry
        .all_entries()
        .iter()
        .map(|e| {
            (
                crate::persist::sanitize(&e.name),
                e.core.node_count() as u64,
            )
        })
        .collect();
    let k = s
        .persistence
        .as_ref()
        .and_then(|p| p.as_redb())
        .map(|r| r.shard_count())
        .unwrap_or(1);
    (loads, k)
}
