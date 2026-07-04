//! Distributed graph compute handlers (CONCEPT:EG-KG.storage.feature).
//!
//! `DistributedCompute` runs a Pregel/GAS cross-shard algorithm (PageRank /
//! connected-components / BFS) across a set of graphs spanning multiple Raft groups,
//! returning the per-vertex result over the UNION. `CreateMatView`/`GetMatView`/
//! `RefreshMatView` manage named, durable, incrementally-maintained materialized
//! views of those results. The heavy superstep loop runs off the reactor.

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};
use crate::raft::pregel::{self, MatView};
#[cfg(feature = "matview")]
use crate::server::matview::{self, PlanMatView};

/// Try to handle a distributed-compute method. `Ok(resp)` = handled; `Err(method)` =
/// not mine.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::DistributedCompute { graphs, algo } => {
            Ok(match pregel::run_distributed(state, &graphs, &algo).await {
                Ok(result) => Response::ok(req_id, ResultPayload::raw(&result)),
                Err(e) => Response::err(req_id, e),
            })
        }
        Method::CreateMatView { name, graphs, algo } => {
            let result = match pregel::run_distributed(state, &graphs, &algo).await {
                Ok(r) => r,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let view = MatView {
                name: name.clone(),
                graphs,
                algo,
                result,
            };
            Ok(persist_and_index(state, req_id, view).await)
        }
        Method::GetMatView { name } => {
            let view = {
                let s = state.read().await;
                let store = s.matviews.lock();
                store.get(&name).cloned()
            };
            Ok(match view {
                Some(v) => Response::ok(req_id, ResultPayload::raw(&v.result)),
                None => Response::err(req_id, format!("no materialized view '{name}'")),
            })
        }
        Method::RefreshMatView { name } => {
            // Read the view's definition, recompute its result over the (possibly
            // changed) graphs, and re-persist. For connected-components the recompute
            // uses the incremental primitive seeded from the prior labeling (proven
            // equal to from-scratch); PageRank/BFS recompute fully (the supersteps are
            // the recompute). Either way the refreshed result reflects the current
            // graphs and stays durable.
            let existing = {
                let s = state.read().await;
                let store = s.matviews.lock();
                store.get(&name).cloned()
            };
            let Some(mut view) = existing else {
                return Ok(Response::err(
                    req_id,
                    format!("no materialized view '{name}'"),
                ));
            };
            let refreshed = match pregel::run_distributed(state, &view.graphs, &view.algo).await {
                Ok(r) => r,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            view.result = refreshed;
            Ok(persist_and_index(state, req_id, view).await)
        }

        // ── Plan-backed materialized views (CONCEPT:EG-KG.storage.plan-backed-matview) ──
        // GENERALIZES the algo-only matviews above: a matview is a named, durable
        // `wire::Plan` over one graph whose RESULT rides the version-keyed, RLS-aware
        // result cache. Define executes + caches, Get serves fresh-or-recomputes, Refresh
        // forces recompute, Drop removes. A committed write bumps the graph version (and
        // the CDC hub marks the view stale), so a stale result is never served.
        #[cfg(feature = "matview")]
        Method::PlanMatViewDefine {
            name,
            graph,
            plan,
            reorder_filter_selectivity,
        } => Ok(define_plan_matview(
            state,
            req_id,
            PlanMatView {
                name,
                graph,
                plan,
                reorder_filter_selectivity,
            },
        )
        .await),
        #[cfg(feature = "matview")]
        Method::PlanMatViewGet { name } => Ok(get_plan_matview(state, req_id, &name).await),
        #[cfg(feature = "matview")]
        Method::PlanMatViewRefresh { name } => Ok(refresh_plan_matview(state, req_id, &name).await),
        #[cfg(feature = "matview")]
        Method::PlanMatViewDrop { name } => Ok(drop_plan_matview(state, req_id, &name).await),

        other => Err(other),
    }
}

// ── Plan-backed matview handlers (CONCEPT:EG-KG.storage.plan-backed-matview) ──────────

/// Clone a graph's `GraphCore` out from under the registry read lock. `None` = the graph
/// does not exist.
#[cfg(feature = "matview")]
async fn resolve_core(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
) -> Option<std::sync::Arc<eg_core::graph::GraphCore>> {
    let s = state.read().await;
    s.registry.get(graph).map(|e| e.core.clone())
}

/// MATERIALIZE a plan-backed matview + cache its serialized result on the graph's
/// `GraphCore` result cache. Returns `(serialized_rows, row_count)`. The result is cached
/// under `(plan_hash, graph_version, actor_scope=0)` (CONCEPT:EG-KG.query.rls-scoped-result-cache):
/// a write bumps `version` (retiring it); an RLS actor's nonzero scope MISSES this
/// system-scoped entry, so an unfiltered result is never served across actors.
#[cfg(feature = "matview")]
async fn materialize_and_cache(
    state: &Arc<RwLock<ServerState>>,
    def: &PlanMatView,
) -> Result<(Vec<u8>, usize), String> {
    let core = resolve_core(state, &def.graph).await.ok_or_else(|| {
        format!(
            "plan matview '{}': graph '{}' not found",
            def.name, def.graph
        )
    })?;
    let (snap, version) = core.analysis_snapshot_versioned();
    let semantic = core.semantic_store.read().clone();
    let rows = matview::materialize(def, &snap, &semantic)?;
    let count = rows.len();
    let bytes =
        rmp_serde::to_vec_named(&rows).map_err(|e| format!("serialize matview rows: {e}"))?;
    let hash = matview::plan_hash(def);
    core.result_cache()
        .put_scoped(hash, version, 0, bytes.clone());
    Ok((bytes, count))
}

/// Persist a plan-backed matview DEFINITION to the durable redb tier (best-effort). `None`
/// = persisted (or no redb backend configured); `Some(e)` = the durable write failed.
#[cfg(feature = "matview")]
async fn persist_def(state: &Arc<RwLock<ServerState>>, def: &PlanMatView) -> Option<String> {
    // No backend / not a redb backend ⇒ nothing to persist (a clean `None` = "ok").
    let backend = { state.read().await.persistence.clone() }?;
    let redb = backend.as_redb()?;
    match matview::encode_def(def) {
        Ok(blob) => redb.plan_matview_put(&def.name, blob).await.err(),
        Err(e) => Some(e),
    }
}

/// `PlanMatViewDefine`: materialize once, cache, persist the definition, index in RAM.
#[cfg(feature = "matview")]
async fn define_plan_matview(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    def: PlanMatView,
) -> Response {
    let count = match materialize_and_cache(state, &def).await {
        Ok((_, count)) => count,
        Err(e) => return Response::err(req_id, e),
    };
    let persist_err = persist_def(state, &def).await;
    matview::manager().define(def);
    match persist_err {
        None => Response::ok(req_id, ResultPayload::Count(count as u64)),
        Some(e) => Response::err(
            req_id,
            format!("plan matview cached in RAM but durable write failed: {e}"),
        ),
    }
}

/// `PlanMatViewGet`: serve the cached result when fresh (no CDC change AND a cache hit at
/// the current version); otherwise recompute + re-cache and clear the stale flag.
#[cfg(feature = "matview")]
async fn get_plan_matview(state: &Arc<RwLock<ServerState>>, req_id: u64, name: &str) -> Response {
    let Some(def) = matview::manager().get(name) else {
        return Response::err(req_id, format!("no plan materialized view '{name}'"));
    };
    // Fast path: not marked stale by CDC AND a live cache hit at the current version.
    if !matview::manager().is_stale(name) {
        if let Some(core) = resolve_core(state, &def.graph).await {
            let version = core.version();
            let hash = matview::plan_hash(&def);
            if let Some(bytes) = core.result_cache().get_scoped(hash, version, 0) {
                return Response::ok(req_id, ResultPayload::Raw(bytes));
            }
        }
    }
    // Stale (a write landed) or a cache miss (evicted / version bumped) → recompute.
    match materialize_and_cache(state, &def).await {
        Ok((bytes, _)) => {
            matview::manager().mark_fresh(name);
            Response::ok(req_id, ResultPayload::Raw(bytes))
        }
        Err(e) => Response::err(req_id, e),
    }
}

/// `PlanMatViewRefresh`: force a re-materialization NOW (bypass the freshness check).
#[cfg(feature = "matview")]
async fn refresh_plan_matview(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    name: &str,
) -> Response {
    let Some(def) = matview::manager().get(name) else {
        return Response::err(req_id, format!("no plan materialized view '{name}'"));
    };
    match materialize_and_cache(state, &def).await {
        Ok((_, count)) => {
            matview::manager().mark_fresh(name);
            Response::ok(req_id, ResultPayload::Count(count as u64))
        }
        Err(e) => Response::err(req_id, e),
    }
}

/// `PlanMatViewDrop`: remove the view from RAM + the durable tier. Returns whether it
/// existed. The cached result (version-keyed) simply ages out of the LRU.
#[cfg(feature = "matview")]
async fn drop_plan_matview(state: &Arc<RwLock<ServerState>>, req_id: u64, name: &str) -> Response {
    let existed = matview::manager().drop_view(name);
    let backend = { state.read().await.persistence.clone() };
    if let Some(backend) = backend {
        if let Some(redb) = backend.as_redb() {
            let _ = redb.plan_matview_delete(name).await;
        }
    }
    Response::ok(req_id, ResultPayload::Bool(existed))
}

/// Persist a matview to the durable redb tier (when available) AND index it in RAM,
/// returning a row-count response. Durability is best-effort-then-error: a redb commit
/// failure surfaces as an error (the in-RAM copy is still updated so reads work, but the
/// caller learns it is not durable).
async fn persist_and_index(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    view: MatView,
) -> Response {
    let rows = view.result.len();
    // Durable write first (if a redb backend is configured), then the in-RAM index.
    let persist_err = {
        let backend = {
            let s = state.read().await;
            s.persistence.clone()
        };
        if let Some(backend) = backend {
            if let Some(redb) = backend.as_redb() {
                match rmp_serde::to_vec_named(&view) {
                    Ok(blob) => redb.matview_put(&view.name, blob).await.err(),
                    Err(e) => Some(format!("serialize matview: {e}")),
                }
            } else {
                None
            }
        } else {
            None
        }
    };
    {
        let s = state.read().await;
        s.matviews.lock().put(view);
    }
    match persist_err {
        None => Response::ok(req_id, ResultPayload::Count(rows as u64)),
        Some(e) => Response::err(
            req_id,
            format!("matview persisted in RAM but durable write failed: {e}"),
        ),
    }
}

/// Reload every persisted materialized view into the in-RAM index on boot
/// (CONCEPT:EG-KG.storage.feature). Called once after the redb store is up. Returns the count
/// reloaded. A missing/empty table reloads nothing (a fresh DB).
pub async fn reload_matviews(state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
    let backend = {
        let s = state.read().await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Ok(0);
    };
    let Some(redb) = backend.as_redb() else {
        return Ok(0);
    };
    let rows = redb.matview_scan()?;
    let mut n = 0usize;
    {
        let s = state.read().await;
        let mut store = s.matviews.lock();
        for (name, blob) in rows {
            match rmp_serde::from_slice::<MatView>(&blob) {
                Ok(view) => {
                    store.put(view);
                    n += 1;
                }
                Err(e) => tracing::warn!("skipping corrupt matview '{name}': {e}"),
            }
        }
    }
    // Re-hydrate the PLAN-BACKED matview manager from its disjoint durable table
    // (CONCEPT:EG-KG.storage.plan-backed-matview). The result rows are NOT persisted (they
    // ride the version-keyed result cache), so a reloaded view materializes lazily on its
    // first `Get` (its cache lookup MISSES — nothing cached yet — and recomputes).
    #[cfg(feature = "matview")]
    {
        for (name, blob) in redb.plan_matview_scan()? {
            match matview::decode_def(&blob) {
                Ok(def) => {
                    matview::manager().define(def);
                    n += 1;
                }
                Err(e) => tracing::warn!("skipping corrupt plan matview '{name}': {e}"),
            }
        }
    }
    Ok(n)
}
