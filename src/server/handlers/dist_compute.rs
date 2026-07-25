//! Distributed graph compute handlers (CONCEPT:EG-KG.storage.feature).
//!
//! `DistributedCompute` runs a Pregel/GAS cross-shard algorithm (PageRank /
//! connected-components / BFS) across a set of graphs spanning multiple Raft groups,
//! returning the per-vertex result over the UNION. `CreateMatView`/`GetMatView`/
//! `RefreshMatView` manage named, durable, incrementally-maintained materialized
//! views of those results. The heavy superstep loop runs off the reactor.

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::access::GraphReadAuthority;
use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};
// The algo-only Pregel matview lives behind `compute-dist` (it runs cross-shard
// supersteps over multi-Raft groups). The plan-backed matview path below shares this
// module but touches NONE of it.
#[cfg(feature = "compute-dist")]
use crate::raft::pregel::{self, MatView};
#[cfg(feature = "matview")]
use crate::server::matview::{self, PlanMatView};

#[cfg(feature = "compute-dist")]
const MAX_DISTRIBUTED_MATVIEW_BYTES: usize = 64 * 1024 * 1024;
#[cfg(feature = "compute-dist")]
const MAX_DISTRIBUTED_MATVIEW_ITEMS: usize = 1_000_000;

#[cfg(feature = "compute-dist")]
fn decode_distributed_matview(blob: &[u8]) -> Result<MatView, String> {
    eg_types::msgpack::decode_bounded(
        blob,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_DISTRIBUTED_MATVIEW_BYTES,
            MAX_DISTRIBUTED_MATVIEW_ITEMS,
            64,
        ),
    )
    .map_err(|_| "invalid durable distributed materialized view".to_string())
}

/// Try to handle a distributed-compute method. `Ok(resp)` = handled; `Err(method)` =
/// not mine.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    read_authority: Option<&GraphReadAuthority>,
    method: Method,
) -> Result<Response, Method> {
    let original_method = method.clone();
    match method {
        #[cfg(feature = "compute-dist")]
        Method::DistributedCompute { graphs, algo } => {
            let Some(read_authority) = read_authority else {
                return Ok(Response::err(
                    req_id,
                    "distributed graph reads require the universal read authority",
                ));
            };
            Ok(
                match pregel::run_distributed(state, &graphs, &algo, read_authority).await {
                    Ok(result) => Response::ok(req_id, ResultPayload::raw(&result)),
                    Err(e) => Response::err(req_id, e),
                },
            )
        }
        #[cfg(feature = "compute-dist")]
        Method::CreateMatView { name, graphs, algo } => {
            let Some(read_authority) = read_authority else {
                return Ok(Response::err(
                    req_id,
                    "distributed materialized views require the universal read authority",
                ));
            };
            if read_authority.is_active() {
                return Ok(Response::err(
                    req_id,
                    "RLS-scoped distributed materialized views are not persistable; use DistributedCompute",
                ));
            }
            let result = match pregel::run_distributed(state, &graphs, &algo, read_authority).await
            {
                Ok(r) => r,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let view = MatView {
                name: name.clone(),
                graphs,
                algo,
                result,
            };
            Ok(persist_and_index(state, req_id, caller, &original_method, view).await)
        }
        #[cfg(feature = "compute-dist")]
        Method::GetMatView { name } => {
            let Some(read_authority) = read_authority else {
                return Ok(Response::err(
                    req_id,
                    "distributed materialized views require the universal read authority",
                ));
            };
            if read_authority.is_active() {
                return Ok(Response::err(
                    req_id,
                    "unscoped distributed materialized views are unavailable under active RLS",
                ));
            }
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
        #[cfg(feature = "compute-dist")]
        Method::RefreshMatView { name } => {
            let Some(read_authority) = read_authority else {
                return Ok(Response::err(
                    req_id,
                    "distributed materialized views require the universal read authority",
                ));
            };
            if read_authority.is_active() {
                return Ok(Response::err(
                    req_id,
                    "unscoped distributed materialized views are unavailable under active RLS",
                ));
            }
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
            let refreshed = match pregel::run_distributed(
                state,
                &view.graphs,
                &view.algo,
                read_authority,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            view.result = refreshed;
            Ok(persist_and_index(state, req_id, caller, &original_method, view).await)
        }

        // ── Plan-backed materialized views (CONCEPT:EG-KG.storage.plan-backed-matview) ──
        // GENERALIZES the algo-only matviews above: a matview is a named, durable
        // `wire::Plan` over one graph whose RESULT rides the version-keyed, RLS-aware
        // result cache. Define executes + caches, Get serves fresh-or-recomputes, Refresh
        // forces recompute, Drop removes. A committed write bumps the graph version (and
        // the CDC hub marks the view stale), so a stale result is never served.
        #[cfg(feature = "matview")]
        Method::PlanMatViewDefine { name, graph, plan } => {
            if read_authority.is_some_and(GraphReadAuthority::is_active) {
                return Ok(Response::err(
                    req_id,
                    "unscoped plan materialized views are unavailable under active RLS",
                ));
            }
            Ok(define_plan_matview(
                state,
                req_id,
                caller,
                &original_method,
                PlanMatView { name, graph, plan },
            )
            .await)
        }
        #[cfg(feature = "matview")]
        Method::PlanMatViewGet { name } => {
            if read_authority.is_some_and(GraphReadAuthority::is_active) {
                return Ok(Response::err(
                    req_id,
                    "unscoped plan materialized views are unavailable under active RLS",
                ));
            }
            Ok(get_plan_matview(state, req_id, &name).await)
        }
        #[cfg(feature = "matview")]
        Method::PlanMatViewRefresh { name } => {
            if read_authority.is_some_and(GraphReadAuthority::is_active) {
                return Ok(Response::err(
                    req_id,
                    "unscoped plan materialized views are unavailable under active RLS",
                ));
            }
            Ok(refresh_plan_matview(state, req_id, caller, &original_method, &name).await)
        }
        #[cfg(feature = "matview")]
        Method::PlanMatViewDrop { name } => {
            if read_authority.is_some_and(GraphReadAuthority::is_active) {
                return Ok(Response::err(
                    req_id,
                    "unscoped plan materialized views are unavailable under active RLS",
                ));
            }
            Ok(drop_plan_matview(state, req_id, caller, &original_method, &name).await)
        }

        other => Err(other),
    }
}

struct ControlSaga {
    backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    saga: crate::server::handlers::admin::AdminSaga,
}

async fn begin_control_saga(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
) -> Result<ControlSaga, String> {
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "materialized-view mutation requires durable redb".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "materialized-view mutation requires durable redb".to_string())?;
    let saga = crate::server::handlers::admin::begin_admin_saga(
        redb,
        req_id,
        caller,
        method,
        crate::mutation_batch::MutationDomain::ControlPlane,
    )?;
    Ok(ControlSaga { backend, saga })
}

fn finish_control_saga(
    control: ControlSaga,
    result: ResultPayload,
) -> Result<ResultPayload, String> {
    let redb = control
        .backend
        .as_redb()
        .ok_or_else(|| "materialized-view mutation lost its durable redb backend".to_string())?;
    crate::server::handlers::admin::finish_admin_saga(
        redb,
        control.saga.batch,
        control.saga.created_at_ms,
        result,
    )
}

async fn replay_control_saga(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    control: &ControlSaga,
) -> Option<Response> {
    let result = control.saga.replayed.clone()?;
    if let Err(error) = reload_matviews(state).await {
        return Some(Response::err(
            req_id,
            format!("committed matview projection reconciliation failed: {error}"),
        ));
    }
    Some(Response::ok(req_id, result))
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

/// `PlanMatViewDefine`: materialize once, cache, persist the definition, index in RAM.
#[cfg(feature = "matview")]
async fn define_plan_matview(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    def: PlanMatView,
) -> Response {
    let control = match begin_control_saga(state, req_id, caller, method).await {
        Ok(control) => control,
        Err(error) => return Response::err(req_id, error),
    };
    if let Some(response) = replay_control_saga(state, req_id, &control).await {
        return response;
    }
    let count = match materialize_and_cache(state, &def).await {
        Ok((_, count)) => count,
        Err(e) => return Response::err(req_id, e),
    };
    let blob = match matview::encode_def(&def) {
        Ok(blob) => blob,
        Err(error) => return Response::err(req_id, error),
    };
    let Some(redb) = control.backend.as_redb() else {
        return Response::err(req_id, "materialized-view mutation requires durable redb");
    };
    if let Err(error) = redb.plan_matview_put(&def.name, blob).await {
        return Response::err(req_id, error);
    }
    let result = match finish_control_saga(control, ResultPayload::Count(count as u64)) {
        Ok(result) => result,
        Err(error) => return Response::err(req_id, error),
    };
    index_matview(state, def).await;
    Response::ok(req_id, result)
}

/// Index a freshly-defined view into the manager: attempt to compile its plan into a DBSP
/// circuit (CONCEPT:EG-KG.storage.incremental-matview). On success it installs the view in
/// `Mode::Incremental` — seeded from the current graph so future CDC deltas maintain it in
/// O(delta) and `Get` serves the maintained result directly — and durably snapshots the
/// operator state. On an unsupported op it falls back to `Mode::Recompute` (today's
/// path, unchanged), NEVER a silently-wrong incremental result.
#[cfg(feature = "matview")]
async fn index_matview(state: &Arc<RwLock<ServerState>>, def: PlanMatView) {
    let mut circuit = match eg_plan::incremental::Circuit::compile(&def.plan) {
        Ok(circuit) => circuit,
        Err(unsupported) => {
            // First-class, queryable fallback (CONCEPT:EG-KG.storage.incremental-matview):
            // record the TYPED reason on the tracked view and log it structured — never a
            // silent drop. The view stays correct on today's recompute-on-`Get` path.
            tracing::info!(
                target: "epistemic_graph::matview",
                view = %def.name,
                graph = %def.graph,
                op_index = unsupported.index,
                reason = %unsupported.reason,
                "plan matview falls back to full recompute (op not incrementally maintainable)",
            );
            matview::manager().note_fallback(def, unsupported.to_string());
            return;
        }
    };
    // Seed the circuit from the current authoritative graph so it reproduces the full
    // materialization through the same gating the incremental path uses.
    let Some(core) = resolve_core(state, &def.graph).await else {
        matview::manager().define(def);
        return;
    };
    let seed = matview::incremental::seed_delta_from_core(&core);
    circuit.apply(&seed);
    // Watermark AFTER seeding (mirrors `cdc::register_query`): later deltas fold from here.
    let through_seq = {
        let s = state.read().await;
        s.cdc
            .as_ref()
            .map(|hub| hub.head_seq(&def.graph))
            .unwrap_or(0)
    };
    // Best-effort durable operator-state snapshot (observability + a future resume seam;
    // reload today conservatively re-materializes in Recompute mode, per the CDC-ring
    // staleness guard — never a silent gap).
    if let Ok(blob) = matview::incremental::encode_operator_state(&circuit) {
        let backend = { state.read().await.persistence.clone() };
        if let Some(redb) = backend.as_ref().and_then(|b| b.as_redb()) {
            if let Err(e) = redb.matview_operator_state_put(&def.name, blob).await {
                tracing::warn!("persist matview '{}' operator state failed: {e}", def.name);
            }
        }
    }
    matview::manager().install_incremental(def, circuit, through_seq);
}

/// `PlanMatViewGet`: serve the cached result when fresh (no CDC change AND a cache hit at
/// the current version); otherwise recompute + re-cache and clear the stale flag.
#[cfg(feature = "matview")]
async fn get_plan_matview(state: &Arc<RwLock<ServerState>>, req_id: u64, name: &str) -> Response {
    let Some(def) = matview::manager().get(name) else {
        return Response::err(req_id, format!("no plan materialized view '{name}'"));
    };
    // INCREMENTAL fast path (CONCEPT:EG-KG.storage.incremental-matview): the view's plan
    // compiled to a DBSP circuit that CDC deltas already maintained, so serve the live
    // `current` result directly — no recompute, no `ResultCache` round-trip, no staleness
    // check (it is fresh by construction).
    if let Some(rows) = matview::manager().incremental_rows(name) {
        return match rmp_serde::to_vec_named(&rows) {
            Ok(bytes) => Response::ok(req_id, ResultPayload::Raw(bytes)),
            Err(e) => Response::err(req_id, format!("serialize matview rows: {e}")),
        };
    }
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
    caller: Option<&str>,
    method: &Method,
    name: &str,
) -> Response {
    let control = match begin_control_saga(state, req_id, caller, method).await {
        Ok(control) => control,
        Err(error) => return Response::err(req_id, error),
    };
    if let Some(response) = replay_control_saga(state, req_id, &control).await {
        return response;
    }
    let Some(def) = matview::manager().get(name) else {
        return Response::err(req_id, format!("no plan materialized view '{name}'"));
    };
    match materialize_and_cache(state, &def).await {
        Ok((_, count)) => {
            let result = match finish_control_saga(control, ResultPayload::Count(count as u64)) {
                Ok(result) => result,
                Err(error) => return Response::err(req_id, error),
            };
            matview::manager().mark_fresh(name);
            Response::ok(req_id, result)
        }
        Err(e) => Response::err(req_id, e),
    }
}

/// `PlanMatViewDrop`: remove the view from RAM + the durable tier. Returns whether it
/// existed. The cached result (version-keyed) simply ages out of the LRU.
#[cfg(feature = "matview")]
async fn drop_plan_matview(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    name: &str,
) -> Response {
    let control = match begin_control_saga(state, req_id, caller, method).await {
        Ok(control) => control,
        Err(error) => return Response::err(req_id, error),
    };
    if let Some(response) = replay_control_saga(state, req_id, &control).await {
        return response;
    }
    let Some(redb) = control.backend.as_redb() else {
        return Response::err(req_id, "materialized-view mutation requires durable redb");
    };
    if let Err(error) = redb.plan_matview_delete(name).await {
        return Response::err(req_id, error);
    }
    // Also drop the incremental operator-state row (CONCEPT:EG-KG.storage.incremental-matview);
    // best-effort — a missing row is a clean no-op.
    if let Err(e) = redb.matview_operator_state_delete(name).await {
        tracing::warn!("drop matview '{name}' operator state failed: {e}");
    }
    let result = match finish_control_saga(control, ResultPayload::Bool(true)) {
        Ok(result) => result,
        Err(error) => return Response::err(req_id, error),
    };
    matview::manager().drop_view(name);
    Response::ok(req_id, result)
}

/// Persist a matview to the durable redb tier under a prepared/committed control-plane
/// saga and publish it into RAM only after the terminal receipt is durable. A redb or
/// coordinator failure therefore leaves no uncommitted in-memory view visible.
#[cfg(feature = "compute-dist")]
async fn persist_and_index(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    view: MatView,
) -> Response {
    let control = match begin_control_saga(state, req_id, caller, method).await {
        Ok(control) => control,
        Err(error) => return Response::err(req_id, error),
    };
    if let Some(response) = replay_control_saga(state, req_id, &control).await {
        return response;
    }
    let rows = view.result.len();
    let blob = match rmp_serde::to_vec_named(&view) {
        Ok(blob) if blob.len() <= MAX_DISTRIBUTED_MATVIEW_BYTES => blob,
        Ok(_) => return Response::err(req_id, "materialized view exceeds storage limit"),
        Err(error) => return Response::err(req_id, format!("serialize matview: {error}")),
    };
    let Some(redb) = control.backend.as_redb() else {
        return Response::err(req_id, "materialized-view mutation requires durable redb");
    };
    if let Err(error) = redb.matview_put(&view.name, blob).await {
        return Response::err(req_id, error);
    }
    let result = match finish_control_saga(control, ResultPayload::Count(rows as u64)) {
        Ok(result) => result,
        Err(error) => return Response::err(req_id, error),
    };
    {
        let s = state.read().await;
        s.matviews.lock().put(view);
    }
    Response::ok(req_id, result)
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
    let mut n = 0usize;
    // The algo-only Pregel matview store (cross-shard) — cluster-only.
    #[cfg(feature = "compute-dist")]
    {
        let rows = redb.matview_scan()?;
        let s = state.read().await;
        let mut store = s.matviews.lock();
        for (name, blob) in rows {
            match decode_distributed_matview(&blob) {
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
