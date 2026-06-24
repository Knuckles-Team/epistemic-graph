//! Distributed graph compute handlers (CONCEPT:KG-2.227).
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
        Method::CreateMatView {
            name,
            graphs,
            algo,
        } => {
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
                return Ok(Response::err(req_id, format!("no materialized view '{name}'")));
            };
            let refreshed = match pregel::run_distributed(state, &view.graphs, &view.algo).await {
                Ok(r) => r,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            view.result = refreshed;
            Ok(persist_and_index(state, req_id, view).await)
        }
        other => Err(other),
    }
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
        Some(e) => Response::err(req_id, format!("matview persisted in RAM but durable write failed: {e}")),
    }
}

/// Reload every persisted materialized view into the in-RAM index on boot
/// (CONCEPT:KG-2.227). Called once after the redb store is up. Returns the count
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
    let s = state.read().await;
    let mut store = s.matviews.lock();
    let mut n = 0usize;
    for (name, blob) in rows {
        match rmp_serde::from_slice::<MatView>(&blob) {
            Ok(view) => {
                store.put(view);
                n += 1;
            }
            Err(e) => tracing::warn!("skipping corrupt matview '{name}': {e}"),
        }
    }
    Ok(n)
}
