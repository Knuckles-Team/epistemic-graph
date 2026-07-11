//! Placement-catalog wire RPC (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4).
//!
//! Exposes [`crate::raft::placement::PlacementCatalog`] — DIST-P2-1's ONE placement
//! authority for virtual partitions — over the served RPC surface via
//! `Method::PlacementRoute`. Until this module, the catalog was consumed only
//! INSIDE the engine (`MultiRaft::route_graph`, DIST-P2-2's cross-shard read
//! fan-out); this is the DIST-P2-4 follow-up `raft/placement.rs`'s module docs
//! call out: "AU-side consumption ... the separate Wave-2 workstream."
//!
//! `PlacementRoute` is always in the `Method` enum (pure serde — no heavy dep, see
//! `protocol.rs`), exactly like the M3 catalog methods (`CatalogList`/`Reshard`/…)
//! `handlers/admin.rs` already routes. It self-routes in `dispatch.rs` BEFORE the
//! per-graph chain (not graph-scoped — the catalog is cluster-wide), so this module
//! ships two `try_handle`s selected by the `raft` feature, same shape as
//! `handlers::admin`:
//!
//! * `#[cfg(feature = "raft")]` — the real answer. If no `MultiRaft` cluster is
//!   running on this node (`state.multi_raft` is `None` — the common single-engine
//!   zero-infra case), that is NOT an error: it is answered exactly like an empty
//!   catalog (`{"explicit": false}`), because a single-engine deployment has no
//!   OTHER group to route to — every caller's existing hash-ring fallback is
//!   correct there by construction.
//! * `#[cfg(not(feature = "raft"))]` — a build with no Raft support at all answers
//!   the SAME `{"explicit": false}` JSON (not a hard error) so a caller's fallback
//!   path triggers identically regardless of which of these three states it hit —
//!   "unreachable/unsupported catalog" and "reachable catalog with nothing placed
//!   yet" are indistinguishable ON PURPOSE (see `epistemic_graph.client`'s
//!   `PlacementClient.route` docstring and AU's `placement_catalog.py` module doc).
//!
//! ## The `endpoint` field is deliberately `null`
//!
//! The catalog resolves a `(tenant, sub_key)` to a Raft [`crate::raft::GroupId`] +
//! routing epoch — an INTERNAL replication-group identity, not a client-facing
//! network address. Mapping a `GroupId` to a connectable `host:port` is deployment
//! topology the engine does not own here (a multi-group cluster may run every group
//! in ONE process behind one client-facing listener, or a future topology could
//! split them across processes) — so the wire answer surfaces `group`/`epoch` and
//! leaves `endpoint` resolution to the caller's own topology config (e.g. AU's
//! `GRAPH_SERVICE_ENDPOINTS`/`shard_topology`). This is a documented, justified
//! partial: real + tested for the group/epoch/redirect routing answer the catalog
//! actually authorities over; endpoint-mapping is deployment-topology, not a
//! catalog concern.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Method, Response};
use crate::server::state::ServerState;

/// Route `Method::PlacementRoute` (CONCEPT:EG-KG.sharding.placement-route-rpc). `Ok(resp)` = handled;
/// `Err(method)` = not a placement method (unreachable — the dispatch arm only routes
/// `PlacementRoute` here).
#[cfg(feature = "raft")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    use crate::protocol::ResultPayload;
    use crate::raft::placement::RouteOutcome;

    let Method::PlacementRoute {
        tenant,
        sub_key,
        client_epoch,
    } = method
    else {
        return Err(method);
    };

    let multi = { state.read().await.multi_raft.clone() };
    let Some(multi) = multi else {
        // No live MultiRaft cluster on this node -- a single-engine zero-infra
        // deployment. There is no other group to route to, so this is the SAME
        // well-formed "nothing explicit" answer an empty catalog gives, not an error.
        return Ok(no_explicit_placement(req_id));
    };

    match multi.placement().route(&tenant, &sub_key).await {
        RouteOutcome::Explicit(r) => {
            // Mirrors `PlacementCatalog::redirect_if_stale`'s exact condition, computed
            // off the SAME route answer instead of a second catalog scan: a caller
            // presenting an epoch behind the entry's current one (including a fresh
            // caller's `client_epoch = 0`) is redirected to the fresh (group, epoch).
            let redirect = client_epoch < r.epoch;
            Ok(Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "explicit": true,
                    "group": r.group,
                    "epoch": r.epoch,
                    "redirect": redirect,
                    // Deliberately null -- see the module docstring's "`endpoint` is
                    // deliberately null" section: group->endpoint is caller topology.
                    "endpoint": serde_json::Value::Null,
                })),
            ))
        }
        RouteOutcome::Fallback => Ok(no_explicit_placement(req_id)),
    }
}

#[cfg(feature = "raft")]
fn no_explicit_placement(req_id: u64) -> Response {
    Response::ok(
        req_id,
        crate::protocol::ResultPayload::Json(serde_json::json!({ "explicit": false })),
    )
}

/// Non-raft build: `PlacementRoute` answers the SAME well-formed "no explicit
/// placement" JSON a raft build with an empty/absent catalog gives (not an error) --
/// see the module docstring for why this is deliberate, not a lesser fallback.
#[cfg(not(feature = "raft"))]
pub(crate) async fn try_handle(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    use crate::protocol::ResultPayload;
    match method {
        Method::PlacementRoute { .. } => Ok(Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({ "explicit": false })),
        )),
        other => Err(other),
    }
}
