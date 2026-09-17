//! The steps of one [`super::CrossShardReader::read`] page: the placement
//! barrier, route resolution, bounded leg fan-out, and per-leg reply/cursor
//! assembly.
//!
//! Split out of `xread.rs` (CCCC burn-down lane L-raft-b): `read` was one
//! 200-line body, and adding its named steps to the parent would have pushed
//! that file past the KISS whole-file line threshold.

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tokio::time::Instant;

use super::{
    fetch_leg, CrossGraphLegCursor, CrossGraphReadError, CrossGraphReadErrorCode,
    CrossGraphReadRequest, MultiRaft, ReadLeg, ReadLegStatus, ReadPageError, ReadPageErrorCode,
    ReadPageReply, RouteToken,
};

/// One leg still to fetch: its input position, graph, current route, and the
/// cursor it continues from.
pub(super) struct PendingLeg {
    index: usize,
    graph_name: String,
    route: RouteToken,
    cursor: CrossGraphLegCursor,
}

fn placement_unavailable(code: ReadPageErrorCode) -> CrossGraphReadError {
    CrossGraphReadError {
        code: CrossGraphReadErrorCode::PlacementUnavailable,
        failed_legs: vec![("placement".to_string(), code)],
    }
}

/// Linearizable barrier on the placement group before any route is resolved.
pub(super) async fn placement_barrier(
    multi: &MultiRaft,
    deadline: Instant,
) -> Result<(), CrossGraphReadError> {
    tokio::time::timeout_at(
        deadline,
        multi.read_barrier_group(super::super::DEFAULT_GROUP),
    )
    .await
    .unwrap_or_else(|_| Err(ReadPageError::new(ReadPageErrorCode::DeadlineExceeded)))
    .map(|_| ())
    .map_err(|error| placement_unavailable(error.code))
}

/// The continuation cursor for every input leg, or a fresh cursor per graph.
pub(super) fn prior_leg_cursors(request: &CrossGraphReadRequest) -> Vec<CrossGraphLegCursor> {
    match &request.cursor {
        Some(cursor) => cursor.legs.clone(),
        None => request
            .graph_names
            .iter()
            .map(|graph_name| CrossGraphLegCursor {
                graph_name: graph_name.clone(),
                route: RouteToken { group: 0, epoch: 0 },
                after_node_id: None,
                snapshot_version: None,
                complete: false,
            })
            .collect(),
    }
}

/// The complete route vector from one placement snapshot, within the deadline.
pub(super) async fn resolve_routes(
    multi: &MultiRaft,
    graph_names: &[String],
    deadline: Instant,
) -> Result<Vec<RouteToken>, CrossGraphReadError> {
    let routes = tokio::time::timeout_at(deadline, multi.route_graphs(graph_names))
        .await
        .map_err(|_| placement_unavailable(ReadPageErrorCode::DeadlineExceeded))?;
    Ok(routes.into_iter().map(RouteToken::from).collect())
}

/// Every leg whose prior cursor is not yet complete.
pub(super) fn pending_legs(
    graph_names: &[String],
    routes: &[RouteToken],
    prior: &[CrossGraphLegCursor],
) -> Vec<PendingLeg> {
    graph_names
        .iter()
        .enumerate()
        .filter(|(index, _)| !prior[*index].complete)
        .map(|(index, graph_name)| PendingLeg {
            index,
            graph_name: graph_name.clone(),
            route: routes[index],
            cursor: prior[index].clone(),
        })
        .collect()
}

/// Fetch one leg under the shared deadline.
async fn fetch_pending_leg(
    multi: Arc<MultiRaft>,
    leg: PendingLeg,
    limit: u32,
    max_bytes: u32,
    deadline: Instant,
) -> (usize, Result<ReadPageReply, ReadPageError>) {
    let operation = fetch_leg(
        multi,
        leg.graph_name,
        leg.route,
        leg.cursor.after_node_id,
        leg.cursor.snapshot_version,
        limit,
        max_bytes,
    );
    let result = tokio::time::timeout_at(deadline, operation)
        .await
        .unwrap_or_else(|_| Err(ReadPageError::new(ReadPageErrorCode::DeadlineExceeded)));
    (leg.index, result)
}

/// Fan out every pending leg with the caller's fan-out bound. The aggregate
/// byte budget is divided deterministically: each leg gets the even share and
/// the first `remainder` legs one extra byte.
pub(super) async fn fetch_pending_legs(
    multi: Arc<MultiRaft>,
    request: &CrossGraphReadRequest,
    work: Vec<PendingLeg>,
    deadline: Instant,
) -> Vec<(usize, Result<ReadPageReply, ReadPageError>)> {
    let limit = request.page_size;
    let active_legs = work.len().max(1);
    let byte_base = request.max_response_bytes as usize / active_legs;
    let byte_remainder = request.max_response_bytes as usize % active_legs;
    stream::iter(work.into_iter().enumerate())
        .map(|(slot, leg)| {
            let max_bytes = (byte_base + usize::from(slot < byte_remainder)) as u32;
            fetch_pending_leg(multi.clone(), leg, limit, max_bytes, deadline)
        })
        .buffer_unordered(request.max_fanout as usize)
        .collect()
        .await
}

/// Fetched replies and failures, indexed by input leg.
pub(super) struct LegOutcomes {
    pub(super) replies: Vec<Option<ReadPageReply>>,
    failures: Vec<Option<ReadPageError>>,
}

impl LegOutcomes {
    /// Index the fetched results; a successful leg's reply route becomes that
    /// leg's current route.
    pub(super) fn collect(
        fetched: Vec<(usize, Result<ReadPageReply, ReadPageError>)>,
        routes: &mut [RouteToken],
    ) -> Self {
        let mut replies: Vec<Option<ReadPageReply>> = vec![None; routes.len()];
        let mut failures: Vec<Option<ReadPageError>> = vec![None; routes.len()];
        for (index, result) in fetched {
            match result {
                Ok(reply) => {
                    routes[index] = reply.route;
                    replies[index] = Some(reply);
                }
                Err(error) => failures[index] = Some(error),
            }
        }
        Self { replies, failures }
    }

    pub(super) fn failed_legs(&self, graph_names: &[String]) -> Vec<(String, ReadPageErrorCode)> {
        self.failures
            .iter()
            .enumerate()
            .filter_map(|(index, error)| {
                error
                    .as_ref()
                    .map(|error| (graph_names[index].clone(), error.code))
            })
            .collect()
    }
}

/// A leg that was already complete before this page.
fn complete_leg(previous: &CrossGraphLegCursor) -> (ReadLeg, CrossGraphLegCursor) {
    let leg = ReadLeg {
        graph_name: previous.graph_name.clone(),
        route: previous.route,
        raft_barrier_index: None,
        snapshot_version: previous.snapshot_version,
        rows_consumed: 0,
        status: ReadLegStatus::Complete,
    };
    (leg, previous.clone())
}

/// A failed leg retains its prior position, re-pointed at the freshest route.
fn failed_leg(
    previous: &CrossGraphLegCursor,
    error: &ReadPageError,
    route: RouteToken,
) -> (ReadLeg, CrossGraphLegCursor) {
    let mut retained = previous.clone();
    retained.route = error.current_route.unwrap_or(route);
    let leg = ReadLeg {
        graph_name: retained.graph_name.clone(),
        route: retained.route,
        raft_barrier_index: None,
        snapshot_version: retained.snapshot_version,
        rows_consumed: 0,
        status: ReadLegStatus::Failed(error.code),
    };
    (leg, retained)
}

/// A replied leg advances its cursor past the rows the merge consumed.
fn replied_leg(
    previous: &CrossGraphLegCursor,
    reply: &ReadPageReply,
    used: usize,
) -> (ReadLeg, CrossGraphLegCursor) {
    let complete = used == reply.nodes.len() && !reply.has_more;
    let after_node_id = match used {
        0 => previous.after_node_id.clone(),
        _ => Some(reply.nodes[used - 1].0.clone()),
    };
    let next = CrossGraphLegCursor {
        graph_name: reply.graph_name.clone(),
        route: reply.route,
        after_node_id,
        snapshot_version: Some(reply.snapshot_version),
        complete,
    };
    let leg = ReadLeg {
        graph_name: reply.graph_name.clone(),
        route: reply.route,
        raft_barrier_index: Some(reply.raft_barrier_index),
        snapshot_version: Some(reply.snapshot_version),
        rows_consumed: used as u32,
        status: if complete {
            ReadLegStatus::Complete
        } else {
            ReadLegStatus::More
        },
    };
    (leg, next)
}

/// The reply leg and continuation cursor for every input leg, in input order.
pub(super) fn assemble_legs(
    prior: &[CrossGraphLegCursor],
    outcomes: &LegOutcomes,
    routes: &[RouteToken],
    consumed: &[usize],
) -> (Vec<ReadLeg>, Vec<CrossGraphLegCursor>) {
    prior
        .iter()
        .enumerate()
        .map(|(index, previous)| {
            if previous.complete {
                return complete_leg(previous);
            }
            if let Some(error) = &outcomes.failures[index] {
                return failed_leg(previous, error, routes[index]);
            }
            let reply = outcomes.replies[index]
                .as_ref()
                .expect("successful leg has reply");
            replied_leg(previous, reply, consumed[index])
        })
        .unzip()
}
