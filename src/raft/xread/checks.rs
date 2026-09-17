//! Bound and shape predicates for cross-group read requests and peer replies.
//!
//! Split out of `xread.rs` (CCCC burn-down lane L-raft-b) so
//! `CrossGraphReadRequest::validate` and `ReadPageReply::validate_for` read as
//! the conjunction of named rules instead of one long disjunction, without
//! growing the parent past the KISS whole-file line threshold.

use std::collections::HashSet;

use super::{
    CrossGraphCursor, CrossGraphReadRequest, ReadPageReply, ReadPageRequest,
    MAX_CROSS_GRAPH_FANOUT, MAX_CROSS_GRAPH_LEGS, MAX_CROSS_GRAPH_PAGE_ROWS,
    MAX_CROSS_GRAPH_RESPONSE_BYTES, MAX_CROSS_GRAPH_TIMEOUT_MS, MAX_GRAPH_NAME_BYTES,
};

/// A graph or node identifier: non-empty, bounded, and free of NUL bytes.
pub(super) fn identifier_is_valid(identifier: &str) -> bool {
    !identifier.is_empty()
        && identifier.len() <= MAX_GRAPH_NAME_BYTES
        && !identifier.bytes().any(|byte| byte == 0)
}

/// Leg count, page size, and the aggregate byte budget (which must give every
/// leg at least one byte).
pub(super) fn page_limits_are_valid(request: &CrossGraphReadRequest) -> bool {
    let legs = request.graph_names.len();
    let max_bytes = request.max_response_bytes as usize;
    (1..=MAX_CROSS_GRAPH_LEGS).contains(&legs)
        && (1..=MAX_CROSS_GRAPH_PAGE_ROWS).contains(&(request.page_size as usize))
        && (legs..=MAX_CROSS_GRAPH_RESPONSE_BYTES).contains(&max_bytes)
}

pub(super) fn fanout_and_timeout_are_valid(request: &CrossGraphReadRequest) -> bool {
    (1..=MAX_CROSS_GRAPH_FANOUT).contains(&(request.max_fanout as usize))
        && (1..=MAX_CROSS_GRAPH_TIMEOUT_MS).contains(&request.timeout_ms)
}

/// Every requested graph name is a valid identifier and appears once.
pub(super) fn graph_names_are_unique_and_valid(graph_names: &[String]) -> bool {
    let mut unique = HashSet::with_capacity(graph_names.len());
    graph_names
        .iter()
        .all(|name| identifier_is_valid(name) && unique.insert(name.as_str()))
}

/// A continuation cursor retains exactly the requested graph vector, in order,
/// and every retained keyset position is a valid node identifier.
pub(super) fn cursor_matches_graphs(cursor: &CrossGraphCursor, graph_names: &[String]) -> bool {
    cursor.legs.len() == graph_names.len()
        && cursor.legs.iter().zip(graph_names).all(|(leg, name)| {
            leg.graph_name == *name && leg.after_node_id.as_deref().is_none_or(identifier_is_valid)
        })
}

/// The reply answers exactly this request: same graph, same route, and the
/// pinned snapshot version when one was requested.
pub(super) fn reply_echoes_request(reply: &ReadPageReply, request: &ReadPageRequest) -> bool {
    reply.graph_name == request.graph_name
        && reply.route == request.route
        && request
            .expected_snapshot_version
            .is_none_or(|expected| expected == reply.snapshot_version)
}

/// The reply stays within the requested row and byte bounds with valid ids.
pub(super) fn reply_rows_are_bounded(reply: &ReadPageReply, request: &ReadPageRequest) -> bool {
    let bytes = reply
        .nodes
        .iter()
        .try_fold(0usize, |total, (id, properties)| {
            total.checked_add(id.len())?.checked_add(properties.len())
        });
    reply.nodes.len() <= request.limit as usize
        && reply
            .nodes
            .iter()
            .all(|(node_id, _)| identifier_is_valid(node_id))
        && bytes.is_some_and(|bytes| bytes <= request.max_bytes as usize)
}

/// Keyset pagination is strictly ordered, advances past the request cursor,
/// names its own last row as the next cursor, and never claims more rows
/// after an empty page.
pub(super) fn reply_pagination_is_consistent(
    reply: &ReadPageReply,
    request: &ReadPageRequest,
) -> bool {
    let sorted = reply.nodes.windows(2).all(|rows| rows[0].0 < rows[1].0);
    let advanced = request
        .after_node_id
        .as_ref()
        .is_none_or(|after| reply.nodes.first().is_none_or(|(first, _)| first > after));
    sorted
        && advanced
        && reply.next_after_node_id.as_ref() == reply.nodes.last().map(|(node_id, _)| node_id)
        && !(reply.has_more && reply.nodes.is_empty())
}
