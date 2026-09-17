//! Bounded, placement-fenced cross-group reads over the authenticated Raft channel.
//!
//! A cross-group read is a collection of **per-group linearizable pages**.  It is
//! deliberately not described as a global snapshot: independent Raft groups do not
//! share a commit index.  Every leg reports its own Raft barrier and authoritative
//! graph version, and a continuation fails if that version changed.  Callers that
//! need a true global snapshot must use a separately replicated global read fence;
//! this module never weakens that contract by relabelling independent barriers.
//!
//! The coordinator resolves the complete route vector only after a linearizable
//! placement-catalog barrier, fans out with a caller-visible bound, and sends remote
//! legs through the same authenticated [`super::network::PeerPool`] and multiplexed
//! [`super::network::GroupRpc`] listener as consensus traffic.  Replies are merged by
//! `(node_id, input-leg-order)`, so pagination is deterministic on every node.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use openraft::ReadPolicy;
use serde::{Deserialize, Serialize};

use super::multi::{GroupRouter, MultiRaft};
use super::placement::{PlacementCatalog, PlacementRoute};
use super::{EgRaft, GroupId};
use crate::server::persistence::PersistenceBackend;

mod assemble;
mod checks;

pub const MAX_CROSS_GRAPH_LEGS: usize = 256;
pub const MAX_CROSS_GRAPH_FANOUT: usize = 32;
pub const MAX_CROSS_GRAPH_PAGE_ROWS: usize = 4_096;
pub const MAX_CROSS_GRAPH_TIMEOUT_MS: u64 = 30_000;
pub const MAX_CROSS_GRAPH_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_GRAPH_NAME_BYTES: usize = 4_096;
const MAX_ROUTE_RETRIES: usize = 1;

/// The only consistency level currently served.  It promises a linearizable read
/// **inside each group**, never an atomic snapshot spanning groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossGraphConsistency {
    PerGroupLinearizable,
}

/// Whether any failed leg invalidates the whole result or is represented explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionPolicy {
    RequireComplete,
    AllowPartial,
}

/// An epoch'd placement token.  Both fields are required to fence a move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteToken {
    pub group: GroupId,
    pub epoch: u64,
}

impl From<PlacementRoute> for RouteToken {
    fn from(value: PlacementRoute) -> Self {
        Self {
            group: value.group,
            epoch: value.epoch,
        }
    }
}

/// Cursor for one graph.  A cursor always retains every input leg so a caller cannot
/// accidentally turn a partial continuation into a complete result by dropping it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossGraphLegCursor {
    pub graph_name: String,
    pub route: RouteToken,
    pub after_node_id: Option<String>,
    pub snapshot_version: Option<u64>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossGraphCursor {
    pub legs: Vec<CrossGraphLegCursor>,
}

/// Public coordinator request.  Every resource dimension is explicitly bounded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossGraphReadRequest {
    pub graph_names: Vec<String>,
    pub consistency: CrossGraphConsistency,
    pub completion: CompletionPolicy,
    pub page_size: u32,
    /// Aggregate uncompressed row-byte budget across every active leg, not a
    /// per-leg multiplier. The coordinator divides it deterministically.
    pub max_response_bytes: u32,
    pub max_fanout: u16,
    pub timeout_ms: u64,
    pub cursor: Option<CrossGraphCursor>,
}

impl CrossGraphReadRequest {
    pub fn first_page(graph_names: Vec<String>, page_size: u32) -> Self {
        Self {
            graph_names,
            consistency: CrossGraphConsistency::PerGroupLinearizable,
            completion: CompletionPolicy::RequireComplete,
            page_size,
            max_response_bytes: (4 * 1024 * 1024) as u32,
            max_fanout: 8,
            timeout_ms: 10_000,
            cursor: None,
        }
    }

    fn validate(&self) -> Result<(), CrossGraphReadError> {
        let valid = checks::page_limits_are_valid(self)
            && checks::fanout_and_timeout_are_valid(self)
            && checks::graph_names_are_unique_and_valid(&self.graph_names)
            && self
                .cursor
                .as_ref()
                .is_none_or(|cursor| checks::cursor_matches_graphs(cursor, &self.graph_names));
        if !valid {
            return Err(CrossGraphReadError::invalid());
        }
        Ok(())
    }
}

/// Internal wire request for one graph page.  This can only arrive over the Raft
/// peer listener, whose handshake authenticates and encrypts every frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPageRequest {
    pub graph_name: String,
    pub route: RouteToken,
    pub after_node_id: Option<String>,
    pub expected_snapshot_version: Option<u64>,
    pub limit: u32,
    pub max_bytes: u32,
}

impl ReadPageRequest {
    pub(crate) fn validate(&self, group: GroupId) -> Result<(), ReadPageError> {
        if !checks::identifier_is_valid(&self.graph_name)
            || self.route.group != group
            || self.limit == 0
            || self.limit as usize > MAX_CROSS_GRAPH_PAGE_ROWS
            || self.max_bytes == 0
            || self.max_bytes as usize > MAX_CROSS_GRAPH_RESPONSE_BYTES
            || !self
                .after_node_id
                .as_deref()
                .is_none_or(checks::identifier_is_valid)
        {
            return Err(ReadPageError::new(ReadPageErrorCode::InvalidRequest));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadPageErrorCode {
    InvalidRequest,
    GroupUnavailable,
    NoLeader,
    BarrierFailed,
    StaleRoute,
    SnapshotChanged,
    StorageUnavailable,
    ResponseTooLarge,
    TransportFailed,
    DeadlineExceeded,
    InvalidResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPageError {
    pub code: ReadPageErrorCode,
    pub current_route: Option<RouteToken>,
}

impl ReadPageError {
    pub(crate) fn new(code: ReadPageErrorCode) -> Self {
        Self {
            code,
            current_route: None,
        }
    }

    fn stale(route: PlacementRoute) -> Self {
        Self {
            code: ReadPageErrorCode::StaleRoute,
            current_route: Some(route.into()),
        }
    }
}

/// One durable, keyset-bounded page.  `snapshot_version` is the authoritative graph
/// version sampled by the redb MVCC transaction; `raft_barrier_index` is the per-group
/// ReadIndex that preceded it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadPageReply {
    pub graph_name: String,
    pub route: RouteToken,
    pub nodes: Vec<(String, Vec<u8>)>,
    pub has_more: bool,
    pub next_after_node_id: Option<String>,
    pub snapshot_version: u64,
    pub raft_barrier_index: u64,
}

impl ReadPageReply {
    /// Treat a peer reply as untrusted typed input even after transport
    /// authentication. This prevents a faulty member from bypassing the requested
    /// row/byte bounds or corrupting keyset pagination.
    pub(crate) fn validate_for(
        &self,
        request: &ReadPageRequest,
        group: GroupId,
    ) -> Result<(), ReadPageError> {
        request.validate(group)?;
        if !checks::reply_echoes_request(self, request)
            || !checks::reply_rows_are_bounded(self, request)
            || !checks::reply_pagination_is_consistent(self, request)
        {
            return Err(ReadPageError::new(ReadPageErrorCode::InvalidResponse));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadLegStatus {
    Complete,
    More,
    Failed(ReadPageErrorCode),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadLeg {
    pub graph_name: String,
    pub route: RouteToken,
    pub raft_barrier_index: Option<u64>,
    pub snapshot_version: Option<u64>,
    pub rows_consumed: u32,
    pub status: ReadLegStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossGraphReadReply {
    pub consistency: CrossGraphConsistency,
    pub legs: Vec<ReadLeg>,
    pub merged: Vec<(String, Vec<u8>)>,
    pub cursor: Option<CrossGraphCursor>,
    pub complete: bool,
    pub partial: bool,
}

impl CrossGraphReadReply {
    pub fn groups_spanned(&self) -> BTreeSet<GroupId> {
        self.legs.iter().map(|leg| leg.route.group).collect()
    }

    pub fn is_cross_shard(&self) -> bool {
        self.groups_spanned().len() >= 2
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossGraphReadErrorCode {
    InvalidRequest,
    PlacementUnavailable,
    RequiredLegFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossGraphReadError {
    pub code: CrossGraphReadErrorCode,
    pub failed_legs: Vec<(String, ReadPageErrorCode)>,
}

impl CrossGraphReadError {
    fn invalid() -> Self {
        Self {
            code: CrossGraphReadErrorCode::InvalidRequest,
            failed_legs: Vec::new(),
        }
    }
}

impl fmt::Display for CrossGraphReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "cross-graph read failed: {:?}", self.code)
    }
}

impl std::error::Error for CrossGraphReadError {}

/// Immutable dependencies used by the authenticated listener's read-page handler.
#[derive(Clone)]
pub(crate) struct ReadPageService {
    backend: Arc<dyn PersistenceBackend>,
    placement: Arc<PlacementCatalog>,
    router: Arc<GroupRouter>,
}

impl ReadPageService {
    pub(crate) fn new(
        backend: Arc<dyn PersistenceBackend>,
        placement: Arc<PlacementCatalog>,
        router: Arc<GroupRouter>,
    ) -> Self {
        Self {
            backend,
            placement,
            router,
        }
    }

    pub(crate) async fn read_page(
        &self,
        raft: EgRaft,
        group: GroupId,
        request: ReadPageRequest,
    ) -> Result<ReadPageReply, ReadPageError> {
        request.validate(group)?;
        let barrier = linearizable_barrier(&raft).await?;

        let (tenant, sub_key) = super::placement::split_tenant_key(&request.graph_name);
        let current = self
            .placement
            .route(tenant, sub_key, self.router.group_of(&request.graph_name))
            .await;
        if RouteToken::from(current) != request.route {
            return Err(ReadPageError::stale(current));
        }

        let backend = self.backend.clone();
        let graph_fname = crate::persist::sanitize(&request.graph_name);
        let after = request.after_node_id.clone();
        let limit = request.limit as usize;
        let max_bytes = request.max_bytes as usize;
        let material = tokio::task::spawn_blocking(move || {
            let cursor = after.map(|node_id| crate::registry::MaterializeCursor {
                node_after: Some(node_id),
                ..Default::default()
            });
            backend.read_graph_material_page_blocking(&graph_fname, cursor, limit + 1)
        })
        .await
        .map_err(|_| ReadPageError::new(ReadPageErrorCode::StorageUnavailable))?
        .map_err(|_| ReadPageError::new(ReadPageErrorCode::StorageUnavailable))?;

        let (mut nodes, snapshot_version) = match material {
            Some(page) => (
                page.nodes,
                page.source_snapshot_version
                    .ok_or_else(|| ReadPageError::new(ReadPageErrorCode::StorageUnavailable))?,
            ),
            None => (Vec::new(), 0),
        };
        if request
            .expected_snapshot_version
            .is_some_and(|expected| expected != snapshot_version)
        {
            return Err(ReadPageError::new(ReadPageErrorCode::SnapshotChanged));
        }

        let mut encoded_bytes = 0usize;
        let mut byte_limited = false;
        let mut retained = 0usize;
        for (node_id, properties) in &nodes {
            let row_bytes = node_id.len().saturating_add(properties.len());
            if row_bytes > max_bytes {
                return Err(ReadPageError::new(ReadPageErrorCode::ResponseTooLarge));
            }
            if retained == limit || encoded_bytes.saturating_add(row_bytes) > max_bytes {
                byte_limited = true;
                break;
            }
            encoded_bytes += row_bytes;
            retained += 1;
        }
        let has_more = byte_limited || nodes.len() > retained;
        nodes.truncate(retained);
        let next_after_node_id = nodes.last().map(|(node_id, _)| node_id.clone());
        Ok(ReadPageReply {
            graph_name: request.graph_name,
            route: request.route,
            nodes,
            has_more,
            next_after_node_id,
            snapshot_version,
            raft_barrier_index: barrier,
        })
    }
}

pub(crate) async fn linearizable_barrier(raft: &EgRaft) -> Result<u64, ReadPageError> {
    raft.ensure_linearizable(ReadPolicy::ReadIndex)
        .await
        .map(|log_id| log_id.map_or(0, |id| id.index))
        .map_err(|_| ReadPageError::new(ReadPageErrorCode::BarrierFailed))
}

pub struct CrossShardReader {
    multi: Arc<MultiRaft>,
}

impl CrossShardReader {
    pub fn new(multi: Arc<MultiRaft>) -> Self {
        Self { multi }
    }

    /// Execute one deterministic page.  The placement barrier, route retries, leg
    /// deadlines, and partial-result policy are all part of this single operation.
    pub async fn read(
        &self,
        request: CrossGraphReadRequest,
    ) -> Result<CrossGraphReadReply, CrossGraphReadError> {
        request.validate()?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(request.timeout_ms);
        assemble::placement_barrier(&self.multi, deadline).await?;
        let prior = assemble::prior_leg_cursors(&request);
        let mut routes =
            assemble::resolve_routes(&self.multi, &request.graph_names, deadline).await?;
        let work = assemble::pending_legs(&request.graph_names, &routes, &prior);
        let fetched =
            assemble::fetch_pending_legs(self.multi.clone(), &request, work, deadline).await;
        let outcomes = assemble::LegOutcomes::collect(fetched, &mut routes);

        let failed_legs = outcomes.failed_legs(&request.graph_names);
        if request.completion == CompletionPolicy::RequireComplete && !failed_legs.is_empty() {
            return Err(CrossGraphReadError {
                code: CrossGraphReadErrorCode::RequiredLegFailed,
                failed_legs,
            });
        }

        let (merged, consumed) = deterministic_merge(&outcomes.replies, request.page_size as usize);
        let (legs, cursor_legs) = assemble::assemble_legs(&prior, &outcomes, &routes, &consumed);
        let complete = cursor_legs.iter().all(|leg| leg.complete);
        Ok(CrossGraphReadReply {
            consistency: request.consistency,
            legs,
            merged,
            cursor: (!complete).then_some(CrossGraphCursor { legs: cursor_legs }),
            complete,
            partial: !failed_legs.is_empty(),
        })
    }
}

async fn fetch_leg(
    multi: Arc<MultiRaft>,
    graph_name: String,
    mut route: RouteToken,
    after_node_id: Option<String>,
    snapshot_version: Option<u64>,
    limit: u32,
    max_bytes: u32,
) -> Result<ReadPageReply, ReadPageError> {
    for attempt in 0..=MAX_ROUTE_RETRIES {
        let request = ReadPageRequest {
            graph_name: graph_name.clone(),
            route,
            after_node_id: after_node_id.clone(),
            expected_snapshot_version: snapshot_version,
            limit,
            max_bytes,
        };
        match multi.read_page_group(route.group, request).await {
            Ok(reply) => {
                multi.read_barrier_group(super::DEFAULT_GROUP).await?;
                let current: RouteToken = multi.route_graph(&graph_name).await.into();
                if current == reply.route {
                    return Ok(reply);
                }
                route = current;
            }
            Err(error)
                if matches!(
                    error.code,
                    ReadPageErrorCode::StaleRoute | ReadPageErrorCode::GroupUnavailable
                ) && attempt < MAX_ROUTE_RETRIES =>
            {
                multi.read_barrier_group(super::DEFAULT_GROUP).await?;
                let refreshed: RouteToken = multi.route_graph(&graph_name).await.into();
                if refreshed == route && error.code == ReadPageErrorCode::GroupUnavailable {
                    return Err(error);
                }
                route = refreshed;
            }
            Err(error) => return Err(error),
        }
    }
    Err(ReadPageError::new(ReadPageErrorCode::StaleRoute))
}

/// K-way merge sorted leg pages.  Every duplicate id is consumed from all legs before
/// the page boundary, preventing it from reappearing on the next continuation.
fn deterministic_merge(
    replies: &[Option<ReadPageReply>],
    page_size: usize,
) -> (Vec<(String, Vec<u8>)>, Vec<usize>) {
    let mut heap: BinaryHeap<Reverse<(String, usize, usize)>> = BinaryHeap::new();
    let mut consumed = vec![0usize; replies.len()];
    for (leg, reply) in replies.iter().enumerate() {
        if let Some((node_id, _)) = reply.as_ref().and_then(|reply| reply.nodes.first()) {
            heap.push(Reverse((node_id.clone(), leg, 0)));
        }
    }

    let mut merged = Vec::with_capacity(page_size);
    while let Some(Reverse((node_id, leg, row))) = heap.pop() {
        let winner = (leg, row);
        let mut duplicates = vec![(leg, row)];
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _, _))| candidate == &node_id)
        {
            let Reverse((_, duplicate_leg, duplicate_row)) = heap.pop().unwrap();
            duplicates.push((duplicate_leg, duplicate_row));
        }
        duplicates.sort_unstable();
        let properties = replies[winner.0]
            .as_ref()
            .and_then(|reply| reply.nodes.get(winner.1))
            .map(|(_, properties)| properties.clone())
            .expect("heap row exists");
        merged.push((node_id, properties));

        for (duplicate_leg, duplicate_row) in duplicates {
            consumed[duplicate_leg] = duplicate_row + 1;
            if let Some((next_id, _)) = replies[duplicate_leg]
                .as_ref()
                .and_then(|reply| reply.nodes.get(duplicate_row + 1))
            {
                heap.push(Reverse((next_id.clone(), duplicate_leg, duplicate_row + 1)));
            }
        }
        if merged.len() == page_size {
            break;
        }
    }
    (merged, consumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(graph: &str, group: GroupId, ids: &[&str], has_more: bool) -> ReadPageReply {
        ReadPageReply {
            graph_name: graph.to_string(),
            route: RouteToken { group, epoch: 1 },
            nodes: ids
                .iter()
                .map(|id| ((*id).to_string(), id.as_bytes().to_vec()))
                .collect(),
            has_more,
            next_after_node_id: ids.last().map(|id| (*id).to_string()),
            snapshot_version: 7,
            raft_barrier_index: 9,
        }
    }

    #[test]
    fn request_rejects_unbounded_or_duplicate_legs() {
        let mut request = CrossGraphReadRequest::first_page(vec!["a".into(), "a".into()], 10);
        assert_eq!(
            request.validate().unwrap_err().code,
            CrossGraphReadErrorCode::InvalidRequest
        );
        request.graph_names = vec!["a".into()];
        request.max_fanout = 0;
        assert!(request.validate().is_err());
        request.max_fanout = 1;
        request.timeout_ms = MAX_CROSS_GRAPH_TIMEOUT_MS + 1;
        assert!(request.validate().is_err());
        request.timeout_ms = 1;
        request.max_response_bytes = (MAX_CROSS_GRAPH_RESPONSE_BYTES + 1) as u32;
        assert!(request.validate().is_err());
    }

    #[test]
    fn deterministic_merge_consumes_duplicates_before_page_boundary() {
        let replies = vec![
            Some(reply("a", 1, &["a", "c", "d"], true)),
            Some(reply("b", 2, &["b", "c", "e"], true)),
        ];
        let (merged, consumed) = deterministic_merge(&replies, 3);
        assert_eq!(
            merged.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(consumed, vec![2, 2], "both copies of c advance");
    }

    #[test]
    fn cursor_must_match_the_exact_graph_vector() {
        let mut request = CrossGraphReadRequest::first_page(vec!["a".into()], 1);
        request.cursor = Some(CrossGraphCursor {
            legs: vec![CrossGraphLegCursor {
                graph_name: "b".into(),
                route: RouteToken { group: 1, epoch: 1 },
                after_node_id: None,
                snapshot_version: None,
                complete: false,
            }],
        });
        assert!(request.validate().is_err());
    }

    #[test]
    fn peer_reply_must_preserve_bounds_and_keyset_order() {
        let request = ReadPageRequest {
            graph_name: "a".into(),
            route: RouteToken { group: 1, epoch: 1 },
            after_node_id: None,
            expected_snapshot_version: Some(7),
            limit: 2,
            max_bytes: 64,
        };
        let valid = reply("a", 1, &["a", "b"], false);
        assert!(valid.validate_for(&request, 1).is_ok());

        let mut unsorted = valid.clone();
        unsorted.nodes.swap(0, 1);
        assert_eq!(
            unsorted.validate_for(&request, 1).unwrap_err().code,
            ReadPageErrorCode::InvalidResponse
        );
        let mut oversized = valid;
        oversized.nodes[0].1 = vec![0; 65];
        assert_eq!(
            oversized.validate_for(&request, 1).unwrap_err().code,
            ReadPageErrorCode::InvalidResponse
        );
    }
}
