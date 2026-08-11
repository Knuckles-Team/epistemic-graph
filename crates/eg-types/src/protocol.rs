// CONCEPT:EG-KG.query.wire-protocol — Epistemic Graph Service Wire Protocol
//
// Length-prefixed MessagePack framing for UDS/TCP communication between
// the Python client and the Tokio service layer. Every request
// is authenticated via HMAC-SHA256.

use serde::{Deserialize, Serialize};

/// Deserialize an explicitly present nullable field.
///
/// Serde otherwise treats a missing `Option<T>` exactly like an explicit null,
/// which would silently admit an older wire shape after a current-only cutover.
fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// serde default for association-rule `min_support` (CONCEPT:EG-KG.mining.frequent-itemset-mining):
/// keep an itemset supported by ≥10% of transactions.
#[cfg(feature = "mining")]
fn default_min_support() -> f64 {
    0.1
}

/// serde default for association-rule `min_confidence` (CONCEPT:EG-KG.mining.frequent-itemset-mining):
/// keep a rule with ≥50% conditional probability.
#[cfg(feature = "mining")]
fn default_min_confidence() -> f64 {
    0.5
}

/// serde default for `Method::ResolveConflict::semantics` (EPI-P3-7) — the unique,
/// always-defined skeptical (grounded) Dung extension, matching every other
/// epistemic op's "narrowest, always-answerable default" convention (mirrors
/// `is_skeptically_accepted`'s own choice of grounded over preferred/stable).
#[cfg(feature = "epistemic")]
fn default_argumentation_semantics() -> String {
    "grounded".to_string()
}

/// Required execution authority for a Cypher statement.
///
/// The caller must declare whether a statement is a read or mutation. The
/// server parses the statement and rejects a mismatch, so a mutation can never
/// obtain read authorization by hiding a keyword in comments, literals, or an
/// unsupported clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CypherMode {
    Read,
    Write,
}

// ── Request ─────────────────────────────────────────────────────────────

/// Top-level request envelope sent by the Python client.
///
/// `auth_token` carries the current `eg2.` verified request context
/// (CONCEPT:EG-KG.security.signed-request-envelope, EG-P0-5). It binds the
/// request id, graph, method, body hash, effective ACL agent, roles, scopes,
/// active policy version, delegation chain, timestamp, nonce, and idempotency
/// key. The server validates audience, tenant, policy version, and durable
/// replay state before dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Monotonically increasing request ID for correlation.
    pub id: u64,
    /// Target graph name (e.g., "agent:planner", "__commons__", "channel:p2p:a:b").
    pub graph: String,
    /// Current `eg2.` verified request-context envelope.
    pub auth_token: String,
    /// Optional caller assertion. The server rejects a mismatch and replaces
    /// this value with the signed effective agent before authorization.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub agent_id: Option<String>,
    /// The operation to perform.
    #[serde(flatten)]
    pub method: Method,
}

/// Canonical byte encoding for a verified request-context envelope (v2).
///
/// The current envelope signs every request/replay binding plus the effective
/// agent, roles, scopes, policy version, and ordered delegation
/// chain.  Every scalar and list item is length-prefixed, and list lengths are
/// explicit, so distinct logical claim sets cannot share an encoding.
#[allow(clippy::too_many_arguments)]
pub fn build_envelope_v2_bytes(
    request_id: u64,
    graph: &str,
    method_name: &str,
    body_hash: &str,
    claims: &crate::acl::RequestContextClaims,
    timestamp: u64,
    nonce: &str,
    idempotency_key: &str,
) -> Vec<u8> {
    fn put(buf: &mut Vec<u8>, value: &str) {
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(value.as_bytes());
    }
    fn put_list(buf: &mut Vec<u8>, values: &[String]) {
        buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
        for value in values {
            put(buf, value);
        }
    }

    let mut buf = Vec::new();
    put(&mut buf, "eg-envelope-v2");
    buf.extend_from_slice(&request_id.to_be_bytes());
    put(&mut buf, graph);
    put(&mut buf, method_name);
    put(&mut buf, body_hash);
    put(&mut buf, &claims.principal);
    put(&mut buf, &claims.tenant);
    put(&mut buf, &claims.audience);
    put(&mut buf, &claims.agent_id);
    put_list(&mut buf, &claims.roles);
    put_list(&mut buf, &claims.scopes);
    put(&mut buf, &claims.policy_version);
    put_list(&mut buf, &claims.delegation);
    buf.extend_from_slice(&timestamp.to_be_bytes());
    put(&mut buf, nonce);
    put(&mut buf, idempotency_key);
    // ADR-3 / W1.9 node-bound envelopes (`reports/wave1/ADR-scale-trio.md`):
    // appended ONLY when the minting client set a target-node claim, so an
    // envelope from a client that predates node binding — an un-upgraded
    // Python client, or one of the non-Python `clients/{js,go}` bindings,
    // neither of which this change touches — encodes BYTE-FOR-BYTE
    // IDENTICALLY to before. This is what makes the wire change genuinely
    // additive rather than a breaking MAC-format bump: those clients keep
    // verifying with zero changes. Presence itself is MAC-covered (not just
    // the value), so a captured envelope's node claim can never be silently
    // stripped or retargeted to a different node without invalidating the MAC.
    if let Some(node) = claims.node.as_deref() {
        buf.push(1);
        put(&mut buf, node);
    }
    // W2.4 engine-native QoS lanes: the advisory admission-priority claim, MAC-
    // covered so it cannot be forged to jump the admission ordering. Appended as
    // a SECOND optional trailer with a DISTINCT tag byte (`2`, vs the node
    // trailer's `1`) so the two trailers stay mutually unambiguous — a
    // node-only envelope and a priority-only envelope can never collide onto the
    // same MAC input (a bare shared marker would let `node="x"` and
    // `priority="x"` sign identically). Presence-gated exactly like `node`, so an
    // envelope without a priority claim encodes byte-for-byte as before.
    if let Some(priority) = claims.priority.as_deref() {
        buf.push(2);
        put(&mut buf, priority);
    }
    buf
}

/// Canonical bytes for a detached administrative-operation signature scoped to
/// a verified context. `body` is the canonical `Method` encoding with its
/// signature field/list cleared, so the signature binds every operation
/// parameter without recursively signing itself.
pub fn build_context_operation_signature_bytes(
    domain: &str,
    claims: &crate::acl::RequestContextClaims,
    idempotency_key: &str,
    graph: &str,
    body: &[u8],
) -> Vec<u8> {
    fn put(buf: &mut Vec<u8>, bytes: &[u8]) {
        buf.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        buf.extend_from_slice(bytes);
    }
    fn put_list(buf: &mut Vec<u8>, values: &[String]) {
        put(buf, &(values.len() as u64).to_be_bytes());
        for value in values {
            put(buf, value.as_bytes());
        }
    }

    let mut buf = Vec::new();
    put(&mut buf, domain.as_bytes());
    put(&mut buf, claims.principal.as_bytes());
    put(&mut buf, claims.tenant.as_bytes());
    put(&mut buf, claims.audience.as_bytes());
    put(&mut buf, claims.agent_id.as_bytes());
    put_list(&mut buf, &claims.roles);
    put_list(&mut buf, &claims.scopes);
    put(&mut buf, claims.policy_version.as_bytes());
    put_list(&mut buf, &claims.delegation);
    put(&mut buf, idempotency_key.as_bytes());
    put(&mut buf, graph.as_bytes());
    put(&mut buf, body);
    buf
}

/// One operation on the placement-catalog admin surface (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc,
/// DIST-P2-5). Nested under `Method::PlacementAdmin { op }` — mirrors
/// [`crate::modality::ServedModalityOp`]'s "one Method variant, many operations" shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlacementAdminOp {
    /// Assign the WHOLE keyspace of `tenant` to `group` (the placement DECISION leg).
    /// Collapses any prior split. Returns `{"epoch": u64}` — the new routing epoch
    /// every subsequent `PlacementRoute`/read observes immediately.
    Assign { tenant: String, group: u64 },
    /// Online-move `tenant`'s partition `[range_start, range_end]` to `target`.
    /// Returns a `PlacementMoveReport` JSON: `{tenant, range, target, epoch,
    /// graphs: [{graph, from_group, to_group, nodes_transferred}]}`.
    Move {
        tenant: String,
        range_start: u64,
        range_end: u64,
        target: u64,
    },
    /// Abort an in-flight online move identified by `move_id` before its cutover
    /// fence. A move already past its epoch fence is rejected (roll-forward only,
    /// matching `TenantManager::abort_move`'s in-process contract). Returns `Bool`.
    AbortMove { move_id: String },
}

/// CONCEPT:EG-KG.query.obda-predicate-pushdown — a LIVE EXTERNAL relational source registered
/// for an OBDA virtual graph (W4.11): a `TriplesMap::logical_source` NAME bound to a `table`
/// in an external Postgres/MySQL database reachable at `dsn`. On a [`Method::SparqlVirtual`]
/// query the engine exposes it as a virtual RDF graph and pushes BOTH the query's column
/// projection AND its row-level `FILTER`s down into a real `SELECT … WHERE …` against the
/// database — the whole table is never scanned. The live SQL path needs a server built with
/// `federation-sql` (reusing its SSRF-validated, read-only, timeout+row-bounded connector); a
/// build without it returns a clean "rebuild with federation-sql" error. Distinct from
/// `tables` (the engine's OWN user tables).
#[cfg(feature = "obda")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObdaExternalSource {
    /// The foreign-source NAME the mapping's `TriplesMap`(s) reference as `logical_source`.
    pub name: String,
    /// The external database DSN (`postgres://…` / `mysql://…`), SSRF-validated server-side.
    pub dsn: String,
    /// The table (or view) name in the external database to expose as the virtual source.
    pub table: String,
}

// ── Method ──────────────────────────────────────────────────────────────

/// The only WorkItem kinds admitted by the development-lane authority.
///
/// The lifecycle and cleanup effects deliberately use different WorkItems and
/// therefore different fences.  A cleanup completion can never be mistaken for
/// a lifecycle attempt, even when both refer to the same immutable lane id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneWorkItemKind {
    #[serde(rename = "lane.lifecycle")]
    Lifecycle,
    #[serde(rename = "lane.cleanup")]
    Cleanup,
}

pub const DEVELOPMENT_LANE_LIFECYCLE_KIND: &str = "lane.lifecycle";
pub const DEVELOPMENT_LANE_CLEANUP_KIND: &str = "lane.cleanup";

/// Lane request DTOs carry `now_ms` for deterministic replay and test vectors.
/// Dispatch must normalize/overwrite that field from the authoritative engine
/// clock before authorization or persistence; a client-supplied timestamp is
/// never trusted as freshness, expiry, or lease authority (RMDD-27 convention).

/// All operations supported by the service.
// `IntoStaticStr` (metrics builds) yields the variant name as the bounded
// `op` label for request counters/histograms (CONCEPT:EG-KG.txn.per-graph-write-isolation).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "metrics", derive(strum::IntoStaticStr))]
#[serde(tag = "method", content = "params", deny_unknown_fields)]
pub enum Method {
    // ── Node CRUD ────────────────────────────────────────────────────
    AddNode {
        node_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },
    /// Create a node only when `node_id` is absent, returning `Bool(true)` only
    /// for the inserting writer. The membership test and insert are one durable
    /// atomic operation; an existing node is never overwritten.
    CreateNodeIfAbsent {
        node_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },
    RemoveNode {
        node_id: String,
    },
    HasNode {
        node_id: String,
    },
    GetNodes,
    /// Labeled + keyset-bounded node fetch: return at most `limit` nodes whose
    /// `type`/`label`/`labels` matches `label`, ordered by node id. `after` is an
    /// exclusive node-id cursor (`None` starts at the first id); callers advance
    /// it to the last id returned. `limit == 0` means no cap. Unlike
    /// `GetNodes` (which materializes the WHOLE graph), this bounds the wire
    /// payload to `limit`, so a `MATCH (n:Label) … LIMIT k` no longer pulls every
    /// node's properties off the engine. (CONCEPT:EG-KG.txn.per-graph-write-isolation)
    ///
    /// An empty `label` (CONCEPT:EG-KG.query.unlabeled-scan-limit-pushdown) means "no label filter" — a bounded scan
    /// of the whole node store, still honouring `limit`. Use this for an
    /// unlabeled `MATCH (n) … LIMIT k`: it stays bounded instead of falling back
    /// to `GetNodes`, which trips the `RESULT_TOO_LARGE` overload guard even when
    /// the caller only wanted `k` rows.
    GetNodesByLabel {
        label: String,
        #[serde(default)]
        after: Option<String>,
        limit: usize,
    },
    GetNodeProperties {
        node_id: String,
    },
    /// Atomic compare-and-set on a node's property blob (CONCEPT:EG-KG.compute.backend backend-
    /// agnostic atomic claim). `conditions_msgpack`/`updates_msgpack` are
    /// MessagePack-encoded JSON objects (field→value maps, same encoding as
    /// `properties_msgpack`). Under the topology write guard: if every condition
    /// matches the node's current value (a MISSING field reads as `null`), the
    /// updates are merged in and `true` is returned; otherwise (node absent, any
    /// condition fails, or decode fails) the node is left untouched and `false`
    /// is returned. One in-engine CAS suffices for all backends (the engine is
    /// the authoritative store; mirrors follow).
    CompareAndSetNodeFields {
        node_id: String,
        #[serde(with = "serde_bytes")]
        conditions_msgpack: Vec<u8>,
        #[serde(with = "serde_bytes")]
        updates_msgpack: Vec<u8>,
    },
    /// Atomically claim the oldest pending node of `label` (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending —
    /// native task queue). Under the topology write guard: among `label`'s nodes
    /// whose `status == "pending"`, pick the smallest `seq`, merge
    /// `updates_msgpack` (the claim marker — computed CLIENT-side, carrying NO
    /// server clock so WAL/Raft replay stays deterministic), and return
    /// `Raw(Option<(node_id, properties)>)` (nil ⇒ nothing claimable). One
    /// in-engine resolve+CAS; deterministic over identical state, so a committed
    /// Raft entry / replayed WAL record reproduces the same claim.
    ClaimNext {
        label: String,
        #[serde(with = "serde_bytes")]
        updates_msgpack: Vec<u8>,
    },
    // ── Message broker (CONCEPT:EG-KG.compute.message-broker-exchanges) ──────────────────────────────
    // Exchange/binding admin + publish DATA ops for the RabbitMQ-class broker built
    // on the KG-2.303 work-queue (queues are pending nodes; consume/ack REUSE
    // `ClaimNext` + `CompareAndSetNodeFields`, so no consume variant is needed).
    // Feature-gated `broker` (PURE serde, no dep) — a build without it drops the
    // variants → the dispatch "not available in this build" catch-all.
    /// Declare (idempotently upsert) an exchange. `kind` is `direct`/`topic`/`fanout`.
    #[cfg(feature = "broker")]
    DeclareExchange {
        exchange: String,
        kind: String,
    },
    /// Delete an exchange and all of its bindings (queues/messages untouched).
    #[cfg(feature = "broker")]
    DeleteExchange {
        exchange: String,
    },
    /// Bind `queue` to `exchange` under `routing_key` (idempotent).
    #[cfg(feature = "broker")]
    BindQueue {
        exchange: String,
        queue: String,
        routing_key: String,
    },
    /// Remove a specific `exchange`/`queue`/`routing_key` binding.
    #[cfg(feature = "broker")]
    UnbindQueue {
        exchange: String,
        queue: String,
        routing_key: String,
    },
    /// Publish `payload` to `exchange` with `routing_key`; the engine routes it to all
    /// matched queues atomically. Returns the delivered-queue count (`Count`).
    #[cfg(feature = "broker")]
    Publish {
        exchange: String,
        routing_key: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    // ── Broker policy extensions (CONCEPT:EG-KG.compute.dead-letter-queues..280) ────────────────
    // All ADDITIVE over the EG-275 broker: a queue with no policy node + a message
    // with no priority/ttl/delay behaves EXACTLY as EG-275. Each variant mutates the
    // control graph deterministically from its EXPLICIT args (no server clock — the
    // caller supplies `now_ms`, mirroring `InvalidateEdge`'s `tx_now`), so WAL/Raft
    // replay reproduces byte-identical state.
    /// Set (idempotently upsert) a queue's policy node (CONCEPT:EG-KG.compute.dead-letter-queues DLQ /
    /// EG-277 TTL / EG-278 priority). All fields optional — an all-`None` policy is a
    /// no-op that keeps the queue behaving exactly as EG-275. Returns `String("ok")`.
    #[cfg(feature = "broker")]
    DeclareQueue {
        queue: String,
        /// EG-276: exchange to republish dead-lettered messages to (`None` ⇒ drop).
        dl_exchange: Option<String>,
        /// EG-276: routing key for dead-lettered messages (`None` ⇒ reuse original).
        dl_routing_key: Option<String>,
        /// EG-276: max delivery attempts before a message is dead-lettered.
        max_delivery_count: Option<u32>,
        /// EG-277: default per-message TTL in ms applied when a publish omits one.
        message_ttl_ms: Option<u64>,
        /// EG-277: queue-expiry hint in ms (unused-queue teardown; advisory).
        queue_expiry_ms: Option<u64>,
        /// EG-278: max priority band the queue honors (advisory ceiling).
        max_priority: Option<u8>,
    },
    /// Policy-carrying publish (CONCEPT:EG-KG.compute.message-ttl-expiry/278/279). Superset of [`Publish`]:
    /// stamps per-message `priority` (EG-278), and — resolving relative intents
    /// against the EXPLICIT `now_ms` — a `deliver_at` eta (EG-279 delay) and an
    /// `expires_at` deadline (EG-277 TTL). With `priority == 0` and all options
    /// `None`, produces a message node identical to a plain [`Publish`]. Returns the
    /// delivered-queue count (`Count`).
    #[cfg(feature = "broker")]
    PublishEx {
        exchange: String,
        routing_key: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        /// EG-278: priority band; higher is delivered first (default 0).
        #[serde(default)]
        priority: i64,
        /// EG-279: hold the message non-claimable for this many ms from `now_ms`.
        #[serde(default)]
        delay_ms: Option<u64>,
        /// EG-277: per-message TTL in ms from `now_ms` (falls back to queue TTL).
        #[serde(default)]
        ttl_ms: Option<u64>,
        /// Caller clock (ms since epoch) used to resolve `delay_ms`/`ttl_ms` to
        /// absolute etas — explicit so WAL replay is deterministic.
        #[serde(default)]
        now_ms: Option<u64>,
    },
    /// Consume one message from `queue` for a named consumer-group member
    /// (CONCEPT:EG-KG.compute.groups-qos-prefetch-honoring groups + QoS/prefetch, honoring EG-277 TTL / EG-278 priority /
    /// EG-279 delay). Claims the highest-priority, oldest, DUE, non-expired message,
    /// enforcing per-consumer `prefetch` (0 ⇒ unlimited) and taking a visibility lease
    /// of `lease_ms` (0 ⇒ no lease). Lazily dead-letters any expired messages it steps
    /// over. Returns `Raw(Option<(node_id, properties)>)` — nil ⇒ nothing deliverable.
    #[cfg(feature = "broker")]
    BrokerConsume {
        queue: String,
        group: String,
        consumer: String,
        now_ms: u64,
        lease_ms: u64,
        prefetch: u32,
    },
    /// Acknowledge (remove) a claimed message, freeing the consumer's in-flight slot
    /// (CONCEPT:EG-KG.compute.groups-qos-prefetch-honoring). Returns `Bool(true)` if the message existed.
    #[cfg(feature = "broker")]
    BrokerAck {
        queue: String,
        node_id: String,
    },
    /// Reject a claimed message (CONCEPT:EG-KG.compute.dead-letter-queues). If `requeue` and the delivery count
    /// is under the queue's `max_delivery_count`, the message returns to claimable;
    /// otherwise it is dead-lettered to the queue's DL target (with `x-death` metadata)
    /// or dropped. Returns `String` outcome (`requeued`/`dead-lettered`/`dropped`/
    /// `absent`).
    #[cfg(feature = "broker")]
    BrokerReject {
        queue: String,
        node_id: String,
        requeue: bool,
        now_ms: u64,
    },
    /// Atomically select and lease the next runnable `WorkItem` node. Selection is
    /// tenant/resource/fairness scoped, priority ascending, then deadline/creation
    /// ordered. A negative result is authoritative and must not trigger another
    /// claim path.
    ClaimWorkItem {
        request: crate::epistemic_operations::ClaimWorkItemRequest,
    },
    /// Mint an opaque native capability for the caller's currently-live
    /// WorkItem lease.  All authority bindings are derived in the engine from
    /// the verified request context and the authoritative WorkItem row.
    MintWorkItemClaimCapability {
        request: crate::epistemic_operations::WorkItemClaimCapabilityMintRequest,
    },
    /// Verify an opaque native WorkItem capability against the current live
    /// lease before any private payload/body lookup.
    VerifyWorkItemClaimCapability {
        request: crate::epistemic_operations::WorkItemClaimCapabilityVerifyRequest,
    },
    /// Renew an existing WorkItem lease. Both epoch and fencing token must match
    /// the durable row, preventing a superseded worker from extending ownership.
    RenewWorkItemLease {
        tenant: String,
        work_item_id: String,
        worker_id: String,
        lease_epoch: u64,
        fencing_token: u64,
        now_ms: u64,
        lease_ms: u64,
    },
    /// Publish a WorkItem terminal/retry result through the same authoritative
    /// transaction as its state transition and mutation outbox. Result bodies are
    /// referenced, never embedded, so the durable control plane stores no PII.
    CommitWorkItemResult {
        tenant: String,
        work_item_id: String,
        worker_id: String,
        lease_epoch: u64,
        fencing_token: u64,
        idempotency_key: String,
        outcome: String,
        #[serde(default)]
        result_ref: Option<String>,
        #[serde(default)]
        error_ref: Option<String>,
        #[serde(default)]
        retryable: bool,
        now_ms: u64,
    },
    /// Cancel a pending WorkItem without first manufacturing a worker lease.
    /// Active, unexpired leases are never stolen: their current owner must use
    /// `CommitWorkItemResult` with the matching epoch/fencing token instead.
    CancelWorkItem {
        tenant: String,
        work_item_id: String,
        idempotency_key: String,
        /// Opaque reference to a redacted cancellation reason. The engine never
        /// persists a caller-supplied reason body in the control-plane node.
        #[serde(default)]
        reason_ref: Option<String>,
        now_ms: u64,
    },
    /// Release a leased WorkItem back to `ready` at an explicit retry time
    /// without consuming an execution attempt. This is the native transition
    /// for self-polling barriers and other cooperative deferrals.
    DeferWorkItem {
        tenant: String,
        work_item_id: String,
        worker_id: String,
        lease_epoch: u64,
        fencing_token: u64,
        idempotency_key: String,
        next_retry_at_ms: u64,
        /// Opaque reference only; no free-form reason body is retained.
        #[serde(default)]
        reason_ref: Option<String>,
        now_ms: u64,
    },
    /// Atomically reserve the immutable shared-host resources for the exact
    /// WorkItem attempt/fence.  The engine re-reads the WorkItem admission
    /// extension and host records; request fields are assertions, never a
    /// caller-owned reservation ledger.
    ReserveWorkItemResources {
        request: crate::epistemic_operations::ResourceReservationRequest,
    },
    /// Atomically release the exact current/terminal WorkItem reservation while
    /// retaining its lifecycle tombstone for exact idempotent replay.
    ReleaseWorkItemResources {
        request: crate::epistemic_operations::ResourceReservationRequest,
    },
    /// Atomically reclaim an expired or superseded reservation.  A stale worker
    /// cannot use this operation to release a newer attempt's capacity.
    ReclaimWorkItemResources {
        request: crate::epistemic_operations::ResourceReservationRequest,
    },
    /// Exact bounded read of a native reservation or retained lifecycle tombstone.
    QueryWorkItemReservation {
        request: crate::epistemic_operations::ResourceReservationStatusRequest,
    },
    /// Bounded native reservation reconciliation/status read.  No local mirror
    /// is sufficient to answer this operation.
    ResourceReservationStatus {
        request: crate::epistemic_operations::ResourceReservationStatusRequest,
    },
    /// Monotonic host capacity/heartbeat/policy update.  Held accounting remains
    /// native and cannot be overwritten by telemetry.
    UpdateResourceHost {
        request: crate::epistemic_operations::ResourceHostUpdateRequest,
    },
    /// Atomically allocate the typed development-lane hold for the exact
    /// `lane.lifecycle` WorkItem attempt/fence. Branch and managed-worktree
    /// uniqueness plus every configured quota scope are charged together.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    ReserveDevelopmentLane {
        request: crate::epistemic_operations::DevelopmentLaneReserveRequest,
    },
    /// Renew one existing lane hold in place. This never appends a WorkItem or
    /// history row and requires the current WorkItem lease/fence.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    RenewDevelopmentLane {
        request: crate::epistemic_operations::DevelopmentLaneRenewRequest,
    },
    /// Replace the monotonic observed retained footprint on one exact hold.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    ObserveDevelopmentLane {
        request: crate::epistemic_operations::DevelopmentLaneObserveRequest,
    },
    /// Release active-count charge after the exact lifecycle WorkItem reaches a
    /// terminal state while retaining disk/exclusivity until cleanup.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    FinishDevelopmentLane {
        request: crate::epistemic_operations::DevelopmentLaneFinishRequest,
    },
    /// Release retained disk and identity indexes after a distinct current
    /// `lane.cleanup` WorkItem proves the guarded filesystem effect complete.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    CleanupDevelopmentLane {
        request: crate::epistemic_operations::DevelopmentLaneCleanupCompleteRequest,
    },
    /// Exact authenticated read of one lane hold or tombstone.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    QueryDevelopmentLane {
        request: crate::epistemic_operations::DevelopmentLaneQueryRequest,
    },
    /// Bounded authenticated status page with maintained native counters.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    DevelopmentLaneStatus {
        request: crate::epistemic_operations::DevelopmentLaneStatusRequest,
    },
    /// Controller/admin-only monotonic quota-policy update.
    /// `request.now_ms` is overwritten from the authoritative engine clock.
    UpdateDevelopmentLaneQuota {
        request: crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequest,
    },
    /// Reaper sweep (CONCEPT:EG-KG.compute.message-ttl-expiry): dead-letter/drop messages whose `expires_at`
    /// has passed and return messages whose visibility lease has expired to claimable,
    /// across every known queue. Called periodically by the scheduler with the current
    /// clock. Returns `Count` of messages acted on.
    #[cfg(feature = "broker")]
    SweepExpired {
        now_ms: u64,
    },
    // ── Replayable append-log streams (CONCEPT:EG-KG.compute.replayable-append-log) ────────────────
    // A `Stream` is a Kafka-class RETAIN + read-by-offset log living ALONGSIDE the
    // EG-275 work-queue on the same control graph: messages are labeled `smsg:<stream>`
    // with a per-stream monotonic offset and are NEVER deleted by a read — only by an
    // explicit retention trim. A queue with no stream usage is byte-for-byte unchanged.
    // Each mutation is deterministic from graph state + the EXPLICIT `now_ms`, so
    // WAL/Raft replay reproduces byte-identical nodes.
    /// Declare (idempotently upsert) a stream's retention policy (CONCEPT:EG-KG.compute.replayable-append-log).
    /// Both bounds optional — an all-`None` policy is an unbounded append log that a
    /// trim never touches. Also ensures the offset counter so the stream is publishable.
    /// Returns `String("ok")`.
    #[cfg(feature = "broker")]
    StreamDeclare {
        stream: String,
        /// Keep at most this many newest messages (older dropped on trim).
        max_messages: Option<u64>,
        /// Drop messages older than this many ms (`now_ms - ts`) on trim.
        max_age_ms: Option<u64>,
    },
    /// Append `payload` to `stream`, returning its assigned monotonic offset (`Count`)
    /// (CONCEPT:EG-KG.compute.replayable-append-log). The message is RETAINED (read by offset), never auto-consumed.
    #[cfg(feature = "broker")]
    StreamPublish {
        stream: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        /// Caller clock (ms) stamped as the message `ts` for age-based retention.
        now_ms: u64,
    },
    /// Read up to `max` retained messages from `stream` starting at `from_offset`,
    /// WITHOUT deleting (CONCEPT:EG-KG.compute.replayable-append-log — replay). `from_offset < 0` ⇒ from the current
    /// end ("only new"); `0` ⇒ earliest; otherwise that explicit offset. `max == 0` ⇒
    /// uncapped. Returns `Raw(Vec<(offset, payload)>)` ascending by offset. Read-only.
    #[cfg(feature = "broker")]
    StreamRead {
        stream: String,
        from_offset: i64,
        max: u64,
    },
    /// Trim `stream` per its declared retention (CONCEPT:EG-KG.compute.replayable-append-log): drop messages beyond
    /// `max_messages` (oldest first) and/or older than `max_age_ms`. Returns `Count`
    /// of messages removed. An undeclared / unbounded stream trims nothing.
    #[cfg(feature = "broker")]
    StreamTrim {
        stream: String,
        now_ms: u64,
    },
    /// Commit a consumer-group's read `offset` on `stream` so it can resume
    /// (CONCEPT:EG-KG.compute.replayable-append-log). Idempotent upsert; returns `String("ok")`.
    #[cfg(feature = "broker")]
    StreamCommitOffset {
        stream: String,
        group: String,
        offset: i64,
    },
    /// Read a consumer-group's committed offset on `stream` (CONCEPT:EG-KG.compute.replayable-append-log). Returns
    /// `Raw(Option<i64>)` — nil ⇒ the group has never committed. Read-only.
    #[cfg(feature = "broker")]
    StreamCommittedOffset {
        stream: String,
        group: String,
    },
    // ── Publisher confirms + consumer QoS acks (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos) ───────
    // At-least-once on top of the EG-275 publish + claim path: a confirm allocates a
    // broker-wide monotonic delivery-tag once the message is durably enqueued (or nacks
    // on an unknown exchange); consumer ack/nack address the message by that tag.
    /// Publish with a publisher confirm (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos) — a superset of [`PublishEx`]
    /// that also allocates a monotonic delivery-tag. Returns `Raw(ConfirmToken)` with
    /// `confirmed = true` once durably enqueued (exchange exists) or a nack on an
    /// unknown exchange. The tag increments on every call (confirms and nacks alike).
    #[cfg(feature = "broker")]
    PublishConfirmed {
        exchange: String,
        routing_key: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        #[serde(default)]
        priority: i64,
        #[serde(default)]
        delay_ms: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
        #[serde(default)]
        now_ms: Option<u64>,
    },
    /// Publish with an OPTIONAL `(producer_id, seq)` idempotency stamp for
    /// effectively-once delivery (CONCEPT:EG-KG.ingest.broker-reject-publish) — a superset of [`PublishConfirmed`].
    /// With `producer_id == None` this is the plain at-least-once path (byte-identical
    /// to [`PublishEx`]). With `producer_id == Some`, the broker dedups against that
    /// producer's durable monotonic high-water mark: a `seq` at/under the mark is a
    /// DUPLICATE (dropped but still confirmed), a `seq` above it advances the mark and
    /// the message is enqueued. Returns `Raw(IdempotentPublish)`
    /// (`confirmed`/`duplicate`/`delivered`). Deterministic: the dedup + mark bump
    /// derive purely from graph state + explicit args, so WAL/Raft replay reproduces
    /// byte-identical state.
    #[cfg(feature = "broker")]
    PublishIdempotent {
        exchange: String,
        routing_key: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        /// Stable publisher identity; `None`/empty ⇒ at-least-once (no dedup).
        #[serde(default)]
        producer_id: Option<String>,
        /// Per-producer monotonic sequence number (dedup key).
        #[serde(default)]
        seq: i64,
        #[serde(default)]
        priority: i64,
        #[serde(default)]
        delay_ms: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
        #[serde(default)]
        now_ms: Option<u64>,
    },
    /// Acknowledge (remove) a claimed message by its positive `delivery_tag`
    /// (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos). The caller must name the claiming `consumer`; the
    /// status, current tag, and owner are fenced atomically. Returns `Bool(false)`
    /// for an absent, stale, or foreign-owned generation.
    #[cfg(feature = "broker")]
    BrokerAckTag {
        delivery_tag: i64,
        consumer: String,
    },
    /// Nack a claimed message by its positive `delivery_tag` (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos). The caller
    /// must name the claiming `consumer`; status, current tag, and owner are fenced
    /// atomically. With `requeue` the message returns to claimable (at-least-once
    /// redelivery) unless its delivery budget is exhausted. Returns `String` outcome
    /// (`requeued`/`dead-lettered`/`dropped`/`absent`).
    #[cfg(feature = "broker")]
    BrokerNackTag {
        delivery_tag: i64,
        consumer: String,
        requeue: bool,
        now_ms: u64,
    },
    /// Extend a still-live claimed delivery lease for its current owner. The
    /// status, tag, owner, and unexpired lease are fenced atomically. The requested
    /// deadline must advance the current deadline. `now_ms` is explicit so durable
    /// replay is deterministic.
    #[cfg(feature = "broker")]
    BrokerRenewTag {
        delivery_tag: i64,
        consumer: String,
        now_ms: u64,
        lease_ms: u64,
    },
    // ── Agent-memory / scene-graph / trajectory wire ops (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ──
    // Expose the eg-core LIBRARY primitives for hierarchical summaries (EG-220),
    // episodic→semantic consolidation (EG-221), decay/reinforce/evict maintenance
    // (EG-222), the 3D scene-graph (EG-087), and action/policy trajectory memory
    // (EG-099) over the wire. These are ADDITIVE + UNGATED (the eg-core `graph` /
    // `scene` modules are always compiled — unlike the feature-gated broker), so
    // every serving tier (`server`/`full`/`pi`) carries them. The mutating variants
    // mirror the EG-276..284 broker precedent EXACTLY: every generated id is
    // deterministic (SipHash zero-key over sorted inputs, or a monotonic
    // node-count / step-ordinal), and any clock is the EXPLICIT caller-supplied
    // `now_ms` — never a server clock — so a replayed WAL record / committed Raft
    // entry reproduces byte-identical state (`mutation_apply::apply`). Non-security /
    // non-broker builds are unaffected: a build that never issues these sees no
    // behavioral change.
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-220 — create (or UPSERT) a hierarchical summary node at
    /// abstraction `level`, linked to each of `child_ids` via a `SUMMARIZES`
    /// provenance edge. `props_msgpack` is a MessagePack-encoded JSON object (the
    /// LLM summary text + any caller fields; an `id` string is honoured). Durable +
    /// deterministic (the id derives from `(level, sorted children)` with no clock),
    /// so WAL replay upserts the identical node. Returns the summary node id
    /// (`String`).
    CreateSummaryNode {
        level: u32,
        child_ids: Vec<String>,
        #[serde(with = "serde_bytes")]
        props_msgpack: Vec<u8>,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-221 — consolidate a cluster of `episodic_ids` into ONE
    /// semantic node (LOCALIZED maintenance — nothing outside the cluster is
    /// touched). `semantic_props_msgpack` is a MessagePack-encoded JSON object. The
    /// semantic id derives deterministically from the sorted cluster, so WAL replay
    /// reproduces it. Returns the semantic node id (`String`).
    Consolidate {
        episodic_ids: Vec<String>,
        #[serde(with = "serde_bytes")]
        semantic_props_msgpack: Vec<u8>,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-222 — reinforce a memory node: bump access/recency +
    /// importance as of the EXPLICIT `now_ms` (no server clock ⇒ deterministic
    /// replay). Returns `Bool` (whether the node existed).
    Reinforce {
        node_id: String,
        now_ms: u64,
        weight: f64,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-222 — Ebbinghaus-decay a single memory node's importance to
    /// the EXPLICIT `now_ms` given `half_life_ms`. Deterministic (caller clock).
    /// Returns `Bool` (whether it decayed/stamped the node).
    DecayNode {
        node_id: String,
        now_ms: u64,
        half_life_ms: u64,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-222 — batch-decay a caller-supplied working set of memory
    /// `ids` to the EXPLICIT `now_ms` (localized — no global scan). Returns `Count`
    /// (nodes decayed).
    DecayMemories {
        now_ms: u64,
        half_life_ms: u64,
        ids: Vec<String>,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-222 — prune the sub-`threshold`-importance members of the
    /// working set `ids`. `delete == false` marks `forgotten` (provenance-preserving);
    /// `true` hard-removes. Deterministic (no clock). Returns the pruned ids (`Ids`,
    /// sorted).
    EvictBelow {
        ids: Vec<String>,
        threshold: f64,
        delete: bool,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-222 — decay-THEN-evict the working set `ids` in ONE atomic
    /// pass as of the EXPLICIT `now_ms` (the primitive the AU maintenance loop
    /// schedules). Deterministic (caller clock). Returns `Raw((decayed_count,
    /// pruned_ids))`.
    Maintain {
        ids: Vec<String>,
        now_ms: u64,
        half_life_ms: u64,
        evict_threshold: f64,
        delete: bool,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-220 — the direct children of summary node `node_id` (targets
    /// of its `SUMMARIZES` edges), sorted + deduped. Read-only. Returns `Ids`.
    SummaryChildren {
        node_id: String,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-220 — all summary node ids at abstraction `level`, sorted +
    /// deduped. Read-only. Returns `Ids`.
    SummariesAtLevel {
        level: u32,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — create a `:SceneObject` with LOCAL `pose_msgpack` (a
    /// MessagePack-encoded `{translation,rotation,scale}` JSON), optionally parented
    /// under `parent` via a `CHILD_OF`/`HAS_CHILD` link. The id derives
    /// deterministically from `(live node count, parent, pose)`, so WAL replay
    /// reproduces it. Returns the new object's id (`String`).
    AddSceneObject {
        #[serde(with = "serde_bytes")]
        pose_msgpack: Vec<u8>,
        parent: Option<String>,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — overwrite scene object `node_id`'s LOCAL pose with
    /// `pose_msgpack`. Deterministic. Returns `Bool` (whether the node existed).
    SetPose {
        node_id: String,
        #[serde(with = "serde_bytes")]
        pose_msgpack: Vec<u8>,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — re-parent scene object `node_id` under `new_parent`
    /// (`None` ⇒ detach to a root). Deterministic. Returns `Bool` (whether it acted).
    Reparent {
        node_id: String,
        new_parent: Option<String>,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — the WORLD pose of scene object `node_id` (its local
    /// pose composed up the `CHILD_OF` chain). Read-only. Returns `Json` — the
    /// `{translation,rotation,scale}` object, or `null` if the node is absent / has
    /// no pose.
    WorldTransform {
        node_id: String,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — the direct transform children of scene object
    /// `node_id` (targets of its `HAS_CHILD` edges), sorted + deduped. Read-only.
    /// Returns `Ids`.
    SceneChildren {
        node_id: String,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-099 — START (or UPSERT) a `:Trajectory` (episode).
    /// `props_msgpack` is a MessagePack-encoded JSON object (an `id` string is
    /// honoured). The id derives deterministically from `(live node count, props)`,
    /// monotonic under replay, so WAL replay reproduces it. Returns the trajectory id
    /// (`String`).
    StartTrajectory {
        #[serde(with = "serde_bytes")]
        props_msgpack: Vec<u8>,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-099 — APPEND a `:Step{action,reward,t,…}` to trajectory
    /// `traj_id`. `action_msgpack` is a MessagePack-encoded JSON action (a string or
    /// structured object); `reward`/`t` are caller-supplied (no clock/RNG ⇒
    /// deterministic). The step id derives from `(traj_id, step ordinal)`, so WAL
    /// replay reproduces the identical chain. Returns `Raw(Option<String>)` — the new
    /// step id, or nil if the trajectory is absent.
    AppendStep {
        traj_id: String,
        #[serde(with = "serde_bytes")]
        action_msgpack: Vec<u8>,
        reward: f64,
        #[serde(default)]
        state_ref: Option<String>,
        #[serde(default)]
        next_state_ref: Option<String>,
        t: u64,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-099 — the DISCOUNTED return `Σ gamma^t · reward` over
    /// trajectory `traj_id`'s ordered steps. Read-only, deterministic (`gamma`
    /// caller-supplied). Returns `Float` (`0.0` for an absent/empty trajectory).
    DiscountedReturn {
        traj_id: String,
        gamma: f64,
    },
    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-099 — the trajectory in `traj_ids` with the HIGHEST
    /// discounted return (prioritized replay / policy selection); ties broken by the
    /// smaller id. Read-only. Returns `Raw(Option<String>)` — nil for empty input.
    BestTrajectory {
        traj_ids: Vec<String>,
        gamma: f64,
    },
    /// Batch property read: fetch properties for many nodes in ONE round-trip
    /// instead of N `GetNodeProperties` calls. Returns a `Raw` list of
    /// `[node_id, properties_msgpack | nil]` in input order (nil ⇒ absent), so the
    /// caller learns which ids were missing. Bounded by `MAX_BATCH_IDS`.
    GetNodePropertiesBatch {
        node_ids: Vec<String>,
    },
    /// Batch existence check: `Raw` list of bools in input order.
    HasNodesBatch {
        node_ids: Vec<String>,
    },
    NodeCount,
    NodeIds,

    // ── Edge CRUD ────────────────────────────────────────────────────
    AddEdge {
        source_id: String,
        target_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },
    RemoveEdge {
        source_id: String,
        target_id: String,
    },
    /// Non-destructively CLOSE a contradicted edge's temporal windows (KG-2.251):
    /// sets `valid_until = invalid_at` and `tx_to = tx_now` on the matching edge
    /// instead of deleting it, so an `AS OF` before `invalid_at` still sees the fact.
    /// A durable mutation (WAL-replayed deterministically from its explicit args).
    InvalidateEdge {
        source_id: String,
        target_id: String,
        relationship: String,
        invalid_at: u64,
        tx_now: u64,
    },
    /// Atomically supersede a prior edge with a new one (KG-2.251): close the prior
    /// edge's validity window and insert `properties_msgpack` as the new edge under
    /// one write guard. Non-destructive — the prior edge survives for history.
    SupersedeEdge {
        source_id: String,
        target_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
        prior_source: String,
        prior_target: String,
        prior_relationship: String,
        valid_at: u64,
        tx_now: u64,
    },
    HasEdge {
        source_id: String,
        target_id: String,
    },
    GetEdges,
    /// Keyset-bounded edge fetch — the edge sibling of `GetNodesByLabel`. Return
    /// at most `limit` edges ordered by `(source, target, ordinal)`; `ordinal`
    /// distinguishes parallel edges stored under the same `(source, target)`
    /// pair (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation). `after` is an exclusive `(source, target,
    /// ordinal)` cursor (`None` starts at the first edge); callers advance it to
    /// the last row returned. `limit == 0` means no cap. Unlike `GetEdges`
    /// (which materializes the WHOLE graph), this bounds the wire payload to
    /// `limit`, so a full edge walk no longer trips the `RESULT_TOO_LARGE`
    /// overload guard. Returns a `Raw`-encoded `Vec<(String, String, u32,
    /// Vec<u8>)>` (source, target, ordinal, properties_msgpack).
    GetEdgesPage {
        #[serde(default)]
        after: Option<(String, String, u32)>,
        limit: usize,
    },
    ClearGraph,
    GetEdgeProperties {
        source_id: String,
        target_id: String,
    },
    /// Batch edge property read: `Raw` list of `properties_msgpack | nil` in input
    /// order (nil ⇒ no such edge). Bounded by `MAX_BATCH_IDS`.
    GetEdgePropertiesBatch {
        edges: Vec<(String, String)>,
    },
    EdgeCount,

    // ── Neighbor Queries ─────────────────────────────────────────────
    InDegree {
        node_id: String,
    },
    OutDegree {
        node_id: String,
    },
    GetPredecessors {
        node_id: String,
    },
    GetSuccessors {
        node_id: String,
    },
    GetNeighbors {
        node_id: String,
    },
    /// Batch neighbor read: fetch neighbor ids for many nodes in ONE round-trip
    /// instead of N `GetNeighbors` calls (D-DPF-1 — the N+1 this closes). Returns
    /// a `Raw` list of `[node_id, Vec<String>]` in input order; a missing/absent
    /// node yields an empty neighbor list rather than failing the whole batch, so
    /// one bad id in a large discover-then-hydrate batch cannot sink the rest.
    /// Bounded by `MAX_BATCH_IDS`.
    GetNeighborsBatch {
        node_ids: Vec<String>,
    },

    // ── Cross-graph union reads (CONCEPT:EG-KG.query.cross-graph-union) ───────────────────
    // Read across a SET of content graphs as if they were one, so writes can be
    // partitioned across per-graph write locks (each lane its own graph/lock)
    // while reads still see the union. Missing graphs in the set are skipped
    // (a lane graph may not exist yet). Routed like the other cross-graph reads
    // (DiffAgainst): the handler re-enters the registry, point-reads/snapshots
    // each core off-lock, and merges — never holding two graph locks at once.
    /// First-found node properties across `graphs` (in order); `Null` if absent
    /// in every graph.
    UnionGetNodeProperties {
        graphs: Vec<String>,
        node_id: String,
    },
    /// Label scan unioned + deduped by node id across `graphs` (limit 0 ⇒ no cap).
    UnionGetNodesByLabel {
        graphs: Vec<String>,
        label: String,
        limit: usize,
    },
    /// Neighbour ids unioned + deduped across every graph that contains the anchor.
    UnionGetNeighbors {
        graphs: Vec<String>,
        node_id: String,
    },

    // ── Graph Algorithms ─────────────────────────────────────────────
    TopologicalSort,
    FindCycle,
    GetShortestPath {
        source_id: String,
        target_id: String,
    },
    GetBlastRadius {
        node_id: String,
        max_depth: usize,
    },
    DegreeCentrality {
        node_id: String,
    },
    DegreeCentralityAll,
    BetweennessCentrality,
    PageRank {
        damping: f64,
        iterations: usize,
    },
    PersonalizedPageRank {
        seed_nodes: Vec<(String, f64)>,
        damping: f64,
        iterations: usize,
    },
    ConnectedComponents,
    StronglyConnectedComponents,
    MinimumSpanningTree,
    CommunityDetection {
        resolution: f64,
    },
    /// Stateless community detection over a call graph passed inline — NO tenant
    /// load, NO persistence. The ingest path previously bulk-loaded ~160k edges
    /// into a throwaway tenant just to run this, then deleted the tenant; passing
    /// the edges directly removes that whole round-trip + the tenant sprawl.
    CommunityDetectEphemeral {
        node_ids: Vec<String>,
        edges: Vec<(String, String)>,
        resolution: f64,
    },
    GraphColoring,
    ComputeSimilarityEdges {
        threshold: f64,
    },
    /// Native entity-resolution candidate generator (CONCEPT:AU-KG.compute.when-exposes-native) — composes
    /// embedding similarity + clustering into one server-side READ op that returns
    /// merge proposals (same_as / extends). Never mutates; the client applies via
    /// `BatchUpdate`. The escalation tier for the agent-utilities dedup ladder.
    ResolveCandidates {
        sim_threshold: f64,
        merge_threshold: f64,
        #[serde(default)]
        node_type: Option<String>,
    },

    // ── Lifecycle ────────────────────────────────────────────────────
    PruneByLifecycle {
        max_age_secs: u64,
        min_score: f64,
    },
    GetContextView {
        agent_id: String,
        max_tokens: u32,
    },
    BatchUpdate {
        #[serde(with = "serde_bytes")]
        operations_msgpack: Vec<u8>,
    },
    /// Batched CROSS-GRAPH write (CONCEPT:EG-KG.storage.multi-graph-batch-write). One
    /// round-trip carries a `BatchUpdate`-shaped op list for MANY named graphs; the
    /// server applies each graph's sub-batch through the normal per-graph write
    /// path CONCURRENTLY, so N distinct graphs commit across N of the K redb shard
    /// writers in parallel instead of the client serializing N round-trips that
    /// each re-acquire one lock. `batches_msgpack` decodes to
    /// `Vec<(graph_name, operations_msgpack)>` — each inner blob is exactly a
    /// `BatchUpdate.operations_msgpack`, so it REUSES the existing batch primitive
    /// (no new per-op op). Carries its graphs in the METHOD (like `NlQuery`), so it
    /// is routed BEFORE the single-`graph` graph-op path in dispatch. The reply is
    /// `{ "results": { graph: <batch_result> }, "errors": { graph: msg } }`.
    MultiGraphBatchUpdate {
        #[serde(with = "serde_bytes")]
        batches_msgpack: Vec<u8>,
    },
    Metrics,
    EvictLRU {
        max_nodes: usize,
    },

    // ── Temporal Decay (CONCEPT:EG-KG.memory.forgetting-curve-decay — Ebbinghaus forgetting curve) ──
    DecaySweep {
        half_life_secs: f64,
        floor: f64,
        prune: bool,
    },
    TouchNodes {
        node_ids: Vec<String>,
    },

    // ── Serialization ────────────────────────────────────────────────
    ToMsgpack,
    FromMsgpack {
        #[serde(with = "serde_bytes")]
        msgpack: Vec<u8>,
    },

    // ── Ledger ───────────────────────────────────────────────────────
    GetLedger,
    ClearLedger,
    ApplyLedger {
        transactions: Vec<String>,
    },
    // Tamper-evident audit log verification (CONCEPT:EG-KG.sharding.row-level-security, feature `security`):
    // walk the target graph's hash-chained audit log and report OK or the first break.
    #[cfg(feature = "security")]
    AuditVerify,
    /// Produce + server-side-verify a Merkle inclusion proof for one node against
    /// a prior provenance anchor (CONCEPT:EG-KG.sharding.row-level-security, feature `security`) — the
    /// extension that lets [`AuditVerify`](Method::AuditVerify)'s tamper-evidence
    /// reach the ANCHORED NODES' CONTENT, not just the ordering of mutations. A
    /// periodic engine job Merkle-hashes the target graph's `:ToolCall`/
    /// `:RunTrace` provenance-node window and folds the root into this SAME
    /// hash chain as one more entry (`audit::provenance_anchor_line`); this
    /// method re-hashes `node_id`'s CURRENT durable content and walks the
    /// anchor-time sibling path up to that chain-protected root — a mismatch
    /// (`MerkleInclusionReport.verified == false`) proves the node's durable
    /// bytes changed after anchoring, whether by raw tampering or an ordinary
    /// later overwrite. `anchor_seq` selects a specific anchor by its
    /// audit-chain seq; `None` uses the target graph's most recent anchor.
    /// Errors when the graph has no anchor yet or `anchor_seq` names an entry
    /// that is not one; `included == false` (not an error) means `node_id`
    /// simply was not part of that anchor's window. Returns
    /// `Raw(MerkleInclusionReport)`.
    #[cfg(feature = "security")]
    AuditProveInclusion {
        node_id: String,
        anchor_seq: Option<u64>,
    },

    // ── Subgraph & Matching ──────────────────────────────────────────
    GetSubgraph {
        node_ids: Vec<String>,
    },
    Fork,
    DiffAgainst {
        other_graph: String,
    },
    CompactNodesByType {
        node_type: String,
        threshold: usize,
    },

    // ── Reasoning ────────────────────────────────────────────────────
    // CONCEPT:EG-KG.compute.compiled-semantic-reasoner - Compiled Semantic Reasoner. A single round of
    // forward-chaining OWL/RDFS inference (Datalog) plus optional
    // domain/range and property-chain inference. All rule sets default to
    // empty so clients may run any subset without sending every field.
    RunDatalogReasoning {
        #[serde(default)]
        subclass_relations: Vec<(String, String)>,
        #[serde(default)]
        subproperty_relations: Vec<(String, String)>,
        #[serde(default)]
        symmetric_properties: Vec<String>,
        #[serde(default)]
        transitive_properties: Vec<String>,
        #[serde(default)]
        inverse_properties: Vec<(String, String)>,
        /// (property, domain_type) — subjects of `property` are inferred to be `domain_type`.
        #[serde(default)]
        domain_rules: Vec<(String, String)>,
        /// (property, range_type) — objects of `property` are inferred to be `range_type`.
        #[serde(default)]
        range_rules: Vec<(String, String)>,
        /// (predicate_a, predicate_b, inferred_predicate) — chain composition.
        #[serde(default)]
        property_chains: Vec<(String, String, String)>,
    },

    // ── Governed change ingestion ────────────────────────────────────
    /// Atomically materialize one externally sourced object and all of its
    /// governance/provenance state. The embedded MutationBatch is the graph-row
    /// mutation authority; blob/feature/evidence/policy/lineage, content version,
    /// typed cursor, durable status, and outbox share its commit point.
    ApplyChangeEnvelope {
        envelope: Box<crate::change_envelope::ChangeEnvelope>,
    },
    /// Atomically materialize a BATCH of externally sourced objects. Envelopes are
    /// grouped by their `mutation.graph` and each graph's envelopes land in ONE
    /// coalesced redb transaction (the atomic graph-batch); envelopes spanning
    /// graphs split into independent per-graph sub-batches. Within a graph-batch the
    /// commit is all-or-nothing — a single failing envelope aborts that graph's
    /// transaction and every envelope in the graph reports the batch outcome. Across
    /// graphs the sub-batches are independent (partial success). Per-envelope results
    /// (applied / idempotent-skip / conflict) are returned in request order. Same
    /// policy class as `ApplyChangeEnvelope`. Bounded by `MAX_ENVELOPES_PER_BATCH`.
    ApplyChangeEnvelopes {
        envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
    },
    /// Read a committed envelope by stable identity for retry reconciliation.
    GetChangeEnvelope {
        envelope_id: String,
        tenant: String,
    },
    /// Read the current typed content version for an object in this graph/tenant.
    GetContentVersion {
        object_id: String,
        tenant: String,
    },
    /// Read the current typed source cursor. Cursors are partition scoped and are
    /// never compared as strings.
    GetChangeCursor {
        source: String,
        #[serde(default)]
        partition: String,
        tenant: String,
    },

    // ── Governed document/image/audio/video serving ─────────────────────
    // Graph-scoped and available in the one main build. Authority comes only from
    // the verified request context; no caller-supplied tenant or policy scope is
    // accepted by the operation DTO.
    #[cfg(feature = "modality-serving")]
    ServedModality {
        op: crate::modality::ServedModalityOp,
    },

    // ── Multi-Tenant Graph Management ────────────────────────────────
    CreateGraph {
        graph_name: String,
        graph_type: GraphType,
    },
    DeleteGraph {
        graph_name: String,
    },
    ListGraphs,

    // ── M3 catalog-driven resharding admin (CONCEPT:EG-KG.backend.m3-admin-dispatch) ───────────
    // The wire surface that DRIVES the M3 ops the engine already has the building
    // blocks for: online single-node resharding (EG-032), the tenant catalog
    // (EG-031), and the rebalancing planner (EG-035) + its execution (EG-039). All
    // are redb-only; in a non-redb build they return a clean "not available" error.
    /// Online-move `graph`'s durable rows to shard `to_shard` while the engine RUNS,
    /// then flip the catalog route (CONCEPT:EG-KG.backend.catalog-shard-resolve). Returns a `ReshardReport` JSON.
    Reshard {
        graph: String,
        to_shard: u32,
    },
    /// Populate / assign an explicit catalog placement for `graph` (CONCEPT:EG-KG.sharding.empty-catalog-routing).
    /// Flips the ROUTE only — to MOVE the rows too use `Reshard`. Returns `Bool`.
    CatalogAssign {
        graph: String,
        shard: u32,
        node: Option<u32>,
    },
    /// Re-place `graph` onto `shard`, preserving its node placement (CONCEPT:EG-KG.sharding.empty-catalog-routing).
    CatalogReassign {
        graph: String,
        shard: u32,
    },
    /// Drop `graph`'s explicit placement — it reverts to EG-026 FNV-1a routing.
    CatalogRemove {
        graph: String,
    },
    /// List every explicit catalog placement `{graph, shard, node}` (JSON).
    CatalogList,
    /// Compute (do NOT execute) a rebalance plan over live per-shard/per-graph load
    /// (CONCEPT:EG-KG.sharding.even-load-rebalance). Returns the ordered `{graph, from_shard, to_shard}` moves +
    /// the per-shard load it planned against, as JSON.
    RebalancePlan {
        tolerance: Option<f64>,
        max_moves: Option<usize>,
    },
    /// Compute a rebalance plan AND execute it move-by-move via online resharding
    /// (CONCEPT:EG-KG.backend.r3-plan-execution, R3 plan execution). Each move is one online `Reshard` — online,
    /// one graph at a time, other graphs unaffected. Returns the executed moves' reports.
    RebalanceExecute {
        tolerance: Option<f64>,
        max_moves: Option<usize>,
    },

    // ── Raft cluster membership admin (CONCEPT:EG-KG.storage.kg-kg-2 — cluster_deployment.md §5 item 2) ──
    // The wire surface that lets an operator attach a fresh node to a LIVE Raft
    // group without a bespoke binary. `MultiRaft::add_group_learner` /
    // `change_group_voters` (src/raft/multi.rs) already implement the openraft
    // add-learner / change-membership lifecycle — they simply had NO external
    // caller, so §2c of the cluster deployment runbook could not actually be
    // driven outside the in-process test harness. Both ops are leader-only; a
    // follower answers `OPERATION_REDIRECTED` naming the current leader
    // (mirroring `PlacementRoute`'s stale-route redirect), and an engine with
    // no live `MultiRaft` returns a clean typed error rather than a silent
    // no-op. Always declared (like the M3 admin block above); the real answer
    // is `raft`-gated, and a non-raft build returns "not available".
    /// Attach `node_id` (reachable at `addr`) to `group` as a NON-VOTING
    /// LEARNER (CONCEPT:EG-KG.storage.kg-kg-2). Starts replication immediately and BLOCKS
    /// until the learner's log is caught up, but does NOT change the voter
    /// set — quorum size and fault tolerance are unaffected. The safe,
    /// always-available first step before optionally promoting the node with
    /// `RaftChangeMembership`. `group` defaults to the single-group
    /// deployment's `raft::DEFAULT_GROUP` (0) when omitted. Returns `Bool` on
    /// success.
    RaftAddLearner {
        group: Option<u64>,
        node_id: u64,
        addr: String,
    },
    /// Set `group`'s VOTER set to exactly `voters` (CONCEPT:EG-KG.storage.kg-kg-2) — openraft
    /// `change_membership`. The usual way to PROMOTE one or more learners
    /// added via `RaftAddLearner`: pass the full desired voter set (existing
    /// voters plus the learner(s) being promoted). Refuses to produce an
    /// empty voter set. `group` defaults to `raft::DEFAULT_GROUP` (0) when
    /// omitted. Returns `Bool` on success.
    RaftChangeMembership {
        group: Option<u64>,
        voters: Vec<u64>,
    },

    // ── Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1) ──────
    // Engine-authoritative client-side cluster discovery, replacing the static
    // hand-maintained `GRAPH_RAFT_GROUP_ENDPOINTS` map (`reports/wave1/ADR-scale-trio.md`
    // §ADR-1). Each node self-reports its own `{node_id, raft_addr,
    // advertised_client_addr, tls_server_name}` via `NodeInfoUpsert`; every node's
    // durable copy converges because the SAME committed log entry applies
    // deterministically on every replica (see
    // `server::persistence::node_info_store` docs) -- the SAME replication story
    // `CatalogAssign` uses for the M3 tenant catalog, NOT graph nodes (the
    // placement catalog's O(N) full-scan lesson). `ClusterMembers` cross-references
    // that durable store against every live `MultiRaft` group's membership/leader
    // to answer a complete topology snapshot from ANY reachable node -- not just
    // the leader, unlike `PlacementRoute` -- so a client's bounded seed-retry (ADR-1
    // decision 3/4) can re-resolve via any healthy contact.
    /// Read the current cluster topology (CONCEPT:EG-KG.sharding.cluster-topology): every known Raft
    /// group's members, each with its role (`leader`/`follower`/`learner`) and
    /// client-reachable endpoint. Gated `cluster:topology-read` -- NOT
    /// `admin:cluster-read` -- so an ordinary service role (not just a cluster
    /// operator) can discover where to reconnect after a failover. Always declared;
    /// a non-raft build or a raft build with no live `MultiRaft` answers a
    /// well-formed empty topology rather than an error. Returns JSON
    /// `{groups: [{group_id, members: [{node_id, role, client_endpoint, tls_name}]}],
    /// epoch}`.
    ClusterMembers,
    /// Self-report this node's identity into the durable, Raft-replicated cluster
    /// topology (CONCEPT:EG-KG.sharding.cluster-topology). Issued by each node at Raft startup
    /// (`raft::node::start`), never by an ordinary client. `advertised_client_addr`
    /// is this node's client-reachable address for the served wire protocol
    /// (`EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR`); `tls_server_name` is the
    /// optional TLS SNI/cert hostname a client should verify when connecting to it.
    /// Idempotent (a repeat upsert for the same `node_id` simply overwrites its
    /// row). Raft/cluster only; a non-raft build returns a typed "not available"
    /// error. Returns `Bool` on success.
    NodeInfoUpsert {
        node_id: u64,
        raft_addr: String,
        advertised_client_addr: String,
        tls_server_name: Option<String>,
    },

    // ── Fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──────
    // Push-registration + lease-TTL heartbeat for fleet MCP/agent servers, writing
    // REAL, queryable knowledge-graph `:Server` nodes -- unlike `NodeInfoUpsert`
    // above (deliberately NOT graph nodes, the placement O(N)-scan lesson), a
    // `:Server` node IS a first-class KG entity the fleet queries (`MATCH
    // (s:Server)-[:PROVIDES]->(r:CallableResource)`), the SAME shape
    // `agent_utilities.knowledge_graph.core.engine_ingestion.ingest_mcp_server`
    // writes today via `MERGE (s:Server {id: $id}) SET s.name=…, s.url=…,
    // s.timestamp=…` (Cypher, `node_type: "Server"` is the canonical label field
    // eg-query's CREATE/MERGE path sets — see `crates/eg-query/src/cypher/exec.rs`
    // `relationship_fixture`). `RegisterServer` self-translates into
    // `Method::AddNode` against `__commons__` (see `dispatch.rs`), reusing the
    // existing durable-commit + CDC + audit machinery byte-for-byte — the SAME
    // "translate then delegate" shape `Method::ApplyMultisigMutation` uses for
    // `Method::ApplyMutation`. Raft/cluster native-consensus wiring is a tracked
    // follow-up (`reports/issue-register.md`); single-node/`full` (the shipped
    // build) is fully wired.
    /// Push-register (or renew, idempotently) this server's fleet identity
    /// (CONCEPT:EG-KG.sharding.server-registry) as a `:Server` node in
    /// `__commons__`. `name` becomes the node id `srv:<name>` (must match
    /// `^[A-Za-z0-9_.-]{1,128}$`, the same bound au's config-sync ingestion
    /// enforces); `url` is a bounded opaque endpoint reference (never a raw
    /// credentialed URL — callers pass the same kind of privacy-safe reference
    /// au's `persistence_reference` produces); `resources_json` is an optional,
    /// size-bounded opaque JSON object (non-sensitive metadata, mirrors au's
    /// `_mcp_persistence_resources`); `ttl_secs` is the caller's requested lease
    /// duration (bounded server-side). The SERVER computes the absolute
    /// `lease_expires_at_ms`/`last_heartbeat_ms` from its own clock — it never
    /// trusts a caller-supplied timestamp. Re-calling with the SAME `name`
    /// renews the lease (a heartbeat is just a repeat `RegisterServer` call) and
    /// refreshes every other field, exactly like `NodeInfoUpsert`'s self-report
    /// semantics; `registered_at_ms` is preserved from the prior row if one
    /// exists. A periodic engine sweep expires (removes) a `:Server` node whose
    /// lease has lapsed, emitting a CDC `RemoveNode` event. Returns `Bool` on
    /// success.
    RegisterServer {
        name: String,
        url: String,
        #[serde(default)]
        resources_json: String,
        ttl_secs: u64,
    },

    // ── Placement-catalog wire consumption (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4) ──
    // Exposes the engine's sole placement authority over the wire. The response is
    // complete even for an unplaced/single-node partition, so callers never hash or
    // guess. A configured Raft node without MultiRaft is an invalid cluster.
    /// Resolve `(tenant, sub_key)`'s current placement (CONCEPT:EG-KG.sharding.placement-route-rpc). `client_epoch`
    /// is the caller's last-known routing epoch for this partition (`0` if never
    /// resolved). Returns the schema-generated `PlacementRoute`. A placed route
    /// always has a non-zero epoch; an authoritative unplaced route uses epoch zero.
    PlacementRoute {
        request: crate::epistemic_operations::PlacementRouteRequest,
    },

    // ── Placement-catalog ADMIN mutations (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc,
    // DIST-P2-5) ─────────────────────────────────────────────────────────────
    // Before this existed, `PlacementCatalog`'s assign/split/merge/online-move machinery
    // (`src/raft/placement.rs`, `src/raft/reshard.rs::TenantManager`) was reachable ONLY
    // from in-process Rust (tests/harnesses) -- there was no wire method to actually
    // TRIGGER a placement decision or an online move from outside the engine process,
    // even on a real multi-group Raft cluster. This closes that gap: a thin RPC entry
    // point over the ALREADY-PROVEN `MultiRaft`/`TenantManager` admin API
    // (`src/server/handlers/placement.rs`), raft/cluster-only, admin-scoped
    // (`"admin:cluster"`, the same tier as `Reshard`/`CatalogAssign`). ONE `Method`
    // variant carrying a nested op enum (mirrors `ServedModality { op }` above) rather
    // than three flat variants, keeping the top-level `Method` enum's growth to +1.
    /// Drive the placement-catalog admin API (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc):
    /// [`PlacementAdminOp::Assign`] (the placement DECISION leg), [`PlacementAdminOp::Move`]
    /// (the full PLAN → EXECUTE → CATALOG-UPDATE leg — snapshot → per-graph
    /// durability-barrier catch-up → fenced cutover, reusing the already-proven
    /// `TenantManager::move_partition` state machine, crash-safe via its durable move
    /// journal), or [`PlacementAdminOp::AbortMove`] (roll back before the cutover
    /// fence). Raft/cluster only; a non-clustered build returns a typed "not available"
    /// error. Returns operation-specific JSON — see [`PlacementAdminOp`].
    PlacementAdmin {
        op: PlacementAdminOp,
    },

    // ── Online backup / restore + PITR (CONCEPT:EG-KG.sharding.reshard-on-restore) ──────────────
    // The wire surface for the DR ops the durable store now supports: an ONLINE
    // consistent backup (per-shard begin_read() MVCC snapshot, EG-027, streamed
    // verbatim to a portable bundle reusing EG-030's raw-row copy) and a restore
    // (verbatim import via the EG-030 engine). Redb-only; in a non-redb build they
    // return a clean "not available" error, exactly like the EG-038 admin surface.
    /// Take an ONLINE consistent backup under the operator-provisioned
    /// `EPISTEMIC_GRAPH_BACKUP_ROOT`. `destination` is a bounded logical bundle name,
    /// never a host path. The RPC is disabled when no private root is configured.
    Backup {
        destination: String,
        label: Option<String>,
    },
    /// Restore the logical bundle name `source` from the operator-provisioned backup
    /// root (CONCEPT:EG-KG.sharding.reshard-on-restore). The engine holds an
    /// exclusive lock on its live store, so this stages the rebuilt copy in an
    /// engine-owned sibling directory and returns only an opaque stage reference for
    /// the operator to correlate after stopping the engine;
    /// an in-place restore uses the offline `restore` CLI. Returns a `RestoreReport` JSON.
    Restore {
        source: String,
        /// Required current target layout. Setting this to a different value from the
        /// bundle proves restore-time migration rather than silently preserving K.
        target_shards: usize,
    },

    // ── Dynamic Communication Channels ───────────────────────────────
    CreateChannel {
        channel_id: String,
        channel_type: ChannelType,
        creator: String,
        initial_members: Vec<String>,
    },
    JoinChannel {
        channel_id: String,
        agent_id: String,
    },
    LeaveChannel {
        channel_id: String,
        agent_id: String,
    },
    CloseChannel {
        channel_id: String,
        /// Optional embedding of the conversation summary.
        summary_embedding: Option<Vec<f32>>,
        /// Optional topic/metadata for the KG imprint.
        topic_metadata: Option<String>,
    },
    SendMessage {
        channel_id: String,
        sender: String,
        payload: String,
    },
    GetChannelMessages {
        channel_id: String,
        limit: Option<usize>,
    },
    ListChannels,
    GetChannelMembers {
        channel_id: String,
    },

    // ── Service-Level ────────────────────────────────────────────────
    Ping,
    Health,
    Shutdown,
    /// Cooperatively cancel an IN-FLIGHT request by its `target_req_id` (CONCEPT:EG-KG.query.streaming-spillable-collect,
    /// L36) — trips the `CancellationToken` the request-scoped registry (`server::request_cancel`)
    /// registered for it, if one is still live. A REAL `Method::Sql` read currently threads a
    /// registered token down to `collect_streaming`, which observes it at the next batch
    /// boundary and stops the stream short (chunk-granular, never mid-batch). Returns
    /// `ResultPayload::Bool(true)` iff a live cancellable request was found and cancelled,
    /// `false` when the request already finished, was never cancellable, or never existed —
    /// never an error (cancelling a request that already completed is a harmless no-op).
    CancelRequest {
        target_req_id: u64,
    },

    // ── Cost / Efficiency (CONCEPT:EG-KG.compute.lane-v, Lane V) ──────────────────
    /// Return a structured resource snapshot for autoscaling: per-graph + per-tenant
    /// resident memory, node/edge counts, queue depth / in-flight, hibernated-vs-
    /// resident counts, eviction rate, plus a process-wide aggregate. The signals an
    /// external autoscaler (agent-utilities OS-5.27) consumes to scale shards. Read
    /// via `ResultPayload::Json` (a `ResourceSnapshot`). Gated by `cost`; a build
    /// without it falls to the dispatch "not available" catch-all.
    #[cfg(feature = "cost")]
    ResourceStats,
    Reconcile {
        graph_name: String,
        #[serde(with = "serde_bytes")]
        msgpack: Vec<u8>,
    },
    ApplyMutation {
        event_type: String,
        query: String,
    },
    /// VF2 subgraph isomorphism match of `pattern_graph_name` against this graph
    /// (CONCEPT:EG-KG.mining.gspan-frequent-subgraph). The backtracking search is NP-hard with no bound
    /// otherwise, so it stops early once it collects `max_results` matches or spends
    /// `max_steps` candidate-pair attempts (whichever first). `0` for either ⇒ the
    /// engine's conservative built-in default (`eg_core::graph::DEFAULT_VF2_MAX_RESULTS`/
    /// `DEFAULT_VF2_MAX_STEPS`) — a caller wanting more must ask for it explicitly.
    /// Returns a `Raw`-encoded [`Vf2MatchResult`].
    Vf2SubgraphMatch {
        pattern_graph_name: String,
        #[serde(default)]
        max_results: usize,
        #[serde(default)]
        max_steps: usize,
    },

    // ── AST Parsing ──────────────────────────────────────────────────
    ParseFile {
        file_path: String,
        #[serde(with = "serde_bytes")]
        source: Vec<u8>,
    },
    /// Batched parse: one round-trip for N files (CONCEPT:EG-KG.memory.forgetting-curve-decay). The blob is
    /// a MessagePack-encoded `Vec<(file_path, source_bytes)>`; the response is an
    /// ordered `Vec<ParseResult>`, one per input file. Mirrors `BatchUpdate`.
    ParseFiles {
        #[serde(with = "serde_bytes")]
        files_msgpack: Vec<u8>,
    },
    /// Parse a batch AND resolve cross-file call/import edges in one round-trip
    /// (CONCEPT:EG-KG.compute.turn-each-project). The blob is the same MessagePack `Vec<(file_path,
    /// source_bytes)>` as `ParseFiles`, but the batch is treated as one
    /// resolution scope (a repository, or a delta set): the response is a SINGLE
    /// resolved `IndexResult` whose `calls`/`depends_on` edges point at real node
    /// ids, not bare names. Use this (not `ParseFiles`) to ingest a repo's symbol
    /// graph; use `ParseFiles` only when per-file raw results are wanted.
    IndexRepository {
        #[serde(with = "serde_bytes")]
        files_msgpack: Vec<u8>,
    },

    // ── Screen Observation (computer-use) ─────────────────────────────
    /// Turn a captured desktop frame into durable session/frame/UIElement graph
    /// entities in one round-trip (CONCEPT:AU-KG.ontology.owl-screen-bridge). The blob is a MessagePack map
    /// `{session_id, frame_seq, prev_frame_id, prev_hash, png: bin, elements: [..]}`;
    /// the response is a SINGLE `ScreenObservationResult` (nodes + edges), mirroring
    /// `IndexRepository`. The screenshot bytes never persist — only its dimensions +
    /// content hash do, for frame-diff.
    ObserveScreen {
        #[serde(with = "serde_bytes")]
        obs_msgpack: Vec<u8>,
    },

    // ── Semantic Compute ─────────────────────────────────────────────
    AddEmbedding {
        node_id: String,
        embedding: Vec<f32>,
    },
    SemanticSearch {
        query_embedding: Vec<f32>,
        n_results: usize,
    },
    /// CONCEPT:EG-KG.retrieval.one-round-trip-discovery — one-round-trip hybrid discovery. Given the caller's
    /// de-duplicated `keywords` plus a `query_embedding`, dense-retrieve candidate
    /// nodes via the HNSW index (the same batch primitive as `SemanticSearch`),
    /// then re-rank each by BOTH its semantic similarity AND lexical keyword
    /// overlap over its `name`/`description`/`type`, returning the top-`k` with
    /// their human-readable text as `[{id,name,description,type,score}, …]`.
    /// Complements `SemanticSearch` (which returns bare `(id, score)`): Discover
    /// folds the keyword signal in and hydrates the result text in one call, so a
    /// router/orchestrator gets a ready-to-read shortlist without an N+1 metadata
    /// fetch. An empty `query_embedding` (embedder/vLLM unavailable) degrades to a
    /// bounded keyword-only scan.
    Discover {
        keywords: Vec<String>,
        query_embedding: Vec<f32>,
        k: usize,
    },
    /// CONCEPT:EG-ORCH.routing.lexical-capability-escalation — embedding-free lexical classification gate: which
    /// capability-node terms (Tool/Skill/MCPServer names+synonyms) appear in the
    /// query. The "free" tier between structural routing and `SemanticSearch`.
    MatchOntologyTerms {
        query: String,
    },
    /// CONCEPT:EG-KG.compute.l2-normalize-batch-vectors — L2-normalize a batch of vectors IN-ENGINE via the `eg-numeric`
    /// kernel (compute-near-data over a resident vector set): returns each row's unit
    /// vector `v/‖v‖` (feature `numeric`).
    BatchL2Normalize {
        vectors: Vec<Vec<f64>>,
    },

    // ── Quantitative Finance ──────────────────────────────────────────
    FinanceOptimizePortfolio {
        expected_returns: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        risk_free_rate: f64,
        min_weight: Option<f64>,
        max_weight: Option<f64>,
    },
    FinanceRiskParity {
        cov_matrix: Vec<Vec<f64>>,
    },
    FinanceBlackLitterman {
        market_weights: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        views: Vec<f64>,
        pick_matrix: Vec<Vec<f64>>,
        tau: f64,
        risk_aversion: f64,
    },
    FinanceEfficientFrontier {
        expected_returns: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        target_return: f64,
    },

    // ── Data Science Primitives (CONCEPT:EG-KG.compute.rust-native-training-loss) ─────────────────────
    DsLinearRegression {
        x: Vec<Vec<f64>>,
        y: Vec<f64>,
    },
    DsKMeans {
        data: Vec<Vec<f64>>,
        k: usize,
        max_iter: usize,
    },
    DsPca {
        data: Vec<Vec<f64>>,
        n_components: usize,
    },
    DsComputeStats {
        data: Vec<Vec<f64>>,
    },
    DsTrainTestSplit {
        data: Vec<Vec<f64>>,
        labels: Vec<f64>,
        test_ratio: f64,
        shuffle: bool,
        seed: u64,
    },
    // These two variants embed `datascience` domain types, so they are gated with
    // the feature — a slim server without `datascience` simply doesn't know them.
    #[cfg(feature = "datascience")]
    DsFitEstimator {
        estimator: String,
        x: Vec<Vec<f64>>,
        y: Vec<f64>,
        #[serde(default)]
        params: crate::wire::EstimatorParams,
    },
    #[cfg(feature = "datascience")]
    DsPredictEstimator {
        model: crate::wire::FittedModel,
        x: Vec<Vec<f64>>,
    },

    // ── Training loss / optimizer kernels (CONCEPT:EG-KG.compute.rust-native-training-loss) ────────────
    DsSoftmax {
        logits: Vec<f64>,
        temperature: f64,
    },
    DsLogSoftmax {
        logits: Vec<f64>,
    },
    DsCrossEntropy {
        logits: Vec<Vec<f64>>,
        labels: Vec<usize>,
    },
    DsDpoLoss {
        policy_chosen: Vec<f64>,
        policy_rejected: Vec<f64>,
        ref_chosen: Vec<f64>,
        ref_rejected: Vec<f64>,
        beta: f64,
    },
    DsGrpoSurrogate {
        logprob: Vec<f64>,
        old_logprob: Vec<f64>,
        advantage: Vec<f64>,
        clip_eps: f64,
    },
    DsKlDivergence {
        logprob: Vec<f64>,
        ref_logprob: Vec<f64>,
    },
    DsAdamStep {
        params: Vec<f64>,
        grads: Vec<f64>,
        m: Vec<f64>,
        v: Vec<f64>,
        lr: f64,
        beta1: f64,
        beta2: f64,
        eps: f64,
        t: u64,
    },
    DsSgdStep {
        params: Vec<f64>,
        grads: Vec<f64>,
        lr: f64,
    },

    // ── Extended Finance: Risk (CONCEPT:AU-KG.memory.mementified-context) ──────────────────────
    FinanceVar {
        returns: Vec<f64>,
        confidence: f64,
    },
    FinanceCvar {
        returns: Vec<f64>,
        confidence: f64,
    },
    FinanceMaxDrawdown {
        returns: Vec<f64>,
    },
    FinanceDrawdownSeries {
        returns: Vec<f64>,
    },
    FinanceDownsideDeviation {
        returns: Vec<f64>,
        target: f64,
    },
    FinanceRiskMetrics {
        returns: Vec<f64>,
        risk_free_rate: f64,
    },
    FinanceMonteCarloVar {
        mean: f64,
        std_dev: f64,
        n_simulations: usize,
        confidence: f64,
    },
    FinanceStressTest {
        weights: Vec<f64>,
        expected_returns: Vec<f64>,
        cov_matrix: Vec<Vec<f64>>,
        shock_factors: Vec<f64>,
    },

    // ── Extended Finance: Regime detection (HMM) ──────────────────────
    FinanceDetectRegimes {
        observations: Vec<f64>,
        n_states: usize,
        max_iter: usize,
        tol: f64,
    },

    // ── Extended Finance: Signals / alpha ─────────────────────────────
    FinanceRollingZscore {
        values: Vec<f64>,
        window: usize,
    },
    FinanceEwma {
        values: Vec<f64>,
        span: usize,
    },
    FinanceSignalDecay {
        signal: Vec<f64>,
        half_life: f64,
    },
    FinanceCombineAlphas {
        signals: Vec<Vec<f64>>,
        weights: Vec<f64>,
    },
    FinanceCrossSectionalRank {
        cross_section: Vec<Vec<f64>>,
    },
    FinanceMomentum {
        prices: Vec<f64>,
        lookback: usize,
    },
    FinanceMeanReversion {
        values: Vec<f64>,
        window: usize,
    },
    FinanceInformationCoefficient {
        signal: Vec<f64>,
        forward_returns: Vec<f64>,
    },

    // ── Extended Finance: Execution / microstructure ──────────────────
    FinanceTwap {
        total_quantity: f64,
        n_slices: usize,
        start_time: u64,
        interval_secs: u64,
    },
    FinanceVwap {
        total_quantity: f64,
        volume_profile: Vec<f64>,
        start_time: u64,
        interval_secs: u64,
    },
    FinanceMarketImpact {
        daily_volatility: f64,
        order_quantity: f64,
        average_daily_volume: f64,
        impact_coefficient: f64,
    },
    FinancePairsTrading {
        prices_a: Vec<f64>,
        prices_b: Vec<f64>,
        lookback: usize,
    },
    // Embeds a `finance` domain type → gated with the feature.
    #[cfg(feature = "finance")]
    FinanceMatchOrders {
        orders: Vec<crate::wire::Order>,
    },

    // ── Market Making / Microstructure (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ─────────────
    FinanceAvellanedaStoikov {
        mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        tau: f64,
    },
    FinanceGltQuotes {
        mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        a: f64,
    },
    FinanceLogitQuotes {
        p_mid: f64,
        inventory: f64,
        sigma: f64,
        gamma: f64,
        kappa: f64,
        tau: f64,
        boundary_m: f64,
    },
    FinanceGlostenMilgromSpread {
        alpha: f64,
        p: f64,
    },
    FinanceExpectedPnlRate {
        delta: f64,
        a: f64,
        kappa: f64,
        alpha: f64,
        p: f64,
        v_h: f64,
        v_l: f64,
    },
    FinanceBreakevenAlpha {
        delta: f64,
        p: f64,
        v_h: f64,
        v_l: f64,
    },
    FinanceOfiSeries {
        ts: Vec<f64>,
        bid_px: Vec<f64>,
        bid_sz: Vec<f64>,
        ask_px: Vec<f64>,
        ask_sz: Vec<f64>,
        window_secs: f64,
    },
    FinanceMicropriceSeries {
        bid_px: Vec<f64>,
        bid_sz: Vec<f64>,
        ask_px: Vec<f64>,
        ask_sz: Vec<f64>,
    },
    FinanceVpinPm {
        buy_vol: Vec<f64>,
        sell_vol: Vec<f64>,
        p_mean: Vec<f64>,
    },
    FinanceHawkesMle {
        times: Vec<f64>,
        t_horizon: f64,
        max_iter: usize,
    },
    FinanceHardimanBouchaud {
        times: Vec<f64>,
        t_horizon: f64,
        n_windows: usize,
    },

    // ── Kyle insider/stealth surveillance (CONCEPT:EG-KG.domains.concept-2) ──────────
    FinanceKyleLambda {
        price_changes: Vec<f64>,
        signed_order_flow: Vec<f64>,
    },
    FinanceSurveillanceRisk {
        buy_vol: Vec<f64>,
        sell_vol: Vec<f64>,
        p_mean: Vec<f64>,
        signed_flow: Vec<f64>,
        price_changes: Vec<f64>,
        baseline_sigma: f64,
    },

    // ── Position Sizing (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ────────────────────────────
    FinanceKellyFraction {
        q: f64,
        c: f64,
        fraction: f64,
    },
    FinanceBayesianKelly {
        alpha: f64,
        beta: f64,
        c: f64,
        n_quadrature: usize,
    },
    FinancePosteriorCredibleInterval {
        alpha: f64,
        beta: f64,
        level: f64,
    },

    // ── Backtest Validation (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ────────────────────────
    FinancePurgedCpcv {
        n_samples: usize,
        n_groups: usize,
        n_test_groups: usize,
        purge_window: usize,
        embargo: usize,
    },
    FinanceDeflatedSharpe {
        observed_sr: f64,
        n_trials: usize,
        sr_returns: Vec<f64>,
    },
    FinanceProbabilityBacktestOverfit {
        insample: Vec<Vec<f64>>,
        oos: Vec<Vec<f64>>,
    },
    FinanceDieboldMariano {
        losses_a: Vec<f64>,
        losses_b: Vec<f64>,
        h: usize,
    },

    // ── Forensic Accounting (CONCEPT:EG-KG.domains.forensic-accounting-kernels) ────────────────────────
    // Embeds `finance` domain types → gated with the feature.
    #[cfg(feature = "finance")]
    FinanceForensicReport {
        this_year: crate::wire::YearData,
        prior_year: crate::wire::YearData,
    },

    // ── State-Space / Stat-Arb (CONCEPT:EG-KG.domains.state-space-statistical-arbitrage) ─────────────────────
    FinanceKalmanFilter1d {
        observations: Vec<f64>,
        f: f64,
        q: f64,
        h: f64,
        r: f64,
        x0: f64,
        p0: f64,
    },
    FinanceKalmanBeta {
        market_returns: Vec<f64>,
        asset_returns: Vec<f64>,
        q: f64,
        r: f64,
        beta0: f64,
        p0: f64,
    },
    FinanceKalmanVolatility {
        returns: Vec<f64>,
        q: f64,
        r: f64,
        log_var0: Option<f64>,
        p0: f64,
        annualization: f64,
    },
    FinanceAdfTest {
        series: Vec<f64>,
        max_lag: usize,
    },
    FinanceOuCalibrate {
        spread: Vec<f64>,
        dt: f64,
    },
    FinanceOuOptimalThresholds {
        theta: f64,
        mu: f64,
        sigma: f64,
        sigma_eq: f64,
        cost: f64,
    },
    FinanceMarkovTransitionMatrix {
        states: Vec<usize>,
        n_states: usize,
    },

    // ── Signal Combination / Sizing / Calibration (CONCEPT:EG-KG.domains.quant-finance) ──
    FinanceOrderBookImbalance {
        v_bid: Vec<f64>,
        v_ask: Vec<f64>,
    },
    FinanceQueueImbalance {
        bid_q: Vec<f64>,
        ask_q: Vec<f64>,
        bid_rate: Vec<f64>,
        ask_rate: Vec<f64>,
    },
    FinanceRealizedVolTick {
        mid: Vec<f64>,
        window: usize,
    },
    FinanceSpreadReversion {
        bid_px: Vec<f64>,
        ask_px: Vec<f64>,
        window: usize,
    },
    FinanceInformationRatio {
        ic: f64,
        n_independent: f64,
    },
    FinanceEffectiveIndependentN {
        returns_matrix: Vec<Vec<f64>>,
    },
    FinanceAlphaCombinationEngine {
        returns_matrix: Vec<Vec<f64>>,
        lookback: usize,
    },
    FinanceBrierScore {
        forecasts: Vec<f64>,
        outcomes: Vec<f64>,
    },
    FinanceConvergenceGate {
        strengths: Vec<f64>,
        strong_threshold: f64,
        min_agree: usize,
    },
    FinanceEmpiricalKelly {
        p: f64,
        b: f64,
        historical_returns: Vec<f64>,
        n_simulations: usize,
        seed: u64,
    },

    // ── Derivatives: SABR volatility surface (CONCEPT:AU-KG.domains.derivatives) ────────
    FinanceSabrImpliedVol {
        f: f64,
        k: f64,
        t: f64,
        alpha: f64,
        beta: f64,
        rho: f64,
        nu: f64,
    },
    FinanceSabrSmile {
        f: f64,
        strikes: Vec<f64>,
        t: f64,
        alpha: f64,
        beta: f64,
        rho: f64,
        nu: f64,
    },
    FinanceSabrCalibrate {
        f: f64,
        t: f64,
        strikes: Vec<f64>,
        market_vols: Vec<f64>,
        beta: f64,
    },

    // ── Zero-Trust Consensus ─────────────────────────────────────────
    RegisterIdentity {
        agent_id: String,
        role: crate::acl::AgentRole,
        teams: Vec<String>,
        signature: String,
        /// RBAC role names this agent holds (CONCEPT:EG-KG.compute.feature).
        roles: Vec<String>,
    },
    /// Administer the RBAC role/grant policy (CONCEPT:EG-KG.compute.feature). Unconditional in the
    /// enum; the handler is gated behind the `security` feature (a non-security build
    /// falls to the dispatch "not available in this build" catch-all, like EG-090's
    /// backup/restore on a non-redb build).
    RbacAdmin {
        op: crate::acl::RbacAdminOp,
    },
    ApplyMultisigMutation {
        signatures: Vec<String>,
        threshold: usize,
        mutation_type: String,
        query: String,
    },

    /// The durable analytics-job plane (CONCEPT:INT-P2-1): async caller control
    /// plus verified remote-worker claim/renew/checkpoint/stage/publish/cancel over
    /// a coordinator-owned `AnalyticsJob` state machine (`eg-jobs`),
    /// whose eventual success commits a provenance'd `:Claim`/`:Evidence` pair (the
    /// SAME typed-node convention `eg-epistemic` reads). ONE variant wrapping an
    /// internal op enum — mirrors `RbacAdmin { op }` above — so the whole
    /// full surface costs exactly one `Method` arm. Gated `jobs`; the handler
    /// (`src/server/handlers/jobs.rs`)
    /// self-routes in `dispatch.rs` before the per-graph chain (jobs are keyed by
    /// `job_id` in their own `jobs.redb`, not a graph — like `TsAppend`/`Kv*`).
    #[cfg(feature = "jobs")]
    AnalyticsJob {
        op: crate::jobs::JobOp,
    },

    /// The native finite-state-machine / statechart engine (CONCEPT:INT-P2-2):
    /// define/instantiate/send_event/get_state/list over a durable, rehydratable
    /// `MachineInstance` `(state, context)` record in `statecharts.redb` (`eg-statechart`).
    /// ONE variant wrapping an internal op enum — mirrors `AnalyticsJob { op }` above —
    /// so the whole engine surface costs exactly one `Method` arm. Gated `statechart`;
    /// the handler (`src/server/handlers/statechart.rs`) self-routes in `dispatch.rs`
    /// before the per-graph chain (instances are keyed by `instance_id` in their own
    /// `statecharts.redb`, not a graph — like `AnalyticsJob`/`TsAppend`/`Kv*`).
    #[cfg(feature = "statechart")]
    Statechart {
        op: crate::statechart::StatechartOp,
    },

    /// The agent-facing quantum control-plane surface (Q8, CONCEPT:EG-KG.compute.quantum-agent-api):
    /// `quantum_rank`/`optimize_with_qaoa`/`quantum_expectation` over a registered
    /// `eg_quantum_core::backend::QuantumBackend` (today: `eg-quantum-sim`'s
    /// `sv-cpu`/`stabilizer`). ONE variant wrapping an internal op enum — mirrors
    /// `AnalyticsJob { op }`/`Statechart { op }` above — so the whole surface costs
    /// exactly one `Method` arm. Gated `quantum`; the handler
    /// (`src/server/handlers/quantum.rs`) self-routes in `dispatch.rs` before the
    /// per-graph chain (a quantum run reads no persisted graph state and writes
    /// nothing durable — every result is returned to the caller as a proposal,
    /// never committed — like `AnalyticsJob`/`Statechart`/`TsAppend`/`Kv*`).
    #[cfg(feature = "quantum")]
    Quantum {
        op: crate::quantum::QuantumOp,
    },

    /// Native visualization render surface (D-VZ-1 lanes V4 "engine integration" /
    /// V6 "graph-native marks"): resolve a caller-provided `eg_viz_core::ViewSpec`
    /// against a dataset (caller-supplied inline columns, or deterministic
    /// engine-side synthetic data) and render it to static PNG/SVG/PDF bytes, or
    /// fetch the mark x surface capability matrix. ONE variant wrapping an
    /// internal op enum — mirrors `AnalyticsJob { op }`/`Statechart { op }` above
    /// — so the whole render surface costs exactly one `Method` arm. Gated `viz`;
    /// the handler (`src/server/handlers/viz.rs`, facade feature
    /// `viz-static-export`) self-routes in `dispatch.rs` before the per-graph
    /// chain — a render is NOT graph-scoped (it resolves a FRESH per-request
    /// `ColumnStore`, never a live graph read), exactly like `AnalyticsJob`/
    /// `Statechart` above. V4-LITE, not full V4: no tile cache, no provenance
    /// inherited from a durable job, no view over a resident `GraphCore` — see
    /// `crate::viz`'s module doc.
    #[cfg(feature = "viz")]
    Viz {
        op: crate::viz::VizOp,
    },

    // ── Query (SQL + Cypher) ──────────────────────────────────────────
    // Read-only relational query surface (CONCEPT:EG-KG.query.read-only-sql-query). `SELECT … FROM
    // nodes …` over ONE graph via DataFusion, gated behind the facade `query`
    // feature; in a slim build the variant falls to the not-built catch-all.
    // `params_msgpack` is reserved for future bound parameters.
    Sql {
        query: String,
        #[serde(default, with = "serde_bytes")]
        params_msgpack: Vec<u8>,
    },
    // Read-only Cypher query surface (CONCEPT:EG-KG.query.dep-free-behind). A `MATCH … WHERE … RETURN
    // … LIMIT …` over ONE graph, compiled to the engine's own primitives (the
    // eg-core label index, `vf2_subgraph_match`, and petgraph BFS) — NO DataFusion,
    // so it ships in the lean Pi build behind the facade `cypher` feature. Reuses
    // the same `QueryResult` carrier as `Sql` (returned via `ResultPayload::raw`):
    // a Cypher RETURN is the same columns+row-blobs shape, so no new payload
    // variant. In a build without `cypher` the variant falls to the not-built
    // catch-all.
    CypherQuery {
        query: String,
        /// Exact requested execution authority; no implicit or inferred mode.
        mode: CypherMode,
    },
    // Read-only GraphQL query surface (CONCEPT:EG-KG.query.sparql-completeness). A GraphQL `query`
    // operation whose root fields are node TYPES (label-scan + `first`/`limit` +
    // property-equality args) with nested EDGE selections (relationship traversal),
    // compiled to scans + BFS over the SAME GraphView the Cypher executor reads
    // (eg-graphql — pure-Rust, NO async-graphql/DataFusion). Returns the GraphQL
    // `{"data": …}` JSON via `ResultPayload::raw`. Gated behind the facade `graphql`
    // feature (kept OUT of pi/default — async-graphql-free but still node/cluster/
    // full only); in a build without it the variant falls to the not-built catch-all.
    #[cfg(feature = "graphql")]
    GraphQl {
        query: String,
        /// Optional GraphQL `$variables` — a JSON object bound at execution
        /// (CONCEPT:EG-KG.query.fragments-variables-directives variables, wired through the wire path as an EG-064
        /// follow-up). The handler binds these via `execute_with_variables`
        /// (`@skip`/`@include` + `$var` args). `None` is encoded explicitly and means
        /// an empty binding.
        #[serde(deserialize_with = "deserialize_required_option")]
        variables: Option<serde_json::Value>,
    },

    /// Pull one bounded Arrow `KnowledgeBatch` from any served query family.
    /// `cursor=None` opens a snapshot; passing the returned cursor resumes only
    /// when authority, graph snapshot, query, schema and batch size still match.
    /// This is the sole native result contract and returns bounded Arrow IPC.
    #[cfg(feature = "knowledge-batch")]
    KnowledgeStream {
        request: crate::knowledge_stream::KnowledgeStreamRequestV1,
    },

    // ── Unified cross-modal query (CONCEPT:AU-KG.compute.vector/209) ──────────────────
    // ONE plan that filters (relational/DataFusion) → traverses (graph/BFS) →
    // ranks (vector/kNN) over the SAME off-lock snapshot, instead of three siloed
    // round-trips. The `plan` is the serializable [`crate::wire::Plan`] AST (an
    // ordered list of `Scan|Filter|Traverse|Rank|Limit` ops over a shared RowSet);
    // the bespoke planner (eg-plan) sequences the existing legs and applies a
    // cost-based filter-vs-vector reorder (CONCEPT:EG-KG.query.concept-14). Read-only this
    // increment. Gated behind the facade `query` feature (the FILTER leg needs
    // DataFusion); in a slim build the variant falls to the not-built catch-all.
    // Result via `ResultPayload::raw` — a list of `[id, score|nil]` rows.
    #[cfg(feature = "query")]
    UnifiedQuery {
        plan: crate::wire::Plan,
    },

    // ── Unified query, TEXT surface — UQL (CONCEPT:AU-KG.query.top-nodes-by-degree) ────────────────
    // The human/agent-writable counterpart of `UnifiedQuery`: a UQL `text` string
    // (e.g. `MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2} |> RANK BY
    // ~[…] |> LIMIT 10`) that the handler PARSES (eg_plan::uql::parse) into the SAME
    // `wire::Plan` AST `UnifiedQuery` carries, then runs through the IDENTICAL
    // `run_unified` executor — NO new execution path, just a front-end. A parse error
    // becomes a clear error Response. Same `query`-gating + `ResultPayload::raw`
    // (`[id, score|nil]` rows) as `UnifiedQuery`.
    #[cfg(feature = "query")]
    UnifiedQueryText {
        text: String,
    },

    // ── EXPLAIN surfaces (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────────────
    // Diagnostics over the SAME `wire::Plan` `UnifiedQuery` carries — no new execution
    // path, just introspection into what the planner did / would do. Read-only.
    /// `EXPLAIN PLAN` — serialize `plan` as a [`crate::wire::Plan`]::PlanDag conversion
    /// (a linear plan is a degenerate chain, CONCEPT:EG-KG.query.plan-dag) both BEFORE and
    /// AFTER the DAG-aware cost optimizer (`eg_plan::optimizer::optimize_dag`), plus the
    /// active rule set (`eg_plan::cost_opt_rule_names()`) — the optimizer rewrite trace.
    /// Returns an `ExplainPlanResult` via `ResultPayload::raw`. Gated `query` (same as
    /// `UnifiedQuery`).
    #[cfg(feature = "query")]
    ExplainPlan {
        plan: crate::wire::Plan,
    },
    /// `EXPLAIN PROVENANCE` — run `plan` and, for each result row, resolve its
    /// EVIDENCE-FOR provenance (the SAME belief-substrate `EvidenceFor` resolution E2's
    /// `Op::EvidenceFor` op runs) over the `KnowledgeSet` (E3) row shape. With the
    /// `epistemic` feature OFF (or absent at runtime) every row's provenance is empty and
    /// `resolved` is `false` — the documented "no epistemic resolution ran" behavior E3's
    /// `KnowledgeSet` already carries (CONCEPT:EG-KG.query.knowledge-set). Returns an
    /// schema-generated `EvidenceBundle` via `ResultPayload::raw`. Gated `query`.
    #[cfg(feature = "query")]
    ExplainProvenance {
        plan: crate::wire::Plan,
    },
    /// `EXPLAIN PROVENANCE BY IDS` (CONCEPT:EG-KB-CURRENCY) — the ID-seeded sibling of
    /// `ExplainProvenance`: skip the `Plan`/`Op` algebra entirely and resolve the SAME
    /// protocol evidence claims directly for `ids` — the
    /// shape a caller that already has a set of node ids from ANY other read path
    /// (a Cypher `MATCH`, a SQL `SELECT`, a prior `UnifiedQuery`) needs to "currency-
    /// upgrade" a plain id list into calibrated, cited, time-versioned rows without
    /// hand-building an `Op` plan first. `ids` is deduplicated, first-occurrence order
    /// preserved (mirrors `RowSet::from_ids`); an id absent from the graph is silently
    /// skipped (never fabricated). Returns an `EvidenceBundle` via
    /// `ResultPayload::raw`, byte-identical in shape to `ExplainProvenance`'s. Gated
    /// `query` (same as `ExplainProvenance`).
    #[cfg(feature = "query")]
    ExplainProvenanceByIds {
        ids: Vec<String>,
    },
    /// `EXPLAIN POLICY` — run `plan` against BOTH the caller's RLS-filtered snapshot and
    /// the UNFILTERED snapshot (reusing the SAME `eg_core::isolation::IsolationLayer`
    /// `filter_view` every read path already applies), reporting which result rows the
    /// policy DENIED. With the `security` feature off (or no caller/RLS configured), no
    /// filtering applies and `policy_denied_ids` is always empty. Returns an
    /// `ExplainPolicyResult` via `ResultPayload::raw`. Gated `query`.
    #[cfg(feature = "query")]
    ExplainPolicy {
        plan: crate::wire::Plan,
    },
    /// `EXPLAIN BELIEF <node_id>` — the FULL, un-flattened E1 justification tree
    /// (`eg_epistemic::JustificationGraph`, via `eg_plan::explain_belief_tree`) rooted at
    /// `node_id` — the standalone verbatim-tree surface E2's plan-`Op::ExplainBelief`
    /// (a flat `RowSet` projection) documents as a follow-up, mirroring
    /// `Method::OwlExplain`'s `ProofNodeWire`. Returns an `ExplainBeliefResult` via
    /// `ResultPayload::raw`. Gated `epistemic` (which implies `query`).
    ///
    /// `disclosure_level` (EPI-P3-4, L51) is `None` by default — the DEFAULT PATH IS
    /// UNCHANGED: the handler runs the classic un-redacted `explain_belief` and returns
    /// `ExplainBeliefResult` exactly as before this field existed. When `Some(_)`, the
    /// caller opts INTO the policy-aware, RLS-redacted proof
    /// (`eg_epistemic::redact::explain_belief_redacted`, feature `epistemic-redaction`
    /// on the facade, which pulls `eg-core/security`) — the handler then returns an
    /// `ExplainBeliefRedactedResult` INSTEAD of `ExplainBeliefResult` in the SAME
    /// `ResultPayload::raw` slot (the caller who set this field knows to decode the
    /// other type). The requested level is a CAP, never a grant: a caller may ask for a
    /// STRICTER view than their own RLS access earns (e.g. always request
    /// `ExistenceOnly` for a privacy-conscious display) but can never loosen what
    /// `explain_belief_redacted` computes from their actual access — see
    /// `eg_epistemic::redact` module docs. If `epistemic-redaction` is OFF at build
    /// time, a request naming `Some(_)` gets an explicit error response (never a silent
    /// fall-back to the un-redacted tree — that would leak exactly what redaction
    /// exists to hide).
    #[cfg(feature = "epistemic")]
    ExplainBelief {
        node_id: String,
        #[serde(default)]
        disclosure_level: Option<DisclosureLevelWire>,
    },
    /// The Phase-3 acceptance capstone (EPI-P3-5, L53): "what do we believe, why, on
    /// exactly which evidence, under whose authority, at what time, with what
    /// uncertainty, and what would invalidate it" — for `node_id`, in ONE typed call
    /// (`eg_epistemic::epistemic_status`, feature `epistemic-tms`). Composes belief
    /// (`is_believed`/confidence), the proof tree (`why`), the diagnostic (`why_not`,
    /// populated iff not believed), the counterfactual (`what_evidence_would_change_this`),
    /// and this claim's own bitemporal window — every sibling facet
    /// `eg_epistemic::query` exposes for ONE claim, so those are not separately wired
    /// as their own `Method`s (a caller wanting just one gets it off this result).
    /// Returns an `EpistemicStatusResult` via `ResultPayload::raw`. Gated `epistemic`
    /// at the wire level; the HANDLER additionally requires `epistemic-tms` — a build
    /// with `epistemic` but not `epistemic-tms` falls to the graph_ops "not available
    /// in this build" catch-all (same convention as every other feature-gated arm).
    #[cfg(feature = "epistemic")]
    EpistemicStatus {
        node_id: String,
    },
    /// **what_changed**(tx_from, tx_to) (EPI-P3-5, L53): between two transaction times,
    /// which beliefs changed and why (`eg_epistemic::what_changed`, feature
    /// `epistemic-tms`) — the one acceptance-query facet that is NOT a sub-field of
    /// `EpistemicStatus` (it is a whole-graph temporal DIFF, not a single claim's
    /// status), so it gets its own `Method`. Returns a `WhatChangedResult` via
    /// `ResultPayload::raw`. Same build-tier fallback convention as `EpistemicStatus`.
    #[cfg(feature = "epistemic")]
    WhatChanged {
        tx_from: u64,
        tx_to: u64,
    },
    /// Fenced recompute/writeback for one stale materialization. The expected source
    /// graph version must exactly match the durable reasoning projection watermark.
    /// Replicated serving commits an opaque recompute intent with the authoritative
    /// graph version fence, then the durable outbox worker resolves provenance from
    /// the graph post-image and fsyncs the side projection before acknowledging it.
    /// A late recompute fails with `STALE_RECOMPUTE_FENCE` rather than overwriting a
    /// newer invalidation.
    #[cfg(feature = "epistemic")]
    RecomputeMaterialization {
        derived_id: String,
        expected_source_graph_version: u64,
    },
    /// Seam 3 — query the CURRENT status (`"Fresh"`/`"Stale"`/`"Retracted"`, or
    /// absent if never registered) of a materialization tracked on the SAME
    /// per-graph durable incremental reasoning projection. Read-only — does not
    /// itself recompute anything. Returns a
    /// `MaterializationStatusResult` via `ResultPayload::raw`. Same build-tier
    /// fallback convention as `RecomputeMaterialization`.
    #[cfg(feature = "epistemic")]
    MaterializationStatus {
        id: String,
    },
    /// Seam 3 follow-up (SURPASS gap-closure: "give staleness a consumer") — the bulk
    /// counterpart of [`Method::MaterializationStatus`]: every opaque materialization
    /// reference CURRENTLY `Stale` in this graph's durable projection.
    /// Same build-tier fallback convention as `MaterializationStatus`.
    #[cfg(feature = "epistemic")]
    StaleMaterializations,
    /// EPI-P3-7 (gap-fill) — standalone paraconsistent conflict resolution: run Dung
    /// abstract-argumentation semantics (`eg_epistemic::tms`, feature `epistemic-tms`)
    /// over a `BeliefGraph` built from the caller's `GraphView`, and report — for each
    /// of `node_ids` — whether it SURVIVES, is DEFEATED, or stays UNDECIDED under
    /// `semantics` (`"grounded"` (default) | `"preferred"` | `"stable"`). This is the
    /// SAME grounded/preferred/stable extension machinery `Method::EpistemicStatus`
    /// already composes internally (via `is_skeptically_accepted`) for a single claim's
    /// acceptance — reachable here as a standalone, multi-claim, semantics-selectable
    /// op instead of only inside that capstone. Returns a `ResolveConflictResult`
    /// (surviving/defeated/undecided id lists + the raw extension set(s) the verdict
    /// was computed from) via `ResultPayload::raw`. Gated `epistemic` at the wire
    /// level; the HANDLER additionally requires `epistemic-tms` — a build with
    /// `epistemic` but not `epistemic-tms` falls to the graph_ops "not available in
    /// this build" catch-all (same convention as `EpistemicStatus`/`WhatChanged`).
    #[cfg(feature = "epistemic")]
    ResolveConflict {
        node_ids: Vec<String>,
        #[serde(default = "default_argumentation_semantics")]
        semantics: String,
    },
    /// X-1 (CONCEPT:EG-X1) — resolve `node_id`'s cited multimodal evidence: build a
    /// `BeliefGraph` off the caller's `GraphView` and walk the SAME support/
    /// contradiction/attack topology `ExplainBelief` walks, returning every
    /// transitively-reachable node that carries one complete governed `EvidenceLocus`
    /// (page region, audio/video interval, row version, code range, trace span, …).
    /// The locus itself carries the opaque subject, policy, and derivation references
    /// (`eg_epistemic::evidence_citations`, feature `evidence-graph`) — "here is
    /// exactly where in the source this claim's evidence came from." Returns an
    /// `ExplainEvidenceResult` via `ResultPayload::raw`. Gated `epistemic` at the wire
    /// level (implies `query`); the HANDLER additionally requires `evidence-graph` —
    /// a build with `epistemic` but not `evidence-graph` falls to the graph_ops "not
    /// available in this build" catch-all (same convention as `EpistemicStatus`/
    /// `epistemic-tms`).
    #[cfg(feature = "epistemic")]
    ExplainEvidence {
        node_id: String,
    },
    /// EPI-P3-3 — a request-carried linear-Gaussian structural causal model query:
    /// `variables` defines the DAG's `StructuralEquation`s in topological
    /// (parents-before-children) order — the SAME invariant
    /// `eg_epistemic::CausalGraph::add_variable` enforces at construction. `mode`
    /// (EPI-P3-6) selects which of `eg_epistemic::CausalGraph`'s two
    /// non-counterfactual queries `do_values` feeds:
    ///
    /// * `CausalQueryModeWire::Intervene` — a **do-calculus intervention**
    ///   `P(· | do(X₁=x₁, X₂=x₂, …))`:
    ///   `do_values` fixes the named variables via graph surgery
    ///   (`CausalGraph::intervene`) — incoming edges are CUT, not conditioned on.
    /// * `CausalQueryModeWire::Observe` — the **observational** query
    ///   `P(· | X₁=x₁, X₂=x₂, …)`: ordinary multivariate-Gaussian conditioning on
    ///   the UNMUTILATED joint (`CausalGraph::observe`). Unlike `Intervene`,
    ///   evidence propagates BACKWARD to ancestors too (e.g. a confounder) — the
    ///   mechanism a naive "condition on the evidence" read of a causal question
    ///   gets wrong, and exactly what distinguishes "seeing X=x" from "doing X=x".
    ///
    /// Either way, returns a calibrated `CausalEstimateResult` (mean/variance/
    /// credible-interval per variable, in `variables` order) via
    /// `ResultPayload::raw`. A pure function over request-carried inputs — no graph
    /// snapshot is read. Gated `epistemic` at the wire level; the HANDLER
    /// additionally requires `epistemic-causal` — same build-tier fallback
    /// convention as `ExplainEvidence`.
    ///
    /// The crate's Pearl point-counterfactual (`CausalGraph::counterfactual`) is a
    /// distinct, DETERMINISTIC (not distributional) query with its own request
    /// shape — see `CausalCounterfactual` below, not this variant.
    #[cfg(feature = "epistemic")]
    CausalEstimate {
        variables: Vec<StructuralEquationWire>,
        do_values: std::collections::BTreeMap<String, f64>,
        mode: CausalQueryModeWire,
    },
    /// EPI-P3-6 — Pearl's point-**counterfactual** recipe
    /// (`eg_epistemic::CausalGraph::counterfactual`, feature `epistemic-causal`):
    /// "given that unit `actual` (a FULLY-observed assignment of every variable in
    /// `variables`) really happened, what would its variables have been had
    /// `do_values` held instead?" — the three-step abduction/action/prediction
    /// recipe (Pearl, *Causality*, ch. 7), replaying the SAME inferred exogenous
    /// noise forward through the (surgered) structural equations.
    ///
    /// DETERMINISTIC given `actual` — not a calibrated distribution like
    /// `CausalEstimate` — so it returns a `CausalCounterfactualResult` (one POINT
    /// value per variable, in `variables` order) via `ResultPayload::raw` instead
    /// of a `CausalEstimateResult`. A pure function over request-carried inputs —
    /// no graph snapshot is read. Gated `epistemic` at the wire level; the HANDLER
    /// additionally requires `epistemic-causal` — same build-tier fallback
    /// convention as `CausalEstimate`.
    #[cfg(feature = "epistemic")]
    CausalCounterfactual {
        variables: Vec<StructuralEquationWire>,
        actual: std::collections::BTreeMap<String, f64>,
        do_values: std::collections::BTreeMap<String, f64>,
    },
    /// EPI-P3-3 — provenance-aware retrieval ranking: order request-carried
    /// `candidates` by a weighted blend of similarity AND evidence quality/
    /// provenance (source reliability, corroboration, calibration precision,
    /// freshness) rather than similarity alone (`eg_epistemic::rank`, feature
    /// `epistemic-causal`). A pure function over request-carried inputs — no graph
    /// snapshot is read. Returns a `RankByProvenanceResult` via `ResultPayload::raw`.
    /// Same build-tier fallback convention as `ExplainEvidence`/`CausalEstimate`.
    #[cfg(feature = "epistemic")]
    RankByProvenance {
        candidates: Vec<RetrievalCandidateWire>,
        #[serde(default)]
        weights: RankWeightsWire,
    },

    // ── Natural-language query (CONCEPT:EG-KG.query.core-query-input/EG-080) ─────────────────────
    /// Natural-language → executable query → rows. `text` is the NL request, `graph`
    /// the target graph (the `/nl` HTTP facade path has no request envelope, so the
    /// graph rides the method; over the wire an empty `graph` falls back to the request
    /// envelope's graph). The handler resolves a configured/injected `NlPlanner`, turns
    /// the NL into a UQL query string, and runs it through the IDENTICAL deterministic
    /// `UnifiedQueryText` pipeline (`eg_plan::uql::parse` → the fused executor) — NO new
    /// execution path, and no LLM in the engine core. Result via `ResultPayload::raw` —
    /// the SAME `[id, score|nil]` rows as `UnifiedQuery`.
    ///
    /// UNCONDITIONAL in the enum (like `RbacAdmin`); the HANDLER is gated behind the
    /// facade `nl-query` feature. A build WITHOUT `nl-query` falls to the dispatch "not
    /// available in this build" catch-all — so the wire stays compatible while the NL
    /// surface is a build-tier choice.
    NlQuery {
        text: String,
        #[serde(default)]
        graph: String,
    },

    // ── Query federation / foreign sources (CONCEPT:EG-KG.query.query-federation, Lane P) ───────
    // Register a named EXTERNAL source so a UnifiedQuery `Op::ForeignScan` can read it
    // as a RowSet and compose it with the local graph/vector/SQL ops in ONE plan. The
    // actual cross-engine/HTTP transport lives in eg-plan behind the `federation` gate;
    // this is the registration surface. Gated behind the facade `federation` feature;
    // in a slim/Pi build the variant falls to the not-built catch-all.
    /// Register (or replace) a foreign RowSet source under `name`. `source` is the
    /// [`crate::wire::ForeignSourceSpec`] (a remote engine or an HTTP/JSON API). A
    /// later `ForeignScan` can name this registered source by id (the registry-backed
    /// form) instead of inlining the whole spec. Returns the name on success.
    #[cfg(feature = "federation")]
    RegisterForeignSource {
        name: String,
        source: crate::wire::ForeignSourceSpec,
    },

    // ── WASM-sandboxed UDF / extension model (CONCEPT:EG-KG.query.rowset-execution) ─────────────
    // An agent pushes a custom compute function as a WebAssembly module the engine
    // runs SANDBOXED (wasmtime, fuel + memory limits, NO host capabilities). Gated
    // behind the facade `wasm-udf` feature (wasmtime is heavy); in a slim/Pi build the
    // variants fall to the not-built catch-all.
    /// Register (compile + cache) a WASM UDF under `id`. `wasm` is the module bytes
    /// (the `.wasm` binary). The module must export `memory`/`alloc`/`udf` and import
    /// NOTHING (the empty linker rejects any host import). Replaces a prior UDF of the
    /// same id. Returns the id on success.
    #[cfg(feature = "wasm-udf")]
    RegisterUdf {
        id: String,
        #[serde(with = "serde_bytes")]
        wasm: Vec<u8>,
    },
    /// Run a registered WASM UDF `id` over an opaque `input` payload, returning the
    /// UDF's output bytes (`ResultPayload::Raw`). Sandboxed + fuel-limited: an
    /// infinite-loop UDF is KILLED (a trap error), never a hang. The bytes are opaque
    /// to the engine — the caller serializes/deserializes its own row payload.
    #[cfg(feature = "wasm-udf")]
    RunUdf {
        id: String,
        #[serde(with = "serde_bytes")]
        input: Vec<u8>,
    },

    // ── Distributed graph compute (CONCEPT:EG-KG.storage.feature) ───────────────────────
    // A Pregel/GAS vertex-centric superstep engine that runs an algorithm ACROSS a
    // SET of graphs spanning multiple Raft groups/shards. Gated behind `compute-dist`
    // (which needs `raft`); in a non-cluster build the variants fall to the not-built
    // catch-all. The single-shard fast path stays the always-on `PageRank` etc.
    /// Run a distributed graph algorithm across `graphs` (each a shard/partition),
    /// with `algo` selecting PageRank / ConnectedComponents / Bfs. Result is
    /// `ResultPayload::Raw` — `[id, score]` rows for PageRank, `[id, label]` for CC/BFS.
    #[cfg(feature = "compute-dist")]
    DistributedCompute {
        graphs: Vec<String>,
        algo: DistAlgo,
    },
    /// Create (or replace) a named, incrementally-maintained MATERIALIZED VIEW of a
    /// distributed-compute result over `graphs`. The view is computed once, persisted,
    /// and refreshed incrementally on a delta (CONCEPT:EG-KG.storage.feature). Returns the row count.
    #[cfg(feature = "compute-dist")]
    CreateMatView {
        name: String,
        graphs: Vec<String>,
        algo: DistAlgo,
    },
    /// Read a materialized view's current rows by name (`ResultPayload::Raw`).
    #[cfg(feature = "compute-dist")]
    GetMatView {
        name: String,
    },
    /// Incrementally refresh a materialized view after the underlying graphs changed —
    /// recomputes only the affected vertices on the delta. Returns the row count.
    #[cfg(feature = "compute-dist")]
    RefreshMatView {
        name: String,
    },

    // ── Plan-backed materialized views (CONCEPT:EG-KG.storage.plan-backed-matview) ───────
    // GENERALIZES the algo-only matview above: a matview is a NAMED, DURABLE `wire::Plan`
    // (the same cross-modal AST `UnifiedQuery` carries) over ONE `graph`. Defining it
    // executes the plan once via the runtime and caches the RESULT in the version-keyed,
    // RLS-aware result cache; a committed write bumps the graph version (and the CDC hub
    // marks the view stale), so the next `Get` recomputes — never serves a stale result.
    // Gated behind the facade `matview` feature (which needs `query` for the `Plan` AST
    // and routes through the `compute-dist` dispatch line); in a build without it these
    // variants fall to the dispatch "not available in this build" catch-all.
    /// Define (or replace) a plan-backed materialized view `name` over `graph`, whose
    /// definition is the cross-modal `plan`. Executes the plan once, caches the result,
    /// and persists the definition durably. Returns the row count of the first
    /// materialization.
    #[cfg(feature = "matview")]
    PlanMatViewDefine {
        name: String,
        graph: String,
        plan: crate::wire::Plan,
    },
    /// Read a plan-backed materialized view's current rows by name
    /// (`ResultPayload::Raw`, `[id, score|nil]`). Serves the cached result when fresh;
    /// recomputes (and re-caches) when a write to the underlying graph — or an explicit
    /// CDC change signal — retired it.
    #[cfg(feature = "matview")]
    PlanMatViewGet {
        name: String,
    },
    /// Force a re-materialization of a plan-backed matview NOW (bypassing the freshness
    /// check), re-executing its stored plan and re-caching. Returns the fresh row count.
    #[cfg(feature = "matview")]
    PlanMatViewRefresh {
        name: String,
    },
    /// Drop a plan-backed matview: remove its definition from RAM + the durable tier and
    /// invalidate its cached result. Returns `Bool(true)` if it existed.
    #[cfg(feature = "matview")]
    PlanMatViewDrop {
        name: String,
    },

    // ── Transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid — multi-op OCC ACID) ───────────────
    // Server-side STAGED, OPTIMISTIC, snapshot-isolation transactions. `BeginTxn`
    // returns a server-issued `txn_id` (String). The `Txn*` ops STAGE durable
    // mutations into a server-held write-set (nothing touches the graph or
    // persistence until commit) and ack with `Bool(true)`. `Commit` takes the
    // topology write lock ONCE — the serialization point — validates the OCC
    // read-set (no targeted node changed since begin), applies the staged write-set
    // atomically through one `GraphTxn`, bumps the version counter, and persists;
    // it returns `Bool(false)` on conflict (true rollback — nothing applied).
    // `Rollback` discards the staged state and returns `Bool(true)`. The write
    // coalescer is NOT involved: staged ops are applied directly via `GraphTxn` at
    // commit, so there is no interaction/deadlock with the per-graph write worker
    // (which only handles NON-transactional single-op writes). A long-open txn
    // never holds `topo.write()`. (A single redb WriteTransaction per commit — a
    // true durability barrier — is a future enhancement; M6 persists per staged op
    // at commit and relies on the single GraphTxn for in-memory atomicity.)
    BeginTxn {
        /// Optional explicit target graph. An explicit `None` selects the request
        /// envelope's `graph`.
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
        /// Reserved isolation hint; only snapshot isolation is implemented.
        #[serde(deserialize_with = "deserialize_required_option")]
        isolation: Option<String>,
    },
    TxnAddNode {
        txn_id: String,
        node_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
        /// Optional target graph for THIS staged op (CONCEPT:EG-KG.txn.routes-cross-shard-txn — multi-graph
        /// txn). An explicit `None` selects the txn's default graph.
        /// A staged op naming a graph that resolves to a DIFFERENT Raft group makes
        /// the txn CROSS-SHARD, routed through 2PC at commit.
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    TxnRemoveNode {
        txn_id: String,
        node_id: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    TxnAddEdge {
        txn_id: String,
        source_id: String,
        target_id: String,
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    TxnRemoveEdge {
        txn_id: String,
        source_id: String,
        target_id: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    TxnCas {
        txn_id: String,
        node_id: String,
        #[serde(with = "serde_bytes")]
        conditions_msgpack: Vec<u8>,
        #[serde(with = "serde_bytes")]
        updates_msgpack: Vec<u8>,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Stage a VECTOR upsert into a txn (CONCEPT:EG-KG.txn.reader-never-sees-node — cross-modal ACID). The
    /// embedding lands atomically WITH the txn's graph/property/blob-ref writes in ONE
    /// redb `WriteTransaction` at commit — never a node without its vector.
    TxnAddEmbedding {
        txn_id: String,
        node_id: String,
        embedding: Vec<f32>,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Stage a BLOB REFERENCE into a txn (CONCEPT:EG-KG.txn.reader-never-sees-node — cross-modal ACID). Records
    /// a durable graph-side link (`__blob__` node property) to an already-stored,
    /// content-addressed blob; lands atomically with the node/vector/property at commit.
    TxnBlobRef {
        txn_id: String,
        node_id: String,
        digest: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Stage a TIME-SERIES measurement batch into a txn (CONCEPT:EG-KG.backend.cross-modal-atomic-commit — extended
    /// cross-modal staging). The points land atomically WITH the txn's graph/property/
    /// vector/blob writes in ONE redb `WriteTransaction` at commit — never a node
    /// without its measurements. `points` is the SAME MessagePack `Vec<(i64 ts, Vec<f64>
    /// values)>` blob `TsAppend` carries (kept opaque here so the protocol enum stays
    /// free of any eg-tsdb type). Ungated in the enum like the `Ts*` family; the
    /// staging/commit handler is `tsdb`-gated at the facade, so a slim build reaches the
    /// dispatch "not available in this build" catch-all.
    TxnAddMeasurement {
        txn_id: String,
        /// Target series id the points belong to.
        series: String,
        /// MessagePack `Vec<(i64, Vec<f64>)>` — the batch of points (one round-trip).
        #[serde(with = "serde_bytes")]
        points: Vec<u8>,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Stage OWL AXIOMS (Turtle) into a txn (CONCEPT:EG-KG.txn.extended-cross-modal — extended cross-modal
    /// staging). At commit the `turtle` axioms lower to graph node/edge writes in the
    /// SAME atomic `WriteTransaction` so the OWL reasoner sees them consistently with the
    /// txn's other staged modalities. Gated `owl` (mirrors `OwlReason`); a build without
    /// it drops the variant → the dispatch "not available in this build" catch-all.
    #[cfg(feature = "owl")]
    TxnAxiom {
        txn_id: String,
        /// OWL axioms as Turtle to stage into the txn.
        turtle: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Stage a SPARQL CONSTRUCT into a txn (CONCEPT:EG-KG.query.extended-cross-modal — extended cross-modal
    /// staging). At commit the `sparql` CONSTRUCT's produced triples lower to graph
    /// node/edge writes in the SAME atomic `WriteTransaction`. Gated `sparql` (mirrors
    /// `Sparql`); a build without it drops the variant → the dispatch "not available in
    /// this build" catch-all.
    #[cfg(feature = "sparql")]
    TxnConstruct {
        txn_id: String,
        /// SPARQL CONSTRUCT query whose triples are staged into the txn.
        sparql: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Stage a PLANNER WRITEBACK into a txn (CONCEPT:EG-KG.query.plan-dag, D7 — the
    /// planner-writeback ACID seam). `plan` (the SAME `wire::Plan` AST `UnifiedQuery`
    /// carries) runs READ-ONLY against the txn's committed snapshot; each id in its
    /// result `RowSet` becomes an `AddEdge { source_id: anchor_id, target_id: id,
    /// relationship }` — e.g. materializing a `Reason`/`Traverse`-inferred edge set —
    /// staged (via `GraphTxnState::stage_plan_writeback`, copying the `TxnAxiom`/
    /// `TxnConstruct` shape verbatim) into the SAME atomic `WriteTransaction` as the
    /// txn's other modalities. Gated `query` (mirrors `UnifiedQuery`); a build without
    /// it drops the variant → the dispatch "not available in this build" catch-all.
    #[cfg(feature = "query")]
    TxnPlanWriteback {
        txn_id: String,
        /// The plan whose result `RowSet` is materialized as edges.
        plan: crate::wire::Plan,
        /// The edge SOURCE every materialized edge is anchored to.
        anchor_id: String,
        /// The `relationship` property every materialized edge carries.
        relationship: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Stage a MATERIALIZE-BELIEF op into a txn (CONCEPT:EG-KG.epistemic.epistemic-substrate,
    /// D5 — the explicit, AUDITED "materialize belief" op the `eg_epistemic` crate docs
    /// call for: `BeliefState.confidence` is derived and "NEVER written back onto
    /// `NodeData.confidence` … unless a caller runs an explicit, logged materialize
    /// belief op"). Computes the propagated belief for `node_id` (via
    /// `eg_epistemic::propagate_confidence` over the graph's SUPPORTS/CONTRADICTS/
    /// ATTACKS evidence topology, read from the txn's COMMITTED snapshot — the SAME
    /// "evaluate now" shape `TxnPlanWriteback`/`TxnConstruct` use) and stages ONE
    /// unconditional `CompareAndSetNodeFields` that writes it onto that node's
    /// `NodeData.confidence` — landing atomically with the txn's other staged
    /// modalities at commit (`GraphTxnState::stage_plan_writeback`, reused verbatim —
    /// same OCC read-set capture + cross-modal commit shape as `TxnAxiom`/
    /// `TxnConstruct`/`TxnPlanWriteback`). The write rides the ALREADY-audited
    /// `CompareAndSetNodeFields` path (the tamper-evident hash chain, CONCEPT:
    /// EG-KG.sharding.row-level-security, plus the unconditional in-memory ledger) —
    /// never silent, no new audit mechanism. OPT-IN: this is the ONLY path that ever
    /// writes a derived belief back onto stored confidence; nothing else in the engine
    /// does so implicitly. Gated `epistemic`; a build without it drops the variant →
    /// the dispatch "not available in this build" catch-all.
    #[cfg(feature = "epistemic")]
    TxnMaterializeBelief {
        txn_id: String,
        /// The node whose propagated belief is computed and written back.
        node_id: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
    },
    /// Run a UNIFIED cross-modal query INSIDE a txn with read-your-own-writes
    /// (CONCEPT:EG-KG.query.txn-cross-modal-ryow — in-txn cross-modal RYOW). Executes the SAME `wire::Plan` AST as
    /// `UnifiedQuery`, but over a snapshot OVERLAID with the txn's staged (uncommitted)
    /// write-set, so a staged node/edge/embedding is visible to THIS txn before commit
    /// and invisible off-txn until commit. Read-only w.r.t. the committed store. Gated
    /// `query` (the plan AST + DataFusion filter leg); a slim build drops the variant →
    /// the dispatch "not available in this build" catch-all.
    #[cfg(feature = "query")]
    TxnUnifiedQuery {
        txn_id: String,
        plan: crate::wire::Plan,
    },
    /// In-txn unified query, TEXT surface — UQL (CONCEPT:EG-KG.query.txn-cross-modal-ryow). The human/agent-
    /// writable counterpart of `TxnUnifiedQuery`: a UQL `text` string PARSED into the
    /// SAME `wire::Plan` AST and run through the IDENTICAL overlaid in-txn executor. Same
    /// `query`-gating + read-your-own-writes semantics as `TxnUnifiedQuery`.
    #[cfg(feature = "query")]
    TxnUnifiedQueryText {
        txn_id: String,
        text: String,
    },
    Commit {
        txn_id: String,
    },
    Rollback {
        txn_id: String,
    },

    // ── Time-series (CONCEPT:AU-KG.retrieval.god-nodes-communities/211 — native TSDB) ──────────────────
    // Native time-series store + query primitives (the eg-tsdb crate), gated
    // behind the facade `tsdb` feature; in a slim build each variant falls to the
    // graph_ops not-built catch-all. Series are keyed by `series_id` in their OWN
    // redb file (`series.redb`) beside the graph shards. Points cross the wire as a
    // MessagePack blob (`Vec<(i64 ts, Vec<f64> values)>`) so the protocol enum (at
    // the bottom of the DAG) stays free of any eg-tsdb type. Query results return
    // via `ResultPayload::raw` (the client double-unpacks), matching `Sql`/`Cypher`.
    //
    // `TsAppend` is the ONE durable write here (handled out-of-band of the graph
    // write-coalescer — it targets the series store, not the graph core); the rest
    // are read-only.
    TsAppend {
        series_id: String,
        /// Field count per point (1 for a scalar series, N for OHLCV…). Used only
        /// when the series is NEW; an existing series' stored schema wins.
        n_fields: usize,
        /// Bucket/time-partition width in nanoseconds (series-creation parameter).
        bucket_ns: u64,
        /// Optional field names (series-creation metadata).
        #[serde(default)]
        field_names: Vec<String>,
        /// MessagePack `Vec<(i64, Vec<f64>)>` — the batch of points (one round-trip).
        #[serde(with = "serde_bytes")]
        points_msgpack: Vec<u8>,
    },
    TsRange {
        series_id: String,
        /// Inclusive lower / exclusive upper ts bound (ns).
        from: i64,
        to: i64,
    },
    TsAsofJoin {
        /// The "right" series each left event is joined to by nearest-prior ts.
        series_id: String,
        /// MessagePack `Vec<i64>` — the left event timestamps (ns).
        #[serde(with = "serde_bytes")]
        left_ts_msgpack: Vec<u8>,
        /// Optional tolerance (ns); a match older than this is dropped (`None` =
        /// unbounded). `-1` encodes `None` over the wire.
        #[serde(default)]
        tolerance: i64,
    },
    TsWindow {
        series_id: String,
        from: i64,
        to: i64,
        /// Window width (ns) for the bucketed aggregate.
        width: i64,
        /// Aggregate function: one of first/last/min/max/mean/sum/count.
        agg: String,
    },
    TsGapFill {
        series_id: String,
        from: i64,
        to: i64,
        /// Grid step (ns) for the LOCF densification.
        step: i64,
    },

    // ── Blob (CONCEPT:EG-KG.storage.blob-namespace — streamed content-addressed media substrate) ──
    // Streamed transfer of a large media blob as MANY ordinary one-Response-per-
    // Request frames sharing a SERVER-SIDE CURSOR — NOT a side-channel socket, NOT
    // a protocol-v2. The whole file is never resident on either side; only one
    // chunk is in flight. `BlobBegin` opens an upload cursor, N `BlobChunkPut`
    // frames push fixed-size chunks (each hashed + stored content-addressed on
    // arrival), `BlobCommit` assembles the manifest → blob digest. Download
    // mirrors it: `BlobFetchBegin(digest)` → repeated `BlobChunkGet(cursor, idx)`.
    // Refcount-GC bookkeeping rides `BlobRef`/`BlobUnref` (a `:Media` node
    // referencing a blob increments; removal decrements; a zero-ref blob's chunks
    // are reclaimed by `BlobGc`). `data` is a MessagePack `bin` (serde_bytes) so a
    // 0x0A inside a chunk is framed by the outer length prefix. Gated `blob`; a
    // build without it drops these variants → dispatch's "not available" catch-all.
    /// Open an upload cursor; server allocates a cursor id and an empty accumulator.
    /// `chunk_size` is the fixed split size the client will use (records it on the
    /// manifest for range-read math later); 0 ⇒ the engine default.
    #[cfg(feature = "blob")]
    BlobBegin {
        #[serde(default)]
        chunk_size: u32,
    },
    /// Push one chunk into an open upload cursor. Hashed + stored on arrival; only
    /// the digest is appended to the cursor (bounded memory).
    #[cfg(feature = "blob")]
    BlobChunkPut {
        cursor: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    /// Finalize an upload cursor → assemble + store the manifest content-addressed,
    /// return the blob digest. Drops the cursor.
    #[cfg(feature = "blob")]
    BlobCommit {
        cursor: u64,
    },
    /// Open a fetch cursor for a stored blob digest; returns `(cursor, n_chunks)`.
    #[cfg(feature = "blob")]
    BlobFetchBegin {
        digest: String,
    },
    /// Pull chunk `idx` of an open fetch cursor (one chunk per frame).
    #[cfg(feature = "blob")]
    BlobChunkGet {
        cursor: u64,
        idx: u32,
    },
    /// Close a fetch cursor (client done streaming down). Idempotent.
    #[cfg(feature = "blob")]
    BlobFetchEnd {
        cursor: u64,
    },
    /// Increment a blob's refcount — a `:Media` node now references it. Returns the
    /// new count. (The graph link itself, `:Media-[:HAS_BLOB]->:Blob`, is created by
    /// the caller via the normal node/edge methods; this maintains the GC refcount.)
    #[cfg(feature = "blob")]
    BlobRef {
        digest: String,
    },
    /// Decrement a blob's refcount — a `:Media` reference was removed. Returns the
    /// new count; a blob at 0 is eligible for the next `BlobGc`.
    #[cfg(feature = "blob")]
    BlobUnref {
        digest: String,
    },
    /// Run the refcount mark-and-sweep GC: reclaim every zero-ref blob's manifest +
    /// the chunks no surviving blob still lists. Returns `(blobs, chunks)` reclaimed.
    #[cfg(feature = "blob")]
    BlobGc,

    // ── Key→Value (CONCEPT:EG-KG.storage.namespaced-kv-surface — generic namespaced KV surface) ──────
    // A drop-in KV store keyed by `(namespace, key)`, layered over the SAME durable
    // redb substrate. NOT graph-scoped (a KV pair lives off the node/edge graph), so
    // these self-route in dispatch like the Blob*/Ts* ops. Writes are durable
    // commit-before-ack. The variants only exist with the `kv` feature; a build
    // without it drops them from the enum (→ the dispatch "not available" catch-all).
    /// Fetch the value bytes at `(namespace, key)`; result is the bytes or null.
    #[cfg(feature = "kv")]
    KvGet {
        namespace: String,
        key: String,
    },
    /// Store `value` at `(namespace, key)` (overwrite). Durable commit-before-ack.
    /// `value` is a MessagePack `bin` (serde_bytes) — opaque bytes, stored verbatim.
    #[cfg(feature = "kv")]
    KvPut {
        namespace: String,
        key: String,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    /// Delete `(namespace, key)`; returns whether the key existed.
    #[cfg(feature = "kv")]
    KvDelete {
        namespace: String,
        key: String,
    },
    /// Ordered `(key, value)` pairs in `namespace` whose key starts with `prefix`
    /// (empty prefix ⇒ the whole namespace). `limit == 0` ⇒ no cap.
    #[cfg(feature = "kv")]
    KvScan {
        namespace: String,
        prefix: String,
        limit: usize,
    },
    /// Atomic compare-and-swap: set `(namespace, key)` to `new` (`None` ⇒ delete) iff
    /// the current value equals `expected` (both absent ⇒ the key must not exist).
    /// Returns whether the swap happened. `expected`/`new` are MessagePack `bin`.
    #[cfg(feature = "kv")]
    KvCas {
        namespace: String,
        key: String,
        #[serde(default, with = "serde_bytes")]
        expected: Option<Vec<u8>>,
        #[serde(default, with = "serde_bytes")]
        new: Option<Vec<u8>>,
    },

    // ── SQLite `.db` file import/export (CONCEPT:EG-KG.query.eg-feature/EG-332) ─────────
    // Read/write a real on-disk `sqlite3` `.db` FILE (the documented EG-075 follow-up),
    // distinct from the `sqlite-wire` NDJSON dialect surface. NOT graph-scoped: both ops
    // accept a logical `.db` filename under an operator-provisioned private transfer root
    // and move rows through the process-global user-table
    // store (the SAME `TableStore` the `Method::Sql` DDL/DML + pgwire paths use), so they
    // self-route in dispatch like the Blob*/Kv* ops. Each is a BATCH op — ONE engine
    // round-trip that reads/writes the whole file, never per-row. The variants only exist
    // with the `sqlite-file` feature (which pulls the bundled C sqlite kept OUT of pi); a
    // build without it drops them from the enum, so a slim/pi build can't reach the arm.
    /// Import every user table (+ its rows) from logical `.db` filename `path`
    /// into the engine's user-table store (CONCEPT:EG-KG.query.eg-feature). A table that already exists
    /// is REPLACED (drop-then-recreate) so the import mirrors the file. Returns a `Json`
    /// report `{"source":"sqlite", "imported_tables":[{"table","rows"},…]}`.
    #[cfg(feature = "sqlite-file")]
    ImportSqliteFile {
        path: String,
    },
    /// Export user tables OUT to a fresh, valid `sqlite3` `.db` logical filename `path` that the
    /// `sqlite3` CLI can open (CONCEPT:EG-KG.query.full-protocol). `tables` empty ⇒ every user table; else
    /// exactly the named tables (each must exist). Publication is private and atomic.
    /// Returns aggregate table counts without a host path.
    #[cfg(feature = "sqlite-file")]
    ExportSqliteFile {
        path: String,
        #[serde(default)]
        tables: Vec<String>,
    },

    // ── RDF/SPARQL (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / KG-2.218 — native semantic-web surface) ──
    // The RDF dataset maps onto the SAME property-graph the rest of the engine uses
    // (resource object ⇒ typed edge `{relationship: predicate}`; literal object ⇒ a typed
    // JSON property cell preserving xsd datatype + @lang; rdf:type ⇒ the engine
    // `type` label; named graph ⇒ the target registry graph). So these are
    // GRAPH-SCOPED ops (they target `req.graph`) and route through the normal
    // dispatch_graph_op chain like Sql/Cypher — NOT a separate top-level store.
    //
    // `AddTriples` is a DURABLE MUTATION: it writes nodes + edges into the target
    // graph. It is replayed by re-parsing its source text (deterministic — the same
    // Turtle yields the same triples ⇒ the same node/edge writes), mirroring how
    // `BatchUpdate` replays. `GetRdf` (serialize OUT) and `Sparql` are read-only.
    /// Parse `turtle` OR `ntriples` (exactly one non-empty) and store the triples
    /// into the request's graph (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql). Returns a `Raw` `LoadReport`
    /// (`{triples, multivalue}`). Gated `rdf`; a build without it
    /// drops the variant → the dispatch not-built catch-all.
    #[cfg(feature = "rdf")]
    AddTriples {
        /// Turtle document (empty ⇒ use `ntriples`).
        #[serde(default)]
        turtle: String,
        /// N-Triples document (empty ⇒ use `turtle`).
        #[serde(default)]
        ntriples: String,
    },
    /// Serialize the request's graph back OUT to RDF as N-Triples (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql).
    /// Returns a `Raw` `String` (the canonical, order-independent form) — the
    /// datatype/lang-faithful inverse of `AddTriples`. Read-only.
    #[cfg(feature = "rdf")]
    GetRdf,
    /// Physically RETRACT triples from the request's graph (CONCEPT:EG-KG.query.named-graph-support) — the
    /// inverse of `AddTriples`. Parses `turtle` OR `ntriples` (exactly one non-empty)
    /// and surgically removes each triple (a literal triple drops the property cell; a
    /// resource triple removes the one matching typed edge). DURABLE (WAL-replayed by
    /// re-parsing + re-removing). This is the reusable retract op the ontology UNLOAD
    /// path + SPARQL `DELETE DATA` build on. Returns a `Raw` count. Gated `rdf`.
    #[cfg(feature = "rdf")]
    RemoveTriples {
        /// Turtle document (empty ⇒ use `ntriples`).
        #[serde(default)]
        turtle: String,
        /// N-Triples document (empty ⇒ use `turtle`).
        #[serde(default)]
        ntriples: String,
    },
    /// DROP the request's named graph (CONCEPT:EG-KG.query.named-graph-support): physically clear ALL of its RDF
    /// content — the property-graph nodes/edges AND the lossless multi-valued-literal
    /// quad-store rows for this graph. DURABLE (WAL-replayed as a clear). The SPARQL
    /// `DROP/CLEAR GRAPH` op + ontology lifecycle teardown route here. Returns a `Raw`
    /// `"ok"`. Gated `rdf`. (Distinct from `DeleteGraph`, which evicts the registry
    /// entry; this empties the graph's RDF while keeping the graph addressable.)
    #[cfg(feature = "rdf")]
    DropNamedGraph,
    /// Evaluate a SPARQL 1.1 SELECT over the request's graph (CONCEPT:EG-KG.ontology.concept-11).
    /// Returns a `Raw` [`SparqlResult`] (`{vars, rows}`; each row a cell list aligned
    /// to `vars`, an unbound cell is `nil`). Read-only. Gated `sparql`.
    ///
    /// `base_iri` + `type_convention` carry an OPTIONAL LPG→RDF projection vocabulary
    /// (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary). Both default to empty ⇒ the IDENTITY projection (node-type
    /// and property keys emitted verbatim, no `rdf:type` synthesis), which preserves
    /// the prior behavior for every existing caller. A caller (e.g. agent-utilities)
    /// that sets `base_iri = "http://agent-utilities.dev/ontology#"` +
    /// `type_convention = "camel"` makes the engine project the LIVE property graph
    /// into that vocabulary — `<node> rdf:type <base + CamelCase(type)>` and
    /// `<node> <base + prop> <v>` — so a by-class query (`?s a au:Agent`) resolves
    /// natively. The engine itself hardcodes NO ontology URL; the vocabulary is the
    /// caller's.
    #[cfg(feature = "sparql")]
    Sparql {
        query: String,
        /// Projection base namespace IRI. Empty ⇒ identity projection.
        #[serde(default)]
        base_iri: String,
        /// `rdf:type` object naming: `"camel"` ⇒ CamelCase the type local name under
        /// `base_iri`; empty / `"raw"` ⇒ verbatim. Only meaningful with `base_iri`.
        #[serde(default)]
        type_convention: String,
    },
    /// OBDA / R2RML VIRTUAL GRAPH query (CONCEPT:EG-KG.query.r2rml-virtual-graph /
    /// CONCEPT:EG-KG.query.obda-query-rewrite) — Ontology-Based Data Access: run a SPARQL query
    /// against a set of foreign tabular sources exposed as RDF via an R2RML-style
    /// mapping, WITHOUT ever materializing the whole dataset. `tables` names the
    /// engine's OWN SQL user tables (the `eg_query::TableStore` behind `query`, the same
    /// store `Method::Sql` DDL/DML and `ImportSqliteFile` write) to register as foreign
    /// sources under their own table name (a [`TriplesMap::logical_source`] target);
    /// `mapping` is either a standard R2RML Turtle document (`@prefix rr: …`) or the
    /// compact EG-101 textual form (`SOURCE`/`SUBJECT`/`CLASS`/`COLUMN`/`REF`/`CONST`
    /// directives) — auto-detected. The query rewrites to a projection-pushed scan of
    /// only the query-relevant table columns (see `eg_rdf::obda`), materializes ONLY
    /// those triples into a transient view, and evaluates the SAME SPARQL engine over
    /// it — so this is a REAL query-rewrite OBDA path, not a full ETL/materialize step.
    /// Returns a `Raw` [`SparqlResult`]. Read-only (never writes the user table OR the
    /// request's graph). Gated `obda` (implies `sparql` + `query`).
    #[cfg(feature = "obda")]
    SparqlVirtual {
        /// The SPARQL query to run against the virtual graph.
        query: String,
        /// An R2RML Turtle document OR the compact EG-101 textual mapping form.
        mapping: String,
        /// The user-table names the mapping's `TriplesMap`s reference as
        /// `logical_source`s — each is registered as a foreign source under its own
        /// name before the mapping is parsed and the query is run.
        tables: Vec<String>,
        /// LIVE external relational sources (Postgres/MySQL) registered as foreign OBDA
        /// sources IN ADDITION to `tables` (CONCEPT:EG-KG.query.obda-predicate-pushdown,
        /// W4.11). Each binds a `logical_source` name to an external DB table; the query's
        /// column projection AND its row-level `FILTER`s are pushed into a real
        /// `SELECT … WHERE …`. Needs a `federation-sql` server build for the live path.
        /// Empty ⇒ engine-own-tables-only (the prior behavior).
        #[serde(default)]
        external_sources: Vec<ObdaExternalSource>,
    },
    /// Run the native OWL 2 (EL⁺ + RL) reasoner over the request's graph and
    /// materialize entailments (CONCEPT:EG-KG.ontology.incremental-materialization). Classifies the OWL axioms already
    /// in the graph (the TBox loaded via `AddTriples`) plus any extra `ontology`
    /// Turtle, then returns a `Raw` [`OwlReasonResult`]: the derived named-class
    /// subsumptions, the inferred instance→class memberships (incl. ones reached only
    /// through existential restrictions / role chains), and a consistency verdict. The
    /// `Op::Reason` plan op reuses the SAME classifier as a RowSet source. Read-only
    /// (it does not mutate the graph). Gated `owl`.
    #[cfg(feature = "owl")]
    OwlReason {
        /// Extra OWL axioms as Turtle (empty ⇒ reason over the graph's own axioms).
        #[serde(default)]
        ontology: String,
        /// When set, restrict the returned instance memberships to this class (its
        /// inferred members) — the materialize-one-class shape. Empty ⇒ all classes.
        #[serde(default)]
        target_class: String,
        /// Confidence threshold τ in `[0,1]` (CONCEPT:EG-KG.ontology.concept-13). The result carries a
        /// per-entailment confidence (axioms/facts may be uncertain; the closure
        /// propagates it — `eg:confidence` annotations × the per-node confidence ×
        /// Ebbinghaus decay). Only entailments with `confidence ≥ min_confidence` are
        /// returned. `0.0` keeps everything (and a HARD ontology yields all `1.0`).
        min_confidence: f64,
    },
    /// DISTRIBUTED confidence-weighted OWL reasoning over the UNION of `graphs`
    /// (CONCEPT:EG-KG.ontology.concept-13): gathers each graph/shard's TBox axioms + decayed-confidence
    /// type facts, runs ONE weighted EL⁺/RL closure over the union (the cross-shard
    /// union-read seam — KG-2.171), and returns the SAME [`OwlReasonResult`] a
    /// single-graph `OwlReason` would over the same axioms in one graph. The single-
    /// shard fast path stays `OwlReason`. Read-only. Gated `owl`.
    #[cfg(feature = "owl")]
    OwlReasonDistributed {
        /// The graphs (shards) whose axioms + facts to union and reason over.
        graphs: Vec<String>,
        /// Extra OWL axioms as Turtle (a shared TBox over the sharded ABox; empty ⇒
        /// only the axioms already present across the graphs).
        #[serde(default)]
        ontology: String,
        /// Restrict instance memberships to this class (empty ⇒ all classes).
        #[serde(default)]
        target_class: String,
        /// Confidence threshold τ in `[0,1]` (see `OwlReason::min_confidence`).
        #[serde(default)]
        min_confidence: f64,
    },

    /// OWL proof-tree EXPLANATION (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation) — Stardog's flagship
    /// "explanation" feature, native here. Classifies the request's graph (its own TBox
    /// axioms, loaded via `AddTriples`, plus any extra `ontology` Turtle) with confidence
    /// propagation, then reconstructs the FULL recursive proof tree for the ONE named-class
    /// subsumption `sub ⊑ sup` — WHICH axiom(s) + WHICH premise subsumption(s) derived it,
    /// recursively down to the asserted/reflexive leaves — via
    /// [`crate`]-independent reconstruction of `eg_rdf::owl::Classification::explain`'s
    /// justification DAG (CONCEPT:EG-KG.ontology.justification-tracking). Returns a `Raw`
    /// [`OwlExplainResult`]. Read-only (does not mutate the graph). Gated `owl`.
    #[cfg(feature = "owl")]
    OwlExplain {
        /// Extra OWL axioms as Turtle (empty ⇒ reason over the graph's own axioms).
        #[serde(default)]
        ontology: String,
        /// The SUBCLASS side of the subsumption to explain (a class IRI, `<...>` or bare —
        /// canonicalized the same way `target_class` is elsewhere).
        sub: String,
        /// The SUPERCLASS side of the subsumption to explain.
        sup: String,
    },

    // ── Custom-rule reasoning (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog / EG-023 — runtime SWRL/Datalog rules) ──
    // Run a parameterised rule-reasoning request over the request's graph view (its
    // folded TBox axioms + asserted facts) PLUS any inline `ontology_ttl` and the
    // user `rules`, returning the inferred facts. Read-only (it reasons over an
    // off-lock snapshot and never mutates the graph), so it routes through the normal
    // `dispatch_graph_op` chain like `Sparql`/`OwlReason`. The fields mirror eg-rdf's
    // `RuleReasonRequest` 1:1 (kept inline so the protocol crate — at the bottom of the
    // DAG — carries no eg-rdf type); the handler rebuilds the request and calls
    // `eg_rdf::run_rule_reasoning_on_view`. Result is a `Raw` `RuleReasonResponse`.
    // Gated `rdf`; a build without it drops the variant → the dispatch not-built catch-all.
    #[cfg(feature = "rdf")]
    RunRules {
        /// Optional Turtle carrying extra TBox axioms AND/OR ABox facts (empty ⇒ reason
        /// over the graph's own folded axioms/facts only).
        #[serde(default)]
        ontology_ttl: String,
        /// User rule strings (SWRL-ish / Datalog syntax).
        #[serde(default)]
        rules: Vec<String>,
        /// When set, restrict the returned facts to this predicate (IRI or bare name).
        #[serde(default)]
        query_predicate: Option<String>,
        /// Drop facts whose confidence is below this threshold.
        #[serde(default)]
        min_confidence: f64,
        /// When true, return only the DERIVED facts (omit the asserted base).
        #[serde(default)]
        derived_only: bool,
    },

    // ── SHACL Core validation (CONCEPT:EG-KG.ontology.concept-6) ───────────────────────────────
    // Validate an RDF DATA graph against an RDF SHAPES graph, producing an
    // `sh:ValidationReport` (`conforms` + a list of `sh:ValidationResult`). The
    // engine half is the pure-Rust `eg-shacl` crate. This variant is UNCONDITIONAL in
    // the enum (like `Backup`/`Restore`, CONCEPT:EG-KG.sharding.reshard-on-restore); the HANDLER is gated on the
    // `shacl` feature — a build without it drops the handler arm and the request falls
    // through to the dispatch "not available in this build" catch-all. The fields are
    // inline Strings so the protocol crate (bottom of the DAG) carries no eg-shacl type;
    // the handler parses both documents and returns a `Json` report.
    /// Validate `data_graph` against `shapes`, both RDF Turtle documents (CONCEPT:EG-KG.ontology.concept-6).
    /// An EMPTY `data_graph` validates against the LIVE RDF of the request's graph (the
    /// same triples `GetRdf` would export). Returns a `Json` `sh:ValidationReport`.
    /// Read-only. Handler gated `shacl` (implies `rdf`).
    ShaclValidate {
        /// The shapes graph as a Turtle document.
        shapes: String,
        /// The data graph as a Turtle document; empty ⇒ use the request's live graph.
        #[serde(default)]
        data_graph: String,
    },

    /// X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard) — (re)register a graph's
    /// SHACL shapes as WRITE-TIME closed-world integrity constraints (ICV, reusing
    /// `eg-shacl`'s existing `IcvPolicyRegistry`/`WriteGuard` verbatim — no new
    /// validator). The required `mode` value is `"enforce"`: a violating change ABORTS the
    /// `AddTriples`/`RemoveTriples`/`ApplyMutation` commit with the introduced
    /// violations, each carrying its SPARQL witness). `graph` names the target graph;
    /// `None` sets the DEFAULT-graph policy. `shapes` is the SHACL shapes Turtle
    /// document — e.g. the SHACL a connector-manifest compiler emits alongside its
    /// RLS/ABAC policy output (agent-utilities side) and must be non-empty. Like `ShaclValidate`,
    /// this variant is UNCONDITIONAL in the enum; the HANDLER is gated `shacl` — a
    /// build without it drops the handler arm and the request falls through to the
    /// dispatch "not available in this build" catch-all.
    IcvConfigure {
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
        mode: String,
        shapes: String,
    },

    // ── ShEx (Shape Expressions) Core validation (CONCEPT:EG-KG.compute.concept-2) ────────────
    // The complement to `ShaclValidate` (EG-132): validate that focus nodes of an RDF
    // DATA graph CONFORM to shape expressions in a ShEx schema, driven by a shape map
    // (focus node → shape label). The engine half is the pure-Rust `eg-shex` crate. Like
    // `ShaclValidate`/`Backup` (EG-090), this variant is UNCONDITIONAL in the enum; the
    // HANDLER is gated on the `shex` feature — a build without it drops the handler arm
    // and the request falls through to the dispatch "not available in this build"
    // catch-all. The fields are inline strings so the protocol crate (bottom of the DAG)
    // carries no eg-shex type; the handler parses the ShExJ schema + the data graph and
    // returns a `Json` `ShexReport`.
    /// Validate `data_graph` against a **ShExJ** `schema` for a `shape_map` (a list of
    /// `[node_iri, shape_label]` pairs; `"START"` selects the schema's start shape)
    /// (CONCEPT:EG-KG.compute.concept-2). `data_graph` is an RDF Turtle document; an EMPTY `data_graph`
    /// validates against the LIVE RDF of the request's graph (the same triples `GetRdf`
    /// would export). Returns a `Json` `ShexReport`. Read-only. Handler gated `shex`
    /// (implies `rdf`).
    ShexValidate {
        /// The ShEx schema as a ShExJ (JSON abstract-syntax) document.
        schema: String,
        /// The data graph as a Turtle document; empty ⇒ use the request's live graph.
        #[serde(default)]
        data_graph: String,
        /// The shape map: `[node_iri, shape_label]` pairs. `shape_label` may be `"START"`.
        #[serde(default)]
        shape_map: Vec<[String; 2]>,
    },

    // ── Streaming / CDC / subscriptions / reactivity (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ──
    // A reactive surface over the engine's per-graph durable change record (the
    // ledger). Every durable mutation the dispatch shell records also emits an
    // ordered, cursor-addressable `CdcEvent` (node/edge add/remove/update with
    // before/after) into a per-graph in-memory feed. Built on the SAME one-Response-
    // per-Request transport — a consumer TAILS via a `from_seq` cursor (CdcRead /
    // Watch long-poll), never a side-channel socket or a protocol-v2. All variants
    // are gated `streaming` (folds into pi/node/cluster/full — no heavy dep); a build
    // without it drops them → the dispatch "not available in this build" catch-all.
    /// Read the ordered change feed for `graph` from cursor `from_seq` (inclusive),
    /// up to `limit` events (CONCEPT:EG-KG.query.streaming-cdc-subscriptions). Returns a `Raw` `Vec<CdcEvent>`. The
    /// consumer re-reads from `last.seq + 1` to skip what it has seen. `limit` 0 ⇒ a
    /// default cap.
    #[cfg(feature = "streaming")]
    CdcRead {
        graph: String,
        from_seq: u64,
        #[serde(default)]
        limit: u32,
    },
    /// Register a continuous query (CONCEPT:EG-KG.query.streaming-cdc-subscriptions): a named, incrementally-
    /// maintained aggregate/filter view over a graph's CDC feed. `spec_msgpack` is a
    /// MessagePack `ContinuousQuerySpec`. Returns a `String` (the name). Re-registering
    /// the same name replaces it (and re-seeds from the current graph state).
    #[cfg(feature = "streaming")]
    RegisterContinuousQuery {
        name: String,
        #[serde(with = "serde_bytes")]
        spec_msgpack: Vec<u8>,
    },
    /// Read the current incrementally-maintained result of a continuous query. Returns
    /// a `Raw` `ContinuousQueryResult`.
    #[cfg(feature = "streaming")]
    ReadContinuousQuery {
        name: String,
    },
    /// Drop a continuous query. Returns `Bool` (true if it existed).
    #[cfg(feature = "streaming")]
    DropContinuousQuery {
        name: String,
    },
    /// LISTEN/NOTIFY-style long-poll subscription (CONCEPT:EG-KG.query.wire-codec): return the
    /// matching CDC changes for `graph` since `from_seq`, blocking up to `timeout_ms`
    /// for the FIRST one if none are pending yet (then returns what arrived). `label`
    /// (empty ⇒ all) filters by node/edge label. Returns a `Raw` `WatchBatch`
    /// (`{events, next_seq}`); the client passes `next_seq` back to resume. Transport-
    /// compatible: one Request → one Response, cursor-driven.
    #[cfg(feature = "streaming")]
    Watch {
        graph: String,
        from_seq: u64,
        #[serde(default)]
        label: String,
        #[serde(default)]
        timeout_ms: u64,
    },
    /// Register a trigger/reaction (CONCEPT:EG-KG.query.wire-codec): when a CDC change in `graph`
    /// matches `label` (empty ⇒ any) + `op` ("add"|"remove"|"update"|"any"), record a
    /// firing carrying `action_msgpack` (an opaque reaction payload). Returns the name.
    #[cfg(feature = "streaming")]
    RegisterTrigger {
        name: String,
        graph: String,
        #[serde(default)]
        label: String,
        op: String,
        #[serde(default, with = "serde_bytes")]
        action_msgpack: Vec<u8>,
    },
    /// Drop a trigger. Returns `Bool`.
    #[cfg(feature = "streaming")]
    DropTrigger {
        name: String,
    },
    /// List the triggers registered on `graph`. Returns a `Raw` `Vec<TriggerInfo>`.
    #[cfg(feature = "streaming")]
    ListTriggers {
        graph: String,
    },
    /// Poll the fired-trigger log for `graph` from cursor `from_seq` (CONCEPT:EG-KG.query.wire-codec):
    /// the reactions that fired since the cursor. Returns a `Raw` `Vec<FiredAction>`;
    /// the consumer dispatches each action then resumes from `last.fire_seq + 1`.
    #[cfg(feature = "streaming")]
    FiredTriggers {
        graph: String,
        from_seq: u64,
        #[serde(default)]
        limit: u32,
    },

    // ── Live CEP standing queries (CONCEPT:EG-KG.query.protocol-types) ───────────────────────────
    // The PUSH half of the event-stream + complex-event-processing modality
    // (CONCEPT:EG-KG.query.pipelined-execution): register a CEP pattern ONCE as a live standing query, then
    // pull the matches it detects as CDC changes flow. The CDC hub (feature
    // `streaming`) is adapted into an `eg_stream::Event` bus that feeds the live
    // `eg_stream::live::CepEngine` (feature `stream`); each detected `Match` is fanned
    // to the registering subscriber over a broadcast channel with drop-oldest + lag
    // backpressure. Transport-compatible with everything else here: one Request → one
    // Response, cursor-free — `CepPoll` LONG-POLLS (like `Watch`) for the next match.
    // Gated `streaming` on the wire so the variants exist wherever the CDC surface
    // does; the ENGINE (and thus a real handler) additionally needs `stream` — a build
    // with `streaming` but not `stream` (e.g. `pi`) drops these to the dispatch
    // "not available in this build" catch-all, exactly like any other feature-off op.
    /// Register a live CEP standing query (CONCEPT:EG-KG.query.protocol-types). `pattern_msgpack` is a
    /// MessagePack `CepPatternSpec` (the same pattern algebra `Op::Cep` carries); `buffer`
    /// (0 ⇒ a default) bounds how many unconsumed matches are retained for a lagging
    /// poller before the oldest are dropped. Returns a `Count` — the subscription id to
    /// pass to `CepPoll` / `CepUnsubscribe`.
    #[cfg(feature = "streaming")]
    CepSubscribe {
        #[serde(with = "serde_bytes")]
        pattern_msgpack: Vec<u8>,
        #[serde(default)]
        buffer: u32,
    },
    /// Poll a CEP subscription for the matches pushed since the last poll
    /// (CONCEPT:EG-KG.query.protocol-types), blocking up to `timeout_ms` for the FIRST one if none are ready
    /// (then returns whatever arrived). Returns a `Raw` `Vec<eg_stream::Match>`; an empty
    /// vec means "nothing yet" (re-poll to keep tailing). A dropped subscription (unknown
    /// `sub_id`) is an error.
    #[cfg(feature = "streaming")]
    CepPoll {
        sub_id: u64,
        #[serde(default)]
        timeout_ms: u64,
    },
    /// Drop a CEP standing query + its subscriber (CONCEPT:EG-KG.query.protocol-types). Returns `Bool` (true
    /// if it existed).
    #[cfg(feature = "streaming")]
    CepUnsubscribe {
        sub_id: u64,
    },

    // ── Mining (CONCEPT:EG-KG.mining.frequent-itemset-mining — descriptive data mining) ──
    // The unified data-mining surface. Phase 1 = association-rule mining; later
    // phases add `Mine{Cluster,Anomaly,Sequence,Forecast,Subgraph,…}` variants into
    // THIS section (kept flat + section-commented per the dispatch conventions).
    //
    // `MineAssociate` is compute-near-data: it accepts EITHER explicit `transactions`
    // (each a set of item labels) OR a graph-derived `source` that turns node
    // neighborhoods into transactions (mine directly over resident graph data). It
    // returns rows `{antecedent, consequent, support, confidence, lift}`. With
    // `writeback=true` it materializes each rule as a typed `:AssociationRule` node
    // linked to its item nodes — a graph MUTATION, so it classifies as a write and
    // WAL-replays by re-mining deterministically (explicit transactions reproduce
    // byte-identically; a graph-derived source re-derives from the graph, like the
    // broker/memory ops). Gated `mining`; a build without it drops the variant → the
    // dispatch "not available in this build" catch-all.
    #[cfg(feature = "mining")]
    MineAssociate {
        /// Explicit transactions — each a set of item labels. Empty ⇒ use `source`.
        #[serde(default)]
        transactions: Vec<Vec<String>>,
        /// Graph-derived transaction source (compute-near-data). Used when
        /// `transactions` is empty.
        #[serde(default)]
        source: Option<TransactionSource>,
        /// Minimum fractional support (0.0–1.0) an itemset must meet.
        #[serde(default = "default_min_support")]
        min_support: f64,
        /// Minimum rule confidence (0.0–1.0) to emit.
        #[serde(default = "default_min_confidence")]
        min_confidence: f64,
        /// Which frequent-itemset engine to run (all agree; FP-Growth default).
        #[serde(default)]
        algorithm: MineAlgorithm,
        /// Materialize each rule as a typed `:AssociationRule` node linked to its
        /// item nodes (the discovery flywheel). Makes this a graph write.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a first-class epistemic object per rule (E6,
        /// CONCEPT:EG-KG.epistemic.epistemic-substrate): a `:Claim` (confidence seeded
        /// from the rule's quality score, normalized to `[0,1]`) plus a provenance
        /// `:Evidence` node, both `SUPPORTS`-linked to the claim so the `eg_epistemic`
        /// belief layer can propagate confidence over the mined finding. Requires
        /// `writeback` (the `:AssociationRule` node is the claim's evidence anchor).
        /// Gated `all(mining, epistemic)`; unset ⇒ write-back is byte-identical.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Clustering (CONCEPT:EG-KG.mining.dbscan-density — completing the family beyond
    /// k-Means/spectral). Partitions a feature matrix into clusters via DBSCAN,
    /// hierarchical agglomerative, GMM (EM), or k-medoids (PAM). Rows come from
    /// EITHER explicit `features` OR a graph-derived `source` (the embeddings of a
    /// node label — the cross-modal "cluster the vectors of these nodes" hook).
    /// Returns rows `{cluster_id, members, centroid, score}` (+ GMM
    /// `responsibilities`). With `writeback=true` it materializes each cluster as a
    /// typed `:Cluster` node linked to its member nodes — a graph MUTATION, so it
    /// classifies as a write and WAL-replays by re-clustering deterministically.
    /// Gated `mining`; a build without it drops the variant.
    #[cfg(feature = "mining")]
    MineCluster {
        /// Explicit feature matrix — each row a point. Empty ⇒ use `source`.
        #[serde(default)]
        features: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `features` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source): an
        /// upstream cross-modal RETRIEVAL plan (`Op::Scan|Filter|Traverse|Rank|…`),
        /// executed FIRST over the resident graph/vector/SQL/time modalities; the
        /// resulting RowSet ids are then resolved to their stored embeddings (the
        /// SAME lookup `VectorSource` uses) to build this op's feature rows — so
        /// `retrieve → cluster → writeback` is ONE plan, ONE round-trip
        /// (compute-near-data, no client marshalling between retrieve and mine).
        /// Takes precedence over `source` when present; ignored when `features`
        /// is non-empty. Gated additionally on `query` (the plan algebra lives
        /// behind that feature) — a `mining`-only build without `query` drops
        /// this field.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Which clustering engine to run.
        #[serde(default)]
        algorithm: ClusterAlgorithm,
        /// DBSCAN neighborhood radius.
        #[serde(default = "default_eps")]
        eps: f64,
        /// DBSCAN minimum points (incl. self) for a core point.
        #[serde(default = "default_min_pts")]
        min_pts: usize,
        /// Target cluster count for hierarchical / GMM / k-medoids.
        #[serde(default = "default_k")]
        k: usize,
        /// Hierarchical linkage: `single` · `complete` · `average` (default).
        #[serde(default)]
        linkage: Linkage,
        /// EM / PAM iteration cap (GMM, k-medoids).
        #[serde(default = "default_max_iter")]
        max_iter: usize,
        /// Seed for GMM's k-means++ init (deterministic).
        #[serde(default)]
        seed: u64,
        /// Materialize each cluster as a typed `:Cluster` node linked to members.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per cluster (E6) —
        /// see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// cluster's compactness score. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Anomaly / outlier detection (CONCEPT:EG-KG.mining.isolation-forest). Scores
    /// every feature row for how anomalous it is via z-score/MAD, Isolation Forest,
    /// LOF, or One-Class SVM, and flags rows over `threshold` (per-algorithm default
    /// when unset). Rows come from EITHER explicit `features`, a 1-D `values` series
    /// (each value → one row — the tsdb RCA hook), OR a graph-derived `source` (node
    /// embeddings). Returns rows `{id, anomaly_score, is_anomaly}`. With
    /// `writeback=true` it materializes each flagged row as a typed `:Anomaly` node
    /// linked to its source node — a graph MUTATION (write, WAL-replayed
    /// deterministically). Gated `mining`.
    #[cfg(feature = "mining")]
    MineAnomaly {
        /// Explicit feature matrix — each row a point. Empty ⇒ use `values`/`source`.
        #[serde(default)]
        features: Vec<Vec<f64>>,
        /// 1-D series convenience — each scalar becomes a one-element row (e.g. a
        /// tsdb window for root-cause analysis). Used when `features` is empty.
        #[serde(default)]
        values: Vec<f64>,
        /// Graph-derived vector source (node embeddings). Used when `features` and
        /// `values` are both empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when
        /// `features`/`values` is non-empty.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Which detector to run.
        #[serde(default)]
        algorithm: AnomalyAlgorithm,
        /// LOF neighbor count.
        #[serde(default = "default_lof_k")]
        k: usize,
        /// Isolation Forest tree count.
        #[serde(default = "default_n_trees")]
        n_trees: usize,
        /// Isolation Forest subsample size.
        #[serde(default = "default_sample_size")]
        sample_size: usize,
        /// Seed for Isolation Forest (deterministic).
        #[serde(default)]
        seed: u64,
        /// One-Class SVM ν ∈ (0,1] (upper bound on the outlier fraction).
        #[serde(default = "default_nu")]
        nu: f64,
        /// One-Class SVM RBF gamma; `≤ 0` ⇒ the `1/n_features` default.
        #[serde(default)]
        gamma: f64,
        /// One-Class SVM kernel: `rbf` (default) · `linear`.
        #[serde(default)]
        kernel: SvmKernel,
        /// Flag threshold (higher score = more anomalous). Unset ⇒ per-algorithm default.
        #[serde(default)]
        threshold: Option<f64>,
        /// Materialize each flagged row as a typed `:Anomaly` node linked to its source.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per flagged anomaly
        /// (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the row's anomaly score. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Classification — FIT (CONCEPT:EG-KG.mining.naive-bayes). PREDICTIVE: fit a
    /// classifier over labeled feature rows and return a serializable model blob
    /// (`FittedClassifier`), mirroring `DsFitEstimator`. Completes the classifier
    /// family beyond the datascience tree/forest/boosting estimators with Naive Bayes
    /// (Gaussian/Multinomial), k-NN, one-vs-rest logistic regression, and linear SVC.
    /// Rows come from EITHER explicit `x` OR a graph-derived `source` (node embeddings
    /// with OWL/ontology feature vectors — "classify nodes using their embeddings").
    /// Read-only (no graph mutation). Gated `mining`.
    #[cfg(feature = "mining")]
    MineClassifyFit {
        /// Explicit feature matrix — each row a sample. Empty ⇒ use `source`.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `x` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when `x`
        /// is non-empty. NOTE: `y` labels must still align by position with the
        /// plan's resulting row order.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Integer class labels, one per row (required).
        #[serde(default)]
        y: Vec<i64>,
        /// Which classifier to fit.
        #[serde(default)]
        algorithm: ClassifyAlgorithm,
        /// k-NN neighbor count.
        #[serde(default = "default_knn_k")]
        k: usize,
        /// Multinomial NB Laplace smoothing.
        #[serde(default = "default_nb_alpha")]
        alpha: f64,
        /// Logistic / SVC learning rate.
        #[serde(default = "default_class_lr")]
        lr: f64,
        /// Logistic / SVC gradient-descent epochs.
        #[serde(default = "default_class_epochs")]
        epochs: usize,
        /// Logistic L2 regularization strength.
        #[serde(default)]
        l2: f64,
        /// Linear-SVC inverse-regularization C.
        #[serde(default = "default_svc_c")]
        c: f64,
    },

    /// Classification — PREDICT (CONCEPT:EG-KG.mining.naive-bayes). Takes a fitted
    /// `model` blob back plus a feature matrix and returns per-row `{labels, proba}`,
    /// mirroring `DsPredictEstimator`. Rows come from EITHER explicit `x` OR a
    /// graph-derived `source` (node embeddings). With `writeback=true` it materializes
    /// each prediction as a typed `:Classification` node linked to its source node — a
    /// graph MUTATION (write, WAL-replayed deterministically). Gated `mining`.
    #[cfg(feature = "mining")]
    MineClassifyPredict {
        /// The fitted model blob from `MineClassifyFit`.
        model: crate::wire::FittedClassifier,
        /// Explicit feature matrix — each row a sample. Empty ⇒ use `source`.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `x` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when `x`
        /// is non-empty.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Materialize each prediction as a typed `:Classification` node linked to its
        /// source node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per prediction (D3,
        /// mirroring E6) — see [`Method::MineAssociate::as_claim`]. Confidence is
        /// seeded from the prediction's OWN max class probability (`out.proba[i]`'s
        /// argmax), already `[0,1]` by construction (a probability simplex row).
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Dimensionality reduction (CONCEPT:EG-KG.mining.truncated-svd). DESCRIPTIVE:
    /// transform a feature matrix into low-D `coords` via truncated SVD, LDA
    /// (supervised — needs `labels`), UMAP, or t-SNE. Rows come from EITHER explicit
    /// `x` OR a graph-derived `source` (node embeddings — "reduce these node vectors
    /// for the graphviz"). Returns rows `{id, coords}`. With `writeback=true` it
    /// materializes each row's reduced vector as a typed `:Embedding2D` node linked to
    /// its source node — a graph MUTATION (write, WAL-replayed deterministically).
    /// Gated `mining`.
    #[cfg(feature = "mining")]
    MineReduce {
        /// Explicit feature matrix — each row a point. Empty ⇒ use `source`.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings). Used when `x` is empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see
        /// `MineCluster::plan`. Takes precedence over `source`; ignored when `x`
        /// is non-empty.
        #[cfg(feature = "query")]
        #[serde(default)]
        plan: Option<crate::wire::Plan>,
        /// Class labels, one per row — REQUIRED for LDA (ignored otherwise).
        #[serde(default)]
        labels: Vec<i64>,
        /// Which reduction engine to run.
        #[serde(default)]
        algorithm: ReduceAlgorithm,
        /// Target dimensionality of the embedding.
        #[serde(default = "default_n_components")]
        n_components: usize,
        /// UMAP neighbor count.
        #[serde(default = "default_umap_neighbors")]
        n_neighbors: usize,
        /// UMAP minimum embedded distance.
        #[serde(default = "default_umap_min_dist")]
        min_dist: f64,
        /// t-SNE perplexity.
        #[serde(default = "default_tsne_perplexity")]
        perplexity: f64,
        /// UMAP / t-SNE optimization epochs.
        #[serde(default = "default_reduce_epochs")]
        epochs: usize,
        /// t-SNE learning rate.
        #[serde(default = "default_tsne_lr")]
        lr: f64,
        /// Seed for UMAP / t-SNE (deterministic layout).
        #[serde(default)]
        seed: u64,
        /// Materialize each row's reduced vector as a typed `:Embedding2D` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) — D3, mirroring E6 — but
        /// ONLY for `svd` (`ReduceAlgorithm::Svd`), the one engine with a principled
        /// `[0,1]` quality score: the retained EXPLAINED-VARIANCE RATIO
        /// (`Σ retained singular_values² / Σ ALL row sum-of-squares`, i.e. how much of
        /// the rows' total variance the kept components capture). `lda`/`umap`/`tsne`
        /// have no such score (LDA's discriminant eigenvalues aren't returned;
        /// UMAP/t-SNE are approximate neighborhood LAYOUTS with no reconstruction-error
        /// analogue) — for those, `as_claim=true` is a documented no-op (no claim is
        /// written; see [`Method::MineAssociate::as_claim`] for the general shape).
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    // ── Graph Learning (CONCEPT:EG-KG.graphlearn.link-predictor — neuro-symbolic KAN) ──
    // A learnable link-predictor over the resident graph whose learned per-feature
    // edge functions are themselves queryable KG nodes (interpretability, not raw
    // accuracy). `GraphLearnFit` learns a KAN model over a graph-derived subgraph
    // (positives = observed edges, negatives = sampled non-edges) and returns the
    // model blob (incl. the learned edge functions); with `writeback=true` it
    // materializes typed `:EdgeFunction` nodes. `GraphLearnPredict` scores candidate
    // node pairs (or the top-k missing links) with a fitted model and, with
    // `writeback=true`, materializes `:PredictedEdge` nodes. Both writeback paths are
    // graph MUTATIONS → classify as writes and WAL-replay by re-deriving from the
    // current graph (like mining). Gated `graphlearn`; a build without it drops the
    // variants → the dispatch "not available in this build" catch-all.
    #[cfg(feature = "graphlearn")]
    GraphLearnFit {
        /// The graph-derived subgraph to learn over (node label + relation/direction).
        source: GraphSource,
        /// Training + architecture knobs (all defaulted).
        #[serde(default)]
        params: GraphLearnParams,
        /// Materialize the learned per-feature `:EdgeFunction` nodes (a graph write).
        #[serde(default)]
        writeback: bool,
    },
    #[cfg(feature = "graphlearn")]
    GraphLearnPredict {
        /// A fitted `KanLinkModel` blob (as returned by `GraphLearnFit`).
        model: serde_json::Value,
        /// The subgraph providing the structural features (usually the same source).
        source: GraphSource,
        /// Explicit candidate pairs `(src, dst)` to score. Empty ⇒ score the top-k
        /// highest-probability MISSING links across the subgraph.
        #[serde(default)]
        candidate_pairs: Vec<(String, String)>,
        /// Cap on returned predictions (0 ⇒ uncapped).
        #[serde(default = "default_gl_top_k")]
        top_k: usize,
        /// Materialize each scored pair as a typed `:PredictedEdge` node (a graph write).
        #[serde(default)]
        writeback: bool,
    },

    // ── ML Pipeline (CONCEPT:EG-KG.mining.ml-pipeline) ──
    // A composable train→eval→serve→predict pipeline over a versioned `:Model`
    // artifact that GENERALIZES the KAN one-off (GraphLearn* above): ordered feature
    // steps → split → a pluggable model family (classify | estimator | graphlearn).
    // GRAPH-SCOPED like mining/graphlearn — features are read off the live subgraph and
    // the versioned `:Model`/`:ServedModel`/`:Prediction` write-backs materialize into
    // the core. Train/Serve/Predict are RUNTIME-CONDITIONAL writes (routed via
    // `commit_conditional_mutation`, like the GraphLearn*/Mine* families); Evaluate and
    // Compare are read-only. Gated `ml-pipeline`; a build without it drops every variant
    // → the graph_ops "not available in this build" catch-all.
    #[cfg(feature = "ml-pipeline")]
    MiningPipelineTrain {
        /// Pipeline name — versioned `:Model` artifacts are keyed by it (`v1`, `v2`…).
        name: String,
        /// The graph-derived node source the feature steps read. Empty ⇒ `x` explicit.
        #[serde(default)]
        source: Option<GraphSource>,
        /// Explicit feature matrix — each row a sample. Empty ⇒ built from `source`
        /// via the spec's feature steps.
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Explicit integer labels aligned to the rows / source node order. Empty ⇒
        /// read from each node's `spec.label_property` (node classification).
        #[serde(default)]
        y: Vec<i64>,
        /// The composable pipeline recipe (features → split → model).
        spec: crate::wire::PipelineSpec,
        /// Persist the fitted model as a versioned `:Model` node (a graph write).
        /// `false` ⇒ dry-run: fit + report metrics without materializing an artifact.
        #[serde(default = "default_true")]
        writeback: bool,
    },
    #[cfg(feature = "ml-pipeline")]
    MiningPipelineEvaluate {
        /// The pipeline whose stored model to score.
        name: String,
        /// Model version to evaluate; `0` ⇒ the currently-served version.
        #[serde(default)]
        version: u64,
        /// Node source to build the evaluation features from (via the model's stored
        /// feature recipe). Empty ⇒ use explicit `x`.
        #[serde(default)]
        source: Option<GraphSource>,
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Ground-truth integer labels; empty ⇒ read the model's `label_property`.
        #[serde(default)]
        y: Vec<i64>,
    },
    #[cfg(feature = "ml-pipeline")]
    MiningPipelineServe {
        /// The pipeline whose version to deploy.
        name: String,
        /// The `:Model` version to mark served (predict-by-name then resolves it).
        version: u64,
    },
    #[cfg(feature = "ml-pipeline")]
    MiningPipelinePredict {
        /// The pipeline to predict with.
        name: String,
        /// Model version; `0` ⇒ the currently-served version.
        #[serde(default)]
        version: u64,
        /// Node source to predict over (rebuilds features via the model's recipe).
        /// Empty ⇒ use explicit `x`.
        #[serde(default)]
        source: Option<GraphSource>,
        #[serde(default)]
        x: Vec<Vec<f64>>,
        /// Materialize each prediction as a typed `:Prediction` node linked to its
        /// source node (a graph write).
        #[serde(default)]
        writeback: bool,
    },
    #[cfg(feature = "ml-pipeline")]
    MiningPipelineCompare {
        /// The pipeline whose two versions to compare.
        name: String,
        /// The two `:Model` versions to diff (held-out metrics).
        version_a: u64,
        version_b: u64,
    },

    /// Sequential-pattern mining (CONCEPT:EG-KG.mining.prefixspan — Phase 4).
    /// Finds frequent ORDERED subsequences (PrefixSpan or GSP; both agree) over
    /// EITHER explicit `sequences` (each a time-ordered list of item labels — an
    /// item may repeat) OR a graph-derived `source` that turns each node's
    /// ordered neighbor list (following resident edge insertion order) into one
    /// sequence — the "what reliably follows what" hook (evolution/commit
    /// timelines, event streams). Returns rows `{items, support, count}`. With
    /// `writeback=true` it materializes each pattern as a typed
    /// `:SequentialPattern` node linked to any item that is a resident node — a
    /// graph MUTATION, WAL-replayed by re-mining deterministically. Gated
    /// `mining`.
    #[cfg(feature = "mining")]
    MineSequence {
        /// Explicit ordered sequences — each a time-ordered list of item labels.
        /// Empty ⇒ use `source`.
        #[serde(default)]
        sequences: Vec<Vec<String>>,
        /// Graph-derived sequence source (compute-near-data). Used when
        /// `sequences` is empty.
        #[serde(default)]
        source: Option<SequenceSource>,
        /// Minimum fractional support (0.0–1.0) a pattern must meet.
        #[serde(default = "default_min_support")]
        min_support: f64,
        /// Which sequential-pattern engine to run (both agree; PrefixSpan default).
        #[serde(default)]
        algorithm: MineSeqAlgorithm,
        /// Materialize each pattern as a typed `:SequentialPattern` node linked to
        /// its resident item nodes.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per pattern (E6) —
        /// see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// pattern's support. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Classical time-series forecasting (CONCEPT:EG-KG.mining.arima — Phase 4).
    /// Forecasts `horizon` future points (with an approximate confidence band)
    /// from a 1-D `values` series — a tsdb window handed in by the caller,
    /// mirroring `MineAnomaly`'s client-supplied `values` cut (the native
    /// in-handler TsScan source is the same documented follow-up). `algorithm`
    /// selects ARIMA(p,d,q) (Hannan-Rissanen), additive Holt-Winters/ETS
    /// (degrades to Holt linear-trend when `period` is 0), or a classical STL-
    /// style decomposition + trend/seasonal extrapolation. With
    /// `writeback=true` it materializes the forecast as a typed `:Forecast`
    /// node — linked to a resident node named `series_id` when one exists — a
    /// graph MUTATION, WAL-replayed by re-forecasting deterministically. Gated
    /// `mining`.
    #[cfg(feature = "mining")]
    MineForecast {
        /// The 1-D series to forecast (required — a tsdb window handed in by
        /// the caller).
        #[serde(default)]
        values: Vec<f64>,
        /// Which forecasting engine to run.
        #[serde(default)]
        algorithm: ForecastAlgorithm,
        /// Steps to forecast beyond the series.
        #[serde(default = "default_horizon")]
        horizon: usize,
        /// ARIMA autoregressive order.
        #[serde(default = "default_arima_p")]
        p: usize,
        /// ARIMA differencing order.
        #[serde(default = "default_arima_d")]
        d: usize,
        /// ARIMA moving-average order.
        #[serde(default)]
        q: usize,
        /// Seasonal period for Holt-Winters / STL (`0` ⇒ non-seasonal Holt
        /// linear-trend fallback for Holt-Winters; trend-only for STL).
        #[serde(default)]
        period: usize,
        /// Holt-Winters level smoothing.
        #[serde(default = "default_hw_alpha")]
        alpha: f64,
        /// Holt-Winters trend smoothing.
        #[serde(default = "default_hw_beta")]
        beta: f64,
        /// Holt-Winters seasonal smoothing.
        #[serde(default = "default_hw_gamma")]
        gamma: f64,
        /// Two-sided confidence level for the forecast band (e.g. `0.95`).
        #[serde(default = "default_confidence")]
        confidence: f64,
        /// Optional identity for the write-back `:Forecast` node; when it names
        /// a resident node, the forecast is linked `FORECAST_OF` → that node.
        /// Empty ⇒ the node id is derived from the input `values` + `algorithm`.
        #[serde(default)]
        series_id: String,
        /// Materialize the forecast as a typed `:Forecast` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the forecast (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// forecast's `confidence` band level. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Text mining (CONCEPT:EG-KG.mining.tfidf — Phase 4). `tfidf` returns each
    /// document's term weights (descriptive, read-only); `lda`/`nmf` fit a
    /// `k`-topic model over the corpus. Documents come from EITHER explicit
    /// `docs` (each a pre-tokenized `Vec<String>` — use `tokenize`-equivalent
    /// client-side, or pass raw words) OR a graph-derived `source` that
    /// tokenizes a text property off a node label (compute-near-data — no
    /// Tantivy/eg-text dependency). With `writeback=true` (`lda`/`nmf` only)
    /// each topic is materialized as a typed `:Topic` node, linked to any
    /// source document that is a resident node — a graph MUTATION,
    /// WAL-replayed by re-mining deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineText {
        /// Explicit pre-tokenized documents. Empty ⇒ use `source`.
        #[serde(default)]
        docs: Vec<Vec<String>>,
        /// Graph-derived text source (compute-near-data). Used when `docs` is
        /// empty.
        #[serde(default)]
        source: Option<TextSource>,
        /// Which text-mining engine to run.
        #[serde(default)]
        algorithm: TextAlgorithm,
        /// Topic count for `lda`/`nmf`.
        #[serde(default = "default_topic_k")]
        k: usize,
        /// LDA symmetric doc-topic Dirichlet prior.
        #[serde(default = "default_lda_alpha")]
        alpha: f64,
        /// LDA symmetric topic-term Dirichlet prior.
        #[serde(default = "default_lda_beta")]
        beta: f64,
        /// Gibbs sweeps (`lda`) / multiplicative-update iterations (`nmf`).
        #[serde(default = "default_text_iterations")]
        iterations: usize,
        /// Seed for LDA's Gibbs sampler / NMF's initial factors (deterministic).
        #[serde(default)]
        seed: u64,
        /// How many terms to keep per document/topic row.
        #[serde(default = "default_top_n")]
        top_n: usize,
        /// Materialize each topic as a typed `:Topic` node (`lda`/`nmf` only —
        /// a no-op for `tfidf`, which has no topics to write back).
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per topic (D3, mirroring
        /// E6) — `lda`/`nmf` only, a no-op for `tfidf` (which has no topics, mirroring
        /// `writeback`). Quality = the topic's mean doc-membership strength among the
        /// documents DOMINANTLY assigned to it (`mean(doc_topics[d][t])` over docs `d`
        /// whose argmax topic is `t`) — a topic-coherence proxy: both LDA's Dirichlet
        /// posterior and NMF's row-normalized `W` are already `[0,1]` distributions that
        /// sum to 1 across topics (see `eg_compute::mining::text` module docs), so this
        /// is a principled, already-bounded score requiring no extra normalization.
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Frequent subgraph mining + motif counting (CONCEPT:EG-KG.mining.gspan-frequent-subgraph
    /// — Phase 4, the graph-native differentiator). UNLIKE every other mining
    /// op in this family, this one mines the RESIDENT GRAPH's own topology
    /// directly — no rows/vectors handed in. `gspan` finds frequent connected
    /// subgraph PATTERNS (level-wise growth up to `max_edges` edges,
    /// canonicalized + exactly re-counted); `motif` censuses small
    /// label-agnostic topological motifs (wedges, triangles, directed
    /// 3-cycles). `label`, when given, restricts the scanned host graph to
    /// nodes of that one type (both edge endpoints must match) — `None` scans
    /// the whole resident graph heterogeneously. With `writeback=true`
    /// (`gspan` only) each frequent pattern is materialized as a typed
    /// `:FrequentSubgraph` node, linked to every host node appearing in any of
    /// its embeddings — a graph MUTATION, WAL-replayed by re-mining
    /// deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineSubgraph {
        /// Optional: restrict the host graph to nodes of this one type.
        /// `None` ⇒ the whole resident graph (heterogeneous).
        #[serde(default)]
        label: Option<String>,
        /// Minimum fractional support (0.0–1.0, of the host's total edge
        /// count) a pattern's embedding count must meet. Ignored by `motif`.
        #[serde(default = "default_min_support")]
        min_support: f64,
        /// Pattern-size growth cap (tractability). Ignored by `motif`.
        #[serde(default = "default_max_subgraph_edges")]
        max_edges: usize,
        /// Which algorithm to run.
        #[serde(default)]
        algorithm: SubgraphAlgorithm,
        /// Materialize each frequent pattern as a typed `:FrequentSubgraph`
        /// node (`gspan` only — a no-op for `motif`, which has no patterns).
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per frequent pattern
        /// (E6, `gspan` only) — see [`Method::MineAssociate::as_claim`]. Confidence
        /// is seeded from the pattern's support. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    // ── Residual insight/mining families (Gap-5) ──────────────────────
    // Rounds out the mining surface begun by `MineAssociate` above with 8 more
    // families, each following the SAME shape (explicit-or-graph-derived input,
    // optional `writeback` of a typed node, optional `as_claim` epistemic
    // writeback gated `all(mining, epistemic)`).
    /// Entity resolution + record linkage (CONCEPT:EG-KG.mining.entity-resolution).
    /// DISTINCT from the existing always-on `ResolveCandidates` op (all-pairs
    /// cosine + union-find dedup-ladder proposals, no epistemic writeback): this
    /// mining family instead supports BOTH Jaccard record linkage over token
    /// attributes (`records`, blocked by an explicit `block_keys`) AND cosine
    /// entity resolution over embeddings (`vectors`/`source`, blocked by a grid
    /// bucket), and materializes each match as a typed `:EntityMatch` node — a
    /// graph MUTATION, WAL-replayed by re-resolving deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineEntityResolve {
        /// Token-attribute records (Jaccard record linkage). Empty ⇒ use
        /// `vectors`/`source`.
        #[serde(default)]
        records: Vec<Vec<String>>,
        /// Blocking key per record, same length as `records`. All-empty-string
        /// (or shorter than `records`) ⇒ one global block (no blocking).
        #[serde(default)]
        block_keys: Vec<String>,
        /// Explicit embedding rows (cosine entity resolution). Used when
        /// `records` is empty; empty ⇒ use `source`.
        #[serde(default)]
        vectors: Vec<Vec<f64>>,
        /// Graph-derived vector source (node embeddings) — used when `records`
        /// and `vectors` are both empty.
        #[serde(default)]
        source: Option<VectorSource>,
        /// Optional external ids parallel to `records`/`vectors` (the explicit
        /// paths only — `source` supplies its own resident node ids). Shorter
        /// than the input ⇒ missing entries fall back to their index.
        #[serde(default)]
        ids: Vec<String>,
        /// Grid-bucket rounding precision for the `vectors`/`source` blocking path.
        #[serde(default = "default_bucket_precision")]
        bucket_precision: i32,
        /// Minimum similarity (Jaccard or Cosine, `[0,1]`) to emit a match.
        #[serde(default = "default_match_threshold")]
        threshold: f64,
        /// Materialize each match as a typed `:EntityMatch` node linked to both
        /// members (when they are resident node ids).
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per match (E6) —
        /// see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// match's OWN similarity. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Causal impact estimation (CONCEPT:EG-KG.mining.causal-impact): interrupted
    /// time series (a single `series`) or difference-in-differences (`series` +
    /// non-empty `control`), split at `intervention_index`. Mirrors
    /// `MineForecast`'s "caller hands in the tsdb window" convention — no direct
    /// tsdb coupling. With `writeback=true` materializes a typed `:CausalEffect`
    /// node — a graph MUTATION, WAL-replayed by re-estimating deterministically.
    /// Gated `mining`.
    #[cfg(feature = "mining")]
    MineCausalImpact {
        /// The (treatment, for DiD) series to analyze — required.
        #[serde(default)]
        series: Vec<f64>,
        /// The control series for difference-in-differences. Empty ⇒ plain
        /// interrupted-time-series (no control).
        #[serde(default)]
        control: Vec<f64>,
        /// Index of the FIRST post-intervention observation (in BOTH series for DiD).
        #[serde(default)]
        intervention_index: usize,
        /// Optional identity for the write-back `:CausalEffect` node. Empty ⇒
        /// derived from the input series + algorithm.
        #[serde(default)]
        series_id: String,
        /// Materialize the estimate as a typed `:CausalEffect` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the estimate
        /// (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded
        /// from the estimate's own significance (`1 - two_sided_p`). Requires
        /// `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Process mining (CONCEPT:EG-KG.mining.process-mining): directly-follows
    /// graph + alpha-miner-lite footprint (causal / parallel / choice relations,
    /// start/end activity sets) over ordered event `traces`. With
    /// `writeback=true` materializes the footprint as a typed `:ProcessModel`
    /// node — a graph MUTATION, WAL-replayed by re-mining deterministically.
    /// Gated `mining`.
    #[cfg(feature = "mining")]
    MineProcess {
        /// Ordered activity-label traces — each a time-ordered event sequence
        /// (an activity may repeat within a trace). Required.
        #[serde(default)]
        traces: Vec<Vec<String>>,
        /// Optional identity for the write-back `:ProcessModel` node. Empty ⇒
        /// derived from the mined footprint's own shape.
        #[serde(default)]
        process_id: String,
        /// Materialize the footprint as a typed `:ProcessModel` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the model (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the fraction of observed activity pairs classified `causal`/`parallel`
        /// (vs. `choice`) — a log-coverage proxy, already `[0,1]`. Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Root-cause propagation (CONCEPT:EG-KG.mining.root-cause): given a directed
    /// weighted dependency graph (`edges`, `cause -> effect`) and a per-node
    /// anomaly `scores` vector (the existing `anomaly` family's own output, or
    /// any other score), find the most-likely upstream root cause of one
    /// `symptom` node. With `writeback=true` materializes the top candidate as a
    /// typed `:RootCause` node linked to the symptom — a graph MUTATION,
    /// WAL-replayed by re-searching deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineRootCause {
        /// Node ids, index-aligned with `scores` and referenced by `edges`.
        #[serde(default)]
        nodes: Vec<String>,
        /// Anomaly score per node, index-aligned with `nodes` (negative clamps to `0.0`).
        #[serde(default)]
        scores: Vec<f64>,
        /// Dependency edges `(cause_id, effect_id, weight)`; `weight` clamped to `[0,1]`.
        #[serde(default)]
        edges: Vec<(String, String, f64)>,
        /// The already-flagged anomalous node whose root cause to find (required).
        #[serde(default)]
        symptom: String,
        /// Search depth cap.
        #[serde(default = "default_max_hops")]
        max_hops: usize,
        /// Per-hop score decay `(0,1]` (mirrors PageRank's damping factor).
        #[serde(default = "default_decay")]
        decay: f64,
        /// Materialize the top candidate as a typed `:RootCause` node linked to the symptom.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the top
        /// candidate (E6) — see [`Method::MineAssociate::as_claim`]. Confidence
        /// mirrors `anomaly`'s `score / (1 + score)` mapping over the candidate's
        /// OWN raw responsibility score (normalizing against the candidate list
        /// would be trivially `1.0` for the top candidate). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Seeded risk propagation (CONCEPT:EG-KG.mining.risk-propagation): personalized
    /// PageRank over a directed weighted graph (`edges`), restarting to a `seed`
    /// risk distribution instead of teleporting uniformly. With `writeback=true`
    /// materializes each node's propagated score as a typed `:RiskScore` node — a
    /// graph MUTATION, WAL-replayed by re-propagating deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineRiskPropagation {
        /// Node ids, index-aligned with `seed` and referenced by `edges`.
        #[serde(default)]
        nodes: Vec<String>,
        /// Seed risk per node, index-aligned with `nodes` (any non-negative
        /// scale — normalized internally; all-zero ⇒ all-zero result).
        #[serde(default)]
        seed: Vec<f64>,
        /// Weighted directed edges `(from_id, to_id, weight)`; `weight` clamped `>= 0`.
        #[serde(default)]
        edges: Vec<(String, String, f64)>,
        /// Damping factor (probability of following an edge vs. restarting to `seed`).
        #[serde(default = "default_damping")]
        damping: f64,
        /// L1 convergence tolerance.
        #[serde(default = "default_risk_tolerance")]
        tolerance: f64,
        /// Hard iteration cap.
        #[serde(default = "default_max_iter")]
        max_iterations: usize,
        /// Materialize each node's propagated score as a typed `:RiskScore` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per scored node (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// node's own propagated share (already `[0,1]`, mass-conserving). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Ontology-gap detection (CONCEPT:EG-KG.mining.ontology-gap): scans the
    /// resident graph's own node-`type`/edge-`relationship` class shape (GRAPH-NATIVE —
    /// no `rdf`/OWL-reasoner dependency) for completeness gaps: no declared
    /// properties, an unresolved `subClassOf` parent (an orphan subclass), or a
    /// fully disconnected class. With `writeback=true` materializes each gap as a
    /// typed `:OntologyGap` node linked to its class — a graph MUTATION,
    /// WAL-replayed by re-scanning deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineOntologyGap {
        /// Optional: restrict the scan to class nodes of this one type
        /// (`None` ⇒ every node whose `type`/`node_type` is `Class` or `OwlClass`).
        #[serde(default)]
        label: Option<String>,
        /// Materialize each gap as a typed `:OntologyGap` node linked to its class.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per gap (E6) — see
        /// [`Method::MineAssociate::as_claim`]. Confidence is seeded from the
        /// gap kind's fixed documented severity (`eg_compute::mining::ontology_gap::GapKind::severity`).
        /// Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Retrieval-quality evaluation (CONCEPT:EG-KG.mining.retrieval-quality):
    /// precision@k / recall@k / MRR over stored retrieval `traces`. With
    /// `writeback=true` materializes the aggregate report as a typed
    /// `:RetrievalQuality` node — a graph MUTATION, WAL-replayed by
    /// re-evaluating deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineRetrievalQuality {
        /// Retrieval traces to evaluate — required.
        #[serde(default)]
        traces: Vec<RetrievalTraceSpec>,
        /// Precision/recall/MRR cutoff. `0` ⇒ use each trace's full retrieved list.
        #[serde(default)]
        k: usize,
        /// Optional identity for the write-back `:RetrievalQuality` node. Empty ⇒
        /// derived from the input traces.
        #[serde(default)]
        query_id: String,
        /// Materialize the aggregate report as a typed `:RetrievalQuality` node.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the report (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the report's own F1 (harmonic mean of precision@k/recall@k, already
        /// `[0,1]`). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },

    /// Community detection as a mining family (CONCEPT:EG-KG.mining.community-writeback):
    /// wraps the EXISTING GDS Louvain / label-propagation kernels
    /// (`eg_compute::graph_algos`, already exposed on the Cypher `CALL gds.*`
    /// surface) — adds NO new algorithm, only the epistemic writeback. Runs over
    /// the resident graph (optionally restricted to one node `label`, like
    /// `MineSubgraph`). With `writeback=true` materializes each community as a
    /// typed `:Community` node linked to its members — a graph MUTATION,
    /// WAL-replayed by re-detecting deterministically. Gated `mining`.
    #[cfg(feature = "mining")]
    MineCommunity {
        /// Optional: restrict the projected graph to nodes of this one type.
        #[serde(default)]
        label: Option<String>,
        /// Which existing GDS kernel to run.
        #[serde(default)]
        algorithm: CommunityAlgorithm,
        /// Louvain modularity resolution (ignored by label-propagation).
        #[serde(default = "default_resolution")]
        resolution: f64,
        /// Iteration/sweep cap.
        #[serde(default = "default_max_iter")]
        max_iterations: usize,
        /// Seed for Louvain's deterministic shuffle (ignored by label-propagation).
        #[serde(default)]
        seed: u64,
        /// Weight neighbor votes by edge weight (label-propagation only; ignored by Louvain).
        #[serde(default = "default_true")]
        weighted: bool,
        /// Materialize each community as a typed `:Community` node linked to its members.
        #[serde(default)]
        writeback: bool,
        /// ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per community (E6)
        /// — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from
        /// the community's own internal-edge density (already `[0,1]`). Requires `writeback`.
        #[cfg(all(feature = "mining", feature = "epistemic"))]
        #[serde(default)]
        as_claim: bool,
    },
}

impl Method {
    /// The wire tag name for this variant (e.g. `"Ping"`, `"AddNode"`) — the
    /// adjacently-tagged `"method"` field serde already writes
    /// (`#[serde(tag = "method", content = "params")]`). Used by the v1
    /// signed-envelope path (CONCEPT:EG-KG.security.signed-request-envelope) to bind the
    /// operation name into the signature without needing the `metrics`
    /// feature's `strum::IntoStaticStr` derive, which isn't compiled into
    /// every serving tier.
    pub fn tag_name(&self) -> String {
        serde_json::to_value(self)
            .ok()
            .and_then(|v| v.get("method").and_then(|m| m.as_str().map(str::to_string)))
            .unwrap_or_default()
    }

    /// Deterministic byte encoding of this method (tag + params) for the v1
    /// envelope's body-hash binding (CONCEPT:EG-KG.security.signed-request-envelope). Reuses
    /// the SAME named-map MessagePack encoder the wire transport already uses
    /// (`rmp_serde::to_vec_named`) so the bytes are stable across the process
    /// (field order is the struct's declared order) without a bespoke
    /// canonicalizer. Hashing this (rather than transmitting a client-supplied
    /// hash) means the verifier NEVER trusts an attacker-stated body hash — it
    /// always recomputes it from the actual `method` that rode the wire.
    pub fn canonical_body_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec_named(self).unwrap_or_default()
    }
}

// ── Supporting Types ────────────────────────────────────────────────────

/// Which frequent-itemset engine `MineAssociate` runs (CONCEPT:EG-KG.mining.frequent-itemset-mining).
/// All three are exact and agree on the frequent-itemset set for a given support;
/// they differ only in traversal strategy. FP-Growth is the default (no candidate
/// generation → fastest on dense baskets).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MineAlgorithm {
    #[default]
    Fpgrowth,
    Apriori,
    Eclat,
}

/// Which sequential-pattern engine `MineSequence` runs (CONCEPT:EG-KG.mining.prefixspan
/// — Phase 4). Both are exact and agree on the frequent-pattern set for a given
/// support. PrefixSpan is the default (projection-based, no candidate generation).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MineSeqAlgorithm {
    #[default]
    Prefixspan,
    Gsp,
}

/// Which forecasting engine `MineForecast` runs (CONCEPT:EG-KG.mining.arima —
/// Phase 4). ARIMA is the default (Hannan-Rissanen AR(p)/MA(q) after `d`-order
/// differencing); `holtwinters` degrades to Holt's linear-trend method when
/// `period` is 0; `stl` is a classical decomposition + extrapolation.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ForecastAlgorithm {
    #[default]
    Arima,
    Holtwinters,
    Stl,
}

/// Which text-mining engine `MineText` runs (CONCEPT:EG-KG.mining.tfidf — Phase
/// 4). TF-IDF is the default (descriptive per-document term weights); `lda`/
/// `nmf` fit a `k`-topic model.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TextAlgorithm {
    #[default]
    Tfidf,
    Lda,
    Nmf,
}

/// Which algorithm `MineSubgraph` runs (CONCEPT:EG-KG.mining.gspan-frequent-subgraph
/// — Phase 4). `gspan` is the default (labeled frequent-subgraph patterns);
/// `motif` is a label-agnostic topological census.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SubgraphAlgorithm {
    #[default]
    Gspan,
    Motif,
}

/// serde default for [`Method::MineSubgraph::max_edges`].
#[cfg(feature = "mining")]
fn default_max_subgraph_edges() -> usize {
    3
}

// ── Residual insight/mining families — supporting types + defaults ─────────

/// One stored retrieval trace for `MineRetrievalQuality` (CONCEPT:EG-KG.mining.retrieval-quality):
/// what a query actually retrieved (ranked) vs. what was actually relevant.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalTraceSpec {
    /// Ranked ids the retrieval actually returned.
    pub retrieved: Vec<String>,
    /// Ground-truth relevant ids for this query.
    pub relevant: Vec<String>,
}

/// Which existing GDS kernel `MineCommunity` wraps (CONCEPT:EG-KG.mining.community-writeback).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CommunityAlgorithm {
    #[default]
    Louvain,
    #[serde(rename = "labelprop")]
    LabelPropagation,
}

/// serde default for [`Method::MineEntityResolve::bucket_precision`].
#[cfg(feature = "mining")]
fn default_bucket_precision() -> i32 {
    1
}

/// serde default for [`Method::MineEntityResolve::threshold`].
#[cfg(feature = "mining")]
fn default_match_threshold() -> f64 {
    0.5
}

/// serde default for [`Method::MineRootCause::max_hops`].
#[cfg(feature = "mining")]
fn default_max_hops() -> usize {
    5
}

/// serde default for [`Method::MineRootCause::decay`].
#[cfg(feature = "mining")]
fn default_decay() -> f64 {
    0.85
}

/// serde default for [`Method::MineRiskPropagation::damping`].
#[cfg(feature = "mining")]
fn default_damping() -> f64 {
    0.85
}

/// serde default for [`Method::MineRiskPropagation::tolerance`].
#[cfg(feature = "mining")]
fn default_risk_tolerance() -> f64 {
    1e-9
}

/// serde default for [`Method::MineCommunity::resolution`].
#[cfg(feature = "mining")]
fn default_resolution() -> f64 {
    1.0
}

/// serde default for [`Method::MineCommunity::weighted`].
#[cfg(feature = "mining")]
fn default_true() -> bool {
    true
}

/// A graph-derived text source for `MineText` (CONCEPT:EG-KG.mining.tfidf —
/// Phase 4). Each node carrying `node_label` contributes one document: its
/// `field` string property, tokenized (lowercase, alnum-run split).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSource {
    /// The node label whose instances each contribute one document.
    pub node_label: String,
    /// The string property to tokenize into the document.
    pub field: String,
    /// Cap the number of nodes scanned (0 = uncapped).
    #[serde(default)]
    pub limit: usize,
}

/// serde default topic count for `lda`/`nmf`.
#[cfg(feature = "mining")]
fn default_topic_k() -> usize {
    3
}

/// serde default LDA symmetric doc-topic prior.
#[cfg(feature = "mining")]
fn default_lda_alpha() -> f64 {
    0.1
}

/// serde default LDA symmetric topic-term prior.
#[cfg(feature = "mining")]
fn default_lda_beta() -> f64 {
    0.01
}

/// serde default Gibbs sweeps / NMF iterations.
#[cfg(feature = "mining")]
fn default_text_iterations() -> usize {
    200
}

/// serde default terms kept per document/topic row.
#[cfg(feature = "mining")]
fn default_top_n() -> usize {
    10
}

/// serde default for [`Method::MineForecast::horizon`].
#[cfg(feature = "mining")]
fn default_horizon() -> usize {
    10
}

/// serde default ARIMA autoregressive order.
#[cfg(feature = "mining")]
fn default_arima_p() -> usize {
    1
}

/// serde default ARIMA differencing order.
#[cfg(feature = "mining")]
fn default_arima_d() -> usize {
    1
}

/// serde default Holt-Winters level smoothing.
#[cfg(feature = "mining")]
fn default_hw_alpha() -> f64 {
    0.3
}

/// serde default Holt-Winters trend smoothing.
#[cfg(feature = "mining")]
fn default_hw_beta() -> f64 {
    0.1
}

/// serde default Holt-Winters seasonal smoothing.
#[cfg(feature = "mining")]
fn default_hw_gamma() -> f64 {
    0.1
}

/// serde default two-sided forecast confidence level.
#[cfg(feature = "mining")]
fn default_confidence() -> f64 {
    0.95
}

/// A graph-derived transaction source for `MineAssociate` (CONCEPT:EG-KG.mining.graph-derived-transactions).
///
/// Each node carrying `node_label` becomes one "basket owner"; the basket is the set
/// of `item_field` values gathered from its neighbors (following edges in
/// `direction`), optionally filtered to a `relation`. This turns node neighborhoods
/// into transactions so mining runs directly over resident graph data — the
/// cross-modal hook (e.g. "for each :Capability, the set of concepts it touches" =
/// one transaction ⇒ concept-co-occurrence rules).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionSource {
    /// The node label whose instances each become one transaction/basket owner.
    pub node_label: String,
    /// Edge direction to gather neighbors: `out` (successors, default), `in`
    /// (predecessors), or `any` (both).
    #[serde(default = "default_mine_direction")]
    pub direction: String,
    /// Which value of each neighbor becomes an item: `label` (the neighbor's
    /// type/label, default) or `prop:<key>` (a neighbor property value). When
    /// `None`, the neighbor's node id is used verbatim.
    #[serde(default)]
    pub item_field: Option<String>,
    /// Optional edge-relation filter: only follow edges whose `relationship`
    /// property equals this. `None` ⇒ all edges.
    #[serde(default)]
    pub relation: Option<String>,
    /// Cap the number of basket owners scanned (0 = uncapped).
    #[serde(default)]
    pub limit: usize,
}

/// serde default for [`TransactionSource::direction`].
#[cfg(feature = "mining")]
fn default_mine_direction() -> String {
    "out".to_string()
}

/// A graph-derived sequence source for `MineSequence` (CONCEPT:EG-KG.mining.prefixspan
/// — Phase 4). Each node carrying `node_label` becomes one ordered sequence: the
/// list of `item_field` values gathered from its neighbors in `direction`,
/// preserving the RESIDENT EDGE INSERTION ORDER (the natural "ordered edge
/// sequence per node" — edges accumulate in the order they were added, so this
/// is compute-near-data over the bitemporal write history without a separate
/// tsdb/event-log dependency), optionally filtered to a `relation`.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceSource {
    /// The node label whose instances each become one ordered sequence.
    pub node_label: String,
    /// Edge direction to gather neighbors: `out` (successors, default — the
    /// natural "what happened after" order), `in` (predecessors), or `any`.
    #[serde(default = "default_mine_direction")]
    pub direction: String,
    /// Which value of each neighbor becomes an item: `label` (the neighbor's
    /// type/label, default) or `prop:<key>` (a neighbor property value). When
    /// `None`, the neighbor's node id is used verbatim.
    #[serde(default)]
    pub item_field: Option<String>,
    /// Optional edge-relation filter: only follow edges whose `relationship`
    /// property equals this. `None` ⇒ all edges.
    #[serde(default)]
    pub relation: Option<String>,
    /// Cap the number of sequence owners scanned (0 = uncapped).
    #[serde(default)]
    pub limit: usize,
}

/// A graph-derived VECTOR source for `MineCluster` / `MineAnomaly`
/// (CONCEPT:EG-KG.mining.node-embedding-source). Each node carrying `node_label`
/// contributes ONE feature row = its stored embedding vector, and its node id is
/// carried alongside so write-back can link the mined `:Cluster` / `:Anomaly` node
/// back to it. This is the cross-modal hook — "cluster / anomaly-detect the
/// embeddings of these nodes" runs compute-near-data over resident vectors.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSource {
    /// The node label whose instances each contribute one embedding row.
    pub node_label: String,
    /// Cap the number of nodes scanned (0 = uncapped).
    #[serde(default)]
    pub limit: usize,
}

/// Which clustering engine `MineCluster` runs (CONCEPT:EG-KG.mining.dbscan-density).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClusterAlgorithm {
    #[default]
    Dbscan,
    Hierarchical,
    Gmm,
    Kmedoids,
}

/// Hierarchical agglomerative linkage criterion (CONCEPT:EG-KG.mining.hierarchical-linkage).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Linkage {
    Single,
    Complete,
    #[default]
    Average,
}

/// Which detector `MineAnomaly` runs (CONCEPT:EG-KG.mining.isolation-forest).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AnomalyAlgorithm {
    #[default]
    Zscore,
    Isoforest,
    Lof,
    Ocsvm,
}

/// One-Class SVM kernel (CONCEPT:EG-KG.mining.oneclass-svm).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SvmKernel {
    #[default]
    Rbf,
    Linear,
}

/// serde default for DBSCAN `eps`.
#[cfg(feature = "mining")]
fn default_eps() -> f64 {
    0.5
}

/// serde default for DBSCAN `min_pts`.
#[cfg(feature = "mining")]
fn default_min_pts() -> usize {
    5
}

/// serde default cluster count `k`.
#[cfg(feature = "mining")]
fn default_k() -> usize {
    3
}

/// serde default EM / PAM iteration cap.
#[cfg(feature = "mining")]
fn default_max_iter() -> usize {
    100
}

/// serde default LOF neighbor count.
#[cfg(feature = "mining")]
fn default_lof_k() -> usize {
    20
}

/// serde default Isolation Forest tree count.
#[cfg(feature = "mining")]
fn default_n_trees() -> usize {
    100
}

/// serde default Isolation Forest subsample size.
#[cfg(feature = "mining")]
fn default_sample_size() -> usize {
    256
}

/// serde default One-Class SVM ν.
#[cfg(feature = "mining")]
fn default_nu() -> f64 {
    0.1
}

/// Which classifier `MineClassifyFit` fits (CONCEPT:EG-KG.mining.naive-bayes).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClassifyAlgorithm {
    /// Gaussian Naive Bayes (default) — continuous features.
    #[default]
    Gaussiannb,
    /// Multinomial Naive Bayes — count features.
    Multinomialnb,
    /// k-nearest-neighbor majority vote.
    Knn,
    /// One-vs-rest logistic regression.
    Logistic,
    /// One-vs-rest linear SVM (SVC).
    Svc,
}

/// Which reduction engine `MineReduce` runs (CONCEPT:EG-KG.mining.truncated-svd).
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReduceAlgorithm {
    /// Truncated SVD (default) — unsupervised linear projection.
    #[default]
    Svd,
    /// Fisher LDA — supervised (needs labels).
    Lda,
    /// UMAP layout.
    Umap,
    /// t-SNE embedding.
    Tsne,
}

/// serde default k-NN neighbor count.
#[cfg(feature = "mining")]
fn default_knn_k() -> usize {
    5
}

/// serde default Multinomial NB Laplace smoothing.
#[cfg(feature = "mining")]
fn default_nb_alpha() -> f64 {
    1.0
}

/// serde default logistic / SVC learning rate.
#[cfg(feature = "mining")]
fn default_class_lr() -> f64 {
    0.1
}

/// serde default logistic / SVC epochs.
#[cfg(feature = "mining")]
fn default_class_epochs() -> usize {
    300
}

/// serde default linear-SVC inverse-regularization C.
#[cfg(feature = "mining")]
fn default_svc_c() -> f64 {
    1.0
}

/// serde default reduced dimensionality.
#[cfg(feature = "mining")]
fn default_n_components() -> usize {
    2
}

/// serde default UMAP neighbor count.
#[cfg(feature = "mining")]
fn default_umap_neighbors() -> usize {
    15
}

/// serde default UMAP minimum embedded distance.
#[cfg(feature = "mining")]
fn default_umap_min_dist() -> f64 {
    0.1
}

/// serde default t-SNE perplexity.
#[cfg(feature = "mining")]
fn default_tsne_perplexity() -> f64 {
    30.0
}

/// serde default UMAP / t-SNE epochs.
#[cfg(feature = "mining")]
fn default_reduce_epochs() -> usize {
    300
}

/// serde default t-SNE learning rate.
#[cfg(feature = "mining")]
fn default_tsne_lr() -> f64 {
    100.0
}

// ── Graph-learning wire types (CONCEPT:EG-KG.graphlearn.link-predictor) ──

/// A graph-derived subgraph source for `GraphLearn*` (CONCEPT:EG-KG.graphlearn.link-predictor).
///
/// Every node carrying `node_label` becomes a vertex; edges among them (following
/// `direction`, optionally filtered to `relation`) are the observed positive links
/// the KAN learns from. Isolated label instances are kept as candidate endpoints.
#[cfg(feature = "graphlearn")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSource {
    /// The node label whose instances form the learning subgraph's vertices.
    pub node_label: String,
    /// Edge direction to gather links: `any` (both, default — links are undirected
    /// for prediction), `out` (successors), or `in` (predecessors).
    #[serde(default = "default_gl_direction")]
    pub direction: String,
    /// Optional edge-relation filter: only use edges whose `relationship` equals
    /// this. `None` ⇒ all edges among the label's nodes.
    #[serde(default)]
    pub relation: Option<String>,
    /// Cap the number of label nodes scanned (0 = uncapped).
    #[serde(default)]
    pub limit: usize,
}

/// serde default for [`GraphSource::direction`].
#[cfg(feature = "graphlearn")]
fn default_gl_direction() -> String {
    "any".to_string()
}

/// Training + architecture knobs for `GraphLearnFit` (CONCEPT:EG-KG.graphlearn.link-predictor).
#[cfg(feature = "graphlearn")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphLearnParams {
    /// Polynomial basis for the edge functions: `chebyshev` (default) or `jacobi`.
    #[serde(default = "default_gl_basis")]
    pub basis: String,
    /// Polynomial degree per edge function.
    #[serde(default = "default_gl_degree")]
    pub degree: usize,
    /// Hidden width; `0` ⇒ a single interpretable layer (one edge fn per feature).
    #[serde(default)]
    pub hidden: usize,
    /// Adam training epochs.
    #[serde(default = "default_gl_epochs")]
    pub epochs: usize,
    /// Adam learning rate.
    #[serde(default = "default_gl_lr")]
    pub lr: f64,
    /// Negatives (sampled non-edges) per positive edge.
    #[serde(default = "default_gl_neg_ratio")]
    pub neg_ratio: f64,
    /// Seed for negative sampling + parameter init (deterministic).
    #[serde(default = "default_gl_seed")]
    pub seed: u64,
    /// 1-hop neighbour-aggregation self-retention for the node-feature channel.
    #[serde(default = "default_gl_alpha")]
    pub alpha: f64,
}

#[cfg(feature = "graphlearn")]
impl Default for GraphLearnParams {
    fn default() -> Self {
        Self {
            basis: default_gl_basis(),
            degree: default_gl_degree(),
            hidden: 0,
            epochs: default_gl_epochs(),
            lr: default_gl_lr(),
            neg_ratio: default_gl_neg_ratio(),
            seed: default_gl_seed(),
            alpha: default_gl_alpha(),
        }
    }
}

#[cfg(feature = "graphlearn")]
fn default_gl_basis() -> String {
    "chebyshev".to_string()
}
#[cfg(feature = "graphlearn")]
fn default_gl_degree() -> usize {
    4
}
#[cfg(feature = "graphlearn")]
fn default_gl_epochs() -> usize {
    200
}
#[cfg(feature = "graphlearn")]
fn default_gl_lr() -> f64 {
    0.05
}
#[cfg(feature = "graphlearn")]
fn default_gl_neg_ratio() -> f64 {
    1.0
}
#[cfg(feature = "graphlearn")]
fn default_gl_seed() -> u64 {
    42
}
#[cfg(feature = "graphlearn")]
fn default_gl_alpha() -> f64 {
    0.5
}
#[cfg(feature = "graphlearn")]
fn default_gl_top_k() -> usize {
    50
}

/// The distributed graph algorithm a `DistributedCompute` / matview runs across
/// shards (CONCEPT:EG-KG.storage.feature). Each is a vertex-centric (Pregel/GAS) computation the
/// cross-shard superstep coordinator drives; the single-shard fast path stays the
/// always-on `PageRank`/`ConnectedComponents` ops.
#[cfg(feature = "compute-dist")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DistAlgo {
    /// PageRank — `damping`·rank-mass propagation for `iterations` supersteps. The
    /// cross-shard result matches the single-graph result on the UNION graph.
    PageRank { damping: f64, iterations: usize },
    /// Weakly-connected components — every vertex labeled with its component's
    /// representative, via label-propagation supersteps to a fixpoint.
    ConnectedComponents,
    /// BFS levels from `source` — every reachable vertex labeled with its hop distance.
    Bfs { source: String },
}

/// Outcome of walking a graph's tamper-evident hash-chained audit log
/// (CONCEPT:EG-KG.sharding.row-level-security, `Method::AuditVerify`). `ok` is true when every entry's stored
/// hash matches the recomputed chain hash AND the sequence is contiguous from 0;
/// `first_broken_seq` carries the position of the first detected break.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditReport {
    pub graph: String,
    pub ok: bool,
    pub entries: u64,
    pub first_broken_seq: Option<u64>,
    pub detail: String,
}

/// Which side of its parent a Merkle audit-path sibling sits on (provenance
/// anchoring, CONCEPT:EG-KG.sharding.row-level-security). The verifier folds the running hash with each
/// step's sibling on the side named here — RFC 6962 §2.1.1 Merkle audit path
/// semantics.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MerkleSide {
    Left,
    Right,
}

/// One Merkle audit-path step, wire-encoded: a hex-encoded sibling subtree hash
/// plus which side it sits on. See [`MerkleInclusionReport`].
#[cfg(feature = "security")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MerkleProofStep {
    pub sibling_sha256: String,
    pub side: MerkleSide,
}

/// Result of `Method::AuditProveInclusion` (provenance anchoring, CONCEPT:EG-KG.sharding.row-level-security): a
/// Merkle inclusion proof for one node against a prior provenance anchor,
/// ALREADY VERIFIED server-side. `verified` is the tamper signal: `false`
/// whenever the node's CURRENT durable content does not re-hash to the leaf
/// folded into `anchored_root_sha256` at anchor time — including when the node
/// was altered by an otherwise-ordinary later write, not just raw byte-level
/// tampering. `included == false` only means `node_id` was never part of that
/// anchor's window (a different anchor, a non-provenance node, or a node created
/// after this anchor ran) — not itself evidence of tampering.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MerkleInclusionReport {
    pub graph: String,
    pub node_id: String,
    pub anchor_seq: u64,
    pub window_size: usize,
    pub included: bool,
    pub verified: bool,
    pub anchored_root_sha256: String,
    pub computed_root_sha256: String,
    pub proof: Vec<MerkleProofStep>,
    pub detail: String,
}

/// Materialized result of a `Method::Vf2SubgraphMatch` run (CONCEPT:EG-KG.mining.gspan-frequent-subgraph). Returned via
/// `ResultPayload::raw`. VF2 subgraph isomorphism is NP-hard with no bound
/// otherwise, so the backtracking search stops early once it hits `max_results`
/// collected matches or `max_steps` candidate-pair attempts (whichever first);
/// `truncated` is `true` when it stopped for either reason — the caller is seeing
/// a PARTIAL result, not proof no further match exists, and must raise
/// `max_results`/`max_steps` explicitly on the request to see more. The matcher
/// and its budget live in eg-core; this is the wire projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vf2MatchResult {
    pub matches: Vec<std::collections::HashMap<String, String>>,
    pub truncated: bool,
}

/// Materialized result of a `Method::Sql` query (CONCEPT:EG-KG.query.read-only-sql-query). Returned via
/// `ResultPayload::raw` — `rows[i]` is a MessagePack-encoded `Vec<serde_json::Value>`
/// aligned to `columns`, so the Python client double-unpacks the top-level `Raw`
/// blob then unpacks each row blob into a list of cells. Lives in eg-types (the
/// wire-DTO crate) so the protocol can embed it; the query algorithm stays in
/// eg-query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<u8>>,
}

/// Materialized result of a `Method::Sparql` SELECT (CONCEPT:EG-KG.ontology.concept-11). Returned via
/// `ResultPayload::raw`. `vars` is the projected variable order; each row is aligned
/// to `vars` with `None` for an unbound (OPTIONAL) variable. Lives in eg-types (the
/// wire-DTO crate) so the protocol can embed it; the evaluator lives in eg-rdf.
#[cfg(feature = "sparql")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparqlResult {
    pub vars: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// Materialized result of a `Method::OwlReason` run (CONCEPT:EG-KG.ontology.incremental-materialization). Returned via
/// `ResultPayload::raw`. The reasoner lives in eg-rdf; this is the wire projection.
#[cfg(feature = "owl")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwlReasonResult {
    /// Derived named-class subsumptions `(sub, sup)` (the reflexive/asserted ones are
    /// included; the closure is the full classification hierarchy).
    pub subclasses: Vec<(String, String)>,
    /// Per-subsumption confidence in `[0,1]` (CONCEPT:EG-KG.ontology.concept-13), ALIGNED index-for-index
    /// with `subclasses`. `1.0` for a hard/asserted subsumption; the propagated
    /// `axiom_conf × ∏ premise_conf` (max over alternative derivations) for an uncertain
    /// one. A fully-hard ontology yields all `1.0`.
    pub subclass_conf: Vec<f64>,
    /// Inferred instance memberships `(instance, class)` — every individual mapped to
    /// every class it (provably) belongs to, INCLUDING classes reached only through
    /// existential restrictions / role chains. When `target_class` was set, restricted
    /// to that class's members. Only memberships with confidence `≥ min_confidence`.
    pub instances: Vec<(String, String)>,
    /// Per-membership confidence in `[0,1]` (CONCEPT:EG-KG.ontology.concept-13), ALIGNED index-for-index
    /// with `instances`: the type fact's confidence (per-node confidence × Ebbinghaus
    /// decay) × the subsumption confidence — so an old/decayed or weakly-asserted fact
    /// yields a lower-confidence membership.
    pub instance_conf: Vec<f64>,
    /// `true` iff the ontology is consistent (no class forced to subsume `owl:Nothing`).
    pub consistent: bool,
    /// Named classes derived to be unsatisfiable (`A ⊑ ⊥`); empty when consistent.
    pub unsatisfiable: Vec<String>,
}

/// One node of a reconstructed OWL proof tree (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation) — the wire
/// projection of `eg_rdf::owl::ProofNode`. `rule == "asserted"` marks a LEAF (a
/// reflexive seed or a base fact with no recorded justification — the proof bottoms
/// out there); any other `rule` is a completion rule name (`"CR-sub"`, `"CR-some⁺"`,
/// `"CR-instance"`, …) that consumed `premises` (each itself a full sub-proof) plus the
/// cited `axioms`. Recursive — `premises` nests to the tree's actual depth (never
/// flattened), so a client walks it exactly like the reasoner derived it.
#[cfg(feature = "owl")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofNodeWire {
    pub sub: String,
    pub sup: String,
    pub rule: String,
    pub axioms: Vec<String>,
    pub confidence: f64,
    pub premises: Vec<ProofNodeWire>,
}

/// Materialized result of a `Method::OwlExplain` run (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation). Returned
/// via `ResultPayload::raw`. `tree` is `None` when `sub ⊑ sup` does not hold (nothing
/// to explain) — `found` mirrors that as a convenience boolean for callers that only
/// json-decode the top level.
#[cfg(feature = "owl")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwlExplainResult {
    /// Whether `sub ⊑ sup` holds under the classification (`tree.is_some()`).
    pub found: bool,
    /// The reconstructed proof tree, or `None` when `sub ⊑ sup` does not hold.
    pub tree: Option<ProofNodeWire>,
    /// `true` iff the classified ontology is consistent.
    pub consistent: bool,
    /// Named classes derived to be unsatisfiable; empty when consistent.
    pub unsatisfiable: Vec<String>,
}

// ── EXPLAIN surface wire results (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────
// Diagnostics-only projections: `op`/`rule` render the underlying `eg_plan`/`eg_epistemic`
// type via its `Debug` impl (a typed wire mirror of the WHOLE cross-modal `Op` algebra —
// or the epistemic `JustRule` enum — would duplicate it for a read-only surface with no
// other consumer; the facade, which depends on `eg_plan`, builds these strings, so
// `eg_types` itself stays free of an `eg_plan`/`eg_epistemic` dependency, matching Rule R1).

/// One node of an `EXPLAIN PLAN` dump — the wire projection of an `eg_plan::dag::PlanNode`.
#[cfg(feature = "query")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainNodeWire {
    pub id: usize,
    /// `Debug`-rendered `eg_plan::Op`.
    pub op: String,
    pub inputs: Vec<usize>,
}

/// Materialized result of a `Method::ExplainPlan` run. Returned via `ResultPayload::raw`.
#[cfg(feature = "query")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlanResult {
    /// The plan as a `PlanDag` BEFORE the DAG-aware cost optimizer.
    pub before: Vec<ExplainNodeWire>,
    /// The plan as a `PlanDag` AFTER `eg_plan::optimizer::optimize_dag`.
    pub after: Vec<ExplainNodeWire>,
    /// The active optimizer rule set, in application order (`eg_plan::cost_opt_rule_names()`).
    pub applied_rules: Vec<String>,
}

/// DAG-safe wire mirror of `eg_modality::ResourceId` for an evidence subject.
#[cfg(feature = "query")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum EvidenceResourceWire {
    Artifact(String),
    Occurrence(String),
    Rendition(String),
    Segment(String),
    Feature(String),
    EvidenceLocus(String),
}

/// DAG-safe wire mirror of `eg_modality::EvidenceAddress`.
#[cfg(feature = "query")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvidenceAddressWire {
    CharacterRange {
        start: u64,
        end: u64,
    },
    TableCellRange {
        row_start: u64,
        row_end: u64,
        col_start: u64,
        col_end: u64,
    },
    ImageRegion {
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    PageRegion {
        page: u32,
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    AudioRange {
        start_ms: u64,
        end_ms: u64,
    },
    VideoTimeRange {
        start_ms: u64,
        end_ms: u64,
    },
    FrameRange {
        start_frame: u64,
        end_frame: u64,
    },
    MetricWindow {
        start_ms: u64,
        end_ms: u64,
    },
    Point {
        x: f64,
        y: f64,
    },
    RowVersion {
        row_ref: String,
        version: u64,
    },
    CodeSymbol {
        revision_ref: String,
        symbol_ref: String,
        start_line: u32,
        end_line: u32,
    },
    TraceSpan {
        trace_ref: String,
        span_ref: String,
    },
}

/// The sole located-evidence wire representation. Its custom deserializer rejects
/// unsafe references and malformed coordinates before a request reaches a handler.
#[cfg(feature = "query")]
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceLocusWire {
    pub id: String,
    pub subject: EvidenceResourceWire,
    pub address: EvidenceAddressWire,
    pub policy_ref: String,
    pub derivation_ref: String,
}

#[cfg(feature = "query")]
impl EvidenceLocusWire {
    fn valid_opaque(value: &str, namespace: Option<&str>) -> bool {
        let parts: Vec<&str> = value.split(':').collect();
        (3..=6).contains(&parts.len())
            && parts.first() == Some(&"eg")
            && namespace.is_none_or(|expected| parts.get(1) == Some(&expected))
            && parts[1..parts.len() - 1].iter().all(|part| {
                !part.is_empty()
                    && part.len() <= 32
                    && part.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'_' | b'-')
                    })
            })
            && parts.last().is_some_and(|token| {
                (16..=128).contains(&token.len())
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            })
    }

    fn valid(&self) -> bool {
        let subject_valid = match &self.subject {
            EvidenceResourceWire::Artifact(value) => Self::valid_opaque(value, Some("artifact")),
            EvidenceResourceWire::Occurrence(value) => {
                Self::valid_opaque(value, Some("occurrence"))
            }
            EvidenceResourceWire::Rendition(value) => Self::valid_opaque(value, Some("rendition")),
            EvidenceResourceWire::Segment(value) => Self::valid_opaque(value, Some("segment")),
            EvidenceResourceWire::Feature(value) => Self::valid_opaque(value, Some("feature")),
            EvidenceResourceWire::EvidenceLocus(value) => Self::valid_opaque(value, Some("locus")),
        };
        let address_valid = match &self.address {
            EvidenceAddressWire::CharacterRange { start, end }
            | EvidenceAddressWire::AudioRange {
                start_ms: start,
                end_ms: end,
            }
            | EvidenceAddressWire::VideoTimeRange {
                start_ms: start,
                end_ms: end,
            }
            | EvidenceAddressWire::MetricWindow {
                start_ms: start,
                end_ms: end,
            } => end > start,
            EvidenceAddressWire::FrameRange {
                start_frame,
                end_frame,
            } => end_frame >= start_frame,
            EvidenceAddressWire::TableCellRange {
                row_start,
                row_end,
                col_start,
                col_end,
            } => row_end >= row_start && col_end >= col_start,
            EvidenceAddressWire::ImageRegion {
                x,
                y,
                width,
                height,
            }
            | EvidenceAddressWire::PageRegion {
                x,
                y,
                width,
                height,
                ..
            } => {
                x.is_finite()
                    && y.is_finite()
                    && width.is_finite()
                    && height.is_finite()
                    && *width > 0.0
                    && *height > 0.0
            }
            EvidenceAddressWire::Point { x, y } => x.is_finite() && y.is_finite(),
            EvidenceAddressWire::RowVersion { row_ref, .. } => Self::valid_opaque(row_ref, None),
            EvidenceAddressWire::CodeSymbol {
                revision_ref,
                symbol_ref,
                start_line,
                end_line,
            } => {
                Self::valid_opaque(revision_ref, None)
                    && Self::valid_opaque(symbol_ref, None)
                    && end_line >= start_line
            }
            EvidenceAddressWire::TraceSpan {
                trace_ref,
                span_ref,
            } => Self::valid_opaque(trace_ref, None) && Self::valid_opaque(span_ref, None),
        };
        Self::valid_opaque(&self.id, Some("locus"))
            && subject_valid
            && address_valid
            && Self::valid_opaque(&self.policy_ref, None)
            && Self::valid_opaque(&self.derivation_ref, Some("derivation"))
    }
}

#[cfg(feature = "query")]
impl<'de> Deserialize<'de> for EvidenceLocusWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Unchecked {
            id: String,
            subject: EvidenceResourceWire,
            address: EvidenceAddressWire,
            policy_ref: String,
            derivation_ref: String,
        }

        let value = Unchecked::deserialize(deserializer)?;
        let locus = Self {
            id: value.id,
            subject: value.subject,
            address: value.address,
            policy_ref: value.policy_ref,
            derivation_ref: value.derivation_ref,
        };
        if locus.valid() {
            Ok(locus)
        } else {
            Err(<D::Error as serde::de::Error>::custom(
                "invalid governed evidence locus",
            ))
        }
    }
}

/// Materialized result of a `Method::ExplainPolicy` run (CONCEPT:EG-KG.sharding.row-level-security).
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "query")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPolicyResult {
    /// Ids the caller's RLS-filtered view actually returns.
    pub visible_ids: Vec<String>,
    /// Ids present in the UNFILTERED result but absent from `visible_ids` — what the
    /// policy denied. Always empty when no RLS filtering applied (no `security` feature,
    /// or no caller/RLS on this connection).
    pub policy_denied_ids: Vec<String>,
}

/// One node of an `EXPLAIN BELIEF` justification tree — the wire projection of
/// `eg_epistemic::ProofNode`. `rule` is the `Debug`-rendered `eg_epistemic::JustRule`
/// (`"Asserted"`, `"DerivedSupport"`, `"DerivedContradiction"`, `"BayesianUpdate"`).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JustificationNodeWire {
    pub claim: String,
    pub rule: String,
    pub confidence: f64,
    pub premises: Vec<JustificationNodeWire>,
}

/// Materialized result of a `Method::ExplainBelief` run. Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainBeliefResult {
    pub root: JustificationNodeWire,
}

/// Wire mirror of `eg_epistemic::redact::DisclosureLevel` (EPI-P3-4, L51) — the
/// `Method::ExplainBelief::disclosure_level` request field AND the
/// `ExplainBeliefRedactedResult::level` response field share this one type. Plain
/// serde/enum, no `eg-epistemic` dependency needed here (`eg-types` sits BELOW
/// `eg-epistemic` in the crate DAG).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DisclosureLevelWire {
    Full,
    Skeleton,
    ExistenceOnly,
}

/// Wire mirror of `eg_epistemic::redact::ExistenceSignal`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExistenceSignalWire {
    Supported,
    Contradicted,
    Uncertain,
}

/// Wire mirror of `eg_epistemic::redact::RedactedProofNode` — structurally parallel to
/// [`JustificationNodeWire`], except `claim` is `None` (with `redaction_label` set)
/// when the requesting actor's RLS access does not extend to that proof-tree node.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactedJustificationNodeWire {
    pub claim: Option<String>,
    pub redaction_label: Option<String>,
    pub rule: String,
    pub confidence: f64,
    pub premises: Vec<RedactedJustificationNodeWire>,
}

/// Result of a `Method::ExplainBelief` call that set `disclosure_level` (feature
/// `epistemic-redaction`) — returned via `ResultPayload::raw` INSTEAD OF
/// `ExplainBeliefResult` for that same call (the caller who set `disclosure_level`
/// already knows to decode this type). Wire mirror of
/// `eg_epistemic::redact::RedactedJustificationGraph`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainBeliefRedactedResult {
    pub level: DisclosureLevelWire,
    pub existence: ExistenceSignalWire,
    /// `Some` at `Full`/`Skeleton`; `None` at `ExistenceOnly` (no structure rendered).
    pub root: Option<RedactedJustificationNodeWire>,
}

/// Wire mirror of `eg_epistemic::AuthorityPolicy` — the confidence-weighting policy an
/// `EpistemicStatus` was computed under ("under whose authority").
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AuthorityPolicyWire {
    pub source_reliability: f64,
    pub attack_multiplier: f64,
    pub prior_strength: f64,
}

/// Wire mirror of `eg_epistemic::query::WhyNot` — flattens `WhyNotReason`'s per-variant
/// payload (`Contradicted { blockers }` / `Undecided { competing }`) into two plain
/// `Vec<String>` fields, each empty unless the matching reason tag applies (a pure-serde
/// enum-with-data mirror would work too, but this keeps the wire shape flat like every
/// other `*Wire` type here).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhyNotWire {
    pub claim: String,
    /// One of `"Unknown"`, `"InsufficientConfidence"`, `"Contradicted"`, `"Undecided"`.
    pub reason: String,
    /// Populated iff `reason == "Contradicted"`.
    pub blockers: Vec<String>,
    /// Populated iff `reason == "Undecided"`.
    pub competing: Vec<String>,
    pub confidence: f64,
}

/// Wire mirror of `eg_epistemic::query::MinimalFlipSet` — "what would invalidate it".
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MinimalFlipSetWire {
    pub claim: String,
    pub believed_now: bool,
    pub evidence_ids: Vec<String>,
    pub believed_after: bool,
}

/// Wire mirror of `eg_epistemic::query::EpistemicStatus` — the Phase-3 acceptance
/// capstone (see `Method::EpistemicStatus` docs).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpistemicStatusWire {
    pub claim: String,
    pub believed: bool,
    pub confidence: f64,
    pub uncertainty: f64,
    pub proof: JustificationNodeWire,
    pub why_not: Option<WhyNotWire>,
    pub evidence: Vec<String>,
    pub contradicting: Vec<String>,
    pub attacking: Vec<String>,
    pub authority: AuthorityPolicyWire,
    pub valid_time: Option<(Option<u64>, Option<u64>)>,
    pub tx_time: Option<(Option<u64>, Option<u64>)>,
    pub what_would_invalidate: Option<MinimalFlipSetWire>,
}

/// Materialized result of a `Method::EpistemicStatus` run. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpistemicStatusResult {
    pub status: EpistemicStatusWire,
}

/// Wire mirror of `eg_epistemic::query::ChangedBelief` — one entry of a
/// `Method::WhatChanged` result.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedBeliefWire {
    pub id: String,
    pub believed_before: bool,
    pub believed_after: bool,
    pub confidence_before: f64,
    pub confidence_after: f64,
    pub evidence_added: Vec<String>,
    pub evidence_removed: Vec<String>,
    pub reason: String,
}

/// Materialized result of a `Method::WhatChanged` run. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhatChangedResult {
    pub changed: Vec<ChangedBeliefWire>,
}

/// Materialized result of a fenced `Method::RecomputeMaterialization` writeback.
/// All identifiers are domain-separated opaque projection references.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecomputeMaterializationResult {
    pub id: String,
    pub depends_on: Vec<String>,
    pub generating_activity: Option<String>,
    pub status: String,
    pub source_graph_version: u64,
    pub fence_epoch: u64,
    /// `true` when the authoritative recompute intent is committed but its local
    /// durable projection image has not yet been acknowledged by the outbox worker.
    pub projection_pending: bool,
}

/// Materialized result of a `Method::MaterializationStatus` run (Seam 3). `status`
/// is one of `"Fresh"`/`"Stale"`/`"Retracted"`, or `None` when `id` was never
/// registered (or a build without `epistemic-tms` never populates the index).
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationStatusResult {
    pub status: Option<String>,
    pub source_graph_version: u64,
}

/// Materialized result of a `Method::StaleMaterializations` run (Seam 3 follow-up).
/// `ids` is every opaque materialization reference currently `Stale` in the
/// durable per-graph projection, sorted by its persisted `BTreeSet`. Empty means
/// nothing is stale; missing or corrupt projection authority is an error.
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleMaterializationsResult {
    pub ids: Vec<String>,
    pub source_graph_version: u64,
}

/// Materialized result of a `Method::ResolveConflict` run (EPI-P3-7). `semantics`
/// echoes the request. Every id in the request's `node_ids` appears in EXACTLY ONE
/// of `surviving`/`defeated`/`undecided`:
///
/// * `grounded`: `surviving` = the unique grounded extension's members (IN);
///   `defeated` = attacked by an IN argument (OUT); `undecided` = neither (caught in
///   an unresolved/paraconsistent conflict, e.g. an odd attack cycle).
/// * `preferred`/`stable`: `surviving` = in EVERY computed extension (unanimous
///   across every admissible "side"); `defeated` = in NO extension (never
///   credulously acceptable); `undecided` = in SOME but not all (contested), or
///   every requested id when NO extension exists at all (a legitimate `stable`
///   result, or the crate's own NP-hardness cap firing on a large graph — see
///   `eg_epistemic::tms` module docs; never a fabricated verdict either way).
///
/// `extension_sets` is the raw extension(s) the verdict was computed from, over the
/// WHOLE graph (not filtered to `node_ids`): exactly one entry for `grounded`,
/// zero-or-more for `preferred`/`stable`. Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveConflictResult {
    pub semantics: String,
    pub surviving: Vec<String>,
    pub defeated: Vec<String>,
    pub undecided: Vec<String>,
    pub extension_sets: Vec<Vec<String>>,
}

// ── X-1 multimodal-evidence citation wiring (CONCEPT:EG-X1, facade feature
// `evidence-graph`) ─────────────────────────────────────────────────────────────

/// Wire mirror of `eg_epistemic::EvidenceCitation` — one node bearing on a claim
/// (support/contradiction/attack), together with its complete governed locus.
/// `kind` is the `Debug`-rendered `eg_epistemic::EdgeKind`
/// (`"Supports"`/`"Contradicts"`/`"Attacks"`), the SAME flat-string convention
/// `JustificationNodeWire::rule` uses for its `JustRule`. `locus` reuses
/// [`EvidenceLocusWire`] (the governed evidence-locus wire mirror —
/// `epistemic` implies `query`, so it is always nameable here).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceCitationWire {
    pub evidence_id: String,
    /// One of `"Supports"`, `"Contradicts"`, `"Attacks"`.
    pub kind: String,
    pub locus: EvidenceLocusWire,
    /// SURPASS gap-closure ("unify the two evidence resolvers"): the REAL content
    /// this citation's `locus` resolves to, via `eg_alignment::EvidenceResolver`
    /// (`src/server/blob/cas_resolver.rs`'s `CasEvidenceResolver`, the SAME
    /// engine-backed resolver that previously had zero served-RPC call sites — only
    /// its own unit tests exercised it). `None` when the build lacks the `alignment`
    /// feature, no blob store is configured, or the resolver had nothing for the
    /// locus subject (e.g. a dangling `blob_ref`) — degrades to the
    /// pre-existing locus-only behavior, never a fabricated resolution.
    #[serde(default)]
    pub resolved: Option<ResolvedArtifactWire>,
}

/// Wire mirror of `eg_alignment::ResolvedArtifact` (SURPASS gap-closure: "unify the
/// two evidence resolvers", "real crop/slice codecs"). `kind` is `"text"` (a real
/// excerpt — currently `CharacterRange` by character range, `CodeSymbol` by line
/// range) or `"blob"` (every other locus kind: the real CAS digest is named, but no
/// in-tree codec exists to crop/slice pixels/audio/video samples out of it yet — see
/// `CasEvidenceResolver`'s module docs for exactly which kinds get which treatment).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedArtifactWire {
    /// `"text"` or `"blob"`.
    pub kind: String,
    pub subject_ref: String,
    /// The resolved excerpt, when `kind == "text"`.
    pub excerpt: Option<String>,
    /// The real CAS digest, when `kind == "blob"`.
    pub blob_ref: Option<String>,
    /// A human-readable note on what the `blob` reference represents (e.g. "no
    /// in-tree codec to crop pixels with"), when `kind == "blob"`.
    pub note: Option<String>,
}

/// Materialized result of a `Method::ExplainEvidence` run. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainEvidenceResult {
    pub citations: Vec<EvidenceCitationWire>,
}

// ── EPI-P3-3 causal reasoning + provenance ranking wiring (facade feature
// `epistemic-causal`) ───────────────────────────────────────────────────────────

/// Wire mirror of `eg_epistemic::StructuralEquation` (EPI-P3-3), keyed by the
/// variable `id` it defines — one entry of `Method::CausalEstimate::variables`.
/// `parents` MUST name only ids that appear EARLIER in that same list (parents
/// before children), mirroring `eg_epistemic::CausalGraph::add_variable`'s own
/// topological-order invariant; the handler surfaces a violation as an explicit
/// error, never a panic.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructuralEquationWire {
    pub id: String,
    /// `(parent id, weight)` pairs.
    pub parents: Vec<(String, f64)>,
    pub bias: f64,
    /// Variance of this variable's own exogenous noise term (`0.0` = deterministic
    /// given its parents).
    pub noise_var: f64,
}

/// Wire mirror of `eg_epistemic::CausalEstimate` (EPI-P3-3) — a calibrated mean/
/// variance/credible-interval result of one causal query for one variable.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CausalEstimateWire {
    pub mean: f64,
    pub variance: f64,
    pub interval: (f64, f64),
    pub level: f64,
}

/// Materialized result of a `Method::CausalEstimate` run: one estimate per
/// variable, in the SAME order as the request's `variables` list. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalEstimateResult {
    pub estimates: Vec<(String, CausalEstimateWire)>,
}

/// Which of `eg_epistemic::CausalGraph`'s two non-counterfactual queries
/// `Method::CausalEstimate::do_values` feeds (EPI-P3-6).
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CausalQueryModeWire {
    Intervene,
    Observe,
}

/// Materialized result of a `Method::CausalCounterfactual` run (EPI-P3-6): one
/// POINT value per variable — not a calibrated distribution, since Pearl's
/// point-counterfactual is deterministic given the abduced exogenous noise — in
/// the SAME order as the request's `variables` list. Returned via
/// `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalCounterfactualResult {
    pub values: Vec<(String, f64)>,
}

/// Wire mirror of `eg_epistemic::Calibration` (EPI-P3-3) — the calibrated interval
/// backing a `RetrievalCandidateWire`'s evidence-quality score, when the candidate
/// has one.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CalibrationWire {
    pub interval: (f64, f64),
    pub level: f64,
    pub evidence_count: usize,
}

/// Wire mirror of `eg_epistemic::RetrievalCandidate` (EPI-P3-3) — one candidate in
/// a `Method::RankByProvenance` request, before ranking.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrievalCandidateWire {
    pub id: String,
    pub similarity: f64,
    pub source_reliability: f64,
    pub freshness: f64,
    /// `None` for a candidate with no evidence-graph backing (ranks on
    /// similarity/reliability/freshness alone — an honest "unknown", never a
    /// fabricated middling score).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calibration: Option<CalibrationWire>,
}

/// Wire mirror of `eg_epistemic::RankWeights` (EPI-P3-3) — weights combining
/// similarity with the evidence-quality/provenance signal for `RankByProvenance`.
/// Defaults to `{ similarity: 0.5, evidence_quality: 0.5 }`, the SAME
/// equal-weighting default `eg_epistemic::RankWeights::default()` uses.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RankWeightsWire {
    pub similarity: f64,
    pub evidence_quality: f64,
}

#[cfg(feature = "epistemic")]
impl Default for RankWeightsWire {
    fn default() -> Self {
        RankWeightsWire {
            similarity: 0.5,
            evidence_quality: 0.5,
        }
    }
}

/// Wire mirror of `eg_epistemic::RankedResult` (EPI-P3-3) — one ranked candidate:
/// the final blended score plus its components, kept separate for explainability.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankedResultWire {
    pub id: String,
    pub score: f64,
    pub similarity: f64,
    pub evidence_quality: f64,
}

/// Materialized result of a `Method::RankByProvenance` run, highest score first.
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankByProvenanceResult {
    pub ranked: Vec<RankedResultWire>,
}

/// Graph type for multi-tenant registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphType {
    Agent,
    Team,
    Global,
    Commons,
}

/// Channel type for dynamic communication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelType {
    /// 1:1 direct messaging between two agents.
    PeerToPeer,
    /// Many-to-many group channel.
    Group,
}

// ── Response ────────────────────────────────────────────────────────────

/// Untagged result payload for efficient serialization without JSON overhead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResultPayload {
    Bool(bool),
    Count(u64),
    Float(f64),
    String(String),
    Ids(Vec<String>),
    NodeList(Vec<(String, serde_json::Value)>),
    EdgeList(Vec<(String, String, Vec<u8>)>),
    PropertiesMsgpack(#[serde(with = "serde_bytes")] Vec<u8>),
    Rows(Vec<Vec<u8>>),
    /// A typed result serialized STRAIGHT to MessagePack (Phase C-D — compact
    /// result encoding). Skips building a `serde_json::Value` tree on the server —
    /// the dominant allocator for large algorithm results (PageRank/centrality/
    /// communities over the whole graph). On the wire it is a MessagePack `bin`
    /// (identical shape to `PropertiesMsgpack`); the Python client decodes any
    /// top-level `bytes` result with a second `unpackb`, recovering the exact same
    /// structure the `Json` path produced. Lives after `PropertiesMsgpack` so the
    /// untagged decoder is unaffected.
    Raw(#[serde(with = "serde_bytes")] Vec<u8>),
    Json(serde_json::Value),
}

impl ResultPayload {
    /// Encode a typed value straight to MessagePack as a [`ResultPayload::Raw`],
    /// bypassing the `serde_json::Value` tree (the dominant allocator for large
    /// algorithm results). The compact encoding is the ONE wire contract — clients
    /// decode a top-level `bytes` result with a second `unpackb`; there is no
    /// alternate encoding flag. (Phase C-D)
    pub fn raw<T: Serialize>(value: &T) -> Self {
        ResultPayload::Raw(rmp_serde::to_vec_named(value).unwrap_or_default())
    }
}

/// Response envelope sent back to the Python client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// Correlation ID matching the request.
    pub id: u64,
    /// Result payload on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ResultPayload>,
    /// Stable error code on failure; structured detail is carried by OperationResult.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    /// Create a successful response.
    pub fn ok(id: u64, result: ResultPayload) -> Self {
        Response {
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Create an error response.
    pub fn err(id: u64, error: impl Into<String>) -> Self {
        Response {
            id,
            result: None,
            error: Some(error.into()),
        }
    }

    /// Schema-generated placement redirect used when this node cannot serve the
    /// graph's current `(group, epoch)`.
    pub fn stale_route(
        id: u64,
        graph: &str,
        group: u64,
        epoch: u64,
        leader: Option<u64>,
        _reason: impl Into<String>,
    ) -> Self {
        use crate::epistemic_operations::{
            OperationRedirect, OperationRedirectKind, OperationResult,
            OperationResultSchemaVersion, OperationResultStatus,
        };

        let detail = OperationResult {
            schema_version: OperationResultSchemaVersion::V1,
            operation_id: format!("request:{id}"),
            status: OperationResultStatus::Redirected,
            result_kind: None,
            result_ref: None,
            error: None,
            redirect: Some(OperationRedirect {
                kind: OperationRedirectKind::Placement,
                target_ref: graph.to_string(),
                group,
                epoch,
                fencing_token: group,
                leader_ref: leader.map(|node| format!("node:{node}")),
            }),
        };
        Response {
            id,
            result: Some(ResultPayload::raw(&detail)),
            error: Some("OPERATION_REDIRECTED".to_string()),
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn node_binding_fixture_claims() -> crate::acl::RequestContextClaims {
        crate::acl::RequestContextClaims {
            principal: "p".into(),
            tenant: "t".into(),
            audience: "a".into(),
            agent_id: "p".into(),
            roles: vec![],
            scopes: vec![],
            policy_version: "v".into(),
            delegation: vec![],
            node: None,
            priority: None,
        }
    }

    #[test]
    fn envelope_v2_bytes_are_unchanged_when_node_claim_is_absent() {
        // ADR-3 / W1.9: rebuild the PRE-ADR-3 canonical encoding by hand and
        // assert `build_envelope_v2_bytes` is byte-for-byte identical when
        // `node` is `None` -- proving the change is genuinely additive for
        // clients (`clients/js`, `clients/go`, or an un-upgraded Python
        // client) that never send the claim at all.
        //
        // Deliberately mirrors `build_envelope_v2_bytes`'s own 8-argument
        // shape (the fixed pre-ADR-3 wire fields) byte-for-byte, so it
        // carries the same scoped allow that function already does above.
        #[allow(clippy::too_many_arguments)]
        fn pre_adr3_bytes(
            request_id: u64,
            graph: &str,
            method_name: &str,
            body_hash: &str,
            claims: &crate::acl::RequestContextClaims,
            timestamp: u64,
            nonce: &str,
            idempotency_key: &str,
        ) -> Vec<u8> {
            fn put(buf: &mut Vec<u8>, value: &str) {
                buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
                buf.extend_from_slice(value.as_bytes());
            }
            fn put_list(buf: &mut Vec<u8>, values: &[String]) {
                buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
                for value in values {
                    put(buf, value);
                }
            }
            let mut buf = Vec::new();
            put(&mut buf, "eg-envelope-v2");
            buf.extend_from_slice(&request_id.to_be_bytes());
            put(&mut buf, graph);
            put(&mut buf, method_name);
            put(&mut buf, body_hash);
            put(&mut buf, &claims.principal);
            put(&mut buf, &claims.tenant);
            put(&mut buf, &claims.audience);
            put(&mut buf, &claims.agent_id);
            put_list(&mut buf, &claims.roles);
            put_list(&mut buf, &claims.scopes);
            put(&mut buf, &claims.policy_version);
            put_list(&mut buf, &claims.delegation);
            buf.extend_from_slice(&timestamp.to_be_bytes());
            put(&mut buf, nonce);
            put(&mut buf, idempotency_key);
            buf
        }

        let claims = node_binding_fixture_claims();
        let current =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
        let legacy = pre_adr3_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
        assert_eq!(
            current, legacy,
            "an absent node claim must encode byte-for-byte identically to the \
             pre-ADR-3 wire format"
        );
    }

    #[test]
    fn envelope_v2_bytes_cover_the_node_claim_when_present() {
        let mut claims = node_binding_fixture_claims();
        let absent = build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
        claims.node = Some("node-a".into());
        let present_a =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
        claims.node = Some("node-b".into());
        let present_b =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
        assert_ne!(
            absent, present_a,
            "a present node claim must change the MAC-covered bytes"
        );
        assert_ne!(
            present_a, present_b,
            "different target nodes must not share an encoding"
        );
    }

    // W2.4 engine-native QoS lanes: the priority claim is additive AND its
    // encoding stays unambiguous against the node trailer (distinct tag bytes).
    #[test]
    fn envelope_v2_bytes_cover_the_priority_claim_without_node_collision() {
        let base = node_binding_fixture_claims();
        let absent = build_envelope_v2_bytes(7, "g", "Ping", "hash", &base, 111, "nonce", "idem");

        // A present priority claim changes the MAC-covered bytes ...
        let mut with_prio = base.clone();
        with_prio.priority = Some("background_ingestion".into());
        let prio_only =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &with_prio, 111, "nonce", "idem");
        assert_ne!(
            absent, prio_only,
            "a present priority claim must change the MAC-covered bytes"
        );

        // ... and distinct priority values encode distinctly.
        let mut with_prio2 = base.clone();
        with_prio2.priority = Some("interactive".into());
        let prio_only2 =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &with_prio2, 111, "nonce", "idem");
        assert_ne!(
            prio_only, prio_only2,
            "different priority classes must not share an encoding"
        );

        // The tag-distinctness guarantee: node="X" (tag 1) and priority="X"
        // (tag 2) with the SAME value must NOT produce the same MAC input — a
        // shared marker byte would let one claim be silently reinterpreted as
        // the other.
        let mut node_x = base.clone();
        node_x.node = Some("X".into());
        let node_only =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &node_x, 111, "nonce", "idem");
        let mut prio_x = base.clone();
        prio_x.priority = Some("X".into());
        let prio_x_bytes =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &prio_x, 111, "nonce", "idem");
        assert_ne!(
            node_only, prio_x_bytes,
            "node and priority trailers with the same value must not collide"
        );

        // Both trailers present: node (tag 1) precedes priority (tag 2), and the
        // combined encoding differs from either alone.
        let mut both = base.clone();
        both.node = Some("X".into());
        both.priority = Some("interactive".into());
        let both_bytes =
            build_envelope_v2_bytes(7, "g", "Ping", "hash", &both, 111, "nonce", "idem");
        assert_ne!(both_bytes, node_only);
        assert_ne!(both_bytes, prio_only2);
    }

    #[test]
    fn test_request_roundtrip_add_node() {
        let req = Request {
            id: 1,
            graph: "agent:planner".to_string(),
            auth_token: "abc123".to_string(),
            agent_id: None,
            method: Method::AddNode {
                node_id: "n1".to_string(),
                properties_msgpack: vec![
                    0x81, 0xa4, 0x74, 0x79, 0x70, 0x65, 0xa5, 0x41, 0x67, 0x65, 0x6e, 0x74,
                ],
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, 1);
        assert_eq!(parsed.graph, "agent:planner");
    }

    #[test]
    fn development_lane_protocol_matches_cross_language_golden_vector() {
        use crate::epistemic_operations::{
            DevelopmentLaneCleanupIntent, DevelopmentLaneCleanupIntentSchemaVersion,
            DevelopmentLaneIntent, DevelopmentLaneIntentHostTargetKind,
            DevelopmentLaneIntentSchemaVersion, DevelopmentLaneQueryRequest,
            DevelopmentLaneQueryRequestSchemaVersion, DevelopmentLaneQuotaUpdateRequest,
            DevelopmentLaneResultDecision,
        };

        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../protocols/epistemic-operations/v1/development-lane.golden.json"
        ))
        .expect("golden vector must be valid JSON");
        assert_eq!(
            serde_json::to_string(&DevelopmentLaneWorkItemKind::Lifecycle).unwrap(),
            "\"lane.lifecycle\""
        );
        assert_eq!(
            serde_json::to_string(&DevelopmentLaneWorkItemKind::Cleanup).unwrap(),
            "\"lane.cleanup\""
        );

        let intent = DevelopmentLaneIntent {
            schema_version: DevelopmentLaneIntentSchemaVersion::V1,
            tenant_ref: "tenant:golden".into(),
            request_id: "request:golden".into(),
            lane_id: "lane:golden".into(),
            repository_id: "repo:golden".into(),
            base_ref: "refs/heads/main".into(),
            base_sha: "0123456789abcdef0123456789abcdef01234567".into(),
            branch: "rmdd-28/golden".into(),
            host_target_kind: DevelopmentLaneIntentHostTargetKind::InventoryAlias,
            host_target_alias: Some("host:golden".into()),
            host_ref: "host-ref:golden".into(),
            resource_reservation_id: "reservation:golden".into(),
            workspace_ref: "workspace:golden".into(),
            worktree_locator: "lanes/golden".into(),
            owner_id: "agent:golden".into(),
            session_id: "session:golden".into(),
            fairness_group: "fairness:golden".into(),
            quota_policy_name: "default".into(),
            quota_policy_version: "1".into(),
            predicted_disk_bytes: 4096,
            ttl_ms: 60000,
            input_fingerprint:
                "v1:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        };
        let encoded = serde_json::to_string(&intent).unwrap();
        assert_eq!(encoded, vector["intent_json"].as_str().unwrap());

        let mut unknown: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        unknown["unexpected"] = serde_json::Value::Bool(true);
        assert!(serde_json::from_value::<DevelopmentLaneIntent>(unknown).is_err());

        let cleanup = DevelopmentLaneCleanupIntent {
            schema_version: DevelopmentLaneCleanupIntentSchemaVersion::V1,
            hold_id: "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            lane_id: "lane:golden".into(),
            expected_hold_revision: 7,
        };
        assert_eq!(
            serde_json::to_string(&cleanup).unwrap(),
            vector["lane_cleanup_extension_json"].as_str().unwrap()
        );
        let mut unknown_cleanup: serde_json::Value = serde_json::to_value(&cleanup).unwrap();
        unknown_cleanup["unexpected"] = serde_json::Value::Bool(true);
        assert!(serde_json::from_value::<DevelopmentLaneCleanupIntent>(unknown_cleanup).is_err());

        let decisions = [
            DevelopmentLaneResultDecision::Accepted,
            DevelopmentLaneResultDecision::Idempotent,
            DevelopmentLaneResultDecision::Stale,
            DevelopmentLaneResultDecision::Conflict,
            DevelopmentLaneResultDecision::InputConflict,
            DevelopmentLaneResultDecision::Quota,
            DevelopmentLaneResultDecision::Policy,
            DevelopmentLaneResultDecision::Drained,
            DevelopmentLaneResultDecision::NotFound,
            DevelopmentLaneResultDecision::WrongKind,
            DevelopmentLaneResultDecision::WrongTenant,
            DevelopmentLaneResultDecision::WrongOwner,
            DevelopmentLaneResultDecision::WrongAttempt,
            DevelopmentLaneResultDecision::WrongLeaseEpoch,
            DevelopmentLaneResultDecision::WrongFence,
            DevelopmentLaneResultDecision::Expired,
            DevelopmentLaneResultDecision::Terminal,
            DevelopmentLaneResultDecision::CleanupRequired,
            DevelopmentLaneResultDecision::Exclusivity,
            DevelopmentLaneResultDecision::Invalid,
        ];
        let actual: Vec<String> = decisions
            .iter()
            .map(|decision| serde_json::to_string(decision).unwrap())
            .collect();
        let expected: Vec<String> = vector["refusal_decisions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| format!("\"{}\"", value.as_str().unwrap()))
            .collect();
        assert_eq!(actual, expected);

        let query = DevelopmentLaneQueryRequest {
            schema_version: DevelopmentLaneQueryRequestSchemaVersion::V1,
            tenant_ref: "tenant:golden".into(),
            hold_id: "hold:golden".into(),
            now_ms: 123,
        };
        let query_json = serde_json::to_value(query).unwrap();
        assert!(serde_json::from_value::<DevelopmentLaneQueryRequest>(query_json).is_ok());

        let policy = serde_json::json!({
            "schema_version": "1",
            "policy_name": "default",
            "policy_version": "2",
            "tenant_count_limit": 1,
            "owner_count_limit": 1,
            "session_count_limit": 1,
            "workspace_count_limit": 1,
            "repository_count_limit": 1,
            "host_count_limit": 1,
            "global_count_limit": 1,
            "tenant_predicted_disk_bytes": 4096,
            "owner_predicted_disk_bytes": 4096,
            "session_predicted_disk_bytes": 4096,
            "workspace_predicted_disk_bytes": 4096,
            "repository_predicted_disk_bytes": 4096,
            "host_predicted_disk_bytes": 4096,
            "global_predicted_disk_bytes": 4096,
            "tenant_observed_disk_bytes": 4096,
            "owner_observed_disk_bytes": 4096,
            "session_observed_disk_bytes": 4096,
            "workspace_observed_disk_bytes": 4096,
            "repository_observed_disk_bytes": 4096,
            "host_observed_disk_bytes": 4096,
            "global_observed_disk_bytes": 4096,
            "tenant_retained_disk_bytes": 4096,
            "owner_retained_disk_bytes": 4096,
            "session_retained_disk_bytes": 4096,
            "workspace_retained_disk_bytes": 4096,
            "repository_retained_disk_bytes": 4096,
            "host_retained_disk_bytes": 4096,
            "global_retained_disk_bytes": 4096,
            "min_ttl_ms": 1000,
            "max_ttl_ms": 60000,
            "max_observation_staleness_ms": 1000,
            "drain_only": false
        });
        let quota_update = serde_json::json!({
            "schema_version": "1",
            "tenant_ref": "tenant:golden",
            "policy": policy,
            "expected_policy_revision": vector["quota_policy_update_expected_revision"],
            "expected_policy_version": "1",
            "idempotency_key": "idem:golden",
            "now_ms": 123
        });
        let parsed: DevelopmentLaneQuotaUpdateRequest =
            serde_json::from_value(quota_update).expect("numeric policy CAS must be required");
        assert_eq!(
            parsed.expected_policy_revision,
            vector["quota_policy_update_expected_revision"]
                .as_u64()
                .unwrap()
        );
    }

    #[test]
    fn stale_route_is_structured_and_carries_fencing_epoch() {
        let response = Response::stale_route(9, "opaque:graph", 3, 17, Some(2), "not leader");
        let detail: crate::epistemic_operations::OperationResult = match response.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected structured redirect, got {other:?}"),
        };
        let redirect = detail.redirect.unwrap();
        assert_eq!(redirect.group, 3);
        assert_eq!(redirect.epoch, 17);
        assert_eq!(redirect.fencing_token, 3);
        assert_eq!(response.error.as_deref(), Some("OPERATION_REDIRECTED"));
    }

    #[test]
    fn test_request_roundtrip_create_channel() {
        let req = Request {
            id: 42,
            graph: "__commons__".to_string(),
            auth_token: "tok".to_string(),
            agent_id: Some("agent:a".to_string()),
            method: Method::CreateChannel {
                channel_id: "channel:p2p:a:b".to_string(),
                channel_type: ChannelType::PeerToPeer,
                creator: "agent:a".to_string(),
                initial_members: vec!["agent:a".to_string(), "agent:b".to_string()],
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, 42);
        if let Method::CreateChannel { channel_type, .. } = parsed.method {
            assert_eq!(channel_type, ChannelType::PeerToPeer);
        } else {
            panic!("Wrong method variant");
        }
    }

    #[test]
    fn test_response_ok() {
        let resp = Response::ok(1, ResultPayload::Json(serde_json::json!({"count": 42})));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"count\":42"));
        assert!(!json.contains("error"));
    }

    #[test]
    fn test_response_err() {
        let resp = Response::err(2, "node not found");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("node not found"));
        assert!(!json.contains("result"));
    }

    #[test]
    fn test_all_graph_types_roundtrip() {
        for gt in [
            GraphType::Agent,
            GraphType::Team,
            GraphType::Global,
            GraphType::Commons,
        ] {
            let json = serde_json::to_string(&gt).unwrap();
            let parsed: GraphType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, gt);
        }
    }

    #[test]
    fn test_method_ping_roundtrip() {
        let method = Method::Ping;
        let json = serde_json::to_string(&method).unwrap();
        let parsed: Method = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, Method::Ping));
    }

    #[test]
    fn retired_methods_and_parameters_are_rejected() {
        for method in [
            "BatchCosineSimilarity",
            "SpectralCluster",
            "HypergraphEncodeInteraction",
            "FindSimilarPairs",
        ] {
            let value = serde_json::json!({"method": method, "params": {}});
            assert!(serde_json::from_value::<Method>(value).is_err());
        }

        let retired_parameter = serde_json::json!({
            "method": "UnifiedQueryText",
            "params": {
                "text": "MATCH (n) |> LIMIT 1",
                "reorder_filter_selectivity": 0.5
            }
        });
        assert!(serde_json::from_value::<Method>(retired_parameter).is_err());
    }

    #[cfg(feature = "mining")]
    #[test]
    fn mining_algorithms_accept_only_the_current_canonical_names() {
        assert_eq!(
            serde_json::from_str::<ForecastAlgorithm>("\"holtwinters\"").unwrap(),
            ForecastAlgorithm::Holtwinters
        );
        assert_eq!(
            serde_json::from_str::<ClassifyAlgorithm>("\"gaussiannb\"").unwrap(),
            ClassifyAlgorithm::Gaussiannb
        );
        assert_eq!(
            serde_json::from_str::<ReduceAlgorithm>("\"svd\"").unwrap(),
            ReduceAlgorithm::Svd
        );
        for retired in [
            "holt_winters",
            "hw",
            "ets",
            "gaussian_nb",
            "gnb",
            "multinomial_nb",
            "mnb",
            "linear_svc",
            "linearsvc",
            "truncated_svd",
            "truncatedsvd",
        ] {
            let encoded = serde_json::to_string(retired).unwrap();
            assert!(serde_json::from_str::<ForecastAlgorithm>(&encoded).is_err());
            assert!(serde_json::from_str::<ClassifyAlgorithm>(&encoded).is_err());
            assert!(serde_json::from_str::<ReduceAlgorithm>(&encoded).is_err());
        }
    }

    #[test]
    fn test_method_pagerank_roundtrip() {
        let method = Method::PageRank {
            damping: 0.85,
            iterations: 100,
        };
        let json = serde_json::to_string(&method).unwrap();
        let parsed: Method = serde_json::from_str(&json).unwrap();
        if let Method::PageRank {
            damping,
            iterations,
        } = parsed
        {
            assert!((damping - 0.85).abs() < f64::EPSILON);
            assert_eq!(iterations, 100);
        } else {
            panic!("Wrong method");
        }
    }

    #[test]
    fn raw_result_payload_decodes_to_typed_value() {
        // Phase C-D compact encoding: a Raw payload carries the typed result as a
        // MessagePack bin. Over the wire it round-trips as a bin and decodes back
        // to the EXACT typed value the JSON path produced — what the Python client
        // does on any top-level `bytes` result.
        let scores: Vec<(String, f64)> = vec![("a".into(), 0.5), ("b".into(), 0.25)];
        let resp = Response::ok(7, ResultPayload::raw(&scores));
        let wire = rmp_serde::to_vec_named(&resp).unwrap();
        let decoded: Response = rmp_serde::from_slice(&wire).unwrap();
        // Untagged: a bin result decodes as the first bin-shaped variant; the inner
        // bytes are identical regardless of the variant name.
        let inner = match decoded.result {
            Some(ResultPayload::Raw(b)) | Some(ResultPayload::PropertiesMsgpack(b)) => b,
            other => panic!("expected a bin result payload, got {:?}", other),
        };
        let back: Vec<(String, f64)> = rmp_serde::from_slice(&inner).unwrap();
        assert_eq!(back, scores);
    }

    #[test]
    fn test_method_apply_mutation_roundtrip() {
        let method = Method::ApplyMutation {
            event_type: "TRIPLE_INSERT".to_string(),
            query: "INSERT DATA { <A> <B> <C> }".to_string(),
        };
        let json = serde_json::to_string(&method).unwrap();
        let parsed: Method = serde_json::from_str(&json).unwrap();
        if let Method::ApplyMutation { event_type, query } = parsed {
            assert_eq!(event_type, "TRIPLE_INSERT");
            assert_eq!(query, "INSERT DATA { <A> <B> <C> }");
        } else {
            panic!("Wrong method");
        }
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller — the memory/scene/trajectory mutation + read variants
    /// round-trip through MessagePack (the on-wire + WAL framing) byte-for-byte,
    /// preserving every field.
    #[test]
    fn eg318_memory_scene_trajectory_methods_roundtrip() {
        let methods = vec![
            Method::CreateSummaryNode {
                level: 2,
                child_ids: vec!["e1".into(), "e2".into()],
                props_msgpack: vec![0x80],
            },
            Method::Consolidate {
                episodic_ids: vec!["a".into(), "b".into()],
                semantic_props_msgpack: vec![0x80],
            },
            Method::Maintain {
                ids: vec!["m1".into()],
                now_ms: 123,
                half_life_ms: 604_800_000,
                evict_threshold: 0.5,
                delete: false,
            },
            Method::AddSceneObject {
                pose_msgpack: vec![0x80],
                parent: Some("root".into()),
            },
            Method::StartTrajectory {
                props_msgpack: vec![0x80],
            },
            Method::AppendStep {
                traj_id: "trajectory:dead".into(),
                action_msgpack: vec![0xa4, b'l', b'e', b'f', b't'],
                reward: 1.5,
                state_ref: None,
                next_state_ref: None,
                t: 0,
            },
            Method::SummaryChildren {
                node_id: "s".into(),
            },
            Method::WorldTransform {
                node_id: "o".into(),
            },
            Method::DiscountedReturn {
                traj_id: "t".into(),
                gamma: 0.9,
            },
        ];
        for m in methods {
            let wire = rmp_serde::to_vec_named(&m).unwrap();
            let back: Method = rmp_serde::from_slice(&wire).unwrap();
            // The tag+content framing survives; spot-check a representative field.
            let re = rmp_serde::to_vec_named(&back).unwrap();
            assert_eq!(wire, re, "EG-318 method must msgpack-roundtrip identically");
        }
    }

    #[cfg(feature = "query")]
    #[test]
    fn governed_evidence_locus_wire_round_trips() {
        let locus = EvidenceLocusWire {
            id: "eg:locus:0000000000000001".to_string(),
            subject: EvidenceResourceWire::Occurrence("eg:occurrence:0000000000000002".to_string()),
            address: EvidenceAddressWire::PageRegion {
                page: 4,
                x: 1.0,
                y: 2.0,
                width: 3.0,
                height: 4.0,
            },
            policy_ref: "eg:policy:0000000000000003".to_string(),
            derivation_ref: "eg:derivation:0000000000000004".to_string(),
        };
        let encoded = rmp_serde::to_vec_named(&locus).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<EvidenceLocusWire>(&encoded).unwrap(),
            locus
        );
    }

    #[cfg(feature = "query")]
    #[test]
    fn governed_evidence_locus_wire_rejects_unsafe_identity_and_coordinates() {
        let unsafe_identity = serde_json::json!({
            "id": "eg:locus:0000000000000001",
            "subject": { "kind": "artifact", "id": "not-an-opaque-reference" },
            "address": { "kind": "character_range", "start": 0, "end": 1 },
            "policy_ref": "eg:policy:0000000000000003",
            "derivation_ref": "eg:derivation:0000000000000004"
        });
        assert!(serde_json::from_value::<EvidenceLocusWire>(unsafe_identity).is_err());

        let invalid_range = serde_json::json!({
            "id": "eg:locus:0000000000000001",
            "subject": {
                "kind": "artifact",
                "id": "eg:artifact:0000000000000002"
            },
            "address": { "kind": "character_range", "start": 1, "end": 1 },
            "policy_ref": "eg:policy:0000000000000003",
            "derivation_ref": "eg:derivation:0000000000000004"
        });
        assert!(serde_json::from_value::<EvidenceLocusWire>(invalid_range).is_err());
    }
}
