macro_rules! __eg_method_chunk_0 {
    () => {
        __eg_method_chunk_1!(@acc [

    // ── Node CRUD ────────────────────────────────────────────────────
    AddNode {
        node_id: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        properties_msgpack: Vec<u8>,
    },

    /// Create a node only when `node_id` is absent, returning `Bool(true)` only
    /// for the inserting writer. The membership test and insert are one durable
    /// atomic operation; an existing node is never overwritten.
    CreateNodeIfAbsent {
        node_id: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        conditions_msgpack: Vec<u8>,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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

    /// Atomically acquire one bounded multi-dimensional capacity lease set.
    /// The native engine owns the cell epoch, fence, expiry, and idempotency
    /// rows; the request's owner digest is only an authenticated assertion.
    AcquireCapacity {
        request: crate::native_control::CapacityAcquireRequest,
    },

    /// Renew every named capacity lease in one all-or-nothing transaction.
    RenewCapacity {
        request: crate::native_control::CapacityLeaseMutationRequest,
    },

    /// Release every named capacity lease in one all-or-nothing transaction.
    ReleaseCapacity {
        request: crate::native_control::CapacityLeaseMutationRequest,
    },

    /// Reclaim a bounded page of expired capacity leases.
    ReclaimExpiredCapacity {
        request: crate::native_control::CapacityReclaimRequest,
    },

    /// Return bounded native cells/leases for reconciliation and status.
    ReconcileCapacity {
        request: crate::native_control::CapacityStatusRequest,
    },

    /// Exact bounded native capacity read.
    CapacityStatus {
        request: crate::native_control::CapacityStatusRequest,
    },

    /// Controller/admin CAS for a capacity cell epoch and resource dimension.
    UpdateCapacityCell {
        request: crate::native_control::CapacityCellUpdateRequest,
    },

    /// Idempotent WorkItem admission and command-log append.
    SubmitWorkItem {
        request: crate::native_control::SubmitWorkItemRequest,
    },

    /// Publish, retire, inspect, or reconcile one durable EG Agent Library
    /// definition through the authenticated ControlPlane owner.
    AgentLibrary {
        op: crate::agent_library::AgentLibraryOp,
    },

    /// Publish, retire, or inspect one durable agent GRAPH -- a composition of
    /// Agent Library entries -- through the same authenticated ControlPlane
    /// owner (RF-ADR-008). `AgentLibrary` records what one agent IS; this
    /// records how several of them are composed to do a task.
    AgentGraph {
        op: crate::agent_graph::AgentGraphOp,
    },

    /// Publish, retire, inspect, or SEARCH one durable agent component --
    /// layer 1 of the hierarchy (RF-ADR-008): the model profiles, prompts,
    /// tools, MCP servers/prompts/resources, skills, schemas and predicates
    /// agents are assembled from. `Search` is the capability query this layer
    /// exists for: "what does an agent trying to do XYZ need?"
    AgentComponent {
        op: crate::agent_component::AgentComponentOp,
    },

    /// Publish, retire, inspect or INSTANTIATE one durable agent template --
    /// RF-ADR-008 item C: a published agent plus declared axes of variation.
    /// `Instantiate` is the operation this layer exists for; it binds
    /// parameters and returns an ordinary `AgentLibraryEntryDraft`, so an
    /// instance is admitted and delegated with no template-aware branch
    /// anywhere downstream.
    ///
    /// Not boxed at this level, for the same reason the three layers beside it
    /// are not: the only oversized request is the publish draft, and
    /// `AgentTemplateOp::Publish` already boxes it, so this variant is no
    /// larger than `AgentComponent` next to it (asserted by
    /// `the_publish_request_is_boxed_so_a_template_op_stays_small`).
    AgentTemplate {
        op: crate::agent_template::AgentTemplateOp,
    },

    /// Drive one durable semantic binding and its S1-S6 tiered ingestion
    /// queue (RF-019). This is the surface an external connector uses to make
    /// something searchable: admit a binding, feed authoritative SQL source
    /// rows into S1, then subscribe a worker, claim stage leases for its queue
    /// class, and complete each stage against its predecessor proof.
    ///
    /// Boxed, unlike the four agent layers beside it: `SemanticIndexOp`'s
    /// stage-completion variants carry a lease, a transition, an artifact and a
    /// successor intent together, which is far larger than any agent op and
    /// would otherwise set the size of EVERY `Method`. `Box` is transparent to
    /// serde, so the wire form is unchanged.
    ///
    /// Declared unconditionally, like the `semantic_index` DTO module it is
    /// built from: the wire contract is one contract in every build. The
    /// engine-side durable tier it needs is what carries the `ann-redb` gate,
    /// so the dispatch arm refuses by name in a build without it -- exactly
    /// how the four agent layers refuse in a build without `redb`.
    SemanticIndex {
        op: Box<crate::semantic_index::SemanticIndexOp>,
    },

    /// Authenticate and lower an Agent Library delegation through the native
    /// WorkItem admission command log. The retained library entry is resolved
    /// by the library owner at the handler boundary.
    KgDelegate {
        /// Boxed: a pinned delegation request carries the whole Agent Library
        /// entry reference and would otherwise set the size of EVERY `Method`.
        /// `Box` is transparent to serde, so the wire form is unchanged.
        request: Box<crate::delegation::KgDelegateRequest>,
    },

    /// Bounded all-or-nothing WorkItem admission batch.
    SubmitWorkItems {
        request: crate::native_control::SubmitWorkItemsRequest,
    },

    /// Mint an opaque native capability for the caller's currently-live
    /// WorkItem lease.  All authority bindings are derived in the engine from
    /// the verified request context and the authoritative WorkItem row.
    MintWorkItemClaimCapability {
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityMintRequest,
    },

    /// Verify an opaque native WorkItem capability against the current live
    /// lease before any private payload/body lookup.
    VerifyWorkItemClaimCapability {
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityVerifyRequest,
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
        /// Optional RF-020/GOC-20 terminal receipts. When present, the
        /// mutation compiler lowers the bound receipt nodes to AddNode
        /// operations and one run-event outbox intent in this same batch.
        ///
        /// Boxed because it dominates the size of this variant and, through it,
        /// of EVERY `Method`. `Option<Box<T>>` and `Option<T>` serialize
        /// identically, so the wire form -- and the frozen contract -- are
        /// unchanged; only the in-memory layout moves.
        #[serde(default)]
        outcome_extension: Option<Box<crate::outcome_bundle::TerminalOutcomeExtension>>,
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

    /// Atomic compare-and-set on one WorkItem's non-authority SCHEDULING
    /// METADATA (`checkpoint_id` / `metadata` / `prio_bucket`) — the native
    /// replacement for a generic `CompareAndSetNodeFields` against a
    /// WorkItem row, which `work_item_capability::validate_generic_method`
    /// (RMDD-29) unconditionally refuses once the row is claimed (BUG-111).
    /// Runs inside the SAME durable WorkItem transaction as `ClaimWorkItem`/
    /// `RenewWorkItemLease`, never a side path. Cannot touch `status`/
    /// `lease_owner`/`lease_epoch`/`fencing_token`/`tenant` — the request
    /// carries no such field — so it can never manufacture or extend native
    /// lease authority.
    CasWorkItemMetadata {
        request: crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
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
        ]);
    };
}

pub(crate) use __eg_method_chunk_0;
