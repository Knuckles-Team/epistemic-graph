macro_rules! __eg_method_chunk_1 {
    () => {
        __eg_method_chunk_2!(@acc [

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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        pose_msgpack: Vec<u8>,
        parent: Option<String>,
    },

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — overwrite scene object `node_id`'s LOCAL pose with
    /// `pose_msgpack`. Deterministic. Returns `Bool` (whether the node existed).
    SetPose {
        node_id: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        ]);
    };
}

pub(crate) use __eg_method_chunk_1;
