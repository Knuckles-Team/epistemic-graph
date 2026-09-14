macro_rules! __eg_method_chunk_6 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_chunk_7!(@acc [
$($variants)*

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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        conditions_msgpack: Vec<u8>,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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

    /// Commit the staged transaction (see the section doc above for the OCC
    /// serialization/apply contract). `idempotency_key` (B-9, 2026-08-13,
    /// optional) is a CALLER-chosen dedup key, extending the SAME
    /// `(tenant, graph, idempotency_key)`-scoped durable dedup mechanism
    /// `ApplyChangeEnvelope` uses (`change_envelope.rs`'s
    /// `idempotency_key`/`applied` vs `idempotent_skip` vocabulary) onto the
    /// txn commit's own durable receipt, rather than a second mechanism.
    /// Without it, retry-safety depends entirely on the caller still holding
    /// the exact server-issued `txn_id` (already durable via the receipt) --
    /// provably safe for "commit succeeded, response lost, retry with the SAME
    /// txn_id", but NOT for "I lost track of txn_id and had to re-`BeginTxn`
    /// and re-stage" (a fresh `txn_id` looks like a brand-new transaction).
    /// WITH a caller key, a retry that re-stages under a fresh `txn_id` still
    /// lands on the SAME durable receipt and replay-skips.
    ///
    /// Returns `Bool` when `idempotency_key` is omitted (UNCHANGED wire shape).
    /// Returns a `Json` `{"committed": bool, "replayed": bool}` when a key is
    /// supplied, so the caller can tell "applied just now" from "already
    /// applied — this is the cached result" (mirrors `ApplyChangeEnvelope`'s
    /// `applied`/`idempotent_skip` status).
    Commit {
        txn_id: String,
        #[serde(deserialize_with = "deserialize_required_option")]
        idempotency_key: Option<String>,
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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

    /// Retention: drop every point of `series_id` strictly older than `cutoff` (ns).
    /// A whole-series-empty legal hold blocks this (`SeriesStore::evict_before`) --
    /// held data survives a retention sweep, provably. Returns the number of WHOLE
    /// chunk buckets removed (`ResultPayload::Count`); a straddling bucket is
    /// trimmed in place rather than counted as removed. This is the SECOND durable
    /// write in the `Ts*` family (alongside `TsAppend`) -- routed through the same
    /// `MutationBatch`-compiled path so retention is commit-before-ack durable, not
    /// a best-effort background trim.
    TsEvict {
        series_id: String,
        cutoff: i64,
    },

    /// Retention: drop `series_id` ENTIRELY -- every chunk plus its meta row -- in
    /// one durable write. Same legal-hold boundary as `TsEvict`. Returns the number
    /// of chunks removed (`ResultPayload::Count`; `0` for an unknown or held
    /// series). Unlike `TsEvict`, a subsequently re-appended series with the same
    /// id starts fresh (no stale count/span carried over).
    TsDeleteSeries {
        series_id: String,
    },

    /// Enumerate every series id under the CALLER's own tenant+graph scope (never
    /// cross-tenant/cross-graph -- mirrors every other `Ts*` method's `scoped_key`
    /// derivation). Returns the caller-facing series ids as `ResultPayload::raw`
    /// (`Vec<String>`), decoded server-side from the store's internally-encoded
    /// `SeriesKey`s and filtered to this scope -- the encoded key itself never
    /// crosses the wire. This is what makes `TsEvict`/`TsDeleteSeries` reachable
    /// for a multi-series retention sweep: without an enumeration primitive a
    /// caller has no way to discover which series exist to retain/evict at all.
    TsListSeries,


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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
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
        ]);
    };
}

pub(crate) use __eg_method_chunk_6;
