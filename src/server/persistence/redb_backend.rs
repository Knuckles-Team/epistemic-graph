//! redb write-through persistence backend (CONCEPT:EG-KG.storage.kg-kg, feature `redb`).
//!
//! The authoritative tier commits every graph mutation into an embedded
//! canonical `redb` shard database (`{persist_dir}/graph-<n>.redb`), keyed by a
//! `(graph, …)` prefix so
//! all tenants share the same tables (not a file per graph). The in-memory graph is
//! a bounded resident projection with durability-gated read-through eviction.
//!
//! ## The #1 risk: never one WriteTransaction per mutation
//!
//! A redb `WriteTransaction::commit` is a B-tree + WAL + (optional) fsync — orders
//! of magnitude more expensive than a single row write. Committing one per graph
//! mutation would collapse write p99. So this backend reuses the EXACT threading
//! model in [`crate::durability`]: a dedicated OS thread owns the
//! `Database`, drains a bounded channel, and folds MANY mutations into ONE
//! `WriteTransaction` per group-commit interval. The [`DurabilityPolicy`] cadence maps
//! onto redb `Durability`:
//!   * `Interval` → commit once per interval with `Durability::Immediate`
//!     (group-commit fsync; bounds hard-power loss to the interval).
//!   * `Each`     → commit `Durability::Immediate` after every drained batch.
//!
//! Backpressure is bounded and lossless: when the writer queue is full, producers
//! wait for capacity rather than shedding persistence work. Authoritative batch
//! callers enqueue from Tokio's blocking pool and await durable completion.
//!
//! ## Tables (all keyed by graph prefix)
//!   * `nodes`          `(graph, id)            -> node properties msgpack`
//!   * `edges`          `(graph, src, tgt, ord) -> edge properties msgpack`
//!   * `ledger`         `(graph, seq)           -> ledger line`
//!   * `semantic_store` `graph                  -> semantic store blob (msgpack)`
//!   * `graph_meta`     `graph                  -> {name, graph_type} blob` (replaces manifest.json)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::oneshot;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use crate::change_envelope::{
    ChangeCursor, ChangeEnvelope, ChangeEnvelopeCommit, ChangeEnvelopeRecord, ContentVersion,
};
use crate::durability::DurabilityPolicy;
use crate::graph::GraphCore;
use crate::mutation_batch::{
    MutationBatch, MutationBatchCommit, MutationBatchRecord, MutationOutboxLease,
    MutationOutboxRecord, MutationProjectionCursor,
};
use crate::protocol::{GraphType, Method};
use crate::redb_layout::shard_filename;
use crate::server::ServerState;

use super::PersistenceBackend;

// The graph table layout + the PURE durable-row machinery (Method→rows apply,
// group-commit, checkpoint/load) now live in the server-INDEPENDENT
// `crate::redb_store` (CONCEPT:EG-KG.backend.engine-modes) so the embedded API can drive the SAME
// durable format with no Tokio. This backend reuses them verbatim — ONE format,
// never duplicated — and adds only the off-reactor group-commit writer thread +
// the `PersistenceBackend` async trait wiring on top.
#[cfg(any(feature = "compute-dist", feature = "matview"))]
use crate::redb_store::MatViewScanResult;
use crate::redb_store::{
    ack_mutation_outbox, claim_mutation_outbox, clear_xshard_decision, clear_xshard_prepare,
    commit_change_envelope, commit_change_envelopes, commit_crossmodal, commit_mutation_batch,
    commit_mutation_batch_crossmodal, commit_mutation_batch_state, commit_ops,
    durable_node_presence as read_durable_node_presence, get_xshard_decision,
    get_xshard_decision_retain, get_xshard_prepare, purge_graph_rows, put_xshard_decision,
    put_xshard_prepare, put_xshard_recoverable_pending, read_all_dumps, read_all_graph_meta,
    read_change_cursor as read_change_cursor_record,
    read_change_envelope as read_change_envelope_record,
    read_content_version as read_content_version_record, read_graph_dump,
    read_mutation_batch as read_mutation_batch_record,
    read_mutation_graph_version as read_mutation_graph_version_record,
    read_mutation_lifecycle_head as read_mutation_lifecycle_head_record,
    read_mutation_outbox as read_mutation_outbox_records,
    read_mutation_projection_cursor as read_mutation_projection_cursor_record, read_one_node,
    read_resource_reservation as read_resource_reservation_record,
    read_resource_reservation_status as read_resource_reservation_status_record,
    scan_xshard_decisions, scan_xshard_prepares, write_graph_meta, GraphDump, XshardDecisionScan,
    XshardPrepareScan, RAFT_LOG,
};
/// `(first, last)` present Raft log index for a group, or an error (CONCEPT:EG-KG.storage.one-fsync-covers-raft).
type LogBoundsResult = Result<(Option<u64>, Option<u64>), String>;
const MAX_DURABLE_SEMANTIC_BYTES: usize = 384 * 1024 * 1024;
const MAX_DURABLE_SEMANTIC_ITEMS: usize = 4_000_000;

fn decode_durable_semantic(
    bytes: &[u8],
) -> Result<crate::compute::semantic::SemanticStore, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_DURABLE_SEMANTIC_BYTES,
            MAX_DURABLE_SEMANTIC_ITEMS,
            64,
        ),
    )
    .map_err(|_| "stored semantic index is invalid".to_string())
}
// Per-group Raft metadata (vote, applied-state pointers, last-purged), keyed by
// `(group_id, key)`. Lives in the authoritative shard alongside the log; Raft-only, so it
// stays here with the Raft helpers rather than in the shared graph store.
pub(crate) const RAFT_META: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("raft_meta");

// Time-series tables (CONCEPT:AU-KG.retrieval.god-nodes-communities). The CANONICAL `(series_id, bucket_start)`
// chunk schema is declared once in the eg-tsdb crate (where the store/query logic
// lives) and re-exported here so it sits WITH the durable tier's other table
// definitions. They use the SAME redb composite-key range-scan idiom as
// NODES/EDGES/LEDGER. The series store opens its OWN `series.redb` file (redb holds
// an exclusive per-process file lock, so it cannot share this backend's shard handle)
// — these aliases document the schema beside the graph tables; the actual
// open + I/O lives in `eg_tsdb::store::SeriesStore`.
#[cfg(feature = "tsdb")]
#[allow(unused_imports)]
pub(crate) use eg_tsdb::store::{SERIES_CHUNKS, SERIES_META};

/// Boxed payload of a [`Cmd::CrossModalCommit`] (CONCEPT:EG-KG.txn.reader-never-sees-node + EG-360). Holds ONE
/// graph's full multi-modal write-set — graph methods (incl. lowered OWL-axiom /
/// SPARQL-CONSTRUCT triples), vector upserts, blob-refs, and time-series measurement
/// batches — all of which land in ONE `WriteTransaction`.
pub(crate) struct CrossModalPayload {
    pub(crate) graph: String,
    pub(crate) methods: Vec<Method>,
    pub(crate) vectors: Vec<(String, Vec<f32>)>,
    pub(crate) blob_refs: Vec<(String, String)>,
    pub(crate) measurements: Vec<crate::MeasurementBatch>,
}

/// Boxed payload for one authoritative MutationBatch writer command.  Keeping
/// the complete batch together is what prevents queue pressure from splitting a
/// logical Commit into independently acknowledged records.
pub(crate) struct MutationBatchPayload {
    pub(crate) graph: String,
    pub(crate) batch: MutationBatch,
    pub(crate) authoritative_state_msgpack: Option<Vec<u8>>,
    pub(crate) result_msgpack: Option<Vec<u8>>,
    pub(crate) committed_at_ms: u64,
    /// Whether this commit should append audit-chain entries. Only meaningful
    /// when `authoritative_state_msgpack` is `Some`; `commit_mutation_batch`
    /// (compact-row, `authoritative_state_msgpack: None`) always stamps `true`
    /// here since that path gates audit per-operation from the (identity-
    /// preserving) method itself. See `redb_store::commit_mutation_batch_inner`'s
    /// doc comment.
    pub(crate) audited: bool,
}

/// One writer command for a cross-modal universal batch. It carries the coordinator
/// record and result so modality rows and status/fence/idempotency/outbox share the
/// exact same fsync point.
pub(crate) struct CrossModalBatchPayload {
    pub(crate) graph: String,
    pub(crate) batch: MutationBatch,
    pub(crate) methods: Vec<Method>,
    pub(crate) vectors: Vec<(String, Vec<f32>)>,
    pub(crate) blob_refs: Vec<(String, String)>,
    pub(crate) measurements: Vec<crate::MeasurementBatch>,
    pub(crate) result_msgpack: Option<Vec<u8>>,
    pub(crate) committed_at_ms: u64,
}

pub(crate) struct ChangeEnvelopePayload {
    pub(crate) graph: String,
    pub(crate) envelope: ChangeEnvelope,
    pub(crate) committed_at_ms: u64,
}

/// Payload of a [`Cmd::ChangeEnvelopesCommit`] — a batch of governed envelopes that
/// all target `graph`, committed in ONE shard transaction.
pub(crate) struct ChangeEnvelopesPayload {
    pub(crate) graph: String,
    pub(crate) envelopes: Vec<ChangeEnvelope>,
    pub(crate) committed_at_ms: u64,
}

/// A bounded page request over one graph's durable rows — every
/// [`Cmd::ReadGraphDumpPage`] field except the routing `graph` name and the
/// reply channel. Boxed inside the command so an unsent `Cmd` (returned whole
/// inside a channel `SendError`) stays small.
pub(crate) struct PageQuery {
    pub(crate) node_offset: usize,
    pub(crate) edge_offset: usize,
    pub(crate) node_after: Option<String>,
    pub(crate) edge_after: Option<(String, String, u32)>,
    pub(crate) page_size: usize,
}

/// One write command handed to the off-reactor thread. A `Mutation` carries the
/// graph file-name + the applied method; the thread translates it into row writes
/// inside the current group-commit transaction.
pub(crate) enum Cmd {
    Mutation {
        graph: String,
        method: Box<Method>,
        /// The writer fires this oneshot after the
        /// `WriteTransaction` carrying this op has durably committed, so the awaiting
        /// dispatch task only acks the client once the write is on disk. Many such
        /// senders ride the SAME group-commit batch — one fsync, N notified writers.
        /// `Err` is sent if that op's commit failed (dispatch → ERROR response).
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Durably persist a graph's identity row (`graph_meta`) so authoritative
    /// `load_all` recovers the graph under its real name/type. Carries a completion
    /// oneshot (commit-before-ack semantics).
    RegisterGraph {
        graph: String,
        name: String,
        graph_type: GraphType,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Drop EVERY durable row for one graph — nodes/edges/ledger/semantic AND the
    /// `graph_meta` identity row — in one durable transaction (CONCEPT:EG-KG.backend.tenant-delete-recreate-same).
    /// Issued when a tenant is DELETED so a recreate of the SAME name starts from a
    /// clean durable slate: without this the stale rows survive (same `graph_fname`
    /// key) and leak into the recreated tenant via the read-through / `load_all`.
    /// Carries a completion oneshot (commit-before-ack: the delete is acked only
    /// after the purge is on disk).
    PurgeGraph {
        graph: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Read a single node's stored properties back (read-through on RAM miss under
    /// authoritative mode). NO LONGER CONSTRUCTED as of CONCEPT:EG-KG.storage.snapshot-read-off-writer — the
    /// point-read path now serves directly off a `begin_read()` MVCC snapshot on the
    /// shard's shared `Database` (see `read_node_blocking`), so it never routes
    /// through the writer. The variant + handler are retained (the writer loop is left
    /// byte-for-byte) but unused; `allow(dead_code)` keeps the build warning-clean.
    #[allow(dead_code)]
    ReadNode {
        graph: String,
        node_id: String,
        reply: std::sync::mpsc::Sender<Result<Option<Vec<u8>>, String>>,
    },
    /// Read the full store back as owned dumps. redb holds an EXCLUSIVE per-process
    /// file lock, so the load MUST go through the one thread that owns the
    /// `Database` rather than opening a second handle (which errors "Database
    /// already open"). The async caller rebuilds the registry from the dumps.
    Load {
        reply: std::sync::mpsc::Sender<Result<Vec<GraphDump>, String>>,
    },
    /// Read ONE graph's durable rows back as an owned dump (CONCEPT:EG-KG.storage.100m-tenant — tenant
    /// rehydration). Goes through the owner thread (exclusive file lock) and flushes
    /// pending writes first so the rehydrated dump reflects the latest durable state.
    ReadGraphDump {
        graph: String,
        reply: std::sync::mpsc::Sender<Result<Option<GraphDump>, String>>,
    },
    /// Read ONE BOUNDED page of one graph's durable rows (CONCEPT:EG-KG.sharding.paged-lazy-open, L38
    /// "paged adjacency") — the memory-bounded sibling of `ReadGraphDump` a paged
    /// lazy-open/page-in call uses so a 10M+-node graph never has its full node/edge
    /// set collected into one `Vec` at the SOURCE. Goes through the owner thread
    /// (exclusive file lock) and flushes pending writes first, same as
    /// `ReadGraphDump`.
    ReadGraphDumpPage {
        graph: String,
        query: Box<PageQuery>,
        reply: std::sync::mpsc::Sender<Result<Option<crate::redb_store::GraphDumpPage>, String>>,
    },
    /// Export ONE graph's rows VERBATIM for an online shard move (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs
    /// on the SOURCE shard's writer: flush pending first (so the snapshot is complete),
    /// then scan the raw value blobs (encryption + audit chain untouched).
    ExportGraphRaw {
        graph: String,
        reply: std::sync::mpsc::Sender<Result<super::online_reshard::RawGraphRows, String>>,
    },
    /// Import ONE graph's verbatim rows on an online shard move (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on
    /// the DESTINATION shard's writer and lands them in ONE `Durability::Immediate` commit
    /// — the commit-before-ack point of the move.
    ImportGraphRaw {
        graph: String,
        rows: Box<super::online_reshard::RawGraphRows>,
        reply: std::sync::mpsc::Sender<Result<(), String>>,
    },
    /// Import ONLY the DELTA of an online shard move (CONCEPT:EG-KG.backend.flush-pending-first, R1 delta-copy). Runs
    /// on the DESTINATION shard's writer under the exclusive routing quiesce; lands the
    /// small set of rows that changed since the bulk pass (upserts + removals) in ONE
    /// `Durability::Immediate` commit — the short under-quiesce write that shrinks the pause.
    ImportGraphDelta {
        graph: String,
        delta: Box<super::online_reshard::RawGraphDelta>,
        reply: std::sync::mpsc::Sender<Result<(), String>>,
    },
    /// Remove replay/outbox and audit history from the old shard after an online
    /// route flip. Tenant deletion uses `PurgeGraph` and deliberately retains history.
    PurgeMovedMutationRows {
        graph: String,
        reply: std::sync::mpsc::Sender<Result<(), String>>,
    },
    /// Verify ONE graph's tamper-evident hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security).
    /// Flushes pending first so the walk reflects the latest durable entries, then
    /// scans `(graph, 0..)` and reports OK or the first break.
    #[cfg(feature = "security")]
    AuditVerify {
        graph: String,
        reply: std::sync::mpsc::Sender<Result<crate::protocol::AuditReport, String>>,
    },
    /// TEST-ONLY tamper of one audit entry (see `test_tamper_audit_entry`).
    #[cfg(all(test, feature = "security"))]
    TestTamperAudit {
        graph: String,
        seq: u64,
        reply: std::sync::mpsc::Sender<Result<(), String>>,
    },
    /// Provenance anchoring (CONCEPT:EG-KG.sharding.row-level-security): durably append a Merkle root over an
    /// ALREADY-HASHED `:ToolCall`/`:RunTrace` window into the graph's tamper-evident
    /// audit chain, plus its member leaf-hash side row. `root`/`members` are
    /// computed by the CALLER off this thread (`provenance_leaf_hashes_blocking`,
    /// which reads via the lock-free MVCC snapshot path, not this channel), so this
    /// command's own cost is O(1) in window size — the periodic sweep's write-path
    /// overhead is bounded regardless of how large the window was. `Ok(None)` means
    /// the root was unchanged since the last anchor (skipped, no row written).
    #[cfg(feature = "security")]
    ProvenanceAnchorCommit {
        graph: String,
        root: crate::audit::Hash,
        members: Vec<(String, crate::audit::Hash)>,
        reply: std::sync::mpsc::Sender<Result<Option<u64>, String>>,
    },
    /// Produce + verify a Merkle inclusion proof for one node against a prior
    /// provenance anchor (CONCEPT:EG-KG.sharding.row-level-security; `Method::AuditProveInclusion`). Routed
    /// through the owner thread (exclusive file lock), which flushes pending first
    /// — mirrors `AuditVerify` so a proof always reflects the latest durable state.
    #[cfg(feature = "security")]
    AuditProveInclusion {
        graph: String,
        node_id: String,
        anchor_seq: Option<u64>,
        reply: std::sync::mpsc::Sender<Result<crate::protocol::MerkleInclusionReport, String>>,
    },
    /// **Cross-modal ACID commit (CONCEPT:EG-KG.txn.reader-never-sees-node).** Land a graph, vector, blob-ref,
    /// and property write-set for ONE graph in ONE `WriteTransaction`, all-or-nothing,
    /// awaiting its durable fsync (commit-before-ack). On any error nothing lands: the
    /// dropped transaction discards every modality (no partial cross-modal commit).
    CrossModalCommit {
        /// The multi-modal write-set, BOXED so the (now five-field) cross-modal payload
        /// does not bloat every `Cmd` variant — keeping `Cmd` (and the
        /// `SendError<Cmd>` the writer-channel sends return) small (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
        payload: Box<CrossModalPayload>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Cross-modal public mutation through the universal batch kernel.
    CrossModalBatchCommit {
        payload: Box<CrossModalBatchPayload>,
        done: oneshot::Sender<Result<MutationBatchCommit, String>>,
    },
    /// Universal authoritative commit: graph rows + durable status +
    /// idempotency + transactional outbox in ONE immediate transaction.
    MutationBatchCommit {
        payload: Box<MutationBatchPayload>,
        done: oneshot::Sender<Result<MutationBatchCommit, String>>,
    },
    /// Native WorkItem capability mint/verify.  These commands flush pending
    /// graph mutations first and execute the control-row authorization plus
    /// private capability ledger operation in one writer-owned transaction.
    MintWorkItemClaimCapability {
        graph: String,
        request: crate::epistemic_operations::WorkItemClaimCapabilityMintRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
        done: oneshot::Sender<
            Result<crate::epistemic_operations::WorkItemClaimCapabilityResult, String>,
        >,
    },
    VerifyWorkItemClaimCapability {
        graph: String,
        request: crate::epistemic_operations::WorkItemClaimCapabilityVerifyRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
        done: oneshot::Sender<
            Result<crate::epistemic_operations::WorkItemClaimCapabilityResult, String>,
        >,
    },
    /// Native development-lane hold/quota mutation (RMDD-28: Reserve/Renew/
    /// Observe/Finish/Cleanup/UpdateQuota). Flushes pending graph mutations
    /// first, then runs the kernel's own self-contained begin_write()/commit()
    /// against the native `development_lane_*` tables in one writer-owned
    /// transaction, same shape as the claim-capability commands above.
    CommitDevelopmentLane {
        graph: String,
        method: Box<Method>,
        now_ms: u64,
        done: oneshot::Sender<Result<Vec<u8>, String>>,
    },
    /// Engine-native governed ingest commit. This is deliberately one writer
    /// command so queue pressure can never split graph/material/governance state.
    ChangeEnvelopeCommit {
        payload: Box<ChangeEnvelopePayload>,
        done: oneshot::Sender<Result<ChangeEnvelopeCommit, String>>,
    },
    /// Engine-native governed ingest commit for a BATCH of envelopes targeting one
    /// graph — one writer command, one shard transaction/fsync for the whole page.
    ChangeEnvelopesCommit {
        payload: Box<ChangeEnvelopesPayload>,
        done: oneshot::Sender<Result<Vec<ChangeEnvelopeCommit>, (usize, String)>>,
    },
    /// Lease pending transactional-outbox events on the writer thread so claim
    /// selection and lease installation are one durable transaction.
    MutationOutboxClaim {
        graph: String,
        consumer: String,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
        done: oneshot::Sender<Result<Vec<MutationOutboxLease>, String>>,
    },
    /// Fence-aware ack plus monotonic projection cursor advancement.
    MutationOutboxAck {
        graph: String,
        lease: Box<MutationOutboxLease>,
        projection: String,
        now_ms: u64,
        done: oneshot::Sender<Result<MutationProjectionCursor, String>>,
    },
    Shutdown {
        reply: std::sync::mpsc::Sender<()>,
    },
    // ── Raft log/meta (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — all on the writer thread because redb
    // holds an EXCLUSIVE per-process file lock, so log + M2 graph data must go
    // through the ONE thread that owns the Database. ──────────────────────────
    /// Append Raft log entries `(group_id, index) -> blob` and await durable commit.
    /// Buffered into the SAME `Pending` batch as M2 mutations so a log append and a
    /// graph mutation coalesce into ONE group-commit `WriteTransaction` / one fsync
    /// (the spike's key optimization). Commit-before-ack: the writer fires `done`
    /// only after the carrying transaction has durably committed.
    RaftLogAppend {
        group_id: u64,
        entries: Vec<(u64, Vec<u8>)>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Read a `[lo, hi]` inclusive index range for one group, in order.
    RaftLogRead {
        group_id: u64,
        lo: u64,
        hi: u64,
        reply: std::sync::mpsc::Sender<Result<Vec<Vec<u8>>, String>>,
    },
    /// Delete entries with index >= `from` for one group (conflict truncation).
    RaftLogDeleteFrom {
        group_id: u64,
        from: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Delete entries with index <= `upto` for one group (purge/compaction).
    RaftLogPurgeUpto {
        group_id: u64,
        upto: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// (first, last) present log index for one group, for `get_log_state`.
    RaftLogBounds {
        group_id: u64,
        reply: std::sync::mpsc::Sender<LogBoundsResult>,
    },
    /// Durably write one Raft metadata key (vote / applied-state / last-purged).
    RaftMetaPut {
        group_id: u64,
        key: String,
        val: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Read one Raft metadata key.
    RaftMetaGet {
        group_id: u64,
        key: String,
        reply: std::sync::mpsc::Sender<Result<Option<Vec<u8>>, String>>,
    },
    // ── Cross-shard 2PC durable records (CONCEPT:EG-KG.storage.lane-n-increment) ──────────────────
    /// Durably persist ONE participant group's PREPARE slice for a cross-shard txn
    /// (commit-before-vote: a group votes yes only after this is on disk).
    XshardPreparePut {
        txn_id: String,
        group_id: u64,
        slice: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Read one exact participant prepare without scanning unrelated transactions.
    XshardPrepareGet {
        txn_id: String,
        group_id: u64,
        reply: std::sync::mpsc::Sender<Result<Option<Vec<u8>>, String>>,
    },
    /// Durably write the coordinator's DECISION for a cross-shard txn (the atomic
    /// commit point), optionally retained until a separate parent is terminal.
    XshardDecisionPut {
        txn_id: String,
        commit: bool,
        retain_for_parent: bool,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Durably mark the start of a parent-recoverable protocol before phase 1.
    XshardRecoverablePendingPut {
        txn_id: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Clear ONE participant's prepare record after the txn is resolved.
    XshardPrepareClear {
        txn_id: String,
        group_id: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Clear a resolved txn's decision record (after every participant cleared).
    XshardDecisionClear {
        txn_id: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Scan ALL in-doubt prepare records (txn_id, group_id, slice) for recovery.
    XshardScanPrepares {
        reply: std::sync::mpsc::Sender<XshardPrepareScan>,
    },
    /// Scan digest-only decisions for parent-aware startup cleanup.
    XshardScanDecisions {
        reply: std::sync::mpsc::Sender<XshardDecisionScan>,
    },
    /// Read a txn's decision (Some(true)=commit, Some(false)=abort, None=undecided).
    XshardDecisionGet {
        txn_id: String,
        reply: std::sync::mpsc::Sender<Result<Option<bool>, String>>,
    },
    /// Is this decision/pending marker retained for a MutationBatch parent?
    XshardDecisionRetainGet {
        txn_id: String,
        reply: std::sync::mpsc::Sender<Result<bool, String>>,
    },
    /// Durably upsert a named materialized view's blob (CONCEPT:EG-KG.storage.feature).
    #[cfg(feature = "compute-dist")]
    MatViewPut {
        name: String,
        blob: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Scan every persisted materialized view `(name, blob)` for reload on boot.
    #[cfg(feature = "compute-dist")]
    MatViewScan {
        reply: std::sync::mpsc::Sender<MatViewScanResult>,
    },
    /// Durably upsert a PLAN-BACKED matview definition (CONCEPT:EG-KG.storage.plan-backed-matview).
    #[cfg(feature = "matview")]
    PlanMatViewPut {
        name: String,
        blob: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Durably delete a plan-backed matview definition.
    #[cfg(feature = "matview")]
    PlanMatViewDelete {
        name: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Scan every persisted plan-backed matview `(name, blob)` for reload on boot.
    #[cfg(feature = "matview")]
    PlanMatViewScan {
        reply: std::sync::mpsc::Sender<MatViewScanResult>,
    },
    /// Durably upsert an incremental matview's operator-state snapshot
    /// (CONCEPT:EG-KG.storage.incremental-matview).
    #[cfg(feature = "matview")]
    MatViewOperatorStatePut {
        name: String,
        blob: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Durably delete an incremental matview's operator-state snapshot.
    #[cfg(feature = "matview")]
    MatViewOperatorStateDelete {
        name: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Scan every persisted incremental-matview operator-state snapshot.
    #[cfg(feature = "matview")]
    MatViewOperatorStateScan {
        reply: std::sync::mpsc::Sender<MatViewScanResult>,
    },
}

/// Adaptive group-commit micro-linger tuning for the redb writer (CONCEPT:EG-KG.backend.adaptive-linger-coalesce).
///
/// Live profiling of the `eg-redb-writer` thread showed it pinned ~100% on ext4
/// writeback (disk ~83% util, ~50ms write latency, queue depth 42) while every
/// tokio worker sat idle — it is the ingestion write ceiling. The machinery already
/// group-commits (many `Cmd::Mutation` fold into ONE `WriteTransaction`/fsync), but
/// because every authoritative write carries a commit-before-ack `done` oneshot,
/// `Pending::has_barrier()` is ALWAYS true, so the writer commits the instant the
/// channel momentarily drains. With low in-flight write concurrency (serial awaits —
/// the idle workers) the batch is whatever incidentally sat in the channel, i.e.
/// ~1 op ⇒ ~1 fsync per write. The batching machinery was starved of a window.
///
/// This adds a bounded, adaptive linger: when about to commit a SHALLOW barrier
/// batch, spend ONE `recv_timeout(linger)` letting more concurrent writers arrive,
/// then drain again. It MIRRORS the in-memory write-coalescer's `max_linger`
/// (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, `write_coalescer.rs`) but for the DURABLE tier — it does NOT
/// touch the coalescer. Durability is unchanged: authoritative writes still commit
/// `Durability::Immediate` BEFORE their `done` fires; we only widen the batch, never
/// defer an ack past its commit. A crash before commit still loses only un-acked writes.
#[derive(Debug, Clone, Copy)]
pub struct RedbGroupCommitConfig {
    /// Max time to linger for more concurrent writers before committing a shallow
    /// barrier batch. `Duration::ZERO` disables lingering entirely (commit-on-drain
    /// = today's behavior, used as the bench baseline).
    pub linger: Duration,
    /// Only linger when `pending.ops.len()` is BELOW this — a deep batch already
    /// coalesces well, so lingering buys nothing and just adds latency (adaptive).
    pub shallow_threshold: usize,
}

impl RedbGroupCommitConfig {
    /// Resolve from env (Configuration discipline: read once at backend open).
    ///   * `EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US` — linger microseconds (default
    ///     `1000` = 1ms; `0` disables lingering / restores commit-on-drain).
    ///   * `EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW` — shallow-batch op threshold
    ///     (default `32`); the writer lingers only while `ops.len()` is under it.
    pub fn from_env() -> Self {
        let linger_us = std::env::var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(1000);
        let shallow = std::env::var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(32);
        Self {
            linger: Duration::from_micros(linger_us),
            // Never above the 4096 early-flush bound; at least 1.
            shallow_threshold: shallow.clamp(1, 4096),
        }
    }
}

impl Default for RedbGroupCommitConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Group-commit observability for the redb writer (CONCEPT:EG-KG.backend.adaptive-linger-coalesce), mirroring
/// `write_coalescer::BatchStats`. `ops / commits` is the average batch size = the
/// fsyncs-saved ratio; `lingered` counts commits that paid a micro-linger window.
/// Cheap relaxed atomics, shared (`Arc`) between the writer thread and any reader.
#[derive(Debug, Default)]
pub struct RedbCommitStats {
    /// Group-commit `WriteTransaction`s issued on the run-loop barrier/timeout path.
    pub commits: AtomicU64,
    /// Total graph ops folded across those commits.
    pub ops: AtomicU64,
    /// How many of those commits paid a micro-linger window.
    pub lingered: AtomicU64,
}

impl RedbCommitStats {
    fn record(&self, ops: usize, lingered: bool) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.ops.fetch_add(ops as u64, Ordering::Relaxed);
        if lingered {
            self.lingered.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }
    pub fn lingered(&self) -> u64 {
        self.lingered.load(Ordering::Relaxed)
    }
    /// Average group-commit batch size (`ops / commits`); 0.0 before any commit.
    pub fn avg_batch(&self) -> f64 {
        let c = self.commits();
        if c == 0 {
            0.0
        } else {
            self.ops() as f64 / c as f64
        }
    }
}

// ── Sharded K-way durable writer (CONCEPT:EG-KG.backend.sharded-k-way-durable) ────────────────────────────
//
// redb is single-writer-PER-FILE: one authoritative shard + one writer thread
// serializes EVERY tenant's durable commits onto ONE core — a 64-core box writes
// on 1 core. EG-026 shards by graph into K independent redb files, each with its
// OWN writer thread / channel / `Pending` (incl. the EG-024 micro-linger + the
// EG-025 audit tail cache), so K cores commit in parallel. A graph ALWAYS routes
// to the same shard (`shard_index(graph_fname) % K`), so its data + audit chain +
// group-commit stay co-located and single-writer-correct PER SHARD — every
// durability invariant (commit-before-ack, group-commit, backpressure-not-drop)
// holds unchanged inside each shard.
//
// K = clamp(cpu/2, 1, 8), overridable via `EPISTEMIC_GRAPH_REDB_SHARDS`. Every K uses
// the same canonical `graph-<n>.redb` naming contract, including `graph-0.redb` for
// K=1. K is FIXED per persist-dir once created: `reconcile_shard_layout` validates
// and honors the current on-disk layout (changing K needs an offline migration).

/// Stable FNV-1a routing of a graph's sanitized fname to a shard index (CONCEPT:EG-KG.backend.sharded-k-way-durable).
/// Deterministic across processes/restarts (NOT `DefaultHasher` randomness) — a graph
/// MUST resolve to the same shard every boot or its durable rows become unreachable.
pub(crate) fn shard_index(graph_fname: &str, k: usize) -> usize {
    if k <= 1 {
        return 0;
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in graph_fname.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % k as u64) as usize
}

/// Run one blocking closure PER shard CONCURRENTLY on the blocking pool and collect
/// their results in shard order (CONCEPT:AU-KG.backend.roadmap-f-parallel-cross, roadmap F — parallel cross-shard read
/// fan-out). EVERY task is spawned BEFORE any is awaited, which is the property that
/// makes a K-shard fan-out overlap instead of serialize (a spawn-then-await-each loop is
/// serial). The closures run off each shard's `begin_read()` MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer),
/// so the fan-out never routes through a writer thread. The first error short-circuits.
async fn join_blocking_in_order<T, F>(tasks: Vec<F>) -> Result<Vec<T>, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    let handles: Vec<_> = tasks.into_iter().map(tokio::task::spawn_blocking).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(
            h.await
                .map_err(|e| format!("shard read join error: {e}"))??,
        );
    }
    Ok(out)
}

/// Resolve the shard count K (CONCEPT:EG-KG.backend.sharded-k-way-durable).
///   * Under the `raft` feature AND a configured Raft node (ADR-2 / W1.2,
///     `reports/wave1/ADR-scale-trio.md` §ADR-2): K == N raft groups — raft group `g`
///     owns redb shard `g`, so HA no longer forces K=1. The count follows the group
///     count (`EPISTEMIC_GRAPH_RAFT_GROUPS`, default cores-derived up to `MAX_SHARD_COUNT`);
///     `EPISTEMIC_GRAPH_REDB_SHARDS` does NOT apply under raft (K is pinned to N so the
///     group↔shard alignment is exact). An existing K=1 store on disk stays K=1 until the
///     offline `migrate-shards` tool rewrites its layout (the detected layout wins at open).
///   * `EPISTEMIC_GRAPH_REDB_SHARDS` overrides the non-raft count (clamped 1..=64).
///   * In `cfg(test)` default to 1 so the existing single-writer durability/audit/
///     group-commit tests run the byte-for-byte K=1 path unless they opt in via the env.
///   * Otherwise K = clamp(cpu/2, 1, 8) — mirrors EG-028 `detect_capacity().cpus`
///     (when that module lands this can call `crate::autosize::detect_capacity()`).
fn resolve_shard_count() -> usize {
    // Raft active ⇒ K == N groups (ADR-2 / W1.2), NOT the forced K=1 of the M2 spike.
    #[cfg(feature = "raft")]
    if std::env::var("EPISTEMIC_GRAPH_RAFT_NODE_ID").is_ok() {
        if std::env::var("EPISTEMIC_GRAPH_REDB_SHARDS").is_ok() {
            tracing::warn!(
                "EPISTEMIC_GRAPH_REDB_SHARDS is ignored under an active Raft node; the durable \
                 shard count follows EPISTEMIC_GRAPH_RAFT_GROUPS (K == N groups, group g owns \
                 shard g)"
            );
        }
        let groups = crate::raft::config::raft_group_count();
        tracing::info!(
            shards = groups,
            "raft active: opening K == N durable shards (ADR-2 W1.2 — raft group g owns redb \
             shard g; N parallel durable writers per node)"
        );
        return groups as usize;
    }
    if let Ok(v) = std::env::var("EPISTEMIC_GRAPH_REDB_SHARDS") {
        if let Ok(n) = v.trim().parse::<usize>() {
            return n.clamp(1, crate::redb_layout::MAX_SHARD_COUNT);
        }
    }
    #[cfg(test)]
    {
        1
    }
    #[cfg(not(test))]
    {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .max(1);
        (cpus / 2).clamp(1, 8)
    }
}

/// Resolve the per-shard early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb — auto-size the
/// previously HARDCODED `4096`). The writer flushes a `Pending` batch early once it
/// holds this many ops, bounding writer memory before the bounded channel saturates.
///   * `EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD` overrides (clamped 64..=1_048_576).
///   * Else ~half the authoritative writer queue depth (`capacity`, itself
///     hardware-auto-sized via `Capacity::writer_queue()`), clamped 256..=16384.
fn resolve_flush_threshold(capacity: usize) -> usize {
    if let Ok(v) = std::env::var("EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD") {
        if let Ok(n) = v.trim().parse::<usize>() {
            if n > 0 {
                return n.clamp(64, 1_048_576);
            }
        }
    }
    (capacity / 2).clamp(256, 16384)
}

/// One durable shard (CONCEPT:EG-KG.backend.sharded-k-way-durable): its OWN redb file + off-reactor group-commit
/// writer thread + bounded channel + `Pending` (incl. the EG-024 linger + EG-025
/// audit tail cache) + drop/commit counters. Single-writer-per-FILE, so K shards
/// commit in parallel on K cores.
struct Shard {
    db_path: String,
    /// `Weak` handle to THIS shard's redb `Database` (CONCEPT:EG-KG.storage.snapshot-read-off-writer — snapshot reads
    /// off the writer). redb 4.1 is MVCC: `Database::begin_read()` opens a consistent
    /// read snapshot that runs CONCURRENTLY with the single writer (no writer
    /// involvement, no commit). The writer thread owns the SOLE STRONG `Arc`; the
    /// point-read / read-through path `upgrade()`s this `Weak` to serve an evicted node
    /// DIRECTLY off a `begin_read()` snapshot on the SAME handle WITHOUT routing through
    /// the writer's channel and WITHOUT forcing a group-commit. Holding a `Weak` (not a
    /// strong clone) is deliberate: the exclusive per-process file lock then releases
    /// EXACTLY when the writer thread exits on `shutdown` (the strong Arc drops),
    /// preserving the pre-EG-027 lifetime — a reopen of the same persist dir after
    /// shutdown succeeds, and a read after shutdown fails fast (upgrade ⇒ `None`)
    /// instead of pinning the file lock. Opening a SECOND `Database` on the file would
    /// hit redb's exclusive lock, which is why reads share this handle.
    db: Weak<Database>,
    tx: SyncSender<Cmd>,
    /// Group-commit batch-size / linger counters (CONCEPT:EG-KG.backend.adaptive-linger-coalesce), per shard.
    stats: Arc<RedbCommitStats>,
    /// Value-blob cipher for snapshot reads off the writer (CONCEPT:EG-KG.storage.snapshot-read-off-writer). The same
    /// cipher the writer thread owns; resolved ONCE at open. `None` ⇒ encryption off ⇒
    /// the read path is byte-for-byte the plaintext path.
    #[cfg(feature = "security")]
    cipher: Option<crate::crypto::ValueCipher>,
    /// Transaction-recovery-plan cipher (D-ORC-50, CONCEPT:EG-KG.txn.multi-op-occ-acid) —
    /// DELIBERATELY SEPARATE from `cipher` above. Resolved from
    /// `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` (falling back to the shared data key when that
    /// alone is set — see `crypto::resolve_txn_recovery_key`), so an operator can
    /// unblock multi-op OCC `Commit` durability WITHOUT enabling at-rest encryption of
    /// ordinary node/edge/property values. Turning this on never changes `cipher`, so it
    /// never changes the durable value format existing rows were written in — no
    /// read-path migration is implied. `None` ⇒ multi-op transaction commits that stage
    /// more than one op (e.g. a compare-and-set touching a node property + an ANN
    /// vector) fail durability with a "configure a key" error, same as before this seam.
    #[cfg(feature = "security")]
    txn_recovery_cipher: Option<crate::crypto::ValueCipher>,
    handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl Shard {
    /// Open (or create) `db_path` and spawn its dedicated group-commit writer thread.
    fn open(
        db_path: String,
        thread_name: String,
        policy: DurabilityPolicy,
        capacity: usize,
        flush_threshold: usize,
    ) -> Result<Self, String> {
        // ONE shared `Database` handle per shard (CONCEPT:EG-KG.storage.snapshot-read-off-writer). The writer thread
        // and the snapshot-read path both hold a clone of this `Arc`; redb's MVCC lets
        // a `begin_read()` on this handle run concurrently with the writer's
        // `begin_write()`, so reads never route through the writer. (A SECOND
        // `Database::create` on the same file would error on the exclusive file lock —
        // hence one shared handle, not a re-open.)
        let db = Arc::new(Database::create(&db_path).map_err(|e| e.to_string())?);
        // Materialize the shared authoritative schema plus the server-only Raft
        // metadata table in one transaction. Read transactions cannot open a table
        // that has never been created.
        {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            crate::redb_store::initialize_canonical_tables(&wtx)?;
            wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
            wtx.commit().map_err(|e| e.to_string())?;
        }
        let (tx, rx) = sync_channel::<Cmd>(capacity.max(1));
        // Adaptive group-commit micro-linger config + observability (CONCEPT:EG-KG.backend.adaptive-linger-coalesce).
        // Resolved once at open (Configuration discipline); the writer thread owns a
        // clone of the stats Arc so callers can read batch-size/throughput live.
        let group_commit = RedbGroupCommitConfig::from_env();
        let stats = Arc::new(RedbCommitStats::default());
        let stats_writer = stats.clone();
        // Encryption-at-rest (CONCEPT:EG-KG.sharding.row-level-security): resolve the value-blob cipher ONCE at
        // open from EPISTEMIC_GRAPH_ENCRYPTION_KEY (the KMS seam). `None` ⇒ encryption
        // OFF ⇒ the durable format + write/read paths are byte-for-byte unchanged.
        #[cfg(feature = "security")]
        let cipher = crate::crypto::ValueCipher::from_env();
        #[cfg(feature = "security")]
        if cipher.is_some() {
            tracing::info!(
                "redb encryption-at-rest ENABLED (value blobs sealed with ChaCha20-Poly1305)"
            );
        }
        // Keep a clone of the cipher for the snapshot-read path (CONCEPT:EG-KG.storage.snapshot-read-off-writer); the
        // writer thread takes ownership of the original below.
        #[cfg(feature = "security")]
        let cipher_for_reads = cipher.clone();
        // Transaction-recovery-plan cipher (D-ORC-50) — resolved SEPARATELY from the
        // data-at-rest cipher above so enabling multi-op OCC transaction durability never
        // implies (and never requires) enabling at-rest encryption of existing plaintext
        // values. See `crypto::TXN_RECOVERY_KEY_ENV` for the full rationale.
        #[cfg(feature = "security")]
        let txn_recovery_cipher = crate::crypto::ValueCipher::from_env_for_txn_recovery();
        #[cfg(feature = "security")]
        if txn_recovery_cipher.is_some() && cipher_for_reads.is_none() {
            tracing::info!(
                "redb transaction-recovery-plan sealing ENABLED via a dedicated key \
                 (EPISTEMIC_GRAPH_TXN_RECOVERY_KEY) — data-at-rest encryption remains OFF"
            );
        }
        // A `Weak` for the off-writer snapshot-read path (CONCEPT:EG-KG.storage.snapshot-read-off-writer). The writer
        // thread below takes the SOLE STRONG `Arc`, so the redb file lock releases
        // exactly when that thread exits on shutdown — matching the pre-EG-027 lifetime
        // (a reopen after shutdown succeeds; a read after shutdown upgrades to `None`).
        let db_weak = Arc::downgrade(&db);
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run(
                    rx,
                    db,
                    policy,
                    group_commit,
                    stats_writer,
                    flush_threshold,
                    #[cfg(feature = "security")]
                    cipher,
                )
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            db_path,
            db: db_weak,
            tx,
            stats,
            #[cfg(feature = "security")]
            cipher: cipher_for_reads,
            #[cfg(feature = "security")]
            txn_recovery_cipher,
            handle: parking_lot::Mutex::new(Some(handle)),
        })
    }

    /// Stop this shard's writer thread (flush + join). Idempotent.
    fn shutdown(&self) {
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            let (reply, rx) = std::sync::mpsc::channel();
            if self.tx.send(Cmd::Shutdown { reply }).is_ok() {
                let _ = rx.recv();
            }
            let _ = handle.join();
        }
    }
}

/// Handle to the redb write-through tier (CONCEPT:EG-KG.storage.kg-kg / EG-026). The dispatch
/// path holds an `Arc` of this and calls `record`/`record_durable`; each routes by
/// graph to one of K independent single-writer [`Shard`]s, so K cores commit in
/// parallel. K=1 holds exactly one shard backed by canonical `graph-0.redb`.
pub struct RedbBackend {
    /// The K shards (len >= 1). Index `shard_index(graph_fname, K)` owns a graph.
    shards: Vec<Shard>,
    /// Optional tenant catalog OVERRIDE for graph→shard routing (CONCEPT:EG-KG.sharding.empty-catalog-routing, M3).
    /// `None` (the default) ⇒ pure EG-026 FNV-1a routing, byte-for-byte unchanged. When
    /// `Some` AND it holds an explicit entry for a graph, that entry's shard wins
    /// (enabling rebalanceable / resharded placement); a graph with no entry STILL
    /// falls back to FNV-1a inside `TenantCatalog::resolve_shard`. So an empty catalog
    /// is indistinguishable from no catalog — the seam never destabilizes EG-026.
    catalog: Option<Arc<crate::server::persistence::tenant_catalog::TenantCatalog>>,
    /// Routing quiesce barrier for online resharding (CONCEPT:EG-KG.backend.catalog-shard-resolve). Catalog-attached
    /// durable writes resolve their shard + enqueue their op while holding a SHARED READ
    /// guard; [`RedbBackend::reshard_graph`] holds the EXCLUSIVE WRITE guard across a
    /// graph's move, so the route flip can never interleave a write (no lost / misrouted
    /// rows). When NO catalog is attached (the default) the write path never touches this
    /// — EG-026 is byte-for-byte unchanged.
    routing_epoch: Arc<RwLock<()>>,
    /// Local durable projection of cluster-wide/admin saga authority. In clustered
    /// serving every writer is routed through the placement Raft group, so this file
    /// is replayable consensus state on every group member rather than a pod-local
    /// coordinator. Single-node serving uses the same image directly.
    admin_mutations: Arc<Database>,
    /// Durable cluster-topology self-report store (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1).
    /// Always opened (like `admin_mutations` above) so the shape of `RedbBackend`
    /// doesn't vary with whether Raft happens to be configured; it is populated
    /// only when a clustered node self-reports at startup (`raft::node::start`).
    /// See `server::persistence::node_info_store` for the replication story.
    node_info: Arc<super::node_info_store::NodeInfoStore>,
}

impl RedbBackend {
    /// Open (or create) the sharded durable tier under `persist_dir` and spawn one
    /// off-reactor group-commit writer thread per shard (CONCEPT:EG-KG.backend.sharded-k-way-durable). The shard
    /// count K is auto-sized (`resolve_shard_count`) and reconciled against any
    /// existing current on-disk layout. The exclusive per-file redb lock for every
    /// shard is acquired here at open.
    pub fn open(
        persist_dir: String,
        policy: DurabilityPolicy,
        capacity: usize,
    ) -> Result<Self, String> {
        let backend =
            Self::open_with_shards(persist_dir.clone(), policy, capacity, resolve_shard_count())?;
        Ok(backend.maybe_attach_catalog_from_env(&persist_dir))
    }

    /// Catalog auto-attach gate (CONCEPT:EG-KG.sharding.r5-feature, R5). At startup attach the durable tenant
    /// catalog to the LIVE routing seam when `EPISTEMIC_GRAPH_TENANT_CATALOG=1` is set OR a
    /// durable `catalog.redb` already exists (a populated catalog from a prior run must be
    /// honored). When NEITHER holds — the default — NO catalog is attached and routing is
    /// byte-for-byte EG-026 FNV-1a. An attached-but-EMPTY catalog also routes identically
    /// (`resolve_shard` == `shard_index`), so turning the flag on is a no-op until an
    /// online reshard assigns a placement. Only [`Self::open`] (the live boot path) calls
    /// this; the explicit-K test constructor [`Self::open_with_shards`] never auto-attaches.
    fn maybe_attach_catalog_from_env(self, persist_dir: &str) -> Self {
        let flag = std::env::var("EPISTEMIC_GRAPH_TENANT_CATALOG")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes" || v == "on"
            })
            .unwrap_or(false);
        let catalog_exists = std::path::Path::new(persist_dir)
            .join("catalog.redb")
            .exists();
        if !flag && !catalog_exists {
            return self; // DEFAULT: pure EG-026, no catalog, no behavior change.
        }
        match crate::server::persistence::tenant_catalog::TenantCatalog::open(persist_dir) {
            Ok(cat) => {
                if cat.is_empty() {
                    tracing::info!(
                        "tenant catalog attached (CONCEPT:EG-KG.sharding.r5-feature) — empty ⇒ pure EG-026 routing \
                         until an online reshard assigns a placement"
                    );
                } else {
                    tracing::info!(
                        "tenant catalog attached (CONCEPT:EG-KG.sharding.r5-feature) — {} explicit placement(s) \
                         override EG-026 hash routing",
                        cat.len()
                    );
                }
                self.with_catalog(Arc::new(cat))
            }
            Err(e) => {
                tracing::warn!(
                    "tenant catalog open failed ({e}); continuing with pure EG-026 routing"
                );
                self
            }
        }
    }

    /// Open with an EXPLICIT requested shard count (CONCEPT:EG-KG.backend.sharded-k-way-durable). Used by `open`
    /// (auto-sized K) and by the sharding tests (deterministic K). The requested K is
    /// still reconciled against the on-disk layout so an existing dir's K wins.
    pub fn open_with_shards(
        persist_dir: String,
        policy: DurabilityPolicy,
        capacity: usize,
        requested_k: usize,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(&persist_dir).map_err(|e| e.to_string())?;
        let requested_k = crate::redb_layout::validate_shard_count(requested_k)?;
        let k = crate::redb_layout::reconcile_shard_layout(
            std::path::Path::new(&persist_dir),
            requested_k,
        )?;
        if k != requested_k {
            tracing::warn!(
                "redb: persist dir has {k} canonical shard file(s) but K={requested_k} requested; \
                 using the on-disk K (changing it requires an offline migration)"
            );
        }
        let flush_threshold = resolve_flush_threshold(capacity);
        if k > 1 {
            tracing::info!(
                "redb: sharded durable writer — K={k} graph-<n>.redb files, {k} writer threads \
                 (flush_threshold={flush_threshold})"
            );
        }
        // D-CDX-65: open all K shards CONCURRENTLY instead of one at a time. Each
        // shard's `Database::create` (redb's own header/allocator validation pass over
        // its file, proportional to file size, not row count) is completely independent
        // of every other shard until the `Shard` structs are collected below — no
        // cross-shard state is touched during open. The prior sequential loop paid
        // sum(per-shard open time) with ZERO log lines in between (a multi-minute
        // startup gap that reads as "dead" — see D-CDX-65: a live incident measured
        // 5m26s of total silence loading 4 shards totalling ~10 GB). Opening
        // concurrently instead pays max(per-shard open time), and each shard now logs
        // its own start/duration so a still-loading start is visibly progressing rather
        // than silent. On the common K=1 deployment this is one thread, so the shape
        // and cost are unchanged.
        let shard_open_start = std::time::Instant::now();
        let mut shard_specs = Vec::with_capacity(k);
        for i in 0..k {
            let db_path = std::path::Path::new(&persist_dir)
                .join(shard_filename(i))
                .to_string_lossy()
                .to_string();
            // The single writer uses the unsuffixed thread name.
            let thread_name = if k <= 1 {
                "eg-redb-writer".to_string()
            } else {
                format!("eg-redb-writer-{i}")
            };
            shard_specs.push((i, db_path, thread_name));
        }
        let opened: Vec<Result<Shard, String>> = std::thread::scope(|scope| {
            let handles: Vec<_> = shard_specs
                .into_iter()
                .map(|(i, db_path, thread_name)| {
                    scope.spawn(move || {
                        let bytes_on_disk = std::fs::metadata(&db_path).map(|m| m.len()).ok();
                        tracing::info!(
                            "redb: opening shard {i}/{k} ({db_path}, {} bytes on disk) ...",
                            bytes_on_disk
                                .map(|n| n.to_string())
                                .unwrap_or_else(|| "new".to_string())
                        );
                        let t0 = std::time::Instant::now();
                        let result = Shard::open(
                            db_path.clone(),
                            thread_name,
                            policy,
                            capacity,
                            flush_threshold,
                        );
                        match &result {
                            Ok(_) => tracing::info!(
                                "redb: shard {i}/{k} open finished in {:?}",
                                t0.elapsed()
                            ),
                            Err(e) => tracing::warn!(
                                "redb: shard {i}/{k} open FAILED after {:?}: {e}",
                                t0.elapsed()
                            ),
                        }
                        result
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .unwrap_or_else(|_| Err("redb shard-open thread panicked".to_string()))
                })
                .collect()
        });
        let mut shards = Vec::with_capacity(k);
        for shard in opened {
            shards.push(shard?);
        }
        if k > 1 {
            tracing::info!(
                "redb: all {k} shard(s) open in {:?} (wall clock; ran concurrently)",
                shard_open_start.elapsed()
            );
        }
        let admin_mutations = Arc::new(
            Database::create(std::path::Path::new(&persist_dir).join("admin-mutations.redb"))
                .map_err(|e| e.to_string())?,
        );
        eg_mutation_store::initialize(&admin_mutations)?;
        let node_info = Arc::new(super::node_info_store::NodeInfoStore::open(&persist_dir)?);
        Ok(Self {
            shards,
            catalog: None,
            routing_epoch: Arc::new(RwLock::new(())),
            admin_mutations,
            node_info,
        })
    }

    /// Attach a tenant catalog to OVERRIDE graph→shard routing (CONCEPT:EG-KG.sharding.empty-catalog-routing, M3).
    /// Builder-style so the open path stays untouched; default (no call) = pure EG-026.
    pub fn with_catalog(
        mut self,
        catalog: Arc<crate::server::persistence::tenant_catalog::TenantCatalog>,
    ) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// The attached tenant catalog, if any (CONCEPT:EG-KG.sharding.r5-feature). `None` ⇒ pure EG-026. The
    /// admin/API surface uses this to populate/persist placements (`assign`/`reassign`/
    /// `remove`); a placement change that must also MOVE the graph's rows goes through
    /// [`Self::reshard_graph`] instead (which flips the route AND migrates the data).
    pub fn catalog(
        &self,
    ) -> Option<Arc<crate::server::persistence::tenant_catalog::TenantCatalog>> {
        self.catalog.clone()
    }

    pub(crate) fn admin_mutation_store(&self) -> &Database {
        &self.admin_mutations
    }

    /// The durable cluster-topology self-report store (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1).
    /// Always present (see the field doc); empty on a single-node deployment that
    /// never ran a clustered startup.
    pub(crate) fn node_info(&self) -> Arc<super::node_info_store::NodeInfoStore> {
        self.node_info.clone()
    }

    /// Stable transaction-recovery-plan cipher handle resolved when the durable backend
    /// opened (D-ORC-50). Private transaction staging (the parent plan, cross-shard
    /// prepares, and coordinator recovery plans in `server::dispatch`) uses this exact
    /// cipher, not a second environment read, so every private-payload site in one
    /// process shares one configured recovery authority for its lifetime.
    ///
    /// DELIBERATELY NOT `self.shard0().cipher` (the data-at-rest cipher): that field
    /// controls the on-disk format of every ordinary node/edge/property value blob,
    /// including ones already durably written before any key existed. Returning it here
    /// would mean "configure durability for a multi-op transaction" and "expect every
    /// existing value in the store to already be sealed" are the same switch — which is
    /// exactly the destructive-read failure mode D-ORC-50 found (enabling the shared key
    /// on a populated plaintext store made every plaintext read fail with "encrypted
    /// durable value is missing sealed framing"). `shard0().txn_recovery_cipher` is
    /// resolved from its own env var (falling back to the shared key only when the
    /// dedicated one is absent), so it can be turned on independently.
    #[cfg(feature = "security")]
    pub(crate) fn transaction_recovery_cipher(&self) -> Option<crate::crypto::ValueCipher> {
        self.shard0().txn_recovery_cipher.clone()
    }

    /// Move ONE graph's rows from its current shard to `dst_shard` while the engine RUNS,
    /// then flip the catalog route (CONCEPT:EG-KG.backend.catalog-shard-resolve — the M3 keystone). Requires an attached
    /// tenant catalog (CONCEPT:EG-KG.sharding.r5-feature / R5). No data loss, single-writer-per-shard
    /// correctness, and audit-chain validity all hold across the move; other graphs are
    /// never touched. See [`super::online_reshard`] for the verbatim copy + crash-ordering.
    ///
    /// `graph_fname` must already be `sanitize`d (the durable key). `dst_shard` is clamped
    /// into `0..K`. A graph already on the target shard is a no-op.
    pub async fn reshard_graph(
        &self,
        graph_fname: &str,
        dst_shard: u32,
    ) -> Result<super::online_reshard::ReshardReport, String> {
        let catalog = self.catalog.clone().ok_or_else(|| {
            "online reshard requires an attached tenant catalog \
             (set EPISTEMIC_GRAPH_TENANT_CATALOG=1)"
                .to_string()
        })?;
        let k = self.shards.len().max(1);
        let dst_idx = (dst_shard as usize) % k;
        let src_idx = catalog.resolve_shard(graph_fname, k);
        if src_idx == dst_idx {
            return Ok(super::online_reshard::ReshardReport::no_op(
                graph_fname,
                src_idx,
            ));
        }
        let src_tx = self.shards[src_idx].tx.clone();
        let dst_tx = self.shards[dst_idx].tx.clone();
        let graph = graph_fname.to_string();

        // CONCEPT:EG-KG.backend.flush-pending-first (R1 delta-copy) — SNAPSHOT + DELTA to shrink the moved graph's
        // write-pause. PHASE 1 copies the BULK verbatim off a src read snapshot WITHOUT
        // the exclusive routing quiesce, so writes keep flowing to `src` while the (large)
        // copy runs — the graph is NOT paused. PHASE 2 takes the exclusive `routing_epoch`
        // WRITE guard (quiescing only THIS catalog's durable writes) and copies just the
        // small DELTA accumulated during phase 1, flips the route, and GCs the source. The
        // pause is therefore O(delta), not O(graph). Crash-consistency is preserved:
        // import(bulk) committed -> import(delta) committed -> catalog flip durable ->
        // purge(src) (a crash before the flip leaves the data on `src` where the route
        // still points; after the flip on `dst` where both bulk+delta already landed).
        let (s1, d1, g1) = (src_tx.clone(), dst_tx.clone(), graph.clone());
        let bulk =
            tokio::task::spawn_blocking(move || super::online_reshard::bulk_copy(&s1, &d1, &g1))
                .await
                .map_err(|e| format!("reshard bulk join error: {e}"))??;

        // Exclusive routing quiesce held ONLY across the delta + flip (the small window):
        // no catalog-attached write can resolve/enqueue while the route flips, so the flip
        // never loses or misroutes a write; once released, every write resolves the catalog
        // AFTER the flip and follows the graph to `dst`.
        let quiesce = self.routing_epoch.clone().write_owned().await;
        tokio::task::spawn_blocking(move || {
            let _held = quiesce;
            super::online_reshard::delta_flip_purge(
                &src_tx, &dst_tx, &catalog, &graph, src_idx, dst_idx, bulk,
            )
        })
        .await
        .map_err(|e| format!("reshard delta join error: {e}"))?
    }

    /// Execute a rebalance PLAN move-by-move via online resharding (CONCEPT:EG-KG.backend.r3-plan-execution, R3
    /// plan execution). Each move is one [`Self::reshard_graph`] — online, ONE graph at a
    /// time, every other graph unaffected. The plan's `from_shard` is informational: each
    /// move resolves its source from the catalog's CURRENT state, so applying the moves in
    /// order is robust even as earlier moves shift placements. Returns the per-move reports.
    /// Requires an attached tenant catalog (every `reshard_graph` does).
    pub async fn rebalance_execute(
        &self,
        plan: &super::rebalance::RebalancePlan,
    ) -> Result<Vec<super::online_reshard::ReshardReport>, String> {
        let mut reports = Vec::with_capacity(plan.moves.len());
        for mv in &plan.moves {
            reports.push(self.reshard_graph(&mv.graph, mv.to_shard).await?);
        }
        Ok(reports)
    }

    /// The shard that owns `graph_fname` (stable routing, CONCEPT:EG-KG.backend.sharded-k-way-durable / EG-031).
    ///
    /// Routing seam: when a tenant catalog is attached AND holds an explicit entry for
    /// this graph, the catalog's shard wins (M3 rebalanceable placement). Otherwise —
    /// no catalog, or a graph the catalog has no entry for — this is the unchanged
    /// EG-026 `FNV-1a(graph_fname) % K`. `resolve_shard` folds both cases + clamps to
    /// the live shard count, so the override can never index out of range.
    fn shard_for(&self, graph_fname: &str) -> &Shard {
        let idx = match &self.catalog {
            Some(cat) => cat.resolve_shard(graph_fname, self.shards.len()),
            None => shard_index(graph_fname, self.shards.len()),
        };
        &self.shards[idx]
    }

    /// Flush and export one graph's complete durable authority for a Raft
    /// snapshot. The catalog read guard keeps routing stable from shard resolve
    /// through the writer-thread snapshot.
    pub(crate) async fn export_graph_raw_for_snapshot(
        &self,
        graph_fname: &str,
    ) -> Result<super::online_reshard::RawGraphRows, String> {
        let routing_guard = if self.catalog.is_some() {
            Some(self.routing_epoch.clone().read_owned().await)
        } else {
            None
        };
        let tx = self.shard_for(graph_fname).tx.clone();
        let graph = graph_fname.to_string();
        tokio::task::spawn_blocking(move || {
            let _routing_guard = routing_guard;
            let (reply, receive) = std::sync::mpsc::channel();
            tx.send(Cmd::ExportGraphRaw { graph, reply })
                .map_err(|_| "redb writer thread is gone".to_string())?;
            receive
                .recv()
                .map_err(|_| "redb writer dropped snapshot export reply".to_string())?
        })
        .await
        .map_err(|error| format!("snapshot export join error: {error}"))?
    }

    /// Atomically replace one graph's complete durable authority while installing
    /// a Raft snapshot. The imported rows include MutationBatch replay/outbox and
    /// governed ChangeEnvelope material, not just the graph projection.
    pub(crate) async fn import_graph_raw_from_snapshot(
        &self,
        graph_fname: &str,
        rows: super::online_reshard::RawGraphRows,
    ) -> Result<(), String> {
        let routing_guard = if self.catalog.is_some() {
            Some(self.routing_epoch.clone().read_owned().await)
        } else {
            None
        };
        let tx = self.shard_for(graph_fname).tx.clone();
        let graph = graph_fname.to_string();
        tokio::task::spawn_blocking(move || {
            let _routing_guard = routing_guard;
            let (reply, receive) = std::sync::mpsc::channel();
            tx.send(Cmd::ImportGraphRaw {
                graph,
                rows: Box::new(rows),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
            receive
                .recv()
                .map_err(|_| "redb writer dropped snapshot import reply".to_string())?
        })
        .await
        .map_err(|error| format!("snapshot import join error: {error}"))?
    }

    /// Shard 0 — the single shard under K=1 and the home of GLOBAL (non-per-graph)
    /// durable records: the Raft log/meta + cross-shard 2PC + materialized views.
    /// Under the `raft` feature K is forced to 1, so shard 0 is the only shard and
    /// these stay single-writer-correct (multi-Raft sharding is M2).
    fn shard0(&self) -> &Shard {
        &self.shards[0]
    }

    /// The shard that owns Raft group `group_id` (ADR-2 / W1.2, `reports/wave1/ADR-scale-trio.md`
    /// §ADR-2 decision 1: **raft group *g* owns redb shard *g***). A group's durable log +
    /// vote + applied-state (keyed `(group_id, …)`) live in THIS shard's file, co-located
    /// with the graph data of every graph the router maps to the group — so one group's
    /// apply loop is one shard's single writer, and the EG-KG.storage.one-fsync-covers-raft
    /// coalescing holds per group. Group ids are not required to be dense `0..K` (the
    /// harness uses 100/200), so the mapping is `group_id % K`; under the production
    /// `configure_group_ring` (`0..K`) with K == N it reduces to the identity `g → shard g`.
    /// `K == 1` collapses every group onto `graph-0.redb` — byte-for-byte the pre-ADR-2
    /// single-shard behavior an un-migrated store keeps.
    fn shard_for_group(&self, group_id: u64) -> &Shard {
        &self.shards[(group_id as usize) % self.shards.len()]
    }

    /// Number of durable shards K (CONCEPT:EG-KG.backend.sharded-k-way-durable).
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// The persist dir this store lives in (CONCEPT:EG-KG.sharding.reshard-on-restore) — derived from shard 0's
    /// file path parent. Used by the live restore RPC to stage a rebuilt copy beside the
    /// running store (an in-place restore needs the engine stopped — the file lock).
    pub fn persist_dir(&self) -> Option<std::path::PathBuf> {
        std::path::Path::new(&self.shard0().db_path)
            .parent()
            .map(|p| p.to_path_buf())
    }

    /// Take an ONLINE consistent backup of the whole durable store into `dst_dir`
    /// (CONCEPT:EG-KG.sharding.reshard-on-restore), while the engine keeps serving. Per shard, opens a
    /// `Database::begin_read()` MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer) on the LIVE writer's
    /// shared handle and streams every table verbatim into a bundle shard file named by
    /// the EG-026 [`shard_filename`] scheme, then writes a `MANIFEST.json`
    /// ([`super::backup::BackupManifest`]). No quiesce: MVCC lets the snapshot read the
    /// shard's latest committed state concurrently with the writer, and commit-before-ack
    /// (CONCEPT:EG-KG.backend.authoritative-dispatch) makes each per-shard snapshot a self-consistent committed prefix.
    ///
    /// `engine_version` / `timestamp_secs` / `label` are CALLER-SUPPLIED — this library
    /// never reads the wall clock. `dst_dir` is created if absent and must not already
    /// hold bundle shard files (it refuses to overwrite).
    pub fn backup(
        &self,
        dst_dir: &std::path::Path,
        engine_version: &str,
        timestamp_secs: u64,
        label: &str,
    ) -> Result<super::backup::BackupReport, String> {
        use super::backup;
        std::fs::create_dir_all(dst_dir).map_err(|e| e.to_string())?;
        let admin_boundary_before =
            eg_mutation_store::recovery_store_fingerprint(&self.admin_mutations)?;
        let shard0 = self
            .shard0()
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let xshard_boundary_before = backup::xshard_recovery_fingerprint(&shard0)?;
        let k = self.shards.len();
        let mut report = backup::BackupReport {
            shards: k,
            ..Default::default()
        };
        for (i, shard) in self.shards.iter().enumerate() {
            // Upgrade the `Weak` to the writer's shared `Database` (CONCEPT:EG-KG.storage.snapshot-read-off-writer).
            // `None` only after shutdown dropped the writer's strong Arc.
            let db = shard
                .db
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            let dst_path = dst_dir.join(shard_filename(i));
            let counts = backup::write_bundle_shard(&db, &dst_path, i == 0)?;
            report.add_shard(counts);
        }
        report.admin_mutations = eg_mutation_store::backup_recovery_store(
            &self.admin_mutations,
            &dst_dir.join(backup::ADMIN_MUTATIONS_FILE),
        )?;
        let admin_boundary_after =
            eg_mutation_store::recovery_store_fingerprint(&self.admin_mutations)?;
        let xshard_boundary_after = backup::xshard_recovery_fingerprint(&shard0)?;
        if admin_boundary_before != admin_boundary_after
            || xshard_boundary_before != xshard_boundary_after
        {
            return Err(
                "recovery coordinator changed during backup; bundle remains unpublished"
                    .to_string(),
            );
        }
        backup::write_manifest(dst_dir, &report, engine_version, timestamp_secs, label)?;
        tracing::info!(
            "online backup complete: {} shards, {} graphs",
            report.shards,
            report.graphs
        );
        Ok(report)
    }

    /// Group-commit batch-size / linger counters (CONCEPT:EG-KG.backend.adaptive-linger-coalesce). Returns shard 0's
    /// LIVE counter Arc (the only shard under K=1; observability callers are K=1). Use
    /// [`commit_stats_all`] for the per-shard view under K>1.
    pub fn commit_stats(&self) -> Arc<RedbCommitStats> {
        self.shard0().stats.clone()
    }

    /// Per-shard group-commit counters (CONCEPT:EG-KG.backend.sharded-k-way-durable observability).
    pub fn commit_stats_all(&self) -> Vec<Arc<RedbCommitStats>> {
        self.shards.iter().map(|s| s.stats.clone()).collect()
    }

    /// On-disk file path of each shard's redb database (CONCEPT:EG-KG.backend.sharded-k-way-durable diagnostics).
    pub fn shard_db_paths(&self) -> Vec<String> {
        self.shards.iter().map(|s| s.db_path.clone()).collect()
    }

    /// TEST-ONLY: flip a byte in the stored audit entry `(graph, seq)` to simulate
    /// tampering, so the verify path can prove detection. Routed through the owner
    /// thread (exclusive file lock).
    #[cfg(all(test, feature = "security"))]
    pub fn test_tamper_audit_entry(&self, graph_fname: &str, seq: u64) -> Result<(), String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::TestTamperAudit {
                graph: graph_fname.to_string(),
                seq,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped tamper reply".to_string())?
    }

    /// Verify ONE graph's tamper-evident hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security).
    /// Routed through the owner thread (exclusive file lock), which flushes pending
    /// writes first so the walk reflects the latest durable entries.
    #[cfg(feature = "security")]
    pub fn audit_verify_blocking(
        &self,
        graph_fname: &str,
    ) -> Result<crate::protocol::AuditReport, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::AuditVerify {
                graph: graph_fname.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped audit_verify reply".to_string())?
    }

    /// Off-writer-thread read: hash each of `node_ids`' CURRENT durable content
    /// into a provenance leaf hash (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring). Lock-free
    /// MVCC snapshot read (mirrors `read_node_blocking`) — never touches the
    /// writer channel, so hashing a large window costs the writer thread nothing.
    #[cfg(feature = "security")]
    pub fn provenance_leaf_hashes_blocking(
        &self,
        graph_fname: &str,
        node_ids: &[String],
    ) -> Result<Vec<(String, crate::audit::Hash)>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        crate::redb_store::provenance_leaf_hashes(&db, graph_fname, node_ids, crypto)
    }

    /// Durably anchor an already-hashed provenance window (CONCEPT:EG-KG.sharding.row-level-security,
    /// provenance anchoring). Routed through the owner thread (exclusive file
    /// lock) since it may write; `Ok(None)` means the root was unchanged and
    /// nothing was written — the common case for an idle graph.
    #[cfg(feature = "security")]
    pub fn provenance_anchor_commit_blocking(
        &self,
        graph_fname: &str,
        root: crate::audit::Hash,
        members: Vec<(String, crate::audit::Hash)>,
    ) -> Result<Option<u64>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::ProvenanceAnchorCommit {
                graph: graph_fname.to_string(),
                root,
                members,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped provenance_anchor_commit reply".to_string())?
    }

    /// Produce + verify a Merkle inclusion proof for one node against a prior
    /// provenance anchor (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring). Routed through
    /// the owner thread (exclusive file lock), which flushes pending writes first.
    #[cfg(feature = "security")]
    pub fn audit_prove_inclusion_blocking(
        &self,
        graph_fname: &str,
        node_id: &str,
        anchor_seq: Option<u64>,
    ) -> Result<crate::protocol::MerkleInclusionReport, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::AuditProveInclusion {
                graph: graph_fname.to_string(),
                node_id: node_id.to_string(),
                anchor_seq,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped audit_prove_inclusion reply".to_string())?
    }

    /// Read ONE graph's durable rows back as an owned dump (CONCEPT:EG-KG.storage.100m-tenant — tenant
    /// rehydration). Routed through the owner thread (exclusive file lock) which
    /// flushes pending writes first. `None` ⇒ the graph has no durable identity.
    pub fn read_graph_dump_blocking(&self, graph_fname: &str) -> Result<Option<GraphDump>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::ReadGraphDump {
                graph: graph_fname.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped read_graph_dump reply".to_string())?
    }

    /// Read ONE bounded page of one graph's durable rows (CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged
    /// adjacency") — the memory-bounded sibling of [`Self::read_graph_dump_blocking`]
    /// backing [`PersistenceBackend::read_graph_material_page_blocking`] below.
    pub(crate) fn read_graph_dump_page_blocking(
        &self,
        graph_fname: &str,
        node_offset: usize,
        edge_offset: usize,
        node_after: Option<String>,
        edge_after: Option<(String, String, u32)>,
        page_size: usize,
    ) -> Result<Option<crate::redb_store::GraphDumpPage>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::ReadGraphDumpPage {
                graph: graph_fname.to_string(),
                query: Box::new(PageQuery {
                    node_offset,
                    edge_offset,
                    node_after,
                    edge_after,
                    page_size,
                }),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped read_graph_dump_page reply".to_string())?
    }

    /// Reconstruct every graph from the redb store into the registry. The actual
    /// DB read runs on the owner thread (via the `Load` command) because redb holds
    /// an exclusive per-process file lock; this rebuilds each `GraphCore` from the
    /// returned dumps via the SAME `add_node`/`add_edge` calls the WAL replay uses.
    async fn load_into(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        // PARALLEL cross-shard read fan-out (CONCEPT:AU-KG.backend.roadmap-f-parallel-cross, roadmap F). Each shard's
        // writer owns only the graphs routed to it, so the registry is rebuilt from the
        // union of all K shards' dumps. Instead of routing each shard's dump SERIALLY
        // through its writer thread's `Cmd::Load` channel, each shard now dumps OFF its
        // OWN `begin_read()` MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer) on the blocking pool, so the
        // K reads run CONCURRENTLY on K cores and NEVER touch a writer thread (the EG-027
        // invariant — a read never forces a group-commit nor serializes behind a write).
        //
        // Consistency: redb is MVCC so each snapshot sees its shard's LATEST COMMITTED
        // state. `load_all` runs at boot BEFORE serving (no concurrent writes), and even
        // under concurrency commit-before-ack (KG-2.187) guarantees any ACKED write is
        // already committed and thus visible — exactly the EG-027 `read_node` reasoning.
        // One closure per shard captures its upgraded `Database` + cipher; build them ALL
        // first, then await them, so the fan-out overlaps (a spawn-then-await-each loop
        // would serialize). The `Cmd::Load` writer-thread path is left intact but unused.
        let mut tasks = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let db = shard
                .db
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            #[cfg(feature = "security")]
            let cipher = shard.cipher.clone();
            tasks.push(move || {
                #[cfg(feature = "security")]
                let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
                #[cfg(not(feature = "security"))]
                let crypto = crate::redb_store::DurableCrypto::none();
                read_all_dumps(&db, crypto)
            });
        }
        let dumps: Vec<GraphDump> = join_blocking_in_order(tasks)
            .await?
            .into_iter()
            .flatten()
            .collect();

        let mut count = 0usize;
        for dump in dumps {
            // Create the live graph (or reuse it) and grab its core.
            let core: Arc<GraphCore> = {
                let mut s = state.write().await;
                if !s.registry.exists(&dump.name) {
                    let _ = s.registry.create_graph_with_incarnation(
                        &dump.name,
                        dump.graph_type,
                        None,
                        dump.incarnation_id.clone(),
                        dump.source_snapshot_version,
                    );
                }
                match s.registry.get_mut(&dump.name).map(|e| e.core.clone()) {
                    Some(c) => c,
                    None => continue,
                }
            };
            // Rebuild via the SAME add_node/add_edge calls the WAL replay uses —
            // these regenerate the ledger as a side effect, so the `ledger` table is
            // only a durable mirror (not separately replayed) to avoid double-
            // applying mutations.
            core.install_integrity_policy(dump.integrity_policy);
            for (id, props) in dump.nodes {
                core.add_node(id, props);
            }
            for (src, tgt, props) in dump.edges {
                let _ = core.add_edge(src, tgt, props);
            }
            // Semantic store restores directly onto the public RwLock field (same
            // destination `from_msgpack` writes).
            if !dump.semantic.is_empty() {
                if let Ok(store) = decode_durable_semantic(&dump.semantic) {
                    *core.semantic_store.write() = store;
                }
            }
            count += 1;
        }
        Ok(count)
    }

    /// Populate the registry's CATALOG from every shard's `graph_meta` table ONLY
    /// (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — NO node/edge/ledger/semantic row is
    /// read. Mirrors `load_into`'s parallel cross-shard fan-out (each shard's
    /// cheap meta scan runs concurrently on the blocking pool) but with a vastly
    /// smaller per-shard read: one small `{name, graph_type}` table instead of
    /// four. `register_catalog_only` takes `&self` on the registry (a `DashMap`
    /// under the hood), so this only needs a READ lock on `ServerState` — booting
    /// with millions of persisted graphs costs one shared-lock scan, not a
    /// per-graph write-lock round-trip.
    async fn load_catalog_into(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        // Write-new half of the one-time graph_meta migration, before the read
        // below. A store written by a pre-versioned build has rows the current
        // record cannot decode; `read_all_graph_meta` can now READ them via the
        // legacy fallback, but leaving them on disk in the old shape would mean
        // taking that path on every subsequent open. Converting here makes the
        // fallback genuinely one-time (and is a no-op on an already-current
        // store, so it costs one read pass per shard at startup and nothing else).
        let mut upgrade_tasks = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let db = shard
                .db
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            upgrade_tasks.push(move || crate::redb_store::upgrade_legacy_graph_meta(&db));
        }
        let upgraded: usize = join_blocking_in_order(upgrade_tasks)
            .await?
            .into_iter()
            .sum();
        if upgraded > 0 {
            tracing::info!(
                "redb: migrated {upgraded} graph metadata row(s) from the pre-versioned \
                 format to schema v{}",
                crate::redb_store::graph_meta_schema_version()
            );
        }

        let mut tasks = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let db = shard
                .db
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            tasks.push(move || read_all_graph_meta(&db));
        }
        let rows: Vec<(String, String, GraphType, String)> = join_blocking_in_order(tasks)
            .await?
            .into_iter()
            .flatten()
            .collect();

        let count = rows.len();
        let s = state.read().await;
        for (_fname, name, graph_type, incarnation_id) in rows {
            s.registry.register_catalog_only_with_incarnation(
                &name,
                graph_type,
                None,
                incarnation_id,
            );
        }
        Ok(count)
    }
}

/// Rebuild a live [`GraphCore`] from a durable [`GraphDump`] (CONCEPT:EG-KG.storage.100m-tenant —
/// tenant rehydration). Uses the SAME `add_node`/`add_edge`/semantic-restore path
/// `load_into` uses, so a rehydrated graph is byte-identical to a freshly loaded one.
/// The core is cleared first so a re-rehydrate is idempotent.
pub fn rehydrate_core_from_dump(core: &GraphCore, dump: &GraphDump) {
    core.clear();
    core.install_integrity_policy(dump.integrity_policy.clone());
    for (id, props) in &dump.nodes {
        core.add_node(id.clone(), props.clone());
    }
    for (src, tgt, props) in &dump.edges {
        let _ = core.add_edge(src.clone(), tgt.clone(), props.clone());
    }
    if !dump.semantic.is_empty() {
        if let Ok(store) = decode_durable_semantic(&dump.semantic) {
            *core.semantic_store.write() = store;
        }
    }
}

#[async_trait::async_trait]
impl PersistenceBackend for RedbBackend {
    fn supports_native_resource_reservations(&self) -> bool {
        true
    }

    async fn load_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        let n = self.load_into(state).await?;
        tracing::info!(
            "redb: loaded {} graph(s) from {} shard(s) under the persist dir",
            n,
            self.shards.len()
        );
        Ok(n)
    }

    /// Populate the registry's CATALOG ONLY (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — every
    /// graph's `{name, graph_type}` identity row, with NO node/edge/ledger/semantic
    /// data read. Each graph's `GraphCore` then materializes lazily on first access
    /// (`server::persistence::cold_offload::lazy_open`), via
    /// `read_through::BackendGraphMaterializer` calling
    /// [`Self::read_graph_material_blocking`] below. Served startup selects this
    /// catalog-first path unconditionally.
    async fn load_catalog(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        let n = self.load_catalog_into(state).await?;
        tracing::info!(
            "redb: catalog-loaded {} graph(s) from {} shard(s) — lazy startup, no node/edge \
             data read (CONCEPT:EG-KG.sharding.lazy-graph-catalog)",
            n,
            self.shards.len()
        );
        Ok(n)
    }

    /// SYNC durable-material fetch for a lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog,
    /// DIST-P2-3) — reuses [`Self::read_graph_dump_blocking`], the SAME per-graph
    /// rehydrate path `shard_migrate`/`backup` already use, so a lazily-opened
    /// graph replays byte-identically to an eagerly-loaded one.
    fn read_graph_material_blocking(
        &self,
        graph_fname: &str,
    ) -> Result<Option<crate::registry::GraphMaterial>, String> {
        Ok(self
            .read_graph_dump_blocking(graph_fname)?
            .map(|dump| crate::registry::GraphMaterial {
                nodes: dump.nodes,
                edges: dump.edges,
                semantic: dump.semantic,
                integrity_policy: dump.integrity_policy,
                incarnation_id: Some(dump.incarnation_id),
                source_snapshot_version: Some(dump.source_snapshot_version),
            }))
    }

    async fn read_authoritative_graph_snapshot(
        &self,
        graph_fname: &str,
    ) -> Result<Option<(crate::graph::GraphSnapshot, u64)>, String> {
        let graph = graph_fname.to_string();
        let shard = self.shard_for(graph_fname);
        let tx = shard.tx.clone();
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let version_graph = graph_fname.to_string();
        let read = move || {
            let (reply, rx) = std::sync::mpsc::channel();
            tx.send(Cmd::ReadGraphDump { graph, reply })
                .map_err(|_| "redb writer thread is gone".to_string())?;
            let dump = rx
                .recv()
                .map_err(|_| "redb writer dropped authoritative snapshot reply".to_string())??;
            let version = read_mutation_graph_version_record(&db, &version_graph)?.unwrap_or(0);
            Ok::<_, String>((dump, version))
        };
        let (dump, version) = if self.catalog.is_some() {
            let routing = self.routing_epoch.clone().read_owned().await;
            tokio::task::spawn_blocking(move || {
                let _routing = routing;
                read()
            })
            .await
            .map_err(|e| format!("authoritative snapshot join error: {e}"))??
        } else {
            tokio::task::spawn_blocking(read)
                .await
                .map_err(|e| format!("authoritative snapshot join error: {e}"))??
        };
        dump.map(|dump| {
            let semantic_store = if dump.semantic.is_empty() {
                crate::compute::semantic::SemanticStore::default()
            } else {
                decode_durable_semantic(&dump.semantic)?
            };
            Ok((
                crate::graph::GraphSnapshot {
                    schema_version: crate::graph::GRAPH_SNAPSHOT_SCHEMA_VERSION,
                    integrity_policy: dump.integrity_policy,
                    nodes: dump
                        .nodes
                        .into_iter()
                        .map(|(id, properties)| (id, Arc::new(properties)))
                        .collect(),
                    edges: dump
                        .edges
                        .into_iter()
                        .map(|(source, target, properties)| (source, target, Arc::new(properties)))
                        .collect(),
                    ledger: dump.ledger,
                    semantic_store,
                },
                version,
            ))
        })
        .transpose()
    }

    /// SYNC bounded-page durable-material fetch (CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged
    /// adjacency") — reuses [`Self::read_graph_dump_page_blocking`], a genuinely
    /// SOURCE-bounded scan (never collects the whole graph's rows into memory first,
    /// unlike the [`Self::read_graph_material_blocking`] override above / the
    /// default trait fallback), closing the honest limitation
    /// `docs/architecture/epistemic-os-hardening.md` names as open ledger item L38:
    /// "first access to a lazily-opened graph still fully rehydrates it".
    fn read_graph_material_page_blocking(
        &self,
        graph_fname: &str,
        cursor: Option<crate::registry::MaterializeCursor>,
        page_size: usize,
    ) -> Result<Option<crate::registry::MaterialPage>, String> {
        let (node_offset, edge_offset, node_after, edge_after) =
            cursor.map_or((0, 0, None, None), |cursor| {
                (
                    cursor.node_offset,
                    cursor.edge_offset,
                    cursor.node_after,
                    cursor.edge_after,
                )
            });
        Ok(self
            .read_graph_dump_page_blocking(
                graph_fname,
                node_offset,
                edge_offset,
                node_after,
                edge_after,
                page_size,
            )?
            .map(|page| {
                let next_cursor = if page.nodes_exhausted && page.edges_exhausted {
                    None
                } else {
                    Some(crate::registry::MaterializeCursor {
                        node_offset: node_offset + page.nodes.len(),
                        edge_offset: if page.nodes_exhausted {
                            edge_offset + page.edges.len()
                        } else {
                            edge_offset
                        },
                        node_after: page.node_after,
                        edge_after: page.edge_after,
                    })
                };
                crate::registry::MaterialPage {
                    nodes: page.nodes,
                    edges: page.edges,
                    semantic: page.semantic,
                    integrity_policy: page.integrity_policy,
                    next_cursor,
                    incarnation_id: Some(page.incarnation_id),
                    source_snapshot_version: Some(page.source_snapshot_version),
                }
            }))
    }

    /// COMMIT-BEFORE-ACK (CONCEPT:EG-KG.backend.authoritative-dispatch). Enqueue the mutation with a completion
    /// oneshot and await its durable commit. Backpressure-NOT-drop: a full queue
    /// BLOCKS for capacity (`SyncSender::send`) instead of shedding the write. The enqueue
    /// + the blocking send both happen on the blocking pool so the Tokio worker is
    /// never parked on disk/lock pressure. Completion is signalled by the writer
    /// AFTER its group-commit `WriteTransaction` commits, so concurrent callers still
    /// coalesce into ONE fsync.
    async fn record_durable(&self, graph_fname: &str, method: &Method) -> Result<(), String> {
        let (done_tx, done_rx) = oneshot::channel();
        let cmd = Cmd::Mutation {
            graph: graph_fname.to_string(),
            method: Box::new(method.clone()),
            done: done_tx,
        };
        // Blocking send = backpressure: park until the bounded channel has room
        // rather than dropping. Off the reactor via spawn_blocking so a saturated
        // writer can't stall the Tokio worker pool. Routed to the graph's shard.
        //
        // CONCEPT:EG-KG.backend.catalog-shard-resolve — when a tenant catalog is attached, resolve the shard AND enqueue
        // the op while holding a SHARED `routing_epoch` READ guard, so an online reshard's
        // exclusive flip cannot interleave (no lost / misrouted write). The guard is moved
        // INTO the blocking send so it is held exactly until the op is enqueued, then
        // dropped. With NO catalog (the default) this is byte-for-byte the EG-026 path.
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("redb record_durable join error: {e}"))?
            .map_err(|_| {
                "redb writer thread is gone; durable mutation not persisted".to_string()
            })?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("redb record_durable join error: {e}"))?
                .map_err(|_| {
                    "redb writer thread is gone; durable mutation not persisted".to_string()
                })?;
        }
        // Await the writer's post-commit signal. A dropped sender (writer gone /
        // commit thread died) is a durability failure, surfaced as Err.
        match done_rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped durable-commit completion".to_string()),
        }
    }

    /// Authoritative universal batch commit.  The bounded writer channel is
    /// entered with blocking `send` on Tokio's blocking pool, so saturation
    /// propagates backpressure and can never shed/partially enqueue a batch.
    async fn commit_mutation_batch(
        &self,
        graph_fname: &str,
        batch: &MutationBatch,
        result_msgpack: Option<&[u8]>,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MutationBatchCommit {
            payload: Box::new(MutationBatchPayload {
                graph: graph_fname.to_string(),
                batch: batch.clone(),
                authoritative_state_msgpack: None,
                result_msgpack: result_msgpack.map(ToOwned::to_owned),
                committed_at_ms,
                // No authoritative_state -> audit is gated per-operation from the
                // (identity-preserving) method itself downstream; this flag is inert.
                audited: true,
            }),
            done,
        };
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("commit_mutation_batch join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("commit_mutation_batch join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
        }
        rx.await
            .map_err(|_| "redb writer dropped MutationBatch completion".to_string())?
    }

    async fn commit_mutation_batch_state(
        &self,
        graph_fname: &str,
        batch: &MutationBatch,
        authoritative_state_msgpack: Vec<u8>,
        result_msgpack: Option<&[u8]>,
        committed_at_ms: u64,
        audited: bool,
    ) -> Result<MutationBatchCommit, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MutationBatchCommit {
            payload: Box::new(MutationBatchPayload {
                graph: graph_fname.to_string(),
                batch: batch.clone(),
                authoritative_state_msgpack: Some(authoritative_state_msgpack),
                result_msgpack: result_msgpack.map(ToOwned::to_owned),
                committed_at_ms,
                audited,
            }),
            done,
        };
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("commit_mutation_batch_state join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("commit_mutation_batch_state join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
        }
        rx.await
            .map_err(|_| "redb writer dropped staged MutationBatch completion".to_string())?
    }

    async fn commit_mutation_batch_crossmodal(
        &self,
        args: super::CrossModalCommitArgs<'_>,
    ) -> Result<MutationBatchCommit, String> {
        let graph_fname = args.graph_fname;
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::CrossModalBatchCommit {
            payload: Box::new(CrossModalBatchPayload {
                graph: graph_fname.to_string(),
                batch: args.batch.clone(),
                methods: args.methods.to_vec(),
                vectors: args.vectors.to_vec(),
                blob_refs: args.blob_refs.to_vec(),
                measurements: args.measurements.to_vec(),
                result_msgpack: args.result_msgpack.map(ToOwned::to_owned),
                committed_at_ms: args.committed_at_ms,
            }),
            done,
        };
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("commit_mutation_batch_crossmodal join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("commit_mutation_batch_crossmodal join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
        }
        rx.await
            .map_err(|_| "redb writer dropped cross-modal MutationBatch completion".to_string())?
    }

    async fn read_mutation_batch(
        &self,
        graph_fname: &str,
        batch_id: &str,
    ) -> Result<Option<MutationBatchRecord>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_mutation_batch_record(&db, batch_id, crypto)
    }

    async fn read_mutation_graph_version(&self, graph_fname: &str) -> Result<Option<u64>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        read_mutation_graph_version_record(&db, graph_fname)
    }

    async fn read_mutation_outbox(
        &self,
        graph_fname: &str,
        batch_id: &str,
    ) -> Result<Vec<MutationOutboxRecord>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_mutation_outbox_records(&db, batch_id, crypto)
    }

    async fn claim_mutation_outbox(
        &self,
        graph_fname: &str,
        consumer: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<MutationOutboxLease>, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MutationOutboxClaim {
            graph: graph_fname.to_string(),
            consumer: consumer.to_string(),
            now_ms,
            lease_ms,
            limit,
            done,
        };
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("claim_mutation_outbox join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("claim_mutation_outbox join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
        }
        rx.await
            .map_err(|_| "redb writer dropped outbox claim completion".to_string())?
    }

    async fn ack_mutation_outbox(
        &self,
        graph_fname: &str,
        lease: &MutationOutboxLease,
        projection: &str,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MutationOutboxAck {
            graph: graph_fname.to_string(),
            lease: Box::new(lease.clone()),
            projection: projection.to_string(),
            now_ms,
            done,
        };
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("ack_mutation_outbox join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("ack_mutation_outbox join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
        }
        rx.await
            .map_err(|_| "redb writer dropped outbox ack completion".to_string())?
    }

    async fn read_mutation_projection_cursor(
        &self,
        graph_fname: &str,
        projection: &str,
        tenant: &str,
    ) -> Result<Option<MutationProjectionCursor>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_mutation_projection_cursor_record(&db, graph_fname, projection, tenant, crypto)
    }

    async fn read_mutation_lifecycle_head(
        &self,
        graph_fname: &str,
    ) -> Result<Option<String>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        read_mutation_lifecycle_head_record(&db, graph_fname)
    }

    async fn commit_change_envelope(
        &self,
        graph_fname: &str,
        envelope: &ChangeEnvelope,
        committed_at_ms: u64,
    ) -> Result<ChangeEnvelopeCommit, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::ChangeEnvelopeCommit {
            payload: Box::new(ChangeEnvelopePayload {
                graph: graph_fname.to_string(),
                envelope: envelope.clone(),
                committed_at_ms,
            }),
            done,
        };
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("commit_change_envelope join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("commit_change_envelope join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
        }
        rx.await
            .map_err(|_| "redb writer dropped ChangeEnvelope completion".to_string())?
    }

    async fn commit_change_envelopes(
        &self,
        graph_fname: &str,
        envelopes: &[ChangeEnvelope],
        committed_at_ms: u64,
    ) -> Result<Vec<ChangeEnvelopeCommit>, (usize, String)> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::ChangeEnvelopesCommit {
            payload: Box::new(ChangeEnvelopesPayload {
                graph: graph_fname.to_string(),
                envelopes: envelopes.to_vec(),
                committed_at_ms,
            }),
            done,
        };
        let send_result = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send_result
            .map_err(|e| (0usize, format!("commit_change_envelopes join error: {e}")))?
            .map_err(|_| (0usize, "redb writer thread is gone".to_string()))?;
        rx.await.map_err(|_| {
            (
                0usize,
                "redb writer dropped ChangeEnvelopes completion".to_string(),
            )
        })?
    }

    async fn read_change_envelope(
        &self,
        graph_fname: &str,
        envelope_id: &str,
    ) -> Result<Option<ChangeEnvelopeRecord>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_change_envelope_record(&db, graph_fname, envelope_id, crypto)
    }

    async fn read_content_version(
        &self,
        graph_fname: &str,
        tenant: &str,
        object_id: &str,
    ) -> Result<Option<ContentVersion>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_content_version_record(&db, tenant, graph_fname, object_id, crypto)
    }

    async fn read_change_cursor(
        &self,
        graph_fname: &str,
        tenant: &str,
        source: &str,
        partition: &str,
    ) -> Result<Option<ChangeCursor>, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_change_cursor_record(&db, tenant, graph_fname, source, partition, crypto)
    }

    async fn read_resource_reservation(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::ResourceReservationStatusRequest,
    ) -> Result<crate::epistemic_operations::ResourceReservationResult, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_resource_reservation_record(&db, graph_fname, request, crypto)
    }

    async fn read_resource_reservation_status(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::ResourceReservationStatusRequest,
    ) -> Result<crate::epistemic_operations::ResourceReservationStatusResult, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_resource_reservation_status_record(&db, graph_fname, request, crypto)
    }

    /// Execute the narrow native WorkItem claim-capability mint operation on
    /// the graph's writer shard.  This is crate-private: external callers can
    /// submit only the typed opaque request through dispatch after authz.
    async fn mint_work_item_claim_capability(
        &self,
        graph_fname: &str,
        request: crate::epistemic_operations::WorkItemClaimCapabilityMintRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
    ) -> Result<crate::epistemic_operations::WorkItemClaimCapabilityResult, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MintWorkItemClaimCapability {
            graph: graph_fname.to_string(),
            request,
            authority,
            done,
        };
        let send = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send.map_err(|error| format!("claim-capability mint join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped claim-capability mint completion".to_string())?
    }

    /// Execute the linearizable native WorkItem claim-capability verification
    /// operation.  The writer command flushes earlier mutations before the
    /// control-row-first authorization/read sequence.
    async fn verify_work_item_claim_capability(
        &self,
        graph_fname: &str,
        request: crate::epistemic_operations::WorkItemClaimCapabilityVerifyRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
    ) -> Result<crate::epistemic_operations::WorkItemClaimCapabilityResult, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::VerifyWorkItemClaimCapability {
            graph: graph_fname.to_string(),
            request,
            authority,
            done,
        };
        let send = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send.map_err(|error| format!("claim-capability verify join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped claim-capability verify completion".to_string())?
    }

    /// Execute a native development-lane mutation (RMDD-28) on the graph's
    /// writer shard. `method` must be one of the six DevelopmentLane write
    /// variants; the kernel validates and rejects anything else.
    async fn commit_development_lane(
        &self,
        graph_fname: &str,
        method: Method,
        now_ms: u64,
    ) -> Result<Vec<u8>, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::CommitDevelopmentLane {
            graph: graph_fname.to_string(),
            method: Box::new(method),
            now_ms,
            done,
        };
        let send = if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
        };
        send.map_err(|error| format!("development-lane commit join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped development-lane commit completion".to_string())?
    }

    /// Exact authenticated native development-lane hold/tombstone read (RMDD-28).
    /// An MVCC snapshot read off the writer shard's shared `Database`, same
    /// posture as `read_resource_reservation` above -- never routed through the
    /// writer thread channel.
    async fn read_development_lane(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::DevelopmentLaneQueryRequest,
        now_ms: u64,
    ) -> Result<crate::epistemic_operations::DevelopmentLaneQueryResult, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::development_lane::read_development_lane(
            &db,
            graph_fname,
            request,
            now_ms,
            crypto,
        )
    }

    /// Bounded native development-lane tenant status page (RMDD-28). An MVCC
    /// snapshot read, same posture as `read_resource_reservation_status` above.
    async fn read_development_lane_status(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::DevelopmentLaneStatusRequest,
        now_ms: u64,
    ) -> Result<crate::epistemic_operations::DevelopmentLaneStatusResult, String> {
        let shard = self.shard_for(graph_fname);
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::development_lane::read_development_lane_status(
            &db,
            graph_fname,
            request,
            now_ms,
            crypto,
        )
    }

    /// **Cross-modal ACID (CONCEPT:EG-KG.txn.reader-never-sees-node).** Land graph + vectors + blob-refs for ONE
    /// graph in ONE redb `WriteTransaction`, awaiting its durable fsync. On any error
    /// the transaction is dropped without commit, so NONE of the modalities land — a
    /// true rollback (no partial cross-modal commit). Routed through the owner thread
    /// (exclusive file lock) via a blocking send off the reactor.
    async fn commit_crossmodal(
        &self,
        graph_fname: &str,
        methods: &[Method],
        vectors: &[(String, Vec<f32>)],
        blob_refs: &[(String, String)],
        measurements: &[crate::MeasurementBatch],
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::CrossModalCommit {
            payload: Box::new(CrossModalPayload {
                graph: graph_fname.to_string(),
                methods: methods.to_vec(),
                vectors: vectors.to_vec(),
                blob_refs: blob_refs.to_vec(),
                measurements: measurements.to_vec(),
            }),
            done,
        };
        // CONCEPT:EG-KG.backend.catalog-shard-resolve — same routing-epoch quiesce as `record_durable` when a catalog
        // is attached, so a cross-modal commit cannot race an online reshard's route flip.
        if self.catalog.is_some() {
            let guard = self.routing_epoch.clone().read_owned().await;
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || {
                let _routing = guard;
                tx.send(cmd).map_err(|_| ())
            })
            .await
            .map_err(|e| format!("commit_crossmodal join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        } else {
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("commit_crossmodal join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
        }
        match rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped commit_crossmodal completion".to_string()),
        }
    }

    async fn register_graph(
        &self,
        graph_fname: &str,
        name: &str,
        graph_type: GraphType,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::RegisterGraph {
            graph: graph_fname.to_string(),
            name: name.to_string(),
            graph_type,
            done,
        };
        let tx = self.shard_for(graph_fname).tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
            .await
            .map_err(|e| format!("redb register_graph join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        match rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped register_graph completion".to_string()),
        }
    }

    async fn purge_graph(&self, graph_fname: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::PurgeGraph {
            graph: graph_fname.to_string(),
            done,
        };
        let tx = self.shard_for(graph_fname).tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
            .await
            .map_err(|e| format!("redb purge_graph join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        match rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped purge_graph completion".to_string()),
        }
    }

    async fn read_node(&self, graph_fname: &str, node_id: &str) -> Result<Option<Vec<u8>>, String> {
        self.read_node_blocking(graph_fname, node_id)
    }

    fn durable_node_presence(
        &self,
        graph_fname: &str,
        node_ids: &[String],
    ) -> Result<Vec<bool>, String> {
        let shard = self.shard_for(graph_fname);
        let database = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        read_durable_node_presence(&database, graph_fname, node_ids)
    }

    fn read_node_blocking(
        &self,
        graph_fname: &str,
        node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        // CONCEPT:EG-KG.storage.snapshot-read-off-writer — SNAPSHOT READ OFF THE WRITER. The read-through point-read
        // (only hit on a RAM miss, CONCEPT:EG-KG.storage.read-through-seam-exercised) now serves the node DIRECTLY from
        // a `begin_read()` MVCC snapshot on the TARGET SHARD's shared `Database`
        // (routed by the SAME EG-026 `shard_for` the writer uses). It NEVER routes
        // through the writer thread's channel and NEVER forces a group-commit, so a
        // read can no longer block on / be serialized behind the durable write path —
        // critical on a Pi (frequent eviction/read-through) and across shards.
        //
        // Consistency: redb is MVCC, so the snapshot sees the LATEST COMMITTED state
        // of this shard. Commit-before-ack (CONCEPT:EG-KG.backend.authoritative-dispatch) guarantees any ACKED
        // write is already committed, so a `begin_read()` opened after that ack sees
        // it. Writes still buffered in the writer's `Pending` are NOT yet acked (no
        // happens-before to any reader), so omitting the old forced commit changes no
        // observable read result. Eviction is durability-gated (a node leaves RAM only
        // after redb confirms it on disk), so an evicted node is always served here.
        let shard = self.shard_for(graph_fname);
        // Upgrade the `Weak` to the writer's shared `Database` (CONCEPT:EG-KG.storage.snapshot-read-off-writer). `None`
        // only after shutdown dropped the writer's strong Arc — fail fast like the old
        // "writer thread is gone" channel error.
        let db = shard
            .db
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(shard.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_one_node(&db, graph_fname, node_id, crypto)
    }

    fn shutdown(&self) {
        // Stop every shard's writer thread (CONCEPT:EG-KG.backend.sharded-k-way-durable).
        for shard in &self.shards {
            shard.shutdown();
        }
    }

    fn as_redb(&self) -> Option<&RedbBackend> {
        Some(self)
    }
}

// ── Time-series STARTUP RECONCILIATION (CONCEPT:EG-KG.backend.ts-startup-reconcile, L16) ──────────────
// EG-P0-4 (see `handlers::txn::commit_cross_modal_txn`'s doc comment) replays a
// cross-modal-committed measurement into the served `series.redb` immediately after the
// the authoritative-shard commit succeeds, but documents one residual: a crash strictly
// BETWEEN those two commits leaves the measurement durable + authoritative in
// after its SERIES-table commit may not be reflected in the served store.
// The pass below closes that window: run ONCE at boot, after both stores are open and
// before the server accepts traffic, so a prior crash's residual never lingers.
#[cfg(feature = "tsdb")]
pub struct TsReconcileReport {
    /// Series whose durable projection cursor/meta did not already match the shard
    /// (the only ones actually inspected point-by-point).
    pub series_examined: usize,
    /// Of those, how many actually needed a replay (a meta mismatch can, in principle,
    /// self-resolve to "nothing missing" once the exact point sets are compared).
    pub series_reconciled: usize,
    /// Total individual points replayed into the served store across all series.
    pub points_replayed: usize,
    /// Durable high-water cursors created or advanced this pass.
    pub projection_cursors_written: usize,
}

#[cfg(feature = "tsdb")]
impl RedbBackend {
    /// Startup reconciliation (CONCEPT:EG-KG.backend.ts-startup-reconcile, L16): scan every shard's
    /// authoritative shard's SERIES tables — the atomic copy a cross-modal commit
    /// writes (EG-P0-4) — and replay into `tsdb_store` (the served `series.redb`) any
    /// measurement durable there but not yet reflected in the served store.
    ///
    /// **Idempotent + duplicate-free.** For each series, a durable projection cursor
    /// `(count, min_ts, max_ts)` is compared first; a current cursor skips the series
    /// without a point scan. An older store with no cursor falls back to full schema/span
    /// metadata and writes the cursor when already converged. A mismatch triggers an
    /// EXACT multiset point-diff (not a
    /// naive "skip the first N" positional diff, which would be WRONG if two batches that
    /// share a time bucket land out of append order — see the point-diff comment below)
    /// between the two stores' full point sets for that series, and only the points
    /// present in the authoritative shard but absent from the served store are appended — so a
    /// partially-replayed crash window is closed exactly, never duplicated. Any
    /// non-canonical key fails startup rather than guessing an owner.
    ///
    /// Read-only against each authoritative shard: uses the SAME shared `Weak<Database>` handle the
    /// snapshot-read path (`read_node_blocking`) upgrades, so this never opens a SECOND
    /// `Database` on the file (redb's exclusive per-process file lock would reject that) —
    /// see `eg_tsdb::store::{list_series_in_rtx, meta_in_rtx, range_in_rtx}`, the read-only
    /// counterparts of `append_batch_in_wtx` extracted for exactly this caller.
    pub async fn reconcile_time_series(
        &self,
        tsdb_store: &eg_tsdb::store::SeriesStore,
    ) -> Result<TsReconcileReport, String> {
        let mut report = TsReconcileReport {
            series_examined: 0,
            series_reconciled: 0,
            points_replayed: 0,
            projection_cursors_written: 0,
        };
        for shard in &self.shards {
            // A shard whose writer thread already exited (shutdown mid-boot-sequence,
            // never happens in the normal boot path but guarded like every other
            // snapshot-read consumer of this handle) has nothing left to reconcile.
            let Some(db) = shard.db.upgrade() else {
                continue;
            };
            let rtx = db.begin_read().map_err(|e| e.to_string())?;
            let series_ids = eg_tsdb::store::list_series_in_rtx(&rtx).map_err(|e| e.to_string())?;
            for series_id in series_ids {
                if eg_tsdb::store::SeriesKey::decode(&series_id).is_none() {
                    return Err("durable time-series key is not canonically scoped".to_string());
                }
                let graph_meta = eg_tsdb::store::meta_in_rtx(&rtx, &series_id)
                    .map_err(|e| e.to_string())?
                    .expect("series id just came from this same SERIES_META scan");
                let source_cursor = eg_tsdb::store::ProjectionCursor::from(&graph_meta);
                let projection = tsdb_store
                    .projection_health_by_storage_key(&series_id)
                    .map_err(|e| e.to_string())?;
                if projection.status == eg_tsdb::store::ProjectionStatus::Ready
                    && projection.cursor.as_ref() == Some(&source_cursor)
                {
                    continue;
                }
                let served_meta = tsdb_store.meta(&series_id).map_err(|e| e.to_string())?;
                if served_meta.as_ref().is_some_and(|m| {
                    m.count == graph_meta.count
                        && m.min_ts == graph_meta.min_ts
                        && m.max_ts == graph_meta.max_ts
                        && m.n_fields == graph_meta.n_fields
                        && m.bucket_ns == graph_meta.bucket_ns
                }) {
                    tsdb_store
                        .mark_projection_ready(&series_id, &graph_meta)
                        .map_err(|e| e.to_string())?;
                    report.projection_cursors_written += 1;
                    continue;
                }
                report.series_examined += 1;
                let graph_points = eg_tsdb::store::range_in_rtx(
                    &rtx,
                    &series_id,
                    eg_tsdb::point::Ts::MIN,
                    eg_tsdb::point::Ts::MAX,
                )
                .map_err(|e| e.to_string())?;
                let served_points = tsdb_store.scan_all(&series_id).map_err(|e| e.to_string())?;
                let missing = missing_points(graph_points, served_points);
                if missing.is_empty() {
                    // The mismatched count can happen without a point actually being
                    // absent — e.g. late/duplicate-ts siblings resolved differently on
                    // each side would never occur here since both stores are fed the
                    // identical append sequence, but this keeps the pass exact rather
                    // than assuming the meta compare alone is sufficient.
                    tsdb_store
                        .mark_projection_ready(&series_id, &graph_meta)
                        .map_err(|e| e.to_string())?;
                    report.projection_cursors_written += 1;
                    continue;
                }
                if let Err(error) = tsdb_store.append_batch(
                    &series_id,
                    graph_meta.n_fields,
                    graph_meta.bucket_ns,
                    &graph_meta.field_names,
                    &missing,
                ) {
                    let message = error.to_string();
                    let _ = tsdb_store.mark_projection_degraded(&series_id, &message);
                    return Err(message);
                }
                tsdb_store
                    .mark_projection_ready(&series_id, &graph_meta)
                    .map_err(|e| e.to_string())?;
                report.series_reconciled += 1;
                report.points_replayed += missing.len();
                report.projection_cursors_written += 1;
                tracing::warn!(
                    "startup reconciliation: a scoped series was durable in the authoritative shard \
                     but {} point(s) had not reached the served time-series store (a crash \
                     between the two EG-P0-4 commits) — replayed",
                    missing.len()
                );
            }
        }
        Ok(report)
    }
}

/// Exact multiset point-diff (CONCEPT:EG-KG.backend.ts-startup-reconcile): the points present in `authoritative`
/// but not already accounted for in `served`, respecting multiplicity (two points sharing
/// a timestamp are legitimate siblings, not duplicates of one another — see
/// `eg_tsdb::store`'s `Chunk::insert` doc comment). A naive "skip the first `served.len()`
/// points of a merged/sorted scan" is WRONG here: two measurement batches that touch the
/// SAME time bucket can interleave within that bucket's sorted point list regardless of
/// which batch replayed to the served store first, so the served store's points are not
/// guaranteed to be a positional PREFIX of the authoritative scan — only a SUBSET of it.
/// `f64` values are compared by exact bit pattern (`to_bits`): both stores hold the
/// IDENTICAL byte-for-byte values the client originally sent (no arithmetic is ever
/// performed on a stored point), so bitwise equality is the correct — and only
/// semantically meaningful — comparison here.
#[cfg(feature = "tsdb")]
fn missing_points(
    authoritative: Vec<eg_tsdb::point::Point>,
    served: Vec<eg_tsdb::point::Point>,
) -> Vec<eg_tsdb::point::Point> {
    use std::collections::HashMap;

    fn key(p: &eg_tsdb::point::Point) -> (i64, Vec<u64>) {
        (p.ts, p.values.iter().map(|v| v.to_bits()).collect())
    }

    let mut served_counts: HashMap<(i64, Vec<u64>), usize> = HashMap::new();
    for p in &served {
        *served_counts.entry(key(p)).or_insert(0) += 1;
    }
    let mut missing = Vec::new();
    for p in authoritative {
        let k = key(&p);
        match served_counts.get_mut(&k) {
            Some(c) if *c > 0 => *c -= 1,
            _ => missing.push(p),
        }
    }
    missing
}

// ── Durable Raft log API (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — inherent methods ────────────────
// The Raft log lives in the SAME authoritative shard Database, written by the SAME
// off-reactor group-commit thread, keyed by `(group_id, index)` so one table
// serves every group (CONCEPT:EG-KG.sharding.raft-resharding). Sharing the writer is what lets a log
// append and its graph mutation coalesce into ONE fsync. The raft/xshard methods are
// individually `raft`-gated; the plan-backed matview persistence methods below are
// `matview`-gated (single-node native), so the impl block opens under EITHER — the
// plan-backed incremental matview needs its durable rows WITHOUT pulling raft.
#[cfg(any(feature = "raft", feature = "matview"))]
impl RedbBackend {
    /// Durably append Raft log entries for a group, awaiting the group-commit fsync
    /// (commit-before-ack). The entries fold into the SAME batch as concurrent M2
    /// mutations, so one fsync covers both.
    #[cfg(feature = "raft")]
    pub async fn raft_log_append(
        &self,
        group_id: u64,
        entries: Vec<(u64, Vec<u8>)>,
    ) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::RaftLogAppend {
            group_id,
            entries,
            done,
        };
        // ADR-2 / W1.2: route to the group's OWN shard (`group_id % K`) so its log
        // append coalesces into the SAME shard writer's fsync as that group's graph
        // mutations (EG-KG.storage.one-fsync-covers-raft), and N groups append to N
        // shards in parallel. K == 1 stores keep every group on shard 0 (unchanged).
        let tx = self.shard_for_group(group_id).tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
            .await
            .map_err(|e| format!("raft_log_append join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_log_append completion".to_string())?
    }

    /// Read an inclusive `[lo, hi]` log index range for a group, in order.
    #[cfg(feature = "raft")]
    pub fn raft_log_read(&self, group_id: u64, lo: u64, hi: u64) -> Result<Vec<Vec<u8>>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for_group(group_id)
            .tx
            .send(Cmd::RaftLogRead {
                group_id,
                lo,
                hi,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped raft_log_read reply".to_string())?
    }

    /// Delete entries with index >= `from` for a group (conflict truncation).
    #[cfg(feature = "raft")]
    pub async fn raft_log_delete_from(&self, group_id: u64, from: u64) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let tx = self.shard_for_group(group_id).tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::RaftLogDeleteFrom {
                group_id,
                from,
                done,
            })
            .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("raft_log_delete_from join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_log_delete_from completion".to_string())?
    }

    /// Delete entries with index <= `upto` for a group (purge/compaction).
    #[cfg(feature = "raft")]
    pub async fn raft_log_purge_upto(&self, group_id: u64, upto: u64) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let tx = self.shard_for_group(group_id).tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::RaftLogPurgeUpto {
                group_id,
                upto,
                done,
            })
            .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("raft_log_purge_upto join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_log_purge_upto completion".to_string())?
    }

    /// `(first, last)` present log index for a group (for `get_log_state`).
    #[cfg(feature = "raft")]
    pub fn raft_log_bounds(&self, group_id: u64) -> LogBoundsResult {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for_group(group_id)
            .tx
            .send(Cmd::RaftLogBounds { group_id, reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped raft_log_bounds reply".to_string())?
    }

    /// Durably write one Raft metadata key (vote / applied-state / last-purged).
    #[cfg(feature = "raft")]
    pub async fn raft_meta_put(
        &self,
        group_id: u64,
        key: &str,
        val: Vec<u8>,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let key = key.to_string();
        // ADR-2 / W1.2: the group's vote/applied-state/last-purged pointer lives in the
        // group's OWN shard, beside its log + graph data (`group_id % K`).
        let tx = self.shard_for_group(group_id).tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::RaftMetaPut {
                group_id,
                key,
                val,
                done,
            })
            .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("raft_meta_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_meta_put completion".to_string())?
    }

    /// Read one Raft metadata key for a group.
    #[cfg(feature = "raft")]
    pub fn raft_meta_get(&self, group_id: u64, key: &str) -> Result<Option<Vec<u8>>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard_for_group(group_id)
            .tx
            .send(Cmd::RaftMetaGet {
                group_id,
                key: key.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped raft_meta_get reply".to_string())?
    }

    // ── Cross-shard 2PC durable records (CONCEPT:EG-KG.storage.lane-n-increment) ─────────────────────
    // The 2PC coordinator persists each participant's PREPARE slice + the final
    // DECISION here so an in-doubt txn is resolvable after a crash. Each write awaits
    // an Immediate-durability commit (commit-before-vote / commit-before-apply): a
    // group only votes yes once its slice is on disk, and the decision is on disk
    // before any participant applies — the atomicity barrier.

    /// Durably persist one participant group's prepared slice. Awaits the fsync.
    #[cfg(feature = "raft")]
    pub async fn xshard_prepare_put(
        &self,
        txn_id: &str,
        group_id: u64,
        slice: Vec<u8>,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardPreparePut {
                txn_id,
                group_id,
                slice,
                done,
            })
            .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("xshard_prepare_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_prepare_put completion".to_string())?
    }

    /// Read one exact participant prepare in logarithmic table-lookup time.
    #[cfg(feature = "raft")]
    pub fn xshard_prepare_get(
        &self,
        txn_id: &str,
        group_id: u64,
    ) -> Result<Option<Vec<u8>>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::XshardPrepareGet {
                txn_id: txn_id.to_string(),
                group_id,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped xshard_prepare_get reply".to_string())?
    }

    /// Durably write the coordinator's decision (the atomic commit point). Awaits fsync.
    #[cfg(feature = "raft")]
    pub async fn xshard_decision_put(&self, txn_id: &str, commit: bool) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardDecisionPut {
                txn_id,
                commit,
                retain_for_parent: false,
                done,
            })
            .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("xshard_decision_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_decision_put completion".to_string())?
    }

    /// Durably write an exact decision that must remain until a separate parent
    /// receipt has committed.
    #[cfg(feature = "raft")]
    pub async fn xshard_recoverable_decision_put(
        &self,
        txn_id: &str,
        commit: bool,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardDecisionPut {
                txn_id,
                commit,
                retain_for_parent: true,
                done,
            })
            .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("xshard recoverable decision join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped recoverable decision completion".to_string())?
    }

    /// Persist the parent-recoverable protocol-start marker before phase 1.
    #[cfg(feature = "raft")]
    pub async fn xshard_recoverable_pending_put(&self, txn_id: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardRecoverablePendingPut { txn_id, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("xshard recoverable pending join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped recoverable pending completion".to_string())?
    }

    /// Clear one participant's prepare record after it is resolved.
    #[cfg(feature = "raft")]
    pub async fn xshard_prepare_clear(&self, txn_id: &str, group_id: u64) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardPrepareClear {
                txn_id,
                group_id,
                done,
            })
            .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("xshard_prepare_clear join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_prepare_clear completion".to_string())?
    }

    /// Clear a resolved txn's decision record.
    #[cfg(feature = "raft")]
    pub async fn xshard_decision_clear(&self, txn_id: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardDecisionClear { txn_id, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("xshard_decision_clear join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_decision_clear completion".to_string())?
    }

    /// Scan every in-doubt prepare record `(txn_id, group_id, slice)` (for recovery).
    #[cfg(feature = "raft")]
    pub fn xshard_scan_prepares(&self) -> XshardPrepareScan {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::XshardScanPrepares { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped xshard_scan_prepares reply".to_string())?
    }

    /// Scan digest-only decision states (no source payloads).
    #[cfg(feature = "raft")]
    pub fn xshard_scan_decisions(&self) -> XshardDecisionScan {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::XshardScanDecisions { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped xshard decision scan".to_string())?
    }

    /// Read a txn's durable decision (Some(true)=commit, Some(false)=abort, None=undecided).
    #[cfg(feature = "raft")]
    pub fn xshard_decision_get(&self, txn_id: &str) -> Result<Option<bool>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::XshardDecisionGet {
                txn_id: txn_id.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped xshard_decision_get reply".to_string())?
    }

    /// Whether this marker is retained for a separate parent receipt.
    #[cfg(feature = "raft")]
    pub fn xshard_decision_retain_get(&self, txn_id: &str) -> Result<bool, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::XshardDecisionRetainGet {
                txn_id: txn_id.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped xshard retain reply".to_string())?
    }

    /// Durably upsert a named materialized view's serialized blob (CONCEPT:EG-KG.storage.feature).
    /// Awaits the fsync so a `CreateMatView`/`RefreshMatView` ack means it is on disk.
    #[cfg(feature = "compute-dist")]
    pub async fn matview_put(&self, name: &str, blob: Vec<u8>) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::MatViewPut { name, blob, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("matview_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped matview_put completion".to_string())?
    }

    /// Scan every persisted materialized view `(name, blob)` (reload on boot).
    #[cfg(feature = "compute-dist")]
    pub fn matview_scan(&self) -> MatViewScanResult {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::MatViewScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped matview_scan reply".to_string())?
    }

    /// Durably upsert a PLAN-BACKED matview definition (CONCEPT:EG-KG.storage.plan-backed-matview).
    /// Awaits the fsync so a `PlanMatViewDefine` ack means the definition is on disk.
    #[cfg(feature = "matview")]
    pub async fn plan_matview_put(&self, name: &str, blob: Vec<u8>) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::PlanMatViewPut { name, blob, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("plan_matview_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped plan_matview_put completion".to_string())?
    }

    /// Durably delete a plan-backed matview definition (awaits the fsync).
    #[cfg(feature = "matview")]
    pub async fn plan_matview_delete(&self, name: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::PlanMatViewDelete { name, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("plan_matview_delete join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped plan_matview_delete completion".to_string())?
    }

    /// Scan every persisted plan-backed matview `(name, blob)` (reload on boot).
    #[cfg(feature = "matview")]
    pub fn plan_matview_scan(&self) -> MatViewScanResult {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::PlanMatViewScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped plan_matview_scan reply".to_string())?
    }

    /// Durably upsert an incremental matview's operator-state snapshot
    /// (CONCEPT:EG-KG.storage.incremental-matview). Awaits the fsync.
    #[cfg(feature = "matview")]
    pub async fn matview_operator_state_put(
        &self,
        name: &str,
        blob: Vec<u8>,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::MatViewOperatorStatePut { name, blob, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("matview_operator_state_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped matview_operator_state_put completion".to_string())?
    }

    /// Durably delete an incremental matview's operator-state snapshot (awaits the fsync).
    #[cfg(feature = "matview")]
    pub async fn matview_operator_state_delete(&self, name: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.shard0().tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::MatViewOperatorStateDelete { name, done })
                .map_err(|_| ())
        })
        .await
        .map_err(|e| format!("matview_operator_state_delete join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await.map_err(|_| {
            "redb writer dropped matview_operator_state_delete completion".to_string()
        })?
    }

    /// Scan every persisted incremental-matview operator-state snapshot `(name, blob)`.
    #[cfg(feature = "matview")]
    pub fn matview_operator_state_scan(&self) -> MatViewScanResult {
        let (reply, rx) = std::sync::mpsc::channel();
        self.shard0()
            .tx
            .send(Cmd::MatViewOperatorStateScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped matview_operator_state_scan reply".to_string())?
    }
}

// ── off-reactor group-commit writer thread ───────────────────────────────

fn run(
    rx: Receiver<Cmd>,
    // Shared `Database` handle (CONCEPT:EG-KG.storage.snapshot-read-off-writer): the writer OWNS one clone of the Arc
    // (kept alive for the thread's whole life); the Shard holds another for off-writer
    // snapshot reads. Rebound to `&Database` immediately so the commit path below is
    // byte-for-byte the pre-EG-027 single-`Database` writer loop.
    db: Arc<Database>,
    policy: DurabilityPolicy,
    group_commit: RedbGroupCommitConfig,
    stats: Arc<RedbCommitStats>,
    // Auto-sized early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb), per shard.
    flush_threshold: usize,
    #[cfg(feature = "security")] cipher: Option<crate::crypto::ValueCipher>,
) {
    // Borrow the shared handle for the rest of the loop; the owned Arc above stays
    // alive until `run` returns, so this reference is valid for the whole thread.
    let db: &Database = &db;
    // Build the durable-crypto handle ONCE (borrows the owned cipher for the thread's
    // lifetime). No-op handle when encryption is off / not compiled.
    #[cfg(feature = "security")]
    let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
    #[cfg(not(feature = "security"))]
    let crypto = crate::redb_store::DurableCrypto::none();
    let tick = policy.tick();
    // Pending mutations folded into the NEXT group commit, each with its optional
    // commit-before-ack completion sender (CONCEPT:EG-KG.backend.authoritative-dispatch). After a commit, EVERY
    // sender in the batch is fired with the batch's result — one fsync, N notified.
    let mut pending: Pending = Pending::default();
    // CONCEPT:EG-KG.backend.adaptive-linger-coalesce — record the group-commit batch size (ops-per-fsync), then
    // commit+notify. Only counts a batch that actually carried work; `lingered` marks
    // commits that paid a micro-linger window so the win is measurable.
    let commit_now = |pending: &mut Pending, durability: Durability, lingered: bool| {
        if !pending.is_empty() {
            stats.record(pending.ops.len(), lingered);
        }
        commit_and_notify(db, pending, durability, crypto);
    };
    loop {
        match rx.recv_timeout(tick) {
            Ok(cmd) => {
                if handle_cmd(cmd, db, &mut pending, flush_threshold, crypto) {
                    // shutdown: flush whatever is pending durably, then stop.
                    commit_now(&mut pending, Durability::Immediate, false);
                    break;
                }
                // Drain the rest of the burst so it coalesces into one commit.
                let mut stop = false;
                while let Ok(cmd) = rx.try_recv() {
                    if handle_cmd(cmd, db, &mut pending, flush_threshold, crypto) {
                        stop = true;
                        break;
                    }
                }
                if stop {
                    commit_now(&mut pending, Durability::Immediate, false);
                    return;
                }
                // Any awaiting commit-before-ack op in the batch MUST be made durable
                // now (don't leave an awaited write parked until the next tick): if a
                // barrier op is pending, commit immediately; otherwise honor policy.
                let must_commit_now =
                    pending.has_barrier() || matches!(policy, DurabilityPolicy::Each);
                if must_commit_now {
                    // CONCEPT:EG-KG.backend.adaptive-linger-coalesce — adaptive group-commit micro-linger. The commit
                    // trigger fires the instant the channel drains, so with low in-flight
                    // write concurrency (serial awaits) the barrier batch is ~1 op ⇒ ~1
                    // fsync/write — the profiled write ceiling. When the about-to-commit
                    // batch is SHALLOW (and no hard barrier needs immediacy), spend ONE
                    // bounded `recv_timeout(linger)` so concurrently-awaiting writers can
                    // land in the channel, then drain again — folding them into the SAME
                    // fsync. Adaptive: a DEEP batch (ops >= shallow_threshold) is already
                    // coalescing, so we linger 0. Guards that PRESERVE latency/correctness:
                    //   * skip when linger == 0 (disabled / bench baseline),
                    //   * skip Raft-log barriers (`raft_log_ops`) so consensus is never
                    //     delayed — only shallow GRAPH-mutation batches linger,
                    //   * the existing 4096 early-flush bound in `handle_cmd` is the upper
                    //     op-count guard, so a linger can never overgrow the batch.
                    // Durability is UNCHANGED: we widen the batch, we do NOT defer any ack
                    // past its own commit (the same `Durability::Immediate` fsync still
                    // precedes every `done` waiter firing).
                    let mut lingered = false;
                    if group_commit.linger > Duration::ZERO
                        && pending.raft_log_ops.is_empty()
                        && !pending.ops.is_empty()
                        && pending.ops.len() < group_commit.shallow_threshold
                    {
                        lingered = true;
                        match rx.recv_timeout(group_commit.linger) {
                            Ok(cmd) => {
                                if handle_cmd(cmd, db, &mut pending, flush_threshold, crypto) {
                                    commit_now(&mut pending, Durability::Immediate, true);
                                    return;
                                }
                                // Drain everyone who arrived during the linger window.
                                while let Ok(cmd) = rx.try_recv() {
                                    if handle_cmd(cmd, db, &mut pending, flush_threshold, crypto) {
                                        commit_now(&mut pending, Durability::Immediate, true);
                                        return;
                                    }
                                }
                            }
                            Err(RecvTimeoutError::Timeout) => {}
                            Err(RecvTimeoutError::Disconnected) => {
                                commit_now(&mut pending, Durability::Immediate, true);
                                break;
                            }
                        }
                    }
                    commit_now(&mut pending, Durability::Immediate, lingered);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // Group-commit boundary: flush pending mutations.
                commit_now(&mut pending, Durability::Immediate, false);
            }
            Err(RecvTimeoutError::Disconnected) => {
                commit_now(&mut pending, Durability::Immediate, false);
                break;
            }
        }
    }
}

/// Buffered mutations awaiting the next group commit, plus the commit-before-ack
/// completion senders that must be fired once the batch is durable.
#[derive(Default)]
struct Pending {
    ops: Vec<(String, Method)>,
    /// Raft log appends `(group_id, index, blob)` folded into the SAME group-commit
    /// transaction as `ops` (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — one fsync covers both the log entry
    /// and the M2 graph mutation.
    raft_log_ops: Vec<(u64, u64, Vec<u8>)>,
    /// One per awaited (commit-before-ack) op in this batch.
    waiters: Vec<oneshot::Sender<Result<(), String>>>,
    /// O(1) per-graph audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store). Lives on `Pending`
    /// because `Pending` is owned by the writer thread's `run` loop for the thread's
    /// LIFETIME (not reset between batches) — so the cached `(seq, hash)` tail stays hot
    /// across group commits, and the per-op range-scan that was burning the now
    /// CPU-bound writer (post-EG-024) is gone. The writer is the sole AUDIT mutator, so
    /// the in-memory tail is authoritative; it is seeded once per graph (incl. after a
    /// restart) from a single scan inside `append_audit_entry`.
    #[cfg(feature = "security")]
    audit_tail: crate::redb_store::AuditTailCache,
    /// Per-graph provenance-anchor tail cache (CONCEPT:EG-KG.sharding.row-level-security), the
    /// `ProvenanceAnchorCommit` sibling of `audit_tail` above — lives on `Pending`
    /// for the same reason: the writer thread's LIFETIME, seeded once per graph
    /// from a single scan (`provenance_anchor_commit`), then kept hot in RAM.
    #[cfg(feature = "security")]
    provenance_anchor_cache: crate::redb_store::ProvenanceAnchorCache,
}

impl Pending {
    fn has_barrier(&self) -> bool {
        !self.waiters.is_empty()
    }
    fn is_empty(&self) -> bool {
        self.ops.is_empty() && self.raft_log_ops.is_empty() && self.waiters.is_empty()
    }
}

/// Returns true if the writer should stop. `Mutation` is buffered into `pending`
/// (committed at the next group boundary); `Checkpoint` is applied + committed
/// immediately (it carries its own reply).
fn handle_cmd(
    cmd: Cmd,
    db: &Database,
    pending: &mut Pending,
    // Auto-sized early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb).
    flush_threshold: usize,
    crypto: crate::redb_store::DurableCrypto<'_>,
) -> bool {
    match cmd {
        Cmd::Mutation {
            graph,
            method,
            done,
        } => {
            pending.ops.push((graph, *method));
            pending.waiters.push(done);
            // Bound memory: if a burst outpaces the tick, flush early. The group
            // still amortizes thousands of row writes per commit, and fires every
            // commit-before-ack waiter for the ops in this flush. The threshold is
            // hardware-auto-sized (CONCEPT:AU-KG.backend.b-auto-sizeb) — small on a Pi, large on a big box.
            if pending.ops.len() >= flush_threshold {
                commit_and_notify(db, pending, Durability::Immediate, crypto);
            }
            false
        }
        Cmd::RegisterGraph {
            graph,
            name,
            graph_type,
            done,
        } => {
            // Flush pending mutations first so a graph's rows and its meta land in a
            // consistent order, then durably write the graph_meta row.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = write_graph_meta(db, &graph, &name, graph_type);
            let _ = done.send(res);
            false
        }
        Cmd::PurgeGraph { graph, done } => {
            // Flush pending mutations first so we never purge a graph and then
            // re-apply a buffered op for it out of order, then drop ALL of its rows
            // (incl. graph_meta) in one durable transaction.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(purge_graph_rows(db, &graph, crypto));
            false
        }
        Cmd::ReadNode {
            graph,
            node_id,
            reply,
        } => {
            // Flush pending (incl. any awaited ops) so the read reflects the latest
            // durable state, then point-read the node row.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_one_node(db, &graph, &node_id, crypto));
            false
        }
        Cmd::Load { reply } => {
            // Flush pending so the read sees the latest, then scan the owned DB.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_all_dumps(db, crypto));
            false
        }
        Cmd::ReadGraphDump { graph, reply } => {
            // Flush pending so the rehydrated dump reflects the latest durable state,
            // then range-scan ONE graph's rows (CONCEPT:EG-KG.storage.100m-tenant).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_graph_dump(db, &graph, crypto));
            false
        }
        Cmd::ReadGraphDumpPage {
            graph,
            query,
            reply,
        } => {
            // Flush pending first (same consistency contract as ReadGraphDump), then
            // fetch ONE bounded page straight off the durable store (CONCEPT:EG-KG.sharding.paged-lazy-open, L38).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(crate::redb_store::read_graph_dump_page(
                db,
                &graph,
                crypto,
                crate::redb_store::PageCursorRef {
                    node_offset: query.node_offset,
                    edge_offset: query.edge_offset,
                    node_after: query.node_after.as_deref(),
                    edge_after: query.edge_after.as_ref().map(|(source, target, ordinal)| {
                        (source.as_str(), target.as_str(), *ordinal)
                    }),
                    page_size: query.page_size,
                },
            ));
            false
        }
        Cmd::ExportGraphRaw { graph, reply } => {
            // CONCEPT:EG-KG.backend.catalog-shard-resolve — flush pending so every committed mutation is captured, then
            // scan this graph's rows VERBATIM (raw blobs — encryption + audit chain kept).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(super::online_reshard::export_graph_raw(db, &graph));
            false
        }
        Cmd::ImportGraphRaw { graph, rows, reply } => {
            // CONCEPT:EG-KG.backend.catalog-shard-resolve — flush pending first (consistency), then land the migrated
            // rows verbatim in ONE durable commit (the move's commit-before-ack point).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(super::online_reshard::import_graph_raw(db, &graph, &rows));
            false
        }
        Cmd::ImportGraphDelta {
            graph,
            delta,
            reply,
        } => {
            // CONCEPT:EG-KG.backend.flush-pending-first — flush pending first (consistency), then land ONLY the delta
            // rows (upserts + removals) in ONE durable commit (the under-quiesce write).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(super::online_reshard::import_graph_delta(
                db, &graph, &delta,
            ));
            false
        }
        Cmd::PurgeMovedMutationRows { graph, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(super::online_reshard::purge_moved_mutation_rows(db, &graph));
            false
        }
        #[cfg(feature = "security")]
        Cmd::AuditVerify { graph, reply } => {
            // Flush pending so the chain walk includes the latest durable audit
            // entries, then verify the hash chain (CONCEPT:EG-KG.sharding.row-level-security).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(crate::redb_store::verify_audit(db, &graph));
            false
        }
        #[cfg(all(test, feature = "security"))]
        Cmd::TestTamperAudit { graph, seq, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = (|| {
                let wtx = db.begin_write().map_err(|e| e.to_string())?;
                {
                    let mut audit = wtx
                        .open_table(crate::redb_store::AUDIT)
                        .map_err(|e| e.to_string())?;
                    let original = audit
                        .get((graph.as_str(), seq))
                        .map_err(|e| e.to_string())?
                        .ok_or_else(|| "no such audit entry".to_string())?
                        .value()
                        .to_vec();
                    let mut mutated = original;
                    let last = mutated.len() - 1;
                    mutated[last] ^= 0xFF;
                    audit
                        .insert((graph.as_str(), seq), mutated.as_slice())
                        .map_err(|e| e.to_string())?;
                }
                wtx.commit().map_err(|e| e.to_string())?;
                Ok(())
            })();
            let _ = reply.send(res);
            false
        }
        #[cfg(feature = "security")]
        Cmd::ProvenanceAnchorCommit {
            graph,
            root,
            members,
            reply,
        } => {
            // Flush pending first so the anchor's cache-seed (on first touch) and
            // its audit-chain append see the latest durable state, mirroring
            // AuditVerify/TestTamperAudit above.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = crate::redb_store::provenance_anchor_commit(
                db,
                &mut pending.provenance_anchor_cache,
                &mut pending.audit_tail,
                &graph,
                root,
                &members,
            );
            let _ = reply.send(res);
            false
        }
        #[cfg(feature = "security")]
        Cmd::AuditProveInclusion {
            graph,
            node_id,
            anchor_seq,
            reply,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = crate::redb_store::prove_inclusion(db, &graph, &node_id, anchor_seq, crypto);
            let _ = reply.send(res);
            false
        }
        Cmd::CrossModalCommit { payload, done } => {
            let CrossModalPayload {
                graph,
                methods,
                vectors,
                blob_refs,
                measurements,
            } = *payload;
            // Flush pending first so this cross-modal txn observes the latest durable
            // state (its vector read-modify-write of the SEMANTIC blob must start from
            // the committed store), then land ALL modalities in ONE WriteTransaction.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = commit_crossmodal(
                db,
                &graph,
                &methods,
                &vectors,
                &blob_refs,
                &measurements,
                crypto,
                // Shares the writer's persistent tail cache (CONCEPT:EG-KG.storage.embedded-store).
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            );
            let _ = done.send(res);
            false
        }
        Cmd::CrossModalBatchCommit { payload, done } => {
            let CrossModalBatchPayload {
                graph,
                batch,
                methods,
                vectors,
                blob_refs,
                measurements,
                result_msgpack,
                committed_at_ms,
            } = *payload;
            // Preserve queue order, then let one immediate transaction own every
            // modality and every universal coordinator record.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = commit_mutation_batch_crossmodal(
                db,
                crate::redb_store::CrossModalCommitInput {
                    graph_fname: &graph,
                    batch: &batch,
                    rows: crate::redb_store::CrossModalBatchRows {
                        methods: &methods,
                        vectors: &vectors,
                        blob_refs: &blob_refs,
                        measurements: &measurements,
                    },
                    result_msgpack: result_msgpack.as_deref(),
                    committed_at_ms,
                },
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            );
            let _ = done.send(res);
            false
        }
        Cmd::MutationBatchCommit { payload, done } => {
            let MutationBatchPayload {
                graph,
                batch,
                authoritative_state_msgpack,
                result_msgpack,
                committed_at_ms,
                audited,
            } = *payload;
            // Preserve command ordering and make the batch its own indivisible
            // commit point.  Pending best-effort/grouped writes land first; none
            // can be folded into or acknowledged as part of half this batch.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = if let Some(state) = authoritative_state_msgpack.as_deref() {
                commit_mutation_batch_state(
                    db,
                    crate::redb_store::StateCommitInput {
                        graph_fname: &graph,
                        batch: &batch,
                        authoritative_state_msgpack: state,
                        result_msgpack: result_msgpack.as_deref(),
                        committed_at_ms,
                        audited,
                    },
                    crypto,
                    #[cfg(feature = "security")]
                    &mut pending.audit_tail,
                )
            } else {
                commit_mutation_batch(
                    db,
                    &graph,
                    &batch,
                    result_msgpack.as_deref(),
                    committed_at_ms,
                    crypto,
                    #[cfg(feature = "security")]
                    &mut pending.audit_tail,
                )
            };
            let _ = done.send(res);
            false
        }
        Cmd::MintWorkItemClaimCapability {
            graph,
            request,
            authority,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = (|| {
                let mut wtx = db.begin_write().map_err(|error| error.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|error| error.to_string())?;
                let result = crate::redb_store::work_item_capability::mint_in_wtx(
                    &wtx, &graph, &request, &authority, crypto,
                )?;
                wtx.commit().map_err(|error| error.to_string())?;
                Ok(result)
            })();
            let _ = done.send(res);
            false
        }
        Cmd::VerifyWorkItemClaimCapability {
            graph,
            request,
            authority,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = (|| {
                let mut wtx = db.begin_write().map_err(|error| error.to_string())?;
                wtx.set_durability(Durability::Immediate)
                    .map_err(|error| error.to_string())?;
                let result = crate::redb_store::work_item_capability::verify_in_wtx(
                    &wtx, &graph, &request, &authority, crypto,
                )?;
                wtx.commit().map_err(|error| error.to_string())?;
                Ok(result)
            })();
            let _ = done.send(res);
            false
        }
        Cmd::CommitDevelopmentLane {
            graph,
            method,
            now_ms,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = crate::redb_store::development_lane::commit_development_lane(
                db, &graph, &method, now_ms, crypto,
            );
            let _ = done.send(res);
            false
        }
        Cmd::ChangeEnvelopeCommit { payload, done } => {
            let ChangeEnvelopePayload {
                graph,
                envelope,
                committed_at_ms,
            } = *payload;
            // Ordering and atomicity mirror MutationBatch: pending grouped writes
            // commit first, then this envelope owns one indivisible fsync point.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = commit_change_envelope(
                db,
                &graph,
                &envelope,
                committed_at_ms,
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            );
            let _ = done.send(res);
            false
        }
        Cmd::ChangeEnvelopesCommit { payload, done } => {
            let ChangeEnvelopesPayload {
                graph,
                envelopes,
                committed_at_ms,
            } = *payload;
            // Same ordering/atomicity as the single envelope: flush any pending
            // grouped writes first, then this whole page owns one indivisible fsync.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = commit_change_envelopes(
                db,
                &graph,
                &envelopes,
                committed_at_ms,
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
            )
            .map_err(|e| (e.index, e.error));
            let _ = done.send(res);
            false
        }
        Cmd::MutationOutboxClaim {
            graph,
            consumer,
            now_ms,
            lease_ms,
            limit,
            done,
        } => {
            // A claim observes every prior batch/outbox write and installs all
            // returned leases atomically before any worker is notified.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let result =
                claim_mutation_outbox(db, &graph, &consumer, now_ms, lease_ms, limit, crypto);
            let _ = done.send(result);
            false
        }
        Cmd::MutationOutboxAck {
            graph,
            lease,
            projection,
            now_ms,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let result = ack_mutation_outbox(db, &graph, &lease, &projection, now_ms, crypto);
            let _ = done.send(result);
            false
        }
        Cmd::Shutdown { reply } => {
            let _ = reply.send(());
            true
        }
        Cmd::RaftLogAppend {
            group_id,
            entries,
            done,
        } => {
            // Buffer into the SAME pending batch as M2 mutations; the awaited `done`
            // makes this a commit-before-ack barrier, so the batch commits durably at
            // the next boundary (or immediately, since has_barrier() is now true) and
            // a concurrently-pending graph mutation rides the SAME fsync.
            for (idx, blob) in entries {
                pending.raft_log_ops.push((group_id, idx, blob));
            }
            pending.waiters.push(done);
            false
        }
        Cmd::RaftLogRead {
            group_id,
            lo,
            hi,
            reply,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_raft_log_range(db, group_id, lo, hi, crypto));
            false
        }
        Cmd::RaftLogDeleteFrom {
            group_id,
            from,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(delete_raft_log_from(db, group_id, from));
            false
        }
        Cmd::RaftLogPurgeUpto {
            group_id,
            upto,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(purge_raft_log_upto(db, group_id, upto));
            false
        }
        Cmd::RaftLogBounds { group_id, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(raft_log_bounds(db, group_id));
            false
        }
        Cmd::RaftMetaPut {
            group_id,
            key,
            val,
            done,
        } => {
            // Flush pending first so meta ordering is consistent with the log, then
            // durably write the meta row in its own transaction.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(put_raft_meta(db, group_id, &key, &val));
            false
        }
        Cmd::RaftMetaGet {
            group_id,
            key,
            reply,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(get_raft_meta(db, group_id, &key));
            false
        }
        Cmd::XshardPreparePut {
            txn_id,
            group_id,
            slice,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(put_xshard_prepare(db, &txn_id, group_id, &slice, crypto));
            false
        }
        Cmd::XshardPrepareGet {
            txn_id,
            group_id,
            reply,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(get_xshard_prepare(db, &txn_id, group_id, crypto));
            false
        }
        Cmd::XshardDecisionPut {
            txn_id,
            commit,
            retain_for_parent,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(put_xshard_decision(db, &txn_id, commit, retain_for_parent));
            false
        }
        Cmd::XshardRecoverablePendingPut { txn_id, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(put_xshard_recoverable_pending(db, &txn_id));
            false
        }
        Cmd::XshardPrepareClear {
            txn_id,
            group_id,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(clear_xshard_prepare(db, &txn_id, group_id));
            false
        }
        Cmd::XshardDecisionClear { txn_id, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(clear_xshard_decision(db, &txn_id));
            false
        }
        Cmd::XshardScanPrepares { reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(scan_xshard_prepares(db, crypto));
            false
        }
        Cmd::XshardScanDecisions { reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(scan_xshard_decisions(db));
            false
        }
        Cmd::XshardDecisionGet { txn_id, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(get_xshard_decision(db, &txn_id));
            false
        }
        Cmd::XshardDecisionRetainGet { txn_id, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(get_xshard_decision_retain(db, &txn_id));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewPut { name, blob, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(crate::redb_store::put_matview(db, &name, &blob));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewScan { reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(crate::redb_store::scan_matviews(db));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewPut { name, blob, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(crate::redb_store::put_plan_matview(db, &name, &blob));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewDelete { name, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(crate::redb_store::delete_plan_matview(db, &name));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewScan { reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(crate::redb_store::scan_plan_matviews(db));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStatePut { name, blob, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(crate::redb_store::put_matview_operator_state(
                db, &name, &blob,
            ));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStateDelete { name, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(crate::redb_store::delete_matview_operator_state(db, &name));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStateScan { reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(crate::redb_store::scan_matview_operator_state(db));
            false
        }
    }
}

/// Commit all buffered mutations in ONE write transaction at the given durability,
/// then fire EVERY commit-before-ack waiter for the ops in this batch with the
/// batch's result (CONCEPT:EG-KG.backend.authoritative-dispatch). Coalescing is preserved: N awaiting writers
/// ride one `WriteTransaction` / one fsync and are all notified after it commits.
/// A waiter is only signalled `Ok` once its op is provably on disk.
fn commit_and_notify(
    db: &Database,
    pending: &mut Pending,
    durability: Durability,
    crypto: crate::redb_store::DurableCrypto<'_>,
) {
    if pending.is_empty() {
        return;
    }
    let res = commit_ops(
        db,
        &mut pending.ops,
        &mut pending.raft_log_ops,
        durability,
        crypto,
        // O(1) audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store), persistent across batches.
        #[cfg(feature = "security")]
        &mut pending.audit_tail,
    );
    let waiters = std::mem::take(&mut pending.waiters);
    let signal = res.map(|_| ());
    for w in waiters {
        let _ = w.send(signal.clone());
    }
}

// commit_ops / write_graph_meta / read_one_node now live in `crate::redb_store`
// (imported above) — shared verbatim with the embedded path, ONE durable format.

// ── Raft log/meta helpers (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — run on the writer thread ───────

/// Read a `[lo, hi]` inclusive log range for one group, in index order.
fn read_raft_log_range(
    db: &Database,
    gid: u64,
    lo: u64,
    hi: u64,
    crypto: crate::redb_store::DurableCrypto<'_>,
) -> Result<Vec<Vec<u8>>, String> {
    const MAX_RAFT_LOG_READ_ENTRIES: usize = 100_000;
    const MAX_RAFT_LOG_READ_BYTES: usize = 1024 * 1024 * 1024;
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut total_bytes = 0usize;
    for kv in t.range((gid, lo)..=(gid, hi)).map_err(|e| e.to_string())? {
        if out.len() >= MAX_RAFT_LOG_READ_ENTRIES {
            return Err("raft log read exceeds resource limits".to_string());
        }
        let (_, v) = kv.map_err(|e| e.to_string())?;
        let value = crypto.unseal(v.value())?;
        total_bytes = total_bytes
            .checked_add(value.len())
            .filter(|total| *total <= MAX_RAFT_LOG_READ_BYTES)
            .ok_or_else(|| "raft log read exceeds resource limits".to_string())?;
        out.push(value);
    }
    Ok(out)
}

/// Delete entries with index >= `from` for one group (conflict truncation).
fn delete_raft_log_from(db: &Database, gid: u64, from: u64) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
        let keys: Vec<u64> = t
            .range((gid, from)..=(gid, u64::MAX))
            .map_err(|e| e.to_string())?
            .filter_map(|kv| kv.ok().map(|(k, _)| k.value().1))
            .collect();
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Delete entries with index <= `upto` for one group (purge/compaction).
fn purge_raft_log_upto(db: &Database, gid: u64, upto: u64) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
        let keys: Vec<u64> = t
            .range((gid, 0)..=(gid, upto))
            .map_err(|e| e.to_string())?
            .filter_map(|kv| kv.ok().map(|(k, _)| k.value().1))
            .collect();
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// (first, last) present log index for one group.
fn raft_log_bounds(db: &Database, gid: u64) -> LogBoundsResult {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut range = t
        .range((gid, 0)..=(gid, u64::MAX))
        .map_err(|e| e.to_string())?;
    let first = range
        .next()
        .and_then(|kv| kv.ok().map(|(k, _)| k.value().1));
    // Re-scan for the last (the iterator was advanced by `next`).
    let last = t
        .range((gid, 0)..=(gid, u64::MAX))
        .map_err(|e| e.to_string())?
        .next_back()
        .and_then(|kv| kv.ok().map(|(k, _)| k.value().1));
    Ok((first, last))
}

/// Durably write one Raft metadata key for a group.
fn put_raft_meta(db: &Database, gid: u64, key: &str, val: &[u8]) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
        t.insert((gid, key), val).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Read one Raft metadata key for a group.
fn get_raft_meta(db: &Database, gid: u64, key: &str) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
    Ok(t.get((gid, key))
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::Request;
    use crate::registry::GraphRegistry;
    use crate::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};
    #[cfg(feature = "tsdb")]
    use sha2::Digest;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Keep the full (`--features full`) dispatcher's state machine behind one heap
    /// indirection — mirrors `src/cost.rs`'s and `src/server/handlers/query.rs`'s
    /// `dispatch_on_heap`. `dispatch_on_heap()` bottoms out in `dispatch_inner`
    /// (`src/server/dispatch.rs`), a single ~8k-line async fn whose generated
    /// `Future` is sized to the UNION of every feature-gated `Method` match arm;
    /// under `full` every arm is compiled in, so that future is large. Awaiting it
    /// INLINE embeds the whole thing in the caller's own generated state machine,
    /// and a test that awaits several in sequence exhausts the harness thread's
    /// stack — which is exactly how
    /// `delete_then_recreate_same_name_keeps_new_writes` aborted CI's
    /// `Test (facade full)` step with `has overflowed its stack` / SIGABRT (and
    /// took 29 sibling tests down with it as collateral, since the whole test
    /// process dies). Route every `dispatch` call in this module through here.
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::protocol::Response> + Send + 'a>>
    {
        Box::pin(crate::server::dispatch(state, request))
    }

    const TEST_AGENT: &str = "unit-test-agent";
    static AUTH_NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn current_isolation() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation
    }

    fn current_request(secret: &str, id: u64, graph: &str, method: Method) -> Request {
        // See `cost.rs`'s `req()` for why this is `Once`-guarded: process-global
        // `set_var`, called from every request built by every test in this module.
        static TEST_AUTH_ENV: std::sync::Once = std::sync::Once::new();
        TEST_AUTH_ENV.call_once(|| {
            std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
            std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
            std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
            std::env::set_var(
                "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
                std::env::temp_dir()
                    .join(format!("epistemic-graph-unit-auth-{}", std::process::id())),
            );
        });
        let context = RequestContextClaims {
            principal: TEST_AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: TEST_AGENT.to_string(),
            roles: Vec::new(),
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = AUTH_NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "redb-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("redb-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            secret,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    #[cfg(feature = "tsdb")]
    fn scoped_series_key(graph: &str, series: &str, principal: &str) -> String {
        let tenant_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "carrier-tenant",
            "verified",
            "tenant-shared",
        );
        let actor_scope = format!(
            "principal:sha256:{}",
            hex::encode(sha2::Sha256::digest(principal.as_bytes()))
        );
        let owner_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "carrier-owner",
            &tenant_scope,
            &actor_scope,
        );
        let graph_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "timeseries-graph",
            &owner_scope,
            graph,
        );
        eg_tsdb::store::SeriesKey::new(tenant_scope, graph_scope, series).encode()
    }

    #[cfg(feature = "tsdb")]
    fn direct_test_series_key(graph: &str, series: &str) -> String {
        scoped_series_key(graph, series, &format!("principal:{TEST_AGENT}"))
    }

    #[cfg(feature = "tsdb")]
    fn envelope_test_series_key(graph: &str, series: &str) -> String {
        scoped_series_key(graph, series, TEST_AGENT)
    }

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// A minimal `ServerState` (no persistence backend stored on it — the test
    /// drives the backend directly) with a persist dir set.
    fn new_state(persist_dir: Option<String>) -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            persist_dir,
            persistence: None,
            max_in_flight: Arc::new(tokio::sync::Semaphore::new(16)),
            read_admission: Arc::new(tokio::sync::Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(dashmap::DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redb_durable_roundtrip() {
        // Held for the whole test: this test opens the backend TWICE (write side,
        // then a fresh reload side) and both opens must resolve the SAME
        // encryption-at-rest cipher or the reload's read fails with "encrypted
        // durable value is missing sealed framing" -- same requirement as
        // `k_gt_1_routes_to_deterministic_shard_and_survives_restart` above. Never
        // sets the key itself; only needs the ambient value to stay constant across
        // both opens. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        // Commit nodes/edges through the awaited barrier, drop, and reload.
        let dir = std::env::temp_dir().join(format!("eg-redb-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        // ── write side ──
        let backend = RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
            .expect("open redb backend");
        backend
            .register_graph("__commons__", "__commons__", GraphType::Commons)
            .await
            .unwrap();
        backend
            .register_graph("g1", "g1", GraphType::Global)
            .await
            .unwrap();

        // The registry must have the graph for checkpoint to dump it; build a
        // minimal ServerState with the graph created + populated in memory.
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
        }
        // Apply mutations in memory AND record them through the backend. Fetch g1
        // by name — registry iteration order is not stable (HashMap), so
        // all_entries()[0] could be the pre-created `__commons__`.
        let core = {
            let s = state.read().await;
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        core.add_node(
            "a".into(),
            props(serde_json::json!({"type": "Task", "n": 1})),
        );
        core.add_node("b".into(), props(serde_json::json!({"type": "Task"})));
        let _ = core.add_edge("a".into(), "b".into(), props(serde_json::json!({"w": 2})));

        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Task", "n": 1})),
                },
            )
            .await
            .unwrap();
        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "b".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Task"})),
                },
            )
            .await
            .unwrap();
        backend
            .record_durable(
                "g1",
                &Method::AddEdge {
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: props(serde_json::json!({"w": 2})),
                },
            )
            .await
            .unwrap();
        backend.shutdown();
        drop(backend);

        // ── reload side: fresh backend + fresh empty state ──
        let backend2 = RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
            .expect("reopen redb backend");
        let state2 = new_state(Some(dir_s.clone()));
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert_eq!(loaded, 2, "g1 + __commons__ reloaded from redb");

        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("g1")
                .map(|e| e.core.clone())
                .expect("g1 reloaded")
        };
        assert_eq!(core2.node_count(), 2);
        assert_eq!(
            core2.get_node_properties("a"),
            Some(props(serde_json::json!({"type": "Task", "n": 1})))
        );
        assert_eq!(core2.get_edges().len(), 1);
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.storage.occ-durable-commit — a committed OCC transaction is durable through the redb
    /// backend: stage nodes/edge in a txn, commit through the full dispatch path
    /// (which commits its MutationBatch durably), drop, then
    /// reload via redb-only → the committed graph is recovered.
    #[tokio::test(flavor = "multi_thread")]
    async fn txn_commit_persists_to_redb() {
        use crate::protocol::ResultPayload;

        // Held for the whole test: the `Commit` below reaches `seal_txn_recovery_plan`
        // (fails closed without a configured `EPISTEMIC_GRAPH_ENCRYPTION_KEY`), and
        // this test ALSO reopens the backend ("reload via redb-only") — both opens
        // must resolve the same cipher. See `crate::crypto::acquire_test_env_lock`'s
        // doc for the full mechanism.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        {
            static ENCRYPTION_KEY: std::sync::Once = std::sync::Once::new();
            ENCRYPTION_KEY.call_once(|| {
                std::env::set_var(
                    crate::crypto::ENCRYPTION_KEY_ENV,
                    "redb-backend-txn-test-recovery-key",
                );
            });
        }

        const SECRET: &str = "redb-txn-secret";
        let dir = std::env::temp_dir().join(format!("eg-redb-txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> = Arc::new(
            RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
                .expect("open redb backend"),
        );
        backend
            .register_graph("__commons__", "__commons__", GraphType::Commons)
            .await
            .unwrap();
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }

        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);
        let txn = match dispatch_on_heap(
            &state,
            req(
                1,
                Method::BeginTxn {
                    graph: None,
                    isolation: None,
                },
            ),
        )
        .await
        .result
        {
            Some(ResultPayload::String(t)) => t,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        for (rid, nid) in [(2u64, "x"), (3, "y")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"type": "Task"})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(matches!(r.result, Some(ResultPayload::Bool(true))));
        }
        let r = dispatch_on_heap(
            &state,
            req(
                4,
                Method::TxnAddEdge {
                    txn_id: txn.clone(),
                    source_id: "x".into(),
                    target_id: "y".into(),
                    properties_msgpack: props(serde_json::json!({})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

        let r = dispatch_on_heap(
            &state,
            req(
                5,
                Method::Commit {
                    txn_id: txn,
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "commit ok: {:?}",
            r.error
        );

        backend.shutdown();
        // `shutdown()` stops the writer threads but does NOT close each shard's
        // `Database`; redb holds its advisory file lock for as long as any
        // `Arc<RedbBackend>` lives, so an in-process reopen of the same directory
        // fails with "Database already open. Cannot acquire lock." unless every
        // reference is actually released first.
        {
            let mut s = state.write().await;
            s.persistence = None;
        }
        drop(backend);

        let backend2 = RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
            .expect("reopen redb backend");
        let state2 = new_state(Some(dir_s.clone()));
        backend2.load_all(&state2).await.unwrap();
        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("__commons__")
                .map(|e| e.core.clone())
                .expect("__commons__ reloaded")
        };
        assert!(
            core2.has_node("x") && core2.has_node("y"),
            "committed txn nodes durable in redb"
        );
        assert_eq!(
            core2.get_edges().len(),
            1,
            "committed txn edge durable in redb"
        );
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.tenant-delete-recreate-same — tenant DELETE + recreate-same-name must not drop the new
    /// graph's writes. Under redb-authoritative mode (read-through wired exactly as
    /// `main.rs` does it) we: create "g", add "n1"={v:1}, DELETE "g", recreate "g",
    /// add "n1"={v:2}, then read "n1" back through the full dispatch path. The
    /// recreated graph's node MUST read back as {v:2} — not the stale {v:1} left in
    /// redb by the first incarnation, not empty. Mirrors the agent-utilities
    /// `test_find_analogous_subgraphs` tenant-churn failure at the engine level.
    #[tokio::test(flavor = "multi_thread")]
    async fn delete_then_recreate_same_name_keeps_new_writes() {
        // Held for the whole test: opens the backend TWICE (initial + an in-process
        // reopen of the SAME dir, see the retry loop below) and both opens must
        // resolve the same encryption-at-rest cipher. See
        // `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        use crate::protocol::ResultPayload;
        use crate::server::persistence::read_through::BackendReadThroughFactory;

        const SECRET: &str = "redb-recreate";
        let dir = std::env::temp_dir().join(format!("eg-redb-recreate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
            // Wire the durable read-through exactly like main.rs does under
            // authoritative mode — this is the read path that serves a RAM miss.
            let factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
            s.registry.set_read_through_factory(factory);
        }

        let req = |id: u64, method: Method| current_request(SECRET, id, "g", method);
        let create = |id: u64| {
            req(
                id,
                Method::CreateGraph {
                    graph_name: "g".into(),
                    graph_type: GraphType::Global,
                },
            )
        };
        let add = |id: u64, node: &str, v: i64| {
            req(
                id,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: props(serde_json::json!({"v": v})),
                },
            )
        };
        let get = |id: u64, node: &str| {
            req(
                id,
                Method::GetNodeProperties {
                    node_id: node.into(),
                },
            )
        };

        // First incarnation: create + write n1={v:1} and stale={v:9}.
        assert!(dispatch_on_heap(&state, create(1)).await.error.is_none());
        assert!(dispatch_on_heap(&state, add(2, "n1", 1))
            .await
            .error
            .is_none());
        assert!(dispatch_on_heap(&state, add(3, "stale", 9))
            .await
            .error
            .is_none());

        // Delete the tenant.
        let del = dispatch_on_heap(
            &state,
            req(
                4,
                Method::DeleteGraph {
                    graph_name: "g".into(),
                },
            ),
        )
        .await;
        assert!(del.error.is_none(), "delete: {:?}", del.error);

        // Recreate SAME name. The new tenant writes ONLY n1={v:2}; it never writes
        // "stale" — that node belongs to the deleted incarnation and must be gone.
        let recreate = dispatch_on_heap(&state, create(5)).await;
        assert!(recreate.error.is_none(), "recreate: {:?}", recreate.error);
        assert!(dispatch_on_heap(&state, add(6, "n1", 2))
            .await
            .error
            .is_none());

        // (a) LIVE read-through: force every node out of RAM so the next read
        // RAM-MISSES and falls to the durable read-through (the eviction path is real
        // under authoritative mode — it bounds memory per CONCEPT:EG-KG.storage.read-through-seam-exercised).
        let ev = dispatch_on_heap(&state, req(7, Method::EvictLRU { max_nodes: 0 })).await;
        assert!(ev.error.is_none(), "evict: {:?}", ev.error);

        // The deleted incarnation's "stale" node must NOT resurrect from redb on a
        // RAM-miss read of the recreated graph.
        let r = dispatch_on_heap(&state, get(8, "stale")).await;
        let stale = match r.result {
            Some(ResultPayload::PropertiesMsgpack(b)) => Some(b),
            Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
            other => panic!("unexpected get result: {other:?}"),
        };
        assert_eq!(
            stale, None,
            "deleted tenant's node 'stale' resurrected via read-through after recreate"
        );

        // And n1 reads back as the NEW write {v:2}.
        let r = dispatch_on_heap(&state, get(9, "n1")).await;
        let got = match r.result {
            Some(ResultPayload::PropertiesMsgpack(b)) => Some(b),
            Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
            other => panic!("unexpected get result: {other:?}"),
        };
        assert_eq!(
            got,
            Some(props(serde_json::json!({"v": 2}))),
            "recreated tenant's node n1 must read back as the NEW write {{v:2}}, not stale/empty"
        );
        // `shutdown()` stops each shard's writer THREAD but does not close its
        // `Database` — the handle lives in the `Shard`, so redb keeps its advisory
        // file lock for as long as ANY `Arc<RedbBackend>` survives (see the
        // identical note in `many_recreate_cycles_keep_inmemory_writes_visible`).
        // This test holds THREE: the local `backend`, the clone parked in
        // `state.persistence`, and the clone `BackendReadThroughFactory` wraps
        // inside `state.registry`'s installed read-through factory. `state` is not
        // read again after this point (part (b) below builds its own `state2`), so
        // dropping it wholesale — instead of clearing each field individually and
        // still risking a missed clone — is what actually releases the lock; the
        // sibling `many_recreate_cycles_keep_inmemory_writes_visible` gets away
        // with clearing only `persistence` because IT never installs a read-through
        // factory on `state` at all.
        backend.shutdown();
        drop(backend);
        drop(state);

        // The per-graph write-coalescer's background worker (spawned for "g"'s
        // SECOND incarnation by `add(6, ...)`, `write_coalescer::GraphWriter::spawn`)
        // also holds an `Arc<GraphCore>` — via its `read_through` field, an
        // `Arc<dyn PersistenceBackend>` clone — for as long as its own tokio task
        // is alive. Dropping `state` above synchronously drops the LAST `Sender`
        // into that worker's channel, but the worker task itself only notices the
        // channel closed (and drops its captured `core`) on its NEXT poll — an
        // async race `GraphWriter::spawn`'s own `JoinHandle` is discarded, so
        // nothing here can directly await. This is a test-only artifact of
        // reopening the SAME redb file IN-PROCESS (in production the OS releases
        // the file lock on process exit regardless — same rationale as the
        // `s.persistence = None` note on `dispatch_authoritative_durable_without_checkpoint`),
        // so bound it with a short retry rather than a flat sleep.
        let backend2: Arc<dyn crate::server::persistence::PersistenceBackend> = {
            let mut attempt = 0;
            loop {
                match RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256) {
                    Ok(backend2) => break Arc::new(backend2),
                    Err(error) if attempt < 100 => {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        let _ = error;
                    }
                    Err(error) => panic!("reopen: {error:?}"),
                }
            }
        };
        let state2 = new_state(Some(dir_s.clone()));
        backend2.load_all(&state2).await.unwrap();
        let core2 = {
            let s = state2.read().await;
            s.registry.get("g").map(|e| e.core.clone())
        };
        if let Some(core2) = core2 {
            assert!(
                !core2.has_node("stale"),
                "deleted tenant's node 'stale' resurrected from redb on load_all after recreate"
            );
            assert_eq!(
                core2.get_node_properties("n1"),
                Some(props(serde_json::json!({"v": 2}))),
                "recreated tenant's n1 must survive a reload as {{v:2}}"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.many-repeated-create-delete — MANY repeated create→delete→recreate cycles on the SAME
    /// graph name must NOT enter a corrupted in-memory state that silently drops the
    /// recreated graph's writes (the vault-lane `__secrets__` failure). DISTINCT from
    /// KG-2.221 (durable redb purge): here every read is served HOT from RAM (no
    /// eviction, no reload) so the bug is purely the in-memory per-graph state keyed
    /// by name that DeleteGraph failed to reset — specifically the write-coalescer's
    /// cached `GraphWriter`, whose worker owns an `Arc<GraphCore>` of the DELETED
    /// incarnation. On recreate, `writer_for` returned the STALE writer (keyed by
    /// name) and routed the new tenant's writes into the orphaned core; the registry's
    /// fresh GraphCore stayed empty, so a hot RAM read saw nothing. The corruption
    /// accumulates: it appears the first cycle the coalescer batched a write.
    ///
    /// Tested for BOTH a plain name and a `__…__`-style reserved name (the report saw
    /// it on `__secrets__`). 50 cycles, each writing a DIFFERENT node so a stale read
    /// can't masquerade as a fresh one.
    #[tokio::test(flavor = "multi_thread")]
    async fn many_recreate_cycles_keep_inmemory_writes_visible() {
        use crate::protocol::ResultPayload;
        use crate::server::persistence::read_through::BackendReadThroughFactory;

        const SECRET: &str = "redb-churn";
        const CYCLES: u64 = 50;

        for graph in ["g", "__secrets__"] {
            let dir = std::env::temp_dir().join(format!(
                "eg-redb-churn-{}-{}",
                std::process::id(),
                graph.trim_matches('_')
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();

            let backend: Arc<dyn crate::server::persistence::PersistenceBackend> = Arc::new(
                RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256).expect("open"),
            );
            let state = new_state(Some(dir_s.clone()));
            {
                let mut s = state.write().await;
                s.auth_secret = SECRET.to_string();
                s.persistence = Some(backend.clone());
                let factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
                s.registry.set_read_through_factory(factory);
            }

            let req = |id: u64, method: Method| current_request(SECRET, id, graph, method);

            let mut id = 0u64;
            let mut next = || {
                id += 1;
                id
            };

            for cycle in 0..CYCLES {
                // create
                let c = dispatch_on_heap(
                    &state,
                    req(
                        next(),
                        Method::CreateGraph {
                            graph_name: graph.into(),
                            graph_type: GraphType::Global,
                        },
                    ),
                )
                .await;
                assert!(c.error.is_none(), "cycle {cycle} create: {:?}", c.error);

                // write a node UNIQUE to this cycle
                let node = format!("n{cycle}");
                let a = dispatch_on_heap(
                    &state,
                    req(
                        next(),
                        Method::AddNode {
                            node_id: node.clone(),
                            properties_msgpack: props(serde_json::json!({"cycle": cycle})),
                        },
                    ),
                )
                .await;
                assert!(a.error.is_none(), "cycle {cycle} add: {:?}", a.error);

                // read it back HOT from RAM (no eviction) — the new tenant's write
                // MUST be visible. This is where a stale coalescer routes the write
                // into the deleted core and the fresh core reads back empty.
                let r = dispatch_on_heap(
                    &state,
                    req(
                        next(),
                        Method::GetNodeProperties {
                            node_id: node.clone(),
                        },
                    ),
                )
                .await;
                let got = match r.result {
                    Some(ResultPayload::PropertiesMsgpack(b)) => Some(b),
                    Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
                    other => panic!("cycle {cycle} unexpected get result: {other:?}"),
                };
                assert_eq!(
                    got,
                    Some(props(serde_json::json!({"cycle": cycle}))),
                    "graph {graph:?} cycle {cycle}: recreated tenant's write to {node:?} \
                     was silently dropped (in-memory churn corruption)"
                );

                // IN-MEMORY proof: NodeCount reads the registry's live GraphCore
                // directly (NO durable read-through), so it sees ONLY what actually
                // landed in RAM. The recreated graph must hold EXACTLY this cycle's one
                // node. If the stale coalescer routed the write to the deleted core,
                // the live core is empty and this is 0 — the durable read-through above
                // would otherwise MASK the corruption by serving redb.
                let nc = dispatch_on_heap(&state, req(next(), Method::NodeCount)).await;
                let count = match nc.result {
                    Some(ResultPayload::Count(c)) => c,
                    other => panic!("cycle {cycle} unexpected node-count result: {other:?}"),
                };
                assert_eq!(
                    count, 1,
                    "graph {graph:?} cycle {cycle}: recreated tenant's live GraphCore must hold \
                     exactly the 1 node written this cycle (in-memory write was dropped)"
                );

                // delete (skip on the last cycle so we leave the graph live)
                if cycle + 1 < CYCLES {
                    let d = dispatch_on_heap(
                        &state,
                        req(
                            next(),
                            Method::DeleteGraph {
                                graph_name: graph.into(),
                            },
                        ),
                    )
                    .await;
                    assert!(d.error.is_none(), "cycle {cycle} delete: {:?}", d.error);
                }
            }

            backend.shutdown();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// CONCEPT:EG-KG.backend.authoritative-dispatch — full dispatch path under AUTHORITATIVE mode: a write acked
    /// through `dispatch` is durable in redb WITHOUT any checkpoint, and reloads via
    /// redb `load_all` (the authoritative source). This proves the commit-before-ack
    /// barrier covers the real dispatch write path (incl. the coalescer) AND that the
    /// graph is recoverable under its real name with no checkpoint (graph_meta is
    /// durably registered on create + backfilled on write).
    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_authoritative_durable_without_checkpoint() {
        // Held for the whole test: opens the backend TWICE (initial dispatch-driven
        // writes, then a redb-only reload) and both opens must resolve the same
        // encryption-at-rest cipher. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        use crate::protocol::ResultPayload;

        const SECRET: &str = "redb-auth-dispatch";
        let dir = std::env::temp_dir().join(format!("eg-redb-authd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "g_auth", method);
        // Create graph (durably registered) then write nodes — each dispatch returns
        // only after the durable commit.
        let r = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CreateGraph {
                    graph_name: "g_auth".into(),
                    graph_type: GraphType::Global,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "create: {:?}", r.error);
        for (rid, nid) in [(2u64, "a"), (3, "b"), (4, "c")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"id": nid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "addnode {nid}: {:?}", r.error);
            assert!(
                matches!(r.result, Some(ResultPayload::Bool(true)) | None) || r.error.is_none()
            );
        }

        // NO checkpoint. Drop the backend (flushes shutdown) and reload redb-only.
        //
        // `shutdown()` stops each shard's writer THREAD but does not close its
        // `Database` -- the handle lives in the `Shard`, so redb keeps its advisory
        // file lock for as long as ANY `Arc<RedbBackend>` survives. This test holds
        // two: the local `backend` and the clone parked in `state.persistence`.
        // Reopening the same directory in-process while either is alive fails with
        // "Database already open. Cannot acquire lock." (In production the process
        // exits and the OS releases the lock, which is why only an in-process
        // reopen like this one is affected.) So actually drop both -- which is what
        // this comment always claimed was happening.
        backend.shutdown();
        {
            let mut s = state.write().await;
            s.persistence = None;
        }
        drop(backend);

        let backend2 =
            RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256).expect("reopen");
        let state2 = new_state(Some(dir_s.clone()));
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert!(loaded >= 1, "graphs recovered from redb without checkpoint");
        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("g_auth")
                .map(|e| e.core.clone())
                .expect("g_auth recovered under real name")
        };
        assert!(core2.has_node("a") && core2.has_node("b") && core2.has_node("c"));
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.authoritative-dispatch — commit-before-ack: `record_durable` returns ONLY after the
    /// op is durably committed. Use DurabilityPolicy::Interval so the op is NOT committed
    /// by an Each-after-batch path; the only way the await completes is the group
    /// commit firing the waiter. After the await returns, a SEPARATE reopened DB sees
    /// the row — proving the await observed durable state, not just an enqueue.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_durable_awaits_commit() {
        let dir = std::env::temp_dir().join(format!("eg-redb-durable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(
            dir_s.clone(),
            DurabilityPolicy::Interval(Duration::from_millis(50)),
            64,
        )
        .expect("open");

        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({"v": 1})),
                },
            )
            .await
            .expect("durable commit");

        // The await returned ⇒ the op is committed. Verify via a point read on the
        // SAME backend (goes through the owner thread, reflecting committed state).
        let got = backend.read_node("g1", "a").await.expect("read");
        assert_eq!(got, Some(props(serde_json::json!({"v": 1}))));
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.authoritative-dispatch — many concurrent `record_durable` calls COALESCE into group
    /// commits (NOT one fsync per op): all N complete, all N are durable. We can't
    /// directly count fsyncs here, but we assert all N awaited writers resolve Ok and
    /// every node is durably present — the coalescing path (one WriteTransaction per
    /// batch firing all the batch's waiters) is what makes that terminate quickly.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_durable_coalesces_many_writers() {
        let dir = std::env::temp_dir().join(format!("eg-redb-coalesce-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(
            RedbBackend::open(
                dir_s.clone(),
                DurabilityPolicy::Interval(Duration::from_millis(20)),
                256,
            )
            .expect("open"),
        );

        let n = 200usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        // Every node durable.
        for i in 0..n {
            let got = backend.read_node("g1", &format!("n{i}")).await.unwrap();
            assert!(got.is_some(), "n{i} durable");
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CONCEPT:EG-KG.storage.snapshot-read-off-writer — snapshot reads off the writer ───────────────────────────
    //
    // The point-read / read-through path (`read_node`) serves directly from a redb
    // `begin_read()` MVCC snapshot on the target shard's shared `Database`, routed by
    // the SAME EG-026 `shard_for`. It NEVER routes through the writer thread's channel
    // and NEVER forces a group-commit. These tests pin that invariant:
    //   (a) a read-through never increments any shard's commit counter (EG-026), and
    //       reads route to the correct shard under K>1 and return the right value;
    //   (b) reads complete CONCURRENTLY while many writes are in flight (MVCC, not
    //       serialized behind the writer queue);
    //   (c) a read after a write-ack sees the latest committed value (consistency).
    // None mutate env, so they need no LINGER_ENV_LOCK guard.

    /// (a) Read-through serves the node from a snapshot and triggers NO writer commit.
    /// Proven via the per-shard commit counters (EG-026 `commit_stats_all`): after the
    /// writes settle, a burst of reads leaves EVERY shard's commit count UNCHANGED.
    /// Uses K=3 so we also prove reads route to the correct shard (the value comes
    /// back) and that no OTHER shard commits either.
    #[tokio::test(flavor = "multi_thread")]
    async fn read_through_snapshot_triggers_no_writer_commit() {
        let dir = std::env::temp_dir().join(format!("eg-redb-snapread-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        // K=3 explicit (cfg(test) defaults to 1; open_with_shards honors the request on
        // a fresh dir). Graph names spread across shards via FNV-1a routing.
        let backend = RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 64, 3)
            .expect("open sharded");
        assert_eq!(backend.shard_count(), 3, "K=3 honored on a fresh dir");

        // Write one node into several graphs (spanning shards) + await each ack.
        let graphs = ["ga", "gb", "gc", "gd", "ge", "gf"];
        for (i, g) in graphs.iter().enumerate() {
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: "n".into(),
                        properties_msgpack: props(serde_json::json!({ "g": g, "i": i })),
                    },
                )
                .await
                .expect("durable commit");
        }

        // Baseline: total commits across ALL shards, captured AFTER the writes settle.
        let baseline: u64 = backend.commit_stats_all().iter().map(|s| s.commits()).sum();

        // A burst of read-throughs on every graph (each routes to its owning shard via
        // `shard_for`, opens a `begin_read()` snapshot, returns the stored blob).
        for _ in 0..25 {
            for (i, g) in graphs.iter().enumerate() {
                let got = backend.read_node(g, "n").await.expect("snapshot read");
                assert_eq!(
                    got,
                    Some(props(serde_json::json!({ "g": g, "i": i }))),
                    "read routed to the correct shard for graph {g}"
                );
            }
        }
        // A genuinely absent node is still None (the snapshot read is not a fabricator).
        assert_eq!(
            backend.read_node("ga", "missing").await.expect("read"),
            None
        );

        // THE PROOF: not a single shard committed because of the reads.
        let after: u64 = backend.commit_stats_all().iter().map(|s| s.commits()).sum();
        assert_eq!(
            after, baseline,
            "reads must NOT route through the writer / force a commit \
             (commits {baseline} -> {after})"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (b) Reads succeed CONCURRENTLY while writes are in flight — the MVCC snapshot
    /// read does not serialize behind the durable write path. We fan a large burst of
    /// `record_durable` writes and, at the same time, fan a burst of reads of an
    /// already-committed seed node; every read resolves Ok and sees the seed value
    /// even while the writer is saturated with commits.
    #[tokio::test(flavor = "multi_thread")]
    async fn reads_run_concurrently_with_inflight_writes() {
        let dir = std::env::temp_dir().join(format!("eg-redb-snapconc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(
            RedbBackend::open(
                dir_s.clone(),
                DurabilityPolicy::Interval(Duration::from_millis(20)),
                256,
            )
            .expect("open"),
        );

        // Seed a committed node the readers will keep seeing.
        let seed = props(serde_json::json!({ "seed": true }));
        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "seed".into(),
                    properties_msgpack: seed.clone(),
                },
            )
            .await
            .expect("seed commit");

        let mut tasks = Vec::new();
        // Writers: many concurrent durable writes keep the single writer busy.
        for i in 0..200usize {
            let b = backend.clone();
            tasks.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("w{i}"),
                        properties_msgpack: props(serde_json::json!({ "i": i })),
                    },
                )
                .await
                .map(|_| None)
            }));
        }
        // Readers: concurrently snapshot-read the seed; must not block on the writer.
        for _ in 0..200usize {
            let b = backend.clone();
            let want = seed.clone();
            tasks.push(tokio::spawn(async move {
                let got = b.read_node("g1", "seed").await?;
                assert_eq!(got, Some(want), "concurrent read sees the committed seed");
                Ok::<Option<()>, String>(Some(()))
            }));
        }
        let mut reads_ok = 0usize;
        for t in tasks {
            if let Some(()) = t.await.unwrap().expect("read/write task ok") {
                reads_ok += 1;
            }
        }
        assert_eq!(reads_ok, 200, "every concurrent reader completed");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (c) A read after a write-ack sees the written value, and after an UPDATE-ack the
    /// snapshot reflects the NEW value — i.e. reads see the latest COMMITTED state per
    /// shard (commit-before-ack ⇒ an acked write is on disk ⇒ a fresh `begin_read`
    /// after the ack sees it).
    #[tokio::test(flavor = "multi_thread")]
    async fn read_after_ack_sees_latest_committed() {
        let dir = std::env::temp_dir().join(format!("eg-redb-snapack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64).expect("open");

        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({ "v": 1 })),
                },
            )
            .await
            .expect("commit v1");
        assert_eq!(
            backend.read_node("g1", "a").await.expect("read v1"),
            Some(props(serde_json::json!({ "v": 1 }))),
            "snapshot opened after the ack sees the committed write"
        );

        // Overwrite the same node; after the ack the snapshot reflects the new value.
        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({ "v": 2 })),
                },
            )
            .await
            .expect("commit v2");
        assert_eq!(
            backend.read_node("g1", "a").await.expect("read v2"),
            Some(props(serde_json::json!({ "v": 2 }))),
            "a fresh snapshot after the update-ack sees the LATEST committed value"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.adaptive-linger-coalesce — the adaptive micro-linger COALESCES concurrent in-flight
    /// authoritative writers into fewer, larger group commits WITHOUT losing
    /// durability. We fan N concurrent `record_durable` calls (each its own task, so
    /// the writer sees them arrive within the linger window) and assert:
    ///   * every awaited writer resolves Ok and every node is durably present
    ///     (durability guarantee unchanged — commit-before-ack still holds),
    ///   * the average batch size (`ops / commits`) climbs well above 1, i.e. the
    ///     linger folded many writers into one fsync (the profiled win),
    ///   * lingered commits were actually exercised.
    ///
    /// `DurabilityPolicy::Each` would commit per-drained-batch regardless, so we use
    /// `Interval` (the live authoritative cadence) where, pre-EG-024, a drained
    /// channel commits immediately at ~1 op/fsync.
    ///
    /// Serializes the env-mutating linger tests. `EPISTEMIC_GRAPH_REDB_GROUP_*` are
    /// process-global and read once inside `RedbBackend::open`, so two parallel tests
    /// setting different values would race the config read (the disabled test could
    /// observe the coalesce test's `2000`). Hold this across set_var → open →
    /// remove_var; `open` is sync so there is no await under the guard.
    static LINGER_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test(flavor = "multi_thread")]
    async fn micro_linger_coalesces_concurrent_writers() {
        let dir = std::env::temp_dir().join(format!("eg-redb-linger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            // Explicit, deterministic knobs; serialized vs the other env-mutating test.
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US", "2000");
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW", "256");
            let b = Arc::new(
                RedbBackend::open(
                    dir_s.clone(),
                    // Long interval so the ONLY thing that commits a batch is the barrier
                    // path (+ its micro-linger), never the tick.
                    DurabilityPolicy::Interval(Duration::from_millis(500)),
                    512,
                )
                .expect("open"),
            );
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US");
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW");
            b
        };

        let stats = backend.commit_stats();
        let n = 256usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        // Durability: every node present.
        for i in 0..n {
            assert!(
                backend
                    .read_node("g1", &format!("n{i}"))
                    .await
                    .unwrap()
                    .is_some(),
                "n{i} durable"
            );
        }
        // The win: many writers folded into far fewer commits than ops.
        let commits = stats.commits();
        let ops = stats.ops();
        assert!(ops >= n as u64, "all {n} ops counted (got {ops})");
        assert!(
            commits < n as u64,
            "linger must coalesce: {commits} commits for {ops} ops (expected << {n})"
        );
        assert!(
            stats.avg_batch() > 1.5,
            "avg batch {:.2} should be well above the 1-op/fsync baseline",
            stats.avg_batch()
        );
        assert!(stats.lingered() > 0, "micro-linger path was exercised");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.adaptive-linger-coalesce — with the linger DISABLED (`LINGER_US=0`) the writer falls back
    /// to the exact commit-on-drain behavior, and durability is identical. This pins
    /// the baseline the bench measures against and proves the knob is a real opt-out.
    #[tokio::test(flavor = "multi_thread")]
    async fn micro_linger_disabled_preserves_durability() {
        let dir = std::env::temp_dir().join(format!("eg-redb-nolinger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            // Serialized vs the coalesce test so its `2000` can't leak into our `open`.
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US", "0");
            let b = Arc::new(
                RedbBackend::open(
                    dir_s.clone(),
                    DurabilityPolicy::Interval(Duration::from_millis(50)),
                    256,
                )
                .expect("open"),
            );
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US");
            b
        };

        let stats = backend.commit_stats();
        let n = 64usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        for i in 0..n {
            assert!(
                backend
                    .read_node("g1", &format!("n{i}"))
                    .await
                    .unwrap()
                    .is_some(),
                "n{i} durable"
            );
        }
        // No commit ever lingered when the knob is 0.
        assert_eq!(
            stats.lingered(),
            0,
            "linger disabled ⇒ zero lingered commits"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CONCEPT:EG-KG.storage.read-through-seam-exercised — read-through-on-RAM-miss + safe authoritative eviction ──

    /// Populate `core` AND redb with `n` durable nodes, then install the read-through
    /// factory + the backend on `state` exactly as `main.rs` does under authoritative
    /// mode. Returns the live core for "g1".
    async fn seed_authoritative(
        backend: &Arc<dyn PersistenceBackend>,
        state: &Arc<RwLock<ServerState>>,
        n: usize,
    ) -> Arc<GraphCore> {
        {
            let mut s = state.write().await;
            s.persistence = Some(backend.clone());
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
            // Wire read-through exactly like startup (attaches to g1 + __commons__).
            let factory = Arc::new(
                crate::server::persistence::read_through::BackendReadThroughFactory::new(
                    backend.clone(),
                ),
            );
            s.registry.set_read_through_factory(factory);
        }
        let core = {
            let s = state.read().await;
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        for i in 0..n {
            let p = props(serde_json::json!({"type": "Task", "i": i}));
            // RAM
            core.add_node(format!("n{i}"), p.clone());
            // Durable (commit-before-ack) — every node is provably on disk.
            backend
                .record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: p,
                    },
                )
                .await
                .expect("durable commit");
        }
        core
    }

    /// (a) Memory bounded: filling past the cap under authoritative mode + read-through
    /// EVICTS down to the cap — RAM resident count is bounded, eviction actually ran.
    /// (b)/(c) An EVICTED node still reads back its correct properties via the
    /// read-through (it is not in RAM, but redb serves it) — no loss across the boundary.
    #[tokio::test(flavor = "multi_thread")]
    async fn authoritative_eviction_bounds_memory_and_reads_through() {
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));

        let n = 50usize;
        let cap = 10usize;
        let core = seed_authoritative(&backend, &state, n).await;
        assert_eq!(
            core.node_count(),
            n,
            "all {n} nodes resident before eviction"
        );

        // Evict down to the cap (the per-graph max-nodes backstop).
        let evicted = crate::persist::evict_oversized_all(&state, cap).await;
        assert_eq!(evicted, n - cap, "evicted everything above the cap");

        // (a) memory bounded: RAM resident count is at the cap, NOT n.
        assert_eq!(
            core.node_count(),
            cap,
            "RAM resident count bounded to the cap after eviction"
        );

        // (b)/(c) the EVICTED nodes (lowest indices n0..) are gone from RAM yet read
        // back their exact properties via read-through from redb — no data loss.
        assert!(!core.has_node("n0"), "n0 evicted from RAM topology");
        for i in 0..(n - cap) {
            let got = core.get_node_properties(&format!("n{i}"));
            assert_eq!(
                got,
                Some(props(serde_json::json!({"type": "Task", "i": i}))),
                "evicted node n{i} reads back correct properties via read-through"
            );
        }
        // A node still resident reads from RAM as before.
        for i in (n - cap)..n {
            assert!(core.has_node(&format!("n{i}")), "n{i} still resident");
            assert_eq!(
                core.get_node_properties(&format!("n{i}")),
                Some(props(serde_json::json!({"type": "Task", "i": i})))
            );
        }
        // A genuinely absent node is still None (read-through is not a fabricator).
        assert_eq!(core.get_node_properties("does-not-exist"), None);

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No data loss: a node NOT durably in redb is NEVER evicted, even if it is the
    /// LRU candidate. We add an extra RAM-only node with the LOWEST index (so it is
    /// first in the LRU order) but do NOT record it durably; eviction must keep it
    /// resident (durability unconfirmed) and instead evict only durable nodes.
    #[tokio::test(flavor = "multi_thread")]
    async fn authoritative_eviction_never_drops_undurable_node() {
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-safe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64).expect("open"));
        let state = new_state(Some(dir_s.clone()));

        // Insert the un-durable node FIRST so it has the lowest NodeIndex (front of
        // the LRU order — the one the cache would normally drop first).
        let core = {
            let mut s = state.write().await;
            s.persistence = Some(backend.clone());
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
            let factory = Arc::new(
                crate::server::persistence::read_through::BackendReadThroughFactory::new(
                    backend.clone(),
                ),
            );
            s.registry.set_read_through_factory(factory);
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        core.add_node(
            "ghost".into(),
            props(serde_json::json!({"type": "Task", "durable": false})),
        );
        // Now 10 durable nodes.
        for i in 0..10usize {
            let p = props(serde_json::json!({"type": "Task", "i": i}));
            core.add_node(format!("n{i}"), p.clone());
            backend
                .record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: p,
                    },
                )
                .await
                .expect("durable commit");
        }
        assert_eq!(core.node_count(), 11);

        // Cap = 5 ⇒ 6 candidates: ghost (lowest index) + n0..n4. Only the 5 durable
        // ones may be dropped; ghost is kept (its durability cannot be confirmed).
        let evicted = crate::persist::evict_oversized_all(&state, 5).await;
        assert_eq!(
            evicted, 5,
            "only the 5 confirmed-durable candidates evicted"
        );
        assert!(
            core.has_node("ghost"),
            "un-durable node kept resident — never evicted (no data loss)"
        );
        // The un-durable node has no redb row, so a (hypothetical) miss would not
        // resurrect it — which is exactly why eviction must not drop it. Confirm it
        // still reads from RAM.
        assert_eq!(
            core.get_node_properties("ghost"),
            Some(props(serde_json::json!({"type": "Task", "durable": false})))
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.storage.one-fsync-covers-raft — ONE fsync covers a Raft log entry AND its graph mutation.
    /// Under `DurabilityPolicy::Interval`, the only way an awaited op completes is the
    /// group commit firing. We launch a `record_durable` (M2 graph mutation) and a
    /// `raft_log_append` (Raft log entry) CONCURRENTLY into the same tick window;
    /// both share ONE `Pending` batch → ONE `WriteTransaction` → ONE fsync. We then
    /// prove BOTH landed durably (the graph row AND the log row).
    #[cfg(feature = "raft")]
    #[tokio::test(flavor = "multi_thread")]
    async fn raft_log_and_mutation_share_one_group_commit() {
        let dir = std::env::temp_dir().join(format!("eg-redb-1txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(
            RedbBackend::open(
                dir_s.clone(),
                DurabilityPolicy::Interval(Duration::from_millis(40)),
                64,
            )
            .expect("open"),
        );

        // Fire both into the SAME group-commit window, concurrently. With Interval
        // fsync, neither completes until the group commit fires — so if they both
        // resolve from ONE flush, they rode ONE transaction together.
        let b1 = backend.clone();
        let mutation = tokio::spawn(async move {
            b1.record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "shared".into(),
                    properties_msgpack: props(serde_json::json!({"v": 7})),
                },
            )
            .await
        });
        let b2 = backend.clone();
        let log_blob = rmp_serde::to_vec_named(&serde_json::json!({"entry": 1})).unwrap();
        let log = tokio::spawn(async move { b2.raft_log_append(0, vec![(1u64, log_blob)]).await });

        mutation.await.unwrap().expect("graph mutation durable");
        log.await.unwrap().expect("raft log append durable");

        // Both are on disk: the graph row AND the log row at (group 0, index 1).
        let node = backend.read_node("g1", "shared").await.expect("read node");
        assert_eq!(node, Some(props(serde_json::json!({"v": 7}))));
        let entries = backend.raft_log_read(0, 1, 1).expect("read log");
        assert_eq!(
            entries.len(),
            1,
            "the log entry committed in the same flush"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Cross-modal ACID (CONCEPT:EG-KG.txn.reader-never-sees-node) ─────────────────────────────────────

    use crate::protocol::{Response, ResultPayload};
    use crate::server::auth::VerifiedRequestContext;
    use crate::server::handlers::txn::try_handle as txn_try_handle;

    fn cm_dir(tag: &str) -> String {
        // The cross-modal handler-commit path seals its transaction recovery plan
        // (`server::handlers::txn::seal_txn_recovery_plan`), which fail-closed REQUIRES
        // `EPISTEMIC_GRAPH_ENCRYPTION_KEY` to be configured — the same seal requirement
        // the xshard harness hit. Provision it ONCE before any backend opens. Encryption
        // is symmetric and transparent to every durable round-trip these tests make, so
        // a keyed store behaves identically for their assertions. The env var is
        // process-global (mirrors `xshard_harness::fresh_dir`). Every caller of
        // `cm_dir` holds `crate::crypto::acquire_test_env_lock()` for its entire test
        // body (see each call site), so this `Once` always fires under that lock —
        // do NOT also acquire it here, `std::sync::Mutex` is not reentrant.
        static ENCRYPTION_KEY: std::sync::Once = std::sync::Once::new();
        ENCRYPTION_KEY.call_once(|| {
            std::env::set_var(
                crate::crypto::ENCRYPTION_KEY_ENV,
                "crossmodal-test-recovery-key",
            )
        });
        let d = std::env::temp_dir().join(format!("eg-crossmodal-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    fn as_bool(r: Response) -> Option<bool> {
        match r.result {
            Some(ResultPayload::Bool(b)) => Some(b),
            _ => None,
        }
    }

    async fn txn_handle(
        state: &Arc<RwLock<ServerState>>,
        req_id: u64,
        _caller: Option<&str>,
        method: Method,
    ) -> Result<Response, Method> {
        let context = VerifiedRequestContext::verified_for_test(TEST_AGENT);
        txn_try_handle(state, req_id, TEST_AGENT, &context, method).await
    }

    /// Drive BeginTxn(g) → TxnAddNode(node) → TxnAddEmbedding → TxnBlobRef, returning
    /// the txn id (the staged cross-modal write-set is ready to Commit).
    async fn stage_crossmodal(
        state: &Arc<RwLock<ServerState>>,
        graph: &str,
        node: &str,
        digest: &str,
    ) -> String {
        let begin = txn_handle(
            state,
            1,
            None,
            Method::BeginTxn {
                graph: Some(graph.to_string()),
                isolation: None,
            },
        )
        .await
        .unwrap();
        let txn_id = match begin.result {
            Some(ResultPayload::String(id)) => id,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    2,
                    None,
                    Method::TxnAddNode {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        properties_msgpack: props(serde_json::json!({"type": "Media"})),
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    3,
                    None,
                    Method::TxnAddEmbedding {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        embedding: vec![0.1, 0.2, 0.3],
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    4,
                    None,
                    Method::TxnBlobRef {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        digest: digest.to_string(),
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        txn_id
    }

    /// HAPPY: a cross-modal txn (node + vector + blob-ref) commits atomically — ALL
    /// modalities land durably in ONE WriteTransaction and survive a reload.
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_txn_commits_all_modalities_atomically() {
        // Held for the whole test: `cm_dir` provisions `EPISTEMIC_GRAPH_ENCRYPTION_KEY`
        // once (process-global) and this test's backend opens depend on it staying set
        // throughout — see `crate::crypto::acquire_test_env_lock`'s doc.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("happy");
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id = stage_crossmodal(&state, "media", "m1", "sha256:abc").await;
        // Nothing applied before commit.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "no apply before commit");
            assert_eq!(
                core.semantic_store.read().len(),
                0,
                "no vector before commit"
            );
        }

        // COMMIT — all three modalities land atomically.
        assert_eq!(
            as_bool(
                txn_handle(
                    &state,
                    5,
                    None,
                    Method::Commit {
                        txn_id,
                        idempotency_key: None
                    }
                )
                .await
                .unwrap()
            ),
            Some(true),
            "cross-modal commit"
        );

        // In-memory: node + vector + blob-ref property all present.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node landed");
            assert_eq!(core.semantic_store.read().len(), 1, "vector landed");
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc")
            );
        }
        backend.shutdown();
        drop(backend);
        // `state` holds a clone of the backend Arc; drop it so the LAST handle is gone
        // and the exclusive per-process redb file lock is released before the reopen.
        drop(state);

        // Reload from redb: every modality is DURABLE (the one WriteTransaction).
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node durable");
            assert_eq!(core.semantic_store.read().len(), 1, "vector durable");
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc"),
                "blob-ref durable"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A backend whose `commit_crossmodal` always FAILS — used to prove the handler
    /// rolls back ALL modalities (applies nothing in-memory) on a durable-commit
    /// failure: no partial cross-modal commit (CONCEPT:EG-KG.txn.reader-never-sees-node).
    struct FailingBackend {
        inner: Arc<RedbBackend>,
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for FailingBackend {
        async fn load_all(&self, s: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
            self.inner.load_all(s).await
        }
        async fn record_durable(&self, g: &str, m: &Method) -> Result<(), String> {
            self.inner.record_durable(g, m).await
        }
        async fn commit_crossmodal(
            &self,
            _g: &str,
            _m: &[Method],
            _v: &[(String, Vec<f32>)],
            _b: &[(String, String)],
            _meas: &[crate::MeasurementBatch],
        ) -> Result<(), String> {
            // Simulate a mid-way durable failure: NOTHING is written to redb.
            Err("injected durable commit failure".to_string())
        }
        fn shutdown(&self) {
            self.inner.shutdown()
        }
    }

    /// ROLLBACK: a cross-modal txn whose durable commit FAILS mid-way applies NONE of
    /// its modalities — no node, no vector, no blob-ref (no partial commit).
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_txn_rolls_back_all_modalities_on_failure() {
        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("rollback");
        let inner = Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let backend: Arc<dyn PersistenceBackend> = Arc::new(FailingBackend {
            inner: inner.clone(),
        });
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id = stage_crossmodal(&state, "media", "m1", "sha256:def").await;

        // COMMIT must FAIL (the durable barrier errored) → Response is an error.
        let resp = txn_handle(
            &state,
            5,
            None,
            Method::Commit {
                txn_id,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
        assert!(resp.error.is_some(), "commit surfaces the durable failure");
        assert!(resp.result.is_none(), "no Bool ack on a failed commit");

        // NO PARTIAL COMMIT: NONE of the modalities applied in-memory.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "node rolled back");
            assert_eq!(core.semantic_store.read().len(), 0, "vector rolled back");
        }

        // And NONE durable in redb either (the WriteTransaction never committed).
        inner.shutdown();
        drop(inner);
        drop(backend);
        // `state` holds a clone of the wrapper backend (which owns `inner`); drop it so
        // the last handle is gone and the redb file lock releases before the reopen.
        drop(state);
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let durable_node = s
                .registry
                .get("media")
                .map(|e| e.core.has_node("m1"))
                .unwrap_or(false);
            assert!(!durable_node, "node never landed durably");
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Extended cross-modal ACID: 5 modalities in ONE wtx (CONCEPT:EG-KG.backend.cross-modal-atomic-commit/361/362) ──

    /// A data-independent CONSTRUCT that yields exactly one triple
    /// `<urn:a> <urn:p> <urn:b>` via an inline VALUES row (needs no committed data), so
    /// its staged lowering is a deterministic node+node+edge write.
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    const CONSTRUCT_Q: &str =
        "CONSTRUCT { ?x <urn:p> ?y } WHERE { VALUES (?x ?y) { (<urn:a> <urn:b>) } }";

    /// Encode a `Vec<(i64, Vec<f64>)>` measurement batch to the wire blob.
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    fn meas_points(pts: &[(i64, Vec<f64>)]) -> Vec<u8> {
        rmp_serde::to_vec(&pts.to_vec()).unwrap()
    }

    /// Stage all FIVE modalities into a fresh txn: graph node + embedding + blob-ref +
    /// time-series measurement + SPARQL CONSTRUCT triple. Returns the txn id.
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    async fn stage_five_modalities(
        state: &Arc<RwLock<ServerState>>,
        graph: &str,
        node: &str,
        digest: &str,
        series: &str,
        points: &[(i64, Vec<f64>)],
    ) -> String {
        let begin = txn_handle(
            state,
            1,
            None,
            Method::BeginTxn {
                graph: Some(graph.to_string()),
                isolation: None,
            },
        )
        .await
        .unwrap();
        let txn_id = match begin.result {
            Some(ResultPayload::String(id)) => id,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        let ok = |r: Response| assert_eq!(as_bool(r), Some(true));
        ok(txn_handle(
            state,
            2,
            None,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: node.to_string(),
                properties_msgpack: props(serde_json::json!({"type": "Media"})),
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            3,
            None,
            Method::TxnAddEmbedding {
                txn_id: txn_id.clone(),
                node_id: node.to_string(),
                embedding: vec![0.1, 0.2, 0.3],
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            4,
            None,
            Method::TxnBlobRef {
                txn_id: txn_id.clone(),
                node_id: node.to_string(),
                digest: digest.to_string(),
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            5,
            None,
            Method::TxnAddMeasurement {
                txn_id: txn_id.clone(),
                series: series.to_string(),
                points: meas_points(points),
                graph: None,
            },
        )
        .await
        .unwrap());
        ok(txn_handle(
            state,
            6,
            None,
            Method::TxnConstruct {
                txn_id: txn_id.clone(),
                sparql: CONSTRUCT_Q.to_string(),
                graph: None,
            },
        )
        .await
        .unwrap());
        txn_id
    }

    /// CAPSTONE: one txn stages node + embedding + blob-ref + measurement + CONSTRUCT
    /// triple; `Commit` lands ALL FIVE atomically in ONE redb `WriteTransaction`; every
    /// modality is durably present after a full backend reload (CONCEPT:EG-KG.backend.cross-modal-atomic-commit/361/362).
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn five_modality_atomic_commit() {
        use eg_tsdb::store::SeriesStore;

        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("five");
        let points = vec![
            (1_000_000_000i64, vec![10.0]),
            (2_000_000_000i64, vec![20.0]),
        ];
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        // The measurement modality of this cross-modal commit is written to the tsdb
        // SeriesStore, so the state MUST carry one (same setup the measurement +
        // reconciliation tests use) — without it the Commit fails writing the tsdb leg.
        let series_store = Arc::new(SeriesStore::open_in_dir(std::path::Path::new(&dir)).unwrap());
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
            s.tsdb_store = Some(series_store.clone());
        }

        let txn_id =
            stage_five_modalities(&state, "media", "m1", "sha256:abc", "sensor", &points).await;
        // Nothing applied before commit.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "no apply before commit");
            assert!(!core.has_node("<urn:a>"), "no CONSTRUCT node before commit");
            assert_eq!(
                core.semantic_store.read().len(),
                0,
                "no vector before commit"
            );
        }

        assert_eq!(
            as_bool(
                txn_handle(
                    &state,
                    7,
                    None,
                    Method::Commit {
                        txn_id,
                        idempotency_key: None
                    }
                )
                .await
                .unwrap()
            ),
            Some(true),
            "five-modality commit"
        );

        // In-memory: graph modalities all present (node + vector + blob + CONSTRUCT edge).
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node landed");
            assert_eq!(core.semantic_store.read().len(), 1, "vector landed");
            assert!(
                core.has_node("<urn:a>") && core.has_node("<urn:b>"),
                "CONSTRUCT nodes landed"
            );
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc")
            );
        }
        backend.shutdown();
        drop(backend);
        // `state` holds a clone of the backend Arc; drop it so the last handle is gone
        // and the redb file lock releases before the same-dir reopens below.
        drop(state);

        // Measurements are durable in the authoritative shard's SERIES tables
        // (same wtx, not series.redb).
        {
            let series_db =
                SeriesStore::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path())
                    .unwrap();
            let key = direct_test_series_key("media", "sensor");
            let meta = series_db.meta(&key).unwrap().expect("series durable");
            assert_eq!(meta.count, 2, "both measurement points durable");
            let scanned = series_db.scan_all(&key).unwrap();
            assert_eq!(scanned.len(), 2, "measurement points readable post-reload");
            assert_eq!(scanned[0].values, vec![10.0]);
            assert_eq!(scanned[1].values, vec![20.0]);
        }

        // Reload the graph tier: node + vector + blob-ref + CONSTRUCT triple all durable.
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node durable");
            assert_eq!(core.semantic_store.read().len(), 1, "vector durable");
            assert!(
                core.has_node("<urn:a>") && core.has_node("<urn:b>"),
                "CONSTRUCT nodes durable"
            );
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc"),
                "blob durable"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// ATOMICITY: a five-modality txn whose durable commit FAILS mid-way (the always-fails
    /// backend, injected AFTER the measurement is staged) lands NONE of the five — no node,
    /// no vector, no CONSTRUCT triple in-memory, and NO series durable (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
    #[cfg(all(feature = "tsdb", feature = "sparql"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn five_modality_rolls_back_all_on_failure() {
        use eg_tsdb::store::SeriesStore;

        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("five-rollback");
        let points = vec![(1_000_000_000i64, vec![10.0])];
        let inner = Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let backend: Arc<dyn PersistenceBackend> = Arc::new(FailingBackend {
            inner: inner.clone(),
        });
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id =
            stage_five_modalities(&state, "media", "m1", "sha256:def", "sensor", &points).await;

        // COMMIT must FAIL (the durable barrier errored) → error Response, no ack.
        let resp = txn_handle(
            &state,
            7,
            None,
            Method::Commit {
                txn_id,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
        assert!(resp.error.is_some(), "commit surfaces the durable failure");
        assert!(resp.result.is_none(), "no Bool ack on a failed commit");

        // NO PARTIAL COMMIT: none of the graph modalities applied in-memory.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "node rolled back");
            assert!(!core.has_node("<urn:a>"), "CONSTRUCT triple rolled back");
            assert_eq!(core.semantic_store.read().len(), 0, "vector rolled back");
        }

        inner.shutdown();
        drop(inner);
        drop(backend);

        // And the measurement never landed durably either (the wtx never committed).
        {
            let series_db =
                SeriesStore::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path())
                    .unwrap();
            let key = direct_test_series_key("media", "sensor");
            assert!(
                series_db.meta(&key).unwrap().is_none(),
                "series never landed durably"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// EG-P0-4 (CONCEPT:EG-KG.backend.ts-served-materialize) — the canonical time-series
    /// read-path unification proof. A measurement committed through the cross-modal txn
    /// path (staged alongside a plain node write in ONE txn — a measurement alone already
    /// makes a txn cross-modal per `GraphTxnState::is_cross_modal`) is:
    ///  1. durable in the authoritative shard's SERIES tables (the atomic barrier,
    ///     unchanged — verified directly against that file, exactly like
    ///     `five_modality_atomic_commit`);
    ///  2. ALSO visible through the PUBLIC `Method::TsRange` read path immediately after
    ///     `Commit` acks — the actual gap this workstream closes (before, a cross-modal
    ///     measurement was durable yet permanently unreachable from `TsRange`/`TsScan`);
    ///  3. STILL visible via `TsRange` after a full process restart (drop + reopen BOTH
    ///     the redb backend AND the served time-series store from the same persist dir,
    ///     on a brand-new `ServerState`) — proving the served-store materialization is
    ///     itself a committed durable write, not an in-memory-only mirror that a restart
    ///     would lose.
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_measurement_visible_via_public_tsrange_post_commit_and_restart() {
        use crate::protocol::ResultPayload;
        use eg_tsdb::store::SeriesStore;

        const SECRET: &str = "ts-unify-secret";
        // Held for the whole test — this one literally reopens the backend
        // ("...post_commit_and_restart"), so cipher stability across BOTH opens is
        // exactly what this lock guarantees. See
        // `crossmodal_txn_commits_all_modalities_atomically` above.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("ts-unify");
        let points = vec![
            (1_000_000_000i64, vec![10.0]),
            (2_000_000_000i64, vec![20.0]),
            (3_000_000_000i64, vec![30.0]),
        ];

        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let series_store = Arc::new(SeriesStore::open_in_dir(std::path::Path::new(&dir)).unwrap());
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
            s.tsdb_store = Some(series_store.clone());
        }

        let req = |id: u64, method: Method| current_request(SECRET, id, "media", method);

        // Stage a node + a measurement in ONE cross-modal txn, then commit.
        let begin = dispatch_on_heap(
            &state,
            req(
                1,
                Method::BeginTxn {
                    graph: Some("media".to_string()),
                    isolation: None,
                },
            ),
        )
        .await;
        let txn_id = match begin.result {
            Some(ResultPayload::String(id)) => id,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        let r = dispatch_on_heap(
            &state,
            req(
                2,
                Method::TxnAddNode {
                    txn_id: txn_id.clone(),
                    node_id: "m1".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Sensor"})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "stage node: {:?}", r.error);
        let r = dispatch_on_heap(
            &state,
            req(
                3,
                Method::TxnAddMeasurement {
                    txn_id: txn_id.clone(),
                    series: "sensor.ts-unify".to_string(),
                    // Encoded inline (NOT the `sparql`-gated `meas_points` helper) so this
                    // test only needs `tsdb`, matching the `#[cfg]` above.
                    points: rmp_serde::to_vec(&points).unwrap(),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "stage measurement: {:?}", r.error);

        let commit = dispatch_on_heap(
            &state,
            req(
                4,
                Method::Commit {
                    txn_id,
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert_eq!(
            as_bool(commit),
            Some(true),
            "cross-modal commit must succeed"
        );

        // (1) POST-COMMIT visibility through the PUBLIC TsRange read path (the served
        // series.redb) — the actual gap this workstream closes. Checked FIRST, while
        // `backend`/`series_store` are both still live (they're two independent redb
        // files/handles, so no lock conflict).
        let ts_range = || {
            req(
                5,
                Method::TsRange {
                    series_id: "sensor.ts-unify".to_string(),
                    from: 0,
                    to: i64::MAX,
                },
            )
        };
        let decode_ts = |r: Response| -> Vec<(i64, Vec<f64>)> {
            assert!(r.error.is_none(), "TsRange error: {:?}", r.error);
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected Raw TsRange result, got {other:?}"),
            }
        };
        let got = decode_ts(dispatch_on_heap(&state, ts_range()).await);
        assert_eq!(
            got, points,
            "measurement committed via the cross-modal txn path must be visible through \
             the PUBLIC TsRange API immediately after Commit"
        );

        // (2) Durable in the authoritative shard too — the atomic barrier.
        // redb holds an EXCLUSIVE per-process file lock, so `backend` (which owns the
        // live shard handle) must release it first — exactly the ordering
        // `five_modality_atomic_commit` uses to open a second, direct handle on the
        // same file.
        backend.shutdown();
        drop(backend);
        {
            let series_db =
                SeriesStore::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path())
                    .unwrap();
            let key = envelope_test_series_key("media", "sensor.ts-unify");
            let meta = series_db
                .meta(&key)
                .unwrap()
                .expect("measurement durable in the authoritative shard");
            assert_eq!(meta.count, 3, "all 3 points durable in the shard");
        }

        // (3) RESTART: drop + reopen BOTH stores from the SAME persist dir on a FRESH
        // ServerState, then re-run the SAME public TsRange call.
        drop(series_store);
        drop(state);

        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        let series_store2 = Arc::new(SeriesStore::open_in_dir(std::path::Path::new(&dir)).unwrap());
        {
            let mut s = state2.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend2.clone());
            s.tsdb_store = Some(series_store2.clone());
        }

        let got2 = decode_ts(dispatch_on_heap(&state2, ts_range()).await);
        assert_eq!(
            got2, points,
            "measurement must STILL be visible through the PUBLIC TsRange API after a \
             full restart (served store reopened from disk, not rebuilt from RAM)"
        );

        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// L16 startup reconciliation (CONCEPT:EG-KG.backend.ts-startup-reconcile) — the proof that closes the
    /// EG-P0-4 residual. Simulates a crash STRICTLY BETWEEN the two commits by landing a
    /// measurement batch in the authoritative shard via the low-level `commit_crossmodal` (the atomic
    /// barrier alone) WITHOUT going through `handlers::txn::commit_cross_modal_txn` (which
    /// is what performs the served-store replay) — so the served `series.redb` never sees
    /// it, exactly the documented gap. Asserts:
    ///  1. Before reconciliation, the measurement is invisible via the PUBLIC `TsRange`.
    ///  2. `RedbBackend::reconcile_time_series` finds + replays it; `TsRange` now returns
    ///     it.
    ///  3. Running reconciliation a SECOND time is a true no-op (nothing reconciled) and
    ///     does NOT duplicate the points — proving idempotency.
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn startup_reconciliation_closes_the_crash_window_gap() {
        use crate::protocol::ResultPayload;
        use eg_tsdb::store::SeriesStore;

        const SECRET: &str = "ts-reconcile-secret";
        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("ts-reconcile");
        let points: Vec<(i64, Vec<f64>)> = vec![
            (1_000_000_000i64, vec![1.0]),
            (2_000_000_000i64, vec![2.0]),
            (3_000_000_000i64, vec![3.0]),
        ];
        const SERIES: &str = "sensor.ts-reconcile";
        const BUCKET_NS: u64 = 3_600_000_000_000; // 1h — matches DEFAULT_MEASUREMENT_BUCKET_NS.

        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), DurabilityPolicy::Each, 64).unwrap());
        let series_store = Arc::new(SeriesStore::open_in_dir(std::path::Path::new(&dir)).unwrap());
        let state = new_state(Some(dir.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
            s.tsdb_store = Some(series_store.clone());
        }

        // ── Simulate the crash window: land the measurement ONLY in the authoritative
        // shard, via the
        // atomic barrier alone, bypassing the txn-handler replay step entirely. ──
        let measurement: crate::MeasurementBatch = (
            envelope_test_series_key("media", SERIES),
            1,
            BUCKET_NS,
            vec!["value".to_string()],
            points.clone(),
        );
        backend
            .commit_crossmodal("media", &[], &[], &[], std::slice::from_ref(&measurement))
            .await
            .expect("authoritative-shard-only commit must succeed");

        let req = |id: u64, method: Method| current_request(SECRET, id, "media", method);
        let ts_range = |id: u64| {
            req(
                id,
                Method::TsRange {
                    series_id: SERIES.to_string(),
                    from: 0,
                    to: i64::MAX,
                },
            )
        };
        let decode_ts = |r: Response| -> Vec<(i64, Vec<f64>)> {
            assert!(r.error.is_none(), "TsRange error: {:?}", r.error);
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected Raw TsRange result, got {other:?}"),
            }
        };

        // (1) BEFORE reconciliation: invisible through the PUBLIC TsRange — the gap
        // `commit_crossmodal` alone (without the txn-handler replay step) leaves open.
        // (Durability in the authoritative shard itself is proven by reconciliation
        // finding + replaying exactly 3 points — a SECOND `Database` handle can't be
        // opened directly on the shard here to double-check, since `backend` still
        // holds redb's exclusive per-process file lock on it, same constraint the
        // EG-P0-4 test works around by dropping its backend first.)
        let before = decode_ts(dispatch_on_heap(&state, ts_range(1)).await);
        assert!(
            before.is_empty(),
            "before reconciliation, a measurement landed only via the authoritative shard \
             commit (the simulated crash) must NOT yet be visible through TsRange"
        );

        // (2) Reconcile: the redb-backed reader is reached the SAME way production code
        // does — downcast the trait object via `as_redb()`.
        let redb = backend.as_redb().expect("redb backend");
        let report = redb
            .reconcile_time_series(&series_store)
            .await
            .expect("reconciliation must succeed");
        assert_eq!(
            report.series_reconciled, 1,
            "exactly one series needed replay"
        );
        assert_eq!(report.points_replayed, 3, "all 3 points replayed");

        let after = decode_ts(dispatch_on_heap(&state, ts_range(2)).await);
        assert_eq!(
            after, points,
            "after reconciliation, the measurement must be visible through the PUBLIC \
             TsRange API — the crash-window gap is closed"
        );

        // (3) IDEMPOTENCY: reconciling again is a true no-op — nothing to replay, and
        // TsRange returns the SAME points (no duplicates).
        let report2 = redb
            .reconcile_time_series(&series_store)
            .await
            .expect("second reconciliation must succeed");
        assert_eq!(
            report2.series_reconciled, 0,
            "a converged series must not be re-reconciled"
        );
        assert_eq!(report2.points_replayed, 0);
        let after2 = decode_ts(dispatch_on_heap(&state, ts_range(3)).await);
        assert_eq!(
            after2, points,
            "running reconciliation twice must not duplicate any point"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// AuditVerify dispatch round-trip (CONCEPT:EG-KG.sharding.row-level-security): durable writes build a
    /// hash-chained audit log; `Method::AuditVerify` over the served dispatch returns
    /// `ok=true`; tampering an entry makes the served verify report the break.
    #[cfg(feature = "security")]
    #[tokio::test]
    async fn audit_verify_dispatch_detects_tamper() {
        use crate::protocol::{AuditReport, ResultPayload};

        const SECRET: &str = "audit-secret";
        let dir = std::env::temp_dir().join(format!("eg-audit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = Arc::new(
            RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
                .expect("open redb backend"),
        );
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // Two durable writes → two chained audit entries (commit-before-ack durable).
        for (rid, nid) in [(1u64, "n1"), (2, "n2")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"v": rid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "add failed: {:?}", r.error);
        }

        // Served AuditVerify ⇒ ok.
        let decode = |r: crate::protocol::Response| -> AuditReport {
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected raw AuditReport, got {other:?}"),
            }
        };
        let report = decode(dispatch_on_heap(&state, req(3, Method::AuditVerify)).await);
        assert!(report.ok, "clean chain should verify: {report:?}");
        assert_eq!(report.entries, 2);

        // Tamper the audit table directly under the writer-thread DB, then re-verify.
        backend
            .test_tamper_audit_entry(&crate::persist::sanitize("__commons__"), 0)
            .expect("tamper");
        let broken = decode(dispatch_on_heap(&state, req(4, Method::AuditVerify)).await);
        assert!(!broken.ok, "tamper should be detected");
        assert_eq!(broken.first_broken_seq, Some(0));

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GetLedger RPC-boundary regression (BUG A1, 2026-08-12): `GetLedger` used to
    /// read the mutation ledger off the RLS-projected `core` shadowed inside
    /// `handlers::graph_ops::try_handle` (`GraphReadAuthority::project_core`),
    /// whose detached copy is built via `add_node_no_ledger`/`add_edge_no_ledger`
    /// and therefore NEVER carries a ledger at all -- see `build_projection`'s own
    /// doc in `server::access`. Because `security` (hence
    /// `GraphReadAuthority::is_active()`) is compiled into the DEFAULT `full`
    /// build, `GetLedger` returned `[]` on EVERY served request, regardless of
    /// how many mutations had actually committed -- indistinguishable from
    /// "nothing to sync" at every real production caller
    /// (`agent_utilities.workflows.epistemic_sync.flush_ledger_to_backend`).
    ///
    /// Commit N real mutations over the SAME served dispatch path this bug
    /// lived on, call `GetLedger`, and assert exactly N entries come back
    /// `populated: true` -- proving the fix reads the REAL, authoritative
    /// ledger (`raw_core`), not the RLS projection's permanently-empty one.
    /// `watermark` must be `0` here: well under the ledger's 100k cap, this
    /// instance has never dropped anything (see
    /// `eg_core::graph::tests::ledger_cap_drop_advances_the_watermark` for
    /// the cap-drop case itself -- that mechanism is a SEPARATE, real gap
    /// this fix does not paper over: the ledger remains a purely in-memory,
    /// ephemeral buffer, not a durable change log).
    #[tokio::test]
    async fn get_ledger_dispatch_returns_real_committed_mutations() {
        use crate::protocol::{LedgerReadResult, ResultPayload};

        const SECRET: &str = "get-ledger-secret";
        let dir = std::env::temp_dir().join(format!("eg-get-ledger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(
            RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
                .expect("open redb backend"),
        );
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // Three real, committed mutations over the served dispatch path.
        for (rid, nid) in [(1u64, "gl1"), (2, "gl2"), (3, "gl3")] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"v": rid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "add failed: {:?}", r.error);
        }

        let decode = |r: crate::protocol::Response| -> LedgerReadResult {
            match r.result {
                Some(ResultPayload::Json(v)) => serde_json::from_value(v).unwrap(),
                other => panic!("expected a typed LedgerReadResult, got {other:?}"),
            }
        };
        let ledger = decode(dispatch_on_heap(&state, req(4, Method::GetLedger)).await);
        assert!(
            ledger.populated,
            "a real, committed ledger must be reported populated: {ledger:?}"
        );
        assert_eq!(
            ledger.entries.len(),
            3,
            "GetLedger must return exactly the 3 committed mutations, not the \
             RLS-projected permanently-empty ledger: {:?}",
            ledger.entries
        );
        assert_eq!(
            ledger.watermark, 0,
            "nothing has been dropped from this fresh, far-under-cap ledger"
        );
        for needle in ["gl1", "gl2", "gl3"] {
            assert!(
                ledger.entries.iter().any(|e| e.contains(needle)),
                "missing {needle} in ledger entries: {:?}",
                ledger.entries
            );
        }

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The genuinely-empty case is explicitly typed too (BUG A1, 2026-08-12): a
    /// freshly-created graph with ZERO committed mutations answers `populated:
    /// true, entries: []` -- a REAL empty ledger, which a bare `Vec<String>`
    /// could never distinguish from "the read failed" and a typed
    /// [`LedgerReadResult`] now can. No prior write, no persistence backend
    /// needed -- `GetLedger` is the very first request this graph ever sees.
    #[tokio::test]
    async fn get_ledger_dispatch_types_a_genuinely_empty_ledger_distinctly() {
        use crate::protocol::{LedgerReadResult, ResultPayload};

        const SECRET: &str = "get-ledger-empty-secret";
        let state = new_state(None);
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        let decode = |r: crate::protocol::Response| -> LedgerReadResult {
            match r.result {
                Some(ResultPayload::Json(v)) => serde_json::from_value(v).unwrap(),
                other => panic!("expected a typed LedgerReadResult, got {other:?}"),
            }
        };
        let ledger = decode(dispatch_on_heap(&state, req(1, Method::GetLedger)).await);
        assert!(
            ledger.populated,
            "a genuinely empty ledger is still POPULATED (the read succeeded, \
             there is simply nothing in it): {ledger:?}"
        );
        assert!(
            ledger.entries.is_empty(),
            "expected zero entries on a never-mutated graph: {:?}",
            ledger.entries
        );
        assert_eq!(
            ledger.watermark, 0,
            "a never-mutated graph has never dropped anything"
        );
    }

    /// Provenance-anchor inclusion proof (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring) —
    /// the tamper-detection acceptance test: a `:ToolCall` node's window is
    /// anchored, its inclusion proof verifies against that anchor's chain-protected
    /// root, and
    /// an overwrite of that SAME node's durable content AFTER anchoring — through
    /// the ORDINARY served write path, not a raw byte-flip — makes the SAME
    /// anchor's inclusion proof fail. A node that was never in the window
    /// reports `included=false` rather than a false pass, and the audit chain
    /// itself (both anchor entries) still verifies clean throughout.
    #[cfg(feature = "security")]
    #[tokio::test]
    async fn provenance_anchor_inclusion_proof_detects_tamper() {
        use crate::protocol::{AuditReport, MerkleInclusionReport, ResultPayload};
        use crate::server::persistence::provenance_anchor;

        const SECRET: &str = "provenance-secret";
        let dir = std::env::temp_dir().join(format!("eg-provenance-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = Arc::new(
            RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64)
                .expect("open redb backend"),
        );
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // Seed a ToolCall, a RunTrace, and an ordinary (non-provenance) node.
        for (rid, nid, node_type) in [
            (1u64, "tc-1", "ToolCall"),
            (2, "rt-1", "RunTrace"),
            (3, "widget-1", "Widget"),
        ] {
            let r = dispatch_on_heap(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(
                            serde_json::json!({"node_type": node_type, "v": 1}),
                        ),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "seed add failed: {:?}", r.error);
        }

        // Sweep: anchors the __commons__ ToolCall+RunTrace window (Widget is out
        // of scope, so it never affects the anchored root).
        let anchored = provenance_anchor::sweep(&state).await;
        assert_eq!(
            anchored, 1,
            "exactly one graph (__commons__) should be freshly anchored"
        );

        let decode_report = |r: crate::protocol::Response| -> MerkleInclusionReport {
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected raw MerkleInclusionReport, got {other:?}"),
            }
        };

        // A clean, freshly-anchored ToolCall node verifies.
        let clean = decode_report(
            dispatch_on_heap(
                &state,
                req(
                    4,
                    Method::AuditProveInclusion {
                        node_id: "tc-1".to_string(),
                        anchor_seq: None,
                    },
                ),
            )
            .await,
        );
        assert!(clean.included, "tc-1 must be part of the anchor's window");
        assert!(clean.verified, "clean node should verify: {clean:?}");
        assert_eq!(clean.window_size, 2, "only tc-1 + rt-1 are in scope");
        let anchor_seq = clean.anchor_seq;

        // Overwrite tc-1's durable content through the ORDINARY served write path
        // (not a raw byte-flip) -- the realistic tamper/insider-edit scenario.
        let r = dispatch_on_heap(
            &state,
            req(
                5,
                Method::AddNode {
                    node_id: "tc-1".to_string(),
                    properties_msgpack: props(
                        serde_json::json!({"node_type": "ToolCall", "v": "TAMPERED"}),
                    ),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "overwrite failed: {:?}", r.error);

        // Same anchor (explicit `anchor_seq`), same node id: now fails
        // verification -- the acceptance property.
        let tampered = decode_report(
            dispatch_on_heap(
                &state,
                req(
                    6,
                    Method::AuditProveInclusion {
                        node_id: "tc-1".to_string(),
                        anchor_seq: Some(anchor_seq),
                    },
                ),
            )
            .await,
        );
        assert!(
            tampered.included,
            "tc-1 is still part of that anchor's window"
        );
        assert!(
            !tampered.verified,
            "tampered node must fail inclusion verification: {tampered:?}"
        );
        assert_ne!(
            tampered.computed_root_sha256, tampered.anchored_root_sha256,
            "a tampered leaf must recompute a different root than the anchor"
        );
        assert_eq!(
            tampered.anchored_root_sha256, clean.anchored_root_sha256,
            "the ANCHORED root itself (chain-protected) must not change"
        );

        // A node that was never in the window reports included=false, never a
        // false "verified".
        let out_of_window = decode_report(
            dispatch_on_heap(
                &state,
                req(
                    7,
                    Method::AuditProveInclusion {
                        node_id: "widget-1".to_string(),
                        anchor_seq: Some(anchor_seq),
                    },
                ),
            )
            .await,
        );
        assert!(!out_of_window.included);
        assert!(!out_of_window.verified);

        // tc-1's overwrite changed the window's root, so a second sweep anchors
        // AGAIN (a second, distinct chain entry) -- and the audit chain itself
        // (both anchor entries, plus the ordinary mutation entries) still
        // verifies clean: provenance anchoring never breaks `AuditVerify`.
        let anchored_again = provenance_anchor::sweep(&state).await;
        assert_eq!(
            anchored_again, 1,
            "the tampered content changed the root, so it anchors again"
        );

        let audit_report: AuditReport = match dispatch_on_heap(&state, req(8, Method::AuditVerify))
            .await
            .result
        {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected raw AuditReport, got {other:?}"),
        };
        assert!(
            audit_report.ok,
            "the audit chain itself (incl. both anchor entries) must still verify: {audit_report:?}"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CONCEPT:EG-KG.backend.sharded-k-way-durable — sharded K-way durable writer ──────────────────────────

    /// Routing is a stable, deterministic FNV-1a — a graph maps to the SAME shard
    /// every process/restart (else its durable rows become unreachable), stays in
    /// `0..K`, and collapses to shard 0 under K<=1.
    #[test]
    fn shard_index_is_stable_and_bounded() {
        for k in [1usize, 2, 3, 4, 8] {
            for name in [
                "__commons__",
                "agent:planner",
                "g1",
                "enterprise-acme:billing",
            ] {
                let a = shard_index(name, k);
                let b = shard_index(name, k);
                assert_eq!(a, b, "routing must be deterministic for {name} (K={k})");
                assert!(a < k, "shard {a} out of range for K={k}");
            }
        }
        // K=1 always routes to the single shard 0.
        assert_eq!(shard_index("anything", 1), 0);
        assert_eq!(shard_index("anything", 0), 0);
        assert_eq!(shard_filename(0), "graph-0.redb");
        assert_eq!(shard_filename(2), "graph-2.redb");
    }

    /// K=1 uses the same indexed filename contract as every other shard count.
    #[tokio::test]
    async fn k1_uses_canonical_indexed_layout() {
        // Defensive: this test opens a backend twice (K=1 shard layout, then a plain
        // `open()` over the same dir after removal+recreation). Neither open ever
        // writes/reads an encrypted value, so an ambient key toggle between the two
        // opens is very unlikely to be observable here -- but hold the lock anyway so
        // this is never the crate's third instance of the "two opens, ambient key
        // mid-flight" class. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-shard-k1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 64, 1)
            .expect("open K=1");
        assert_eq!(backend.shard_count(), 1);
        assert!(
            dir.join("graph-0.redb").exists(),
            "K=1 must use canonical graph-0.redb"
        );
        assert!(
            !dir.join("graph.redb").exists(),
            "normal startup must not create the retired graph.redb layout"
        );
        // cfg(test) auto-resolves to K=1, so a plain open() is the single-file path
        // too. Serialize vs the SHARDS-env-override test (and defensively clear the
        // var) so a concurrent test can't leak K into this layout assertion.
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
        let auto = {
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_SHARDS");
            RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64).expect("open auto")
        };
        assert_eq!(auto.shard_count(), 1, "cfg(test) default K=1");
        assert!(dir.join("graph-0.redb").exists());
        auto.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn normal_startup_rejects_retired_single_file_layout() {
        let dir = std::env::temp_dir().join(format!(
            "eg-shard-retired-layout-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let retired = Database::create(dir.join("graph.redb")).unwrap();
        drop(retired);

        let err = match RedbBackend::open_with_shards(
            dir.to_string_lossy().to_string(),
            DurabilityPolicy::Each,
            64,
            1,
        ) {
            Ok(backend) => {
                backend.shutdown();
                panic!("retired layout must require an offline migration");
            }
            Err(err) => err,
        };
        assert!(err.contains("retired redb layout"), "{err}");
        assert!(!dir.join("graph-0.redb").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// K>1 routes a graph's writes to a DETERMINISTIC shard (proven by per-shard
    /// commit stats: only the owning shard commits) and the write round-trips durably
    /// ACROSS A RESTART (reopen + read the node back from disk).
    #[tokio::test(flavor = "multi_thread")]
    async fn k_gt_1_routes_to_deterministic_shard_and_survives_restart() {
        // Held for the whole test: this test opens the backend TWICE (initial write,
        // then a restart reopen) and both `RedbBackend::open_with_shards` calls must
        // resolve the SAME encryption-at-rest cipher (`ValueCipher::from_env`,
        // resolved fresh at each `open`) or the restart reopen's read fails with
        // "encrypted durable value is missing sealed framing" -- the exact
        // destructive-read mismatch documented on `RedbBackend::transaction_recovery_cipher`.
        // This test never sets `EPISTEMIC_GRAPH_ENCRYPTION_KEY` itself; it only needs
        // the AMBIENT value (set or unset) to stay constant across both opens, which
        // requires excluding every other test that mutates that process-global for its
        // duration. See `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-shard-route-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        const K: usize = 4;
        let graph = "agent:router-test";
        let owner = shard_index(graph, K);

        {
            let backend =
                RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 256, K)
                    .expect("open K=4");
            assert_eq!(backend.shard_count(), K);
            // All K shard files exist (each acquired its exclusive lock at open).
            for i in 0..K {
                assert!(
                    dir.join(format!("graph-{i}.redb")).exists(),
                    "shard {i} file"
                );
            }
            backend
                .record_durable(
                    graph,
                    &Method::AddNode {
                        node_id: "n1".to_string(),
                        properties_msgpack: props(serde_json::json!({"v": 1})),
                    },
                )
                .await
                .expect("durable commit");
            // Determinism proof: ONLY the routed shard committed.
            let stats = backend.commit_stats_all();
            assert!(stats[owner].commits() > 0, "owning shard {owner} committed");
            for (i, st) in stats.iter().enumerate() {
                if i != owner {
                    assert_eq!(st.commits(), 0, "non-owning shard {i} must not commit");
                }
            }
            assert!(
                backend.read_node(graph, "n1").await.unwrap().is_some(),
                "node readable pre-restart"
            );
            backend.shutdown();
        }

        // RESTART: reopen the SAME dir (K reconciled from on-disk layout) and read the
        // node straight back from disk — durability across a process restart.
        {
            let backend =
                RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 256, K)
                    .expect("reopen K=4");
            assert_eq!(backend.shard_count(), K, "K reconciled from disk");
            assert!(
                backend.read_node(graph, "n1").await.unwrap().is_some(),
                "node survived restart, served from the same shard"
            );
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two graphs on DIFFERENT shards commit CONCURRENTLY — the multicore win: K
    /// independent single-writer files commit in parallel. Proven by both writes
    /// succeeding and per-shard stats showing two distinct shards each committed.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_graphs_on_different_shards_commit_concurrently() {
        let dir = std::env::temp_dir().join(format!("eg-shard-par-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        const K: usize = 4;
        // Find two graph names that route to distinct shards.
        let mut a = String::new();
        let mut b = String::new();
        for i in 0..1000 {
            let name = format!("g{i}");
            let s = shard_index(&name, K);
            if a.is_empty() {
                a = name;
            } else if shard_index(&a, K) != s {
                b = name;
                break;
            }
        }
        assert!(!b.is_empty(), "found two graphs on different shards");
        let (sa, sb) = (shard_index(&a, K), shard_index(&b, K));
        assert_ne!(sa, sb);

        let backend = Arc::new(
            RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 256, K)
                .expect("open"),
        );
        // Fire both concurrently — they target different writer threads / files.
        let ba = backend.clone();
        let bb = backend.clone();
        let ga = a.clone();
        let gb = b.clone();
        let ha = tokio::spawn(async move {
            for i in 0..50 {
                ba.record_durable(
                    &ga,
                    &Method::AddNode {
                        node_id: format!("a{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
                .expect("a durable");
            }
        });
        let hb = tokio::spawn(async move {
            for i in 0..50 {
                bb.record_durable(
                    &gb,
                    &Method::AddNode {
                        node_id: format!("b{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
                .expect("b durable");
            }
        });
        ha.await.unwrap();
        hb.await.unwrap();

        let stats = backend.commit_stats_all();
        assert!(stats[sa].commits() > 0, "shard {sa} committed graph A");
        assert!(stats[sb].commits() > 0, "shard {sb} committed graph B");
        assert!(stats[sa].ops() >= 50 && stats[sb].ops() >= 50);
        // Both graphs fully durable.
        assert!(backend.read_node(&a, "a49").await.unwrap().is_some());
        assert!(backend.read_node(&b, "b49").await.unwrap().is_some());
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The `EPISTEMIC_GRAPH_REDB_SHARDS` env override is honored by `open()` even in
    /// cfg(test) (the env check precedes the test default). Serialized vs the other
    /// env-mutating tests via the shared lock.
    #[tokio::test]
    async fn shards_env_override_is_honored() {
        let dir = std::env::temp_dir().join(format!("eg-shard-env-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_SHARDS", "3");
            let b = RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64).expect("open");
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_SHARDS");
            b
        };
        assert_eq!(backend.shard_count(), 3, "env override sets K=3");
        for i in 0..3 {
            assert!(dir.join(format!("graph-{i}.redb")).exists());
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.sharding.r5-feature (R5) — the catalog auto-attach gate + the empty-catalog routing
    /// identity. A durable `catalog.redb` ⇒ `open()` attaches it; absent (and no flag) ⇒
    /// no catalog (pure EG-026). An EMPTY catalog resolves every graph to the exact EG-026
    /// `shard_index` (byte-for-byte), and an explicit assignment overrides the hash.
    #[tokio::test]
    async fn catalog_auto_attach_gate_and_empty_is_fnv1a() {
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        let root = std::env::temp_dir().join(format!("eg-r5-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);

        // (1) No catalog.redb, no env ⇒ open() attaches NOTHING (default EG-026 routing).
        let plain_dir = root.join("plain");
        std::fs::create_dir_all(&plain_dir).unwrap();
        let plain = RedbBackend::open(
            plain_dir.to_string_lossy().to_string(),
            DurabilityPolicy::Each,
            64,
        )
        .unwrap();
        assert!(plain.catalog().is_none(), "default open() has no catalog");
        plain.shutdown();

        // (2) A durable catalog.redb present ⇒ open() auto-attaches it, loading its
        // placements. This is the "attach if a durable catalog exists" gate.
        let cat_dir = root.join("cat");
        let cat_dir_s = cat_dir.to_string_lossy().to_string();
        {
            let cat = TenantCatalog::open(&cat_dir_s).expect("seed catalog");
            cat.assign("pinned", 0, None).unwrap();
        }
        let attached = RedbBackend::open(cat_dir_s.clone(), DurabilityPolicy::Each, 64).unwrap();
        let cat = attached
            .catalog()
            .expect("durable catalog auto-attached at open");
        assert_eq!(cat.len(), 1, "the prior placement survived + reloaded");
        attached.shutdown();

        // (3) Empty catalog routes IDENTICALLY to EG-026 for every graph/K (no regression).
        let empty = TenantCatalog::in_memory();
        for k in [1usize, 2, 4, 8, 16] {
            for g in ["__commons__", "agent:7", "g-move", "tenant_x", "ZZZ"] {
                assert_eq!(
                    empty.resolve_shard(g, k),
                    shard_index(g, k),
                    "empty catalog == FNV-1a for g={g} K={k}"
                );
            }
        }
        // An explicit override wins (the routing flip an online reshard performs).
        empty.assign("g-move", 0, None).unwrap();
        assert_eq!(empty.resolve_shard("g-move", 4), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CONCEPT:EG-KG.backend.catalog-shard-resolve (R1) — move ONE graph between shards while the engine RUNS, with
    /// concurrent writes to that graph ACROSS the flip. Every node/edge survives, the
    /// audit chain stays valid, reads/writes follow the graph to its new shard, the source
    /// rows are GC'd, and an unrelated graph is untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn online_reshard_moves_graph_live_no_loss() {
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();

        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = Arc::new(
            RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 256, K)
                .expect("open K=4")
                .with_catalog(catalog.clone()),
        );
        assert_eq!(backend.shard_count(), K);

        let mover = "g-move";
        let stay = "g-stay";
        let src = shard_index(mover, K);
        let dst = (src + 1) % K;

        for g in [mover, stay] {
            backend
                .register_graph(g, g, GraphType::Global)
                .await
                .unwrap();
        }
        for i in 0..10u32 {
            backend
                .record_durable(
                    mover,
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"g": mover, "i": i})),
                    },
                )
                .await
                .unwrap();
        }
        backend
            .record_durable(
                mover,
                &Method::AddEdge {
                    source_id: "n0".into(),
                    target_id: "n1".into(),
                    properties_msgpack: props(serde_json::json!({"w": 1})),
                },
            )
            .await
            .unwrap();
        for i in 0..5u32 {
            backend
                .record_durable(
                    stay,
                    &Method::AddNode {
                        node_id: format!("s{i}"),
                        properties_msgpack: props(serde_json::json!({"g": stay, "i": i})),
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(
            catalog.resolve_shard(mover, K),
            src,
            "FNV route before any move"
        );

        // Concurrent writes to `mover` fired together with the move — they straddle the
        // route flip and must ALL survive on the destination shard (zero lost/misrouted).
        let writer_backend = backend.clone();
        let writer = tokio::spawn(async move {
            for i in 10..20u32 {
                writer_backend
                    .record_durable(
                        "g-move",
                        &Method::AddNode {
                            node_id: format!("n{i}"),
                            properties_msgpack: props(serde_json::json!({"g": "g-move", "i": i})),
                        },
                    )
                    .await
                    .expect("concurrent durable");
            }
        });
        let report = backend
            .reshard_graph(mover, dst as u32)
            .await
            .expect("reshard");
        writer.await.unwrap();

        assert!(!report.no_op);
        assert_eq!((report.from_shard, report.to_shard), (src, dst));
        assert_eq!(
            catalog.resolve_shard(mover, K),
            dst,
            "route now follows to dst"
        );

        // All 20 nodes + the edge survive and read back from the NEW shard.
        let dump = backend
            .read_graph_dump_blocking(mover)
            .unwrap()
            .expect("mover present after move");
        assert_eq!(dump.nodes.len(), 20, "pre + concurrent nodes all survived");
        assert_eq!(dump.edges.len(), 1, "edge survived");
        for i in 0..20u32 {
            assert!(
                dump.nodes.iter().any(|(id, _)| id == &format!("n{i}")),
                "node n{i} present after move"
            );
        }

        // A post-move write lands on the NEW shard (route followed the graph).
        backend
            .record_durable(
                mover,
                &Method::AddNode {
                    node_id: "post".into(),
                    properties_msgpack: props(serde_json::json!({"g": "g-move"})),
                },
            )
            .await
            .unwrap();
        let dump2 = backend.read_graph_dump_blocking(mover).unwrap().unwrap();
        assert_eq!(dump2.nodes.len(), 21, "post-move write on the new shard");

        // The unrelated graph is completely unaffected.
        let sdump = backend
            .read_graph_dump_blocking(stay)
            .unwrap()
            .expect("stay present");
        assert_eq!(sdump.nodes.len(), 5);

        // The tamper-evident audit chain still verifies on the new shard.
        #[cfg(feature = "security")]
        {
            let audit = backend.audit_verify_blocking(mover).expect("audit verify");
            assert!(
                audit.ok,
                "audit chain valid after the online move: {}",
                audit.detail
            );
        }

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.sharding.eg-r6 (R6) — a cold (idle) graph is offloaded: its whole in-RAM state is
    /// dropped to bound RAM, yet every node still SERVES on access via the KG-2.191
    /// read-through from redb. The shared `__commons__` is never offloaded.
    #[tokio::test(flavor = "multi_thread")]
    async fn cold_offload_evicts_then_serves_on_access() {
        use crate::server::persistence::cold_offload::{offload_cold_tenants, ColdTenantTracker};
        let dir = std::env::temp_dir().join(format!("eg-r6-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 256).expect("open"));
        let state = new_state(Some(dir_s.clone()));

        let core = seed_authoritative(&backend, &state, 12).await;
        assert_eq!(core.node_count(), 12, "all nodes resident before offload");

        let tracker = ColdTenantTracker::new();
        tracker.touch("g1");
        // Window 0 ⇒ g1 is cold ⇒ offloaded; __commons__ is skipped.
        let offloaded = offload_cold_tenants(&state, &tracker, std::time::Duration::ZERO).await;
        assert_eq!(offloaded, 1, "exactly g1 offloaded");
        assert!(tracker.is_offloaded("g1"));
        assert_eq!(tracker.offloaded_total(), 1);
        assert_eq!(core.node_count(), 0, "in-RAM state evicted by offload");

        // Served on access: every node reads back from redb via the read-through seam.
        for i in 0..12usize {
            assert_eq!(
                core.get_node_properties(&format!("n{i}")),
                Some(props(serde_json::json!({"type": "Task", "i": i}))),
                "offloaded node n{i} serves from redb on access"
            );
        }

        // Re-touch clears the offload mark + resets the idle clock (windowing).
        tracker.touch("g1");
        assert!(!tracker.is_offloaded("g1"));
        assert!(
            tracker
                .cold_graphs(std::time::Duration::from_secs(3600))
                .is_empty(),
            "a just-touched graph is not cold under a long window"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:AU-KG.backend.roadmap-f-parallel-cross (roadmap F) — the per-shard read fan-out runs CONCURRENTLY, not
    /// serially. A `Barrier(K)` only releases once all K closures are running at the SAME
    /// time; a serial spawn-then-await-each impl would block forever, which the timeout
    /// converts into a test failure. Results come back in shard order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fan_out_shard_reads_runs_concurrently() {
        use std::sync::Barrier;
        const K: usize = 4;
        let barrier = Arc::new(Barrier::new(K));
        let tasks: Vec<_> = (0..K)
            .map(|i| {
                let b = barrier.clone();
                move || -> Result<usize, String> {
                    b.wait();
                    Ok(i)
                }
            })
            .collect();
        let out = tokio::time::timeout(Duration::from_secs(10), join_blocking_in_order(tasks))
            .await
            .expect("fan-out ran concurrently (a serial impl would deadlock on the barrier)")
            .expect("all shard reads ok");
        assert_eq!(out, vec![0, 1, 2, 3], "results returned in shard order");
    }

    /// CONCEPT:AU-KG.backend.roadmap-f-parallel-cross (roadmap F) — `load_all` fans each shard's dump CONCURRENTLY off a
    /// `begin_read()` snapshot (off the writer) and unions them. Seed graphs spread across
    /// K=4 shards, commit, drop, reopen, load → every graph is recovered from its shard.
    #[tokio::test(flavor = "multi_thread")]
    async fn parallel_load_recovers_all_shards_off_the_writer() {
        // Held for the whole test: same requirement as
        // `k_gt_1_routes_to_deterministic_shard_and_survives_restart` above -- this
        // test opens the backend TWICE (initial write, then a restart reopen after
        // `shutdown()`/drop) and both opens must resolve the same encryption-at-rest
        // cipher or the reload's read fails with "encrypted durable value is missing
        // sealed framing". Never sets the key itself; only needs the ambient value to
        // stay constant across both opens. See `crate::crypto::acquire_test_env_lock`'s
        // doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-f-load-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let names = [
            "alpha", "beta", "gamma", "delta", "eps", "zeta", "eta", "theta",
        ];

        let backend = RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 64, K)
            .expect("open K=4");
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            for n in names {
                let _ = s.registry.create_graph(n, GraphType::Global, None);
            }
        }
        for n in names {
            backend
                .register_graph(n, n, GraphType::Global)
                .await
                .unwrap();
            let core = {
                let s = state.read().await;
                s.registry.get(n).map(|e| e.core.clone()).unwrap()
            };
            core.add_node("x".into(), props(serde_json::json!({"g": n})));
            backend
                .record_durable(
                    n,
                    &Method::AddNode {
                        node_id: "x".into(),
                        properties_msgpack: props(serde_json::json!({"g": n})),
                    },
                )
                .await
                .unwrap();
        }
        // The seed graphs must span >1 shard, else the parallel union proves nothing.
        let used: std::collections::HashSet<usize> = names
            .iter()
            .map(|n| shard_index(&crate::persist::sanitize(n), K))
            .collect();
        assert!(used.len() >= 2, "seed graphs span multiple shards");
        backend.shutdown();
        // `shutdown()` stops the writer threads but does NOT close each shard's
        // `Database`; redb holds its advisory file lock for as long as any
        // `Arc<RedbBackend>` lives, so an in-process reopen of the same directory
        // fails with "Database already open. Cannot acquire lock." unless every
        // reference is actually released first.
        {
            let mut s = state.write().await;
            s.persistence = None;
        }
        drop(backend);

        let backend2 = RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 64, K)
            .expect("reopen K=4");
        let state2 = new_state(Some(dir_s.clone()));
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert!(loaded >= names.len(), "all seeded graphs recovered");
        for n in names {
            let core = {
                let s = state2.read().await;
                s.registry
                    .get(n)
                    .map(|e| e.core.clone())
                    .expect("graph recovered")
            };
            assert_eq!(
                core.get_node_properties("x"),
                Some(props(serde_json::json!({"g": n}))),
                "graph {n} recovered from its shard via the parallel fan-out"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.r3-plan-execution (R3 plan execution) — a fully-skewed placement (every graph pinned to
    /// shard 0) → `plan_rebalance` → `rebalance_execute` applies each move via online
    /// resharding → graphs spread across shards and every node survives (no loss).
    #[tokio::test(flavor = "multi_thread")]
    async fn rebalance_execute_balances_and_preserves_data() {
        use crate::server::persistence::rebalance::{
            plan_rebalance, shard_loads_from_catalog, RebalanceOptions,
        };
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 256, K)
            .expect("open K=4")
            .with_catalog(catalog.clone());

        let names = ["g0", "g1", "g2", "g3", "g4", "g5"];
        for n in names {
            catalog.assign(n, 0, None).unwrap(); // pin ALL to shard 0 (skew)
            backend
                .register_graph(n, n, GraphType::Global)
                .await
                .unwrap();
            backend
                .record_durable(
                    n,
                    &Method::AddNode {
                        node_id: "a".into(),
                        properties_msgpack: props(serde_json::json!({"g": n})),
                    },
                )
                .await
                .unwrap();
        }
        assert!(
            names.iter().all(|n| catalog.resolve_shard(n, K) == 0),
            "fully skewed onto shard 0 before rebalance"
        );

        let loads: Vec<(String, u64)> = names.iter().map(|n| (n.to_string(), 1u64)).collect();
        let shards = shard_loads_from_catalog(&catalog, &loads, K);
        let plan = plan_rebalance(&shards, RebalanceOptions::default());
        assert!(!plan.is_empty(), "a fully-skewed set yields moves");
        let reports = backend.rebalance_execute(&plan).await.expect("execute");
        assert_eq!(reports.len(), plan.moves.len(), "one report per move");

        let distinct: std::collections::HashSet<usize> =
            names.iter().map(|n| catalog.resolve_shard(n, K)).collect();
        assert!(
            distinct.len() >= 2,
            "graphs spread across >1 shard after rebalance, got {distinct:?}"
        );
        for n in names {
            assert!(
                backend.read_node_blocking(n, "a").unwrap().is_some(),
                "graph {n} node survives the rebalance move"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.flush-pending-first (R1 delta-copy) — moving an IDLE graph copies the whole graph in the
    /// UNQUIESCED bulk pass, so the under-quiesce DELTA (the work that actually pauses the
    /// moved graph's writes) is 0. Proves the snapshot+delta path shrank the pause to ~0
    /// for the common idle case, with no data loss.
    #[tokio::test(flavor = "multi_thread")]
    async fn online_reshard_delta_is_small_for_idle_graph() {
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r1-delta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 256, K)
            .expect("open")
            .with_catalog(catalog.clone());

        let g = "idle-graph";
        backend
            .register_graph(g, g, GraphType::Global)
            .await
            .unwrap();
        for i in 0..20u32 {
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
                .unwrap();
        }
        let src = catalog.resolve_shard(g, K);
        let dst = (src + 1) % K;
        let report = backend.reshard_graph(g, dst as u32).await.expect("reshard");
        assert!(!report.no_op);
        assert!(
            report.nodes >= 20,
            "bulk pass copied the whole graph ({})",
            report.nodes
        );
        assert_eq!(
            report.delta_nodes, 0,
            "idle graph ⇒ zero under-quiesce node copy"
        );
        assert_eq!(
            report.delta_edges, 0,
            "idle graph ⇒ zero under-quiesce edge copy"
        );
        for i in 0..20u32 {
            assert!(
                backend
                    .read_node_blocking(g, &format!("n{i}"))
                    .unwrap()
                    .is_some(),
                "node n{i} survives the move"
            );
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.m3-admin-dispatch — drive the M3 admin ops over the FULL dispatch path (protocol →
    /// `handlers::admin` → the persistence APIs): assign a catalog placement, list it back,
    /// and get a rebalance plan. Proves the WIRE surface, not just the backend methods.
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_rpc_dispatch_roundtrip() {
        use crate::protocol::ResultPayload;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const SECRET: &str = "admin-rpc";
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-admin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open_with_shards(dir_s.clone(), DurabilityPolicy::Each, 256, K)
                .expect("open")
                .with_catalog(catalog.clone()),
        );
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| current_request(SECRET, id, "__commons__", method);

        // CatalogAssign → Bool(true).
        let r = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CatalogAssign {
                    graph: "g".into(),
                    shard: 2,
                    node: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "assign ok: {:?}",
            r.error
        );

        // CatalogList → JSON containing the placement we just wrote.
        let r = dispatch_on_heap(&state, req(2, Method::CatalogList)).await;
        match r.result {
            Some(ResultPayload::Json(v)) => {
                let placements = v
                    .get("placements")
                    .and_then(|p| p.as_array())
                    .cloned()
                    .unwrap_or_default();
                assert!(
                    placements.iter().any(|p| {
                        p.get("graph").and_then(|g| g.as_str()) == Some("g")
                            && p.get("shard").and_then(|s| s.as_u64()) == Some(2)
                    }),
                    "placement present in list: {placements:?}"
                );
            }
            other => panic!("CatalogList json, got {other:?}"),
        }

        // RebalancePlan → JSON with `moves` + `shards` arrays (read-only).
        let r = dispatch_on_heap(
            &state,
            req(
                3,
                Method::RebalancePlan {
                    tolerance: None,
                    max_moves: None,
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::Json(v)) => {
                assert!(v.get("moves").map(|m| m.is_array()).unwrap_or(false));
                assert!(v.get("shards").map(|m| m.is_array()).unwrap_or(false));
            }
            other => panic!("RebalancePlan json, got {other:?}"),
        }

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.r6-feature (R6 touch wiring) — the cold sweep selects a graph IDLE past the
    /// window but NEVER a recently-touched one. Proves the touch-driven selection semantics
    /// the dispatch read/write path relies on.
    #[test]
    fn touch_keeps_accessed_graph_resident_idle_is_cold() {
        use crate::server::persistence::cold_offload::ColdTenantTracker;
        let tracker = ColdTenantTracker::new();
        let window = Duration::from_millis(40);
        tracker.touch("hot");
        tracker.touch("cold");
        std::thread::sleep(Duration::from_millis(70)); // both idle past the window
        tracker.touch("hot"); // re-access "hot"
        let cold = tracker.cold_graphs(window);
        assert!(
            cold.contains(&"cold".to_string()),
            "an idle graph is a cold candidate"
        );
        assert!(
            !cold.contains(&"hot".to_string()),
            "a recently-touched graph stays resident"
        );
    }
}
