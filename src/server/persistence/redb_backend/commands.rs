use super::*;
use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome};
use tokio::sync::oneshot;

use crate::change_envelope::{ChangeEnvelope, ChangeEnvelopeCommit};
use crate::mutation_batch::{
    MutationBatch, MutationBatchCommit, MutationOutboxLease, MutationProjectionCursor,
};
use crate::protocol::{GraphType, Method};
#[cfg(any(feature = "compute-dist", feature = "matview"))]
use crate::redb_store::MatViewScanResult;
use crate::redb_store::{XshardDecisionScan, XshardPrepareScan};

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

/// How long a shard writer may take to tear down after acknowledging
/// `Cmd::Shutdown` before shutdown stops waiting on it and says so. Teardown
/// after the ack is a redb close, not data work, so this is short.
pub(super) const SHARD_WRITER_JOIN_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(60);

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
    /// Read ONE graph's durable rows back as an owned dump (CONCEPT:EG-KG.storage.100m-tenant — tenant
    /// rehydration). Goes through the owner thread (exclusive file lock) and flushes
    /// pending writes first so the rehydrated dump reflects the latest durable state.
    ReadGraphDump {
        graph: String,
        reply: std::sync::mpsc::SyncSender<Result<Option<GraphDump>, String>>,
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
        reply:
            std::sync::mpsc::SyncSender<Result<Option<crate::redb_store::GraphDumpPage>, String>>,
    },
    /// Export ONE graph's rows VERBATIM for an online shard move (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs
    /// on the SOURCE shard's writer: flush pending first (so the snapshot is complete),
    /// then scan the raw value blobs (encryption + audit chain untouched).
    ExportGraphRaw {
        graph: String,
        reply:
            std::sync::mpsc::SyncSender<Result<super::super::online_reshard::RawGraphRows, String>>,
    },
    /// Import ONE graph's verbatim rows on an online shard move (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on
    /// the DESTINATION shard's writer and lands them in ONE commit — the
    /// commit-before-ack point of the move.
    ImportGraphRaw {
        graph: String,
        rows: Box<super::super::online_reshard::RawGraphRows>,
        reply: std::sync::mpsc::SyncSender<Result<(), String>>,
    },
    /// Import ONLY the DELTA of an online shard move (CONCEPT:EG-KG.backend.flush-pending-first, R1 delta-copy). Runs
    /// on the DESTINATION shard's writer under the exclusive routing quiesce; lands the
    /// small set of rows that changed since the bulk pass (upserts + removals) in ONE
    /// commit — the short under-quiesce write that shrinks the pause.
    ImportGraphDelta {
        graph: String,
        delta: Box<super::super::online_reshard::RawGraphDelta>,
        reply: std::sync::mpsc::SyncSender<Result<(), String>>,
    },
    /// Verify ONE graph's tamper-evident hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security).
    /// Flushes pending first so the walk reflects the latest durable entries, then
    /// scans `(graph, 0..)` and reports OK or the first break.
    #[cfg(feature = "security")]
    AuditVerify {
        graph: String,
        reply: std::sync::mpsc::SyncSender<Result<crate::protocol::AuditReport, String>>,
    },
    /// TEST-ONLY tamper of one audit entry (see `test_tamper_audit_entry`).
    #[cfg(all(test, feature = "security"))]
    TestTamperAudit {
        graph: String,
        seq: u64,
        reply: std::sync::mpsc::SyncSender<Result<(), String>>,
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
        reply: std::sync::mpsc::SyncSender<Result<Option<u64>, String>>,
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
        reply: std::sync::mpsc::SyncSender<Result<crate::protocol::MerkleInclusionReport, String>>,
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
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityMintRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
        done: oneshot::Sender<
            Result<crate::epistemic_operations_ext::WorkItemClaimCapabilityResult, String>,
        >,
    },
    VerifyWorkItemClaimCapability {
        graph: String,
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityVerifyRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
        done: oneshot::Sender<
            Result<crate::epistemic_operations_ext::WorkItemClaimCapabilityResult, String>,
        >,
    },
    /// Native development-lane hold/quota mutation (RMDD-28: Reserve/Renew/
    /// Observe/Finish/Cleanup/UpdateQuota). Flushes pending graph mutations
    /// first, then admits its own group over the native `development_lane_*`
    /// tables in one writer-owned commit, same shape as the claim-capability
    /// commands above.
    CommitDevelopmentLane {
        graph: String,
        method: Box<Method>,
        now_ms: u64,
        done: oneshot::Sender<Result<Vec<u8>, String>>,
    },
    /// Native capacity-cell/lease mutation.  Like the development-lane
    /// authority, this flushes queued graph mutations first and executes the
    /// complete CAS/usage update in one writer-owned immediate transaction.
    CommitCapacityLease {
        graph: String,
        method: Box<Method>,
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
    /// Durably bind one projection consumer to its outbox topic on the writer
    /// thread. Subscription and claim both use the same shard-owned ledger;
    /// keeping this as a command prevents a read-then-write backend adapter
    /// from bypassing the single-writer authority.
    MutationOutboxSubscribe {
        graph: String,
        consumer: String,
        topic: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Lease pending transactional-outbox events on the writer thread so claim
    /// selection and lease installation are one durable transaction.
    MutationOutboxClaim {
        graph: String,
        consumer: String,
        budget: Box<OutboxClaimBudget>,
        done: oneshot::Sender<Result<(OutboxClaimOutcome, OutboxClaimBudget), String>>,
    },
    /// Ack one held lease. The ack IS the projection-cursor advance — the kernel
    /// marks the row delivered and moves the consumer's watermark in ONE
    /// transaction, so there is no separate "advance the cursor" step (and no
    /// crash window between the two) for this command to carry.
    MutationOutboxAck {
        graph: String,
        lease: Box<MutationOutboxLease>,
        now_ms: u64,
        done: oneshot::Sender<Result<MutationProjectionCursor, String>>,
    },
    Shutdown {
        reply: std::sync::mpsc::SyncSender<()>,
    },
    // ── Raft log/meta (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — all on the writer thread because redb
    // holds an EXCLUSIVE per-process file lock, so log + M2 graph data must go
    // through the ONE thread that owns the shard. ─────────────────────────────
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
        reply: std::sync::mpsc::SyncSender<Result<Vec<Vec<u8>>, String>>,
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
        reply: std::sync::mpsc::SyncSender<LogBoundsResult>,
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
        reply: std::sync::mpsc::SyncSender<Result<Option<Vec<u8>>, String>>,
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
        reply: std::sync::mpsc::SyncSender<Result<Option<Vec<u8>>, String>>,
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
        reply: std::sync::mpsc::SyncSender<XshardPrepareScan>,
    },
    /// Scan digest-only decisions for parent-aware startup cleanup.
    XshardScanDecisions {
        reply: std::sync::mpsc::SyncSender<XshardDecisionScan>,
    },
    /// Read a txn's decision (Some(true)=commit, Some(false)=abort, None=undecided).
    XshardDecisionGet {
        txn_id: String,
        reply: std::sync::mpsc::SyncSender<Result<Option<bool>, String>>,
    },
    /// Is this decision/pending marker retained for a MutationBatch parent?
    XshardDecisionRetainGet {
        txn_id: String,
        reply: std::sync::mpsc::SyncSender<Result<bool, String>>,
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
        reply: std::sync::mpsc::SyncSender<MatViewScanResult>,
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
        reply: std::sync::mpsc::SyncSender<MatViewScanResult>,
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
        reply: std::sync::mpsc::SyncSender<MatViewScanResult>,
    },
}
