//! redb write-through persistence backend (CONCEPT:EG-KG.storage.kg-kg, feature `redb`).
//!
//! The authoritative tier commits every graph mutation into an embedded
//! canonical `redb` shard database (`{persist_dir}/graph-<n>.redb`), keyed by a
//! `(graph, …)` prefix so
//! all tenants share the same tables (not a file per graph). The in-memory graph is
//! a bounded resident projection with durability-gated read-through eviction.
//!
//! ## The #1 risk: never one commit per mutation
//!
//! A shard commit is a B-tree + WAL + fsync — orders of magnitude more expensive
//! than a row write, so one per graph mutation would collapse write p99. A
//! dedicated OS thread therefore owns the file's
//! [`Shard`](crate::redb_store::shard::Shard), drains a bounded channel, and
//! folds MANY mutations into ONE admitted scope group per group-commit boundary.
//!
//! ## Every shard commit is `Durability::Immediate`, and there is no runtime knob
//!
//! The level is not this backend's to choose any more: `eg_storage`'s
//! `physical::root::WRITE_DURABILITY` is a CONSTANT `Durability::Immediate` on
//! the one `begin_write` every kernel mutation takes, because a weaker level
//! would let redb roll a committed ledger back on crash — un-consuming an
//! acknowledged replay nonce and re-enabling the double apply. Group commit still
//! folds N ops into ONE Immediate fsync, while every batch pays that real fsync at
//! the group-commit boundary.
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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::oneshot;

use redb::{ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use crate::change_envelope::{
    ChangeCursor, ChangeEnvelope, ChangeEnvelopeCommit, ChangeEnvelopeRecord, ContentVersion,
};
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
use crate::redb_store::shard::{Shard, ShardWrite};
#[cfg(any(feature = "compute-dist", feature = "matview"))]
use crate::redb_store::MatViewScanResult;
use crate::redb_store::{
    clear_xshard_decision, clear_xshard_prepare, commit_change_envelope, commit_change_envelopes,
    commit_crossmodal, commit_mutation_batch, commit_mutation_batch_crossmodal,
    commit_mutation_batch_state, commit_ops, durable_node_presence as read_durable_node_presence,
    get_xshard_decision, get_xshard_decision_retain, get_xshard_prepare, purge_graph_rows,
    put_xshard_decision, put_xshard_prepare, put_xshard_recoverable_pending, read_all_dumps,
    read_all_graph_meta, read_change_cursor as read_change_cursor_record,
    read_change_envelope as read_change_envelope_record,
    read_content_version as read_content_version_record, read_graph_dump,
    read_mutation_batch_for_graph as read_mutation_batch_record,
    read_mutation_graph_version as read_mutation_graph_version_record,
    read_mutation_outbox as read_mutation_outbox_records, read_one_node,
    read_resource_reservation as read_resource_reservation_record,
    read_resource_reservation_status as read_resource_reservation_status_record,
    scan_xshard_decisions, scan_xshard_prepares, write_graph_meta, GraphDump, XshardDecisionScan,
    XshardPrepareScan, RAFT_LOG,
};
use crate::server::persistence::writer_reply::await_writer_reply;
use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome};
/// `(first, last)` present Raft log index for a group, or an error (CONCEPT:EG-KG.storage.one-fsync-covers-raft).
type LogBoundsResult = Result<(Option<u64>, Option<u64>), String>;

/// Per-ATTEMPT identity for one shard write this backend admits.
///
/// The kernel resolves a batch id that already carries a durable receipt to
/// `Begin::Replay` and SKIPS it, so a reused id would silently drop a drain after
/// a restart. Hence `(pid, one nonce per process, counter)` — never derived from
/// `(raft_group, index)` or any caller key; see `redb_store::shard::drain_batch`.
fn shard_write_attempt_id(label: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    static NONCE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let nonce = *NONCE.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos() as u64)
            .unwrap_or(0)
    });
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{label}/{}-{nonce}:{seq}", std::process::id())
}
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

/// Encryption-at-rest key-mismatch canary (GOC-16 / BUG-248). The table carries one
/// AEAD-sealed canary row plus one non-secret, versioned key-binding row. The sealed
/// plaintext includes the binding, so changing the plaintext metadata alone cannot
/// make a different key reference appear valid. `ShardWriter::open` verifies both
/// before spawning a writer thread or binding a listener.
///
/// The table is copied verbatim by online backup, offline shard migration, and
/// restore. A backup therefore preserves the key identity/version boundary; restore
/// never silently re-establishes a canary under whatever key happens to be present.
/// A changed key reference is an explicit rotation boundary and fails closed until
/// the documented offline re-encryption ceremony has completed.
pub(crate) const ENCRYPTION_CANARY: TableDefinition<&str, &[u8]> =
    TableDefinition::new("encryption_canary");

#[cfg(feature = "security")]
const ENCRYPTION_CANARY_KEY: &str = "v1";
#[cfg(feature = "security")]
pub(crate) const ENCRYPTION_KEY_BINDING_KEY: &str = "key-binding-v1";
#[cfg(feature = "security")]
const ENCRYPTION_CANARY_PLAINTEXT: &[u8] = b"epistemic-graph-encryption-canary";

/// What [`verify_or_establish_encryption_canary`] did, so the open path can log the
/// TRUTH rather than one undifferentiated "ENABLED" line (BUG-PE-055).
#[cfg(feature = "security")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanaryOutcome {
    /// The store's existing canary decrypted under the configured key.
    Verified,
    /// A pre-NE-028 fixed canary was verified and upgraded to the bound pair.
    UpgradedLegacy,
    /// FIRST use of a key on an EMPTY store — a new canary was written. On a store
    /// that already held rows this is refused, not logged.
    Established,
}

/// Does this shard already hold durable rows (BUG-PE-055)?
///
/// The question the canary path could not previously ask. Establishing a canary is
/// only safe on an EMPTY store: on a populated PLAINTEXT store it silently converts
/// every subsequent write to sealed framing while every pre-existing value stays
/// plaintext, so each of those reads then fails closed with
/// `"encrypted durable value is missing sealed framing"`. `crypto`'s
/// [`TXN_RECOVERY_KEY_ENV`](crate::crypto::TXN_RECOVERY_KEY_ENV) doc already names that
/// as "a destructive-read operation on a populated plaintext store, not a config
/// toggle"; this is the probe that lets the open path refuse it.
/// Asked of the file's CATALOG, which is the only reader class that exists before
/// any graph scope is bound — and this runs at open, before one is. That is not a
/// weakening: `graph_meta` is file-wide, and every graph that receives a row gets
/// its catalog entry backfilled in the SAME admitted group as those rows
/// (`redb_store::backfill_graph_meta_row`), so a shard with rows always has a
/// catalog entry. The one direction this can be wrong — a registered graph that
/// never received a row — refuses to establish a canary on a store that is empty
/// in fact, which is the safe direction to be wrong in.
#[cfg(feature = "security")]
fn shard_holds_durable_rows(shard: &Shard) -> Result<bool, String> {
    let control = shard.control_read()?;
    let catalog = control.open_owner_table(crate::redb_store::GRAPH_META)?;
    let has_rows = {
        let mut rows = catalog.iter().map_err(|e| e.to_string())?;
        match rows.next() {
            None => false,
            Some(row) => {
                row.map_err(|e| e.to_string())?;
                true
            }
        }
    };
    Ok(has_rows)
}

/// Does this shard carry encryption-at-rest metadata (BUG-PE-055)?
///
/// Answered on the NO-KEY path, which previously never looked: a store whose values
/// are sealed opened cleanly with no key at all and then failed per-read. Startup is
/// where that belongs.
#[cfg(feature = "security")]
fn shard_carries_encryption_metadata(shard: &Shard) -> Result<bool, String> {
    let control = shard.control_read()?;
    let table = control.open_owner_table(ENCRYPTION_CANARY)?;
    for key in [ENCRYPTION_CANARY_KEY, ENCRYPTION_KEY_BINDING_KEY] {
        if table.get(key).map_err(|e| e.to_string())?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Verify (or, on first use, establish) that `cipher` is the SAME key and stable
/// identity/version that sealed this shard's previous data. Called from
/// `ShardWriter::open` before any writer thread spawns or any listener binds.
///
/// A pre-key-lifecycle store may have only the original fixed plaintext canary. That
/// legacy row is verified and upgraded atomically with the new binding record. A
/// store with a binding row but no canary (or vice versa) is treated as tampered and
/// fails closed; neither half is ever silently recreated.
#[cfg(feature = "security")]
fn plan_encryption_canary(
    shard: &Shard,
    cipher: &crate::crypto::ValueCipher,
) -> Result<CanaryPlan, String> {
    let (existing_canary, existing_binding) = {
        let control = shard.control_read()?;
        let table = control.open_owner_table(ENCRYPTION_CANARY)?;
        let canary = table
            .get(ENCRYPTION_CANARY_KEY)
            .map_err(|e| e.to_string())?
            .map(|v| v.value().to_vec());
        let binding = table
            .get(ENCRYPTION_KEY_BINDING_KEY)
            .map_err(|e| e.to_string())?
            .map(|v| v.value().to_vec());
        (canary, binding)
    };

    let configured_ref = cipher.key_ref();
    let expected_plaintext = crate::crypto::encryption_canary_plaintext(configured_ref);
    match (existing_canary, existing_binding) {
        (Some(sealed), Some(binding_bytes)) => {
            let binding = crate::crypto::EncryptionKeyBinding::decode(&binding_bytes)?;
            let persisted_ref = binding.key_ref()?;
            if &persisted_ref != configured_ref {
                return Err(format!(
                    "refusing to open the durable graph store: configured encryption key \
                     reference {}@{} does not match persisted reference {}@{}; complete \
                     the documented offline key-rotation procedure before changing it",
                    configured_ref.id,
                    configured_ref.version,
                    persisted_ref.id,
                    persisted_ref.version,
                ));
            }
            let plaintext = cipher.unseal(&sealed).map_err(|_| {
                format!(
                    "refusing to open the durable graph store: {} does not match the key \
                     that previously encrypted this store (canary decryption failed)",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                )
            })?;
            if plaintext != expected_plaintext {
                return Err(
                    "refusing to open the durable graph store: encryption canary binding \
                     is invalid or tampered"
                        .to_string(),
                );
            }
            Ok(CanaryPlan::Verified(CanaryOutcome::Verified))
        }
        (Some(sealed), None) => {
            // Upgrade the pre-NE-028 fixed canary only after proving the configured
            // key can decrypt it. The binding is then written in the SAME transaction
            // as the new authenticated canary, so a crash leaves either the legacy
            // row (safe to retry) or the complete new pair.
            let plaintext = cipher.unseal(&sealed).map_err(|_| {
                format!(
                    "refusing to open the durable graph store: {} does not match the key \
                     that previously encrypted this store (legacy canary decryption failed)",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                )
            })?;
            if plaintext != ENCRYPTION_CANARY_PLAINTEXT {
                return Err(
                    "refusing to open the durable graph store: legacy encryption canary is \
                     invalid or tampered"
                        .to_string(),
                );
            }
            let binding = crate::crypto::EncryptionKeyBinding::from_key_ref(configured_ref);
            let binding_bytes = binding.encode()?;
            let sealed = cipher.seal(&expected_plaintext);
            Ok(CanaryPlan::Write {
                sealed,
                binding_bytes,
                outcome: CanaryOutcome::UpgradedLegacy,
            })
        }
        (None, Some(_)) => Err(
            "refusing to open the durable graph store: encryption key binding exists but \
             its canary is missing"
                .to_string(),
        ),
        (None, None) => {
            // BUG-PE-055: this arm used to be taken IDENTICALLY for a brand-new store
            // and for a populated store that was written in plaintext — it established
            // a canary and started encrypting, silently, logging only
            // "redb encryption-at-rest ENABLED". Every pre-existing value then became
            // unreadable one read at a time. Establishing a canary is only ever
            // correct on an EMPTY store.
            if shard_holds_durable_rows(shard)? {
                return Err(format!(
                    "refusing to open the durable graph store: {} is set but this store \
                     carries no encryption canary and already holds durable rows, so it \
                     was written in PLAINTEXT. Enabling encryption-at-rest on a populated \
                     plaintext store is a destructive-read operation, not a config \
                     toggle: every existing value would fail to unseal. Complete the \
                     documented offline re-encryption procedure into a fresh persist \
                     dir, or unset {} to keep serving this store as it is.",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                    crate::crypto::ENCRYPTION_KEY_ENV,
                ));
            }
            let binding = crate::crypto::EncryptionKeyBinding::from_key_ref(configured_ref);
            let binding_bytes = binding.encode()?;
            let sealed = cipher.seal(&expected_plaintext);
            Ok(CanaryPlan::Write {
                sealed,
                binding_bytes,
                outcome: CanaryOutcome::Established,
            })
        }
    }
}

/// What [`plan_encryption_canary`] decided this store needs, WITHOUT doing it.
///
/// The two halves are separate because the decision is per-STORE while the
/// refusal is per-PERSIST-DIR. A dir holds K shard files, each with its own
/// canary table and its own local view of "am I empty?", and `open` refuses the
/// whole dir if ANY of them refuses. While establishing the canary happened
/// inside the per-shard decision, a refusal on the one shard that held the data
/// still left every OTHER shard — genuinely empty, and so genuinely eligible to
/// have a canary established — durably converted to a key-required store.
///
/// The dir was then bricked in both directions: it would not open WITH the key
/// (the data shard refuses "this store was written in PLAINTEXT") and would not
/// open WITHOUT it (the converted shards refuse "this store carries
/// encryption-at-rest metadata"), which makes the first refusal's own documented
/// remediation -- "unset EPISTEMIC_GRAPH_ENCRYPTION_KEY to keep serving this
/// store as it is" -- false. K is greater than one by default in a served
/// process (`resolve_shard_count` autosizes from CPUs, and under Raft K == N
/// groups), so this was the ordinary case, not an edge one.
///
/// Planning first makes the refusal byte-clean: the open path plans every shard,
/// and only once every shard has agreed does it apply any plan.
#[cfg(feature = "security")]
enum CanaryPlan {
    /// This store already carries a canary this key verifies, or carries none
    /// and needs none. Nothing to write.
    Verified(CanaryOutcome),
    /// This store needs its canary written before it can be served.
    Write {
        sealed: Vec<u8>,
        binding_bytes: Vec<u8>,
        outcome: CanaryOutcome,
    },
}

/// Commit a [`CanaryPlan`], after every store in the persist dir has agreed.
#[cfg(feature = "security")]
fn apply_encryption_canary(shard: &Shard, plan: CanaryPlan) -> Result<CanaryOutcome, String> {
    match plan {
        CanaryPlan::Verified(outcome) => Ok(outcome),
        CanaryPlan::Write {
            sealed,
            binding_bytes,
            outcome,
        } => {
            write_encryption_canary(shard, &sealed, &binding_bytes)?;
            Ok(outcome)
        }
    }
}

/// Write the sealed canary and its key-binding row as ONE control-only commit, so
/// a crash leaves the store's previous state or the complete new pair, never half.
/// Control-only because `encryption_canary` is file-wide: it belongs to the file,
/// not to any graph it hosts.
#[cfg(feature = "security")]
fn write_encryption_canary(
    shard: &Shard,
    sealed: &[u8],
    binding_bytes: &[u8],
) -> Result<(), String> {
    in_control_write(shard, "encryption_canary", |write| {
        let mut table = write.control().open_table(ENCRYPTION_CANARY)?;
        table
            .insert(ENCRYPTION_CANARY_KEY, sealed)
            .map_err(|e| e.to_string())?;
        table
            .insert(ENCRYPTION_KEY_BINDING_KEY, binding_bytes)
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
}

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
const SHARD_WRITER_JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

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
        reply: std::sync::mpsc::SyncSender<Result<super::online_reshard::RawGraphRows, String>>,
    },
    /// Import ONE graph's verbatim rows on an online shard move (CONCEPT:EG-KG.backend.catalog-shard-resolve). Runs on
    /// the DESTINATION shard's writer and lands them in ONE commit — the
    /// commit-before-ack point of the move.
    ImportGraphRaw {
        graph: String,
        rows: Box<super::online_reshard::RawGraphRows>,
        reply: std::sync::mpsc::SyncSender<Result<(), String>>,
    },
    /// Import ONLY the DELTA of an online shard move (CONCEPT:EG-KG.backend.flush-pending-first, R1 delta-copy). Runs
    /// on the DESTINATION shard's writer under the exclusive routing quiesce; lands the
    /// small set of rows that changed since the bulk pass (upserts + removals) in ONE
    /// commit — the short under-quiesce write that shrinks the pause.
    ImportGraphDelta {
        graph: String,
        delta: Box<super::online_reshard::RawGraphDelta>,
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
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct RedbGroupCommitTestControl {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

#[cfg(test)]
impl RedbGroupCommitTestControl {
    pub(crate) fn new() -> (
        Arc<Self>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release, release_rx) = std::sync::mpsc::sync_channel(1);
        (
            Arc::new(Self {
                entered,
                release: std::sync::Mutex::new(Some(release_rx)),
            }),
            entered_rx,
            release,
        )
    }

    fn wait_until_released(&self) {
        let _ = self.entered.send(());
        let release = self
            .release
            .lock()
            .expect("group-commit test control lock poisoned")
            .take();
        if let Some(release) = release {
            crate::test_rendezvous::recv_within(
                &release,
                "the group-commit test control releasing the writer",
            );
        }
    }
}

#[cfg(test)]
struct ReleaseOnDrop(Option<std::sync::mpsc::SyncSender<()>>);

#[cfg(test)]
impl ReleaseOnDrop {
    fn new(sender: std::sync::mpsc::SyncSender<()>) -> Self {
        Self(Some(sender))
    }

    fn release(&mut self) -> Result<(), std::sync::mpsc::SendError<()>> {
        self.0
            .take()
            .expect("release guard must be used at most once")
            .send(())
    }
}

#[cfg(test)]
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedbGroupCommitConfig {
    /// Max time to linger for more concurrent writers before committing a shallow
    /// barrier batch. `Duration::ZERO` disables lingering entirely (commit-on-drain
    /// = today's behavior, used as the bench baseline).
    pub linger: Duration,
    /// Only linger when `pending.ops.len()` is BELOW this — a deep batch already
    /// coalesces well, so lingering buys nothing and just adds latency (adaptive).
    pub shallow_threshold: usize,
    /// Test-only gate used to hold the writer at the start of the linger window
    /// while the fixture queues the rest of its burst. Production opens never set
    /// this, so the live writer path has no synchronization hook.
    #[cfg(test)]
    pub(crate) test_control: Option<Arc<RedbGroupCommitTestControl>>,
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
            #[cfg(test)]
            test_control: None,
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
/// `linger_waiting` exposes whether the writer is currently inside that bounded
/// receive window, which makes operational probes and deterministic contention
/// tests observe the mechanism instead of guessing from scheduler timing.
#[derive(Debug, Default)]
pub struct RedbCommitStats {
    /// Group-commit `WriteTransaction`s issued on the run-loop barrier/timeout path.
    pub commits: AtomicU64,
    /// Total graph ops folded across those commits.
    pub ops: AtomicU64,
    /// How many of those commits paid a micro-linger window.
    pub lingered: AtomicU64,
    /// True only while the writer is blocked in the bounded micro-linger receive.
    linger_waiting: AtomicBool,
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
    /// Whether this writer is currently waiting for a concurrent mutation in its
    /// bounded micro-linger window.
    pub fn is_linger_waiting(&self) -> bool {
        self.linger_waiting.load(Ordering::Acquire)
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
// K = clamp(effective-cgroup-cpu/2, 1, 8), overridable via `EPISTEMIC_GRAPH_REDB_SHARDS`. Every K uses
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
///   * Otherwise K = clamp(effective-cgroup-cpu/2, 1, 8) — mirrors the shared
///     `crate::autosize::detect_capacity()` seam.
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
        let cpus = crate::autosize::detect_capacity().reserved_cpus();
        (cpus / 2).clamp(1, 8)
    }
}

/// Resolve the per-shard early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb — auto-size the
/// previously HARDCODED `4096`). The writer flushes a `Pending` batch early once it
/// holds this many ops, bounding writer memory before the bounded channel saturates.
///   * `EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD` may lower the automatic bound,
///     but is capped by it after the hard 64..=1_048_576 validation window.
///   * Else ~half the authoritative writer queue depth (`capacity`, itself
///     hardware-auto-sized via `Capacity::writer_queue()`), clamped 256..=16384.
fn resolve_flush_threshold(capacity: usize) -> usize {
    let automatic = (capacity / 2).clamp(256, 16_384);
    if let Ok(v) = std::env::var("EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD") {
        if let Ok(n) = v.trim().parse::<usize>() {
            if n > 0 {
                return n.clamp(64, 1_048_576).min(automatic);
            }
        }
    }
    automatic
}

/// The WRITER of one durable shard (CONCEPT:EG-KG.backend.sharded-k-way-durable): the off-reactor
/// group-commit thread that owns one shard file, its bounded channel, its `Pending`
/// (incl. the EG-024 linger + EG-025 audit tail cache) and its commit counters.
/// Single-writer-per-FILE, so K writers commit in parallel on K cores.
///
/// It is NOT the store: the store is [`crate::redb_store::shard::Shard`], the
/// kernel-owned `OwnerLayout::GraphShard` file this writer commits into and every
/// off-writer snapshot read is issued from. This type is the thread and the queue
/// in front of it, which is what its name says.
struct ShardWriter {
    db_path: String,
    /// `Weak` handle to THIS shard's kernel-owned store (CONCEPT:EG-KG.storage.snapshot-read-off-writer —
    /// snapshot reads off the writer). redb 4.1 is MVCC: a kernel-issued `ScopedRead`
    /// runs CONCURRENTLY with the single writer, so the point-read / read-through
    /// path `upgrade()`s this `Weak` and serves an evicted node DIRECTLY off
    /// `shard.read(&shard.graph(g)?)` without touching the writer's channel and
    /// without forcing a group-commit. `Weak`, not a strong clone, is deliberate:
    /// the writer thread owns the SOLE STRONG `Arc`, so the exclusive per-process
    /// file lock releases EXACTLY when that thread exits on `shutdown` — a reopen of
    /// the persist dir then succeeds, and a read after shutdown fails fast
    /// (upgrade ⇒ `None`) instead of pinning the lock. The `Shard` (not a raw
    /// database) is what is shared because it carries the bound-handle cache, so a
    /// warm graph never re-binds.
    shard: Weak<Shard>,
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

/// One shard file opened and DECIDED, with nothing written yet.
///
/// The second half of the two-phase open (see [`CanaryPlan`]): the persist dir's
/// refusal is collective, so every shard is planned before any shard is changed.
struct PreparedShard {
    db_path: String,
    shard: Arc<Shard>,
    #[cfg(feature = "security")]
    cipher: Option<crate::crypto::ValueCipher>,
    #[cfg(feature = "security")]
    canary: Option<CanaryPlan>,
    #[cfg(feature = "security")]
    txn_recovery_cipher: Option<crate::crypto::ValueCipher>,
}

impl ShardWriter {
    /// Open (or create) `db_path` as a kernel-owned store and DECIDE its
    /// encryption posture, writing nothing.
    ///
    /// Every fallible, refusing step lives here and every durable one lives in
    /// [`Self::spawn`], so a persist dir whose open is refused is byte-identical
    /// to what it was before the call — see [`CanaryPlan`] for the bricking this
    /// separation prevents.
    fn prepare(db_path: String) -> Result<PreparedShard, String> {
        // ONE shared `Shard` per file (CONCEPT:EG-KG.storage.snapshot-read-off-writer): the writer thread and
        // the snapshot-read path both hold a clone of this `Arc`, and redb's exclusive
        // per-file lock is why reads share it rather than re-opening.
        //
        // No schema bootstrap runs here any more. `StorageKernel::create_owner`
        // materializes the WHOLE declared `OwnerLayout::GraphShard` census — the 41
        // scope-prefixed tables, the 12 file-wide ones (`raft_meta` and
        // `encryption_canary` among them) and the ledger — and re-validates it on
        // every open, so the hand-written `initialize_canonical_tables` bootstrap
        // that used to run here is deleted: beside `create_owner` it would be a
        // second physical authority over the same file.
        let shard = Arc::new(Shard::open(std::path::Path::new(&db_path))?);
        // Encryption-at-rest (CONCEPT:EG-KG.sharding.row-level-security): resolve the value-blob cipher ONCE at
        // open from EPISTEMIC_GRAPH_ENCRYPTION_KEY (the KMS seam). `None` ⇒ encryption
        // OFF ⇒ the durable format + write/read paths are byte-for-byte unchanged.
        #[cfg(feature = "security")]
        let cipher = crate::crypto::ValueCipher::from_env_checked()?;
        // GOC-16: encryption-at-rest readiness posture (EPISTEMIC_GRAPH_ENCRYPTION_
        // REQUIRED). Previously the `None` branch here logged NOTHING at all — a
        // production deployment could run fully unencrypted with zero signal to the
        // operator. `Warn` (the shipped default) fixes the silence without changing
        // startup behavior; `On` fails closed BEFORE the writer thread spawns or any
        // listener binds, by returning the same `Result<Self, String>` every other
        // fallible step in this function already uses (`RedbBackend::open`
        // propagates it to `main.rs`'s existing `eprintln!` + `exit(1)` refusal).
        #[cfg(feature = "security")]
        let mut canary_plan: Option<CanaryPlan> = None;
        #[cfg(feature = "security")]
        match &cipher {
            Some(c) => {
                // GOC-16 / BUG-248: fail closed BEFORE the writer thread spawns or any
                // listener binds if the configured key does not match the key that
                // sealed this store's existing data — see `plan_encryption_canary`'s
                // doc for exactly what this does and does not cover.
                //
                // DECIDE only. The plan is applied in `spawn`, after every shard in
                // this persist dir has agreed — see `CanaryPlan`'s doc for why a
                // per-shard write here bricked a multi-shard dir on a refusal.
                canary_plan = Some(plan_encryption_canary(&shard, c)?);
            }
            None => {
                // BUG-PE-055: the no-key path never looked at the canary, so a store
                // whose values are SEALED opened cleanly with no key and then failed
                // one read at a time ("encrypted durable value requires configured key
                // material"). This is the symmetric partner of the wrong-key refusal
                // above, and it holds regardless of the required-mode posture — a
                // missing key for an encrypted store is not a posture choice.
                if shard_carries_encryption_metadata(&shard)? {
                    return Err(format!(
                        "refusing to open the durable graph store: this store carries \
                         encryption-at-rest metadata (its value blobs are sealed) but {} \
                         is not set, so every read of an existing value would fail. \
                         Configure the key reference and material that encrypted this \
                         store.",
                        crate::crypto::ENCRYPTION_KEY_ENV,
                    ));
                }
                match crate::crypto::encryption_required_mode() {
                    crate::crypto::EncryptionRequiredMode::Off => {}
                    crate::crypto::EncryptionRequiredMode::Warn => {
                        tracing::warn!(
                            "redb encryption-at-rest is OFF ({} is not set) — value blobs are \
                         stored in PLAINTEXT. Set {}=on to refuse to start instead of \
                         warning.",
                            crate::crypto::ENCRYPTION_KEY_ENV,
                            crate::crypto::ENCRYPTION_REQUIRED_ENV,
                        );
                    }
                    crate::crypto::EncryptionRequiredMode::On => {
                        return Err(format!(
                            "refusing to open the durable graph store: at-rest encryption is \
                         REQUIRED ({}=on) but {} is not set",
                            crate::crypto::ENCRYPTION_REQUIRED_ENV,
                            crate::crypto::ENCRYPTION_KEY_ENV,
                        ));
                    }
                }
            }
        }
        // Transaction-recovery-plan cipher (D-ORC-50) — resolved SEPARATELY from the
        // data-at-rest cipher above so enabling multi-op OCC transaction durability never
        // implies (and never requires) enabling at-rest encryption of existing plaintext
        // values. See `crypto::TXN_RECOVERY_KEY_ENV` for the full rationale.
        #[cfg(feature = "security")]
        let txn_recovery_cipher = crate::crypto::ValueCipher::from_env_for_txn_recovery();
        #[cfg(feature = "security")]
        if txn_recovery_cipher.is_some() && cipher.is_none() {
            tracing::info!(
                "redb transaction-recovery-plan sealing ENABLED via a dedicated key \
                 (EPISTEMIC_GRAPH_TXN_RECOVERY_KEY) — data-at-rest encryption remains OFF"
            );
        }
        Ok(PreparedShard {
            db_path,
            shard,
            #[cfg(feature = "security")]
            cipher,
            #[cfg(feature = "security")]
            canary: canary_plan,
            #[cfg(feature = "security")]
            txn_recovery_cipher,
        })
    }

    /// Commit this shard's decided encryption plan and spawn its group-commit
    /// writer thread. Called only after EVERY shard in the persist dir prepared
    /// successfully, so this is the first point at which anything is written.
    fn spawn(
        prepared: PreparedShard,
        thread_name: String,
        capacity: usize,
        flush_threshold: usize,
        group_commit: RedbGroupCommitConfig,
    ) -> Result<Self, String> {
        let PreparedShard {
            db_path,
            shard,
            #[cfg(feature = "security")]
            cipher,
            #[cfg(feature = "security")]
            canary,
            #[cfg(feature = "security")]
            txn_recovery_cipher,
        } = prepared;
        // BUG-PE-055: log AFTER the decision, and say which of the three things
        // actually happened. The old unconditional "ENABLED" line was emitted
        // before the check ran, so it appeared even on a store that was about to
        // be refused — and, worse, it read identically whether the key was the
        // store's existing key or a brand-new one being imposed on it.
        #[cfg(feature = "security")]
        if let Some(plan) = canary {
            match apply_encryption_canary(&shard, plan)? {
                CanaryOutcome::Verified => tracing::info!(
                    "redb encryption-at-rest ENABLED (value blobs sealed with \
                     ChaCha20-Poly1305); the configured key matches this store's \
                     existing key binding"
                ),
                CanaryOutcome::UpgradedLegacy => tracing::info!(
                    "redb encryption-at-rest ENABLED (value blobs sealed with \
                     ChaCha20-Poly1305); this store's pre-key-lifecycle canary was \
                     verified and upgraded to a bound key reference"
                ),
                CanaryOutcome::Established => tracing::warn!(
                    "redb encryption-at-rest ENABLED (value blobs sealed with \
                     ChaCha20-Poly1305) and a NEW key binding was established: this \
                     store had no encryption canary and no durable rows, so {} is \
                     being used for the FIRST time here. If you expected this store \
                     to already hold data, it is not the store you meant — check the \
                     persist dir before writing to it.",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                ),
            }
        }
        let (tx, rx) = sync_channel::<Cmd>(capacity.max(1));
        // Adaptive group-commit micro-linger config + observability (CONCEPT:EG-KG.backend.adaptive-linger-coalesce).
        // Resolved once by the backend open path (Configuration discipline); the
        // writer thread owns the supplied config and a clone of the stats Arc so
        // callers can read batch-size/throughput live.
        let stats = Arc::new(RedbCommitStats::default());
        let stats_writer = stats.clone();
        // Keep a clone of the cipher for the snapshot-read path (CONCEPT:EG-KG.storage.snapshot-read-off-writer); the
        // writer thread takes ownership of the original below.
        #[cfg(feature = "security")]
        let cipher_for_reads = cipher.clone();
        // A `Weak` for the off-writer snapshot-read path (CONCEPT:EG-KG.storage.snapshot-read-off-writer). The writer
        // thread below takes the SOLE STRONG `Arc`, so the redb file lock releases
        // exactly when that thread exits on shutdown — matching the pre-EG-027 lifetime
        // (a reopen after shutdown succeeds; a read after shutdown upgrades to `None`).
        let shard_weak = Arc::downgrade(&shard);
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run(
                    rx,
                    shard,
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
            shard: shard_weak,
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
            let (reply, rx) = std::sync::mpsc::sync_channel(1);
            if self.tx.send(Cmd::Shutdown { reply }).is_ok() {
                let _ = await_writer_reply(&rx, "shutdown");
            }
            // The ack above means the writer's loop has returned, so this join
            // is prompt in the healthy case. It is still bounded: a writer that
            // acked and then wedged in its own teardown would otherwise hang
            // shutdown forever.
            if let Err(error) = crate::bounded_join::join_within(
                handle,
                "the redb shard writer thread",
                SHARD_WRITER_JOIN_TIMEOUT,
            ) {
                eprintln!("redb shard writer did not shut down cleanly: {error}");
            }
        }
    }
}

impl Drop for ShardWriter {
    fn drop(&mut self) {
        // A backend may be released through its last `Arc` without an explicit
        // shutdown (test states and startup-error paths do this). Join the writer
        // here so its sole strong `Shard` handle and redb file lock are released
        // before the next in-process open, rather than detaching a resource-owning
        // thread until the scheduler happens to observe channel disconnect.
        self.shutdown();
    }
}

/// Fixed [`eg_types::MutationScopeIdentity`] for the admin-mutations / "cluster-admin"
/// coordinator store (`{persist_dir}/admin-mutations.redb`) — see [`AdminMutationStore`].
///
/// GRAPH-shaped, not native: every batch here is compiled with an "either"-family domain
/// (`ControlPlane`/`MultiGraph`), which `mutation_batch::finish_batch` maps to a
/// graph-shaped identity, so this identity must match that shape exactly or every
/// admin-saga commit fails closed with "mutation scope binding identity mismatch". Only
/// [`AdminOwner`]'s `OwnerLayout::LedgerOnly` can serve it: declaring no owner tables, it
/// is the one layout that places no shape requirement on the scopes it binds.
pub(crate) fn cluster_admin_scope_identity() -> Result<eg_types::MutationScopeIdentity, String> {
    eg_types::MutationScopeIdentity::fixed_graph(
        "native",
        "cluster-admin",
        crate::server::mutation_batch::COMPILED_BATCH_INCARNATION,
    )
}

/// [`eg_storage::PrivatePayloadIntegrity`] for the admin-mutations store, backed by the
/// SAME transaction-recovery-plan cipher (D-ORC-50) that sealed the bytes in the first
/// place (`handlers::txn::seal_txn_recovery_plan`/`open_txn_recovery_plan`). The storage
/// kernel enforces it on every private-payload read/write, so without a real authority
/// here every 2PC recovery-plan write/read fails closed — not optional plumbing.
#[cfg(feature = "security")]
struct TxnRecoveryPrivateIntegrity(crate::crypto::ValueCipher);

#[cfg(feature = "security")]
impl eg_storage::PrivatePayloadIntegrity for TxnRecoveryPrivateIntegrity {
    fn authenticate(&self, sealed: &[u8], expected_plaintext_digest: &str) -> Result<(), String> {
        use sha2::{Digest, Sha256};
        let plaintext = self.0.unseal(sealed)?;
        let actual = hex::encode(Sha256::digest(&plaintext));
        if actual != expected_plaintext_digest {
            return Err(
                "private recovery payload digest does not match its parent receipt".to_string(),
            );
        }
        Ok(())
    }
}

/// Resolve the admin-mutations store's private-payload integrity authority. Reads the
/// SAME `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` (falling back to the shared data key) as every
/// [`ShardWriter`]'s own `txn_recovery_cipher`, but as a function of the environment
/// rather than of any one shard (none is constructed yet at this call site). `None` without the
/// `security` feature or the key: the write/read path already fails closed itself there.
#[cfg(feature = "security")]
pub(crate) fn admin_mutations_private_integrity(
) -> Option<Arc<dyn eg_storage::PrivatePayloadIntegrity>> {
    crate::crypto::ValueCipher::from_env_for_txn_recovery().map(|cipher| {
        Arc::new(TxnRecoveryPrivateIntegrity(cipher))
            as Arc<dyn eg_storage::PrivatePayloadIntegrity>
    })
}

#[cfg(not(feature = "security"))]
pub(crate) fn admin_mutations_private_integrity(
) -> Option<Arc<dyn eg_storage::PrivatePayloadIntegrity>> {
    None
}

/// Operator-facing identity of the ONE physical `admin-mutations.redb` owner file.
pub(crate) const ADMIN_MUTATIONS_STORE: &str = "epistemic-graph:admin-mutations";

/// The owner domain of `admin-mutations.redb`: it declares no owner tables of its own,
/// so `eg_storage` materializes only the ledger census for it.
pub(crate) type AdminOwner = eg_storage::LedgerOnlyOwner;
/// One kernel-issued scoped read over that store, as the consumers name it.
pub(crate) type AdminScopedRead<'a> = eg_storage::ScopedRead<'a, AdminOwner>;

/// The kernel-owned admin-mutations coordinator store (RF-RULING-004): the storage
/// kernel that owns the file, the one mutation kernel it issued, and the single bound
/// serving scope. It hands no database back out. Every row the file holds is the
/// ledger's own bookkeeping, so `create_owner` materializes the whole declared census
/// at creation and no "ensure the table exists" bootstrap of ours remains.
type AdminMutationStore = crate::sidecar_store::SidecarStore<AdminOwner>;

/// Open (creating if absent) the one admin-mutations owner file.
fn open_admin_mutations(path: &std::path::Path) -> Result<AdminMutationStore, String> {
    AdminMutationStore::open_with(
        path,
        ADMIN_MUTATIONS_STORE,
        cluster_admin_scope_identity()?,
        admin_mutations_private_integrity(),
        crate::store_authority::process_authority(),
    )
}

/// Handle to the redb write-through tier (CONCEPT:EG-KG.storage.kg-kg / EG-026). The dispatch
/// path holds an `Arc` of this and calls `record`/`record_durable`; each routes by
/// graph to one of K independent single-writer [`ShardWriter`]s, so K cores commit in
/// parallel. K=1 holds exactly one shard backed by canonical `graph-0.redb`.
pub struct RedbBackend {
    /// The K shards (len >= 1). Index `shard_index(graph_fname, K)` owns a graph.
    shards: Vec<ShardWriter>,
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
    /// Local durable projection of cluster-wide/admin saga authority: in clustered
    /// serving this file is replayable consensus state on every group member rather than
    /// a pod-local coordinator, and single-node serving uses the same image directly.
    admin_mutations: AdminMutationStore,
    /// Durable cluster-topology self-report store (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1).
    /// Always opened (like `admin_mutations` above) so the shape of `RedbBackend`
    /// doesn't vary with whether Raft happens to be configured; it is populated
    /// only when a clustered node self-reports at startup (`raft::node::start`).
    /// See `server::persistence::node_info_store` for the replication story.
    node_info: Arc<super::node_info_store::NodeInfoStore>,
    /// Durable cluster-hierarchy cache (CONCEPT:EG-KG.compute.leiden-hierarchy, VIZ-1). Own-file,
    /// like `node_info` above, but carries no replication story — a cache entry
    /// is node-local and always safely recomputable. See
    /// `server::persistence::cluster_hierarchy_store` for why membership never
    /// rides graph nodes/edges.
    cluster_hierarchy: Arc<super::cluster_hierarchy_store::ClusterHierarchyStore>,
}

impl RedbBackend {
    /// Open (or create) the sharded durable tier under `persist_dir` and spawn one
    /// off-reactor group-commit writer thread per shard (CONCEPT:EG-KG.backend.sharded-k-way-durable). The shard
    /// count K is auto-sized (`resolve_shard_count`) and reconciled against any
    /// existing current on-disk layout. The exclusive per-file redb lock for every
    /// shard is acquired here at open.
    pub fn open(persist_dir: String, capacity: usize) -> Result<Self, String> {
        let backend = Self::open_with_shards(persist_dir.clone(), capacity, resolve_shard_count())?;
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
        capacity: usize,
        requested_k: usize,
    ) -> Result<Self, String> {
        Self::open_with_shards_and_config(
            persist_dir,
            capacity,
            requested_k,
            RedbGroupCommitConfig::from_env(),
        )
    }

    #[cfg(test)]
    fn open_with_group_commit_config(
        persist_dir: String,
        capacity: usize,
        group_commit: RedbGroupCommitConfig,
    ) -> Result<Self, String> {
        Self::open_with_shards_and_config(persist_dir, capacity, 1, group_commit)
    }

    fn open_with_shards_and_config(
        persist_dir: String,
        capacity: usize,
        requested_k: usize,
        group_commit: RedbGroupCommitConfig,
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
        // shard's `Shard::open` (redb's own header/allocator validation pass over
        // its file, proportional to file size, not row count) is completely independent
        // of every other shard until the `ShardWriter` structs are collected below — no
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
        let opened: Vec<Result<(PreparedShard, String), String>> = std::thread::scope(|scope| {
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
                        let result = ShardWriter::prepare(db_path.clone());
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
                        result.map(|prepared| (prepared, thread_name))
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
        // PHASE 1 of the open is now complete and NOTHING has been written. A
        // refusal from ANY shard aborts here, leaving the persist dir
        // byte-identical -- which is what makes the refusals above (and their
        // advice to unset the key and carry on) actually true for a K > 1 store.
        // See `CanaryPlan`'s doc.
        let mut prepared = Vec::with_capacity(k);
        for shard in opened {
            prepared.push(shard?);
        }
        // PHASE 2: every shard agreed, so commit each decided plan and start each
        // writer thread.
        let mut shards = Vec::with_capacity(k);
        for (shard, thread_name) in prepared {
            shards.push(ShardWriter::spawn(
                shard,
                thread_name,
                capacity,
                flush_threshold,
                group_commit.clone(),
            )?);
        }
        if k > 1 {
            tracing::info!(
                "redb: all {k} shard(s) open in {:?} (wall clock; ran concurrently)",
                shard_open_start.elapsed()
            );
        }
        let admin_path = std::path::Path::new(&persist_dir).join("admin-mutations.redb");
        let admin_mutations = open_admin_mutations(&admin_path)?;
        let node_info = Arc::new(super::node_info_store::NodeInfoStore::open(&persist_dir)?);
        let cluster_hierarchy = Arc::new(
            super::cluster_hierarchy_store::ClusterHierarchyStore::open(&persist_dir)?,
        );
        Ok(Self {
            shards,
            catalog: None,
            routing_epoch: Arc::new(RwLock::new(())),
            admin_mutations,
            node_info,
            cluster_hierarchy,
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

    /// One scoped read over the admin-mutations ledger — the capability every
    /// `eg_transaction::read_*` / `version` call over this store is a function of.
    pub(crate) fn admin_mutations_read(&self) -> Result<AdminScopedRead<'_>, String> {
        self.admin_mutations.read()
    }

    /// Prepare one admin saga, with its sealed private recovery payload when it has one:
    /// a durable `Prepared` receipt, no owner rows yet.
    pub(crate) fn admin_saga_step(
        &self,
        batch: &eg_types::MutationBatch,
        prepared_at_ms: u64,
        private_payload: Option<&[u8]>,
    ) -> Result<eg_transaction::SagaBegin, String> {
        let store = &self.admin_mutations;
        store
            .mutations()
            .saga_step(store.owner(), batch, prepared_at_ms, private_payload)
    }

    /// Terminalize one prepared admin saga. The `bool` is `true` when the saga had
    /// already committed and this call only replayed its receipt.
    pub(crate) fn admin_saga_end(
        &self,
        batch: &eg_types::MutationBatch,
        result_msgpack: Vec<u8>,
        committed_at_ms: u64,
    ) -> Result<(eg_types::MutationBatchRecord, bool), String> {
        let store = &self.admin_mutations;
        store
            .mutations()
            .saga_end(store.owner(), batch, result_msgpack, committed_at_ms)
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
        let source_shard = self.shards[src_idx]
            .shard
            .upgrade()
            .ok_or_else(|| "redb source writer thread is gone".to_string())?;
        let destination_shard = self.shards[dst_idx]
            .shard
            .upgrade()
            .ok_or_else(|| "redb destination writer thread is gone".to_string())?;
        let s1 = src_tx.clone();
        let d1 = dst_tx.clone();
        let g1 = graph.clone();
        let bulk_source = Arc::clone(&source_shard);
        let bulk_destination = Arc::clone(&destination_shard);
        let bulk = tokio::task::spawn_blocking(move || {
            let endpoints = super::online_reshard::ReshardEndpoints {
                source: bulk_source.as_ref(),
                source_tx: &s1,
                source_index: src_idx,
                destination: bulk_destination.as_ref(),
                destination_tx: &d1,
                destination_index: dst_idx,
            };
            super::online_reshard::bulk_copy(&endpoints, &g1)
        })
        .await
        .map_err(|e| format!("reshard bulk join error: {e}"))??;

        // Exclusive routing quiesce held ONLY across the delta + flip (the small window):
        // no catalog-attached write can resolve/enqueue while the route flips, so the flip
        // never loses or misroutes a write; once released, every write resolves the catalog
        // AFTER the flip and follows the graph to `dst`.
        let quiesce = self.routing_epoch.clone().write_owned().await;
        tokio::task::spawn_blocking(move || {
            let _held = quiesce;
            let endpoints = super::online_reshard::ReshardEndpoints {
                source: source_shard.as_ref(),
                source_tx: &src_tx,
                source_index: src_idx,
                destination: destination_shard.as_ref(),
                destination_tx: &dst_tx,
                destination_index: dst_idx,
            };
            super::online_reshard::delta_flip_purge(&endpoints, catalog.as_ref(), &graph, bulk)
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
    fn shard_for(&self, graph_fname: &str) -> &ShardWriter {
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
            let (reply, receive) = std::sync::mpsc::sync_channel(1);
            tx.send(Cmd::ExportGraphRaw { graph, reply })
                .map_err(|_| "redb writer thread is gone".to_string())?;
            await_writer_reply(&receive, "snapshot export")?
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
            let (reply, receive) = std::sync::mpsc::sync_channel(1);
            tx.send(Cmd::ImportGraphRaw {
                graph,
                rows: Box::new(rows),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
            await_writer_reply(&receive, "snapshot import")?
        })
        .await
        .map_err(|error| format!("snapshot import join error: {error}"))?
    }

    /// Shard 0 — the home of GLOBAL (non-per-graph) durable records: the Raft
    /// log/meta + cross-shard 2PC + materialized views. Under K=1 this is also
    /// the only shard; under active multi-Raft, each group's graph data/log stays
    /// co-located with its own shard while global records remain on shard 0.
    fn shard0(&self) -> &ShardWriter {
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
    fn shard_for_group(&self, group_id: u64) -> &ShardWriter {
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
    /// (CONCEPT:EG-KG.sharding.reshard-on-restore), while the engine keeps serving. Per shard, opens an
    /// MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer) on the LIVE writer's shared
    /// `Shard` and streams every table verbatim into a bundle shard file named by
    /// the EG-026 [`shard_filename`] scheme, then writes a `MANIFEST.json`
    /// ([`super::backup::BackupManifest`]). No quiesce: MVCC lets the snapshot read the
    /// shard's latest committed state concurrently with the writer, and commit-before-ack
    /// (CONCEPT:EG-KG.backend.authoritative-dispatch) makes each per-shard snapshot a self-consistent committed prefix.
    ///
    /// `engine_version` / `timestamp_secs` / `label` are CALLER-SUPPLIED — this library
    /// never reads the wall clock. `dst_dir` is created if absent and must not already
    /// hold bundle shard files (it refuses to overwrite).
    ///
    /// `extra_stores` carries the durable stores this backend does NOT own but that a
    /// restore is incomplete without — `rbac.redb` (identity/RBAC) and `kv.redb`. redb
    /// takes an exclusive per-file lock, so this path cannot open them itself; the
    /// caller (which holds the live `ServerState`) hands in the live handles. The
    /// stores this backend DOES own (`node_info.redb`, `catalog.redb`) are added here.
    /// Every bundled store is declared in the manifest, as is every store deliberately
    /// left out — see [`super::durable_stores`].
    pub fn backup(
        &self,
        dst_dir: &std::path::Path,
        engine_version: &str,
        timestamp_secs: u64,
        label: &str,
        extra_stores: &[&dyn super::durable_stores::BundledStoreSource],
    ) -> Result<super::backup::BackupReport, String> {
        use super::backup;
        std::fs::create_dir_all(dst_dir).map_err(|e| e.to_string())?;
        let admin_boundary_before =
            eg_storage::recovery_store_fingerprint(self.admin_mutations.kernel())?;
        let shard0 = self
            .shard0()
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let xshard_boundary_before = backup::xshard_recovery_fingerprint(&shard0)?;
        let k = self.shards.len();
        let mut report = backup::BackupReport {
            shards: k,
            ..Default::default()
        };
        #[cfg(feature = "security")]
        {
            // Every shard in one persist-dir must be bound to the same stable key
            // reference.  Capture only the non-secret identity in the manifest; raw
            // key material never crosses this boundary.
            let key_ref = self.shards.iter().find_map(|writer| {
                writer
                    .cipher
                    .as_ref()
                    .map(|cipher| cipher.key_ref().clone())
            });
            if self.shards.iter().any(|writer| {
                writer
                    .cipher
                    .as_ref()
                    .map(|cipher| Some(cipher.key_ref()) != key_ref.as_ref())
                    .unwrap_or(false)
            }) {
                return Err(
                    "encryption key reference differs between durable shards; refusing to publish backup"
                        .to_string(),
                );
            }
            if let Some(key_ref) = key_ref {
                report.encryption_key_id = Some(key_ref.id);
                report.encryption_key_version = Some(key_ref.version);
            }
        }
        for (i, writer) in self.shards.iter().enumerate() {
            // Upgrade the `Weak` to the writer's shared `Shard` (CONCEPT:EG-KG.storage.snapshot-read-off-writer).
            // `None` only after shutdown dropped the writer's strong Arc.
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            let dst_path = dst_dir.join(shard_filename(i));
            let counts = backup::write_bundle_shard(&shard, &dst_path)?;
            report.add_shard(counts);
        }
        report.admin_mutations = eg_storage::backup_recovery_store(
            self.admin_mutations.kernel(),
            &dst_dir.join(backup::ADMIN_MUTATIONS_FILE),
        )?;
        // Non-shard durable stores: the ones this backend owns, then the ones handed in.
        let node_info = self.node_info();
        let catalog = self.catalog.clone();
        let mut owned: Vec<&dyn super::durable_stores::BundledStoreSource> =
            vec![node_info.as_ref()];
        if let Some(catalog) = catalog.as_deref() {
            owned.push(catalog);
        }
        for store in owned.into_iter().chain(extra_stores.iter().copied()) {
            if !store.is_durable() {
                continue;
            }
            let name = store.file_name();
            match super::durable_stores::lookup(name).map(|entry| entry.scope) {
                Some(super::durable_stores::BackupScope::Bundled) => {}
                _ => {
                    return Err(format!(
                        "{name} is not a registered bundled durable store; declare it in                          durable_stores::DURABLE_STORES before backing it up"
                    ));
                }
            }
            if report.bundled_stores.contains_key(name) {
                return Err(format!("duplicate bundled durable store {name}"));
            }
            let rows = store.copy_into(&dst_dir.join(name))?;
            report.bundled_stores.insert(name.to_string(), rows);
        }
        let admin_boundary_after =
            eg_storage::recovery_store_fingerprint(self.admin_mutations.kernel())?;
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
            "online backup complete: {} shards, {} graphs, {} non-shard durable store(s)",
            report.shards,
            report.graph_scopes(),
            report.bundled_stores.len()
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::TestTamperAudit {
                graph: graph_fname.to_string(),
                seq,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "tamper")?
    }

    /// Verify ONE graph's tamper-evident hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security).
    /// Routed through the owner thread (exclusive file lock), which flushes pending
    /// writes first so the walk reflects the latest durable entries.
    #[cfg(feature = "security")]
    pub fn audit_verify_blocking(
        &self,
        graph_fname: &str,
    ) -> Result<crate::protocol::AuditReport, String> {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::AuditVerify {
                graph: graph_fname.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "audit_verify")?
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
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        crate::redb_store::provenance_leaf_hashes(&shard, graph_fname, node_ids, crypto)
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::ProvenanceAnchorCommit {
                graph: graph_fname.to_string(),
                root,
                members,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "provenance_anchor_commit")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::AuditProveInclusion {
                graph: graph_fname.to_string(),
                node_id: node_id.to_string(),
                anchor_seq,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "audit_prove_inclusion")?
    }

    /// Read ONE graph's durable rows as a read-only materialization view
    /// (CONCEPT:EG-KG.storage.100m-tenant — tenant rehydration). Routed through the
    /// owner thread, which flushes pending writes first. This is not a transfer
    /// image; cross-store moves use [`Self::reshard_graph`]. `None` means the graph
    /// has no durable identity.
    pub fn read_graph_dump_blocking(&self, graph_fname: &str) -> Result<Option<GraphDump>, String> {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for(graph_fname)
            .tx
            .send(Cmd::ReadGraphDump {
                graph: graph_fname.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "read_graph_dump")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
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
        await_writer_reply(&rx, "read_graph_dump_page")?
    }

    /// Reconstruct every graph from the redb store into the registry. The actual
    /// DB read runs on the owner thread (via the `Load` command) because redb holds
    /// an exclusive per-process file lock; this rebuilds each `GraphCore` from the
    /// returned dumps via the SAME `add_node`/`add_edge` calls the WAL replay uses.
    async fn load_into(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        // PARALLEL cross-shard read fan-out (CONCEPT:AU-KG.backend.roadmap-f-parallel-cross, roadmap F). Each shard's
        // writer owns only the graphs routed to it, so the registry is rebuilt from the
        // union of all K shards' dumps. Instead of routing each shard's dump SERIALLY
        // through its writer thread's channel, each shard now dumps OFF its
        // OWN kernel-issued MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer) on the blocking pool, so the
        // K reads run CONCURRENTLY on K cores and NEVER touch a writer thread (the EG-027
        // invariant — a read never forces a group-commit nor serializes behind a write).
        //
        // Consistency: redb is MVCC so each snapshot sees its shard's LATEST COMMITTED
        // state. `load_all` runs at boot BEFORE serving (no concurrent writes), and even
        // under concurrency commit-before-ack (KG-2.187) guarantees any ACKED write is
        // already committed and thus visible — exactly the EG-027 `read_node` reasoning.
        // One closure per shard captures its upgraded `Shard` + cipher; build them ALL
        // first, then await them, so the fan-out overlaps (a spawn-then-await-each loop
        let mut tasks = Vec::with_capacity(self.shards.len());
        for writer in &self.shards {
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            #[cfg(feature = "security")]
            let cipher = writer.cipher.clone();
            tasks.push(move || {
                #[cfg(feature = "security")]
                let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
                #[cfg(not(feature = "security"))]
                let crypto = crate::redb_store::DurableCrypto::none();
                read_all_dumps(&shard, crypto)
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
            // Publish the authoritative watermark onto a projection this function did
            // NOT create.
            //
            // `GraphRegistry::new` pre-creates `__commons__`, so the branch above never
            // runs for it and its `create_graph_with_incarnation` adopt never happens.
            // Recovery then replayed the durable rows onto a projection still serving at
            // version 0 while this dump proves the ledger is already at N, and
            // `mutation_batch::compile::authoritative_graph_version`'s preflight refused
            // EVERY subsequent mutation on the recovered commons graph with
            // "authoritative graph version N does not match the serving projection 0":
            // a restarted node could never write to `__commons__` again, and — because a
            // replicated apply runs the same preflight — could never catch up on it
            // either. `load_catalog_into` has handled this case since it was written
            // (`reconcile_bootstrap_catalog_entry`); the eager path, which is the
            // development profile's recovery path and every harness's, had not.
            //
            // `adopt_materialized_version` is a one-shot 0 -> N transition by design, so
            // the version guard here keeps this to exactly the fresh-projection case and
            // leaves a live projection alone rather than rewinding it. It runs BEFORE the
            // row replay, like the created path, so the replay's `dirty` bookkeeping is
            // identical either way.
            if dump.source_snapshot_version > 0 && core.version() == 0 {
                core.adopt_materialized_version(dump.source_snapshot_version)?;
            }
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
    /// four. Catalog rows are registered through the registry's `DashMap`, but
    /// startup also has to reconcile the synthetic `__commons__` placeholder
    /// seeded by `GraphRegistry::new` with its durable incarnation. Take one
    /// bounded write lock for that reconciliation, then retain a shared lock
    /// for the bulk catalog scan rather than leaving an empty resident core that
    /// would shadow lazy materialization after restart.
    async fn load_catalog_into(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        // Write-new half of the one-time graph_meta migration, before the read
        // below. A store written by a pre-versioned build has rows the current
        // record cannot decode; `read_all_graph_meta` can now READ them via the
        // legacy fallback, but leaving them on disk in the old shape would mean
        // taking that path on every subsequent open. Converting here makes the
        // fallback genuinely one-time (and is a no-op on an already-current
        // store, so it costs one read pass per shard at startup and nothing else).
        let mut upgrade_tasks = Vec::with_capacity(self.shards.len());
        for writer in &self.shards {
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            upgrade_tasks.push(move || crate::redb_store::upgrade_legacy_graph_meta(&shard));
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
        for writer in &self.shards {
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            tasks.push(move || read_all_graph_meta(&shard));
        }
        let rows: Vec<(String, String, GraphType, String)> = join_blocking_in_order(tasks)
            .await?
            .into_iter()
            .flatten()
            .collect();

        let count = rows.len();
        if let Some((_, name, graph_type, incarnation_id)) =
            rows.iter().find(|(_, name, _, _)| name == "__commons__")
        {
            let mut s = state.write().await;
            s.registry.reconcile_bootstrap_catalog_entry(
                name,
                *graph_type,
                None,
                incarnation_id.clone(),
            );
        }
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

    /// Enqueue one writer command for `graph_fname`.
    ///
    /// When a tenant catalog is attached, the routing-quiesce READ guard is held
    /// across both the shard resolve and the send, so an online reshard cannot flip
    /// the route between the two (no lost or misrouted write). With no catalog —
    /// the default — there is no guard and this is the plain EG-026 send.
    async fn enqueue(&self, graph_fname: &str, cmd: Cmd, what: &str) -> Result<(), String> {
        let routing = if self.catalog.is_some() {
            Some(self.routing_epoch.clone().read_owned().await)
        } else {
            None
        };
        let tx = self.shard_for(graph_fname).tx.clone();
        tokio::task::spawn_blocking(move || {
            let _routing = routing;
            tx.send(cmd).map_err(|_| ())
        })
        .await
        .map_err(|error| format!("{what} join error: {error}"))?
        .map_err(|_| "redb writer thread is gone".to_string())
    }

    /// Run one typed MVCC read on Tokio's blocking pool. Redb snapshot reads
    /// are independent of the writer channel, but opening a snapshot and
    /// decoding rows are still synchronous work; keeping that shell here makes
    /// every typed adapter off-reactor without hiding its reader-specific
    /// arguments or return type. When a tenant catalog is attached, retain the
    /// routing read guard from shard resolution through the snapshot so an
    /// online reshard cannot flip and purge the selected shard while this read
    /// is waiting for the blocking pool.
    ///
    /// A private helper shared by several `PersistenceBackend` trait method
    /// implementations below (NOT itself a trait method — `PersistenceBackend`
    /// declares no generic methods, so this stays in `RedbBackend`'s own
    /// inherent impl; Rust method resolution finds it from `self.read_snapshot(...)`
    /// regardless of which impl block it lives in).
    async fn read_snapshot<T, F>(&self, graph_fname: &str, read: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a Shard, crate::redb_store::DurableCrypto<'a>) -> Result<T, String>
            + Send
            + 'static,
    {
        let routing_guard = if self.catalog.is_some() {
            Some(self.routing_epoch.clone().read_owned().await)
        } else {
            None
        };
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let cipher = writer.cipher.clone();
        tokio::task::spawn_blocking(move || {
            let _routing_guard = routing_guard;
            #[cfg(feature = "security")]
            let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
            #[cfg(not(feature = "security"))]
            let crypto = crate::redb_store::DurableCrypto::none();
            read(shard.as_ref(), crypto)
        })
        .await
        .map_err(|error| format!("redb snapshot read join error: {error}"))?
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

    fn supports_native_capacity_leases(&self) -> bool {
        true
    }

    fn supports_native_work_item_submission(&self) -> bool {
        true
    }

    fn supports_cluster_hierarchy_cache(&self) -> bool {
        true
    }

    async fn save_cluster_hierarchy(&self, graph_fname: &str, blob: Vec<u8>) -> Result<(), String> {
        self.cluster_hierarchy.put(graph_fname, blob)
    }

    async fn load_cluster_hierarchy(&self, graph_fname: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.cluster_hierarchy.get(graph_fname))
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
        let writer = self.shard_for(graph_fname);
        let tx = writer.tx.clone();
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let version_graph = graph_fname.to_string();
        let read = move || {
            let (reply, rx) = std::sync::mpsc::sync_channel(1);
            tx.send(Cmd::ReadGraphDump { graph, reply })
                .map_err(|_| "redb writer thread is gone".to_string())?;
            let dump = await_writer_reply(&rx, "authoritative snapshot")??;
            let version = read_mutation_graph_version_record(&shard, &version_graph)?;
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
        self.enqueue(graph_fname, cmd, "commit_mutation_batch")
            .await?;
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
        self.enqueue(graph_fname, cmd, "commit_mutation_batch_state")
            .await?;
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
        self.enqueue(graph_fname, cmd, "commit_mutation_batch_crossmodal")
            .await?;
        rx.await
            .map_err(|_| "redb writer dropped cross-modal MutationBatch completion".to_string())?
    }

    async fn read_mutation_batch(
        &self,
        graph_fname: &str,
        batch_id: &str,
    ) -> Result<Option<MutationBatchRecord>, String> {
        let batch_id = batch_id.to_owned();
        let requested_graph = graph_fname.to_owned();
        self.read_snapshot(graph_fname, move |shard, _crypto| {
            read_mutation_batch_record(shard, &requested_graph, &batch_id)
        })
        .await
    }

    async fn read_mutation_graph_version(&self, graph_fname: &str) -> Result<Option<u64>, String> {
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        read_mutation_graph_version_record(&shard, graph_fname).map(Some)
    }

    async fn read_mutation_outbox(
        &self,
        graph_fname: &str,
        batch_id: &str,
    ) -> Result<Vec<MutationOutboxRecord>, String> {
        let batch_id = batch_id.to_owned();
        let requested_graph = graph_fname.to_owned();
        self.read_snapshot(graph_fname, move |shard, _crypto| {
            read_mutation_outbox_records(shard, &requested_graph, &batch_id)
        })
        .await
    }

    async fn subscribe_mutation_outbox(
        &self,
        graph_fname: &str,
        consumer: &str,
        topic: &str,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MutationOutboxSubscribe {
            graph: graph_fname.to_string(),
            consumer: consumer.to_string(),
            topic: topic.to_string(),
            done,
        };
        self.enqueue(graph_fname, cmd, "subscribe_mutation_outbox")
            .await?;
        rx.await
            .map_err(|_| "redb writer dropped outbox subscription completion".to_string())?
    }

    async fn claim_mutation_outbox(
        &self,
        graph_fname: &str,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MutationOutboxClaim {
            graph: graph_fname.to_string(),
            consumer: consumer.to_string(),
            budget: Box::new(budget.clone()),
            done,
        };
        self.enqueue(graph_fname, cmd, "claim_mutation_outbox")
            .await?;
        let (outcome, updated) = rx
            .await
            .map_err(|_| "redb writer dropped outbox claim completion".to_string())??;
        *budget = updated;
        Ok(outcome)
    }

    async fn ack_mutation_outbox(
        &self,
        graph_fname: &str,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::MutationOutboxAck {
            graph: graph_fname.to_string(),
            lease: Box::new(lease.clone()),
            now_ms,
            done,
        };
        self.enqueue(graph_fname, cmd, "ack_mutation_outbox")
            .await?;
        rx.await
            .map_err(|_| "redb writer dropped outbox ack completion".to_string())?
    }

    /// One consumer's durable projection watermark.
    ///
    /// `projection` and `consumer` were two names for one thing and are now one:
    /// under a single ledger the cursor is keyed `(scope, consumer)`. The caller's
    /// tenant left the key with them — a graph shard's scope is
    /// `(GRAPH_SHARD_TENANT, graph)` (RF-RULING-004 application note 2), so the
    /// graph name IS the isolation here, as it has always been for every other
    /// shard row.
    async fn read_mutation_projection_cursor(
        &self,
        graph_fname: &str,
        consumer: &str,
    ) -> Result<Option<MutationProjectionCursor>, String> {
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let consumer = consumer.to_owned();
        self.read_snapshot(&routing_graph, move |shard, _crypto| {
            shard.outbox_cursor(&graph_fname, &consumer)
        })
        .await
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
        self.enqueue(graph_fname, cmd, "commit_change_envelope")
            .await?;
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
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let envelope_id = envelope_id.to_owned();
        self.read_snapshot(&routing_graph, move |shard, crypto| {
            read_change_envelope_record(shard, &graph_fname, &envelope_id, crypto)
        })
        .await
    }

    async fn read_content_version(
        &self,
        graph_fname: &str,
        tenant: &str,
        object_id: &str,
    ) -> Result<Option<ContentVersion>, String> {
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let tenant = tenant.to_owned();
        let object_id = object_id.to_owned();
        self.read_snapshot(&routing_graph, move |shard, crypto| {
            read_content_version_record(shard, &tenant, &graph_fname, &object_id, crypto)
        })
        .await
    }

    async fn read_change_cursor(
        &self,
        graph_fname: &str,
        tenant: &str,
        source: &str,
        partition: &str,
    ) -> Result<Option<ChangeCursor>, String> {
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let tenant = tenant.to_owned();
        let source = source.to_owned();
        let partition = partition.to_owned();
        self.read_snapshot(&routing_graph, move |shard, crypto| {
            read_change_cursor_record(shard, &tenant, &graph_fname, &source, &partition, crypto)
        })
        .await
    }

    async fn read_resource_reservation(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::ResourceReservationStatusRequest,
    ) -> Result<crate::epistemic_operations::ResourceReservationResult, String> {
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let request = request.clone();
        self.read_snapshot(&routing_graph, move |shard, crypto| {
            read_resource_reservation_record(shard, &graph_fname, &request, crypto)
        })
        .await
    }

    async fn read_resource_reservation_status(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::ResourceReservationStatusRequest,
    ) -> Result<crate::epistemic_operations::ResourceReservationStatusResult, String> {
        let graph_fname = graph_fname.to_owned();
        let routing_graph = graph_fname.clone();
        let request = request.clone();
        self.read_snapshot(&routing_graph, move |shard, crypto| {
            read_resource_reservation_status_record(shard, &graph_fname, &request, crypto)
        })
        .await
    }

    /// Execute the narrow native WorkItem claim-capability mint operation on
    /// the graph's writer shard.  This is crate-private: external callers can
    /// submit only the typed opaque request through dispatch after authz.
    async fn mint_work_item_claim_capability(
        &self,
        graph_fname: &str,
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityMintRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
    ) -> Result<crate::epistemic_operations_ext::WorkItemClaimCapabilityResult, String> {
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
        request: crate::epistemic_operations_ext::WorkItemClaimCapabilityVerifyRequest,
        authority: crate::redb_store::work_item_capability::AuthenticatedAuthority,
    ) -> Result<crate::epistemic_operations_ext::WorkItemClaimCapabilityResult, String> {
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

    async fn commit_capacity_lease(
        &self,
        graph_fname: &str,
        method: Method,
    ) -> Result<Vec<u8>, String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::CommitCapacityLease {
            graph: graph_fname.to_string(),
            method: Box::new(method),
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
        send.map_err(|error| format!("capacity lease commit join error: {error}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped capacity lease completion".to_string())?
    }

    async fn read_capacity_status(
        &self,
        graph_fname: &str,
        request: &eg_types::native_control::CapacityStatusRequest,
    ) -> Result<eg_types::native_control::CapacityStatusResult, String> {
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::capacity_lease::read(&shard, graph_fname, request, crypto)
    }

    /// Exact authenticated native development-lane hold/tombstone read (RMDD-28).
    /// An MVCC snapshot read off the writer shard's shared `Shard`, same
    /// posture as `read_resource_reservation` above -- never routed through the
    /// writer thread channel.
    async fn read_development_lane(
        &self,
        graph_fname: &str,
        request: &crate::epistemic_operations::DevelopmentLaneQueryRequest,
        now_ms: u64,
    ) -> Result<crate::epistemic_operations::DevelopmentLaneQueryResult, String> {
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::development_lane::read_development_lane(
            &shard,
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
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        crate::redb_store::development_lane::read_development_lane_status(
            &shard,
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
        self.enqueue(graph_fname, cmd, "commit_crossmodal").await?;
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
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        read_durable_node_presence(&shard, graph_fname, node_ids)
    }

    fn read_node_blocking(
        &self,
        graph_fname: &str,
        node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        // CONCEPT:EG-KG.storage.snapshot-read-off-writer — SNAPSHOT READ OFF THE WRITER. The read-through point-read
        // (only hit on a RAM miss, CONCEPT:EG-KG.storage.read-through-seam-exercised) now serves the node DIRECTLY from
        // a kernel-issued MVCC snapshot on the TARGET SHARD's shared `Shard`
        // (routed by the SAME EG-026 `shard_for` the writer uses). It NEVER routes
        // through the writer thread's channel and NEVER forces a group-commit, so a
        // read can no longer block on / be serialized behind the durable write path —
        // critical on a Pi (frequent eviction/read-through) and across shards.
        //
        // Consistency: redb is MVCC, so the snapshot sees the LATEST COMMITTED state
        // of this shard. Commit-before-ack (CONCEPT:EG-KG.backend.authoritative-dispatch) guarantees any ACKED
        // write is already committed, so a snapshot opened after that ack sees
        // it. Writes still buffered in the writer's `Pending` are NOT yet acked (no
        // happens-before to any reader), so omitting the old forced commit changes no
        // observable read result. Eviction is durability-gated (a node leaves RAM only
        // after redb confirms it on disk), so an evicted node is always served here.
        let writer = self.shard_for(graph_fname);
        // Upgrade the `Weak` to the writer's shared `Shard` (CONCEPT:EG-KG.storage.snapshot-read-off-writer). `None`
        // only after shutdown dropped the writer's strong Arc — fail fast like the old
        // "writer thread is gone" channel error.
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
        #[cfg(not(feature = "security"))]
        let crypto = crate::redb_store::DurableCrypto::none();
        read_one_node(&shard, graph_fname, node_id, crypto)
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
    /// Read-only against each authoritative shard: uses the SAME shared `Weak<Shard>` handle the
    /// snapshot-read path (`read_node_blocking`) upgrades, so this never opens the
    /// file a SECOND time (redb's exclusive per-process file lock would reject that) —
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
        for writer in &self.shards {
            // A shard whose writer thread already exited (shutdown mid-boot-sequence,
            // never happens in the normal boot path but guarded like every other
            // snapshot-read consumer of this handle) has nothing left to reconcile.
            let Some(shard) = writer.shard.upgrade() else {
                continue;
            };
            // The three series tables are FILE-WIDE, so the control scope is the
            // reader class that owns them — and it is the one scope that exists
            // before any graph is bound.
            let rtx = shard.control_read()?;
            let series_ids = eg_tsdb::store::list_series_in_rtx(&rtx).map_err(|e| e.to_string())?;
            for series_id in series_ids {
                if eg_tsdb::store::SeriesKey::decode(&series_id).is_none() {
                    return Err("durable time-series key is not canonically scoped".to_string());
                }
                let graph_meta = eg_tsdb::store::meta_in_rtx(&rtx, &series_id)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| {
                        format!(
                            "durable time-series '{series_id}' has no meta row in the scan \
                             that named it"
                        )
                    })?;
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
// The Raft log lives in the SAME authoritative shard file, written by the SAME
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for_group(group_id)
            .tx
            .send(Cmd::RaftLogRead {
                group_id,
                lo,
                hi,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "raft_log_read")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for_group(group_id)
            .tx
            .send(Cmd::RaftLogBounds { group_id, reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "raft_log_bounds")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_for_group(group_id)
            .tx
            .send(Cmd::RaftMetaGet {
                group_id,
                key: key.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "raft_meta_get")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::XshardPrepareGet {
                txn_id: txn_id.to_string(),
                group_id,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "xshard_prepare_get")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::XshardScanPrepares { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "xshard_scan_prepares")?
    }

    /// Scan digest-only decision states (no source payloads).
    #[cfg(feature = "raft")]
    pub fn xshard_scan_decisions(&self) -> XshardDecisionScan {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::XshardScanDecisions { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "xshard decision scan")?
    }

    /// Read a txn's durable decision (Some(true)=commit, Some(false)=abort, None=undecided).
    #[cfg(feature = "raft")]
    pub fn xshard_decision_get(&self, txn_id: &str) -> Result<Option<bool>, String> {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::XshardDecisionGet {
                txn_id: txn_id.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "xshard_decision_get")?
    }

    /// Whether this marker is retained for a separate parent receipt.
    #[cfg(feature = "raft")]
    pub fn xshard_decision_retain_get(&self, txn_id: &str) -> Result<bool, String> {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::XshardDecisionRetainGet {
                txn_id: txn_id.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "xshard retain")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::MatViewScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "matview_scan")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::PlanMatViewScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "plan_matview_scan")?
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
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.shard0()
            .tx
            .send(Cmd::MatViewOperatorStateScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        await_writer_reply(&rx, "matview_operator_state_scan")?
    }
}

// ── off-reactor group-commit writer thread ───────────────────────────────

/// How long the writer waits for work before flushing whatever it holds.
///
/// A commit-before-ack write never waits for this (a pending barrier commits the
/// instant the channel drains), so it bounds only how long a NON-acknowledged
/// internal batch sits unflushed. This is the fixed group-commit boundary; it
/// never changes the Immediate durability level.
const GROUP_COMMIT_TICK: Duration = Duration::from_millis(100);

fn run(
    rx: Receiver<Cmd>,
    // Shared kernel-owned `Shard` (CONCEPT:EG-KG.storage.snapshot-read-off-writer): the writer OWNS one clone of
    // the Arc (kept alive for the thread's whole life); the `ShardWriter` holds a
    // `Weak` for off-writer snapshot reads. Rebound to `&Shard` immediately so every
    // commit below reads as the same single-writer loop it has always been.
    shard: Arc<Shard>,
    group_commit: RedbGroupCommitConfig,
    stats: Arc<RedbCommitStats>,
    // Auto-sized early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb), per shard.
    flush_threshold: usize,
    #[cfg(feature = "security")] cipher: Option<crate::crypto::ValueCipher>,
) {
    // Borrow the shared handle for the rest of the loop; the owned Arc above stays
    // alive until `run` returns, so this reference is valid for the whole thread.
    let shard: &Shard = &shard;
    // Build the durable-crypto handle ONCE (borrows the owned cipher for the thread's
    // lifetime). No-op handle when encryption is off / not compiled.
    #[cfg(feature = "security")]
    let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
    #[cfg(not(feature = "security"))]
    let crypto = crate::redb_store::DurableCrypto::none();
    let tick = GROUP_COMMIT_TICK;
    // Pending mutations folded into the NEXT group commit, each with its optional
    // commit-before-ack completion sender (CONCEPT:EG-KG.backend.authoritative-dispatch). After a commit, EVERY
    // sender in the batch is fired with the batch's result — one fsync, N notified.
    let mut pending: Pending = Pending::default();
    // CONCEPT:EG-KG.backend.adaptive-linger-coalesce — record the group-commit batch size (ops-per-fsync), then
    // commit+notify. Only counts a batch that actually carried work; `lingered` marks
    // commits that paid a micro-linger window so the win is measurable.
    //
    // There is no runtime durability argument. `eg_storage`'s
    // `physical::root::WRITE_DURABILITY` is a CONSTANT
    // `redb::Durability::Immediate` on the one `begin_write` every kernel mutation
    // takes, "because a weaker level would let redb roll a committed ledger back on
    // crash — un-consuming an acknowledged replay nonce and re-enabling the double
    // apply". Every commit this loop makes is therefore Immediate. The coalescing
    // is untouched: N ops still fold into ONE Immediate fsync.
    let commit_now = |pending: &mut Pending, lingered: bool| {
        if !pending.is_empty() {
            stats.record(pending.ops.len(), lingered);
        }
        commit_and_notify(shard, pending, crypto);
    };
    loop {
        match rx.recv_timeout(tick) {
            Ok(cmd) => {
                if handle_cmd(cmd, shard, &mut pending, flush_threshold, crypto, &stats) {
                    // shutdown: flush whatever is pending durably, then stop.
                    commit_now(&mut pending, false);
                    break;
                }
                // Drain the rest of the burst so it coalesces into one commit.
                let mut stop = false;
                while let Ok(cmd) = rx.try_recv() {
                    if handle_cmd(cmd, shard, &mut pending, flush_threshold, crypto, &stats) {
                        stop = true;
                        break;
                    }
                }
                if stop {
                    commit_now(&mut pending, false);
                    return;
                }
                // Any awaiting commit-before-ack op in the batch MUST be made durable
                // now — don't leave an awaited write parked until the next tick. With
                // the policy gone this is the ONLY immediacy trigger besides the tick,
                // which is what it always effectively was: `Each` differed from
                // `Interval` only for batches that had no waiter to keep waiting.
                let must_commit_now = pending.has_barrier();
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
                        stats.linger_waiting.store(true, Ordering::Release);
                        #[cfg(test)]
                        if let Some(control) = group_commit.test_control.as_ref() {
                            control.wait_until_released();
                        }
                        let linger_result = rx.recv_timeout(group_commit.linger);
                        stats.linger_waiting.store(false, Ordering::Release);
                        match linger_result {
                            Ok(cmd) => {
                                if handle_cmd(
                                    cmd,
                                    shard,
                                    &mut pending,
                                    flush_threshold,
                                    crypto,
                                    &stats,
                                ) {
                                    commit_now(&mut pending, true);
                                    return;
                                }
                                // Drain everyone who arrived during the linger window.
                                while let Ok(cmd) = rx.try_recv() {
                                    if handle_cmd(
                                        cmd,
                                        shard,
                                        &mut pending,
                                        flush_threshold,
                                        crypto,
                                        &stats,
                                    ) {
                                        commit_now(&mut pending, true);
                                        return;
                                    }
                                }
                            }
                            Err(RecvTimeoutError::Timeout) => {}
                            Err(RecvTimeoutError::Disconnected) => {
                                commit_now(&mut pending, true);
                                break;
                            }
                        }
                    }
                    commit_now(&mut pending, lingered);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // Group-commit boundary: flush pending mutations.
                commit_now(&mut pending, false);
            }
            Err(RecvTimeoutError::Disconnected) => {
                commit_now(&mut pending, false);
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
    shard: &Shard,
    pending: &mut Pending,
    // Auto-sized early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb).
    flush_threshold: usize,
    crypto: crate::redb_store::DurableCrypto<'_>,
    // Group-commit observability (CONCEPT:EG-KG.backend.adaptive-linger-coalesce). Every flush this
    // function performs — the early-flush bound below AND every "flush pending
    // mutations first" side effect of a read/purge/register/etc. command — is a REAL
    // group commit and must be accounted identically to the run-loop's own
    // `commit_now`, or `commit_stats()` silently undercounts (down to 0) any batch
    // that happens to settle on one of these paths instead, even though the ops
    // durably landed and every commit-before-ack waiter still fires correctly. See
    // the local `flush` helper immediately below and GOC-70 rule 1 (account for the
    // op on whichever path it took — don't assert a specific path was taken).
    stats: &RedbCommitStats,
) -> bool {
    // Every `commit_and_notify` call in this function MUST go through this helper so
    // the batch is recorded before it is committed+acked, exactly like `run`'s
    // `commit_now` — same ordering guarantee (record happens-before the oneshot
    // notify on this single writer thread), just reused across every early-flush
    // call site instead of only the run-loop's own drain/linger/tick path.
    let flush = |pending: &mut Pending| {
        if !pending.is_empty() {
            stats.record(pending.ops.len(), false);
        }
        commit_and_notify(shard, pending, crypto);
    };
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
                flush(pending);
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
            flush(pending);
            let res = write_graph_meta(shard, &graph, &name, graph_type);
            let _ = done.send(res);
            false
        }
        Cmd::PurgeGraph { graph, done } => {
            // Flush pending mutations first so we never purge a graph and then
            // re-apply a buffered op for it out of order, then drop ALL of its rows
            // (incl. graph_meta) in one durable transaction.
            flush(pending);
            let _ = done.send(purge_graph_rows(shard, &graph));
            false
        }
        Cmd::ReadGraphDump { graph, reply } => {
            // Flush pending so the rehydrated dump reflects the latest durable state,
            // then range-scan ONE graph's rows (CONCEPT:EG-KG.storage.100m-tenant).
            flush(pending);
            let _ = reply.send(read_graph_dump(shard, &graph, crypto));
            false
        }
        Cmd::ReadGraphDumpPage {
            graph,
            query,
            reply,
        } => {
            // Flush pending first (same consistency contract as ReadGraphDump), then
            // fetch ONE bounded page straight off the durable store (CONCEPT:EG-KG.sharding.paged-lazy-open, L38).
            flush(pending);
            let _ = reply.send(crate::redb_store::read_graph_dump_page(
                shard,
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
            flush(pending);
            let _ = reply.send(super::online_reshard::export_graph_raw(shard, &graph));
            false
        }
        Cmd::ImportGraphRaw { graph, rows, reply } => {
            // CONCEPT:EG-KG.backend.catalog-shard-resolve — flush pending first (consistency), then land the migrated
            // rows verbatim in ONE durable commit (the move's commit-before-ack point).
            flush(pending);
            let _ = reply.send(super::online_reshard::import_graph_raw(
                shard, &graph, &rows,
            ));
            false
        }
        Cmd::ImportGraphDelta {
            graph,
            delta,
            reply,
        } => {
            // CONCEPT:EG-KG.backend.flush-pending-first — flush pending first (consistency), then land ONLY the delta
            // rows (upserts + removals) in ONE durable commit (the under-quiesce write).
            flush(pending);
            let _ = reply.send(super::online_reshard::import_graph_delta(
                shard, &graph, &delta,
            ));
            false
        }
        #[cfg(feature = "security")]
        Cmd::AuditVerify { graph, reply } => {
            // Flush pending so the chain walk includes the latest durable audit
            // entries, then verify the hash chain (CONCEPT:EG-KG.sharding.row-level-security).
            flush(pending);
            let _ = reply.send(crate::redb_store::verify_audit(shard, &graph));
            false
        }
        #[cfg(all(test, feature = "security"))]
        Cmd::TestTamperAudit { graph, seq, reply } => {
            flush(pending);
            let res = in_graph_write(shard, &graph, "test_tamper_audit", |write| {
                let mut audit = write
                    .graph(&graph)?
                    .open_scoped_table(crate::redb_store::AUDIT)?;
                let mut mutated = audit
                    .get((graph.as_str(), seq))?
                    .ok_or_else(|| "no such audit entry".to_string())?
                    .value()
                    .to_vec();
                let last = mutated
                    .len()
                    .checked_sub(1)
                    .ok_or_else(|| "audit entry is empty".to_string())?;
                mutated[last] ^= 0xFF;
                audit.insert((graph.as_str(), seq), mutated.as_slice())
            });
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
            flush(pending);
            let res = crate::redb_store::provenance_anchor_commit(
                shard,
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
            flush(pending);
            let res =
                crate::redb_store::prove_inclusion(shard, &graph, &node_id, anchor_seq, crypto);
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
            flush(pending);
            let op_id = shard_write_attempt_id("commit_crossmodal");
            let res = commit_crossmodal(
                shard,
                &graph,
                crate::redb_store::CrossModalStaged {
                    methods: &methods,
                    vectors: &vectors,
                    blob_refs: &blob_refs,
                    measurements: &measurements,
                },
                &op_id,
                crate::server::txn::now_ms(),
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
            flush(pending);
            let res = commit_mutation_batch_crossmodal(
                shard,
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
            flush(pending);
            let res = if let Some(state) = authoritative_state_msgpack.as_deref() {
                commit_mutation_batch_state(
                    shard,
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
                    shard,
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
            flush(pending);
            let res = in_graph_write(shard, &graph, "work_item_capability_mint", |write| {
                crate::redb_store::work_item_capability::mint_claim_capability(
                    write, &graph, &request, &authority, crypto,
                )
            });
            let _ = done.send(res);
            false
        }
        Cmd::VerifyWorkItemClaimCapability {
            graph,
            request,
            authority,
            done,
        } => {
            flush(pending);
            let res = in_graph_write(shard, &graph, "work_item_capability_verify", |write| {
                crate::redb_store::work_item_capability::verify_claim_capability(
                    write, &graph, &request, &authority, crypto,
                )
            });
            let _ = done.send(res);
            false
        }
        Cmd::CommitDevelopmentLane {
            graph,
            method,
            now_ms,
            done,
        } => {
            flush(pending);
            let res = crate::redb_store::development_lane::commit_development_lane(
                shard, &graph, &method, now_ms, crypto,
            );
            let _ = done.send(res);
            false
        }
        Cmd::CommitCapacityLease {
            graph,
            method,
            done,
        } => {
            flush(pending);
            let res = crate::redb_store::capacity_lease::commit(
                shard,
                &graph,
                &method,
                crypto,
                #[cfg(feature = "security")]
                &mut pending.audit_tail,
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
            flush(pending);
            let res = commit_change_envelope(
                shard,
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
            flush(pending);
            let res = commit_change_envelopes(
                shard,
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
        Cmd::MutationOutboxSubscribe {
            graph,
            consumer,
            topic,
            done,
        } => {
            // The subscription is an immediate ledger write. Flush pending
            // graph mutations first so a subsequent claim sees one committed
            // ordering, then let the shard/kernel enforce same-topic
            // idempotence and cross-topic refusal atomically.
            flush(pending);
            let result = shard.outbox_subscribe(&graph, &consumer, &topic);
            let _ = done.send(result);
            false
        }
        Cmd::MutationOutboxClaim {
            graph,
            consumer,
            budget,
            done,
        } => {
            // A claim observes every prior batch/outbox write and installs all
            // returned leases atomically before any worker is notified. The budget is
            // the value a sweep carries between scopes. The command owns a clone
            // while the writer executes, then returns the updated state with the
            // complete claim outcome so the caller can continue the same sweep
            // without losing an explicit deferral reason.
            flush(pending);
            let mut budget = *budget;
            let result = shard
                .outbox_claim(&graph, &consumer, &mut budget)
                .map(|outcome| (outcome, budget));
            let _ = done.send(result);
            false
        }
        Cmd::MutationOutboxAck {
            graph,
            lease,
            now_ms,
            done,
        } => {
            // One call, not two: the kernel marks the lease delivered and advances
            // this consumer's projection cursor in the SAME transaction, so the
            // crash window between "delivered" and "watermark moved" that a separate
            // cursor write left open is not representable any more.
            flush(pending);
            let result = shard.outbox_ack(&graph, &lease, now_ms);
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
            flush(pending);
            let _ = reply.send(read_raft_log_range(shard, group_id, lo, hi, crypto));
            false
        }
        Cmd::RaftLogDeleteFrom {
            group_id,
            from,
            done,
        } => {
            flush(pending);
            let _ = done.send(delete_raft_log_from(shard, group_id, from));
            false
        }
        Cmd::RaftLogPurgeUpto {
            group_id,
            upto,
            done,
        } => {
            flush(pending);
            let _ = done.send(purge_raft_log_upto(shard, group_id, upto));
            false
        }
        Cmd::RaftLogBounds { group_id, reply } => {
            flush(pending);
            let _ = reply.send(raft_log_bounds(shard, group_id));
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
            flush(pending);
            let _ = done.send(put_raft_meta(shard, group_id, &key, &val));
            false
        }
        Cmd::RaftMetaGet {
            group_id,
            key,
            reply,
        } => {
            flush(pending);
            let _ = reply.send(get_raft_meta(shard, group_id, &key));
            false
        }
        Cmd::XshardPreparePut {
            txn_id,
            group_id,
            slice,
            done,
        } => {
            flush(pending);
            let _ = done.send(put_xshard_prepare(shard, &txn_id, group_id, &slice, crypto));
            false
        }
        Cmd::XshardPrepareGet {
            txn_id,
            group_id,
            reply,
        } => {
            flush(pending);
            let _ = reply.send(get_xshard_prepare(shard, &txn_id, group_id, crypto));
            false
        }
        Cmd::XshardDecisionPut {
            txn_id,
            commit,
            retain_for_parent,
            done,
        } => {
            flush(pending);
            let _ = done.send(put_xshard_decision(
                shard,
                &txn_id,
                commit,
                retain_for_parent,
            ));
            false
        }
        Cmd::XshardRecoverablePendingPut { txn_id, done } => {
            flush(pending);
            let _ = done.send(put_xshard_recoverable_pending(shard, &txn_id));
            false
        }
        Cmd::XshardPrepareClear {
            txn_id,
            group_id,
            done,
        } => {
            flush(pending);
            let _ = done.send(clear_xshard_prepare(shard, &txn_id, group_id));
            false
        }
        Cmd::XshardDecisionClear { txn_id, done } => {
            flush(pending);
            let _ = done.send(clear_xshard_decision(shard, &txn_id));
            false
        }
        Cmd::XshardScanPrepares { reply } => {
            flush(pending);
            let _ = reply.send(scan_xshard_prepares(shard, crypto));
            false
        }
        Cmd::XshardScanDecisions { reply } => {
            flush(pending);
            let _ = reply.send(scan_xshard_decisions(shard));
            false
        }
        Cmd::XshardDecisionGet { txn_id, reply } => {
            flush(pending);
            let _ = reply.send(get_xshard_decision(shard, &txn_id));
            false
        }
        Cmd::XshardDecisionRetainGet { txn_id, reply } => {
            flush(pending);
            let _ = reply.send(get_xshard_decision_retain(shard, &txn_id));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewPut { name, blob, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::put_matview(shard, &name, &blob));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewScan { reply } => {
            flush(pending);
            let _ = reply.send(crate::redb_store::scan_matviews(shard));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewPut { name, blob, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::put_plan_matview(shard, &name, &blob));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewDelete { name, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::delete_plan_matview(shard, &name));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::PlanMatViewScan { reply } => {
            flush(pending);
            let _ = reply.send(crate::redb_store::scan_plan_matviews(shard));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStatePut { name, blob, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::put_matview_operator_state(
                shard, &name, &blob,
            ));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStateDelete { name, done } => {
            flush(pending);
            let _ = done.send(crate::redb_store::delete_matview_operator_state(
                shard, &name,
            ));
            false
        }
        #[cfg(feature = "matview")]
        Cmd::MatViewOperatorStateScan { reply } => {
            flush(pending);
            let _ = reply.send(crate::redb_store::scan_matview_operator_state(shard));
            false
        }
    }
}

/// Commit all buffered mutations as ONE admitted scope group — the control member
/// plus one member per touched graph — then fire EVERY commit-before-ack waiter for
/// the ops in this batch with the batch's result (CONCEPT:EG-KG.backend.authoritative-dispatch).
/// Coalescing is preserved: N awaiting writers ride one commit / one fsync and are
/// all notified after it lands, and the Raft entries in the same `Pending` ride that
/// same fsync on the control member. A waiter is only signalled `Ok` once its op is
/// provably on disk.
///
/// There is no durability argument: every commit is `eg_storage`'s constant
/// `WRITE_DURABILITY` (`Durability::Immediate`) — see `run`'s `commit_now`.
///
/// A burst wider than one group does NOT fail and is not split by luck:
/// `commit_ops` sorts the drain by graph into a `BTreeMap`, splits that key list
/// with `redb_store::shard::chunk_graphs` (`MAX_SHARD_GROUP_GRAPHS` = 1023, the
/// kernel's member budget less the always-present control member) and commits
/// each chunk as one group / one fsync, deterministically and in order, so two
/// replicas chunk the same burst the same way. Handing it the WHOLE drain in ONE
/// call is what lets it see the full burst — this function must not pre-split.
fn commit_and_notify(
    shard: &Shard,
    pending: &mut Pending,
    crypto: crate::redb_store::DurableCrypto<'_>,
) {
    if pending.is_empty() {
        return;
    }
    let res = commit_ops(
        shard,
        &mut pending.ops,
        &mut pending.raft_log_ops,
        &shard_write_attempt_id("shard_drain"),
        crate::server::txn::now_ms(),
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

// ── One-shot writer-thread commits ───────────────────────────────────────
//
// The writer's own bookkeeping — a Raft truncation, a meta pointer, a minted
// capability, a planted test row — carries no caller operation identity, so under
// RF-RULING-005 it is the ledgered MAINTENANCE class: durable and version-bumping
// like any other mutation, but outside operation-replay conflict semantics. Both
// helpers ABORT the group on failure rather than dropping it: a dropped
// unfinished admission poisons the shared transaction; an aborted one decides
// nothing.

/// Run `rows` in ONE control-only maintenance group: the 12 file-wide tables.
fn in_control_write<T>(
    shard: &Shard,
    label: &str,
    rows: impl FnOnce(&ShardWrite<'_>) -> Result<T, String>,
) -> Result<T, String> {
    in_write(shard, &[], label, rows)
}

/// Run `rows` in ONE maintenance group carrying `graph` plus the control member.
fn in_graph_write<T>(
    shard: &Shard,
    graph: &str,
    label: &str,
    rows: impl FnOnce(&ShardWrite<'_>) -> Result<T, String>,
) -> Result<T, String> {
    let members = shard.graph_members(&[graph])?;
    in_write(shard, &members, label, rows)
}

type ShardMember = (
    String,
    Arc<eg_storage::OwnedStoreHandle<eg_storage::GraphShardOwner>>,
);

fn in_write<T>(
    shard: &Shard,
    members: &[ShardMember],
    label: &str,
    rows: impl FnOnce(&ShardWrite<'_>) -> Result<T, String>,
) -> Result<T, String> {
    let op_id = shard_write_attempt_id(label);
    let (group, batches) = shard.admit_maintenance(members, &op_id)?;
    let applied = (|| {
        let write = ShardWrite::open(shard, &group, members, &batches)?;
        let value = rows(&write)?;
        write.finish()?;
        Ok(value)
    })();
    match applied {
        Ok(value) => {
            shard.commit_drain(group, &batches, crate::server::txn::now_ms())?;
            Ok(value)
        }
        Err(error) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

// ── Raft log/meta helpers (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — run on the writer thread ───────
//
// `raft_log` and `raft_meta` are FILE-WIDE rows: their keys lead with the Raft
// group id, not with a graph name, so they belong to the shard file's own control
// scope and are reached through `write.control()` / `read.open_owner_table`.

/// Read a `[lo, hi]` inclusive log range for one group, in index order.
fn read_raft_log_range(
    shard: &Shard,
    gid: u64,
    lo: u64,
    hi: u64,
    crypto: crate::redb_store::DurableCrypto<'_>,
) -> Result<Vec<Vec<u8>>, String> {
    const MAX_RAFT_LOG_READ_ENTRIES: usize = 100_000;
    const MAX_RAFT_LOG_READ_BYTES: usize = 1024 * 1024 * 1024;
    let rtx = shard.control_read()?;
    let t = rtx.open_owner_table(RAFT_LOG)?;
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
fn delete_raft_log_from(shard: &Shard, gid: u64, from: u64) -> Result<(), String> {
    in_control_write(shard, "raft_log_delete_from", |write| {
        let mut t = write.control().open_table(RAFT_LOG)?;
        let keys: Vec<u64> = t
            .range((gid, from)..=(gid, u64::MAX))
            .map_err(|e| e.to_string())?
            .filter_map(|kv| kv.ok().map(|(k, _)| k.value().1))
            .collect();
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
        Ok(())
    })
}

/// Delete entries with index <= `upto` for one group (purge/compaction).
fn purge_raft_log_upto(shard: &Shard, gid: u64, upto: u64) -> Result<(), String> {
    in_control_write(shard, "raft_log_purge_upto", |write| {
        let mut t = write.control().open_table(RAFT_LOG)?;
        let keys: Vec<u64> = t
            .range((gid, 0)..=(gid, upto))
            .map_err(|e| e.to_string())?
            .filter_map(|kv| kv.ok().map(|(k, _)| k.value().1))
            .collect();
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
        Ok(())
    })
}

/// (first, last) present log index for one group.
fn raft_log_bounds(shard: &Shard, gid: u64) -> LogBoundsResult {
    let rtx = shard.control_read()?;
    let t = rtx.open_owner_table(RAFT_LOG)?;
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
fn put_raft_meta(shard: &Shard, gid: u64, key: &str, val: &[u8]) -> Result<(), String> {
    in_control_write(shard, "raft_meta_put", |write| {
        let mut t = write.control().open_table(RAFT_META)?;
        t.insert((gid, key), val).map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Read one Raft metadata key for a group.
fn get_raft_meta(shard: &Shard, gid: u64, key: &str) -> Result<Option<Vec<u8>>, String> {
    let rtx = shard.control_read()?;
    let t = rtx.open_owner_table(RAFT_META)?;
    Ok(t.get((gid, key))
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec()))
}

/// Insert one raw row directly into `table` for a shard file, bypassing every
/// request/validation path (WD5-BUG-04: the RESOURCE_*/development_lane_*/
/// capacity_lease_* subsystems require heavy native-operation preconditions — a
/// registered host, a matching WorkItem node, a pre-existing reservation — that a
/// raw seed sidesteps, exactly like `shard_migrate.rs`'s own `seed_raw_two_tuple_row`
/// test helper). Generic over the table's key/value shape: every raw-seed idiom
/// this lane's coverage needs — `(graph, second_key) -> blob`, `(graph, seq) ->
/// blob`, `graph -> u64`, `name -> blob` — is the same open/begin_write/
/// open_table/insert/commit sequence, differing only in what `K`/`V` happen to
/// be. `backup.rs`'s own raw-seed coverage reaches the identical shard files
/// through this same helper rather than carrying a second copy. Test-only:
/// production code reaches these tables through the shard's admitted write.
#[cfg(test)]
pub(crate) fn seed_raw_row<'k, 'v, K, V>(
    shard_path: &std::path::Path,
    table: TableDefinition<K, V>,
    key: K::SelfType<'k>,
    value: V::SelfType<'v>,
) where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let db = redb::Database::open(shard_path).expect("open shard for raw seed");
    let wtx = db.begin_write().expect("begin write");
    {
        let mut t = wtx.open_table(table).expect("open table for raw seed");
        t.insert(key, value).expect("insert raw seed row");
    }
    wtx.commit().expect("commit raw seed row");
}

#[cfg(test)]
mod tests {
    use super::*;
    // A raw `redb::Database` survives HERE and nowhere else: these fixtures plant
    // rows the production paths deliberately refuse to write, so they must reach
    // the file underneath the kernel's admission. Each opens the file only after
    // the backend that owns it shut down (redb's lock admits one holder).
    use redb::{Database, ReadableDatabase};

    use crate::mutation_batch::{
        DurabilityDomain, IncarnationId, LogicalName, MutationOperation, MutationOutboxIntent,
        MutationScopeIdentity, MutationSurface, ScopeTenantId, VersionExpectation,
        MUTATION_ACTOR_HEADER, MUTATION_BATCH_VERSION,
    };
    use crate::protocol::Request;
    use crate::server::auth::{
        build_shared_test_request, dispatch_test_on_heap as dispatch_on_heap,
    };
    #[cfg(feature = "tsdb")]
    use sha2::Digest;

    const TEST_AGENT: &str = "unit-test-agent";

    fn current_request(secret: &str, id: u64, graph: &str, method: Method) -> Request {
        build_shared_test_request(secret, id, graph, TEST_AGENT, method)
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

    fn caller_outbox_batch(graph: &str, batch_id: &str) -> MutationBatch {
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("caller-tenant").unwrap(),
            LogicalName::new(graph).unwrap(),
            IncarnationId::new("incarnation:test:redb-backend").unwrap(),
        );
        let actor = format!("principal:sha256:{}", "b".repeat(64));
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            envelope: crate::redb_store::fixture_operation_envelope(
                &identity, &actor, 17, batch_id,
            ),
            identity,
            placement_epoch: 1,
            version_expectation: VersionExpectation::Graph(0),
            fencing_token: Some(1),
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "backend-outbox-node".to_string(),
                    properties_msgpack: props(serde_json::json!({"source": "backend"})),
                },
            }],
            outbox: vec![MutationOutboxIntent {
                topic: "projection.backend".to_string(),
                key: batch_id.to_string(),
                payload: rmp_serde::to_vec_named(&serde_json::json!({"event": "backend"})).unwrap(),
                headers: std::collections::BTreeMap::from([(
                    MUTATION_ACTOR_HEADER.to_string(),
                    actor,
                )]),
            }],
            created_at_ms: 10,
        };
        let schema_digest = batch
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .method_schema_digest;
        batch.reseal_envelope(schema_digest).unwrap();
        batch
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redb_backend_outbox_subscription_is_durable_and_topic_bound() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-outbox-subscribe-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let dir_s = dir.to_string_lossy().to_string();
        let graph = "backend-outbox-graph";
        let consumer = "backend-projection";
        let topic = "projection.backend";
        let batch = caller_outbox_batch(graph, "backend-outbox-batch");

        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open redb backend");
        PersistenceBackend::commit_mutation_batch(&backend, graph, &batch, None, 11)
            .await
            .expect("caller batch commits through the backend");
        PersistenceBackend::subscribe_mutation_outbox(&backend, graph, consumer, topic)
            .await
            .expect("first subscription commits");
        PersistenceBackend::subscribe_mutation_outbox(&backend, graph, consumer, topic)
            .await
            .expect("same-topic subscription is idempotent");
        let conflict = PersistenceBackend::subscribe_mutation_outbox(
            &backend,
            graph,
            consumer,
            "projection.other",
        )
        .await
        .unwrap_err();
        assert!(conflict.contains("already subscribed to another topic"));
        backend.shutdown();
        drop(backend);

        let backend = RedbBackend::open(dir_s.clone(), 64).expect("reopen redb backend");
        PersistenceBackend::subscribe_mutation_outbox(&backend, graph, consumer, topic)
            .await
            .expect("subscription survives restart");
        let mut budget = OutboxClaimBudget::new(8, 100, 100).unwrap();
        let outcome =
            PersistenceBackend::claim_mutation_outbox(&backend, graph, consumer, &mut budget)
                .await
                .expect("subscribed backend claim succeeds");
        assert!(!outcome.claims.is_empty());
        backend.shutdown();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// An implicit last-owner drop must synchronously join every shard writer.
    /// Reopening the same directory repeatedly catches the redb lock leak, while
    /// the per-open timeout catches a detached writer join without changing the
    /// process limit. The isolated xshard lifecycle child covers process-wide FD
    /// accumulation around an accepted Raft connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn implicit_drop_releases_writer_before_reopen() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-implicit-drop-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let dir_s = dir.to_string_lossy().to_string();

        for _ in 0..8 {
            let open_dir = dir_s.clone();
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::task::spawn_blocking(move || {
                    let _backend = RedbBackend::open(open_dir, 64)
                        .expect("implicit drop must release the writer before reopen");
                }),
            )
            .await
            .expect("implicit writer drop must not hang")
            .expect("implicit writer-drop worker must join");
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Row count of ONE table in a shard file opened fresh, offline (WD5-BUG-04 —
    /// mirrors `shard_migrate.rs`'s own test helper of the same name).
    fn table_row_count<K, V>(path: &std::path::Path, def: redb::TableDefinition<K, V>) -> usize
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let db = Database::open(path).expect("open shard for inspection");
        let rtx = db.begin_read().expect("begin read");
        match rtx.open_table(def) {
            Ok(t) => t.iter().expect("iterate table").count(),
            Err(_) => 0,
        }
    }

    /// Seed ONE valid, purge-safe `RESOURCE_RESERVATIONS` row (plus its matching
    /// `RESOURCE_RESERVATION_TENANT_INDEX` entry).
    ///
    /// WD5-BUG-05: unlike backup/restore and `migrate_shards` (pure byte copies),
    /// online reshard's PurgeGraph step (`purge_graph_rows` -> `clear_resource_rows`
    /// -> `check_resource_reservations_active`/`check_resource_tenant_index_consistency`)
    /// is PRE-EXISTING lifecycle-authority validation, unrelated to this lane's
    /// routing fix, that decodes every reservation row as a real
    /// `DurableResourceReservation` and requires a matching tenant-index entry
    /// before it will clear the source. A raw junk blob (`seed_raw_row`)
    /// fails that decode with "durable value is invalid" — not a fix defect, a
    /// seed-strategy mismatch. This builds a real, terminal/zero-held record (so
    /// `resource_reservation_row_is_active` is false) that the purge accepts.
    fn seed_valid_resource_reservation(
        shard_path: &std::path::Path,
        graph: &str,
        reservation_id: &str,
        tenant: &str,
    ) {
        use crate::epistemic_operations::{
            ResourceCapacitySnapshot, ResourceRequirement, ResourceReservationRecord,
            ResourceReservationRecordState, ResourceReservationRecordTargetKind,
            ResourceTargetSnapshot, ResourceTargetSnapshotKind,
        };

        // Field-for-field mirror of the private `redb_store::DurableResourceReservation`
        // wrapper (msgpack is structural, not nominal, so a same-shaped local type
        // encodes bytes the real type decodes) — see that struct's doc comment.
        #[derive(serde::Serialize)]
        struct DurableResourceReservationShadow {
            record: ResourceReservationRecord,
            held_cpu_weight: u64,
            held_memory_mib: u64,
            held_disk_mib: u64,
            held_process_slots: u64,
            fairness_debt: u64,
        }

        let record = ResourceReservationRecord {
            reservation_id: reservation_id.to_string(),
            tenant_ref: tenant.to_string(),
            owner_id: "owner:cx054".into(),
            work_item_id: "work:cx054".into(),
            fence: "fence:cx054".into(),
            attempt: 1,
            lease_epoch: 1,
            fencing_token: 1,
            input_fingerprint: "fingerprint:cx054".into(),
            host_ref: "host:cx054".into(),
            profile_name: "default".into(),
            profile_version: "1".into(),
            requirement: ResourceRequirement {
                cpu_weight: 0,
                memory_mib: 0,
                disk_mib: 0,
                process_slots: 0,
            },
            capacity_snapshot: ResourceCapacitySnapshot {
                cpu_weight: 0,
                memory_mib: 0,
                disk_mib: 0,
                process_slots: 0,
                host_revision: 0,
            },
            selected_target: ResourceTargetSnapshot {
                kind: ResourceTargetSnapshotKind::Local,
                alias: None,
                capability_labels: Vec::new(),
            },
            target_kind: ResourceReservationRecordTargetKind::Local,
            target_alias: None,
            repository_id: "repository:cx054".into(),
            branch: "branch:cx054".into(),
            concurrency_key: "concurrency:cx054".into(),
            concurrency_limit: None,
            repository_exclusive: false,
            branch_exclusive: false,
            required_labels: Vec::new(),
            anti_affinity: Vec::new(),
            fairness_group: "fairness:cx054".into(),
            fairness_cost: 0,
            disk_low_watermark_mib: None,
            disk_high_watermark_mib: None,
            disk_policy_key: "policy:cx054".into(),
            reserved_at_ms: 0,
            expires_at_ms: 0,
            expected_host_revision: None,
            expected_lifecycle_revision: None,
            // Terminal + zero held resources: `resource_reservation_row_is_active`
            // requires exactly this to let PurgeGraph clear the row.
            state: ResourceReservationRecordState::Released,
            revision: 1,
            lifecycle_revision: 1,
            tombstone: true,
        };
        let durable = DurableResourceReservationShadow {
            record,
            held_cpu_weight: 0,
            held_memory_mib: 0,
            held_disk_mib: 0,
            held_process_slots: 0,
            fairness_debt: 0,
        };
        let bytes = rmp_serde::to_vec_named(&durable).expect("encode reservation");

        let db = Database::open(shard_path).expect("open shard for raw seed");
        let wtx = db.begin_write().expect("begin write");
        {
            let mut reservations = wtx
                .open_table(crate::redb_store::RESOURCE_RESERVATIONS)
                .expect("open reservations table for raw seed");
            reservations
                .insert((graph, reservation_id), bytes.as_slice())
                .expect("insert reservation row");
            let mut tenant_index = wtx
                .open_table(crate::redb_store::RESOURCE_RESERVATION_TENANT_INDEX)
                .expect("open tenant index table for raw seed");
            tenant_index
                .insert((graph, tenant, reservation_id), reservation_id)
                .expect("insert tenant index row");
        }
        wtx.commit().expect("commit reservation seed");
    }

    /// Seed ONE valid, purge-safe `development_lane::HOLDS` row.
    ///
    /// WD5-BUG-05: `purge_graph_rows` -> `development_lane::clear_native_graph_rows_in_wtx`
    /// -> `drain_lane_holds` is the SAME pre-existing (not this lane's) lifecycle
    /// guard as the reservation case above: it decodes every hold as a real
    /// `DurableLaneHold` and fails closed unless the hold is drained (terminal
    /// state, no active charge, zero retained bytes). Field values below are the
    /// exact known-good shape `development_lane.rs`'s own `mod tests::hold()`
    /// helper uses (same `hold_id`/`input_fingerprint`/`base_sha` formats), just
    /// with `state`/`active_count_charged`/`retained_disk_bytes` set drained.
    fn seed_valid_development_lane_hold(
        shard_path: &std::path::Path,
        graph: &str,
        hold_id: &str,
        tenant: &str,
    ) {
        use crate::epistemic_operations::{
            DevelopmentLaneHold, DevelopmentLaneHoldHostTargetKind,
            DevelopmentLaneHoldSchemaVersion, DevelopmentLaneHoldState, DevelopmentLaneQuotaCharge,
            DevelopmentLaneQuotaChargeSchemaVersion,
        };

        // Field-for-field mirror of the private `development_lane::DurableLaneHold`
        // wrapper — see `seed_valid_resource_reservation`'s doc comment for why a
        // same-shaped local type is sufficient (msgpack is structural).
        #[derive(serde::Serialize)]
        struct DurableLaneHoldShadow {
            hold: DevelopmentLaneHold,
            observation_revision: u64,
            last_observed_at_ms: Option<u64>,
            terminal_state: Option<String>,
            terminal_expected_hold_revision: Option<u64>,
            cleanup_removal_proof_ref: Option<String>,
            cleanup_expected_hold_revision: Option<u64>,
            terminal_source_attempt: Option<u64>,
            terminal_source_lease_epoch: Option<u64>,
            terminal_source_fencing_token: Option<u64>,
            terminal_source_work_item_fence: Option<String>,
            resource_reservation_id: String,
            ttl_ms: u64,
        }

        let charge = DevelopmentLaneQuotaCharge {
            schema_version: DevelopmentLaneQuotaChargeSchemaVersion::V1,
            tenant_count: 0,
            owner_count: 0,
            session_count: 0,
            workspace_count: 0,
            repository_count: 0,
            host_count: 0,
            global_count: 0,
            tenant_predicted_disk_bytes: 0,
            owner_predicted_disk_bytes: 0,
            session_predicted_disk_bytes: 0,
            workspace_predicted_disk_bytes: 0,
            repository_predicted_disk_bytes: 0,
            host_predicted_disk_bytes: 0,
            global_predicted_disk_bytes: 0,
            tenant_observed_disk_bytes: 0,
            owner_observed_disk_bytes: 0,
            session_observed_disk_bytes: 0,
            workspace_observed_disk_bytes: 0,
            repository_observed_disk_bytes: 0,
            host_observed_disk_bytes: 0,
            global_observed_disk_bytes: 0,
            tenant_retained_disk_bytes: 0,
            owner_retained_disk_bytes: 0,
            session_retained_disk_bytes: 0,
            workspace_retained_disk_bytes: 0,
            repository_retained_disk_bytes: 0,
            host_retained_disk_bytes: 0,
            global_retained_disk_bytes: 0,
            revision: 0,
            policy_revision: 1,
        };

        let hold = DevelopmentLaneHold {
            schema_version: DevelopmentLaneHoldSchemaVersion::V1,
            hold_id: format!("v1:{}", "a".repeat(64)),
            lane_id: "lane:cx054".into(),
            tenant_ref: tenant.to_string(),
            request_id: "request:cx054".into(),
            work_item_id: "work:cx054".into(),
            owner_id: "owner:cx054".into(),
            session_id: "session:cx054".into(),
            fairness_group: "fairness:cx054".into(),
            workspace_ref: "workspace:cx054".into(),
            repository_id: "repository:cx054".into(),
            base_ref: "refs/heads/main".into(),
            base_sha: "a".repeat(40),
            branch: "branch:cx054".into(),
            worktree_locator: "lanes/cx054".into(),
            host_target_kind: DevelopmentLaneHoldHostTargetKind::Local,
            host_target_alias: None,
            host_ref: "host:cx054".into(),
            quota_policy_name: "default".into(),
            quota_policy_version: "1".into(),
            input_fingerprint: format!("v1:{}", "b".repeat(64)),
            predicted_disk_bytes: 0,
            observed_disk_bytes: 0,
            // Drained: zero retained bytes, no active charge, a terminal state
            // outside `drain_lane_holds`'s blocking set — exactly what lets
            // PurgeGraph clear the row.
            retained_disk_bytes: 0,
            active_count_charged: false,
            quota_charge: charge,
            state: DevelopmentLaneHoldState::Aborted,
            attempt: 1,
            lease_epoch: 1,
            fencing_token: 1,
            work_item_fence: "fence:cx054".into(),
            hold_revision: 1,
            lifecycle_revision: 1,
            allocation_revision: 1,
            cleanup_revision: 0,
            expires_at_ms: 10_000,
            last_renewed_at_ms: 1_000,
            cleanup_work_item_id: None,
            cleanup_work_item_fence: None,
            cleanup_attempt: None,
            cleanup_lease_epoch: None,
            cleanup_fencing_token: None,
            tombstone: true,
        };

        let durable = DurableLaneHoldShadow {
            hold,
            observation_revision: 0,
            last_observed_at_ms: None,
            terminal_state: None,
            terminal_expected_hold_revision: None,
            cleanup_removal_proof_ref: None,
            cleanup_expected_hold_revision: None,
            terminal_source_attempt: None,
            terminal_source_lease_epoch: None,
            terminal_source_fencing_token: None,
            terminal_source_work_item_fence: None,
            resource_reservation_id: "reservation:cx054".into(),
            ttl_ms: 1_000,
        };
        let bytes = rmp_serde::to_vec_named(&durable).expect("encode lane hold");

        let db = Database::open(shard_path).expect("open shard for raw seed");
        let wtx = db.begin_write().expect("begin write");
        {
            let mut holds = wtx
                .open_table(crate::redb_store::development_lane::HOLDS)
                .expect("open holds table for raw seed");
            holds
                .insert((graph, hold_id), bytes.as_slice())
                .expect("insert hold row");
        }
        wtx.commit().expect("commit hold seed");
    }

    /// A minimal `ServerState` (no persistence backend stored on it — the test
    /// drives the backend directly) with a persist dir set.
    fn new_state(persist_dir: Option<String>) -> Arc<RwLock<ServerState>> {
        let mut state = ServerState::new_for_test("test", ServerState::test_isolation(TEST_AGENT));
        state.persist_dir = persist_dir;
        Arc::new(RwLock::new(state))
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
        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open redb backend");
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
        let backend2 = RedbBackend::open(dir_s.clone(), 64).expect("reopen redb backend");
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

    /// GOC-16/NE-028: save/restore-on-drop for the encryption env vars, held under the SAME
    /// crate-wide [`crate::crypto::acquire_test_env_lock`] every other encryption-key
    /// test in this file and in `crypto.rs` contends on. Unlike
    /// `txn_commit_persists_to_redb`'s `std::sync::Once` (which only ever SETS
    /// `ENCRYPTION_KEY_ENV`, never unsets it — safe because no other test depends on
    /// it being absent), these tests specifically need the key ABSENT at least once,
    /// so a one-directional Once cannot be reused here; this restores the exact prior
    /// value (present or absent) on drop instead.
    #[cfg(feature = "security")]
    struct EncryptionRequiredEnvGuard {
        prev_key: Option<String>,
        prev_key_id: Option<String>,
        prev_key_version: Option<String>,
        prev_required: Option<String>,
    }

    #[cfg(feature = "security")]
    impl EncryptionRequiredEnvGuard {
        fn set(key: Option<&str>, required_mode: &str) -> Self {
            let prev_key = std::env::var(crate::crypto::ENCRYPTION_KEY_ENV).ok();
            let prev_key_id = std::env::var(crate::crypto::ENCRYPTION_KEY_ID_ENV).ok();
            let prev_key_version = std::env::var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV).ok();
            let prev_required = std::env::var(crate::crypto::ENCRYPTION_REQUIRED_ENV).ok();
            match key {
                Some(k) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_ENV, k),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ENV),
            }
            std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ID_ENV);
            std::env::remove_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV);
            std::env::set_var(crate::crypto::ENCRYPTION_REQUIRED_ENV, required_mode);
            Self {
                prev_key,
                prev_key_id,
                prev_key_version,
                prev_required,
            }
        }

        fn set_with_ref(key: &str, key_id: &str, key_version: &str) -> Self {
            let guard = Self::set(Some(key), "warn");
            std::env::set_var(crate::crypto::ENCRYPTION_KEY_ID_ENV, key_id);
            std::env::set_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV, key_version);
            guard
        }
    }

    #[cfg(feature = "security")]
    impl Drop for EncryptionRequiredEnvGuard {
        fn drop(&mut self) {
            match self.prev_key.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ENV),
            }
            match self.prev_key_id.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_ID_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_ID_ENV),
            }
            match self.prev_key_version.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_KEY_VERSION_ENV),
            }
            match self.prev_required.take() {
                Some(v) => std::env::set_var(crate::crypto::ENCRYPTION_REQUIRED_ENV, v),
                None => std::env::remove_var(crate::crypto::ENCRYPTION_REQUIRED_ENV),
            }
        }
    }

    /// GOC-16 known-bad proof: `EPISTEMIC_GRAPH_ENCRYPTION_REQUIRED=on` with no
    /// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` configured must refuse to open the durable
    /// store — before any writer thread spawns, before any listener binds — with a
    /// bounded, actionable diagnostic naming both env vars, never a silent open.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_required_on_refuses_to_open_without_a_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard = EncryptionRequiredEnvGuard::set(None, "on");

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-required-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let result = RedbBackend::open(dir_s.clone(), 64);

        // `unwrap_err()` would require `RedbBackend: Debug` (it formats the Ok
        // side on failure); the backend deliberately does not derive it, since
        // it owns live handles. Destructure instead of widening a public trait
        // bound just to satisfy a test.
        let message = match result {
            Ok(_) => panic!("opening with ENCRYPTION_REQUIRED=on and no key must fail closed"),
            Err(message) => message,
        };
        assert!(
            message.contains("REQUIRED") && message.contains("EPISTEMIC_GRAPH_ENCRYPTION_KEY"),
            "diagnostic must be bounded and name the missing key, got: {message}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16: the mirror of the above — `ENCRYPTION_REQUIRED=on` with a key
    /// configured opens normally (a present key satisfies every mode identically).
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_required_on_succeeds_when_a_key_is_set() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard =
            EncryptionRequiredEnvGuard::set(Some("redb-encryption-required-test-key"), "on");

        let dir =
            std::env::temp_dir().join(format!("eg-redb-enc-required-ok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = RedbBackend::open(dir_s.clone(), 64)
            .expect("a configured key must open cleanly under ENCRYPTION_REQUIRED=on");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16 / BUG-248 known-bad proof: reopening an ALREADY-ENCRYPTED store with a
    /// DIFFERENT `EPISTEMIC_GRAPH_ENCRYPTION_KEY` than the one that sealed it must
    /// fail closed at open time — before any writer thread spawns or any listener
    /// binds — instead of opening successfully and only surfacing the mismatch later
    /// as a decrypt failure on whichever value a caller happens to read first.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_key_mismatch_refuses_to_reopen() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-mismatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        {
            let _guard = EncryptionRequiredEnvGuard::set(Some("original-key-material"), "warn");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("first open with a fresh key must establish the canary and succeed");
            backend.shutdown();
        }

        let result = {
            let _guard =
                EncryptionRequiredEnvGuard::set(Some("a-completely-different-key"), "warn");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!("reopening with the WRONG key must fail closed, not open silently"),
            Err(message) => message,
        };
        assert!(
            message.contains(crate::crypto::ENCRYPTION_KEY_ENV)
                && message.contains("does not match"),
            "diagnostic must be bounded and name the mismatch, got: {message}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// NE-028: changing only the non-secret key reference is still a rotation
    /// boundary.  Even when the material happens to be unchanged, startup refuses
    /// the ambiguous configuration instead of silently moving the pin.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_key_reference_mismatch_refuses_to_reopen() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-enc-key-ref-mismatch-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        {
            let _guard =
                EncryptionRequiredEnvGuard::set_with_ref("stable-key-material", "kms/graph", "1");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("first open must establish the pinned reference");
            backend.shutdown();
        }

        let result = {
            let _guard =
                EncryptionRequiredEnvGuard::set_with_ref("stable-key-material", "kms/graph", "2");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!("changing key version must fail closed before writer startup"),
            Err(message) => message,
        };
        assert!(message.contains("reference") && message.contains("does not match"));
        assert!(!message.contains("stable-key-material"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16 / BUG-248: the mirror of the above — reopening with the SAME key
    /// verifies cleanly every time (the canary is checked, never rewritten, on a
    /// key that still matches).
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_key_same_key_reopens_cleanly_across_multiple_opens() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard = EncryptionRequiredEnvGuard::set(Some("stable-key-material"), "warn");

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-stable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        for _ in 0..3 {
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("reopening with the SAME key must keep succeeding");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16 / BUG-248: a store that has NEVER had a key configured (encryption
    /// stays off) opens exactly as before — no canary table write, no mismatch
    /// check, byte-for-byte the pre-existing plaintext behavior.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_never_configured_skips_the_canary_check_entirely() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _guard = EncryptionRequiredEnvGuard::set(None, "off");

        let dir = std::env::temp_dir().join(format!("eg-redb-enc-none-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        for _ in 0..2 {
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("no key configured must keep opening exactly as before");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BUG-PE-055 known-bad proof, direction 1: a POPULATED PLAINTEXT store must not
    /// silently accept a brand-new key.
    ///
    /// Observed before this fix: the engine opened a previously-unencrypted store with
    /// a fresh key WITHOUT complaint, wrote a canary, and logged
    /// `redb encryption-at-rest ENABLED`. Every value already in the store then failed
    /// to unseal, one read at a time. This is the shape the deployment hits when the
    /// key lives under an ephemeral `AGENT_UTILITIES_DATA_DIR` (an emptyDir) while the
    /// store is on a persistent hostPath: a fresh key on every restart, against
    /// durable data.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn populated_plaintext_store_refuses_a_brand_new_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eg-redb-enc-adopt-plaintext-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        // Write real durable rows with encryption OFF — a plaintext store.
        {
            let _guard = EncryptionRequiredEnvGuard::set(None, "off");
            let backend = RedbBackend::open(dir_s.clone(), 64).expect("plaintext open");
            backend
                .register_graph("plain", "plain", crate::protocol::GraphType::Global)
                .await
                .expect("register");
            backend.shutdown();
        }

        // Now hand it a key it has never seen.
        let result = {
            let _guard = EncryptionRequiredEnvGuard::set(Some("a-brand-new-key"), "warn");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!(
                "a populated PLAINTEXT store must refuse a brand-new key, not silently \
                 start encrypting over it"
            ),
            Err(message) => message,
        };
        assert!(
            message.contains(crate::crypto::ENCRYPTION_KEY_ENV)
                && message.contains("PLAINTEXT")
                && message.contains("destructive-read"),
            "the diagnostic must name the variable and the hazard, got: {message}"
        );
        assert!(
            !message.contains("a-brand-new-key"),
            "key material must never reach a diagnostic, got: {message}"
        );

        // The store is untouched: it still opens with encryption off.
        {
            let _guard = EncryptionRequiredEnvGuard::set(None, "off");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("the refused open must leave the plaintext store serviceable");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BUG-PE-055 known-bad proof, direction 2: an ENCRYPTED store must not open with
    /// NO key at all.
    ///
    /// The no-key path never looked at the canary, so a store whose values are sealed
    /// opened cleanly and then failed one read at a time with
    /// `"encrypted durable value requires configured key material"`. That is the
    /// symmetric partner of the wrong-key refusal, and it holds regardless of
    /// `EPISTEMIC_GRAPH_ENCRYPTION_REQUIRED` — a missing key for an encrypted store is
    /// not a rollout-posture choice.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encrypted_store_refuses_to_open_without_a_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-enc-nokey-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        {
            let _guard = EncryptionRequiredEnvGuard::set(Some("sealing-key-material"), "warn");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("first open establishes the canary on an empty store");
            backend
                .register_graph("sealed", "sealed", crate::protocol::GraphType::Global)
                .await
                .expect("register");
            backend.shutdown();
        }

        // `off` deliberately: the refusal must not depend on the required-mode posture.
        let result = {
            let _guard = EncryptionRequiredEnvGuard::set(None, "off");
            RedbBackend::open(dir_s.clone(), 64)
        };
        let message = match result {
            Ok(_) => panic!("an ENCRYPTED store must refuse to open with no key configured"),
            Err(message) => message,
        };
        assert!(
            message.contains(crate::crypto::ENCRYPTION_KEY_ENV)
                && message.contains("encryption-at-rest metadata"),
            "the diagnostic must name the variable and the cause, got: {message}"
        );

        // With the original key it opens again — the refusal changed nothing on disk.
        {
            let _guard = EncryptionRequiredEnvGuard::set(Some("sealing-key-material"), "warn");
            let backend = RedbBackend::open(dir_s.clone(), 64)
                .expect("the original key must still open the store");
            backend.shutdown();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// GOC-16: `off` and unset (`warn`, the shipped default) must NOT change
    /// today's behavior — a missing key still opens successfully, only the log
    /// output differs (proven at the unit level in `crypto.rs`, not observable
    /// through this `Result`).
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread")]
    async fn encryption_required_off_and_warn_still_open_without_a_key() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;

        for mode in ["off", "warn"] {
            let _guard = EncryptionRequiredEnvGuard::set(None, mode);
            let dir = std::env::temp_dir().join(format!(
                "eg-redb-enc-required-{mode}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();

            let backend = RedbBackend::open(dir_s.clone(), 64)
                .unwrap_or_else(|e| panic!("mode {mode:?} must still open without a key: {e}"));
            backend.shutdown();
            let _ = std::fs::remove_dir_all(&dir);
        }
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
        // Under the write guard taken above, so it does not acquire anything itself.
        // ONE shared key value for the whole binary -- see
        // `crate::crypto::TEST_AT_REST_KEY`'s doc for why a per-module value was the
        // bug and not a convenience.
        #[cfg(feature = "security")]
        crate::crypto::provision_test_at_rest_key_under_write_guard();

        const SECRET: &str = "redb-txn-secret";
        let dir = std::env::temp_dir().join(format!("eg-redb-txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
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

        let backend2 = RedbBackend::open(dir_s.clone(), 64).expect("reopen redb backend");
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
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
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
            Some(ResultPayload::Raw(b)) => Some(b),
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
            Some(ResultPayload::Raw(b)) => Some(b),
            Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
            other => panic!("unexpected get result: {other:?}"),
        };
        assert_eq!(
            got,
            Some(props(serde_json::json!({"v": 2}))),
            "recreated tenant's node n1 must read back as the NEW write {{v:2}}, not stale/empty"
        );
        // `shutdown()` stops each shard's writer THREAD but does not close its
        // `Database` — the handle lives in the `ShardWriter`, so redb keeps its advisory
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
                match RedbBackend::open(dir_s.clone(), 256) {
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
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

            let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
                Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
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
                    Some(ResultPayload::Raw(b)) => Some(b),
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
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
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
        // `Database` -- the handle lives in the `ShardWriter`, so redb keeps its advisory
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

        let backend2 = RedbBackend::open(dir_s.clone(), 256).expect("reopen");
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
    /// op is durably committed. The ONLY way the await completes is the group commit
    /// firing the waiter — there is no per-drained-batch commit path left for it to
    /// settle on incidentally. After the await returns, a SEPARATE reopened store
    /// sees the row — proving the await observed durable state, not just an enqueue.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_durable_awaits_commit() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-durable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open");

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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-coalesce-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));

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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-snapread-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        // K=3 explicit (cfg(test) defaults to 1; open_with_shards honors the request on
        // a fresh dir). Graph names spread across shards via FNV-1a routing.
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 64, 3).expect("open sharded");
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-snapconc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));

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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-snapack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s.clone(), 64).expect("open");

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
    /// durability. We enqueue N ops directly onto the shard's writer channel (see the
    /// GOC-70 rule 3 note below) and assert:
    ///   * every awaited writer resolves Ok and every node is durably present
    ///     (durability guarantee unchanged — commit-before-ack still holds),
    ///   * `ops` accounts for every op, exactly (no double count, no drop),
    ///   * the average batch size (`ops / commits`) climbs well above 1, i.e. the
    ///     linger folded many writers into one fsync (the profiled win),
    ///   * lingered commits were actually exercised.
    ///
    /// The live cadence is the only one now (the `Each` per-drained-batch policy is
    /// deleted), and pre-EG-024 a drained channel committed immediately at ~1
    /// op/fsync — which is exactly the shape the linger exists to widen.
    ///
    /// GOC-70 (fix/micro-linger-stats-race): on the real 2-vCPU CI runner this test
    /// once failed with `ops == 0` — NOT a "stats read before the writer ran" race
    /// (the durability assertions right above prove every write was already durably
    /// on disk by then, and `stats.record()` is called on the SAME writer thread
    /// strictly before that batch's `done` oneshots fire, so anything already acked
    /// is, by program order + the oneshot channel's release/acquire handoff, already
    /// reflected in the counters). The real bug: `handle_cmd`'s early-flush bound
    /// (`pending.ops.len() >= flush_threshold`, memory-safety valve for a burst that
    /// outpaces the tick) called `commit_and_notify` DIRECTLY, bypassing
    /// `stats.record()` entirely — a batch that settles via that path durably lands
    /// (acks fire correctly) but was silently never accounted for. `n` (256) here
    /// used to exactly equal the old capacity-512-derived `flush_threshold` (256),
    /// so whenever every op landed in the channel before the writer's first drain —
    /// routine when a couple of tokio workers contend for 2 real CPUs, rare when the
    /// writer OS thread gets its own core almost immediately on a many-core dev
    /// box — the WHOLE batch took that unaccounted path. Fixed at the root
    /// (`handle_cmd` now records stats on every commit path it takes, not only the
    /// run-loop's own drain/linger/tick path — see the `flush` helper in
    /// `handle_cmd`) so `ops`/`commits` are accounted identically no matter which
    /// path a batch settles through, on any core count (GOC-70 rule 1). The capacity
    /// below is also raised so `flush_threshold` sits well above `n`, keeping this
    /// test's OWN outcome about the micro-linger mechanism specifically, not
    /// incidentally about the separate early-flush bound.
    ///
    /// GOC-70 rule 3 (deterministic contention, not scheduler luck): the original
    /// shape fanned N `record_durable` calls out across N separate `tokio::spawn`
    /// tasks and relied on the executor overlapping enough of them within a short
    /// linger window — exactly the anti-pattern this file's own
    /// `write_coalescer::tests::concurrent_writes_coalesce_into_fewer_lock_
    /// acquisitions` was rewritten away from after it broke
    /// `dispatch_coalesces_concurrent_writes_to_one_graph` in 2.25.0 (true on a
    /// lightly-loaded many-core host; not guaranteed when a couple of tokio workers
    /// and a `spawn_blocking` hop each contend for 2 real CPUs). A test-only gate
    /// pauses the writer immediately before it starts the linger receive, signals
    /// the fixture, and is released only after the remaining 255 commands are
    /// queued. The burst therefore lands in the same pending batch by construction;
    /// no scheduling overlap or wall-clock window is part of the assertion.
    ///
    /// Serializes the remaining env-mutating linger fixture. The coalescing fixture
    /// injects its config directly; the disabled baseline still reads
    /// `EPISTEMIC_GRAPH_REDB_GROUP_*` once at open.
    static LINGER_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test(flavor = "multi_thread")]
    async fn micro_linger_coalesces_concurrent_writers() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-linger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let (control, entered_rx, release_tx) = RedbGroupCommitTestControl::new();
        let backend = Arc::new(
            RedbBackend::open_with_group_commit_config(
                dir_s.clone(),
                // Channel capacity 4096 ⇒ `resolve_flush_threshold` (capacity/2, clamped
                // 256..16384) resolves to 2048 — comfortably above `n` below (256), so
                // the writer's early-flush memory bound cannot fire for this batch. This
                // keeps the fixture focused on the micro-linger mechanism rather than
                // the separate early-flush bound.
                4096,
                RedbGroupCommitConfig {
                    linger: Duration::from_millis(2),
                    shallow_threshold: 256,
                    test_control: Some(control),
                },
            )
            .expect("open"),
        );
        // Declare this after `backend`: if an assertion unwinds while the writer
        // is held at the injected gate, the guard releases it before the backend's
        // implicit Drop tries to join that writer.
        let mut release = ReleaseOnDrop::new(release_tx);

        let n = 256usize;
        // GOC-70 rule 3: queue the first command, then wait for the writer's
        // injected pre-linger signal before queueing the rest. The writer is held
        // until this loop has filled the same channel batch, so this remains
        // deterministic even when the test binary is CPU-starved.
        let shard = backend.shard_for("g1");
        // Observe the writer that actually owns this graph. `commit_stats()` is the
        // shard-0 compatibility view and is not a valid oracle when auto-sizing K>1.
        let stats = shard.stats.clone();
        let mut receivers = Vec::with_capacity(n);
        let enqueue = |i: usize| {
            let (done, rx) = oneshot::channel();
            shard
                .tx
                .send(Cmd::Mutation {
                    graph: "g1".to_string(),
                    method: Box::new(Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    }),
                    done,
                })
                .expect("redb writer thread alive");
            rx
        };
        receivers.push(enqueue(0));
        tokio::task::spawn_blocking(move || {
            crate::test_rendezvous::recv_within(
                &entered_rx,
                "the redb writer entering the injected linger gate",
            );
        })
        .await
        .expect("linger gate waiter must complete");
        for i in 1..n {
            receivers.push(enqueue(i));
        }
        release
            .release()
            .expect("redb writer must still be held at the linger gate");
        for rx in receivers {
            rx.await
                .expect("redb writer dropped completion")
                .expect("each durable commit ok");
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
        // The win: both writers folded into one commit rather than one commit each.
        let commits = stats.commits();
        let ops = stats.ops();
        assert_eq!(ops, n as u64, "every op must be accounted for exactly once");
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-nolinger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            // Serialized vs the coalesce test so its linger value can't leak into our `open`.
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US", "0");
            let b = Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-safe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open"));
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
    /// The only way an awaited op completes is the group commit firing. We launch a
    /// `record_durable` (M2 graph mutation) and a
    /// `raft_log_append` (Raft log entry) CONCURRENTLY into the same tick window;
    /// both share ONE `Pending` batch → ONE `WriteTransaction` → ONE fsync. We then
    /// prove BOTH landed durably (the graph row AND the log row).
    #[cfg(feature = "raft")]
    #[tokio::test(flavor = "multi_thread")]
    async fn raft_log_and_mutation_share_one_group_commit() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-redb-1txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open"));

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
        // process-global, and it is now ONE shared value for the whole binary (see
        // `crate::crypto::TEST_AT_REST_KEY`'s doc -- a per-module value is what made
        // the ambient key change up to seven times over one run). Every caller of
        // `cm_dir` holds `crate::crypto::acquire_test_env_lock()` for its entire test
        // body (see each call site), so the provisioning below always runs under that
        // lock -- do NOT acquire it here, `tokio::sync::RwLock` is not reentrant.
        crate::crypto::provision_test_at_rest_key_under_write_guard();
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
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
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
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
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

    /// A backend whose cross-modal DURABLE COMMIT always FAILS — used to prove the
    /// handler rolls back ALL modalities (applies nothing in-memory) on a
    /// durable-commit failure: no partial cross-modal commit
    /// (CONCEPT:EG-KG.txn.reader-never-sees-node).
    ///
    /// Everything that is NOT the injected failure delegates to a real
    /// `RedbBackend`, including the `as_redb` downcast. That delegation is load
    /// bearing twice over, and both halves were missing:
    ///
    /// * `as_redb` defaults to `None` on the trait (`persistence/mod.rs`), and
    ///   `handlers::txn::begin_txn_lifecycle_receipt` refuses every
    ///   txn-lifecycle method — `BeginTxn` included — without it ("transaction
    ///   lifecycle requires durable redb"). So this double failed the very FIRST
    ///   call of `stage_crossmodal`, and both tests using it died on
    ///   `panic!("BeginTxn id, got None")` before reaching the rollback they
    ///   name. The staging half of the scenario was never exercised at all.
    /// * the commit path calls `commit_mutation_batch_crossmodal`, not the
    ///   lower-level `commit_crossmodal` this double used to override. Overriding
    ///   only the latter left the injection attached to a method the handler no
    ///   longer calls: the commit would have failed anyway, but on the trait's
    ///   own "backend does not support atomic cross-modal MutationBatch commits"
    ///   default — i.e. the test would have asserted rollback after a
    ///   NOT-IMPLEMENTED error rather than after the mid-way durable failure it
    ///   describes. Both are overridden now, with the same injected message, so
    ///   whichever seam a future refactor routes through, the failure the test
    ///   names is the failure it gets.
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
        fn as_redb(&self) -> Option<&RedbBackend> {
            self.inner.as_redb()
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
        async fn commit_mutation_batch_crossmodal(
            &self,
            _args: crate::server::persistence::CrossModalCommitArgs<'_>,
        ) -> Result<crate::server::persistence::MutationBatchCommit, String> {
            // The seam the cross-modal Commit handler actually calls.
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
        let inner = Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
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
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
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
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        // The measurement modality of this cross-modal commit is written to the tsdb
        // SeriesStore, so the state MUST carry one (same setup the measurement +
        // reconciliation tests use) — without it the Commit fails writing the tsdb leg.
        let series_store = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
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
            // graph-0.redb is a GraphShard owner file; reopen it through its
            // sole owner and read the file-wide SERIES tables from the control
            // scope. Opening it as a standalone SeriesStore would ask the
            // storage kernel for a different owner manifest.
            let shard =
                Shard::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path()).unwrap();
            let read = shard.control_read().unwrap();
            let key = direct_test_series_key("media", "sensor");
            let meta = eg_tsdb::store::meta_in_rtx(&read, &key)
                .unwrap()
                .expect("series durable");
            assert_eq!(meta.count, 2, "both measurement points durable");
            let scanned = eg_tsdb::store::range_in_rtx(
                &read,
                &key,
                eg_tsdb::point::Ts::MIN,
                eg_tsdb::point::Ts::MAX,
            )
            .unwrap();
            assert_eq!(scanned.len(), 2, "measurement points readable post-reload");
            assert_eq!(scanned[0].values, vec![10.0]);
            assert_eq!(scanned[1].values, vec![20.0]);
        }

        // Reload the graph tier: node + vector + blob-ref + CONSTRUCT triple all durable.
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
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
        // See `crossmodal_txn_commits_all_modalities_atomically` above: held for the
        // whole test.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = cm_dir("five-rollback");
        let points = vec![(1_000_000_000i64, vec![10.0])];
        let inner = Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
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
            let shard =
                Shard::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path()).unwrap();
            let read = shard.control_read().unwrap();
            let key = direct_test_series_key("media", "sensor");
            assert!(
                eg_tsdb::store::meta_in_rtx(&read, &key).unwrap().is_none(),
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
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let series_store = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
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
            let shard =
                Shard::open(std::path::Path::new(&dir).join(shard_filename(0)).as_path()).unwrap();
            let read = shard.control_read().unwrap();
            let key = envelope_test_series_key("media", "sensor.ts-unify");
            let meta = eg_tsdb::store::meta_in_rtx(&read, &key)
                .unwrap()
                .expect("measurement durable in the authoritative shard");
            assert_eq!(meta.count, 3, "all 3 points durable in the shard");
        }

        // (3) RESTART: drop + reopen BOTH stores from the SAME persist dir on a FRESH
        // ServerState, then re-run the SAME public TsRange call.
        drop(series_store);
        drop(state);

        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let state2 = new_state(Some(dir.clone()));
        backend2.load_all(&state2).await.unwrap();
        let series_store2 = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
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
            Arc::new(RedbBackend::open(dir.clone(), 64).unwrap());
        let series_store = Arc::new(
            SeriesStore::open_in_dir(
                std::path::Path::new(&dir),
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .unwrap(),
        );
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::{AuditReport, ResultPayload};

        const SECRET: &str = "audit-secret";
        let dir = std::env::temp_dir().join(format!("eg-audit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::{LedgerReadResult, ResultPayload};

        const SECRET: &str = "get-ledger-secret";
        let dir = std::env::temp_dir().join(format!("eg-get-ledger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::{AuditReport, MerkleInclusionReport, ResultPayload};
        use crate::server::persistence::provenance_anchor;

        const SECRET: &str = "provenance-secret";
        let dir = std::env::temp_dir().join(format!("eg-provenance-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
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

        let core = state
            .read()
            .await
            .registry
            .get("__commons__")
            .unwrap()
            .core
            .clone();
        let anchored_version =
            PersistenceBackend::read_mutation_graph_version(backend.as_ref(), "__commons__")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            anchored_version, 4,
            "three node writes and one anchor commit"
        );
        assert_eq!(
            core.version(),
            anchored_version,
            "anchor publishes the durable OCC fence"
        );
        assert_eq!(
            provenance_anchor::sweep(&state).await,
            0,
            "unchanged root is idle"
        );
        assert_eq!(
            core.version(),
            anchored_version,
            "idle sweep must not bump RAM"
        );
        assert_eq!(
            PersistenceBackend::read_mutation_graph_version(backend.as_ref(), "__commons__")
                .await
                .unwrap(),
            Some(anchored_version),
            "idle sweep must not bump authority"
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

        assert_eq!(
            core.version(),
            6,
            "overwrite and second anchor each publish once"
        );
        assert_eq!(
            PersistenceBackend::read_mutation_graph_version(backend.as_ref(), "__commons__")
                .await
                .unwrap(),
            Some(core.version()),
            "second anchor retains authoritative/serving parity"
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
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 64, 1).expect("open K=1");
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
            RedbBackend::open(dir_s.clone(), 64).expect("open auto")
        };
        assert_eq!(auto.shard_count(), 1, "cfg(test) default K=1");
        assert!(dir.join("graph-0.redb").exists());
        auto.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn normal_startup_rejects_retired_single_file_layout() {
        // Sync `#[test]`, so the blocking counterpart. It opens a durable store
        // and therefore needs the encryption env to hold still; it never mutates
        // it, but `blocking_read` is the only guard available off a runtime.
        let _env_read_lock = crate::crypto::TEST_ENV_LOCK.blocking_read();
        let dir = std::env::temp_dir().join(format!(
            "eg-shard-retired-layout-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let retired = Database::create(dir.join("graph.redb")).unwrap();
        drop(retired);

        let err = match RedbBackend::open_with_shards(dir.to_string_lossy().to_string(), 64, 1) {
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
        // resolve the SAME encryption-at-rest cipher (`ValueCipher::from_env_checked`,
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
        // `record_durable`/`read_node`/`shard_index` all take a graph FNAME -- the
        // sanitized storage spelling, as their parameter names and `shard_index`'s
        // own doc say, and as every production caller passes
        // (`server::mutation::commit_finalize_durable` does
        // `crate::persist::sanitize(ctx.graph_name)` first). This test is the only
        // one in the file that picked a name needing sanitization (`:` is outside
        // the storage alphabet) and then skipped the step, so its durable commit
        // was refused by `validate_ordinary_physical_graph_key` with "physical
        // graph key must use the sanitized storage alphabet". Sanitize once and
        // use the fname everywhere the API asks for one -- which keeps the point
        // of the test (a punctuated, namespaced graph name routes deterministically
        // and survives a restart) rather than trading it for an unpunctuated name.
        let graph = "agent:router-test";
        let graph_fname = crate::persist::sanitize(graph);
        let owner = shard_index(&graph_fname, K);

        {
            let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K).expect("open K=4");
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
                    &graph_fname,
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
                backend
                    .read_node(&graph_fname, "n1")
                    .await
                    .unwrap()
                    .is_some(),
                "node readable pre-restart"
            );
            backend.shutdown();
        }

        // RESTART: reopen the SAME dir (K reconciled from on-disk layout) and read the
        // node straight back from disk — durability across a process restart.
        {
            let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K).expect("reopen K=4");
            assert_eq!(backend.shard_count(), K, "K reconciled from disk");
            assert!(
                backend
                    .read_node(&graph_fname, "n1")
                    .await
                    .unwrap()
                    .is_some(),
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
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

        let backend = Arc::new(RedbBackend::open_with_shards(dir_s.clone(), 256, K).expect("open"));
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-shard-env-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_SHARDS", "3");
            let b = RedbBackend::open(dir_s.clone(), 64).expect("open");
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        let root = std::env::temp_dir().join(format!("eg-r5-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);

        // (1) No catalog.redb, no env ⇒ open() attaches NOTHING (default EG-026 routing).
        let plain_dir = root.join("plain");
        std::fs::create_dir_all(&plain_dir).unwrap();
        let plain = RedbBackend::open(plain_dir.to_string_lossy().to_string(), 64).unwrap();
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
        let attached = RedbBackend::open(cat_dir_s.clone(), 64).unwrap();
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();

        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = Arc::new(
            RedbBackend::open_with_shards(dir_s.clone(), 256, K)
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::cold_offload::{offload_cold_tenants, ColdTenantTracker};
        let dir = std::env::temp_dir().join(format!("eg-r6-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 256).expect("open"));
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
                    // `Barrier::wait` is a `disallowed_methods` entry because an
                    // unbounded wait hangs instead of failing. Here the bound
                    // already exists, and in a better place: the whole fan-out
                    // is driven under the `tokio::time::timeout(10s)` below,
                    // which this test's doc comment names as the mechanism that
                    // "converts into a test failure" the serial-impl regression
                    // this barrier exists to detect. A second deadline inside
                    // the closure could only fire after the outer one already
                    // failed the test.
                    #[allow(clippy::disallowed_methods)]
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

        let backend = RedbBackend::open_with_shards(dir_s.clone(), 64, K).expect("open K=4");
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

        let backend2 = RedbBackend::open_with_shards(dir_s.clone(), 64, K).expect("reopen K=4");
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::rebalance::{
            plan_rebalance, shard_loads_from_catalog, RebalanceOptions,
        };
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
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
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-r1-delta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
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

    /// WD5-BUG-04 (online-reshard half of the SAME defect class WD3-BUG-01 fixed
    /// offline in `shard_migrate.rs::migrate_shards`, see its commit's coverage
    /// matrix): a representative row from RESOURCE_*, development_lane_*,
    /// capacity_lease_*, and (under `security`) PROVENANCE_ANCHOR_MEMBERS, plus a
    /// WORK_ITEM_COMMAND_SEQUENCE row, is seeded directly (raw redb — these
    /// subsystems require heavy native-operation preconditions a raw seed
    /// sidesteps) into the graph's SOURCE shard; the graph is then resharded onto a
    /// different shard via the public `reshard_graph` API, and every row is
    /// confirmed present on the DESTINATION shard afterward — and (except
    /// `PROVENANCE_ANCHOR_MEMBERS`; see below) absent from the source, proving a
    /// MOVE, not a copy. Deliberately does NOT seed `PLAN_MATVIEWS`/
    /// `MATVIEW_OPERATOR_STATE` (GLOBAL, no graph key — moving them per-graph would
    /// be its own corruption) or the `work_item_capability` trio (deliberately
    /// purged-not-copied by this module's own design). Confirmed FAILING before
    /// this fix — every `table_row_count(&dst_path, ...)` below was 0 against the
    /// unmodified `export_graph_raw`/`import_graph_raw`.
    #[tokio::test(flavor = "multi_thread")]
    async fn online_reshard_preserves_resource_lane_capacity_and_provenance_tables() {
        // WD5-BUG-05: unlike the sibling reshard tests, this one raw-seeds
        // RESOURCE_RESERVATIONS/development_lane::HOLDS with PLAINTEXT msgpack
        // (`seed_valid_resource_reservation`/`seed_valid_development_lane_hold`
        // below) and later reads them back through the backend's OWN decode path
        // (PurgeGraph's pre-existing lifecycle-authority validation — see those
        // helpers' doc comments). `cm_dir` (used by the crossmodal txn tests
        // above) provisions `EPISTEMIC_GRAPH_ENCRYPTION_KEY` PROCESS-GLOBALLY via
        // a `std::sync::Once` and, by its own documented design, NEVER unsets it.
        // A plain READ lock does not protect against that already-set ambient
        // state left behind by an earlier test in the same binary (confirmed:
        // this test fails with "encrypted durable value is missing sealed
        // framing" when run after `crossmodal_txn_commits_all_modalities_atomically`
        // / `crossmodal_measurement_visible_via_public_tsrange_post_commit_and_restart`,
        // but passes standalone). Take the WRITE lock and force encryption OFF for
        // this test's whole body instead, so the seeded plaintext always
        // round-trips regardless of test execution order/composition.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        let _enc_guard = EncryptionRequiredEnvGuard::set(None, "off");
        #[cfg(feature = "security")]
        use crate::redb_store::PROVENANCE_ANCHOR_MEMBERS;
        use crate::redb_store::{capacity_lease, development_lane};
        use crate::redb_store::{RESOURCE_RESERVATIONS, WORK_ITEM_COMMAND_SEQUENCE};
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-reshard-cx054-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));

        let g = "cx054-graph";
        // Register the graph THROUGH a live backend so GRAPH_META exists, then shut
        // it down to raw-seed the WD5-BUG-04 tables — the same "open, register,
        // shutdown, raw-seed" sequence `shard_migrate.rs`'s own coverage test uses.
        {
            let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
                .expect("open")
                .with_catalog(catalog.clone());
            backend
                .register_graph(g, g, GraphType::Global)
                .await
                .unwrap();
            backend.shutdown();
        }

        let src_idx = catalog.resolve_shard(g, K);
        let dst_idx = (src_idx + 1) % K;
        let src_path = dir.join(shard_filename(src_idx));

        // RESOURCE_RESERVATIONS and development_lane::HOLDS must be VALID, purge-safe
        // records (not raw junk bytes): unlike backup/restore, online reshard's
        // PurgeGraph step decodes+validates these two tables as a pre-existing
        // lifecycle-authority guard (see the two helpers' doc comments).
        seed_valid_resource_reservation(&src_path, g, "r1", "cx054-tenant");
        seed_valid_development_lane_hold(&src_path, g, "h1", "cx054-tenant");
        seed_raw_row(&src_path, capacity_lease::CELLS, (g, "c1"), b"cell");
        seed_raw_row(&src_path, WORK_ITEM_COMMAND_SEQUENCE, g, 7u64);
        #[cfg(feature = "security")]
        seed_raw_row(
            &src_path,
            PROVENANCE_ANCHOR_MEMBERS,
            (g, 1u64),
            b"anchor-member",
        );

        // Sanity: every seeded row landed in the source before the reshard.
        assert_eq!(table_row_count(&src_path, RESOURCE_RESERVATIONS), 1);
        assert_eq!(table_row_count(&src_path, development_lane::HOLDS), 1);
        assert_eq!(table_row_count(&src_path, capacity_lease::CELLS), 1);
        assert_eq!(table_row_count(&src_path, WORK_ITEM_COMMAND_SEQUENCE), 1);
        #[cfg(feature = "security")]
        assert_eq!(table_row_count(&src_path, PROVENANCE_ANCHOR_MEMBERS), 1);

        let backend = RedbBackend::open_with_shards(dir_s.clone(), 256, K)
            .expect("reopen")
            .with_catalog(catalog.clone());
        let report = backend
            .reshard_graph(g, dst_idx as u32)
            .await
            .expect("reshard");
        assert!(!report.no_op);
        assert!(
            report.capability_and_resource >= 4,
            "WD5-BUG-04 tables counted in the reshard report ({})",
            report.capability_and_resource
        );
        backend.shutdown();

        let dst_path = dir.join(shard_filename(dst_idx));
        assert_eq!(
            table_row_count(&dst_path, RESOURCE_RESERVATIONS),
            1,
            "resource_reservations survives the online reshard (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&dst_path, development_lane::HOLDS),
            1,
            "development_lane_holds survives the online reshard (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&dst_path, capacity_lease::CELLS),
            1,
            "capacity_cells survives the online reshard (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&dst_path, WORK_ITEM_COMMAND_SEQUENCE),
            1,
            "work_item_command_sequence survives the online reshard (WD5-BUG-04)"
        );
        #[cfg(feature = "security")]
        assert_eq!(
            table_row_count(&dst_path, PROVENANCE_ANCHOR_MEMBERS),
            1,
            "provenance_anchor_members survives the online reshard (WD5-BUG-04)"
        );

        // The move, not a copy: `Cmd::PurgeGraph`'s `purge_graph_rows` (step 4 of
        // `delta_flip_purge`) already unconditionally clears RESOURCE_*/
        // development_lane_*/capacity_lease_*/WORK_ITEM_COMMAND_SEQUENCE for the
        // graph at the source — that call was ALREADY correct before this fix and is
        // exactly why the pre-fix bug was true data loss (drop from source, no copy
        // to destination), not a leak. `PROVENANCE_ANCHOR_MEMBERS` is the one
        // exception: `purge_graph_rows` does not clear it (a pre-existing gap in
        // `redb_store.rs`, outside this lane's two-file scope — see the lane
        // report), so its source-side row is orphaned rather than purged.
        assert_eq!(
            table_row_count(&src_path, RESOURCE_RESERVATIONS),
            0,
            "resource_reservations purged from the source after the move"
        );
        assert_eq!(
            table_row_count(&src_path, development_lane::HOLDS),
            0,
            "development_lane_holds purged from the source after the move"
        );
        assert_eq!(
            table_row_count(&src_path, capacity_lease::CELLS),
            0,
            "capacity_cells purged from the source after the move"
        );
        assert_eq!(
            table_row_count(&src_path, WORK_ITEM_COMMAND_SEQUENCE),
            0,
            "work_item_command_sequence purged from the source after the move"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.backend.m3-admin-dispatch — drive the M3 admin ops over the FULL dispatch path (protocol →
    /// `handlers::admin` → the persistence APIs): assign a catalog placement, list it back,
    /// and get a rebalance plan. Proves the WIRE surface, not just the backend methods.
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_rpc_dispatch_roundtrip() {
        // Holds the encryption env still for this test's whole body: it opens a
        // durable store, so a concurrent key mutation would break its canary
        // check. A READ guard, so these tests still run concurrently with each
        // other -- only a key MUTATOR is excluded.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::protocol::ResultPayload;
        use crate::server::persistence::tenant_catalog::TenantCatalog;
        const SECRET: &str = "admin-rpc";
        const K: usize = 4;
        let dir = std::env::temp_dir().join(format!("eg-admin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let catalog = Arc::new(TenantCatalog::open(&dir_s).expect("catalog"));
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open_with_shards(dir_s.clone(), 256, K)
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
