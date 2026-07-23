//! Raft storage (CONCEPT:AU-KG.ingest.source-sync-canonical + KG-2.204 + KG-2.273) — durable log store + state
//! machine, on openraft 0.10's **v2 split-storage** API.
//!
//! openraft 0.10 removed the combined `RaftStorage` trait (and the `Adaptor` that
//! split it). A store now implements TWO traits directly:
//!
//! * [`RaftLogStorage`] (+ its super-trait [`RaftLogReader`]) — the durable LOG, vote,
//!   and (optionally) the committed pointer; and
//! * [`RaftStateMachine`] (+ [`RaftSnapshotBuilder`]) — apply + snapshot.
//!
//! Both are implemented on `Arc<EgStore>`, so [`super::multi::create_group`] passes the
//! SAME `Arc` as both the log store and the state machine (no adaptor needed). Two
//! engine-specific properties carry over from the 0.9 implementation:
//!
//! 1. **The state machine IS the engine.** Ordinary committed methods are staged
//!    deterministically from the authoritative pre-image, then graph state and the
//!    universal MutationBatch status/fence/idempotency/outbox authority commit in one
//!    redb transaction before RAM publication. `ApplyChangeEnvelope` is intentionally
//!    not decomposed: its graph rows and auxiliary authority commit through one native
//!    redb transaction before an atomic in-memory snapshot publication.
//!
//! 2. **Durable redb Raft log (CONCEPT:EG-KG.storage.one-fsync-covers-raft).** The log entries, the vote, and
//!    the applied-state pointers all live in the SAME authoritative shard as the
//!    M2 graph data — keyed by `(group_id, index)` / `(group_id, key)` so ONE redb
//!    file serves the M2 store AND every Raft group's log. Because the log shares the
//!    M2 `RedbBackend`'s off-reactor group-commit writer, a log append and its graph
//!    mutation COALESCE into ONE `WriteTransaction` / one fsync. A restarted node
//!    recovers its log tail LOCALLY from redb.
//!
//! ### 0.10 API notes
//!
//! * Every storage method now returns `std::io::Error` directly (the 0.9
//!   `StorageError`/`StorageIOError` constructors are gone) — failures map through the
//!   small [`ioerr`] helper.
//! * Types are parameterized by the type config via the `…Of<C>` aliases
//!   ([`LogIdOf`], [`VoteOf`], [`SnapshotMetaOf`], …) instead of `LogId<NodeId>` etc.
//! * [`RaftLogStorage::append`] returns after the in-memory save and signals
//!   durability through an [`IOFlushed`] callback — our redb append is synchronously
//!   durable, so we fire the callback right after the group-commit fsync resolves.
//! * [`RaftStateMachine::apply`] consumes a `Stream` of `(entry, responder)`; the
//!   per-entry [`ApplyResponder`] is `send`-ed the response after the effect lands.
//! * The chunked `install_snapshot` is replaced by full-snapshot transfer; the state
//!   machine still installs a full graph dump.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::io;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::storage::EntryResponder;
use openraft::storage::{
    IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
    Snapshot, SnapshotMeta,
};
use openraft::type_config::alias::{
    EntryOf, LogIdOf, SnapshotDataOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::{EntryPayload, OptionalSend, StoredMembership};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::{
    AppCtx, GroupId, NativeMutationCommand, RaftRequest, RaftResponse, ReplicatedMutation,
    TypeConfig,
};
use crate::protocol::{GraphType, Method};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

const KEY_VOTE: &str = "vote";
const KEY_APPLIED: &str = "applied_state";
const KEY_PURGED: &str = "last_purged";
const KEY_NATIVE_HISTORY_PREFIX: &str = "native_history/";
const KEY_NATIVE_HISTORY_BITMAP_PREFIX: &str = "native_history_bitmap/";
const NATIVE_HISTORY_BITMAP_BITS: u64 = 1024;
const NATIVE_HISTORY_BITMAP_BYTES: usize = (NATIVE_HISTORY_BITMAP_BITS / 8) as usize;
const MAX_RAFT_META_BYTES: usize = 4 * 1024 * 1024;
const MAX_RAFT_LOG_ENTRY_BYTES: usize = super::network::MAX_RAFT_FRAME_BYTES - 1024 * 1024;
const MAX_RAFT_SNAPSHOT_BYTES: usize = MAX_RAFT_LOG_ENTRY_BYTES;
const MAX_RAFT_LOG_ITEMS: usize = 4_000_000;
const MAX_RAFT_SNAPSHOT_ITEMS: usize = 16_000_000;
const MAX_RAFT_LOG_BATCH_ENTRIES: usize = 100_000;
const RAFT_SNAPSHOT_SCHEMA_VERSION: u16 = 4;

fn decode_raft_value<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
    max_items: usize,
) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            max_bytes,
            max_items,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "raft value is invalid or exceeds resource limits".to_string())
}

fn validate_raft_value(bytes: &[u8], max_bytes: usize, max_items: usize) -> Result<(), String> {
    eg_types::msgpack::validate_single_value(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            max_bytes,
            max_items,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "raft value is invalid or exceeds resource limits".to_string())
}

/// Map any `Display` error (the redb backend + rmp_serde all surface `String`/`E:
/// Display`) into the `io::Error` the 0.10 storage traits now require.
fn ioerr<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

fn native_history_key(index: u64) -> String {
    format!("{KEY_NATIVE_HISTORY_PREFIX}{index:020}")
}

fn native_history_bitmap_key(chunk: u64) -> String {
    format!("{KEY_NATIVE_HISTORY_BITMAP_PREFIX}{chunk:020}")
}

fn is_replayable_native_request(request: &RaftRequest) -> bool {
    matches!(
        &request.command,
        ReplicatedMutation::Native { command } if command.domain().is_some()
    )
}

/// One graph's sole authoritative durable image, captured so a follower can
/// rebuild it on `install_snapshot` even if it never saw the per-entry log.
///
/// Do not add a second decoded node/edge/semantic image here. Besides doubling
/// snapshot memory and wire size, that would serialize plaintext properties next
/// to encrypted-at-rest rows and permit the two copies to disagree on restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphSnapshot {
    schema_version: u16,
    fname: String,
    /// Complete graph-scoped redb authority, required for every current snapshot;
    /// its `graph_meta` row is the sole source of the logical name and graph type.
    durable: crate::server::persistence::online_reshard::RawGraphRows,
}

impl GraphSnapshot {
    fn validate_and_identity(&self) -> Result<(String, GraphType, String), String> {
        if self.schema_version != RAFT_SNAPSHOT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported Raft graph snapshot schema {} (expected {})",
                self.schema_version, RAFT_SNAPSHOT_SCHEMA_VERSION
            ));
        }
        self.durable
            .durable_identity(&self.fname)?
            .ok_or_else(|| "Raft graph snapshot is missing durable identity".to_string())
    }
}

/// The serialized state-machine snapshot body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
struct SmSnapshotData {
    schema_version: u16,
    #[serde(deserialize_with = "deserialize_required_option")]
    last_applied_log: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
    graphs: Vec<GraphSnapshot>,
    /// Successful encrypted native commands, ordered by committed log index.
    /// Replaying them reconstructs stores that do not live in the authoritative shard.
    native_history: Vec<NativeHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeHistoryEntry {
    log_index: u64,
    request: RaftRequest,
}

/// The on-disk applied-state pointers persisted to redb after every apply.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
struct AppliedState {
    #[serde(deserialize_with = "deserialize_required_option")]
    last_applied_log: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
}

#[cfg(test)]
mod current_snapshot_schema_tests {
    use super::*;

    #[derive(Serialize)]
    #[serde(bound = "")]
    struct SnapshotWithoutSchema {
        last_applied_log: Option<LogIdOf<TypeConfig>>,
        last_membership: StoredMembershipOf<TypeConfig>,
        graphs: Vec<GraphSnapshot>,
        native_history: Vec<NativeHistoryEntry>,
    }

    #[derive(Serialize)]
    struct GraphWithoutDurableRows {
        schema_version: u16,
        fname: String,
    }

    #[test]
    fn current_snapshot_schema_is_required_and_round_trips() {
        let body = SmSnapshotData {
            schema_version: RAFT_SNAPSHOT_SCHEMA_VERSION,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            graphs: Vec::new(),
            native_history: Vec::new(),
        };
        let encoded = rmp_serde::to_vec_named(&body).unwrap();
        let decoded: SmSnapshotData = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.schema_version, RAFT_SNAPSHOT_SCHEMA_VERSION);

        let incomplete = SnapshotWithoutSchema {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            graphs: Vec::new(),
            native_history: Vec::new(),
        };
        let encoded = rmp_serde::to_vec_named(&incomplete).unwrap();
        assert!(rmp_serde::from_slice::<SmSnapshotData>(&encoded).is_err());
    }

    #[test]
    fn graph_snapshot_requires_durable_rows_and_their_schema() {
        let incomplete = GraphWithoutDurableRows {
            schema_version: RAFT_SNAPSHOT_SCHEMA_VERSION,
            fname: "graph".to_string(),
        };
        let encoded = rmp_serde::to_vec_named(&incomplete).unwrap();
        assert!(rmp_serde::from_slice::<GraphSnapshot>(&encoded).is_err());

        let rows = crate::server::persistence::online_reshard::RawGraphRows {
            schema_version: 0,
            ..Default::default()
        };
        assert!(rows.validate_schema().is_err());

        let mut orphaned = crate::server::persistence::online_reshard::RawGraphRows::default();
        orphaned.nodes.push(("node".to_string(), Vec::new()));
        assert!(orphaned.durable_identity("graph").is_err());

        let durable = crate::server::persistence::online_reshard::RawGraphRows {
            meta: Some(
                crate::redb_store::encode_meta_with_incarnation(
                    "graph",
                    GraphType::Global,
                    "incarnation:test:raft-snapshot",
                )
                .unwrap(),
            ),
            ..Default::default()
        };
        let graph = GraphSnapshot {
            schema_version: RAFT_SNAPSHOT_SCHEMA_VERSION,
            fname: "graph".to_string(),
            durable,
        };
        let encoded = rmp_serde::to_vec_named(&graph).unwrap();
        let decoded: GraphSnapshot = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(
            decoded.validate_and_identity().unwrap(),
            (
                "graph".to_string(),
                GraphType::Global,
                "incarnation:test:raft-snapshot".to_string()
            )
        );
    }
}

/// A held snapshot: its metadata + the serialized body.
type HeldSnapshot = (SnapshotMetaOf<TypeConfig>, Vec<u8>);

/// In-RAM state-machine pointers (the actual graph data lives in GraphCore + M2).
#[derive(Debug, Clone, Default)]
struct StateMachine {
    last_applied_log: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
}

/// The combined Raft storage for ONE group: a durable redb-backed log + the
/// engine-backed state machine. The log, vote and applied-state are persisted in the
/// shared M2 authoritative shard ([`RedbBackend`]), keyed by this store's [`GroupId`].
pub struct EgStore {
    /// This group's id — the composite-key prefix for its log + meta rows.
    group_id: GroupId,
    /// The shared M2 persistence backend — owns the authoritative shard and its group-commit
    /// writer. Held as the trait object (the same `Arc` `ServerState` holds); the
    /// concrete [`RedbBackend`] is recovered via [`PersistenceBackend::as_redb`] so
    /// the log rides the SAME writer/transaction as the M2 graph mutations.
    backend: Arc<dyn PersistenceBackend>,
    last_purged_log_id: RwLock<Option<LogIdOf<TypeConfig>>>,
    committed: RwLock<Option<LogIdOf<TypeConfig>>>,
    vote: RwLock<Option<VoteOf<TypeConfig>>>,
    sm: RwLock<StateMachine>,
    current_snapshot: RwLock<Option<HeldSnapshot>>,
    /// Successful encrypted native commands keyed by committed log index. This is
    /// the replay image for stores outside the authoritative shard and is captured atomically
    /// with graph rows by `apply_snapshot_gate`.
    native_history: RwLock<BTreeSet<u64>>,
    snapshot_idx: parking_lot::Mutex<u64>,
    /// Serializes one state-machine apply (including its applied pointer) with
    /// snapshot capture/install, so graph rows and auxiliary authority describe
    /// the exact same committed prefix.
    apply_snapshot_gate: Mutex<()>,
    /// Engine context: registry + persistence the state machine applies into.
    ctx: AppCtx,
}

impl EgStore {
    /// Open the store for `group_id`, recovering the durable vote + applied state +
    /// last-purged pointer from the shared authoritative shard (keyed by group id). The
    /// graph DATA is recovered separately by the M2 `load_all` path before Raft
    /// starts, so on boot the applied pointers and the on-disk graph data agree.
    pub fn open(
        group_id: GroupId,
        backend: Arc<dyn PersistenceBackend>,
        ctx: AppCtx,
    ) -> Result<Arc<Self>, String> {
        let redb = backend
            .as_redb()
            .ok_or_else(|| "raft requires the redb persistence backend".to_string())?;
        let vote = match redb.raft_meta_get(group_id, KEY_VOTE)? {
            Some(b) => decode_raft_value(&b, MAX_RAFT_META_BYTES, 100_000)?,
            None => None,
        };
        let applied: AppliedState = match redb.raft_meta_get(group_id, KEY_APPLIED)? {
            Some(b) => decode_raft_value(&b, MAX_RAFT_META_BYTES, 100_000)?,
            None => AppliedState::default(),
        };
        let purged: Option<LogIdOf<TypeConfig>> = match redb.raft_meta_get(group_id, KEY_PURGED)? {
            Some(b) => decode_raft_value(&b, MAX_RAFT_META_BYTES, 100_000)?,
            None => None,
        };
        let mut native_history = BTreeSet::new();
        if let Some(last_applied) = applied.last_applied_log {
            let last_chunk = last_applied.index / NATIVE_HISTORY_BITMAP_BITS;
            for chunk in 0..=last_chunk {
                let Some(bitmap) =
                    redb.raft_meta_get(group_id, &native_history_bitmap_key(chunk))?
                else {
                    continue;
                };
                if bitmap.len() != NATIVE_HISTORY_BITMAP_BYTES {
                    return Err("persisted native history bitmap is invalid".to_string());
                }
                for (byte_index, byte) in bitmap.into_iter().enumerate() {
                    for bit in 0..8u8 {
                        if byte & (1u8 << bit) == 0 {
                            continue;
                        }
                        let index = chunk * NATIVE_HISTORY_BITMAP_BITS
                            + (byte_index as u64) * 8
                            + u64::from(bit);
                        if index > last_applied.index {
                            return Err(
                                "persisted native history exceeds applied state".to_string()
                            );
                        }
                        let bytes = redb
                            .raft_meta_get(group_id, &native_history_key(index))?
                            .ok_or_else(|| {
                                "persisted native history bitmap has no command".to_string()
                            })?;
                        let request: RaftRequest = decode_raft_value(
                            &bytes,
                            MAX_RAFT_LOG_ENTRY_BYTES,
                            MAX_RAFT_LOG_ITEMS,
                        )?;
                        if !is_replayable_native_request(&request) {
                            return Err("persisted native history entry is invalid".to_string());
                        }
                        native_history.insert(index);
                    }
                }
            }
        }
        Ok(Arc::new(Self {
            group_id,
            backend,
            last_purged_log_id: RwLock::new(purged),
            committed: RwLock::new(None),
            vote: RwLock::new(vote),
            sm: RwLock::new(StateMachine {
                last_applied_log: applied.last_applied_log,
                last_membership: applied.last_membership,
            }),
            current_snapshot: RwLock::new(None),
            native_history: RwLock::new(native_history),
            snapshot_idx: parking_lot::Mutex::new(0),
            apply_snapshot_gate: Mutex::new(()),
            ctx,
        }))
    }

    /// The concrete redb backend (the raft store is only constructed over redb).
    fn redb(&self) -> &RedbBackend {
        self.backend
            .as_redb()
            .expect("raft store backend is always redb (checked at open)")
    }

    async fn persist_vote(&self, vote: &VoteOf<TypeConfig>) -> Result<(), String> {
        let b = rmp_serde::to_vec_named(vote).map_err(|e| e.to_string())?;
        self.redb().raft_meta_put(self.group_id, KEY_VOTE, b).await
    }

    async fn persist_applied(&self, sm: &StateMachine) -> Result<(), String> {
        let a = AppliedState {
            last_applied_log: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
        };
        let b = rmp_serde::to_vec_named(&a).map_err(|e| e.to_string())?;
        self.redb()
            .raft_meta_put(self.group_id, KEY_APPLIED, b)
            .await
    }

    async fn persist_native_history_entry(
        &self,
        log_index: u64,
        request: &RaftRequest,
    ) -> Result<(), String> {
        if !is_replayable_native_request(request) {
            return Err("attempted to persist a non-native replay entry".to_string());
        }
        let bytes = rmp_serde::to_vec_named(request).map_err(|error| error.to_string())?;
        validate_raft_value(&bytes, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)?;
        self.redb()
            .raft_meta_put(self.group_id, &native_history_key(log_index), bytes)
            .await?;
        let chunk = log_index / NATIVE_HISTORY_BITMAP_BITS;
        let offset = log_index % NATIVE_HISTORY_BITMAP_BITS;
        let bitmap_key = native_history_bitmap_key(chunk);
        let mut bitmap = match self.redb().raft_meta_get(self.group_id, &bitmap_key)? {
            Some(value) if value.len() == NATIVE_HISTORY_BITMAP_BYTES => value,
            Some(_) => return Err("persisted native history bitmap is invalid".to_string()),
            None => vec![0; NATIVE_HISTORY_BITMAP_BYTES],
        };
        bitmap[(offset / 8) as usize] |= 1u8 << (offset % 8);
        self.redb()
            .raft_meta_put(self.group_id, &bitmap_key, bitmap)
            .await
    }

    /// Apply ONE committed request to the engine. Every ordinary method is staged
    /// against the authoritative image and committed as one state-backed
    /// MutationBatch before RAM publication. A ChangeEnvelope invokes its richer
    /// native atomic kernel as one state-machine command.
    ///
    /// Explicitly boxed (rather than a bare `async fn`) so its return type is a
    /// concrete `Pin<Box<dyn Future>>` instead of an inferred opaque type: this
    /// function is the shared, ~500-line body BOTH `apply` and `install_snapshot`
    /// (the two huge `RaftStateMachine` trait-method coroutines above) call, and an
    /// opaque return type here entangles their two independent `Send`-bound
    /// computations into a single rustc query cycle (`error[E0391]: cycle detected
    /// ... coroutine witness ... Send`) once enough unrelated type-checking work
    /// exists elsewhere in the crate to shift query evaluation order — the standard,
    /// behavior-preserving fix for this class of async-fn-shared-by-two-trait-impls
    /// cycle is to make the shared callee's future type explicit.
    fn apply_request<'a>(
        &'a self,
        req: &'a RaftRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RaftResponse, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            req.validate()?;
            let server_secret = self.ctx.state.read().await.auth_secret.clone();

            // Every bounded native family executes only after its command is committed.
            // Re-entering the domain dispatcher under the replicated-apply scope reuses
            // the existing MutationBatch/saga kernels while suppressing any nested Raft
            // proposal and replacing local wall-clock reads with the leader-selected
            // commit time. The reconstructed authority contains only opaque scopes.
            if let ReplicatedMutation::Native { command } = &req.command {
                match command {
                    NativeMutationCommand::TransactionParticipant {
                        phase,
                        coordinator_id,
                        participant_id,
                        ..
                    } => {
                        let plan = command.open_transaction_plan(&server_secret)?;
                        let outcome = crate::server::apply_replicated_transaction_participant(
                            &self.ctx.state,
                            req.mutation.request_id,
                            req.committed_at_ms,
                            &req.mutation,
                            self.group_id,
                            *phase,
                            crate::server::ReplicatedParticipantRef {
                                coordinator_id,
                                participant_id: *participant_id,
                                plan: plan.as_deref(),
                            },
                        )
                        .await;
                        return Ok(match outcome {
                            Ok(value) => RaftResponse {
                                applied: true,
                                native_result: Some(crate::protocol::ResultPayload::Bool(value)),
                                ..Default::default()
                            },
                            Err(error) => RaftResponse {
                                applied: true,
                                native_error: Some(error),
                                ..Default::default()
                            },
                        });
                    }
                    NativeMutationCommand::TransactionDecision {
                        coordinator_id,
                        commit,
                    } => {
                        let outcome = crate::server::apply_replicated_transaction_decision(
                            &self.ctx.state,
                            req.committed_at_ms,
                            &req.mutation,
                            coordinator_id,
                            *commit,
                        )
                        .await;
                        return Ok(match outcome {
                            Ok(value) => RaftResponse {
                                applied: true,
                                native_result: Some(crate::protocol::ResultPayload::Bool(value)),
                                ..Default::default()
                            },
                            Err(error) => RaftResponse {
                                applied: true,
                                native_error: Some(error),
                                ..Default::default()
                            },
                        });
                    }
                    NativeMutationCommand::TransactionFinalize {
                        coordinator_id,
                        commit,
                    } => {
                        let outcome = crate::server::apply_replicated_transaction_finalize(
                            &self.ctx.state,
                            req.committed_at_ms,
                            &req.mutation,
                            coordinator_id,
                            *commit,
                        )
                        .await;
                        return Ok(match outcome {
                            Ok(value) => RaftResponse {
                                applied: true,
                                native_result: Some(crate::protocol::ResultPayload::Bool(value)),
                                ..Default::default()
                            },
                            Err(error) => RaftResponse {
                                applied: true,
                                native_error: Some(error),
                                ..Default::default()
                            },
                        });
                    }
                    #[cfg(feature = "jobs")]
                    NativeMutationCommand::JobPublicationCommit { coordinator_id, .. } => {
                        let plan = command.open_job_publication_payload(&server_secret)?;
                        let outcome = crate::server::apply_replicated_job_publication_commit(
                            &self.ctx.state,
                            req.mutation.request_id,
                            req.committed_at_ms,
                            &req.mutation,
                            self.group_id,
                            coordinator_id,
                            &plan,
                        )
                        .await;
                        return Ok(match outcome {
                            Ok(value) => RaftResponse {
                                applied: true,
                                native_result: Some(crate::protocol::ResultPayload::Bool(value)),
                                ..Default::default()
                            },
                            Err(error) => RaftResponse {
                                applied: true,
                                native_error: Some(error),
                                ..Default::default()
                            },
                        });
                    }
                    #[cfg(feature = "jobs")]
                    NativeMutationCommand::JobPublicationFinalize { coordinator_id, .. } => {
                        let receipt = command.open_job_publication_payload(&server_secret)?;
                        let outcome = crate::server::apply_replicated_job_publication_finalize(
                            &self.ctx.state,
                            req.committed_at_ms,
                            &req.mutation,
                            coordinator_id,
                            &receipt,
                        )
                        .await;
                        return Ok(match outcome {
                            Ok(result) => RaftResponse {
                                applied: true,
                                native_result: Some(result),
                                ..Default::default()
                            },
                            Err(error) => RaftResponse {
                                applied: true,
                                native_error: Some(error),
                                ..Default::default()
                            },
                        });
                    }
                    _ => {}
                }
                if let Some(method) = command.open_public_method(&server_secret)? {
                    if crate::server::txn::consensus_graph_is_prepared(&req.graph_name)
                        || crate::server::txn::consensus_control_conflicts(&method)
                    {
                        return Ok(RaftResponse {
                            applied: true,
                            native_error: Some(
                                "graph is reserved by a prepared consensus transaction".to_string(),
                            ),
                            ..Default::default()
                        });
                    }
                    let response = if matches!(method, Method::Commit { .. }) {
                        let Method::Commit { txn_id } = method else {
                            unreachable!();
                        };
                        crate::server::apply_replicated_transaction_prepare(
                            &self.ctx.state,
                            req.mutation.request_id,
                            req.committed_at_ms,
                            &req.mutation,
                            &txn_id,
                        )
                        .await
                    } else {
                        crate::server::apply_replicated_native(
                            &self.ctx.state,
                            req.graph_name.clone(),
                            req.mutation.request_id,
                            req.committed_at_ms,
                            &req.mutation,
                            method,
                        )
                        .await
                    };
                    return Ok(RaftResponse {
                        applied: true,
                        native_result: response.result,
                        native_error: response.error,
                        ..Default::default()
                    });
                }
            }

            if crate::server::txn::consensus_graph_is_prepared(&req.graph_name) {
                return Ok(RaftResponse {
                    applied: true,
                    native_error: Some(
                        "graph is reserved by a prepared consensus transaction".to_string(),
                    ),
                    ..Default::default()
                });
            }

            // Resolve the target graph's RESIDENT core. Mirrors the live dispatch
            // cold-path: a follower replaying a CreateGraph->write sequence may find the
            // graph catalog-known but not yet materialized (evicted mid-replay, or
            // catalog-only after a restart) -- `exists()` true but `get()` None. Lazy-open
            // it FIRST (a no-op for a genuinely-unknown name, so a follower's first sight
            // of a brand-new graph still falls through to create), then create only if
            // genuinely absent. Fixes the follower catch-up "missing after create" apply
            // failure that stalls a lagging node from ever finishing catch-up.
            #[cfg(feature = "redb")]
            {
                let miss = {
                    let s = self.ctx.state.read().await;
                    s.registry.get(&req.graph_name).is_none()
                };
                if miss {
                    let cap = crate::server::persistence::cold_offload::max_resident_graphs();
                    let page_size = crate::server::persistence::cold_offload::lazy_open_page_size();
                    crate::server::persistence::cold_offload::lazy_open(
                        &self.ctx.state,
                        &req.graph_name,
                        cap,
                        page_size,
                    )
                    .await;
                }
            }
            let (core, persistence) = {
                let mut s = self.ctx.state.write().await;
                if !s.registry.exists(&req.graph_name) {
                    s.registry
                        .create_graph(&req.graph_name, req.graph_type, None)
                        .map_err(|e| {
                            format!("graph '{}' create failed on replay: {e}", req.graph_name)
                        })?;
                }
                let core = match s.registry.get(&req.graph_name).map(|e| e.core.clone()) {
                    Some(c) => c,
                    None => return Err(format!("graph '{}' missing after create", req.graph_name)),
                };
                (core, s.persistence.clone())
            };

            let graph_method = req.command.open_graph(&server_secret)?;
            if let Some(method) = graph_method.as_ref() {
                if !crate::mutation_apply::is_durable_mutation(method)
                    || crate::server::mutation_batch::is_work_item_method(method)
                {
                    return Err(
                        "Raft graph command is not a deterministic replicated mutation".to_string(),
                    );
                }
            }
            let change_envelope = req.command.open_change_envelope(&server_secret)?;
            #[cfg(feature = "modality-serving")]
            let modality_command = match &req.command {
                ReplicatedMutation::Native {
                    command: NativeMutationCommand::ServedModality { command },
                } => {
                    command.validate(&server_secret)?;
                    Some(command)
                }
                _ => None,
            };

            // ChangeEnvelope is one atomic state-machine command. Decomposing it into
            // graph operations would lose its content-version, cursor, governance,
            // lineage, evidence, and outbox authority.
            if let Some(envelope) = change_envelope.as_ref() {
                if envelope.mutation.graph != req.graph_name {
                    return Err(
                        "replicated ChangeEnvelope graph does not match request authority"
                            .to_string(),
                    );
                }
                let expected_tenant_scope = crate::server::mutation_batch::opaque_coordinator_key(
                    "carrier-tenant",
                    "verified",
                    &envelope.mutation.tenant,
                );
                if req.mutation.batch_id != envelope.mutation.batch_id
                    || req.mutation.request_id != envelope.mutation.context.request_id
                    || req.mutation.tenant_scope != expected_tenant_scope
                    || req.mutation.principal_fingerprint != envelope.mutation.context.principal
                    || req.mutation.placement_epoch != envelope.mutation.placement_epoch
                    || req.mutation.fencing_token != envelope.mutation.fencing_token
                    || req.mutation.created_at_ms != envelope.mutation.created_at_ms
                {
                    return Err(
                        "replicated ChangeEnvelope does not match its mutation authority"
                            .to_string(),
                    );
                }
                let committed_at_ms = req.committed_at_ms;
                let backend = persistence.as_ref().ok_or_else(|| {
                    "replicated ChangeEnvelope requires a configured persistence backend"
                        .to_string()
                })?;
                let committed = backend
                    .commit_change_envelope(&req.graph_fname, envelope, committed_at_ms)
                    .await?;
                let projection_pending = if committed.replayed {
                    false
                } else {
                    match crate::server::mutation_batch::publish_change_envelope_projection(
                        &core, envelope,
                    ) {
                        Ok(()) => false,
                        Err(error) => {
                            tracing::warn!(
                                graph = %req.graph_fname,
                                error = %error,
                                "replicated ChangeEnvelope projection queued for repair"
                            );
                            true
                        }
                    }
                };
                return Ok(RaftResponse {
                    applied: true,
                    change_envelope_commit: Some(committed),
                    projection_pending,
                    ..Default::default()
                });
            }

            let persistence = persistence.ok_or_else(|| {
                "ordinary replicated mutation requires a configured persistence backend".to_string()
            })?;

            #[cfg(feature = "modality-serving")]
            let durable_method = modality_command
                .map(super::SanitizedModalityRaftCommand::receipt_method)
                .or_else(|| graph_method.clone())
                .ok_or_else(|| "replicated native command has no receipt method".to_string())?;
            #[cfg(not(feature = "modality-serving"))]
            let durable_method = graph_method
                .clone()
                .ok_or_else(|| "replicated native command has no receipt method".to_string())?;

            use sha2::{Digest, Sha256};
            let authority = &req.mutation;
            let batch_id = authority.batch_id.as_str();
            let expected_principal = authority.principal_fingerprint.as_str();

            // Phase-2 recovery intentionally submits the same deterministic child
            // authority again.  Resolve that receipt BEFORE staging from today's graph
            // image; otherwise its authoritative-state digest would differ and turn a
            // valid replay into an idempotency conflict.
            if let Some(record) = persistence
                .read_mutation_batch(&req.graph_fname, batch_id)
                .await?
            {
                let encoded_method =
                    rmp_serde::to_vec_named(&durable_method).map_err(|error| error.to_string())?;
                let expected_digest =
                    format!("sha256:{}", hex::encode(Sha256::digest(&encoded_method)));
                let operation_matches = record.batch.operations.len() == 1
                    && matches!(
                        &record.batch.operations[0].method,
                        crate::protocol::Method::ApplyMutation { event_type, query }
                            if event_type == "authoritative_state_operation"
                                && query == &expected_digest
                    );
                #[cfg(feature = "modality-serving")]
                let expected_result = if let Some(command) = modality_command.as_ref() {
                    command.result_msgpack.clone()
                } else {
                    rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true))
                        .map_err(|error| error.to_string())?
                };
                #[cfg(not(feature = "modality-serving"))]
                let expected_result =
                    rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true))
                        .map_err(|error| error.to_string())?;
                if record.status != crate::mutation_batch::MutationBatchStatus::Committed
                    || record.batch.batch_id != batch_id
                    || record.batch.tenant != authority.tenant_scope
                    || record.batch.graph != req.graph_name
                    || record.batch.context.principal != expected_principal
                    || !operation_matches
                    || record.result_msgpack.as_deref() != Some(expected_result.as_slice())
                {
                    return Err(
                        "replicated child receipt conflicts with replay authority".to_string()
                    );
                }
                let (snapshot, version) = persistence
                    .read_authoritative_graph_snapshot(&req.graph_fname)
                    .await?
                    .ok_or_else(|| "committed replicated graph image is missing".to_string())?;
                core.install_committed_snapshot(snapshot, version)?;
                return Ok(RaftResponse {
                    applied: true,
                    ..Default::default()
                });
            }

            // Stage from the durable pre-image. This handles the complete mutation
            // vocabulary (including runtime-result/multi-row methods) without applying a
            // speculative write to the serving projection.
            let (base_snapshot, source_version) = match persistence
                .read_authoritative_graph_snapshot(&req.graph_fname)
                .await?
            {
                Some(value) => value,
                None => {
                    let version = persistence
                        .read_mutation_graph_version(&req.graph_fname)
                        .await?
                        .unwrap_or_else(|| core.version());
                    (core.snapshot(), version)
                }
            };
            let staged = crate::graph::GraphCore::from_snapshot(base_snapshot, source_version)?;
            #[cfg(feature = "modality-serving")]
            if let Some(command) = modality_command.as_ref() {
                staged.add_node(
                    command.node_id.clone(),
                    command.sealed_runtime_state.clone(),
                );
            } else {
                crate::mutation_apply::apply(
                    &staged,
                    graph_method.as_ref().ok_or_else(|| {
                        "replicated graph mutation is missing its typed method".to_string()
                    })?,
                );
            }
            #[cfg(not(feature = "modality-serving"))]
            crate::mutation_apply::apply(
                &staged,
                graph_method.as_ref().ok_or_else(|| {
                    "replicated graph mutation is missing its typed method".to_string()
                })?,
            );
            let staged_snapshot = staged.snapshot();
            let state_msgpack = staged_snapshot.to_msgpack()?;
            let descriptor = crate::mutation_batch::MutationStateDescriptor {
                algorithm: "sha256".to_string(),
                digest: hex::encode(Sha256::digest(&state_msgpack)),
                source_graph_version: source_version,
                target_graph_version: source_version.saturating_add(1),
            };
            let created_at_ms = authority.created_at_ms;
            // Resolve BEFORE `durable_method` is moved into `compile_methods` below --
            // `compile_methods` erases it into an opaque state receipt (see
            // `redb_store::commit_mutation_batch_inner`'s doc comment), so the
            // policy-audited answer for the REAL replicated method must be captured
            // here or it becomes unrecoverable.
            let audited = eg_capabilities::policy(&durable_method).audited;
            let batch = crate::server::mutation_batch::compile_methods(
                crate::server::mutation_batch::CompileBatch {
                    batch_id,
                    request_id: authority.request_id,
                    principal: Some(expected_principal),
                    tenant: &authority.tenant_scope,
                    graph: &req.graph_name,
                    placement_epoch: authority.placement_epoch,
                    idempotency_key: batch_id,
                    expected_graph_version: Some(source_version),
                    fencing_token: authority.fencing_token,
                    created_at_ms,
                    default_surface: crate::mutation_batch::MutationSurface::Graph,
                    authoritative_state: Some(descriptor),
                },
                vec![durable_method],
            )?;
            batch.validate()?;
            #[cfg(feature = "modality-serving")]
            let result = if let Some(command) = modality_command.as_ref() {
                command.result_msgpack.clone()
            } else {
                rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true))
                    .map_err(|error| error.to_string())?
            };
            #[cfg(not(feature = "modality-serving"))]
            let result = rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true))
                .map_err(|error| error.to_string())?;
            let committed = persistence
                .commit_mutation_batch_state(
                    &req.graph_fname,
                    &batch,
                    state_msgpack,
                    Some(&result),
                    created_at_ms,
                    audited,
                )
                .await?;
            if committed.replayed {
                let (snapshot, version) = persistence
                    .read_authoritative_graph_snapshot(&req.graph_fname)
                    .await?
                    .ok_or_else(|| "committed replicated graph image is missing".to_string())?;
                core.install_committed_snapshot(snapshot, version)?;
            } else {
                core.prepare_snapshot_publish(staged_snapshot, source_version)?;
                core.mark_dirty();
            }
            #[cfg(all(feature = "modality-serving", feature = "streaming"))]
            if !committed.replayed {
                if let Some(command) = modality_command.as_ref() {
                    let hub = self.ctx.state.read().await.cdc.clone();
                    if let Some(hub) = hub.as_ref() {
                        crate::server::cdc::emit_served_modality(
                            hub,
                            &req.graph_name,
                            command.modality,
                        );
                    }
                }
            }
            Ok(RaftResponse {
                applied: true,
                ..Default::default()
            })
        })
    }

    /// Dump THIS group's graphs for a snapshot (CONCEPT:AU-KG.ingest.staged). When the store runs
    /// under a [`super::multi::MultiRaft`] its ctx carries the router, so the dump is
    /// SCOPED to graphs whose tenant range resolves to this group — a large tenant in
    /// one group never bloats another group's snapshot. Enumeration uses the complete
    /// durable catalog, not only resident cores, so an evicted cold graph remains part
    /// of the state-machine image. Without a router (a direct single-store open) the
    /// whole registry is dumped (the unscoped scaffold path).
    async fn dump_graphs(&self) -> Result<Vec<GraphSnapshot>, String> {
        let identities: Vec<(String, GraphType, String)> = {
            let s = self.ctx.state.read().await;
            s.registry
                .list()
                .into_iter()
                .filter(|(name, _)| match &self.ctx.router {
                    Some(router) => router.group_of(name) == self.group_id,
                    None => true,
                })
                .map(|(name, graph_type)| {
                    let fname = crate::persist::sanitize(&name);
                    (name, graph_type, fname)
                })
                .collect()
        };
        let mut graphs = Vec::with_capacity(identities.len());
        for (expected_name, expected_type, fname) in identities {
            let graph = GraphSnapshot {
                schema_version: RAFT_SNAPSHOT_SCHEMA_VERSION,
                durable: self.redb().export_graph_raw_for_snapshot(&fname).await?,
                fname,
            };
            let (durable_name, durable_type, _) = graph.validate_and_identity()?;
            if durable_name != expected_name || durable_type != expected_type {
                return Err(format!(
                    "Raft snapshot registry identity for '{}' disagrees with durable authority",
                    expected_name
                ));
            }
            graphs.push(graph);
        }
        Ok(graphs)
    }

    /// Test-only: the sorted graph NAMES this group's snapshot would capture, AFTER
    /// per-group scoping (CONCEPT:AU-KG.ingest.staged). Lets a test assert a group's snapshot
    /// carries ONLY its own tenant-range graphs without reaching into private types.
    #[cfg(test)]
    pub(crate) async fn scoped_snapshot_graph_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .dump_graphs()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|g| g.validate_and_identity().ok().map(|identity| identity.0))
            .collect();
        names.sort();
        names
    }

    fn validate_snapshot_graphs(
        &self,
        graphs: &[GraphSnapshot],
    ) -> Result<Vec<(String, GraphType, String)>, String> {
        let mut names = std::collections::BTreeSet::new();
        let mut fnames = std::collections::BTreeSet::new();
        let mut identities = Vec::with_capacity(graphs.len());
        for graph in graphs {
            let (name, graph_type, incarnation_id) = graph.validate_and_identity()?;
            if !names.insert(name.clone()) || !fnames.insert(graph.fname.clone()) {
                return Err("Raft graph snapshot identity is invalid or duplicated".to_string());
            }
            if self
                .ctx
                .router
                .as_ref()
                .is_some_and(|router| router.group_of(&name) != self.group_id)
            {
                return Err("Raft graph snapshot contains a graph from another group".to_string());
            }
            identities.push((name, graph_type, incarnation_id));
        }
        Ok(identities)
    }

    /// Rebuild graphs from a snapshot into the registry + M2 store.
    async fn install_graphs(&self, graphs: &[GraphSnapshot]) -> Result<(), String> {
        let identities = self.validate_snapshot_graphs(graphs)?;
        let names: std::collections::BTreeSet<&str> = identities
            .iter()
            .map(|(name, _, _)| name.as_str())
            .collect();

        // Identify stale graph authority before the first durable import. Installing
        // a Raft snapshot is replacement, not a merge: graphs in this group but
        // absent from the committed image must not survive a rollback/rejoin.
        let stale_names = {
            let s = self.ctx.state.read().await;
            let stale: Vec<String> = s
                .registry
                .list()
                .into_iter()
                .map(|(name, _)| name)
                .filter(|name| {
                    let belongs_to_group = match &self.ctx.router {
                        Some(router) => router.group_of(name) == self.group_id,
                        None => true,
                    };
                    belongs_to_group && !names.contains(name.as_str())
                })
                .collect();
            if stale.iter().any(|name| name == "__commons__") {
                return Err("Raft snapshot omits the mandatory commons graph".to_string());
            }
            stale
        };

        for (g, (name, graph_type, incarnation_id)) in graphs.iter().zip(&identities) {
            self.redb()
                .import_graph_raw_from_snapshot(&g.fname, g.durable.clone())
                .await?;
            let (snapshot, version) = self
                .backend
                .read_authoritative_graph_snapshot(&g.fname)
                .await?
                .ok_or_else(|| format!("snapshot graph '{}' has no durable image", name))?;
            let mut s = self.ctx.state.write().await;
            let owner = s.registry.catalog_record(name).and_then(|record| {
                if record.incarnation_id == incarnation_id.as_str() {
                    record.owner
                } else {
                    None
                }
            });
            let core = s.registry.install_committed_graph(
                name,
                *graph_type,
                owner,
                incarnation_id.clone(),
                snapshot,
                version,
            )?;
            s.write_coalescer.remove(name);
            s.per_graph_inflight.remove(name);
            #[cfg(feature = "redb")]
            s.cold_tracker.forget(name);
            crate::metrics::set_graph_size(
                name,
                i64::try_from(core.node_count()).unwrap_or(i64::MAX),
                i64::try_from(core.edge_count()).unwrap_or(i64::MAX),
            );
        }

        for name in stale_names {
            let fname = crate::persist::sanitize(&name);
            self.redb()
                .import_graph_raw_from_snapshot(
                    &fname,
                    crate::server::persistence::online_reshard::RawGraphRows::default(),
                )
                .await?;
            let mut s = self.ctx.state.write().await;
            if s.registry.exists(&name) {
                s.registry.delete_graph(&name)?;
            }
            s.write_coalescer.remove(&name);
            s.per_graph_inflight.remove(&name);
            #[cfg(feature = "redb")]
            s.cold_tracker.forget(&name);
            crate::metrics::drop_graph(&name);
        }
        Ok(())
    }

    /// Read ONE stored log entry by index from redb (helper for `get_log_state`).
    fn read_one_entry(&self, idx: u64) -> Result<Option<EntryOf<TypeConfig>>, String> {
        let blobs = self.redb().raft_log_read(self.group_id, idx, idx)?;
        match blobs.into_iter().next() {
            Some(b) => Ok(Some(decode_raft_value(
                &b,
                MAX_RAFT_LOG_ENTRY_BYTES,
                MAX_RAFT_LOG_ITEMS,
            )?)),
            None => Ok(None),
        }
    }
}

/// Translate an arbitrary `RangeBounds<u64>` into the inclusive `[lo, hi]` redb
/// scans on (a saturating-bounded variant of) the requested range.
fn inclusive_bounds<RB: RangeBounds<u64>>(range: &RB) -> (u64, u64) {
    let lo = match range.start_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => i.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => i.saturating_sub(1),
        Bound::Unbounded => u64::MAX,
    };
    (lo, hi)
}

impl RaftLogReader<TypeConfig> for Arc<EgStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        let (lo, hi) = inclusive_bounds(&range);
        if lo > hi {
            return Ok(Vec::new());
        }
        let blobs = self
            .redb()
            .raft_log_read(self.group_id, lo, hi)
            .map_err(ioerr)?;
        if blobs.len() > MAX_RAFT_LOG_BATCH_ENTRIES
            || blobs
                .iter()
                .try_fold(0usize, |total, blob| total.checked_add(blob.len()))
                .is_none_or(|total| total > MAX_RAFT_SNAPSHOT_BYTES)
        {
            return Err(ioerr("raft log read exceeds resource limits"));
        }
        let mut out = Vec::with_capacity(blobs.len());
        for b in blobs {
            let e: EntryOf<TypeConfig> =
                decode_raft_value(&b, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)
                    .map_err(ioerr)?;
            out.push(e);
        }
        Ok(out)
    }

    /// The last saved vote (moved onto [`RaftLogReader`] in openraft 0.10).
    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(*self.vote.read().await)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<EgStore> {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig>, io::Error> {
        let _snapshot_gate = self.apply_snapshot_gate.lock().await;
        let (last_applied_log, last_membership) = {
            let sm = self.sm.read().await;
            (sm.last_applied_log, sm.last_membership.clone())
        };
        let graphs = self.dump_graphs().await.map_err(ioerr)?;
        let native_indexes: Vec<u64> = self.native_history.read().await.iter().copied().collect();
        let mut native_history = Vec::with_capacity(native_indexes.len());
        for log_index in native_indexes {
            let bytes = self
                .redb()
                .raft_meta_get(self.group_id, &native_history_key(log_index))
                .map_err(ioerr)?
                .ok_or_else(|| ioerr("native snapshot history command is missing"))?;
            let request: RaftRequest =
                decode_raft_value(&bytes, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)
                    .map_err(ioerr)?;
            if !is_replayable_native_request(&request) {
                return Err(ioerr("native snapshot history command is invalid"));
            }
            native_history.push(NativeHistoryEntry { log_index, request });
        }
        let body = SmSnapshotData {
            schema_version: RAFT_SNAPSHOT_SCHEMA_VERSION,
            last_applied_log,
            last_membership: last_membership.clone(),
            graphs,
            native_history,
        };
        let data = rmp_serde::to_vec_named(&body).map_err(ioerr)?;
        validate_raft_value(&data, MAX_RAFT_SNAPSHOT_BYTES, MAX_RAFT_SNAPSHOT_ITEMS)
            .map_err(ioerr)?;

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock();
            *l += 1;
            *l
        };
        let snapshot_id = match &last_applied_log {
            Some(last) => format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx),
            None => format!("--{}", snapshot_idx),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        *self.current_snapshot.write().await = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftLogStorage<TypeConfig> for Arc<EgStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let (_, last_idx) = self.redb().raft_log_bounds(self.group_id).map_err(ioerr)?;
        let last_purged = *self.last_purged_log_id.read().await;
        // Reconstruct the last log id from the stored entry (redb holds the Entry),
        // so a restart knows its log tail WITHOUT the leader.
        let last_log_id = match last_idx {
            Some(i) => match self.read_one_entry(i).map_err(ioerr)? {
                Some(e) => Some(e.log_id()),
                None => last_purged,
            },
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        self.persist_vote(vote).await.map_err(ioerr)?;
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        *self.committed.write().await = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        Ok(*self.committed.read().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        for entry in entries {
            if batch.len() >= MAX_RAFT_LOG_BATCH_ENTRIES {
                return Err(ioerr("raft log batch exceeds resource limits"));
            }
            let blob = rmp_serde::to_vec_named(&entry).map_err(ioerr)?;
            validate_raft_value(&blob, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)
                .map_err(ioerr)?;
            batch_bytes = batch_bytes
                .checked_add(blob.len())
                .filter(|total| *total <= MAX_RAFT_SNAPSHOT_BYTES)
                .ok_or_else(|| ioerr("raft log batch exceeds resource limits"))?;
            batch.push((entry.log_id().index, blob));
        }
        // Durable append: rides the SAME group-commit transaction as any concurrent
        // M2 graph mutation (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — one fsync covers both. Our append is
        // synchronously durable, so we fire the 0.10 `IOFlushed` callback the moment
        // the group-commit fsync resolves (openraft treats the entry as on-disk then).
        match self.redb().raft_log_append(self.group_id, batch).await {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let err = ioerr(&e);
                callback.io_completed(Err(ioerr(&e)));
                Err(err)
            }
        }
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        // Delete every entry AFTER `last_log_id` (exclusive). `None` ⇒ wipe the whole
        // log. `raft_log_delete_from(from)` removes index >= from.
        let from = match last_log_id {
            Some(id) => id.index + 1,
            None => 0,
        };
        self.redb()
            .raft_log_delete_from(self.group_id, from)
            .await
            .map_err(ioerr)
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        {
            let mut ld = self.last_purged_log_id.write().await;
            if ld.as_ref().map(|l| l.index) < Some(log_id.index) {
                *ld = Some(log_id);
            }
        }
        let b = rmp_serde::to_vec_named(&Some(log_id)).map_err(ioerr)?;
        self.redb()
            .raft_meta_put(self.group_id, KEY_PURGED, b)
            .await
            .map_err(ioerr)?;
        self.redb()
            .raft_log_purge_upto(self.group_id, log_id.index)
            .await
            .map_err(ioerr)
    }
}

impl RaftStateMachine<TypeConfig> for Arc<EgStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let _apply_gate = self.apply_snapshot_gate.lock().await;
            let resp = match &entry.payload {
                EntryPayload::Blank => RaftResponse {
                    applied: false,
                    ..Default::default()
                },
                EntryPayload::Normal(req) => self.apply_request(req).await.map_err(ioerr)?,
                EntryPayload::Membership(mem) => {
                    let mut sm = self.sm.write().await;
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    RaftResponse {
                        applied: false,
                        ..Default::default()
                    }
                }
            };
            if resp.native_error.is_none() {
                if let EntryPayload::Normal(request) = &entry.payload {
                    if is_replayable_native_request(request) {
                        self.persist_native_history_entry(entry.log_id.index, request)
                            .await
                            .map_err(ioerr)?;
                        self.native_history.write().await.insert(entry.log_id.index);
                    }
                }
            }
            // Record the applied index (durably) AFTER the effect landed.
            {
                let mut sm = self.sm.write().await;
                sm.last_applied_log = Some(entry.log_id);
                let snapshot = sm.clone();
                drop(sm);
                self.persist_applied(&snapshot).await.map_err(ioerr)?;
            }
            // Send the client response (only present for entries proposed on THIS
            // node as leader — followers get `None`).
            if let Some(responder) = responder {
                responder.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotDataOf<TypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: SnapshotDataOf<TypeConfig>,
    ) -> Result<(), io::Error> {
        let _snapshot_gate = self.apply_snapshot_gate.lock().await;
        let data = snapshot.into_inner();
        let body: SmSnapshotData =
            decode_raft_value(&data, MAX_RAFT_SNAPSHOT_BYTES, MAX_RAFT_SNAPSHOT_ITEMS)
                .map_err(ioerr)?;
        if body.schema_version != RAFT_SNAPSHOT_SCHEMA_VERSION {
            return Err(ioerr(format!(
                "unsupported Raft snapshot schema {} (expected {})",
                body.schema_version, RAFT_SNAPSHOT_SCHEMA_VERSION
            )));
        }
        if meta.last_log_id != body.last_applied_log {
            return Err(ioerr("Raft snapshot metadata does not match its body"));
        }
        // Validate the complete graph and replay manifests before the first state
        // transition. A malformed tail must not leave a valid prefix applied.
        self.validate_snapshot_graphs(&body.graphs).map_err(ioerr)?;
        let server_secret = self.ctx.state.read().await.auth_secret.clone();
        let mut previous_native_index = None;
        for entry in &body.native_history {
            if previous_native_index.is_some_and(|index| index >= entry.log_index)
                || body.last_applied_log.is_none()
                || body
                    .last_applied_log
                    .is_some_and(|last| entry.log_index > last.index)
                || !is_replayable_native_request(&entry.request)
                || self.ctx.router.as_ref().is_some_and(|router| {
                    router.group_of(&entry.request.graph_name) != self.group_id
                })
            {
                return Err(ioerr("Raft native snapshot history is invalid"));
            }
            entry.request.validate().map_err(ioerr)?;
            let ReplicatedMutation::Native { command } = &entry.request.command else {
                return Err(ioerr("Raft native snapshot history is invalid"));
            };
            command
                .validate_replay_authentication(&server_secret)
                .map_err(ioerr)?;
            previous_native_index = Some(entry.log_index);
        }

        let mut native_history = BTreeSet::new();
        for entry in &body.native_history {
            let response = self.apply_request(&entry.request).await.map_err(ioerr)?;
            if let Some(error) = response.native_error {
                return Err(ioerr(format!(
                    "Raft native snapshot replay failed: {error}"
                )));
            }
            self.persist_native_history_entry(entry.log_index, &entry.request)
                .await
                .map_err(ioerr)?;
            native_history.insert(entry.log_index);
        }
        // Materialize the final graph image after native replay. This overwrites
        // graph projections affected by replay with the exact committed prefix.
        self.install_graphs(&body.graphs).await.map_err(ioerr)?;
        {
            let mut sm = self.sm.write().await;
            sm.last_applied_log = body.last_applied_log;
            sm.last_membership = body.last_membership.clone();
            let snapshot = sm.clone();
            drop(sm);
            self.persist_applied(&snapshot).await.map_err(ioerr)?;
        }
        *self.native_history.write().await = native_history;
        *self.current_snapshot.write().await = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TypeConfig>>, io::Error> {
        match &*self.current_snapshot.read().await {
            Some((meta, data)) => Ok(Some(Snapshot {
                meta: meta.clone(),
                snapshot: Cursor::new(data.clone()),
            })),
            None => Ok(None),
        }
    }
}
