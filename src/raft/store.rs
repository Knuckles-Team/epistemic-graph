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
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};

use super::{
    AppCtx, GroupId, NativeMutationCommand, RaftRequest, RaftResponse, ReplicatedMutation,
    TypeConfig,
};
use crate::protocol::{GraphType, Method, ResultPayload};
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

    fn encoded_result(result: ResultPayload) -> Vec<u8> {
        rmp_serde::to_vec_named(&result).expect("test result must encode")
    }

    fn cas_method() -> Method {
        Method::CompareAndSetNodeFields {
            node_id: "node".to_string(),
            conditions_msgpack: Vec::new(),
            updates_msgpack: Vec::new(),
        }
    }

    #[cfg(feature = "jobs")]
    fn receipt_for_transport(replayed: bool) -> crate::mutation_batch::MutationBatchCommit {
        let conditions = rmp_serde::to_vec_named(&serde_json::json!({"epoch": 0})).unwrap();
        let updates = rmp_serde::to_vec_named(&serde_json::json!({"epoch": 1})).unwrap();
        let batch = crate::server::mutation_batch::compile_methods(
            crate::server::mutation_batch::CompileBatch {
                batch_id: "raft-store-receipt-cas",
                request_id: 9,
                // Keep the synthetic fresh and replay receipts byte-identical;
                // production compilation mints this nonce once per attempt,
                // while this fixture builds both views of one stored receipt.
                attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([7; 32])),
                principal: Some("receipt-test-principal"),
                tenant: "receipt-tenant",
                graph: "receipt-graph",
                placement_epoch: 0,
                idempotency_key: "raft-store-receipt-cas",
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 13,
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: None,
            },
            vec![Method::CompareAndSetNodeFields {
                node_id: "reservation".to_string(),
                conditions_msgpack: conditions,
                updates_msgpack: updates,
            }],
        )
        .unwrap();
        let identity = batch.identity.clone();
        let commit = crate::mutation_batch::MutationBatchCommit {
            record: crate::mutation_batch::MutationBatchRecord {
                batch,
                identity: identity.clone(),
                status: crate::mutation_batch::MutationBatchStatus::Committed,
                committed_version: crate::mutation_batch::CommittedVersion::Graph {
                    source: 0,
                    target: 1,
                },
                result_msgpack: Some(encoded_result(ResultPayload::Bool(false))),
                committed_at_ms: 13,
            },
            identity,
            replayed,
        };
        commit.validate().unwrap();
        commit
    }

    #[cfg(feature = "jobs")]
    #[test]
    fn native_commit_response_keeps_exact_ack_loss_receipt_and_cas_false() {
        let fresh = receipt_for_transport(false);
        let replay = receipt_for_transport(true);
        let fresh_record = rmp_serde::to_vec_named(&fresh.record).unwrap();
        let replay_record = rmp_serde::to_vec_named(&replay.record).unwrap();
        assert_eq!(fresh_record, replay_record);

        for commit in [fresh, replay] {
            let response = EgStore::native_commit_outcome_to_response(Ok(commit));
            response.validate().unwrap();
            assert!(response.native_result.is_none());
            assert!(response.native_commit.is_some());
            assert!(response.native_commit.as_ref().is_some_and(|receipt| {
                matches!(
                    rmp_serde::from_slice::<ResultPayload>(
                        receipt.record.result_msgpack.as_deref().unwrap()
                    ),
                    Ok(ResultPayload::Bool(false))
                )
            }));
        }

        let error = EgStore::native_commit_outcome_to_response(Err("apply failed".to_string()));
        assert!(error.native_commit.is_none());
        assert!(error.native_result.is_none());
        assert_eq!(error.native_error.as_deref(), Some("apply failed"));
    }

    #[test]
    fn ordinary_replay_requires_a_stored_true_boolean() {
        let method = Method::AddNode {
            node_id: "node".to_string(),
            properties_msgpack: Vec::new(),
        };
        let ok = encoded_result(ResultPayload::Bool(true));
        assert!(EgStore::decode_replayed_graph_result(Some(&ok), &method, false).is_ok());

        let false_result = encoded_result(ResultPayload::Bool(false));
        let error = EgStore::decode_replayed_graph_result(Some(&false_result), &method, false)
            .expect_err("ordinary graph replay cannot claim a false terminal result");
        assert!(error.contains("Bool(true)"), "{error}");

        let count = encoded_result(ResultPayload::Count(1));
        let error = EgStore::decode_replayed_graph_result(Some(&count), &method, false)
            .expect_err("ordinary graph replay cannot reinterpret a count");
        assert!(error.contains("Bool(true)"), "{error}");

        let error = EgStore::decode_replayed_graph_result(Some(&[0xc1]), &method, false)
            .expect_err("ordinary graph replay must reject corrupt MessagePack");
        assert!(error.contains("corrupt"), "{error}");
        let error = EgStore::decode_replayed_graph_result(None, &method, false)
            .expect_err("ordinary graph replay must reject a missing receipt result");
        assert!(error.contains("missing"), "{error}");
    }

    #[test]
    fn cas_replay_accepts_only_a_boolean_apply_outcome() {
        let method = cas_method();
        let false_result = encoded_result(ResultPayload::Bool(false));
        assert!(matches!(
            EgStore::decode_replayed_graph_result(Some(&false_result), &method, false),
            Ok(Some(ResultPayload::Bool(false)))
        ));

        let count = encoded_result(ResultPayload::Count(1));
        let error = EgStore::decode_replayed_graph_result(Some(&count), &method, false)
            .expect_err("CAS receipt must not turn a non-boolean payload into contention");
        assert!(error.contains("must be Bool"), "{error}");

        let error = EgStore::decode_replayed_graph_result(Some(&[0xc1]), &method, false)
            .expect_err("CAS receipt must reject corrupt MessagePack");
        assert!(error.contains("corrupt"), "{error}");
        let error = EgStore::decode_replayed_graph_result(None, &method, false)
            .expect_err("CAS receipt must reject a missing apply result");
        assert!(error.contains("missing"), "{error}");
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

#[path = "store/apply.rs"]
mod apply;
#[path = "store/commit.rs"]
mod commit;
#[path = "store/log.rs"]
mod log;
#[path = "store/native.rs"]
mod native;
#[path = "store/open.rs"]
mod open;
#[path = "store/replay.rs"]
mod replay;
#[path = "store/snapshot.rs"]
mod snapshot;
#[path = "store/state_machine.rs"]
mod state_machine;
