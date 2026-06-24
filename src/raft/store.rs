//! Raft storage (CONCEPT:KG-2.188 + KG-2.204) — durable log store + state machine.
//!
//! Built on openraft's v1 [`RaftStorage`] trait (split into a log store + state
//! machine by [`openraft::storage::Adaptor`] in [`super::node`]). Two
//! engine-specific properties:
//!
//! 1. **The state machine IS the engine.** `apply_to_state_machine` applies each
//!    committed [`RaftRequest`]'s durable [`Method`] to the target graph's
//!    [`GraphCore`] via the SAME [`crate::wal::apply`] path a replayed WAL record
//!    uses, then awaits [`PersistenceBackend::record_durable`] (the M2 / KG-2.187
//!    commit-before-ack barrier) so committed graph data is durable in `graph.redb`.
//!
//! 2. **Durable redb Raft log (CONCEPT:KG-2.204).** The log entries, the vote, and
//!    the applied-state pointers all live in the SAME `graph.redb` Database as the
//!    M2 graph data — keyed by `(group_id, index)` / `(group_id, key)` so ONE redb
//!    file serves the M2 store AND every Raft group's log (the spike's "one DB,
//!    composite key" shape — NOT a file per group, which hits the FD ceiling).
//!    Because the log shares the M2 `RedbBackend`'s off-reactor group-commit writer,
//!    a log append and its graph mutation COALESCE into ONE `WriteTransaction` /
//!    one fsync. A restarted node recovers its log tail LOCALLY from redb — it no
//!    longer needs the leader to refill an un-snapshotted tail.
//!
//! The snapshot carries a full graph dump so a follower that installs a snapshot
//! (rather than replaying every log entry) still materializes the data into its
//! registry + M2 store.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::Snapshot;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogId;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::{AppCtx, GroupId, NodeId, RaftRequest, RaftResponse, TypeConfig};
use crate::protocol::GraphType;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;

const KEY_VOTE: &str = "vote";
const KEY_APPLIED: &str = "applied_state";
const KEY_PURGED: &str = "last_purged";

/// One graph's data, captured for a snapshot so a follower can rebuild it on
/// `install_snapshot` even if it never saw the per-entry log.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphSnapshot {
    name: String,
    fname: String,
    graph_type: GraphType,
    nodes: Vec<(String, Vec<u8>)>,
    edges: Vec<(String, String, Vec<u8>)>,
}

/// The serialized state-machine snapshot body.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SmSnapshotData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    graphs: Vec<GraphSnapshot>,
}

/// The on-disk applied-state pointers persisted to redb after every apply.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AppliedState {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
}

/// A held snapshot: its metadata + the serialized body.
type HeldSnapshot = (SnapshotMeta<NodeId, openraft::BasicNode>, Vec<u8>);

/// In-RAM state-machine pointers (the actual graph data lives in GraphCore + M2).
#[derive(Debug, Clone, Default)]
struct StateMachine {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
}

/// The combined Raft storage for ONE group: a durable redb-backed log + the
/// engine-backed state machine. The log, vote and applied-state are persisted in the
/// shared M2 `graph.redb` ([`RedbBackend`]), keyed by this store's [`GroupId`].
pub struct EgStore {
    /// This group's id — the composite-key prefix for its log + meta rows.
    group_id: GroupId,
    /// The shared M2 persistence backend — owns `graph.redb` and its group-commit
    /// writer. Held as the trait object (the same `Arc` `ServerState` holds); the
    /// concrete [`RedbBackend`] is recovered via [`PersistenceBackend::as_redb`] so
    /// the log rides the SAME writer/transaction as the M2 graph mutations.
    backend: Arc<dyn PersistenceBackend>,
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,
    committed: RwLock<Option<LogId<NodeId>>>,
    vote: RwLock<Option<Vote<NodeId>>>,
    sm: RwLock<StateMachine>,
    current_snapshot: RwLock<Option<HeldSnapshot>>,
    snapshot_idx: parking_lot::Mutex<u64>,
    /// Engine context: registry + persistence the state machine applies into.
    ctx: AppCtx,
}

impl EgStore {
    /// Open the store for `group_id`, recovering the durable vote + applied state +
    /// last-purged pointer from the shared `graph.redb` (keyed by group id). The
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
            Some(b) => rmp_serde::from_slice(&b).map_err(|e| e.to_string())?,
            None => None,
        };
        let applied: AppliedState = match redb.raft_meta_get(group_id, KEY_APPLIED)? {
            Some(b) => rmp_serde::from_slice(&b).map_err(|e| e.to_string())?,
            None => AppliedState::default(),
        };
        let purged: Option<LogId<NodeId>> = match redb.raft_meta_get(group_id, KEY_PURGED)? {
            Some(b) => rmp_serde::from_slice(&b).map_err(|e| e.to_string())?,
            None => None,
        };
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
            snapshot_idx: parking_lot::Mutex::new(0),
            ctx,
        }))
    }

    /// The concrete redb backend (the raft store is only constructed over redb).
    fn redb(&self) -> &RedbBackend {
        self.backend
            .as_redb()
            .expect("raft store backend is always redb (checked at open)")
    }

    async fn persist_vote(&self, vote: &Vote<NodeId>) -> Result<(), String> {
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

    /// Apply ONE committed request to the engine: ensure the graph exists, apply the
    /// durable Method to its GraphCore via the shared WAL-apply path, then await the
    /// M2 durable commit (commit-before-ack barrier). The same path a replayed WAL
    /// record takes — so a committed Raft entry and a replayed WAL record are
    /// byte-identical in effect.
    async fn apply_request(&self, req: &RaftRequest) -> Result<RaftResponse, String> {
        // Resolve (creating if absent) the target graph's core. A follower that has
        // never seen this graph creates it from the request's name/type.
        let (core, persistence, authoritative) = {
            let mut s = self.ctx.state.write().await;
            if !s.registry.exists(&req.graph_name) {
                let _ = s
                    .registry
                    .create_graph(&req.graph_name, req.graph_type, None);
            }
            let core = match s.registry.get(&req.graph_name).map(|e| e.core.clone()) {
                Some(c) => c,
                None => return Err(format!("graph '{}' missing after create", req.graph_name)),
            };
            (core, s.persistence.clone(), s.redb_authoritative)
        };

        // Apply to the in-memory graph (shared with WAL replay).
        crate::wal::apply(&core, &req.method);
        core.mark_dirty();

        // Make it durable through the M2 path. Under authoritative mode this awaits
        // the redb group-commit (one fsync per batch). A commit failure is a hard
        // error — the committed entry must land on disk on every node.
        if let Some(p) = persistence {
            if authoritative {
                p.record_durable(&req.graph_fname, &req.method).await?;
            } else {
                p.record(&req.graph_fname, &req.method);
            }
        }
        Ok(RaftResponse { applied: true })
    }

    /// Dump every graph for a snapshot. Reads off the registry under the read lock.
    async fn dump_graphs(&self) -> Vec<GraphSnapshot> {
        let s = self.ctx.state.read().await;
        s.registry
            .all_entries()
            .iter()
            .map(|e| GraphSnapshot {
                name: e.name.clone(),
                fname: crate::persist::sanitize(&e.name),
                graph_type: e.graph_type,
                nodes: e.core.get_nodes(),
                edges: e.core.get_edges(),
            })
            .collect()
    }

    /// Rebuild graphs from a snapshot into the registry + M2 store.
    async fn install_graphs(&self, graphs: &[GraphSnapshot]) -> Result<(), String> {
        for g in graphs {
            let (core, persistence, authoritative) = {
                let mut s = self.ctx.state.write().await;
                if !s.registry.exists(&g.name) {
                    let _ = s.registry.create_graph(&g.name, g.graph_type, None);
                }
                let core = match s.registry.get(&g.name).map(|e| e.core.clone()) {
                    Some(c) => c,
                    None => continue,
                };
                (core, s.persistence.clone(), s.redb_authoritative)
            };
            for (id, props) in &g.nodes {
                core.add_node(id.clone(), props.clone());
                if let Some(p) = &persistence {
                    let m = crate::protocol::Method::AddNode {
                        node_id: id.clone(),
                        properties_msgpack: props.clone(),
                    };
                    if authoritative {
                        p.record_durable(&g.fname, &m).await?;
                    } else {
                        p.record(&g.fname, &m);
                    }
                }
            }
            for (src, tgt, props) in &g.edges {
                let _ = core.add_edge(src.clone(), tgt.clone(), props.clone());
                if let Some(p) = &persistence {
                    let m = crate::protocol::Method::AddEdge {
                        source_id: src.clone(),
                        target_id: tgt.clone(),
                        properties_msgpack: props.clone(),
                    };
                    if authoritative {
                        p.record_durable(&g.fname, &m).await?;
                    } else {
                        p.record(&g.fname, &m);
                    }
                }
            }
            core.mark_dirty();
        }
        Ok(())
    }

    /// Read ONE stored log entry by index from redb (helper for `get_log_state`).
    fn read_one_entry(&self, idx: u64) -> Result<Option<Entry<TypeConfig>>, String> {
        let blobs = self.redb().raft_log_read(self.group_id, idx, idx)?;
        match blobs.into_iter().next() {
            Some(b) => Ok(Some(rmp_serde::from_slice(&b).map_err(|e| e.to_string())?)),
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
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let (lo, hi) = inclusive_bounds(&range);
        if lo > hi {
            return Ok(Vec::new());
        }
        let blobs = self
            .redb()
            .raft_log_read(self.group_id, lo, hi)
            .map_err(|e| StorageIOError::read_logs(&AnyErr(e)))?;
        let mut out = Vec::with_capacity(blobs.len());
        for b in blobs {
            let e: Entry<TypeConfig> = rmp_serde::from_slice(&b)
                .map_err(|e| StorageIOError::read_logs(&AnyErr(e.to_string())))?;
            out.push(e);
        }
        Ok(out)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<EgStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied_log, last_membership) = {
            let sm = self.sm.read().await;
            (sm.last_applied_log, sm.last_membership.clone())
        };
        let graphs = self.dump_graphs().await;
        let body = SmSnapshotData {
            last_applied_log,
            last_membership: last_membership.clone(),
            graphs,
        };
        let data =
            rmp_serde::to_vec_named(&body).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock();
            *l += 1;
            *l
        };
        let snapshot_id = match last_applied_log {
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
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<EgStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let (_, last_idx) = self
            .redb()
            .raft_log_bounds(self.group_id)
            .map_err(|e| StorageIOError::read_logs(&AnyErr(e)))?;
        let last_purged = *self.last_purged_log_id.read().await;
        // Reconstruct the last log id from the stored entry (redb holds the Entry),
        // so a restart knows its log tail WITHOUT the leader.
        let last_log_id = match last_idx {
            Some(i) => match self
                .read_one_entry(i)
                .map_err(|e| StorageIOError::read_logs(&AnyErr(e)))?
            {
                Some(e) => Some(*e.get_log_id()),
                None => last_purged,
            },
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.persist_vote(vote)
            .await
            .map_err(|e| StorageIOError::write_vote(&AnyErr(e)))?;
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        *self.committed.write().await = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(*self.committed.read().await)
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.redb()
            .raft_log_delete_from(self.group_id, log_id.index)
            .await
            .map_err(|e| StorageIOError::write_logs(&AnyErr(e)).into())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }
        let b = rmp_serde::to_vec_named(&Some(log_id))
            .map_err(|e| StorageIOError::write_logs(&AnyErr(e.to_string())))?;
        self.redb()
            .raft_meta_put(self.group_id, KEY_PURGED, b)
            .await
            .map_err(|e| StorageIOError::write_logs(&AnyErr(e)))?;
        self.redb()
            .raft_log_purge_upto(self.group_id, log_id.index)
            .await
            .map_err(|e| StorageIOError::write_logs(&AnyErr(e)).into())
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut batch = Vec::new();
        for entry in entries {
            let blob = rmp_serde::to_vec_named(&entry)
                .map_err(|e| StorageIOError::write_logs(&AnyErr(e.to_string())))?;
            batch.push((entry.log_id.index, blob));
        }
        // Durable append: rides the SAME group-commit transaction as any concurrent
        // M2 graph mutation (CONCEPT:KG-2.204) — one fsync covers both.
        self.redb()
            .raft_log_append(self.group_id, batch)
            .await
            .map_err(|e| StorageIOError::write_logs(&AnyErr(e)).into())
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<RaftResponse>, StorageError<NodeId>> {
        let mut res = Vec::with_capacity(entries.len());
        for entry in entries {
            let resp = match &entry.payload {
                EntryPayload::Blank => RaftResponse { applied: false },
                EntryPayload::Normal(req) => self
                    .apply_request(req)
                    .await
                    .map_err(|e| StorageIOError::apply(entry.log_id, &AnyErr(e)))?,
                EntryPayload::Membership(mem) => {
                    let mut sm = self.sm.write().await;
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    RaftResponse { applied: false }
                }
            };
            // Record the applied index (durably) AFTER the effect landed.
            {
                let mut sm = self.sm.write().await;
                sm.last_applied_log = Some(entry.log_id);
                let snapshot = sm.clone();
                drop(sm);
                self.persist_applied(&snapshot)
                    .await
                    .map_err(|e| StorageIOError::write_state_machine(&AnyErr(e)))?;
            }
            res.push(resp);
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let body: SmSnapshotData = rmp_serde::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        // Materialize the graph data into the registry + M2 store.
        self.install_graphs(&body.graphs)
            .await
            .map_err(|e| StorageIOError::write_state_machine(&AnyErr(e)))?;
        {
            let mut sm = self.sm.write().await;
            sm.last_applied_log = body.last_applied_log;
            sm.last_membership = body.last_membership.clone();
            let snapshot = sm.clone();
            drop(sm);
            self.persist_applied(&snapshot)
                .await
                .map_err(|e| StorageIOError::write_state_machine(&AnyErr(e)))?;
        }
        *self.current_snapshot.write().await = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some((meta, data)) => Ok(Some(Snapshot {
                meta: meta.clone(),
                snapshot: Box::new(Cursor::new(data.clone())),
            })),
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

/// Adapter so a plain `String` error satisfies `Into<AnyError>` for the
/// `StorageIOError` constructors (they take `impl Into<AnyError>`).
#[derive(Debug)]
struct AnyErr(String);

impl std::fmt::Display for AnyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for AnyErr {}
