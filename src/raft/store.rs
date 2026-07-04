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
//! 1. **The state machine IS the engine.** [`RaftStateMachine::apply`] applies each
//!    committed [`RaftRequest`]'s durable [`Method`] to the target graph's
//!    [`GraphCore`] via the SAME [`crate::wal::apply`] path a replayed WAL record
//!    uses, then awaits [`PersistenceBackend::record_durable`] (the M2 / KG-2.187
//!    commit-before-ack barrier) so committed graph data is durable in `graph.redb`.
//!
//! 2. **Durable redb Raft log (CONCEPT:EG-KG.storage.one-fsync-covers-raft).** The log entries, the vote, and
//!    the applied-state pointers all live in the SAME `graph.redb` Database as the
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
use tokio::sync::RwLock;

use super::{AppCtx, GroupId, RaftRequest, RaftResponse, TypeConfig};
use crate::protocol::GraphType;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;

const KEY_VOTE: &str = "vote";
const KEY_APPLIED: &str = "applied_state";
const KEY_PURGED: &str = "last_purged";

/// Map any `Display` error (the redb backend + rmp_serde all surface `String`/`E:
/// Display`) into the `io::Error` the 0.10 storage traits now require.
fn ioerr<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

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
#[serde(bound = "")]
struct SmSnapshotData {
    last_applied_log: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
    graphs: Vec<GraphSnapshot>,
}

/// The on-disk applied-state pointers persisted to redb after every apply.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(bound = "")]
struct AppliedState {
    last_applied_log: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
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
/// shared M2 `graph.redb` ([`RedbBackend`]), keyed by this store's [`GroupId`].
pub struct EgStore {
    /// This group's id — the composite-key prefix for its log + meta rows.
    group_id: GroupId,
    /// The shared M2 persistence backend — owns `graph.redb` and its group-commit
    /// writer. Held as the trait object (the same `Arc` `ServerState` holds); the
    /// concrete [`RedbBackend`] is recovered via [`PersistenceBackend::as_redb`] so
    /// the log rides the SAME writer/transaction as the M2 graph mutations.
    backend: Arc<dyn PersistenceBackend>,
    last_purged_log_id: RwLock<Option<LogIdOf<TypeConfig>>>,
    committed: RwLock<Option<LogIdOf<TypeConfig>>>,
    vote: RwLock<Option<VoteOf<TypeConfig>>>,
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
        let purged: Option<LogIdOf<TypeConfig>> = match redb.raft_meta_get(group_id, KEY_PURGED)? {
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

    /// Dump THIS group's graphs for a snapshot (CONCEPT:AU-KG.ingest.staged). When the store runs
    /// under a [`super::multi::MultiRaft`] its ctx carries the router, so the dump is
    /// SCOPED to graphs whose tenant range resolves to this group — a large tenant in
    /// one group never bloats another group's snapshot. Without a router (a direct
    /// single-store open) the whole registry is dumped (the unscoped scaffold path).
    async fn dump_graphs(&self) -> Vec<GraphSnapshot> {
        let s = self.ctx.state.read().await;
        s.registry
            .all_entries()
            .iter()
            .filter(|e| match &self.ctx.router {
                Some(router) => router.group_of(&e.name) == self.group_id,
                None => true,
            })
            .map(|e| GraphSnapshot {
                name: e.name.clone(),
                fname: crate::persist::sanitize(&e.name),
                graph_type: e.graph_type,
                nodes: e.core.get_nodes(),
                edges: e.core.get_edges(),
            })
            .collect()
    }

    /// Test-only: the sorted graph NAMES this group's snapshot would capture, AFTER
    /// per-group scoping (CONCEPT:AU-KG.ingest.staged). Lets a test assert a group's snapshot
    /// carries ONLY its own tenant-range graphs without reaching into private types.
    #[cfg(test)]
    pub(crate) async fn scoped_snapshot_graph_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .dump_graphs()
            .await
            .into_iter()
            .map(|g| g.name)
            .collect();
        names.sort();
        names
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
    fn read_one_entry(&self, idx: u64) -> Result<Option<EntryOf<TypeConfig>>, String> {
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
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        let (lo, hi) = inclusive_bounds(&range);
        if lo > hi {
            return Ok(Vec::new());
        }
        let blobs = self
            .redb()
            .raft_log_read(self.group_id, lo, hi)
            .map_err(ioerr)?;
        let mut out = Vec::with_capacity(blobs.len());
        for b in blobs {
            let e: EntryOf<TypeConfig> = rmp_serde::from_slice(&b).map_err(ioerr)?;
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
        let data = rmp_serde::to_vec_named(&body).map_err(ioerr)?;

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
        for entry in entries {
            let blob = rmp_serde::to_vec_named(&entry).map_err(ioerr)?;
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
            let resp = match &entry.payload {
                EntryPayload::Blank => RaftResponse { applied: false },
                EntryPayload::Normal(req) => self.apply_request(req).await.map_err(ioerr)?,
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
        let data = snapshot.into_inner();
        let body: SmSnapshotData = rmp_serde::from_slice(&data).map_err(ioerr)?;
        // Materialize the graph data into the registry + M2 store.
        self.install_graphs(&body.graphs).await.map_err(ioerr)?;
        {
            let mut sm = self.sm.write().await;
            sm.last_applied_log = body.last_applied_log;
            sm.last_membership = body.last_membership.clone();
            let snapshot = sm.clone();
            drop(sm);
            self.persist_applied(&snapshot).await.map_err(ioerr)?;
        }
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
