//! Raft storage (CONCEPT:KG-2.188) — log store + state machine over the engine.
//!
//! Built on openraft's v1 [`RaftStorage`] trait (split into a log store + state
//! machine by [`openraft::storage::Adaptor`] in [`super::node`]). Mirrors the
//! canonical `openraft-memstore` reference, with two engine-specific changes:
//!
//! 1. **The state machine IS the engine.** `apply_to_state_machine` applies each
//!    committed [`RaftRequest`]'s durable [`Method`] to the target graph's
//!    [`GraphCore`] via the SAME [`crate::wal::apply`] path a replayed WAL record
//!    uses, then awaits [`PersistenceBackend::record_durable`] (the M2 / KG-2.187
//!    commit-before-ack barrier) so committed graph data is durable in `graph.redb`.
//!
//! 2. **Durable Raft metadata.** `vote`, `last_applied_log` and `last_membership`
//!    are persisted to a SEPARATE `raft.redb` ([`RaftMeta`]) so a restarted node
//!    recovers its vote + applied index without reaching into the M2 backend's file
//!    (redb is single-handle-per-process). The Raft LOG itself is in-memory (a
//!    documented first-increment follow-up): committed state is never lost because
//!    it is on a quorum AND durable via M2; an un-snapshotted log tail re-replicates
//!    from the leader on restart (Raft's normal catch-up).
//!
//! The snapshot carries a full graph dump so a follower that installs a snapshot
//! (rather than replaying every log entry) still materializes the data into its
//! registry + M2 store.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
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

use super::{AppCtx, NodeId, RaftRequest, RaftResponse, TypeConfig};
use crate::protocol::GraphType;

/// Durable Raft metadata sidecar: a tiny redb DB (`raft.redb`) holding the vote and
/// the applied-state pointers. Kept separate from the M2 `graph.redb` so this module
/// never opens a second handle to the M2 file.
mod meta {
    use redb::{Database, ReadableDatabase, TableDefinition};

    const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

    /// Owns `raft.redb`. Synchronous redb calls run inside the (already off the hot
    /// path) Raft storage callbacks; metadata writes are tiny + infrequent.
    pub struct RaftMeta {
        db: Database,
    }

    impl RaftMeta {
        pub fn open(persist_dir: &str) -> Result<Self, String> {
            std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
            let path = std::path::Path::new(persist_dir).join("raft.redb");
            let db = Database::create(&path).map_err(|e| e.to_string())?;
            {
                let wtx = db.begin_write().map_err(|e| e.to_string())?;
                wtx.open_table(META).map_err(|e| e.to_string())?;
                wtx.commit().map_err(|e| e.to_string())?;
            }
            Ok(Self { db })
        }

        pub fn put(&self, key: &str, val: &[u8]) -> Result<(), String> {
            let wtx = self.db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut t = wtx.open_table(META).map_err(|e| e.to_string())?;
                t.insert(key, val).map_err(|e| e.to_string())?;
            }
            // Immediate durability: a vote/applied-index must be on disk before the
            // storage callback returns (Raft correctness depends on a persisted vote).
            wtx.commit().map_err(|e| e.to_string())?;
            Ok(())
        }

        pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
            let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
            let t = rtx.open_table(META).map_err(|e| e.to_string())?;
            Ok(t.get(key)
                .map_err(|e| e.to_string())?
                .map(|v| v.value().to_vec()))
        }
    }
}

use meta::RaftMeta;

const KEY_VOTE: &str = "vote";
const KEY_APPLIED: &str = "applied_state";

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

/// The on-disk applied-state pointers persisted to `raft.redb` after every apply.
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

/// The combined Raft storage: in-memory log + the engine-backed state machine, with
/// a durable `raft.redb` metadata sidecar.
pub struct EgStore {
    /// The Raft log (in-memory; see module docs for the durability follow-up).
    log: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,
    committed: RwLock<Option<LogId<NodeId>>>,
    vote: RwLock<Option<Vote<NodeId>>>,
    sm: RwLock<StateMachine>,
    current_snapshot: RwLock<Option<HeldSnapshot>>,
    snapshot_idx: parking_lot::Mutex<u64>,
    /// Durable metadata sidecar.
    meta: RaftMeta,
    /// Engine context: registry + persistence the state machine applies into.
    ctx: AppCtx,
}

impl EgStore {
    /// Open the store, recovering the durable vote + applied state from `raft.redb`.
    /// The actual graph data is recovered separately by the M2 `load_all` path
    /// before Raft starts, so on boot the state machine's applied pointers and the
    /// on-disk graph data are consistent.
    pub fn open(persist_dir: &str, ctx: AppCtx) -> Result<Arc<Self>, String> {
        let meta = RaftMeta::open(persist_dir)?;
        let vote = match meta.get(KEY_VOTE)? {
            Some(b) => rmp_serde::from_slice(&b).map_err(|e| e.to_string())?,
            None => None,
        };
        let applied: AppliedState = match meta.get(KEY_APPLIED)? {
            Some(b) => rmp_serde::from_slice(&b).map_err(|e| e.to_string())?,
            None => AppliedState::default(),
        };
        Ok(Arc::new(Self {
            log: RwLock::new(BTreeMap::new()),
            last_purged_log_id: RwLock::new(applied.last_applied_log),
            committed: RwLock::new(None),
            vote: RwLock::new(vote),
            sm: RwLock::new(StateMachine {
                last_applied_log: applied.last_applied_log,
                last_membership: applied.last_membership,
            }),
            current_snapshot: RwLock::new(None),
            snapshot_idx: parking_lot::Mutex::new(0),
            meta,
            ctx,
        }))
    }

    fn persist_vote(&self, vote: &Vote<NodeId>) -> Result<(), String> {
        let b = rmp_serde::to_vec_named(vote).map_err(|e| e.to_string())?;
        self.meta.put(KEY_VOTE, &b)
    }

    fn persist_applied(&self, sm: &StateMachine) -> Result<(), String> {
        let a = AppliedState {
            last_applied_log: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
        };
        let b = rmp_serde::to_vec_named(&a).map_err(|e| e.to_string())?;
        self.meta.put(KEY_APPLIED, &b)
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
}

impl RaftLogReader<TypeConfig> for Arc<EgStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        Ok(log.range(range).map(|(_, e)| e.clone()).collect())
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
        let log = self.log.read().await;
        let last = log.iter().next_back().map(|(_, e)| *e.get_log_id());
        let last_purged = *self.last_purged_log_id.read().await;
        let last = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.persist_vote(vote)
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
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            log.remove(&k);
        }
        Ok(())
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
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
                self.persist_applied(&sm)
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
            self.persist_applied(&sm)
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
