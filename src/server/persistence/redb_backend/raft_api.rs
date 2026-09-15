use super::*;
#[cfg(feature = "raft")]
use crate::server::persistence::writer_reply::await_writer_reply;
#[cfg(feature = "raft")]
use tokio::sync::oneshot;

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
}
