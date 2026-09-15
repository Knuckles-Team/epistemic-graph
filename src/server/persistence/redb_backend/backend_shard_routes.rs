use super::*;
use crate::server::persistence::writer_reply::await_writer_reply;

impl RedbBackend {
    /// The shard that owns `graph_fname` (stable routing, CONCEPT:EG-KG.backend.sharded-k-way-durable / EG-031).
    ///
    /// Routing seam: when a tenant catalog is attached AND holds an explicit entry for
    /// this graph, the catalog's shard wins (M3 rebalanceable placement). Otherwise —
    /// no catalog, or a graph the catalog has no entry for — this is the unchanged
    /// EG-026 `FNV-1a(graph_fname) % K`. `resolve_shard` folds both cases + clamps to
    /// the live shard count, so the override can never index out of range.
    pub(super) fn shard_for(&self, graph_fname: &str) -> &ShardWriter {
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
    ) -> Result<super::super::online_reshard::RawGraphRows, String> {
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
        rows: super::super::online_reshard::RawGraphRows,
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
    pub(super) fn shard0(&self) -> &ShardWriter {
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
    pub(super) fn shard_for_group(&self, group_id: u64) -> &ShardWriter {
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
}
