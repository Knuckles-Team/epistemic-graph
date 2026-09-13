use super::*;
use crate::server::persistence::writer_reply::await_writer_reply;

impl RedbBackend {
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
}
