//! Synchronous in-process redb durable store for the embedded engine
//! (CONCEPT:KG-2.216).
//!
//! Unlike the server's `redb_backend` — which spawns an off-reactor group-commit
//! writer thread so thousands of concurrent socket clients amortize fsyncs — the
//! embedded engine has ONE in-process caller, so it owns the redb `Database`
//! directly and commits each durable mutation INLINE with `Durability::Immediate`.
//! That is the commit-before-return durability barrier (the in-process analogue of
//! the server's commit-before-ack): the call returns only after the row is on disk.
//!
//! It reuses the EXACT shared durable machinery in [`crate::redb_store`]
//! (table layout, `Method → rows` apply, checkpoint, load) — the SAME format the
//! server writes — so it adds no duplicate durable logic and a graph written here
//! reopens in the server.

use redb::Database;

use crate::protocol::{GraphType, Method};
use crate::redb_store::{self, GraphDump};

/// Owns `{persist_dir}/graph.redb` and commits synchronously.
pub(super) struct EmbeddedRedbStore {
    db: Database,
}

impl EmbeddedRedbStore {
    /// Open (or create) the durable store under `persist_dir`, ensuring every table
    /// exists (so a fresh DB's read-path `open_table` doesn't error on a missing
    /// table) — identical bootstrap to the server's `RedbBackend::open`.
    pub(super) fn open(persist_dir: &std::path::Path) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let db_path = persist_dir.join("graph.redb");
        let db = Database::create(&db_path).map_err(|e| e.to_string())?;
        {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            wtx.open_table(redb_store::NODES)
                .map_err(|e| e.to_string())?;
            wtx.open_table(redb_store::EDGES)
                .map_err(|e| e.to_string())?;
            wtx.open_table(redb_store::LEDGER)
                .map_err(|e| e.to_string())?;
            wtx.open_table(redb_store::SEMANTIC)
                .map_err(|e| e.to_string())?;
            wtx.open_table(redb_store::GRAPH_META)
                .map_err(|e| e.to_string())?;
            wtx.commit().map_err(|e| e.to_string())?;
        }
        Ok(Self { db })
    }

    /// Commit one durable mutation INLINE with `Durability::Immediate`
    /// (commit-before-return). Reuses the shared `commit_ops` so the row format is
    /// byte-identical to the server's group-commit writer. The embedded path never
    /// appends Raft log ops, so that vector is always empty.
    pub(super) fn commit(&self, graph_fname: &str, method: &Method) -> Result<(), String> {
        if !crate::wal::is_durable_mutation(method) {
            return Ok(());
        }
        let mut ops = vec![(graph_fname.to_string(), method.clone())];
        let mut raft_log_ops = Vec::new();
        redb_store::commit_ops(
            &self.db,
            &mut ops,
            &mut raft_log_ops,
            redb::Durability::Immediate,
        )
    }

    /// Durably register a graph's identity (name/type) so `load_all` recovers it
    /// under its REAL name even before the first checkpoint.
    pub(super) fn register_graph(
        &self,
        graph_fname: &str,
        name: &str,
        graph_type: GraphType,
    ) -> Result<(), String> {
        redb_store::write_graph_meta(&self.db, graph_fname, name, graph_type)
    }

    /// Snapshot the whole registry dump into redb in one durable transaction.
    pub(super) fn checkpoint(&self, dumps: Vec<GraphDump>) -> Result<usize, String> {
        let mut pending = Vec::new();
        redb_store::apply_checkpoint(&self.db, &mut pending, dumps)
    }

    /// Read the entire durable store back into per-graph dumps (boot recovery).
    pub(super) fn load_all(&self) -> Result<Vec<GraphDump>, String> {
        redb_store::read_all_dumps(&self.db)
    }
}
