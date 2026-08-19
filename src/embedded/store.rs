//! Synchronous in-process redb durable store for the embedded engine
//! (CONCEPT:EG-KG.backend.engine-modes).
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

/// Owns the canonical single shard `{persist_dir}/graph-0.redb` and commits synchronously.
pub(super) struct EmbeddedRedbStore {
    db: Database,
    /// Encryption-at-rest cipher (CONCEPT:EG-KG.sharding.row-level-security), resolved once from
    /// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` at open. `None` ⇒ encryption off ⇒ durable
    /// format unchanged. Only present in a `security` build.
    #[cfg(feature = "security")]
    cipher: Option<crate::crypto::ValueCipher>,
}

impl EmbeddedRedbStore {
    /// The durable-crypto handle threaded into the shared redb_store read/write path.
    #[inline]
    fn crypto(&self) -> redb_store::DurableCrypto<'_> {
        #[cfg(feature = "security")]
        {
            redb_store::DurableCrypto::new(self.cipher.as_ref())
        }
        #[cfg(not(feature = "security"))]
        {
            redb_store::DurableCrypto::none()
        }
    }

    /// Open (or create) the durable store under `persist_dir`, ensuring every table
    /// exists (so a fresh DB's read-path `open_table` doesn't error on a missing
    /// table) — identical bootstrap to the server's `RedbBackend::open`.
    pub(super) fn open(persist_dir: &std::path::Path) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let shards = crate::redb_layout::reconcile_shard_layout(persist_dir, 1)?;
        if shards != 1 {
            return Err(format!(
                "embedded mode requires one canonical redb shard, found {shards}; run migrate-shards offline"
            ));
        }
        let db_path = persist_dir.join(crate::redb_layout::shard_filename(0));
        let db = Database::create(&db_path).map_err(|e| e.to_string())?;
        {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            redb_store::initialize_canonical_tables(&wtx)?;
            wtx.commit().map_err(|e| e.to_string())?;
        }
        Ok(Self {
            db,
            #[cfg(feature = "security")]
            cipher: crate::crypto::ValueCipher::from_env_checked()?,
        })
    }

    /// Commit one durable mutation INLINE with `Durability::Immediate`
    /// (commit-before-return). Reuses the shared `commit_ops` so the row format is
    /// byte-identical to the server's group-commit writer. The embedded path never
    /// appends Raft log ops, so that vector is always empty.
    pub(super) fn commit(&self, graph_fname: &str, method: &Method) -> Result<(), String> {
        if !crate::mutation_apply::is_durable_mutation(method) {
            return Ok(());
        }
        let mut ops = vec![(graph_fname.to_string(), method.clone())];
        let mut raft_log_ops = Vec::new();
        // The embedded path commits ONE op per transaction in its own process, so a
        // fresh per-call tail cache (CONCEPT:EG-KG.storage.embedded-store) seeds from one scan and is O(1) for
        // the single op — identical cost to before. The hot, CPU-bound writer is the
        // server's group-commit thread, which keeps its cache hot across batches.
        #[cfg(feature = "security")]
        let mut audit_tail = redb_store::AuditTailCache::new();
        redb_store::commit_ops(
            &self.db,
            &mut ops,
            &mut raft_log_ops,
            redb::Durability::Immediate,
            self.crypto(),
            #[cfg(feature = "security")]
            &mut audit_tail,
        )
    }

    /// Durably register a graph's identity (name/type) so `load_all` recovers it
    /// under its REAL name even before the first checkpoint.
    pub(super) fn register_graph(
        &self,
        graph_fname: &str,
        name: &str,
        graph_type: GraphType,
        incarnation_id: &str,
    ) -> Result<(), String> {
        redb_store::write_graph_meta_with_incarnation(
            &self.db,
            graph_fname,
            name,
            graph_type,
            incarnation_id,
        )
    }

    /// Snapshot the whole registry dump into redb in one durable transaction.
    pub(super) fn checkpoint(&self, dumps: Vec<GraphDump>) -> Result<usize, String> {
        let mut pending = Vec::new();
        redb_store::apply_checkpoint(&self.db, &mut pending, dumps, self.crypto())
    }

    /// Read the entire durable store back into per-graph dumps (boot recovery).
    pub(super) fn load_all(&self) -> Result<Vec<GraphDump>, String> {
        redb_store::read_all_dumps(&self.db, self.crypto())
    }

    /// Durably PURGE every row for a deleted graph (nodes/edges/ledger/semantic +
    /// the `graph_meta` identity) in one immediate transaction (CONCEPT:EG-KG.backend.tenant-delete-recreate-same).
    /// Reuses the SHARED `purge_graph_rows`, so a recreate of the same name starts
    /// from a clean durable slate — the embedded analogue of the server's
    /// `Cmd::PurgeGraph` tenant-delete teardown.
    pub(super) fn purge(&self, graph_fname: &str) -> Result<(), String> {
        redb_store::purge_graph_rows(&self.db, graph_fname, self.crypto())
    }
}
