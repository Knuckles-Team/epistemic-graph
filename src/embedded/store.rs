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

use crate::protocol::{GraphType, Method};
use crate::redb_store::{self, shard::Shard, GraphDump};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(test)]
struct RegisterFailureBlock {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

/// Owns the canonical single shard `{persist_dir}/graph-0.redb` and commits synchronously.
pub(super) struct EmbeddedRedbStore {
    shard: Shard,
    /// Encryption-at-rest cipher (CONCEPT:EG-KG.sharding.row-level-security), resolved once from
    /// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` at open. `None` ⇒ encryption off ⇒ durable
    /// format unchanged. Only present in a `security` build.
    #[cfg(feature = "security")]
    cipher: Option<crate::crypto::ValueCipher>,
    /// Unit-test-only hooks exercise lifecycle rollback paths without relying
    /// on filesystem or process failure injection.
    #[cfg(test)]
    fail_next_purge: AtomicBool,
    #[cfg(test)]
    register_failure_block: std::sync::Mutex<Option<RegisterFailureBlock>>,
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
        let shard = Shard::open(&db_path)?;
        Ok(Self {
            shard,
            #[cfg(feature = "security")]
            cipher: crate::crypto::ValueCipher::from_env_checked()?,
            #[cfg(test)]
            fail_next_purge: AtomicBool::new(false),
            #[cfg(test)]
            register_failure_block: std::sync::Mutex::new(None),
        })
    }

    /// Block the next registration until the returned release sender is used,
    /// then fail it. The entered receiver makes the lifecycle lock ordering
    /// test deterministic without sleeps.
    #[cfg(test)]
    pub(super) fn block_next_register_failure(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.register_failure_block.lock().unwrap() = Some(RegisterFailureBlock {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }

    /// Inject one purge failure for the embedded lifecycle tests.
    #[cfg(test)]
    pub(super) fn fail_next_purge(&self) {
        self.fail_next_purge.store(true, Ordering::Release);
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
        static NEXT_ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let drain_id = format!(
            "embedded/{}/{}",
            std::process::id(),
            NEXT_ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        redb_store::commit_ops(
            &self.shard,
            &mut ops,
            &mut raft_log_ops,
            &drain_id,
            0,
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
        #[cfg(test)]
        if let Some(block) = self.register_failure_block.lock().unwrap().take() {
            let _ = block.entered.send(());
            let _ = block.release.recv();
            return Err("injected embedded graph registration failure".to_string());
        }
        redb_store::write_graph_meta_with_incarnation(
            &self.shard,
            graph_fname,
            name,
            graph_type,
            incarnation_id,
        )
    }

    /// Snapshot the whole registry dump into redb in one durable transaction.
    pub(super) fn checkpoint(&self, dumps: Vec<GraphDump>) -> Result<usize, String> {
        let mut pending = Vec::new();
        redb_store::apply_checkpoint(&self.shard, &mut pending, dumps, self.crypto())
    }

    /// Read the entire durable store back into per-graph dumps (boot recovery).
    pub(super) fn load_all(&self) -> Result<Vec<GraphDump>, String> {
        redb_store::read_all_dumps(&self.shard, self.crypto())
    }

    /// Durably PURGE every row for a deleted graph (nodes/edges/ledger/semantic +
    /// the `graph_meta` identity) in one immediate transaction (CONCEPT:EG-KG.backend.tenant-delete-recreate-same).
    /// Reuses the SHARED `purge_graph_rows`, so a recreate of the same name starts
    /// from a clean durable slate — the embedded analogue of the server's
    /// `Cmd::PurgeGraph` tenant-delete teardown.
    pub(super) fn purge(&self, graph_fname: &str) -> Result<(), String> {
        #[cfg(test)]
        if self.fail_next_purge.swap(false, Ordering::AcqRel) {
            return Err("injected embedded graph purge failure".to_string());
        }
        redb_store::purge_graph_rows(&self.shard, graph_fname)
    }
}
