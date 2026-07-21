//! Authoritative durable graph-store contract (CONCEPT:EG-KG.storage.kg-kg).
//!
//! The served engine has one implementation, [`redb_backend::RedbBackend`]. The
//! trait keeps mutation, recovery, read-through, backup, and Raft consumers on one
//! contract without exposing storage internals. Every public mutation is committed
//! before acknowledgement; bounded queues apply backpressure and never drop work.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::change_envelope::{
    ChangeCursor, ChangeEnvelope, ChangeEnvelopeCommit, ChangeEnvelopeRecord, ContentVersion,
};
use crate::mutation_batch::{
    MutationBatch, MutationBatchCommit, MutationBatchRecord, MutationOutboxLease,
    MutationOutboxRecord, MutationProjectionCursor,
};
use crate::protocol::Method;
use crate::server::ServerState;

pub mod read_through;

#[cfg(feature = "redb")]
pub mod redb_backend;

// M3 — catalog-driven resharding (CONCEPT:EG-KG.sharding.atomic-shard-swap / EG-031). Both are redb-only:
//   * `shard_migrate` — OFFLINE K-shard migration tool that rewrites an existing
//     canonical `graph-<n>.redb` set into a new K using the SAME routing,
//     preserving every durable row verbatim (incl. the tamper-evident audit chain).
//   * `tenant_catalog` — durable graph/tenant→shard (and future →node) map + a
//     read-only routing-override seam, defaulting to EG-026 FNV-1a when empty.
#[cfg(feature = "redb")]
pub mod shard_migrate;

#[cfg(feature = "redb")]
pub mod tenant_catalog;

// M3 keystone (CONCEPT:EG-KG.backend.catalog-shard-resolve / EG-034). Both redb-only:
//   * `online_reshard` — move ONE graph between shards while the engine RUNS (verbatim
//     row copy + catalog route flip + source GC), building on EG-030's copy + EG-031's
//     catalog. The keystone the offline EG-030 tool skips.
//   * `cold_offload` — time-windowed whole-graph offload of idle tenants (hibernate +
//     read-through serve) to bound RAM across many tenants.
#[cfg(feature = "redb")]
pub mod online_reshard;

#[cfg(feature = "redb")]
pub mod cold_offload;

// M3 R3 — rebalancing planner (CONCEPT:EG-KG.sharding.even-load-rebalance). A PURE, deterministic policy layer
// over observable per-shard/per-graph load + the EG-031 catalog that EMITS a plan of
// `{graph, from_shard, to_shard}` moves to even out load. It does NOT execute the
// plan — that is R1 online resharding (online_reshard above). Parallel-safe, no M2 dep.
#[cfg(feature = "redb")]
pub mod rebalance;

// EG-090 — online consistent backup/restore + PITR foundation. Redb-only:
//   * `backup` — per-shard `begin_read()` MVCC snapshot (EG-027) streamed verbatim
//     (reusing EG-030's raw-row copy) into a portable bundle + manifest, ONLINE (no
//     stop-the-world), with stable admin/cross-shard recovery-boundary fingerprints;
//     preserves at-rest ciphertext + the KG-2.231 audit chain byte-for-byte.
//   * `restore_bundle` — rebuilds a persist-dir from a bundle (verbatim import via the
//     EG-030 migration engine; supports re-shard-on-restore). Backing the DR / PITR story.
#[cfg(feature = "redb")]
pub mod backup;

/// A durable persistence tier for the graph registry.
///
/// Recovery and mutation APIs are asynchronous because every acknowledged
/// mutation must cross the durable commit barrier. `shutdown` remains synchronous
/// so process teardown can stop the backend after request admission has closed.
#[async_trait::async_trait]
pub trait PersistenceBackend: Send + Sync {
    /// Reconstruct the registry from durable storage at boot. Returns the number
    /// of graphs loaded. No-op (Ok(0)) when nothing is configured.
    async fn load_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String>;

    /// Populate the registry's CATALOG ONLY at boot (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) —
    /// every graph's identity (name/type), with NO node/edge data read. Each
    /// graph's `GraphCore` then materializes lazily on first access (see
    /// `server::persistence::cold_offload::lazy_open`) via
    /// [`Self::read_graph_material_blocking`]. The default falls back to the eager
    /// `load_all` — only the redb backend (the only one with a `graph_meta`-keyed
    /// durable catalog cheaper than a full dump) overrides it. Selected by
    /// `main.rs` when `EPISTEMIC_GRAPH_LAZY_STARTUP` resolves true. Explicit env
    /// values win; an unset production profile defaults to catalog-only recovery,
    /// while development preserves eager recovery.
    async fn load_catalog(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        self.load_all(state).await
    }

    /// Record a single DATA mutation and await its durable commit
    /// (CONCEPT:EG-KG.backend.authoritative-dispatch commit-before-ack barrier).
    /// Dispatch awaits this before acknowledging the write, so a write is never
    /// acknowledged unless it is durable. The enqueue must
    /// still fold into the SAME group-commit batch as every other concurrent
    /// awaiting writer (one fsync per batch, NOT one fsync per op) — it returns
    /// only after the `WriteTransaction` carrying this op has committed. On a
    /// durable-commit failure it returns `Err(_)`, and dispatch turns that into an
    /// ERROR response (the write did NOT land).
    ///
    /// Backpressure (NOT drop): a full writer queue must block/await capacity or
    /// fail loudly — never silently discard the mutation.
    ///
    /// Implementations must provide a real awaited commit barrier.
    async fn record_durable(&self, graph_fname: &str, method: &Method) -> Result<(), String>;

    /// Commit one canonical mutation batch as the authoritative all-or-nothing
    /// unit.  Implementations must persist operation rows, terminal status,
    /// idempotency, result, and outbox before returning `Ok`; they must apply
    /// backpressure rather than drop work when a bounded writer queue is full.
    ///
    /// The default fails closed. Falling back to independent single-operation commits
    /// would reintroduce the exact acknowledged-partial-commit bug this contract
    /// exists to remove.
    async fn commit_mutation_batch(
        &self,
        _graph_fname: &str,
        _batch: &MutationBatch,
        _result_msgpack: Option<&[u8]>,
        _committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        Err("persistence backend does not support atomic MutationBatch commits".to_string())
    }

    /// Commit authenticated staged graph material with its MutationBatch metadata
    /// in one transaction. Implementations must verify the descriptor digest before
    /// replacing or updating any row; the default fails closed.
    ///
    /// `audited`: the caller's already-resolved `MutationPlan::audited` (or
    /// equivalent `eg_capabilities::policy(method).audited`) for the ORIGINAL
    /// method, captured BEFORE it was compiled into this batch's operations.
    /// State-backed operations are compiled into an opaque digest receipt that no
    /// longer carries the original method's identity, so an implementation must
    /// use this flag -- not the compiled operation -- to decide whether to append
    /// a tamper-evident audit-chain entry (e.g. `TouchNodes` is durable but
    /// intentionally unaudited).
    async fn commit_mutation_batch_state(
        &self,
        _graph_fname: &str,
        _batch: &MutationBatch,
        _authoritative_state_msgpack: Vec<u8>,
        _result_msgpack: Option<&[u8]>,
        _committed_at_ms: u64,
        _audited: bool,
    ) -> Result<MutationBatchCommit, String> {
        Err("persistence backend does not support atomic staged-state commits".to_string())
    }

    /// Commit graph rows and non-topology cross-modal projections through the
    /// universal MutationBatch kernel. Implementations must write every modality,
    /// terminal status, OCC/fencing state, idempotency row, result and outbox in one
    /// durability transaction. The default fails closed; a data-only
    /// `commit_crossmodal` is not an acceptable substitute for a public mutation.
    async fn commit_mutation_batch_crossmodal(
        &self,
        _graph_fname: &str,
        _batch: &MutationBatch,
        _methods: &[Method],
        _vectors: &[(String, Vec<f32>)],
        _blob_refs: &[(String, String)],
        _measurements: &[crate::MeasurementBatch],
        _result_msgpack: Option<&[u8]>,
        _committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        Err(
            "persistence backend does not support atomic cross-modal MutationBatch commits"
                .to_string(),
        )
    }

    /// Read durable terminal status for retry/restart reconciliation.  `None`
    /// means no batch with this id committed on the graph's owning shard.
    async fn read_mutation_batch(
        &self,
        _graph_fname: &str,
        _batch_id: &str,
    ) -> Result<Option<MutationBatchRecord>, String> {
        Ok(None)
    }

    /// Durable OCC version used by compact row-local batches without fetching a
    /// complete graph image.
    async fn read_mutation_graph_version(&self, _graph_fname: &str) -> Result<Option<u64>, String> {
        Ok(None)
    }

    /// Read immutable transactional outbox rows for projection/replay workers.
    async fn read_mutation_outbox(
        &self,
        _graph_fname: &str,
        _batch_id: &str,
    ) -> Result<Vec<MutationOutboxRecord>, String> {
        Ok(Vec::new())
    }

    /// Atomically lease pending outbox rows for one projection consumer. The
    /// default fails closed because a read-then-mark implementation can duplicate
    /// or lose projection work under concurrency.
    async fn claim_mutation_outbox(
        &self,
        _graph_fname: &str,
        _consumer: &str,
        _now_ms: u64,
        _lease_ms: u64,
        _limit: usize,
    ) -> Result<Vec<MutationOutboxLease>, String> {
        Err("persistence backend does not support durable outbox claims".to_string())
    }

    /// Acknowledge an exact lease and advance the named projection cursor in one
    /// durable transaction. A stale/expired lease must fail without advancing.
    async fn ack_mutation_outbox(
        &self,
        _graph_fname: &str,
        _lease: &MutationOutboxLease,
        _projection: &str,
        _now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        Err("persistence backend does not support durable outbox acknowledgements".to_string())
    }

    async fn read_mutation_projection_cursor(
        &self,
        _graph_fname: &str,
        _projection: &str,
        _tenant: &str,
    ) -> Result<Option<MutationProjectionCursor>, String> {
        Ok(None)
    }

    /// Latest lifecycle generation for a graph, used to fence stale retries.
    async fn read_mutation_lifecycle_head(
        &self,
        _graph_fname: &str,
    ) -> Result<Option<String>, String> {
        Ok(None)
    }

    /// Commit the engine-native governed ingest unit. Implementations must not
    /// decompose this into graph/object/governance/cursor writes.
    async fn commit_change_envelope(
        &self,
        _graph_fname: &str,
        _envelope: &ChangeEnvelope,
        _committed_at_ms: u64,
    ) -> Result<ChangeEnvelopeCommit, String> {
        Err("persistence backend does not support atomic ChangeEnvelope commits".to_string())
    }

    async fn read_change_envelope(
        &self,
        _graph_fname: &str,
        _envelope_id: &str,
    ) -> Result<Option<ChangeEnvelopeRecord>, String> {
        Ok(None)
    }

    async fn read_content_version(
        &self,
        _graph_fname: &str,
        _tenant: &str,
        _object_id: &str,
    ) -> Result<Option<ContentVersion>, String> {
        Ok(None)
    }

    async fn read_change_cursor(
        &self,
        _graph_fname: &str,
        _tenant: &str,
        _source: &str,
        _partition: &str,
    ) -> Result<Option<ChangeCursor>, String> {
        Ok(None)
    }

    /// Durably register a graph's identity (CONCEPT:EG-KG.backend.authoritative-dispatch). The
    /// per-mutation write path persists node/edge rows but not the graph's
    /// name/type, yet `load_all` rebuilds the registry from a durable graph manifest
    /// (redb `graph_meta`). So dispatch calls this when a graph is created (and it
    /// must commit durably) so a `kill -9` between creation and the next checkpoint
    /// still recovers the graph under its real name/type. Idempotent and fail-closed
    /// unless implemented by the backend.
    async fn register_graph(
        &self,
        _graph_fname: &str,
        _name: &str,
        _graph_type: crate::protocol::GraphType,
    ) -> Result<(), String> {
        Err("persistence backend cannot register graph identity".to_string())
    }

    /// Durably remove ALL state for a graph when its tenant is DELETED
    /// (CONCEPT:EG-KG.backend.tenant-delete-recreate-same). The default per-mutation write path persists rows keyed by
    /// the graph's sanitized name but has NO row-removal on `DeleteGraph`; that left
    /// a deleted tenant's nodes/edges/meta resident in the durable tier, so a
    /// recreate of the SAME name inherited them via the read-through-on-RAM-miss path
    /// and via `load_all` — silently corrupting/dropping the new tenant's writes.
    /// Dispatch awaits this on `DeleteGraph` so the purge is
    /// COMMITTED before the delete is acked (commit-before-ack), making a same-name
    /// recreate start from a clean durable slate with no race. Idempotent and
    /// fail-closed unless implemented by the backend.
    async fn purge_graph(&self, _graph_fname: &str) -> Result<(), String> {
        Err("persistence backend cannot purge graph state".to_string())
    }

    /// Read a single node's stored properties back from the durable tier
    /// (CONCEPT:EG-KG.backend.authoritative-dispatch read-through). Returns `Ok(None)` when the node is not
    /// present durably, `Ok(Some(props))` when it is. The default is `Ok(None)`
    /// — only an authoritative backend that can serve a RAM-miss read implements
    /// it. Provided so a future read-through-on-eviction path has a backend seam
    /// without another trait revision; see the eviction note in `persist.rs`.
    async fn read_node(
        &self,
        _graph_fname: &str,
        _node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    /// Confirm durable presence for a batch of eviction candidates. The default
    /// fails closed (all false); an authoritative backend overrides this with one
    /// storage snapshot so a memory sweep never opens one transaction per node.
    fn durable_node_presence(
        &self,
        _graph_fname: &str,
        node_ids: &[String],
    ) -> Result<Vec<bool>, String> {
        Ok(vec![false; node_ids.len()])
    }

    /// SYNC read-through of a single node's stored properties (CONCEPT:EG-KG.storage.read-through-seam-exercised).
    /// The eg-core read path (`GraphCore::get_node_properties`) is synchronous, so
    /// the read-through it consults on a RAM miss must be sync too. The redb backend
    /// already performs its point-read over a blocking channel to its off-reactor
    /// writer thread, so a sync variant is natural and adds no async runtime to
    /// eg-core. Same return contract as [`read_node`].
    fn read_node_blocking(
        &self,
        _graph_fname: &str,
        _node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    /// SYNC durable-material fetch for a lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog,
    /// DIST-P2-3): the WHOLE graph's nodes/edges/semantic-store blob, replayed into
    /// a freshly constructed `GraphCore` on catalog-only → resident promotion
    /// (mirrors `read_node_blocking`'s sync read-through contract, but for a whole
    /// graph rather than one node). Default `Ok(None)` — only the redb backend
    /// (which alone has a durable per-graph dump cheaper than reconstructing from
    /// scratch) overrides it; a backend with no lazy-materialize support just
    /// gets an empty core on lazy-open (the rebuildable-cache model's contract).
    fn read_graph_material_blocking(
        &self,
        _graph_fname: &str,
    ) -> Result<Option<crate::registry::GraphMaterial>, String> {
        Ok(None)
    }

    /// Fetch the complete authoritative persistent graph image for an isolated
    /// runtime-result mutation. Unlike the lazy materializer shape this includes
    /// ledger state, and unlike a live RAM snapshot it cannot omit evicted rows.
    async fn read_authoritative_graph_snapshot(
        &self,
        _graph_fname: &str,
    ) -> Result<Option<(crate::graph::GraphSnapshot, u64)>, String> {
        Err("persistence backend cannot stage an authoritative graph snapshot".to_string())
    }

    /// SYNC bounded-PAGE durable-material fetch for a paged lazy first-open
    /// (CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged adjacency") — the working-subset
    /// counterpart of [`Self::read_graph_material_blocking`] backing
    /// `eg_core::registry::GraphRegistry::open_lazy_paged`/`page_in`, mirroring
    /// `eg_core::registry::GraphMaterializer::materialize_page`'s own default/
    /// override split one layer up. The default here falls back to ONE full
    /// [`Self::read_graph_material_blocking`] fetch and then slices it in memory —
    /// correct for ANY backend, but it does not avoid the full-fetch cost AT THE
    /// SOURCE (documented in `eg_core::registry::GraphMaterializer::materialize_page`'s
    /// own doc comment). Only [`redb_backend::RedbBackend`] overrides this with a
    /// genuinely bounded scan straight off its durable store (never collecting the
    /// whole graph into memory first) — the concrete fix for the L38 ledger item.
    fn read_graph_material_page_blocking(
        &self,
        graph_fname: &str,
        cursor: Option<crate::registry::MaterializeCursor>,
        page_size: usize,
    ) -> Result<Option<crate::registry::MaterialPage>, String> {
        // Reuse eg-core's OWN `materialize_page` default slicing algorithm (rather
        // than duplicating its offset arithmetic here) by wrapping the one full
        // fetch in a throwaway `GraphMaterializer` that just returns it. The trait
        // must be IN SCOPE for its default `materialize_page` method to be callable.
        use crate::registry::GraphMaterializer as _;
        struct Once(Option<crate::registry::GraphMaterial>);
        impl crate::registry::GraphMaterializer for Once {
            fn materialize(&self, _graph_name: &str) -> Option<crate::registry::GraphMaterial> {
                self.0.clone()
            }
        }
        let material = self.read_graph_material_blocking(graph_fname)?;
        Ok(Once(material).materialize_page(graph_fname, cursor, page_size))
    }

    /// **Cross-modal ACID commit (CONCEPT:EG-KG.txn.reader-never-sees-node + EG-360).** Land a graph + vector +
    /// blob-ref + measurement + property write-set for ONE graph atomically. The redb
    /// backend overrides this to commit ALL modalities in ONE `WriteTransaction`
    /// (commit-before-ack): on any error nothing lands (full rollback — no partial
    /// cross-modal commit). The default implementation durably commits graph methods;
    /// vectors/blob-refs/measurements require a backend that implements their atomic
    /// storage rather than silently dropping a modality.
    ///
    /// Note (EG-360): staged OWL axioms + SPARQL CONSTRUCT triples are lowered to
    /// graph-native `AddNode`/`AddEdge` at STAGE time and arrive folded into `methods`,
    /// so they ride this same durable path as ordinary graph mutations; only the
    /// genuinely non-graph modalities (vectors/blob-refs/measurements) get their own
    /// arguments + off-redb guard.
    async fn commit_crossmodal(
        &self,
        graph_fname: &str,
        methods: &[Method],
        vectors: &[(String, Vec<f32>)],
        blob_refs: &[(String, String)],
        measurements: &[crate::MeasurementBatch],
    ) -> Result<(), String> {
        if !vectors.is_empty() || !blob_refs.is_empty() || !measurements.is_empty() {
            return Err(
                "cross-modal txn (vectors / blob-refs / measurements) requires the redb persistence backend"
                    .to_string(),
            );
        }
        for m in methods {
            if crate::mutation_apply::is_durable_mutation(m) {
                self.record_durable(graph_fname, m).await?;
            }
        }
        Ok(())
    }

    /// Flush and stop any background writer threads at graceful shutdown.
    /// Idempotent.
    fn shutdown(&self);

    /// Downcast hook (CONCEPT:EG-KG.storage.one-fsync-covers-raft / KG-2.231). The Raft store needs the CONCRETE
    /// [`redb_backend::RedbBackend`] to reach its durable-log API (which is not part
    /// of this trait — the log is a Raft concern, not a general persistence one); the
    /// `security` audit path likewise needs it for `audit_verify_blocking`. So this
    /// lets a caller recover the concrete type from the `Arc<dyn PersistenceBackend>`
    /// in `ServerState`. The default returns `None`; only the redb backend overrides it.
    /// Gated on `redb` (the only build where `RedbBackend` exists) — also consumed by the
    /// M3 catalog-driven resharding admin RPC (CONCEPT:EG-KG.backend.m3-admin-dispatch).
    #[cfg(feature = "redb")]
    fn as_redb(&self) -> Option<&redb_backend::RedbBackend> {
        None
    }
}
