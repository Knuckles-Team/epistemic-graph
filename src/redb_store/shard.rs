//! The graph shard as a kernel-owned store (RF-RULING-004 step 8, RF-RULING-008).
//!
//! One `graph-N.redb` file hosts many graphs. Under the kernel that file is one
//! `OwnerLayout::GraphShard` owner store, and each graph it hosts is one bound
//! serving scope on it, alongside the file's own reserved control scope. This
//! module owns the two types that replace the raw `redb` handles the shard used
//! to pass around:
//!
//! * [`Shard`] replaces `db: &redb::Database`. It holds the file's
//!   [`StorageKernel`], its [`MutationKernel`], the bound control handle, and an
//!   `Arc` cache of one bound handle per graph -- the same shape `eg-tsdb`'s
//!   `SeriesStore` uses per series, and for the same reason:
//!   `OwnedStoreHandle` is not `Clone` because a handle IS a capability, so the
//!   cache hands out `Arc` clones of the one bound handle rather than copies.
//! * [`ShardWrite`] replaces `wtx: &redb::WriteTransaction`. It is a view over
//!   one [`AdmittedGroup`] -- N graph members plus the control member over ONE
//!   physical write transaction -- and hands out the per-member owner-row write
//!   each row access needs, by graph name.
//!
//! Reads have no wrapper: `&ScopedRead<'_, GraphShardOwner>` is already the
//! read-side equivalent of the `rtx: &redb::ReadTransaction` it replaces, and
//! [`Shard::read`] / [`Shard::control_read`] issue it.
//!
//! # Two behaviour bounds this shape imposes
//!
//! **Binding a cold graph is its own preceding write transaction.**
//! `StorageKernel::bind_serving_scope` opens and commits its own
//! `begin_write`, so it cannot run inside a group's transaction (redb admits one
//! writer). A drain that sees a graph this process has never bound therefore
//! binds it first, in a separate transaction, and only then admits the group:
//! the first-ever operation on a new graph costs two fsyncs. Every later
//! operation on it costs one, because the handle is cached and
//! `bind_scope_in` is idempotent for an identical proposed binding.
//!
//! **A group is bounded at `MAX_SHARD_GROUP_GRAPHS` graph members.** The kernel
//! admits at most `eg_storage` `MAX_SCOPE_GROUP_MEMBERS` (1024) members
//! *including* the control member, so one drain covers at most 1023 distinct
//! graphs. A burst touching more flushes in chunks -- see [`chunk_graphs`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "server"))]
use std::sync::Mutex;
use std::sync::{Arc, RwLock};
#[cfg(all(test, feature = "server"))]
use std::sync::{Barrier, OnceLock};

use eg_storage::{
    GraphShardOwner, OwnedStoreHandle, OwnerDomain, PhysicalStoreIdentity, ScopedRead,
    StorageKernel, GRAPH_SHARD_CONTROL_GRAPH, GRAPH_SHARD_TENANT,
};
use eg_transaction::{
    AdmittedGroup, AdmittedOwnerWrite, Begin, CurrentIntent, MutationKernel, OutboxClaimBudget,
    OutboxClaimOutcome, OutboxStatus, ScopedIntent,
};
#[cfg(feature = "server")]
use eg_transaction::{GraftDestination, GraftSource, GraftedScope, OwnerPayloadTransfer};
use eg_types::mutation_batch::COMPILED_BATCH_INCARNATION;
use eg_types::{
    MutationBatch, MutationBatchRecord, MutationOutboxLease, MutationProjectionCursor,
    MutationScopeIdentity,
};

mod batch;
pub(crate) use batch::bind_caller_batch;
use batch::drain_batch;

use super::CommitPhaseTimer;

/// Physical store identity of every graph shard file.
///
/// One identity for all K shards: it names the *kind* of store, and the kernel
/// binds it to a specific file through that file's own store incarnation, so two
/// shards never share an authority despite sharing this name.
pub(crate) const SHARD_PHYSICAL_STORE: &str = "epistemic-graph:graph-shard";

/// Distinct graphs one admitted group may carry.
///
/// `MutationOwnerAuthority::group_write_capabilities` refuses a group whose
/// `members.len() + 1` exceeds `eg-storage`'s `MAX_SCOPE_GROUP_MEMBERS` (1024),
/// and the control member is always present, so the graph budget is one less.
/// Stated here rather than imported because the kernel's constant is private;
/// [`the_group_budget_matches_the_kernels_own_bound`] proves the two agree by
/// admitting a group of exactly this size and refusing one graph more.
pub(crate) const MAX_SHARD_GROUP_GRAPHS: usize = 1023;

/// A bound serving scope on a shard file. Not `Clone` -- a handle IS a
/// capability -- so the cache hands out `Arc` clones of one bound handle.
type ShardHandle = Arc<OwnedStoreHandle<GraphShardOwner>>;

/// Test-only fault boundary for the two durable loser-cleanup steps.  The
/// catalog transaction intentionally commits before reservation cancellation;
/// injecting a failure at that boundary proves the reservation remains the
/// retry handle while the destination is still fenced.
///
/// Armed for ONE NAMED GRAPH, not globally. It used to be a process-wide
/// `AtomicBool` consumed by whichever graft reached the boundary first, and
/// `cargo test` runs this binary's tests in parallel: a sibling graft test
/// (`reserved_import_and_loser_cleanup_are_one_graph_protocol`) could take the
/// fault armed for `losing_graph_reservation_retires_staged_owner_rows_before_reopening`,
/// which left BOTH failing on assertions about state the other test's fault had
/// changed -- while each passed alone at `--test-threads=1`. Keying it by graph
/// name is the same device the rendezvous slots below already use, and it
/// removes the shared state rather than serializing the tests around it.
#[cfg(test)]
static FAIL_AFTER_GRAFT_CATALOG_CLEANUP: Mutex<Option<String>> = Mutex::new(None);

/// Take the armed catalog-cleanup fault if it names `graph_fname`.
#[cfg(test)]
fn take_graft_catalog_cleanup_fault(graph_fname: &str) -> bool {
    let Ok(mut armed) = FAIL_AFTER_GRAFT_CATALOG_CLEANUP.lock() else {
        return false;
    };
    if armed.as_deref() == Some(graph_fname) {
        *armed = None;
        return true;
    }
    false
}

/// A test-only rendezvous barrier armed for one named graph: `None` until a
/// test arms it with `(graph_fname, barrier)`, taken (and cleared) the first
/// time [`take_test_barrier`] sees that same graph name.
#[cfg(all(test, feature = "server"))]
type NamedBarrierSlot = OnceLock<Mutex<Option<(String, Arc<Barrier>)>>>;

/// Test-only rendezvous after the staged owner transaction and before the
/// separate catalog transaction.  The graft/import interleave regression holds
/// the import at this boundary while it attempts the competing cleanup.
#[cfg(all(test, feature = "server"))]
static IMPORT_AFTER_RESERVED_STAGE: NamedBarrierSlot = OnceLock::new();

/// Test-only rendezvous before a graft takes the graph protocol guard.  It lets
/// the interleave regression prove that the competing cleanup has entered the
/// protocol before the staged import is released.
#[cfg(all(test, feature = "server"))]
static GRAFT_BEFORE_PROTOCOL_LOCK: NamedBarrierSlot = OnceLock::new();

#[cfg(all(test, feature = "server"))]
fn take_test_barrier(slot: &NamedBarrierSlot, graph_fname: &str) -> Option<Arc<Barrier>> {
    let mut barrier = slot.get()?.lock().ok()?;
    if barrier
        .as_ref()
        .is_some_and(|(expected_graph, _)| expected_graph == graph_fname)
    {
        barrier.take().map(|(_, barrier)| barrier)
    } else {
        None
    }
}

/// The scope identity of one graph on a shard file.
///
/// Derived from the durable `graph_fname` ALONE (RF-RULING-004 application note
/// 2): the shard's tenant is the reserved `GRAPH_SHARD_TENANT`, never the
/// caller's, because no durable structure records a graph -> tenant binding and
/// the two paths that must bind without one -- the Raft follower apply path and
/// `load_all` recovery -- replay durable entries and have no request carrier.
/// Tenant isolation on a shard is by graph, as it has always been: the shard's
/// OCC counter and every shard table key lead with the graph name.
pub(crate) fn graph_scope_identity(graph_fname: &str) -> Result<MutationScopeIdentity, String> {
    MutationScopeIdentity::fixed_graph(GRAPH_SHARD_TENANT, graph_fname, COMPILED_BATCH_INCARNATION)
}

/// The scope identity of the shard file's own control scope, which owns the
/// Raft log and meta rows, the cross-shard 2PC records, the matview and series
/// key spaces, the encryption canary and the graph catalog.
pub(crate) fn control_scope_identity() -> Result<MutationScopeIdentity, String> {
    MutationScopeIdentity::fixed_graph(
        GRAPH_SHARD_TENANT,
        GRAPH_SHARD_CONTROL_GRAPH,
        COMPILED_BATCH_INCARNATION,
    )
}

/// Split a drained graph list into groups the kernel will admit.
///
/// Deterministic and order-preserving: the caller sorts the drained ops into a
/// `BTreeMap` by graph before calling, so every replica chunks the same burst
/// the same way and applies the chunks in the same order. Each chunk becomes one
/// admitted group -- one `begin_write`, one fsync -- so a burst of more than
/// [`MAX_SHARD_GROUP_GRAPHS`] graphs costs one fsync per chunk rather than
/// failing.
pub(crate) fn chunk_graphs<T: Clone>(graphs: &[T]) -> Vec<Vec<T>> {
    graphs
        .chunks(MAX_SHARD_GROUP_GRAPHS)
        .map(<[T]>::to_vec)
        .collect()
}

/// One graph shard file, opened through the storage kernel.
pub(crate) struct Shard {
    kernel: StorageKernel,
    mutations: MutationKernel,
    /// The exact path this shard was opened with (EH-290 write-amplification
    /// measurement, CONCEPT:EG-KG.storage.commit-ops-phase-timing): a stat of
    /// this path around a commit is the on-disk byte growth that commit cost.
    /// Diagnostic only -- not the kernel's canonicalized physical root -- so a
    /// caller-relative path is exactly as valid here as an absolute one.
    physical_path: PathBuf,
    /// The file's own control scope, bound at open: it exists before any graph
    /// is known, which is what lets the boot scan read the graph catalog.
    control: ShardHandle,
    /// Bound graph scopes, keyed by `graph_fname`. Bound on first use; see the
    /// module doc's note on the two-fsync cost of a graph's first operation.
    graphs: RwLock<BTreeMap<String, ShardHandle>>,
    /// One in-process protocol guard per graph.  Reserved owner imports and
    /// graft cleanup each span multiple durable transactions, so their whole
    /// protocol must be serialized even though each individual redb write is
    /// already serialized by the shard writer.
    #[cfg(feature = "server")]
    graft_protocols: RwLock<BTreeMap<String, Arc<Mutex<()>>>>,
}

impl Shard {
    /// Open (creating if absent) one shard file as a kernel-owned store.
    ///
    /// `create_owner` materializes the WHOLE declared `OwnerLayout::GraphShard`
    /// census, so the hand-written `initialize_canonical_tables` bootstrap the
    /// raw path needed is not repeated here.
    pub(crate) fn open(path: &Path) -> Result<Self, String> {
        let (kernel, mutations, control) = open_kernel_owned_store::<GraphShardOwner>(
            path,
            SHARD_PHYSICAL_STORE,
            &control_scope_identity()?,
        )?;
        Ok(Self {
            kernel,
            mutations,
            physical_path: path.to_path_buf(),
            control,
            graphs: RwLock::new(BTreeMap::new()),
            #[cfg(feature = "server")]
            graft_protocols: RwLock::new(BTreeMap::new()),
        })
    }

    /// The physical shard file's current on-disk size, or 0 if it cannot be
    /// stat'd right now (EH-290: a before/after pair around one `commit_ops`
    /// drain is that drain's on-disk byte growth, the write-amplification
    /// signal the storage durability design calls for). Diagnostic only --
    /// never load-bearing, so a transient stat failure must not become a
    /// write-path error.
    pub(crate) fn physical_file_len(&self) -> u64 {
        std::fs::metadata(&self.physical_path)
            .map(|meta| meta.len())
            .unwrap_or(0)
    }

    pub(crate) fn mutations(&self) -> &MutationKernel {
        &self.mutations
    }

    /// Return the process-local protocol guard for one graph's multi-transaction
    /// graft/import operations.  The durable reservation remains the recovery
    /// authority across restart; this guard only closes the live interleave
    /// between owner staging/catalog metadata and source-writer cancellation.
    #[cfg(feature = "server")]
    pub(crate) fn graft_protocol_guard(&self, graph_fname: &str) -> Result<Arc<Mutex<()>>, String> {
        let mut guards = self
            .graft_protocols
            .write()
            .map_err(|_| "graph graft protocol map is poisoned".to_string())?;
        Ok(Arc::clone(
            guards
                .entry(graph_fname.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        ))
    }

    /// The file's storage kernel.
    ///
    /// Needed by the whole-FILE operations that are the storage kernel's own
    /// and not any scope's: `backup_recovery_store`, `recovery_store_fingerprint`
    /// and `validate_recovery_store`. Handing it out is not a widening -- a
    /// `StorageKernel` mints capabilities, it does not bypass them, and every
    /// row access still goes through a bound scope.
    pub(crate) fn kernel(&self) -> &StorageKernel {
        &self.kernel
    }

    /// The file's control handle.
    pub(crate) fn control(&self) -> &OwnedStoreHandle<GraphShardOwner> {
        &self.control
    }

    /// The bound handle for one graph, binding it on first use.
    ///
    /// The bind is its own write transaction and CANNOT be called while a
    /// group's transaction is open. Callers that are about to admit a group
    /// resolve every handle they need through this first -- see
    /// [`Shard::graph_handles`].
    pub(crate) fn graph(&self, graph_fname: &str) -> Result<ShardHandle, String> {
        crate::redb_store::reject_reserved_graph(graph_fname)?;
        if let Some(handle) = self
            .graphs
            .read()
            .map_err(|_| "graph shard scope cache is poisoned".to_string())?
            .get(graph_fname)
        {
            return Ok(Arc::clone(handle));
        }
        let handle =
            bind_scope::<GraphShardOwner>(&self.kernel, &graph_scope_identity(graph_fname)?)?;
        self.graphs
            .write()
            .map_err(|_| "graph shard scope cache is poisoned".to_string())?
            .insert(graph_fname.to_string(), Arc::clone(&handle));
        Ok(handle)
    }

    /// Resolve every graph handle a group is about to need, binding the cold
    /// ones, BEFORE the group's write transaction is opened.
    ///
    /// This is the one place the two-fsync cost of a new graph is paid, and
    /// keeping it here is what makes the group path itself free of any
    /// transaction-opening call.
    pub(crate) fn graph_handles(&self, graphs: &[String]) -> Result<Vec<ShardHandle>, String> {
        graphs.iter().map(|graph| self.graph(graph)).collect()
    }

    /// The `(name, handle)` members one group needs, binding the cold graphs,
    /// in the caller's order.
    ///
    /// This is the shape every write path wants -- [`Self::admit_drain`],
    /// [`Self::admit_maintenance`] and [`ShardWrite::open`] all take it -- so
    /// the pairing is done once here rather than at each of the shard's write
    /// sites.
    pub(crate) fn graph_members<S: AsRef<str>>(
        &self,
        graphs: &[S],
    ) -> Result<Vec<(String, ShardHandle)>, String> {
        graphs
            .iter()
            .map(|graph| {
                let graph = graph.as_ref();
                Ok((graph.to_string(), self.graph(graph)?))
            })
            .collect()
    }

    /// Forget a graph handle after its serving scope has been retired.
    ///
    /// A cached `OwnedStoreHandle` is a capability for one exact incarnation;
    /// retaining it after `purge_scope_with` or `graft_scope` would make a
    /// same-name recreate reuse a retired binding and fail closed forever. The
    /// identity check keeps a concurrent replacement from being removed by a
    /// late cleanup of the old generation.
    pub(crate) fn forget_graph(
        &self,
        graph_fname: &str,
        retired_identity: &MutationScopeIdentity,
    ) -> Result<(), String> {
        let mut graphs = self
            .graphs
            .write()
            .map_err(|_| "graph shard scope cache is poisoned".to_string())?;
        if graphs
            .get(graph_fname)
            .is_some_and(|handle| handle.identity() == retired_identity)
        {
            graphs.remove(graph_fname);
        }
        Ok(())
    }

    /// Admit one drain: the control member plus one member per graph, each at
    /// its OWN authoritative version resolved inside the group's single write
    /// transaction (RF-RULING-008 + `admit_group_current`).
    ///
    /// `handles` must already be bound -- resolve them through
    /// [`Self::graph_handles`] first, because binding opens its own transaction
    /// and redb admits one writer.
    ///
    /// Returns the group and the batches the kernel built, control member
    /// first, in admission order: [`ShardWrite::open`] pairs them back up.
    /// A coalesced drain has no single caller replay carrier, so every member is
    /// an owner-maintenance write; the outbox retains per-operation attribution.
    pub(crate) fn admit_drain<'a>(
        &'a self,
        members: &'a [(String, ShardHandle)],
        drain_id: &'a str,
    ) -> Result<(AdmittedGroup<'a, GraphShardOwner>, Vec<MutationBatch>), String> {
        self.mutations.admit_group_current(
            self.control_intent(drain_id),
            members.iter().map(move |(graph, handle)| {
                CurrentIntent::maintenance(handle, move |version| {
                    drain_batch(handle, drain_id, graph, version)
                })
            }),
        )
    }

    /// Admit the shard's OWN bookkeeping over the control scope and zero or
    /// more graphs: a graph-meta backfill, a matview or cross-shard record, a
    /// checkpoint apply, a purge.
    ///
    /// The difference from [`Self::admit_drain`] is the class, and the class is
    /// the honest label rather than a knob: none of these writes carries a
    /// caller operation identity, so under RF-RULING-005 they are the ledgered
    /// maintenance class -- durable and version-bumping like any other
    /// mutation, but outside operation-replay conflict semantics entirely.
    /// `members` may be empty, which is the control-only write.
    pub(crate) fn admit_maintenance<'a>(
        &'a self,
        members: &'a [(String, ShardHandle)],
        op_id: &'a str,
    ) -> Result<(AdmittedGroup<'a, GraphShardOwner>, Vec<MutationBatch>), String> {
        self.mutations.admit_group_current(
            self.control_intent(op_id),
            members.iter().map(move |(graph, handle)| {
                CurrentIntent::maintenance(handle, move |version| {
                    drain_batch(handle, op_id, graph, version)
                })
            }),
        )
    }

    /// Admit ONE caller-originated batch on its graph, with the control member
    /// alongside it for the file-wide rows the same commit writes (the graph's
    /// catalog entry, cross-modal series rows, matview state).
    ///
    /// The caller's batch is admitted verbatim -- its own `batch_id`,
    /// idempotency key, fence and outbox intents are the kernel's inputs, which
    /// is what makes an exact retry a replay rather than a second commit. Its
    /// OCC claim is checked against the version resolved INSIDE this
    /// transaction, so the window between the caller's read and this write is
    /// closed rather than retried around.
    ///
    /// `batch.identity` must already be the shard's own scope identity for
    /// `graph` (RF-RULING-004 application note 2): the caller's tenant is
    /// request-boundary authorization and outbox attribution, never part of the
    /// scope. [`bind_caller_batch`] performs that rewrite at the one place the
    /// compiled batch enters the shard; the envelope's serving principal is rebound
    /// while its verified caller remains in the authority context and outbox.
    pub(crate) fn admit_batch<'a>(
        &'a self,
        graph: &'a str,
        handle: &'a ShardHandle,
        batch: &'a MutationBatch,
        op_id: &'a str,
    ) -> Result<(AdmittedGroup<'a, GraphShardOwner>, Vec<MutationBatch>), String> {
        let expected = graph_scope_identity(graph)?.identity_digest();
        if batch.identity.identity_digest() != expected {
            return Err(
                "mutation batch identity is not this graph's shard scope identity".to_string(),
            );
        }
        self.mutations.admit_group_current_control(
            self.control_intent(op_id),
            std::iter::once(ScopedIntent::new(handle, batch)),
        )
    }

    /// Build the shard's control-scope maintenance batch for a later member of
    /// a shared caller transaction.  The first control member is created by
    /// [`Self::admit_batch`]; subsequent envelope members reuse the same
    /// admitted control capability and need the same canonical maintenance
    /// batch shape at its next in-transaction version.
    pub(crate) fn maintenance_batch_at(
        &self,
        operation_id: &str,
        version: u64,
    ) -> Result<MutationBatch, String> {
        drain_batch(
            &self.control,
            operation_id,
            GRAPH_SHARD_CONTROL_GRAPH,
            version,
        )
    }

    /// The control member every shard group carries.
    fn control_intent<'a>(&'a self, op_id: &'a str) -> CurrentIntent<'a, GraphShardOwner> {
        CurrentIntent::maintenance(&self.control, move |version| {
            drain_batch(&self.control, op_id, GRAPH_SHARD_CONTROL_GRAPH, version)
        })
    }

    /// Seal every member of an admitted drain and commit the whole group as ONE
    /// physical transaction.
    ///
    /// Finishing each member is not optional and is not the caller's to
    /// remember: `commit_group` refuses a member whose terminal metadata is
    /// missing ("mutation commit does not match a finished batch"), so the two
    /// steps are one operation here. A member admission resolved to
    /// `Begin::Replay` has a durable receipt already and is skipped -- it wrote
    /// no rows, and re-finishing it would rewrite the receipt of a batch this
    /// drain did not apply.
    pub(crate) fn commit_drain(
        &self,
        group: AdmittedGroup<'_, GraphShardOwner>,
        batches: &[MutationBatch],
        committed_at_ms: u64,
    ) -> Result<(), String> {
        // EH-290 phase split: `ledger_finish` is per-batch terminal-metadata
        // bookkeeping, still inside the one shared transaction (no fsync yet).
        // `durability_commit` is sealing every member and ending the group's
        // transaction -- the redb `commit()` call that performs the actual
        // `Durability::Immediate` fsync. See `CommitPhaseTimer`.
        let ledger_finish = CommitPhaseTimer::start("ledger_finish");
        for (index, batch) in batches.iter().enumerate() {
            let Begin::Apply { source_version } = group.begun(index)? else {
                continue;
            };
            let source_version = *source_version;
            self.mutations.finish(
                group.member(index)?,
                batch,
                None,
                committed_at_ms,
                source_version,
            )?;
        }
        ledger_finish.finish();
        let refs: Vec<&MutationBatch> = batches.iter().collect();
        let durability_commit = CommitPhaseTimer::start("durability_commit");
        let result = self.mutations.commit_group(group, &refs);
        durability_commit.finish();
        result
    }

    // -- the two kernel surfaces this shard reaches through ONE call site each --
    //
    // K4's outbox protocol and `graft_scope` are still under dual review, so
    // every shard-side use of them is funnelled through the wrappers below
    // rather than spread across the movers and the backend. A signature change
    // upstream is then a change here, once, instead of at every call site --
    // which is also the right shape independently: both take a bound handle,
    // and resolving a graph name to its bound handle is this type's job.

    /// Subscribe one durable consumer to a graph's outbox stream.
    ///
    /// A precondition of [`Self::outbox_claim`], not a convenience: the
    /// subscription is the durable record of which topic a consumer reads, and
    /// a claim by an unsubscribed consumer is refused rather than silently
    /// treated as "everything".
    pub(crate) fn outbox_subscribe(
        &self,
        graph_fname: &str,
        consumer: &str,
        topic: &str,
    ) -> Result<(), String> {
        let handle = self.graph(graph_fname)?;
        self.mutations
            .outbox_subscribe(handle.as_ref(), consumer, topic)
    }

    /// Claim up to `limit` pending outbox rows of one graph for one consumer.
    ///
    /// Selection and lease installation share one transaction, so queue
    /// pressure can delay a claim but never lose one. Fairness -- DESIGN.md's
    /// per-tenant weighted round robin with the 25% consecutive cap -- lives in
    /// the budget, which a sweep carries between graphs and which the kernel
    /// also mirrors durably per `(scope, consumer)` so the cap survives a
    /// restart.
    pub(crate) fn outbox_claim(
        &self,
        graph_fname: &str,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        let handle = self.graph(graph_fname)?;
        self.mutations
            .outbox_claim(handle.as_ref(), consumer, budget)
    }

    /// Acknowledge one lease. The ack IS the cursor advance, in one
    /// transaction: a crash between the two is not representable.
    pub(crate) fn outbox_ack(
        &self,
        graph_fname: &str,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        let handle = self.graph(graph_fname)?;
        self.mutations.outbox_ack(handle.as_ref(), lease, now_ms)
    }

    /// Give one lease back unacknowledged, so another worker may claim it now
    /// rather than after it expires.
    pub(crate) fn outbox_release(
        &self,
        graph_fname: &str,
        lease: &MutationOutboxLease,
    ) -> Result<(), String> {
        let handle = self.graph(graph_fname)?;
        self.mutations.outbox_release(handle.as_ref(), lease)
    }

    /// Expire one consumer's timed-out leases; returns how many were reclaimed.
    pub(crate) fn outbox_expire(
        &self,
        graph_fname: &str,
        consumer: &str,
        now_ms: u64,
    ) -> Result<u32, String> {
        let handle = self.graph(graph_fname)?;
        self.mutations
            .outbox_expire(handle.as_ref(), consumer, now_ms)
    }

    /// One consumer's durable projection watermark on one graph.
    pub(crate) fn outbox_cursor(
        &self,
        graph_fname: &str,
        consumer: &str,
    ) -> Result<Option<MutationProjectionCursor>, String> {
        let handle = self.graph(graph_fname)?;
        eg_transaction::outbox_cursor(&self.read(&handle)?, consumer)
    }

    /// One consumer's queue depth, lag and saturation on one graph.
    pub(crate) fn outbox_status(
        &self,
        graph_fname: &str,
        consumer: &str,
        now_ms: u64,
    ) -> Result<OutboxStatus, String> {
        let handle = self.graph(graph_fname)?;
        eg_transaction::outbox_status(&self.read(&handle)?, consumer, now_ms)
    }

    /// Reserve a graph's destination before an online move copies owner rows.
    /// The source must already be bound; the destination reservation is the
    /// only authority allowed to admit the staged owner payload that follows.
    #[cfg(feature = "server")]
    pub(crate) fn reserve_graft_destination(
        &self,
        source: &Shard,
        graph_fname: &str,
    ) -> Result<(), String> {
        let identity = graph_scope_identity(graph_fname)?;
        if source.kernel().owner_authority_digest()? == self.kernel.owner_authority_digest()? {
            return Err("graft source and destination use the same authority".to_string());
        }
        if !source.kernel().scope_binding_exists(&identity)? {
            return Err("graft source scope is not bound".to_string());
        }
        let source_handle = source.graph(graph_fname)?;
        if source_graft_marker(self, source, &source_handle)?
            == SourceGraftMarker::ForAnotherDestination
        {
            return Err(FOREIGN_GRAFT_MARKER.to_string());
        }
        let destination_handle = self.graph(graph_fname)?;
        if !self.graft_destination_reserved(graph_fname)? {
            let existing =
                crate::server::persistence::online_reshard::export_graph_raw(self, graph_fname)?;
            if !existing.empty_owner_payload() {
                return Err("graft destination scope already carries owner payload".to_string());
            }
        }
        let destination =
            GraftDestination::new(self.mutations(), self.kernel(), destination_handle.as_ref());
        source
            .mutations()
            .graft_reserve_destination(source_handle.as_ref(), &destination)
    }

    /// Run one owner-only staging transaction under the destination's exact
    /// graft reservation. The source authority digest binds the staged rows to
    /// the source that will later provide Phase B's ledger copy.
    #[cfg(feature = "server")]
    pub(crate) fn stage_graft_owner_payload<F>(
        &self,
        source: &Shard,
        graph_fname: &str,
        stage: F,
    ) -> Result<(), String>
    where
        F: for<'cap, 'store> FnOnce(
            &eg_transaction::GraftOwnerWrite<'cap, 'store, GraphShardOwner>,
        ) -> Result<(), String>,
    {
        let identity = graph_scope_identity(graph_fname)?;
        let destination_handle = self.graph(graph_fname)?;
        let destination =
            GraftDestination::new(self.mutations(), self.kernel(), destination_handle.as_ref());
        let source_digest = hex::encode(source.kernel().owner_authority_digest()?);
        self.mutations.graft_stage_destination_payload(
            &destination,
            &identity,
            &source_digest,
            stage,
        )
    }

    /// Whether this graph carries a durable graft reservation.  The read is
    /// used only to select the owner-only staging path; the subsequent write
    /// re-proves the exact reservation and its source/destination digests.
    #[cfg(feature = "server")]
    pub(crate) fn graft_destination_reserved(&self, graph_fname: &str) -> Result<bool, String> {
        let handle = self.graph(graph_fname)?;
        let read = self.read(&handle)?;
        Ok(eg_transaction::read_batches(&read)?.iter().any(|record| {
            record
                .batch
                .batch_id
                .starts_with("kernel.graft/reservation/")
        }))
    }

    /// Re-enter the authenticated graft path for a destination that already
    /// owns a reservation when the source has durably chosen another target.
    ///
    /// The scope-binding check is deliberately before `graph()`: a wrong-target
    /// refusal must not bind an absent destination.  Once a reservation is
    /// proven to exist, `graft_begin` rechecks the source marker while holding
    /// the source writer and invokes the kernel's proof-gated loser cleanup.  A
    /// retained reservation is left fenced as the recovery handle; this helper
    /// never clears it by observation alone.
    #[cfg(feature = "server")]
    fn cleanup_wrong_target_reservation(
        &self,
        source: &Shard,
        graph_fname: &str,
        source_handle: &ShardHandle,
        identity: &MutationScopeIdentity,
    ) -> Result<(), String> {
        if !self.kernel.scope_binding_exists(identity)?
            || !self.graft_destination_reserved(graph_fname)?
        {
            return Ok(());
        }
        let destination_handle = self.graph(graph_fname)?;
        let destination = retiring_graft_destination(self, &destination_handle);
        let source_digest = hex::encode(source.kernel().owner_authority_digest()?);
        // `graft_destination_reserved` is intentionally broad because import
        // selection only needs to know whether the owner-only path may apply.
        // Cleanup has a destructive catalog side effect, so it must first prove
        // the exact source/target reservation.  An unrelated reservation is a
        // live recovery handle and cannot authorize deleting this graph's
        // catalog.
        if !self
            .mutations()
            .graft_reservation_matches(&destination, identity, &source_digest)?
        {
            return Ok(());
        }
        // `graph_meta` is file-wide and therefore cannot be swept through the
        // graph owner capability.  Reservation admission proved this row was
        // absent; a row observed here is separately staged payload for this
        // graph.  Remove only this graph's catalog entry while the reservation
        // still fences the destination.  If this transaction fails, the
        // source-writer proof below is never attempted and the reservation stays
        // available for retry; a crash after this commit has the same property.
        let remove_staged_catalog = || -> Result<(), String> {
            let has_staged_catalog = {
                let read = self.control_read()?;
                let catalog = read
                    .open_owner_table(crate::redb_store::GRAPH_META)
                    .map_err(|error| error.to_string())?;
                let value = catalog
                    .get(graph_fname)
                    .map_err(|error| error.to_string())?;
                value.is_some()
            };
            if has_staged_catalog {
                crate::redb_store::remove_graph_catalog_row(self, graph_fname)?;
            }
            Ok(())
        };
        remove_staged_catalog()?;
        #[cfg(test)]
        if take_graft_catalog_cleanup_fault(graph_fname) {
            return Err("injected failure after durable graft catalog cleanup".to_string());
        }

        let _ = source
            .mutations()
            .graft_begin(source_handle.as_ref(), &destination);

        // The source-writer proof and the destination transaction are the only
        // authority for cancellation.  If either proof was inconclusive, keep
        // the reservation fenced and let the caller return its original
        // wrong-target refusal.
        if self.graft_destination_reserved(graph_fname)? {
            return Ok(());
        }
        Ok(())
    }

    /// Stage owner rows through the unique reservation already held by this
    /// destination.  The kernel authenticates the source digest from that
    /// durable receipt, so the command need not trust an out-of-band source
    /// string.
    #[cfg(feature = "server")]
    pub(crate) fn stage_reserved_graft_owner_payload<F>(
        &self,
        graph_fname: &str,
        stage: F,
    ) -> Result<(), String>
    where
        F: for<'cap, 'store> FnOnce(
            &eg_transaction::GraftOwnerWrite<'cap, 'store, GraphShardOwner>,
        ) -> Result<(), String>,
    {
        let identity = graph_scope_identity(graph_fname)?;
        let destination_handle = self.graph(graph_fname)?;
        let destination =
            GraftDestination::new(self.mutations(), self.kernel(), destination_handle.as_ref());
        let result = self
            .mutations
            .graft_stage_reserved_payload(&destination, &identity, stage);
        #[cfg(all(test, feature = "server"))]
        if let Some(barrier) = take_test_barrier(&IMPORT_AFTER_RESERVED_STAGE, graph_fname) {
            // Trip the barrier on BOTH outcomes. It is a rendezvous, not a
            // success signal: the waiting test has already committed to two
            // parties arriving. Skipping it when `result` is an error stranded
            // the test on `Barrier::wait` FOREVER -- an operation that failed
            // was reported as a hang instead of as the assertion failure it is,
            // and a hung test binary silently truncates the whole run. The
            // error still propagates below and fails the test on its merits.
            crate::test_rendezvous::meet(&barrier, "import staged-payload rendezvous");
            // The first trip tells the test that the staged transaction has
            // committed; the second keeps this import paused until the
            // competing cleanup has attempted the same graph guard.
            crate::test_rendezvous::meet(&barrier, "import staged-payload release");
        }
        result
    }

    /// Move one graph's whole ledger out of `source` into this shard.
    ///
    /// The kernel's, not the mover's: under one mutation authority a domain
    /// crate cannot write a ledger row, and re-admitting each moved batch is
    /// not equivalent to moving it -- re-admission re-executes effects,
    /// re-emits outbox rows, stamps new receipts, and resets the version an
    /// in-flight OCC expectation depends on. The graft copies every ledger row
    /// verbatim, retires the source binding and the source's owner rows, and
    /// preserves the version, which is what makes an online move survivable.
    ///
    /// The owner rows are transferred through the graft's reservation-authenticated
    /// payload callback in the same Phase-B destination transaction; the graft
    /// retires the source's payload as part of the move. This wrapper establishes
    /// the reservation/marker before entering the kernel copy, and uses
    /// marker-backed recovery when Phase C already retired the source.
    #[cfg(feature = "server")]
    pub(crate) fn graft_graph_from(
        &self,
        source: &Shard,
        graph_fname: &str,
    ) -> Result<GraftedScope, String> {
        let mut transfer =
            crate::server::persistence::shard_migrate::GraphShardPayloadTransfer::without_report();
        self.graft_graph_from_with_payload(source, graph_fname, Some(&mut transfer))
    }

    /// Move one graph and, when requested, stage its owner payload through the
    /// exact destination reservation.  Owner rows never arrive as an ordinary
    /// maintenance batch, so the ledger remains at the reservation baseline
    /// until the kernel graft copies it.
    #[cfg(feature = "server")]
    pub(crate) fn graft_graph_from_with_payload(
        &self,
        source: &Shard,
        graph_fname: &str,
        payload: Option<&mut dyn OwnerPayloadTransfer<GraphShardOwner, GraphShardOwner>>,
    ) -> Result<GraftedScope, String> {
        #[cfg(all(test, feature = "server"))]
        if let Some(barrier) = take_test_barrier(&GRAFT_BEFORE_PROTOCOL_LOCK, graph_fname) {
            crate::test_rendezvous::meet(&barrier, "graft before-protocol-lock rendezvous");
        }
        let protocol_guard = self.graft_protocol_guard(graph_fname)?;
        let _protocol_guard = protocol_guard
            .lock()
            .map_err(|_| "graph graft protocol guard is poisoned".to_string())?;
        let source_identity = graph_scope_identity(graph_fname)?;
        // Refusal preflight must be side-effect free.  Binding the destination
        // before proving a source/recovery state would leave a live scope after
        // a same-authority or absent-source refusal.
        if source.kernel().owner_authority_digest()? == self.kernel.owner_authority_digest()? {
            return Err("graft source and destination use the same authority".to_string());
        }
        let source_bound = source.kernel().scope_binding_exists(&source_identity)?;
        if !source_bound && !self.kernel.scope_binding_exists(&source_identity)? {
            return Err(
                "graft source scope is not bound and destination has no recovery binding"
                    .to_string(),
            );
        }
        // A completed Phase C retires the source binding.  Do not call
        // `graph()` in that state: binding the old name again would resurrect
        // the retired scope and defeat the destination marker's recovery
        // proof.  The recovery API authenticates the destination marker and
        // proves source binding absence directly from the source kernel.
        if !source_bound {
            return recover_retired_graft(self, source, graph_fname, &source_identity);
        }
        let source_handle = source.graph(graph_fname)?;
        // Prove that the source can be opened before creating or rebinding a
        // destination handle.  A corrupt/missing source must fail without
        // leaving a newly bound destination behind for a later retry.
        let source_marker = source_graft_marker(self, source, &source_handle)?;
        if source_marker == SourceGraftMarker::ForAnotherDestination {
            self.cleanup_wrong_target_reservation(
                source,
                graph_fname,
                &source_handle,
                &source_identity,
            )?;
            return Err(FOREIGN_GRAFT_MARKER.to_string());
        }
        let destination_handle = self.graph(graph_fname)?;
        let destination = retiring_graft_destination(self, &destination_handle);
        if source_marker == SourceGraftMarker::Absent {
            self.reserve_graft_destination(source, graph_fname)?;
        }
        // Phase A is part of the storage move's cutover contract.  Establish
        // the destination reservation and source maximum fence before phase B
        // asks the transaction kernel to copy the ledger.  Calling graft_scope
        // directly leaves the source without the durable marker it is required
        // to prove, which makes every backup/reshard/migration caller fail at
        // the same precondition.
        source
            .mutations()
            .graft_begin(source_handle.as_ref(), &destination)?;
        let payload = payload
            .ok_or_else(|| "graph shard graft requires an owner payload transfer".to_string())?;
        let grafted = self.mutations.graft_scope_with_payload(
            GraftSource::new(source.mutations(), source.kernel(), source_handle.as_ref())
                .with_owner_payload(&crate::redb_store::GraphShardRetirement),
            &destination,
            payload,
        )?;
        source.forget_graph(graph_fname, &source_identity)?;
        Ok(grafted)
    }

    /// Seal and commit a group whose ONE graph member carries a caller batch,
    /// handing that member's receipt back.
    ///
    /// [`Self::commit_drain`] is the coalesced form and records no result; a
    /// caller batch has one, and the receipt it produces is what the caller is
    /// waiting for. A member whose admission resolved to [`Begin::Replay`]
    /// already has a durable receipt: it wrote nothing, it must not be
    /// re-finished, and its stored receipt is the answer.
    pub(crate) fn commit_batch(
        &self,
        group: AdmittedGroup<'_, GraphShardOwner>,
        batches: &[MutationBatch],
        result_msgpack: Option<Vec<u8>>,
        committed_at_ms: u64,
    ) -> Result<CommittedBatch, String> {
        const MEMBER: usize = 1;
        if batches.len() != 2 {
            return Err("a caller batch commits exactly one graph member".to_string());
        }
        if let Begin::Replay(record) = group.begun(MEMBER)? {
            let record = record.as_ref().clone();
            // The control member still has real rows to seal -- the catalog
            // entry and any file-wide row this commit wrote -- so the group
            // commits even though the caller's member replayed.
            self.finish_member(&group, batches, 0, None, committed_at_ms)?;
            let refs: Vec<&MutationBatch> = batches.iter().collect();
            self.mutations.commit_group(group, &refs)?;
            return Ok(CommittedBatch {
                record,
                replayed: true,
            });
        }
        self.finish_member(&group, batches, 0, None, committed_at_ms)?;
        let record =
            self.finish_member(&group, batches, MEMBER, result_msgpack, committed_at_ms)?;
        let refs: Vec<&MutationBatch> = batches.iter().collect();
        self.mutations.commit_group(group, &refs)?;
        let record =
            record.ok_or_else(|| "admitted caller batch produced no receipt".to_string())?;
        Ok(CommittedBatch {
            record,
            replayed: false,
        })
    }

    /// Seal ONE member, returning its receipt. A replayed member is skipped:
    /// re-finishing it would rewrite the receipt of a batch this commit did not
    /// apply.
    fn finish_member(
        &self,
        group: &AdmittedGroup<'_, GraphShardOwner>,
        batches: &[MutationBatch],
        index: usize,
        result_msgpack: Option<Vec<u8>>,
        committed_at_ms: u64,
    ) -> Result<Option<MutationBatchRecord>, String> {
        let Begin::Apply { source_version } = group.begun(index)? else {
            return Ok(None);
        };
        let source_version = *source_version;
        let batch = batches
            .get(index)
            .ok_or_else(|| "admitted scope group has no batch for that member".to_string())?;
        self.mutations
            .finish(
                group.member(index)?,
                batch,
                result_msgpack,
                committed_at_ms,
                source_version,
            )
            .map(Some)
    }

    /// One kernel-issued scoped read bounded to one graph's rows.
    pub(crate) fn read(
        &self,
        owner: &OwnedStoreHandle<GraphShardOwner>,
    ) -> Result<ScopedRead<'_, GraphShardOwner>, String> {
        self.kernel.read_scope(owner)
    }

    /// One kernel-issued scoped read on the file's control scope: the graph
    /// catalog, the Raft rows, the cross-shard records and the other key spaces
    /// that belong to the file rather than to any one graph.
    pub(crate) fn control_read(&self) -> Result<ScopedRead<'_, GraphShardOwner>, String> {
        self.kernel.read_scope(&self.control)
    }
}

#[cfg(feature = "server")]
const FOREIGN_GRAFT_MARKER: &str =
    "graft source carries a marker for a different authority or destination";

/// What the source's durable graft marker says about this destination.
#[cfg(feature = "server")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceGraftMarker {
    /// No graft has begun from the source.
    Absent,
    /// A graft to exactly this (source authority, destination authority) began.
    ForThisDestination,
    /// A graft to another authority or destination began.
    ForAnotherDestination,
}

/// Classify the source's graft marker against this destination.
///
/// The destination's mutation and storage kernels must first prove one
/// authority, since the marker binds the destination by that digest.
#[cfg(feature = "server")]
fn source_graft_marker(
    destination: &Shard,
    source: &Shard,
    source_handle: &ShardHandle,
) -> Result<SourceGraftMarker, String> {
    let source_digest = hex::encode(source.kernel().owner_authority_digest()?);
    let destination_storage_digest = destination.kernel.owner_authority_digest()?;
    if destination.mutations().owner_authority_digest()? != destination_storage_digest {
        return Err(
            "graft destination mutation and storage kernels use different authorities".to_string(),
        );
    }
    let destination_digest = hex::encode(destination_storage_digest);
    Ok(
        match source
            .mutations()
            .graft_source_marker(source_handle.as_ref())?
        {
            None => SourceGraftMarker::Absent,
            Some(marker)
                if marker.source == source_digest && marker.destination == destination_digest =>
            {
                SourceGraftMarker::ForThisDestination
            }
            Some(_) => SourceGraftMarker::ForAnotherDestination,
        },
    )
}

/// The graft destination over `handle` that may retire staged owner rows.
#[cfg(feature = "server")]
fn retiring_graft_destination<'a>(
    shard: &'a Shard,
    handle: &'a ShardHandle,
) -> GraftDestination<'a, GraphShardOwner> {
    GraftDestination::new(shard.mutations(), shard.kernel(), handle.as_ref())
        .with_owner_payload(&crate::redb_store::GraphShardRetirement)
}

/// Finish a graft whose Phase C already retired the source binding.
///
/// `graph()` is never called on the source here: binding the old name again
/// would resurrect the retired scope. The recovery API authenticates the
/// destination marker and proves the source binding absent from the source
/// kernel directly.
#[cfg(feature = "server")]
fn recover_retired_graft(
    destination: &Shard,
    source: &Shard,
    graph_fname: &str,
    source_identity: &MutationScopeIdentity,
) -> Result<GraftedScope, String> {
    let destination_handle = destination.graph(graph_fname)?;
    let graft_destination = GraftDestination::new(
        destination.mutations(),
        destination.kernel(),
        destination_handle.as_ref(),
    );
    let recovered = destination.mutations.graft_recover(
        source.kernel(),
        source_identity,
        &graft_destination,
    )?;
    source.forget_graph(graph_fname, source_identity)?;
    Ok(recovered)
}

/// Authenticate and bind ONE serving scope on a kernel-owned store, generic
/// over the store's owner domain. The proof bytes are the composition root's;
/// the caller supplies only the identity and picks the layout through `D`.
/// Shared by every kernel-owned store this crate binds a scope on -- a graph
/// shard's control/graph scopes here, the KV store's per-namespace scopes, and
/// the blob store's per-(tenant, resource) scopes -- which differ only in
/// which [`OwnerDomain`] they authenticate against.
pub(crate) fn bind_scope<D: OwnerDomain>(
    kernel: &StorageKernel,
    scope: &MutationScopeIdentity,
) -> Result<Arc<OwnedStoreHandle<D>>, String> {
    let authority = crate::store_authority::process_authority();
    let grant = kernel.authenticate_scope::<D>(
        authority.as_ref(),
        scope.clone(),
        authority.principal().to_string(),
        &authority.proof(),
    )?;
    kernel.bind_serving_scope(grant, 0).map(Arc::new)
}

/// The bootstrap sequence shared by every kernel-owned store this crate opens:
/// open the file if it exists, else create it; split the kernel into its read
/// half and its one mutation authority; wrap that authority in a
/// [`MutationKernel`]; authenticate and bind the store's one bootstrap/control
/// scope; and run that scope's ledger bootstrap. A graph shard's file control
/// scope and the KV store's cross-namespace bootstrap scope both start this
/// way, diverging only afterward in what else the caller's struct holds.
pub(crate) fn open_kernel_owned_store<D: OwnerDomain>(
    path: &Path,
    physical_name: &str,
    scope_identity: &MutationScopeIdentity,
) -> Result<(StorageKernel, MutationKernel, Arc<OwnedStoreHandle<D>>), String> {
    let physical = PhysicalStoreIdentity::new(physical_name)?;
    let kernel = if path.exists() {
        StorageKernel::open_owner::<D>(path, physical, None)
    } else {
        StorageKernel::create_owner::<D>(path, physical, None)
    }?;
    let (kernel, authority) = kernel.into_read_and_mutation_authority()?;
    let mutations = MutationKernel::new(authority);
    let bound = bind_scope::<D>(&kernel, scope_identity)?;
    mutations.bootstrap_ledger(&bound)?;
    Ok((kernel, mutations, bound))
}

/// What one committed caller batch produced.
///
/// `replayed` is the kernel's answer, not a pre-check the shard ran first: an
/// exact retry resolves to [`Begin::Replay`] at admission and its durable
/// receipt IS the result, so there is no second idempotency authority to
/// consult and no window between checking and committing.
pub(crate) struct CommittedBatch {
    pub(crate) record: MutationBatchRecord,
    pub(crate) replayed: bool,
}

/// The owner-row writes of one admitted scope group, addressable by graph.
///
/// Replaces the `wtx: &redb::WriteTransaction` the shard used to thread through
/// every row helper. The substitution at each row access is mechanical:
///
/// ```text
/// wtx.open_table(NODES)      ->  w.graph(g)?.open_scoped_table(NODES)   // 41 scope-prefixed
/// wtx.open_table(RAFT_LOG)   ->  w.control().open_table(RAFT_LOG)       // 12 file-wide
/// ```
///
/// **Table-handle discipline.** Holding several members' owner writes at once is
/// fine -- they open no tables by themselves -- but `redb` refuses to open one
/// table twice in a transaction while the first handle is alive. Two graph
/// members writing `nodes` must therefore open it in turn: finish one graph's
/// row work, drop its tables, then start the next. The per-graph loop the
/// coalesced write path uses does exactly that.
pub(crate) struct ShardWrite<'g> {
    control: AdmittedOwnerWrite<'g, GraphShardOwner>,
    graphs: BTreeMap<String, AdmittedOwnerWrite<'g, GraphShardOwner>>,
}

impl<'g> ShardWrite<'g> {
    /// Open the owner-row admission of every member of `group`.
    ///
    /// `members[i]` is the `(graph_fname, handle, batch)` that member `i + 1`
    /// was admitted with -- member 0 is always the control member -- and
    /// `control` is the control member's own handle and batch. The kernel has
    /// already bound each member to its scope; this only opens the owner-row
    /// gate on each, which is what `open_scoped_table` and `open_table` require.
    pub(crate) fn open(
        shard: &Shard,
        group: &'g AdmittedGroup<'g, GraphShardOwner>,
        members: &[(String, ShardHandle)],
        batches: &[MutationBatch],
    ) -> Result<Self, String> {
        if batches.len() != members.len() + 1 {
            return Err("admitted scope group and its batches disagree on length".to_string());
        }
        let control = group.control().owner_rows(shard.control(), &batches[0])?;
        let mut graphs = BTreeMap::new();
        for (index, (graph, handle)) in members.iter().enumerate() {
            let write = group
                .member(index + 1)?
                .owner_rows(handle, &batches[index + 1])?;
            if graphs.insert(graph.clone(), write).is_some() {
                return Err("an admitted scope group may not repeat a graph".to_string());
            }
        }
        Ok(Self { control, graphs })
    }

    /// The control member's owner-row write: the 12 file-wide shard tables.
    pub(crate) fn control(&self) -> &AdmittedOwnerWrite<'g, GraphShardOwner> {
        &self.control
    }

    /// One graph member's owner-row write: the 41 scope-prefixed shard tables,
    /// bounded to that graph's rows by the capability, not by an argument.
    pub(crate) fn graph(
        &self,
        graph_fname: &str,
    ) -> Result<&AdmittedOwnerWrite<'g, GraphShardOwner>, String> {
        self.graphs
            .get(graph_fname)
            .ok_or_else(|| format!("'{graph_fname}' is not a member of this admitted scope group"))
    }

    /// The graphs this group carries, in deterministic order.
    pub(crate) fn graphs(&self) -> impl Iterator<Item = &str> {
        self.graphs.keys().map(String::as_str)
    }

    /// Close every member's owner-row admission. Dropping one unfinished
    /// poisons the shared transaction, so this is not optional.
    pub(crate) fn finish(self) -> Result<(), String> {
        for (_, write) in self.graphs {
            write.finish_owner()?;
        }
        self.control.finish_owner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redb_store::{GRAPH_META, NODES, RAFT_LOG};
    use eg_types::mutation_batch::{authority_scope_for, DurabilityDomain};
    use eg_types::protocol::Method;
    use eg_types::{
        MutationOperation, MutationSurface, VersionExpectation, MUTATION_BATCH_VERSION,
    };
    use redb::ReadableTable;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        crate::redb_store::temp_path("eg-shard-seam", tag)
    }

    fn members(shard: &Shard, graphs: &[&str]) -> Vec<(String, ShardHandle)> {
        let names: Vec<String> = graphs.iter().map(|g| (*g).to_string()).collect();
        let handles = shard.graph_handles(&names).unwrap();
        names.into_iter().zip(handles).collect()
    }

    /// The seam end to end: N graph members plus the control member write their
    /// own rows through ONE admitted group, and every row is durable together.
    ///
    /// This is the property `commit_ops` exists for -- "one fsync, N notified",
    /// with the Raft entry riding the same fsync as the graph rows -- expressed
    /// through the kernel instead of through a raw `WriteTransaction`, and
    /// admitted at each member's in-lock version.
    #[test]
    fn a_group_writes_every_graphs_rows_and_the_control_rows_in_one_transaction() {
        let path = temp_path("group");
        let shard = Shard::open(&path).unwrap();
        let members = members(&shard, &["graph-a", "graph-b"]);
        let (group, batches) = shard.admit_drain(&members, "drain-1").unwrap();

        let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
        // One graph's tables at a time: `redb` refuses a second open of a table
        // whose first handle is still alive, and every graph member writes the
        // same `nodes` table.
        for (graph, _) in &members {
            let mut nodes = write
                .graph(graph)
                .unwrap()
                .open_scoped_table(NODES)
                .unwrap();
            nodes
                .insert((graph.as_str(), "n1"), b"row".as_slice())
                .unwrap();
        }
        write
            .control()
            .open_table(RAFT_LOG)
            .unwrap()
            .insert((1u64, 1u64), b"entry".as_slice())
            .unwrap();
        assert_eq!(
            write.graphs().collect::<Vec<_>>(),
            vec!["graph-a", "graph-b"]
        );
        write.finish().unwrap();

        shard.commit_drain(group, &batches, 2).unwrap();
        drop(shard);

        // Reopen: every member's rows survived the one commit, each reachable
        // only through the scope that owns it.
        let shard = Shard::open(&path).unwrap();
        for (graph, _) in &members {
            let handle = shard.graph(graph).unwrap();
            let read = shard.read(&handle).unwrap();
            let nodes = read.scoped_owner_table(NODES).unwrap();
            assert!(
                nodes.get((graph.as_str(), "n1")).unwrap().is_some(),
                "{graph} lost its row"
            );
            assert!(
                nodes.get(("graph-other", "n1")).is_err(),
                "{graph}'s scope reached another graph's rows"
            );
        }
        let control = shard.control_read().unwrap();
        assert!(control
            .open_owner_table(RAFT_LOG)
            .unwrap()
            .get((1u64, 1u64))
            .unwrap()
            .is_some());
        let _ = std::fs::remove_file(&path);
    }

    /// Each member advances its OWN version, and the next drain is admitted at
    /// the version the previous one produced -- resolved in-lock, not carried
    /// from a read the caller took before the transaction existed (G2-B1).
    #[test]
    fn each_drain_is_admitted_at_the_version_the_previous_drain_produced() {
        let path = temp_path("versions");
        let shard = Shard::open(&path).unwrap();
        let members = members(&shard, &["graph-a"]);

        let mut seen = Vec::new();
        for attempt in 0..3 {
            let drain_id = format!("drain-{attempt}");
            let (group, batches) = shard.admit_drain(&members, &drain_id).unwrap();
            seen.push(batches[1].version_expectation);
            let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
            write
                .graph("graph-a")
                .unwrap()
                .open_scoped_table(NODES)
                .unwrap()
                .insert(("graph-a", "n1"), b"row".as_slice())
                .unwrap();
            write.finish().unwrap();
            shard.commit_drain(group, &batches, 2).unwrap();
        }
        assert_eq!(
            seen,
            vec![
                VersionExpectation::Graph(0),
                VersionExpectation::Graph(1),
                VersionExpectation::Graph(2),
            ]
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The catalog is readable on the scope that exists before any graph is
    /// known -- the boot scan's precondition (G2-B2).
    #[test]
    fn the_control_scope_enumerates_the_catalog_before_any_graph_is_bound() {
        let path = temp_path("catalog");
        let shard = Shard::open(&path).unwrap();
        let control = shard.control_read().unwrap();
        // Enumerable, not merely openable: the scan needs `iter`, which only the
        // raw `ReadOnlyTable` a file-wide owner table yields can provide.
        assert_eq!(
            control
                .open_owner_table(GRAPH_META)
                .unwrap()
                .iter()
                .unwrap()
                .count(),
            0
        );
        // The same table through a graph scope is refused, so the catalog has
        // exactly one reader class.
        let handle = shard.graph("graph-a").unwrap();
        let read = shard.read(&handle).unwrap();
        assert!(read.open_owner_table(GRAPH_META).is_err());
        assert!(read.scoped_owner_table(GRAPH_META).is_err());
        let _ = std::fs::remove_file(&path);
    }

    /// A caller cannot bind the file's own control scope as if it were a graph.
    #[test]
    fn the_reserved_control_name_is_not_a_bindable_graph() {
        let path = temp_path("reserved");
        let shard = Shard::open(&path).unwrap();
        assert!(shard.graph(GRAPH_SHARD_CONTROL_GRAPH).is_err());
        let _ = std::fs::remove_file(&path);
    }

    /// A complete caller envelope must not smuggle the shard's internal tenant
    /// through the request boundary and then get rewritten as a graph member.
    #[test]
    fn complete_caller_envelope_rejects_the_reserved_shard_tenant() {
        let path = temp_path("reserved-caller");
        let shard = Shard::open(&path).unwrap();
        let handle = shard.graph("graph-a").unwrap();
        let identity = MutationScopeIdentity::fixed_graph(
            GRAPH_SHARD_TENANT,
            "graph-a",
            COMPILED_BATCH_INCARNATION,
        )
        .unwrap();
        let actor = format!("principal:sha256:{}", "a".repeat(64));
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "reserved-caller-batch".to_string(),
            envelope: super::super::fixture_operation_envelope(
                &identity,
                &actor,
                42,
                "reserved-caller-idem",
            ),
            identity,
            placement_epoch: 0,
            version_expectation: VersionExpectation::Graph(0),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Graph,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "node-a".to_string(),
                    properties_msgpack: Vec::new(),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 1,
        };
        batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();

        let error = bind_caller_batch(handle.as_ref(), "graph-a", &batch).unwrap_err();
        assert!(error.contains("reserved scope tenant"), "got: {error}");
        let _ = std::fs::remove_file(&path);
    }

    /// A complete caller envelope must retain the logical authority contract:
    /// a sanitized physical spelling in a non-reserved caller tenant is
    /// rejected before the batch can be rebound onto the shard scope.
    #[test]
    fn complete_caller_envelope_rejects_physical_spelling_before_rebind() {
        let path = temp_path("physical-caller");
        let shard = Shard::open(&path).unwrap();
        let logical_identity = MutationScopeIdentity::fixed_graph(
            "tenant-a",
            "tenant:scope",
            COMPILED_BATCH_INCARNATION,
        )
        .unwrap();
        let malformed_identity = MutationScopeIdentity::fixed_graph(
            "tenant-a",
            "tenant~3ascope",
            COMPILED_BATCH_INCARNATION,
        )
        .unwrap();
        let physical_graph = crate::redb_store::sanitize("tenant~3ascope");
        let handle = shard.graph(&physical_graph).unwrap();
        let actor = format!("principal:sha256:{}", "a".repeat(64));
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "physical-caller-batch".to_string(),
            envelope: super::super::fixture_operation_envelope(
                &logical_identity,
                &actor,
                43,
                "physical-caller-idem",
            ),
            identity: logical_identity,
            placement_epoch: 0,
            version_expectation: VersionExpectation::Graph(0),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Graph,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "node-a".to_string(),
                    properties_msgpack: Vec::new(),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 1,
        };
        batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        batch.identity = malformed_identity;

        let error = bind_caller_batch(handle.as_ref(), &physical_graph, &batch).unwrap_err();
        assert!(
            error.contains("canonical ASCII identifier alphabet"),
            "got: {error}"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// A source marker for one destination must be authenticated before a
    /// different destination is bound. Refusing the wrong target therefore
    /// leaves both the source and the attempted destination untouched.
    #[cfg(feature = "server")]
    #[test]
    fn graft_marker_for_another_destination_refuses_before_binding() {
        let source_path = temp_path("graft-marker-source");
        let first_destination_path = temp_path("graft-marker-first-destination");
        let second_destination_path = temp_path("graft-marker-second-destination");
        let source = Shard::open(&source_path).unwrap();
        let first_destination = Shard::open(&first_destination_path).unwrap();
        let second_destination = Shard::open(&second_destination_path).unwrap();
        let identity = graph_scope_identity("graph-a").unwrap();
        let source_handle = source.graph("graph-a").unwrap();
        let first_handle = first_destination.graph("graph-a").unwrap();
        let first = GraftDestination::new(
            first_destination.mutations(),
            first_destination.kernel(),
            first_handle.as_ref(),
        );
        source
            .mutations()
            .graft_begin(source_handle.as_ref(), &first)
            .unwrap();

        let error = second_destination
            .graft_graph_from(&source, "graph-a")
            .unwrap_err();
        assert!(
            error.contains("different authority or destination"),
            "got: {error}"
        );
        assert!(
            !second_destination
                .kernel()
                .scope_binding_exists(&identity)
                .unwrap(),
            "wrong-target refusal bound the destination"
        );
        assert!(
            source.kernel().scope_binding_exists(&identity).unwrap(),
            "wrong-target refusal altered the source binding"
        );

        let _ = std::fs::remove_file(&source_path);
        let _ = std::fs::remove_file(&first_destination_path);
        let _ = std::fs::remove_file(&second_destination_path);
    }

    /// A destination may already be reserved for a different source while a
    /// source marker chooses another winner.  The broad reservation scan is
    /// enough to select the owner-only path, but it is not authorization to
    /// delete this graph's catalog: only the exact `(source, destination)`
    /// reservation may authorize wrong-target cleanup.
    #[cfg(feature = "server")]
    #[test]
    fn unrelated_reservation_preserves_catalog_payload_and_fence() {
        let source_path = temp_path("graft-unrelated-source");
        let other_source_path = temp_path("graft-unrelated-other-source");
        let winner_path = temp_path("graft-unrelated-winner");
        let destination_path = temp_path("graft-unrelated-destination");
        let source = Shard::open(&source_path).unwrap();
        let other_source = Shard::open(&other_source_path).unwrap();
        let winner = Shard::open(&winner_path).unwrap();
        let destination = Shard::open(&destination_path).unwrap();
        let graph = "graph-unrelated-reservation";
        let identity = graph_scope_identity(graph).unwrap();
        let source_handle = source.graph(graph).unwrap();
        let _other_source_handle = other_source.graph(graph).unwrap();
        let winner_handle = winner.graph(graph).unwrap();
        let destination_handle = destination.graph(graph).unwrap();
        let winner_target =
            GraftDestination::new(winner.mutations(), winner.kernel(), winner_handle.as_ref());
        let destination_target = GraftDestination::new(
            destination.mutations(),
            destination.kernel(),
            destination_handle.as_ref(),
        );

        // D is reserved by S2, and has owner rows staged under that exact
        // reservation.  The catalog row is deliberately committed in the
        // separate control transaction used by the importer.
        destination
            .reserve_graft_destination(&other_source, graph)
            .unwrap();
        destination
            .stage_reserved_graft_owner_payload(graph, |write| {
                write
                    .open_scoped_table(NODES)?
                    .insert((graph, "s2-staged"), b"payload-s2".as_slice())
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
            .unwrap();
        crate::redb_store::write_graph_meta(
            &destination,
            graph,
            "catalog-s2",
            crate::protocol::GraphType::Global,
        )
        .unwrap();
        let catalog_before = {
            let read = destination.control_read().unwrap();
            let table = read.open_owner_table(GRAPH_META).unwrap();
            table
                .get(graph)
                .unwrap()
                .map(|value| value.value().to_vec())
        };
        let payload_before = {
            let read = destination.read(&destination_handle).unwrap();
            read.scoped_owner_table(NODES)
                .unwrap()
                .get((graph, "s2-staged"))
                .unwrap()
                .map(|value| value.value().to_vec())
        };
        let fence_before = {
            let read = destination.read(&destination_handle).unwrap();
            eg_transaction::read_fences(&read)
                .unwrap()
                .expect("the S2 reservation fences its destination")
        };

        // S1 has a durable marker for W.  There is no S1->D reservation, so D
        // must refuse without deleting S2's catalog, payload, or recovery
        // fence.
        winner.reserve_graft_destination(&source, graph).unwrap();
        source
            .mutations()
            .graft_begin(source_handle.as_ref(), &winner_target)
            .unwrap();
        let error = destination.graft_graph_from(&source, graph).unwrap_err();
        assert!(
            error.contains("different authority or destination"),
            "got: {error}"
        );

        assert!(destination.graft_destination_reserved(graph).unwrap());
        let source2_digest = hex::encode(other_source.kernel().owner_authority_digest().unwrap());
        assert!(destination
            .mutations()
            .graft_reservation_matches(&destination_target, &identity, &source2_digest,)
            .unwrap());
        let catalog_after = {
            let read = destination.control_read().unwrap();
            let table = read.open_owner_table(GRAPH_META).unwrap();
            table
                .get(graph)
                .unwrap()
                .map(|value| value.value().to_vec())
        };
        assert_eq!(catalog_after, catalog_before);
        let payload_after = {
            let read = destination.read(&destination_handle).unwrap();
            read.scoped_owner_table(NODES)
                .unwrap()
                .get((graph, "s2-staged"))
                .unwrap()
                .map(|value| value.value().to_vec())
        };
        assert_eq!(payload_after, payload_before);
        let fence_after = {
            let read = destination.read(&destination_handle).unwrap();
            eg_transaction::read_fences(&read)
                .unwrap()
                .expect("the unrelated reservation remains fenced")
        };
        assert_eq!(
            (fence_after.placement_epoch, fence_after.fencing_token),
            (fence_before.placement_epoch, fence_before.fencing_token)
        );
        assert_eq!(
            (fence_after.placement_epoch, fence_after.fencing_token),
            (u64::MAX, u64::MAX)
        );

        let _ = std::fs::remove_file(&source_path);
        let _ = std::fs::remove_file(&other_source_path);
        let _ = std::fs::remove_file(&winner_path);
        let _ = std::fs::remove_file(&destination_path);
    }

    /// A graph owner may have staged rows while two destination reservations
    /// race. A different authenticated source marker can cancel the losing
    /// reservation only after the GraphShard retirement capability purges that
    /// payload in the same destination transaction; reopening the graph must
    /// then start from the empty v0 baseline.
    #[cfg(feature = "server")]
    #[test]
    fn losing_graph_reservation_retires_staged_owner_rows_before_reopening() {
        let source_path = temp_path("graft-owner-source");
        let winner_path = temp_path("graft-owner-winner");
        let loser_path = temp_path("graft-owner-loser");
        let source = Shard::open(&source_path).unwrap();
        let winner = Shard::open(&winner_path).unwrap();
        let loser = Shard::open(&loser_path).unwrap();
        let source_handle = source.graph("graph-a").unwrap();
        let winner_handle = winner.graph("graph-a").unwrap();
        let loser_handle = loser.graph("graph-a").unwrap();
        let winner_target =
            GraftDestination::new(winner.mutations(), winner.kernel(), winner_handle.as_ref());
        // Reserve and stage through the authenticated destination path before
        // the source marker chooses the winner. This is the case a
        // LedgerOnly-only cancellation test cannot distinguish.
        loser.reserve_graft_destination(&source, "graph-a").unwrap();
        loser
            .stage_reserved_graft_owner_payload("graph-a", |write| {
                write
                    .open_scoped_table(NODES)?
                    .insert(("graph-a", "loser-staged"), b"staged".as_slice())
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
            .unwrap();
        let staged = loser.read(&loser_handle).unwrap();
        assert!(staged
            .scoped_owner_table(NODES)
            .unwrap()
            .get(("graph-a", "loser-staged"))
            .unwrap()
            .is_some());
        drop(staged);
        // `graph_meta` is file-wide, so this deliberately models the separate
        // control transaction used by the reshard importer.  The target graph
        // had no catalog at reservation time; an unrelated graph's catalog is
        // the preservation control against an over-broad cleanup.
        crate::redb_store::write_graph_meta(
            &loser,
            "graph-b",
            "stable-graph-b",
            crate::protocol::GraphType::Global,
        )
        .unwrap();
        crate::redb_store::write_graph_meta(
            &loser,
            "graph-a",
            "staged-graph-a",
            crate::protocol::GraphType::Global,
        )
        .unwrap();

        winner
            .reserve_graft_destination(&source, "graph-a")
            .unwrap();
        source
            .mutations()
            .graft_begin(source_handle.as_ref(), &winner_target)
            .unwrap();
        *FAIL_AFTER_GRAFT_CATALOG_CLEANUP.lock().unwrap() = Some("graph-a".to_string());
        let retry_error = loser.graft_graph_from(&source, "graph-a").unwrap_err();
        assert!(
            retry_error.contains("injected failure after durable graft catalog cleanup"),
            "{retry_error}"
        );
        assert!(loser.graft_destination_reserved("graph-a").unwrap());
        assert!(
            crate::redb_store::dump::read_catalog_record(&loser, "graph-a")
                .unwrap()
                .is_none()
        );
        assert!(
            crate::redb_store::dump::read_catalog_record(&loser, "graph-b")
                .unwrap()
                .is_some()
        );
        let retained = loser.read(&loser_handle).unwrap();
        assert!(retained
            .scoped_owner_table(NODES)
            .unwrap()
            .get(("graph-a", "loser-staged"))
            .unwrap()
            .is_some());
        let fence = eg_transaction::read_fences(&retained)
            .unwrap()
            .expect("the retry handle remains fenced");
        assert_eq!(fence.placement_epoch, u64::MAX);
        assert_eq!(fence.fencing_token, u64::MAX);
        drop(retained);

        let error = loser.graft_graph_from(&source, "graph-a").unwrap_err();
        assert!(
            error.contains("different authority or destination"),
            "{error}"
        );

        assert!(!loser.graft_destination_reserved("graph-a").unwrap());
        let after_cancel = loser.read(&loser_handle).unwrap();
        assert!(after_cancel
            .scoped_owner_table(NODES)
            .unwrap()
            .get(("graph-a", "loser-staged"))
            .unwrap()
            .is_none());
        drop(after_cancel);
        assert!(
            crate::redb_store::dump::read_catalog_record(&loser, "graph-a")
                .unwrap()
                .is_none()
        );
        assert!(
            crate::redb_store::dump::read_catalog_record(&loser, "graph-b")
                .unwrap()
                .is_some()
        );
        let _ = winner_target;
        drop(loser_handle);
        drop(loser);
        let loser = Shard::open(&loser_path).unwrap();
        let loser_handle = loser.graph("graph-a").unwrap();
        assert!(!loser.graft_destination_reserved("graph-a").unwrap());

        // The canceled loser is a genuine empty v0 scope again, so the next
        // owner write is admitted and visible without inheriting staged rows.
        let members = members(&loser, &["graph-a"]);
        let (group, batches) = loser.admit_drain(&members, "loser-after-cancel").unwrap();
        let write = ShardWrite::open(&loser, &group, &members, &batches).unwrap();
        write
            .graph("graph-a")
            .unwrap()
            .open_scoped_table(NODES)
            .unwrap()
            .insert(("graph-a", "loser-live"), b"live".as_slice())
            .unwrap();
        write.finish().unwrap();
        loser.commit_drain(group, &batches, 2).unwrap();
        let reopened = loser.read(&loser_handle).unwrap();
        let nodes = reopened.scoped_owner_table(NODES).unwrap();
        assert!(nodes.get(("graph-a", "loser-staged")).unwrap().is_none());
        assert!(nodes.get(("graph-a", "loser-live")).unwrap().is_some());
        drop(reopened);

        let _ = std::fs::remove_file(&source_path);
        let _ = std::fs::remove_file(&winner_path);
        let _ = std::fs::remove_file(&loser_path);
    }

    /// A reserved import's owner stage and catalog commit must not be
    /// interleaved with wrong-target cleanup.  The stage deliberately pauses
    /// before its second transaction; cleanup enters the same graph protocol
    /// and therefore cannot delete the catalog and cancel the reservation while
    /// the importer is still able to rewrite it.
    #[cfg(feature = "server")]
    use crate::test_rendezvous::{join_bounded, meet};

    #[test]
    fn reserved_import_and_loser_cleanup_are_one_graph_protocol() {
        let source_path = temp_path("graft-protocol-source");
        let winner_path = temp_path("graft-protocol-winner");
        let loser_path = temp_path("graft-protocol-loser");
        let source = Arc::new(Shard::open(&source_path).unwrap());
        let winner = Arc::new(Shard::open(&winner_path).unwrap());
        let loser = Arc::new(Shard::open(&loser_path).unwrap());
        let source_handle = source.graph("graph-protocol").unwrap();
        let winner_handle = winner.graph("graph-protocol").unwrap();
        let loser_handle = loser.graph("graph-protocol").unwrap();
        let winner_target =
            GraftDestination::new(winner.mutations(), winner.kernel(), winner_handle.as_ref());

        loser
            .reserve_graft_destination(source.as_ref(), "graph-protocol")
            .unwrap();
        loser
            .stage_reserved_graft_owner_payload("graph-protocol", |write| {
                write
                    .open_scoped_table(NODES)?
                    .insert(
                        ("graph-protocol", "staged-before-import"),
                        b"staged".as_slice(),
                    )
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
            .unwrap();
        crate::redb_store::write_graph_meta(
            loser.as_ref(),
            "graph-b",
            "stable-graph-b",
            crate::protocol::GraphType::Global,
        )
        .unwrap();
        // The catalog NAME must sanitize back to the physical key: an exported
        // raw image is refused by `RawGraphRows::durable_identity` ("raw graph
        // rows durable identity does not match its key") when it does not, so
        // the old "staged-graph-protocol" spelling made `import_graph_raw`
        // fail on its FIRST statement -- before the staged-payload hook that
        // this test's rendezvous waits on, which then timed out after 120s and
        // reported a fixture error as a concurrency hang. The staging this test
        // is about is the reserved owner payload written above, not the display
        // name. Corrected 2026-09-11 by the redb_store absolute-green lane (F1).
        crate::redb_store::write_graph_meta(
            loser.as_ref(),
            "graph-protocol",
            "graph-protocol",
            crate::protocol::GraphType::Global,
        )
        .unwrap();
        let mut rows = crate::server::persistence::online_reshard::export_graph_raw(
            loser.as_ref(),
            "graph-protocol",
        )
        .unwrap();
        rows.requires_graft_reservation = true;
        let stale_rows = rows.clone();

        winner
            .reserve_graft_destination(source.as_ref(), "graph-protocol")
            .unwrap();
        source
            .mutations()
            .graft_begin(source_handle.as_ref(), &winner_target)
            .unwrap();

        let stage_barrier = Arc::new(Barrier::new(2));
        assert!(IMPORT_AFTER_RESERVED_STAGE
            .set(Mutex::new(Some((
                "graph-protocol".to_string(),
                Arc::clone(&stage_barrier),
            ))))
            .is_ok());
        let graft_start_barrier = Arc::new(Barrier::new(2));
        assert!(GRAFT_BEFORE_PROTOCOL_LOCK
            .set(Mutex::new(Some((
                "graph-protocol".to_string(),
                Arc::clone(&graft_start_barrier),
            ))))
            .is_ok());
        let import_loser = Arc::clone(&loser);
        let import = std::thread::spawn(move || {
            crate::server::persistence::online_reshard::import_graph_raw(
                import_loser.as_ref(),
                "graph-protocol",
                &rows,
            )
        });
        // The import owns the protocol guard and is paused after its scoped
        // owner transaction, before its separate catalog transaction.
        meet(&stage_barrier, "import staged-payload rendezvous");

        let cleanup_loser = Arc::clone(&loser);
        let cleanup_source = Arc::clone(&source);
        let cleanup = std::thread::spawn(move || {
            cleanup_loser.graft_graph_from(cleanup_source.as_ref(), "graph-protocol")
        });
        // Ensure the competing cleanup has entered the graft operation before
        // probing the actual per-graph mutex held by the paused import.  The
        // cleanup then blocks on that same mutex until the import's catalog
        // transaction completes.
        meet(&graft_start_barrier, "competing graft-start rendezvous");
        let protocol_probe = loser.graft_protocol_guard("graph-protocol").unwrap();
        if protocol_probe.try_lock().is_ok() {
            // Unblock both workers before failing, so the regression never
            // leaves redb files or test threads live after the assertion.
            meet(&stage_barrier, "import staged-payload rendezvous");
            let _ = join_bounded(import, "the paused graft import");
            let _ = join_bounded(cleanup, "the competing graft cleanup");
            panic!("loser cleanup crossed catalog deletion while import held the graph protocol");
        }

        meet(&stage_barrier, "import staged-payload rendezvous");
        assert!(join_bounded(import, "the paused graft import").is_ok());
        let cleanup_result = join_bounded(cleanup, "the competing graft cleanup");
        assert!(cleanup_result.is_err());

        assert!(!loser.graft_destination_reserved("graph-protocol").unwrap());
        assert!(
            crate::redb_store::dump::read_catalog_record(loser.as_ref(), "graph-protocol")
                .unwrap()
                .is_none()
        );
        assert!(
            crate::redb_store::dump::read_catalog_record(loser.as_ref(), "graph-b")
                .unwrap()
                .is_some()
        );
        let stale_import = crate::server::persistence::online_reshard::import_graph_raw(
            loser.as_ref(),
            "graph-protocol",
            &stale_rows,
        )
        .unwrap_err();
        assert!(stale_import.contains("lost its destination reservation"));
        let read = loser.read(&loser_handle).unwrap();
        assert!(read
            .scoped_owner_table(NODES)
            .unwrap()
            .get(("graph-protocol", "staged-before-import"))
            .unwrap()
            .is_none());

        let _ = std::fs::remove_file(&source_path);
        let _ = std::fs::remove_file(&winner_path);
        let _ = std::fs::remove_file(&loser_path);
    }

    /// A request-bound logical identity must not be reinterpreted as an
    /// already-sanitized physical key merely because it contains `~xx`.
    #[test]
    fn caller_physical_spelling_is_rejected_before_shard_rebind() {
        let identity = MutationScopeIdentity::fixed_graph(
            "tenant-a",
            "tenant~3ascope",
            COMPILED_BATCH_INCARNATION,
        )
        .unwrap();
        let error = authority_scope_for(&identity).unwrap_err();
        assert!(
            error.contains("canonical ASCII identifier alphabet"),
            "got: {error}"
        );
    }

    /// A logical graph name may contain punctuation that the shard's physical
    /// key escapes. The owner-row scope remains that physical key across a
    /// restart, while the authority boundary records the complete physical
    /// spelling in its internal subject namespace without widening `ResourceId`
    /// to admit `~`.
    #[test]
    fn escaped_graph_key_keeps_rows_and_authority_scope_across_reopen() {
        let path = temp_path("escaped-reopen");
        let physical = crate::redb_store::sanitize("tenant:scope");
        assert_eq!(physical, "tenant~3ascope");

        {
            let shard = Shard::open(&path).unwrap();
            let members = members(&shard, &[physical.as_str()]);
            let (group, batches) = shard.admit_drain(&members, "drain-escaped").unwrap();
            let eg_types::MutationEnvelope::Maintenance(maintenance) = &batches[1].envelope else {
                panic!("shard drain must use a maintenance envelope");
            };
            assert_eq!(
                maintenance.subject.as_str(),
                "physical:74656e616e747e336173636f7065"
            );
            let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
            write
                .graph(&physical)
                .unwrap()
                .open_scoped_table(NODES)
                .unwrap()
                .insert((physical.as_str(), "n1"), b"row".as_slice())
                .unwrap();
            write.finish().unwrap();
            shard.commit_drain(group, &batches, 2).unwrap();
        }

        {
            let shard = Shard::open(&path).unwrap();
            let handle = shard.graph(&physical).unwrap();
            let read = shard.read(&handle).unwrap();
            assert!(read
                .scoped_owner_table(NODES)
                .unwrap()
                .get((physical.as_str(), "n1"))
                .unwrap()
                .is_some());
        }

        let _ = std::fs::remove_file(&path);
    }

    /// `MAX_SHARD_GROUP_GRAPHS` is the kernel's own bound, not a guess.
    ///
    /// Asserted by admitting a group of exactly that many graph members and
    /// then one more, because `eg-storage`'s `MAX_SCOPE_GROUP_MEMBERS` is
    /// private: if the kernel's budget moved, one of these two halves would
    /// fail.
    #[test]
    fn the_group_budget_matches_the_kernels_own_bound() {
        let path = temp_path("budget");
        let shard = Shard::open(&path).unwrap();
        let names: Vec<String> = (0..=MAX_SHARD_GROUP_GRAPHS)
            .map(|i| format!("graph-{i}"))
            .collect();
        let handles = shard.graph_handles(&names).unwrap();
        let all: Vec<(String, ShardHandle)> = names.into_iter().zip(handles).collect();

        let full = shard.admit_drain(&all[..MAX_SHARD_GROUP_GRAPHS], "drain-full");
        assert!(full.is_ok(), "{:?}", full.as_ref().err());
        shard.mutations().abort_group(full.unwrap().0).unwrap();

        let over = shard.admit_drain(&all, "drain-over");
        assert_eq!(
            over.err().as_deref(),
            Some("admitted scope group exceeds its member budget")
        );
        let _ = std::fs::remove_file(&path);
    }

    /// A burst wider than one group flushes in chunks, deterministically.
    #[test]
    fn a_burst_wider_than_one_group_flushes_in_chunks() {
        let graphs: Vec<usize> = (0..MAX_SHARD_GROUP_GRAPHS * 2 + 7).collect();
        let chunks = chunk_graphs(&graphs);
        assert_eq!(chunks.len(), 3);
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![MAX_SHARD_GROUP_GRAPHS, MAX_SHARD_GROUP_GRAPHS, 7]
        );
        // Order preserving: replicas that drained the same burst chunk it the
        // same way and apply the chunks in the same order.
        assert_eq!(chunks.concat(), graphs);
        assert!(chunk_graphs::<usize>(&[]).is_empty());
    }
}
