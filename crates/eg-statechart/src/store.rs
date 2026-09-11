//! Durable `StatechartDef` + `MachineInstance` store over redb (CONCEPT:INT-P2-2).
//!
//! Mirrors `eg-jobs`' `JobStore` discipline (the most disciplined state primitive in
//! this repo): a single redb file with authoritative tables, msgpack-encoded records
//! decoded through `eg-types`' BOUNDED decoder, per-call guarded state transitions
//! (an invalid *request* is a hard `Err`, never a silent overwrite), monotonic
//! server-issued ids, and — like every other durable record here — an OCC `version`
//! for optimistic compare-and-set updates.
//!
//! Two tables:
//!   * `STATECHART_DEFS`      — `def_id -> msgpack(StatechartDef)`. Content-addressed,
//!     so storing a byte-identical chart twice is idempotent.
//!   * `STATECHART_INSTANCES` — `instance_id -> msgpack(MachineInstance)`, one small
//!     row per running machine.
//!
//! Cluster ordering: every authoritative instance write (`instantiate`/
//! `instantiate_batch` and a FIRING `send_event`) routes through the universal
//! `MutationBatch` / durable-commit gateway (`eg-transaction`) — the SAME
//! `begin` → change-rows → `finish` → `commit` sequence `eg-jobs`' `mutate_job_batch`
//! uses, on the *same* redb `WriteTransaction` that mutates the
//! `statechart_instances` owner row. So each transition carries an atomic terminal batch
//! record, a monotonic domain version, a route fence, an idempotency row and an
//! outbox intent — exactly the evidence a Raft state machine needs to order and
//! replay it. The per-instance OCC `version` compare-and-set is preserved on top
//! of that (it still guards lost updates on a single node). A well-defined NO-OP
//! event still writes nothing — it never opens a batch.
//!
//! **RF-RULING-004/005.** `eg-storage` is the sole physical owner of
//! `statecharts.redb` (declared `OwnerLayout::Statechart`, owner tables
//! `statechart_defs` + `statechart_instances`) and `eg-transaction` the sole
//! writer. `define` used to write `statechart_defs` on a private write
//! transaction outside any batch; there is no un-ledgered write path any more,
//! so it is admitted as a **maintenance** mutation — ledgered, fenced and
//! version-bumping like any other, but carrying no caller identity and
//! therefore outside operation-replay semantics. It stays idempotent by content
//! address: the definition digest is the batch's idempotency key, so storing a
//! byte-identical chart twice replays instead of rewriting.
//!
//! **Determinism (CONCEPT:INT-P2-2, was tracked as D-DE7-2 — now closed).**
//! `instantiate` reads the local wall clock and a local `AtomicU64` instance-id
//! counter; `send_event`'s content-addressed transition batch would embed
//! whichever value it read. Neither is safe to replay byte-identically across Raft
//! replicas, mirroring the exact gap `eg-jobs`' wall-clock `submit` has (that
//! crate's own module doc calls `eg-statechart` its "disciplined sibling" — this is
//! the one property that used to NOT hold). The fix follows `eg-jobs`' own
//! resolution exactly: [`StatechartStore::instantiate_batch`] derives the new
//! instance's id from a caller-supplied, pre-Raft-proposal `request_batch_id`
//! (mirrors `job_id_for_batch`) instead of the local counter, and
//! [`StatechartStore::send_event`] takes an explicit `now_ms: i64` parameter
//! instead of reading the clock internally (mirrors `claim_next`/
//! `checkpoint_fenced`/…). The served `Method::Statechart` handler
//! (`src/server/handlers/statechart.rs`) uses ONLY these deterministic entry
//! points, sourcing `now_ms` from `authoritative_now_ms()` — which resolves to the
//! SAME leader-selected commit time on every replica. `instantiate`'s wall-clock/
//! local-counter form remains for single-node/test callers only (mirrors
//! `eg-jobs::submit`), exactly as `crates/eg-statechart/tests/replay_determinism.rs`
//! now proves.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use eg_storage::{
    OwnedStoreHandle, PhysicalStoreIdentity, ScopeGrantVerifier, ScopedRead, StatechartOwner,
    StorageKernel,
};
use eg_transaction::{Begin, MutationKernel};
use eg_types::mutation_batch::{
    DurabilityDomain, MutationBatch, MutationBatchRecord, MutationEnvelope, MutationOperation,
    MutationOutboxIntent, MutationScopeIdentity, MutationSurface, VersionExpectation,
    MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;
use redb::{ReadableTable, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

mod batches;

use batches::{
    creation_batch, decode_instance_result, definition_batch, instance_batch,
    instance_id_for_request, instance_mutation_identity, STATECHART_PHYSICAL_STORE,
};

use crate::action::apply_all;
use crate::check::validate;
use crate::context::{Context, EventInput};
use crate::instance::{InstanceId, InstanceStatus, MachineInstance};
use crate::model::{DefId, StatechartDef};
use crate::transition::{initial_configuration, step, StepOutcome, TransitionError};

const DEFS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("statechart_defs");
const INSTANCES: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("statechart_instances");

const MAX_STORED_BYTES: usize = 16 * 1024 * 1024;
const MAX_STORED_ITEMS: usize = 1_000_000;
const MAX_ID_BYTES: usize = 256;
const MAX_STRING_BYTES: usize = 4 * 1024;
const MAX_LIST_ITEMS: usize = 100_000;

/// Store error type. Small and string-carrying, mirroring `eg-jobs`' `JobError`: the
/// guard messages ARE the diagnostic, surfaced by callers as plain protocol errors.
#[derive(Debug)]
pub enum StatechartError {
    Redb(String),
    Codec(String),
    NotFound(String),
    /// The submitted DEFINITION failed structural validation (see [`crate::check`]).
    InvalidDefinition(Vec<crate::check::DefError>),
    /// A `send_event` request was itself malformed (unknown state/event, …). Distinct
    /// from a legitimate no-op, which is NOT an error.
    InvalidTransition {
        instance_id: String,
        reason: String,
    },
    /// OCC conflict: the caller's `expected_version` did not match the stored version.
    VersionConflict {
        instance_id: String,
        expected: u64,
        actual: u64,
    },
}

impl std::fmt::Display for StatechartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatechartError::Redb(m) => write!(f, "statechart redb error: {m}"),
            StatechartError::Codec(m) => write!(f, "statechart codec error: {m}"),
            StatechartError::NotFound(id) => write!(f, "statechart record not found: {id}"),
            StatechartError::InvalidDefinition(errors) => {
                write!(f, "invalid statechart definition: ")?;
                for (i, e) in errors.iter().enumerate() {
                    if i > 0 {
                        write!(f, "; ")?;
                    }
                    write!(f, "{e}")?;
                }
                Ok(())
            }
            StatechartError::InvalidTransition { instance_id, reason } => {
                write!(f, "invalid transition on instance {instance_id}: {reason}")
            }
            StatechartError::VersionConflict { instance_id, expected, actual } => write!(
                f,
                "occ conflict on instance {instance_id}: expected version {expected}, found {actual}"
            ),
        }
    }
}

impl std::error::Error for StatechartError {}

type Result<T> = std::result::Result<T, StatechartError>;

fn redb_err<E: std::fmt::Display>(e: E) -> StatechartError {
    StatechartError::Redb(e.to_string())
}
fn codec_err<E: std::fmt::Display>(e: E) -> StatechartError {
    StatechartError::Codec(e.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn decode_stored<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(MAX_STORED_BYTES, MAX_STORED_ITEMS, 64),
    )
    .map_err(|_| codec_err("stored statechart record is invalid"))
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_STRING_BYTES && !value.contains('\0')
}

fn encode_def(def: &StatechartDef) -> Result<Vec<u8>> {
    encode_bounded(def, "statechart definition exceeds storage limits")
}

fn encode_instance(instance: &MachineInstance) -> Result<Vec<u8>> {
    let states_valid = !instance.configuration.active.is_empty()
        && instance
            .configuration
            .active
            .iter()
            .all(|s| valid_identifier(s));
    if instance.instance_id.len() > MAX_ID_BYTES
        || !valid_identifier(&instance.def_id)
        || !states_valid
    {
        return Err(codec_err(
            "statechart instance record exceeds storage limits",
        ));
    }
    encode_bounded(instance, "statechart instance exceeds storage limits")
}

fn encode_bounded<T: Serialize>(value: &T, limit_error: &str) -> Result<Vec<u8>> {
    let bytes = rmp_serde::to_vec_named(value).map_err(codec_err)?;
    if bytes.len() > MAX_STORED_BYTES {
        return Err(codec_err(limit_error));
    }
    Ok(bytes)
}

fn open_statechart_kernel(path: &Path, physical: PhysicalStoreIdentity) -> Result<StorageKernel> {
    let kernel = if path.exists() {
        StorageKernel::open_owner::<StatechartOwner>(path, physical, None)
    } else {
        StorageKernel::create_owner::<StatechartOwner>(path, physical, None)
    };
    kernel.map_err(redb_err)
}

fn read_stored_record<T: DeserializeOwned>(
    read: &ScopedRead<'_, StatechartOwner>,
    table_definition: TableDefinition<'static, &str, &[u8]>,
    key: &str,
) -> Result<T> {
    let table = read.open_owner_table(table_definition).map_err(redb_err)?;
    let row = table.get(key).map_err(redb_err)?;
    row.map_or_else(
        || Err(StatechartError::NotFound(key.to_string())),
        |blob| decode_stored(blob.value()),
    )
}

/// The outcome of a [`StatechartStore::send_event`] call: the (possibly unchanged)
/// durable instance plus the pure transition result that produced it — so a caller
/// sees BOTH the new persisted `(state, context)` and the ordered actions/effects the
/// transition decided.
#[derive(Clone, Debug)]
pub struct SendOutcome {
    /// The durable instance after the event (unchanged on a no-op).
    pub instance: MachineInstance,
    /// The pure step result (its `fired` flag distinguishes a real transition from a
    /// no-op; its `actions` are the effects for the interpreter to run). Configuration-
    /// aware, so it carries the whole next active set for hierarchical/parallel charts.
    pub outcome: StepOutcome,
}

/// A durable statechart store, backed by `statecharts.redb`.
pub struct StatechartStore {
    /// Sole physical owner of `statecharts.redb`. `statechart_defs` and
    /// `statechart_instances` are reachable only through the scoped read and
    /// admitted-owner-write capabilities it issues.
    kernel: StorageKernel,
    /// Sole writer. Holds this file's one move-once mutation authority.
    mutations: MutationKernel,
    /// The one authenticated, bound serving scope -- the fixed native identity
    /// of `instance_mutation_identity`, validated exactly once per open.
    owner: OwnedStoreHandle<StatechartOwner>,
    /// Monotonic instance-id source (mirrors `eg-jobs`' `next_id`): `"sc-<hex>"`.
    next_id: AtomicU64,
}

impl StatechartStore {
    /// Open (or create) the store at an exact file path through the storage
    /// kernel, seeding the id counter from the highest existing id.
    ///
    /// The kernel creates the file under `OwnerLayout::Statechart`, whose
    /// declared owner tables are exactly `statechart_defs` and
    /// `statechart_instances`, so an empty DB is queryable without a
    /// hand-written bootstrap closure. `verifier` is the composition root's
    /// proof authority (RF-RULING-004): only it may decide that `principal` may
    /// serve the fixed `statechart-instances` scope.
    pub fn open(
        path: &Path,
        verifier: &dyn ScopeGrantVerifier,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self> {
        let identity = instance_mutation_identity()?;
        let physical = PhysicalStoreIdentity::new(STATECHART_PHYSICAL_STORE).map_err(redb_err)?;
        let kernel = open_statechart_kernel(path, physical)?;
        let (kernel, authority) = kernel
            .into_read_and_mutation_authority()
            .map_err(redb_err)?;
        let mutations = MutationKernel::new(authority);
        let grant = kernel
            .authenticate_scope::<StatechartOwner>(verifier, identity, principal.to_string(), proof)
            .map_err(redb_err)?;
        let owner = kernel.bind_serving_scope(grant, 0).map_err(redb_err)?;
        mutations.bootstrap_ledger(&owner).map_err(redb_err)?;
        let store = Self {
            kernel,
            mutations,
            owner,
            next_id: AtomicU64::new(0),
        };
        let seed = initialize_next_id(&store.scoped_read()?)?;
        store.next_id.store(seed, Ordering::Relaxed);
        Ok(store)
    }

    /// Open `{persist_dir}/statecharts.redb` — the durable location beside the graph
    /// shards, exactly as `eg-jobs` opens `jobs.redb`.
    pub fn open_in_dir(
        persist_dir: &Path,
        verifier: &dyn ScopeGrantVerifier,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self> {
        std::fs::create_dir_all(persist_dir)
            .map_err(|e| StatechartError::Redb(format!("create persist dir: {e}")))?;
        Self::open(
            &persist_dir.join("statecharts.redb"),
            verifier,
            principal,
            proof,
        )
    }

    /// One kernel-issued scoped read over this store's bound serving scope.
    fn scoped_read(&self) -> Result<ScopedRead<'_, StatechartOwner>> {
        self.kernel.read_scope(&self.owner).map_err(redb_err)
    }

    fn next_instance_id(&self) -> InstanceId {
        let n = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("sc-{n:016x}")
    }

    // ── Definitions ──────────────────────────────────────────────────────────────

    /// Validate and durably store a definition (CONCEPT:INT-P2-2). Content-addressed
    /// and idempotent: the returned [`DefId`] is a pure hash of the definition, so
    /// storing a byte-identical chart again is a no-op that returns the same id. An
    /// invalid definition is rejected BEFORE any write (fail-closed).
    pub fn define(&self, def: &StatechartDef) -> Result<DefId> {
        validate(def).map_err(|report| StatechartError::InvalidDefinition(report.errors))?;
        let def_id = def.def_id();
        let blob = encode_def(def)?;
        // Content-addressed idempotency, resolved by a read before admission:
        // `def_id` is a pure hash of the definition, so a row already under that
        // key holds byte-identical bytes and re-storing it is a no-op. The
        // admitted path still mints its maintenance claim from the current
        // version under the write lock.
        {
            let read = self.scoped_read()?;
            let table = read.open_owner_table(DEFS).map_err(redb_err)?;
            if table.get(def_id.as_str()).map_err(redb_err)?.is_some() {
                return Ok(def_id);
            }
        }
        // The definition's maintenance claim is minted from the authoritative
        // version held by this admission.  The read above is only the cheap
        // content-addressed fast path; it cannot supply the write's fence.
        let (write, batch, begun) = self
            .mutations
            .admit_current(&self.owner, |version| {
                definition_batch(
                    def_id.as_str(),
                    self.owner.identity(),
                    self.owner.principal(),
                    version,
                )
                .map_err(|error| error.to_string())
            })
            .map_err(redb_err)?;
        let source_version = match begun {
            // A concurrent writer committed this exact batch between the read
            // above and this admission. Byte-identical by content address, so
            // the result is the same id and nothing more is written.
            Begin::Replay(_) => {
                write.abort().map_err(redb_err)?;
                return Ok(def_id);
            }
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = write.owner_rows(&self.owner, &batch).map_err(redb_err)?;
        owner_write
            .open_table(DEFS)
            .map_err(redb_err)?
            .insert(def_id.as_str(), blob.as_slice())
            .map_err(redb_err)?;
        owner_write.finish_owner().map_err(redb_err)?;
        self.mutations
            .finish(&write, &batch, Some(blob), 0, source_version)
            .map_err(redb_err)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(def_id)
    }

    /// Fetch a stored definition by id.
    pub fn get_def(&self, def_id: &str) -> Result<StatechartDef> {
        if !valid_identifier(def_id) {
            return Err(codec_err("statechart definition id is invalid"));
        }
        let read = self.scoped_read()?;
        read_stored_record(&read, DEFS, def_id)
    }

    /// List every stored definition id.
    pub fn list_def_ids(&self) -> Result<Vec<DefId>> {
        let read = self.scoped_read()?;
        let table = read.open_owner_table(DEFS).map_err(redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(redb_err)? {
            let (k, _) = entry.map_err(redb_err)?;
            if out.len() >= MAX_LIST_ITEMS {
                return Err(codec_err(
                    "statechart definition list exceeds response limits",
                ));
            }
            out.push(k.value().to_string());
        }
        Ok(out)
    }

    // ── Instances ────────────────────────────────────────────────────────────────

    /// Create a fresh instance of a stored definition in its initial state
    /// (CONCEPT:INT-P2-2). The initial context is seeded from `initial_context`, then
    /// the initial state's Moore `entry` actions are applied (entering s₀ fires its
    /// entry). `version` starts at 0.
    ///
    /// Reads the local wall clock and a local `AtomicU64` counter for the instance
    /// id, exactly like `eg-jobs`' wall-clock `submit`. NOT replay-safe across Raft
    /// replicas (see that method's doc); the served `Method::Statechart` handler
    /// uses [`Self::instantiate_batch`] instead. Kept for single-node/test callers.
    pub fn instantiate(
        &self,
        def_id: &str,
        initial_context: Context,
        tenant: &str,
        actor: &str,
    ) -> Result<MachineInstance> {
        let def = self.get_def(def_id)?;

        // Enter s₀ (descending into its default children for a composite/parallel chart)
        // and fold the ordered entry actions into the seeded context. A stored, validated
        // def always has a real initial state; a missing one maps to a defensive error.
        let (configuration, entry_actions) =
            initial_configuration(&def).map_err(|error| StatechartError::InvalidTransition {
                instance_id: "<new>".to_string(),
                reason: error.to_string(),
            })?;
        let entry_event = EventInput::new("__init__");
        let context = apply_all(initial_context, &entry_actions, &entry_event);

        let now = now_ms();
        let status = if configuration.is_final(&def) {
            InstanceStatus::Final
        } else {
            InstanceStatus::Active
        };
        let instance = MachineInstance {
            instance_id: self.next_instance_id(),
            def_id: def_id.to_string(),
            configuration,
            context,
            version: 0,
            status,
            tenant: tenant.to_string(),
            actor: actor.to_string(),
            events_seen: 0,
            transitions_fired: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        let blob = encode_instance(&instance)?;
        let expected_version = self.mutation_version()?;
        let batch = instance_batch(&instance, &blob, &self.owner, expected_version)?;
        let committed_at_ms = instance.updated_at_ms.max(0) as u64;
        self.commit_instance_blob(&instance, &blob, &batch, committed_at_ms)?;
        Ok(instance)
    }

    /// Create a fresh instance deterministically (CONCEPT:INT-P2-2), mirroring
    /// `eg-jobs`' `submit_batch`: the caller supplies `request_batch_id` — an
    /// opaque identity derived from the REQUEST (e.g. request id + method), fixed
    /// BEFORE Raft proposal and therefore identical on every state-machine replica
    /// — plus the authoritative `committed_at_ms` (the leader-selected commit time
    /// every replica applies via `authoritative_now_ms()`, never a local read of
    /// `SystemTime::now()`). The new instance's id is derived from
    /// `request_batch_id` rather than a local `AtomicU64` counter, exactly as
    /// `eg-jobs::job_id_for_batch` derives a job id from its pre-agreed batch
    /// identity — so no local clock or local counter enters the replicated state.
    /// Returns `(instance, replayed)`: `replayed` is `true` when this exact
    /// request already committed (idempotent replay).
    pub fn instantiate_batch(
        &self,
        def_id: &str,
        initial_context: Context,
        tenant: &str,
        actor: &str,
        request_batch_id: &str,
        committed_at_ms: u64,
    ) -> Result<(MachineInstance, bool)> {
        let def = self.get_def(def_id)?;
        let (configuration, entry_actions) =
            initial_configuration(&def).map_err(|error| StatechartError::InvalidTransition {
                instance_id: "<new>".to_string(),
                reason: error.to_string(),
            })?;
        let entry_event = EventInput::new("__init__");
        let context = apply_all(initial_context, &entry_actions, &entry_event);

        let now = committed_at_ms as i64;
        let status = if configuration.is_final(&def) {
            InstanceStatus::Final
        } else {
            InstanceStatus::Active
        };
        let instance = MachineInstance {
            instance_id: instance_id_for_request(request_batch_id),
            def_id: def_id.to_string(),
            configuration,
            context,
            version: 0,
            status,
            tenant: tenant.to_string(),
            actor: actor.to_string(),
            events_seen: 0,
            transitions_fired: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        let blob = encode_instance(&instance)?;
        let expected_version = self.mutation_version()?;
        let batch = creation_batch(&instance, request_batch_id, &self.owner, expected_version)?;
        self.commit_instance_blob(&instance, &blob, &batch, committed_at_ms)
    }

    /// Persist an instance image through the universal durable-commit gateway, one
    /// all-or-nothing redb txn covering the row write and its terminal batch/
    /// version/fence/idempotency/outbox evidence (mirrors `eg-jobs`'
    /// `put_raw_batch`). `batch` is caller-supplied so both a content-addressed
    /// transition batch ([`instance_batch`]/[`Self::send_event`]) and a
    /// request-addressed creation batch ([`creation_batch`]/
    /// [`Self::instantiate_batch`]) share one commit path. Returns
    /// `(instance, replayed)`: on `Begin::Replay` the REPLAYED image (decoded from
    /// the already-committed record) is returned, which is byte-identical to
    /// `instance` for any batch whose identity is a pure function of the persisted
    /// image or of an idempotent request identity.
    fn commit_instance_blob(
        &self,
        instance: &MachineInstance,
        blob: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(MachineInstance, bool)> {
        let (write, begun) = self.mutations.admit(&self.owner, batch).map_err(redb_err)?;
        match begun {
            Begin::Replay(record) => {
                let replayed = decode_instance_result(&record)?;
                if replayed != *instance {
                    write.abort().map_err(redb_err)?;
                    return Err(codec_err(
                        "replayed statechart instance differs from the requested image",
                    ));
                }
                self.mutations.commit(write, batch).map_err(redb_err)?;
                Ok((replayed, true))
            }
            Begin::Apply { source_version } => {
                let owner_write = write.owner_rows(&self.owner, batch).map_err(redb_err)?;
                {
                    let mut table = owner_write.open_table(INSTANCES).map_err(redb_err)?;
                    stage_instance_row(&mut table, &instance.instance_id, blob)?;
                }
                owner_write.finish_owner().map_err(redb_err)?;
                self.mutations
                    .finish(
                        &write,
                        batch,
                        Some(blob.to_vec()),
                        committed_at_ms,
                        source_version,
                    )
                    .map_err(redb_err)?;
                self.mutations.commit(write, batch).map_err(redb_err)?;
                Ok((instance.clone(), false))
            }
        }
    }

    /// Fetch (rehydrate) an instance by id.
    pub fn get_instance(&self, instance_id: &str) -> Result<MachineInstance> {
        if instance_id.is_empty() || instance_id.len() > MAX_ID_BYTES {
            return Err(codec_err("statechart instance id is invalid"));
        }
        let read = self.scoped_read()?;
        read_stored_record(&read, INSTANCES, instance_id)
    }

    /// Deliver an event to an instance and durably persist the result
    /// (CONCEPT:INT-P2-2). This is the rehydrate → apply-pure-δ → persist cycle in one
    /// call, under one redb write transaction:
    ///
    /// * If `expected_version` is `Some` and disagrees with the stored version, the
    ///   call fails with [`StatechartError::VersionConflict`] and writes nothing (OCC).
    /// * If the pure transition FIRES, the new `(state, context)` is written, `version`
    ///   and `transitions_fired` increment, and `status` becomes `Final` iff the new
    ///   state is in F.
    /// * If the event is a NO-OP (undefined edge or all guards false), nothing is
    ///   written and the unchanged instance is returned — a no-op costs one read.
    /// * A malformed request (event not in Σ, corrupt stored state) is an `Err`.
    ///
    /// `now_ms` is caller-supplied (mirrors `eg-jobs`' fenced transition methods —
    /// `claim_next`/`checkpoint_fenced`/… — which ALL take an explicit `now: i64`
    /// rather than reading `SystemTime::now()` internally): the served
    /// `Method::Statechart` handler passes `authoritative_now_ms()`, which resolves
    /// to the SAME leader-selected commit time on every Raft state-machine replica,
    /// so the resulting instance image — and the `MutationBatch` content-addressed
    /// from it — is byte-identical across replicas.
    pub fn send_event(
        &self,
        instance_id: &str,
        event: &EventInput,
        expected_version: Option<u64>,
        now_ms: i64,
    ) -> Result<SendOutcome> {
        // Read + decide first; the pure transition function does not need the write txn
        // open, and a no-op must not take a write at all.
        let instance = self.get_instance(instance_id)?;
        if let Some(expected) = expected_version {
            if expected != instance.version {
                return Err(StatechartError::VersionConflict {
                    instance_id: instance_id.to_string(),
                    expected,
                    actual: instance.version,
                });
            }
        }
        let def = self.get_def(&instance.def_id)?;
        let outcome = step(&def, &instance.configuration, &instance.context, event)
            .map_err(|error| map_transition_error(instance_id, error))?;

        if !outcome.fired {
            // Well-defined no-op: stay put, persist nothing.
            return Ok(SendOutcome { instance, outcome });
        }

        // A firing transition. Compute the RESULTING instance image up front — the new
        // hierarchical CONFIGURATION the pure `step` transition decided, folded onto a
        // clone of the current instance — so the MutationBatch is content-addressed by
        // the exact bytes we will persist (the key that makes a replayed Raft proposal
        // an idempotent no-op). A well-defined no-op returned above; it never reaches
        // here.
        let next = transitioned_instance(&instance, &def, &outcome, now_ms);
        let blob = encode_instance(&next)?;
        let expected_version = self.mutation_version()?;
        let batch = instance_batch(&next, &blob, &self.owner, expected_version)?;
        commit_transition(
            self,
            instance_id,
            (&instance, next, outcome),
            (blob, batch),
            now_ms.max(0) as u64,
        )
    }

    /// The gateway's monotonic mutation-domain version for the statechart instance
    /// scope — the count of durable instance batches committed through
    /// `eg-mutation-store` (mirrors `eg-jobs`' `mutation_version`). A cluster layer
    /// reads this to order/replay instance transitions.
    pub fn mutation_version(&self) -> Result<u64> {
        eg_transaction::version(&self.scoped_read()?).map_err(redb_err)
    }

    /// List instance ids, optionally filtered to one definition. Diagnostic/admin use;
    /// ownership filtering is the caller's responsibility (see the dispatch handler).
    pub fn list_instance_ids(&self, def_id: Option<&str>) -> Result<Vec<InstanceId>> {
        let read = self.scoped_read()?;
        let table = read.open_owner_table(INSTANCES).map_err(redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(redb_err)? {
            let (k, v) = entry.map_err(redb_err)?;
            if out.len() >= MAX_LIST_ITEMS {
                return Err(codec_err(
                    "statechart instance list exceeds response limits",
                ));
            }
            match def_id {
                None => out.push(k.value().to_string()),
                Some(want) => {
                    let instance: MachineInstance = decode_stored(v.value())?;
                    if instance.def_id == want {
                        out.push(k.value().to_string());
                    }
                }
            }
        }
        Ok(out)
    }

    /// List full instance records owned by `(tenant, actor)`, optionally filtered to
    /// one definition — the ownership-scoped listing the handler surfaces to a caller.
    pub fn list_owned_instances(
        &self,
        tenant: &str,
        actor: &str,
        def_id: Option<&str>,
    ) -> Result<Vec<MachineInstance>> {
        let read = self.scoped_read()?;
        let table = read.open_owner_table(INSTANCES).map_err(redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(redb_err)? {
            let (_, v) = entry.map_err(redb_err)?;
            if out.len() >= MAX_LIST_ITEMS {
                return Err(codec_err(
                    "statechart instance list exceeds response limits",
                ));
            }
            let instance: MachineInstance = decode_stored(v.value())?;
            let owned = instance.tenant == tenant && instance.actor == actor;
            let matches_def = def_id.is_none_or(|want| instance.def_id == want);
            if owned && matches_def {
                out.push(instance);
            }
        }
        Ok(out)
    }
}

fn stage_instance_row(
    table: &mut redb::Table<'_, &str, &[u8]>,
    instance_id: &str,
    blob: &[u8],
) -> Result<()> {
    table.insert(instance_id, blob).map_err(redb_err)?;
    Ok(())
}

fn transitioned_instance(
    instance: &MachineInstance,
    def: &StatechartDef,
    outcome: &StepOutcome,
    now_ms: i64,
) -> MachineInstance {
    let mut next = instance.clone();
    next.configuration = outcome.next.clone();
    next.context = outcome.next_context.clone();
    next.version = next.version.saturating_add(1);
    next.transitions_fired = next.transitions_fired.saturating_add(1);
    next.events_seen = next.events_seen.saturating_add(1);
    next.status = if next.configuration.is_final(def) {
        InstanceStatus::Final
    } else {
        InstanceStatus::Active
    };
    next.updated_at_ms = now_ms;
    next
}

fn commit_transition(
    store: &StatechartStore,
    instance_id: &str,
    // `transition` groups the pre-transition instance, the computed resulting
    // instance image, and the pure `step` outcome that produced it.
    transition: (&MachineInstance, MachineInstance, StepOutcome),
    // `payload` groups the encoded instance blob and the mutation batch built
    // from it — the exact bytes this commit will persist.
    payload: (Vec<u8>, MutationBatch),
    committed_at_ms: u64,
) -> Result<SendOutcome> {
    let (previous, next, outcome) = transition;
    let (blob, batch) = payload;
    // Commit the row change and its terminal batch/version/fence/idempotency/
    // outbox evidence through the `eg-transaction` gateway `eg-jobs` uses, on
    // the one admitted write. Inside that write the row is re-read and its OCC
    // `version` re-checked (compare-and-set) so a concurrent writer cannot be
    // silently clobbered — the per-instance guard is preserved on top of the
    // gateway.
    let (write, begun) = store
        .mutations
        .admit(&store.owner, &batch)
        .map_err(redb_err)?;
    match begun {
        Begin::Replay(record) => {
            // This exact resulting image already committed durably. Validate
            // the durable image before consuming the fresh replay nonce; a
            // mismatch is corruption or an identity bug and must abort.
            let replayed = decode_instance_result(&record)?;
            if replayed != next {
                write.abort().map_err(redb_err)?;
                return Err(codec_err(
                    "replayed statechart transition differs from the requested image",
                ));
            }
            store.mutations.commit(write, &batch).map_err(redb_err)?;
            Ok(SendOutcome {
                instance: replayed,
                outcome,
            })
        }
        Begin::Apply { source_version } => {
            // Re-open the row inside the admitted owner write and re-check the
            // OCC version. The inner block owns every borrow of `INSTANCES` and
            // yields a plain `Result<(), u64>` — `Ok(())` staged the write,
            // `Err(actual)` found a compare-and-set conflict — so the table
            // borrow is dropped before the owner capability is finished.
            let owner_write = write.owner_rows(&store.owner, &batch).map_err(redb_err)?;
            let occ: std::result::Result<(), u64> = {
                let mut table = owner_write.open_table(INSTANCES).map_err(redb_err)?;
                let current_bytes = {
                    let guard = table
                        .get(instance_id)
                        .map_err(redb_err)?
                        .ok_or_else(|| StatechartError::NotFound(instance_id.to_string()))?;
                    guard.value().to_vec()
                };
                let current: MachineInstance = decode_stored(&current_bytes)?;
                if current.version != previous.version {
                    // Someone advanced the instance between our read and our write.
                    Err(current.version)
                } else {
                    stage_instance_row(&mut table, instance_id, &blob)?;
                    Ok(())
                }
            };
            // Always close the owner capability explicitly: dropping it
            // unfinished poisons the write, which would mask the OCC conflict
            // this method must report.
            owner_write.finish_owner().map_err(redb_err)?;
            match occ {
                Ok(()) => {
                    store
                        .mutations
                        .finish(&write, &batch, Some(blob), committed_at_ms, source_version)
                        .map_err(redb_err)?;
                    store.mutations.commit(write, &batch).map_err(redb_err)?;
                    Ok(SendOutcome {
                        instance: next,
                        outcome,
                    })
                }
                Err(actual) => {
                    // Nothing was staged; discard the whole admitted write so the
                    // conflict costs no durable evidence.
                    write.abort().map_err(redb_err)?;
                    Err(StatechartError::VersionConflict {
                        instance_id: instance_id.to_string(),
                        expected: previous.version,
                        actual,
                    })
                }
            }
        }
    }
}

fn map_transition_error(instance_id: &str, error: TransitionError) -> StatechartError {
    StatechartError::InvalidTransition {
        instance_id: instance_id.to_string(),
        reason: error.to_string(),
    }
}

/// Seed the monotonic id counter from the highest `sc-<hex>` id already present, so ids
/// never collide across restarts (mirrors `eg-jobs`' `max_job_sequence` backfill).
fn initialize_next_id(read: &ScopedRead<'_, StatechartOwner>) -> Result<u64> {
    let table = read.open_owner_table(INSTANCES).map_err(redb_err)?;
    let mut max = 0u64;
    for entry in table.iter().map_err(redb_err)? {
        let (k, _) = entry.map_err(redb_err)?;
        if let Some(seq) = instance_seq(k.value()) {
            max = max.max(seq);
        }
    }
    Ok(max)
}

fn instance_seq(id: &str) -> Option<u64> {
    id.strip_prefix("sc-")
        .and_then(|hex| u64::from_str_radix(hex, 16).ok())
}

/// Test-only composition root. Production supplies the real scope-grant proof
/// authority; this one still checks every field the kernel hands it, so a store
/// opened with the wrong layout, scope or principal fails closed in tests too.
#[cfg(test)]
pub(crate) mod test_support {
    use super::batches::INSTANCE_MUTATION_TENANT;
    use super::*;
    use eg_storage::{OwnerLayout, PhysicalStoreIdentity};

    pub(crate) const TEST_PRINCIPAL: &str =
        "principal:sha256:5f2b41b1e0dc61f2b6a4c0a1ee4b9a53d3d2fbb4a45c2d6c7ecb0a7fbb6d0c1e";
    pub(crate) const TEST_PROOF: &[u8] = b"eg-statechart-test-scope-grant";

    pub(crate) struct TestScopeVerifier;

    impl ScopeGrantVerifier for TestScopeVerifier {
        fn verify(
            &self,
            _physical: &PhysicalStoreIdentity,
            layout: OwnerLayout,
            identity: &MutationScopeIdentity,
            principal: &str,
            proof: &[u8],
        ) -> std::result::Result<(), String> {
            if layout != OwnerLayout::Statechart
                || identity.tenant().as_str() != INSTANCE_MUTATION_TENANT
                || principal != TEST_PRINCIPAL
                || proof != TEST_PROOF
            {
                return Err("test scope authority rejected".to_string());
            }
            Ok(())
        }
    }

    /// Open the store the way the composition root would.
    pub(crate) fn open_test_store(dir: &Path) -> super::Result<StatechartStore> {
        StatechartStore::open_in_dir(dir, &TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::open_test_store;
    use super::*;
    use crate::model::{State, Transition};

    fn turnstile() -> StatechartDef {
        StatechartDef {
            name: "turnstile".into(),
            schema_version: 1,
            states: vec![State::new("locked"), State::new("unlocked")],
            alphabet: vec!["coin".into(), "push".into()],
            transitions: vec![
                Transition::new("locked", "coin", "unlocked"),
                Transition::new("unlocked", "push", "locked"),
            ],
            initial: "locked".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    fn store() -> (StatechartStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path()).unwrap();
        (store, dir)
    }

    #[test]
    fn define_is_content_addressed_and_idempotent() {
        let (store, _dir) = store();
        let id1 = store.define(&turnstile()).unwrap();
        let id2 = store.define(&turnstile()).unwrap();
        assert_eq!(id1, id2);
        assert_eq!(store.list_def_ids().unwrap().len(), 1);
    }

    #[test]
    fn invalid_definition_is_rejected_before_storage() {
        let (store, _dir) = store();
        let mut bad = turnstile();
        bad.initial = "ghost".into();
        assert!(matches!(
            store.define(&bad),
            Err(StatechartError::InvalidDefinition(_))
        ));
        assert!(store.list_def_ids().unwrap().is_empty());
    }

    #[test]
    fn instance_persists_and_rehydrates_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let def_id;
        let instance_id;
        {
            let store = open_test_store(dir.path()).unwrap();
            def_id = store.define(&turnstile()).unwrap();
            let instance = store
                .instantiate(&def_id, Context::new(), "tenant-x", "actor-y")
                .unwrap();
            instance_id = instance.instance_id.clone();
            // drive it forward once
            let out = store
                .send_event(&instance_id, &EventInput::new("coin"), None, now_ms())
                .unwrap();
            assert!(out.outcome.fired);
            assert!(out.instance.in_state("unlocked"));
            assert_eq!(out.instance.version, 1);
        }
        // Reopen a brand-new store handle on the same dir: the waiting machine is just
        // (state, context) on disk — rehydrate and continue.
        let store = open_test_store(dir.path()).unwrap();
        let rehydrated = store.get_instance(&instance_id).unwrap();
        assert!(rehydrated.in_state("unlocked"));
        assert_eq!(rehydrated.version, 1);
        let out = store
            .send_event(&instance_id, &EventInput::new("push"), None, now_ms())
            .unwrap();
        assert!(out.instance.in_state("locked"));
        assert_eq!(out.instance.version, 2);
    }

    #[test]
    fn noop_event_writes_nothing_and_leaves_version() {
        let (store, _dir) = store();
        let def_id = store.define(&turnstile()).unwrap();
        let instance = store
            .instantiate(&def_id, Context::new(), "t", "a")
            .unwrap();
        // 'push' from 'locked' is undefined ⇒ no-op.
        let out = store
            .send_event(
                &instance.instance_id,
                &EventInput::new("push"),
                None,
                now_ms(),
            )
            .unwrap();
        assert!(!out.outcome.fired);
        assert_eq!(out.instance.version, 0);
        assert!(out.instance.in_state("locked"));
    }

    #[test]
    fn occ_expected_version_mismatch_is_rejected() {
        let (store, _dir) = store();
        let def_id = store.define(&turnstile()).unwrap();
        let instance = store
            .instantiate(&def_id, Context::new(), "t", "a")
            .unwrap();
        // stored version is 0; claim to be at 5.
        let err = store
            .send_event(
                &instance.instance_id,
                &EventInput::new("coin"),
                Some(5),
                now_ms(),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StatechartError::VersionConflict {
                expected: 5,
                actual: 0,
                ..
            }
        ));
        // and the instance did not advance
        assert_eq!(
            store.get_instance(&instance.instance_id).unwrap().version,
            0
        );
    }

    #[test]
    fn event_outside_alphabet_is_an_error_not_a_noop() {
        let (store, _dir) = store();
        let def_id = store.define(&turnstile()).unwrap();
        let instance = store
            .instantiate(&def_id, Context::new(), "t", "a")
            .unwrap();
        let err = store
            .send_event(
                &instance.instance_id,
                &EventInput::new("teleport"),
                None,
                now_ms(),
            )
            .unwrap_err();
        assert!(matches!(err, StatechartError::InvalidTransition { .. }));
    }

    #[test]
    fn transitions_commit_through_the_mutation_gateway() {
        let (store, _dir) = store();
        let def_id = store.define(&turnstile()).unwrap();

        // instantiate itself is an authoritative instance write: it must land one
        // durable batch through the gateway, advancing the mutation-domain version.
        let instance = store
            .instantiate(&def_id, Context::new(), "t", "a")
            .unwrap();
        let v_after_instantiate = store.mutation_version().unwrap();
        assert_eq!(
            v_after_instantiate, 2,
            "define commits one maintenance batch and instantiate one operation batch"
        );

        // A FIRING transition advances the gateway version by exactly one and leaves a
        // committed batch keyed by the resulting instance image, carrying the
        // transition result payload and the post-commit outbox intent — the eg-jobs
        // precedent, mirrored.
        let out = store
            .send_event(
                &instance.instance_id,
                &EventInput::new("coin"),
                None,
                now_ms(),
            )
            .unwrap();
        assert!(out.outcome.fired);
        assert!(out.instance.in_state("unlocked"));
        assert_eq!(out.instance.version, 1);
        let v_after_fire = store.mutation_version().unwrap();
        assert_eq!(
            v_after_fire, 3,
            "a firing transition commits exactly one gateway batch"
        );

        // The committed batch is content-addressed by the exact persisted image and is
        // terminally Committed; its result payload rehydrates to that same image.
        let blob = encode_instance(&out.instance).unwrap();
        // `batch` here is only used for its (content-addressed) `batch_id` to look
        // the already-committed record back up: `batch_id` is a pure hash of
        // `blob`, independent of `version_expectation`, so any live version value
        // reconstructs the identical id -- `v_after_fire` is simply a genuine one
        // already in scope.
        let batch = instance_batch(&out.instance, &blob, &store.owner, v_after_fire).unwrap();
        let read = store.scoped_read().unwrap();
        let record = eg_transaction::read_ledger(&read, &batch.batch_id)
            .unwrap()
            .expect("a firing transition must leave a committed batch record");
        assert_eq!(record.status, eg_types::MutationBatchStatus::Committed);
        let committed_image: MachineInstance =
            decode_stored(record.result_msgpack.as_ref().unwrap()).unwrap();
        assert_eq!(committed_image, out.instance);
        let outbox = eg_transaction::read_outbox(&read, &batch.batch_id).unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].intent.topic,
            "engine.statechart-instance.transitioned"
        );

        // A well-defined NO-OP must NOT open a batch: the gateway version is unchanged.
        // 'coin' from 'unlocked' is undefined ⇒ no-op.
        let noop = store
            .send_event(
                &instance.instance_id,
                &EventInput::new("coin"),
                None,
                now_ms(),
            )
            .unwrap();
        assert!(!noop.outcome.fired);
        assert_eq!(
            store.mutation_version().unwrap(),
            v_after_fire,
            "a no-op event must commit no gateway batch"
        );

        // And an OCC conflict likewise commits nothing through the gateway.
        let conflict = store
            .send_event(
                &instance.instance_id,
                &EventInput::new("push"),
                Some(99),
                now_ms(),
            )
            .unwrap_err();
        assert!(matches!(conflict, StatechartError::VersionConflict { .. }));
        assert_eq!(
            store.mutation_version().unwrap(),
            v_after_fire,
            "a rejected OCC transition must commit no gateway batch"
        );
    }

    #[test]
    fn mismatched_replay_aborts_before_nonce_consumption() {
        let (store, _dir) = store();
        let def_id = store.define(&turnstile()).unwrap();
        let instance = store
            .instantiate(&def_id, Context::new(), "t", "a")
            .unwrap();
        let out = store
            .send_event(&instance.instance_id, &EventInput::new("coin"), None, 2_000)
            .unwrap();
        let blob = encode_instance(&out.instance).unwrap();
        let mut mismatched = out.instance.clone();
        mismatched.version += 1;
        let mismatched_blob = encode_instance(&mismatched).unwrap();
        let batch = instance_batch(
            &out.instance,
            &blob,
            &store.owner,
            store.mutation_version().unwrap(),
        )
        .unwrap();

        let error = store
            .commit_instance_blob(&mismatched, &mismatched_blob, &batch, 2_001)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("replayed statechart instance differs"),
            "{error}"
        );

        // The failed validation aborted the admitted replay write, so its
        // fresh attempt nonce remains reusable for the exact durable image.
        let (replayed, was_replay) = store
            .commit_instance_blob(&out.instance, &blob, &batch, 2_002)
            .unwrap();
        assert!(was_replay);
        assert_eq!(replayed, out.instance);
    }

    #[test]
    fn ownership_listing_filters_by_tenant_actor_and_def() {
        let (store, _dir) = store();
        let def_id = store.define(&turnstile()).unwrap();
        store
            .instantiate(&def_id, Context::new(), "t1", "a1")
            .unwrap();
        store
            .instantiate(&def_id, Context::new(), "t1", "a1")
            .unwrap();
        store
            .instantiate(&def_id, Context::new(), "t2", "a2")
            .unwrap();
        assert_eq!(
            store.list_owned_instances("t1", "a1", None).unwrap().len(),
            2
        );
        assert_eq!(
            store.list_owned_instances("t2", "a2", None).unwrap().len(),
            1
        );
        assert_eq!(
            store
                .list_owned_instances("t1", "a1", Some(&def_id))
                .unwrap()
                .len(),
            2
        );
        assert!(store
            .list_owned_instances("t1", "a1", Some("eg:statechart:nope"))
            .unwrap()
            .is_empty());
    }

    /// A composite chart round-trips through the durable store: instantiate descends into
    /// the initial child, a parent-level edge applies to the active descendant, and the
    /// whole configuration rehydrates across a store reopen.
    fn nested() -> StatechartDef {
        let mut active = State::new("active");
        active.children = vec!["idle".into(), "running".into()];
        active.initial_child = Some("idle".into());
        StatechartDef {
            name: "nested".into(),
            schema_version: 1,
            states: vec![
                active,
                State::new("idle"),
                State::new("running"),
                State::new("off"),
            ],
            alphabet: vec!["start".into(), "kill".into()],
            transitions: vec![
                Transition::new("idle", "start", "running"),
                Transition::new("active", "kill", "off"),
            ],
            initial: "active".into(),
            finals: vec![],
            meta: Default::default(),
        }
    }

    #[test]
    fn hierarchical_instance_persists_and_rehydrates_its_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let instance_id;
        {
            let store = open_test_store(dir.path()).unwrap();
            let def_id = store.define(&nested()).unwrap();
            let inst = store
                .instantiate(&def_id, Context::new(), "t", "a")
                .unwrap();
            instance_id = inst.instance_id.clone();
            // Initial configuration descended into the default child.
            assert!(inst.in_state("active") && inst.in_state("idle"));
            let out = store
                .send_event(&instance_id, &EventInput::new("start"), None, now_ms())
                .unwrap();
            assert!(out.instance.in_state("active") && out.instance.in_state("running"));
        }
        // Rehydrate: the parent-level `kill` edge applies to the active `running` state.
        let store = open_test_store(dir.path()).unwrap();
        let rehydrated = store.get_instance(&instance_id).unwrap();
        assert!(rehydrated.in_state("running"));
        let out = store
            .send_event(&instance_id, &EventInput::new("kill"), None, now_ms())
            .unwrap();
        assert!(out.instance.in_state("off"));
        assert!(!out.instance.in_state("active"));
    }
}
