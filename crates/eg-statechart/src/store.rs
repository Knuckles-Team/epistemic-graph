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
//! Cluster ordering: every authoritative instance write (both `instantiate` and a
//! FIRING `send_event`) routes through the universal `MutationBatch` /
//! durable-commit gateway (`eg-mutation-store`) — the SAME `begin` → change-rows →
//! `finish` → `commit` sequence `eg-jobs`' `mutate_job_batch` uses, on the *same*
//! redb `WriteTransaction` that mutates the `STATECHART_INSTANCES` row. So each
//! transition carries an atomic terminal batch record, a monotonic domain version,
//! a route fence, an idempotency row and an outbox intent — exactly the evidence a
//! Raft state machine needs to order and replay it deterministically, with no
//! coordinator/native-row split-brain window. The per-instance OCC `version`
//! compare-and-set is preserved on top of that (it still guards lost updates on a
//! single node), and single-node behavior is byte-for-byte identical: a batch is
//! content-addressed by the exact instance image it persists, so a replayed
//! proposal is an idempotent no-op. A well-defined NO-OP event still writes nothing
//! — it never opens a batch.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use eg_types::mutation_batch::{
    MutationBatch, MutationDomain, MutationOperation, MutationOutboxIntent, MutationRequestContext,
    MutationSurface, MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;

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
    let bytes = rmp_serde::to_vec_named(def).map_err(codec_err)?;
    if bytes.len() > MAX_STORED_BYTES {
        return Err(codec_err("statechart definition exceeds storage limits"));
    }
    Ok(bytes)
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
    let bytes = rmp_serde::to_vec_named(instance).map_err(codec_err)?;
    if bytes.len() > MAX_STORED_BYTES {
        return Err(codec_err("statechart instance exceeds storage limits"));
    }
    Ok(bytes)
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
    db: Database,
    /// Monotonic instance-id source (mirrors `eg-jobs`' `next_id`): `"sc-<hex>"`.
    next_id: AtomicU64,
}

impl StatechartStore {
    /// Open (or create) the store at an exact file path, materializing the schema so an
    /// empty DB is queryable, and seeding the id counter from the highest existing id.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_err(redb_err)?;
        {
            let wtx = db.begin_write().map_err(redb_err)?;
            wtx.open_table(DEFS).map_err(redb_err)?;
            wtx.open_table(INSTANCES).map_err(redb_err)?;
            wtx.commit().map_err(redb_err)?;
        }
        // Materialize the shared batch/version/fence/idempotency/outbox tables in the
        // same redb file, exactly as `eg-jobs::JobStore::open` does for `jobs.redb`.
        eg_mutation_store::initialize(&db).map_err(redb_err)?;
        let seed = initialize_next_id(&db)?;
        Ok(Self {
            db,
            next_id: AtomicU64::new(seed),
        })
    }

    /// Open `{persist_dir}/statecharts.redb` — the durable location beside the graph
    /// shards, exactly as `eg-jobs` opens `jobs.redb`.
    pub fn open_in_dir(persist_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(persist_dir)
            .map_err(|e| StatechartError::Redb(format!("create persist dir: {e}")))?;
        Self::open(&persist_dir.join("statecharts.redb"))
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
        let wtx = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = wtx.open_table(DEFS).map_err(redb_err)?;
            table
                .insert(def_id.as_str(), blob.as_slice())
                .map_err(redb_err)?;
        }
        wtx.commit().map_err(redb_err)?;
        Ok(def_id)
    }

    /// Fetch a stored definition by id.
    pub fn get_def(&self, def_id: &str) -> Result<StatechartDef> {
        if !valid_identifier(def_id) {
            return Err(codec_err("statechart definition id is invalid"));
        }
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(DEFS).map_err(redb_err)?;
        let blob = table
            .get(def_id)
            .map_err(redb_err)?
            .ok_or_else(|| StatechartError::NotFound(def_id.to_string()))?;
        decode_stored(blob.value())
    }

    /// List every stored definition id.
    pub fn list_def_ids(&self) -> Result<Vec<DefId>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(DEFS).map_err(redb_err)?;
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
        self.put_instance(&instance)?;
        Ok(instance)
    }

    /// Persist a fresh instance image through the universal durable-commit gateway
    /// (the `instantiate` write path). This is the blind-insert analogue of
    /// `eg-jobs`' `put_raw_batch`: the row change and its terminal batch/version/
    /// fence/idempotency/outbox evidence commit as one all-or-nothing redb txn. The
    /// batch is content-addressed by the exact bytes being written, so a replayed
    /// proposal (same image) is an idempotent no-op.
    fn put_instance(&self, instance: &MachineInstance) -> Result<()> {
        let blob = encode_instance(instance)?;
        let batch = instance_batch(instance, &blob)?;
        let committed_at_ms = instance.updated_at_ms.max(0) as u64;
        let wtx = self.db.begin_write().map_err(redb_err)?;
        match eg_mutation_store::begin(&wtx, &batch).map_err(redb_err)? {
            eg_mutation_store::Begin::Replay(_record) => {
                // This exact instance image already committed durably; nothing to do.
                wtx.abort().map_err(redb_err)?;
                Ok(())
            }
            eg_mutation_store::Begin::Apply { source_version } => {
                {
                    let mut table = wtx.open_table(INSTANCES).map_err(redb_err)?;
                    table
                        .insert(instance.instance_id.as_str(), blob.as_slice())
                        .map_err(redb_err)?;
                }
                eg_mutation_store::finish(
                    &wtx,
                    &batch,
                    Some(blob),
                    committed_at_ms,
                    source_version,
                )
                .map_err(redb_err)?;
                eg_mutation_store::commit(wtx, &batch).map_err(redb_err)?;
                Ok(())
            }
        }
    }

    /// Fetch (rehydrate) an instance by id.
    pub fn get_instance(&self, instance_id: &str) -> Result<MachineInstance> {
        if instance_id.is_empty() || instance_id.len() > MAX_ID_BYTES {
            return Err(codec_err("statechart instance id is invalid"));
        }
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(INSTANCES).map_err(redb_err)?;
        let blob = table
            .get(instance_id)
            .map_err(redb_err)?
            .ok_or_else(|| StatechartError::NotFound(instance_id.to_string()))?;
        decode_stored(blob.value())
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
    pub fn send_event(
        &self,
        instance_id: &str,
        event: &EventInput,
        expected_version: Option<u64>,
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
        let now = now_ms();
        let mut next = instance.clone();
        next.configuration = outcome.next.clone();
        next.context = outcome.next_context.clone();
        next.version = next.version.saturating_add(1);
        next.transitions_fired = next.transitions_fired.saturating_add(1);
        next.events_seen = next.events_seen.saturating_add(1);
        next.status = if next.configuration.is_final(&def) {
            InstanceStatus::Final
        } else {
            InstanceStatus::Active
        };
        next.updated_at_ms = now;
        let blob = encode_instance(&next)?;
        let batch = instance_batch(&next, &blob)?;
        let committed_at_ms = now.max(0) as u64;

        // Commit the row change and its terminal batch/version/fence/idempotency/
        // outbox evidence through the SAME `eg-mutation-store` gateway `eg-jobs` uses,
        // on one redb `WriteTransaction`. Inside that txn the row is re-read and its
        // OCC `version` re-checked (compare-and-set) so a concurrent writer cannot be
        // silently clobbered — the per-instance guard is preserved on top of the
        // gateway.
        let wtx = self.db.begin_write().map_err(redb_err)?;
        match eg_mutation_store::begin(&wtx, &batch).map_err(redb_err)? {
            eg_mutation_store::Begin::Replay(_record) => {
                // This exact resulting image already committed durably (idempotent
                // replay of a re-proposed transition); the instance is already at
                // `next`. Report it without writing again.
                wtx.abort().map_err(redb_err)?;
                Ok(SendOutcome {
                    instance: next,
                    outcome,
                })
            }
            eg_mutation_store::Begin::Apply { source_version } => {
                // Re-open the row inside the write txn and re-check the OCC version.
                // The block owns every borrow of the `INSTANCES` table and yields a
                // plain `Result<(), u64>` — `Ok(())` staged the write, `Err(actual)`
                // found a compare-and-set conflict — so the table borrow is dropped
                // before `finish`/`commit`/`abort` touch the gateway's own tables.
                let occ: std::result::Result<(), u64> = {
                    let mut table = wtx.open_table(INSTANCES).map_err(redb_err)?;
                    let current_bytes = {
                        let guard = table
                            .get(instance_id)
                            .map_err(redb_err)?
                            .ok_or_else(|| StatechartError::NotFound(instance_id.to_string()))?;
                        guard.value().to_vec()
                    };
                    let current: MachineInstance = decode_stored(&current_bytes)?;
                    if current.version != instance.version {
                        // Someone advanced the instance between our read and our write.
                        Err(current.version)
                    } else {
                        table
                            .insert(instance_id, blob.as_slice())
                            .map_err(redb_err)?;
                        Ok(())
                    }
                };
                match occ {
                    Ok(()) => {
                        eg_mutation_store::finish(
                            &wtx,
                            &batch,
                            Some(blob),
                            committed_at_ms,
                            source_version,
                        )
                        .map_err(redb_err)?;
                        eg_mutation_store::commit(wtx, &batch).map_err(redb_err)?;
                        Ok(SendOutcome {
                            instance: next,
                            outcome,
                        })
                    }
                    Err(actual) => {
                        wtx.abort().map_err(redb_err)?;
                        Err(StatechartError::VersionConflict {
                            instance_id: instance_id.to_string(),
                            expected: instance.version,
                            actual,
                        })
                    }
                }
            }
        }
    }

    /// The gateway's monotonic mutation-domain version for the statechart instance
    /// scope — the count of durable instance batches committed through
    /// `eg-mutation-store` (mirrors `eg-jobs`' `mutation_version`). A cluster layer
    /// reads this to order/replay instance transitions.
    pub fn mutation_version(&self) -> Result<u64> {
        eg_mutation_store::version(&self.db, INSTANCE_MUTATION_TENANT, INSTANCE_MUTATION_GRAPH)
            .map_err(redb_err)
    }

    /// List instance ids, optionally filtered to one definition. Diagnostic/admin use;
    /// ownership filtering is the caller's responsibility (see the dispatch handler).
    pub fn list_instance_ids(&self, def_id: Option<&str>) -> Result<Vec<InstanceId>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(INSTANCES).map_err(redb_err)?;
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
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(INSTANCES).map_err(redb_err)?;
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

/// Fixed mutation-domain scope for statechart instance writes. A single
/// `(tenant, graph)` scope gives every instance transition one totally-ordered
/// mutation log — exactly what a Raft state machine orders and replays — mirroring
/// `eg-jobs`' fixed `native`/`analytics-jobs` scope.
const INSTANCE_MUTATION_TENANT: &str = "native";
const INSTANCE_MUTATION_GRAPH: &str = "statechart-instances";

/// Give each durable instance image the same deterministic, digest-only
/// `MutationBatch` `eg-jobs`' `internal_job_batch` stamps on every `JOBS` transition,
/// so an `instantiate` or a firing `send_event` carries identical atomic
/// status/version/fence/idempotency/outbox evidence and is Raft-orderable and
/// crash-consistent across nodes. The batch identity is a pure hash of the resulting
/// instance image (`encoded`), so a re-proposed transition replays idempotently
/// rather than double-applying.
fn instance_batch(instance: &MachineInstance, encoded: &[u8]) -> Result<MutationBatch> {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(encoded));
    // Attribute the transition to the authenticated, server-hashed owner of the
    // instance. The actor is already an opaque scope; never emit a raw label.
    let principal = format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(instance.actor.as_bytes()))
    );
    let batch_id = format!("statechart-transition:{digest}");
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: MutationDomain::Lifecycle,
        method: Method::ApplyMutation {
            event_type: "statechart_instance_transition".to_string(),
            query: format!("sha256:{digest}"),
        },
    };
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: 0,
            principal,
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
        },
        tenant: INSTANCE_MUTATION_TENANT.to_string(),
        graph: INSTANCE_MUTATION_GRAPH.to_string(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        expected_graph_version: None,
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation.clone()],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.statechart-instance.transitioned".to_string(),
            key: batch_id,
            payload: rmp_serde::to_vec_named(&operation).map_err(codec_err)?,
            headers: std::collections::BTreeMap::new(),
        }],
        created_at_ms: instance.updated_at_ms.max(0) as u64,
    };
    batch.validate().map_err(codec_err)?;
    Ok(batch)
}

fn map_transition_error(instance_id: &str, error: TransitionError) -> StatechartError {
    StatechartError::InvalidTransition {
        instance_id: instance_id.to_string(),
        reason: error.to_string(),
    }
}

/// Seed the monotonic id counter from the highest `sc-<hex>` id already present, so ids
/// never collide across restarts (mirrors `eg-jobs`' `max_job_sequence` backfill).
fn initialize_next_id(db: &Database) -> Result<u64> {
    let rtx = db.begin_read().map_err(redb_err)?;
    let table = rtx.open_table(INSTANCES).map_err(redb_err)?;
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

#[cfg(test)]
mod tests {
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
        let store = StatechartStore::open_in_dir(dir.path()).unwrap();
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
            let store = StatechartStore::open_in_dir(dir.path()).unwrap();
            def_id = store.define(&turnstile()).unwrap();
            let instance = store
                .instantiate(&def_id, Context::new(), "tenant-x", "actor-y")
                .unwrap();
            instance_id = instance.instance_id.clone();
            // drive it forward once
            let out = store
                .send_event(&instance_id, &EventInput::new("coin"), None)
                .unwrap();
            assert!(out.outcome.fired);
            assert!(out.instance.in_state("unlocked"));
            assert_eq!(out.instance.version, 1);
        }
        // Reopen a brand-new store handle on the same dir: the waiting machine is just
        // (state, context) on disk — rehydrate and continue.
        let store = StatechartStore::open_in_dir(dir.path()).unwrap();
        let rehydrated = store.get_instance(&instance_id).unwrap();
        assert!(rehydrated.in_state("unlocked"));
        assert_eq!(rehydrated.version, 1);
        let out = store
            .send_event(&instance_id, &EventInput::new("push"), None)
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
            .send_event(&instance.instance_id, &EventInput::new("push"), None)
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
            .send_event(&instance.instance_id, &EventInput::new("coin"), Some(5))
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
            .send_event(&instance.instance_id, &EventInput::new("teleport"), None)
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
            v_after_instantiate, 1,
            "instantiate must commit exactly one gateway batch"
        );

        // A FIRING transition advances the gateway version by exactly one and leaves a
        // committed batch keyed by the resulting instance image, carrying the
        // transition result payload and the post-commit outbox intent — the eg-jobs
        // precedent, mirrored.
        let out = store
            .send_event(&instance.instance_id, &EventInput::new("coin"), None)
            .unwrap();
        assert!(out.outcome.fired);
        assert!(out.instance.in_state("unlocked"));
        assert_eq!(out.instance.version, 1);
        let v_after_fire = store.mutation_version().unwrap();
        assert_eq!(
            v_after_fire, 2,
            "a firing transition commits exactly one gateway batch"
        );

        // The committed batch is content-addressed by the exact persisted image and is
        // terminally Committed; its result payload rehydrates to that same image.
        let blob = encode_instance(&out.instance).unwrap();
        let batch = instance_batch(&out.instance, &blob).unwrap();
        let record = eg_mutation_store::read_record(&store.db, &batch.batch_id)
            .unwrap()
            .expect("a firing transition must leave a committed batch record");
        assert_eq!(record.status, eg_types::MutationBatchStatus::Committed);
        let committed_image: MachineInstance =
            decode_stored(record.result_msgpack.as_ref().unwrap()).unwrap();
        assert_eq!(committed_image, out.instance);
        let outbox = eg_mutation_store::read_outbox(&store.db, &batch.batch_id).unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].intent.topic,
            "engine.statechart-instance.transitioned"
        );

        // A well-defined NO-OP must NOT open a batch: the gateway version is unchanged.
        // 'coin' from 'unlocked' is undefined ⇒ no-op.
        let noop = store
            .send_event(&instance.instance_id, &EventInput::new("coin"), None)
            .unwrap();
        assert!(!noop.outcome.fired);
        assert_eq!(
            store.mutation_version().unwrap(),
            v_after_fire,
            "a no-op event must commit no gateway batch"
        );

        // And an OCC conflict likewise commits nothing through the gateway.
        let conflict = store
            .send_event(&instance.instance_id, &EventInput::new("push"), Some(99))
            .unwrap_err();
        assert!(matches!(conflict, StatechartError::VersionConflict { .. }));
        assert_eq!(
            store.mutation_version().unwrap(),
            v_after_fire,
            "a rejected OCC transition must commit no gateway batch"
        );
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
            let store = StatechartStore::open_in_dir(dir.path()).unwrap();
            let def_id = store.define(&nested()).unwrap();
            let inst = store
                .instantiate(&def_id, Context::new(), "t", "a")
                .unwrap();
            instance_id = inst.instance_id.clone();
            // Initial configuration descended into the default child.
            assert!(inst.in_state("active") && inst.in_state("idle"));
            let out = store
                .send_event(&instance_id, &EventInput::new("start"), None)
                .unwrap();
            assert!(out.instance.in_state("active") && out.instance.in_state("running"));
        }
        // Rehydrate: the parent-level `kill` edge applies to the active `running` state.
        let store = StatechartStore::open_in_dir(dir.path()).unwrap();
        let rehydrated = store.get_instance(&instance_id).unwrap();
        assert!(rehydrated.in_state("running"));
        let out = store
            .send_event(&instance_id, &EventInput::new("kill"), None)
            .unwrap();
        assert!(out.instance.in_state("off"));
        assert!(!out.instance.in_state("active"));
    }
}
