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
//! What this store deliberately does NOT do in phase-1: it does not route through the
//! consensus `MutationBatch` gateway (`eg-mutation-store`) the way `eg-jobs` does, so
//! transitions are not yet Raft-ordered across a cluster. The OCC `version` gives
//! single-node correctness today; cluster replication is a documented phase-2 layer
//! (it slots in exactly where `eg-jobs`' `mutate_job_batch` sits, without changing the
//! record shape). See the crate `README`/lib docs.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;

use crate::action::apply_all;
use crate::check::validate;
use crate::context::{Context, EventInput};
use crate::instance::{InstanceId, InstanceStatus, MachineInstance};
use crate::model::{DefId, StatechartDef};
use crate::transition::{transition, TransitionError, TransitionOutcome};

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
    InvalidTransition { instance_id: String, reason: String },
    /// OCC conflict: the caller's `expected_version` did not match the stored version.
    VersionConflict { instance_id: String, expected: u64, actual: u64 },
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
    if instance.instance_id.len() > MAX_ID_BYTES
        || !valid_identifier(&instance.def_id)
        || !valid_identifier(&instance.state)
    {
        return Err(codec_err("statechart instance record exceeds storage limits"));
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
    /// The pure transition result (its `fired` flag distinguishes a real transition
    /// from a no-op; its `actions` are the effects for the interpreter to run).
    pub outcome: TransitionOutcome,
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
            table.insert(def_id.as_str(), blob.as_slice()).map_err(redb_err)?;
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
                return Err(codec_err("statechart definition list exceeds response limits"));
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
        let initial_state = def.state(&def.initial).ok_or_else(|| {
            // A stored, validated def always has a real initial state; this is defensive.
            StatechartError::InvalidTransition {
                instance_id: "<new>".to_string(),
                reason: "definition has no initial state".to_string(),
            }
        })?;

        // Entering s₀ fires its entry actions (Moore). Use a null-payload event.
        let entry_event = EventInput::new("__init__");
        let context = apply_all(initial_context, &initial_state.entry, &entry_event);

        let now = now_ms();
        let status = if def.is_final(&def.initial) {
            InstanceStatus::Final
        } else {
            InstanceStatus::Active
        };
        let instance = MachineInstance {
            instance_id: self.next_instance_id(),
            def_id: def_id.to_string(),
            state: def.initial.clone(),
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

    fn put_instance(&self, instance: &MachineInstance) -> Result<()> {
        let blob = encode_instance(instance)?;
        let wtx = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = wtx.open_table(INSTANCES).map_err(redb_err)?;
            table
                .insert(instance.instance_id.as_str(), blob.as_slice())
                .map_err(redb_err)?;
        }
        wtx.commit().map_err(redb_err)?;
        Ok(())
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
        let mut instance = self.get_instance(instance_id)?;
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
        let outcome = transition(&def, &instance.state, &instance.context, event).map_err(
            |error| map_transition_error(instance_id, error),
        )?;

        if !outcome.fired {
            // Well-defined no-op: stay put, persist nothing.
            return Ok(SendOutcome { instance, outcome });
        }

        // A firing transition: commit the new (state, context) under a fresh version.
        // Re-open the row inside a write txn and re-check the version so a concurrent
        // writer cannot be silently clobbered (compare-and-set). The block below owns
        // every borrow of the write txn (the redb `AccessGuard`/`Table`) and returns a
        // plain `Result<(), u64>` — `Ok(())` staged the write, `Err(actual)` found an
        // OCC conflict — so the txn can be committed or aborted with no borrow alive.
        let wtx = self.db.begin_write().map_err(redb_err)?;
        let staged: std::result::Result<std::result::Result<(), u64>, StatechartError> = (|| {
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
                return Ok(Err(current.version));
            }
            let now = now_ms();
            instance.state = outcome.next_state.clone();
            instance.context = outcome.next_context.clone();
            instance.version = instance.version.saturating_add(1);
            instance.transitions_fired = instance.transitions_fired.saturating_add(1);
            instance.events_seen = instance.events_seen.saturating_add(1);
            instance.status = if def.is_final(&instance.state) {
                InstanceStatus::Final
            } else {
                InstanceStatus::Active
            };
            instance.updated_at_ms = now;
            let blob = encode_instance(&instance)?;
            table.insert(instance_id, blob.as_slice()).map_err(redb_err)?;
            Ok(Ok(()))
        })();
        match staged {
            Ok(Ok(())) => {
                wtx.commit().map_err(redb_err)?;
                Ok(SendOutcome { instance, outcome })
            }
            Ok(Err(actual)) => {
                wtx.abort().map_err(redb_err)?;
                Err(StatechartError::VersionConflict {
                    instance_id: instance_id.to_string(),
                    expected: instance.version,
                    actual,
                })
            }
            Err(error) => {
                let _ = wtx.abort();
                Err(error)
            }
        }
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
                return Err(codec_err("statechart instance list exceeds response limits"));
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
                return Err(codec_err("statechart instance list exceeds response limits"));
            }
            let instance: MachineInstance = decode_stored(v.value())?;
            let owned = instance.tenant == tenant && instance.actor == actor;
            let matches_def = def_id.map_or(true, |want| instance.def_id == want);
            if owned && matches_def {
                out.push(instance);
            }
        }
        Ok(out)
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
            assert_eq!(out.instance.state, "unlocked");
            assert_eq!(out.instance.version, 1);
        }
        // Reopen a brand-new store handle on the same dir: the waiting machine is just
        // (state, context) on disk — rehydrate and continue.
        let store = StatechartStore::open_in_dir(dir.path()).unwrap();
        let rehydrated = store.get_instance(&instance_id).unwrap();
        assert_eq!(rehydrated.state, "unlocked");
        assert_eq!(rehydrated.version, 1);
        let out = store
            .send_event(&instance_id, &EventInput::new("push"), None)
            .unwrap();
        assert_eq!(out.instance.state, "locked");
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
        assert_eq!(out.instance.state, "locked");
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
        assert!(matches!(err, StatechartError::VersionConflict { expected: 5, actual: 0, .. }));
        // and the instance did not advance
        assert_eq!(store.get_instance(&instance.instance_id).unwrap().version, 0);
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
    fn ownership_listing_filters_by_tenant_actor_and_def() {
        let (store, _dir) = store();
        let def_id = store.define(&turnstile()).unwrap();
        store.instantiate(&def_id, Context::new(), "t1", "a1").unwrap();
        store.instantiate(&def_id, Context::new(), "t1", "a1").unwrap();
        store.instantiate(&def_id, Context::new(), "t2", "a2").unwrap();
        assert_eq!(store.list_owned_instances("t1", "a1", None).unwrap().len(), 2);
        assert_eq!(store.list_owned_instances("t2", "a2", None).unwrap().len(), 1);
        assert_eq!(
            store.list_owned_instances("t1", "a1", Some(&def_id)).unwrap().len(),
            2
        );
        assert!(store
            .list_owned_instances("t1", "a1", Some("eg:statechart:nope"))
            .unwrap()
            .is_empty());
    }
}
