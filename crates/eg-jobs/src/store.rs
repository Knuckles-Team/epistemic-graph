//! Durable `AnalyticsJob` state-machine projection over redb (CONCEPT:INT-P2-1).
//! In a cluster, every transition is first ordered by the owning Raft group and then
//! applied deterministically to each replica's `jobs.redb`; the consensus log is the
//! scheduler authority and `jobs.redb` is its restartable local projection. A
//! single-node deployment uses the same state machine with `jobs.redb` directly.
//!
//! [`JobStore`] owns the authoritative tables below plus transactionally maintained
//! scheduler secondary indexes (priority-ready, worker lease, expiry, deadline,
//! cancellation, opaque tenant totals and monotonic sequence metadata):
//!   * `JOBS`             — `job_id -> msgpack(AnalyticsJob)`, the durable record.
//!   * `COMMITTED_RESULTS` — `result_ref -> job_id` (the FIRST job that committed
//!     it), a store-level idempotency ledger independent of any live graph — see
//!     [`JobStore::mark_result_committed`]. The `claim.rs` writeback additionally
//!     guards on `GraphCore::has_node` for the claim id itself, so idempotency holds
//!     even if a caller wires a DIFFERENT graph/claim path on top of this store.
//!   * `JOB_INTENTS` — `name -> msgpack(`[`crate::intent::JobIntent`]`)`, the
//!     declarative trigger registry (CONCEPT:INT-P2-1, daemon-consolidation design
//!     Phase 3): a job DECLARED with a schedule, evaluated by
//!     [`JobStore::due_intents`] / [`JobStore::record_intent_tick`] rather than
//!     driven by an external caller deciding due-ness itself.
//!   * `IDEMPOTENCY_LEDGER` — `key -> owner` (first-wins), the GENERALIZED sibling of
//!     `COMMITTED_RESULTS`: `COMMITTED_RESULTS` dedupes one specific thing (a
//!     completed analytics RESULT, keyed by `result_ref`); `IDEMPOTENCY_LEDGER`
//!     dedupes ANY caller-supplied key (e.g. a `JobIntent` tick window), so a job
//!     kind beyond analytics-compute gets first-wins single-flight for free — see
//!     [`JobStore::claim_idempotency`].
//!
//! Every transition is a guarded state-machine edge (CONCEPT:INT-P2-1): an invalid
//! edge (e.g. checkpointing a `Cancelled` job) is a hard `Err`, never a silent
//! overwrite — a durable job record's history should never appear to un-happen.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use eg_storage::{
    JobsOwner, OwnedStoreHandle, PhysicalStoreIdentity, ScopeGrantVerifier, ScopedRead,
    StorageKernel,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite, Begin, MutationKernel};
use eg_types::mutation_batch::{
    DurabilityDomain, MutationBatch, MutationBatchRecord, MutationOperation, MutationOutboxIntent,
    MutationRequestContext, MutationScope, MutationScopeIdentity, MutationSurface,
    VersionExpectation, MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;
use redb::{ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;

use crate::intent::JobIntent;
use crate::model::{AnalyticsJob, Checkpoint, JobId, JobPolicy, JobState, WorkerLease};
use crate::result::TypedJobResult;

const JOBS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("analytics_jobs");
const COMMITTED_RESULTS: TableDefinition<'static, &str, &str> =
    TableDefinition::new("analytics_job_committed_results");
const JOB_INTENTS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("job_intents");
const IDEMPOTENCY_LEDGER: TableDefinition<'static, &str, &str> =
    TableDefinition::new("job_idempotency_ledger");
const RESULTS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("analytics_job_knowledge_batches");
// Durable scheduler indexes. Every entry is maintained in the SAME redb write
// transaction as its authoritative JOBS row; `open` performs a one-time backfill
// for databases created before this schema existed.
const JOB_META: TableDefinition<'static, &str, u64> =
    TableDefinition::new("analytics_job_scheduler_meta");
const JOB_READY: TableDefinition<'static, (u32, i64, &str), ()> =
    TableDefinition::new("analytics_job_ready_by_priority");
/// One bounded placement row per ready job.  The first component is either the
/// digest of one deterministic required placement token or the fixed
/// unconstrained sentinel; raw capability/pool/region labels never enter the
/// durable index.  A worker range-seeks only the anchors it can satisfy.
const JOB_READY_BY_CAPABILITY: TableDefinition<'static, (&str, u32, i64, &str), ()> =
    TableDefinition::new("analytics_job_ready_by_capability");
const JOB_LEASE_BY_WORKER: TableDefinition<'static, &str, &str> =
    TableDefinition::new("analytics_job_lease_by_worker");
const JOB_LEASE_EXPIRY: TableDefinition<'static, (i64, &str), ()> =
    TableDefinition::new("analytics_job_lease_by_expiry");
const JOB_TENANT_TOTALS: TableDefinition<'static, &str, (u64, u64)> =
    TableDefinition::new("analytics_job_active_totals_by_tenant");
const JOB_DEADLINE: TableDefinition<'static, (i64, &str), ()> =
    TableDefinition::new("analytics_job_by_deadline");
const JOB_CANCELLATION: TableDefinition<'static, &str, ()> =
    TableDefinition::new("analytics_job_cancellation_reconcile");

const SCHEDULER_INDEX_VERSION: u64 = 2;
const META_INDEX_VERSION: &str = "scheduler_index_version";
const META_MAX_JOB_SEQUENCE: &str = "max_job_sequence";
const UNCONSTRAINED_CAPABILITY_ANCHOR: &str = "0";

const MAX_STORED_JOB_BYTES: usize = 64 * 1024 * 1024;
const MAX_STORED_JOB_ITEMS: usize = 1_000_000;
const MAX_JOB_ID_BYTES: usize = 256;
const MAX_JOB_STRING_BYTES: usize = 4 * 1024;
const MAX_JOB_REASON_BYTES: usize = 64 * 1024;
const MAX_JOB_OPAQUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_JOB_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAX_JOB_LIST_ITEMS: usize = 100_000;
const MAX_JOB_LIST_BYTES: usize = 64 * 1024 * 1024;
const MAX_JOB_REBUILD_BYTES: usize = 512 * 1024 * 1024;
const MAX_JOB_REBUILD_ITEMS: usize = 1_000_000;
const MAX_SCHEDULER_RECONCILE_ITEMS: usize = 100_000;

/// Per-tenant admission limit applied when a worker claims work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TenantJobQuota {
    pub max_active: usize,
    pub max_reserved_cpu_ms: u64,
}

impl Default for TenantJobQuota {
    fn default() -> Self {
        Self {
            max_active: 4,
            max_reserved_cpu_ms: u64::MAX,
        }
    }
}

/// A fenced assignment returned by the distributed scheduler.
#[derive(Clone, Debug)]
pub struct WorkerClaim {
    pub job: AnalyticsJob,
    pub lease: WorkerLease,
}

/// Job-store error type. Deliberately small and string-carrying (mirrors `eg-tsdb`'s
/// `TsError`) rather than a rich enum — callers surface these as plain protocol
/// error strings, and the state-machine guard messages ARE the diagnostic.
#[derive(Debug)]
pub enum JobError {
    Redb(String),
    Codec(String),
    NotFound(String),
    InvalidTransition {
        job_id: String,
        state: &'static str,
        reason: &'static str,
    },
}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobError::Redb(m) => write!(f, "jobs redb error: {m}"),
            JobError::Codec(m) => write!(f, "jobs codec error: {m}"),
            JobError::NotFound(id) => write!(f, "job not found: {id}"),
            JobError::InvalidTransition {
                job_id,
                state,
                reason,
            } => write!(
                f,
                "invalid transition on job {job_id} in state {state}: {reason}"
            ),
        }
    }
}

impl std::error::Error for JobError {}

type Result<T> = std::result::Result<T, JobError>;

fn redb_err<E: std::fmt::Display>(e: E) -> JobError {
    JobError::Redb(e.to_string())
}
fn codec_err<E: std::fmt::Display>(e: E) -> JobError {
    JobError::Codec(e.to_string())
}

fn decode_stored<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(MAX_STORED_JOB_BYTES, MAX_STORED_JOB_ITEMS, 64),
    )
    .map_err(|_| codec_err("stored analytics-job record is invalid"))
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_JOB_STRING_BYTES && !value.contains('\0')
}

fn validate_checkpoint(checkpoint: &Checkpoint) -> Result<()> {
    if !checkpoint.progress.is_finite()
        || !(0.0..=1.0).contains(&checkpoint.progress)
        || checkpoint.stage.len() > MAX_JOB_STRING_BYTES
        || checkpoint
            .state_blob
            .as_ref()
            .is_some_and(|blob| blob.len() > MAX_JOB_OPAQUE_BYTES)
    {
        return Err(codec_err("analytics-job checkpoint exceeds storage limits"));
    }
    Ok(())
}

/// Whether any scalar/opaque field on `job` exceeds its storage limit. Split
/// out of `validate_job` (extract-method, cx/wD8) — one flat guard, same terms
/// and same order as before.
/// Whether `job`'s id/input-snapshot/policy scalar fields exceed their
/// storage limit. Split out of `job_exceeds_storage_limits` (extract-method,
/// cx/wD8) — same terms, same order as before.
fn job_identity_fields_exceed_limits(job: &AnalyticsJob) -> bool {
    job.job_id.is_empty()
        || job.job_id.len() > MAX_JOB_ID_BYTES
        || !valid_identifier(&job.input_snapshot.graph)
        || job.input_snapshot.dataset_ref.len() > MAX_JOB_STRING_BYTES
        || job.input_snapshot.content_digest.len() > MAX_JOB_STRING_BYTES
        || !valid_identifier(&job.policy.tenant)
        || !valid_identifier(&job.policy.actor)
        || job.policy.purpose.len() > MAX_JOB_STRING_BYTES
        || job.policy.policy_fingerprint.len() > MAX_JOB_STRING_BYTES
}

/// Whether `job`'s algo/payload/worker-ref/lease fields exceed their storage
/// limit. Split out of `job_exceeds_storage_limits` (extract-method,
/// cx/wD8) — same terms, same order as before.
fn job_algo_and_runtime_fields_exceed_limits(job: &AnalyticsJob) -> bool {
    !valid_identifier(&job.algo.family)
        || !valid_identifier(&job.algo.algorithm)
        || job.algo.params_digest.len() > MAX_JOB_STRING_BYTES
        || job.algo.code_version.len() > MAX_JOB_STRING_BYTES
        || job.algo.env_version.len() > MAX_JOB_STRING_BYTES
        || job
            .input_payload
            .as_ref()
            .is_some_and(|payload| payload.len() > MAX_JOB_OPAQUE_BYTES)
        || job.last_worker_ref.len() > MAX_JOB_STRING_BYTES
        || job
            .lease
            .as_ref()
            .is_some_and(|lease| !valid_identifier(&lease.worker_ref))
}

fn job_exceeds_storage_limits(job: &AnalyticsJob) -> bool {
    job_identity_fields_exceed_limits(job) || job_algo_and_runtime_fields_exceed_limits(job)
}

fn validate_job(job: &AnalyticsJob) -> Result<()> {
    if job_exceeds_storage_limits(job) {
        return Err(codec_err("analytics-job record exceeds storage limits"));
    }
    validate_placement(&job.policy)?;
    if let Some(checkpoint) = job.state.checkpoint() {
        validate_checkpoint(checkpoint)?;
    }
    match &job.state {
        JobState::Publishing { result_ref, .. } | JobState::Succeeded { result_ref, .. }
            if !valid_identifier(result_ref) =>
        {
            return Err(codec_err("analytics-job result reference is invalid"));
        }
        JobState::Failed { reason, .. } if reason.len() > MAX_JOB_REASON_BYTES => {
            return Err(codec_err(
                "analytics-job failure reason exceeds storage limits",
            ));
        }
        _ => {}
    }
    if let Some(output) = &job.output {
        output.validate().map_err(codec_err)?;
    }
    Ok(())
}

fn decode_job(bytes: &[u8]) -> Result<AnalyticsJob> {
    let job = decode_stored(bytes)?;
    validate_job(&job)?;
    Ok(job)
}

/// Whether `job` should count as ready in `metric_counts`. Split out of that
/// function (extract-method, cx/wD8) — same terms, same order as before,
/// including the cancellation-reconcile carve-out (an expired,
/// cancellation-requested lease still needs one worker poll to drive the
/// durable record to its terminal Cancelled state).
fn job_is_ready_for_scheduling(job: &AnalyticsJob, live_lease: bool, now_ms: i64) -> bool {
    let cancellation_reconcile = job.cancel_requested
        && matches!(
            &job.state,
            JobState::Running { .. } | JobState::Publishing { .. }
        )
        && !live_lease;
    cancellation_reconcile
        || (!job.cancel_requested
            && job.not_before_ms <= now_ms
            && (matches!(&job.state, JobState::Submitted)
                || matches!(
                    &job.state,
                    JobState::Running { .. } | JobState::Publishing { .. }
                ) && !live_lease))
}

/// Whether `claim_next`'s arguments fail its opaque-reference/limit guard.
/// Split out of that function (extract-method, cx/wD8) — same terms, same
/// order as before.
fn claim_next_args_invalid(
    worker_ref: &str,
    worker_capabilities: &[String],
    lease_ms: u64,
    quota: &TenantJobQuota,
) -> bool {
    worker_ref.trim().is_empty()
        || worker_ref.len() > 256
        || lease_ms == 0
        || quota.max_active == 0
        || worker_capabilities.len() > 256
        || worker_capabilities
            .iter()
            .any(|value| value.is_empty() || value.len() > 256)
}

fn encode_job(job: &AnalyticsJob) -> Result<Vec<u8>> {
    validate_job(job)?;
    let bytes = rmp_serde::to_vec_named(job).map_err(codec_err)?;
    if bytes.len() > MAX_STORED_JOB_BYTES {
        return Err(codec_err("analytics-job record exceeds storage limits"));
    }
    Ok(bytes)
}

fn decode_result(bytes: &[u8]) -> Result<TypedJobResult> {
    let result: TypedJobResult = decode_stored(bytes)?;
    result.validate().map_err(codec_err)?;
    Ok(result)
}

fn validate_intent(intent: &JobIntent) -> Result<()> {
    if !valid_identifier(&intent.name)
        || matches!(&intent.trigger, crate::intent::Trigger::Cron(expr) if expr.len() > MAX_JOB_STRING_BYTES)
    {
        return Err(codec_err("analytics-job intent exceeds storage limits"));
    }
    validate_placement(&intent.policy)
}

fn decode_intent(bytes: &[u8]) -> Result<JobIntent> {
    let intent = decode_stored(bytes)?;
    validate_intent(&intent)?;
    Ok(intent)
}

fn encode_intent(intent: &JobIntent) -> Result<Vec<u8>> {
    validate_intent(intent)?;
    let bytes = rmp_serde::to_vec_named(intent).map_err(codec_err)?;
    if bytes.len() > MAX_STORED_JOB_BYTES {
        return Err(codec_err("analytics-job intent exceeds storage limits"));
    }
    Ok(bytes)
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Everything [`JobStore::submit`] needs to create a fresh [`AnalyticsJob`].
#[derive(Clone, Debug)]
pub struct SubmitSpec {
    pub input_snapshot: crate::model::InputSnapshotHandle,
    pub policy: JobPolicy,
    pub algo: crate::model::AlgoVersion,
    pub input_payload: Option<Vec<u8>>,
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

/// A durable local projection of [`AnalyticsJob`] records, backed by `jobs.redb`.
pub struct JobStore {
    /// Sole physical owner of `jobs.redb` (declared `OwnerLayout::Jobs`, whose 13
    /// owner tables are the authoritative `JOBS` plus every scheduler secondary
    /// index). Reads are the scoped reads it issues; nothing here can open a
    /// database.
    kernel: StorageKernel,
    /// Sole writer. Holds this file's one move-once mutation authority, so every
    /// job transition is admitted, ordered, fenced and committed through it.
    mutations: MutationKernel,
    /// The one authenticated, bound serving scope -- the fixed native identity of
    /// `analytics_job_scope_identity`, validated exactly once per open.
    owner: OwnedStoreHandle<JobsOwner>,
    /// No-rand monotonic id source (mirrors `src/server/txn.rs::TxnIdGen`): `"job-<hex>"`.
    next_id: AtomicU64,
}

impl JobStore {
    /// Open (or create) the job store at an exact file path through the storage
    /// kernel.
    ///
    /// The kernel materializes the whole declared `OwnerLayout::Jobs` census when
    /// the file is created and re-validates it on every open, so the 13-table
    /// bootstrap closure the retired raw constructor needed is gone. `verifier`
    /// is the composition root's proof authority: only it may decide that
    /// `principal` may serve the fixed `analytics-jobs` scope.
    pub fn open(
        path: &Path,
        verifier: &dyn ScopeGrantVerifier,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self> {
        let identity = analytics_job_scope_identity()?;
        let physical = PhysicalStoreIdentity::new(JOBS_PHYSICAL_STORE).map_err(redb_err)?;
        let kernel = if path.exists() {
            StorageKernel::open_owner::<JobsOwner>(path, physical, None)
        } else {
            StorageKernel::create_owner::<JobsOwner>(path, physical, None)
        }
        .map_err(redb_err)?;
        let (kernel, authority) = kernel
            .into_read_and_mutation_authority()
            .map_err(redb_err)?;
        let mutations = MutationKernel::new(authority);
        let grant = kernel
            .authenticate_scope::<JobsOwner>(verifier, identity, principal.to_string(), proof)
            .map_err(redb_err)?;
        let owner = kernel.bind_serving_scope(grant, 0).map_err(redb_err)?;
        mutations.bootstrap_ledger(&owner).map_err(redb_err)?;
        let store = Self {
            kernel,
            mutations,
            owner,
            next_id: AtomicU64::new(0),
        };
        let next_id = initialize_scheduler_indexes(&store)?;
        store.next_id.store(next_id, Ordering::Relaxed);
        Ok(store)
    }

    /// One kernel-issued scoped read over this store's bound serving scope.
    fn scoped_read(&self) -> Result<ScopedRead<'_, JobsOwner>> {
        self.kernel.read_scope(&self.owner).map_err(redb_err)
    }

    /// The scope's authoritative mutation version, read outside any write.
    fn live_version(&self) -> Result<u64> {
        eg_transaction::version(&self.scoped_read()?).map_err(redb_err)
    }

    /// Run one store-level MAINTENANCE mutation (RF-RULING-005).
    ///
    /// The scheduler index rebuild, the two idempotency ledgers and the intent
    /// registry carry no caller identity and are not job transitions, but there
    /// is no un-ledgered owner-write path any more, so they land as ledgered,
    /// fenced, version-bumping maintenance batches. The batch identity is
    /// `(kind, scope version)`: unique per attempt and stable across a
    /// crash-retry of that attempt, so a retry replays instead of colliding.
    fn maintain<T, F>(&self, kind: &str, apply: F) -> Result<T>
    where
        F: FnOnce(&AdmittedOwnerWrite<'_, JobsOwner>) -> Result<T>,
    {
        let expected_version = self.live_version()?;
        let batch = maintenance_batch(
            kind,
            self.owner.identity(),
            self.owner.principal(),
            expected_version,
        )?;
        let (write, begun) = self
            .mutations
            .admit_maintenance(&self.owner, &batch)
            .map_err(redb_err)?;
        let source_version = match begun {
            Begin::Replay(_) => {
                write.abort().map_err(redb_err)?;
                return Err(codec_err(
                    "analytics-job maintenance batch already committed",
                ));
            }
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = write.owner_rows(&self.owner, &batch).map_err(redb_err)?;
        let outcome = apply(&owner_write);
        // Always close the owner capability: dropping it unfinished poisons the
        // write and would mask the staging error.
        owner_write.finish_owner().map_err(redb_err)?;
        let outcome = match outcome {
            Ok(value) => value,
            Err(error) => {
                write.abort().map_err(redb_err)?;
                return Err(error);
            }
        };
        self.mutations
            .finish(&write, &batch, None, 0, source_version)
            .map_err(redb_err)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(outcome)
    }

    /// Open `{persist_dir}/jobs.redb` — the durable location beside the graph shards.
    pub fn open_in_dir(
        persist_dir: &Path,
        verifier: &dyn ScopeGrantVerifier,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self> {
        std::fs::create_dir_all(persist_dir)
            .map_err(|e| JobError::Redb(format!("create persist dir: {e}")))?;
        Self::open(&persist_dir.join("jobs.redb"), verifier, principal, proof)
    }

    fn next_job_id(&self) -> JobId {
        let n = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("job-{n:016x}")
    }

    fn get_raw(&self, job_id: &str) -> Result<AnalyticsJob> {
        if job_id.is_empty() || job_id.len() > MAX_JOB_ID_BYTES {
            return Err(codec_err("analytics-job identifier is invalid"));
        }
        let read = self.scoped_read()?;
        let table = read.open_owner_table(JOBS).map_err(redb_err)?;
        let blob = table
            .get(job_id)
            .map_err(redb_err)?
            .ok_or_else(|| JobError::NotFound(job_id.to_string()))?;
        decode_job(blob.value())
    }

    fn put_raw(&self, job: &AnalyticsJob) -> Result<()> {
        let expected_version = self.live_version()?;
        let batch = internal_job_batch(
            job,
            self.owner.identity(),
            self.owner.principal(),
            expected_version,
        )?;
        self.put_raw_batch(job, &batch, job.updated_at_ms.max(0) as u64)
            .map(|_| ())
    }

    fn put_raw_batch(
        &self,
        job: &AnalyticsJob,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<AnalyticsJob> {
        let blob = encode_job(job)?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        match write.begin(batch).map_err(redb_err)? {
            Begin::Replay(record) => {
                let replayed = decode_job_result(&record)?;
                write.abort().map_err(redb_err)?;
                Ok(replayed)
            }
            Begin::Apply { source_version } => {
                let owner_write = write.owner_rows(&self.owner, batch).map_err(redb_err)?;
                let staged = persist_job_image(&owner_write, job, blob.as_slice());
                owner_write.finish_owner().map_err(redb_err)?;
                staged?;
                self.mutations
                    .finish(&write, batch, Some(blob), committed_at_ms, source_version)
                    .map_err(redb_err)?;
                self.mutations.commit(write, batch).map_err(redb_err)?;
                Ok(job.clone())
            }
        }
    }

    /// Submit a new job — the `Submitted` entry point. Returns the durable record
    /// (including its server-issued `job_id`).
    pub fn submit(&self, spec: SubmitSpec) -> Result<AnalyticsJob> {
        validate_placement(&spec.policy)?;
        let now = now_ms();
        let job = AnalyticsJob {
            job_id: self.next_job_id(),
            input_snapshot: spec.input_snapshot,
            policy: spec.policy,
            algo: spec.algo,
            input_payload: spec.input_payload,
            retry: crate::model::RetryPolicy {
                max_attempts: spec.max_attempts.max(1),
                backoff_ms: spec.backoff_ms,
                attempts_made: 0,
            },
            state: JobState::Submitted,
            cancel_requested: false,
            lease_epoch: 0,
            lease: None,
            last_worker_ref: String::new(),
            not_before_ms: 0,
            output: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.put_raw(&job)?;
        Ok(job)
    }

    /// Submit and persist the server-issued job plus the caller's universal batch
    /// result/status/fence/idempotency/outbox in one `jobs.redb` transaction.
    pub fn submit_batch(
        &self,
        spec: SubmitSpec,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(AnalyticsJob, bool)> {
        validate_placement(&spec.policy)?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        match write.begin(batch).map_err(redb_err)? {
            Begin::Replay(record) => {
                let replayed = decode_job_result(&record)?;
                write.abort().map_err(redb_err)?;
                Ok((replayed, true))
            }
            Begin::Apply { source_version } => {
                let now = committed_at_ms as i64;
                let job = AnalyticsJob {
                    // The batch identity is selected before Raft proposal and is
                    // identical on every state-machine replica. Deriving the job id
                    // from it removes local AtomicU64 timing from replicated state.
                    job_id: job_id_for_batch(batch),
                    input_snapshot: spec.input_snapshot,
                    policy: spec.policy,
                    algo: spec.algo,
                    input_payload: spec.input_payload,
                    retry: crate::model::RetryPolicy {
                        max_attempts: spec.max_attempts.max(1),
                        backoff_ms: spec.backoff_ms,
                        attempts_made: 0,
                    },
                    state: JobState::Submitted,
                    cancel_requested: false,
                    lease_epoch: 0,
                    lease: None,
                    last_worker_ref: String::new(),
                    not_before_ms: 0,
                    output: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                };
                let blob = encode_job(&job)?;
                let owner_write = write.owner_rows(&self.owner, batch).map_err(redb_err)?;
                let staged = persist_job_image(&owner_write, &job, blob.as_slice());
                owner_write.finish_owner().map_err(redb_err)?;
                staged?;
                self.mutations
                    .finish(&write, batch, Some(blob), committed_at_ms, source_version)
                    .map_err(redb_err)?;
                self.mutations.commit(write, batch).map_err(redb_err)?;
                Ok((job, false))
            }
        }
    }

    /// `tenant`/`graph` are accepted for call-site compatibility with every
    /// external caller that compiles a [`MutationBatch`] against this store
    /// (e.g. `src/server/handlers/jobs.rs::compile_job_batch`), but are
    /// otherwise unused: this store has exactly ONE fixed native mutation
    /// scope (`analytics_job_scope_identity`) for the whole `jobs.redb`, so
    /// the live version of THAT scope is always the correct answer regardless
    /// of which tenant/graph string a caller names.
    pub fn mutation_version(&self, _tenant: &str, _graph: &str) -> Result<u64> {
        self.live_version()
    }

    /// Fetch a job's current durable record.
    pub fn get(&self, job_id: &str) -> Result<AnalyticsJob> {
        self.get_raw(job_id)
    }

    /// List every durable job id (diagnostic/admin use).
    pub fn list_ids(&self) -> Result<Vec<JobId>> {
        let read = self.scoped_read()?;
        let table = read.open_owner_table(JOBS).map_err(redb_err)?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for entry in table.iter().map_err(redb_err)? {
            let (k, _) = entry.map_err(redb_err)?;
            let id = k.value();
            if id.is_empty() || id.len() > MAX_JOB_ID_BYTES || out.len() >= MAX_JOB_LIST_ITEMS {
                return Err(codec_err("analytics-job list exceeds response limits"));
            }
            bytes = bytes
                .checked_add(id.len())
                .filter(|total| *total <= MAX_JOB_LIST_BYTES)
                .ok_or_else(|| codec_err("analytics-job list exceeds response limits"))?;
            out.push(id.to_string());
        }
        Ok(out)
    }

    /// Aggregate durable queue state without exposing job, tenant or worker ids.
    pub fn metric_counts(&self, now_ms: i64) -> Result<(i64, i64, i64)> {
        let read = self.scoped_read()?;
        let table = read.open_owner_table(JOBS).map_err(redb_err)?;
        let mut ready = 0_i64;
        let mut active = 0_i64;
        let mut publishing = 0_i64;
        let mut scanned = 0usize;
        for entry in table.iter().map_err(redb_err)? {
            scanned = scanned
                .checked_add(1)
                .filter(|count| *count <= MAX_JOB_LIST_ITEMS)
                .ok_or_else(|| codec_err("analytics-job metric scan exceeds limits"))?;
            let (_, value) = entry.map_err(redb_err)?;
            let job = decode_job(value.value())?;
            let live_lease = job
                .lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at_ms > now_ms);
            if live_lease {
                active = active.saturating_add(1);
            }
            if matches!(&job.state, JobState::Publishing { .. }) {
                publishing = publishing.saturating_add(1);
            }
            if job_is_ready_for_scheduling(&job, live_lease, now_ms) {
                ready = ready.saturating_add(1);
            }
        }
        Ok((ready, active, publishing))
    }

    /// Lease the highest-priority eligible job. Selection, quota evaluation,
    /// lease-epoch increment, state transition and durable MutationBatch record
    /// share one immediate redb transaction.
    pub fn claim_next(
        &self,
        worker_ref: &str,
        worker_capabilities: &[String],
        now_ms: i64,
        lease_ms: u64,
        quota: TenantJobQuota,
    ) -> Result<Option<WorkerClaim>> {
        if claim_next_args_invalid(worker_ref, worker_capabilities, lease_ms, &quota) {
            return Err(codec_err(
                "worker claim requires an opaque worker reference and limits",
            ));
        }
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        // A caller may lose the Claim response after the transaction committed.
        // The worker secondary index turns this from an all-job scan into one
        // logarithmic lookup; concurrent worker slots use distinct references.
        if let Some(claim) = live_worker_claim_in_wtx(&write, worker_ref, now_ms)? {
            // No public abort; dropping `write` un-committed here aborts the
            // transaction -- nothing was written on this early return.
            return Ok(Some(claim));
        }

        // Reconcile abandoned work in the SAME transaction used to choose the
        // next lease. Secondary expiry/deadline/cancellation indexes bound this to
        // rows whose reconciliation condition is due, not total durable history.
        //
        // `expected_version` starts from a live read of this store's ONE fixed
        // native scope, taken before any write in this transaction. Each
        // reconciled/claimed job transition inside `write` advances it locally
        // (rather than re-reading `JobStore::live_version`, which opens a
        // FRESH read snapshot and would not observe this transaction's own
        // still-uncommitted `finish()` writes) -- see `reconcile_scheduler` /
        // `write_job_transition`.
        let expected_version = self.live_version()?;
        let (reconcile_batch, expected_version) =
            reconcile_scheduler(self, &write, now_ms, expected_version)?;
        let capabilities: BTreeSet<_> = worker_capabilities.iter().map(String::as_str).collect();

        // Each ready job has exactly one deterministic placement anchor.  Seek
        // only the unconstrained queue and the capability/pool/region anchors the
        // worker supplied, then merge the first eligible row from each ordered
        // range. Jobs whose anchor the worker cannot satisfy are never decoded.
        let selected =
            select_ready_for_worker(&write, worker_capabilities, &capabilities, now_ms, quota)?;
        let Some(mut job) = selected else {
            if let Some(batch) = reconcile_batch {
                self.mutations.commit(write, &batch).map_err(redb_err)?;
            }
            // else: nothing was written in this transaction; dropping `write`
            // un-committed aborts it.
            return Ok(None);
        };
        let starts_attempt = !matches!(&job.state, JobState::Publishing { .. });
        job.lease_epoch = job.lease_epoch.saturating_add(1);
        if starts_attempt {
            job.retry.attempts_made = job.retry.attempts_made.saturating_add(1);
        }
        let lease = WorkerLease {
            worker_ref: worker_ref.to_string(),
            epoch: job.lease_epoch,
            acquired_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(lease_ms as i64),
        };
        job.lease = Some(lease.clone());
        job.last_worker_ref = worker_ref.to_string();
        job.not_before_ms = 0;
        if matches!(&job.state, JobState::Submitted) {
            job.state = JobState::Running {
                checkpoint: Checkpoint {
                    progress: 0.0,
                    stage: "leased".to_string(),
                    state_blob: None,
                    updated_at_ms: now_ms,
                },
            };
        }
        job.updated_at_ms = now_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(Some(WorkerClaim { job, lease }))
    }

    pub fn renew_lease(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        now_ms: i64,
        lease_ms: u64,
    ) -> Result<WorkerLease> {
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        require_lease(&job, worker_ref, epoch, now_ms)?;
        let lease = job.lease.as_mut().expect("require_lease checked presence");
        lease.expires_at_ms = now_ms.saturating_add(lease_ms as i64);
        let renewed = lease.clone();
        job.updated_at_ms = now_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(renewed)
    }

    /// Read and validate exact live lease ownership without mutating its expiry.
    pub fn verify_lease(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        now_ms: i64,
    ) -> Result<AnalyticsJob> {
        let job = self.get_raw(job_id)?;
        require_lease(&job, worker_ref, epoch, now_ms)?;
        Ok(job)
    }

    pub fn checkpoint_fenced(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        checkpoint: Checkpoint,
        now_ms: i64,
    ) -> Result<AnalyticsJob> {
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        require_lease(&job, worker_ref, epoch, now_ms)?;
        if !matches!(&job.state, JobState::Running { .. }) {
            return Err(invalid_transition(
                &job,
                "fenced checkpoint requires Running",
            ));
        }
        job.state = JobState::Running { checkpoint };
        job.updated_at_ms = now_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(job)
    }

    /// Persist the complete typed result and enter non-terminal ``Publishing``.
    pub fn stage_result_fenced(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        result: TypedJobResult,
        now_ms: i64,
    ) -> Result<AnalyticsJob> {
        result.validate().map_err(codec_err)?;
        let result_bytes = rmp_serde::to_vec_named(&result).map_err(codec_err)?;
        if result_bytes.len() > MAX_JOB_RESULT_BYTES {
            return Err(codec_err("typed result exceeds the storage limit"));
        }
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        require_lease(&job, worker_ref, epoch, now_ms)?;
        if !lineage_matches_job(&result.reproducibility, &job) {
            return Err(codec_err(
                "typed result reproducibility manifest does not match its leased job",
            ));
        }
        if matches!(&job.state, JobState::Publishing { .. }) {
            if job.output.as_ref() == Some(&result) {
                // No public abort; dropping `write` un-committed aborts it.
                return Ok(job);
            }
            return Err(invalid_transition(
                &job,
                "a different result is already staged for this job",
            ));
        }
        let checkpoint = match &job.state {
            JobState::Running { checkpoint } => checkpoint.clone(),
            other => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: other.label(),
                    reason: "staging a result requires Running",
                })
            }
        };
        if let Some(limit) = job.policy.resources.output_bytes {
            if result_bytes.len() as u64 > limit {
                return Err(codec_err("typed result exceeds the job output budget"));
            }
        }
        let result_ref = job.result_ref();
        job.output = Some(result.clone());
        job.state = JobState::Publishing {
            result_ref,
            checkpoint: Checkpoint {
                progress: 1.0,
                stage: "publishing".to_string(),
                updated_at_ms: now_ms,
                ..checkpoint
            },
        };
        job.updated_at_ms = now_ms;
        let (batch, _next_version) = write_job_transition(
            self,
            &write,
            &job,
            expected_version,
            Some((&result.dataset_ref, &result_bytes)),
        )?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(job)
    }

    /// Mark success only after the claim/result MutationBatch committed.
    pub fn complete_publication_fenced(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        now_ms: i64,
    ) -> Result<AnalyticsJob> {
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        require_lease(&job, worker_ref, epoch, now_ms)?;
        let (result_ref, mut checkpoint) = match &job.state {
            JobState::Publishing {
                result_ref,
                checkpoint,
            } => (result_ref.clone(), checkpoint.clone()),
            other => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: other.label(),
                    reason: "publication completion requires Publishing",
                })
            }
        };
        checkpoint.stage = "published".to_string();
        checkpoint.updated_at_ms = now_ms;
        job.state = JobState::Succeeded {
            result_ref,
            checkpoint,
        };
        job.lease = None;
        job.updated_at_ms = now_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(job)
    }

    /// Finalize a publication whose target-graph commit was durably ordered by
    /// another Raft group. The scheduler prepare already proved a live lease; the
    /// target commit may legitimately outlast that lease, so finalization checks
    /// the immutable staged result and exact worker epoch without consulting wall
    /// clock expiry. A newer lease epoch always fences this receipt out.
    pub fn complete_publication_prepared(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        expected_result_ref: &str,
        committed_at_ms: i64,
    ) -> Result<AnalyticsJob> {
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        if job.last_worker_ref != worker_ref || job.lease_epoch != epoch {
            return Err(invalid_transition(&job, "stale publication fencing token"));
        }
        match &job.state {
            JobState::Succeeded { result_ref, .. } if result_ref == expected_result_ref => {
                // No public abort; dropping `write` un-committed aborts it.
                return Ok(job);
            }
            JobState::Publishing {
                result_ref,
                checkpoint,
            } if result_ref == expected_result_ref => {
                let mut checkpoint = checkpoint.clone();
                checkpoint.stage = "published".to_string();
                checkpoint.updated_at_ms = committed_at_ms;
                job.state = JobState::Succeeded {
                    result_ref: result_ref.clone(),
                    checkpoint,
                };
            }
            JobState::Publishing { .. } => {
                return Err(invalid_transition(
                    &job,
                    "publication receipt does not match staged result",
                ));
            }
            _ => {
                return Err(invalid_transition(
                    &job,
                    "publication finalization requires Publishing",
                ));
            }
        }
        job.lease = None;
        job.updated_at_ms = committed_at_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(job)
    }

    /// Release a publication lease without changing the durable result or state.
    /// Another worker can immediately replay the idempotent claim batch under a
    /// higher epoch instead of waiting for the failed publisher's lease timeout.
    pub fn release_publication_lease_fenced(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        now_ms: i64,
    ) -> Result<AnalyticsJob> {
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        require_lease(&job, worker_ref, epoch, now_ms)?;
        if !matches!(&job.state, JobState::Publishing { .. }) {
            return Err(invalid_transition(
                &job,
                "publication lease release requires Publishing",
            ));
        }
        job.lease = None;
        job.updated_at_ms = now_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(job)
    }

    /// Release a failed fenced attempt.  Retryable work returns to ``Submitted``
    /// with a durable backoff timestamp; an exhausted job becomes terminal.
    pub fn fail_attempt_fenced(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        reason: impl Into<String>,
        now_ms: i64,
    ) -> Result<AnalyticsJob> {
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        require_lease(&job, worker_ref, epoch, now_ms)?;
        let checkpoint = match &job.state {
            JobState::Running { checkpoint } => checkpoint.clone(),
            other => {
                return Err(invalid_transition(
                    &job,
                    match other {
                        JobState::Publishing { .. } => {
                            "publication failures must retain Publishing for replay"
                        }
                        _ => "failed attempts require Running",
                    },
                ))
            }
        };
        if job.retry.retries_remaining() {
            job.state = JobState::Submitted;
            job.not_before_ms = now_ms.saturating_add(job.retry.backoff_ms as i64);
            // Retain the resumable checkpoint in the opaque input payload; the
            // executor refreshes it on the next fenced claim.
        } else {
            job.state = JobState::Failed {
                reason: reason.into(),
                checkpoint: Some(checkpoint),
            };
        }
        job.lease = None;
        job.updated_at_ms = now_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(job)
    }

    /// Finalize cooperative cancellation with lease fencing.
    pub fn mark_cancelled_fenced(
        &self,
        job_id: &str,
        worker_ref: &str,
        epoch: u64,
        now_ms: i64,
    ) -> Result<AnalyticsJob> {
        let expected_version = self.live_version()?;
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        let mut job = read_job_in_wtx(&write, job_id)?;
        // Cancellation acknowledgement is the one fenced worker mutation that
        // must remain legal after `cancel_requested` is set. It still requires
        // exact live ownership/epoch; only the generic cancellation rejection is
        // skipped here.
        require_lease_ownership(&job, worker_ref, epoch, now_ms)?;
        let checkpoint = match &job.state {
            JobState::Running { checkpoint } if job.cancel_requested => checkpoint.clone(),
            JobState::Running { .. } => {
                return Err(invalid_transition(&job, "cancellation was not requested"))
            }
            _ => {
                return Err(invalid_transition(
                    &job,
                    "fenced cancellation requires Running",
                ))
            }
        };
        job.state = JobState::Cancelled {
            checkpoint: Some(checkpoint),
        };
        job.lease = None;
        job.updated_at_ms = now_ms;
        let (batch, _next_version) =
            write_job_transition(self, &write, &job, expected_version, None)?;
        self.mutations.commit(write, &batch).map_err(redb_err)?;
        Ok(job)
    }

    pub fn get_result(&self, dataset_ref: &str) -> Result<TypedJobResult> {
        if !valid_identifier(dataset_ref) {
            return Err(codec_err("analytics-job result reference is invalid"));
        }
        let read = self.scoped_read()?;
        let table = read.open_owner_table(RESULTS).map_err(redb_err)?;
        let row = table
            .get(dataset_ref)
            .map_err(redb_err)?
            .ok_or_else(|| JobError::NotFound(dataset_ref.to_string()))?;
        decode_result(row.value())
    }

    /// Cooperative cancel (CONCEPT:INT-P2-1): sets `cancel_requested`, and for a job
    /// that has not started yet (`Submitted`) transitions straight to `Cancelled`
    /// (nothing is running to observe the flag). A `Running` job stays `Running`
    /// until the executor observes `cancel_requested` and calls
    /// [`Self::mark_cancelled_fenced`] — cancellation of in-flight work is
    /// cooperative, not preemptive.
    pub fn request_cancel_batch(
        &self,
        job_id: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(AnalyticsJob, bool)> {
        self.mutate_job_batch(job_id, batch, committed_at_ms, |job| {
            if job.state.is_terminal() {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: job.state.label(),
                    reason: "cannot cancel a job already in a terminal state",
                });
            }
            if matches!(&job.state, JobState::Publishing { .. }) {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: "publishing",
                    reason: "a durable result publication cannot be cancelled",
                });
            }
            job.cancel_requested = true;
            if matches!(&job.state, JobState::Submitted) {
                job.state = JobState::Cancelled { checkpoint: None };
            }
            job.updated_at_ms = committed_at_ms as i64;
            Ok(())
        })
    }

    /// Resume a job from its last checkpoint (CONCEPT:INT-P2-1): valid from
    /// `Failed` (a deliberate retry — requires retries remaining) or from `Running`
    /// (an ORPHANED job — the record says "running" but the process that was running
    /// it crashed/restarted with no live task; resuming re-enters `Running` from the
    /// SAME checkpoint so the caller's executor can pick the work back up). A
    /// `Cancelled` job is a terminal, deliberate stop — NOT resumable; resubmit
    /// instead. A `Succeeded` job is already done — resuming it is also rejected
    /// (fetch the existing `result_ref` instead).
    pub fn resume_batch(
        &self,
        job_id: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(AnalyticsJob, bool)> {
        self.mutate_job_batch(job_id, batch, committed_at_ms, |job| {
            match &job.state {
                JobState::Running { checkpoint } => {
                    job.state = JobState::Running {
                        checkpoint: checkpoint.clone(),
                    };
                }
                JobState::Failed { checkpoint, .. } => {
                    if !job.retry.retries_remaining() {
                        return Err(JobError::InvalidTransition {
                            job_id: job_id.to_string(),
                            state: "failed",
                            reason: "no retries remaining (retry.attempts_made >= max_attempts)",
                        });
                    }
                    job.cancel_requested = false;
                    job.state = JobState::Running {
                        checkpoint: checkpoint.clone().unwrap_or_default(),
                    };
                }
                other => {
                    return Err(JobError::InvalidTransition {
                        job_id: job_id.to_string(),
                        state: other.label(),
                        reason:
                            "resume requires Failed (with retries remaining) or an orphaned Running job",
                    });
                }
            }
            job.updated_at_ms = committed_at_ms as i64;
            Ok(())
        })
    }

    fn mutate_job_batch<F>(
        &self,
        job_id: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
        mutate: F,
    ) -> Result<(AnalyticsJob, bool)>
    where
        F: FnOnce(&mut AnalyticsJob) -> Result<()>,
    {
        let write = self.mutations.open_write(&self.owner).map_err(redb_err)?;
        match write.begin(batch).map_err(redb_err)? {
            Begin::Replay(record) => {
                let replayed = decode_job_result(&record)?;
                write.abort().map_err(redb_err)?;
                Ok((replayed, true))
            }
            Begin::Apply { source_version } => {
                let mut job = read_job_in_wtx(&write, job_id)?;
                mutate(&mut job)?;
                let blob = encode_job(&job)?;
                let owner_write = write.owner_rows(&self.owner, batch).map_err(redb_err)?;
                let staged = persist_job_image(&owner_write, &job, blob.as_slice());
                owner_write.finish_owner().map_err(redb_err)?;
                staged?;
                self.mutations
                    .finish(&write, batch, Some(blob), committed_at_ms, source_version)
                    .map_err(redb_err)?;
                self.mutations.commit(write, batch).map_err(redb_err)?;
                Ok((job, false))
            }
        }
    }

    /// Store-level idempotency ledger (CONCEPT:INT-P2-1 determinism): atomically
    /// claims `result_ref` for `job_id`. Returns `true` the FIRST time a given
    /// `result_ref` is claimed (the caller should proceed to commit the claim into
    /// the graph); `false` if it was already claimed by ANY job (this one or another
    /// with identical lineage) — the caller's commit is then a no-op. Independent of
    /// (and a cheaper first check than) the `GraphCore::has_node` guard `claim.rs`
    /// also applies, so idempotency holds even across stores/processes that share a
    /// `jobs.redb` but talk to different graph replicas.
    pub fn mark_result_committed(&self, result_ref: &str, job_id: &str) -> Result<bool> {
        if !valid_identifier(result_ref) || job_id.is_empty() || job_id.len() > MAX_JOB_ID_BYTES {
            return Err(codec_err("analytics-job idempotency key is invalid"));
        }
        self.maintain("result-committed", |wtx| {
            let mut table = wtx.open_table(COMMITTED_RESULTS).map_err(redb_err)?;
            let existing = table
                .get(result_ref)
                .map_err(redb_err)?
                .map(|v| v.value().to_string());
            match existing {
                Some(_) => Ok(false),
                None => {
                    table.insert(result_ref, job_id).map_err(redb_err)?;
                    Ok(true)
                }
            }
        })
    }

    /// Which job (if any) first committed `result_ref`.
    pub fn result_committed_by(&self, result_ref: &str) -> Result<Option<JobId>> {
        self.first_wins_owner(
            COMMITTED_RESULTS,
            result_ref,
            "analytics-job result reference is invalid",
        )
    }

    /// The owner recorded against `subject` in a first-wins ledger table, or `None` when
    /// nobody has claimed it. Shared by the `COMMITTED_RESULTS` and `IDEMPOTENCY_LEDGER`
    /// readers, which differ only in their table and the message for a malformed subject.
    fn first_wins_owner(
        &self,
        ledger: TableDefinition<'static, &str, &str>,
        subject: &str,
        invalid: &str,
    ) -> Result<Option<String>> {
        if !valid_identifier(subject) {
            return Err(codec_err(invalid));
        }
        let read = self.scoped_read()?;
        let table = read.open_owner_table(ledger).map_err(redb_err)?;
        Ok(table
            .get(subject)
            .map_err(redb_err)?
            .map(|v| v.value().to_string()))
    }

    // ── Generalized idempotency ledger (CONCEPT:INT-P2-1, Phase 3) ──────────────
    // The sibling of `mark_result_committed`/`result_committed_by`, generalized
    // beyond `result_ref -> job_id` to ANY caller-supplied `(key, owner)` pair — the
    // primitive `JobIntent` tick-dedup (and any future non-analytics job kind) reuses
    // instead of re-inventing first-wins claiming.

    /// Atomically claim `key` for `owner`. Returns `true` the FIRST time a given key
    /// is claimed by ANYONE (the caller should proceed); `false` if it was already
    /// claimed (by this owner or another) — the caller's action is then a no-op. This
    /// is the exact same first-wins shape as [`Self::mark_result_committed`], just
    /// over an independent table so a `JobIntent` tick window doesn't collide with an
    /// analytics `result_ref` namespace.
    pub fn claim_idempotency(&self, key: &str, owner: &str) -> Result<bool> {
        if !valid_identifier(key) || !valid_identifier(owner) {
            return Err(codec_err("analytics-job idempotency key is invalid"));
        }
        self.maintain("idempotency-claim", |wtx| {
            let mut table = wtx.open_table(IDEMPOTENCY_LEDGER).map_err(redb_err)?;
            let existing = table
                .get(key)
                .map_err(redb_err)?
                .map(|v| v.value().to_string());
            match existing {
                Some(_) => Ok(false),
                None => {
                    table.insert(key, owner).map_err(redb_err)?;
                    Ok(true)
                }
            }
        })
    }

    /// Which owner (if any) first claimed `key`.
    pub fn idempotency_claimed_by(&self, key: &str) -> Result<Option<String>> {
        self.first_wins_owner(
            IDEMPOTENCY_LEDGER,
            key,
            "analytics-job idempotency key is invalid",
        )
    }

    // ── `JobIntent` registry (CONCEPT:INT-P2-1, daemon-consolidation design Phase 3) ──
    // A declarative trigger registry additive to the `AnalyticsJob` state machine
    // above: a job DECLARED with a schedule (cron/interval/manual) rather than driven
    // by an external caller. Lives in the SAME `jobs.redb` (a second `Database::open`
    // on the same file would hit redb's exclusive per-process file lock).

    fn get_intent_raw(&self, name: &str) -> Result<JobIntent> {
        if !valid_identifier(name) {
            return Err(codec_err("analytics-job intent identifier is invalid"));
        }
        let read = self.scoped_read()?;
        let table = read.open_owner_table(JOB_INTENTS).map_err(redb_err)?;
        let blob = table
            .get(name)
            .map_err(redb_err)?
            .ok_or_else(|| JobError::NotFound(name.to_string()))?;
        decode_intent(blob.value())
    }

    fn put_intent_raw(&self, intent: &JobIntent) -> Result<()> {
        let blob = encode_intent(intent)?;
        self.maintain("intent-register", |wtx| {
            wtx.open_table(JOB_INTENTS)
                .map_err(redb_err)?
                .insert(intent.name.as_str(), blob.as_slice())
                .map_err(redb_err)?;
            Ok(())
        })
    }

    /// Register (or re-register) a [`JobIntent`] by `name` (upsert). Re-registering
    /// an EXISTING name preserves its durable `last_run_ms`/`created_at_ms` history
    /// (only `trigger`/`policy`/`enabled` are overwritten) — the same
    /// "dual-write is idempotent" property AU's own schedule registration relies on,
    /// so a restart that redeclares its intents never resets their due-ness clock.
    pub fn register_intent(&self, mut intent: JobIntent) -> Result<JobIntent> {
        match self.get_intent_raw(&intent.name) {
            Ok(existing) => {
                intent.last_run_ms = existing.last_run_ms;
                intent.created_at_ms = existing.created_at_ms;
            }
            Err(JobError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        intent.updated_at_ms = now_ms();
        self.put_intent_raw(&intent)?;
        Ok(intent)
    }

    /// Fetch a registered intent's current durable record.
    pub fn get_intent(&self, name: &str) -> Result<JobIntent> {
        self.get_intent_raw(name)
    }

    /// List every registered intent (diagnostic/admin use, and the basis for
    /// [`Self::due_intents`]).
    pub fn list_intents(&self) -> Result<Vec<JobIntent>> {
        let read = self.scoped_read()?;
        let table = read.open_owner_table(JOB_INTENTS).map_err(redb_err)?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for entry in table.iter().map_err(redb_err)? {
            let (_, v) = entry.map_err(redb_err)?;
            if out.len() >= MAX_JOB_LIST_ITEMS {
                return Err(codec_err(
                    "analytics-job intent list exceeds response limits",
                ));
            }
            bytes = bytes
                .checked_add(v.value().len())
                .filter(|total| *total <= MAX_JOB_LIST_BYTES)
                .ok_or_else(|| codec_err("analytics-job intent list exceeds response limits"))?;
            out.push(decode_intent(v.value())?);
        }
        Ok(out)
    }

    /// Every registered, enabled intent whose trigger reports due as of `now_ms`
    /// (CONCEPT:INT-P2-1) — the due-evaluator half of the design doc's ONE
    /// job-intent registry (§B.1). Pure read: does NOT record a tick or mutate
    /// `last_run_ms` (call [`Self::record_intent_tick`] to actually claim + advance
    /// one).
    pub fn due_intents(&self, now_ms: i64) -> Result<Vec<JobIntent>> {
        Ok(self
            .list_intents()?
            .into_iter()
            .filter(|i| i.is_due(now_ms))
            .collect())
    }

    /// Claim ONE tick of `name`'s trigger window at `now_ms` and advance
    /// `last_run_ms` (CONCEPT:INT-P2-1 dedup/single-flight): computes the trigger's
    /// deterministic [`crate::intent::Trigger::tick_id`] for this window and claims
    /// it via [`Self::claim_idempotency`]. Returns `Ok(true)` the FIRST time this
    /// window is claimed — the caller should now actually run the job/sweep;
    /// `Ok(false)` if this window was already claimed (by this evaluator or a
    /// concurrent one) — a no-op, the coalesce guarantee AU's `collapse_stale_ticks`
    /// provides one layer up. Does NOT check `is_due`/`enabled` itself — callers
    /// drive `record_intent_tick` from `due_intents`'s output (or explicitly, for a
    /// `Manual` trigger), keeping "is this due" and "claim this window" separable.
    pub fn record_intent_tick(&self, name: &str, now_ms: i64) -> Result<bool> {
        let intent = self.get_intent_raw(name)?;
        let tick_id = intent.trigger.tick_id(name, now_ms);
        let claimed = self.claim_idempotency(&tick_id, name)?;
        if claimed {
            let mut intent = intent;
            intent.last_run_ms = Some(now_ms);
            intent.updated_at_ms = now_ms;
            self.put_intent_raw(&intent)?;
        }
        Ok(claimed)
    }
}

fn job_id_for_batch(batch: &MutationBatch) -> JobId {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"eg-jobs.consensus-job-id.v1\0");
    let tenant = batch.identity.tenant().as_str();
    digest.update((tenant.len() as u64).to_le_bytes());
    digest.update(tenant.as_bytes());
    // v2's flat `graph` field named the batch's target resource regardless of
    // scope kind; v1 splits that into a typed `MutationScope`. Fold in whichever
    // logical name the scope actually carries (graph name, or native resource
    // name) rather than substituting a sentinel for the arm that doesn't apply.
    let resource = match batch.identity.scope() {
        MutationScope::Graph { graph } => graph.as_str(),
        MutationScope::Native { resource, .. } => resource.as_str(),
    };
    digest.update((resource.len() as u64).to_le_bytes());
    digest.update(resource.as_bytes());
    digest.update((batch.batch_id.len() as u64).to_le_bytes());
    digest.update(batch.batch_id.as_bytes());
    format!("job-{}", hex::encode(&digest.finalize()[..16]))
}

fn initialize_scheduler_indexes(store: &JobStore) -> Result<u64> {
    // Fast path: a scoped READ, so a store whose indexes are already current
    // costs no mutation at all.
    {
        let read = store.scoped_read()?;
        let meta = read.open_owner_table(JOB_META).map_err(redb_err)?;
        let version = meta
            .get(META_INDEX_VERSION)
            .map_err(redb_err)?
            .map(|value| value.value());
        let max_sequence = meta
            .get(META_MAX_JOB_SEQUENCE)
            .map_err(redb_err)?
            .map(|value| value.value());
        if version == Some(SCHEDULER_INDEX_VERSION) {
            if let Some(max_sequence) = max_sequence {
                return Ok(max_sequence);
            }
        }
    }

    // One-time migration/backfill, as an admitted maintenance mutation. Decode
    // before clearing any index so a corrupt authoritative row aborts without
    // publishing a partial scheduler view.
    store.maintain("index-rebuild", |wtx| {
        let jobs = {
            let table = wtx.open_table(JOBS).map_err(redb_err)?;
            let mut jobs = Vec::new();
            let mut bytes = 0usize;
            for row in table.iter().map_err(redb_err)? {
                let (_, value) = row.map_err(redb_err)?;
                if jobs.len() >= MAX_JOB_REBUILD_ITEMS {
                    return Err(codec_err("scheduler index rebuild exceeds limits"));
                }
                bytes = bytes
                    .checked_add(value.value().len())
                    .filter(|total| *total <= MAX_JOB_REBUILD_BYTES)
                    .ok_or_else(|| codec_err("scheduler index rebuild exceeds limits"))?;
                jobs.push(decode_job(value.value())?);
            }
            jobs
        };
        clear_scheduler_indexes(wtx)?;
        let mut max_sequence = 0u64;
        for job in &jobs {
            add_job_indexes(wtx, job)?;
            if let Some(sequence) = job_sequence(&job.job_id) {
                max_sequence = max_sequence.max(sequence);
            }
        }
        {
            let mut meta = wtx.open_table(JOB_META).map_err(redb_err)?;
            meta.insert(META_MAX_JOB_SEQUENCE, max_sequence)
                .map_err(redb_err)?;
            // Publish the schema marker LAST. Both rows still commit atomically,
            // while this order documents the recovery invariant for future
            // migrations.
            meta.insert(META_INDEX_VERSION, SCHEDULER_INDEX_VERSION)
                .map_err(redb_err)?;
        }
        Ok(max_sequence)
    })
}

fn clear_scheduler_indexes(wtx: &AdmittedOwnerWrite<'_, JobsOwner>) -> Result<()> {
    wtx.open_table(JOB_READY)
        .map_err(redb_err)?
        .retain(|_, _| false)
        .map_err(redb_err)?;
    wtx.open_table(JOB_READY_BY_CAPABILITY)
        .map_err(redb_err)?
        .retain(|_, _| false)
        .map_err(redb_err)?;
    wtx.open_table(JOB_LEASE_BY_WORKER)
        .map_err(redb_err)?
        .retain(|_, _| false)
        .map_err(redb_err)?;
    wtx.open_table(JOB_LEASE_EXPIRY)
        .map_err(redb_err)?
        .retain(|_, _| false)
        .map_err(redb_err)?;
    wtx.open_table(JOB_TENANT_TOTALS)
        .map_err(redb_err)?
        .retain(|_, _| false)
        .map_err(redb_err)?;
    wtx.open_table(JOB_DEADLINE)
        .map_err(redb_err)?
        .retain(|_, _| false)
        .map_err(redb_err)?;
    wtx.open_table(JOB_CANCELLATION)
        .map_err(redb_err)?
        .retain(|_, _| false)
        .map_err(redb_err)?;
    Ok(())
}

fn persist_job_image(
    wtx: &AdmittedOwnerWrite<'_, JobsOwner>,
    job: &AnalyticsJob,
    bytes: &[u8],
) -> Result<()> {
    let previous = {
        let table = wtx.open_table(JOBS).map_err(redb_err)?;
        let value = table
            .get(job.job_id.as_str())
            .map_err(redb_err)?
            .map(|value| decode_job(value.value()))
            .transpose()?;
        value
    };
    let is_new = previous.is_none();
    if let Some(previous) = previous.as_ref() {
        remove_job_indexes(wtx, previous)?;
    }
    {
        let mut table = wtx.open_table(JOBS).map_err(redb_err)?;
        table.insert(job.job_id.as_str(), bytes).map_err(redb_err)?;
    }
    add_job_indexes(wtx, job)?;
    if is_new {
        update_max_job_sequence(wtx, &job.job_id)?;
    }
    Ok(())
}

fn add_job_indexes(wtx: &AdmittedOwnerWrite<'_, JobsOwner>, job: &AnalyticsJob) -> Result<()> {
    if ready_indexed(job) {
        let rank = priority_rank(job.policy.priority);
        wtx.open_table(JOB_READY)
            .map_err(redb_err)?
            .insert((rank, job.created_at_ms, job.job_id.as_str()), ())
            .map_err(redb_err)?;
        let anchor = placement_anchor(job);
        wtx.open_table(JOB_READY_BY_CAPABILITY)
            .map_err(redb_err)?
            .insert(
                (
                    anchor.as_str(),
                    rank,
                    job.created_at_ms,
                    job.job_id.as_str(),
                ),
                (),
            )
            .map_err(redb_err)?;
    }
    if let Some(lease) = &job.lease {
        wtx.open_table(JOB_LEASE_BY_WORKER)
            .map_err(redb_err)?
            .insert(lease.worker_ref.as_str(), job.job_id.as_str())
            .map_err(redb_err)?;
        wtx.open_table(JOB_LEASE_EXPIRY)
            .map_err(redb_err)?
            .insert((lease.expires_at_ms, job.job_id.as_str()), ())
            .map_err(redb_err)?;
        adjust_tenant_total(wtx, job, true)?;
    }
    if deadline_indexed(job) {
        let deadline = job
            .policy
            .deadline_unix_ms
            .expect("deadline-indexed job has a deadline");
        wtx.open_table(JOB_DEADLINE)
            .map_err(redb_err)?
            .insert((deadline, job.job_id.as_str()), ())
            .map_err(redb_err)?;
    }
    if cancellation_indexed(job) {
        wtx.open_table(JOB_CANCELLATION)
            .map_err(redb_err)?
            .insert(job.job_id.as_str(), ())
            .map_err(redb_err)?;
    }
    Ok(())
}

fn remove_job_indexes(wtx: &AdmittedOwnerWrite<'_, JobsOwner>, job: &AnalyticsJob) -> Result<()> {
    if ready_indexed(job) {
        let rank = priority_rank(job.policy.priority);
        wtx.open_table(JOB_READY)
            .map_err(redb_err)?
            .remove((rank, job.created_at_ms, job.job_id.as_str()))
            .map_err(redb_err)?;
        let anchor = placement_anchor(job);
        wtx.open_table(JOB_READY_BY_CAPABILITY)
            .map_err(redb_err)?
            .remove((
                anchor.as_str(),
                rank,
                job.created_at_ms,
                job.job_id.as_str(),
            ))
            .map_err(redb_err)?;
    }
    if let Some(lease) = &job.lease {
        {
            let mut workers = wtx.open_table(JOB_LEASE_BY_WORKER).map_err(redb_err)?;
            let points_to_job = workers
                .get(lease.worker_ref.as_str())
                .map_err(redb_err)?
                .is_some_and(|value| value.value() == job.job_id.as_str());
            if points_to_job {
                workers
                    .remove(lease.worker_ref.as_str())
                    .map_err(redb_err)?;
            }
        }
        wtx.open_table(JOB_LEASE_EXPIRY)
            .map_err(redb_err)?
            .remove((lease.expires_at_ms, job.job_id.as_str()))
            .map_err(redb_err)?;
        adjust_tenant_total(wtx, job, false)?;
    }
    if deadline_indexed(job) {
        wtx.open_table(JOB_DEADLINE)
            .map_err(redb_err)?
            .remove((
                job.policy.deadline_unix_ms.expect("indexed deadline"),
                job.job_id.as_str(),
            ))
            .map_err(redb_err)?;
    }
    if cancellation_indexed(job) {
        wtx.open_table(JOB_CANCELLATION)
            .map_err(redb_err)?
            .remove(job.job_id.as_str())
            .map_err(redb_err)?;
    }
    Ok(())
}

fn ready_indexed(job: &AnalyticsJob) -> bool {
    !job.cancel_requested
        && job.lease.is_none()
        && matches!(
            &job.state,
            JobState::Submitted | JobState::Running { .. } | JobState::Publishing { .. }
        )
}

fn deadline_indexed(job: &AnalyticsJob) -> bool {
    job.policy.deadline_unix_ms.is_some()
        && job.lease.is_none()
        && matches!(&job.state, JobState::Submitted | JobState::Running { .. })
}

fn cancellation_indexed(job: &AnalyticsJob) -> bool {
    job.cancel_requested && job.lease.is_none() && matches!(&job.state, JobState::Running { .. })
}

fn reserved_cpu(job: &AnalyticsJob) -> u64 {
    job.policy
        .resources
        .cpu_ms
        .or(job.policy.quota_cpu_ms)
        .unwrap_or(0)
}

fn tenant_index_key(tenant: &str) -> String {
    index_key(b"eg-jobs.tenant-index.v1\0", tenant)
}

fn adjust_tenant_total(
    wtx: &AdmittedOwnerWrite<'_, JobsOwner>,
    job: &AnalyticsJob,
    add: bool,
) -> Result<()> {
    let key = tenant_index_key(&job.policy.tenant);
    let cpu = reserved_cpu(job);
    let mut totals = wtx.open_table(JOB_TENANT_TOTALS).map_err(redb_err)?;
    let (count, reserved) = totals
        .get(key.as_str())
        .map_err(redb_err)?
        .map(|value| value.value())
        .unwrap_or((0, 0));
    let next = if add {
        (
            count
                .checked_add(1)
                .ok_or_else(|| codec_err("tenant active-count index overflow"))?,
            reserved
                .checked_add(cpu)
                .ok_or_else(|| codec_err("tenant CPU reservation index overflow"))?,
        )
    } else {
        (
            count
                .checked_sub(1)
                .ok_or_else(|| codec_err("tenant active-count index underflow"))?,
            reserved
                .checked_sub(cpu)
                .ok_or_else(|| codec_err("tenant CPU reservation index underflow"))?,
        )
    };
    if next == (0, 0) {
        totals.remove(key.as_str()).map_err(redb_err)?;
    } else {
        totals.insert(key.as_str(), next).map_err(redb_err)?;
    }
    Ok(())
}

fn priority_rank(priority: i32) -> u32 {
    u32::MAX - (priority as i64 - i32::MIN as i64) as u32
}

fn job_sequence(job_id: &str) -> Option<u64> {
    u64::from_str_radix(job_id.strip_prefix("job-")?, 16).ok()
}

fn update_max_job_sequence(wtx: &AdmittedOwnerWrite<'_, JobsOwner>, job_id: &str) -> Result<()> {
    let Some(sequence) = job_sequence(job_id) else {
        return Ok(());
    };
    let mut meta = wtx.open_table(JOB_META).map_err(redb_err)?;
    let current = meta
        .get(META_MAX_JOB_SEQUENCE)
        .map_err(redb_err)?
        .map(|value| value.value())
        .unwrap_or(0);
    if sequence > current {
        meta.insert(META_MAX_JOB_SEQUENCE, sequence)
            .map_err(redb_err)?;
    }
    Ok(())
}

fn live_worker_claim_in_wtx(
    wtx: &AdmittedMutation<'_, JobsOwner>,
    worker_ref: &str,
    now_ms: i64,
) -> Result<Option<WorkerClaim>> {
    let job_id = {
        let workers = wtx.open_read_table(JOB_LEASE_BY_WORKER).map_err(redb_err)?;
        let value = workers
            .get(worker_ref)
            .map_err(redb_err)?
            .map(|value| value.value().to_string());
        value
    };
    let Some(job_id) = job_id else {
        return Ok(None);
    };
    let job = read_job_in_wtx(wtx, &job_id)?;
    let Some(lease) = job.lease.as_ref() else {
        return Ok(None);
    };
    if lease.worker_ref != worker_ref || lease.expires_at_ms <= now_ms {
        return Ok(None);
    }
    let lease = lease.clone();
    Ok(Some(WorkerClaim { lease, job }))
}

fn tenant_active_total(
    wtx: &AdmittedMutation<'_, JobsOwner>,
    tenant: &str,
) -> Result<(usize, u64)> {
    let totals = wtx.open_read_table(JOB_TENANT_TOTALS).map_err(redb_err)?;
    let (count, cpu) = totals
        .get(tenant_index_key(tenant).as_str())
        .map_err(redb_err)?
        .map(|value| value.value())
        .unwrap_or((0, 0));
    Ok((usize::try_from(count).unwrap_or(usize::MAX), cpu))
}

fn capability_index_key(token: &str) -> String {
    index_key(b"eg-jobs.capability-index.v1\0", token)
}

/// A stable secondary-index key: SHA-256 over the NUL-terminated `domain` tag followed by
/// `value`, hex-encoded. The domain tag is what keeps the tenant and capability index
/// spaces from colliding on the same input string.
fn index_key(domain: &[u8], value: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

/// Whether `policy`'s tenant/actor/purpose/fingerprint scalars exceed their
/// storage limit. Split out of `validate_placement` (extract-method, cx/wD8).
fn policy_scalars_exceed_limits(policy: &JobPolicy) -> bool {
    !valid_identifier(&policy.tenant)
        || !valid_identifier(&policy.actor)
        || policy.purpose.len() > MAX_JOB_STRING_BYTES
        || policy.policy_fingerprint.len() > MAX_JOB_STRING_BYTES
}

/// Whether `placement`'s pool/region/capability fields exceed their storage
/// limit. Split out of `validate_placement` (extract-method, cx/wD8).
fn placement_fields_exceed_limits(placement: &crate::model::JobPlacement) -> bool {
    placement.required_capabilities.len() > 64
        || placement.pool.len() > 256
        || placement.region.len() > 256
        || placement.pool.contains('\0')
        || placement.region.contains('\0')
        || placement
            .required_capabilities
            .iter()
            .any(|value| value.is_empty() || value.len() > 256 || value.contains('\0'))
}

fn validate_placement(policy: &JobPolicy) -> Result<()> {
    let placement = &policy.placement;
    if policy_scalars_exceed_limits(policy) || placement_fields_exceed_limits(placement) {
        return Err(codec_err("job placement exceeds scheduler limits"));
    }
    Ok(())
}

/// Pick one stable anchor from all mandatory placement tokens.  Exactly one
/// secondary row is stored per job regardless of how many capabilities it
/// requires, keeping durable fan-out bounded while still excluding workers that
/// cannot possibly satisfy the chosen token.
fn placement_anchor(job: &AnalyticsJob) -> String {
    let placement = &job.policy.placement;
    placement
        .required_capabilities
        .iter()
        .map(|value| capability_index_key(value))
        .chain(
            (!placement.pool.is_empty())
                .then(|| capability_index_key(&format!("pool:{}", placement.pool))),
        )
        .chain(
            (!placement.region.is_empty())
                .then(|| capability_index_key(&format!("region:{}", placement.region))),
        )
        .min()
        .unwrap_or_else(|| UNCONSTRAINED_CAPABILITY_ANCHOR.to_string())
}

fn select_ready_for_worker(
    wtx: &AdmittedMutation<'_, JobsOwner>,
    worker_capabilities: &[String],
    capabilities: &BTreeSet<&str>,
    now_ms: i64,
    quota: TenantJobQuota,
) -> Result<Option<AnalyticsJob>> {
    let mut anchors = BTreeSet::from([UNCONSTRAINED_CAPABILITY_ANCHOR.to_string()]);
    anchors.extend(
        worker_capabilities
            .iter()
            .map(|value| capability_index_key(value)),
    );

    let ready = wtx
        .open_read_table(JOB_READY_BY_CAPABILITY)
        .map_err(redb_err)?;
    let jobs = wtx.open_read_table(JOBS).map_err(redb_err)?;
    let mut best: Option<((u32, i64, String), AnalyticsJob)> = None;
    let mut examined = 0usize;
    for anchor in anchors {
        let low = (anchor.as_str(), u32::MIN, i64::MIN, "");
        let high = (anchor.as_str(), u32::MAX, i64::MAX, "\u{10ffff}");
        for row in ready.range_inclusive(low, high).map_err(redb_err)? {
            examined = examined
                .checked_add(1)
                .filter(|count| *count <= MAX_SCHEDULER_RECONCILE_ITEMS)
                .ok_or_else(|| codec_err("scheduler ready scan exceeds limits"))?;
            let (key, _) = row.map_err(redb_err)?;
            let (_, rank, created_at_ms, job_id) = key.value();
            let Some(value) = jobs.get(job_id).map_err(redb_err)? else {
                continue;
            };
            let job = decode_job(value.value())?;
            if !job_ready_for_claim(
                wtx,
                &job,
                &anchor,
                worker_capabilities,
                capabilities,
                now_ms,
                quota,
            )? {
                continue;
            }
            let order = (rank, created_at_ms, job_id.to_string());
            if best.as_ref().is_none_or(|(current, _)| order < *current) {
                best = Some((order, job));
            }
            // The range is priority/FIFO ordered. Once one row from this anchor
            // is eligible, no later row in the same range can beat it.
            break;
        }
    }
    Ok(best.map(|(_, job)| job))
}

/// Whether `job` (found at `anchor`) is eligible to be claimed by a worker
/// with `worker_capabilities`/`capabilities`, under `quota`. Split out of
/// `select_ready_for_worker` (extract-method, cx/wD8) — same terms, same
/// order, same short-circuiting as before.
fn job_ready_for_claim(
    wtx: &AdmittedMutation<'_, JobsOwner>,
    job: &AnalyticsJob,
    anchor: &str,
    worker_capabilities: &[String],
    capabilities: &BTreeSet<&str>,
    now_ms: i64,
    quota: TenantJobQuota,
) -> Result<bool> {
    if !ready_indexed(job)
        || placement_anchor(job) != anchor
        || job.not_before_ms > now_ms
        || job
            .policy
            .deadline_unix_ms
            .is_some_and(|deadline| deadline <= now_ms)
        || !job_matches_worker(job, worker_capabilities, capabilities)
    {
        return Ok(false);
    }
    let (active_count, active_cpu) = tenant_active_total(wtx, &job.policy.tenant)?;
    let requested_cpu = reserved_cpu(job);
    Ok(active_count < quota.max_active
        && active_cpu.saturating_add(requested_cpu) <= quota.max_reserved_cpu_ms)
}

fn job_matches_worker(
    job: &AnalyticsJob,
    worker_capabilities: &[String],
    capabilities: &BTreeSet<&str>,
) -> bool {
    if !job
        .policy
        .placement
        .required_capabilities
        .iter()
        .all(|required| capabilities.contains(required.as_str()))
    {
        return false;
    }
    if !job.policy.placement.pool.is_empty()
        && !worker_capabilities
            .iter()
            .any(|value| value.strip_prefix("pool:") == Some(job.policy.placement.pool.as_str()))
    {
        return false;
    }
    job.policy.placement.region.is_empty()
        || worker_capabilities.iter().any(|value| {
            value.strip_prefix("region:") == Some(job.policy.placement.region.as_str())
        })
}

/// Collect the durable job ids awaiting cancellation reconciliation. Split
/// out of `reconcile_scheduler` (extract-method, cx/wD8) — same limit check,
/// same order as before.
fn collect_cancellation_ids(wtx: &AdmittedMutation<'_, JobsOwner>) -> Result<Vec<String>> {
    let table = wtx.open_read_table(JOB_CANCELLATION).map_err(redb_err)?;
    let mut ids = Vec::new();
    for row in table.iter().map_err(redb_err)? {
        let (key, _) = row.map_err(redb_err)?;
        if ids.len() >= MAX_SCHEDULER_RECONCILE_ITEMS {
            return Err(codec_err("scheduler reconciliation exceeds limits"));
        }
        ids.push(key.value().to_string());
    }
    Ok(ids)
}

/// Reconcile one cancellation-pending job. Split out of `reconcile_scheduler`
/// (extract-method, cx/wD8) — same terms, same order as before. Returns the
/// batch and the version now authoritative after this call (see
/// `write_job_transition`) when the durable record changed; `None` if not.
fn reconcile_cancellation_job(
    store: &JobStore,
    write: &AdmittedMutation<'_, JobsOwner>,
    job_id: &str,
    now_ms: i64,
    expected_version: u64,
) -> Result<Option<(MutationBatch, u64)>> {
    let mut job = read_job_in_wtx(write, job_id)?;
    let lease_expired = job
        .lease
        .as_ref()
        .is_none_or(|lease| lease.expires_at_ms <= now_ms);
    if !lease_expired || !cancellation_indexed(&job) {
        return Ok(None);
    }
    let checkpoint = job.state.checkpoint().cloned();
    job.state = JobState::Cancelled { checkpoint };
    job.lease = None;
    job.updated_at_ms = now_ms;
    let outcome = write_job_transition(store, write, &job, expected_version, None)?;
    Ok(Some(outcome))
}

/// Collect the durable job ids whose lease has expired by `now_ms`. Split out
/// of `reconcile_scheduler` (extract-method, cx/wD8) — same limit check, same
/// order as before.
fn collect_expired_lease_ids(
    wtx: &AdmittedMutation<'_, JobsOwner>,
    now_ms: i64,
) -> Result<Vec<String>> {
    collect_due_ids(wtx, JOB_LEASE_EXPIRY, now_ms)
}

/// The job ids in a `(timestamp, job_id)`-keyed scheduler index whose timestamp is at or
/// before `now_ms`, in key order. Refuses to return more than one reconciliation batch's
/// worth, so a corrupted or runaway index cannot make a sweep unbounded.
fn collect_due_ids(
    wtx: &AdmittedMutation<'_, JobsOwner>,
    index: TableDefinition<'static, (i64, &str), ()>,
    now_ms: i64,
) -> Result<Vec<String>> {
    let table = wtx.open_read_table(index).map_err(redb_err)?;
    let mut ids = Vec::new();
    for row in table
        .range_inclusive((i64::MIN, ""), (now_ms, "\u{10ffff}"))
        .map_err(redb_err)?
    {
        let (key, _) = row.map_err(redb_err)?;
        if ids.len() >= MAX_SCHEDULER_RECONCILE_ITEMS {
            return Err(codec_err("scheduler reconciliation exceeds limits"));
        }
        ids.push(key.value().1.to_string());
    }
    Ok(ids)
}

/// Reconcile one lease-expired job. Split out of `reconcile_scheduler`
/// (extract-method, cx/wD8) — same terms, same order as before. Returns the
/// batch and the version now authoritative after this call (see
/// `write_job_transition`) when the durable record changed; `None` if not.
fn reconcile_expired_lease_job(
    store: &JobStore,
    write: &AdmittedMutation<'_, JobsOwner>,
    job_id: &str,
    now_ms: i64,
    expected_version: u64,
) -> Result<Option<(MutationBatch, u64)>> {
    let mut job = read_job_in_wtx(write, job_id)?;
    if job
        .lease
        .as_ref()
        .is_none_or(|lease| lease.expires_at_ms > now_ms)
    {
        return Ok(None);
    }
    if job.cancel_requested && matches!(&job.state, JobState::Running { .. }) {
        let checkpoint = job.state.checkpoint().cloned();
        job.state = JobState::Cancelled { checkpoint };
    } else if job
        .policy
        .deadline_unix_ms
        .is_some_and(|deadline| now_ms >= deadline)
        && matches!(&job.state, JobState::Submitted | JobState::Running { .. })
    {
        let checkpoint = job.state.checkpoint().cloned();
        job.state = JobState::Failed {
            reason: "deadline_exceeded".to_string(),
            checkpoint,
        };
    } else if matches!(&job.state, JobState::Running { .. })
        && job.retry.attempts_made >= job.retry.max_attempts.max(1)
    {
        let checkpoint = job.state.checkpoint().cloned();
        job.state = JobState::Failed {
            reason: "lease_expired".to_string(),
            checkpoint,
        };
    }
    // A non-terminal expired owner is durably detached before it enters the
    // ready queue. Fencing still advances only when the next worker leases it.
    job.lease = None;
    job.updated_at_ms = now_ms;
    let outcome = write_job_transition(store, write, &job, expected_version, None)?;
    Ok(Some(outcome))
}

/// Collect the durable job ids whose deadline has passed by `now_ms`. Split
/// out of `reconcile_scheduler` (extract-method, cx/wD8) — same limit check,
/// same order as before.
fn collect_deadline_ids(wtx: &AdmittedMutation<'_, JobsOwner>, now_ms: i64) -> Result<Vec<String>> {
    collect_due_ids(wtx, JOB_DEADLINE, now_ms)
}

/// Reconcile one deadline-exceeded job. Split out of `reconcile_scheduler`
/// (extract-method, cx/wD8) — same terms, same order as before. Returns the
/// batch and the version now authoritative after this call (see
/// `write_job_transition`) when the durable record changed; `None` if not.
fn reconcile_deadline_job(
    store: &JobStore,
    write: &AdmittedMutation<'_, JobsOwner>,
    job_id: &str,
    now_ms: i64,
    expected_version: u64,
) -> Result<Option<(MutationBatch, u64)>> {
    let mut job = read_job_in_wtx(write, job_id)?;
    let lease_live = job
        .lease
        .as_ref()
        .is_some_and(|lease| lease.expires_at_ms > now_ms);
    if lease_live
        || !deadline_indexed(&job)
        || job
            .policy
            .deadline_unix_ms
            .is_none_or(|deadline| deadline > now_ms)
    {
        return Ok(None);
    }
    let checkpoint = job.state.checkpoint().cloned();
    job.state = JobState::Failed {
        reason: "deadline_exceeded".to_string(),
        checkpoint,
    };
    job.lease = None;
    job.updated_at_ms = now_ms;
    let outcome = write_job_transition(store, write, &job, expected_version, None)?;
    Ok(Some(outcome))
}

/// Reconcile every due scheduler condition inside `write`'s ONE open
/// transaction. `expected_version` is this store's live authoritative version
/// as of BEFORE `write` opened (see `claim_next`); each reconciled job that
/// actually transitions advances it locally rather than re-reading
/// `JobStore::live_version` (which would open a fresh read snapshot that
/// cannot observe this transaction's own not-yet-committed `finish()` writes).
/// Returns the LAST batch applied (any one is representative for the final
/// `MutationKernel::commit`, since `binding_for_write` only checks scope
/// identity, which is identical across every call here) and the version now
/// authoritative after every reconciled transition.
fn reconcile_scheduler(
    store: &JobStore,
    write: &AdmittedMutation<'_, JobsOwner>,
    now_ms: i64,
    mut expected_version: u64,
) -> Result<(Option<MutationBatch>, u64)> {
    let mut last_batch = None;
    for job_id in collect_cancellation_ids(write)? {
        if let Some((batch, next)) =
            reconcile_cancellation_job(store, write, &job_id, now_ms, expected_version)?
        {
            last_batch = Some(batch);
            expected_version = next;
        }
    }
    for job_id in collect_expired_lease_ids(write, now_ms)? {
        if let Some((batch, next)) =
            reconcile_expired_lease_job(store, write, &job_id, now_ms, expected_version)?
        {
            last_batch = Some(batch);
            expected_version = next;
        }
    }
    for job_id in collect_deadline_ids(write, now_ms)? {
        if let Some((batch, next)) =
            reconcile_deadline_job(store, write, &job_id, now_ms, expected_version)?
        {
            last_batch = Some(batch);
            expected_version = next;
        }
    }
    Ok((last_batch, expected_version))
}

fn decode_job_result(record: &MutationBatchRecord) -> Result<AnalyticsJob> {
    let bytes = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| codec_err("committed analytics-job batch has no result"))?;
    decode_job(bytes)
}

fn read_job_in_wtx(wtx: &AdmittedMutation<'_, JobsOwner>, job_id: &str) -> Result<AnalyticsJob> {
    let table = wtx.open_read_table(JOBS).map_err(redb_err)?;
    let row = table
        .get(job_id)
        .map_err(redb_err)?
        .ok_or_else(|| JobError::NotFound(job_id.to_string()))?;
    decode_job(row.value())
}

/// Whether a staged result's reproducibility manifest matches the job it was
/// computed for. Split out of `stage_result_fenced` (extract-method, cx/wD8).
fn lineage_matches_job(
    lineage: &crate::result::ReproducibilityManifest,
    job: &AnalyticsJob,
) -> bool {
    lineage.input_dataset_ref == job.input_snapshot.dataset_ref
        && lineage.input_content_digest == job.input_snapshot.content_digest
        && lineage.input_snapshot_version == job.input_snapshot.version
        && lineage.algorithm_ref == format!("{}:{}", job.algo.family, job.algo.algorithm)
        && lineage.params_digest == job.algo.params_digest
        && lineage.implementation_version == job.algo.code_version
        && lineage.environment_version == job.algo.env_version
        && lineage.policy_fingerprint == job.policy.policy_fingerprint
}

fn invalid_transition(job: &AnalyticsJob, reason: &'static str) -> JobError {
    JobError::InvalidTransition {
        job_id: job.job_id.clone(),
        state: job.state.label(),
        reason,
    }
}

fn require_lease(job: &AnalyticsJob, worker_ref: &str, epoch: u64, now_ms: i64) -> Result<()> {
    require_lease_ownership(job, worker_ref, epoch, now_ms)?;
    if job.cancel_requested {
        return Err(invalid_transition(job, "job cancellation was requested"));
    }
    Ok(())
}

fn require_lease_ownership(
    job: &AnalyticsJob,
    worker_ref: &str,
    epoch: u64,
    now_ms: i64,
) -> Result<()> {
    let lease = job
        .lease
        .as_ref()
        .ok_or_else(|| invalid_transition(job, "worker mutation requires a lease"))?;
    if lease.worker_ref != worker_ref || lease.epoch != epoch {
        return Err(invalid_transition(job, "stale worker fencing token"));
    }
    if lease.expires_at_ms < now_ms {
        return Err(invalid_transition(job, "worker lease expired"));
    }
    Ok(())
}

/// Persist a job image, optional result dataset, transition batch/status and
/// outbox in one transaction. A replay is safe only because the first commit
/// already included the same result row.
///
/// `expected_version` must already be the fixed `analytics-jobs` scope's
/// authoritative version AS OF `write`'s current state (see
/// `reconcile_scheduler`'s doc comment for why callers track it locally
/// instead of re-reading `JobStore::live_version` mid-transaction).
/// Returns the batch that was begun (Replay or Apply) and the version that is
/// now authoritative after this call: unchanged on a Replay (nothing was
/// written), `expected_version + 1` on an Apply (mirrors exactly what
/// `MutationKernel::finish` -> `CommittedVersion::checked_native` computes
/// as `target`). Never calls `MutationKernel::commit` itself -- a single
/// `write` may carry several transitions (see `reconcile_scheduler` +
/// `claim_next`), so only the top-level caller, once it knows no further
/// transition is coming, commits.
fn write_job_transition(
    store: &JobStore,
    write: &AdmittedMutation<'_, JobsOwner>,
    job: &AnalyticsJob,
    expected_version: u64,
    result: Option<(&str, &[u8])>,
) -> Result<(MutationBatch, u64)> {
    let batch = internal_job_batch(
        job,
        store.owner.identity(),
        store.owner.principal(),
        expected_version,
    )?;
    match write.begin(&batch).map_err(redb_err)? {
        Begin::Replay(record) => {
            let replayed = decode_job_result(&record)?;
            if replayed != *job {
                return Err(codec_err(
                    "job transition replay does not match durable state",
                ));
            }
            Ok((batch, expected_version))
        }
        Begin::Apply { source_version } => {
            let job_bytes = encode_job(job)?;
            let owner_write = write.owner_rows(&store.owner, &batch).map_err(redb_err)?;
            let staged = (|| -> Result<()> {
                persist_job_image(&owner_write, job, job_bytes.as_slice())?;
                if let Some((dataset_ref, result_bytes)) = result {
                    if !valid_identifier(dataset_ref) || result_bytes.len() > MAX_JOB_RESULT_BYTES {
                        return Err(codec_err("typed result exceeds the storage limit"));
                    }
                    owner_write
                        .open_table(RESULTS)
                        .map_err(redb_err)?
                        .insert(dataset_ref, result_bytes)
                        .map_err(redb_err)?;
                }
                Ok(())
            })();
            // Always close the owner capability: dropping it unfinished poisons
            // the write and would mask the staging error.
            owner_write.finish_owner().map_err(redb_err)?;
            staged?;
            store
                .mutations
                .finish(
                    write,
                    &batch,
                    Some(job_bytes),
                    job.updated_at_ms.max(0) as u64,
                    source_version,
                )
                .map_err(redb_err)?;
            let next = expected_version
                .checked_add(1)
                .ok_or_else(|| codec_err("analytics-job mutation-scope version overflow"))?;
            Ok((batch, next))
        }
    }
}

/// Fixed native mutation-domain scope for every `JOBS` row transition (submit,
/// cancel, executor-driven state changes): one totally-ordered mutation log for
/// the whole `jobs.redb`, mirroring `eg-statechart`'s fixed `INSTANCE_MUTATION_*`
/// scope. `ANALYTICS_JOB_SCOPE_INCARNATION` is a fixed literal rather than one
/// derived from the physical file: this scope has exactly one lifecycle
/// generation for the life of a `jobs.redb`, so a versioned constant is the
/// correct opaque generation id here (compare `crates/eg-types/src/
/// mutation_batch.rs`'s own `"incarnation:bootstrap:1"` fixtures for a fixed
/// scope) -- it is not a placeholder standing in for an unknown value.
const ANALYTICS_JOB_SCOPE_TENANT: &str = "native";
const ANALYTICS_JOB_SCOPE_RESOURCE: &str = "analytics-jobs";
const ANALYTICS_JOB_SCOPE_INCARNATION: &str = "incarnation:eg-jobs:analytics-jobs:1";

/// Build the fixed [`MutationScopeIdentity`] for this store's native
/// `analytics-jobs` mutation scope (see the `ANALYTICS_JOB_SCOPE_*` constants
/// above). A plain function rather than a cached constant: `native()` derives
/// a digest, which is not `const`-evaluable, and every constructor here is
/// fallible by construction, so failures are propagated rather than
/// `.unwrap()`/`.expect()`'d away even though the fixed literals above are
/// known-valid by inspection (mirrors `rbac_persist.rs`'s
/// `native_security_control_identity`).
/// The one analytics-job scope identity.
///
/// Exported deliberately. The integration test previously redeclared the three
/// scope constants verbatim because they were private, so production and test
/// could drift apart silently while both compiled. Exporting the IDENTITY rather
/// than the constants makes that drift unrepresentable.
pub fn analytics_job_scope_identity() -> Result<MutationScopeIdentity> {
    MutationScopeIdentity::fixed_native(
        ANALYTICS_JOB_SCOPE_TENANT,
        DurabilityDomain::AnalyticsJob,
        ANALYTICS_JOB_SCOPE_RESOURCE,
        ANALYTICS_JOB_SCOPE_INCARNATION,
    )
    .map_err(codec_err)
}

/// Internal executor transitions (running/checkpoint/succeeded/failed/cancelled)
/// are durable mutations too, even though they are not separate wire requests.
/// Give each resulting state image a deterministic digest-only MutationBatch so
/// every `JOBS` row transition has the same atomic status/fence/idempotency/outbox
/// evidence as submit/cancel/resume.
///
/// `expected_version` is the caller-supplied live scope version this batch's
/// `VersionExpectation::Native` must equal for `AdmittedMutation::begin`'s
/// OCC check to accept it -- see `JobStore::put_raw` and `write_job_transition`
/// for how callers obtain it.
/// Operator-facing identity of the ONE physical `jobs.redb` owner file. Names the
/// physical authority boundary the storage kernel stamps into the owner manifest,
/// independent of the logical serving scope.
const JOBS_PHYSICAL_STORE: &str = "eg-jobs:analytics-jobs";

/// The batch for one store-level maintenance mutation (RF-RULING-005).
///
/// The scheduler index rebuild, the two idempotency ledgers and the intent
/// registry carry no caller identity and are not job transitions, so they are
/// outside operation-replay semantics -- but they are still full ledgered,
/// fenced, version-bumping mutations, because an un-ledgered owner write would be
/// a second physical authority. `batch_id` is `(kind, scope version)`: exactly one
/// batch commits per version, so it is unique per attempt and stable across a
/// crash-retry of that attempt.
fn maintenance_batch(
    kind: &str,
    identity: &MutationScopeIdentity,
    principal: &str,
    expected_version: u64,
) -> Result<MutationBatch> {
    let batch_id = format!("analytics-job-{kind}:v{expected_version}");
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: 0,
            principal: principal.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // A maintenance mutation claims no capability: a plain
            // `Native`-versioned write, not the reserved-system `Unversioned`
            // path. Empty is the true fact here, not a placeholder.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: identity.clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        version_expectation: VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: DurabilityDomain::AnalyticsJob,
            method: Method::ApplyMutation {
                event_type: format!("analytics_job_{kind}"),
                query: batch_id,
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate().map_err(codec_err)?;
    Ok(batch)
}

fn internal_job_batch(
    job: &AnalyticsJob,
    identity: &MutationScopeIdentity,
    principal: &str,
    expected_version: u64,
) -> Result<MutationBatch> {
    use sha2::{Digest, Sha256};
    let encoded = encode_job(job)?;
    let digest = hex::encode(Sha256::digest(&encoded));
    // RF-RULING-004: the batch actor is the principal the storage kernel
    // authenticated this store's serving scope for -- one physical file serves
    // one bound scope and one principal, and the mutation kernel refuses any
    // batch naming another. Executor-side attribution is not lost: the
    // server-hashed worker slot that owns (or most recently owned) the fencing
    // epoch travels on the outbox row below, and `lease`/`last_worker_ref` are
    // fields of the persisted job image itself. Caller identity is already
    // pseudonymized; never fall back to a raw worker label.
    let principal = principal.to_string();
    let transition_actor = job
        .lease
        .as_ref()
        .map(|lease| lease.worker_ref.as_str())
        .filter(|value| !value.is_empty())
        .or_else(|| (!job.last_worker_ref.is_empty()).then_some(job.last_worker_ref.as_str()))
        .unwrap_or(job.policy.actor.as_str());
    let actor_digest = format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(transition_actor.as_bytes()))
    );
    let batch_id = format!("job-transition:{digest}");
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Job,
        domain: DurabilityDomain::AnalyticsJob,
        method: Method::ApplyMutation {
            event_type: "analytics_job_transition".to_string(),
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
            // No admission boundary verifies a capability for this internal,
            // executor-driven transition -- it is a plain `Native`-versioned
            // mutation, not the reserved-system `Unversioned` path, so it
            // legitimately needs none. Empty is the true fact here, not a
            // default standing in for an unknown value.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: identity.clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        // The scope's live authoritative version, supplied by the caller (see
        // this function's doc comment) -- `MutationKernel::finish` requires
        // `VersionExpectation::Native` to equal the scope's CURRENT
        // authoritative version (see
        // `crates/eg-mutation-store/src/store/apply.rs::committed_version`).
        // `Unversioned` is not legal for this scope (`validate_version_expectation`
        // gates it to `ControlPlane`/`Lifecycle` domains under the reserved-system
        // tenant, and this is `AnalyticsJob`/"native").
        version_expectation: VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation.clone()],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.analytics-job.transitioned".to_string(),
            key: batch_id,
            payload: rmp_serde::to_vec_named(&operation).map_err(codec_err)?,
            headers: std::collections::BTreeMap::from([("actor".to_string(), actor_digest)]),
        }],
        created_at_ms: job.updated_at_ms.max(0) as u64,
    };
    batch.validate().map_err(codec_err)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_scope_grant::{open_dev_store, open_dev_store_in_dir};
    use crate::model::{AlgoVersion, InputSnapshotHandle};
    use redb::ReadableTableMetadata;

    #[test]
    fn stored_job_decoder_rejects_declared_allocation_bomb() {
        let array32_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_stored::<AnalyticsJob>(&array32_bomb).is_err());
    }

    fn empty_result(job: &AnalyticsJob) -> TypedJobResult {
        let schema = [
            "id",
            "kind",
            "confidence",
            "evidence_refs",
            "source_refs",
            "proof_ids",
            "contradiction_ids",
        ]
        .into_iter()
        .map(|name| crate::result::ResultColumn {
            name: name.to_string(),
            logical_type: "json".to_string(),
            nullable: false,
        })
        .collect();
        TypedJobResult::new(
            schema,
            Vec::new(),
            vec![job.input_snapshot.dataset_ref.clone()],
            Vec::new(),
            None,
            None,
            crate::result::ReproducibilityManifest {
                input_dataset_ref: job.input_snapshot.dataset_ref.clone(),
                input_content_digest: job.input_snapshot.content_digest.clone(),
                input_snapshot_version: job.input_snapshot.version,
                algorithm_ref: format!("{}:{}", job.algo.family, job.algo.algorithm),
                params_digest: job.algo.params_digest.clone(),
                implementation_version: job.algo.code_version.clone(),
                environment_version: job.algo.env_version.clone(),
                policy_fingerprint: job.policy.policy_fingerprint.clone(),
            },
        )
        .unwrap()
    }

    fn spec(graph: &str, version: u64) -> SubmitSpec {
        SubmitSpec {
            input_snapshot: InputSnapshotHandle::new(graph, version).with_dataset(
                "eg:job_input:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ),
            policy: JobPolicy {
                tenant: "acme".into(),
                actor: "agent:planner".into(),
                purpose: "quarterly-mining".into(),
                priority: 0,
                quota_cpu_ms: None,
                deadline_unix_ms: None,
                ..JobPolicy::default()
            },
            algo: AlgoVersion {
                family: "mining.association".into(),
                algorithm: "fpgrowth".into(),
                params_digest: "deadbeef".into(),
                code_version: "test".into(),
                env_version: "test".into(),
            },
            input_payload: None,
            max_attempts: 3,
            backoff_ms: 10,
        }
    }

    /// `expected_version` must be the TARGET store's live `analytics-jobs`
    /// scope version at the moment the returned batch will be applied (the
    /// batch's `VersionExpectation::Native` is baked in here, exactly like a
    /// production admission boundary would compute it via
    /// `JobStore::mutation_version`) -- NOT necessarily the version of
    /// whichever store `job` happened to come from, since a batch built once
    /// (e.g. by a Raft leader) is applied identically to every replica.
    fn request_batch(job: &AnalyticsJob, action: &str, expected_version: u64) -> MutationBatch {
        static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
        let sequence = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        let identity = analytics_job_scope_identity().unwrap();
        let mut batch = internal_job_batch(
            job,
            &identity,
            crate::dev_scope_grant::DEV_PRINCIPAL,
            expected_version,
        )
        .unwrap();
        let request_id = format!("job-request:{action}:{sequence}");
        batch.batch_id = request_id.clone();
        batch.idempotency_key = request_id.clone();
        batch.context.request_id = sequence;
        batch.created_at_ms = job.updated_at_ms.max(0) as u64;
        let operation = MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::AnalyticsJob,
            method: Method::ApplyMutation {
                event_type: format!("analytics_job_{action}"),
                query: format!("request:{sequence}"),
            },
        };
        batch.operations = vec![operation.clone()];
        batch.outbox[0].key = request_id;
        batch.outbox[0].payload = rmp_serde::to_vec_named(&operation).unwrap();
        batch.validate().unwrap();
        batch
    }

    #[test]
    fn submit_starts_in_submitted_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        assert_eq!(job.state, JobState::Submitted);
        assert!(job.job_id.starts_with("job-"));
    }

    #[test]
    fn replicated_submission_and_claim_converge_across_independent_projections() {
        let seed_dir = tempfile::tempdir().unwrap();
        let seed_store = open_dev_store_in_dir(seed_dir.path()).unwrap();
        let seed = seed_store.submit(spec("g1", 9)).unwrap();
        // `left`/`right` below are both brand-new stores. Opening one costs
        // exactly one committed batch: under RF-RULING-005 the scheduler-index
        // bootstrap is a ledgered maintenance mutation, not an un-ledgered owner
        // write, and it runs once per freshly created file -- so a fresh store is
        // at version 1, not 0, and every replica is at the same 1. The batch is
        // built once and applied identically to both, mirroring a Raft leader
        // compiling one batch for every replica to apply.
        let batch = request_batch(&seed, "replicated-submit", 1);
        let left_dir = tempfile::tempdir().unwrap();
        let right_dir = tempfile::tempdir().unwrap();
        let left = open_dev_store_in_dir(left_dir.path()).unwrap();
        let right = open_dev_store_in_dir(right_dir.path()).unwrap();

        let (left_job, _) = left.submit_batch(spec("g1", 9), &batch, 10_000).unwrap();
        let (right_job, _) = right.submit_batch(spec("g1", 9), &batch, 10_000).unwrap();
        assert_eq!(left_job, right_job);

        let left_claim = left
            .claim_next(
                "worker:replica",
                &[],
                10_001,
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        let right_claim = right
            .claim_next(
                "worker:replica",
                &[],
                10_001,
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(left_claim.job, right_claim.job);
        assert_eq!(left_claim.lease, right_claim.lease);
    }

    #[test]
    fn full_lifecycle_submit_running_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let _job = store.submit(spec("g1", 7)).unwrap();

        let claim = store
            .claim_next(
                "worker:test",
                &[],
                now_ms(),
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        let job = claim.job;
        assert!(matches!(job.state, JobState::Running { .. }));

        let job = store
            .checkpoint_fenced(
                &job.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                Checkpoint {
                    progress: 0.5,
                    stage: "half-way".to_string(),
                    state_blob: Some(vec![1, 2, 3]),
                    updated_at_ms: now_ms(),
                },
                now_ms(),
            )
            .unwrap();
        match &job.state {
            JobState::Running { checkpoint } => {
                assert_eq!(checkpoint.progress, 0.5);
                assert_eq!(checkpoint.stage, "half-way");
                assert_eq!(checkpoint.state_blob, Some(vec![1, 2, 3]));
            }
            other => panic!("expected Running, got {other:?}"),
        }

        let result_ref = job.result_ref();
        let staged = store
            .stage_result_fenced(
                &job.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                empty_result(&job),
                now_ms(),
            )
            .unwrap();
        assert!(matches!(staged.state, JobState::Publishing { .. }));
        let job = store
            .complete_publication_fenced(
                &job.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                now_ms(),
            )
            .unwrap();
        match &job.state {
            JobState::Succeeded {
                result_ref: r,
                checkpoint,
            } => {
                assert_eq!(*r, result_ref);
                assert_eq!(checkpoint.progress, 1.0);
            }
            other => panic!("expected Succeeded, got {other:?}"),
        }
    }

    #[test]
    fn prepared_publication_finalizes_after_lease_expiry_but_not_new_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let submitted = store.submit(spec("g1", 11)).unwrap();
        let began = now_ms();
        let first = store
            .claim_next("worker:first", &[], began, 1, TenantJobQuota::default())
            .unwrap()
            .unwrap();
        let staged = store
            .stage_result_fenced(
                &submitted.job_id,
                &first.lease.worker_ref,
                first.lease.epoch,
                empty_result(&first.job),
                began,
            )
            .unwrap();
        let result_ref = staged.result_ref();
        let completed = store
            .complete_publication_prepared(
                &submitted.job_id,
                &first.lease.worker_ref,
                first.lease.epoch,
                &result_ref,
                began + 10,
            )
            .unwrap();
        assert!(matches!(completed.state, JobState::Succeeded { .. }));
        assert!(store
            .complete_publication_prepared(
                &submitted.job_id,
                &first.lease.worker_ref,
                first.lease.epoch,
                &result_ref,
                began + 11,
            )
            .is_ok());

        let submitted = store.submit(spec("g2", 12)).unwrap();
        let first = store
            .claim_next("worker:first", &[], began, 1, TenantJobQuota::default())
            .unwrap()
            .unwrap();
        let staged = store
            .stage_result_fenced(
                &submitted.job_id,
                &first.lease.worker_ref,
                first.lease.epoch,
                empty_result(&first.job),
                began,
            )
            .unwrap();
        let result_ref = staged.result_ref();
        let second = store
            .claim_next(
                "worker:second",
                &[],
                began + 2,
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        assert!(second.lease.epoch > first.lease.epoch);
        assert!(store
            .complete_publication_prepared(
                &submitted.job_id,
                &first.lease.worker_ref,
                first.lease.epoch,
                &result_ref,
                began + 3,
            )
            .is_err());
    }

    #[test]
    fn cancel_mid_run_stops_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let submitted = store.submit(spec("g1", 1)).unwrap();
        let now = now_ms();
        let claim = store
            .claim_next(
                "worker:cancel-mid-run",
                &[],
                now,
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        store
            .checkpoint_fenced(
                &submitted.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                Checkpoint {
                    progress: 0.3,
                    stage: "chunk-1".to_string(),
                    state_blob: None,
                    updated_at_ms: now,
                },
                now,
            )
            .unwrap();

        let current = store.get(&submitted.job_id).unwrap();
        let expected = store.live_version().unwrap();
        let (job, replayed) = store
            .request_cancel_batch(
                &submitted.job_id,
                &request_batch(&current, "cancel", expected),
                now as u64,
            )
            .unwrap();
        assert!(!replayed);
        assert!(job.cancel_requested);
        assert!(matches!(job.state, JobState::Running { .. }));

        let job = store
            .mark_cancelled_fenced(
                &submitted.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                now,
            )
            .unwrap();
        assert!(matches!(job.state, JobState::Cancelled { .. }));

        assert!(store
            .checkpoint_fenced(
                &job.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                Checkpoint::default(),
                now,
            )
            .is_err());
        let expected = store.live_version().unwrap();
        assert!(store
            .request_cancel_batch(
                &job.job_id,
                &request_batch(&job, "cancel-terminal", expected),
                now as u64,
            )
            .is_err());
        assert!(store
            .resume_batch(
                &job.job_id,
                &request_batch(&job, "resume-cancelled", expected),
                now as u64,
            )
            .is_err());
    }

    #[test]
    fn fenced_worker_can_acknowledge_a_requested_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let submitted = store.submit(spec("g1", 1)).unwrap();
        let claim = store
            .claim_next(
                "worker:cancel",
                &[],
                now_ms(),
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        let expected = store.live_version().unwrap();
        let batch = request_batch(&claim.job, "cancel-fenced", expected);
        store
            .request_cancel_batch(&submitted.job_id, &batch, now_ms() as u64)
            .unwrap();
        let cancelled = store
            .mark_cancelled_fenced(
                &submitted.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                now_ms(),
            )
            .unwrap();
        assert!(matches!(cancelled.state, JobState::Cancelled { .. }));
    }

    #[test]
    fn cancel_before_start_is_immediate() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        let expected = store.live_version().unwrap();
        let batch = request_batch(&job, "cancel-before-start", expected);
        let (job, replayed) = store
            .request_cancel_batch(&job.job_id, &batch, now_ms() as u64)
            .unwrap();
        assert!(!replayed);
        assert!(matches!(job.state, JobState::Cancelled { .. }));
    }

    #[test]
    fn mark_cancelled_requires_a_prior_request() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let submitted = store.submit(spec("g1", 1)).unwrap();
        let now = now_ms();
        let claim = store
            .claim_next(
                "worker:cancel-without-request",
                &[],
                now,
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        assert!(store
            .mark_cancelled_fenced(
                &submitted.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                now,
            )
            .is_err());
    }

    #[test]
    fn resume_from_checkpoint_after_simulated_crash() {
        let dir = tempfile::tempdir().unwrap();
        let started_at = now_ms();
        let job_id = {
            let store = open_dev_store_in_dir(dir.path()).unwrap();
            let submitted = store.submit(spec("g1", 1)).unwrap();
            let claim = store
                .claim_next(
                    "worker:before-crash",
                    &[],
                    started_at,
                    1,
                    TenantJobQuota::default(),
                )
                .unwrap()
                .unwrap();
            store
                .checkpoint_fenced(
                    &submitted.job_id,
                    &claim.lease.worker_ref,
                    claim.lease.epoch,
                    Checkpoint {
                        progress: 0.4,
                        stage: "chunk-2".to_string(),
                        state_blob: Some(vec![9, 9]),
                        updated_at_ms: started_at,
                    },
                    started_at,
                )
                .unwrap();
            submitted.job_id
            // `store` (and its redb `Database` handle) drops here — simulates the
            // process exiting mid-run with the job durably left in `Running`.
        };

        // Reopen at the SAME path (CONCEPT:INT-P2-1 restart-durability): the
        // orphaned `Running` job + its checkpoint must have survived.
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let job = store.get(&job_id).unwrap();
        match &job.state {
            JobState::Running { checkpoint } => {
                assert_eq!(checkpoint.progress, 0.4);
                assert_eq!(checkpoint.stage, "chunk-2");
            }
            other => panic!("expected orphaned Running, got {other:?}"),
        }

        let claim = store
            .claim_next(
                "worker:resume",
                &[],
                started_at + 2,
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        match &claim.job.state {
            JobState::Running { checkpoint } => assert_eq!(checkpoint.progress, 0.4),
            other => panic!("expected reassigned Running job, got {other:?}"),
        }
        let _job = store
            .stage_result_fenced(
                &job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                empty_result(&claim.job),
                started_at + 2,
            )
            .unwrap();
        let job = store
            .complete_publication_fenced(
                &job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                started_at + 2,
            )
            .unwrap();
        assert!(matches!(job.state, JobState::Succeeded { .. }));
    }

    #[test]
    fn deterministic_result_ref_same_lineage_same_ref() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        // Two DIFFERENT jobs (distinct job_id), IDENTICAL input snapshot + algo.
        let job_a = store.submit(spec("g1", 42)).unwrap();
        let job_b = store.submit(spec("g1", 42)).unwrap();
        assert_ne!(job_a.job_id, job_b.job_id);
        assert_eq!(job_a.result_ref(), job_b.result_ref());

        // A different snapshot version yields a different result_ref.
        let job_c = store.submit(spec("g1", 43)).unwrap();
        assert_ne!(job_a.result_ref(), job_c.result_ref());
    }

    #[test]
    fn expired_lease_reassignment_fences_the_old_worker() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        let now = now_ms();
        let first = store
            .claim_next("worker:first", &[], now, 10, TenantJobQuota::default())
            .unwrap()
            .unwrap();
        let second = store
            .claim_next(
                "worker:second",
                &[],
                now + 11,
                10,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        assert!(second.lease.epoch > first.lease.epoch);
        assert!(store
            .checkpoint_fenced(
                &job.job_id,
                &first.lease.worker_ref,
                first.lease.epoch,
                Checkpoint::default(),
                now + 12,
            )
            .is_err());
    }

    #[test]
    fn scheduler_indexes_follow_authoritative_state_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let submitted = store.submit(spec("g1", 1)).unwrap();
        {
            let rtx = store.scoped_read().unwrap();
            assert_eq!(rtx.open_owner_table(JOB_READY).unwrap().len().unwrap(), 1);
            assert_eq!(
                rtx.open_owner_table(JOB_READY_BY_CAPABILITY)
                    .unwrap()
                    .len()
                    .unwrap(),
                1
            );
            assert_eq!(
                rtx.open_owner_table(JOB_META)
                    .unwrap()
                    .get(META_MAX_JOB_SEQUENCE)
                    .unwrap()
                    .unwrap()
                    .value(),
                job_sequence(&submitted.job_id).unwrap()
            );
        }

        let now = now_ms();
        let claim = store
            .claim_next("worker:index", &[], now, 30_000, TenantJobQuota::default())
            .unwrap()
            .unwrap();
        {
            let rtx = store.scoped_read().unwrap();
            assert_eq!(rtx.open_owner_table(JOB_READY).unwrap().len().unwrap(), 0);
            assert_eq!(
                rtx.open_owner_table(JOB_READY_BY_CAPABILITY)
                    .unwrap()
                    .len()
                    .unwrap(),
                0
            );
            assert_eq!(
                rtx.open_owner_table(JOB_LEASE_EXPIRY)
                    .unwrap()
                    .len()
                    .unwrap(),
                1
            );
            let active = rtx.open_owner_table(JOB_TENANT_TOTALS).unwrap();
            assert_eq!(active.len().unwrap(), 1);
            let (key, value) = active.iter().unwrap().next().unwrap().unwrap();
            assert_ne!(key.value(), "acme", "tenant index keys stay opaque");
            assert_eq!(value.value(), (1, 0));
            assert_eq!(
                rtx.open_owner_table(JOB_LEASE_BY_WORKER)
                    .unwrap()
                    .get("worker:index")
                    .unwrap()
                    .unwrap()
                    .value(),
                submitted.job_id
            );
        }

        let expected = store.live_version().unwrap();
        let cancel = request_batch(&claim.job, "cancel-indexed", expected);
        store
            .request_cancel_batch(&submitted.job_id, &cancel, (now + 1) as u64)
            .unwrap();
        store
            .mark_cancelled_fenced(
                &submitted.job_id,
                &claim.lease.worker_ref,
                claim.lease.epoch,
                now + 1,
            )
            .unwrap();
        let rtx = store.scoped_read().unwrap();
        assert_eq!(rtx.open_owner_table(JOB_READY).unwrap().len().unwrap(), 0);
        assert_eq!(
            rtx.open_owner_table(JOB_READY_BY_CAPABILITY)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
        assert_eq!(
            rtx.open_owner_table(JOB_LEASE_EXPIRY)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
        assert_eq!(
            rtx.open_owner_table(JOB_TENANT_TOTALS)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
        assert_eq!(
            rtx.open_owner_table(JOB_LEASE_BY_WORKER)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
        assert_eq!(
            rtx.open_owner_table(JOB_CANCELLATION)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
    }

    #[test]
    fn ready_index_preserves_priority_then_fifo_selection() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let mut low = spec("g1", 1);
        low.policy.priority = -10;
        let low = store.submit(low).unwrap();
        let mut high = spec("g1", 2);
        high.policy.priority = 25;
        let high = store.submit(high).unwrap();

        let claim = store
            .claim_next(
                "worker:priority",
                &[],
                now_ms(),
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(claim.job.job_id, high.job_id);
        assert_ne!(claim.job.job_id, low.job_id);
    }

    #[test]
    fn capability_anchor_skips_unsatisfied_ready_jobs_without_raw_labels() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let mut gpu = spec("g1", 1);
        gpu.policy.priority = 100;
        gpu.policy.placement.required_capabilities = vec!["accelerator".to_string()];
        let gpu = store.submit(gpu).unwrap();
        let ordinary = store.submit(spec("g1", 2)).unwrap();

        let rtx = store.scoped_read().unwrap();
        let capability_rows = rtx.open_owner_table(JOB_READY_BY_CAPABILITY).unwrap();
        assert_eq!(capability_rows.len().unwrap(), 2);
        for row in capability_rows.iter().unwrap() {
            let (key, _) = row.unwrap();
            let (anchor, _, _, _) = key.value();
            assert_ne!(anchor, "accelerator", "capability labels must stay opaque");
        }
        drop(capability_rows);
        drop(rtx);

        let cpu_claim = store
            .claim_next(
                "worker:cpu",
                &[],
                now_ms(),
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(cpu_claim.job.job_id, ordinary.job_id);

        let accelerator_claim = store
            .claim_next(
                "worker:accelerator",
                &["accelerator".to_string()],
                now_ms(),
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(accelerator_claim.job.job_id, gpu.job_id);
    }

    #[test]
    fn reconciliation_commits_even_when_no_job_can_be_claimed() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let now = now_ms();
        let mut expired = spec("g1", 1);
        expired.policy.deadline_unix_ms = Some(now.saturating_sub(1));
        let expired = store.submit(expired).unwrap();

        assert!(store
            .claim_next(
                "worker:deadline",
                &[],
                now,
                30_000,
                TenantJobQuota::default(),
            )
            .unwrap()
            .is_none());
        assert!(matches!(
            store.get(&expired.job_id).unwrap().state,
            JobState::Failed { ref reason, .. } if reason == "deadline_exceeded"
        ));
    }

    #[test]
    fn opening_a_pre_index_store_backfills_once_and_restores_ready_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.redb");
        let first_id = {
            let store = open_dev_store(&path).unwrap();
            store.submit(spec("g1", 1)).unwrap().job_id
        };
        {
            // Regress the store to its pre-index shape, through the ONLY write
            // path this crate now has: an admitted maintenance mutation. The
            // next open must notice the missing schema marker and backfill.
            let store = open_dev_store(&path).unwrap();
            store
                .maintain("test-drop-indexes", |wtx| {
                    wtx.open_table(JOB_META)
                        .map_err(redb_err)?
                        .remove(META_INDEX_VERSION)
                        .map_err(redb_err)?;
                    wtx.open_table(JOB_READY)
                        .map_err(redb_err)?
                        .retain(|_, _| false)
                        .map_err(redb_err)?;
                    Ok(())
                })
                .unwrap();
        }

        let store = open_dev_store(&path).unwrap();
        let rtx = store.scoped_read().unwrap();
        assert_eq!(rtx.open_owner_table(JOB_READY).unwrap().len().unwrap(), 1);
        drop(rtx);
        let second = store.submit(spec("g1", 2)).unwrap();
        assert!(job_sequence(&second.job_id).unwrap() > job_sequence(&first_id).unwrap());
    }

    #[test]
    fn tenant_active_quota_is_enforced_inside_claim_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        store.submit(spec("g1", 1)).unwrap();
        store.submit(spec("g1", 2)).unwrap();
        let now = now_ms();
        let quota = TenantJobQuota {
            max_active: 1,
            max_reserved_cpu_ms: u64::MAX,
        };
        assert!(store
            .claim_next("worker:first", &[], now, 10_000, quota)
            .unwrap()
            .is_some());
        assert!(store
            .claim_next("worker:second", &[], now + 1, 10_000, quota)
            .unwrap()
            .is_none());
    }

    #[test]
    fn mark_result_committed_is_first_wins() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let job_a = store.submit(spec("g1", 1)).unwrap();
        let job_b = store.submit(spec("g1", 1)).unwrap();
        let result_ref = job_a.result_ref();
        assert_eq!(job_a.result_ref(), job_b.result_ref());

        assert!(store
            .mark_result_committed(&result_ref, &job_a.job_id)
            .unwrap());
        // Second caller (even a DIFFERENT job with identical lineage) is a no-op.
        assert!(!store
            .mark_result_committed(&result_ref, &job_b.job_id)
            .unwrap());
        assert_eq!(
            store.result_committed_by(&result_ref).unwrap(),
            Some(job_a.job_id)
        );
    }

    #[test]
    fn job_store_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let job_id = {
            let store = open_dev_store_in_dir(dir.path()).unwrap();
            store.submit(spec("g1", 1)).unwrap().job_id
        };
        // Reopen a FRESH `JobStore` at the same path — the durable record + the
        // monotonic id sequence must both survive.
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let job = store.get(&job_id).unwrap();
        assert_eq!(job.state, JobState::Submitted);

        let next = store.submit(spec("g1", 1)).unwrap();
        assert_ne!(
            next.job_id, job_id,
            "id sequence must not be reused after restart"
        );
    }

    #[test]
    fn get_missing_job_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let job_id = "job-0000000000000000";
        assert!(matches!(store.get(job_id), Err(JobError::NotFound(_))));
    }

    // ── Generalized idempotency ledger ───────────────────────────────────────

    #[test]
    fn claim_idempotency_is_first_wins() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        assert!(store.claim_idempotency("tick:a", "owner-1").unwrap());
        // A second, even different, owner does not steal an already-claimed key.
        assert!(!store.claim_idempotency("tick:a", "owner-2").unwrap());
        assert_eq!(
            store.idempotency_claimed_by("tick:a").unwrap(),
            Some("owner-1".to_string())
        );
        // An independent key is unaffected.
        assert!(store.claim_idempotency("tick:b", "owner-2").unwrap());
    }

    // ── `JobIntent` registry (daemon-consolidation design Phase 3) ──────────────

    fn intent_policy() -> JobPolicy {
        JobPolicy {
            tenant: "acme".into(),
            actor: "agent:planner".into(),
            purpose: "test".into(),
            priority: 0,
            quota_cpu_ms: None,
            deadline_unix_ms: None,
            ..JobPolicy::default()
        }
    }

    #[test]
    fn register_and_fetch_intent_round_trips() {
        use crate::intent::{JobIntent, Trigger};
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        let intent = JobIntent::new(
            "nightly-sweep",
            Trigger::Interval { secs: 3600 },
            intent_policy(),
        );
        let registered = store.register_intent(intent.clone()).unwrap();
        assert_eq!(registered.name, "nightly-sweep");
        let fetched = store.get_intent("nightly-sweep").unwrap();
        assert_eq!(fetched.trigger, Trigger::Interval { secs: 3600 });
        assert_eq!(fetched.last_run_ms, None);
    }

    #[test]
    fn re_registering_an_intent_preserves_last_run_history() {
        use crate::intent::{JobIntent, Trigger};
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        store
            .register_intent(JobIntent::new(
                "x",
                Trigger::Interval { secs: 10 },
                intent_policy(),
            ))
            .unwrap();
        assert!(store.record_intent_tick("x", 1_000).unwrap());
        assert_eq!(store.get_intent("x").unwrap().last_run_ms, Some(1_000));

        // Redeclaring the SAME intent (e.g. on restart) must not reset the clock.
        store
            .register_intent(JobIntent::new(
                "x",
                Trigger::Interval { secs: 10 },
                intent_policy(),
            ))
            .unwrap();
        assert_eq!(store.get_intent("x").unwrap().last_run_ms, Some(1_000));
    }

    #[test]
    fn due_intents_only_returns_enabled_due_ones() {
        use crate::intent::{JobIntent, Trigger};
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        store
            .register_intent(JobIntent::new(
                "never-run-yet",
                Trigger::Interval { secs: 60 },
                intent_policy(),
            ))
            .unwrap();
        let mut disabled = JobIntent::new(
            "disabled-one",
            Trigger::Interval { secs: 1 },
            intent_policy(),
        );
        disabled.enabled = false;
        store.register_intent(disabled).unwrap();

        let due = store.due_intents(10_000).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].name, "never-run-yet");
    }

    #[test]
    fn record_intent_tick_is_single_flight_within_a_window() {
        use crate::intent::{JobIntent, Trigger};
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();
        store
            .register_intent(JobIntent::new(
                "sweep",
                Trigger::Interval { secs: 60 },
                intent_policy(),
            ))
            .unwrap();

        // First evaluator in this 60s window wins...
        assert!(store.record_intent_tick("sweep", 1_000).unwrap());
        // ...a second evaluator re-checking the SAME window is a no-op (coalesce).
        assert!(!store.record_intent_tick("sweep", 30_000).unwrap());
        // A later window claims cleanly again.
        assert!(store.record_intent_tick("sweep", 61_000).unwrap());
    }

    #[test]
    fn cold_offload_proof_of_concept_registers_disabled_and_can_be_ticked() {
        use crate::intent::cold_offload_intent;
        let dir = tempfile::tempdir().unwrap();
        let store = open_dev_store_in_dir(dir.path()).unwrap();

        // The proof-of-concept constructor mirrors the live sweep's exact cadence
        // knob (EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS) but registers DISABLED, so simply
        // declaring the intent is never itself a behavior change.
        let intent = cold_offload_intent(3600);
        let registered = store.register_intent(intent).unwrap();
        assert!(!registered.enabled);
        assert!(store.due_intents(0).unwrap().is_empty());

        // An operator/orchestrator opting in flips `enabled` and re-registers —
        // still additive, still gated by the SAME single knob the live sweep reads.
        let mut enabled = store.get_intent("cold_offload").unwrap();
        enabled.enabled = true;
        store.register_intent(enabled).unwrap();

        // Now due immediately (never run) — proving the engine CAN self-schedule
        // this durable job kind end-to-end: due-eval -> single-flight claim.
        let due = store.due_intents(0).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].name, "cold_offload");
        assert!(store.record_intent_tick("cold_offload", 0).unwrap());
        // Immediately re-checking is due-false (interval not elapsed) and, even if
        // forced, would collapse to the same tick window as a no-op.
        assert!(store.due_intents(1_000).unwrap().is_empty());
        assert!(!store.record_intent_tick("cold_offload", 1_000).unwrap());
    }
}
