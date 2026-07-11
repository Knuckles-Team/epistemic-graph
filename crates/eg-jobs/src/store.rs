//! Durable `AnalyticsJob` store over redb (CONCEPT:INT-P2-1), mirroring the
//! `eg-tsdb`/`eg-kvcache` shape: a SEPARATE `jobs.redb` file beside `graph.redb` (redb
//! holds an exclusive per-process file lock, so a job store can never share a handle
//! with the graph registry's own file), independent of the graph write-coalescer
//! (jobs are keyed by `job_id`, not a graph).
//!
//! [`JobStore`] owns BOTH tables:
//!   * `JOBS`             — `job_id -> msgpack(AnalyticsJob)`, the durable record.
//!   * `COMMITTED_RESULTS` — `result_ref -> job_id` (the FIRST job that committed
//!     it), a store-level idempotency ledger independent of any live graph — see
//!     [`JobStore::mark_result_committed`]. The `claim.rs` writeback additionally
//!     guards on `GraphCore::has_node` for the claim id itself, so idempotency holds
//!     even if a caller wires a DIFFERENT graph/claim path on top of this store.
//!
//! Every transition is a guarded state-machine edge (CONCEPT:INT-P2-1): an invalid
//! edge (e.g. checkpointing a `Cancelled` job) is a hard `Err`, never a silent
//! overwrite — a durable job record's history should never appear to un-happen.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::model::{AnalyticsJob, Checkpoint, JobId, JobPolicy, JobState};

const JOBS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("analytics_jobs");
const COMMITTED_RESULTS: TableDefinition<'static, &str, &str> =
    TableDefinition::new("analytics_job_committed_results");

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
            } => write!(f, "invalid transition on job {job_id} in state {state}: {reason}"),
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
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

/// A durable store of [`AnalyticsJob`] records, backed by its own `jobs.redb`.
pub struct JobStore {
    db: Database,
    /// No-rand monotonic id source (mirrors `src/server/txn.rs::TxnIdGen`): `"job-<hex>"`.
    next_id: AtomicU64,
}

impl JobStore {
    /// Open (or create) the job store at an exact file path, materializing the
    /// schema so an empty DB is queryable.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_err(redb_err)?;
        {
            let wtx = db.begin_write().map_err(redb_err)?;
            wtx.open_table(JOBS).map_err(redb_err)?;
            wtx.open_table(COMMITTED_RESULTS).map_err(redb_err)?;
            wtx.commit().map_err(redb_err)?;
        }
        let next_id = AtomicU64::new(Self::max_existing_seq(&db)?);
        Ok(Self { db, next_id })
    }

    /// Open `{persist_dir}/jobs.redb` — the durable location beside `graph.redb`.
    pub fn open_in_dir(persist_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(persist_dir)
            .map_err(|e| JobError::Redb(format!("create persist dir: {e}")))?;
        Self::open(&persist_dir.join("jobs.redb"))
    }

    /// Recover the next monotonic id sequence from durable state (CONCEPT:INT-P2-1
    /// restart-durability): scans existing `job-<hex>` ids and returns one past the
    /// max, so a restart never reissues an id a prior process already handed out.
    fn max_existing_seq(db: &Database) -> Result<u64> {
        let rtx = db.begin_read().map_err(redb_err)?;
        let table = match rtx.open_table(JOBS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(e) => return Err(redb_err(e)),
        };
        let mut max_seq = 0u64;
        for entry in table.iter().map_err(redb_err)? {
            let (k, _) = entry.map_err(redb_err)?;
            if let Some(hex) = k.value().strip_prefix("job-") {
                if let Ok(n) = u64::from_str_radix(hex, 16) {
                    max_seq = max_seq.max(n);
                }
            }
        }
        Ok(max_seq)
    }

    fn next_job_id(&self) -> JobId {
        let n = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("job-{n:016x}")
    }

    fn get_raw(&self, job_id: &str) -> Result<AnalyticsJob> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(JOBS).map_err(redb_err)?;
        let blob = table
            .get(job_id)
            .map_err(redb_err)?
            .ok_or_else(|| JobError::NotFound(job_id.to_string()))?;
        rmp_serde::from_slice(blob.value()).map_err(codec_err)
    }

    fn put_raw(&self, job: &AnalyticsJob) -> Result<()> {
        let blob = rmp_serde::to_vec_named(job).map_err(codec_err)?;
        let wtx = self.db.begin_write().map_err(redb_err)?;
        {
            let mut table = wtx.open_table(JOBS).map_err(redb_err)?;
            table.insert(job.job_id.as_str(), blob.as_slice()).map_err(redb_err)?;
        }
        wtx.commit().map_err(redb_err)?;
        Ok(())
    }

    /// Submit a new job — the `Submitted` entry point. Returns the durable record
    /// (including its server-issued `job_id`).
    pub fn submit(&self, spec: SubmitSpec) -> Result<AnalyticsJob> {
        let now = now_ms();
        let job = AnalyticsJob {
            job_id: self.next_job_id(),
            input_snapshot: spec.input_snapshot,
            policy: spec.policy,
            algo: spec.algo,
            retry: crate::model::RetryPolicy {
                max_attempts: spec.max_attempts.max(1),
                backoff_ms: spec.backoff_ms,
                attempts_made: 0,
            },
            state: JobState::Submitted,
            cancel_requested: false,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.put_raw(&job)?;
        Ok(job)
    }

    /// Fetch a job's current durable record.
    pub fn get(&self, job_id: &str) -> Result<AnalyticsJob> {
        self.get_raw(job_id)
    }

    /// List every durable job id (diagnostic/admin use).
    pub fn list_ids(&self) -> Result<Vec<JobId>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(JOBS).map_err(redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(redb_err)? {
            let (k, _) = entry.map_err(redb_err)?;
            out.push(k.value().to_string());
        }
        Ok(out)
    }

    /// `Submitted -> Running(checkpoint=0)`, OR a no-op re-entry if ALREADY `Running`
    /// (idempotent start — an executor that crashed right after starting and retries
    /// `start_running` on reconnect should not error). Rejects every OTHER state
    /// (a terminal job cannot be (re)started; use [`Self::resume`]).
    pub fn start_running(&self, job_id: &str) -> Result<AnalyticsJob> {
        let mut job = self.get_raw(job_id)?;
        match &job.state {
            JobState::Submitted => {
                job.state = JobState::Running {
                    checkpoint: Checkpoint {
                        progress: 0.0,
                        stage: "started".to_string(),
                        state_blob: None,
                        updated_at_ms: now_ms(),
                    },
                };
            }
            JobState::Running { .. } => { /* idempotent re-entry */ }
            other => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: other.label(),
                    reason: "start_running requires Submitted (or already Running)",
                });
            }
        }
        job.updated_at_ms = now_ms();
        self.put_raw(&job)?;
        Ok(job)
    }

    /// Record progress on a `Running` job. `progress` is clamped to `[0,1]`.
    pub fn checkpoint(
        &self,
        job_id: &str,
        progress: f64,
        stage: impl Into<String>,
        state_blob: Option<Vec<u8>>,
    ) -> Result<AnalyticsJob> {
        let mut job = self.get_raw(job_id)?;
        match &job.state {
            JobState::Running { .. } => {
                job.state = JobState::Running {
                    checkpoint: Checkpoint {
                        progress: progress.clamp(0.0, 1.0),
                        stage: stage.into(),
                        state_blob,
                        updated_at_ms: now_ms(),
                    },
                };
            }
            other => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: other.label(),
                    reason: "checkpoint requires Running",
                });
            }
        }
        job.updated_at_ms = now_ms();
        self.put_raw(&job)?;
        Ok(job)
    }

    /// `Running -> Succeeded(result_ref)`. Idempotent: calling this again on an
    /// ALREADY-`Succeeded` job with the SAME `result_ref` is a no-op success (the
    /// common "the async task's completion callback fired twice" case); a DIFFERENT
    /// `result_ref` on an already-succeeded job is a hard error (that would mean the
    /// same job_id somehow computed two different answers, a correctness bug, never
    /// silently overwritten).
    pub fn succeed(&self, job_id: &str, result_ref: impl Into<String>) -> Result<AnalyticsJob> {
        let result_ref = result_ref.into();
        let mut job = self.get_raw(job_id)?;
        match &job.state {
            JobState::Running { checkpoint } => {
                let mut checkpoint = checkpoint.clone();
                checkpoint.progress = 1.0;
                checkpoint.updated_at_ms = now_ms();
                job.state = JobState::Succeeded {
                    result_ref,
                    checkpoint,
                };
            }
            JobState::Succeeded {
                result_ref: existing,
                ..
            } if *existing == result_ref => {
                return Ok(job); // idempotent re-ack, no-op
            }
            other => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: other.label(),
                    reason: "succeed requires Running (or a matching already-Succeeded no-op)",
                });
            }
        }
        job.updated_at_ms = now_ms();
        self.put_raw(&job)?;
        Ok(job)
    }

    /// `Running -> Failed`, bumping `retry.attempts_made`. If retries remain the
    /// caller should [`Self::resume`] (or re-`submit`) the job; this method itself
    /// only records the failure, it never auto-retries.
    pub fn fail(&self, job_id: &str, reason: impl Into<String>) -> Result<AnalyticsJob> {
        let mut job = self.get_raw(job_id)?;
        let checkpoint = job.state.checkpoint().cloned();
        match &job.state {
            JobState::Running { .. } => {
                job.retry.attempts_made += 1;
                job.state = JobState::Failed {
                    reason: reason.into(),
                    checkpoint,
                };
            }
            other => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: other.label(),
                    reason: "fail requires Running",
                });
            }
        }
        job.updated_at_ms = now_ms();
        self.put_raw(&job)?;
        Ok(job)
    }

    /// Cooperative cancel (CONCEPT:INT-P2-1): sets `cancel_requested`, and for a job
    /// that has not started yet (`Submitted`) transitions straight to `Cancelled`
    /// (nothing is running to observe the flag). A `Running` job stays `Running`
    /// until the executor observes `cancel_requested` and calls
    /// [`Self::mark_cancelled`] — cancellation of in-flight work is COOPERATIVE, not
    /// preemptive (the engine has no thread to forcibly kill).
    pub fn request_cancel(&self, job_id: &str) -> Result<AnalyticsJob> {
        let mut job = self.get_raw(job_id)?;
        if job.state.is_terminal() {
            return Err(JobError::InvalidTransition {
                job_id: job_id.to_string(),
                state: job.state.label(),
                reason: "cannot cancel a job already in a terminal state",
            });
        }
        job.cancel_requested = true;
        if matches!(job.state, JobState::Submitted) {
            job.state = JobState::Cancelled { checkpoint: None };
        }
        job.updated_at_ms = now_ms();
        self.put_raw(&job)?;
        Ok(job)
    }

    /// Finalize a cooperative cancel of a `Running` job (the executor calls this
    /// after observing `cancel_requested` and stopping). Requires `Running` AND
    /// `cancel_requested` — an executor cannot mark a job cancelled that was never
    /// asked to cancel (that would silently drop a result the caller still wants).
    pub fn mark_cancelled(&self, job_id: &str) -> Result<AnalyticsJob> {
        let mut job = self.get_raw(job_id)?;
        match &job.state {
            JobState::Running { checkpoint } if job.cancel_requested => {
                job.state = JobState::Cancelled {
                    checkpoint: Some(checkpoint.clone()),
                };
            }
            JobState::Running { .. } => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: "running",
                    reason: "mark_cancelled requires a prior request_cancel",
                });
            }
            other => {
                return Err(JobError::InvalidTransition {
                    job_id: job_id.to_string(),
                    state: other.label(),
                    reason: "mark_cancelled requires Running",
                });
            }
        }
        job.updated_at_ms = now_ms();
        self.put_raw(&job)?;
        Ok(job)
    }

    /// Resume a job from its last checkpoint (CONCEPT:INT-P2-1): valid from
    /// `Failed` (a deliberate retry — requires retries remaining) or from `Running`
    /// (an ORPHANED job — the record says "running" but the process that was running
    /// it crashed/restarted with no live task; resuming re-enters `Running` from the
    /// SAME checkpoint so the caller's executor can pick the work back up). A
    /// `Cancelled` job is a terminal, deliberate stop — NOT resumable; resubmit
    /// instead. A `Succeeded` job is already done — resuming it is also rejected
    /// (fetch the existing `result_ref` instead).
    pub fn resume(&self, job_id: &str) -> Result<AnalyticsJob> {
        let mut job = self.get_raw(job_id)?;
        match &job.state {
            JobState::Running { checkpoint } => {
                // Orphaned-running re-entry: same checkpoint, cleared cancel flag
                // only if one was never actually observed by an executor.
                let checkpoint = checkpoint.clone();
                job.state = JobState::Running { checkpoint };
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
                    reason: "resume requires Failed (with retries remaining) or an orphaned Running job",
                });
            }
        }
        job.updated_at_ms = now_ms();
        self.put_raw(&job)?;
        Ok(job)
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
        let wtx = self.db.begin_write().map_err(redb_err)?;
        let first = {
            let mut table = wtx.open_table(COMMITTED_RESULTS).map_err(redb_err)?;
            let existing = table.get(result_ref).map_err(redb_err)?.map(|v| v.value().to_string());
            match existing {
                Some(_) => false,
                None => {
                    table.insert(result_ref, job_id).map_err(redb_err)?;
                    true
                }
            }
        };
        wtx.commit().map_err(redb_err)?;
        Ok(first)
    }

    /// Which job (if any) first committed `result_ref`.
    pub fn result_committed_by(&self, result_ref: &str) -> Result<Option<JobId>> {
        let rtx = self.db.begin_read().map_err(redb_err)?;
        let table = rtx.open_table(COMMITTED_RESULTS).map_err(redb_err)?;
        Ok(table.get(result_ref).map_err(redb_err)?.map(|v| v.value().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AlgoVersion, InputSnapshotHandle};

    fn spec(graph: &str, version: u64) -> SubmitSpec {
        SubmitSpec {
            input_snapshot: InputSnapshotHandle::new(graph, version),
            policy: JobPolicy {
                tenant: "acme".into(),
                actor: "agent:planner".into(),
                purpose: "quarterly-mining".into(),
                priority: 0,
                quota_cpu_ms: None,
                deadline_unix_ms: None,
            },
            algo: AlgoVersion {
                family: "mining.association".into(),
                algorithm: "fpgrowth".into(),
                params_digest: "deadbeef".into(),
                code_version: "test".into(),
                env_version: "test".into(),
            },
            max_attempts: 3,
            backoff_ms: 10,
        }
    }

    #[test]
    fn submit_starts_in_submitted_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        assert_eq!(job.state, JobState::Submitted);
        assert!(job.job_id.starts_with("job-"));
    }

    #[test]
    fn full_lifecycle_submit_running_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 7)).unwrap();

        let job = store.start_running(&job.job_id).unwrap();
        assert!(matches!(job.state, JobState::Running { .. }));

        let job = store
            .checkpoint(&job.job_id, 0.5, "half-way", Some(vec![1, 2, 3]))
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
        let job = store.succeed(&job.job_id, result_ref.clone()).unwrap();
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

        // Idempotent re-ack: calling succeed again with the SAME result_ref is a no-op.
        let job2 = store.succeed(&job.job_id, result_ref).unwrap();
        assert_eq!(job2.state, job.state);
    }

    #[test]
    fn succeed_with_different_result_ref_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        store.start_running(&job.job_id).unwrap();
        store.succeed(&job.job_id, "result:aaa").unwrap();
        let err = store.succeed(&job.job_id, "result:bbb").unwrap_err();
        assert!(matches!(err, JobError::InvalidTransition { .. }));
    }

    #[test]
    fn cancel_mid_run_stops_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        store.start_running(&job.job_id).unwrap();
        store
            .checkpoint(&job.job_id, 0.3, "chunk-1", None)
            .unwrap();

        let job = store.request_cancel(&job.job_id).unwrap();
        assert!(job.cancel_requested);
        assert!(matches!(job.state, JobState::Running { .. }));

        let job = store.mark_cancelled(&job.job_id).unwrap();
        assert!(matches!(job.state, JobState::Cancelled { .. }));

        // Terminal: no further checkpoint/succeed/cancel is accepted.
        assert!(store.checkpoint(&job.job_id, 0.5, "x", None).is_err());
        assert!(store.succeed(&job.job_id, "result:x").is_err());
        assert!(store.request_cancel(&job.job_id).is_err());
    }

    #[test]
    fn cancel_before_start_is_immediate() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        let job = store.request_cancel(&job.job_id).unwrap();
        assert!(matches!(job.state, JobState::Cancelled { .. }));
    }

    #[test]
    fn mark_cancelled_requires_a_prior_request() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        store.start_running(&job.job_id).unwrap();
        assert!(store.mark_cancelled(&job.job_id).is_err());
    }

    #[test]
    fn resume_from_checkpoint_after_simulated_crash() {
        let dir = tempfile::tempdir().unwrap();
        let job_id = {
            let store = JobStore::open_in_dir(dir.path()).unwrap();
            let job = store.submit(spec("g1", 1)).unwrap();
            store.start_running(&job.job_id).unwrap();
            store
                .checkpoint(&job.job_id, 0.4, "chunk-2", Some(vec![9, 9]))
                .unwrap();
            job.job_id
            // `store` (and its redb `Database` handle) drops here — simulates the
            // process exiting mid-run with the job durably left in `Running`.
        };

        // Reopen at the SAME path (CONCEPT:INT-P2-1 restart-durability): the
        // orphaned `Running` job + its checkpoint must have survived.
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.get(&job_id).unwrap();
        match &job.state {
            JobState::Running { checkpoint } => {
                assert_eq!(checkpoint.progress, 0.4);
                assert_eq!(checkpoint.stage, "chunk-2");
            }
            other => panic!("expected orphaned Running, got {other:?}"),
        }

        let job = store.resume(&job_id).unwrap();
        match &job.state {
            JobState::Running { checkpoint } => assert_eq!(checkpoint.progress, 0.4),
            other => panic!("expected Running after resume, got {other:?}"),
        }

        // Finish the remaining work from where the checkpoint left off.
        let job = store.checkpoint(&job_id, 1.0, "done", None).unwrap();
        let result_ref = job.result_ref();
        let job = store.succeed(&job_id, result_ref).unwrap();
        assert!(matches!(job.state, JobState::Succeeded { .. }));
    }

    #[test]
    fn resume_a_failed_job_retries_when_attempts_remain() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        store.start_running(&job.job_id).unwrap();
        store
            .checkpoint(&job.job_id, 0.2, "chunk-1", None)
            .unwrap();
        let job = store.fail(&job.job_id, "transient error").unwrap();
        assert_eq!(job.retry.attempts_made, 1);

        let job = store.resume(&job.job_id).unwrap();
        match &job.state {
            JobState::Running { checkpoint } => assert_eq!(checkpoint.progress, 0.2),
            other => panic!("expected Running after resume, got {other:?}"),
        }
    }

    #[test]
    fn resume_exhausted_retries_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let mut spec = spec("g1", 1);
        spec.max_attempts = 1;
        let store_job = store.submit(spec).unwrap();
        store.start_running(&store_job.job_id).unwrap();
        store.fail(&store_job.job_id, "boom").unwrap();
        assert!(store.resume(&store_job.job_id).is_err());
    }

    #[test]
    fn cancelled_job_is_not_resumable() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.submit(spec("g1", 1)).unwrap();
        let job = store.request_cancel(&job.job_id).unwrap();
        assert!(matches!(job.state, JobState::Cancelled { .. }));
        assert!(store.resume(&job.job_id).is_err());
    }

    #[test]
    fn deterministic_result_ref_same_lineage_same_ref() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
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
    fn mark_result_committed_is_first_wins() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
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
            let store = JobStore::open_in_dir(dir.path()).unwrap();
            store.submit(spec("g1", 1)).unwrap().job_id
        };
        // Reopen a FRESH `JobStore` at the same path — the durable record + the
        // monotonic id sequence must both survive.
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job = store.get(&job_id).unwrap();
        assert_eq!(job.state, JobState::Submitted);

        let next = store.submit(spec("g1", 1)).unwrap();
        assert_ne!(next.job_id, job_id, "id sequence must not be reused after restart");
    }

    #[test]
    fn get_missing_job_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open_in_dir(dir.path()).unwrap();
        let job_id = "job-0000000000000000";
        assert!(matches!(store.get(job_id), Err(JobError::NotFound(_))));
    }
}
