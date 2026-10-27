//! `JobOp` — the wire op `Method::AnalyticsJob` carries (CONCEPT:INT-P2-1), gated
//! `jobs`. Mirrors `acl::RbacAdminOp`'s "one Method variant, one internal op enum"
//! shape so the durable analytics-job plane's caller and fenced remote-worker
//! surfaces cost the wire protocol exactly ONE new `Method` variant.
//!
//! Pure serde — no dependency on `eg-core`/`eg-jobs` (which sit ABOVE this crate in
//! the DAG). The facade (`src/server/handlers/jobs.rs`) is the one place that
//! translates a `SubmitJobSpec` into an `eg_jobs::store::SubmitSpec` + runs the
//! actual analytics work; this module only shapes what crosses the wire.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// The analytics-job control-plane operation (CONCEPT:INT-P2-1).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum JobOp {
    /// Submit a new job. Runs asynchronously off the request — the response carries
    /// the freshly durable `Submitted` job record (including its server-issued id),
    /// not the eventual result.
    Submit(SubmitJobSpec),
    /// Fetch a job's current durable state (including checkpoint/progress).
    Status { job_id: String },
    /// Cooperatively cancel a job (immediate if not yet started; the running
    /// executor observes the flag and stops otherwise).
    Cancel { job_id: String },
    /// Resume a job from its last checkpoint — a `Failed` job with retries
    /// remaining, or a `Running` job orphaned by a crashed/restarted process.
    Resume { job_id: String },
    /// Atomically claim the highest-priority eligible job. `worker_instance`
    /// distinguishes concurrent slots for one authenticated principal; the server
    /// hashes it with the verified identity before persistence.
    WorkerClaim {
        #[serde(default)]
        worker_instance: String,
        #[serde(default)]
        capabilities: Vec<String>,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    /// Renew the exact lease epoch owned by this authenticated worker slot.
    WorkerRenew {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    /// Persist a fenced progress checkpoint. `state_ref`, when present, must be an
    /// opaque external state reference rather than inline source/user content.
    WorkerCheckpoint {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        progress: f64,
        stage: String,
        #[serde(default)]
        state_ref: Option<String>,
    },
    /// Atomically stage the complete typed result before publication.
    WorkerStage {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        result: JobResult,
    },
    /// Commit evidence-bearing claims for a staged result and then mark success.
    WorkerPublish {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
    },
    /// Release a failed compute attempt through server-owned retry/backoff policy.
    WorkerFail {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        reason_code: String,
    },
    /// Confirm that a worker cooperatively stopped after a cancellation request.
    WorkerCancel {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
    },
}

fn default_lease_ms() -> u64 {
    60_000
}

/// Portable typed result column for remote analytics workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobResultColumn {
    pub name: String,
    pub logical_type: String,
    pub nullable: bool,
}

/// Reproducibility manifest bound server-side to the leased job lineage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobReproducibilityManifest {
    pub input_dataset_ref: String,
    pub input_content_digest: String,
    pub input_snapshot_version: u64,
    pub algorithm_ref: String,
    pub params_digest: String,
    pub implementation_version: String,
    pub environment_version: String,
    pub policy_fingerprint: String,
}

/// KnowledgeBatch-shaped result currency accepted by [`JobOp::WorkerStage`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobResult {
    pub schema_version: u16,
    pub dataset_ref: String,
    pub content_digest: String,
    pub schema: Vec<JobResultColumn>,
    pub rows: Vec<BTreeMap<String, serde_json::Value>>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub counterexample_refs: Vec<String>,
    #[serde(default)]
    pub uncertainty: Option<f64>,
    #[serde(default)]
    pub calibration: Option<(f64, f64)>,
    pub reproducibility: JobReproducibilityManifest,
}

/// Wire spec for [`JobOp::Submit`] (CONCEPT:INT-P2-1). `graph` is BOTH the tenancy
/// anchor (where the eventual result claim lands) and the input-snapshot handle's
/// graph name — the facade stamps in the graph's live `GraphCore::version()` at
/// submit time, so the snapshot handle is never client-supplied (a client cannot
/// forge which graph-version a job actually ran against).
///
/// Worker kernels reuse the same durable lease/publication state machine. The jobs
/// feature provides association-rule mining; the program-optimization feature adds
/// the graph-native, governed LM-program compiler.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SubmitJobSpec {
    /// Target graph: where the result claim lands, and whose `version()` at submit
    /// time becomes the job's immutable input-snapshot handle.
    pub graph: String,
    #[serde(default)]
    pub tenant: String,
    #[serde(default)]
    pub actor: String,
    #[serde(default)]
    pub purpose: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub deadline_unix_ms: Option<i64>,
    #[serde(default)]
    pub quota_cpu_ms: Option<u64>,
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    #[serde(default)]
    pub io_bytes: Option<u64>,
    #[serde(default)]
    pub output_bytes: Option<u64>,
    #[serde(default)]
    pub worker_pool: String,
    #[serde(default)]
    pub worker_region: String,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Retry cap (including the first attempt); `0`/`1` ⇒ no retry. Default `1`.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff_ms: u64,
    /// The job kind + its parameters. `params_digest` lineage is derived from
    /// `kind`'s own fields by the facade — a client never supplies it directly.
    pub kind: JobKind,
}

fn default_max_attempts() -> u32 {
    1
}

/// Which analytics computation a job runs (CONCEPT:INT-P2-1).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum JobKind {
    /// Association-rule mining over explicit transactions (mirrors
    /// `Method::MineAssociate`'s non-graph-derived path).
    MineAssociate {
        transactions: Vec<Vec<String>>,
        #[serde(default = "default_min_support")]
        min_support: f64,
        #[serde(default = "default_min_confidence")]
        min_confidence: f64,
        /// `"apriori" | "fpgrowth" | "eclat"`; unrecognized ⇒ facade error.
        #[serde(default = "default_algorithm")]
        algorithm: String,
    },
    /// Governed graph-native program compilation. The nested MessagePack is a
    /// versioned `eg_program::OptimizationRequest`; `eg-types` remains the bottom
    /// of the crate DAG and the server decodes/rebinds it at the verified boundary.
    #[cfg(feature = "program-optimization")]
    ProgramOptimize {
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        request_msgpack: Vec<u8>,
    },
}

fn default_min_support() -> f64 {
    0.1
}
fn default_min_confidence() -> f64 {
    0.5
}
fn default_algorithm() -> String {
    "fpgrowth".to_string()
}

// ---------------------------------------------------------------------------
// Generic durable task queue (EG-DURABLE-KERNEL-R071)
// ---------------------------------------------------------------------------

/// A task accepted by the durable queue.
///
/// The queue stores an opaque payload reference rather than the payload body.
/// The serving/outbox implementation owns persistence; this module only keeps
/// the producer-side wire model and its deterministic refusal rules.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TaskEnqueue {
    pub task_id: String,
    pub queue: String,
    pub payload_ref: String,
    pub idempotency_key: String,
}

impl TaskEnqueue {
    /// Construct an enqueue request without changing queue state.
    pub fn new(
        task_id: impl Into<String>,
        queue: impl Into<String>,
        payload_ref: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            queue: queue.into(),
            payload_ref: payload_ref.into(),
            idempotency_key: idempotency_key.into(),
        }
    }

    fn validate(&self) -> Result<(), TaskQueueRefusal> {
        if self.queue.trim().is_empty() {
            return Err(TaskQueueRefusal::EmptyQueueName);
        }
        Ok(())
    }
}

/// A worker's claim and its requested lease interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TaskClaim {
    pub task_id: String,
    pub queue: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub lease_ms: u64,
    pub claimed_at_ms: u64,
}

impl TaskClaim {
    /// Construct a claim without changing queue state.
    pub fn new(
        task_id: impl Into<String>,
        queue: impl Into<String>,
        worker_id: impl Into<String>,
        lease_epoch: u64,
        lease_ms: u64,
        claimed_at_ms: u64,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            queue: queue.into(),
            worker_id: worker_id.into(),
            lease_epoch,
            lease_ms,
            claimed_at_ms,
        }
    }

    fn validate(&self) -> Result<(), TaskQueueRefusal> {
        if self.queue.trim().is_empty() {
            return Err(TaskQueueRefusal::EmptyQueueName);
        }
        if self.lease_ms == 0 {
            return Err(TaskQueueRefusal::ZeroLease);
        }
        self.claimed_at_ms
            .checked_add(self.lease_ms)
            .ok_or(TaskQueueRefusal::LeaseOverflow)?;
        Ok(())
    }

    fn expires_at_ms(&self) -> Result<u64, TaskQueueRefusal> {
        self.claimed_at_ms
            .checked_add(self.lease_ms)
            .ok_or(TaskQueueRefusal::LeaseOverflow)
    }

    fn is_expired(&self, now_ms: u64) -> Result<bool, TaskQueueRefusal> {
        Ok(now_ms >= self.expires_at_ms()?)
    }
}

/// A worker acknowledgement fenced by the claim's lease epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TaskAck {
    pub task_id: String,
    pub queue: String,
    pub worker_id: String,
    pub lease_epoch: u64,
}

impl TaskAck {
    /// Construct an acknowledgement without changing queue state.
    pub fn new(
        task_id: impl Into<String>,
        queue: impl Into<String>,
        worker_id: impl Into<String>,
        lease_epoch: u64,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            queue: queue.into(),
            worker_id: worker_id.into(),
            lease_epoch,
        }
    }
}

/// Refusals that the durable task queue can determine before or at claim/ack.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TaskQueueRefusal {
    EmptyQueueName,
    ZeroLease,
    LeaseOverflow,
    UnknownTask,
    DuplicateTaskId,
    AlreadyClaimed,
    QueueMismatch,
    LeaseMismatch,
    UnclaimedLease,
    ExpiredLease,
    DuplicateIdempotencyKey,
}

/// Deterministic pure state machine used by the task-queue producer tests.
///
/// A served durable outbox can use these same transitions without making this
/// bottom-of-the-DAG crate depend on storage or a worker runtime.
#[derive(Clone, Debug, Default)]
pub struct TaskQueue {
    entries: BTreeMap<String, TaskQueueEntry>,
    idempotency_keys: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct TaskQueueEntry {
    request: TaskEnqueue,
    claim: Option<TaskClaim>,
}

impl TaskQueue {
    /// Enqueue once per idempotency key; refusal never mutates queue state.
    pub fn enqueue(&mut self, request: TaskEnqueue) -> Result<(), TaskQueueRefusal> {
        request.validate()?;
        if self.idempotency_keys.contains(&request.idempotency_key) {
            return Err(TaskQueueRefusal::DuplicateIdempotencyKey);
        }
        if self.entries.contains_key(&request.task_id) {
            return Err(TaskQueueRefusal::DuplicateTaskId);
        }
        self.idempotency_keys
            .insert(request.idempotency_key.clone());
        self.entries.insert(
            request.task_id.clone(),
            TaskQueueEntry {
                request,
                claim: None,
            },
        );
        Ok(())
    }

    /// Claim an enqueued task, replacing only a claim whose lease has expired.
    pub fn claim(&mut self, claim: TaskClaim) -> Result<(), TaskQueueRefusal> {
        claim.validate()?;
        let entry = self
            .entries
            .get_mut(&claim.task_id)
            .ok_or(TaskQueueRefusal::UnknownTask)?;
        if entry.request.queue != claim.queue {
            return Err(TaskQueueRefusal::QueueMismatch);
        }
        if let Some(current) = entry.claim.as_ref() {
            if !current.is_expired(claim.claimed_at_ms)? {
                return Err(TaskQueueRefusal::AlreadyClaimed);
            }
        }
        entry.claim = Some(claim);
        Ok(())
    }

    /// Acknowledge an unexpired claim and return its original enqueue record.
    pub fn ack(&mut self, ack: TaskAck, now_ms: u64) -> Result<TaskEnqueue, TaskQueueRefusal> {
        let entry = self
            .entries
            .get(&ack.task_id)
            .ok_or(TaskQueueRefusal::UnknownTask)?;
        let claim = entry
            .claim
            .as_ref()
            .ok_or(TaskQueueRefusal::UnclaimedLease)?;
        if claim.is_expired(now_ms)? {
            return Err(TaskQueueRefusal::ExpiredLease);
        }
        if entry.request.queue != ack.queue
            || claim.worker_id != ack.worker_id
            || claim.lease_epoch != ack.lease_epoch
        {
            return Err(TaskQueueRefusal::LeaseMismatch);
        }
        let entry = self
            .entries
            .remove(&ack.task_id)
            .expect("entry was checked immediately above");
        Ok(entry.request)
    }

    /// Number of enqueued tasks that have not been acknowledged.
    pub fn pending_len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod task_queue_tests {
    use super::*;
    use serde::de::DeserializeOwned;

    fn enqueue(task_id: &str, queue: &str, key: &str) -> TaskEnqueue {
        TaskEnqueue::new(task_id, queue, format!("payload:{task_id}"), key)
    }

    fn claim(
        task_id: &str,
        queue: &str,
        worker_id: &str,
        epoch: u64,
        start: u64,
        lease: u64,
    ) -> TaskClaim {
        TaskClaim::new(task_id, queue, worker_id, epoch, lease, start)
    }

    fn ack(task_id: &str, queue: &str, worker_id: &str, epoch: u64) -> TaskAck {
        TaskAck::new(task_id, queue, worker_id, epoch)
    }

    fn round_trip<T>(value: T) -> T
    where
        T: DeserializeOwned + Serialize + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_vec(&value).expect("encode task wire type");
        serde_json::from_slice(&encoded).expect("decode task wire type")
    }

    // The managed native qualification covers this pure model; served queue
    // integration remains the separate EG-DURABLE-KERNEL-R072 requirement.
    // spec: EG-DURABLE-KERNEL-R071
    #[test]
    fn task_wire_types_round_trip() {
        assert_eq!(
            round_trip(enqueue("task-1", "analytics", "request-1")),
            enqueue("task-1", "analytics", "request-1")
        );
        assert_eq!(
            round_trip(claim("task-1", "analytics", "worker-1", 7, 100, 1_000)),
            claim("task-1", "analytics", "worker-1", 7, 100, 1_000)
        );
        assert_eq!(
            round_trip(ack("task-1", "analytics", "worker-1", 7)),
            ack("task-1", "analytics", "worker-1", 7)
        );
    }

    // spec: EG-DURABLE-KERNEL-R071
    #[test]
    fn listed_task_queue_refusals_are_typed_and_leave_state_unchanged() {
        let mut queue = TaskQueue::default();
        let mut blank = enqueue("blank", "queue", "blank-key");
        blank.queue = "  ".into();
        assert_eq!(queue.enqueue(blank), Err(TaskQueueRefusal::EmptyQueueName));
        assert_eq!(queue.pending_len(), 0);

        let zero_lease = claim("missing", "queue", "worker-1", 1, 0, 0);
        assert_eq!(queue.claim(zero_lease), Err(TaskQueueRefusal::ZeroLease));
        assert_eq!(queue.pending_len(), 0);

        queue.enqueue(enqueue("task-1", "queue", "key-1")).unwrap();
        assert_eq!(
            queue.ack(ack("task-1", "queue", "worker-1", 1), 0),
            Err(TaskQueueRefusal::UnclaimedLease)
        );
        assert_eq!(queue.pending_len(), 1);

        queue
            .claim(claim("task-1", "queue", "worker-1", 1, 100, 10))
            .unwrap();
        assert_eq!(
            queue.ack(ack("task-1", "queue", "worker-1", 1), 110),
            Err(TaskQueueRefusal::ExpiredLease)
        );
        assert_eq!(queue.pending_len(), 1);

        assert_eq!(
            queue.enqueue(enqueue("task-2", "queue", "key-1")),
            Err(TaskQueueRefusal::DuplicateIdempotencyKey)
        );
        assert_eq!(queue.pending_len(), 1);
    }

    // spec: EG-DURABLE-KERNEL-R071
    #[test]
    fn live_claim_acknowledges_and_removes_only_the_claimed_task() {
        let mut queue = TaskQueue::default();
        queue.enqueue(enqueue("task-1", "queue", "key-1")).unwrap();
        queue
            .claim(claim("task-1", "queue", "worker-1", 3, 100, 10))
            .unwrap();
        let completed = queue
            .ack(ack("task-1", "queue", "worker-1", 3), 109)
            .unwrap();
        assert_eq!(completed.task_id, "task-1");
        assert_eq!(queue.pending_len(), 0);
    }
}
