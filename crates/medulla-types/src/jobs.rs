//! `JobOp` — the wire op `Method::AnalyticsJob` carries (CONCEPT:INT-P2-1), gated
//! `jobs`. Mirrors `acl::RbacAdminOp`'s "one Method variant, one internal op enum"
//! shape so the durable analytics-job plane's caller and fenced remote-worker
//! surfaces cost the wire protocol exactly ONE new `Method` variant.
//!
//! Pure serde — no dependency on `medulla-core`/`medulla-jobs` (which sit ABOVE this crate in
//! the DAG). The facade (`src/server/handlers/jobs.rs`) is the one place that
//! translates a `SubmitJobSpec` into an `medulla_jobs::store::SubmitSpec` + runs the
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
    /// versioned `medulla_program::OptimizationRequest`; `medulla-types` remains the bottom
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
// Generic durable task queue (MEDULLA-DURABLE-KERNEL-R071)
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
    /// The worker lane that owns this task.  The generic queue is shared by
    /// every lane; this is routing metadata, not a second queue identity.
    #[serde(default = "default_task_lane")]
    pub lane: String,
    /// The stage tier used when choosing work within a lane.
    #[serde(default)]
    pub queue_class: TaskQueueClass,
    /// Higher values are considered before lower values within the selected
    /// lane.  Lane rotation always happens before this ordering.
    #[serde(default)]
    pub priority: u32,
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
            lane: default_task_lane(),
            queue_class: TaskQueueClass::default(),
            priority: 0,
            payload_ref: payload_ref.into(),
            idempotency_key: idempotency_key.into(),
        }
    }

    /// Set lane-aware scheduling metadata without changing queue state.
    pub fn with_lane(
        mut self,
        lane: impl Into<String>,
        queue_class: TaskQueueClass,
        priority: u32,
    ) -> Self {
        self.lane = lane.into();
        self.queue_class = queue_class;
        self.priority = priority;
        self
    }

    fn validate(&self) -> Result<(), TaskQueueRefusal> {
        if self.queue.trim().is_empty() {
            return Err(TaskQueueRefusal::EmptyQueueName);
        }
        if self.lane.trim().is_empty() {
            return Err(TaskQueueRefusal::EmptyLaneName);
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
    #[serde(default = "default_task_lane")]
    pub lane: String,
    #[serde(default)]
    pub queue_class: TaskQueueClass,
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
            lane: default_task_lane(),
            queue_class: TaskQueueClass::default(),
            worker_id: worker_id.into(),
            lease_epoch,
            lease_ms,
            claimed_at_ms,
        }
    }

    /// Construct a claim with explicit lane and stage tier metadata.
    #[allow(clippy::too_many_arguments)]
    pub fn for_lane(
        task_id: impl Into<String>,
        queue: impl Into<String>,
        lane: impl Into<String>,
        queue_class: TaskQueueClass,
        worker_id: impl Into<String>,
        lease_epoch: u64,
        lease_ms: u64,
        claimed_at_ms: u64,
    ) -> Self {
        Self::new(
            task_id,
            queue,
            worker_id,
            lease_epoch,
            lease_ms,
            claimed_at_ms,
        )
        .with_lane(lane, queue_class)
    }

    /// Set lane and stage tier metadata without changing lease state.
    pub fn with_lane(mut self, lane: impl Into<String>, queue_class: TaskQueueClass) -> Self {
        self.lane = lane.into();
        self.queue_class = queue_class;
        self
    }

    fn validate(&self) -> Result<(), TaskQueueRefusal> {
        if self.queue.trim().is_empty() {
            return Err(TaskQueueRefusal::EmptyQueueName);
        }
        if self.lane.trim().is_empty() {
            return Err(TaskQueueRefusal::EmptyLaneName);
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
    #[serde(default = "default_task_lane")]
    pub lane: String,
    #[serde(default)]
    pub queue_class: TaskQueueClass,
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
            lane: default_task_lane(),
            queue_class: TaskQueueClass::default(),
            worker_id: worker_id.into(),
            lease_epoch,
        }
    }

    /// Construct an acknowledgement carrying the claimed lane and tier.
    pub fn for_lane(
        task_id: impl Into<String>,
        queue: impl Into<String>,
        lane: impl Into<String>,
        queue_class: TaskQueueClass,
        worker_id: impl Into<String>,
        lease_epoch: u64,
    ) -> Self {
        Self::new(task_id, queue, worker_id, lease_epoch).with_lane(lane, queue_class)
    }

    /// Set lane and stage tier metadata without changing lease identity.
    pub fn with_lane(mut self, lane: impl Into<String>, queue_class: TaskQueueClass) -> Self {
        self.lane = lane.into();
        self.queue_class = queue_class;
        self
    }
}

fn default_task_lane() -> String {
    "default".to_string()
}

/// Stage tier used by the lane-fair task queue.
///
/// The values match the ingest worker contract: source commits are `Fast`,
/// projection/lexical work is `Medium`, and vector/index work is `SlowHeavy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TaskQueueClass {
    #[default]
    Fast,
    Medium,
    SlowHeavy,
}

/// Short compatibility name for callers that use the ingest contract's term.
pub type QueueClass = TaskQueueClass;

/// Refusals that the durable task queue can determine before or at claim/ack.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TaskQueueRefusal {
    EmptyQueueName,
    EmptyLaneName,
    ZeroLease,
    LeaseOverflow,
    UnknownTask,
    DuplicateTaskId,
    AlreadyClaimed,
    QueueMismatch,
    LaneMismatch,
    QueueClassMismatch,
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
    /// Last lane selected by `claim_next`; the next claim starts after it in
    /// canonical lane order.  The queue remains one shared backend.
    last_claimed_lane: BTreeMap<(String, TaskQueueClass), String>,
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
        if entry.request.lane != claim.lane {
            return Err(TaskQueueRefusal::LaneMismatch);
        }
        if entry.request.queue_class != claim.queue_class {
            return Err(TaskQueueRefusal::QueueClassMismatch);
        }
        if let Some(current) = entry.claim.as_ref() {
            if !current.is_expired(claim.claimed_at_ms)? {
                return Err(TaskQueueRefusal::AlreadyClaimed);
            }
        }
        entry.claim = Some(claim);
        Ok(())
    }

    /// Claim the next task from one shared queue, rotating across lanes before
    /// considering priority within a lane.  A queue class selects the stage
    /// tier, while the lane cursor keeps workers from starving another lane.
    pub fn claim_next(
        &mut self,
        queue: impl Into<String>,
        queue_class: TaskQueueClass,
        worker_id: impl Into<String>,
        lease_epoch: u64,
        lease_ms: u64,
        claimed_at_ms: u64,
    ) -> Result<Option<TaskClaim>, TaskQueueRefusal> {
        let queue = queue.into();
        if queue.trim().is_empty() {
            return Err(TaskQueueRefusal::EmptyQueueName);
        }
        if lease_ms == 0 {
            return Err(TaskQueueRefusal::ZeroLease);
        }
        claimed_at_ms
            .checked_add(lease_ms)
            .ok_or(TaskQueueRefusal::LeaseOverflow)?;

        let mut lanes = BTreeSet::new();
        for entry in self.entries.values() {
            if entry.request.queue != queue || entry.request.queue_class != queue_class {
                continue;
            }
            let available = entry
                .claim
                .as_ref()
                .map(|claim| claim.is_expired(claimed_at_ms))
                .transpose()?
                .unwrap_or(true);
            if available {
                lanes.insert(entry.request.lane.clone());
            }
        }
        if lanes.is_empty() {
            return Ok(None);
        }

        let lane_order: Vec<_> = lanes.into_iter().collect();
        let cursor_key = (queue.clone(), queue_class);
        let start = self
            .last_claimed_lane
            .get(&cursor_key)
            // The previous lane may have no available tasks now and therefore
            // be absent from `lane_order`. Start at the first available lane
            // after its canonical position, wrapping only when none follows.
            .map(|last| lane_order.partition_point(|lane| lane <= last) % lane_order.len())
            .unwrap_or(0);

        let mut selected: Option<(String, u32, String)> = None;
        for offset in 0..lane_order.len() {
            let lane = &lane_order[(start + offset) % lane_order.len()];
            let mut lane_candidate: Option<(u32, String)> = None;
            for (task_id, entry) in &self.entries {
                if entry.request.queue != queue
                    || entry.request.lane != *lane
                    || entry.request.queue_class != queue_class
                {
                    continue;
                }
                let available = entry
                    .claim
                    .as_ref()
                    .map(|claim| claim.is_expired(claimed_at_ms))
                    .transpose()?
                    .unwrap_or(true);
                if !available {
                    continue;
                }
                let candidate = (entry.request.priority, task_id.clone());
                if lane_candidate.as_ref().is_none_or(|current| {
                    candidate.0 > current.0 || (candidate.0 == current.0 && candidate.1 < current.1)
                }) {
                    lane_candidate = Some(candidate);
                }
            }
            if let Some((priority, task_id)) = lane_candidate {
                selected = Some((lane.clone(), priority, task_id));
                break;
            }
        }

        let Some((lane, _priority, task_id)) = selected else {
            return Ok(None);
        };
        let claim = TaskClaim::for_lane(
            task_id,
            queue,
            lane.clone(),
            queue_class,
            worker_id,
            lease_epoch,
            lease_ms,
            claimed_at_ms,
        );
        self.claim(claim.clone())?;
        self.last_claimed_lane.insert(cursor_key, lane);
        Ok(Some(claim))
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
        if entry.request.queue != ack.queue {
            return Err(TaskQueueRefusal::LeaseMismatch);
        }
        if entry.request.lane != ack.lane {
            return Err(TaskQueueRefusal::LaneMismatch);
        }
        if entry.request.queue_class != ack.queue_class {
            return Err(TaskQueueRefusal::QueueClassMismatch);
        }
        if claim.worker_id != ack.worker_id || claim.lease_epoch != ack.lease_epoch {
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

    /// Number of queued (not currently claimed) tasks for one lane and tier.
    pub fn lane_depth(&self, lane: &str, queue_class: TaskQueueClass) -> usize {
        self.entries
            .values()
            .filter(|entry| {
                entry.request.lane == lane
                    && entry.request.queue_class == queue_class
                    && entry.claim.is_none()
            })
            .count()
    }
}

#[cfg(test)]
mod task_queue_tests {
    use super::*;
    use serde::de::DeserializeOwned;

    fn enqueue(task_id: &str, queue: &str, key: &str) -> TaskEnqueue {
        TaskEnqueue::new(task_id, queue, format!("payload:{task_id}"), key)
    }

    fn lane_enqueue(task_id: &str, lane: &str, priority: u32) -> TaskEnqueue {
        enqueue(task_id, "shared", &format!("key:{task_id}")).with_lane(
            lane,
            TaskQueueClass::Fast,
            priority,
        )
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
    // integration remains the separate MEDULLA-DURABLE-KERNEL-R072 requirement.
    // spec: MEDULLA-DURABLE-KERNEL-R071
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

    // spec: MEDULLA-DURABLE-KERNEL-R071
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

    // spec: MEDULLA-DURABLE-KERNEL-R071
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

    // Lane-aware queue acceptance scenario; canonical status remains unbound
    // until managed integration and performance proof are available.
    #[test]
    fn lane_claim_rotates_before_priority_and_reports_depth() {
        let mut queue = TaskQueue::default();
        queue.enqueue(lane_enqueue("a-low", "lane-a", 1)).unwrap();
        queue.enqueue(lane_enqueue("a-high", "lane-a", 99)).unwrap();
        queue.enqueue(lane_enqueue("b-only", "lane-b", 1)).unwrap();
        queue.enqueue(lane_enqueue("c-only", "lane-c", 1)).unwrap();

        assert_eq!(queue.lane_depth("lane-a", TaskQueueClass::Fast), 2);
        assert_eq!(queue.lane_depth("lane-b", TaskQueueClass::Fast), 1);
        assert_eq!(queue.lane_depth("lane-c", TaskQueueClass::Fast), 1);

        let first = queue
            .claim_next("shared", TaskQueueClass::Fast, "worker-1", 1, 100, 10)
            .unwrap()
            .unwrap();
        assert_eq!(
            (first.task_id.as_str(), first.lane.as_str()),
            ("a-high", "lane-a")
        );
        assert_eq!(first.queue_class, TaskQueueClass::Fast);
        assert_eq!(queue.lane_depth("lane-a", TaskQueueClass::Fast), 1);

        let second = queue
            .claim_next("shared", TaskQueueClass::Fast, "worker-2", 1, 100, 10)
            .unwrap()
            .unwrap();
        assert_eq!(
            (second.task_id.as_str(), second.lane.as_str()),
            ("b-only", "lane-b")
        );
        let third = queue
            .claim_next("shared", TaskQueueClass::Fast, "worker-3", 1, 100, 10)
            .unwrap()
            .unwrap();
        assert_eq!(
            (third.task_id.as_str(), third.lane.as_str()),
            ("c-only", "lane-c")
        );
        assert_eq!(queue.lane_depth("lane-b", TaskQueueClass::Fast), 0);
        assert_eq!(queue.lane_depth("lane-c", TaskQueueClass::Fast), 0);

        // Once the other lanes have no pending work, rotation wraps to lane-a.
        let wrapped = queue
            .claim_next("shared", TaskQueueClass::Fast, "worker-4", 1, 100, 10)
            .unwrap()
            .unwrap();
        assert_eq!(
            (wrapped.task_id.as_str(), wrapped.lane.as_str()),
            ("a-low", "lane-a")
        );
        assert_eq!(queue.lane_depth("lane-a", TaskQueueClass::Fast), 0);
    }
}
