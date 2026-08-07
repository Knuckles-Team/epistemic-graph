//! DE7 — replay-determinism property test for `eg-jobs::JobStore`.
//!
//! Restate's own test discipline holds one property sacred for its journal: the
//! same input, replayed across a crash, must reach byte-identical state. This test
//! proves the equivalent for `JobStore`'s fenced-lease job-lifecycle state machine:
//! a fully deterministic script (explicit worker refs, explicit `now_ms` clocks, a
//! content-addressed `MutationBatch` for job creation — no wall clock, no RNG)
//! reaches the IDENTICAL final durable state whether it runs straight through on
//! one open store, or is interrupted by a SIMULATED CRASH (the `JobStore` — and
//! with it the underlying `redb::Database` — is dropped mid-script with no
//! graceful shutdown call, exactly like `kill -9`) at an arbitrary cut point and
//! then reopened from the same `jobs.redb` file to finish the script.
//!
//! Every operation this script drives is chosen because it exposes the caller a
//! deterministic, replay-safe entry point: `submit_batch` (not the wall-clock
//! `submit`) for creation, and the fenced transition methods
//! (`claim_next`/`checkpoint_fenced`/`stage_result_fenced`/
//! `complete_publication_fenced`), which all take an explicit `now_ms: i64`
//! parameter rather than reading `SystemTime::now()` internally. See this lane's
//! report for the one entry point (`JobStore::submit`) that is NOT replay-safe by
//! this standard, and why `submit_batch` is used here instead.

use eg_jobs::{
    AlgoVersion, AnalyticsJob, Checkpoint, InputSnapshotHandle, JobId, JobPolicy,
    ReproducibilityManifest, ResultColumn, SubmitSpec, TenantJobQuota, TypedJobResult,
};
use eg_types::mutation_batch::{
    MutationBatch, MutationDomain, MutationOperation, MutationOutboxIntent, MutationRequestContext,
    MutationSurface, MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;
use proptest::prelude::*;
use std::collections::BTreeMap;

/// A fixed, valid-shaped digest — `MutationBatch::validate` only checks the
/// `principal:sha256:<64 lowercase hex>` SHAPE, not that it is a real digest of
/// anything, so a constant satisfies it without pulling `sha2` into this test
/// crate's dev-dependencies.
const TEST_PRINCIPAL: &str =
    "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn spec() -> SubmitSpec {
    SubmitSpec {
        input_snapshot: InputSnapshotHandle::new("crash-replay-graph", 1).with_dataset(
            "eg:job_input:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        ),
        policy: JobPolicy {
            tenant: "acme".into(),
            actor: "agent:crash-replay".into(),
            purpose: "de7-replay-determinism".into(),
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

/// A deterministic, content-addressed-by-`batch_id` `MutationBatch` for job
/// creation — the replay-safe sibling of the private `internal_job_batch`/
/// `request_batch` helpers `store.rs`'s own unit tests use for transitions.
fn submit_batch(batch_id: &str, committed_at_ms: u64) -> MutationBatch {
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Job,
        domain: MutationDomain::AnalyticsJob,
        method: Method::ApplyMutation {
            event_type: "analytics_job_submit".to_string(),
            query: batch_id.to_string(),
        },
    };
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        context: MutationRequestContext {
            request_id: 0,
            principal: TEST_PRINCIPAL.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
        },
        tenant: "crash-replay".to_string(),
        graph: "jobs".to_string(),
        placement_epoch: 0,
        idempotency_key: batch_id.to_string(),
        expected_graph_version: None,
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation.clone()],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.analytics-job.submitted".to_string(),
            key: batch_id.to_string(),
            payload: rmp_serde::to_vec_named(&operation).unwrap(),
            headers: BTreeMap::new(),
        }],
        created_at_ms: committed_at_ms,
    };
    batch
        .validate()
        .expect("hand-built submit batch must validate");
    batch
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
    .map(|name| ResultColumn {
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
        ReproducibilityManifest {
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

/// Per-processing-slot bookkeeping the test DRIVER (not the store) carries across
/// a simulated crash: which job a given `claim_next` call returned, so the
/// following checkpoint/stage/complete calls in the SAME script slot target it.
/// This is driver-local memory of what already happened — legitimate, since only
/// the store (the thing under test) is being crash-simulated, never the test
/// process itself.
#[derive(Default)]
struct ScriptState {
    claims: Vec<Option<(JobId, String, u64)>>,
}

/// Execute script steps `range` of a `5*n`-step deterministic job-lifecycle
/// script against `store`: steps `0..n` each submit one job (via the
/// content-addressed `submit_batch`, never wall-clock `submit`); steps `n..5n`
/// are grouped into `n` four-step slots — claim, checkpoint, stage-result,
/// complete-publication — each using only caller-supplied clocks/identities.
fn run_steps(
    store: &eg_jobs::JobStore,
    n: usize,
    state: &mut ScriptState,
    range: std::ops::Range<usize>,
) {
    for step in range {
        if step < n {
            let i = step;
            let batch = submit_batch(&format!("crash-replay-submit:{i}"), 1_000 + i as u64 * 100);
            store
                .submit_batch(spec(), &batch, 1_000 + i as u64 * 100)
                .expect("submit_batch");
        } else {
            let rel = step - n;
            let slot = rel / 4;
            let phase = rel % 4;
            let now = 2_000 + step as i64 * 100;
            if state.claims.len() <= slot {
                state.claims.resize(slot + 1, None);
            }
            match phase {
                0 => {
                    let claim = store
                        .claim_next(
                            "worker:crash-test",
                            &[],
                            now,
                            600_000,
                            TenantJobQuota::default(),
                        )
                        .expect("claim_next")
                        .expect("a ready job must exist for this slot");
                    state.claims[slot] = Some((
                        claim.job.job_id.clone(),
                        claim.lease.worker_ref.clone(),
                        claim.lease.epoch,
                    ));
                }
                1 => {
                    let (job_id, worker, epoch) = state.claims[slot].clone().unwrap();
                    store
                        .checkpoint_fenced(
                            &job_id,
                            &worker,
                            epoch,
                            Checkpoint {
                                progress: 0.5,
                                stage: "half".to_string(),
                                state_blob: Some(vec![slot as u8]),
                                updated_at_ms: now,
                            },
                            now,
                        )
                        .expect("checkpoint_fenced");
                }
                2 => {
                    let (job_id, worker, epoch) = state.claims[slot].clone().unwrap();
                    let job = store.get(&job_id).expect("get");
                    let result = empty_result(&job);
                    store
                        .stage_result_fenced(&job_id, &worker, epoch, result, now)
                        .expect("stage_result_fenced");
                }
                3 => {
                    let (job_id, worker, epoch) = state.claims[slot].clone().unwrap();
                    store
                        .complete_publication_fenced(&job_id, &worker, epoch, now)
                        .expect("complete_publication_fenced");
                }
                _ => unreachable!(),
            }
        }
    }
}

/// Canonical snapshot of every durable job record, sorted by id so it can be
/// compared byte-for-byte regardless of internal iteration order.
fn snapshot(store: &eg_jobs::JobStore) -> Vec<(JobId, AnalyticsJob)> {
    let mut ids = store.list_ids().expect("list_ids");
    ids.sort();
    ids.into_iter()
        .map(|id| {
            let job = store.get(&id).expect("get");
            (id, job)
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// The DE7 headline property: for ANY number of jobs (1..=3) and ANY cut
    /// point in the resulting script, a crash-and-reopen mid-script reaches the
    /// SAME final durable state as an uninterrupted run of the identical script.
    #[test]
    fn crash_replay_reaches_identical_state(n in 1usize..=3, crash_frac in 0usize..=100usize) {
        let total = 5 * n;
        let crash_at = (crash_frac * total) / 100;

        // Uninterrupted baseline.
        let clean_dir = tempfile::tempdir().unwrap();
        let clean_store = eg_jobs::JobStore::open_in_dir(clean_dir.path()).unwrap();
        let mut clean_state = ScriptState::default();
        run_steps(&clean_store, n, &mut clean_state, 0..total);
        let clean_snapshot = snapshot(&clean_store);

        // The clean and crash runs execute back-to-back in the same process; a
        // short sleep guarantees they land in DIFFERENT wall-clock milliseconds.
        // This has NO effect on a correct, replay-safe store (every timestamp in
        // the script is caller-supplied, never read from the clock) — it exists
        // only so a real wall-clock leak (the exact defect class this test
        // targets) is reliably observable instead of passing by coincidence when
        // both runs happen to land in the same millisecond.
        std::thread::sleep(std::time::Duration::from_millis(15));

        // Crash-interrupted: same script, same persist dir, a hard drop (no
        // graceful shutdown) at `crash_at`, then reopen and finish the script.
        let crash_dir = tempfile::tempdir().unwrap();
        let mut crash_state = ScriptState::default();
        {
            let store = eg_jobs::JobStore::open_in_dir(crash_dir.path()).unwrap();
            run_steps(&store, n, &mut crash_state, 0..crash_at);
            // `store` drops here — simulated crash: no close/shutdown call.
        }
        let reopened = eg_jobs::JobStore::open_in_dir(crash_dir.path()).unwrap();
        run_steps(&reopened, n, &mut crash_state, crash_at..total);
        let crash_snapshot = snapshot(&reopened);

        prop_assert_eq!(clean_snapshot.len(), n);
        prop_assert_eq!(clean_snapshot, crash_snapshot);
    }
}
