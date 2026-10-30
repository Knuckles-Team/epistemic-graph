//! Colocated executor behaviour with a synthetic mining kernel injected through
//! the same `MiningKernel` seam production fills with the real kernel.

use super::core::open_job_store;
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

type Rules = Result<Vec<association::LabeledRule>, association::MiningCancelled>;

const SECRET_LABEL: &str = "secret-payload-label";

/// Runs until the executor asks it to stop, as a long mining pass would.
fn spinning_kernel(
    _: &[Vec<String>],
    _: f64,
    _: f64,
    _: association::Algorithm,
    stop: Arc<AtomicBool>,
) -> Rules {
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(5));
    }
    Err(association::MiningCancelled)
}

fn panicking_kernel(
    _: &[Vec<String>],
    _: f64,
    _: f64,
    _: association::Algorithm,
    _: Arc<AtomicBool>,
) -> Rules {
    panic!("kernel exploded over {SECRET_LABEL}");
}

struct Env {
    _dir: tempfile::TempDir,
    store: Arc<JobStore>,
    state: Arc<RwLock<ServerState>>,
    authority: CarrierAuthority,
}

fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let store = open_job_store(dir.path()).unwrap();
    let state = Arc::new(RwLock::new(ServerState::new_for_test(
        "secret",
        ServerState::test_isolation("owner"),
    )));
    Env {
        _dir: dir,
        store,
        state,
        authority: CarrierAuthority::verified_for_test("owner"),
    }
}

fn spec(deadline: Option<u64>, cpu: Option<u64>, attempts: u32, backoff_ms: u64) -> SubmitJobSpec {
    SubmitJobSpec {
        graph: "__commons__".to_string(),
        tenant: String::new(),
        actor: String::new(),
        purpose: "executor-test".to_string(),
        priority: 0,
        deadline_unix_ms: deadline.map(|value| value as i64),
        quota_cpu_ms: cpu,
        memory_bytes: None,
        io_bytes: None,
        output_bytes: None,
        worker_pool: String::new(),
        worker_region: String::new(),
        required_capabilities: Vec::new(),
        max_attempts: attempts,
        backoff_ms,
        kind: JobKind::MineAssociate {
            transactions: vec![
                vec![SECRET_LABEL.to_string(), "b".to_string()],
                vec![SECRET_LABEL.to_string(), "c".to_string()],
            ],
            min_support: 0.3,
            min_confidence: 0.5,
            algorithm: "fpgrowth".to_string(),
        },
    }
}

async fn submit(env: &Env, spec: SubmitJobSpec) {
    let response = submit::handle_submit_op(&env.state, &env.store, 1, &env.authority, None, spec)
        .await;
    assert!(response.error.is_none(), "submit refused: {response:?}");
}

fn claim(env: &Env, now_ms: i64) -> Option<WorkerClaim> {
    let capabilities = vec![
        opaque_worker_capability("mining.association"),
        opaque_worker_capability("pool:default"),
    ];
    env.store
        .claim_next(
            "worker:test",
            &capabilities,
            now_ms,
            60_000,
            tenant_worker_quota(),
        )
        .unwrap()
}

async fn wait_until_mining(env: &Env, job_id: &str) {
    for _ in 0..200 {
        if let Ok(job) = env.store.get(job_id) {
            if let JobState::Running { checkpoint } = &job.state {
                if checkpoint.stage == "mining" {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("job never reached the mining stage");
}

async fn run_to_stop(env: &Env, deadline: Option<u64>, cpu: Option<u64>) -> eg_jobs::AnalyticsJob {
    submit(env, spec(deadline, cpu, 1, 0)).await;
    let claimed = claim(env, unix_ms()).expect("the submitted job is claimable");
    let job_id = claimed.job.job_id.clone();
    run_claim(&env.state, &env.store, claimed, spinning_kernel).await;
    env.store.get(&job_id).unwrap()
}

// spec: EG-DURABLE-KERNEL-R082.7
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_job_stops_on_cancel_deadline_and_cpu_budget_without_renewal() {
    // Cancel request.
    let cancel_env = env();
    submit(&cancel_env, spec(None, None, 1, 0)).await;
    let claimed = claim(&cancel_env, unix_ms()).expect("claimable");
    let job_id = claimed.job.job_id.clone();
    let (state, store) = (cancel_env.state.clone(), cancel_env.store.clone());
    let runner = tokio::spawn(async move {
        run_claim(&state, &store, claimed, spinning_kernel).await;
    });
    wait_until_mining(&cancel_env, &job_id).await;
    let cancelled = submit::handle_cancel_op(
        &cancel_env.store,
        2,
        &cancel_env.authority,
        None,
        job_id.clone(),
    );
    assert!(cancelled.error.is_none(), "cancel refused: {cancelled:?}");
    runner.await.unwrap();
    let job = cancel_env.store.get(&job_id).unwrap();
    assert!(matches!(job.state, JobState::Cancelled { .. }), "{:?}", job.state);
    assert!(job.lease.is_none(), "no lease is held after the stop");

    // Past deadline.
    let deadline_env = env();
    let job = run_to_stop(&deadline_env, Some(unix_ms() as u64 + 400), None).await;
    match &job.state {
        JobState::Failed { reason, .. } => assert_eq!(reason, "deadline_exceeded"),
        other => panic!("expected Failed(deadline_exceeded), got {other:?}"),
    }
    assert!(job.lease.is_none());

    // Tiny CPU budget.
    let cpu_env = env();
    let job = run_to_stop(&cpu_env, None, Some(1)).await;
    match &job.state {
        JobState::Failed { reason, .. } => assert_eq!(reason, "cpu_budget_exceeded"),
        other => panic!("expected Failed(cpu_budget_exceeded), got {other:?}"),
    }
    assert!(job.lease.is_none());
}

// spec: EG-DURABLE-KERNEL-R082.8
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_attempts_use_bounded_codes_and_wait_for_backoff() {
    let env = env();
    submit(&env, spec(None, None, 2, 60_000)).await;
    let now = unix_ms();

    let first = claim(&env, now).expect("first attempt is claimable");
    let job_id = first.job.job_id.clone();
    run_claim(&env.state, &env.store, first, panicking_kernel).await;
    let after_first = env.store.get(&job_id).unwrap();
    assert!(matches!(after_first.state, JobState::Submitted));
    assert_eq!(after_first.retry.attempts_made, 1);
    assert!(
        claim(&env, now + 1_000).is_none(),
        "a retry must not be claimable before its backoff elapses"
    );

    let second = claim(&env, now + 61_000).expect("claimable once the backoff elapsed");
    run_claim(&env.state, &env.store, second, panicking_kernel).await;
    let done = env.store.get(&job_id).unwrap();
    assert_eq!(done.retry.attempts_made, 2);
    match &done.state {
        JobState::Failed { reason, checkpoint } => {
            assert_eq!(reason, "kernel_failure");
            assert!(!reason.contains(SECRET_LABEL));
            let stage = checkpoint.as_ref().map(|c| c.stage.clone()).unwrap_or_default();
            assert!(!stage.contains(SECRET_LABEL));
        }
        other => panic!("expected Failed(kernel_failure), got {other:?}"),
    }
    assert!(done.output.is_none(), "a failed job records no payload text");
}
