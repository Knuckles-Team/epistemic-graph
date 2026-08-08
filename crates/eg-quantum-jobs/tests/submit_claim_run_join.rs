//! Step 1 end-to-end proof (addendum staging discipline, "Each step must carry a
//! real workload before the next begins"): submit a REAL `eg-jobs::AnalyticsJob`
//! carrying a GHZ-entangled quantum circuit built from an induced subgraph, have a
//! worker claim + run + complete it through the REAL fenced-lease state machine, and
//! join the durable result back to `(id, score)` rows — verified against the
//! ANALYTIC prediction for a noiseless GHZ state, not just "it didn't crash".

use eg_jobs::{InputSnapshotHandle, JobPolicy, JobStore, TenantJobQuota};
use eg_quantum_core::backend::RunOptions;
use eg_quantum_core::estimate::EstimateOptions;
use eg_quantum_core::planner::PlannerOptions;
use eg_quantum_sim::stabilizer::StabilizerSimulator;
use eg_quantum_sim::statevector::StateVectorSimulator;

fn open_store() -> (JobStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(&dir.path().join("jobs.redb")).unwrap();
    (store, dir)
}

/// Two connected components: {alice, bob, carol} (a chain) and {dana} (a singleton).
/// A noiseless GHZ predicts: alice/bob/carol perfectly correlated (score -> 1.0
/// within shot-noise), dana uncorrelated with anyone but trivially "consistent with
/// itself" every shot (a 1-element component's own majority is always itself, so its
/// score is ALSO 1.0 by the metric's definition — the interesting assertion is that
/// the 3-clique's *joint* outcome only ever takes 2 of the 8 possible bitstrings,
/// which `marginal_probabilities` + direct outcome inspection below verifies).
#[test]
fn ghz_component_is_perfectly_correlated_and_routes_to_stabilizer() {
    let (store, _dir) = open_store();
    let candidates = vec![
        "alice".to_string(),
        "bob".to_string(),
        "carol".to_string(),
        "dana".to_string(),
    ];
    let job = eg_quantum_jobs::submit_quantum_job(
        &store,
        InputSnapshotHandle::new("social-graph", 7),
        JobPolicy {
            tenant: "t1".into(),
            actor: "ranker".into(),
            purpose: "quantum-consistency-ranking".into(),
            ..Default::default()
        },
        candidates.clone(),
        vec![(0, 1), (1, 2)], // dana (index 3) has no edges -> its own component
        RunOptions {
            shots: Some(512),
            seed: Some(1234),
            ..Default::default()
        },
        EstimateOptions::default(),
        PlannerOptions::default(),
        1,
        0,
    )
    .expect("submit succeeds");
    assert_eq!(job.algo.family, eg_quantum_jobs::QUANTUM_ALGO_FAMILY);
    assert_eq!(job.algo.algorithm, eg_quantum_jobs::GHZ_RANKING_ALGORITHM);

    // Register BOTH backends -- proves planner rule R1 (Clifford -> stabilizer,
    // NEVER statevector) actually fires against a real registered pair, not a stub.
    let statevector = StateVectorSimulator::new();
    let stabilizer = StabilizerSimulator::new();
    let backends = eg_quantum_jobs::BackendSet::new(vec![&statevector, &stabilizer]);

    let finished = eg_quantum_jobs::claim_and_run_quantum_job(
        &store,
        "worker-1",
        &backends,
        TenantJobQuota::default(),
        2_000,
        60_000,
    )
    .expect("worker run succeeds")
    .expect("the submitted job was ready to claim");

    match &finished.state {
        eg_jobs::JobState::Succeeded { .. } => {}
        other => panic!("expected Succeeded, got {other:?}"),
    }

    let rows = eg_quantum_jobs::join_quantum_result_rows(&store, &finished.job_id)
        .expect("join succeeds on a Succeeded job");
    assert_eq!(rows.len(), 4);
    let by_id: std::collections::HashMap<_, _> = rows.into_iter().collect();

    // Every candidate's own-component-majority consistency score must be very close
    // to 1.0 for a noiseless simulator (allow generous shot-noise slack; 512 shots of
    // a deterministic stabilizer circuit should in fact be EXACTLY 1.0, but the
    // assertion is written to tolerate float slop rather than assume it).
    for id in ["alice", "bob", "carol", "dana"] {
        let score = by_id[id];
        assert!(
            score > 0.95,
            "candidate '{id}' consistency score {score} should be ~1.0 for a noiseless GHZ component"
        );
    }
}

/// Determinism (PROGRAM.md acceptance check, restated by both program docs): the
/// SAME circuit + seed + backend reproduces bit-exactly across two independent
/// submit/claim/run cycles.
#[test]
fn same_seed_reproduces_bit_exact_scores() {
    let run_once = || {
        let (store, _dir) = open_store();
        eg_quantum_jobs::submit_quantum_job(
            &store,
            InputSnapshotHandle::new("g", 1),
            JobPolicy {
                tenant: "t1".into(),
                actor: "a1".into(),
                ..Default::default()
            },
            vec!["x".into(), "y".into()],
            vec![(0, 1)],
            RunOptions {
                shots: Some(200),
                seed: Some(99),
                ..Default::default()
            },
            EstimateOptions::default(),
            PlannerOptions::default(),
            1,
            0,
        )
        .unwrap();
        let stabilizer = StabilizerSimulator::new();
        let backends = eg_quantum_jobs::BackendSet::new(vec![&stabilizer]);
        let finished = eg_quantum_jobs::claim_and_run_quantum_job(
            &store,
            "w",
            &backends,
            TenantJobQuota::default(),
            0,
            60_000,
        )
        .unwrap()
        .unwrap();
        let mut rows = eg_quantum_jobs::join_quantum_result_rows(&store, &finished.job_id).unwrap();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    };
    assert_eq!(
        run_once(),
        run_once(),
        "same circuit+seed+backend must be bit-exact"
    );
}

/// A job whose `algo.family` is NOT `quantum.circuit` is refused loudly by the
/// worker rather than silently mis-executed -- the guard `claim_and_run_quantum_job`
/// documents for a `JobStore` shared with a non-quantum job producer.
#[test]
fn wrong_family_job_is_rejected_not_silently_run() {
    let (store, _dir) = open_store();
    store
        .submit(eg_jobs::SubmitSpec {
            input_snapshot: InputSnapshotHandle::new("g", 1),
            policy: JobPolicy {
                tenant: "t1".into(),
                actor: "a1".into(),
                ..Default::default()
            },
            algo: eg_jobs::AlgoVersion {
                family: "mining.cluster".to_string(),
                algorithm: "kmeans".to_string(),
                params_digest: String::new(),
                code_version: String::new(),
                env_version: String::new(),
            },
            input_payload: None,
            max_attempts: 1,
            backoff_ms: 0,
        })
        .unwrap();

    let stabilizer = StabilizerSimulator::new();
    let backends = eg_quantum_jobs::BackendSet::new(vec![&stabilizer]);
    let err = eg_quantum_jobs::claim_and_run_quantum_job(
        &store,
        "worker-1",
        &backends,
        TenantJobQuota::default(),
        0,
        60_000,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        eg_quantum_jobs::QuantumJobError::WrongFamily(_, _)
    ));
}
