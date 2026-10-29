//! MEDULLA-DECISION-ENGINE-R803.6: decide over eight candidate component versions
//! with two hundred stored contrasts in the evidence. JSON on stdout.
//! Budget: p99 5 ms in the optimized profile.
//! Run with `cargo bench -p eg-numeric --features decision --bench harness_contrast`.

use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::time::Instant;

use eg_types::decision::harness_contrast::{decide_contrast, ContrastCandidate, ContrastEvidence};
use eg_types::orchestration::improvement::{HarnessContrast, HarnessProbeRun};

const CONDITIONS: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const TASK_REF: &str = "task:deploy";
const TASK_CLASS: &str = "deploy";
const BASELINE: &str = "trace:baseline";
const SAMPLES: usize = 2_000;

fn contrast(i: u32, eval: i64, cost: i64) -> HarnessContrast {
    HarnessContrast {
        task_ref: TASK_REF.into(),
        conditions_digest: CONDITIONS.into(),
        run_a_trace_id: BASELINE.into(),
        run_b_trace_id: format!("trace:candidate-{i}"),
        retry_delta: (i as i64 - 4) << 32,
        tool_selection_delta: (i as i64 - 4) << 30,
        eval_score_delta: eval,
        cost_delta: cost,
    }
}

fn main() {
    // Eight candidates: one clearly improving, one neutral, one worse, and
    // five in between; every candidate probed under the identical conditions.
    let evals: [i64; 8] = [
        2 << 32,
        0,
        -(1 << 32),
        1 << 30,
        -(1 << 30),
        1 << 32,
        0,
        -(1 << 31),
    ];
    let deltas: Vec<HarnessContrast> = (0..8)
        .map(|i: usize| contrast(i as u32, evals[i], (i as i64 - 3) << 30))
        .collect();
    // Two hundred stored contrasts: the eight the candidates cite, matched in
    // full, and one hundred ninety-two of unrelated run pairs behind them.
    let mut stored_contrasts: BTreeMap<String, HarnessContrast> = BTreeMap::new();
    for delta in &deltas {
        stored_contrasts.insert(delta.node_id(), delta.clone());
    }
    for i in 0..192 {
        let noise = HarnessContrast {
            task_ref: format!("task:other-{i}"),
            conditions_digest: format!("sha256:{i:064x}"),
            run_a_trace_id: format!("trace:noise-a-{i}"),
            run_b_trace_id: format!("trace:noise-b-{i}"),
            retry_delta: (i as i64 % 7) << 32,
            tool_selection_delta: (i as i64 % 5) << 30,
            eval_score_delta: (i as i64 % 9) << 32,
            cost_delta: (i as i64 % 3) << 30,
        };
        stored_contrasts.insert(noise.node_id(), noise);
    }
    assert_eq!(stored_contrasts.len(), 200);
    let held_versions: BTreeSet<String> = (0..8)
        .map(|i| format!("harness-component:prompt:digest-{i:016x}"))
        .collect();
    let mut probe_runs: BTreeMap<String, HarnessProbeRun> = BTreeMap::new();
    probe_runs.insert(
        BASELINE.to_string(),
        HarnessProbeRun {
            trace_id: BASELINE.into(),
            conditions_digest: CONDITIONS.into(),
        },
    );
    for i in 0..8 {
        let trace = format!("trace:candidate-{i}");
        probe_runs.insert(
            trace.clone(),
            HarnessProbeRun {
                trace_id: trace,
                conditions_digest: CONDITIONS.into(),
            },
        );
    }
    let evidence = ContrastEvidence {
        held_versions: &held_versions,
        probe_runs: &probe_runs,
        stored_contrasts: &stored_contrasts,
    };
    let version_refs: Vec<String> = (0..8)
        .map(|i| format!("harness-component:prompt:digest-{i:016x}"))
        .collect();
    let mut micros = Vec::with_capacity(SAMPLES);
    let mut adopted = String::new();
    for _ in 0..SAMPLES {
        let candidates: Vec<ContrastCandidate> = deltas
            .iter()
            .zip(version_refs.iter())
            .map(|(delta, version_ref)| ContrastCandidate {
                version_ref,
                probe_trace_id: Some(delta.run_b_trace_id.as_str()),
                deltas: delta,
            })
            .collect();
        let started = Instant::now();
        let decision = black_box(decide_contrast(
            TASK_REF,
            TASK_CLASS,
            CONDITIONS,
            BASELINE,
            &candidates,
            &evidence,
        ))
        .unwrap();
        micros.push(started.elapsed().as_micros());
        adopted = decision.adopted_version_ref.clone().unwrap_or_default();
    }
    micros.sort_unstable();
    let (p50, p99) = (micros[SAMPLES / 2], micros[SAMPLES * 99 / 100]);
    println!(
        "{{\"bench\":\"harness_contrast\",\"candidates\":8,\"stored_contrasts\":200,\
         \"adopted\":\"{adopted}\",\"p50_us\":{p50},\"p99_us\":{p99},\
         \"budget_p99_us\":5000,\"optimized\":{}}}",
        !cfg!(debug_assertions)
    );
    if !cfg!(debug_assertions) {
        assert!(p99 <= 5_000, "the contrast decision exceeded its budget");
    }
}
