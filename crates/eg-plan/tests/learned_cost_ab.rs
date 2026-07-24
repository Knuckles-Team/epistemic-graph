//! KAN-learned cardinality-correction A/B harness (CONCEPT:EG-KG.query.adaptive-reoptimization,
//! W4.12 phase 2 — "planner feedback + learned costs").
//!
//! The whole point of `crate::learned_cost` is that a systematic estimation bias
//! learned from PAST `(estimated, actual)` pairs should make FUTURE estimates better
//! than the static model alone. This harness proves that quantitatively: it simulates
//! a realistic skewed workload (a `Traverse` estimator that consistently under-shoots
//! by a fixed multiplicative factor with per-sample noise — the same SHAPE the
//! hub-fan-out fixture in `src/tests.rs` and `benches/adaptive_reopt.rs` exercises
//! end-to-end, but driven numerically here so the comparison is exact and fast), feeds
//! it through [`eg_plan::LearnedCostStore`] one observation at a time (exactly how
//! `exec::run_with_adaptive_reopt` feeds it in production), and compares the STATIC
//! arm's estimate error against the LEARNED (KAN-corrected) arm's — the A/B the task
//! spec asks for, with the numbers printed (`cargo test --features learned-cost
//! learned_cost_ab -- --nocapture`) as well as asserted.
//!
//! `#![cfg(feature = "learned-cost")]` below means this whole file compiles to nothing
//! without the feature — a default `cargo test -p eg-plan` builds no eg-compute here
//! and this binary is empty, matching every other optional-feature surface in this
//! crate.

#![cfg(feature = "learned-cost")]

use eg_plan::{learned_cost_artifacts, CostCorrectionArtifact, LearnedCostStore};

/// Deterministic LCG (no `rand` dep, matching the convention `benches/hybrid_queries.rs`
/// and `src/learned_cost.rs`'s own tests already use) — a unit-ish multiplier in
/// `[0.8, 1.2)` so the simulated workload has realistic per-sample noise without the
/// comparison becoming nondeterministic across runs.
struct Lcg(u64);
impl Lcg {
    fn next_jitter(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((self.0 >> 33) as f64 / u32::MAX as f64) * 0.4 + 0.8
    }
}

/// The core A/B claim: online-trained per-sample, the LEARNED arm's mean absolute
/// log-error against a systematically skewed `Traverse` workload ends up well below
/// the STATIC arm's — and the numbers are printed for inspection, not just asserted.
#[test]
fn ab_harness_learned_beats_static_on_skewed_traversal() {
    let store = LearnedCostStore::new();
    const OP_KIND: &str = "Traverse";
    // A hub whose real fan-out the graph's global degree-average estimator
    // structurally cannot see: the same order of magnitude as `src/tests.rs`'s
    // hub/reached fixture (a 500-node fan-out against a ~0.5 global average).
    const SKEW: f64 = 12.0;
    const N: usize = 400;

    let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);
    let mut static_errs = Vec::with_capacity(N);
    let mut learned_errs = Vec::with_capacity(N);
    for i in 0..N {
        // The plan-time estimate a degree-average `Traverse` estimator would produce
        // for slightly different queries against similar hubs — small, and varying a
        // little query to query.
        let estimated = 20.0 * (1.0 + (i % 5) as f64 * 0.2) * rng.next_jitter();
        // The REAL fan-out: consistently ~SKEW times the estimate, with its own noise.
        let actual = estimated * SKEW * rng.next_jitter();
        let (static_err, learned_err) = store.observe(OP_KIND, estimated, actual);
        static_errs.push(static_err);
        learned_errs.push(learned_err);
    }

    // Compare the LAST HALF (curve warmed up) against the static baseline over the
    // SAME window — an honest, non-cherry-picked comparison, not "best epoch vs worst".
    let warm = N / 2;
    let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
    let static_mean = mean(&static_errs[warm..]);
    let learned_mean = mean(&learned_errs[warm..]);
    let improvement_pct = (1.0 - learned_mean / static_mean) * 100.0;

    println!("=== learned-cost A/B harness — op_kind={OP_KIND} samples={N} skew={SKEW}x ===");
    println!("  static  (raw estimate)   mean |ln(actual) - ln(estimate)| = {static_mean:.4}");
    println!("  learned (KAN-corrected)  mean |ln(actual) - ln(estimate)| = {learned_mean:.4}");
    println!("  improvement = {improvement_pct:.1}%");

    assert!(
        learned_mean < static_mean * 0.5,
        "the learned arm must at least halve the static arm's log-error on a genuinely \
         systematic skew: static={static_mean:.4} learned={learned_mean:.4}"
    );

    // Held-out generalization: a fresh estimate this exact value was never `observe`d
    // on must still land close to the true skewed value.
    let holdout_estimate = 33.0;
    let corrected = store.correct(OP_KIND, holdout_estimate);
    let true_actual = holdout_estimate * SKEW;
    let rel_err_learned = (corrected - true_actual).abs() / true_actual;
    let rel_err_static = (holdout_estimate - true_actual).abs() / true_actual;
    println!(
        "  held-out: estimate={holdout_estimate:.1} true={true_actual:.1} \
         static_pred={holdout_estimate:.1} (rel_err={rel_err_static:.3}) \
         learned_pred={corrected:.1} (rel_err={rel_err_learned:.3})"
    );
    assert!(
        rel_err_learned < rel_err_static * 0.5,
        "held-out corrected estimate must be much closer to the true skewed value than \
         the raw estimate: rel_err_learned={rel_err_learned:.4} rel_err_static={rel_err_static:.4}"
    );

    // The queryable-artifact surface: the SAME store's snapshot carries exactly the
    // numbers just printed above, serializable for external inspection.
    let snap = store.snapshot();
    let artifact = snap
        .iter()
        .find(|a| a.op_kind == OP_KIND)
        .expect("Traverse artifact must exist after N observations");
    assert_eq!(artifact.samples, N as u64);
    println!(
        "  artifact: op_kind={} basis={:?} degree={} coeffs={:?} samples={}",
        artifact.op_kind, artifact.basis, artifact.degree, artifact.coeffs, artifact.samples
    );

    let json = serde_json::to_string_pretty(&snap).expect("artifact must serialize");
    let back: Vec<CostCorrectionArtifact> =
        serde_json::from_str(&json).expect("artifact must deserialize");
    assert_eq!(
        back.len(),
        snap.len(),
        "artifact snapshot must round-trip through JSON"
    );
}

/// The crate-root free function a caller OUTSIDE `eg_plan::learned_cost` actually
/// reaches ([`learned_cost_artifacts`]) reflects the SAME process-wide singleton
/// [`exec::run_with_adaptive_reopt`]/[`cost::ModalityCardinality::rows_out`] read and
/// write when the runtime flag is on — proven here by observing it grow after a direct
/// `LearnedCostStore::global()` write, without going through a real `Plan::execute`.
#[test]
fn global_artifacts_surface_reflects_global_store() {
    // A key unlikely to collide with another test's use of this SAME process-wide
    // singleton (`cargo test` runs tests in threads within one process).
    let key = "AsOf";
    let before = learned_cost_artifacts()
        .into_iter()
        .find(|a| a.op_kind == key)
        .map(|a| a.samples)
        .unwrap_or(0);
    LearnedCostStore::global().observe(key, 40.0, 44.0);
    let after = learned_cost_artifacts()
        .into_iter()
        .find(|a| a.op_kind == key)
        .map(|a| a.samples)
        .unwrap_or(0);
    assert!(
        after > before,
        "learned_cost_artifacts() must reflect LearnedCostStore::global()'s state: before={before} after={after}"
    );
}
