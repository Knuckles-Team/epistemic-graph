//! KAN-learned cardinality-estimate correction (CONCEPT:EG-KG.query.adaptive-reoptimization,
//! W4.12 phase 2 — "planner feedback + learned costs").
//!
//! Phase 1 (`optimizer::reoptimize_remaining`, auto-wired into
//! [`crate::exec::run_with_adaptive_reopt`]) is purely REACTIVE and purely PER-QUERY:
//! it re-costs a plan's not-yet-run tail the moment an earlier op's ACTUAL cardinality
//! blows the plan-time estimate, then forgets everything. If op X in this graph
//! systematically under/over-estimates (a hub whose fan-out the degree-AVERAGE
//! estimator can never see coming), the NEXT query pays the identical miss — and the
//! identical re-plan churn — all over again.
//!
//! This module closes that loop. [`LearnedCostStore`] holds one small KAN edge
//! function per op-kind — `eg_compute::graphlearn::edge_fn::KanEdgeFn`, the SAME
//! polynomial-basis primitive W4.2's link-predictor trains (reused, not
//! reinvented) — trained ONLINE from the identical `(estimated, actual)` pairs
//! [`crate::exec::run_with_adaptive_reopt`] already computes every op: no extra
//! instrumentation, no offline job, no new data path. [`crate::cost::ModalityCardinality::rows_out`]
//! applies the learned correction to its static estimate before returning it, so a
//! bias learned on query N nudges the ESTIMATE — not just the reactive re-plan — on
//! query N+1. That is "the learned correction adjusts future estimates" from the task
//! spec.
//!
//! ## Why a curve per op-kind, not one global correction
//!
//! A `Traverse`'s degree-average miss and a `Filter`'s selectivity miss are different
//! error DISTRIBUTIONS; lumping them into one correction would have each contaminate
//! the other's fit. [`op_kind`] buckets by the SAME per-modality split
//! [`crate::cost::ModalityCardinality::rows_out`] already uses.
//!
//! ## Why log-space, residual-around-identity
//!
//! Each curve fits `ln(actual) ≈ ln(estimated) + f(ln(estimated))` rather than
//! `actual ≈ g(estimated)` directly: log-space keeps a multiplicative bias (a
//! systematic 10× under-count) a SMOOTH, small-magnitude target regardless of scale,
//! and the RESIDUAL-around-identity form means a freshly-zeroed curve (`f ≡ 0`, every
//! coefficient `0.0`) is an EXACT no-op — flipping [`enabled`] on for the first time
//! never perturbs an as-yet-untrained op-kind's estimate ([`KanEdgeFn::zeros`]).
//!
//! ## Opt-in, two gates
//!
//! 1. **Compile-time** — the `learned-cost` cargo feature (pulls `eg-compute`'s
//!    dependency-light `graphlearn` domain). A default/Pi build links none of it.
//! 2. **Runtime** — the `EPISTEMIC_GRAPH_LEARNED_COST=1` env flag ([`enabled`], read
//!    fresh on every call, mirroring [`crate::optimizer::enabled`]'s convention). A
//!    build that compiles the capability in still ships it OFF by default: every
//!    existing planner/exec test, and every deployment that doesn't opt in, sees
//!    BYTE-FOR-BYTE the pre-existing static-estimate behavior.
//!
//! ## Queryable artifact
//!
//! [`LearnedCostStore::snapshot`] (and the free function [`artifacts`]) returns one
//! [`CostCorrectionArtifact`] per trained op-kind — the KAN basis/degree/coefficients
//! (the interpretable curve itself, exactly as `KanEdgeFn`'s own docs describe) plus a
//! sample count and the running static-vs-learned error — a plain
//! `Serialize`/`Deserialize` struct (proven by `artifact_json_roundtrip` below) ready
//! for a caller to expose however it likes. A server-side KG writeback mirroring the
//! graphlearn `:EdgeFunction` node convention (`src/server/handlers/graphlearn.rs`) is
//! the natural next step, but that write path needs a mutable `GraphCore` + protocol/
//! dispatch plumbing that lives outside eg-plan's boundary — logged as a follow-up in
//! `reports/issue-register.md` rather than built here.

use eg_compute::graphlearn::edge_fn::{Basis, KanEdgeFn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use crate::algebra::Op;

/// Runtime opt-in switch (CONCEPT:EG-KG.query.adaptive-reoptimization). Checked fresh on every
/// call — no caching — exactly like [`crate::optimizer::enabled`], so a test (or an
/// operator) can flip the env var and see the effect on the very next call. Unset, or
/// anything other than the literal `"1"`, means disabled: the capability is compiled
/// in but inert.
pub fn enabled() -> bool {
    matches!(
        std::env::var("EPISTEMIC_GRAPH_LEARNED_COST")
            .ok()
            .as_deref(),
        Some("1")
    )
}

/// Learning rate for the single-sample online gradient step [`Correction::observe`]
/// takes per `(estimated, actual)` pair. Small enough that one wild outlier cannot
/// swing the curve; large enough that a genuinely systematic miss visibly corrects
/// within the handful of observations one hot op-kind produces in real traffic.
const LEARNING_RATE: f64 = 0.05;

/// Degree of the per-op-kind Chebyshev correction curve. Deliberately small: a
/// cardinality miss is a smooth, roughly log-linear systematic bias (an under-counted
/// hub, a stale histogram), not a function needing many degrees of freedom — and a
/// low-degree curve stays well-behaved under one-sample-at-a-time SGD.
const CORRECTION_DEGREE: usize = 3;

/// Numeric floor so `ln(0)` never happens on a genuinely empty estimate/actual.
const EPS: f64 = 1e-6;

/// EWMA smoothing for the running error numbers [`CostCorrectionArtifact`] reports —
/// recent samples dominate (so the artifact reflects the curve's CURRENT fit) without
/// a single outlier swamping it.
const EWMA_ALPHA: f64 = 0.2;

/// One op-kind's learned correction curve plus the bookkeeping
/// [`CostCorrectionArtifact`] reports (CONCEPT:EG-KG.query.adaptive-reoptimization). See the
/// module doc for why log-space + residual-around-identity.
#[derive(Clone, Debug)]
struct Correction {
    kan: KanEdgeFn,
    samples: u64,
    /// Running EWMA of `|ln(actual) - ln(estimated)|` — the STATIC model's error,
    /// unaffected by learning (the "A" side of the A/B comparison).
    ewma_static_abs_err: f64,
    /// Running EWMA of `|ln(actual) - (ln(estimated) + kan.eval(ln(estimated)))|`,
    /// evaluated with the curve AS IT STOOD just before that sample corrected it —
    /// the "B" side.
    ewma_learned_abs_err: f64,
}

impl Correction {
    fn new() -> Self {
        Self {
            kan: KanEdgeFn::zeros(Basis::Chebyshev, CORRECTION_DEGREE),
            samples: 0,
            ewma_static_abs_err: 0.0,
            ewma_learned_abs_err: 0.0,
        }
    }

    /// Apply the learned residual to a raw estimate: `exp(x + f(x))` with
    /// `x = ln(estimated)`. Clamped to a bounded multiplicative window around the
    /// input (`[estimate * 1e-4, estimate * 1e4]`) so a still-training (or
    /// pathologically fit) curve can never hand a downstream permutation search a
    /// wildly-off-scale number — the correction can move an estimate, never blow it
    /// up or collapse it to zero.
    fn correct(&self, estimated: f64) -> f64 {
        let est = estimated.max(EPS);
        let x = est.ln();
        let y = x + self.kan.eval(x);
        y.exp().clamp(est * 1e-4, est * 1e4)
    }

    /// One online SGD step from a fresh `(estimated, actual)` observation — squared
    /// log-error loss `L = (y - (x + f(x)))^2`, so `dL/dcoeffs[k] = 2·residual·B_k(u)`
    /// where `residual = (x + f(x)) - y` and `B_k(u)` is exactly
    /// [`KanEdgeFn::grad_coeffs`] (its own doc: `∂f/∂coeffs[k] = B_k(u)`). Returns the
    /// `(static_abs_err, learned_abs_err)` pair for THIS sample (using the curve as it
    /// stood before this step), the A/B harness's per-sample numbers.
    fn observe(&mut self, estimated: f64, actual: f64) -> (f64, f64) {
        let est = estimated.max(EPS);
        let act = actual.max(EPS);
        let x = est.ln();
        let y = act.ln();

        let static_err = (y - x).abs();
        let learned_pred = x + self.kan.eval(x);
        let learned_err = (y - learned_pred).abs();

        let residual = learned_pred - y;
        let grad = self.kan.grad_coeffs(x);
        for (c, g) in self.kan.coeffs.iter_mut().zip(grad.iter()) {
            *c -= LEARNING_RATE * residual * g;
        }

        self.samples += 1;
        let a = if self.samples == 1 { 1.0 } else { EWMA_ALPHA };
        self.ewma_static_abs_err = a * static_err + (1.0 - a) * self.ewma_static_abs_err;
        self.ewma_learned_abs_err = a * learned_err + (1.0 - a) * self.ewma_learned_abs_err;

        (static_err, learned_err)
    }
}

/// The op-kind tag [`LearnedCostStore`] buckets corrections by
/// (CONCEPT:EG-KG.query.adaptive-reoptimization) — mirrors EXACTLY the per-modality split
/// [`crate::cost::ModalityCardinality::rows_out`] already draws (same ops, same feature
/// gates), so a curve only ever trains on the modality it names. `None` for an op this
/// tier has no real cardinality MODEL for (`Limit`'s output is exact arithmetic —
/// `in_card.min(k)` — nothing to learn; every other passthrough op in `rows_out`'s
/// wildcard arm) — the caller skips learning for those rather than fitting noise.
pub fn op_kind(op: &Op) -> Option<&'static str> {
    match op {
        Op::Scan { .. } => Some("Scan"),
        Op::Filter { .. } => Some("Filter"),
        Op::Traverse { .. } => Some("Traverse"),
        Op::Rank { .. } | Op::RankEmbed { .. } => Some("Rank"),
        Op::AsOf { .. } => Some("AsOf"),
        #[cfg(feature = "owl")]
        Op::Reason { .. } => Some("Reason"),
        #[cfg(feature = "text")]
        Op::FuseRrf { .. } => Some("FuseRrf"),
        #[cfg(feature = "text")]
        Op::RankText { .. } => Some("RankText"),
        _ => None,
    }
}

/// A registry of per-op-kind [`Correction`] curves (CONCEPT:EG-KG.query.adaptive-reoptimization).
/// Production code reaches the single process-wide instance via [`Self::global`];
/// tests and standalone tools construct their OWN isolated instance via [`Self::new`]
/// so concurrent `cargo test` runs never share (and therefore never contaminate) each
/// other's learned state.
pub struct LearnedCostStore {
    corrections: RwLock<HashMap<String, Correction>>,
}

impl Default for LearnedCostStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LearnedCostStore {
    /// A fresh, empty, independent store — use this in tests/tools; production code
    /// wants [`Self::global`] so learning persists across queries.
    pub fn new() -> Self {
        Self {
            corrections: RwLock::new(HashMap::new()),
        }
    }

    /// The single process-wide instance [`crate::cost::ModalityCardinality::rows_out`]
    /// and [`crate::exec::run_with_adaptive_reopt`] read/write when [`enabled`] is
    /// true. Deliberately NOT scoped to a [`crate::exec::PlanCtx`]/[`crate::cost::PlanStats`]
    /// snapshot (unlike `PlanStats`'s per-view memo): the whole point of this module is
    /// that a miss learned on query N adjusts query N+1's estimate, and a fresh
    /// `PlanCtx`/`GraphView` is built per query / per committed write — tying the store
    /// to either would silently reset learning on every one.
    pub fn global() -> &'static LearnedCostStore {
        static STORE: OnceLock<LearnedCostStore> = OnceLock::new();
        STORE.get_or_init(LearnedCostStore::new)
    }

    /// Apply `op_kind`'s learned correction to a raw estimate. An op-kind with no
    /// observations yet returns `estimated` unchanged (an untrained curve IS the
    /// identity per [`Correction::new`]'s zero-init, but skipping the lookup entirely
    /// for a never-seen key avoids allocating an entry just to compute a no-op).
    pub fn correct(&self, op_kind: &str, estimated: f64) -> f64 {
        let guard = self
            .corrections
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.get(op_kind) {
            Some(c) => c.correct(estimated),
            None => estimated,
        }
    }

    /// Record one `(estimated, actual)` observation for `op_kind` and take one online
    /// gradient step, creating the curve on first sight. Returns the
    /// `(static_abs_log_error, learned_abs_log_error)` pair for this sample — see
    /// [`Correction::observe`].
    pub fn observe(&self, op_kind: &str, estimated: f64, actual: f64) -> (f64, f64) {
        let mut guard = self
            .corrections
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = guard
            .entry(op_kind.to_string())
            .or_insert_with(Correction::new);
        entry.observe(estimated, actual)
    }

    /// The queryable-artifact surface (CONCEPT:EG-KG.query.adaptive-reoptimization) — one
    /// [`CostCorrectionArtifact`] per op-kind observed so far, in no particular order.
    /// See the module doc's "Queryable artifact" section.
    pub fn snapshot(&self) -> Vec<CostCorrectionArtifact> {
        let guard = self
            .corrections
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .iter()
            .map(|(kind, c)| CostCorrectionArtifact {
                op_kind: kind.clone(),
                basis: c.kan.basis,
                degree: c.kan.degree,
                coeffs: c.kan.coeffs.clone(),
                samples: c.samples,
                mean_abs_log_error_static: c.ewma_static_abs_err,
                mean_abs_log_error_learned: c.ewma_learned_abs_err,
            })
            .collect()
    }
}

/// One op-kind's learned correction curve, serialized for external inspection
/// (CONCEPT:EG-KG.query.adaptive-reoptimization) — "stored as queryable artifacts" per the task
/// spec. Plain data (`Serialize`/`Deserialize`), so any caller — a test, a future MCP
/// tool, a server-side KG writeback mirroring the graphlearn `:EdgeFunction`
/// convention — can inspect *why* a cardinality estimate for this op-kind now differs
/// from the static model: `coeffs` (with `basis`/`degree`) IS the interpretable curve,
/// exactly as with `KanEdgeFn` itself, and `mean_abs_log_error_{static,learned}` is
/// the running evidence that it helps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CostCorrectionArtifact {
    /// The [`Op`] discriminant this curve corrects (`"Filter"`, `"Traverse"`, …) — see
    /// [`op_kind`].
    pub op_kind: String,
    pub basis: Basis,
    pub degree: usize,
    /// The learned basis coefficients — the interpretable curve itself.
    pub coeffs: Vec<f64>,
    /// Observations folded into this curve so far.
    pub samples: u64,
    /// Running EWMA of the STATIC model's `|ln(actual) - ln(estimated)|` — unaffected
    /// by learning, the "A" side of the A/B comparison.
    pub mean_abs_log_error_static: f64,
    /// Running EWMA of the LEARNED (KAN-corrected) estimate's absolute log-error,
    /// each sample scored against the curve as it stood just before that sample
    /// updated it — the "B" side.
    pub mean_abs_log_error_learned: f64,
}

/// The process-wide store's queryable-artifact surface (CONCEPT:EG-KG.query.adaptive-reoptimization)
/// — `LearnedCostStore::global().snapshot()`. See the module doc's "Queryable
/// artifact" section.
pub fn artifacts() -> Vec<CostCorrectionArtifact> {
    LearnedCostStore::global().snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A freshly-observed op-kind's FIRST correction (before any training) is the
    /// identity: `KanEdgeFn::zeros` means `f ≡ 0`, so `correct` returns (within
    /// floating-point round-trip through `ln`/`exp`) exactly the input estimate. This
    /// is the property that makes flipping [`enabled`] on for the first time safe —
    /// no existing plan's estimates jump on the very first query.
    #[test]
    fn untrained_curve_is_identity() {
        let store = LearnedCostStore::new();
        // No `observe` yet: `op_kind` isn't even in the map, so `correct` short-circuits
        // to the raw estimate.
        assert_eq!(store.correct("Traverse", 250.0), 250.0);

        // After ONE `observe` (which creates the zero-init curve and takes one gradient
        // step), a FRESH estimate this curve has never seen is still very close to
        // identity — the single step nudges the curve toward the ONE sample it just
        // saw, not toward a generic input.
        store.observe("Traverse", 250.0, 250.0); // no miss at all: gradient is ~0.
        let corrected = store.correct("Traverse", 999.0);
        assert!(
            (corrected - 999.0).abs() < 1.0,
            "an on-target observation must not perturb the curve: got {corrected}"
        );
    }

    /// The core claim: online training on a SYSTEMATIC miss (here, actual is always
    /// ~8x the estimate — a stand-in for a hub whose real fan-out the degree-average
    /// estimator can't see) measurably reduces the learned arm's error relative to the
    /// static arm, and the learned CORRECTION converges toward actually predicting the
    /// bias on held-out estimates the curve was never directly trained on.
    #[test]
    fn training_reduces_error_on_a_systematic_bias() {
        let store = LearnedCostStore::new();
        const SKEW: f64 = 8.0;

        // Deterministic LCG jitter (no `rand` dep) so estimates vary run to run within
        // the training loop without becoming nondeterministic across test runs.
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut next = move || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((state >> 33) as f64 / u32::MAX as f64) * 0.4 + 0.8 // in [0.8, 1.2)
        };

        let mut static_errs = Vec::new();
        let mut learned_errs = Vec::new();
        for i in 0..300 {
            let estimated = 50.0 * next() * (1.0 + (i % 7) as f64 * 0.1);
            let actual = estimated * SKEW * next();
            let (s, l) = store.observe("Traverse", estimated, actual);
            static_errs.push(s);
            learned_errs.push(l);
        }

        // Compare the LAST THIRD (curve warmed up) against the static baseline over
        // the SAME window — an honest, non-cherry-picked comparison window.
        let last_third = static_errs.len() * 2 / 3;
        let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
        let static_mean = mean(&static_errs[last_third..]);
        let learned_mean = mean(&learned_errs[last_third..]);
        println!(
            "training_reduces_error_on_a_systematic_bias: static_mean_abs_log_err={static_mean:.4} \
             learned_mean_abs_log_err={learned_mean:.4} (n={})",
            static_errs.len() - last_third
        );
        assert!(
            learned_mean < static_mean * 0.5,
            "learned correction must at least halve the static model's log-error on a \
             systematic bias: static={static_mean:.4} learned={learned_mean:.4}"
        );

        // Held-out check: a fresh estimate the curve was never directly `observe`d on
        // must land close to the TRUE skewed value, not the raw (unskewed) estimate.
        let holdout_estimate = 500.0;
        let corrected = store.correct("Traverse", holdout_estimate);
        let true_actual = holdout_estimate * SKEW;
        let rel_err_learned = (corrected - true_actual).abs() / true_actual;
        let rel_err_static = (holdout_estimate - true_actual).abs() / true_actual;
        assert!(
            rel_err_learned < rel_err_static * 0.5,
            "held-out corrected estimate ({corrected:.1}) must be much closer to the \
             true skewed value ({true_actual:.1}) than the raw estimate \
             ({holdout_estimate:.1}) was: rel_err_learned={rel_err_learned:.4} \
             rel_err_static={rel_err_static:.4}"
        );
    }

    /// An op-kind this tier assigns no cardinality model to (`Limit` — exact
    /// arithmetic) is never bucketed, so it can never absorb noise from an unrelated
    /// modality's miss.
    #[test]
    fn limit_has_no_learned_op_kind() {
        assert_eq!(op_kind(&Op::Limit { k: 5 }), None);
        assert_eq!(
            op_kind(&Op::Traverse {
                rel: "R".into(),
                min: 1,
                max: 1
            }),
            Some("Traverse")
        );
        assert_eq!(op_kind(&Op::Scan { label: "L".into() }), Some("Scan"));
    }

    /// The queryable-artifact surface is genuinely inspectable: it round-trips through
    /// JSON byte-for-byte (mirrors `KanEdgeFn`'s own `edge_fn_serde_roundtrip`), and
    /// carries the interpretable coefficients directly.
    #[test]
    fn artifact_json_roundtrip() {
        let store = LearnedCostStore::new();
        store.observe("Filter", 100.0, 40.0);
        store.observe("Filter", 120.0, 45.0);
        store.observe("Traverse", 2.0, 500.0);

        let snap = store.snapshot();
        assert_eq!(snap.len(), 2, "one artifact per distinct op-kind observed");

        let json = serde_json::to_string(&snap).expect("artifact must serialize");
        let back: Vec<CostCorrectionArtifact> =
            serde_json::from_str(&json).expect("artifact must deserialize");
        assert_eq!(back.len(), snap.len());
        for (a, b) in snap.iter().zip(back.iter()) {
            assert_eq!(a.op_kind, b.op_kind);
            assert_eq!(a.coeffs, b.coeffs);
            assert_eq!(a.samples, b.samples);
            assert_eq!(a.degree, b.degree);
        }

        let filter_artifact = snap.iter().find(|a| a.op_kind == "Filter").unwrap();
        assert_eq!(filter_artifact.samples, 2);
        assert_eq!(filter_artifact.degree, CORRECTION_DEGREE);
    }

    /// [`enabled`] defaults OFF (relies on the var being unset in the test env — the
    /// CI default, the same convention `runtime::tests::config_defaults_to_serial`
    /// documents for its own env-gated default).
    #[test]
    fn learned_cost_defaults_disabled() {
        if std::env::var_os("EPISTEMIC_GRAPH_LEARNED_COST").is_none() {
            assert!(!enabled());
        }
    }

    /// The process-wide singleton is a real `'static` singleton (two calls to
    /// `global()` observe each other's writes) — proves [`LearnedCostStore::global`]
    /// is not accidentally handing out a fresh store each time.
    #[test]
    fn global_store_is_shared() {
        // Use an op-kind unlikely to collide with another test's use of the SAME
        // process-wide singleton (tests run in threads within one process).
        let key = "AsOf";
        let before = LearnedCostStore::global().snapshot();
        let before_samples = before
            .iter()
            .find(|a| a.op_kind == key)
            .map(|a| a.samples)
            .unwrap_or(0);
        LearnedCostStore::global().observe(key, 10.0, 10.0);
        let after_samples = LearnedCostStore::global()
            .snapshot()
            .into_iter()
            .find(|a| a.op_kind == key)
            .map(|a| a.samples)
            .unwrap_or(0);
        assert!(
            after_samples > before_samples,
            "global() must return the SAME store across calls: before={before_samples} after={after_samples}"
        );
    }
}
