// CONCEPT:EG-KG.mining.causal-impact — Interrupted time series / diff-in-differences.
//
// Pure-Rust, dependency-light: estimate the causal effect of an INTERVENTION at a
// known point in a time series.
//
//   * **Interrupted time series** (`interrupted_time_series`, `its`) — a single
//     series split at `intervention_index` into a pre- and post-period; the
//     effect is the shift in the mean level (`post_mean - pre_mean`), a standard
//     one-group ITS estimator (no counterfactual model beyond "the pre-period
//     level would have continued", the simplest and most transparent ITS form).
//   * **Difference-in-differences** (`diff_in_diff`, `did`) — a treatment AND a
//     control series, each split at the SAME `intervention_index`; the effect is
//     `(treatment_post - treatment_pre) - (control_post - control_pre)` — the
//     control's own pre→post drift is subtracted out, isolating the treatment's
//     incremental effect from any shared time trend (the classic 2×2 DiD
//     estimator).
//
// Both report a standard error (pooled within-period variance) and a two-sided
// significance `confidence = 1 - p` from a Normal approximation to the effect's
// z-statistic (the same asymptotic-Normal treatment `anomaly::z_score_mad`'s
// z-scores get elsewhere in this crate — no new statistical machinery).

use eg_numeric::detkernel::kernels::normal_sf;

/// The result of one causal-impact estimate.
#[derive(Debug, Clone, PartialEq)]
pub struct CausalEffect {
    pub pre_mean: f64,
    pub post_mean: f64,
    /// The point estimate of the causal effect (ITS: level shift; DiD: the DiD estimator).
    pub effect_size: f64,
    /// `effect_size / pre_mean` when `pre_mean != 0.0`, else `0.0` (undefined base rate).
    pub relative_effect: f64,
    pub std_error: f64,
    /// `1 - two_sided_p_value`, clamped to `[0,1]` — how confidently `effect_size`
    /// differs from zero (NOT a Bayesian posterior; a frequentist significance
    /// framed as a confidence score for the epistemic writeback, consistent with
    /// this crate's other z-score-based confidences).
    pub confidence: f64,
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

fn variance(v: &[f64], m: f64) -> f64 {
    if v.len() < 2 {
        0.0
    } else {
        v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64
    }
}

/// Standard-Normal two-sided p-value for z-statistic `z` (`P(|Z| >= |z|)`):
/// `2 * P(Z > |z|)` from the pinned `erfc`-based survival, never `1 - erf`, so a
/// far-tail p-value keeps its relative accuracy.
fn two_sided_p(z: f64) -> f64 {
    (2.0 * normal_sf(z.abs())).min(1.0)
}

fn effect_from(
    pre_mean: f64,
    post_mean: f64,
    effect_size: f64,
    pre: &[f64],
    post: &[f64],
    extra_var: f64,
) -> CausalEffect {
    let pooled_var = variance(pre, mean(pre)) / pre.len().max(1) as f64
        + variance(post, mean(post)) / post.len().max(1) as f64
        + extra_var;
    let std_error = pooled_var.sqrt();
    let z = if std_error > 0.0 {
        effect_size / std_error
    } else if effect_size != 0.0 {
        f64::INFINITY
    } else {
        0.0
    };
    let confidence = (1.0 - two_sided_p(z)).clamp(0.0, 1.0);
    let relative_effect = if pre_mean != 0.0 {
        effect_size / pre_mean
    } else {
        0.0
    };
    CausalEffect {
        pre_mean,
        post_mean,
        effect_size,
        relative_effect,
        std_error,
        confidence,
    }
}

/// Interrupted-time-series estimate: split `series` at `intervention_index` (the
/// FIRST post-intervention observation) into pre/post windows and estimate the
/// level shift. `intervention_index` is clamped into `[0, series.len()]`; either
/// window may be empty (an empty window contributes a mean/variance of `0.0`,
/// yielding a low-confidence result rather than a panic).
pub fn interrupted_time_series(series: &[f64], intervention_index: usize) -> CausalEffect {
    let idx = intervention_index.min(series.len());
    let (pre, post) = series.split_at(idx);
    let pre_mean = mean(pre);
    let post_mean = mean(post);
    effect_from(pre_mean, post_mean, post_mean - pre_mean, pre, post, 0.0)
}

/// Difference-in-differences estimate: `treatment` and `control` are each split
/// at the SAME `intervention_index`; the effect subtracts the control's own
/// pre→post drift from the treatment's, isolating the treatment-specific effect.
pub fn diff_in_diff(treatment: &[f64], control: &[f64], intervention_index: usize) -> CausalEffect {
    let t_idx = intervention_index.min(treatment.len());
    let c_idx = intervention_index.min(control.len());
    let (t_pre, t_post) = treatment.split_at(t_idx);
    let (c_pre, c_post) = control.split_at(c_idx);
    let t_pre_mean = mean(t_pre);
    let t_post_mean = mean(t_post);
    let c_pre_mean = mean(c_pre);
    let c_post_mean = mean(c_post);
    let effect = (t_post_mean - t_pre_mean) - (c_post_mean - c_pre_mean);
    // Control's variance contribution folds into the pooled std-error as extra terms.
    let extra_var = variance(c_pre, c_pre_mean) / c_pre.len().max(1) as f64
        + variance(c_post, c_post_mean) / c_post.len().max(1) as f64;
    effect_from(t_pre_mean, t_post_mean, effect, t_pre, t_post, extra_var)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn its_detects_a_clear_level_shift() {
        let series = vec![1.0, 1.1, 0.9, 1.0, 1.05, 5.0, 5.1, 4.9, 5.0, 5.05];
        let out = interrupted_time_series(&series, 5);
        assert!((out.pre_mean - 1.01).abs() < 1e-6);
        assert!((out.post_mean - 5.01).abs() < 1e-6);
        assert!((out.effect_size - 4.0).abs() < 1e-6);
        assert!(
            out.confidence > 0.9,
            "confidence {} too low for a clear shift",
            out.confidence
        );
    }

    #[test]
    fn its_flat_series_yields_near_zero_effect_and_low_confidence() {
        let series = vec![2.0; 20];
        let out = interrupted_time_series(&series, 10);
        assert!((out.effect_size).abs() < 1e-9);
        assert!(out.confidence < 0.5);
    }

    #[test]
    fn did_subtracts_shared_trend_from_control() {
        // Both series drift up by 1.0 regardless of treatment (a shared time trend);
        // the treatment ADDITIONALLY jumps by 3.0 at the intervention.
        let treatment = vec![1.0, 1.0, 1.0, 5.0, 5.0, 5.0]; // pre mean 1, post mean 5 (+4 raw)
        let control = vec![1.0, 1.0, 1.0, 2.0, 2.0, 2.0]; // pre mean 1, post mean 2 (+1 raw, the shared trend)
        let out = diff_in_diff(&treatment, &control, 3);
        // DiD effect = (5-1) - (2-1) = 3.0 (the trend-adjusted, treatment-specific effect).
        assert!((out.effect_size - 3.0).abs() < 1e-9);
    }

    /// `2 * Phi(-z)` from mpmath 1.3.0 at 50 significant digits
    /// (`mp.dps = 50; float(2 * mpmath.ncdf(-z))`), each the nearest `f64`.
    const TWO_SIDED_P_REFERENCES: [(f64, f64); 5] = [
        (3.0, 2.699_796_063_260_189e-3),
        (6.0, 1.973_175_290_075_396e-9),
        (8.0, 1.244_192_114_854_356_8e-15),
        (10.0, 1.523_970_604_832_105e-23),
        (20.0, 5.507_248_237_212_467_5e-89),
    ];

    #[test]
    fn two_sided_p_keeps_relative_accuracy_in_the_far_tail() {
        // `1 - erf(z / sqrt 2)` returns 0 (or noise at the approximation's
        // 1.5e-7 floor) out here; the erfc survival keeps every digit.
        for (z, reference) in TWO_SIDED_P_REFERENCES {
            for signed in [z, -z] {
                let relative = (two_sided_p(signed) / reference - 1.0).abs();
                assert!(relative < 1e-12, "z = {signed}: relative error {relative}");
            }
        }
        assert_eq!(two_sided_p(0.0), 1.0);
        assert_eq!(two_sided_p(f64::INFINITY), 0.0);
    }

    #[test]
    fn empty_post_window_does_not_panic() {
        let series = vec![1.0, 2.0, 3.0];
        let out = interrupted_time_series(&series, 10); // clamped to series.len()
        assert_eq!(out.post_mean, 0.0);
    }
}
