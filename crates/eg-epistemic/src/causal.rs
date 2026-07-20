//! CONCEPT:EG-KG.epistemic.causal-reasoning — EPI-P3-3: calibrated causal reasoning.
//!
//! A linear-Gaussian **structural causal model** (SCM, Pearl's `M = (U, V, F)`) over
//! named variables: each variable `X = bias + Σ weight·parent + noise`, `noise ~
//! N(0, noise_var)` independent across variables. Small, dependency-light
//! (`eg_types::Distribution` is the only import — already an unconditional
//! dependency of this crate), and — crucially — it computes the joint distribution
//! **exactly** via linear algebra over the DAG's topological order, not by sampling.
//!
//! This module answers the three causal-reasoning questions [`crate::propagate`]'s
//! forward-only support/attack walk cannot:
//!
//! * [`CausalGraph::observe`] — the *observational* query `P(Y | X=x)`: ordinary
//!   multivariate-Gaussian conditioning on the **unmutilated** joint. Evidence about
//!   `X` updates belief about `X`'s ancestors too (backward inference), which is
//!   exactly the mechanism through which a confounder biases a naive
//!   "condition on the evidence" read of a causal question.
//! * [`CausalGraph::intervene`] — the *interventional* query `P(Y | do(X=x))`:
//!   genuine do-calculus via **graph surgery** (Pearl, *Causality*, ch. 3) — the
//!   `do` variables' incoming edges are CUT (not conditioned on) before the joint is
//!   recomputed, so no information flows backward through them. This is the
//!   textbook "mutilated graph" construction, not a re-labelled conditional.
//! * [`CausalGraph::counterfactual`] — Pearl's three-step abduction/action/prediction
//!   recipe for a single, FULLY-observed unit: infer each variable's exogenous noise
//!   from what actually happened (abduction), apply the intervention to the
//!   structural equations (action), then replay forward with the SAME inferred
//!   noise (prediction) to get "what `Y` would have been had `X` been `x'`".
//!
//! Every query result carries a calibrated interval
//! ([`CausalEstimate::interval`]) via `Distribution::Gaussian{..}.credible_interval`
//! — the SAME interval primitive [`crate::model::Calibration`] uses for the belief-
//! propagation core, so "calibrated" means the same thing on both sides of this
//! crate.
//!
//! Feature-gated behind `epistemic-causal` (default OFF, no new dependency —
//! see the crate `Cargo.toml`).

use std::collections::HashMap;

use eg_types::Distribution;

/// Default credible-mass for a [`CausalEstimate::interval`] — mirrors
/// `crate::propagate`'s `DEFAULT_CALIBRATION_LEVEL`.
const DEFAULT_CAUSAL_LEVEL: f64 = 0.95;

/// One variable's structural equation: `X = bias + Σ weight·parent + noise`.
#[derive(Clone, Debug, PartialEq)]
pub struct StructuralEquation {
    /// `(parent id, weight)` — MUST all already be variables in the graph (enforced
    /// by [`CausalGraph::add_variable`], which is how acyclicity + a valid
    /// topological order are guaranteed by construction).
    pub parents: Vec<(String, f64)>,
    pub bias: f64,
    /// Variance of this variable's own exogenous noise term. `0.0` = deterministic
    /// given its parents (used internally by [`CausalGraph::intervene`]'s graph
    /// surgery to fix a `do`-variable to an exact value).
    pub noise_var: f64,
}

/// A linear-Gaussian structural causal model: a DAG of [`StructuralEquation`]s in
/// topological (insertion) order.
#[derive(Clone, Debug, Default)]
pub struct CausalGraph {
    equations: HashMap<String, StructuralEquation>,
    /// Insertion order == a valid topological order (parents always precede
    /// children — enforced at `add_variable` time).
    order: Vec<String>,
}

/// A calibrated result of one causal query: the estimated mean, its variance, and
/// the Gaussian-moment-matched credible interval around it (EPI-P3-3).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CausalEstimate {
    pub mean: f64,
    pub variance: f64,
    /// Central credible interval at `level` (constructed via
    /// `Distribution::Gaussian{mean, std}.credible_interval(level)` — the same
    /// primitive [`crate::model::Calibration`] uses).
    pub interval: (f64, f64),
    pub level: f64,
}

impl CausalEstimate {
    fn from_moments(mean: f64, variance: f64, level: f64) -> Self {
        let std = variance.max(0.0).sqrt();
        let interval = Distribution::Gaussian { mean, std }.credible_interval(level);
        CausalEstimate {
            mean,
            variance,
            interval,
            level,
        }
    }
}

impl CausalGraph {
    pub fn new() -> Self {
        CausalGraph::default()
    }

    /// Add one variable. `parents` must already be present in the graph (checked
    /// here), so a graph built purely through this method is acyclic by
    /// construction and `self.order` is always a valid topological order —
    /// exactly the invariant [`Self::joint`]'s single forward pass relies on.
    pub fn add_variable(
        &mut self,
        id: impl Into<String>,
        parents: Vec<(&str, f64)>,
        bias: f64,
        noise_var: f64,
    ) -> Result<(), String> {
        let id = id.into();
        if self.equations.contains_key(&id) {
            return Err(format!("variable '{id}' already defined"));
        }
        if noise_var < 0.0 {
            return Err(format!("variable '{id}': noise_var must be >= 0"));
        }
        let mut owned_parents = Vec::with_capacity(parents.len());
        for (p, w) in parents {
            if !self.equations.contains_key(p) {
                return Err(format!(
                    "variable '{id}': parent '{p}' is not yet defined — variables \
                     must be added in topological order (parents before children)"
                ));
            }
            owned_parents.push((p.to_string(), w));
        }
        self.equations.insert(
            id.clone(),
            StructuralEquation {
                parents: owned_parents,
                bias,
                noise_var,
            },
        );
        self.order.push(id);
        Ok(())
    }

    pub fn contains(&self, id: &str) -> bool {
        self.equations.contains_key(id)
    }

    fn require(&self, id: &str) -> Result<(), String> {
        if self.contains(id) {
            Ok(())
        } else {
            Err(format!("unknown variable '{id}'"))
        }
    }

    /// The exact joint mean vector + covariance matrix over ALL variables, in
    /// `self.order`. Computed by a single forward pass in topological order using
    /// the closed-form linear-SCM recursion (no matrix inversion needed for the
    /// joint itself — see module docs):
    ///
    /// `mean[i] = bias_i + Σ_p w_ip · mean[p]`
    /// `Cov(i, k) = Σ_p w_ip · Cov(p, k)` for `k` processed strictly before `i`
    /// `Var(i) = Σ_p w_ip · Cov(i, p) + noise_var_i`
    ///
    /// (valid because each variable's own exogenous noise is independent of every
    /// variable that does not causally depend on it, and topological order
    /// guarantees no such dependency exists among already-processed variables).
    fn joint(&self) -> (Vec<f64>, Vec<Vec<f64>>, HashMap<String, usize>) {
        let n = self.order.len();
        let idx: HashMap<String, usize> = self
            .order
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i))
            .collect();
        let mut mean = vec![0.0_f64; n];
        let mut cov = vec![vec![0.0_f64; n]; n];

        for (i, id) in self.order.iter().enumerate() {
            let eq = &self.equations[id];
            let parent_idx: Vec<(usize, f64)> =
                eq.parents.iter().map(|(p, w)| (idx[p], *w)).collect();

            mean[i] = eq.bias + parent_idx.iter().map(|(p, w)| w * mean[*p]).sum::<f64>();

            // Cov(i, k) for every already-known k < i (both directions written —
            // the matrix stays symmetric by construction since Cov(i,k)==Cov(k,i)).
            let cross_cov: Vec<f64> = (0..i)
                .map(|k| parent_idx.iter().map(|(p, w)| w * cov[*p][k]).sum())
                .collect();
            for (k, &c) in cross_cov.iter().enumerate() {
                cov[i][k] = c;
                cov[k][i] = c;
            }
            // Var(i): the k==i case folds in the variable's own noise term (which
            // is independent of every strictly-earlier variable by construction).
            let var_from_parents: f64 = parent_idx.iter().map(|(p, w)| w * cov[i][*p]).sum();
            cov[i][i] = var_from_parents + eq.noise_var;
        }
        (mean, cov, idx)
    }

    /// Build a "mutilated" copy of this graph (Pearl's graph surgery): every
    /// variable in `do_` has its incoming edges CUT and is fixed to its given
    /// value (`bias = value`, `parents = []`, `noise_var = 0.0`) — genuine
    /// do-calculus, not a conditioning shortcut. All other equations, and the
    /// topological order, are unchanged.
    fn mutilate(&self, do_: &HashMap<String, f64>) -> CausalGraph {
        let mut equations = HashMap::with_capacity(self.equations.len());
        for id in &self.order {
            if let Some(&v) = do_.get(id) {
                equations.insert(
                    id.clone(),
                    StructuralEquation {
                        parents: Vec::new(),
                        bias: v,
                        noise_var: 0.0,
                    },
                );
            } else {
                equations.insert(id.clone(), self.equations[id].clone());
            }
        }
        CausalGraph {
            equations,
            order: self.order.clone(),
        }
    }

    /// **Interventional** query `P(· | do(X₁=x₁, X₂=x₂, …))` (EPI-P3-3) — genuine
    /// do-calculus via graph surgery ([`Self::mutilate`]), then the exact joint
    /// moments of the MUTILATED graph. Returns a calibrated [`CausalEstimate`] for
    /// every variable (including the `do` variables themselves, trivially exact —
    /// `variance == 0.0`).
    pub fn intervene(
        &self,
        do_: &HashMap<String, f64>,
    ) -> Result<HashMap<String, CausalEstimate>, String> {
        for id in do_.keys() {
            self.require(id)?;
        }
        let mutilated = self.mutilate(do_);
        let (mean, cov, idx) = mutilated.joint();
        Ok(self
            .order
            .iter()
            .map(|id| {
                let i = idx[id];
                (
                    id.clone(),
                    CausalEstimate::from_moments(mean[i], cov[i][i], DEFAULT_CAUSAL_LEVEL),
                )
            })
            .collect())
    }

    /// **Observational** query `P(· | X₁=x₁, X₂=x₂, …)` (EPI-P3-3) — ordinary
    /// multivariate-Gaussian conditioning on the joint of the ORIGINAL
    /// (unmutilated) graph: `E[Y|E=e] = μ_Y + Σ_YE Σ_EE⁻¹ (e − μ_E)`,
    /// `Var[Y|E=e] = Σ_YY − Σ_YE Σ_EE⁻¹ Σ_EY`. Unlike [`Self::intervene`], this
    /// does NOT cut any edges, so evidence about a variable propagates backward to
    /// its ancestors too — the mechanism through which a confounder biases a naive
    /// conditional read of a causal question (see the module docs' worked
    /// distinction, proven in `tests::do_differs_from_observe_under_confounding`).
    pub fn observe(
        &self,
        evidence: &HashMap<String, f64>,
    ) -> Result<HashMap<String, CausalEstimate>, String> {
        for id in evidence.keys() {
            self.require(id)?;
        }
        let (mean, cov, idx) = self.joint();
        if evidence.is_empty() {
            return Ok(self
                .order
                .iter()
                .map(|id| {
                    let i = idx[id];
                    (
                        id.clone(),
                        CausalEstimate::from_moments(mean[i], cov[i][i], DEFAULT_CAUSAL_LEVEL),
                    )
                })
                .collect());
        }

        let ev_ids: Vec<&String> = evidence.keys().collect();
        let ev_idx: Vec<usize> = ev_ids.iter().map(|id| idx[*id]).collect();
        let k = ev_idx.len();

        let mut sigma_ee = vec![vec![0.0_f64; k]; k];
        for a in 0..k {
            for b in 0..k {
                sigma_ee[a][b] = cov[ev_idx[a]][ev_idx[b]];
            }
        }
        let sigma_ee_inv = invert_matrix(&sigma_ee).ok_or_else(|| {
            "observe: evidence covariance is singular (degenerate/duplicated evidence?)".to_string()
        })?;
        let diff: Vec<f64> = ev_idx
            .iter()
            .zip(ev_ids.iter())
            .map(|(&i, &id)| evidence[id] - mean[i])
            .collect();
        let alpha = matvec(&sigma_ee_inv, &diff);

        Ok(self
            .order
            .iter()
            .map(|id| {
                let i = idx[id];
                let sigma_ye: Vec<f64> = ev_idx.iter().map(|&ei| cov[i][ei]).collect();
                let mean_shift: f64 = sigma_ye.iter().zip(alpha.iter()).map(|(a, b)| a * b).sum();
                let cond_mean = mean[i] + mean_shift;
                let tmp = matvec(&sigma_ee_inv, &sigma_ye);
                let var_reduction: f64 = sigma_ye.iter().zip(tmp.iter()).map(|(a, b)| a * b).sum();
                let cond_var = (cov[i][i] - var_reduction).max(0.0);
                (
                    id.clone(),
                    CausalEstimate::from_moments(cond_mean, cond_var, DEFAULT_CAUSAL_LEVEL),
                )
            })
            .collect())
    }

    /// **Counterfactual** query (EPI-P3-3): "given that unit `actual` (a FULLY
    /// observed assignment of every variable) really happened, what would its
    /// variables have been had `do_` held instead?" — Pearl's deterministic
    /// three-step point-counterfactual recipe (*Causality*, ch. 7):
    ///
    /// 1. **Abduction**: infer each variable's realized exogenous noise from what
    ///    actually happened: `noise_i = actual_i − bias_i − Σ_p w_ip·actual_p`.
    /// 2. **Action**: apply the intervention — `do_` variables are fixed, cutting
    ///    their structural equations (same surgery as [`Self::intervene`]).
    /// 3. **Prediction**: replay the (surgered) structural equations forward in
    ///    topological order using the SAME inferred noise, so every variable NOT
    ///    downstream of a `do_` variable reproduces its actual value exactly, and
    ///    every downstream variable gets its counterfactual value.
    ///
    /// Requires `actual` to cover every variable (the standard "single fully-
    /// observed unit" form of the point-counterfactual method); a partially
    /// observed unit needs [`Self::observe`]'s posterior over the missing
    /// variables first — a probabilistic (not point) counterfactual, deferred as
    /// a follow-up (see module docs).
    pub fn counterfactual(
        &self,
        actual: &HashMap<String, f64>,
        do_: &HashMap<String, f64>,
    ) -> Result<HashMap<String, f64>, String> {
        for id in do_.keys() {
            self.require(id)?;
        }
        for id in &self.order {
            if !actual.contains_key(id) {
                return Err(format!(
                    "counterfactual requires a fully-observed unit: missing actual value for '{id}'"
                ));
            }
        }

        // 1. Abduction.
        let mut noise: HashMap<String, f64> = HashMap::with_capacity(self.order.len());
        for id in &self.order {
            let eq = &self.equations[id];
            let predicted: f64 =
                eq.bias + eq.parents.iter().map(|(p, w)| w * actual[p]).sum::<f64>();
            noise.insert(id.clone(), actual[id] - predicted);
        }

        // 2. Action + 3. Prediction, replaying forward with the inferred noise.
        let mut cf: HashMap<String, f64> = HashMap::with_capacity(self.order.len());
        for id in &self.order {
            if let Some(&v) = do_.get(id) {
                cf.insert(id.clone(), v);
                continue;
            }
            let eq = &self.equations[id];
            let predicted: f64 = eq.bias + eq.parents.iter().map(|(p, w)| w * cf[p]).sum::<f64>();
            cf.insert(id.clone(), predicted + noise[id]);
        }
        Ok(cf)
    }
}

/// `A · x` for a square-ish matrix `A` (rows) and vector `x`.
fn matvec(a: &[Vec<f64>], x: &[f64]) -> Vec<f64> {
    a.iter()
        .map(|row| row.iter().zip(x.iter()).map(|(a, b)| a * b).sum())
        .collect()
}

/// Gauss-Jordan matrix inverse with partial pivoting, pure Rust (no `nalgebra` —
/// this crate stays dependency-light; `k` here is the evidence-set size, always
/// small in practice). Returns `None` if `m` is singular (within `1e-12`).
fn invert_matrix(m: &[Vec<f64>]) -> Option<Vec<Vec<f64>>> {
    let n = m.len();
    if n == 0 {
        return Some(Vec::new());
    }
    // Augmented [A | I].
    let mut aug: Vec<Vec<f64>> = m
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let mut r = row.clone();
            r.resize(2 * n, 0.0);
            r[n + i] = 1.0;
            r
        })
        .collect();

    for col in 0..n {
        // Partial pivot: largest-magnitude entry in this column at/below `col`.
        let pivot_row = (col..n).max_by(|&a, &b| {
            aug[a][col]
                .abs()
                .partial_cmp(&aug[b][col].abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
        if aug[pivot_row][col].abs() < 1e-12 {
            return None; // singular
        }
        aug.swap(col, pivot_row);

        let pivot = aug[col][col];
        for v in aug[col].iter_mut() {
            *v /= pivot;
        }
        let pivot_row_vals = aug[col].clone();
        for (row, row_vals) in aug.iter_mut().enumerate() {
            if row == col {
                continue;
            }
            let factor = row_vals[col];
            if factor != 0.0 {
                for (v, pivot_v) in row_vals.iter_mut().zip(pivot_row_vals.iter()) {
                    *v -= factor * pivot_v;
                }
            }
        }
    }

    Some(aug.into_iter().map(|row| row[n..].to_vec()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-6;

    // Z (confounder) -> X, Z -> Y, X -> Y. The classic backdoor-confounding
    // fixture: the observational regression of Y on X is biased by the Z->X->…
    // and Z->Y backdoor path, while do(X) severs it.
    fn confounded_graph() -> CausalGraph {
        let mut g = CausalGraph::new();
        g.add_variable("z", vec![], 0.0, 1.0).unwrap();
        g.add_variable("x", vec![("z", 1.0)], 0.0, 0.25).unwrap();
        g.add_variable("y", vec![("z", 1.0), ("x", 0.5)], 0.0, 0.25)
            .unwrap();
        g
    }

    #[test]
    fn joint_moments_match_hand_derivation() {
        let g = confounded_graph();
        let (mean, cov, idx) = g.joint();
        // mean all 0 (no bias anywhere).
        for m in &mean {
            assert!(m.abs() < EPS);
        }
        assert!((cov[idx["z"]][idx["z"]] - 1.0).abs() < EPS);
        assert!((cov[idx["x"]][idx["x"]] - 1.25).abs() < EPS); // 1*1 + 0.25
        assert!((cov[idx["x"]][idx["z"]] - 1.0).abs() < EPS); // w_zx * var_z
        assert!((cov[idx["y"]][idx["y"]] - 2.5625).abs() < EPS);
        assert!((cov[idx["y"]][idx["x"]] - 1.625).abs() < EPS);
    }

    /// THE core proof: do-calculus != conditioning under confounding.
    #[test]
    fn do_differs_from_observe_under_confounding() {
        let g = confounded_graph();

        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 2.0);
        let interventional = g.intervene(&do_x).unwrap();

        let mut obs_x = HashMap::new();
        obs_x.insert("x".to_string(), 2.0);
        let observational = g.observe(&obs_x).unwrap();

        // Interventional: E[Y|do(X=2)] = w_xy * 2 = 0.5*2 = 1.0 exactly (the
        // confounder's contribution is cut off, mean_z stays 0).
        assert!(
            (interventional["y"].mean - 1.0).abs() < EPS,
            "do(X=2) should give the TRUE structural effect 1.0, got {}",
            interventional["y"].mean
        );
        // Observational: E[Y|X=2] = (Cov(Y,X)/Var(X)) * 2 = (1.625/1.25)*2 = 2.6,
        // inflated by the backdoor path through Z.
        assert!(
            (observational["y"].mean - 2.6).abs() < EPS,
            "observing X=2 should give the CONFOUNDED regression estimate 2.6, got {}",
            observational["y"].mean
        );
        // The whole point: these must differ, and by a lot (not rounding noise).
        assert!(
            (interventional["y"].mean - observational["y"].mean).abs() > 1.0,
            "do(X) ({}) and observe(X) ({}) must differ substantially under confounding",
            interventional["y"].mean,
            observational["y"].mean
        );

        // Observing X should also move our belief about the confounder Z itself
        // (backward inference) — do(X) must NOT (the edge into X is cut).
        assert!(
            observational["z"].mean.abs() > EPS,
            "conditioning on X should update belief about confounder Z"
        );
        assert!(
            interventional["z"].mean.abs() < EPS,
            "do(X) must leave the (now edge-cut) confounder Z's belief untouched"
        );
    }

    #[test]
    fn intervening_on_a_variable_pins_it_with_zero_variance() {
        let g = confounded_graph();
        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 3.5);
        let out = g.intervene(&do_x).unwrap();
        assert!((out["x"].mean - 3.5).abs() < EPS);
        assert_eq!(out["x"].variance, 0.0);
        assert_eq!(out["x"].interval, (3.5, 3.5));
    }

    #[test]
    fn every_causal_estimate_carries_a_valid_calibrated_interval() {
        let g = confounded_graph();
        let out = g.observe(&HashMap::new()).unwrap();
        for (id, est) in &out {
            let (lo, hi) = est.interval;
            assert!(
                lo <= est.mean && est.mean <= hi,
                "{id}: interval must bracket the mean"
            );
            assert!(
                lo < hi || est.variance == 0.0,
                "{id}: non-degenerate interval must have lo<hi"
            );
            assert!((est.level - DEFAULT_CAUSAL_LEVEL).abs() < 1e-9);
        }
    }

    // A counterfactual genuinely changes an observed unit's downstream outcome —
    // and leaves upstream/unrelated variables exactly as they actually were.
    #[test]
    fn counterfactual_changes_downstream_outcome_only() {
        let g = confounded_graph();
        // A fully-observed unit consistent with the structural equations:
        // z=1.0, x = 1*z + noise_x => pick noise_x=0.5 => x=1.5,
        // y = 1*z + 0.5*x + noise_y => pick noise_y=0.2 => y = 1.0+0.75+0.2=1.95.
        let mut actual = HashMap::new();
        actual.insert("z".to_string(), 1.0);
        actual.insert("x".to_string(), 1.5);
        actual.insert("y".to_string(), 1.95);

        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 4.0); // "what if X had been 4.0 instead of 1.5?"
        let cf = g.counterfactual(&actual, &do_x).unwrap();

        // Z is upstream of X, unaffected by the intervention on X: reproduces
        // its actual value exactly.
        assert!((cf["z"] - 1.0).abs() < EPS);
        // X is pinned to the counterfactual value.
        assert!((cf["x"] - 4.0).abs() < EPS);
        // Y is downstream of X: its counterfactual value differs from the
        // actual 1.95, using the SAME inferred noise_y=0.2:
        // y_cf = 1*z_actual + 0.5*4.0 + noise_y = 1.0 + 2.0 + 0.2 = 3.2.
        assert!(
            (cf["y"] - 3.2).abs() < EPS,
            "expected counterfactual y=3.2, got {}",
            cf["y"]
        );
        assert!(
            (cf["y"] - 1.95).abs() > 1.0,
            "counterfactual y must differ substantially from the actual outcome"
        );
    }

    #[test]
    fn counterfactual_requires_a_fully_observed_unit() {
        let g = confounded_graph();
        let mut partial = HashMap::new();
        partial.insert("z".to_string(), 1.0);
        // missing "x" and "y".
        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 4.0);
        assert!(g.counterfactual(&partial, &do_x).is_err());
    }

    #[test]
    fn unknown_variable_is_an_error_not_a_panic() {
        let g = confounded_graph();
        let mut bad = HashMap::new();
        bad.insert("not_a_variable".to_string(), 1.0);
        assert!(g.intervene(&bad).is_err());
        assert!(g.observe(&bad).is_err());
    }

    #[test]
    fn add_variable_rejects_out_of_order_parent() {
        let mut g = CausalGraph::new();
        // "x" depends on "z", which is not defined yet — must be rejected so the
        // topological-order invariant `joint()` relies on always holds.
        assert!(g.add_variable("x", vec![("z", 1.0)], 0.0, 1.0).is_err());
    }

    #[test]
    fn invert_2x2_matrix_matches_hand_computation() {
        // [[4,7],[2,6]]^-1 = 1/10 * [[6,-7],[-2,4]]
        let m = vec![vec![4.0, 7.0], vec![2.0, 6.0]];
        let inv = invert_matrix(&m).unwrap();
        assert!((inv[0][0] - 0.6).abs() < EPS);
        assert!((inv[0][1] - (-0.7)).abs() < EPS);
        assert!((inv[1][0] - (-0.2)).abs() < EPS);
        assert!((inv[1][1] - 0.4).abs() < EPS);
    }

    #[test]
    fn singular_matrix_returns_none() {
        let m = vec![vec![1.0, 2.0], vec![2.0, 4.0]];
        assert!(invert_matrix(&m).is_none());
    }
}
