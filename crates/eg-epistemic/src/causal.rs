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

// ─────────────────────────────────────────────────────────────────────────────
// EPI-P3-3 (W1e) — DISCRETE conditional-probability-table (categorical) SCM.
//
// The [`CausalGraph`] above answers the three causal questions for CONTINUOUS,
// linear-Gaussian variables. Many real causal questions are over CATEGORICAL
// variables (a treatment that is on/off, a diagnosis in {healthy, mild, severe},
// …) whose mechanism is a **conditional probability table** (CPT), not a linear
// equation. [`DiscreteCausalGraph`] is the categorical sibling: each variable
// `X` takes a value in `{0, …, cardinality-1}` and carries a CPT
// `P(X = v | parents = pa)` keyed by the tuple of its parents' values.
//
// It answers the SAME three questions, by the SAME constructions, so the
// discrete/continuous split is a model-KIND choice ([`CausalModelKind`]) and
// nothing else:
//
// * [`DiscreteCausalGraph::observe`] — the observational query `P(· | E=e)` by
//   EXACT inference: enumerate the joint over all assignments (the categorical
//   analogue of the linear model's closed-form joint — exact, not sampled),
//   restrict to the assignments consistent with the evidence, and renormalize.
//   Evidence propagates BACKWARD to ancestors, exactly as Gaussian conditioning
//   does — the confounder-biasing mechanism, now for categoricals.
// * [`DiscreteCausalGraph::intervene`] — the do-query `P(· | do(X=v))` by the
//   SAME graph surgery as the linear model: the `do` variables' incoming edges
//   are CUT and the variable is pinned to a point-mass CPT before the joint is
//   recomputed. No information flows backward through a `do` variable.
// * [`DiscreteCausalGraph::counterfactual`] — Pearl's abduction/action/prediction
//   for a single fully-observed unit, using the canonical CDF-inversion
//   structural function `X = f(pa, u)`, `u ~ Uniform[0,1)`: abduct each
//   variable's exogenous quantile `u` from what actually happened, apply the
//   intervention, then replay forward with the SAME quantiles (the discrete
//   "same inferred noise" — a monotonic/quantile-preserving counterfactual, the
//   discrete analogue of reusing the Gaussian's inferred noise term).
//
// Pure Rust, no new dependency (categorical — it does not even need
// `Distribution`). Additive: it does not touch the linear-Gaussian path above.
// ─────────────────────────────────────────────────────────────────────────────

/// Which structural-causal-model family a causal query runs against — the
/// model-KIND selector that lets a caller (e.g. the `Method::CausalEstimate` /
/// `Method::CausalCounterfactual` handler) pick the continuous linear-Gaussian
/// model ([`CausalGraph`]) or the discrete categorical CPT model
/// ([`DiscreteCausalGraph`]) for the SAME observe / intervene / counterfactual
/// question. Defaults to [`CausalModelKind::LinearGaussian`] so the historical
/// behaviour is preserved when no kind is specified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CausalModelKind {
    /// The continuous linear-Gaussian SCM — [`CausalGraph`].
    #[default]
    LinearGaussian,
    /// The discrete categorical conditional-probability-table SCM —
    /// [`DiscreteCausalGraph`].
    DiscreteCpt,
}

impl CausalModelKind {
    /// Parse the wire/parameter spelling of a model-kind (case-insensitive,
    /// hyphen/underscore-insensitive) so a `model_kind` string parameter on a
    /// causal `Method` can select the discrete model. Unknown spellings are an
    /// error rather than a silent fallback.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().replace(['-', ' '], "_").as_str() {
            "" | "linear_gaussian" | "lineargaussian" | "gaussian" | "linear" | "continuous" => {
                Ok(CausalModelKind::LinearGaussian)
            }
            "discrete_cpt" | "discretecpt" | "discrete" | "categorical" | "cpt" => {
                Ok(CausalModelKind::DiscreteCpt)
            }
            other => Err(format!(
                "unknown causal model kind '{other}' (expected 'linear_gaussian' or 'discrete_cpt')"
            )),
        }
    }
}

/// One categorical variable's structural equation: a conditional probability
/// table over its `cardinality` categories `{0, …, cardinality-1}`, keyed by the
/// tuple of its parents' category values in `parents` order (the empty tuple for
/// a root). Every parent-value combination must be present and each row must be a
/// valid probability vector — enforced by [`DiscreteCausalGraph::add_variable`].
#[derive(Clone, Debug, PartialEq)]
pub struct DiscreteStructuralEquation {
    /// Parent variable ids, in the order their values index [`Self::table`] keys.
    /// All must already be in the graph (enforced at add time → acyclic +
    /// topological order by construction, exactly like the linear model).
    pub parents: Vec<String>,
    /// Number of categories this variable can take: values are `0..cardinality`.
    pub cardinality: usize,
    /// `parent-value-tuple → P(this = 0), P(this = 1), …` (length `cardinality`,
    /// non-negative, sums to 1). A root has the single key `vec![]`.
    pub table: HashMap<Vec<usize>, Vec<f64>>,
}

/// A discrete categorical structural causal model: a DAG of
/// [`DiscreteStructuralEquation`]s in topological (insertion) order — the
/// categorical sibling of [`CausalGraph`].
#[derive(Clone, Debug, Default)]
pub struct DiscreteCausalGraph {
    equations: HashMap<String, DiscreteStructuralEquation>,
    /// Insertion order == a valid topological order (parents precede children).
    order: Vec<String>,
    /// `id → cardinality`, cached for O(1) parent-config validation/enumeration.
    cardinality: HashMap<String, usize>,
}

/// A calibrated result of one DISCRETE causal query: the full posterior
/// categorical distribution over a variable's categories, plus the maximum-a-
/// posteriori category. The discrete analogue of [`CausalEstimate`] (whose
/// mean/variance/interval assume a continuous variable).
#[derive(Clone, Debug, PartialEq)]
pub struct CategoricalEstimate {
    /// `P(X = 0), P(X = 1), …` — length equals the variable's `cardinality`.
    pub probs: Vec<f64>,
    /// `argmax_v P(X = v)` — the most probable category (ties resolve to the
    /// lowest index).
    pub map_category: usize,
}

impl CategoricalEstimate {
    fn from_probs(probs: Vec<f64>) -> Self {
        let map_category = probs
            .iter()
            .enumerate()
            .fold((0usize, f64::NEG_INFINITY), |(bi, bv), (i, &v)| {
                if v > bv {
                    (i, v)
                } else {
                    (bi, bv)
                }
            })
            .0;
        CategoricalEstimate {
            probs,
            map_category,
        }
    }
}

/// Tolerance a CPT row's probabilities may deviate from summing to 1.
const CPT_SUM_TOL: f64 = 1e-9;

impl DiscreteCausalGraph {
    pub fn new() -> Self {
        DiscreteCausalGraph::default()
    }

    /// Add one categorical variable. `parents` must already be present (checked),
    /// so a graph built purely through this method is acyclic by construction and
    /// `self.order` is a valid topological order. `table` must contain a row for
    /// EVERY combination of the parents' category values, and each row must be a
    /// length-`cardinality` probability vector (non-negative, sums to 1 within
    /// [`CPT_SUM_TOL`]).
    pub fn add_variable(
        &mut self,
        id: impl Into<String>,
        parents: Vec<&str>,
        cardinality: usize,
        table: Vec<(Vec<usize>, Vec<f64>)>,
    ) -> Result<(), String> {
        let id = id.into();
        if self.equations.contains_key(&id) {
            return Err(format!("variable '{id}' already defined"));
        }
        if cardinality == 0 {
            return Err(format!("variable '{id}': cardinality must be >= 1"));
        }
        let mut owned_parents = Vec::with_capacity(parents.len());
        let mut parent_cards = Vec::with_capacity(parents.len());
        for p in parents {
            let pc = self.cardinality.get(p).ok_or_else(|| {
                format!(
                    "variable '{id}': parent '{p}' is not yet defined — variables \
                     must be added in topological order (parents before children)"
                )
            })?;
            owned_parents.push(p.to_string());
            parent_cards.push(*pc);
        }

        // Index the supplied rows and validate each is a proper probability
        // vector over `cardinality` categories.
        let mut map: HashMap<Vec<usize>, Vec<f64>> = HashMap::with_capacity(table.len());
        for (key, row) in table {
            if key.len() != owned_parents.len() {
                return Err(format!(
                    "variable '{id}': CPT key {key:?} has {} entries but the variable has {} parents",
                    key.len(),
                    owned_parents.len()
                ));
            }
            for (pos, &kv) in key.iter().enumerate() {
                if kv >= parent_cards[pos] {
                    return Err(format!(
                        "variable '{id}': CPT key {key:?} value {kv} out of range for parent \
                         '{}' (cardinality {})",
                        owned_parents[pos], parent_cards[pos]
                    ));
                }
            }
            if row.len() != cardinality {
                return Err(format!(
                    "variable '{id}': CPT row for key {key:?} has length {} but cardinality is {cardinality}",
                    row.len()
                ));
            }
            let mut sum = 0.0;
            for &p in &row {
                if p < 0.0 {
                    return Err(format!(
                        "variable '{id}': CPT row for key {key:?} has a negative probability"
                    ));
                }
                sum += p;
            }
            if (sum - 1.0).abs() > CPT_SUM_TOL {
                return Err(format!(
                    "variable '{id}': CPT row for key {key:?} sums to {sum}, not 1"
                ));
            }
            if map.insert(key.clone(), row).is_some() {
                return Err(format!("variable '{id}': duplicate CPT key {key:?}"));
            }
        }

        // Every parent-value combination must be covered (a total CPT).
        let expected = parent_cards.iter().product::<usize>().max(1);
        if map.len() != expected {
            return Err(format!(
                "variable '{id}': CPT has {} rows but needs {expected} (one per parent-value \
                 combination)",
                map.len()
            ));
        }
        for combo in cartesian(&parent_cards) {
            if !map.contains_key(&combo) {
                return Err(format!(
                    "variable '{id}': CPT is missing a row for parent-value combination {combo:?}"
                ));
            }
        }

        self.cardinality.insert(id.clone(), cardinality);
        self.equations.insert(
            id.clone(),
            DiscreteStructuralEquation {
                parents: owned_parents,
                cardinality,
                table: map,
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

    /// The exact joint distribution over ALL variables as `(assignment, prob)`
    /// pairs, where `assignment[i]` is the category of `self.order[i]`. Built by a
    /// single forward pass in topological order, multiplying in each variable's
    /// CPT row for the (already-known) parent values — the categorical analogue of
    /// the linear model's closed-form joint, and exact (full enumeration, no
    /// sampling). Cost is the product of cardinalities, so intended for the small
    /// SCMs causal queries pose in practice.
    fn joint_assignments(&self) -> (Vec<(Vec<usize>, f64)>, HashMap<String, usize>) {
        let idx: HashMap<String, usize> = self
            .order
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i))
            .collect();
        let mut states: Vec<(Vec<usize>, f64)> = vec![(Vec::new(), 1.0)];
        for id in &self.order {
            let eq = &self.equations[id];
            let parent_pos: Vec<usize> = eq.parents.iter().map(|p| idx[p]).collect();
            let mut next: Vec<(Vec<usize>, f64)> = Vec::with_capacity(states.len() * eq.cardinality);
            for (assignment, prob) in &states {
                let key: Vec<usize> = parent_pos.iter().map(|&pp| assignment[pp]).collect();
                let row = &eq.table[&key];
                for (v, &pv) in row.iter().enumerate() {
                    if pv == 0.0 {
                        continue; // impossible branch — prune (keeps the joint sparse)
                    }
                    let mut a = assignment.clone();
                    a.push(v);
                    next.push((a, prob * pv));
                }
            }
            states = next;
        }
        (states, idx)
    }

    /// Marginal categorical distribution per variable from a set of weighted
    /// assignments (already renormalized by the caller if conditioned).
    fn marginals(
        &self,
        assignments: &[(Vec<usize>, f64)],
        idx: &HashMap<String, usize>,
        norm: f64,
    ) -> HashMap<String, CategoricalEstimate> {
        self.order
            .iter()
            .map(|id| {
                let i = idx[id];
                let card = self.equations[id].cardinality;
                let mut probs = vec![0.0_f64; card];
                for (assignment, prob) in assignments {
                    probs[assignment[i]] += *prob;
                }
                if norm > 0.0 {
                    for p in &mut probs {
                        *p /= norm;
                    }
                }
                (id.clone(), CategoricalEstimate::from_probs(probs))
            })
            .collect()
    }

    /// Build a mutilated copy (Pearl's graph surgery): every variable in `do_`
    /// has its incoming edges CUT and is pinned to a point-mass CPT at its given
    /// category. All other equations and the topological order are unchanged.
    fn mutilate(&self, do_: &HashMap<String, usize>) -> DiscreteCausalGraph {
        let mut equations = HashMap::with_capacity(self.equations.len());
        for id in &self.order {
            if let Some(&v) = do_.get(id) {
                let card = self.equations[id].cardinality;
                let mut row = vec![0.0_f64; card];
                row[v] = 1.0;
                let mut table = HashMap::with_capacity(1);
                table.insert(Vec::new(), row);
                equations.insert(
                    id.clone(),
                    DiscreteStructuralEquation {
                        parents: Vec::new(),
                        cardinality: card,
                        table,
                    },
                );
            } else {
                equations.insert(id.clone(), self.equations[id].clone());
            }
        }
        DiscreteCausalGraph {
            equations,
            order: self.order.clone(),
            cardinality: self.cardinality.clone(),
        }
    }

    /// **Interventional** query `P(· | do(X₁=v₁, …))` — genuine do-calculus via
    /// graph surgery ([`Self::mutilate`]) then the exact joint marginals of the
    /// mutilated graph. Returns a [`CategoricalEstimate`] for every variable
    /// (the `do` variables trivially a point mass on their fixed category).
    pub fn intervene(
        &self,
        do_: &HashMap<String, usize>,
    ) -> Result<HashMap<String, CategoricalEstimate>, String> {
        for (id, &v) in do_ {
            self.require(id)?;
            let card = self.equations[id].cardinality;
            if v >= card {
                return Err(format!(
                    "do({id}={v}) is out of range (variable '{id}' has cardinality {card})"
                ));
            }
        }
        let mutilated = self.mutilate(do_);
        let (assignments, idx) = mutilated.joint_assignments();
        Ok(mutilated.marginals(&assignments, &idx, 1.0))
    }

    /// **Observational** query `P(· | E=e)` by exact inference: enumerate the
    /// joint of the ORIGINAL (unmutilated) graph, keep the assignments consistent
    /// with `evidence`, and renormalize. Unlike [`Self::intervene`] this cuts no
    /// edges, so evidence propagates BACKWARD to ancestors (a confounder) — the
    /// categorical mirror of the linear model's Gaussian conditioning.
    pub fn observe(
        &self,
        evidence: &HashMap<String, usize>,
    ) -> Result<HashMap<String, CategoricalEstimate>, String> {
        for (id, &v) in evidence {
            self.require(id)?;
            let card = self.equations[id].cardinality;
            if v >= card {
                return Err(format!(
                    "evidence {id}={v} is out of range (variable '{id}' has cardinality {card})"
                ));
            }
        }
        let (all, idx) = self.joint_assignments();
        if evidence.is_empty() {
            return Ok(self.marginals(&all, &idx, 1.0));
        }
        let ev_pos: Vec<(usize, usize)> =
            evidence.iter().map(|(id, &v)| (idx[id], v)).collect();
        let kept: Vec<(Vec<usize>, f64)> = all
            .into_iter()
            .filter(|(a, _)| ev_pos.iter().all(|&(pos, v)| a[pos] == v))
            .collect();
        let norm: f64 = kept.iter().map(|(_, p)| *p).sum();
        if norm <= 0.0 {
            return Err(
                "observe: the evidence assignment has probability zero under the model".to_string(),
            );
        }
        Ok(self.marginals(&kept, &idx, norm))
    }

    /// **Counterfactual** query for a single fully-observed unit — Pearl's
    /// abduction/action/prediction over the canonical CDF-inversion structural
    /// function `X = f(pa, u)`, `u ~ Uniform[0,1)`:
    ///
    /// 1. **Abduction**: for each variable, from its ACTUAL value and actual
    ///    parent config, recover the exogenous quantile `u` as the midpoint of the
    ///    consistent CDF interval — the canonical representative of "the noise that
    ///    produced this outcome" (a quantile-preserving / monotonic counterfactual,
    ///    the discrete analogue of recovering the Gaussian model's noise term).
    /// 2. **Action**: pin the `do_` variables (same surgery as [`Self::intervene`]).
    /// 3. **Prediction**: replay forward in topological order re-evaluating each
    ///    non-`do_` variable's structural function at its SAME abducted quantile —
    ///    so every variable not downstream of a `do_` variable reproduces its
    ///    actual value and downstream variables get their counterfactual category.
    ///
    /// Requires `actual` to cover every variable and to have non-zero probability
    /// under the model.
    pub fn counterfactual(
        &self,
        actual: &HashMap<String, usize>,
        do_: &HashMap<String, usize>,
    ) -> Result<HashMap<String, usize>, String> {
        for (id, &v) in do_ {
            self.require(id)?;
            let card = self.equations[id].cardinality;
            if v >= card {
                return Err(format!(
                    "do({id}={v}) is out of range (variable '{id}' has cardinality {card})"
                ));
            }
        }
        for id in &self.order {
            match actual.get(id) {
                None => {
                    return Err(format!(
                        "counterfactual requires a fully-observed unit: missing actual value for '{id}'"
                    ));
                }
                Some(&v) if v >= self.equations[id].cardinality => {
                    return Err(format!(
                        "actual {id}={v} is out of range (variable '{id}' has cardinality {})",
                        self.equations[id].cardinality
                    ));
                }
                Some(_) => {}
            }
        }

        // 1. Abduction: recover each variable's exogenous quantile.
        let mut quantile: HashMap<String, f64> = HashMap::with_capacity(self.order.len());
        for id in &self.order {
            let eq = &self.equations[id];
            let key: Vec<usize> = eq.parents.iter().map(|p| actual[p]).collect();
            let row = &eq.table[&key];
            let v = actual[id];
            let lo: f64 = row[..v].iter().sum();
            let hi: f64 = lo + row[v];
            if row[v] <= 0.0 {
                return Err(format!(
                    "counterfactual: actual assignment gives '{id}={v}' zero probability under the model"
                ));
            }
            quantile.insert(id.clone(), 0.5 * (lo + hi));
        }

        // 2. Action + 3. Prediction, replaying forward with the abducted quantiles.
        let mut cf: HashMap<String, usize> = HashMap::with_capacity(self.order.len());
        for id in &self.order {
            if let Some(&v) = do_.get(id) {
                cf.insert(id.clone(), v);
                continue;
            }
            let eq = &self.equations[id];
            let key: Vec<usize> = eq.parents.iter().map(|p| cf[p]).collect();
            let row = &eq.table[&key];
            cf.insert(id.clone(), invert_cdf(row, quantile[id]));
        }
        Ok(cf)
    }
}

/// The canonical CDF-inversion structural function: the smallest category `v`
/// whose cumulative probability strictly exceeds the quantile `u ∈ [0,1)`.
fn invert_cdf(row: &[f64], u: f64) -> usize {
    let mut cum = 0.0;
    for (v, &p) in row.iter().enumerate() {
        cum += p;
        if u < cum {
            return v;
        }
    }
    row.len().saturating_sub(1)
}

/// Every combination of category values for a list of cardinalities, in
/// lexicographic order (`[]` yields the single empty combination — a root's only
/// parent-value tuple).
fn cartesian(cards: &[usize]) -> Vec<Vec<usize>> {
    let mut out: Vec<Vec<usize>> = vec![Vec::new()];
    for &c in cards {
        let mut next = Vec::with_capacity(out.len() * c);
        for combo in &out {
            for v in 0..c {
                let mut e = combo.clone();
                e.push(v);
                next.push(e);
            }
        }
        out = next;
    }
    out
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

    // ── Discrete (categorical CPT) SCM ───────────────────────────────────────

    // A binary confounded chain Z -> X -> Y with Z -> X too, so do(X) and
    // observe(X) genuinely disagree on the confounder Z:
    //   Z:  P(Z=1) = 0.5
    //   X | Z=0: P(X=1)=0.2 ;  X | Z=1: P(X=1)=0.8
    //   Y | X=0: P(Y=1)=0.1 ;  Y | X=1: P(Y=1)=0.7   (Y depends only on X)
    fn discrete_chain() -> DiscreteCausalGraph {
        let mut g = DiscreteCausalGraph::new();
        g.add_variable("z", vec![], 2, vec![(vec![], vec![0.5, 0.5])])
            .unwrap();
        g.add_variable(
            "x",
            vec!["z"],
            2,
            vec![(vec![0], vec![0.8, 0.2]), (vec![1], vec![0.2, 0.8])],
        )
        .unwrap();
        g.add_variable(
            "y",
            vec!["x"],
            2,
            vec![(vec![0], vec![0.9, 0.1]), (vec![1], vec![0.3, 0.7])],
        )
        .unwrap();
        g
    }

    #[test]
    fn discrete_add_variable_rejects_bad_cpts() {
        let mut g = DiscreteCausalGraph::new();
        // parent not yet defined
        assert!(g
            .add_variable("x", vec!["z"], 2, vec![(vec![0], vec![1.0, 0.0])])
            .is_err());
        g.add_variable("z", vec![], 2, vec![(vec![], vec![0.5, 0.5])])
            .unwrap();
        // row does not sum to 1
        assert!(g
            .add_variable("a", vec![], 2, vec![(vec![], vec![0.5, 0.4])])
            .is_err());
        // missing a parent-value combination (only z=0 row present)
        assert!(g
            .add_variable("b", vec!["z"], 2, vec![(vec![0], vec![0.5, 0.5])])
            .is_err());
        // wrong row length for the cardinality
        assert!(g
            .add_variable("c", vec![], 3, vec![(vec![], vec![0.5, 0.5])])
            .is_err());
    }

    #[test]
    fn discrete_intervene_cuts_the_backdoor_and_pins_the_target() {
        let g = discrete_chain();
        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 1usize);
        let out = g.intervene(&do_x).unwrap();

        // X is pinned to a point mass on category 1.
        assert!((out["x"].probs[1] - 1.0).abs() < EPS);
        assert!((out["x"].probs[0]).abs() < EPS);
        assert_eq!(out["x"].map_category, 1);

        // Y | do(X=1) = the CPT row for X=1 exactly: P(Y=1)=0.7.
        assert!(
            (out["y"].probs[1] - 0.7).abs() < EPS,
            "P(Y=1|do(X=1)) should be 0.7, got {}",
            out["y"].probs[1]
        );

        // do(X) severs Z->X, so the confounder keeps its prior: P(Z=1)=0.5.
        assert!(
            (out["z"].probs[1] - 0.5).abs() < EPS,
            "do(X) must leave the (edge-cut) confounder Z at its prior 0.5, got {}",
            out["z"].probs[1]
        );
    }

    #[test]
    fn discrete_observe_differs_from_intervene_on_the_confounder() {
        let g = discrete_chain();
        let mut ev = HashMap::new();
        ev.insert("x".to_string(), 1usize);
        let obs = g.observe(&ev).unwrap();

        // Backward inference: P(Z=1|X=1) = P(X=1|Z=1)P(Z=1)/P(X=1)
        //   P(X=1) = 0.2*0.5 + 0.8*0.5 = 0.5 ; so = 0.8*0.5/0.5 = 0.8.
        assert!(
            (obs["z"].probs[1] - 0.8).abs() < EPS,
            "observing X=1 should update the confounder to P(Z=1)=0.8, got {}",
            obs["z"].probs[1]
        );
        // Y depends only on X, so P(Y=1|X=1)=0.7 — same as do here, but Z differs.
        assert!((obs["y"].probs[1] - 0.7).abs() < EPS);

        // The whole point: seeing vs doing disagree on Z (0.8 vs 0.5).
        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 1usize);
        let ivn = g.intervene(&do_x).unwrap();
        assert!(
            (obs["z"].probs[1] - ivn["z"].probs[1]).abs() > 0.25,
            "observe(X) ({}) and do(X) ({}) must disagree on the confounder Z",
            obs["z"].probs[1],
            ivn["z"].probs[1]
        );
    }

    #[test]
    fn discrete_counterfactual_known_answer() {
        let g = discrete_chain();
        // Fully-observed unit consistent with the model: Z=1, X=1, Y=1.
        let mut actual = HashMap::new();
        actual.insert("z".to_string(), 1usize);
        actual.insert("x".to_string(), 1usize);
        actual.insert("y".to_string(), 1usize);

        // "What would Y have been had X been 0 instead of 1?"
        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 0usize);
        let cf = g.counterfactual(&actual, &do_x).unwrap();

        // Hand derivation of the abduction (midpoint of the consistent CDF band):
        //   Z=1: row [0.5,0.5], band for cat 1 = [0.5,1.0) -> u_z = 0.75
        //   X=1 | Z=1: row [0.2,0.8], band for cat 1 = [0.2,1.0) -> u_x = 0.6
        //   Y=1 | X=1: row [0.3,0.7], band for cat 1 = [0.3,1.0) -> u_y = 0.65
        // Prediction under do(X=0):
        //   Z: unaffected upstream, replay u_z=0.75 -> cat 1 (reproduces actual).
        //   X: pinned to 0.
        //   Y | X=0: row [0.9,0.1], cum[0]=0.9 ; u_y=0.65 < 0.9 -> cat 0.
        assert_eq!(cf["z"], 1, "Z is upstream of X and must reproduce its actual value");
        assert_eq!(cf["x"], 0, "X is pinned to the counterfactual category");
        assert_eq!(
            cf["y"], 0,
            "counterfactual Y must flip 1 -> 0 under do(X=0) at the abducted quantile"
        );
    }

    #[test]
    fn discrete_empty_do_counterfactual_reproduces_the_actual_unit() {
        // Consistency: abduction then prediction with no intervention returns the
        // exact observed unit (the quantile-preserving property).
        let g = discrete_chain();
        let mut actual = HashMap::new();
        actual.insert("z".to_string(), 1usize);
        actual.insert("x".to_string(), 1usize);
        actual.insert("y".to_string(), 1usize);
        let cf = g.counterfactual(&actual, &HashMap::new()).unwrap();
        assert_eq!(cf["z"], 1);
        assert_eq!(cf["x"], 1);
        assert_eq!(cf["y"], 1);
    }

    #[test]
    fn discrete_counterfactual_requires_a_fully_observed_unit() {
        let g = discrete_chain();
        let mut partial = HashMap::new();
        partial.insert("z".to_string(), 1usize); // missing x, y
        let mut do_x = HashMap::new();
        do_x.insert("x".to_string(), 0usize);
        assert!(g.counterfactual(&partial, &do_x).is_err());
    }

    #[test]
    fn discrete_observe_marginals_are_a_normalized_distribution() {
        let g = discrete_chain();
        let out = g.observe(&HashMap::new()).unwrap();
        for (id, est) in &out {
            let sum: f64 = est.probs.iter().sum();
            assert!(
                (sum - 1.0).abs() < EPS,
                "{id}: marginal must sum to 1, got {sum}"
            );
            assert!(est.map_category < est.probs.len());
        }
        // Unconditioned marginal of X: P(X=1) = 0.2*0.5 + 0.8*0.5 = 0.5.
        assert!((out["x"].probs[1] - 0.5).abs() < EPS);
    }

    #[test]
    fn discrete_out_of_range_do_and_evidence_are_errors() {
        let g = discrete_chain();
        let mut bad_do = HashMap::new();
        bad_do.insert("x".to_string(), 5usize); // cardinality is 2
        assert!(g.intervene(&bad_do).is_err());
        let mut bad_ev = HashMap::new();
        bad_ev.insert("y".to_string(), 9usize);
        assert!(g.observe(&bad_ev).is_err());
    }

    #[test]
    fn model_kind_parses_and_defaults_to_linear_gaussian() {
        assert_eq!(CausalModelKind::default(), CausalModelKind::LinearGaussian);
        assert_eq!(
            CausalModelKind::parse("").unwrap(),
            CausalModelKind::LinearGaussian
        );
        assert_eq!(
            CausalModelKind::parse("Linear-Gaussian").unwrap(),
            CausalModelKind::LinearGaussian
        );
        assert_eq!(
            CausalModelKind::parse("discrete_cpt").unwrap(),
            CausalModelKind::DiscreteCpt
        );
        assert_eq!(
            CausalModelKind::parse("Categorical").unwrap(),
            CausalModelKind::DiscreteCpt
        );
        assert!(CausalModelKind::parse("nonsense").is_err());
    }
}
