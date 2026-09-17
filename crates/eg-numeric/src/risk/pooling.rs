//! Beta–binomial partial pooling over a hierarchy (for example kind ->
//! capability class -> option).
//!
//! Each node's posterior is its prior updated with the node's aggregate counts.
//! A child's prior has the parent posterior's mean and a concentration fitted by
//! the method of moments from the spread of the siblings' success rates, clamped
//! to caller bounds: siblings that agree shrink hard toward the parent, and
//! siblings that disagree keep their own rates. This is empirical-Bayes partial
//! pooling, not a full hierarchical posterior. Children are visited in key
//! order (`BTreeMap`), so results are deterministic.

use super::beta::beta_interval;
use super::counts::Counts;
use crate::detkernel::reduce::serial_sum;
use crate::detkernel::{validate, Level, StatError, StatResult};
use std::collections::BTreeMap;
use std::ops::Deref;

/// `successes` out of `trials` (zero trials allowed). `Deref`s to [`Counts`]
/// for `successes()`/`trials()`, defined once there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct GroupCounts(Counts);

impl Deref for GroupCounts {
    type Target = Counts;

    fn deref(&self) -> &Counts {
        &self.0
    }
}

impl GroupCounts {
    /// Validate `successes <= trials`.
    pub fn new(successes: u64, trials: u64) -> StatResult<Self> {
        let counts = Counts::checked(successes, trials)?;
        Ok(Self(counts))
    }

    fn checked_add(self, other: Self) -> StatResult<Self> {
        let overflow = StatError::ArithmeticOverflow {
            what: "pooled counts",
        };
        let successes = self
            .successes()
            .checked_add(other.successes())
            .ok_or(overflow)?;
        let trials = self.trials().checked_add(other.trials()).ok_or(overflow)?;
        let counts = Counts::checked(successes, trials)?;
        Ok(Self(counts))
    }
}

/// A `Beta(alpha, beta)` distribution with positive finite shapes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BetaDistribution {
    alpha: f64,
    beta: f64,
}

impl BetaDistribution {
    /// Validate shapes.
    pub fn new(alpha: f64, beta: f64) -> StatResult<Self> {
        validate::parameter(alpha.is_finite() && alpha > 0.0, "alpha", "finite and > 0")?;
        validate::parameter(beta.is_finite() && beta > 0.0, "beta", "finite and > 0")?;
        Ok(Self { alpha, beta })
    }

    /// Shape `alpha`.
    pub fn alpha(self) -> f64 {
        self.alpha
    }

    /// Shape `beta`.
    pub fn beta(self) -> f64 {
        self.beta
    }

    /// Mean `alpha / (alpha + beta)`.
    pub fn mean(self) -> f64 {
        self.alpha / (self.alpha + self.beta)
    }

    /// Concentration `alpha + beta`.
    pub fn concentration(self) -> f64 {
        self.alpha + self.beta
    }

    /// The conjugate posterior after `counts`.
    pub fn update(self, counts: GroupCounts) -> Self {
        Self {
            alpha: self.alpha + counts.successes() as f64,
            beta: self.beta + (counts.trials() - counts.successes()) as f64,
        }
    }

    /// Equal-tailed credible interval with total tail mass `delta`.
    pub fn credible_interval(self, delta: Level) -> StatResult<(f64, f64)> {
        beta_interval(self.alpha, self.beta, delta)
    }
}

/// Clamp range for fitted concentrations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConcentrationBounds {
    min: f64,
    max: f64,
}

impl ConcentrationBounds {
    /// `0 < min <= max`, both finite.
    pub fn new(min: f64, max: f64) -> StatResult<Self> {
        validate::parameter(min.is_finite() && min > 0.0, "min", "finite and > 0")?;
        validate::parameter(max.is_finite() && max >= min, "max", "finite and >= min")?;
        Ok(Self { min, max })
    }
}

/// Method-of-moments concentration of the siblings around `mean`. With fewer
/// than two informative siblings, or no excess spread, the result is `max`.
pub fn fit_concentration(siblings: &[GroupCounts], mean: f64, bounds: ConcentrationBounds) -> f64 {
    let informative: Vec<GroupCounts> = siblings
        .iter()
        .copied()
        .filter(|g| g.trials() > 0)
        .collect();
    let trial_counts: Vec<f64> = informative.iter().map(|g| g.trials() as f64).collect();
    let trials = serial_sum(&trial_counts);
    let spread = mean * (1.0 - mean);
    if informative.len() < 2 || spread <= 0.0 || trials <= informative.len() as f64 {
        return bounds.max;
    }
    let rho = intra_class_correlation(&informative, mean, spread, trials);
    if rho <= 0.0 {
        return bounds.max;
    }
    (1.0 / rho - 1.0).clamp(bounds.min, bounds.max)
}

/// `rho = 1 / (M + 1)` solved from the trial-weighted rate variance, whose
/// beta-binomial expectation is `spread (G + rho (N - G)) / N`.
fn intra_class_correlation(groups: &[GroupCounts], mean: f64, spread: f64, trials: f64) -> f64 {
    let count = groups.len() as f64;
    let mut weighted = 0.0;
    for g in groups {
        let rate = g.successes() as f64 / g.trials() as f64;
        weighted += g.trials() as f64 * (rate - mean) * (rate - mean);
    }
    let variance = weighted / trials * count / (count - 1.0);
    (variance * trials / spread - count) / (trials - count)
}

/// A hierarchy of named groups; leaves hold counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolTree {
    /// Observed counts.
    Leaf(GroupCounts),
    /// Named children.
    Branch(BTreeMap<String, PoolTree>),
}

/// A pooled node: aggregate counts, posterior and pooled children.
#[derive(Debug, Clone, PartialEq)]
pub struct PooledNode {
    /// Counts summed over the subtree.
    pub counts: GroupCounts,
    /// Posterior for this node.
    pub posterior: BetaDistribution,
    /// Pooled children in key order.
    pub children: BTreeMap<String, PooledNode>,
}

fn aggregate(tree: &PoolTree) -> StatResult<GroupCounts> {
    match tree {
        PoolTree::Leaf(counts) => Ok(*counts),
        PoolTree::Branch(children) => children
            .values()
            .try_fold(GroupCounts::default(), |acc, child| {
                acc.checked_add(aggregate(child)?)
            }),
    }
}

/// Pool a hierarchy under `root_prior`.
pub fn pool_hierarchy(
    tree: &PoolTree,
    root_prior: BetaDistribution,
    bounds: ConcentrationBounds,
) -> StatResult<PooledNode> {
    let counts = aggregate(tree)?;
    let posterior = root_prior.update(counts);
    let children = match tree {
        PoolTree::Leaf(_) => BTreeMap::new(),
        PoolTree::Branch(branch) => pool_children(branch, posterior.mean(), bounds)?,
    };
    Ok(PooledNode {
        counts,
        posterior,
        children,
    })
}

fn pool_children(
    branch: &BTreeMap<String, PoolTree>,
    mean: f64,
    bounds: ConcentrationBounds,
) -> StatResult<BTreeMap<String, PooledNode>> {
    let sibling_counts = branch
        .values()
        .map(aggregate)
        .collect::<StatResult<Vec<_>>>()?;
    let concentration = fit_concentration(&sibling_counts, mean, bounds);
    let prior = BetaDistribution::new(mean * concentration, (1.0 - mean) * concentration)?;
    branch
        .iter()
        .map(|(key, child)| Ok((key.clone(), pool_hierarchy(child, prior, bounds)?)))
        .collect()
}
