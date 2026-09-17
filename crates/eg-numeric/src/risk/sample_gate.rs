//! Minimum-sample gates (`n_min`). Below the gate a step makes no coverage or
//! risk claim: it abstains or returns an explicitly uncalibrated advisory score.

use crate::detkernel::{validate, Level, StatError, StatResult};
use std::collections::BTreeMap;

/// A minimum sample size, at least 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SampleGate {
    n_min: u64,
}

/// Whether a sample size clears a gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SampleAssessment {
    /// `n >= n_min`: a claim may be made.
    Sufficient { n: u64 },
    /// `n < n_min`: no claim.
    Insufficient { n: u64, n_min: u64 },
}

impl SampleGate {
    /// A gate at `n_min >= 1`.
    pub fn new(n_min: u64) -> StatResult<Self> {
        validate::parameter(n_min >= 1, "n_min", "n_min >= 1")?;
        Ok(Self { n_min })
    }

    /// The gate for split conformal at `alpha` combined with a policy minimum:
    /// the larger of `policy_n_min` and `ceil((1 - alpha) / alpha)`, below which
    /// every split-conformal set is the trivial full set.
    pub fn for_conformal(alpha: Level, policy_n_min: u64) -> StatResult<Self> {
        Self::new(policy_n_min.max(alpha.conformal_minimum_n()).max(1))
    }

    /// The minimum.
    pub fn n_min(self) -> u64 {
        self.n_min
    }

    /// `true` when `n` clears the gate.
    pub fn admits(self, n: u64) -> bool {
        n >= self.n_min
    }

    /// Assess `n`.
    pub fn assess(self, n: u64) -> SampleAssessment {
        if self.admits(n) {
            SampleAssessment::Sufficient { n }
        } else {
            SampleAssessment::Insufficient {
                n,
                n_min: self.n_min,
            }
        }
    }

    /// Refuse `n` below the gate with a typed error naming `what`.
    pub fn require(self, n: u64, what: &'static str) -> StatResult<()> {
        if self.admits(n) {
            return Ok(());
        }
        Err(StatError::InsufficientSamples {
            what,
            required: self.n_min,
            actual: n,
        })
    }

    /// Assess per-class counts in key order.
    pub fn assess_classes<K: Ord + Clone>(
        self,
        counts: &BTreeMap<K, u64>,
    ) -> BTreeMap<K, SampleAssessment> {
        counts
            .iter()
            .map(|(key, &n)| (key.clone(), self.assess(n)))
            .collect()
    }
}

/// Approximate standard deviation of split-conformal realised coverage,
/// `sqrt(alpha (1 - alpha) / (n + 2))`: about 0.03 at `alpha = 0.1, n = 100`.
pub fn coverage_standard_deviation(alpha: Level, n: u64) -> f64 {
    let a = alpha.to_f64();
    (a * (1.0 - a) / (n as f64 + 2.0)).sqrt()
}
