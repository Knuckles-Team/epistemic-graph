//! EG-DURABLE-KERNEL-R014.1: the typed flake-budget / retry-policy model.
//!
//! EG-DURABLE-KERNEL-R014 requires the leader-rebalance and three-node
//! placement-admin cluster tests to pass reliably under representative load,
//! not only on an idle host, backed by causal trace capture on any failure.
//! That needs a representative-load generator and a harness that actually
//! drives repeated runs -- both a later child. This is the typed-model half:
//! a [`FlakeBudget`] names exactly how many TRANSIENT (load-induced, never
//! persistent-bug) failures a run may retry past, and at what backoff, so a
//! retry policy is an explicit, validated, bounded decision rather than an
//! ad hoc loop. A malformed budget is REFUSED at construction, never
//! silently clamped into something that retries forever or not at all.

/// How many transient failures a test run under load may retry past, and at
/// what backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlakeBudget {
    /// Maximum number of retry attempts after the first failure. Zero is
    /// refused: a "budget" that allows no retry is not a flake budget, it is
    /// a single-shot run that happens to be mislabeled.
    pub max_retries: u32,
    /// Base backoff before the first retry, in milliseconds.
    pub backoff_ms: u64,
    /// Hard cap on backoff, in milliseconds, after exponential growth.
    pub max_backoff_ms: u64,
}

impl FlakeBudget {
    /// Construct and validate a budget. Refuses a budget that cannot express
    /// a sane retry policy rather than accepting it and misbehaving later.
    pub fn new(max_retries: u32, backoff_ms: u64, max_backoff_ms: u64) -> Result<Self, String> {
        let budget = Self {
            max_retries,
            backoff_ms,
            max_backoff_ms,
        };
        budget.validate()?;
        Ok(budget)
    }

    /// Reject a budget with no retry headroom, a zero backoff (a retry
    /// storm, not a budget), or a cap below the base backoff.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_retries == 0 {
            return Err(
                "flake budget: max_retries must allow at least one retry under load".into(),
            );
        }
        if self.backoff_ms == 0 {
            return Err(
                "flake budget: backoff_ms must be positive (zero backoff is a retry storm, \
                 not a budget)"
                    .into(),
            );
        }
        if self.max_backoff_ms < self.backoff_ms {
            return Err("flake budget: max_backoff_ms must be >= backoff_ms".into());
        }
        Ok(())
    }

    /// Backoff, in milliseconds, before retry attempt `attempt` (1-based),
    /// growing exponentially from `backoff_ms` and capped at
    /// `max_backoff_ms`. An attempt beyond `max_retries` is refused: the
    /// caller must stop retrying and report the failure, never loop past
    /// the declared budget.
    pub fn backoff_for(&self, attempt: u32) -> Result<u64, String> {
        if attempt == 0 || attempt > self.max_retries {
            return Err(format!(
                "flake budget: retry attempt {attempt} exceeds max_retries {} \
                 (or is zero); the caller must stop and report the failure",
                self.max_retries
            ));
        }
        let shift = attempt.saturating_sub(1).min(32);
        let scaled = self.backoff_ms.saturating_mul(1u64 << shift);
        Ok(scaled.min(self.max_backoff_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DURABLE-KERNEL-R014.1
    #[test]
    fn well_formed_budget_is_accepted() {
        FlakeBudget::new(3, 50, 2_000).unwrap();
    }

    // spec: EG-DURABLE-KERNEL-R014.1
    #[test]
    fn zero_retries_is_refused() {
        let err = FlakeBudget::new(0, 50, 2_000).unwrap_err();
        assert!(err.contains("at least one retry"), "{err}");
    }

    // spec: EG-DURABLE-KERNEL-R014.1
    #[test]
    fn zero_backoff_is_refused() {
        let err = FlakeBudget::new(3, 0, 2_000).unwrap_err();
        assert!(err.contains("retry storm"), "{err}");
    }

    // spec: EG-DURABLE-KERNEL-R014.1
    #[test]
    fn max_backoff_below_backoff_is_refused() {
        let err = FlakeBudget::new(3, 500, 100).unwrap_err();
        assert!(err.contains("max_backoff_ms must be"), "{err}");
    }

    // spec: EG-DURABLE-KERNEL-R014.1
    #[test]
    fn retry_beyond_budget_is_refused() {
        let budget = FlakeBudget::new(2, 10, 1_000).unwrap();
        let err = budget.backoff_for(3).unwrap_err();
        assert!(err.contains("exceeds max_retries"), "{err}");
    }

    // spec: EG-DURABLE-KERNEL-R014.1
    #[test]
    fn zero_attempt_is_refused() {
        let budget = FlakeBudget::new(2, 10, 1_000).unwrap();
        let err = budget.backoff_for(0).unwrap_err();
        assert!(err.contains("exceeds max_retries"), "{err}");
    }

    // spec: EG-DURABLE-KERNEL-R014.1
    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let budget = FlakeBudget::new(5, 10, 100).unwrap();
        assert_eq!(budget.backoff_for(1).unwrap(), 10);
        assert_eq!(budget.backoff_for(2).unwrap(), 20);
        assert_eq!(budget.backoff_for(3).unwrap(), 40);
        // Would be 160 uncapped; the budget's max_backoff_ms clamps it.
        assert_eq!(budget.backoff_for(5).unwrap(), 100);
    }
}
