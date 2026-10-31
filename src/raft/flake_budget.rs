//! MEDULLA-DURABLE-KERNEL-R014.1: the typed flake-budget / retry-policy model.
//!
//! MEDULLA-DURABLE-KERNEL-R014 requires the leader-rebalance and three-node
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

/// One failed diagnostic execution retained while an optional retry is pending.
///
/// The trace is deliberately owned by the log rather than replaced by a later
/// successful attempt: a flaky success must not erase the failure that made the
/// diagnostic retry necessary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticFailure {
    /// One-based execution number, including the initial attempt.
    pub attempt: usize,
    /// The bounded failure/trace text captured for this attempt.
    pub trace: String,
}

/// Decision returned after recording one diagnostic failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticRetryDecision {
    /// The caller may run another attempt after this backoff.
    Retry { attempt: usize, backoff_ms: u64 },
    /// The recorded failure is terminal for this diagnostic run.
    Exhausted { attempt: usize },
}

/// Retains diagnostic failures across optional retries without allowing a retry
/// past the declared [`FlakeBudget`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiagnosticRetryLog {
    failures: Vec<DiagnosticFailure>,
}

impl DiagnosticRetryLog {
    /// Create an empty failure log for one diagnostic run.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a failed attempt and return the bounded next-step decision.
    ///
    /// The first failure is attempt one; `max_retries` therefore permits at
    /// most `max_retries + 1` recorded failures. Once the terminal failure is
    /// retained, another call is refused instead of silently retrying forever.
    pub fn record_failure(
        &mut self,
        budget: &FlakeBudget,
        trace: impl Into<String>,
    ) -> Result<DiagnosticRetryDecision, String> {
        budget.validate()?;
        let attempt = self
            .failures
            .len()
            .checked_add(1)
            .ok_or_else(|| "diagnostic retry attempt count overflowed".to_string())?;
        let max_attempts = (budget.max_retries as usize).saturating_add(1);
        if attempt > max_attempts {
            return Err(format!(
                "diagnostic retry budget exhausted after {max_attempts} attempts"
            ));
        }

        self.failures.push(DiagnosticFailure {
            attempt,
            trace: trace.into(),
        });
        if attempt <= budget.max_retries as usize {
            let backoff_ms = budget.backoff_for(attempt as u32)?;
            Ok(DiagnosticRetryDecision::Retry {
                attempt,
                backoff_ms,
            })
        } else {
            Ok(DiagnosticRetryDecision::Exhausted { attempt })
        }
    }

    /// Return every retained failure in execution order.
    pub fn failures(&self) -> &[DiagnosticFailure] {
        &self.failures
    }
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

    // spec: MEDULLA-DURABLE-KERNEL-R014.1
    #[test]
    fn well_formed_budget_is_accepted() {
        FlakeBudget::new(3, 50, 2_000).unwrap();
    }

    // spec: MEDULLA-DURABLE-KERNEL-R014.1
    #[test]
    fn zero_retries_is_refused() {
        let err = FlakeBudget::new(0, 50, 2_000).unwrap_err();
        assert!(err.contains("at least one retry"), "{err}");
    }

    // spec: MEDULLA-DURABLE-KERNEL-R014.1
    #[test]
    fn zero_backoff_is_refused() {
        let err = FlakeBudget::new(3, 0, 2_000).unwrap_err();
        assert!(err.contains("retry storm"), "{err}");
    }

    // spec: MEDULLA-DURABLE-KERNEL-R014.1
    #[test]
    fn max_backoff_below_backoff_is_refused() {
        let err = FlakeBudget::new(3, 500, 100).unwrap_err();
        assert!(err.contains("max_backoff_ms must be"), "{err}");
    }

    // spec: MEDULLA-DURABLE-KERNEL-R014.1
    #[test]
    fn retry_beyond_budget_is_refused() {
        let budget = FlakeBudget::new(2, 10, 1_000).unwrap();
        let err = budget.backoff_for(3).unwrap_err();
        assert!(err.contains("exceeds max_retries"), "{err}");
    }

    // spec: MEDULLA-DURABLE-KERNEL-R014.1
    #[test]
    fn zero_attempt_is_refused() {
        let budget = FlakeBudget::new(2, 10, 1_000).unwrap();
        let err = budget.backoff_for(0).unwrap_err();
        assert!(err.contains("exceeds max_retries"), "{err}");
    }

    // spec: MEDULLA-DURABLE-KERNEL-R014.1
    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let budget = FlakeBudget::new(5, 10, 100).unwrap();
        assert_eq!(budget.backoff_for(1).unwrap(), 10);
        assert_eq!(budget.backoff_for(2).unwrap(), 20);
        assert_eq!(budget.backoff_for(3).unwrap(), 40);
        // Would be 160 uncapped; the budget's max_backoff_ms clamps it.
        assert_eq!(budget.backoff_for(5).unwrap(), 100);
    }

    #[test]
    fn diagnostic_retry_log_preserves_failures_before_a_later_success() {
        let budget = FlakeBudget::new(2, 10, 100).unwrap();
        let mut log = DiagnosticRetryLog::new();
        assert_eq!(
            log.record_failure(&budget, "first failure trace"),
            Ok(DiagnosticRetryDecision::Retry {
                attempt: 1,
                backoff_ms: 10,
            })
        );
        assert_eq!(
            log.record_failure(&budget, "second failure trace"),
            Ok(DiagnosticRetryDecision::Retry {
                attempt: 2,
                backoff_ms: 20,
            })
        );

        // A later successful retry is owned by the caller; the retained failed
        // attempts remain available for the diagnostic receipt.
        assert_eq!(
            log.failures(),
            [
                DiagnosticFailure {
                    attempt: 1,
                    trace: "first failure trace".into(),
                },
                DiagnosticFailure {
                    attempt: 2,
                    trace: "second failure trace".into(),
                },
            ]
        );
    }

    #[test]
    fn diagnostic_retry_log_refuses_after_terminal_failure() {
        let budget = FlakeBudget::new(1, 10, 100).unwrap();
        let mut log = DiagnosticRetryLog::new();
        assert_eq!(
            log.record_failure(&budget, "initial trace"),
            Ok(DiagnosticRetryDecision::Retry {
                attempt: 1,
                backoff_ms: 10,
            })
        );
        assert_eq!(
            log.record_failure(&budget, "terminal trace"),
            Ok(DiagnosticRetryDecision::Exhausted { attempt: 2 })
        );
        assert!(log
            .record_failure(&budget, "out-of-budget trace")
            .unwrap_err()
            .contains("budget exhausted"));
        assert_eq!(log.failures().len(), 2);
    }
}
