//! Pure context-pressure selection (EG-DECISION-ENGINE-R128.5).

use super::{ContextPolicy, HarnessPart};
use crate::contract::BoundedVec;
use crate::decision::numeric::UnitRationalWire;

const Q32: u64 = 1 << 32;

/// Rule thresholds, passed explicitly after the caller resolves its policy.
/// Tenant-policy lookup and premise sealing remain R128.4/R128.11.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessDefaults {
    pub soft_threshold: UnitRationalWire,
    pub hard_threshold: UnitRationalWire,
    pub recent_window_fraction: UnitRationalWire,
}

impl Default for HarnessDefaults {
    fn default() -> Self {
        let fraction = |numerator, denominator| {
            UnitRationalWire::new(Q32 * numerator / denominator, Q32)
                .expect("default fractions are within one")
        };
        Self {
            soft_threshold: fraction(3, 5),
            hard_threshold: fraction(17, 20),
            recent_window_fraction: fraction(3, 10),
        }
    }
}

impl HarnessDefaults {
    /// All thresholds use Q32; soft precedes hard, and each fraction is positive.
    pub fn validate(&self) -> Result<(), String> {
        for fraction in [
            self.soft_threshold,
            self.hard_threshold,
            self.recent_window_fraction,
        ] {
            if fraction.denominator() != Q32 || fraction.numerator() == 0 {
                return Err("harness thresholds require positive Q32 fractions".into());
            }
        }
        if self.soft_threshold.numerator() >= self.hard_threshold.numerator() {
            return Err("harness soft threshold must precede hard threshold".into());
        }
        Ok(())
    }
}

/// Pressure and the exact thresholds used alongside the selected policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextPolicyDecision {
    pub part: HarnessPart<ContextPolicy>,
    /// `floor(expected_trajectory_tokens * 2^32 / context_window_tokens)`.
    /// Unsigned storage covers the full u32 token range, including pressure > 1.
    pub pressure_q32: Option<u64>,
    pub thresholds: HarnessDefaults,
}

/// Resolve context handling without treating a point estimate as a hard bound.
/// `task_token_upper_bound` must be an enforced bound on the complete context
/// for the task class, including prompt, generated and tool tokens. A measured
/// maximum or an expected trajectory is not such a bound. Its source and
/// enforcement are the caller's responsibility; absent it, retain compaction.
pub fn decide_context_policy(
    context_window_tokens: Option<u32>,
    expected_trajectory_tokens: Option<u32>,
    task_token_upper_bound: Option<u32>,
    thresholds: &HarnessDefaults,
) -> Result<ContextPolicyDecision, String> {
    thresholds.validate()?;
    if context_window_tokens == Some(0) {
        return Err("context window must be non-zero".into());
    }
    if let (Some(expected), Some(bound)) = (expected_trajectory_tokens, task_token_upper_bound) {
        if expected > bound {
            return Err("trajectory estimate exceeds the task token upper bound".into());
        }
    }
    match (context_window_tokens, expected_trajectory_tokens) {
        (Some(window), Some(expected)) => Ok(ContextPolicyDecision {
            part: HarnessPart::Decided {
                value: if task_token_upper_bound.is_some_and(|bound| bound <= window) {
                    ContextPolicy::None
                } else {
                    ContextPolicy::ElideThenSummarize
                },
            },
            pressure_q32: Some((u64::from(expected) << 32) / u64::from(window)),
            thresholds: thresholds.clone(),
        }),
        _ => Ok(missing_context_inputs(
            context_window_tokens,
            expected_trajectory_tokens,
            thresholds,
        )),
    }
}

fn missing_context_inputs(
    window: Option<u32>,
    trajectory: Option<u32>,
    thresholds: &HarnessDefaults,
) -> ContextPolicyDecision {
    let mut missing = Vec::new();
    if window.is_none() {
        missing.push("contextWindowTokens".into());
    }
    if trajectory.is_none() {
        missing.push("expected_trajectory_tokens".into());
    }
    ContextPolicyDecision {
        part: HarnessPart::Abstained {
            missing_fact: BoundedVec::new(missing).expect("at most two missing facts"),
            conservative_default: ContextPolicy::ElideThenSummarize,
        },
        pressure_q32: None,
        thresholds: thresholds.clone(),
    }
}

#[cfg(test)]
mod tests;
