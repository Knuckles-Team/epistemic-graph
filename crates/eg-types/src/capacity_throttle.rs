//! Error-budget adaptive throttling on a `CapacityCell` (EH-406).
//!
//! An operator declares a cell's `capacity` (the ceiling) and, optionally, an
//! AIMD [`CapacityThrottlePolicy`]. An automatic controller reports one
//! [`ErrorBudgetSample`] per observation window -- requests and 5XX/error
//! counts from the live error-rate facts -- and the engine moves the cell's
//! effective ceiling:
//!
//! * **multiplicative decrease** when the window's error rate exceeds the
//!   declared budget: `ceiling * decrease_per_mille / 1000`, never below the
//!   declared floor;
//! * **additive increase** only on recovery evidence -- a window with enough
//!   requests and an error rate at or below the recovery threshold -- by
//!   `increase_step`, and never above the declared `capacity`;
//! * **hold** otherwise (too few samples, inside the budget but not healthy,
//!   inside the cooldown, or already at a bound).
//!
//! Automatic actions therefore only narrow, or give back what they narrowed.
//! Raising the declared ceiling, or restoring it in one step, is the
//! operator's `UpdateCapacityCell` (`capacity:admin`); the throttle method
//! (`capacity:throttle`) cannot touch `capacity`, the policy, or the floor.
//! Every action, a hold included, is appended to the cell's bounded history
//! and to the graph's hash-chained audit log.
//!
//! A narrowed ceiling gates NEW admissions only; leases already held keep
//! running to their own expiry.

use serde::{Deserialize, Serialize};

/// Most action records a cell's throttle keeps (newest last).
pub const MAX_THROTTLE_HISTORY: usize = 32;
/// Error rates are parts per million.
pub const PPM: u64 = 1_000_000;

/// The operator-declared AIMD bounds of one cell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityThrottlePolicy {
    /// A window whose error rate is ABOVE this narrows the cell.
    pub error_budget_ppm: u64,
    /// A window whose error rate is AT OR BELOW this is recovery evidence.
    pub recovery_ppm: u64,
    /// A window with fewer requests is no evidence either way.
    pub min_samples: u64,
    /// Multiplicative decrease factor, per mille (500 halves the ceiling).
    pub decrease_per_mille: u64,
    /// Additive increase per healthy window.
    pub increase_step: u64,
    /// The lowest ceiling a narrowing may reach. At least 1, so a narrowed
    /// cell still admits the traffic that produces recovery evidence.
    pub floor: u64,
    /// Minimum time between two automatic ceiling changes.
    pub cooldown_ms: u64,
}

impl CapacityThrottlePolicy {
    /// The policy must be internally ordered and fit under `declared`.
    pub fn validate(&self, declared: u64) -> Result<(), String> {
        let ordered = self.recovery_ppm <= self.error_budget_ppm && self.error_budget_ppm < PPM;
        let factor = (1..1000).contains(&self.decrease_per_mille);
        let bounded = (1..=declared).contains(&self.floor);
        if !ordered || !factor || !bounded || self.min_samples == 0 || self.increase_step == 0 {
            return Err(
                "throttle policy must satisfy recovery <= budget < 1e6 ppm, 0 < decrease < 1000, \
                 1 <= floor <= capacity, min_samples > 0 and increase_step > 0"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// What one throttle step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ThrottleAction {
    Narrowed,
    Recovered,
    Held,
}

/// Why a throttle step did what it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ThrottleReason {
    ErrorBudgetExceeded,
    HealthyWindow,
    InsufficientSamples,
    WithinBudget,
    CoolingDown,
    AtFloor,
    AtDeclaredCeiling,
}

/// One recorded throttle step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ThrottleActionRecord {
    pub at_ms: u64,
    pub action: ThrottleAction,
    pub reason: ThrottleReason,
    pub from: u64,
    pub to: u64,
    pub requests: u64,
    pub errors: u64,
    pub error_ppm: u64,
    pub window_end_ms: u64,
}

/// A cell's throttle: the declared policy plus the state automatic steps own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityThrottle {
    pub policy: CapacityThrottlePolicy,
    /// The effective ceiling: never above the cell's declared `capacity`.
    pub ceiling: u64,
    pub last_change_at_ms: u64,
    /// End of the newest window already counted; an older or equal window is
    /// refused, so one sample can never be counted twice.
    pub last_window_end_ms: u64,
    pub history: Vec<ThrottleActionRecord>,
}

impl CapacityThrottle {
    /// A fresh throttle, fully open at the declared ceiling.
    pub fn open(policy: CapacityThrottlePolicy, declared: u64) -> Self {
        Self {
            policy,
            ceiling: declared,
            last_change_at_ms: 0,
            last_window_end_ms: 0,
            history: Vec::new(),
        }
    }

    pub fn validate(&self, declared: u64) -> Result<(), String> {
        self.policy.validate(declared)?;
        if self.ceiling > declared || self.history.len() > MAX_THROTTLE_HISTORY {
            return Err("throttle ceiling exceeds the declared capacity".to_string());
        }
        Ok(())
    }
}

/// One observation window's error-budget facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ErrorBudgetSample {
    pub requests: u64,
    /// 5XX / error responses among `requests`.
    pub errors: u64,
    pub window_end_ms: u64,
}

impl ErrorBudgetSample {
    /// Structural bounds, before any cell is read.
    pub fn validate(&self, now_ms: u64) -> Result<(), String> {
        if self.errors > self.requests {
            return Err("an error-budget sample cannot hold more errors than requests".to_string());
        }
        if self.window_end_ms > now_ms {
            return Err("an error-budget sample cannot end in the future".to_string());
        }
        Ok(())
    }

    fn error_ppm(&self) -> u64 {
        let rate = u128::from(self.errors) * u128::from(PPM) / u128::from(self.requests.max(1));
        u64::try_from(rate).unwrap_or(PPM)
    }
}

/// Whether the cooldown since the last change still holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cooldown {
    Elapsed,
    Active,
}

/// The step's decision before it is recorded: action, reason, new ceiling.
type Step = (ThrottleAction, ThrottleReason, u64);

/// Apply one window to a cell's throttle at `now_ms` and record the step.
/// `declared` is the cell's operator-declared capacity, the hard upper bound
/// of every automatic step. Refused (nothing changes) when the cell declares
/// no policy or the window was already counted.
pub fn apply_error_budget(
    throttle: Option<&mut CapacityThrottle>,
    declared: u64,
    sample: &ErrorBudgetSample,
    now_ms: u64,
) -> Result<ThrottleActionRecord, String> {
    let throttle = throttle
        .ok_or_else(|| "THROTTLE_NO_POLICY: the cell declares no throttle policy".to_string())?;
    sample.validate(now_ms)?;
    throttle.validate(declared)?;
    if sample.window_end_ms <= throttle.last_window_end_ms {
        return Err("THROTTLE_STALE_SAMPLE: this window was already counted".to_string());
    }
    let cooling = if throttle.last_change_at_ms > 0
        && now_ms
            < throttle
                .last_change_at_ms
                .saturating_add(throttle.policy.cooldown_ms)
    {
        Cooldown::Active
    } else {
        Cooldown::Elapsed
    };
    let from = throttle.ceiling;
    let (action, reason, to) = decide(&throttle.policy, from, declared, sample, cooling);
    let record = ThrottleActionRecord {
        at_ms: now_ms,
        action,
        reason,
        from,
        to,
        requests: sample.requests,
        errors: sample.errors,
        error_ppm: sample.error_ppm(),
        window_end_ms: sample.window_end_ms,
    };
    throttle.ceiling = to;
    throttle.last_window_end_ms = sample.window_end_ms;
    if to != from {
        throttle.last_change_at_ms = now_ms;
    }
    throttle.history.push(record.clone());
    if throttle.history.len() > MAX_THROTTLE_HISTORY {
        throttle.history.remove(0);
    }
    Ok(record)
}

fn decide(
    policy: &CapacityThrottlePolicy,
    from: u64,
    declared: u64,
    sample: &ErrorBudgetSample,
    cooling: Cooldown,
) -> Step {
    if sample.requests < policy.min_samples {
        return (
            ThrottleAction::Held,
            ThrottleReason::InsufficientSamples,
            from,
        );
    }
    let error_ppm = sample.error_ppm();
    if error_ppm > policy.error_budget_ppm {
        return narrow(policy, from, cooling);
    }
    if error_ppm > policy.recovery_ppm {
        return (ThrottleAction::Held, ThrottleReason::WithinBudget, from);
    }
    recover(policy, from, declared, cooling)
}

fn narrow(policy: &CapacityThrottlePolicy, from: u64, cooling: Cooldown) -> Step {
    if cooling == Cooldown::Active {
        return (ThrottleAction::Held, ThrottleReason::CoolingDown, from);
    }
    let scaled = u128::from(from) * u128::from(policy.decrease_per_mille) / 1000;
    let to = u64::try_from(scaled).unwrap_or(from).max(policy.floor);
    if to >= from {
        return (ThrottleAction::Held, ThrottleReason::AtFloor, from);
    }
    (
        ThrottleAction::Narrowed,
        ThrottleReason::ErrorBudgetExceeded,
        to,
    )
}

fn recover(policy: &CapacityThrottlePolicy, from: u64, declared: u64, cooling: Cooldown) -> Step {
    if from >= declared {
        return (
            ThrottleAction::Held,
            ThrottleReason::AtDeclaredCeiling,
            from,
        );
    }
    if cooling == Cooldown::Active {
        return (ThrottleAction::Held, ThrottleReason::CoolingDown, from);
    }
    let to = from.saturating_add(policy.increase_step).min(declared);
    (ThrottleAction::Recovered, ThrottleReason::HealthyWindow, to)
}

#[cfg(test)]
mod tests;
