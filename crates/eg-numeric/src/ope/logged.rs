//! Logged bandit decisions with exact logging propensities, and support checks.
//!
//! A record holds the executed action, its observed reward, the logging
//! policy's exact propensity for every action, the evaluated (target) policy's
//! probability for every action and, optionally, a reward model's prediction
//! for every action. Off-policy estimates are only defined where the target
//! policy's support lies inside the logging support: a target action with
//! positive probability but logging propensity 0 is refused, and
//! [`support_report`] states how much target mass lies outside.

use crate::detkernel::rational::sums_to_one;
use crate::detkernel::{validate, Propensity, StatError, StatResult};

/// One logged decision.
#[derive(Debug, Clone, PartialEq)]
pub struct LoggedDecision {
    action: usize,
    reward: f64,
    logging: Vec<Propensity>,
    target: Vec<f64>,
    reward_model: Option<Vec<f64>>,
}

impl LoggedDecision {
    /// Validate a record: the logging propensities sum to exactly one, the
    /// target is a probability vector over the same actions, the reward is
    /// finite, and the executed action had positive logging propensity.
    pub fn new(
        action: usize,
        reward: f64,
        logging: Vec<Propensity>,
        target: Vec<f64>,
    ) -> StatResult<Self> {
        validate::non_empty(&logging, "logging propensities")?;
        validate::same_len(logging.len(), target.len(), "target policy")?;
        validate::labels_below(&[action], logging.len())?;
        validate::all_finite(&[reward], "reward")?;
        validate::probability_vector(&target, "target policy")?;
        validate::parameter(sums_to_one(&logging)?, "logging propensities", "sum exactly to 1")?;
        if logging[action].is_zero() {
            return Err(StatError::ZeroLoggingPropensity { action });
        }
        Ok(Self {
            action,
            reward,
            logging,
            target,
            reward_model: None,
        })
    }

    /// Attach a finite reward-model prediction for every action.
    pub fn with_reward_model(mut self, predictions: Vec<f64>) -> StatResult<Self> {
        validate::same_len(self.logging.len(), predictions.len(), "reward model")?;
        validate::all_finite(&predictions, "reward model")?;
        self.reward_model = Some(predictions);
        Ok(self)
    }

    /// Executed action.
    pub fn action(&self) -> usize {
        self.action
    }

    /// Observed reward.
    pub fn reward(&self) -> f64 {
        self.reward
    }

    /// Logging propensities.
    pub fn logging(&self) -> &[Propensity] {
        &self.logging
    }

    /// Target policy probabilities.
    pub fn target(&self) -> &[f64] {
        &self.target
    }

    /// Reward-model predictions, if attached.
    pub fn reward_model(&self) -> Option<&[f64]> {
        self.reward_model.as_deref()
    }

    /// Importance weight of `action`: target over logging, 0 where the target
    /// has no mass, `None` where the logging propensity is 0 but the target is not.
    pub fn weight_of(&self, action: usize) -> Option<f64> {
        if self.target[action] == 0.0 {
            return Some(0.0);
        }
        self.logging[action].importance_weight(self.target[action])
    }

    /// Importance weight of the executed action (always defined).
    pub fn executed_weight(&self) -> f64 {
        self.weight_of(self.action).unwrap_or(0.0)
    }

    /// The first action with target mass and logging propensity 0.
    pub fn unsupported_action(&self) -> Option<usize> {
        (0..self.target.len()).find(|&a| self.weight_of(a).is_none())
    }

    /// Target probability mass on actions outside the logging support.
    pub fn unsupported_mass(&self) -> f64 {
        let mut mass = 0.0;
        for (p, logged) in self.target.iter().zip(&self.logging) {
            if logged.is_zero() {
                mass += p;
            }
        }
        mass
    }
}

/// How much of the target policy lies outside the logging support.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SupportReport {
    /// Records examined.
    pub records: u64,
    /// Records with any unsupported target mass.
    pub unsupported_records: u64,
    /// Mean unsupported target mass per record.
    pub mean_unsupported_mass: f64,
}

/// Report unsupported target mass without refusing.
pub fn support_report(records: &[LoggedDecision]) -> StatResult<SupportReport> {
    validate::non_empty(records, "logged decisions")?;
    let mut unsupported_records = 0;
    let mut mass = 0.0;
    for record in records {
        let record_mass = record.unsupported_mass();
        unsupported_records += u64::from(record_mass > 0.0);
        mass += record_mass;
    }
    Ok(SupportReport {
        records: records.len() as u64,
        unsupported_records,
        mean_unsupported_mass: mass / records.len() as f64,
    })
}

/// Refuse an empty log or any record whose target leaves the logging support.
pub fn require_support(records: &[LoggedDecision]) -> StatResult<()> {
    validate::non_empty(records, "logged decisions")?;
    for (index, record) in records.iter().enumerate() {
        if let Some(action) = record.unsupported_action() {
            return Err(StatError::UnsupportedAction {
                record: index,
                action,
            });
        }
    }
    Ok(())
}
