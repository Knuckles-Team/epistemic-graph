//! Walk-forward policy replay (EH-528, ANALYTICS-HARVEST AH-08).
//!
//! The steps are time-ordered decision points. Each carries its options and
//! each option's utility, which by the declared `PolicyIndependent` assumption
//! does not depend on what the policy chose. For every walk-forward fold the
//! candidate is prepared on the fold's training steps (a refit, or nothing),
//! then allocates the shared budget over each test step's options; the
//! incumbent does the same. A step decided with training data recorded at or
//! after it is a look-ahead and refuses the whole run.
//!
//! The kernel is policy-agnostic: [`ReplayPolicy`] is what a decision head,
//! a uniform baseline or any other allocator implements.

mod budget;
mod folds;
mod head;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use eg_types::decision::statistical::StatisticalErrorCode;

pub use budget::{max_drawdown, mean, proportional, sharpe};
pub use folds::{walk_forward, Fold};
pub use head::{gold_steps, head_digest, time_ordered, HeadReplay};

use super::refusal::{Refusal, RefusalResult};
use eg_types::decision::replay::WalkForward;

/// One replayable decision point.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayStep {
    /// When the step's inputs were recorded.
    pub at_ms: u64,
    pub option_ids: Vec<String>,
    /// Each option's utility, aligned with `option_ids`.
    pub utilities: Vec<f64>,
}

/// A policy the replay runs forward.
pub trait ReplayPolicy {
    /// Get ready to replay one fold after training on `train` (step
    /// indices). Returns the digest of the head the fold replays.
    fn prepare(&mut self, train: &[usize]) -> RefusalResult<String>;
    /// The requested allocation over step `index`'s options, aligned with its
    /// `option_ids`; `None` abstains (allocates nothing).
    fn requests(&self, index: usize) -> RefusalResult<Option<Vec<f64>>>;
}

/// The uniform allocation over every step's options: the default incumbent.
#[derive(Debug, Clone, Copy)]
pub struct Uniform<'a> {
    pub steps: &'a [ReplayStep],
}

impl ReplayPolicy for Uniform<'_> {
    fn prepare(&mut self, _train: &[usize]) -> RefusalResult<String> {
        Ok("uniform".to_string())
    }

    fn requests(&self, index: usize) -> RefusalResult<Option<Vec<f64>>> {
        let width = self.steps[index].option_ids.len();
        Ok((width > 0).then(|| vec![1.0 / width as f64; width]))
    }
}

/// One fold replayed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FoldOutcome {
    pub fold: Fold,
    pub head_digest: String,
    /// The candidate's utility at each test step.
    pub path: Vec<f64>,
    pub incumbent_path: Vec<f64>,
    /// Mean utility over the fold's training steps (the in-sample leg of the
    /// overfitting statistic), candidate and incumbent.
    pub insample: f64,
    pub incumbent_insample: f64,
    pub abstained: u32,
    /// Per-option credit in this fold, retained so a resumed job can rebuild
    /// the aggregate without recomputing committed folds.
    pub contributions: BTreeMap<String, (f64, f64)>,
}

/// A whole replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayOutcome {
    pub folds: Vec<FoldOutcome>,
    /// Option id -> (applied, utility), summed over the candidate's test steps.
    pub contributions: BTreeMap<String, (f64, f64)>,
}

impl ReplayOutcome {
    /// The candidate's utility at every replayed step, in time order.
    pub fn path(&self) -> Vec<f64> {
        self.folds.iter().flat_map(|f| f.path.clone()).collect()
    }

    /// The incumbent's utility at every replayed step, in time order.
    pub fn incumbent_path(&self) -> Vec<f64> {
        self.folds
            .iter()
            .flat_map(|f| f.incumbent_path.clone())
            .collect()
    }
}

/// One step decided: the applied amounts, or `None` when the policy abstained.
struct Decided {
    applied: Option<Vec<f64>>,
    utility: f64,
}

/// The steps and the budget every fold shares.
struct Replayer<'a> {
    steps: &'a [ReplayStep],
    cap: f64,
}

impl Replayer<'_> {
    fn decide(&self, policy: &dyn ReplayPolicy, index: usize) -> RefusalResult<Decided> {
        let step = &self.steps[index];
        let Some(requests) = policy.requests(index)? else {
            return Ok(Decided {
                applied: None,
                utility: 0.0,
            });
        };
        if requests.len() != step.utilities.len() || requests.iter().any(|r| !r.is_finite()) {
            return Err(Refusal::new(
                StatisticalErrorCode::ReplaySpecInvalid,
                format!("the policy's requests at step {index} do not match its options"),
            ));
        }
        let applied = proportional(&requests, self.cap);
        let utility = applied
            .iter()
            .zip(&step.utilities)
            .map(|(a, u)| a * u)
            .sum();
        Ok(Decided {
            applied: Some(applied),
            utility,
        })
    }

    fn insample(&self, policy: &dyn ReplayPolicy, train: &[usize]) -> RefusalResult<f64> {
        let utilities = train
            .iter()
            .map(|&index| self.decide(policy, index).map(|d| d.utility))
            .collect::<RefusalResult<Vec<f64>>>()?;
        Ok(mean(&utilities))
    }

    fn check_no_look_ahead(&self, fold: &Fold) -> RefusalResult<()> {
        let first_test = self.steps[fold.test.start].at_ms;
        match fold.train.iter().find(|&&i| self.steps[i].at_ms >= first_test) {
            Some(&late) => Err(Refusal::new(
                StatisticalErrorCode::LookAhead,
                format!(
                    "training step {late} (recorded at {}) is not before the test window starting at {first_test}",
                    self.steps[late].at_ms
                ),
            )),
            None => Ok(()),
        }
    }

    fn credit(
        &self,
        contributions: &mut BTreeMap<String, (f64, f64)>,
        index: usize,
        applied: &[f64],
    ) {
        let step = &self.steps[index];
        for ((id, amount), utility) in step.option_ids.iter().zip(applied).zip(&step.utilities) {
            let entry = contributions.entry(id.clone()).or_insert((0.0, 0.0));
            entry.0 += amount;
            entry.1 += amount * utility;
        }
    }

    fn fold(
        &self,
        fold: Fold,
        pair: (&mut dyn ReplayPolicy, &mut dyn ReplayPolicy),
    ) -> RefusalResult<FoldOutcome> {
        let (candidate, incumbent) = pair;
        self.check_no_look_ahead(&fold)?;
        let head_digest = candidate.prepare(&fold.train)?;
        incumbent.prepare(&fold.train)?;
        let mut outcome = FoldOutcome {
            head_digest,
            path: Vec::with_capacity(fold.test.len()),
            incumbent_path: Vec::with_capacity(fold.test.len()),
            insample: self.insample(candidate, &fold.train)?,
            incumbent_insample: self.insample(incumbent, &fold.train)?,
            abstained: 0,
            contributions: BTreeMap::new(),
            fold: fold.clone(),
        };
        for index in fold.test {
            let decided = self.decide(candidate, index)?;
            match &decided.applied {
                Some(applied) => self.credit(&mut outcome.contributions, index, applied),
                None => outcome.abstained += 1,
            }
            outcome.path.push(decided.utility);
            outcome
                .incumbent_path
                .push(self.decide(incumbent, index)?.utility);
        }
        Ok(outcome)
    }
}

/// Execute one fold independently; a job worker persists the returned outcome
/// before it advances to the next fold.
pub fn replay_fold(
    steps: &[ReplayStep],
    fold: Fold,
    cap: f64,
    candidate: &mut dyn ReplayPolicy,
    incumbent: &mut dyn ReplayPolicy,
) -> RefusalResult<FoldOutcome> {
    if !(cap.is_finite() && cap > 0.0) {
        return Err(Refusal::new(
            StatisticalErrorCode::ReplaySpecInvalid,
            "the shared cap must be positive",
        ));
    }
    Replayer { steps, cap }.fold(fold, (candidate, incumbent))
}

/// Combine durable fold outcomes in their original order after a resume.
pub fn collect_replay(folds: Vec<FoldOutcome>) -> ReplayOutcome {
    let mut contributions = BTreeMap::new();
    for fold in &folds {
        for (option_id, (applied, utility)) in &fold.contributions {
            let total = contributions.entry(option_id.clone()).or_insert((0.0, 0.0));
            total.0 += *applied;
            total.1 += *utility;
        }
    }
    ReplayOutcome {
        folds,
        contributions,
    }
}

/// Replay `candidate` against `incumbent` over `steps` (time-ordered), every
/// step's allocation scaled under the shared `cap`.
pub fn replay(
    steps: &[ReplayStep],
    spec: &WalkForward,
    cap: f64,
    candidate: &mut dyn ReplayPolicy,
    incumbent: &mut dyn ReplayPolicy,
) -> RefusalResult<ReplayOutcome> {
    if !(cap.is_finite() && cap > 0.0) {
        return Err(Refusal::new(
            StatisticalErrorCode::ReplaySpecInvalid,
            "the shared cap must be positive",
        ));
    }
    let mut folds = Vec::new();
    for fold in walk_forward(steps.len(), spec)? {
        folds.push(replay_fold(steps, fold, cap, candidate, incumbent)?);
    }
    Ok(collect_replay(folds))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod checkpoint_tests;
