//! A decision head as a [`ReplayPolicy`], over a full-label dataset.
//!
//! Each admitted gold item is one step: its candidates are the options and an
//! option's utility is 1 when it is in the item's acceptability set, else 0.
//! Acceptability is fixed by the label, not by what the head chose, which is
//! exactly the policy-independence replay needs. A bandit label carries an
//! outcome for the executed option only, so it is refused here.

use eg_types::decision::statistical::body::{canonical_body_bytes, content_digest_of};
use eg_types::decision::statistical::dataset::{ItemLabel, LabelledDataset, LabelledItem};
use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::statistical::StatisticalErrorCode;

use super::{ReplayPolicy, ReplayStep};
use crate::decision::evaluate::read_item;
use crate::decision::fit::{fit, FitSpec};
use crate::decision::refusal::{Refusal, RefusalResult};

/// `items` in time order: by `recorded_at_ms`, ties by `item_id`.
pub fn time_ordered<'a>(items: &[&'a LabelledItem]) -> Vec<&'a LabelledItem> {
    let mut ordered = items.to_vec();
    ordered.sort_by(|a, b| {
        (a.recorded_at_ms, a.item_id.as_str()).cmp(&(b.recorded_at_ms, b.item_id.as_str()))
    });
    ordered
}

fn gold_step(item: &LabelledItem) -> RefusalResult<ReplayStep> {
    let ItemLabel::Gold { acceptable, .. } = &item.label else {
        return Err(Refusal::new(
            StatisticalErrorCode::ReplayPolicyDependent,
            format!(
                "item {} carries a bandit label: its outcome depends on the executed option; use the off-policy estimators",
                item.item_id
            ),
        ));
    };
    let utilities = item
        .candidate_ids
        .iter()
        .map(|id| f64::from(u8::from(acceptable.iter().any(|a| a == id))))
        .collect();
    Ok(ReplayStep {
        at_ms: item.recorded_at_ms,
        option_ids: item.candidate_ids.iter().cloned().collect(),
        utilities,
    })
}

/// One replay step per time-ordered gold item.
pub fn gold_steps(items: &[&LabelledItem]) -> RefusalResult<Vec<ReplayStep>> {
    items.iter().map(|item| gold_step(item)).collect()
}

/// The content digest a published head of this body would carry.
pub fn head_digest(head: &DecisionHeadBody) -> RefusalResult<String> {
    canonical_body_bytes(head)
        .map(|bytes| content_digest_of(&bytes))
        .map_err(|detail| Refusal::new(StatisticalErrorCode::HeadInvalid, detail))
}

/// A head replayed over `items` (time-ordered, aligned with the steps),
/// refitted on each fold's training items when `refit` is set.
pub struct HeadReplay<'a> {
    pub dataset: &'a LabelledDataset,
    pub items: &'a [&'a LabelledItem],
    pub head: DecisionHeadBody,
    pub refit: Option<FitSpec<'a>>,
}

impl ReplayPolicy for HeadReplay<'_> {
    fn prepare(&mut self, train: &[usize]) -> RefusalResult<String> {
        if let Some(spec) = &self.refit {
            let training: Vec<&LabelledItem> = train.iter().map(|&i| self.items[i]).collect();
            self.head = fit(self.dataset, &training, spec)?;
        }
        head_digest(&self.head)
    }

    fn requests(&self, index: usize) -> RefusalResult<Option<Vec<f64>>> {
        Ok(read_item(&self.head, self.dataset, self.items[index])?.probabilities)
    }
}
