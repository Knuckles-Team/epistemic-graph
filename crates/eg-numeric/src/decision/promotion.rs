//! The promotion protocol for a full-label evaluation (EH-295).
//!
//! On top of accuracy, Brier, ECE and empirical coverage (see
//! [`super::evaluate`]), a candidate head is measured on: soft accuracy and
//! score error (the Laya four, comparable to published numbers); abstention
//! AND accuracy on what it answered -- a good abstainer must be more accurate
//! on what it answers, else its abstention is noise; cost per decision in
//! multiply-accumulates; decision stability -- two independent reads of the
//! same state must be byte-identical, because the decision is recorded and
//! must replay; calibration drift between the earlier and the later half of
//! the items by recorded time; and coverage per calibration class (domain).
//! Each measurement that fails its gate names the gate in the receipt, and a
//! receipt with any failed gate does not pass, so the head is not promoted.

use std::collections::BTreeMap;

use eg_types::contract::BoundedVec;
use eg_types::decision::jobs::{ClassCoverage, PromotionMetrics};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::{StatisticalPolicy, UnitRationalWire};

use super::head_eval::top_index;
use super::quant::{exact_wire, q32};
use super::refusal::{Refusal, RefusalResult};

/// Largest mean cost a promoted head may spend per decision, in
/// multiply-accumulates. The largest resident scorer (64 options, 32
/// features, width 16) spends 217,088.
pub const MAX_MACS_PER_DECISION: u64 = 1 << 18;

/// One evaluated item as the protocol sees it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Observation<'a> {
    pub(crate) class_key: &'a str,
    pub(crate) recorded_at_ms: u64,
    /// `None` when the item was out of distribution (the head abstained).
    pub(crate) probabilities: Option<&'a [f64]>,
    pub(crate) acceptable: &'a [usize],
    /// The prediction set met the acceptability set (calibrated heads only).
    pub(crate) covered: bool,
    /// The act rule acted.
    pub(crate) answered: bool,
    pub(crate) macs: u64,
    pub(crate) stable: bool,
}

/// Running totals over the evaluated items.
#[derive(Debug, Clone, Default)]
pub(crate) struct Protocol {
    n: u64,
    n_read: u64,
    soft_mass: f64,
    absolute_error: f64,
    answered: u64,
    answered_hits: u64,
    macs: u64,
    stable: u64,
    per_class: BTreeMap<String, (u64, u64)>,
    timed: Vec<(u64, bool)>,
}

/// `hits / n` against `1 - alpha`, exactly.
fn meets_coverage(hits: u64, n: u64, alpha: UnitRationalWire) -> bool {
    let (num, den) = (
        u128::from(alpha.numerator()),
        u128::from(alpha.denominator()),
    );
    u128::from(hits) * den >= u128::from(n) * (den - num)
}

/// Answered accuracy must reach overall accuracy, and exceed it whenever the
/// head abstained on something and was not already perfect.
fn answered_ok(answered: u64, answered_hits: u64, top1: u64, n: u64) -> bool {
    let abstained = n - answered;
    if answered == 0 || abstained == 0 {
        return true;
    }
    let (lhs, rhs) = (
        u128::from(answered_hits) * u128::from(n),
        u128::from(top1) * u128::from(answered),
    );
    if top1 < n {
        lhs > rhs
    } else {
        lhs >= rhs
    }
}

/// Early-minus-late coverage shortfall at most alpha.
fn drift_ok(early: (u64, u64), late: (u64, u64), alpha: UnitRationalWire) -> bool {
    let ((ec, en), (lc, ln)) = (early, late);
    let shortfall = i128::from(ec) * i128::from(ln) - i128::from(lc) * i128::from(en);
    shortfall * i128::from(alpha.denominator())
        <= i128::from(alpha.numerator()) * i128::from(en) * i128::from(ln)
}

fn covered_count(slice: &[(u64, bool)]) -> (u64, u64) {
    (
        slice.iter().filter(|(_, c)| *c).count() as u64,
        slice.len() as u64,
    )
}

fn rate(pair: (u64, u64)) -> RefusalResult<UnitRationalWire> {
    exact_wire(pair.0, pair.1.max(1))
}

impl Protocol {
    /// Record one item.
    pub(crate) fn record(&mut self, item: &Observation) {
        self.n += 1;
        self.macs += item.macs;
        self.stable += u64::from(item.stable);
        let class = self
            .per_class
            .entry(item.class_key.to_string())
            .or_default();
        class.0 += 1;
        class.1 += u64::from(item.covered);
        self.timed.push((item.recorded_at_ms, item.covered));
        let Some(p) = item.probabilities else {
            return;
        };
        let top = top_index(p).unwrap_or(0);
        let hit = item.acceptable.contains(&top);
        self.n_read += 1;
        self.soft_mass += item.acceptable.iter().map(|&i| p[i]).sum::<f64>();
        self.absolute_error += (p[top] - f64::from(u8::from(hit))).abs();
        self.answered += u64::from(item.answered);
        self.answered_hits += u64::from(item.answered && hit);
    }

    /// Early and late halves by recorded time; `None` when every item
    /// carries one time.
    fn halves(&self) -> Option<((u64, u64), (u64, u64))> {
        let mut timed = self.timed.clone();
        timed.sort_by_key(|(at, _)| *at);
        let (first, last) = (timed.first()?.0, timed.last()?.0);
        if first == last {
            return None;
        }
        let (early, late) = timed.split_at(timed.len() / 2);
        Some((covered_count(early), covered_count(late)))
    }

    fn gates(
        &self,
        top1: u64,
        calibrated: bool,
        statistical: &StatisticalPolicy,
    ) -> Vec<&'static str> {
        let alpha = statistical.alpha;
        let classes_ok = self
            .per_class
            .values()
            .all(|&(n, covered)| n < statistical.n_min || meets_coverage(covered, n, alpha));
        let drift = self.halves().is_none_or(|(e, l)| drift_ok(e, l, alpha));
        [
            (
                "accuracy_on_answered",
                answered_ok(self.answered, self.answered_hits, top1, self.n),
            ),
            ("decision_stability", self.stable == self.n),
            (
                "cost_per_decision",
                self.macs <= MAX_MACS_PER_DECISION.saturating_mul(self.n),
            ),
            ("calibration_drift", !calibrated || drift),
            ("class_coverage", !calibrated || classes_ok),
        ]
        .into_iter()
        .filter(|(_, ok)| !ok)
        .map(|(gate, _)| gate)
        .collect()
    }

    /// The metrics and every failed gate's name.
    pub(crate) fn finish(
        &self,
        top1: u64,
        calibrated: bool,
        statistical: &StatisticalPolicy,
    ) -> RefusalResult<(PromotionMetrics, Vec<String>)> {
        let read = self.n_read.max(1) as f64;
        let halves = self.halves().filter(|_| calibrated);
        let per_class = self
            .per_class
            .iter()
            .take(64)
            .map(|(class_key, &(n_items, covered))| ClassCoverage {
                class_key: class_key.clone(),
                n_items,
                covered,
            })
            .collect();
        let metrics = PromotionMetrics {
            soft_accuracy: q32(self.soft_mass / read)?,
            score_mae: q32(self.absolute_error / read)?,
            answered: self.answered,
            answered_hits: self.answered_hits,
            abstained: self.n - self.answered,
            macs_total: self.macs,
            stable_items: self.stable,
            early_coverage: halves.map(|(e, _)| rate(e)).transpose()?,
            late_coverage: halves.map(|(_, l)| rate(l)).transpose()?,
            per_class: BoundedVec::new(per_class)
                .map_err(|detail| Refusal::new(StatisticalErrorCode::DatasetInvalid, detail))?,
        };
        let failed = self.gates(top1, calibrated, statistical);
        Ok((metrics, failed.into_iter().map(str::to_string).collect()))
    }
}
