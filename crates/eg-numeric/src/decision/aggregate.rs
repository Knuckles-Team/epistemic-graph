//! The outcome aggregate over the decision log (EH-061, EH-012, EH-023).
//!
//! Each committed, executed record contributes its independent evaluations
//! to the row of its executed option under its question and policy digest,
//! and -- when asked -- to that option's cross-question row. An evaluation is
//! a LABEL only when it is an observation (or better), produced by neither the
//! selected agent, its lease holder nor the principal that decided, traced at
//! or above the fidelity floor and not censored; anything else is counted as
//! refused or censored, never as a failure. Rates are pooled question -> option
//! (Beta-Binomial) and reported only at `min_support`: a row below it is
//! omitted entirely, so small counts reveal no individual run.

use std::collections::BTreeMap;

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::log::{FidelityCounts, OptionAggregate, StoredEvaluation};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::{EvidenceClass, TraceFidelityLevel};

use super::admission::{fidelity_rank, floor_rank};
use super::quant::q32;
use super::refusal::{Refusal, RefusalResult};
use crate::risk::{
    pool_hierarchy, BetaDistribution, ConcentrationBounds, GroupCounts, PoolTree, PooledNode,
};

/// One committed record joined with its evaluations.
#[derive(Debug, Clone, Copy)]
pub struct JoinedRecord<'a> {
    pub option_id: &'a str,
    pub question_id: &'a str,
    pub policy_digest: &'a str,
    /// The principal that made the decision; it may not label it.
    pub decider: &'a str,
    pub evaluations: &'a [StoredEvaluation],
}

/// How the aggregate counts.
#[derive(Debug, Clone, Copy)]
pub struct AggregateRules {
    pub min_support: u64,
    pub fidelity_floor: TraceFidelityLevel,
    pub cross_question: bool,
}

type RowKey = (String, Option<String>, Option<String>);

#[derive(Default)]
struct Tally {
    trials: u64,
    successes: u64,
    refused: u64,
    by_fidelity: FidelityCounts,
}

fn independent(stored: &StoredEvaluation, decider: &str) -> bool {
    let e = &stored.evaluation;
    stored.producer != e.selected_agent
        && stored.producer != e.lease_holder
        && stored.producer != decider
}

impl Tally {
    fn add(&mut self, stored: &StoredEvaluation, decider: &str, rules: &AggregateRules) {
        let e = &stored.evaluation;
        let rank = fidelity_rank(e.fidelity);
        match rank {
            Some(0) => self.by_fidelity.full_step += 1,
            Some(1) => self.by_fidelity.tool_calls += 1,
            Some(_) => self.by_fidelity.final_output += 1,
            None => self.by_fidelity.censored += 1,
        }
        let (Some(rank), Some(success)) = (rank, e.success) else {
            return;
        };
        let label = e.class != EvidenceClass::Claim
            && independent(stored, decider)
            && rank <= floor_rank(rules.fidelity_floor);
        if label {
            self.trials += 1;
            self.successes += u64::from(success);
        } else {
            self.refused += 1;
        }
    }
}

fn keys(record: &JoinedRecord, rules: &AggregateRules) -> Vec<RowKey> {
    let mut keys = vec![(
        record.option_id.to_string(),
        Some(record.question_id.to_string()),
        Some(record.policy_digest.to_string()),
    )];
    if rules.cross_question {
        keys.push((record.option_id.to_string(), None, None));
    }
    keys
}

fn tallies(records: &[JoinedRecord], rules: &AggregateRules) -> BTreeMap<RowKey, Tally> {
    let mut rows: BTreeMap<RowKey, Tally> = BTreeMap::new();
    for record in records {
        for key in keys(record, rules) {
            let tally = rows.entry(key).or_default();
            for stored in record.evaluations {
                tally.add(stored, record.decider, rules);
            }
        }
    }
    rows
}

/// Pool the labelled rows of one kind (per-question or cross-question):
/// `group -> option` leaves under one root.
fn pooled(rows: &BTreeMap<RowKey, Tally>, cross: bool) -> RefusalResult<Option<PooledNode>> {
    let mut tree: BTreeMap<String, BTreeMap<String, PoolTree>> = BTreeMap::new();
    for ((option, question, policy), tally) in rows {
        if question.is_none() != cross {
            continue;
        }
        let group = format!(
            "{}|{}",
            question.as_deref().unwrap_or("*"),
            policy.as_deref().unwrap_or("*")
        );
        let leaf = PoolTree::Leaf(GroupCounts::new(tally.successes, tally.trials)?);
        tree.entry(group).or_default().insert(option.clone(), leaf);
    }
    if tree.is_empty() {
        return Ok(None);
    }
    let root = PoolTree::Branch(
        tree.into_iter()
            .map(|(g, leaves)| (g, PoolTree::Branch(leaves)))
            .collect(),
    );
    let prior = BetaDistribution::new(1.0, 1.0)?;
    Ok(Some(pool_hierarchy(
        &root,
        prior,
        ConcentrationBounds::new(1.0, 1_000.0)?,
    )?))
}

fn pooled_mean(root: Option<&PooledNode>, key: &RowKey) -> Option<f64> {
    let (option, question, policy) = key;
    let group = format!(
        "{}|{}",
        question.as_deref().unwrap_or("*"),
        policy.as_deref().unwrap_or("*")
    );
    root?
        .children
        .get(&group)?
        .children
        .get(option)
        .map(|leaf| leaf.posterior.mean())
}

/// The aggregate rows, in key order, at or above `min_support`.
pub fn aggregate(
    records: &[JoinedRecord],
    rules: &AggregateRules,
) -> RefusalResult<BoundedVec<OptionAggregate, 1024>> {
    let rows = tallies(records, rules);
    let per_question = pooled(&rows, false)?;
    let cross = pooled(&rows, true)?;
    let mut out = Vec::new();
    for (key, tally) in &rows {
        if tally.trials < rules.min_support.max(1) {
            continue;
        }
        let root = if key.1.is_some() {
            per_question.as_ref()
        } else {
            cross.as_ref()
        };
        let rate = pooled_mean(root, key).map(q32).transpose()?;
        out.push(OptionAggregate {
            option_id: key.0.clone(),
            question_id: key.1.clone(),
            policy_digest: key.2.clone(),
            trials: tally.trials,
            successes: tally.successes,
            refused: tally.refused,
            by_fidelity: tally.by_fidelity,
            pooled_rate: rate,
        });
    }
    BoundedVec::new(out)
        .map_err(|detail| Refusal::new(StatisticalErrorCode::ParameterInvalid, detail))
}
