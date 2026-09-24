//! Retrieval-outcome learning signals (EH-394, EH-395, EH-398): pure
//! functions over already-visibility-filtered log rows.
//!
//! A retrieval outcome teaches nothing until an INDEPENDENT evaluation of its
//! record is admitted: an observation or better, produced by neither the
//! selected agent, its lease holder nor the principal that attested the
//! outcome, traced at or above the fidelity floor, not censored (EH-016). When
//! several admitted evaluations disagree, the run counts as a failure -- the
//! conservative reading, so disagreement can never manufacture a positive.

use std::collections::BTreeMap;

use eg_types::decision::statistical::log::StoredEvaluation;
use eg_types::decision::statistical::retrieval::{
    ClassUsage, HardNegative, ProvenPath, RetrievalOutcome, RetrievalPathTemplate,
};
use eg_types::decision::{EvidenceClass, TraceFidelityLevel};

use super::admission::{fidelity_rank, floor_rank};

/// What the admitted evaluations of one record say about its run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Every admitted evaluation judged the run a success.
    Success,
    /// At least one admitted evaluation judged it a failure.
    Failure,
    /// No evaluation is admissible as a label.
    Unjudged,
}

fn admitted(stored: &StoredEvaluation, attester: &str, floor: TraceFidelityLevel) -> Option<bool> {
    let e = &stored.evaluation;
    let producer = stored.producer.as_str();
    let independent =
        producer != e.selected_agent && producer != e.lease_holder && producer != attester;
    let traced = fidelity_rank(e.fidelity).is_some_and(|rank| rank <= floor_rank(floor));
    (independent && traced && e.class != EvidenceClass::Claim)
        .then_some(e.success)
        .flatten()
}

/// The verdict of one record's evaluations, for an outcome `attester`.
pub fn verdict(
    evaluations: &[StoredEvaluation],
    attester: &str,
    floor: TraceFidelityLevel,
) -> Verdict {
    let mut verdict = Verdict::Unjudged;
    for success in evaluations
        .iter()
        .filter_map(|stored| admitted(stored, attester, floor))
    {
        verdict = match (verdict, success) {
            (Verdict::Failure, _) | (_, false) => Verdict::Failure,
            (Verdict::Success | Verdict::Unjudged, true) => Verdict::Success,
        };
    }
    verdict
}

/// The hard negatives of one successful run: every returned, uncited unit
/// ranked above at least one cited unit, with how many it outranked.
pub fn hard_negatives(outcome: &RetrievalOutcome) -> Vec<HardNegative> {
    let cited: Vec<usize> = outcome
        .returned
        .iter()
        .enumerate()
        .filter(|(_, e)| outcome.cited.iter().any(|id| *id == e.evidence_id))
        .map(|(index, _)| index)
        .collect();
    outcome
        .returned
        .iter()
        .enumerate()
        .filter(|(index, _)| !cited.contains(index))
        .filter_map(|(index, e)| {
            let outranked = cited.iter().filter(|c| **c > index).count();
            (outranked > 0).then(|| HardNegative {
                record_id: outcome.record_id.clone(),
                evidence_id: e.evidence_id.clone(),
                rank: u32::try_from(index + 1).unwrap_or(u32::MAX),
                outranked_cited: u32::try_from(outranked).unwrap_or(u32::MAX),
            })
        })
        .collect()
}

/// Per content-class usage over `outcomes`; a class returned fewer than
/// `min_support` times reports zero on both counts (k-anonymity). Sorted by
/// class name.
pub fn class_usage<'a>(
    outcomes: impl Iterator<Item = &'a RetrievalOutcome>,
    min_support: u64,
) -> Vec<ClassUsage> {
    let mut counts: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    for outcome in outcomes {
        for unit in &outcome.returned {
            let Some(class) = unit.content_class.as_deref() else {
                continue;
            };
            let slot = counts.entry(class).or_default();
            slot.0 += 1;
            slot.1 += u64::from(outcome.cited.iter().any(|id| *id == unit.evidence_id));
        }
    }
    counts
        .into_iter()
        .map(|(class, (returned, cited))| {
            let supported = returned >= min_support;
            ClassUsage {
                content_class: class.to_string(),
                returned: if supported { returned } else { 0 },
                cited: if supported { cited } else { 0 },
            }
        })
        .collect()
}

/// Group judged templates into proven paths: a template appears once it has at
/// least one judged success. Most successes first, then fewest failures, then
/// digest; at most `limit`.
pub fn proven_paths<'a>(
    judged: impl Iterator<Item = (String, &'a RetrievalPathTemplate, Verdict)>,
    limit: usize,
) -> Vec<ProvenPath> {
    let mut by_digest: BTreeMap<String, ProvenPath> = BTreeMap::new();
    for (digest, template, verdict) in judged {
        let slot = by_digest
            .entry(digest.clone())
            .or_insert_with(|| ProvenPath {
                template_digest: digest,
                template: template.clone(),
                successes: 0,
                failures: 0,
            });
        slot.successes += u64::from(verdict == Verdict::Success);
        slot.failures += u64::from(verdict == Verdict::Failure);
    }
    let mut rows: Vec<ProvenPath> = by_digest
        .into_values()
        .filter(|path| path.successes > 0)
        .collect();
    rows.sort_by(|a, b| {
        b.successes
            .cmp(&a.successes)
            .then(a.failures.cmp(&b.failures))
            .then(a.template_digest.cmp(&b.template_digest))
    });
    rows.truncate(limit);
    rows
}
