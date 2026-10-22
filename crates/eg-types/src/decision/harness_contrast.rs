//! The contrastive harness composition decision (EG-DECISION-ENGINE-R803).
//!
//! The engine composes the adopted harness of one task from the probed
//! candidates. A caller presents two to eight candidate component versions,
//! the identical probe conditions and the measured contrasts: each candidate
//! was probed against the same baseline run under the same task, seed, model
//! and budget. The engine adopts a candidate only when its measured delta
//! shows improvement — a positive `eval_score_delta` no other candidate beats,
//! with cost as the tie-break. It abstains and keeps the current harness when
//! no candidate improves.
//!
//! The evidence is verified, not trusted: a candidate version the graph does
//! not hold refuses, and a probe trace that the store cannot match to its
//! stored run — or whose stored conditions digest differs — refuses. A
//! candidate the caller never probed is not evidence at all: the decision
//! abstains with `insufficient_evidence` rather than adopting on the
//! unmeasured.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::contract::closed_error_codes;
use crate::contract::BoundedVec;
use crate::decision::statistical::{
    FeatureMatrixRef, QuestionKind, StatisticalDecisionRecord, StatisticalOutcome,
};
use crate::decision::QuantScaleTag;
use crate::orchestration::improvement::{AdoptedHarness, HarnessContrast};

/// The fewest candidate component versions one contrast decides over.
pub const MIN_CONTRAST_CANDIDATES: usize = 2;
/// The most candidate component versions one contrast decides over.
pub const MAX_CONTRAST_CANDIDATES: usize = 8;
/// The four measured deltas, in the sorted order the decision record's
/// feature matrix names them (row-major over the candidates, Q32).
pub const CONTRAST_FEATURE_NAMES: &[&str] = &[
    "cost_delta",
    "eval_score_delta",
    "retry_delta",
    "tool_selection_delta",
];

closed_error_codes! {
    /// Every refusal one contrast decision can report (R803.4).
    pub enum HarnessContrastErrorCode {
        /// A candidate set under two.
        MinCandidates => "HARNESS_CONTRAST_MIN_CANDIDATES",
        /// A probe trace whose conditions digest differs from the request.
        TraceMismatch => "HARNESS_CONTRAST_TRACE_MISMATCH",
        /// A candidate version reference the graph does not hold.
        VersionUnknown => "HARNESS_COMPONENT_VERSION_UNKNOWN",
        /// A candidate count over eight.
        TooManyCandidates => "HARNESS_CONTRAST_TOO_MANY_CANDIDATES",
    }
}

/// The closed abstain reasons of one contrast decision (R803.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HarnessContrastAbstainReason {
    /// Every delta was zero or negative; the current harness is kept.
    NoImprovement,
    /// The named evidence cannot be matched to the stored runs.
    TraceMismatch,
    /// A candidate was never probed, so its delta is not evidence.
    InsufficientEvidence,
}

/// One contrast decision, recorded (R803.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessContrastDecision {
    /// The task the contrast probed.
    pub task_ref: String,
    /// The task class the adoption applies to.
    pub task_class: String,
    /// The candidate component-version node ids, in declared order.
    pub candidate_version_refs: BoundedVec<String, MAX_CONTRAST_CANDIDATES>,
    /// The digest of the identical probe conditions (task, seed, model,
    /// budget).
    pub conditions_digest: String,
    /// The probe-run trace id per candidate, aligned with the candidates;
    /// `None` where the caller never probed.
    pub probe_trace_ids: BoundedVec<Option<String>, MAX_CONTRAST_CANDIDATES>,
    /// The measured contrast per candidate, aligned with the candidates.
    pub deltas: BoundedVec<HarnessContrast, MAX_CONTRAST_CANDIDATES>,
    /// The candidate adopted; set exactly when `abstain_reason` is absent.
    pub adopted_version_ref: Option<String>,
    /// Why the decision abstained; set exactly when `adopted_version_ref` is
    /// absent.
    pub abstain_reason: Option<HarnessContrastAbstainReason>,
}

impl HarnessContrastDecision {
    /// The decision invariant (R803.1): adoption and abstention are
    /// exclusive and complete, the aligned fields agree in length, and the
    /// recorded outcome of the decision is the one its stored inputs
    /// actually produce.
    pub fn check(&self) -> Result<(), String> {
        let n = self.candidate_version_refs.len();
        if (MIN_CONTRAST_CANDIDATES..=MAX_CONTRAST_CANDIDATES).contains(&n)
            && self.probe_trace_ids.len() == n
            && self.deltas.len() == n
        {
        } else {
            return Err(
                "the decision's candidate, trace and delta rows must align within 2 to 8"
                    .to_string(),
            );
        }
        match (&self.adopted_version_ref, &self.abstain_reason) {
            (Some(_), Some(_)) => return Err("the decision both adopts and abstains".to_string()),
            (None, None) => return Err("the decision neither adopts nor abstains".to_string()),
            (Some(ref_), None) if !self.candidate_version_refs.as_slice().contains(ref_) => {
                return Err("the decision adopts a candidate it did not present".to_string())
            }
            _ => {}
        }
        if self.abstain_reason == Some(HarnessContrastAbstainReason::NoImprovement)
            && self.deltas.iter().any(|delta| delta.eval_score_delta > 0)
        {
            return Err(
                "the recorded abstention is no_improvement but a positive delta is stored"
                    .to_string(),
            );
        }
        if self.abstain_reason == Some(HarnessContrastAbstainReason::InsufficientEvidence)
            && self.probe_trace_ids.iter().all(Option::is_some)
        {
            return Err(
                "the recorded abstention is insufficient_evidence but every candidate was probed"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// The typed refusal of one contrast decision; each variant carries its
/// served code verbatim (R803.4 names all four).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessContrastRefusal {
    /// A candidate set under two.
    MinCandidates,
    /// A candidate count over eight.
    TooManyCandidates,
    /// A candidate version reference the graph does not hold.
    VersionUnknown,
    /// The named evidence does not agree with the stored runs.
    TraceMismatch,
}

impl HarnessContrastRefusal {
    /// The served error code, verbatim.
    pub fn code(self) -> &'static str {
        match self {
            Self::MinCandidates => HarnessContrastErrorCode::MinCandidates,
            Self::TooManyCandidates => HarnessContrastErrorCode::TooManyCandidates,
            Self::VersionUnknown => HarnessContrastErrorCode::VersionUnknown,
            Self::TraceMismatch => HarnessContrastErrorCode::TraceMismatch,
        }
        .as_str()
    }
}

/// One candidate presented to one contrast decision.
#[derive(Clone, Copy)]
pub struct ContrastCandidate<'a> {
    /// The component-version node id the candidate is.
    pub version_ref: &'a str,
    /// The probe run that measured the candidate side (run B), by trace id;
    /// `None` when the caller never probed it.
    pub probe_trace_id: Option<&'a str>,
    /// The measured contrast of the candidate run against the baseline.
    pub deltas: &'a HarnessContrast,
}

/// The evidence a contrast decision reads out of the graph (R803.2).
pub struct ContrastEvidence<'a> {
    /// The component versions the graph holds, by node id.
    pub held_versions: &'a BTreeSet<String>,
    /// The stored probe runs, by trace id.
    pub probe_runs: &'a BTreeMap<String, crate::orchestration::improvement::HarnessProbeRun>,
    /// The stored contrasts, by node id.
    pub stored_contrasts: &'a BTreeMap<String, HarnessContrast>,
}

/// Decide one contrast (R803.2).
///
/// The refusals are checked before any abstention: the candidate count
/// bounds, the existence of every candidate version, and the agreement of
/// every named piece of evidence with the store. A candidate the caller
/// never probed makes the decision abstain with `insufficient_evidence`;
/// otherwise the decision adopts the best-improving candidate (positive
/// `eval_score_delta`, cost as the tie-break, declared order the last
/// resort) or abstains with `no_improvement`.
pub fn decide_contrast(
    task_ref: &str,
    task_class: &str,
    conditions_digest: &str,
    baseline_trace_id: &str,
    candidates: &[ContrastCandidate<'_>],
    evidence: &ContrastEvidence<'_>,
) -> Result<HarnessContrastDecision, HarnessContrastRefusal> {
    if candidates.len() < MIN_CONTRAST_CANDIDATES {
        return Err(HarnessContrastRefusal::MinCandidates);
    }
    if candidates.len() > MAX_CONTRAST_CANDIDATES {
        return Err(HarnessContrastRefusal::TooManyCandidates);
    }
    for candidate in candidates {
        if !evidence.held_versions.contains(candidate.version_ref) {
            return Err(HarnessContrastRefusal::VersionUnknown);
        }
        verify_candidate(
            task_ref,
            conditions_digest,
            baseline_trace_id,
            candidate,
            evidence,
        )?;
    }
    let (adopted, reason) = if candidates
        .iter()
        .any(|candidate| candidate.probe_trace_id.is_none())
    {
        (
            None,
            Some(HarnessContrastAbstainReason::InsufficientEvidence),
        )
    } else {
        match adoption_index(
            candidates
                .iter()
                .map(|c| c.deltas.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        ) {
            Some(index) => (Some(candidates[index].version_ref.to_string()), None),
            None => (None, Some(HarnessContrastAbstainReason::NoImprovement)),
        }
    };
    Ok(HarnessContrastDecision {
        task_ref: task_ref.to_string(),
        task_class: task_class.to_string(),
        candidate_version_refs: BoundedVec::new(
            candidates
                .iter()
                .map(|c| c.version_ref.to_string())
                .collect(),
        )
        .expect("the count is checked against the bound"),
        conditions_digest: conditions_digest.to_string(),
        probe_trace_ids: BoundedVec::new(
            candidates
                .iter()
                .map(|c| c.probe_trace_id.map(str::to_string))
                .collect(),
        )
        .expect("the count is checked against the bound"),
        deltas: BoundedVec::new(candidates.iter().map(|c| c.deltas.clone()).collect())
            .expect("the count is checked against the bound"),
        adopted_version_ref: adopted,
        abstain_reason: reason,
    })
}

/// The agreement of one candidate's evidence with the store (R803.4).
fn verify_candidate(
    task_ref: &str,
    conditions_digest: &str,
    baseline_trace_id: &str,
    candidate: &ContrastCandidate<'_>,
    evidence: &ContrastEvidence<'_>,
) -> Result<(), HarnessContrastRefusal> {
    let mismatch = Err(HarnessContrastRefusal::TraceMismatch);
    let deltas = candidate.deltas;
    if deltas.task_ref != task_ref
        || deltas.conditions_digest != conditions_digest
        || deltas.run_a_trace_id != baseline_trace_id
    {
        return mismatch;
    }
    if let Some(trace) = candidate.probe_trace_id {
        if trace != deltas.run_b_trace_id {
            return mismatch;
        }
        match evidence.probe_runs.get(trace) {
            None => return mismatch,
            Some(run) if run.conditions_digest != conditions_digest => return mismatch,
            Some(_) => {}
        }
    }
    // The baseline run the delta names, when the store holds it, must have
    // run under the same conditions; a contrast measured across conditions
    // is not evidence at all.
    if let Some(run) = evidence.probe_runs.get(&deltas.run_a_trace_id) {
        if run.conditions_digest != conditions_digest {
            return mismatch;
        }
    }
    // A stored contrast at the delta's content-addressed identity must agree
    // with the declared delta in full.
    if let Some(stored) = evidence.stored_contrasts.get(&deltas.node_id()) {
        if stored != deltas {
            return mismatch;
        }
    }
    Ok(())
}

/// The index of the candidate to adopt (R803.2): the highest positive
/// `eval_score_delta`, the lower `cost_delta` as the tie-break, the earlier
/// declaration the last resort. `None` when no candidate improves.
pub fn adoption_index(deltas: &[HarnessContrast]) -> Option<usize> {
    let mut best: Option<(i64, i64, usize)> = None;
    for (index, delta) in deltas.iter().enumerate() {
        if delta.eval_score_delta <= 0 {
            continue;
        }
        let better = match best {
            None => true,
            Some((eval, cost, _)) => {
                delta.eval_score_delta > eval
                    || (delta.eval_score_delta == eval && delta.cost_delta < cost)
            }
        };
        if better {
            best = Some((delta.eval_score_delta, delta.cost_delta, index));
        }
    }
    best.map(|(_, _, index)| index)
}

/// The supersession citation of one adoption (R803.5, R802.4): the node id
/// of the current adoption a new adoption of the same agent and task class
/// replaces — `None` when the named agent and task class hold no current
/// adoption. The adoption record carries the citation; the decision
/// record's fields stay closed.
pub fn supersession_citation(
    head: Option<&AdoptedHarness>,
    agent_ref: &str,
    task_class: &str,
) -> Option<String> {
    head.filter(|head| head.agent_ref == agent_ref && head.task_class == task_class)
        .map(AdoptedHarness::node_id)
}

/// The row-major matrix values of one decision's stored deltas: candidate
/// order outer, [`CONTRAST_FEATURE_NAMES`] inner.
pub fn matrix_values(decision: &HarnessContrastDecision) -> Vec<i64> {
    decision
        .deltas
        .iter()
        .flat_map(|delta| {
            [
                delta.cost_delta,
                delta.eval_score_delta,
                delta.retry_delta,
                delta.tool_selection_delta,
            ]
        })
        .collect()
}

/// Re-derive one stored contrast record from its stored inputs (R803.5).
///
/// The record's digest is checked by the caller; this verifies the
/// deterministic core: the record's matrix must carry exactly the recorded
/// decision's candidates and deltas, and re-running the adoption rule over
/// the stored deltas must give the recorded outcome — the recorded
/// `no_improvement` is provable because no positive eval may exist, and a
/// recorded adoption must be the one the stored deltas produce. The graph
/// evidence (held versions, probe runs, stored contrasts) was read at
/// decide time and is not replayable; the replay verifies the core and the
/// digest, the same trust model as the generic replay over a stored matrix.
pub fn replay_record(record: &StatisticalDecisionRecord) -> Result<(), String> {
    if record.question.kind != QuestionKind::HarnessContrast {
        return Err("the record's question kind is not harness_contrast".to_string());
    }
    let Some(decision) = &record.inputs.harness_contrast else {
        return Err("the record carries no harness contrast decision".to_string());
    };
    decision
        .check()
        .map_err(|detail| format!("the stored decision is malformed: {detail}"))?;
    if !matches!(
        record.inputs.feature_matrix,
        FeatureMatrixRef::Inline { .. }
    ) {
        return Err("the record's feature matrix is not inline".to_string());
    }
    let FeatureMatrixRef::Inline {
        candidate_ids,
        feature_names,
        scale,
        values,
    } = &record.inputs.feature_matrix
    else {
        unreachable!("checked above")
    };
    let names: Vec<&str> = CONTRAST_FEATURE_NAMES.to_vec();
    if candidate_ids.as_slice() != decision.candidate_version_refs.as_slice() {
        return Err("the record's matrix names other candidates than the decision".to_string());
    }
    if feature_names.as_slice() != names {
        return Err(
            "the record's matrix names other features than the four contrast deltas".to_string(),
        );
    }
    if *scale != QuantScaleTag::Q32 {
        return Err("the record's matrix is not on the Q32 scale".to_string());
    }
    if values.as_slice() != matrix_values(decision) {
        return Err("the record's stored values disagree with the decision's deltas".to_string());
    }
    match (&decision.adopted_version_ref, &decision.abstain_reason) {
        (Some(ref_), None) => {
            let index = decision
                .candidate_version_refs
                .iter()
                .position(|id| id == ref_)
                .expect("check() verified the adopted candidate was presented");
            if adoption_index(decision.deltas.as_slice()) != Some(index) {
                return Err("the stored deltas do not adopt the recorded candidate".to_string());
            }
        }
        (None, Some(HarnessContrastAbstainReason::NoImprovement))
            if adoption_index(decision.deltas.as_slice()).is_some() =>
        {
            return Err(
                "the recorded abstention is no_improvement but the stored deltas improve"
                    .to_string(),
            );
        }
        _ => {}
    }
    match (&record.outcome, &decision.adopted_version_ref) {
        (StatisticalOutcome::Explored { option_id, .. }, Some(ref_)) if option_id == ref_ => {}
        (StatisticalOutcome::Abstained { .. }, None) => {}
        _ => return Err("the record's outcome does not match its decision state".to_string()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::BoundedVec;
    use crate::decision::digest::{digest_text, statistical_record_digest};
    use crate::decision::statistical::{
        ShortlistProvenance, StatisticalInputs, StatisticalQuestion,
    };
    use crate::decision::CandidateSourceRecord;
    use crate::decision::{
        DecisionPolicyRef, EvidenceClass, PremiseRef, ResolutionKind, StatisticalOutcome,
        TraceFidelity, UnitRationalWire, STATISTICAL_DECISION_RECORD_SCHEMA_VERSION,
    };
    use crate::orchestration::improvement::HarnessProbeRun;

    /// The input-digest domain the server's `seal` uses for statistical
    /// records.
    const INPUTS_DOMAIN: &str = "eg/decision-statistical-inputs/v1";

    fn contrast(
        run_a: &str,
        run_b: &str,
        retry: i64,
        selection: i64,
        eval: i64,
        cost: i64,
    ) -> HarnessContrast {
        HarnessContrast {
            task_ref: "task:deploy".into(),
            conditions_digest: "sha256:conditions".into(),
            run_a_trace_id: run_a.into(),
            run_b_trace_id: run_b.into(),
            retry_delta: retry,
            tool_selection_delta: selection,
            eval_score_delta: eval,
            cost_delta: cost,
        }
    }

    fn evidence_set(
        held: &[&str],
        runs: &[(&str, &str)],
        stored: &[HarnessContrast],
    ) -> (
        BTreeSet<String>,
        BTreeMap<String, HarnessProbeRun>,
        BTreeMap<String, HarnessContrast>,
    ) {
        let held_versions = held.iter().map(|s| s.to_string()).collect();
        let probe_runs = runs
            .iter()
            .map(|(trace, conditions)| {
                (
                    trace.to_string(),
                    HarnessProbeRun {
                        trace_id: trace.to_string(),
                        conditions_digest: conditions.to_string(),
                    },
                )
            })
            .collect();
        let stored_contrasts = stored.iter().map(|c| (c.node_id(), c.clone())).collect();
        (held_versions, probe_runs, stored_contrasts)
    }

    fn decide(
        candidates: &[ContrastCandidate<'_>],
        evidence: &ContrastEvidence<'_>,
    ) -> Result<HarnessContrastDecision, HarnessContrastRefusal> {
        decide_contrast(
            "task:deploy",
            "deploy",
            "sha256:conditions",
            "trace:baseline",
            candidates,
            evidence,
        )
    }

    // spec: EG-DECISION-ENGINE-R803.1
    #[test]
    fn decision_round_trips() {
        let (held, runs, stored) = evidence_set(
            &["harness-component:prompt:a", "harness-component:prompt:b"],
            &[
                ("trace:baseline", "sha256:conditions"),
                ("trace:a", "sha256:conditions"),
                ("trace:b", "sha256:conditions"),
            ],
            &[],
        );
        let evidence = ContrastEvidence {
            held_versions: &held,
            probe_runs: &runs,
            stored_contrasts: &stored,
        };
        let candidates = [
            ContrastCandidate {
                version_ref: "harness-component:prompt:a",
                probe_trace_id: Some("trace:a"),
                deltas: &contrast(
                    "trace:baseline",
                    "trace:a",
                    -1 << 32,
                    1 << 30,
                    2 << 32,
                    -1 << 30,
                ),
            },
            ContrastCandidate {
                version_ref: "harness-component:prompt:b",
                probe_trace_id: None,
                deltas: &contrast("trace:baseline", "trace:missing", 0, 0, 0, 0),
            },
        ];
        let decision = decide(&candidates, &evidence).unwrap();
        assert_eq!(
            decision.abstain_reason,
            Some(HarnessContrastAbstainReason::InsufficientEvidence)
        );
        let bytes = rmp_serde::to_vec_named(&decision).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<HarnessContrastDecision>(&bytes).unwrap(),
            decision
        );
        // The abstain reason is closed: an open string is rejected.
        assert!(
            serde_json::from_value::<HarnessContrastAbstainReason>(serde_json::json!(
                "we_feel_fine"
            ))
            .is_err()
        );
        assert_eq!(
            serde_json::to_value(HarnessContrastAbstainReason::NoImprovement).unwrap(),
            "no_improvement"
        );
        // The decision invariant is checked on the round trip.
        assert!(decision.check().is_ok());
        let mut broken = decision.clone();
        broken.adopted_version_ref = Some("harness-component:prompt:z".into());
        assert!(broken.check().is_err());
    }

    // spec: EG-DECISION-ENGINE-R803.2
    #[test]
    fn adoption_requires_positive_delta() {
        let (held, runs, stored) = evidence_set(
            &[
                "harness-component:prompt:a",
                "harness-component:prompt:b",
                "harness-component:prompt:c",
            ],
            &[
                ("trace:baseline", "sha256:conditions"),
                ("trace:a", "sha256:conditions"),
                ("trace:b", "sha256:conditions"),
                ("trace:c", "sha256:conditions"),
            ],
            &[],
        );
        let evidence = ContrastEvidence {
            held_versions: &held,
            probe_runs: &runs,
            stored_contrasts: &stored,
        };
        let improving = contrast("trace:baseline", "trace:a", 0, 0, 2 << 32, 1 << 30);
        let neutral = contrast("trace:baseline", "trace:b", 0, 0, 0, -1 << 30);
        let worse = contrast("trace:baseline", "trace:c", 0, 0, -(1 << 32), -(1 << 30));
        let candidates = [
            ContrastCandidate {
                version_ref: "harness-component:prompt:a",
                probe_trace_id: Some("trace:a"),
                deltas: &improving,
            },
            ContrastCandidate {
                version_ref: "harness-component:prompt:b",
                probe_trace_id: Some("trace:b"),
                deltas: &neutral,
            },
            ContrastCandidate {
                version_ref: "harness-component:prompt:c",
                probe_trace_id: Some("trace:c"),
                deltas: &worse,
            },
        ];
        let decision = decide(&candidates, &evidence).unwrap();
        assert_eq!(
            decision.adopted_version_ref.as_deref(),
            Some("harness-component:prompt:a")
        );
        assert_eq!(decision.abstain_reason, None);
        // Every delta zero or negative abstains with no_improvement.
        let neutral_a = contrast("trace:baseline", "trace:a", 0, 0, 0, -1 << 30);
        let worse_b = contrast("trace:baseline", "trace:b", 0, 0, -(1 << 32), -(1 << 30));
        let nothing = [
            ContrastCandidate {
                version_ref: "harness-component:prompt:a",
                probe_trace_id: Some("trace:a"),
                deltas: &neutral_a,
            },
            ContrastCandidate {
                version_ref: "harness-component:prompt:b",
                probe_trace_id: Some("trace:b"),
                deltas: &worse_b,
            },
        ];
        let abstained = decide(&nothing, &evidence).unwrap();
        assert_eq!(
            abstained.abstain_reason,
            Some(HarnessContrastAbstainReason::NoImprovement)
        );
        assert_eq!(abstained.adopted_version_ref, None);
        // The tie-break: equal positive eval, lower cost wins.
        let tie_high = contrast("trace:baseline", "trace:b", 0, 0, 2 << 32, 9 << 30);
        let tie_low = contrast("trace:baseline", "trace:a", 0, 0, 2 << 32, 1 << 30);
        let tied = [
            ContrastCandidate {
                version_ref: "harness-component:prompt:b",
                probe_trace_id: Some("trace:b"),
                deltas: &tie_high,
            },
            ContrastCandidate {
                version_ref: "harness-component:prompt:a",
                probe_trace_id: Some("trace:a"),
                deltas: &tie_low,
            },
        ];
        let on_tie = decide(&tied, &evidence).unwrap();
        assert_eq!(
            on_tie.adopted_version_ref.as_deref(),
            Some("harness-component:prompt:a")
        );
        // The exposed core agrees with the decision.
        assert_eq!(
            adoption_index(&[improving.clone(), neutral.clone()]),
            Some(0)
        );
        assert_eq!(adoption_index(&[neutral.clone(), worse.clone()]), None);
    }

    // spec: EG-DECISION-ENGINE-R803.4
    #[test]
    fn contrast_refusals() {
        let improving = contrast("trace:baseline", "trace:a", 0, 0, 1 << 32, 0);
        // Under two candidates.
        let one = [ContrastCandidate {
            version_ref: "harness-component:prompt:a",
            probe_trace_id: Some("trace:a"),
            deltas: &improving,
        }];
        let (held, runs, stored) = evidence_set(
            &["harness-component:prompt:a"],
            &[("trace:a", "sha256:conditions")],
            &[],
        );
        let evidence = ContrastEvidence {
            held_versions: &held,
            probe_runs: &runs,
            stored_contrasts: &stored,
        };
        assert_eq!(
            decide(&one, &evidence).unwrap_err(),
            HarnessContrastRefusal::MinCandidates
        );
        assert_eq!(
            HarnessContrastRefusal::MinCandidates.code(),
            "HARNESS_CONTRAST_MIN_CANDIDATES"
        );
        // A candidate the graph does not hold.
        let two = [
            one[0],
            ContrastCandidate {
                version_ref: "harness-component:prompt:ghost",
                probe_trace_id: Some("trace:a"),
                deltas: &improving,
            },
        ];
        assert_eq!(
            decide(&two, &evidence).unwrap_err(),
            HarnessContrastRefusal::VersionUnknown
        );
        assert_eq!(
            HarnessContrastRefusal::VersionUnknown.code(),
            "HARNESS_COMPONENT_VERSION_UNKNOWN"
        );
        // A probe trace the store holds under other conditions.
        let (held2, runs2, stored2) = evidence_set(
            &["harness-component:prompt:a", "harness-component:prompt:b"],
            &[("trace:a", "sha256:other-conditions")],
            &[],
        );
        let evidence2 = ContrastEvidence {
            held_versions: &held2,
            probe_runs: &runs2,
            stored_contrasts: &stored2,
        };
        assert_eq!(
            decide(&two, &evidence2).unwrap_err(),
            HarnessContrastRefusal::TraceMismatch
        );
        assert_eq!(
            HarnessContrastRefusal::TraceMismatch.code(),
            "HARNESS_CONTRAST_TRACE_MISMATCH"
        );
        // A named trace the store does not hold at all.
        let (held3, runs3, stored3) = evidence_set(
            &["harness-component:prompt:a", "harness-component:prompt:b"],
            &[],
            &[],
        );
        let evidence3 = ContrastEvidence {
            held_versions: &held3,
            probe_runs: &runs3,
            stored_contrasts: &stored3,
        };
        assert_eq!(
            decide(&two, &evidence3).unwrap_err(),
            HarnessContrastRefusal::TraceMismatch
        );
        // A stored contrast that disagrees with the declared delta.
        let different = contrast("trace:baseline", "trace:a", 0, 0, -(1 << 32), 0);
        let (held4, runs4, stored4) = evidence_set(
            &["harness-component:prompt:a", "harness-component:prompt:b"],
            &[("trace:a", "sha256:conditions")],
            &[different],
        );
        let evidence4 = ContrastEvidence {
            held_versions: &held4,
            probe_runs: &runs4,
            stored_contrasts: &stored4,
        };
        assert_eq!(
            decide(&two, &evidence4).unwrap_err(),
            HarnessContrastRefusal::TraceMismatch
        );
        // Over eight candidates.
        let many = [ContrastCandidate {
            version_ref: "harness-component:prompt:a",
            probe_trace_id: Some("trace:a"),
            deltas: &improving,
        }; 9];
        assert_eq!(
            decide(&many, &evidence).unwrap_err(),
            HarnessContrastRefusal::TooManyCandidates
        );
        assert_eq!(
            HarnessContrastRefusal::TooManyCandidates.code(),
            "HARNESS_CONTRAST_TOO_MANY_CANDIDATES"
        );
    }

    /// One stored value drifted by `drift` in `slot`: the record rebuilt with
    /// the tampered matrix, the pins untouched.
    fn drifted(
        record: &StatisticalDecisionRecord,
        slot: usize,
        drift: i64,
    ) -> StatisticalDecisionRecord {
        let mut tampered = record.clone();
        let FeatureMatrixRef::Inline {
            candidate_ids,
            feature_names,
            scale,
            values,
        } = &tampered.inputs.feature_matrix
        else {
            panic!("the record stores an inline matrix")
        };
        let mut values = values.as_slice().to_vec();
        values[slot] += drift;
        tampered.inputs.feature_matrix = FeatureMatrixRef::Inline {
            candidate_ids: BoundedVec::new(candidate_ids.as_slice().to_vec()).unwrap(),
            feature_names: BoundedVec::new(feature_names.as_slice().to_vec()).unwrap(),
            scale: *scale,
            values: BoundedVec::new(values).unwrap(),
        };
        tampered
    }

    fn record_for(decision: &HarnessContrastDecision) -> StatisticalDecisionRecord {
        let values = matrix_values(decision);
        let question = StatisticalQuestion {
            question_id: "q-contrast".into(),
            kind: QuestionKind::HarnessContrast,
            safety: crate::decision::statistical::QuestionSafety::Ordinary,
        };
        let inputs = StatisticalInputs {
            harness_evaluation: None,
            feature_schema: crate::agent_component::ComponentDependency {
                component_id: "schema:contrast".into(),
                kind: crate::agent_component::AgentComponentKind::FeatureSchema,
                definition_digest: "sha256:schema".into(),
            },
            head: None,
            policy: DecisionPolicyRef::Default,
            policy_digest: "sha256:policy".into(),
            params: BoundedVec::new(Vec::new()).unwrap(),
            classification_rules: None,
            feature_matrix: FeatureMatrixRef::Inline {
                candidate_ids: BoundedVec::new(
                    decision.candidate_version_refs.iter().cloned().collect(),
                )
                .unwrap(),
                feature_names: BoundedVec::new(
                    CONTRAST_FEATURE_NAMES
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                )
                .unwrap(),
                scale: QuantScaleTag::Q32,
                values: BoundedVec::new(values).unwrap(),
            },
            shortlist: ShortlistProvenance {
                ann_recall_mode: None,
                now_ms: 1_700_000_000_000,
                embedder_model_digest: None,
            },
            exploration: None,
            belief_slices: BoundedVec::new(Vec::new()).unwrap(),
            harness_contrast: Some(decision.clone()),
        };
        let inputs_digest = digest_text(INPUTS_DOMAIN, &(question.clone(), &inputs));
        let outcome = match &decision.adopted_version_ref {
            Some(ref_) => StatisticalOutcome::Explored {
                option_id: ref_.clone(),
                propensity: UnitRationalWire::new(1, 1).unwrap(),
            },
            None => StatisticalOutcome::Abstained {
                reasons: BoundedVec::new(vec![
                    crate::decision::AbstainReason::InsufficientConfidence,
                ])
                .unwrap(),
            },
        };
        let mut record = StatisticalDecisionRecord {
            schema_version: STATISTICAL_DECISION_RECORD_SCHEMA_VERSION,
            record_id: format!("decision:{}", inputs_digest.trim_start_matches("sha256:")),
            tenant_id: "tenant:one".into(),
            caller_principal: "principal:one".into(),
            created_at_ms: 1_700_000_000_123,
            question,
            candidate_source: CandidateSourceRecord::Declared {
                options_digest: "sha256:options".into(),
            },
            inputs,
            inputs_digest,
            resolution_kind: ResolutionKind::Statistical,
            evidence_class: EvidenceClass::Claim,
            trace_fidelity: TraceFidelity::FullStep,
            premises: BoundedVec::new(Vec::<PremiseRef>::new()).unwrap(),
            outcome,
            calibration: None,
            explanation: None,
            audit: None,
            logging_propensities: BoundedVec::new(Vec::new()).unwrap(),
            nl_binding: None,
            belief: BoundedVec::new(Vec::new()).unwrap(),
            synthetic_evidence: false,
            record_digest: String::new(),
        };
        record.record_digest = statistical_record_digest(&record);
        record
    }

    // spec: EG-DECISION-ENGINE-R803.5
    #[test]
    fn record_pins_evidence_and_replays() {
        let improving = contrast("trace:baseline", "trace:a", -1 << 32, 0, 2 << 32, -1 << 30);
        let neutral = contrast("trace:baseline", "trace:b", 0, 0, 0, 0);
        let (held, runs, stored) = evidence_set(
            &["harness-component:prompt:a", "harness-component:prompt:b"],
            &[
                ("trace:baseline", "sha256:conditions"),
                ("trace:a", "sha256:conditions"),
                ("trace:b", "sha256:conditions"),
            ],
            &[],
        );
        let evidence = ContrastEvidence {
            held_versions: &held,
            probe_runs: &runs,
            stored_contrasts: &stored,
        };
        let candidates = [
            ContrastCandidate {
                version_ref: "harness-component:prompt:a",
                probe_trace_id: Some("trace:a"),
                deltas: &improving,
            },
            ContrastCandidate {
                version_ref: "harness-component:prompt:b",
                probe_trace_id: Some("trace:b"),
                deltas: &neutral,
            },
        ];
        let decision = decide(&candidates, &evidence).unwrap();
        assert_eq!(
            decision.adopted_version_ref.as_deref(),
            Some("harness-component:prompt:a")
        );
        // A rerun with the same inputs produces the same record digest: the
        // record pins the request digest, the candidate version digests (in
        // the node ids), the probe trace ids and the per-candidate deltas.
        let first = record_for(&decision);
        let second = record_for(&decision);
        assert_eq!(first.record_id, second.record_id);
        assert_eq!(first.record_digest, second.record_digest);
        assert!(replay_record(&first).is_ok());
        // A stored value that drifts from the pinned delta fails the replay.
        assert!(replay_record(&drifted(&first, 0, 1)).is_err());
        // The no_improvement abstention replays only while no positive eval
        // exists.
        let neutral_a = contrast("trace:baseline", "trace:a", 0, 0, 0, 0);
        let nothing = decide(
            &[
                ContrastCandidate {
                    version_ref: "harness-component:prompt:a",
                    probe_trace_id: Some("trace:a"),
                    deltas: &neutral_a,
                },
                ContrastCandidate {
                    version_ref: "harness-component:prompt:b",
                    probe_trace_id: Some("trace:b"),
                    deltas: &neutral,
                },
            ],
            &evidence,
        )
        .unwrap();
        assert_eq!(
            nothing.abstain_reason,
            Some(HarnessContrastAbstainReason::NoImprovement)
        );
        let abstained = record_for(&nothing);
        assert!(replay_record(&abstained).is_ok());
        // A positive eval smuggled into the stored matrix breaks the
        // recorded no_improvement abstention.
        assert!(replay_record(&drifted(
            &abstained,
            CONTRAST_FEATURE_NAMES.len() + 1,
            1 << 32
        ))
        .is_err());
        // The supersession citation resolves to the current adoption of the
        // same agent and task class, and only to it.
        let head = AdoptedHarness {
            agent_ref: "agent:one".into(),
            task_class: "deploy".into(),
            component_version_refs: BoundedVec::new(vec![
                "harness-component:prompt:old".to_string()
            ])
            .unwrap(),
            decision_ref: "decision:old".into(),
            contrast_ref: None,
        };
        assert_eq!(
            supersession_citation(Some(&head), "agent:one", "deploy"),
            Some(head.node_id())
        );
        assert_eq!(
            supersession_citation(Some(&head), "agent:two", "deploy"),
            None
        );
        assert_eq!(
            supersession_citation(Some(&head), "agent:one", "build"),
            None
        );
        assert_eq!(supersession_citation(None, "agent:one", "deploy"), None);
    }
}
