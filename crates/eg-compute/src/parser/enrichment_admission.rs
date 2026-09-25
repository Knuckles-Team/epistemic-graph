//! EH-557: deterministic post-parse admission for queued code enrichment.
//!
//! This is a planner, not a second executor. Rungs 0–2 run in the native
//! repository parser; rungs 3–5 are only candidates for EG's durable WorkItem
//! path. The caller must supply ENGINE-VERIFIED abstention and policy verdicts
//! for each digest. A serving adapter must bind those facts to committed rows
//! and submit the resulting candidates through the native WorkItem authority.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::ingestion_wire::{IndexFileOutcome, IndexFileStatus};

const MAX_OUTCOMES: usize = 4_096;
const MAX_CANDIDATES: usize = 1_024;

/// One content+parser version. The same bytes under two grammar capabilities
/// may have different parse outcomes, so a content digest alone is insufficient.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct UnitKey {
    pub content_digest: String,
    pub parser_capability_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrichmentStage {
    Classical,
    Embedding,
    Llm,
}

impl EnrichmentStage {
    pub fn work_kind(self) -> &'static str {
        match self {
            Self::Classical => "enrichment.classical",
            Self::Embedding => "enrichment.embed",
            Self::Llm => "enrichment.llm",
        }
    }

    fn after_abstention(rung: u8) -> Option<Self> {
        match rung {
            2 => Some(Self::Classical),
            3 => Some(Self::Embedding),
            4 => Some(Self::Llm),
            _ => None,
        }
    }
}

/// Trusted policy input for one content+parser version. `abstained_through`
/// names the LAST rung that explicitly abstained; lower rungs may never be
/// overwritten by the next stage. `compute_units` is the admitted reservation,
/// not a latency estimate supplied by the content producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnitAdmission {
    pub abstained_through: u8,
    pub allowed: bool,
    pub compute_units: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichmentCandidate {
    pub unit: UnitKey,
    pub stage: EnrichmentStage,
    pub demanded: bool,
    pub reserved_compute_units: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionPlan {
    pub candidates: Vec<EnrichmentCandidate>,
    pub reserved_compute_units: u64,
    pub deferred_for_budget: usize,
    pub deferred_for_capacity: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    TooManyOutcomes,
    InvalidBudget,
}

/// Plan a bounded queue from successful native parse outcomes.
///
/// Duplicate membership of one blob is coalesced by digest+grammar. Conflicting
/// outcomes for the same unit are withheld. A demand signal reorders only
/// eligible work; it cannot bypass abstention, policy or budget. The result is
/// deterministic across input order and makes no network, model or graph call.
pub fn plan_enrichment(
    outcomes: &[IndexFileOutcome],
    admissions: &BTreeMap<UnitKey, UnitAdmission>,
    demanded: &BTreeSet<UnitKey>,
    budget_units: u64,
) -> Result<AdmissionPlan, AdmissionError> {
    if outcomes.len() > MAX_OUTCOMES {
        return Err(AdmissionError::TooManyOutcomes);
    }
    if budget_units == 0 {
        return Err(AdmissionError::InvalidBudget);
    }
    let mut status: BTreeMap<UnitKey, Option<IndexFileStatus>> = BTreeMap::new();
    for outcome in outcomes {
        let key = UnitKey {
            content_digest: outcome.content_digest.clone(),
            parser_capability_digest: outcome.parser_capability_digest.clone(),
        };
        status
            .entry(key)
            .and_modify(|prior| {
                if *prior != Some(outcome.status) {
                    *prior = None;
                }
            })
            .or_insert(Some(outcome.status));
    }
    let mut eligible: Vec<_> = status
        .into_iter()
        .filter_map(|(unit, parsed)| (parsed == Some(IndexFileStatus::Success)).then_some(unit))
        .filter_map(|unit| {
            let admission = admissions.get(&unit)?;
            if !admission.allowed || admission.compute_units == 0 {
                return None;
            }
            let stage = EnrichmentStage::after_abstention(admission.abstained_through)?;
            Some(EnrichmentCandidate {
                demanded: demanded.contains(&unit),
                reserved_compute_units: admission.compute_units,
                unit,
                stage,
            })
        })
        .collect();
    eligible.sort_by(|a, b| b.demanded.cmp(&a.demanded).then(a.unit.cmp(&b.unit)));

    let mut plan = AdmissionPlan {
        candidates: Vec::new(),
        reserved_compute_units: 0,
        deferred_for_budget: 0,
        deferred_for_capacity: 0,
    };
    for candidate in eligible {
        if plan.candidates.len() >= MAX_CANDIDATES {
            plan.deferred_for_capacity += 1;
            continue;
        }
        if candidate.reserved_compute_units > budget_units - plan.reserved_compute_units {
            plan.deferred_for_budget += 1;
            continue;
        }
        plan.reserved_compute_units += candidate.reserved_compute_units;
        plan.candidates.push(candidate);
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::contract::BoundedVec;

    fn outcome(digest: &str, status: IndexFileStatus) -> IndexFileOutcome {
        IndexFileOutcome {
            file_path: format!("{digest}.py"),
            status,
            content_digest: digest.into(),
            parser_capability_digest: "grammar:v1".into(),
            diagnostics: BoundedVec::new(Vec::new()).unwrap(),
        }
    }

    fn key(digest: &str) -> UnitKey {
        UnitKey {
            content_digest: digest.into(),
            parser_capability_digest: "grammar:v1".into(),
        }
    }

    #[test]
    fn demand_is_prioritized_but_cannot_bypass_policy_or_budget() {
        let outcomes = vec![
            outcome("a", IndexFileStatus::Success),
            outcome("b", IndexFileStatus::Success),
            outcome("c", IndexFileStatus::Success),
        ];
        let admissions = BTreeMap::from([
            (
                key("a"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 4,
                },
            ),
            (
                key("b"),
                UnitAdmission {
                    abstained_through: 3,
                    allowed: true,
                    compute_units: 4,
                },
            ),
            (
                key("c"),
                UnitAdmission {
                    abstained_through: 4,
                    allowed: false,
                    compute_units: 1,
                },
            ),
        ]);
        let demanded = BTreeSet::from([key("b"), key("c")]);
        let plan = plan_enrichment(&outcomes, &admissions, &demanded, 4).unwrap();
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].unit, key("b"));
        assert_eq!(plan.candidates[0].stage.work_kind(), "enrichment.embed");
        assert_eq!(plan.reserved_compute_units, 4);
        assert_eq!(plan.deferred_for_budget, 1);
    }

    #[test]
    fn only_explicit_abstention_advances_the_ladder() {
        let outcomes = vec![outcome("a", IndexFileStatus::Success)];
        for (abstained, expected) in [
            (0, None),
            (1, None),
            (2, Some("enrichment.classical")),
            (3, Some("enrichment.embed")),
            (4, Some("enrichment.llm")),
            (5, None),
        ] {
            let admissions = BTreeMap::from([(
                key("a"),
                UnitAdmission {
                    abstained_through: abstained,
                    allowed: true,
                    compute_units: 1,
                },
            )]);
            let plan = plan_enrichment(&outcomes, &admissions, &BTreeSet::new(), 1).unwrap();
            assert_eq!(
                plan.candidates.first().map(|item| item.stage.work_kind()),
                expected
            );
        }
    }

    #[test]
    fn duplicate_or_conflicting_blob_outcomes_are_conservative() {
        let good = outcome("a", IndexFileStatus::Success);
        let admissions = BTreeMap::from([(
            key("a"),
            UnitAdmission {
                abstained_through: 2,
                allowed: true,
                compute_units: 1,
            },
        )]);
        let same = [good.clone(), good];
        assert_eq!(
            plan_enrichment(&same, &admissions, &BTreeSet::new(), 1)
                .unwrap()
                .candidates
                .len(),
            1
        );
        let mixed = [
            outcome("a", IndexFileStatus::Success),
            outcome("a", IndexFileStatus::Error),
        ];
        assert!(plan_enrichment(&mixed, &admissions, &BTreeSet::new(), 1)
            .unwrap()
            .candidates
            .is_empty());
    }

    #[test]
    fn ordering_is_independent_of_membership_order() {
        let admissions = BTreeMap::from([
            (
                key("a"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
            (
                key("b"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
        ]);
        let first = [
            outcome("a", IndexFileStatus::Success),
            outcome("b", IndexFileStatus::Success),
        ];
        let second = [
            outcome("b", IndexFileStatus::Success),
            outcome("a", IndexFileStatus::Success),
        ];
        assert_eq!(
            plan_enrichment(&first, &admissions, &BTreeSet::new(), 2),
            plan_enrichment(&second, &admissions, &BTreeSet::new(), 2)
        );
    }

    #[test]
    fn parser_capability_is_part_of_the_queue_identity() {
        let mut next_grammar = outcome("a", IndexFileStatus::Success);
        next_grammar.parser_capability_digest = "grammar:v2".into();
        let v2 = UnitKey {
            parser_capability_digest: "grammar:v2".into(),
            ..key("a")
        };
        let admissions = BTreeMap::from([
            (
                key("a"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
            (
                v2,
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
        ]);
        let plan = plan_enrichment(
            &[outcome("a", IndexFileStatus::Success), next_grammar],
            &admissions,
            &BTreeSet::new(),
            2,
        )
        .unwrap();
        assert_eq!(plan.candidates.len(), 2);
    }
}
