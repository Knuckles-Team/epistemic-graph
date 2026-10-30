//! Verifier-free pairwise ranking of answer candidates
//! (EG-DECISION-ENGINE-R134.104 to R134.108).
//!
//! Each pairwise comparison is a caller claim. The engine derives one global
//! order from the complete round robin, retains the top `k` and proposes one
//! bounded revision for every other candidate. No verifier takes part.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

use super::digest::digest_text;
use super::{PremiseClass, PremiseProvenance, PremiseRef};

pub const ANSWER_RANKING_SCHEMA: &str = "answer-candidate-ranking/v1";
pub const MAX_ANSWER_CANDIDATES: usize = 16;
/// Every unordered pair of the largest candidate set.
pub const MAX_PAIRWISE_COMPARISONS: usize = MAX_ANSWER_CANDIDATES * (MAX_ANSWER_CANDIDATES - 1) / 2;
/// The engine never proposes more revisions than this in one ranking.
pub const MAX_REVISION_ACTIONS: usize = 8;

/// Which side of one comparison the caller preferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PairwisePreference {
    First,
    Second,
    Tie,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PairwiseComparison {
    pub first: String,
    pub second: String,
    pub preference: PairwisePreference,
}

/// How equal scores are ordered. The only rule is the lowest candidate id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AnswerTieRule {
    LowestCandidateId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnswerRankingRequest {
    pub candidate_ids: BoundedVec<String, MAX_ANSWER_CANDIDATES>,
    pub comparisons: BoundedVec<PairwiseComparison, MAX_PAIRWISE_COMPARISONS>,
    /// A named verifier is refused: this ranking is verifier-free by contract.
    #[serde(default)]
    pub verifier: Option<String>,
    pub retain_top_k: u16,
    pub max_revisions: u16,
}

/// One ranked candidate. `score` counts two per pairwise win and one per tie.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RankedAnswer {
    pub candidate_id: String,
    pub score: u32,
}

/// A bounded revision proposal for one non-retained candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RevisionAction {
    pub candidate_id: String,
    pub revise_toward: String,
    pub lost_to: BoundedVec<String, MAX_ANSWER_CANDIDATES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnswerCandidateRankingV1 {
    pub schema: String,
    pub candidate_ids: BoundedVec<String, MAX_ANSWER_CANDIDATES>,
    pub comparisons: BoundedVec<PairwiseComparison, MAX_PAIRWISE_COMPARISONS>,
    pub verifier_present: bool,
    pub global_order: BoundedVec<RankedAnswer, MAX_ANSWER_CANDIDATES>,
    pub tie_rule: AnswerTieRule,
    pub retained_ids: BoundedVec<String, MAX_ANSWER_CANDIDATES>,
    pub revision_actions: BoundedVec<RevisionAction, MAX_REVISION_ACTIONS>,
    pub ranking_digest: String,
}

/// Why a ranking request was refused. No ranking is produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AnswerRankingRefusal {
    VerifierNamed { verifier: String },
    TooFewCandidates { count: usize },
    RevisionBoundExceeded { needed: usize, bound: usize },
    InvalidRetain { retain_top_k: u16 },
    DuplicateCandidate { candidate_id: String },
    UnknownCandidate { candidate_id: String },
    DuplicateComparison { first: String, second: String },
    IncompleteComparisons { missing: usize },
}

fn check_shape(request: &AnswerRankingRequest) -> Result<BTreeSet<&str>, AnswerRankingRefusal> {
    if let Some(verifier) = &request.verifier {
        return Err(AnswerRankingRefusal::VerifierNamed {
            verifier: verifier.clone(),
        });
    }
    let count = request.candidate_ids.len();
    if count < 2 {
        return Err(AnswerRankingRefusal::TooFewCandidates { count });
    }
    let k = usize::from(request.retain_top_k);
    if k == 0 || k > count {
        return Err(AnswerRankingRefusal::InvalidRetain {
            retain_top_k: request.retain_top_k,
        });
    }
    let bound = usize::from(request.max_revisions).min(MAX_REVISION_ACTIONS);
    if count - k > bound {
        return Err(AnswerRankingRefusal::RevisionBoundExceeded {
            needed: count - k,
            bound,
        });
    }
    let mut ids = BTreeSet::new();
    for id in request.candidate_ids.iter() {
        if !ids.insert(id.as_str()) {
            return Err(AnswerRankingRefusal::DuplicateCandidate {
                candidate_id: id.clone(),
            });
        }
    }
    Ok(ids)
}

type Outcomes<'a> = BTreeMap<(&'a str, &'a str), PairwisePreference>;

fn check_comparisons<'a>(
    request: &'a AnswerRankingRequest,
    ids: &BTreeSet<&str>,
) -> Result<Outcomes<'a>, AnswerRankingRefusal> {
    let mut outcomes = BTreeMap::new();
    for c in request.comparisons.iter() {
        for id in [&c.first, &c.second] {
            if !ids.contains(id.as_str()) {
                return Err(AnswerRankingRefusal::UnknownCandidate {
                    candidate_id: id.clone(),
                });
            }
        }
        let key = if c.first < c.second {
            (c.first.as_str(), c.second.as_str())
        } else {
            (c.second.as_str(), c.first.as_str())
        };
        if c.first == c.second || outcomes.insert(key, c.preference).is_some() {
            return Err(AnswerRankingRefusal::DuplicateComparison {
                first: c.first.clone(),
                second: c.second.clone(),
            });
        }
    }
    let pairs = ids.len() * (ids.len() - 1) / 2;
    if outcomes.len() != pairs {
        return Err(AnswerRankingRefusal::IncompleteComparisons {
            missing: pairs - outcomes.len(),
        });
    }
    Ok(outcomes)
}

/// The winner and loser of one comparison, or `None` for a tie.
fn decided(c: &PairwiseComparison) -> Option<(&str, &str)> {
    match c.preference {
        PairwisePreference::First => Some((&c.first, &c.second)),
        PairwisePreference::Second => Some((&c.second, &c.first)),
        PairwisePreference::Tie => None,
    }
}

fn order(request: &AnswerRankingRequest) -> Vec<RankedAnswer> {
    let mut scores: BTreeMap<&str, u32> = request
        .candidate_ids
        .iter()
        .map(|id| (id.as_str(), 0))
        .collect();
    for c in request.comparisons.iter() {
        match decided(c) {
            Some((winner, _)) => *scores.get_mut(winner).expect("checked id") += 2,
            None => {
                *scores.get_mut(c.first.as_str()).expect("checked id") += 1;
                *scores.get_mut(c.second.as_str()).expect("checked id") += 1;
            }
        }
    }
    let mut ranked: Vec<RankedAnswer> = scores
        .into_iter()
        .map(|(id, score)| RankedAnswer {
            candidate_id: id.to_string(),
            score,
        })
        .collect();
    ranked.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.candidate_id.cmp(&b.candidate_id))
    });
    ranked
}

fn revisions(
    request: &AnswerRankingRequest,
    ranked: &[RankedAnswer],
    k: usize,
) -> Vec<RevisionAction> {
    let top = &ranked[0].candidate_id;
    ranked[k..]
        .iter()
        .map(|answer| {
            let lost_to: BTreeSet<String> = request
                .comparisons
                .iter()
                .filter_map(decided)
                .filter(|(_, loser)| *loser == answer.candidate_id)
                .map(|(winner, _)| winner.to_string())
                .collect();
            RevisionAction {
                candidate_id: answer.candidate_id.clone(),
                revise_toward: top.clone(),
                lost_to: BoundedVec::new(lost_to.into_iter().collect()).expect("candidate bound"),
            }
        })
        .collect()
}

/// Rank answer candidates from a complete set of pairwise caller claims.
pub fn rank_answer_candidates(
    request: &AnswerRankingRequest,
) -> Result<AnswerCandidateRankingV1, AnswerRankingRefusal> {
    let ids = check_shape(request)?;
    check_comparisons(request, &ids)?;
    let ranked = order(request);
    let k = usize::from(request.retain_top_k);
    let mut comparisons = request.comparisons.as_slice().to_vec();
    comparisons.sort_by(|a, b| (&a.first, &a.second).cmp(&(&b.first, &b.second)));
    let mut ranking = AnswerCandidateRankingV1 {
        schema: ANSWER_RANKING_SCHEMA.to_string(),
        candidate_ids: BoundedVec::new(ids.iter().map(|id| id.to_string()).collect())
            .expect("candidate bound"),
        comparisons: BoundedVec::new(comparisons).expect("comparison bound"),
        verifier_present: false,
        retained_ids: BoundedVec::new(ranked[..k].iter().map(|r| r.candidate_id.clone()).collect())
            .expect("candidate bound"),
        revision_actions: BoundedVec::new(revisions(request, &ranked, k)).expect("revision bound"),
        global_order: BoundedVec::new(ranked).expect("candidate bound"),
        tie_rule: AnswerTieRule::LowestCandidateId,
        ranking_digest: String::new(),
    };
    ranking.ranking_digest = digest_text("eg/answer-ranking/v1", &ranking);
    Ok(ranking)
}

/// One claim premise per comparison, one definition premise for the global
/// order, and the retained set and every revision action as recorded facts.
pub fn answer_ranking_premises(ranking: &AnswerCandidateRankingV1) -> Vec<PremiseRef> {
    let request = |field: &str| PremiseProvenance::Request {
        field: field.to_string(),
    };
    let mut out: Vec<PremiseRef> = ranking
        .comparisons
        .iter()
        .map(|c| PremiseRef {
            subject: format!("{}|{}", c.first, c.second),
            fact: format!(
                "answer_pairwise:preference={}",
                serde_json::to_value(c.preference)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default()
            ),
            class: PremiseClass::Claim,
            provenance: request("params.answer_ranking.comparisons"),
            trust_tier: None,
        })
        .collect();
    let joined = |items: Vec<&str>| items.join(",");
    out.push(PremiseRef {
        subject: ranking.schema.clone(),
        fact: format!(
            "answer_global_order:order={};tie_rule=lowest_candidate_id;retained={};ranking_digest={}",
            joined(ranking.global_order.iter().map(|r| r.candidate_id.as_str()).collect()),
            joined(ranking.retained_ids.iter().map(String::as_str).collect()),
            ranking.ranking_digest
        ),
        class: PremiseClass::Definition,
        provenance: request("params.answer_ranking"),
        trust_tier: None,
    });
    out.extend(ranking.revision_actions.iter().map(|action| PremiseRef {
        subject: action.candidate_id.clone(),
        fact: format!(
            "answer_revision:revise_toward={};lost_to={}",
            action.revise_toward,
            joined(action.lost_to.iter().map(String::as_str).collect())
        ),
        class: PremiseClass::Definition,
        provenance: request("params.answer_ranking"),
        trust_tier: None,
    }));
    out
}

#[cfg(test)]
mod tests;
