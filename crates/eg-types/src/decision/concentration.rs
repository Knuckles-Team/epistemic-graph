//! Cross-round edit concentration cap (EG-DECISION-ENGINE-R173.1, .2, .4).
//!
//! A candidate harness edit is reverted when the shipped edits on its
//! dimension inside the policy window, counting the candidate, reach the
//! policy's `min_shipped`. Otherwise it is kept.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

fn positive<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = u32::deserialize(deserializer)?;
    if value == 0 {
        return Err(serde::de::Error::custom(
            "concentration bounds are positive",
        ));
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConcentrationPolicy {
    pub policy_iri: String,
    #[serde(deserialize_with = "positive")]
    pub min_shipped: u32,
    #[serde(deserialize_with = "positive")]
    pub window_rounds: u32,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EditStatus {
    Proposed,
    Shipped,
    Reverted,
}

/// One recorded harness edit. `policy_revision` names the policy revision the
/// edit was judged under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EditFact {
    pub edit_id: String,
    pub dimension_iri: String,
    pub round: u32,
    pub status: EditStatus,
    pub policy_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ConcentrationAction {
    Keep,
    Revert,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConcentrationVerdict {
    pub dimension_iri: String,
    pub count: u32,
    pub first_round: u32,
    pub last_round: u32,
    pub action: ConcentrationAction,
}

/// Why no verdict was given.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ConcentrationAbstention {
    NoConcentrationPolicy,
    MixedPolicyRevision { revisions: Vec<u64> },
}

/// Per-dimension ring buffers of shipped rounds inside one window. Each
/// shipped edit is pushed once and evicted at most once, so a new edit costs
/// O(1) amortized.
#[derive(Debug, Clone, Default)]
pub struct ConcentrationLedger {
    window_rounds: u32,
    rounds: BTreeMap<String, VecDeque<u32>>,
}

impl ConcentrationLedger {
    pub fn new(window_rounds: u32) -> Self {
        Self {
            window_rounds,
            rounds: BTreeMap::new(),
        }
    }

    fn evict(buffer: &mut VecDeque<u32>, window_rounds: u32, latest: u32) {
        let first = latest.saturating_sub(window_rounds.saturating_sub(1));
        while buffer.front().is_some_and(|round| *round < first) {
            buffer.pop_front();
        }
    }

    /// Record one shipped edit. Rounds arrive in non-decreasing order.
    pub fn ship(&mut self, dimension_iri: &str, round: u32) {
        let buffer = self.rounds.entry(dimension_iri.to_string()).or_default();
        buffer.push_back(round);
        Self::evict(buffer, self.window_rounds, round);
    }

    /// Shipped rounds inside the window that ends at `round`.
    pub fn window(&mut self, dimension_iri: &str, round: u32) -> Vec<u32> {
        let Some(buffer) = self.rounds.get_mut(dimension_iri) else {
            return Vec::new();
        };
        Self::evict(buffer, self.window_rounds, round);
        buffer.iter().copied().filter(|r| *r <= round).collect()
    }
}

fn check_revisions(
    facts: &[EditFact],
    candidate: &EditFact,
) -> Result<(), ConcentrationAbstention> {
    let mut revisions: Vec<u64> = facts
        .iter()
        .chain(std::iter::once(candidate))
        .map(|fact| fact.policy_revision)
        .collect();
    revisions.sort_unstable();
    revisions.dedup();
    if revisions.len() > 1 {
        return Err(ConcentrationAbstention::MixedPolicyRevision { revisions });
    }
    Ok(())
}

/// Keep or revert one candidate edit given the recorded edits.
pub fn concentration_verdict(
    facts: &[EditFact],
    candidate: &EditFact,
    policy: Option<&ConcentrationPolicy>,
) -> Result<ConcentrationVerdict, ConcentrationAbstention> {
    let policy = policy.ok_or(ConcentrationAbstention::NoConcentrationPolicy)?;
    check_revisions(facts, candidate)?;
    let mut shipped: Vec<&EditFact> = facts
        .iter()
        .filter(|f| f.status == EditStatus::Shipped && f.dimension_iri == candidate.dimension_iri)
        .filter(|f| f.round <= candidate.round && f.edit_id != candidate.edit_id)
        .collect();
    shipped.sort_by_key(|f| f.round);
    let mut ledger = ConcentrationLedger::new(policy.window_rounds);
    for fact in shipped {
        ledger.ship(&fact.dimension_iri, fact.round);
    }
    let mut rounds = ledger.window(&candidate.dimension_iri, candidate.round);
    rounds.push(candidate.round);
    let count = u32::try_from(rounds.len()).unwrap_or(u32::MAX);
    Ok(ConcentrationVerdict {
        dimension_iri: candidate.dimension_iri.clone(),
        count,
        first_round: rounds[0],
        last_round: candidate.round,
        action: if count >= policy.min_shipped {
            ConcentrationAction::Revert
        } else {
            ConcentrationAction::Keep
        },
    })
}

/// What one verdict counted (EG-DECISION-ENGINE-R173.5): the shipped edit
/// ids inside the window (the candidate last), the window bounds, the
/// threshold and the policy that supplied it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConcentrationEvidence {
    pub counted_edit_ids: Vec<String>,
    pub window_start: u32,
    pub window_end: u32,
    pub min_shipped: u32,
    pub policy_iri: String,
    pub policy_revision: u64,
}

/// [`concentration_verdict`] plus the evidence it counted. The counted ids
/// are exactly the edits the verdict's `count` covers.
pub fn concentration_judgement(
    facts: &[EditFact],
    candidate: &EditFact,
    policy: Option<&ConcentrationPolicy>,
) -> Result<(ConcentrationVerdict, ConcentrationEvidence), ConcentrationAbstention> {
    let verdict = concentration_verdict(facts, candidate, policy)?;
    let policy = policy.ok_or(ConcentrationAbstention::NoConcentrationPolicy)?;
    let window_start = candidate
        .round
        .saturating_sub(policy.window_rounds.saturating_sub(1));
    let mut counted: Vec<&EditFact> = facts
        .iter()
        .filter(|f| f.status == EditStatus::Shipped && f.dimension_iri == candidate.dimension_iri)
        .filter(|f| f.edit_id != candidate.edit_id)
        .filter(|f| (window_start..=candidate.round).contains(&f.round))
        .collect();
    counted.sort_by(|a, b| (a.round, &a.edit_id).cmp(&(b.round, &b.edit_id)));
    let mut counted_edit_ids: Vec<String> = counted.iter().map(|f| f.edit_id.clone()).collect();
    counted_edit_ids.push(candidate.edit_id.clone());
    Ok((
        verdict,
        ConcentrationEvidence {
            counted_edit_ids,
            window_start,
            window_end: candidate.round,
            min_shipped: policy.min_shipped,
            policy_iri: policy.policy_iri.clone(),
            policy_revision: policy.revision,
        },
    ))
}

#[cfg(test)]
mod tests;
