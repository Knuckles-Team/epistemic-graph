//! EG-DECISION-ENGINE-R045: the smallest tool subset that still covers every
//! required capability within a caller-specified context-token budget, so a
//! multiplexer can expose fewer tools to a caller without losing coverage.
//!
//! This module is the typed request body and the selection algorithm only.
//! It has no server or candidate-source dependency, so it is testable in
//! isolation; wiring it into a `Decide` question kind is a separate change.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// One candidate tool: the capabilities it covers and the context-token cost
/// of exposing it to a caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ToolCoverageCandidate {
    pub tool_id: String,
    pub capabilities: BoundedVec<String, 64>,
    pub token_cost: u32,
}

/// A request for the smallest tool subset covering every required capability
/// within a context-token budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ToolSubsetRequestBody {
    pub required_capabilities: BoundedVec<String, 64>,
    pub context_budget_tokens: u32,
    pub candidates: BoundedVec<ToolCoverageCandidate, 256>,
}

/// Why no covering subset could be returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ToolSubsetRefusal {
    /// No combination of the offered candidates covers every required
    /// capability, regardless of budget.
    CapabilityUncoverable,
    /// Every capability is coverable, but the cheapest covering choice this
    /// greedy pass found exceeds the context budget.
    BudgetExceeded,
}

/// Greedily select the smallest tool subset that covers every required
/// capability without exceeding `context_budget_tokens`.
///
/// Standard weighted-set-cover greedy: repeatedly take the remaining
/// candidate that covers the most still-uncovered required capabilities per
/// token spent (ties broken by the lower token cost, then by `tool_id`, so
/// the result is deterministic), until every capability is covered or no
/// remaining candidate adds coverage. This is the classical ln(n)-factor
/// approximation to minimum set cover; it does not backtrack once a pick is
/// over budget, so a refusal reports the first failure reason it meets.
pub fn smallest_covering_subset(
    body: &ToolSubsetRequestBody,
) -> Result<Vec<String>, ToolSubsetRefusal> {
    let mut uncovered: std::collections::BTreeSet<&str> = body
        .required_capabilities
        .iter()
        .map(String::as_str)
        .collect();
    if uncovered.is_empty() {
        return Ok(Vec::new());
    }

    let mut remaining: Vec<&ToolCoverageCandidate> = body.candidates.iter().collect();
    let mut chosen: Vec<String> = Vec::new();
    let mut spent: u64 = 0;

    while !uncovered.is_empty() {
        let best = remaining
            .iter()
            .enumerate()
            .map(|(idx, candidate)| {
                let gain = candidate
                    .capabilities
                    .iter()
                    .filter(|cap| uncovered.contains(cap.as_str()))
                    .count();
                (idx, gain, candidate.token_cost, &candidate.tool_id)
            })
            .filter(|(_, gain, ..)| *gain > 0)
            .max_by(|a, b| {
                let density_a = a.1 as f64 / a.2.max(1) as f64;
                let density_b = b.1 as f64 / b.2.max(1) as f64;
                density_a
                    .partial_cmp(&density_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.2.cmp(&a.2)) // lower cost wins a tie
                    .then_with(|| b.3.cmp(a.3)) // deterministic tie-break
            });

        let Some((idx, _gain, cost, _tool_id)) = best else {
            return Err(ToolSubsetRefusal::CapabilityUncoverable);
        };
        if spent + u64::from(cost) > u64::from(body.context_budget_tokens) {
            return Err(ToolSubsetRefusal::BudgetExceeded);
        }
        let candidate = remaining.remove(idx);
        spent += u64::from(candidate.token_cost);
        for cap in candidate.capabilities.iter() {
            uncovered.remove(cap.as_str());
        }
        chosen.push(candidate.tool_id.clone());
    }
    Ok(chosen)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, caps: &[&str], cost: u32) -> ToolCoverageCandidate {
        ToolCoverageCandidate {
            tool_id: id.to_string(),
            capabilities: BoundedVec::new(caps.iter().map(|s| s.to_string()).collect()).unwrap(),
            token_cost: cost,
        }
    }

    fn body(
        required: &[&str],
        budget: u32,
        candidates: Vec<ToolCoverageCandidate>,
    ) -> ToolSubsetRequestBody {
        ToolSubsetRequestBody {
            required_capabilities: BoundedVec::new(
                required.iter().map(|s| s.to_string()).collect(),
            )
            .unwrap(),
            context_budget_tokens: budget,
            candidates: BoundedVec::new(candidates).unwrap(),
        }
    }

    // spec: EG-DECISION-ENGINE-R045.1
    #[test]
    fn selects_the_single_cheap_tool_that_covers_everything_over_several_narrow_ones() {
        let req = body(
            &["search", "fetch", "rank"],
            100,
            vec![
                candidate("broad", &["search", "fetch", "rank"], 30),
                candidate("search-only", &["search"], 50),
                candidate("fetch-only", &["fetch"], 50),
                candidate("rank-only", &["rank"], 50),
            ],
        );
        let chosen = smallest_covering_subset(&req).unwrap();
        assert_eq!(
            chosen,
            vec!["broad".to_string()],
            "minimal: one tool suffices"
        );
    }

    // spec: EG-DECISION-ENGINE-R045.1
    #[test]
    fn covers_every_required_capability_with_a_subset_when_no_single_tool_suffices() {
        let req = body(
            &["search", "fetch", "rank"],
            100,
            vec![
                candidate("search-fetch", &["search", "fetch"], 10),
                candidate("rank-only", &["rank"], 10),
            ],
        );
        let chosen = smallest_covering_subset(&req).unwrap();
        let covered: std::collections::BTreeSet<&str> = req
            .candidates
            .iter()
            .filter(|c| chosen.contains(&c.tool_id))
            .flat_map(|c| c.capabilities.iter().map(String::as_str))
            .collect();
        for cap in ["search", "fetch", "rank"] {
            assert!(covered.contains(cap), "{cap} must be covered");
        }
        assert_eq!(
            chosen.len(),
            2,
            "search-fetch + rank-only is the minimal pair: {chosen:?}"
        );
    }

    // spec: EG-DECISION-ENGINE-R045.1
    #[test]
    fn refuses_when_no_combination_covers_every_required_capability() {
        let req = body(
            &["search", "unobtainable"],
            1_000,
            vec![candidate("search-only", &["search"], 10)],
        );
        assert_eq!(
            smallest_covering_subset(&req).unwrap_err(),
            ToolSubsetRefusal::CapabilityUncoverable
        );
    }

    // spec: EG-DECISION-ENGINE-R045.1
    #[test]
    fn refuses_when_the_only_covering_choice_exceeds_the_context_budget() {
        let req = body(
            &["search", "fetch"],
            10,
            vec![candidate("broad", &["search", "fetch"], 50)],
        );
        assert_eq!(
            smallest_covering_subset(&req).unwrap_err(),
            ToolSubsetRefusal::BudgetExceeded
        );
    }

    #[test]
    fn empty_requirement_set_needs_no_tools() {
        let req = body(&[], 0, vec![candidate("unused", &["search"], 5)]);
        assert_eq!(
            smallest_covering_subset(&req).unwrap(),
            Vec::<String>::new()
        );
    }
}
