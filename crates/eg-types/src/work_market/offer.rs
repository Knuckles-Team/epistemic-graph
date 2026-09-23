//! `WorkOffer`: the versioned, derived pricing of one Gap's current WorkItem,
//! and the deterministic utility rate the first `Decide` stage ranks by.
//!
//! ```text
//! utility_rate = floor(expected_utility_micros * probability_of_closure_ppm
//!                      / max(expected_cost_microunits, cost_floor))
//! ```
//!
//! Checked integer arithmetic only. The product is taken in `u128`, where it
//! cannot overflow (`u64 * u32 < 2^96`); a quotient that does not fit a `u64`
//! is refused rather than saturated, so an offer's rate is always exact.

use serde::{Deserialize, Serialize};

use super::{bounded_list, bounded_ppm};

/// Identity of the scoring stage, recorded beside every rate it produced.
pub const WORK_OFFER_UTILITY_RATE_STAGE: &str = "eg/work-offer-utility-rate/v1";
/// The cost floor a stored offer's rate is computed at. A `Decide` policy may
/// configure a different floor; it then recomputes with
/// [`work_offer_utility_rate`], never trusts the stored figure.
pub const DEFAULT_COST_FLOOR_MICROUNITS: u64 = 1_000;
/// Most capabilities, repositories or dependencies one offer may name.
pub const MAX_OFFER_REFS: usize = 16;
/// Most evidence digests one offer may cite.
pub const MAX_OFFER_EVIDENCE: usize = 32;

/// How far a change made for this offer can reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum BlastRadius {
    /// One module or file set.
    Local,
    /// One repository.
    Repository,
    /// Several repositories or the running fleet.
    Fleet,
}

/// The pricing inputs of one offer. Every money and probability figure is a
/// fixed-point integer; the evidence it cites must already be on the Gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkOffer {
    pub expected_utility_micros: u64,
    pub probability_of_closure_ppm: u32,
    /// Token/GPU/CI/time cost, in the market's cost microunits.
    pub expected_cost_microunits: u64,
    pub cost_uncertainty_ppm: u32,
    pub blast_radius: BlastRadius,
    pub reversible: bool,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub repository_scope: Vec<String>,
    /// No attempt before this time (0 = none).
    #[serde(default)]
    pub cooldown_until_ms: u64,
    /// Canonical ids of Gaps that must be resolved first.
    #[serde(default)]
    pub depends_on_gap_ids: Vec<String>,
    /// The Gap evidence this pricing was derived from (at least one).
    pub evidence_digests: Vec<String>,
}

/// The recorded offer: its inputs, version and engine-computed rate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkOfferView {
    /// 1 for the first offer on a Gap, +1 per replacement, across
    /// generations (see `GapView::offer_version`).
    pub version: u64,
    /// The Gap generation (and so the WorkItem) this offer prices.
    pub generation: u32,
    pub work_item_id: String,
    pub offer: WorkOffer,
    /// The rate at [`DEFAULT_COST_FLOOR_MICROUNITS`].
    pub utility_rate: u64,
    pub stage: String,
    pub offered_at_ms: u64,
}

/// The deterministic utility rate of `offer` at `cost_floor_microunits`, or
/// `None` when the quotient does not fit a `u64`.
pub fn work_offer_utility_rate(offer: &WorkOffer, cost_floor_microunits: u64) -> Option<u64> {
    let numerator = u128::from(offer.expected_utility_micros)
        .checked_mul(u128::from(offer.probability_of_closure_ppm))?;
    let denominator = u128::from(
        offer
            .expected_cost_microunits
            .max(cost_floor_microunits)
            .max(1),
    );
    u64::try_from(numerator / denominator).ok()
}

impl WorkOffer {
    pub fn validate(&self) -> Result<(), String> {
        bounded_ppm(
            "probability_of_closure_ppm",
            self.probability_of_closure_ppm,
        )?;
        bounded_ppm("cost_uncertainty_ppm", self.cost_uncertainty_ppm)?;
        bounded_list(
            "required_capabilities",
            &self.required_capabilities,
            MAX_OFFER_REFS,
        )?;
        bounded_list("repository_scope", &self.repository_scope, MAX_OFFER_REFS)?;
        bounded_list(
            "depends_on_gap_ids",
            &self.depends_on_gap_ids,
            MAX_OFFER_REFS,
        )?;
        bounded_list(
            "evidence_digests",
            &self.evidence_digests,
            MAX_OFFER_EVIDENCE,
        )?;
        if self.evidence_digests.is_empty() {
            return Err("a work offer must cite at least one Gap evidence digest".to_string());
        }
        work_offer_utility_rate(self, DEFAULT_COST_FLOOR_MICROUNITS)
            .map(|_| ())
            .ok_or_else(|| "work offer utility rate does not fit the native range".to_string())
    }
}
