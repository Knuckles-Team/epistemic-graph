//! Belief over temporal slices of one decision state (EH-297).
//!
//! SalesRLAgent's durable idea -- a probability per conversation prefix -- is
//! `BELIEF AS OF t` over the engine's own clock: the same visible, legal
//! option set, its features recomputed with the feature clock at each `t`
//! (every time-dependent feature reads that clock), and the pinned head read
//! over each slice. A slice never adds an option; it re-reads the ones the
//! decision already has. The slices' matrices are stored so the belief
//! verify-replays like the decision itself.

use serde::{Deserialize, Serialize};

use super::super::numeric::QuantisedValue;
use crate::contract::BoundedVec;

/// Most belief slices one `Decide` may ask for.
pub const MAX_BELIEF_SLICES: usize = 8;

/// The stored input of one slice: its clock and its feature matrix, in the
/// decision matrix's candidate and feature order. Empty values mean a fact a
/// feature needs was unknown at that time (the slice has no belief).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BeliefSliceInputs {
    pub as_of_ms: u64,
    #[serde(default)]
    pub values: BoundedVec<i64, 2048>,
}

/// The head's belief at one slice: a distribution over the candidates on
/// `Q32`, or `None` when the slice had no complete matrix, was out of the
/// head's fitted range, or the head is not listwise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct BeliefPoint {
    pub as_of_ms: u64,
    #[serde(default)]
    pub probabilities: Option<BoundedVec<QuantisedValue, 64>>,
}
