//! Contribution attribution (EH-523, ANALYTICS-HARVEST AH-03): how much of a value each
//! contributor earned.
//!
//! * [`linear`] — the exact split of an ADDITIVE value (`v(S) = Σ_{i∈S} v({i})`); a
//!   non-additive game is refused, never split proportionally.
//! * [`shapley`] — Shapley values: exact over a memoised coalition table for at most
//!   [`EXACT_MAX_PLAYERS`] players, or sampled over antithetic permutations with a
//!   per-player CLT interval and a recorded seed.
//! * [`owen`] — Owen values: Shapley with a declared hierarchy (players in unions).
//! * [`regression`] — multi-factor OLS attribution with Newey–West (HAC) standard errors
//!   and the TRUE residual series (whose sum is zero by construction when an intercept is
//!   fitted — so it is never reported as a component).
//!
//! A game is anything that can value a coalition ([`Game`]); [`AggregateGame`] is the
//! general one the query surfaces use: `v(S) = agg{x_i : i ∈ S}`, `v(∅) = 0`.
//!
//! Every routine is deterministic: coalitions are enumerated in mask order, sums are
//! serial, and the sampled estimator draws its permutations from a seeded ChaCha20
//! stream, so an attribution replays bit-identically from its inputs and seed.

pub mod game;
pub mod linear;
pub mod owen;
pub mod regression;
pub mod shapley;

#[cfg(test)]
mod tests;

pub use game::{Aggregate, AggregateGame, Game};
pub use linear::linear_split;
pub use owen::owen_values;
pub use regression::{factor_ols, FactorAttribution, HacLags};
pub use shapley::{shapley_exact, shapley_sampled, SampleSpec};

use std::fmt;

/// The largest game exact Shapley enumerates (`2^16` memoised coalition values).
pub const EXACT_MAX_PLAYERS: usize = 16;

/// Why an attribution was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributionCode {
    /// More players than the exact method (or a sampled budget) admits.
    TooManyPlayers,
    /// The coalition evaluations the request needs exceed its work budget.
    BudgetExceeded,
    /// A linear split was asked of a game that is not additive.
    NonAdditive,
    /// A coalition the game has no observed value for (a logged game).
    UnsupportedCoalition,
    /// Malformed input: empty, non-finite, mismatched lengths, a bad parameter.
    InvalidInput,
    /// The regression design is rank-deficient.
    Singular,
}

impl AttributionCode {
    /// The wire code.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TooManyPlayers => "ATTRIBUTION_TOO_MANY_PLAYERS",
            Self::BudgetExceeded => "ATTRIBUTION_BUDGET_EXCEEDED",
            Self::NonAdditive => "ATTRIBUTION_NON_ADDITIVE",
            Self::UnsupportedCoalition => "UNSUPPORTED_COALITION",
            Self::InvalidInput => "ATTRIBUTION_INVALID_INPUT",
            Self::Singular => "ATTRIBUTION_SINGULAR_DESIGN",
        }
    }
}

/// A typed attribution refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributionError {
    pub code: AttributionCode,
    pub detail: String,
}

impl AttributionError {
    /// A refusal with `code`.
    pub fn new(code: AttributionCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for AttributionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.detail)
    }
}

impl std::error::Error for AttributionError {}

/// Result alias for attribution.
pub type AttributionResult<T> = std::result::Result<T, AttributionError>;

pub(crate) fn invalid(detail: impl Into<String>) -> AttributionError {
    AttributionError::new(AttributionCode::InvalidInput, detail)
}

/// One player-level attribution. `phi` sums to `grand - empty` (efficiency) for every
/// method: exactly for the exact and linear methods, and per permutation (hence exactly
/// in the mean) for the sampled one.
#[derive(Debug, Clone, PartialEq)]
pub struct Attribution {
    /// Per-player contribution, in player order.
    pub phi: Vec<f64>,
    /// Per-player CI half-width (sampled Shapley only), at the requested level.
    pub half_width: Option<Vec<f64>>,
    /// `v(N)`.
    pub grand: f64,
    /// `v(∅)`.
    pub empty: f64,
    /// Coalition evaluations spent.
    pub evaluations: u64,
}

impl Attribution {
    /// `v(N) - v(∅)`: what the contributions split.
    pub fn total(&self) -> f64 {
        self.grand - self.empty
    }
}
