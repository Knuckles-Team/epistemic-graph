//! The exact split of an additive value: `φ_i = v({i}) - v(∅)`.
//!
//! For an additive game this IS the Shapley value (and every other efficient,
//! symmetric, null-player value), at `n + 2` evaluations instead of `2^n`. It is
//! refused — never applied "proportionally" — when the singleton contributions do not
//! add up to `v(N) - v(∅)`: that game is not additive, and a linear split of it would
//! invent an attribution the value does not support.

use super::{invalid, Attribution, AttributionCode, AttributionError, AttributionResult, Game};
use crate::detkernel::reduce::serial_sum;

/// Relative tolerance of the additivity check (a few ulps of accumulated rounding).
const ADDITIVITY_TOLERANCE: f64 = 1e-9;

/// The linear split of an additive `game`.
pub fn linear_split<G: Game + ?Sized>(game: &G) -> AttributionResult<Attribution> {
    let players = game.players();
    if players == 0 {
        return Err(invalid("a game needs at least one player"));
    }
    let empty = game.value(&[])?;
    let phi = (0..players)
        .map(|p| Ok(game.value(&[p])? - empty))
        .collect::<AttributionResult<Vec<f64>>>()?;
    let all: Vec<usize> = (0..players).collect();
    let grand = game.value(&all)?;
    let split = serial_sum(&phi);
    let scale = (grand - empty).abs().max(split.abs()).max(1.0);
    if (split - (grand - empty)).abs() > ADDITIVITY_TOLERANCE * scale {
        return Err(AttributionError::new(
            AttributionCode::NonAdditive,
            "the singleton contributions do not sum to v(N) - v(∅); use Shapley",
        ));
    }
    Ok(Attribution {
        phi,
        half_width: None,
        grand,
        empty,
        evaluations: players as u64 + 2,
    })
}

/// The additive fit of a logged game: the weights `w` minimising
/// `Σ_{observed S} (v(S) - Σ_{i∈S} w_i)²` (no intercept: `v(∅) = 0`). This is the
/// per-component utility under a DECLARED additive-reward assumption — evidence of class
/// claim, never an observation. Refused (`ATTRIBUTION_SINGULAR_DESIGN`) when the observed
/// coalitions do not identify every player's weight.
pub fn additive_fit(game: &super::game::LoggedGame) -> AttributionResult<Attribution> {
    use ndarray::{Array1, Array2};
    let players = game.players();
    let observed = game.observed();
    let masks: Vec<u64> = observed.keys().copied().collect();
    let design = Array2::from_shape_fn((masks.len(), players), |(row, p)| {
        f64::from(u8::from(masks[row] & (1 << p) != 0))
    });
    let target = Array1::from(observed.values().copied().collect::<Vec<f64>>());
    let gram = design.t().dot(&design);
    let inverse = super::regression::checked_inverse(
        &gram,
        "the observed coalitions do not identify every player",
    )?;
    let phi = inverse.dot(&design.t().dot(&target)).to_vec();
    let all: Vec<usize> = (0..players).collect();
    Ok(Attribution {
        grand: game.value(&all).unwrap_or_else(|_| serial_sum(&phi)),
        phi,
        half_width: None,
        empty: 0.0,
        evaluations: observed.len() as u64,
    })
}
