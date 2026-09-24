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
