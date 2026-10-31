//! Shapley values: exact over a memoised coalition table, or sampled over antithetic
//! permutations with a per-player CLT interval.

use super::EXACT_MAX_PLAYERS;
use super::{invalid, Attribution, AttributionCode, AttributionError, AttributionResult, Game};
use crate::detkernel::kernels::normal_quantile;
use crate::random::Generator;

/// The ascending member list of coalition `mask`.
pub(super) fn members_of(mask: usize, players: usize) -> Vec<usize> {
    (0..players).filter(|p| mask & (1 << p) != 0).collect()
}

/// `C(n, k)` exactly (`n <= 63` here, so it fits `u64`).
fn binomial(n: usize, k: usize) -> u64 {
    let k = k.min(n - k);
    let mut out: u64 = 1;
    for i in 0..k {
        out = out * (n - i) as u64 / (i as u64 + 1);
    }
    out
}

/// The Shapley weight of a coalition of size `s` not containing the player:
/// `s! (n - s - 1)! / n! = 1 / (n · C(n - 1, s))`.
pub(super) fn shapley_weights(players: usize) -> Vec<f64> {
    (0..players)
        .map(|s| 1.0 / (players as f64 * binomial(players - 1, s) as f64))
        .collect()
}

pub(super) fn budget_refusal(needed: u64, budget: u64) -> AttributionError {
    AttributionError::new(
        AttributionCode::BudgetExceeded,
        format!("{needed} coalition evaluations exceed the budget of {budget}"),
    )
}

/// Every coalition's value, indexed by member mask.
fn coalition_table<G: Game + ?Sized>(game: &G, players: usize) -> AttributionResult<Vec<f64>> {
    (0..1usize << players)
        .map(|mask| game.value(&members_of(mask, players)))
        .collect()
}

fn player_phi(table: &[f64], weights: &[f64], player: usize) -> f64 {
    let bit = 1usize << player;
    let mut acc = 0.0;
    for (mask, &without) in table.iter().enumerate() {
        if mask & bit == 0 {
            let size = mask.count_ones() as usize;
            acc += weights[size] * (table[mask | bit] - without);
        }
    }
    acc
}

/// Exact Shapley values of a game with at most [`EXACT_MAX_PLAYERS`] players: every
/// coalition is valued ONCE into a `2^n` table, and each player's value is the
/// weighted sum of its marginal contributions over that table. Refuses more players, or
/// a table larger than `max_evaluations`.
pub fn shapley_exact<G: Game + ?Sized>(
    game: &G,
    max_evaluations: u64,
) -> AttributionResult<Attribution> {
    let players = game.players();
    if players == 0 {
        return Err(invalid("a game needs at least one player"));
    }
    if players > EXACT_MAX_PLAYERS {
        return Err(AttributionError::new(
            AttributionCode::TooManyPlayers,
            format!(
                "exact Shapley enumerates at most {EXACT_MAX_PLAYERS} players, got {players}; \
                 sample instead"
            ),
        ));
    }
    let size = 1u64 << players;
    if size > max_evaluations {
        return Err(budget_refusal(size, max_evaluations));
    }
    let table = coalition_table(game, players)?;
    let weights = shapley_weights(players);
    let phi = (0..players)
        .map(|p| player_phi(&table, &weights, p))
        .collect();
    Ok(Attribution {
        phi,
        half_width: None,
        grand: table[table.len() - 1],
        empty: table[0],
        evaluations: size,
    })
}

/// How a sampled Shapley estimate is drawn.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleSpec {
    /// Antithetic permutation PAIRS (a permutation and its reverse), at least 2.
    pub pairs: u32,
    /// The ChaCha20 seed the permutations are drawn from (recorded with the result).
    pub seed: u64,
    /// Two-sided confidence of the per-player interval, strictly inside `(0, 1)`.
    pub confidence: f64,
}

/// Welford running mean and sum of squared deviations, one per player.
struct Moments {
    mean: Vec<f64>,
    m2: Vec<f64>,
    count: f64,
}

impl Moments {
    fn new(players: usize) -> Self {
        Self {
            mean: vec![0.0; players],
            m2: vec![0.0; players],
            count: 0.0,
        }
    }

    fn push(&mut self, sample: &[f64]) {
        self.count += 1.0;
        for (p, &x) in sample.iter().enumerate() {
            let delta = x - self.mean[p];
            self.mean[p] += delta / self.count;
            self.m2[p] += delta * (x - self.mean[p]);
        }
    }

    /// `z · sd / √count` per player.
    fn half_widths(&self, z: f64) -> Vec<f64> {
        self.m2
            .iter()
            .map(|m2| z * (m2 / (self.count - 1.0) / self.count).sqrt())
            .collect()
    }
}

/// Marginal contribution of each player along `order`.
fn marginals<G: Game + ?Sized>(
    game: &G,
    order: &[usize],
    empty: f64,
) -> AttributionResult<Vec<f64>> {
    let prefix = game.prefix_values(order)?;
    let mut out = vec![0.0; order.len()];
    let mut previous = empty;
    for (&player, &value) in order.iter().zip(&prefix) {
        out[player] = value - previous;
        previous = value;
    }
    Ok(out)
}

/// One antithetic pair: the mean of each player's marginal along a permutation and
/// along its reverse (negatively correlated, so the pair mean has lower variance).
fn antithetic_pair<G: Game + ?Sized>(
    game: &G,
    rng: &mut Generator,
    empty: f64,
) -> AttributionResult<Vec<f64>> {
    let order = rng
        .try_permutation_indices(game.players())
        .map_err(|e| invalid(e.to_string()))?;
    let forward = marginals(game, &order, empty)?;
    let reversed: Vec<usize> = order.iter().rev().copied().collect();
    let backward = marginals(game, &reversed, empty)?;
    Ok(forward
        .iter()
        .zip(&backward)
        .map(|(f, b)| 0.5 * (f + b))
        .collect())
}

fn check_spec(spec: &SampleSpec) -> AttributionResult<f64> {
    if spec.pairs < 2 {
        return Err(invalid(
            "sampled Shapley needs at least 2 permutation pairs",
        ));
    }
    if !(spec.confidence > 0.0 && spec.confidence < 1.0) {
        return Err(invalid("the confidence level must be inside (0, 1)"));
    }
    normal_quantile(0.5 + spec.confidence / 2.0).map_err(|e| invalid(e.to_string()))
}

/// Sampled Shapley values: `spec.pairs` antithetic permutation pairs drawn from
/// `spec.seed`. Every permutation's marginals sum to `v(N) - v(∅)`, so the estimate is
/// exactly efficient; each player gets a two-sided CLT half-width at `spec.confidence`.
/// Refuses when `2 · pairs · n` evaluations exceed `max_evaluations`.
pub fn shapley_sampled<G: Game + ?Sized>(
    game: &G,
    spec: SampleSpec,
    max_evaluations: u64,
) -> AttributionResult<Attribution> {
    let players = game.players();
    if players == 0 {
        return Err(invalid("a game needs at least one player"));
    }
    let z = check_spec(&spec)?;
    let needed = 2 * u64::from(spec.pairs) * players as u64;
    if needed > max_evaluations {
        return Err(budget_refusal(needed, max_evaluations));
    }
    let empty = game.value(&[])?;
    let mut rng = Generator::new(spec.seed);
    let mut moments = Moments::new(players);
    for _ in 0..spec.pairs {
        moments.push(&antithetic_pair(game, &mut rng, empty)?);
    }
    let all: Vec<usize> = (0..players).collect();
    Ok(Attribution {
        half_width: Some(moments.half_widths(z)),
        phi: moments.mean,
        grand: game.value(&all)?,
        empty,
        evaluations: needed,
    })
}
