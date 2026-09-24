//! Exact Shapley attribution of the seeds for the noisy-OR value game.
//!
//! The value of a coalition `S` of seeds at a target is the noisy-OR
//! probability of the target when only `S` is seeded. Every coalition is
//! evaluated once (`2^s` propagations), so the seed count is bounded; the
//! attributions sum to the target's probability (efficiency, `v(empty) = 0`).

use super::{noisy_or, ImpactGraph, Seed};

/// The largest seed set attributed exactly (`2^12` propagations).
pub const MAX_EXACT_SEEDS: usize = 12;

/// `attribution[t][s]`: seed `s`'s Shapley value at `targets[t]`, or `None`
/// when there are more than [`MAX_EXACT_SEEDS`] seeds.
pub fn noisy_or_attribution(
    graph: &ImpactGraph,
    seeds: &[Seed],
    targets: &[usize],
    hops: u32,
) -> Option<Vec<Vec<f64>>> {
    let s = seeds.len();
    if s > MAX_EXACT_SEEDS {
        return None;
    }
    let values: Vec<Vec<f64>> = (0..1usize << s)
        .map(|mask| coalition_values(graph, seeds, targets, hops, mask))
        .collect();
    let weights = shapley_weights(s);
    Some(
        (0..targets.len())
            .map(|t| (0..s).map(|i| phi(&values, &weights, t, i)).collect())
            .collect(),
    )
}

/// The target values when exactly the seeds in `mask` are seeded.
fn coalition_values(
    graph: &ImpactGraph,
    seeds: &[Seed],
    targets: &[usize],
    hops: u32,
    mask: usize,
) -> Vec<f64> {
    let members: Vec<Seed> = seeds
        .iter()
        .enumerate()
        .filter(|(i, _)| mask & (1 << i) != 0)
        .map(|(_, seed)| *seed)
        .collect();
    let result = noisy_or(graph, &members, hops);
    targets
        .iter()
        .map(|&t| result.probability.get(t).copied().unwrap_or(0.0))
        .collect()
}

/// `w(k) = k! (s - k - 1)! / s!` for a coalition of size `k` not holding the player.
fn shapley_weights(s: usize) -> Vec<f64> {
    let factorial = |k: usize| (1..=k).map(|x| x as f64).product::<f64>();
    (0..s.max(1))
        .map(|k| factorial(k) * factorial(s.saturating_sub(k + 1)) / factorial(s))
        .collect()
}

fn phi(values: &[Vec<f64>], weights: &[f64], target: usize, player: usize) -> f64 {
    let bit = 1usize << player;
    (0..values.len())
        .filter(|mask| mask & bit == 0)
        .map(|mask| {
            let size = mask.count_ones() as usize;
            weights[size] * (values[mask | bit][target] - values[mask][target])
        })
        .sum()
}
