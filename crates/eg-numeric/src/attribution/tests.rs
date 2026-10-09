//! Oracles for attribution (EH-523): brute-force permutation enumeration, the four
//! Shapley axioms, the linear game's closed form, empirical CI coverage of the sampled
//! estimator, Owen's quotient-game property, and the OLS residual identity.

use super::shapley::members_of;
use super::*;
use crate::detkernel::kernels::normal_quantile;
use crate::random::Generator;

const BUDGET: u64 = 10_000_000;

/// A game given by an explicit value per coalition mask.
struct TableGame {
    players: usize,
    values: Vec<f64>,
}

impl TableGame {
    fn random(players: usize, seed: u64) -> Self {
        let mut rng = Generator::new(seed);
        let mut values = rng.uniform(-5.0, 5.0, 1 << players);
        values[0] = 0.0;
        Self { players, values }
    }

    fn mask(members: &[usize]) -> usize {
        members.iter().fold(0, |m, p| m | (1 << p))
    }
}

impl Game for TableGame {
    fn players(&self) -> usize {
        self.players
    }

    fn value(&self, members: &[usize]) -> AttributionResult<f64> {
        Ok(self.values[Self::mask(members)])
    }
}

/// Every permutation of `0..n` (Heap's algorithm, iterative).
fn permutations(n: usize) -> Vec<Vec<usize>> {
    let mut items: Vec<usize> = (0..n).collect();
    let mut counters = vec![0usize; n];
    let mut out = vec![items.clone()];
    let mut i = 0;
    while i < n {
        if counters[i] < i {
            let j = if i % 2 == 0 { 0 } else { counters[i] };
            items.swap(j, i);
            out.push(items.clone());
            counters[i] += 1;
            i = 0;
        } else {
            counters[i] = 0;
            i += 1;
        }
    }
    out
}

/// The mean marginal contribution over `orders` — Shapley's definition when the
/// orders are all permutations, Owen's when they are the union-consistent ones.
fn mean_marginals(game: &TableGame, orders: &[Vec<usize>]) -> Vec<f64> {
    let mut sums = vec![0.0; game.players];
    for order in orders {
        let mut mask = 0usize;
        for &p in order {
            sums[p] += game.values[mask | (1 << p)] - game.values[mask];
            mask |= 1 << p;
        }
    }
    sums.iter().map(|s| s / orders.len() as f64).collect()
}

fn assert_close(a: &[f64], b: &[f64], tol: f64) {
    assert_eq!(a.len(), b.len());
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        assert!((x - y).abs() <= tol, "player {i}: {x} vs {y}");
    }
}

#[test]
fn exact_matches_brute_force_permutations() {
    for n in 1..=7 {
        let game = TableGame::random(n, 100 + n as u64);
        let exact = shapley_exact(&game, BUDGET).unwrap();
        assert_close(&exact.phi, &mean_marginals(&game, &permutations(n)), 1e-9);
    }
}

#[test]
fn exact_satisfies_efficiency_symmetry_null_and_additivity() {
    let n = 6;
    let mut game = TableGame::random(n, 7);
    // Make player 5 null and players 0 and 1 symmetric.
    for mask in 0..1usize << n {
        if mask & (1 << 5) != 0 {
            game.values[mask] = game.values[mask & !(1 << 5)];
        }
    }
    for mask in 0..1usize << n {
        let swapped = (mask & !3) | ((mask & 1) << 1) | ((mask >> 1) & 1);
        game.values[swapped.max(mask)] = game.values[swapped.min(mask)];
    }
    let phi = shapley_exact(&game, BUDGET).unwrap();
    let total: f64 = phi.phi.iter().sum();
    assert!((total - phi.total()).abs() < 1e-9, "efficiency");
    assert!((phi.phi[0] - phi.phi[1]).abs() < 1e-9, "symmetry");
    assert!(phi.phi[5].abs() < 1e-9, "null player");
    let other = TableGame::random(n, 8);
    let summed = TableGame {
        players: n,
        values: game
            .values
            .iter()
            .zip(&other.values)
            .map(|(a, b)| a + b)
            .collect(),
    };
    let lhs = shapley_exact(&summed, BUDGET).unwrap().phi;
    let rhs: Vec<f64> = phi
        .phi
        .iter()
        .zip(shapley_exact(&other, BUDGET).unwrap().phi)
        .map(|(a, b)| a + b)
        .collect();
    assert_close(&lhs, &rhs, 1e-9);
}

#[test]
fn the_sum_game_splits_to_each_value_by_every_method() {
    let values = vec![3.0, -1.5, 0.25, 7.0, 2.0];
    let game = AggregateGame::new(values.clone(), Aggregate::Sum).unwrap();
    assert_close(&linear_split(&game).unwrap().phi, &values, 1e-12);
    assert_close(&shapley_exact(&game, BUDGET).unwrap().phi, &values, 1e-9);
    let unions = vec![vec![0, 1], vec![2], vec![3, 4]];
    assert_close(
        &owen_values(&game, &unions, BUDGET).unwrap().phi,
        &values,
        1e-9,
    );
}

#[test]
fn a_linear_split_of_a_non_additive_value_is_refused() {
    let game = AggregateGame::new(vec![1.0, 4.0, 2.0], Aggregate::Max).unwrap();
    let error = linear_split(&game).unwrap_err();
    assert_eq!(error.code, AttributionCode::NonAdditive);
}

#[test]
fn exact_refuses_too_many_players_and_a_small_budget() {
    let game = AggregateGame::new(vec![1.0; 17], Aggregate::Max).unwrap();
    assert_eq!(
        shapley_exact(&game, BUDGET).unwrap_err().code,
        AttributionCode::TooManyPlayers
    );
    let game = AggregateGame::new(vec![1.0; 10], Aggregate::Max).unwrap();
    assert_eq!(
        shapley_exact(&game, 100).unwrap_err().code,
        AttributionCode::BudgetExceeded
    );
}

#[test]
fn every_aggregate_prefix_matches_revaluing_the_prefix() {
    let values = vec![5.0, -2.0, 9.5, 0.0, 3.25, 3.25, -7.0];
    let order = [4, 0, 6, 2, 1, 5, 3];
    let aggregates = [
        Aggregate::Sum,
        Aggregate::Mean,
        Aggregate::Max,
        Aggregate::Min,
        Aggregate::Quantile(0.95),
        Aggregate::Quantile(0.5),
    ];
    for aggregate in aggregates {
        let game = AggregateGame::new(values.clone(), aggregate).unwrap();
        let running = game.prefix_values(&order).unwrap();
        let revalued: Vec<f64> = (1..=order.len())
            .map(|k| {
                let mut members = order[..k].to_vec();
                members.sort_unstable();
                game.value(&members).unwrap()
            })
            .collect();
        assert_close(&running, &revalued, 1e-12);
    }
}

fn sample_spec(seed: u64) -> SampleSpec {
    SampleSpec {
        pairs: 64,
        seed,
        confidence: 0.95,
    }
}

#[test]
fn sampled_is_efficient_and_replays_from_its_seed() {
    let game = TableGame::random(9, 3);
    let a = shapley_sampled(&game, sample_spec(11), BUDGET).unwrap();
    let b = shapley_sampled(&game, sample_spec(11), BUDGET).unwrap();
    assert_eq!(a, b);
    let total: f64 = a.phi.iter().sum();
    assert!((total - a.total()).abs() < 1e-9, "{total} vs {}", a.total());
    assert_ne!(a, shapley_sampled(&game, sample_spec(12), BUDGET).unwrap());
}

#[test]
fn sampled_intervals_cover_the_exact_value_at_their_level() {
    let game = TableGame::random(10, 5);
    let exact = shapley_exact(&game, BUDGET).unwrap().phi;
    let (mut covered, mut total) = (0usize, 0usize);
    for seed in 0..200 {
        let sampled = shapley_sampled(&game, sample_spec(seed), BUDGET).unwrap();
        let widths = sampled.half_width.unwrap();
        for ((estimate, width), truth) in sampled.phi.iter().zip(&widths).zip(&exact) {
            covered += usize::from((estimate - truth).abs() <= *width);
            total += 1;
        }
    }
    let coverage = covered as f64 / total as f64;
    assert!(
        coverage >= 0.93,
        "empirical coverage {coverage} at nominal 0.95"
    );
}

#[test]
fn sampled_refuses_one_pair_and_an_overdrawn_budget() {
    let game = TableGame::random(4, 1);
    let mut spec = sample_spec(1);
    spec.pairs = 1;
    assert_eq!(
        shapley_sampled(&game, spec, BUDGET).unwrap_err().code,
        AttributionCode::InvalidInput
    );
    let error = shapley_sampled(&game, sample_spec(1), 10).unwrap_err();
    assert_eq!(error.code, AttributionCode::BudgetExceeded);
}

/// Permutations in which every union's members are contiguous.
fn union_consistent(unions: &[Vec<usize>], n: usize) -> Vec<Vec<usize>> {
    permutations(n)
        .into_iter()
        .filter(|order| {
            unions.iter().all(|u| {
                let at: Vec<usize> = u
                    .iter()
                    .map(|m| order.iter().position(|p| p == m).unwrap())
                    .collect();
                at.iter().max().unwrap() - at.iter().min().unwrap() + 1 == u.len()
            })
        })
        .collect()
}

#[test]
fn owen_matches_union_consistent_permutations_and_the_quotient_game() {
    let game = TableGame::random(5, 9);
    let unions = vec![vec![0, 1], vec![2], vec![3, 4]];
    let owen = owen_values(&game, &unions, BUDGET).unwrap();
    assert_close(
        &owen.phi,
        &mean_marginals(&game, &union_consistent(&unions, 5)),
        1e-9,
    );
    // The quotient game between unions: its Shapley value is each union's Owen total.
    let quotient = TableGame {
        players: 3,
        values: (0..8usize)
            .map(|mask| {
                let members: Vec<usize> = members_of(mask, 3)
                    .into_iter()
                    .flat_map(|u| unions[u].clone())
                    .collect();
                game.values[TableGame::mask(&members)]
            })
            .collect(),
    };
    let union_totals: Vec<f64> = unions
        .iter()
        .map(|u| u.iter().map(|&p| owen.phi[p]).sum())
        .collect();
    assert_close(
        &shapley_exact(&quotient, BUDGET).unwrap().phi,
        &union_totals,
        1e-9,
    );
    let singletons: Vec<Vec<usize>> = (0..5).map(|p| vec![p]).collect();
    let shapley = shapley_exact(&game, BUDGET).unwrap().phi;
    assert_close(
        &owen_values(&game, &singletons, BUDGET).unwrap().phi,
        &shapley,
        1e-9,
    );
}

#[test]
fn owen_refuses_a_non_partition() {
    let game = TableGame::random(3, 2);
    let error = owen_values(&game, &[vec![0, 1], vec![1, 2]], BUDGET).unwrap_err();
    assert_eq!(error.code, AttributionCode::InvalidInput);
}

fn factor_fixture() -> (Vec<f64>, Vec<Vec<f64>>) {
    let mut rng = Generator::new(21);
    let f1 = rng.normal(0.0, 1.0, 200);
    let f2 = rng.normal(0.0, 2.0, 200);
    let noise = rng.normal(0.0, 0.1, 200);
    let y = (0..200)
        .map(|t| 0.5 + 1.5 * f1[t] - 0.25 * f2[t] + noise[t])
        .collect();
    (y, vec![f1, f2])
}

#[test]
fn factor_ols_recovers_loadings_and_its_residuals_sum_to_zero() {
    let (y, factors) = factor_fixture();
    let fit = factor_ols(&y, &factors, HacLags::Auto).unwrap();
    assert!((fit.alpha - 0.5).abs() < 0.05, "alpha {}", fit.alpha);
    assert!((fit.betas[0] - 1.5).abs() < 0.05 && (fit.betas[1] + 0.25).abs() < 0.05);
    assert!(fit.residual_sum.abs() < 1e-9, "Σε = {}", fit.residual_sum);
    let parts = fit.alpha_contribution + fit.factor_contributions.iter().sum::<f64>();
    assert!((parts + fit.residual_sum - y.iter().sum::<f64>()).abs() < 1e-8);
    assert!(fit.r_squared > 0.99 && fit.alpha_se > 0.0 && fit.beta_se.iter().all(|s| *s > 0.0));
    assert_eq!(fit.lags, 4);
}

#[test]
fn factor_ols_refuses_a_collinear_design() {
    let (y, factors) = factor_fixture();
    let doubled: Vec<f64> = factors[0].iter().map(|v| 2.0 * v).collect();
    let error = factor_ols(&y, &[factors[0].clone(), doubled], HacLags::Fixed(0)).unwrap_err();
    assert_eq!(error.code, AttributionCode::Singular);
}

#[test]
fn the_normal_quantile_matches_its_reference_values() {
    assert!((normal_quantile(0.975).unwrap() - 1.959_963_984_540_054).abs() < 1e-12);
    assert!(normal_quantile(0.5).unwrap().abs() < 1e-15);
    assert!(normal_quantile(0.0).is_err() && normal_quantile(1.0).is_err());
}

fn logged(players: usize, values: &[(u64, f64)]) -> LoggedGame {
    LoggedGame::new(players, values.iter().copied().collect()).unwrap()
}

#[test]
fn a_fully_logged_game_has_the_tables_shapley_value() {
    let table = TableGame::random(4, 17);
    let observed: Vec<(u64, f64)> = (1..16u64).map(|m| (m, table.values[m as usize])).collect();
    let game = logged(4, &observed);
    let expected = shapley_exact(&table, BUDGET).unwrap().phi;
    assert_close(&shapley_exact(&game, BUDGET).unwrap().phi, &expected, 1e-12);
}

#[test]
fn an_unobserved_coalition_is_refused_not_interpolated() {
    let game = logged(2, &[(0b01, 0.4), (0b11, 0.9)]);
    let error = shapley_exact(&game, BUDGET).unwrap_err();
    assert_eq!(error.code, AttributionCode::UnsupportedCoalition);
}

#[test]
fn the_additive_fit_recovers_additive_weights_from_partial_logs() {
    // v(S) = Σ w_i with w = (0.1, 0.25, 0.4); only four of seven coalitions logged.
    let w = [0.1, 0.25, 0.4];
    let value = |mask: u64| {
        (0..3)
            .filter(|p| mask & (1 << p) != 0)
            .map(|p| w[p])
            .sum::<f64>()
    };
    let observed: Vec<(u64, f64)> = [0b011, 0b101, 0b110, 0b111]
        .iter()
        .map(|&m| (m, value(m)))
        .collect();
    let fit = additive_fit(&logged(3, &observed)).unwrap();
    assert_close(&fit.phi, &w, 1e-12);
    let only_pair = logged(2, &[(0b11, 0.5)]);
    assert_eq!(
        additive_fit(&only_pair).unwrap_err().code,
        AttributionCode::Singular
    );
}
