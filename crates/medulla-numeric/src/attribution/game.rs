//! Cooperative games: a value for every coalition of players.

use super::{invalid, AttributionResult};
use crate::detkernel::reduce::{serial_sum, sorted_ascending};

/// A cooperative game over players `0..players()`.
pub trait Game {
    /// Number of players.
    fn players(&self) -> usize;

    /// `v(S)` for the coalition whose members are `members` (ascending player indices;
    /// empty for `∅`).
    fn value(&self, members: &[usize]) -> AttributionResult<f64>;

    /// `v` of every non-empty prefix of `order` (`order[..1]`, `order[..2]`, …). The
    /// default re-values each prefix; a game with an incremental value overrides it.
    fn prefix_values(&self, order: &[usize]) -> AttributionResult<Vec<f64>> {
        let mut members: Vec<usize> = Vec::with_capacity(order.len());
        let mut out = Vec::with_capacity(order.len());
        for &player in order {
            let at = members.partition_point(|&m| m < player);
            members.insert(at, player);
            out.push(self.value(&members)?);
        }
        Ok(out)
    }
}

/// The aggregate a coalition's value is.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Aggregate {
    /// `Σ x_i` — additive, so its Shapley value is `x_i` itself.
    Sum,
    /// Mean of the members' values.
    Mean,
    /// Largest member value.
    Max,
    /// Smallest member value.
    Min,
    /// The `q`-quantile (linear interpolation, numpy's default), `q` in `[0, 1]`.
    Quantile(f64),
}

impl Aggregate {
    /// Whether `v(S) = Σ v({i})` holds for every coalition (the linear split applies).
    pub fn is_additive(self) -> bool {
        matches!(self, Self::Sum)
    }

    fn of(self, values: &[f64]) -> f64 {
        match self {
            Self::Sum => serial_sum(values),
            Self::Mean => serial_sum(values) / values.len() as f64,
            Self::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            Self::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
            Self::Quantile(q) => quantile_sorted(&sorted_ascending(values), q),
        }
    }
}

/// numpy `quantile` (linear) of an ascending, non-empty slice.
fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    let position = q * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64)
}

/// `v(S) = agg{x_i : i ∈ S}` with `v(∅) = 0`: the game the query surfaces attribute
/// (players are rows, `x_i` is each row's value).
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateGame {
    values: Vec<f64>,
    aggregate: Aggregate,
}

impl AggregateGame {
    /// Validate finite values and a quantile level in `[0, 1]`.
    pub fn new(values: Vec<f64>, aggregate: Aggregate) -> AttributionResult<Self> {
        if let Some(index) = values.iter().position(|v| !v.is_finite()) {
            return Err(invalid(format!("player {index} has a non-finite value")));
        }
        if let Aggregate::Quantile(q) = aggregate {
            if !(0.0..=1.0).contains(&q) {
                return Err(invalid("the quantile level must be in [0, 1]"));
            }
        }
        Ok(Self { values, aggregate })
    }

    /// The aggregate.
    pub fn aggregate(&self) -> Aggregate {
        self.aggregate
    }

    /// Player `i`'s own value.
    pub fn player_value(&self, player: usize) -> f64 {
        self.values[player]
    }
}

impl Game for AggregateGame {
    fn players(&self) -> usize {
        self.values.len()
    }

    fn value(&self, members: &[usize]) -> AttributionResult<f64> {
        if members.is_empty() {
            return Ok(0.0);
        }
        let values: Vec<f64> = members.iter().map(|&m| self.values[m]).collect();
        Ok(self.aggregate.of(&values))
    }

    /// Running sum / max / min in O(1) per prefix; a quantile keeps a sorted prefix.
    fn prefix_values(&self, order: &[usize]) -> AttributionResult<Vec<f64>> {
        let mut state = Running::new(self.aggregate);
        Ok(order.iter().map(|&p| state.push(self.values[p])).collect())
    }
}

/// The incremental state of one aggregate along a permutation.
struct Running {
    aggregate: Aggregate,
    sum: f64,
    count: f64,
    max: f64,
    min: f64,
    sorted: Vec<f64>,
}

impl Running {
    fn new(aggregate: Aggregate) -> Self {
        Self {
            aggregate,
            sum: 0.0,
            count: 0.0,
            max: f64::NEG_INFINITY,
            min: f64::INFINITY,
            sorted: Vec::new(),
        }
    }

    fn push(&mut self, value: f64) -> f64 {
        self.sum += value;
        self.count += 1.0;
        self.max = self.max.max(value);
        self.min = self.min.min(value);
        match self.aggregate {
            Aggregate::Sum => self.sum,
            Aggregate::Mean => self.sum / self.count,
            Aggregate::Max => self.max,
            Aggregate::Min => self.min,
            Aggregate::Quantile(q) => {
                let at = self.sorted.partition_point(|x| x.total_cmp(&value).is_lt());
                self.sorted.insert(at, value);
                quantile_sorted(&self.sorted, q)
            }
        }
    }
}

/// A game known only where it was OBSERVED: `v(S)` for the coalitions a log holds (for
/// example assembled slates whose outcomes were independently evaluated), `v(∅) = 0`.
/// Asking for any other coalition is refused with `UNSUPPORTED_COALITION` — a logged
/// game is never interpolated.
#[derive(Debug, Clone, PartialEq)]
pub struct LoggedGame {
    players: usize,
    values: std::collections::BTreeMap<u64, f64>,
}

impl LoggedGame {
    /// `values` maps member masks (bit `i` = player `i`) to observed values; at most 63
    /// players.
    pub fn new(
        players: usize,
        values: std::collections::BTreeMap<u64, f64>,
    ) -> AttributionResult<Self> {
        if players == 0 || players > 63 {
            return Err(invalid("a logged game has 1..=63 players"));
        }
        let limit = 1u64 << players;
        if values.keys().any(|&mask| mask == 0 || mask >= limit) {
            return Err(invalid(
                "an observed coalition names a player outside the game",
            ));
        }
        if values.values().any(|v| !v.is_finite()) {
            return Err(invalid("an observed coalition value is not finite"));
        }
        Ok(Self { players, values })
    }

    /// The observed coalitions and their values, in mask order.
    pub fn observed(&self) -> &std::collections::BTreeMap<u64, f64> {
        &self.values
    }
}

impl Game for LoggedGame {
    fn players(&self) -> usize {
        self.players
    }

    fn value(&self, members: &[usize]) -> AttributionResult<f64> {
        let mask = members.iter().fold(0u64, |m, &p| m | (1 << p));
        if mask == 0 {
            return Ok(0.0);
        }
        self.values.get(&mask).copied().ok_or_else(|| {
            super::AttributionError::new(
                super::AttributionCode::UnsupportedCoalition,
                format!("no observed value for the coalition {members:?}"),
            )
        })
    }
}
