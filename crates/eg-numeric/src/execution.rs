//! Regime detection and optimal-execution kernels (MEDULLA-FINANCE-PRIMITIVES-R613).
//!
//! A Markov chain over thresholded returns estimates a transition matrix and
//! forecasts regime probabilities; Almgren-Chriss schedules solve the discrete
//! and continuous impact-minimising trajectories; Avellaneda-Stoikov quotes and
//! pairs-spread signals follow from declared parameters. Every kernel is pure
//! and deterministic: transcendentals go through the pinned
//! [`crate::detkernel::math`], and a bad parameter is refused with the stable
//! code `EXEC_PARAM_INVALID` naming the field.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::detkernel::math;

/// The stable code of a refused execution parameter.
pub const EXEC_PARAM_INVALID: &str = "EXEC_PARAM_INVALID";

/// A refused parameter; `field` names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecError {
    pub field: &'static str,
}

impl ExecError {
    /// The stable wire code of this refusal.
    pub fn code(&self) -> &'static str {
        EXEC_PARAM_INVALID
    }
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{EXEC_PARAM_INVALID}: {}", self.field)
    }
}

impl std::error::Error for ExecError {}

fn invalid(field: &'static str) -> ExecError {
    ExecError { field }
}

fn positive(value: f64, field: &'static str) -> Result<f64, ExecError> {
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(invalid(field))
    }
}

/// A validated identifier: 1 to 64 characters of `[A-Za-z0-9_.:-]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Ident(String);

impl Ident {
    pub fn new(value: &str) -> Result<Self, ExecError> {
        let valid = !value.is_empty()
            && value.len() <= 64
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'));
        if valid {
            Ok(Self(value.to_string()))
        } else {
            Err(invalid("identifier"))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Ident {
    type Error = ExecError;

    fn try_from(value: String) -> Result<Self, ExecError> {
        Ident::new(&value)
    }
}

impl From<Ident> for String {
    fn from(value: Ident) -> String {
        value.0
    }
}

/// One regime of the thresholded-return chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegimeState {
    pub id: Ident,
    pub index: usize,
}

/// A row-stochastic transition matrix over named regimes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitionMatrix {
    pub states: Vec<Ident>,
    pub probabilities: Vec<Vec<f64>>,
}

impl TransitionMatrix {
    /// Validate squareness and that every row is non-negative and sums to one.
    pub fn new(states: Vec<Ident>, probabilities: Vec<Vec<f64>>) -> Result<Self, ExecError> {
        let n = states.len();
        if n == 0 || probabilities.len() != n {
            return Err(invalid("transition_matrix"));
        }
        for row in &probabilities {
            let sum: f64 = row.iter().sum();
            if row.len() != n
                || row.iter().any(|p| !p.is_finite() || *p < 0.0)
                || (sum - 1.0).abs() > 1e-9
            {
                return Err(invalid("transition_matrix"));
            }
        }
        Ok(Self {
            states,
            probabilities,
        })
    }

    /// `p * T`: one step of the chain applied to a probability row vector.
    fn step(&self, p: &[f64]) -> Vec<f64> {
        let n = self.states.len();
        (0..n)
            .map(|j| (0..n).map(|i| p[i] * self.probabilities[i][j]).sum())
            .collect()
    }
}

/// The regimes of the thresholded chain, in index order.
pub const REGIME_DOWN: usize = 0;
pub const REGIME_FLAT: usize = 1;
pub const REGIME_UP: usize = 2;

/// Fit result: the transition matrix, regime probabilities `horizon` steps
/// ahead of the last observation, and a one-step backtest summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeForecast {
    pub matrix: TransitionMatrix,
    pub current_state: usize,
    pub forecast: Vec<f64>,
    pub horizon: usize,
    pub threshold: f64,
    /// Fraction of one-step predictions whose most likely regime was realised.
    pub hit_rate: f64,
    pub transitions: usize,
    pub solver_status: SolverStatus,
}

/// How a result was produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SolverStatus {
    /// An exact closed-form or counting solution.
    ClosedForm,
}

fn regime_of(r: f64, threshold: f64) -> usize {
    if r < -threshold {
        REGIME_DOWN
    } else if r > threshold {
        REGIME_UP
    } else {
        REGIME_FLAT
    }
}

/// Estimate the three-regime transition matrix from `returns` thresholded at
/// `+-threshold` by transition counting (a row with no outgoing transition is
/// uniform), forecast `horizon` steps from the last regime, and summarise the
/// one-step predictions. Needs at least two returns.
pub fn fit_regime_chain(
    returns: &[f64],
    threshold: f64,
    horizon: usize,
) -> Result<RegimeForecast, ExecError> {
    positive(threshold, "threshold")?;
    if horizon == 0 {
        return Err(invalid("horizon"));
    }
    if returns.len() < 2 || returns.iter().any(|r| !r.is_finite()) {
        return Err(invalid("returns"));
    }
    let states: Vec<usize> = returns.iter().map(|&r| regime_of(r, threshold)).collect();
    let mut counts = [[0u32; 3]; 3];
    for pair in states.windows(2) {
        counts[pair[0]][pair[1]] += 1;
    }
    let rows: Vec<Vec<f64>> = counts
        .iter()
        .map(|row| {
            let total: u32 = row.iter().sum();
            if total == 0 {
                vec![1.0 / 3.0; 3]
            } else {
                row.iter()
                    .map(|&c| f64::from(c) / f64::from(total))
                    .collect()
            }
        })
        .collect();
    let ids = ["down", "flat", "up"]
        .iter()
        .map(|id| Ident::new(id))
        .collect::<Result<Vec<_>, _>>()?;
    let matrix = TransitionMatrix::new(ids, rows)?;
    let current = states[states.len() - 1];
    let mut forecast = vec![0.0; 3];
    forecast[current] = 1.0;
    for _ in 0..horizon {
        forecast = matrix.step(&forecast);
    }
    let hits = states
        .windows(2)
        .filter(|pair| {
            let row = &matrix.probabilities[pair[0]];
            let predicted = (0..3).fold(0, |best, j| if row[j] > row[best] { j } else { best });
            predicted == pair[1]
        })
        .count();
    let transitions = states.len() - 1;
    Ok(RegimeForecast {
        matrix,
        current_state: current,
        forecast,
        horizon,
        threshold,
        hit_rate: hits as f64 / transitions as f64,
        transitions,
        solver_status: SolverStatus::ClosedForm,
    })
}

/// Almgren-Chriss parameters: liquidate `total_shares` over `horizon` in
/// `slices` equal intervals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlmgrenChriss {
    pub symbol: Ident,
    pub total_shares: f64,
    pub horizon: f64,
    pub slices: usize,
    pub volatility: f64,
    pub risk_aversion: f64,
    /// Temporary impact per share rate.
    pub temporary_impact: f64,
    /// Permanent impact per share (a path-independent constant `gamma X^2 / 2`).
    pub permanent_impact: f64,
}

impl AlmgrenChriss {
    /// Validate every parameter; `risk_aversion`, `horizon` and `volatility`
    /// must be positive.
    pub fn validate(&self) -> Result<(), ExecError> {
        positive(self.risk_aversion, "risk_aversion")?;
        positive(self.horizon, "horizon")?;
        positive(self.volatility, "volatility")?;
        positive(self.total_shares, "total_shares")?;
        positive(self.temporary_impact, "temporary_impact")?;
        if !self.permanent_impact.is_finite() || self.permanent_impact < 0.0 {
            return Err(invalid("permanent_impact"));
        }
        if self.slices == 0 || self.slices > 1_000_000 {
            return Err(invalid("slices"));
        }
        Ok(())
    }
}

/// A liquidation schedule with the parameters and objective that produced it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub params: AlmgrenChriss,
    pub continuous: bool,
    /// Holdings `x_0 .. x_N` at the slice boundaries; `x_0` is the total and `x_N` is zero.
    pub holdings: Vec<f64>,
    /// Shares sold in each slice.
    pub trades: Vec<f64>,
    /// Expected impact cost plus `risk_aversion` times the variance of the path.
    pub objective: f64,
    pub solver_status: SolverStatus,
}

fn sinh(x: f64) -> f64 {
    (math::exp(x) - math::exp(-x)) / 2.0
}

/// The impact-minimising liquidation schedule. The continuous solution samples
/// `x(t) = X sinh(kappa (T - t)) / sinh(kappa T)` with `kappa^2 = lambda sigma^2 / eta`;
/// the discrete solution uses the exact grid decay `cosh(kappa tau) = 1 + kappa^2 tau^2 / 2`.
pub fn almgren_chriss_schedule(
    params: &AlmgrenChriss,
    continuous: bool,
) -> Result<ExecutionPlan, ExecError> {
    params.validate()?;
    let n = params.slices;
    let tau = params.horizon / n as f64;
    let kappa_sq =
        params.risk_aversion * params.volatility * params.volatility / params.temporary_impact;
    let kappa_tilde = if continuous {
        kappa_sq.sqrt()
    } else {
        let y = 1.0 + kappa_sq * tau * tau / 2.0;
        math::ln(y + (y * y - 1.0).sqrt()) / tau
    };
    let denominator = sinh(kappa_tilde * params.horizon);
    if !denominator.is_finite() || denominator <= 0.0 {
        return Err(invalid("risk_aversion"));
    }
    let holdings: Vec<f64> = (0..=n)
        .map(|k| {
            if k == n {
                0.0
            } else {
                params.total_shares * sinh(kappa_tilde * (params.horizon - tau * k as f64))
                    / denominator
            }
        })
        .collect();
    let trades: Vec<f64> = holdings.windows(2).map(|w| w[0] - w[1]).collect();
    let impact: f64 = trades
        .iter()
        .map(|t| params.temporary_impact * t * t / tau)
        .sum::<f64>()
        + 0.5 * params.permanent_impact * params.total_shares * params.total_shares;
    let variance: f64 = holdings[1..]
        .iter()
        .map(|x| params.volatility * params.volatility * tau * x * x)
        .sum();
    Ok(ExecutionPlan {
        params: params.clone(),
        continuous,
        holdings,
        trades,
        objective: impact + params.risk_aversion * variance,
        solver_status: SolverStatus::ClosedForm,
    })
}

/// A two-sided market-making quote around a reservation price.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketMakingQuote {
    pub symbol: Ident,
    pub reservation_price: f64,
    pub spread: f64,
    pub bid: f64,
    pub ask: f64,
}

/// Avellaneda-Stoikov quote: reservation `r = s - q gamma sigma^2 (T - t)` and
/// spread `gamma sigma^2 (T - t) + (2 / gamma) ln(1 + gamma / k)`.
pub fn market_making_quote(
    symbol: &str,
    mid: f64,
    inventory: f64,
    risk_aversion: f64,
    volatility: f64,
    time_remaining: f64,
    order_arrival_decay: f64,
) -> Result<MarketMakingQuote, ExecError> {
    let symbol = Ident::new(symbol)?;
    positive(mid, "mid")?;
    if !inventory.is_finite() {
        return Err(invalid("inventory"));
    }
    positive(risk_aversion, "risk_aversion")?;
    positive(volatility, "volatility")?;
    positive(time_remaining, "horizon")?;
    positive(order_arrival_decay, "order_arrival_decay")?;
    let variance_term = risk_aversion * volatility * volatility * time_remaining;
    let reservation_price = mid - inventory * variance_term;
    let spread =
        variance_term + (2.0 / risk_aversion) * math::ln(1.0 + risk_aversion / order_arrival_decay);
    Ok(MarketMakingQuote {
        symbol,
        reservation_price,
        spread,
        bid: reservation_price - spread / 2.0,
        ask: reservation_price + spread / 2.0,
    })
}

/// The side a pairs spread signal recommends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PairsSide {
    /// The spread is rich: sell `y`, buy `hedge_ratio` of `x`.
    ShortSpread,
    /// The spread is cheap: buy `y`, sell `hedge_ratio` of `x`.
    LongSpread,
    Flat,
}

/// A pairs-trading signal on the spread `y - hedge_ratio x`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairsSignal {
    pub y: Ident,
    pub x: Ident,
    pub hedge_ratio: f64,
    pub z_score: f64,
    pub entry_z: f64,
    pub side: PairsSide,
}

/// Score the newest spread against its history's population mean and standard
/// deviation and recommend a side when `|z|` reaches `entry_z`.
pub fn pairs_signal(
    y_name: &str,
    x_name: &str,
    y: &[f64],
    x: &[f64],
    hedge_ratio: f64,
    entry_z: f64,
) -> Result<PairsSignal, ExecError> {
    let (y_id, x_id) = (Ident::new(y_name)?, Ident::new(x_name)?);
    if !hedge_ratio.is_finite() {
        return Err(invalid("hedge_ratio"));
    }
    positive(entry_z, "entry_z")?;
    if y.len() != x.len() || y.len() < 3 || y.iter().chain(x).any(|v| !v.is_finite()) {
        return Err(invalid("series"));
    }
    let spread: Vec<f64> = y.iter().zip(x).map(|(a, b)| a - hedge_ratio * b).collect();
    let n = spread.len() as f64;
    let mean = spread.iter().sum::<f64>() / n;
    let std = (spread.iter().map(|s| (s - mean) * (s - mean)).sum::<f64>() / n).sqrt();
    if std <= 0.0 {
        return Err(invalid("series"));
    }
    let z_score = (spread[spread.len() - 1] - mean) / std;
    let side = if z_score >= entry_z {
        PairsSide::ShortSpread
    } else if z_score <= -entry_z {
        PairsSide::LongSpread
    } else {
        PairsSide::Flat
    };
    Ok(PairsSignal {
        y: y_id,
        x: x_id,
        hedge_ratio,
        z_score,
        entry_z,
        side,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(risk_aversion: f64, slices: usize) -> AlmgrenChriss {
        AlmgrenChriss {
            symbol: Ident::new("AAPL").unwrap(),
            total_shares: 1_000.0,
            horizon: 1.0,
            slices,
            volatility: 1.0,
            risk_aversion,
            temporary_impact: 1.0,
            permanent_impact: 0.0,
        }
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R613.1
    #[test]
    fn each_execution_type_round_trips_and_an_invalid_identifier_is_rejected() {
        let plan = almgren_chriss_schedule(&params(0.5, 4), false).unwrap();
        let json = serde_json::to_string(&plan).unwrap();
        assert_eq!(serde_json::from_str::<ExecutionPlan>(&json).unwrap(), plan);
        let params_json = serde_json::to_string(&plan.params).unwrap();
        assert_eq!(
            serde_json::from_str::<AlmgrenChriss>(&params_json).unwrap(),
            plan.params
        );
        let forecast = fit_regime_chain(&[0.02, -0.02, 0.0, 0.02, 0.02, -0.02], 0.01, 2).unwrap();
        let json = serde_json::to_string(&forecast).unwrap();
        assert_eq!(
            serde_json::from_str::<RegimeForecast>(&json).unwrap(),
            forecast
        );
        let state = RegimeState {
            id: Ident::new("up").unwrap(),
            index: REGIME_UP,
        };
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(serde_json::from_str::<RegimeState>(&json).unwrap(), state);
        let matrix = TransitionMatrix::new(
            vec![Ident::new("a").unwrap(), Ident::new("b").unwrap()],
            vec![vec![0.25, 0.75], vec![0.5, 0.5]],
        )
        .unwrap();
        let json = serde_json::to_string(&matrix).unwrap();
        assert_eq!(
            serde_json::from_str::<TransitionMatrix>(&json).unwrap(),
            matrix
        );
        let quote = market_making_quote("AAPL", 100.0, 0.0, 0.1, 2.0, 1.0, 1.5).unwrap();
        let json = serde_json::to_string(&quote).unwrap();
        assert_eq!(
            serde_json::from_str::<MarketMakingQuote>(&json).unwrap(),
            quote
        );
        let signal = pairs_signal("A", "B", &[1.0, 2.0, 3.0], &[1.0, 1.0, 1.0], 1.0, 1.0).unwrap();
        let json = serde_json::to_string(&signal).unwrap();
        assert_eq!(serde_json::from_str::<PairsSignal>(&json).unwrap(), signal);
        // Identifiers are validated at construction, including on deserialisation.
        let too_long = "x".repeat(65);
        for bad in ["", " ", "has space", "semi;colon", too_long.as_str()] {
            assert_eq!(Ident::new(bad).unwrap_err().field, "identifier");
        }
        assert!(serde_json::from_str::<Ident>("\"bad id\"").is_err());
        assert!(TransitionMatrix::new(vec![Ident::new("a").unwrap()], vec![vec![0.5]]).is_err());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R613.2
    #[test]
    fn markov_chain_estimates_the_matrix_and_forecasts_regime_probabilities() {
        // Regimes at +-1%: up, down, up, up, down, flat -> states 2,0,2,2,0,1.
        // Transitions: 2->0, 0->2, 2->2, 2->0, 0->1.
        // Row 0 (down): to up 1/2, to flat 1/2. Row 2 (up): to down 2/3, to up 1/3.
        // Row 1 (flat) has no outgoing transition and is uniform.
        let returns = [0.02, -0.02, 0.02, 0.02, -0.02, 0.0];
        let fit = fit_regime_chain(&returns, 0.01, 1).unwrap();
        let m = &fit.matrix.probabilities;
        assert_eq!(m[0], vec![0.0, 0.5, 0.5]);
        assert_eq!(m[1], vec![1.0 / 3.0; 3]);
        assert_eq!(m[2], vec![2.0 / 3.0, 0.0, 1.0 / 3.0]);
        assert_eq!(fit.current_state, REGIME_FLAT);
        // One step from flat is its uniform row; two steps average the three rows.
        assert_eq!(fit.forecast, vec![1.0 / 3.0; 3]);
        let two = fit_regime_chain(&returns, 0.01, 2).unwrap();
        let expected_down = (0.0 + 1.0 / 3.0 + 2.0 / 3.0) / 3.0;
        assert!((two.forecast[0] - expected_down).abs() < 1e-12);
        assert!((two.forecast.iter().sum::<f64>() - 1.0).abs() < 1e-12);
        // Backtest: the most likely next regime is flat after down (first of the
        // tied 1/2 entries) and down after up. Realised: 2->0 hit, 0->2 miss,
        // 2->2 miss, 2->0 hit, 0->1 hit.
        assert_eq!(fit.transitions, 5);
        assert!((fit.hit_rate - 3.0 / 5.0).abs() < 1e-12);
        assert_eq!(fit, fit_regime_chain(&returns, 0.01, 1).unwrap());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R613.2
    #[test]
    fn continuous_schedule_matches_the_hand_computed_sinh_trajectory() {
        // kappa T = ln 2 with eta = sigma = T = 1, so lambda = (ln 2)^2 and
        // x(T/2) / X = sinh(ln2 / 2) / sinh(ln 2) = (sqrt2 - 1/sqrt2) / 2 / 0.75.
        let ln2 = std::f64::consts::LN_2;
        let plan = almgren_chriss_schedule(&params(ln2 * ln2, 2), true).unwrap();
        let want = 1_000.0 * 0.471_404_520_791_031_7;
        assert!((plan.holdings[1] - want).abs() < 1e-9);
        assert_eq!(plan.holdings[0], 1_000.0);
        assert_eq!(plan.holdings[2], 0.0);
        assert!((plan.trades.iter().sum::<f64>() - 1_000.0).abs() < 1e-9);
        assert!(plan.continuous);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R613.2
    #[test]
    fn discrete_schedule_is_the_impact_minimiser_and_risk_aversion_front_loads() {
        let plan = almgren_chriss_schedule(&params(2.0, 5), false).unwrap();
        assert_eq!(plan.holdings.len(), 6);
        assert!((plan.trades.iter().sum::<f64>() - 1_000.0).abs() < 1e-9);
        assert!(plan.trades.windows(2).all(|t| t[0] > t[1]), "front-loaded");
        // Perturbing any interior holding raises the objective.
        let objective = |holdings: &[f64]| {
            let tau = 0.2;
            let cost: f64 = holdings
                .windows(2)
                .map(|w| (w[0] - w[1]) * (w[0] - w[1]) / tau)
                .sum();
            let var: f64 = holdings[1..].iter().map(|x| tau * x * x).sum();
            cost + 2.0 * var
        };
        let best = objective(&plan.holdings);
        assert!((best - plan.objective).abs() < 1e-6 * best);
        for k in 1..5 {
            for delta in [-1.0, 1.0] {
                let mut moved = plan.holdings.clone();
                moved[k] += delta;
                assert!(objective(&moved) > best);
            }
        }
        // Greater risk aversion sells more in the first slice; tiny aversion is near TWAP.
        let patient = almgren_chriss_schedule(&params(1e-6, 5), false).unwrap();
        assert!(plan.trades[0] > patient.trades[0]);
        assert!((patient.trades[0] - 200.0).abs() < 0.01);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R613.2
    #[test]
    fn market_making_quote_and_pairs_signal_follow_their_formulas() {
        // gamma 0.1, sigma 2, T-t 1: variance term 0.4; inventory 5 shifts the
        // reservation by 2. Spread 0.4 + 20 ln(1 + 0.1/1.5).
        let quote = market_making_quote("AAPL", 100.0, 5.0, 0.1, 2.0, 1.0, 1.5).unwrap();
        assert!((quote.reservation_price - 98.0).abs() < 1e-12);
        let spread = 0.4 + 20.0 * math::ln(1.0 + 0.1 / 1.5);
        assert!((quote.spread - spread).abs() < 1e-9);
        assert!((quote.bid - (98.0 - spread / 2.0)).abs() < 1e-9);
        assert!((quote.ask - (98.0 + spread / 2.0)).abs() < 1e-9);
        // Spread y - x over [0, 0, 0, 4]: mean 1, population std sqrt(3), z = sqrt(3).
        let y = [10.0, 11.0, 12.0, 17.0];
        let x = [10.0, 11.0, 12.0, 13.0];
        let rich = pairs_signal("Y", "X", &y, &x, 1.0, 1.5).unwrap();
        assert!((rich.z_score - 3.0_f64.sqrt()).abs() < 1e-12);
        assert_eq!(rich.side, PairsSide::ShortSpread);
        assert_eq!(
            pairs_signal("Y", "X", &y, &x, 1.0, 2.0).unwrap().side,
            PairsSide::Flat
        );
        let cheap = pairs_signal("Y", "X", &[10.0, 11.0, 12.0, 9.0], &x, 1.0, 1.5).unwrap();
        assert_eq!(cheap.side, PairsSide::LongSpread);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R613.4
    #[test]
    fn non_positive_risk_aversion_horizon_or_volatility_is_refused_with_the_field_name() {
        let mut bad = params(0.5, 4);
        bad.risk_aversion = 0.0;
        let error = almgren_chriss_schedule(&bad, false).unwrap_err();
        assert_eq!(error.code(), "EXEC_PARAM_INVALID");
        assert_eq!(error.field, "risk_aversion");
        let mut bad = params(0.5, 4);
        bad.horizon = -1.0;
        assert_eq!(
            almgren_chriss_schedule(&bad, true).unwrap_err().field,
            "horizon"
        );
        let mut bad = params(0.5, 4);
        bad.volatility = f64::NAN;
        assert_eq!(
            almgren_chriss_schedule(&bad, false).unwrap_err().field,
            "volatility"
        );
        let mut bad = params(0.5, 0);
        assert_eq!(
            almgren_chriss_schedule(&bad, false).unwrap_err().field,
            "slices"
        );
        bad.slices = 4;
        bad.temporary_impact = 0.0;
        assert_eq!(
            almgren_chriss_schedule(&bad, false).unwrap_err().field,
            "temporary_impact"
        );
        // The same refusals guard the quote and regime kernels; nothing is returned.
        assert_eq!(
            market_making_quote("A", 100.0, 0.0, 0.0, 1.0, 1.0, 1.0)
                .unwrap_err()
                .field,
            "risk_aversion"
        );
        assert_eq!(
            market_making_quote("A", 100.0, 0.0, 0.1, -1.0, 1.0, 1.0)
                .unwrap_err()
                .field,
            "volatility"
        );
        assert_eq!(
            market_making_quote("A", 100.0, 0.0, 0.1, 1.0, 0.0, 1.0)
                .unwrap_err()
                .field,
            "horizon"
        );
        assert_eq!(
            fit_regime_chain(&[0.1, 0.2], 0.0, 1).unwrap_err().field,
            "threshold"
        );
        assert_eq!(
            fit_regime_chain(&[0.1, 0.2], 0.01, 0).unwrap_err().field,
            "horizon"
        );
        assert_eq!(
            fit_regime_chain(&[0.1], 0.01, 1).unwrap_err().field,
            "returns"
        );
        assert_eq!(
            pairs_signal("Y", "X", &[1.0, 1.0, 1.0], &[1.0, 1.0, 1.0], 1.0, 1.0)
                .unwrap_err()
                .field,
            "series"
        );
    }
}
