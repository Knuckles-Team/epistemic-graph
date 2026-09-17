//! Additive Holt-Winters forecasting.

use super::{linear_regression, mean, residual_std, widening_bounds, Forecast};

// ─────────────────────────── Holt-Winters / ETS ───────────────────────────

/// Additive Holt-Winters (CONCEPT:EG-KG.mining.holt-winters): level/trend/seasonal
/// exponential smoothing. `period == 0` (or too little data for two full
/// seasonal cycles) degrades to Holt's linear-trend method (ETS(A,A,N) — no
/// seasonal component).
pub fn holt_winters(
    series: &[f64],
    period: usize,
    alpha: f64,
    beta: f64,
    gamma: f64,
    horizon: usize,
    confidence: f64,
) -> Forecast {
    let n = series.len();
    let mut state = initialise_state(series, period);
    let resid = smooth(series, period, alpha, beta, gamma, &mut state);
    let point = forecast_points(&state, n, period, horizon);
    let (lower, upper) = widening_bounds(&point, residual_std(&resid, state.start_t), confidence);
    Forecast {
        values: point,
        lower,
        upper,
        trend: Vec::new(),
        seasonal: Vec::new(),
        residual: Vec::new(),
    }
}

struct HoltState {
    seasonal_on: bool,
    start_t: usize,
    level: f64,
    trend: f64,
    season: Vec<f64>,
}

fn initialise_state(series: &[f64], period: usize) -> HoltState {
    let n = series.len();
    let seasonal_on = period >= 2 && n >= 2 * period;

    // Initialize level/trend from a whole-series OLS regression (NOT a
    // first-vs-second-season mean difference, which centers the estimate mid-
    // window and introduces a systematic phase lag once the recursion starts at
    // `start_t`). The regression line's value AT `start_t - 1` is the level the
    // recursion below assumes it already has when it begins updating at
    // `start_t`.
    let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let (a, b) = linear_regression(&ts, series);
    let start_t = if seasonal_on { period } else { 1 };
    let level = a + b * (start_t as f64 - 1.0);
    let trend = b;
    let season = if seasonal_on {
        seasonal_initial_values(series, period, a, b)
    } else {
        Vec::new()
    };
    HoltState {
        seasonal_on,
        start_t,
        level,
        trend,
        season,
    }
}

fn seasonal_initial_values(series: &[f64], period: usize, a: f64, b: f64) -> Vec<f64> {
    // Detrend every point against the SAME regression line, then average by
    // phase over every full cycle available (not just the first two) —
    // robust + lag-free.
    let mut sums = vec![0.0; period];
    let mut counts = vec![0usize; period];
    for (t, &y) in series.iter().enumerate() {
        let fitted = a + b * t as f64;
        sums[t % period] += y - fitted;
        counts[t % period] += 1;
    }
    let mut season: Vec<f64> = sums
        .iter()
        .zip(&counts)
        .map(|(&sum, &count)| if count > 0 { sum / count as f64 } else { 0.0 })
        .collect();
    let season_mean = mean(&season);
    for value in season.iter_mut() {
        *value -= season_mean;
    }
    season
}

fn smooth(
    series: &[f64],
    period: usize,
    alpha: f64,
    beta: f64,
    gamma: f64,
    state: &mut HoltState,
) -> Vec<f64> {
    let n = series.len();
    let mut fitted = vec![0.0; n];
    if state.start_t <= n {
        fitted[..state.start_t].copy_from_slice(&series[..state.start_t]);
    }
    let mut resid = vec![0.0; n];

    for t in state.start_t..n {
        // `season` is a length-`period` ring updated in place each step, so the
        // "previous" value for slot `t % period` is simply its current entry.
        let seasonal_prev = if state.seasonal_on {
            state.season[t % period]
        } else {
            0.0
        };
        let pred = state.level + state.trend + seasonal_prev;
        fitted[t] = pred;
        resid[t] = series[t] - pred;

        let new_level = if state.seasonal_on {
            alpha * (series[t] - seasonal_prev) + (1.0 - alpha) * (state.level + state.trend)
        } else {
            alpha * series[t] + (1.0 - alpha) * (state.level + state.trend)
        };
        let new_trend = beta * (new_level - state.level) + (1.0 - beta) * state.trend;
        if state.seasonal_on {
            state.season[t % period] =
                gamma * (series[t] - new_level) + (1.0 - gamma) * seasonal_prev;
        }
        state.level = new_level;
        state.trend = new_trend;
    }
    resid
}

fn forecast_points(state: &HoltState, n: usize, period: usize, horizon: usize) -> Vec<f64> {
    let mut point = Vec::with_capacity(horizon);
    for h in 1..=horizon {
        let seasonal_h = if state.seasonal_on {
            state.season[(n + h - 1) % period]
        } else {
            0.0
        };
        point.push(state.level + h as f64 * state.trend + seasonal_h);
    }
    point
}
