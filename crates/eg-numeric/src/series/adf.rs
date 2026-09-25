//! Generic augmented Dickey-Fuller series test (constant, no trend).

use serde::{Deserialize, Serialize};

use super::ols::ols_with_se;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AdfEstimate {
    pub statistic: f64,
    pub used_lag: usize,
    pub n_obs: usize,
    pub crit_1pct: f64,
    pub crit_5pct: f64,
    pub crit_10pct: f64,
    pub p_value_approx: f64,
    pub stationary_1pct: bool,
    pub stationary_5pct: bool,
    pub stationary_10pct: bool,
}

/// Finite-sample MacKinnon critical value for the ADF "constant, no trend" case:
/// CV(T) = β∞ + β1/T + β2/T² (MacKinnon 1991 response-surface coefficients).
fn mackinnon_crit(level: u8, t: f64) -> f64 {
    // (β∞, β1, β2) per significance level for the τ_c (constant) case.
    let (b0, b1, b2) = match level {
        1 => (-3.43035, -6.5393, -16.786),
        5 => (-2.86154, -2.8903, -4.234),
        _ => (-2.56677, -1.5384, -2.809), // 10%
    };
    b0 + b1 / t + b2 / (t * t)
}

/// Monotone approximate p-value from the ADF statistic and the three interpolated
/// critical values. Piecewise-linear in the statistic; clearly an approximation
/// (the exact value needs MacKinnon's full surface), but correct in ordering and
/// bracket — more useful than a single fixed cutoff.
fn adf_pvalue(stat: f64, c1: f64, c5: f64, c10: f64) -> f64 {
    // anchors: (stat, p). More-negative stat ⇒ smaller p.
    if stat <= c1 {
        return (0.01 * (stat / c1).clamp(0.0, 1.0)).clamp(1e-4, 0.01);
    }
    let interp = |s: f64, lo_s: f64, hi_s: f64, lo_p: f64, hi_p: f64| {
        lo_p + (s - lo_s) / (hi_s - lo_s) * (hi_p - lo_p)
    };
    if stat <= c5 {
        return interp(stat, c1, c5, 0.01, 0.05);
    }
    if stat <= c10 {
        return interp(stat, c5, c10, 0.05, 0.10);
    }
    // above 10% critical: p rises toward ~1 as stat → 0+
    interp(stat, c10, 0.0, 0.10, 0.90).clamp(0.10, 0.999)
}

/// Augmented Dickey-Fuller test (constant, no trend). Regresses
/// Δy_t = α + γ·y_{t-1} + Σ δ_i Δy_{t-i} + ε; the ADF statistic is the t-stat on γ.
pub fn test(series: &[f64], max_lag: usize) -> AdfEstimate {
    let n = series.len();
    // need enough points after differencing + lags
    if n < max_lag + 4 {
        return AdfEstimate {
            statistic: 0.0,
            used_lag: max_lag,
            n_obs: 0,
            crit_1pct: -3.43,
            crit_5pct: -2.86,
            crit_10pct: -2.57,
            p_value_approx: 1.0,
            stationary_1pct: false,
            stationary_5pct: false,
            stationary_10pct: false,
        };
    }
    let dy: Vec<f64> = (1..n).map(|i| series[i] - series[i - 1]).collect();
    // build design: rows from t=max_lag .. dy.len()-1
    let start = max_lag;
    let mut xrows: Vec<Vec<f64>> = vec![];
    let mut yvec: Vec<f64> = vec![];
    for t in start..dy.len() {
        let mut row = vec![1.0, series[t]]; // intercept, y_{t-1} (series index t aligns with dy[t]=series[t+1]-series[t])
        for i in 1..=max_lag {
            row.push(dy[t - i]);
        }
        xrows.push(row);
        yvec.push(dy[t]);
    }
    let (stat, n_obs) = match ols_with_se(&xrows, &yvec) {
        Some((coefs, ses, _)) => {
            // coefs[1] is γ on y_{t-1}; t-stat = γ / se(γ)
            let gamma = coefs[1];
            let se = ses[1];
            let t = if se.abs() > 1e-18 { gamma / se } else { 0.0 };
            (t, yvec.len())
        }
        None => (0.0, 0),
    };
    let t = n_obs.max(1) as f64;
    let c1 = mackinnon_crit(1, t);
    let c5 = mackinnon_crit(5, t);
    let c10 = mackinnon_crit(10, t);
    AdfEstimate {
        statistic: stat,
        used_lag: max_lag,
        n_obs,
        crit_1pct: c1,
        crit_5pct: c5,
        crit_10pct: c10,
        p_value_approx: adf_pvalue(stat, c1, c5, c10),
        stationary_1pct: stat < c1,
        stationary_5pct: stat < c5,
        stationary_10pct: stat < c10,
    }
}

#[cfg(test)]
mod tests {
    use super::test;

    #[test]
    fn stationary_series_has_a_more_negative_statistic_than_a_random_walk() {
        let stationary: Vec<f64> = (0..128).map(|i| libm::sin(i as f64 * 0.37) * 0.1).collect();
        let random_walk: Vec<f64> = (0..128)
            .scan(0.0, |state, i| {
                *state += libm::sin(i as f64 * 0.37) * 0.1;
                Some(*state)
            })
            .collect();
        let stationary_result = test(&stationary, 1);
        let walk_result = test(&random_walk, 1);
        assert_eq!(stationary_result.n_obs, 126);
        assert!(stationary_result.statistic < walk_result.statistic);
    }
}
