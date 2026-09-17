// CONCEPT:EG-KG.mining.arima — Classical time-series forecasting.
//
// Pure-Rust, dependency-light, batch (one round-trip): given a 1-D numeric
// series (a tsdb window handed in by the caller, mirroring `anomaly`'s
// client-supplied `values` cut — the native in-handler TsScan source is the
// same documented follow-up as Phase 2's anomaly detector), forecast the next
// `horizon` points with an approximate confidence band. Three interchangeable
// engines:
//
//   * **ARIMA(p,d,q)**   (CONCEPT:EG-KG.mining.arima) — `d`-order differencing to
//     stationarity, then AR(p)/MA(q) coefficients fit by the Hannan-Rissanen
//     two-stage method (a long auxiliary AR gives a residual proxy, then AR+MA
//     terms are jointly estimated by ordinary least squares) — hand-rolled, no
//     forecasting crate. Deterministic (closed-form OLS, no randomness).
//   * **Holt-Winters/ETS** (CONCEPT:EG-KG.mining.holt-winters) — additive
//     level/trend/seasonal exponential smoothing (`alpha`/`beta`/`gamma`,
//     seasonal `period`); degrades to Holt's linear-trend method (ETS(A,A,N))
//     when `period` is 0 or the series is shorter than two seasonal cycles.
//   * **STL decomposition** (CONCEPT:EG-KG.mining.stl-decomposition) — a
//     classical (moving-average) trend/seasonal/residual decomposition, then a
//     linear-trend + repeated-last-cycle forecast extension. A lightweight,
//     hand-rolled stand-in for full iterative Loess-STL (documented scope cut,
//     like Phase 3's approximate UMAP/t-SNE).
//
// This module is graph/tsdb-agnostic: the handler (`src/server/handlers/mining.rs`)
// supplies the series (explicit `values`, mirroring the anomaly RCA path) and does
// the KG write-back (`:Forecast{horizon, values}`).

// The linear-algebra kernels here (least-squares normal equations, the
// Gauss-elimination solver, Acklam's inverse-normal quantile) read more clearly
// with explicit `for i in 0..n { m[i][j] }` indexing than enumerate/zip rewrites.
// Scope the lint to this compute module rather than contorting the math.
#![allow(clippy::needless_range_loop)]

/// Which forecasting engine to run, with its parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Algorithm {
    /// `(p, d, q)` ARIMA order.
    Arima { p: usize, d: usize, q: usize },
    /// Additive Holt-Winters (`period == 0` ⇒ Holt's linear-trend fallback).
    HoltWinters {
        period: usize,
        alpha: f64,
        beta: f64,
        gamma: f64,
    },
    /// Classical (moving-average) STL-style decomposition + trend/seasonal
    /// extrapolation.
    Stl { period: usize },
}

/// The forecast outcome: `horizon` point forecasts with an approximate
/// `confidence`-level band, plus (for `Stl`) the fitted decomposition.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Forecast {
    pub values: Vec<f64>,
    pub lower: Vec<f64>,
    pub upper: Vec<f64>,
    /// STL only: the fitted trend component (same length as the input series).
    pub trend: Vec<f64>,
    /// STL only: the fitted seasonal component (same length as the input series).
    pub seasonal: Vec<f64>,
    /// STL only: `series - trend - seasonal` (same length as the input series).
    pub residual: Vec<f64>,
}

/// Run the chosen forecasting engine over `series`, producing `horizon` future
/// points at the given `confidence` level (e.g. `0.95`). An empty or
/// too-short series yields an empty forecast (never a panic).
pub fn forecast(series: &[f64], algorithm: Algorithm, horizon: usize, confidence: f64) -> Forecast {
    if series.is_empty() || horizon == 0 {
        return Forecast::default();
    }
    match algorithm {
        Algorithm::Arima { p, d, q } => arima(series, p, d, q, horizon, confidence),
        Algorithm::HoltWinters {
            period,
            alpha,
            beta,
            gamma,
        } => holt_winters(series, period, alpha, beta, gamma, horizon, confidence),
        Algorithm::Stl { period } => stl_forecast(series, period, horizon, confidence),
    }
}

// ─────────────────────────── forecast engines ───────────────────────────

mod arima;
mod holt_winters;

pub use arima::arima;
pub use holt_winters::holt_winters;

// ─────────────────────────── STL decomposition ───────────────────────────

/// Classical (moving-average) STL-style decomposition (CONCEPT:EG-KG.mining.stl-decomposition):
/// a centered moving-average trend, a period-averaged (mean-centered) seasonal
/// component, and the residual. `period < 2` or too little data degrades to a
/// trend-only decomposition (no seasonal).
pub fn stl_decompose(series: &[f64], period: usize) -> Forecast {
    let n = series.len();
    let trend = centered_moving_average(series, period.max(1));
    let seasonal_on = period >= 2 && n >= 2 * period;
    let seasonal = if seasonal_on {
        // Detrend, then average by phase (skipping points with no trend estimate).
        let mut sums = vec![0.0; period];
        let mut counts = vec![0usize; period];
        for t in 0..n {
            if trend[t].is_finite() {
                sums[t % period] += series[t] - trend[t];
                counts[t % period] += 1;
            }
        }
        let mut pattern: Vec<f64> = sums
            .iter()
            .zip(&counts)
            .map(|(&s, &c)| if c > 0 { s / c as f64 } else { 0.0 })
            .collect();
        let pm = mean(&pattern);
        for v in pattern.iter_mut() {
            *v -= pm;
        }
        (0..n).map(|t| pattern[t % period]).collect::<Vec<f64>>()
    } else {
        vec![0.0; n]
    };
    let residual: Vec<f64> = (0..n)
        .map(|t| {
            let tr = if trend[t].is_finite() {
                trend[t]
            } else {
                series[t] - seasonal[t]
            };
            series[t] - tr - seasonal[t]
        })
        .collect();
    // Fill trend edges (no centered-MA estimate) with the nearest valid value so
    // callers get a full-length, finite trend series.
    let mut filled_trend = trend.clone();
    fill_edges(&mut filled_trend);
    Forecast {
        values: Vec::new(),
        lower: Vec::new(),
        upper: Vec::new(),
        trend: filled_trend,
        seasonal,
        residual,
    }
}

/// STL-based forecast (CONCEPT:EG-KG.mining.stl-decomposition): decompose, linearly
/// extrapolate the trend from its last two points, and repeat the last seasonal
/// cycle.
pub fn stl_forecast(series: &[f64], period: usize, horizon: usize, confidence: f64) -> Forecast {
    let decomp = stl_decompose(series, period);
    let n = series.len();
    // Extrapolate from the last TWO VALID (pre-edge-fill) trend points, not the
    // filled (flat-extended) tail — the moving-average trend has no estimate for
    // the last `period/2` points, and `stl_decompose` flat-fills those edges for
    // callers that just want a full-length trend series; using the flat-filled
    // tail here would zero out the slope and lose the trend entirely.
    let raw_trend = centered_moving_average(series, period.max(1));
    let valid: Vec<(usize, f64)> = raw_trend
        .iter()
        .enumerate()
        .filter(|(_, v)| v.is_finite())
        .map(|(i, &v)| (i, v))
        .collect();
    let (last_trend, slope) = match valid.len() {
        0 => (*series.last().unwrap_or(&0.0), 0.0),
        1 => (valid[0].1, 0.0),
        _ => {
            let (i1, v1) = valid[valid.len() - 2];
            let (i2, v2) = valid[valid.len() - 1];
            let step = (i2 - i1) as f64;
            let s = if step > 0.0 { (v2 - v1) / step } else { 0.0 };
            // Project the last valid trend point forward to `n - 1` before
            // extending the forecast horizon from there.
            (v2 + s * (n as f64 - 1.0 - i2 as f64), s)
        }
    };
    let seasonal_on = period >= 2 && n >= 2 * period;

    let mut point = Vec::with_capacity(horizon);
    for h in 1..=horizon {
        let seasonal_h = if seasonal_on {
            decomp.seasonal[(n + h - 1) % period]
        } else {
            0.0
        };
        point.push(last_trend + h as f64 * slope + seasonal_h);
    }
    let (lower, upper) = widening_bounds(&point, std_dev(&decomp.residual), confidence);
    Forecast {
        values: point,
        lower,
        upper,
        trend: decomp.trend,
        seasonal: decomp.seasonal,
        residual: decomp.residual,
    }
}

/// Centered moving average with window `period` (the classical trend
/// estimator): odd windows are a plain centered MA; even windows use the
/// standard "2×period" symmetric MA (average of two offset period-length MAs).
/// Edge points with no full window return `f64::NAN` (filled by [`fill_edges`]).
fn centered_moving_average(series: &[f64], period: usize) -> Vec<f64> {
    let n = series.len();
    let mut out = vec![f64::NAN; n];
    if period <= 1 {
        return series.to_vec();
    }
    if period % 2 == 1 {
        let half = period / 2;
        for t in half..n.saturating_sub(half) {
            out[t] = series[t - half..=t + half].iter().sum::<f64>() / period as f64;
        }
    } else {
        let half = period / 2;
        for t in half..n.saturating_sub(half) {
            // 2×period MA: average of the two overlapping period-window means.
            let a: f64 = series[t - half..t - half + period].iter().sum::<f64>() / period as f64;
            let b: f64 = series[t - half + 1..t - half + 1 + period]
                .iter()
                .sum::<f64>()
                / period as f64;
            out[t] = (a + b) / 2.0;
        }
    }
    out
}

/// Fill leading/trailing `NAN`s with the nearest finite value (flat extrapolation).
fn fill_edges(v: &mut [f64]) {
    let n = v.len();
    if n == 0 {
        return;
    }
    let first_finite = v.iter().position(|x| x.is_finite());
    if let Some(fi) = first_finite {
        let val = v[fi];
        for x in v.iter_mut().take(fi) {
            *x = val;
        }
    }
    let last_finite = v.iter().rposition(|x| x.is_finite());
    if let Some(li) = last_finite {
        let val = v[li];
        for x in v.iter_mut().skip(li + 1) {
            *x = val;
        }
    }
}

// ─────────────────────────── shared helpers ───────────────────────────

/// A flat forecast at `last` (used by degenerate/too-short-series fallbacks).
fn flat_forecast(last: f64, horizon: usize, sigma: f64, confidence: f64) -> Forecast {
    let values = vec![last; horizon];
    let (lower, upper) = widening_bounds(&values, sigma, confidence);
    Forecast {
        values,
        lower,
        upper,
        trend: Vec::new(),
        seasonal: Vec::new(),
        residual: Vec::new(),
    }
}

fn diff_once(s: &[f64]) -> Vec<f64> {
    s.windows(2).map(|w| w[1] - w[0]).collect()
}

/// Difference `series` `d` times (each pass shortens it by one).
fn difference(series: &[f64], d: usize) -> Vec<f64> {
    let mut cur = series.to_vec();
    for _ in 0..d {
        if cur.len() < 2 {
            break;
        }
        cur = diff_once(&cur);
    }
    cur
}

/// Integrate a `d`-times-differenced forecast back to the original scale, by
/// rebuilding the chain of intermediate differenced series and cumulatively
/// summing from each level's last actual value up to the next.
fn integrate_forecast(diff_forecast: &[f64], series: &[f64], d: usize) -> Vec<f64> {
    if d == 0 {
        return diff_forecast.to_vec();
    }
    let mut levels: Vec<Vec<f64>> = Vec::with_capacity(d + 1);
    levels.push(series.to_vec());
    for i in 0..d {
        if levels[i].len() < 2 {
            break;
        }
        levels.push(diff_once(&levels[i]));
    }
    let built = levels.len() - 1; // how many differencing levels we actually have
    let mut current = diff_forecast.to_vec();
    for level in (0..built).rev() {
        let last_val = *levels[level].last().unwrap();
        let mut acc = last_val;
        let mut integrated = Vec::with_capacity(current.len());
        for &f in &current {
            acc += f;
            integrated.push(acc);
        }
        current = integrated;
    }
    current
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

/// Simple OLS line `y = a + b*x` (used for a lag-free level/trend seed —
/// unlike a first-vs-second-window mean difference, the fitted line's value at
/// any single `x` is not biased toward the center of a window).
fn linear_regression(x: &[f64], y: &[f64]) -> (f64, f64) {
    if x.len() < 2 {
        return (y.first().copied().unwrap_or(0.0), 0.0);
    }
    let mx = mean(x);
    let my = mean(y);
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..x.len() {
        num += (x[i] - mx) * (y[i] - my);
        den += (x[i] - mx) * (x[i] - mx);
    }
    let b = if den > 0.0 { num / den } else { 0.0 };
    let a = my - b * mx;
    (a, b)
}

fn std_dev(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let m = mean(v);
    (v.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / v.len() as f64).sqrt()
}

/// Residual std-dev, skipping the warm-up region `[0, warmup)` (whose residual
/// is defined as 0 by construction, which would bias the estimate downward).
fn residual_std(resid: &[f64], warmup: usize) -> f64 {
    if resid.len() <= warmup {
        return 0.0;
    }
    std_dev(&resid[warmup..])
}

/// The approximate confidence band around a forecast: `point ± z·sigma·√h` for
/// the `h`-th step ahead, so the band widens with the forecast horizon. Shared
/// by every engine; each supplies its own residual `sigma`.
fn widening_bounds(point: &[f64], sigma: f64, confidence: f64) -> (Vec<f64>, Vec<f64>) {
    let z = z_score(confidence);
    let mut lower = Vec::with_capacity(point.len());
    let mut upper = Vec::with_capacity(point.len());
    for (h, &f) in point.iter().enumerate() {
        let margin = z * sigma * ((h + 1) as f64).sqrt();
        lower.push(f - margin);
        upper.push(f + margin);
    }
    (lower, upper)
}

/// The z-multiplier for a two-sided `confidence` level (e.g. `0.95` → `1.96`),
/// via Acklam's rational approximation to the inverse standard-normal CDF
/// (accurate to ~1.15e-9, no dependency). Falls back to the common `1.96`
/// default outside `(0, 1)`.
fn z_score(confidence: f64) -> f64 {
    if !(0.0..1.0).contains(&confidence) {
        return 1.959_963_984_540_054;
    }
    let p = 0.5 + confidence / 2.0;
    inverse_normal_cdf(p)
}

/// Acklam's algorithm for the inverse standard normal CDF.
fn inverse_normal_cdf(p: f64) -> f64 {
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    // Coefficients for the rational approximations.
    const A: [f64; 6] = [
        -3.969_683_028_665_376e+01,
        2.209_460_984_245_205e+02,
        -2.759_285_104_469_687e+02,
        1.383_577_518_672_69e+02,
        -3.066_479_806_614_716e+01,
        2.506_628_277_459_239e+00,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e+01,
        1.615_858_368_580_409e+02,
        -1.556_989_798_598_866e+02,
        6.680_131_188_771_972e+01,
        -1.328_068_155_288_572e+01,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-03,
        -3.223_964_580_411_365e-01,
        -2.400_758_277_161_838e+00,
        -2.549_732_539_343_734e+00,
        4.374_664_141_464_968e+00,
        2.938_163_982_698_783e+00,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-03,
        3.224_671_290_700_398e-01,
        2.445_134_137_142_996e+00,
        3.754_408_661_907_416e+00,
    ];
    const P_LOW: f64 = 0.024_25;
    let p_high = 1.0 - P_LOW;

    if p < P_LOW {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= p_high {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic SplitMix64 PRNG — test-fixture-only, mirrors
    /// `cluster.rs`'s hand-rolled generator (kept dependency-free).
    struct SplitMix64 {
        state: u64,
    }
    impl SplitMix64 {
        fn new(seed: u64) -> Self {
            SplitMix64 {
                state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
            }
        }
        fn next_f64(&mut self) -> f64 {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (z >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    /// A linear-trend + additive-seasonal fixture with tiny deterministic
    /// jitter (fixed seed 42): `y_t = 10 + 2t + 5*sin(2*pi*t/12) + jitter`.
    fn trend_seasonal_fixture(n: usize) -> Vec<f64> {
        let mut rng = SplitMix64::new(42);
        (0..n)
            .map(|t| {
                let jitter = (rng.next_f64() - 0.5) * 0.2; // +/-0.1, tiny
                10.0 + 2.0 * t as f64
                    + 5.0 * (2.0 * std::f64::consts::PI * t as f64 / 12.0).sin()
                    + jitter
            })
            .collect()
    }

    #[test]
    fn holt_winters_forecasts_trend_and_seasonality() {
        let series = trend_seasonal_fixture(60); // 5 full cycles of 12
        let out = holt_winters(&series, 12, 0.5, 0.3, 0.3, 12, 0.95);
        assert_eq!(out.values.len(), 12);
        for h in 0..12 {
            let t = 60 + h;
            let truth =
                10.0 + 2.0 * t as f64 + 5.0 * (2.0 * std::f64::consts::PI * t as f64 / 12.0).sin();
            let err = (out.values[h] - truth).abs();
            assert!(
                err < 3.0,
                "holt-winters forecast[{h}] = {} too far from truth {truth} (err {err})",
                out.values[h]
            );
            // Confidence band must bracket the point forecast and widen with horizon.
            assert!(out.lower[h] <= out.values[h] && out.values[h] <= out.upper[h]);
        }
        assert!(out.upper[11] - out.lower[11] >= out.upper[0] - out.lower[0]);
    }

    #[test]
    fn arima_forecasts_a_stationary_ar1_process() {
        // A deterministic AR(1)-ish process built by direct recursion (phi=0.6),
        // no differencing needed (d=0) since it's already stationary around a mean.
        let mut y = vec![0.0f64; 40];
        let mut rng = SplitMix64::new(7);
        for t in 1..y.len() {
            let jitter = (rng.next_f64() - 0.5) * 0.05;
            y[t] = 0.6 * y[t - 1] + jitter;
        }
        let out = arima(&y, 1, 0, 0, 5, 0.95);
        assert_eq!(out.values.len(), 5);
        // AR(1) with |phi|<1 must decay toward 0 (the process mean) as h grows.
        assert!(out.values[4].abs() <= out.values[0].abs() + 1e-6);
        for h in 0..5 {
            assert!(out.lower[h] <= out.values[h] && out.values[h] <= out.upper[h]);
        }
    }

    #[test]
    fn arima_with_differencing_tracks_a_linear_trend() {
        // A pure linear trend (deterministic, tiny jitter) — ARIMA(1,1,0) should
        // recover it near-exactly since one difference makes it ~constant.
        let mut rng = SplitMix64::new(99);
        let series: Vec<f64> = (0..30)
            .map(|t| {
                let jitter = (rng.next_f64() - 0.5) * 0.02;
                5.0 + 3.0 * t as f64 + jitter
            })
            .collect();
        let out = arima(&series, 1, 1, 0, 5, 0.95);
        for h in 0..5 {
            let t = 30 + h;
            let truth = 5.0 + 3.0 * t as f64;
            let err = (out.values[h] - truth).abs();
            assert!(
                err < 2.0,
                "arima(1,1,0) forecast[{h}]={} truth={truth} err={err}",
                out.values[h]
            );
        }
    }

    #[test]
    fn arima_ma_branch_smoke_preserves_forecast_shape() {
        let series = vec![
            1.0, 1.8, 1.2, 2.4, 1.6, 2.9, 2.1, 3.2, 2.8, 3.7, 3.1, 4.4, 3.9, 5.0, 4.5, 5.6,
        ];
        let out = arima(&series, 1, 0, 1, 3, 0.95);
        assert_eq!(out.values.len(), 3);
        assert!(out.values.iter().all(|value| value.is_finite()));
        for h in 0..3 {
            assert!(out.lower[h] <= out.values[h] && out.values[h] <= out.upper[h]);
        }
    }

    #[test]
    fn stl_decompose_recovers_planted_seasonal_pattern() {
        let series = trend_seasonal_fixture(48); // 4 cycles of 12
        let decomp = stl_decompose(&series, 12);
        assert_eq!(decomp.seasonal.len(), 48);
        assert_eq!(decomp.trend.len(), 48);
        // The recovered seasonal pattern at phase 3 (peak-ish, sin(pi/2)=1) should
        // be noticeably larger than at phase 9 (trough-ish, sin(3pi/2)=-1).
        assert!(decomp.seasonal[3] > decomp.seasonal[9]);
        // Residuals should be small (the series is a clean trend+seasonal+tiny jitter).
        let resid_mag: f64 = decomp.residual.iter().map(|r| r.abs()).sum::<f64>() / 48.0;
        assert!(resid_mag < 1.0, "mean |residual| too large: {resid_mag}");
    }

    #[test]
    fn stl_forecast_extends_trend_and_repeats_seasonal_cycle() {
        let series = trend_seasonal_fixture(48);
        let out = stl_forecast(&series, 12, 12, 0.95);
        assert_eq!(out.values.len(), 12);
        for h in 0..12 {
            let t = 48 + h;
            let truth =
                10.0 + 2.0 * t as f64 + 5.0 * (2.0 * std::f64::consts::PI * t as f64 / 12.0).sin();
            let err = (out.values[h] - truth).abs();
            assert!(
                err < 4.0,
                "stl forecast[{h}]={} truth={truth} err={err}",
                out.values[h]
            );
        }
    }

    #[test]
    fn empty_series_yields_empty_forecast() {
        let out = forecast(&[], Algorithm::Arima { p: 1, d: 0, q: 0 }, 5, 0.95);
        assert!(out.values.is_empty());
        let out2 = forecast(
            &[1.0, 2.0],
            Algorithm::HoltWinters {
                period: 0,
                alpha: 0.3,
                beta: 0.1,
                gamma: 0.1,
            },
            0,
            0.95,
        );
        assert!(out2.values.is_empty());
    }

    #[test]
    fn z_score_matches_common_confidence_levels() {
        assert!((z_score(0.95) - 1.96).abs() < 0.01);
        assert!((z_score(0.90) - 1.645).abs() < 0.01);
        assert!((z_score(0.99) - 2.576).abs() < 0.01);
    }

    #[test]
    fn confidence_band_widens_with_horizon() {
        let series = trend_seasonal_fixture(60);
        let out = forecast(
            &series,
            Algorithm::HoltWinters {
                period: 12,
                alpha: 0.5,
                beta: 0.3,
                gamma: 0.3,
            },
            12,
            0.95,
        );
        let width0 = out.upper[0] - out.lower[0];
        let width_last = out.upper[11] - out.lower[11];
        assert!(width_last >= width0);
    }
}
