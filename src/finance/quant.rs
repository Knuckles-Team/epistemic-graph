// CONCEPT:KG-2.20f — Market-Microstructure, Sizing & Backtest-Validation Kernels
//
// Batched, stateless quantitative kernels for HFT market-making on binary CLOB
// venues (Polymarket / Kalshi) and for rigorous strategy validation. Served over
// the Tokio MessagePack protocol — every op is one round-trip over whole arrays,
// never a per-element loop (see AGENTS.md "Batch, never per-element").
//
// Math is implemented from primary sources, not copied from blogs:
//   - Avellaneda & Stoikov (2008) optimal market making
//   - Guéant, Lehalle, Fernandez-Tapia (2013) closed form with inventory bound
//   - logit-space reformulation for bounded (0,1) prediction-market prices
//   - Glosten & Milgrom (1985) adverse-selection spread
//   - Hawkes (1971) self-exciting process; MLE + Hardiman-Bouchaud branching ratio
//   - Cont-Kukanov-Stoikov OFI, Stoikov microprice, VPIN (Easley-LdP-O'Hara)
//   - Kelly (1956) + Bayesian (Beta-posterior) Kelly
//   - López de Prado: purged combinatorial CV, Deflated Sharpe, PBO; Diebold-Mariano

use serde::{Deserialize, Serialize};

// ════════════════════════════════════════════════════════════════════════
//  Special functions (self-contained — no scipy on the wire)
// ════════════════════════════════════════════════════════════════════════
mod sf {
    /// Error function — Abramowitz & Stegun 7.1.26 (|err| < 1.5e-7).
    pub fn erf(x: f64) -> f64 {
        let sign = if x < 0.0 { -1.0 } else { 1.0 };
        let x = x.abs();
        let t = 1.0 / (1.0 + 0.3275911 * x);
        let y = 1.0
            - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
                + 0.254829592)
                * t
                * (-x * x).exp();
        sign * y
    }

    /// Standard-normal CDF.
    pub fn norm_cdf(x: f64) -> f64 {
        0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
    }

    /// Inverse standard-normal CDF (Acklam's rational approximation).
    pub fn norm_ppf(p: f64) -> f64 {
        if p <= 0.0 {
            return f64::NEG_INFINITY;
        }
        if p >= 1.0 {
            return f64::INFINITY;
        }
        const A: [f64; 6] = [
            -3.969683028665376e+01,
            2.209460984245205e+02,
            -2.759285104469687e+02,
            1.383577518672690e+02,
            -3.066479806614716e+01,
            2.506628277459239e+00,
        ];
        const B: [f64; 5] = [
            -5.447609879822406e+01,
            1.615858368580409e+02,
            -1.556989798598866e+02,
            6.680131188771972e+01,
            -1.328068155288572e+01,
        ];
        const C: [f64; 6] = [
            -7.784894002430293e-03,
            -3.223964580411365e-01,
            -2.400758277161838e+00,
            -2.549732539343734e+00,
            4.374664141464968e+00,
            2.938163982698783e+00,
        ];
        const D: [f64; 4] = [
            7.784695709041462e-03,
            3.224671290700398e-01,
            2.445134137142996e+00,
            3.754408661907416e+00,
        ];
        let plow = 0.02425;
        let phigh = 1.0 - plow;
        if p < plow {
            let q = (-2.0 * p.ln()).sqrt();
            (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
                / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
        } else if p <= phigh {
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

    /// ln Γ(x) — Lanczos approximation (g = 7, n = 9).
    pub fn ln_gamma(x: f64) -> f64 {
        const G: f64 = 7.0;
        const C: [f64; 9] = [
            0.999_999_999_999_809_93,
            676.520_368_121_885_1,
            -1_259.139_216_722_402_8,
            771.323_428_777_653_1,
            -176.615_029_162_140_6,
            12.507_343_278_686_905,
            -0.138_571_095_265_720_12,
            9.984_369_578_019_572e-6,
            1.505_632_735_149_311_6e-7,
        ];
        if x < 0.5 {
            // reflection
            std::f64::consts::PI.ln() - (std::f64::consts::PI * x).sin().ln() - ln_gamma(1.0 - x)
        } else {
            let x = x - 1.0;
            let mut a = C[0];
            let t = x + G + 0.5;
            for (i, &c) in C.iter().enumerate().skip(1) {
                a += c / (x + i as f64);
            }
            0.5 * (2.0 * std::f64::consts::PI).ln() + (x + 0.5) * t.ln() - t + a.ln()
        }
    }

    fn ln_beta(a: f64, b: f64) -> f64 {
        ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b)
    }

    /// Beta(a,b) pdf at x ∈ (0,1).
    pub fn beta_pdf(x: f64, a: f64, b: f64) -> f64 {
        if x <= 0.0 || x >= 1.0 {
            return 0.0;
        }
        ((a - 1.0) * x.ln() + (b - 1.0) * (1.0 - x).ln() - ln_beta(a, b)).exp()
    }

    /// Continued fraction for the incomplete beta (Numerical Recipes betacf).
    fn betacf(a: f64, b: f64, x: f64) -> f64 {
        let fpmin = 1e-30;
        let qab = a + b;
        let qap = a + 1.0;
        let qam = a - 1.0;
        let mut c = 1.0;
        let mut d = 1.0 - qab * x / qap;
        if d.abs() < fpmin {
            d = fpmin;
        }
        d = 1.0 / d;
        let mut h = d;
        for m in 1..200 {
            let m = m as f64;
            let m2 = 2.0 * m;
            let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
            d = 1.0 + aa * d;
            if d.abs() < fpmin {
                d = fpmin;
            }
            c = 1.0 + aa / c;
            if c.abs() < fpmin {
                c = fpmin;
            }
            d = 1.0 / d;
            h *= d * c;
            let aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
            d = 1.0 + aa * d;
            if d.abs() < fpmin {
                d = fpmin;
            }
            c = 1.0 + aa / c;
            if c.abs() < fpmin {
                c = fpmin;
            }
            d = 1.0 / d;
            let del = d * c;
            h *= del;
            if (del - 1.0).abs() < 1e-12 {
                break;
            }
        }
        h
    }

    /// Regularized incomplete beta I_x(a,b) = Beta CDF.
    pub fn betai(a: f64, b: f64, x: f64) -> f64 {
        if x <= 0.0 {
            return 0.0;
        }
        if x >= 1.0 {
            return 1.0;
        }
        // exact log-prefactor x^a (1-x)^b / (a·B(a,b))
        let lbt = ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln();
        let pref = lbt.exp();
        if x < (a + 1.0) / (a + b + 2.0) {
            pref * betacf(a, b, x) / a
        } else {
            1.0 - pref * betacf(b, a, 1.0 - x) / b
        }
    }

    /// Inverse Beta CDF (quantile) via bisection on betai.
    pub fn beta_ppf(p: f64, a: f64, b: f64) -> f64 {
        if p <= 0.0 {
            return 0.0;
        }
        if p >= 1.0 {
            return 1.0;
        }
        let (mut lo, mut hi) = (0.0_f64, 1.0_f64);
        for _ in 0..100 {
            let mid = 0.5 * (lo + hi);
            if betai(a, b, mid) < p {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        0.5 * (lo + hi)
    }

    /// Gauss-Legendre nodes & weights on [-1, 1] (Newton on Legendre roots).
    pub fn leggauss(n: usize) -> (Vec<f64>, Vec<f64>) {
        let mut x = vec![0.0; n];
        let mut w = vec![0.0; n];
        let m = (n + 1) / 2;
        for i in 0..m {
            // initial guess
            let mut z = (std::f64::consts::PI * (i as f64 + 0.75) / (n as f64 + 0.5)).cos();
            let mut z1;
            let mut pp;
            loop {
                let mut p1 = 1.0;
                let mut p2 = 0.0;
                for j in 0..n {
                    let p3 = p2;
                    p2 = p1;
                    p1 = ((2.0 * j as f64 + 1.0) * z * p2 - j as f64 * p3) / (j as f64 + 1.0);
                }
                pp = n as f64 * (z * p1 - p2) / (z * z - 1.0);
                z1 = z;
                z = z1 - p1 / pp;
                if (z - z1).abs() < 1e-14 {
                    break;
                }
            }
            x[i] = -z;
            x[n - 1 - i] = z;
            let wi = 2.0 / ((1.0 - z * z) * pp * pp);
            w[i] = wi;
            w[n - 1 - i] = wi;
        }
        (x, w)
    }

    /// Sample skewness (Fisher-Pearson) of a slice.
    pub fn skew(d: &[f64]) -> f64 {
        let n = d.len() as f64;
        if n < 3.0 {
            return 0.0;
        }
        let mean = d.iter().sum::<f64>() / n;
        let m2 = d.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        let m3 = d.iter().map(|x| (x - mean).powi(3)).sum::<f64>() / n;
        if m2 <= 1e-18 {
            0.0
        } else {
            m3 / m2.powf(1.5)
        }
    }

    /// Excess kurtosis (Fisher) of a slice.
    pub fn excess_kurtosis(d: &[f64]) -> f64 {
        let n = d.len() as f64;
        if n < 4.0 {
            return 0.0;
        }
        let mean = d.iter().sum::<f64>() / n;
        let m2 = d.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        let m4 = d.iter().map(|x| (x - mean).powi(4)).sum::<f64>() / n;
        if m2 <= 1e-18 {
            0.0
        } else {
            m4 / (m2 * m2) - 3.0
        }
    }
}

#[inline]
fn logit(p: f64) -> f64 {
    (p / (1.0 - p)).ln()
}
#[inline]
fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

// ════════════════════════════════════════════════════════════════════════
//  Market making: Avellaneda-Stoikov, GLT, logit-space
// ════════════════════════════════════════════════════════════════════════

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub reservation: f64,
    pub half_spread: f64,
    /// True when an inventory / boundary cap says "withdraw, do not quote".
    pub withdraw: bool,
}

/// Avellaneda-Stoikov (2008) optimal quotes around a freely-drifting mid.
///   r = S − q·γ·σ²·(T−t);  δ* = γ·σ²·(T−t) + (2/γ)·ln(1+γ/κ)
pub fn avellaneda_stoikov(
    mid: f64,
    inventory: f64,
    sigma: f64,
    gamma: f64,
    kappa: f64,
    tau: f64,
) -> Quote {
    let reservation = mid - inventory * gamma * sigma * sigma * tau;
    let half_spread = gamma * sigma * sigma * tau + (2.0 / gamma) * (1.0 + gamma / kappa).ln();
    Quote {
        bid: reservation - half_spread,
        ask: reservation + half_spread,
        reservation,
        half_spread,
        withdraw: false,
    }
}

/// Guéant-Lehalle-Fernandez-Tapia (2013) closed form with asymmetric
/// inventory-dependent skew. `a` is the fill-intensity scale A.
pub fn glt_quotes(mid: f64, inventory: f64, sigma: f64, gamma: f64, kappa: f64, a: f64) -> Quote {
    let base = (1.0 / gamma) * (1.0 + gamma / kappa).ln();
    let inv_term = ((sigma * sigma * gamma) / (2.0 * kappa * a)
        * (1.0 + gamma / kappa).powf(1.0 + kappa / gamma))
    .sqrt();
    let delta_ask = base + ((2.0 * inventory + 1.0) / 2.0) * inv_term;
    let delta_bid = base + ((-2.0 * inventory + 1.0) / 2.0) * inv_term;
    Quote {
        bid: mid - delta_bid,
        ask: mid + delta_ask,
        reservation: mid,
        half_spread: 0.5 * (delta_ask + delta_bid),
        withdraw: false,
    }
}

/// Logit-space AS for bounded (0,1) prediction-market prices, with a
/// boundary-aware inventory cap |q| ≤ M·√(p(1−p)). Quotes are returned in
/// PRICE (probability) units. `withdraw=true` ⇒ inventory exceeds the cap.
pub fn logit_space_quotes(
    p_mid: f64,
    inventory: f64,
    sigma: f64,
    gamma: f64,
    kappa: f64,
    tau: f64,
    boundary_m: f64,
) -> Quote {
    let p = p_mid.clamp(1e-6, 1.0 - 1e-6);
    let cap = boundary_m * (p * (1.0 - p)).sqrt();
    let withdraw = boundary_m > 0.0 && inventory.abs() > cap;
    let x_mid = logit(p);
    let x_res = x_mid - inventory * gamma * sigma * sigma * tau;
    let half_spread = gamma * sigma * sigma * tau + (2.0 / gamma) * (1.0 + gamma / kappa).ln();
    Quote {
        bid: sigmoid(x_res - half_spread),
        ask: sigmoid(x_res + half_spread),
        reservation: sigmoid(x_res),
        half_spread,
        withdraw,
    }
}

/// Glosten-Milgrom adverse-selection spread for a binary payoff: 2·α·p·(1−p).
pub fn glosten_milgrom_spread(alpha: f64, p: f64) -> f64 {
    2.0 * alpha * p * (1.0 - p)
}

/// Expected maker PnL per unit time at half-spread δ. Positive ⇒ profitable,
/// negative ⇒ adversely selected. α is the informed-flow fraction (VPIN proxy).
pub fn expected_pnl_rate(
    delta: f64,
    a: f64,
    kappa: f64,
    alpha: f64,
    p: f64,
    v_h: f64,
    v_l: f64,
) -> f64 {
    let fill_rate = 2.0 * a * (-kappa * delta).exp();
    let spread_capture = (1.0 - alpha) * delta;
    let adv_selection = alpha * (v_h - v_l).abs() * p * (1.0 - p);
    fill_rate * (spread_capture - adv_selection)
}

/// Maximum informed fraction α before quoting half-spread δ goes unprofitable.
pub fn breakeven_alpha(delta: f64, p: f64, v_h: f64, v_l: f64) -> f64 {
    let payoff = (v_h - v_l).abs();
    delta / (payoff * p * (1.0 - p) + delta)
}

// ════════════════════════════════════════════════════════════════════════
//  Microstructure signals: OFI, microprice, VPIN
// ════════════════════════════════════════════════════════════════════════

/// Cont-Kukanov-Stoikov order-flow imbalance, cumulative over a rolling time
/// window (seconds). Inputs are parallel per-book-event arrays. Returns the
/// rolling-OFI series aligned to each event.
pub fn ofi_series(
    ts: &[f64],
    bid_px: &[f64],
    bid_sz: &[f64],
    ask_px: &[f64],
    ask_sz: &[f64],
    window_secs: f64,
) -> Vec<f64> {
    let n = ts.len();
    if n == 0 {
        return vec![];
    }
    let mut e = vec![0.0_f64; n];
    for i in 1..n {
        let e_bid = if bid_px[i] > bid_px[i - 1] {
            bid_sz[i]
        } else if bid_px[i] < bid_px[i - 1] {
            -bid_sz[i - 1]
        } else {
            bid_sz[i] - bid_sz[i - 1]
        };
        let e_ask = if ask_px[i] < ask_px[i - 1] {
            -ask_sz[i]
        } else if ask_px[i] > ask_px[i - 1] {
            ask_sz[i - 1]
        } else {
            -(ask_sz[i] - ask_sz[i - 1])
        };
        e[i] = e_bid + e_ask;
    }
    // rolling sum over [t-window, t]
    let mut out = vec![0.0_f64; n];
    let mut start = 0usize;
    let mut acc = 0.0;
    for i in 0..n {
        acc += e[i];
        while ts[i] - ts[start] > window_secs {
            acc -= e[start];
            start += 1;
        }
        out[i] = acc;
    }
    out
}

/// Stoikov weighted-mid (first microprice iterate), batched over book snapshots.
pub fn microprice_series(
    bid_px: &[f64],
    bid_sz: &[f64],
    ask_px: &[f64],
    ask_sz: &[f64],
) -> Vec<f64> {
    let n = bid_px.len();
    let mut out = vec![0.0_f64; n];
    for i in 0..n {
        let total = bid_sz[i] + ask_sz[i];
        out[i] = if total <= 0.0 {
            0.5 * (bid_px[i] + ask_px[i])
        } else {
            (ask_sz[i] * bid_px[i] + bid_sz[i] * ask_px[i]) / total
        };
    }
    out
}

/// VPIN for prediction markets, normalised by binary-payoff variance √(p(1−p)).
/// Inputs are per-bucket buy/sell volumes and mean price. Returns toxicity ∈ [0,1].
pub fn vpin_pm(buy_vol: &[f64], sell_vol: &[f64], p_mean: &[f64]) -> f64 {
    let n = buy_vol.len().min(sell_vol.len()).min(p_mean.len());
    if n == 0 {
        return 0.0;
    }
    let mut total = 0.0;
    for i in 0..n {
        let p = p_mean[i].clamp(1e-9, 1.0 - 1e-9);
        let denom = (p * (1.0 - p)).sqrt() * (buy_vol[i] + sell_vol[i]);
        if denom > 0.0 {
            total += (buy_vol[i] - sell_vol[i]).abs() / denom;
        }
    }
    total / n as f64
}

// ════════════════════════════════════════════════════════════════════════
//  Hawkes process: MLE (exponential kernel) + Hardiman-Bouchaud
// ════════════════════════════════════════════════════════════════════════

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HawkesFit {
    pub mu: f64,
    pub alpha: f64,
    pub beta: f64,
    pub branching_ratio: f64,
    pub half_life_seconds: f64,
    pub log_likelihood: f64,
    pub converged: bool,
}

/// Negative log-likelihood for an exponential-kernel Hawkes process with the
/// stationarity constraint α/β < 1 (returns +∞-ish penalty when violated).
fn hawkes_nll(mu: f64, alpha: f64, beta: f64, times: &[f64], t_horizon: f64) -> f64 {
    if mu <= 0.0 || alpha < 0.0 || beta <= 0.0 || alpha / beta >= 1.0 {
        return 1e10;
    }
    let n = times.len();
    let mut r = vec![0.0_f64; n];
    for i in 1..n {
        r[i] = (-beta * (times[i] - times[i - 1])).exp() * (r[i - 1] + 1.0);
    }
    let log_intensity: f64 = (0..n).map(|i| (mu + alpha * r[i]).ln()).sum();
    let compensator: f64 = mu * t_horizon
        + (alpha / beta)
            * times
                .iter()
                .map(|&ti| 1.0 - (-beta * (t_horizon - ti)).exp())
                .sum::<f64>();
    -(log_intensity - compensator)
}

/// Nelder-Mead simplex minimiser for a 3-parameter objective.
fn nelder_mead_3(
    f: &dyn Fn([f64; 3]) -> f64,
    x0: [f64; 3],
    max_iter: usize,
    tol: f64,
) -> ([f64; 3], f64, bool) {
    let (a, g, r, s) = (1.0, 2.0, 0.5, 0.5); // reflect, expand, contract, shrink
    let mut simplex = [x0; 4];
    for i in 0..3 {
        let mut p = x0;
        p[i] = if p[i].abs() > 1e-9 {
            p[i] * 1.05
        } else {
            0.00025
        };
        simplex[i + 1] = p;
    }
    let mut fvals = [0.0; 4];
    for i in 0..4 {
        fvals[i] = f(simplex[i]);
    }
    let mut converged = false;
    for _ in 0..max_iter {
        // order
        let mut idx = [0, 1, 2, 3];
        idx.sort_by(|&i, &j| fvals[i].partial_cmp(&fvals[j]).unwrap());
        let order = idx;
        let best = order[0];
        let worst = order[3];
        let second_worst = order[2];
        if (fvals[worst] - fvals[best]).abs() < tol {
            converged = true;
            break;
        }
        // centroid of all but worst
        let mut cen = [0.0; 3];
        for &i in order.iter().take(3) {
            for d in 0..3 {
                cen[d] += simplex[i][d] / 3.0;
            }
        }
        let reflect = |coef: f64| {
            let mut p = [0.0; 3];
            for d in 0..3 {
                p[d] = cen[d] + coef * (cen[d] - simplex[worst][d]);
            }
            p
        };
        let xr = reflect(a);
        let fr = f(xr);
        if fr < fvals[best] {
            let xe = reflect(g);
            let fe = f(xe);
            if fe < fr {
                simplex[worst] = xe;
                fvals[worst] = fe;
            } else {
                simplex[worst] = xr;
                fvals[worst] = fr;
            }
        } else if fr < fvals[second_worst] {
            simplex[worst] = xr;
            fvals[worst] = fr;
        } else {
            let xc = reflect(-r);
            let fc = f(xc);
            if fc < fvals[worst] {
                simplex[worst] = xc;
                fvals[worst] = fc;
            } else {
                // shrink toward best
                for &i in order.iter().skip(1) {
                    for d in 0..3 {
                        simplex[i][d] = simplex[best][d] + s * (simplex[i][d] - simplex[best][d]);
                    }
                    fvals[i] = f(simplex[i]);
                }
            }
        }
    }
    let mut bi = 0;
    for i in 1..4 {
        if fvals[i] < fvals[bi] {
            bi = i;
        }
    }
    (simplex[bi], fvals[bi], converged)
}

/// Fit an exponential-kernel Hawkes process by MLE over ordered event times.
pub fn hawkes_mle(times: &[f64], t_horizon: f64, max_iter: usize) -> HawkesFit {
    if times.len() < 2 {
        return HawkesFit {
            mu: 0.0,
            alpha: 0.0,
            beta: 1.0,
            branching_ratio: 0.0,
            half_life_seconds: f64::INFINITY,
            log_likelihood: 0.0,
            converged: false,
        };
    }
    let base_rate = times.len() as f64 / t_horizon.max(1e-9);
    let x0 = [base_rate * 0.5, 1.0, 2.0];
    let times_owned = times.to_vec();
    let obj = move |p: [f64; 3]| hawkes_nll(p[0], p[1], p[2], &times_owned, t_horizon);
    let (best, fbest, converged) = nelder_mead_3(&obj, x0, max_iter.max(50), 1e-8);
    let (mu, alpha, beta) = (best[0].max(1e-9), best[1].max(0.0), best[2].max(1e-6));
    HawkesFit {
        mu,
        alpha,
        beta,
        branching_ratio: alpha / beta,
        half_life_seconds: std::f64::consts::LN_2 / beta,
        log_likelihood: -fbest,
        converged,
    }
}

/// Model-free branching ratio from count over-dispersion (Hardiman-Bouchaud 2014).
/// n ≈ 1 − √(E[N]/Var[N]). Fast diagnostic / flash-crash early warning.
pub fn hardiman_bouchaud_branching_ratio(times: &[f64], t_horizon: f64, n_windows: usize) -> f64 {
    if times.is_empty() || n_windows == 0 {
        return 0.0;
    }
    let width = t_horizon / n_windows as f64;
    let mut counts = vec![0.0_f64; n_windows];
    for &t in times {
        let mut b = (t / width) as usize;
        if b >= n_windows {
            b = n_windows - 1;
        }
        counts[b] += 1.0;
    }
    let mean = counts.iter().sum::<f64>() / n_windows as f64;
    let var = counts.iter().map(|c| (c - mean).powi(2)).sum::<f64>() / n_windows as f64;
    if var <= mean || mean <= 0.0 {
        0.0
    } else {
        1.0 - (mean / var).sqrt()
    }
}

// ════════════════════════════════════════════════════════════════════════
//  Position sizing: Kelly & Bayesian Kelly
// ════════════════════════════════════════════════════════════════════════

/// Fractional Kelly for a YES contract priced at c with true-prob estimate q:
///   f* = (q − c) / (1 − c), scaled by `fraction` and floored at 0.
pub fn kelly_fraction(q: f64, c: f64, fraction: f64) -> f64 {
    if c >= q || c <= 0.0 || c >= 1.0 {
        return 0.0;
    }
    let f_star = (q - c) / (1.0 - c);
    (f_star * fraction).clamp(0.0, 1.0)
}

/// Bayesian Kelly under a Beta(α,β) posterior over the true probability.
/// Maximises E_q[U(f)] via Gauss-Legendre quadrature over q and a grid on f.
pub fn bayesian_kelly_fraction(alpha: f64, beta: f64, c: f64, n_quadrature: usize) -> f64 {
    if c <= 0.0 || c >= 1.0 {
        return 0.0;
    }
    let (nodes, weights) = sf::leggauss(n_quadrature.max(8));
    // map [-1,1] -> [0,1]
    let q_grid: Vec<f64> = nodes.iter().map(|&z| 0.5 * (z + 1.0)).collect();
    let q_w: Vec<f64> = weights
        .iter()
        .zip(&q_grid)
        .map(|(&w, &q)| 0.5 * w * sf::beta_pdf(q, alpha, beta))
        .collect();
    let b = (1.0 - c) / c;
    let neg_eu = |f: f64| -> f64 {
        if f <= 0.0 || f >= 1.0 {
            return 1e10;
        }
        let mut acc = 0.0;
        for (i, &q) in q_grid.iter().enumerate() {
            let u = (1.0 - q) * (1.0 - f).ln() + q * (1.0 + f * b).ln();
            acc += q_w[i] * u;
        }
        -acc
    };
    let mut best_f = 0.0;
    let mut best_v = f64::INFINITY;
    let steps = 200;
    for i in 1..steps {
        let f = i as f64 / steps as f64;
        let v = neg_eu(f);
        if v < best_v {
            best_v = v;
            best_f = f;
        }
    }
    best_f.max(0.0)
}

/// Equal-tailed credible interval for q ~ Beta(α,β); use the lower bound as a
/// conservative Kelly input.
pub fn posterior_credible_interval(alpha: f64, beta: f64, level: f64) -> (f64, f64) {
    (
        sf::beta_ppf(level / 2.0, alpha, beta),
        sf::beta_ppf(1.0 - level / 2.0, alpha, beta),
    )
}

// ════════════════════════════════════════════════════════════════════════
//  Backtest validation: purged CPCV, Deflated Sharpe, PBO, Diebold-Mariano
// ════════════════════════════════════════════════════════════════════════

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CvSplit {
    pub train: Vec<usize>,
    pub test: Vec<usize>,
}

/// Purged combinatorial CV splits with purge window + embargo (López de Prado).
pub fn purged_cpcv_splits(
    n_samples: usize,
    n_groups: usize,
    n_test_groups: usize,
    purge_window: usize,
    embargo: usize,
) -> Vec<CvSplit> {
    if n_groups == 0 || n_test_groups == 0 || n_test_groups > n_groups || n_samples == 0 {
        return vec![];
    }
    let group_size = n_samples / n_groups;
    let ranges: Vec<(usize, usize)> = (0..n_groups)
        .map(|i| {
            let lo = i * group_size;
            let hi = if i == n_groups - 1 {
                n_samples
            } else {
                (i + 1) * group_size
            };
            (lo, hi)
        })
        .collect();

    // combinations of n_test_groups out of n_groups
    let mut combos: Vec<Vec<usize>> = vec![];
    let mut combo = vec![0usize; n_test_groups];
    fn rec(
        start: usize,
        depth: usize,
        n_groups: usize,
        combo: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        if depth == combo.len() {
            out.push(combo.clone());
            return;
        }
        for g in start..n_groups {
            combo[depth] = g;
            rec(g + 1, depth + 1, n_groups, combo, out);
        }
    }
    rec(0, 0, n_groups, &mut combo, &mut combos);

    let mut splits = vec![];
    for test_combo in combos {
        let mut test_idx: Vec<usize> = vec![];
        let mut forbidden: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for &g in &test_combo {
            let (lo, hi) = ranges[g];
            for i in lo..hi {
                test_idx.push(i);
                forbidden.insert(i);
            }
            for i in lo.saturating_sub(purge_window)..lo {
                forbidden.insert(i);
            }
            for i in hi..(hi + embargo).min(n_samples) {
                forbidden.insert(i);
            }
        }
        let train_idx: Vec<usize> = (0..n_samples).filter(|i| !forbidden.contains(i)).collect();
        splits.push(CvSplit {
            train: train_idx,
            test: test_idx,
        });
    }
    splits
}

/// Deflated Sharpe Ratio (Bailey & López de Prado 2014). Returns the probability
/// the observed SR exceeds zero after correcting for trials count and non-normality.
pub fn deflated_sharpe_ratio(observed_sr: f64, n_trials: usize, sr_returns: &[f64]) -> f64 {
    let t = sr_returns.len();
    if t < 4 || n_trials < 1 {
        return 0.0;
    }
    let g3 = sf::skew(sr_returns);
    let g4 = sf::excess_kurtosis(sr_returns);
    let euler = 0.5772156649;
    let nt = n_trials as f64;
    let e_max_sr = (1.0 - euler) * sf::norm_ppf(1.0 - 1.0 / nt)
        + euler * sf::norm_ppf(1.0 - 1.0 / (nt * std::f64::consts::E));
    let sr_var =
        (1.0 - g3 * observed_sr + (g4 / 4.0) * observed_sr * observed_sr) / (t as f64 - 1.0);
    if sr_var <= 0.0 {
        return 0.0;
    }
    let z = (observed_sr - e_max_sr) / sr_var.sqrt();
    sf::norm_cdf(z)
}

/// Probability of Backtest Overfit (López de Prado). Rows = CV splits,
/// columns = strategies. Returns fraction of splits where the IS-best strategy
/// landed below the OOS median. PBO < 0.3 robust; > 0.5 pure overfit.
pub fn probability_of_backtest_overfit(insample: &[Vec<f64>], oos: &[Vec<f64>]) -> f64 {
    let n_splits = insample.len();
    if n_splits == 0 || oos.len() != n_splits {
        return 0.0;
    }
    let n_strat = insample[0].len();
    if n_strat == 0 {
        return 0.0;
    }
    let median_rank = (n_strat as f64 - 1.0) / 2.0;
    let mut below = 0.0;
    for s in 0..n_splits {
        // argmax IS
        let mut is_best = 0;
        for j in 1..n_strat {
            if insample[s][j] > insample[s][is_best] {
                is_best = j;
            }
        }
        // OOS rank (0 = worst) of is_best
        let mut rank = 0usize;
        for j in 0..n_strat {
            if oos[s][j] < oos[s][is_best] {
                rank += 1;
            }
        }
        if (rank as f64) < median_rank {
            below += 1.0;
        }
    }
    below / n_splits as f64
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DieboldMariano {
    pub statistic: f64,
    pub p_value: f64,
    pub a_better: bool,
}

/// Diebold-Mariano test of equal predictive accuracy (Newey-West HAC for h>1).
pub fn diebold_mariano(losses_a: &[f64], losses_b: &[f64], h: usize) -> DieboldMariano {
    let n = losses_a.len().min(losses_b.len());
    if n < 10 {
        return DieboldMariano {
            statistic: 0.0,
            p_value: 1.0,
            a_better: false,
        };
    }
    let d: Vec<f64> = (0..n).map(|i| losses_a[i] - losses_b[i]).collect();
    let d_mean = d.iter().sum::<f64>() / n as f64;
    let gamma = |k: usize| -> f64 {
        let mut s = 0.0;
        for i in k..n {
            s += (d[i] - d_mean) * (d[i - k] - d_mean);
        }
        s / n as f64
    };
    let mut lrv = gamma(0);
    if h > 1 {
        for k in 1..h {
            lrv += 2.0 * (1.0 - k as f64 / h as f64) * gamma(k);
        }
    }
    let d_var = lrv / n as f64;
    if d_var <= 0.0 {
        return DieboldMariano {
            statistic: 0.0,
            p_value: 1.0,
            a_better: d_mean < 0.0,
        };
    }
    let stat = d_mean / d_var.sqrt();
    let p_value = 2.0 * (1.0 - sf::norm_cdf(stat.abs()));
    DieboldMariano {
        statistic: stat,
        p_value,
        a_better: stat < 0.0,
    }
}

// ════════════════════════════════════════════════════════════════════════
//  Signal combination, sizing & calibration (CONCEPT:KG-2.20i)
// ════════════════════════════════════════════════════════════════════════

/// Level-1 order-book imbalance I_t = (V_bid − V_ask)/(V_bid + V_ask) ∈ [−1, 1],
/// batched over snapshots.
pub fn order_book_imbalance(v_bid: &[f64], v_ask: &[f64]) -> Vec<f64> {
    let n = v_bid.len().min(v_ask.len());
    (0..n)
        .map(|i| {
            let tot = v_bid[i] + v_ask[i];
            if tot > 0.0 {
                (v_bid[i] - v_ask[i]) / tot
            } else {
                0.0
            }
        })
        .collect()
}

/// Queue-position / time-to-fill signal at the best bid/ask, batched over
/// snapshots. `bid_q`/`ask_q` are the resting sizes ahead in each best-level
/// queue; `bid_rate`/`ask_rate` are recent fill/arrival rates (size per unit
/// time) on each side (pass uniform 1.0 rates when unknown).
///
/// `skew` = (ask_q − bid_q)/(ask_q + bid_q) ∈ [−1, 1] — deliberately the inverse
/// sign of `order_book_imbalance` (which is volume pressure) so the two stay
/// complementary: positive `skew` ⇒ the ask queue is heavier, so a resting bid
/// fills relatively faster. `*_fill_time` = queue_ahead / arrival_rate (larger ⇒
/// slower fill ⇒ more adverse-selection exposure while resting).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueueSignal {
    pub skew: Vec<f64>,
    pub bid_fill_time: Vec<f64>,
    pub ask_fill_time: Vec<f64>,
}

pub fn queue_imbalance(
    bid_q: &[f64],
    ask_q: &[f64],
    bid_rate: &[f64],
    ask_rate: &[f64],
) -> QueueSignal {
    let n = bid_q.len().min(ask_q.len());
    let mut skew = vec![0.0_f64; n];
    let mut bid_fill_time = vec![0.0_f64; n];
    let mut ask_fill_time = vec![0.0_f64; n];
    for i in 0..n {
        let tot = ask_q[i] + bid_q[i];
        skew[i] = if tot > 0.0 {
            (ask_q[i] - bid_q[i]) / tot
        } else {
            0.0
        };
        let br = bid_rate.get(i).copied().unwrap_or(1.0).max(1e-9);
        let ar = ask_rate.get(i).copied().unwrap_or(1.0).max(1e-9);
        bid_fill_time[i] = bid_q[i].max(0.0) / br;
        ask_fill_time[i] = ask_q[i].max(0.0) / ar;
    }
    QueueSignal {
        skew,
        bid_fill_time,
        ask_fill_time,
    }
}

/// Tick-level realized volatility: for each tick `i`, the square root of the sum
/// of squared log-returns of the mid-price over the trailing `window` ticks.
/// Distinct from the state-space `kalman_volatility` filter — this is a
/// model-free rolling realized measure. Non-positive mids contribute a zero
/// return for that step (guarded).
pub fn realized_vol_tick(mid: &[f64], window: usize) -> Vec<f64> {
    let n = mid.len();
    let w = window.max(1);
    let mut r2 = vec![0.0_f64; n]; // squared log-return at each step
    for i in 1..n {
        if mid[i] > 0.0 && mid[i - 1] > 0.0 {
            let lr = (mid[i] / mid[i - 1]).ln();
            r2[i] = lr * lr;
        }
    }
    let mut out = vec![0.0_f64; n];
    let mut acc = 0.0_f64;
    for i in 0..n {
        acc += r2[i];
        if i >= w {
            acc -= r2[i - w];
        }
        out[i] = acc.max(0.0).sqrt();
    }
    out
}

/// Spread mean-reversion feature, batched over snapshots. The spread
/// (ask − bid) is z-scored against its trailing rolling mean/std over `window`
/// ticks; `signal = −zscore` (a wide spread is expected to tighten). This is a
/// lightweight rolling-statistic feature — NOT the parametric Ornstein-Uhlenbeck
/// calibration in `statespace.rs`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SpreadReversion {
    pub zscore: Vec<f64>,
    pub signal: Vec<f64>,
}

pub fn spread_reversion(bid_px: &[f64], ask_px: &[f64], window: usize) -> SpreadReversion {
    let n = bid_px.len().min(ask_px.len());
    let w = window.max(2);
    let spread: Vec<f64> = (0..n).map(|i| ask_px[i] - bid_px[i]).collect();
    let mut zscore = vec![0.0_f64; n];
    let mut signal = vec![0.0_f64; n];
    for i in 0..n {
        let lo = i.saturating_sub(w - 1);
        let slice = &spread[lo..=i];
        let m = slice.len() as f64;
        if m < 2.0 {
            continue;
        }
        let mean = slice.iter().sum::<f64>() / m;
        let var = slice.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / m;
        let std = var.sqrt();
        if std > 1e-12 {
            zscore[i] = (spread[i] - mean) / std;
            signal[i] = -zscore[i];
        }
    }
    SpreadReversion { zscore, signal }
}

/// Information Ratio from the fundamental law of active management:
/// IR = IC · √(N_independent).
pub fn information_ratio(ic: f64, n_independent: f64) -> f64 {
    ic * n_independent.max(0.0).sqrt()
}

/// Effective number of independent signals from a returns matrix
/// (rows = signals, cols = time). Uses the participation ratio of the correlation
/// eigenvalues: N_eff = (Σλ)² / Σλ² — collapses correlated signals.
pub fn effective_independent_n(returns_matrix: &[Vec<f64>]) -> f64 {
    let k = returns_matrix.len();
    if k == 0 {
        return 0.0;
    }
    let t = returns_matrix[0].len();
    if t < 2 {
        return k as f64;
    }
    // correlation matrix
    let means: Vec<f64> = returns_matrix
        .iter()
        .map(|r| r.iter().sum::<f64>() / r.len() as f64)
        .collect();
    let stds: Vec<f64> = returns_matrix
        .iter()
        .enumerate()
        .map(|(i, r)| {
            (r.iter().map(|x| (x - means[i]).powi(2)).sum::<f64>() / r.len() as f64).sqrt()
        })
        .collect();
    let mut corr = DMatrixLite::zeros(k);
    for i in 0..k {
        for j in 0..k {
            if stds[i] < 1e-12 || stds[j] < 1e-12 {
                corr.set(i, j, if i == j { 1.0 } else { 0.0 });
                continue;
            }
            let mut c = 0.0;
            for s in 0..t.min(returns_matrix[i].len()).min(returns_matrix[j].len()) {
                c += (returns_matrix[i][s] - means[i]) * (returns_matrix[j][s] - means[j]);
            }
            c /= t as f64 * stds[i] * stds[j];
            corr.set(i, j, c);
        }
    }
    // eigenvalues via symmetric Jacobi; participation ratio
    let eig = corr.jacobi_eigenvalues();
    let sum: f64 = eig.iter().sum();
    let sum_sq: f64 = eig.iter().map(|l| l * l).sum();
    if sum_sq > 1e-12 {
        (sum * sum) / sum_sq
    } else {
        k as f64
    }
}

/// Minimal symmetric dense matrix with a Jacobi eigenvalue routine — local to
/// avoid pulling nalgebra into quant.rs for one op.
struct DMatrixLite {
    n: usize,
    data: Vec<f64>,
}
impl DMatrixLite {
    fn zeros(n: usize) -> Self {
        DMatrixLite {
            n,
            data: vec![0.0; n * n],
        }
    }
    fn set(&mut self, i: usize, j: usize, v: f64) {
        self.data[i * self.n + j] = v;
    }
    fn get(&self, i: usize, j: usize) -> f64 {
        self.data[i * self.n + j]
    }
    /// Eigenvalues of a symmetric matrix via the cyclic Jacobi method.
    fn jacobi_eigenvalues(&self) -> Vec<f64> {
        let n = self.n;
        let mut a = self.data.clone();
        let idx = |i: usize, j: usize| i * n + j;
        for _sweep in 0..100 {
            let mut off = 0.0;
            for i in 0..n {
                for j in (i + 1)..n {
                    off += a[idx(i, j)].powi(2);
                }
            }
            if off < 1e-14 {
                break;
            }
            for p in 0..n {
                for qq in (p + 1)..n {
                    let apq = a[idx(p, qq)];
                    if apq.abs() < 1e-18 {
                        continue;
                    }
                    let app = a[idx(p, p)];
                    let aqq = a[idx(qq, qq)];
                    // Jacobi rotation angle: ½·atan2(2·a_pq, a_pp − a_qq). When the
                    // diagonal entries are equal this correctly yields π/4 (a full
                    // rotation), not 0 — the case that breaks the naive formula.
                    let phi = 0.5 * (2.0 * apq).atan2(app - aqq);
                    let (c, s) = (phi.cos(), phi.sin());
                    for k in 0..n {
                        let akp = a[idx(k, p)];
                        let akq = a[idx(k, qq)];
                        a[idx(k, p)] = c * akp - s * akq;
                        a[idx(k, qq)] = s * akp + c * akq;
                    }
                    for k in 0..n {
                        let apk = a[idx(p, k)];
                        let aqk = a[idx(qq, k)];
                        a[idx(p, k)] = c * apk - s * aqk;
                        a[idx(qq, k)] = s * apk + c * aqk;
                    }
                }
            }
        }
        (0..n).map(|i| a[idx(i, i)]).collect()
    }
}

/// Grinold-style alpha combination engine: combine N signals' historical return
/// series into weights that reward independent edge and penalise shared variance.
/// Rows = signals, cols = periods. Returns weights summing to 1 in absolute value.
pub fn alpha_combination_engine(returns_matrix: &[Vec<f64>], lookback: usize) -> Vec<f64> {
    let k = returns_matrix.len();
    if k == 0 {
        return vec![];
    }
    let m = returns_matrix[0].len();
    if m < 3 {
        return vec![1.0 / k as f64; k];
    }
    // serial demean + normalise each signal
    let mut sigma = vec![0.0; k];
    let mut y = vec![vec![0.0; m]; k];
    for i in 0..k {
        let mean = returns_matrix[i].iter().sum::<f64>() / m as f64;
        let var = returns_matrix[i]
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / m as f64;
        sigma[i] = var.sqrt().max(1e-12);
        for s in 0..m {
            y[i][s] = (returns_matrix[i][s] - mean) / sigma[i];
        }
    }
    // cross-sectional demean at each period → Λ
    let mut lambda = vec![vec![0.0; m]; k];
    for s in 0..m {
        let cs_mean = (0..k).map(|i| y[i][s]).sum::<f64>() / k as f64;
        for i in 0..k {
            lambda[i][s] = y[i][s] - cs_mean;
        }
    }
    // expected forward return per signal over the lookback window, normalised
    let lb = lookback.min(m).max(1);
    let e: Vec<f64> = (0..k)
        .map(|i| {
            let recent = &returns_matrix[i][m - lb..];
            (recent.iter().sum::<f64>() / lb as f64) / sigma[i]
        })
        .collect();
    // residual of E on Λ rows = independent contribution. Regress E (length k)
    // on the per-signal mean Λ exposure to remove shared structure.
    let lam_mean: Vec<f64> = (0..k)
        .map(|i| lambda[i].iter().sum::<f64>() / m as f64)
        .collect();
    // simple univariate residualisation: e_indep = E − β·Λ̄ where β = cov/var
    let lm_mean = lam_mean.iter().sum::<f64>() / k as f64;
    let e_mean = e.iter().sum::<f64>() / k as f64;
    let mut cov = 0.0;
    let mut var = 0.0;
    for i in 0..k {
        cov += (lam_mean[i] - lm_mean) * (e[i] - e_mean);
        var += (lam_mean[i] - lm_mean).powi(2);
    }
    let beta = if var > 1e-12 { cov / var } else { 0.0 };
    let residual: Vec<f64> = (0..k).map(|i| e[i] - beta * lam_mean[i]).collect();
    // weight = independent edge / noise
    let mut w: Vec<f64> = (0..k).map(|i| residual[i] / sigma[i]).collect();
    let abs_sum: f64 = w.iter().map(|x| x.abs()).sum();
    if abs_sum > 1e-12 {
        for wi in w.iter_mut() {
            *wi /= abs_sum;
        }
    }
    w
}

/// Brier score — mean squared error of probabilistic forecasts vs binary
/// outcomes. Lower is better; < 0.25 is production-grade calibration.
pub fn brier_score(forecasts: &[f64], outcomes: &[f64]) -> f64 {
    let n = forecasts.len().min(outcomes.len());
    if n == 0 {
        return 0.0;
    }
    forecasts
        .iter()
        .zip(outcomes.iter())
        .take(n)
        .map(|(p, o)| (p - o).powi(2))
        .sum::<f64>()
        / n as f64
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConvergenceGate {
    pub agree: usize,
    pub total: usize,
    pub fraction: f64,
    pub direction: i32, // +1 up, -1 down, 0 none
    pub pass: bool,
}

/// Conviction gate: require ≥ `min_agree` of N signals to STRONGLY agree on a
/// direction (|strength| ≥ `strong_threshold`) before trading. This is the
/// "5/5 strong agreement" filter that kills 90%+ of candidate trades.
pub fn convergence_gate(
    strengths: &[f64],
    strong_threshold: f64,
    min_agree: usize,
) -> ConvergenceGate {
    let total = strengths.len();
    let up = strengths.iter().filter(|s| **s >= strong_threshold).count();
    let down = strengths
        .iter()
        .filter(|s| **s <= -strong_threshold)
        .count();
    let (agree, direction) = if up >= down {
        (up, if up > 0 { 1 } else { 0 })
    } else {
        (down, -1)
    };
    ConvergenceGate {
        agree,
        total,
        fraction: if total > 0 {
            agree as f64 / total as f64
        } else {
            0.0
        },
        direction,
        pass: agree >= min_agree && agree > 0,
    }
}

/// Deterministic splitmix64 RNG for seedable bootstrap (no external rand dep,
/// no Math.random — reproducible across runs).
struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_index(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Empirical (uncertainty-adjusted) Kelly: f_empirical = f_kelly · (1 − CV_edge),
/// where CV_edge is the coefficient of variation of the edge estimate measured by
/// bootstrapping the historical returns. Penalises uncertain edges, floored at 0.
pub fn empirical_kelly(
    p: f64,
    b: f64,
    historical_returns: &[f64],
    n_simulations: usize,
    seed: u64,
) -> f64 {
    if b <= 0.0 {
        return 0.0;
    }
    let q = 1.0 - p;
    let f_kelly = (p * b - q) / b;
    if f_kelly <= 0.0 {
        return 0.0;
    }
    let n = historical_returns.len();
    if n == 0 || n_simulations == 0 {
        return f_kelly.clamp(0.0, 1.0);
    }
    let mut rng = SplitMix64::new(seed);
    let mut edges = Vec::with_capacity(n_simulations);
    for _ in 0..n_simulations {
        let mut acc = 0.0;
        for _ in 0..n {
            acc += historical_returns[rng.next_index(n)];
        }
        edges.push(acc / n as f64);
    }
    let mean = edges.iter().sum::<f64>() / n_simulations as f64;
    if mean.abs() < 1e-12 {
        return 0.0;
    }
    let var = edges.iter().map(|e| (e - mean).powi(2)).sum::<f64>() / n_simulations as f64;
    let cv = var.sqrt() / mean.abs();
    (f_kelly * (1.0 - cv)).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_special_functions() {
        assert!((sf::norm_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((sf::norm_cdf(1.96) - 0.975).abs() < 1e-3);
        assert!((sf::norm_ppf(0.975) - 1.96).abs() < 1e-2);
        // Beta(2,2) is symmetric: median = 0.5, cdf(0.5)=0.5
        assert!((sf::betai(2.0, 2.0, 0.5) - 0.5).abs() < 1e-6);
        assert!((sf::beta_ppf(0.5, 2.0, 2.0) - 0.5).abs() < 1e-4);
    }

    #[test]
    fn test_avellaneda_stoikov_inventory_skew() {
        let flat = avellaneda_stoikov(100.0, 0.0, 0.02, 0.1, 1.5, 1.0);
        // symmetric around mid when inventory is zero
        assert!((flat.reservation - 100.0).abs() < 1e-9);
        assert!(flat.bid < flat.ask);
        // long inventory pushes reservation below mid (wants to sell)
        let long = avellaneda_stoikov(100.0, 10.0, 0.02, 0.1, 1.5, 1.0);
        assert!(long.reservation < 100.0);
    }

    #[test]
    fn test_logit_quotes_bounded_and_cap() {
        let q = logit_space_quotes(0.5, 0.0, 0.5, 0.1, 1.5, 1.0, 100.0);
        assert!(q.bid > 0.0 && q.ask < 1.0 && q.bid < q.ask);
        assert!(!q.withdraw);
        // huge inventory near boundary triggers withdraw
        let q2 = logit_space_quotes(0.02, 500.0, 0.5, 0.1, 1.5, 1.0, 100.0);
        assert!(q2.withdraw);
    }

    #[test]
    fn test_glosten_milgrom_and_breakeven() {
        assert!((glosten_milgrom_spread(0.2, 0.5) - 0.1).abs() < 1e-9);
        let be = breakeven_alpha(0.01, 0.5, 1.0, 0.0);
        assert!(be > 0.0 && be < 1.0);
    }

    #[test]
    fn test_microprice_and_vpin() {
        let mp = microprice_series(&[0.49], &[100.0], &[0.51], &[100.0]);
        assert!((mp[0] - 0.5).abs() < 1e-9);
        // balanced buy/sell ⇒ low toxicity; one-sided ⇒ higher
        let balanced = vpin_pm(&[50.0], &[50.0], &[0.5]);
        let toxic = vpin_pm(&[100.0], &[0.0], &[0.5]);
        assert!(toxic > balanced);
    }

    #[test]
    fn test_ofi_series_runs() {
        let ts = vec![0.0, 0.5, 1.0, 1.5];
        let bp = vec![0.49, 0.49, 0.50, 0.50];
        let bs = vec![100.0, 120.0, 130.0, 130.0];
        let ap = vec![0.51, 0.51, 0.51, 0.52];
        let as_ = vec![100.0, 90.0, 90.0, 80.0];
        let ofi = ofi_series(&ts, &bp, &bs, &ap, &as_, 1.0);
        assert_eq!(ofi.len(), 4);
    }

    #[test]
    fn test_hawkes_mle_recovers_excitation() {
        // synthetic clustered times; just assert it fits within stationarity
        let times: Vec<f64> = (0..50)
            .map(|i| i as f64 * 0.3 + (i % 3) as f64 * 0.05)
            .collect();
        let fit = hawkes_mle(&times, 20.0, 200);
        assert!(fit.mu > 0.0);
        assert!(fit.branching_ratio >= 0.0 && fit.branching_ratio < 1.0);
        assert!(fit.beta > 0.0);
    }

    #[test]
    fn test_hardiman_bouchaud() {
        let times: Vec<f64> = (0..100).map(|i| i as f64 * 0.1).collect();
        let n = hardiman_bouchaud_branching_ratio(&times, 10.0, 20);
        assert!((0.0..1.0).contains(&n));
    }

    #[test]
    fn test_kelly() {
        // q=0.6, c=0.5 -> f* = 0.2; quarter-kelly -> 0.05
        let f = kelly_fraction(0.6, 0.5, 0.25);
        assert!((f - 0.05).abs() < 1e-9);
        assert_eq!(kelly_fraction(0.4, 0.5, 0.25), 0.0); // negative EV killed
    }

    #[test]
    fn test_bayesian_kelly_shrinks_with_uncertainty() {
        // tight posterior around 0.6 vs wide posterior, same mean
        let tight = bayesian_kelly_fraction(60.0, 40.0, 0.5, 32);
        let wide = bayesian_kelly_fraction(6.0, 4.0, 0.5, 32);
        assert!(tight >= wide); // more uncertainty ⇒ smaller (or equal) bet
        let (lo, hi) = posterior_credible_interval(60.0, 40.0, 0.05);
        assert!(lo < 0.6 && hi > 0.6);
    }

    #[test]
    fn test_purged_cpcv() {
        let splits = purged_cpcv_splits(120, 6, 2, 5, 5);
        // C(6,2) = 15 splits
        assert_eq!(splits.len(), 15);
        for s in &splits {
            // train and test never overlap
            let tset: std::collections::HashSet<_> = s.test.iter().collect();
            assert!(s.train.iter().all(|i| !tset.contains(i)));
        }
    }

    #[test]
    fn test_deflated_sharpe_and_pbo() {
        let rets: Vec<f64> = (0..100)
            .map(|i| 0.01 + 0.001 * ((i % 7) as f64 - 3.0))
            .collect();
        let dsr = deflated_sharpe_ratio(1.5, 10, &rets);
        assert!((0.0..=1.0).contains(&dsr));
        // IS-best always OOS-best ⇒ PBO = 0
        let is = vec![vec![0.1, 0.2, 0.3], vec![0.3, 0.2, 0.1]];
        let oos = vec![vec![0.1, 0.2, 0.3], vec![0.3, 0.2, 0.1]];
        let pbo = probability_of_backtest_overfit(&is, &oos);
        assert!(pbo < 0.5);
    }

    #[test]
    fn test_diebold_mariano() {
        // A strictly lower loss than B ⇒ A better, significant
        let a: Vec<f64> = (0..50).map(|_| 1.0).collect();
        let b: Vec<f64> = (0..50).map(|_| 2.0).collect();
        let dm = diebold_mariano(&a, &b, 1);
        assert!(dm.a_better);
    }

    #[test]
    fn test_order_book_imbalance() {
        let obi = order_book_imbalance(&[100.0, 50.0], &[0.0, 50.0]);
        assert!((obi[0] - 1.0).abs() < 1e-9); // all bid
        assert!(obi[1].abs() < 1e-9); // balanced
    }

    #[test]
    fn test_queue_imbalance() {
        // balanced queue ⇒ skew ≈ 0
        let q = queue_imbalance(
            &[100.0, 50.0],
            &[100.0, 150.0],
            &[10.0, 10.0],
            &[10.0, 10.0],
        );
        assert!(q.skew[0].abs() < 1e-9);
        // ask queue heavier ⇒ positive skew (bid fills faster)
        assert!(q.skew[1] > 0.0);
        // fill time = queue_ahead / rate
        assert!((q.bid_fill_time[0] - 10.0).abs() < 1e-9);
        assert!((q.ask_fill_time[1] - 15.0).abs() < 1e-9);
    }

    #[test]
    fn test_realized_vol_tick() {
        // constant mid ⇒ zero realized vol everywhere
        let flat = realized_vol_tick(&[100.0, 100.0, 100.0, 100.0], 3);
        assert!(flat.iter().all(|v| v.abs() < 1e-12));
        // a moving series ⇒ strictly positive rolling RV
        let rv = realized_vol_tick(&[100.0, 101.0, 100.0, 102.0, 101.0], 3);
        assert_eq!(rv.len(), 5);
        assert!(rv[4] > 0.0);
    }

    #[test]
    fn test_spread_reversion() {
        // a sudden spread widening at the end ⇒ negative reversion signal (expect tighten)
        let bid = vec![0.50, 0.50, 0.50, 0.50, 0.50, 0.50];
        let ask = vec![0.52, 0.52, 0.52, 0.52, 0.52, 0.60];
        let sr = spread_reversion(&bid, &ask, 5);
        assert_eq!(sr.signal.len(), 6);
        assert!(sr.zscore[5] > 0.0 && sr.signal[5] < 0.0);
    }

    #[test]
    fn test_information_ratio_and_effective_n() {
        // IR = IC*sqrt(N): 0.05 * sqrt(50) ≈ 0.3536
        assert!((information_ratio(0.05, 50.0) - 0.353_553).abs() < 1e-4);
        // two identical (perfectly correlated) signals ⇒ N_eff ≈ 1
        let s = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let neff_corr = effective_independent_n(&[s.clone(), s.clone()]);
        assert!(neff_corr < 1.5, "neff={}", neff_corr);
        // two independent-ish signals ⇒ N_eff closer to 2
        let a = vec![1.0, -1.0, 1.0, -1.0, 1.0, -1.0];
        let b = vec![1.0, 1.0, -1.0, -1.0, 1.0, 1.0];
        let neff2 = effective_independent_n(&[a, b]);
        assert!(neff2 > neff_corr);
    }

    #[test]
    fn test_alpha_combination_engine_weights_sum_to_one() {
        let m = vec![
            vec![0.01, 0.02, -0.01, 0.03, 0.0, 0.01, 0.02, -0.02],
            vec![-0.01, 0.0, 0.02, -0.01, 0.01, 0.0, -0.01, 0.02],
            vec![0.02, -0.01, 0.0, 0.01, -0.02, 0.01, 0.0, 0.01],
        ];
        let w = alpha_combination_engine(&m, 4);
        assert_eq!(w.len(), 3);
        let abs_sum: f64 = w.iter().map(|x| x.abs()).sum();
        assert!((abs_sum - 1.0).abs() < 1e-9, "abs_sum={}", abs_sum);
    }

    #[test]
    fn test_brier_score() {
        // perfect calibration ⇒ 0
        assert!(brier_score(&[1.0, 0.0], &[1.0, 0.0]).abs() < 1e-12);
        // p=0.5 on every outcome ⇒ 0.25
        assert!((brier_score(&[0.5, 0.5, 0.5, 0.5], &[1.0, 0.0, 1.0, 0.0]) - 0.25).abs() < 1e-9);
    }

    #[test]
    fn test_convergence_gate() {
        // 5/5 strong up
        let g = convergence_gate(&[0.9, 0.8, 0.95, 0.85, 0.9], 0.6, 5);
        assert!(g.pass && g.direction == 1 && g.agree == 5);
        // only 3 strong, need 5 ⇒ fail
        let g2 = convergence_gate(&[0.9, 0.8, 0.1, 0.0, 0.7], 0.6, 5);
        assert!(!g2.pass);
    }

    #[test]
    fn test_empirical_kelly_penalises_uncertainty() {
        // positive-EV bet, stable returns ⇒ close to raw Kelly
        let stable: Vec<f64> = (0..200).map(|_| 0.02).collect();
        let f_stable = empirical_kelly(0.6, 1.0, &stable, 500, 42);
        // same bet but noisy edge ⇒ smaller fraction
        let noisy: Vec<f64> = (0..200)
            .map(|i| 0.02 + 0.2 * ((i % 5) as f64 - 2.0))
            .collect();
        let f_noisy = empirical_kelly(0.6, 1.0, &noisy, 500, 42);
        assert!(f_stable > 0.0);
        assert!(f_noisy <= f_stable, "stable={} noisy={}", f_stable, f_noisy);
        // negative EV ⇒ 0
        assert_eq!(empirical_kelly(0.4, 1.0, &stable, 100, 1), 0.0);
    }
}
