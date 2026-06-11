// CONCEPT:KG-2.20j — Derivatives: SABR Stochastic-Volatility Surface
//
// Hagan-Kumar-Lesniewski-Woodward (2002) SABR implied-volatility approximation
// and surface calibration. Lets the finance domain price/trade volatility smiles
// and skews (vol arbitrage) instead of treating implied vol as a single number.
//
// SABR dynamics:  dF = α·F^β dW₁ ,  dα = ν·α dW₂ ,  d⟨W₁,W₂⟩ = ρ dt.
// Parameters: α (vol level), β∈[0,1] (CEV exponent), ρ∈(−1,1) (correlation),
// ν (vol-of-vol). Source: Hagan et al., "Managing Smile Risk" (Wilmott, 2002).

use serde::{Deserialize, Serialize};

/// SABR lognormal (Black) implied volatility for a single strike (Hagan 2002).
/// `f` = forward, `k` = strike, `t` = time to expiry (years).
pub fn sabr_implied_vol(f: f64, k: f64, t: f64, alpha: f64, beta: f64, rho: f64, nu: f64) -> f64 {
    if f <= 0.0 || k <= 0.0 || alpha <= 0.0 {
        return 0.0;
    }
    let one_m_beta = 1.0 - beta;
    let fk_beta = (f * k).powf(one_m_beta / 2.0);
    let log_fk = (f / k).ln();

    // The (z / x(z)) ratio (→ 1 at the money).
    let z = (nu / alpha) * fk_beta * log_fk;
    let z_over_x = if z.abs() < 1e-12 {
        1.0
    } else {
        let inner = ((1.0 - 2.0 * rho * z + z * z).sqrt() + z - rho) / (1.0 - rho);
        if inner <= 0.0 {
            1.0
        } else {
            z / inner.ln()
        }
    };

    // Denominator series in log(F/K).
    let log_fk2 = log_fk * log_fk;
    let denom = fk_beta
        * (1.0
            + (one_m_beta.powi(2) / 24.0) * log_fk2
            + (one_m_beta.powi(4) / 1920.0) * log_fk2 * log_fk2);

    // Time-dependent correction bracket.
    let term1 = (one_m_beta.powi(2) / 24.0) * alpha * alpha / (f * k).powf(one_m_beta);
    let term2 = 0.25 * rho * beta * nu * alpha / fk_beta;
    let term3 = ((2.0 - 3.0 * rho * rho) / 24.0) * nu * nu;
    let correction = 1.0 + (term1 + term2 + term3) * t;

    (alpha / denom) * z_over_x * correction
}

/// SABR implied-vol smile across a vector of strikes.
pub fn sabr_smile(
    f: f64,
    strikes: &[f64],
    t: f64,
    alpha: f64,
    beta: f64,
    rho: f64,
    nu: f64,
) -> Vec<f64> {
    strikes
        .iter()
        .map(|&k| sabr_implied_vol(f, k, t, alpha, beta, rho, nu))
        .collect()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SabrFit {
    pub alpha: f64,
    pub beta: f64,
    pub rho: f64,
    pub nu: f64,
    pub rmse: f64,
    pub converged: bool,
}

/// Calibrate SABR (α, ρ, ν) to a market smile with `beta` fixed (the usual
/// convention — β is a modelling choice, not fit). Minimises RMSE of model vs
/// market implied vols via Nelder-Mead, with α seeded from the ATM vol.
pub fn sabr_calibrate(f: f64, t: f64, strikes: &[f64], market_vols: &[f64], beta: f64) -> SabrFit {
    let n = strikes.len().min(market_vols.len());
    if n == 0 {
        return SabrFit {
            alpha: 0.0,
            beta,
            rho: 0.0,
            nu: 0.0,
            rmse: 0.0,
            converged: false,
        };
    }
    // ATM-ish seed for α: pick the strike closest to F.
    let atm_idx = (0..n)
        .min_by(|&a, &b| {
            (strikes[a] - f)
                .abs()
                .partial_cmp(&(strikes[b] - f).abs())
                .unwrap()
        })
        .unwrap_or(0);
    let atm_vol = market_vols[atm_idx].max(1e-4);
    let alpha0 = atm_vol * f.powf(1.0 - beta);

    let rmse = |alpha: f64, rho: f64, nu: f64| -> f64 {
        if alpha <= 0.0 || rho <= -0.999 || rho >= 0.999 || nu < 0.0 {
            return 1e6;
        }
        let mut sse = 0.0;
        for i in 0..n {
            let model = sabr_implied_vol(f, strikes[i], t, alpha, beta, rho, nu);
            sse += (model - market_vols[i]).powi(2);
        }
        (sse / n as f64).sqrt()
    };

    // Nelder-Mead over (alpha, rho, nu).
    let obj = |p: [f64; 3]| rmse(p[0], p[1], p[2]);
    let x0 = [alpha0, 0.0, 0.5];
    let (best, fbest, converged) = nelder_mead_3(&obj, x0, 400, 1e-10);
    SabrFit {
        alpha: best[0].max(1e-9),
        beta,
        rho: best[1].clamp(-0.999, 0.999),
        nu: best[2].max(0.0),
        rmse: fbest,
        converged,
    }
}

/// Nelder-Mead simplex minimiser for a 3-parameter objective (local copy so this
/// module is self-contained; mirrors the one used for Hawkes MLE).
fn nelder_mead_3(
    f: &dyn Fn([f64; 3]) -> f64,
    x0: [f64; 3],
    max_iter: usize,
    tol: f64,
) -> ([f64; 3], f64, bool) {
    let (a, g, r, s) = (1.0, 2.0, 0.5, 0.5);
    let mut simplex = [x0; 4];
    for i in 0..3 {
        let mut p = x0;
        p[i] = if p[i].abs() > 1e-9 { p[i] * 1.05 } else { 0.05 };
        simplex[i + 1] = p;
    }
    let mut fvals = [0.0; 4];
    for i in 0..4 {
        fvals[i] = f(simplex[i]);
    }
    let mut converged = false;
    for _ in 0..max_iter {
        let mut order = [0, 1, 2, 3];
        order.sort_by(|&i, &j| fvals[i].partial_cmp(&fvals[j]).unwrap());
        let best = order[0];
        let worst = order[3];
        let second_worst = order[2];
        if (fvals[worst] - fvals[best]).abs() < tol {
            converged = true;
            break;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sabr_atm_matches_formula() {
        // At F==K the (z/x) ratio is 1; vol ≈ (α/F^(1-β))·(1+corr·T)
        let (f, t, alpha, beta, rho, nu) = (100.0, 1.0, 0.2, 0.5, -0.3, 0.4);
        let v = sabr_implied_vol(f, f, t, alpha, beta, rho, nu);
        let base = alpha / f.powf(1.0 - beta);
        assert!(v > 0.0);
        // ATM vol within a sensible band of the leading term
        assert!((v - base).abs() / base < 0.2, "v={} base={}", v, base);
    }

    #[test]
    fn test_sabr_smile_is_convex_skew() {
        // negative rho ⇒ downside strikes carry higher implied vol (skew)
        let strikes = vec![80.0, 90.0, 100.0, 110.0, 120.0];
        let smile = sabr_smile(100.0, &strikes, 1.0, 0.2, 0.5, -0.5, 0.5);
        assert_eq!(smile.len(), 5);
        assert!(smile.iter().all(|v| *v > 0.0 && v.is_finite()));
        // low-strike (put) vol exceeds high-strike (call) vol under negative rho
        assert!(smile[0] > smile[4], "skew not present: {:?}", smile);
    }

    #[test]
    fn test_sabr_calibration_recovers_params() {
        // generate a synthetic smile, then recover (alpha, rho, nu) with beta fixed
        let (f, t, beta) = (100.0, 1.0, 0.5);
        let (alpha, rho, nu) = (0.25, -0.3, 0.4);
        let strikes = vec![80.0, 90.0, 100.0, 110.0, 120.0];
        let market = sabr_smile(f, &strikes, t, alpha, beta, rho, nu);
        let fit = sabr_calibrate(f, t, &strikes, &market, beta);
        assert!(fit.rmse < 1e-3, "rmse={}", fit.rmse);
        assert!((fit.alpha - alpha).abs() < 0.05, "alpha={}", fit.alpha);
        assert!((fit.nu - nu).abs() < 0.15, "nu={}", fit.nu);
    }
}
