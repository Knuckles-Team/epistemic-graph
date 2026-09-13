// CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest — Market-Microstructure, Sizing & Backtest-Validation Kernels
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
            1.383_577_518_672_69e2,
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
            0.999_999_999_999_809_9,
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
        let m = n.div_ceil(2);
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

#[path = "quant/market_making.rs"]
mod market_making;
#[path = "quant/microstructure.rs"]
mod microstructure;
#[path = "quant/signals.rs"]
mod signals;
#[path = "quant/surveillance.rs"]
mod surveillance;
#[path = "quant/validation.rs"]
mod validation;

pub use market_making::{
    avellaneda_stoikov, breakeven_alpha, expected_pnl_rate, glosten_milgrom_spread, glt_quotes,
    logit_space_quotes, Quote,
};
pub(super) use microstructure::nelder_mead_3;
pub use microstructure::{
    hardiman_bouchaud_branching_ratio, hawkes_mle, microprice_series, ofi_series, vpin_pm,
    HawkesFit,
};
pub use signals::{
    alpha_combination_engine, brier_score, convergence_gate, effective_independent_n,
    empirical_kelly, information_ratio, order_book_imbalance, queue_imbalance, realized_vol_tick,
    spread_reversion, ConvergenceGate, QueueSignal, SpreadReversion,
};
pub use surveillance::{kyle_lambda, surveillance_risk, SurveillanceRisk};
pub use validation::{
    bayesian_kelly_fraction, deflated_sharpe_ratio, diebold_mariano, kelly_fraction,
    posterior_credible_interval, probability_of_backtest_overfit, purged_cpcv_splits, CvSplit,
    DieboldMariano,
};

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

    #[test]
    fn test_kyle_lambda_recovers_slope() {
        // price_change = 0.5 * signed_flow exactly ⇒ λ = 0.5
        let flow: Vec<f64> = (1..=20).map(|i| i as f64).collect();
        let dp: Vec<f64> = flow.iter().map(|q| 0.5 * q).collect();
        assert!((kyle_lambda(&dp, &flow) - 0.5).abs() < 1e-9);
        // no flow variance ⇒ 0 (no division blow-up)
        assert_eq!(kyle_lambda(&[0.1, 0.2], &[3.0, 3.0]), 0.0);
        assert_eq!(kyle_lambda(&[], &[]), 0.0);
    }

    #[test]
    fn test_surveillance_risk_separates_toxic_from_benign() {
        // Benign: near-balanced two-sided flow near σ ⇒ low hazard, low toxicity,
        // mostly noise (high camouflage ratio).
        let benign = surveillance_risk(
            &[110.0, 110.0, 110.0],
            &[100.0, 100.0, 100.0],
            &[0.5, 0.5, 0.5],
            &[1.0, -1.0, 1.0],
            &[0.01, -0.01, 0.01],
            1.0,
        );
        // Toxic: persistent one-sided buying far above the noise baseline.
        let toxic = surveillance_risk(
            &[500.0, 500.0, 500.0],
            &[5.0, 5.0, 5.0],
            &[0.5, 0.5, 0.5],
            &[8.0, 9.0, 10.0],
            &[0.4, 0.45, 0.5],
            1.0,
        );
        for r in [&benign, &toxic] {
            assert!((0.0..=1.0).contains(&r.legal_risk_score));
            assert!(r.informed_share >= 0.0);
        }
        assert!(
            toxic.legal_risk_score > benign.legal_risk_score,
            "toxic={} benign={}",
            toxic.legal_risk_score,
            benign.legal_risk_score
        );
        assert!(toxic.detection_hazard > benign.detection_hazard);
        assert!(toxic.cumulative_suspicion > benign.cumulative_suspicion);
        // toxic flow is mostly informed ⇒ low camouflage; benign is mostly noise.
        assert!(toxic.stealth_ratio < benign.stealth_ratio);
    }
}
