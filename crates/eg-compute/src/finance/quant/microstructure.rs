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

pub use eg_types::compute_result::finance::HawkesFit;

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

/// Initialize the 4-point simplex around `x0` (the vertex-perturbation step
/// of `nelder_mead_3`). Split out (extract-method, cx/wD8) — same terms,
/// same order as before.
fn nelder_mead_initial_simplex(x0: [f64; 3], zero_step: f64) -> [[f64; 3]; 4] {
    let mut simplex = [x0; 4];
    for i in 0..3 {
        let mut p = x0;
        p[i] = if p[i].abs() > 1e-9 {
            p[i] * 1.05
        } else {
            zero_step
        };
        simplex[i + 1] = p;
    }
    simplex
}

struct NelderMeadCoefficients {
    alpha: f64,
    gamma: f64,
    rho: f64,
    sigma: f64,
}

/// Reflect/expand/contract/shrink the simplex about `cen` for one Nelder-Mead
/// iteration whose ordering is already known. Split out of
/// `nelder_mead_iteration` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before.
fn nelder_mead_update_simplex(
    f: &dyn Fn([f64; 3]) -> f64,
    simplex: &mut [[f64; 3]; 4],
    fvals: &mut [f64; 4],
    order: &[usize; 4],
    cen: [f64; 3],
    coefficients: NelderMeadCoefficients,
) {
    let best = order[0];
    let worst = order[3];
    let second_worst = order[2];
    let reflect = |coef: f64| {
        let mut p = [0.0; 3];
        for d in 0..3 {
            p[d] = cen[d] + coef * (cen[d] - simplex[worst][d]);
        }
        p
    };
    let xr = reflect(coefficients.alpha);
    let fr = f(xr);
    if fr < fvals[best] {
        let xe = reflect(coefficients.gamma);
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
        let xc = reflect(-coefficients.rho);
        let fc = f(xc);
        if fc < fvals[worst] {
            simplex[worst] = xc;
            fvals[worst] = fc;
        } else {
            nelder_mead_shrink(f, simplex, fvals, order, coefficients.sigma);
        }
    }
}

/// Shrink every non-best simplex vertex toward the best vertex. Split out of
/// `nelder_mead_update_simplex` (extract-method, cx/wD8) — same terms, same
/// order as before.
fn nelder_mead_shrink(
    f: &dyn Fn([f64; 3]) -> f64,
    simplex: &mut [[f64; 3]; 4],
    fvals: &mut [f64; 4],
    order: &[usize; 4],
    s: f64,
) {
    let best = order[0];
    for &i in order.iter().skip(1) {
        for d in 0..3 {
            simplex[i][d] = simplex[best][d] + s * (simplex[i][d] - simplex[best][d]);
        }
        fvals[i] = f(simplex[i]);
    }
}

/// One Nelder-Mead iteration: order, check convergence, reflect/expand/
/// contract/shrink. Split out of `nelder_mead_3` (extract-method, cx/wD8) —
/// same terms, same arithmetic order as before. Returns whether the simplex
/// has converged (`fvals[worst] - fvals[best]` within `tol`).
fn nelder_mead_iteration(
    f: &dyn Fn([f64; 3]) -> f64,
    simplex: &mut [[f64; 3]; 4],
    fvals: &mut [f64; 4],
    a: f64,
    g: f64,
    r: f64,
    s: f64,
    tol: f64,
) -> bool {
    // order
    let mut idx = [0, 1, 2, 3];
    idx.sort_by(|&i, &j| fvals[i].partial_cmp(&fvals[j]).unwrap());
    let order = idx;
    let best = order[0];
    let worst = order[3];
    if (fvals[worst] - fvals[best]).abs() < tol {
        return true;
    }
    // centroid of all but worst
    let mut cen = [0.0; 3];
    for &i in order.iter().take(3) {
        for d in 0..3 {
            cen[d] += simplex[i][d] / 3.0;
        }
    }
    nelder_mead_update_simplex(
        f,
        simplex,
        fvals,
        &order,
        cen,
        NelderMeadCoefficients {
            alpha: a,
            gamma: g,
            rho: r,
            sigma: s,
        },
    );
    false
}

/// Nelder-Mead simplex minimiser for a 3-parameter objective.
pub(in crate::finance) fn nelder_mead_3(
    f: &dyn Fn([f64; 3]) -> f64,
    x0: [f64; 3],
    max_iter: usize,
    tol: f64,
    zero_step: f64,
) -> ([f64; 3], f64, bool) {
    let (a, g, r, s) = (1.0, 2.0, 0.5, 0.5); // reflect, expand, contract, shrink
    let mut simplex = nelder_mead_initial_simplex(x0, zero_step);
    let mut fvals = [0.0; 4];
    for i in 0..4 {
        fvals[i] = f(simplex[i]);
    }
    let mut converged = false;
    for _ in 0..max_iter {
        if nelder_mead_iteration(f, &mut simplex, &mut fvals, a, g, r, s, tol) {
            converged = true;
            break;
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
    let (best, fbest, converged) = nelder_mead_3(&obj, x0, max_iter.max(50), 1e-8, 0.00025);
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
