// ════════════════════════════════════════════════════════════════════════
//  Signal combination, sizing & calibration (CONCEPT:EG-KG.domains.quant-finance)
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

pub use eg_types::compute_result::finance::QueueSignal;

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

pub use eg_types::compute_result::finance::SpreadReversion;

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
/// Build the k×k correlation matrix of `returns_matrix`'s rows. Split out of
/// `effective_independent_n` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before.
fn build_correlation_matrix(
    returns_matrix: &[Vec<f64>],
    means: &[f64],
    stds: &[f64],
    k: usize,
    t: usize,
) -> DMatrixLite {
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
    corr
}

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
    let corr = build_correlation_matrix(returns_matrix, &means, &stds, k, t);
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
        for _sweep in 0..100 {
            if jacobi_off_diagonal_norm_sq(&a, n) < 1e-14 {
                break;
            }
            for p in 0..n {
                for qq in (p + 1)..n {
                    apply_jacobi_rotation(&mut a, n, p, qq);
                }
            }
        }
        (0..n).map(|i| a[i * n + i]).collect()
    }
}

/// Sum of squared strictly-upper-triangle entries. Split out of
/// `jacobi_eigenvalues` (extract-method, cx/wD8) — same terms, same order as
/// before.
fn jacobi_off_diagonal_norm_sq(a: &[f64], n: usize) -> f64 {
    let mut off = 0.0;
    for i in 0..n {
        for j in (i + 1)..n {
            off += a[i * n + j].powi(2);
        }
    }
    off
}

/// Apply one Jacobi rotation zeroing `a[p][qq]`, in place. Split out of
/// `jacobi_eigenvalues` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before (the `apq.abs() < 1e-18` guard's `continue`
/// becomes an early `return`, equivalent since it was the last statement in
/// the loop body).
fn apply_jacobi_rotation(a: &mut [f64], n: usize, p: usize, qq: usize) {
    let idx = |i: usize, j: usize| i * n + j;
    let apq = a[idx(p, qq)];
    if apq.abs() < 1e-18 {
        return;
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

/// Grinold-style alpha combination engine: combine N signals' historical return
/// series into weights that reward independent edge and penalise shared variance.
/// Rows = signals, cols = periods. Returns weights summing to 1 in absolute value.
/// Serial demean + normalise each signal. Split out of
/// `alpha_combination_engine` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before. Returns (sigma, y).
fn demean_normalize_signals(
    returns_matrix: &[Vec<f64>],
    k: usize,
    m: usize,
) -> (Vec<f64>, Vec<Vec<f64>>) {
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
    (sigma, y)
}

/// Cross-sectional demean at each period → Λ. Split out of
/// `alpha_combination_engine` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before.
fn cross_sectional_demean(y: &[Vec<f64>], k: usize, m: usize) -> Vec<Vec<f64>> {
    let mut lambda = vec![vec![0.0; m]; k];
    for s in 0..m {
        let cs_mean = (0..k).map(|i| y[i][s]).sum::<f64>() / k as f64;
        for i in 0..k {
            lambda[i][s] = y[i][s] - cs_mean;
        }
    }
    lambda
}

/// Residual of E on Λ rows = independent contribution: regress E (length k)
/// on the per-signal mean Λ exposure to remove shared structure. Split out
/// of `alpha_combination_engine` (extract-method, cx/wD8) — same terms, same
/// arithmetic order as before.
fn residualize_independent_edge(e: &[f64], lambda: &[Vec<f64>], k: usize, m: usize) -> Vec<f64> {
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
    (0..k).map(|i| e[i] - beta * lam_mean[i]).collect()
}

pub fn alpha_combination_engine(returns_matrix: &[Vec<f64>], lookback: usize) -> Vec<f64> {
    let k = returns_matrix.len();
    if k == 0 {
        return vec![];
    }
    let m = returns_matrix[0].len();
    if m < 3 {
        return vec![1.0 / k as f64; k];
    }
    let (sigma, y) = demean_normalize_signals(returns_matrix, k, m);
    let lambda = cross_sectional_demean(&y, k, m);
    // expected forward return per signal over the lookback window, normalised
    let lb = lookback.min(m).max(1);
    let e: Vec<f64> = (0..k)
        .map(|i| {
            let recent = &returns_matrix[i][m - lb..];
            (recent.iter().sum::<f64>() / lb as f64) / sigma[i]
        })
        .collect();
    let residual = residualize_independent_edge(&e, &lambda, k, m);
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

pub use eg_types::compute_result::finance::ConvergenceGate;

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
    let mut rng = crate::SplitMix64::new(seed);
    let mut edges = Vec::with_capacity(n_simulations);
    for _ in 0..n_simulations {
        let mut acc = 0.0;
        for _ in 0..n {
            acc += historical_returns[rng.below(n)];
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
