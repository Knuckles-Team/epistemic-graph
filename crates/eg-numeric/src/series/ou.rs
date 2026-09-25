//! Generic Ornstein-Uhlenbeck calibration and first-passage bands.

use serde::{Deserialize, Serialize};

use super::ols::ols_with_se;
use crate::detkernel::math;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OuEstimate {
    pub theta: f64,
    pub mu: f64,
    pub sigma: f64,
    pub half_life: f64,
    pub sigma_eq: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OuBands {
    pub entry_long: f64,
    pub entry_short: f64,
    pub exit: f64,
    pub z: f64,
    pub expected_return_per_unit_time: f64,
}

/// Calibrate an OU process dS = θ(μ−S)dt + σ dW from a discretely-sampled spread
/// via the exact AR(1) discretisation S_t = a + b·S_{t-1} + ε (Euler-Maruyama /
/// MLE-equivalent). `dt` is the sampling interval.
pub fn calibrate(spread: &[f64], dt: f64) -> OuEstimate {
    let n = spread.len();
    if n < 3 {
        return OuEstimate {
            theta: 0.0,
            mu: spread.iter().sum::<f64>() / n.max(1) as f64,
            sigma: 0.0,
            half_life: f64::INFINITY,
            sigma_eq: 0.0,
        };
    }
    let x: Vec<Vec<f64>> = (0..n - 1).map(|i| vec![1.0, spread[i]]).collect();
    let y: Vec<f64> = (1..n).map(|i| spread[i]).collect();
    let (a, b, resid_var) = match ols_with_se(&x, &y) {
        Some((coefs, _, sigma2)) => (coefs[0], coefs[1].clamp(-0.999_999, 0.999_999), sigma2),
        None => (0.0, 0.0, 0.0),
    };
    let theta = if b > 0.0 { -math::ln(b) / dt } else { 1.0 / dt };
    let mu = if (1.0 - b).abs() > 1e-9 {
        a / (1.0 - b)
    } else {
        y.iter().sum::<f64>() / y.len() as f64
    };
    // σ from residual variance: Var(ε) = σ²(1−e^{-2θΔt})/(2θ)
    let denom = 1.0 - math::exp(-2.0 * theta * dt);
    let sigma = if denom > 1e-12 {
        (resid_var * 2.0 * theta / denom).sqrt()
    } else {
        resid_var.sqrt()
    };
    let sigma_eq = if theta > 1e-12 {
        sigma / (2.0 * theta).sqrt()
    } else {
        sigma
    };
    OuEstimate {
        theta,
        mu,
        sigma,
        half_life: if theta > 1e-12 {
            std::f64::consts::LN_2 / theta
        } else {
            f64::INFINITY
        },
        sigma_eq,
    }
}

/// Expected first-passage time (in σ_eq units) of a normalised OU from deviation
/// `b` back to the mean (0), solved from the backward-Kolmogorov MFPT ODE
/// u'' − z·u' = −1/θ on [0, b] with u(0)=0 (exit at mean) and u'(b)=0 (reflecting
/// at entry), via a tridiagonal finite-difference solve.
fn ou_mfpt(theta: f64, b: f64, n: usize) -> f64 {
    if b <= 0.0 || theta <= 0.0 {
        return f64::INFINITY;
    }
    let h = b / n as f64;
    // unknowns u_1..u_n (u_0 = 0). Node i at z_i = i*h.
    // interior i=1..n-1: (u_{i+1}-2u_i+u_{i-1})/h² − z_i(u_{i+1}-u_{i-1})/(2h) = −1/θ
    // boundary i=n: reflecting u'(b)=0 → ghost u_{n+1}=u_{n-1}; gives
    //   (2u_{n-1}-2u_n)/h² = −1/θ
    let m = n; // unknown count (indices 1..=n)
    let mut lower = vec![0.0; m];
    let mut diag = vec![0.0; m];
    let mut upper = vec![0.0; m];
    let mut rhs = vec![0.0; m];
    let inv_h2 = 1.0 / (h * h);
    for i in 1..=n {
        let z = i as f64 * h;
        let row = i - 1;
        if i < n {
            let a_low = inv_h2 + z / (2.0 * h);
            let a_diag = -2.0 * inv_h2;
            let a_up = inv_h2 - z / (2.0 * h);
            lower[row] = a_low;
            diag[row] = a_diag;
            upper[row] = a_up;
            rhs[row] = -1.0 / theta;
            if i == 1 {
                // u_0 = 0 → drop lower contribution
                lower[row] = 0.0;
            }
        } else {
            // reflecting boundary at i=n
            lower[row] = 2.0 * inv_h2;
            diag[row] = -2.0 * inv_h2;
            upper[row] = 0.0;
            rhs[row] = -1.0 / theta;
        }
    }
    // Thomas algorithm
    for i in 1..m {
        let w = lower[i] / diag[i - 1];
        diag[i] -= w * upper[i - 1];
        rhs[i] -= w * rhs[i - 1];
    }
    let mut u = vec![0.0; m];
    u[m - 1] = rhs[m - 1] / diag[m - 1];
    for i in (0..m - 1).rev() {
        u[i] = (rhs[i] - upper[i] * u[i + 1]) / diag[i];
    }
    u[m - 1] // MFPT from entry (z=b) to mean
}

/// MFPT-optimal OU entry/exit band. Grid-searches the entry deviation z (in σ_eq
/// units) that maximises expected profit per unit time
/// J(z) = (z·σ_eq − cost) / MFPT(z), capturing the move from entry back to the mean.
pub fn optimal_thresholds(params: &OuEstimate, cost: f64) -> OuBands {
    let s = params.sigma_eq;
    let theta = params.theta;
    let (best_z, best_j) = if s > 1e-12 && theta > 1e-12 {
        search_best_entry_z(theta, s, cost)
    } else {
        (1.0, f64::NEG_INFINITY)
    };
    OuBands {
        entry_long: params.mu - best_z * s,
        entry_short: params.mu + best_z * s,
        exit: params.mu,
        z: best_z,
        expected_return_per_unit_time: if best_j.is_finite() { best_j } else { 0.0 },
    }
}

/// Grid-search the entry deviation `z` (in `σ_eq` units) maximizing expected
/// profit per unit time `J(z) = (z·σ_eq − cost) / MFPT(z)`. Returns `(best_z, best_j)`.
fn search_best_entry_z(theta: f64, s: f64, cost: f64) -> (f64, f64) {
    let mut best_z = 1.0;
    let mut best_j = f64::NEG_INFINITY;
    let steps = 60;
    for i in 1..=steps {
        let z = 0.05 * i as f64; // up to 3.0 σ_eq
        let profit = z * s - cost;
        if profit <= 0.0 {
            continue;
        }
        let t = ou_mfpt(theta, z, 50);
        if !t.is_finite() || t <= 0.0 {
            continue;
        }
        let j = profit / t;
        if j > best_j {
            best_j = j;
            best_z = z;
        }
    }
    (best_z, best_j)
}

#[cfg(test)]
mod tests {
    use super::{calibrate, optimal_thresholds};

    #[test]
    fn mean_reverting_series_produces_ordered_bands() {
        let mut spread = vec![1.0];
        for i in 1..128 {
            let next = 0.75 * spread[i - 1] + 0.25 + libm::sin(i as f64 * 0.31) * 0.02;
            spread.push(next);
        }
        let estimate = calibrate(&spread, 1.0);
        let bands = optimal_thresholds(&estimate, 0.001);
        assert!(estimate.theta.is_finite() && estimate.theta > 0.0);
        assert!(bands.entry_long < bands.exit);
        assert!(bands.exit < bands.entry_short);
    }
}
