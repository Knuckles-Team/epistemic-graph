// CONCEPT:EG-KG.graphlearn.kan-edge-function — a learnable univariate edge function.
//
// The atom of a Kolmogorov-Arnold Network (KAN, arXiv:2404.19756): instead of a
// fixed nonlinearity on nodes + a linear weight on edges, the EDGE itself carries a
// small learnable univariate function `f(x)`. We parameterize it as a
// POLYNOMIAL-BASIS expansion (CONCEPT:EG-KG.graphlearn.chebyshev-basis) —
// Chebyshev (first kind) by default, Jacobi optional — deliberately NOT a B-spline
// or RBF: a polynomial basis is a closed-form three-term recurrence with zero
// special functions, matching the engine's no-BLAS/no-libm-extras Raspberry-Pi
// contract (see the KAN synergy report §3a). Several list entries — `ChebyKAN`,
// `JacobiKAN`, `TaylorKAN` — validate the polynomial basis as a competitive,
// dependency-free substitute for splines.
//
// The input is squashed through `tanh` into the basis's natural domain `[-1, 1]`
// (Chebyshev/Jacobi are orthogonal there), so arbitrary-scale structural features
// are admissible. Both the forward `eval` and the analytic gradients (w.r.t. the
// coefficients — for training — and w.r.t. the input — for message passing) are
// hand-derived, exactly like `datascience::training`'s loss kernels: no autodiff
// engine, because a 1-D closed-form function needs none.
//
// The COEFFICIENTS are the interpretable artifact: they serialize onto a queryable
// `:EdgeFunction` KG node (CONCEPT:EG-KG.graphlearn.edge-function-writeback), so the
// learned curve that explains *why* a feature pushes two nodes together is
// answerable from Cypher/SQL — the one place KAN's interpretability claim is
// genuinely load-bearing for this product.

use serde::{Deserialize, Serialize};

/// Which orthogonal-polynomial basis a [`KanEdgeFn`] expands over
/// (CONCEPT:EG-KG.graphlearn.chebyshev-basis). Both are closed-form three-term
/// recurrences — no special functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Basis {
    /// Chebyshev polynomials of the first kind `T_k` (the default — cheapest and
    /// numerically well-conditioned on `[-1, 1]`).
    #[default]
    Chebyshev,
    /// Jacobi polynomials `P_k^{(α,β)}` (generalises Legendre/Chebyshev; carries the
    /// `alpha`/`beta` shape parameters stored on the [`KanEdgeFn`]).
    Jacobi,
}

/// A single learnable univariate edge function `f(x) = Σ_k coeffs[k] · B_k(tanh x)`
/// over the chosen polynomial `Basis`. `coeffs.len() == degree + 1`.
///
/// CONCEPT:EG-KG.graphlearn.kan-edge-function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KanEdgeFn {
    /// The polynomial basis family.
    pub basis: Basis,
    /// Polynomial degree `d`; there are `d + 1` coefficients (`B_0 … B_d`).
    pub degree: usize,
    /// Basis coefficients — the learned, interpretable curve. `coeffs[k]` weights `B_k`.
    pub coeffs: Vec<f64>,
    /// Jacobi α (> -1); ignored for Chebyshev.
    #[serde(default)]
    pub alpha: f64,
    /// Jacobi β (> -1); ignored for Chebyshev.
    #[serde(default)]
    pub beta: f64,
}

impl KanEdgeFn {
    /// A zero-initialised edge function of the given basis + degree.
    pub fn zeros(basis: Basis, degree: usize) -> Self {
        Self {
            basis,
            degree,
            coeffs: vec![0.0; degree + 1],
            alpha: 0.0,
            beta: 0.0,
        }
    }

    /// A Chebyshev edge function with explicit coefficients.
    pub fn chebyshev(coeffs: Vec<f64>) -> Self {
        let degree = coeffs.len().saturating_sub(1);
        Self {
            basis: Basis::Chebyshev,
            degree,
            coeffs,
            alpha: 0.0,
            beta: 0.0,
        }
    }

    /// Squash a raw input into the basis's orthogonality domain `[-1, 1]`.
    #[inline]
    fn squash(x: f64) -> f64 {
        x.tanh()
    }

    /// Basis values `[B_0(u) … B_degree(u)]` at `u ∈ [-1, 1]`.
    pub fn basis_values(&self, u: f64) -> Vec<f64> {
        match self.basis {
            Basis::Chebyshev => chebyshev_values(u, self.degree),
            Basis::Jacobi => jacobi_values(u, self.degree, self.alpha, self.beta),
        }
    }

    /// Basis derivatives `[B'_0(u) … B'_degree(u)]` w.r.t. `u`.
    fn basis_derivs(&self, u: f64) -> Vec<f64> {
        match self.basis {
            Basis::Chebyshev => chebyshev_derivs(u, self.degree),
            Basis::Jacobi => jacobi_derivs(u, self.degree, self.alpha, self.beta),
        }
    }

    /// Forward: `f(x) = Σ_k coeffs[k] · B_k(tanh x)`.
    pub fn eval(&self, x: f64) -> f64 {
        let u = Self::squash(x);
        self.basis_values(u)
            .iter()
            .zip(self.coeffs.iter())
            .map(|(b, c)| b * c)
            .sum()
    }

    /// Analytic gradient of `f(x)` w.r.t. each coefficient — this is just the basis
    /// vector `[B_k(tanh x)]`, since `∂f/∂coeffs[k] = B_k(u)`. Drives training.
    /// CONCEPT:EG-KG.graphlearn.kan-edge-function
    pub fn grad_coeffs(&self, x: f64) -> Vec<f64> {
        self.basis_values(Self::squash(x))
    }

    /// Analytic gradient of `f(x)` w.r.t. the INPUT `x` (chained through `tanh`):
    /// `f'(x) = (Σ_k coeffs[k] · B'_k(u)) · (1 − u²)` with `u = tanh x`. Used by
    /// KAN-layer backprop and the (optional) edge-weighted neighbor aggregation.
    pub fn grad_input(&self, x: f64) -> f64 {
        let u = Self::squash(x);
        let dbasis = self.basis_derivs(u);
        let inner: f64 = dbasis
            .iter()
            .zip(self.coeffs.iter())
            .map(|(b, c)| b * c)
            .sum();
        inner * (1.0 - u * u)
    }
}

// ─────────────────────────── Chebyshev (first kind) ───────────────────────────

/// `[T_0(u) … T_d(u)]` via the recurrence `T_0=1, T_1=u, T_k = 2u·T_{k-1} − T_{k-2}`.
fn chebyshev_values(u: f64, degree: usize) -> Vec<f64> {
    let mut t = Vec::with_capacity(degree + 1);
    t.push(1.0);
    if degree >= 1 {
        t.push(u);
    }
    for k in 2..=degree {
        let next = 2.0 * u * t[k - 1] - t[k - 2];
        t.push(next);
    }
    t
}

/// `[T'_0(u) … T'_d(u)]`. `T'_k = k · U_{k-1}` where `U` is the Chebyshev basis of
/// the SECOND kind (`U_0=1, U_1=2u, U_k = 2u·U_{k-1} − U_{k-2}`); `T'_0 = 0`.
fn chebyshev_derivs(u: f64, degree: usize) -> Vec<f64> {
    let mut d = Vec::with_capacity(degree + 1);
    d.push(0.0);
    if degree >= 1 {
        // U_{0} = 1 ⇒ T'_1 = 1·U_0 = 1.
        let mut u_prev = 1.0; // U_0
        d.push(1.0 * u_prev);
        if degree >= 2 {
            let mut u_curr = 2.0 * u; // U_1
            d.push(2.0 * u_curr);
            for k in 3..=degree {
                let u_next = 2.0 * u * u_curr - u_prev; // U_{k-1}
                d.push(k as f64 * u_next);
                u_prev = u_curr;
                u_curr = u_next;
            }
        }
    }
    d
}

// ─────────────────────────── Jacobi P^{(α,β)} ───────────────────────────

/// `[P_0 … P_d]` for `P_k^{(α,β)}(u)` via the standard three-term recurrence.
fn jacobi_values(u: f64, degree: usize, alpha: f64, beta: f64) -> Vec<f64> {
    let mut p = Vec::with_capacity(degree + 1);
    p.push(1.0);
    if degree >= 1 {
        p.push(0.5 * (alpha - beta) + 0.5 * (alpha + beta + 2.0) * u);
    }
    for n in 2..=degree {
        let nf = n as f64;
        let a1 = 2.0 * nf * (nf + alpha + beta) * (2.0 * nf + alpha + beta - 2.0);
        let a2 = 2.0 * nf + alpha + beta - 1.0;
        let a3 = (2.0 * nf + alpha + beta) * (2.0 * nf + alpha + beta - 2.0);
        let a4 = 2.0 * (nf + alpha - 1.0) * (nf + beta - 1.0) * (2.0 * nf + alpha + beta);
        if a1.abs() < 1e-300 {
            p.push(0.0);
            continue;
        }
        let val = (a2 * (a3 * u + alpha * alpha - beta * beta) * p[n - 1] - a4 * p[n - 2]) / a1;
        p.push(val);
    }
    p
}

/// `[P'_0 … P'_d]`. `d/du P_n^{(α,β)} = ½(n+α+β+1) · P_{n-1}^{(α+1,β+1)}`; `P'_0 = 0`.
fn jacobi_derivs(u: f64, degree: usize, alpha: f64, beta: f64) -> Vec<f64> {
    let mut d = Vec::with_capacity(degree + 1);
    d.push(0.0);
    if degree >= 1 {
        // Values of the shifted family P^{(α+1,β+1)} up to degree-1.
        let shifted = jacobi_values(u, degree.saturating_sub(1), alpha + 1.0, beta + 1.0);
        for n in 1..=degree {
            let coeff = 0.5 * (n as f64 + alpha + beta + 1.0);
            d.push(coeff * shifted.get(n - 1).copied().unwrap_or(0.0));
        }
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() < tol
    }

    #[test]
    fn chebyshev_values_match_closed_form() {
        // T_0=1, T_1=u, T_2=2u²−1, T_3=4u³−3u at u=0.5.
        let u = 0.5;
        let v = chebyshev_values(u, 3);
        assert!(approx(v[0], 1.0, 1e-12));
        assert!(approx(v[1], 0.5, 1e-12));
        assert!(approx(v[2], 2.0 * u * u - 1.0, 1e-12));
        assert!(approx(v[3], 4.0 * u * u * u - 3.0 * u, 1e-12));
    }

    #[test]
    fn chebyshev_derivs_match_numeric() {
        // Compare analytic T'_k(u) against a central finite difference of T_k.
        let u = 0.37;
        let h = 1e-6;
        let d = chebyshev_derivs(u, 5);
        let plus = chebyshev_values(u + h, 5);
        let minus = chebyshev_values(u - h, 5);
        for k in 0..=5 {
            let num = (plus[k] - minus[k]) / (2.0 * h);
            assert!(
                approx(d[k], num, 1e-5),
                "T'_{k} mismatch: {} vs {num}",
                d[k]
            );
        }
    }

    #[test]
    fn edge_fn_recovers_known_polynomial() {
        // f set to exactly T_2 ⇒ f(x) = 2·tanh(x)² − 1.
        let f = KanEdgeFn::chebyshev(vec![0.0, 0.0, 1.0]);
        for &x in &[-2.0f64, -0.5, 0.0, 0.5, 2.0] {
            let u = x.tanh();
            assert!(approx(f.eval(x), 2.0 * u * u - 1.0, 1e-12));
        }
    }

    #[test]
    fn edge_fn_grad_coeffs_is_basis_vector() {
        let f = KanEdgeFn::chebyshev(vec![0.3, -0.7, 0.2, 0.1]);
        let x = 0.8;
        let g = f.grad_coeffs(x);
        let expect = chebyshev_values(x.tanh(), 3);
        for (a, b) in g.iter().zip(expect.iter()) {
            assert!(approx(*a, *b, 1e-12));
        }
    }

    #[test]
    fn edge_fn_grad_input_matches_numeric() {
        let f = KanEdgeFn::chebyshev(vec![0.5, -0.4, 0.3, -0.2, 0.1]);
        let x = 0.6;
        let h = 1e-6;
        let num = (f.eval(x + h) - f.eval(x - h)) / (2.0 * h);
        assert!(approx(f.grad_input(x), num, 1e-5));
    }

    #[test]
    fn jacobi_reduces_to_legendre_and_derivs_match_numeric() {
        // α=β=0 ⇒ Legendre. P_2(u)=½(3u²−1), P_3(u)=½(5u³−3u).
        let u = 0.42;
        let v = jacobi_values(u, 3, 0.0, 0.0);
        assert!(approx(v[2], 0.5 * (3.0 * u * u - 1.0), 1e-10));
        assert!(approx(v[3], 0.5 * (5.0 * u * u * u - 3.0 * u), 1e-10));
        // Analytic derivative vs finite difference.
        let h = 1e-6;
        let d = jacobi_derivs(u, 3, 0.0, 0.0);
        let plus = jacobi_values(u + h, 3, 0.0, 0.0);
        let minus = jacobi_values(u - h, 3, 0.0, 0.0);
        for k in 0..=3 {
            let num = (plus[k] - minus[k]) / (2.0 * h);
            assert!(approx(d[k], num, 1e-4), "P'_{k} mismatch");
        }
    }

    #[test]
    fn edge_fn_serde_roundtrip() {
        let f = KanEdgeFn::chebyshev(vec![1.0, 2.0, 3.0]);
        let s = serde_json::to_string(&f).unwrap();
        let back: KanEdgeFn = serde_json::from_str(&s).unwrap();
        assert_eq!(back.coeffs, f.coeffs);
        assert_eq!(back.basis, Basis::Chebyshev);
    }
}
