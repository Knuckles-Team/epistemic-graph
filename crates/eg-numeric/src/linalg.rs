//! LAPACK-class linalg — the "hard 6" numpy surface: `norm/dot/matmul/solve/svd/
//! eigh/pinv/lstsq/qr/cholesky/det/matrix_power`. Backed by **faer** (pure Rust,
//! NO system BLAS/LAPACK). Parity-tested exact vs numpy (PoC-validated).

use crate::error::{NumericError, Result};
use faer::prelude::*;
use faer::Side;
use ndarray::{Array1, Array2, ArrayView1, ArrayView2};

// ---- ndarray <-> faer bridges ----

fn nd_to_faer(a: ArrayView2<f64>) -> Mat<f64> {
    let (m, n) = a.dim();
    Mat::from_fn(m, n, |i, j| a[[i, j]])
}

fn faer_to_nd(m: MatRef<f64>) -> Array2<f64> {
    let (r, c) = (m.nrows(), m.ncols());
    Array2::from_shape_fn((r, c), |(i, j)| m[(i, j)])
}

// ---- vector ops ----

/// numpy `linalg.norm(v)` — vector L2 norm.
pub fn norm(v: ArrayView1<f64>) -> f64 {
    v.dot(&v).sqrt()
}

/// numpy `linalg.norm(v, ord)` for the common vector orders (1, 2, inf).
pub fn norm_ord(v: ArrayView1<f64>, ord: f64) -> f64 {
    if ord.is_infinite() {
        if ord > 0.0 {
            v.iter().fold(0.0f64, |a, &x| a.max(x.abs()))
        } else {
            v.iter().fold(f64::INFINITY, |a, &x| a.min(x.abs()))
        }
    } else if ord == 1.0 {
        v.iter().map(|x| x.abs()).sum()
    } else if ord == 2.0 {
        norm(v)
    } else {
        v.iter()
            .map(|x| x.abs().powf(ord))
            .sum::<f64>()
            .powf(1.0 / ord)
    }
}

/// L2-normalize a single vector to its unit direction `v/‖v‖` (CONCEPT:EG-330). A
/// slice-in/`Vec`-out API so an engine caller WITHOUT `ndarray` in scope (e.g. the
/// server `Method` handler) can normalize a resident vector set through the kernel. A
/// zero-norm vector is returned unchanged (all-zero) — a safe divide, no NaN.
pub fn l2_normalize_slice(v: &[f64]) -> Vec<f64> {
    let n = norm(ArrayView1::from(v));
    if n == 0.0 {
        v.to_vec()
    } else {
        v.iter().map(|x| x / n).collect()
    }
}

/// L2-normalize every row of a batch (CONCEPT:EG-330) — the kernel behind the engine's
/// `BatchL2Normalize` Method (compute-near-data over a resident vector set). Each row is
/// unit-normalized by [`l2_normalize_slice`].
pub fn batch_l2_normalize(vectors: &[Vec<f64>]) -> Vec<Vec<f64>> {
    vectors.iter().map(|v| l2_normalize_slice(v)).collect()
}

/// numpy `dot` for 1-D vectors → scalar.
pub fn dot(a: ArrayView1<f64>, b: ArrayView1<f64>) -> Result<f64> {
    if a.len() != b.len() {
        return Err(NumericError::shape("dot: shape mismatch"));
    }
    Ok(a.dot(&b))
}

/// numpy `matmul`/`dot` for 2-D matrices.
pub fn matmul(a: ArrayView2<f64>, b: ArrayView2<f64>) -> Result<Array2<f64>> {
    if a.ncols() != b.nrows() {
        return Err(NumericError::shape("matmul: shape mismatch"));
    }
    let out = nd_to_faer(a) * nd_to_faer(b);
    Ok(faer_to_nd(out.as_ref()))
}

// ---- decompositions / solves ----

fn require_square(a: ArrayView2<f64>, what: &str) -> Result<usize> {
    let (m, n) = a.dim();
    if m != n {
        return Err(NumericError::linalg(format!(
            "{what}: matrix must be square"
        )));
    }
    Ok(m)
}

/// numpy `linalg.solve(A, b)` — square system, partial-pivot LU. Raises
/// LinAlgError on a singular matrix.
pub fn solve(a: ArrayView2<f64>, b: ArrayView1<f64>) -> Result<Array1<f64>> {
    let n = require_square(a, "solve")?;
    if b.len() != n {
        return Err(NumericError::shape("solve: b length mismatch"));
    }
    let af = nd_to_faer(a);
    // Detect singularity via the SVD condition (faer LU does not error on singular).
    let svals = af.singular_values();
    if svals
        .last()
        .map(|s| *s <= 1e-12 * svals[0].max(1.0))
        .unwrap_or(true)
    {
        return Err(NumericError::linalg("Singular matrix"));
    }
    let rhs = Mat::from_fn(n, 1, |i, _| b[i]);
    let x = af.partial_piv_lu().solve(&rhs);
    Ok(Array1::from_shape_fn(n, |i| x[(i, 0)]))
}

/// Singular values (descending) — numpy `linalg.svd(A, compute_uv=False)`.
pub fn svdvals(a: ArrayView2<f64>) -> Vec<f64> {
    nd_to_faer(a).singular_values()
}

/// Full SVD → (U, s, Vt) matching numpy `linalg.svd(A)` (Vt is V transposed).
pub fn svd(a: ArrayView2<f64>) -> Result<(Array2<f64>, Array1<f64>, Array2<f64>)> {
    let af = nd_to_faer(a);
    let svd = af.svd();
    let u = faer_to_nd(svd.u());
    let v = faer_to_nd(svd.v());
    let s_diag = svd.s_diagonal();
    let s = Array1::from_shape_fn(s_diag.nrows(), |i| s_diag[i]);
    // numpy returns Vt = V^H; V is real here so plain transpose.
    let vt = v.reversed_axes().to_owned();
    Ok((u, s, vt))
}

/// numpy `linalg.eigh(A)` for a symmetric matrix → (eigenvalues ascending,
/// eigenvectors as columns).
pub fn eigh(a: ArrayView2<f64>) -> Result<(Array1<f64>, Array2<f64>)> {
    let n = require_square(a, "eigh")?;
    let af = nd_to_faer(a);
    let eig = af.selfadjoint_eigendecomposition(Side::Lower);
    let s = eig.s().column_vector();
    let w = Array1::from_shape_fn(n, |i| s[i]);
    let v = faer_to_nd(eig.u());
    // faer returns ascending eigenvalues, matching numpy's eigh.
    Ok((w, v))
}

/// numpy `linalg.pinv(A)` — Moore-Penrose pseudoinverse via SVD.
pub fn pinv(a: ArrayView2<f64>) -> Result<Array2<f64>> {
    let af = nd_to_faer(a);
    // THIN svd → U is (m, k), V is (n, k) with k = min(m, n), so the
    // reconstruction V·diag(1/s)·Uᵀ is dimensionally correct for non-square A.
    let svd = af.thin_svd();
    let u = svd.u(); // (m, k)
    let v = svd.v(); // (n, k)
    let s = svd.s_diagonal();
    let k = s.nrows();
    let smax = (0..k).map(|i| s[i]).fold(0.0f64, f64::max);
    // numpy rcond default = max(M,N)*eps
    let (m, n) = a.dim();
    let rcond = (m.max(n) as f64) * f64::EPSILON;
    let tol = rcond * smax;
    // pinv = V * diag(1/s) * U^T (only s > tol)
    let sinv = Mat::from_fn(k, k, |i, j| {
        if i == j && s[i] > tol {
            1.0 / s[i]
        } else {
            0.0
        }
    });
    let vmat = v.to_owned();
    let umat = u.to_owned();
    let out = vmat * sinv * umat.transpose();
    Ok(faer_to_nd(out.as_ref()))
}

/// numpy `linalg.lstsq(A, b)` least-squares solution (returns x only).
pub fn lstsq(a: ArrayView2<f64>, b: ArrayView1<f64>) -> Result<Array1<f64>> {
    let (m, _n) = a.dim();
    if b.len() != m {
        return Err(NumericError::shape("lstsq: b length mismatch"));
    }
    // x = pinv(A) @ b — SVD-based least squares (matches numpy's lstsq minimum-norm).
    let p = pinv(a)?;
    let mut out = Array1::zeros(p.nrows());
    for i in 0..p.nrows() {
        let mut acc = 0.0;
        for j in 0..p.ncols() {
            acc += p[[i, j]] * b[j];
        }
        out[i] = acc;
    }
    Ok(out)
}

/// numpy `linalg.qr(A)` → (Q, R) reduced.
pub fn qr(a: ArrayView2<f64>) -> Result<(Array2<f64>, Array2<f64>)> {
    let af = nd_to_faer(a);
    let qr = af.qr();
    let q = faer_to_nd(qr.compute_thin_q().as_ref());
    let r = faer_to_nd(qr.compute_thin_r().as_ref());
    Ok((q, r))
}

/// numpy `linalg.cholesky(A)` → lower-triangular L (A = L Lᵀ). LinAlgError if not PD.
pub fn cholesky(a: ArrayView2<f64>) -> Result<Array2<f64>> {
    let _n = require_square(a, "cholesky")?;
    let af = nd_to_faer(a);
    match af.cholesky(Side::Lower) {
        Ok(ch) => {
            let l = ch.compute_l();
            Ok(faer_to_nd(l.as_ref()))
        }
        Err(_) => Err(NumericError::linalg("Matrix is not positive definite")),
    }
}

/// numpy `linalg.det(A)`.
pub fn det(a: ArrayView2<f64>) -> Result<f64> {
    let n = require_square(a, "det")?;
    if n == 0 {
        return Ok(1.0);
    }
    Ok(nd_to_faer(a).determinant())
}

/// numpy `linalg.matrix_power(A, n)` for integer n (including negatives via inverse).
pub fn matrix_power(a: ArrayView2<f64>, p: i64) -> Result<Array2<f64>> {
    let n = require_square(a, "matrix_power")?;
    let identity = Array2::<f64>::eye(n);
    if p == 0 {
        return Ok(identity);
    }
    let (base, mut exp) = if p < 0 {
        // A^-k = (A^-1)^k ; invert via solve against identity.
        (inverse(a)?, (-p) as u64)
    } else {
        (a.to_owned(), p as u64)
    };
    // Exponentiation by squaring.
    let mut result = identity;
    let mut b = base;
    while exp > 0 {
        if exp & 1 == 1 {
            result = mm(result.view(), b.view());
        }
        exp >>= 1;
        if exp > 0 {
            b = mm(b.view(), b.view());
        }
    }
    Ok(result)
}

fn mm(a: ArrayView2<f64>, b: ArrayView2<f64>) -> Array2<f64> {
    faer_to_nd((nd_to_faer(a) * nd_to_faer(b)).as_ref())
}

/// numpy `linalg.inv(A)`.
pub fn inverse(a: ArrayView2<f64>) -> Result<Array2<f64>> {
    let n = require_square(a, "inv")?;
    let af = nd_to_faer(a);
    let svals = af.singular_values();
    if svals
        .last()
        .map(|s| *s <= 1e-12 * svals[0].max(1.0))
        .unwrap_or(true)
    {
        return Err(NumericError::linalg("Singular matrix"));
    }
    let identity = Mat::from_fn(n, n, |i, j| if i == j { 1.0 } else { 0.0 });
    let inv = af.partial_piv_lu().solve(&identity);
    Ok(faer_to_nd(inv.as_ref()))
}
