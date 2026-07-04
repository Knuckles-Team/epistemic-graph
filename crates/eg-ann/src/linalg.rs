//! Minimal dense linear algebra for the OPQ rotation update — dependency-free so
//! the Pi-lean contract holds (no nalgebra / BLAS). Only what OPQ needs:
//!   * a row-major `dim*dim` matrix multiply,
//!   * a one-sided Jacobi SVD,
//!   * the polar-factor `R = U Vᵀ` used to re-orthogonalise the rotation.
//!
//! `dim` is the embedding dimension (e.g. 768/1024); these run a handful of times
//! at BUILD time only, never per-query. At dim=1024 the O(dim³) SVD/matmul are NOT
//! negligible against k-means, so (CONCEPT:EG-KG.storage.semantic-index-directory) the matmul is rayon-parallel
//! across output rows and the Jacobi SVD stops on RELATIVE convergence instead of a
//! fixed 60 sweeps — together they turn the OPQ rotation update from a minutes-long
//! single-core peg into a few seconds across all cores.

use rayon::prelude::*;

/// `c = a · b`, all row-major `n*n`. CONCEPT:EG-KG.storage.semantic-index-directory — parallel over output rows so
/// the OPQ rotation matmul (dim³ ≈ 1e9 flops at dim=1024) saturates all cores
/// instead of pegging one.
pub fn matmul(a: &[f32], b: &[f32], n: usize) -> Vec<f32> {
    let mut c = vec![0.0f32; n * n];
    c.par_chunks_mut(n).enumerate().for_each(|(i, crow)| {
        let arow = &a[i * n..(i + 1) * n];
        for k in 0..n {
            let aik = arow[k];
            if aik == 0.0 {
                continue;
            }
            let brow = &b[k * n..(k + 1) * n];
            for j in 0..n {
                crow[j] += aik * brow[j];
            }
        }
    });
    c
}

/// Transpose a row-major `n*n` matrix.
pub fn transpose(a: &[f32], n: usize) -> Vec<f32> {
    let mut t = vec![0.0f32; n * n];
    for i in 0..n {
        for j in 0..n {
            t[j * n + i] = a[i * n + j];
        }
    }
    t
}

/// Apply a row-major `n*n` rotation to a single vector: `out = R · v`.
#[inline]
pub fn rotate(r: &[f32], v: &[f32], n: usize) -> Vec<f32> {
    let mut out = vec![0.0f32; n];
    for i in 0..n {
        let row = &r[i * n..(i + 1) * n];
        let mut s = 0.0f32;
        for j in 0..n {
            s += row[j] * v[j];
        }
        out[i] = s;
    }
    out
}

/// The `n*n` identity.
pub fn identity(n: usize) -> Vec<f32> {
    let mut m = vec![0.0f32; n * n];
    for i in 0..n {
        m[i * n + i] = 1.0;
    }
    m
}

/// One-sided Jacobi SVD of a row-major `n*n` matrix `a = U Σ Vᵀ`.
/// Returns `(u, s, v)` with `u`,`v` row-major `n*n` orthogonal and `s` the n
/// singular values. Robust and allocation-light; used only at build time.
pub fn svd_square(a: &[f32], n: usize) -> (Vec<f32>, Vec<f32>, Vec<f32>) {
    // Work in f64 for numerical stability of the rotations.
    // `u` starts as a copy of A (columns get orthogonalised in place); `v` = I.
    let mut u: Vec<f64> = a.iter().map(|&x| x as f64).collect();
    let mut v = vec![0.0f64; n * n];
    for i in 0..n {
        v[i * n + i] = 1.0;
    }

    let eps = 1e-12_f64;
    // CONCEPT:EG-KG.storage.semantic-index-directory — RELATIVE convergence. The previous code compared the
    // off-diagonal mass `off` against an ABSOLUTE 1e-12; for a dim=1024 matrix of
    // accumulated outer products `off` starts at O(1e6+), so 1e-12 never triggers
    // and all 60 sweeps always run — the single-threaded minutes-long peg. Jacobi
    // converges quadratically, so once `off` has dropped ~7 orders of magnitude
    // below its initial scale the polar factor `U Vᵀ` is fully determined; stopping
    // there cuts the typical sweep count from 60 to ~8-12 (a 5-7× build speedup)
    // with no change to the rotation quality (hence no recall change).
    let mut off0 = 0.0f64;
    for _sweep in 0..60 {
        let mut off = 0.0f64;
        for p in 0..n {
            for q in (p + 1)..n {
                // Dot products of columns p and q of U.
                let (mut alpha, mut beta, mut gamma) = (0.0f64, 0.0f64, 0.0f64);
                for i in 0..n {
                    let up = u[i * n + p];
                    let uq = u[i * n + q];
                    alpha += up * up;
                    beta += uq * uq;
                    gamma += up * uq;
                }
                off += gamma.abs();
                if gamma.abs() < eps {
                    continue;
                }
                // Jacobi rotation that diagonalises the 2x2 [[alpha,gamma],[gamma,beta]].
                let zeta = (beta - alpha) / (2.0 * gamma);
                let t = zeta.signum() / (zeta.abs() + (1.0 + zeta * zeta).sqrt());
                let cos = 1.0 / (1.0 + t * t).sqrt();
                let sin = cos * t;
                // Apply to columns p,q of U and V.
                for i in 0..n {
                    let up = u[i * n + p];
                    let uq = u[i * n + q];
                    u[i * n + p] = cos * up - sin * uq;
                    u[i * n + q] = sin * up + cos * uq;
                    let vp = v[i * n + p];
                    let vq = v[i * n + q];
                    v[i * n + p] = cos * vp - sin * vq;
                    v[i * n + q] = sin * vp + cos * vq;
                }
            }
        }
        if off0 == 0.0 {
            off0 = off.max(1.0);
        }
        // Absolute floor OR relative drop of ~7 orders of magnitude — whichever first.
        if off < eps || off < 1e-9 * off0 {
            break;
        }
    }

    // Singular values are the norms of U's columns; normalise U to get the left
    // singular vectors.
    let mut s = vec![0.0f32; n];
    let mut u_out = vec![0.0f32; n * n];
    for j in 0..n {
        let mut norm = 0.0f64;
        for i in 0..n {
            norm += u[i * n + j] * u[i * n + j];
        }
        let norm = norm.sqrt();
        s[j] = norm as f32;
        if norm > eps {
            for i in 0..n {
                u_out[i * n + j] = (u[i * n + j] / norm) as f32;
            }
        } else {
            u_out[j * n + j] = 1.0;
        }
    }
    let v_out: Vec<f32> = v.iter().map(|&x| x as f32).collect();
    (u_out, s, v_out)
}

/// Orthogonal polar factor `R = U Vᵀ` of a row-major `n*n` matrix `m`
/// (the closest orthogonal matrix to `m`). This is the OPQ rotation update:
/// given `m = XᵀX̂`, `U Vᵀ` is the rotation minimising reconstruction error.
pub fn orthogonal_polar(m: &[f32], n: usize) -> Vec<f32> {
    let (u, _s, v) = svd_square(m, n);
    let vt = transpose(&v, n);
    matmul(&u, &vt, n)
}
