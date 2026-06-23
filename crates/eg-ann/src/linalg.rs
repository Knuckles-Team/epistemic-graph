//! Minimal dense linear algebra for the OPQ rotation update — dependency-free so
//! the Pi-lean contract holds (no nalgebra / BLAS). Only what OPQ needs:
//!   * a row-major `dim*dim` matrix multiply,
//!   * a one-sided Jacobi SVD,
//!   * the polar-factor `R = U Vᵀ` used to re-orthogonalise the rotation.
//!
//! `dim` is the embedding dimension (e.g. 768/1024); these run a handful of
//! times at BUILD time only, never per-query, so the O(dim³) cost is irrelevant
//! against k-means.

/// `c = a · b`, all row-major `n*n`.
pub fn matmul(a: &[f32], b: &[f32], n: usize) -> Vec<f32> {
    let mut c = vec![0.0f32; n * n];
    for i in 0..n {
        for k in 0..n {
            let aik = a[i * n + k];
            if aik == 0.0 {
                continue;
            }
            let brow = &b[k * n..(k + 1) * n];
            let crow = &mut c[i * n..(i + 1) * n];
            for j in 0..n {
                crow[j] += aik * brow[j];
            }
        }
    }
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
        if off < eps {
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
