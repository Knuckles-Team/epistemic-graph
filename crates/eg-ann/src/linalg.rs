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

mod svd;

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
    svd::svd_square(a, n)
}

/// Orthogonal polar factor `R = U Vᵀ` of a row-major `n*n` matrix `m`
/// (the closest orthogonal matrix to `m`). This is the OPQ rotation update:
/// given `m = XᵀX̂`, `U Vᵀ` is the rotation minimising reconstruction error.
pub fn orthogonal_polar(m: &[f32], n: usize) -> Vec<f32> {
    let (u, _s, v) = svd_square(m, n);
    let vt = transpose(&v, n);
    matmul(&u, &vt, n)
}
