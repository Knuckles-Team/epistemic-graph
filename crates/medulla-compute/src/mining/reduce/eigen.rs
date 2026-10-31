//! Deterministic Jacobi eigendecomposition for symmetric matrices.

/// Jacobi eigenvalue algorithm for a symmetric matrix. Returns `(eigenvalues,
/// eigenvectors)` where `eigenvectors[d][j]` is component `d` of eigenvector `j`
/// (columns are eigenvectors). Dependency-free + deterministic; the matrices here are
/// feature-dimension sized (small), so O(iters·d³) is fine.
pub(super) fn jacobi_eigen(matrix: &[Vec<f64>]) -> (Vec<f64>, Vec<Vec<f64>>) {
    let n = matrix.len();
    let mut a: Vec<Vec<f64>> = matrix.to_vec();
    let mut v = vec![vec![0.0f64; n]; n];
    for (i, row) in v.iter_mut().enumerate() {
        row[i] = 1.0;
    }
    for _ in 0..100 {
        let (p, q, off) = largest_off_diagonal(&a);
        if off < 1e-12 || n < 2 {
            break;
        }
        let (c, s) = rotation_parameters(a[p][p], a[q][q], a[p][q]);
        rotate_columns(&mut a, p, q, c, s);
        rotate_rows(&mut a, p, q, c, s);
        rotate_eigenvectors(&mut v, p, q, c, s);
    }
    let eigvals = diagonal_values(&a);
    (eigvals, v)
}

fn largest_off_diagonal(a: &[Vec<f64>]) -> (usize, usize, f64) {
    let (mut p, mut q, mut off) = (0usize, 1usize, 0.0f64);
    for (i, arow) in a.iter().enumerate() {
        for (j, &aij) in arow.iter().enumerate().skip(i + 1) {
            if aij.abs() > off {
                off = aij.abs();
                p = i;
                q = j;
            }
        }
    }
    (p, q, off)
}

fn rotation_parameters(app: f64, aqq: f64, apq: f64) -> (f64, f64) {
    let theta = 0.5 * (aqq - app) / apq;
    let t = theta.signum() / (theta.abs() + (theta * theta + 1.0).sqrt());
    let c = 1.0 / (t * t + 1.0).sqrt();
    let s = t * c;
    (c, s)
}

fn rotate_columns(a: &mut [Vec<f64>], p: usize, q: usize, c: f64, s: f64) {
    for arow in a.iter_mut() {
        let aip = arow[p];
        let aiq = arow[q];
        arow[p] = c * aip - s * aiq;
        arow[q] = s * aip + c * aiq;
    }
}

fn rotate_rows(a: &mut [Vec<f64>], p: usize, q: usize, c: f64, s: f64) {
    let (left, right) = a.split_at_mut(q);
    let rp = &mut left[p];
    let rq = &mut right[0];
    for (rpi, rqi) in rp.iter_mut().zip(rq.iter_mut()) {
        let api = *rpi;
        let aqi = *rqi;
        *rpi = c * api - s * aqi;
        *rqi = s * api + c * aqi;
    }
}

fn rotate_eigenvectors(v: &mut [Vec<f64>], p: usize, q: usize, c: f64, s: f64) {
    for vrow in v.iter_mut() {
        let vip = vrow[p];
        let viq = vrow[q];
        vrow[p] = c * vip - s * viq;
        vrow[q] = s * vip + c * viq;
    }
}

fn diagonal_values(a: &[Vec<f64>]) -> Vec<f64> {
    (0..a.len()).map(|i| a[i][i]).collect()
}
