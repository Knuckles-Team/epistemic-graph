//! Dense linear algebra for the linear-Gaussian SCM's conditioning step: a small
//! pure-Rust Gauss-Jordan inverse (no `nalgebra`; this crate stays dependency-light).

/// Gauss-Jordan matrix inverse with partial pivoting, pure Rust (no `nalgebra` —
/// this crate stays dependency-light; `k` here is the evidence-set size, always
/// small in practice). Returns `None` if `m` is singular (within `1e-12`).
pub(super) fn invert_matrix(m: &[Vec<f64>]) -> Option<Vec<Vec<f64>>> {
    let n = m.len();
    if n == 0 {
        return Some(Vec::new());
    }
    let mut aug = augment_with_identity(m);
    for col in 0..n {
        let pivot_row = partial_pivot_row(&aug, col)?;
        if aug[pivot_row][col].abs() < 1e-12 {
            return None; // singular
        }
        aug.swap(col, pivot_row);
        normalize_pivot_row(&mut aug[col], col);
        eliminate_column(&mut aug, col);
    }
    Some(aug.into_iter().map(|row| row[n..].to_vec()).collect())
}

/// The augmented matrix `[A | I]` for an `n x n` matrix `A`.
fn augment_with_identity(m: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n = m.len();
    m.iter()
        .enumerate()
        .map(|(i, row)| {
            let mut r = row.clone();
            r.resize(2 * n, 0.0);
            r[n + i] = 1.0;
            r
        })
        .collect()
}

/// Partial pivot: the row at/below `col` with the largest-magnitude entry in `col`.
fn partial_pivot_row(aug: &[Vec<f64>], col: usize) -> Option<usize> {
    (col..aug.len()).max_by(|&a, &b| {
        aug[a][col]
            .abs()
            .partial_cmp(&aug[b][col].abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    })
}

/// Scale the pivot row so its `col` entry becomes 1.
fn normalize_pivot_row(row: &mut [f64], col: usize) {
    let pivot = row[col];
    for v in row.iter_mut() {
        *v /= pivot;
    }
}

/// Subtract multiples of the (normalized) pivot row `col` from every other row so
/// column `col` is zero outside the pivot.
fn eliminate_column(aug: &mut [Vec<f64>], col: usize) {
    let pivot_row_vals = aug[col].clone();
    for (row, row_vals) in aug.iter_mut().enumerate() {
        if row == col {
            continue;
        }
        let factor = row_vals[col];
        if factor != 0.0 {
            for (v, pivot_v) in row_vals.iter_mut().zip(pivot_row_vals.iter()) {
                *v -= factor * pivot_v;
            }
        }
    }
}
