//! LAPACK-class linear algebra for the NumPy-compatible surface. Backed by
//! `nalgebra`'s pure-Rust decompositions; no system BLAS/LAPACK or native toolchain
//! is required.

use crate::error::{NumericError, Result};
use nalgebra::{DMatrix, DVector, SymmetricEigen};
use ndarray::{Array1, Array2, ArrayView1, ArrayView2};

fn nd_to_na(a: ArrayView2<'_, f64>) -> DMatrix<f64> {
    let (rows, cols) = a.dim();
    DMatrix::from_fn(rows, cols, |row, col| a[[row, col]])
}

fn na_to_nd(matrix: &DMatrix<f64>) -> Array2<f64> {
    Array2::from_shape_fn((matrix.nrows(), matrix.ncols()), |(row, col)| {
        matrix[(row, col)]
    })
}

fn require_square(a: ArrayView2<'_, f64>, operation: &str) -> Result<usize> {
    let (rows, cols) = a.dim();
    if rows != cols {
        return Err(NumericError::linalg(format!(
            "{operation}: matrix must be square"
        )));
    }
    Ok(rows)
}

/// NumPy `linalg.norm(v)` — vector L2 norm.
pub fn norm(v: ArrayView1<'_, f64>) -> f64 {
    v.dot(&v).sqrt()
}

/// NumPy `linalg.norm(v, ord)` for the common vector orders.
pub fn norm_ord(v: ArrayView1<'_, f64>, ord: f64) -> f64 {
    if ord.is_infinite() {
        if ord > 0.0 {
            v.iter().fold(0.0f64, |acc, &value| acc.max(value.abs()))
        } else {
            v.iter()
                .fold(f64::INFINITY, |acc, &value| acc.min(value.abs()))
        }
    } else if ord == 1.0 {
        v.iter().map(|value| value.abs()).sum()
    } else if ord == 2.0 {
        norm(v)
    } else {
        v.iter()
            .map(|value| value.abs().powf(ord))
            .sum::<f64>()
            .powf(1.0 / ord)
    }
}

/// L2-normalize one vector. A zero vector is returned unchanged.
pub fn l2_normalize_slice(v: &[f64]) -> Vec<f64> {
    let magnitude = norm(ArrayView1::from(v));
    if magnitude == 0.0 {
        v.to_vec()
    } else {
        v.iter().map(|value| value / magnitude).collect()
    }
}

/// L2-normalize every row in a batch.
pub fn batch_l2_normalize(vectors: &[Vec<f64>]) -> Vec<Vec<f64>> {
    vectors
        .iter()
        .map(|vector| l2_normalize_slice(vector))
        .collect()
}

/// NumPy `dot` for 1-D vectors.
pub fn dot(a: ArrayView1<'_, f64>, b: ArrayView1<'_, f64>) -> Result<f64> {
    if a.len() != b.len() {
        return Err(NumericError::shape("dot: shape mismatch"));
    }
    Ok(a.dot(&b))
}

/// NumPy `matmul` for 2-D matrices.
pub fn matmul(a: ArrayView2<'_, f64>, b: ArrayView2<'_, f64>) -> Result<Array2<f64>> {
    if a.ncols() != b.nrows() {
        return Err(NumericError::shape("matmul: shape mismatch"));
    }
    Ok(na_to_nd(&(nd_to_na(a) * nd_to_na(b))))
}

/// NumPy `linalg.solve(A, b)` using partial-pivot LU.
pub fn solve(a: ArrayView2<'_, f64>, b: ArrayView1<'_, f64>) -> Result<Array1<f64>> {
    let order = require_square(a, "solve")?;
    if b.len() != order {
        return Err(NumericError::shape("solve: b length mismatch"));
    }
    let rhs = DVector::from_iterator(order, b.iter().copied());
    let solution = nd_to_na(a)
        .lu()
        .solve(&rhs)
        .ok_or_else(|| NumericError::linalg("Singular matrix"))?;
    Ok(Array1::from_iter(solution.iter().copied()))
}

/// Singular values in descending order.
pub fn svdvals(a: ArrayView2<'_, f64>) -> Vec<f64> {
    nd_to_na(a)
        .svd(false, false)
        .singular_values
        .iter()
        .copied()
        .collect()
}

/// Full SVD `(U, s, Vt)`, matching NumPy's default `full_matrices=True` shapes.
pub fn svd(a: ArrayView2<'_, f64>) -> Result<(Array2<f64>, Array1<f64>, Array2<f64>)> {
    let (rows, cols) = a.dim();
    let decomposition = nd_to_na(a).svd(true, true);
    let u_thin = decomposition
        .u
        .ok_or_else(|| NumericError::linalg("SVD did not produce U"))?;
    let vt_thin = decomposition
        .v_t
        .ok_or_else(|| NumericError::linalg("SVD did not produce Vt"))?;
    let u = complete_orthonormal_basis(&u_thin, rows);
    let v = complete_orthonormal_basis(&vt_thin.transpose(), cols);
    Ok((
        na_to_nd(&u),
        Array1::from_iter(decomposition.singular_values.iter().copied()),
        na_to_nd(&v.transpose()),
    ))
}

/// Complete a thin orthonormal column basis with deterministic standard-basis
/// vectors. Modified Gram-Schmidt keeps the public full-SVD shape contract.
fn complete_orthonormal_basis(thin: &DMatrix<f64>, size: usize) -> DMatrix<f64> {
    if thin.ncols() == size {
        return thin.clone();
    }
    let mut columns: Vec<DVector<f64>> = thin
        .column_iter()
        .map(|column| column.into_owned())
        .collect();
    for axis in 0..size {
        if columns.len() == size {
            break;
        }
        let mut candidate = DVector::zeros(size);
        candidate[axis] = 1.0;
        for column in &columns {
            candidate -= column * column.dot(&candidate);
        }
        let magnitude = candidate.norm();
        if magnitude > 64.0 * f64::EPSILON {
            columns.push(candidate / magnitude);
        }
    }
    DMatrix::from_columns(&columns)
}

/// NumPy `linalg.eigh(A)` for a symmetric matrix. Results are sorted ascending.
pub fn eigh(a: ArrayView2<'_, f64>) -> Result<(Array1<f64>, Array2<f64>)> {
    let order = require_square(a, "eigh")?;
    let decomposition = SymmetricEigen::new(nd_to_na(a));
    let mut indices: Vec<usize> = (0..order).collect();
    indices.sort_by(|&left, &right| {
        decomposition.eigenvalues[left].total_cmp(&decomposition.eigenvalues[right])
    });
    let values = Array1::from_shape_fn(order, |index| decomposition.eigenvalues[indices[index]]);
    let vectors = Array2::from_shape_fn((order, order), |(row, col)| {
        decomposition.eigenvectors[(row, indices[col])]
    });
    Ok((values, vectors))
}

/// The `k` smallest-magnitude eigenpairs of a symmetric matrix.
pub fn eigsh_smallest(a: ArrayView2<'_, f64>, k: usize) -> Result<(Array1<f64>, Array2<f64>)> {
    let order = require_square(a, "eigsh")?;
    if k == 0 {
        return Err(NumericError::shape("eigsh: k must be >= 1"));
    }
    if k > order {
        return Err(NumericError::shape(
            "eigsh: k must be <= n (the matrix order)",
        ));
    }
    let (values, vectors) = eigh(a)?;
    let mut indices: Vec<usize> = (0..order).collect();
    indices.sort_by(|&left, &right| values[left].abs().total_cmp(&values[right].abs()));
    indices.truncate(k);
    indices.sort_by(|&left, &right| values[left].total_cmp(&values[right]));
    Ok((
        Array1::from_shape_fn(k, |index| values[indices[index]]),
        Array2::from_shape_fn((order, k), |(row, col)| vectors[[row, indices[col]]]),
    ))
}

/// NumPy `linalg.pinv(A)` via SVD.
pub fn pinv(a: ArrayView2<'_, f64>) -> Result<Array2<f64>> {
    let (rows, cols) = a.dim();
    let decomposition = nd_to_na(a).svd(true, true);
    let largest = decomposition
        .singular_values
        .iter()
        .copied()
        .fold(0.0f64, f64::max);
    let tolerance = (rows.max(cols) as f64) * f64::EPSILON * largest;
    let inverse = decomposition
        .pseudo_inverse(tolerance)
        .map_err(|error| NumericError::linalg(error))?;
    Ok(na_to_nd(&inverse))
}

/// NumPy `linalg.lstsq(A, b)` minimum-norm solution.
pub fn lstsq(a: ArrayView2<'_, f64>, b: ArrayView1<'_, f64>) -> Result<Array1<f64>> {
    if b.len() != a.nrows() {
        return Err(NumericError::shape("lstsq: b length mismatch"));
    }
    let inverse = pinv(a)?;
    let rhs = b.to_owned();
    Ok(inverse.dot(&rhs))
}

/// NumPy reduced QR decomposition.
pub fn qr(a: ArrayView2<'_, f64>) -> Result<(Array2<f64>, Array2<f64>)> {
    let decomposition = nd_to_na(a).qr();
    Ok((na_to_nd(&decomposition.q()), na_to_nd(&decomposition.r())))
}

/// NumPy lower-triangular Cholesky factor.
pub fn cholesky(a: ArrayView2<'_, f64>) -> Result<Array2<f64>> {
    require_square(a, "cholesky")?;
    let decomposition = nalgebra::linalg::Cholesky::new(nd_to_na(a))
        .ok_or_else(|| NumericError::linalg("Matrix is not positive definite"))?;
    Ok(na_to_nd(&decomposition.l()))
}

/// NumPy `linalg.det(A)`.
pub fn det(a: ArrayView2<'_, f64>) -> Result<f64> {
    let order = require_square(a, "det")?;
    if order == 0 {
        return Ok(1.0);
    }
    Ok(nd_to_na(a).lu().determinant())
}

/// NumPy `linalg.matrix_power(A, p)`, including negative powers.
pub fn matrix_power(a: ArrayView2<'_, f64>, p: i64) -> Result<Array2<f64>> {
    let order = require_square(a, "matrix_power")?;
    if p == 0 {
        return Ok(Array2::eye(order));
    }
    let (mut base, mut exponent) = if p < 0 {
        (inverse(a)?, p.unsigned_abs())
    } else {
        (a.to_owned(), p as u64)
    };
    let mut result = Array2::eye(order);
    while exponent > 0 {
        if exponent & 1 == 1 {
            result = result.dot(&base);
        }
        exponent >>= 1;
        if exponent > 0 {
            base = base.dot(&base);
        }
    }
    Ok(result)
}

/// NumPy `linalg.inv(A)`.
pub fn inverse(a: ArrayView2<'_, f64>) -> Result<Array2<f64>> {
    require_square(a, "inv")?;
    let inverse = nd_to_na(a)
        .lu()
        .try_inverse()
        .ok_or_else(|| NumericError::linalg("Singular matrix"))?;
    Ok(na_to_nd(&inverse))
}
