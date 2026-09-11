use super::conversion::{norm_axis, py_array, to_f64_dyn};
use super::{MAX_INPUT_ELEMENTS, MAX_INPUT_RANK};
use ndarray::{ArrayD, ArrayViewD, Axis, IxDyn};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PySequence, PySequenceMethods};

// ── Array construction and shape manipulation (NE-249) ──────────────
//
// Before `b7d5825` these names existed on this module only because it did
// `m.add(name, numpy.getattr(name))` -- a NumPy passthrough behind an
// `eg-numeric` badge. That commit deleted the passthrough (correctly: the
// kernel must not import NumPy) but did not reimplement the surface, so
// parity was lost as collateral rather than as a decision. These are the
// native replacements.
//
// They return the SAME nested-builtin-list shape every other op here
// returns -- there is no array object, no dtype, and no NumPy anywhere on
// the path. "No array object" and "no way to construct data" were being
// conflated; only the first was ever the contract.

/// Parse a NumPy-style `shape` argument: an int, or a sequence of ints.
///
/// Bounded by the same rank/element limits as every other input, so a
/// shape argument can never be the thing that allocates without a ceiling.
fn to_shape(value: &Bound<'_, PyAny>) -> PyResult<Vec<usize>> {
    let dims: Vec<i64> = if let Ok(single) = value.extract::<i64>() {
        vec![single]
    } else {
        let sequence = value.cast::<PySequence>().map_err(|_| {
            PyValueError::new_err("shape must be an integer or a sequence of integers")
        })?;
        let length = sequence.len()?;
        if length > MAX_INPUT_RANK {
            return Err(PyValueError::new_err(format!(
                "shape exceeds the rank-{MAX_INPUT_RANK} limit"
            )));
        }
        let mut dims = Vec::with_capacity(length);
        for index in 0..length {
            dims.push(
                sequence
                    .get_item(index)?
                    .extract::<i64>()
                    .map_err(|_| PyValueError::new_err("shape entries must be integers"))?,
            );
        }
        dims
    };
    let mut shape = Vec::with_capacity(dims.len());
    let mut product: usize = 1;
    for dim in dims {
        if dim < 0 {
            return Err(PyValueError::new_err("shape entries must be non-negative"));
        }
        let dim = dim as usize;
        product = product
            .checked_mul(dim)
            .ok_or_else(|| PyValueError::new_err("shape overflows the element limit"))?;
        if product > MAX_INPUT_ELEMENTS {
            return Err(PyValueError::new_err(format!(
                "shape exceeds the {MAX_INPUT_ELEMENTS}-element limit"
            )));
        }
        shape.push(dim);
    }
    Ok(shape)
}

fn filled(py: Python<'_>, shape: &Bound<'_, PyAny>, value: f64) -> PyResult<Py<PyAny>> {
    let shape = to_shape(shape)?;
    py_array(py, ArrayD::from_elem(IxDyn(&shape), value))
}

#[pyfunction]
pub(super) fn zeros(py: Python<'_>, shape: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    filled(py, shape, 0.0)
}

#[pyfunction]
pub(super) fn ones(py: Python<'_>, shape: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    filled(py, shape, 1.0)
}

/// Deliberately zero-filled, not uninitialized.
///
/// NumPy's `empty` hands back whatever was in the allocation. Values here
/// cross into Python as real list elements, so "uninitialized" would mean
/// publishing arbitrary heap contents across a process boundary. The name
/// is kept for call-site parity; the guarantee is strictly stronger.
#[pyfunction]
pub(super) fn empty(py: Python<'_>, shape: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    filled(py, shape, 0.0)
}

#[pyfunction]
pub(super) fn full(
    py: Python<'_>,
    shape: &Bound<'_, PyAny>,
    fill_value: f64,
) -> PyResult<Py<PyAny>> {
    filled(py, shape, fill_value)
}

#[pyfunction]
#[pyo3(signature = (n, m=None, k=0))]
pub(super) fn eye(py: Python<'_>, n: i64, m: Option<i64>, k: i64) -> PyResult<Py<PyAny>> {
    if n < 0 || m.is_some_and(|value| value < 0) {
        return Err(PyValueError::new_err("eye dimensions must be non-negative"));
    }
    let rows = n as usize;
    let columns = m.unwrap_or(n) as usize;
    if rows
        .checked_mul(columns)
        .is_none_or(|total| total > MAX_INPUT_ELEMENTS)
    {
        return Err(PyValueError::new_err(format!(
            "eye exceeds the {MAX_INPUT_ELEMENTS}-element limit"
        )));
    }
    let mut out = ArrayD::<f64>::zeros(IxDyn(&[rows, columns]));
    for row in 0..rows {
        let column = row as i64 + k;
        if column >= 0 && (column as usize) < columns {
            out[[row, column as usize]] = 1.0;
        }
    }
    py_array(py, out)
}

/// `arange(stop)` / `arange(start, stop[, step])`, matching NumPy's
/// one-argument-is-`stop` convention.
#[pyfunction]
#[pyo3(signature = (start, stop=None, step=1.0))]
pub(super) fn arange(
    py: Python<'_>,
    start: f64,
    stop: Option<f64>,
    step: f64,
) -> PyResult<Py<PyAny>> {
    let (start, stop) = match stop {
        Some(stop) => (start, stop),
        None => (0.0, start),
    };
    if step == 0.0 || !step.is_finite() {
        return Err(PyValueError::new_err(
            "arange step must be non-zero and finite",
        ));
    }
    if !start.is_finite() || !stop.is_finite() {
        return Err(PyValueError::new_err("arange bounds must be finite"));
    }
    let span = (stop - start) / step;
    let count = if span <= 0.0 { 0 } else { span.ceil() as usize };
    if count > MAX_INPUT_ELEMENTS {
        return Err(PyValueError::new_err(format!(
            "arange exceeds the {MAX_INPUT_ELEMENTS}-element limit"
        )));
    }
    let values: Vec<f64> = (0..count)
        .map(|index| start + step * index as f64)
        .collect();
    py_array(py, ndarray::Array1::from_vec(values).into_dyn())
}

#[pyfunction]
#[pyo3(signature = (start, stop, num=50, endpoint=true))]
pub(super) fn linspace(
    py: Python<'_>,
    start: f64,
    stop: f64,
    num: i64,
    endpoint: bool,
) -> PyResult<Py<PyAny>> {
    if num < 0 {
        return Err(PyValueError::new_err("linspace num must be non-negative"));
    }
    let num = num as usize;
    if num > MAX_INPUT_ELEMENTS {
        return Err(PyValueError::new_err(format!(
            "linspace exceeds the {MAX_INPUT_ELEMENTS}-element limit"
        )));
    }
    if !start.is_finite() || !stop.is_finite() {
        return Err(PyValueError::new_err("linspace bounds must be finite"));
    }
    let divisor = if endpoint { num.saturating_sub(1) } else { num };
    let values: Vec<f64> = (0..num)
        .map(|index| {
            if divisor == 0 {
                start
            } else {
                start + (stop - start) * index as f64 / divisor as f64
            }
        })
        .collect();
    py_array(py, ndarray::Array1::from_vec(values).into_dyn())
}

/// Validate and normalize a scalar/sequence tree into the canonical nested
/// builtin-list form -- the boundary's equivalent of `numpy.array`.
///
/// Not an identity function: it enforces rectangularity, the rank and
/// element ceilings, and float coercion, so a caller that round-trips
/// through it is guaranteed to hold something every other op here accepts.
#[pyfunction]
pub(super) fn array(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    py_array(py, to_f64_dyn(a)?)
}

#[pyfunction]
pub(super) fn asarray(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    py_array(py, to_f64_dyn(a)?)
}

#[pyfunction]
#[pyo3(signature = (a, b, rtol=1e-05, atol=1e-08, equal_nan=false))]
pub(super) fn isclose(
    py: Python<'_>,
    a: &Bound<'_, PyAny>,
    b: &Bound<'_, PyAny>,
    rtol: f64,
    atol: f64,
    equal_nan: bool,
) -> PyResult<Py<PyAny>> {
    let left = to_f64_dyn(a)?;
    let right = to_f64_dyn(b)?;
    if left.shape() != right.shape() {
        return Err(PyValueError::new_err("isclose: shape mismatch"));
    }
    let flags: Vec<bool> = left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| {
            if x.is_nan() || y.is_nan() {
                return equal_nan && x.is_nan() && y.is_nan();
            }
            if x.is_infinite() || y.is_infinite() {
                return x == y;
            }
            (x - y).abs() <= atol + rtol * y.abs()
        })
        .collect();
    let shape = left.shape().to_vec();
    py_array(
        py,
        ArrayD::from_shape_vec(IxDyn(&shape), flags)
            .map_err(|error| PyValueError::new_err(format!("invalid shape: {error}")))?,
    )
}

/// Collect a Python sequence-of-arrays argument into owned ndarrays.
fn to_f64_dyn_list(seq: &Bound<'_, PyAny>) -> PyResult<Vec<ArrayD<f64>>> {
    let sequence = seq
        .cast::<PySequence>()
        .map_err(|_| PyValueError::new_err("expected a sequence of numeric sequences"))?;
    let length = sequence.len()?;
    if length == 0 {
        return Err(PyValueError::new_err("need at least one array to stack"));
    }
    let mut parts = Vec::with_capacity(length);
    for index in 0..length {
        parts.push(to_f64_dyn(&sequence.get_item(index)?)?);
    }
    Ok(parts)
}

#[pyfunction]
#[pyo3(signature = (arrays, axis=0))]
pub(super) fn concatenate(
    py: Python<'_>,
    arrays: &Bound<'_, PyAny>,
    axis: isize,
) -> PyResult<Py<PyAny>> {
    let parts = to_f64_dyn_list(arrays)?;
    let ndim = parts[0].ndim();
    let axis = norm_axis(Some(axis), ndim)?
        .ok_or_else(|| PyValueError::new_err("concatenate requires an axis"))?;
    let views: Vec<ArrayViewD<'_, f64>> = parts.iter().map(|part| part.view()).collect();
    ndarray::concatenate(Axis(axis), &views)
        .map_err(|error| PyValueError::new_err(format!("concatenate: {error}")))
        .and_then(|joined| py_array(py, joined))
}

#[pyfunction]
pub(super) fn reshape(
    py: Python<'_>,
    a: &Bound<'_, PyAny>,
    shape: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let input = to_f64_dyn(a)?;
    let shape = to_shape(shape)?;
    let requested: usize = shape.iter().product();
    if requested != input.len() {
        return Err(PyValueError::new_err(format!(
            "cannot reshape array of size {} into shape with {requested} elements",
            input.len()
        )));
    }
    let values: Vec<f64> = input.iter().copied().collect();
    py_array(
        py,
        ArrayD::from_shape_vec(IxDyn(&shape), values)
            .map_err(|error| PyValueError::new_err(format!("invalid shape: {error}")))?,
    )
}

#[pyfunction]
#[pyo3(signature = (arrays, axis=0))]
pub(super) fn stack(py: Python<'_>, arrays: &Bound<'_, PyAny>, axis: isize) -> PyResult<Py<PyAny>> {
    let parts = to_f64_dyn_list(arrays)?;
    let axis = norm_axis(Some(axis), parts[0].ndim() + 1)?
        .ok_or_else(|| PyValueError::new_err("stack requires an axis"))?;
    let views: Vec<ArrayViewD<'_, f64>> = parts.iter().map(|part| part.view()).collect();
    ndarray::stack(Axis(axis), &views)
        .map_err(|error| PyValueError::new_err(format!("stack: {error}")))
        .and_then(|stacked| py_array(py, stacked))
}

/// Row-wise stack. 1-D inputs are promoted to single rows first, matching
/// NumPy, so `vstack([[1,2],[3,4]])` is a 2x2 rather than an error.
#[pyfunction]
pub(super) fn vstack(py: Python<'_>, arrays: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let parts = to_f64_dyn_list(arrays)?;
    let promoted: Vec<ArrayD<f64>> = parts
        .into_iter()
        .map(|part| {
            if part.ndim() == 1 {
                let length = part.len();
                let values: Vec<f64> = part.iter().copied().collect();
                ArrayD::from_shape_vec(IxDyn(&[1, length]), values)
                    .map_err(|error| PyValueError::new_err(format!("invalid shape: {error}")))
            } else {
                Ok(part)
            }
        })
        .collect::<PyResult<Vec<_>>>()?;
    let views: Vec<ArrayViewD<'_, f64>> = promoted.iter().map(|part| part.view()).collect();
    ndarray::concatenate(Axis(0), &views)
        .map_err(|error| PyValueError::new_err(format!("vstack: {error}")))
        .and_then(|joined| py_array(py, joined))
}

/// `diag` is NumPy's overloaded pair: extract a diagonal from a 2-D input,
/// or build a diagonal matrix from a 1-D one.
#[pyfunction]
#[pyo3(signature = (a, k=0))]
pub(super) fn diag(py: Python<'_>, a: &Bound<'_, PyAny>, k: i64) -> PyResult<Py<PyAny>> {
    let input = to_f64_dyn(a)?;
    let output = match input.ndim() {
        1 => diagonal_matrix(input, k)?,
        2 => diagonal_values(&input, k),
        _ => Err(PyValueError::new_err(
            "diag expects a 1- or 2-dimensional input",
        ))?,
    };
    py_array(py, output)
}

fn diagonal_position(index: usize, k: i64) -> (usize, usize) {
    if k >= 0 {
        (index, index + k as usize)
    } else {
        (index + k.unsigned_abs() as usize, index)
    }
}

fn diagonal_matrix(input: ArrayD<f64>, k: i64) -> PyResult<ArrayD<f64>> {
    let values: Vec<f64> = input.iter().copied().collect();
    let side = values.len() + k.unsigned_abs() as usize;
    if side
        .checked_mul(side)
        .is_none_or(|total| total > MAX_INPUT_ELEMENTS)
    {
        return Err(PyValueError::new_err(format!(
            "diag exceeds the {MAX_INPUT_ELEMENTS}-element limit"
        )));
    }
    let mut output = ArrayD::<f64>::zeros(IxDyn(&[side, side]));
    for (index, value) in values.into_iter().enumerate() {
        let (row, column) = diagonal_position(index, k);
        output[[row, column]] = value;
    }
    Ok(output)
}

fn diagonal_values(input: &ArrayD<f64>, k: i64) -> ArrayD<f64> {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    let mut values = Vec::new();
    let mut index = 0usize;
    loop {
        let (row, column) = diagonal_position(index, k);
        if row >= rows || column >= columns {
            break;
        }
        values.push(input[[row, column]]);
        index += 1;
    }
    ndarray::Array1::from_vec(values).into_dyn()
}

/// Returns a NEW value rather than mutating in place.
///
/// NumPy's `fill_diagonal` mutates its argument; a nested Python list that
/// crossed this boundary is a copy, so in-place mutation could not be
/// observed by the caller anyway. Returning the result makes that explicit
/// instead of silently doing nothing.
#[pyfunction]
pub(super) fn fill_diagonal(
    py: Python<'_>,
    a: &Bound<'_, PyAny>,
    value: f64,
) -> PyResult<Py<PyAny>> {
    let mut input = to_f64_dyn(a)?;
    if input.ndim() != 2 {
        return Err(PyValueError::new_err(
            "fill_diagonal expects a two-dimensional input",
        ));
    }
    let side = input.shape()[0].min(input.shape()[1]);
    for index in 0..side {
        input[[index, index]] = value;
    }
    py_array(py, input)
}

#[pyfunction]
#[pyo3(signature = (a, n=1, axis=-1))]
pub(super) fn diff(
    py: Python<'_>,
    a: &Bound<'_, PyAny>,
    n: i64,
    axis: isize,
) -> PyResult<Py<PyAny>> {
    if n < 0 {
        return Err(PyValueError::new_err("diff order must be non-negative"));
    }
    let mut current = to_f64_dyn(a)?;
    let axis = norm_axis(Some(axis), current.ndim())?
        .ok_or_else(|| PyValueError::new_err("diff requires an axis"))?;
    for _ in 0..n {
        let length = current.shape()[axis];
        if length == 0 {
            break;
        }
        let head = current.slice_axis(Axis(axis), (1..length).into());
        let tail = current.slice_axis(Axis(axis), (0..length - 1).into());
        current = &head - &tail;
    }
    py_array(py, current)
}

#[pyfunction]
#[pyo3(signature = (a, axis=-1))]
pub(super) fn sort(py: Python<'_>, a: &Bound<'_, PyAny>, axis: isize) -> PyResult<Py<PyAny>> {
    let mut input = to_f64_dyn(a)?;
    let axis = norm_axis(Some(axis), input.ndim())?
        .ok_or_else(|| PyValueError::new_err("sort requires an axis"))?;
    for mut lane in input.lanes_mut(Axis(axis)) {
        let mut values: Vec<f64> = lane.iter().copied().collect();
        values.sort_by(|left, right| left.total_cmp(right));
        for (slot, value) in lane.iter_mut().zip(values) {
            *slot = value;
        }
    }
    py_array(py, input)
}
