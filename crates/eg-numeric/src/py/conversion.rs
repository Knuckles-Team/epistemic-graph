// ---- built-in sequence extraction + axis dispatch ----

use super::{map_err, MAX_INPUT_ELEMENTS, MAX_INPUT_RANK};
use ndarray::{ArrayD, ArrayViewD, Axis, Ix1, Ix2, IxDyn};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{
    PyByteArray, PyBytes, PyList, PyMapping, PySequence, PySequenceMethods, PyString,
};
use pyo3::IntoPyObjectExt;

struct FlatF64Input {
    values: Vec<f64>,
    shape: Vec<usize>,
    element_count: usize,
    shape_product: usize,
}

impl FlatF64Input {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            shape: Vec::new(),
            element_count: 0,
            shape_product: 1,
        }
    }

    fn flatten(&mut self, value: &Bound<'_, PyAny>, depth: usize) -> PyResult<()> {
        reject_numeric_container(value)?;
        match value.extract::<f64>() {
            Ok(number) => self.push_number(number, depth),
            Err(_) => self.push_sequence(value, depth),
        }
    }

    fn push_number(&mut self, number: f64, depth: usize) -> PyResult<()> {
        if self.shape.len() != depth {
            return Err(PyValueError::new_err("numeric input must be rectangular"));
        }
        self.element_count = self
            .element_count
            .checked_add(1)
            .ok_or_else(|| PyValueError::new_err("numeric input exceeds the element limit"))?;
        if self.element_count > MAX_INPUT_ELEMENTS {
            return Err(element_limit_error("numeric input exceeds"));
        }
        self.values.push(number);
        Ok(())
    }

    fn push_sequence(&mut self, value: &Bound<'_, PyAny>, depth: usize) -> PyResult<()> {
        if depth >= MAX_INPUT_RANK {
            return Err(PyValueError::new_err(format!(
                "numeric input exceeds the rank-{MAX_INPUT_RANK} limit"
            )));
        }
        let sequence = value.cast::<PySequence>().map_err(|_| {
            PyValueError::new_err("numeric input must contain only real numbers and sequences")
        })?;
        let length = sequence.len()?;
        self.record_dimension(depth, length)?;
        for index in 0..length {
            self.flatten(&sequence.get_item(index)?, depth + 1)?;
        }
        Ok(())
    }

    fn record_dimension(&mut self, depth: usize, length: usize) -> PyResult<()> {
        if self.shape.len() == depth {
            self.shape_product = self.shape_product.checked_mul(length).ok_or_else(|| {
                PyValueError::new_err("numeric input shape overflows the element limit")
            })?;
            if self.shape_product > MAX_INPUT_ELEMENTS {
                return Err(element_limit_error("numeric input exceeds"));
            }
            self.shape.push(length);
        } else if self.shape[depth] != length {
            return Err(PyValueError::new_err("numeric input must be rectangular"));
        }
        Ok(())
    }
}

fn element_limit_error(subject: &str) -> PyErr {
    PyValueError::new_err(format!("{subject} the {MAX_INPUT_ELEMENTS}-element limit"))
}

fn text_bytes_or_mapping(value: &Bound<'_, PyAny>) -> bool {
    value.is_instance_of::<PyString>()
        || value.is_instance_of::<PyBytes>()
        || value.is_instance_of::<PyByteArray>()
        || value.cast::<PyMapping>().is_ok()
}

fn reject_numeric_container(value: &Bound<'_, PyAny>) -> PyResult<()> {
    if text_bytes_or_mapping(value) {
        Err(PyValueError::new_err(
            "numeric input cannot be text, bytes, or a mapping",
        ))
    } else {
        Ok(())
    }
}

/// Flatten a rectangular Python scalar/sequence tree into ndarray storage.
///
/// The Python boundary intentionally accepts only numeric scalars and the
/// sequence protocol. This keeps the native module importable when NumPy is
/// absent and avoids silently adopting a dataframe/array runtime.
fn flatten_f64(value: &Bound<'_, PyAny>) -> PyResult<FlatF64Input> {
    let mut input = FlatF64Input::new();
    input.flatten(value, 0)?;
    Ok(input)
}

/// Coerce a scalar or rectangular built-in Python sequence to an owned ndarray.
pub(super) fn to_f64_dyn(a: &Bound<'_, PyAny>) -> PyResult<ArrayD<f64>> {
    let input = flatten_f64(a)?;
    ArrayD::from_shape_vec(IxDyn(&input.shape), input.values)
        .map_err(|error| PyValueError::new_err(format!("invalid numeric shape: {error}")))
}

pub(super) fn to_f64_1d(a: &Bound<'_, PyAny>) -> PyResult<ndarray::Array1<f64>> {
    to_f64_dyn(a)?
        .into_dimensionality::<Ix1>()
        .map_err(|_| PyValueError::new_err("expected a one-dimensional numeric sequence"))
}

pub(super) fn to_f64_2d(a: &Bound<'_, PyAny>) -> PyResult<ndarray::Array2<f64>> {
    to_f64_dyn(a)?
        .into_dimensionality::<Ix2>()
        .map_err(|_| PyValueError::new_err("expected a two-dimensional numeric sequence"))
}

pub(super) fn to_bool_1d(a: &Bound<'_, PyAny>) -> PyResult<Vec<bool>> {
    if text_bytes_or_mapping(a) {
        return Err(PyValueError::new_err(
            "condition cannot be text, bytes, or a mapping",
        ));
    }
    let sequence = a
        .cast::<PySequence>()
        .map_err(|_| PyValueError::new_err("condition must be a one-dimensional sequence"))?;
    let length = sequence.len()?;
    if length > MAX_INPUT_ELEMENTS {
        return Err(PyValueError::new_err(format!(
            "condition exceeds the {MAX_INPUT_ELEMENTS}-element limit"
        )));
    }
    let mut values = Vec::with_capacity(length);
    for index in 0..length {
        let item = sequence.get_item(index)?;
        if text_bytes_or_mapping(&item) || item.cast::<PySequence>().is_ok() {
            return Err(PyValueError::new_err(
                "condition must contain only boolean scalars",
            ));
        }
        values.push(
            item.extract::<bool>().map_err(|_| {
                PyValueError::new_err("condition must contain only boolean scalars")
            })?,
        );
    }
    Ok(values)
}

pub(super) fn check_output_size(size: usize) -> PyResult<()> {
    if size > MAX_INPUT_ELEMENTS {
        return Err(PyValueError::new_err(format!(
            "output size exceeds the {MAX_INPUT_ELEMENTS}-element limit"
        )));
    }
    Ok(())
}

/// Materialize a flat C-order buffer as the nested Python lists the boundary
/// returns for an array result. One walker serves every element type the
/// kernel produces (`f64`, `i64`, `bool`) — they differed only in that type.
fn nested<T>(py: Python<'_>, values: &[T], shape: &[usize], depth: usize) -> PyResult<Py<PyAny>>
where
    T: Copy + for<'p> IntoPyObject<'p>,
{
    if depth == shape.len() {
        return values[0].into_py_any(py);
    }
    let list = PyList::empty(py);
    let stride = shape[depth + 1..].iter().product::<usize>();
    for index in 0..shape[depth] {
        let start = index * stride;
        let end = start + stride;
        list.append(nested(py, &values[start..end], shape, depth + 1)?)?;
    }
    Ok(list.into_any().unbind())
}

/// Convert a kernel array result to the boundary's nested-Python-list form.
pub(super) fn py_array<T>(py: Python<'_>, array: ArrayD<T>) -> PyResult<Py<PyAny>>
where
    T: Copy + for<'p> IntoPyObject<'p>,
{
    let shape = array.shape().to_vec();
    let values: Vec<T> = array.iter().copied().collect();
    nested(py, &values, &shape, 0)
}

/// Normalize a possibly-negative axis to `Some(usize)` (or `None`).
pub(super) fn norm_axis(axis: Option<isize>, ndim: usize) -> PyResult<Option<usize>> {
    match axis {
        None => Ok(None),
        Some(k) => {
            let n = ndim as isize;
            let kk = if k < 0 { k + n } else { k };
            if kk < 0 || kk >= n {
                return Err(PyValueError::new_err(format!(
                    "axis {k} is out of bounds for array of dimension {ndim}"
                )));
            }
            Ok(Some(kk as usize))
        }
    }
}

/// Finish a reduction: a Python scalar for `axis=None` (keepdims=False), else
/// nested Python lists (keepdims re-inserts the collapsed axis). The float
/// reductions and the integer-index reductions (argmin/argmax) differ only in
/// the element type they produce, so they share this one finisher.
pub(super) fn finish<T>(
    py: Python<'_>,
    a: ArrayD<f64>,
    axis: Option<usize>,
    keepdims: bool,
    flat: impl Fn(ArrayViewD<f64>) -> crate::Result<T>,
    axisfn: impl Fn(ArrayViewD<f64>, usize) -> crate::Result<ArrayD<T>>,
) -> PyResult<Py<PyAny>>
where
    T: Copy + for<'p> IntoPyObject<'p>,
{
    match axis {
        None => {
            let scalar = flat(a.view()).map_err(map_err)?;
            if keepdims {
                let shape: Vec<usize> = a.shape().iter().map(|_| 1).collect();
                py_array(py, ArrayD::from_elem(IxDyn(&shape), scalar))
            } else {
                scalar.into_py_any(py)
            }
        }
        Some(ax) => {
            let mut out = axisfn(a.view(), ax).map_err(map_err)?;
            if keepdims {
                out = out.insert_axis(Axis(ax));
            }
            py_array(py, out)
        }
    }
}
