//! # eg-numeric — the epistemic-graph numeric kernel (CONCEPT:AU-KG.compute.numeric-kernel)
//!
//! A slim, **BLAS/LAPACK-free** numeric kernel: `ndarray` (arrays / reductions /
//! element-wise) + `nalgebra` (pure-Rust LAPACK-class linalg) — the "one kernel, two
//! surfaces" foundation of the Analytics Program.
//!
//! * **Surface A (`python` feature):** a pyo3 extension module `epistemic_graph.numeric`
//!   with bounded built-in Python sequence conversion and Python detachment for kernels,
//!   consumed by `agent_utilities.numeric.xp`. It has no Python numeric runtime dependency.
//! * **Surface B (rlib, always):** the same pure kernel the engine links for
//!   in-database analytics (DataFusion UDFs, graph/vector/timeseries ops) — no FFI,
//!   compute-near-data. `python` is OFF by default so the engine links **no pyo3**
//!   (the Plan-01 no-pyo3-in-engine contract holds).
//!
//! The pure kernel (`reductions`, `elementwise`, `linalg`, `random`) is
//! parity-tested against the isolated developer reference implementation in
//! `agent_utilities` and in-crate tests.

pub mod cluster;
pub mod elementwise;
pub mod error;
pub mod linalg;
pub mod random;
pub mod reductions;
// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for
// cluster::KMeansResult` + the `modality_conformance_tests!` battery. Behind the
// crate's own opt-in `contract` feature (default OFF). See `src/contract.rs`.
#[cfg(feature = "contract")]
mod contract;
// scipy.stats-parity ops (CONCEPT:EG-KG.compute.numeric-stats/EG-358). Gated behind `analytics` (pulled
// by `python`) so a `pi`/`default` engine build linking the rlib pulls no statrs.
#[cfg(feature = "analytics")]
pub mod stats;
// Complex64 surface (D-QN-3 handoff, executed in Q1 lane w3-quantum-q1): a thin
// `Complex64` re-export + generic dense-block index application that
// `eg-quantum-sim`'s statevector backend consumes. Behind the crate's own opt-in
// `complex` feature (default OFF) — see `src/complex.rs` for the numeric-stack
// decision this executes.
#[cfg(feature = "complex")]
pub mod complex;

pub use error::{NumericError, Result};

// ---------------------------------------------------------------------------
// Surface A — pyo3 Python extension module `epistemic_graph.numeric`.
// Gated behind the `python` feature so the engine-linked rlib pulls no pyo3.
// The operation names remain stable, but the boundary contract is deliberately
// built-in-only: scalar results are Python scalars, array results are nested
// Python lists, and NumPy-only constructors/dtype passthroughs are not exported.
// The richer compatibility facade is owned by the agent-utilities numeric layer.
// ---------------------------------------------------------------------------
#[cfg(feature = "python")]
// pyo3 bindings carry unavoidable boilerplate lints: PyErr .into() round-trips
// (useless_conversion), complex return types (type_complexity), and PyO3
// macro-generated cfgs are scoped to this optional extension module.
#[allow(clippy::useless_conversion, clippy::type_complexity, unexpected_cfgs)]
mod py {
    use crate::{cluster, elementwise, linalg, random, reductions, stats};
    use ndarray::{ArrayD, ArrayViewD, Axis, Ix1, Ix2, IxDyn};
    use pyo3::create_exception;
    use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{
        PyByteArray, PyBytes, PyList, PyMapping, PySequence, PySequenceMethods, PyString,
    };

    create_exception!(numeric, LinAlgError, PyException);

    const MAX_INPUT_RANK: usize = 8;
    const MAX_INPUT_ELEMENTS: usize = random::MAX_RANDOM_ELEMENTS;
    const MAX_KMEANS_ITERATIONS: usize = 10_000;

    fn map_err(e: crate::NumericError) -> PyErr {
        match e {
            crate::NumericError::LinAlg(m) => LinAlgError::new_err(m),
            crate::NumericError::Shape(m) => PyValueError::new_err(m),
            crate::NumericError::Type(m) => PyTypeError::new_err(m),
            crate::NumericError::Bounds(m)
            | crate::NumericError::Resource(m)
            | crate::NumericError::Random(m) => PyValueError::new_err(m),
        }
    }

    // ---- built-in sequence extraction + axis dispatch ----

    /// Flatten a rectangular Python scalar/sequence tree into ndarray storage.
    ///
    /// The Python boundary intentionally accepts only numeric scalars and the
    /// sequence protocol. This keeps the native module importable when NumPy is
    /// absent and avoids silently adopting a dataframe/array runtime.
    fn flatten_f64(
        value: &Bound<'_, PyAny>,
        values: &mut Vec<f64>,
        shape: &mut Vec<usize>,
        depth: usize,
        element_count: &mut usize,
        shape_product: &mut usize,
    ) -> PyResult<()> {
        if value.is_instance_of::<PyString>()
            || value.is_instance_of::<PyBytes>()
            || value.is_instance_of::<PyByteArray>()
            || value.cast::<PyMapping>().is_ok()
        {
            return Err(PyValueError::new_err(
                "numeric input cannot be text, bytes, or a mapping",
            ));
        }
        if let Ok(number) = value.extract::<f64>() {
            if shape.len() != depth {
                return Err(PyValueError::new_err("numeric input must be rectangular"));
            }
            *element_count = element_count
                .checked_add(1)
                .ok_or_else(|| PyValueError::new_err("numeric input exceeds the element limit"))?;
            if *element_count > MAX_INPUT_ELEMENTS {
                return Err(PyValueError::new_err(format!(
                    "numeric input exceeds the {MAX_INPUT_ELEMENTS}-element limit"
                )));
            }
            values.push(number);
            return Ok(());
        }
        if depth >= MAX_INPUT_RANK {
            return Err(PyValueError::new_err(format!(
                "numeric input exceeds the rank-{MAX_INPUT_RANK} limit"
            )));
        }
        let sequence = value.cast::<PySequence>().map_err(|_| {
            PyValueError::new_err("numeric input must contain only real numbers and sequences")
        })?;
        let length = sequence.len()?;
        if shape.len() == depth {
            *shape_product = shape_product.checked_mul(length).ok_or_else(|| {
                PyValueError::new_err("numeric input shape overflows the element limit")
            })?;
            if *shape_product > MAX_INPUT_ELEMENTS {
                return Err(PyValueError::new_err(format!(
                    "numeric input exceeds the {MAX_INPUT_ELEMENTS}-element limit"
                )));
            }
            shape.push(length);
        } else if shape[depth] != length {
            return Err(PyValueError::new_err("numeric input must be rectangular"));
        }
        for index in 0..length {
            flatten_f64(
                &sequence.get_item(index)?,
                values,
                shape,
                depth + 1,
                element_count,
                shape_product,
            )?;
        }
        Ok(())
    }

    /// Coerce a scalar or rectangular built-in Python sequence to an owned ndarray.
    fn to_f64_dyn(a: &Bound<'_, PyAny>) -> PyResult<ArrayD<f64>> {
        let mut values = Vec::new();
        let mut shape = Vec::new();
        let mut element_count = 0;
        let mut shape_product = 1;
        flatten_f64(
            a,
            &mut values,
            &mut shape,
            0,
            &mut element_count,
            &mut shape_product,
        )?;
        ArrayD::from_shape_vec(IxDyn(&shape), values)
            .map_err(|error| PyValueError::new_err(format!("invalid numeric shape: {error}")))
    }

    fn to_f64_1d(a: &Bound<'_, PyAny>) -> PyResult<ndarray::Array1<f64>> {
        to_f64_dyn(a)?
            .into_dimensionality::<Ix1>()
            .map_err(|_| PyValueError::new_err("expected a one-dimensional numeric sequence"))
    }

    fn to_f64_2d(a: &Bound<'_, PyAny>) -> PyResult<ndarray::Array2<f64>> {
        to_f64_dyn(a)?
            .into_dimensionality::<Ix2>()
            .map_err(|_| PyValueError::new_err("expected a two-dimensional numeric sequence"))
    }

    fn to_bool_1d(a: &Bound<'_, PyAny>) -> PyResult<Vec<bool>> {
        if a.is_instance_of::<PyString>()
            || a.is_instance_of::<PyBytes>()
            || a.is_instance_of::<PyByteArray>()
            || a.cast::<PyMapping>().is_ok()
        {
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
            if item.is_instance_of::<PyString>()
                || item.is_instance_of::<PyBytes>()
                || item.is_instance_of::<PyByteArray>()
                || item.cast::<PyMapping>().is_ok()
                || item.cast::<PySequence>().is_ok()
            {
                return Err(PyValueError::new_err(
                    "condition must contain only boolean scalars",
                ));
            }
            values.push(item.extract::<bool>().map_err(|_| {
                PyValueError::new_err("condition must contain only boolean scalars")
            })?);
        }
        Ok(values)
    }

    fn check_output_size(size: usize) -> PyResult<()> {
        if size > MAX_INPUT_ELEMENTS {
            return Err(PyValueError::new_err(format!(
                "output size exceeds the {MAX_INPUT_ELEMENTS}-element limit"
            )));
        }
        Ok(())
    }

    fn nested_f64(
        py: Python<'_>,
        values: &[f64],
        shape: &[usize],
        depth: usize,
    ) -> PyResult<Py<PyAny>> {
        if depth == shape.len() {
            return Ok(values[0].into_pyobject(py)?.to_owned().into_any().unbind());
        }
        let list = PyList::empty(py);
        let stride = shape[depth + 1..].iter().product::<usize>();
        for index in 0..shape[depth] {
            let start = index * stride;
            let end = start + stride;
            list.append(nested_f64(py, &values[start..end], shape, depth + 1)?)?;
        }
        Ok(list.into_any().unbind())
    }

    fn nested_i64(
        py: Python<'_>,
        values: &[i64],
        shape: &[usize],
        depth: usize,
    ) -> PyResult<Py<PyAny>> {
        if depth == shape.len() {
            return Ok(values[0].into_pyobject(py)?.into_any().unbind());
        }
        let list = PyList::empty(py);
        let stride = shape[depth + 1..].iter().product::<usize>();
        for index in 0..shape[depth] {
            let start = index * stride;
            let end = start + stride;
            list.append(nested_i64(py, &values[start..end], shape, depth + 1)?)?;
        }
        Ok(list.into_any().unbind())
    }

    fn nested_bool(
        py: Python<'_>,
        values: &[bool],
        shape: &[usize],
        depth: usize,
    ) -> PyResult<Py<PyAny>> {
        if depth == shape.len() {
            return Ok(values[0].into_pyobject(py)?.to_owned().into_any().unbind());
        }
        let list = PyList::empty(py);
        let stride = shape[depth + 1..].iter().product::<usize>();
        for index in 0..shape[depth] {
            let start = index * stride;
            let end = start + stride;
            list.append(nested_bool(py, &values[start..end], shape, depth + 1)?)?;
        }
        Ok(list.into_any().unbind())
    }

    fn py_f64(py: Python<'_>, array: ArrayD<f64>) -> PyResult<Py<PyAny>> {
        let shape = array.shape().to_vec();
        let values: Vec<f64> = array.iter().copied().collect();
        nested_f64(py, &values, &shape, 0)
    }

    fn py_i64(py: Python<'_>, array: ArrayD<i64>) -> PyResult<Py<PyAny>> {
        let shape = array.shape().to_vec();
        let values: Vec<i64> = array.iter().copied().collect();
        nested_i64(py, &values, &shape, 0)
    }

    fn py_bool(py: Python<'_>, array: ArrayD<bool>) -> PyResult<Py<PyAny>> {
        let shape = array.shape().to_vec();
        let values: Vec<bool> = array.iter().copied().collect();
        nested_bool(py, &values, &shape, 0)
    }

    /// Normalize a possibly-negative axis to `Some(usize)` (or `None`).
    fn norm_axis(axis: Option<isize>, ndim: usize) -> PyResult<Option<usize>> {
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

    /// Finish a float-valued reduction: a Python scalar for `axis=None`
    /// (keepdims=False), else nested Python lists (keepdims inserts the collapsed axis).
    fn finish_f64(
        py: Python<'_>,
        a: ArrayD<f64>,
        axis: Option<usize>,
        keepdims: bool,
        flat: impl Fn(ArrayViewD<f64>) -> crate::Result<f64>,
        axisfn: impl Fn(ArrayViewD<f64>, usize) -> crate::Result<ArrayD<f64>>,
    ) -> PyResult<Py<PyAny>> {
        match axis {
            None => {
                let s = flat(a.view()).map_err(map_err)?;
                if keepdims {
                    let shape: Vec<usize> = a.shape().iter().map(|_| 1).collect();
                    let arr = ArrayD::from_elem(IxDyn(&shape), s);
                    py_f64(py, arr)
                } else {
                    Ok(s.into_pyobject(py)?.into_any().unbind())
                }
            }
            Some(ax) => {
                let mut out = axisfn(a.view(), ax).map_err(map_err)?;
                if keepdims {
                    out = out.insert_axis(Axis(ax));
                }
                py_f64(py, out)
            }
        }
    }

    /// Finish an integer-index reduction (argmin/argmax).
    fn finish_i64(
        py: Python<'_>,
        a: ArrayD<f64>,
        axis: Option<usize>,
        keepdims: bool,
        flat: impl Fn(ArrayViewD<f64>) -> crate::Result<usize>,
        axisfn: impl Fn(ArrayViewD<f64>, usize) -> crate::Result<ArrayD<i64>>,
    ) -> PyResult<Py<PyAny>> {
        match axis {
            None => {
                let s = flat(a.view()).map_err(map_err)? as i64;
                if keepdims {
                    let shape: Vec<usize> = a.shape().iter().map(|_| 1).collect();
                    let arr = ArrayD::from_elem(IxDyn(&shape), s);
                    py_i64(py, arr)
                } else {
                    Ok(s.into_pyobject(py)?.into_any().unbind())
                }
            }
            Some(ax) => {
                let mut out = axisfn(a.view(), ax).map_err(map_err)?;
                if keepdims {
                    out = out.insert_axis(Axis(ax));
                }
                py_i64(py, out)
            }
        }
    }

    // ---- reductions / stats (axis / keepdims / integer arrays — CONCEPT:EG-KG.compute.concept-4) ----
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, keepdims=false))]
    fn sum(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_f64(
            py,
            arr,
            ax,
            keepdims,
            |v| Ok(reductions::sum_all(v)),
            reductions::sum_axis,
        )
    }
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, keepdims=false))]
    fn prod(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_f64(
            py,
            arr,
            ax,
            keepdims,
            |v| Ok(reductions::prod_all(v)),
            reductions::prod_axis,
        )
    }
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, keepdims=false))]
    fn mean(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_f64(
            py,
            arr,
            ax,
            keepdims,
            |v| Ok(reductions::mean_all(v)),
            reductions::mean_axis,
        )
    }
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, ddof=0, keepdims=false))]
    fn var(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        ddof: usize,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_f64(
            py,
            arr,
            ax,
            keepdims,
            |v| Ok(reductions::var_all(v, ddof)),
            |v, k| reductions::var_axis(v, k, ddof),
        )
    }
    #[pyfunction(name = "std")]
    #[pyo3(signature = (a, axis=None, ddof=0, keepdims=false))]
    fn std_(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        ddof: usize,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_f64(
            py,
            arr,
            ax,
            keepdims,
            |v| Ok(reductions::std_all(v, ddof)),
            |v, k| reductions::std_axis(v, k, ddof),
        )
    }
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, keepdims=false))]
    fn amin(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_f64(
            py,
            arr,
            ax,
            keepdims,
            reductions::min_all,
            reductions::min_axis,
        )
    }
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, keepdims=false))]
    fn amax(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_f64(
            py,
            arr,
            ax,
            keepdims,
            reductions::max_all,
            reductions::max_axis,
        )
    }
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, keepdims=false))]
    fn argmin(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_i64(
            py,
            arr,
            ax,
            keepdims,
            reductions::argmin_all,
            reductions::argmin_axis,
        )
    }
    #[pyfunction]
    #[pyo3(signature = (a, axis=None, keepdims=false))]
    fn argmax(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        axis: Option<isize>,
        keepdims: bool,
    ) -> PyResult<Py<PyAny>> {
        let arr = to_f64_dyn(a)?;
        let ax = norm_axis(axis, arr.ndim())?;
        finish_i64(
            py,
            arr,
            ax,
            keepdims,
            reductions::argmax_all,
            reductions::argmax_axis,
        )
    }
    #[pyfunction]
    fn argsort(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_1d(a)?;
        let idx = reductions::argsort(input.view())
            .into_iter()
            .map(|i| i as i64)
            .collect::<Vec<_>>();
        py_i64(py, ndarray::Array1::from_vec(idx).into_dyn())
    }
    #[pyfunction]
    fn cumsum(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_1d(a)?;
        py_f64(py, reductions::cumsum(input.view()).into_dyn())
    }
    #[pyfunction]
    fn cumprod(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_1d(a)?;
        py_f64(py, reductions::cumprod(input.view()).into_dyn())
    }
    #[pyfunction]
    fn percentile(a: &Bound<'_, PyAny>, q: f64) -> PyResult<f64> {
        let input = to_f64_1d(a)?;
        reductions::percentile(input.view(), q).map_err(map_err)
    }
    #[pyfunction]
    fn quantile(a: &Bound<'_, PyAny>, q: f64) -> PyResult<f64> {
        let input = to_f64_1d(a)?;
        reductions::quantile(input.view(), q).map_err(map_err)
    }

    // ---- element-wise ----
    macro_rules! ew1 {
        ($name:ident, $f:path) => {
            #[pyfunction]
            fn $name(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
                let input = to_f64_1d(a)?;
                py_f64(py, $f(input.view()).into_dyn())
            }
        };
    }
    ew1!(sqrt, elementwise::sqrt);
    ew1!(log, elementwise::log);
    ew1!(exp, elementwise::exp);
    ew1!(absolute, elementwise::abs);
    ew1!(tanh, elementwise::tanh);

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
        py_f64(py, ArrayD::from_elem(IxDyn(&shape), value))
    }

    #[pyfunction]
    fn zeros(py: Python<'_>, shape: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        filled(py, shape, 0.0)
    }

    #[pyfunction]
    fn ones(py: Python<'_>, shape: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        filled(py, shape, 1.0)
    }

    /// Deliberately zero-filled, not uninitialized.
    ///
    /// NumPy's `empty` hands back whatever was in the allocation. Values here
    /// cross into Python as real list elements, so "uninitialized" would mean
    /// publishing arbitrary heap contents across a process boundary. The name
    /// is kept for call-site parity; the guarantee is strictly stronger.
    #[pyfunction]
    fn empty(py: Python<'_>, shape: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        filled(py, shape, 0.0)
    }

    #[pyfunction]
    fn full(py: Python<'_>, shape: &Bound<'_, PyAny>, fill_value: f64) -> PyResult<Py<PyAny>> {
        filled(py, shape, fill_value)
    }

    #[pyfunction]
    #[pyo3(signature = (n, m=None, k=0))]
    fn eye(py: Python<'_>, n: i64, m: Option<i64>, k: i64) -> PyResult<Py<PyAny>> {
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
        py_f64(py, out)
    }

    /// `arange(stop)` / `arange(start, stop[, step])`, matching NumPy's
    /// one-argument-is-`stop` convention.
    #[pyfunction]
    #[pyo3(signature = (start, stop=None, step=1.0))]
    fn arange(py: Python<'_>, start: f64, stop: Option<f64>, step: f64) -> PyResult<Py<PyAny>> {
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
        py_f64(py, ndarray::Array1::from_vec(values).into_dyn())
    }

    #[pyfunction]
    #[pyo3(signature = (start, stop, num=50, endpoint=true))]
    fn linspace(
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
        py_f64(py, ndarray::Array1::from_vec(values).into_dyn())
    }

    /// Validate and normalize a scalar/sequence tree into the canonical nested
    /// builtin-list form -- the boundary's equivalent of `numpy.array`.
    ///
    /// Not an identity function: it enforces rectangularity, the rank and
    /// element ceilings, and float coercion, so a caller that round-trips
    /// through it is guaranteed to hold something every other op here accepts.
    #[pyfunction]
    fn array(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        py_f64(py, to_f64_dyn(a)?)
    }

    #[pyfunction]
    fn asarray(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        py_f64(py, to_f64_dyn(a)?)
    }

    #[pyfunction]
    #[pyo3(signature = (a, b, rtol=1e-05, atol=1e-08, equal_nan=false))]
    fn isclose(
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
        py_bool(
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
    fn concatenate(py: Python<'_>, arrays: &Bound<'_, PyAny>, axis: isize) -> PyResult<Py<PyAny>> {
        let parts = to_f64_dyn_list(arrays)?;
        let ndim = parts[0].ndim();
        let axis = norm_axis(Some(axis), ndim)?
            .ok_or_else(|| PyValueError::new_err("concatenate requires an axis"))?;
        let views: Vec<ArrayViewD<'_, f64>> = parts.iter().map(|part| part.view()).collect();
        ndarray::concatenate(Axis(axis), &views)
            .map_err(|error| PyValueError::new_err(format!("concatenate: {error}")))
            .and_then(|joined| py_f64(py, joined))
    }

    #[pyfunction]
    fn reshape(
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
        py_f64(
            py,
            ArrayD::from_shape_vec(IxDyn(&shape), values)
                .map_err(|error| PyValueError::new_err(format!("invalid shape: {error}")))?,
        )
    }

    #[pyfunction]
    #[pyo3(signature = (arrays, axis=0))]
    fn stack(py: Python<'_>, arrays: &Bound<'_, PyAny>, axis: isize) -> PyResult<Py<PyAny>> {
        let parts = to_f64_dyn_list(arrays)?;
        let axis = norm_axis(Some(axis), parts[0].ndim() + 1)?
            .ok_or_else(|| PyValueError::new_err("stack requires an axis"))?;
        let views: Vec<ArrayViewD<'_, f64>> = parts.iter().map(|part| part.view()).collect();
        ndarray::stack(Axis(axis), &views)
            .map_err(|error| PyValueError::new_err(format!("stack: {error}")))
            .and_then(|stacked| py_f64(py, stacked))
    }

    /// Row-wise stack. 1-D inputs are promoted to single rows first, matching
    /// NumPy, so `vstack([[1,2],[3,4]])` is a 2x2 rather than an error.
    #[pyfunction]
    fn vstack(py: Python<'_>, arrays: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
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
            .and_then(|joined| py_f64(py, joined))
    }

    /// `diag` is NumPy's overloaded pair: extract a diagonal from a 2-D input,
    /// or build a diagonal matrix from a 1-D one.
    #[pyfunction]
    #[pyo3(signature = (a, k=0))]
    fn diag(py: Python<'_>, a: &Bound<'_, PyAny>, k: i64) -> PyResult<Py<PyAny>> {
        let input = to_f64_dyn(a)?;
        match input.ndim() {
            1 => {
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
                let mut out = ArrayD::<f64>::zeros(IxDyn(&[side, side]));
                for (index, value) in values.into_iter().enumerate() {
                    let (row, column) = if k >= 0 {
                        (index, index + k as usize)
                    } else {
                        (index + k.unsigned_abs() as usize, index)
                    };
                    out[[row, column]] = value;
                }
                py_f64(py, out)
            }
            2 => {
                let rows = input.shape()[0];
                let columns = input.shape()[1];
                let mut values = Vec::new();
                let mut index = 0usize;
                loop {
                    let (row, column) = if k >= 0 {
                        (index, index + k as usize)
                    } else {
                        (index + k.unsigned_abs() as usize, index)
                    };
                    if row >= rows || column >= columns {
                        break;
                    }
                    values.push(input[[row, column]]);
                    index += 1;
                }
                py_f64(py, ndarray::Array1::from_vec(values).into_dyn())
            }
            _ => Err(PyValueError::new_err(
                "diag expects a 1- or 2-dimensional input",
            )),
        }
    }

    /// Returns a NEW value rather than mutating in place.
    ///
    /// NumPy's `fill_diagonal` mutates its argument; a nested Python list that
    /// crossed this boundary is a copy, so in-place mutation could not be
    /// observed by the caller anyway. Returning the result makes that explicit
    /// instead of silently doing nothing.
    #[pyfunction]
    fn fill_diagonal(py: Python<'_>, a: &Bound<'_, PyAny>, value: f64) -> PyResult<Py<PyAny>> {
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
        py_f64(py, input)
    }

    #[pyfunction]
    #[pyo3(signature = (a, n=1, axis=-1))]
    fn diff(py: Python<'_>, a: &Bound<'_, PyAny>, n: i64, axis: isize) -> PyResult<Py<PyAny>> {
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
        py_f64(py, current)
    }

    #[pyfunction]
    #[pyo3(signature = (a, axis=-1))]
    fn sort(py: Python<'_>, a: &Bound<'_, PyAny>, axis: isize) -> PyResult<Py<PyAny>> {
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
        py_f64(py, input)
    }

    #[pyfunction]
    fn clip(py: Python<'_>, a: &Bound<'_, PyAny>, lo: f64, hi: f64) -> PyResult<Py<PyAny>> {
        let input = to_f64_1d(a)?;
        py_f64(py, elementwise::clip(input.view(), lo, hi).into_dyn())
    }
    #[pyfunction]
    #[pyo3(signature = (a, nan=0.0, posinf=f64::MAX, neginf=f64::MIN))]
    fn nan_to_num(
        py: Python<'_>,
        a: &Bound<'_, PyAny>,
        nan: f64,
        posinf: f64,
        neginf: f64,
    ) -> PyResult<Py<PyAny>> {
        let input = to_f64_dyn(a)?;
        py_f64(
            py,
            elementwise::nan_to_num(input.view(), nan, posinf, neginf),
        )
    }
    #[pyfunction]
    fn isnan(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_1d(a)?;
        py_bool(
            py,
            ndarray::Array1::from_vec(elementwise::isnan(input.view())).into_dyn(),
        )
    }
    #[pyfunction]
    fn maximum(py: Python<'_>, a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let left = to_f64_1d(a)?;
        let right = to_f64_1d(b)?;
        py_f64(
            py,
            elementwise::maximum(left.view(), right.view())
                .map_err(map_err)?
                .into_dyn(),
        )
    }
    #[pyfunction]
    fn minimum(py: Python<'_>, a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let left = to_f64_1d(a)?;
        let right = to_f64_1d(b)?;
        py_f64(
            py,
            elementwise::minimum(left.view(), right.view())
                .map_err(map_err)?
                .into_dyn(),
        )
    }
    #[pyfunction]
    fn where_(
        py: Python<'_>,
        cond: &Bound<'_, PyAny>,
        a: &Bound<'_, PyAny>,
        b: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let cond = to_bool_1d(cond)?;
        let left = to_f64_1d(a)?;
        let right = to_f64_1d(b)?;
        py_f64(
            py,
            elementwise::where_(&cond, left.view(), right.view())
                .map_err(map_err)?
                .into_dyn(),
        )
    }

    // ---- linalg (GIL released for the heavy decompositions) ----
    #[pyfunction]
    fn norm(a: &Bound<'_, PyAny>) -> PyResult<f64> {
        Ok(linalg::norm(to_f64_1d(a)?.view()))
    }
    #[pyfunction]
    fn norm_ord(a: &Bound<'_, PyAny>, ord: f64) -> PyResult<f64> {
        Ok(linalg::norm_ord(to_f64_1d(a)?.view(), ord))
    }
    #[pyfunction]
    fn dot(a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<f64> {
        let left = to_f64_1d(a)?;
        let right = to_f64_1d(b)?;
        linalg::dot(left.view(), right.view()).map_err(map_err)
    }
    #[pyfunction]
    fn matmul(py: Python<'_>, a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let left = to_f64_2d(a)?;
        let right = to_f64_2d(b)?;
        let out = py
            .detach(|| linalg::matmul(left.view(), right.view()))
            .map_err(map_err)?;
        py_f64(py, out.into_dyn())
    }
    #[pyfunction]
    fn solve(py: Python<'_>, a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let left = to_f64_2d(a)?;
        let right = to_f64_1d(b)?;
        let out = py
            .detach(|| linalg::solve(left.view(), right.view()))
            .map_err(map_err)?;
        py_f64(py, out.into_dyn())
    }
    #[pyfunction]
    fn svdvals(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_2d(a)?;
        let s = py.detach(|| linalg::svdvals(input.view()));
        py_f64(py, ndarray::Array1::from_vec(s).into_dyn())
    }
    #[pyfunction]
    fn svd(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
        let input = to_f64_2d(a)?;
        let (u, s, vt) = py.detach(|| linalg::svd(input.view())).map_err(map_err)?;
        Ok((
            py_f64(py, u.into_dyn())?,
            py_f64(py, s.into_dyn())?,
            py_f64(py, vt.into_dyn())?,
        ))
    }
    #[pyfunction]
    fn eigh(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let input = to_f64_2d(a)?;
        let (w, v) = py.detach(|| linalg::eigh(input.view())).map_err(map_err)?;
        Ok((py_f64(py, w.into_dyn())?, py_f64(py, v.into_dyn())?))
    }
    /// `scipy.sparse.linalg.eigsh(A, k, which="SM")` — the k smallest-magnitude
    /// symmetric eigenpairs (CONCEPT:EG-KG.compute.concept-5). Dense first cut (O(n^3)).
    #[pyfunction]
    fn eigsh(py: Python<'_>, a: &Bound<'_, PyAny>, k: usize) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let input = to_f64_2d(a)?;
        let (w, v) = py
            .detach(|| linalg::eigsh_smallest(input.view(), k))
            .map_err(map_err)?;
        Ok((py_f64(py, w.into_dyn())?, py_f64(py, v.into_dyn())?))
    }
    #[pyfunction]
    fn pinv(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_2d(a)?;
        let out = py.detach(|| linalg::pinv(input.view())).map_err(map_err)?;
        py_f64(py, out.into_dyn())
    }
    #[pyfunction]
    fn lstsq(py: Python<'_>, a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let left = to_f64_2d(a)?;
        let right = to_f64_1d(b)?;
        let out = py
            .detach(|| linalg::lstsq(left.view(), right.view()))
            .map_err(map_err)?;
        py_f64(py, out.into_dyn())
    }
    #[pyfunction]
    fn qr(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let input = to_f64_2d(a)?;
        let (q, r) = py.detach(|| linalg::qr(input.view())).map_err(map_err)?;
        Ok((py_f64(py, q.into_dyn())?, py_f64(py, r.into_dyn())?))
    }
    #[pyfunction]
    fn cholesky(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_2d(a)?;
        let out = py
            .detach(|| linalg::cholesky(input.view()))
            .map_err(map_err)?;
        py_f64(py, out.into_dyn())
    }
    #[pyfunction]
    fn det(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<f64> {
        let input = to_f64_2d(a)?;
        py.detach(|| linalg::det(input.view())).map_err(map_err)
    }
    #[pyfunction]
    fn inv(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let input = to_f64_2d(a)?;
        let out = py
            .detach(|| linalg::inverse(input.view()))
            .map_err(map_err)?;
        py_f64(py, out.into_dyn())
    }
    #[pyfunction]
    fn matrix_power(py: Python<'_>, a: &Bound<'_, PyAny>, p: i64) -> PyResult<Py<PyAny>> {
        let input = to_f64_2d(a)?;
        let out = py
            .detach(|| linalg::matrix_power(input.view(), p))
            .map_err(map_err)?;
        py_f64(py, out.into_dyn())
    }

    // ---- scipy.stats-parity ops (CONCEPT:EG-KG.compute.numeric-stats/EG-358) ----
    /// `scipy.stats.spearmanr(a, b)` → `(rho, pvalue)`.
    #[pyfunction]
    fn spearmanr(a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<(f64, f64)> {
        let left = to_f64_1d(a)?;
        let right = to_f64_1d(b)?;
        stats::spearmanr(left.view(), right.view()).map_err(map_err)
    }
    /// `scipy.stats.ks_2samp(a, b)` → `(statistic, pvalue)` (asymptotic p-value).
    #[pyfunction]
    fn ks_2samp(a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<(f64, f64)> {
        let left = to_f64_1d(a)?;
        let right = to_f64_1d(b)?;
        stats::ks_2samp(left.view(), right.view()).map_err(map_err)
    }
    /// `scipy.stats.norm.ppf(q, loc, scale)` — normal inverse CDF (quantile).
    #[pyfunction]
    #[pyo3(signature = (q, loc=0.0, scale=1.0))]
    fn norm_ppf(q: f64, loc: f64, scale: f64) -> PyResult<f64> {
        stats::norm_ppf(q, loc, scale).map_err(map_err)
    }
    /// `scipy.stats.norm.pdf(x, loc, scale)` — normal probability density.
    #[pyfunction]
    #[pyo3(signature = (x, loc=0.0, scale=1.0))]
    fn norm_pdf(x: f64, loc: f64, scale: f64) -> PyResult<f64> {
        stats::norm_pdf(x, loc, scale).map_err(map_err)
    }

    // ---- clustering (CONCEPT:EG-KG.query.kmeans-clustering-half-one) ----
    #[pyfunction]
    #[pyo3(signature = (data, k, max_iter=100, seed=cluster::KMEANS_DEFAULT_SEED))]
    fn kmeans(
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        k: usize,
        max_iter: usize,
        seed: u64,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        if k > MAX_INPUT_ELEMENTS {
            return Err(PyValueError::new_err(format!(
                "k exceeds the {MAX_INPUT_ELEMENTS}-element limit"
            )));
        }
        if max_iter > MAX_KMEANS_ITERATIONS {
            return Err(PyValueError::new_err(format!(
                "max_iter exceeds the {MAX_KMEANS_ITERATIONS}-iteration limit"
            )));
        }
        let input = to_f64_2d(data)?;
        let res = py
            .detach(|| cluster::kmeans(input.view(), k, max_iter, seed))
            .map_err(map_err)?;
        let labels: Vec<i64> = res.labels.into_iter().map(|c| c as i64).collect();
        Ok((
            py_i64(py, ndarray::Array1::from_vec(labels).into_dyn())?,
            py_f64(py, res.centroids.into_dyn())?,
        ))
    }

    // ---- random ----
    #[pyfunction]
    #[pyo3(signature = (loc, scale, size, seed))]
    fn normal(py: Python<'_>, loc: f64, scale: f64, size: usize, seed: u64) -> PyResult<Py<PyAny>> {
        check_output_size(size)?;
        let mut g = random::Generator::new(seed);
        let values = g.try_normal(loc, scale, size).map_err(map_err)?;
        py_f64(py, ndarray::Array1::from_vec(values).into_dyn())
    }
    #[pyfunction]
    #[pyo3(signature = (low, high, size, seed))]
    fn uniform(py: Python<'_>, low: f64, high: f64, size: usize, seed: u64) -> PyResult<Py<PyAny>> {
        check_output_size(size)?;
        let mut g = random::Generator::new(seed);
        let values = g.try_uniform(low, high, size).map_err(map_err)?;
        py_f64(py, ndarray::Array1::from_vec(values).into_dyn())
    }
    #[pyfunction]
    #[pyo3(signature = (low, high, size, seed))]
    fn integers(
        py: Python<'_>,
        low: i64,
        high: i64,
        size: usize,
        seed: u64,
    ) -> PyResult<Py<PyAny>> {
        check_output_size(size)?;
        let mut g = random::Generator::new(seed);
        let values = g.try_integers(low, high, size).map_err(map_err)?;
        py_i64(py, ndarray::Array1::from_vec(values).into_dyn())
    }

    /// Draw a bounded batch of population indices, optionally weighted.
    ///
    /// `weights` is kept as a Python object until the existing bounded,
    /// one-dimensional native extractor has validated its rank and element
    /// count.  The Rust kernel owns all sampling loops; callers receive one
    /// detached list of indices and can map those indices to arbitrary values.
    #[pyfunction]
    #[pyo3(signature = (population, size, replace=true, weights=None, seed=0))]
    fn choice_indices(
        py: Python<'_>,
        population: usize,
        size: usize,
        replace: bool,
        weights: Option<&Bound<'_, PyAny>>,
        seed: u64,
    ) -> PyResult<Py<PyAny>> {
        if population > MAX_INPUT_ELEMENTS {
            return Err(PyValueError::new_err(format!(
                "population size exceeds the {MAX_INPUT_ELEMENTS}-element limit"
            )));
        }
        check_output_size(size)?;
        let weights = weights
            .map(to_f64_1d)
            .transpose()?
            .map(|weights| weights.to_vec());
        let mut generator = random::Generator::new(seed);
        let values = generator
            .try_choice_indices(population, size, replace, weights.as_deref())
            .map_err(map_err)?;
        let values = values.into_iter().map(|value| value as i64).collect();
        py_i64(py, ndarray::Array1::from_vec(values).into_dyn())
    }

    /// Return one bounded, uniformly random permutation of population indices.
    #[pyfunction]
    #[pyo3(signature = (population, seed=0))]
    fn permutation_indices(py: Python<'_>, population: usize, seed: u64) -> PyResult<Py<PyAny>> {
        let mut generator = random::Generator::new(seed);
        let values = generator
            .try_permutation_indices(population)
            .map_err(map_err)?;
        let values = values.into_iter().map(|value| value as i64).collect();
        py_i64(py, ndarray::Array1::from_vec(values).into_dyn())
    }

    /// The `epistemic_graph.numeric` extension module.
    #[pymodule]
    fn numeric(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("LinAlgError", m.py().get_type::<LinAlgError>())?;
        m.add("__kernel__", "eg-numeric")?;
        // NE-249: scalar constants, natively defined. Previously these were
        // `numpy.pi`/`numpy.inf`/`numpy.nan` forwarded through the passthrough
        // loop this module no longer has. They are plain f64 values -- nothing
        // about them needed NumPy.
        m.add("pi", std::f64::consts::PI)?;
        m.add("inf", f64::INFINITY)?;
        m.add("nan", f64::NAN)?;
        macro_rules! add {
            ($($f:ident),* $(,)?) => { $( m.add_function(wrap_pyfunction!($f, m)?)?; )* };
        }
        add!(
            sum,
            prod,
            mean,
            var,
            std_,
            amin,
            amax,
            argmin,
            argmax,
            argsort,
            cumsum,
            cumprod,
            percentile,
            quantile,
            sqrt,
            log,
            exp,
            absolute,
            tanh,
            clip,
            nan_to_num,
            isnan,
            maximum,
            minimum,
            where_,
            norm,
            norm_ord,
            dot,
            matmul,
            solve,
            svdvals,
            svd,
            eigh,
            eigsh,
            pinv,
            lstsq,
            qr,
            cholesky,
            det,
            inv,
            matrix_power,
            spearmanr,
            ks_2samp,
            norm_ppf,
            norm_pdf,
            kmeans,
            normal,
            uniform,
            integers,
            choice_indices,
            permutation_indices,
            // NE-249: native array construction / shape manipulation. These
            // restore the surface `b7d5825` removed along with the NumPy
            // passthrough, implemented in Rust over builtin lists instead.
            zeros,
            ones,
            empty,
            full,
            eye,
            arange,
            linspace,
            array,
            asarray,
            isclose,
            concatenate,
            reshape,
            stack,
            vstack,
            diag,
            fill_diagonal,
            diff,
            sort
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{linalg, reductions};
    use ndarray::{array, Array1};

    #[test]
    fn reductions_basic() {
        let a: Array1<f64> = array![1.0, 2.0, 3.0, 4.0];
        assert_eq!(reductions::sum(a.view()), 10.0);
        assert_eq!(reductions::mean(a.view()), 2.5);
        assert!((reductions::std(a.view(), 0) - 1.118_033_988_749_895).abs() < 1e-12);
        assert_eq!(reductions::argmax(a.view()).unwrap(), 3);
    }

    #[test]
    fn solve_matches_hand() {
        // [[3,2],[1,2]] x = [7,5] -> x = [1, 2]
        let a = array![[3.0, 2.0], [1.0, 2.0]];
        let b = array![7.0, 5.0];
        let x = linalg::solve(a.view(), b.view()).unwrap();
        assert!((x[0] - 1.0).abs() < 1e-10);
        assert!((x[1] - 2.0).abs() < 1e-10);
    }

    #[test]
    fn batch_l2_normalize_units() {
        // [3,4] → [0.6,0.8]; a zero vector is returned unchanged (safe divide).
        let out = linalg::batch_l2_normalize(&[vec![3.0, 4.0], vec![0.0, 0.0]]);
        assert!((out[0][0] - 0.6).abs() < 1e-12);
        assert!((out[0][1] - 0.8).abs() < 1e-12);
        assert_eq!(out[1], vec![0.0, 0.0]);
        // A unit vector's L2 norm is 1.
        let n = linalg::norm(ndarray::ArrayView1::from(&out[0]));
        assert!((n - 1.0).abs() < 1e-12);
    }

    #[test]
    fn singular_solve_errors() {
        let a = array![[1.0, 2.0], [2.0, 4.0]];
        let b = array![1.0, 2.0];
        assert!(linalg::solve(a.view(), b.view()).is_err());
    }
}
