//! # eg-numeric — the epistemic-graph numeric kernel (CONCEPT:AU-KG.compute.numeric-kernel)
//!
//! A slim, **BLAS/LAPACK-free** numeric kernel: `ndarray` (arrays / reductions /
//! element-wise) + `faer` (pure-Rust LAPACK-class linalg) — the "one kernel, two
//! surfaces" foundation of the Analytics Program.
//!
//! * **Surface A (`python` feature):** a pyo3 extension module `epistemic_graph.numeric`
//!   with zero-copy numpy interop (`rust-numpy`) + GIL release (`allow_threads`),
//!   consumed by `agent_utilities.numeric.xp`. Migration off numpy is then mechanical.
//! * **Surface B (rlib, always):** the same pure kernel the engine links for
//!   in-database analytics (DataFusion UDFs, graph/vector/timeseries ops) — no FFI,
//!   compute-near-data. `python` is OFF by default so the engine links **no pyo3**
//!   (the Plan-01 no-pyo3-in-engine contract holds).
//!
//! The pure kernel (`reductions`, `elementwise`, `linalg`, `random`) is
//! parity-tested `np.allclose` vs numpy in `agent_utilities` and in-crate tests.

pub mod cluster;
pub mod elementwise;
pub mod error;
pub mod linalg;
pub mod random;
pub mod reductions;
// scipy.stats-parity ops (CONCEPT:EG-KG.compute.numeric-stats/EG-358). Gated behind `analytics` (pulled
// by `python`) so a `pi`/`default` engine build linking the rlib pulls no statrs.
#[cfg(feature = "analytics")]
pub mod stats;

pub use error::{NumericError, Result};

// ---------------------------------------------------------------------------
// Surface A — pyo3 Python extension module `epistemic_graph.numeric`.
// Gated behind the `python` feature so the engine-linked rlib pulls no pyo3.
// ---------------------------------------------------------------------------
#[cfg(feature = "python")]
// pyo3 bindings carry unavoidable boilerplate lints: PyErr .into() round-trips
// (useless_conversion), complex numpy return types (type_complexity), and a
// pyo3-0.22-macro-emitted cfg(gil-refs). Scope-allow them to this module.
#[allow(clippy::useless_conversion, clippy::type_complexity, unexpected_cfgs)]
mod py {
    use crate::{cluster, elementwise, linalg, random, reductions, stats};
    use ndarray::{ArrayD, ArrayViewD, Axis, IxDyn};
    use numpy::{
        IntoPyArray, PyArray1, PyArray2, PyReadonlyArray1, PyReadonlyArray2, PyReadonlyArrayDyn,
    };
    use pyo3::create_exception;
    use pyo3::exceptions::{PyException, PyValueError};
    use pyo3::prelude::*;

    create_exception!(numeric, LinAlgError, PyException);

    fn map_err(e: crate::NumericError) -> PyErr {
        match e {
            crate::NumericError::LinAlg(m) => LinAlgError::new_err(m),
            crate::NumericError::Shape(m) => PyValueError::new_err(m),
        }
    }

    // ---- N-D / integer extraction + axis dispatch (CONCEPT:EG-KG.compute.concept-4) ----

    /// Coerce any array-like to an owned `ArrayD<f64>`. Native float64/float32 and
    /// int64/int32 arrays are extracted zero-copy-ish (cast for the narrower/int
    /// dtypes); anything else (lists, other dtypes) is coerced through the numpy
    /// container substrate (`np.asarray(x, float64)`) — the shim never imports
    /// numpy itself, but the kernel privately uses it as its container substrate.
    fn to_f64_dyn(a: &Bound<'_, PyAny>) -> PyResult<ArrayD<f64>> {
        if let Ok(arr) = a.extract::<PyReadonlyArrayDyn<f64>>() {
            return Ok(arr.as_array().to_owned());
        }
        if let Ok(arr) = a.extract::<PyReadonlyArrayDyn<i64>>() {
            return Ok(arr.as_array().mapv(|v| v as f64));
        }
        if let Ok(arr) = a.extract::<PyReadonlyArrayDyn<i32>>() {
            return Ok(arr.as_array().mapv(|v| v as f64));
        }
        if let Ok(arr) = a.extract::<PyReadonlyArrayDyn<f32>>() {
            return Ok(arr.as_array().mapv(|v| v as f64));
        }
        let np = a.py().import_bound("numpy")?;
        let f64ty = np.getattr("float64")?;
        let coerced = np.getattr("asarray")?.call1((a, f64ty))?;
        let arr: PyReadonlyArrayDyn<f64> = coerced.extract()?;
        Ok(arr.as_array().to_owned())
    }

    /// Normalize a possibly-negative numpy axis to `Some(usize)` (or `None`).
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
    /// (keepdims=False), else a numpy array (keepdims inserts the collapsed axis).
    fn finish_f64(
        py: Python<'_>,
        a: ArrayD<f64>,
        axis: Option<usize>,
        keepdims: bool,
        flat: impl Fn(ArrayViewD<f64>) -> crate::Result<f64>,
        axisfn: impl Fn(ArrayViewD<f64>, usize) -> crate::Result<ArrayD<f64>>,
    ) -> PyResult<PyObject> {
        match axis {
            None => {
                let s = flat(a.view()).map_err(map_err)?;
                if keepdims {
                    let shape: Vec<usize> = a.shape().iter().map(|_| 1).collect();
                    let arr = ArrayD::from_elem(IxDyn(&shape), s);
                    Ok(arr.into_pyarray_bound(py).into_any().unbind())
                } else {
                    Ok(s.into_py(py))
                }
            }
            Some(ax) => {
                let mut out = axisfn(a.view(), ax).map_err(map_err)?;
                if keepdims {
                    out = out.insert_axis(Axis(ax));
                }
                Ok(out.into_pyarray_bound(py).into_any().unbind())
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
    ) -> PyResult<PyObject> {
        match axis {
            None => {
                let s = flat(a.view()).map_err(map_err)? as i64;
                if keepdims {
                    let shape: Vec<usize> = a.shape().iter().map(|_| 1).collect();
                    let arr = ArrayD::from_elem(IxDyn(&shape), s);
                    Ok(arr.into_pyarray_bound(py).into_any().unbind())
                } else {
                    Ok(s.into_py(py))
                }
            }
            Some(ax) => {
                let mut out = axisfn(a.view(), ax).map_err(map_err)?;
                if keepdims {
                    out = out.insert_axis(Axis(ax));
                }
                Ok(out.into_pyarray_bound(py).into_any().unbind())
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    ) -> PyResult<PyObject> {
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
    fn argsort(py: Python<'_>, a: PyReadonlyArray1<f64>) -> Py<PyArray1<i64>> {
        let idx: Vec<i64> = reductions::argsort(a.as_array())
            .into_iter()
            .map(|i| i as i64)
            .collect();
        idx.into_pyarray_bound(py).unbind()
    }
    #[pyfunction]
    fn cumsum(py: Python<'_>, a: PyReadonlyArray1<f64>) -> Py<PyArray1<f64>> {
        reductions::cumsum(a.as_array())
            .into_pyarray_bound(py)
            .unbind()
    }
    #[pyfunction]
    fn cumprod(py: Python<'_>, a: PyReadonlyArray1<f64>) -> Py<PyArray1<f64>> {
        reductions::cumprod(a.as_array())
            .into_pyarray_bound(py)
            .unbind()
    }
    #[pyfunction]
    fn percentile(a: PyReadonlyArray1<f64>, q: f64) -> PyResult<f64> {
        reductions::percentile(a.as_array(), q).map_err(map_err)
    }
    #[pyfunction]
    fn quantile(a: PyReadonlyArray1<f64>, q: f64) -> PyResult<f64> {
        reductions::quantile(a.as_array(), q).map_err(map_err)
    }

    // ---- element-wise ----
    macro_rules! ew1 {
        ($name:ident, $f:path) => {
            #[pyfunction]
            fn $name(py: Python<'_>, a: PyReadonlyArray1<f64>) -> Py<PyArray1<f64>> {
                $f(a.as_array()).into_pyarray_bound(py).unbind()
            }
        };
    }
    ew1!(sqrt, elementwise::sqrt);
    ew1!(log, elementwise::log);
    ew1!(exp, elementwise::exp);
    ew1!(absolute, elementwise::abs);
    ew1!(tanh, elementwise::tanh);

    #[pyfunction]
    fn clip(py: Python<'_>, a: PyReadonlyArray1<f64>, lo: f64, hi: f64) -> Py<PyArray1<f64>> {
        elementwise::clip(a.as_array(), lo, hi)
            .into_pyarray_bound(py)
            .unbind()
    }
    #[pyfunction]
    #[pyo3(signature = (a, nan=0.0, posinf=f64::MAX, neginf=f64::MIN))]
    fn nan_to_num(
        py: Python<'_>,
        a: PyReadonlyArray1<f64>,
        nan: f64,
        posinf: f64,
        neginf: f64,
    ) -> Py<PyArray1<f64>> {
        elementwise::nan_to_num(a.as_array(), nan, posinf, neginf)
            .into_pyarray_bound(py)
            .unbind()
    }
    #[pyfunction]
    fn isnan(py: Python<'_>, a: PyReadonlyArray1<f64>) -> Py<PyArray1<bool>> {
        numpy::PyArray1::from_vec_bound(py, elementwise::isnan(a.as_array())).unbind()
    }
    #[pyfunction]
    fn maximum(
        py: Python<'_>,
        a: PyReadonlyArray1<f64>,
        b: PyReadonlyArray1<f64>,
    ) -> PyResult<Py<PyArray1<f64>>> {
        Ok(elementwise::maximum(a.as_array(), b.as_array())
            .map_err(map_err)?
            .into_pyarray_bound(py)
            .unbind())
    }
    #[pyfunction]
    fn minimum(
        py: Python<'_>,
        a: PyReadonlyArray1<f64>,
        b: PyReadonlyArray1<f64>,
    ) -> PyResult<Py<PyArray1<f64>>> {
        Ok(elementwise::minimum(a.as_array(), b.as_array())
            .map_err(map_err)?
            .into_pyarray_bound(py)
            .unbind())
    }
    #[pyfunction]
    fn where_(
        py: Python<'_>,
        cond: Vec<bool>,
        a: PyReadonlyArray1<f64>,
        b: PyReadonlyArray1<f64>,
    ) -> PyResult<Py<PyArray1<f64>>> {
        Ok(elementwise::where_(&cond, a.as_array(), b.as_array())
            .map_err(map_err)?
            .into_pyarray_bound(py)
            .unbind())
    }

    // ---- linalg (GIL released for the heavy decompositions) ----
    #[pyfunction]
    fn norm(a: PyReadonlyArray1<f64>) -> f64 {
        linalg::norm(a.as_array())
    }
    #[pyfunction]
    fn norm_ord(a: PyReadonlyArray1<f64>, ord: f64) -> f64 {
        linalg::norm_ord(a.as_array(), ord)
    }
    #[pyfunction]
    fn dot(a: PyReadonlyArray1<f64>, b: PyReadonlyArray1<f64>) -> PyResult<f64> {
        linalg::dot(a.as_array(), b.as_array()).map_err(map_err)
    }
    #[pyfunction]
    fn matmul(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
        b: PyReadonlyArray2<f64>,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let av = a.as_array();
        let bv = b.as_array();
        let out = py
            .allow_threads(|| linalg::matmul(av, bv))
            .map_err(map_err)?;
        Ok(out.into_pyarray_bound(py).unbind())
    }
    #[pyfunction]
    fn solve(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
        b: PyReadonlyArray1<f64>,
    ) -> PyResult<Py<PyArray1<f64>>> {
        let av = a.as_array();
        let bv = b.as_array();
        let out = py
            .allow_threads(|| linalg::solve(av, bv))
            .map_err(map_err)?;
        Ok(out.into_pyarray_bound(py).unbind())
    }
    #[pyfunction]
    fn svdvals(py: Python<'_>, a: PyReadonlyArray2<f64>) -> Py<PyArray1<f64>> {
        let av = a.as_array();
        let s = py.allow_threads(|| linalg::svdvals(av));
        s.into_pyarray_bound(py).unbind()
    }
    #[pyfunction]
    fn svd(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
    ) -> PyResult<(Py<PyArray2<f64>>, Py<PyArray1<f64>>, Py<PyArray2<f64>>)> {
        let av = a.as_array();
        let (u, s, vt) = py.allow_threads(|| linalg::svd(av)).map_err(map_err)?;
        Ok((
            u.into_pyarray_bound(py).unbind(),
            s.into_pyarray_bound(py).unbind(),
            vt.into_pyarray_bound(py).unbind(),
        ))
    }
    #[pyfunction]
    fn eigh(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
    ) -> PyResult<(Py<PyArray1<f64>>, Py<PyArray2<f64>>)> {
        let av = a.as_array();
        let (w, v) = py.allow_threads(|| linalg::eigh(av)).map_err(map_err)?;
        Ok((
            w.into_pyarray_bound(py).unbind(),
            v.into_pyarray_bound(py).unbind(),
        ))
    }
    /// `scipy.sparse.linalg.eigsh(A, k, which="SM")` — the k smallest-magnitude
    /// symmetric eigenpairs (CONCEPT:EG-KG.compute.concept-5). Dense first cut (O(n^3)).
    #[pyfunction]
    fn eigsh(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
        k: usize,
    ) -> PyResult<(Py<PyArray1<f64>>, Py<PyArray2<f64>>)> {
        let av = a.as_array();
        let (w, v) = py
            .allow_threads(|| linalg::eigsh_smallest(av, k))
            .map_err(map_err)?;
        Ok((
            w.into_pyarray_bound(py).unbind(),
            v.into_pyarray_bound(py).unbind(),
        ))
    }
    #[pyfunction]
    fn pinv(py: Python<'_>, a: PyReadonlyArray2<f64>) -> PyResult<Py<PyArray2<f64>>> {
        let av = a.as_array();
        let out = py.allow_threads(|| linalg::pinv(av)).map_err(map_err)?;
        Ok(out.into_pyarray_bound(py).unbind())
    }
    #[pyfunction]
    fn lstsq(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
        b: PyReadonlyArray1<f64>,
    ) -> PyResult<Py<PyArray1<f64>>> {
        let av = a.as_array();
        let bv = b.as_array();
        let out = py
            .allow_threads(|| linalg::lstsq(av, bv))
            .map_err(map_err)?;
        Ok(out.into_pyarray_bound(py).unbind())
    }
    #[pyfunction]
    fn qr(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
    ) -> PyResult<(Py<PyArray2<f64>>, Py<PyArray2<f64>>)> {
        let av = a.as_array();
        let (q, r) = py.allow_threads(|| linalg::qr(av)).map_err(map_err)?;
        Ok((
            q.into_pyarray_bound(py).unbind(),
            r.into_pyarray_bound(py).unbind(),
        ))
    }
    #[pyfunction]
    fn cholesky(py: Python<'_>, a: PyReadonlyArray2<f64>) -> PyResult<Py<PyArray2<f64>>> {
        let av = a.as_array();
        let out = py.allow_threads(|| linalg::cholesky(av)).map_err(map_err)?;
        Ok(out.into_pyarray_bound(py).unbind())
    }
    #[pyfunction]
    fn det(py: Python<'_>, a: PyReadonlyArray2<f64>) -> PyResult<f64> {
        let av = a.as_array();
        py.allow_threads(|| linalg::det(av)).map_err(map_err)
    }
    #[pyfunction]
    fn inv(py: Python<'_>, a: PyReadonlyArray2<f64>) -> PyResult<Py<PyArray2<f64>>> {
        let av = a.as_array();
        let out = py.allow_threads(|| linalg::inverse(av)).map_err(map_err)?;
        Ok(out.into_pyarray_bound(py).unbind())
    }
    #[pyfunction]
    fn matrix_power(
        py: Python<'_>,
        a: PyReadonlyArray2<f64>,
        p: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let av = a.as_array();
        let out = py
            .allow_threads(|| linalg::matrix_power(av, p))
            .map_err(map_err)?;
        Ok(out.into_pyarray_bound(py).unbind())
    }

    // ---- scipy.stats-parity ops (CONCEPT:EG-KG.compute.numeric-stats/EG-358) ----
    /// `scipy.stats.spearmanr(a, b)` → `(rho, pvalue)`.
    #[pyfunction]
    fn spearmanr(a: PyReadonlyArray1<f64>, b: PyReadonlyArray1<f64>) -> PyResult<(f64, f64)> {
        stats::spearmanr(a.as_array(), b.as_array()).map_err(map_err)
    }
    /// `scipy.stats.ks_2samp(a, b)` → `(statistic, pvalue)` (asymptotic p-value).
    #[pyfunction]
    fn ks_2samp(a: PyReadonlyArray1<f64>, b: PyReadonlyArray1<f64>) -> PyResult<(f64, f64)> {
        stats::ks_2samp(a.as_array(), b.as_array()).map_err(map_err)
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
        data: PyReadonlyArray2<f64>,
        k: usize,
        max_iter: usize,
        seed: u64,
    ) -> PyResult<(Py<PyArray1<i64>>, Py<PyArray2<f64>>)> {
        let av = data.as_array();
        let res = py
            .allow_threads(|| cluster::kmeans(av, k, max_iter, seed))
            .map_err(map_err)?;
        let labels: Vec<i64> = res.labels.into_iter().map(|c| c as i64).collect();
        Ok((
            labels.into_pyarray_bound(py).unbind(),
            res.centroids.into_pyarray_bound(py).unbind(),
        ))
    }

    // ---- random ----
    #[pyfunction]
    #[pyo3(signature = (loc, scale, size, seed))]
    fn normal(py: Python<'_>, loc: f64, scale: f64, size: usize, seed: u64) -> Py<PyArray1<f64>> {
        let mut g = random::Generator::new(seed);
        g.normal(loc, scale, size).into_pyarray_bound(py).unbind()
    }
    #[pyfunction]
    #[pyo3(signature = (low, high, size, seed))]
    fn uniform(py: Python<'_>, low: f64, high: f64, size: usize, seed: u64) -> Py<PyArray1<f64>> {
        let mut g = random::Generator::new(seed);
        g.uniform(low, high, size).into_pyarray_bound(py).unbind()
    }
    #[pyfunction]
    #[pyo3(signature = (low, high, size, seed))]
    fn integers(py: Python<'_>, low: i64, high: i64, size: usize, seed: u64) -> Py<PyArray1<i64>> {
        let mut g = random::Generator::new(seed);
        g.integers(low, high, size).into_pyarray_bound(py).unbind()
    }

    /// Array constructors + dtype/module-attribute surface (CONCEPT:EG-KG.compute.concept-3).
    ///
    /// numpy is the engine-**private container substrate** (rust-numpy binds the
    /// official CPython numpy, and every kernel array IS a `numpy.ndarray`). So the
    /// container-allocation surface — `array`/`zeros`/`arange`/… and the dtype/
    /// `newaxis`/`pi`/`inf`/`nan`/`ndarray` attributes — is re-exported here as thin
    /// numpy passthroughs. The POINT is that `agent_utilities.numeric.xp` imports
    /// these from `epistemic_graph.numeric`, so agent-utilities imports numpy
    /// NOWHERE; the compute surface (reductions/linalg/stats/random) is genuine
    /// pure-Rust faer/ndarray/statrs kernel below.
    fn add_numpy_passthroughs(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let np = m.py().import_bound("numpy")?;
        // constructors + array manipulation
        const NAMES: &[&str] = &[
            "array",
            "asarray",
            "zeros",
            "ones",
            "empty",
            "full",
            "arange",
            "linspace",
            "eye",
            "diag",
            "fill_diagonal",
            "diff",
            "sort",
            "concatenate",
            "reshape",
            "vstack",
            "stack",
            // dtypes + type handle
            "float64",
            "float32",
            "int64",
            "int32",
            "bool_",
            "ndarray",
            // scalar/constant module attributes
            "newaxis",
            "pi",
            "inf",
            "nan",
        ];
        for name in NAMES {
            m.add(*name, np.getattr(*name)?)?;
        }
        Ok(())
    }

    /// The `epistemic_graph.numeric` extension module.
    #[pymodule]
    fn numeric(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("LinAlgError", m.py().get_type_bound::<LinAlgError>())?;
        m.add("__kernel__", "eg-numeric")?;
        add_numpy_passthroughs(m)?;
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
            integers
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
