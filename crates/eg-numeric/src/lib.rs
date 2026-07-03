//! # eg-numeric — the epistemic-graph numeric kernel (CONCEPT:EG-321)
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

pub mod elementwise;
pub mod error;
pub mod linalg;
pub mod random;
pub mod reductions;

pub use error::{NumericError, Result};

// ---------------------------------------------------------------------------
// Surface A — pyo3 Python extension module `epistemic_graph.numeric`.
// Gated behind the `python` feature so the engine-linked rlib pulls no pyo3.
// ---------------------------------------------------------------------------
#[cfg(feature = "python")]
mod py {
    use crate::{elementwise, linalg, random, reductions};
    use numpy::{IntoPyArray, PyArray1, PyArray2, PyReadonlyArray1, PyReadonlyArray2};
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

    // ---- reductions / stats ----
    #[pyfunction]
    fn sum(a: PyReadonlyArray1<f64>) -> f64 {
        reductions::sum(a.as_array())
    }
    #[pyfunction]
    fn prod(a: PyReadonlyArray1<f64>) -> f64 {
        reductions::prod(a.as_array())
    }
    #[pyfunction]
    fn mean(a: PyReadonlyArray1<f64>) -> f64 {
        reductions::mean(a.as_array())
    }
    #[pyfunction]
    #[pyo3(signature = (a, ddof=0))]
    fn var(a: PyReadonlyArray1<f64>, ddof: usize) -> f64 {
        reductions::var(a.as_array(), ddof)
    }
    #[pyfunction(name = "std")]
    #[pyo3(signature = (a, ddof=0))]
    fn std_(a: PyReadonlyArray1<f64>, ddof: usize) -> f64 {
        reductions::std(a.as_array(), ddof)
    }
    #[pyfunction]
    fn amin(a: PyReadonlyArray1<f64>) -> PyResult<f64> {
        reductions::min(a.as_array()).map_err(map_err)
    }
    #[pyfunction]
    fn amax(a: PyReadonlyArray1<f64>) -> PyResult<f64> {
        reductions::max(a.as_array()).map_err(map_err)
    }
    #[pyfunction]
    fn argmin(a: PyReadonlyArray1<f64>) -> PyResult<usize> {
        reductions::argmin(a.as_array()).map_err(map_err)
    }
    #[pyfunction]
    fn argmax(a: PyReadonlyArray1<f64>) -> PyResult<usize> {
        reductions::argmax(a.as_array()).map_err(map_err)
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

    /// The `epistemic_graph.numeric` extension module.
    #[pymodule]
    fn numeric(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("LinAlgError", m.py().get_type_bound::<LinAlgError>())?;
        m.add("__kernel__", "eg-numeric")?;
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
            pinv,
            lstsq,
            qr,
            cholesky,
            det,
            inv,
            matrix_power,
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
    fn singular_solve_errors() {
        let a = array![[1.0, 2.0], [2.0, 4.0]];
        let b = array![1.0, 2.0];
        assert!(linalg::solve(a.view(), b.view()).is_err());
    }
}
