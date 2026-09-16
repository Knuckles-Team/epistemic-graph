use crate::{cluster, elementwise, linalg, random, reductions, stats};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;

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

mod conversion;
mod wire;
use conversion::{
    check_output_size, finish, norm_axis, py_array, to_bool_1d, to_f64_1d, to_f64_2d, to_f64_dyn,
};

// ---- reductions / stats (axis / keepdims / integer arrays — CONCEPT:EG-KG.compute.concept-4) ----
//
// Every axis/keepdims reduction binding is the same boundary work — coerce to
// a dynamic f64 array, normalize the axis, finish — around a different kernel
// reduction, so the bindings are declared rather than written out. `reduce_ddof`
// is the same shape for the two reductions that also take `ddof`.
macro_rules! reduce {
    ($name:ident, $python_name:literal, $flat:expr, $collapse:expr) => {
        #[pyfunction(name = $python_name)]
        #[pyo3(signature = (a, axis=None, keepdims=false))]
        fn $name(
            py: Python<'_>,
            a: &Bound<'_, PyAny>,
            axis: Option<isize>,
            keepdims: bool,
        ) -> PyResult<Py<PyAny>> {
            let arr = to_f64_dyn(a)?;
            let ax = norm_axis(axis, arr.ndim())?;
            finish(py, arr, ax, keepdims, $flat, $collapse)
        }
    };
}
macro_rules! reduce_ddof {
    ($name:ident, $python_name:literal, $flat:path, $collapse:path) => {
        #[pyfunction(name = $python_name)]
        #[pyo3(signature = (a, axis=None, ddof=0, keepdims=false))]
        fn $name(
            py: Python<'_>,
            a: &Bound<'_, PyAny>,
            axis: Option<isize>,
            ddof: usize,
            keepdims: bool,
        ) -> PyResult<Py<PyAny>> {
            let arr = to_f64_dyn(a)?;
            let ax = norm_axis(axis, arr.ndim())?;
            finish(
                py,
                arr,
                ax,
                keepdims,
                |v| Ok($flat(v, ddof)),
                |v, k| $collapse(v, k, ddof),
            )
        }
    };
}
reduce!(sum, "sum", |v| Ok(reductions::sum(v)), reductions::sum_axis);
reduce!(
    prod,
    "prod",
    |v| Ok(reductions::prod(v)),
    reductions::prod_axis
);
reduce!(
    mean,
    "mean",
    |v| Ok(reductions::mean(v)),
    reductions::mean_axis
);
reduce!(amin, "amin", reductions::min, reductions::min_axis);
reduce!(amax, "amax", reductions::max, reductions::max_axis);
reduce!(
    argmin,
    "argmin",
    |v| reductions::argmin(v).map(|index| index as i64),
    reductions::argmin_axis
);
reduce!(
    argmax,
    "argmax",
    |v| reductions::argmax(v).map(|index| index as i64),
    reductions::argmax_axis
);
reduce_ddof!(var, "var", reductions::var, reductions::var_axis);
reduce_ddof!(std_, "std", reductions::std, reductions::std_axis);

#[pyfunction]
fn argsort(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let input = to_f64_1d(a)?;
    let idx = reductions::argsort(input.view())
        .into_iter()
        .map(|i| i as i64)
        .collect::<Vec<_>>();
    py_array(py, ndarray::Array1::from_vec(idx).into_dyn())
}
#[pyfunction]
fn cumsum(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let input = to_f64_1d(a)?;
    py_array(py, reductions::cumsum(input.view()).into_dyn())
}
#[pyfunction]
fn cumprod(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let input = to_f64_1d(a)?;
    py_array(py, reductions::cumprod(input.view()).into_dyn())
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
            py_array(py, $f(input.view()).into_dyn())
        }
    };
}
ew1!(sqrt, elementwise::sqrt);
ew1!(log, elementwise::log);
ew1!(exp, elementwise::exp);
ew1!(absolute, elementwise::abs);
ew1!(tanh, elementwise::tanh);

mod arrays;
use arrays::*;

#[pyfunction]
fn clip(py: Python<'_>, a: &Bound<'_, PyAny>, lo: f64, hi: f64) -> PyResult<Py<PyAny>> {
    let input = to_f64_1d(a)?;
    py_array(py, elementwise::clip(input.view(), lo, hi).into_dyn())
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
    py_array(
        py,
        elementwise::nan_to_num(input.view(), nan, posinf, neginf),
    )
}
#[pyfunction]
fn isnan(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let input = to_f64_1d(a)?;
    py_array(
        py,
        ndarray::Array1::from_vec(elementwise::isnan(input.view())).into_dyn(),
    )
}
// The two element-wise pair bindings share the `ew1` treatment above, one
// operand wider.
macro_rules! ew2 {
    ($name:ident, $f:path) => {
        #[pyfunction]
        fn $name(
            py: Python<'_>,
            a: &Bound<'_, PyAny>,
            b: &Bound<'_, PyAny>,
        ) -> PyResult<Py<PyAny>> {
            let left = to_f64_1d(a)?;
            let right = to_f64_1d(b)?;
            py_array(
                py,
                $f(left.view(), right.view()).map_err(map_err)?.into_dyn(),
            )
        }
    };
}
ew2!(maximum, elementwise::maximum);
ew2!(minimum, elementwise::minimum);
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
    py_array(
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
// Dense-linalg bindings come in three shapes: one matrix in / one array out,
// a matrix plus a second operand / one array out, and one matrix in / a
// factorization pair out. Each coerces its operands, runs the kernel with the
// GIL detached, and returns nested Python lists — so the shape is declared
// once and each operation names only its kernel and its operand widths.
macro_rules! dense_unary {
    ($name:ident, $f:path) => {
        #[pyfunction]
        fn $name(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
            let input = to_f64_2d(a)?;
            let out = py.detach(|| $f(input.view())).map_err(map_err)?;
            py_array(py, out.into_dyn())
        }
    };
}
macro_rules! dense_binary {
    ($name:ident, $coerce_right:ident, $f:path) => {
        #[pyfunction]
        fn $name(
            py: Python<'_>,
            a: &Bound<'_, PyAny>,
            b: &Bound<'_, PyAny>,
        ) -> PyResult<Py<PyAny>> {
            let left = to_f64_2d(a)?;
            let right = $coerce_right(b)?;
            let out = py
                .detach(|| $f(left.view(), right.view()))
                .map_err(map_err)?;
            py_array(py, out.into_dyn())
        }
    };
}
macro_rules! dense_factorization {
    ($name:ident, $f:path) => {
        #[pyfunction]
        fn $name(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
            let input = to_f64_2d(a)?;
            let (first, second) = py.detach(|| $f(input.view())).map_err(map_err)?;
            Ok((
                py_array(py, first.into_dyn())?,
                py_array(py, second.into_dyn())?,
            ))
        }
    };
}
// Two 1-D operands reduced to a scalar summary: the inner product and the
// two-sample statistics differ only in the kernel and the summary type.
macro_rules! pairwise_scalar {
    ($(#[$doc:meta])* $name:ident, $f:path, $summary:ty) => {
        $(#[$doc])*
        #[pyfunction]
        fn $name(a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<$summary> {
            let left = to_f64_1d(a)?;
            let right = to_f64_1d(b)?;
            $f(left.view(), right.view()).map_err(map_err)
        }
    };
}
pairwise_scalar!(dot, linalg::dot, f64);
dense_binary!(matmul, to_f64_2d, linalg::matmul);
dense_binary!(solve, to_f64_1d, linalg::solve);
#[pyfunction]
fn svdvals(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let input = to_f64_2d(a)?;
    let s = py.detach(|| linalg::svdvals(input.view()));
    py_array(py, ndarray::Array1::from_vec(s).into_dyn())
}
#[pyfunction]
fn svd(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
    let input = to_f64_2d(a)?;
    let (u, s, vt) = py.detach(|| linalg::svd(input.view())).map_err(map_err)?;
    Ok((
        py_array(py, u.into_dyn())?,
        py_array(py, s.into_dyn())?,
        py_array(py, vt.into_dyn())?,
    ))
}
dense_factorization!(eigh, linalg::eigh);
/// `scipy.sparse.linalg.eigsh(A, k, which="SM")` — the k smallest-magnitude
/// symmetric eigenpairs (CONCEPT:EG-KG.compute.concept-5). Dense first cut (O(n^3)).
#[pyfunction]
fn eigsh(py: Python<'_>, a: &Bound<'_, PyAny>, k: usize) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    let input = to_f64_2d(a)?;
    let (w, v) = py
        .detach(|| linalg::eigsh_smallest(input.view(), k))
        .map_err(map_err)?;
    Ok((py_array(py, w.into_dyn())?, py_array(py, v.into_dyn())?))
}
dense_unary!(pinv, linalg::pinv);
dense_binary!(lstsq, to_f64_1d, linalg::lstsq);
dense_factorization!(qr, linalg::qr);
dense_unary!(cholesky, linalg::cholesky);
#[pyfunction]
fn det(py: Python<'_>, a: &Bound<'_, PyAny>) -> PyResult<f64> {
    let input = to_f64_2d(a)?;
    py.detach(|| linalg::det(input.view())).map_err(map_err)
}
dense_unary!(inv, linalg::inverse);
#[pyfunction]
fn matrix_power(py: Python<'_>, a: &Bound<'_, PyAny>, p: i64) -> PyResult<Py<PyAny>> {
    let input = to_f64_2d(a)?;
    let out = py
        .detach(|| linalg::matrix_power(input.view(), p))
        .map_err(map_err)?;
    py_array(py, out.into_dyn())
}

// ---- scipy.stats-parity ops (CONCEPT:EG-KG.compute.numeric-stats/EG-358) ----
pairwise_scalar!(
    /// `scipy.stats.spearmanr(a, b)` → `(rho, pvalue)`.
    spearmanr,
    stats::spearmanr,
    (f64, f64)
);
pairwise_scalar!(
    /// `scipy.stats.ks_2samp(a, b)` → `(statistic, pvalue)` (asymptotic p-value).
    ks_2samp,
    stats::ks_2samp,
    (f64, f64)
);
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
        py_array(py, ndarray::Array1::from_vec(labels).into_dyn())?,
        py_array(py, res.centroids.into_dyn())?,
    ))
}

// ---- random ----
//
// The bounded two-parameter draws differ only in the parameter type and the
// kernel they call; the size bound, the seeded generator and the result
// conversion are the same for all three.
macro_rules! bounded_draw {
    ($name:ident, $first:ident: $parameter:ty, $second:ident, $draw:ident) => {
        #[pyfunction]
        #[pyo3(signature = ($first, $second, size, seed))]
        fn $name(
            py: Python<'_>,
            $first: $parameter,
            $second: $parameter,
            size: usize,
            seed: u64,
        ) -> PyResult<Py<PyAny>> {
            check_output_size(size)?;
            let mut generator = random::Generator::new(seed);
            let values = generator.$draw($first, $second, size).map_err(map_err)?;
            py_array(py, ndarray::Array1::from_vec(values).into_dyn())
        }
    };
}
bounded_draw!(normal, loc: f64, scale, try_normal);
bounded_draw!(uniform, low: f64, high, try_uniform);
bounded_draw!(integers, low: i64, high, try_integers);

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
    py_array(py, ndarray::Array1::from_vec(values).into_dyn())
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
    py_array(py, ndarray::Array1::from_vec(values).into_dyn())
}

/// The `epistemic_graph.numeric` extension module.
#[pymodule]
fn numeric(m: &Bound<'_, PyModule>) -> PyResult<()> {
    wire::register(m)?;
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
