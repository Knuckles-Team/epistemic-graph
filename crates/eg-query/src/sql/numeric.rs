//! Surface-B numeric analytics UDFs/UDAFs (CONCEPT:EG-329) — the Analytics-Program
//! numeric kernel (`eg-numeric`: faer + ndarray, BLAS/LAPACK-free) exposed as DataFusion
//! SQL operators so analytics run IN-ENGINE over resident columns (compute-near-data, no
//! fetch-to-Python, no FFI). Gated behind the `numeric` cargo feature (out of `pi`).
//!
//! Registered on the SQL `SessionContext` (see `exec::register_numeric`):
//!   * `cosine_sim(a, b) -> Float64`      — scalar: cosine similarity of two vectors,
//!     `a·b / (‖a‖‖b‖)`, backed by [`eg_numeric::linalg::dot`]/`norm`. Accepts the same
//!     operand forms as the pgvector distance UDFs (a stored `List<Float{32,64}>` column
//!     or a `'[1,2,3]'` text literal). NULL on a dimension mismatch / undecodable operand.
//!   * `l2_normalize(v) -> List<Float32>` — scalar: the unit vector `v/‖v‖`, kernel-normed
//!     (the engine's native pgvector `vector` type, so it feeds `cosine_sim`/ANN in-query).
//!     A zero-norm vector is returned unchanged (all-zero); NULL on an undecodable operand.
//!   * `zscore(col) -> Float64`           — scalar-over-batch: standardize a numeric column
//!     `(x - mean)/std` (population `ddof=0`), mean/std from [`eg_numeric::reductions`].
//!     DataFusion hands a scalar UDF the whole column batch at once, so this standardizes
//!     within the MATERIALIZED batch — exact for the engine's single-partition MemTable/
//!     NodesTableProvider materialization (one batch per table); a multi-batch scan would
//!     standardize per-batch (documented limitation — a true global two-pass is the window
//!     form `(x - avg(x) OVER ()) / stddev(x) OVER ()`). std==0 ⇒ 0.0; NULL-in ⇒ NULL-out.
//!   * `covariance(a, b) -> Float64`      — UDAF: sample covariance (`ddof=1`) over two
//!     Float64 columns, `Σ(aᵢ-ā)(bᵢ-b̄)/(n-1)`, means from [`eg_numeric::reductions::mean`].
//!     A single accumulator buffers the aligned pairs and computes at `evaluate()` so
//!     multi-phase grouping merges losslessly (state = two `List<Float64>` columns).
//!   * `svd(vec_col) -> List<Float64>`    — UDAF (CONCEPT:EG-336): the singular values
//!     (descending) of the matrix whose ROWS are the aggregated vector column, backed by
//!     [`eg_numeric::linalg::svdvals`]. Accepts the same operand forms as `cosine_sim`
//!     (a stored `List<Float{32,64}>` column or a `'[1,2,3]'` text literal); each row is
//!     one matrix row, so `n` rows of dimension `d` build an `n×d` `ndarray::Array2`.
//!   * `pca(vec_col, k) -> List<List<Float64>>` — UDAF (CONCEPT:EG-335): the top-`k`
//!     principal-component DIRECTIONS (loadings) of the aggregated vector column. The rows
//!     are stacked into an `n×d` matrix, mean-centered per column, its `d×d` sample
//!     covariance (`ddof=1`, [`eg_numeric::reductions::mean`]) eigendecomposed via
//!     [`eg_numeric::linalg::eigh`]; the `k` eigenvectors of largest eigenvalue are
//!     returned **descending by explained variance** as a list of `k` `d`-length unit
//!     vectors. Sign is arbitrary (eigenvector convention). Projected coordinates are
//!     then `X_centered · componentsᵀ` downstream. `k` is clamped to `d`.
//!
//! Both `svd`/`pca` are the deferred-in-P4 column→`ndarray::Array2` marshalling: an
//! `Accumulator` buffers each row's decoded vector (row-major flat `Vec<f64>` + `dim`) and
//! pivots to a dense matrix at `evaluate()`, so multi-phase grouping merges losslessly
//! (state = a flat `List<Float64>` + the `dim`/`k` `Int64`s). See
//! `docs/architecture/numeric-kernel.md`.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Builder, Float64Array, Int64Array, ListArray, ListBuilder,
};
use arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    create_udaf, Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use ndarray::{Array1, Array2};

use super::udfs::row_to_vector;

/// Build a 1-D `ndarray` view-owning array from an f32 vector for the kernel (the kernel
/// operates on `f64`; embeddings arrive as `f32`).
fn to_f64(v: &[f32]) -> Array1<f64> {
    v.iter().map(|&x| x as f64).collect()
}

// ── cosine_sim(a, b) -> Float64 (scalar) ────────────────────────────────────

/// `cosine_sim(a, b)` — kernel-backed cosine similarity (CONCEPT:EG-329). Complements the
/// EG-115 `vector_cosine` DISTANCE UDF (`1 - sim`); this returns the raw similarity, the
/// value clustering/ranking wants. Backed by `eg_numeric::linalg::{dot, norm}`.
#[derive(Debug)]
struct CosineSimUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CosineSimUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "cosine_sim"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        if arrays.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "cosine_sim expects exactly 2 arguments".into(),
            ));
        }
        let n = arrays[0].len().max(arrays[1].len());
        let out: Float64Array = (0..n)
            .map(|i| {
                let li = i.min(arrays[0].len().saturating_sub(1));
                let ri = i.min(arrays[1].len().saturating_sub(1));
                let a = to_f64(&row_to_vector(arrays[0].as_ref(), li)?);
                let b = to_f64(&row_to_vector(arrays[1].as_ref(), ri)?);
                // Kernel dot errors on a dimension mismatch ⇒ NULL (never error).
                let dot = eg_numeric::linalg::dot(a.view(), b.view()).ok()?;
                let na = eg_numeric::linalg::norm(a.view());
                let nb = eg_numeric::linalg::norm(b.view());
                if na == 0.0 || nb == 0.0 {
                    None
                } else {
                    Some(dot / (na * nb))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

pub(crate) fn cosine_sim_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CosineSimUdf {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

// ── l2_normalize(v) -> List<Float32> (scalar) ───────────────────────────────

/// `l2_normalize(v)` — the unit vector `v/‖v‖` (CONCEPT:EG-329), kernel-normed. Returns a
/// `List<Float32>` (the engine's native pgvector `vector` type) so the normalized vector
/// can feed a subsequent `cosine_sim`/ANN op in-query and render as pgvector text. A
/// zero-norm vector is returned unchanged (all-zero, matching a safe divide).
#[derive(Debug)]
struct L2NormalizeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for L2NormalizeUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "l2_normalize"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float32,
            true,
        ))))
    }
    fn invoke(&self, args: &[ColumnarValue]) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        if arrays.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "l2_normalize expects exactly 1 argument".into(),
            ));
        }
        let col = arrays[0].as_ref();
        let n = col.len();
        let mut builder = ListBuilder::new(Float32Builder::new());
        for i in 0..n {
            match row_to_vector(col, i) {
                Some(v) => {
                    // Norm in f64 (kernel) for accuracy, emit the unit vector as f32.
                    let a = to_f64(&v);
                    let norm = eg_numeric::linalg::norm(a.view());
                    for x in a.iter() {
                        let out = if norm == 0.0 { *x } else { *x / norm };
                        builder.values().append_value(out as f32);
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        let list: ListArray = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(list) as ArrayRef))
    }
}

pub(crate) fn l2_normalize_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(L2NormalizeUdf {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

// ── zscore(col) -> Float64 (scalar-over-batch) ──────────────────────────────

/// `zscore(col)` — standardize a numeric column `(x - mean)/std` (CONCEPT:EG-329) with the
/// batch mean/std from [`eg_numeric::reductions`] (population `ddof=0`, matching
/// `scipy.stats.zscore`'s default). See the module docs on the single-batch semantics.
#[derive(Debug)]
struct ZScoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ZScoreUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "zscore"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        // Cast the argument column to Float64 so integer columns standardize too.
        let col = arrow::compute::cast(arrays[0].as_ref(), &DataType::Float64)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("zscore: {e}")))?;
        let col = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("zscore: argument must be numeric".into())
        })?;
        // Non-null values feed the mean/std; nulls pass through as null.
        let present: Vec<f64> = (0..col.len())
            .filter(|&i| !col.is_null(i))
            .map(|i| col.value(i))
            .collect();
        let arr = Array1::from(present);
        let mean = eg_numeric::reductions::mean(arr.view());
        let std = eg_numeric::reductions::std(arr.view(), 0);
        let out: Float64Array = (0..col.len())
            .map(|i| {
                if col.is_null(i) {
                    None
                } else if std == 0.0 {
                    Some(0.0)
                } else {
                    Some((col.value(i) - mean) / std)
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

pub(crate) fn zscore_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ZScoreUdf {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

// ── covariance(a, b) -> Float64 (UDAF) ──────────────────────────────────────

/// Accumulator buffering the aligned `(a, b)` pairs (both non-null) so the sample
/// covariance is computed once at `evaluate`. State is the two buffers as `List<Float64>`
/// columns for lossless multi-phase merge — the same shape as the finance risk UDAFs.
#[derive(Debug)]
struct CovAcc {
    a: Vec<f64>,
    b: Vec<f64>,
}

impl CovAcc {
    fn new() -> Self {
        Self {
            a: Vec::new(),
            b: Vec::new(),
        }
    }

    /// Pull aligned non-null pairs out of an `[a, b]` batch.
    fn ingest(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let want = |arr: &ArrayRef| -> DfResult<Float64Array> {
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "covariance: arguments must be Float64".into(),
                    )
                })
        };
        let a = want(&values[0])?;
        let b = want(&values[1])?;
        for i in 0..a.len() {
            if !a.is_null(i) && !b.is_null(i) {
                self.a.push(a.value(i));
                self.b.push(b.value(i));
            }
        }
        Ok(())
    }
}

impl Accumulator for CovAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        self.ingest(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        // Two List<Float64> state columns; flatten each row's child values in, in step.
        let flatten = |arr: &ArrayRef| -> DfResult<Vec<f64>> {
            let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "covariance: state must be List<Float64>".into(),
                )
            })?;
            let mut out = Vec::new();
            for i in 0..list.len() {
                if list.is_null(i) {
                    continue;
                }
                let child = list.value(i);
                let vals = child
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "covariance: state child must be Float64".into(),
                        )
                    })?;
                for j in 0..vals.len() {
                    if !vals.is_null(j) {
                        out.push(vals.value(j));
                    }
                }
            }
            Ok(out)
        };
        self.a.extend(flatten(&states[0])?);
        self.b.extend(flatten(&states[1])?);
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let to_list = |v: &[f64]| -> ScalarValue {
            let scalars: Vec<ScalarValue> =
                v.iter().map(|x| ScalarValue::Float64(Some(*x))).collect();
            ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &DataType::Float64))
        };
        Ok(vec![to_list(&self.a), to_list(&self.b)])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let n = self.a.len();
        if n < 2 {
            // Sample covariance is undefined for < 2 observations (numpy returns nan).
            return Ok(ScalarValue::Float64(Some(f64::NAN)));
        }
        let a = Array1::from(self.a.clone());
        let b = Array1::from(self.b.clone());
        let ma = eg_numeric::reductions::mean(a.view());
        let mb = eg_numeric::reductions::mean(b.view());
        let sxy: f64 = self
            .a
            .iter()
            .zip(&self.b)
            .map(|(x, y)| (x - ma) * (y - mb))
            .sum();
        Ok(ScalarValue::Float64(Some(sxy / (n as f64 - 1.0))))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (self.a.capacity() + self.b.capacity()) * std::mem::size_of::<f64>()
    }
}

pub(crate) fn covariance_udaf() -> AggregateUDF {
    create_udaf(
        "covariance",
        vec![DataType::Float64, DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(CovAcc::new()) as Box<dyn Accumulator>)),
        // State: two List<Float64> columns (the buffered a/b observations).
        Arc::new(vec![
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        ]),
    )
}

// ── svd(vec_col) / pca(vec_col, k) — column→Array2 marshalling UDAFs ─────────
//
// Both aggregate a COLUMN OF VECTORS into a dense matrix, then run a faer kernel. The
// deferred-in-P4 bridge is exactly this columnar↔matrix pivot: each ingested row decodes
// (via `row_to_vector`, same operand forms as `cosine_sim`) into one matrix ROW, buffered
// row-major into a flat `Vec<f64>` alongside the fixed `dim`; `evaluate()` reshapes to an
// `ndarray::Array2` and calls the kernel. State is the flat buffer as a `List<Float64>`
// plus `dim` (+ `k` for pca) as `Int64`, so partial-aggregate merge is lossless.

/// Which faer kernel a [`MatrixAcc`] runs at `evaluate()` over the marshalled matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatrixOp {
    /// `svd` — singular values (descending) as `List<Float64>` (CONCEPT:EG-336).
    Svd,
    /// `pca` — top-`k` principal-component directions as `List<List<Float64>>`
    /// (CONCEPT:EG-335).
    Pca,
}

/// Accumulator buffering a column of equal-length vectors row-major into a flat `Vec<f64>`
/// (`n·dim` values) so the matrix is materialized once at `evaluate()` (CONCEPT:EG-335/
/// EG-336). The first decodable row fixes `dim`; rows of a different length are skipped
/// (NULL-safe, never error), matching the scalar operators' undecodable-operand handling.
#[derive(Debug)]
struct MatrixAcc {
    op: MatrixOp,
    flat: Vec<f64>,
    dim: Option<usize>,
    /// `pca` only — the requested component count, read from the 2nd argument column.
    k: Option<usize>,
}

impl MatrixAcc {
    fn new(op: MatrixOp) -> Self {
        Self {
            op,
            flat: Vec::new(),
            dim: None,
            k: None,
        }
    }

    /// Push one decoded row into the flat buffer, fixing/enforcing `dim`.
    fn push_row(&mut self, v: &[f32]) {
        match self.dim {
            None => self.dim = Some(v.len()),
            Some(d) if d != v.len() => return, // ragged row — skip, don't corrupt the matrix
            _ => {}
        }
        self.flat.extend(v.iter().map(|&x| x as f64));
    }

    /// Rebuild the `n×dim` matrix from the flat buffer, or `None` if empty/degenerate.
    fn matrix(&self) -> Option<Array2<f64>> {
        let dim = self.dim.filter(|&d| d > 0)?;
        if self.flat.is_empty() || !self.flat.len().is_multiple_of(dim) {
            return None;
        }
        let n = self.flat.len() / dim;
        Array2::from_shape_vec((n, dim), self.flat.clone()).ok()
    }
}

impl Accumulator for MatrixAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let col = values[0].as_ref();
        for i in 0..col.len() {
            if let Some(v) = row_to_vector(col, i) {
                self.push_row(&v);
            }
        }
        // pca: the 2nd argument is the (constant) component count `k`.
        if self.op == MatrixOp::Pca && self.k.is_none() && values.len() > 1 {
            let ks = arrow::compute::cast(values[1].as_ref(), &DataType::Int64)
                .ok()
                .and_then(|a| a.as_any().downcast_ref::<Int64Array>().cloned());
            if let Some(ks) = ks {
                for i in 0..ks.len() {
                    if !ks.is_null(i) {
                        self.k = Some(ks.value(i).max(0) as usize);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        // state[0] = List<Float64> flat rows (one list per partial); state[1] = dim;
        // state[2] = k (pca). Flatten every partial's flat buffer in, preserving order.
        let list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "svd/pca: state[0] must be List<Float64>".into(),
                )
            })?;
        let dims = states[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "svd/pca: state[1] (dim) must be Int64".into(),
                )
            })?;
        for i in 0..list.len() {
            if !dims.is_null(i) {
                self.dim.get_or_insert(dims.value(i) as usize);
            }
            if list.is_null(i) {
                continue;
            }
            let child = list.value(i);
            if let Some(vals) = child.as_any().downcast_ref::<Float64Array>() {
                for j in 0..vals.len() {
                    if !vals.is_null(j) {
                        self.flat.push(vals.value(j));
                    }
                }
            }
        }
        if self.op == MatrixOp::Pca && self.k.is_none() && states.len() > 2 {
            if let Some(ks) = states[2].as_any().downcast_ref::<Int64Array>() {
                for i in 0..ks.len() {
                    if !ks.is_null(i) {
                        self.k = Some(ks.value(i).max(0) as usize);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let scalars: Vec<ScalarValue> = self
            .flat
            .iter()
            .map(|x| ScalarValue::Float64(Some(*x)))
            .collect();
        let flat = ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &DataType::Float64));
        let dim = ScalarValue::Int64(self.dim.map(|d| d as i64));
        let mut out = vec![flat, dim];
        if self.op == MatrixOp::Pca {
            out.push(ScalarValue::Int64(self.k.map(|k| k as i64)));
        }
        Ok(out)
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        match self.op {
            MatrixOp::Svd => Ok(svd_result(self.matrix().as_ref())),
            MatrixOp::Pca => Ok(pca_result(self.matrix().as_ref(), self.k.unwrap_or(0))),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.flat.capacity() * std::mem::size_of::<f64>()
    }
}

/// `evaluate` for `svd` — singular values (descending) as a `List<Float64>` scalar; a NULL
/// list on an empty/degenerate matrix.
fn svd_result(m: Option<&Array2<f64>>) -> ScalarValue {
    let Some(m) = m else {
        // NULL List<Float64>.
        return ScalarValue::new_null_list(DataType::Float64, true, 1);
    };
    let svals = eg_numeric::linalg::svdvals(m.view());
    let scalars: Vec<ScalarValue> = svals
        .into_iter()
        .map(|x| ScalarValue::Float64(Some(x)))
        .collect();
    ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &DataType::Float64))
}

/// `evaluate` for `pca` — top-`k` principal-component directions (descending by explained
/// variance) as a `List<List<Float64>>` scalar; a NULL list when the matrix is empty, has
/// `< 2` rows (sample covariance undefined), or the eigensolve fails.
fn pca_result(m: Option<&Array2<f64>>, k: usize) -> ScalarValue {
    let child_ty = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
    // NULL List<List<Float64>> (new_null_list wraps its arg one List deep).
    let null = || ScalarValue::new_null_list(child_ty.clone(), true, 1);
    let Some(m) = m else { return null() };
    let (n, d) = m.dim();
    if n < 2 || d == 0 || k == 0 {
        return null();
    }
    // Mean-center each column.
    let mut centered = m.clone();
    for j in 0..d {
        let col = centered.column(j).to_owned();
        let mean = eg_numeric::reductions::mean(col.view());
        for i in 0..n {
            centered[[i, j]] -= mean;
        }
    }
    // d×d sample covariance C = Xcᵀ·Xc / (n-1).
    let mut cov = Array2::<f64>::zeros((d, d));
    let denom = (n as f64) - 1.0;
    for a in 0..d {
        for b in a..d {
            let mut s = 0.0;
            for i in 0..n {
                s += centered[[i, a]] * centered[[i, b]];
            }
            let c = s / denom;
            cov[[a, b]] = c;
            cov[[b, a]] = c;
        }
    }
    // eigh → ascending eigenvalues, eigenvectors as columns; take the last `k` (largest),
    // reversed to descending explained variance.
    let Ok((_evals, evecs)) = eg_numeric::linalg::eigh(cov.view()) else {
        return null();
    };
    let k = k.min(d);
    let mut components: Vec<ScalarValue> = Vec::with_capacity(k);
    for idx in 0..k {
        let col = d - 1 - idx; // largest eigenvalue is the last column (ascending order)
        let pc: Vec<ScalarValue> = (0..d)
            .map(|r| ScalarValue::Float64(Some(evecs[[r, col]])))
            .collect();
        components.push(ScalarValue::List(ScalarValue::new_list_nullable(
            &pc,
            &DataType::Float64,
        )));
    }
    ScalarValue::List(ScalarValue::new_list_nullable(&components, &child_ty))
}

/// Shared `AggregateUDFImpl` for the two column→matrix operators. `Signature::any` accepts
/// the vector operand in any of `row_to_vector`'s forms (List / text) without coercion —
/// the same flexibility `cosine_sim` relies on.
#[derive(Debug)]
struct MatrixAggUdf {
    op: MatrixOp,
    name: &'static str,
    signature: Signature,
}

impl AggregateUDFImpl for MatrixAggUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        let float_list = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
        Ok(match self.op {
            MatrixOp::Svd => float_list,
            // List<List<Float64>>: k principal-component vectors.
            MatrixOp::Pca => DataType::List(Arc::new(Field::new("item", float_list, true))),
        })
    }
    fn accumulator(&self, _: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(MatrixAcc::new(self.op)))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<Field>> {
        let float_list = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
        let mut fields = vec![
            Field::new(format!("{}__flat", args.name), float_list, true),
            Field::new(format!("{}__dim", args.name), DataType::Int64, true),
        ];
        if self.op == MatrixOp::Pca {
            fields.push(Field::new(
                format!("{}__k", args.name),
                DataType::Int64,
                true,
            ));
        }
        Ok(fields)
    }
}

/// `svd(vec_col) -> List<Float64>` — singular values of the aggregated matrix
/// (CONCEPT:EG-336).
pub(crate) fn svd_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(MatrixAggUdf {
        op: MatrixOp::Svd,
        name: "svd",
        signature: Signature::any(1, Volatility::Immutable),
    })
}

/// `pca(vec_col, k) -> List<List<Float64>>` — top-`k` principal-component directions
/// (CONCEPT:EG-335).
pub(crate) fn pca_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(MatrixAggUdf {
        op: MatrixOp::Pca,
        name: "pca",
        signature: Signature::any(2, Volatility::Immutable),
    })
}
