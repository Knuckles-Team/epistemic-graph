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
//!
//! DEFERRED within P4 (next increment): `pca(col, k)` / `svd(matrix)`. Both need a
//! column→`Array2` marshalling step (a set of vector columns, or a `List<List<Float64>>`
//! matrix argument, pivoted into a dense `ndarray::Array2`) that is materially more work
//! than the scalar/aggregate marshalling here; the faer SVD kernel
//! ([`eg_numeric::linalg::svd`]) is already present and validated, so this is purely the
//! columnar↔matrix bridge. See `docs/architecture/numeric-kernel.md`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float32Builder, Float64Array, ListArray, ListBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{
    create_udaf, Accumulator, AggregateUDF, ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use ndarray::Array1;

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
