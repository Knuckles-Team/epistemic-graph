//! Scalar UDFs (CONCEPT:KG-2.178) that decode the raw `props: Binary` msgpack blob
//! to reach a field the inferred schema widened (e.g. JSON-stringified) or dropped:
//!   json_get(props, key)     -> Utf8   (string value, or JSON-stringified scalar)
//!   json_get_f64(props, key) -> Float64
//!   json_get_i64(props, key) -> Int64
//! All return null when the blob doesn't decode, the key is absent, or the value
//! can't be coerced to the requested type.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use serde_json::Value;

/// Decode a single msgpack blob and return the JSON value at `key`, if present.
fn field(blob: &[u8], key: &str) -> Option<Value> {
    match rmp_serde::from_slice::<Value>(blob).ok()? {
        Value::Object(mut m) => m.remove(key),
        _ => None,
    }
}

/// Pull the (props, key) argument pair out of the columnar inputs as plain arrays.
/// `key` is taken from the first row (a scalar literal in practice); we read it per
/// call to stay correct even if a column is passed.
fn as_inputs(args: &[ColumnarValue]) -> datafusion::error::Result<(BinaryArray, StringArray)> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let props = arrays[0]
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "json_get: first argument must be Binary (props)".into(),
            )
        })?
        .clone();
    let keys = arrays[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "json_get: second argument must be Utf8 (key)".into(),
            )
        })?
        .clone();
    Ok((props, keys))
}

/// json_get(props, key) -> Utf8
pub(crate) fn json_get_udf() -> ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| {
        let (props, keys) = as_inputs(args)?;
        let out: StringArray = (0..props.len())
            .map(|i| {
                if props.is_null(i) || keys.is_null(i) {
                    return None;
                }
                match field(props.value(i), keys.value(i)) {
                    Some(Value::String(s)) => Some(s),
                    Some(Value::Null) | None => None,
                    Some(other) => Some(other.to_string()),
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    });
    create_udf(
        "json_get",
        vec![DataType::Binary, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        fun,
    )
}

/// json_get_f64(props, key) -> Float64
pub(crate) fn json_get_f64_udf() -> ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| {
        let (props, keys) = as_inputs(args)?;
        let out: Float64Array = (0..props.len())
            .map(|i| {
                if props.is_null(i) || keys.is_null(i) {
                    return None;
                }
                field(props.value(i), keys.value(i)).and_then(|v| match v {
                    Value::Number(_) => v.as_f64(),
                    Value::String(s) => s.parse::<f64>().ok(),
                    _ => None,
                })
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    });
    create_udf(
        "json_get_f64",
        vec![DataType::Binary, DataType::Utf8],
        DataType::Float64,
        Volatility::Immutable,
        fun,
    )
}

/// json_get_i64(props, key) -> Int64
pub(crate) fn json_get_i64_udf() -> ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| {
        let (props, keys) = as_inputs(args)?;
        let out: Int64Array = (0..props.len())
            .map(|i| {
                if props.is_null(i) || keys.is_null(i) {
                    return None;
                }
                field(props.value(i), keys.value(i)).and_then(|v| match v {
                    Value::Number(_) => v.as_i64(),
                    Value::String(s) => s.parse::<i64>().ok(),
                    _ => None,
                })
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    });
    create_udf(
        "json_get_i64",
        vec![DataType::Binary, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        fun,
    )
}

// ── epistemic_decay scalar UDF (CONCEPT:KG-2.184) ──────────────────────────

/// Ebbinghaus 30-day-half-life confidence decay, lifted from the engine's
/// `weight_semantic_results` (`src/server/compute.rs` ~line 44) so SQL projections
/// can salience-weight a fact in-query:
///   `epistemic_decay(confidence, valid_from, now) -> Float64`
/// All three args are Float64 (epoch-seconds for `valid_from`/`now`). Returns
///   confidence                                    when now <= valid_from
///   confidence * exp(-ln2/30 * age_days)          when now  > valid_from
/// and null if any argument is null. Pure / immutable scalar.
pub(crate) fn epistemic_decay_udf() -> ScalarUDF {
    let fun = Arc::new(|args: &[ColumnarValue]| {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let want = |i: usize| -> datafusion::error::Result<Float64Array> {
            arrays[i]
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "epistemic_decay: arguments must be Float64".into(),
                    )
                })
        };
        let confidence = want(0)?;
        let valid_from = want(1)?;
        let now = want(2)?;
        // Half-life of 30 days (decay rate lambda = ln(2) / 30), matching
        // weight_semantic_results.
        let decay_rate = std::f64::consts::LN_2 / 30.0;
        let out: Float64Array = (0..confidence.len())
            .map(|i| {
                if confidence.is_null(i) || valid_from.is_null(i) || now.is_null(i) {
                    return None;
                }
                let c = confidence.value(i);
                let vf = valid_from.value(i);
                let n = now.value(i);
                if n > vf {
                    let age_days = (n - vf) / 86_400.0;
                    Some(c * (-decay_rate * age_days).exp())
                } else {
                    Some(c)
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    });
    create_udf(
        "epistemic_decay",
        vec![DataType::Float64, DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        fun,
    )
}

// ── pgvector distance UDFs (CONCEPT:EG-115) ─────────────────────────────────

/// Extract row `row` of `array` as a dense `Vec<f32>` (CONCEPT:EG-115), accepting the
/// forms a vector argument arrives in: a `List`/`FixedSizeList` of `Float32`/`Float64`
/// (a stored vector column, materialized as `List<Float32>`), or a `Utf8` pgvector text
/// literal `[1,2,3]` (an `ORDER BY emb <-> '[1,2,3]'` query literal). Returns `None` for
/// a NULL or an unrecognized/unparseable value.
fn row_to_vector(array: &dyn Array, row: usize) -> Option<Vec<f32>> {
    use arrow::array::{
        Float32Array, Float64Array, LargeStringArray, ListArray, StringArray, StringViewArray,
    };
    use arrow::datatypes::DataType;
    if array.is_null(row) {
        return None;
    }
    let child_to_floats = |child: ArrayRef| -> Option<Vec<f32>> {
        if let Some(a) = child.as_any().downcast_ref::<Float32Array>() {
            return Some((0..a.len()).map(|i| a.value(i)).collect());
        }
        child
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| (0..a.len()).map(|i| a.value(i) as f32).collect())
    };
    match array.data_type() {
        DataType::Utf8 => crate::tables::schema::parse_vector_text(
            array.as_any().downcast_ref::<StringArray>()?.value(row),
        )
        .ok(),
        DataType::LargeUtf8 => crate::tables::schema::parse_vector_text(
            array.as_any().downcast_ref::<LargeStringArray>()?.value(row),
        )
        .ok(),
        DataType::Utf8View => crate::tables::schema::parse_vector_text(
            array.as_any().downcast_ref::<StringViewArray>()?.value(row),
        )
        .ok(),
        DataType::List(_) => {
            child_to_floats(array.as_any().downcast_ref::<ListArray>()?.value(row))
        }
        DataType::FixedSizeList(_, _) => child_to_floats(
            array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()?
                .value(row),
        ),
        _ => None,
    }
}

/// L2 (Euclidean) distance `‖a − b‖₂`. `None` on a dimension mismatch.
fn dist_l2(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }
    Some(
        a.iter()
            .zip(b)
            .map(|(x, y)| ((*x - *y) as f64).powi(2))
            .sum::<f64>()
            .sqrt(),
    )
}

/// Cosine distance `1 − (a·b)/(‖a‖‖b‖)`. `None` on a dimension mismatch; a zero-norm
/// operand yields distance `1.0` (maximally dissimilar), matching pgvector.
fn dist_cosine(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }
    let dot: f64 = a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum();
    let na: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|y| (*y as f64).powi(2)).sum::<f64>().sqrt();
    if na == 0.0 || nb == 0.0 {
        return Some(1.0);
    }
    Some(1.0 - dot / (na * nb))
}

/// Negative inner product `−(a·b)` — pgvector's `<#>` (so ascending order still ranks
/// most-similar first). `None` on a dimension mismatch.
fn dist_neg_ip(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }
    Some(-a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum::<f64>())
}

/// A pgvector distance scalar UDF (CONCEPT:EG-115): `fn(vector, vector) -> Float64`.
/// The signature accepts ANY two argument types (`Signature::any(2)`) so it takes both
/// a stored `List<Float32>` column AND a `Utf8` query literal (`'[1,2,3]'`) in either
/// position; each row's operands are decoded by [`row_to_vector`] and reduced by
/// `kernel`. A row where either operand doesn't decode yields NULL (never an error).
#[derive(Debug)]
struct VectorDistanceUdf {
    name: &'static str,
    signature: datafusion::logical_expr::Signature,
    kernel: fn(&[f32], &[f32]) -> Option<f64>,
}

impl datafusion::logical_expr::ScalarUDFImpl for VectorDistanceUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        if arrays.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} expects exactly 2 arguments",
                self.name
            )));
        }
        let n = arrays[0].len().max(arrays[1].len());
        let out: Float64Array = (0..n)
            .map(|i| {
                let li = i.min(arrays[0].len().saturating_sub(1));
                let ri = i.min(arrays[1].len().saturating_sub(1));
                let a = row_to_vector(arrays[0].as_ref(), li)?;
                let b = row_to_vector(arrays[1].as_ref(), ri)?;
                (self.kernel)(&a, &b)
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

fn vector_distance_udf(
    name: &'static str,
    kernel: fn(&[f32], &[f32]) -> Option<f64>,
) -> ScalarUDF {
    use datafusion::logical_expr::Signature;
    ScalarUDF::new_from_impl(VectorDistanceUdf {
        name,
        signature: Signature::any(2, Volatility::Immutable),
        kernel,
    })
}

/// `vector_l2(a, b)` — L2 distance, the `<->` operator (CONCEPT:EG-115).
pub(crate) fn vector_l2_udf() -> ScalarUDF {
    vector_distance_udf("vector_l2", dist_l2)
}

/// `vector_cosine(a, b)` — cosine distance, the `<=>` operator (CONCEPT:EG-115).
pub(crate) fn vector_cosine_udf() -> ScalarUDF {
    vector_distance_udf("vector_cosine", dist_cosine)
}

/// `vector_ip(a, b)` — negative inner product, the `<#>` operator (CONCEPT:EG-115).
pub(crate) fn vector_ip_udf() -> ScalarUDF {
    vector_distance_udf("vector_ip", dist_neg_ip)
}

// ── finance aggregate UDFs (CONCEPT:KG-2.184, feature `finance`) ────────────

/// `var(returns) -> Float64` and `cvar(returns) -> Float64` aggregate UDFs over a
/// Float64 returns column, delegating the kernel to `eg_compute::finance::risk`.
/// Aggregates (not scalars) because VaR/CVaR are sample statistics over the whole
/// column — a single accumulator buffers the returns and computes at `evaluate()`.
/// Confidence is fixed at 0.95 (the engine's risk-metrics default). Gated behind
/// `finance` so a no-finance build links neither these nor nalgebra.
#[cfg(feature = "finance")]
mod finance_udf {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Float64Array};
    use arrow::datatypes::DataType;
    use datafusion::error::Result as DfResult;
    use datafusion::logical_expr::{create_udaf, Accumulator, AggregateUDF, Volatility};
    use datafusion::scalar::ScalarValue;

    /// 95% confidence — the engine's default risk-metric level.
    const CONFIDENCE: f64 = 0.95;

    /// Accumulator that buffers every observed return, then applies a closure
    /// (historical_var / historical_cvar) at `evaluate`. State is the full buffer
    /// as a `List<Float64>` so multi-phase grouping merges losslessly.
    #[derive(Debug)]
    struct RiskAcc {
        returns: Vec<f64>,
        kernel: fn(&[f64], f64) -> f64,
    }

    impl RiskAcc {
        fn new(kernel: fn(&[f64], f64) -> f64) -> Self {
            Self {
                returns: Vec::new(),
                kernel,
            }
        }

        fn ingest(&mut self, values: &[ArrayRef]) -> DfResult<()> {
            let arr = values[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "var/cvar: argument must be Float64".into(),
                    )
                })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    self.returns.push(arr.value(i));
                }
            }
            Ok(())
        }
    }

    impl Accumulator for RiskAcc {
        fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
            self.ingest(values)
        }

        fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
            // Each state row is a List<Float64>; flatten its child values in.
            let lists = states[0]
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "var/cvar: state must be List<Float64>".into(),
                    )
                })?;
            for i in 0..lists.len() {
                if lists.is_null(i) {
                    continue;
                }
                let child = lists.value(i);
                self.ingest(&[child])?;
            }
            Ok(())
        }

        fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
            let scalars: Vec<ScalarValue> = self
                .returns
                .iter()
                .map(|v| ScalarValue::Float64(Some(*v)))
                .collect();
            let list =
                ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &DataType::Float64));
            Ok(vec![list])
        }

        fn evaluate(&mut self) -> DfResult<ScalarValue> {
            let v = (self.kernel)(&self.returns, CONFIDENCE);
            Ok(ScalarValue::Float64(Some(v)))
        }

        fn size(&self) -> usize {
            std::mem::size_of_val(self) + self.returns.capacity() * std::mem::size_of::<f64>()
        }
    }

    fn risk_udaf(name: &str, kernel: fn(&[f64], f64) -> f64) -> AggregateUDF {
        create_udaf(
            name,
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(move |_| Ok(Box::new(RiskAcc::new(kernel)) as Box<dyn Accumulator>)),
            // State: a single List<Float64> column (the buffered returns).
            Arc::new(vec![DataType::List(Arc::new(
                arrow::datatypes::Field::new("item", DataType::Float64, true),
            ))]),
        )
    }

    /// `var(returns)` — historical Value-at-Risk at 95%.
    pub(crate) fn var_udaf() -> AggregateUDF {
        risk_udaf("var", eg_compute::finance::risk::historical_var)
    }

    /// `cvar(returns)` — historical Conditional VaR (expected shortfall) at 95%.
    pub(crate) fn cvar_udaf() -> AggregateUDF {
        risk_udaf("cvar", eg_compute::finance::risk::historical_cvar)
    }
}

#[cfg(feature = "finance")]
pub(crate) use finance_udf::{cvar_udaf, var_udaf};
