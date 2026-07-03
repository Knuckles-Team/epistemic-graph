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
pub(crate) fn row_to_vector(array: &dyn Array, row: usize) -> Option<Vec<f32>> {
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
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()?
                .value(row),
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
    Some(
        -a.iter()
            .zip(b)
            .map(|(x, y)| *x as f64 * *y as f64)
            .sum::<f64>(),
    )
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

fn vector_distance_udf(name: &'static str, kernel: fn(&[f32], &[f32]) -> Option<f64>) -> ScalarUDF {
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

// ── TimescaleDB time_bucket (CONCEPT:EG-117) ────────────────────────────────

/// Parse a Postgres interval spelling (`'1 hour'`, `'30 minutes'`, `'15 min'`,
/// `'1 day'`, `'2 weeks'`, `'10 s'`) OR a bare integer to a width in SECONDS. `None`
/// for an un-parseable spelling.
fn interval_to_seconds(s: &str) -> Option<i64> {
    let t = s.trim();
    if let Ok(n) = t.parse::<i64>() {
        return Some(n);
    }
    let mut it = t.split_whitespace();
    let n: i64 = it.next()?.parse().ok()?;
    let unit = it.next().unwrap_or("seconds").to_ascii_lowercase();
    let mult = match unit.trim_end_matches('s') {
        "second" | "sec" | "s" => 1,
        "minute" | "min" | "m" => 60,
        "hour" | "hr" | "h" => 3_600,
        "day" | "d" => 86_400,
        "week" | "w" => 604_800,
        _ => return None,
    };
    Some(n * mult)
}

/// Read row `row` of `array` as an i64 epoch-seconds timestamp, accepting `Int64`,
/// `Float64` (truncated), or a `Timestamp` (converted to whole seconds).
fn row_to_epoch_secs(array: &dyn Array, row: usize) -> Option<i64> {
    use arrow::array::{
        Float64Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int64 => Some(array.as_any().downcast_ref::<Int64Array>()?.value(row)),
        DataType::Float64 => Some(array.as_any().downcast_ref::<Float64Array>()?.value(row) as i64),
        DataType::Timestamp(unit, _) => {
            let raw = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()?
                    .value(row),
                TimeUnit::Millisecond => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()?
                        .value(row)
                        / 1_000
                }
                TimeUnit::Microsecond => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()?
                        .value(row)
                        / 1_000_000
                }
                TimeUnit::Nanosecond => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()?
                        .value(row)
                        / 1_000_000_000
                }
            };
            Some(raw)
        }
        _ => None,
    }
}

/// Read row `row` of `array` as a UTF-8 string (any of the Arrow string encodings).
fn row_to_string(array: &dyn Array, row: usize) -> Option<String> {
    use arrow::array::{LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::DataType;
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(row)
                .to_string(),
        ),
        DataType::LargeUtf8 => Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()?
                .value(row)
                .to_string(),
        ),
        DataType::Utf8View => Some(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()?
                .value(row)
                .to_string(),
        ),
        _ => None,
    }
}

/// `time_bucket(width, ts) -> Int64` (CONCEPT:EG-117) — floor `ts` (epoch seconds) to
/// the start of its `width`-wide bucket, the TimescaleDB time-bucket label. `width` is
/// a Postgres interval string (`'1 hour'`) or an integer number of seconds; `ts` is an
/// Int64/Float64 epoch or a Timestamp. NULL on an un-parseable width or a NULL ts —
/// the same "never error" discipline as the other UDFs. This is the exact bucket
/// labelling the eg-tsdb `Op::Window` (EG-067) aggregation uses; gap-fill is a follow-up.
pub(crate) fn time_bucket_udf() -> ScalarUDF {
    use datafusion::logical_expr::{ScalarUDFImpl, Signature};
    #[derive(Debug)]
    struct TimeBucketUdf {
        signature: Signature,
    }
    impl ScalarUDFImpl for TimeBucketUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "time_bucket"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            if arrays.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "time_bucket expects exactly 2 arguments (width, ts)".into(),
                ));
            }
            let n = arrays[0].len().max(arrays[1].len());
            let out: Int64Array = (0..n)
                .map(|i| {
                    let wi = i.min(arrays[0].len().saturating_sub(1));
                    let ti = i.min(arrays[1].len().saturating_sub(1));
                    let width = row_to_string(arrays[0].as_ref(), wi)
                        .and_then(|s| interval_to_seconds(&s))
                        .or_else(|| row_to_epoch_secs(arrays[0].as_ref(), wi))?;
                    if width <= 0 {
                        return None;
                    }
                    let ts = row_to_epoch_secs(arrays[1].as_ref(), ti)?;
                    Some((ts.div_euclid(width)) * width)
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
    ScalarUDF::new_from_impl(TimeBucketUdf {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

// ── ParadeDB BM25 UDFs (CONCEPT:EG-119) ─────────────────────────────────────

/// `bm25_match(col, query) -> Boolean` (CONCEPT:EG-119) — the desugaring target of the
/// ParadeDB `col @@@ 'query'` operator. A lexical term-contains filter (case-insensitive
/// whitespace tokens) so a `WHERE col @@@ 'q'` query runs directly through DataFusion;
/// the full BM25-ranked pushdown onto eg-text's Tantivy index is the server-side
/// lowering (see `plan_bm25_search`). NULL/absent operands ⇒ false.
pub(crate) fn bm25_match_udf() -> ScalarUDF {
    use arrow::array::BooleanArray;
    use datafusion::logical_expr::{ScalarUDFImpl, Signature};
    #[derive(Debug)]
    struct Bm25MatchUdf {
        signature: Signature,
    }
    impl ScalarUDFImpl for Bm25MatchUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "bm25_match"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Boolean)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            if arrays.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "bm25_match expects exactly 2 arguments (col, query)".into(),
                ));
            }
            let n = arrays[0].len().max(arrays[1].len());
            let out: BooleanArray = (0..n)
                .map(|i| {
                    let ci = i.min(arrays[0].len().saturating_sub(1));
                    let qi = i.min(arrays[1].len().saturating_sub(1));
                    let col = row_to_string(arrays[0].as_ref(), ci)?.to_ascii_lowercase();
                    let query = row_to_string(arrays[1].as_ref(), qi)?.to_ascii_lowercase();
                    Some(query.split_whitespace().any(|term| col.contains(term)))
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
    ScalarUDF::new_from_impl(Bm25MatchUdf {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

/// `bm25_score(…) -> Float64` — the desugaring target of `paradedb.score(...)`
/// (CONCEPT:EG-119), now backed by REAL BM25 relevance (CONCEPT:EG-311).
///
/// A per-row DataFusion scalar UDF only sees its own arguments — so which call form it
/// receives decides whether it can rank:
///   * `bm25_score(doc_text, 'query')` (2 args) ⇒ REAL per-row BM25 via
///     [`eg_text::bm25_score`] (the informative form: the text column + the search
///     query are both in scope).
///   * `bm25_score(id)` (the single-arg form the standard `@@@` + `paradedb.score(id)`
///     desugaring emits) ⇒ falls back to `1.0`, because the `@@@` query is NOT threaded
///     to a per-row UDF. The corpus-TRUE ranking for that path is the server-side
///     lowering of [`crate::Bm25SearchPlan`] onto `eg_text::TextIndex::search(query, k)`
///     — the ONLY place both the query and the whole corpus/index co-exist (as EG-119
///     noted). See the module docs on the two-path design.
pub(crate) fn bm25_score_udf() -> ScalarUDF {
    use datafusion::logical_expr::{ScalarUDFImpl, Signature, TypeSignature};
    #[derive(Debug)]
    struct Bm25ScoreUdf {
        signature: Signature,
    }
    impl ScalarUDFImpl for Bm25ScoreUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "bm25_score"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Float64)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            // 2-arg informative form `(doc_text, query)` ⇒ real per-row BM25.
            if args.len() == 2 {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let n = arrays[0].len().max(arrays[1].len());
                let out: Float64Array = (0..n)
                    .map(|i| {
                        let di = i.min(arrays[0].len().saturating_sub(1));
                        let qi = i.min(arrays[1].len().saturating_sub(1));
                        let doc = row_to_string(arrays[0].as_ref(), di)?;
                        let query = row_to_string(arrays[1].as_ref(), qi)?;
                        Some(eg_text::bm25_score(&query, &doc) as f64)
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef));
            }
            // Single-arg / other forms lack the query ⇒ neutral placeholder (see docs).
            let n = args
                .iter()
                .map(|a| match a {
                    ColumnarValue::Array(arr) => arr.len(),
                    ColumnarValue::Scalar(_) => 1,
                })
                .max()
                .unwrap_or(1);
            let out: Float64Array = (0..n).map(|_| Some(1.0)).collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
    ScalarUDF::new_from_impl(Bm25ScoreUdf {
        signature: Signature::one_of(
            vec![TypeSignature::VariadicAny, TypeSignature::Any(0)],
            Volatility::Immutable,
        ),
    })
}

/// `bm25_snippet(…) -> Utf8` — the desugaring target of `paradedb.snippet(...)`
/// (CONCEPT:EG-119), now producing a REAL highlighted fragment (CONCEPT:EG-311).
///
/// Like `bm25_score`, the call form decides what it can do:
///   * `bm25_snippet(doc_text, 'query' [, maxlen])` ⇒ a real `<b>…</b>`-highlighted
///     window via [`eg_text::bm25_snippet`] (default `maxlen` 200 when omitted).
///   * `bm25_snippet(doc_text)` (the single-arg form the standard desugaring emits) ⇒
///     echoes the text unchanged, because the `@@@` query needed to pick + highlight a
///     fragment is not threaded to a per-row UDF (the server-side lowering is the
///     query-aware path — see `bm25_score_udf` docs).
pub(crate) fn bm25_snippet_udf() -> ScalarUDF {
    use datafusion::logical_expr::{ScalarUDFImpl, Signature, TypeSignature};
    /// Default snippet width (chars) when `paradedb.snippet` is called without an
    /// explicit max length — a readable one-line fragment.
    const DEFAULT_MAXLEN: usize = 200;
    #[derive(Debug)]
    struct Bm25SnippetUdf {
        signature: Signature,
    }
    impl ScalarUDFImpl for Bm25SnippetUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "bm25_snippet"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let n = arrays.iter().map(|a| a.len()).max().unwrap_or(1);
            // 2+ args ⇒ query is in scope ⇒ real highlighted snippet.
            if arrays.len() >= 2 {
                let out: StringArray = (0..n)
                    .map(|i| {
                        let di = i.min(arrays[0].len().saturating_sub(1));
                        let qi = i.min(arrays[1].len().saturating_sub(1));
                        let doc = row_to_string(arrays[0].as_ref(), di)?;
                        let query = row_to_string(arrays[1].as_ref(), qi)?;
                        let maxlen = arrays
                            .get(2)
                            .and_then(|a| {
                                let mi = i.min(a.len().saturating_sub(1));
                                row_to_i64(a.as_ref(), mi)
                            })
                            .filter(|&m| m > 0)
                            .map(|m| m as usize)
                            .unwrap_or(DEFAULT_MAXLEN);
                        Some(eg_text::bm25_snippet(&query, &doc, maxlen))
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef));
            }
            // Single-arg form lacks the query ⇒ echo the text unchanged (see docs).
            let out: StringArray = (0..n)
                .map(|i| {
                    arrays.first().and_then(|a| {
                        let idx = i.min(a.len().saturating_sub(1));
                        row_to_string(a.as_ref(), idx)
                    })
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
    ScalarUDF::new_from_impl(Bm25SnippetUdf {
        signature: Signature::one_of(
            vec![TypeSignature::VariadicAny, TypeSignature::Any(0)],
            Volatility::Immutable,
        ),
    })
}

// ── greatest / least (CONCEPT:EG-104) ───────────────────────────────────────

/// The common result type for a `greatest`/`least` call (CONCEPT:EG-104): `Int64` when
/// every non-NULL argument is an integer, `Float64` when any is floating, else `Utf8`
/// (strings / mixed) — mirroring Postgres' "resolve to a common type" rule with a small
/// bounded lattice. NULL-typed arguments (a bare `NULL` literal) are neutral.
fn unify_minmax(types: &[DataType]) -> DataType {
    use DataType::*;
    let mut has_float = false;
    let mut all_num = true;
    let mut any = false;
    for t in types {
        match t {
            Null => {}
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => any = true,
            Float16 | Float32 | Float64 => {
                has_float = true;
                any = true;
            }
            _ => {
                all_num = false;
                any = true;
            }
        }
    }
    if !any {
        Int64
    } else if !all_num {
        Utf8
    } else if has_float {
        Float64
    } else {
        Int64
    }
}

/// `greatest(...)` / `least(...)` variadic scalar UDFs (CONCEPT:EG-104) — DataFusion 43
/// ships neither. Each argument column is cast to the unified result type
/// ([`unify_minmax`]) and reduced row-wise, IGNORING NULLs (Postgres semantics: the
/// result is NULL only when EVERY argument is NULL in that row). `is_greatest` picks max
/// vs min.
fn minmax_udf(name: &'static str, is_greatest: bool) -> ScalarUDF {
    use datafusion::logical_expr::{ScalarUDFImpl, Signature, TypeSignature};
    #[derive(Debug)]
    struct MinMaxUdf {
        name: &'static str,
        signature: Signature,
        is_greatest: bool,
    }
    impl ScalarUDFImpl for MinMaxUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            self.name
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(unify_minmax(arg_types))
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            if arrays.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "{} expects at least one argument",
                    self.name
                )));
            }
            let types: Vec<DataType> = arrays.iter().map(|a| a.data_type().clone()).collect();
            let target = unify_minmax(&types);
            let casted: Vec<ArrayRef> = arrays
                .iter()
                .map(|a| arrow::compute::cast(a, &target))
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{}: cast: {e}",
                        self.name
                    ))
                })?;
            let n = casted.iter().map(|a| a.len()).max().unwrap_or(0);
            let gt = self.is_greatest;
            let out: ArrayRef = match target {
                DataType::Int64 => {
                    let cols: Vec<&Int64Array> = casted
                        .iter()
                        .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
                        .collect();
                    let a: Int64Array = (0..n)
                        .map(|r| {
                            cols.iter()
                                .filter(|c| !c.is_null(r))
                                .map(|c| c.value(r))
                                .reduce(|x, y| if (y > x) == gt { y } else { x })
                        })
                        .collect();
                    Arc::new(a)
                }
                DataType::Float64 => {
                    let cols: Vec<&Float64Array> = casted
                        .iter()
                        .map(|a| a.as_any().downcast_ref::<Float64Array>().unwrap())
                        .collect();
                    let a: Float64Array = (0..n)
                        .map(|r| {
                            cols.iter()
                                .filter(|c| !c.is_null(r))
                                .map(|c| c.value(r))
                                .reduce(|x, y| if (y > x) == gt { y } else { x })
                        })
                        .collect();
                    Arc::new(a)
                }
                _ => {
                    let cols: Vec<&StringArray> = casted
                        .iter()
                        .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
                        .collect();
                    let a: StringArray = (0..n)
                        .map(|r| {
                            cols.iter()
                                .filter(|c| !c.is_null(r))
                                .map(|c| c.value(r))
                                .reduce(|x, y| if (y > x) == gt { y } else { x })
                                .map(|s| s.to_string())
                        })
                        .collect();
                    Arc::new(a)
                }
            };
            Ok(ColumnarValue::Array(out))
        }
    }
    ScalarUDF::new_from_impl(MinMaxUdf {
        name,
        signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        is_greatest,
    })
}

/// `greatest(...)` — the row-wise maximum, ignoring NULLs (CONCEPT:EG-104).
pub(crate) fn greatest_udf() -> ScalarUDF {
    minmax_udf("greatest", true)
}

/// `least(...)` — the row-wise minimum, ignoring NULLs (CONCEPT:EG-104).
pub(crate) fn least_udf() -> ScalarUDF {
    minmax_udf("least", false)
}

// ── PostgreSQL range types (CONCEPT:EG-104) ─────────────────────────────────

/// A range normalized to a discrete half-open integer interval `[start, end)`
/// (CONCEPT:EG-104). Postgres' full range-type fidelity (a dedicated type OID, discrete
/// canonicalization, float/`numeric` element ranges, and multiranges) is heavier than
/// DataFusion's type system supports, so EG-104 models a range PRAGMATICALLY as its
/// canonical Postgres TEXT form `[lo,hi)` and reduces it to a discrete i64 half-open
/// interval for the predicates. An empty/omitted bound means unbounded (`i64::MIN` /
/// `i64::MAX`); the literal `empty` (or any `start >= end`) is the empty range.
///
/// DOCUMENTED LIMITATION: bounds are coerced to i64 — `int4range` uses the integers
/// directly, `tsrange` uses whole epoch-SECONDS (sub-second precision is dropped) — and
/// exclusive bounds are canonicalized on the integer grid (`(l` ⇒ `l+1`, `h]` ⇒ `h+1`).
/// This is exact for integer/second-granular points (the common ORM/BI filter), but is
/// NOT a faithful continuous `tsrange` nor a `numrange`; those remain a follow-up.
#[derive(Clone, Copy, Debug)]
struct HalfOpen {
    empty: bool,
    start: i64,
    end: i64,
}

impl HalfOpen {
    fn empty() -> Self {
        Self {
            empty: true,
            start: 0,
            end: 0,
        }
    }
    fn contains_point(&self, x: i64) -> bool {
        !self.empty && self.start <= x && x < self.end
    }
    fn overlaps(&self, other: &HalfOpen) -> bool {
        !self.empty && !other.empty && self.start < other.end && other.start < self.end
    }
    /// `self @> other` — does `self` cover all of `other`?
    fn contains_range(&self, other: &HalfOpen) -> bool {
        if other.empty {
            return true;
        }
        if self.empty {
            return false;
        }
        self.start <= other.start && other.end <= self.end
    }
}

/// Parse a Postgres range text form (`[1,5)`, `(0,10]`, `[10,)`, `empty`) into the
/// discrete half-open interval (CONCEPT:EG-104). `None` when the text isn't a range —
/// the predicate UDFs then yield NULL (the "never error" discipline). Bounds are parsed
/// as i64; an omitted bound is ±∞.
fn parse_range(s: &str) -> Option<HalfOpen> {
    let t = s.trim();
    if t.eq_ignore_ascii_case("empty") {
        return Some(HalfOpen::empty());
    }
    let bytes = t.as_bytes();
    if bytes.len() < 3 {
        return None;
    }
    let lo_inc = match bytes[0] {
        b'[' => true,
        b'(' => false,
        _ => return None,
    };
    let hi_inc = match bytes[bytes.len() - 1] {
        b']' => true,
        b')' => false,
        _ => return None,
    };
    let inner = &t[1..t.len() - 1];
    let (lo_str, hi_str) = inner.split_once(',')?;
    let parse_bound = |b: &str| -> Option<Option<i64>> {
        let b = b.trim().trim_matches('"');
        if b.is_empty() {
            Some(None) // unbounded
        } else {
            b.parse::<i64>().ok().map(Some)
        }
    };
    let lo = parse_bound(lo_str)?;
    let hi = parse_bound(hi_str)?;
    // Normalize to a discrete half-open interval [start, end).
    let start = match lo {
        None => i64::MIN,
        Some(l) if lo_inc => l,
        Some(l) => l.saturating_add(1),
    };
    let end = match hi {
        None => i64::MAX,
        Some(h) if hi_inc => h.saturating_add(1),
        Some(h) => h,
    };
    if start >= end {
        Some(HalfOpen::empty())
    } else {
        Some(HalfOpen {
            empty: false,
            start,
            end,
        })
    }
}

/// Read row `row` of `array` as an i64, reusing [`row_to_epoch_secs`] (Int64/Float64/
/// Timestamp) and additionally accepting an integer-valued Utf8 string.
fn row_to_i64(array: &dyn Array, row: usize) -> Option<i64> {
    row_to_epoch_secs(array, row).or_else(|| row_to_string(array, row)?.trim().parse::<i64>().ok())
}

/// `int4range(lo, hi[, bounds]) -> Utf8` (CONCEPT:EG-104) — the Postgres integer-range
/// constructor. Emits the canonical text `[lo,hi)` (2-arg default `'[)'`); the optional
/// 3rd argument is a bounds spelling (`'[]'`, `'[)'`, `'(]'`, `'()'`). An omitted/NULL
/// bound is rendered as unbounded (`[lo,)`). NULL when both bounds are NULL.
pub(crate) fn int4range_udf() -> ScalarUDF {
    range_ctor_udf("int4range")
}

/// `tsrange(lo, hi[, bounds]) -> Utf8` (CONCEPT:EG-104) — the Postgres timestamp-range
/// constructor. Bounds are coerced to whole epoch-seconds (see [`HalfOpen`] limitation)
/// and rendered as the canonical text `[lo,hi)`, so `tsrange` and `int4range` share the
/// same predicate UDFs.
pub(crate) fn tsrange_udf() -> ScalarUDF {
    range_ctor_udf("tsrange")
}

/// Shared range constructor: `(lo, hi[, bounds]) -> Utf8` canonical range text.
fn range_ctor_udf(name: &'static str) -> ScalarUDF {
    use datafusion::logical_expr::{ScalarUDFImpl, Signature, TypeSignature};
    #[derive(Debug)]
    struct RangeCtorUdf {
        name: &'static str,
        signature: Signature,
    }
    impl ScalarUDFImpl for RangeCtorUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            self.name
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            if arrays.len() < 2 {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "{} expects (lo, hi[, bounds])",
                    self.name
                )));
            }
            let n = arrays.iter().map(|a| a.len()).max().unwrap_or(1);
            let out: StringArray = (0..n)
                .map(|i| {
                    let li = i.min(arrays[0].len().saturating_sub(1));
                    let hi_i = i.min(arrays[1].len().saturating_sub(1));
                    let lo = row_to_i64(arrays[0].as_ref(), li);
                    let hi = row_to_i64(arrays[1].as_ref(), hi_i);
                    if lo.is_none() && hi.is_none() {
                        return None;
                    }
                    // Optional bounds spelling (e.g. "[]"); default half-open "[)".
                    let bounds = arrays
                        .get(2)
                        .and_then(|a| {
                            let bi = i.min(a.len().saturating_sub(1));
                            row_to_string(a.as_ref(), bi)
                        })
                        .filter(|s| s.len() == 2)
                        .unwrap_or_else(|| "[)".to_string());
                    let b = bounds.as_bytes();
                    let lo_s = lo.map(|v| v.to_string()).unwrap_or_default();
                    let hi_s = hi.map(|v| v.to_string()).unwrap_or_default();
                    Some(format!("{}{lo_s},{hi_s}{}", b[0] as char, b[1] as char))
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
    ScalarUDF::new_from_impl(RangeCtorUdf {
        name,
        signature: Signature::one_of(
            vec![TypeSignature::Any(2), TypeSignature::Any(3)],
            Volatility::Immutable,
        ),
    })
}

/// `range_contains(range, value) -> Boolean` (CONCEPT:EG-104) — the range `@>` element
/// test: is the point `value` inside the range text `range`? NULL operands ⇒ NULL.
pub(crate) fn range_contains_udf() -> ScalarUDF {
    use arrow::array::BooleanArray;
    use datafusion::logical_expr::{ScalarUDFImpl, Signature};
    #[derive(Debug)]
    struct RangeContainsUdf {
        signature: Signature,
    }
    impl ScalarUDFImpl for RangeContainsUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "range_contains"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Boolean)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            if arrays.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "range_contains expects (range, value)".into(),
                ));
            }
            let n = arrays[0].len().max(arrays[1].len());
            let out: BooleanArray = (0..n)
                .map(|i| {
                    let ri = i.min(arrays[0].len().saturating_sub(1));
                    let vi = i.min(arrays[1].len().saturating_sub(1));
                    let range = parse_range(&row_to_string(arrays[0].as_ref(), ri)?)?;
                    let x = row_to_i64(arrays[1].as_ref(), vi)?;
                    Some(range.contains_point(x))
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
    ScalarUDF::new_from_impl(RangeContainsUdf {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

/// A binary range→range predicate UDF (CONCEPT:EG-104): `(range, range) -> Boolean`.
/// Backs `range_overlaps` (`&&`), `range_contains_range` (`@>`), and `range_contained_by`
/// (`<@`) — each parses both text operands and applies `kernel`. NULL operands ⇒ NULL.
fn range_pred_udf(name: &'static str, kernel: fn(&HalfOpen, &HalfOpen) -> bool) -> ScalarUDF {
    use arrow::array::BooleanArray;
    use datafusion::logical_expr::{ScalarUDFImpl, Signature};
    #[derive(Debug)]
    struct RangePredUdf {
        name: &'static str,
        signature: Signature,
        kernel: fn(&HalfOpen, &HalfOpen) -> bool,
    }
    impl ScalarUDFImpl for RangePredUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            self.name
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Boolean)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            if arrays.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "{} expects (range, range)",
                    self.name
                )));
            }
            let n = arrays[0].len().max(arrays[1].len());
            let out: BooleanArray = (0..n)
                .map(|i| {
                    let ai = i.min(arrays[0].len().saturating_sub(1));
                    let bi = i.min(arrays[1].len().saturating_sub(1));
                    let a = parse_range(&row_to_string(arrays[0].as_ref(), ai)?)?;
                    let b = parse_range(&row_to_string(arrays[1].as_ref(), bi)?)?;
                    Some((self.kernel)(&a, &b))
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
    ScalarUDF::new_from_impl(RangePredUdf {
        name,
        signature: Signature::any(2, Volatility::Immutable),
        kernel,
    })
}

/// `range_overlaps(a, b) -> Boolean` (CONCEPT:EG-104) — the range `&&` overlap test.
pub(crate) fn range_overlaps_udf() -> ScalarUDF {
    range_pred_udf("range_overlaps", |a, b| a.overlaps(b))
}

/// `range_contains_range(a, b) -> Boolean` (CONCEPT:EG-104) — the range `@>` test
/// (`a` covers all of `b`).
pub(crate) fn range_contains_range_udf() -> ScalarUDF {
    range_pred_udf("range_contains_range", |a, b| a.contains_range(b))
}

/// `range_contained_by(a, b) -> Boolean` (CONCEPT:EG-104) — the range `<@` test
/// (`a` is covered by `b`).
pub(crate) fn range_contained_by_udf() -> ScalarUDF {
    range_pred_udf("range_contained_by", |a, b| b.contains_range(a))
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
