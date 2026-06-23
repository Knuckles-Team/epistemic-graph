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
