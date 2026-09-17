//! [`super::scalar_to_key`]'s per-`ScalarValue`-type-group arms
//! (CONCEPT:EG-KG.query.read-only-sql-query).
//!
//! Split out of `providers.rs` (a "shared module instead of growing a file
//! past the file-size caps" split, not a rename: `providers.rs` keeps its own
//! path, `scalar_to_key` itself, and every caller; this module holds only the
//! three arm-group helpers `scalar_to_key` chains through).

use datafusion::common::ScalarValue;

use super::IndexKey;

/// The string/boolean arm of [`super::scalar_to_key`].
pub(super) fn text_or_bool_key(v: &ScalarValue) -> Option<IndexKey> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// The signed/unsigned integer arm of [`super::scalar_to_key`].
pub(super) fn integer_key(v: &ScalarValue) -> Option<IndexKey> {
    match v {
        ScalarValue::Int8(Some(n)) => Some(n.to_string()),
        ScalarValue::Int16(Some(n)) => Some(n.to_string()),
        ScalarValue::Int32(Some(n)) => Some(n.to_string()),
        ScalarValue::Int64(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt8(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt16(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt32(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt64(Some(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// The floating-point arm of [`super::scalar_to_key`].
pub(super) fn float_key(v: &ScalarValue) -> Option<IndexKey> {
    match v {
        ScalarValue::Float32(Some(f)) => Some((*f as f64).to_string()),
        ScalarValue::Float64(Some(f)) => Some(f.to_string()),
        _ => None,
    }
}
