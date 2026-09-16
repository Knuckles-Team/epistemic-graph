//! Scalar fidelity and bounded structured content for SQL source submissions.

use crate::contract::{BoundedVec, MAX_RECORD_BYTES};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const MAX_SQL_SOURCE_VECTOR_DIMENSIONS: usize = 4_096;

/// A finite IEEE-754 double. Invalid values cannot enter through deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceFloat(f64);

impl SqlSourceFloat {
    pub fn new(value: f64) -> Result<Self, String> {
        if !value.is_finite() {
            return Err("SQL source float must be finite".into());
        }
        Ok(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for SqlSourceFloat {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// A bounded, finite native f32 embedding. An empty embedding is invalid.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceVector(BoundedVec<f32, MAX_SQL_SOURCE_VECTOR_DIMENSIONS>);

impl SqlSourceVector {
    pub fn new(values: Vec<f32>) -> Result<Self, String> {
        Self::checked(BoundedVec::new(values)?)
    }

    fn checked(values: BoundedVec<f32, MAX_SQL_SOURCE_VECTOR_DIMENSIONS>) -> Result<Self, String> {
        if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
            return Err("SQL source vector must contain finite values".into());
        }
        Ok(Self(values))
    }

    pub fn as_slice(&self) -> &[f32] {
        self.0.as_slice()
    }
}

impl<'de> Deserialize<'de> for SqlSourceVector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::checked(BoundedVec::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Bounded JSON content, stored as canonical JSON bytes so object ordering does
/// not change semantic digests. This is JSON data, never JSON-encoded row cells.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceJson(
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    crate::contract::RecordBytes,
);

impl SqlSourceJson {
    pub fn new(value: Value) -> Result<Self, String> {
        validate_json_depth(&value, 0)?;
        let encoded = bounded_json(&value)?;
        Self::from_bytes(crate::contract::RecordBytes::new(encoded)?)
    }

    pub fn from_bytes(bytes: crate::contract::RecordBytes) -> Result<Self, String> {
        // Parsing is bounded by RecordBytes, and serde_json's default recursion
        // guard stays enabled. The lower shared wire depth is checked below.
        let value = serde_json::from_slice::<crate::msgpack::UniqueJsonValue>(bytes.as_slice())
            .map_err(|_| "SQL source JSON content is invalid or has duplicate keys")?
            .0;
        validate_json_depth(&value, 0)?;
        let canonical = canonical_json(value);
        let bytes = bounded_json(&canonical)?;
        Ok(Self(crate::contract::RecordBytes::new(bytes)?))
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn value(&self) -> Result<Value, String> {
        serde_json::from_slice(self.canonical_bytes()).map_err(|error| error.to_string())
    }
}

impl<'de> Deserialize<'de> for SqlSourceJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::from_bytes(crate::contract::RecordBytes::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

fn validate_json_depth(value: &Value, depth: usize) -> Result<(), String> {
    if depth > crate::msgpack::DEFAULT_MAX_DEPTH {
        return Err("SQL source JSON nesting limit exceeded".into());
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_json_depth(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_json_depth(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let sorted: std::collections::BTreeMap<_, _> = values.into_iter().collect();
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        other => other,
    }
}

/// Text is capped in UTF-8 bytes, without restricting source-system characters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceText(String);

impl SqlSourceText {
    pub fn new(value: String) -> Result<Self, String> {
        if value.len() > MAX_RECORD_BYTES {
            return Err("SQL source text exceeds the record byte limit".into());
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SqlSourceText {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Serialization caps apply while writing, before an oversized temporary is built.
struct CappedBuffer {
    bytes: Vec<u8>,
    maximum: usize,
}

impl CappedBuffer {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }
}

impl std::io::Write for CappedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "SQL source content byte limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bounded_json(value: &Value) -> Result<Vec<u8>, String> {
    let mut buffer = CappedBuffer::new(MAX_RECORD_BYTES);
    serde_json::to_writer(&mut buffer, value).map_err(|_| "SQL source JSON byte limit exceeded")?;
    Ok(buffer.bytes)
}

pub(super) fn bounded_msgpack(value: &impl Serialize, maximum: usize) -> Result<Vec<u8>, String> {
    let mut buffer = CappedBuffer::new(maximum);
    let mut serializer = rmp_serde::Serializer::new(&mut buffer).with_struct_map();
    value
        .serialize(&mut serializer)
        .map_err(|_| "SQL source batch byte limit exceeded")?;
    Ok(buffer.bytes)
}
