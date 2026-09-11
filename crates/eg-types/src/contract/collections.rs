//! Bounded collection and record-byte values.

use super::{Digest256, MAX_RECORD_BYTES};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedVec<T, const MAXIMUM: usize>(Vec<T>);

impl<T, const MAXIMUM: usize> BoundedVec<T, MAXIMUM> {
    pub fn new(values: Vec<T>) -> Result<Self, String> {
        if values.len() > MAXIMUM {
            return Err(format!("collection exceeds {MAXIMUM} items"));
        }
        Ok(Self(values))
    }
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }
    pub fn windows(&self, size: usize) -> std::slice::Windows<'_, T> {
        self.0.windows(size)
    }
}

impl<'a, T, const MAXIMUM: usize> IntoIterator for &'a BoundedVec<T, MAXIMUM> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T: Serialize, const MAXIMUM: usize> Serialize for BoundedVec<T, MAXIMUM> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

struct BoundedVecVisitor<T, const MAXIMUM: usize>(std::marker::PhantomData<T>);

impl<'de, T: Deserialize<'de>, const MAXIMUM: usize> Visitor<'de>
    for BoundedVecVisitor<T, MAXIMUM>
{
    type Value = BoundedVec<T, MAXIMUM>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "an array containing at most {MAXIMUM} items")
    }
    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence.size_hint().is_some_and(|size| size > MAXIMUM) {
            return Err(A::Error::custom(format!(
                "collection exceeds {MAXIMUM} items"
            )));
        }
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAXIMUM));
        while let Some(value) = sequence.next_element()? {
            if values.len() == MAXIMUM {
                return Err(A::Error::custom(format!(
                    "collection exceeds {MAXIMUM} items"
                )));
            }
            values.push(value);
        }
        Ok(BoundedVec(values))
    }
}

impl<'de, T: Deserialize<'de>, const MAXIMUM: usize> Deserialize<'de> for BoundedVec<T, MAXIMUM> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedVecVisitor(std::marker::PhantomData))
    }
}

/// A bounded array of `T`. The bound is part of the contract, so it is emitted
/// as `maxItems` rather than left to prose.
#[cfg(feature = "contract-schema")]
impl<T: schemars::JsonSchema, const MAXIMUM: usize> schemars::JsonSchema
    for BoundedVec<T, MAXIMUM>
{
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(format!("BoundedVec_{}_{}", T::schema_name(), MAXIMUM))
    }
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let items = generator.subschema_for::<T>();
        schemars::Schema::try_from(serde_json::json!({
            "type": "array",
            "items": items,
            "maxItems": MAXIMUM,
        }))
        .expect("a JSON object is a valid schema")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBytes(Vec<u8>);

impl RecordBytes {
    pub fn new(bytes: Vec<u8>) -> Result<Self, String> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err("record exceeds the 1 MiB contract limit".into());
        }
        Ok(Self(bytes))
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
    pub fn digest(&self) -> Result<Digest256, String> {
        Digest256::framed(b"eg/record-bytes/v1", &[self.as_slice()])
    }
}

impl Serialize for RecordBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_slice())
    }
}

struct RecordBytesVisitor;

impl<'de> Visitor<'de> for RecordBytesVisitor {
    type Value = RecordBytes;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("at most 1 MiB of record bytes")
    }
    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value.len() > MAX_RECORD_BYTES {
            return Err(E::custom("record exceeds the 1 MiB contract limit"));
        }
        Ok(RecordBytes(value.to_vec()))
    }
    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        if sequence
            .size_hint()
            .is_some_and(|size| size > MAX_RECORD_BYTES)
        {
            return Err(A::Error::custom("record exceeds the 1 MiB contract limit"));
        }
        let mut bytes = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_RECORD_BYTES));
        while let Some(byte) = sequence.next_element()? {
            if bytes.len() == MAX_RECORD_BYTES {
                return Err(A::Error::custom("record exceeds the 1 MiB contract limit"));
            }
            bytes.push(byte);
        }
        Ok(RecordBytes(bytes))
    }
}

impl<'de> Deserialize<'de> for RecordBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(RecordBytesVisitor)
    }
}

#[cfg(test)]
pub(super) struct BinaryFixture(pub Vec<u8>);

#[cfg(test)]
impl Serialize for BinaryFixture {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{TenantId, MAX_TENANT_ID_BYTES};

    #[test]
    fn messagepack_decode_rejects_oversize_string_and_collection() {
        let encoded = rmp_serde::to_vec(&"x".repeat(MAX_TENANT_ID_BYTES + 1)).unwrap();
        assert!(rmp_serde::from_slice::<TenantId>(&encoded).is_err());
        let encoded = rmp_serde::to_vec(&vec![1_u8, 2, 3]).unwrap();
        assert!(rmp_serde::from_slice::<BoundedVec<u8, 2>>(&encoded).is_err());
    }

    #[test]
    fn record_bytes_reject_oversize_borrowed_input_before_copy() {
        let encoded = rmp_serde::to_vec(&BinaryFixture(vec![0; MAX_RECORD_BYTES + 1])).unwrap();
        assert!(rmp_serde::from_slice::<RecordBytes>(&encoded).is_err());
    }
}
