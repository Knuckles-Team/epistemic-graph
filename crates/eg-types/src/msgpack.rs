//! Allocation-free MessagePack structural validation.
//!
//! A byte-length limit alone does not protect a serde decoder: a five-byte
//! `array32` header can advertise billions of elements and influence a visitor's
//! allocation before the truncated body is noticed.  This module lives in
//! `eg-types` so both the outer RPC transport and every nested MessagePack field
//! can run the same grammar-level preflight before deserialization.

use std::fmt;

use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

/// Conservative default nesting ceiling used by the native wire protocol.
pub const DEFAULT_MAX_DEPTH: usize = 64;
/// Shared ceiling for one graph property blob recovered from durable storage or a
/// nested protocol field. Individual transports may impose a smaller outer limit.
pub const MAX_PROPERTY_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_PROPERTY_ITEMS: usize = 1_000_000;

/// Resource limits for exactly one MessagePack value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MsgpackLimits {
    pub max_bytes: usize,
    pub max_items: usize,
    pub max_depth: usize,
}

impl MsgpackLimits {
    pub const fn new(max_bytes: usize, max_items: usize, max_depth: usize) -> Self {
        Self {
            max_bytes,
            max_items,
            max_depth,
        }
    }
}

/// Deliberately opaque error: callers should not reflect parser internals or
/// attacker-controlled content into logs or protocol responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MsgpackValidationError;

fn take(input: &[u8], cursor: &mut usize, len: usize) -> Result<(), MsgpackValidationError> {
    *cursor = (*cursor)
        .checked_add(len)
        .filter(|end| *end <= input.len())
        .ok_or(MsgpackValidationError)?;
    Ok(())
}

fn length(input: &[u8], cursor: &mut usize, width: usize) -> Result<usize, MsgpackValidationError> {
    let start = *cursor;
    take(input, cursor, width)?;
    Ok(match width {
        1 => input[start] as usize,
        2 => u16::from_be_bytes([input[start], input[start + 1]]) as usize,
        4 => u32::from_be_bytes([
            input[start],
            input[start + 1],
            input[start + 2],
            input[start + 3],
        ]) as usize,
        _ => return Err(MsgpackValidationError),
    })
}

fn scan_collection(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    depth: usize,
    remaining_items: &mut usize,
    max_depth: usize,
) -> Result<(), MsgpackValidationError> {
    // Every encoded value consumes at least one byte. Reject impossible declared
    // lengths before looping, independently of the item budget.
    if count > input.len().saturating_sub(*cursor) || count > *remaining_items {
        return Err(MsgpackValidationError);
    }
    for _ in 0..count {
        scan_value(input, cursor, depth + 1, remaining_items, max_depth)?;
    }
    Ok(())
}

fn scan_map(
    input: &[u8],
    cursor: &mut usize,
    pairs: usize,
    depth: usize,
    remaining_items: &mut usize,
    max_depth: usize,
) -> Result<(), MsgpackValidationError> {
    let values = pairs.checked_mul(2).ok_or(MsgpackValidationError)?;
    scan_collection(input, cursor, values, depth, remaining_items, max_depth)
}

fn scan_value(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    remaining_items: &mut usize,
    max_depth: usize,
) -> Result<(), MsgpackValidationError> {
    if depth > max_depth || *remaining_items == 0 {
        return Err(MsgpackValidationError);
    }
    *remaining_items -= 1;
    let marker = *input.get(*cursor).ok_or(MsgpackValidationError)?;
    *cursor += 1;
    match marker {
        // Positive/negative fixint, nil, and booleans have no payload.
        0x00..=0x7f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => Ok(()),
        0x80..=0x8f => scan_map(
            input,
            cursor,
            (marker & 0x0f) as usize,
            depth,
            remaining_items,
            max_depth,
        ),
        0x90..=0x9f => scan_collection(
            input,
            cursor,
            (marker & 0x0f) as usize,
            depth,
            remaining_items,
            max_depth,
        ),
        0xa0..=0xbf => take(input, cursor, (marker & 0x1f) as usize),
        0xc4 | 0xd9 => {
            let len = length(input, cursor, 1)?;
            take(input, cursor, len)
        }
        0xc5 | 0xda => {
            let len = length(input, cursor, 2)?;
            take(input, cursor, len)
        }
        0xc6 | 0xdb => {
            let len = length(input, cursor, 4)?;
            take(input, cursor, len)
        }
        // ext8/ext16/ext32: one type byte followed by `len` data bytes.
        0xc7 => {
            let len = length(input, cursor, 1)?
                .checked_add(1)
                .ok_or(MsgpackValidationError)?;
            take(input, cursor, len)
        }
        0xc8 => {
            let len = length(input, cursor, 2)?
                .checked_add(1)
                .ok_or(MsgpackValidationError)?;
            take(input, cursor, len)
        }
        0xc9 => {
            let len = length(input, cursor, 4)?
                .checked_add(1)
                .ok_or(MsgpackValidationError)?;
            take(input, cursor, len)
        }
        0xca | 0xce | 0xd2 => take(input, cursor, 4),
        0xcb | 0xcf | 0xd3 => take(input, cursor, 8),
        0xcc | 0xd0 => take(input, cursor, 1),
        0xcd | 0xd1 => take(input, cursor, 2),
        // fixext payload includes the signed extension type byte.
        0xd4 => take(input, cursor, 2),
        0xd5 => take(input, cursor, 3),
        0xd6 => take(input, cursor, 5),
        0xd7 => take(input, cursor, 9),
        0xd8 => take(input, cursor, 17),
        0xdc => {
            let count = length(input, cursor, 2)?;
            scan_collection(input, cursor, count, depth, remaining_items, max_depth)
        }
        0xdd => {
            let count = length(input, cursor, 4)?;
            scan_collection(input, cursor, count, depth, remaining_items, max_depth)
        }
        0xde => {
            let count = length(input, cursor, 2)?;
            scan_map(input, cursor, count, depth, remaining_items, max_depth)
        }
        0xdf => {
            let count = length(input, cursor, 4)?;
            scan_map(input, cursor, count, depth, remaining_items, max_depth)
        }
        // 0xc1 is reserved and every defined marker is covered above.
        _ => Err(MsgpackValidationError),
    }
}

/// Validate exactly one complete MessagePack value without allocating.
pub fn validate_single_value(
    input: &[u8],
    limits: MsgpackLimits,
) -> Result<(), MsgpackValidationError> {
    if input.is_empty()
        || input.len() > limits.max_bytes
        || limits.max_items == 0
        || limits.max_depth == 0
    {
        return Err(MsgpackValidationError);
    }
    let mut cursor = 0usize;
    let mut remaining_items = limits.max_items;
    scan_value(
        input,
        &mut cursor,
        0,
        &mut remaining_items,
        limits.max_depth,
    )?;
    if cursor == input.len() {
        Ok(())
    } else {
        Err(MsgpackValidationError)
    }
}

/// Deserialize exactly one MessagePack value only after the allocation-free
/// structural preflight has accepted its byte, item, and nesting budgets.
///
/// This is the required entry point for bytes recovered from durable storage or
/// embedded in another protocol.  Those surfaces do not pass through the native
/// RPC frame validator and therefore must not call `rmp_serde::from_slice`
/// directly.  The deliberately opaque error prevents parser details or stored
/// attacker-controlled content from being reflected into logs and responses.
pub fn decode_bounded<T: DeserializeOwned>(
    input: &[u8],
    limits: MsgpackLimits,
) -> Result<T, MsgpackValidationError> {
    validate_single_value(input, limits)?;
    rmp_serde::from_slice(input).map_err(|_| MsgpackValidationError)
}

/// Decode the repository's canonical MessagePack-encoded JSON property value under
/// one allocation/depth policy. Keeping this in `eg-types` prevents query, compute,
/// indexing, and server adapters from accidentally reintroducing unbounded serde
/// allocation hints at each property read site.
pub fn decode_property_value(input: &[u8]) -> Result<serde_json::Value, MsgpackValidationError> {
    validate_single_value(
        input,
        MsgpackLimits::new(MAX_PROPERTY_BYTES, MAX_PROPERTY_ITEMS, DEFAULT_MAX_DEPTH),
    )?;
    rmp_serde::from_slice::<UniqueJsonValue>(input)
        .map(|value| value.0)
        .map_err(|_| MsgpackValidationError)
}

/// JSON-compatible MessagePack value whose map visitor rejects a repeated key at
/// every nesting level. Deserializing directly into `serde_json::Value` would make
/// duplicate maps last-write-wins, which is ambiguous for schemas, authorization
/// metadata, and graph properties.
struct UniqueJsonValue(serde_json::Value);

impl<'de> Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON-compatible MessagePack value with unique map keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .map(UniqueJsonValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueJsonValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(UniqueJsonValue(value)) = seq.next_element()? {
            values.push(value);
        }
        Ok(UniqueJsonValue(serde_json::Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some((key, UniqueJsonValue(value))) =
            map.next_entry::<String, UniqueJsonValue>()?
        {
            if values.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate MessagePack map key"));
            }
        }
        Ok(UniqueJsonValue(serde_json::Value::Object(values)))
    }
}

/// Decode a canonical property object, rejecting valid non-map values as malformed
/// for call sites whose schema requires an object.
pub fn decode_property_object(
    input: &[u8],
) -> Result<serde_json::Map<String, serde_json::Value>, MsgpackValidationError> {
    match decode_property_value(input)? {
        serde_json::Value::Object(object) => Ok(object),
        _ => Err(MsgpackValidationError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(items: usize) -> MsgpackLimits {
        MsgpackLimits::new(1024, items, 8)
    }

    #[test]
    fn rejects_declared_allocation_bombs_and_truncation() {
        assert!(validate_single_value(&[0xdd, 0xff, 0xff, 0xff, 0xff], limits(1000)).is_err());
        assert!(validate_single_value(&[0xdc, 0x00, 0x02, 0xc0], limits(1000)).is_err());
        let three_nils = [0x93, 0xc0, 0xc0, 0xc0];
        assert!(validate_single_value(&three_nils, limits(3)).is_err());
        assert!(validate_single_value(&three_nils, limits(4)).is_ok());
    }

    #[test]
    fn bounds_depth_bytes_and_exact_framing() {
        let nested = [0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0xc0];
        assert!(validate_single_value(&nested, limits(100)).is_err());
        assert!(validate_single_value(&[0x81, 0xa1, b'k', 0x92, 0x01, 0x02], limits(10)).is_ok());
        assert!(validate_single_value(&[0xc0, 0xc0], limits(10)).is_err());
        assert!(
            validate_single_value(&[0xa3, b'a', b'b', b'c'], MsgpackLimits::new(3, 2, 2)).is_err()
        );
    }

    #[test]
    fn bounded_decode_rejects_declared_collection_before_serde() {
        let allocation_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_bounded::<Vec<serde_json::Value>>(&allocation_bomb, limits(1000)).is_err());

        let encoded = [0x92, 0x01, 0x02];
        assert_eq!(
            decode_bounded::<Vec<u8>>(&encoded, limits(3)).unwrap(),
            vec![1, 2]
        );
    }

    #[test]
    fn property_decode_rejects_duplicate_keys_at_every_depth() {
        let duplicate_root = [0x82, 0xa1, b'a', 0x01, 0xa1, b'a', 0x02];
        assert!(decode_property_value(&duplicate_root).is_err());

        let duplicate_nested = [0x81, 0xa1, b'o', 0x82, 0xa1, b'a', 0x01, 0xa1, b'a', 0x02];
        assert!(decode_property_value(&duplicate_nested).is_err());

        let unique = [0x82, 0xa1, b'a', 0x01, 0xa1, b'b', 0x92, 0x02, 0x03];
        assert_eq!(
            decode_property_value(&unique).unwrap(),
            serde_json::json!({"a": 1, "b": [2, 3]})
        );
    }
}
