//! Strictly versioned ANN metadata wire.
//!
//! Records are postcard payloads prefixed with an explicit magic/version. Unknown
//! formats fail closed: this workspace owns every consumer and intentionally does
//! not carry dual-format readers or compatibility fallbacks.

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;

const MAGIC: &[u8] = b"EGANN\x01\0";

#[derive(Debug)]
pub(crate) struct CodecError(String);

impl CodecError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CodecError {}

pub(crate) fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    let payload = postcard::to_stdvec(value)
        .map_err(|_| CodecError::new("ANN metadata serialization failed"))?;
    let mut encoded = Vec::with_capacity(MAGIC.len() + payload.len());
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub(crate) fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    let payload = bytes
        .strip_prefix(MAGIC)
        .ok_or_else(|| CodecError::new("unsupported ANN metadata format; rebuild the index"))?;
    let (value, remaining) = postcard::take_from_bytes(payload)
        .map_err(|_| CodecError::new("ANN metadata is invalid"))?;
    if !remaining.is_empty() {
        return Err(CodecError::new("ANN metadata has trailing bytes"));
    }
    Ok(value)
}

#[cfg(test)]
pub(crate) fn is_current(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_wire_is_versioned_and_round_trips() {
        let value = (42_u32, "ok".to_string());
        let encoded = serialize(&value).unwrap();
        assert!(is_current(&encoded));
        assert_eq!(deserialize::<(u32, String)>(&encoded).unwrap(), value);
    }

    #[test]
    fn rejects_unknown_formats_and_trailing_data() {
        assert!(deserialize::<u32>(&[42]).is_err());

        let mut encoded = serialize(&42_u32).unwrap();
        encoded.push(0);
        assert!(deserialize::<u32>(&encoded).is_err());
    }
}
