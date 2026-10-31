//! Exact wire scalars shared by the model, the certificate and the verifier.
//!
//! `i128` values cross the wire as decimal strings, so no JSON or MessagePack
//! codec narrows them to 64 bits or to a float. Digests are lower-case hex.

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// An exact 128-bit signed integer (objective values, bounds, multipliers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "contract-schema", schemars(with = "String"))]
pub struct Scalar(i128);

impl Scalar {
    pub const fn new(value: i128) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i128 {
        self.0
    }
}

impl From<Scalar> for String {
    fn from(value: Scalar) -> Self {
        value.0.to_string()
    }
}

/// A wire scalar that is not a canonical decimal `i128`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarParseError(pub String);

impl fmt::Display for ScalarParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not a canonical decimal i128: {:?}", self.0)
    }
}

impl std::error::Error for ScalarParseError {}

impl TryFrom<String> for Scalar {
    type Error = ScalarParseError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        match text.parse::<i128>() {
            Ok(value) if value.to_string() == text => Ok(Self(value)),
            _ => Err(ScalarParseError(text)),
        }
    }
}

/// A SHA-256 digest, serialised as 64 lower-case hex characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "contract-schema", schemars(with = "String"))]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Digest `value`'s JSON encoding under a domain-separation tag.
    ///
    /// The solver's wire types hold no maps and no floats, so their JSON
    /// encoding is a pure function of the value: field order is declaration
    /// order and every sequence keeps its order.
    pub fn of_json<T: Serialize>(domain: &str, value: &T) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(domain.as_bytes());
        hasher.update([0u8]);
        serde_json::to_writer(&mut hasher, value).expect(
            "solver wire types have no maps or non-string keys, so JSON encoding cannot fail",
        );
        Self(hasher.finalize().into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<Sha256Digest> for String {
    fn from(value: Sha256Digest) -> Self {
        hex::encode(value.0)
    }
}

impl TryFrom<String> for Sha256Digest {
    type Error = ScalarParseError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        let canonical =
            text.len() == 64 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        let mut bytes = [0u8; 32];
        if canonical && hex::decode_to_slice(&text, &mut bytes).is_ok() {
            Ok(Self(bytes))
        } else {
            Err(ScalarParseError(text))
        }
    }
}
