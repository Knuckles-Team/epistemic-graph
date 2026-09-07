use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt;

pub const SEMANTIC_BINDING_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-binding/v1";
pub const SEMANTIC_VECTOR_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-vector/v1";
pub const SEMANTIC_LEXICAL_INDEX_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-lexical-index/v1";
pub const SEMANTIC_ANN_INDEX_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-ann-index/v1";
pub const SEMANTIC_BINDING_STATE_RECEIPT_DOMAIN: &[u8] = b"au-eg/semantic-binding-state-receipt/v1";
pub const SEMANTIC_APPROVAL_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-index-approval/v1";
pub const SEMANTIC_POLICY_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-policy/v1";
pub const SEMANTIC_ACTIVATION_TARGET_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-activation-target/v1";
pub const SEMANTIC_STAGE_INTENT_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-stage-intent/v1";
pub const SEMANTIC_STAGE_RECEIPT_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-stage-receipt/v1";
pub const SEMANTIC_DEAD_LETTER_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-dead-letter/v1";
pub const SEMANTIC_TOMBSTONE_RECEIPT_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-tombstone-receipt/v1";
pub const SEMANTIC_ENTITY_SET_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-entity-set/v1";
pub const SEMANTIC_AGGREGATE_RECEIPT_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-aggregate-receipt/v1";
pub const SEMANTIC_AGGREGATE_ARTIFACT_DIGEST_DOMAIN: &[u8] =
    b"au-eg/semantic-aggregate-artifact/v1";
pub const SEMANTIC_GENERATION_CHECKPOINT_DIGEST_DOMAIN: &[u8] =
    b"au-eg/semantic-generation-checkpoint/v1";
pub const SEMANTIC_SQL_SOURCE_MANIFEST_DIGEST_DOMAIN: &[u8] =
    b"au-eg/semantic-sql-source-manifest/v1";
pub const SEMANTIC_SQL_SOURCE_IDENTITY_DIGEST_DOMAIN: &[u8] =
    b"au-eg/semantic-sql-source-identity/v1";
pub const SEMANTIC_GRAPH_PROJECTION_MANIFEST_DIGEST_DOMAIN: &[u8] =
    b"au-eg/semantic-graph-projection-manifest/v1";
pub const SEMANTIC_AUTHORIZATION_RECEIPT_DIGEST_DOMAIN: &[u8] =
    b"au-eg/semantic-authorization-receipt/v1";
pub const SEMANTIC_LINEAGE_DIGEST_DOMAIN: &[u8] = b"au-eg/semantic-lineage/v1";

/// A SHA-256 semantic contract identity, serialized as `sha256:<lower-hex>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SemanticDigest([u8; 32]);

impl SemanticDigest {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        let encoded = value
            .strip_prefix("sha256:")
            .ok_or_else(|| "semantic digest must start with 'sha256:'".to_string())?;
        if encoded.len() != 64
            || encoded
                .bytes()
                .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err("semantic digest must contain exactly 64 hexadecimal digits".to_string());
        }
        let bytes = hex::decode(encoded)
            .map_err(|error| format!("semantic digest is not valid hexadecimal: {error}"))?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "semantic digest must decode to 32 bytes".to_string())?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for SemanticDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sha256:{}", hex::encode(self.0))
    }
}

impl Serialize for SemanticDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SemanticDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

pub(crate) fn domain_digest(domain: &[u8], subject: Vec<u8>) -> SemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(subject);
    SemanticDigest(hasher.finalize().into())
}

/// Small, closed RFC 8949 core-deterministic encoder for RF-019 digest subjects.
/// It cannot emit indefinite lengths, non-preferred integers, or non-finite
/// floats. Map keys are sorted by their deterministic encoded bytes.
pub(crate) mod cbor {
    pub(crate) fn text(value: &str) -> Vec<u8> {
        let mut encoded = head(3, value.len() as u64);
        encoded.extend_from_slice(value.as_bytes());
        encoded
    }

    pub(crate) fn unsigned(value: u64) -> Vec<u8> {
        head(0, value)
    }

    pub(crate) fn boolean(value: bool) -> Vec<u8> {
        vec![if value { 0xf5 } else { 0xf4 }]
    }

    pub(crate) fn null() -> Vec<u8> {
        vec![0xf6]
    }

    pub(crate) fn digest(value: super::SemanticDigest) -> Vec<u8> {
        text(&value.to_string())
    }

    pub(crate) fn array(values: impl IntoIterator<Item = Vec<u8>>) -> Vec<u8> {
        let values: Vec<_> = values.into_iter().collect();
        let mut encoded = head(4, values.len() as u64);
        for value in values {
            encoded.extend(value);
        }
        encoded
    }

    pub(crate) fn map(entries: impl IntoIterator<Item = (&'static str, Vec<u8>)>) -> Vec<u8> {
        map_encoded(
            entries
                .into_iter()
                .map(|(key, value)| (text(key), value))
                .collect(),
        )
    }

    pub(crate) fn map_owned(entries: impl IntoIterator<Item = (String, Vec<u8>)>) -> Vec<u8> {
        map_encoded(
            entries
                .into_iter()
                .map(|(key, value)| (text(&key), value))
                .collect(),
        )
    }

    fn map_encoded(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        assert!(
            entries.windows(2).all(|pair| pair[0].0 != pair[1].0),
            "deterministic CBOR identity maps cannot contain duplicate keys"
        );
        let mut encoded = head(5, entries.len() as u64);
        for (key, value) in entries {
            encoded.extend(key);
            encoded.extend(value);
        }
        encoded
    }

    pub(crate) fn finite_f32(value: f32) -> Result<Vec<u8>, String> {
        if !value.is_finite() {
            return Err("semantic vector values must be finite".to_string());
        }
        if let Some(bits) = exact_f16_bits(value) {
            let mut encoded = vec![0xf9];
            encoded.extend_from_slice(&bits.to_be_bytes());
            return Ok(encoded);
        }
        let mut encoded = vec![0xfa];
        encoded.extend_from_slice(&value.to_bits().to_be_bytes());
        Ok(encoded)
    }

    fn head(major: u8, value: u64) -> Vec<u8> {
        let prefix = major << 5;
        match value {
            0..=23 => vec![prefix | value as u8],
            24..=0xff => vec![prefix | 24, value as u8],
            0x100..=0xffff => {
                let mut encoded = vec![prefix | 25];
                encoded.extend_from_slice(&(value as u16).to_be_bytes());
                encoded
            }
            0x1_0000..=0xffff_ffff => {
                let mut encoded = vec![prefix | 26];
                encoded.extend_from_slice(&(value as u32).to_be_bytes());
                encoded
            }
            _ => {
                let mut encoded = vec![prefix | 27];
                encoded.extend_from_slice(&value.to_be_bytes());
                encoded
            }
        }
    }

    fn exact_f16_bits(value: f32) -> Option<u16> {
        let bits = value.to_bits();
        let sign = ((bits >> 16) & 0x8000) as u16;
        let exponent = ((bits >> 23) & 0xff) as i32;
        let fraction = bits & 0x7f_ffff;
        let half = if exponent == 0 {
            sign
        } else {
            let unbiased = exponent - 127;
            if (-14..=15).contains(&unbiased) {
                if fraction & 0x1fff != 0 {
                    return None;
                }
                sign | (((unbiased + 15) as u16) << 10) | ((fraction >> 13) as u16)
            } else if (-24..=-15).contains(&unbiased) {
                let significand = fraction | 0x80_0000;
                let shift = (-unbiased - 1) as u32;
                let mask = (1_u32 << shift) - 1;
                if significand & mask != 0 {
                    return None;
                }
                sign | ((significand >> shift) as u16)
            } else {
                return None;
            }
        };
        (f16_to_f32(half).to_bits() == bits).then_some(half)
    }

    pub(crate) fn f16_to_f32(bits: u16) -> f32 {
        let sign = ((bits as u32) & 0x8000) << 16;
        let exponent = ((bits >> 10) & 0x1f) as u32;
        let fraction = (bits & 0x03ff) as u32;
        let decoded = match exponent {
            0 if fraction == 0 => sign,
            0 => {
                let leading = 31 - fraction.leading_zeros();
                let normalized_fraction = (fraction << (10 - leading)) & 0x03ff;
                let f32_exponent = 127 - 14 - (10 - leading);
                sign | (f32_exponent << 23) | (normalized_fraction << 13)
            }
            31 => sign | 0x7f80_0000 | (fraction << 13),
            _ => sign | ((exponent + 112) << 23) | (fraction << 13),
        };
        f32::from_bits(decoded)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn preferred_float_width_is_used() {
            assert_eq!(finite_f32(1.5).unwrap(), vec![0xf9, 0x3e, 0x00]);
            assert_eq!(
                finite_f32(1_000_000.5).unwrap(),
                vec![0xfa, 0x49, 0x74, 0x24, 0x08]
            );
            assert!(finite_f32(f32::NAN).is_err());
        }

        #[test]
        #[should_panic(expected = "cannot contain duplicate keys")]
        fn maps_reject_duplicate_keys() {
            map([("binding_id", unsigned(1)), ("binding_id", unsigned(2))]);
        }

        #[test]
        fn maps_sort_by_encoded_key_bytes() {
            assert_eq!(
                map([("z", unsigned(1)), ("a", unsigned(2))]),
                vec![0xa2, 0x61, b'a', 0x02, 0x61, b'z', 0x01]
            );
        }
    }
}
