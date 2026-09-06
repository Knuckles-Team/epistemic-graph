//! Fixed-width cryptographic values and canonical digest framing.

use super::{ED25519_SIGNATURE_BYTES, NONCE_BYTES, SHA256_BYTES, SHA256_HEX_BYTES};
use serde::de::{Error as _, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest256V1([u8; SHA256_BYTES]);

impl Digest256V1 {
    pub fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Self {
        Self(bytes)
    }
    pub fn parse(value: &str) -> Result<Self, String> {
        parse_fixed_hex(value)
            .map(Self)
            .map_err(|_| "digest must be exactly 64 lowercase hexadecimal characters".into())
    }
    pub fn as_bytes(&self) -> &[u8; SHA256_BYTES] {
        &self.0
    }
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
    pub fn framed(domain: &[u8], fields: &[&[u8]]) -> Result<Self, String> {
        if domain.is_empty() || domain.len() > u32::MAX as usize {
            return Err("digest domain is empty or too large".into());
        }
        let field_count = u32::try_from(fields.len())
            .map_err(|_| "digest field count exceeds the framing limit")?;
        let mut hasher = Sha256::new();
        hasher.update(b"eg/framed-sha256/v1\0");
        hasher.update((domain.len() as u32).to_be_bytes());
        hasher.update(domain);
        hasher.update(field_count.to_be_bytes());
        for field in fields {
            let field_len =
                u64::try_from(field.len()).map_err(|_| "digest field exceeds the framing limit")?;
            hasher.update(field_len.to_be_bytes());
            hasher.update(field);
        }
        Ok(Self(hasher.finalize().into()))
    }
}

impl fmt::Debug for Digest256V1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Digest256V1")
            .field(&self.to_hex())
            .finish()
    }
}
impl fmt::Display for Digest256V1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonceV1([u8; NONCE_BYTES]);

impl NonceV1 {
    pub fn from_bytes(bytes: [u8; NONCE_BYTES]) -> Self {
        Self(bytes)
    }
    pub fn parse(value: &str) -> Result<Self, String> {
        parse_fixed_hex(value)
            .map(Self)
            .map_err(|_| "nonce must be exactly 32 bytes encoded as lowercase hex".into())
    }
    pub fn as_bytes(&self) -> &[u8; NONCE_BYTES] {
        &self.0
    }
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}
impl fmt::Debug for NonceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NonceV1([redacted])")
    }
}

fn serialize_fixed_hex<S>(bytes: &[u8; SHA256_BYTES], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&hex::encode(bytes))
}

fn parse_fixed_hex(value: &str) -> Result<[u8; SHA256_BYTES], String> {
    if value.len() != SHA256_HEX_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("value must be exactly 32 bytes encoded as lowercase hex".into());
    }
    let mut bytes = [0_u8; SHA256_BYTES];
    hex::decode_to_slice(value, &mut bytes)
        .map_err(|_| "value must be exactly 32 bytes encoded as lowercase hex".to_string())?;
    Ok(bytes)
}

struct FixedHexVisitor;
impl Visitor<'_> for FixedHexVisitor {
    type Value = [u8; SHA256_BYTES];
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exactly 64 lowercase hexadecimal characters")
    }
    fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        parse_fixed_hex(value).map_err(E::custom)
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        parse_fixed_hex(value).map_err(E::custom)
    }
}

macro_rules! fixed_hex_serde {
    ($name:ident) => {
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serialize_fixed_hex(&self.0, serializer)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer
                    .deserialize_str(FixedHexVisitor)
                    .map(Self)
                    .map_err(D::Error::custom)
            }
        }
    };
}
fixed_hex_serde!(Digest256V1);
fixed_hex_serde!(NonceV1);

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ed25519SignatureV1([u8; ED25519_SIGNATURE_BYTES]);
impl Ed25519SignatureV1 {
    pub fn from_bytes(bytes: [u8; ED25519_SIGNATURE_BYTES]) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; ED25519_SIGNATURE_BYTES] {
        &self.0
    }
}
impl fmt::Debug for Ed25519SignatureV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Ed25519SignatureV1([64 bytes])")
    }
}
impl Serialize for Ed25519SignatureV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

struct Ed25519SignatureVisitor;
impl Visitor<'_> for Ed25519SignatureVisitor {
    type Value = Ed25519SignatureV1;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exactly 64 bytes of Ed25519 signature material")
    }
    fn visit_borrowed_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_bytes(value)
    }
    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let bytes: [u8; ED25519_SIGNATURE_BYTES] = value
            .try_into()
            .map_err(|_| E::custom("Ed25519 signature must contain exactly 64 bytes"))?;
        Ok(Ed25519SignatureV1(bytes))
    }
}
impl<'de> Deserialize<'de> for Ed25519SignatureV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(Ed25519SignatureVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::super::collections::BinaryFixture;
    use super::*;
    #[test]
    fn digest_and_nonce_reject_uppercase() {
        let uppercase = "AA".repeat(32);
        assert!(Digest256V1::parse(&uppercase).is_err());
        assert!(NonceV1::parse(&uppercase).is_err());
        assert!(Digest256V1::parse(&"aa".repeat(32)).is_ok());
        assert!(NonceV1::parse(&"aa".repeat(32)).is_ok());
    }
    #[test]
    fn framing_preserves_field_boundaries() {
        let first = Digest256V1::framed(b"test/v1", &[b"ab", b"c"]).unwrap();
        let second = Digest256V1::framed(b"test/v1", &[b"a", b"bc"]).unwrap();
        assert_ne!(first, second);
    }
    #[test]
    fn ed25519_signature_requires_exact_binary_width() {
        let exact = rmp_serde::to_vec(&BinaryFixture(vec![7; ED25519_SIGNATURE_BYTES])).unwrap();
        assert!(rmp_serde::from_slice::<Ed25519SignatureV1>(&exact).is_ok());
        let short =
            rmp_serde::to_vec(&BinaryFixture(vec![7; ED25519_SIGNATURE_BYTES - 1])).unwrap();
        assert!(rmp_serde::from_slice::<Ed25519SignatureV1>(&short).is_err());
        let long = rmp_serde::to_vec(&BinaryFixture(vec![7; ED25519_SIGNATURE_BYTES + 1])).unwrap();
        assert!(rmp_serde::from_slice::<Ed25519SignatureV1>(&long).is_err());
    }
}
