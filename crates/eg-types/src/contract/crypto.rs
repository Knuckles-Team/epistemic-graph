//! Fixed-width cryptographic values and canonical digest framing.

use super::{ED25519_SIGNATURE_BYTES, NONCE_BYTES, SHA256_BYTES, SHA256_HEX_BYTES};
use serde::de::{Error as _, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest256([u8; SHA256_BYTES]);

impl Digest256 {
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
    /// SHA-256 of the exact bytes, without domain framing.
    ///
    /// Use this for contracts that identify an externally supplied byte body
    /// (for example a Turtle schema document). Semantic records should prefer
    /// [`Self::framed`], which prevents ambiguous concatenation and binds a
    /// domain.
    pub fn sha256(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self(hasher.finalize().into())
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

impl fmt::Debug for Digest256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Digest256")
            .field(&self.to_hex())
            .finish()
    }
}
impl fmt::Display for Digest256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Nonce([u8; NONCE_BYTES]);

impl Nonce {
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
impl Nonce {
    /// Mint one attempt nonce, server-side.
    ///
    /// `NonceReplayKey` is the ATTEMPT identity: it exists so a duplicated
    /// attempt of one operation is refused while a fresh attempt of the same
    /// operation replays. A nonce is therefore never accepted from a caller --
    /// a caller-pinned value would let a caller make its own retry
    /// undeliverable, and the caller's stable retry identity is the idempotency
    /// key, which lives inside `OperationReplayIdentity`.
    ///
    /// The property this mint owes is UNIQUENESS PER ATTEMPT, not
    /// unpredictability: the value is minted and consumed inside the engine,
    /// compared only against the engine's own `mutation_replay_nonces` rows, and
    /// never round-trips through anything a caller can influence. Guessing one
    /// buys nothing, because there is no path that accepts a guessed value. The
    /// nonce a caller DOES see -- the transport envelope's -- is authenticated
    /// by its MAC at the request boundary and threaded in explicitly; it is not
    /// this.
    ///
    /// Uniqueness comes from three independent sources folded together: this
    /// process's `RandomState` (seeded by the OS at first use), a monotonic
    /// per-process counter, and the wall clock. Two attempts in the same
    /// process differ by the counter; two processes differ by the seed.
    pub fn minted() -> Self {
        use std::hash::{BuildHasher, Hasher};
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::OnceLock;
        use std::time::{SystemTime, UNIX_EPOCH};

        static PROCESS_SEED: OnceLock<u64> = OnceLock::new();
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let seed = *PROCESS_SEED.get_or_init(|| {
            let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
            hasher.write_u64(std::process::id().into());
            hasher.finish()
        });
        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64);
        let mut hasher = Sha256::new();
        hasher.update(b"eg/attempt-nonce/v1\0");
        hasher.update(seed.to_be_bytes());
        hasher.update(sequence.to_be_bytes());
        hasher.update(elapsed.to_be_bytes());
        Self(hasher.finalize().into())
    }
}

impl fmt::Debug for Nonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Nonce([redacted])")
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
fixed_hex_serde!(Digest256);
fixed_hex_serde!(Nonce);

/// JSON Schema for the two fixed-width hex scalars.
///
/// Both serialize as exactly 64 lowercase hex characters (see
/// [`serialize_fixed_hex`]/[`parse_fixed_hex`]), so the schema states that shape
/// rather than the private byte array the type stores. Hand-written because the
/// serde impls are hand-written: a derive would describe the Rust field, not the
/// wire value, and the generated contract must describe the wire.
#[cfg(feature = "contract-schema")]
macro_rules! fixed_hex_schema {
    ($name:ident, $description:literal) => {
        impl schemars::JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!($name))
            }
            fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::Schema::try_from(serde_json::json!({
                    "type": "string",
                    "description": $description,
                    "pattern": "^[0-9a-f]{64}$",
                    "minLength": SHA256_HEX_BYTES,
                    "maxLength": SHA256_HEX_BYTES,
                }))
                .expect("a JSON object is a valid schema")
            }
        }
    };
}
#[cfg(feature = "contract-schema")]
fixed_hex_schema!(Digest256, "32 bytes of SHA-256 as lowercase hexadecimal");
#[cfg(feature = "contract-schema")]
fixed_hex_schema!(Nonce, "32 bytes of attempt nonce as lowercase hexadecimal");

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ed25519Signature([u8; ED25519_SIGNATURE_BYTES]);
impl Ed25519Signature {
    pub fn from_bytes(bytes: [u8; ED25519_SIGNATURE_BYTES]) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; ED25519_SIGNATURE_BYTES] {
        &self.0
    }
}
impl fmt::Debug for Ed25519Signature {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Ed25519Signature([64 bytes])")
    }
}
impl Serialize for Ed25519Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

struct Ed25519SignatureVisitor;
impl Visitor<'_> for Ed25519SignatureVisitor {
    type Value = Ed25519Signature;
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
        Ok(Ed25519Signature(bytes))
    }
}
impl<'de> Deserialize<'de> for Ed25519Signature {
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
        assert!(Digest256::parse(&uppercase).is_err());
        assert!(Nonce::parse(&uppercase).is_err());
        assert!(Digest256::parse(&"aa".repeat(32)).is_ok());
        assert!(Nonce::parse(&"aa".repeat(32)).is_ok());
    }
    #[test]
    fn framing_preserves_field_boundaries() {
        let first = Digest256::framed(b"test/v1", &[b"ab", b"c"]).unwrap();
        let second = Digest256::framed(b"test/v1", &[b"a", b"bc"]).unwrap();
        assert_ne!(first, second);
    }
    #[test]
    fn ed25519_signature_requires_exact_binary_width() {
        let exact = rmp_serde::to_vec(&BinaryFixture(vec![7; ED25519_SIGNATURE_BYTES])).unwrap();
        assert!(rmp_serde::from_slice::<Ed25519Signature>(&exact).is_ok());
        let short =
            rmp_serde::to_vec(&BinaryFixture(vec![7; ED25519_SIGNATURE_BYTES - 1])).unwrap();
        assert!(rmp_serde::from_slice::<Ed25519Signature>(&short).is_err());
        let long = rmp_serde::to_vec(&BinaryFixture(vec![7; ED25519_SIGNATURE_BYTES + 1])).unwrap();
        assert!(rmp_serde::from_slice::<Ed25519Signature>(&long).is_err());
    }
}
